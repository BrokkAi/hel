//! Target-side checkpoint collection and controller-side verified transfer.
//!
//! Targets own the Git worktrees and native harness history, so they build the
//! archive. The controller downloads into a same-directory temporary file and
//! only returns a teardown gate after reopening and verifying the installed
//! archive.

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::hel_archive::{
    ArchiveInput, BundleManifest, CanonicalSessionSnapshot, CanonicalTranscriptBody,
    GitCollectionSpec, GitCommand, GitCommandRunner, GitHistoryMode, NativeArtifact, PayloadRole,
    RepositorySnapshot, SessionManifest, SystemGit, TargetManifest, collect_git_metadata_snapshot,
    collect_git_snapshot, has_origin_refs, read_archive_verified, restore_git_snapshot,
    verify_archive_streaming, write_archive_hashed,
};
use crate::hel_config::HarnessKind;
use crate::hel_targets::{
    CommandExecutor, CommandPlan, CommandSpec, SshTarget, TargetLocator, join_remote_command,
    worker_root,
};
const MAX_NATIVE_FILE: u64 = 1024 * 1024 * 1024;
const MAX_NATIVE_TOTAL: u64 = 8 * 1024 * 1024 * 1024;
pub const RESTORED_CANONICAL_SESSION_FILE: &str = "canonical-session.json";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointRepositorySpec {
    pub id: String,
    pub relative_destination: PathBuf,
    pub capture: CheckpointRepositoryCapture,
    pub origin_override: Option<String>,
}

/// Required repository capture semantics for the checkpoint protocol floor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum CheckpointRepositoryCapture {
    /// Bundle commits reachable from HEAD but from no origin ref.
    SessionDelta,
    /// Bundle the repository relative to an explicit commit.
    DeltaFrom { base_commit: String },
    /// Preserve Git provenance only. The existing worktree remains the source.
    MetadataOnly,
}

/// Uploaded target-side input. It contains provenance and paths, never secrets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointExportSpec {
    pub session: SessionManifest,
    pub target: TargetManifest,
    pub bundle: BundleManifest,
    pub relay_root: PathBuf,
    pub harness_home: PathBuf,
    pub workspace_root: PathBuf,
    pub repositories: Vec<CheckpointRepositorySpec>,
    /// Controller projection latched at the relay's checkpoint barrier.
    pub canonical_session: CanonicalSessionSnapshot,
    pub output_path: PathBuf,
}

impl CheckpointExportSpec {
    pub fn read(path: &Path) -> Result<Self> {
        let body = fs::read(path)
            .with_context(|| format!("read checkpoint export spec {}", path.display()))?;
        serde_json::from_slice(&body)
            .with_context(|| format!("parse checkpoint export spec {}", path.display()))
    }

    pub fn read_from(reader: &mut impl std::io::Read) -> Result<Self> {
        let mut body = Vec::new();
        reader
            .read_to_end(&mut body)
            .context("read checkpoint export spec from standard input")?;
        serde_json::from_slice(&body).context("parse checkpoint export spec from standard input")
    }

    pub fn write(&self, path: &Path) -> Result<()> {
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;
        let body = serde_json::to_vec_pretty(self)?;
        let mut options = fs::OpenOptions::new();
        options.create(true).truncate(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(path)?;
        std::io::Write::write_all(&mut file, &body)?;
        file.sync_all()?;
        restrict_permissions(path)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetCheckpoint {
    pub path: PathBuf,
    pub sha256: String,
    pub event_frontier: u64,
    pub event_frontier_digest: String,
}

/// Spec argument that means "read the export spec from standard input".
///
/// Streaming the spec saves one round trip to the target, which is time the
/// relay spends with ACP dispatch frozen behind the checkpoint barrier.
pub const EXPORT_SPEC_STDIN: &str = "-";

/// Hidden target CLI entry point: `hel worker export-checkpoint --spec PATH|-`.
pub fn export_from_spec_file(path: &Path) -> Result<TargetCheckpoint> {
    if path == Path::new(EXPORT_SPEC_STDIN) {
        return export_from_spec_reader(&mut std::io::stdin().lock());
    }
    export_checkpoint(&CheckpointExportSpec::read(path)?)
}

pub fn export_from_spec_reader(reader: &mut impl std::io::Read) -> Result<TargetCheckpoint> {
    export_checkpoint(&CheckpointExportSpec::read_from(reader)?)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointRestoreSpec {
    pub archive_path: PathBuf,
    pub workspace_root: PathBuf,
    pub relay_root: PathBuf,
    pub harness_home: PathBuf,
    pub restore_repositories: bool,
    pub restore_native: bool,
    pub discard_queued_prompts: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryRestoreSpec {
    pub archive_path: PathBuf,
    pub workspace_root: PathBuf,
}

pub fn restore_repositories_from_spec_file(path: &Path) -> Result<()> {
    let body = fs::read(path)
        .with_context(|| format!("read repository restore spec {}", path.display()))?;
    let mut spec: RepositoryRestoreSpec = serde_json::from_slice(&body)
        .with_context(|| format!("parse repository restore spec {}", path.display()))?;
    spec.archive_path = resolve_target_path(&spec.archive_path)?;
    spec.workspace_root = resolve_target_path(&spec.workspace_root)?;
    restore_repositories(&spec.archive_path, &spec.workspace_root, &SystemGit)
}

pub fn restore_from_spec_file(path: &Path) -> Result<()> {
    let body = fs::read(path)
        .with_context(|| format!("read checkpoint restore spec {}", path.display()))?;
    let mut spec: CheckpointRestoreSpec = serde_json::from_slice(&body)
        .with_context(|| format!("parse checkpoint restore spec {}", path.display()))?;
    spec.archive_path = resolve_target_path(&spec.archive_path)?;
    spec.workspace_root = resolve_target_path(&spec.workspace_root)?;
    spec.relay_root = resolve_target_path(&spec.relay_root)?;
    spec.harness_home = resolve_target_path(&spec.harness_home)?;
    restore_checkpoint(&spec, &SystemGit)
}

pub fn restore_checkpoint(spec: &CheckpointRestoreSpec, git: &dyn GitCommandRunner) -> Result<()> {
    ensure!(spec.workspace_root.is_dir(), "restore workspace is missing");
    let archive = read_archive_verified(&spec.archive_path)?;
    // Deserialize and validate the schema-2 canonical projection before any
    // repository, relay, or native-session state can be mutated.
    let mut canonical_session = archive.canonical_session()?;
    if spec.discard_queued_prompts {
        canonical_session.queued_prompts.clear();
    }
    if spec.restore_repositories {
        restore_repositories_from_archive(&archive, &spec.workspace_root, git)?;
    }

    fs::create_dir_all(&spec.relay_root)?;
    crate::hel_worker::clear_native_session_identity(&spec.relay_root)?;
    write_private_file(
        &restored_canonical_session_path(&spec.relay_root),
        &serde_json::to_vec_pretty(&canonical_session)?,
        0o600,
    )?;

    if spec.restore_native {
        for descriptor in &archive.manifest.payloads {
            let PayloadRole::NativeArtifact { relative_path } = &descriptor.role else {
                continue;
            };
            let native_data = archive.payload(descriptor)?;
            let relative_path = restored_native_relative_path(
                archive.manifest.session.harness_kind,
                relative_path,
                &archive.manifest.bundle.primary_repository,
                &archive.manifest.repositories,
                &spec.workspace_root,
            )?;
            let native_data = restored_native_artifact_bytes(
                archive.manifest.session.harness_kind,
                &relative_path,
                native_data,
                &archive.manifest.bundle.primary_repository,
                &archive.manifest.repositories,
                &spec.workspace_root,
                &spec.harness_home,
            )?;
            validate_relative_path(&relative_path)?;
            ensure!(
                !secret_like_path(&relative_path),
                "native artifact path is secret-like"
            );
            let destination = spec.harness_home.join(relative_path);
            write_private_file(&destination, &native_data, descriptor.mode)?;
        }
    }
    Ok(())
}

pub fn restored_canonical_session_path(relay_root: &Path) -> PathBuf {
    relay_root.join(RESTORED_CANONICAL_SESSION_FILE)
}

/// Reads the controller-owned projection without restoring target artifacts.
pub fn read_checkpoint_session(path: &Path) -> Result<CanonicalSessionSnapshot> {
    Ok(verify_archive_streaming(path)?.canonical_session)
}

pub fn restore_repositories(
    archive_path: &Path,
    workspace_root: &Path,
    git: &dyn GitCommandRunner,
) -> Result<()> {
    ensure!(workspace_root.is_dir(), "restore workspace is missing");
    let archive = read_archive_verified(archive_path)?;
    restore_repositories_from_archive(&archive, workspace_root, git)
}

fn restore_repositories_from_archive(
    archive: &crate::hel_archive::VerifiedArchive,
    workspace_root: &Path,
    git: &dyn GitCommandRunner,
) -> Result<()> {
    for repository in &archive.manifest.repositories {
        let id = &repository.metadata.id;
        let snapshot = RepositorySnapshot {
            metadata: repository.metadata.clone(),
            committed_bundle: archive
                .payload_by_role(&PayloadRole::GitBundle {
                    repository_id: id.clone(),
                })?
                .to_vec(),
            staged_patch: archive
                .payload_by_role(&PayloadRole::GitStagedPatch {
                    repository_id: id.clone(),
                })?
                .to_vec(),
            unstaged_patch: archive
                .payload_by_role(&PayloadRole::GitUnstagedPatch {
                    repository_id: id.clone(),
                })?
                .to_vec(),
            untracked_tar: archive
                .payload_by_role(&PayloadRole::GitUntrackedTar {
                    repository_id: id.clone(),
                })?
                .to_vec(),
        };
        let path = workspace_root.join(&repository.metadata.relative_destination);
        restore_git_snapshot(git, &path, &snapshot)
            .with_context(|| format!("restore repository {id:?}"))?;
    }
    Ok(())
}

/// Native session files use harness-specific working-directory keys. Rewrite
fn restored_native_relative_path(
    harness: HarnessKind,
    relative_path: &Path,
    primary_repository: &str,
    repositories: &[crate::hel_archive::RepositoryManifest],
    workspace_root: &Path,
) -> Result<PathBuf> {
    let Some(target_cwd) = target_primary_cwd(primary_repository, repositories, workspace_root)
    else {
        return Ok(relative_path.to_path_buf());
    };
    let mut components = relative_path.components();
    match harness {
        HarnessKind::Claude => {
            if components.next() != Some(Component::Normal("projects".as_ref()))
                || components.next().is_none()
            {
                return Ok(relative_path.to_path_buf());
            }
            let mut rewritten = PathBuf::from("projects");
            rewritten.push(claude_project_slug(&target_cwd));
            rewritten.extend(components);
            Ok(rewritten)
        }
        HarnessKind::Kimi => {
            if components.next() != Some(Component::Normal("sessions".as_ref()))
                || components.next().is_none()
            {
                return Ok(relative_path.to_path_buf());
            }
            let mut rewritten = PathBuf::from("sessions");
            rewritten.push(kimi_workspace_key(&target_cwd));
            rewritten.extend(components);
            Ok(rewritten)
        }
        HarnessKind::Codex => Ok(relative_path.to_path_buf()),
    }
}

fn target_primary_cwd(
    primary_repository: &str,
    repositories: &[crate::hel_archive::RepositoryManifest],
    workspace_root: &Path,
) -> Option<PathBuf> {
    repositories
        .iter()
        .find(|repository| repository.metadata.id == primary_repository)
        .map(|primary| workspace_root.join(&primary.metadata.relative_destination))
}
fn restored_native_artifact_bytes(
    harness: HarnessKind,
    relative_path: &Path,
    data: &[u8],
    primary_repository: &str,
    repositories: &[crate::hel_archive::RepositoryManifest],
    workspace_root: &Path,
    harness_home: &Path,
) -> Result<Vec<u8>> {
    if harness != HarnessKind::Kimi {
        return Ok(data.to_vec());
    }
    let Some(target_cwd) = target_primary_cwd(primary_repository, repositories, workspace_root)
    else {
        return Ok(data.to_vec());
    };
    if relative_path == Path::new("workspaces.json") {
        return rewrite_kimi_workspace_registry(data, &target_cwd);
    }
    if relative_path == Path::new("session_index.jsonl") {
        return rewrite_kimi_session_index(data, &target_cwd, harness_home);
    }
    if !is_kimi_session_state(relative_path) {
        return Ok(data.to_vec());
    }
    let mut state: Value =
        serde_json::from_slice(data).context("parse Kimi native session state")?;
    let object = state
        .as_object_mut()
        .context("Kimi native session state is not a JSON object")?;
    for key in ["workDir", "cwd"] {
        if object.contains_key(key) {
            object.insert(
                key.into(),
                Value::String(target_cwd.to_string_lossy().into_owned()),
            );
        }
    }
    Ok(serde_json::to_vec(&state)?)
}

fn rewrite_kimi_workspace_registry(data: &[u8], target_cwd: &Path) -> Result<Vec<u8>> {
    let mut registry: Value =
        serde_json::from_slice(data).context("parse Kimi workspace registry")?;
    let workspaces = registry
        .get_mut("workspaces")
        .and_then(Value::as_object_mut)
        .context("Kimi workspace registry has no workspaces object")?;
    ensure!(
        workspaces.len() == 1,
        "Kimi workspace registry must contain one imported workspace"
    );
    let (_, mut workspace) = std::mem::take(workspaces)
        .into_iter()
        .next()
        .expect("one workspace was checked");
    let workspace = workspace
        .as_object_mut()
        .context("Kimi workspace registry entry is not an object")?;
    workspace.insert(
        "root".into(),
        Value::String(target_cwd.to_string_lossy().into_owned()),
    );
    if let Some(name) = target_cwd.file_name().and_then(|name| name.to_str()) {
        workspace.insert("name".into(), Value::String(name.to_owned()));
    }
    workspaces.insert(kimi_workspace_key(target_cwd), workspace.clone().into());
    Ok(serde_json::to_vec(&registry)?)
}

fn rewrite_kimi_session_index(
    data: &[u8],
    target_cwd: &Path,
    harness_home: &Path,
) -> Result<Vec<u8>> {
    let mut rewritten = Vec::new();
    let target_workspace = kimi_workspace_key(target_cwd);
    for (line_number, line) in std::str::from_utf8(data)
        .context("decode Kimi session index")?
        .lines()
        .enumerate()
    {
        if line.trim().is_empty() {
            continue;
        }
        let mut entry: Value = serde_json::from_str(line)
            .with_context(|| format!("parse Kimi session index line {}", line_number + 1))?;
        let session_id = entry
            .get("sessionId")
            .and_then(Value::as_str)
            .context("Kimi session index entry lacks sessionId")?
            .to_owned();
        let entry = entry
            .as_object_mut()
            .context("Kimi session index entry is not an object")?;
        entry.insert(
            "workDir".into(),
            Value::String(target_cwd.to_string_lossy().into_owned()),
        );
        entry.insert(
            "sessionDir".into(),
            Value::String(
                harness_home
                    .join("sessions")
                    .join(&target_workspace)
                    .join(session_id)
                    .to_string_lossy()
                    .into_owned(),
            ),
        );
        serde_json::to_writer(&mut rewritten, &entry)?;
        rewritten.push(b'\n');
    }
    ensure!(
        !rewritten.is_empty(),
        "Kimi session index has no imported sessions"
    );
    Ok(rewritten)
}

fn is_kimi_session_state(relative_path: &Path) -> bool {
    let mut components = relative_path.components();
    matches!(components.next(), Some(Component::Normal(component)) if component == "sessions")
        && matches!(components.next(), Some(Component::Normal(_)))
        && matches!(components.next(), Some(Component::Normal(component)) if component.to_string_lossy().starts_with("session_"))
        && matches!(components.next(), Some(Component::Normal(component)) if component == "state.json")
        && components.next().is_none()
}

/// Claude Code's on-disk project-key algorithm, captured from local rollouts:
/// every non-ASCII-alphanumeric cwd character becomes a hyphen.
pub fn claude_project_slug(cwd: &Path) -> String {
    cwd.to_string_lossy()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '-'
            }
        })
        .collect()
}

/// Kimi Code keys session directories by the final cwd component and the
/// first 12 hexadecimal digits of SHA-256(cwd), captured from real rollouts.
fn kimi_workspace_key(cwd: &Path) -> String {
    let basename = cwd
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("workspace");
    let digest = format!("{:x}", Sha256::digest(cwd.to_string_lossy().as_bytes()));
    format!("wd_{basename}_{}", &digest[..12])
}

fn write_private_file(path: &Path, bytes: &[u8], mode: u32) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    ensure!(!path.exists() || !fs::symlink_metadata(path)?.file_type().is_symlink());
    fs::write(path, bytes)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(mode & 0o700))?;
    }
    #[cfg(not(unix))]
    let _ = mode;
    Ok(())
}

pub fn export_checkpoint(spec: &CheckpointExportSpec) -> Result<TargetCheckpoint> {
    export_checkpoint_with_git(spec, &SystemGit)
}

pub fn export_checkpoint_with_git(
    spec: &CheckpointExportSpec,
    git: &dyn GitCommandRunner,
) -> Result<TargetCheckpoint> {
    let mut resolved = spec.clone();
    resolved.relay_root = resolve_target_path(&resolved.relay_root)?;
    resolved.harness_home = resolve_target_path(&resolved.harness_home)?;
    resolved.workspace_root = resolve_target_path(&resolved.workspace_root)?;
    resolved.output_path = resolve_target_path(&resolved.output_path)?;
    let spec = &resolved;
    validate_export_spec(spec)?;
    let event_frontier = spec.canonical_session.event_frontier;
    let event_frontier_digest = spec.canonical_session.event_frontier_digest.clone();
    // A session that never accepted a prompt legitimately has no native
    // harness artifacts yet; requiring them would make an unused session
    // impossible to close cleanly.
    let prompted = canonical_session_contains_prompt(&spec.canonical_session);
    let native_artifacts = collect_native_artifacts(
        spec.session.harness_kind,
        &spec.harness_home,
        &spec.session.native_session_id,
        !prompted,
    )?;
    // Indexed parallel iteration preserves the spec order, which in turn keeps
    // the manifest and ZIP entry order deterministic.
    let repositories = spec
        .repositories
        .par_iter()
        .map(|repository| {
            let path = spec.workspace_root.join(&repository.relative_destination);
            ensure!(path.is_dir(), "repository {} is missing", path.display());
            let history = match &repository.capture {
                CheckpointRepositoryCapture::MetadataOnly => {
                    return collect_git_metadata_snapshot(
                        git,
                        &path,
                        &GitCollectionSpec {
                            id: repository.id.clone(),
                            relative_destination: repository.relative_destination.clone(),
                            history: GitHistoryMode::NoBundle,
                            origin_override: repository.origin_override.clone(),
                        },
                    )
                    .with_context(|| format!("repository '{}'", repository.id));
                }
                CheckpointRepositoryCapture::SessionDelta => {
                    repair_origin_refs(git, &path, &repository.id)?;
                    GitHistoryMode::SessionDelta
                }
                CheckpointRepositoryCapture::DeltaFrom { base_commit } => {
                    GitHistoryMode::DeltaFrom(base_commit.clone())
                }
            };
            reject_dirty_submodules(git, &path)
                .with_context(|| format!("repository '{}'", repository.id))?;
            collect_git_snapshot(
                git,
                &path,
                &GitCollectionSpec {
                    id: repository.id.clone(),
                    relative_destination: repository.relative_destination.clone(),
                    history,
                    origin_override: repository.origin_override.clone(),
                },
            )
            .with_context(|| format!("repository '{}'", repository.id))
        })
        .collect::<Result<Vec<_>>>()?;
    // The export runs while the relay's barrier freezes ACP dispatch, so it
    // hashes the archive it just wrote instead of structurally re-reading it.
    // `CheckpointTransfer::execute` performs the one full structural verify,
    // on the copy the controller actually installs.
    let sha256 = write_archive_hashed(
        &spec.output_path,
        &ArchiveInput {
            session: spec.session.clone(),
            target: spec.target.clone(),
            bundle: spec.bundle.clone(),
            canonical_session: spec.canonical_session.clone(),
            native_artifacts,
            repositories,
        },
    )?;
    Ok(TargetCheckpoint {
        path: spec.output_path.clone(),
        sha256,
        event_frontier,
        event_frontier_digest,
    })
}

/// A session delta is measured against every origin ref, so a repository that
/// lost its remote-tracking refs would silently have nothing to exclude. Try
/// one repair fetch, then fail the checkpoint instead of bundling full history.
fn repair_origin_refs(git: &dyn GitCommandRunner, path: &Path, id: &str) -> Result<()> {
    let listed = || has_origin_refs(git, path).with_context(|| format!("repository '{id}'"));
    if listed()? {
        return Ok(());
    }
    let fetch = git.run(
        path,
        &GitCommand {
            arguments: vec!["fetch".into(), "origin".into()],
            stdin: Vec::new(),
        },
    )?;
    if listed()? {
        return Ok(());
    }
    let outcome = if fetch.status == 0 {
        "repair fetch produced no origin refs".to_owned()
    } else {
        format!(
            "repair fetch failed with status {}: {}",
            fetch.status,
            String::from_utf8_lossy(&fetch.stderr).trim()
        )
    };
    bail!(
        "repository '{id}' has no origin refs to delta against; refusing to bundle full history ({outcome})"
    )
}

fn validate_export_spec(spec: &CheckpointExportSpec) -> Result<()> {
    spec.canonical_session.validate()?;
    validate_component(&spec.session.id, "session ID")?;
    validate_component(&spec.session.native_session_id, "native session ID")?;
    ensure!(spec.relay_root.is_dir(), "relay root is missing");
    ensure!(spec.harness_home.is_dir(), "harness home is missing");
    ensure!(spec.workspace_root.is_dir(), "workspace root is missing");
    ensure!(
        !spec.repositories.is_empty(),
        "checkpoint has no repositories"
    );
    let mut ids = BTreeSet::new();
    let mut destinations = BTreeSet::new();
    for repository in &spec.repositories {
        validate_component(&repository.id, "repository ID")?;
        validate_relative_path(&repository.relative_destination)?;
        if let CheckpointRepositoryCapture::DeltaFrom { base_commit } = &repository.capture {
            ensure!(!base_commit.trim().is_empty(), "base commit is empty");
        }
        ensure!(ids.insert(&repository.id), "duplicate repository ID");
        ensure!(
            destinations.insert(&repository.relative_destination),
            "duplicate destination"
        );
    }
    let parent = spec.output_path.parent().unwrap_or_else(|| Path::new("."));
    ensure!(
        parent.starts_with(&spec.relay_root),
        "archive must be beneath relay root"
    );
    Ok(())
}

fn canonical_session_contains_prompt(snapshot: &CanonicalSessionSnapshot) -> bool {
    snapshot
        .transcript
        .iter()
        .any(|item| matches!(&item.body, CanonicalTranscriptBody::User { .. }))
}

pub fn collect_native_artifacts(
    harness: HarnessKind,
    home: &Path,
    session_id: &str,
    allow_empty: bool,
) -> Result<Vec<NativeArtifact>> {
    validate_component(session_id, "native session ID")?;
    let roots: &[&str] = match harness {
        HarnessKind::Codex => &["sessions", "archived_sessions"],
        HarnessKind::Claude => &["projects", "session-env", "file-history"],
        HarnessKind::Kimi => &["sessions"],
    };
    let mut output = Vec::new();
    for relative in roots {
        let root = home.join(relative);
        if root.is_dir() {
            collect_native_tree(harness, home, &root, session_id, false, &mut output)?;
        }
    }
    if harness == HarnessKind::Kimi && !output.is_empty() {
        collect_kimi_registry_artifacts(home, session_id, &mut output)?;
    }
    output.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    ensure!(
        allow_empty || !output.is_empty(),
        "no session artifacts found"
    );
    let total = output
        .iter()
        .try_fold(0_u64, |total, artifact| {
            total.checked_add(artifact.data.len() as u64)
        })
        .context("native artifact size overflow")?;
    ensure!(
        total <= MAX_NATIVE_TOTAL,
        "native session artifacts are too large"
    );
    Ok(output)
}
/// Collect native artifacts for an import whose locator already resolved the
/// exact source artifact. Codex rollouts are standalone JSONL files, so this
/// avoids probing every historical rollout (and any unrelated corrupt one).
pub fn collect_import_native_artifacts(
    harness: HarnessKind,
    home: &Path,
    session_id: &str,
    source_path: &Path,
) -> Result<Vec<NativeArtifact>> {
    if harness != HarnessKind::Codex {
        return collect_native_artifacts(harness, home, session_id, false);
    }
    validate_component(session_id, "native session ID")?;
    let relative = source_path.strip_prefix(home).with_context(|| {
        format!(
            "Codex rollout {} is outside {}",
            source_path.display(),
            home.display()
        )
    })?;
    validate_relative_path(relative)?;
    ensure!(
        matches!(relative.components().next(), Some(Component::Normal(component)) if component == "sessions" || component == "archived_sessions"),
        "Codex rollout '{}' is outside a session root",
        source_path.display()
    );
    let name = source_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    ensure!(
        name.ends_with(".jsonl") || name.ends_with(".jsonl.zst"),
        "Codex rollout '{}' is not a JSONL artifact",
        source_path.display()
    );
    ensure!(
        !secret_like_path(relative),
        "Codex rollout '{}' has a forbidden path",
        source_path.display()
    );
    let metadata = fs::symlink_metadata(source_path)
        .with_context(|| format!("stat Codex rollout {}", source_path.display()))?;
    ensure!(
        !metadata.file_type().is_symlink() && metadata.is_file(),
        "Codex rollout '{}' is not a regular file",
        source_path.display()
    );
    ensure!(
        metadata.len() <= MAX_NATIVE_FILE,
        "Codex rollout is too large"
    );
    Ok(vec![NativeArtifact {
        relative_path: relative.to_path_buf(),
        data: fs::read(source_path)
            .with_context(|| format!("read Codex rollout {}", source_path.display()))?,
        mode: file_mode(&metadata),
    }])
}

fn collect_kimi_registry_artifacts(
    home: &Path,
    session_id: &str,
    output: &mut Vec<NativeArtifact>,
) -> Result<()> {
    let source_workspace = output
        .iter()
        .find_map(|artifact| kimi_source_workspace(&artifact.relative_path, session_id))
        .context("Kimi native session state artifact is missing")?;
    let workspaces_path = home.join("workspaces.json");
    let metadata = fs::symlink_metadata(&workspaces_path)
        .with_context(|| format!("read Kimi workspace registry {}", workspaces_path.display()))?;
    ensure!(
        !metadata.file_type().is_symlink() && metadata.is_file(),
        "Kimi workspace registry is not a regular file"
    );
    ensure!(
        metadata.len() <= MAX_NATIVE_FILE,
        "Kimi workspace registry is too large"
    );
    let workspaces: Value = serde_json::from_slice(&fs::read(&workspaces_path)?)
        .context("parse Kimi workspace registry")?;
    let workspace = workspaces
        .pointer(&format!("/workspaces/{source_workspace}"))
        .cloned()
        .with_context(|| format!("Kimi workspace {source_workspace:?} is missing from registry"))?;
    let mut selected_workspaces = serde_json::Map::new();
    selected_workspaces.insert(source_workspace, workspace);
    output.push(NativeArtifact {
        relative_path: PathBuf::from("workspaces.json"),
        data: serde_json::to_vec(&json!({
            "version": workspaces.get("version").cloned().unwrap_or(Value::Null),
            "deleted_workspace_ids": [],
            "workspaces": selected_workspaces,
        }))?,
        mode: file_mode(&metadata),
    });

    let index_path = home.join("session_index.jsonl");
    let metadata = fs::symlink_metadata(&index_path)
        .with_context(|| format!("read Kimi session index {}", index_path.display()))?;
    ensure!(
        !metadata.file_type().is_symlink() && metadata.is_file(),
        "Kimi session index is not a regular file"
    );
    ensure!(
        metadata.len() <= MAX_NATIVE_FILE,
        "Kimi session index is too large"
    );
    let mut selected = Vec::new();
    for (line_number, line) in fs::read_to_string(&index_path)?.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let entry: Value = serde_json::from_str(line)
            .with_context(|| format!("parse Kimi session index line {}", line_number + 1))?;
        if entry.get("sessionId").and_then(Value::as_str) == Some(session_id) {
            serde_json::to_writer(&mut selected, &entry)?;
            selected.push(b'\n');
        }
    }
    ensure!(
        !selected.is_empty(),
        "Kimi session index does not contain native session {session_id:?}"
    );
    output.push(NativeArtifact {
        relative_path: PathBuf::from("session_index.jsonl"),
        data: selected,
        mode: file_mode(&metadata),
    });
    Ok(())
}

fn kimi_source_workspace(relative_path: &Path, session_id: &str) -> Option<String> {
    let mut components = relative_path.components();
    (components.next() == Some(Component::Normal("sessions".as_ref()))).then_some(())?;
    let workspace = components.next()?.as_os_str().to_str()?.to_owned();
    let session = components.next()?.as_os_str().to_str()?;
    let file = components.next()?.as_os_str().to_str()?;
    (components.next().is_none()
        && file == "state.json"
        && (session == session_id || session == format!("session_{session_id}")))
    .then_some(workspace)
}

fn collect_native_tree(
    harness: HarnessKind,
    home: &Path,
    path: &Path,
    session_id: &str,
    inside_session: bool,
    output: &mut Vec<NativeArtifact>,
) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Ok(());
    }
    let inside = inside_session
        || path.file_name().is_some_and(|name| {
            name == session_id
                || (harness == HarnessKind::Kimi
                    && name.to_str() == Some(&format!("session_{session_id}")))
        });
    if metadata.is_dir() {
        for entry in fs::read_dir(path)? {
            collect_native_tree(harness, home, &entry?.path(), session_id, inside, output)?;
        }
        return Ok(());
    }
    ensure!(metadata.is_file(), "native artifact is not a regular file");
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    let relative = path.strip_prefix(home)?;
    let selected = match harness {
        HarnessKind::Codex => {
            (name.contains(session_id)
                && (name.ends_with(".jsonl") || name.ends_with(".jsonl.zst")))
                || (name.ends_with(".jsonl") && codex_rollout_has_session_id(path, session_id))
        }
        HarnessKind::Claude => inside || name == format!("{session_id}.jsonl"),
        HarnessKind::Kimi => inside && kimi_session_artifact(relative, session_id),
    };
    if !selected || secret_like_path(relative) {
        return Ok(());
    }
    ensure!(
        metadata.len() <= MAX_NATIVE_FILE,
        "native artifact is too large"
    );
    validate_relative_path(relative)?;
    output.push(NativeArtifact {
        relative_path: relative.to_path_buf(),
        data: fs::read(path)?,
        mode: file_mode(&metadata),
    });
    Ok(())
}

fn kimi_session_artifact(relative: &Path, session_id: &str) -> bool {
    let components = relative
        .components()
        .filter_map(|component| match component {
            Component::Normal(component) => component.to_str(),
            _ => None,
        })
        .collect::<Vec<_>>();
    let Some(session_index) = components.iter().position(|component| {
        *component == session_id || *component == format!("session_{session_id}")
    }) else {
        return false;
    };
    matches!(&components[session_index + 1..], ["state.json"])
        || matches!(
            &components[session_index + 1..],
            ["agents", _, "wire.jsonl"]
        )
}

fn codex_rollout_has_session_id(path: &Path, session_id: &str) -> bool {
    let Ok(file) = File::open(path) else {
        return false;
    };
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    for _ in 0..8 {
        line.clear();
        let Ok(read) = reader.read_line(&mut line) else {
            return false;
        };
        if read == 0 {
            break;
        }
        let Ok(record) = serde_json::from_str::<Value>(&line) else {
            return false;
        };
        if record.get("type").and_then(Value::as_str) == Some("session_meta")
            && record
                .pointer("/payload/session_id")
                .and_then(Value::as_str)
                == Some(session_id)
        {
            return true;
        }
    }
    false
}

fn reject_dirty_submodules(runner: &dyn GitCommandRunner, repository: &Path) -> Result<()> {
    let output = runner.run(
        repository,
        &GitCommand {
            arguments: [
                "submodule",
                "foreach",
                "--recursive",
                "--quiet",
                "git status --porcelain",
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
            stdin: Vec::new(),
        },
    )?;
    ensure!(
        output.status == 0,
        "failed to inspect submodules: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    ensure!(
        output.stdout.iter().all(u8::is_ascii_whitespace),
        "dirty submodule is unsupported"
    );
    Ok(())
}

/// Export by streaming the spec to the worker's standard input.
///
/// Every wrapper this builds keeps the target's stdin attached: the container
/// engines are invoked with `exec -i` and `ssh` forwards stdin by default.
pub fn export_stdin_command(locator: &TargetLocator, session_id: &str) -> Result<CommandSpec> {
    export_command(locator, session_id, EXPORT_SPEC_STDIN)
}

pub fn export_command(
    locator: &TargetLocator,
    session_id: &str,
    spec_path: &str,
) -> Result<CommandSpec> {
    validate_remote_path(spec_path)?;
    let root = worker_root(locator, session_id)?;
    let args = vec![
        format!("{root}/hel"),
        "worker".into(),
        "export-checkpoint".into(),
        "--spec".into(),
        spec_path.into(),
    ];
    let command = match locator {
        TargetLocator::LocalBare { .. } => {
            let mut args = args;
            CommandSpec::new(args.remove(0), args)
        }
        TargetLocator::LocalPodman { container_id } => container_exec("podman", container_id, args),
        TargetLocator::AppleContainer { container_id } => {
            container_exec("container", container_id, args)
        }
        TargetLocator::AwsEc2 { ssh, .. } | TargetLocator::SshBare { ssh, .. } => {
            ssh_command(ssh, args)
        }
        TargetLocator::SshPodman { ssh, container_id } => {
            let mut remote = vec![
                "podman".into(),
                "exec".into(),
                "-i".into(),
                container_id.clone(),
            ];
            remote.extend(args);
            ssh_command(ssh, remote)
        }
    };
    Ok(command.purpose("export target checkpoint"))
}

pub fn restore_command(
    locator: &TargetLocator,
    session_id: &str,
    spec_path: &str,
) -> Result<CommandSpec> {
    validate_remote_path(spec_path)?;
    let root = worker_root(locator, session_id)?;
    let args = vec![
        format!("{root}/hel"),
        "worker".into(),
        "restore-checkpoint".into(),
        "--spec".into(),
        spec_path.into(),
    ];
    let command = match locator {
        TargetLocator::LocalBare { .. } => {
            let mut args = args;
            CommandSpec::new(args.remove(0), args)
        }
        TargetLocator::LocalPodman { container_id } => container_exec("podman", container_id, args),
        TargetLocator::AppleContainer { container_id } => {
            container_exec("container", container_id, args)
        }
        TargetLocator::AwsEc2 { ssh, .. } | TargetLocator::SshBare { ssh, .. } => {
            ssh_command(ssh, args)
        }
        TargetLocator::SshPodman { ssh, container_id } => {
            let mut remote = vec![
                "podman".into(),
                "exec".into(),
                "-i".into(),
                container_id.clone(),
            ];
            remote.extend(args);
            ssh_command(ssh, remote)
        }
    };
    Ok(command.purpose("restore target checkpoint"))
}

#[derive(Debug, Clone)]
pub struct CheckpointTransfer<'a> {
    pub locator: &'a TargetLocator,
    pub session_id: &'a str,
    pub remote_archive: &'a str,
    pub destination: &'a Path,
    pub expected_event_frontier: Option<u64>,
    pub expected_event_frontier_digest: Option<&'a str>,
}

/// Unforgeable outside this module: proof that a controller-local archive was
/// verified after its atomic install.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedCheckpoint {
    session_id: String,
    archive_path: PathBuf,
    sha256: String,
    event_frontier: u64,
    event_frontier_digest: String,
}

impl VerifiedCheckpoint {
    pub fn archive_path(&self) -> &Path {
        &self.archive_path
    }
    pub fn sha256(&self) -> &str {
        &self.sha256
    }
    pub fn event_frontier(&self) -> u64 {
        self.event_frontier
    }
    pub fn event_frontier_digest(&self) -> &str {
        &self.event_frontier_digest
    }
    pub const fn teardown_allowed(&self) -> bool {
        true
    }
}

impl CheckpointTransfer<'_> {
    pub fn execute(&self, executor: &impl CommandExecutor) -> Result<VerifiedCheckpoint> {
        validate_remote_path(self.remote_archive)?;
        let parent = self.destination.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;
        let temporary = tempfile::Builder::new()
            .prefix(".hel-checkpoint-")
            .tempfile_in(parent)?;
        let path = temporary.path().to_path_buf();
        transfer_plan(self.locator, self.session_id, self.remote_archive, &path)?
            .execute(executor)
            .context("download target checkpoint")?;
        let verified = verify_archive_streaming(&path).context("verify downloaded checkpoint")?;
        ensure!(
            verified.manifest.session.id == self.session_id,
            "checkpoint session mismatch"
        );
        let canonical_session = verified.canonical_session;
        let event_frontier = canonical_session.event_frontier;
        let event_frontier_digest = canonical_session.event_frontier_digest;
        if let Some(expected) = self.expected_event_frontier {
            ensure!(
                event_frontier == expected,
                "checkpoint event frontier mismatch"
            );
        }
        if let Some(expected) = self.expected_event_frontier_digest {
            ensure!(
                event_frontier_digest == expected,
                "checkpoint event frontier digest mismatch"
            );
        }
        let sha256 = verified.archive_sha256;
        temporary
            .persist(self.destination)
            .map_err(|error| error.error)?;
        // The bytes were already verified in this same directory and the rename
        // is atomic, so installation only has to make the copy private and
        // durable; re-reading it would hash the same archive a second time.
        let post_install = (|| -> Result<()> {
            restrict_permissions(self.destination)?;
            sync_directory(parent)
        })();
        if let Err(error) = post_install {
            return Err(remove_failed_checkpoint_install(self.destination, error));
        }
        Ok(VerifiedCheckpoint {
            session_id: self.session_id.to_owned(),
            archive_path: self.destination.to_path_buf(),
            sha256,
            event_frontier,
            event_frontier_digest,
        })
    }

    pub fn cleanup_plan(&self, gate: &VerifiedCheckpoint) -> Result<CommandPlan> {
        ensure!(
            gate.session_id == self.session_id,
            "checkpoint gate belongs to another session"
        );
        cleanup_plan(self.locator, self.session_id, self.remote_archive)
    }
}

pub fn transfer_plan(
    locator: &TargetLocator,
    session_id: &str,
    remote_archive: &str,
    local_temporary: &Path,
) -> Result<CommandPlan> {
    validate_remote_path(remote_archive)?;
    ensure!(
        local_temporary.is_absolute(),
        "local temporary path must be absolute"
    );
    worker_root(locator, session_id)?;
    let local = local_temporary.to_string_lossy().into_owned();
    let mut commands = match locator {
        TargetLocator::LocalBare { .. } => vec![
            CommandSpec::new("cp", [remote_archive, local.as_str()])
                .purpose("copy local bare checkpoint"),
        ],
        TargetLocator::LocalPodman { container_id } => vec![
            CommandSpec::new(
                "podman",
                ["cp", &format!("{container_id}:{remote_archive}"), &local],
            )
            .purpose("download checkpoint from local Podman"),
        ],
        TargetLocator::AppleContainer { container_id } => vec![
            CommandSpec::new(
                "container",
                ["cp", &format!("{container_id}:{remote_archive}"), &local],
            )
            .purpose("download checkpoint from Apple container"),
        ],
        TargetLocator::AwsEc2 { ssh, .. } | TargetLocator::SshBare { ssh, .. } => {
            vec![scp_command(ssh, remote_archive, &local).purpose("download checkpoint over SSH")]
        }
        TargetLocator::SshPodman { ssh, container_id } => {
            let staging = remote_staging_path(session_id)?;
            vec![
                ssh_command(ssh, ["mkdir", "-p", ".local/share/hel/transfers"])
                    .purpose("create remote checkpoint staging directory"),
                ssh_command(
                    ssh,
                    [
                        "podman",
                        "cp",
                        &format!("{container_id}:{remote_archive}"),
                        &staging,
                    ],
                )
                .purpose("stage remote Podman checkpoint"),
            ]
        }
    };
    if let TargetLocator::SshPodman { ssh, .. } = locator {
        commands.push(
            scp_command(ssh, &remote_staging_path(session_id)?, &local)
                .purpose("download remote Podman checkpoint over SSH"),
        );
    }
    Ok(CommandPlan {
        description: format!("download checkpoint for {session_id}"),
        commands,
    })
}

fn cleanup_plan(locator: &TargetLocator, session_id: &str, remote: &str) -> Result<CommandPlan> {
    validate_remote_path(remote)?;
    worker_root(locator, session_id)?;
    let commands = match locator {
        TargetLocator::LocalBare { .. } => vec![
            CommandSpec::new("rm", ["-f", "--", remote])
                .purpose("remove local bare checkpoint staging"),
        ],
        TargetLocator::LocalPodman { container_id } => vec![container_exec(
            "podman",
            container_id,
            ["rm", "-f", "--", remote],
        )],
        TargetLocator::AppleContainer { container_id } => vec![container_exec(
            "container",
            container_id,
            ["rm", "-f", "--", remote],
        )],
        TargetLocator::AwsEc2 { ssh, .. } | TargetLocator::SshBare { ssh, .. } => {
            vec![ssh_command(ssh, ["rm", "-f", "--", remote])]
        }
        TargetLocator::SshPodman { ssh, container_id } => vec![
            ssh_command(
                ssh,
                ["podman", "exec", container_id, "rm", "-f", "--", remote],
            ),
            ssh_command(ssh, ["rm", "-f", "--", &remote_staging_path(session_id)?]),
        ],
    };
    Ok(CommandPlan {
        description: format!("clean checkpoint for {session_id}"),
        commands,
    })
}

fn remote_staging_path(session_id: &str) -> Result<String> {
    validate_component(session_id, "session ID")?;
    Ok(format!(".local/share/hel/transfers/{session_id}.hel.zip"))
}

fn scp_command(ssh: &SshTarget, remote: &str, local: &str) -> CommandSpec {
    let mut args = ssh.ssh_args.clone();
    for argument in &mut args {
        if argument == "-p" {
            *argument = "-P".into();
        }
    }
    args.push(format!("{}:{remote}", ssh.destination));
    args.push(local.into());
    CommandSpec::new("scp", args)
}

fn ssh_command(ssh: &SshTarget, args: impl IntoIterator<Item = impl AsRef<str>>) -> CommandSpec {
    let remote = args
        .into_iter()
        .map(|arg| arg.as_ref().to_owned())
        .collect::<Vec<_>>();
    let mut command = ssh.ssh_args.clone();
    command.push(ssh.destination.clone());
    command.push(join_remote_command(&remote));
    CommandSpec::new("ssh", command)
}

fn container_exec(
    engine: &str,
    id: &str,
    args: impl IntoIterator<Item = impl Into<String>>,
) -> CommandSpec {
    let mut command = vec!["exec".into(), "-i".into(), id.into()];
    command.extend(args.into_iter().map(Into::into));
    CommandSpec::new(engine, command)
}

fn validate_remote_path(path: &str) -> Result<()> {
    ensure!(!path.is_empty());
    ensure!(
        path.bytes()
            .all(|byte| byte.is_ascii_alphanumeric()
                || matches!(byte, b'/' | b'~' | b'.' | b'-' | b'_')),
        "unsafe remote path"
    );
    ensure!(
        !path.split('/').any(|component| component == ".."),
        "remote path traverses parent"
    );
    Ok(())
}

fn validate_component(value: &str, label: &str) -> Result<()> {
    ensure!(
        !value.is_empty() && value != "." && value != "..",
        "invalid {label}"
    );
    ensure!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "invalid {label}"
    );
    Ok(())
}

fn validate_relative_path(path: &Path) -> Result<()> {
    ensure!(
        !path.as_os_str().is_empty() && !path.is_absolute(),
        "invalid relative path"
    );
    ensure!(
        path.components()
            .all(|part| matches!(part, Component::Normal(_))),
        "relative path traversal"
    );
    Ok(())
}

fn resolve_target_path(path: &Path) -> Result<PathBuf> {
    ensure!(
        !path.components().any(|part| part == Component::ParentDir),
        "target path traverses a parent"
    );
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    let mut components = path.components();
    if components
        .next()
        .is_some_and(|part| part.as_os_str() == "~")
    {
        let home = std::env::var_os("HOME").context("HOME is required to expand target path")?;
        let mut expanded = PathBuf::from(home);
        expanded.extend(components);
        return Ok(expanded);
    }
    ensure!(false, "target path must be absolute or start with '~'");
    unreachable!()
}

fn secret_like_path(path: &Path) -> bool {
    path.components().any(|component| {
        let Component::Normal(value) = component else {
            return true;
        };
        let value = value.to_string_lossy().to_ascii_lowercase();
        value == ".env"
            || value.starts_with(".env.")
            || matches!(
                value.as_str(),
                "auth.json"
                    | "credentials"
                    | "credentials.json"
                    | ".credentials.json"
                    | "token"
                    | "token.json"
                    | "config.json"
                    | "config.toml"
                    | "settings.json"
                    | ".git-credentials"
            )
    })
}

#[cfg(unix)]
fn file_mode(metadata: &fs::Metadata) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode() & 0o777
}

#[cfg(not(unix))]
fn file_mode(_metadata: &fs::Metadata) -> u32 {
    0o600
}

fn restrict_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn remove_failed_checkpoint_install(path: &Path, error: anyhow::Error) -> anyhow::Error {
    match fs::remove_file(path) {
        Ok(()) => error,
        Err(remove_error) if remove_error.kind() == std::io::ErrorKind::NotFound => error,
        Err(remove_error) => error.context(format!(
            "also failed to remove incomplete checkpoint install {}: {remove_error}",
            path.display()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::io::{Read, Write};
    use std::process::Command;
    use std::sync::Mutex;

    use crate::hel_archive::{
        CanonicalExecutionState, CanonicalQueuedPrompt, CanonicalSessionState,
        CanonicalTranscriptItem, GitOutput,
    };
    use crate::hel_targets::CommandOutput;

    const SESSION: &str = "018f9dd2-a3b4-7c8d-9000-123456789abc";
    const NATIVE: &str = "0190aabb-ccdd-7eef-9000-abcdef012345";

    /// Runs real Git, but counts repair fetches and can make them fail without
    /// reaching a network remote.
    struct RecordingGit {
        fetch_failure: bool,
        fetches: Mutex<usize>,
    }

    impl RecordingGit {
        fn forwarding() -> Self {
            Self {
                fetch_failure: false,
                fetches: Mutex::new(0),
            }
        }

        fn with_fetch_failure() -> Self {
            Self {
                fetch_failure: true,
                fetches: Mutex::new(0),
            }
        }

        fn fetches(&self) -> usize {
            *self.fetches.lock().unwrap()
        }
    }

    impl GitCommandRunner for RecordingGit {
        fn run(&self, repository: &Path, command: &GitCommand) -> Result<GitOutput> {
            if command
                .arguments
                .first()
                .is_some_and(|first| first == "fetch")
            {
                *self.fetches.lock().unwrap() += 1;
                if self.fetch_failure {
                    return Ok(GitOutput {
                        status: 128,
                        stdout: Vec::new(),
                        stderr: b"fatal: could not read from remote repository".to_vec(),
                    });
                }
            }
            SystemGit.run(repository, command)
        }
    }

    fn ssh() -> SshTarget {
        SshTarget {
            destination: "dev@example.test".into(),
            ssh_args: vec!["-p".into(), "2222".into()],
        }
    }

    fn locators() -> Vec<TargetLocator> {
        let name = crate::hel_targets::resource_name(SESSION).unwrap();
        vec![
            TargetLocator::LocalBare {
                worker_root: format!("/var/lib/hel/workers/{SESSION}"),
            },
            TargetLocator::LocalPodman {
                container_id: name.clone(),
            },
            TargetLocator::AppleContainer {
                container_id: name.clone(),
            },
            TargetLocator::AwsEc2 {
                profile: "default".into(),
                region: "us-east-1".into(),
                instance_id: "i-0123456789abcdef0".into(),
                ssh: ssh(),
                workspace: format!("~/hel/{SESSION}"),
            },
            TargetLocator::SshBare {
                ssh: ssh(),
                workspace: format!("~/hel/{SESSION}"),
            },
            TargetLocator::SshPodman {
                ssh: ssh(),
                container_id: name,
            },
        ]
    }

    #[test]
    fn transfer_plans_cover_all_target_boundaries() {
        let locators = locators();
        let plans = locators
            .iter()
            .map(|locator| {
                transfer_plan(
                    locator,
                    SESSION,
                    "/var/lib/hel/workers/checkpoint.hel.zip",
                    Path::new("/var/tmp/checkpoint.zip"),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(plans[0].commands[0].program, "cp");
        assert_eq!(plans[1].commands[0].program, "podman");
        assert_eq!(plans[2].commands[0].program, "container");
        assert_eq!(plans[3].commands[0].program, "scp");
        assert_eq!(plans[4].commands[0].program, "scp");
        assert_eq!(plans[5].commands.len(), 3);
        assert!(
            plans[5].commands[1]
                .args
                .last()
                .unwrap()
                .contains("'podman' 'cp'")
        );
        assert!(
            !plans[5]
                .commands
                .iter()
                .flat_map(|command| &command.args)
                .any(|arg| arg == "--remote")
        );
        assert!(plans[3].commands[0].args.contains(&"-P".into()));
    }

    #[test]
    fn empty_native_artifacts_allowed_only_for_unprompted_sessions() {
        let temp = tempfile::tempdir().unwrap();
        let error = collect_native_artifacts(HarnessKind::Codex, temp.path(), NATIVE, false)
            .unwrap_err()
            .to_string();
        assert_eq!(error, "no session artifacts found");
        let artifacts =
            collect_native_artifacts(HarnessKind::Codex, temp.path(), NATIVE, true).unwrap();
        assert!(artifacts.is_empty());
    }
    #[test]
    fn codex_collection_ignores_malformed_unrelated_rollouts() {
        let temp = tempfile::tempdir().unwrap();
        let sessions = temp.path().join("sessions/2026/08/10");
        fs::create_dir_all(&sessions).unwrap();
        fs::write(sessions.join("rollout-unrelated.jsonl"), b"{malformed\n").unwrap();
        let selected = sessions.join("rollout-renamed.jsonl");
        fs::write(
            &selected,
            format!("{{\"type\":\"session_meta\",\"payload\":{{\"session_id\":\"{NATIVE}\"}}}}\n"),
        )
        .unwrap();

        let artifacts =
            collect_native_artifacts(HarnessKind::Codex, temp.path(), NATIVE, false).unwrap();
        assert_eq!(artifacts.len(), 1);
        assert_eq!(
            artifacts[0].relative_path,
            PathBuf::from("sessions/2026/08/10/rollout-renamed.jsonl")
        );
    }

    #[test]
    fn prompt_detection_reads_the_materialized_transcript() {
        let mut snapshot = CanonicalSessionSnapshot {
            event_frontier: 1,
            event_frontier_digest: "a".repeat(64),
            session: CanonicalSessionState {
                execution: CanonicalExecutionState::Idle,
                last_activity_at_ms: Some(1),
                session_title: None,
                configuration: Default::default(),
            },
            transcript: Vec::new(),
            queued_prompts: Vec::new(),
        };
        assert!(!canonical_session_contains_prompt(&snapshot));
        snapshot.transcript.push(CanonicalTranscriptItem {
            stable_id: "user-1".into(),
            position: 1,
            latest_content_event_ordinal: None,
            created_at_ms: 1,
            last_changed_at_ms: 1,
            body: CanonicalTranscriptBody::User {
                content: vec![json!({"type": "text", "text": "hi"})],
            },
        });
        assert!(canonical_session_contains_prompt(&snapshot));
    }

    #[test]
    fn native_allowlist_excludes_credentials_and_other_sessions() {
        let temp = tempfile::tempdir().unwrap();
        let session = temp
            .path()
            .join("sessions/workspace")
            .join(format!("session_{NATIVE}"));
        fs::create_dir_all(session.join("agents/main")).unwrap();
        fs::write(session.join("state.json"), b"state").unwrap();
        fs::write(session.join("agents/main/wire.jsonl"), b"events").unwrap();
        fs::create_dir_all(session.join("agents/main/tasks/bash-noise")).unwrap();
        fs::write(
            session.join("agents/main/tasks/bash-noise/output.log"),
            b"tool output",
        )
        .unwrap();
        fs::write(
            session.join("agents/main/wire.jsonl.bak-before-edit"),
            b"backup",
        )
        .unwrap();
        fs::create_dir_all(session.join("logs")).unwrap();
        fs::write(session.join("logs/kimi-code.log"), b"log").unwrap();
        fs::write(session.join("credentials.json"), b"secret").unwrap();
        let other = temp.path().join("sessions/workspace/other");
        fs::create_dir_all(&other).unwrap();
        fs::write(other.join("state.json"), b"other").unwrap();
        fs::write(
            temp.path().join("workspaces.json"),
            r#"{"version":1,"deleted_workspace_ids":[],"workspaces":{"workspace":{"root":"/work/app","name":"app"}}}"#,
        )
        .unwrap();
        fs::write(
            temp.path().join("session_index.jsonl"),
            format!(
                "{{\"sessionId\":\"{NATIVE}\",\"workDir\":\"/work/app\",\"sessionDir\":\"/sessions/workspace/session_{NATIVE}\"}}\n{{\"sessionId\":\"other\"}}\n"
            ),
        )
        .unwrap();
        let artifacts =
            collect_native_artifacts(HarnessKind::Kimi, temp.path(), NATIVE, false).unwrap();
        assert_eq!(artifacts.len(), 4);
        assert!(
            artifacts.iter().any(|artifact| {
                artifact.relative_path.as_path() == Path::new("workspaces.json")
            })
        );
        let index = artifacts
            .iter()
            .find(|artifact| artifact.relative_path.as_path() == Path::new("session_index.jsonl"))
            .unwrap();
        assert!(std::str::from_utf8(&index.data).unwrap().contains(NATIVE));
        assert!(!std::str::from_utf8(&index.data).unwrap().contains("other"));
        assert!(artifacts.iter().all(|artifact| {
            !artifact
                .relative_path
                .to_string_lossy()
                .contains("credentials")
        }));
        assert!(artifacts.iter().all(|artifact| {
            let path = artifact.relative_path.to_string_lossy();
            !path.contains("tasks") && !path.contains(".bak") && !path.contains("logs")
        }));
    }

    #[test]
    fn claude_allowlist_collects_transcript_and_session_subtree_only() {
        let temp = tempfile::tempdir().unwrap();
        let project = temp.path().join("projects/-workspace-app");
        let subagents = project.join(NATIVE).join("subagents");
        fs::create_dir_all(&subagents).unwrap();
        fs::write(project.join(format!("{NATIVE}.jsonl")), b"transcript").unwrap();
        fs::write(subagents.join("agent-a.jsonl"), b"subagent").unwrap();
        fs::write(project.join("other-session.jsonl"), b"other").unwrap();
        fs::write(project.join("settings.json"), b"secret config").unwrap();

        let artifacts =
            collect_native_artifacts(HarnessKind::Claude, temp.path(), NATIVE, false).unwrap();
        assert_eq!(artifacts.len(), 2);
        assert!(
            artifacts
                .iter()
                .any(|artifact| artifact.relative_path.ends_with(format!("{NATIVE}.jsonl")))
        );
        assert!(
            artifacts
                .iter()
                .any(|artifact| { artifact.relative_path.ends_with("subagents/agent-a.jsonl") })
        );
    }

    #[test]
    fn claude_project_slug_matches_captured_local_rollout_fixtures() {
        // These cwd/directory pairs come from the local Claude home used to
        // establish the import format. Dots are substituted just like slash.
        assert_eq!(
            claude_project_slug(Path::new("/home/jonathan/Projects/mjolnir/.mjolnir/repro")),
            "-home-jonathan-Projects-mjolnir--mjolnir-repro"
        );
        assert_eq!(
            claude_project_slug(Path::new("/tmp/mj-live-transcript.59w2Hg")),
            "-tmp-mj-live-transcript-59w2Hg"
        );
    }

    #[test]
    fn restore_rewrites_claude_project_artifacts_for_target_workspace() {
        let repositories = vec![crate::hel_archive::RepositoryManifest {
            metadata: crate::hel_archive::RepositoryMetadata {
                id: "app".into(),
                relative_destination: "app".into(),
                origin: "owner/app".into(),
                base_commit: "a".repeat(40),
                head_commit: "a".repeat(40),
                branch: Some("main".into()),
            },
            committed_bundle_path: "repositories/app/committed.bundle".into(),
            staged_patch_path: "repositories/app/staged.patch".into(),
            unstaged_patch_path: "repositories/app/unstaged.patch".into(),
            untracked_tar_path: "repositories/app/untracked.tar".into(),
        }];
        let path = restored_native_relative_path(
            HarnessKind::Claude,
            Path::new("projects/-home-me-app/session.jsonl"),
            "app",
            &repositories,
            Path::new("/workspace"),
        )
        .unwrap();
        assert_eq!(path, PathBuf::from("projects/-workspace-app/session.jsonl"));
    }

    #[test]
    fn restore_rewrites_kimi_workspace_and_state_for_target_workspace() {
        let repositories = vec![crate::hel_archive::RepositoryManifest {
            metadata: crate::hel_archive::RepositoryMetadata {
                id: "app".into(),
                relative_destination: "app".into(),
                origin: "owner/app".into(),
                base_commit: "a".repeat(40),
                head_commit: "a".repeat(40),
                branch: Some("main".into()),
            },
            committed_bundle_path: "repositories/app/committed.bundle".into(),
            staged_patch_path: "repositories/app/staged.patch".into(),
            unstaged_patch_path: "repositories/app/unstaged.patch".into(),
            untracked_tar_path: "repositories/app/untracked.tar".into(),
        }];
        let path = restored_native_relative_path(
            HarnessKind::Kimi,
            Path::new(
                "sessions/wd_kimi-code_78153cfca00c/session_1b6c3192-2480-48e0-8f49-4b8a1572f5b2/state.json",
            ),
            "app",
            &repositories,
            Path::new("/workspace"),
        )
        .unwrap();
        assert_eq!(
            path,
            PathBuf::from(
                "sessions/wd_app_af7e243d70b1/session_1b6c3192-2480-48e0-8f49-4b8a1572f5b2/state.json",
            )
        );
        let state = restored_native_artifact_bytes(
            HarnessKind::Kimi,
            &path,
            br#"{"workDir":"/home/jonathan/Projects/kimi-code","cwd":"/home/jonathan/Projects/kimi-code"}"#,
            "app",
            &repositories,
            Path::new("/workspace"),
            Path::new("/profiles/imported"),
        )
        .unwrap();
        let state: Value = serde_json::from_slice(&state).unwrap();
        assert_eq!(state["workDir"], "/workspace/app");
        assert_eq!(state["cwd"], "/workspace/app");
        let registry = restored_native_artifact_bytes(
            HarnessKind::Kimi,
            Path::new("workspaces.json"),
            br#"{"version":1,"deleted_workspace_ids":[],"workspaces":{"wd_kimi-code_78153cfca00c":{"root":"/home/jonathan/Projects/kimi-code","name":"kimi-code"}}}"#,
            "app",
            &repositories,
            Path::new("/workspace"),
            Path::new("/profiles/imported"),
        )
        .unwrap();
        let registry: Value = serde_json::from_slice(&registry).unwrap();
        assert_eq!(
            registry["workspaces"]["wd_app_af7e243d70b1"]["root"],
            "/workspace/app"
        );
        assert!(
            registry["workspaces"]
                .get("wd_kimi-code_78153cfca00c")
                .is_none()
        );

        let index = restored_native_artifact_bytes(
            HarnessKind::Kimi,
            Path::new("session_index.jsonl"),
            br#"{"sessionId":"session_1b6c3192-2480-48e0-8f49-4b8a1572f5b2","workDir":"/home/jonathan/Projects/kimi-code","sessionDir":"/home/jonathan/.kimi-code/sessions/wd_kimi-code_78153cfca00c/session_1b6c3192-2480-48e0-8f49-4b8a1572f5b2"}"#,
            "app",
            &repositories,
            Path::new("/workspace"),
            Path::new("/profiles/imported"),
        )
        .unwrap();
        let index: Value = serde_json::from_slice(&index).unwrap();
        assert_eq!(index["workDir"], "/workspace/app");
        assert_eq!(
            index["sessionDir"],
            "/profiles/imported/sessions/wd_app_af7e243d70b1/session_1b6c3192-2480-48e0-8f49-4b8a1572f5b2"
        );
    }

    fn git(repository: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .args(args)
            .current_dir(repository)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().into()
    }

    fn fixture(temp: &Path) -> (CheckpointExportSpec, PathBuf) {
        let worker_root = temp.join("worker");
        fs::create_dir_all(&worker_root).unwrap();
        let harness_home = temp.join("codex");
        let native = harness_home.join("sessions/2026/08/09");
        fs::create_dir_all(&native).unwrap();
        fs::write(native.join(format!("rollout-{NATIVE}.jsonl")), b"native").unwrap();
        let workspace = temp.join("workspace");
        let repository = workspace.join("app");
        fs::create_dir_all(&repository).unwrap();
        git(&repository, &["init"]);
        git(&repository, &["config", "user.email", "hel@example.test"]);
        git(&repository, &["config", "user.name", "Hel Test"]);
        fs::write(repository.join("README.md"), b"hello").unwrap();
        git(&repository, &["add", "."]);
        git(&repository, &["commit", "-m", "base"]);
        git(
            &repository,
            &[
                "remote",
                "add",
                "origin",
                "https://github.com/example/app.git",
            ],
        );
        let base = git(&repository, &["rev-parse", "HEAD"]);
        let output = worker_root.join("source.hel.zip");
        (
            CheckpointExportSpec {
                session: SessionManifest {
                    id: SESSION.into(),
                    title: "test".into(),
                    harness_kind: HarnessKind::Codex,
                    profile_id: "codex-1".into(),
                    native_session_id: NATIVE.into(),
                    created_at: "2026-08-09T00:00:00Z".into(),
                    checkpointed_at: "2026-08-09T00:01:00Z".into(),
                    hel_version: "0.1.0".into(),
                    relay_version: "0.1.0".into(),
                    adapter_version: "test".into(),
                },
                target: TargetManifest {
                    template_id: "local".into(),
                    target_kind: "podman".into(),
                    details: Default::default(),
                },
                bundle: BundleManifest {
                    id: "bundle".into(),
                    primary_repository: "app".into(),
                },
                relay_root: worker_root,
                harness_home,
                workspace_root: workspace,
                repositories: vec![CheckpointRepositorySpec {
                    id: "app".into(),
                    relative_destination: "app".into(),
                    capture: CheckpointRepositoryCapture::DeltaFrom { base_commit: base },
                    origin_override: None,
                }],
                canonical_session: CanonicalSessionSnapshot {
                    event_frontier: 1,
                    event_frontier_digest: "a".repeat(64),
                    session: CanonicalSessionState {
                        execution: CanonicalExecutionState::Idle,
                        last_activity_at_ms: Some(1),
                        session_title: Some("test".into()),
                        configuration: Default::default(),
                    },
                    transcript: vec![CanonicalTranscriptItem {
                        stable_id: "user-1".into(),
                        position: 1,
                        latest_content_event_ordinal: None,
                        created_at_ms: 1,
                        last_changed_at_ms: 1,
                        body: CanonicalTranscriptBody::User {
                            content: vec![json!({"type": "text", "text": "hello"})],
                        },
                    }],
                    queued_prompts: vec![CanonicalQueuedPrompt {
                        command_id: "queued-1".into(),
                        content: vec![json!({"type": "text", "text": "next"})],
                        queued_at_ms: 2,
                    }],
                },
                output_path: output.clone(),
            },
            output,
        )
    }

    fn copy_archive_with_schema(source: &Path, destination: &Path, schema_version: u32) {
        let source = File::open(source).unwrap();
        let mut archive = zip::ZipArchive::new(source).unwrap();
        let mut entries = Vec::new();
        for index in 0..archive.len() {
            let mut entry = archive.by_index(index).unwrap();
            let name = entry.name().to_owned();
            let mode = entry.unix_mode().unwrap_or(0o600);
            let mut body = Vec::new();
            entry.read_to_end(&mut body).unwrap();
            if name == "manifest.json" {
                let mut manifest: Value = serde_json::from_slice(&body).unwrap();
                manifest["schema_version"] = json!(schema_version);
                body = serde_json::to_vec_pretty(&manifest).unwrap();
            }
            entries.push((name, mode, body));
        }
        let output = File::create(destination).unwrap();
        let mut writer = zip::ZipWriter::new(output);
        for (name, mode, body) in entries {
            writer
                .start_file(
                    name,
                    zip::write::SimpleFileOptions::default().unix_permissions(mode),
                )
                .unwrap();
            writer.write_all(&body).unwrap();
        }
        writer.finish().unwrap();
    }

    fn copy_archive_with_canonical_session(
        source: &Path,
        destination: &Path,
        canonical_session: &CanonicalSessionSnapshot,
    ) {
        let source = File::open(source).unwrap();
        let mut archive = zip::ZipArchive::new(source).unwrap();
        let mut entries = Vec::new();
        for index in 0..archive.len() {
            let mut entry = archive.by_index(index).unwrap();
            let name = entry.name().to_owned();
            let mode = entry.unix_mode().unwrap_or(0o600);
            let mut body = Vec::new();
            entry.read_to_end(&mut body).unwrap();
            entries.push((name, mode, body));
        }

        let canonical_body = serde_json::to_vec_pretty(canonical_session).unwrap();
        let canonical_sha256 = format!("{:x}", Sha256::digest(&canonical_body));
        entries
            .iter_mut()
            .find(|(name, _, _)| name == "canonical/session.json")
            .unwrap()
            .2 = canonical_body.clone();
        let manifest_body = &mut entries
            .iter_mut()
            .find(|(name, _, _)| name == "manifest.json")
            .unwrap()
            .2;
        let mut manifest: Value = serde_json::from_slice(manifest_body).unwrap();
        let descriptor = manifest["payloads"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|payload| payload["path"] == "canonical/session.json")
            .unwrap();
        descriptor["size"] = json!(canonical_body.len());
        descriptor["sha256"] = json!(canonical_sha256);
        *manifest_body = serde_json::to_vec_pretty(&manifest).unwrap();

        let output = File::create(destination).unwrap();
        let mut writer = zip::ZipWriter::new(output);
        for (name, mode, body) in entries {
            writer
                .start_file(
                    name,
                    zip::write::SimpleFileOptions::default().unix_permissions(mode),
                )
                .unwrap();
            writer.write_all(&body).unwrap();
        }
        writer.finish().unwrap();
    }

    fn assert_canonical_restore_rejected_before_mutation(
        spec: &CheckpointExportSpec,
        invalid_archive: &Path,
        canonical_session: &CanonicalSessionSnapshot,
        expected_error: &str,
    ) {
        copy_archive_with_canonical_session(&spec.output_path, invalid_archive, canonical_session);
        let readme = spec.workspace_root.join("app/README.md");
        let before = fs::read(&readme).unwrap();
        let relay_root = invalid_archive.with_extension("relay");
        let harness_home = invalid_archive.with_extension("harness");

        let error = restore_checkpoint(
            &CheckpointRestoreSpec {
                archive_path: invalid_archive.to_path_buf(),
                workspace_root: spec.workspace_root.clone(),
                relay_root: relay_root.clone(),
                harness_home: harness_home.clone(),
                restore_repositories: true,
                restore_native: true,
                discard_queued_prompts: false,
            },
            &SystemGit,
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains(expected_error), "{error:#}");
        assert_eq!(fs::read(&readme).unwrap(), before);
        assert!(!relay_root.exists());
        assert!(!harness_home.exists());
    }

    struct CopyExecutor {
        source: PathBuf,
        calls: RefCell<usize>,
    }
    impl CommandExecutor for CopyExecutor {
        fn execute(&self, command: &CommandSpec) -> Result<CommandOutput> {
            *self.calls.borrow_mut() += 1;
            fs::copy(
                &self.source,
                command.args.last().context("missing destination")?,
            )?;
            Ok(CommandOutput {
                status: 0,
                stdout: vec![],
                stderr: vec![],
            })
        }
    }

    #[test]
    fn export_and_transfer_only_gate_after_local_verification() {
        let temp = tempfile::tempdir().unwrap();
        let (spec, source) = fixture(temp.path());
        let target = export_checkpoint(&spec).unwrap();
        assert_eq!(target.event_frontier, 1);
        assert_eq!(
            target.event_frontier_digest,
            spec.canonical_session.event_frontier_digest
        );
        let destination = temp.path().join("controller/session.hel.zip");
        let locator = &locators()[0];
        let gate = CheckpointTransfer {
            locator,
            session_id: SESSION,
            remote_archive: "/var/lib/hel/workers/source.hel.zip",
            destination: &destination,
            expected_event_frontier: Some(1),
            expected_event_frontier_digest: Some(&spec.canonical_session.event_frontier_digest),
        }
        .execute(&CopyExecutor {
            source,
            calls: RefCell::new(0),
        })
        .unwrap();
        assert!(gate.teardown_allowed());
        assert_eq!(gate.event_frontier(), 1);
        assert_eq!(
            gate.event_frontier_digest(),
            spec.canonical_session.event_frontier_digest
        );
        assert_eq!(
            read_archive_verified(&destination).unwrap().archive_sha256,
            gate.sha256()
        );
    }

    /// The controller streams the export spec to save a round trip to the
    /// target. Both spellings have to produce the same archive.
    #[test]
    fn a_streamed_spec_exports_the_same_archive_as_a_spec_file() {
        let temp = tempfile::tempdir().unwrap();
        let (mut spec, _) = fixture(temp.path());
        let from_file = export_from_spec_file(&spec.output_path.with_extension("spec.json"))
            .err()
            .map(|error| format!("{error:#}"));
        assert!(
            from_file.is_some_and(|error| error.contains("read checkpoint export spec")),
            "a missing spec file must still be reported as a read failure"
        );

        let spec_path = temp.path().join("checkpoint-spec.json");
        spec.write(&spec_path).unwrap();
        let from_file = export_from_spec_file(&spec_path).unwrap();
        let file_archive = fs::read(&spec.output_path).unwrap();

        spec.output_path = temp.path().join("worker/streamed.hel.zip");
        let body = serde_json::to_vec(&spec).unwrap();
        let streamed = export_from_spec_reader(&mut body.as_slice()).unwrap();
        let streamed_archive = fs::read(&spec.output_path).unwrap();

        assert_eq!(streamed.sha256, from_file.sha256);
        assert_eq!(streamed.event_frontier, from_file.event_frontier);
        assert_eq!(
            streamed.event_frontier_digest,
            from_file.event_frontier_digest
        );
        assert_eq!(streamed_archive, file_archive);
        assert_eq!(
            read_archive_verified(&spec.output_path)
                .unwrap()
                .archive_sha256,
            streamed.sha256
        );
    }

    #[test]
    fn transfer_rejects_a_same_ordinal_frontier_digest_mismatch() {
        let temp = tempfile::tempdir().unwrap();
        let (spec, source) = fixture(temp.path());
        export_checkpoint(&spec).unwrap();
        let destination = temp.path().join("controller/session.hel.zip");
        let unexpected_digest = "b".repeat(64);

        let error = CheckpointTransfer {
            locator: &locators()[0],
            session_id: SESSION,
            remote_archive: "/var/lib/hel/workers/source.hel.zip",
            destination: &destination,
            expected_event_frontier: Some(1),
            expected_event_frontier_digest: Some(&unexpected_digest),
        }
        .execute(&CopyExecutor {
            source,
            calls: RefCell::new(0),
        })
        .unwrap_err();

        assert!(format!("{error:#}").contains("event frontier digest mismatch"));
        assert!(!destination.exists());
    }

    #[test]
    fn raw_project_export_keeps_git_metadata_without_git_contents() {
        let temp = tempfile::tempdir().unwrap();
        let (mut spec, _) = fixture(temp.path());
        spec.repositories[0].capture = CheckpointRepositoryCapture::MetadataOnly;
        let repository = spec.workspace_root.join("app");
        fs::write(repository.join("README.md"), b"dirty").unwrap();
        fs::write(repository.join("untracked.txt"), b"untracked").unwrap();

        export_checkpoint(&spec).unwrap();
        let archive = read_archive_verified(&spec.output_path).unwrap();
        let repository = &archive.manifest.repositories[0];
        assert_eq!(
            repository.metadata.base_commit,
            repository.metadata.head_commit
        );
        for role in [
            PayloadRole::GitBundle {
                repository_id: "app".into(),
            },
            PayloadRole::GitStagedPatch {
                repository_id: "app".into(),
            },
            PayloadRole::GitUnstagedPatch {
                repository_id: "app".into(),
            },
            PayloadRole::GitUntrackedTar {
                repository_id: "app".into(),
            },
        ] {
            assert!(archive.payload_by_role(&role).unwrap().is_empty());
        }
    }

    #[test]
    fn session_delta_without_origin_refs_repairs_once_then_fails() {
        let temp = tempfile::tempdir().unwrap();
        let (mut spec, _) = fixture(temp.path());
        spec.repositories[0].capture = CheckpointRepositoryCapture::SessionDelta;
        let git = RecordingGit::with_fetch_failure();

        let error = export_checkpoint_with_git(&spec, &git).unwrap_err();

        assert_eq!(git.fetches(), 1);
        let error = format!("{error:#}");
        assert!(
            error.contains("repository 'app' has no origin refs"),
            "{error}"
        );
        assert!(error.contains("refusing to bundle full history"), "{error}");
        assert!(error.contains("repair fetch failed"), "{error}");
    }

    #[test]
    fn invalid_canonical_session_is_rejected_before_repository_repair() {
        let temp = tempfile::tempdir().unwrap();
        let (mut spec, _) = fixture(temp.path());
        spec.repositories[0].capture = CheckpointRepositoryCapture::SessionDelta;
        spec.canonical_session.session.execution =
            CanonicalExecutionState::Running { started_at_ms: 3 };
        let git = RecordingGit::with_fetch_failure();

        let error = export_checkpoint_with_git(&spec, &git).unwrap_err();

        assert!(format!("{error:#}").contains("not idle at the checkpoint barrier"));
        assert_eq!(git.fetches(), 0);
        assert!(!spec.output_path.exists());
    }

    #[test]
    fn session_delta_export_succeeds_when_the_repair_fetch_restores_origin_refs() {
        let temp = tempfile::tempdir().unwrap();
        let (mut spec, _) = fixture(temp.path());
        spec.repositories[0].capture = CheckpointRepositoryCapture::SessionDelta;
        let repository = spec.workspace_root.join("app");
        let origin = temp.path().join("origin.git");
        git(
            &spec.workspace_root,
            &["clone", "-q", "--bare", "app", origin.to_str().unwrap()],
        );
        git(
            &repository,
            &["remote", "set-url", "origin", origin.to_str().unwrap()],
        );
        fs::write(repository.join("later.txt"), b"later").unwrap();
        git(&repository, &["add", "."]);
        git(&repository, &["commit", "-qm", "later"]);
        let git_runner = RecordingGit::forwarding();

        export_checkpoint_with_git(&spec, &git_runner).unwrap();

        assert_eq!(git_runner.fetches(), 1);
        let archive = read_archive_verified(&spec.output_path).unwrap();
        assert_eq!(archive.manifest.repositories[0].metadata.base_commit, "");
        assert!(
            !archive
                .payload_by_role(&PayloadRole::GitBundle {
                    repository_id: "app".into(),
                })
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn raw_project_without_git_metadata_fails_checkpoint_export() {
        let temp = tempfile::tempdir().unwrap();
        let (mut spec, _) = fixture(temp.path());
        spec.repositories[0].capture = CheckpointRepositoryCapture::MetadataOnly;
        fs::remove_dir_all(spec.workspace_root.join("app/.git")).unwrap();

        let error = export_checkpoint(&spec).unwrap_err();

        assert!(format!("{error:#}").contains("repository has no valid Git HEAD"));
    }

    #[test]
    fn checkpoint_wire_requires_the_new_capture_mode_and_rejects_legacy_fields() {
        let legacy = serde_json::from_value::<CheckpointRepositorySpec>(json!({
            "id": "app",
            "relative_destination": "app",
            "base_commit": "HEAD"
        }));
        assert!(legacy.is_err());

        let repository: CheckpointRepositorySpec = serde_json::from_value(json!({
            "id": "app",
            "relative_destination": "app",
            "capture": { "mode": "delta_from", "base_commit": "HEAD" },
            "origin_override": null
        }))
        .unwrap();
        assert_eq!(
            repository.capture,
            CheckpointRepositoryCapture::DeltaFrom {
                base_commit: "HEAD".into()
            }
        );

        let mixed = serde_json::from_value::<CheckpointRepositorySpec>(json!({
            "id": "app",
            "relative_destination": "app",
            "capture": { "mode": "session_delta" },
            "origin_override": null,
            "session_delta": true
        }));
        assert!(mixed.is_err());

        // The compatibility reset does not reinterpret the old event frontier.
        let target = serde_json::from_value::<TargetCheckpoint>(json!({
            "path": "/worker/checkpoint.hel.zip",
            "sha256": "abc",
            "event_sequence": 7,
            "full_history_fallbacks": ["app"]
        }));
        assert!(target.is_err());

        let restore = json!({
            "archive_path": "/relay/checkpoint.hel.zip",
            "workspace_root": "/workspace",
            "relay_root": "/relay",
            "harness_home": "/harness",
            "restore_repositories": true,
            "restore_native": true,
            "discard_queued_prompts": false
        });
        assert!(serde_json::from_value::<CheckpointRestoreSpec>(restore.clone()).is_ok());
        let mut missing_flag = restore.clone();
        missing_flag
            .as_object_mut()
            .unwrap()
            .remove("discard_queued_prompts");
        assert!(serde_json::from_value::<CheckpointRestoreSpec>(missing_flag).is_err());
        let mut retired_root = restore;
        retired_root
            .as_object_mut()
            .unwrap()
            .insert("worker_root".into(), json!("/legacy"));
        assert!(serde_json::from_value::<CheckpointRestoreSpec>(retired_root).is_err());

        let repository_restore = serde_json::from_value::<RepositoryRestoreSpec>(json!({
            "archive_path": "/relay/checkpoint.hel.zip",
            "workspace_root": "/workspace",
            "legacy": true
        }));
        assert!(repository_restore.is_err());
    }

    #[test]
    fn checkpoint_export_wire_uses_relay_root_and_rejects_retired_worker_root() {
        let temp = tempfile::tempdir().unwrap();
        let (spec, _) = fixture(temp.path());
        let mut value = serde_json::to_value(&spec).unwrap();
        assert!(value.get("relay_root").is_some());
        assert!(value.get("worker_root").is_none());

        value
            .as_object_mut()
            .unwrap()
            .insert("worker_root".into(), json!("/legacy"));
        assert!(serde_json::from_value::<CheckpointExportSpec>(value).is_err());
    }

    #[test]
    fn checkpoint_round_trips_the_latched_materialized_session() {
        let temp = tempfile::tempdir().unwrap();
        let (mut spec, _) = fixture(temp.path());
        spec.canonical_session.event_frontier = 9;
        spec.canonical_session.transcript[0].position = 7;

        let target = export_checkpoint(&spec).unwrap();
        assert_eq!(target.event_frontier, 9);
        assert_eq!(
            read_checkpoint_session(&spec.output_path).unwrap(),
            spec.canonical_session
        );
    }

    #[test]
    fn restore_seeds_materialized_session_and_can_discard_queued_prompts() {
        let temp = tempfile::tempdir().unwrap();
        let (spec, _) = fixture(temp.path());
        export_checkpoint(&spec).unwrap();
        let relay_root = temp.path().join("restored-relay");
        restore_checkpoint(
            &CheckpointRestoreSpec {
                archive_path: spec.output_path.clone(),
                workspace_root: spec.workspace_root.clone(),
                relay_root: relay_root.clone(),
                harness_home: temp.path().join("restored-harness"),
                restore_repositories: false,
                restore_native: false,
                discard_queued_prompts: true,
            },
            &SystemGit,
        )
        .unwrap();

        let restored: CanonicalSessionSnapshot = serde_json::from_slice(
            &fs::read(restored_canonical_session_path(&relay_root)).unwrap(),
        )
        .unwrap();
        assert_eq!(
            restored.event_frontier,
            spec.canonical_session.event_frontier
        );
        assert_eq!(restored.transcript, spec.canonical_session.transcript);
        assert!(restored.queued_prompts.is_empty());
        assert!(!relay_root.join("events.jsonl").exists());
    }

    #[test]
    fn incompatible_schema_is_rejected_before_restore_mutates_target() {
        let temp = tempfile::tempdir().unwrap();
        let (spec, _) = fixture(temp.path());
        export_checkpoint(&spec).unwrap();
        let readme = spec.workspace_root.join("app/README.md");
        let before = fs::read(&readme).unwrap();

        for schema_version in [1, crate::hel_archive::ARCHIVE_SCHEMA_VERSION + 1] {
            let incompatible = temp
                .path()
                .join(format!("incompatible-{schema_version}.hel.zip"));
            copy_archive_with_schema(&spec.output_path, &incompatible, schema_version);
            let relay_root = temp.path().join(format!("relay-{schema_version}"));
            let error = restore_checkpoint(
                &CheckpointRestoreSpec {
                    archive_path: incompatible,
                    workspace_root: spec.workspace_root.clone(),
                    relay_root: relay_root.clone(),
                    harness_home: temp.path().join("restored-harness"),
                    restore_repositories: true,
                    restore_native: true,
                    discard_queued_prompts: false,
                },
                &SystemGit,
            )
            .unwrap_err();

            assert!(
                format!("{error:#}").contains("incompatible Hel archive schema"),
                "{error:#}"
            );
            assert_eq!(fs::read(&readme).unwrap(), before);
            assert!(!relay_root.exists());
        }
    }

    #[test]
    fn non_idle_canonical_session_is_rejected_before_restore_mutates_target() {
        let temp = tempfile::tempdir().unwrap();
        let (spec, _) = fixture(temp.path());
        export_checkpoint(&spec).unwrap();
        let mut invalid = spec.canonical_session.clone();
        invalid.session.execution = CanonicalExecutionState::Running { started_at_ms: 3 };

        assert_canonical_restore_rejected_before_mutation(
            &spec,
            &temp.path().join("non-idle.hel.zip"),
            &invalid,
            "not idle at the checkpoint barrier",
        );
    }

    #[test]
    fn invalid_frontier_digest_is_rejected_before_restore_mutates_target() {
        let temp = tempfile::tempdir().unwrap();
        let (spec, _) = fixture(temp.path());
        export_checkpoint(&spec).unwrap();
        let mut invalid = spec.canonical_session.clone();
        invalid.event_frontier_digest = "A".repeat(64);

        assert_canonical_restore_rejected_before_mutation(
            &spec,
            &temp.path().join("invalid-digest.hel.zip"),
            &invalid,
            "64 lowercase hexadecimal characters",
        );
    }

    #[test]
    fn unrestorable_queue_is_rejected_before_restore_mutates_target() {
        let temp = tempfile::tempdir().unwrap();
        let (spec, _) = fixture(temp.path());
        export_checkpoint(&spec).unwrap();

        let mut empty = spec.canonical_session.clone();
        empty.queued_prompts[0].content.clear();
        assert_canonical_restore_rejected_before_mutation(
            &spec,
            &temp.path().join("empty-prompt.hel.zip"),
            &empty,
            "has no content",
        );

        let mut malformed = spec.canonical_session.clone();
        malformed.queued_prompts[0].content = vec![json!({"type": "not_an_acp_block"})];
        assert_canonical_restore_rejected_before_mutation(
            &spec,
            &temp.path().join("malformed-prompt.hel.zip"),
            &malformed,
            "has invalid ACP content block 0",
        );
    }

    #[test]
    fn parallel_repository_collection_preserves_manifest_order() {
        let temp = tempfile::tempdir().unwrap();
        let (mut spec, _) = fixture(temp.path());
        git(&spec.workspace_root, &["clone", "-q", "app", "worker"]);
        let worker = spec.workspace_root.join("worker");
        let base = git(&worker, &["rev-parse", "HEAD"]);
        spec.repositories.push(CheckpointRepositorySpec {
            id: "worker".into(),
            relative_destination: "worker".into(),
            capture: CheckpointRepositoryCapture::DeltaFrom { base_commit: base },
            origin_override: None,
        });

        export_checkpoint(&spec).unwrap();
        let verified = read_archive_verified(&spec.output_path).unwrap();
        let repository_ids = verified
            .manifest
            .repositories
            .iter()
            .map(|repository| repository.metadata.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(repository_ids, ["app", "worker"]);
    }

    #[test]
    fn corrupt_transfer_preserves_previous_checkpoint() {
        let temp = tempfile::tempdir().unwrap();
        let corrupt = temp.path().join("bad.zip");
        fs::write(&corrupt, b"bad").unwrap();
        let destination = temp.path().join("session.hel.zip");
        fs::write(&destination, b"previous").unwrap();
        let locator = &locators()[0];
        let result = CheckpointTransfer {
            locator,
            session_id: SESSION,
            remote_archive: "/var/lib/hel/workers/source.hel.zip",
            destination: &destination,
            expected_event_frontier: None,
            expected_event_frontier_digest: None,
        }
        .execute(&CopyExecutor {
            source: corrupt,
            calls: RefCell::new(0),
        });
        assert!(result.is_err());
        assert_eq!(fs::read(destination).unwrap(), b"previous");
    }
}
