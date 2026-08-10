//! Target-side checkpoint collection and controller-side verified transfer.
//!
//! Targets own the Git worktrees and native harness history, so they build the
//! archive. The controller downloads into a same-directory temporary file and
//! only returns a teardown gate after reopening and verifying the installed
//! archive.

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

use crate::hel_archive::{
    ArchiveInput, BundleManifest, GitCollectionSpec, GitCommand, GitCommandRunner, NativeArtifact,
    PayloadRole, RepositorySnapshot, SessionManifest, SystemGit, TargetManifest,
    collect_git_snapshot, read_archive_verified, restore_git_snapshot, write_archive_atomic,
};
use crate::hel_config::HarnessKind;
use crate::hel_targets::{
    CommandExecutor, CommandPlan, CommandSpec, SshTarget, TargetLocator, join_remote_command,
    worker_root,
};
use crate::hel_worker::SequencedEvent;

const MAX_NATIVE_FILE: u64 = 1024 * 1024 * 1024;
const MAX_NATIVE_TOTAL: u64 = 8 * 1024 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointRepositorySpec {
    pub id: String,
    pub relative_destination: PathBuf,
    pub base_commit: String,
}

/// Uploaded target-side input. It contains provenance and paths, never secrets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointExportSpec {
    pub session: SessionManifest,
    pub target: TargetManifest,
    pub bundle: BundleManifest,
    pub worker_root: PathBuf,
    pub harness_home: PathBuf,
    pub workspace_root: PathBuf,
    pub repositories: Vec<CheckpointRepositorySpec>,
    pub output_path: PathBuf,
}

impl CheckpointExportSpec {
    pub fn read(path: &Path) -> Result<Self> {
        let body = fs::read(path)
            .with_context(|| format!("read checkpoint export spec {}", path.display()))?;
        serde_json::from_slice(&body)
            .with_context(|| format!("parse checkpoint export spec {}", path.display()))
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
pub struct TargetCheckpoint {
    pub path: PathBuf,
    pub sha256: String,
    pub event_sequence: u64,
}

/// Hidden target CLI entry point: `hel worker export-checkpoint --spec PATH`.
pub fn export_from_spec_file(path: &Path) -> Result<TargetCheckpoint> {
    export_checkpoint(&CheckpointExportSpec::read(path)?)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointRestoreSpec {
    pub archive_path: PathBuf,
    pub workspace_root: PathBuf,
    pub worker_root: PathBuf,
    pub harness_home: PathBuf,
    pub restore_native: bool,
}

pub fn restore_from_spec_file(path: &Path) -> Result<()> {
    let body = fs::read(path)
        .with_context(|| format!("read checkpoint restore spec {}", path.display()))?;
    let mut spec: CheckpointRestoreSpec = serde_json::from_slice(&body)
        .with_context(|| format!("parse checkpoint restore spec {}", path.display()))?;
    spec.archive_path = resolve_target_path(&spec.archive_path)?;
    spec.workspace_root = resolve_target_path(&spec.workspace_root)?;
    spec.worker_root = resolve_target_path(&spec.worker_root)?;
    spec.harness_home = resolve_target_path(&spec.harness_home)?;
    restore_checkpoint(&spec, &SystemGit)
}

pub fn restore_checkpoint(spec: &CheckpointRestoreSpec, git: &dyn GitCommandRunner) -> Result<()> {
    ensure!(spec.workspace_root.is_dir(), "restore workspace is missing");
    let archive = read_archive_verified(&spec.archive_path)?;
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
        let path = spec
            .workspace_root
            .join(&repository.metadata.relative_destination);
        restore_git_snapshot(git, &path, &snapshot)
            .with_context(|| format!("restore repository {id:?}"))?;
    }

    fs::create_dir_all(&spec.worker_root)?;
    let events = archive.payload_by_role(&PayloadRole::CanonicalEvents)?;
    let _ = verify_canonical_payload(events)?;
    write_private_file(&spec.worker_root.join("events.jsonl"), events, 0o600)?;

    if spec.restore_native {
        for descriptor in &archive.manifest.payloads {
            let PayloadRole::NativeArtifact { relative_path } = &descriptor.role else {
                continue;
            };
            validate_relative_path(relative_path)?;
            ensure!(
                !secret_like_path(relative_path),
                "native artifact path is secret-like"
            );
            let destination = spec.harness_home.join(relative_path);
            write_private_file(&destination, archive.payload(descriptor)?, descriptor.mode)?;
        }
    }
    Ok(())
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
    resolved.worker_root = resolve_target_path(&resolved.worker_root)?;
    resolved.harness_home = resolve_target_path(&resolved.harness_home)?;
    resolved.workspace_root = resolve_target_path(&resolved.workspace_root)?;
    resolved.output_path = resolve_target_path(&resolved.output_path)?;
    let spec = &resolved;
    validate_export_spec(spec)?;
    let (canonical_events, event_sequence) = collect_canonical_events(&spec.worker_root)?;
    let native_artifacts = collect_native_artifacts(
        spec.session.harness_kind,
        &spec.harness_home,
        &spec.session.native_session_id,
    )?;
    let mut repositories = Vec::with_capacity(spec.repositories.len());
    for repository in &spec.repositories {
        let path = spec.workspace_root.join(&repository.relative_destination);
        ensure!(path.is_dir(), "repository {} is missing", path.display());
        reject_dirty_submodules(git, &path)
            .with_context(|| format!("repository '{}'", repository.id))?;
        repositories.push(collect_git_snapshot(
            git,
            &path,
            &GitCollectionSpec {
                id: repository.id.clone(),
                relative_destination: repository.relative_destination.clone(),
                base_commit: repository.base_commit.clone(),
            },
        )?);
    }
    let verified = write_archive_atomic(
        &spec.output_path,
        &ArchiveInput {
            session: spec.session.clone(),
            target: spec.target.clone(),
            bundle: spec.bundle.clone(),
            canonical_events,
            native_artifacts,
            repositories,
        },
    )?;
    ensure!(verified.manifest.session.id == spec.session.id);
    Ok(TargetCheckpoint {
        path: spec.output_path.clone(),
        sha256: verified.archive_sha256,
        event_sequence,
    })
}

fn validate_export_spec(spec: &CheckpointExportSpec) -> Result<()> {
    validate_component(&spec.session.id, "session ID")?;
    validate_component(&spec.session.native_session_id, "native session ID")?;
    ensure!(spec.worker_root.is_dir(), "worker root is missing");
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
        ensure!(
            !repository.base_commit.trim().is_empty(),
            "base commit is empty"
        );
        ensure!(ids.insert(&repository.id), "duplicate repository ID");
        ensure!(
            destinations.insert(&repository.relative_destination),
            "duplicate destination"
        );
    }
    let parent = spec.output_path.parent().unwrap_or_else(|| Path::new("."));
    ensure!(
        parent.starts_with(&spec.worker_root),
        "archive must be beneath worker root"
    );
    Ok(())
}

fn collect_canonical_events(worker_root: &Path) -> Result<(Vec<u8>, u64)> {
    let path = worker_root.join("events.jsonl");
    let bytes = fs::read(&path).with_context(|| format!("read {}", path.display()))?;
    let sequence = verify_canonical_payload(&bytes)?;
    Ok((bytes, sequence))
}

pub fn collect_native_artifacts(
    harness: HarnessKind,
    home: &Path,
    session_id: &str,
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
    output.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    ensure!(
        !output.is_empty(),
        "no allowlisted native session artifacts found"
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
    let inside = inside_session || path.file_name().is_some_and(|name| name == session_id);
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
    let selected = match harness {
        HarnessKind::Codex => {
            name.contains(session_id) && (name.ends_with(".jsonl") || name.ends_with(".jsonl.zst"))
        }
        HarnessKind::Claude => inside || name == format!("{session_id}.jsonl"),
        HarnessKind::Kimi => inside,
    };
    let relative = path.strip_prefix(home)?;
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
    pub expected_event_sequence: Option<u64>,
}

/// Unforgeable outside this module: proof that a controller-local archive was
/// verified after its atomic install.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedCheckpoint {
    session_id: String,
    archive_path: PathBuf,
    sha256: String,
    event_sequence: u64,
}

impl VerifiedCheckpoint {
    pub fn archive_path(&self) -> &Path {
        &self.archive_path
    }
    pub fn sha256(&self) -> &str {
        &self.sha256
    }
    pub fn event_sequence(&self) -> u64 {
        self.event_sequence
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
        let verified = read_archive_verified(&path).context("verify downloaded checkpoint")?;
        ensure!(
            verified.manifest.session.id == self.session_id,
            "checkpoint session mismatch"
        );
        let sequence =
            verify_canonical_payload(verified.payload_by_role(&PayloadRole::CanonicalEvents)?)?;
        if let Some(expected) = self.expected_event_sequence {
            ensure!(sequence == expected, "checkpoint event sequence mismatch");
        }
        let sha256 = verified.archive_sha256;
        temporary
            .persist(self.destination)
            .map_err(|error| error.error)?;
        restrict_permissions(self.destination)?;
        sync_directory(parent)?;
        let installed = read_archive_verified(self.destination)?;
        ensure!(
            installed.archive_sha256 == sha256,
            "installed archive checksum changed"
        );
        Ok(VerifiedCheckpoint {
            session_id: self.session_id.to_owned(),
            archive_path: self.destination.to_path_buf(),
            sha256,
            event_sequence: sequence,
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

fn verify_canonical_payload(bytes: &[u8]) -> Result<u64> {
    ensure!(
        bytes.is_empty() || bytes.ends_with(b"\n"),
        "incomplete canonical event frame"
    );
    let mut expected = 1_u64;
    for line in bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let event: SequencedEvent = serde_json::from_slice(line)?;
        ensure!(event.seq == expected, "canonical event sequence gap");
        expected += 1;
    }
    Ok(expected - 1)
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
    Ok(())
}

fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::process::Command;

    use crate::hel_targets::CommandOutput;
    use crate::hel_worker::DurableWorker;

    const SESSION: &str = "018f9dd2-a3b4-7c8d-9000-123456789abc";
    const NATIVE: &str = "0190aabb-ccdd-7eef-9000-abcdef012345";

    fn ssh() -> SshTarget {
        SshTarget {
            destination: "dev@example.test".into(),
            ssh_args: vec!["-p".into(), "2222".into()],
        }
    }

    fn locators() -> Vec<TargetLocator> {
        let name = crate::hel_targets::resource_name(SESSION).unwrap();
        vec![
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
        assert_eq!(plans[0].commands[0].program, "podman");
        assert_eq!(plans[1].commands[0].program, "container");
        assert_eq!(plans[2].commands[0].program, "scp");
        assert_eq!(plans[3].commands[0].program, "scp");
        assert_eq!(plans[4].commands.len(), 3);
        assert!(
            plans[4].commands[1]
                .args
                .last()
                .unwrap()
                .contains("'podman' 'cp'")
        );
        assert!(
            !plans[4]
                .commands
                .iter()
                .flat_map(|command| &command.args)
                .any(|arg| arg == "--remote")
        );
        assert!(plans[2].commands[0].args.contains(&"-P".into()));
    }

    #[test]
    fn native_allowlist_excludes_credentials_and_other_sessions() {
        let temp = tempfile::tempdir().unwrap();
        let session = temp.path().join("sessions/workspace").join(NATIVE);
        fs::create_dir_all(session.join("agents/main")).unwrap();
        fs::write(session.join("state.json"), b"state").unwrap();
        fs::write(session.join("agents/main/wire.jsonl"), b"events").unwrap();
        fs::write(session.join("credentials.json"), b"secret").unwrap();
        let other = temp.path().join("sessions/workspace/other");
        fs::create_dir_all(&other).unwrap();
        fs::write(other.join("state.json"), b"other").unwrap();
        let artifacts = collect_native_artifacts(HarnessKind::Kimi, temp.path(), NATIVE).unwrap();
        assert_eq!(artifacts.len(), 2);
        assert!(
            artifacts
                .iter()
                .all(|artifact| artifact.relative_path.to_string_lossy().contains(NATIVE))
        );
        assert!(artifacts.iter().all(|artifact| {
            !artifact
                .relative_path
                .to_string_lossy()
                .contains("credentials")
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

        let artifacts = collect_native_artifacts(HarnessKind::Claude, temp.path(), NATIVE).unwrap();
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
        let mut worker = DurableWorker::open(&worker_root, SESSION, "0.1.0").unwrap();
        worker
            .record_adapter_event("notice", serde_json::json!({}))
            .unwrap();
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
                    worker_version: "0.1.0".into(),
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
                worker_root,
                harness_home,
                workspace_root: workspace,
                repositories: vec![CheckpointRepositorySpec {
                    id: "app".into(),
                    relative_destination: "app".into(),
                    base_commit: base,
                }],
                output_path: output.clone(),
            },
            output,
        )
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
        assert_eq!(target.event_sequence, 1);
        let destination = temp.path().join("controller/session.hel.zip");
        let locator = &locators()[0];
        let gate = CheckpointTransfer {
            locator,
            session_id: SESSION,
            remote_archive: "/var/lib/hel/workers/source.hel.zip",
            destination: &destination,
            expected_event_sequence: Some(1),
        }
        .execute(&CopyExecutor {
            source,
            calls: RefCell::new(0),
        })
        .unwrap();
        assert!(gate.teardown_allowed());
        assert_eq!(gate.event_sequence(), 1);
        assert_eq!(
            read_archive_verified(&destination).unwrap().archive_sha256,
            gate.sha256()
        );
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
            expected_event_sequence: None,
        }
        .execute(&CopyExecutor {
            source: corrupt,
            calls: RefCell::new(0),
        });
        assert!(result.is_err());
        assert_eq!(fs::read(destination).unwrap(), b"previous");
    }
}
