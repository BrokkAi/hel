//! Versioned, verified checkpoint archives for Hel sessions.
//!
//! This module deliberately accepts native harness artifacts one file at a
//! time.  Harness adapters must use a versioned allowlist; recursively copying
//! a profile home would risk archiving credentials and configuration.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context, Result, anyhow, bail, ensure};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zip::write::SimpleFileOptions;

use crate::hel_config::HarnessKind;

pub const ARCHIVE_SCHEMA_VERSION: u32 = 1;
pub const ARCHIVE_FORMAT: &str = "hel-session";

const MANIFEST_PATH: &str = "manifest.json";
const CANONICAL_EVENTS_PATH: &str = "canonical/events.jsonl";
const MAX_MANIFEST_BYTES: u64 = 8 * 1024 * 1024;
const MAX_PAYLOAD_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const MAX_ARCHIVE_BYTES: u64 = 32 * 1024 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionManifest {
    pub id: String,
    pub title: String,
    pub harness_kind: HarnessKind,
    pub profile_id: String,
    pub native_session_id: String,
    pub created_at: String,
    pub checkpointed_at: String,
    pub hel_version: String,
    pub worker_version: String,
    pub adapter_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleManifest {
    pub id: String,
    pub primary_repository: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetManifest {
    pub template_id: String,
    pub target_kind: String,
    /// Informational provenance only. It must not contain credentials.
    #[serde(default)]
    pub details: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryMetadata {
    pub id: String,
    pub relative_destination: PathBuf,
    pub origin: String,
    pub base_commit: String,
    /// The committed bundle includes the base commit and is sufficient to
    /// populate an empty repository.
    #[serde(default)]
    pub full_history: bool,
    pub head_commit: String,
    pub branch: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryManifest {
    #[serde(flatten)]
    pub metadata: RepositoryMetadata,
    pub committed_bundle_path: String,
    pub staged_patch_path: String,
    pub unstaged_patch_path: String,
    pub untracked_tar_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PayloadRole {
    CanonicalEvents,
    NativeArtifact { relative_path: PathBuf },
    GitBundle { repository_id: String },
    GitStagedPatch { repository_id: String },
    GitUnstagedPatch { repository_id: String },
    GitUntrackedTar { repository_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadDescriptor {
    pub path: String,
    pub sha256: String,
    pub size: u64,
    pub mode: u32,
    pub role: PayloadRole,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveManifest {
    pub schema_version: u32,
    pub format: String,
    pub session: SessionManifest,
    pub target: TargetManifest,
    pub bundle: BundleManifest,
    pub repositories: Vec<RepositoryManifest>,
    pub payloads: Vec<PayloadDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeArtifact {
    pub relative_path: PathBuf,
    pub data: Vec<u8>,
    pub mode: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositorySnapshot {
    pub metadata: RepositoryMetadata,
    pub committed_bundle: Vec<u8>,
    pub staged_patch: Vec<u8>,
    pub unstaged_patch: Vec<u8>,
    pub untracked_tar: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveInput {
    pub session: SessionManifest,
    pub target: TargetManifest,
    pub bundle: BundleManifest,
    pub canonical_events: Vec<u8>,
    pub native_artifacts: Vec<NativeArtifact>,
    pub repositories: Vec<RepositorySnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedArchive {
    pub manifest: ArchiveManifest,
    /// Payload bytes keyed by the exact manifest path.
    pub payloads: BTreeMap<String, Vec<u8>>,
    pub archive_sha256: String,
}

impl VerifiedArchive {
    pub fn payload(&self, descriptor: &PayloadDescriptor) -> Result<&[u8]> {
        self.payloads
            .get(&descriptor.path)
            .map(Vec::as_slice)
            .ok_or_else(|| anyhow!("verified payload '{}' is missing", descriptor.path))
    }

    pub fn payload_by_role(&self, role: &PayloadRole) -> Result<&[u8]> {
        let descriptor = self
            .manifest
            .payloads
            .iter()
            .find(|descriptor| &descriptor.role == role)
            .ok_or_else(|| anyhow!("archive does not contain payload role {role:?}"))?;
        self.payload(descriptor)
    }
}

#[derive(Debug)]
pub enum CloseVerification {
    Verified {
        archive_path: PathBuf,
        archive_sha256: String,
    },
    /// The target must remain live and retryable while this result is blocked.
    Blocked { error: anyhow::Error },
}

impl CloseVerification {
    pub fn teardown_allowed(&self) -> bool {
        matches!(self, Self::Verified { .. })
    }
}

#[derive(Debug)]
struct PendingPayload {
    descriptor: PayloadDescriptor,
    data: Vec<u8>,
}

/// Writes, fsyncs, atomically replaces, reopens, and verifies an archive.
///
/// Success means it is safe for the caller's close state machine to tear down
/// the target. Failure leaves an existing destination untouched whenever the
/// failure occurs before the final same-directory rename.
pub fn write_archive_atomic(path: &Path, input: &ArchiveInput) -> Result<VerifiedArchive> {
    let (manifest, payloads) = prepare_archive(input)?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
        .with_context(|| format!("create archive directory {}", parent.display()))?;

    let mut temporary = tempfile::NamedTempFile::new_in(parent)
        .with_context(|| format!("create temporary archive in {}", parent.display()))?;
    restrict_archive_permissions(temporary.path())?;
    write_zip(temporary.as_file_mut(), &manifest, &payloads)?;
    temporary
        .as_file_mut()
        .sync_all()
        .with_context(|| format!("fsync temporary archive in {}", parent.display()))?;
    temporary
        .persist(path)
        .map_err(|error| error.error)
        .with_context(|| format!("atomically replace {}", path.display()))?;
    restrict_archive_permissions(path)?;
    sync_directory(parent)?;

    read_archive_verified(path)
        .with_context(|| format!("verify newly written archive {}", path.display()))
}

pub fn checkpoint_for_close(path: &Path, input: &ArchiveInput) -> CloseVerification {
    match write_archive_atomic(path, input) {
        Ok(verified) => CloseVerification::Verified {
            archive_path: path.to_path_buf(),
            archive_sha256: verified.archive_sha256,
        },
        Err(error) => CloseVerification::Blocked { error },
    }
}

pub fn read_archive_verified(path: &Path) -> Result<VerifiedArchive> {
    let mut file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let archive_sha256 = digest_reader(&mut file)?;
    file.seek(SeekFrom::Start(0))?;
    read_verified_zip(file, archive_sha256)
}

fn prepare_archive(input: &ArchiveInput) -> Result<(ArchiveManifest, Vec<PendingPayload>)> {
    ensure!(!input.session.id.trim().is_empty(), "session id is empty");
    ensure!(!input.bundle.id.trim().is_empty(), "bundle id is empty");
    validate_secret_free_map(&input.target.details)?;

    let mut payloads = Vec::new();
    push_payload(
        &mut payloads,
        CANONICAL_EVENTS_PATH.to_string(),
        input.canonical_events.clone(),
        0o600,
        PayloadRole::CanonicalEvents,
    )?;

    for artifact in &input.native_artifacts {
        validate_archive_relative_path(&artifact.relative_path)?;
        ensure_not_secret_path(&artifact.relative_path)?;
        let archive_path = format!("native/{}", slash_path(&artifact.relative_path)?);
        push_payload(
            &mut payloads,
            archive_path,
            artifact.data.clone(),
            artifact.mode,
            PayloadRole::NativeArtifact {
                relative_path: artifact.relative_path.clone(),
            },
        )?;
    }

    let mut repository_ids = BTreeSet::new();
    let mut repositories = Vec::with_capacity(input.repositories.len());
    for repository in &input.repositories {
        validate_component(&repository.metadata.id, "repository id")?;
        ensure!(
            repository_ids.insert(repository.metadata.id.clone()),
            "duplicate repository id '{}'",
            repository.metadata.id
        );
        validate_archive_relative_path(&repository.metadata.relative_destination)?;
        ensure!(
            !origin_contains_credentials(&repository.metadata.origin),
            "repository '{}' origin contains credentials",
            repository.metadata.id
        );
        validate_untracked_tar(&repository.untracked_tar)?;

        let root = format!("repositories/{}", repository.metadata.id);
        let committed_bundle_path = format!("{root}/committed.bundle");
        let staged_patch_path = format!("{root}/staged.patch");
        let unstaged_patch_path = format!("{root}/unstaged.patch");
        let untracked_tar_path = format!("{root}/untracked.tar");
        for (path, data, role) in [
            (
                &committed_bundle_path,
                &repository.committed_bundle,
                PayloadRole::GitBundle {
                    repository_id: repository.metadata.id.clone(),
                },
            ),
            (
                &staged_patch_path,
                &repository.staged_patch,
                PayloadRole::GitStagedPatch {
                    repository_id: repository.metadata.id.clone(),
                },
            ),
            (
                &unstaged_patch_path,
                &repository.unstaged_patch,
                PayloadRole::GitUnstagedPatch {
                    repository_id: repository.metadata.id.clone(),
                },
            ),
            (
                &untracked_tar_path,
                &repository.untracked_tar,
                PayloadRole::GitUntrackedTar {
                    repository_id: repository.metadata.id.clone(),
                },
            ),
        ] {
            push_payload(&mut payloads, path.clone(), data.clone(), 0o600, role)?;
        }
        repositories.push(RepositoryManifest {
            metadata: repository.metadata.clone(),
            committed_bundle_path,
            staged_patch_path,
            unstaged_patch_path,
            untracked_tar_path,
        });
    }

    payloads.par_iter_mut().for_each(|payload| {
        payload.descriptor.sha256 = digest_bytes(&payload.data);
    });

    let mut paths = BTreeSet::new();
    for payload in &payloads {
        ensure!(
            paths.insert(payload.descriptor.path.clone()),
            "duplicate archive payload path '{}'",
            payload.descriptor.path
        );
    }
    let manifest = ArchiveManifest {
        schema_version: ARCHIVE_SCHEMA_VERSION,
        format: ARCHIVE_FORMAT.to_string(),
        session: input.session.clone(),
        target: input.target.clone(),
        bundle: input.bundle.clone(),
        repositories,
        payloads: payloads
            .iter()
            .map(|payload| payload.descriptor.clone())
            .collect(),
    };
    validate_manifest(&manifest)?;
    Ok((manifest, payloads))
}

fn push_payload(
    payloads: &mut Vec<PendingPayload>,
    path: String,
    data: Vec<u8>,
    mode: u32,
    role: PayloadRole,
) -> Result<()> {
    validate_archive_relative_path(Path::new(&path))?;
    ensure!(
        data.len() as u64 <= MAX_PAYLOAD_BYTES,
        "archive payload '{path}' is too large"
    );
    let descriptor = PayloadDescriptor {
        path,
        sha256: String::new(),
        size: data.len() as u64,
        mode: normalized_mode(mode)?,
        role,
    };
    payloads.push(PendingPayload { descriptor, data });
    Ok(())
}

fn write_zip(
    output: &mut File,
    manifest: &ArchiveManifest,
    payloads: &[PendingPayload],
) -> Result<()> {
    let mut writer = zip::ZipWriter::new(output);
    let manifest_bytes =
        serde_json::to_vec_pretty(manifest).context("serialize archive manifest")?;
    ensure!(
        manifest_bytes.len() as u64 <= MAX_MANIFEST_BYTES,
        "archive manifest is too large"
    );
    writer
        .start_file(
            MANIFEST_PATH,
            SimpleFileOptions::default().unix_permissions(0o600),
        )
        .context("start manifest ZIP entry")?;
    writer
        .write_all(&manifest_bytes)
        .context("write manifest ZIP entry")?;
    for payload in payloads {
        writer
            .start_file(
                &payload.descriptor.path,
                SimpleFileOptions::default().unix_permissions(payload.descriptor.mode),
            )
            .with_context(|| format!("start ZIP entry '{}'", payload.descriptor.path))?;
        writer
            .write_all(&payload.data)
            .with_context(|| format!("write ZIP entry '{}'", payload.descriptor.path))?;
    }
    writer.finish().context("finish Hel archive ZIP")?;
    Ok(())
}

fn read_verified_zip<R: Read + Seek>(reader: R, archive_sha256: String) -> Result<VerifiedArchive> {
    let mut archive = zip::ZipArchive::new(reader).context("open Hel archive ZIP")?;
    let mut raw_entries = BTreeMap::<String, (Vec<u8>, u32)>::new();
    let mut total_size = 0_u64;
    for index in 0..archive.len() {
        let mut entry = archive
            .by_index(index)
            .with_context(|| format!("read ZIP entry {index}"))?;
        ensure!(
            !entry.is_dir(),
            "archive contains directory entry '{}'; only files are allowed",
            entry.name()
        );
        let enclosed = entry
            .enclosed_name()
            .ok_or_else(|| anyhow!("unsafe ZIP entry path '{}'", entry.name()))?;
        validate_archive_relative_path(&enclosed)?;
        ensure!(
            entry.size() <= MAX_PAYLOAD_BYTES,
            "ZIP entry '{}' is too large",
            entry.name()
        );
        total_size = total_size
            .checked_add(entry.size())
            .ok_or_else(|| anyhow!("archive expanded size overflow"))?;
        ensure!(
            total_size <= MAX_ARCHIVE_BYTES,
            "archive expanded size exceeds limit"
        );
        let name = slash_path(&enclosed)?;
        let mode = entry.unix_mode().unwrap_or(0o600) & 0o7777;
        let mut bytes = Vec::with_capacity(entry.size().min(usize::MAX as u64) as usize);
        entry
            .read_to_end(&mut bytes)
            .with_context(|| format!("read ZIP entry '{name}'"))?;
        ensure!(
            raw_entries.insert(name.clone(), (bytes, mode)).is_none(),
            "duplicate ZIP entry '{name}'"
        );
    }

    let (manifest_bytes, _) = raw_entries
        .remove(MANIFEST_PATH)
        .ok_or_else(|| anyhow!("archive is missing {MANIFEST_PATH}"))?;
    ensure!(
        manifest_bytes.len() as u64 <= MAX_MANIFEST_BYTES,
        "archive manifest is too large"
    );
    let manifest: ArchiveManifest =
        serde_json::from_slice(&manifest_bytes).context("parse archive manifest")?;
    validate_manifest(&manifest)?;

    let expected_paths: BTreeSet<_> = manifest
        .payloads
        .iter()
        .map(|descriptor| descriptor.path.as_str())
        .collect();
    let actual_paths: BTreeSet<_> = raw_entries.keys().map(String::as_str).collect();
    ensure!(
        expected_paths == actual_paths,
        "archive payload list does not match manifest"
    );

    let mut payloads = BTreeMap::new();
    for descriptor in &manifest.payloads {
        let (bytes, mode) = raw_entries
            .remove(&descriptor.path)
            .ok_or_else(|| anyhow!("missing payload '{}'", descriptor.path))?;
        ensure!(
            bytes.len() as u64 == descriptor.size,
            "size mismatch for payload '{}'",
            descriptor.path
        );
        ensure!(
            digest_bytes(&bytes) == descriptor.sha256,
            "SHA-256 mismatch for payload '{}'",
            descriptor.path
        );
        ensure!(
            mode == descriptor.mode,
            "mode mismatch for payload '{}'",
            descriptor.path
        );
        if let PayloadRole::GitUntrackedTar { .. } = descriptor.role {
            validate_untracked_tar(&bytes)
                .with_context(|| format!("validate payload '{}'", descriptor.path))?;
        }
        payloads.insert(descriptor.path.clone(), bytes);
    }
    Ok(VerifiedArchive {
        manifest,
        payloads,
        archive_sha256,
    })
}

fn validate_manifest(manifest: &ArchiveManifest) -> Result<()> {
    ensure!(
        manifest.schema_version == ARCHIVE_SCHEMA_VERSION,
        "unsupported Hel archive schema {}",
        manifest.schema_version
    );
    ensure!(
        manifest.format == ARCHIVE_FORMAT,
        "unsupported archive format '{}'",
        manifest.format
    );
    ensure!(
        !manifest.session.id.trim().is_empty(),
        "manifest session id is empty"
    );
    validate_secret_free_map(&manifest.target.details)?;
    let mut paths = BTreeSet::new();
    for descriptor in &manifest.payloads {
        validate_archive_relative_path(Path::new(&descriptor.path))?;
        ensure!(
            descriptor.path != MANIFEST_PATH,
            "manifest cannot describe itself as a payload"
        );
        ensure!(
            paths.insert(&descriptor.path),
            "duplicate manifest payload '{}'",
            descriptor.path
        );
        ensure!(
            descriptor.size <= MAX_PAYLOAD_BYTES,
            "payload '{}' exceeds size limit",
            descriptor.path
        );
        normalized_mode(descriptor.mode)?;
        ensure!(
            is_lower_hex_sha256(&descriptor.sha256),
            "invalid SHA-256 for payload '{}'",
            descriptor.path
        );
        if let PayloadRole::NativeArtifact { relative_path } = &descriptor.role {
            validate_archive_relative_path(relative_path)?;
            ensure_not_secret_path(relative_path)?;
            ensure!(
                descriptor.path == format!("native/{}", slash_path(relative_path)?),
                "native artifact path does not match its role"
            );
        }
    }
    let canonical_count = manifest
        .payloads
        .iter()
        .filter(|payload| payload.role == PayloadRole::CanonicalEvents)
        .count();
    ensure!(
        canonical_count == 1,
        "archive must contain exactly one canonical event payload"
    );

    let mut repository_ids = BTreeSet::new();
    for repository in &manifest.repositories {
        let metadata = &repository.metadata;
        validate_component(&metadata.id, "repository id")?;
        ensure!(
            repository_ids.insert(metadata.id.as_str()),
            "duplicate repository id '{}'",
            metadata.id
        );
        validate_archive_relative_path(&metadata.relative_destination)?;
        ensure!(
            !origin_contains_credentials(&metadata.origin),
            "repository '{}' origin contains credentials",
            metadata.id
        );
        let expected = [
            (
                repository.committed_bundle_path.as_str(),
                PayloadRole::GitBundle {
                    repository_id: metadata.id.clone(),
                },
            ),
            (
                repository.staged_patch_path.as_str(),
                PayloadRole::GitStagedPatch {
                    repository_id: metadata.id.clone(),
                },
            ),
            (
                repository.unstaged_patch_path.as_str(),
                PayloadRole::GitUnstagedPatch {
                    repository_id: metadata.id.clone(),
                },
            ),
            (
                repository.untracked_tar_path.as_str(),
                PayloadRole::GitUntrackedTar {
                    repository_id: metadata.id.clone(),
                },
            ),
        ];
        for (path, role) in expected {
            ensure!(
                manifest
                    .payloads
                    .iter()
                    .any(|payload| payload.path == path && payload.role == role),
                "repository '{}' is missing its {:?} payload",
                metadata.id,
                role
            );
        }
    }
    for payload in &manifest.payloads {
        let repository_id = match &payload.role {
            PayloadRole::GitBundle { repository_id }
            | PayloadRole::GitStagedPatch { repository_id }
            | PayloadRole::GitUnstagedPatch { repository_id }
            | PayloadRole::GitUntrackedTar { repository_id } => Some(repository_id),
            PayloadRole::CanonicalEvents | PayloadRole::NativeArtifact { .. } => None,
        };
        if let Some(repository_id) = repository_id {
            ensure!(
                repository_ids.contains(repository_id.as_str()),
                "payload '{}' refers to unknown repository '{}'",
                payload.path,
                repository_id
            );
        }
    }
    Ok(())
}

fn normalized_mode(mode: u32) -> Result<u32> {
    ensure!(mode & !0o7777 == 0, "invalid payload mode {mode:o}");
    // Writable archive artifacts never need setuid/setgid/sticky bits.
    Ok(mode & 0o0777)
}

fn is_lower_hex_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn digest_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn digest_reader(reader: &mut impl Read) -> Result<String> {
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer).context("hash Hel archive")?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

#[cfg(unix)]
fn restrict_archive_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("set 0600 permissions on {}", path.display()))
}

#[cfg(not(unix))]
fn restrict_archive_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .with_context(|| format!("fsync archive directory {}", path.display()))
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<()> {
    Ok(())
}

fn validate_archive_relative_path(path: &Path) -> Result<()> {
    ensure!(!path.as_os_str().is_empty(), "archive path is empty");
    ensure!(
        !path.is_absolute(),
        "archive path '{}' is absolute",
        path.display()
    );
    for component in path.components() {
        ensure!(
            matches!(component, Component::Normal(_)),
            "unsafe archive path '{}'",
            path.display()
        );
    }
    Ok(())
}

fn slash_path(path: &Path) -> Result<String> {
    validate_archive_relative_path(path)?;
    let parts = path
        .components()
        .map(|component| match component {
            Component::Normal(part) => part
                .to_str()
                .map(str::to_owned)
                .ok_or_else(|| anyhow!("archive path '{}' is not UTF-8", path.display())),
            _ => unreachable!("validated normal path component"),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(parts.join("/"))
}

fn validate_component(value: &str, label: &str) -> Result<()> {
    ensure!(!value.is_empty(), "{label} is empty");
    ensure!(value != "." && value != "..", "invalid {label} '{value}'");
    ensure!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "invalid {label} '{value}'"
    );
    Ok(())
}

fn ensure_not_secret_path(path: &Path) -> Result<()> {
    for component in path.components() {
        let Component::Normal(component) = component else {
            bail!("unsafe artifact path '{}'", path.display());
        };
        let component = component.to_string_lossy().to_ascii_lowercase();
        let forbidden = component == ".env"
            || component.starts_with(".env.")
            || matches!(
                component.as_str(),
                ".git-credentials"
                    | "credentials"
                    | "credentials.json"
                    | "auth.json"
                    | "auth.toml"
                    | "token"
                    | "token.json"
                    | "config.json"
                    | "config.toml"
                    | "settings.json"
            )
            || component.ends_with("_credentials.json")
            || component.ends_with("-credentials.json");
        ensure!(
            !forbidden,
            "refusing to archive credential/config path '{}'",
            path.display()
        );
    }
    Ok(())
}

fn validate_secret_free_map(map: &BTreeMap<String, String>) -> Result<()> {
    for key in map.keys() {
        let key_lower = key.to_ascii_lowercase();
        ensure!(
            !["token", "secret", "password", "credential", "private_key"]
                .iter()
                .any(|needle| key_lower.contains(needle)),
            "target provenance key '{key}' may contain a secret"
        );
    }
    Ok(())
}

fn origin_contains_credentials(origin: &str) -> bool {
    let Ok(url) = url::Url::parse(origin) else {
        return false;
    };
    matches!(url.scheme(), "http" | "https")
        && (!url.username().is_empty() || url.password().is_some())
}

fn redact_origin_credentials(origin: &str) -> Result<String> {
    let Ok(mut url) = url::Url::parse(origin) else {
        return Ok(origin.to_string());
    };
    if !matches!(url.scheme(), "http" | "https") {
        return Ok(origin.to_string());
    }
    if url.username().is_empty() && url.password().is_none() {
        return Ok(origin.to_string());
    }
    url.set_username("")
        .map_err(|()| anyhow!("cannot redact username from Git origin"))?;
    url.set_password(None)
        .map_err(|()| anyhow!("cannot redact password from Git origin"))?;
    Ok(url.to_string())
}

/// A command boundary that lets Git collection and restore be tested without
/// invoking the host's Git executable.
pub trait GitCommandRunner: Sync {
    fn run(&self, repository: &Path, command: &GitCommand) -> Result<GitOutput>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitCommand {
    pub arguments: Vec<OsString>,
    pub stdin: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitOutput {
    pub status: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SystemGit;

impl GitCommandRunner for SystemGit {
    fn run(&self, repository: &Path, command: &GitCommand) -> Result<GitOutput> {
        let mut child = Command::new("git")
            .args(&command.arguments)
            .current_dir(repository)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("start git in {}", repository.display()))?;
        if !command.stdin.is_empty() {
            child
                .stdin
                .take()
                .expect("piped Git stdin")
                .write_all(&command.stdin)
                .context("write Git stdin")?;
        }
        let output = child.wait_with_output().context("wait for Git")?;
        Ok(GitOutput {
            status: output.status.code().unwrap_or(-1),
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitCollectionSpec {
    pub id: String,
    pub relative_destination: PathBuf,
    pub base_commit: String,
    pub full_history: bool,
    pub origin_override: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GitSnapshotProgress {
    UntrackedFile {
        current: usize,
        total: usize,
        path: PathBuf,
    },
}

pub fn collect_git_snapshot(
    runner: &dyn GitCommandRunner,
    repository: &Path,
    spec: &GitCollectionSpec,
) -> Result<RepositorySnapshot> {
    collect_git_snapshot_with_progress(runner, repository, spec, true, &|_| Ok(()))
}

pub fn collect_git_snapshot_with_progress(
    runner: &dyn GitCommandRunner,
    repository: &Path,
    spec: &GitCollectionSpec,
    include_untracked: bool,
    progress: &(dyn Fn(GitSnapshotProgress) -> Result<()> + Sync),
) -> Result<RepositorySnapshot> {
    validate_component(&spec.id, "repository id")?;
    validate_archive_relative_path(&spec.relative_destination)?;
    ensure!(!spec.base_commit.trim().is_empty(), "base commit is empty");

    let origin = if let Some(origin) = &spec.origin_override {
        origin.clone()
    } else {
        let output = run_git(runner, repository, ["remote", "get-url", "origin"], &[])?;
        if output.status == 0 {
            redact_origin_credentials(&trim_output(&output.stdout, "read Git origin")?)?
        } else {
            String::new()
        }
    };
    let base_commit = git_text(runner, repository, ["rev-parse", &spec.base_commit])?;
    let head_commit = git_text(runner, repository, ["rev-parse", "HEAD"])?;
    let branch_output = run_git(
        runner,
        repository,
        ["symbolic-ref", "--quiet", "--short", "HEAD"],
        &[],
    )?;
    let branch = if branch_output.status == 0 {
        Some(trim_output(&branch_output.stdout, "read Git branch")?)
    } else if branch_output.status == 1 {
        None
    } else {
        return Err(git_failure("read Git branch", &branch_output));
    };
    let count = git_text(
        runner,
        repository,
        ["rev-list", "--count", &format!("{base_commit}..HEAD")],
    )?
    .parse::<u64>()
    .context("parse committed delta count")?;
    // These commands only inspect repository state and produce independent
    // payloads. Nested joins share Rayon's bounded worker pool, including when
    // several repositories are being collected at once.
    let ((committed_bundle, staged_patch), (unstaged_patch, untracked_tar)) = rayon::join(
        || {
            rayon::join(
                || {
                    if spec.full_history {
                        git_bytes(
                            runner,
                            repository,
                            ["bundle", "create", "-", "HEAD"],
                            &[],
                            "create full Git bundle",
                        )
                    } else if count == 0 {
                        Ok(Vec::new())
                    } else {
                        git_bytes(
                            runner,
                            repository,
                            ["bundle", "create", "-", "HEAD", &format!("^{base_commit}")],
                            &[],
                            "create committed delta bundle",
                        )
                    }
                },
                || {
                    git_bytes(
                        runner,
                        repository,
                        ["diff", "--binary", "--cached", "--no-ext-diff"],
                        &[],
                        "collect staged Git patch",
                    )
                },
            )
        },
        || {
            rayon::join(
                || {
                    git_bytes(
                        runner,
                        repository,
                        ["diff", "--binary", "--no-ext-diff"],
                        &[],
                        "collect unstaged Git patch",
                    )
                },
                || {
                    if !include_untracked {
                        return Ok(Vec::new());
                    }
                    let untracked = git_bytes(
                        runner,
                        repository,
                        ["ls-files", "--others", "--exclude-standard", "-z"],
                        &[],
                        "list nonignored untracked files",
                    )?;
                    build_untracked_tar(repository, &untracked, progress)
                },
            )
        },
    );
    let committed_bundle = committed_bundle?;
    let staged_patch = staged_patch?;
    let unstaged_patch = unstaged_patch?;
    let untracked_tar = untracked_tar?;

    Ok(RepositorySnapshot {
        metadata: RepositoryMetadata {
            id: spec.id.clone(),
            relative_destination: spec.relative_destination.clone(),
            origin,
            base_commit,
            full_history: spec.full_history,
            head_commit,
            branch,
        },
        committed_bundle,
        staged_patch,
        unstaged_patch,
        untracked_tar,
    })
}

pub fn restore_git_snapshot(
    runner: &dyn GitCommandRunner,
    repository: &Path,
    snapshot: &RepositorySnapshot,
) -> Result<()> {
    if !snapshot.committed_bundle.is_empty() {
        let mut bundle = tempfile::NamedTempFile::new_in(repository)
            .with_context(|| format!("create temporary Git bundle in {}", repository.display()))?;
        bundle
            .write_all(&snapshot.committed_bundle)
            .context("write temporary Git bundle")?;
        bundle.flush().context("flush temporary Git bundle")?;
        let bundle_path = bundle.path().as_os_str().to_os_string();
        git_success(
            runner,
            repository,
            GitCommand {
                arguments: vec![OsString::from("fetch"), bundle_path, OsString::from("HEAD")],
                stdin: Vec::new(),
            },
            "fetch committed delta bundle",
        )?;
    }
    if snapshot.metadata.full_history {
        git_bytes(
            runner,
            repository,
            [
                "update-ref",
                "refs/hel/base",
                &snapshot.metadata.base_commit,
            ],
            &[],
            "record Hel repository base",
        )?;
    }
    let checkout_target = if snapshot.committed_bundle.is_empty() {
        snapshot.metadata.head_commit.as_str()
    } else {
        "FETCH_HEAD"
    };
    if let Some(branch) = &snapshot.metadata.branch {
        git_bytes(
            runner,
            repository,
            ["check-ref-format", "--branch", branch],
            &[],
            "validate restored branch",
        )?;
        git_bytes(
            runner,
            repository,
            ["checkout", "-B", branch, checkout_target],
            &[],
            "restore committed branch",
        )?;
    } else {
        git_bytes(
            runner,
            repository,
            ["checkout", "--detach", checkout_target],
            &[],
            "restore detached commit",
        )?;
    }
    if !snapshot.staged_patch.is_empty() {
        git_bytes(
            runner,
            repository,
            ["apply", "--binary", "--index", "-"],
            &snapshot.staged_patch,
            "restore staged Git patch",
        )?;
    }
    if !snapshot.unstaged_patch.is_empty() {
        git_bytes(
            runner,
            repository,
            ["apply", "--binary", "-"],
            &snapshot.unstaged_patch,
            "restore unstaged Git patch",
        )?;
    }
    restore_untracked_tar(repository, &snapshot.untracked_tar)
}

fn run_git<const N: usize>(
    runner: &dyn GitCommandRunner,
    repository: &Path,
    arguments: [&str; N],
    stdin: &[u8],
) -> Result<GitOutput> {
    runner.run(
        repository,
        &GitCommand {
            arguments: arguments.into_iter().map(OsString::from).collect(),
            stdin: stdin.to_vec(),
        },
    )
}

fn git_success(
    runner: &dyn GitCommandRunner,
    repository: &Path,
    command: GitCommand,
    action: &str,
) -> Result<Vec<u8>> {
    let output = runner.run(repository, &command)?;
    ensure!(output.status == 0, "{}", git_failure(action, &output));
    Ok(output.stdout)
}

fn git_bytes<const N: usize>(
    runner: &dyn GitCommandRunner,
    repository: &Path,
    arguments: [&str; N],
    stdin: &[u8],
    action: &str,
) -> Result<Vec<u8>> {
    let output = run_git(runner, repository, arguments, stdin)?;
    ensure!(output.status == 0, "{}", git_failure(action, &output));
    Ok(output.stdout)
}

fn git_text<const N: usize>(
    runner: &dyn GitCommandRunner,
    repository: &Path,
    arguments: [&str; N],
) -> Result<String> {
    let output = run_git(runner, repository, arguments, &[])?;
    ensure!(
        output.status == 0,
        "{}",
        git_failure("run Git command", &output)
    );
    trim_output(&output.stdout, "decode Git output")
}

fn trim_output(output: &[u8], action: &str) -> Result<String> {
    Ok(std::str::from_utf8(output)
        .with_context(|| action.to_string())?
        .trim()
        .to_string())
}

fn git_failure(action: &str, output: &GitOutput) -> anyhow::Error {
    let stderr = String::from_utf8_lossy(&output.stderr);
    anyhow!(
        "{action} failed with status {}: {}",
        output.status,
        stderr.trim()
    )
}

fn build_untracked_tar(
    repository: &Path,
    nul_paths: &[u8],
    progress: &(dyn Fn(GitSnapshotProgress) -> Result<()> + Sync),
) -> Result<Vec<u8>> {
    let paths = nul_paths
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .collect::<Vec<_>>();
    let total = paths.len();
    let mut output = Vec::new();
    {
        let mut builder = tar::Builder::new(&mut output);
        for (index, raw_path) in paths.into_iter().enumerate() {
            let relative = path_from_git_bytes(raw_path)?;
            validate_archive_relative_path(&relative)?;
            // In addition to Git's ignore rules, skip conventional credential
            // paths so they can never enter the untracked payload.
            if ensure_not_secret_path(&relative).is_err() {
                continue;
            }
            progress(GitSnapshotProgress::UntrackedFile {
                current: index + 1,
                total,
                path: relative.clone(),
            })?;
            let source = repository.join(&relative);
            ensure_no_symlink_ancestors(repository, &relative)?;
            let metadata = fs::symlink_metadata(&source)
                .with_context(|| format!("stat untracked path {}", source.display()))?;
            if metadata.file_type().is_symlink() {
                let target = fs::read_link(&source)
                    .with_context(|| format!("read symlink {}", source.display()))?;
                validate_symlink_target(&relative, &target)?;
                let mut header = tar::Header::new_gnu();
                header.set_entry_type(tar::EntryType::Symlink);
                header.set_size(0);
                header.set_mode(0o777);
                header.set_link_name(&target)?;
                header.set_cksum();
                builder.append_data(&mut header, &relative, std::io::empty())?;
            } else if metadata.is_file() {
                let mut header = tar::Header::new_gnu();
                header.set_metadata(&metadata);
                header.set_cksum();
                let mut file = File::open(&source)
                    .with_context(|| format!("open untracked file {}", source.display()))?;
                builder.append_data(&mut header, &relative, &mut file)?;
            } else {
                bail!("unsupported untracked file type at {}", source.display());
            }
        }
        builder.finish().context("finish untracked-file tar")?;
    }
    Ok(output)
}

fn path_from_git_bytes(bytes: &[u8]) -> Result<PathBuf> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        Ok(PathBuf::from(std::ffi::OsStr::from_bytes(bytes)))
    }
    #[cfg(not(unix))]
    {
        Ok(PathBuf::from(
            std::str::from_utf8(bytes).context("Git path is not UTF-8")?,
        ))
    }
}

fn validate_untracked_tar(bytes: &[u8]) -> Result<()> {
    let mut archive = tar::Archive::new(Cursor::new(bytes));
    for entry in archive.entries().context("read untracked-file tar")? {
        let entry = entry.context("read untracked-file tar entry")?;
        let relative = entry.path().context("read untracked-file tar path")?;
        validate_archive_relative_path(&relative)?;
        ensure_not_secret_path(&relative)?;
        let entry_type = entry.header().entry_type();
        ensure!(
            entry_type.is_file() || entry_type.is_symlink(),
            "untracked tar contains unsupported entry type for '{}'",
            relative.display()
        );
        if entry_type.is_symlink() {
            let target = entry
                .link_name()
                .context("read untracked symlink target")?
                .ok_or_else(|| {
                    anyhow!("untracked symlink '{}' has no target", relative.display())
                })?;
            validate_symlink_target(&relative, &target)?;
        }
    }
    Ok(())
}

fn restore_untracked_tar(repository: &Path, bytes: &[u8]) -> Result<()> {
    validate_untracked_tar(bytes)?;
    let mut archive = tar::Archive::new(Cursor::new(bytes));
    for entry in archive.entries().context("read untracked-file tar")? {
        let mut entry = entry.context("read untracked-file tar entry")?;
        let relative = entry.path()?.into_owned();
        ensure_no_symlink_ancestors(repository, &relative)?;
        let destination = repository.join(&relative);
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create directory {}", parent.display()))?;
            ensure_no_symlink_ancestors(repository, &relative)?;
        }
        ensure!(
            !destination.exists(),
            "refusing to overwrite restored untracked path '{}'",
            destination.display()
        );
        if entry.header().entry_type().is_symlink() {
            let target = entry.link_name()?.ok_or_else(|| {
                anyhow!("untracked symlink '{}' has no target", relative.display())
            })?;
            create_symlink(&target, &destination)?;
        } else {
            let mut output = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&destination)
                .with_context(|| format!("create untracked file {}", destination.display()))?;
            std::io::copy(&mut entry, &mut output)?;
            output.sync_all()?;
            #[cfg(unix)]
            if let Ok(mode) = entry.header().mode() {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&destination, fs::Permissions::from_mode(mode & 0o777))?;
            }
        }
    }
    Ok(())
}

fn ensure_no_symlink_ancestors(root: &Path, relative: &Path) -> Result<()> {
    let mut current = root.to_path_buf();
    let components: Vec<_> = relative.components().collect();
    for component in components.iter().take(components.len().saturating_sub(1)) {
        let Component::Normal(component) = component else {
            bail!("unsafe path '{}'", relative.display());
        };
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) => ensure!(
                !metadata.file_type().is_symlink(),
                "path '{}' traverses a symlink",
                relative.display()
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => {
                return Err(error).with_context(|| format!("inspect {}", current.display()));
            }
        }
    }
    Ok(())
}

fn validate_symlink_target(link_path: &Path, target: &Path) -> Result<()> {
    ensure!(
        !target.is_absolute(),
        "symlink '{}' has an absolute target",
        link_path.display()
    );
    let mut depth = link_path
        .parent()
        .map(|parent| parent.components().count())
        .unwrap_or(0);
    for component in target.components() {
        match component {
            Component::Normal(_) => depth += 1,
            Component::CurDir => {}
            Component::ParentDir if depth > 0 => depth -= 1,
            Component::ParentDir => {
                bail!("symlink '{}' escapes the repository", link_path.display())
            }
            Component::RootDir | Component::Prefix(_) => {
                bail!("symlink '{}' escapes the repository", link_path.display())
            }
        }
    }
    Ok(())
}

#[cfg(unix)]
fn create_symlink(target: &Path, destination: &Path) -> Result<()> {
    std::os::unix::fs::symlink(target, destination)
        .with_context(|| format!("create symlink {}", destination.display()))
}

#[cfg(windows)]
fn create_symlink(target: &Path, destination: &Path) -> Result<()> {
    std::os::windows::fs::symlink_file(target, destination)
        .with_context(|| format!("create symlink {}", destination.display()))
}

#[cfg(not(any(unix, windows)))]
fn create_symlink(_target: &Path, _destination: &Path) -> Result<()> {
    bail!("symlink restore is not supported on this platform")
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Barrier, Mutex};

    use super::*;

    #[derive(Default)]
    struct FakeGit {
        outputs: Mutex<VecDeque<GitOutput>>,
        commands: Mutex<Vec<GitCommand>>,
    }

    impl FakeGit {
        fn with_outputs(outputs: impl IntoIterator<Item = GitOutput>) -> Self {
            Self {
                outputs: Mutex::new(outputs.into_iter().collect()),
                commands: Mutex::new(Vec::new()),
            }
        }

        fn commands(&self) -> Vec<GitCommand> {
            self.commands.lock().unwrap().clone()
        }
    }

    impl GitCommandRunner for FakeGit {
        fn run(&self, _repository: &Path, command: &GitCommand) -> Result<GitOutput> {
            self.commands.lock().unwrap().push(command.clone());
            self.outputs
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| anyhow!("unexpected Git command: {:?}", command.arguments))
        }
    }

    struct CollectionGit {
        delta_count: u64,
        payload_barrier: Option<Barrier>,
        commands: Mutex<Vec<GitCommand>>,
    }

    impl CollectionGit {
        fn new(delta_count: u64, concurrent_payloads: bool) -> Self {
            Self {
                delta_count,
                payload_barrier: concurrent_payloads.then(|| Barrier::new(4)),
                commands: Mutex::new(Vec::new()),
            }
        }

        fn commands(&self) -> Vec<GitCommand> {
            self.commands.lock().unwrap().clone()
        }
    }

    impl GitCommandRunner for CollectionGit {
        fn run(&self, _repository: &Path, command: &GitCommand) -> Result<GitOutput> {
            self.commands.lock().unwrap().push(command.clone());
            let arguments = command
                .arguments
                .iter()
                .map(|argument| argument.to_string_lossy())
                .collect::<Vec<_>>();
            let stdout = match arguments.first().map(|argument| argument.as_ref()) {
                Some("remote") => b"https://token@github.com/example/repo.git\n".to_vec(),
                Some("rev-parse") => format!("{}\n", "b".repeat(40)).into_bytes(),
                Some("symbolic-ref") => b"feature/hel\n".to_vec(),
                Some("rev-list") => format!("{}\n", self.delta_count).into_bytes(),
                Some("bundle") => {
                    self.payload_barrier.as_ref().unwrap().wait();
                    b"bundle".to_vec()
                }
                Some("diff") => {
                    if let Some(barrier) = &self.payload_barrier {
                        barrier.wait();
                    }
                    if arguments.iter().any(|argument| argument == "--cached") {
                        b"staged".to_vec()
                    } else {
                        b"unstaged".to_vec()
                    }
                }
                Some("ls-files") => {
                    if let Some(barrier) = &self.payload_barrier {
                        barrier.wait();
                    }
                    b"note.txt\0.env\0".to_vec()
                }
                other => return Err(anyhow!("unexpected Git command: {other:?}")),
            };
            Ok(git_ok(stdout))
        }
    }

    fn git_ok(stdout: impl Into<Vec<u8>>) -> GitOutput {
        GitOutput {
            status: 0,
            stdout: stdout.into(),
            stderr: Vec::new(),
        }
    }

    fn git(repository: &Path, arguments: &[&str]) -> Vec<u8> {
        let output = Command::new("git")
            .args(arguments)
            .current_dir(repository)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {arguments:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        output.stdout
    }

    fn tar_with_file(path: &str, contents: &[u8], mode: u32) -> Vec<u8> {
        let mut output = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut output);
            let mut header = tar::Header::new_gnu();
            header.set_path(path).unwrap();
            header.set_size(contents.len() as u64);
            header.set_mode(mode);
            header.set_cksum();
            builder.append(&header, contents).unwrap();
            builder.finish().unwrap();
        }
        output
    }

    fn repository(id: &str) -> RepositorySnapshot {
        RepositorySnapshot {
            metadata: RepositoryMetadata {
                id: id.to_string(),
                relative_destination: PathBuf::from(id),
                origin: format!("https://github.com/example/{id}.git"),
                base_commit: "a".repeat(40),
                full_history: false,
                head_commit: "b".repeat(40),
                branch: Some("feature/hel".to_string()),
            },
            committed_bundle: format!("bundle-{id}").into_bytes(),
            staged_patch: format!("staged-{id}").into_bytes(),
            unstaged_patch: format!("unstaged-{id}").into_bytes(),
            untracked_tar: tar_with_file("scripts/tool.sh", b"#!/bin/sh\n", 0o755),
        }
    }

    fn input() -> ArchiveInput {
        ArchiveInput {
            session: SessionManifest {
                id: "session-1".into(),
                title: "Forge Hel".into(),
                harness_kind: HarnessKind::Codex,
                profile_id: "codex-1".into(),
                native_session_id: "native-1".into(),
                created_at: "2026-08-09T10:00:00Z".into(),
                checkpointed_at: "2026-08-09T10:05:00Z".into(),
                hel_version: "0.1.0".into(),
                worker_version: "0.1.0".into(),
                adapter_version: "0.1.0".into(),
            },
            target: TargetManifest {
                template_id: "podman-rust".into(),
                target_kind: "local_podman".into(),
                details: BTreeMap::from([("image".into(), "fedora:latest".into())]),
            },
            bundle: BundleManifest {
                id: "hel".into(),
                primary_repository: "hel".into(),
            },
            canonical_events: b"{\"seq\":1}\n".to_vec(),
            native_artifacts: vec![NativeArtifact {
                relative_path: PathBuf::from("sessions/native-1/rollout.jsonl"),
                data: b"native transcript".to_vec(),
                mode: 0o600,
            }],
            repositories: vec![repository("hel"), repository("worker")],
        }
    }

    #[test]
    fn archive_round_trip_verifies_multi_repo_payloads_and_mode() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("session.hel.zip");
        let verified = write_archive_atomic(&path, &input()).unwrap();
        assert_eq!(verified.manifest.repositories.len(), 2);
        assert_eq!(
            verified
                .payload_by_role(&PayloadRole::CanonicalEvents)
                .unwrap(),
            b"{\"seq\":1}\n"
        );
        assert_eq!(verified.archive_sha256.len(), 64);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn corruption_is_detected_by_payload_digest() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("session.hel.zip");
        write_archive_atomic(&path, &input()).unwrap();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let mut archive = zip::ZipArchive::new(file).unwrap();
        let entry = archive.by_name(CANONICAL_EVENTS_PATH).unwrap();
        let data_start = entry.data_start();
        drop(entry);
        let mut file = archive.into_inner();
        file.seek(SeekFrom::Start(data_start)).unwrap();
        file.write_all(b"X").unwrap();
        drop(file);

        assert!(read_archive_verified(&path).is_err());
    }

    #[test]
    fn traversal_and_credentials_are_rejected_before_destination_changes() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("session.hel.zip");
        fs::write(&path, b"existing archive").unwrap();
        let mut unsafe_input = input();
        unsafe_input.native_artifacts[0].relative_path = PathBuf::from("../auth.json");
        assert!(write_archive_atomic(&path, &unsafe_input).is_err());
        assert_eq!(fs::read(&path).unwrap(), b"existing archive");

        unsafe_input.native_artifacts[0].relative_path = PathBuf::from("auth.json");
        assert!(write_archive_atomic(&path, &unsafe_input).is_err());
        assert_eq!(fs::read(&path).unwrap(), b"existing archive");
    }

    #[test]
    fn close_verification_blocks_teardown_on_archive_failure() {
        let directory = tempfile::tempdir().unwrap();
        let mut unsafe_input = input();
        unsafe_input.repositories[0].metadata.origin =
            "https://secret@github.com/example/hel.git".into();
        let result = checkpoint_for_close(&directory.path().join("x.hel.zip"), &unsafe_input);
        assert!(!result.teardown_allowed());
        assert!(matches!(result, CloseVerification::Blocked { .. }));
    }

    #[test]
    fn untracked_tar_preserves_executable_mode_and_safe_symlink() {
        let source = tempfile::tempdir().unwrap();
        fs::create_dir_all(source.path().join("scripts")).unwrap();
        let tool = source.path().join("scripts/tool");
        fs::write(&tool, b"tool").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&tool, fs::Permissions::from_mode(0o755)).unwrap();
            std::os::unix::fs::symlink("tool", source.path().join("scripts/current")).unwrap();
        }
        #[cfg(unix)]
        let paths = b"scripts/tool\0scripts/current\0".to_vec();
        #[cfg(not(unix))]
        let paths = b"scripts/tool\0".to_vec();
        let tar = build_untracked_tar(source.path(), &paths, &|_| Ok(())).unwrap();
        validate_untracked_tar(&tar).unwrap();

        let destination = tempfile::tempdir().unwrap();
        restore_untracked_tar(destination.path(), &tar).unwrap();
        assert_eq!(
            fs::read(destination.path().join("scripts/tool")).unwrap(),
            b"tool"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(destination.path().join("scripts/tool"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o755
            );
            assert_eq!(
                fs::read_link(destination.path().join("scripts/current")).unwrap(),
                PathBuf::from("tool")
            );
        }
    }

    #[test]
    fn untracked_tar_progress_can_cancel_before_opening_the_next_file() {
        let source = tempfile::tempdir().unwrap();
        fs::write(source.path().join("first.txt"), b"first").unwrap();
        let paths = b"first.txt\0missing.txt\0";
        let seen = std::sync::Mutex::new(Vec::new());

        let error = build_untracked_tar(source.path(), paths, &|progress| {
            let GitSnapshotProgress::UntrackedFile { current, total, .. } = progress;
            seen.lock().unwrap().push((current, total));
            ensure!(current < 2, "cancelled by test");
            Ok(())
        })
        .unwrap_err();

        assert!(error.to_string().contains("cancelled by test"));
        assert_eq!(*seen.lock().unwrap(), vec![(1, 2), (2, 2)]);
    }

    #[test]
    fn malicious_untracked_tar_is_rejected() {
        assert!(validate_archive_relative_path(Path::new("../escape")).is_err());
        let tar = tar_with_file(".env", b"secret", 0o600);
        assert!(validate_untracked_tar(&tar).is_err());
    }

    #[test]
    fn unsafe_zip_entry_is_rejected_even_without_extraction() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("unsafe.hel.zip");
        {
            let file = File::create(&path).unwrap();
            let mut writer = zip::ZipWriter::new(file);
            writer
                .start_file("../escape", SimpleFileOptions::default())
                .unwrap();
            writer.write_all(b"no").unwrap();
            writer.finish().unwrap();
        }
        assert!(read_archive_verified(&path).is_err());
        assert!(!directory.path().join("escape").exists());
    }

    #[test]
    fn git_collection_is_abstracted_redacts_origin_and_skips_credentials() {
        let repository = tempfile::tempdir().unwrap();
        fs::write(repository.path().join("note.txt"), b"keep").unwrap();
        fs::write(repository.path().join(".env"), b"SECRET=nope").unwrap();
        let runner = CollectionGit::new(0, false);
        let snapshot = collect_git_snapshot(
            &runner,
            repository.path(),
            &GitCollectionSpec {
                id: "repo".into(),
                relative_destination: PathBuf::from("repo"),
                base_commit: "a".repeat(40),
                full_history: false,
                origin_override: None,
            },
        )
        .unwrap();
        assert_eq!(
            snapshot.metadata.origin,
            "https://github.com/example/repo.git"
        );
        let mut archive = tar::Archive::new(Cursor::new(&snapshot.untracked_tar));
        let paths: Vec<_> = archive
            .entries()
            .unwrap()
            .map(|entry| entry.unwrap().path().unwrap().into_owned())
            .collect();
        assert_eq!(paths, vec![PathBuf::from("note.txt")]);
        assert_eq!(runner.commands().len(), 8);
    }

    #[test]
    fn git_collection_can_omit_untracked_files_without_losing_tracked_changes() {
        let repository = tempfile::tempdir().unwrap();
        fs::write(repository.path().join("note.txt"), b"untracked").unwrap();
        let runner = CollectionGit::new(0, false);
        let snapshot = collect_git_snapshot_with_progress(
            &runner,
            repository.path(),
            &GitCollectionSpec {
                id: "repo".into(),
                relative_destination: PathBuf::from("repo"),
                base_commit: "a".repeat(40),
                full_history: false,
                origin_override: None,
            },
            false,
            &|_| Ok(()),
        )
        .unwrap();

        assert_eq!(snapshot.staged_patch, b"staged");
        assert_eq!(snapshot.unstaged_patch, b"unstaged");
        assert!(snapshot.untracked_tar.is_empty());
        assert!(runner.commands().iter().all(|command| {
            command
                .arguments
                .first()
                .is_none_or(|argument| argument != "ls-files")
        }));
    }

    #[test]
    fn git_collection_builds_independent_payloads_concurrently() {
        let repository = tempfile::tempdir().unwrap();
        fs::write(repository.path().join("note.txt"), b"keep").unwrap();
        fs::write(repository.path().join(".env"), b"SECRET=nope").unwrap();
        let runner = CollectionGit::new(1, true);
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .unwrap();

        let snapshot = pool
            .install(|| {
                collect_git_snapshot(
                    &runner,
                    repository.path(),
                    &GitCollectionSpec {
                        id: "repo".into(),
                        relative_destination: PathBuf::from("repo"),
                        base_commit: "a".repeat(40),
                        full_history: false,
                        origin_override: None,
                    },
                )
            })
            .unwrap();

        assert_eq!(snapshot.committed_bundle, b"bundle");
        assert_eq!(snapshot.staged_patch, b"staged");
        assert_eq!(snapshot.unstaged_patch, b"unstaged");
        assert_eq!(runner.commands().len(), 9);
    }

    #[test]
    fn git_restore_routes_patches_through_injected_runner() {
        let destination = tempfile::tempdir().unwrap();
        let runner = FakeGit::with_outputs([
            git_ok(Vec::new()),
            git_ok(Vec::new()),
            git_ok(Vec::new()),
            git_ok(Vec::new()),
        ]);
        let mut snapshot = repository("repo");
        snapshot.committed_bundle.clear();
        snapshot.untracked_tar = tar_with_file("new.sh", b"echo hi\n", 0o755);
        restore_git_snapshot(&runner, destination.path(), &snapshot).unwrap();
        let commands = runner.commands();
        assert_eq!(commands.len(), 4);
        assert_eq!(commands[2].stdin, b"staged-repo");
        assert_eq!(commands[3].stdin, b"unstaged-repo");
        assert_eq!(
            fs::read(destination.path().join("new.sh")).unwrap(),
            b"echo hi\n"
        );
    }

    #[test]
    fn system_git_round_trip_restores_commits_index_worktree_and_untracked() {
        let source = tempfile::tempdir().unwrap();
        git(source.path(), &["init", "-q", "-b", "main"]);
        git(source.path(), &["config", "user.name", "Hel Test"]);
        git(source.path(), &["config", "user.email", "hel@example.test"]);
        git(
            source.path(),
            &[
                "remote",
                "add",
                "origin",
                "https://github.com/example/repo.git",
            ],
        );
        fs::write(source.path().join("tracked.txt"), b"base\n").unwrap();
        fs::write(source.path().join("dirty.txt"), b"clean\n").unwrap();
        git(source.path(), &["add", "."]);
        git(source.path(), &["commit", "-qm", "base"]);
        let base = String::from_utf8(git(source.path(), &["rev-parse", "HEAD"]))
            .unwrap()
            .trim()
            .to_string();
        fs::write(source.path().join("tracked.txt"), b"committed\n").unwrap();
        git(source.path(), &["commit", "-qam", "delta"]);
        fs::write(source.path().join("staged.txt"), b"staged\n").unwrap();
        git(source.path(), &["add", "staged.txt"]);
        fs::write(source.path().join("dirty.txt"), b"dirty\n").unwrap();
        fs::write(source.path().join("new.txt"), b"untracked\n").unwrap();

        let snapshot = collect_git_snapshot(
            &SystemGit,
            source.path(),
            &GitCollectionSpec {
                id: "repo".into(),
                relative_destination: PathBuf::from("repo"),
                base_commit: base.clone(),
                full_history: false,
                origin_override: None,
            },
        )
        .unwrap();
        assert!(!snapshot.committed_bundle.is_empty());

        let destination_parent = tempfile::tempdir().unwrap();
        let destination = destination_parent.path().join("restore");
        git(
            destination_parent.path(),
            &[
                "clone",
                "-q",
                source.path().to_str().unwrap(),
                destination.to_str().unwrap(),
            ],
        );
        git(&destination, &["reset", "--hard", &base]);
        restore_git_snapshot(&SystemGit, &destination, &snapshot).unwrap();

        assert_eq!(
            fs::read(destination.join("tracked.txt")).unwrap(),
            b"committed\n"
        );
        assert_eq!(fs::read(destination.join("dirty.txt")).unwrap(), b"dirty\n");
        assert_eq!(
            fs::read(destination.join("new.txt")).unwrap(),
            b"untracked\n"
        );
        let status = String::from_utf8(git(&destination, &["status", "--short"])).unwrap();
        assert!(status.contains("A  staged.txt"));
        assert!(status.contains(" M dirty.txt"));
        assert!(status.contains("?? new.txt"));
    }
}
