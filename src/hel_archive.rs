//! Versioned, verified checkpoint archives for Hel sessions.
//!
//! This module deliberately accepts native harness artifacts one file at a
//! time.  Harness adapters must use a versioned allowlist; recursively copying
//! a profile home would risk archiving credentials and configuration.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::{Cursor, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context, Result, anyhow, bail, ensure};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zip::CompressionMethod;
use zip::write::SimpleFileOptions;

use crate::hel_config::HarnessKind;

/// Baseline schema. Every payload occupies exactly one ZIP entry, so any build
/// that understands schema 2 can read the archive.
pub const ARCHIVE_SCHEMA_VERSION: u32 = 2;
/// Schema 2 plus sharded payloads. A payload larger than
/// [`PAYLOAD_PART_BYTES`] is written as several `*.helpart.NNNNN` ZIP entries
/// so compression and verification can run in parallel. Archives declare this
/// schema only when at least one payload is sharded, which keeps small
/// sessions readable by builds that predate sharding and makes older builds
/// reject sharded archives with an explicit version error instead of
/// misreading part entries.
pub const ARCHIVE_SCHEMA_VERSION_SHARDED: u32 = 3;
pub const ARCHIVE_FORMAT: &str = "hel-session";
pub const EVENT_FRONTIER_GENESIS_DIGEST: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";

const MANIFEST_PATH: &str = "manifest.json";
const CANONICAL_SESSION_PATH: &str = "canonical/session.json";
const MAX_MANIFEST_BYTES: u64 = 8 * 1024 * 1024;
const MAX_PAYLOAD_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const MAX_ARCHIVE_BYTES: u64 = 32 * 1024 * 1024 * 1024;
/// Compressible payloads larger than this are split into parts of this size.
/// DEFLATE is sequential inside one stream, so parts are what let both the
/// writer and the reader use every core on a large payload.
const PAYLOAD_PART_BYTES: usize = 16 * 1024 * 1024;
const PAYLOAD_PART_SUFFIX: &str = ".helpart.";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionManifest {
    pub id: String,
    pub title: String,
    pub harness_kind: HarnessKind,
    pub profile_id: String,
    pub native_session_id: String,
    pub created_at: String,
    pub checkpointed_at: String,
    pub hel_version: String,
    pub relay_version: String,
    pub adapter_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BundleManifest {
    pub id: String,
    pub primary_repository: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetManifest {
    pub template_id: String,
    pub target_kind: String,
    /// Informational provenance only. It must not contain credentials.
    pub details: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryMetadata {
    pub id: String,
    pub relative_destination: PathBuf,
    pub origin: String,
    /// Informational provenance. Session deltas exclude every origin ref rather
    /// than a single base, so they record an empty string.
    pub base_commit: String,
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

/// Controller-owned, materialized state captured at a checkpoint barrier.
///
/// These types deliberately do not depend on the live ACP or relay types. ACP
/// content blocks and evolving tool/plan details are stored as JSON values at
/// the stable archive boundary, while the identity, ordering, and timestamps
/// needed to rebuild controller state remain explicit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalSessionSnapshot {
    /// Highest relay event ordinal incorporated into this projection.
    pub event_frontier: u64,
    /// Relay-authored rolling digest of the exact event prefix at the frontier.
    pub event_frontier_digest: String,
    pub session: CanonicalSessionState,
    pub transcript: Vec<CanonicalTranscriptItem>,
    pub queued_prompts: Vec<CanonicalQueuedPrompt>,
}

impl CanonicalSessionSnapshot {
    pub fn validate(&self) -> Result<()> {
        validate_canonical_session(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalSessionState {
    pub execution: CanonicalExecutionState,
    /// Monotonic controller projection watermark derived from relay events.
    pub last_activity_at_ms: Option<i64>,
    pub session_title: Option<String>,
    pub configuration: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum CanonicalExecutionState {
    Idle,
    Running { started_at_ms: i64 },
    Closing,
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalTranscriptItem {
    pub stable_id: String,
    /// Ordinal of the event that created this logical transcript item.
    pub position: u64,
    /// Ordinal of the most recent content chunk for an agent message. This is
    /// `None` for every other logical item.
    pub latest_content_event_ordinal: Option<u64>,
    pub created_at_ms: i64,
    pub last_changed_at_ms: i64,
    pub body: CanonicalTranscriptBody,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CanonicalTranscriptBody {
    User {
        /// ACP content blocks in their JSON representation.
        content: Vec<serde_json::Value>,
    },
    Agent {
        /// Complete ACP `ContentChunk` values.
        chunks: Vec<serde_json::Value>,
        streaming: bool,
    },
    Thought {
        /// Complete ACP `ContentChunk` values.
        chunks: Vec<serde_json::Value>,
        streaming: bool,
    },
    Tool {
        /// Complete current ACP `ToolCall` value.
        call: serde_json::Value,
    },
    Plan {
        /// Complete current ACP `Plan` value.
        plan: serde_json::Value,
    },
    System {
        text: String,
    },
}

/// What a queued entry does when its turn comes. Archives written before
/// configuration changes could be queued carry no `kind`, so it defaults to a
/// prompt.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanonicalQueuedCommandKind {
    #[default]
    Prompt,
    SetConfig {
        key: String,
        value: String,
    },
}

impl CanonicalQueuedCommandKind {
    fn is_prompt(&self) -> bool {
        matches!(self, Self::Prompt)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalQueuedPrompt {
    pub command_id: String,
    #[serde(default, skip_serializing_if = "CanonicalQueuedCommandKind::is_prompt")]
    pub kind: CanonicalQueuedCommandKind,
    /// ACP content blocks in their JSON representation. A queued configuration
    /// change carries the composer text that produced it.
    pub content: Vec<serde_json::Value>,
    pub queued_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PayloadRole {
    CanonicalSession,
    NativeArtifact { relative_path: PathBuf },
    GitBundle { repository_id: String },
    GitStagedPatch { repository_id: String },
    GitUnstagedPatch { repository_id: String },
    GitUntrackedTar { repository_id: String },
}

/// One byte range of a sharded payload, stored as its own ZIP entry.
///
/// Parts are contiguous and ordered: part `i` covers the bytes right after
/// part `i - 1`, and concatenating every part in order reproduces the payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PayloadPartDescriptor {
    pub path: String,
    pub sha256: String,
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PayloadDescriptor {
    pub path: String,
    pub sha256: String,
    pub size: u64,
    pub mode: u32,
    pub role: PayloadRole,
    /// Empty for a whole payload stored in one ZIP entry. Otherwise the
    /// ordered parts the payload was split into; `path` then names no ZIP
    /// entry of its own and `sha256`/`size` describe the reassembled payload.
    /// The field is absent from schema-2 manifests, so builds that predate
    /// sharding also reject it through `deny_unknown_fields`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parts: Vec<PayloadPartDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    pub canonical_session: CanonicalSessionSnapshot,
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

    pub fn canonical_session(&self) -> Result<CanonicalSessionSnapshot> {
        let snapshot: CanonicalSessionSnapshot =
            serde_json::from_slice(self.payload_by_role(&PayloadRole::CanonicalSession)?)
                .context("parse canonical session snapshot")?;
        snapshot.validate()?;
        Ok(snapshot)
    }
}

/// Fully verified archive metadata for callers that do not need to restore
/// payload bodies. Verification streams repository and native payloads instead
/// of retaining them, so memory is bounded by the manifest, canonical session,
/// and ZIP read buffers rather than the archive's expanded size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedArchiveMetadata {
    pub manifest: ArchiveManifest,
    pub canonical_session: CanonicalSessionSnapshot,
    pub archive_sha256: String,
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
struct PendingPayload<'a> {
    descriptor: PayloadDescriptor,
    data: Cow<'a, [u8]>,
}

/// Writes, fsyncs, atomically replaces, reopens, and verifies an archive.
///
/// Success means it is safe for the caller's close state machine to tear down
/// the target. Failure leaves an existing destination untouched whenever the
/// failure occurs before the final same-directory rename.
pub fn write_archive_atomic(path: &Path, input: &ArchiveInput) -> Result<VerifiedArchiveMetadata> {
    write_archive_installed(path, input)?;
    verify_archive_streaming(path)
        .with_context(|| format!("verify newly written archive {}", path.display()))
}

/// Writes and installs an archive exactly as [`write_archive_atomic`] does, then
/// hashes it in one sequential pass instead of structurally re-reading it.
///
/// The checkpoint export path uses this: the target just wrote the ZIP from
/// validated input, and the controller structurally verifies the same bytes
/// after downloading them. Callers that install an archive nothing else will
/// verify must keep using [`write_archive_atomic`].
pub fn write_archive_hashed(path: &Path, input: &ArchiveInput) -> Result<String> {
    write_archive_installed(path, input)?;
    let mut file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    digest_reader(&mut file)
        .with_context(|| format!("hash newly written archive {}", path.display()))
}

fn write_archive_installed(path: &Path, input: &ArchiveInput) -> Result<()> {
    write_archive_installed_with_part_size(path, input, PAYLOAD_PART_BYTES)
}

fn write_archive_installed_with_part_size(
    path: &Path,
    input: &ArchiveInput,
    part_bytes: usize,
) -> Result<()> {
    let (manifest, payloads) = prepare_archive_with_part_size(input, part_bytes)?;
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
    drop(payloads);
    drop(manifest);
    Ok(())
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
    let contents = read_verified_zip(path, PayloadRetention::All)?;
    let mut file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    Ok(VerifiedArchive {
        manifest: contents.manifest,
        payloads: contents.payloads,
        archive_sha256: digest_reader(&mut file)?,
    })
}

/// Verify an archive without materializing repository or native payloads.
/// Every ZIP entry is still fully read, hashed, and checked; Git untracked
/// payloads are parsed through the same path-safety validator while streaming.
///
/// A sharded untracked tar is the one exception to streaming: its parts are
/// held in memory long enough to reassemble and parse the tar, because tar
/// safety is a property of the whole payload rather than of one part.
pub fn verify_archive_streaming(path: &Path) -> Result<VerifiedArchiveMetadata> {
    let contents = read_verified_zip(path, PayloadRetention::CanonicalOnly)?;
    let mut file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    Ok(VerifiedArchiveMetadata {
        manifest: contents.manifest,
        canonical_session: contents.canonical_session,
        archive_sha256: digest_reader(&mut file)?,
    })
}

#[cfg(test)]
fn prepare_archive(input: &ArchiveInput) -> Result<(ArchiveManifest, Vec<PendingPayload<'_>>)> {
    prepare_archive_with_part_size(input, PAYLOAD_PART_BYTES)
}

fn prepare_archive_with_part_size(
    input: &ArchiveInput,
    part_bytes: usize,
) -> Result<(ArchiveManifest, Vec<PendingPayload<'_>>)> {
    ensure!(part_bytes > 0, "archive payload part size is zero");
    ensure!(!input.session.id.trim().is_empty(), "session id is empty");
    ensure!(!input.bundle.id.trim().is_empty(), "bundle id is empty");
    validate_secret_free_map(&input.target.details)?;
    input.canonical_session.validate()?;

    let mut payloads = Vec::new();
    push_payload(
        &mut payloads,
        CANONICAL_SESSION_PATH.to_string(),
        Cow::Owned(
            serde_json::to_vec_pretty(&input.canonical_session)
                .context("serialize canonical session snapshot")?,
        ),
        0o600,
        PayloadRole::CanonicalSession,
    )?;

    for artifact in &input.native_artifacts {
        validate_archive_relative_path(&artifact.relative_path)?;
        ensure_not_secret_path(&artifact.relative_path)?;
        let archive_path = format!("native/{}", slash_path(&artifact.relative_path)?);
        push_payload(
            &mut payloads,
            archive_path,
            Cow::Borrowed(artifact.data.as_slice()),
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
            push_payload(
                &mut payloads,
                path.clone(),
                Cow::Borrowed(data.as_slice()),
                0o600,
                role,
            )?;
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
        payload.descriptor.parts = plan_payload_parts(
            &payload.descriptor.path,
            &payload.data,
            payload_compression(&payload.descriptor.role),
            part_bytes,
        );
    });

    let mut paths = BTreeSet::new();
    for payload in &payloads {
        ensure!(
            paths.insert(payload.descriptor.path.clone()),
            "duplicate archive payload path '{}'",
            payload.descriptor.path
        );
    }
    let sharded = payloads
        .iter()
        .any(|payload| !payload.descriptor.parts.is_empty());
    let manifest = ArchiveManifest {
        schema_version: if sharded {
            ARCHIVE_SCHEMA_VERSION_SHARDED
        } else {
            ARCHIVE_SCHEMA_VERSION
        },
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

fn push_payload<'a>(
    payloads: &mut Vec<PendingPayload<'a>>,
    path: String,
    data: Cow<'a, [u8]>,
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
        parts: Vec::new(),
    };
    payloads.push(PendingPayload { descriptor, data });
    Ok(())
}

/// Git bundles carry packfiles whose objects are already zlib-compressed;
/// re-deflating one saves well under 1% while costing the whole export window,
/// so bundles are stored verbatim. Everything else compresses enough to be
/// worth DEFLATE at the default level.
fn payload_compression(role: &PayloadRole) -> CompressionMethod {
    match role {
        PayloadRole::GitBundle { .. } => CompressionMethod::Stored,
        PayloadRole::CanonicalSession
        | PayloadRole::NativeArtifact { .. }
        | PayloadRole::GitStagedPatch { .. }
        | PayloadRole::GitUnstagedPatch { .. }
        | PayloadRole::GitUntrackedTar { .. } => CompressionMethod::Deflated,
    }
}

fn payload_part_path(path: &str, index: usize) -> String {
    format!("{path}{PAYLOAD_PART_SUFFIX}{index:05}")
}

/// Splits a compressible payload that is larger than `part_bytes` into ordered
/// parts. Stored payloads keep one entry: they cost no compression time, and
/// splitting them would only add entries a reader has to stitch back together.
fn plan_payload_parts(
    path: &str,
    data: &[u8],
    method: CompressionMethod,
    part_bytes: usize,
) -> Vec<PayloadPartDescriptor> {
    if method == CompressionMethod::Stored || data.len() <= part_bytes {
        return Vec::new();
    }
    data.par_chunks(part_bytes)
        .enumerate()
        .map(|(index, chunk)| PayloadPartDescriptor {
            path: payload_part_path(path, index),
            sha256: digest_bytes(chunk),
            size: chunk.len() as u64,
        })
        .collect()
}

/// One ZIP entry to write: either a whole payload, one part of a sharded
/// payload, or the manifest.
struct PlannedEntry<'a> {
    name: &'a str,
    mode: u32,
    method: CompressionMethod,
    data: &'a [u8],
}

fn write_zip(
    output: &mut File,
    manifest: &ArchiveManifest,
    payloads: &[PendingPayload<'_>],
) -> Result<()> {
    let manifest_bytes =
        serde_json::to_vec_pretty(manifest).context("serialize archive manifest")?;
    ensure!(
        manifest_bytes.len() as u64 <= MAX_MANIFEST_BYTES,
        "archive manifest is too large"
    );
    let mut entries = vec![PlannedEntry {
        name: MANIFEST_PATH,
        mode: 0o600,
        method: CompressionMethod::Deflated,
        data: &manifest_bytes,
    }];
    for payload in payloads {
        let descriptor = &payload.descriptor;
        let method = payload_compression(&descriptor.role);
        if descriptor.parts.is_empty() {
            entries.push(PlannedEntry {
                name: &descriptor.path,
                mode: descriptor.mode,
                method,
                data: &payload.data,
            });
            continue;
        }
        let mut offset = 0_usize;
        for part in &descriptor.parts {
            let end = usize::try_from(part.size)
                .ok()
                .and_then(|size| offset.checked_add(size))
                .filter(|end| *end <= payload.data.len())
                .ok_or_else(|| {
                    anyhow!("payload '{}' parts do not fit its body", descriptor.path)
                })?;
            entries.push(PlannedEntry {
                name: &part.path,
                mode: descriptor.mode,
                method,
                data: &payload.data[offset..end],
            });
            offset = end;
        }
        ensure!(
            offset == payload.data.len(),
            "payload '{}' parts do not cover its body",
            descriptor.path
        );
    }

    // Compression is the export freeze window, so every entry deflates on its
    // own core; the container is then assembled sequentially in plan order so
    // the archive layout stays deterministic.
    let compressed = entries
        .par_iter()
        .map(compress_entry)
        .collect::<Result<Vec<_>>>()?;

    let mut writer = zip::ZipWriter::new(output);
    for (entry, buffer) in entries.iter().zip(compressed) {
        let mut source = zip::ZipArchive::new(Cursor::new(buffer))
            .with_context(|| format!("reopen compressed ZIP entry '{}'", entry.name))?;
        let compressed_entry = source
            .by_index(0)
            .with_context(|| format!("read compressed ZIP entry '{}'", entry.name))?;
        writer
            .raw_copy_file(compressed_entry)
            .with_context(|| format!("write ZIP entry '{}'", entry.name))?;
    }
    writer.finish().context("finish Hel archive ZIP")?;
    Ok(())
}

/// Compresses one entry into a single-entry ZIP so the assembly pass can copy
/// the finished deflate stream verbatim with [`zip::ZipWriter::raw_copy_file`].
fn compress_entry(entry: &PlannedEntry<'_>) -> Result<Vec<u8>> {
    let mut buffer = Cursor::new(Vec::with_capacity(entry.data.len() / 2 + 512));
    let mut writer = zip::ZipWriter::new(&mut buffer);
    writer
        .start_file(
            entry.name,
            SimpleFileOptions::default()
                .compression_method(entry.method)
                .unix_permissions(entry.mode)
                .large_file(entry.data.len() as u64 > zip::ZIP64_BYTES_THR),
        )
        .with_context(|| format!("start ZIP entry '{}'", entry.name))?;
    writer
        .write_all(entry.data)
        .with_context(|| format!("write ZIP entry '{}'", entry.name))?;
    writer
        .finish()
        .with_context(|| format!("compress ZIP entry '{}'", entry.name))?;
    Ok(buffer.into_inner())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PayloadRetention {
    All,
    CanonicalOnly,
}

struct VerifiedZipContents {
    manifest: ArchiveManifest,
    canonical_session: CanonicalSessionSnapshot,
    payloads: BTreeMap<String, Vec<u8>>,
}

/// Structural facts about one ZIP entry, read from the central directory
/// before any body is decompressed.
struct ZipEntryMeta {
    index: usize,
    name: String,
    size: u64,
    mode: u32,
}

/// What the manifest says a ZIP entry must contain.
#[derive(Clone, Copy)]
enum EntryExpectation<'a> {
    Whole(&'a PayloadDescriptor),
    Part {
        payload: &'a PayloadDescriptor,
        part: &'a PayloadPartDescriptor,
    },
}

/// Each parallel reader owns one of these: ZIP entries can only be read one at
/// a time through a single handle, and the deflate decoder already buffers.
fn open_archive(path: &Path) -> Result<zip::ZipArchive<File>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    zip::ZipArchive::new(file).context("open Hel archive ZIP")
}

/// Payload bodies the caller keeps.
fn retains_payload(retention: PayloadRetention, role: &PayloadRole) -> bool {
    retention == PayloadRetention::All || *role == PayloadRole::CanonicalSession
}

/// Part bodies the reader has to hold until reassembly. Tar safety is a
/// property of the whole payload, so a sharded untracked tar is reassembled
/// even when the caller does not want the bytes.
fn retains_parts(retention: PayloadRetention, role: &PayloadRole) -> bool {
    retains_payload(retention, role) || matches!(role, PayloadRole::GitUntrackedTar { .. })
}

fn read_verified_zip(path: &Path, retention: PayloadRetention) -> Result<VerifiedZipContents> {
    let mut archive = open_archive(path)?;
    let mut entries = Vec::with_capacity(archive.len());
    let mut actual_paths = BTreeSet::<String>::new();
    let mut manifest_count = 0_usize;
    let mut total_size = 0_u64;
    for index in 0..archive.len() {
        let entry = archive
            .by_index_raw(index)
            .with_context(|| format!("read ZIP entry metadata {index}"))?;
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
        ensure!(
            actual_paths.insert(name.clone()),
            "duplicate ZIP entry '{name}'"
        );
        manifest_count += usize::from(name == MANIFEST_PATH);
        entries.push(ZipEntryMeta {
            index,
            name,
            size: entry.size(),
            mode: entry.unix_mode().unwrap_or(0o600) & 0o7777,
        });
    }
    ensure!(
        manifest_count == 1,
        "archive must contain exactly one {MANIFEST_PATH}"
    );
    let manifest_bytes = {
        let mut entry = archive
            .by_name(MANIFEST_PATH)
            .with_context(|| format!("archive is missing {MANIFEST_PATH}"))?;
        ensure!(!entry.is_dir(), "archive manifest is a directory entry");
        ensure!(
            entry.size() <= MAX_MANIFEST_BYTES,
            "archive manifest is too large"
        );
        let mut bytes = Vec::with_capacity(entry.size().min(usize::MAX as u64) as usize);
        entry
            .read_to_end(&mut bytes)
            .context("read archive manifest")?;
        bytes
    };
    drop(archive);
    let manifest = parse_archive_manifest(&manifest_bytes)?;

    let mut expectations = BTreeMap::<&str, EntryExpectation<'_>>::new();
    for descriptor in &manifest.payloads {
        if descriptor.parts.is_empty() {
            expectations.insert(
                descriptor.path.as_str(),
                EntryExpectation::Whole(descriptor),
            );
            continue;
        }
        for part in &descriptor.parts {
            expectations.insert(
                part.path.as_str(),
                EntryExpectation::Part {
                    payload: descriptor,
                    part,
                },
            );
        }
    }

    let mut payload_entries = Vec::with_capacity(entries.len());
    for meta in &entries {
        if meta.name == MANIFEST_PATH {
            ensure!(
                meta.size == manifest_bytes.len() as u64,
                "archive manifest size changed while it was read"
            );
            continue;
        }
        let expectation = expectations
            .get(meta.name.as_str())
            .ok_or_else(|| anyhow!("archive contains unlisted payload '{}'", meta.name))?;
        payload_entries.push((meta, *expectation));
    }
    // Entry names are unique and every one of them is listed, so equal counts
    // mean the manifest and the container describe the same entry set.
    ensure!(
        payload_entries.len() == expectations.len(),
        "archive payload list does not match manifest"
    );

    // DEFLATE cannot be inflated in parallel inside one stream, so read
    // parallelism comes from entries: each worker owns its own archive handle
    // and verifies whole entries end to end.
    let outcomes = payload_entries
        .par_iter()
        .map_init(
            || open_archive(path).map_err(|error| format!("{error:#}")),
            |archive, (meta, expectation)| {
                let archive = match archive {
                    Ok(archive) => archive,
                    Err(error) => bail!("{error}"),
                };
                read_verified_entry(archive, meta, *expectation, retention)
            },
        )
        .collect::<Vec<_>>();
    let mut bodies = BTreeMap::<&str, Vec<u8>>::new();
    for ((meta, _), outcome) in payload_entries.iter().zip(outcomes) {
        if let Some(bytes) = outcome? {
            bodies.insert(meta.name.as_str(), bytes);
        }
    }

    let mut payloads = BTreeMap::new();
    let mut canonical_session = None;
    for descriptor in &manifest.payloads {
        let bytes = reassemble_payload(descriptor, &mut bodies, retention)?;
        if matches!(descriptor.role, PayloadRole::GitUntrackedTar { .. })
            && let Some(bytes) = bytes.as_deref()
        {
            validate_untracked_tar(bytes)
                .with_context(|| format!("validate payload '{}'", descriptor.path))?;
        }
        if descriptor.role == PayloadRole::CanonicalSession {
            let bytes = bytes
                .as_deref()
                .expect("canonical session payload is always retained");
            let snapshot: CanonicalSessionSnapshot =
                serde_json::from_slice(bytes).context("parse canonical session snapshot")?;
            snapshot.validate()?;
            ensure!(
                canonical_session.replace(snapshot).is_none(),
                "archive contains duplicate canonical session payloads"
            );
        }
        if retention == PayloadRetention::All {
            payloads.insert(
                descriptor.path.clone(),
                bytes.expect("all payloads are retained by the full reader"),
            );
        }
    }
    Ok(VerifiedZipContents {
        manifest,
        canonical_session: canonical_session.context("archive canonical session is missing")?,
        payloads,
    })
}

/// Reads one ZIP entry, verifies it against the manifest, and returns its body
/// when the caller needs it.
fn read_verified_entry(
    archive: &mut zip::ZipArchive<File>,
    meta: &ZipEntryMeta,
    expectation: EntryExpectation<'_>,
    retention: PayloadRetention,
) -> Result<Option<Vec<u8>>> {
    let (payload, expected_size, expected_sha256, retain) = match expectation {
        EntryExpectation::Whole(payload) => (
            payload,
            payload.size,
            payload.sha256.as_str(),
            retains_payload(retention, &payload.role),
        ),
        EntryExpectation::Part { payload, part } => (
            payload,
            part.size,
            part.sha256.as_str(),
            retains_parts(retention, &payload.role),
        ),
    };
    let label = match expectation {
        EntryExpectation::Whole(_) => format!("payload '{}'", payload.path),
        EntryExpectation::Part { .. } => format!("payload part '{}'", meta.name),
    };
    ensure!(meta.size == expected_size, "size mismatch for {label}");
    ensure!(meta.mode == payload.mode, "mode mismatch for {label}");
    // Only a whole untracked tar can be parsed while it streams past; sharded
    // ones are reassembled after every part is verified.
    let stream_untracked_tar = !retain
        && matches!(expectation, EntryExpectation::Whole(_))
        && matches!(payload.role, PayloadRole::GitUntrackedTar { .. });

    let name = meta.name.as_str();
    let mut entry = archive
        .by_index(meta.index)
        .with_context(|| format!("read ZIP entry {}", meta.index))?;
    // Every worker opens the path again, so confirm this handle still sees the
    // entry the structural pass indexed instead of reporting a replaced file
    // as payload corruption.
    let enclosed = entry
        .enclosed_name()
        .ok_or_else(|| anyhow!("unsafe ZIP entry path '{}'", entry.name()))?;
    ensure!(
        slash_path(&enclosed)? == meta.name,
        "ZIP entry {} changed while the archive was read",
        meta.index
    );
    let mut bytes =
        retain.then(|| Vec::with_capacity(expected_size.min(usize::MAX as u64) as usize));
    let mut digesting = DigestingReader::new(&mut entry);
    if let Some(bytes) = bytes.as_mut() {
        digesting
            .read_to_end(bytes)
            .with_context(|| format!("read ZIP entry '{name}'"))?;
    } else if stream_untracked_tar {
        validate_untracked_tar_reader(&mut digesting)
            .with_context(|| format!("validate payload '{}'", payload.path))?;
        std::io::copy(&mut digesting, &mut std::io::sink())
            .with_context(|| format!("finish reading ZIP entry '{name}'"))?;
    } else {
        std::io::copy(&mut digesting, &mut std::io::sink())
            .with_context(|| format!("read ZIP entry '{name}'"))?;
    }
    let (actual_size, actual_digest) = digesting.finish();
    ensure!(actual_size == expected_size, "size mismatch for {label}");
    ensure!(
        actual_digest == expected_sha256,
        "SHA-256 mismatch for {label}"
    );
    Ok(bytes)
}

/// Turns verified entry bodies back into one payload body. Restore and import
/// therefore never see part entries, whatever the archive layout is.
fn reassemble_payload(
    descriptor: &PayloadDescriptor,
    bodies: &mut BTreeMap<&str, Vec<u8>>,
    retention: PayloadRetention,
) -> Result<Option<Vec<u8>>> {
    if descriptor.parts.is_empty() {
        return Ok(bodies.remove(descriptor.path.as_str()));
    }
    if !retains_parts(retention, &descriptor.role) {
        return Ok(None);
    }
    let mut assembled = Vec::with_capacity(descriptor.size.min(usize::MAX as u64) as usize);
    for part in &descriptor.parts {
        let chunk = bodies.remove(part.path.as_str()).ok_or_else(|| {
            anyhow!(
                "payload '{}' is missing part '{}'",
                descriptor.path,
                part.path
            )
        })?;
        assembled.extend_from_slice(&chunk);
    }
    ensure!(
        assembled.len() as u64 == descriptor.size,
        "size mismatch for payload '{}'",
        descriptor.path
    );
    ensure!(
        digest_bytes(&assembled) == descriptor.sha256,
        "SHA-256 mismatch for payload '{}'",
        descriptor.path
    );
    Ok(Some(assembled))
}

struct DigestingReader<R> {
    inner: R,
    digest: Sha256,
    bytes_read: u64,
}

impl<R> DigestingReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            digest: Sha256::new(),
            bytes_read: 0,
        }
    }

    fn finish(self) -> (u64, String) {
        (self.bytes_read, format!("{:x}", self.digest.finalize()))
    }
}

impl<R: Read> Read for DigestingReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buffer)?;
        self.bytes_read = self.bytes_read.saturating_add(read as u64);
        self.digest.update(&buffer[..read]);
        Ok(read)
    }
}

fn parse_archive_manifest(manifest_bytes: &[u8]) -> Result<ArchiveManifest> {
    #[derive(Deserialize)]
    struct ArchiveHeader {
        schema_version: u32,
        format: String,
    }
    let header: ArchiveHeader =
        serde_json::from_slice(manifest_bytes).context("parse archive manifest header")?;
    ensure!(
        header.format == ARCHIVE_FORMAT,
        "unsupported archive format '{}'",
        header.format
    );
    ensure!(
        header.schema_version == ARCHIVE_SCHEMA_VERSION
            || header.schema_version == ARCHIVE_SCHEMA_VERSION_SHARDED,
        "incompatible Hel archive schema {}; this build requires schema {}",
        header.schema_version,
        ARCHIVE_SCHEMA_VERSION
    );
    let manifest: ArchiveManifest =
        serde_json::from_slice(manifest_bytes).context("parse Hel archive manifest")?;
    validate_manifest(&manifest)?;
    Ok(manifest)
}

/// The schema an archive must declare for the payload layout it carries.
/// Sharded payloads are exactly what schema 3 adds, so declaring the wrong
/// version is a manifest error either way round.
fn expected_schema_version(payloads: &[PayloadDescriptor]) -> u32 {
    if payloads.iter().any(|payload| !payload.parts.is_empty()) {
        ARCHIVE_SCHEMA_VERSION_SHARDED
    } else {
        ARCHIVE_SCHEMA_VERSION
    }
}

fn validate_manifest(manifest: &ArchiveManifest) -> Result<()> {
    let expected_schema = expected_schema_version(&manifest.payloads);
    ensure!(
        manifest.schema_version == expected_schema,
        "incompatible Hel archive schema {}; this build requires schema {}",
        manifest.schema_version,
        expected_schema
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
            paths.insert(descriptor.path.as_str()),
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
        validate_payload_parts(descriptor, &mut paths)?;
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
        .filter(|payload| payload.role == PayloadRole::CanonicalSession)
        .count();
    ensure!(
        canonical_count == 1,
        "archive must contain exactly one canonical session payload"
    );
    ensure!(
        manifest.payloads.iter().any(|payload| {
            payload.role == PayloadRole::CanonicalSession && payload.path == CANONICAL_SESSION_PATH
        }),
        "canonical session payload must be stored at {CANONICAL_SESSION_PATH}"
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
            PayloadRole::CanonicalSession | PayloadRole::NativeArtifact { .. } => None,
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

/// Checks that a sharded payload can be reassembled exactly, and that its part
/// entries occupy names nothing else in the archive claims. Every failure here
/// is loud: a missing, extra, renamed, or reordered part cannot be read as a
/// silently truncated or scrambled payload.
fn validate_payload_parts<'a>(
    descriptor: &'a PayloadDescriptor,
    paths: &mut BTreeSet<&'a str>,
) -> Result<()> {
    if descriptor.parts.is_empty() {
        return Ok(());
    }
    ensure!(
        descriptor.parts.len() > 1,
        "payload '{}' is sharded into a single part",
        descriptor.path
    );
    let mut covered = 0_u64;
    for (index, part) in descriptor.parts.iter().enumerate() {
        validate_archive_relative_path(Path::new(&part.path))?;
        ensure!(
            part.path == payload_part_path(&descriptor.path, index),
            "payload '{}' part {index} has unexpected path '{}'",
            descriptor.path,
            part.path
        );
        ensure!(
            paths.insert(part.path.as_str()),
            "duplicate manifest payload '{}'",
            part.path
        );
        ensure!(
            part.size > 0,
            "payload '{}' part {index} is empty",
            descriptor.path
        );
        ensure!(
            is_lower_hex_sha256(&part.sha256),
            "invalid SHA-256 for payload part '{}'",
            part.path
        );
        covered = covered
            .checked_add(part.size)
            .ok_or_else(|| anyhow!("payload '{}' part sizes overflow", descriptor.path))?;
    }
    ensure!(
        covered == descriptor.size,
        "payload '{}' parts cover {covered} bytes but the payload is {} bytes",
        descriptor.path,
        descriptor.size
    );
    Ok(())
}

fn validate_canonical_session(snapshot: &CanonicalSessionSnapshot) -> Result<()> {
    ensure!(
        is_lower_hex_sha256(&snapshot.event_frontier_digest),
        "canonical event frontier digest must be 64 lowercase hexadecimal characters"
    );
    ensure!(
        (snapshot.event_frontier == 0)
            == (snapshot.event_frontier_digest == EVENT_FRONTIER_GENESIS_DIGEST),
        "canonical event frontier and digest are inconsistent"
    );
    ensure!(
        (snapshot.event_frontier == 0) == snapshot.session.last_activity_at_ms.is_none(),
        "canonical event frontier and activity watermark are inconsistent"
    );
    ensure!(
        snapshot.session.execution == CanonicalExecutionState::Idle,
        "canonical session is not idle at the checkpoint barrier"
    );
    ensure!(
        snapshot
            .session
            .session_title
            .as_ref()
            .is_none_or(|title| !title.trim().is_empty()),
        "canonical session title is empty"
    );
    let mut item_ids = BTreeSet::new();
    let mut previous_position = 0_u64;
    for item in &snapshot.transcript {
        ensure!(
            !item.stable_id.trim().is_empty(),
            "canonical transcript item id is empty"
        );
        ensure!(
            item_ids.insert(item.stable_id.as_str()),
            "duplicate canonical transcript item id '{}'",
            item.stable_id
        );
        ensure!(
            item.position > 0,
            "canonical transcript item '{}' has zero position",
            item.stable_id
        );
        ensure!(
            item.position >= previous_position,
            "canonical transcript items are out of position order"
        );
        ensure!(
            item.position <= snapshot.event_frontier,
            "canonical transcript item '{}' is beyond event frontier {}",
            item.stable_id,
            snapshot.event_frontier
        );
        match (&item.body, item.latest_content_event_ordinal) {
            (CanonicalTranscriptBody::Agent { .. }, Some(ordinal)) => ensure!(
                ordinal >= item.position && ordinal <= snapshot.event_frontier,
                "canonical agent message '{}' has invalid latest content ordinal {ordinal}",
                item.stable_id
            ),
            (CanonicalTranscriptBody::Agent { .. }, None) => bail!(
                "canonical agent message '{}' has no latest content ordinal",
                item.stable_id
            ),
            (_, Some(ordinal)) => bail!(
                "canonical non-agent transcript item '{}' has latest content ordinal {ordinal}",
                item.stable_id
            ),
            (_, None) => {}
        }
        ensure!(
            item.last_changed_at_ms >= item.created_at_ms,
            "canonical transcript item '{}' changed before it was created",
            item.stable_id
        );
        ensure!(
            !matches!(
                &item.body,
                CanonicalTranscriptBody::Agent {
                    streaming: true,
                    ..
                } | CanonicalTranscriptBody::Thought {
                    streaming: true,
                    ..
                }
            ),
            "canonical transcript item '{}' is still streaming at the checkpoint barrier",
            item.stable_id
        );
        match &item.body {
            CanonicalTranscriptBody::User { content } => {
                for (index, block) in content.iter().enumerate() {
                    serde_json::from_value::<agent_client_protocol::schema::v1::ContentBlock>(
                        block.clone(),
                    )
                    .with_context(|| {
                        format!(
                            "canonical transcript item '{}' has invalid ACP content block {index}",
                            item.stable_id
                        )
                    })?;
                }
            }
            CanonicalTranscriptBody::Agent { chunks, .. }
            | CanonicalTranscriptBody::Thought { chunks, .. } => {
                for (index, chunk) in chunks.iter().enumerate() {
                    serde_json::from_value::<agent_client_protocol::schema::v1::ContentChunk>(
                        chunk.clone(),
                    )
                    .with_context(|| {
                        format!(
                            "canonical transcript item '{}' has invalid ACP content chunk {index}",
                            item.stable_id
                        )
                    })?;
                }
            }
            CanonicalTranscriptBody::Tool { call } => {
                serde_json::from_value::<agent_client_protocol::schema::v1::ToolCall>(call.clone())
                    .with_context(|| {
                        format!(
                            "canonical transcript item '{}' has invalid ACP tool call",
                            item.stable_id
                        )
                    })?;
            }
            CanonicalTranscriptBody::Plan { plan } => {
                serde_json::from_value::<agent_client_protocol::schema::v1::Plan>(plan.clone())
                    .with_context(|| {
                        format!(
                            "canonical transcript item '{}' has invalid ACP plan",
                            item.stable_id
                        )
                    })?;
            }
            CanonicalTranscriptBody::System { .. } => {}
        }
        previous_position = item.position;
    }

    let mut queue_ids = BTreeSet::new();
    for prompt in &snapshot.queued_prompts {
        ensure!(
            !prompt.command_id.trim().is_empty(),
            "canonical queued prompt id is empty"
        );
        ensure!(
            queue_ids.insert(prompt.command_id.as_str()),
            "duplicate canonical queued prompt id '{}'",
            prompt.command_id
        );
        ensure!(
            !prompt.content.is_empty(),
            "canonical queued prompt '{}' has no content",
            prompt.command_id
        );
        if let CanonicalQueuedCommandKind::SetConfig { key, value } = &prompt.kind {
            ensure!(
                !key.trim().is_empty() && !value.trim().is_empty(),
                "canonical queued configuration change '{}' is incomplete",
                prompt.command_id
            );
        }
        for (index, content) in prompt.content.iter().enumerate() {
            serde_json::from_value::<agent_client_protocol::schema::v1::ContentBlock>(
                content.clone(),
            )
            .with_context(|| {
                format!(
                    "canonical queued prompt '{}' has invalid ACP content block {index}",
                    prompt.command_id
                )
            })?;
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

/// How much committed history a snapshot has to carry. Committed work that is
/// already reachable from an origin ref is durable at its source, so it is
/// never bundled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GitHistoryMode {
    /// Bundle commits reachable from HEAD but from no `refs/remotes/origin/*`
    /// ref. Errors when the repository has no origin refs at all.
    SessionDelta,
    /// Bundle commits since `merge-base(HEAD, rev)`. Errors when the revision
    /// or the merge base cannot be resolved.
    DeltaFrom(String),
    /// No committed bundle: origin serves all committed history. Dirty state
    /// and identity are still collected.
    NoBundle,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitCollectionSpec {
    pub id: String,
    pub relative_destination: PathBuf,
    pub history: GitHistoryMode,
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

    let identity = collect_git_identity(runner, repository, spec.origin_override.as_deref())?;
    let history = select_git_history(runner, repository, &spec.history, &identity.head_commit)?;
    collect_git_contents(
        runner,
        repository,
        spec,
        identity,
        history,
        include_untracked,
        progress,
    )
}

/// Collect only enough Git identity to associate native harness state with an
/// existing project. The project itself remains the recovery source.
pub fn collect_git_metadata_snapshot(
    runner: &dyn GitCommandRunner,
    repository: &Path,
    spec: &GitCollectionSpec,
) -> Result<RepositorySnapshot> {
    validate_component(&spec.id, "repository id")?;
    validate_archive_relative_path(&spec.relative_destination)?;
    let identity = collect_git_identity(runner, repository, spec.origin_override.as_deref())?;
    Ok(RepositorySnapshot {
        metadata: RepositoryMetadata {
            id: spec.id.clone(),
            relative_destination: spec.relative_destination.clone(),
            origin: identity.origin,
            base_commit: identity.head_commit.clone(),
            head_commit: identity.head_commit,
            branch: identity.branch,
        },
        committed_bundle: Vec::new(),
        staged_patch: Vec::new(),
        unstaged_patch: Vec::new(),
        untracked_tar: Vec::new(),
    })
}

struct CollectedGitIdentity {
    origin: String,
    head_commit: String,
    branch: Option<String>,
}

fn collect_git_identity(
    runner: &dyn GitCommandRunner,
    repository: &Path,
    origin_override: Option<&str>,
) -> Result<CollectedGitIdentity> {
    let head_commit = git_text(runner, repository, ["rev-parse", "--verify", "HEAD"])
        .context("repository has no valid Git HEAD")?;
    let origin = if let Some(origin) = origin_override {
        origin.to_owned()
    } else {
        let output = run_git(runner, repository, ["remote", "get-url", "origin"], &[])?;
        if output.status == 0 {
            redact_origin_credentials(&trim_output(&output.stdout, "read Git origin")?)?
        } else {
            String::new()
        }
    };
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
    Ok(CollectedGitIdentity {
        origin,
        head_commit,
        branch,
    })
}

fn merge_base(
    runner: &dyn GitCommandRunner,
    repository: &Path,
    base_revision: &str,
) -> Result<Option<String>> {
    let base = run_git(
        runner,
        repository,
        ["rev-parse", "--verify", base_revision],
        &[],
    )?;
    if base.status != 0 {
        return Ok(None);
    }
    let base = trim_output(&base.stdout, "decode Git base revision")?;
    if base.is_empty() {
        return Ok(None);
    }
    let merged = run_git(runner, repository, ["merge-base", "HEAD", &base], &[])?;
    match merged.status {
        0 => {
            let merged = trim_output(&merged.stdout, "decode Git merge base")?;
            Ok((!merged.is_empty()).then_some(merged))
        }
        1 => Ok(None),
        _ => Err(git_failure("find Git merge base", &merged)),
    }
}

/// True when the repository has at least one `refs/remotes/origin/*` ref, the
/// exclusion set every session delta is measured against.
pub fn has_origin_refs(runner: &dyn GitCommandRunner, repository: &Path) -> Result<bool> {
    let refs = git_text(
        runner,
        repository,
        [
            "for-each-ref",
            "--format=%(objectname)",
            "refs/remotes/origin",
        ],
    )
    .context("list origin refs")?;
    Ok(!refs.is_empty())
}

struct GitHistorySelection {
    /// Informational only; an empty string for session deltas.
    base_commit: String,
    /// Arguments for the bundle command, or None when nothing has to be sent.
    bundle_arguments: Option<Vec<String>>,
}

fn select_git_history(
    runner: &dyn GitCommandRunner,
    repository: &Path,
    mode: &GitHistoryMode,
    head_commit: &str,
) -> Result<GitHistorySelection> {
    match mode {
        GitHistoryMode::SessionDelta => {
            // Collection stays side-effect free; callers repair missing origin
            // refs before asking for a session delta.
            ensure!(
                has_origin_refs(runner, repository)?,
                "repository has no origin refs to delta against"
            );
            let count = git_text(
                runner,
                repository,
                ["rev-list", "--count", "HEAD", "--not", "--remotes=origin"],
            )?
            .parse::<u64>()
            .context("parse committed delta count")?;
            Ok(GitHistorySelection {
                base_commit: String::new(),
                bundle_arguments: (count > 0).then(|| {
                    ["bundle", "create", "-", "HEAD", "--not", "--remotes=origin"]
                        .map(String::from)
                        .to_vec()
                }),
            })
        }
        GitHistoryMode::DeltaFrom(revision) => {
            let base_commit = merge_base(runner, repository, revision)?
                .with_context(|| format!("delta base {revision} is unresolvable"))?;
            let count = git_text(
                runner,
                repository,
                ["rev-list", "--count", &format!("{base_commit}..HEAD")],
            )?
            .parse::<u64>()
            .context("parse committed delta count")?;
            let bundle_arguments = (count > 0).then(|| {
                vec![
                    "bundle".to_owned(),
                    "create".to_owned(),
                    "-".to_owned(),
                    "HEAD".to_owned(),
                    format!("^{base_commit}"),
                ]
            });
            Ok(GitHistorySelection {
                base_commit,
                bundle_arguments,
            })
        }
        GitHistoryMode::NoBundle => Ok(GitHistorySelection {
            base_commit: head_commit.to_owned(),
            bundle_arguments: None,
        }),
    }
}

fn collect_git_contents(
    runner: &dyn GitCommandRunner,
    repository: &Path,
    spec: &GitCollectionSpec,
    identity: CollectedGitIdentity,
    history: GitHistorySelection,
    include_untracked: bool,
    progress: &(dyn Fn(GitSnapshotProgress) -> Result<()> + Sync),
) -> Result<RepositorySnapshot> {
    let GitHistorySelection {
        base_commit,
        bundle_arguments,
    } = history;
    // These commands only inspect repository state and produce independent
    // payloads. Nested joins share Rayon's bounded worker pool, including when
    // several repositories are being collected at once.
    let ((committed_bundle, staged_patch), (unstaged_patch, untracked_tar)) = rayon::join(
        || {
            rayon::join(
                || match &bundle_arguments {
                    Some(arguments) => git_bytes_owned(
                        runner,
                        repository,
                        arguments,
                        "create committed delta bundle",
                    ),
                    None => Ok(Vec::new()),
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
            origin: identity.origin,
            base_commit,
            head_commit: identity.head_commit,
            branch: identity.branch,
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
        )
        .with_context(|| checkout_advice(snapshot))?;
    } else {
        git_bytes(
            runner,
            repository,
            ["checkout", "--detach", checkout_target],
            &[],
            "restore detached commit",
        )
        .with_context(|| checkout_advice(snapshot))?;
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

/// A snapshot without a bundle relies on origin for its committed history, so
/// name the missing commit and how to make it reachable again.
fn checkout_advice(snapshot: &RepositorySnapshot) -> String {
    let head_commit = &snapshot.metadata.head_commit;
    if snapshot.committed_bundle.is_empty() {
        format!(
            "restore commit {head_commit}: the archive carries no committed bundle, so this commit must be reachable from the repository's origin; fetch the origin ref that contains it and retry"
        )
    } else {
        format!("restore commit {head_commit}")
    }
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

fn git_bytes_owned(
    runner: &dyn GitCommandRunner,
    repository: &Path,
    arguments: &[String],
    action: &str,
) -> Result<Vec<u8>> {
    let output = runner.run(
        repository,
        &GitCommand {
            arguments: arguments.iter().map(OsString::from).collect(),
            stdin: Vec::new(),
        },
    )?;
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
    validate_untracked_tar_reader(Cursor::new(bytes))
}

fn validate_untracked_tar_reader(reader: impl Read) -> Result<()> {
    let mut archive = tar::Archive::new(reader);
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
    use std::io::{Seek, SeekFrom};
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
                Some("merge-base") => format!("{}\n", "b".repeat(40)).into_bytes(),
                Some("for-each-ref") => format!("{}\n", "c".repeat(40)).into_bytes(),
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

    fn git_line(repository: &Path, arguments: &[&str]) -> String {
        String::from_utf8(git(repository, arguments))
            .unwrap()
            .trim()
            .to_owned()
    }

    fn initialize_repository(path: &Path) {
        fs::create_dir_all(path).unwrap();
        git(path, &["init", "-q", "-b", "main"]);
        git(path, &["config", "user.name", "Hel Test"]);
        git(path, &["config", "user.email", "hel@example.test"]);
    }

    fn clone_repository(parent: &Path, origin: &Path, name: &str) -> PathBuf {
        let destination = parent.join(name);
        git(
            parent,
            &[
                "clone",
                "-q",
                origin.to_str().unwrap(),
                destination.to_str().unwrap(),
            ],
        );
        git(&destination, &["config", "user.name", "Hel Test"]);
        git(&destination, &["config", "user.email", "hel@example.test"]);
        destination
    }

    fn commit_file(repository: &Path, name: &str, contents: &[u8], message: &str) {
        fs::write(repository.join(name), contents).unwrap();
        git(repository, &["add", "."]);
        git(repository, &["commit", "-qm", message]);
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
                relay_version: "0.1.0".into(),
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
            canonical_session: CanonicalSessionSnapshot {
                event_frontier: 4,
                event_frontier_digest: "a".repeat(64),
                session: CanonicalSessionState {
                    execution: CanonicalExecutionState::Idle,
                    last_activity_at_ms: Some(104),
                    session_title: Some("Forge Hel".into()),
                    configuration: BTreeMap::from([(
                        "reasoning_effort".into(),
                        serde_json::json!("high"),
                    )]),
                },
                transcript: vec![
                    CanonicalTranscriptItem {
                        stable_id: "user-1".into(),
                        position: 1,
                        latest_content_event_ordinal: None,
                        created_at_ms: 100,
                        last_changed_at_ms: 100,
                        body: CanonicalTranscriptBody::User {
                            content: vec![serde_json::json!({"type": "text", "text": "hello"})],
                        },
                    },
                    CanonicalTranscriptItem {
                        stable_id: "agent-2".into(),
                        position: 2,
                        latest_content_event_ordinal: Some(2),
                        created_at_ms: 101,
                        last_changed_at_ms: 103,
                        body: CanonicalTranscriptBody::Agent {
                            chunks: vec![serde_json::json!({
                                "content": {"type": "text", "text": "hi"}
                            })],
                            streaming: false,
                        },
                    },
                ],
                queued_prompts: vec![CanonicalQueuedPrompt {
                    command_id: "prompt-4".into(),
                    kind: CanonicalQueuedCommandKind::Prompt,
                    content: vec![serde_json::json!({"type": "text", "text": "next"})],
                    queued_at_ms: 104,
                }],
            },
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
        assert_eq!(verified.canonical_session, input().canonical_session);
        assert_eq!(verified.manifest.schema_version, ARCHIVE_SCHEMA_VERSION);
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
    fn archive_preparation_borrows_existing_payload_bodies() {
        let archive_input = input();
        let native = archive_input.native_artifacts[0].data.as_slice();
        let bundle = archive_input.repositories[0].committed_bundle.as_slice();
        let (_manifest, payloads) = prepare_archive(&archive_input).unwrap();

        let canonical = payloads
            .iter()
            .find(|payload| payload.descriptor.role == PayloadRole::CanonicalSession)
            .unwrap();
        assert!(matches!(&canonical.data, Cow::Owned(_)));

        let prepared_native = payloads
            .iter()
            .find(|payload| matches!(&payload.descriptor.role, PayloadRole::NativeArtifact { .. }))
            .unwrap();
        assert!(matches!(&prepared_native.data, Cow::Borrowed(_)));
        assert_eq!(prepared_native.data.as_ptr(), native.as_ptr());

        let prepared_bundle = payloads
            .iter()
            .find(|payload| {
                matches!(
                    &payload.descriptor.role,
                    PayloadRole::GitBundle { repository_id } if repository_id == "hel"
                )
            })
            .unwrap();
        assert!(matches!(&prepared_bundle.data, Cow::Borrowed(_)));
        assert_eq!(prepared_bundle.data.as_ptr(), bundle.as_ptr());
    }

    #[test]
    fn streaming_verification_does_not_retain_large_noncanonical_payloads() {
        const LARGE_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("large.hel.zip");
        let mut archive_input = input();
        archive_input.repositories.clear();
        archive_input.native_artifacts = vec![NativeArtifact {
            relative_path: PathBuf::from("sessions/native-1/large-rollout.jsonl"),
            data: vec![b'x'; LARGE_PAYLOAD_BYTES],
            mode: 0o600,
        }];

        let verified = write_archive_atomic(&path, &archive_input).unwrap();
        drop(archive_input);

        let native = verified
            .manifest
            .payloads
            .iter()
            .find(|payload| matches!(payload.role, PayloadRole::NativeArtifact { .. }))
            .unwrap();
        assert_eq!(native.size, LARGE_PAYLOAD_BYTES as u64);
        assert_eq!(verified.canonical_session, input().canonical_session);
        let retained_metadata_bytes = serde_json::to_vec(&verified.manifest).unwrap().len()
            + serde_json::to_vec(&verified.canonical_session)
                .unwrap()
                .len()
            + verified.archive_sha256.len();
        assert!(retained_metadata_bytes < LARGE_PAYLOAD_BYTES / 100);
    }

    const TEST_PART_BYTES: usize = 4096;

    fn zip_entry_names(path: &Path) -> Vec<String> {
        let archive = zip::ZipArchive::new(File::open(path).unwrap()).unwrap();
        archive.file_names().map(str::to_owned).collect()
    }

    fn zip_entry_method(path: &Path, name: &str) -> CompressionMethod {
        let mut archive = zip::ZipArchive::new(File::open(path).unwrap()).unwrap();
        archive.by_name(name).unwrap().compression()
    }

    /// Writes an archive whose native artifact and first untracked tar are both
    /// larger than `TEST_PART_BYTES`, so the sharded paths are exercised without
    /// allocating the production 16 MiB threshold.
    fn sharded_input() -> (ArchiveInput, Vec<u8>, Vec<u8>) {
        let mut archive_input = input();
        let native = b"native rollout line\n".repeat(2_000);
        let untracked = tar_with_file(
            "notes/large.txt",
            &b"untracked payload line\n".repeat(1_000),
            0o644,
        );
        archive_input.native_artifacts[0].data = native.clone();
        archive_input.repositories[0].untracked_tar = untracked.clone();
        (archive_input, native, untracked)
    }

    #[test]
    fn oversized_payloads_shard_into_parts_and_read_back_whole() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("sharded.hel.zip");
        let (archive_input, native, untracked) = sharded_input();
        write_archive_installed_with_part_size(&path, &archive_input, TEST_PART_BYTES).unwrap();

        let native_path = "native/sessions/native-1/rollout.jsonl";
        let names = zip_entry_names(&path);
        assert!(
            !names.contains(&native_path.to_string()),
            "a sharded payload owns no entry of its own: {names:?}"
        );
        let part_names = names
            .iter()
            .filter(|name| name.starts_with(&format!("{native_path}{PAYLOAD_PART_SUFFIX}")))
            .count();
        assert_eq!(part_names, native.len().div_ceil(TEST_PART_BYTES));
        assert!(part_names > 1);

        let metadata = verify_archive_streaming(&path).unwrap();
        assert_eq!(
            metadata.manifest.schema_version,
            ARCHIVE_SCHEMA_VERSION_SHARDED
        );
        assert_eq!(metadata.canonical_session, archive_input.canonical_session);

        let verified = read_archive_verified(&path).unwrap();
        assert_eq!(verified.archive_sha256, metadata.archive_sha256);
        assert_eq!(
            verified
                .payload_by_role(&PayloadRole::NativeArtifact {
                    relative_path: PathBuf::from("sessions/native-1/rollout.jsonl"),
                })
                .unwrap(),
            native.as_slice()
        );
        assert_eq!(
            verified
                .payload_by_role(&PayloadRole::GitUntrackedTar {
                    repository_id: "hel".into(),
                })
                .unwrap(),
            untracked.as_slice()
        );
        assert_eq!(
            verified.canonical_session().unwrap(),
            archive_input.canonical_session
        );
        assert!(
            verified
                .payloads
                .keys()
                .all(|path| !path.contains(PAYLOAD_PART_SUFFIX)),
            "restore consumers only ever see whole payload paths"
        );
        assert!(verified.payloads.contains_key(native_path));
    }

    #[test]
    fn payload_parts_follow_the_threshold_and_never_split_stored_payloads() {
        let bundle = PayloadRole::GitBundle {
            repository_id: "hel".into(),
        };
        let artifact = PayloadRole::NativeArtifact {
            relative_path: PathBuf::from("rollout.jsonl"),
        };
        assert_eq!(payload_compression(&bundle), CompressionMethod::Stored);
        assert_eq!(payload_compression(&artifact), CompressionMethod::Deflated);

        let body = vec![b'x'; 10];
        assert!(
            plan_payload_parts(
                "repositories/hel/committed.bundle",
                &body,
                CompressionMethod::Stored,
                4,
            )
            .is_empty()
        );
        assert!(
            plan_payload_parts(
                "native/rollout.jsonl",
                &body,
                CompressionMethod::Deflated,
                10
            )
            .is_empty(),
            "a payload at the threshold stays whole"
        );
        let parts = plan_payload_parts(
            "native/rollout.jsonl",
            &body,
            CompressionMethod::Deflated,
            4,
        );
        assert_eq!(
            parts
                .iter()
                .map(|part| part.path.as_str())
                .collect::<Vec<_>>(),
            [
                "native/rollout.jsonl.helpart.00000",
                "native/rollout.jsonl.helpart.00001",
                "native/rollout.jsonl.helpart.00002",
            ]
        );
        assert_eq!(
            parts.iter().map(|part| part.size).collect::<Vec<_>>(),
            [4, 4, 2]
        );
        assert_eq!(parts[2].sha256, digest_bytes(&body[8..]));
    }

    #[test]
    fn git_bundles_are_stored_and_other_payloads_are_deflated() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("stored-bundle.hel.zip");
        write_archive_atomic(&path, &input()).unwrap();

        assert_eq!(
            zip_entry_method(&path, "repositories/hel/committed.bundle"),
            CompressionMethod::Stored
        );
        assert_eq!(
            zip_entry_method(&path, CANONICAL_SESSION_PATH),
            CompressionMethod::Deflated
        );
        assert_eq!(
            zip_entry_method(&path, "repositories/hel/untracked.tar"),
            CompressionMethod::Deflated
        );

        let verified = read_archive_verified(&path).unwrap();
        assert_eq!(
            verified
                .payload_by_role(&PayloadRole::GitBundle {
                    repository_id: "hel".into(),
                })
                .unwrap(),
            b"bundle-hel"
        );
    }

    /// Replaces a repository's untracked tar in an already prepared archive,
    /// resharding it so the manifest and the payload body stay consistent.
    fn replace_untracked_tar(
        manifest: &mut ArchiveManifest,
        payloads: &mut [PendingPayload<'_>],
        repository_id: &str,
        tar: Vec<u8>,
        part_bytes: usize,
    ) {
        let role = PayloadRole::GitUntrackedTar {
            repository_id: repository_id.to_string(),
        };
        let descriptor = manifest
            .payloads
            .iter_mut()
            .find(|payload| payload.role == role)
            .unwrap();
        descriptor.size = tar.len() as u64;
        descriptor.sha256 = digest_bytes(&tar);
        descriptor.parts = plan_payload_parts(
            &descriptor.path,
            &tar,
            CompressionMethod::Deflated,
            part_bytes,
        );
        let descriptor = descriptor.clone();
        let payload = payloads
            .iter_mut()
            .find(|payload| payload.descriptor.path == descriptor.path)
            .unwrap();
        payload.descriptor = descriptor;
        payload.data = Cow::Owned(tar);
    }

    #[test]
    fn streaming_verification_parses_a_sharded_untracked_tar_for_safety() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("unsafe-sharded-untracked.hel.zip");
        let (archive_input, _, _) = sharded_input();
        let (mut manifest, mut payloads) =
            prepare_archive_with_part_size(&archive_input, TEST_PART_BYTES).unwrap();
        let malicious = tar_with_file(".env", &b"secret\n".repeat(2_000), 0o600);
        assert!(malicious.len() > TEST_PART_BYTES);
        replace_untracked_tar(
            &mut manifest,
            &mut payloads,
            "hel",
            malicious,
            TEST_PART_BYTES,
        );
        let mut file = File::create(&path).unwrap();
        write_zip(&mut file, &manifest, &payloads).unwrap();
        drop(file);

        let error = format!("{:#}", verify_archive_streaming(&path).unwrap_err());
        assert!(error.contains("credential/config path"), "{error}");
    }

    #[test]
    fn sharded_manifests_reject_reordered_or_missing_parts() {
        let directory = tempfile::tempdir().unwrap();
        let (archive_input, _, _) = sharded_input();

        let (mut manifest, payloads) =
            prepare_archive_with_part_size(&archive_input, TEST_PART_BYTES).unwrap();
        let descriptor = manifest
            .payloads
            .iter_mut()
            .find(|payload| !payload.parts.is_empty())
            .unwrap();
        descriptor.parts.swap(0, 1);
        let reordered = directory.path().join("reordered.hel.zip");
        let mut file = File::create(&reordered).unwrap();
        write_zip(&mut file, &manifest, &payloads).unwrap();
        drop(file);
        let error = format!("{:#}", verify_archive_streaming(&reordered).unwrap_err());
        assert!(error.contains("part 0 has unexpected path"), "{error}");

        let (mut manifest, payloads) =
            prepare_archive_with_part_size(&archive_input, TEST_PART_BYTES).unwrap();
        let descriptor = manifest
            .payloads
            .iter_mut()
            .find(|payload| !payload.parts.is_empty())
            .unwrap();
        descriptor.parts.pop().unwrap();
        let truncated = directory.path().join("truncated.hel.zip");
        let mut file = File::create(&truncated).unwrap();
        write_zip(&mut file, &manifest, &payloads).unwrap();
        drop(file);
        let error = format!("{:#}", verify_archive_streaming(&truncated).unwrap_err());
        assert!(error.contains("parts cover"), "{error}");
    }

    #[test]
    fn readers_without_part_support_reject_sharded_archives() {
        // The schema-2 wire types as a build that predates sharding sees them.
        #[derive(Debug, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct LegacyPayloadDescriptor {
            #[allow(dead_code)]
            path: String,
            #[allow(dead_code)]
            sha256: String,
            #[allow(dead_code)]
            size: u64,
            #[allow(dead_code)]
            mode: u32,
            #[allow(dead_code)]
            role: PayloadRole,
        }
        #[derive(Debug, Deserialize)]
        struct LegacyManifest {
            schema_version: u32,
            payloads: Vec<LegacyPayloadDescriptor>,
        }

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("sharded.hel.zip");
        let (archive_input, _, _) = sharded_input();
        write_archive_installed_with_part_size(&path, &archive_input, TEST_PART_BYTES).unwrap();
        let mut archive = zip::ZipArchive::new(File::open(&path).unwrap()).unwrap();
        let mut manifest_bytes = Vec::new();
        archive
            .by_name(MANIFEST_PATH)
            .unwrap()
            .read_to_end(&mut manifest_bytes)
            .unwrap();

        // Gate 1: the version an old build compares for equality against 2.
        let header: serde_json::Value = serde_json::from_slice(&manifest_bytes).unwrap();
        assert_eq!(
            header["schema_version"],
            serde_json::json!(ARCHIVE_SCHEMA_VERSION_SHARDED)
        );

        // Gate 2: even ignoring the version, the old payload type cannot parse
        // a descriptor that carries parts.
        let error = serde_json::from_slice::<LegacyManifest>(&manifest_bytes).unwrap_err();
        assert!(
            error.to_string().contains("unknown field `parts`"),
            "{error}"
        );

        // Gate 3: an archive that claims schema 2 while carrying parts, or
        // schema 3 while carrying none, is rejected by this build too.
        let (mut manifest, payloads) =
            prepare_archive_with_part_size(&archive_input, TEST_PART_BYTES).unwrap();
        manifest.schema_version = ARCHIVE_SCHEMA_VERSION;
        let downgraded = directory.path().join("downgraded.hel.zip");
        let mut file = File::create(&downgraded).unwrap();
        write_zip(&mut file, &manifest, &payloads).unwrap();
        drop(file);
        let error = format!("{:#}", read_archive_verified(&downgraded).unwrap_err());
        assert!(
            error.contains("incompatible Hel archive schema 2; this build requires schema 3"),
            "{error}"
        );

        // An archive with no sharded payload stays schema 2 and still parses
        // with the old wire types, so small sessions keep full compatibility.
        let whole = directory.path().join("whole.hel.zip");
        write_archive_atomic(&whole, &input()).unwrap();
        let mut archive = zip::ZipArchive::new(File::open(&whole).unwrap()).unwrap();
        let mut whole_manifest = Vec::new();
        archive
            .by_name(MANIFEST_PATH)
            .unwrap()
            .read_to_end(&mut whole_manifest)
            .unwrap();
        let legacy: LegacyManifest = serde_json::from_slice(&whole_manifest).unwrap();
        assert_eq!(legacy.schema_version, ARCHIVE_SCHEMA_VERSION);
        assert!(!legacy.payloads.is_empty());
    }

    #[test]
    fn a_corrupt_part_fails_the_parallel_read_with_the_part_name() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("corrupt-part.hel.zip");
        let (archive_input, _, _) = sharded_input();
        write_archive_installed_with_part_size(&path, &archive_input, TEST_PART_BYTES).unwrap();

        let corrupt = "native/sessions/native-1/rollout.jsonl.helpart.00001";
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let mut archive = zip::ZipArchive::new(file).unwrap();
        let entry = archive.by_name(corrupt).unwrap();
        let data_start = entry.data_start();
        drop(entry);
        let mut file = archive.into_inner();
        file.seek(SeekFrom::Start(data_start)).unwrap();
        file.write_all(b"\xff\xff\xff\xff").unwrap();
        drop(file);

        for error in [
            verify_archive_streaming(&path).unwrap_err(),
            read_archive_verified(&path).unwrap_err(),
        ] {
            let error = format!("{error:#}");
            assert!(error.contains(corrupt), "{error}");
        }
    }

    #[test]
    fn writing_the_same_input_twice_produces_identical_archives() {
        let directory = tempfile::tempdir().unwrap();
        let (archive_input, _, _) = sharded_input();
        let first = directory.path().join("first.hel.zip");
        let second = directory.path().join("second.hel.zip");
        write_archive_installed_with_part_size(&first, &archive_input, TEST_PART_BYTES).unwrap();
        write_archive_installed_with_part_size(&second, &archive_input, TEST_PART_BYTES).unwrap();
        assert_eq!(fs::read(&first).unwrap(), fs::read(&second).unwrap());
        assert_eq!(zip_entry_names(&first), zip_entry_names(&second));
    }

    #[test]
    fn streaming_verification_rejects_noncanonical_corruption() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("corrupt-native.hel.zip");
        let mut archive_input = input();
        archive_input.repositories.clear();
        archive_input.native_artifacts[0].data = b"native payload".to_vec();
        write_archive_atomic(&path, &archive_input).unwrap();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let mut archive = zip::ZipArchive::new(file).unwrap();
        let entry = archive
            .by_name("native/sessions/native-1/rollout.jsonl")
            .unwrap();
        let data_start = entry.data_start();
        drop(entry);
        let mut file = archive.into_inner();
        file.seek(SeekFrom::Start(data_start)).unwrap();
        file.write_all(b"X").unwrap();
        drop(file);

        assert!(verify_archive_streaming(&path).is_err());
    }

    #[test]
    fn streaming_verification_rejects_unsafe_extra_zip_entry() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("unsafe-extra.hel.zip");
        let archive_input = input();
        let (manifest, payloads) = prepare_archive(&archive_input).unwrap();
        let file = File::create(&path).unwrap();
        let mut writer = zip::ZipWriter::new(file);
        writer
            .start_file(
                MANIFEST_PATH,
                SimpleFileOptions::default().unix_permissions(0o600),
            )
            .unwrap();
        writer
            .write_all(&serde_json::to_vec_pretty(&manifest).unwrap())
            .unwrap();
        for payload in payloads {
            writer
                .start_file(
                    payload.descriptor.path,
                    SimpleFileOptions::default().unix_permissions(payload.descriptor.mode),
                )
                .unwrap();
            writer.write_all(&payload.data).unwrap();
        }
        writer
            .start_file("../escape", SimpleFileOptions::default())
            .unwrap();
        writer.write_all(b"unsafe").unwrap();
        writer.finish().unwrap();

        let error = verify_archive_streaming(&path).unwrap_err();
        assert!(format!("{error:#}").contains("unsafe ZIP entry path"));
    }

    #[test]
    fn streaming_verification_parses_untracked_tar_for_safety() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("unsafe-untracked.hel.zip");
        let archive_input = input();
        let (mut manifest, mut payloads) = prepare_archive(&archive_input).unwrap();
        let malicious = tar_with_file(".env", b"secret", 0o600);
        let descriptor = manifest
            .payloads
            .iter_mut()
            .find(|payload| {
                matches!(
                    payload.role,
                    PayloadRole::GitUntrackedTar { ref repository_id }
                        if repository_id == "hel"
                )
            })
            .unwrap();
        descriptor.size = malicious.len() as u64;
        descriptor.sha256 = digest_bytes(&malicious);
        let payload = payloads
            .iter_mut()
            .find(|payload| payload.descriptor.path == descriptor.path)
            .unwrap();
        payload.descriptor = descriptor.clone();
        payload.data = Cow::Owned(malicious);
        let mut file = File::create(&path).unwrap();
        write_zip(&mut file, &manifest, &payloads).unwrap();
        drop(file);

        let error = verify_archive_streaming(&path).unwrap_err();
        assert!(format!("{error:#}").contains("credential/config path"));
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
        let entry = archive.by_name(CANONICAL_SESSION_PATH).unwrap();
        let data_start = entry.data_start();
        drop(entry);
        let mut file = archive.into_inner();
        file.seek(SeekFrom::Start(data_start)).unwrap();
        file.write_all(b"X").unwrap();
        drop(file);

        assert!(read_archive_verified(&path).is_err());
    }

    #[test]
    fn old_and_future_schemas_are_rejected_explicitly() {
        for schema_version in [1, ARCHIVE_SCHEMA_VERSION + 1] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("session.hel.zip");
            let archive_input = input();
            let (mut manifest, payloads) = prepare_archive(&archive_input).unwrap();
            manifest.schema_version = schema_version;
            let mut file = File::create(&path).unwrap();
            write_zip(&mut file, &manifest, &payloads).unwrap();
            drop(file);

            let error = read_archive_verified(&path).unwrap_err();
            let error = format!("{error:#}");
            assert!(
                error.contains(&format!(
                    "incompatible Hel archive schema {schema_version}; this build requires schema {ARCHIVE_SCHEMA_VERSION}"
                )),
                "{error}"
            );
        }
    }

    #[test]
    fn schema_two_wire_rejects_unknown_and_omitted_required_fields() {
        let mut canonical = serde_json::to_value(input().canonical_session).unwrap();
        canonical
            .as_object_mut()
            .unwrap()
            .insert("revision".into(), serde_json::json!(4));
        assert!(serde_json::from_value::<CanonicalSessionSnapshot>(canonical).is_err());

        let mut canonical = serde_json::to_value(input().canonical_session).unwrap();
        canonical["session"]
            .as_object_mut()
            .unwrap()
            .remove("configuration");
        assert!(serde_json::from_value::<CanonicalSessionSnapshot>(canonical).is_err());

        let mut target = serde_json::to_value(input().target).unwrap();
        target.as_object_mut().unwrap().remove("details");
        assert!(serde_json::from_value::<TargetManifest>(target).is_err());

        let mut metadata = serde_json::to_value(repository("repo").metadata).unwrap();
        metadata.as_object_mut().unwrap().remove("base_commit");
        assert!(serde_json::from_value::<RepositoryMetadata>(metadata).is_err());
    }

    #[test]
    fn canonical_session_rejects_duplicate_or_out_of_frontier_items() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("session.hel.zip");
        let mut invalid = input();
        invalid.canonical_session.transcript[1].stable_id = "user-1".into();
        let error = write_archive_atomic(&path, &invalid).unwrap_err();
        assert!(format!("{error:#}").contains("duplicate canonical transcript item id"));

        invalid = input();
        invalid.canonical_session.transcript[1].position = 5;
        let error = write_archive_atomic(&path, &invalid).unwrap_err();
        assert!(format!("{error:#}").contains("beyond event frontier"));
        assert!(!path.exists());
    }

    #[test]
    fn canonical_session_requires_a_valid_latest_agent_content_ordinal() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("session.hel.zip");
        let mut invalid = input();
        invalid.canonical_session.transcript[1].latest_content_event_ordinal = None;
        let error = write_archive_atomic(&path, &invalid).unwrap_err();
        assert!(format!("{error:#}").contains("has no latest content ordinal"));

        invalid = input();
        invalid.canonical_session.transcript[1].latest_content_event_ordinal = Some(5);
        let error = write_archive_atomic(&path, &invalid).unwrap_err();
        assert!(format!("{error:#}").contains("invalid latest content ordinal"));

        invalid = input();
        invalid.canonical_session.transcript[0].latest_content_event_ordinal = Some(1);
        let error = write_archive_atomic(&path, &invalid).unwrap_err();
        assert!(format!("{error:#}").contains("non-agent transcript item"));
    }

    #[test]
    fn canonical_session_rejects_a_stream_still_open_at_the_barrier() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("session.hel.zip");
        let mut invalid = input();
        invalid.canonical_session.transcript[1].body = CanonicalTranscriptBody::Agent {
            chunks: vec![serde_json::json!({
                "content": {"type": "text", "text": "partial"}
            })],
            streaming: true,
        };

        let error = write_archive_atomic(&path, &invalid).unwrap_err();

        assert!(format!("{error:#}").contains("still streaming at the checkpoint barrier"));
        assert!(!path.exists());
    }

    #[test]
    fn canonical_session_rejects_non_idle_execution_at_the_barrier() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("session.hel.zip");
        let mut invalid = input();
        invalid.canonical_session.session.execution =
            CanonicalExecutionState::Running { started_at_ms: 105 };

        let error = write_archive_atomic(&path, &invalid).unwrap_err();

        assert!(format!("{error:#}").contains("not idle at the checkpoint barrier"));
        assert!(!path.exists());
    }

    #[test]
    fn canonical_session_rejects_an_invalid_event_frontier_digest() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("session.hel.zip");
        let mut invalid = input();
        invalid.canonical_session.event_frontier_digest = "A".repeat(64);

        let error = write_archive_atomic(&path, &invalid).unwrap_err();

        assert!(format!("{error:#}").contains("64 lowercase hexadecimal characters"));
        assert!(!path.exists());

        invalid = input();
        invalid.canonical_session.event_frontier_digest = EVENT_FRONTIER_GENESIS_DIGEST.into();
        let error = write_archive_atomic(&path, &invalid).unwrap_err();
        assert!(format!("{error:#}").contains("frontier and digest are inconsistent"));
        assert!(!path.exists());
    }

    #[test]
    fn canonical_session_rejects_unrestorable_queued_prompts() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("session.hel.zip");
        let mut invalid = input();
        invalid.canonical_session.queued_prompts[0].content.clear();

        let error = write_archive_atomic(&path, &invalid).unwrap_err();
        assert!(format!("{error:#}").contains("has no content"));
        assert!(!path.exists());

        invalid.canonical_session.queued_prompts[0].content =
            vec![serde_json::json!({"type": "not_an_acp_content_block"})];
        let error = write_archive_atomic(&path, &invalid).unwrap_err();
        assert!(format!("{error:#}").contains("has invalid ACP content block 0"));
        assert!(!path.exists());

        invalid = input();
        invalid.canonical_session.queued_prompts[0].kind = CanonicalQueuedCommandKind::SetConfig {
            key: "model".into(),
            value: "  ".into(),
        };
        let error = write_archive_atomic(&path, &invalid).unwrap_err();
        assert!(format!("{error:#}").contains("is incomplete"));
        assert!(!path.exists());
    }

    #[test]
    fn queued_entries_written_before_config_changes_load_as_prompts() {
        let stored: CanonicalQueuedPrompt = serde_json::from_value(serde_json::json!({
            "command_id": "queued-1",
            "content": [{"type": "text", "text": "hello"}],
            "queued_at_ms": 5,
        }))
        .unwrap();
        assert_eq!(stored.kind, CanonicalQueuedCommandKind::Prompt);
        // A prompt entry still serializes exactly as it did before.
        assert_eq!(
            serde_json::to_value(&stored).unwrap(),
            serde_json::json!({
                "command_id": "queued-1",
                "content": [{"type": "text", "text": "hello"}],
                "queued_at_ms": 5,
            })
        );

        let config = CanonicalQueuedPrompt {
            command_id: "queued-2".into(),
            kind: CanonicalQueuedCommandKind::SetConfig {
                key: "model".into(),
                value: "sonnet".into(),
            },
            content: vec![serde_json::json!({"type": "text", "text": "/model sonnet"})],
            queued_at_ms: 6,
        };
        let encoded = serde_json::to_value(&config).unwrap();
        assert_eq!(
            serde_json::from_value::<CanonicalQueuedPrompt>(encoded).unwrap(),
            config
        );
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
                history: GitHistoryMode::DeltaFrom("a".repeat(40)),
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
        assert_eq!(runner.commands().len(), 9);
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
                history: GitHistoryMode::DeltaFrom("a".repeat(40)),
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
                        history: GitHistoryMode::DeltaFrom("a".repeat(40)),
                        origin_override: None,
                    },
                )
            })
            .unwrap();

        assert_eq!(snapshot.committed_bundle, b"bundle");
        assert_eq!(snapshot.staged_patch, b"staged");
        assert_eq!(snapshot.unstaged_patch, b"unstaged");
        assert_eq!(runner.commands().len(), 10);
    }

    #[test]
    fn metadata_snapshot_requires_git_head_but_omits_repository_contents() {
        let repository = tempfile::tempdir().unwrap();
        git(repository.path(), &["init", "-q", "-b", "main"]);
        git(repository.path(), &["config", "user.name", "Hel Test"]);
        git(
            repository.path(),
            &["config", "user.email", "hel@example.test"],
        );
        fs::write(repository.path().join("tracked.txt"), b"base\n").unwrap();
        git(repository.path(), &["add", "."]);
        git(repository.path(), &["commit", "-qm", "base"]);
        fs::write(repository.path().join("tracked.txt"), b"dirty\n").unwrap();
        fs::write(repository.path().join("untracked.txt"), b"untracked\n").unwrap();

        let snapshot = collect_git_metadata_snapshot(
            &SystemGit,
            repository.path(),
            &GitCollectionSpec {
                id: "project".into(),
                relative_destination: "project".into(),
                history: GitHistoryMode::NoBundle,
                origin_override: None,
            },
        )
        .unwrap();

        assert_eq!(snapshot.metadata.base_commit, snapshot.metadata.head_commit);
        assert!(snapshot.committed_bundle.is_empty());
        assert!(snapshot.staged_patch.is_empty());
        assert!(snapshot.unstaged_patch.is_empty());
        assert!(snapshot.untracked_tar.is_empty());

        fs::remove_dir_all(repository.path().join(".git")).unwrap();
        let error = collect_git_metadata_snapshot(
            &SystemGit,
            repository.path(),
            &GitCollectionSpec {
                id: "project".into(),
                relative_destination: "project".into(),
                history: GitHistoryMode::NoBundle,
                origin_override: None,
            },
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("repository has no valid Git HEAD"));
    }

    #[test]
    fn session_delta_bundles_commits_missing_from_every_origin_ref() {
        let directory = tempfile::tempdir().unwrap();
        let origin = directory.path().join("origin");
        initialize_repository(&origin);
        commit_file(&origin, "base.txt", b"base\n", "base");
        git(&origin, &["checkout", "-q", "-b", "release"]);
        commit_file(&origin, "release.txt", b"release\n", "release");
        git(&origin, &["checkout", "-q", "main"]);
        let source = clone_repository(directory.path(), &origin, "source");
        commit_file(&source, "first.txt", b"first\n", "first");
        commit_file(&source, "second.txt", b"second\n", "second");
        let head = git_line(&source, &["rev-parse", "HEAD"]);

        let snapshot = collect_git_snapshot(
            &SystemGit,
            &source,
            &GitCollectionSpec {
                id: "repo".into(),
                relative_destination: "repo".into(),
                history: GitHistoryMode::SessionDelta,
                origin_override: None,
            },
        )
        .unwrap();

        assert!(!snapshot.committed_bundle.is_empty());
        assert_eq!(snapshot.metadata.base_commit, "");
        assert_eq!(snapshot.metadata.head_commit, head);

        let destination = clone_repository(directory.path(), &origin, "restored");
        restore_git_snapshot(&SystemGit, &destination, &snapshot).unwrap();
        assert_eq!(git_line(&destination, &["rev-parse", "HEAD"]), head);
        assert_eq!(
            git_line(
                &destination,
                &["rev-list", "--count", "HEAD", "--not", "--remotes=origin"]
            ),
            "2"
        );
        assert_eq!(
            fs::read(destination.join("release.txt"))
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::NotFound
        );
    }

    /// Provisioning must fetch a local repository's history before restoring
    /// into it: the delta bundle carries only the commits origin lacks.
    #[test]
    fn session_delta_restore_requires_the_fetched_origin_history() {
        let directory = tempfile::tempdir().unwrap();
        let origin = directory.path().join("origin");
        initialize_repository(&origin);
        commit_file(&origin, "base.txt", b"base\n", "base");
        let source = clone_repository(directory.path(), &origin, "source");
        commit_file(&source, "session.txt", b"session\n", "session work");
        let head = git_line(&source, &["rev-parse", "HEAD"]);
        let snapshot = collect_git_snapshot(
            &SystemGit,
            &source,
            &GitCollectionSpec {
                id: "repo".into(),
                relative_destination: "repo".into(),
                history: GitHistoryMode::SessionDelta,
                origin_override: None,
            },
        )
        .unwrap();

        let destination = directory.path().join("target");
        initialize_repository(&destination);
        let unfetched = restore_git_snapshot(&SystemGit, &destination, &snapshot).unwrap_err();
        assert!(
            format!("{unfetched:#}").contains("fetch committed delta bundle"),
            "{unfetched:#}"
        );

        git(
            &destination,
            &["remote", "add", "origin", origin.to_str().unwrap()],
        );
        git(&destination, &["fetch", "-q", "origin"]);
        restore_git_snapshot(&SystemGit, &destination, &snapshot).unwrap();

        assert_eq!(git_line(&destination, &["rev-parse", "HEAD"]), head);
        assert_eq!(
            fs::read(destination.join("session.txt")).unwrap(),
            b"session\n"
        );
    }

    #[test]
    fn session_delta_errors_without_origin_refs() {
        let source = tempfile::tempdir().unwrap();
        initialize_repository(source.path());
        commit_file(source.path(), "base.txt", b"base\n", "base");

        let error = collect_git_snapshot(
            &SystemGit,
            source.path(),
            &GitCollectionSpec {
                id: "repo".into(),
                relative_destination: "repo".into(),
                history: GitHistoryMode::SessionDelta,
                origin_override: None,
            },
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("repository has no origin refs to delta against"),
            "{error:#}"
        );
    }

    #[test]
    fn delta_from_errors_when_the_base_is_unresolvable() {
        let source = tempfile::tempdir().unwrap();
        initialize_repository(source.path());
        commit_file(source.path(), "old.txt", b"old\n", "old root");

        let missing = collect_git_snapshot(
            &SystemGit,
            source.path(),
            &GitCollectionSpec {
                id: "repo".into(),
                relative_destination: "repo".into(),
                history: GitHistoryMode::DeltaFrom("refs/hel/missing".into()),
                origin_override: None,
            },
        )
        .unwrap_err();
        assert!(
            format!("{missing:#}").contains("delta base refs/hel/missing is unresolvable"),
            "{missing:#}"
        );

        let old_head = git_line(source.path(), &["rev-parse", "HEAD"]);
        git(source.path(), &["checkout", "-q", "--orphan", "rewritten"]);
        git(source.path(), &["rm", "-q", "-rf", "."]);
        commit_file(source.path(), "new.txt", b"new root\n", "new root");
        let unrelated = collect_git_snapshot(
            &SystemGit,
            source.path(),
            &GitCollectionSpec {
                id: "repo".into(),
                relative_destination: "repo".into(),
                history: GitHistoryMode::DeltaFrom(old_head.clone()),
                origin_override: None,
            },
        )
        .unwrap_err();
        assert!(
            format!("{unrelated:#}").contains(&format!("delta base {old_head} is unresolvable")),
            "{unrelated:#}"
        );
    }

    #[test]
    fn no_bundle_snapshot_carries_dirty_state_without_committed_history() {
        let directory = tempfile::tempdir().unwrap();
        let origin = directory.path().join("origin");
        initialize_repository(&origin);
        commit_file(&origin, "tracked.txt", b"base\n", "base");
        commit_file(&origin, "dirty.txt", b"clean\n", "clean");
        let source = clone_repository(directory.path(), &origin, "source");
        fs::write(source.join("staged.txt"), b"staged\n").unwrap();
        git(&source, &["add", "staged.txt"]);
        fs::write(source.join("dirty.txt"), b"dirty\n").unwrap();
        fs::write(source.join("new.txt"), b"untracked\n").unwrap();
        let head = git_line(&source, &["rev-parse", "HEAD"]);

        let snapshot = collect_git_snapshot(
            &SystemGit,
            &source,
            &GitCollectionSpec {
                id: "repo".into(),
                relative_destination: "repo".into(),
                history: GitHistoryMode::NoBundle,
                origin_override: Some("hel-local:repo".into()),
            },
        )
        .unwrap();

        assert!(snapshot.committed_bundle.is_empty());
        assert_eq!(snapshot.metadata.origin, "hel-local:repo");
        assert_eq!(snapshot.metadata.base_commit, head);
        assert!(!snapshot.staged_patch.is_empty());
        assert!(!snapshot.unstaged_patch.is_empty());
        assert!(!snapshot.untracked_tar.is_empty());

        let destination = clone_repository(directory.path(), &origin, "restored");
        git(&destination, &["checkout", "-q", "--detach", "HEAD"]);
        restore_git_snapshot(&SystemGit, &destination, &snapshot).unwrap();

        assert_eq!(git_line(&destination, &["rev-parse", "HEAD"]), head);
        assert_eq!(
            git_line(&destination, &["symbolic-ref", "--short", "HEAD"]),
            "main"
        );
        assert_eq!(fs::read(destination.join("dirty.txt")).unwrap(), b"dirty\n");
        assert_eq!(
            fs::read(destination.join("new.txt")).unwrap(),
            b"untracked\n"
        );
        let status = String::from_utf8(git(&destination, &["status", "--short"])).unwrap();
        assert!(status.contains("A  staged.txt"), "{status}");
        assert!(status.contains(" M dirty.txt"), "{status}");
        assert!(status.contains("?? new.txt"), "{status}");
    }

    #[test]
    fn restore_without_a_bundle_reports_an_unreachable_commit_actionably() {
        let directory = tempfile::tempdir().unwrap();
        let origin = directory.path().join("origin");
        initialize_repository(&origin);
        commit_file(&origin, "base.txt", b"base\n", "base");
        let source = clone_repository(directory.path(), &origin, "source");
        commit_file(&source, "local.txt", b"local\n", "local only");
        let snapshot = collect_git_snapshot(
            &SystemGit,
            &source,
            &GitCollectionSpec {
                id: "repo".into(),
                relative_destination: "repo".into(),
                history: GitHistoryMode::NoBundle,
                origin_override: None,
            },
        )
        .unwrap();

        let destination = clone_repository(directory.path(), &origin, "restored");
        let error = restore_git_snapshot(&SystemGit, &destination, &snapshot).unwrap_err();

        assert!(
            format!("{error:#}").contains("must be reachable from the repository's origin"),
            "{error:#}"
        );
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
                history: GitHistoryMode::DeltaFrom(base.clone()),
                origin_override: None,
            },
        )
        .unwrap();
        assert!(!snapshot.committed_bundle.is_empty());
        assert_eq!(snapshot.metadata.base_commit, base);

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
