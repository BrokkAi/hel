//! Controller-side lifecycle transitions and canonical-to-backend conversion.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use agent_client_protocol::schema::v1::{ContentBlock, Plan, TextContent, ToolCall};
use anyhow::{Context, Result, bail, ensure};
use chrono::Utc;
use sha2::{Digest, Sha256};

use crate::hel_archive::{
    ArchiveInput, BundleManifest, CanonicalSessionSnapshot, CanonicalTranscriptBody,
    GitCollectionSpec, GitHistoryMode, SessionManifest, SystemGit, TargetManifest,
    collect_git_snapshot, verify_archive_streaming, write_archive_atomic,
};
use crate::hel_checkpoint::{
    CheckpointExportSpec, CheckpointRepositoryCapture, CheckpointRepositorySpec,
    CheckpointRestoreSpec, CheckpointTransfer, RepositoryRestoreSpec, export_command,
    restore_command,
};
use crate::hel_config::{
    AwsAddressSource, HelConfig, ProjectBundle, SshConnection, TargetTemplate, atomic_write,
    data_dir, sessions_dir,
};
use crate::hel_database::advance_detached_after_event_ordinal;
use crate::hel_git_proxy::{GitBrokerSpec, broker_is_alive};
use crate::hel_local_git::{canonical_repository, dirty_local_repositories};
use crate::hel_projection::{
    canonical_session_from_materialized, materialized_session_from_canonical,
};
use crate::hel_session_manager::{
    ManagedSessionHandle, ManagedSessionLease, ManagedSessionSnapshot, SessionManagerControl,
    StandaloneSession, new_command_id,
};
use crate::hel_state::{
    CheckpointMetadata, HelState, ManagedWorktree, ManagedWorktreeTarget, MaterializedSession,
    SessionRecord, SessionResourceAllocation, SessionState, TargetLocator, new_session_id,
    normalize_session_title,
};
use crate::hel_targets::{
    self, AdditionalMount, AwsTemplate, CancellableProcessExecutor, CommandExecutor, CommandOutput,
    CommandSpec, ContainerTemplate, ProcessExecutor, ProjectBundleSpec, RepositorySpec, SshTarget,
};
use crate::hel_worker::{RelayCommand, RelayCursor, RelayExecutionState};
use crate::hel_worker_runtime::{WorkerLaunchConfig, WorkerOwnership};

const INHERITED_GIT_SETTINGS: &[&str] = &[
    "diff.algorithm",
    "fetch.prune",
    "fetch.prunetags",
    "init.defaultbranch",
    "merge.conflictstyle",
    "pull.ff",
    "pull.rebase",
    "push.autosetupremote",
    "push.default",
    "rebase.autostash",
    "rerere.autoupdate",
    "rerere.enabled",
    "user.email",
    "user.name",
];

pub struct Controller {
    pub config: HelConfig,
    pub state: HelState,
}

/// Machine-wide advisory lock for one controller data store. This prevents a
/// dashboard, server, or CLI lifecycle command from concurrently acting as a
/// second controller against the same SQLite state and relay sessions.
#[derive(Debug)]
pub struct ControllerStoreGuard {
    file: File,
}

impl ControllerStoreGuard {
    pub fn acquire() -> Result<Self> {
        let directory = data_dir();
        Self::acquire_at(&directory)
    }

    fn acquire_at(directory: &Path) -> Result<Self> {
        std::fs::create_dir_all(directory)
            .with_context(|| format!("create controller data directory {}", directory.display()))?;
        let path = directory.join("controller.lock");
        let mut options = OpenOptions::new();
        options.create(true).read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options
            .open(&path)
            .with_context(|| format!("open controller lock {}", path.display()))?;
        match file.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => bail!(
                "another Hel controller is already using {}; stop it before starting this command",
                directory.display()
            ),
            Err(std::fs::TryLockError::Error(error)) => {
                return Err(error)
                    .with_context(|| format!("lock controller store {}", directory.display()));
            }
        }
        Ok(Self { file })
    }
}

impl Drop for ControllerStoreGuard {
    fn drop(&mut self) {
        // Make release explicit. `File` also unlocks on close, but an explicit
        // unlock keeps same-process handoff deterministic across platforms.
        let _ = self.file.unlock();
    }
}

/// Remove checkpoint archives installed by a process that exited before its
/// database transaction committed. Call this only while holding the
/// machine-wide controller-store guard and before starting background work.
pub fn reconcile_managed_checkpoint_archives() -> Result<usize> {
    let state = HelState::load()?;
    reconcile_managed_checkpoint_archives_in(&sessions_dir(), &state)
}

fn reconcile_managed_checkpoint_archives_in(directory: &Path, state: &HelState) -> Result<usize> {
    if !directory.exists() {
        return Ok(0);
    }
    let referenced_names = state
        .sessions
        .values()
        .filter_map(|session| session.checkpoint.as_ref())
        .filter_map(|checkpoint| checkpoint.archive_path.file_name())
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>();
    let mut removed = 0;
    for entry in std::fs::read_dir(directory)
        .with_context(|| format!("scan checkpoint directory {}", directory.display()))?
    {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if !file_type.is_file()
            || !is_managed_checkpoint_archive_name(&entry.file_name())
            || referenced_names.contains(&entry.file_name())
        {
            continue;
        }
        std::fs::remove_file(entry.path()).with_context(|| {
            format!(
                "remove unreferenced managed checkpoint {}",
                entry.path().display()
            )
        })?;
        removed += 1;
    }
    Ok(removed)
}

fn is_managed_checkpoint_archive_name(name: &OsStr) -> bool {
    let Some(stem) = name.to_str().and_then(|name| name.strip_suffix(".hel.zip")) else {
        return false;
    };
    let Some((frontier_prefix, nonce)) = stem.rsplit_once("-archive-") else {
        return false;
    };
    if nonce.len() != 32
        || !nonce
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return false;
    }
    let Some((session_id, frontier)) = frontier_prefix.rsplit_once('-') else {
        return false;
    };
    !session_id.is_empty()
        && frontier.parse::<u64>().is_ok()
        && crate::hel_config::validate_id("session", session_id).is_ok()
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct RecoveryCandidate {
    pub session_id: String,
    pub target_template_id: String,
    pub locator: TargetLocator,
    pub ownership: Option<WorkerOwnership>,
}

#[derive(Debug, Default, serde::Serialize)]
pub struct RecoveryScan {
    pub candidates: Vec<RecoveryCandidate>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct CheckpointArtifact {
    pub metadata: CheckpointMetadata,
    pub native_session_id: String,
    /// Digest paired with `metadata.event_frontier` at the relay barrier.
    pub event_frontier_digest: String,
}

/// The relay connection one lifecycle operation talks to.
///
/// A managed operation borrows the session actor's own connection instead of
/// opening a competing one. Exclusivity is only needed while a checkpoint
/// latches its projection at the barrier's ready cursor; `end_latch` hands the
/// connection back so the dashboard keeps syncing and submitting while the
/// archive exports and transfers.
enum ControllerRelayLease {
    Managed {
        handle: ManagedSessionHandle,
        lease: Option<ManagedSessionLease>,
    },
    Standalone(StandaloneSession),
}

impl ControllerRelayLease {
    /// The exclusively held connection. Only a latch phase, or an operation
    /// that deliberately holds its lease to the end, may use this.
    fn connection_mut(&mut self) -> &mut StandaloneSession {
        match self {
            Self::Managed { lease, .. } => lease
                .as_mut()
                .expect("checkpoint latch has already returned its connection")
                .connection_mut(),
            Self::Standalone(connection) => connection,
        }
    }

    async fn submit(&mut self, command_id: String, command: RelayCommand) -> Result<u64> {
        match self {
            Self::Managed {
                lease: Some(lease), ..
            } => lease.connection_mut().submit(command_id, command).await,
            Self::Managed { handle, .. } => handle.submit(command_id, command).await,
            Self::Standalone(connection) => connection.submit(command_id, command).await,
        }
    }

    async fn sync_snapshot(&mut self) -> Result<ManagedSessionSnapshot> {
        match self {
            Self::Managed {
                lease: Some(lease), ..
            } => lease.connection_mut().sync().await,
            Self::Managed { handle, .. } => {
                handle.sync_now().await?;
                handle
                    .view()
                    .snapshot
                    .context("managed session has no snapshot")
            }
            Self::Standalone(connection) => connection.sync().await,
        }
    }

    /// Return the connection to its session actor now that the projection is
    /// latched. Releasing keeps the connection alive, so the relay barrier it
    /// opened stays open. Idempotent.
    fn end_latch(&mut self) {
        if let Self::Managed { lease, .. } = self
            && let Some(lease) = lease.take()
        {
            lease.release();
        }
    }

    /// Abandon a checkpoint barrier this controller can no longer complete.
    ///
    /// A relay barrier belongs to the connection that opened it and only a
    /// disconnect cancels it (`cancel_checkpoint_barrier_on_disconnect`).
    /// Completing it instead would advance the relay's recovery floor past
    /// history that no verified checkpoint covers, so reclaim the connection
    /// and drop it: the worker cancels the barrier and resumes dispatch.
    async fn cancel_abandoned_barrier(&mut self) -> Result<()> {
        let Self::Managed { handle, lease } = self else {
            // A standalone connection is dropped with this value, which the
            // worker sees as the same disconnect.
            return Ok(());
        };
        match lease.take() {
            Some(lease) => drop(lease),
            None => drop(handle.lease_connection().await?),
        }
        Ok(())
    }

    fn release(self) {
        if let Self::Managed {
            lease: Some(lease), ..
        } = self
        {
            lease.release();
        }
    }
}

/// Whether a checkpoint keeps its exclusive connection after latching.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LatchExclusivity {
    /// Ordinary and recovery checkpoints only need exclusivity to latch the
    /// projection at the barrier's ready cursor. Everything after that runs
    /// through the session actor, so prompts keep flowing while the archive
    /// exports and transfers.
    ReleaseAfterLatch,
    /// Close seals the relay at the exact latched cursor, so nothing else may
    /// reach the relay between the barrier and its Close command.
    HoldThroughClose,
}

struct LatchedCheckpoint {
    artifact: CheckpointArtifact,
    relay: ControllerRelayLease,
    barrier_command_id: String,
    cursor: RelayCursor,
}

/// A latched checkpoint owns an open relay barrier, and that barrier freezes
/// ACP dispatch until something ends it. Every path out of one must therefore
/// either [`LatchedCheckpoint::complete`] it or [`LatchedCheckpoint::abandon`]
/// it; both consume the value so a new exit cannot quietly skip the choice.
/// Close is the exception: it holds its lease to the end, so dropping that
/// lease is what ends its barrier.
impl LatchedCheckpoint {
    /// Release the barrier that sealed a durably installed archive.
    async fn complete(mut self) -> Result<()> {
        let command_id = new_command_id("checkpoint-complete")?;
        self.relay
            .submit(
                command_id,
                RelayCommand::CompleteCheckpoint {
                    barrier_command_id: self.barrier_command_id.clone(),
                },
            )
            .await
            .map(|_| ())
    }

    /// Cancel the barrier of a checkpoint the caller could not install.
    ///
    /// The latch is already back with the session actor, whose connection can
    /// stay healthy for the rest of the session, so nothing else would ever
    /// end this barrier.
    async fn abandon(mut self, session_id: &str) {
        if let Err(error) = self.relay.cancel_abandoned_barrier().await {
            tracing::warn!(
                session_id,
                "abandoned checkpoint could not cancel its relay barrier: {error:#}"
            );
        }
    }
}

/// Whether connecting local repositories may also carry the user's current
/// uncommitted changes into a still-empty target checkout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalBootstrap {
    /// A fresh target starts from `git init`, so seed its branch and dirty
    /// state from the local repository.
    Seed,
    /// Resume restores the session's own dirty state from the checkpoint
    /// archive; seeding the local repository's would collide with it.
    Skip,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProvisioningFailureDisposition {
    /// A freshly registered session has no durable history to retain.
    Discard,
    /// Resume owns rollback to the archived record and checkpoint lineage.
    Preserve,
}

pub struct SessionLaunchOptions {
    pub additional_mounts: Vec<AdditionalMount>,
    pub allow_dirty_local: bool,
    pub resource_allocation: Option<SessionResourceAllocation>,
    pub project_directory: Option<PathBuf>,
    pub session_title_override: Option<String>,
}

pub struct SessionResumeOptions {
    pub additional_mounts: Option<Vec<AdditionalMount>>,
    pub resource_allocation: Option<SessionResourceAllocation>,
    pub discard_queue: bool,
}

impl Controller {
    pub fn resolve_aws_resource_options(
        &self,
        target_id: &str,
        executor: &impl CommandExecutor,
    ) -> Result<Vec<SessionResourceAllocation>> {
        let TargetTemplate::AwsEc2 {
            aws_profile,
            region,
            launch_template,
            launch_template_version,
            ..
        } = self
            .config
            .targets
            .get(target_id)
            .with_context(|| format!("unknown target template {target_id:?}"))?
        else {
            bail!("target {target_id:?} is not an AWS EC2 target");
        };
        let profile = aws_profile.as_deref().unwrap_or("default");
        let launch_key = if launch_template.starts_with("lt-") {
            "--launch-template-id"
        } else {
            "--launch-template-name"
        };
        let version = launch_template_version.as_deref().unwrap_or("$Default");
        let describe_template = CommandSpec::new(
            "aws",
            [
                "--profile",
                profile,
                "--region",
                region,
                "ec2",
                "describe-launch-template-versions",
                launch_key,
                launch_template,
                "--versions",
                version,
                "--output",
                "json",
            ],
        )
        .purpose("resolve EC2 launch template instance family");
        let output = executor.execute(&describe_template)?;
        if output.status != 0 {
            bail!(
                "{} failed with status {}: {}",
                describe_template.purpose,
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        let response: serde_json::Value =
            serde_json::from_slice(&output.stdout).context("parse EC2 launch template response")?;
        let instance_type = response
            .pointer("/LaunchTemplateVersions/0/LaunchTemplateData/InstanceType")
            .and_then(serde_json::Value::as_str)
            .context("launch template does not specify a concrete instance type")?;
        let family = instance_type
            .rsplit_once('.')
            .map(|(family, _)| family)
            .context("launch template instance type has no size suffix")?;
        let filter = format!("Name=instance-type,Values={family}.*");
        let describe_types = CommandSpec::new(
            "aws",
            [
                "--profile",
                profile,
                "--region",
                region,
                "ec2",
                "describe-instance-types",
                "--filters",
                &filter,
                "--output",
                "json",
            ],
        )
        .purpose("discover EC2 instance sizes");
        let output = executor.execute(&describe_types)?;
        if output.status != 0 {
            bail!(
                "{} failed with status {}: {}",
                describe_types.purpose,
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        let response: serde_json::Value =
            serde_json::from_slice(&output.stdout).context("parse EC2 instance type response")?;
        let mut options = response
            .get("InstanceTypes")
            .and_then(serde_json::Value::as_array)
            .context("EC2 instance type response omitted InstanceTypes")?
            .iter()
            .filter_map(|entry| {
                Some(SessionResourceAllocation::AwsEc2 {
                    instance_type: entry.get("InstanceType")?.as_str()?.to_owned(),
                    vcpus: entry.pointer("/VCpuInfo/DefaultVCpus")?.as_u64()?,
                    memory_bytes: entry
                        .pointer("/MemoryInfo/SizeInMiB")?
                        .as_u64()?
                        .checked_mul(1024 * 1024)?,
                })
            })
            .collect::<Vec<_>>();
        options.sort_by_key(allocation_vcpus);
        if !options.iter().any(|option| allocation_vcpus(option) == 8) {
            bail!("EC2 family {family:?} has no exact 8-vCPU baseline size");
        }
        Ok(options)
    }

    pub fn load() -> Result<Self> {
        let config = HelConfig::load()?;
        let state = HelState::load()?;
        state.validate_against_config(&config)?;
        Ok(Self { config, state })
    }

    pub fn reload(&mut self) -> Result<()> {
        *self = Self::load()?;
        Ok(())
    }

    fn persist_session_state(&self, session_id: &str) -> Result<()> {
        match self.state.sessions.get(session_id) {
            Some(session) => crate::hel_database::save_lifecycle_session(session),
            None => crate::hel_database::delete_session(session_id),
        }
    }

    fn persist_session_transition_or_restore(
        &mut self,
        session_id: &str,
        previous: &SessionRecord,
        context: &'static str,
    ) -> Result<()> {
        persist_session_record_transition_or_restore(
            &mut self.state,
            session_id,
            previous,
            context,
            &crate::hel_database::save_lifecycle_session,
        )
    }

    fn persist_checkpoint_transition_or_restore(
        &mut self,
        session_id: &str,
        previous: &SessionRecord,
        context: &'static str,
    ) -> Result<()> {
        persist_session_record_transition_or_restore(
            &mut self.state,
            session_id,
            previous,
            context,
            &crate::hel_database::save_checkpointed_session,
        )
    }

    fn persist_failed_checkpoint_state_or_restore(
        &mut self,
        session_id: &str,
        previous: &SessionRecord,
        primary: anyhow::Error,
    ) -> anyhow::Error {
        match self.persist_session_state(session_id) {
            Ok(()) => primary,
            Err(error) => self.restore_prior_session_after_persistence_failure(
                session_id,
                previous,
                primary.context(format!(
                    "failed to persist the checkpoint rollback state: {error:#}"
                )),
            ),
        }
    }

    fn restore_prior_session_after_persistence_failure(
        &mut self,
        session_id: &str,
        previous: &SessionRecord,
        primary: anyhow::Error,
    ) -> anyhow::Error {
        restore_session_after_persistence_failure(
            &mut self.state,
            session_id,
            previous,
            primary,
            crate::hel_database::save_lifecycle_session,
        )
    }

    /// Find managed resources which are not represented by the controller's
    /// current state. Labels/tags establish Hel ownership; the worker marker
    /// supplies profile and bundle metadata when it is available.
    pub fn scan_orphan_workers(&self, executor: &impl CommandExecutor) -> RecoveryScan {
        let mut scan = RecoveryScan::default();
        for (target_id, template) in &self.config.targets {
            match scan_target_workers(target_id, template, executor) {
                Ok(candidates) => {
                    scan.candidates
                        .extend(candidates.into_iter().filter(|candidate| {
                            !self.state.sessions.contains_key(&candidate.session_id)
                        }))
                }
                Err(error) => scan.warnings.push(format!("target {target_id}: {error:#}")),
            }
        }
        scan.candidates.sort_by(|left, right| {
            (&left.session_id, &left.target_template_id)
                .cmp(&(&right.session_id, &right.target_template_id))
        });
        scan.candidates.dedup_by(|left, right| {
            left.session_id == right.session_id
                && left.target_template_id == right.target_template_id
        });
        scan
    }

    pub async fn adopt_orphan_worker(
        &mut self,
        session_id: &str,
        target_id: &str,
        profile_override: Option<&str>,
        bundle_override: Option<&str>,
        executor: &impl CommandExecutor,
    ) -> Result<()> {
        if self.state.sessions.contains_key(session_id) {
            bail!("session {session_id} is already tracked");
        }
        let candidate = self
            .scan_orphan_workers(executor)
            .candidates
            .into_iter()
            .find(|candidate| {
                candidate.session_id == session_id && candidate.target_template_id == target_id
            })
            .with_context(|| {
                format!("no managed orphan {session_id} was found on target {target_id:?}")
            })?;
        let profile_id = profile_override
            .map(str::to_owned)
            .or_else(|| {
                candidate
                    .ownership
                    .as_ref()
                    .map(|marker| marker.profile_id.clone())
            })
            .context("orphan has no ownership marker; pass --profile")?;
        let bundle_id = bundle_override
            .map(str::to_owned)
            .or_else(|| {
                candidate
                    .ownership
                    .as_ref()
                    .map(|marker| marker.bundle_id.clone())
            })
            .context("orphan has no ownership marker; pass --bundle")?;
        let profile = self
            .config
            .profiles
            .get(&profile_id)
            .with_context(|| format!("unknown profile {profile_id:?}"))?;
        self.config
            .bundles
            .get(&bundle_id)
            .with_context(|| format!("unknown bundle {bundle_id:?}"))?;
        let now = now();
        let record = SessionRecord {
            id: session_id.to_owned(),
            title: format!("Recovered {}", &session_id[..session_id.len().min(8)]),
            harness_kind: profile.kind,
            last_profile: profile_id,
            bundle_id,
            project_directory: None,
            managed_worktree: None,
            target_template_id: target_id.to_owned(),
            resource_allocation: None,
            additional_mounts: Vec::new(),
            state: SessionState::Disconnected,
            target: Some(candidate.locator),
            native_session_id: None,
            acp_session_title: None,
            session_title_override: None,
            created_at: now.clone(),
            updated_at: now,
            detached_after_event_ordinal: 0,
            draft_input: String::new(),
            last_error: None,
            last_checkpoint_error: None,
            checkpoint: None,
        };
        let backend = backend_locator(record.target.as_ref().unwrap(), &record, &self.config)?;
        let spec = hel_targets::reconnect_plan(&backend, session_id)?
            .commands
            .into_iter()
            .next()
            .context("reconnect plan is empty")?;
        self.state
            .sessions
            .insert(session_id.to_owned(), record.clone());
        self.persist_session_state(session_id)?;
        let mut relay = StandaloneSession::connect_command(&spec, session_id)
            .await
            .context("orphan relay did not complete the v1 handshake")?;
        let native_session_id = wait_for_native_session(&mut relay, executor).await?;
        self.state.sessions.insert(session_id.to_owned(), record);
        self.mark_worker_connected(session_id, Some(native_session_id))?;
        if let Some(title) = relay.snapshot().materialized.session_title {
            crate::hel_database::set_session_acp_title(session_id, Some(&title))?;
            self.state
                .sessions
                .get_mut(session_id)
                .expect("adopted session disappeared while saving its ACP title")
                .acp_session_title = Some(title);
        }
        Ok(())
    }

    pub fn destroy_orphan_worker(
        &self,
        session_id: &str,
        target_id: &str,
        confirmation: &str,
        executor: &impl CommandExecutor,
    ) -> Result<()> {
        if confirmation != session_id {
            bail!("refusing destructive recovery: --confirm must exactly match the session ID");
        }
        let candidate = self
            .scan_orphan_workers(executor)
            .candidates
            .into_iter()
            .find(|candidate| {
                candidate.session_id == session_id && candidate.target_template_id == target_id
            })
            .with_context(|| {
                format!("no managed orphan {session_id} was found on target {target_id:?}")
            })?;
        let template = self.config.targets.get(target_id).unwrap();
        let backend = recovery_backend_locator(template, &candidate.locator, session_id)?;
        hel_targets::close_plan(&backend, session_id)?
            .execute(executor)
            .map(|_| ())
    }

    /// Complete a mount source at the same host that will run the container.
    pub fn complete_mount_source(
        &self,
        target_id: &str,
        prefix: &str,
        executor: &impl CommandExecutor,
    ) -> Result<Vec<String>> {
        let target = self
            .config
            .targets
            .get(target_id)
            .with_context(|| format!("unknown target template {target_id:?}"))?;
        match target {
            TargetTemplate::LocalPodman { .. }
            | TargetTemplate::AppleContainer { .. }
            | TargetTemplate::AwsEc2 { .. } => Ok(hel_targets::local_directory_completions(prefix)),
            TargetTemplate::SshPodman { ssh, .. } => {
                hel_targets::ssh_directory_completions(&backend_ssh(ssh), prefix, executor)
            }
            TargetTemplate::LocalBare | TargetTemplate::SshBare { .. } => {
                bail!("resource path completion is unsupported for bare targets")
            }
        }
    }

    /// Verify a mount source on the host where Hel will consume it.
    pub fn validate_mount_source(
        &self,
        target_id: &str,
        source: &Path,
        executor: &impl CommandExecutor,
    ) -> Result<()> {
        let target = self
            .config
            .targets
            .get(target_id)
            .with_context(|| format!("unknown target template {target_id:?}"))?;
        let exists = match target {
            TargetTemplate::LocalPodman { .. }
            | TargetTemplate::AppleContainer { .. }
            | TargetTemplate::AwsEc2 { .. } => std::fs::metadata(source)
                .map(|metadata| metadata.is_dir())
                .or_else(|error| {
                    if error.kind() == std::io::ErrorKind::NotFound {
                        Ok(false)
                    } else {
                        Err(error)
                    }
                })
                .with_context(|| format!("inspect resource source {}", source.display()))?,
            TargetTemplate::SshPodman { ssh, .. } => {
                hel_targets::ssh_directory_exists(&backend_ssh(ssh), source, executor)?
            }
            TargetTemplate::LocalBare | TargetTemplate::SshBare { .. } => {
                bail!("resource attachments are unsupported for bare targets")
            }
        };
        ensure!(
            exists,
            "source path {} does not exist or is not a directory",
            source.display()
        );
        Ok(())
    }

    /// Verify a bare project before leaving the project-directory dialog.
    pub fn validate_project_directory(
        &self,
        target_id: &str,
        directory: &Path,
        executor: &impl CommandExecutor,
    ) -> Result<()> {
        let target = self
            .config
            .targets
            .get(target_id)
            .with_context(|| format!("unknown target template {target_id:?}"))?;
        match target {
            TargetTemplate::LocalBare => {
                ensure!(
                    directory.is_dir(),
                    "project directory does not exist or is not a directory"
                );
                let output = executor.execute(
                    &CommandSpec::new(
                        "git",
                        [
                            "-C",
                            &directory.to_string_lossy(),
                            "rev-parse",
                            "--verify",
                            "HEAD",
                        ],
                    )
                    .purpose("verify local bare Git project"),
                )?;
                ensure!(
                    output.status == 0
                        && !String::from_utf8_lossy(&output.stdout).trim().is_empty(),
                    "project directory has no valid Git HEAD: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                );
                Ok(())
            }
            TargetTemplate::SshBare { ssh, .. } => {
                hel_targets::validate_bare_project_directory(&backend_ssh(ssh), directory, executor)
            }
            _ => bail!("project directory validation requires a bare target"),
        }
    }

    fn prepare_managed_raw_worktree(
        &mut self,
        session_id: &str,
        executor: &impl CommandExecutor,
    ) -> Result<bool> {
        let session = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?
            .clone();
        let Some(selected) = session.project_directory.as_deref() else {
            return Ok(false);
        };
        if session.managed_worktree.is_some() {
            return Ok(false);
        }
        let template = self
            .config
            .targets
            .get(&session.target_template_id)
            .context("raw session target template disappeared during provisioning")?;
        let target = managed_worktree_target(template)?;
        let inspection = inspect_raw_project(executor, &target, selected)?;
        if !inspection.primary_checkout {
            return Ok(false);
        }
        let relative_directory = inspection
            .source_project_directory
            .strip_prefix(&inspection.source_repository)
            .context("raw project directory is outside its repository")?
            .to_path_buf();
        let worktree_root = inspection
            .source_repository
            .join(".hel")
            .join("worktrees")
            .join(session_id);
        let managed = ManagedWorktree {
            source_project_directory: inspection.source_project_directory,
            source_repository: inspection.source_repository,
            worktree_root: worktree_root.clone(),
            branch: format!("hel/{session_id}"),
            target,
        };
        ensure_managed_worktree_available(executor, &managed)?;
        let record = self.state.sessions.get_mut(session_id).unwrap();
        record.project_directory = Some(worktree_root.join(relative_directory));
        record.managed_worktree = Some(managed.clone());
        record.updated_at = now();
        self.persist_session_state(session_id)?;
        create_managed_worktree(executor, &managed, inspection.upstream.as_deref())?;
        Ok(true)
    }

    fn cleanup_new_session_worktree(
        &self,
        session_id: &str,
        executor: &impl CommandExecutor,
    ) -> Result<()> {
        let Some(worktree) = self
            .state
            .sessions
            .get(session_id)
            .and_then(|session| session.managed_worktree.as_ref())
        else {
            return Ok(());
        };
        cleanup_managed_worktree(executor, worktree)
    }

    fn cleanup_new_session_worktree_after_failure(
        &self,
        session_id: &str,
        executor: &impl CommandExecutor,
    ) -> Result<()> {
        if executor.cancellation_requested() {
            let cleanup_executor =
                CancellableProcessExecutor::with_timeout(Duration::from_secs(15));
            self.cleanup_new_session_worktree(session_id, &cleanup_executor)
        } else {
            self.cleanup_new_session_worktree(session_id, executor)
        }
    }

    fn fail_new_session_with_cleanup(
        &mut self,
        session_id: &str,
        error: anyhow::Error,
        executor: &impl CommandExecutor,
    ) -> Result<anyhow::Error> {
        let original = format!("{error:#}");
        let cleanup_error = self
            .cleanup_new_session_worktree_after_failure(session_id, executor)
            .err()
            .map(|cleanup_error| format!("{cleanup_error:#}"));
        let failure = apply_failed_new_session_rollback(
            &mut self.state,
            session_id,
            &original,
            cleanup_error,
        );
        self.persist_session_state(session_id)?;
        Ok(failure)
    }

    pub fn register_session(
        &mut self,
        profile_id: &str,
        bundle_id: &str,
        target_id: &str,
        title: impl Into<String>,
    ) -> Result<String> {
        self.register_session_with_mounts(profile_id, bundle_id, target_id, title, Vec::new())
    }

    pub fn register_session_with_mounts(
        &mut self,
        profile_id: &str,
        bundle_id: &str,
        target_id: &str,
        title: impl Into<String>,
        additional_mounts: Vec<AdditionalMount>,
    ) -> Result<String> {
        self.register_session_with_mounts_allow_dirty(
            profile_id,
            bundle_id,
            target_id,
            title,
            additional_mounts,
            false,
        )
    }

    pub fn register_session_with_mounts_allow_dirty(
        &mut self,
        profile_id: &str,
        bundle_id: &str,
        target_id: &str,
        title: impl Into<String>,
        additional_mounts: Vec<AdditionalMount>,
        allow_dirty_local: bool,
    ) -> Result<String> {
        self.register_session_with_resources(
            profile_id,
            bundle_id,
            target_id,
            title,
            SessionLaunchOptions {
                additional_mounts,
                allow_dirty_local,
                resource_allocation: None,
                project_directory: None,
                session_title_override: None,
            },
        )
    }

    pub fn register_session_with_resources(
        &mut self,
        profile_id: &str,
        bundle_id: &str,
        target_id: &str,
        title: impl Into<String>,
        options: SessionLaunchOptions,
    ) -> Result<String> {
        let SessionLaunchOptions {
            additional_mounts,
            allow_dirty_local,
            resource_allocation,
            project_directory,
            session_title_override,
        } = options;
        let session_title_override = match session_title_override {
            Some(title) => {
                Some(normalize_session_title(&title).context("session name cannot be empty")?)
            }
            None => None,
        };
        let profile = self
            .config
            .profiles
            .get(profile_id)
            .with_context(|| format!("unknown profile {profile_id:?}"))?;
        let template = self
            .config
            .targets
            .get(target_id)
            .with_context(|| format!("unknown target template {target_id:?}"))?;
        if project_directory.is_some() != is_bare_project_target(template) {
            bail!("raw project directories require a bare target, and bare targets require one");
        }
        if let Some(path) = &project_directory
            && (!path.is_absolute()
                || path
                    .components()
                    .any(|part| part == std::path::Component::ParentDir))
        {
            bail!("bare project directory must be an absolute safe path");
        }
        let bundle = project_directory
            .is_none()
            .then(|| self.config.bundles.get(bundle_id))
            .flatten();
        if project_directory.is_none() && bundle.is_none() {
            bail!("unknown bundle {bundle_id:?}");
        }
        let dirty = bundle
            .map(dirty_local_repositories)
            .transpose()?
            .unwrap_or_default();
        if !allow_dirty_local && !dirty.is_empty() {
            let repositories = dirty
                .iter()
                .map(|repository| format!("{} ({})", repository.path.display(), repository.summary))
                .collect::<Vec<_>>()
                .join(", ");
            bail!(
                "local repositories have uncommitted changes: {repositories}; explicit confirmation is required"
            );
        }
        validate_resource_allocation(template, resource_allocation.as_ref())?;
        if !additional_mounts.is_empty() && mount_history_host(template).is_none() {
            bail!("attached resources are unsupported for this target");
        }
        hel_targets::validate_additional_mounts(&additional_mounts)?;
        let id = new_session_id()?;
        let now = now();
        let record = SessionRecord {
            id: id.clone(),
            title: title.into(),
            harness_kind: profile.kind,
            last_profile: profile_id.to_string(),
            bundle_id: bundle_id.to_string(),
            project_directory,
            managed_worktree: None,
            target_template_id: target_id.to_string(),
            resource_allocation,
            additional_mounts: additional_mounts.clone(),
            state: SessionState::Provisioning,
            target: None,
            native_session_id: None,
            acp_session_title: None,
            session_title_override,
            created_at: now.clone(),
            updated_at: now,
            detached_after_event_ordinal: 0,
            draft_input: String::new(),
            last_error: None,
            last_checkpoint_error: None,
            checkpoint: None,
        };
        self.state.sessions.insert(id.clone(), record);
        if let Some(host) = mount_history_host(template) {
            self.state.remember_mount_sources(&host, &additional_mounts);
        }
        self.persist_session_state(&id)?;
        if let Some(host) = mount_history_host(template) {
            crate::hel_database::remember_mount_sources(&host, &additional_mounts)?;
        }
        Ok(id)
    }

    pub async fn provision_session(&mut self, session_id: &str) -> Result<()> {
        self.provision_session_controlled(session_id, &ProcessExecutor)
            .await
    }

    pub async fn provision_session_controlled(
        &mut self,
        session_id: &str,
        executor: &(impl CommandExecutor + Sync),
    ) -> Result<()> {
        self.provision_session_controlled_with_commit(session_id, executor, || Ok(()))
            .await
    }

    pub async fn provision_session_controlled_with_commit(
        &mut self,
        session_id: &str,
        executor: &(impl CommandExecutor + Sync),
        grant_commit: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        let github_token = controller_github_token();
        self.provision_session_with_github_token(session_id, executor, github_token.as_deref())
            .await?;
        match self.install_and_connect_worker(session_id, executor).await {
            Ok(native_session_id) => {
                if let Err(error) = grant_commit() {
                    return Err(self.rollback_failed_new_session(session_id, error, executor)?);
                }
                self.mark_worker_connected(session_id, native_session_id)
            }
            Err(error) => Err(self.rollback_failed_new_session(session_id, error, executor)?),
        }
    }

    fn rollback_failed_new_session(
        &mut self,
        session_id: &str,
        error: anyhow::Error,
        executor: &impl CommandExecutor,
    ) -> Result<anyhow::Error> {
        let session = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?
            .clone();
        let target_cleanup = match session.target.as_ref() {
            Some(locator) => (|| -> Result<()> {
                let backend = backend_locator(locator, &session, &self.config)?;
                hel_targets::close_plan(&backend, session_id)?
                    // Rollback must remain possible after the foreground
                    // operation's cancellation token has been set.
                    .execute(&CancellableProcessExecutor::with_timeout(
                        Duration::from_secs(15),
                    ))
                    .map(|_| ())
            })(),
            None => Ok(()),
        };
        let worktree_cleanup =
            self.cleanup_new_session_worktree_after_failure(session_id, executor);
        let cleanup_error = [target_cleanup, worktree_cleanup]
            .into_iter()
            .filter_map(Result::err)
            .map(|error| format!("{error:#}"))
            .collect::<Vec<_>>()
            .join("; ");
        let original = format!("{error:#}");
        let original = match persist_launch_failure(session_id, &original) {
            Ok(path) => format!("{original}; full diagnostic saved to {}", path.display()),
            Err(save_error) => {
                format!("{original}; saving the local diagnostic failed: {save_error:#}")
            }
        };
        let failure = apply_failed_new_session_rollback(
            &mut self.state,
            session_id,
            &original,
            (!cleanup_error.is_empty()).then_some(cleanup_error),
        );
        self.persist_session_state(session_id)?;
        Ok(failure)
    }

    pub fn rename_session(&mut self, session_id: &str, title: &str) -> Result<String> {
        let title = normalize_session_title(title).context("session name cannot be empty")?;
        ensure!(
            self.state.sessions.contains_key(session_id),
            "unknown session {session_id}"
        );
        let updated_at = now();
        crate::hel_database::set_session_title_override(session_id, &title, &updated_at)?;
        let record = self
            .state
            .sessions
            .get_mut(session_id)
            .expect("session was checked before updating its title");
        record.session_title_override = Some(title.clone());
        record.updated_at = updated_at;
        Ok(title)
    }

    pub fn mark_session_detached_after(
        &mut self,
        session_id: &str,
        event_ordinal: u64,
    ) -> Result<()> {
        ensure!(
            self.state.sessions.contains_key(session_id),
            "unknown session {session_id}"
        );
        let receipt = advance_detached_after_event_ordinal(session_id, event_ordinal)?;
        let record = self
            .state
            .sessions
            .get_mut(session_id)
            .expect("session was checked before advancing its read receipt");
        record.detached_after_event_ordinal = receipt;
        Ok(())
    }

    pub async fn provision_session_with(
        &mut self,
        session_id: &str,
        executor: &(impl CommandExecutor + Sync),
    ) -> Result<()> {
        self.provision_session_with_github_token(session_id, executor, None)
            .await
    }

    async fn provision_session_with_github_token(
        &mut self,
        session_id: &str,
        executor: &(impl CommandExecutor + Sync),
        github_token: Option<&str>,
    ) -> Result<()> {
        self.provision_session_with_failure_disposition(
            session_id,
            executor,
            github_token,
            ProvisioningFailureDisposition::Discard,
        )
        .await
    }

    async fn provision_session_with_failure_disposition(
        &mut self,
        session_id: &str,
        executor: &(impl CommandExecutor + Sync),
        github_token: Option<&str>,
        failure_disposition: ProvisioningFailureDisposition,
    ) -> Result<()> {
        let session = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?
            .clone();
        if session.state != SessionState::Provisioning {
            bail!("session {session_id} is not provisioning");
        }
        let created_worktree = match self.prepare_managed_raw_worktree(session_id, executor) {
            Ok(created) => created,
            Err(error) if failure_disposition == ProvisioningFailureDisposition::Discard => {
                return Err(self.fail_new_session_with_cleanup(session_id, error, executor)?);
            }
            Err(error) => return Err(error),
        };
        let session = self
            .state
            .sessions
            .get(session_id)
            .expect("session retained after managed worktree preparation")
            .clone();
        // Keep planning, preflight, creation, and locator discovery in one
        // result so the caller's failure disposition applies to every error.
        let result = (|| {
            let template = self
                .config
                .targets
                .get(&session.target_template_id)
                .context("target template disappeared during provisioning")?;
            if matches!(template, TargetTemplate::AwsEc2 { .. }) {
                for resource in &session.additional_mounts {
                    ensure!(
                        resource.source.is_dir(),
                        "attached resource source is not a directory: {}",
                        resource.source.display()
                    );
                }
            }
            let mut target = backend_target(template, session.resource_allocation.as_ref())?;
            let runtime_mounts = if matches!(target, hel_targets::TargetTemplate::AwsEc2(_)) {
                &[][..]
            } else {
                session.additional_mounts.as_slice()
            };
            let mut bundle = session
                .project_directory
                .is_none()
                .then(|| self.config.bundles.get(&session.bundle_id))
                .flatten()
                .map(backend_bundle)
                .transpose()?;
            if let Some(token) = github_token
                && inject_github_token(&mut target, token)
                && let Some(bundle) = bundle.as_mut()
            {
                use_github_https_urls(bundle);
            }
            let provision = if let Some(project_directory) = &session.project_directory {
                hel_targets::provision_bare_project_plan(
                    &target,
                    session_id,
                    &project_directory.to_string_lossy(),
                )?
            } else {
                hel_targets::provision_plan(
                    &target,
                    session_id,
                    bundle
                        .as_ref()
                        .context("project bundle disappeared during provisioning")?,
                    runtime_mounts,
                )?
            };

            let outputs =
                preflight_target(template, executor).and_then(|()| provision.execute(executor))?;
            locator_after_provision(
                template,
                &target,
                session_id,
                outputs.first(),
                executor,
                bundle.as_ref(),
            )
            .map_err(|error| {
                match cleanup_failed_provision(template, session_id, outputs.first(), executor) {
                    Some(note) => error.context(note),
                    None => error,
                }
            })
        })();
        let result = match result {
            Err(error)
                if created_worktree
                    && failure_disposition == ProvisioningFailureDisposition::Discard =>
            {
                return Err(self.fail_new_session_with_cleanup(session_id, error, executor)?);
            }
            Err(error) if failure_disposition == ProvisioningFailureDisposition::Preserve => {
                Err(error)
            }
            result => apply_new_session_provisioning_result(&mut self.state, session_id, result),
        };
        if result.is_ok()
            && let Some(session) = self.state.sessions.get(session_id)
            && let Some(directory) = session
                .managed_worktree
                .as_ref()
                .map(|worktree| worktree.source_project_directory.clone())
                .or_else(|| session.project_directory.clone())
            && let Some(template) = self.config.targets.get(&session.target_template_id)
        {
            let host = match template {
                TargetTemplate::LocalBare => Some("local"),
                TargetTemplate::SshBare { ssh, .. } => Some(ssh.host.as_str()),
                _ => None,
            };
            if let Some(host) = host {
                self.state.remember_project_directory(host, &directory);
                crate::hel_database::remember_project_directory(host, &directory)?;
            }
        }
        self.persist_session_state(session_id)?;
        result
    }

    pub fn mark_worker_connected(
        &mut self,
        session_id: &str,
        native_session_id: Option<String>,
    ) -> Result<()> {
        let session = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?;
        if session.target.is_none() {
            bail!("session {session_id} has no provisioned target");
        }
        let updated_at = now();
        crate::hel_database::mark_session_worker_connected(
            session_id,
            native_session_id.as_deref(),
            &updated_at,
        )?;
        let session = self
            .state
            .sessions
            .get_mut(session_id)
            .expect("session disappeared after its worker connection was saved");
        session.state = SessionState::Running;
        if native_session_id.is_some() {
            session.native_session_id = native_session_id;
        }
        session.updated_at = updated_at;
        session.last_error = None;
        Ok(())
    }

    async fn install_and_connect_worker(
        &self,
        session_id: &str,
        executor: &impl CommandExecutor,
    ) -> Result<Option<String>> {
        let (backend, worker_root) = self.prepare_worker_files(session_id, executor)?;
        install_attached_resources(&self.state, session_id, &backend, &worker_root, executor)?;
        self.connect_local_repositories(
            session_id,
            &backend,
            &worker_root,
            executor,
            LocalBootstrap::Seed,
        )?;
        start_worker(executor, &backend, &worker_root)?;
        let reconnect = &hel_targets::reconnect_plan(&backend, session_id)?.commands[0];
        let readiness = async {
            let mut relay =
                connect_started_worker(reconnect, session_id, executor, &backend, &worker_root)
                    .await?;
            let native_session_id = wait_for_native_session(&mut relay, executor).await?;
            Ok(Some(native_session_id))
        }
        .await;
        match readiness {
            Ok(native_session_id) => Ok(native_session_id),
            Err(error) => Err(worker_probe_diagnosis(
                executor,
                &backend,
                &worker_root,
                error,
            )),
        }
    }

    fn prepare_worker_files(
        &self,
        session_id: &str,
        executor: &impl CommandExecutor,
    ) -> Result<(hel_targets::TargetLocator, String)> {
        let session = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?;
        let profile = self
            .config
            .profiles
            .get(&session.last_profile)
            .context("session profile is missing")?;
        let bundle = session
            .project_directory
            .is_none()
            .then(|| self.config.bundles.get(&session.bundle_id))
            .flatten();
        let locator = session
            .target
            .as_ref()
            .context("session target is missing")?;
        let backend = backend_locator(locator, session, &self.config)?;
        let worker_root = hel_targets::worker_root(&backend, session_id)?;
        let target_profile_home = target_profile_home(&backend, session_id, profile);
        let workspace = if let Some(project_directory) = &session.project_directory {
            (project_directory.to_string_lossy().into_owned(), Vec::new())
        } else {
            workspace_paths(
                &backend,
                bundle.context("session bundle is missing")?,
                session_id,
            )?
        };
        let mut additional_directories = workspace
            .1
            .into_iter()
            .map(PathBuf::from)
            .collect::<Vec<_>>();
        additional_directories.extend(
            session
                .additional_mounts
                .iter()
                .map(|resource| resource.destination.clone()),
        );
        let (bridge_command, bridge_args) =
            bridge_launch(profile.kind, profile.executable.as_deref());
        let mut environment = profile.environment.clone();
        environment.insert(profile.home_env().into(), target_profile_home.clone());
        let launch = WorkerLaunchConfig {
            session_id: session_id.to_string(),
            harness: profile.kind,
            bridge_command: PathBuf::from(bridge_command),
            bridge_args,
            environment,
            cwd: PathBuf::from(&workspace.0),
            additional_directories,
            native_session_id: session.native_session_id.clone(),
            force_unrestricted_mode: force_unrestricted_mode(&backend),
        };

        let staging = tempfile::tempdir().context("create worker staging directory")?;
        let launch_path = staging.path().join("launch.json");
        launch.write(&launch_path)?;
        let ownership_path = staging.path().join("ownership.json");
        WorkerOwnership {
            version: WorkerOwnership::VERSION,
            session_id: session_id.to_string(),
            profile_id: session.last_profile.clone(),
            bundle_id: session.bundle_id.clone(),
            target_template_id: session.target_template_id.clone(),
        }
        .write(&ownership_path)?;
        let profile_stage = staging.path().join("profile");
        if !matches!(backend, hel_targets::TargetLocator::LocalBare { .. }) {
            stage_profile(profile, &profile_stage)?;
        }
        let worker_binary = worker_binary_for(&backend, executor)?;

        install_worker_files(
            executor,
            &backend,
            session_id,
            &worker_root,
            &target_profile_home,
            &worker_binary,
            &launch_path,
            &ownership_path,
            &profile_stage,
        )?;
        install_inherited_git_settings(executor, &backend, session_id)?;
        Ok((backend, worker_root))
    }

    /// Point the target's checkouts at the `hel-local` Git proxy and fetch the
    /// committed history it serves. `bootstrap` decides whether a still-empty
    /// checkout is also seeded with the local repository's uncommitted changes.
    fn connect_local_repositories(
        &self,
        session_id: &str,
        backend: &hel_targets::TargetLocator,
        worker_root: &str,
        executor: &impl CommandExecutor,
        bootstrap: LocalBootstrap,
    ) -> Result<()> {
        let session = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?;
        if session.project_directory.is_some() {
            return Ok(());
        }
        let bundle = self
            .config
            .bundles
            .get(&session.bundle_id)
            .context("session bundle is missing")?;
        let local = bundle
            .repositories
            .iter()
            .filter_map(|repository| repository.local.as_ref().map(|path| (repository, path)))
            .collect::<Vec<_>>();
        if local.is_empty() {
            return Ok(());
        }

        let absolute_worker_root =
            absolute_target_path(executor, backend, session_id, worker_root)?;
        let repositories = local
            .iter()
            .map(|(repository, path)| Ok((repository.id.clone(), canonical_repository(path)?)))
            .collect::<Result<BTreeMap<_, _>>>()?;
        ensure_git_broker(session_id, backend, repositories)?;

        let workspace_root = match backend {
            hel_targets::TargetLocator::LocalPodman { .. }
            | hel_targets::TargetLocator::AppleContainer { .. }
            | hel_targets::TargetLocator::SshPodman { .. } => "/workspace".to_owned(),
            hel_targets::TargetLocator::AwsEc2 { workspace, .. }
            | hel_targets::TargetLocator::SshBare { workspace, .. } => workspace.clone(),
            hel_targets::TargetLocator::LocalBare { worker_root } => worker_root.clone(),
        };
        let mut missing = Vec::new();
        for &(repository, source) in &local {
            local_branch(source)?;
            let destination = format!(
                "{workspace_root}/{}",
                repository.destination.to_string_lossy()
            );
            let origin = local_origin_url(&absolute_worker_root, &repository.id);
            for (args, purpose) in [
                (
                    vec![
                        "git".into(),
                        "-C".into(),
                        destination.clone(),
                        "config".into(),
                        "protocol.ext.allow".into(),
                        "always".into(),
                    ],
                    "enable the confined local Git transport",
                ),
                (
                    vec![
                        "git".into(),
                        "-C".into(),
                        destination.clone(),
                        "config".into(),
                        "remote.origin.url".into(),
                        origin,
                    ],
                    "configure local Git origin",
                ),
                (
                    vec![
                        "git".into(),
                        "-C".into(),
                        destination.clone(),
                        "config".into(),
                        "remote.origin.fetch".into(),
                        "+refs/heads/*:refs/remotes/origin/*".into(),
                    ],
                    "configure local Git fetch refspec",
                ),
            ] {
                execute_checked(
                    executor,
                    hel_targets::command_on_locator(backend, session_id, args, purpose)?,
                )?;
            }
            let has_head = executor.execute(&hel_targets::command_on_locator(
                backend,
                session_id,
                vec![
                    "git".into(),
                    "-C".into(),
                    destination.clone(),
                    "rev-parse".into(),
                    "--verify".into(),
                    "HEAD".into(),
                ],
                "inspect local Git bootstrap state",
            )?)?;
            if has_head.status != 0 {
                missing.push((repository, source));
            }
        }
        // Fetch before bootstrapping: the proxy delivers every branch, so the
        // bootstrap archive only has to carry identity and dirty state, and
        // the commit it checks out is already present.
        for (repository, _) in &local {
            let destination = format!(
                "{workspace_root}/{}",
                repository.destination.to_string_lossy()
            );
            execute_checked(
                executor,
                hel_targets::command_on_locator(
                    backend,
                    session_id,
                    vec![
                        "git".into(),
                        "-C".into(),
                        destination.clone(),
                        "fetch".into(),
                        "origin".into(),
                    ],
                    "fetch local Git origin",
                )?,
            )?;
        }
        if bootstrap == LocalBootstrap::Seed && !missing.is_empty() {
            bootstrap_local_repositories(
                executor,
                backend,
                session,
                bundle,
                &workspace_root,
                worker_root,
                &missing,
            )?;
        }
        Ok(())
    }

    /// Collect the dead worker's exit record and log tail for a session whose
    /// worker has become unreachable. Best-effort; returns None when the
    /// target no longer exists or has no diagnostics.
    pub fn diagnose_worker(&self, session_id: &str) -> Option<String> {
        self.diagnose_worker_controlled(session_id, &ProcessExecutor)
    }

    pub fn diagnose_worker_controlled(
        &self,
        session_id: &str,
        executor: &impl CommandExecutor,
    ) -> Option<String> {
        let session = self.state.sessions.get(session_id)?;
        let locator = session.target.as_ref()?;
        let backend = backend_locator(locator, session, &self.config).ok()?;
        let worker_root = hel_targets::worker_root(&backend, session_id).ok()?;
        worker_last_words(executor, &backend, &worker_root)
    }

    pub fn reconnect_command(&self, session_id: &str) -> Result<CommandSpec> {
        let session = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?;
        let locator = session.target.as_ref().context("session has no target")?;
        let backend = backend_locator(locator, session, &self.config)?;
        hel_targets::reconnect_plan(&backend, session_id)?
            .commands
            .into_iter()
            .next()
            .context("reconnect plan is empty")
    }

    pub fn resource_probe(&self, session_id: &str) -> Result<hel_targets::SessionResourceProbe> {
        let session = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?;
        let locator = session.target.as_ref().context("session has no target")?;
        let backend = backend_locator(locator, session, &self.config)?;
        hel_targets::resource_probe(&backend, session_id)
    }

    pub fn deployment_capacity_targets(&self) -> Vec<hel_targets::DeploymentCapacityTarget> {
        use hel_targets::{DeploymentCapacityKind, DeploymentCapacityTarget};

        let mut local_ids = Vec::new();
        let mut ssh_hosts: BTreeMap<String, (Vec<String>, Vec<CommandSpec>)> = BTreeMap::new();
        let mut targets = Vec::new();
        for (target_id, template) in &self.config.targets {
            match template {
                TargetTemplate::LocalBare
                | TargetTemplate::LocalPodman { .. }
                | TargetTemplate::AppleContainer { .. } => {
                    local_ids.push(target_id.clone());
                }
                TargetTemplate::SshBare { ssh, .. } | TargetTemplate::SshPodman { ssh, .. } => {
                    let entry = ssh_hosts.entry(ssh.host.clone()).or_default();
                    entry.0.push(target_id.clone());
                    let command = hel_targets::ssh_host_capacity_command(&backend_ssh(ssh));
                    if !entry.1.contains(&command) {
                        entry.1.push(command);
                    }
                }
                TargetTemplate::AwsEc2 { .. } => {
                    let mut probes = Vec::new();
                    let mut probe_error = None;
                    for session in self.state.sessions.values().filter(|session| {
                        session.target_template_id == *target_id
                            && session.state.is_active()
                            && session.target.is_some()
                    }) {
                        let result = backend_locator(
                            session.target.as_ref().expect("filtered target"),
                            session,
                            &self.config,
                        )
                        .and_then(|locator| {
                            hel_targets::aws_allocated_capacity_command(&locator, &session.id)
                        });
                        match result {
                            Ok(command) => probes.push(command),
                            Err(error) => probe_error = Some(format!("{error:#}")),
                        }
                    }
                    targets.push(DeploymentCapacityTarget {
                        id: format!("aws:{target_id}"),
                        host: target_id.clone(),
                        target_ids: vec![target_id.clone()],
                        kind: DeploymentCapacityKind::AwsFleet,
                        local: false,
                        probes,
                        probe_error,
                    });
                }
            }
        }
        if !local_ids.is_empty() {
            targets.push(DeploymentCapacityTarget {
                id: "local".into(),
                host: "local".into(),
                target_ids: local_ids,
                kind: DeploymentCapacityKind::Host,
                local: true,
                probes: Vec::new(),
                probe_error: None,
            });
        }
        targets.extend(ssh_hosts.into_iter().map(|(host, (target_ids, probes))| {
            DeploymentCapacityTarget {
                id: format!("ssh:{host}"),
                host,
                target_ids,
                kind: DeploymentCapacityKind::Host,
                local: false,
                probes,
                probe_error: None,
            }
        }));
        targets.sort_by(|left, right| left.id.cmp(&right.id));
        targets
    }

    /// Resume an archived logical session on any configured profile and
    /// target. Cross-harness resume restores Git and canonical history, starts
    /// a fresh native session, and supplies the prior transcript as its first
    /// context turn.
    pub async fn resume_session(
        &mut self,
        session_id: &str,
        profile_id: &str,
        target_id: &str,
    ) -> Result<()> {
        self.resume_session_with_resources(session_id, profile_id, target_id, None)
            .await
    }

    pub async fn resume_session_with_resources(
        &mut self,
        session_id: &str,
        profile_id: &str,
        target_id: &str,
        resource_allocation: Option<SessionResourceAllocation>,
    ) -> Result<()> {
        self.resume_session_with_options(
            session_id,
            profile_id,
            target_id,
            None,
            resource_allocation,
        )
        .await
        .map(|_| ())
    }

    pub async fn resume_session_with_options(
        &mut self,
        session_id: &str,
        profile_id: &str,
        target_id: &str,
        additional_mounts: Option<Vec<AdditionalMount>>,
        resource_allocation: Option<SessionResourceAllocation>,
    ) -> Result<MaterializedSession> {
        self.resume_session_with_options_and_queue_disposition(
            session_id,
            profile_id,
            target_id,
            additional_mounts,
            resource_allocation,
            false,
        )
        .await
    }

    pub async fn resume_session_with_queue_disposition(
        &mut self,
        session_id: &str,
        profile_id: &str,
        target_id: &str,
        discard_queue: bool,
    ) -> Result<()> {
        self.resume_session_with_options_and_queue_disposition(
            session_id,
            profile_id,
            target_id,
            None,
            None,
            discard_queue,
        )
        .await
        .map(|_| ())
    }

    pub async fn resume_session_with_options_and_queue_disposition(
        &mut self,
        session_id: &str,
        profile_id: &str,
        target_id: &str,
        additional_mounts: Option<Vec<AdditionalMount>>,
        resource_allocation: Option<SessionResourceAllocation>,
        discard_queue: bool,
    ) -> Result<MaterializedSession> {
        self.resume_session_controlled(
            session_id,
            profile_id,
            target_id,
            SessionResumeOptions {
                additional_mounts,
                resource_allocation,
                discard_queue,
            },
            &ProcessExecutor,
        )
        .await
    }

    pub async fn resume_session_controlled(
        &mut self,
        session_id: &str,
        profile_id: &str,
        target_id: &str,
        options: SessionResumeOptions,
        executor: &(impl CommandExecutor + Sync),
    ) -> Result<MaterializedSession> {
        let SessionResumeOptions {
            additional_mounts,
            resource_allocation,
            discard_queue,
        } = options;
        let previous = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?
            .clone();
        if !matches!(previous.state, SessionState::Archived | SessionState::Error) {
            bail!("session {session_id} is not archived or retryable");
        }
        let checkpoint = previous
            .checkpoint
            .as_ref()
            .context("session has no checkpoint")?;
        let archive = verify_archive_streaming(&checkpoint.archive_path)?;
        if archive.archive_sha256 != checkpoint.sha256 || archive.manifest.session.id != session_id
        {
            bail!("persisted checkpoint verification failed");
        }
        let profile = self
            .config
            .profiles
            .get(profile_id)
            .with_context(|| format!("unknown profile {profile_id:?}"))?
            .clone();
        let target_template = self
            .config
            .targets
            .get(target_id)
            .with_context(|| format!("unknown target template {target_id:?}"))?;
        if let Some(project_directory) = &previous.project_directory {
            if let Some(worktree) = &previous.managed_worktree {
                let resume_target = managed_worktree_target(target_template)?;
                if resume_target != worktree.target {
                    bail!("managed raw project sessions must resume on their original host");
                }
            } else {
                let previous_template = self
                    .config
                    .targets
                    .get(&previous.target_template_id)
                    .context("previous bare target template is missing")?;
                if !is_bare_project_target(target_template)
                    || matches!(previous_template, TargetTemplate::LocalBare)
                        != matches!(target_template, TargetTemplate::LocalBare)
                {
                    bail!("raw project sessions must resume on the same bare target kind");
                }
            }
            self.validate_project_directory(target_id, project_directory, executor)
                .context("raw project is unavailable for resume")?;
        }
        let resource_allocation =
            resource_allocation.or_else(|| previous.resource_allocation.clone());
        let additional_mounts =
            additional_mounts.unwrap_or_else(|| previous.additional_mounts.clone());
        validate_resource_allocation(target_template, resource_allocation.as_ref())?;
        if !additional_mounts.is_empty() && mount_history_host(target_template).is_none() {
            bail!("attached resources are unsupported for this target");
        }
        hel_targets::validate_additional_mounts(&additional_mounts)?;
        let history_host = mount_history_host(target_template);
        let history_mounts = additional_mounts.clone();
        if previous.state == SessionState::Error
            && let Some(locator) = &previous.target
        {
            let backend = backend_locator(locator, &previous, &self.config)?;
            hel_targets::close_plan(&backend, session_id)?
                .execute(executor)
                .context("clean up target from failed resume")?;
        }
        let same_harness = profile.kind == archive.manifest.session.harness_kind;
        let canonical_session = archive.canonical_session.clone();
        let context_bytes = profile
            .context_window_bytes
            .unwrap_or(crate::hel_compaction::DEFAULT_CONTEXT_BYTES);
        let portable_session = (!same_harness).then(|| canonical_session.clone());
        let github_token = controller_github_token();

        let record = self.state.sessions.get_mut(session_id).unwrap();
        record.harness_kind = profile.kind;
        record.last_profile = profile_id.to_string();
        record.target_template_id = target_id.to_string();
        record.resource_allocation = resource_allocation;
        record.additional_mounts = additional_mounts;
        record.target = None;
        record.native_session_id =
            same_harness.then(|| archive.manifest.session.native_session_id.clone());
        record.state = SessionState::Provisioning;
        record.updated_at = now();
        record.last_error = None;
        if let Some(host) = history_host {
            self.state.remember_mount_sources(&host, &history_mounts);
            crate::hel_database::remember_mount_sources(&host, &history_mounts)?;
        }
        self.persist_session_state(session_id)?;

        let result = async {
            self.provision_session_with_failure_disposition(
                session_id,
                executor,
                github_token.as_deref(),
                ProvisioningFailureDisposition::Preserve,
            )
            .await?;
            let (backend, worker_root) = self.prepare_worker_files(session_id, executor)?;
            let harness_home = target_profile_home(&backend, session_id, &profile);
            let workspace_root = if let Some(project_directory) = &previous.project_directory {
                project_directory
                    .parent()
                    .context("bare project directory has no parent")?
                    .to_string_lossy()
                    .into_owned()
            } else {
                match &backend {
                    hel_targets::TargetLocator::LocalPodman { .. }
                    | hel_targets::TargetLocator::AppleContainer { .. }
                    | hel_targets::TargetLocator::SshPodman { .. } => "/workspace".to_string(),
                    hel_targets::TargetLocator::AwsEc2 { workspace, .. }
                    | hel_targets::TargetLocator::SshBare { workspace, .. } => workspace.clone(),
                    hel_targets::TargetLocator::LocalBare { worker_root } => worker_root.clone(),
                }
            };
            let target_path = |path: &str| match &backend {
                hel_targets::TargetLocator::AwsEc2 { .. }
                | hel_targets::TargetLocator::SshBare { .. }
                    if !path.starts_with('/') =>
                {
                    PathBuf::from(format!("~/{path}"))
                }
                _ => PathBuf::from(path),
            };
            // The restore needs the fetched objects: a committed delta bundle
            // cannot be applied without its prerequisites, and a bundle-free
            // snapshot checks out a head commit only the proxy can supply. The
            // archive carries this session's dirty state, so nothing is seeded.
            self.connect_local_repositories(
                session_id,
                &backend,
                &worker_root,
                executor,
                LocalBootstrap::Skip,
            )?;
            let remote_archive = format!("{worker_root}/restore.hel.zip");
            let remote_spec = format!("{worker_root}/restore-spec.json");
            let restore = CheckpointRestoreSpec {
                archive_path: target_path(&remote_archive),
                workspace_root: target_path(&workspace_root),
                relay_root: target_path(&worker_root),
                harness_home: target_path(&harness_home),
                restore_repositories: previous.project_directory.is_none(),
                restore_native: same_harness,
                discard_queued_prompts: discard_queue || !same_harness,
            };
            let staging = tempfile::tempdir().context("create restore staging")?;
            let local_spec = staging.path().join("restore-spec.json");
            std::fs::write(&local_spec, serde_json::to_vec_pretty(&restore)?)?;
            upload_checkpoint_spec(
                executor,
                &backend,
                session_id,
                &checkpoint.archive_path,
                &remote_archive,
            )?;
            upload_checkpoint_spec(executor, &backend, session_id, &local_spec, &remote_spec)?;
            execute_checked(
                executor,
                restore_command(&backend, session_id, &remote_spec)?,
            )?;
            install_attached_resources(&self.state, session_id, &backend, &worker_root, executor)?;
            self.connect_local_repositories(
                session_id,
                &backend,
                &worker_root,
                executor,
                LocalBootstrap::Seed,
            )?;
            let mut restored_projection =
                materialized_session_from_canonical(session_id, &canonical_session)?;
            if discard_queue || !same_harness {
                restored_projection.queued_prompts.clear();
            }
            crate::hel_database::save_materialized_session(&restored_projection)?;
            start_worker(executor, &backend, &worker_root)?;
            let spec = self.reconnect_command(session_id)?;
            let readiness = async {
                let mut relay =
                    connect_started_worker(&spec, session_id, executor, &backend, &worker_root)
                        .await?;
                let native_session_id = wait_for_native_session(&mut relay, executor).await?;
                Ok::<_, anyhow::Error>((relay, native_session_id))
            }
            .await;
            let (mut relay, native_session_id) = readiness
                .map_err(|error| worker_probe_diagnosis(executor, &backend, &worker_root, error))?;
            if same_harness {
                if native_session_id != archive.manifest.session.native_session_id {
                    bail!(
                        "ACP loaded native session {native_session_id}, expected {}",
                        archive.manifest.session.native_session_id
                    );
                }
            } else {
                let portable = portable_session
                    .as_ref()
                    .context("cross-harness resume is missing canonical session")?;
                let context = canonical_handoff_text(portable, context_bytes);
                relay
                    .submit(
                        new_command_id("cross-harness-handoff")?,
                        RelayCommand::Prompt {
                            prompt: vec![ContentBlock::Text(TextContent::new(context))],
                        },
                    )
                    .await?;
                if !discard_queue {
                    for prompt in &canonical_session.queued_prompts {
                        let content = prompt
                            .content
                            .iter()
                            .cloned()
                            .map(serde_json::from_value)
                            .collect::<serde_json::Result<Vec<ContentBlock>>>()?;
                        relay
                            .submit(
                                prompt.command_id.clone(),
                                RelayCommand::Prompt { prompt: content },
                            )
                            .await?;
                    }
                }
            }
            self.mark_worker_connected(session_id, Some(native_session_id))?;
            Ok::<_, anyhow::Error>(relay.sync().await?.materialized)
        }
        .await;
        match result {
            Ok(materialized) => Ok(materialized),
            Err(error) => {
                if let Ok(previous_projection) =
                    materialized_session_from_canonical(session_id, &canonical_session)
                {
                    let _ = crate::hel_database::save_materialized_session(&previous_projection);
                }
                Err(self.rollback_failed_resume(session_id, &previous, error, executor)?)
            }
        }
    }

    fn rollback_failed_resume(
        &mut self,
        session_id: &str,
        previous: &SessionRecord,
        error: anyhow::Error,
        _executor: &impl CommandExecutor,
    ) -> Result<anyhow::Error> {
        let current = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?
            .clone();
        let cleanup = match current.target.as_ref() {
            Some(locator) => (|| -> Result<()> {
                let backend = backend_locator(locator, &current, &self.config)?;
                hel_targets::close_plan(&backend, session_id)?
                    // Use a fresh executor: cancellation applies to the
                    // requested operation, not to its compensating cleanup.
                    .execute(&CancellableProcessExecutor::with_timeout(
                        Duration::from_secs(15),
                    ))
                    .map(|_| ())
            })(),
            None => Ok(()),
        };
        let worktree_cleanup = match (
            current.managed_worktree.as_ref(),
            previous.managed_worktree.as_ref(),
        ) {
            (Some(current), Some(previous)) if current == previous => Ok(()),
            (Some(worktree), _) => cleanup_managed_worktree(
                &CancellableProcessExecutor::with_timeout(Duration::from_secs(15)),
                worktree,
            ),
            (None, _) => Ok(()),
        };
        let cleanup_error = [cleanup, worktree_cleanup]
            .into_iter()
            .filter_map(Result::err)
            .map(|cleanup_error| format!("{cleanup_error:#}"))
            .collect::<Vec<_>>()
            .join("; ");
        let original = format!("{error:#}");
        let record = self.state.sessions.get_mut(session_id).unwrap();
        let failure = apply_failed_resume_rollback(
            record,
            previous,
            &original,
            (!cleanup_error.is_empty()).then_some(cleanup_error),
        );
        self.persist_session_state(session_id)?;
        Ok(failure)
    }

    /// Materialize and locally verify a complete session checkpoint while the
    /// target remains live. A failed export or transfer leaves the previous
    /// archive and target untouched.
    pub async fn checkpoint_session(&mut self, session_id: &str) -> Result<CheckpointMetadata> {
        self.checkpoint_session_controlled(session_id, &ProcessExecutor)
            .await
    }

    pub async fn checkpoint_session_controlled(
        &mut self,
        session_id: &str,
        executor: &(impl CommandExecutor + Sync),
    ) -> Result<CheckpointMetadata> {
        self.checkpoint_session_controlled_with_manager(session_id, executor, None)
            .await
    }

    pub async fn checkpoint_session_managed_controlled(
        &mut self,
        session_id: &str,
        executor: &(impl CommandExecutor + Sync),
        manager: &SessionManagerControl,
    ) -> Result<CheckpointMetadata> {
        self.checkpoint_session_controlled_with_manager(session_id, executor, Some(manager))
            .await
    }

    async fn checkpoint_session_controlled_with_manager(
        &mut self,
        session_id: &str,
        executor: &(impl CommandExecutor + Sync),
        manager: Option<&SessionManagerControl>,
    ) -> Result<CheckpointMetadata> {
        let previous = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?
            .clone();
        ensure!(
            !matches!(
                previous.state,
                SessionState::Closing | SessionState::Destroying
            ),
            "session {session_id} is already closing; resume that close instead of starting an ordinary checkpoint"
        );
        let record = self.state.sessions.get_mut(session_id).unwrap();
        record.state = SessionState::Checkpointing;
        record.updated_at = now();
        record.last_checkpoint_error = None;
        self.persist_session_transition_or_restore(
            session_id,
            &previous,
            "persist checkpointing state before creating a checkpoint",
        )?;

        match self
            .checkpoint_session_latched(
                session_id,
                executor,
                manager,
                LatchExclusivity::ReleaseAfterLatch,
            )
            .await
        {
            Ok(latched) => {
                let artifact = latched.artifact.clone();
                {
                    let record = self.state.sessions.get_mut(session_id).unwrap();
                    record.state = SessionState::Running;
                    record.native_session_id = Some(artifact.native_session_id.clone());
                    record.checkpoint = Some(artifact.metadata.clone());
                    record.updated_at = now();
                    record.last_error = None;
                    record.last_checkpoint_error = None;
                }
                if let Err(error) = self.persist_checkpoint_transition_or_restore(
                    session_id,
                    &previous,
                    "persist verified checkpoint before releasing relay history",
                ) {
                    latched.abandon(session_id).await;
                    return Err(error);
                }
                prune_replaced_checkpoint(previous.checkpoint.as_ref(), &artifact.metadata);
                if let Err(error) = latched.complete().await {
                    // The actor retries a failed submission over a fresh
                    // connection, and the worker cancels barriers whose
                    // submitting connection dropped, so the barrier cannot
                    // dangle even when this report is the last word on it.
                    tracing::warn!(
                        session_id,
                        "verified checkpoint was saved, but its relay barrier release could not be confirmed: {error:#}"
                    );
                }
                Ok(artifact.metadata)
            }
            Err(error) => {
                if let Some(record) = self.state.sessions.get_mut(session_id) {
                    record.state = if previous.state == SessionState::Checkpointing {
                        SessionState::Running
                    } else {
                        previous.state
                    };
                    record.updated_at = now();
                    record.last_checkpoint_error = Some(format!("{error:#}"));
                }
                Err(self.persist_failed_checkpoint_state_or_restore(session_id, &previous, error))
            }
        }
    }

    /// Create, verify, and durably install a recovery archive before allowing
    /// the relay to garbage-collect through its event frontier.
    pub async fn create_recovery_checkpoint(&self, session_id: &str) -> Result<CheckpointArtifact> {
        self.create_recovery_checkpoint_with_manager(session_id, None, &ProcessExecutor)
            .await
    }

    pub async fn create_recovery_checkpoint_managed(
        &self,
        session_id: &str,
        manager: &SessionManagerControl,
    ) -> Result<CheckpointArtifact> {
        self.create_recovery_checkpoint_with_manager(session_id, Some(manager), &ProcessExecutor)
            .await
    }

    pub async fn create_recovery_checkpoint_managed_controlled(
        &self,
        session_id: &str,
        manager: &SessionManagerControl,
        executor: &(impl CommandExecutor + Sync),
    ) -> Result<CheckpointArtifact> {
        self.create_recovery_checkpoint_with_manager(session_id, Some(manager), executor)
            .await
    }

    async fn create_recovery_checkpoint_with_manager(
        &self,
        session_id: &str,
        manager: Option<&SessionManagerControl>,
        executor: &(impl CommandExecutor + Sync),
    ) -> Result<CheckpointArtifact> {
        let previous_checkpoint = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?
            .checkpoint
            .clone();
        let latched = self
            .checkpoint_session_latched(
                session_id,
                executor,
                manager,
                LatchExclusivity::ReleaseAfterLatch,
            )
            .await?;
        let artifact = latched.artifact.clone();
        if let Err(error) = verify_checkpoint_artifact(session_id, &artifact) {
            latched.abandon(session_id).await;
            return Err(remove_uninstalled_checkpoint(
                &artifact.metadata.archive_path,
                error.context("final recovery checkpoint verification"),
            ));
        }
        if let Err(error) = crate::hel_database::record_recovery_success(
            session_id,
            &artifact.native_session_id,
            &artifact.metadata,
        ) {
            latched.abandon(session_id).await;
            return Err(error
                .context("persist verified recovery checkpoint before releasing relay history"));
        }
        if let Err(error) = latched.complete().await {
            // The actor retries a failed submission over a fresh connection,
            // and the worker cancels barriers whose submitting connection
            // dropped, so the barrier cannot dangle.
            tracing::warn!(
                session_id,
                "recovery checkpoint was saved, but its relay barrier release could not be confirmed: {error:#}"
            );
        }
        prune_replaced_checkpoint(previous_checkpoint.as_ref(), &artifact.metadata);
        Ok(artifact)
    }

    async fn checkpoint_session_latched(
        &self,
        session_id: &str,
        executor: &(impl CommandExecutor + Sync),
        manager: Option<&SessionManagerControl>,
        exclusivity: LatchExclusivity,
    ) -> Result<LatchedCheckpoint> {
        let session = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?
            .clone();
        let locator = session
            .target
            .as_ref()
            .context("session has no live target")?;
        let backend = backend_locator(locator, &session, &self.config)?;
        let profile = self
            .config
            .profiles
            .get(&session.last_profile)
            .context("session profile is missing")?;
        let bundle = session
            .project_directory
            .is_none()
            .then(|| self.config.bundles.get(&session.bundle_id))
            .flatten();
        let reconnect = hel_targets::reconnect_plan(&backend, session_id)?
            .commands
            .into_iter()
            .next()
            .context("reconnect plan is empty")?;
        let mut relay = if let Some(manager) = manager {
            let handle = manager
                .wait_for_session(session_id, Duration::from_secs(5))
                .await?;
            let lease = handle.lease_connection().await?;
            ControllerRelayLease::Managed {
                handle,
                lease: Some(lease),
            }
        } else {
            ControllerRelayLease::Standalone(
                StandaloneSession::connect_command(&reconnect, session_id).await?,
            )
        };
        let barrier_command_id = new_command_id("checkpoint")?;
        let barrier = {
            let connection = relay.connection_mut();
            connection
                .submit(
                    barrier_command_id.clone(),
                    RelayCommand::BeginCheckpoint {
                        reason: Some("controller archive checkpoint".into()),
                    },
                )
                .await?;
            wait_for_checkpoint_barrier(connection, &barrier_command_id).await?
        };
        let cursor = barrier
            .operational
            .checkpoint_ready
            .clone()
            .context("relay reported a checkpoint barrier without its ready cursor")?;
        let materialized = barrier.materialized;
        let expected_ordinal = materialized.applied_event_ordinal;
        let expected_digest = materialized.applied_event_digest.clone();
        ensure!(
            expected_ordinal == barrier.operational.latest_ordinal,
            "checkpoint projection frontier {expected_ordinal} does not match relay frontier {}",
            barrier.operational.latest_ordinal
        );
        ensure!(
            expected_digest == barrier.operational.latest_digest,
            "checkpoint projection digest does not match the relay frontier digest"
        );
        ensure!(
            cursor.ordinal == expected_ordinal && cursor.digest == expected_digest,
            "checkpoint-ready cursor does not match the latched controller projection"
        );
        let canonical_session = canonical_session_from_materialized(&materialized)?;
        let native_session_id = barrier
            .operational
            .native_session_id
            .or_else(|| session.native_session_id.clone())
            .context("harness did not report its native session ID")?;

        // The latch holds: this projection sits exactly at the barrier's ready
        // cursor. Exporting and transferring the archive needs the barrier, not
        // the connection, so hand it back and let the dashboard keep syncing
        // and submitting while the slow phase runs.
        if exclusivity == LatchExclusivity::ReleaseAfterLatch {
            relay.end_latch();
        }

        let exported: Result<CheckpointArtifact> = async {
            let worker_root = hel_targets::worker_root(&backend, session_id)?;
            let harness_home = target_profile_home(&backend, session_id, profile);
            let (workspace_root, primary_repository, repositories) =
                if let Some(project_directory) = &session.project_directory {
                    let parent = project_directory
                        .parent()
                        .context("bare project directory has no parent")?;
                    let destination = project_directory
                        .file_name()
                        .context("bare project directory cannot be the filesystem root")?;
                    (
                        parent.to_string_lossy().into_owned(),
                        "project".to_owned(),
                        vec![CheckpointRepositorySpec {
                            id: "project".into(),
                            relative_destination: PathBuf::from(destination),
                            capture: CheckpointRepositoryCapture::MetadataOnly,
                            origin_override: None,
                        }],
                    )
                } else {
                    let bundle = bundle.context("session bundle is missing")?;
                    let workspace_root = match &backend {
                        hel_targets::TargetLocator::LocalPodman { .. }
                        | hel_targets::TargetLocator::AppleContainer { .. }
                        | hel_targets::TargetLocator::SshPodman { .. } => "/workspace".to_string(),
                        hel_targets::TargetLocator::AwsEc2 { workspace, .. }
                        | hel_targets::TargetLocator::SshBare { workspace, .. } => workspace.clone(),
                        hel_targets::TargetLocator::LocalBare { worker_root } => worker_root.clone(),
                    };
                    let repositories = bundle
                        .repositories
                        .iter()
                        .map(|repository| CheckpointRepositorySpec {
                            id: repository.id.clone(),
                            relative_destination: repository.destination.clone(),
                            capture: CheckpointRepositoryCapture::SessionDelta,
                            origin_override: repository
                                .is_local()
                                .then(|| format!("hel-local:{}", repository.id)),
                        })
                        .collect();
                    (workspace_root, bundle.primary_repo.clone(), repositories)
                };
            let target_path = |path: &str| match &backend {
                hel_targets::TargetLocator::AwsEc2 { .. }
                | hel_targets::TargetLocator::SshBare { .. }
                    if !path.starts_with('/') =>
                {
                    PathBuf::from(format!("~/{path}"))
                }
                _ => PathBuf::from(path),
            };
            let remote_spec = format!("{worker_root}/checkpoint-spec.json");
            let remote_archive = format!("{worker_root}/checkpoint.hel.zip");
            let checkpointed_at = now();
            let spec = CheckpointExportSpec {
                session: SessionManifest {
                    id: session.id.clone(),
                    title: session.title.clone(),
                    harness_kind: session.harness_kind,
                    profile_id: session.last_profile.clone(),
                    native_session_id: native_session_id.clone(),
                    created_at: session.created_at.clone(),
                    checkpointed_at: checkpointed_at.clone(),
                    hel_version: env!("CARGO_PKG_VERSION").into(),
                    relay_version: env!("CARGO_PKG_VERSION").into(),
                    adapter_version: "acp-v1".into(),
                },
                target: TargetManifest {
                    template_id: session.target_template_id.clone(),
                    target_kind: target_kind(&backend).into(),
                    details: Default::default(),
                },
                bundle: BundleManifest {
                    id: session.bundle_id.clone(),
                    primary_repository,
                },
                relay_root: target_path(&worker_root),
                harness_home: target_path(&harness_home),
                workspace_root: target_path(&workspace_root),
                repositories,
                canonical_session,
                output_path: target_path(&remote_archive),
            };
            let staging = tempfile::tempdir().context("create checkpoint staging")?;
            let local_spec = staging.path().join("checkpoint-spec.json");
            spec.write(&local_spec)?;
            upload_checkpoint_spec(executor, &backend, session_id, &local_spec, &remote_spec)?;
            let exported = execute_checked(
                executor,
                export_command(&backend, session_id, &remote_spec)?,
            )?;
            let target_checkpoint: crate::hel_checkpoint::TargetCheckpoint =
                serde_json::from_slice(&exported.stdout)
                    .context("decode target checkpoint result")?;
            if target_checkpoint.event_frontier != expected_ordinal {
                bail!(
                    "target checkpoint event frontier changed: expected {expected_ordinal}, found {}",
                    target_checkpoint.event_frontier
                );
            }
            if target_checkpoint.event_frontier_digest != expected_digest {
                bail!("target checkpoint event frontier digest changed");
            }

            // Checkpoint archives are immutable once controller metadata points
            // at them. A repeated checkpoint may have the same event frontier,
            // so a frontier-only name could overwrite the last known-good
            // archive before the metadata swap commits.
            let archive_id = new_command_id("archive")?;
            let destination = sessions_dir().join(format!(
                "{session_id}-{}-{archive_id}.hel.zip",
                target_checkpoint.event_frontier
            ));
            let transfer = CheckpointTransfer {
                locator: &backend,
                session_id,
                remote_archive: &remote_archive,
                destination: &destination,
                expected_event_frontier: Some(target_checkpoint.event_frontier),
                expected_event_frontier_digest: Some(&target_checkpoint.event_frontier_digest),
            };
            let verified = transfer.execute(executor)?;
            let installed_archive = verified.archive_path().to_path_buf();
            let validate_transferred = || -> Result<()> {
                ensure!(
                    verified.sha256() == target_checkpoint.sha256,
                    "target and controller checkpoint checksums differ"
                );
                ensure!(
                    verified.event_frontier_digest() == expected_digest,
                    "verified checkpoint event frontier digest changed"
                );
                Ok(())
            };
            if let Err(error) = validate_transferred() {
                return Err(remove_uninstalled_checkpoint(&installed_archive, error));
            }
            let revalidated = relay.sync_snapshot().await.and_then(|snapshot| {
                validate_checkpoint_barrier_snapshot(&snapshot, &barrier_command_id, &cursor)
            });
            if let Err(error) = revalidated {
                return Err(remove_uninstalled_checkpoint(
                    &installed_archive,
                    error.context("checkpoint barrier changed while transferring its archive"),
                ));
            }
            if let Err(error) = transfer
                .cleanup_plan(&verified)
                .and_then(|plan| plan.execute(executor).map(|_| ()))
            {
                return Err(remove_uninstalled_checkpoint(
                    &installed_archive,
                    error.context("clean target checkpoint staging"),
                ));
            }
            let metadata = CheckpointMetadata {
                archive_path: verified.archive_path().to_path_buf(),
                sha256: verified.sha256().to_string(),
                created_at: checkpointed_at,
                event_frontier: verified.event_frontier(),
            };
            Ok(CheckpointArtifact {
                metadata,
                native_session_id,
                event_frontier_digest: expected_digest,
            })
        }
        .await;

        let artifact = match exported {
            Ok(artifact) => artifact,
            Err(error) => {
                // The barrier freezes ACP dispatch until it ends. Nothing will
                // complete it now, and the connection that opened it is back
                // with the session actor, so cancel it instead of leaving the
                // harness frozen until that connection happens to drop.
                if let Err(cancel_error) = relay.cancel_abandoned_barrier().await {
                    tracing::warn!(
                        session_id,
                        "failed checkpoint could not cancel its relay barrier: {cancel_error:#}"
                    );
                }
                return Err(error);
            }
        };
        Ok(LatchedCheckpoint {
            artifact,
            relay,
            barrier_command_id,
            cursor,
        })
    }

    /// Checkpoint, ask the harness to close, and only then tear down the exact
    /// provisioned target. Checkpoint failure is deliberately non-destructive.
    pub async fn close_session(&mut self, session_id: &str) -> Result<()> {
        self.close_session_controlled(session_id, &ProcessExecutor)
            .await
    }

    pub async fn close_session_controlled(
        &mut self,
        session_id: &str,
        executor: &(impl CommandExecutor + Sync),
    ) -> Result<()> {
        self.close_session_controlled_with_manager(session_id, executor, None)
            .await
    }

    pub async fn close_session_managed_controlled(
        &mut self,
        session_id: &str,
        executor: &(impl CommandExecutor + Sync),
        manager: &SessionManagerControl,
    ) -> Result<()> {
        self.close_session_controlled_with_manager(session_id, executor, Some(manager))
            .await
    }

    async fn close_session_controlled_with_manager(
        &mut self,
        session_id: &str,
        executor: &(impl CommandExecutor + Sync),
        manager: Option<&SessionManagerControl>,
    ) -> Result<()> {
        let previous = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?
            .clone();
        let record = self.state.sessions.get_mut(session_id).unwrap();
        // Persist the close intent before beginning its checkpoint. A process
        // exit anywhere below must leave enough state for the next controller
        // to retry the close, even when no checkpoint has been installed yet.
        apply_close_checkpoint_started(record, now());
        self.persist_session_transition_or_restore(
            session_id,
            &previous,
            "persist closing state before checkpointing the session",
        )?;

        // Close seals the relay at the exact latched cursor, so this checkpoint
        // keeps its exclusive connection until the relay reports Closed.
        let mut latched = match self
            .checkpoint_session_latched(
                session_id,
                executor,
                manager,
                LatchExclusivity::HoldThroughClose,
            )
            .await
        {
            Ok(latched) => latched,
            Err(error) => {
                let record = self.state.sessions.get_mut(session_id).unwrap();
                record.state = previous.state;
                record.updated_at = now();
                record.last_checkpoint_error = Some(format!("{error:#}"));
                return Err(
                    self.persist_failed_checkpoint_state_or_restore(session_id, &previous, error)
                );
            }
        };

        let artifact = latched.artifact.clone();
        let record = self.state.sessions.get_mut(session_id).unwrap();
        record.state = SessionState::Closing;
        record.native_session_id = Some(artifact.native_session_id.clone());
        record.checkpoint = Some(artifact.metadata.clone());
        record.updated_at = now();
        record.last_error = None;
        record.last_checkpoint_error = None;
        self.persist_checkpoint_transition_or_restore(
            session_id,
            &previous,
            "persist verified checkpoint and closing state before sealing the relay",
        )?;
        prune_replaced_checkpoint(previous.checkpoint.as_ref(), &artifact.metadata);

        let close_command_id = new_command_id("close")?;
        let barrier_command_id = latched.barrier_command_id.clone();
        if let Err(error) = latched
            .relay
            .connection_mut()
            .submit(
                close_command_id,
                RelayCommand::Close {
                    barrier_command_id: barrier_command_id.clone(),
                    expected: latched.cursor.clone(),
                },
            )
            .await
        {
            self.record_interrupted_close(session_id, &error)?;
            return Err(error.context("seal verified checkpoint for close"));
        }
        if let Err(error) = latched
            .relay
            .connection_mut()
            .submit(
                new_command_id("checkpoint-complete")?,
                RelayCommand::CompleteCheckpoint { barrier_command_id },
            )
            .await
        {
            self.record_interrupted_close(session_id, &error)?;
            return Err(error.context("release verified close checkpoint"));
        }
        if let Err(error) = wait_for_relay_closed(latched.relay.connection_mut()).await {
            self.record_interrupted_close(session_id, &error)?;
            return Err(error);
        }
        latched.relay.release();

        if let Err(error) =
            self.destroy_after_verified_checkpoint(session_id, &artifact.metadata, executor)
        {
            self.record_interrupted_close(session_id, &error)?;
            return Err(error);
        }
        Ok(())
    }

    /// Resume the durable closing state after a controller restart. If the
    /// relay had accepted Close, wait for it and destroy through the exact
    /// installed checkpoint gate. If it had not, take a fresh checkpoint;
    /// the previously installed archive may have become stale after EOF
    /// released its barrier.
    pub async fn recover_interrupted_close_managed(
        &mut self,
        session_id: &str,
        executor: &(impl CommandExecutor + Sync),
        manager: &SessionManagerControl,
    ) -> Result<()> {
        let (state, verified) = {
            let session = self
                .state
                .sessions
                .get(session_id)
                .with_context(|| format!("unknown session {session_id}"))?;
            ensure!(
                matches!(
                    session.state,
                    SessionState::Closing | SessionState::Destroying
                ),
                "session {session_id} has no interrupted close to recover"
            );
            (session.state, session.checkpoint.clone())
        };
        if state == SessionState::Destroying {
            let verified = verified.context("destroying session has no verified checkpoint")?;
            return self.destroy_after_verified_checkpoint(session_id, &verified, executor);
        }
        ensure!(
            state == SessionState::Closing,
            "session {session_id} has no relay close to recover"
        );
        let handle = manager
            .wait_for_session(session_id, Duration::from_secs(5))
            .await?;
        let mut lease = handle.lease_connection().await?;
        let execution = lease.connection_mut().sync().await?.operational.execution;
        match execution {
            RelayExecutionState::Closed => {}
            RelayExecutionState::Closing => {
                wait_for_relay_closed(lease.connection_mut()).await?;
            }
            RelayExecutionState::Idle | RelayExecutionState::Running => {
                lease.release();
                return self
                    .close_session_controlled_with_manager(session_id, executor, Some(manager))
                    .await;
            }
        }
        lease.release();
        let verified = verified.context("closed relay has no verified checkpoint")?;
        self.destroy_after_verified_checkpoint(session_id, &verified, executor)
    }

    fn record_interrupted_close(&mut self, session_id: &str, error: &anyhow::Error) -> Result<()> {
        let record = self.state.sessions.get_mut(session_id).unwrap();
        apply_interrupted_close_error(record, error, &now());
        self.persist_session_state(session_id)
    }

    /// Execute cleanup only after the close state machine has installed a
    /// verified checkpoint on the record.
    fn destroy_after_verified_checkpoint(
        &mut self,
        session_id: &str,
        verified: &CheckpointMetadata,
        executor: &impl CommandExecutor,
    ) -> Result<()> {
        self.destroy_after_verified_checkpoint_with(
            session_id,
            verified,
            executor,
            crate::hel_database::save_lifecycle_session,
        )
    }

    fn destroy_after_verified_checkpoint_with(
        &mut self,
        session_id: &str,
        verified: &CheckpointMetadata,
        executor: &impl CommandExecutor,
        persist: impl Fn(&SessionRecord) -> Result<()>,
    ) -> Result<()> {
        let session = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?
            .clone();
        ensure!(
            matches!(
                session.state,
                SessionState::Closing | SessionState::Destroying
            ),
            "refusing to destroy session {session_id}: it is not closing or destroying"
        );
        ensure!(
            session.checkpoint.as_ref() == Some(verified),
            "refusing to destroy session {session_id}: verified checkpoint gate is stale"
        );
        if session.state == SessionState::Closing {
            let record = self.state.sessions.get_mut(session_id).unwrap();
            record.state = SessionState::Destroying;
            record.updated_at = now();
            record.last_error = None;
            persist_session_record_transition_or_restore(
                &mut self.state,
                session_id,
                &session,
                "persist destroying state before target cleanup",
                &persist,
            )?;
        }

        let destroying = self
            .state
            .sessions
            .get(session_id)
            .expect("destroying session disappeared")
            .clone();
        verify_installed_checkpoint_gate(session_id, verified)?;
        let locator = destroying
            .target
            .as_ref()
            .context("session has no target")?;
        let backend = backend_locator(locator, &destroying, &self.config)?;
        if let Err(cleanup_error) = hel_targets::close_plan(&backend, session_id)?.execute(executor)
        {
            match hel_targets::cleanup_target_is_confirmed_absent(&backend, session_id, executor) {
                Ok(true) => {}
                Ok(false) => return Err(cleanup_error),
                Err(probe_error) => {
                    return Err(cleanup_error.context(format!(
                        "target cleanup failed and exact absence could not be confirmed: {probe_error:#}"
                    )));
                }
            }
        }
        let record = self.state.sessions.get_mut(session_id).unwrap();
        record.state = SessionState::Archived;
        record.target = None;
        record.updated_at = now();
        record.last_error = None;
        persist_session_record_transition_or_restore(
            &mut self.state,
            session_id,
            &destroying,
            "persist archived state after target cleanup",
            &persist,
        )
    }

    pub fn force_destroy(
        &mut self,
        session_id: &str,
        executor: &impl CommandExecutor,
    ) -> Result<()> {
        let session = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?
            .clone();
        if let Some(locator) = &session.target {
            let backend = backend_locator(locator, &session, &self.config)?;
            hel_targets::close_plan(&backend, session_id)?.execute(executor)?;
        }
        let record = self.state.sessions.get_mut(session_id).unwrap();
        record.state = SessionState::DestroyedWithDataLoss;
        record.target = None;
        record.updated_at = now();
        self.persist_session_state(session_id)
    }

    /// Permanently remove an inactive session and every artifact Hel owns for it.
    /// External cleanup happens before the durable record is dropped so failures
    /// remain visible and retryable.
    pub fn delete_session_controlled(
        &mut self,
        session_id: &str,
        executor: &impl CommandExecutor,
    ) -> Result<()> {
        let session = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?
            .clone();
        if session.state.is_active() {
            bail!("refusing to delete active session {session_id}");
        }
        if let Some(worktree) = &session.managed_worktree {
            cleanup_managed_worktree(executor, worktree)
                .context("remove managed raw-session worktree")?;
        }
        if let Some(checkpoint) = &session.checkpoint
            && let Err(error) = std::fs::remove_file(&checkpoint.archive_path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            return Err(error).with_context(|| {
                format!(
                    "remove session recovery archive {}",
                    checkpoint.archive_path.display()
                )
            });
        }
        crate::hel_database::delete_session(session_id)
            .context("delete paused session from database")?;
        self.state.remove_archived_session(session_id)?;
        Ok(())
    }
}

const MAX_LAUNCH_DIAGNOSTIC_BYTES: usize = 64 * 1024;
const RETAINED_LAUNCH_DIAGNOSTICS: usize = 20;

fn persist_launch_failure(session_id: &str, detail: &str) -> Result<PathBuf> {
    persist_launch_failure_to(&data_dir().join("diagnostics"), session_id, detail)
}

fn persist_launch_failure_to(directory: &Path, session_id: &str, detail: &str) -> Result<PathBuf> {
    crate::hel_config::validate_id("session", session_id)?;
    std::fs::create_dir_all(directory).with_context(|| {
        format!(
            "create launch diagnostics directory {}",
            directory.display()
        )
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700))?;
    }
    let path = directory.join(format!("{session_id}-launch-error.txt"));
    let detail = bounded_launch_diagnostic(detail);
    let body = format!(
        "Hel session launch failure\nsession: {session_id}\nat: {}\n\n{detail}\n",
        now()
    );
    atomic_write(&path, body.as_bytes())?;
    prune_launch_diagnostics(directory)?;
    Ok(path)
}

fn bounded_launch_diagnostic(detail: &str) -> String {
    if detail.len() <= MAX_LAUNCH_DIAGNOSTIC_BYTES {
        return detail.to_owned();
    }
    let mut head_end = MAX_LAUNCH_DIAGNOSTIC_BYTES / 4;
    while !detail.is_char_boundary(head_end) {
        head_end -= 1;
    }
    let tail_bytes = MAX_LAUNCH_DIAGNOSTIC_BYTES - head_end;
    let mut tail_start = detail.len() - tail_bytes;
    while !detail.is_char_boundary(tail_start) {
        tail_start += 1;
    }
    format!(
        "{}\n\n[... launch diagnostic truncated ...]\n\n{}",
        &detail[..head_end],
        &detail[tail_start..]
    )
}

fn prune_launch_diagnostics(directory: &Path) -> Result<()> {
    let mut diagnostics = Vec::new();
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        if !entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.ends_with("-launch-error.txt"))
        {
            continue;
        }
        diagnostics.push((entry.metadata()?.modified()?, entry.path()));
    }
    diagnostics.sort_by_key(|entry| std::cmp::Reverse(entry.0));
    for (_, path) in diagnostics.into_iter().skip(RETAINED_LAUNCH_DIAGNOSTICS) {
        std::fs::remove_file(&path)
            .with_context(|| format!("prune old launch diagnostic {}", path.display()))?;
    }
    Ok(())
}

fn apply_new_session_provisioning_result(
    state: &mut HelState,
    session_id: &str,
    result: Result<TargetLocator>,
) -> Result<()> {
    match result {
        Ok(locator) => {
            let record = state.sessions.get_mut(session_id).unwrap();
            record.target = Some(locator);
            // Provisioning has completed, but Running is reserved for a
            // successful worker handshake.
            record.state = SessionState::Disconnected;
            record.updated_at = now();
            record.last_error = None;
            Ok(())
        }
        Err(error) => {
            state.sessions.remove(session_id);
            Err(error)
        }
    }
}

fn apply_failed_new_session_rollback(
    state: &mut HelState,
    session_id: &str,
    original_error: &str,
    cleanup_error: Option<String>,
) -> anyhow::Error {
    match cleanup_error {
        None => {
            state.sessions.remove(session_id);
            anyhow::anyhow!(
                "{original_error}; partial target removed and provisional session discarded"
            )
        }
        Some(cleanup_error) => {
            let failure = format!(
                "{original_error}; cleanup of the failed session target failed: {cleanup_error}"
            );
            let record = state.sessions.get_mut(session_id).unwrap();
            record.state = SessionState::Error;
            record.updated_at = now();
            record.last_error = Some(format!("worker bootstrap failed: {failure}"));
            anyhow::anyhow!(failure)
        }
    }
}

fn apply_failed_resume_rollback(
    current: &mut SessionRecord,
    previous: &SessionRecord,
    original_error: &str,
    cleanup_error: Option<String>,
) -> anyhow::Error {
    match cleanup_error {
        None => {
            *current = previous.clone();
            current.state = SessionState::Archived;
            current.target = None;
            current.updated_at = now();
            let failure = format!(
                "{original_error}; partial target removed and session returned to archived"
            );
            current.last_error = Some(format!("resume failed: {failure}"));
            anyhow::anyhow!(failure)
        }
        Some(cleanup_error) => {
            let failure = format!(
                "{original_error}; cleanup of the partial resume target failed: {cleanup_error}"
            );
            current.state = SessionState::Error;
            current.updated_at = now();
            current.last_error = Some(format!("resume failed: {failure}"));
            anyhow::anyhow!(failure)
        }
    }
}

fn allocation_vcpus(allocation: &SessionResourceAllocation) -> u64 {
    match allocation {
        SessionResourceAllocation::Container { cpus, .. } => *cpus,
        SessionResourceAllocation::AwsEc2 { vcpus, .. } => *vcpus,
    }
}

fn preflight_target(template: &TargetTemplate, executor: &impl CommandExecutor) -> Result<()> {
    match template {
        TargetTemplate::LocalPodman { .. } => hel_targets::verify_local_podman(executor)
            .map(|_| ())
            .map_err(|error| {
                anyhow::anyhow!(
                    "local Podman preflight failed; run `hel doctor` for actionable prerequisites: {error:#}"
                )
            }),
        TargetTemplate::SshPodman { ssh, .. } => {
            let ssh = backend_ssh(ssh);
            hel_targets::verify_ssh_podman(&ssh, executor)
                .map(|_| ())
                .map_err(|error| {
                    anyhow::anyhow!(
                        "remote Podman preflight failed for {}; run `hel doctor` for actionable prerequisites: {error:#}",
                        ssh.destination
                    )
                })
        }
        TargetTemplate::AppleContainer { .. } => {
            let command = CommandSpec::new("container", ["system", "status"])
                .purpose("preflight Apple container runtime");
            let output = executor.execute(&command).map_err(|error| {
                anyhow::anyhow!(
                    "Apple container preflight failed; run `hel doctor` for actionable prerequisites: {error}"
                )
            })?;
            if output.status != 0 {
                bail!(
                    "Apple container preflight failed; run `hel doctor` for actionable prerequisites: container system status exited {}: {}",
                    output.status,
                    String::from_utf8_lossy(&output.stderr).trim()
                );
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn canonical_handoff_text(snapshot: &CanonicalSessionSnapshot, maximum_bytes: usize) -> String {
    const PREFIX: &str = "Continue this coding session from the portable transcript below. Preserve the user's intent and the work already completed.\n\n";
    let mut transcript = String::new();
    for item in &snapshot.transcript {
        let (label, body) = match &item.body {
            CanonicalTranscriptBody::User { content } => {
                ("User", crate::hel_chat::materialized_content_text(content))
            }
            CanonicalTranscriptBody::Agent { chunks, .. } => {
                ("Agent", crate::hel_chat::materialized_chunks_text(chunks))
            }
            CanonicalTranscriptBody::Thought { chunks, .. } => (
                "Agent reasoning",
                crate::hel_chat::materialized_chunks_text(chunks),
            ),
            CanonicalTranscriptBody::Tool { call } => (
                "Tool",
                serde_json::from_value::<ToolCall>(call.clone())
                    .map(|call| format!("{} [{:?}]", call.title, call.status))
                    .unwrap_or_else(|_| "[invalid tool call]".into()),
            ),
            CanonicalTranscriptBody::Plan { plan } => (
                "Plan",
                serde_json::from_value::<Plan>(plan.clone())
                    .map(|plan| plan.entries)
                    .unwrap_or_default()
                    .into_iter()
                    .map(|entry| {
                        format!(
                            "- [{}] {}",
                            format!("{:?}", entry.status).to_ascii_lowercase(),
                            entry.content
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n"),
            ),
            CanonicalTranscriptBody::System { text } => ("System", text.clone()),
        };
        if !body.trim().is_empty() {
            transcript.push_str(label);
            transcript.push_str(":\n");
            transcript.push_str(&body);
            transcript.push_str("\n\n");
        }
    }
    let available = maximum_bytes.saturating_sub(PREFIX.len());
    if transcript.len() > available {
        let mut start = transcript.len() - available;
        while !transcript.is_char_boundary(start) {
            start += 1;
        }
        transcript.drain(..start);
    }
    format!("{PREFIX}{transcript}")
}

/// A harness such as Codex can spend minutes on its first launch, so the
/// readiness wait has to outlast a slow harness boot rather than a fast one.
const NATIVE_SESSION_STARTUP_TIMEOUT: Duration = Duration::from_secs(300);
/// A freshly started worker binds its control socket only after it recovers
/// durable state, so the first connection attempt is retried for this long.
const WORKER_STARTUP_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
/// Delay between connection attempts against a worker that is still starting.
const WORKER_STARTUP_CONNECT_INTERVAL: Duration = Duration::from_millis(500);
/// How often a wait loop looks for cancellation while it is idle.
const CANCELLATION_POLL_INTERVAL: Duration = Duration::from_millis(25);
/// Marker that opens the exit record a dying worker writes to its root.
const WORKER_EXIT_RECORD_MARKER: &str = "--- worker-exit.json ---";

enum NativeSessionReadiness {
    Waiting,
    Ready(String),
    Closed,
}

trait NativeSessionProbe {
    async fn native_session_readiness(&mut self) -> Result<NativeSessionReadiness>;
}

impl NativeSessionProbe for StandaloneSession {
    async fn native_session_readiness(&mut self) -> Result<NativeSessionReadiness> {
        let snapshot = self.sync().await?;
        if let Some(native_session_id) = snapshot.operational.native_session_id {
            Ok(NativeSessionReadiness::Ready(native_session_id))
        } else if snapshot.operational.execution == RelayExecutionState::Closed {
            Ok(NativeSessionReadiness::Closed)
        } else {
            Ok(NativeSessionReadiness::Waiting)
        }
    }
}

async fn wait_for_native_session(
    relay: &mut impl NativeSessionProbe,
    executor: &impl CommandExecutor,
) -> Result<String> {
    let deadline = tokio::time::Instant::now() + NATIVE_SESSION_STARTUP_TIMEOUT;
    loop {
        if executor.cancellation_requested() {
            bail!("operation cancelled while waiting for ACP runtime startup");
        }
        let readiness = {
            let readiness = relay.native_session_readiness();
            tokio::pin!(readiness);
            loop {
                let now = tokio::time::Instant::now();
                if now >= deadline {
                    bail!(
                        "ACP runtime did not report session startup within {}s",
                        NATIVE_SESSION_STARTUP_TIMEOUT.as_secs()
                    );
                }
                let cancellation_poll = std::cmp::min(deadline, now + CANCELLATION_POLL_INTERVAL);
                tokio::select! {
                    readiness = &mut readiness => break readiness?,
                    _ = tokio::time::sleep_until(cancellation_poll) => {
                        if executor.cancellation_requested() {
                            bail!("operation cancelled while waiting for ACP runtime startup");
                        }
                    }
                }
            }
        };
        if executor.cancellation_requested() {
            bail!("operation cancelled while waiting for ACP runtime startup");
        }
        match readiness {
            NativeSessionReadiness::Ready(native_session_id) => return Ok(native_session_id),
            NativeSessionReadiness::Closed => {
                bail!("ACP runtime stopped before starting its session")
            }
            NativeSessionReadiness::Waiting => {}
        }
        if executor.cancellation_requested() {
            bail!("operation cancelled while waiting for ACP runtime startup");
        }
        if tokio::time::Instant::now() >= deadline {
            bail!(
                "ACP runtime did not report session startup within {}s",
                NATIVE_SESSION_STARTUP_TIMEOUT.as_secs()
            );
        }
        let next_poll = std::cmp::min(
            deadline,
            tokio::time::Instant::now() + std::time::Duration::from_millis(100),
        );
        loop {
            let now = tokio::time::Instant::now();
            if now >= next_poll {
                break;
            }
            tokio::time::sleep_until(std::cmp::min(next_poll, now + CANCELLATION_POLL_INTERVAL))
                .await;
            if executor.cancellation_requested() {
                bail!("operation cancelled while waiting for ACP runtime startup");
            }
        }
    }
}

/// One connection attempt against a worker that was started moments ago, plus
/// a way to notice that the worker already died so the retry loop can stop.
trait StartingWorkerProbe {
    type Relay;

    async fn connect(&mut self) -> Result<Self::Relay>;

    /// Diagnostics from a worker that already recorded its exit, or `None`
    /// while the worker has not reported a death.
    fn death_report(&self) -> Option<String>;
}

struct StartingWorkerConnection<'a, E: CommandExecutor> {
    spec: &'a CommandSpec,
    session_id: &'a str,
    executor: &'a E,
    locator: &'a hel_targets::TargetLocator,
    worker_root: &'a str,
}

impl<E: CommandExecutor> StartingWorkerProbe for StartingWorkerConnection<'_, E> {
    type Relay = StandaloneSession;

    async fn connect(&mut self) -> Result<StandaloneSession> {
        StandaloneSession::connect_command(self.spec, self.session_id).await
    }

    fn death_report(&self) -> Option<String> {
        worker_last_words(self.executor, self.locator, self.worker_root)
            .filter(|last_words| last_words.contains(WORKER_EXIT_RECORD_MARKER))
    }
}

/// Connect to a worker daemon that was just started. The daemon binds its
/// control socket only after it recovers durable state, so the first attempts
/// usually fail; retry until the worker accepts, until the worker reports its
/// own death, or until the startup window closes.
async fn connect_started_worker(
    spec: &CommandSpec,
    session_id: &str,
    executor: &impl CommandExecutor,
    locator: &hel_targets::TargetLocator,
    worker_root: &str,
) -> Result<StandaloneSession> {
    let mut connection = StartingWorkerConnection {
        spec,
        session_id,
        executor,
        locator,
        worker_root,
    };
    connect_to_starting_worker(&mut connection, executor).await
}

async fn connect_to_starting_worker<P: StartingWorkerProbe>(
    probe: &mut P,
    executor: &impl CommandExecutor,
) -> Result<P::Relay> {
    let deadline = tokio::time::Instant::now() + WORKER_STARTUP_CONNECT_TIMEOUT;
    let mut last_error: Option<anyhow::Error> = None;
    loop {
        if executor.cancellation_requested() {
            bail!("operation cancelled while connecting to the worker relay");
        }
        let attempt = {
            let attempt = probe.connect();
            tokio::pin!(attempt);
            loop {
                let now = tokio::time::Instant::now();
                if now >= deadline {
                    break None;
                }
                let cancellation_poll = std::cmp::min(deadline, now + CANCELLATION_POLL_INTERVAL);
                tokio::select! {
                    attempt = &mut attempt => break Some(attempt),
                    _ = tokio::time::sleep_until(cancellation_poll) => {
                        if executor.cancellation_requested() {
                            bail!("operation cancelled while connecting to the worker relay");
                        }
                    }
                }
            }
        };
        let error = match attempt {
            Some(Ok(relay)) => return Ok(relay),
            Some(Err(error)) => error,
            // The attempt was still pending when the window closed.
            None => break,
        };
        // A worker that already wrote its exit record will never accept a
        // connection, so report the recorded cause instead of waiting it out.
        if let Some(death_report) = probe.death_report() {
            return Err(error.context(death_report));
        }
        last_error = Some(error);
        if executor.cancellation_requested() {
            bail!("operation cancelled while connecting to the worker relay");
        }
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        let next_attempt = std::cmp::min(
            deadline,
            tokio::time::Instant::now() + WORKER_STARTUP_CONNECT_INTERVAL,
        );
        loop {
            let now = tokio::time::Instant::now();
            if now >= next_attempt {
                break;
            }
            tokio::time::sleep_until(std::cmp::min(
                next_attempt,
                now + CANCELLATION_POLL_INTERVAL,
            ))
            .await;
            if executor.cancellation_requested() {
                bail!("operation cancelled while connecting to the worker relay");
            }
        }
    }
    let waited = WORKER_STARTUP_CONNECT_TIMEOUT.as_secs();
    match last_error {
        Some(error) => Err(error.context(format!(
            "worker relay did not accept a connection in {waited}s"
        ))),
        None => bail!("worker relay did not accept a connection in {waited}s"),
    }
}

async fn wait_for_checkpoint_barrier(
    relay: &mut StandaloneSession,
    command_id: &str,
) -> Result<ManagedSessionSnapshot> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let snapshot = relay.sync().await?;
        if checkpoint_barrier_is_ready(&snapshot, command_id) {
            return Ok(snapshot);
        }
        if snapshot.operational.execution == RelayExecutionState::Closed {
            bail!("ACP runtime stopped before reaching the checkpoint barrier");
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("ACP relay did not reach checkpoint barrier {command_id}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

fn checkpoint_barrier_is_ready(snapshot: &ManagedSessionSnapshot, command_id: &str) -> bool {
    snapshot.operational.checkpoint_barrier.as_deref() == Some(command_id)
        && snapshot.operational.checkpoint_ready.is_some()
}

/// Prove the barrier that latched an archive is still the same barrier, still
/// held at the same ready cursor.
///
/// The relay frontier may have moved past that cursor: an active ordinary
/// barrier still accepts and journals submissions, it only freezes ACP
/// dispatch. Nothing the harness could write reaches the workspace while
/// dispatch is frozen, so an advanced frontier does not invalidate the archive.
/// Requiring frontier equality here would fail every checkpoint that overlapped
/// a prompt.
fn validate_checkpoint_barrier_snapshot(
    snapshot: &ManagedSessionSnapshot,
    command_id: &str,
    expected: &RelayCursor,
) -> Result<()> {
    ensure!(
        snapshot.operational.checkpoint_barrier.as_deref() == Some(command_id),
        "checkpoint barrier {command_id} is no longer active"
    );
    ensure!(
        snapshot.operational.checkpoint_ready.as_ref() == Some(expected),
        "checkpoint barrier {command_id} has a different ready cursor"
    );
    Ok(())
}

fn remove_uninstalled_checkpoint(path: &Path, error: anyhow::Error) -> anyhow::Error {
    match std::fs::remove_file(path) {
        Ok(()) => error,
        Err(remove_error) if remove_error.kind() == std::io::ErrorKind::NotFound => error,
        Err(remove_error) => error.context(format!(
            "also failed to remove uninstalled checkpoint {}: {remove_error}",
            path.display()
        )),
    }
}

async fn wait_for_relay_closed(relay: &mut StandaloneSession) -> Result<()> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if relay.sync().await?.operational.execution == RelayExecutionState::Closed {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("ACP runtime did not close within 30 seconds");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerBinaryAvailability {
    Local {
        path: PathBuf,
        source: String,
    },
    Remote {
        url: String,
        sha256: String,
        triple: String,
    },
}

fn packaged_worker_binary_path(directory: &Path, triple: &str) -> PathBuf {
    directory.join(format!("hel-worker-{triple}"))
}

/// Find a worker source without downloading it.
///
/// Container provisioning resolves this after discovering the target
/// architecture. Doctor uses the same lookup with the selected container's
/// expected architecture, so it can recommend a fix without creating a
/// container or making a network request.
pub fn worker_binary_prerequisite_for_arch(arch: &str) -> Result<WorkerBinaryAvailability> {
    let triple = format!("{arch}-unknown-linux-musl");
    if let Some(path) = std::env::var_os("HEL_WORKER_BINARY").map(PathBuf::from) {
        if !path.is_file() {
            bail!("HEL_WORKER_BINARY is not a file: {}", path.display());
        }
        return Ok(WorkerBinaryAvailability::Local {
            path,
            source: "HEL_WORKER_BINARY".into(),
        });
    }
    let current = std::env::current_exe().context("resolve Hel controller binary")?;
    let mut candidates = Vec::new();
    if let Some(directory) = std::env::var_os("HEL_WORKER_DIR").map(PathBuf::from) {
        candidates.push((
            packaged_worker_binary_path(&directory, &triple),
            "HEL_WORKER_DIR",
        ));
        candidates.push((directory.join(&triple).join("hel"), "HEL_WORKER_DIR"));
    }
    if let Some(directory) = current.parent() {
        candidates.push((
            packaged_worker_binary_path(directory, &triple),
            "beside the Hel binary",
        ));
        // Development checkout: a controller at target/<profile>/hel finds its
        // musl sibling at target/<triple>/<profile>/hel. The static build is
        // preferred because the target's glibc may be older than the host's.
        if let (Some(profile), Some(target_dir)) = (
            directory.file_name().map(std::ffi::OsString::from),
            directory.parent(),
        ) {
            candidates.push((
                target_dir.join(&triple).join(profile).join("hel"),
                "development musl sibling",
            ));
        }
    }
    if let Some((path, source)) = candidates.into_iter().find(|(path, _)| path.is_file()) {
        return Ok(WorkerBinaryAvailability::Local {
            path,
            source: source.into(),
        });
    }
    if cfg!(target_os = "linux")
        && ((arch == "x86_64" && cfg!(target_arch = "x86_64"))
            || (arch == "aarch64" && cfg!(target_arch = "aarch64")))
    {
        return Ok(WorkerBinaryAvailability::Local {
            path: stable_running_executable(&current)?,
            source: "native Linux Hel binary".into(),
        });
    }
    if let Ok(template) = std::env::var("HEL_WORKER_URL") {
        let expected = std::env::var("HEL_WORKER_SHA256")
            .context("HEL_WORKER_URL requires HEL_WORKER_SHA256")?;
        validate_worker_sha256(&expected)?;
        return Ok(WorkerBinaryAvailability::Remote {
            url: template.replace("{target}", &triple),
            sha256: expected,
            triple,
        });
    }
    bail!(
        "no Linux worker for {triple}; install hel-worker-{triple} beside Hel, set HEL_WORKER_DIR/HEL_WORKER_BINARY, or configure HEL_WORKER_URL and HEL_WORKER_SHA256"
    )
}

fn stable_running_executable(current: &Path) -> Result<PathBuf> {
    if current.is_file() {
        return Ok(current.to_path_buf());
    }
    #[cfg(target_os = "linux")]
    {
        let proc_exe = PathBuf::from(format!("/proc/{}/exe", std::process::id()));
        let directory = data_dir().join("workers").join("running");
        let cached = directory.join(format!("hel-{}", std::process::id()));
        materialize_running_executable(current, &proc_exe, &cached)
    }
    #[cfg(not(target_os = "linux"))]
    bail!(
        "resolved Hel controller executable is no longer readable: {}",
        current.display()
    )
}

#[cfg(target_os = "linux")]
fn materialize_running_executable(
    current: &Path,
    proc_exe: &Path,
    cached: &Path,
) -> Result<PathBuf> {
    if !proc_exe.is_file() {
        bail!(
            "resolved Hel controller executable is no longer readable: {}",
            current.display()
        );
    }
    let parent = cached
        .parent()
        .context("worker executable cache has no parent")?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("create worker executable cache {}", parent.display()))?;
    std::fs::copy(proc_exe, cached).with_context(|| {
        format!(
            "copy running Hel executable from {} after {} was replaced",
            proc_exe.display(),
            current.display()
        )
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(cached, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(cached.to_path_buf())
}

fn worker_binary_for(
    locator: &hel_targets::TargetLocator,
    executor: &impl CommandExecutor,
) -> Result<PathBuf> {
    let arch = target_architecture(locator, executor)?;
    match worker_binary_prerequisite_for_arch(arch)? {
        WorkerBinaryAvailability::Local { path, .. } => Ok(path),
        WorkerBinaryAvailability::Remote {
            url,
            sha256,
            triple,
        } => download_worker(&url, &sha256, &triple),
    }
}

fn target_architecture(
    locator: &hel_targets::TargetLocator,
    executor: &impl CommandExecutor,
) -> Result<&'static str> {
    let command = match locator {
        hel_targets::TargetLocator::LocalBare { .. } => CommandSpec::new("uname", ["-m"]),
        hel_targets::TargetLocator::LocalPodman { container_id } => {
            CommandSpec::new("podman", ["exec", container_id, "uname", "-m"])
        }
        hel_targets::TargetLocator::AppleContainer { container_id } => {
            CommandSpec::new("container", ["exec", container_id, "uname", "-m"])
        }
        hel_targets::TargetLocator::AwsEc2 { ssh, .. }
        | hel_targets::TargetLocator::SshBare { ssh, .. } => ssh_command_spec(ssh, ["uname", "-m"]),
        hel_targets::TargetLocator::SshPodman { ssh, container_id } => {
            ssh_command_spec(ssh, ["podman", "exec", container_id, "uname", "-m"])
        }
    }
    .purpose("detect target architecture");
    let output = execute_checked(executor, command)?;
    match String::from_utf8(output.stdout)?.trim() {
        "x86_64" | "amd64" => Ok("x86_64"),
        "aarch64" | "arm64" => Ok("aarch64"),
        architecture => bail!("unsupported target architecture {architecture:?}"),
    }
}

fn download_worker(url: &str, expected_sha256: &str, triple: &str) -> Result<PathBuf> {
    validate_worker_sha256(expected_sha256)?;
    let directory = data_dir()
        .join("workers")
        .join(env!("CARGO_PKG_VERSION"))
        .join(triple);
    std::fs::create_dir_all(&directory)?;
    let destination = directory.join("hel");
    if destination.is_file() {
        let bytes = std::fs::read(&destination)?;
        if format!("{:x}", Sha256::digest(&bytes)).eq_ignore_ascii_case(expected_sha256) {
            return Ok(destination);
        }
    }
    let bytes = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .build()?
        .get(url)
        .send()?
        .error_for_status()?
        .bytes()?;
    let actual = format!("{:x}", Sha256::digest(&bytes));
    if !actual.eq_ignore_ascii_case(expected_sha256) {
        bail!("downloaded worker checksum mismatch: expected {expected_sha256}, got {actual}");
    }
    let mut temporary = tempfile::NamedTempFile::new_in(&directory)?;
    std::io::Write::write_all(&mut temporary, &bytes)?;
    temporary.as_file_mut().sync_all()?;
    temporary
        .persist(&destination)
        .map_err(|error| error.error)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&destination, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(destination)
}

fn validate_worker_sha256(expected_sha256: &str) -> Result<()> {
    if expected_sha256.len() != 64 || !expected_sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        bail!("HEL_WORKER_SHA256 must be a 64-character hexadecimal digest");
    }
    Ok(())
}

fn target_kind(locator: &hel_targets::TargetLocator) -> &'static str {
    match locator {
        hel_targets::TargetLocator::LocalBare { .. } => "local-bare",
        hel_targets::TargetLocator::LocalPodman { .. } => "local-podman",
        hel_targets::TargetLocator::AppleContainer { .. } => "apple-container",
        hel_targets::TargetLocator::AwsEc2 { .. } => "aws-ec2",
        hel_targets::TargetLocator::SshBare { .. } => "ssh-bare",
        hel_targets::TargetLocator::SshPodman { .. } => "ssh-podman",
    }
}

fn target_profile_home(
    locator: &hel_targets::TargetLocator,
    session_id: &str,
    profile: &crate::hel_config::HarnessProfile,
) -> String {
    match locator {
        hel_targets::TargetLocator::LocalBare { .. } => profile.home.to_string_lossy().into_owned(),
        hel_targets::TargetLocator::LocalPodman { .. }
        | hel_targets::TargetLocator::AppleContainer { .. }
        | hel_targets::TargetLocator::SshPodman { .. } => {
            format!("/var/lib/hel/profiles/{session_id}")
        }
        hel_targets::TargetLocator::AwsEc2 { .. } | hel_targets::TargetLocator::SshBare { .. } => {
            format!(".local/share/hel/profiles/{session_id}")
        }
    }
}

fn upload_checkpoint_spec(
    executor: &impl CommandExecutor,
    locator: &hel_targets::TargetLocator,
    session_id: &str,
    local: &Path,
    remote: &str,
) -> Result<()> {
    match locator {
        hel_targets::TargetLocator::LocalBare { .. } => {
            std::fs::copy(local, remote)
                .with_context(|| format!("copy checkpoint specification to {remote}"))?;
            Ok(())
        }
        hel_targets::TargetLocator::LocalPodman { container_id } => execute_checked(
            executor,
            CommandSpec::new(
                "podman",
                [
                    "cp".into(),
                    local.to_string_lossy().into_owned(),
                    format!("{container_id}:{remote}"),
                ],
            )
            .purpose("upload checkpoint specification"),
        )
        .map(|_| ()),
        hel_targets::TargetLocator::AppleContainer { container_id } => execute_checked(
            executor,
            CommandSpec::new(
                "container",
                [
                    "cp".into(),
                    local.to_string_lossy().into_owned(),
                    format!("{container_id}:{remote}"),
                ],
            )
            .purpose("upload checkpoint specification"),
        )
        .map(|_| ()),
        hel_targets::TargetLocator::AwsEc2 { ssh, .. }
        | hel_targets::TargetLocator::SshBare { ssh, .. } => execute_checked(
            executor,
            scp_command_spec(ssh, local, remote, false).purpose("upload checkpoint specification"),
        )
        .map(|_| ()),
        hel_targets::TargetLocator::SshPodman { ssh, container_id } => {
            let staging = format!(".local/share/hel/uploads/{session_id}-checkpoint.json");
            execute_checked(
                executor,
                ssh_command_spec(ssh, ["mkdir", "-p", ".local/share/hel/uploads"])
                    .purpose("create remote checkpoint staging"),
            )?;
            execute_checked(
                executor,
                scp_command_spec(ssh, local, &staging, false)
                    .purpose("upload remote Podman checkpoint specification"),
            )?;
            execute_checked(
                executor,
                ssh_command_spec(
                    ssh,
                    [
                        "podman",
                        "cp",
                        &staging,
                        &format!("{container_id}:{remote}"),
                    ],
                )
                .purpose("install remote Podman checkpoint specification"),
            )?;
            execute_checked(
                executor,
                ssh_command_spec(ssh, ["rm", "-f", "--", &staging])
                    .purpose("remove remote checkpoint staging"),
            )?;
            Ok(())
        }
    }?;
    Ok(())
}

fn backend_bundle(bundle: &ProjectBundle) -> Result<ProjectBundleSpec> {
    let primary = bundle.primary().context("bundle primary is missing")?;
    Ok(ProjectBundleSpec {
        primary: primary.destination.to_string_lossy().into_owned(),
        repositories: bundle
            .repositories
            .iter()
            .map(|repository| RepositorySpec {
                url: repository.github.as_deref().map(github_url),
                destination: repository.destination.to_string_lossy().into_owned(),
                git_ref: repository.git_ref.clone(),
            })
            .collect(),
    })
}

fn github_url(source: &str) -> String {
    if source.contains("://") || source.starts_with("git@") {
        source.to_string()
    } else {
        format!("https://github.com/{}.git", source.trim_end_matches(".git"))
    }
}

fn scan_target_workers(
    target_id: &str,
    template: &TargetTemplate,
    executor: &impl CommandExecutor,
) -> Result<Vec<RecoveryCandidate>> {
    let mut candidates = match template {
        // Local bare sessions persist their locator in the controller database.
        // Do not infer an adoptable project from Hel's transient worker directory.
        TargetTemplate::LocalBare => Vec::new(),
        TargetTemplate::LocalPodman { .. } => scan_container_engine(
            target_id,
            template,
            "podman",
            vec![
                "ps".into(),
                "--all".into(),
                "--filter".into(),
                format!("label={}=true", hel_targets::MANAGED_LABEL),
                "--format".into(),
                "json".into(),
            ],
            executor,
        )?,
        TargetTemplate::AppleContainer { .. } => scan_container_engine(
            target_id,
            template,
            "container",
            vec![
                "list".into(),
                "--all".into(),
                "--format".into(),
                "json".into(),
            ],
            executor,
        )?,
        TargetTemplate::SshPodman { ssh, .. } => {
            let remote = hel_targets::join_remote_command(&[
                "podman".into(),
                "ps".into(),
                "--all".into(),
                "--filter".into(),
                format!("label={}=true", hel_targets::MANAGED_LABEL),
                "--format".into(),
                "json".into(),
            ]);
            let output = execute_scan(
                executor,
                ssh_spec(ssh, [remote]),
                "scan remote Podman workers",
            )?;
            candidates_from_container_json(target_id, template, &output.stdout)?
        }
        TargetTemplate::AwsEc2 {
            aws_profile,
            region,
            address_source,
            ..
        } => {
            let profile = aws_profile.clone().unwrap_or_else(|| "default".into());
            let output = execute_scan(
                executor,
                CommandSpec::new(
                    "aws",
                    [
                        "--profile".into(),
                        profile,
                        "--region".into(),
                        region.clone(),
                        "ec2".into(),
                        "describe-instances".into(),
                        "--filters".into(),
                        format!("Name=tag:{},Values=true", hel_targets::MANAGED_TAG),
                        "Name=instance-state-name,Values=pending,running,stopping,stopped".into(),
                        "--output".into(),
                        "json".into(),
                    ],
                )
                .purpose("scan managed EC2 workers"),
                "scan managed EC2 workers",
            )?;
            candidates_from_aws_json(target_id, address_source.clone(), &output.stdout)?
        }
        TargetTemplate::SshBare { ssh, .. } => {
            let output = execute_scan(
                executor,
                ssh_spec(
                    ssh,
                    [hel_targets::join_remote_command(&[
                        "find".into(),
                        ".local/share/hel/workers".into(),
                        "-mindepth".into(),
                        "2".into(),
                        "-maxdepth".into(),
                        "2".into(),
                        "-name".into(),
                        "ownership.json".into(),
                        "-print".into(),
                    ])],
                ),
                "scan bare SSH worker markers",
            )?;
            output
                .stdout
                .split(|byte| *byte == b'\n')
                .filter_map(|line| {
                    let path = std::str::from_utf8(line).ok()?.trim();
                    let session_id = Path::new(path).parent()?.file_name()?.to_str()?;
                    hel_targets::resource_name(session_id).ok()?;
                    let backend = backend_target(template, None).ok()?;
                    let workspace = hel_targets::workspace_for(&backend, session_id).ok()?;
                    Some(RecoveryCandidate {
                        session_id: session_id.to_owned(),
                        target_template_id: target_id.to_owned(),
                        locator: TargetLocator::SshBare {
                            host: ssh.host.clone(),
                            workspace: PathBuf::from(workspace),
                            worker_id: None,
                        },
                        ownership: None,
                    })
                })
                .collect()
        }
    };
    for candidate in &mut candidates {
        candidate.ownership = read_recovery_ownership(template, candidate, executor);
    }
    Ok(candidates)
}

fn scan_container_engine(
    target_id: &str,
    template: &TargetTemplate,
    engine: &str,
    args: Vec<String>,
    executor: &impl CommandExecutor,
) -> Result<Vec<RecoveryCandidate>> {
    let output = execute_scan(
        executor,
        CommandSpec::new(engine, args).purpose("scan managed container workers"),
        "scan managed container workers",
    )?;
    candidates_from_container_json(target_id, template, &output.stdout)
}

fn candidates_from_container_json(
    target_id: &str,
    template: &TargetTemplate,
    stdout: &[u8],
) -> Result<Vec<RecoveryCandidate>> {
    let value: serde_json::Value =
        serde_json::from_slice(stdout).context("parse container list JSON")?;
    let mut sessions = Vec::new();
    collect_managed_sessions(&value, &mut sessions);
    sessions.sort();
    sessions.dedup();
    Ok(sessions
        .into_iter()
        .filter_map(|session_id| {
            let generated = hel_targets::resource_name(&session_id).ok()?;
            let locator = match template {
                TargetTemplate::LocalPodman { .. } => TargetLocator::LocalPodman {
                    container_id: generated,
                },
                TargetTemplate::AppleContainer { .. } => TargetLocator::AppleContainer {
                    container_id: generated,
                },
                TargetTemplate::SshPodman { ssh, .. } => TargetLocator::SshPodman {
                    host: ssh.host.clone(),
                    container_id: generated,
                },
                _ => return None,
            };
            Some(RecoveryCandidate {
                session_id,
                target_template_id: target_id.to_owned(),
                locator,
                ownership: None,
            })
        })
        .collect())
}

fn collect_managed_sessions(value: &serde_json::Value, sessions: &mut Vec<String>) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                collect_managed_sessions(value, sessions);
            }
        }
        serde_json::Value::Object(object) => {
            for label_key in ["Labels", "labels"] {
                if let Some(labels) = object.get(label_key) {
                    let managed = label_value(labels, hel_targets::MANAGED_LABEL)
                        .is_some_and(|value| value == "true");
                    if managed
                        && let Some(session) = label_value(labels, hel_targets::SESSION_LABEL)
                    {
                        sessions.push(session);
                    }
                }
            }
            for value in object.values() {
                collect_managed_sessions(value, sessions);
            }
        }
        _ => {}
    }
}

fn label_value(labels: &serde_json::Value, key: &str) -> Option<String> {
    match labels {
        serde_json::Value::Object(object) => object.get(key)?.as_str().map(str::to_owned),
        serde_json::Value::String(text) => text
            .split(',')
            .find_map(|label| {
                label
                    .trim()
                    .split_once('=')
                    .filter(|(name, _)| *name == key)
            })
            .map(|(_, value)| value.to_owned()),
        _ => None,
    }
}

fn candidates_from_aws_json(
    target_id: &str,
    address_source: AwsAddressSource,
    stdout: &[u8],
) -> Result<Vec<RecoveryCandidate>> {
    let value: serde_json::Value =
        serde_json::from_slice(stdout).context("parse AWS instance JSON")?;
    let mut result = Vec::new();
    let reservations = value
        .get("Reservations")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    for instance in reservations.iter().flat_map(|reservation| {
        reservation
            .get("Instances")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
    }) {
        let tags = instance
            .get("Tags")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();
        let tag = |key: &str| {
            tags.iter()
                .find(|tag| tag.get("Key").and_then(serde_json::Value::as_str) == Some(key))
                .and_then(|tag| tag.get("Value"))
                .and_then(serde_json::Value::as_str)
        };
        if tag(hel_targets::MANAGED_TAG) != Some("true") {
            continue;
        }
        let Some(session_id) = tag(hel_targets::SESSION_TAG).map(str::to_owned) else {
            continue;
        };
        hel_targets::resource_name(&session_id)?;
        let instance_id = instance
            .get("InstanceId")
            .and_then(serde_json::Value::as_str)
            .context("managed EC2 instance omitted InstanceId")?
            .to_owned();
        let field = match address_source {
            AwsAddressSource::PublicDns => "PublicDnsName",
            AwsAddressSource::PublicIp => "PublicIpAddress",
            AwsAddressSource::PrivateDns => "PrivateDnsName",
            AwsAddressSource::PrivateIp => "PrivateIpAddress",
        };
        let address = instance
            .get(field)
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_owned);
        result.push(RecoveryCandidate {
            session_id,
            target_template_id: target_id.to_owned(),
            locator: TargetLocator::AwsEc2 {
                instance_id,
                address,
            },
            ownership: None,
        });
    }
    Ok(result)
}

fn execute_scan(
    executor: &impl CommandExecutor,
    command: CommandSpec,
    operation: &str,
) -> Result<CommandOutput> {
    let output = executor.execute(&command)?;
    if output.status != 0 {
        bail!(
            "{operation} failed with status {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(output)
}

fn ssh_spec(ssh: &SshConnection, remote: impl IntoIterator<Item = String>) -> CommandSpec {
    let backend = backend_ssh(ssh);
    let mut args = backend.ssh_args;
    args.push(backend.destination);
    args.extend(remote);
    CommandSpec::new("ssh", args)
}

fn read_recovery_ownership(
    template: &TargetTemplate,
    candidate: &RecoveryCandidate,
    executor: &impl CommandExecutor,
) -> Option<WorkerOwnership> {
    let backend =
        recovery_backend_locator(template, &candidate.locator, &candidate.session_id).ok()?;
    let root = hel_targets::worker_root(&backend, &candidate.session_id).ok()?;
    let command = hel_targets::command_on_locator(
        &backend,
        &candidate.session_id,
        vec!["cat".into(), format!("{root}/ownership.json")],
        "read worker ownership marker",
    )
    .ok()?;
    let output = executor.execute(&command).ok()?;
    if output.status != 0 {
        return None;
    }
    let marker: WorkerOwnership = serde_json::from_slice(&output.stdout).ok()?;
    (marker.version == WorkerOwnership::VERSION
        && marker.session_id == candidate.session_id
        && marker.target_template_id == candidate.target_template_id)
        .then_some(marker)
}

fn recovery_backend_locator(
    template: &TargetTemplate,
    locator: &TargetLocator,
    session_id: &str,
) -> Result<hel_targets::TargetLocator> {
    Ok(match (template, locator) {
        (TargetTemplate::LocalBare, TargetLocator::LocalBare { worker_root }) => {
            hel_targets::TargetLocator::LocalBare {
                worker_root: worker_root.to_string_lossy().into_owned(),
            }
        }
        (TargetTemplate::LocalPodman { .. }, TargetLocator::LocalPodman { container_id }) => {
            hel_targets::TargetLocator::LocalPodman {
                container_id: container_id.clone(),
            }
        }
        (TargetTemplate::AppleContainer { .. }, TargetLocator::AppleContainer { container_id }) => {
            hel_targets::TargetLocator::AppleContainer {
                container_id: container_id.clone(),
            }
        }
        (TargetTemplate::SshPodman { ssh, .. }, TargetLocator::SshPodman { container_id, .. }) => {
            hel_targets::TargetLocator::SshPodman {
                ssh: backend_ssh(ssh),
                container_id: container_id.clone(),
            }
        }
        (TargetTemplate::SshBare { ssh, .. }, TargetLocator::SshBare { workspace, .. }) => {
            hel_targets::TargetLocator::SshBare {
                ssh: backend_ssh(ssh),
                workspace: workspace.to_string_lossy().into_owned(),
            }
        }
        (
            TargetTemplate::AwsEc2 {
                aws_profile,
                region,
                ssh_user,
                identity_file,
                ssh_args,
                ..
            },
            TargetLocator::AwsEc2 {
                instance_id,
                address,
            },
        ) => hel_targets::TargetLocator::AwsEc2 {
            profile: aws_profile.clone().unwrap_or_else(|| "default".into()),
            region: region.clone(),
            instance_id: instance_id.clone(),
            ssh: SshTarget {
                destination: format!(
                    "{ssh_user}@{}",
                    address.as_deref().unwrap_or("unavailable.invalid")
                ),
                ssh_args: ssh_args_with_identity(ssh_args, identity_file.as_deref()),
            },
            workspace: format!(".local/share/hel/workspaces/{session_id}"),
        },
        _ => bail!("recovery target locator does not match target template"),
    })
}

fn backend_target(
    template: &TargetTemplate,
    allocation: Option<&SessionResourceAllocation>,
) -> Result<hel_targets::TargetTemplate> {
    Ok(match template {
        TargetTemplate::LocalBare => hel_targets::TargetTemplate::LocalBare,
        TargetTemplate::LocalPodman { container } => {
            hel_targets::TargetTemplate::LocalPodman(backend_container(container, allocation))
        }
        TargetTemplate::AppleContainer { container } => {
            hel_targets::TargetTemplate::AppleContainer(backend_container(container, allocation))
        }
        TargetTemplate::AwsEc2 {
            aws_profile,
            region,
            launch_template,
            launch_template_version,
            ssh_user,
            identity_file,
            ssh_args,
            ..
        } => hel_targets::TargetTemplate::AwsEc2(AwsTemplate {
            profile: aws_profile.clone().unwrap_or_else(|| "default".into()),
            region: region.clone(),
            launch_template: launch_template.clone(),
            launch_template_version: launch_template_version.clone(),
            instance_type: match allocation {
                Some(SessionResourceAllocation::AwsEc2 { instance_type, .. }) => {
                    Some(instance_type.clone())
                }
                _ => None,
            },
            // The address is filled after describe-instances.
            ssh: SshTarget {
                destination: format!("{ssh_user}@pending.invalid"),
                ssh_args: ssh_args_with_identity(ssh_args, identity_file.as_deref()),
            },
        }),
        TargetTemplate::SshBare {
            ssh,
            workspace_prefix,
        } => hel_targets::TargetTemplate::SshBare {
            ssh: backend_ssh(ssh),
            workspace_prefix: workspace_prefix.to_string_lossy().into_owned(),
        },
        TargetTemplate::SshPodman { ssh, container } => hel_targets::TargetTemplate::SshPodman {
            ssh: backend_ssh(ssh),
            container: backend_container(container, allocation),
        },
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RawProjectInspection {
    source_project_directory: PathBuf,
    source_repository: PathBuf,
    primary_checkout: bool,
    upstream: Option<String>,
}

fn managed_worktree_target(template: &TargetTemplate) -> Result<ManagedWorktreeTarget> {
    match template {
        TargetTemplate::LocalBare => Ok(ManagedWorktreeTarget::Local),
        TargetTemplate::SshBare { ssh, .. } => {
            let ssh = backend_ssh(ssh);
            Ok(ManagedWorktreeTarget::Ssh {
                destination: ssh.destination,
                ssh_args: ssh.ssh_args,
            })
        }
        _ => bail!("managed raw worktrees require a bare target"),
    }
}

fn managed_target_ssh(target: &ManagedWorktreeTarget) -> Option<SshTarget> {
    match target {
        ManagedWorktreeTarget::Local => None,
        ManagedWorktreeTarget::Ssh {
            destination,
            ssh_args,
        } => Some(SshTarget {
            destination: destination.clone(),
            ssh_args: ssh_args.clone(),
        }),
    }
}

fn managed_target_command(
    target: &ManagedWorktreeTarget,
    program: &str,
    args: impl IntoIterator<Item = impl AsRef<str>>,
) -> CommandSpec {
    let args = args
        .into_iter()
        .map(|arg| arg.as_ref().to_owned())
        .collect::<Vec<_>>();
    match managed_target_ssh(target) {
        None => CommandSpec::new(program, args),
        Some(ssh) => {
            let mut remote = vec![program.to_owned()];
            remote.extend(args);
            ssh_command_spec(&ssh, remote)
        }
    }
}

fn managed_git_command(
    target: &ManagedWorktreeTarget,
    directory: &Path,
    args: impl IntoIterator<Item = impl AsRef<str>>,
    purpose: impl Into<String>,
) -> CommandSpec {
    let mut command_args = vec!["-C".to_owned(), directory.to_string_lossy().into_owned()];
    command_args.extend(args.into_iter().map(|arg| arg.as_ref().to_owned()));
    managed_target_command(target, "git", command_args).purpose(purpose)
}

fn command_stdout(output: CommandOutput, purpose: &str) -> Result<String> {
    if output.status != 0 {
        bail!(
            "{purpose} failed with status {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let stdout = String::from_utf8(output.stdout)
        .with_context(|| format!("{purpose} produced non-UTF-8 output"))?;
    Ok(stdout.trim_end_matches(['\r', '\n']).to_owned())
}

fn managed_git_stdout(
    executor: &impl CommandExecutor,
    target: &ManagedWorktreeTarget,
    directory: &Path,
    args: impl IntoIterator<Item = impl AsRef<str>>,
    purpose: &str,
) -> Result<String> {
    let command = managed_git_command(target, directory, args, purpose);
    command_stdout(executor.execute(&command)?, purpose)
}

fn inspect_raw_project(
    executor: &impl CommandExecutor,
    target: &ManagedWorktreeTarget,
    selected: &Path,
) -> Result<RawProjectInspection> {
    let repository = PathBuf::from(managed_git_stdout(
        executor,
        target,
        selected,
        ["rev-parse", "--path-format=absolute", "--show-toplevel"],
        "resolve raw project repository root",
    )?);
    let prefix = managed_git_stdout(
        executor,
        target,
        selected,
        ["rev-parse", "--show-prefix"],
        "resolve raw project relative directory",
    )?;
    let git_dir = PathBuf::from(managed_git_stdout(
        executor,
        target,
        selected,
        ["rev-parse", "--absolute-git-dir"],
        "resolve raw project Git directory",
    )?);
    let common_git_dir = PathBuf::from(managed_git_stdout(
        executor,
        target,
        selected,
        ["rev-parse", "--path-format=absolute", "--git-common-dir"],
        "resolve raw project common Git directory",
    )?);
    let branch_command = managed_git_command(
        target,
        selected,
        ["symbolic-ref", "--quiet", "--short", "HEAD"],
        "resolve raw project branch",
    );
    let branch_output = executor.execute(&branch_command)?;
    let branch = match branch_output.status {
        0 => Some(
            String::from_utf8(branch_output.stdout)
                .context("raw project branch was not UTF-8")?
                .trim()
                .to_owned(),
        ),
        1 | 128 => None,
        status => bail!(
            "resolve raw project branch failed with status {status}: {}",
            String::from_utf8_lossy(&branch_output.stderr).trim()
        ),
    };
    let upstream = match branch {
        Some(branch) => {
            let reference = format!("refs/heads/{branch}");
            let upstream = managed_git_stdout(
                executor,
                target,
                selected,
                ["for-each-ref", "--format=%(upstream:short)", &reference],
                "resolve raw project upstream",
            )?;
            (!upstream.is_empty()).then_some(upstream)
        }
        None => None,
    };
    Ok(RawProjectInspection {
        source_project_directory: repository.join(prefix),
        source_repository: repository,
        primary_checkout: git_dir == common_git_dir,
        upstream,
    })
}

fn ensure_managed_worktree_excluded(
    executor: &impl CommandExecutor,
    target: &ManagedWorktreeTarget,
    repository: &Path,
) -> Result<()> {
    let check = managed_git_command(
        target,
        repository,
        [
            "check-ignore",
            "--quiet",
            "--no-index",
            "--",
            ".hel/worktrees/",
        ],
        "check managed worktree exclusion",
    );
    let output = executor.execute(&check)?;
    match output.status {
        0 => return Ok(()),
        1 => {}
        status => bail!(
            "check managed worktree exclusion failed with status {status}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ),
    }
    let exclude_path = PathBuf::from(managed_git_stdout(
        executor,
        target,
        repository,
        [
            "rev-parse",
            "--path-format=absolute",
            "--git-path",
            "info/exclude",
        ],
        "resolve repository-local exclude file",
    )?);
    const ENTRY: &str = "/.hel/worktrees/";
    match target {
        ManagedWorktreeTarget::Local => {
            use std::io::Write;
            let existing = match std::fs::read_to_string(&exclude_path) {
                Ok(existing) => existing,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
                Err(error) => return Err(error.into()),
            };
            if existing.lines().any(|line| line.trim() == ENTRY) {
                return Ok(());
            }
            if let Some(parent) = exclude_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&exclude_path)
                .with_context(|| format!("open {}", exclude_path.display()))?;
            if !existing.is_empty() && !existing.ends_with('\n') {
                writeln!(file)?;
            }
            writeln!(file, "# Hel managed worktrees\n{ENTRY}")?;
        }
        ManagedWorktreeTarget::Ssh { .. } => {
            const SCRIPT: &str = "set -eu\nexclude=$1\nentry=$2\nmkdir -p \"$(dirname \"$exclude\")\"\ntouch \"$exclude\"\nif ! grep -Fqx \"$entry\" \"$exclude\"; then\n  if [ -s \"$exclude\" ] && [ \"$(tail -c 1 \"$exclude\" | wc -l)\" -eq 0 ]; then printf '\\n' >>\"$exclude\"; fi\n  printf '# Hel managed worktrees\\n%s\\n' \"$entry\" >>\"$exclude\"\nfi";
            let command = managed_target_command(
                target,
                "sh",
                [
                    "-c",
                    SCRIPT,
                    "hel-exclude",
                    &exclude_path.to_string_lossy(),
                    ENTRY,
                ],
            )
            .purpose("update remote repository-local exclude file");
            execute_checked(executor, command)?;
        }
    }
    Ok(())
}

fn path_exists_on_managed_target(
    executor: &impl CommandExecutor,
    target: &ManagedWorktreeTarget,
    path: &Path,
) -> Result<bool> {
    match target {
        ManagedWorktreeTarget::Local => Ok(path.exists()),
        ManagedWorktreeTarget::Ssh { .. } => {
            let command = managed_target_command(target, "test", ["-e", &path.to_string_lossy()])
                .purpose("check managed worktree path");
            let output = executor.execute(&command)?;
            match output.status {
                0 => Ok(true),
                1 => Ok(false),
                status => bail!(
                    "check managed worktree path failed with status {status}: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                ),
            }
        }
    }
}

fn create_managed_worktree(
    executor: &impl CommandExecutor,
    worktree: &ManagedWorktree,
    upstream: Option<&str>,
) -> Result<()> {
    ensure_managed_worktree_excluded(executor, &worktree.target, &worktree.source_repository)?;
    let status = managed_git_stdout(
        executor,
        &worktree.target,
        &worktree.source_repository,
        ["status", "--porcelain=v1", "--untracked-files=all"],
        "inspect primary checkout changes",
    )?;
    if !status.is_empty() {
        let paths = status.lines().take(20).collect::<Vec<_>>().join("\n  ");
        bail!(
            "primary checkout has uncommitted changes; commit or stash them before creating a raw session worktree:\n  {paths}"
        );
    }
    let parent = worktree
        .worktree_root
        .parent()
        .context("managed worktree root has no parent")?;
    execute_checked(
        executor,
        managed_target_command(&worktree.target, "mkdir", ["-p", &parent.to_string_lossy()])
            .purpose("create managed worktree directory"),
    )?;
    execute_checked(
        executor,
        managed_git_command(
            &worktree.target,
            &worktree.source_repository,
            [
                "worktree",
                "add",
                "-b",
                &worktree.branch,
                &worktree.worktree_root.to_string_lossy(),
                "HEAD",
            ],
            "create managed raw-session worktree",
        ),
    )?;
    if let Some(upstream) = upstream {
        execute_checked(
            executor,
            managed_git_command(
                &worktree.target,
                &worktree.worktree_root,
                ["branch", "--set-upstream-to", upstream, &worktree.branch],
                "set managed worktree branch upstream",
            ),
        )?;
    }
    Ok(())
}

fn ensure_managed_worktree_available(
    executor: &impl CommandExecutor,
    worktree: &ManagedWorktree,
) -> Result<()> {
    if path_exists_on_managed_target(executor, &worktree.target, &worktree.worktree_root)? {
        bail!(
            "managed worktree path already exists: {}",
            worktree.worktree_root.display()
        );
    }
    let branch_ref = format!("refs/heads/{}", worktree.branch);
    let check = managed_git_command(
        &worktree.target,
        &worktree.source_repository,
        ["show-ref", "--verify", "--quiet", &branch_ref],
        "check managed worktree branch availability",
    );
    let output = executor.execute(&check)?;
    match output.status {
        0 => bail!(
            "managed worktree branch already exists: {}",
            worktree.branch
        ),
        1 => Ok(()),
        status => bail!(
            "check managed worktree branch availability failed with status {status}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ),
    }
}

fn cleanup_managed_worktree(
    executor: &impl CommandExecutor,
    worktree: &ManagedWorktree,
) -> Result<()> {
    if !path_exists_on_managed_target(executor, &worktree.target, &worktree.source_repository)? {
        return Ok(());
    }
    if path_exists_on_managed_target(executor, &worktree.target, &worktree.worktree_root)? {
        execute_checked(
            executor,
            managed_git_command(
                &worktree.target,
                &worktree.source_repository,
                [
                    "worktree",
                    "remove",
                    "--force",
                    &worktree.worktree_root.to_string_lossy(),
                ],
                "remove managed raw-session worktree",
            ),
        )?;
    }
    execute_checked(
        executor,
        managed_git_command(
            &worktree.target,
            &worktree.source_repository,
            ["worktree", "prune"],
            "prune managed worktree metadata",
        ),
    )?;
    let branch_ref = format!("refs/heads/{}", worktree.branch);
    let check = managed_git_command(
        &worktree.target,
        &worktree.source_repository,
        ["show-ref", "--verify", "--quiet", &branch_ref],
        "check managed worktree branch",
    );
    let output = executor.execute(&check)?;
    match output.status {
        0 => {
            execute_checked(
                executor,
                managed_git_command(
                    &worktree.target,
                    &worktree.source_repository,
                    ["branch", "-D", "--", &worktree.branch],
                    "delete managed raw-session branch",
                ),
            )?;
        }
        1 => {}
        status => bail!(
            "check managed worktree branch failed with status {status}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ),
    }
    let worktrees = worktree.source_repository.join(".hel").join("worktrees");
    let hel = worktree.source_repository.join(".hel");
    match &worktree.target {
        ManagedWorktreeTarget::Local => {
            for directory in [&worktrees, &hel] {
                match std::fs::remove_dir(directory) {
                    Ok(()) => {}
                    Err(error)
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::NotFound | std::io::ErrorKind::DirectoryNotEmpty
                        ) => {}
                    Err(error) => return Err(error.into()),
                }
            }
        }
        ManagedWorktreeTarget::Ssh { .. } => {
            let command = managed_target_command(
                &worktree.target,
                "rmdir",
                ["--", &worktrees.to_string_lossy(), &hel.to_string_lossy()],
            )
            .purpose("remove empty managed worktree directories");
            let _ = executor.execute(&command)?;
        }
    }
    Ok(())
}

fn controller_github_token() -> Option<String> {
    for name in ["GH_TOKEN", "GITHUB_TOKEN"] {
        if let Ok(token) = std::env::var(name)
            && let Some(token) = usable_github_token(&token)
        {
            return Some(token.to_owned());
        }
    }
    let output = Command::new("gh")
        .args(["auth", "token", "--hostname", "github.com"])
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    output
        .status
        .success()
        .then_some(())
        .and_then(|()| std::str::from_utf8(&output.stdout).ok())
        .and_then(usable_github_token)
        .map(str::to_owned)
}

fn usable_github_token(token: &str) -> Option<&str> {
    let token = token.trim();
    (!token.is_empty() && !token.chars().any(char::is_whitespace)).then_some(token)
}

fn inject_github_token(target: &mut hel_targets::TargetTemplate, token: &str) -> bool {
    let container = match target {
        hel_targets::TargetTemplate::LocalPodman(container)
        | hel_targets::TargetTemplate::AppleContainer(container)
        | hel_targets::TargetTemplate::SshPodman { container, .. } => container,
        hel_targets::TargetTemplate::LocalBare
        | hel_targets::TargetTemplate::AwsEc2(_)
        | hel_targets::TargetTemplate::SshBare { .. } => return false,
    };
    container
        .extra_run_args
        .extend(["--env".to_owned(), format!("GH_TOKEN={token}")]);
    true
}

fn use_github_https_urls(bundle: &mut hel_targets::ProjectBundleSpec) {
    for repository in &mut bundle.repositories {
        let Some(source) = repository.url.as_deref() else {
            continue;
        };
        let Some(github) = crate::hel_setup::github_repository_from_origin(source) else {
            continue;
        };
        repository.url = Some(format!(
            "https://github.com/{}/{}.git",
            github.owner, github.repository
        ));
    }
}

fn mount_history_host(template: &TargetTemplate) -> Option<String> {
    match template {
        TargetTemplate::LocalPodman { .. }
        | TargetTemplate::AppleContainer { .. }
        | TargetTemplate::AwsEc2 { .. } => Some("local".into()),
        TargetTemplate::SshPodman { ssh, .. } => Some(ssh.host.clone()),
        TargetTemplate::LocalBare | TargetTemplate::SshBare { .. } => None,
    }
}

fn backend_container(
    container: &crate::hel_config::ContainerTemplate,
    allocation: Option<&SessionResourceAllocation>,
) -> ContainerTemplate {
    let mut extra_run_args = Vec::new();
    if let Some(platform) = &container.platform {
        extra_run_args.push(format!("--platform={platform}"));
    }
    if let Some(SessionResourceAllocation::Container { cpus, memory_bytes }) = allocation {
        extra_run_args.push(format!("--cpus={cpus}"));
        extra_run_args.push(format!("--memory={memory_bytes}"));
    } else {
        if let Some(cpus) = &container.cpus {
            extra_run_args.push(format!("--cpus={cpus}"));
        }
        if let Some(memory) = &container.memory {
            extra_run_args.push(format!("--memory={memory}"));
        }
    }
    for (key, value) in &container.environment {
        extra_run_args.extend(["--env".to_string(), format!("{key}={value}")]);
    }
    ContainerTemplate {
        image: container.image.clone(),
        extra_run_args,
    }
}

fn validate_resource_allocation(
    template: &TargetTemplate,
    allocation: Option<&SessionResourceAllocation>,
) -> Result<()> {
    match (template, allocation) {
        (_, None)
        | (
            TargetTemplate::LocalPodman { .. }
            | TargetTemplate::AppleContainer { .. }
            | TargetTemplate::SshPodman { .. },
            Some(SessionResourceAllocation::Container { .. }),
        )
        | (TargetTemplate::AwsEc2 { .. }, Some(SessionResourceAllocation::AwsEc2 { .. })) => Ok(()),
        (TargetTemplate::LocalBare | TargetTemplate::SshBare { .. }, Some(_)) => {
            bail!("bare targets have fixed host resources")
        }
        _ => bail!("resource allocation does not match the selected target kind"),
    }
}

fn is_bare_project_target(template: &TargetTemplate) -> bool {
    matches!(
        template,
        TargetTemplate::LocalBare | TargetTemplate::SshBare { .. }
    )
}

pub(crate) fn backend_ssh(ssh: &SshConnection) -> SshTarget {
    let destination = match &ssh.user {
        Some(user) => format!("{user}@{}", ssh.host),
        None => ssh.host.clone(),
    };
    SshTarget {
        destination,
        ssh_args: ssh_args_with_identity(&ssh.extra_args, ssh.identity_file.as_deref()),
    }
}

fn ssh_args_with_identity(args: &[String], identity: Option<&Path>) -> Vec<String> {
    // Hel drives ssh non-interactively from a TUI; a host-key or password
    // prompt would steal the terminal and wedge provisioning. BatchMode fails
    // fast instead of prompting, and accept-new trusts a first-seen host key
    // (fresh EC2 instances are always first-seen) while still rejecting
    // changed keys. User-supplied ssh_args come last so they can override.
    let mut result = vec![
        "-o".into(),
        "BatchMode=yes".into(),
        "-o".into(),
        "StrictHostKeyChecking=accept-new".into(),
        "-o".into(),
        "ConnectTimeout=15".into(),
    ];
    result.extend(args.iter().cloned());
    if let Some(identity) = identity {
        result.push("-i".into());
        result.push(identity.to_string_lossy().into_owned());
    }
    result
}

fn install_attached_resources(
    state: &HelState,
    session_id: &str,
    backend: &hel_targets::TargetLocator,
    worker_root: &str,
    executor: &impl CommandExecutor,
) -> Result<()> {
    let hel_targets::TargetLocator::AwsEc2 { .. } = backend else {
        return Ok(());
    };
    let session = state
        .sessions
        .get(session_id)
        .with_context(|| format!("unknown session {session_id}"))?;
    if session.additional_mounts.is_empty() {
        return Ok(());
    }
    for resource in &session.additional_mounts {
        let install = hel_targets::command_on_locator(
            backend,
            session_id,
            vec![
                format!("{worker_root}/hel"),
                "worker".into(),
                "install-resource".into(),
                "--destination".into(),
                resource.destination.to_string_lossy().into_owned(),
            ],
            "stream attached resource",
        )?;
        crate::hel_resources::stream_resource(&resource.source, |stream| {
            execute_checked_with_stdin(executor, &install, stream).map(|_| ())
        })
        .with_context(|| format!("stream attached resource {}", resource.source.display()))?;
    }
    Ok(())
}

/// Best-effort teardown of a freshly created resource whose provisioning
/// failed before a locator was recorded. Returns a note describing what
/// happened for inclusion in the session error.
fn cleanup_failed_provision(
    canonical: &TargetTemplate,
    session_id: &str,
    first_output: Option<&CommandOutput>,
    executor: &impl CommandExecutor,
) -> Option<String> {
    let command = match canonical {
        TargetTemplate::LocalPodman { .. } => {
            let name = hel_targets::resource_name(session_id).ok()?;
            CommandSpec::new("podman", ["rm", "--force", &name])
                .purpose("remove container after failed provisioning")
        }
        TargetTemplate::AppleContainer { .. } => {
            let name = hel_targets::resource_name(session_id).ok()?;
            CommandSpec::new("container", ["rm", "--force", &name])
                .purpose("remove container after failed provisioning")
        }
        TargetTemplate::AwsEc2 {
            aws_profile,
            region,
            ..
        } => {
            let instance_id = serde_json::from_slice::<serde_json::Value>(&first_output?.stdout)
                .ok()?
                .pointer("/Instances/0/InstanceId")?
                .as_str()?
                .to_string();
            let profile = aws_profile.clone().unwrap_or_else(|| "default".into());
            CommandSpec::new(
                "aws",
                [
                    "--profile",
                    &profile,
                    "--region",
                    region,
                    "ec2",
                    "terminate-instances",
                    "--instance-ids",
                    &instance_id,
                ],
            )
            .purpose("terminate EC2 instance after failed provisioning")
        }
        // SSH machines are persistent; nothing was created that must die.
        TargetTemplate::LocalBare
        | TargetTemplate::SshBare { .. }
        | TargetTemplate::SshPodman { .. } => return None,
    };
    let purpose = command.purpose.clone();
    match executor.execute(&command) {
        Ok(output) if output.status == 0 => Some(format!("cleanup succeeded: {purpose}")),
        Ok(output) => Some(format!(
            "cleanup FAILED ({purpose}, status {}): the resource may still exist; find it via its dev.hel.session={session_id} label/tag",
            output.status
        )),
        Err(error) => Some(format!(
            "cleanup FAILED ({purpose}): {error:#}; the resource may still exist; find it via its dev.hel.session={session_id} label/tag"
        )),
    }
}

fn locator_after_provision(
    canonical: &TargetTemplate,
    backend: &hel_targets::TargetTemplate,
    session_id: &str,
    first_output: Option<&CommandOutput>,
    executor: &impl CommandExecutor,
    bundle: Option<&ProjectBundleSpec>,
) -> Result<TargetLocator> {
    let generated = hel_targets::resource_name(session_id)?;
    Ok(match canonical {
        TargetTemplate::LocalBare => TargetLocator::LocalBare {
            worker_root: data_dir().join("workers").join(session_id),
        },
        TargetTemplate::LocalPodman { .. } => TargetLocator::LocalPodman {
            container_id: generated,
        },
        TargetTemplate::AppleContainer { .. } => TargetLocator::AppleContainer {
            container_id: generated,
        },
        TargetTemplate::SshBare { ssh, .. } => TargetLocator::SshBare {
            host: ssh.host.clone(),
            workspace: PathBuf::from(hel_targets::workspace_for(backend, session_id)?),
            worker_id: None,
        },
        TargetTemplate::SshPodman { ssh, .. } => TargetLocator::SshPodman {
            host: ssh.host.clone(),
            container_id: generated,
        },
        TargetTemplate::AwsEc2 {
            aws_profile,
            region,
            ssh_user,
            address_source,
            identity_file,
            ssh_args,
            ..
        } => {
            let output = first_output.context("AWS launch produced no output")?;
            let json: serde_json::Value = serde_json::from_slice(&output.stdout)
                .context("parse aws ec2 run-instances response")?;
            let instance_id = json
                .pointer("/Instances/0/InstanceId")
                .and_then(serde_json::Value::as_str)
                .context("AWS response omitted instance ID")?
                .to_string();
            let profile = aws_profile.clone().unwrap_or_else(|| "default".into());
            execute_checked(
                executor,
                CommandSpec::new(
                    "aws",
                    [
                        "--profile".into(),
                        profile.clone(),
                        "--region".into(),
                        region.clone(),
                        "ec2".into(),
                        "wait".into(),
                        "instance-status-ok".into(),
                        "--instance-ids".into(),
                        instance_id.clone(),
                    ],
                )
                .purpose("wait for EC2 session instance"),
            )?;
            let field = match address_source {
                AwsAddressSource::PublicDns => "PublicDnsName",
                AwsAddressSource::PublicIp => "PublicIpAddress",
                AwsAddressSource::PrivateDns => "PrivateDnsName",
                AwsAddressSource::PrivateIp => "PrivateIpAddress",
            };
            let address = execute_checked(
                executor,
                CommandSpec::new(
                    "aws",
                    [
                        "--profile".into(),
                        profile.clone(),
                        "--region".into(),
                        region.clone(),
                        "ec2".into(),
                        "describe-instances".into(),
                        "--instance-ids".into(),
                        instance_id.clone(),
                        "--query".into(),
                        format!("Reservations[0].Instances[0].{field}"),
                        "--output".into(),
                        "text".into(),
                    ],
                )
                .purpose("resolve EC2 session address"),
            )?;
            let address = String::from_utf8(address.stdout)
                .context("AWS address was not UTF-8")?
                .trim()
                .to_string();
            if address.is_empty() || address == "None" {
                bail!("AWS instance {instance_id} has no configured address");
            }
            let ssh = SshTarget {
                destination: format!("{ssh_user}@{address}"),
                ssh_args: ssh_args_with_identity(ssh_args, identity_file.as_deref()),
            };
            let backend_locator = hel_targets::TargetLocator::AwsEc2 {
                profile: profile.clone(),
                region: region.clone(),
                instance_id: instance_id.clone(),
                ssh,
                workspace: format!(".local/share/hel/workspaces/{session_id}"),
            };
            hel_targets::provision_on_locator_plan(
                &backend_locator,
                session_id,
                bundle.context("AWS provisioning requires a project bundle")?,
            )?
            .execute(executor)?;
            TargetLocator::AwsEc2 {
                instance_id,
                address: Some(address),
            }
        }
    })
}

fn backend_locator(
    locator: &TargetLocator,
    session: &SessionRecord,
    config: &HelConfig,
) -> Result<hel_targets::TargetLocator> {
    let template = config
        .targets
        .get(&session.target_template_id)
        .context("session target template is missing")?;
    Ok(match locator {
        TargetLocator::LocalBare { worker_root } => {
            let TargetTemplate::LocalBare = template else {
                bail!("session locator/template mismatch")
            };
            hel_targets::TargetLocator::LocalBare {
                worker_root: worker_root.to_string_lossy().into_owned(),
            }
        }
        TargetLocator::LocalPodman { container_id } => hel_targets::TargetLocator::LocalPodman {
            container_id: container_id.clone(),
        },
        TargetLocator::AppleContainer { container_id } => {
            hel_targets::TargetLocator::AppleContainer {
                container_id: container_id.clone(),
            }
        }
        TargetLocator::SshBare { workspace, .. } => {
            let TargetTemplate::SshBare { ssh, .. } = template else {
                bail!("session locator/template mismatch")
            };
            hel_targets::TargetLocator::SshBare {
                ssh: backend_ssh(ssh),
                workspace: workspace.to_string_lossy().into_owned(),
            }
        }
        TargetLocator::SshPodman { container_id, .. } => {
            let TargetTemplate::SshPodman { ssh, .. } = template else {
                bail!("session locator/template mismatch")
            };
            hel_targets::TargetLocator::SshPodman {
                ssh: backend_ssh(ssh),
                container_id: container_id.clone(),
            }
        }
        TargetLocator::AwsEc2 {
            instance_id,
            address,
        } => {
            let TargetTemplate::AwsEc2 {
                aws_profile,
                region,
                ssh_user,
                identity_file,
                ssh_args,
                ..
            } = template
            else {
                bail!("session locator/template mismatch")
            };
            let address = address.as_deref().context("AWS locator has no address")?;
            hel_targets::TargetLocator::AwsEc2 {
                profile: aws_profile.clone().unwrap_or_else(|| "default".into()),
                region: region.clone(),
                instance_id: instance_id.clone(),
                ssh: SshTarget {
                    destination: format!("{ssh_user}@{address}"),
                    ssh_args: ssh_args_with_identity(ssh_args, identity_file.as_deref()),
                },
                workspace: format!(".local/share/hel/workspaces/{}", session.id),
            }
        }
    })
}

fn absolute_target_path(
    executor: &impl CommandExecutor,
    locator: &hel_targets::TargetLocator,
    session_id: &str,
    path: &str,
) -> Result<String> {
    if path.starts_with('/') {
        return Ok(path.to_owned());
    }
    let output = execute_checked(
        executor,
        hel_targets::command_on_locator(
            locator,
            session_id,
            vec!["pwd".into()],
            "resolve target home directory",
        )?,
    )?;
    let directory = String::from_utf8(output.stdout).context("decode target working directory")?;
    let directory = directory.trim_end_matches(['\r', '\n', '/']);
    if directory.is_empty() || !directory.starts_with('/') {
        bail!("target returned an invalid working directory {directory:?}");
    }
    Ok(format!("{directory}/{path}"))
}

fn local_branch(repository: &Path) -> Result<String> {
    let output = Command::new("git")
        .args(["symbolic-ref", "--quiet", "--short", "HEAD"])
        .current_dir(repository)
        .output()
        .with_context(|| format!("read current branch in {}", repository.display()))?;
    if !output.status.success() {
        bail!(
            "local repository {} must have a branch checked out before Hel can expose it as origin",
            repository.display()
        );
    }
    let branch = String::from_utf8(output.stdout).context("decode local Git branch")?;
    let branch = branch.trim().to_owned();
    if branch.is_empty() {
        bail!("local repository has an empty current branch");
    }
    Ok(branch)
}

fn local_origin_url(worker_root: &str, repository_id: &str) -> String {
    fn ext_argument(value: &str) -> String {
        value.replace('%', "%%").replace(' ', "% ")
    }
    format!(
        "ext::{}/hel worker git-proxy --root {} --repository {} %S",
        ext_argument(worker_root),
        ext_argument(worker_root),
        repository_id,
    )
}

/// Carry a local repository's identity and uncommitted changes into a freshly
/// initialized target checkout. Committed history is never bundled here: the
/// caller fetches it through the `hel-local` proxy first.
fn bootstrap_local_repositories(
    executor: &impl CommandExecutor,
    locator: &hel_targets::TargetLocator,
    session: &SessionRecord,
    bundle: &ProjectBundle,
    workspace_root: &str,
    worker_root: &str,
    repositories: &[(&crate::hel_config::ProjectRepository, &PathBuf)],
) -> Result<()> {
    let snapshots = repositories
        .iter()
        .map(|(repository, source)| {
            collect_git_snapshot(
                &SystemGit,
                source,
                &GitCollectionSpec {
                    id: repository.id.clone(),
                    relative_destination: repository.destination.clone(),
                    history: GitHistoryMode::NoBundle,
                    origin_override: Some(format!("hel-local:{}", repository.id)),
                },
            )
            .with_context(|| format!("snapshot local repository {:?}", repository.id))
        })
        .collect::<Result<Vec<_>>>()?;
    let staging = data_dir().join("git-seeds");
    std::fs::create_dir_all(&staging)?;
    let archive_path = staging.join(format!("{}.hel.zip", session.id));
    write_archive_atomic(
        &archive_path,
        &ArchiveInput {
            session: SessionManifest {
                id: session.id.clone(),
                title: session.title.clone(),
                harness_kind: session.harness_kind,
                profile_id: session.last_profile.clone(),
                native_session_id: session.native_session_id.clone().unwrap_or_default(),
                created_at: session.created_at.clone(),
                checkpointed_at: now(),
                hel_version: env!("CARGO_PKG_VERSION").into(),
                relay_version: env!("CARGO_PKG_VERSION").into(),
                adapter_version: "acp-v1".into(),
            },
            target: TargetManifest {
                template_id: session.target_template_id.clone(),
                target_kind: target_kind(locator).into(),
                details: Default::default(),
            },
            bundle: BundleManifest {
                id: session.bundle_id.clone(),
                primary_repository: bundle.primary_repo.clone(),
            },
            canonical_session: canonical_session_from_materialized(
                &crate::hel_state::MaterializedSession::empty(session.id.clone()),
            )?,
            native_artifacts: Vec::new(),
            repositories: snapshots,
        },
    )?;

    let remote_archive = format!("{worker_root}/local-seed.hel.zip");
    let remote_spec = format!("{worker_root}/local-seed.json");
    let target_path = |path: &str| match locator {
        hel_targets::TargetLocator::AwsEc2 { .. } | hel_targets::TargetLocator::SshBare { .. } => {
            PathBuf::from(format!("~/{path}"))
        }
        _ => PathBuf::from(path),
    };
    let spec = RepositoryRestoreSpec {
        archive_path: target_path(&remote_archive),
        workspace_root: target_path(workspace_root),
    };
    let local_spec = staging.join(format!("{}.json", session.id));
    crate::hel_config::atomic_write(&local_spec, &serde_json::to_vec_pretty(&spec)?)?;
    upload_checkpoint_spec(
        executor,
        locator,
        &session.id,
        &archive_path,
        &remote_archive,
    )?;
    upload_checkpoint_spec(executor, locator, &session.id, &local_spec, &remote_spec)?;
    execute_checked(
        executor,
        hel_targets::command_on_locator(
            locator,
            &session.id,
            vec![
                format!("{worker_root}/hel"),
                "worker".into(),
                "restore-repositories".into(),
                "--spec".into(),
                remote_spec,
            ],
            "restore local repository bootstrap",
        )?,
    )?;
    Ok(())
}

fn ensure_git_broker(
    session_id: &str,
    locator: &hel_targets::TargetLocator,
    repositories: BTreeMap<String, PathBuf>,
) -> Result<()> {
    let directory = data_dir().join("git-brokers");
    std::fs::create_dir_all(&directory)?;
    let spec_path = directory.join(format!("{session_id}.json"));
    let ready_path = directory.join(format!("{session_id}.ready"));
    let pid_path = directory.join(format!("{session_id}.pid"));
    let log_path = directory.join(format!("{session_id}.log"));
    let spec = GitBrokerSpec {
        session_id: session_id.to_owned(),
        bridge: hel_targets::git_bridge_command(locator, session_id)?,
        repositories,
        ready_path: ready_path.clone(),
        pid_path: pid_path.clone(),
    };
    if broker_is_alive(&pid_path) {
        if GitBrokerSpec::read(&spec_path).is_ok_and(|existing| existing == spec)
            && ready_path.exists()
        {
            return Ok(());
        }
        bail!(
            "a different local Git broker is still active for session {session_id}; close its target before reconnecting"
        );
    }
    let _ = std::fs::remove_file(&ready_path);
    spec.write(&spec_path)?;

    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .with_context(|| format!("open Git broker log {}", log_path.display()))?;
    let stderr = log.try_clone()?;
    let executable = std::env::current_exe().context("locate Hel controller executable")?;
    let mut command = Command::new(executable);
    command
        .args(["broker", "--spec"])
        .arg(&spec_path)
        .stdin(Stdio::null())
        .stdout(log)
        .stderr(stderr);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let mut child = command.spawn().context("start local Git broker")?;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if ready_path.exists() && broker_is_alive(&pid_path) {
            std::thread::spawn(move || {
                let _ = child.wait();
            });
            return Ok(());
        }
        if let Some(status) = child.try_wait().context("poll local Git broker")? {
            bail!(
                "local Git broker exited with {status}; see {}",
                log_path.display()
            );
        }
        if Instant::now() >= deadline {
            bail!(
                "timed out starting local Git broker; see {}",
                log_path.display()
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn execute_checked(executor: &impl CommandExecutor, command: CommandSpec) -> Result<CommandOutput> {
    let output = executor.execute(&command)?;
    if output.status != 0 {
        bail!(
            "{} failed with status {}: {}",
            command.purpose,
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(output)
}

fn execute_checked_with_stdin(
    executor: &impl CommandExecutor,
    command: &CommandSpec,
    input: &mut (dyn std::io::Read + Send),
) -> Result<CommandOutput> {
    let output = executor.execute_with_stdin(command, input)?;
    if output.status != 0 {
        bail!(
            "{} failed with status {}: {}",
            command.purpose,
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(output)
}

fn install_inherited_git_settings(
    executor: &impl CommandExecutor,
    locator: &hel_targets::TargetLocator,
    session_id: &str,
) -> Result<()> {
    let settings = if inherits_controller_git_settings(locator) {
        controller_git_settings()?
    } else {
        BTreeMap::new()
    };
    for command in inherited_git_setting_commands(locator, session_id, settings)? {
        execute_checked(executor, command)?;
    }
    Ok(())
}

fn inherits_controller_git_settings(locator: &hel_targets::TargetLocator) -> bool {
    !matches!(
        locator,
        hel_targets::TargetLocator::LocalBare { .. } | hel_targets::TargetLocator::SshBare { .. }
    )
}

fn force_unrestricted_mode(locator: &hel_targets::TargetLocator) -> bool {
    !matches!(locator, hel_targets::TargetLocator::LocalBare { .. })
}

fn inherited_git_setting_commands(
    locator: &hel_targets::TargetLocator,
    session_id: &str,
    settings: BTreeMap<String, String>,
) -> Result<Vec<CommandSpec>> {
    if matches!(locator, hel_targets::TargetLocator::SshBare { .. }) {
        return Ok(Vec::new());
    }
    settings
        .into_iter()
        .map(|(key, value)| {
            hel_targets::command_on_locator(
                locator,
                session_id,
                vec![
                    "git".into(),
                    "config".into(),
                    "--global".into(),
                    "--replace-all".into(),
                    "--".into(),
                    key.clone(),
                    value,
                ],
                format!("inherit Git setting {key}"),
            )
        })
        .collect()
}

fn controller_git_settings() -> Result<BTreeMap<String, String>> {
    let output = match Command::new("git")
        .args(["config", "--global", "--includes", "--null", "--list"])
        .stdin(Stdio::null())
        .output()
    {
        Ok(output) => output,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(error) => return Err(error).context("read controller Git configuration"),
    };
    if !output.status.success() {
        bail!(
            "read controller Git configuration failed with status {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    parse_inherited_git_settings(&output.stdout)
}

fn parse_inherited_git_settings(output: &[u8]) -> Result<BTreeMap<String, String>> {
    let mut settings = BTreeMap::new();
    for entry in output
        .split(|byte| *byte == 0)
        .filter(|entry| !entry.is_empty())
    {
        let entry = std::str::from_utf8(entry).context("decode controller Git configuration")?;
        let (key, value) = entry
            .split_once('\n')
            .with_context(|| format!("controller Git returned malformed entry {entry:?}"))?;
        let key = key.to_ascii_lowercase();
        if INHERITED_GIT_SETTINGS.contains(&key.as_str()) {
            settings.insert(key, value.to_owned());
        }
    }
    Ok(settings)
}

fn workspace_paths(
    locator: &hel_targets::TargetLocator,
    bundle: &ProjectBundle,
    session_id: &str,
) -> Result<(String, Vec<String>)> {
    let root = match locator {
        hel_targets::TargetLocator::LocalBare { .. } => {
            bail!("local bare projects use their selected directory")
        }
        hel_targets::TargetLocator::LocalPodman { .. }
        | hel_targets::TargetLocator::AppleContainer { .. }
        | hel_targets::TargetLocator::SshPodman { .. } => "/workspace".to_string(),
        hel_targets::TargetLocator::AwsEc2 { workspace, .. }
        | hel_targets::TargetLocator::SshBare { workspace, .. } => workspace.clone(),
    };
    if matches!(locator, hel_targets::TargetLocator::AwsEc2 { .. }) {
        let expected = format!(".local/share/hel/workspaces/{session_id}");
        if root != expected {
            bail!("AWS workspace does not match session")
        }
    }
    let primary = bundle.primary().context("bundle primary is missing")?;
    let primary_path = format!("{root}/{}", primary.destination.to_string_lossy());
    let additional = bundle
        .repositories
        .iter()
        .filter(|repository| repository.id != bundle.primary_repo)
        .map(|repository| format!("{root}/{}", repository.destination.to_string_lossy()))
        .collect();
    Ok((primary_path, additional))
}

// npx fallbacks for images that do not already carry an ACP bridge. Keep these
// in lockstep with the global npm installs in
// containers/Containerfile.agent-dev; bridge_pins_match_containerfile() below
// fails the build when they drift.
const CODEX_ACP_FALLBACK_VERSION: &str = "1.1.14";
const CLAUDE_AGENT_ACP_FALLBACK_VERSION: &str = "0.68.0";

fn bridge_launch(
    harness: crate::hel_config::HarnessKind,
    executable: Option<&Path>,
) -> (String, Vec<String>) {
    if let Some(executable) = executable {
        let args = if harness == crate::hel_config::HarnessKind::Kimi {
            vec!["acp".into()]
        } else {
            Vec::new()
        };
        return (executable.to_string_lossy().into_owned(), args);
    }
    match harness {
        crate::hel_config::HarnessKind::Codex => (
            "sh".into(),
            vec![
                "-lc".into(),
                format!("if command -v codex-acp >/dev/null 2>&1; then exec codex-acp; fi; {}; exec npx -y @agentclientprotocol/codex-acp@{CODEX_ACP_FALLBACK_VERSION}", ensure_node_script()),
            ],
        ),
        crate::hel_config::HarnessKind::Claude => (
            "sh".into(),
            vec![
                "-lc".into(),
                format!("if command -v claude-agent-acp >/dev/null 2>&1; then exec claude-agent-acp; fi; {}; exec npx -y @agentclientprotocol/claude-agent-acp@{CLAUDE_AGENT_ACP_FALLBACK_VERSION}", ensure_node_script()),
            ],
        ),
        crate::hel_config::HarnessKind::Kimi => (
            "sh".into(),
            vec![
                "-lc".into(),
                "if command -v kimi >/dev/null 2>&1; then exec kimi acp; elif [ -x \"$HOME/.kimi-code/bin/kimi\" ]; then exec \"$HOME/.kimi-code/bin/kimi\" acp; elif command -v curl >/dev/null 2>&1; then curl -fsSL https://code.kimi.com/kimi-code/install.sh | bash && exec \"$HOME/.kimi-code/bin/kimi\" acp; else echo 'Hel needs compatible Kimi Code or curl for its official installer' >&2; exit 127; fi".into(),
            ],
        ),
    }
}

fn ensure_node_script() -> &'static str {
    "if ! command -v npx >/dev/null 2>&1; then if [ \"$(id -u)\" = 0 ]; then SUDO=''; elif command -v sudo >/dev/null 2>&1 && sudo -n true; then SUDO='sudo'; else echo 'Hel needs Node/npx or passwordless sudo to install it' >&2; exit 127; fi; if command -v apt-get >/dev/null 2>&1; then $SUDO apt-get update && $SUDO apt-get install -y nodejs npm; elif command -v dnf >/dev/null 2>&1; then $SUDO dnf install -y nodejs npm; elif command -v yum >/dev/null 2>&1; then $SUDO yum install -y nodejs npm; elif command -v apk >/dev/null 2>&1; then $SUDO apk add --no-cache nodejs npm; else echo 'Hel cannot install Node on this image; bake npx or a compatible ACP bridge into it' >&2; exit 127; fi; fi"
}

const HEL_CONTAINER_ENVIRONMENT: &str = "## Hel container environment\n\nThis session runs in a disposable Hel container. When the session closes, Hel checkpoints everything in project workspace directories (`/workspace/...`), including committed work, staged and unstaged changes, and untracked files.\n\nEverything outside the workspace, including installed packages, `$HOME`, and `/tmp`, is ephemeral and will be lost when the session ends. Keep durable results in the workspace or push them to a remote.\n";

fn stage_profile(profile: &crate::hel_config::HarnessProfile, destination: &Path) -> Result<()> {
    let harness = profile.kind;
    let source = profile.home.as_path();
    std::fs::create_dir_all(destination)?;
    let allowlist: &[&str] = match harness {
        crate::hel_config::HarnessKind::Codex => &[
            "auth.json",
            "config.toml",
            "AGENTS.md",
            "instructions.md",
            "rules",
            "skills",
        ],
        crate::hel_config::HarnessKind::Claude => &[
            ".claude.json",
            ".credentials.json",
            "settings.json",
            "CLAUDE.md",
            "skills",
            "plugins",
        ],
        crate::hel_config::HarnessKind::Kimi => &[
            "credentials",
            "config.toml",
            "device_id",
            "AGENTS.md",
            "SYSTEM.md",
            "mcp.json",
            "skills",
            "agents",
            "plugins",
        ],
    };
    for name in allowlist {
        let from = source.join(name);
        if from.exists() {
            copy_profile_entry(&from, &destination.join(name))?;
        }
    }
    append_hel_container_environment(profile.kind, destination)
}

/// Add the Hel lifecycle guidance only to the staged per-session profile.
fn append_hel_container_environment(
    harness: crate::hel_config::HarnessKind,
    destination: &Path,
) -> Result<()> {
    let instructions = match harness {
        crate::hel_config::HarnessKind::Codex => "AGENTS.md",
        crate::hel_config::HarnessKind::Claude => "CLAUDE.md",
        crate::hel_config::HarnessKind::Kimi => "SYSTEM.md",
    };
    let path = destination.join(instructions);
    let separator = match std::fs::read_to_string(&path) {
        Ok(contents) if !contents.is_empty() && !contents.ends_with('\n') => "\n\n",
        Ok(contents) if !contents.is_empty() => "\n",
        Ok(_) => "",
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => "",
        Err(error) => return Err(error.into()),
    };
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("open staged harness instructions {}", path.display()))?;
    file.write_all(separator.as_bytes())?;
    file.write_all(HEL_CONTAINER_ENVIRONMENT.as_bytes())?;
    Ok(())
}

fn copy_profile_entry(source: &Path, destination: &Path) -> Result<()> {
    let metadata = std::fs::symlink_metadata(source)?;
    if metadata.file_type().is_symlink() {
        return Ok(());
    }
    if metadata.is_file() {
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::copy(source, destination)?;
        return Ok(());
    }
    if metadata.is_dir() {
        std::fs::create_dir_all(destination)?;
        for entry in std::fs::read_dir(source)? {
            let entry = entry?;
            copy_profile_entry(&entry.path(), &destination.join(entry.file_name()))?;
        }
        std::fs::set_permissions(destination, metadata.permissions())?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn install_worker_files(
    executor: &impl CommandExecutor,
    locator: &hel_targets::TargetLocator,
    session_id: &str,
    worker_root: &str,
    profile_home: &str,
    worker_binary: &Path,
    launch_config: &Path,
    ownership: &Path,
    profile_stage: &Path,
) -> Result<()> {
    match locator {
        hel_targets::TargetLocator::LocalBare { .. } => {
            for command in [
                CommandSpec::new("mkdir", ["-p", worker_root])
                    .purpose("create local bare worker directory"),
                CommandSpec::new(
                    "cp",
                    [
                        worker_binary.to_string_lossy().into_owned(),
                        format!("{worker_root}/hel"),
                    ],
                )
                .purpose("install local Hel worker"),
                CommandSpec::new(
                    "cp",
                    [
                        launch_config.to_string_lossy().into_owned(),
                        format!("{worker_root}/launch.json"),
                    ],
                )
                .purpose("install local worker launch configuration"),
                CommandSpec::new(
                    "cp",
                    [
                        ownership.to_string_lossy().into_owned(),
                        format!("{worker_root}/ownership.json"),
                    ],
                )
                .purpose("install local worker ownership marker"),
                CommandSpec::new("chmod", ["700", &format!("{worker_root}/hel")])
                    .purpose("make local Hel worker executable"),
            ] {
                execute_checked(executor, command)?;
            }
        }
        hel_targets::TargetLocator::LocalPodman { container_id }
        | hel_targets::TargetLocator::AppleContainer { container_id } => {
            let engine = if matches!(locator, hel_targets::TargetLocator::LocalPodman { .. }) {
                "podman"
            } else {
                "container"
            };
            for command in [
                CommandSpec::new(
                    engine,
                    [
                        "exec".into(),
                        container_id.clone(),
                        "mkdir".into(),
                        "-p".into(),
                        worker_root.into(),
                        profile_home.into(),
                    ],
                )
                .purpose("create target worker directories"),
                CommandSpec::new(
                    engine,
                    [
                        "cp".into(),
                        worker_binary.to_string_lossy().into_owned(),
                        format!("{container_id}:{worker_root}/hel"),
                    ],
                )
                .purpose("upload Hel worker"),
                CommandSpec::new(
                    engine,
                    [
                        "cp".into(),
                        launch_config.to_string_lossy().into_owned(),
                        format!("{container_id}:{worker_root}/launch.json"),
                    ],
                )
                .purpose("upload worker launch configuration"),
                CommandSpec::new(
                    engine,
                    [
                        "cp".into(),
                        ownership.to_string_lossy().into_owned(),
                        format!("{container_id}:{worker_root}/ownership.json"),
                    ],
                )
                .purpose("upload worker ownership marker"),
                CommandSpec::new(
                    engine,
                    [
                        "cp".into(),
                        format!("{}/.", profile_stage.display()),
                        format!("{container_id}:{profile_home}"),
                    ],
                )
                .purpose("upload harness profile allowlist"),
                CommandSpec::new(
                    engine,
                    [
                        "exec".into(),
                        container_id.clone(),
                        "chmod".into(),
                        "700".into(),
                        format!("{worker_root}/hel"),
                    ],
                )
                .purpose("make Hel worker executable"),
                CommandSpec::new(
                    engine,
                    [
                        "exec".into(),
                        container_id.clone(),
                        "chmod".into(),
                        "-R".into(),
                        "go-rwx".into(),
                        profile_home.into(),
                    ],
                )
                .purpose("restrict harness profile permissions"),
            ] {
                execute_checked(executor, command)?;
            }
        }
        hel_targets::TargetLocator::AwsEc2 { ssh, .. }
        | hel_targets::TargetLocator::SshBare { ssh, .. } => {
            install_worker_over_ssh(
                executor,
                ssh,
                worker_root,
                profile_home,
                worker_binary,
                launch_config,
                ownership,
                profile_stage,
            )?;
        }
        hel_targets::TargetLocator::SshPodman { ssh, container_id } => {
            // The worker binary is 10-30 MB and identical across sessions, so
            // keep it in a content-addressed cache on the remote host and copy
            // it over the wire only once per unique binary.
            let digest = worker_binary_digest(worker_binary)?;
            // Home-relative, not "~/": ssh_command_spec single-quotes every
            // argument, so a tilde would stay literal in the remote shell
            // while scp expands it, and the two sides would disagree. Both
            // ssh commands (cwd is the login home) and scp resolve a relative
            // path against the remote home.
            let cache_dir = format!(".cache/hel/workers/{digest}");
            let cached_worker = format!("{cache_dir}/hel");
            let cached = matches!(
                executor.execute(
                    &ssh_command_spec(ssh, ["test", "-f", &cached_worker])
                        .purpose("probe cached remote Hel worker"),
                ),
                Ok(output) if output.status == 0
            );
            if !cached {
                execute_checked(
                    executor,
                    ssh_command_spec(ssh, ["mkdir", "-p", &cache_dir])
                        .purpose("create remote worker cache"),
                )?;
                let partial = format!("{cache_dir}/hel.partial-{session_id}");
                execute_checked(
                    executor,
                    scp_command_spec(ssh, worker_binary, &partial, false)
                        .purpose("upload remote Podman worker binary"),
                )?;
                // Rename within the cache directory so the final path only
                // ever names a complete upload.
                execute_checked(
                    executor,
                    ssh_command_spec(ssh, ["mv", &partial, &cached_worker])
                        .purpose("publish cached remote Hel worker"),
                )?;
            }
            let upload = format!(".cache/hel/uploads/{session_id}");
            execute_checked(
                executor,
                ssh_command_spec(ssh, ["mkdir", "-p", &upload])
                    .purpose("create remote upload staging"),
            )?;
            for (source, name) in [
                (launch_config, "launch.json"),
                (ownership, "ownership.json"),
            ] {
                execute_checked(
                    executor,
                    scp_command_spec(ssh, source, &format!("{upload}/{name}"), false)
                        .purpose("upload remote Podman worker file"),
                )?;
            }
            execute_checked(
                executor,
                scp_command_spec(ssh, profile_stage, &format!("{upload}/profile"), true)
                    .purpose("upload remote Podman profile allowlist"),
            )?;
            let remote = [
                vec![
                    "podman".into(),
                    "exec".into(),
                    container_id.clone(),
                    "mkdir".into(),
                    "-p".into(),
                    worker_root.into(),
                    profile_home.into(),
                ],
                vec![
                    "podman".into(),
                    "cp".into(),
                    cached_worker.clone(),
                    format!("{container_id}:{worker_root}/hel"),
                ],
                vec![
                    "podman".into(),
                    "cp".into(),
                    format!("{upload}/launch.json"),
                    format!("{container_id}:{worker_root}/launch.json"),
                ],
                vec![
                    "podman".into(),
                    "cp".into(),
                    format!("{upload}/ownership.json"),
                    format!("{container_id}:{worker_root}/ownership.json"),
                ],
                vec![
                    "podman".into(),
                    "cp".into(),
                    format!("{upload}/profile/."),
                    format!("{container_id}:{profile_home}"),
                ],
                vec![
                    "podman".into(),
                    "exec".into(),
                    container_id.clone(),
                    "chmod".into(),
                    "700".into(),
                    format!("{worker_root}/hel"),
                ],
                vec![
                    "podman".into(),
                    "exec".into(),
                    container_id.clone(),
                    "chmod".into(),
                    "-R".into(),
                    "go-rwx".into(),
                    profile_home.into(),
                ],
                vec!["rm".into(), "-rf".into(), "--".into(), upload.clone()],
            ];
            for args in remote {
                execute_checked(
                    executor,
                    ssh_command_spec(ssh, args).purpose("install remote Podman worker"),
                )?;
            }
        }
    }
    Ok(())
}

/// Content address for the worker binary, used as the remote cache key.
fn worker_binary_digest(worker_binary: &Path) -> Result<String> {
    let bytes = std::fs::read(worker_binary).with_context(|| {
        format!(
            "failed to read worker binary {} for cache addressing",
            worker_binary.display()
        )
    })?;
    Ok(format!("{:x}", Sha256::digest(&bytes)))
}

#[allow(clippy::too_many_arguments)]
fn install_worker_over_ssh(
    executor: &impl CommandExecutor,
    ssh: &SshTarget,
    worker_root: &str,
    profile_home: &str,
    worker_binary: &Path,
    launch_config: &Path,
    ownership: &Path,
    profile_stage: &Path,
) -> Result<()> {
    execute_checked(
        executor,
        ssh_command_spec(ssh, ["mkdir", "-p", worker_root, profile_home])
            .purpose("create SSH worker directories"),
    )?;
    for (source, remote, recursive) in [
        (worker_binary, format!("{worker_root}/hel"), false),
        (launch_config, format!("{worker_root}/launch.json"), false),
        (ownership, format!("{worker_root}/ownership.json"), false),
    ] {
        execute_checked(
            executor,
            scp_command_spec(ssh, source, &remote, recursive).purpose("upload SSH worker file"),
        )?;
    }
    let incoming_profile = format!("{profile_home}.incoming");
    execute_checked(
        executor,
        scp_command_spec(ssh, profile_stage, &incoming_profile, true)
            .purpose("upload SSH harness profile allowlist"),
    )?;
    execute_checked(
        executor,
        ssh_command_spec(
            ssh,
            ["cp", "-R", &format!("{incoming_profile}/."), profile_home],
        )
        .purpose("install SSH harness profile allowlist"),
    )?;
    execute_checked(
        executor,
        ssh_command_spec(ssh, ["rm", "-rf", "--", &incoming_profile])
            .purpose("remove SSH profile staging"),
    )?;
    execute_checked(
        executor,
        ssh_command_spec(ssh, ["chmod", "700", &format!("{worker_root}/hel")])
            .purpose("make SSH worker executable"),
    )?;
    execute_checked(
        executor,
        ssh_command_spec(ssh, ["chmod", "-R", "go-rwx", profile_home])
            .purpose("restrict SSH harness profile permissions"),
    )?;
    Ok(())
}

fn start_worker(
    executor: &impl CommandExecutor,
    locator: &hel_targets::TargetLocator,
    worker_root: &str,
) -> Result<()> {
    let binary = format!("{worker_root}/hel");
    let config = format!("{worker_root}/launch.json");
    // The exit record describes the worker's previous life. Clear it as part
    // of the launch, before the new daemon can be probed: the startup connect
    // loop treats that file as proof the worker it just started has died, so
    // a stale record would abort every restart.
    let clear_exit_record = format!(
        "rm -f {}; ",
        hel_targets::join_remote_command(&[format!("{worker_root}/worker-exit.json")]),
    );
    let detached_script = format!(
        "{clear_exit_record}nohup {} >{} 2>&1 </dev/null &",
        hel_targets::join_remote_command(&[
            binary.clone(),
            "worker".into(),
            "run".into(),
            "--root".into(),
            worker_root.into(),
            "--config".into(),
            config.clone(),
        ]),
        hel_targets::join_remote_command(&[format!("{worker_root}/worker.log")]),
    );
    // Redirect daemon output to worker.log in every launch mode; an
    // unexplained dead worker is undebuggable without it.
    let exec_script = format!(
        "{clear_exit_record}exec {} >{} 2>&1",
        hel_targets::join_remote_command(&[
            binary.clone(),
            "worker".into(),
            "run".into(),
            "--root".into(),
            worker_root.into(),
            "--config".into(),
            config.clone(),
        ]),
        hel_targets::join_remote_command(&[format!("{worker_root}/worker.log")]),
    );
    let command = match locator {
        hel_targets::TargetLocator::LocalBare { .. } => {
            CommandSpec::new("sh", ["-c", &detached_script])
        }
        hel_targets::TargetLocator::LocalPodman { container_id } => CommandSpec::new(
            "podman",
            ["exec", "--detach", container_id, "sh", "-c", &exec_script],
        ),
        hel_targets::TargetLocator::AppleContainer { container_id } => CommandSpec::new(
            "container",
            ["exec", "--detach", container_id, "sh", "-c", &exec_script],
        ),
        hel_targets::TargetLocator::AwsEc2 { ssh, .. }
        | hel_targets::TargetLocator::SshBare { ssh, .. } => {
            ssh_command_spec(ssh, ["sh", "-lc", &detached_script])
        }
        hel_targets::TargetLocator::SshPodman { ssh, container_id } => ssh_command_spec(
            ssh,
            [
                "podman",
                "exec",
                "--detach",
                container_id,
                "sh",
                "-c",
                &exec_script,
            ],
        ),
    }
    .purpose("start detached Hel worker");
    execute_checked(executor, command)?;
    Ok(())
}

fn ssh_command_spec(
    ssh: &SshTarget,
    args: impl IntoIterator<Item = impl AsRef<str>>,
) -> CommandSpec {
    let remote = args
        .into_iter()
        .map(|arg| arg.as_ref().to_string())
        .collect::<Vec<_>>();
    let mut command_args = ssh.ssh_args.clone();
    command_args.push(ssh.destination.clone());
    command_args.push(hel_targets::join_remote_command(&remote));
    CommandSpec::new("ssh", command_args)
}

fn scp_command_spec(ssh: &SshTarget, source: &Path, remote: &str, recursive: bool) -> CommandSpec {
    let mut args = ssh.ssh_args.clone();
    if recursive {
        args.push("-r".into());
    }
    args.push(source.to_string_lossy().into_owned());
    args.push(format!("{}:{remote}", ssh.destination));
    CommandSpec::new("scp", args)
}

/// Enrich an opaque handshake failure by running the installed worker binary
/// directly in the target. This surfaces loader errors (for example a
/// glibc-linked worker inside an older-glibc container) that a detached start
/// swallows.
fn worker_probe_diagnosis(
    executor: &impl CommandExecutor,
    locator: &hel_targets::TargetLocator,
    worker_root: &str,
    error: anyhow::Error,
) -> anyhow::Error {
    let binary = format!("{worker_root}/hel");
    let command = match locator {
        hel_targets::TargetLocator::LocalBare { .. } => {
            CommandSpec::new(binary.clone(), ["--version"])
        }
        hel_targets::TargetLocator::LocalPodman { container_id } => {
            CommandSpec::new("podman", ["exec", container_id, &binary, "--version"])
        }
        hel_targets::TargetLocator::AppleContainer { container_id } => {
            CommandSpec::new("container", ["exec", container_id, &binary, "--version"])
        }
        hel_targets::TargetLocator::AwsEc2 { ssh, .. }
        | hel_targets::TargetLocator::SshBare { ssh, .. } => {
            ssh_command_spec(ssh, [binary.as_str(), "--version"])
        }
        hel_targets::TargetLocator::SshPodman { ssh, container_id } => ssh_command_spec(
            ssh,
            ["podman", "exec", container_id, binary.as_str(), "--version"],
        ),
    }
    .purpose("probe installed worker binary");
    let error = match executor.execute(&command) {
        Ok(output) if output.status == 0 => error,
        Ok(output) => {
            let detail = String::from_utf8_lossy(&output.stderr).trim().to_string();
            error.context(format!(
                "worker binary {binary} fails to run in the target: {detail}; \
                 if this is a loader/glibc error, provide a musl worker \
                 (cargo build --release --target <arch>-unknown-linux-musl, \
                 or set HEL_WORKER_BINARY/HEL_WORKER_DIR)"
            ))
        }
        Err(probe_error) => error.context(format!("worker probe failed: {probe_error:#}")),
    };
    match worker_last_words(executor, locator, worker_root) {
        Some(last_words) => error.context(last_words),
        None => error,
    }
}

/// Fetch the dead worker's structured exit record and log tail from the
/// target, so unreachable-worker errors carry the root cause.
fn worker_last_words(
    executor: &impl CommandExecutor,
    locator: &hel_targets::TargetLocator,
    worker_root: &str,
) -> Option<String> {
    let script = format!(
        "if [ -f {root}/worker-exit.json ]; then echo '{marker}'; cat {root}/worker-exit.json; fi; if [ -f {root}/worker.log ]; then echo '--- worker.log (tail) ---'; tail -n 20 {root}/worker.log; fi",
        root = worker_root,
        marker = WORKER_EXIT_RECORD_MARKER
    );
    let command = match locator {
        hel_targets::TargetLocator::LocalBare { .. } => CommandSpec::new("sh", ["-c", &script]),
        hel_targets::TargetLocator::LocalPodman { container_id } => {
            CommandSpec::new("podman", ["exec", container_id, "sh", "-c", &script])
        }
        hel_targets::TargetLocator::AppleContainer { container_id } => {
            CommandSpec::new("container", ["exec", container_id, "sh", "-c", &script])
        }
        hel_targets::TargetLocator::AwsEc2 { ssh, .. }
        | hel_targets::TargetLocator::SshBare { ssh, .. } => {
            ssh_command_spec(ssh, ["sh", "-lc", &script])
        }
        hel_targets::TargetLocator::SshPodman { ssh, container_id } => {
            ssh_command_spec(ssh, ["podman", "exec", container_id, "sh", "-c", &script])
        }
    }
    .purpose("collect worker last words");
    let output = executor.execute(&command).ok()?;
    let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
    (!text.is_empty()).then(|| format!("worker diagnostics:\n{text}"))
}

fn now() -> String {
    Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn apply_close_checkpoint_started(record: &mut SessionRecord, updated_at: String) {
    record.state = SessionState::Closing;
    record.updated_at = updated_at;
    record.last_checkpoint_error = None;
}

fn verify_installed_checkpoint_gate(
    session_id: &str,
    checkpoint: &CheckpointMetadata,
) -> Result<()> {
    let archive = verify_archive_streaming(&checkpoint.archive_path).with_context(|| {
        format!(
            "re-open installed checkpoint {} before target cleanup",
            checkpoint.archive_path.display()
        )
    })?;
    ensure!(
        archive.archive_sha256 == checkpoint.sha256,
        "refusing target cleanup for session {session_id}: installed checkpoint SHA changed"
    );
    ensure!(
        archive.manifest.session.id == session_id,
        "refusing target cleanup for session {session_id}: installed checkpoint belongs to session {}",
        archive.manifest.session.id
    );
    let canonical = archive.canonical_session;
    ensure!(
        canonical.event_frontier == checkpoint.event_frontier,
        "refusing target cleanup for session {session_id}: installed checkpoint frontier changed from {} to {}",
        checkpoint.event_frontier,
        canonical.event_frontier
    );
    Ok(())
}

fn verify_checkpoint_artifact(session_id: &str, artifact: &CheckpointArtifact) -> Result<()> {
    let archive = verify_archive_streaming(&artifact.metadata.archive_path).with_context(|| {
        format!(
            "re-open completed checkpoint {}",
            artifact.metadata.archive_path.display()
        )
    })?;
    ensure!(
        archive.archive_sha256 == artifact.metadata.sha256,
        "completed checkpoint SHA changed before persistence"
    );
    ensure!(
        archive.manifest.session.id == session_id,
        "completed checkpoint belongs to session {} instead of {session_id}",
        archive.manifest.session.id
    );
    let canonical = archive.canonical_session;
    ensure!(
        canonical.event_frontier == artifact.metadata.event_frontier,
        "completed checkpoint frontier changed from {} to {}",
        artifact.metadata.event_frontier,
        canonical.event_frontier
    );
    ensure!(
        canonical.event_frontier_digest == artifact.event_frontier_digest,
        "completed checkpoint frontier digest changed before persistence"
    );
    Ok(())
}

fn apply_interrupted_close_error(
    record: &mut SessionRecord,
    error: &anyhow::Error,
    updated_at: &str,
) {
    let destroying = record.state == SessionState::Destroying;
    if !destroying {
        record.state = SessionState::Closing;
    }
    record.updated_at = updated_at.to_owned();
    record.last_error = Some(if destroying {
        format!("target cleanup is safely retryable from its verified checkpoint: {error:#}")
    } else {
        format!("close is safely resumable from its verified checkpoint: {error:#}")
    });
}

fn restore_session_after_persistence_failure(
    state: &mut HelState,
    session_id: &str,
    previous: &SessionRecord,
    primary: anyhow::Error,
    persist: impl FnOnce(&SessionRecord) -> Result<()>,
) -> anyhow::Error {
    state
        .sessions
        .insert(session_id.to_owned(), previous.clone());
    let restored = state
        .sessions
        .get(session_id)
        .expect("restored session record disappeared");
    match persist(restored) {
        Ok(()) => primary,
        Err(error) => primary.context(format!(
            "restored prior session state in memory, but failed to persist the rollback: {error:#}"
        )),
    }
}

fn persist_session_record_transition_or_restore(
    state: &mut HelState,
    session_id: &str,
    previous: &SessionRecord,
    context: &'static str,
    persist: &impl Fn(&SessionRecord) -> Result<()>,
) -> Result<()> {
    let result = persist(
        state
            .sessions
            .get(session_id)
            .expect("checkpoint session disappeared before persistence"),
    );
    match result {
        Ok(()) => Ok(()),
        Err(error) => Err(restore_session_after_persistence_failure(
            state,
            session_id,
            previous,
            error.context(context),
            persist,
        )),
    }
}

fn prune_replaced_checkpoint(previous: Option<&CheckpointMetadata>, current: &CheckpointMetadata) {
    let Some(previous) = previous.filter(|old| old.archive_path != current.archive_path) else {
        return;
    };
    if let Err(error) = std::fs::remove_file(&previous.archive_path)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(
            path = %previous.archive_path.display(),
            "could not remove superseded recovery copy: {error}"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;
    use crate::hel_config::{
        ContainerTemplate as ConfigContainer, HarnessProfile, ProjectRepository,
    };

    const RESUME_ROLLBACK_TEST_CHILD: &str = "HEL_RESUME_ROLLBACK_TEST_CHILD";

    fn checkpoint_test_session(session_id: &str) -> SessionRecord {
        SessionRecord {
            id: session_id.into(),
            title: "checkpoint transition".into(),
            harness_kind: crate::hel_config::HarnessKind::Codex,
            last_profile: "codex".into(),
            bundle_id: "project".into(),
            project_directory: None,
            managed_worktree: None,
            target_template_id: "podman".into(),
            resource_allocation: None,
            additional_mounts: Vec::new(),
            state: SessionState::Running,
            target: None,
            native_session_id: Some("native-session".into()),
            acp_session_title: None,
            session_title_override: None,
            created_at: "2026-08-12T00:00:00Z".into(),
            updated_at: "2026-08-12T00:00:00Z".into(),
            detached_after_event_ordinal: 0,
            draft_input: String::new(),
            last_error: None,
            last_checkpoint_error: None,
            checkpoint: None,
        }
    }

    #[tokio::test]
    async fn native_session_wait_stops_as_soon_as_cancellation_is_observed() {
        struct CancellingProbe {
            cancelled: std::sync::Arc<std::sync::atomic::AtomicBool>,
            polls: usize,
        }

        impl NativeSessionProbe for CancellingProbe {
            async fn native_session_readiness(&mut self) -> Result<NativeSessionReadiness> {
                self.polls += 1;
                self.cancelled
                    .store(true, std::sync::atomic::Ordering::Release);
                Ok(NativeSessionReadiness::Waiting)
            }
        }

        let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let executor = CancellableProcessExecutor::new(cancelled.clone());
        let mut probe = CancellingProbe {
            cancelled,
            polls: 0,
        };

        let error = wait_for_native_session(&mut probe, &executor)
            .await
            .unwrap_err();

        assert_eq!(probe.polls, 1);
        assert!(
            error
                .to_string()
                .contains("operation cancelled while waiting for ACP runtime startup")
        );
    }

    #[tokio::test]
    async fn native_session_wait_cancels_while_readiness_probe_is_pending() {
        struct PendingProbe {
            polls: usize,
        }

        impl NativeSessionProbe for PendingProbe {
            async fn native_session_readiness(&mut self) -> Result<NativeSessionReadiness> {
                self.polls += 1;
                std::future::pending().await
            }
        }

        let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let executor = CancellableProcessExecutor::new(cancelled.clone());
        let mut probe = PendingProbe { polls: 0 };
        let cancellation = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            cancelled.store(true, std::sync::atomic::Ordering::Release);
        });
        let started = tokio::time::Instant::now();

        let error = wait_for_native_session(&mut probe, &executor)
            .await
            .unwrap_err();
        cancellation.await.unwrap();

        assert_eq!(probe.polls, 1);
        assert!(started.elapsed() < std::time::Duration::from_millis(250));
        assert!(
            error
                .to_string()
                .contains("operation cancelled while waiting for ACP runtime startup")
        );
    }

    /// Scripted stand-in for a worker that is still binding its control
    /// socket. It fails every connection until `accepts_after_attempts`, and
    /// reports a recorded death once `death_after_attempts` attempts ran.
    struct FakeStartingWorker {
        attempts: usize,
        accepts_after_attempts: Option<usize>,
        death_after_attempts: Option<usize>,
        cancel_on_attempt: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    }

    impl FakeStartingWorker {
        fn never_accepts() -> Self {
            Self {
                attempts: 0,
                accepts_after_attempts: None,
                death_after_attempts: None,
                cancel_on_attempt: None,
            }
        }

        fn accepting_after(attempts: usize) -> Self {
            Self {
                accepts_after_attempts: Some(attempts),
                ..Self::never_accepts()
            }
        }
    }

    impl StartingWorkerProbe for FakeStartingWorker {
        type Relay = &'static str;

        async fn connect(&mut self) -> Result<&'static str> {
            self.attempts += 1;
            if let Some(cancel) = &self.cancel_on_attempt {
                cancel.store(true, std::sync::atomic::Ordering::Release);
            }
            match self.accepts_after_attempts {
                Some(accepts) if self.attempts >= accepts => Ok("relay"),
                _ => bail!("connect attempt {} refused", self.attempts),
            }
        }

        fn death_report(&self) -> Option<String> {
            let died_after = self.death_after_attempts?;
            (self.attempts >= died_after).then(|| {
                format!(
                    "worker diagnostics:\n{WORKER_EXIT_RECORD_MARKER}\n\
                     {{\"reason\":\"durable relay open failed\"}}"
                )
            })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn startup_connect_retries_until_worker_accepts() {
        let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let executor = CancellableProcessExecutor::new(cancelled);
        let mut worker = FakeStartingWorker::accepting_after(4);

        let relay = connect_to_starting_worker(&mut worker, &executor)
            .await
            .unwrap();

        assert_eq!(relay, "relay");
        assert_eq!(worker.attempts, 4);
    }

    #[tokio::test(start_paused = true)]
    async fn startup_connect_reports_a_worker_that_recorded_its_death() {
        let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let executor = CancellableProcessExecutor::new(cancelled);
        let mut worker = FakeStartingWorker {
            death_after_attempts: Some(1),
            ..FakeStartingWorker::never_accepts()
        };
        let started = tokio::time::Instant::now();

        let error = connect_to_starting_worker(&mut worker, &executor)
            .await
            .unwrap_err();

        assert_eq!(worker.attempts, 1);
        assert!(started.elapsed() < WORKER_STARTUP_CONNECT_INTERVAL);
        let reported = format!("{error:#}");
        assert!(reported.contains(WORKER_EXIT_RECORD_MARKER), "{reported}");
        assert!(reported.contains("connect attempt 1 refused"), "{reported}");
    }

    #[tokio::test(start_paused = true)]
    async fn startup_connect_stops_as_soon_as_cancellation_is_observed() {
        let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let executor = CancellableProcessExecutor::new(cancelled.clone());
        let mut worker = FakeStartingWorker {
            cancel_on_attempt: Some(cancelled),
            ..FakeStartingWorker::never_accepts()
        };

        let error = connect_to_starting_worker(&mut worker, &executor)
            .await
            .unwrap_err();

        assert_eq!(worker.attempts, 1);
        assert!(
            error
                .to_string()
                .contains("operation cancelled while connecting to the worker relay"),
            "{error:#}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn startup_connect_gives_up_with_the_last_error_after_the_deadline() {
        let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let executor = CancellableProcessExecutor::new(cancelled);
        let mut worker = FakeStartingWorker::never_accepts();
        let started = tokio::time::Instant::now();

        let error = connect_to_starting_worker(&mut worker, &executor)
            .await
            .unwrap_err();

        assert!(worker.attempts > 1, "{} attempts", worker.attempts);
        assert!(started.elapsed() >= WORKER_STARTUP_CONNECT_TIMEOUT);
        let reported = format!("{error:#}");
        assert!(
            reported.contains(&format!("connect attempt {} refused", worker.attempts)),
            "{reported}"
        );
    }

    #[test]
    fn startup_reconciliation_only_removes_unreferenced_controller_checkpoints() {
        let directory = tempfile::tempdir().unwrap();
        let session_id = "1123456789abcdef0123456789abcdef";
        let referenced_name =
            format!("{session_id}-7-archive-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.hel.zip");
        let orphan_name =
            format!("{session_id}-8-archive-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.hel.zip");
        let imported_name = format!("{session_id}.hel.zip");
        for name in [
            &referenced_name,
            &orphan_name,
            &imported_name,
            "notes.hel.zip",
        ] {
            std::fs::write(directory.path().join(name), b"test").unwrap();
        }
        let mut state = HelState::default();
        let mut session = checkpoint_test_session(session_id);
        session.checkpoint = Some(CheckpointMetadata {
            archive_path: directory.path().join(&referenced_name),
            sha256: "c".repeat(64),
            created_at: "2026-08-12T00:00:00Z".into(),
            event_frontier: 7,
        });
        state.sessions.insert(session_id.into(), session);

        assert_eq!(
            reconcile_managed_checkpoint_archives_in(directory.path(), &state).unwrap(),
            1
        );
        assert!(directory.path().join(referenced_name).exists());
        assert!(!directory.path().join(orphan_name).exists());
        assert!(directory.path().join(imported_name).exists());
        assert!(directory.path().join("notes.hel.zip").exists());
    }

    fn write_checkpoint_gate_archive(
        directory: &Path,
        session_id: &str,
        event_frontier: u64,
    ) -> CheckpointMetadata {
        let archive_path = directory.join(format!("{session_id}.hel.zip"));
        let verified = write_archive_atomic(
            &archive_path,
            &ArchiveInput {
                session: SessionManifest {
                    id: session_id.into(),
                    title: "checkpoint gate".into(),
                    harness_kind: crate::hel_config::HarnessKind::Codex,
                    profile_id: "codex".into(),
                    native_session_id: "native-session".into(),
                    created_at: "2026-08-12T00:00:00Z".into(),
                    checkpointed_at: "2026-08-14T12:00:00Z".into(),
                    hel_version: "test".into(),
                    relay_version: "test".into(),
                    adapter_version: "test".into(),
                },
                target: TargetManifest {
                    template_id: "local".into(),
                    target_kind: "local-bare".into(),
                    details: BTreeMap::new(),
                },
                bundle: BundleManifest {
                    id: "project".into(),
                    primary_repository: "project".into(),
                },
                canonical_session: crate::hel_archive::CanonicalSessionSnapshot {
                    event_frontier,
                    event_frontier_digest: if event_frontier == 0 {
                        crate::hel_archive::EVENT_FRONTIER_GENESIS_DIGEST.into()
                    } else {
                        "a".repeat(64)
                    },
                    session: crate::hel_archive::CanonicalSessionState {
                        execution: crate::hel_archive::CanonicalExecutionState::Idle,
                        last_activity_at_ms: (event_frontier > 0).then_some(1_234),
                        session_title: None,
                        configuration: BTreeMap::new(),
                    },
                    transcript: Vec::new(),
                    queued_prompts: Vec::new(),
                },
                native_artifacts: Vec::new(),
                repositories: Vec::new(),
            },
        )
        .unwrap();
        CheckpointMetadata {
            archive_path,
            sha256: verified.archive_sha256,
            created_at: "2026-08-14T12:00:00Z".into(),
            event_frontier,
        }
    }

    #[test]
    fn recovery_artifact_final_verification_checks_the_latched_digest() {
        let directory = tempfile::tempdir().unwrap();
        let session_id = "1123456789abcdef0123456789abcdef";
        let metadata = write_checkpoint_gate_archive(directory.path(), session_id, 7);
        let mut artifact = CheckpointArtifact {
            metadata,
            native_session_id: "native-session".into(),
            event_frontier_digest: "a".repeat(64),
        };

        verify_checkpoint_artifact(session_id, &artifact).unwrap();
        artifact.event_frontier_digest = "b".repeat(64);
        assert!(
            verify_checkpoint_artifact(session_id, &artifact)
                .unwrap_err()
                .to_string()
                .contains("frontier digest changed")
        );
    }

    /// A snapshot of a session whose checkpoint barrier is open but not yet
    /// ready, projected exactly at `cursor`.
    fn checkpoint_barrier_snapshot(cursor: &RelayCursor) -> ManagedSessionSnapshot {
        let mut materialized = MaterializedSession::empty("session-1");
        materialized.applied_event_ordinal = cursor.ordinal;
        materialized.applied_event_digest = cursor.digest.clone();
        ManagedSessionSnapshot {
            materialized,
            latest_auth_failure_ordinal: None,
            operational: crate::hel_worker::RelayOperationalState {
                session_id: "session-1".into(),
                execution: RelayExecutionState::Idle,
                latest_ordinal: cursor.ordinal,
                latest_digest: cursor.digest.clone(),
                acknowledged_through: cursor.ordinal,
                acknowledged_digest: cursor.digest.clone(),
                recovery_floor_ordinal: 0,
                recovery_floor_digest: crate::hel_worker::RELAY_EVENT_GENESIS_DIGEST.into(),
                native_session_id: Some("native-session".into()),
                agent_capabilities: None,
                agent_info: None,
                config_options: Vec::new(),
                available_commands: Vec::new(),
                config: BTreeMap::new(),
                active_prompt: None,
                queued_prompts: Vec::new(),
                checkpoint_barrier: Some("checkpoint-1".into()),
                checkpoint_ready: None,
            },
        }
    }

    #[test]
    fn checkpoint_barrier_is_not_reached_until_its_ready_cursor_is_projected() {
        let cursor = RelayCursor {
            ordinal: 7,
            digest: "a".repeat(64),
        };
        let mut snapshot = checkpoint_barrier_snapshot(&cursor);

        assert!(!checkpoint_barrier_is_ready(&snapshot, "checkpoint-1"));
        snapshot.operational.checkpoint_ready = Some(cursor.clone());
        assert!(checkpoint_barrier_is_ready(&snapshot, "checkpoint-1"));
        validate_checkpoint_barrier_snapshot(&snapshot, "checkpoint-1", &cursor).unwrap();
    }

    #[test]
    fn checkpoint_revalidation_accepts_a_frontier_that_moved_past_the_ready_cursor() {
        let cursor = RelayCursor {
            ordinal: 7,
            digest: "a".repeat(64),
        };
        let mut snapshot = checkpoint_barrier_snapshot(&cursor);
        snapshot.operational.checkpoint_ready = Some(cursor.clone());

        // An open ordinary barrier keeps accepting and journalling commands; it
        // only freezes dispatch. The archive still matches the sealed
        // workspace, so a frontier past the ready cursor stays valid.
        snapshot.operational.latest_ordinal = cursor.ordinal + 2;
        snapshot.operational.latest_digest = "b".repeat(64);
        snapshot.materialized.applied_event_ordinal = cursor.ordinal + 2;
        snapshot.materialized.applied_event_digest = "b".repeat(64);
        validate_checkpoint_barrier_snapshot(&snapshot, "checkpoint-1", &cursor).unwrap();

        // Losing the barrier, or reaching a different cut, still invalidates it.
        snapshot.operational.checkpoint_ready = Some(RelayCursor {
            ordinal: cursor.ordinal + 1,
            digest: "c".repeat(64),
        });
        assert!(validate_checkpoint_barrier_snapshot(&snapshot, "checkpoint-1", &cursor).is_err());
        snapshot.operational.checkpoint_ready = Some(cursor.clone());
        snapshot.operational.checkpoint_barrier = None;
        assert!(validate_checkpoint_barrier_snapshot(&snapshot, "checkpoint-1", &cursor).is_err());
    }

    const LATCH_RELAY_ROOT: &str = "HEL_TEST_LATCH_RELAY_ROOT";
    const LATCH_RELAY_STARTS: &str = "HEL_TEST_LATCH_RELAY_STARTS";
    const LATCH_TEST_CHILD: &str = "HEL_TEST_LATCH_CHILD";
    const ABANDON_TEST_CHILD: &str = "HEL_TEST_ABANDON_LATCH_CHILD";
    const LATCH_RELAY_SESSION: &str = "018f9dd2-a3b4-7c8d-9000-0123456789ab";

    /// Relay server half of the checkpoint latch test.
    ///
    /// A durable relay only reports a checkpoint barrier ready once a dispatch
    /// driver claims it, so this also runs the one step the worker runtime
    /// performs for a barrier. It does nothing unless a parent test points it
    /// at a relay journal root.
    #[test]
    fn latch_relay_child_serves_stdio() {
        let Some(root) = std::env::var_os(LATCH_RELAY_ROOT) else {
            return;
        };
        // With `--nocapture` libtest writes `test <name> ... ` without a
        // trailing newline before the body runs. End that line first so it
        // cannot glue itself onto the first protocol frame.
        println!();
        // Record this start so a parent test can tell a reconnect from a reused
        // connection.
        if let Some(starts) = std::env::var_os(LATCH_RELAY_STARTS) {
            use std::io::Write;
            let mut log = OpenOptions::new()
                .create(true)
                .append(true)
                .open(starts)
                .expect("open the relay start log");
            writeln!(log, "{}", std::process::id()).expect("record this relay start");
        }
        let mut relay =
            crate::hel_worker::DurableRelay::open(Path::new(&root), LATCH_RELAY_SESSION, "1.0.0")
                .expect("open the test relay journal");
        let mut reader = std::io::stdin().lock();
        let mut writer = std::io::stdout().lock();
        while let Some(request) =
            crate::hel_worker::read_relay_frame(&mut reader).expect("read a relay request")
        {
            let response = relay.handle(request);
            crate::hel_worker::write_relay_frame(&mut writer, &response)
                .expect("answer a relay request");
            for claimed in relay
                .claim_pending_commands(true)
                .expect("claim relay commands")
            {
                if matches!(claimed.command, RelayCommand::BeginCheckpoint { .. }) {
                    relay
                        .record_checkpoint_ready(&claimed.command_id)
                        .expect("report the checkpoint barrier ready");
                }
            }
        }
    }

    /// A relay target served by this test binary over stdio. Each start of the
    /// server appends to `starts`, if given.
    #[cfg(unix)]
    fn latch_relay_target(
        relay_root: &Path,
        starts: Option<&Path>,
    ) -> crate::hel_session_manager::RelaySessionTarget {
        // `RelayClient` parses every stdout line as JSON, so libtest's own
        // progress lines are dropped before they reach the protocol reader.
        let script = format!(
            "\"$0\" --exact {}::latch_relay_child_serves_stdio --nocapture | \
             grep --line-buffered '^{{'",
            module_path!()
                .strip_prefix("hel::")
                .unwrap_or(module_path!())
        );
        let mut spec = CommandSpec::new(
            "sh",
            [
                "-c".to_owned(),
                script,
                std::env::current_exe()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned(),
            ],
        )
        .purpose("test latch relay");
        spec.env.insert(
            LATCH_RELAY_ROOT.to_owned(),
            relay_root.to_string_lossy().into_owned(),
        );
        if let Some(starts) = starts {
            spec.env.insert(
                LATCH_RELAY_STARTS.to_owned(),
                starts.to_string_lossy().into_owned(),
            );
        }
        crate::hel_session_manager::RelaySessionTarget {
            session_id: LATCH_RELAY_SESSION.to_owned(),
            spec,
        }
    }

    /// Start a session manager against a live relay and latch a checkpoint on
    /// it, exactly as [`Controller::checkpoint_session_latched`] does.
    #[cfg(unix)]
    async fn latch_a_live_checkpoint(
        relay_root: &Path,
        starts: Option<&Path>,
    ) -> (
        crate::hel_session_manager::SessionManagerChannels,
        ManagedSessionHandle,
        ControllerRelayLease,
        String,
        RelayCursor,
    ) {
        // The projection refuses events for sessions the controller does not
        // know, so register the one the relay journals for.
        crate::hel_database::save_session(&checkpoint_test_session(LATCH_RELAY_SESSION)).unwrap();
        let channels = crate::hel_session_manager::spawn_session_manager().unwrap();
        channels
            .targets
            .send(vec![latch_relay_target(relay_root, starts)])
            .unwrap();
        let handle = channels
            .control
            .wait_for_session(LATCH_RELAY_SESSION, Duration::from_secs(10))
            .await
            .unwrap();

        let lease = handle.lease_connection().await.unwrap();
        let mut relay = ControllerRelayLease::Managed {
            handle: handle.clone(),
            lease: Some(lease),
        };
        let barrier_command_id = new_command_id("checkpoint").unwrap();
        let connection = relay.connection_mut();
        connection
            .submit(
                barrier_command_id.clone(),
                RelayCommand::BeginCheckpoint { reason: None },
            )
            .await
            .unwrap();
        let barrier = wait_for_checkpoint_barrier(connection, &barrier_command_id)
            .await
            .unwrap();
        assert_eq!(
            barrier.materialized.applied_event_ordinal,
            barrier.operational.latest_ordinal
        );
        let cursor = barrier.operational.checkpoint_ready.clone().unwrap();
        (channels, handle, relay, barrier_command_id, cursor)
    }

    /// The session actor absorbs a returned connection on its own task, so the
    /// first command after a latch ends may still be refused.
    #[cfg(unix)]
    async fn wait_until_the_actor_serves_again(handle: &ManagedSessionHandle) {
        for attempt in 0.. {
            if handle.sync_now().await.is_ok() {
                return;
            }
            assert!(attempt < 200, "the actor never took its connection back");
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    /// Ending the latch is the whole point of the split checkpoint: the actor
    /// serves the dashboard again while the archive is still being exported,
    /// and the events it accepts do not invalidate the latched archive.
    #[cfg(unix)]
    #[tokio::test]
    async fn ending_the_checkpoint_latch_returns_the_connection_to_its_actor() {
        // HEL_DATA_DIR is process-global, so run the database-backed half in an
        // exact child test instead of racing unrelated tests in this process.
        if std::env::var_os(LATCH_TEST_CHILD).is_none() {
            let directory = tempfile::tempdir().unwrap();
            let test_name = format!(
                "{}::ending_the_checkpoint_latch_returns_the_connection_to_its_actor",
                module_path!()
                    .strip_prefix("hel::")
                    .unwrap_or(module_path!())
            );
            let output = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", &test_name, "--nocapture"])
                .env(LATCH_TEST_CHILD, "1")
                .env("HEL_DATA_DIR", directory.path())
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "isolated checkpoint latch test failed\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }

        // A connection that never comes back would hang the suite instead of
        // failing it, so turn a stall into a hard error.
        std::thread::spawn(|| {
            std::thread::sleep(std::time::Duration::from_secs(120));
            eprintln!("the checkpoint latch never returned its connection");
            std::process::exit(101);
        });

        let relay_root = tempfile::tempdir().unwrap();
        let (_channels, handle, mut relay, barrier_command_id, cursor) =
            latch_a_live_checkpoint(relay_root.path(), None).await;

        // Latch phase: the projection must be read at the exact ready cursor,
        // so the actor cannot reach the relay at all.
        assert!(
            handle.sync_now().await.is_err(),
            "a latched projection must not be advanced by its own actor"
        );

        relay.end_latch();
        wait_until_the_actor_serves_again(&handle).await;

        // Slow phase, before anything else reaches the relay: the controller
        // reads its barrier back through the actor, which must report what the
        // latch already applied.
        let latched = relay.sync_snapshot().await.unwrap();
        validate_checkpoint_barrier_snapshot(&latched, &barrier_command_id, &cursor).unwrap();

        // A prompt accepted while the archive transfers moves the frontier past
        // the ready cursor. The barrier still seals the same workspace.
        let prompt_ordinal = relay
            .submit(
                new_command_id("prompt").unwrap(),
                RelayCommand::Prompt {
                    prompt: vec![ContentBlock::Text(TextContent::new("hello"))],
                },
            )
            .await
            .unwrap();
        assert!(prompt_ordinal > cursor.ordinal);
        let snapshot = relay.sync_snapshot().await.unwrap();
        assert!(snapshot.operational.latest_ordinal > cursor.ordinal);
        validate_checkpoint_barrier_snapshot(&snapshot, &barrier_command_id, &cursor).unwrap();

        latched_checkpoint(relay, barrier_command_id, cursor)
            .complete()
            .await
            .unwrap();
        handle.sync_now().await.unwrap();
        assert_eq!(
            handle
                .view()
                .snapshot
                .expect("the actor published the completed barrier")
                .operational
                .checkpoint_barrier,
            None
        );
    }

    /// A caller that cannot install a latched archive has to cancel its
    /// barrier. The latch is already back with the session actor, so the only
    /// thing that ends the barrier is dropping the connection that opened it:
    /// the worker cancels barriers whose connection disappears.
    #[cfg(unix)]
    #[tokio::test]
    async fn abandoning_a_latched_checkpoint_drops_the_connection_that_opened_its_barrier() {
        // HEL_DATA_DIR is process-global, so run the database-backed half in an
        // exact child test instead of racing unrelated tests in this process.
        if std::env::var_os(ABANDON_TEST_CHILD).is_none() {
            let directory = tempfile::tempdir().unwrap();
            let test_name = format!(
                "{}::abandoning_a_latched_checkpoint_drops_the_connection_that_opened_its_barrier",
                module_path!()
                    .strip_prefix("hel::")
                    .unwrap_or(module_path!())
            );
            let output = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", &test_name, "--nocapture"])
                .env(ABANDON_TEST_CHILD, "1")
                .env("HEL_DATA_DIR", directory.path())
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "isolated abandoned checkpoint test failed\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }

        // An abandoned barrier that never releases its connection would hang
        // the suite instead of failing it, so turn a stall into a hard error.
        std::thread::spawn(|| {
            std::thread::sleep(std::time::Duration::from_secs(120));
            eprintln!("an abandoned checkpoint never released its relay connection");
            std::process::exit(101);
        });

        let relay_root = tempfile::tempdir().unwrap();
        let start_log = tempfile::tempdir().unwrap();
        let start_log = start_log.path().join("relay-starts");
        let (_channels, handle, mut relay, barrier_command_id, cursor) =
            latch_a_live_checkpoint(relay_root.path(), Some(&start_log)).await;
        relay.end_latch();
        wait_until_the_actor_serves_again(&handle).await;
        assert_eq!(relay_starts(&start_log), 1);

        latched_checkpoint(relay, barrier_command_id, cursor)
            .abandon(LATCH_RELAY_SESSION)
            .await;

        // The actor serves again, which proves the reclaimed lease was not
        // leaked, and it is talking to a new relay process, which proves the
        // connection that opened the barrier was dropped rather than handed
        // back alive.
        wait_until_the_actor_serves_again(&handle).await;
        assert_eq!(relay_starts(&start_log), 2);
    }

    #[cfg(unix)]
    fn relay_starts(path: &Path) -> usize {
        std::fs::read_to_string(path)
            .unwrap_or_default()
            .lines()
            .count()
    }

    /// A latched checkpoint carrying a placeholder artifact. These tests
    /// exercise its relay barrier, not the archive it names.
    #[cfg(unix)]
    fn latched_checkpoint(
        relay: ControllerRelayLease,
        barrier_command_id: String,
        cursor: RelayCursor,
    ) -> LatchedCheckpoint {
        LatchedCheckpoint {
            artifact: CheckpointArtifact {
                metadata: CheckpointMetadata {
                    archive_path: PathBuf::from("checkpoint.hel.zip"),
                    sha256: "a".repeat(64),
                    created_at: now(),
                    event_frontier: cursor.ordinal,
                },
                native_session_id: "native-session".into(),
                event_frontier_digest: cursor.digest.clone(),
            },
            relay,
            barrier_command_id,
            cursor,
        }
    }

    #[test]
    fn controller_store_lock_excludes_a_second_process_owner() {
        let directory = tempfile::tempdir().unwrap();
        let first = ControllerStoreGuard::acquire_at(directory.path()).unwrap();
        run_controller_lock_probe(directory.path(), true);
        drop(first);
        run_controller_lock_probe(directory.path(), false);
    }

    fn run_controller_lock_probe(directory: &Path, expect_locked: bool) {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "hel_controller::tests::controller_store_lock_subprocess_probe",
                "--nocapture",
            ])
            .env("HEL_CONTROLLER_LOCK_PROBE", directory)
            .env(
                "HEL_CONTROLLER_LOCK_EXPECTED",
                if expect_locked { "locked" } else { "available" },
            )
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "controller lock subprocess failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn controller_store_lock_subprocess_probe() {
        let Some(directory) = std::env::var_os("HEL_CONTROLLER_LOCK_PROBE") else {
            return;
        };
        let expected = std::env::var("HEL_CONTROLLER_LOCK_EXPECTED").unwrap();
        let acquired = ControllerStoreGuard::acquire_at(Path::new(&directory));
        match expected.as_str() {
            "locked" => {
                let error = acquired.expect_err("a second process acquired the controller store");
                assert!(error.to_string().contains("another Hel controller"));
            }
            "available" => {
                acquired.expect("released controller store stayed locked");
            }
            value => panic!("unexpected lock probe expectation {value:?}"),
        }
    }

    #[test]
    fn local_mount_source_must_be_an_existing_directory() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("file");
        std::fs::write(&file, "not a directory").unwrap();
        let mut config = HelConfig::default();
        config.targets.insert(
            "local".into(),
            TargetTemplate::LocalPodman {
                container: ConfigContainer {
                    image: "ubuntu:24.04".into(),
                    platform: None,
                    cpus: None,
                    memory: None,
                    environment: BTreeMap::new(),
                },
            },
        );
        let controller = Controller {
            config,
            state: HelState::default(),
        };

        assert!(
            controller
                .validate_mount_source("local", directory.path(), &ProcessExecutor)
                .is_ok()
        );
        for invalid in [file, directory.path().join("missing")] {
            let error = controller
                .validate_mount_source("local", &invalid, &ProcessExecutor)
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("does not exist or is not a directory")
            );
        }
    }

    #[test]
    fn local_bare_project_validation_runs_git_in_the_selected_directory() {
        struct GitExecutor {
            commands: RefCell<Vec<CommandSpec>>,
        }
        impl CommandExecutor for GitExecutor {
            fn execute(&self, command: &CommandSpec) -> Result<CommandOutput> {
                self.commands.borrow_mut().push(command.clone());
                Ok(CommandOutput {
                    status: 0,
                    stdout: b"true\n".to_vec(),
                    stderr: Vec::new(),
                })
            }
        }

        let project = tempfile::tempdir().unwrap();
        let mut config = HelConfig::default();
        config
            .targets
            .insert("raw-localhost".into(), TargetTemplate::LocalBare);
        let controller = Controller {
            config,
            state: HelState::default(),
        };
        let executor = GitExecutor {
            commands: RefCell::new(Vec::new()),
        };

        controller
            .validate_project_directory("raw-localhost", project.path(), &executor)
            .unwrap();
        let commands = executor.commands.borrow();
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].program, "git");
        assert_eq!(commands[0].args[0], "-C");
        assert_eq!(commands[0].args[1], project.path().to_string_lossy());
        assert_eq!(commands[0].args[2..], ["rev-parse", "--verify", "HEAD"]);
    }

    #[test]
    fn packaged_worker_names_match_release_archives() {
        let directory = Path::new("/opt/hel/bin");
        assert_eq!(
            packaged_worker_binary_path(directory, "x86_64-unknown-linux-musl"),
            directory.join("hel-worker-x86_64-unknown-linux-musl")
        );
        assert_eq!(
            packaged_worker_binary_path(directory, "aarch64-unknown-linux-musl"),
            directory.join("hel-worker-aarch64-unknown-linux-musl")
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn replaced_running_executable_is_materialized_for_worker_upload() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let replaced = directory.path().join("hel (deleted)");
        let proc_exe = directory.path().join("proc-exe");
        let cached = directory.path().join("workers/running/hel-1");
        std::fs::write(&proc_exe, b"running executable").unwrap();

        assert_eq!(
            materialize_running_executable(&replaced, &proc_exe, &cached).unwrap(),
            cached
        );
        assert_eq!(std::fs::read(&cached).unwrap(), b"running executable");
        assert_eq!(
            std::fs::metadata(&cached).unwrap().permissions().mode() & 0o777,
            0o700
        );
    }

    #[test]
    fn failed_new_session_provisioning_discards_provisional_record() {
        let session_id = "0123456789abcdef0123456789abcdef";
        let record = SessionRecord {
            id: session_id.into(),
            title: "new session".into(),
            harness_kind: crate::hel_config::HarnessKind::Codex,
            last_profile: "codex".into(),
            bundle_id: "project".into(),
            project_directory: None,
            managed_worktree: None,
            target_template_id: "podman".into(),
            resource_allocation: None,
            additional_mounts: Vec::new(),
            state: SessionState::Provisioning,
            target: None,
            native_session_id: None,
            acp_session_title: None,
            session_title_override: None,
            created_at: "2026-08-12T00:00:00Z".into(),
            updated_at: "2026-08-12T00:00:00Z".into(),
            detached_after_event_ordinal: 0,
            draft_input: String::new(),
            last_error: None,
            last_checkpoint_error: None,
            checkpoint: None,
        };
        let mut state = HelState::default();
        state.sessions.insert(session_id.into(), record);

        let result = apply_new_session_provisioning_result(
            &mut state,
            session_id,
            Err(anyhow::anyhow!("container creation failed")),
        );

        assert!(result.is_err());
        assert!(!state.sessions.contains_key(session_id));
    }

    #[test]
    fn checkpoint_persistence_rollback_restores_memory_and_reports_both_failures() {
        let session_id = "0123456789abcdef0123456789abcdef";
        let previous = checkpoint_test_session(session_id);
        let mut changed = previous.clone();
        changed.state = SessionState::Closing;
        changed.last_checkpoint_error = Some("partially installed checkpoint".into());
        let mut state = HelState::default();
        state.sessions.insert(session_id.into(), changed);

        let error = restore_session_after_persistence_failure(
            &mut state,
            session_id,
            &previous,
            anyhow::anyhow!("verified checkpoint persistence failed"),
            |record| {
                assert_eq!(record, &previous);
                Err(anyhow::anyhow!("rollback database write failed"))
            },
        );

        assert_eq!(state.sessions.get(session_id), Some(&previous));
        let detail = format!("{error:#}");
        assert!(detail.contains("verified checkpoint persistence failed"));
        assert!(detail.contains("rollback database write failed"));
    }

    #[test]
    fn starting_close_persists_its_intent_before_checkpointing() {
        let mut session = checkpoint_test_session("0123456789abcdef0123456789abcdef");
        session.state = SessionState::Running;
        session.last_checkpoint_error = Some("old failure".into());

        apply_close_checkpoint_started(&mut session, "2026-08-14T12:00:00Z".into());

        assert_eq!(session.state, SessionState::Closing);
        assert_eq!(session.updated_at, "2026-08-14T12:00:00Z");
        assert!(session.last_checkpoint_error.is_none());
    }

    /// A worker that died leaves an exit record behind. Starting a new worker
    /// must clear it first, or the startup connect loop reads the previous
    /// death as this worker's and gives up on a healthy daemon.
    #[test]
    fn starting_a_worker_clears_the_previous_exit_record_before_launching() {
        struct RecordingExecutor {
            commands: RefCell<Vec<CommandSpec>>,
        }

        impl CommandExecutor for RecordingExecutor {
            fn execute(&self, command: &CommandSpec) -> Result<CommandOutput> {
                self.commands.borrow_mut().push(command.clone());
                Ok(CommandOutput {
                    status: 0,
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                })
            }
        }

        for locator in [
            hel_targets::TargetLocator::LocalBare {
                worker_root: "/worker/root".into(),
            },
            hel_targets::TargetLocator::LocalPodman {
                container_id: "container-1".into(),
            },
        ] {
            let executor = RecordingExecutor {
                commands: RefCell::new(Vec::new()),
            };
            start_worker(&executor, &locator, "/worker/root").unwrap();

            let commands = executor.commands.borrow();
            let script = commands
                .iter()
                .flat_map(|command| command.args.iter())
                .find(|argument| argument.contains("worker-exit.json"))
                .unwrap_or_else(|| {
                    panic!("no launch script cleared the exit record: {commands:?}")
                });
            let cleared = script.find("rm -f").expect("the exit record is removed");
            let launched = script.find("worker").expect("the daemon is launched");
            assert!(
                cleared < launched,
                "the exit record must be cleared before the daemon starts: {script}"
            );
        }
    }

    #[test]
    fn target_cleanup_persists_destroying_and_rechecks_the_installed_archive() {
        struct RecordingExecutor {
            commands: RefCell<Vec<CommandSpec>>,
        }

        impl CommandExecutor for RecordingExecutor {
            fn execute(&self, command: &CommandSpec) -> Result<CommandOutput> {
                self.commands.borrow_mut().push(command.clone());
                Ok(CommandOutput {
                    status: 0,
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                })
            }
        }

        let directory = tempfile::tempdir().unwrap();
        let session_id = "0123456789abcdef0123456789abcdef";
        let checkpoint = write_checkpoint_gate_archive(directory.path(), session_id, 7);
        let mut session = checkpoint_test_session(session_id);
        session.target_template_id = "local".into();
        session.state = SessionState::Closing;
        session.target = Some(TargetLocator::LocalBare {
            worker_root: directory.path().join(session_id),
        });
        session.checkpoint = Some(checkpoint.clone());
        let mut config = HelConfig::default();
        config
            .targets
            .insert("local".into(), TargetTemplate::LocalBare);
        let mut controller = Controller {
            config,
            state: HelState {
                sessions: BTreeMap::from([(session_id.into(), session)]),
                ..HelState::default()
            },
        };
        let executor = RecordingExecutor {
            commands: RefCell::new(Vec::new()),
        };
        let persisted = RefCell::new(Vec::new());

        controller
            .destroy_after_verified_checkpoint_with(session_id, &checkpoint, &executor, |record| {
                persisted.borrow_mut().push(record.state);
                Ok(())
            })
            .unwrap();

        assert_eq!(
            persisted.into_inner(),
            vec![SessionState::Destroying, SessionState::Archived]
        );
        assert_eq!(executor.commands.borrow().len(), 1);
        let archived = &controller.state.sessions[session_id];
        assert_eq!(archived.state, SessionState::Archived);
        assert!(archived.target.is_none());
    }

    #[test]
    fn destroying_retry_blocks_cleanup_when_the_archive_gate_changed() {
        struct RecordingExecutor {
            calls: RefCell<usize>,
        }

        impl CommandExecutor for RecordingExecutor {
            fn execute(&self, _command: &CommandSpec) -> Result<CommandOutput> {
                *self.calls.borrow_mut() += 1;
                Ok(CommandOutput {
                    status: 0,
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                })
            }
        }

        let directory = tempfile::tempdir().unwrap();
        let session_id = "0123456789abcdef0123456789abcdef";
        let mut checkpoint = write_checkpoint_gate_archive(directory.path(), session_id, 7);
        checkpoint.event_frontier = 8;
        let mut session = checkpoint_test_session(session_id);
        session.target_template_id = "local".into();
        session.state = SessionState::Destroying;
        session.target = Some(TargetLocator::LocalBare {
            worker_root: directory.path().join(session_id),
        });
        session.checkpoint = Some(checkpoint.clone());
        let mut config = HelConfig::default();
        config
            .targets
            .insert("local".into(), TargetTemplate::LocalBare);
        let mut controller = Controller {
            config,
            state: HelState {
                sessions: BTreeMap::from([(session_id.into(), session)]),
                ..HelState::default()
            },
        };
        let executor = RecordingExecutor {
            calls: RefCell::new(0),
        };
        let persisted = RefCell::new(Vec::new());

        let error = controller
            .destroy_after_verified_checkpoint_with(session_id, &checkpoint, &executor, |record| {
                persisted.borrow_mut().push(record.state);
                Ok(())
            })
            .unwrap_err();

        assert!(error.to_string().contains("checkpoint frontier changed"));
        assert_eq!(*executor.calls.borrow(), 0);
        assert!(persisted.into_inner().is_empty());
        assert_eq!(
            controller.state.sessions[session_id].state,
            SessionState::Destroying
        );
    }

    #[test]
    fn destroying_retry_finalizes_when_apple_container_is_confirmed_absent() {
        struct AlreadyRemovedExecutor {
            commands: RefCell<Vec<CommandSpec>>,
        }

        impl CommandExecutor for AlreadyRemovedExecutor {
            fn execute(&self, command: &CommandSpec) -> Result<CommandOutput> {
                self.commands.borrow_mut().push(command.clone());
                if command
                    .args
                    .first()
                    .is_some_and(|argument| argument == "rm")
                {
                    Ok(CommandOutput {
                        status: 1,
                        stdout: Vec::new(),
                        stderr: b"container not found".to_vec(),
                    })
                } else {
                    Ok(CommandOutput {
                        status: 0,
                        stdout: Vec::new(),
                        stderr: Vec::new(),
                    })
                }
            }
        }

        let directory = tempfile::tempdir().unwrap();
        let session_id = "0123456789abcdef0123456789abcdef";
        let checkpoint = write_checkpoint_gate_archive(directory.path(), session_id, 7);
        let mut session = checkpoint_test_session(session_id);
        session.target_template_id = "apple".into();
        session.state = SessionState::Destroying;
        session.target = Some(TargetLocator::AppleContainer {
            container_id: hel_targets::resource_name(session_id).unwrap(),
        });
        session.checkpoint = Some(checkpoint.clone());
        let mut config = HelConfig::default();
        config.targets.insert(
            "apple".into(),
            TargetTemplate::AppleContainer {
                container: ConfigContainer {
                    image: "test:latest".into(),
                    platform: None,
                    cpus: None,
                    memory: None,
                    environment: BTreeMap::new(),
                },
            },
        );
        let mut controller = Controller {
            config,
            state: HelState {
                sessions: BTreeMap::from([(session_id.into(), session)]),
                ..HelState::default()
            },
        };
        let executor = AlreadyRemovedExecutor {
            commands: RefCell::new(Vec::new()),
        };
        let persisted = RefCell::new(Vec::new());

        controller
            .destroy_after_verified_checkpoint_with(session_id, &checkpoint, &executor, |record| {
                persisted.borrow_mut().push(record.state);
                Ok(())
            })
            .unwrap();

        let commands = executor.commands.borrow();
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0].args[0], "rm");
        assert_eq!(commands[1].args, ["list", "--all", "--quiet"]);
        assert_eq!(persisted.into_inner(), vec![SessionState::Archived]);
        assert_eq!(
            controller.state.sessions[session_id].state,
            SessionState::Archived
        );
    }

    #[test]
    fn installed_checkpoint_gate_reopens_and_checks_sha_session_and_frontier() {
        let directory = tempfile::tempdir().unwrap();
        let session_id = "0123456789abcdef0123456789abcdef";
        let checkpoint = write_checkpoint_gate_archive(directory.path(), session_id, 7);
        verify_installed_checkpoint_gate(session_id, &checkpoint).unwrap();

        let mut wrong_sha = checkpoint.clone();
        wrong_sha.sha256 = "b".repeat(64);
        assert!(
            verify_installed_checkpoint_gate(session_id, &wrong_sha)
                .unwrap_err()
                .to_string()
                .contains("SHA changed")
        );
        assert!(
            verify_installed_checkpoint_gate("1123456789abcdef0123456789abcdef", &checkpoint)
                .unwrap_err()
                .to_string()
                .contains("belongs to session")
        );
        let mut wrong_frontier = checkpoint.clone();
        wrong_frontier.event_frontier += 1;
        assert!(
            verify_installed_checkpoint_gate(session_id, &wrong_frontier)
                .unwrap_err()
                .to_string()
                .contains("frontier changed")
        );

        std::fs::write(
            &checkpoint.archive_path,
            b"changed after first verification",
        )
        .unwrap();
        assert!(
            format!(
                "{:#}",
                verify_installed_checkpoint_gate(session_id, &checkpoint).unwrap_err()
            )
            .contains("re-open installed checkpoint")
        );
    }

    #[test]
    fn interrupted_close_error_preserves_destroying_phase() {
        let session_id = "0123456789abcdef0123456789abcdef";
        let mut session = checkpoint_test_session(session_id);
        session.state = SessionState::Destroying;

        apply_interrupted_close_error(
            &mut session,
            &anyhow::anyhow!("podman unavailable"),
            "2026-08-14T12:00:00Z",
        );

        assert_eq!(session.state, SessionState::Destroying);
        assert_eq!(session.updated_at, "2026-08-14T12:00:00Z");
        assert!(
            session
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("cleanup is safely retryable"))
        );
    }

    #[test]
    fn failed_new_worker_start_discards_session_only_after_target_cleanup() {
        let session_id = "0123456789abcdef0123456789abcdef";
        let mut session = SessionRecord {
            id: session_id.into(),
            title: "new session".into(),
            harness_kind: crate::hel_config::HarnessKind::Kimi,
            last_profile: "kimi".into(),
            bundle_id: "raw-project".into(),
            project_directory: Some("/srv/project".into()),
            managed_worktree: None,
            target_template_id: "remote".into(),
            resource_allocation: None,
            additional_mounts: Vec::new(),
            state: SessionState::Disconnected,
            target: Some(TargetLocator::SshBare {
                host: "builder".into(),
                workspace: format!(".local/share/hel/workspaces/{session_id}").into(),
                worker_id: None,
            }),
            native_session_id: None,
            acp_session_title: None,
            session_title_override: None,
            created_at: "2026-08-12T00:00:00Z".into(),
            updated_at: "2026-08-12T00:00:00Z".into(),
            detached_after_event_ordinal: 0,
            draft_input: String::new(),
            last_error: None,
            last_checkpoint_error: None,
            checkpoint: None,
        };
        let mut cleaned = HelState::default();
        cleaned.sessions.insert(session_id.into(), session.clone());

        let failure =
            apply_failed_new_session_rollback(&mut cleaned, session_id, "ACP startup failed", None);

        assert!(!cleaned.sessions.contains_key(session_id));
        assert!(
            failure
                .to_string()
                .contains("provisional session discarded")
        );

        session.state = SessionState::Disconnected;
        let mut cleanup_failed = HelState::default();
        cleanup_failed.sessions.insert(session_id.into(), session);
        let failure = apply_failed_new_session_rollback(
            &mut cleanup_failed,
            session_id,
            "ACP startup failed",
            Some("ssh unavailable".into()),
        );
        let retained = cleanup_failed.sessions.get(session_id).unwrap();
        assert_eq!(retained.state, SessionState::Error);
        assert!(retained.target.is_some());
        assert!(failure.to_string().contains("cleanup"));
    }

    #[test]
    fn launch_failure_is_persisted_separately_from_session_state() {
        let directory = tempfile::tempdir().unwrap();
        let session_id = "0123456789abcdef0123456789abcdef";
        let detail = format!(
            "specific startup cause\n{}\nstderr tail survives",
            "x".repeat(MAX_LAUNCH_DIAGNOSTIC_BYTES)
        );

        let path = persist_launch_failure_to(directory.path(), session_id, &detail).unwrap();
        let saved = std::fs::read_to_string(path).unwrap();

        assert!(saved.contains("specific startup cause"));
        assert!(saved.contains("launch diagnostic truncated"));
        assert!(saved.contains("stderr tail survives"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(directory.path())
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
        }
    }

    #[test]
    fn failed_resume_rolls_back_only_after_target_cleanup() {
        let previous = SessionRecord {
            id: "0123456789abcdef0123456789abcdef".into(),
            title: "imported session".into(),
            harness_kind: crate::hel_config::HarnessKind::Codex,
            last_profile: "codex-old".into(),
            bundle_id: "project".into(),
            project_directory: None,
            managed_worktree: None,
            target_template_id: "podman-old".into(),
            resource_allocation: None,
            additional_mounts: Vec::new(),
            state: SessionState::Archived,
            target: None,
            native_session_id: Some("native-session".into()),
            acp_session_title: None,
            session_title_override: None,
            created_at: "2026-08-12T00:00:00Z".into(),
            updated_at: "2026-08-12T00:00:00Z".into(),
            detached_after_event_ordinal: 0,
            draft_input: String::new(),
            last_error: None,
            last_checkpoint_error: None,
            checkpoint: None,
        };
        let partial_target = TargetLocator::LocalPodman {
            container_id: "partial-container".into(),
        };
        let mut cleaned = previous.clone();
        cleaned.state = SessionState::Error;
        cleaned.last_profile = "codex-new".into();
        cleaned.target = Some(partial_target.clone());

        let failure =
            apply_failed_resume_rollback(&mut cleaned, &previous, "worker upload failed", None);

        assert_eq!(cleaned.state, SessionState::Archived);
        assert_eq!(cleaned.last_profile, "codex-old");
        assert_eq!(cleaned.target, None);
        assert!(failure.to_string().contains("returned to archived"));

        let mut cleanup_failed = previous.clone();
        cleanup_failed.state = SessionState::Error;
        cleanup_failed.last_profile = "codex-new".into();
        cleanup_failed.target = Some(partial_target.clone());

        let failure = apply_failed_resume_rollback(
            &mut cleanup_failed,
            &previous,
            "worker upload failed",
            Some("podman rm failed".into()),
        );

        assert_eq!(cleanup_failed.state, SessionState::Error);
        assert_eq!(cleanup_failed.last_profile, "codex-new");
        assert_eq!(cleanup_failed.target, Some(partial_target));
        assert!(failure.to_string().contains("cleanup"));
    }

    #[test]
    fn failed_resume_provisioning_preserves_checkpoint_and_projection_lineage() {
        // HEL_DATA_DIR is process-global, so run the database-backed half in an
        // exact child test instead of racing unrelated tests in this process.
        if std::env::var_os(RESUME_ROLLBACK_TEST_CHILD).is_none() {
            let directory = tempfile::tempdir().unwrap();
            let test_name = format!(
                "{}::failed_resume_provisioning_preserves_checkpoint_and_projection_lineage",
                module_path!()
                    .strip_prefix("hel::")
                    .unwrap_or(module_path!())
            );
            let output = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", &test_name, "--nocapture"])
                .env(RESUME_ROLLBACK_TEST_CHILD, "1")
                .env("HEL_DATA_DIR", directory.path())
                .env("GH_TOKEN", "test-token")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "isolated resume rollback test failed\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }

        struct FailingPreflightExecutor;

        impl CommandExecutor for FailingPreflightExecutor {
            fn execute(&self, command: &CommandSpec) -> Result<CommandOutput> {
                assert_eq!(command.program, "podman");
                Ok(CommandOutput {
                    status: 1,
                    stdout: Vec::new(),
                    stderr: b"podman is temporarily unavailable".to_vec(),
                })
            }
        }

        let data_directory = PathBuf::from(std::env::var_os("HEL_DATA_DIR").unwrap());
        let archive_directory = data_directory.join("archives");
        std::fs::create_dir_all(&archive_directory).unwrap();
        let session_id = "0123456789abcdef0123456789abcdef";
        let checkpoint = write_checkpoint_gate_archive(&archive_directory, session_id, 7);
        let archive = verify_archive_streaming(&checkpoint.archive_path).unwrap();
        let expected_projection =
            materialized_session_from_canonical(session_id, &archive.canonical_session).unwrap();

        let mut session = checkpoint_test_session(session_id);
        session.state = SessionState::Archived;
        session.checkpoint = Some(checkpoint.clone());
        let previous = session.clone();
        let profile_home = data_directory.join("profile");
        std::fs::create_dir_all(&profile_home).unwrap();
        let mut config = HelConfig::default();
        config.profiles.insert(
            "codex".into(),
            HarnessProfile {
                kind: crate::hel_config::HarnessKind::Codex,
                home: profile_home,
                executable: None,
                environment: BTreeMap::new(),
                context_window_bytes: None,
            },
        );
        config.bundles.insert(
            "project".into(),
            ProjectBundle {
                primary_repo: "project".into(),
                repositories: vec![ProjectRepository {
                    id: "project".into(),
                    github: Some("example/project".into()),
                    local: None,
                    destination: "project".into(),
                    git_ref: None,
                }],
            },
        );
        config.targets.insert(
            "podman".into(),
            TargetTemplate::LocalPodman {
                container: ConfigContainer {
                    image: "example.invalid/hel-test:latest".into(),
                    platform: None,
                    cpus: None,
                    memory: None,
                    environment: BTreeMap::new(),
                },
            },
        );
        let mut controller = Controller {
            config,
            state: HelState {
                sessions: BTreeMap::from([(session_id.into(), session)]),
                ..HelState::default()
            },
        };
        crate::hel_database::save_state(&controller.state).unwrap();
        crate::hel_database::save_materialized_session(&expected_projection).unwrap();

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let error = runtime
            .block_on(controller.resume_session_controlled(
                session_id,
                "codex",
                "podman",
                SessionResumeOptions {
                    additional_mounts: None,
                    resource_allocation: None,
                    discard_queue: false,
                },
                &FailingPreflightExecutor,
            ))
            .unwrap_err();
        let detail = format!("{error:#}");
        assert!(detail.contains("returned to archived"), "{detail}");
        assert!(!detail.contains("unknown session"), "{detail}");

        let retained = controller.state.sessions.get(session_id).unwrap();
        assert_eq!(retained.state, SessionState::Archived);
        assert_eq!(retained.checkpoint, previous.checkpoint);
        assert_eq!(retained.managed_worktree, previous.managed_worktree);
        assert!(checkpoint.archive_path.is_file());

        let durable = crate::hel_database::load_state().unwrap();
        let durable_session = durable.sessions.get(session_id).unwrap();
        assert_eq!(durable_session.state, SessionState::Archived);
        assert_eq!(durable_session.checkpoint, previous.checkpoint);
        assert_eq!(
            crate::hel_database::load_materialized_session(session_id).unwrap(),
            Some(expected_projection)
        );
    }

    #[test]
    fn aws_resources_are_compressed_into_one_streamed_ssh_command() {
        struct RecordingExecutor {
            commands: RefCell<Vec<CommandSpec>>,
            streams: RefCell<Vec<Vec<u8>>>,
        }
        impl CommandExecutor for RecordingExecutor {
            fn execute(&self, command: &CommandSpec) -> Result<CommandOutput> {
                self.commands.borrow_mut().push(command.clone());
                Ok(CommandOutput {
                    status: 0,
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                })
            }

            fn execute_with_stdin(
                &self,
                command: &CommandSpec,
                input: &mut (dyn std::io::Read + Send),
            ) -> Result<CommandOutput> {
                self.commands.borrow_mut().push(command.clone());
                let mut stream = Vec::new();
                input.read_to_end(&mut stream)?;
                self.streams.borrow_mut().push(stream);
                Ok(CommandOutput {
                    status: 0,
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                })
            }
        }

        let source = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(source.path().join("many/files")).unwrap();
        std::fs::write(source.path().join("many/files/one"), b"one").unwrap();
        std::fs::write(source.path().join("many/files/two"), b"two").unwrap();
        let session_id = "0123456789abcdef0123456789abcdef";
        let record = SessionRecord {
            id: session_id.into(),
            title: "AWS resources".into(),
            harness_kind: crate::hel_config::HarnessKind::Codex,
            last_profile: "codex".into(),
            bundle_id: "project".into(),
            project_directory: None,
            managed_worktree: None,
            target_template_id: "aws".into(),
            resource_allocation: None,
            additional_mounts: vec![AdditionalMount {
                source: source.path().to_path_buf(),
                destination: "/home/ubuntu/hel-resources/data".into(),
            }],
            state: SessionState::Disconnected,
            target: None,
            native_session_id: None,
            acp_session_title: None,
            session_title_override: None,
            created_at: "2026-08-12T00:00:00Z".into(),
            updated_at: "2026-08-12T00:00:00Z".into(),
            detached_after_event_ordinal: 0,
            draft_input: String::new(),
            last_error: None,
            last_checkpoint_error: None,
            checkpoint: None,
        };
        let state = HelState {
            version: crate::hel_state::STATE_VERSION,
            sessions: BTreeMap::from([(session_id.into(), record)]),
            mount_history: BTreeMap::new(),
        };
        let backend = hel_targets::TargetLocator::AwsEc2 {
            profile: "default".into(),
            region: "us-east-1".into(),
            instance_id: "i-1234567890abcdef0".into(),
            ssh: SshTarget {
                destination: "ubuntu@example.test".into(),
                ssh_args: Vec::new(),
            },
            workspace: format!(".local/share/hel/workspaces/{session_id}"),
        };
        let executor = RecordingExecutor {
            commands: RefCell::new(Vec::new()),
            streams: RefCell::new(Vec::new()),
        };

        install_attached_resources(
            &state,
            session_id,
            &backend,
            ".local/share/hel/workers/session",
            &executor,
        )
        .unwrap();

        let commands = executor.commands.borrow();
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].program, "ssh");
        assert!(
            commands[0]
                .args
                .iter()
                .any(|argument| argument.contains("install-resource"))
        );
        let streams = executor.streams.borrow();
        assert_eq!(streams.len(), 1);
        assert_eq!(&streams[0][..2], &[0x1f, 0x8b]);
    }

    struct PodmanInstallExecutor {
        commands: RefCell<Vec<CommandSpec>>,
        worker_cached: bool,
    }

    impl CommandExecutor for PodmanInstallExecutor {
        fn execute(&self, command: &CommandSpec) -> Result<CommandOutput> {
            self.commands.borrow_mut().push(command.clone());
            let probing_cache = command
                .args
                .iter()
                .any(|argument| argument.contains("'test' '-f'"));
            let status = if probing_cache && !self.worker_cached {
                1
            } else {
                0
            };
            Ok(CommandOutput {
                status,
                stdout: Vec::new(),
                stderr: Vec::new(),
            })
        }
    }

    struct PodmanInstallFixture {
        _root: tempfile::TempDir,
        worker_binary: PathBuf,
        launch_config: PathBuf,
        ownership: PathBuf,
        profile_stage: PathBuf,
        locator: hel_targets::TargetLocator,
        digest: String,
    }

    fn podman_install_fixture() -> PodmanInstallFixture {
        let root = tempfile::tempdir().unwrap();
        let worker_binary = root.path().join("hel");
        std::fs::write(&worker_binary, b"worker-binary-bytes").unwrap();
        let launch_config = root.path().join("launch.json");
        std::fs::write(&launch_config, b"{}").unwrap();
        let ownership = root.path().join("ownership.json");
        std::fs::write(&ownership, b"{}").unwrap();
        let profile_stage = root.path().join("profile");
        std::fs::create_dir_all(&profile_stage).unwrap();
        let digest = format!("{:x}", Sha256::digest(b"worker-binary-bytes"));
        PodmanInstallFixture {
            _root: root,
            worker_binary,
            launch_config,
            ownership,
            profile_stage,
            locator: hel_targets::TargetLocator::SshPodman {
                ssh: SshTarget {
                    destination: "user@example.test".into(),
                    ssh_args: Vec::new(),
                },
                container_id: "container-1".into(),
            },
            digest,
        }
    }

    fn run_podman_install(worker_cached: bool) -> (Vec<CommandSpec>, PodmanInstallFixture) {
        let fixture = podman_install_fixture();
        let executor = PodmanInstallExecutor {
            commands: RefCell::new(Vec::new()),
            worker_cached,
        };
        install_worker_files(
            &executor,
            &fixture.locator,
            "0123456789abcdef0123456789abcdef",
            "/workspace/.hel/worker",
            "/workspace/.hel/profile",
            &fixture.worker_binary,
            &fixture.launch_config,
            &fixture.ownership,
            &fixture.profile_stage,
        )
        .unwrap();
        let commands = executor.commands.borrow().clone();
        (commands, fixture)
    }

    fn rendered(commands: &[CommandSpec]) -> Vec<String> {
        commands
            .iter()
            .map(|command| format!("{} {}", command.program, command.args.join(" ")))
            .collect()
    }

    #[test]
    fn ssh_podman_install_caches_the_worker_binary_on_a_cache_miss() {
        let (commands, fixture) = run_podman_install(false);
        let lines = rendered(&commands);
        let digest = &fixture.digest;
        let cache_dir = format!(".cache/hel/workers/{digest}");
        let session = "0123456789abcdef0123456789abcdef";

        assert!(
            lines
                .iter()
                .any(|line| line.starts_with("ssh") && line.contains("'test' '-f'")),
            "expected a cache probe, got {lines:#?}"
        );
        assert!(
            !lines.iter().any(|line| line.contains('~')),
            "remote staging paths must be home-relative: ssh arguments are \
             single-quoted so a tilde stays literal in the remote shell while \
             scp expands it, got {lines:#?}"
        );
        assert!(
            lines.iter().any(|line| line.starts_with("ssh")
                && line.contains(&format!("'mkdir' '-p' '{cache_dir}'"))),
            "expected the cache directory to be created, got {lines:#?}"
        );
        let partial = format!("{cache_dir}/hel.partial-{session}");
        assert!(
            lines.iter().any(|line| line
                == &format!(
                    "scp {} user@example.test:{partial}",
                    fixture.worker_binary.display()
                )),
            "expected the worker to be uploaded to the partial cache path, got {lines:#?}"
        );
        assert!(
            lines.iter().any(|line| line.starts_with("ssh")
                && line.contains(&format!("'mv' '{partial}' '{cache_dir}/hel'"))),
            "expected an atomic rename into the cache, got {lines:#?}"
        );
        assert!(
            lines.iter().any(|line| line.contains("'podman' 'cp'")
                && line.contains(&format!("'{cache_dir}/hel'"))),
            "expected podman cp to read the cached worker, got {lines:#?}"
        );
        assert!(
            !lines.iter().any(|line| line.starts_with("scp")
                && line.ends_with(&format!(
                    "user@example.test:.cache/hel/uploads/{session}/hel"
                ))),
            "the worker must not be staged in the per-session upload directory, got {lines:#?}"
        );
    }

    #[test]
    fn ssh_podman_install_skips_the_worker_upload_on_a_cache_hit() {
        let (commands, fixture) = run_podman_install(true);
        let lines = rendered(&commands);
        let digest = &fixture.digest;
        let cache_dir = format!(".cache/hel/workers/{digest}");
        let session = "0123456789abcdef0123456789abcdef";

        assert!(
            !lines.iter().any(|line| line.starts_with("scp")
                && line.contains(&fixture.worker_binary.display().to_string())),
            "a cached worker must not be re-uploaded, got {lines:#?}"
        );
        assert!(
            !lines.iter().any(|line| line.contains("'mv'")),
            "a cache hit must not rename anything, got {lines:#?}"
        );
        assert!(
            lines.iter().any(|line| line.contains("'podman' 'cp'")
                && line.contains(&format!("'{cache_dir}/hel'"))),
            "expected podman cp to read the cached worker, got {lines:#?}"
        );
        for name in ["launch.json", "ownership.json"] {
            assert!(
                lines.iter().any(|line| line.starts_with("scp")
                    && line.ends_with(&format!(
                        "user@example.test:.cache/hel/uploads/{session}/{name}"
                    ))),
                "expected {name} to still be uploaded per session, got {lines:#?}"
            );
        }
    }

    #[test]
    fn default_bridges_pin_command_capable_adapter_versions() {
        let (_, codex_arguments) = bridge_launch(crate::hel_config::HarnessKind::Codex, None);
        assert!(codex_arguments[1].contains("@agentclientprotocol/codex-acp@1.1.14"));

        let (_, claude_arguments) = bridge_launch(crate::hel_config::HarnessKind::Claude, None);
        assert!(claude_arguments[1].contains("@agentclientprotocol/claude-agent-acp@0.68.0"));
    }

    #[test]
    fn bridge_fallback_pins_match_the_agent_dev_containerfile() {
        const CONTAINERFILE: &str = include_str!("../containers/Containerfile.agent-dev");

        let codex = format!("codex-acp@{CODEX_ACP_FALLBACK_VERSION}");
        assert!(
            CONTAINERFILE.contains(&codex),
            "containers/Containerfile.agent-dev must install {codex}. The image and the \
             bridge_launch() npx fallbacks have to stay in lockstep, otherwise a container \
             session and an npx session run different adapter versions."
        );

        let claude = format!("claude-agent-acp@{CLAUDE_AGENT_ACP_FALLBACK_VERSION}");
        assert!(
            CONTAINERFILE.contains(&claude),
            "containers/Containerfile.agent-dev must install {claude}. The image and the \
             bridge_launch() npx fallbacks have to stay in lockstep, otherwise a container \
             session and an npx session run different adapter versions."
        );
    }

    #[test]
    fn kimi_default_bridge_uses_bash_for_the_official_installer() {
        let (command, arguments) = bridge_launch(crate::hel_config::HarnessKind::Kimi, None);
        assert_eq!(command, "sh");
        assert_eq!(arguments[0], "-lc");
        assert!(arguments[1].contains("install.sh | bash &&"));
        assert!(arguments[1].contains("$HOME/.kimi-code/bin/kimi"));
    }

    #[test]
    fn stage_claude_profile_preserves_rollout_identity() {
        let home = tempfile::tempdir().unwrap();
        let identity = r#"{
            "machineID": "stable-machine",
            "userID": "stable-user",
            "cachedGrowthBookFeatures": {
                "tengu_velvet_mallet_fable_5": true
            }
        }"#;
        std::fs::write(home.path().join(".claude.json"), identity).unwrap();
        let staged = tempfile::tempdir().unwrap();
        let profile = crate::hel_config::HarnessProfile {
            kind: crate::hel_config::HarnessKind::Claude,
            home: home.path().to_path_buf(),
            executable: None,
            environment: BTreeMap::new(),
            context_window_bytes: None,
        };

        stage_profile(&profile, staged.path()).unwrap();

        assert_eq!(
            std::fs::read_to_string(staged.path().join(".claude.json")).unwrap(),
            identity
        );
    }

    #[test]
    fn stage_kimi_profile_preserves_device_identity() {
        let home = tempfile::tempdir().unwrap();
        std::fs::write(home.path().join("config.toml"), "default_model = \"k3\"\n").unwrap();
        std::fs::write(home.path().join("device_id"), "stable-device-id").unwrap();
        std::fs::create_dir(home.path().join("credentials")).unwrap();
        std::fs::write(
            home.path().join("credentials/kimi-code.json"),
            "{\"access_token\":\"secret\"}",
        )
        .unwrap();
        let staged = tempfile::tempdir().unwrap();
        let profile = crate::hel_config::HarnessProfile {
            kind: crate::hel_config::HarnessKind::Kimi,
            home: home.path().to_path_buf(),
            executable: None,
            environment: BTreeMap::new(),
            context_window_bytes: None,
        };

        stage_profile(&profile, staged.path()).unwrap();

        assert_eq!(
            std::fs::read_to_string(staged.path().join("device_id")).unwrap(),
            "stable-device-id"
        );
        assert!(staged.path().join("credentials/kimi-code.json").is_file());
    }

    #[test]
    fn stage_profile_appends_container_environment_for_each_harness_without_touching_home() {
        for (kind, instructions) in [
            (crate::hel_config::HarnessKind::Codex, "AGENTS.md"),
            (crate::hel_config::HarnessKind::Claude, "CLAUDE.md"),
            (crate::hel_config::HarnessKind::Kimi, "SYSTEM.md"),
        ] {
            let home = tempfile::tempdir().unwrap();
            let original = "# Controller instructions\n\nKeep this source unchanged.\n";
            let source_instructions = home.path().join(instructions);
            std::fs::write(&source_instructions, original).unwrap();
            let staged = tempfile::tempdir().unwrap();
            let profile = crate::hel_config::HarnessProfile {
                kind,
                home: home.path().to_path_buf(),
                executable: None,
                environment: std::collections::BTreeMap::new(),
                context_window_bytes: None,
            };

            stage_profile(&profile, staged.path()).unwrap();

            assert_eq!(
                std::fs::read_to_string(staged.path().join(instructions)).unwrap(),
                format!("{original}\n{HEL_CONTAINER_ENVIRONMENT}"),
                "{instructions} receives the section in the staged profile"
            );
            assert_eq!(
                std::fs::read_to_string(source_instructions).unwrap(),
                original,
                "{instructions} in the controller-side home stays untouched"
            );
        }
    }

    #[test]
    fn stage_profile_creates_missing_staged_container_instructions() {
        let home = tempfile::tempdir().unwrap();
        let staged = tempfile::tempdir().unwrap();
        let profile = crate::hel_config::HarnessProfile {
            kind: crate::hel_config::HarnessKind::Kimi,
            home: home.path().to_path_buf(),
            executable: None,
            environment: std::collections::BTreeMap::new(),
            context_window_bytes: None,
        };

        stage_profile(&profile, staged.path()).unwrap();

        assert_eq!(
            std::fs::read_to_string(staged.path().join("SYSTEM.md")).unwrap(),
            HEL_CONTAINER_ENVIRONMENT
        );
        assert!(!home.path().join("SYSTEM.md").exists());
    }

    #[test]
    fn inherited_git_settings_allow_only_portable_non_executable_values() {
        let settings = parse_inherited_git_settings(
            b"user.name\nAgent User\0USER.EMAIL\nagent@example.test\0pull.rebase\ntrue\0alias.deploy\n!ship\0credential.helper\nstore\0core.editor\nvim\0include.path\n/host/config\0user.name\nFinal User\0",
        )
        .unwrap();

        assert_eq!(
            settings,
            BTreeMap::from([
                ("pull.rebase".into(), "true".into()),
                ("user.email".into(), "agent@example.test".into()),
                ("user.name".into(), "Final User".into()),
            ])
        );
    }

    #[test]
    fn inherited_git_settings_reject_malformed_or_non_utf8_output() {
        assert!(parse_inherited_git_settings(b"user.name\0").is_err());
        assert!(parse_inherited_git_settings(b"user.name\n\xff\0").is_err());
    }

    #[test]
    fn inherited_git_settings_target_only_isolated_workers() {
        let ssh = SshTarget {
            destination: "worker@example.test".into(),
            ssh_args: vec!["-p".into(), "2222".into()],
        };
        let ephemeral = [
            hel_targets::TargetLocator::LocalPodman {
                container_id: "abcdef012345".into(),
            },
            hel_targets::TargetLocator::AppleContainer {
                container_id: "abcdef012346".into(),
            },
            hel_targets::TargetLocator::AwsEc2 {
                profile: "default".into(),
                region: "us-east-1".into(),
                instance_id: "i-1234567890abcdef0".into(),
                ssh: ssh.clone(),
                workspace: ".local/share/hel/workspaces/018f9dd2-a3b4-7c8d-9000-123456789abc"
                    .into(),
            },
            hel_targets::TargetLocator::SshPodman {
                ssh: ssh.clone(),
                container_id: "abcdef012347".into(),
            },
        ];
        for locator in &ephemeral {
            assert!(inherits_controller_git_settings(locator));
            let commands = inherited_git_setting_commands(
                locator,
                "018f9dd2-a3b4-7c8d-9000-123456789abc",
                BTreeMap::from([("user.name".into(), "- Agent O'Brien 日本語".into())]),
            )
            .unwrap();
            assert_eq!(commands.len(), 1);
            assert!(
                commands[0]
                    .args
                    .iter()
                    .any(|argument| argument.contains("user.name"))
            );
            assert!(
                commands[0]
                    .args
                    .iter()
                    .any(|argument| argument.contains("- Agent O'"))
            );
        }

        let persistent = hel_targets::TargetLocator::SshBare {
            ssh,
            workspace: "/srv/hel/018f9dd2-a3b4-7c8d-9000-123456789abc".into(),
        };
        let local = hel_targets::TargetLocator::LocalBare {
            worker_root: "/var/lib/hel/workers/018f9dd2-a3b4-7c8d-9000-123456789abc".into(),
        };
        assert!(!inherits_controller_git_settings(&persistent));
        assert!(!inherits_controller_git_settings(&local));
        assert!(!force_unrestricted_mode(&local));
        assert!(force_unrestricted_mode(&ephemeral[0]));
        assert!(
            inherited_git_setting_commands(
                &persistent,
                "018f9dd2-a3b4-7c8d-9000-123456789abc",
                BTreeMap::from([("user.name".into(), "Agent".into())]),
            )
            .unwrap()
            .is_empty()
        );
    }

    #[test]
    fn canonical_bundle_maps_github_shorthand_and_primary_destination() {
        let bundle = ProjectBundle {
            primary_repo: "app".into(),
            repositories: vec![ProjectRepository {
                id: "app".into(),
                github: Some("example/app".into()),
                local: None,
                destination: PathBuf::from("services/app"),
                git_ref: Some("main".into()),
            }],
        };
        let backend = backend_bundle(&bundle).unwrap();
        assert_eq!(backend.primary, "services/app");
        assert_eq!(
            backend.repositories[0].url.as_deref(),
            Some("https://github.com/example/app.git")
        );
    }

    #[test]
    fn container_resources_and_environment_become_argv() {
        let template = TargetTemplate::LocalPodman {
            container: ConfigContainer {
                image: "dev:1".into(),
                platform: Some("linux/arm64".into()),
                cpus: Some("4".into()),
                memory: Some("8g".into()),
                environment: std::collections::BTreeMap::from([("A".into(), "b c".into())]),
            },
        };
        let hel_targets::TargetTemplate::LocalPodman(container) =
            backend_target(&template, None).unwrap()
        else {
            unreachable!()
        };
        assert!(container.extra_run_args.contains(&"--cpus=4".into()));
        assert!(container.extra_run_args.contains(&"A=b c".into()));
    }

    #[test]
    fn github_token_is_injected_only_into_managed_containers() {
        let mut podman = hel_targets::TargetTemplate::LocalPodman(ContainerTemplate {
            image: "dev:1".into(),
            extra_run_args: vec![],
        });
        assert!(inject_github_token(&mut podman, "github-token"));
        let hel_targets::TargetTemplate::LocalPodman(container) = podman else {
            unreachable!()
        };
        assert!(
            container
                .extra_run_args
                .windows(2)
                .any(|arguments| arguments == ["--env", "GH_TOKEN=github-token"])
        );

        let mut bare = hel_targets::TargetTemplate::LocalBare;
        assert!(!inject_github_token(&mut bare, "github-token"));
        assert_eq!(bare, hel_targets::TargetTemplate::LocalBare);
        assert_eq!(usable_github_token("  token-value\n"), Some("token-value"));
        assert_eq!(usable_github_token("not a token"), None);

        let mut bundle = hel_targets::ProjectBundleSpec {
            primary: "app".into(),
            repositories: vec![hel_targets::RepositorySpec {
                url: Some("git@github.com:example/app.git".into()),
                destination: "app".into(),
                git_ref: None,
            }],
        };
        use_github_https_urls(&mut bundle);
        assert_eq!(
            bundle.repositories[0].url.as_deref(),
            Some("https://github.com/example/app.git")
        );
    }

    #[test]
    fn aws_resource_options_follow_the_launch_template_family() {
        let mut config = HelConfig::default();
        config.targets.insert(
            "aws".into(),
            TargetTemplate::AwsEc2 {
                aws_profile: None,
                region: "us-east-1".into(),
                launch_template: "hel-runson".into(),
                launch_template_version: None,
                ssh_user: "ubuntu".into(),
                address_source: AwsAddressSource::PublicIp,
                identity_file: None,
                ssh_args: Vec::new(),
            },
        );
        let executor = PreflightExecutor {
            outputs: RefCell::new(vec![
                CommandOutput {
                    status: 0,
                    stdout: br#"{"LaunchTemplateVersions":[{"LaunchTemplateData":{"InstanceType":"m8i-flex.large"}}]}"#.to_vec(),
                    stderr: Vec::new(),
                },
                CommandOutput {
                    status: 0,
                    stdout: br#"{"InstanceTypes":[{"InstanceType":"m8i-flex.4xlarge","VCpuInfo":{"DefaultVCpus":16},"MemoryInfo":{"SizeInMiB":65536}},{"InstanceType":"m8i-flex.2xlarge","VCpuInfo":{"DefaultVCpus":8},"MemoryInfo":{"SizeInMiB":32768}}]}"#.to_vec(),
                    stderr: Vec::new(),
                },
            ]),
        };
        let controller = Controller {
            config,
            state: HelState::default(),
        };

        let options = controller
            .resolve_aws_resource_options("aws", &executor)
            .unwrap();
        assert_eq!(
            options.iter().map(allocation_vcpus).collect::<Vec<_>>(),
            [8, 16]
        );
    }

    #[test]
    fn deployment_capacity_groups_local_and_same_host_targets() {
        let container = || ConfigContainer {
            image: "dev:1".into(),
            platform: None,
            cpus: None,
            memory: None,
            environment: BTreeMap::new(),
        };
        let ssh = |host: &str| SshConnection {
            host: host.into(),
            user: Some("builder".into()),
            identity_file: None,
            extra_args: Vec::new(),
        };
        let config = HelConfig {
            version: crate::hel_config::CONFIG_VERSION,
            profiles: BTreeMap::new(),
            bundles: BTreeMap::new(),
            targets: BTreeMap::from([
                (
                    "apple".into(),
                    TargetTemplate::AppleContainer {
                        container: container(),
                    },
                ),
                (
                    "local".into(),
                    TargetTemplate::LocalPodman {
                        container: container(),
                    },
                ),
                (
                    "bare".into(),
                    TargetTemplate::SshBare {
                        ssh: ssh("builder"),
                        workspace_prefix: ".local/share/hel/workspaces".into(),
                    },
                ),
                (
                    "remote-container".into(),
                    TargetTemplate::SshPodman {
                        ssh: ssh("builder"),
                        container: container(),
                    },
                ),
                (
                    "alias".into(),
                    TargetTemplate::SshBare {
                        ssh: ssh("builder-alias"),
                        workspace_prefix: ".local/share/hel/workspaces".into(),
                    },
                ),
            ]),
        };
        let controller = Controller {
            config,
            state: HelState::default(),
        };

        let targets = controller.deployment_capacity_targets();

        assert_eq!(targets.len(), 3);
        let local = targets.iter().find(|target| target.id == "local").unwrap();
        assert_eq!(local.target_ids, ["apple", "local"]);
        let builder = targets
            .iter()
            .find(|target| target.id == "ssh:builder")
            .unwrap();
        assert_eq!(builder.target_ids, ["bare", "remote-container"]);
        assert_eq!(builder.probes.len(), 1);
        assert!(
            targets
                .iter()
                .any(|target| target.id == "ssh:builder-alias")
        );
    }

    struct PreflightExecutor {
        outputs: RefCell<Vec<CommandOutput>>,
    }

    impl CommandExecutor for PreflightExecutor {
        fn execute(&self, _command: &CommandSpec) -> Result<CommandOutput> {
            Ok(self.outputs.borrow_mut().remove(0))
        }
    }

    #[test]
    fn local_podman_preflight_failures_recommend_doctor() {
        let template = TargetTemplate::LocalPodman {
            container: ConfigContainer {
                image: "ubuntu:24.04".into(),
                platform: None,
                cpus: None,
                memory: None,
                environment: std::collections::BTreeMap::new(),
            },
        };
        let executor = PreflightExecutor {
            outputs: RefCell::new(vec![CommandOutput {
                status: 0,
                stdout: b"podman version 3.4.7\n".to_vec(),
                stderr: vec![],
            }]),
        };

        let error = preflight_target(&template, &executor)
            .unwrap_err()
            .to_string();
        assert!(error.contains("hel doctor"));
        assert!(error.contains("Podman 4.0.0"));
    }

    #[test]
    fn ssh_podman_preflight_failures_name_the_destination_and_recommend_doctor() {
        let template = TargetTemplate::SshPodman {
            ssh: SshConnection {
                host: "example.test".into(),
                user: Some("dev".into()),
                identity_file: None,
                extra_args: vec![],
            },
            container: ConfigContainer {
                image: "ubuntu:24.04".into(),
                platform: None,
                cpus: None,
                memory: None,
                environment: std::collections::BTreeMap::new(),
            },
        };
        let executor = PreflightExecutor {
            outputs: RefCell::new(vec![CommandOutput {
                status: 0,
                stdout: b"podman version 3.4.7\n".to_vec(),
                stderr: vec![],
            }]),
        };

        let error = preflight_target(&template, &executor)
            .unwrap_err()
            .to_string();
        assert!(error.contains("hel doctor"));
        assert!(error.contains("dev@example.test"));
        assert!(error.contains("Podman 4.0.0"));
    }

    #[test]
    fn apple_container_preflight_failures_recommend_doctor() {
        let template = TargetTemplate::AppleContainer {
            container: ConfigContainer {
                image: "ubuntu:24.04".into(),
                platform: None,
                cpus: None,
                memory: None,
                environment: std::collections::BTreeMap::new(),
            },
        };
        let executor = PreflightExecutor {
            outputs: RefCell::new(vec![CommandOutput {
                status: 1,
                stdout: vec![],
                stderr: b"daemon is not running".to_vec(),
            }]),
        };

        let error = preflight_target(&template, &executor)
            .unwrap_err()
            .to_string();
        assert!(error.contains("hel doctor"));
        assert!(error.contains("daemon is not running"));
    }

    #[test]
    fn recovery_container_scan_requires_both_managed_and_session_labels() {
        let template = TargetTemplate::LocalPodman {
            container: ConfigContainer {
                image: "ignored".into(),
                platform: None,
                cpus: None,
                memory: None,
                environment: BTreeMap::new(),
            },
        };
        let json = serde_json::json!([
            {"Labels": {"dev.hel.managed": "true", "dev.hel.session": "0123456789abcdef0123456789abcdef"}},
            {"Labels": {"dev.hel.managed": "false", "dev.hel.session": "not-owned"}},
            {"configuration": {"labels": "dev.hel.managed=true,dev.hel.session=abcdef0123456789abcdef0123456789"}}
        ]);
        let candidates = candidates_from_container_json(
            "local",
            &template,
            serde_json::to_string(&json).unwrap().as_bytes(),
        )
        .unwrap();
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].session_id, "0123456789abcdef0123456789abcdef");
    }

    #[test]
    fn recovery_aws_scan_uses_exact_tagged_instance_and_address() {
        let json = serde_json::json!({"Reservations": [{"Instances": [{
            "InstanceId": "i-exact",
            "PrivateIpAddress": "10.0.0.7",
            "Tags": [
                {"Key": "dev.hel.managed", "Value": "true"},
                {"Key": "dev.hel.session", "Value": "0123456789abcdef0123456789abcdef"}
            ]
        }]}]});
        let candidates = candidates_from_aws_json(
            "aws",
            AwsAddressSource::PrivateIp,
            serde_json::to_string(&json).unwrap().as_bytes(),
        )
        .unwrap();
        assert!(matches!(
            &candidates[0].locator,
            TargetLocator::AwsEc2 { instance_id, address }
                if instance_id == "i-exact" && address.as_deref() == Some("10.0.0.7")
        ));
    }

    fn test_git(directory: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .arg("-C")
            .arg(directory)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }

    fn committed_repository() -> tempfile::TempDir {
        let directory = tempfile::tempdir().unwrap();
        test_git(directory.path(), &["init", "--initial-branch=master"]);
        test_git(directory.path(), &["config", "user.name", "Hel Tests"]);
        test_git(
            directory.path(),
            &["config", "user.email", "hel@example.invalid"],
        );
        std::fs::create_dir(directory.path().join("nested")).unwrap();
        std::fs::write(directory.path().join("nested/file.txt"), "base\n").unwrap();
        test_git(directory.path(), &["add", "."]);
        test_git(directory.path(), &["commit", "-m", "base"]);
        directory
    }

    #[test]
    fn managed_raw_worktree_inherits_upstream_and_cleans_up_owned_artifacts() {
        let repository = committed_repository();
        let remote_parent = tempfile::tempdir().unwrap();
        let remote = remote_parent.path().join("remote.git");
        let output = Command::new("git")
            .args(["init", "--bare"])
            .arg(&remote)
            .output()
            .unwrap();
        assert!(output.status.success());
        test_git(
            repository.path(),
            &["remote", "add", "origin", &remote.to_string_lossy()],
        );
        test_git(
            repository.path(),
            &["push", "--set-upstream", "origin", "master"],
        );

        let target = ManagedWorktreeTarget::Local;
        let inspection =
            inspect_raw_project(&ProcessExecutor, &target, &repository.path().join("nested"))
                .unwrap();
        assert!(inspection.primary_checkout);
        assert_eq!(inspection.upstream.as_deref(), Some("origin/master"));
        // git rev-parse canonicalizes symlinks (macOS tempdirs live behind the
        // /var -> /private/var link), so compare against the canonical path.
        assert_eq!(
            inspection.source_project_directory,
            repository.path().canonicalize().unwrap().join("nested")
        );

        let session_id = "0123456789abcdef0123456789abcdef";
        let worktree = ManagedWorktree {
            source_project_directory: inspection.source_project_directory,
            source_repository: inspection.source_repository,
            worktree_root: repository.path().join(".hel/worktrees").join(session_id),
            branch: format!("hel/{session_id}"),
            target,
        };
        create_managed_worktree(&ProcessExecutor, &worktree, inspection.upstream.as_deref())
            .unwrap();
        assert!(worktree.worktree_root.join("nested/file.txt").is_file());
        assert_eq!(
            test_git(
                &worktree.worktree_root,
                &[
                    "rev-parse",
                    "--abbrev-ref",
                    "--symbolic-full-name",
                    "@{upstream}"
                ]
            ),
            "origin/master"
        );
        assert_eq!(test_git(repository.path(), &["status", "--porcelain"]), "");
        std::fs::write(worktree.worktree_root.join("dirty.txt"), "session\n").unwrap();

        cleanup_managed_worktree(&ProcessExecutor, &worktree).unwrap();
        assert!(!worktree.worktree_root.exists());
        assert!(!repository.path().join(".hel").exists());
        let output = Command::new("git")
            .arg("-C")
            .arg(repository.path())
            .args([
                "show-ref",
                "--verify",
                &format!("refs/heads/{}", worktree.branch),
            ])
            .output()
            .unwrap();
        assert!(!output.status.success());
    }

    #[test]
    fn cancelled_new_session_cleanup_removes_managed_worktree_and_branch() {
        let repository = committed_repository();
        let session_id = "0123456789abcdef0123456789abcdef";
        let worktree = ManagedWorktree {
            source_project_directory: repository.path().to_path_buf(),
            source_repository: repository.path().to_path_buf(),
            worktree_root: repository.path().join(".hel/worktrees").join(session_id),
            branch: format!("hel/{session_id}"),
            target: ManagedWorktreeTarget::Local,
        };
        create_managed_worktree(&ProcessExecutor, &worktree, None).unwrap();

        let mut session = checkpoint_test_session(session_id);
        session.project_directory = Some(worktree.worktree_root.clone());
        session.managed_worktree = Some(worktree.clone());
        let controller = Controller {
            config: HelConfig::default(),
            state: HelState {
                sessions: BTreeMap::from([(session_id.into(), session)]),
                ..HelState::default()
            },
        };
        let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let executor = CancellableProcessExecutor::new(cancelled);

        controller
            .cleanup_new_session_worktree_after_failure(session_id, &executor)
            .unwrap();

        assert!(!worktree.worktree_root.exists());
        assert!(!repository.path().join(".hel").exists());
        let branch = Command::new("git")
            .arg("-C")
            .arg(repository.path())
            .args([
                "show-ref",
                "--verify",
                &format!("refs/heads/{}", worktree.branch),
            ])
            .output()
            .unwrap();
        assert!(!branch.status.success());
    }

    #[test]
    fn managed_raw_worktree_refuses_dirty_primary_and_skips_existing_worktree() {
        let repository = committed_repository();
        std::fs::write(repository.path().join("dirty.txt"), "dirty\n").unwrap();
        let target = ManagedWorktreeTarget::Local;
        let inspection = inspect_raw_project(&ProcessExecutor, &target, repository.path()).unwrap();
        let session_id = "fedcba9876543210fedcba9876543210";
        let managed = ManagedWorktree {
            source_project_directory: inspection.source_project_directory,
            source_repository: inspection.source_repository,
            worktree_root: repository.path().join(".hel/worktrees").join(session_id),
            branch: format!("hel/{session_id}"),
            target: target.clone(),
        };
        let error = create_managed_worktree(&ProcessExecutor, &managed, None).unwrap_err();
        assert!(error.to_string().contains("uncommitted changes"));
        assert!(!managed.worktree_root.exists());

        std::fs::remove_file(repository.path().join("dirty.txt")).unwrap();
        let existing = repository.path().join("existing-worktree");
        test_git(
            repository.path(),
            &[
                "worktree",
                "add",
                "--detach",
                &existing.to_string_lossy(),
                "HEAD",
            ],
        );
        let linked = inspect_raw_project(&ProcessExecutor, &target, &existing).unwrap();
        assert!(!linked.primary_checkout);
    }

    #[test]
    fn managed_worktree_preflight_preserves_colliding_branch_and_directory() {
        let repository = committed_repository();
        let target = ManagedWorktreeTarget::Local;
        let session_id = "abcdef0123456789abcdef0123456789";
        let branch = format!("hel/{session_id}");
        test_git(repository.path(), &["branch", &branch]);
        let worktree = ManagedWorktree {
            source_project_directory: repository.path().to_path_buf(),
            source_repository: repository.path().to_path_buf(),
            worktree_root: repository.path().join(".hel/worktrees").join(session_id),
            branch: branch.clone(),
            target,
        };

        let error = ensure_managed_worktree_available(&ProcessExecutor, &worktree).unwrap_err();
        assert!(error.to_string().contains("branch already exists"));
        assert!(
            !test_git(
                repository.path(),
                &["show-ref", "--verify", &format!("refs/heads/{branch}")]
            )
            .is_empty()
        );
        std::fs::create_dir_all(&worktree.worktree_root).unwrap();
        let error = ensure_managed_worktree_available(&ProcessExecutor, &worktree).unwrap_err();
        assert!(error.to_string().contains("path already exists"));
        assert!(worktree.worktree_root.is_dir());
    }

    #[test]
    fn managed_worktree_ssh_commands_preserve_hostile_path_boundaries() {
        let target = ManagedWorktreeTarget::Ssh {
            destination: "builder".into(),
            ssh_args: vec!["-o".into(), "BatchMode=yes".into()],
        };
        let command = managed_git_command(
            &target,
            Path::new("/srv/project with ' quote"),
            ["worktree", "prune"],
            "prune test",
        );
        assert_eq!(command.program, "ssh");
        assert_eq!(&command.args[..3], ["-o", "BatchMode=yes", "builder"]);
        assert_eq!(
            command.args[3],
            "'git' '-C' '/srv/project with '\\'' quote' 'worktree' 'prune'"
        );
    }
}
