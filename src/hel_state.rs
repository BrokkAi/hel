//! Durable controller-side state for Hel-managed sessions.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};

use crate::hel_config::{HarnessKind, HelConfig, atomic_write, data_dir, validate_id};
use crate::hel_targets::{AdditionalMount, validate_additional_mounts};
use crate::hel_worker::{
    RELAY_EVENT_GENESIS_DIGEST, RelayOperationalState, SequencedEvent, WorkerEvent,
};

pub const STATE_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SessionState {
    Provisioning,
    Running,
    Disconnected,
    Checkpointing,
    Closing,
    Destroying,
    Archived,
    Lost,
    Error,
    DestroyedWithDataLoss,
}

/// Controller-owned execution state derived from the relay event stream.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum MaterializedExecutionState {
    #[default]
    Idle,
    Running {
        started_at_ms: i64,
    },
    Closing,
    Closed,
}

pub use crate::hel_transcript::{TranscriptBody, TranscriptItem};

/// What a durable queue entry does when its turn comes.
///
/// Serialized without a tag for prompts so entries written before configuration
/// changes could be queued keep loading unchanged.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueuedCommandKind {
    #[default]
    Prompt,
    SetConfig {
        key: String,
        value: String,
    },
}

impl QueuedCommandKind {
    pub fn is_prompt(&self) -> bool {
        matches!(self, Self::Prompt)
    }
}

/// The composer form of a configuration change, used both as the queue entry's
/// display text and as the text peeled back into the composer for editing.
pub fn config_command_text(key: &str, value: &str) -> String {
    format!("/{key} {value}")
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaterializedQueuedPrompt {
    pub command_id: String,
    #[serde(default, skip_serializing_if = "QueuedCommandKind::is_prompt")]
    pub kind: QueuedCommandKind,
    pub content: Vec<serde_json::Value>,
    pub queued_at_ms: i64,
}

/// Canonical controller projection for one logical ACP session.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaterializedSession {
    pub session_id: String,
    pub applied_event_ordinal: u64,
    pub applied_event_digest: String,
    /// Monotonic controller projection watermark derived from relay event
    /// receipt times. It is deliberately independent of retained rows.
    pub last_activity_at_ms: Option<i64>,
    pub execution: MaterializedExecutionState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_title: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub configuration: BTreeMap<String, serde_json::Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    /// Transcript items are shared by pointer so cloning a snapshot copies
    /// handles rather than the whole conversation.
    pub transcript: Vec<Arc<TranscriptItem>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub queued_prompts: Vec<MaterializedQueuedPrompt>,
}

impl MaterializedSession {
    pub fn empty(session_id: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            applied_event_ordinal: 0,
            applied_event_digest: RELAY_EVENT_GENESIS_DIGEST.into(),
            last_activity_at_ms: None,
            execution: MaterializedExecutionState::Idle,
            session_title: None,
            configuration: BTreeMap::new(),
            transcript: Vec::new(),
            queued_prompts: Vec::new(),
        }
    }

    pub fn last_activity_at_ms(&self) -> Option<i64> {
        self.last_activity_at_ms
    }

    pub fn unread_agent_messages_after(&self, detached_after_event_ordinal: u64) -> u64 {
        self.transcript
            .iter()
            .filter(|item| {
                item.latest_content_event_ordinal
                    .is_some_and(|ordinal| ordinal > detached_after_event_ordinal)
                    && item.is_nonempty_agent_message()
            })
            .count() as u64
    }

    pub(crate) fn validate(&self) -> Result<()> {
        validate_id("session", &self.session_id)?;
        validate_relay_event_frontier(
            self.applied_event_ordinal,
            &self.applied_event_digest,
            "materialized session event frontier",
        )?;
        if self
            .session_title
            .as_ref()
            .is_some_and(|title| title.trim().is_empty())
        {
            bail!("materialized session has an empty title");
        }
        let mut item_ids = BTreeSet::new();
        for item in &self.transcript {
            item.validate(self.applied_event_ordinal)?;
            if !item_ids.insert(item.stable_id.as_str()) {
                bail!(
                    "materialized transcript contains duplicate item {:?}",
                    item.stable_id
                );
            }
        }
        let mut command_ids = BTreeSet::new();
        for prompt in &self.queued_prompts {
            if prompt.command_id.trim().is_empty() {
                bail!("materialized prompt queue has an empty command id");
            }
            if !command_ids.insert(prompt.command_id.as_str()) {
                bail!(
                    "materialized prompt queue contains duplicate command {:?}",
                    prompt.command_id
                );
            }
            if let QueuedCommandKind::SetConfig { key, value } = &prompt.kind
                && (key.trim().is_empty() || value.trim().is_empty())
            {
                bail!(
                    "materialized queued configuration change {:?} is incomplete",
                    prompt.command_id
                );
            }
        }
        Ok(())
    }
}

/// A materialized session paired with the live worker's relay state. The
/// session manager hands this to every reader that needs both the durable
/// projection and the connection's operational status.
#[derive(Debug, Clone, PartialEq)]
pub struct ManagedSessionSnapshot {
    pub materialized: MaterializedSession,
    pub operational: RelayOperationalState,
    /// Newest relay event observed by this live actor that reports an
    /// authentication failure. This is intentionally ephemeral: it avoids
    /// retaining raw replay pages or rescanning projected history.
    pub latest_auth_failure_ordinal: Option<u64>,
}

/// One session's activity, reported to the recovery coordinator.
#[derive(Debug, Clone)]
pub struct RecoveryObservation {
    pub session: SessionRecord,
    pub config: HelConfig,
    pub latest_completed_turn_ordinal: Option<u64>,
    pub execution: MaterializedExecutionState,
}

pub fn latest_completed_turn_ordinal(session: &MaterializedSession) -> Option<u64> {
    if session.execution != MaterializedExecutionState::Idle {
        return None;
    }
    session
        .transcript
        .iter()
        .rev()
        .find(|item| matches!(item.body, TranscriptBody::User { .. }))
        .map(|item| item.position)
}

/// Reports session activity to the recovery coordinator.
///
/// Reporting is a queued hand-off, never a round trip: the caller is often a
/// UI event loop, and a copy decision must never hold that loop up. The queue
/// is unbounded so an observation is never dropped, which matters because the
/// idle observation that ends a turn is the one that makes a copy due. Queue
/// depth stays small in practice: the coordinator only folds an observation
/// into per-session policy state and hands the copy itself to another task.
/// It does pause while it records a failed copy, and the queue is what absorbs
/// that pause instead of the caller.
///
/// A caller that must know no copy can start uses [`RecoveryObserver::reserve`]
/// rather than the queue: the reservation blocks a copy from starting whether
/// or not queued observations have been read yet.
#[derive(Clone)]
pub struct RecoveryObserver {
    pub(crate) observations: mpsc::UnboundedSender<RecoveryObservation>,
    pub(crate) busy: watch::Receiver<BTreeSet<String>>,
    pub(crate) gate: Arc<RecoveryGate>,
}

/// A per-session reservation held by a foreground lifecycle operation. The
/// coordinator cannot start another recovery copy until this value is dropped.
pub struct RecoveryReservation {
    session_id: String,
    gate: Arc<RecoveryGate>,
}

impl Drop for RecoveryReservation {
    fn drop(&mut self) {
        self.gate.release(&self.session_id);
    }
}

#[derive(Default)]
pub(crate) struct RecoveryGate {
    state: Mutex<RecoveryGateState>,
}

#[derive(Default)]
struct RecoveryGateState {
    /// In-flight copies, each with the cancel flag its executor watches, so a
    /// foreground lifecycle operation can preempt one instead of waiting.
    busy: BTreeMap<String, Arc<AtomicBool>>,
    reservations: BTreeMap<String, usize>,
}

impl RecoveryGate {
    pub(crate) fn reserve(self: &Arc<Self>, session_id: &str) -> RecoveryReservation {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        *state.reservations.entry(session_id.to_owned()).or_default() += 1;
        RecoveryReservation {
            session_id: session_id.to_owned(),
            gate: self.clone(),
        }
    }

    fn release(&self, session_id: &str) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let Some(count) = state.reservations.get_mut(session_id) else {
            return;
        };
        *count -= 1;
        if *count == 0 {
            state.reservations.remove(session_id);
        }
    }

    /// Claims the session for a copy and returns the cancel flag that copy
    /// must watch, or `None` when a copy or a reservation already holds it.
    pub(crate) fn try_start(&self, session_id: &str) -> Option<Arc<AtomicBool>> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.busy.contains_key(session_id) || state.reservations.contains_key(session_id) {
            return None;
        }
        let cancelled = Arc::new(AtomicBool::new(false));
        state.busy.insert(session_id.to_owned(), cancelled.clone());
        Some(cancelled)
    }

    pub(crate) fn finish(&self, session_id: &str) {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .busy
            .remove(session_id);
    }

    pub(crate) fn is_busy(&self, session_id: &str) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .busy
            .contains_key(session_id)
    }

    /// Asks the in-flight copy for this session, if any, to stop.
    pub(crate) fn cancel_busy(&self, session_id: &str) {
        if let Some(cancelled) = self
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .busy
            .get(session_id)
        {
            cancelled.store(true, Ordering::Release);
        }
    }

    /// Asks every in-flight copy to stop, used when the coordinator shuts down.
    pub(crate) fn cancel_all(&self) {
        for cancelled in self
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .busy
            .values()
        {
            cancelled.store(true, Ordering::Release);
        }
    }

    pub(crate) fn busy_sessions(&self) -> BTreeSet<String> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .busy
            .keys()
            .cloned()
            .collect()
    }
}

/// What a chat view or session lifecycle operation holds to report activity
/// to, and check busy status with, the recovery coordinator for one session.
#[derive(Clone)]
pub struct RecoveryContext {
    pub observer: RecoveryObserver,
    pub session: SessionRecord,
    pub config: HelConfig,
}

impl RecoveryContext {
    pub fn observe(&self, materialized: &MaterializedSession) {
        self.observer.observe(RecoveryObservation {
            session: self.session.clone(),
            config: self.config.clone(),
            latest_completed_turn_ordinal: latest_completed_turn_ordinal(materialized),
            execution: materialized.execution,
        });
    }

    pub fn is_busy(&self) -> bool {
        self.observer.is_busy(&self.session.id)
    }
}

impl RecoveryObserver {
    /// Queues one observation for the coordinator. Returns as soon as the
    /// observation is queued; a stopped coordinator makes this a no-op.
    pub fn observe(&self, observation: RecoveryObservation) {
        let _ = self.observations.send(observation);
    }

    pub fn is_busy(&self, session_id: &str) -> bool {
        self.gate.is_busy(session_id)
    }

    /// Holds off any recovery copy for this session until the returned
    /// reservation is dropped. This, not the observation queue, is what a
    /// lifecycle operation relies on: queued observations may still be
    /// unread, and the coordinator refuses to start a copy for a reserved
    /// session whenever it reads them.
    pub fn reserve(&self, session_id: &str) -> RecoveryReservation {
        self.gate.reserve(session_id)
    }

    /// Asks an in-flight recovery copy for this session to stop. A foreground
    /// lifecycle operation calls this after reserving so it preempts the copy
    /// instead of waiting behind it.
    pub fn cancel_busy(&self, session_id: &str) {
        self.gate.cancel_busy(session_id);
    }

    pub async fn wait_idle(&self, session_id: &str) {
        let mut busy = self.busy.clone();
        while self.is_busy(session_id) {
            if busy.changed().await.is_err() {
                break;
            }
        }
    }
}

pub(crate) fn validate_relay_event_digest(digest: &str, name: &str) -> Result<()> {
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("{name} must be a lowercase SHA-256 digest");
    }
    Ok(())
}

pub(crate) fn validate_relay_event_frontier(ordinal: u64, digest: &str, name: &str) -> Result<()> {
    validate_relay_event_digest(digest, name)?;
    if (ordinal == 0) != (digest == RELAY_EVENT_GENESIS_DIGEST) {
        bail!("{name} has inconsistent ordinal {ordinal} and digest {digest}");
    }
    Ok(())
}

impl SessionState {
    pub const fn is_active(self) -> bool {
        matches!(
            self,
            Self::Provisioning
                | Self::Running
                | Self::Disconnected
                | Self::Checkpointing
                | Self::Closing
                | Self::Destroying
                | Self::Error
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum TargetLocator {
    LocalBare {
        worker_root: PathBuf,
    },
    LocalPodman {
        container_id: String,
    },
    AppleContainer {
        container_id: String,
    },
    AwsEc2 {
        instance_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        address: Option<String>,
    },
    SshBare {
        host: String,
        workspace: PathBuf,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        worker_id: Option<String>,
    },
    SshPodman {
        host: String,
        container_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ManagedWorktreeTarget {
    Local,
    Ssh {
        destination: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        ssh_args: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedWorktree {
    pub source_project_directory: PathBuf,
    pub source_repository: PathBuf,
    pub worktree_root: PathBuf,
    pub branch: String,
    pub target: ManagedWorktreeTarget,
}

impl ManagedWorktree {
    fn validate(&self, session_id: &str, project_directory: Option<&Path>) -> Result<()> {
        for (label, path) in [
            ("source project directory", &self.source_project_directory),
            ("source repository", &self.source_repository),
            ("worktree root", &self.worktree_root),
        ] {
            if !path.is_absolute() || path.components().any(|part| part == Component::ParentDir) {
                bail!("managed worktree {label} must be an absolute safe path");
            }
        }
        if !self
            .source_project_directory
            .starts_with(&self.source_repository)
        {
            bail!("managed worktree source directory is outside its repository");
        }
        let expected_root = self
            .source_repository
            .join(".hel")
            .join("worktrees")
            .join(session_id);
        if self.worktree_root != expected_root {
            bail!("managed worktree root does not match the session-owned path");
        }
        if self.branch != format!("hel/{session_id}") {
            bail!("managed worktree branch does not match the session id");
        }
        let relative = self
            .source_project_directory
            .strip_prefix(&self.source_repository)
            .expect("source relationship checked above");
        if project_directory != Some(self.worktree_root.join(relative).as_path()) {
            bail!("session project directory does not match its managed worktree");
        }
        match &self.target {
            ManagedWorktreeTarget::Local => {}
            ManagedWorktreeTarget::Ssh { destination, .. } if destination.trim().is_empty() => {
                bail!("managed SSH worktree has an empty destination")
            }
            ManagedWorktreeTarget::Ssh { .. } => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum SessionResourceAllocation {
    Container {
        cpus: u64,
        memory_bytes: u64,
    },
    AwsEc2 {
        instance_type: String,
        vcpus: u64,
        memory_bytes: u64,
    },
}

impl SessionResourceAllocation {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Container { cpus, memory_bytes } if *cpus == 0 || *memory_bytes == 0 => {
                bail!("container resource allocation must have non-zero CPU and memory")
            }
            Self::AwsEc2 {
                instance_type,
                vcpus,
                memory_bytes,
            } if instance_type.trim().is_empty() || *vcpus == 0 || *memory_bytes == 0 => {
                bail!("EC2 resource allocation must have an instance type, CPU, and memory")
            }
            _ => Ok(()),
        }
    }
}

/// The CPU count an allocation grants, regardless of target kind.
pub fn allocation_cpus(allocation: &SessionResourceAllocation) -> u64 {
    match allocation {
        SessionResourceAllocation::Container { cpus, .. } => *cpus,
        SessionResourceAllocation::AwsEc2 { vcpus, .. } => *vcpus,
    }
}

/// The memory, in bytes, an allocation grants, regardless of target kind.
pub fn allocation_memory(allocation: &SessionResourceAllocation) -> u64 {
    match allocation {
        SessionResourceAllocation::Container { memory_bytes, .. }
        | SessionResourceAllocation::AwsEc2 { memory_bytes, .. } => *memory_bytes,
    }
}

impl TargetLocator {
    fn validate(&self, session_id: &str) -> Result<()> {
        match self {
            Self::LocalBare { worker_root } => {
                if !worker_root.is_absolute()
                    || worker_root
                        .components()
                        .any(|part| part == Component::ParentDir)
                    || !worker_root.ends_with(session_id)
                {
                    bail!(
                        "local bare worker root must be an absolute safe path ending in the session id"
                    );
                }
            }
            Self::LocalPodman { container_id }
            | Self::AppleContainer { container_id }
            | Self::SshPodman { container_id, .. }
                if container_id.trim().is_empty() =>
            {
                bail!("target locator has an empty container id")
            }
            Self::AwsEc2 { instance_id, .. } if instance_id.trim().is_empty() => {
                bail!("target locator has an empty AWS instance id")
            }
            Self::SshBare {
                host, workspace, ..
            } => {
                if host.trim().is_empty() {
                    bail!("bare SSH target locator has an empty host");
                }
                if workspace.as_os_str().is_empty()
                    || workspace
                        .components()
                        .any(|part| part == Component::ParentDir)
                    || !workspace.ends_with(session_id)
                {
                    bail!("bare SSH target locator must be a safe path ending in the session id");
                }
            }
            Self::SshPodman { host, .. } if host.trim().is_empty() => {
                bail!("SSH Podman target locator has an empty host")
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointMetadata {
    pub archive_path: PathBuf,
    /// Lowercase SHA-256 digest of the verified archive.
    pub sha256: String,
    pub created_at: String,
    pub event_frontier: u64,
}

impl CheckpointMetadata {
    fn validate(&self) -> Result<()> {
        if self.archive_path.as_os_str().is_empty() {
            bail!("checkpoint archive path is empty");
        }
        if self.sha256.len() != 64
            || !self
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            bail!("checkpoint SHA-256 must be 64 lowercase hexadecimal characters");
        }
        if self.created_at.trim().is_empty() {
            bail!("checkpoint timestamp is empty");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionRecord {
    pub id: String,
    pub title: String,
    pub harness_kind: HarnessKind,
    pub last_profile: String,
    pub bundle_id: String,
    /// Existing project directory used directly by a local or SSH bare target.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_directory: Option<PathBuf>,
    /// Git worktree created and owned by Hel for this raw-project session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub managed_worktree: Option<ManagedWorktree>,
    pub target_template_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_allocation: Option<SessionResourceAllocation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub additional_mounts: Vec<AdditionalMount>,
    pub state: SessionState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<TargetLocator>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acp_session_title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_title_override: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub detached_after_event_ordinal: u64,
    /// Unsent chat input carried across a detach, so returning to a session
    /// restores what the user was typing. Empty means no draft.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub draft_input: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_checkpoint_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<CheckpointMetadata>,
}

impl SessionRecord {
    /// User-visible session name, independent of the initial prompt stored in `title`.
    pub fn display_title(&self) -> &str {
        self.session_title_override
            .as_deref()
            .or(self.acp_session_title.as_deref())
            .unwrap_or(&self.id)
    }

    /// Project this session works in, as the session list and the chat header
    /// both name it: the source repository of a managed worktree, else the
    /// project directory, else the bundle's primary repository, else the
    /// bundle id.
    pub fn project_name(&self, config: &HelConfig) -> String {
        if let Some(worktree) = &self.managed_worktree {
            return path_leaf(&worktree.source_repository);
        }
        if let Some(project_directory) = &self.project_directory {
            return path_leaf(project_directory);
        }
        config
            .bundles
            .get(&self.bundle_id)
            .and_then(|bundle| {
                bundle
                    .repositories
                    .iter()
                    .find(|repository| repository.id == bundle.primary_repo)
                    .or_else(|| bundle.repositories.first())
            })
            .map(|repository| path_leaf(&repository.destination))
            .unwrap_or_else(|| path_leaf(Path::new(&self.bundle_id)))
    }

    /// Orders two sessions the way the session list's sequence view does:
    /// oldest first by creation time, with the id as a stable tiebreak. A
    /// session whose timestamp does not parse sorts last.
    pub fn compare_by_creation(&self, other: &Self) -> std::cmp::Ordering {
        match (
            created_at_seconds(&self.created_at),
            created_at_seconds(&other.created_at),
        ) {
            (Some(left), Some(right)) => left.cmp(&right),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        }
        .then_with(|| self.id.cmp(&other.id))
    }

    fn validate(&self, map_id: &str) -> Result<()> {
        validate_id("session", &self.id)?;
        if self.id != map_id {
            bail!(
                "session map key {map_id:?} does not match record id {:?}",
                self.id
            );
        }
        validate_id("profile", &self.last_profile)?;
        validate_id("bundle", &self.bundle_id)?;
        if let Some(project_directory) = &self.project_directory
            && (!project_directory.is_absolute()
                || project_directory
                    .components()
                    .any(|part| part == Component::ParentDir))
        {
            bail!("session {:?} has an unsafe project directory", self.id);
        }
        if let Some(managed_worktree) = &self.managed_worktree {
            managed_worktree.validate(&self.id, self.project_directory.as_deref())?;
        }
        validate_id("target template", &self.target_template_id)?;
        if let Some(allocation) = &self.resource_allocation {
            allocation.validate()?;
        }
        validate_additional_mounts(&self.additional_mounts)?;
        if self.title.trim().is_empty() {
            bail!("session {:?} has an empty title", self.id);
        }
        if self
            .acp_session_title
            .as_ref()
            .is_some_and(|title| title.trim().is_empty())
            || self
                .session_title_override
                .as_ref()
                .is_some_and(|title| title.trim().is_empty())
        {
            bail!("session {:?} has an empty display title", self.id);
        }
        if self.created_at.trim().is_empty() || self.updated_at.trim().is_empty() {
            bail!("session {:?} has an empty timestamp", self.id);
        }
        if let Some(target) = &self.target {
            target.validate(&self.id)?;
        }
        if let Some(checkpoint) = &self.checkpoint {
            checkpoint.validate()?;
        }
        Ok(())
    }
}

/// Last component of a path, falling back to the whole path when it has none.
fn path_leaf(path: &Path) -> String {
    path.file_name()
        .unwrap_or(path.as_os_str())
        .to_string_lossy()
        .into_owned()
}

fn created_at_seconds(timestamp: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(timestamp)
        .ok()
        .map(|timestamp| timestamp.timestamp())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HelState {
    pub version: u32,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub sessions: BTreeMap<String, SessionRecord>,
    /// Recently used source directories, keyed by `local` or SSH host name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub mount_history: BTreeMap<String, Vec<PathBuf>>,
}

impl Default for HelState {
    fn default() -> Self {
        Self {
            version: STATE_VERSION,
            sessions: BTreeMap::new(),
            mount_history: BTreeMap::new(),
        }
    }
}

impl HelState {
    pub fn validate(&self) -> Result<()> {
        if self.version != STATE_VERSION {
            bail!(
                "unsupported Hel state version {}; expected {STATE_VERSION}",
                self.version
            );
        }
        for (id, session) in &self.sessions {
            session.validate(id)?;
        }
        for (host, sources) in &self.mount_history {
            if host.trim().is_empty() {
                bail!("mount history contains an empty host key");
            }
            if sources.iter().any(|source| !source.is_absolute()) {
                bail!("mount history for {host:?} contains a non-absolute source path");
            }
        }
        Ok(())
    }

    pub fn remember_mount_sources(&mut self, host: &str, mounts: &[AdditionalMount]) {
        if mounts.is_empty() {
            return;
        }
        let sources = self.mount_history.entry(host.to_owned()).or_default();
        for mount in mounts.iter().rev() {
            sources.retain(|source| source != &mount.source);
            sources.insert(0, mount.source.clone());
        }
        sources.truncate(20);
    }

    pub fn project_directories(&self, host: &str) -> &[PathBuf] {
        self.mount_history
            .get(&project_history_key(host))
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    pub fn remember_project_directory(&mut self, host: &str, directory: &Path) {
        let key = project_history_key(host);
        let directories = self.mount_history.entry(key).or_default();
        directories.retain(|existing| existing != directory);
        directories.insert(0, directory.to_path_buf());
        directories.truncate(20);
    }

    pub fn remove_archived_session(&mut self, session_id: &str) -> Result<SessionRecord> {
        let session = self
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?;
        if session.state.is_active() {
            bail!("refusing to delete active session {session_id}");
        }
        Ok(self
            .sessions
            .remove(session_id)
            .expect("session checked above"))
    }

    /// Validate persisted foreign keys without preventing config entries from
    /// being renamed after a session is fully archived.
    pub fn validate_against_config(&self, config: &HelConfig) -> Result<()> {
        self.validate()?;
        config.validate()?;
        for session in self
            .sessions
            .values()
            .filter(|session| session.state.is_active())
        {
            let profile = config.profiles.get(&session.last_profile).ok_or_else(|| {
                anyhow::anyhow!(
                    "active session {:?} references missing profile {:?}",
                    session.id,
                    session.last_profile
                )
            })?;
            if profile.kind != session.harness_kind {
                bail!(
                    "active session {:?} expects {:?}, but profile {:?} is {:?}",
                    session.id,
                    session.harness_kind,
                    session.last_profile,
                    profile.kind
                );
            }
            if session.project_directory.is_none()
                && !config.bundles.contains_key(&session.bundle_id)
            {
                bail!(
                    "active session {:?} references missing bundle {:?}",
                    session.id,
                    session.bundle_id
                );
            }
            if !config.targets.contains_key(&session.target_template_id) {
                bail!(
                    "active session {:?} references missing target template {:?}",
                    session.id,
                    session.target_template_id
                );
            }
        }
        Ok(())
    }

    pub fn load() -> Result<Self> {
        crate::hel_database::migrate_legacy_state()?;
        crate::hel_database::load_state()
    }

    pub fn load_from(path: &Path) -> Result<Self> {
        Self::load_json_from(path)
    }

    pub(crate) fn load_json_from(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let body = fs::read(path).with_context(|| format!("read Hel state {}", path.display()))?;
        let state: Self = serde_json::from_slice(&body)
            .with_context(|| format!("parse Hel state {}", path.display()))?;
        state.validate()?;
        Ok(state)
    }

    pub fn save(&self) -> Result<()> {
        crate::hel_database::save_state(self)
    }

    pub fn save_to(&self, path: &Path) -> Result<()> {
        self.validate()?;
        let body = serde_json::to_vec_pretty(self).context("serialize Hel state")?;
        atomic_write(path, &body)
    }
}

fn project_history_key(host: &str) -> String {
    format!("project:{host}")
}

pub fn state_path() -> PathBuf {
    data_dir().join("state.json")
}

/// Generate an opaque, filesystem-safe stable id for a new logical session.
pub fn new_session_id() -> Result<String> {
    let mut random = [0u8; 16];
    getrandom::fill(&mut random)
        .map_err(|error| anyhow::anyhow!("generate Hel session id: {error}"))?;
    let mut encoded = String::with_capacity(32);
    for byte in random {
        use std::fmt::Write as _;
        write!(encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    Ok(encoded)
}

/// Return the newest clean ACP session title from canonical worker events.
pub fn harness_session_title(events: &[SequencedEvent]) -> Option<String> {
    events.iter().rev().find_map(|event| {
        let WorkerEvent::Adapter { payload, .. } = &event.event else {
            return None;
        };
        let crate::hel_acp::RuntimeEvent::SessionUpdate { update } =
            serde_json::from_value(payload.clone()).ok()?
        else {
            return None;
        };
        let kind = update
            .get("sessionUpdate")
            .and_then(serde_json::Value::as_str)?;
        let title = match kind {
            "session_info_update" | "session_title" => {
                update.get("title").and_then(serde_json::Value::as_str)
            }
            _ => None,
        }?;
        normalize_session_title(title)
    })
}

pub(crate) fn normalize_session_title(title: &str) -> Option<String> {
    let normalized = title.split_whitespace().collect::<Vec<_>>().join(" ");
    (!normalized.is_empty()).then_some(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hel_config::{
        CONFIG_VERSION, ContainerTemplate, HarnessProfile, ProjectBundle, ProjectRepository,
        TargetTemplate,
    };

    fn sample_state() -> HelState {
        let session = SessionRecord {
            id: "0123456789abcdef".into(),
            title: "Build Hel".into(),
            harness_kind: HarnessKind::Codex,
            last_profile: "codex-1".into(),
            bundle_id: "hel".into(),
            project_directory: None,
            managed_worktree: None,
            target_template_id: "podman".into(),
            resource_allocation: None,
            additional_mounts: vec![AdditionalMount {
                source: PathBuf::from("/home/test/cache"),
                destination: PathBuf::from("/mnt/cache"),
            }],
            state: SessionState::Running,
            target: Some(TargetLocator::LocalPodman {
                container_id: "afb67d".into(),
            }),
            native_session_id: Some("native-1".into()),
            acp_session_title: Some("Build Hel".into()),
            session_title_override: None,
            created_at: "2026-08-09T12:00:00Z".into(),
            updated_at: "2026-08-09T12:01:00Z".into(),
            detached_after_event_ordinal: 0,
            draft_input: String::new(),
            last_error: None,
            last_checkpoint_error: None,
            checkpoint: Some(CheckpointMetadata {
                archive_path: PathBuf::from("sessions/0123456789abcdef.hel.zip"),
                sha256: "a".repeat(64),
                created_at: "2026-08-09T12:01:00Z".into(),
                event_frontier: 42,
            }),
        };
        HelState {
            version: STATE_VERSION,
            sessions: BTreeMap::from([(session.id.clone(), session)]),
            mount_history: BTreeMap::from([(
                "local".into(),
                vec![PathBuf::from("/home/test/cache")],
            )]),
        }
    }

    fn sample_config() -> HelConfig {
        HelConfig {
            version: CONFIG_VERSION,
            profiles: BTreeMap::from([(
                "codex-1".into(),
                HarnessProfile {
                    context_window_bytes: None,
                    kind: HarnessKind::Codex,
                    home: PathBuf::from("/home/test/.codex"),
                    executable: None,
                    environment: BTreeMap::new(),
                },
            )]),
            bundles: BTreeMap::from([(
                "hel".into(),
                ProjectBundle {
                    primary_repo: "hel".into(),
                    repositories: vec![ProjectRepository {
                        id: "hel".into(),
                        github: Some("BrokkAi/hel".into()),
                        local: None,
                        destination: PathBuf::from("hel"),
                        git_ref: None,
                    }],
                },
            )]),
            targets: BTreeMap::from([(
                "podman".into(),
                TargetTemplate::LocalPodman {
                    container: ContainerTemplate {
                        image: "ubuntu:24.04".into(),
                        platform: None,
                        cpus: None,
                        memory: None,
                        environment: BTreeMap::new(),
                    },
                },
            )]),
        }
    }

    fn sample_session() -> SessionRecord {
        sample_state()
            .sessions
            .remove("0123456789abcdef")
            .expect("sample session")
    }

    #[test]
    fn project_name_prefers_a_worktree_source_then_a_project_directory_then_the_bundle() {
        let mut config = sample_config();
        config
            .bundles
            .get_mut("hel")
            .expect("bundle")
            .repositories
            .push(ProjectRepository {
                id: "docs".into(),
                github: Some("BrokkAi/docs".into()),
                local: None,
                destination: PathBuf::from("documentation"),
                git_ref: None,
            });
        let mut session = sample_session();

        assert_eq!(session.project_name(&config), "hel");

        session.project_directory = Some(PathBuf::from("/home/test/Projects/raw-project"));
        assert_eq!(session.project_name(&config), "raw-project");

        session.project_directory = Some(PathBuf::from(
            "/home/test/Projects/source/.hel/worktrees/0123456789abcdef",
        ));
        session.managed_worktree = Some(ManagedWorktree {
            source_project_directory: PathBuf::from("/home/test/Projects/source"),
            source_repository: PathBuf::from("/home/test/Projects/source"),
            worktree_root: PathBuf::from(
                "/home/test/Projects/source/.hel/worktrees/0123456789abcdef",
            ),
            branch: "hel/0123456789abcdef".into(),
            target: ManagedWorktreeTarget::Local,
        });
        assert_eq!(session.project_name(&config), "source");
    }

    #[test]
    fn sessions_order_by_creation_time_and_fall_back_to_the_id() {
        let older = sample_session();
        let mut newer = sample_session();
        newer.id = "0000000000000001".into();
        newer.created_at = "2026-08-09T13:00:00Z".into();
        let mut unparsable = sample_session();
        unparsable.id = "0000000000000002".into();
        unparsable.created_at = "not a timestamp".into();
        let mut same_time = sample_session();
        same_time.id = "zzzzzzzzzzzzzzzz".into();

        let mut sessions = [&unparsable, &newer, &same_time, &older];
        sessions.sort_by(|left, right| left.compare_by_creation(right));

        assert_eq!(
            sessions
                .iter()
                .map(|session| &session.id)
                .collect::<Vec<_>>(),
            [&older.id, &same_time.id, &newer.id, &unparsable.id]
        );
    }

    #[test]
    fn retired_checkpoint_and_detach_cursor_names_are_rejected() {
        let session_id = "0123456789abcdef";

        let mut old_checkpoint = serde_json::to_value(sample_state()).unwrap();
        let checkpoint = old_checkpoint["sessions"][session_id]["checkpoint"]
            .as_object_mut()
            .unwrap();
        let frontier = checkpoint.remove("event_frontier").unwrap();
        checkpoint.insert("event_sequence".into(), frontier);
        assert!(serde_json::from_value::<HelState>(old_checkpoint).is_err());

        let mut old_detach_cursor = serde_json::to_value(sample_state()).unwrap();
        let session = old_detach_cursor["sessions"][session_id]
            .as_object_mut()
            .unwrap();
        let ordinal = session.remove("detached_after_event_ordinal").unwrap();
        session.insert("last_viewed_event_sequence".into(), ordinal);
        assert!(serde_json::from_value::<HelState>(old_detach_cursor).is_err());
    }

    #[test]
    fn state_written_before_drafts_loads_with_an_empty_draft() {
        let session_id = "0123456789abcdef";
        let mut without_draft = serde_json::to_value(sample_state()).unwrap();
        let session = without_draft["sessions"][session_id]
            .as_object_mut()
            .unwrap();
        session.remove("draft_input");

        let state = serde_json::from_value::<HelState>(without_draft).unwrap();
        assert_eq!(state.sessions[session_id].draft_input, "");
    }

    #[test]
    fn json_state_round_trip_is_atomic() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("nested/state.json");
        let state = sample_state();
        state.save_to(&path).unwrap();
        assert_eq!(HelState::load_from(&path).unwrap(), state);
        assert!(
            fs::read_dir(directory.path().join("nested"))
                .unwrap()
                .all(|entry| {
                    !entry
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .ends_with(".tmp")
                })
        );
    }

    #[test]
    fn mount_history_keeps_unique_recent_sources_per_host() {
        let mut state = HelState::default();
        state.remember_mount_sources(
            "builder.example.test",
            &[
                AdditionalMount {
                    source: "/srv/first".into(),
                    destination: "/mnt/first".into(),
                },
                AdditionalMount {
                    source: "/srv/second".into(),
                    destination: "/mnt/second".into(),
                },
            ],
        );
        state.remember_mount_sources(
            "builder.example.test",
            &[AdditionalMount {
                source: "/srv/first".into(),
                destination: "/mnt/again".into(),
            }],
        );

        assert_eq!(
            state.mount_history["builder.example.test"],
            vec![PathBuf::from("/srv/first"), PathBuf::from("/srv/second")]
        );
    }

    #[test]
    fn materialized_activity_watermark_does_not_regress_when_detail_is_removed() {
        let mut materialized = MaterializedSession::empty("session-1");
        assert_eq!(materialized.last_activity_at_ms(), None);

        materialized.execution = MaterializedExecutionState::Running { started_at_ms: 300 };
        materialized.transcript.push(Arc::new(TranscriptItem {
            stable_id: "system:1".into(),
            position: 1,
            latest_content_event_ordinal: None,
            created_at_ms: 350,
            last_changed_at_ms: 400,
            body: TranscriptBody::System {
                text: "working".into(),
            },
        }));
        materialized.queued_prompts.push(MaterializedQueuedPrompt {
            command_id: "prompt-2".into(),
            kind: QueuedCommandKind::Prompt,
            content: Vec::new(),
            queued_at_ms: 500,
        });
        materialized.last_activity_at_ms = Some(500);
        assert_eq!(materialized.last_activity_at_ms(), Some(500));

        materialized.queued_prompts.clear();
        assert_eq!(materialized.last_activity_at_ms(), Some(500));
        materialized.transcript.clear();
        assert_eq!(materialized.last_activity_at_ms(), Some(500));
        materialized.execution = MaterializedExecutionState::Idle;
        assert_eq!(materialized.last_activity_at_ms(), Some(500));
    }

    /// Shared transcript items must stay plain JSON on the wire: sharing is a
    /// controller memory concern, not part of the serialized shape.
    #[test]
    fn shared_transcript_items_serialize_as_plain_items() {
        let mut materialized = MaterializedSession::empty("session-1");
        materialized.applied_event_ordinal = 1;
        materialized.applied_event_digest = "a".repeat(64);
        let item = Arc::new(TranscriptItem {
            stable_id: "system:1".into(),
            position: 1,
            latest_content_event_ordinal: None,
            created_at_ms: 10,
            last_changed_at_ms: 10,
            body: TranscriptBody::System {
                text: "started".into(),
            },
        });
        // The same item twice would be deduplicated by serde's pointer-aware
        // encodings; stable ids keep it a legal transcript.
        materialized.transcript.push(Arc::clone(&item));
        let mut second = TranscriptItem::clone(&item);
        second.stable_id = "system:2".into();
        materialized.transcript.push(Arc::new(second));
        materialized.validate().unwrap();

        let encoded = serde_json::to_value(&materialized).unwrap();
        assert_eq!(encoded["transcript"][0]["stable_id"], "system:1");
        assert_eq!(encoded["transcript"][0]["body"]["kind"], "system");
        assert_eq!(encoded["transcript"][0]["body"]["text"], "started");
        assert_eq!(encoded["transcript"][1]["stable_id"], "system:2");

        let restored: MaterializedSession = serde_json::from_value(encoded).unwrap();
        assert_eq!(restored, materialized);
    }

    #[test]
    fn materialized_event_frontier_requires_the_matching_digest_kind() {
        let mut materialized = MaterializedSession::empty("session-1");
        materialized.validate().unwrap();

        materialized.applied_event_ordinal = 1;
        assert!(
            materialized
                .validate()
                .unwrap_err()
                .to_string()
                .contains("inconsistent ordinal")
        );

        materialized.applied_event_digest = "A".repeat(64);
        assert!(
            materialized
                .validate()
                .unwrap_err()
                .to_string()
                .contains("lowercase SHA-256")
        );
    }

    #[test]
    fn project_directory_history_is_recent_and_isolated_per_remote_host() {
        let mut state = HelState::default();
        state.remember_project_directory("builder-a", Path::new("/srv/one"));
        state.remember_project_directory("builder-a", Path::new("/srv/two"));
        state.remember_project_directory("builder-a", Path::new("/srv/one"));
        state.remember_project_directory("builder-b", Path::new("/work/other"));

        assert_eq!(
            state.project_directories("builder-a"),
            [PathBuf::from("/srv/one"), PathBuf::from("/srv/two")]
        );
        assert_eq!(
            state.project_directories("builder-b"),
            [PathBuf::from("/work/other")]
        );
    }

    #[test]
    fn active_state_validates_references_and_harness_kind() {
        let state = sample_state();
        state.validate_against_config(&sample_config()).unwrap();

        let mut config = sample_config();
        config.profiles.get_mut("codex-1").unwrap().kind = HarnessKind::Claude;
        assert!(
            state
                .validate_against_config(&config)
                .unwrap_err()
                .to_string()
                .contains("expects Codex")
        );
    }

    #[test]
    fn archived_session_does_not_pin_renamed_config_entries() {
        let mut state = sample_state();
        state.sessions.values_mut().next().unwrap().state = SessionState::Archived;
        state
            .validate_against_config(&HelConfig::default())
            .unwrap();
    }

    #[test]
    fn only_inactive_sessions_can_be_removed_from_the_archive() {
        let mut state = sample_state();
        assert!(
            state
                .remove_archived_session("0123456789abcdef")
                .unwrap_err()
                .to_string()
                .contains("active session")
        );
        assert!(state.sessions.contains_key("0123456789abcdef"));

        state.sessions.values_mut().next().unwrap().state = SessionState::Archived;
        let removed = state.remove_archived_session("0123456789abcdef").unwrap();
        assert_eq!(removed.id, "0123456789abcdef");
        assert!(state.sessions.is_empty());
    }

    #[test]
    fn harness_title_prefers_the_newest_session_info_update() {
        let events = vec![
            SequencedEvent {
                seq: 1,
                recorded_at_ms: None,
                request_id: None,
                event: WorkerEvent::Adapter {
                    kind: "session_update".into(),
                    payload: serde_json::json!({
                        "type": "session_update",
                        "update": {
                            "sessionUpdate": "session_info_update",
                            "title": "First title"
                        }
                    }),
                },
            },
            SequencedEvent {
                seq: 2,
                recorded_at_ms: None,
                request_id: None,
                event: WorkerEvent::Adapter {
                    kind: "session_update".into(),
                    payload: serde_json::json!({
                        "type": "session_update",
                        "update": {
                            "sessionUpdate": "session_summary",
                            "summary": "  Build   the dashboard  "
                        }
                    }),
                },
            },
        ];

        assert_eq!(
            harness_session_title(&events).as_deref(),
            Some("First title")
        );
    }

    #[test]
    fn extension_session_title_is_cleaned_without_losing_available_text() {
        let first_prompt = format!("{}overflow", "word ".repeat(20));
        let expected = first_prompt.trim().to_string();
        let events = vec![
            SequencedEvent {
                seq: 1,
                recorded_at_ms: None,
                request_id: Some("prompt-1".into()),
                event: WorkerEvent::PromptAccepted {
                    request_id: "prompt-1".into(),
                    text: format!("  {first_prompt}\n"),
                    attachments: vec![],
                },
            },
            SequencedEvent {
                seq: 2,
                recorded_at_ms: None,
                request_id: None,
                event: WorkerEvent::Adapter {
                    kind: "session_update".into(),
                    payload: serde_json::json!({
                        "type": "session_update",
                        "update": {
                            "sessionUpdate": "session_title",
                            "title": first_prompt
                        }
                    }),
                },
            },
        ];

        assert_eq!(
            harness_session_title(&events).as_deref(),
            Some(expected.as_str())
        );
    }

    #[test]
    fn first_prompt_is_not_used_as_an_acp_session_title() {
        let events = vec![SequencedEvent {
            seq: 1,
            recorded_at_ms: None,
            request_id: Some("prompt-1".into()),
            event: WorkerEvent::PromptAccepted {
                request_id: "prompt-1".into(),
                text: "Do not use me as a title".into(),
                attachments: vec![],
            },
        }];

        assert_eq!(harness_session_title(&events), None);
    }

    #[test]
    fn harness_titles_are_normalized_to_one_complete_line() {
        let events = vec![SequencedEvent {
            seq: 1,
            recorded_at_ms: None,
            request_id: None,
            event: WorkerEvent::Adapter {
                kind: "session_update".into(),
                payload: serde_json::json!({
                    "type": "session_update",
                    "update": {
                        "sessionUpdate": "session_title",
                        "title": "first\nsecond\tthird fourth fifth sixth seventh eighth ninth tenth eleventh twelfth thirteenth"
                    }
                }),
            },
        }];

        assert_eq!(
            harness_session_title(&events).as_deref(),
            Some(
                "first second third fourth fifth sixth seventh eighth ninth tenth eleventh twelfth thirteenth"
            )
        );
    }

    #[test]
    fn locator_rejects_parent_traversal() {
        let mut state = sample_state();
        state.sessions.values_mut().next().unwrap().target = Some(TargetLocator::SshBare {
            host: "builder".into(),
            workspace: PathBuf::from("~/hel/../other"),
            worker_id: None,
        });
        assert!(
            state
                .validate()
                .unwrap_err()
                .to_string()
                .contains("safe path ending")
        );
    }

    #[test]
    fn generated_session_ids_are_valid_and_distinct() {
        let first = new_session_id().unwrap();
        let second = new_session_id().unwrap();
        validate_id("session", &first).unwrap();
        assert_eq!(first.len(), 32);
        assert_ne!(first, second);
    }
}
