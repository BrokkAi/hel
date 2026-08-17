//! Persistent, transport-neutral protocol core for a Hel target worker.
//!
//! The worker never listens on a network port. Controllers speak newline-
//! delimited JSON through `hel worker proxy`, which can itself be carried over
//! SSH or a container exec stream.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, Write};
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};

use agent_client_protocol::schema::ProtocolVersion as AcpProtocolVersion;
use agent_client_protocol::schema::v1::{
    AgentCapabilities, AvailableCommand, ContentBlock, Implementation, SessionConfigOption,
    SessionUpdate,
};
use anyhow::{Context, Result, anyhow, bail};
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::hel_archive::CanonicalQueuedCommandKind;

pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
/// Serialized bytes of observations allowed in one attach response, well under
/// `MAX_FRAME_BYTES` to leave room for the envelope.
pub const RELAY_REPLAY_BYTE_BUDGET: usize = 4 * 1024 * 1024;
/// A single durable command must remain comfortably smaller than a relay
/// frame. Commands are repeated in the event journal and private dispatch
/// state, so admitting a frame-sized command would make later attaches
/// impossible to encode.
pub const RELAY_COMMAND_BYTE_BUDGET: usize = 1024 * 1024;
/// Every event must fit by itself in a replay page.
pub const RELAY_EVENT_BYTE_BUDGET: usize = 2 * 1024 * 1024;
/// The public operational state shares an attach frame with a replay page.
pub const RELAY_STATE_BYTE_BUDGET: usize = 2 * 1024 * 1024;
/// Headroom left for an event's envelope — ordinals, digests, timestamp and
/// command id — when clamping an observation to `RELAY_EVENT_BYTE_BUDGET`.
const RELAY_EVENT_ENVELOPE_RESERVE: usize = 8 * 1024;
/// Clamping never shortens a string below this. Identifiers, type tags and
/// paths stay whole; only genuinely large payloads are candidates.
const RELAY_TRUNCATION_FLOOR: usize = 4 * 1024;
/// The private snapshot also has a hard ceiling so repeated accepted commands
/// cannot grow the durable state file without bound between checkpoints.
const RELAY_SNAPSHOT_BYTE_BUDGET: usize = 16 * 1024 * 1024;
/// The first protocol for the durable ACP relay. There is deliberately no
/// wire-level compatibility or negotiation with the retired worker protocol.
pub const RELAY_PROTOCOL_VERSION: u32 = 1;
pub const RELAY_MIN_PROTOCOL_VERSION: u32 = RELAY_PROTOCOL_VERSION;
/// Digest for the empty relay event prefix (ordinal zero).
pub const RELAY_EVENT_GENESIS_DIGEST: &str = crate::hel_archive::EVENT_FRONTIER_GENESIS_DIGEST;
const RELAY_EVENT_DIGEST_DOMAIN: &[u8] = b"hel-relay-event-v1\0";
const RELAY_STATE_VERSION: u32 = 1;
const RELAY_STATE_FILE: &str = "relay-state.json";
const RELAY_JOURNAL_DIR: &str = "relay-journal";
const RELAY_ACTIVE_SEGMENT: &str = "active.jsonl";
const RELAY_SEGMENT_BYTE_LIMIT: u64 = 1024 * 1024;
const RELAY_HOT_EVENT_CAPACITY: usize = 32;
const NATIVE_SESSION_IDENTITY_FILE: &str = "native-session.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelayVersionRange {
    pub min: u32,
    pub max: u32,
}

impl RelayVersionRange {
    pub const CURRENT: Self = Self {
        min: RELAY_MIN_PROTOCOL_VERSION,
        max: RELAY_PROTOCOL_VERSION,
    };

    pub fn negotiate(self, peer: Self) -> Option<u32> {
        let minimum = self.min.max(peer.min);
        let maximum = self.max.min(peer.max);
        (minimum <= maximum).then_some(maximum)
    }
}

/// A request on the new controller-to-relay boundary. ACP payloads remain ACP
/// payloads; only durability and queue-control operations are Hel-specific.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "method",
    content = "params",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum RelayRequest {
    Hello {
        controller_version: String,
        supported: RelayVersionRange,
    },
    Attach {
        after_ordinal: u64,
        after_digest: String,
    },
    Acknowledge {
        through_ordinal: u64,
        through_digest: String,
    },
    Submit {
        command_id: String,
        command: RelayCommand,
    },
    Status,
    /// Report non-secret metadata for this session's harness credentials.
    /// The runtime handles credential requests on the connection and never
    /// passes them through the durable relay.
    CredentialState,
    /// Read this session's harness credential file as base64. The payload is
    /// connection-only and must never enter relay state or observations.
    ReadCredentials,
    /// Install a base64-encoded credential file into this session's harness
    /// home. The destination path is fixed by the worker launch config.
    InstallCredentials {
        data: String,
    },
    /// Report non-secret metadata for this session's synced skills trees.
    /// Handled on the connection like credential requests; the durable relay
    /// never sees them.
    SkillsState,
    /// Replace this session's synced skills trees with a base64-encoded
    /// `hel_skills` archive. The destination directories are fixed by the
    /// worker launch config and the harness skills whitelist.
    InstallSkills {
        data: String,
    },
}

impl RelayRequest {
    pub const fn method_name(&self) -> &'static str {
        match self {
            Self::Hello { .. } => "hello",
            Self::Attach { .. } => "attach",
            Self::Acknowledge { .. } => "acknowledge",
            Self::Submit { .. } => "submit",
            Self::Status => "status",
            Self::CredentialState => "credential_state",
            Self::ReadCredentials => "read_credentials",
            Self::InstallCredentials { .. } => "install_credentials",
            Self::SkillsState => "skills_state",
            Self::InstallSkills { .. } => "install_skills",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelayRequestEnvelope {
    pub request_id: String,
    pub protocol_version: u32,
    pub request: RelayRequest,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelayResponseEnvelope {
    pub request_id: String,
    pub protocol_version: u32,
    #[serde(flatten)]
    pub body: RelayResponseBody,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
// This is a short-lived wire DTO. Boxing every successful response would add
// an allocation without reducing retained relay state.
#[allow(clippy::large_enum_variant)]
pub enum RelayResponseBody {
    Ok { payload: RelayResponsePayload },
    Error { error: RelayProtocolError },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum RelayResponsePayload {
    Hello {
        negotiated: u32,
        relay_version: String,
        session_id: String,
    },
    Attached {
        state: RelayOperationalState,
        events: Vec<RelayEvent>,
        through_ordinal: u64,
        through_digest: String,
    },
    Acknowledged {
        through_ordinal: u64,
        through_digest: String,
    },
    Accepted {
        command_id: String,
        ordinal: u64,
    },
    Status(RelayOperationalState),
    /// Fingerprint and freshness of a session's harness credentials. Neither
    /// value is secret.
    CredentialState {
        present: bool,
        fingerprint: String,
        freshness_epoch_ms: Option<i64>,
    },
    /// Base64 of a session's credential file. Sent only on the connection
    /// socket, never recorded.
    Credentials {
        data: String,
    },
    /// Fingerprint of a session's synced skills trees. Not secret.
    SkillsState {
        present: bool,
        fingerprint: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelayProtocolError {
    pub code: RelayErrorCode,
    pub message: String,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<RelayErrorDetail>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelayErrorCode {
    IncompatibleProtocol,
    InvalidRequest,
    InvalidState,
    Desynchronized,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RelayErrorDetail {
    Desynchronized {
        requested_after: u64,
        requested_digest: String,
        earliest_available: u64,
        earliest_digest: String,
        latest: u64,
        latest_digest: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum RelayCommand {
    Prompt {
        prompt: Vec<ContentBlock>,
    },
    RemoveQueuedPrompt {
        queued_command_id: String,
    },
    ClearQueuedPrompts,
    SetConfig {
        key: String,
        value: String,
    },
    /// Opaque ACP `session/set_mode` id. Hel uses it for harnesses whose plan
    /// mode is a session mode rather than an advertised slash command.
    SetSessionMode {
        mode_id: String,
    },
    Cancel,
    Close {
        barrier_command_id: String,
        expected: RelayCursor,
    },
    BeginCheckpoint {
        reason: Option<String>,
    },
    CompleteCheckpoint {
        barrier_command_id: String,
    },
    /// Resume ACP dispatch for a barrier whose archive is exported but not yet
    /// installed on the controller. The recovery floor deliberately stays put:
    /// only [`RelayCommand::AdvanceRecoveryFloor`] may release journal history,
    /// and only once an archive covering that history is durably installed.
    ReleaseCheckpoint {
        barrier_command_id: String,
    },
    /// Move the recovery floor to a cursor that an installed archive covers.
    /// Valid with or without an active barrier.
    AdvanceRecoveryFloor {
        through: RelayCursor,
    },
    /// Put a controller-authored line into the conversation. The agent never
    /// sees it: it explains something Hel did to the session, such as moving
    /// its checkout, to the person reading the transcript.
    RecordNotice {
        text: String,
    },
}

impl RelayCommand {
    /// Whether this command waits its turn in the durable command queue.
    fn is_queue_entry(&self) -> bool {
        matches!(self, Self::Prompt { .. } | Self::SetConfig { .. })
    }

    fn is_relay_local(&self) -> bool {
        matches!(
            self,
            Self::RemoveQueuedPrompt { .. }
                | Self::ClearQueuedPrompts
                | Self::CompleteCheckpoint { .. }
                | Self::ReleaseCheckpoint { .. }
                | Self::AdvanceRecoveryFloor { .. }
                | Self::RecordNotice { .. }
        )
    }

    fn is_effectful_acp(&self) -> bool {
        matches!(
            self,
            Self::Prompt { .. }
                | Self::SetConfig { .. }
                | Self::SetSessionMode { .. }
                | Self::Cancel
                | Self::Close { .. }
        )
    }

    pub const fn kind(&self) -> RelayCommandKind {
        match self {
            Self::Prompt { .. } => RelayCommandKind::Prompt,
            Self::RemoveQueuedPrompt { .. } => RelayCommandKind::RemoveQueuedPrompt,
            Self::ClearQueuedPrompts => RelayCommandKind::ClearQueuedPrompts,
            Self::SetConfig { .. } => RelayCommandKind::SetConfig,
            Self::SetSessionMode { .. } => RelayCommandKind::SetSessionMode,
            Self::Cancel => RelayCommandKind::Cancel,
            Self::Close { .. } => RelayCommandKind::Close,
            Self::BeginCheckpoint { .. } => RelayCommandKind::BeginCheckpoint,
            Self::CompleteCheckpoint { .. } => RelayCommandKind::CompleteCheckpoint,
            Self::ReleaseCheckpoint { .. } => RelayCommandKind::ReleaseCheckpoint,
            Self::AdvanceRecoveryFloor { .. } => RelayCommandKind::AdvanceRecoveryFloor,
            Self::RecordNotice { .. } => RelayCommandKind::RecordNotice,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelayCommandKind {
    Prompt,
    RemoveQueuedPrompt,
    ClearQueuedPrompts,
    SetConfig,
    SetSessionMode,
    Cancel,
    Close,
    BeginCheckpoint,
    CompleteCheckpoint,
    ReleaseCheckpoint,
    AdvanceRecoveryFloor,
    RecordNotice,
}

/// Payload-free queue identity exposed in attach/status responses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueuedRelayPrompt {
    pub command_id: String,
    pub created_at_ms: i64,
}

/// Payload-free active prompt identity exposed in attach/status responses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveRelayPrompt {
    pub command_id: String,
    pub created_at_ms: i64,
    pub started_at_ms: i64,
}

/// One entry of the durable command queue. Prompts and configuration changes
/// share the queue so they run in the order the user submitted them.
///
/// The payload is untagged so entries written before configuration changes
/// could be queued still load: they carry a `prompt` field and nothing else.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct StoredQueuedRelayCommand {
    command_id: String,
    #[serde(flatten)]
    payload: StoredQueuedRelayPayload,
    created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
enum StoredQueuedRelayPayload {
    Prompt { prompt: Vec<ContentBlock> },
    SetConfig { key: String, value: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct StoredActiveRelayPrompt {
    command_id: String,
    prompt: Vec<ContentBlock>,
    created_at_ms: i64,
    started_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelayExecutionState {
    Idle,
    Running,
    Closing,
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelayCursor {
    pub ordinal: u64,
    pub digest: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelayOperationalState {
    pub session_id: String,
    pub execution: RelayExecutionState,
    pub latest_ordinal: u64,
    pub latest_digest: String,
    pub acknowledged_through: u64,
    pub acknowledged_digest: String,
    /// Highest verified checkpoint frontier. Events newer than this remain in
    /// the relay journal even after acknowledgement.
    pub recovery_floor_ordinal: u64,
    pub recovery_floor_digest: String,
    pub native_session_id: Option<String>,
    pub agent_capabilities: Option<Box<AgentCapabilities>>,
    pub agent_info: Option<Implementation>,
    pub config_options: Vec<SessionConfigOption>,
    pub available_commands: Vec<AvailableCommand>,
    pub config: BTreeMap<String, String>,
    pub active_prompt: Option<ActiveRelayPrompt>,
    pub queued_prompts: Vec<QueuedRelayPrompt>,
    pub checkpoint_barrier: Option<String>,
    pub checkpoint_ready: Option<RelayCursor>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelayEvent {
    pub ordinal: u64,
    pub previous_digest: String,
    pub digest: String,
    pub recorded_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command_id: Option<String>,
    pub observation: RelayObservation,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum RelayObservation {
    AgentInitialized {
        protocol_version: AcpProtocolVersion,
        capabilities: Box<AgentCapabilities>,
        agent_info: Option<Implementation>,
    },
    SessionOpened {
        native_session_id: String,
        resumed: bool,
    },
    SessionConfigured {
        config_options: Vec<SessionConfigOption>,
    },
    SessionUpdate {
        update: Box<SessionUpdate>,
    },
    PermissionAutoApproved {
        option_id: String,
        option_name: String,
    },
    CommandQueued {
        command_id: String,
        command: RelayCommand,
        created_at_ms: i64,
    },
    CommandStarted {
        command_id: String,
        started_at_ms: i64,
    },
    CommandCompleted {
        command_id: String,
        outcome: RelayCommandOutcome,
    },
    CommandRejected {
        command_id: String,
        command: RelayCommandKind,
        message: String,
    },
    CommandInterrupted {
        command_id: String,
        command: RelayCommandKind,
        message: String,
    },
    ConfigurationUpdated {
        key: String,
        value: String,
    },
    CheckpointReady {
        command_id: String,
        through: u64,
    },
    Warning {
        message: String,
    },
    /// A controller-authored conversation line. Unlike a warning it reports
    /// something Hel did on purpose, so it reaches the transcript unadorned.
    Notice {
        message: String,
    },
    Closing,
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum RelayCommandOutcome {
    Prompt { stop_reason: String },
    Configured,
    SessionModeSet,
    Cancelled,
    Closed,
    QueueChanged { removed_command_ids: Vec<String> },
    CheckpointCompleted,
    CheckpointReleased,
    RecoveryFloorAdvanced,
    NoticeRecorded,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClaimedRelayCommand {
    pub command_id: String,
    pub accepted_ordinal: u64,
    pub command: RelayCommand,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RelayDispatchState {
    Queued,
    Pending,
    InFlight,
    Completed,
    Rejected,
    Interrupted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct RelayDispatchRecord {
    command: RelayCommand,
    state: RelayDispatchState,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct HandledRelayCommand {
    command: RelayCommand,
    accepted_ordinal: u64,
    terminal_ordinal: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RelaySnapshot {
    format_version: u32,
    session_id: String,
    execution: RelayExecutionState,
    latest_ordinal: u64,
    latest_digest: String,
    acknowledged_through: u64,
    acknowledged_digest: String,
    recovery_floor_ordinal: u64,
    recovery_floor_digest: String,
    native_session_id: Option<String>,
    agent_capabilities: Option<Box<AgentCapabilities>>,
    agent_info: Option<Implementation>,
    config_options: Vec<SessionConfigOption>,
    available_commands: Vec<AvailableCommand>,
    config: BTreeMap<String, String>,
    active_prompt: Option<StoredActiveRelayPrompt>,
    queued_prompts: Vec<StoredQueuedRelayCommand>,
    checkpoint_barrier: Option<String>,
    checkpoint_ready_through: Option<u64>,
    checkpoint_ready_digest: Option<String>,
    handled_commands: BTreeMap<String, HandledRelayCommand>,
    dispatches: BTreeMap<String, RelayDispatchRecord>,
}

impl RelaySnapshot {
    fn new(session_id: String) -> Self {
        Self {
            format_version: RELAY_STATE_VERSION,
            session_id,
            execution: RelayExecutionState::Idle,
            latest_ordinal: 0,
            latest_digest: RELAY_EVENT_GENESIS_DIGEST.to_owned(),
            acknowledged_through: 0,
            acknowledged_digest: RELAY_EVENT_GENESIS_DIGEST.to_owned(),
            recovery_floor_ordinal: 0,
            recovery_floor_digest: RELAY_EVENT_GENESIS_DIGEST.to_owned(),
            native_session_id: None,
            agent_capabilities: None,
            agent_info: None,
            config_options: Vec::new(),
            available_commands: Vec::new(),
            config: BTreeMap::new(),
            active_prompt: None,
            queued_prompts: Vec::new(),
            checkpoint_barrier: None,
            checkpoint_ready_through: None,
            checkpoint_ready_digest: None,
            handled_commands: BTreeMap::new(),
            dispatches: BTreeMap::new(),
        }
    }

    fn operational_state(&self) -> RelayOperationalState {
        RelayOperationalState {
            session_id: self.session_id.clone(),
            execution: self.execution,
            latest_ordinal: self.latest_ordinal,
            latest_digest: self.latest_digest.clone(),
            acknowledged_through: self.acknowledged_through,
            acknowledged_digest: self.acknowledged_digest.clone(),
            recovery_floor_ordinal: self.recovery_floor_ordinal,
            recovery_floor_digest: self.recovery_floor_digest.clone(),
            native_session_id: self.native_session_id.clone(),
            agent_capabilities: self.agent_capabilities.clone(),
            agent_info: self.agent_info.clone(),
            config_options: self.config_options.clone(),
            available_commands: self.available_commands.clone(),
            config: self.config.clone(),
            active_prompt: self.active_prompt.as_ref().map(|prompt| ActiveRelayPrompt {
                command_id: prompt.command_id.clone(),
                created_at_ms: prompt.created_at_ms,
                started_at_ms: prompt.started_at_ms,
            }),
            queued_prompts: self
                .queued_prompts
                .iter()
                .map(|prompt| QueuedRelayPrompt {
                    command_id: prompt.command_id.clone(),
                    created_at_ms: prompt.created_at_ms,
                })
                .collect(),
            checkpoint_barrier: self.checkpoint_barrier.clone(),
            checkpoint_ready: self
                .checkpoint_ready_through
                .zip(self.checkpoint_ready_digest.as_ref())
                .map(|(ordinal, digest)| RelayCursor {
                    ordinal,
                    digest: digest.clone(),
                }),
        }
    }

    fn retained_through(&self) -> u64 {
        self.acknowledged_through.min(self.recovery_floor_ordinal)
    }

    fn retained_digest(&self) -> &str {
        if self.acknowledged_through <= self.recovery_floor_ordinal {
            &self.acknowledged_digest
        } else {
            &self.recovery_floor_digest
        }
    }
}

/// Durable, session-side ACP store-and-forward relay.
///
/// `RelaySnapshot` is the canonical operational state. Journal spans retain
/// observations newer than the last frontier covered by both a durable
/// controller acknowledgement and a verified checkpoint.
pub struct DurableRelay {
    root: PathBuf,
    relay_version: String,
    snapshot: RelaySnapshot,
    /// Canonical, non-overlapping slices of the durable journal. Event bodies
    /// stay on disk; only enough metadata to locate a requested ordinal is
    /// retained in memory.
    journal_spans: Vec<RelayJournalSpan>,
    /// A small optimization for recent cursor validation. This is never the
    /// source of truth and is deliberately fixed-size.
    hot_events: VecDeque<RelayEvent>,
}

#[derive(Debug, Clone)]
struct RelayJournalSpan {
    path: PathBuf,
    /// Physical first ordinal in `path`; it may precede `after_ordinal` when a
    /// crash left an overlapping active/sealed copy.
    file_first_ordinal: u64,
    file_first_previous_digest: String,
    file_last_ordinal: u64,
    file_last_digest: String,
    /// This canonical span contributes only ordinals greater than this value.
    after_ordinal: u64,
}

impl DurableRelay {
    pub fn open(
        root: impl Into<PathBuf>,
        session_id: impl Into<String>,
        relay_version: impl Into<String>,
    ) -> Result<Self> {
        let root = root.into();
        let session_id = session_id.into();
        validate_identifier(&session_id, "session ID")?;
        fs::create_dir_all(root.join(RELAY_JOURNAL_DIR))
            .with_context(|| format!("create relay state directory {}", root.display()))?;

        let state_path = root.join(RELAY_STATE_FILE);
        let mut snapshot = if state_path.exists() {
            let bytes =
                fs::read(&state_path).with_context(|| format!("read {}", state_path.display()))?;
            ensure_byte_budget(bytes.len(), RELAY_SNAPSHOT_BYTE_BUDGET, "relay snapshot")?;
            let snapshot: RelaySnapshot = serde_json::from_slice(&bytes)
                .with_context(|| format!("parse {}", state_path.display()))?;
            if snapshot.format_version != RELAY_STATE_VERSION {
                bail!(
                    "relay state schema {} is incompatible with schema {RELAY_STATE_VERSION}",
                    snapshot.format_version
                );
            }
            if snapshot.session_id != session_id {
                bail!(
                    "relay state belongs to session {}, not {session_id}",
                    snapshot.session_id
                );
            }
            snapshot
        } else {
            let mut snapshot = RelaySnapshot::new(session_id);
            if let Some(restored) = read_restored_relay_seed(&root)? {
                snapshot.latest_ordinal = restored.event_frontier;
                snapshot.latest_digest = restored.event_frontier_digest.clone();
                snapshot.acknowledged_through = restored.event_frontier;
                snapshot.acknowledged_digest = restored.event_frontier_digest.clone();
                snapshot.recovery_floor_ordinal = restored.event_frontier;
                snapshot.recovery_floor_digest = restored.event_frontier_digest;
                for queued in restored.queued_prompts {
                    validate_identifier(&queued.command_id, "restored queued command ID")?;
                    if queued.content.is_empty() {
                        bail!("restored queued command {} is empty", queued.command_id);
                    }
                    if snapshot.handled_commands.contains_key(&queued.command_id) {
                        bail!(
                            "restored canonical session contains duplicate queued command {}",
                            queued.command_id
                        );
                    }
                    let payload = match queued.kind {
                        CanonicalQueuedCommandKind::Prompt => StoredQueuedRelayPayload::Prompt {
                            prompt: queued
                                .content
                                .into_iter()
                                .map(serde_json::from_value)
                                .collect::<serde_json::Result<Vec<ContentBlock>>>()
                                .with_context(|| {
                                    format!(
                                        "decode ACP content for restored queued command {}",
                                        queued.command_id
                                    )
                                })?,
                        },
                        CanonicalQueuedCommandKind::SetConfig { key, value } => {
                            StoredQueuedRelayPayload::SetConfig { key, value }
                        }
                    };
                    let command = match &payload {
                        StoredQueuedRelayPayload::Prompt { prompt } => RelayCommand::Prompt {
                            prompt: prompt.clone(),
                        },
                        StoredQueuedRelayPayload::SetConfig { key, value } => {
                            RelayCommand::SetConfig {
                                key: key.clone(),
                                value: value.clone(),
                            }
                        }
                    };
                    snapshot.handled_commands.insert(
                        queued.command_id.clone(),
                        HandledRelayCommand {
                            command: command.clone(),
                            accepted_ordinal: restored.event_frontier,
                            terminal_ordinal: None,
                        },
                    );
                    snapshot.dispatches.insert(
                        queued.command_id.clone(),
                        RelayDispatchRecord {
                            command,
                            state: RelayDispatchState::Queued,
                        },
                    );
                    snapshot.queued_prompts.push(StoredQueuedRelayCommand {
                        command_id: queued.command_id,
                        payload,
                        created_at_ms: queued.queued_at_ms,
                    });
                }
            }
            snapshot
        };

        validate_relay_snapshot_frontiers(&snapshot)?;
        let retained_through = snapshot.retained_through();
        let retained_digest = snapshot.retained_digest().to_owned();
        let snapshot_ordinal = snapshot.latest_ordinal;
        let (journal_spans, hot_events) = open_relay_journal(
            &root.join(RELAY_JOURNAL_DIR),
            retained_through,
            &retained_digest,
            snapshot_ordinal,
            &mut snapshot,
        )?;
        ensure_serialized_budget(
            &snapshot.operational_state(),
            RELAY_STATE_BYTE_BUDGET,
            "relay operational state",
        )?;

        let mut relay = Self {
            root,
            relay_version: relay_version.into(),
            snapshot,
            journal_spans,
            hot_events,
        };
        if !state_path.exists() || relay.snapshot.latest_ordinal > snapshot_ordinal {
            relay.persist_snapshot()?;
        }
        relay.adopt_unqueued_queue_commands()?;
        relay.recover_nonterminal_commands()?;
        relay.promote_next_queued_command()?;
        Ok(relay)
    }

    /// Adopt queueable commands that an earlier relay format accepted outside
    /// the durable queue. Configuration changes used to dispatch through the
    /// control path, so a command accepted just before an upgrade would
    /// otherwise stay accepted forever without ever being promoted.
    fn adopt_unqueued_queue_commands(&mut self) -> Result<()> {
        let mut adopted: Vec<(u64, StoredQueuedRelayCommand)> = Vec::new();
        for (command_id, dispatch) in &self.snapshot.dispatches {
            if dispatch.state != RelayDispatchState::Queued
                || !dispatch.command.is_queue_entry()
                || self
                    .snapshot
                    .queued_prompts
                    .iter()
                    .any(|queued| queued.command_id == *command_id)
            {
                continue;
            }
            let Some(handled) = self.snapshot.handled_commands.get(command_id) else {
                continue;
            };
            let payload = match &dispatch.command {
                RelayCommand::Prompt { prompt } => StoredQueuedRelayPayload::Prompt {
                    prompt: prompt.clone(),
                },
                RelayCommand::SetConfig { key, value } => StoredQueuedRelayPayload::SetConfig {
                    key: key.clone(),
                    value: value.clone(),
                },
                _ => continue,
            };
            adopted.push((
                handled.accepted_ordinal,
                StoredQueuedRelayCommand {
                    command_id: command_id.clone(),
                    payload,
                    created_at_ms: now_unix_millis(),
                },
            ));
        }
        if adopted.is_empty() {
            return Ok(());
        }
        let existing = std::mem::take(&mut self.snapshot.queued_prompts);
        let mut ordered: Vec<(u64, StoredQueuedRelayCommand)> = existing
            .into_iter()
            .map(|queued| {
                let accepted = self
                    .snapshot
                    .handled_commands
                    .get(&queued.command_id)
                    .map_or(0, |handled| handled.accepted_ordinal);
                (accepted, queued)
            })
            .collect();
        ordered.extend(adopted);
        ordered.sort_by_key(|(accepted, _)| *accepted);
        self.snapshot.queued_prompts = ordered.into_iter().map(|(_, queued)| queued).collect();
        self.persist_snapshot()
    }

    pub fn operational_state(&self) -> RelayOperationalState {
        self.snapshot.operational_state()
    }

    pub fn latest_ordinal(&self) -> u64 {
        self.snapshot.latest_ordinal
    }

    pub fn latest_digest(&self) -> &str {
        &self.snapshot.latest_digest
    }

    pub fn acknowledged_through(&self) -> u64 {
        self.snapshot.acknowledged_through
    }

    pub fn acknowledged_digest(&self) -> &str {
        &self.snapshot.acknowledged_digest
    }

    pub fn events_after(&self, after_ordinal: u64, after_digest: &str) -> Result<Vec<RelayEvent>> {
        self.validate_replay_cursor(after_ordinal, after_digest)?;
        self.read_events_after(after_ordinal, usize::MAX)
            .map(|page| page.events)
    }

    #[cfg(test)]
    fn retained_event_body_count(&self) -> usize {
        self.hot_events.len()
    }

    pub fn handle(&mut self, envelope: RelayRequestEnvelope) -> RelayResponseEnvelope {
        let request_id = envelope.request_id.clone();
        let body = self
            .handle_inner(&envelope)
            .unwrap_or_else(|error| RelayResponseBody::Error {
                error: RelayProtocolError {
                    code: RelayErrorCode::Internal,
                    message: format!("{error:#}"),
                    retryable: true,
                    detail: None,
                },
            });
        let protocol_version = match &body {
            RelayResponseBody::Ok {
                payload: RelayResponsePayload::Hello { negotiated, .. },
            } => *negotiated,
            _ => envelope.protocol_version,
        };
        RelayResponseEnvelope {
            request_id,
            protocol_version,
            body,
        }
    }

    fn handle_inner(&mut self, envelope: &RelayRequestEnvelope) -> Result<RelayResponseBody> {
        if envelope.request_id.trim().is_empty() || envelope.request_id.len() > 256 {
            return Ok(relay_error(
                RelayErrorCode::InvalidRequest,
                "request_id is required",
                false,
                None,
            ));
        }
        if let RelayRequest::Hello { supported, .. } = &envelope.request {
            let Some(negotiated) = RelayVersionRange::CURRENT.negotiate(*supported) else {
                return Ok(relay_error(
                    RelayErrorCode::IncompatibleProtocol,
                    format!(
                        "controller supports {}-{}, relay requires protocol {RELAY_PROTOCOL_VERSION}",
                        supported.min, supported.max
                    ),
                    false,
                    None,
                ));
            };
            return Ok(RelayResponseBody::Ok {
                payload: RelayResponsePayload::Hello {
                    negotiated,
                    relay_version: self.relay_version.clone(),
                    session_id: self.snapshot.session_id.clone(),
                },
            });
        }
        if envelope.protocol_version != RELAY_PROTOCOL_VERSION {
            return Ok(relay_error(
                RelayErrorCode::IncompatibleProtocol,
                format!(
                    "request uses protocol {}, relay requires protocol {RELAY_PROTOCOL_VERSION}",
                    envelope.protocol_version
                ),
                false,
                None,
            ));
        }

        let payload = match &envelope.request {
            RelayRequest::Hello { .. } => unreachable!(),
            RelayRequest::Attach {
                after_ordinal,
                after_digest,
            } => match self.attach(*after_ordinal, after_digest)? {
                Ok(payload) => payload,
                Err(error) => return Ok(RelayResponseBody::Error { error }),
            },
            RelayRequest::Acknowledge {
                through_ordinal,
                through_digest,
            } => match self.acknowledge(*through_ordinal, through_digest)? {
                Ok(payload) => payload,
                Err(error) => return Ok(RelayResponseBody::Error { error }),
            },
            RelayRequest::Submit {
                command_id,
                command,
            } => match self.submit_command(command_id, command.clone())? {
                Ok(payload) => payload,
                Err(error) => return Ok(RelayResponseBody::Error { error }),
            },
            RelayRequest::Status => {
                let state = self.operational_state();
                ensure_serialized_budget(
                    &state,
                    RELAY_STATE_BYTE_BUDGET,
                    "relay operational state",
                )?;
                RelayResponsePayload::Status(state)
            }
            RelayRequest::CredentialState
            | RelayRequest::ReadCredentials
            | RelayRequest::InstallCredentials { .. }
            | RelayRequest::SkillsState
            | RelayRequest::InstallSkills { .. } => {
                return Ok(relay_error(
                    RelayErrorCode::InvalidState,
                    "credential and skills requests must be handled by the live relay transport",
                    false,
                    None,
                ));
            }
        };
        Ok(RelayResponseBody::Ok { payload })
    }

    fn attach(
        &mut self,
        after_ordinal: u64,
        after_digest: &str,
    ) -> Result<std::result::Result<RelayResponsePayload, RelayProtocolError>> {
        if let Err(error) = self.validate_replay_cursor(after_ordinal, after_digest) {
            return Ok(Err(relay_protocol_error(
                RelayErrorCode::Desynchronized,
                error.to_string(),
                false,
                Some(self.desynchronized_detail(after_ordinal, after_digest)),
            )));
        }
        let page = self.read_events_after(after_ordinal, RELAY_REPLAY_BYTE_BUDGET)?;
        let state = self.operational_state();
        ensure_serialized_budget(&state, RELAY_STATE_BYTE_BUDGET, "relay operational state")?;
        Ok(Ok(RelayResponsePayload::Attached {
            state,
            events: page.events,
            through_ordinal: page.through_ordinal,
            through_digest: page.through_digest,
        }))
    }

    fn acknowledge(
        &mut self,
        through_ordinal: u64,
        through_digest: &str,
    ) -> Result<std::result::Result<RelayResponsePayload, RelayProtocolError>> {
        if let Err(error) = self.validate_replay_cursor(through_ordinal, through_digest) {
            return Ok(Err(relay_protocol_error(
                RelayErrorCode::Desynchronized,
                error.to_string(),
                false,
                Some(self.desynchronized_detail(through_ordinal, through_digest)),
            )));
        }
        if through_ordinal > self.snapshot.acknowledged_through {
            let mut next_snapshot = self.snapshot.clone();
            next_snapshot.acknowledged_through = through_ordinal;
            next_snapshot.acknowledged_digest = through_digest.to_owned();
            // The acknowledgement becomes durable before any journal GC.
            persist_relay_snapshot(&self.root, &next_snapshot)?;
            self.snapshot = next_snapshot;
        }
        // An earlier attempt may have durably advanced the ACK and then
        // failed while rewriting or pruning history. Retrying the exact ACK
        // must retry that cleanup instead of treating it as wholly complete.
        if through_ordinal == self.snapshot.acknowledged_through {
            self.garbage_collect_relay_history()?;
        }
        Ok(Ok(RelayResponsePayload::Acknowledged {
            through_ordinal: self.snapshot.acknowledged_through,
            through_digest: self.snapshot.acknowledged_digest.clone(),
        }))
    }

    fn submit_command(
        &mut self,
        command_id: &str,
        command: RelayCommand,
    ) -> Result<std::result::Result<RelayResponsePayload, RelayProtocolError>> {
        if let Err(error) =
            ensure_serialized_budget(&command, RELAY_COMMAND_BYTE_BUDGET, "relay command")
        {
            return Ok(Err(relay_protocol_error(
                RelayErrorCode::InvalidRequest,
                error.to_string(),
                false,
                None,
            )));
        }
        if validate_identifier(command_id, "command ID").is_err() {
            return Ok(Err(relay_protocol_error(
                RelayErrorCode::InvalidRequest,
                "invalid command ID",
                false,
                None,
            )));
        }
        if self.snapshot.handled_commands.contains_key(command_id) {
            let accepted_ordinal = {
                let handled = &self.snapshot.handled_commands[command_id];
                if handled.command != command {
                    return Ok(Err(relay_protocol_error(
                        RelayErrorCode::InvalidRequest,
                        "command ID was already used for a different command",
                        false,
                        None,
                    )));
                }
                handled.accepted_ordinal
            };
            // A journal append can succeed before snapshot persistence reports
            // an error. Retrying the durable command must resume any remaining
            // relay-local transition instead of merely echoing its first ACK.
            if command.is_relay_local() {
                self.finish_relay_local_command(command_id)?;
            }
            return Ok(Ok(RelayResponsePayload::Accepted {
                command_id: command_id.to_owned(),
                ordinal: accepted_ordinal,
            }));
        }
        let pending_close_barrier = self.pending_close_barrier_id().map(str::to_owned);
        let completes_pending_close = pending_close_barrier.as_deref().is_some_and(|barrier| {
            matches!(
                &command,
                RelayCommand::CompleteCheckpoint { barrier_command_id }
                    if barrier_command_id == barrier
            )
        });
        if pending_close_barrier.is_some() && !completes_pending_close {
            return Ok(Err(relay_protocol_error(
                RelayErrorCode::InvalidState,
                "relay session is sealed for close",
                false,
                None,
            )));
        }
        if matches!(
            self.snapshot.execution,
            RelayExecutionState::Closing | RelayExecutionState::Closed
        ) && !completes_pending_close
        {
            return Ok(Err(relay_protocol_error(
                RelayErrorCode::InvalidState,
                "relay session is closing",
                false,
                None,
            )));
        }
        if let RelayCommand::Prompt { prompt } = &command
            && prompt.is_empty()
        {
            return Ok(Err(relay_protocol_error(
                RelayErrorCode::InvalidRequest,
                "prompt is empty",
                false,
                None,
            )));
        }
        if let RelayCommand::SetConfig { key, value } = &command
            && (key.trim().is_empty() || value.trim().is_empty())
        {
            return Ok(Err(relay_protocol_error(
                RelayErrorCode::InvalidRequest,
                "configuration key and value are required",
                false,
                None,
            )));
        }
        if let RelayCommand::RecordNotice { text } = &command
            && text.trim().is_empty()
        {
            return Ok(Err(relay_protocol_error(
                RelayErrorCode::InvalidRequest,
                "notice text is required",
                false,
                None,
            )));
        }
        if let RelayCommand::Cancel = command
            && self.snapshot.active_prompt.is_none()
        {
            return Ok(Err(relay_protocol_error(
                RelayErrorCode::InvalidState,
                "there is no active prompt to cancel",
                false,
                None,
            )));
        }
        if let RelayCommand::RemoveQueuedPrompt { queued_command_id } = &command
            && !self
                .snapshot
                .queued_prompts
                .iter()
                .any(|queued| queued.command_id == *queued_command_id)
        {
            return Ok(Err(relay_protocol_error(
                RelayErrorCode::InvalidRequest,
                "unknown queued prompt",
                false,
                None,
            )));
        }
        if let RelayCommand::CompleteCheckpoint { barrier_command_id }
        | RelayCommand::ReleaseCheckpoint { barrier_command_id } = &command
            && (self.snapshot.checkpoint_barrier.as_deref() != Some(barrier_command_id)
                || self.snapshot.checkpoint_ready_through.is_none())
        {
            return Ok(Err(relay_protocol_error(
                RelayErrorCode::InvalidState,
                "checkpoint barrier is not active",
                false,
                None,
            )));
        }
        if let RelayCommand::AdvanceRecoveryFloor { through } = &command
            && let Some(message) = self.recovery_floor_rejection(through)?
        {
            return Ok(Err(relay_protocol_error(
                RelayErrorCode::InvalidState,
                message,
                false,
                None,
            )));
        }
        if let RelayCommand::Close {
            barrier_command_id,
            expected,
        } = &command
        {
            let ready = self
                .snapshot
                .checkpoint_ready_through
                .zip(self.snapshot.checkpoint_ready_digest.as_ref());
            let exact_cut = self.snapshot.checkpoint_barrier.as_deref() == Some(barrier_command_id)
                && ready.is_some_and(|(ordinal, digest)| {
                    ordinal == expected.ordinal && digest == &expected.digest
                })
                && self.snapshot.latest_ordinal == expected.ordinal
                && self.snapshot.latest_digest == expected.digest;
            if !exact_cut {
                return Ok(Err(relay_protocol_error(
                    RelayErrorCode::InvalidState,
                    "close does not match the current checkpoint cut",
                    false,
                    None,
                )));
            }
        }

        let created_at_ms = now_unix_millis();
        let accepted_ordinal = self.append_relay_event(
            Some(command_id),
            RelayObservation::CommandQueued {
                command_id: command_id.to_owned(),
                command: command.clone(),
                created_at_ms,
            },
        )?;

        if command.is_relay_local() {
            self.finish_relay_local_command(command_id)?;
        }
        self.promote_next_queued_command()?;
        Ok(Ok(RelayResponsePayload::Accepted {
            command_id: command_id.to_owned(),
            ordinal: accepted_ordinal,
        }))
    }

    /// Why a recovery floor move must be refused, or `None` when it is valid.
    ///
    /// Journal garbage collection retains history through this ordinal, so a
    /// cursor off this relay's own event chain would discard events that no
    /// installed archive covers. The floor therefore only moves forward, only
    /// within the durable frontier, and only to a matching digest.
    fn recovery_floor_rejection(&self, through: &RelayCursor) -> Result<Option<String>> {
        if through.ordinal > self.snapshot.latest_ordinal {
            return Ok(Some(format!(
                "recovery floor {} is ahead of the relay frontier {}",
                through.ordinal, self.snapshot.latest_ordinal
            )));
        }
        if through.ordinal < self.snapshot.recovery_floor_ordinal {
            return Ok(Some(format!(
                "recovery floor {} is behind the current floor {}",
                through.ordinal, self.snapshot.recovery_floor_ordinal
            )));
        }
        if validate_relay_digest(&through.digest, "recovery floor digest").is_err() {
            return Ok(Some("recovery floor digest is malformed".to_owned()));
        }
        let Some(expected) = self.digest_at(through.ordinal)? else {
            return Ok(Some(format!(
                "relay digest is unavailable at event {}",
                through.ordinal
            )));
        };
        if through.digest != expected {
            return Ok(Some(format!(
                "recovery floor digest does not match the relay event chain at event {}",
                through.ordinal
            )));
        }
        Ok(None)
    }

    /// Finish a relay-local command from its durable dispatch record. This is
    /// deliberately restartable: every intermediate mutation is an event, so
    /// reopening the relay can resume after any append without duplicating or
    /// skipping the remaining queue transition.
    fn finish_relay_local_command(&mut self, command_id: &str) -> Result<()> {
        let dispatch = self
            .snapshot
            .dispatches
            .get(command_id)
            .with_context(|| format!("unknown relay-local command {command_id}"))?;
        if !dispatch.command.is_relay_local() {
            bail!("command {command_id} is not relay-local");
        }
        let command = dispatch.command.clone();
        let state = dispatch.state;
        if matches!(
            state,
            RelayDispatchState::Completed
                | RelayDispatchState::Rejected
                | RelayDispatchState::Interrupted
        ) {
            if state == RelayDispatchState::Completed && releases_history(&command) {
                // The completion event may be durable even if its following
                // journal GC reported a transient persistence error.
                self.garbage_collect_relay_history()?;
            }
            return Ok(());
        }
        // Recorded before the command starts, and unguarded by dispatch state:
        // a retry that repeats this append is harmless because the projection
        // keys the transcript line on this command, not on the event ordinal.
        if let RelayCommand::RecordNotice { text } = &command {
            let message = text.clone();
            self.append_relay_event(Some(command_id), RelayObservation::Notice { message })?;
        }
        if state == RelayDispatchState::Queued {
            self.append_relay_event(
                Some(command_id),
                RelayObservation::CommandStarted {
                    command_id: command_id.to_owned(),
                    started_at_ms: now_unix_millis(),
                },
            )?;
        }

        let removed_command_ids = match &command {
            RelayCommand::RemoveQueuedPrompt { queued_command_id } => {
                if self
                    .snapshot
                    .queued_prompts
                    .iter()
                    .any(|queued| queued.command_id == *queued_command_id)
                {
                    vec![queued_command_id.clone()]
                } else {
                    Vec::new()
                }
            }
            RelayCommand::ClearQueuedPrompts => self
                .snapshot
                .queued_prompts
                .iter()
                .map(|queued| queued.command_id.clone())
                .collect(),
            _ => Vec::new(),
        };
        let outcome = match &command {
            RelayCommand::CompleteCheckpoint { .. } => RelayCommandOutcome::CheckpointCompleted,
            RelayCommand::ReleaseCheckpoint { .. } => RelayCommandOutcome::CheckpointReleased,
            RelayCommand::AdvanceRecoveryFloor { .. } => RelayCommandOutcome::RecoveryFloorAdvanced,
            RelayCommand::RecordNotice { .. } => RelayCommandOutcome::NoticeRecorded,
            _ => RelayCommandOutcome::QueueChanged {
                removed_command_ids,
            },
        };
        self.append_relay_event(
            Some(command_id),
            RelayObservation::CommandCompleted {
                command_id: command_id.to_owned(),
                outcome,
            },
        )?;
        if releases_history(&command) {
            self.garbage_collect_relay_history()?;
        }
        Ok(())
    }

    /// Durably claim commands before they are handed to the live ACP driver.
    /// A checkpoint barrier is admitted only after every previously-started
    /// ACP effect is terminal. Once admitted, it is the sole claimable command
    /// and all ACP dispatch remains frozen until that exact barrier completes.
    pub fn claim_pending_commands(
        &mut self,
        acp_session_configured: bool,
    ) -> Result<Vec<ClaimedRelayCommand>> {
        self.claim_pending_commands_up_to(acp_session_configured, usize::MAX)
    }

    /// Claim no more work than the caller can hand to ACP without waiting.
    /// Keeping the durable in-flight batch within the transport's available
    /// capacity prevents command backpressure from blocking the coordinator
    /// that must drain ACP's bounded event channel.
    pub fn claim_pending_commands_up_to(
        &mut self,
        acp_session_configured: bool,
        maximum: usize,
    ) -> Result<Vec<ClaimedRelayCommand>> {
        if !acp_session_configured || maximum == 0 {
            return Ok(Vec::new());
        }
        self.promote_next_queued_command()?;
        if self.snapshot.checkpoint_barrier.is_none() {
            if let Some((barrier_id, barrier_ordinal)) = self.next_queued_checkpoint() {
                let mut earlier_controls = self.queued_controls_before(barrier_ordinal);
                if !earlier_controls.is_empty() {
                    earlier_controls.truncate(maximum);
                    self.start_queued_controls(earlier_controls)?;
                } else if !self.effectful_command_in_progress() {
                    self.append_relay_event(
                        Some(&barrier_id),
                        RelayObservation::CommandStarted {
                            command_id: barrier_id.clone(),
                            started_at_ms: now_unix_millis(),
                        },
                    )?;
                }
            } else {
                let mut controls = self.queued_controls_before(u64::MAX);
                controls.truncate(maximum);
                self.start_queued_controls(controls)?;
            }
        }

        let active_barrier = self.snapshot.checkpoint_barrier.as_deref();
        let mut claimable: Vec<(u64, String)> = self
            .snapshot
            .dispatches
            .iter()
            .filter_map(|(command_id, dispatch)| {
                if dispatch.state != RelayDispatchState::Pending {
                    return None;
                }
                match active_barrier {
                    Some(barrier_id) if command_id == barrier_id => self
                        .snapshot
                        .handled_commands
                        .get(command_id)
                        .map(|handled| (handled.accepted_ordinal, command_id.clone())),
                    Some(_) => None,
                    None => self
                        .snapshot
                        .handled_commands
                        .get(command_id)
                        .map(|handled| (handled.accepted_ordinal, command_id.clone())),
                }
            })
            .collect();
        claimable.sort_by_key(|(accepted_ordinal, _)| *accepted_ordinal);
        claimable.truncate(maximum);
        let mut claimed = Vec::with_capacity(claimable.len());
        let mut next_snapshot = self.snapshot.clone();
        for (accepted_ordinal, command_id) in claimable {
            let dispatch = next_snapshot
                .dispatches
                .get_mut(&command_id)
                .expect("claimable command disappeared");
            dispatch.state = RelayDispatchState::InFlight;
            claimed.push(ClaimedRelayCommand {
                command_id,
                accepted_ordinal,
                command: dispatch.command.clone(),
            });
        }
        if !claimed.is_empty() {
            persist_relay_snapshot(&self.root, &next_snapshot)?;
            self.snapshot = next_snapshot;
        }
        Ok(claimed)
    }

    fn start_queued_controls(&mut self, command_ids: Vec<String>) -> Result<()> {
        for command_id in command_ids {
            self.append_relay_event(
                Some(&command_id),
                RelayObservation::CommandStarted {
                    command_id: command_id.clone(),
                    started_at_ms: now_unix_millis(),
                },
            )?;
        }
        Ok(())
    }

    fn queued_controls_before(&self, before_ordinal: u64) -> Vec<String> {
        let active_prompt_ordinal = self.snapshot.active_prompt.as_ref().and_then(|active| {
            self.snapshot
                .handled_commands
                .get(&active.command_id)
                .map(|handled| handled.accepted_ordinal)
        });
        let mut controls: Vec<(u64, String)> = self
            .snapshot
            .dispatches
            .iter()
            .filter_map(|(command_id, dispatch)| {
                if dispatch.state != RelayDispatchState::Queued
                    || !dispatch.command.is_effectful_acp()
                    || dispatch.command.is_queue_entry()
                {
                    return None;
                }
                let accepted = self
                    .snapshot
                    .handled_commands
                    .get(command_id)?
                    .accepted_ordinal;
                // Preserve controls accepted before the active prompt (they
                // must reach ACP first), but keep later controls queued until
                // that prompt finishes. Cancel is the one ACP control that
                // deliberately bypasses a running prompt.
                if active_prompt_ordinal.is_some_and(|prompt| accepted > prompt)
                    && !matches!(dispatch.command, RelayCommand::Cancel)
                {
                    return None;
                }
                if accepted < before_ordinal {
                    Some((accepted, command_id.clone()))
                } else {
                    None
                }
            })
            .collect();
        controls.sort_by_key(|(ordinal, _)| *ordinal);
        controls
            .into_iter()
            .map(|(_, command_id)| command_id)
            .collect()
    }

    fn effectful_command_in_progress(&self) -> bool {
        self.snapshot.active_prompt.is_some()
            || self.snapshot.dispatches.values().any(|dispatch| {
                dispatch.command.is_effectful_acp()
                    && matches!(
                        dispatch.state,
                        RelayDispatchState::Pending | RelayDispatchState::InFlight
                    )
            })
    }

    fn next_queued_checkpoint(&self) -> Option<(String, u64)> {
        self.snapshot
            .dispatches
            .iter()
            .filter(|(_, dispatch)| {
                dispatch.state == RelayDispatchState::Queued
                    && matches!(dispatch.command, RelayCommand::BeginCheckpoint { .. })
            })
            .filter_map(|(command_id, _)| {
                self.snapshot
                    .handled_commands
                    .get(command_id)
                    .map(|handled| (command_id.clone(), handled.accepted_ordinal))
            })
            .min_by_key(|(_, accepted)| *accepted)
    }

    pub fn record_observation(&mut self, observation: RelayObservation) -> Result<u64> {
        self.append_relay_event(None, observation)
    }

    pub fn record_session_update(&mut self, update: SessionUpdate) -> Result<u64> {
        self.record_observation(RelayObservation::SessionUpdate {
            update: Box::new(update),
        })
    }

    pub fn record_command_completed(
        &mut self,
        command_id: &str,
        outcome: RelayCommandOutcome,
    ) -> Result<u64> {
        self.require_in_flight(command_id)?;
        if matches!(
            self.snapshot.dispatches[command_id].command,
            RelayCommand::BeginCheckpoint { .. }
        ) {
            bail!("checkpoint barriers complete through record_checkpoint_ready");
        }
        let ordinal = self.append_relay_event(
            Some(command_id),
            RelayObservation::CommandCompleted {
                command_id: command_id.to_owned(),
                outcome,
            },
        )?;
        self.promote_next_queued_command()?;
        Ok(ordinal)
    }

    pub fn record_command_rejected(
        &mut self,
        command_id: &str,
        message: impl Into<String>,
    ) -> Result<u64> {
        self.require_dispatch(command_id)?;
        let command = self.snapshot.dispatches[command_id].command.kind();
        let ordinal = self.append_relay_event(
            Some(command_id),
            RelayObservation::CommandRejected {
                command_id: command_id.to_owned(),
                command,
                message: message.into(),
            },
        )?;
        self.promote_next_queued_command()?;
        Ok(ordinal)
    }

    pub fn record_command_interrupted(
        &mut self,
        command_id: &str,
        message: impl Into<String>,
    ) -> Result<u64> {
        self.require_dispatch(command_id)?;
        let command = self.snapshot.dispatches[command_id].command.kind();
        let ordinal = self.append_relay_event(
            Some(command_id),
            RelayObservation::CommandInterrupted {
                command_id: command_id.to_owned(),
                command,
                message: message.into(),
            },
        )?;
        self.promote_next_queued_command()?;
        Ok(ordinal)
    }

    pub fn record_checkpoint_ready(&mut self, command_id: &str) -> Result<u64> {
        self.require_in_flight(command_id)?;
        if self.snapshot.checkpoint_barrier.as_deref() != Some(command_id) {
            bail!("checkpoint barrier {command_id} is not active");
        }
        if self.snapshot.checkpoint_ready_through.is_some() {
            bail!("checkpoint barrier {command_id} is already ready");
        }
        if !matches!(
            self.snapshot.dispatches[command_id].command,
            RelayCommand::BeginCheckpoint { .. }
        ) {
            bail!("command {command_id} is not a checkpoint barrier");
        }
        let through = self
            .snapshot
            .latest_ordinal
            .checked_add(1)
            .ok_or_else(|| anyhow!("relay event ordinal exhausted"))?;
        self.append_relay_event(
            Some(command_id),
            RelayObservation::CheckpointReady {
                command_id: command_id.to_owned(),
                through,
            },
        )
    }

    /// Release checkpoint barriers owned by a controller connection that
    /// disappeared. The runtime calls this when that connection drops so an
    /// offline prompt queue can never remain paused indefinitely.
    pub fn cancel_checkpoint_barrier_on_disconnect(
        &mut self,
        command_id: &str,
    ) -> Result<Option<u64>> {
        let Some(dispatch) = self.snapshot.dispatches.get(command_id) else {
            return Ok(None);
        };
        if !matches!(dispatch.command, RelayCommand::BeginCheckpoint { .. })
            || !matches!(
                dispatch.state,
                RelayDispatchState::Queued
                    | RelayDispatchState::Pending
                    | RelayDispatchState::InFlight
            )
        {
            return Ok(None);
        }
        let ordinal = self.append_relay_event(
            Some(command_id),
            RelayObservation::CommandInterrupted {
                command_id: command_id.to_owned(),
                command: RelayCommandKind::BeginCheckpoint,
                message: "checkpoint barrier cancelled because its controller disconnected"
                    .to_owned(),
            },
        )?;
        self.promote_next_queued_command()?;
        Ok(Some(ordinal))
    }

    fn require_dispatch(&self, command_id: &str) -> Result<()> {
        if !self.snapshot.dispatches.contains_key(command_id) {
            bail!("unknown relay command {command_id}");
        }
        Ok(())
    }

    fn require_in_flight(&self, command_id: &str) -> Result<()> {
        let Some(dispatch) = self.snapshot.dispatches.get(command_id) else {
            bail!("unknown relay command {command_id}");
        };
        if dispatch.state != RelayDispatchState::InFlight {
            bail!("relay command {command_id} is not in flight");
        }
        Ok(())
    }

    fn close_requested(&self) -> bool {
        self.pending_close_barrier_id().is_some()
    }

    fn pending_close_barrier_id(&self) -> Option<&str> {
        self.snapshot.dispatches.values().find_map(|dispatch| {
            if matches!(
                dispatch.state,
                RelayDispatchState::Completed
                    | RelayDispatchState::Rejected
                    | RelayDispatchState::Interrupted
            ) {
                return None;
            }
            match &dispatch.command {
                RelayCommand::Close {
                    barrier_command_id, ..
                } => Some(barrier_command_id.as_str()),
                _ => None,
            }
        })
    }

    fn validate_replay_cursor(&self, after_ordinal: u64, after_digest: &str) -> Result<()> {
        let retained_through = self.snapshot.retained_through();
        if after_ordinal < retained_through {
            bail!(
                "event {after_ordinal} is no longer available; relay retained events after {}",
                retained_through
            );
        }
        if after_ordinal > self.snapshot.latest_ordinal {
            bail!(
                "event {after_ordinal} is newer than relay frontier {}",
                self.snapshot.latest_ordinal
            );
        }
        validate_relay_digest(after_digest, "event cursor digest")?;
        let expected = self
            .digest_at(after_ordinal)?
            .ok_or_else(|| anyhow!("relay digest missing at event {after_ordinal}"))?;
        if after_digest != expected {
            bail!("event {after_ordinal} digest does not match the relay event chain");
        }
        Ok(())
    }

    fn digest_at(&self, ordinal: u64) -> Result<Option<String>> {
        if ordinal == 0 {
            return Ok(Some(RELAY_EVENT_GENESIS_DIGEST.to_owned()));
        }
        if ordinal == self.snapshot.latest_ordinal {
            return Ok(Some(self.snapshot.latest_digest.clone()));
        }
        if ordinal == self.snapshot.acknowledged_through {
            return Ok(Some(self.snapshot.acknowledged_digest.clone()));
        }
        if ordinal == self.snapshot.recovery_floor_ordinal {
            return Ok(Some(self.snapshot.recovery_floor_digest.clone()));
        }
        if let Some(span) = self.journal_spans.iter().find(|span| {
            span.file_first_ordinal.checked_sub(1) == Some(ordinal) && ordinal >= span.after_ordinal
        }) {
            return Ok(Some(span.file_first_previous_digest.clone()));
        }
        if let Some(event) = self
            .hot_events
            .iter()
            .find(|event| event.ordinal == ordinal)
        {
            return Ok(Some(event.digest.clone()));
        }
        let Some(span) = self
            .journal_spans
            .iter()
            .find(|span| ordinal > span.after_ordinal && ordinal <= span.file_last_ordinal)
        else {
            return Ok(None);
        };
        let mut digest = None;
        visit_relay_journal_file(&span.path, false, |event, _| {
            if event.ordinal == ordinal {
                digest = Some(event.digest.clone());
                return Ok(ControlFlow::Break(()));
            }
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(digest)
    }

    fn read_events_after(&self, after_ordinal: u64, byte_budget: usize) -> Result<RelayEventPage> {
        let mut events = Vec::new();
        let mut used = 0_usize;
        let mut through_ordinal = after_ordinal;
        let mut through_digest = self
            .digest_at(after_ordinal)?
            .ok_or_else(|| anyhow!("relay digest missing at event {after_ordinal}"))?;
        let mut page_full = false;

        for span in &self.journal_spans {
            if page_full || span.file_last_ordinal <= through_ordinal {
                continue;
            }
            visit_relay_journal_file(&span.path, false, |event, encoded_len| {
                if event.ordinal <= span.after_ordinal || event.ordinal <= through_ordinal {
                    return Ok(ControlFlow::Continue(()));
                }
                if event.ordinal
                    != through_ordinal
                        .checked_add(1)
                        .ok_or_else(|| anyhow!("relay event ordinal exhausted"))?
                {
                    bail!(
                        "relay journal page has a gap after event {through_ordinal}: found {}",
                        event.ordinal
                    );
                }
                if !events.is_empty() && used.saturating_add(encoded_len) > byte_budget {
                    page_full = true;
                    return Ok(ControlFlow::Break(()));
                }
                used = used.saturating_add(encoded_len);
                through_ordinal = event.ordinal;
                through_digest = event.digest.clone();
                events.push(event);
                Ok(ControlFlow::Continue(()))
            })?;
        }
        if !page_full {
            through_ordinal = self.snapshot.latest_ordinal;
            through_digest = self.snapshot.latest_digest.clone();
        }
        Ok(RelayEventPage {
            events,
            through_ordinal,
            through_digest,
        })
    }

    fn desynchronized_detail(
        &self,
        requested_after: u64,
        requested_digest: &str,
    ) -> RelayErrorDetail {
        RelayErrorDetail::Desynchronized {
            requested_after,
            requested_digest: requested_digest.to_owned(),
            earliest_available: self.snapshot.retained_through(),
            earliest_digest: self.snapshot.retained_digest().to_owned(),
            latest: self.snapshot.latest_ordinal,
            latest_digest: self.snapshot.latest_digest.clone(),
        }
    }

    /// Start the head of the durable command queue once the relay is idle.
    /// Entries run strictly one at a time, in the order they were accepted.
    fn promote_next_queued_command(&mut self) -> Result<Option<u64>> {
        if self.snapshot.active_prompt.is_some()
            || self.promoted_config_in_progress()
            || self.snapshot.checkpoint_barrier.is_some()
            || self.snapshot.execution != RelayExecutionState::Idle
            || self.pending_checkpoint_barrier()
            || self.close_requested()
        {
            return Ok(None);
        }
        let Some(queued) = self.snapshot.queued_prompts.first().cloned() else {
            return Ok(None);
        };
        let ordinal = self.append_relay_event(
            Some(&queued.command_id),
            RelayObservation::CommandStarted {
                command_id: queued.command_id.clone(),
                started_at_ms: now_unix_millis(),
            },
        )?;
        Ok(Some(ordinal))
    }

    /// A promoted configuration change leaves execution idle while it reaches
    /// ACP, so the queue needs its own guard to stay sequential. Completion,
    /// rejection, and interruption all promote the next entry.
    fn promoted_config_in_progress(&self) -> bool {
        self.snapshot.dispatches.values().any(|dispatch| {
            matches!(dispatch.command, RelayCommand::SetConfig { .. })
                && matches!(
                    dispatch.state,
                    RelayDispatchState::Pending | RelayDispatchState::InFlight
                )
        })
    }

    fn pending_checkpoint_barrier(&self) -> bool {
        self.snapshot.dispatches.values().any(|dispatch| {
            matches!(dispatch.command, RelayCommand::BeginCheckpoint { .. })
                && matches!(
                    dispatch.state,
                    RelayDispatchState::Queued
                        | RelayDispatchState::Pending
                        | RelayDispatchState::InFlight
                )
        })
    }

    fn append_relay_event(
        &mut self,
        command_id: Option<&str>,
        observation: RelayObservation,
    ) -> Result<u64> {
        let ordinal = self
            .snapshot
            .latest_ordinal
            .checked_add(1)
            .ok_or_else(|| anyhow!("relay event ordinal exhausted"))?;
        // Clamp before digesting so the recorded digest covers what was
        // actually written, and so recording an observation cannot fail on
        // size alone.
        let observation = clamp_observation(
            observation,
            RELAY_EVENT_BYTE_BUDGET - RELAY_EVENT_ENVELOPE_RESERVE,
        )?;
        let event = RelayEvent {
            ordinal,
            previous_digest: self.snapshot.latest_digest.clone(),
            digest: String::new(),
            recorded_at_ms: now_unix_millis(),
            command_id: command_id.map(str::to_owned),
            observation,
        };
        let event = RelayEvent {
            digest: relay_event_digest(&event)?,
            ..event
        };
        ensure_serialized_budget(&event, RELAY_EVENT_BYTE_BUDGET, "relay event")?;
        let mut next_snapshot = self.snapshot.clone();
        apply_relay_event(&mut next_snapshot, &event)?;
        ensure_serialized_budget(&next_snapshot, RELAY_SNAPSHOT_BYTE_BUDGET, "relay snapshot")?;
        ensure_serialized_budget(
            &next_snapshot.operational_state(),
            RELAY_STATE_BYTE_BUDGET,
            "relay operational state",
        )?;
        self.seal_active_segment_if_needed()?;
        let journal = self.root.join(RELAY_JOURNAL_DIR);
        let path = journal.join(RELAY_ACTIVE_SEGMENT);
        let created_active_segment = !path.exists();
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("open {}", path.display()))?;
        serde_json::to_writer(&mut file, &event)?;
        file.write_all(b"\n")?;
        file.sync_data()?;
        if created_active_segment {
            sync_directory(&journal)?;
        }

        self.snapshot = next_snapshot;
        self.record_journal_append(&path, &event);
        self.push_hot_event(event);
        self.persist_snapshot()?;
        Ok(ordinal)
    }

    fn persist_snapshot(&self) -> Result<()> {
        persist_relay_snapshot(&self.root, &self.snapshot)
    }

    fn seal_active_segment_if_needed(&mut self) -> Result<()> {
        let journal = self.root.join(RELAY_JOURNAL_DIR);
        let active = journal.join(RELAY_ACTIVE_SEGMENT);
        if !active.exists() || active.metadata()?.len() < RELAY_SEGMENT_BYTE_LIMIT {
            return Ok(());
        }
        let Some(index) = self
            .journal_spans
            .iter()
            .position(|span| span.path == active)
        else {
            bail!("active relay segment has data but no journal metadata");
        };
        seal_active_relay_segment(&journal, &mut self.journal_spans[index])
    }

    fn record_journal_append(&mut self, active: &Path, event: &RelayEvent) {
        if let Some(span) = self
            .journal_spans
            .last_mut()
            .filter(|span| span.path == active)
        {
            debug_assert_eq!(span.file_last_ordinal + 1, event.ordinal);
            span.file_last_ordinal = event.ordinal;
            span.file_last_digest = event.digest.clone();
            return;
        }
        self.journal_spans.push(RelayJournalSpan {
            path: active.to_owned(),
            file_first_ordinal: event.ordinal,
            file_first_previous_digest: event.previous_digest.clone(),
            file_last_ordinal: event.ordinal,
            file_last_digest: event.digest.clone(),
            after_ordinal: event.ordinal - 1,
        });
    }

    fn push_hot_event(&mut self, event: RelayEvent) {
        if self.hot_events.len() == RELAY_HOT_EVENT_CAPACITY {
            self.hot_events.pop_front();
        }
        self.hot_events.push_back(event);
    }

    fn rewrite_relay_journal(&mut self, retain_after: u64) -> Result<()> {
        let journal = self.root.join(RELAY_JOURNAL_DIR);
        fs::create_dir_all(&journal)?;
        let replacement = journal.join("active.jsonl.new");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&replacement)?;
        let mut first: Option<RelayEvent> = None;
        let mut last: Option<RelayEvent> = None;
        let mut written_through = retain_after;
        for span in &self.journal_spans {
            visit_relay_journal_file(&span.path, false, |event, _| {
                if event.ordinal <= span.after_ordinal || event.ordinal <= written_through {
                    return Ok(ControlFlow::Continue(()));
                }
                if event.ordinal <= retain_after {
                    written_through = event.ordinal;
                    return Ok(ControlFlow::Continue(()));
                }
                let expected = written_through
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("relay event ordinal exhausted"))?;
                if event.ordinal != expected {
                    bail!(
                        "relay journal rewrite has a gap: expected {expected}, found {}",
                        event.ordinal
                    );
                }
                serde_json::to_writer(&mut file, &event)?;
                file.write_all(b"\n")?;
                if first.is_none() {
                    first = Some(event.clone());
                }
                written_through = event.ordinal;
                last = Some(event);
                Ok(ControlFlow::Continue(()))
            })?;
        }
        file.sync_all()?;
        let active = journal.join(RELAY_ACTIVE_SEGMENT);
        let next_spans = match (first, last) {
            (Some(first), Some(last)) => vec![RelayJournalSpan {
                path: active.clone(),
                file_first_ordinal: first.ordinal,
                file_first_previous_digest: first.previous_digest,
                file_last_ordinal: last.ordinal,
                file_last_digest: last.digest,
                after_ordinal: retain_after,
            }],
            (None, None) => Vec::new(),
            _ => unreachable!("relay journal rewrite recorded only one boundary"),
        };
        fs::rename(&replacement, &active)?;
        // Publish the new canonical path immediately after the atomic rename.
        // If directory sync or redundant-copy cleanup fails, the live relay
        // must not retain paths that no longer contain its canonical events.
        self.journal_spans = next_spans;
        // Make the replacement durable before removing any segment that may
        // contain the same unacknowledged observations.
        sync_directory(&journal)?;
        for entry in fs::read_dir(&journal)? {
            let path = entry?.path();
            if path.extension().is_some_and(|extension| extension == "gz") {
                fs::remove_file(path)?;
            }
        }
        sync_directory(&journal)?;
        Ok(())
    }

    fn garbage_collect_relay_history(&mut self) -> Result<()> {
        let through = self.snapshot.retained_through();
        let mut next_snapshot = self.snapshot.clone();
        Self::prune_command_ledger(&mut next_snapshot, through);
        let journal_floor = self
            .journal_spans
            .first()
            .map_or(self.snapshot.latest_ordinal, |span| span.after_ordinal);

        // Stage every in-memory mutation. The already-durable ACK/recovery
        // frontiers make either the old or rewritten journal valid after a
        // crash. Only publish the pruned ledger in memory after both durable
        // writes succeed, so a transient write failure cannot forget command
        // IDs while the daemon keeps serving retries.
        if through > journal_floor {
            self.rewrite_relay_journal(through)?;
        }
        persist_relay_snapshot(&self.root, &next_snapshot)?;
        self.snapshot = next_snapshot;
        self.hot_events.retain(|event| event.ordinal > through);
        Ok(())
    }

    fn prune_command_ledger(snapshot: &mut RelaySnapshot, through: u64) {
        let removable: Vec<String> = snapshot
            .handled_commands
            .iter()
            .filter_map(|(command_id, handled)| {
                if handled
                    .terminal_ordinal
                    .is_some_and(|terminal| terminal <= through)
                {
                    Some(command_id.clone())
                } else {
                    None
                }
            })
            .collect();
        for command_id in removable {
            snapshot.handled_commands.remove(&command_id);
            snapshot.dispatches.remove(&command_id);
        }
    }

    fn recover_nonterminal_commands(&mut self) -> Result<()> {
        let mut relay_local: Vec<(u64, String)> = self
            .snapshot
            .dispatches
            .iter()
            .filter(|(_, dispatch)| {
                dispatch.command.is_relay_local()
                    && !matches!(
                        dispatch.state,
                        RelayDispatchState::Completed
                            | RelayDispatchState::Rejected
                            | RelayDispatchState::Interrupted
                    )
            })
            .filter_map(|(command_id, _)| {
                self.snapshot
                    .handled_commands
                    .get(command_id)
                    .map(|handled| (handled.accepted_ordinal, command_id.clone()))
            })
            .collect();
        relay_local.sort();
        for (_, command_id) in relay_local {
            self.finish_relay_local_command(&command_id)?;
        }

        // Checkpoint barriers are controller-owned coordination commands. A
        // restarted relay has no owner that can complete them, regardless of
        // whether they were merely accepted, started, or already ready.
        let mut ownerless_barriers: Vec<(u64, String)> = self
            .snapshot
            .dispatches
            .iter()
            .filter(|(_, dispatch)| {
                matches!(dispatch.command, RelayCommand::BeginCheckpoint { .. })
                    && !matches!(
                        dispatch.state,
                        RelayDispatchState::Completed
                            | RelayDispatchState::Rejected
                            | RelayDispatchState::Interrupted
                    )
            })
            .filter_map(|(command_id, _)| {
                self.snapshot
                    .handled_commands
                    .get(command_id)
                    .map(|handled| (handled.accepted_ordinal, command_id.clone()))
            })
            .collect();
        ownerless_barriers.sort();
        for (_, command_id) in ownerless_barriers {
            self.record_command_interrupted(
                &command_id,
                "relay restarted without the controller that owned the checkpoint barrier",
            )?;
        }

        let mut in_flight: Vec<(u64, String)> = self
            .snapshot
            .dispatches
            .iter()
            .filter(|(_, dispatch)| dispatch.state == RelayDispatchState::InFlight)
            .filter_map(|(command_id, _)| {
                self.snapshot
                    .handled_commands
                    .get(command_id)
                    .map(|handled| (handled.accepted_ordinal, command_id.clone()))
            })
            .collect();
        in_flight.sort();
        let mut restored_close = false;
        for (_, command_id) in in_flight {
            if matches!(
                self.snapshot.dispatches[&command_id].command,
                RelayCommand::Close { .. }
            ) {
                // Closing an already-closed session is idempotent. Preserve
                // the durable close intent across a relay process restart.
                self.snapshot
                    .dispatches
                    .get_mut(&command_id)
                    .expect("in-flight close disappeared")
                    .state = RelayDispatchState::Pending;
                restored_close = true;
                continue;
            }
            self.record_command_interrupted(
                &command_id,
                "relay restarted while the ACP command was in flight; it was not replayed",
            )?;
        }
        if restored_close {
            self.persist_snapshot()?;
        }
        Ok(())
    }
}

struct RelayEventPage {
    events: Vec<RelayEvent>,
    through_ordinal: u64,
    through_digest: String,
}

fn ensure_serialized_budget(
    value: &impl Serialize,
    budget: usize,
    description: &str,
) -> Result<()> {
    let size = serde_json::to_vec(value)
        .with_context(|| format!("serialize {description} for size validation"))?
        .len();
    ensure_byte_budget(size, budget, description)
}

fn ensure_byte_budget(size: usize, budget: usize, description: &str) -> Result<()> {
    if size > budget {
        bail!("{description} is too large ({size} bytes; maximum {budget})");
    }
    Ok(())
}

/// One step in a JSON document, used to revisit a located string mutably.
#[derive(Debug, Clone, PartialEq, Eq)]
enum JsonSegment {
    Key(String),
    Index(usize),
}

/// Locate the longest string in a JSON document, with the path to reach it.
fn longest_string_path(value: &Value) -> Option<(Vec<JsonSegment>, usize)> {
    fn walk(
        value: &Value,
        path: &mut Vec<JsonSegment>,
        best: &mut Option<(Vec<JsonSegment>, usize)>,
    ) {
        match value {
            Value::String(text) => {
                if best.as_ref().is_none_or(|(_, length)| text.len() > *length) {
                    *best = Some((path.clone(), text.len()));
                }
            }
            Value::Array(items) => {
                for (index, item) in items.iter().enumerate() {
                    path.push(JsonSegment::Index(index));
                    walk(item, path, best);
                    path.pop();
                }
            }
            Value::Object(entries) => {
                for (key, entry) in entries {
                    path.push(JsonSegment::Key(key.clone()));
                    walk(entry, path, best);
                    path.pop();
                }
            }
            _ => {}
        }
    }

    let mut best = None;
    walk(value, &mut Vec::new(), &mut best);
    best
}

fn string_at_path<'a>(value: &'a mut Value, path: &[JsonSegment]) -> Option<&'a mut String> {
    let mut cursor = value;
    for segment in path {
        cursor = match (segment, cursor) {
            (JsonSegment::Key(key), Value::Object(entries)) => entries.get_mut(key)?,
            (JsonSegment::Index(index), Value::Array(items)) => items.get_mut(*index)?,
            _ => return None,
        };
    }
    match cursor {
        Value::String(text) => Some(text),
        _ => None,
    }
}

/// Shorten `text` to at most `keep` bytes and describe what was dropped.
/// Truncation lands on a character boundary, so the result stays valid UTF-8.
fn truncate_with_marker(text: &mut String, keep: usize) {
    let mut end = keep.min(text.len());
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    let dropped = text.len() - end;
    text.truncate(end);
    text.push_str(&format!("… [hel truncated {dropped} bytes]"));
}

/// Fit an observation inside `budget` serialized bytes by shortening its
/// largest text payloads.
///
/// The ACP peer decides what the agent said; the relay only decides how much
/// of it one durable event can carry. So an oversized payload is recorded in
/// truncated form rather than rejected — refusing it would strand a live
/// session over a transport limit it cannot see or control.
fn clamp_observation(observation: RelayObservation, budget: usize) -> Result<RelayObservation> {
    let mut value =
        serde_json::to_value(&observation).context("serialize relay observation for clamping")?;
    let mut size = serde_json::to_vec(&value)
        .context("measure relay observation")?
        .len();
    if size <= budget {
        return Ok(observation);
    }
    let original = size;
    while size > budget {
        let Some((path, length)) = longest_string_path(&value) else {
            break;
        };
        if length <= RELAY_TRUNCATION_FLOOR {
            break;
        }
        let Some(text) = string_at_path(&mut value, &path) else {
            break;
        };
        // Leave room for the marker itself so one pass usually suffices.
        let keep = length
            .saturating_sub(size - budget + 64)
            .max(RELAY_TRUNCATION_FLOOR);
        truncate_with_marker(text, keep);
        size = serde_json::to_vec(&value)
            .context("measure clamped relay observation")?
            .len();
    }
    if size > budget {
        return Ok(RelayObservation::Warning {
            message: format!(
                "dropped an observation that cannot be recorded: {original} bytes exceeds the {budget} byte event budget and its payload is not truncatable"
            ),
        });
    }
    match serde_json::from_value(value) {
        Ok(clamped) => {
            tracing::warn!(
                original,
                clamped = size,
                "truncated an oversized relay observation"
            );
            Ok(clamped)
        }
        Err(error) => Ok(RelayObservation::Warning {
            message: format!(
                "dropped an observation of {original} bytes: it could not be re-read after truncation: {error}"
            ),
        }),
    }
}

fn persist_relay_snapshot(root: &Path, snapshot: &RelaySnapshot) -> Result<()> {
    let body = serde_json::to_vec_pretty(snapshot)?;
    ensure_byte_budget(body.len(), RELAY_SNAPSHOT_BYTE_BUDGET, "relay snapshot")?;
    crate::hel_config::atomic_write(&root.join(RELAY_STATE_FILE), &body)
}

#[derive(Serialize)]
struct RelayEventDigestPayload<'a> {
    ordinal: u64,
    previous_digest: &'a str,
    recorded_at_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    command_id: Option<&'a str>,
    observation: &'a RelayObservation,
}

/// Compute the domain-separated SHA-256 digest for a relay event. The digest
/// field itself is excluded; every other wire-significant field is included.
pub fn relay_event_digest(event: &RelayEvent) -> Result<String> {
    validate_relay_digest(&event.previous_digest, "previous event digest")?;
    let payload = RelayEventDigestPayload {
        ordinal: event.ordinal,
        previous_digest: &event.previous_digest,
        recorded_at_ms: event.recorded_at_ms,
        command_id: event.command_id.as_deref(),
        observation: &event.observation,
    };
    let encoded = serde_json::to_vec(&payload).context("serialize relay event digest payload")?;
    let mut hasher = Sha256::new();
    hasher.update(RELAY_EVENT_DIGEST_DOMAIN);
    hasher.update(encoded);
    Ok(format!("{:x}", hasher.finalize()))
}

/// Verify an event against the exact previously applied event cursor. This is
/// the shared validation contract for both the relay journal and controller
/// projections.
pub fn validate_relay_event(
    previous_ordinal: u64,
    previous_digest: &str,
    event: &RelayEvent,
) -> Result<()> {
    validate_relay_digest(previous_digest, "previous cursor digest")?;
    let expected_ordinal = previous_ordinal
        .checked_add(1)
        .ok_or_else(|| anyhow!("relay event ordinal exhausted"))?;
    if event.ordinal != expected_ordinal {
        bail!(
            "relay event gap: expected {expected_ordinal}, found {}",
            event.ordinal
        );
    }
    if event.previous_digest != previous_digest {
        bail!(
            "relay event {} previous digest does not match cursor",
            event.ordinal
        );
    }
    validate_relay_digest(&event.digest, "event digest")?;
    let expected_digest = relay_event_digest(event)?;
    if event.digest != expected_digest {
        bail!("relay event {} digest is invalid", event.ordinal);
    }
    Ok(())
}

fn validate_relay_digest(digest: &str, name: &str) -> Result<()> {
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("{name} must be 64 lowercase hexadecimal characters");
    }
    Ok(())
}

fn apply_relay_event(snapshot: &mut RelaySnapshot, event: &RelayEvent) -> Result<()> {
    validate_relay_event(snapshot.latest_ordinal, &snapshot.latest_digest, event)?;
    match &event.observation {
        RelayObservation::AgentInitialized {
            capabilities,
            agent_info,
            ..
        } => {
            snapshot.agent_capabilities = Some(capabilities.clone());
            snapshot.agent_info = agent_info.clone();
        }
        RelayObservation::SessionOpened {
            native_session_id, ..
        } => snapshot.native_session_id = Some(native_session_id.clone()),
        RelayObservation::SessionConfigured { config_options } => {
            snapshot.config_options = config_options.clone();
        }
        RelayObservation::CommandQueued {
            command_id,
            command,
            created_at_ms,
        } => {
            snapshot.handled_commands.insert(
                command_id.clone(),
                HandledRelayCommand {
                    command: command.clone(),
                    accepted_ordinal: event.ordinal,
                    terminal_ordinal: None,
                },
            );
            snapshot.dispatches.insert(
                command_id.clone(),
                RelayDispatchRecord {
                    command: command.clone(),
                    state: RelayDispatchState::Queued,
                },
            );
            // Prompts and configuration changes share one FIFO queue so they
            // reach the agent in the order the user submitted them.
            let payload = match command {
                RelayCommand::Prompt { prompt } => Some(StoredQueuedRelayPayload::Prompt {
                    prompt: prompt.clone(),
                }),
                RelayCommand::SetConfig { key, value } => {
                    Some(StoredQueuedRelayPayload::SetConfig {
                        key: key.clone(),
                        value: value.clone(),
                    })
                }
                _ => None,
            };
            if let Some(payload) = payload {
                snapshot.queued_prompts.push(StoredQueuedRelayCommand {
                    command_id: command_id.clone(),
                    payload,
                    created_at_ms: *created_at_ms,
                });
            }
            if matches!(command, RelayCommand::Close { .. }) {
                snapshot.execution = RelayExecutionState::Closing;
            }
        }
        RelayObservation::CommandStarted {
            command_id,
            started_at_ms,
        } => {
            let dispatch = snapshot
                .dispatches
                .get_mut(command_id)
                .ok_or_else(|| anyhow!("started unknown relay command {command_id}"))?;
            dispatch.state = RelayDispatchState::Pending;
            match &dispatch.command {
                RelayCommand::Prompt { .. } => {
                    let index = snapshot
                        .queued_prompts
                        .iter()
                        .position(|queued| queued.command_id == *command_id)
                        .ok_or_else(|| anyhow!("started prompt {command_id} was not queued"))?;
                    let queued = snapshot.queued_prompts.remove(index);
                    let StoredQueuedRelayPayload::Prompt { prompt } = queued.payload else {
                        bail!("queued command {command_id} is not a prompt");
                    };
                    snapshot.execution = RelayExecutionState::Running;
                    snapshot.active_prompt = Some(StoredActiveRelayPrompt {
                        command_id: queued.command_id,
                        prompt,
                        created_at_ms: queued.created_at_ms,
                        started_at_ms: *started_at_ms,
                    });
                }
                // A configuration change leaves the queue when it starts, but
                // the ACP session stays idle: it applies between turns.
                RelayCommand::SetConfig { .. } => {
                    let index = snapshot
                        .queued_prompts
                        .iter()
                        .position(|queued| queued.command_id == *command_id)
                        .ok_or_else(|| {
                            anyhow!("started configuration change {command_id} was not queued")
                        })?;
                    snapshot.queued_prompts.remove(index);
                }
                RelayCommand::Close { .. } => snapshot.execution = RelayExecutionState::Closing,
                RelayCommand::BeginCheckpoint { .. } => {
                    if snapshot.checkpoint_barrier.is_some() {
                        bail!("checkpoint barrier started while another barrier was active");
                    }
                    snapshot.checkpoint_barrier = Some(command_id.clone());
                    snapshot.checkpoint_ready_through = None;
                    snapshot.checkpoint_ready_digest = None;
                }
                _ => {}
            }
        }
        RelayObservation::CommandCompleted {
            command_id,
            outcome,
        } => {
            let command = snapshot
                .dispatches
                .get(command_id)
                .ok_or_else(|| anyhow!("completed unknown relay command {command_id}"))?
                .command
                .clone();
            snapshot
                .dispatches
                .get_mut(command_id)
                .expect("dispatch disappeared")
                .state = RelayDispatchState::Completed;
            snapshot
                .handled_commands
                .get_mut(command_id)
                .ok_or_else(|| anyhow!("completed command {command_id} is not in the ledger"))?
                .terminal_ordinal = Some(event.ordinal);
            match (command, outcome) {
                (RelayCommand::Prompt { .. }, RelayCommandOutcome::Prompt { .. }) => {
                    if snapshot
                        .active_prompt
                        .as_ref()
                        .map(|active| &active.command_id)
                        == Some(command_id)
                    {
                        snapshot.active_prompt = None;
                    }
                    if snapshot.execution == RelayExecutionState::Running {
                        snapshot.execution = RelayExecutionState::Idle;
                    }
                }
                (
                    RelayCommand::RemoveQueuedPrompt { queued_command_id },
                    RelayCommandOutcome::QueueChanged {
                        removed_command_ids,
                    },
                ) => {
                    let expected = snapshot
                        .queued_prompts
                        .iter()
                        .any(|queued| queued.command_id == queued_command_id)
                        .then_some(vec![queued_command_id]);
                    if expected.as_deref() != Some(removed_command_ids.as_slice()) {
                        bail!("removed queue outcome does not match the durable queue");
                    }
                    terminalize_removed_prompts(snapshot, removed_command_ids, event.ordinal)?;
                }
                (
                    RelayCommand::ClearQueuedPrompts,
                    RelayCommandOutcome::QueueChanged {
                        removed_command_ids,
                    },
                ) => {
                    let expected: Vec<String> = snapshot
                        .queued_prompts
                        .iter()
                        .map(|queued| queued.command_id.clone())
                        .collect();
                    if expected != *removed_command_ids {
                        bail!("cleared queue outcome does not match the durable queue");
                    }
                    terminalize_removed_prompts(snapshot, removed_command_ids, event.ordinal)?;
                }
                (RelayCommand::SetConfig { key, value }, RelayCommandOutcome::Configured) => {
                    snapshot.config.insert(key, value);
                }
                (RelayCommand::SetSessionMode { mode_id }, RelayCommandOutcome::SessionModeSet) => {
                    snapshot.config.insert("mode".to_owned(), mode_id);
                }
                (RelayCommand::Cancel, RelayCommandOutcome::Cancelled) => {}
                (RelayCommand::Close { .. }, RelayCommandOutcome::Closed) => {
                    snapshot.execution = RelayExecutionState::Closed;
                    snapshot.active_prompt = None;
                }
                (
                    RelayCommand::CompleteCheckpoint { barrier_command_id },
                    RelayCommandOutcome::CheckpointCompleted,
                ) => {
                    if snapshot.checkpoint_barrier.as_deref() != Some(&barrier_command_id) {
                        bail!("checkpoint completion does not match the active barrier");
                    }
                    let ready_through = snapshot
                        .checkpoint_ready_through
                        .ok_or_else(|| anyhow!("checkpoint barrier was not ready"))?;
                    let ready_digest = snapshot
                        .checkpoint_ready_digest
                        .clone()
                        .ok_or_else(|| anyhow!("checkpoint barrier ready digest is missing"))?;
                    snapshot.recovery_floor_ordinal = ready_through;
                    snapshot.recovery_floor_digest = ready_digest;
                    snapshot.checkpoint_barrier = None;
                    snapshot.checkpoint_ready_through = None;
                    snapshot.checkpoint_ready_digest = None;
                    if let Some(barrier) = snapshot.dispatches.get_mut(&barrier_command_id) {
                        barrier.state = RelayDispatchState::Completed;
                    }
                    if let Some(barrier) = snapshot.handled_commands.get_mut(&barrier_command_id) {
                        barrier.terminal_ordinal = Some(event.ordinal);
                    }
                }
                (
                    RelayCommand::ReleaseCheckpoint { barrier_command_id },
                    RelayCommandOutcome::CheckpointReleased,
                ) => {
                    if snapshot.checkpoint_barrier.as_deref() != Some(&barrier_command_id) {
                        bail!("checkpoint release does not match the active barrier");
                    }
                    if snapshot.checkpoint_ready_through.is_none() {
                        bail!("checkpoint barrier was not ready");
                    }
                    // Dispatch resumes, but the recovery floor stays where the
                    // last installed archive left it: nothing yet proves this
                    // archive reached the controller's disk.
                    snapshot.checkpoint_barrier = None;
                    snapshot.checkpoint_ready_through = None;
                    snapshot.checkpoint_ready_digest = None;
                    if let Some(barrier) = snapshot.dispatches.get_mut(&barrier_command_id) {
                        barrier.state = RelayDispatchState::Completed;
                    }
                    if let Some(barrier) = snapshot.handled_commands.get_mut(&barrier_command_id) {
                        barrier.terminal_ordinal = Some(event.ordinal);
                    }
                }
                (
                    RelayCommand::AdvanceRecoveryFloor { through },
                    RelayCommandOutcome::RecoveryFloorAdvanced,
                ) => {
                    if through.ordinal < snapshot.recovery_floor_ordinal {
                        bail!("recovery floor cannot move back");
                    }
                    snapshot.recovery_floor_ordinal = through.ordinal;
                    snapshot.recovery_floor_digest = through.digest;
                }
                (RelayCommand::RecordNotice { .. }, RelayCommandOutcome::NoticeRecorded) => {}
                (RelayCommand::BeginCheckpoint { .. }, _) => {
                    bail!("checkpoint barriers complete through checkpoint-ready")
                }
                (command, outcome) => {
                    bail!(
                        "relay command {:?} has incompatible completion outcome {outcome:?}",
                        command.kind()
                    )
                }
            }
        }
        RelayObservation::CommandRejected {
            command_id,
            command: observed_command,
            ..
        }
        | RelayObservation::CommandInterrupted {
            command_id,
            command: observed_command,
            ..
        } => {
            let state = if matches!(event.observation, RelayObservation::CommandRejected { .. }) {
                RelayDispatchState::Rejected
            } else {
                RelayDispatchState::Interrupted
            };
            let command = snapshot
                .dispatches
                .get(command_id)
                .ok_or_else(|| anyhow!("terminated unknown relay command {command_id}"))?
                .command
                .clone();
            if command.kind() != *observed_command {
                bail!("terminated command {command_id} has the wrong command identity");
            }
            snapshot
                .dispatches
                .get_mut(command_id)
                .expect("dispatch disappeared")
                .state = state;
            snapshot
                .handled_commands
                .get_mut(command_id)
                .ok_or_else(|| anyhow!("terminated command {command_id} is not in the ledger"))?
                .terminal_ordinal = Some(event.ordinal);
            snapshot
                .queued_prompts
                .retain(|queued| queued.command_id != *command_id);
            if snapshot
                .active_prompt
                .as_ref()
                .map(|active| &active.command_id)
                == Some(command_id)
            {
                snapshot.active_prompt = None;
                snapshot.execution = RelayExecutionState::Idle;
            }
            if matches!(command, RelayCommand::BeginCheckpoint { .. })
                && snapshot.checkpoint_barrier.as_deref() == Some(command_id)
            {
                snapshot.checkpoint_barrier = None;
                snapshot.checkpoint_ready_through = None;
                snapshot.checkpoint_ready_digest = None;
            }
            if matches!(command, RelayCommand::Close { .. })
                && snapshot.execution == RelayExecutionState::Closing
            {
                snapshot.execution = RelayExecutionState::Idle;
            }
        }
        RelayObservation::ConfigurationUpdated { key, value } => {
            snapshot.config.insert(key.clone(), value.clone());
        }
        RelayObservation::CheckpointReady {
            command_id,
            through,
        } => {
            let Some(dispatch) = snapshot.dispatches.get(command_id) else {
                bail!("checkpoint ready for unknown command {command_id}");
            };
            if !matches!(dispatch.command, RelayCommand::BeginCheckpoint { .. }) {
                bail!("checkpoint ready for non-barrier command {command_id}");
            }
            if snapshot.checkpoint_barrier.as_deref() != Some(command_id) {
                bail!("checkpoint ready does not match the active barrier");
            }
            if *through != event.ordinal {
                bail!("checkpoint ready frontier does not match its event ordinal");
            }
            snapshot.checkpoint_ready_through = Some(*through);
            snapshot.checkpoint_ready_digest = Some(event.digest.clone());
        }
        RelayObservation::Closing => snapshot.execution = RelayExecutionState::Closing,
        RelayObservation::Closed => {
            snapshot.execution = RelayExecutionState::Closed;
            snapshot.active_prompt = None;
        }
        RelayObservation::SessionUpdate { update } => match update.as_ref() {
            SessionUpdate::AvailableCommandsUpdate(update) => {
                snapshot.available_commands = update.available_commands.clone();
            }
            SessionUpdate::ConfigOptionUpdate(update) => {
                snapshot.config_options = update.config_options.clone();
            }
            SessionUpdate::CurrentModeUpdate(update) => {
                snapshot
                    .config
                    .insert("mode".to_owned(), update.current_mode_id.to_string());
            }
            _ => {}
        },
        RelayObservation::PermissionAutoApproved { .. }
        | RelayObservation::Warning { .. }
        | RelayObservation::Notice { .. } => {}
    }
    snapshot.latest_ordinal = event.ordinal;
    snapshot.latest_digest = event.digest.clone();
    Ok(())
}

/// Whether finishing this relay-local command can let journal GC drop history.
/// Only a recovery-floor move does; releasing a barrier deliberately leaves the
/// floor where an installed archive left it.
fn releases_history(command: &RelayCommand) -> bool {
    matches!(
        command,
        RelayCommand::CompleteCheckpoint { .. } | RelayCommand::AdvanceRecoveryFloor { .. }
    )
}

fn terminalize_removed_prompts(
    snapshot: &mut RelaySnapshot,
    removed_command_ids: &[String],
    terminal_ordinal: u64,
) -> Result<()> {
    for command_id in removed_command_ids {
        let dispatch = snapshot
            .dispatches
            .get_mut(command_id)
            .ok_or_else(|| anyhow!("removed unknown queued command {command_id}"))?;
        if !dispatch.command.is_queue_entry() || dispatch.state != RelayDispatchState::Queued {
            bail!("removed command {command_id} is not a queued command");
        }
        dispatch.state = RelayDispatchState::Rejected;
        snapshot
            .handled_commands
            .get_mut(command_id)
            .ok_or_else(|| anyhow!("removed command {command_id} is not in the ledger"))?
            .terminal_ordinal = Some(terminal_ordinal);
    }
    snapshot.queued_prompts.retain(|queued| {
        !removed_command_ids
            .iter()
            .any(|command_id| command_id == &queued.command_id)
    });
    Ok(())
}

fn open_relay_journal(
    journal: &Path,
    retained_through: u64,
    retained_digest: &str,
    snapshot_ordinal: u64,
    snapshot: &mut RelaySnapshot,
) -> Result<(Vec<RelayJournalSpan>, VecDeque<RelayEvent>)> {
    let mut paths = Vec::new();
    if journal.exists() {
        for entry in fs::read_dir(journal)? {
            let path = entry?.path();
            if path
                .file_name()
                .is_some_and(|name| name == RELAY_ACTIVE_SEGMENT)
                || path.extension().is_some_and(|extension| extension == "gz")
            {
                paths.push(path);
            }
        }
    }
    paths.sort();
    let active = journal.join(RELAY_ACTIVE_SEGMENT);
    let mut files = Vec::new();
    for path in paths {
        if let Some(metadata) = inspect_relay_journal_file(&path, path == active)? {
            files.push(metadata);
        }
    }

    let original_frontiers = [
        (
            "snapshot",
            snapshot.latest_ordinal,
            snapshot.latest_digest.clone(),
        ),
        (
            "acknowledgement",
            snapshot.acknowledged_through,
            snapshot.acknowledged_digest.clone(),
        ),
        (
            "recovery floor",
            snapshot.recovery_floor_ordinal,
            snapshot.recovery_floor_digest.clone(),
        ),
    ];
    for (name, ordinal, digest) in &original_frontiers {
        if *ordinal == retained_through && digest != retained_digest {
            bail!("relay {name} digest conflicts with retained frontier");
        }
    }

    let journal_latest = files
        .iter()
        .map(|file| file.file_last_ordinal)
        .max()
        .unwrap_or(retained_through);
    let mut previous_ordinal = retained_through;
    let mut previous_digest = retained_digest.to_owned();
    let mut spans = Vec::new();
    let mut hot_events = VecDeque::new();

    while previous_ordinal < journal_latest {
        let next_ordinal = previous_ordinal
            .checked_add(1)
            .ok_or_else(|| anyhow!("relay event ordinal exhausted"))?;
        let Some(candidate) = files
            .iter()
            .filter(|file| {
                file.file_first_ordinal <= next_ordinal && file.file_last_ordinal > previous_ordinal
            })
            .max_by_key(|file| (file.file_last_ordinal, usize::from(file.path == active)))
            .cloned()
        else {
            bail!("relay journal has a gap after event {previous_ordinal}");
        };
        let contribution_after = previous_ordinal;
        let mut overlap_verified = candidate.file_first_ordinal == next_ordinal;
        visit_relay_journal_file(&candidate.path, false, |event, _| {
            if event.ordinal < contribution_after {
                return Ok(ControlFlow::Continue(()));
            }
            if event.ordinal == contribution_after {
                if event.digest != previous_digest {
                    bail!(
                        "overlapping relay journal {} conflicts at event {}",
                        candidate.path.display(),
                        event.ordinal
                    );
                }
                overlap_verified = true;
                return Ok(ControlFlow::Continue(()));
            }
            if !overlap_verified {
                bail!(
                    "overlapping relay journal {} does not contain boundary event {}",
                    candidate.path.display(),
                    contribution_after
                );
            }
            validate_relay_event(previous_ordinal, &previous_digest, &event)
                .context("validate relay journal event chain")?;
            for (name, ordinal, digest) in &original_frontiers {
                if event.ordinal == *ordinal && event.digest != *digest {
                    bail!(
                        "relay {name} digest conflicts with journal event {}",
                        event.ordinal
                    );
                }
            }
            if event.ordinal > snapshot_ordinal {
                apply_relay_event(snapshot, &event)?;
            }
            previous_ordinal = event.ordinal;
            previous_digest = event.digest.clone();
            if hot_events.len() == RELAY_HOT_EVENT_CAPACITY {
                hot_events.pop_front();
            }
            hot_events.push_back(event);
            Ok(ControlFlow::Continue(()))
        })?;
        if previous_ordinal != candidate.file_last_ordinal {
            bail!(
                "relay journal {} ended at event {previous_ordinal}, expected {}",
                candidate.path.display(),
                candidate.file_last_ordinal
            );
        }
        spans.push(RelayJournalSpan {
            after_ordinal: contribution_after,
            ..candidate
        });
    }

    if journal_latest < snapshot_ordinal {
        bail!(
            "relay snapshot frontier {} is ahead of retained journal {journal_latest}",
            snapshot_ordinal
        );
    }
    for (name, ordinal, _) in &original_frontiers {
        if *ordinal > retained_through && *ordinal > journal_latest {
            bail!("relay {name} event {ordinal} is not retained");
        }
    }

    // An active copy left behind by a crash may be fully covered by a sealed
    // span. Keep it as a zero-width canonical span when it reaches the current
    // frontier so future appends remain contiguous. A stale shorter active
    // copy is safe to truncate because the selected sealed chain covers it.
    if let Some(active_file) = files.iter().find(|file| file.path == active)
        && !spans.iter().any(|span| span.path == active)
    {
        if active_file.file_last_ordinal == previous_ordinal {
            spans.push(RelayJournalSpan {
                after_ordinal: previous_ordinal,
                ..active_file.clone()
            });
        } else if active_file.file_last_ordinal < previous_ordinal {
            truncate_active_relay_journal(journal, &active)?;
        }
    }
    let canonical_paths = spans
        .iter()
        .map(|span| span.path.clone())
        .collect::<BTreeSet<_>>();
    let mut removed_redundant_copy = false;
    for file in &files {
        if file
            .path
            .extension()
            .is_some_and(|extension| extension == "gz")
            && !canonical_paths.contains(&file.path)
        {
            fs::remove_file(&file.path).with_context(|| {
                format!("remove redundant relay segment {}", file.path.display())
            })?;
            removed_redundant_copy = true;
        }
    }
    if removed_redundant_copy {
        sync_directory(journal)?;
    }
    Ok((spans, hot_events))
}

fn validate_relay_snapshot_frontiers(snapshot: &RelaySnapshot) -> Result<()> {
    if snapshot.acknowledged_through > snapshot.latest_ordinal {
        bail!("relay acknowledgement is ahead of the event frontier");
    }
    if snapshot.recovery_floor_ordinal > snapshot.latest_ordinal {
        bail!("relay recovery floor is ahead of the event frontier");
    }
    validate_relay_digest(&snapshot.latest_digest, "relay latest digest")?;
    validate_relay_digest(
        &snapshot.acknowledged_digest,
        "relay acknowledgement digest",
    )?;
    validate_relay_digest(
        &snapshot.recovery_floor_digest,
        "relay recovery floor digest",
    )?;
    if (snapshot.latest_ordinal == 0) != (snapshot.latest_digest == RELAY_EVENT_GENESIS_DIGEST) {
        bail!("relay latest frontier and genesis digest disagree");
    }
    if (snapshot.acknowledged_through == 0)
        != (snapshot.acknowledged_digest == RELAY_EVENT_GENESIS_DIGEST)
    {
        bail!("relay acknowledgement frontier and genesis digest disagree");
    }
    if (snapshot.recovery_floor_ordinal == 0)
        != (snapshot.recovery_floor_digest == RELAY_EVENT_GENESIS_DIGEST)
    {
        bail!("relay recovery floor and genesis digest disagree");
    }
    Ok(())
}

fn seal_active_relay_segment(journal: &Path, metadata: &mut RelayJournalSpan) -> Result<()> {
    let active = journal.join(RELAY_ACTIVE_SEGMENT);
    let sealed_name = format!(
        "segment-{:020}-{:020}.jsonl.gz",
        metadata.file_first_ordinal, metadata.file_last_ordinal
    );
    let temporary = journal.join(format!("{sealed_name}.new"));
    let destination = journal.join(sealed_name);
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    let mut encoder = GzEncoder::new(file, Compression::default());
    visit_relay_journal_file(&active, false, |event, _| {
        serde_json::to_writer(&mut encoder, &event)?;
        encoder.write_all(b"\n")?;
        Ok(ControlFlow::Continue(()))
    })?;
    let file = encoder.finish()?;
    file.sync_all()?;
    fs::rename(&temporary, &destination)?;
    // The sealed copy must be durable before the active segment is replaced.
    sync_directory(journal)?;

    let replacement = journal.join("active.jsonl.new");
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&replacement)?;
    file.sync_all()?;
    fs::rename(&replacement, active)?;
    // From this point the live active path is empty. Publish the sealed path
    // in memory before the final directory sync so even a reported sync error
    // cannot make a subsequent retry append against stale file metadata.
    metadata.path = destination;
    sync_directory(journal)
}

fn inspect_relay_journal_file(
    path: &Path,
    repair_partial_tail: bool,
) -> Result<Option<RelayJournalSpan>> {
    let mut first: Option<RelayEvent> = None;
    let mut previous: Option<RelayEvent> = None;
    visit_relay_journal_file(path, repair_partial_tail, |event, encoded_len| {
        ensure_byte_budget(encoded_len, RELAY_EVENT_BYTE_BUDGET, "relay event")?;
        if let Some(previous) = &previous {
            validate_relay_event(previous.ordinal, &previous.digest, &event)
                .with_context(|| format!("validate relay journal {}", path.display()))?;
        } else {
            let previous_ordinal = event
                .ordinal
                .checked_sub(1)
                .ok_or_else(|| anyhow!("relay event ordinal zero is invalid"))?;
            validate_relay_event(previous_ordinal, &event.previous_digest, &event)
                .with_context(|| format!("validate relay journal {}", path.display()))?;
            first = Some(event.clone());
        }
        previous = Some(event);
        Ok(ControlFlow::Continue(()))
    })?;
    Ok(first.zip(previous).map(|(first, last)| RelayJournalSpan {
        path: path.to_owned(),
        file_first_ordinal: first.ordinal,
        file_first_previous_digest: first.previous_digest,
        file_last_ordinal: last.ordinal,
        file_last_digest: last.digest,
        after_ordinal: 0,
    }))
}

fn visit_relay_journal_file(
    path: &Path,
    repair_partial_tail: bool,
    mut visitor: impl FnMut(RelayEvent, usize) -> Result<ControlFlow<()>>,
) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let compressed = path.extension().is_some_and(|extension| extension == "gz");
    if compressed {
        let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
        let decoder = GzDecoder::new(file);
        let mut reader = std::io::BufReader::new(decoder);
        visit_relay_journal_reader(path, &mut reader, false, &mut visitor)?;
        return Ok(());
    }

    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = std::io::BufReader::new(file);
    let scan = visit_relay_journal_reader(path, &mut reader, repair_partial_tail, &mut visitor)?;
    drop(reader);
    if let Some(valid_len) = scan.truncate_to {
        let file = OpenOptions::new()
            .write(true)
            .open(path)
            .with_context(|| format!("open torn relay journal {}", path.display()))?;
        file.set_len(valid_len)?;
        file.sync_data()?;
        if let Some(parent) = path.parent() {
            sync_directory(parent)?;
        }
    }
    Ok(())
}

struct RelayJournalScan {
    truncate_to: Option<u64>,
}

fn visit_relay_journal_reader(
    path: &Path,
    reader: &mut impl BufRead,
    repair_partial_tail: bool,
    visitor: &mut impl FnMut(RelayEvent, usize) -> Result<ControlFlow<()>>,
) -> Result<RelayJournalScan> {
    let mut line = Vec::new();
    let mut complete_bytes = 0_u64;
    loop {
        let (consumed, terminated) = read_bounded_line(reader, &mut line, RELAY_EVENT_BYTE_BUDGET)
            .with_context(|| format!("read relay journal {}", path.display()))?;
        if consumed == 0 {
            return Ok(RelayJournalScan { truncate_to: None });
        }
        if !terminated && repair_partial_tail {
            return Ok(RelayJournalScan {
                truncate_to: Some(complete_bytes),
            });
        }
        complete_bytes = complete_bytes
            .checked_add(u64::try_from(consumed).context("relay journal length overflow")?)
            .ok_or_else(|| anyhow!("relay journal length overflow"))?;
        if line.is_empty() {
            continue;
        }
        let event = serde_json::from_slice(&line)
            .with_context(|| format!("parse relay journal {}", path.display()))?;
        if visitor(event, line.len())?.is_break() {
            return Ok(RelayJournalScan { truncate_to: None });
        }
        if !terminated {
            return Ok(RelayJournalScan { truncate_to: None });
        }
    }
}

fn read_bounded_line(
    reader: &mut impl BufRead,
    line: &mut Vec<u8>,
    maximum_bytes: usize,
) -> Result<(usize, bool)> {
    line.clear();
    let mut consumed_total = 0_usize;
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok((consumed_total, false));
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let content_bytes = newline.unwrap_or(available.len());
        let next_len = line
            .len()
            .checked_add(content_bytes)
            .ok_or_else(|| anyhow!("relay journal line length overflow"))?;
        ensure_byte_budget(next_len, maximum_bytes, "relay journal event")?;
        line.extend_from_slice(&available[..content_bytes]);
        let consumed = content_bytes + usize::from(newline.is_some());
        reader.consume(consumed);
        consumed_total = consumed_total
            .checked_add(consumed)
            .ok_or_else(|| anyhow!("relay journal length overflow"))?;
        if newline.is_some() {
            return Ok((consumed_total, true));
        }
    }
}

fn truncate_active_relay_journal(journal: &Path, active: &Path) -> Result<()> {
    let replacement = journal.join("active.jsonl.new");
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&replacement)?;
    file.sync_all()?;
    fs::rename(&replacement, active)?;
    sync_directory(journal)
}

/// File name of the controller-owned canonical session projection restored
/// alongside a checkpoint's target artifacts.
pub const RESTORED_CANONICAL_SESSION_FILE: &str = "canonical-session.json";

#[derive(Debug)]
struct RestoredRelaySeed {
    event_frontier: u64,
    event_frontier_digest: String,
    queued_prompts: Vec<RestoredQueuedPrompt>,
}

#[derive(Debug)]
struct RestoredQueuedPrompt {
    command_id: String,
    kind: CanonicalQueuedCommandKind,
    content: Vec<Value>,
    queued_at_ms: i64,
}

fn read_restored_relay_seed(root: &Path) -> Result<Option<RestoredRelaySeed>> {
    let path = root.join(RESTORED_CANONICAL_SESSION_FILE);
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(&path).with_context(|| format!("read {}", path.display()))?;
    let canonical: crate::hel_archive::CanonicalSessionSnapshot =
        serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))?;
    canonical
        .validate()
        .with_context(|| format!("validate {}", path.display()))?;
    Ok(Some(RestoredRelaySeed {
        event_frontier: canonical.event_frontier,
        event_frontier_digest: canonical.event_frontier_digest,
        queued_prompts: canonical
            .queued_prompts
            .into_iter()
            .map(|queued| RestoredQueuedPrompt {
                command_id: queued.command_id,
                kind: queued.kind,
                content: queued.content,
                queued_at_ms: queued.queued_at_ms,
            })
            .collect(),
    }))
}

fn sync_directory(path: &Path) -> Result<()> {
    // Directory fsync is only available on Unix; Windows cannot open a
    // directory handle through File::open.
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn relay_protocol_error(
    code: RelayErrorCode,
    message: impl Into<String>,
    retryable: bool,
    detail: Option<RelayErrorDetail>,
) -> RelayProtocolError {
    RelayProtocolError {
        code,
        message: message.into(),
        retryable,
        detail,
    }
}

fn relay_error(
    code: RelayErrorCode,
    message: impl Into<String>,
    retryable: bool,
    detail: Option<RelayErrorDetail>,
) -> RelayResponseBody {
    RelayResponseBody::Error {
        error: relay_protocol_error(code, message, retryable, detail),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attachment {
    pub name: String,
    pub media_type: String,
    /// A target-local path or opaque adapter reference. File contents do not
    /// travel in control messages.
    pub reference: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerPhase {
    Idle,
    Running,
    Closing,
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerSessionSummary {
    pub phase: WorkerPhase,
    pub latest_seq: u64,
    pub latest_completed_turn_seq: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_session_id: Option<String>,
    pub session_title: Option<String>,
    pub unread_agent_messages: u64,
    pub agent_text_stream_open: bool,
    pub last_agent_message_id: Option<String>,
    pub transcript_tail: Vec<crate::hel_transcript::ChatEntry>,
    #[serde(default)]
    pub queued_prompts: Vec<QueuedPrompt>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivePrompt {
    pub request_id: String,
    pub text: String,
    pub attachments: Vec<Attachment>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueuedPrompt {
    pub id: String,
    pub text: String,
    pub attachments: Vec<Attachment>,
    pub created_at_ms: i64,
}

/// Minimal snapshot shape retained for importing pre-relay event histories
/// into the controller-owned materialized projection. It is not a relay wire
/// message and is never persisted by `DurableRelay`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerSnapshot {
    pub session_id: String,
    pub phase: WorkerPhase,
    pub latest_seq: u64,
    pub last_checkpoint_seq: Option<u64>,
    pub active_prompt: Option<ActivePrompt>,
    pub config: BTreeMap<String, Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub queued_prompts: Vec<QueuedPrompt>,
}

impl WorkerSnapshot {
    pub(crate) fn summary(session_id: String, phase: WorkerPhase, latest_seq: u64) -> Self {
        Self {
            session_id,
            phase,
            latest_seq,
            last_checkpoint_seq: None,
            active_prompt: None,
            config: BTreeMap::new(),
            queued_prompts: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequencedEvent {
    pub seq: u64,
    /// UTC receive/accept time recorded by the durable worker. Legacy and
    /// imported event streams may omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recorded_at_ms: Option<i64>,
    /// Present for controller mutations. Persisting the id beside the event
    /// closes the crash window between appending the event and snapshotting
    /// the idempotency result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    pub event: WorkerEvent,
}

/// Event shape used only to decode/import histories produced before relay-v1.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum WorkerEvent {
    PromptAccepted {
        request_id: String,
        text: String,
        attachments: Vec<Attachment>,
    },
    QueuedPromptAdded {
        prompt: QueuedPrompt,
    },
    QueuedPromptRemoved {
        queue_id: String,
    },
    QueuedPromptPromoted {
        prompt: QueuedPrompt,
        request_id: String,
    },
    QueuedPromptsCleared,
    TurnCompleted,
    Cancelled,
    ConfigChanged {
        key: String,
        value: Value,
    },
    Checkpointed {
        reason: Option<String>,
        #[serde(default, skip_serializing_if = "is_false")]
        quiescent: bool,
    },
    Closing,
    Closed,
    Adapter {
        kind: String,
        payload: Value,
    },
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn now_unix_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

pub(crate) fn clear_native_session_identity(root: &Path) -> Result<()> {
    let path = root.join(NATIVE_SESSION_IDENTITY_FILE);
    match fs::remove_file(&path) {
        Ok(()) => {
            #[cfg(unix)]
            File::open(root)?.sync_all()?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("remove {}", path.display())),
    }
}

fn validate_identifier(value: &str, name: &str) -> Result<()> {
    if value.len() < 8
        || value.len() > 128
        || !value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    {
        bail!("invalid {name}");
    }
    Ok(())
}

pub fn unsupported_relay_method_response(
    request_id: String,
    protocol_version: u32,
    method: String,
) -> RelayResponseEnvelope {
    RelayResponseEnvelope {
        request_id,
        protocol_version,
        body: relay_error(
            RelayErrorCode::InvalidRequest,
            format!("relay does not support method {method:?}"),
            false,
            None,
        ),
    }
}

pub fn invalid_relay_request_response(
    request_id: String,
    protocol_version: u32,
    message: String,
) -> RelayResponseEnvelope {
    RelayResponseEnvelope {
        request_id,
        protocol_version,
        body: relay_error(RelayErrorCode::InvalidRequest, message, false, None),
    }
}

pub fn read_relay_frame(reader: &mut impl BufRead) -> Result<Option<RelayRequestEnvelope>> {
    let mut bytes = Vec::new();
    let (read, _) = read_bounded_line(reader, &mut bytes, MAX_FRAME_BYTES)
        .context("read relay protocol frame")?;
    if read == 0 {
        return Ok(None);
    }
    if bytes.last() == Some(&b'\r') {
        bytes.pop();
    }
    if bytes.is_empty() {
        bail!("empty relay protocol frame");
    }
    serde_json::from_slice(&bytes)
        .context("parse relay protocol request")
        .map(Some)
}

pub fn write_relay_frame(writer: &mut impl Write, response: &RelayResponseEnvelope) -> Result<()> {
    serde_json::to_writer(&mut *writer, response)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

pub fn serve_relay_json_lines(
    reader: &mut impl BufRead,
    writer: &mut impl Write,
    relay: &mut DurableRelay,
) -> Result<()> {
    while let Some(request) = read_relay_frame(reader)? {
        let response = relay.handle(request);
        write_relay_frame(writer, &response)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_client_protocol::schema::v1::ContentChunk;

    const SESSION: &str = "018f9dd2-a3b4-7c8d-9000-123456789abc";

    fn relay_request(request_id: &str, request: RelayRequest) -> RelayRequestEnvelope {
        RelayRequestEnvelope {
            request_id: request_id.to_owned(),
            protocol_version: RELAY_PROTOCOL_VERSION,
            request,
        }
    }

    fn submit_relay(relay: &mut DurableRelay, command_id: &str, command: RelayCommand) -> u64 {
        let response = relay.handle(relay_request(
            &format!("request-{command_id}"),
            RelayRequest::Submit {
                command_id: command_id.to_owned(),
                command,
            },
        ));
        let RelayResponseBody::Ok {
            payload:
                RelayResponsePayload::Accepted {
                    command_id: accepted,
                    ordinal,
                },
        } = response.body
        else {
            panic!("expected accepted relay command, got {:?}", response.body);
        };
        assert_eq!(accepted, command_id);
        ordinal
    }

    fn attach_relay(
        relay: &mut DurableRelay,
        request_id: &str,
        after_ordinal: u64,
    ) -> RelayResponseEnvelope {
        let after_digest = relay.digest_at(after_ordinal).unwrap().unwrap();
        relay.handle(relay_request(
            request_id,
            RelayRequest::Attach {
                after_ordinal,
                after_digest,
            },
        ))
    }

    fn acknowledge_relay(
        relay: &mut DurableRelay,
        request_id: &str,
        through_ordinal: u64,
    ) -> RelayResponseEnvelope {
        let through_digest = relay.digest_at(through_ordinal).unwrap().unwrap();
        relay.handle(relay_request(
            request_id,
            RelayRequest::Acknowledge {
                through_ordinal,
                through_digest,
            },
        ))
    }

    fn prompt(text: &str) -> RelayCommand {
        RelayCommand::Prompt {
            prompt: vec![ContentBlock::from(text)],
        }
    }

    fn set_config(key: &str, value: &str) -> RelayCommand {
        RelayCommand::SetConfig {
            key: key.to_owned(),
            value: value.to_owned(),
        }
    }

    fn queued_command_ids(relay: &DurableRelay) -> Vec<String> {
        relay
            .operational_state()
            .queued_prompts
            .into_iter()
            .map(|queued| queued.command_id)
            .collect()
    }

    fn finish_prompt(relay: &mut DurableRelay, command_id: &str) {
        relay
            .record_command_completed(
                command_id,
                RelayCommandOutcome::Prompt {
                    stop_reason: "end_turn".into(),
                },
            )
            .unwrap();
    }

    fn retained_events(relay: &DurableRelay) -> Vec<RelayEvent> {
        relay
            .events_after(
                relay.snapshot.retained_through(),
                relay.snapshot.retained_digest(),
            )
            .unwrap()
    }

    fn submit_release(
        relay: &mut DurableRelay,
        command_id: &str,
        barrier_command_id: &str,
    ) -> RelayResponseEnvelope {
        relay.handle(relay_request(
            &format!("request-{command_id}"),
            RelayRequest::Submit {
                command_id: command_id.to_owned(),
                command: RelayCommand::ReleaseCheckpoint {
                    barrier_command_id: barrier_command_id.to_owned(),
                },
            },
        ))
    }

    fn submit_floor(
        relay: &mut DurableRelay,
        command_id: &str,
        through: RelayCursor,
    ) -> RelayResponseEnvelope {
        relay.handle(relay_request(
            &format!("request-{command_id}"),
            RelayRequest::Submit {
                command_id: command_id.to_owned(),
                command: RelayCommand::AdvanceRecoveryFloor { through },
            },
        ))
    }

    fn ready_checkpoint(relay: &mut DurableRelay, command_id: &str) -> RelayCursor {
        submit_relay(
            relay,
            command_id,
            RelayCommand::BeginCheckpoint { reason: None },
        );
        let claimed = relay.claim_pending_commands(true).unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].command_id, command_id);
        relay.record_checkpoint_ready(command_id).unwrap();
        relay.operational_state().checkpoint_ready.unwrap()
    }

    #[test]
    fn relay_runs_queued_prompts_in_order_without_a_controller() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        submit_relay(&mut relay, "command-one", prompt("one"));
        submit_relay(&mut relay, "command-two", prompt("two"));
        submit_relay(&mut relay, "command-three", prompt("three"));

        let first = relay.claim_pending_commands(true).unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].command_id, "command-one");
        relay
            .record_command_completed(
                "command-one",
                RelayCommandOutcome::Prompt {
                    stop_reason: "end_turn".into(),
                },
            )
            .unwrap();
        let second = relay.claim_pending_commands(true).unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].command_id, "command-two");

        // A relay/process restart interrupts only the command actually handed
        // to ACP and then continues the durable offline queue.
        drop(relay);
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        let third = relay.claim_pending_commands(true).unwrap();
        assert_eq!(third.len(), 1);
        assert_eq!(third[0].command_id, "command-three");
        assert!(retained_events(&relay).iter().any(|event| matches!(
            &event.observation,
            RelayObservation::CommandInterrupted { command_id, .. }
                if command_id == "command-two"
        )));
    }

    #[test]
    fn claims_wait_for_the_current_acp_session_configuration() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        submit_relay(&mut relay, "wait-for-config", prompt("later"));

        assert!(relay.claim_pending_commands(false).unwrap().is_empty());
        let claimed = relay.claim_pending_commands(true).unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].command_id, "wait-for-config");
    }

    #[test]
    fn restart_finishes_a_durably_accepted_queue_removal() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        submit_relay(&mut relay, "active-prompt", prompt("active"));
        submit_relay(&mut relay, "queued-prompt", prompt("remove me"));
        relay
            .append_relay_event(
                Some("remove-after-crash"),
                RelayObservation::CommandQueued {
                    command_id: "remove-after-crash".into(),
                    command: RelayCommand::RemoveQueuedPrompt {
                        queued_command_id: "queued-prompt".into(),
                    },
                    created_at_ms: now_unix_millis(),
                },
            )
            .unwrap();
        drop(relay);

        let relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        assert!(relay.operational_state().queued_prompts.is_empty());
        assert!(retained_events(&relay).iter().any(|event| matches!(
            &event.observation,
            RelayObservation::CommandCompleted {
                command_id,
                outcome: RelayCommandOutcome::QueueChanged { removed_command_ids },
            } if command_id == "remove-after-crash"
                && removed_command_ids == &["queued-prompt".to_owned()]
        )));
    }

    #[test]
    fn restart_finishes_a_durably_accepted_queue_clear() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        submit_relay(&mut relay, "active-prompt", prompt("active"));
        submit_relay(&mut relay, "queued-one", prompt("one"));
        submit_relay(&mut relay, "queued-two", prompt("two"));
        relay
            .append_relay_event(
                Some("clear-after-crash"),
                RelayObservation::CommandQueued {
                    command_id: "clear-after-crash".into(),
                    command: RelayCommand::ClearQueuedPrompts,
                    created_at_ms: now_unix_millis(),
                },
            )
            .unwrap();
        drop(relay);

        let relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        assert!(relay.operational_state().queued_prompts.is_empty());
        assert!(retained_events(&relay).iter().any(|event| matches!(
            &event.observation,
            RelayObservation::CommandCompleted {
                command_id,
                outcome: RelayCommandOutcome::QueueChanged { removed_command_ids },
            } if command_id == "clear-after-crash"
                && removed_command_ids
                    == &["queued-one".to_owned(), "queued-two".to_owned()]
        )));
    }

    #[test]
    fn restart_finishes_checkpoint_completion_before_releasing_ownerless_barriers() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        let ready = ready_checkpoint(&mut relay, "barrier-command");
        relay
            .append_relay_event(
                Some("complete-after-crash"),
                RelayObservation::CommandQueued {
                    command_id: "complete-after-crash".into(),
                    command: RelayCommand::CompleteCheckpoint {
                        barrier_command_id: "barrier-command".into(),
                    },
                    created_at_ms: now_unix_millis(),
                },
            )
            .unwrap();
        drop(relay);

        let relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        assert_eq!(relay.snapshot.recovery_floor_ordinal, ready.ordinal);
        assert!(relay.snapshot.checkpoint_barrier.is_none());
        assert!(!retained_events(&relay).iter().any(|event| matches!(
            &event.observation,
            RelayObservation::CommandInterrupted { command_id, .. }
                if command_id == "barrier-command"
        )));
    }

    #[test]
    fn restart_interrupts_ownerless_checkpoint_but_preserves_accepted_close() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        let expected = ready_checkpoint(&mut relay, "close-barrier");
        submit_relay(
            &mut relay,
            "accepted-close",
            RelayCommand::Close {
                barrier_command_id: "close-barrier".into(),
                expected,
            },
        );
        drop(relay);

        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        assert!(retained_events(&relay).iter().any(|event| matches!(
            &event.observation,
            RelayObservation::CommandInterrupted {
                command_id,
                command: RelayCommandKind::BeginCheckpoint,
                ..
            } if command_id == "close-barrier"
        )));
        assert_eq!(
            relay.operational_state().execution,
            RelayExecutionState::Closing
        );
        let claimed = relay.claim_pending_commands(true).unwrap();
        assert!(matches!(
            claimed.as_slice(),
            [ClaimedRelayCommand {
                command_id,
                command: RelayCommand::Close { .. },
                ..
            }] if command_id == "accepted-close"
        ));
    }

    #[test]
    fn relay_operational_state_tracks_mutable_acp_options_and_commands() {
        use agent_client_protocol::schema::v1::{
            AvailableCommandsUpdate, ConfigOptionUpdate, CurrentModeUpdate,
            SessionConfigSelectOption,
        };

        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        let option = SessionConfigOption::select(
            "thinking",
            "Thinking",
            "on",
            vec![SessionConfigSelectOption::new("on", "On")],
        );
        relay
            .record_session_update(SessionUpdate::ConfigOptionUpdate(ConfigOptionUpdate::new(
                vec![option.clone()],
            )))
            .unwrap();
        relay
            .record_session_update(SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new(
                "plan",
            )))
            .unwrap();
        relay
            .record_session_update(SessionUpdate::AvailableCommandsUpdate(
                AvailableCommandsUpdate::new(vec![AvailableCommand::new(
                    "review",
                    "Review the current work",
                )]),
            ))
            .unwrap();

        let state = relay.operational_state();
        assert_eq!(state.config_options, vec![option]);
        assert_eq!(state.config["mode"], "plan");
        assert_eq!(state.available_commands[0].name, "review");
    }

    #[test]
    fn relay_command_submission_is_idempotent_across_restart() {
        let temp = tempfile::tempdir().unwrap();
        let first_ordinal;
        {
            let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
            first_ordinal = submit_relay(&mut relay, "stable-command", prompt("once"));
        }
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        let repeated = submit_relay(&mut relay, "stable-command", prompt("once"));
        assert_eq!(repeated, first_ordinal);
        assert_eq!(
            retained_events(&relay)
                .iter()
                .filter(|event| matches!(
                    &event.observation,
                    RelayObservation::CommandQueued { command_id, .. }
                        if command_id == "stable-command"
                ))
                .count(),
            1
        );

        let response = relay.handle(relay_request(
            "request-conflict",
            RelayRequest::Submit {
                command_id: "stable-command".into(),
                command: prompt("different"),
            },
        ));
        assert!(matches!(
            response.body,
            RelayResponseBody::Error {
                error: RelayProtocolError {
                    code: RelayErrorCode::InvalidRequest,
                    ..
                }
            }
        ));
    }

    #[test]
    fn command_idempotency_survives_ack_until_checkpoint_covers_terminal_event() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        let accepted = submit_relay(&mut relay, "checkpointed-command", prompt("once"));
        assert_eq!(
            relay.claim_pending_commands(true).unwrap()[0].command_id,
            "checkpointed-command"
        );
        relay
            .record_command_completed(
                "checkpointed-command",
                RelayCommandOutcome::Prompt {
                    stop_reason: "end_turn".into(),
                },
            )
            .unwrap();
        let terminal = relay.latest_ordinal();
        attach_relay(&mut relay, "attach-idempotency", 0);
        acknowledge_relay(&mut relay, "ack-idempotency", terminal);
        assert_eq!(
            submit_relay(&mut relay, "checkpointed-command", prompt("once")),
            accepted,
            "ACK must not prune the stable command ID"
        );

        let ready = ready_checkpoint(&mut relay, "idempotency-barrier");
        acknowledge_relay(&mut relay, "ack-idempotency-barrier", ready.ordinal);
        submit_relay(
            &mut relay,
            "complete-idempotency-barrier",
            RelayCommand::CompleteCheckpoint {
                barrier_command_id: "idempotency-barrier".into(),
            },
        );
        let accepted_again = submit_relay(&mut relay, "checkpointed-command", prompt("once"));
        assert!(accepted_again > accepted);
    }

    #[test]
    fn checkpoint_barrier_pauses_offline_prompt_promotion_until_release() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        submit_relay(
            &mut relay,
            "barrier-command",
            RelayCommand::BeginCheckpoint {
                reason: Some("test".into()),
            },
        );
        submit_relay(&mut relay, "after-barrier", prompt("later"));
        let claimed = relay.claim_pending_commands(true).unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].command_id, "barrier-command");
        relay.record_checkpoint_ready("barrier-command").unwrap();
        assert!(relay.claim_pending_commands(true).unwrap().is_empty());
        assert!(relay.operational_state().active_prompt.is_none());

        submit_relay(
            &mut relay,
            "release-command",
            RelayCommand::CompleteCheckpoint {
                barrier_command_id: "barrier-command".into(),
            },
        );
        let claimed = relay.claim_pending_commands(true).unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].command_id, "after-barrier");
    }

    #[test]
    fn controller_disconnect_cannot_leave_checkpoint_barrier_paused() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        submit_relay(
            &mut relay,
            "barrier-disconnect",
            RelayCommand::BeginCheckpoint { reason: None },
        );
        submit_relay(&mut relay, "queued-offline", prompt("continue"));
        assert_eq!(relay.claim_pending_commands(true).unwrap().len(), 1);
        relay.record_checkpoint_ready("barrier-disconnect").unwrap();

        let cancelled = relay
            .cancel_checkpoint_barrier_on_disconnect("barrier-disconnect")
            .unwrap();
        assert!(cancelled.is_some());
        assert!(relay.operational_state().checkpoint_barrier.is_none());
        let next = relay.claim_pending_commands(true).unwrap();
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].command_id, "queued-offline");
    }

    /// The controller releases dispatch once the archive exists on the target,
    /// long before that archive is installed. Journal history must stay put
    /// until an installed archive covers it.
    #[test]
    fn releasing_a_checkpoint_resumes_dispatch_without_moving_the_recovery_floor() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        let ready = ready_checkpoint(&mut relay, "released-barrier");
        submit_relay(&mut relay, "queued-during-release", prompt("later"));
        attach_relay(&mut relay, "attach-release", 0);
        acknowledge_relay(&mut relay, "ack-release", ready.ordinal);
        assert!(relay.claim_pending_commands(true).unwrap().is_empty());
        let floor_before = relay.snapshot.recovery_floor_ordinal;
        let retained_before = relay.snapshot.retained_through();

        submit_relay(
            &mut relay,
            "release-command",
            RelayCommand::ReleaseCheckpoint {
                barrier_command_id: "released-barrier".into(),
            },
        );

        assert!(relay.operational_state().checkpoint_barrier.is_none());
        assert!(relay.operational_state().checkpoint_ready.is_none());
        assert_eq!(relay.snapshot.recovery_floor_ordinal, floor_before);
        assert_eq!(relay.snapshot.retained_through(), retained_before);
        let claimed = relay.claim_pending_commands(true).unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].command_id, "queued-during-release");
        // The released barrier is terminal, so the controller connection that
        // opened it can drop without cancelling anything.
        assert!(
            relay
                .cancel_checkpoint_barrier_on_disconnect("released-barrier")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn releasing_a_checkpoint_requires_that_exact_ready_barrier() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        let missing = submit_release(&mut relay, "release-without-barrier", "no-such-barrier");
        assert!(matches!(
            missing.body,
            RelayResponseBody::Error {
                error: RelayProtocolError {
                    code: RelayErrorCode::InvalidState,
                    ..
                }
            }
        ));

        submit_relay(
            &mut relay,
            "unready-barrier",
            RelayCommand::BeginCheckpoint { reason: None },
        );
        assert_eq!(relay.claim_pending_commands(true).unwrap().len(), 1);
        let unready = submit_release(&mut relay, "release-unready", "unready-barrier");
        assert!(matches!(
            unready.body,
            RelayResponseBody::Error {
                error: RelayProtocolError {
                    code: RelayErrorCode::InvalidState,
                    ..
                }
            }
        ));

        relay.record_checkpoint_ready("unready-barrier").unwrap();
        let wrong = submit_release(&mut relay, "release-wrong", "another-barrier");
        assert!(matches!(
            wrong.body,
            RelayResponseBody::Error {
                error: RelayProtocolError {
                    code: RelayErrorCode::InvalidState,
                    ..
                }
            }
        ));
        assert_eq!(
            relay.operational_state().checkpoint_barrier.as_deref(),
            Some("unready-barrier")
        );
    }

    /// Installing the archive is what earns the journal release, and the
    /// recovery floor is how the relay records it.
    #[test]
    fn advancing_the_recovery_floor_releases_history_only_forward_and_on_chain() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        let ready = ready_checkpoint(&mut relay, "installed-barrier");
        submit_relay(
            &mut relay,
            "release-installed",
            RelayCommand::ReleaseCheckpoint {
                barrier_command_id: "installed-barrier".into(),
            },
        );
        // Acknowledge past the ready cursor so the recovery floor alone decides
        // what the relay retains.
        attach_relay(&mut relay, "attach-floor", 0);
        let acknowledged = relay.latest_ordinal();
        acknowledge_relay(&mut relay, "ack-floor", acknowledged);
        assert_eq!(relay.snapshot.retained_through(), 0);

        let mismatched = submit_floor(
            &mut relay,
            "floor-wrong-digest",
            RelayCursor {
                ordinal: ready.ordinal,
                digest: "b".repeat(64),
            },
        );
        assert!(matches!(
            mismatched.body,
            RelayResponseBody::Error {
                error: RelayProtocolError {
                    code: RelayErrorCode::InvalidState,
                    ..
                }
            }
        ));
        let beyond_frontier = RelayCursor {
            ordinal: relay.latest_ordinal() + 1,
            digest: relay.snapshot.latest_digest.clone(),
        };
        let ahead = submit_floor(&mut relay, "floor-ahead", beyond_frontier);
        assert!(matches!(
            ahead.body,
            RelayResponseBody::Error {
                error: RelayProtocolError {
                    code: RelayErrorCode::InvalidState,
                    ..
                }
            }
        ));
        assert_eq!(relay.snapshot.recovery_floor_ordinal, 0);

        submit_relay(
            &mut relay,
            "floor-installed",
            RelayCommand::AdvanceRecoveryFloor {
                through: ready.clone(),
            },
        );
        assert_eq!(relay.snapshot.recovery_floor_ordinal, ready.ordinal);
        assert_eq!(relay.snapshot.recovery_floor_digest, ready.digest);
        assert_eq!(relay.snapshot.retained_through(), ready.ordinal);

        let backwards = submit_floor(
            &mut relay,
            "floor-backwards",
            RelayCursor {
                ordinal: 0,
                digest: RELAY_EVENT_GENESIS_DIGEST.to_owned(),
            },
        );
        assert!(matches!(
            backwards.body,
            RelayResponseBody::Error {
                error: RelayProtocolError {
                    code: RelayErrorCode::InvalidState,
                    ..
                }
            }
        ));
        assert_eq!(relay.snapshot.recovery_floor_ordinal, ready.ordinal);
    }

    /// The legacy one-step completion is unchanged: it both resumes dispatch
    /// and advances the recovery floor.
    #[test]
    fn completing_a_checkpoint_still_resumes_dispatch_and_advances_the_floor() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        let ready = ready_checkpoint(&mut relay, "completed-barrier");
        submit_relay(&mut relay, "queued-during-completion", prompt("later"));
        attach_relay(&mut relay, "attach-completion", 0);
        acknowledge_relay(&mut relay, "ack-completion", ready.ordinal);

        submit_relay(
            &mut relay,
            "complete-command",
            RelayCommand::CompleteCheckpoint {
                barrier_command_id: "completed-barrier".into(),
            },
        );

        assert!(relay.operational_state().checkpoint_barrier.is_none());
        assert_eq!(relay.snapshot.recovery_floor_ordinal, ready.ordinal);
        assert_eq!(relay.snapshot.retained_through(), ready.ordinal);
        let claimed = relay.claim_pending_commands(true).unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].command_id, "queued-during-completion");
    }

    #[test]
    fn checkpoint_barriers_are_serialized_and_only_exact_completion_releases_them() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        submit_relay(
            &mut relay,
            "first-barrier",
            RelayCommand::BeginCheckpoint { reason: None },
        );
        submit_relay(
            &mut relay,
            "second-barrier",
            RelayCommand::BeginCheckpoint { reason: None },
        );

        let first = relay.claim_pending_commands(true).unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].command_id, "first-barrier");
        relay.record_checkpoint_ready("first-barrier").unwrap();
        assert!(relay.claim_pending_commands(true).unwrap().is_empty());

        let wrong = relay.handle(relay_request(
            "wrong-completion",
            RelayRequest::Submit {
                command_id: "wrong-complete-command".into(),
                command: RelayCommand::CompleteCheckpoint {
                    barrier_command_id: "second-barrier".into(),
                },
            },
        ));
        assert!(matches!(
            wrong.body,
            RelayResponseBody::Error {
                error: RelayProtocolError {
                    code: RelayErrorCode::InvalidState,
                    ..
                }
            }
        ));

        submit_relay(
            &mut relay,
            "complete-first",
            RelayCommand::CompleteCheckpoint {
                barrier_command_id: "first-barrier".into(),
            },
        );
        let second = relay.claim_pending_commands(true).unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].command_id, "second-barrier");
    }

    #[test]
    fn checkpoint_waits_for_earlier_queued_control_and_freezes_later_control() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        submit_relay(
            &mut relay,
            "config-before",
            RelayCommand::SetConfig {
                key: "model".into(),
                value: "before".into(),
            },
        );
        submit_relay(
            &mut relay,
            "control-barrier",
            RelayCommand::BeginCheckpoint { reason: None },
        );

        let control = relay.claim_pending_commands(true).unwrap();
        assert_eq!(control.len(), 1);
        assert_eq!(control[0].command_id, "config-before");
        relay
            .record_command_completed("config-before", RelayCommandOutcome::Configured)
            .unwrap();
        assert_eq!(relay.operational_state().config["model"], "before");

        let barrier = relay.claim_pending_commands(true).unwrap();
        assert_eq!(barrier.len(), 1);
        assert_eq!(barrier[0].command_id, "control-barrier");
        relay.record_checkpoint_ready("control-barrier").unwrap();
        submit_relay(
            &mut relay,
            "config-after",
            RelayCommand::SetConfig {
                key: "model".into(),
                value: "after".into(),
            },
        );
        assert!(relay.claim_pending_commands(true).unwrap().is_empty());

        submit_relay(
            &mut relay,
            "complete-control-barrier",
            RelayCommand::CompleteCheckpoint {
                barrier_command_id: "control-barrier".into(),
            },
        );
        let later = relay.claim_pending_commands(true).unwrap();
        assert_eq!(later.len(), 1);
        assert_eq!(later[0].command_id, "config-after");
    }

    #[test]
    fn a_recorded_notice_becomes_one_verbatim_system_line() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        let text = "This session moved from /home/dev/project into a container.";

        submit_relay(
            &mut relay,
            "resume-notice-1",
            RelayCommand::RecordNotice { text: text.into() },
        );

        // A notice never reaches ACP: it completes inside the relay.
        assert!(relay.claim_pending_commands(true).unwrap().is_empty());
        let mut session = crate::hel_state::MaterializedSession::empty(SESSION);
        for event in relay.events_after(0, RELAY_EVENT_GENESIS_DIGEST).unwrap() {
            let projected = crate::hel_projection::project_relay_event(&session, &event).unwrap();
            crate::hel_projection::apply_committed_projection_event(
                &mut session,
                &event,
                projected.mutation,
            )
            .unwrap();
        }

        let notices = session
            .transcript
            .iter()
            .filter_map(|item| match &item.body {
                crate::hel_state::TranscriptBody::System { text } => Some(text.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(notices, vec![text.to_owned()]);
    }

    #[test]
    fn a_repeated_notice_append_still_leaves_one_conversation_line() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        let text = "The working tree moved while this session was paused.";
        submit_relay(
            &mut relay,
            "resume-notice-1",
            RelayCommand::RecordNotice { text: text.into() },
        );
        // Stand in for a retry that re-appended the notice after a transient
        // persistence failure reported a durable append as unfinished.
        relay
            .append_relay_event(
                Some("resume-notice-1"),
                RelayObservation::Notice {
                    message: text.into(),
                },
            )
            .unwrap();

        let mut session = crate::hel_state::MaterializedSession::empty(SESSION);
        for event in relay.events_after(0, RELAY_EVENT_GENESIS_DIGEST).unwrap() {
            let projected = crate::hel_projection::project_relay_event(&session, &event).unwrap();
            crate::hel_projection::apply_committed_projection_event(
                &mut session,
                &event,
                projected.mutation,
            )
            .unwrap();
        }

        assert_eq!(session.transcript.len(), 1);
    }

    #[test]
    fn cancel_dispatches_while_the_prompt_it_targets_is_in_flight() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        submit_relay(&mut relay, "cancelled-prompt", prompt("keep running"));
        let prompt = relay.claim_pending_commands(true).unwrap();
        assert_eq!(prompt.len(), 1);
        assert_eq!(prompt[0].command_id, "cancelled-prompt");

        submit_relay(&mut relay, "cancel-command", RelayCommand::Cancel);
        let cancel = relay.claim_pending_commands(true).unwrap();
        assert_eq!(cancel.len(), 1);
        assert_eq!(cancel[0].command_id, "cancel-command");
        assert!(matches!(cancel[0].command, RelayCommand::Cancel));
    }

    #[test]
    fn config_accepted_during_a_prompt_waits_while_cancel_bypasses_it() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        submit_relay(&mut relay, "active-prompt", prompt("keep running"));
        assert_eq!(
            relay.claim_pending_commands(true).unwrap()[0].command_id,
            "active-prompt"
        );

        submit_relay(
            &mut relay,
            "config-after-prompt",
            set_config("model", "later"),
        );
        submit_relay(&mut relay, "cancel-after-config", RelayCommand::Cancel);

        let cancel = relay.claim_pending_commands(true).unwrap();
        assert_eq!(cancel.len(), 1);
        assert_eq!(cancel[0].command_id, "cancel-after-config");
        assert!(matches!(cancel[0].command, RelayCommand::Cancel));
        relay
            .record_command_completed("cancel-after-config", RelayCommandOutcome::Cancelled)
            .unwrap();
        assert!(relay.claim_pending_commands(true).unwrap().is_empty());

        relay
            .record_command_completed(
                "active-prompt",
                RelayCommandOutcome::Prompt {
                    stop_reason: "cancelled".into(),
                },
            )
            .unwrap();
        let config = relay.claim_pending_commands(true).unwrap();
        assert_eq!(config.len(), 1);
        assert_eq!(config[0].command_id, "config-after-prompt");
    }

    #[test]
    fn config_accepted_before_a_prompt_keeps_acceptance_order() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        submit_relay(&mut relay, "config-first", set_config("model", "first"));
        submit_relay(&mut relay, "prompt-second", prompt("then run"));

        // Queue entries run one at a time, so the prompt waits for the
        // configuration change accepted before it.
        let config = relay.claim_pending_commands(true).unwrap();
        assert_eq!(config.len(), 1);
        assert_eq!(config[0].command_id, "config-first");
        assert!(relay.claim_pending_commands(true).unwrap().is_empty());

        relay
            .record_command_completed("config-first", RelayCommandOutcome::Configured)
            .unwrap();
        let claimed = relay.claim_pending_commands(true).unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].command_id, "prompt-second");
    }

    #[test]
    fn config_queued_behind_a_prompt_applies_in_queue_order() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        submit_relay(&mut relay, "prompt-one", prompt("one"));
        assert_eq!(
            relay.claim_pending_commands(true).unwrap()[0].command_id,
            "prompt-one"
        );

        submit_relay(&mut relay, "prompt-two", prompt("two"));
        submit_relay(&mut relay, "config-third", set_config("model", "sonnet"));
        submit_relay(&mut relay, "prompt-four", prompt("four"));
        assert_eq!(
            queued_command_ids(&relay),
            ["prompt-two", "config-third", "prompt-four"]
        );
        assert!(relay.claim_pending_commands(true).unwrap().is_empty());

        finish_prompt(&mut relay, "prompt-one");
        assert_eq!(
            relay.claim_pending_commands(true).unwrap()[0].command_id,
            "prompt-two"
        );
        finish_prompt(&mut relay, "prompt-two");

        let config = relay.claim_pending_commands(true).unwrap();
        assert_eq!(config.len(), 1);
        assert_eq!(config[0].command_id, "config-third");
        assert!(matches!(config[0].command, RelayCommand::SetConfig { .. }));
        // A configuration change applies between turns, so the relay stays
        // idle and the prompt behind it waits for the change to finish.
        assert_eq!(
            relay.operational_state().execution,
            RelayExecutionState::Idle
        );
        assert!(relay.claim_pending_commands(true).unwrap().is_empty());

        relay
            .record_command_completed("config-third", RelayCommandOutcome::Configured)
            .unwrap();
        assert_eq!(
            relay
                .operational_state()
                .config
                .get("model")
                .map(String::as_str),
            Some("sonnet")
        );
        assert_eq!(
            relay.claim_pending_commands(true).unwrap()[0].command_id,
            "prompt-four"
        );
    }

    #[test]
    fn removing_a_queued_config_stops_it_from_dispatching() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        submit_relay(&mut relay, "active-prompt", prompt("running"));
        assert_eq!(
            relay.claim_pending_commands(true).unwrap()[0].command_id,
            "active-prompt"
        );
        submit_relay(&mut relay, "config-queued", set_config("effort", "high"));
        submit_relay(
            &mut relay,
            "remove-config",
            RelayCommand::RemoveQueuedPrompt {
                queued_command_id: "config-queued".into(),
            },
        );

        assert!(queued_command_ids(&relay).is_empty());
        assert_eq!(
            relay.snapshot.dispatches["config-queued"].state,
            RelayDispatchState::Rejected
        );
        assert!(
            relay.snapshot.handled_commands["config-queued"]
                .terminal_ordinal
                .is_some()
        );

        finish_prompt(&mut relay, "active-prompt");
        assert!(relay.claim_pending_commands(true).unwrap().is_empty());
        assert!(relay.operational_state().config.is_empty());
    }

    #[test]
    fn clearing_the_queue_drops_queued_configuration_changes() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        submit_relay(&mut relay, "active-prompt", prompt("running"));
        assert_eq!(
            relay.claim_pending_commands(true).unwrap()[0].command_id,
            "active-prompt"
        );
        submit_relay(&mut relay, "queued-prompt", prompt("later"));
        submit_relay(&mut relay, "queued-config", set_config("model", "later"));

        submit_relay(&mut relay, "clear-queue", RelayCommand::ClearQueuedPrompts);
        assert!(queued_command_ids(&relay).is_empty());

        finish_prompt(&mut relay, "active-prompt");
        assert!(relay.claim_pending_commands(true).unwrap().is_empty());
    }

    #[test]
    fn restart_redispatches_a_promoted_configuration_change() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        submit_relay(&mut relay, "config-command", set_config("model", "sonnet"));
        // The change was started but never handed to ACP.
        assert!(queued_command_ids(&relay).is_empty());
        drop(relay);

        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        let claimed = relay.claim_pending_commands(true).unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].command_id, "config-command");
        assert!(matches!(claimed[0].command, RelayCommand::SetConfig { .. }));
    }

    #[test]
    fn restart_adopts_a_config_accepted_outside_the_durable_queue() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        // The prompt is started but never handed to ACP, so restarting keeps
        // it active instead of interrupting it.
        submit_relay(&mut relay, "active-prompt", prompt("running"));
        submit_relay(&mut relay, "config-queued", set_config("model", "sonnet"));
        drop(relay);

        // Rewrite the durable snapshot the way an older relay wrote it: the
        // configuration change is accepted, but not in the command queue.
        let state_path = temp.path().join(RELAY_STATE_FILE);
        let mut state: Value = serde_json::from_slice(&fs::read(&state_path).unwrap()).unwrap();
        state["queued_prompts"]
            .as_array_mut()
            .unwrap()
            .retain(|queued| queued["command_id"] != "config-queued");
        fs::write(&state_path, serde_json::to_vec(&state).unwrap()).unwrap();

        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        assert_eq!(queued_command_ids(&relay), ["config-queued"]);
        assert_eq!(
            relay.claim_pending_commands(true).unwrap()[0].command_id,
            "active-prompt"
        );
        finish_prompt(&mut relay, "active-prompt");
        assert_eq!(
            relay.claim_pending_commands(true).unwrap()[0].command_id,
            "config-queued"
        );
    }

    #[test]
    fn queue_entries_written_before_config_changes_still_load() {
        let stored: StoredQueuedRelayCommand = serde_json::from_value(serde_json::json!({
            "command_id": "queued-1",
            "prompt": [{"type": "text", "text": "hello"}],
            "created_at_ms": 7,
        }))
        .unwrap();
        assert!(matches!(
            stored.payload,
            StoredQueuedRelayPayload::Prompt { .. }
        ));

        let config = StoredQueuedRelayCommand {
            command_id: "queued-2".into(),
            payload: StoredQueuedRelayPayload::SetConfig {
                key: "model".into(),
                value: "sonnet".into(),
            },
            created_at_ms: 8,
        };
        let encoded = serde_json::to_value(&config).unwrap();
        assert_eq!(encoded["key"], "model");
        assert_eq!(
            serde_json::from_value::<StoredQueuedRelayCommand>(encoded).unwrap(),
            config
        );
    }

    #[test]
    fn an_incomplete_configuration_change_is_refused() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        let response = relay.handle(relay_request(
            "reject-empty-config",
            RelayRequest::Submit {
                command_id: "empty-config".into(),
                command: set_config("model", "  "),
            },
        ));
        assert!(matches!(
            response.body,
            RelayResponseBody::Error {
                error: RelayProtocolError {
                    code: RelayErrorCode::InvalidRequest,
                    ..
                }
            }
        ));
        assert!(queued_command_ids(&relay).is_empty());
    }

    #[test]
    fn close_requires_exact_checkpoint_cut_and_survives_controller_disconnect() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        let stale_cut = ready_checkpoint(&mut relay, "stale-close-barrier");
        relay
            .record_observation(RelayObservation::Warning {
                message: "post-cut drift".into(),
            })
            .unwrap();
        let rejected = relay.handle(relay_request(
            "reject-stale-close",
            RelayRequest::Submit {
                command_id: "stale-close-command".into(),
                command: RelayCommand::Close {
                    barrier_command_id: "stale-close-barrier".into(),
                    expected: stale_cut,
                },
            },
        ));
        assert!(matches!(
            rejected.body,
            RelayResponseBody::Error {
                error: RelayProtocolError {
                    code: RelayErrorCode::InvalidState,
                    ..
                }
            }
        ));
        relay
            .cancel_checkpoint_barrier_on_disconnect("stale-close-barrier")
            .unwrap();

        let exact_cut = ready_checkpoint(&mut relay, "exact-close-barrier");
        let accepted = submit_relay(
            &mut relay,
            "exact-close-command",
            RelayCommand::Close {
                barrier_command_id: "exact-close-barrier".into(),
                expected: exact_cut.clone(),
            },
        );
        assert!(accepted > exact_cut.ordinal);
        assert_eq!(
            relay.operational_state().execution,
            RelayExecutionState::Closing
        );
        let later = relay.handle(relay_request(
            "post-close-command",
            RelayRequest::Submit {
                command_id: "post-close-prompt".into(),
                command: prompt("must not run"),
            },
        ));
        assert!(matches!(
            later.body,
            RelayResponseBody::Error {
                error: RelayProtocolError {
                    code: RelayErrorCode::InvalidState,
                    ..
                }
            }
        ));

        assert!(
            relay
                .cancel_checkpoint_barrier_on_disconnect("exact-close-barrier")
                .unwrap()
                .is_some()
        );
        let close = relay.claim_pending_commands(true).unwrap();
        assert_eq!(close.len(), 1);
        assert_eq!(close[0].command_id, "exact-close-command");
        assert!(matches!(close[0].command, RelayCommand::Close { .. }));
    }

    #[test]
    fn exact_close_allows_checkpoint_completion_before_close_dispatch() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        let expected = ready_checkpoint(&mut relay, "normal-close-barrier");
        submit_relay(
            &mut relay,
            "normal-close-command",
            RelayCommand::Close {
                barrier_command_id: "normal-close-barrier".into(),
                expected,
            },
        );
        submit_relay(
            &mut relay,
            "normal-close-complete",
            RelayCommand::CompleteCheckpoint {
                barrier_command_id: "normal-close-barrier".into(),
            },
        );

        let close = relay.claim_pending_commands(true).unwrap();
        assert_eq!(close.len(), 1);
        assert_eq!(close[0].command_id, "normal-close-command");
        assert!(matches!(close[0].command, RelayCommand::Close { .. }));
    }

    #[test]
    fn acknowledgement_only_garbage_collects_through_a_verified_checkpoint() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        relay
            .record_observation(RelayObservation::Warning {
                message: "remember me".into(),
            })
            .unwrap();
        let attach = attach_relay(&mut relay, "attach-1", 0);
        assert!(matches!(
            attach.body,
            RelayResponseBody::Ok {
                payload: RelayResponsePayload::Attached {
                    ref events,
                    through_ordinal: 1,
                    ..
                }
            } if events.len() == 1
        ));
        let acknowledged = acknowledge_relay(&mut relay, "ack-1", 1);
        assert!(matches!(
            acknowledged.body,
            RelayResponseBody::Ok {
                payload: RelayResponsePayload::Acknowledged {
                    through_ordinal: 1,
                    ..
                }
            }
        ));
        assert_eq!(
            retained_events(&relay).len(),
            1,
            "ACK alone is not a recovery cut"
        );

        let ready = ready_checkpoint(&mut relay, "gc-barrier");
        acknowledge_relay(&mut relay, "ack-checkpoint", ready.ordinal);
        submit_relay(
            &mut relay,
            "complete-checkpoint",
            RelayCommand::CompleteCheckpoint {
                barrier_command_id: "gc-barrier".into(),
            },
        );
        assert!(
            retained_events(&relay)
                .iter()
                .all(|event| event.ordinal > ready.ordinal)
        );

        drop(relay);
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        let stale = attach_relay(&mut relay, "attach-stale", 0);
        assert!(matches!(
            stale.body,
            RelayResponseBody::Error {
                error: RelayProtocolError {
                    code: RelayErrorCode::Desynchronized,
                    detail: Some(RelayErrorDetail::Desynchronized {
                        earliest_available,
                        ..
                    }),
                    ..
                }
            } if earliest_available == ready.ordinal
        ));
    }

    #[test]
    fn relay_recovers_an_event_fsynced_before_its_snapshot() {
        let temp = tempfile::tempdir().unwrap();
        let relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        drop(relay);
        let event = RelayEvent {
            ordinal: 1,
            previous_digest: RELAY_EVENT_GENESIS_DIGEST.to_owned(),
            digest: String::new(),
            recorded_at_ms: now_unix_millis(),
            command_id: None,
            observation: RelayObservation::Warning {
                message: "after journal fsync".into(),
            },
        };
        let event = RelayEvent {
            digest: relay_event_digest(&event).unwrap(),
            ..event
        };
        let path = temp
            .path()
            .join(RELAY_JOURNAL_DIR)
            .join(RELAY_ACTIVE_SEGMENT);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        serde_json::to_writer(&mut file, &event).unwrap();
        file.write_all(b"\n").unwrap();
        file.sync_all().unwrap();

        let relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        assert_eq!(relay.latest_ordinal(), 1);
        assert_eq!(
            relay.events_after(0, RELAY_EVENT_GENESIS_DIGEST).unwrap(),
            vec![event]
        );
    }

    #[test]
    fn relay_truncates_a_torn_active_tail_before_appending_again() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        relay
            .record_observation(RelayObservation::Warning {
                message: "durable prefix".into(),
            })
            .unwrap();
        let active = temp
            .path()
            .join(RELAY_JOURNAL_DIR)
            .join(RELAY_ACTIVE_SEGMENT);
        let durable_len = active.metadata().unwrap().len();
        drop(relay);

        let mut file = OpenOptions::new().append(true).open(&active).unwrap();
        file.write_all(br#"{"ordinal":2,"previous_digest":"#)
            .unwrap();
        file.sync_all().unwrap();
        drop(file);
        assert!(active.metadata().unwrap().len() > durable_len);

        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        assert_eq!(relay.latest_ordinal(), 1);
        assert_eq!(active.metadata().unwrap().len(), durable_len);
        relay
            .record_observation(RelayObservation::Warning {
                message: "after repair".into(),
            })
            .unwrap();
        drop(relay);

        let relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        assert_eq!(relay.latest_ordinal(), 2);
        assert_eq!(retained_events(&relay).len(), 2);
    }

    #[test]
    fn first_active_journal_file_is_reopenable_after_its_first_append() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        relay
            .record_observation(RelayObservation::Warning {
                message: "first durable event".into(),
            })
            .unwrap();
        drop(relay);

        let relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        assert_eq!(relay.latest_ordinal(), 1);
        assert_eq!(retained_events(&relay).len(), 1);
    }

    #[test]
    fn failed_gc_persistence_keeps_command_idempotency_in_memory() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        let accepted = submit_relay(&mut relay, "keep-idempotent", prompt("once"));
        assert_eq!(
            relay.claim_pending_commands(true).unwrap()[0].command_id,
            "keep-idempotent"
        );
        relay
            .record_command_completed(
                "keep-idempotent",
                RelayCommandOutcome::Prompt {
                    stop_reason: "end_turn".into(),
                },
            )
            .unwrap();
        let terminal = relay.latest_ordinal();
        let digest = relay.latest_digest().to_owned();
        relay.snapshot.acknowledged_through = terminal;
        relay.snapshot.acknowledged_digest.clone_from(&digest);
        relay.snapshot.recovery_floor_ordinal = terminal;
        relay.snapshot.recovery_floor_digest = digest;
        relay.persist_snapshot().unwrap();

        let state_path = temp.path().join(RELAY_STATE_FILE);
        fs::remove_file(&state_path).unwrap();
        fs::create_dir(&state_path).unwrap();
        let error = relay.garbage_collect_relay_history().unwrap_err();
        assert!(format!("{error:#}").contains("relay-state.json"));
        assert!(
            relay
                .snapshot
                .handled_commands
                .contains_key("keep-idempotent")
        );
        assert_eq!(
            submit_relay(&mut relay, "keep-idempotent", prompt("once")),
            accepted
        );
    }

    #[test]
    fn retry_resumes_a_relay_local_command_after_snapshot_persistence_failure() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        let state_path = temp.path().join(RELAY_STATE_FILE);
        fs::remove_file(&state_path).unwrap();
        fs::create_dir(&state_path).unwrap();
        let command = RelayCommand::ClearQueuedPrompts;

        let failed = relay.handle(relay_request(
            "first-local-attempt",
            RelayRequest::Submit {
                command_id: "retry-local".into(),
                command: command.clone(),
            },
        ));
        assert!(matches!(
            failed.body,
            RelayResponseBody::Error {
                error: RelayProtocolError {
                    code: RelayErrorCode::Internal,
                    ..
                }
            }
        ));
        assert_eq!(
            relay.snapshot.dispatches["retry-local"].state,
            RelayDispatchState::Queued
        );

        fs::remove_dir(&state_path).unwrap();
        let retried = relay.handle(relay_request(
            "retry-local-attempt",
            RelayRequest::Submit {
                command_id: "retry-local".into(),
                command,
            },
        ));
        assert!(matches!(
            retried.body,
            RelayResponseBody::Ok {
                payload: RelayResponsePayload::Accepted { .. }
            }
        ));
        assert_eq!(
            relay.snapshot.dispatches["retry-local"].state,
            RelayDispatchState::Completed
        );
    }

    #[test]
    fn duplicate_acknowledgement_retries_incomplete_journal_gc() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        relay
            .record_observation(RelayObservation::Warning {
                message: "collect after retry".into(),
            })
            .unwrap();
        let through = relay.latest_ordinal();
        let digest = relay.latest_digest().to_owned();
        relay.snapshot.acknowledged_through = through;
        relay.snapshot.acknowledged_digest.clone_from(&digest);
        relay.snapshot.recovery_floor_ordinal = through;
        relay.snapshot.recovery_floor_digest.clone_from(&digest);
        relay.persist_snapshot().unwrap();

        let state_path = temp.path().join(RELAY_STATE_FILE);
        fs::remove_file(&state_path).unwrap();
        fs::create_dir(&state_path).unwrap();
        let failed = relay.handle(relay_request(
            "gc-fails-after-ack",
            RelayRequest::Acknowledge {
                through_ordinal: through,
                through_digest: digest.clone(),
            },
        ));
        assert!(matches!(
            failed.body,
            RelayResponseBody::Error {
                error: RelayProtocolError {
                    code: RelayErrorCode::Internal,
                    ..
                }
            }
        ));

        fs::remove_dir(&state_path).unwrap();
        let retried = relay.handle(relay_request(
            "gc-retry-after-ack",
            RelayRequest::Acknowledge {
                through_ordinal: through,
                through_digest: digest.clone(),
            },
        ));
        assert!(matches!(
            retried.body,
            RelayResponseBody::Ok {
                payload: RelayResponsePayload::Acknowledged {
                    through_ordinal,
                    ..
                }
            } if through_ordinal == through
        ));
        assert_eq!(relay.retained_event_body_count(), 0);
        assert!(relay.events_after(through, &digest).unwrap().is_empty());
    }

    #[test]
    fn failed_ack_persistence_does_not_advance_the_live_cursor() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        relay
            .record_observation(RelayObservation::Warning {
                message: "retain until ACK is durable".into(),
            })
            .unwrap();
        let digest = relay.latest_digest().to_owned();
        let state_path = temp.path().join(RELAY_STATE_FILE);
        fs::remove_file(&state_path).unwrap();
        fs::create_dir(&state_path).unwrap();

        let failed = relay.handle(relay_request(
            "ack-persistence-fails",
            RelayRequest::Acknowledge {
                through_ordinal: 1,
                through_digest: digest.clone(),
            },
        ));
        assert!(matches!(
            failed.body,
            RelayResponseBody::Error {
                error: RelayProtocolError {
                    code: RelayErrorCode::Internal,
                    ..
                }
            }
        ));
        assert_eq!(relay.acknowledged_through(), 0);
        assert_eq!(retained_events(&relay).len(), 1);

        fs::remove_dir(&state_path).unwrap();
        let retry = relay.handle(relay_request(
            "ack-persistence-retry",
            RelayRequest::Acknowledge {
                through_ordinal: 1,
                through_digest: digest,
            },
        ));
        assert!(matches!(
            retry.body,
            RelayResponseBody::Ok {
                payload: RelayResponsePayload::Acknowledged {
                    through_ordinal: 1,
                    ..
                }
            }
        ));
    }

    #[test]
    fn failed_claim_persistence_leaves_the_command_claimable() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        submit_relay(&mut relay, "claim-retry", prompt("run once"));
        assert_eq!(
            relay.snapshot.dispatches["claim-retry"].state,
            RelayDispatchState::Pending
        );
        let state_path = temp.path().join(RELAY_STATE_FILE);
        fs::remove_file(&state_path).unwrap();
        fs::create_dir(&state_path).unwrap();

        assert!(relay.claim_pending_commands(true).is_err());
        assert_eq!(
            relay.snapshot.dispatches["claim-retry"].state,
            RelayDispatchState::Pending
        );

        fs::remove_dir(&state_path).unwrap();
        let claimed = relay.claim_pending_commands(true).unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].command_id, "claim-retry");
        assert_eq!(
            relay.snapshot.dispatches["claim-retry"].state,
            RelayDispatchState::InFlight
        );
    }

    #[test]
    fn oversized_commands_are_rejected_before_journaling() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        let response = relay.handle(relay_request(
            "oversized-command",
            RelayRequest::Submit {
                command_id: "oversized-command".into(),
                command: prompt(&"x".repeat(RELAY_COMMAND_BYTE_BUDGET)),
            },
        ));
        assert!(matches!(
            response.body,
            RelayResponseBody::Error {
                error: RelayProtocolError {
                    code: RelayErrorCode::InvalidRequest,
                    ..
                }
            }
        ));
        assert_eq!(relay.latest_ordinal(), 0);
    }

    #[test]
    fn oversized_observations_are_truncated_instead_of_failing() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();

        let ordinal = relay
            .record_observation(RelayObservation::Warning {
                message: "x".repeat(RELAY_EVENT_BYTE_BUDGET),
            })
            .expect("an oversized observation is recorded, not rejected");
        assert_eq!(ordinal, 1);
        assert_eq!(relay.latest_ordinal(), 1);

        let page = relay
            .read_events_after(0, RELAY_REPLAY_BYTE_BUDGET)
            .unwrap();
        let recorded = &page.events[0];
        let RelayObservation::Warning { message } = &recorded.observation else {
            panic!(
                "expected the truncated warning, found {:?}",
                recorded.observation
            );
        };
        assert!(
            message.starts_with("xxxx"),
            "the head of the payload is kept"
        );
        assert!(
            message.contains("[hel truncated"),
            "truncation is disclosed"
        );
        assert!(serde_json::to_vec(recorded).unwrap().len() <= RELAY_EVENT_BYTE_BUDGET);
    }

    /// A relay that truncated an event must still be able to reopen the
    /// journal it wrote — the readback path bounds lines by the same budget.
    #[test]
    fn a_truncated_event_can_be_read_back_after_reopening() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        relay
            .record_session_update(SessionUpdate::AgentMessageChunk(ContentChunk::new(
                ContentBlock::from("y".repeat(3 * 1024 * 1024)),
            )))
            .unwrap();
        drop(relay);

        let reopened = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        assert_eq!(reopened.latest_ordinal(), 1);
    }

    #[test]
    fn operational_state_is_payload_free_and_bounded() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        submit_relay(
            &mut relay,
            "secret-prompt",
            prompt("payload-that-must-not-be-in-operational-state"),
        );
        let encoded = serde_json::to_vec(&relay.operational_state()).unwrap();
        assert!(encoded.len() <= RELAY_STATE_BYTE_BUDGET);
        let encoded = String::from_utf8(encoded).unwrap();
        assert!(encoded.contains("secret-prompt"));
        assert!(!encoded.contains("payload-that-must-not-be-in-operational-state"));

        let half_budget = RELAY_STATE_BYTE_BUDGET / 2;
        relay
            .record_observation(RelayObservation::ConfigurationUpdated {
                key: "large-one".into(),
                value: "a".repeat(half_budget),
            })
            .unwrap();
        let before = relay.latest_ordinal();
        let error = relay
            .record_observation(RelayObservation::ConfigurationUpdated {
                key: "large-two".into(),
                value: "b".repeat(half_budget),
            })
            .unwrap_err();
        assert!(error.to_string().contains("operational state is too large"));
        assert_eq!(relay.latest_ordinal(), before);
    }

    #[test]
    fn event_chain_detects_cursor_and_body_desynchronization() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        relay
            .record_observation(RelayObservation::Warning {
                message: "authentic".into(),
            })
            .unwrap();
        let event = retained_events(&relay)[0].clone();
        validate_relay_event(0, RELAY_EVENT_GENESIS_DIGEST, &event).unwrap();

        let mut tampered = event;
        tampered.observation = RelayObservation::Warning {
            message: "tampered".into(),
        };
        assert!(validate_relay_event(0, RELAY_EVENT_GENESIS_DIGEST, &tampered).is_err());

        let mismatch = relay.handle(relay_request(
            "attach-wrong-digest",
            RelayRequest::Attach {
                after_ordinal: 0,
                after_digest: "a".repeat(64),
            },
        ));
        assert!(matches!(
            mismatch.body,
            RelayResponseBody::Error {
                error: RelayProtocolError {
                    code: RelayErrorCode::Desynchronized,
                    ..
                }
            }
        ));
    }

    #[test]
    fn sealed_relay_segments_replay_and_are_removed_after_checkpointed_ack() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        relay
            .record_observation(RelayObservation::Warning {
                message: "sealed".into(),
            })
            .unwrap();
        let mut metadata = relay
            .journal_spans
            .iter()
            .find(|span| span.path.ends_with(RELAY_ACTIVE_SEGMENT))
            .unwrap()
            .clone();
        seal_active_relay_segment(&temp.path().join(RELAY_JOURNAL_DIR), &mut metadata).unwrap();
        assert!(
            fs::read_dir(temp.path().join(RELAY_JOURNAL_DIR))
                .unwrap()
                .filter_map(|entry| entry.ok())
                .any(|entry| entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "gz"))
        );
        drop(relay);

        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        assert_eq!(
            relay
                .events_after(0, RELAY_EVENT_GENESIS_DIGEST)
                .unwrap()
                .len(),
            1
        );
        let _ = attach_relay(&mut relay, "attach-sealed", 0);
        let _ = acknowledge_relay(&mut relay, "ack-sealed", 1);
        assert!(
            fs::read_dir(temp.path().join(RELAY_JOURNAL_DIR))
                .unwrap()
                .filter_map(|entry| entry.ok())
                .any(|entry| entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "gz"))
        );
        let ready = ready_checkpoint(&mut relay, "sealed-barrier");
        acknowledge_relay(&mut relay, "ack-sealed-checkpoint", ready.ordinal);
        submit_relay(
            &mut relay,
            "sealed-complete",
            RelayCommand::CompleteCheckpoint {
                barrier_command_id: "sealed-barrier".into(),
            },
        );
        assert!(
            !fs::read_dir(temp.path().join(RELAY_JOURNAL_DIR))
                .unwrap()
                .filter_map(|entry| entry.ok())
                .any(|entry| entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "gz"))
        );
    }

    #[test]
    fn large_relay_history_stays_disk_backed_and_replays_across_segments() {
        const EVENT_COUNT: usize = 80;
        const MESSAGE_BYTES: usize = 64 * 1024;

        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        for index in 0..EVENT_COUNT {
            relay
                .record_observation(RelayObservation::Warning {
                    message: format!("{index:04}:{}", "x".repeat(MESSAGE_BYTES)),
                })
                .unwrap();
        }
        assert_eq!(relay.retained_event_body_count(), RELAY_HOT_EVENT_CAPACITY);
        assert!(
            fs::read_dir(temp.path().join(RELAY_JOURNAL_DIR))
                .unwrap()
                .filter_map(|entry| entry.ok())
                .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "gz"))
                .count()
                >= 2,
            "test history did not cross multiple sealed journal files"
        );
        let expected_latest = relay.latest_ordinal();
        let expected_digest = relay.latest_digest().to_owned();
        drop(relay);

        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        assert_eq!(relay.latest_ordinal(), expected_latest);
        assert_eq!(relay.latest_digest(), expected_digest);
        assert_eq!(relay.retained_event_body_count(), RELAY_HOT_EVENT_CAPACITY);

        let mut cursor = RelayCursor {
            ordinal: 0,
            digest: RELAY_EVENT_GENESIS_DIGEST.into(),
        };
        let mut replayed = 0_usize;
        let mut pages = 0_usize;
        while cursor.ordinal < expected_latest {
            let response = relay.handle(relay_request(
                &format!("paged-replay-{pages}"),
                RelayRequest::Attach {
                    after_ordinal: cursor.ordinal,
                    after_digest: cursor.digest.clone(),
                },
            ));
            let RelayResponseBody::Ok {
                payload:
                    RelayResponsePayload::Attached {
                        events,
                        through_ordinal,
                        through_digest,
                        ..
                    },
            } = response.body
            else {
                panic!("disk-backed replay failed: {:?}", response.body);
            };
            assert!(!events.is_empty());
            for event in &events {
                validate_relay_event(cursor.ordinal, &cursor.digest, event).unwrap();
                cursor.ordinal = event.ordinal;
                cursor.digest = event.digest.clone();
            }
            assert_eq!(cursor.ordinal, through_ordinal);
            assert_eq!(cursor.digest, through_digest);
            replayed += events.len();
            pages += 1;
        }
        assert_eq!(replayed, EVENT_COUNT);
        assert!(pages >= 2, "history unexpectedly fit in one replay page");
        assert_eq!(cursor.digest, expected_digest);
    }

    #[test]
    fn relay_has_a_hard_protocol_v1_floor() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        let response = relay.handle(RelayRequestEnvelope {
            request_id: "hello-old".into(),
            protocol_version: 0,
            request: RelayRequest::Hello {
                controller_version: "old".into(),
                supported: RelayVersionRange { min: 0, max: 0 },
            },
        });
        assert!(matches!(
            response.body,
            RelayResponseBody::Error {
                error: RelayProtocolError {
                    code: RelayErrorCode::IncompatibleProtocol,
                    retryable: false,
                    ..
                }
            }
        ));
    }

    #[test]
    fn credential_requests_cannot_enter_durable_relay_state() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();

        for (request_id, request) in [
            ("credential-state", RelayRequest::CredentialState),
            ("read-credentials", RelayRequest::ReadCredentials),
            (
                "install-credentials",
                RelayRequest::InstallCredentials {
                    data: "e30=".into(),
                },
            ),
        ] {
            let response = relay.handle(relay_request(request_id, request));
            assert!(matches!(
                response.body,
                RelayResponseBody::Error {
                    error: RelayProtocolError {
                        code: RelayErrorCode::InvalidState,
                        retryable: false,
                        ..
                    }
                }
            ));
        }

        assert_eq!(relay.latest_ordinal(), 0);
        assert!(
            relay
                .events_after(0, RELAY_EVENT_GENESIS_DIGEST)
                .unwrap()
                .is_empty()
        );
        let persisted = fs::read_to_string(temp.path().join(RELAY_STATE_FILE)).unwrap();
        assert!(!persisted.contains("e30="));
        assert!(relay.snapshot.handled_commands.is_empty());
    }

    #[test]
    fn restored_relay_continues_after_canonical_event_frontier() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(
            temp.path()
                .join(RESTORED_CANONICAL_SESSION_FILE),
            serde_json::to_vec(&serde_json::json!({
                "event_frontier": 41,
                "event_frontier_digest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "session": {
                    "execution": {"state": "idle"},
                    "last_activity_at_ms": 1234,
                    "session_title": null,
                    "configuration": {}
                },
                "transcript": [],
                "queued_prompts": [{
                    "command_id": "restored-command",
                    "content": [{"type": "text", "text": "continue offline"}],
                    "queued_at_ms": 1234
                }]
            }))
            .unwrap(),
        )
        .unwrap();

        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        assert_eq!(relay.latest_ordinal(), 42);
        assert_eq!(relay.acknowledged_through(), 41);
        assert_eq!(
            relay.claim_pending_commands(true).unwrap()[0].command_id,
            "restored-command"
        );
        let ordinal = relay
            .record_observation(RelayObservation::Warning {
                message: "restored".into(),
            })
            .unwrap();
        assert_eq!(ordinal, 43);

        drop(relay);
        let relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        assert_eq!(
            relay
                .events_after(
                    41,
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                )
                .unwrap()[0]
                .ordinal,
            42
        );
    }

    #[test]
    fn restored_relay_rebuilds_a_queued_configuration_change() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(
            temp.path()
                .join(RESTORED_CANONICAL_SESSION_FILE),
            serde_json::to_vec(&serde_json::json!({
                "event_frontier": 41,
                "event_frontier_digest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "session": {
                    "execution": {"state": "idle"},
                    "last_activity_at_ms": 1234,
                    "session_title": null,
                    "configuration": {}
                },
                "transcript": [],
                "queued_prompts": [
                    {
                        "command_id": "restored-config",
                        "kind": {"set_config": {"key": "model", "value": "sonnet"}},
                        "content": [{"type": "text", "text": "/model sonnet"}],
                        "queued_at_ms": 1234
                    },
                    {
                        "command_id": "restored-prompt",
                        "content": [{"type": "text", "text": "continue offline"}],
                        "queued_at_ms": 1235
                    }
                ]
            }))
            .unwrap(),
        )
        .unwrap();

        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        let claimed = relay.claim_pending_commands(true).unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].command_id, "restored-config");
        assert_eq!(
            claimed[0].command,
            set_config("model", "sonnet"),
            "a restored configuration change must not become a prompt"
        );

        relay
            .record_command_completed("restored-config", RelayCommandOutcome::Configured)
            .unwrap();
        assert_eq!(
            relay.claim_pending_commands(true).unwrap()[0].command_id,
            "restored-prompt"
        );
    }

    #[test]
    fn restored_relay_rejects_an_invalid_canonical_frontier() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(
            temp.path().join(RESTORED_CANONICAL_SESSION_FILE),
            br#"{"event_frontier":"forty-one"}"#,
        )
        .unwrap();
        let error = DurableRelay::open(temp.path(), SESSION, "1.0.0")
            .err()
            .expect("invalid frontier should fail");
        assert!(error.to_string().contains("canonical-session.json"));
        assert!(!temp.path().join(RELAY_STATE_FILE).exists());
    }
}
