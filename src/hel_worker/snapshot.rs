//! The deterministic relay state machine: commands, events, observations,
//! and the snapshot they fold into. `apply_relay_event` is the single place
//! that turns one more event into the next snapshot; everything else here is
//! either a type that shape describes, or the byte-budget/truncation and
//! digest machinery that keeps events and snapshots bounded and verifiable.
//! Nothing in this module touches the filesystem.

use std::collections::BTreeMap;

use agent_client_protocol::schema::ProtocolVersion as AcpProtocolVersion;
use agent_client_protocol::schema::v1::{
    AgentCapabilities, AvailableCommand, ContentBlock, Implementation, SessionConfigOption,
    SessionModeState, SessionUpdate,
};
use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};

use crate::hel_elicitation::ElicitationRequest;
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::{
    RELAY_EVENT_DIGEST_DOMAIN, RELAY_EVENT_GENESIS_DIGEST, RELAY_STATE_VERSION,
    RELAY_TRUNCATION_FLOOR,
};

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
    pub(crate) fn is_queue_entry(&self) -> bool {
        matches!(self, Self::Prompt { .. } | Self::SetConfig { .. })
    }

    pub(crate) fn is_relay_local(&self) -> bool {
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

    pub(crate) fn is_effectful_acp(&self) -> bool {
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
pub(crate) struct StoredQueuedRelayCommand {
    pub(crate) command_id: String,
    #[serde(flatten)]
    pub(crate) payload: StoredQueuedRelayPayload,
    pub(crate) created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum StoredQueuedRelayPayload {
    Prompt { prompt: Vec<ContentBlock> },
    SetConfig { key: String, value: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredActiveRelayPrompt {
    pub(crate) command_id: String,
    pub(crate) prompt: Vec<ContentBlock>,
    pub(crate) created_at_ms: i64,
    pub(crate) started_at_ms: i64,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modes: Option<SessionModeState>,
    pub available_commands: Vec<AvailableCommand>,
    pub config: BTreeMap<String, String>,
    pub active_prompt: Option<ActiveRelayPrompt>,
    pub queued_prompts: Vec<QueuedRelayPrompt>,
    pub checkpoint_barrier: Option<String>,
    pub checkpoint_ready: Option<RelayCursor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_acp_activity_at_ms: Option<i64>,
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
    SessionModesConfigured {
        modes: Option<SessionModeState>,
    },
    SessionUpdate {
        update: Box<SessionUpdate>,
    },
    PermissionAutoApproved {
        option_id: String,
        option_name: String,
    },
    ElicitationRequested {
        request: ElicitationRequest,
    },
    ElicitationResolved {
        elicitation_id: String,
        action: String,
    },
    ElicitationsCleared,
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
    /// What a client-run terminal produced, journaled once when its child was
    /// reaped. The agent already read the full output over `terminal/output`;
    /// this copy is tail-capped for the person reading the transcript.
    TerminalOutput {
        terminal_id: String,
        output: String,
        truncated: bool,
        exit_code: Option<u32>,
        signal: Option<String>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hidden_prompt_context: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PendingPromptContext {
    pub(crate) text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) attached_command_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RelayDispatchState {
    Queued,
    Pending,
    InFlight,
    Completed,
    Rejected,
    Interrupted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct RelayDispatchRecord {
    pub(crate) command: RelayCommand,
    pub(crate) state: RelayDispatchState,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct HandledRelayCommand {
    pub(crate) command: RelayCommand,
    pub(crate) accepted_ordinal: u64,
    pub(crate) terminal_ordinal: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RelaySnapshot {
    pub(crate) format_version: u32,
    pub(crate) session_id: String,
    pub(crate) execution: RelayExecutionState,
    pub(crate) latest_ordinal: u64,
    pub(crate) latest_digest: String,
    pub(crate) acknowledged_through: u64,
    pub(crate) acknowledged_digest: String,
    pub(crate) recovery_floor_ordinal: u64,
    pub(crate) recovery_floor_digest: String,
    pub(crate) native_session_id: Option<String>,
    pub(crate) agent_capabilities: Option<Box<AgentCapabilities>>,
    pub(crate) agent_info: Option<Implementation>,
    pub(crate) config_options: Vec<SessionConfigOption>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) modes: Option<SessionModeState>,
    pub(crate) available_commands: Vec<AvailableCommand>,
    pub(crate) config: BTreeMap<String, String>,
    pub(crate) active_prompt: Option<StoredActiveRelayPrompt>,
    pub(crate) queued_prompts: Vec<StoredQueuedRelayCommand>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) pending_prompt_context: Option<PendingPromptContext>,
    pub(crate) checkpoint_barrier: Option<String>,
    pub(crate) checkpoint_ready_through: Option<u64>,
    pub(crate) checkpoint_ready_digest: Option<String>,
    pub(crate) handled_commands: BTreeMap<String, HandledRelayCommand>,
    pub(crate) dispatches: BTreeMap<String, RelayDispatchRecord>,
}

impl RelaySnapshot {
    pub(crate) fn new(session_id: String) -> Self {
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
            modes: None,
            available_commands: Vec::new(),
            config: BTreeMap::new(),
            active_prompt: None,
            queued_prompts: Vec::new(),
            pending_prompt_context: None,
            checkpoint_barrier: None,
            checkpoint_ready_through: None,
            checkpoint_ready_digest: None,
            handled_commands: BTreeMap::new(),
            dispatches: BTreeMap::new(),
        }
    }

    pub(crate) fn operational_state(&self) -> RelayOperationalState {
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
            modes: self.modes.clone(),
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
            last_acp_activity_at_ms: None,
        }
    }

    pub(crate) fn retained_through(&self) -> u64 {
        self.acknowledged_through.min(self.recovery_floor_ordinal)
    }

    pub(crate) fn retained_digest(&self) -> &str {
        if self.acknowledged_through <= self.recovery_floor_ordinal {
            &self.acknowledged_digest
        } else {
            &self.recovery_floor_digest
        }
    }
}

pub(crate) fn ensure_serialized_budget(
    value: &impl Serialize,
    budget: usize,
    description: &str,
) -> Result<()> {
    let size = serde_json::to_vec(value)
        .with_context(|| format!("serialize {description} for size validation"))?
        .len();
    ensure_byte_budget(size, budget, description)
}

pub(crate) fn ensure_byte_budget(size: usize, budget: usize, description: &str) -> Result<()> {
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

/// Keep at most the last `keep` bytes of `text` and describe what was dropped.
/// The kept part starts on a character boundary, so the result stays valid
/// UTF-8. Returns whether anything was dropped.
///
/// This is the mirror of [`truncate_with_marker`] for output whose end is the
/// interesting part, such as a terminal's tail.
///
/// The Unix worker is the only production caller; the helper stays compiled
/// on Windows so its unit test still builds under `cargo test --no-run`.
#[cfg_attr(not(unix), allow(dead_code))]
pub(crate) fn truncate_start_with_marker(text: &mut String, keep: usize) -> bool {
    if text.len() <= keep {
        return false;
    }
    let mut start = text.len() - keep;
    while start < text.len() && !text.is_char_boundary(start) {
        start += 1;
    }
    let dropped = start;
    text.drain(..start);
    text.insert_str(0, &format!("[hel dropped {dropped} earlier bytes]\n"));
    true
}

/// Fit an observation inside `budget` serialized bytes by shortening its
/// largest text payloads.
///
/// The ACP peer decides what the agent said; the relay only decides how much
/// of it one durable event can carry. So an oversized payload is recorded in
/// truncated form rather than rejected — refusing it would strand a live
/// session over a transport limit it cannot see or control.
pub(crate) fn clamp_observation(
    observation: RelayObservation,
    budget: usize,
) -> Result<RelayObservation> {
    let mut size = serde_json::to_vec(&observation)
        .context("measure relay observation")?
        .len();
    if size <= budget {
        return Ok(observation);
    }
    // Only an observation that really has to shrink pays for the JSON tree
    // the truncation pass walks.
    let mut value =
        serde_json::to_value(&observation).context("serialize relay observation for clamping")?;
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

pub(crate) fn validate_relay_digest(digest: &str, name: &str) -> Result<()> {
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("{name} must be 64 lowercase hexadecimal characters");
    }
    Ok(())
}

/// Whether applying this observation moves durable relay state beyond the
/// event frontier.
///
/// Transcript observations do not: replaying them from the journal reaches the
/// same snapshot, so appending one need not stage a snapshot copy, re-check the
/// snapshot budgets, or rewrite `relay-state.json`. Every arm here mirrors an
/// arm of [`apply_relay_event`]; `transcript_observations_move_nothing_but_the_frontier`
/// fails if the two ever disagree.
pub(crate) fn observation_changes_state(observation: &RelayObservation) -> bool {
    match observation {
        RelayObservation::AgentInitialized { .. }
        | RelayObservation::SessionOpened { .. }
        | RelayObservation::SessionConfigured { .. }
        | RelayObservation::SessionModesConfigured { .. }
        | RelayObservation::CommandQueued { .. }
        | RelayObservation::CommandStarted { .. }
        | RelayObservation::CommandCompleted { .. }
        | RelayObservation::CommandRejected { .. }
        | RelayObservation::CommandInterrupted { .. }
        | RelayObservation::ConfigurationUpdated { .. }
        | RelayObservation::CheckpointReady { .. }
        | RelayObservation::Closing
        | RelayObservation::Closed => true,
        RelayObservation::SessionUpdate { update } => matches!(
            update.as_ref(),
            SessionUpdate::AvailableCommandsUpdate(_)
                | SessionUpdate::ConfigOptionUpdate(_)
                | SessionUpdate::CurrentModeUpdate(_)
        ),
        RelayObservation::PermissionAutoApproved { .. }
        | RelayObservation::ElicitationRequested { .. }
        | RelayObservation::ElicitationResolved { .. }
        | RelayObservation::ElicitationsCleared
        | RelayObservation::Warning { .. }
        | RelayObservation::TerminalOutput { .. }
        | RelayObservation::Notice { .. } => false,
    }
}

pub(crate) fn apply_relay_event(snapshot: &mut RelaySnapshot, event: &RelayEvent) -> Result<()> {
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
        RelayObservation::SessionModesConfigured { modes } => {
            snapshot.modes = modes.clone();
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
                    if snapshot
                        .pending_prompt_context
                        .as_ref()
                        .and_then(|context| context.attached_command_id.as_deref())
                        == Some(command_id.as_str())
                    {
                        snapshot.pending_prompt_context = None;
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
            if snapshot
                .pending_prompt_context
                .as_ref()
                .and_then(|context| context.attached_command_id.as_deref())
                == Some(command_id.as_str())
            {
                snapshot
                    .pending_prompt_context
                    .as_mut()
                    .expect("pending prompt context disappeared")
                    .attached_command_id = None;
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
                if let Some(modes) = snapshot.modes.as_mut() {
                    modes.current_mode_id = update.current_mode_id.clone();
                }
                snapshot
                    .config
                    .insert("mode".to_owned(), update.current_mode_id.to_string());
            }
            _ => {}
        },
        RelayObservation::PermissionAutoApproved { .. }
        | RelayObservation::ElicitationRequested { .. }
        | RelayObservation::ElicitationResolved { .. }
        | RelayObservation::ElicitationsCleared
        | RelayObservation::Warning { .. }
        | RelayObservation::TerminalOutput { .. }
        | RelayObservation::Notice { .. } => {}
    }
    snapshot.latest_ordinal = event.ordinal;
    snapshot.latest_digest = event.digest.clone();
    Ok(())
}

/// Whether finishing this relay-local command can let journal GC drop history.
/// Only a recovery-floor move does; releasing a barrier deliberately leaves the
/// floor where an installed archive left it.
pub(crate) fn releases_history(command: &RelayCommand) -> bool {
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

pub(crate) fn validate_relay_snapshot_frontiers(snapshot: &RelaySnapshot) -> Result<()> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hel_worker::test_support::*;
    use crate::hel_worker::{
        DurableRelay, RELAY_COMMAND_BYTE_BUDGET, RELAY_EVENT_BYTE_BUDGET, RELAY_STATE_BYTE_BUDGET,
        RelayErrorCode, RelayProtocolError, RelayRequest, RelayResponseBody,
    };

    #[test]
    fn relay_operational_state_tracks_mutable_acp_options_and_commands() {
        use agent_client_protocol::schema::v1::{
            AvailableCommandsUpdate, ConfigOptionUpdate, CurrentModeUpdate,
            SessionConfigSelectOption, SessionMode, SessionModeState,
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
            .record_observation(RelayObservation::SessionModesConfigured {
                modes: Some(SessionModeState::new(
                    "default",
                    vec![
                        SessionMode::new("default", "Default"),
                        SessionMode::new("plan", "Plan"),
                    ],
                )),
            })
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
        assert_eq!(
            state.modes.unwrap().current_mode_id.to_string(),
            "plan",
            "current_mode_update keeps the legacy catalogue synchronized"
        );
        assert_eq!(state.available_commands[0].name, "review");
    }

    #[test]
    fn snapshots_without_legacy_modes_still_deserialize() {
        let snapshot = RelaySnapshot::new(SESSION.into());
        let mut encoded = serde_json::to_value(snapshot).unwrap();
        encoded.as_object_mut().unwrap().remove("modes");

        let restored: RelaySnapshot = serde_json::from_value(encoded).unwrap();

        assert_eq!(restored.modes, None);
    }

    /// Transcript observations skip the staged snapshot copy and its budget
    /// checks, which is only sound while applying one really moves nothing but
    /// the frontier. Anything that can grow the snapshot must classify as a
    /// state move so its budget is still checked before it is journaled.
    #[test]
    fn transcript_observations_move_nothing_but_the_frontier() {
        use agent_client_protocol::schema::v1::{
            AvailableCommandsUpdate, ContentBlock, ContentChunk,
        };

        let transcript = [
            RelayObservation::Warning {
                message: "warned".into(),
            },
            RelayObservation::Notice {
                message: "noticed".into(),
            },
            RelayObservation::TerminalOutput {
                terminal_id: "terminal-1".into(),
                output: "output".into(),
                truncated: false,
                exit_code: Some(0),
                signal: None,
            },
            RelayObservation::PermissionAutoApproved {
                option_id: "allow".into(),
                option_name: "Allow".into(),
            },
            RelayObservation::ElicitationRequested {
                request: crate::hel_elicitation::ElicitationRequest {
                    id: "elicitation-1".into(),
                    message: "confirm".into(),
                    title: None,
                    description: None,
                    fields: Vec::new(),
                },
            },
            RelayObservation::ElicitationResolved {
                elicitation_id: "elicitation-1".into(),
                action: "accept".into(),
            },
            RelayObservation::ElicitationsCleared,
            RelayObservation::SessionUpdate {
                update: Box::new(SessionUpdate::AgentMessageChunk(ContentChunk::new(
                    ContentBlock::from("streamed"),
                ))),
            },
        ];
        for observation in transcript {
            assert!(
                !observation_changes_state(&observation),
                "{observation:?} is classified as a state move"
            );
            let mut snapshot = RelaySnapshot::new(SESSION.to_owned());
            let event = RelayEvent {
                ordinal: 1,
                previous_digest: RELAY_EVENT_GENESIS_DIGEST.to_owned(),
                digest: String::new(),
                recorded_at_ms: 7,
                command_id: None,
                observation,
            };
            let event = RelayEvent {
                digest: relay_event_digest(&event).unwrap(),
                ..event
            };
            let mut expected = snapshot.clone();
            expected.latest_ordinal = event.ordinal;
            expected.latest_digest.clone_from(&event.digest);
            apply_relay_event(&mut snapshot, &event).unwrap();
            assert_eq!(
                snapshot, expected,
                "{:?} changed durable state",
                event.observation
            );
        }

        for observation in [
            RelayObservation::CommandQueued {
                command_id: "queued-command".into(),
                command: prompt("grow the snapshot"),
                created_at_ms: 7,
            },
            RelayObservation::SessionUpdate {
                update: Box::new(SessionUpdate::AvailableCommandsUpdate(
                    AvailableCommandsUpdate::new(vec![AvailableCommand::new(
                        "review",
                        "Review the current work",
                    )]),
                )),
            },
        ] {
            assert!(
                observation_changes_state(&observation),
                "{observation:?} can grow the snapshot and must be budget-checked"
            );
        }
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
    fn truncate_start_keeps_the_tail_and_discloses_the_drop() {
        let mut short = "abcdefghij".to_owned();
        assert!(!truncate_start_with_marker(&mut short, 100));
        assert_eq!(short, "abcdefghij");

        let mut long = "abcdefghij".to_owned();
        assert!(truncate_start_with_marker(&mut long, 4));
        assert!(
            long.starts_with("[hel dropped "),
            "the drop must be disclosed: {long:?}"
        );
        assert!(long.ends_with("ghij"), "the tail must be kept: {long:?}");
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

        let replayed = relay
            .events_after(0, crate::hel_worker::RELAY_EVENT_GENESIS_DIGEST)
            .unwrap();
        let recorded = &replayed[0];
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
}
