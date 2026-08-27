//! Persistent, transport-neutral protocol core for a Hel target worker.
//!
//! The worker never listens on a network port. Controllers speak newline-
//! delimited JSON through `hel worker proxy`, which can itself be carried over
//! SSH or a container exec stream.
//!
//! This module is the root of the relay implementation and keeps the
//! `DurableRelay` request-handling and scheduling core: opening/recovering a
//! session, handling requests, and the command-claim scheduler. The wire
//! protocol lives in [`protocol`], the deterministic event/snapshot state
//! machine lives in [`snapshot`], durable journal I/O lives in [`journal`],
//! and worker-facing types unrelated to the relay live in [`types`]. Every
//! external path this module exposed before the split is re-exported here so
//! callers outside this module never need to change.

mod journal;
mod protocol;
mod snapshot;
mod types;

pub use journal::{RestoredRelaySeed, restored_relay_seed_path};
pub use protocol::{
    RelayErrorCode, RelayErrorDetail, RelayProtocolError, RelayRequest, RelayRequestEnvelope,
    RelayResponseBody, RelayResponseEnvelope, RelayResponsePayload, RelayVersionRange,
    incompatible_request_protocol_response, invalid_relay_request_response, read_relay_frame,
    serve_relay_json_lines, unsupported_relay_method_response, write_relay_frame,
};
#[cfg(unix)]
pub(crate) use snapshot::truncate_start_with_marker;
pub use snapshot::{
    ActiveAgentTerminal, ActiveRelayPrompt, ActiveUserShell, ClaimedRelayCommand,
    QueuedRelayPrompt, RelayCommand, RelayCommandKind, RelayCommandOutcome, RelayCursor,
    RelayEvent, RelayExecutionState, RelayObservation, RelayOperationalState, UserShellResult,
    UserShellStatus, relay_event_digest, validate_relay_event,
};
pub use types::{
    ActivePrompt, Attachment, QueuedPrompt, SequencedEvent, WorkerEvent, WorkerPhase,
    WorkerSessionSummary, WorkerSnapshot,
};

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
#[cfg(unix)]
use std::fs::File;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use agent_client_protocol::schema::v1::{ContentBlock, SessionUpdate};
use anyhow::{Context, Result, anyhow, bail};

use crate::clock::epoch_millis;
use crate::hel_archive::CanonicalQueuedCommandKind;
use journal::{
    RelayJournalSpan, open_relay_journal, read_restored_relay_seed, visit_relay_journal_file,
};
use protocol::{incompatible_request_protocol, relay_error, relay_protocol_error};
use snapshot::{
    HandledRelayCommand, PendingPromptContext, RelayDispatchRecord, RelayDispatchState,
    RelaySnapshot, StoredQueuedRelayCommand, StoredQueuedRelayPayload, ensure_byte_budget,
    ensure_serialized_budget, releases_history, validate_relay_digest,
    validate_relay_snapshot_frontiers,
};

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
/// Bytes of terminal output one journal entry keeps. The agent read the whole
/// stream over `terminal/output`; the journal copy is for the person reading
/// the transcript, so it keeps the tail and stays far below the event budget.
pub const TERMINAL_JOURNAL_OUTPUT_BYTES: usize = 256 * 1024;
/// Headroom left for an event's envelope — ordinals, digests, timestamp and
/// command id — when clamping an observation to `RELAY_EVENT_BYTE_BUDGET`.
const RELAY_EVENT_ENVELOPE_RESERVE: usize = 8 * 1024;
/// Clamping never shortens a string below this. Identifiers, type tags and
/// paths stay whole; only genuinely large payloads are candidates.
const RELAY_TRUNCATION_FLOOR: usize = 4 * 1024;
/// The private snapshot also has a hard ceiling so repeated accepted commands
/// cannot grow the durable state file without bound between checkpoints.
const RELAY_SNAPSHOT_BYTE_BUDGET: usize = 16 * 1024 * 1024;
/// Current durable ACP relay protocol. Peers that only speak an older
/// version in [`RELAY_MIN_PROTOCOL_VERSION`]..=this range still connect.
/// Protocol 0 is the retired pre-relay worker protocol and is rejected.
pub const RELAY_PROTOCOL_VERSION: u32 = 5;
pub const RELAY_MIN_PROTOCOL_VERSION: u32 = 1;
/// Digest for the empty relay event prefix (ordinal zero).
pub const RELAY_EVENT_GENESIS_DIGEST: &str = crate::hel_archive::EVENT_FRONTIER_GENESIS_DIGEST;
const RELAY_EVENT_DIGEST_DOMAIN: &[u8] = b"hel-relay-event-v1\0";
const RELAY_STATE_VERSION: u32 = 1;
/// The relay snapshot inside a worker root. Teardown and restore name it from
/// here rather than repeating the literal.
pub const RELAY_STATE_FILE: &str = "relay-state.json";
/// The relay's durable event journal inside a worker root.
pub const RELAY_JOURNAL_DIR: &str = "relay-journal";
/// The seed a checkpoint restore leaves in a worker root for the relay that
/// opens next. It carries only what a fresh relay cannot derive on its own.
pub const RESTORED_RELAY_SEED_FILE: &str = "relay-seed.json";
/// File in which a running worker daemon records its own PID, inside its
/// worker root. Session teardown reads it to stop that daemon before the root
/// it writes to is removed.
pub const WORKER_PID_FILE: &str = "worker.pid";
const RELAY_ACTIVE_SEGMENT: &str = "active.jsonl";
const RELAY_SEGMENT_BYTE_LIMIT: u64 = 1024 * 1024;
/// Journal bytes a restart may have to replay before the snapshot is rewritten.
/// Transcript observations are reconstructed by that replay, so they are
/// journaled without their own snapshot write until this much has accumulated.
const RELAY_SNAPSHOT_LAG_BYTE_LIMIT: usize = 1024 * 1024;
const RELAY_HOT_EVENT_CAPACITY: usize = 32;
const RELAY_REPLAY_CURSOR_CAPACITY: usize = 32;
const NATIVE_SESSION_IDENTITY_FILE: &str = "native-session.json";

/// Remove controller-only context that an ACP harness copied into a user-facing
/// prompt or title. Hidden context is prepended as reserved XML-like blocks;
/// an unterminated reserved block is treated as a truncated hidden value, not
/// as text safe to display.
pub fn strip_hidden_prompt_context(mut text: &str) -> &str {
    loop {
        text = text.trim_start();
        let Some(after_open) = text.strip_prefix('<') else {
            return text;
        };
        let Some(open_end) = after_open.find('>') else {
            return if reserved_hidden_context_prefix(after_open) {
                ""
            } else {
                text
            };
        };
        let tag = &after_open[..open_end];
        if !reserved_hidden_context_tag(tag) {
            return text;
        }
        let close = format!("</{tag}>");
        let after_open = &after_open[open_end + 1..];
        let Some(close_start) = after_open.rfind(&close) else {
            return "";
        };
        text = &after_open[close_start + close.len()..];
    }
}

fn reserved_hidden_context_prefix(text: &str) -> bool {
    text.starts_with("hel-") || text.starts_with("user_shell_command")
}

fn reserved_hidden_context_tag(tag: &str) -> bool {
    (tag.starts_with("hel-")
        && tag
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'))
        || tag == "user_shell_command"
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
    /// Digests proven while serving older replay pages. Controllers normally
    /// return the previous page's frontier, so retaining those sparse cursors
    /// avoids decompressing that segment again merely to validate the next
    /// request. Event bodies remain on disk and authoritative.
    replay_cursors: VecDeque<(u64, String)>,
    /// Journal bytes appended since `relay-state.json` last matched this
    /// snapshot. Zero means the durable file is exactly this state.
    unpersisted_journal_bytes: usize,
    /// Bumped whenever a captured [`RelayReplayPlan`] stops describing the
    /// journal on disk: sealing moves the active segment's events into a new
    /// file, and garbage collection rewrites and prunes segments. Plain
    /// appends never bump it — they only extend the active segment, which a
    /// lock-free reader already tolerates.
    journal_generation: u64,
    acp_activity: AcpActivityClock,
    /// ACP terminals are connection-owned and disappear when that connection
    /// is torn down, so they belong in memory rather than the durable relay
    /// snapshot or transcript journal.
    active_agent_terminals: BTreeMap<String, ActiveAgentTerminal>,
    /// A short-lived guard against a fast child reporting close before the
    /// create handler publishes its started event on the shared channel.
    closed_agent_terminals: BTreeSet<String>,
    /// Benchmark aid: stage and persist a snapshot on every append, the way
    /// the relay did before transcript appends were amortized, so both
    /// policies can be timed in one process.
    #[cfg(test)]
    stage_snapshot_every_append: bool,
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
            replay_cursors: VecDeque::new(),
            unpersisted_journal_bytes: 0,
            journal_generation: 0,
            acp_activity: AcpActivityClock::default(),
            active_agent_terminals: BTreeMap::new(),
            closed_agent_terminals: BTreeSet::new(),
            #[cfg(test)]
            stage_snapshot_every_append: false,
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
                    created_at_ms: epoch_millis(),
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
        let mut state = self.snapshot.operational_state();
        state.last_acp_activity_at_ms = self.acp_activity.last_at_ms();
        state.active_agent_terminals = self.active_agent_terminals.values().cloned().collect();
        state
    }

    pub fn agent_terminal_started(&mut self, terminal: ActiveAgentTerminal) {
        if self.closed_agent_terminals.remove(&terminal.terminal_id) {
            return;
        }
        self.active_agent_terminals
            .insert(terminal.terminal_id.clone(), terminal);
    }

    pub fn agent_terminal_closed(&mut self, terminal_id: &str) {
        self.active_agent_terminals.remove(terminal_id);
        self.closed_agent_terminals.insert(terminal_id.to_owned());
    }

    pub fn clear_agent_terminals(&mut self) {
        self.active_agent_terminals.clear();
        self.closed_agent_terminals.clear();
    }

    pub fn acp_activity_clock(&self) -> AcpActivityClock {
        self.acp_activity.clone()
    }

    /// The directory holding this relay's durable state.
    pub fn root(&self) -> &Path {
        &self.root
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
        let plan = self.replay_plan();
        plan.validate_cursor(after_ordinal, after_digest)?;
        plan.read_events_after(after_ordinal, after_digest, usize::MAX)
            .map(|page| page.events)
    }

    /// How many times the journal's files stopped matching a captured
    /// [`RelayReplayPlan`]. A lock-free reader compares this before and after
    /// its read to tell a stale plan from a real desynchronization.
    pub fn journal_generation(&self) -> u64 {
        self.journal_generation
    }

    /// Capture everything a replay page needs from this relay, so the file
    /// reads and gzip decompression behind it can run without the relay lock.
    fn replay_plan(&self) -> RelayReplayPlan {
        RelayReplayPlan {
            spans: self.journal_spans.clone(),
            hot_digests: self
                .hot_events
                .iter()
                .map(|event| (event.ordinal, event.digest.clone()))
                .chain(self.replay_cursors.iter().cloned())
                .collect(),
            latest_ordinal: self.snapshot.latest_ordinal,
            latest_digest: self.snapshot.latest_digest.clone(),
            acknowledged_through: self.snapshot.acknowledged_through,
            acknowledged_digest: self.snapshot.acknowledged_digest.clone(),
            recovery_floor_ordinal: self.snapshot.recovery_floor_ordinal,
            recovery_floor_digest: self.snapshot.recovery_floor_digest.clone(),
            retained_through: self.snapshot.retained_through(),
            retained_digest: self.snapshot.retained_digest().to_owned(),
            generation: self.journal_generation,
        }
    }

    /// Retain a cursor this worker just proved while reading a replay page.
    /// This is an optimization only: losing the cache causes another journal
    /// validation scan, never a loss of durable history.
    pub fn remember_replay_cursor(&mut self, response: &RelayResponseEnvelope) {
        let RelayResponseBody::Ok {
            payload:
                RelayResponsePayload::Attached {
                    through_ordinal,
                    through_digest,
                    ..
                },
        } = &response.body
        else {
            return;
        };
        if self
            .replay_cursors
            .back()
            .is_some_and(|(ordinal, digest)| ordinal == through_ordinal && digest == through_digest)
        {
            return;
        }
        self.replay_cursors
            .retain(|(ordinal, _)| ordinal != through_ordinal);
        if self.replay_cursors.len() == RELAY_REPLAY_CURSOR_CAPACITY {
            self.replay_cursors.pop_front();
        }
        self.replay_cursors
            .push_back((*through_ordinal, through_digest.clone()));
    }

    /// Split an attach into the cheap part that needs the relay lock and the
    /// expensive part that does not.
    ///
    /// Catch-up over a long offline history reads page after page from disk
    /// and decompresses sealed segments. Doing that under the relay lock
    /// blocks live event recording until it finishes, which is exactly what a
    /// controller attaching is not supposed to cost the session. `None` means
    /// this envelope is not an attach, or is one that cannot be served at all;
    /// the caller falls back to [`Self::handle`], which answers it.
    pub fn take_deferred_attach(
        &self,
        envelope: &RelayRequestEnvelope,
    ) -> Option<DeferredRelayAttach> {
        let RelayRequest::Attach {
            after_ordinal,
            after_digest,
        } = &envelope.request
        else {
            return None;
        };
        if self.envelope_rejection(envelope).is_some() {
            return None;
        }
        let state = self.operational_state();
        Some(DeferredRelayAttach {
            request_id: envelope.request_id.clone(),
            protocol_version: envelope.protocol_version,
            plan: self.replay_plan(),
            state,
            after_ordinal: *after_ordinal,
            after_digest: after_digest.clone(),
        })
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

    /// Everything answered before a request reaches relay state: a usable
    /// request ID, protocol negotiation, and a method this peer's protocol
    /// version admits. `Some` is the response to send instead of handling.
    /// [`Self::take_deferred_attach`] consults the same checks so an attach it
    /// defers is one the normal path would have accepted.
    fn envelope_rejection(&self, envelope: &RelayRequestEnvelope) -> Option<RelayResponseBody> {
        if envelope.request_id.trim().is_empty() || envelope.request_id.len() > 256 {
            return Some(relay_error(
                RelayErrorCode::InvalidRequest,
                "request_id is required",
                false,
                None,
            ));
        }
        if let RelayRequest::Hello { supported, .. } = &envelope.request {
            let Some(negotiated) = RelayVersionRange::CURRENT.negotiate(*supported) else {
                return Some(relay_error(
                    RelayErrorCode::IncompatibleProtocol,
                    format!(
                        "controller supports {}-{}, relay supports protocol {}-{}",
                        supported.min,
                        supported.max,
                        RELAY_MIN_PROTOCOL_VERSION,
                        RELAY_PROTOCOL_VERSION
                    ),
                    false,
                    None,
                ));
            };
            return Some(RelayResponseBody::Ok {
                payload: RelayResponsePayload::Hello {
                    negotiated,
                    relay_version: self.relay_version.clone(),
                    session_id: self.snapshot.session_id.clone(),
                },
            });
        }
        if !envelope.request.supported_at(envelope.protocol_version) {
            return Some(incompatible_request_protocol(envelope.protocol_version));
        }
        None
    }

    fn handle_inner(&mut self, envelope: &RelayRequestEnvelope) -> Result<RelayResponseBody> {
        if let Some(body) = self.envelope_rejection(envelope) {
            return Ok(body);
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
            RelayRequest::InstallPromptContext { text } => {
                self.install_prompt_context(text.clone())?;
                RelayResponsePayload::PromptContextInstalled
            }
            RelayRequest::CredentialState
            | RelayRequest::ReadCredentials
            | RelayRequest::InstallCredentials { .. }
            | RelayRequest::SkillsState
            | RelayRequest::InstallSkills { .. }
            | RelayRequest::GithubTokenState
            | RelayRequest::InstallGithubToken { .. }
            | RelayRequest::RemoveGithubToken
            | RelayRequest::ProjectMemorySnapshot
            | RelayRequest::InstallProjectMemorySnapshot { .. } => {
                return Ok(relay_error(
                    RelayErrorCode::InvalidState,
                    "connection-only requests must be handled by the live relay transport",
                    false,
                    None,
                ));
            }
            RelayRequest::Compact { .. } => {
                return Ok(relay_error(
                    RelayErrorCode::InvalidState,
                    "compaction requests must be handled by the live relay transport",
                    false,
                    None,
                ));
            }
            RelayRequest::RespondElicitation { .. } => {
                return Ok(relay_error(
                    RelayErrorCode::InvalidState,
                    "elicitation responses must be handled by the live relay transport",
                    false,
                    None,
                ));
            }
        };
        Ok(RelayResponseBody::Ok { payload })
    }

    pub(crate) fn install_prompt_context(&mut self, text: String) -> Result<()> {
        if text.trim().is_empty() {
            bail!("pending prompt context is empty");
        }
        if self.snapshot.active_prompt.is_some()
            || self
                .snapshot
                .pending_prompt_context
                .as_ref()
                .is_some_and(|context| context.attached_command_id.is_some())
        {
            bail!("cannot replace prompt context while its prompt is active");
        }
        let mut next = self.snapshot.clone();
        match next.pending_prompt_context.as_mut() {
            Some(context) if context.text != text => {
                context.text.push_str("\n\n");
                context.text.push_str(&text);
            }
            Some(_) => {}
            None => {
                next.pending_prompt_context = Some(PendingPromptContext {
                    text,
                    attached_command_id: None,
                });
            }
        }
        ensure_serialized_budget(
            &next,
            RELAY_SNAPSHOT_BYTE_BUDGET,
            "relay snapshot with pending prompt context",
        )?;
        self.commit_snapshot(next)
    }

    fn attach(
        &mut self,
        after_ordinal: u64,
        after_digest: &str,
    ) -> Result<std::result::Result<RelayResponsePayload, RelayProtocolError>> {
        let state = self.operational_state();
        self.replay_plan()
            .attach(after_ordinal, after_digest, state)
    }

    fn acknowledge(
        &mut self,
        through_ordinal: u64,
        through_digest: &str,
    ) -> Result<std::result::Result<RelayResponsePayload, RelayProtocolError>> {
        let plan = self.replay_plan();
        if let Err(error) = plan.validate_cursor(through_ordinal, through_digest) {
            return Ok(Err(relay_protocol_error(
                RelayErrorCode::Desynchronized,
                error.to_string(),
                false,
                Some(plan.desynchronized_detail(through_ordinal, through_digest)),
            )));
        }
        if through_ordinal > self.snapshot.acknowledged_through {
            let mut next_snapshot = self.snapshot.clone();
            next_snapshot.acknowledged_through = through_ordinal;
            next_snapshot.acknowledged_digest = through_digest.to_owned();
            // The acknowledgement becomes durable before any journal GC.
            self.commit_snapshot(next_snapshot)?;
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
        if let RelayCommand::RunUserShell { command } = &command
            && command.trim().is_empty()
        {
            return Ok(Err(relay_protocol_error(
                RelayErrorCode::InvalidRequest,
                "shell command is empty",
                false,
                None,
            )));
        }
        if let RelayCommand::CancelUserShell { shell_command_id } = &command
            && !self
                .snapshot
                .active_user_shells
                .contains_key(shell_command_id)
        {
            return Ok(Err(relay_protocol_error(
                RelayErrorCode::InvalidState,
                "there is no active shell command with that ID",
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

        let created_at_ms = epoch_millis();
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
                    started_at_ms: epoch_millis(),
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

    /// Claim no more work than the caller already holds transport permits for.
    /// The dispatcher reserves one ACP command permit per claimed command
    /// before calling, so every claim can be handed over without waiting even
    /// when other senders share that channel. Bounding the durable in-flight
    /// batch that way is what keeps command backpressure from parking the
    /// coordinator that must keep draining ACP's bounded event channel.
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
                            started_at_ms: epoch_millis(),
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
            let hidden_prompt_context = matches!(dispatch.command, RelayCommand::Prompt { .. })
                .then(|| {
                    let mut contexts = Vec::new();
                    if let Some(context) = next_snapshot.pending_prompt_context.as_mut() {
                        if context.attached_command_id.is_none() {
                            context.attached_command_id = Some(command_id.clone());
                        }
                        if context.attached_command_id.as_deref() == Some(command_id.as_str()) {
                            contexts.push(context.text.clone());
                        }
                    }
                    for context in &mut next_snapshot.pending_user_shell_contexts {
                        if context.accepted_ordinal >= accepted_ordinal {
                            continue;
                        }
                        if context.attached_command_id.is_none() {
                            context.attached_command_id = Some(command_id.clone());
                        }
                        if context.attached_command_id.as_deref() == Some(command_id.as_str()) {
                            contexts.push(context.text.clone());
                        }
                    }
                    (!contexts.is_empty()).then(|| contexts.join("\n\n"))
                })
                .flatten();
            claimed.push(ClaimedRelayCommand {
                command_id,
                accepted_ordinal,
                command: dispatch.command.clone(),
                hidden_prompt_context,
            });
        }
        if !claimed.is_empty() {
            // An in-flight claim is not in the journal, so it is only durable
            // once the snapshot itself is.
            self.commit_snapshot(next_snapshot)?;
        }
        Ok(claimed)
    }

    /// Claim user shell work independently of ACP turns. Run commands honor
    /// the caller's concurrency limit; cancellation controls bypass it so a
    /// full shell pool can always be stopped.
    pub fn claim_pending_user_shell_commands_up_to(
        &mut self,
        maximum_runs: usize,
    ) -> Result<Vec<ClaimedRelayCommand>> {
        if self.snapshot.checkpoint_barrier.is_some() {
            return Ok(Vec::new());
        }
        let barrier_ordinal = self
            .next_queued_checkpoint()
            .map_or(u64::MAX, |(_, ordinal)| ordinal);
        let mut cancels = Vec::new();
        let cancelled_shells: std::collections::BTreeSet<String> = self
            .snapshot
            .dispatches
            .values()
            .filter(|dispatch| dispatch.state == RelayDispatchState::Queued)
            .filter_map(|dispatch| match &dispatch.command {
                RelayCommand::CancelUserShell { shell_command_id } => {
                    Some(shell_command_id.clone())
                }
                _ => None,
            })
            .collect();
        let mut runs = Vec::new();
        for (command_id, dispatch) in &self.snapshot.dispatches {
            if dispatch.state != RelayDispatchState::Queued {
                continue;
            }
            let Some(handled) = self.snapshot.handled_commands.get(command_id) else {
                continue;
            };
            match dispatch.command {
                RelayCommand::CancelUserShell { .. } => {
                    cancels.push((handled.accepted_ordinal, command_id.clone()));
                }
                RelayCommand::RunUserShell { .. }
                    if handled.accepted_ordinal < barrier_ordinal
                        && !cancelled_shells.contains(command_id) =>
                {
                    runs.push((handled.accepted_ordinal, command_id.clone()));
                }
                _ => {}
            }
        }
        cancels.sort();
        runs.sort();
        runs.truncate(maximum_runs);
        let mut selected = cancels;
        selected.extend(runs);
        selected.sort();
        for (_, command_id) in &selected {
            self.append_relay_event(
                Some(command_id),
                RelayObservation::CommandStarted {
                    command_id: command_id.clone(),
                    started_at_ms: epoch_millis(),
                },
            )?;
        }
        let mut next_snapshot = self.snapshot.clone();
        let mut claimed = Vec::with_capacity(selected.len());
        for (accepted_ordinal, command_id) in selected {
            let dispatch = next_snapshot
                .dispatches
                .get_mut(&command_id)
                .expect("claimed shell command disappeared");
            dispatch.state = RelayDispatchState::InFlight;
            claimed.push(ClaimedRelayCommand {
                command_id,
                accepted_ordinal,
                command: dispatch.command.clone(),
                hidden_prompt_context: None,
            });
        }
        if !claimed.is_empty() {
            self.commit_snapshot(next_snapshot)?;
        }
        Ok(claimed)
    }

    fn start_queued_controls(&mut self, command_ids: Vec<String>) -> Result<()> {
        for command_id in command_ids {
            self.append_relay_event(
                Some(&command_id),
                RelayObservation::CommandStarted {
                    command_id: command_id.clone(),
                    started_at_ms: epoch_millis(),
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
                (dispatch.command.is_effectful_acp() || dispatch.command.is_effectful_user_shell())
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

    fn digest_at(&self, ordinal: u64) -> Result<Option<String>> {
        self.replay_plan().digest_at(ordinal)
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
        let queued_ordinal = self
            .snapshot
            .handled_commands
            .get(&queued.command_id)
            .map_or(u64::MAX, |handled| handled.accepted_ordinal);
        if self.snapshot.active_user_shells.keys().any(|command_id| {
            self.snapshot
                .handled_commands
                .get(command_id)
                .is_some_and(|handled| handled.accepted_ordinal < queued_ordinal)
        }) {
            return Ok(None);
        }
        let ordinal = self.append_relay_event(
            Some(&queued.command_id),
            RelayObservation::CommandStarted {
                command_id: queued.command_id.clone(),
                started_at_ms: epoch_millis(),
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
}

/// Process-local clock for inbound ACP traffic. It deliberately stays out of
/// the durable relay journal: this is render timing, not recoverable session
/// history.
#[derive(Debug, Clone, Default)]
pub struct AcpActivityClock(Arc<AtomicI64>);

impl AcpActivityClock {
    pub fn mark(&self) {
        self.0.store(epoch_millis(), Ordering::Release);
    }

    pub fn last_at_ms(&self) -> Option<i64> {
        let value = self.0.load(Ordering::Acquire);
        (value > 0).then_some(value)
    }
}

struct RelayEventPage {
    events: Vec<RelayEvent>,
    through_ordinal: u64,
    through_digest: String,
}

/// A point-in-time view of the durable journal: everything needed to validate
/// a replay cursor and assemble a replay page, and nothing that requires the
/// relay lock to read.
///
/// Sealed segments are immutable and the active segment is append-only, so a
/// captured span list stays readable while the relay keeps recording events.
/// Sealing and garbage collection do invalidate it, so every read here is
/// written to fail loudly rather than return a short or torn page, and
/// `generation` lets the caller recognize that failure as a stale plan instead
/// of a real desynchronization.
pub struct RelayReplayPlan {
    spans: Vec<RelayJournalSpan>,
    /// Digests of recent live events and proven replay-page cursors, so the
    /// next sequential attachment validates without touching old segments.
    hot_digests: Vec<(u64, String)>,
    latest_ordinal: u64,
    latest_digest: String,
    acknowledged_through: u64,
    acknowledged_digest: String,
    recovery_floor_ordinal: u64,
    recovery_floor_digest: String,
    retained_through: u64,
    retained_digest: String,
    generation: u64,
}

impl RelayReplayPlan {
    fn attach(
        &self,
        after_ordinal: u64,
        after_digest: &str,
        state: RelayOperationalState,
    ) -> Result<std::result::Result<RelayResponsePayload, RelayProtocolError>> {
        if let Err(error) = self.validate_cursor(after_ordinal, after_digest) {
            return Ok(Err(relay_protocol_error(
                RelayErrorCode::Desynchronized,
                error.to_string(),
                false,
                Some(self.desynchronized_detail(after_ordinal, after_digest)),
            )));
        }
        let page = self.read_events_after(after_ordinal, after_digest, RELAY_REPLAY_BYTE_BUDGET)?;
        ensure_serialized_budget(&state, RELAY_STATE_BYTE_BUDGET, "relay operational state")?;
        Ok(Ok(RelayResponsePayload::Attached {
            state,
            events: page.events,
            through_ordinal: page.through_ordinal,
            through_digest: page.through_digest,
        }))
    }

    fn validate_cursor(&self, after_ordinal: u64, after_digest: &str) -> Result<()> {
        if after_ordinal < self.retained_through {
            bail!(
                "event {after_ordinal} is no longer available; relay retained events after {}",
                self.retained_through
            );
        }
        if after_ordinal > self.latest_ordinal {
            bail!(
                "event {after_ordinal} is newer than relay frontier {}",
                self.latest_ordinal
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
        if ordinal == self.latest_ordinal {
            return Ok(Some(self.latest_digest.clone()));
        }
        if ordinal == self.acknowledged_through {
            return Ok(Some(self.acknowledged_digest.clone()));
        }
        if ordinal == self.recovery_floor_ordinal {
            return Ok(Some(self.recovery_floor_digest.clone()));
        }
        if let Some((_, digest)) = self.hot_digests.iter().find(|(hot, _)| *hot == ordinal) {
            return Ok(Some(digest.clone()));
        }
        if let Some(span) = self.spans.iter().find(|span| {
            span.file_first_ordinal.checked_sub(1) == Some(ordinal) && ordinal >= span.after_ordinal
        }) {
            if let Some(digest) = &span.file_first_previous_digest {
                return Ok(Some(digest.clone()));
            }
            let mut digest = None;
            visit_relay_journal_file(&span.path, false, |event, _| {
                let previous_ordinal = event
                    .ordinal
                    .checked_sub(1)
                    .ok_or_else(|| anyhow!("relay event ordinal zero is invalid"))?;
                validate_relay_event(previous_ordinal, &event.previous_digest, &event)
                    .with_context(|| format!("validate relay journal {}", span.path.display()))?;
                digest = Some(event.previous_digest);
                Ok(ControlFlow::Break(()))
            })?;
            return Ok(digest);
        }
        let Some(span) = self
            .spans
            .iter()
            .find(|span| ordinal > span.after_ordinal && ordinal <= span.file_last_ordinal)
        else {
            return Ok(None);
        };
        let mut digest = None;
        let mut previous: Option<RelayEvent> = None;
        visit_relay_journal_file(&span.path, false, |event, _| {
            if let Some(previous) = &previous {
                validate_relay_event(previous.ordinal, &previous.digest, &event)
                    .with_context(|| format!("validate relay journal {}", span.path.display()))?;
            } else {
                let previous_ordinal = event
                    .ordinal
                    .checked_sub(1)
                    .ok_or_else(|| anyhow!("relay event ordinal zero is invalid"))?;
                validate_relay_event(previous_ordinal, &event.previous_digest, &event)
                    .with_context(|| format!("validate relay journal {}", span.path.display()))?;
            }
            if event.ordinal == ordinal {
                digest = Some(event.digest.clone());
                return Ok(ControlFlow::Break(()));
            }
            previous = Some(event);
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(digest)
    }

    fn read_events_after(
        &self,
        after_ordinal: u64,
        after_digest: &str,
        byte_budget: usize,
    ) -> Result<RelayEventPage> {
        let mut events = Vec::new();
        let mut used = 0_usize;
        let mut through_ordinal = after_ordinal;
        // `attach` validated this exact cursor immediately before entering the
        // reader. Reusing it avoids a second decompression pass over the
        // cursor's sealed segment.
        let mut through_digest = after_digest.to_owned();
        let mut page_full = false;

        for span in &self.spans {
            if page_full || span.file_last_ordinal <= through_ordinal {
                continue;
            }
            visit_relay_journal_file(&span.path, false, |event, encoded_len| {
                if event.ordinal <= span.after_ordinal || event.ordinal <= through_ordinal {
                    return Ok(ControlFlow::Continue(()));
                }
                if event.ordinal > self.latest_ordinal {
                    // The active segment kept growing after this plan was
                    // captured. Those events are real, but the reply's
                    // operational state describes the frontier the plan saw,
                    // so the page stops there and the caller asks again.
                    return Ok(ControlFlow::Break(()));
                }
                let expected = through_ordinal
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("relay event ordinal exhausted"))?;
                if event.ordinal != expected {
                    bail!(
                        "relay journal page has a gap after event {through_ordinal}: found {}",
                        event.ordinal
                    );
                }
                // The page is assembled off the relay lock, so it carries its
                // own proof that it is one unbroken run of the chain the cursor
                // named rather than fragments of a journal that moved.
                if event.previous_digest != through_digest {
                    bail!(
                        "relay journal event {} does not chain from event {through_ordinal}",
                        event.ordinal
                    );
                }
                validate_relay_event(through_ordinal, &through_digest, &event)
                    .context("validate relay journal page event")?;
                if !events.is_empty() && used.saturating_add(encoded_len) > byte_budget {
                    page_full = true;
                    return Ok(ControlFlow::Break(()));
                }
                used = used.saturating_add(encoded_len);
                through_ordinal = event.ordinal;
                through_digest.clone_from(&event.digest);
                events.push(event);
                Ok(ControlFlow::Continue(()))
            })?;
            // A canonical span contributes every ordinal through its last one.
            // Stopping short means the file no longer holds what this plan
            // captured: it was sealed, rewritten, or pruned under the reader.
            if !page_full && through_ordinal < span.file_last_ordinal.min(self.latest_ordinal) {
                bail!(
                    "relay journal {} no longer covers event {}",
                    span.path.display(),
                    span.file_last_ordinal
                );
            }
        }
        if !page_full
            && (through_ordinal != self.latest_ordinal || through_digest != self.latest_digest)
        {
            // The spans end at the frontier this plan captured. Anything else
            // is a page assembled from a journal that moved, never a short
            // answer a caller could mistake for a complete one.
            bail!(
                "relay journal ended at event {through_ordinal}, expected frontier {}",
                self.latest_ordinal
            );
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
            earliest_available: self.retained_through,
            earliest_digest: self.retained_digest.clone(),
            latest: self.latest_ordinal,
            latest_digest: self.latest_digest.clone(),
        }
    }
}

/// An attach whose disk work has been lifted out of the relay lock.
///
/// [`DurableRelay::take_deferred_attach`] builds one while holding the lock;
/// [`Self::finish`] then does the reading, decompressing and page assembly
/// with the lock released, so live event recording keeps running underneath a
/// controller's catch-up.
pub struct DeferredRelayAttach {
    request_id: String,
    protocol_version: u32,
    plan: RelayReplayPlan,
    state: RelayOperationalState,
    after_ordinal: u64,
    after_digest: String,
}

impl DeferredRelayAttach {
    /// The journal generation this attach was planned against. Compare it with
    /// [`DurableRelay::journal_generation`] after [`Self::finish`] fails: an
    /// unchanged generation means the failure is real, and a changed one means
    /// the journal was resealed or collected mid-read and the controller
    /// should simply attach again.
    pub fn journal_generation(&self) -> u64 {
        self.plan.generation
    }

    /// Blocking: reads journal segments and decompresses sealed ones. Callers
    /// on an async runtime must run this off the event loop.
    pub fn finish(self) -> RelayResponseEnvelope {
        let body = match self
            .plan
            .attach(self.after_ordinal, &self.after_digest, self.state)
        {
            Ok(Ok(payload)) => RelayResponseBody::Ok { payload },
            Ok(Err(error)) => RelayResponseBody::Error { error },
            Err(error) => RelayResponseBody::Error {
                error: RelayProtocolError {
                    code: RelayErrorCode::Internal,
                    message: format!("{error:#}"),
                    retryable: true,
                    detail: None,
                },
            },
        };
        RelayResponseEnvelope {
            request_id: self.request_id,
            protocol_version: self.protocol_version,
            body,
        }
    }

    /// The answer for an attach whose plan went stale while it was reading:
    /// nothing is wrong with the controller's cursor, so it retries against
    /// the journal as it now stands.
    pub fn stale_journal_response(
        request_id: String,
        protocol_version: u32,
    ) -> RelayResponseEnvelope {
        RelayResponseEnvelope {
            request_id,
            protocol_version,
            body: RelayResponseBody::Error {
                error: RelayProtocolError {
                    code: RelayErrorCode::Internal,
                    message: "relay journal was resealed or collected while the replay page was \
                              being read; attach again"
                        .to_owned(),
                    retryable: true,
                    detail: None,
                },
            },
        }
    }
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

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    pub(crate) const SESSION: &str = "018f9dd2-a3b4-7c8d-9000-123456789abc";

    pub(crate) fn relay_request(request_id: &str, request: RelayRequest) -> RelayRequestEnvelope {
        RelayRequestEnvelope {
            request_id: request_id.to_owned(),
            protocol_version: RELAY_PROTOCOL_VERSION,
            request,
        }
    }

    pub(crate) fn submit_relay(
        relay: &mut DurableRelay,
        command_id: &str,
        command: RelayCommand,
    ) -> u64 {
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

    pub(crate) fn attach_relay(
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

    pub(crate) fn acknowledge_relay(
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

    pub(crate) fn prompt(text: &str) -> RelayCommand {
        RelayCommand::Prompt {
            prompt: vec![ContentBlock::from(text)],
        }
    }

    pub(crate) fn set_config(key: &str, value: &str) -> RelayCommand {
        RelayCommand::SetConfig {
            key: key.to_owned(),
            value: value.to_owned(),
        }
    }

    pub(crate) fn queued_command_ids(relay: &DurableRelay) -> Vec<String> {
        relay
            .operational_state()
            .queued_prompts
            .into_iter()
            .map(|queued| queued.command_id)
            .collect()
    }

    pub(crate) fn finish_prompt(relay: &mut DurableRelay, command_id: &str) {
        relay
            .record_command_completed(
                command_id,
                RelayCommandOutcome::Prompt {
                    stop_reason: "end_turn".into(),
                },
            )
            .unwrap();
    }

    pub(crate) fn retained_events(relay: &DurableRelay) -> Vec<RelayEvent> {
        relay
            .events_after(
                relay.snapshot.retained_through(),
                relay.snapshot.retained_digest(),
            )
            .unwrap()
    }

    pub(crate) fn submit_release(
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

    pub(crate) fn submit_floor(
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

    pub(crate) fn ready_checkpoint(relay: &mut DurableRelay, command_id: &str) -> RelayCursor {
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
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use test_support::*;

    #[test]
    fn hidden_prompt_context_is_removed_from_harness_visible_text() {
        let text = concat!(
            "<hel-project-memory>private memory</hel-project-memory>\n\n",
            "<user_shell_command>private output</user_shell_command>\n",
            "ship the visible change"
        );

        assert_eq!(strip_hidden_prompt_context(text), "ship the visible change");
        assert_eq!(
            strip_hidden_prompt_context("<hel-project-memory>truncated"),
            ""
        );
        assert_eq!(
            strip_hidden_prompt_context("<user-request>keep me</user-request>"),
            "<user-request>keep me</user-request>"
        );
    }

    #[test]
    fn acp_activity_clock_is_shared_with_operational_status_but_not_persisted() {
        let temp = tempfile::tempdir().unwrap();
        let relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        assert_eq!(relay.operational_state().last_acp_activity_at_ms, None);
        relay.acp_activity_clock().mark();
        assert!(relay.operational_state().last_acp_activity_at_ms.is_some());

        let reopened = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        assert_eq!(reopened.operational_state().last_acp_activity_at_ms, None);
    }

    #[test]
    fn hidden_context_waits_for_a_prompt_and_survives_an_interruption() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        let installed = relay.handle(relay_request(
            "install-context",
            RelayRequest::InstallPromptContext {
                text: "<hel-background>memory</hel-background>".into(),
            },
        ));
        assert!(matches!(
            installed.body,
            RelayResponseBody::Ok {
                payload: RelayResponsePayload::PromptContextInstalled
            }
        ));

        submit_relay(
            &mut relay,
            "configure-first",
            RelayCommand::SetConfig {
                key: "model".into(),
                value: "default".into(),
            },
        );
        let config = relay.claim_pending_commands(true).unwrap();
        assert_eq!(config.len(), 1);
        assert_eq!(config[0].hidden_prompt_context, None);
        relay
            .record_command_completed("configure-first", RelayCommandOutcome::Configured)
            .unwrap();

        submit_relay(&mut relay, "first-prompt", prompt("do it"));
        let first = relay.claim_pending_commands(true).unwrap();
        assert_eq!(
            first[0].hidden_prompt_context.as_deref(),
            Some("<hel-background>memory</hel-background>")
        );
        assert_eq!(first[0].command, prompt("do it"));
        relay
            .record_command_interrupted("first-prompt", "restart")
            .unwrap();

        submit_relay(&mut relay, "second-prompt", prompt("continue"));
        let second = relay.claim_pending_commands(true).unwrap();
        assert_eq!(
            second[0].hidden_prompt_context.as_deref(),
            Some("<hel-background>memory</hel-background>")
        );
        relay
            .record_command_completed(
                "second-prompt",
                RelayCommandOutcome::Prompt {
                    stop_reason: "end_turn".into(),
                },
            )
            .unwrap();

        submit_relay(&mut relay, "third-prompt", prompt("again"));
        let third = relay.claim_pending_commands(true).unwrap();
        assert_eq!(third[0].hidden_prompt_context, None);
    }

    /// A history large enough to seal several segments and to overflow one
    /// replay page, so an attach against it really does read and decompress.
    fn record_paged_history(relay: &mut DurableRelay, events: usize) {
        for index in 0..events {
            relay
                .record_observation(RelayObservation::Warning {
                    message: format!("{index:04}:{}", "x".repeat(64 * 1024)),
                })
                .unwrap();
        }
    }

    fn attach_envelope(
        relay: &DurableRelay,
        request_id: &str,
        after_ordinal: u64,
    ) -> RelayRequestEnvelope {
        relay_request(
            request_id,
            RelayRequest::Attach {
                after_ordinal,
                after_digest: relay.digest_at(after_ordinal).unwrap().unwrap(),
            },
        )
    }

    #[test]
    fn a_deferred_attach_reads_its_page_while_the_relay_keeps_recording() {
        let temp = tempfile::tempdir().unwrap();
        let relay = Arc::new(Mutex::new(
            DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap(),
        ));
        record_paged_history(&mut relay.lock().unwrap(), 80);
        let planned_frontier = relay.lock().unwrap().latest_ordinal();

        let deferred = {
            let guard = relay.lock().unwrap();
            guard
                .take_deferred_attach(&attach_envelope(&guard, "catch-up", 0))
                .expect("an attach is deferred off the relay lock")
        };

        // The catch-up page has not been assembled yet, and the relay is free.
        for index in 0..8 {
            relay
                .lock()
                .expect("the relay lock is not held by the pending replay")
                .record_observation(RelayObservation::Warning {
                    message: format!("live-{index}"),
                })
                .unwrap();
        }
        let live_frontier = relay.lock().unwrap().latest_ordinal();
        assert_eq!(live_frontier, planned_frontier + 8);

        let response = deferred.finish();
        let RelayResponseBody::Ok {
            payload:
                RelayResponsePayload::Attached {
                    events,
                    through_ordinal,
                    through_digest,
                    state,
                },
        } = response.body
        else {
            panic!("deferred attach failed");
        };
        assert!(
            !events.is_empty() && through_ordinal < planned_frontier,
            "the history should not fit in one page: {through_ordinal} of {planned_frontier}"
        );
        // The page is one unbroken run of the chain the cursor asked for, and
        // it reports the frontier captured with the plan rather than the one
        // the live appends moved it to.
        let mut cursor = RelayCursor {
            ordinal: 0,
            digest: RELAY_EVENT_GENESIS_DIGEST.to_owned(),
        };
        for event in &events {
            validate_relay_event(cursor.ordinal, &cursor.digest, event).unwrap();
            cursor.ordinal = event.ordinal;
            cursor.digest.clone_from(&event.digest);
        }
        assert_eq!(cursor.ordinal, through_ordinal);
        assert_eq!(cursor.digest, through_digest);
        assert_eq!(state.latest_ordinal, planned_frontier);
    }

    #[test]
    fn a_proven_replay_cursor_does_not_reread_its_old_segment() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        record_paged_history(&mut relay, 80);
        let first_request = attach_envelope(&relay, "first", 0);
        let first = relay.handle(first_request);
        let RelayResponseBody::Ok {
            payload:
                RelayResponsePayload::Attached {
                    through_ordinal,
                    through_digest,
                    ..
                },
        } = &first.body
        else {
            panic!("first replay page failed: {:?}", first.body);
        };
        assert!(*through_ordinal < relay.latest_ordinal());
        relay.remember_replay_cursor(&first);

        let old_segment = relay
            .journal_spans
            .iter()
            .find(|span| {
                *through_ordinal > span.after_ordinal && *through_ordinal <= span.file_last_ordinal
            })
            .expect("the replay cursor belongs to a journal segment")
            .path
            .clone();
        std::fs::rename(&old_segment, old_segment.with_extension("moved")).unwrap();

        assert_eq!(
            relay.digest_at(*through_ordinal).unwrap().as_deref(),
            Some(through_digest.as_str()),
            "validating the returned cursor must not reopen its old segment"
        );
    }

    #[test]
    fn a_deferred_attach_refuses_a_page_from_a_collected_journal() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        record_paged_history(&mut relay, 80);
        let sealed = |root: &Path| {
            fs::read_dir(root.join(RELAY_JOURNAL_DIR))
                .unwrap()
                .filter_map(std::result::Result::ok)
                .filter(|entry| entry.path().extension().is_some_and(|name| name == "gz"))
                .count()
        };
        assert!(sealed(temp.path()) >= 2, "history did not seal segments");

        let deferred = relay
            .take_deferred_attach(&attach_envelope(&relay, "catch-up", 0))
            .expect("an attach is deferred off the relay lock");
        let generation = deferred.journal_generation();

        // Another controller acknowledges the whole history, which rewrites
        // the journal and deletes every sealed segment this plan named.
        let frontier = relay.latest_ordinal();
        let frontier_digest = relay.latest_digest().to_owned();
        submit_floor(
            &mut relay,
            "floor-command-0001",
            RelayCursor {
                ordinal: frontier,
                digest: frontier_digest,
            },
        );
        acknowledge_relay(&mut relay, "ack-everything", frontier);
        assert_eq!(sealed(temp.path()), 0, "collection kept sealed segments");

        let response = deferred.finish();
        let RelayResponseBody::Error { error } = &response.body else {
            panic!("a page read from a collected journal must not be served: {response:?}");
        };
        assert!(
            error.retryable,
            "a collected journal is retryable, not a controller fault: {error:?}"
        );
        assert_ne!(
            relay.journal_generation(),
            generation,
            "collection must mark captured replay plans stale"
        );
    }

    /// A replay page is assembled from files the relay lock no longer guards,
    /// so a span whose file was pruned under the reader must fail the read.
    /// Silently contributing nothing would answer a catch-up with a page that
    /// claims to reach the frontier while carrying none of the events.
    #[test]
    fn a_deferred_attach_never_serves_a_torn_page_after_its_segments_are_pruned() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        record_paged_history(&mut relay, 80);
        let frontier = relay.latest_ordinal();

        let deferred = relay
            .take_deferred_attach(&attach_envelope(&relay, "catch-up", 0))
            .expect("an attach is deferred off the relay lock");
        for entry in fs::read_dir(temp.path().join(RELAY_JOURNAL_DIR)).unwrap() {
            fs::remove_file(entry.unwrap().path()).unwrap();
        }

        let response = deferred.finish();
        match response.body {
            RelayResponseBody::Error { error } => assert!(
                error.retryable,
                "a pruned segment is retryable, not a controller fault: {error:?}"
            ),
            RelayResponseBody::Ok {
                payload:
                    RelayResponsePayload::Attached {
                        events,
                        through_ordinal,
                        ..
                    },
            } => panic!(
                "served {} events but claimed to reach event {through_ordinal} of {frontier}",
                events.len()
            ),
            other => panic!("unexpected attach response: {other:?}"),
        }
    }

    #[test]
    fn a_deferred_attach_refuses_a_page_from_a_resealed_segment() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        // Fill the active segment without crossing its seal threshold.
        record_paged_history(&mut relay, 8);
        let deferred = relay
            .take_deferred_attach(&attach_envelope(&relay, "catch-up", 0))
            .expect("an attach is deferred off the relay lock");
        let generation = deferred.journal_generation();

        // The next append seals the active segment, moving every event the
        // plan expects to find in `active.jsonl` into a compressed file.
        record_paged_history(&mut relay, 12);
        assert_ne!(relay.journal_generation(), generation);

        let response = deferred.finish();
        let RelayResponseBody::Error { error } = &response.body else {
            panic!("a page read from a resealed segment must not be served: {response:?}");
        };
        assert!(
            error.retryable,
            "a resealed segment is retryable: {error:?}"
        );
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

    fn successful_shell(command: &str, stdout: &str) -> UserShellResult {
        UserShellResult {
            command: command.to_owned(),
            stdout: stdout.to_owned(),
            stderr: String::new(),
            stdout_truncated: false,
            stderr_truncated: false,
            exit_code: Some(0),
            signal: None,
            duration_ms: 12,
            status: UserShellStatus::Exited,
            error: None,
        }
    }

    #[test]
    fn shell_runs_during_an_active_turn_and_barriers_the_later_prompt() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        submit_relay(&mut relay, "prompt-command-1", prompt("first"));
        assert_eq!(
            relay.claim_pending_commands(true).unwrap()[0].command_id,
            "prompt-command-1"
        );
        submit_relay(
            &mut relay,
            "shell-command-01",
            RelayCommand::RunUserShell {
                command: "printf ready".into(),
            },
        );
        submit_relay(&mut relay, "prompt-command-2", prompt("after shell"));

        let shell = relay.claim_pending_user_shell_commands_up_to(4).unwrap();
        assert_eq!(shell[0].command_id, "shell-command-01");
        relay
            .record_command_completed(
                "prompt-command-1",
                RelayCommandOutcome::Prompt {
                    stop_reason: "end_turn".into(),
                },
            )
            .unwrap();
        assert!(relay.claim_pending_commands(true).unwrap().is_empty());

        relay
            .record_command_completed(
                "shell-command-01",
                RelayCommandOutcome::UserShell {
                    result: successful_shell("printf ready", "ready"),
                },
            )
            .unwrap();
        let prompt = relay.claim_pending_commands(true).unwrap();
        assert_eq!(prompt[0].command_id, "prompt-command-2");
        assert!(
            prompt[0]
                .hidden_prompt_context
                .as_deref()
                .is_some_and(
                    |context| context.contains("<user_shell_command>") && context.contains("ready")
                )
        );
    }

    #[test]
    fn a_prompt_accepted_before_a_shell_keeps_priority_and_does_not_consume_it() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        submit_relay(&mut relay, "prompt-command-1", prompt("first"));
        relay.claim_pending_commands(true).unwrap();
        submit_relay(&mut relay, "prompt-command-2", prompt("already queued"));
        submit_relay(
            &mut relay,
            "shell-command-01",
            RelayCommand::RunUserShell {
                command: "printf later".into(),
            },
        );
        relay.claim_pending_user_shell_commands_up_to(4).unwrap();
        relay
            .record_command_completed(
                "prompt-command-1",
                RelayCommandOutcome::Prompt {
                    stop_reason: "end_turn".into(),
                },
            )
            .unwrap();

        let prompt = relay.claim_pending_commands(true).unwrap();
        assert_eq!(prompt[0].command_id, "prompt-command-2");
        assert!(prompt[0].hidden_prompt_context.is_none());
    }

    #[test]
    fn a_shell_cancelled_before_launch_still_reaches_the_next_prompt() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        submit_relay(
            &mut relay,
            "shell-command-01",
            RelayCommand::RunUserShell {
                command: "sleep 60".into(),
            },
        );
        submit_relay(
            &mut relay,
            "cancel-shell-01",
            RelayCommand::CancelUserShell {
                shell_command_id: "shell-command-01".into(),
            },
        );

        let claimed = relay.claim_pending_user_shell_commands_up_to(4).unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].command_id, "cancel-shell-01");
        relay
            .record_command_interrupted(
                "shell-command-01",
                "shell command was cancelled before it started",
            )
            .unwrap();
        relay
            .record_command_completed("cancel-shell-01", RelayCommandOutcome::UserShellCancelled)
            .unwrap();

        submit_relay(&mut relay, "prompt-command-1", prompt("what happened?"));
        let prompt = relay.claim_pending_commands(true).unwrap();
        assert!(
            prompt[0]
                .hidden_prompt_context
                .as_deref()
                .is_some_and(|context| {
                    context.contains("status: interrupted")
                        && context.contains("cancelled before it started")
                })
        );
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
        let text = "The working tree moved while this session was stopped.";
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
    fn an_in_flight_prompt_blocks_checkpoint_barrier_admission() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION, "1.0.0").unwrap();
        submit_relay(&mut relay, "stuck-prompt", prompt("keep running"));
        let prompt = relay.claim_pending_commands(true).unwrap();
        assert_eq!(prompt.len(), 1);
        assert_eq!(prompt[0].command_id, "stuck-prompt");

        submit_relay(
            &mut relay,
            "barrier-command",
            RelayCommand::BeginCheckpoint { reason: None },
        );
        assert!(
            relay.claim_pending_commands(true).unwrap().is_empty(),
            "a live ACP turn must keep the checkpoint barrier queued"
        );

        relay
            .record_command_interrupted("stuck-prompt", "worker restarted")
            .unwrap();
        let claimed = relay.claim_pending_commands(true).unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].command_id, "barrier-command");
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
}
