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

pub use protocol::{
    RelayErrorCode, RelayErrorDetail, RelayProtocolError, RelayRequest, RelayRequestEnvelope,
    RelayResponseBody, RelayResponseEnvelope, RelayResponsePayload, RelayVersionRange,
    invalid_relay_request_response, read_relay_frame, serve_relay_json_lines,
    unsupported_relay_method_response, write_relay_frame,
};
pub use snapshot::{
    ActiveRelayPrompt, ClaimedRelayCommand, QueuedRelayPrompt, RelayCommand, RelayCommandKind,
    RelayCommandOutcome, RelayCursor, RelayEvent, RelayExecutionState, RelayObservation,
    RelayOperationalState, relay_event_digest, validate_relay_event,
};
pub use types::{
    ActivePrompt, Attachment, QueuedPrompt, RESTORED_CANONICAL_SESSION_FILE, SequencedEvent,
    WorkerEvent, WorkerPhase, WorkerSessionSummary, WorkerSnapshot,
};

use std::collections::VecDeque;
use std::fs::{self, File};
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};

use agent_client_protocol::schema::v1::{ContentBlock, SessionUpdate};
use anyhow::{Context, Result, anyhow, bail};

use crate::clock::epoch_millis;
use crate::hel_archive::CanonicalQueuedCommandKind;
use journal::{
    RelayJournalSpan, open_relay_journal, persist_relay_snapshot, read_restored_relay_seed,
    visit_relay_journal_file,
};
use protocol::{relay_error, relay_protocol_error};
use snapshot::{
    HandledRelayCommand, RelayDispatchRecord, RelayDispatchState, RelaySnapshot,
    StoredQueuedRelayCommand, StoredQueuedRelayPayload, ensure_byte_budget,
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

struct RelayEventPage {
    events: Vec<RelayEvent>,
    through_ordinal: u64,
    through_digest: String,
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
    use super::*;
    use test_support::*;

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
