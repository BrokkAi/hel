//! Persistent, transport-neutral protocol core for a Hel target worker.
//!
//! The worker never listens on a network port. Controllers speak newline-
//! delimited JSON through `hel worker proxy`, which can itself be carried over
//! SSH or a container exec stream.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const PROTOCOL_VERSION: u32 = 2;
pub const MIN_PROTOCOL_VERSION: u32 = 1;
pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
/// Serialized bytes of events allowed in one replay response, well under
/// `MAX_FRAME_BYTES` to leave room for the envelope.
pub const REPLAY_BYTE_BUDGET: usize = 4 * 1024 * 1024;
const NATIVE_SESSION_IDENTITY_VERSION: u32 = 1;
const NATIVE_SESSION_IDENTITY_FILE: &str = "native-session.json";

/// Trim a replay to the byte budget. Returns the events to send and the
/// sequence the client is current through: the last included event's seq, or
/// `latest_seq` when nothing had to be trimmed (also when the replay is empty).
fn page_events(
    events: Vec<SequencedEvent>,
    after_seq: u64,
    latest_seq: u64,
) -> (Vec<SequencedEvent>, u64) {
    let mut used = 0_usize;
    let mut included = Vec::new();
    for event in events {
        let size = serde_json::to_vec(&event).map_or(usize::MAX, |bytes| bytes.len());
        if !included.is_empty() && used.saturating_add(size) > REPLAY_BYTE_BUDGET {
            let through = included
                .last()
                .map_or(after_seq, |last: &SequencedEvent| last.seq);
            return (included, through);
        }
        used = used.saturating_add(size);
        included.push(event);
    }
    (included, latest_seq)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionRange {
    pub min: u32,
    pub max: u32,
}

impl VersionRange {
    pub const CURRENT: Self = Self {
        min: MIN_PROTOCOL_VERSION,
        max: PROTOCOL_VERSION,
    };

    pub fn negotiate(self, peer: Self) -> Option<u32> {
        let minimum = self.min.max(peer.min);
        let maximum = self.max.min(peer.max);
        (minimum <= maximum).then_some(maximum)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestEnvelope {
    pub request_id: String,
    pub protocol_version: u32,
    pub request: WorkerRequest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "method", content = "params", rename_all = "snake_case")]
pub enum WorkerRequest {
    Hello {
        client_version: String,
        supported: VersionRange,
    },
    Status,
    Snapshot,
    Subscribe {
        after_seq: u64,
    },
    Prompt {
        text: String,
        #[serde(default)]
        attachments: Vec<Attachment>,
    },
    /// Run a prompt in a disposable ACP session. This does not enter the
    /// destination session's canonical history.
    Compact {
        text: String,
    },
    Cancel,
    SetConfig {
        key: String,
        value: Value,
    },
    Checkpoint {
        reason: Option<String>,
    },
    CheckpointWhenQuiescent {
        reason: Option<String>,
    },
    Close,
}

impl WorkerRequest {
    pub(crate) const fn method_name(&self) -> &'static str {
        match self {
            Self::Hello { .. } => "hello",
            Self::Status => "status",
            Self::Snapshot => "snapshot",
            Self::Subscribe { .. } => "subscribe",
            Self::Prompt { .. } => "prompt",
            Self::Compact { .. } => "compact",
            Self::Cancel => "cancel",
            Self::SetConfig { .. } => "set_config",
            Self::Checkpoint { .. } => "checkpoint",
            Self::CheckpointWhenQuiescent { .. } => "checkpoint_when_quiescent",
            Self::Close => "close",
        }
    }

    const fn minimum_protocol(&self) -> u32 {
        match self {
            Self::CheckpointWhenQuiescent { .. } => 2,
            _ => 1,
        }
    }

    fn is_mutating(&self) -> bool {
        matches!(
            self,
            Self::Prompt { .. }
                | Self::Compact { .. }
                | Self::Cancel
                | Self::SetConfig { .. }
                | Self::Checkpoint { .. }
                | Self::CheckpointWhenQuiescent { .. }
                | Self::Close
        )
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseEnvelope {
    pub request_id: String,
    pub protocol_version: u32,
    #[serde(flatten)]
    pub body: ResponseBody,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum ResponseBody {
    Ok { payload: ResponsePayload },
    Error { error: ProtocolError },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum ResponsePayload {
    Hello {
        negotiated: u32,
        worker_version: String,
        session_id: String,
    },
    Status(WorkerStatus),
    Snapshot(WorkerSnapshot),
    Replay {
        events: Vec<SequencedEvent>,
        latest_seq: u64,
    },
    Accepted {
        seq: u64,
    },
    Compacted {
        text: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolError {
    pub code: ErrorCode,
    pub message: String,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<ProtocolErrorDetail>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProtocolErrorDetail {
    UnsupportedMethod { method: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    IncompatibleProtocol,
    InvalidRequest,
    InvalidState,
    SequenceOutOfRange,
    Internal,
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
pub struct WorkerStatus {
    pub session_id: String,
    pub phase: WorkerPhase,
    pub latest_seq: u64,
    pub last_checkpoint_seq: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivePrompt {
    pub request_id: String,
    pub text: String,
    pub attachments: Vec<Attachment>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerSnapshot {
    pub session_id: String,
    pub phase: WorkerPhase,
    pub latest_seq: u64,
    pub last_checkpoint_seq: Option<u64>,
    pub active_prompt: Option<ActivePrompt>,
    pub config: BTreeMap<String, Value>,
    #[serde(default)]
    handled_requests: BTreeMap<String, HandledRequest>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    dispatches: BTreeMap<String, DispatchRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct HandledRequest {
    request: WorkerRequest,
    payload: ResponsePayload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DispatchState {
    Pending,
    InFlight,
    Completed,
    Interrupted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DispatchRecord {
    request: WorkerRequest,
    state: DispatchState,
}

impl WorkerSnapshot {
    fn new(session_id: String) -> Self {
        Self {
            session_id,
            phase: WorkerPhase::Idle,
            latest_seq: 0,
            last_checkpoint_seq: None,
            active_prompt: None,
            config: BTreeMap::new(),
            handled_requests: BTreeMap::new(),
            dispatches: BTreeMap::new(),
        }
    }

    fn status(&self) -> WorkerStatus {
        WorkerStatus {
            session_id: self.session_id.clone(),
            phase: self.phase,
            latest_seq: self.latest_seq,
            last_checkpoint_seq: self.last_checkpoint_seq,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeSessionIdentity {
    version: u32,
    native_session_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum WorkerEvent {
    PromptAccepted {
        request_id: String,
        text: String,
        attachments: Vec<Attachment>,
    },
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

pub struct DurableWorker {
    root: PathBuf,
    worker_version: String,
    snapshot: WorkerSnapshot,
    events: Vec<SequencedEvent>,
    native_session_id: Option<String>,
}

impl DurableWorker {
    pub fn open(
        root: impl Into<PathBuf>,
        session_id: impl Into<String>,
        worker_version: impl Into<String>,
    ) -> Result<Self> {
        let root = root.into();
        let session_id = session_id.into();
        validate_identifier(&session_id, "session ID")?;
        fs::create_dir_all(&root)
            .with_context(|| format!("create worker state directory {}", root.display()))?;

        let snapshot_path = root.join("snapshot.json");
        let mut snapshot = if snapshot_path.exists() {
            let bytes = fs::read(&snapshot_path)
                .with_context(|| format!("read {}", snapshot_path.display()))?;
            let snapshot: WorkerSnapshot = serde_json::from_slice(&bytes)
                .with_context(|| format!("parse {}", snapshot_path.display()))?;
            if snapshot.session_id != session_id {
                bail!(
                    "worker state belongs to session {}, not {session_id}",
                    snapshot.session_id
                );
            }
            snapshot
        } else {
            WorkerSnapshot::new(session_id)
        };

        let events = load_event_log(&root.join("events.jsonl"))?;
        validate_event_sequence(&events)?;
        let logged_seq = events.last().map_or(0, |event| event.seq);
        if snapshot.latest_seq > logged_seq {
            bail!("snapshot is ahead of durable event log");
        }
        let snapshot_seq = snapshot.latest_seq;
        for event in events.iter().filter(|event| event.seq > snapshot_seq) {
            apply_event(&mut snapshot, event)?;
        }

        let native_session_id = read_native_session_identity(&root)?;
        let mut worker = Self {
            root,
            worker_version: worker_version.into(),
            snapshot,
            events,
            native_session_id,
        };
        if !snapshot_path.exists() || snapshot_seq < logged_seq {
            worker.persist_snapshot()?;
        }
        worker.recover_dispatches()?;
        Ok(worker)
    }

    pub fn snapshot(&self) -> &WorkerSnapshot {
        &self.snapshot
    }

    /// Native ACP identity persisted by the worker after session startup.
    /// This is intentionally separate from canonical history: restoring a
    /// cross-harness archive must not make the destination load the source
    /// harness's native session.
    pub fn native_session_id(&self) -> Option<&str> {
        self.native_session_id.as_deref()
    }

    /// Recover identities written by workers that predate native-session.json.
    /// Prefer the newest identity that accepted a prompt. This repairs the
    /// legacy failure mode where a daemon restart appended a newer, empty
    /// session_started event after the real session.
    pub fn recover_native_session_id_from_events(&self) -> Option<String> {
        recover_native_session_id(&self.events)
    }

    pub fn events_after(&self, after_seq: u64) -> Result<Vec<SequencedEvent>> {
        if after_seq > self.snapshot.latest_seq {
            bail!(
                "requested sequence {after_seq} is newer than {}",
                self.snapshot.latest_seq
            );
        }
        Ok(self
            .events
            .iter()
            .filter(|event| event.seq > after_seq)
            .cloned()
            .collect())
    }

    /// Record an event produced by the ACP adapter. This uses the same durable
    /// sequence as controller actions, so reconnect replay has one total order.
    pub fn record_adapter_event(&mut self, kind: impl Into<String>, payload: Value) -> Result<u64> {
        self.append_event(
            None,
            WorkerEvent::Adapter {
                kind: kind.into(),
                payload,
            },
        )
    }

    /// Durably bind this logical worker to the native ACP session that
    /// actually started. Recording the canonical event first leaves a safe
    /// migration path if the identity-file write is interrupted.
    pub fn record_native_session_started(
        &mut self,
        kind: impl Into<String>,
        payload: Value,
        native_session_id: &str,
    ) -> Result<u64> {
        validate_identifier(native_session_id, "native session ID")?;
        let seq = self.record_adapter_event(kind, payload)?;
        let identity = NativeSessionIdentity {
            version: NATIVE_SESSION_IDENTITY_VERSION,
            native_session_id: native_session_id.to_owned(),
        };
        let body = serde_json::to_vec_pretty(&identity)?;
        crate::hel_config::atomic_write(&self.root.join(NATIVE_SESSION_IDENTITY_FILE), &body)?;
        self.native_session_id = Some(native_session_id.to_owned());
        Ok(seq)
    }

    pub fn record_turn_completed(&mut self) -> Result<u64> {
        if self.snapshot.phase != WorkerPhase::Running {
            bail!(
                "cannot complete a turn while worker is {:?}",
                self.snapshot.phase
            );
        }
        let seq = self.append_event(None, WorkerEvent::TurnCompleted)?;
        self.complete_prompt_dispatches()?;
        Ok(seq)
    }

    pub fn record_closed(&mut self) -> Result<u64> {
        if self.snapshot.phase != WorkerPhase::Closing {
            bail!(
                "cannot finish close while worker is {:?}",
                self.snapshot.phase
            );
        }
        let seq = self.append_event(None, WorkerEvent::Closed)?;
        self.complete_dispatches_matching(|request| matches!(request, WorkerRequest::Close))?;
        Ok(seq)
    }

    /// Durably claim commands before handing them to the ACP runtime.  A
    /// proxy response is independent from this ledger, so losing the response
    /// cannot suppress or duplicate runtime execution.
    pub fn claim_pending_dispatches(&mut self) -> Result<Vec<(String, WorkerRequest)>> {
        let mut claimed = Vec::new();
        for (request_id, dispatch) in &mut self.snapshot.dispatches {
            if dispatch.state == DispatchState::Pending {
                dispatch.state = DispatchState::InFlight;
                claimed.push((request_id.clone(), dispatch.request.clone()));
            }
        }
        if !claimed.is_empty() {
            self.persist_snapshot()?;
        }
        Ok(claimed)
    }

    pub fn complete_dispatch(&mut self, request_id: &str) -> Result<()> {
        let Some(dispatch) = self.snapshot.dispatches.get_mut(request_id) else {
            return Ok(());
        };
        dispatch.state = DispatchState::Completed;
        self.persist_snapshot()
    }

    pub fn handle(&mut self, envelope: RequestEnvelope) -> ResponseEnvelope {
        let request_id = envelope.request_id.clone();
        let body = self
            .handle_inner(&envelope)
            .unwrap_or_else(|error| ResponseBody::Error {
                error: ProtocolError {
                    code: ErrorCode::Internal,
                    message: format!("{error:#}"),
                    retryable: true,
                    detail: None,
                },
            });
        let protocol_version = match &body {
            ResponseBody::Ok {
                payload: ResponsePayload::Hello { negotiated, .. },
            } => *negotiated,
            _ => envelope.protocol_version,
        };
        ResponseEnvelope {
            request_id,
            protocol_version,
            body,
        }
    }

    fn handle_inner(&mut self, envelope: &RequestEnvelope) -> Result<ResponseBody> {
        if envelope.request_id.trim().is_empty() || envelope.request_id.len() > 256 {
            return Ok(error(
                ErrorCode::InvalidRequest,
                "request_id is required",
                false,
            ));
        }
        if let WorkerRequest::Hello { supported, .. } = &envelope.request {
            let Some(negotiated) = VersionRange::CURRENT.negotiate(*supported) else {
                return Ok(error(
                    ErrorCode::IncompatibleProtocol,
                    format!(
                        "controller supports {}-{}, worker supports {}-{}",
                        supported.min, supported.max, MIN_PROTOCOL_VERSION, PROTOCOL_VERSION
                    ),
                    false,
                ));
            };
            return Ok(ResponseBody::Ok {
                payload: ResponsePayload::Hello {
                    negotiated,
                    worker_version: self.worker_version.clone(),
                    session_id: self.snapshot.session_id.clone(),
                },
            });
        }
        if !(MIN_PROTOCOL_VERSION..=PROTOCOL_VERSION).contains(&envelope.protocol_version) {
            return Ok(error(
                ErrorCode::IncompatibleProtocol,
                format!(
                    "request uses protocol {}, worker supports {}-{}",
                    envelope.protocol_version, MIN_PROTOCOL_VERSION, PROTOCOL_VERSION
                ),
                false,
            ));
        }
        let minimum = envelope.request.minimum_protocol();
        if envelope.protocol_version < minimum {
            return Ok(error(
                ErrorCode::IncompatibleProtocol,
                format!(
                    "{} requires worker protocol {minimum}",
                    envelope.request.method_name()
                ),
                false,
            ));
        }
        if envelope.request.is_mutating()
            && let Some(handled) = self.snapshot.handled_requests.get(&envelope.request_id)
        {
            if handled.request != envelope.request {
                return Ok(error(
                    ErrorCode::InvalidRequest,
                    "request_id was already used for a different mutation",
                    false,
                ));
            }
            return Ok(ResponseBody::Ok {
                payload: handled.payload.clone(),
            });
        }

        let payload = match &envelope.request {
            WorkerRequest::Hello { .. } => unreachable!(),
            WorkerRequest::Status => ResponsePayload::Status(self.snapshot.status()),
            WorkerRequest::Snapshot => ResponsePayload::Snapshot(self.snapshot.clone()),
            WorkerRequest::Subscribe { after_seq } => match self.events_after(*after_seq) {
                Ok(events) => {
                    // A full transcript can exceed the protocol frame cap, so
                    // replay pages by byte budget. latest_seq reports the last
                    // sequence actually included; clients page until a replay
                    // comes back empty.
                    let (events, latest_seq) =
                        page_events(events, *after_seq, self.snapshot.latest_seq);
                    ResponsePayload::Replay { events, latest_seq }
                }
                Err(error) => {
                    return Ok(super_error(
                        ErrorCode::SequenceOutOfRange,
                        error.to_string(),
                        false,
                    ));
                }
            },
            WorkerRequest::Prompt { text, attachments } => {
                if self.snapshot.phase != WorkerPhase::Idle {
                    return Ok(error(
                        ErrorCode::InvalidState,
                        format!("cannot prompt while worker is {:?}", self.snapshot.phase),
                        false,
                    ));
                }
                if text.is_empty() && attachments.is_empty() {
                    return Ok(error(ErrorCode::InvalidRequest, "prompt is empty", false));
                }
                let seq = self.append_event(
                    Some(&envelope.request_id),
                    WorkerEvent::PromptAccepted {
                        request_id: envelope.request_id.clone(),
                        text: text.clone(),
                        attachments: attachments.clone(),
                    },
                )?;
                ResponsePayload::Accepted { seq }
            }
            WorkerRequest::Compact { .. } => {
                return Ok(error(
                    ErrorCode::InvalidRequest,
                    "compact requests must be handled by the ACP runtime",
                    false,
                ));
            }
            WorkerRequest::Cancel => {
                if self.snapshot.phase != WorkerPhase::Running {
                    return Ok(error(
                        ErrorCode::InvalidState,
                        format!("cannot cancel while worker is {:?}", self.snapshot.phase),
                        false,
                    ));
                }
                let seq = self.append_event(Some(&envelope.request_id), WorkerEvent::Cancelled)?;
                ResponsePayload::Accepted { seq }
            }
            WorkerRequest::SetConfig { key, value } => {
                if let Err(validation) = validate_config_key(key) {
                    return Ok(error(
                        ErrorCode::InvalidRequest,
                        validation.to_string(),
                        false,
                    ));
                }
                if matches!(
                    self.snapshot.phase,
                    WorkerPhase::Closing | WorkerPhase::Closed
                ) {
                    return Ok(error(ErrorCode::InvalidState, "worker is closing", false));
                }
                let seq = self.append_event(
                    Some(&envelope.request_id),
                    WorkerEvent::ConfigChanged {
                        key: key.clone(),
                        value: value.clone(),
                    },
                )?;
                ResponsePayload::Accepted { seq }
            }
            WorkerRequest::Checkpoint { reason } => {
                if self.snapshot.phase == WorkerPhase::Closed {
                    return Ok(error(ErrorCode::InvalidState, "worker is closed", false));
                }
                let seq = self.append_event(
                    Some(&envelope.request_id),
                    WorkerEvent::Checkpointed {
                        reason: reason.clone(),
                        quiescent: false,
                    },
                )?;
                ResponsePayload::Accepted { seq }
            }
            WorkerRequest::CheckpointWhenQuiescent { reason } => {
                if self.snapshot.phase == WorkerPhase::Closed {
                    return Ok(error(ErrorCode::InvalidState, "worker is closed", false));
                }
                let seq = self.append_event(
                    Some(&envelope.request_id),
                    WorkerEvent::Checkpointed {
                        reason: reason.clone(),
                        quiescent: true,
                    },
                )?;
                ResponsePayload::Accepted { seq }
            }
            WorkerRequest::Close => {
                if self.snapshot.phase == WorkerPhase::Closed {
                    return Ok(error(
                        ErrorCode::InvalidState,
                        "worker is already closed",
                        false,
                    ));
                }
                let seq = self.append_event(Some(&envelope.request_id), WorkerEvent::Closing)?;
                ResponsePayload::Accepted { seq }
            }
        };

        if envelope.request.is_mutating() {
            self.snapshot.handled_requests.insert(
                envelope.request_id.clone(),
                HandledRequest {
                    request: envelope.request.clone(),
                    payload: payload.clone(),
                },
            );
            if runtime_bound_request(&envelope.request) {
                self.snapshot.dispatches.insert(
                    envelope.request_id.clone(),
                    DispatchRecord {
                        request: envelope.request.clone(),
                        state: DispatchState::Pending,
                    },
                );
            }
            self.persist_snapshot()?;
        }
        Ok(ResponseBody::Ok { payload })
    }

    fn append_event(&mut self, request_id: Option<&str>, event: WorkerEvent) -> Result<u64> {
        let seq = self
            .snapshot
            .latest_seq
            .checked_add(1)
            .ok_or_else(|| anyhow!("event sequence exhausted"))?;
        let event = SequencedEvent {
            seq,
            recorded_at_ms: Some(chrono::Utc::now().timestamp_millis()),
            request_id: request_id.map(str::to_owned),
            event,
        };
        let event_path = self.root.join("events.jsonl");
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&event_path)
            .with_context(|| format!("open {}", event_path.display()))?;
        serde_json::to_writer(&mut file, &event)?;
        file.write_all(b"\n")?;
        file.sync_data()?;

        apply_event(&mut self.snapshot, &event)?;
        self.events.push(event);
        self.persist_snapshot()?;
        Ok(seq)
    }

    fn persist_snapshot(&self) -> Result<()> {
        let path = self.root.join("snapshot.json");
        let temporary = self.root.join("snapshot.json.new");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .with_context(|| format!("open {}", temporary.display()))?;
        serde_json::to_writer_pretty(&mut file, &self.snapshot)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(&temporary, &path).with_context(|| format!("replace {}", path.display()))?;
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }

    fn recover_dispatches(&mut self) -> Result<()> {
        // Current-baseline workers may have persisted accepted runtime
        // mutations without a dispatch ledger.  Reconstruct only operations
        // that are safe to repeat.  An active prompt is ambiguous and must
        // never be automatically replayed.
        for (request_id, handled) in self.snapshot.handled_requests.clone() {
            if self.snapshot.dispatches.contains_key(&request_id) {
                continue;
            }
            if matches!(
                handled.request,
                WorkerRequest::SetConfig { .. } | WorkerRequest::Close
            ) {
                self.snapshot.dispatches.insert(
                    request_id,
                    DispatchRecord {
                        request: handled.request,
                        state: DispatchState::Pending,
                    },
                );
            }
        }

        let has_prompt_dispatch = self
            .snapshot
            .dispatches
            .values()
            .any(|dispatch| matches!(dispatch.request, WorkerRequest::Prompt { .. }));
        let mut interrupted_prompt = self.snapshot.active_prompt.is_some() && !has_prompt_dispatch;
        for dispatch in self.snapshot.dispatches.values_mut() {
            if dispatch.state != DispatchState::InFlight {
                continue;
            }
            match dispatch.request {
                WorkerRequest::Prompt { .. } => {
                    dispatch.state = DispatchState::Interrupted;
                    interrupted_prompt = true;
                }
                WorkerRequest::Cancel => dispatch.state = DispatchState::Completed,
                WorkerRequest::SetConfig { .. } | WorkerRequest::Close => {
                    dispatch.state = DispatchState::Pending;
                }
                _ => dispatch.state = DispatchState::Completed,
            }
        }
        self.persist_snapshot()?;

        if interrupted_prompt {
            self.record_adapter_event(
                "dispatch_interrupted",
                serde_json::json!({
                    "type": "warning",
                    "message": "the worker restarted while a prompt may have been in flight; it was not replayed"
                }),
            )?;
            if self.snapshot.phase == WorkerPhase::Running {
                self.append_event(None, WorkerEvent::TurnCompleted)?;
            }
            self.complete_prompt_dispatches()?;
        }
        Ok(())
    }

    fn complete_prompt_dispatches(&mut self) -> Result<()> {
        self.complete_dispatches_matching(|request| {
            matches!(
                request,
                WorkerRequest::Prompt { .. } | WorkerRequest::Cancel
            )
        })
    }

    fn complete_dispatches_matching(
        &mut self,
        predicate: impl Fn(&WorkerRequest) -> bool,
    ) -> Result<()> {
        let mut changed = false;
        for dispatch in self.snapshot.dispatches.values_mut() {
            if predicate(&dispatch.request)
                && matches!(
                    dispatch.state,
                    DispatchState::Pending | DispatchState::InFlight
                )
            {
                dispatch.state = DispatchState::Completed;
                changed = true;
            }
        }
        if changed {
            self.persist_snapshot()?;
        }
        Ok(())
    }
}

fn runtime_bound_request(request: &WorkerRequest) -> bool {
    matches!(
        request,
        WorkerRequest::Prompt { .. }
            | WorkerRequest::Cancel
            | WorkerRequest::SetConfig { .. }
            | WorkerRequest::Close
    )
}

/// Recover the native identity from canonical history written by older
/// workers. This is also used by checkpoint repair when controller state lacks
/// the ID. A newer session with no accepted prompt is an empty restart, not a
/// replacement for an older session that contains the actual work.
pub(crate) fn recover_native_session_id(events: &[SequencedEvent]) -> Option<String> {
    let mut candidates = Vec::<(String, bool)>::new();
    for event in events {
        match &event.event {
            WorkerEvent::Adapter { kind, payload } if kind == "session_started" => {
                let Some(id) = payload.get("native_session_id").and_then(Value::as_str) else {
                    continue;
                };
                if validate_identifier(id, "native session ID").is_ok() {
                    candidates.push((id.to_owned(), false));
                }
            }
            WorkerEvent::PromptAccepted { .. } => {
                if let Some((_, prompted)) = candidates.last_mut() {
                    *prompted = true;
                }
            }
            _ => {}
        }
    }
    candidates
        .iter()
        .rev()
        .find_map(|(id, prompted)| prompted.then(|| id.clone()))
        .or_else(|| candidates.last().map(|(id, _)| id.clone()))
}

fn apply_event(snapshot: &mut WorkerSnapshot, event: &SequencedEvent) -> Result<()> {
    if event.seq != snapshot.latest_seq + 1 {
        bail!(
            "event sequence gap: expected {}, found {}",
            snapshot.latest_seq + 1,
            event.seq
        );
    }
    match &event.event {
        WorkerEvent::PromptAccepted {
            request_id,
            text,
            attachments,
        } => {
            snapshot.phase = WorkerPhase::Running;
            snapshot.active_prompt = Some(ActivePrompt {
                request_id: request_id.clone(),
                text: text.clone(),
                attachments: attachments.clone(),
            });
        }
        WorkerEvent::TurnCompleted => {
            snapshot.phase = WorkerPhase::Idle;
            snapshot.active_prompt = None;
        }
        // Cancellation acceptance precedes the ACP prompt future resolving.
        // Keep rejecting prompts until the runtime records TurnCompleted.
        WorkerEvent::Cancelled => {}
        WorkerEvent::ConfigChanged { key, value } => {
            snapshot.config.insert(key.clone(), value.clone());
        }
        WorkerEvent::Checkpointed { .. } => snapshot.last_checkpoint_seq = Some(event.seq),
        WorkerEvent::Closing => snapshot.phase = WorkerPhase::Closing,
        WorkerEvent::Closed => {
            snapshot.phase = WorkerPhase::Closed;
            snapshot.active_prompt = None;
        }
        WorkerEvent::Adapter { .. } => {}
    }
    snapshot.latest_seq = event.seq;
    if let Some(request_id) = &event.request_id {
        let request = match &event.event {
            WorkerEvent::PromptAccepted {
                text, attachments, ..
            } => WorkerRequest::Prompt {
                text: text.clone(),
                attachments: attachments.clone(),
            },
            WorkerEvent::Cancelled => WorkerRequest::Cancel,
            WorkerEvent::ConfigChanged { key, value } => WorkerRequest::SetConfig {
                key: key.clone(),
                value: value.clone(),
            },
            WorkerEvent::Checkpointed { reason, quiescent } => {
                if *quiescent {
                    WorkerRequest::CheckpointWhenQuiescent {
                        reason: reason.clone(),
                    }
                } else {
                    WorkerRequest::Checkpoint {
                        reason: reason.clone(),
                    }
                }
            }
            WorkerEvent::Closing => WorkerRequest::Close,
            WorkerEvent::TurnCompleted | WorkerEvent::Closed | WorkerEvent::Adapter { .. } => {
                bail!("non-controller event carries a request ID")
            }
        };
        snapshot.handled_requests.insert(
            request_id.clone(),
            HandledRequest {
                request,
                payload: ResponsePayload::Accepted { seq: event.seq },
            },
        );
    }
    Ok(())
}

fn load_event_log(path: &Path) -> Result<Vec<SequencedEvent>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;
    let has_partial_tail = !bytes.is_empty() && !bytes.ends_with(b"\n");
    let mut events = Vec::new();
    for (index, line) in bytes.split(|byte| *byte == b'\n').enumerate() {
        if line.is_empty() {
            continue;
        }
        match serde_json::from_slice(line) {
            Ok(event) => events.push(event),
            Err(_)
                if has_partial_tail && index == bytes.split(|byte| *byte == b'\n').count() - 1 =>
            {
                // A crash can leave only the last append incomplete. It was
                // never fsynced as a valid frame and is safe to ignore.
            }
            Err(error) => return Err(error).with_context(|| format!("parse event {}", index + 1)),
        }
    }
    Ok(events)
}

fn read_native_session_identity(root: &Path) -> Result<Option<String>> {
    let path = root.join(NATIVE_SESSION_IDENTITY_FILE);
    if !path.exists() {
        return Ok(None);
    }
    let body = fs::read(&path).with_context(|| format!("read {}", path.display()))?;
    let identity: NativeSessionIdentity =
        serde_json::from_slice(&body).with_context(|| format!("parse {}", path.display()))?;
    if identity.version != NATIVE_SESSION_IDENTITY_VERSION {
        bail!(
            "unsupported native session identity version {}",
            identity.version
        );
    }
    validate_identifier(&identity.native_session_id, "native session ID")?;
    Ok(Some(identity.native_session_id))
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

fn validate_event_sequence(events: &[SequencedEvent]) -> Result<()> {
    for (index, event) in events.iter().enumerate() {
        let expected = index as u64 + 1;
        if event.seq != expected {
            bail!(
                "event log sequence gap: expected {expected}, found {}",
                event.seq
            );
        }
    }
    Ok(())
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

fn validate_config_key(key: &str) -> Result<()> {
    if key.is_empty()
        || key.len() > 128
        || !key.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.')
        })
    {
        bail!("invalid config key");
    }
    Ok(())
}

fn error(code: ErrorCode, message: impl Into<String>, retryable: bool) -> ResponseBody {
    super_error(code, message, retryable)
}

fn super_error(code: ErrorCode, message: impl Into<String>, retryable: bool) -> ResponseBody {
    ResponseBody::Error {
        error: ProtocolError {
            code,
            message: message.into(),
            retryable,
            detail: None,
        },
    }
}

pub fn unsupported_method_response(
    request_id: String,
    protocol_version: u32,
    method: String,
) -> ResponseEnvelope {
    ResponseEnvelope {
        request_id,
        protocol_version,
        body: ResponseBody::Error {
            error: ProtocolError {
                code: ErrorCode::InvalidRequest,
                message: format!("worker does not support method {method:?}"),
                retryable: false,
                detail: Some(ProtocolErrorDetail::UnsupportedMethod { method }),
            },
        },
    }
}

pub fn invalid_request_response(
    request_id: String,
    protocol_version: u32,
    message: String,
) -> ResponseEnvelope {
    ResponseEnvelope {
        request_id,
        protocol_version,
        body: super_error(ErrorCode::InvalidRequest, message, false),
    }
}

pub fn read_frame(reader: &mut impl BufRead) -> Result<Option<RequestEnvelope>> {
    let mut bytes = Vec::new();
    let read = reader.read_until(b'\n', &mut bytes)?;
    if read == 0 {
        return Ok(None);
    }
    if bytes.len() > MAX_FRAME_BYTES {
        bail!("worker protocol frame exceeds {MAX_FRAME_BYTES} bytes");
    }
    while bytes
        .last()
        .is_some_and(|byte| matches!(byte, b'\n' | b'\r'))
    {
        bytes.pop();
    }
    if bytes.is_empty() {
        bail!("empty worker protocol frame");
    }
    serde_json::from_slice(&bytes)
        .context("parse worker protocol request")
        .map(Some)
}

pub fn write_frame(writer: &mut impl Write, response: &ResponseEnvelope) -> Result<()> {
    serde_json::to_writer(&mut *writer, response)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

pub fn serve_json_lines(
    reader: &mut impl BufRead,
    writer: &mut impl Write,
    worker: &mut DurableWorker,
) -> Result<()> {
    while let Some(request) = read_frame(reader)? {
        let response = worker.handle(request);
        write_frame(writer, &response)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufReader, Cursor};

    const SESSION: &str = "018f9dd2-a3b4-7c8d-9000-123456789abc";

    fn request(request_id: &str, request: WorkerRequest) -> RequestEnvelope {
        RequestEnvelope {
            request_id: request_id.to_owned(),
            protocol_version: PROTOCOL_VERSION,
            request,
        }
    }

    fn accepted(response: &ResponseEnvelope) -> u64 {
        let ResponseBody::Ok {
            payload: ResponsePayload::Accepted { seq },
        } = &response.body
        else {
            panic!("expected accepted response, got {:?}", response.body);
        };
        *seq
    }

    #[test]
    fn replay_pages_stop_at_byte_budget_and_report_included_seq() {
        let big_text = "x".repeat(REPLAY_BYTE_BUDGET / 3);
        let events: Vec<SequencedEvent> = (1..=3)
            .map(|seq| SequencedEvent {
                seq,
                recorded_at_ms: None,
                request_id: None,
                event: WorkerEvent::PromptAccepted {
                    request_id: format!("r{seq}"),
                    text: big_text.clone(),
                    attachments: vec![],
                },
            })
            .collect();
        let (page, through) = page_events(events.clone(), 0, 3);
        assert_eq!(page.len(), 2, "two half-budget events fill the page");
        assert_eq!(through, 2, "reports the last included sequence");
        let (rest, through) = page_events(events[2..].to_vec(), 2, 3);
        assert_eq!(rest.len(), 1);
        assert_eq!(through, 3, "an untrimmed page reports the global latest");
        let (empty, through) = page_events(Vec::new(), 3, 3);
        assert!(empty.is_empty());
        assert_eq!(through, 3);
    }

    #[test]
    fn protocol_frames_round_trip_as_json_lines() {
        let envelope = request(
            "hello-1",
            WorkerRequest::Hello {
                client_version: "1.2.3".to_owned(),
                supported: VersionRange::CURRENT,
            },
        );
        let bytes = format!("{}\n", serde_json::to_string(&envelope).unwrap()).into_bytes();
        let decoded = read_frame(&mut BufReader::new(Cursor::new(bytes)))
            .unwrap()
            .unwrap();
        assert_eq!(decoded, envelope);

        let response = ResponseEnvelope {
            request_id: "hello-1".to_owned(),
            protocol_version: PROTOCOL_VERSION,
            body: ResponseBody::Ok {
                payload: ResponsePayload::Hello {
                    negotiated: 1,
                    worker_version: "1.0.0".to_owned(),
                    session_id: SESSION.to_owned(),
                },
            },
        };
        let mut output = Vec::new();
        write_frame(&mut output, &response).unwrap();
        assert!(output.ends_with(b"\n"));
        assert_eq!(
            serde_json::from_slice::<ResponseEnvelope>(&output).unwrap(),
            response
        );
    }

    #[test]
    fn current_v1_wire_shapes_match_frozen_fixtures() {
        let hello = RequestEnvelope {
            request_id: "hello-1".into(),
            protocol_version: 1,
            request: WorkerRequest::Hello {
                client_version: "1.2.3".into(),
                supported: VersionRange { min: 1, max: 1 },
            },
        };
        let response = ResponseEnvelope {
            request_id: "hello-1".into(),
            protocol_version: 1,
            body: ResponseBody::Ok {
                payload: ResponsePayload::Hello {
                    negotiated: 1,
                    worker_version: "1.0.0".into(),
                    session_id: SESSION.into(),
                },
            },
        };
        let event = SequencedEvent {
            seq: 1,
            recorded_at_ms: None,
            request_id: Some("prompt-1".into()),
            event: WorkerEvent::PromptAccepted {
                request_id: "prompt-1".into(),
                text: "fix it".into(),
                attachments: Vec::new(),
            },
        };
        assert_eq!(
            serde_json::to_string(&hello).unwrap(),
            include_str!("../tests/fixtures/worker-v1/hello-request.json").trim()
        );
        assert_eq!(
            serde_json::to_string(&response).unwrap(),
            include_str!("../tests/fixtures/worker-v1/hello-response.json").trim()
        );
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            include_str!("../tests/fixtures/worker-v1/prompt-event.json").trim()
        );
    }

    #[test]
    fn unknown_methods_are_structured_fail_on_use_errors() {
        let response = unsupported_method_response(
            "future-1".into(),
            PROTOCOL_VERSION,
            "future_action".into(),
        );
        assert!(matches!(
            response.body,
            ResponseBody::Error {
                error: ProtocolError {
                    code: ErrorCode::InvalidRequest,
                    retryable: false,
                    detail: Some(ProtocolErrorDetail::UnsupportedMethod { ref method }),
                    ..
                }
            } if method == "future_action"
        ));
    }

    #[test]
    fn pending_dispatch_survives_restart_and_is_claimed_once() {
        let temp = tempfile::tempdir().unwrap();
        {
            let mut worker = DurableWorker::open(temp.path(), SESSION, "1.0.0").unwrap();
            accepted(&worker.handle(request(
                "prompt-pending",
                WorkerRequest::Prompt {
                    text: "once".into(),
                    attachments: Vec::new(),
                },
            )));
        }
        let mut worker = DurableWorker::open(temp.path(), SESSION, "1.0.0").unwrap();
        assert_eq!(
            worker.claim_pending_dispatches().unwrap(),
            vec![(
                "prompt-pending".into(),
                WorkerRequest::Prompt {
                    text: "once".into(),
                    attachments: Vec::new(),
                }
            )]
        );
        assert!(worker.claim_pending_dispatches().unwrap().is_empty());
    }

    #[test]
    fn durable_events_record_worker_wall_clock_time() {
        let temp = tempfile::tempdir().unwrap();
        let before = chrono::Utc::now().timestamp_millis();
        let mut worker = DurableWorker::open(temp.path(), SESSION, "1.0.0").unwrap();
        accepted(&worker.handle(request(
            "prompt-time",
            WorkerRequest::Prompt {
                text: "when".into(),
                attachments: Vec::new(),
            },
        )));
        let after = chrono::Utc::now().timestamp_millis();

        let recorded = worker.events_after(0).unwrap()[0]
            .recorded_at_ms
            .expect("new event timestamp");
        assert!((before..=after).contains(&recorded));
    }

    #[test]
    fn in_flight_prompt_becomes_visible_interruption_instead_of_replay() {
        let temp = tempfile::tempdir().unwrap();
        {
            let mut worker = DurableWorker::open(temp.path(), SESSION, "1.0.0").unwrap();
            accepted(&worker.handle(request(
                "prompt-in-flight",
                WorkerRequest::Prompt {
                    text: "maybe ran".into(),
                    attachments: Vec::new(),
                },
            )));
            assert_eq!(worker.claim_pending_dispatches().unwrap().len(), 1);
        }
        let mut worker = DurableWorker::open(temp.path(), SESSION, "1.0.0").unwrap();
        assert!(worker.claim_pending_dispatches().unwrap().is_empty());
        assert_eq!(worker.snapshot().phase, WorkerPhase::Idle);
        assert!(worker.events_after(0).unwrap().iter().any(|event| matches!(
            &event.event,
            WorkerEvent::Adapter { kind, .. } if kind == "dispatch_interrupted"
        )));
    }

    #[test]
    fn incompatible_version_returns_structured_error() {
        let temp = tempfile::tempdir().unwrap();
        let mut worker = DurableWorker::open(temp.path(), SESSION, "1.0.0").unwrap();
        let response = worker.handle(request(
            "hello-old",
            WorkerRequest::Hello {
                client_version: "old".to_owned(),
                supported: VersionRange { min: 7, max: 9 },
            },
        ));
        assert!(matches!(
            response.body,
            ResponseBody::Error {
                error: ProtocolError {
                    code: ErrorCode::IncompatibleProtocol,
                    retryable: false,
                    ..
                }
            }
        ));
    }

    #[test]
    fn protocol_v1_rejects_quiescent_checkpoint_without_rejecting_legacy_checkpoint() {
        let temp = tempfile::tempdir().unwrap();
        let mut worker = DurableWorker::open(temp.path(), SESSION, "1.0.0").unwrap();
        let quiescent = worker.handle(RequestEnvelope {
            request_id: "checkpoint-v2".into(),
            protocol_version: 1,
            request: WorkerRequest::CheckpointWhenQuiescent { reason: None },
        });
        assert!(matches!(
            quiescent.body,
            ResponseBody::Error {
                error: ProtocolError {
                    code: ErrorCode::IncompatibleProtocol,
                    retryable: false,
                    ..
                }
            }
        ));

        let legacy = worker.handle(RequestEnvelope {
            request_id: "checkpoint-v1".into(),
            protocol_version: 1,
            request: WorkerRequest::Checkpoint { reason: None },
        });
        assert_eq!(legacy.protocol_version, 1);
        assert_eq!(accepted(&legacy), 1);
    }

    #[test]
    fn durable_replay_survives_reopen() {
        let temp = tempfile::tempdir().unwrap();
        {
            let mut worker = DurableWorker::open(temp.path(), SESSION, "1.0.0").unwrap();
            assert_eq!(
                accepted(&worker.handle(request(
                    "prompt-1",
                    WorkerRequest::Prompt {
                        text: "fix it".to_owned(),
                        attachments: vec![]
                    }
                ))),
                1
            );
            worker
                .record_adapter_event("text_delta", serde_json::json!({ "text": "done" }))
                .unwrap();
            worker.record_turn_completed().unwrap();
        }
        let worker = DurableWorker::open(temp.path(), SESSION, "1.0.0").unwrap();
        assert_eq!(worker.snapshot.phase, WorkerPhase::Idle);
        assert_eq!(worker.snapshot.latest_seq, 3);
        assert_eq!(worker.events_after(1).unwrap().len(), 2);
    }

    #[test]
    fn native_session_identity_survives_worker_reopen() {
        let temp = tempfile::tempdir().unwrap();
        let native = "019feb39-865b-7392-b358-96932c672a42";
        {
            let mut worker = DurableWorker::open(temp.path(), SESSION, "1.0.0").unwrap();
            worker
                .record_native_session_started(
                    "session_started",
                    serde_json::json!({
                        "type": "session_started",
                        "native_session_id": native,
                        "resumed": false,
                    }),
                    native,
                )
                .unwrap();
        }

        let worker = DurableWorker::open(temp.path(), SESSION, "1.0.0").unwrap();
        assert_eq!(worker.native_session_id(), Some(native));
    }

    #[test]
    fn legacy_identity_recovery_ignores_newer_unprompted_session() {
        let temp = tempfile::tempdir().unwrap();
        let original = "019feb39-865b-7392-b358-96932c672a42";
        let empty_restart = "019feb5d-0047-7f21-88c5-814eb58b7992";
        let mut worker = DurableWorker::open(temp.path(), SESSION, "1.0.0").unwrap();
        for native_session_id in [original, empty_restart] {
            worker
                .record_adapter_event(
                    "session_started",
                    serde_json::json!({
                        "type": "session_started",
                        "native_session_id": native_session_id,
                        "resumed": false,
                    }),
                )
                .unwrap();
            if native_session_id == original {
                accepted(&worker.handle(request(
                    "prompt-before-restart",
                    WorkerRequest::Prompt {
                        text: "do work".into(),
                        attachments: vec![],
                    },
                )));
                worker.record_turn_completed().unwrap();
            }
        }

        assert_eq!(
            worker.recover_native_session_id_from_events().as_deref(),
            Some(original)
        );
    }

    #[test]
    fn duplicate_mutation_is_idempotent_across_restart() {
        let temp = tempfile::tempdir().unwrap();
        let prompt = request(
            "prompt-stable-key",
            WorkerRequest::Prompt {
                text: "once".to_owned(),
                attachments: vec![],
            },
        );
        {
            let mut worker = DurableWorker::open(temp.path(), SESSION, "1.0.0").unwrap();
            assert_eq!(accepted(&worker.handle(prompt.clone())), 1);
        }
        let mut worker = DurableWorker::open(temp.path(), SESSION, "1.0.0").unwrap();
        assert_eq!(accepted(&worker.handle(prompt)), 1);
        assert_eq!(worker.snapshot.latest_seq, 1);
        assert_eq!(worker.events.len(), 1);
    }

    #[test]
    fn idempotency_key_cannot_be_reused_for_another_mutation() {
        let temp = tempfile::tempdir().unwrap();
        let mut worker = DurableWorker::open(temp.path(), SESSION, "1.0.0").unwrap();
        accepted(&worker.handle(request(
            "mutation-key",
            WorkerRequest::SetConfig {
                key: "mode".to_owned(),
                value: Value::String("auto".to_owned()),
            },
        )));
        let response = worker.handle(request("mutation-key", WorkerRequest::Close));
        assert!(matches!(
            response.body,
            ResponseBody::Error {
                error: ProtocolError {
                    code: ErrorCode::InvalidRequest,
                    ..
                }
            }
        ));
        assert_eq!(worker.snapshot.phase, WorkerPhase::Idle);
        assert_eq!(worker.snapshot.latest_seq, 1);
    }

    #[test]
    fn state_machine_rejects_parallel_prompts_and_replays_after_sequence() {
        let temp = tempfile::tempdir().unwrap();
        let mut worker = DurableWorker::open(temp.path(), SESSION, "1.0.0").unwrap();
        accepted(&worker.handle(request(
            "p1",
            WorkerRequest::Prompt {
                text: "one".to_owned(),
                attachments: vec![],
            },
        )));
        let second = worker.handle(request(
            "p2",
            WorkerRequest::Prompt {
                text: "two".to_owned(),
                attachments: vec![],
            },
        ));
        assert!(matches!(
            second.body,
            ResponseBody::Error {
                error: ProtocolError {
                    code: ErrorCode::InvalidState,
                    ..
                }
            }
        ));
        worker.record_turn_completed().unwrap();
        let replay = worker.handle(request("sub", WorkerRequest::Subscribe { after_seq: 1 }));
        let ResponseBody::Ok {
            payload: ResponsePayload::Replay { events, latest_seq },
        } = replay.body
        else {
            panic!("expected replay");
        };
        assert_eq!(latest_seq, 2);
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn cancellation_keeps_worker_busy_until_runtime_finishes_the_turn() {
        let temp = tempfile::tempdir().unwrap();
        let mut worker = DurableWorker::open(temp.path(), SESSION, "1.0.0").unwrap();
        accepted(&worker.handle(request(
            "prompt",
            WorkerRequest::Prompt {
                text: "wait".to_owned(),
                attachments: vec![],
            },
        )));
        accepted(&worker.handle(request("cancel", WorkerRequest::Cancel)));
        assert_eq!(worker.snapshot.phase, WorkerPhase::Running);
        assert!(worker.snapshot.active_prompt.is_some());

        let early = worker.handle(request(
            "too-early",
            WorkerRequest::Prompt {
                text: "next".to_owned(),
                attachments: vec![],
            },
        ));
        assert!(matches!(
            early.body,
            ResponseBody::Error {
                error: ProtocolError {
                    code: ErrorCode::InvalidState,
                    ..
                }
            }
        ));

        worker.record_turn_completed().unwrap();
        assert_eq!(worker.snapshot.phase, WorkerPhase::Idle);
        assert!(worker.snapshot.active_prompt.is_none());
    }

    #[test]
    fn config_checkpoint_and_close_are_persisted() {
        let temp = tempfile::tempdir().unwrap();
        let mut worker = DurableWorker::open(temp.path(), SESSION, "1.0.0").unwrap();
        assert_eq!(
            accepted(&worker.handle(request(
                "config",
                WorkerRequest::SetConfig {
                    key: "mode".to_owned(),
                    value: Value::String("auto".to_owned())
                }
            ))),
            1
        );
        assert_eq!(
            accepted(&worker.handle(request(
                "checkpoint",
                WorkerRequest::Checkpoint {
                    reason: Some("manual".to_owned())
                }
            ))),
            2
        );
        assert_eq!(
            accepted(&worker.handle(request("close", WorkerRequest::Close))),
            3
        );
        assert_eq!(worker.snapshot.phase, WorkerPhase::Closing);
        assert_eq!(worker.snapshot.last_checkpoint_seq, Some(2));
        assert_eq!(worker.snapshot.config["mode"], "auto");
        worker.record_closed().unwrap();
        assert_eq!(worker.snapshot.phase, WorkerPhase::Closed);
    }

    #[test]
    fn subscribe_rejects_future_sequence() {
        let temp = tempfile::tempdir().unwrap();
        let mut worker = DurableWorker::open(temp.path(), SESSION, "1.0.0").unwrap();
        let response = worker.handle(request(
            "future",
            WorkerRequest::Subscribe { after_seq: 42 },
        ));
        assert!(matches!(
            response.body,
            ResponseBody::Error {
                error: ProtocolError {
                    code: ErrorCode::SequenceOutOfRange,
                    ..
                }
            }
        ));
    }

    #[test]
    fn json_lines_server_processes_multiple_requests_without_a_port() {
        let temp = tempfile::tempdir().unwrap();
        let mut worker = DurableWorker::open(temp.path(), SESSION, "1.0.0").unwrap();
        let input = [
            request("status", WorkerRequest::Status),
            request("snapshot", WorkerRequest::Snapshot),
        ]
        .into_iter()
        .map(|value| serde_json::to_string(&value).unwrap())
        .collect::<Vec<_>>()
        .join("\n")
            + "\n";
        let mut output = Vec::new();
        serve_json_lines(
            &mut BufReader::new(Cursor::new(input.into_bytes())),
            &mut output,
            &mut worker,
        )
        .unwrap();
        assert_eq!(
            output
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.is_empty())
                .count(),
            2
        );
    }
}
