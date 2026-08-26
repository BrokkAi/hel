//! Minimal full-screen chat for one persistent Hel worker.
//!
//! The view state lives here; the concerns around it are split into
//! submodules: [`input`] edits the composer, [`history`] recalls earlier
//! prompts, [`autocomplete`] parses and completes slash commands,
//! [`transcript`] projects and draws the conversation, [`remote`] runs the
//! relay operations a key press asks for, and [`active`] wires a live session
//! to all of them.

mod active;
mod autocomplete;
mod elicitation;
mod history;
mod input;
mod remote;
mod rendering;
mod transcript;

#[cfg(test)]
mod test_support;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use agent_client_protocol::schema::v1::{
    AvailableCommand, ContentBlock, ContentChunk, Plan, PlanEntry, PlanEntryPriority,
    PlanEntryStatus, SessionConfigOption, SessionModeState, SessionUpdate, TextContent, ToolCall,
    ToolCallContent, ToolCallStatus,
};
use crossterm::event::{
    KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};
use ratatui::layout::{Position, Rect};
use ratatui::style::Color;
use sha2::{Digest, Sha256};

use crate::clock::epoch_seconds;
use crate::hel_acp::{RuntimeEvent, find_session_config_option, select_contains};
use crate::hel_config::HarnessKind;
use crate::hel_elicitation::ElicitationValue;
use crate::hel_elicitation::{ElicitationRequest, ElicitationResponse};
use crate::hel_state::{
    MaterializedExecutionState, MaterializedQueuedPrompt, MaterializedSession, QueuedCommandKind,
    TranscriptBody, TranscriptItem,
};
use crate::hel_transcript::{
    ChatEntry, ChatRole, PlanLine, PlanStatus, ToolStatus, TranscriptSource,
};
use crate::hel_worker::{
    ActiveAgentTerminal, RELAY_EVENT_GENESIS_DIGEST, SequencedEvent, WorkerEvent, WorkerPhase,
    WorkerSnapshot,
};

use autocomplete::{
    Autocomplete, CommandChoice, ConfigValueChoice, LocalCommand, builtin_command_choices,
    config_current_value, is_goal_prompt, parse_local_command,
};
use elicitation::ElicitationDialog;
use history::{HistorySearch, HistorySearchRequest};
use rendering::{TranscriptRenderMode, sanitize_terminal_text};
use transcript::{
    TAIL_SEED_ITEMS, ToolDiffstatRequest, TranscriptAnchor, TranscriptRenderCache,
    content_block_text, materialized_chat_entries_reusing, plan_status, tool_content_details,
    tool_diff_paths, tool_location_details, tool_status,
};

pub use active::ActiveChat;
pub use transcript::{
    BrowserTranscript, BrowserTranscriptEntry, TranscriptSnapshot, materialized_chunks_text,
    materialized_content_text, materialized_tool_diffstats, render_agent_message_head,
    render_agent_message_tail,
};

const MOUSE_SCROLL_ROWS: usize = 3;

/// What one terminal event asked the chat to do.
///
/// `None` means the event only changed local state, which lets the caller keep
/// draining a paste burst before it redraws. Every exit reports the ordinal the
/// user has now seen, which becomes the session's read receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChatEventOutcome {
    None,
    Handled,
    Back {
        last_seen_event_ordinal: u64,
    },
    /// The user picked another conversation. The caller saves this session the
    /// way it saves a `Back` and then opens `session_id`.
    SwitchSession {
        session_id: String,
        last_seen_event_ordinal: u64,
    },
    QuitDetach {
        last_seen_event_ordinal: u64,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ChatAction {
    None,
    Prompt(String),
    RunShell(String),
    RemoveQueuedPrompt {
        id: String,
        text: String,
        kind: QueuedCommandKind,
    },
    SetConfig {
        key: String,
        value: String,
    },
    SetSessionMode {
        mode_id: String,
    },
    PlanCommand {
        original: String,
        control: PlanControl,
        requested_active: bool,
        prompt: Option<String>,
    },
    Cancel,
    RespondElicitation {
        request: ElicitationRequest,
        response: ElicitationResponse,
    },
    PasteFromClipboard,
    ToggleVoice,
    SwitchSession {
        session_id: String,
    },
    Back,
    QuitDetach,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanControl {
    SetConfig { key: String, value: String },
    SetSessionMode { mode_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueuedPrompt {
    id: String,
    text: String,
    kind: QueuedCommandKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlanReviewFollowup {
    desired_active: bool,
    control: Option<PlanControl>,
    prompt: Option<String>,
}

impl QueuedPrompt {
    /// The label shown above the composer. A queued configuration change is
    /// marked so it is never mistaken for a prompt waiting to be sent.
    fn queue_label(&self) -> &'static str {
        if self.kind.is_prompt() {
            "queued"
        } else {
            "queued config"
        }
    }
}

/// What the chat's session header shows when it opens: where this session sits
/// among the same-project active sessions, and the other sessions it lists.
#[derive(Debug, Clone, Default)]
pub struct SessionHeaderIdentity {
    pub position: usize,
    pub others: Vec<OtherSessionIdentity>,
}

/// Identity of another same-project session, snapshotted when the chat opens.
/// `position` is its place in that list at that moment, not a live value.
#[derive(Debug, Clone)]
pub struct OtherSessionIdentity {
    pub session_id: String,
    pub position: usize,
}

/// What the conversations pane says about one other session. The id travels
/// with the activity so a row the user picks resolves to a session to open.
#[derive(Debug, Clone, PartialEq, Eq)]
struct OtherSessionActivity {
    session_id: String,
    position: usize,
    turn_started_at_epoch_seconds: Option<u64>,
    last_agent_line: Option<String>,
}

/// Which part of the chat view the keyboard is driving.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum ChatFocus {
    #[default]
    Prompt,
    Conversations,
}

/// Constructors that need [`sanitize_terminal_text`], which is chat-view
/// specific and so cannot live with the rest of [`ChatEntry`] in
/// `hel_transcript`.
impl ChatEntry {
    fn plain(seq: u64, role: ChatRole, text: impl Into<String>) -> Self {
        Self {
            start_seq: seq,
            seq,
            role,
            text: sanitize_terminal_text(&text.into()),
            recorded_at_ms: None,
            revision: 0,
            message_id: None,
            tool_call_id: None,
            tool_status: None,
            tool_content: Vec::new(),
            tool_diffstats: Vec::new(),
            tool_locations: Vec::new(),
            plan: Vec::new(),
            leading_omitted: false,
            raw_only: false,
            source: TranscriptSource::default(),
        }
    }

    fn tool(
        seq: u64,
        title: impl Into<String>,
        tool_call_id: Option<String>,
        tool_status: ToolStatus,
    ) -> Self {
        Self {
            start_seq: seq,
            seq,
            role: ChatRole::Tool,
            text: sanitize_terminal_text(&title.into()),
            recorded_at_ms: None,
            revision: 0,
            message_id: None,
            tool_call_id,
            tool_status: Some(tool_status),
            tool_content: Vec::new(),
            tool_diffstats: Vec::new(),
            tool_locations: Vec::new(),
            plan: Vec::new(),
            leading_omitted: false,
            raw_only: false,
            source: TranscriptSource::default(),
        }
    }
}

pub struct ChatState {
    session_id: String,
    bundle_id: Option<String>,
    phase: WorkerPhase,
    latest_seq: u64,
    last_compaction_seq: u64,
    entries: Vec<ChatEntry>,
    pending_diffstats: VecDeque<ToolDiffstatRequest>,
    scheduled_diffstats: BTreeSet<(String, u64)>,
    /// Leading transcript items that are not converted to entries yet, because
    /// a large session opens on its tail and converts the rest off the event
    /// loop. Zero whenever the projection is complete.
    unconverted_prefix: usize,
    /// The last unconverted transcript item: the projection item a pending
    /// prefix has to end at to be spliced in front of the tail. `None`
    /// whenever the projection is complete.
    prefix_seam: Option<Arc<TranscriptItem>>,
    input: String,
    input_cursor: usize,
    /// Stored prompts from other sessions in this project, oldest-first.
    project_history: Vec<String>,
    /// Stored prompts from this session, oldest-first.
    session_history: Vec<String>,
    project_history_error: Option<String>,
    prompt_history: Vec<String>,
    history_index: Option<usize>,
    history_draft: String,
    kill_buffer: String,
    /// Set by Ctrl-K so the next Ctrl-K appends instead of replacing.
    chain_kill: bool,
    preferred_column: Option<usize>,
    history_search: Option<HistorySearch>,
    next_history_search_generation: u64,
    pending_history_search: Option<HistorySearchRequest>,
    queued_prompts: VecDeque<QueuedPrompt>,
    active_user_shells: Vec<String>,
    active_agent_terminals: Vec<ActiveAgentTerminal>,
    claimed_agent_terminals: BTreeMap<String, i64>,
    elicitation: Option<ElicitationDialog>,
    recovery_busy: bool,
    goal_prompt_active: bool,
    config_options: Vec<SessionConfigOption>,
    session_modes: Option<SessionModeState>,
    /// Latest ACP session mode, from `current_mode_update` by way of the
    /// projection, or set optimistically when Hel asks for a change.
    current_mode: Option<String>,
    harness_kind: Option<HarnessKind>,
    plan_command_pending: bool,
    agent_commands: Vec<AvailableCommand>,
    command_choices: Vec<CommandChoice>,
    model_values: Vec<ConfigValueChoice>,
    effort_values: Vec<ConfigValueChoice>,
    current_model: Option<String>,
    current_effort: Option<String>,
    autocomplete: Option<Autocomplete>,
    anchor: TranscriptAnchor,
    last_viewport_height: usize,
    render_mode: TranscriptRenderMode,
    render_cache: TranscriptRenderCache,
    notices: Notices,
    voice_active: bool,
    /// Project name and header position of this session, snapshotted when the
    /// chat opened.
    position: usize,
    turn_started_at_epoch_seconds: Option<u64>,
    other_sessions: Vec<OtherSessionActivity>,
    focus: ChatFocus,
    /// Where the conversations pane's window starts. `None` centres it on the
    /// current session; the wheel pins it somewhere else until the keyboard
    /// moves through the list again.
    conversations_window_start: Option<usize>,
    /// The pane's hitbox, recorded each frame so the wheel knows what it is
    /// over. `None` before the first draw.
    conversations_area: Option<Rect>,
}

impl ChatState {
    pub fn new(snapshot: &WorkerSnapshot, events: &[SequencedEvent]) -> Self {
        let mut state = Self {
            session_id: snapshot.session_id.clone(),
            bundle_id: None,
            phase: snapshot.phase,
            latest_seq: 0,
            last_compaction_seq: 0,
            entries: Vec::new(),
            pending_diffstats: VecDeque::new(),
            scheduled_diffstats: BTreeSet::new(),
            unconverted_prefix: 0,
            prefix_seam: None,
            input: String::new(),
            input_cursor: 0,
            project_history: Vec::new(),
            session_history: Vec::new(),
            project_history_error: None,
            prompt_history: Vec::new(),
            history_index: None,
            history_draft: String::new(),
            kill_buffer: String::new(),
            chain_kill: false,
            preferred_column: None,
            history_search: None,
            next_history_search_generation: 0,
            pending_history_search: None,
            queued_prompts: VecDeque::new(),
            active_user_shells: Vec::new(),
            active_agent_terminals: Vec::new(),
            claimed_agent_terminals: BTreeMap::new(),
            elicitation: None,
            recovery_busy: false,
            goal_prompt_active: snapshot
                .active_prompt
                .as_ref()
                .is_some_and(|prompt| is_goal_prompt(&prompt.text)),
            config_options: Vec::new(),
            session_modes: None,
            current_mode: snapshot
                .config
                .get("mode")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
            harness_kind: None,
            plan_command_pending: false,
            agent_commands: Vec::new(),
            command_choices: builtin_command_choices(),
            model_values: Vec::new(),
            effort_values: Vec::new(),
            current_model: snapshot
                .config
                .get("model")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
            current_effort: snapshot
                .config
                .get("effort")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
            autocomplete: None,
            anchor: TranscriptAnchor::Bottom,
            last_viewport_height: 0,
            render_mode: TranscriptRenderMode::Rich,
            render_cache: TranscriptRenderCache::default(),
            notices: Notices::default(),
            voice_active: false,
            position: 0,
            turn_started_at_epoch_seconds: None,
            other_sessions: Vec::new(),
            focus: ChatFocus::Prompt,
            conversations_window_start: None,
            conversations_area: None,
        };
        state.apply_events(events);
        // Bootstrap replays the full canonical log for transcript projection,
        // while the snapshot is authoritative for the queue at that frontier.
        state.queued_prompts = snapshot
            .queued_prompts
            .iter()
            .map(|prompt| QueuedPrompt {
                id: prompt.id.clone(),
                text: prompt.text.clone(),
                kind: QueuedCommandKind::Prompt,
            })
            .collect();
        state.latest_seq = state.latest_seq.max(snapshot.latest_seq);
        state
    }

    pub fn from_tail(
        session_id: String,
        phase: WorkerPhase,
        latest_seq: u64,
        entries: Vec<ChatEntry>,
    ) -> Self {
        let snapshot = WorkerSnapshot::summary(session_id, phase, latest_seq);
        let mut state = Self::new(&snapshot, &[]);
        state.entries = entries;
        state
    }

    pub fn from_materialized(
        session: &MaterializedSession,
        config_options: &[SessionConfigOption],
        available_commands: &[AvailableCommand],
    ) -> Self {
        Self::from_materialized_with_prefix(session, config_options, available_commands, 0)
    }

    /// Like `from_materialized`, but a session longer than `TAIL_SEED_ITEMS`
    /// converts only its tail here. The caller converts the recorded prefix off
    /// the event loop and hands it back to `splice_transcript_prefix`, so
    /// opening a long conversation costs the tail rather than the history.
    pub fn from_materialized_tail(
        session: &MaterializedSession,
        config_options: &[SessionConfigOption],
        available_commands: &[AvailableCommand],
    ) -> Self {
        let prefix = session.transcript.len().saturating_sub(TAIL_SEED_ITEMS);
        Self::from_materialized_with_prefix(session, config_options, available_commands, prefix)
    }

    fn from_materialized_with_prefix(
        session: &MaterializedSession,
        config_options: &[SessionConfigOption],
        available_commands: &[AvailableCommand],
        unconverted_prefix: usize,
    ) -> Self {
        let phase = match session.execution {
            MaterializedExecutionState::Idle => WorkerPhase::Idle,
            MaterializedExecutionState::Running { .. } => WorkerPhase::Running,
            MaterializedExecutionState::Closing => WorkerPhase::Closing,
            MaterializedExecutionState::Closed => WorkerPhase::Closed,
        };
        let snapshot = WorkerSnapshot::summary(
            session.session_id.clone(),
            phase,
            session.applied_event_ordinal,
        );
        let mut state = Self::new(&snapshot, &[]);
        state.latest_seq = u64::MAX;
        state.unconverted_prefix = unconverted_prefix;
        state.apply_materialized(session, config_options, available_commands);
        state
    }

    pub fn apply_materialized(
        &mut self,
        session: &MaterializedSession,
        config_options: &[SessionConfigOption],
        available_commands: &[AvailableCommand],
    ) {
        let rebuild_projection = session.applied_event_ordinal != self.latest_seq;
        self.phase = match session.execution {
            MaterializedExecutionState::Idle => WorkerPhase::Idle,
            MaterializedExecutionState::Running { .. } => WorkerPhase::Running,
            MaterializedExecutionState::Closing => WorkerPhase::Closing,
            MaterializedExecutionState::Closed => WorkerPhase::Closed,
        };
        self.latest_seq = session.applied_event_ordinal;
        // The controller's projection is authoritative for the turn clock.
        self.turn_started_at_epoch_seconds = turn_started_at_epoch_seconds(session.execution);
        self.sync_elicitation(&session.pending_elicitations);
        if rebuild_projection {
            // While a prefix conversion is in flight the entries stand for the
            // tail only, so the rebuild has to line up with the same tail.
            // Compaction can shrink the transcript under the recorded prefix;
            // reseat it on the current tail rather than rebuilding the whole
            // history here. The pending prefix then fails its alignment check
            // and is rebuilt off the loop.
            if self.unconverted_prefix > session.transcript.len() {
                self.unconverted_prefix = session.transcript.len().saturating_sub(TAIL_SEED_ITEMS);
                self.entries.clear();
                self.invalidate_render_cache();
            }
            self.entries = materialized_chat_entries_reusing(
                session,
                self.unconverted_prefix,
                std::mem::take(&mut self.entries),
            );
            // Re-read the seam from the projection that produced this tail, so
            // a prefix converted against replaced history is refused.
            self.prefix_seam = self
                .unconverted_prefix
                .checked_sub(1)
                .and_then(|index| session.transcript.get(index))
                .cloned();
            for item in session.transcript.iter().skip(self.unconverted_prefix) {
                let Some(request) = ToolDiffstatRequest::from_item(item) else {
                    continue;
                };
                let key = (request.tool_call_id.clone(), request.revision);
                if self.scheduled_diffstats.insert(key) {
                    self.pending_diffstats.push_back(request);
                }
            }
            self.queued_prompts = session
                .queued_prompts
                .iter()
                .map(|prompt| QueuedPrompt {
                    id: prompt.command_id.clone(),
                    text: materialized_content_text(&prompt.content),
                    kind: prompt.kind.clone(),
                })
                .collect();
        }
        self.set_config_options(config_options);
        // `current_mode_update` lands in the projected configuration. Only
        // overwrite when it is there, so an optimistic toggle survives until
        // the agent confirms it.
        let plan_mode_key = if self.harness_kind == Some(HarnessKind::Codex) {
            "collaboration_mode"
        } else {
            "mode"
        };
        if let Some(mode) = session
            .configuration
            .get(plan_mode_key)
            .and_then(serde_json::Value::as_str)
        {
            self.current_mode = Some(mode.to_owned());
        }
        self.agent_commands = available_commands.to_vec();
        self.rebuild_command_choices();
        self.current_model = session
            .configuration
            .get("model")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .or_else(|| config_current_value(config_options, "model"));
        self.current_effort = session
            .configuration
            .get("effort")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .or_else(|| config_current_value(config_options, "effort"));
    }

    fn take_diffstat_requests(&mut self, maximum: usize) -> Vec<ToolDiffstatRequest> {
        let count = maximum.min(self.pending_diffstats.len());
        self.pending_diffstats.drain(..count).collect()
    }

    fn queue_diffstat_requests(&mut self, requests: Vec<ToolDiffstatRequest>) {
        for request in requests {
            let key = (request.tool_call_id.clone(), request.revision);
            if self.scheduled_diffstats.insert(key) {
                self.pending_diffstats.push_back(request);
            }
        }
    }

    pub(super) fn apply_diffstats(
        &mut self,
        tool_call_id: &str,
        revision: u64,
        result: std::result::Result<Vec<String>, String>,
    ) {
        let key = (tool_call_id.to_owned(), revision);
        let Some(entry) = self.entries.iter_mut().rev().find(|entry| {
            entry.tool_call_id.as_deref() == Some(tool_call_id) && entry.revision == revision
        }) else {
            self.scheduled_diffstats.remove(&key);
            return;
        };
        match result {
            Ok(diffstats) => {
                entry.tool_diffstats = diffstats;
                self.invalidate_render_cache();
            }
            Err(error) => {
                self.scheduled_diffstats.remove(&key);
                self.set_notice(format!("Could not calculate diff summary: {error}"));
            }
        }
    }

    fn sync_elicitation(&mut self, pending: &[ElicitationRequest]) {
        if self
            .elicitation
            .as_ref()
            .is_some_and(|dialog| pending.iter().any(|request| request.id == dialog.id()))
        {
            return;
        }
        self.elicitation = pending.first().cloned().map(ElicitationDialog::new);
    }

    fn restore_elicitation(&mut self, request: ElicitationRequest) {
        if self.elicitation.is_none() {
            self.elicitation = Some(ElicitationDialog::new(request));
        }
    }

    #[cfg(test)]
    pub(crate) fn bounded_entries(
        &self,
        maximum_entries: usize,
        maximum_bytes: usize,
    ) -> Vec<ChatEntry> {
        let start = self.entries.len().saturating_sub(maximum_entries);
        let mut entries = self.entries[start..]
            .iter()
            .cloned()
            .map(ChatEntry::bounded_for_dashboard)
            .collect::<Vec<_>>();
        while entries.len() > 1
            && serde_json::to_vec(&entries).is_ok_and(|body| body.len() > maximum_bytes)
        {
            entries.remove(0);
        }
        entries
    }

    pub fn phase(&self) -> WorkerPhase {
        self.phase
    }

    pub fn set_history_context(&mut self, bundle_id: impl Into<String>) {
        self.bundle_id = Some(bundle_id.into());
    }

    pub fn set_session_modes(&mut self, modes: Option<SessionModeState>) {
        let changed = self.session_modes != modes;
        self.session_modes = modes;
        if changed || self.current_mode.is_none() {
            self.set_plan_mode_from_surfaces();
        }
        self.rebuild_command_choices();
    }

    pub fn set_harness_kind(&mut self, harness_kind: HarnessKind) {
        self.harness_kind = Some(harness_kind);
        self.set_plan_mode_from_surfaces();
        self.rebuild_command_choices();
    }

    fn set_plan_mode_from_surfaces(&mut self) {
        let config_key = match self.harness_kind {
            Some(HarnessKind::Codex) => "collaboration_mode",
            Some(HarnessKind::Claude | HarnessKind::Kimi) => "mode",
            _ => "mode",
        };
        if let Some(value) = config_current_value(&self.config_options, config_key) {
            self.current_mode = Some(value);
        } else if self.harness_kind != Some(HarnessKind::Codex) {
            self.current_mode = self
                .session_modes
                .as_ref()
                .map(|modes| modes.current_mode_id.to_string());
        }
    }

    fn supports_plan_mode(&self) -> bool {
        self.plan_control(true).is_ok()
    }

    fn advertised_plan_modes(&self) -> bool {
        self.session_modes.as_ref().is_some_and(|modes| {
            ["plan", "default"].into_iter().all(|desired| {
                modes
                    .available_modes
                    .iter()
                    .any(|mode| mode.id.to_string() == desired)
            })
        })
    }

    fn config_has_plan_pair(&self, key: &str) -> bool {
        find_session_config_option(&self.config_options, key).is_some_and(|option| {
            select_contains(&option.kind, "plan") && select_contains(&option.kind, "default")
        })
    }

    fn exact_config_has_plan_pair(&self, key: &str) -> bool {
        self.config_options.iter().any(|option| {
            option.id.to_string() == key
                && select_contains(&option.kind, "plan")
                && select_contains(&option.kind, "default")
        })
    }

    fn plan_control(&self, active: bool) -> Result<PlanControl, &'static str> {
        let value = if active { "plan" } else { "default" };
        match self.harness_kind {
            Some(HarnessKind::Deepseek) => Err("Plan mode is unsupported in DSH."),
            Some(HarnessKind::Codex) => self
                .exact_config_has_plan_pair("collaboration_mode")
                .then(|| PlanControl::SetConfig {
                    key: "collaboration_mode".into(),
                    value: value.into(),
                })
                .ok_or("This Codex ACP version does not expose collaboration_mode with plan/default values."),
            Some(HarnessKind::Claude | HarnessKind::Kimi) => {
                if self.exact_config_has_plan_pair("mode") {
                    Ok(PlanControl::SetConfig {
                        key: "mode".into(),
                        value: value.into(),
                    })
                } else if self.advertised_plan_modes() {
                    Ok(PlanControl::SetSessionMode { mode_id: value.into() })
                } else {
                    Err("This ACP harness does not expose compatible plan/default modes.")
                }
            }
            Some(HarnessKind::Grok) => Ok(PlanControl::SetSessionMode {
                mode_id: value.into(),
            }),
            None => {
                if self.config_has_plan_pair("mode") {
                    Ok(PlanControl::SetConfig {
                        key: "mode".into(),
                        value: value.into(),
                    })
                } else if self.advertised_plan_modes() {
                    Ok(PlanControl::SetSessionMode { mode_id: value.into() })
                } else {
                    Err("This ACP harness does not expose compatible plan/default modes.")
                }
            }
        }
    }

    fn plan_mode_active(&self) -> bool {
        self.supports_plan_mode() && self.current_mode.as_deref() == Some("plan")
    }

    fn plan_review_followup(
        &self,
        request: &ElicitationRequest,
        response: &ElicitationResponse,
    ) -> Option<PlanReviewFollowup> {
        if !request.id.starts_with("plan-review-") {
            return None;
        }
        let ElicitationResponse::Accept { content } = response else {
            return Some(PlanReviewFollowup {
                desired_active: true,
                control: None,
                prompt: None,
            });
        };
        let action = match content.get("action") {
            Some(ElicitationValue::String(action)) => action.as_str(),
            _ => "keep_planning",
        };
        let feedback = match content.get("feedback") {
            Some(ElicitationValue::String(feedback)) if !feedback.trim().is_empty() => {
                Some(feedback.clone())
            }
            _ => None,
        };
        Some(match action {
            "implement" => PlanReviewFollowup {
                desired_active: false,
                control: None,
                prompt: None,
            },
            "exit" => PlanReviewFollowup {
                desired_active: false,
                control: self.plan_control(false).ok(),
                prompt: None,
            },
            "revise" => PlanReviewFollowup {
                desired_active: true,
                control: None,
                // Grok carries feedback in its native response. Standard ACP
                // permission responses cannot, so send it as the next planning turn.
                prompt: (!request.id.starts_with("plan-review-grok-"))
                    .then_some(feedback)
                    .flatten(),
            },
            _ => PlanReviewFollowup {
                desired_active: true,
                control: None,
                prompt: None,
            },
        })
    }

    /// Names this session in the header and places its line among the other
    /// sessions. Both are fixed for the visit.
    pub fn set_header_position(&mut self, position: usize) {
        self.position = position;
    }

    /// Last line of this session's most recent agent message that has text.
    fn last_agent_line(&self) -> Option<String> {
        self.entries
            .iter()
            .rev()
            .filter(|entry| entry.role == ChatRole::Agent)
            .find_map(|entry| last_nonempty_line(&entry.text))
    }

    pub fn latest_seq(&self) -> u64 {
        self.latest_seq
    }

    fn mark_prompt_submitted(&mut self, prompt: &str) {
        self.phase = WorkerPhase::Running;
        self.goal_prompt_active = is_goal_prompt(prompt);
        self.notices.clear();
        // Local echo: start the clock now so the header moves with the send.
        // The next materialized update replaces this with the recorded start.
        self.turn_started_at_epoch_seconds = Some(epoch_seconds());
    }

    /// Starts the header clock for a turn the event log just reported. An
    /// event with no recorded time falls back to now, because the turn is
    /// running either way.
    fn start_turn_clock(&mut self, recorded_at_ms: Option<i64>) {
        self.turn_started_at_epoch_seconds = recorded_at_ms
            .and_then(|recorded_at_ms| u64::try_from(recorded_at_ms).ok())
            .map(|recorded_at_ms| recorded_at_ms / 1_000)
            .or_else(|| Some(epoch_seconds()));
    }

    fn pursuing_goal(&self) -> bool {
        self.goal_prompt_active
            && self
                .agent_commands
                .iter()
                .any(|command| command.name == "goal")
    }
    pub fn entries(&self) -> &[ChatEntry] {
        &self.entries
    }

    /// Convert a legacy/import transcript projection into the controller's
    /// canonical logical-session model. Native importers use this at their
    /// boundary; live relay sessions are projected directly from relay events.
    pub fn materialized_session(&self) -> MaterializedSession {
        let mut stable_ids = BTreeSet::new();
        let transcript = self
            .entries
            .iter()
            .filter(|entry| entry.start_seq > 0)
            .map(|entry| {
                let base_id = match entry.role {
                    ChatRole::User => format!("user:{}", entry.start_seq),
                    ChatRole::Agent => entry.message_id.as_ref().map_or_else(
                        || format!("agent:{}", entry.start_seq),
                        |id| format!("agent:{id}"),
                    ),
                    ChatRole::Thought => entry.message_id.as_ref().map_or_else(
                        || format!("thought:{}", entry.start_seq),
                        |id| format!("thought:{id}"),
                    ),
                    ChatRole::Tool => entry.tool_call_id.as_ref().map_or_else(
                        || format!("tool:{}", entry.start_seq),
                        |id| format!("tool:{id}"),
                    ),
                    ChatRole::Plan => format!("plan:{}", entry.start_seq),
                    ChatRole::System => format!("system:{}", entry.start_seq),
                };
                let stable_id = if stable_ids.insert(base_id.clone()) {
                    base_id
                } else {
                    format!("{base_id}:{}", entry.start_seq)
                };
                let body = match entry.role {
                    ChatRole::User => TranscriptBody::User {
                        content: vec![serde_json::json!({
                            "type": "text",
                            "text": entry.text,
                        })],
                    },
                    ChatRole::Agent | ChatRole::Thought => {
                        let mut chunk = ContentChunk::new(ContentBlock::Text(TextContent::new(
                            entry.text.clone(),
                        )));
                        if let Some(message_id) = &entry.message_id {
                            chunk = chunk.message_id(message_id.as_str());
                        }
                        let chunks = vec![
                            serde_json::to_value(chunk)
                                .expect("ACP content chunk serialization cannot fail"),
                        ];
                        if entry.role == ChatRole::Agent {
                            TranscriptBody::Agent {
                                chunks,
                                streaming: false,
                            }
                        } else {
                            TranscriptBody::Thought {
                                chunks,
                                streaming: false,
                            }
                        }
                    }
                    ChatRole::Tool => {
                        let call_id = entry
                            .tool_call_id
                            .clone()
                            .unwrap_or_else(|| stable_id.clone());
                        let content = entry
                            .tool_content
                            .iter()
                            .cloned()
                            .map(|text| {
                                ToolCallContent::from(ContentBlock::Text(TextContent::new(text)))
                            })
                            .collect();
                        let mut call = ToolCall::new(call_id, entry.text.clone())
                            .status(match entry.tool_status.unwrap_or(ToolStatus::Pending) {
                                ToolStatus::Pending => ToolCallStatus::Pending,
                                ToolStatus::Running => ToolCallStatus::InProgress,
                                ToolStatus::Completed => ToolCallStatus::Completed,
                                ToolStatus::Failed => ToolCallStatus::Failed,
                            })
                            .content(content);
                        if !entry.tool_diffstats.is_empty() || !entry.tool_locations.is_empty() {
                            call = call.raw_output(serde_json::json!({
                                "legacyDiffstats": entry.tool_diffstats,
                                "legacyLocations": entry.tool_locations,
                            }));
                        }
                        TranscriptBody::Tool {
                            call: serde_json::to_value(call)
                                .expect("ACP tool call serialization cannot fail"),
                            terminal_outputs: Vec::new(),
                            terminal_refs: Vec::new(),
                        }
                    }
                    ChatRole::Plan => TranscriptBody::Plan {
                        plan: serde_json::to_value(Plan::new(
                            entry
                                .plan
                                .iter()
                                .map(|line| {
                                    PlanEntry::new(
                                        line.text.clone(),
                                        PlanEntryPriority::Medium,
                                        match line.status {
                                            PlanStatus::Pending => PlanEntryStatus::Pending,
                                            PlanStatus::Running => PlanEntryStatus::InProgress,
                                            PlanStatus::Completed => PlanEntryStatus::Completed,
                                        },
                                    )
                                })
                                .collect(),
                        ))
                        .expect("ACP plan serialization cannot fail"),
                    },
                    ChatRole::System => TranscriptBody::System {
                        text: entry.text.clone(),
                    },
                };
                let timestamp = entry.recorded_at_ms.unwrap_or_default();
                Arc::new(TranscriptItem {
                    stable_id,
                    position: entry.start_seq,
                    latest_content_event_ordinal: (entry.role == ChatRole::Agent)
                        .then_some(entry.seq),
                    created_at_ms: timestamp,
                    last_changed_at_ms: timestamp,
                    body,
                })
            })
            .collect::<Vec<_>>();
        let started_at_ms = self
            .entries
            .iter()
            .rev()
            .find(|entry| entry.role == ChatRole::User)
            .and_then(|entry| entry.recorded_at_ms)
            .unwrap_or_default();
        let mut configuration = BTreeMap::new();
        if let Some(model) = &self.current_model {
            configuration.insert("model".into(), serde_json::Value::String(model.clone()));
        }
        if let Some(effort) = &self.current_effort {
            configuration.insert("effort".into(), serde_json::Value::String(effort.clone()));
        }
        let applied_event_digest = if self.latest_seq == 0 {
            RELAY_EVENT_GENESIS_DIGEST.to_owned()
        } else {
            let mut digest = Sha256::new();
            digest.update(b"hel-imported-transcript-frontier-v1\0");
            digest.update(self.session_id.as_bytes());
            digest.update(self.latest_seq.to_le_bytes());
            format!("{:x}", digest.finalize())
        };
        MaterializedSession {
            session_id: self.session_id.clone(),
            applied_event_ordinal: self.latest_seq,
            applied_event_digest,
            last_activity_at_ms: self
                .entries
                .iter()
                .filter_map(|entry| entry.recorded_at_ms)
                .max(),
            execution: match self.phase {
                WorkerPhase::Idle => MaterializedExecutionState::Idle,
                WorkerPhase::Running => MaterializedExecutionState::Running { started_at_ms },
                WorkerPhase::Closing => MaterializedExecutionState::Closing,
                WorkerPhase::Closed => MaterializedExecutionState::Closed,
            },
            session_title: None,
            configuration,
            transcript,
            queued_prompts: self
                .queued_prompts
                .iter()
                .map(|prompt| MaterializedQueuedPrompt {
                    command_id: prompt.id.clone(),
                    kind: prompt.kind.clone(),
                    content: vec![serde_json::json!({
                        "type": "text",
                        "text": prompt.text,
                    })],
                    queued_at_ms: 0,
                })
                .collect(),
            pending_elicitations: self
                .elicitation
                .as_ref()
                .map(|dialog| vec![dialog.request().clone()])
                .unwrap_or_default(),
        }
    }

    pub fn queued_prompt_snapshot(&self) -> Vec<crate::hel_worker::QueuedPrompt> {
        self.queued_prompts
            .iter()
            .map(|prompt| crate::hel_worker::QueuedPrompt {
                id: prompt.id.clone(),
                text: prompt.text.clone(),
                attachments: Vec::new(),
                created_at_ms: 0,
            })
            .collect()
    }

    pub fn set_notice(&mut self, notice: impl Into<String>) {
        self.notices.set(notice);
    }

    /// The current shared notice, if any.
    pub fn notice(&self) -> Option<String> {
        self.notices.current()
    }

    pub fn apply_events(&mut self, events: &[SequencedEvent]) {
        for event in events {
            if event.seq <= self.latest_seq {
                continue;
            }
            self.apply_event(event);
            self.latest_seq = event.seq;
        }
    }

    fn reset_interaction(&mut self) {
        self.prompt_history.clear();
        self.history_index = None;
        self.history_draft.clear();
        self.preferred_column = None;
        self.history_search = None;
        self.queued_prompts.clear();
        self.autocomplete = None;
        self.anchor = TranscriptAnchor::Bottom;
        self.last_viewport_height = 0;
        self.render_mode = TranscriptRenderMode::Rich;
        self.notices.clear();
        self.voice_active = false;
        self.focus = ChatFocus::Prompt;
        self.conversations_window_start = None;
    }

    fn set_input(&mut self, input: String) {
        self.input = input;
        self.input_cursor = self.input.len();
        self.history_index = None;
        self.preferred_column = None;
        self.update_autocomplete();
    }

    fn clear_input(&mut self) {
        self.set_input(String::new());
    }

    /// Reinstate the input saved when the user last detached, leaving the
    /// cursor at the end. An empty draft leaves the composer alone.
    fn restore_draft(&mut self, draft: String) {
        if draft.is_empty() {
            return;
        }
        self.set_input(draft);
    }

    fn edit_latest_queued_prompt(&mut self) -> ChatAction {
        let Some(queued) = self.queued_prompts.pop_back() else {
            return ChatAction::None;
        };
        self.set_input(queued.text.clone());
        self.set_notice(if queued.kind.is_prompt() {
            "Editing the most recently queued prompt"
        } else {
            "Editing the most recently queued configuration change"
        });
        ChatAction::RemoveQueuedPrompt {
            id: queued.id,
            text: queued.text,
            kind: queued.kind,
        }
    }

    fn submit_input(&mut self) -> ChatAction {
        let prompt = self.input.trim().to_owned();
        if prompt.is_empty() {
            return ChatAction::None;
        }
        if self.plan_command_pending {
            self.set_notice("A plan-mode transition is still in progress");
            return ChatAction::None;
        }
        if let Some(command) = prompt.strip_prefix('!') {
            if command.trim().is_empty() {
                self.set_notice("usage: !<bash command>");
                return ChatAction::None;
            }
            if matches!(self.phase, WorkerPhase::Closing | WorkerPhase::Closed) {
                self.set_notice("The worker is closing; this shell command was not sent");
                return ChatAction::None;
            }
            self.record_prompt_history(&prompt);
            self.clear_input();
            return ChatAction::RunShell(command.to_owned());
        }
        if let Some((command, args)) = parse_local_command(&prompt) {
            return match command {
                LocalCommand::Help => {
                    self.clear_input();
                    self.show_help();
                    ChatAction::None
                }
                LocalCommand::Detach => {
                    self.clear_input();
                    ChatAction::Back
                }
                LocalCommand::Model | LocalCommand::Effort => {
                    let key = if command == LocalCommand::Model {
                        "model"
                    } else {
                        "effort"
                    };
                    if args.is_empty() {
                        self.set_notice(format!("usage: /{key} <value>"));
                        return ChatAction::None;
                    }
                    if matches!(self.phase, WorkerPhase::Closing | WorkerPhase::Closed) {
                        self.set_notice(
                            "The worker is closing; this configuration change was not sent",
                        );
                        return ChatAction::None;
                    }
                    // A busy agent does not refuse the change: it waits in the
                    // command queue and applies when its turn comes.
                    self.clear_input();
                    ChatAction::SetConfig {
                        key: key.to_owned(),
                        value: args.to_owned(),
                    }
                }
                LocalCommand::Plan => {
                    let (requested, followup) = match args.to_ascii_lowercase().as_str() {
                        "" => (!self.plan_mode_active(), None),
                        "on" => (true, None),
                        "off" => (false, None),
                        _ => (true, Some(args.to_owned())),
                    };
                    if self.phase != WorkerPhase::Idle {
                        self.set_notice("/plan is only available while the agent is idle");
                        return ChatAction::None;
                    }
                    if requested && self.plan_mode_active() {
                        if let Some(followup) = followup {
                            return self.submit_prompt_with_history(followup, prompt);
                        }
                        self.record_prompt_history(&prompt);
                        self.clear_input();
                        self.set_notice("Plan mode is already on");
                        return ChatAction::None;
                    }
                    if !requested && !self.plan_mode_active() && args.eq_ignore_ascii_case("off") {
                        self.record_prompt_history(&prompt);
                        self.clear_input();
                        self.set_notice("Plan mode is already off");
                        return ChatAction::None;
                    }
                    let control = match self.plan_control(requested) {
                        Ok(control) => control,
                        Err(message) => {
                            self.set_notice(message);
                            return ChatAction::None;
                        }
                    };
                    self.record_prompt_history(&prompt);
                    self.clear_input();
                    self.current_mode = Some(if requested { "plan" } else { "default" }.into());
                    self.plan_command_pending = true;
                    self.set_notice(if requested {
                        "Plan mode on"
                    } else {
                        "Plan mode off"
                    });
                    ChatAction::PlanCommand {
                        original: prompt,
                        control,
                        requested_active: requested,
                        prompt: followup,
                    }
                }
                LocalCommand::Implement => {
                    if let Err(message) = self.plan_control(false) {
                        self.set_notice(message);
                        return ChatAction::None;
                    }
                    let instruction = if args.is_empty() {
                        "Implement the approved plan.".to_owned()
                    } else {
                        args.to_owned()
                    };
                    if !self.plan_mode_active() {
                        return self.submit_prompt_with_history(instruction, prompt);
                    }
                    if self.phase != WorkerPhase::Idle {
                        self.set_notice("/implement is only available while the agent is idle");
                        return ChatAction::None;
                    }
                    let control = match self.plan_control(false) {
                        Ok(control) => control,
                        Err(message) => {
                            self.set_notice(message);
                            return ChatAction::None;
                        }
                    };
                    self.record_prompt_history(&prompt);
                    self.clear_input();
                    self.current_mode = Some("default".into());
                    self.plan_command_pending = true;
                    ChatAction::PlanCommand {
                        original: prompt,
                        control,
                        requested_active: false,
                        prompt: Some(instruction),
                    }
                }
            };
        }
        self.submit_prompt(prompt)
    }

    fn submit_prompt(&mut self, prompt: String) -> ChatAction {
        self.submit_prompt_with_history(prompt.clone(), prompt)
    }

    fn submit_prompt_with_history(&mut self, prompt: String, history: String) -> ChatAction {
        if matches!(self.phase, WorkerPhase::Closing | WorkerPhase::Closed) {
            self.set_notice("The worker is closing; this prompt was not sent");
            return ChatAction::None;
        }
        self.record_prompt_history(&history);
        self.clear_input();
        ChatAction::Prompt(prompt)
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> ChatAction {
        if key.kind != KeyEventKind::Press && key.kind != KeyEventKind::Repeat {
            return ChatAction::None;
        }
        // Any key breaks a Ctrl-K chain; only the Ctrl-K arm sets it again.
        let chained = std::mem::take(&mut self.chain_kill);
        let (code, modifiers) = normalize_key(key.code, key.modifiers);

        // Leaving the view is never an answer to the agent, so these two come
        // before the elicitation dialog. A pending elicitation is durable
        // projection state: it is rebuilt from `pending_elicitations` the next
        // time the session is opened, so stepping out loses nothing but field
        // text that was typed and not submitted.
        if modifiers.contains(KeyModifiers::CONTROL) && code == KeyCode::Char('g') {
            return ChatAction::Back;
        }
        if modifiers.contains(KeyModifiers::CONTROL) && code == KeyCode::Char('q') {
            return ChatAction::QuitDetach;
        }

        if let Some(dialog) = self.elicitation.as_mut() {
            if code == KeyCode::Char('v')
                && modifiers.intersects(KeyModifiers::CONTROL | KeyModifiers::SUPER)
            {
                return ChatAction::PasteFromClipboard;
            }
            let request = dialog.request().clone();
            if let Some(response) = dialog.handle_key(code, modifiers) {
                self.elicitation = None;
                return ChatAction::RespondElicitation { request, response };
            }
            return ChatAction::None;
        }

        if code == KeyCode::Char('v')
            && modifiers.intersects(KeyModifiers::CONTROL | KeyModifiers::SUPER)
        {
            return ChatAction::PasteFromClipboard;
        }

        if modifiers.contains(KeyModifiers::ALT) && code == KeyCode::Char('v') {
            return ChatAction::ToggleVoice;
        }

        if self.history_search.is_some() {
            self.handle_history_search_key(code, modifiers);
            return ChatAction::None;
        }

        if self.focus == ChatFocus::Conversations {
            return self.handle_conversations_key(code, modifiers);
        }

        if code == KeyCode::Esc {
            return if self.phase == WorkerPhase::Running || !self.active_user_shells.is_empty() {
                ChatAction::Cancel
            } else {
                ChatAction::None
            };
        }
        if modifiers.contains(KeyModifiers::CONTROL) {
            match code {
                KeyCode::Char('r') => {
                    self.begin_history_search();
                    return ChatAction::None;
                }
                KeyCode::Char('t') => {
                    self.toggle_render_mode();
                    return ChatAction::None;
                }
                KeyCode::Char('a') => self.move_to_line_start(true),
                KeyCode::Char('e') => self.move_to_line_end(true),
                KeyCode::Char('b') => self.move_input_cursor(-1),
                KeyCode::Char('f') => self.move_input_cursor(1),
                KeyCode::Char('h') => self.backspace(),
                KeyCode::Char('d') => self.delete(),
                KeyCode::Char('u') => self.kill_to_line_start(),
                KeyCode::Char('k') => {
                    self.kill_to_line_end(chained);
                    self.chain_kill = true;
                }
                KeyCode::Char('w') => {
                    let start = self.previous_word_start();
                    self.kill_range(start..self.input_cursor);
                }
                KeyCode::Char('c') => {
                    // Stash the abandoned prompt so history can recall it.
                    if !self.input.is_empty() {
                        let stashed = std::mem::take(&mut self.input);
                        self.record_prompt_history(&stashed);
                        self.clear_input();
                    }
                }
                KeyCode::Char('y') => self.yank(),
                KeyCode::Char('j') | KeyCode::Char('m') => self.insert_character('\n'),
                KeyCode::Char('p') => {
                    if self.input.is_empty() && !self.queued_prompts.is_empty() {
                        return self.edit_latest_queued_prompt();
                    } else if self.input.is_empty() || self.history_index.is_some() {
                        self.move_history(-1);
                    } else {
                        self.move_vertical(-1);
                    }
                }
                KeyCode::Char('n') => {
                    if self.history_index.is_some() {
                        self.move_history(1);
                    } else {
                        self.move_vertical(1);
                    }
                }
                KeyCode::Left => self.move_word(-1),
                KeyCode::Right => self.move_word(1),
                KeyCode::Backspace => {
                    let start = self.previous_word_start();
                    self.kill_range(start..self.input_cursor);
                }
                KeyCode::Delete => {
                    let end = self.next_word_end();
                    self.kill_range(self.input_cursor..end);
                }
                KeyCode::Home => {
                    self.anchor = TranscriptAnchor::Row { entry: 0, row: 0 };
                }
                KeyCode::End => self.anchor = TranscriptAnchor::Bottom,
                _ => {}
            }
            return ChatAction::None;
        }
        if modifiers.contains(KeyModifiers::ALT) {
            match code {
                KeyCode::Char('b') | KeyCode::Left => self.move_word(-1),
                KeyCode::Char('f') | KeyCode::Right => self.move_word(1),
                KeyCode::Char('d') | KeyCode::Delete => {
                    let end = self.next_word_end();
                    self.kill_range(self.input_cursor..end);
                }
                KeyCode::Backspace => {
                    let start = self.previous_word_start();
                    self.kill_range(start..self.input_cursor);
                }
                KeyCode::Enter => self.insert_character('\n'),
                KeyCode::Up if !self.queued_prompts.is_empty() => {
                    return self.edit_latest_queued_prompt();
                }
                _ => {}
            }
            return ChatAction::None;
        }
        match code {
            KeyCode::Enter if modifiers.contains(KeyModifiers::SHIFT) => {
                self.insert_character('\n');
                ChatAction::None
            }
            KeyCode::Enter => {
                if self.accept_autocomplete() {
                    ChatAction::None
                } else {
                    self.submit_input()
                }
            }
            KeyCode::Backspace => {
                self.backspace();
                ChatAction::None
            }
            KeyCode::Delete => {
                self.delete();
                ChatAction::None
            }
            // Tab completes an open popup first; with none open it is the
            // handle on the conversations pane.
            KeyCode::Tab => {
                if !self.accept_autocomplete() {
                    self.focus_conversations();
                }
                ChatAction::None
            }
            KeyCode::Char(character)
                if !modifiers.intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
            {
                self.insert_character(character);
                ChatAction::None
            }
            KeyCode::Up if self.autocomplete.is_some() => {
                self.move_autocomplete(-1);
                ChatAction::None
            }
            KeyCode::Down if self.autocomplete.is_some() => {
                self.move_autocomplete(1);
                ChatAction::None
            }
            KeyCode::Up => {
                if self.input.is_empty() && !self.queued_prompts.is_empty() {
                    return self.edit_latest_queued_prompt();
                } else if self.input.is_empty() || self.history_index.is_some() {
                    self.move_history(-1);
                } else {
                    self.move_vertical(-1);
                }
                ChatAction::None
            }
            KeyCode::Down => {
                if self.history_index.is_some() {
                    self.move_history(1);
                } else {
                    self.move_vertical(1);
                }
                ChatAction::None
            }
            KeyCode::Left
                if modifiers.contains(KeyModifiers::SHIFT) && !self.queued_prompts.is_empty() =>
            {
                self.edit_latest_queued_prompt()
            }
            KeyCode::Left => {
                self.move_input_cursor(-1);
                ChatAction::None
            }
            KeyCode::Right => {
                self.move_input_cursor(1);
                ChatAction::None
            }
            KeyCode::PageUp => {
                self.scroll_history_up(self.last_viewport_height.max(1));
                ChatAction::None
            }
            KeyCode::PageDown => {
                self.scroll_history_down(self.last_viewport_height.max(1));
                ChatAction::None
            }
            KeyCode::Home => {
                self.move_to_line_start(false);
                ChatAction::None
            }
            KeyCode::End => {
                self.move_to_line_end(false);
                ChatAction::None
            }
            _ => ChatAction::None,
        }
    }

    pub(super) fn set_active_user_shells(&mut self, shells: &[crate::hel_worker::ActiveUserShell]) {
        self.active_user_shells = shells
            .iter()
            .map(|shell| shell.command_id.clone())
            .collect();
    }

    pub(super) fn set_active_agent_terminals(
        &mut self,
        terminals: &[ActiveAgentTerminal],
        session: &MaterializedSession,
    ) {
        self.active_agent_terminals = terminals.to_vec();
        self.claimed_agent_terminals.clear();
        let mut unresolved = terminals
            .iter()
            .map(|terminal| (terminal.terminal_id.as_str(), terminal.started_at_ms))
            .collect::<BTreeMap<_, _>>();
        // Claims are normally on the newest item, so walking backward stops
        // immediately. The full-history path is reserved for the uncommon
        // unclaimed fallback this state exists to cover.
        for item in session.transcript.iter().rev() {
            let TranscriptBody::Tool { terminal_refs, .. } = &item.body else {
                continue;
            };
            for terminal_id in terminal_refs {
                let Some(started_at_ms) = unresolved.get(terminal_id.as_str()) else {
                    continue;
                };
                if item.last_changed_at_ms >= *started_at_ms {
                    self.claimed_agent_terminals
                        .insert(terminal_id.clone(), item.last_changed_at_ms);
                    unresolved.remove(terminal_id.as_str());
                }
            }
            if unresolved.is_empty() {
                break;
            }
        }
    }

    pub(super) fn active_user_shell_ids(&self) -> Vec<String> {
        self.active_user_shells.clone()
    }

    fn toggle_render_mode(&mut self) {
        self.render_mode = self.render_mode.toggled();
        self.notices.set(match self.render_mode {
            TranscriptRenderMode::Rich => "Rich transcript rendering enabled",
            TranscriptRenderMode::Raw => "Raw transcript source enabled",
        });
    }

    pub fn handle_mouse(&mut self, mouse: MouseEvent) -> ChatAction {
        // Hover decides what scrolls; only Tab moves focus.
        let over_conversations = self
            .conversations_area
            .is_some_and(|area| area.contains(Position::new(mouse.column, mouse.row)));
        match (mouse.kind, over_conversations) {
            (MouseEventKind::ScrollUp, true) => self.scroll_conversations(-1),
            (MouseEventKind::ScrollDown, true) => self.scroll_conversations(1),
            (MouseEventKind::ScrollUp, false) => self.scroll_history_up(MOUSE_SCROLL_ROWS),
            (MouseEventKind::ScrollDown, false) => self.scroll_history_down(MOUSE_SCROLL_ROWS),
            (MouseEventKind::Down(MouseButton::Left), true) => {
                return self.click_conversation_row(mouse);
            }
            _ => {}
        }
        ChatAction::None
    }

    fn apply_event(&mut self, event: &SequencedEvent) {
        match &event.event {
            WorkerEvent::PromptAccepted { text, .. } => {
                self.mark_prompt_submitted(text);
                self.start_turn_clock(event.recorded_at_ms);
                self.entries.push(
                    ChatEntry::plain(event.seq, ChatRole::User, text)
                        .with_recorded_at(event.recorded_at_ms),
                );
            }
            WorkerEvent::TurnCompleted => {
                self.phase = WorkerPhase::Idle;
                self.goal_prompt_active = false;
                self.turn_started_at_epoch_seconds = None;
            }
            // The durable worker records cancellation acceptance before the
            // ACP prompt future resolves. Keep the chat busy until the later
            // TurnCompleted event so a queued prompt cannot race the runtime.
            WorkerEvent::Cancelled => {
                self.phase = WorkerPhase::Running;
            }
            WorkerEvent::Closing => self.phase = WorkerPhase::Closing,
            WorkerEvent::Closed => self.phase = WorkerPhase::Closed,
            WorkerEvent::Checkpointed { .. } => {}
            WorkerEvent::Adapter { payload, .. } => {
                if is_compaction_artifact(payload) {
                    self.last_compaction_seq = event.seq;
                }
                self.apply_adapter(event.seq, event.recorded_at_ms, payload)
            }
            WorkerEvent::QueuedPromptAdded { prompt } => {
                self.queued_prompts.push_back(QueuedPrompt {
                    id: prompt.id.clone(),
                    text: prompt.text.clone(),
                    kind: QueuedCommandKind::Prompt,
                });
            }
            WorkerEvent::QueuedPromptRemoved { queue_id } => {
                self.queued_prompts.retain(|prompt| prompt.id != *queue_id);
            }
            WorkerEvent::QueuedPromptPromoted { prompt, .. } => {
                self.queued_prompts.retain(|queued| queued.id != prompt.id);
                self.phase = WorkerPhase::Running;
                self.start_turn_clock(event.recorded_at_ms);
                self.entries.push(
                    ChatEntry::plain(event.seq, ChatRole::User, &prompt.text)
                        .with_recorded_at(event.recorded_at_ms),
                );
            }
            WorkerEvent::QueuedPromptsCleared => self.queued_prompts.clear(),
            WorkerEvent::ConfigChanged { .. } => {}
        }
    }

    fn apply_adapter(
        &mut self,
        seq: u64,
        recorded_at_ms: Option<i64>,
        payload: &serde_json::Value,
    ) {
        let Ok(runtime) = serde_json::from_value::<RuntimeEvent>(payload.clone()) else {
            return;
        };
        match runtime {
            RuntimeEvent::SessionUpdate { update } => {
                self.apply_session_update_at(seq, recorded_at_ms, &update)
            }
            RuntimeEvent::Warning { message } => self.entries.push(ChatEntry::plain(
                seq,
                ChatRole::System,
                format!("warning: {message}"),
            )),
            RuntimeEvent::ConfigApplied { key, value, .. } => self.entries.push(ChatEntry::plain(
                seq,
                ChatRole::System,
                format!("{key} set to {value}"),
            )),
            RuntimeEvent::SessionConfigured { config_options } => {
                self.set_config_options(&config_options)
            }
            RuntimeEvent::SessionModesConfigured { modes } => self.set_session_modes(modes),
            RuntimeEvent::SessionStarted { resumed, .. } => self.entries.push(ChatEntry::plain(
                seq,
                ChatRole::System,
                if resumed {
                    "harness session resumed"
                } else {
                    "harness session started"
                },
            )),
            _ => {}
        }
    }

    /// Project one typed ACP update into stable transcript items. The runtime
    /// keeps JSON at the persistence boundary so old event logs remain wire
    /// compatible; rendering never guesses at arbitrary JSON shapes.
    #[cfg(test)]
    fn apply_session_update(&mut self, seq: u64, update: &serde_json::Value) {
        self.apply_session_update_at(seq, None, update);
    }

    fn apply_session_update_at(
        &mut self,
        seq: u64,
        recorded_at_ms: Option<i64>,
        update: &serde_json::Value,
    ) {
        let parsed = match serde_json::from_value::<SessionUpdate>(update.clone()) {
            Ok(parsed) => parsed,
            Err(error) => {
                tracing::debug!(%error, "ignoring invalid ACP session update");
                return;
            }
        };
        match parsed {
            SessionUpdate::AgentMessageChunk(chunk) => {
                let message_id = chunk.message_id.map(|id| id.to_string());
                if let Some(text) = content_block_text(&chunk.content) {
                    self.push_streamed(seq, recorded_at_ms, ChatRole::Agent, message_id, &text);
                }
            }
            SessionUpdate::AgentThoughtChunk(chunk) => {
                let message_id = chunk.message_id.map(|id| id.to_string());
                if let Some(text) = content_block_text(&chunk.content) {
                    self.push_streamed(seq, recorded_at_ms, ChatRole::Thought, message_id, &text);
                }
            }
            // PromptAccepted is the canonical local user-message event. ACP
            // user chunks would duplicate it during replay.
            SessionUpdate::UserMessageChunk(_) => {}
            SessionUpdate::ToolCall(call) => {
                let mut entry = ChatEntry::tool(
                    seq,
                    call.title,
                    Some(call.tool_call_id.to_string()),
                    tool_status(&call.status),
                );
                entry.tool_content =
                    tool_content_details(&call.content, &[], call.raw_output.as_ref());
                entry.tool_diffstats = tool_diff_paths(&call.content);
                entry.tool_locations = tool_location_details(&call.locations);
                self.entries.push(entry);
            }
            SessionUpdate::ToolCallUpdate(update) => {
                let tool_call_id = update.tool_call_id.to_string();
                let Some(entry) = self.entries.iter_mut().rev().find(|entry| {
                    entry.role == ChatRole::Tool
                        && entry.tool_call_id.as_deref() == Some(tool_call_id.as_str())
                }) else {
                    return;
                };
                entry.touch(seq);
                if let Some(title) = update.fields.title {
                    entry.text = sanitize_terminal_text(&title);
                }
                if let Some(status) = update.fields.status {
                    entry.tool_status = Some(tool_status(&status));
                }
                if let Some(content) = update.fields.content {
                    entry.tool_content =
                        tool_content_details(&content, &[], update.fields.raw_output.as_ref());
                    entry.tool_diffstats = tool_diff_paths(&content);
                }
                if let Some(locations) = update.fields.locations {
                    entry.tool_locations = tool_location_details(&locations);
                }
            }
            SessionUpdate::Plan(plan) => {
                let lines = plan
                    .entries
                    .into_iter()
                    .map(|entry| PlanLine {
                        text: sanitize_terminal_text(&entry.content),
                        status: plan_status(&entry.status),
                    })
                    .collect();
                let latest_user_seq = self
                    .entries
                    .iter()
                    .rev()
                    .find(|entry| entry.role == ChatRole::User)
                    .map_or(0, |entry| entry.seq);
                if let Some(entry) = self
                    .entries
                    .iter_mut()
                    .rev()
                    .find(|entry| entry.role == ChatRole::Plan && entry.seq > latest_user_seq)
                {
                    entry.touch(seq);
                    entry.plan = lines;
                } else {
                    self.entries.push(ChatEntry::plan(seq, lines));
                }
            }
            SessionUpdate::AvailableCommandsUpdate(update) => {
                self.agent_commands = update.available_commands;
                self.rebuild_command_choices();
            }
            SessionUpdate::ConfigOptionUpdate(update) => {
                self.set_config_options(&update.config_options);
            }
            SessionUpdate::CurrentModeUpdate(update)
                if self.harness_kind != Some(HarnessKind::Codex) =>
            {
                self.current_mode = Some(update.current_mode_id.to_string());
                if let Some(modes) = self.session_modes.as_mut() {
                    modes.current_mode_id = update.current_mode_id;
                }
            }
            _ => {}
        }
    }

    fn push_streamed(
        &mut self,
        seq: u64,
        recorded_at_ms: Option<i64>,
        role: ChatRole,
        message_id: Option<String>,
        text: &str,
    ) {
        let text = sanitize_terminal_text(text);
        if let Some(last) = self.entries.last_mut()
            && last.role == role
            && (role == ChatRole::Thought || last.message_id == message_id)
        {
            last.touch(seq);
            if role == ChatRole::Thought
                && last.message_id != message_id
                && !last.text.is_empty()
                && !text.is_empty()
            {
                while last.text.ends_with('\n') {
                    last.text.pop();
                }
                last.text.push('\n');
                last.text.push_str(text.trim_start_matches('\n'));
            } else {
                last.text.push_str(&text);
            }
            return;
        }
        let mut entry = ChatEntry::plain(seq, role, text).with_recorded_at(recorded_at_ms);
        entry.message_id = message_id;
        self.entries.push(entry);
    }
}

fn is_compaction_artifact(payload: &serde_json::Value) -> bool {
    let update = payload.get("update").unwrap_or(payload);
    matches!(
        update
            .get("sessionUpdate")
            .and_then(serde_json::Value::as_str),
        Some("compaction" | "context_compaction" | "compaction_summary")
    ) || update.get("encrypted_content").is_some()
        || update.get("encryptedContent").is_some()
}

fn normalize_key(code: KeyCode, mut modifiers: KeyModifiers) -> (KeyCode, KeyModifiers) {
    let KeyCode::Char(character) = code else {
        return (code, modifiers);
    };
    if modifiers.is_empty() {
        let value = u32::from(character);
        if (1..=26).contains(&value)
            && let Some(control) = char::from_u32(value - 1 + u32::from('a'))
        {
            modifiers.insert(KeyModifiers::CONTROL);
            return (KeyCode::Char(control), modifiers);
        }
    }
    if character.is_ascii_uppercase() {
        modifiers.insert(KeyModifiers::SHIFT);
        if modifiers.intersects(KeyModifiers::CONTROL | KeyModifiers::ALT | KeyModifiers::SUPER) {
            return (KeyCode::Char(character.to_ascii_lowercase()), modifiers);
        }
    }
    (code, modifiers)
}

fn queued_prompt_preview(prompt: &str) -> String {
    const WIDTH: usize = 72;
    let collapsed = prompt.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.chars().count() <= WIDTH {
        return collapsed;
    }
    let mut preview = collapsed.chars().take(WIDTH - 1).collect::<String>();
    preview.push('…');
    preview
}

/// How long a notice is guaranteed on screen before an unrelated key press
/// may dismiss it. Background failures report through this bar and nowhere
/// else, so a keystroke that races one must not wipe it unread.
pub const NOTICE_MINIMUM_DISPLAY: std::time::Duration = std::time::Duration::from_secs(4);

#[derive(Debug)]
struct Notice {
    text: String,
    set_at: std::time::Instant,
    protected: bool,
}

#[derive(Debug, Default)]
struct NoticeSlot {
    notice: Option<Notice>,
    /// Bumped on every write, so a dirty-gated renderer can tell that the bar
    /// moved without keeping a copy of its text.
    generation: u64,
}

impl NoticeSlot {
    fn write(&mut self, notice: Option<Notice>) {
        self.notice = notice;
        self.generation = self.generation.wrapping_add(1);
    }
}

/// The one-line notifications bar shared by every view. Cloning shares the
/// same underlying slot; the latest notice wins and a clear in one view
/// clears it for all.
///
/// Each notice carries the time it was set. That is what lets an incidental
/// key press dismiss a notice the user has had a chance to read while leaving
/// a fresh one standing.
#[derive(Debug, Clone, Default)]
pub struct Notices(std::sync::Arc<std::sync::Mutex<NoticeSlot>>);

impl Notices {
    /// Sets the notice, replacing whatever is showing. Sanitizes the text so
    /// escape sequences or stray carriage returns from background work
    /// cannot corrupt the footer row.
    pub fn set(&self, notice: impl Into<String>) {
        let text = sanitize_terminal_text(&notice.into());
        let mut slot = self.lock();
        if slot.notice.as_ref().is_some_and(|current| {
            current.protected && current.set_at.elapsed() < NOTICE_MINIMUM_DISPLAY
        }) {
            return;
        }
        slot.write(Some(Notice {
            text,
            set_at: std::time::Instant::now(),
            protected: false,
        }));
    }

    /// Sets a failure notice that routine background updates cannot replace
    /// before it has been readable for [`NOTICE_MINIMUM_DISPLAY`]. A newer
    /// failure still replaces it immediately.
    pub fn set_failure(&self, notice: impl Into<String>) {
        let text = sanitize_terminal_text(&notice.into());
        self.lock().write(Some(Notice {
            text,
            set_at: std::time::Instant::now(),
            protected: true,
        }));
    }

    /// Replaces the notice only if it still reads `expected`, so a
    /// background task can upgrade its own "in progress" notice to a result
    /// without clobbering whatever replaced it in the meantime. Returns
    /// whether the replacement happened. The replacement is a new report, so
    /// it starts its own display period.
    pub fn replace_if(&self, expected: &str, replacement: impl Into<String>) -> bool {
        let mut slot = self.lock();
        if slot.notice.as_ref().map(|notice| notice.text.as_str()) != Some(expected) {
            return false;
        }
        let text = sanitize_terminal_text(&replacement.into());
        slot.write(Some(Notice {
            text,
            set_at: std::time::Instant::now(),
            protected: false,
        }));
        true
    }

    /// Clears the notice everywhere it is shown, however recent it is. This
    /// is for callers that know the notice no longer applies; a key press
    /// that merely happened to arrive uses [`Notices::dismiss`].
    pub fn clear(&self) {
        let mut slot = self.lock();
        if slot.notice.is_some() {
            slot.write(None);
        }
    }

    /// Clears the notice if it has been showing for at least
    /// [`NOTICE_MINIMUM_DISPLAY`] at `now`. Returns whether the bar is clear
    /// afterwards, so a caller can tell a survivor from a dismissal.
    pub fn dismiss(&self, now: std::time::Instant) -> bool {
        let mut slot = self.lock();
        match slot.notice.as_ref() {
            None => true,
            Some(notice) => {
                if now.saturating_duration_since(notice.set_at) < NOTICE_MINIMUM_DISPLAY {
                    return false;
                }
                slot.write(None);
                true
            }
        }
    }

    /// The current notice, if any.
    pub fn current(&self) -> Option<String> {
        self.lock()
            .notice
            .as_ref()
            .map(|notice| notice.text.clone())
    }

    /// Counts writes to the shared slot. A renderer that records this with
    /// each frame can tell that the bar changed since it last drew, which is
    /// what keeps a notice set by background work from being missed by a
    /// dirty-gated draw.
    pub fn generation(&self) -> u64 {
        self.lock().generation
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, NoticeSlot> {
        self.0.lock().expect("notices lock poisoned")
    }
}

/// Color of an active session's line. The terminal palette's plain yellow
/// (amber or orange in common palettes) marks a session whose turn is still
/// running; a session with no turn in flight is waiting on the user and
/// switches to the brighter light yellow.
pub fn turn_band_color(turn_in_flight: bool) -> Color {
    if turn_in_flight {
        Color::Yellow
    } else {
        Color::LightYellow
    }
}

/// When the session's current turn started, in epoch seconds. `None` means no
/// turn is in flight.
fn turn_started_at_epoch_seconds(execution: MaterializedExecutionState) -> Option<u64> {
    match execution {
        MaterializedExecutionState::Running { started_at_ms } => {
            u64::try_from(started_at_ms).ok().map(|value| value / 1_000)
        }
        MaterializedExecutionState::Idle
        | MaterializedExecutionState::Closing
        | MaterializedExecutionState::Closed => None,
    }
}

/// Last line of a message that has any text on it, trimmed. `None` means the
/// message is blank.
fn last_nonempty_line(text: &str) -> Option<String> {
    text.lines()
        .map(str::trim)
        .rfind(|line| !line.is_empty())
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hel_chat::test_support::{
        advertise, ctrl, grok_chat, key, mode_config_option, queued, select_config_option, snapshot,
    };
    use crate::hel_worker::ActivePrompt;

    /// Mirrors what `ActiveChat::open` does for a session with no warm view:
    /// build the state from the snapshot, then seed the saved draft.
    fn freshly_opened_chat(saved_draft: &str) -> ChatState {
        let mut chat =
            ChatState::from_materialized(&MaterializedSession::empty("session-fresh"), &[], &[]);
        chat.set_history_context("bundle-1");
        chat.restore_draft(saved_draft.to_owned());
        chat
    }

    #[test]
    fn alt_v_toggles_voice_without_editing_prompt() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        let action = chat.handle_key(KeyEvent::new(KeyCode::Char('v'), KeyModifiers::ALT));
        assert_eq!(action, ChatAction::ToggleVoice);
        assert!(chat.input.is_empty());
    }

    #[test]
    fn a_saved_draft_reopens_in_the_composer_with_the_cursor_at_its_end() {
        let mut chat = ChatState::new(&snapshot(), &[]);

        chat.restore_draft("half typed thought".into());

        assert_eq!(chat.input, "half typed thought");
        assert_eq!(chat.input_cursor, "half typed thought".len());
    }

    #[test]
    fn an_empty_saved_draft_leaves_the_composer_untouched() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.set_input("typed since opening".into());

        chat.restore_draft(String::new());

        assert_eq!(chat.input, "typed since opening");
    }

    #[test]
    fn a_fresh_chat_opens_with_the_session_s_saved_draft_in_the_composer() {
        let chat = freshly_opened_chat("half typed thought");

        assert_eq!(chat.input, "half typed thought");
        assert_eq!(chat.input_cursor, "half typed thought".len());
    }

    #[test]
    fn a_fresh_chat_for_a_session_with_no_saved_draft_opens_empty() {
        assert_eq!(freshly_opened_chat("").input, "");
    }

    #[test]
    fn control_g_detaches_without_emitting_close() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        let control_g = KeyEvent::new(KeyCode::Char('g'), KeyModifiers::CONTROL);
        assert_eq!(chat.handle_key(control_g), ChatAction::Back);
    }

    #[test]
    fn control_q_quits_from_conversation() {
        let mut chat = ChatState::new(&snapshot(), &[]);

        assert_eq!(chat.handle_key(ctrl('q')), ChatAction::QuitDetach);
    }

    #[test]
    fn enter_submits_to_the_worker_while_idle_or_running() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.handle_key(key(KeyCode::Char('h')));
        chat.handle_key(key(KeyCode::Char('i')));
        assert_eq!(
            chat.handle_key(key(KeyCode::Enter)),
            ChatAction::Prompt("hi".into())
        );

        let mut running = snapshot();
        running.phase = WorkerPhase::Running;
        running.active_prompt = Some(ActivePrompt {
            request_id: "p".into(),
            text: "busy".into(),
            attachments: vec![],
        });
        let mut chat = ChatState::new(&running, &[]);
        chat.handle_key(key(KeyCode::Char('x')));
        assert_eq!(
            chat.handle_key(key(KeyCode::Enter)),
            ChatAction::Prompt("x".into())
        );
        assert!(chat.queued_prompts.is_empty());
        assert!(chat.entries.is_empty());
    }

    #[test]
    fn bang_prefix_submits_a_bash_command_without_starting_a_prompt() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.set_input("!printf '%s' hello | tr a-z A-Z".into());

        assert_eq!(
            chat.handle_key(key(KeyCode::Enter)),
            ChatAction::RunShell("printf '%s' hello | tr a-z A-Z".into())
        );
        assert!(chat.input.is_empty());
        assert_eq!(
            chat.prompt_history.last().map(String::as_str),
            Some("!printf '%s' hello | tr a-z A-Z")
        );
    }

    #[test]
    fn empty_bang_command_stays_in_the_composer_and_shows_usage() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.set_input("!   ".into());

        assert_eq!(chat.handle_key(key(KeyCode::Enter)), ChatAction::None);
        assert_eq!(chat.input, "!   ");
        assert_eq!(chat.notice().as_deref(), Some("usage: !<bash command>"));
    }

    #[test]
    fn enter_does_not_send_a_prompt_while_the_worker_is_closing_or_closed() {
        for phase in [WorkerPhase::Closing, WorkerPhase::Closed] {
            let mut chat = ChatState::new(&snapshot(), &[]);
            chat.phase = phase;
            chat.input = "hello".into();
            assert_eq!(chat.handle_key(key(KeyCode::Enter)), ChatAction::None);
            assert_eq!(
                chat.notices.current().as_deref(),
                Some("The worker is closing; this prompt was not sent")
            );
            assert_eq!(chat.input, "hello");
        }
    }

    #[test]
    fn bootstrap_uses_snapshot_queue_without_duplicating_replayed_additions() {
        let worker: WorkerSnapshot = serde_json::from_value(serde_json::json!({
            "session_id": "1234567890",
            "phase": "running",
            "latest_seq": 1,
            "last_checkpoint_seq": null,
            "active_prompt": null,
            "config": {},
            "queued_prompts": [{
                "id": "queued-0001",
                "text": "next",
                "attachments": [],
                "created_at_ms": 1
            }],
            "handled_requests": {}
        }))
        .unwrap();
        let events = [SequencedEvent {
            seq: 1,
            recorded_at_ms: Some(1),
            request_id: Some("enqueue-1".into()),
            event: WorkerEvent::QueuedPromptAdded {
                prompt: crate::hel_worker::QueuedPrompt {
                    id: "queued-0001".into(),
                    text: "next".into(),
                    attachments: vec![],
                    created_at_ms: 1,
                },
            },
        }];

        let chat = ChatState::new(&worker, &events);

        assert_eq!(chat.queued_prompts.len(), 1);
        assert_eq!(chat.queued_prompts[0].id, "queued-0001");
    }

    #[test]
    fn submitting_a_prompt_clears_a_stale_queue_notice() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.set_notice("Queued 1: next");

        chat.mark_prompt_submitted("hello");

        assert_eq!(chat.phase, WorkerPhase::Running);
        assert!(chat.notice().is_none());
    }

    #[test]
    fn notices_set_replace_if_and_clear() {
        let notices = Notices::default();
        assert_eq!(notices.current(), None);

        notices.set("first notice");
        assert_eq!(notices.current().as_deref(), Some("first notice"));

        assert!(!notices.replace_if("wrong expectation", "replaced"));
        assert_eq!(notices.current().as_deref(), Some("first notice"));

        assert!(notices.replace_if("first notice", "second notice"));
        assert_eq!(notices.current().as_deref(), Some("second notice"));

        notices.clear();
        assert_eq!(notices.current(), None);
    }

    #[test]
    fn a_fresh_failure_notice_survives_routine_background_notices() {
        let notices = Notices::default();
        notices.set_failure("Resume failed: archived transcript is invalid");

        notices.set("Profile quotas refreshed");
        assert_eq!(
            notices.current().as_deref(),
            Some("Resume failed: archived transcript is invalid")
        );

        notices.set_failure("Resume failed: target disconnected");
        assert_eq!(
            notices.current().as_deref(),
            Some("Resume failed: target disconnected")
        );

        let after_set = std::time::Instant::now();
        assert!(notices.dismiss(after_set + NOTICE_MINIMUM_DISPLAY));
        notices.set("Profile quotas refreshed");
        assert_eq!(
            notices.current().as_deref(),
            Some("Profile quotas refreshed")
        );
    }

    #[test]
    fn cloned_notices_share_one_slot() {
        let notices = Notices::default();
        let clone = notices.clone();

        notices.set("set through the original");
        assert_eq!(clone.current().as_deref(), Some("set through the original"));

        clone.clear();
        assert_eq!(notices.current(), None);
    }

    /// Dismissal is what an incidental key press asks for, and a notice that
    /// nobody has had time to read must survive it.
    #[test]
    fn a_notice_is_dismissed_only_once_it_has_been_showing_long_enough() {
        let notices = Notices::default();
        assert!(notices.dismiss(std::time::Instant::now()));

        notices.set("Credential sync failed");
        let after_set = std::time::Instant::now();
        assert!(!notices.dismiss(after_set));
        assert_eq!(notices.current().as_deref(), Some("Credential sync failed"));

        assert!(notices.dismiss(after_set + NOTICE_MINIMUM_DISPLAY));
        assert_eq!(notices.current(), None);
    }

    /// Draws are gated on a dirty flag that background work never sets, so a
    /// renderer tells the bar moved by recording this counter with each frame.
    #[test]
    fn every_write_to_the_notice_slot_bumps_its_generation() {
        let notices = Notices::default();
        let drawn = notices.generation();

        notices.set("Import failed");
        assert_ne!(notices.generation(), drawn);
        let drawn = notices.generation();

        assert!(notices.replace_if("Import failed", "Import failed: no space left"));
        assert_ne!(notices.generation(), drawn);
        let drawn = notices.generation();

        notices.clear();
        assert_ne!(notices.generation(), drawn);

        // Clearing an empty bar changes nothing on screen.
        let drawn = notices.generation();
        notices.clear();
        assert_eq!(notices.generation(), drawn);
    }

    fn text_elicitation() -> ElicitationRequest {
        ElicitationRequest {
            id: "ask-1".into(),
            message: "Which branch should I use?".into(),
            title: None,
            description: None,
            fields: vec![crate::hel_elicitation::ElicitationField {
                id: "branch".into(),
                title: "Branch".into(),
                description: None,
                required: false,
                secret: false,
                custom_answer_for: None,
                kind: crate::hel_elicitation::ElicitationFieldKind::Text {
                    default: None,
                    min_length: None,
                    max_length: None,
                    pattern: None,
                    format: None,
                },
            }],
        }
    }

    /// A pending elicitation is durable projection state, rebuilt from the
    /// session the next time it is opened, so leaving the view is a different
    /// act from answering the agent.
    #[test]
    fn control_g_and_control_q_leave_a_chat_whose_elicitation_is_still_open() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        let request = text_elicitation();
        chat.restore_elicitation(request.clone());

        assert_eq!(chat.handle_key(ctrl('g')), ChatAction::Back);
        assert_eq!(chat.handle_key(ctrl('q')), ChatAction::QuitDetach);
        assert_eq!(
            chat.materialized_session().pending_elicitations,
            vec![request.clone()]
        );

        // Every other key still belongs to the form, and Escape still answers
        // the agent rather than leaving.
        assert_eq!(chat.handle_key(key(KeyCode::Char('q'))), ChatAction::None);
        assert_eq!(
            chat.handle_key(key(KeyCode::Esc)),
            ChatAction::RespondElicitation {
                request,
                response: ElicitationResponse::Cancel,
            }
        );
        assert!(chat.materialized_session().pending_elicitations.is_empty());
    }

    #[test]
    fn escape_only_cancels_an_active_turn_and_control_g_detaches() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        let control_c = KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL);
        let control_g = KeyEvent::new(KeyCode::Char('g'), KeyModifiers::CONTROL);
        assert_eq!(chat.handle_key(control_c), ChatAction::None);
        assert_eq!(chat.handle_key(key(KeyCode::Esc)), ChatAction::None);
        assert_eq!(chat.handle_key(control_g), ChatAction::Back);

        chat.phase = WorkerPhase::Running;
        assert_eq!(chat.handle_key(key(KeyCode::Esc)), ChatAction::Cancel);
        assert_eq!(chat.handle_key(control_c), ChatAction::None);
    }

    #[test]
    fn cancellation_waits_for_turn_completion_before_queue_can_drain() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.phase = WorkerPhase::Running;
        chat.queued_prompts.push_back(queued("queued-1", "next"));
        chat.apply_event(&SequencedEvent {
            seq: 1,
            recorded_at_ms: None,
            request_id: Some("cancel".into()),
            event: WorkerEvent::Cancelled,
        });
        assert_eq!(chat.phase, WorkerPhase::Running);

        chat.apply_event(&SequencedEvent {
            seq: 2,
            recorded_at_ms: None,
            request_id: None,
            event: WorkerEvent::TurnCompleted,
        });
        assert_eq!(chat.phase, WorkerPhase::Idle);
        assert_eq!(chat.queued_prompts.front().unwrap().text, "next");
    }

    #[test]
    fn alt_up_recovers_the_latest_queued_prompt_for_editing() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.queued_prompts.push_back(queued("queued-1", "first"));
        chat.queued_prompts.push_back(queued("queued-2", "second"));

        assert_eq!(
            chat.handle_key(KeyEvent::new(KeyCode::Up, KeyModifiers::ALT)),
            ChatAction::RemoveQueuedPrompt {
                id: "queued-2".into(),
                text: "second".into(),
                kind: QueuedCommandKind::Prompt,
            }
        );

        assert_eq!(chat.input, "second");
        assert_eq!(chat.queued_prompts.len(), 1);
        assert_eq!(chat.queued_prompts[0].text, "first");
    }

    #[test]
    fn up_and_control_p_peel_queued_prompts_back_into_the_editor() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        for (id, text) in [
            ("queued-1", "first"),
            ("queued-2", "second"),
            ("queued-3", "third"),
        ] {
            chat.queued_prompts.push_back(queued(id, text));
        }

        chat.handle_key(key(KeyCode::Up));
        assert_eq!(chat.input, "third");
        assert_eq!(chat.queued_prompts.len(), 2);

        chat.clear_input();
        chat.handle_key(ctrl('p'));
        assert_eq!(chat.input, "second");
        assert_eq!(chat.queued_prompts.len(), 1);

        chat.clear_input();
        chat.handle_key(key(KeyCode::Up));
        assert_eq!(chat.input, "first");
        assert!(chat.queued_prompts.is_empty());

        chat.clear_input();
        chat.handle_key(key(KeyCode::Up));
        assert!(chat.input.is_empty());
    }

    #[test]
    fn model_and_effort_slash_commands_change_live_session_config() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.input = "/model gpt-5.6-luna".into();
        assert_eq!(
            chat.handle_key(key(KeyCode::Enter)),
            ChatAction::SetConfig {
                key: "model".into(),
                value: "gpt-5.6-luna".into(),
            }
        );

        chat.input = "/effort xhigh".into();
        assert_eq!(
            chat.handle_key(key(KeyCode::Enter)),
            ChatAction::SetConfig {
                key: "effort".into(),
                value: "xhigh".into(),
            }
        );
    }

    #[test]
    fn config_commands_are_queued_while_the_agent_is_busy() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.phase = WorkerPhase::Running;

        chat.input = "/model".into();
        assert_eq!(chat.handle_key(key(KeyCode::Enter)), ChatAction::None);
        assert_eq!(
            chat.notices.current().as_deref(),
            Some("usage: /model <value>")
        );

        chat.input = "/model sonnet".into();
        assert_eq!(
            chat.handle_key(key(KeyCode::Enter)),
            ChatAction::SetConfig {
                key: "model".into(),
                value: "sonnet".into(),
            }
        );
        assert!(chat.input.is_empty());

        chat.phase = WorkerPhase::Closing;
        chat.input = "/model sonnet".into();
        assert_eq!(chat.handle_key(key(KeyCode::Enter)), ChatAction::None);
        assert_eq!(
            chat.notices.current().as_deref(),
            Some("The worker is closing; this configuration change was not sent")
        );
    }

    #[test]
    fn a_queued_config_change_peels_back_into_the_composer() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        let mut session = MaterializedSession::empty("1234567890");
        // The projection only rebuilds when its frontier moved.
        session.applied_event_ordinal = 5;
        session.queued_prompts.push(MaterializedQueuedPrompt {
            command_id: "queued-config".into(),
            kind: QueuedCommandKind::SetConfig {
                key: "model".into(),
                value: "sonnet".into(),
            },
            content: vec![serde_json::json!({"type": "text", "text": "/model sonnet"})],
            queued_at_ms: 10,
        });
        chat.apply_materialized(&session, &[], &[]);
        assert_eq!(chat.queued_prompts.len(), 1);
        assert_eq!(chat.queued_prompts[0].queue_label(), "queued config");

        assert_eq!(
            chat.handle_key(ctrl('p')),
            ChatAction::RemoveQueuedPrompt {
                id: "queued-config".into(),
                text: "/model sonnet".into(),
                kind: QueuedCommandKind::SetConfig {
                    key: "model".into(),
                    value: "sonnet".into(),
                },
            }
        );
        assert_eq!(chat.input, "/model sonnet");
        assert!(chat.queued_prompts.is_empty());

        // Resubmitting the peeled-back text parses as the same change.
        chat.phase = WorkerPhase::Running;
        assert_eq!(
            chat.handle_key(key(KeyCode::Enter)),
            ChatAction::SetConfig {
                key: "model".into(),
                value: "sonnet".into(),
            }
        );
    }

    #[test]
    fn plan_toggles_the_session_mode_for_a_harness_without_a_plan_command() {
        let mut chat = grok_chat();
        chat.set_input("/plan".into());

        assert_eq!(
            chat.submit_input(),
            ChatAction::PlanCommand {
                original: "/plan".into(),
                control: PlanControl::SetSessionMode {
                    mode_id: "plan".into()
                },
                requested_active: true,
                prompt: None,
            }
        );
        assert!(chat.input.is_empty());
        assert!(chat.notices.current().unwrap().contains("Plan mode on"));

        chat.plan_command_pending = false;
        chat.set_input("/plan".into());
        assert_eq!(
            chat.submit_input(),
            ChatAction::PlanCommand {
                original: "/plan".into(),
                control: PlanControl::SetSessionMode {
                    mode_id: "default".into()
                },
                requested_active: false,
                prompt: None,
            }
        );
        assert!(chat.notices.current().unwrap().contains("Plan mode off"));
    }

    #[test]
    fn plan_accepts_explicit_on_and_off_arguments() {
        let mut chat = grok_chat();
        chat.set_input("/plan off".into());
        assert_eq!(chat.submit_input(), ChatAction::None);

        chat.set_input("/plan ON".into());
        assert_eq!(
            chat.submit_input(),
            ChatAction::PlanCommand {
                original: "/plan ON".into(),
                control: PlanControl::SetSessionMode {
                    mode_id: "plan".into()
                },
                requested_active: true,
                prompt: None,
            }
        );

        chat.plan_command_pending = false;
        chat.set_input("/plan sideways".into());
        assert_eq!(chat.submit_input(), ChatAction::Prompt("sideways".into()));
    }

    #[test]
    fn plan_uses_an_advertised_mode_config_option() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.set_config_options(&[mode_config_option("default", &["default", "plan"])]);
        chat.set_input("/plan".into());

        assert_eq!(
            chat.submit_input(),
            ChatAction::PlanCommand {
                original: "/plan".into(),
                control: PlanControl::SetConfig {
                    key: "mode".into(),
                    value: "plan".into()
                },
                requested_active: true,
                prompt: None,
            }
        );
    }

    #[test]
    fn grok_uses_its_trusted_set_mode_fallback_even_with_an_unrelated_mode_config() {
        let mut chat = grok_chat();
        chat.set_config_options(&[mode_config_option("default", &["default", "act"])]);
        chat.set_input("/plan".into());

        assert!(matches!(
            chat.submit_input(),
            ChatAction::PlanCommand {
                control: PlanControl::SetSessionMode { .. },
                ..
            }
        ));
    }

    #[test]
    fn an_unchanged_mode_catalogue_does_not_undo_an_optimistic_toggle() {
        let options = [mode_config_option("default", &["default", "plan"])];
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.set_config_options(&options);
        chat.set_input("/plan".into());
        assert!(matches!(
            chat.submit_input(),
            ChatAction::PlanCommand { .. }
        ));

        chat.set_config_options(&options);

        assert!(chat.plan_mode_active());
    }

    #[test]
    fn an_agent_plan_command_does_not_override_hels_unified_command() {
        let mut chat = grok_chat();
        advertise(&mut chat, 1, &["plan"]);
        chat.set_input("/plan the migration".into());

        assert_eq!(
            chat.submit_input(),
            ChatAction::PlanCommand {
                original: "/plan the migration".into(),
                control: PlanControl::SetSessionMode {
                    mode_id: "plan".into()
                },
                requested_active: true,
                prompt: Some("the migration".into()),
            }
        );
    }

    #[test]
    fn plan_is_kept_local_without_a_compatible_mode_surface() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.set_input("/plan".into());

        assert_eq!(chat.submit_input(), ChatAction::None);
        assert_eq!(chat.input, "/plan");
        assert!(chat.notices.current().unwrap().contains("does not expose"));
    }

    #[test]
    fn codex_plan_uses_collaboration_mode_not_the_permission_mode() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.set_harness_kind(HarnessKind::Codex);
        chat.set_config_options(&[
            select_config_option("mode", "read-only", &["read-only", "full-access"]),
            select_config_option("collaboration_mode", "default", &["default", "plan"]),
        ]);
        chat.set_input("/plan inspect the migration".into());

        assert_eq!(
            chat.submit_input(),
            ChatAction::PlanCommand {
                original: "/plan inspect the migration".into(),
                control: PlanControl::SetConfig {
                    key: "collaboration_mode".into(),
                    value: "plan".into(),
                },
                requested_active: true,
                prompt: Some("inspect the migration".into()),
            }
        );
        assert_eq!(
            chat.prompt_history.last().map(String::as_str),
            Some("/plan inspect the migration")
        );
    }

    #[test]
    fn claude_and_kimi_prefer_the_exact_mode_config() {
        for harness in [HarnessKind::Claude, HarnessKind::Kimi] {
            let mut chat = ChatState::new(&snapshot(), &[]);
            chat.set_harness_kind(harness);
            chat.set_config_options(&[select_config_option(
                "mode",
                "default",
                &["default", "plan"],
            )]);
            chat.set_input("/plan".into());
            assert!(matches!(
                chat.submit_input(),
                ChatAction::PlanCommand {
                    control: PlanControl::SetConfig { ref key, .. },
                    ..
                } if key == "mode"
            ));
        }
    }

    #[test]
    fn grok_uses_set_mode_without_advertising_modes() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.set_harness_kind(HarnessKind::Grok);
        chat.set_input("/plan".into());
        assert!(matches!(
            chat.submit_input(),
            ChatAction::PlanCommand {
                control: PlanControl::SetSessionMode { ref mode_id },
                ..
            } if mode_id == "plan"
        ));
    }

    #[test]
    fn deepseek_rejects_plan_and_implement_locally() {
        let mut chat = grok_chat();
        chat.set_harness_kind(HarnessKind::Deepseek);
        for command in ["/plan design it", "/implement"] {
            chat.set_input(command.into());
            assert_eq!(chat.submit_input(), ChatAction::None);
            assert_eq!(chat.input, command);
            assert!(
                chat.notices
                    .current()
                    .unwrap()
                    .contains("unsupported in DSH")
            );
        }
    }

    #[test]
    fn implement_exits_plan_mode_before_submitting_the_instruction() {
        let mut chat = grok_chat();
        chat.current_mode = Some("plan".into());
        chat.set_input("/implement start with the parser".into());
        assert_eq!(
            chat.submit_input(),
            ChatAction::PlanCommand {
                original: "/implement start with the parser".into(),
                control: PlanControl::SetSessionMode {
                    mode_id: "default".into()
                },
                requested_active: false,
                prompt: Some("start with the parser".into()),
            }
        );
    }

    #[test]
    fn plan_review_choices_have_distinct_followup_directions() {
        let mut chat = grok_chat();
        chat.current_mode = Some("plan".into());
        let standard = ElicitationRequest {
            id: "plan-review-1".into(),
            message: "review".into(),
            title: None,
            description: None,
            fields: Vec::new(),
        };
        let response = |action: &str, feedback: Option<&str>| {
            let mut content = BTreeMap::new();
            content.insert("action".into(), ElicitationValue::String(action.into()));
            if let Some(feedback) = feedback {
                content.insert("feedback".into(), ElicitationValue::String(feedback.into()));
            }
            ElicitationResponse::Accept { content }
        };

        assert_eq!(
            chat.plan_review_followup(&standard, &response("implement", None)),
            Some(PlanReviewFollowup {
                desired_active: false,
                control: None,
                prompt: None,
            })
        );
        assert_eq!(
            chat.plan_review_followup(&standard, &response("revise", Some("add tests"))),
            Some(PlanReviewFollowup {
                desired_active: true,
                control: None,
                prompt: Some("add tests".into()),
            })
        );
        assert!(matches!(
            chat.plan_review_followup(&standard, &response("exit", None)),
            Some(PlanReviewFollowup {
                desired_active: false,
                control: Some(PlanControl::SetSessionMode { .. }),
                prompt: None,
            })
        ));
    }

    #[test]
    fn plan_waits_for_an_idle_agent() {
        let mut chat = grok_chat();
        chat.phase = WorkerPhase::Running;
        chat.set_input("/plan".into());

        assert_eq!(chat.submit_input(), ChatAction::None);
        assert!(chat.notices.current().unwrap().contains("only available"));
    }

    #[test]
    fn a_current_mode_update_corrects_the_locally_tracked_plan_mode() {
        let mut chat = grok_chat();
        chat.set_input("/plan".into());
        chat.submit_input();
        assert!(chat.plan_mode_active());

        let mut session = MaterializedSession::empty("1234567890");
        session
            .configuration
            .insert("mode".into(), serde_json::Value::String("default".into()));
        chat.apply_materialized(&session, &[], &[]);

        assert!(!chat.plan_mode_active());
    }

    #[test]
    fn config_slash_command_without_value_shows_usage() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.input = "/model".into();

        assert_eq!(chat.handle_key(key(KeyCode::Enter)), ChatAction::None);
        assert_eq!(chat.notice().as_deref(), Some("usage: /model <value>"));
    }

    #[test]
    fn editor_preserves_uppercase_text_while_shortcuts_remain_case_insensitive() {
        let mut chat = ChatState::new(&snapshot(), &[]);

        chat.handle_key(KeyEvent::new(KeyCode::Char('H'), KeyModifiers::SHIFT));
        // Some terminals report the uppercase character without a Shift modifier.
        chat.handle_key(key(KeyCode::Char('I')));
        assert_eq!(chat.input, "HI");

        chat.handle_key(ctrl('r'));
        chat.handle_key(KeyEvent::new(KeyCode::Char('N'), KeyModifiers::SHIFT));
        assert_eq!(chat.history_search.as_ref().unwrap().query, "N");
        chat.handle_key(key(KeyCode::Esc));

        chat.handle_key(KeyEvent::new(
            KeyCode::Char('T'),
            KeyModifiers::CONTROL | KeyModifiers::SHIFT,
        ));
        assert_eq!(chat.render_mode, TranscriptRenderMode::Raw);
    }

    #[test]
    fn ctrl_v_returns_paste_request_action() {
        let mut chat = ChatState::new(&snapshot(), &[]);

        assert_eq!(chat.handle_key(ctrl('v')), ChatAction::PasteFromClipboard);
        assert!(chat.input.is_empty());
    }

    #[test]
    fn control_t_replaces_control_r_as_raw_transcript_toggle() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.handle_key(ctrl('t'));
        assert_eq!(chat.render_mode, TranscriptRenderMode::Raw);
        chat.handle_key(ctrl('r'));
        assert_eq!(chat.render_mode, TranscriptRenderMode::Raw);
        assert!(chat.history_search.is_some());
    }

    #[test]
    fn replay_projects_user_and_agent_text() {
        let runtime = RuntimeEvent::SessionUpdate {
            update: serde_json::json!({
                "sessionUpdate": "agent_message_chunk",
                "content": {"type": "text", "text": "done"}
            }),
        };
        let events = vec![
            SequencedEvent {
                seq: 1,
                recorded_at_ms: None,
                request_id: Some("p".into()),
                event: WorkerEvent::PromptAccepted {
                    request_id: "p".into(),
                    text: "work".into(),
                    attachments: vec![],
                },
            },
            SequencedEvent {
                seq: 2,
                recorded_at_ms: None,
                request_id: None,
                event: WorkerEvent::Adapter {
                    kind: "session_update".into(),
                    payload: serde_json::to_value(runtime).unwrap(),
                },
            },
        ];
        let mut initial = snapshot();
        initial.latest_seq = 2;
        let chat = ChatState::new(&initial, &events);
        assert_eq!(chat.entries.len(), 2);
        assert_eq!(chat.entries[0].role, ChatRole::User);
        assert_eq!(chat.entries[1].text, "done");
    }

    #[test]
    fn hydrated_tail_continues_the_last_streamed_message() {
        let first = RuntimeEvent::SessionUpdate {
            update: serde_json::json!({
                "sessionUpdate": "agent_message_chunk",
                "messageId": "answer",
                "content": {"type": "text", "text": "hello"}
            }),
        };
        let event = SequencedEvent {
            seq: 1,
            recorded_at_ms: None,
            request_id: None,
            event: WorkerEvent::Adapter {
                kind: "session_update".into(),
                payload: serde_json::to_value(first).unwrap(),
            },
        };
        let mut initial = snapshot();
        initial.latest_seq = 1;
        let full = ChatState::new(&initial, &[event]);
        let entries = full.bounded_entries(10, 512 * 1024);
        let mut tail =
            ChatState::from_tail(initial.session_id.clone(), WorkerPhase::Running, 1, entries);
        let second = RuntimeEvent::SessionUpdate {
            update: serde_json::json!({
                "sessionUpdate": "agent_message_chunk",
                "messageId": "answer",
                "content": {"type": "text", "text": " world"}
            }),
        };
        tail.apply_events(&[SequencedEvent {
            seq: 2,
            recorded_at_ms: None,
            request_id: None,
            event: WorkerEvent::Adapter {
                kind: "session_update".into(),
                payload: serde_json::to_value(second).unwrap(),
            },
        }]);

        assert_eq!(tail.entries.len(), 1);
        assert_eq!(tail.entries[0].text, "hello world");
        let materialized = tail.materialized_session();
        assert_eq!(materialized.transcript[0].position, 1);
        assert_eq!(
            materialized.transcript[0].latest_content_event_ordinal,
            Some(2)
        );
        assert_eq!(materialized.unread_agent_messages_after(1), 1);
    }

    #[test]
    fn streamed_message_chunks_coalesce_into_one_entry() {
        let mut initial = snapshot();
        initial.latest_seq = 0;
        let mut chat = ChatState::new(&initial, &[]);
        for (seq, text) in [(1, "gpt"), (2, "-5.6"), (3, "-terra")] {
            chat.apply_session_update(
                seq,
                &serde_json::json!({
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"type": "text", "text": text}
                }),
            );
        }
        chat.apply_session_update(
            4,
            &serde_json::json!({
                "sessionUpdate": "agent_thought_chunk",
                "content": {"type": "text", "text": "hmm"}
            }),
        );
        assert_eq!(chat.entries.len(), 2);
        assert_eq!(chat.entries[0].role, ChatRole::Agent);
        assert_eq!(chat.entries[0].text, "gpt-5.6-terra");
        assert_eq!(chat.entries[1].role, ChatRole::Thought);
    }

    #[test]
    fn tool_calls_render_title_and_updates_stay_quiet() {
        let mut initial = snapshot();
        initial.latest_seq = 0;
        let mut chat = ChatState::new(&initial, &[]);
        chat.apply_session_update(
            1,
            &serde_json::json!({"sessionUpdate": "tool_call",
                "toolCallId": "grep-config",
                "title": "grep config", "status": "pending"}),
        );
        chat.apply_session_update(
            2,
            &serde_json::json!({"sessionUpdate": "tool_call_update",
                "toolCallId": "grep-config", "status": "completed",
                "content": [{"type": "content", "content": {"type": "text", "text": "noise"}}]}),
        );
        assert_eq!(chat.entries.len(), 1);
        assert_eq!(chat.entries[0].role, ChatRole::Tool);
        assert_eq!(chat.entries[0].text, "grep config");
    }

    #[test]
    fn partial_tool_updates_preserve_unchanged_structured_fields() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.apply_session_update(
            1,
            &serde_json::json!({
                "sessionUpdate": "tool_call",
                "toolCallId": "inspect",
                "title": "inspect",
                "content": [{
                    "type": "content",
                    "content": {"type": "text", "text": "first result"}
                }],
                "locations": [{"path": "src/lib.rs", "line": 7}]
            }),
        );
        chat.apply_session_update(
            2,
            &serde_json::json!({
                "sessionUpdate": "tool_call_update",
                "toolCallId": "inspect",
                "content": [{
                    "type": "content",
                    "content": {"type": "text", "text": "replacement result"}
                }]
            }),
        );

        assert_eq!(chat.entries[0].tool_content, ["replacement result"]);
        assert_eq!(chat.entries[0].tool_locations, ["src/lib.rs:7"]);
    }

    #[test]
    fn unknown_json_does_not_leak_nested_text_into_the_transcript() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.apply_session_update(
            1,
            &serde_json::json!({"items": [{"text": "not an ACP message"}]}),
        );
        assert!(chat.entries.is_empty());
    }

    #[test]
    fn message_ids_keep_adjacent_agent_messages_separate() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        for (seq, id, text) in [(1, "one", "first"), (2, "two", "second")] {
            chat.apply_session_update(
                seq,
                &serde_json::json!({
                    "sessionUpdate": "agent_message_chunk",
                    "messageId": id,
                    "content": {"type": "text", "text": text}
                }),
            );
        }
        assert_eq!(chat.entries.len(), 2);
        assert_eq!(chat.entries[0].text, "first");
        assert_eq!(chat.entries[1].text, "second");
    }

    #[test]
    fn plan_updates_replace_the_current_turn_plan() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        for (seq, status) in [(1, "pending"), (2, "completed")] {
            chat.apply_session_update(
                seq,
                &serde_json::json!({
                    "sessionUpdate": "plan",
                    "entries": [{
                        "content": "inspect renderer",
                        "priority": "high",
                        "status": status
                    }]
                }),
            );
        }
        assert_eq!(chat.entries.len(), 1);
        assert_eq!(chat.entries[0].role, ChatRole::Plan);
        assert_eq!(chat.entries[0].plan[0].status, PlanStatus::Completed);
    }

    #[test]
    fn apply_materialized_skips_rebuild_at_same_ordinal() {
        let mut session = MaterializedSession::empty("session-same-ordinal");
        session.applied_event_ordinal = 1;
        session.transcript.push(Arc::new(TranscriptItem {
            stable_id: "user:1".into(),
            position: 1,
            latest_content_event_ordinal: None,
            created_at_ms: 10,
            last_changed_at_ms: 10,
            body: TranscriptBody::User {
                content: vec![serde_json::json!("first")],
            },
        }));

        let mut chat = ChatState::from_materialized(&session, &[], &[]);
        assert_eq!(chat.entries[0].text, "first");

        Arc::make_mut(&mut session.transcript[0]).body = TranscriptBody::User {
            content: vec![serde_json::json!("changed without new ordinal")],
        };
        session.queued_prompts.push(MaterializedQueuedPrompt {
            command_id: "queued".into(),
            kind: QueuedCommandKind::Prompt,
            content: vec![serde_json::json!("queued prompt")],
            queued_at_ms: 20,
        });
        chat.apply_materialized(&session, &[], &[]);

        assert_eq!(chat.entries[0].text, "first");
        assert!(chat.queued_prompts.is_empty());
    }

    #[test]
    fn materialized_diff_counts_arrive_after_the_path_and_ignore_stale_revisions() {
        let mut session = MaterializedSession::empty("session-diffstats");
        session.applied_event_ordinal = 1;
        session.transcript.push(Arc::new(TranscriptItem {
            stable_id: "tool:edit".into(),
            position: 1,
            latest_content_event_ordinal: None,
            created_at_ms: 10,
            last_changed_at_ms: 10,
            body: TranscriptBody::Tool {
                call: serde_json::json!({
                    "toolCallId": "edit",
                    "title": "Edit src/lib.rs",
                    "status": "completed",
                    "content": [{
                        "type": "diff",
                        "path": "/workspace/src/lib.rs",
                        "oldText": "alpha\n",
                        "newText": "alpha\nbeta\n"
                    }]
                }),
                terminal_outputs: Vec::new(),
                terminal_refs: Vec::new(),
            },
        }));

        let mut chat = ChatState::from_materialized(&session, &[], &[]);
        assert_eq!(chat.entries[0].tool_diffstats, ["/workspace/src/lib.rs"]);
        let request = chat.take_diffstat_requests(1).pop().unwrap();
        let exact = request.clone().compute();
        chat.apply_diffstats("tool:edit", 9, exact.clone());
        assert_eq!(chat.entries[0].tool_diffstats, ["/workspace/src/lib.rs"]);
        chat.apply_diffstats("tool:edit", 10, exact);
        assert_eq!(
            chat.entries[0].tool_diffstats,
            ["/workspace/src/lib.rs  +1 −0"]
        );
    }
}
