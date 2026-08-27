//! The live chat view: the conversations pane, the background feeds behind an
//! open session, and the frame the dashboard draws.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use crossterm::event::{Event, KeyCode, KeyModifiers, MouseEvent};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Borders, Paragraph, Wrap};

use crate::hel_database::{HistoryScope, PromptHistoryEntry};
use crate::hel_session_manager::{
    ManagedSessionHandle, ManagedSessionView, SessionManagerControl, ViewError, new_command_id,
};
use crate::hel_state::{
    MaterializedSession, RecoveryContext, TranscriptBody, TranscriptItem, config_command_text,
};
use crate::hel_transcript::ChatEntry;
use crate::hel_worker::WorkerPhase;

use super::autocomplete::render_autocomplete;
use super::elicitation::render_elicitation;
use super::history::{highlighted_input_lines, history_scope_name, history_search_footer};
use super::input::{input_cursor_visual_position, input_visual_rows, set_input_cursor};
use super::remote::{
    ChatRemoteOperation, ChatRemoteResult, ChatRemoteSupervisor, apply_chat_remote_result,
    queue_chat_remote_operation, restore_unsent_input,
};
use super::rendering::{display_width, truncate_line_to_width, truncate_to_width};
use super::transcript::{
    ToolDiffstatRequest, TranscriptAnchor, materialized_chunks_text, materialized_prefix_entries,
    render_transcript,
};
use super::{
    ChatAction, ChatEventOutcome, ChatFocus, ChatState, Notices, OtherSessionActivity,
    OtherSessionIdentity, SessionHeaderIdentity, last_nonempty_line, queued_prompt_preview,
    turn_band_color, turn_started_at_epoch_seconds,
};

use crate::clock::epoch_seconds;

/// How many conversations the pane shows at once. The transcript owns the rest
/// of the screen, so the list stays a window however many sessions are open.
const CONVERSATIONS_PANE_MAX_ROWS: usize = 7;
const MAX_DIFFSTAT_TASKS: usize = 2;

#[derive(Debug)]
enum ChatIoUpdate {
    ProjectHistoryPrefetched(std::result::Result<Vec<PromptHistoryEntry>, String>),
    HistorySearchResults {
        generation: u64,
        result: std::result::Result<Vec<PromptHistoryEntry>, String>,
    },
    ClipboardText(std::result::Result<String, String>),
    OtherSessions(Vec<OtherSessionActivity>),
    /// The history a large session did not convert when it opened, built off
    /// the event loop. `attempt` counts the tries so far, so a transcript that
    /// keeps changing under the conversion cannot retry for ever.
    TranscriptPrefix {
        attempt: u32,
        result: std::result::Result<(Vec<ChatEntry>, Vec<ToolDiffstatRequest>), String>,
    },
    ToolDiffstats {
        tool_call_id: String,
        revision: u64,
        result: std::result::Result<Vec<String>, String>,
    },
}

/// How many times a refused prefix is rebuilt before the view settles for its
/// tail. Compaction rewriting the history under a pending conversion is rare,
/// and one rebuild against the current snapshot normally lands.
const MAX_PREFIX_CONVERSION_ATTEMPTS: u32 = 3;

impl ChatState {
    /// Keys while the conversations pane has focus. The pane owns the arrows
    /// and the vi keys; the transcript keys keep working from either focus, and
    /// text keys are dropped rather than leaking into the composer.
    pub(super) fn handle_conversations_key(
        &mut self,
        code: KeyCode,
        modifiers: KeyModifiers,
    ) -> ChatAction {
        let control = modifiers.contains(KeyModifiers::CONTROL);
        let plain = !modifiers.intersects(KeyModifiers::CONTROL | KeyModifiers::ALT);
        match code {
            KeyCode::Char('t') if control => {
                self.toggle_render_mode();
                return ChatAction::None;
            }
            KeyCode::Home if control => {
                self.anchor = TranscriptAnchor::Row { entry: 0, row: 0 };
                return ChatAction::None;
            }
            KeyCode::End if control => {
                self.anchor = TranscriptAnchor::Bottom;
                return ChatAction::None;
            }
            KeyCode::PageUp => {
                self.scroll_history_up(self.last_viewport_height.max(1));
                return ChatAction::None;
            }
            KeyCode::PageDown => {
                self.scroll_history_down(self.last_viewport_height.max(1));
                return ChatAction::None;
            }
            _ => {}
        }
        let step = if plain && matches!(code, KeyCode::Up | KeyCode::Char('k'))
            || control && code == KeyCode::Char('p')
        {
            Some(-1)
        } else if plain && matches!(code, KeyCode::Down | KeyCode::Char('j'))
            || control && code == KeyCode::Char('n')
        {
            Some(1)
        } else {
            None
        };
        if let Some(step) = step {
            // Walking the list re-centres it, whatever the wheel left behind.
            self.conversations_window_start = None;
            // No wrap: the ends of the list stay put.
            return match self.neighbour_session(step) {
                Some(session_id) => ChatAction::SwitchSession { session_id },
                None => ChatAction::None,
            };
        }
        if matches!(code, KeyCode::Tab | KeyCode::Enter | KeyCode::Esc) {
            self.focus = ChatFocus::Prompt;
        }
        ChatAction::None
    }

    pub(super) fn focus_conversations(&mut self) {
        self.focus = ChatFocus::Conversations;
        self.conversations_window_start = None;
    }

    /// The session `step` rows from the current one in the pane's order.
    /// `None` at either end of the list, and for a list of one.
    fn neighbour_session(&self, step: isize) -> Option<String> {
        let rows = conversation_rows(self);
        let current = rows.iter().position(|row| row.current)?;
        let target = current.checked_add_signed(step)?;
        rows.get(target).map(|row| row.session_id.clone())
    }

    /// A left click inside the conversations pane switches to the clicked
    /// session, mirroring keyboard selection. Focus is left alone, the same
    /// as the wheel: only Tab moves focus.
    pub(super) fn click_conversation_row(&mut self, mouse: MouseEvent) -> ChatAction {
        let Some(area) = self.conversations_area else {
            return ChatAction::None;
        };
        let height = usize::from(area.height);
        if height == 0 {
            return ChatAction::None;
        }
        let rows = conversation_rows(self);
        let current = rows.iter().position(|row| row.current).unwrap_or(0);
        let start = conversations_window_start(
            rows.len(),
            current,
            height,
            self.conversations_window_start,
        );
        let clicked = usize::from(mouse.row.saturating_sub(area.y));
        if clicked >= height {
            return ChatAction::None;
        }
        let Some(row) = rows.get(start + clicked) else {
            return ChatAction::None;
        };
        if row.current {
            return ChatAction::None;
        }
        self.conversations_window_start = None;
        ChatAction::SwitchSession {
            session_id: row.session_id.clone(),
        }
    }

    /// Moves the conversations window by `rows`, leaving the current session
    /// where it is. Clamped to the list, so the window cannot run off either
    /// end.
    pub(super) fn scroll_conversations(&mut self, rows: isize) {
        let entries = conversation_rows(self);
        let height = self
            .conversations_area
            .map_or(0, |area| usize::from(area.height));
        if height == 0 || entries.len() <= height {
            return;
        }
        let current = entries.iter().position(|entry| entry.current).unwrap_or(0);
        let start = conversations_window_start(
            entries.len(),
            current,
            height,
            self.conversations_window_start,
        );
        self.conversations_window_start = Some(
            start
                .saturating_add_signed(rows)
                .min(entries.len() - height),
        );
    }
}

fn dispatch_history_search_request(
    chat: &mut ChatState,
    updates: &tokio::sync::mpsc::UnboundedSender<ChatIoUpdate>,
) {
    let Some(request) = chat.take_history_search_request() else {
        return;
    };
    let generation = request.generation;
    let updates = updates.clone();
    tokio::spawn(async move {
        let result = match tokio::task::spawn_blocking(move || {
            ChatState::resolve_history_search_request(request)
        })
        .await
        {
            Ok(result) => result,
            Err(error) => Err(format!("history search task failed: {error}")),
        };
        if let Err(error) = updates.send(ChatIoUpdate::HistorySearchResults { generation, result })
        {
            tracing::debug!(%error, "history search result dropped because the chat closed");
        }
    });
}

fn dispatch_diffstat_requests(
    chat: &mut ChatState,
    updates: &tokio::sync::mpsc::UnboundedSender<ChatIoUpdate>,
    in_flight: &mut usize,
) {
    let available = MAX_DIFFSTAT_TASKS.saturating_sub(*in_flight);
    for request in chat.take_diffstat_requests(available) {
        *in_flight += 1;
        let tool_call_id = request.tool_call_id.clone();
        let revision = request.revision;
        let updates = updates.clone();
        tokio::spawn(async move {
            let result = match tokio::task::spawn_blocking(move || request.compute()).await {
                Ok(result) => result,
                Err(error) => Err(format!("diff summary task failed: {error}")),
            };
            if let Err(error) = updates.send(ChatIoUpdate::ToolDiffstats {
                tool_call_id,
                revision,
                result,
            }) {
                tracing::debug!(%error, "tool diff summary dropped because the chat closed");
            }
        });
    }
}

/// The history a tail-first open still owes the view: the transcript items in
/// front of the loaded tail, and the frontier they are converted against.
/// Cloning the item vector copies handles, not conversations.
struct PendingPrefix {
    items: Vec<Arc<TranscriptItem>>,
    frontier: u64,
}

impl PendingPrefix {
    fn of(session: &MaterializedSession, length: usize) -> Option<Self> {
        let length = length.min(session.transcript.len());
        (length > 0).then(|| Self {
            items: session.transcript[..length].to_vec(),
            frontier: session.applied_event_ordinal,
        })
    }
}

/// Converts the unloaded history on a blocking thread and reports it back over
/// the chat's I/O feed, including the failure of the conversion itself.
fn spawn_transcript_prefix(
    pending: PendingPrefix,
    attempt: u32,
    updates: tokio::sync::mpsc::UnboundedSender<ChatIoUpdate>,
) {
    tokio::spawn(async move {
        let result = match tokio::task::spawn_blocking(move || {
            let entries = materialized_prefix_entries(&pending.items, pending.frontier);
            let diffstats = pending
                .items
                .iter()
                .filter_map(ToolDiffstatRequest::from_item)
                .collect();
            (entries, diffstats)
        })
        .await
        {
            Ok(entries) => Ok(entries),
            Err(error) => Err(format!("history conversion task failed: {error}")),
        };
        if let Err(error) = updates.send(ChatIoUpdate::TranscriptPrefix { attempt, result }) {
            tracing::debug!(%error, "transcript conversion result dropped because the chat closed");
        }
    });
}

/// Whether applying an update left history that still has to be converted.
#[derive(Debug, PartialEq, Eq)]
enum PrefixRebuild {
    NotNeeded,
    /// The converted history no longer lines up with the tail, so it has to be
    /// rebuilt from the session's current snapshot. `attempt` numbers the try.
    Needed {
        attempt: u32,
    },
}

fn apply_chat_io_update(chat: &mut ChatState, update: ChatIoUpdate) -> PrefixRebuild {
    match update {
        ChatIoUpdate::TranscriptPrefix { attempt, result } => match result {
            Ok((entries, diffstats)) => {
                if chat.splice_transcript_prefix(entries) {
                    chat.queue_diffstat_requests(diffstats);
                    return PrefixRebuild::NotNeeded;
                }
                if attempt >= MAX_PREFIX_CONVERSION_ATTEMPTS {
                    chat.set_notice(
                        "Earlier messages could not be loaded; showing the recent history only.",
                    );
                    return PrefixRebuild::NotNeeded;
                }
                return PrefixRebuild::Needed {
                    attempt: attempt.saturating_add(1),
                };
            }
            Err(error) => {
                tracing::warn!(%error, "earlier chat history could not be converted");
                chat.set_notice(format!("Earlier messages failed to load: {error}"));
            }
        },
        ChatIoUpdate::ProjectHistoryPrefetched(Ok(entries)) => chat.set_project_history(entries),
        ChatIoUpdate::ProjectHistoryPrefetched(Err(error)) => {
            tracing::warn!(%error, "project chat history prefetch failed");
            chat.set_project_history_unavailable(error);
        }
        ChatIoUpdate::HistorySearchResults { generation, result } => {
            chat.apply_history_search_results(generation, result);
        }
        ChatIoUpdate::ClipboardText(Ok(text)) => chat.handle_paste(&text),
        ChatIoUpdate::ClipboardText(Err(error)) => {
            tracing::warn!(%error, "clipboard read failed and was shown in the UI");
            chat.set_notice(format!("Paste failed: {error}"));
        }
        ChatIoUpdate::OtherSessions(sessions) => chat.other_sessions = sessions,
        ChatIoUpdate::ToolDiffstats {
            tool_call_id,
            revision,
            result,
        } => chat.apply_diffstats(&tool_call_id, revision, result),
    }
    PrefixRebuild::NotNeeded
}

/// Marker on the current session's header line. The session list uses the same
/// glyph for the row the user has selected.
const CURRENT_SESSION_CARET: &str = "› ";

/// One session's row in the conversations pane, whichever session it belongs
/// to.
#[derive(Debug)]
struct ConversationRow {
    session_id: String,
    position: usize,
    current: bool,
    turn_started_at_epoch_seconds: Option<u64>,
    last_agent_line: Option<String>,
}

/// The window the conversations pane draws, and where the current session sits
/// inside it.
#[derive(Debug)]
struct ConversationsPane {
    lines: Vec<Line<'static>>,
    /// Row of the current session within the window. `None` once the wheel has
    /// scrolled it out of view.
    current_row: Option<usize>,
}

/// Rows for every active session, the current one included, in the order the
/// session list shows them.
fn conversation_rows(chat: &ChatState) -> Vec<ConversationRow> {
    let mut rows = vec![ConversationRow {
        session_id: chat.session_id.clone(),
        position: chat.position,
        current: true,
        turn_started_at_epoch_seconds: chat.turn_started_at_epoch_seconds,
        last_agent_line: chat.last_agent_line(),
    }];
    rows.extend(chat.other_sessions.iter().map(|session| ConversationRow {
        session_id: session.session_id.clone(),
        position: session.position,
        current: false,
        turn_started_at_epoch_seconds: session.turn_started_at_epoch_seconds,
        last_agent_line: session.last_agent_line.clone(),
    }));
    rows.sort_by_key(|row| row.position);
    rows
}

/// Where a `height`-row window onto `rows` starts. Without an override the
/// window centres on the current session; the wheel supplies one. Both are
/// clamped, so the window never runs past either end of the list.
fn conversations_window_start(
    rows: usize,
    current: usize,
    height: usize,
    override_start: Option<usize>,
) -> usize {
    let last_start = rows.saturating_sub(height);
    match override_start {
        Some(start) => start.min(last_start),
        None => current.saturating_sub(height / 2).min(last_start),
    }
}

/// The rows the pane shows, at most `max_rows` of them.
fn conversations_pane(
    chat: &ChatState,
    now_epoch_seconds: u64,
    width: usize,
    max_rows: usize,
) -> ConversationsPane {
    let rows = conversation_rows(chat);
    let height = max_rows.max(1).min(rows.len());
    let current = rows.iter().position(|row| row.current).unwrap_or(0);
    let start =
        conversations_window_start(rows.len(), current, height, chat.conversations_window_start);
    ConversationsPane {
        lines: rows
            .iter()
            .skip(start)
            .take(height)
            .map(|row| conversation_line(row, now_epoch_seconds, width))
            .collect(),
        current_row: current.checked_sub(start).filter(|offset| *offset < height),
    }
}

fn conversation_line(row: &ConversationRow, now_epoch_seconds: u64, width: usize) -> Line<'static> {
    let caret = if row.current {
        CURRENT_SESSION_CARET
    } else {
        "  "
    };
    let clock = crate::usage_format::format_turn_clock(
        now_epoch_seconds,
        row.turn_started_at_epoch_seconds,
    );
    let band = Style::default().fg(turn_band_color(row.turn_started_at_epoch_seconds.is_some()));
    let last_line = row.last_agent_line.as_deref().unwrap_or_default().trim();
    let prefix = if last_line.is_empty() {
        format!("{caret}{clock}")
    } else {
        format!("{caret}{clock} ")
    };
    truncate_line_to_width(Line::styled(format!("{prefix}{last_line}"), band), width)
}

fn other_session_activity(
    identity: &OtherSessionIdentity,
    session: &MaterializedSession,
) -> OtherSessionActivity {
    OtherSessionActivity {
        session_id: identity.session_id.clone(),
        position: identity.position,
        turn_started_at_epoch_seconds: turn_started_at_epoch_seconds(session.execution),
        last_agent_line: session
            .transcript
            .iter()
            .rev()
            .find(|item| item.is_nonempty_agent_message())
            .and_then(|item| match &item.body {
                TranscriptBody::Agent { chunks, .. } => {
                    last_nonempty_line(&materialized_chunks_text(chunks))
                }
                _ => None,
            }),
    }
}

/// How often the poller retries sessions with no live actor. Resolving costs a
/// manager round trip, and a stopped or lost session never resolves.
const OTHER_SESSION_RESOLVE_TICKS: u64 = 5;

/// Poll the other sessions once a second so the chat view can show what is
/// happening elsewhere. The task ends when the chat loop drops its receiver,
/// so it cannot outlive the view it feeds.
fn spawn_other_session_poller(
    control: SessionManagerControl,
    others: Vec<OtherSessionIdentity>,
    updates: tokio::sync::mpsc::UnboundedSender<ChatIoUpdate>,
) {
    if others.is_empty() {
        return;
    }
    tokio::spawn(async move {
        let mut resolved: HashMap<String, ManagedSessionHandle> = HashMap::new();
        let mut sent: Vec<OtherSessionActivity> = Vec::new();
        let mut tick: u64 = 0;
        while !updates.is_closed() {
            if tick.is_multiple_of(OTHER_SESSION_RESOLVE_TICKS) {
                for identity in &others {
                    if resolved.contains_key(&identity.session_id) {
                        continue;
                    }
                    // No live actor is normal for stopped or lost sessions.
                    if let Ok(handle) = control.session(identity.session_id.clone()).await {
                        resolved.insert(identity.session_id.clone(), handle);
                    }
                }
            }
            let mut summaries = Vec::with_capacity(others.len());
            let mut stopped = Vec::new();
            for identity in &others {
                let Some(handle) = resolved.get(&identity.session_id) else {
                    continue;
                };
                if handle.has_changed().is_err() {
                    stopped.push(identity.session_id.clone());
                    continue;
                }
                // `with_view` reads in place; `view()` would clone the whole
                // transcript of every other session once a second.
                let summary = handle.with_view(|view| {
                    view.snapshot
                        .as_ref()
                        .map(|snapshot| other_session_activity(identity, &snapshot.materialized))
                });
                summaries.extend(summary);
            }
            for session_id in stopped {
                resolved.remove(&session_id);
            }
            if summaries != sent {
                if let Err(error) = updates.send(ChatIoUpdate::OtherSessions(summaries.clone())) {
                    tracing::debug!(%error, "other-session activity result dropped because the chat closed");
                    return;
                }
                sent = summaries;
            }
            tick = tick.wrapping_add(1);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
}

/// Applies one session view to the chat. `false` means the session manager has
/// stopped and can never send again, so its feed must not be awaited any more.
///
/// This runs whether or not the chat is on screen: a warm chat behind the
/// session list stays as current as one the user is watching.
fn apply_session_view(state: &mut ChatState, view: Result<ManagedSessionView>) -> bool {
    let view = match view {
        Ok(view) => view,
        Err(error) => {
            // Keep the transcript readable rather than tearing the dashboard
            // down around a stopped manager.
            tracing::warn!(error = format!("{error:#}"), "chat session view failed");
            state.set_transcript_loading(false);
            state.set_notice(format!("connection lost: {error:#}"));
            return false;
        }
    };
    if view.snapshot.is_some() || view.error.is_some() {
        state.set_transcript_loading(false);
    }
    if let Some(snapshot) = view.snapshot {
        state.apply_materialized(
            &snapshot.materialized,
            &snapshot.operational.config_options,
            &snapshot.operational.available_commands,
        );
        state.set_session_modes(snapshot.operational.modes.clone());
        state.set_active_user_shells(&snapshot.operational.active_user_shells);
        state.set_active_agent_terminals(
            &snapshot.operational.active_agent_terminals,
            &snapshot.materialized,
        );
    }
    if let Some(error) = view.error {
        match error {
            ViewError::Unreachable(detail) => {
                tracing::warn!(%detail, "chat session became unreachable");
                state.set_notice(format!("connection lost: {detail}"))
            }
            ViewError::TargetMissing(detail) => {
                tracing::warn!(%detail, "chat session target is missing");
                state.set_notice(format!("managed target lost: {detail}"))
            }
            ViewError::ProjectionIntegrity(detail) => {
                tracing::error!(%detail, "chat transcript projection failed");
                state.set_notice(format!("transcript projection failed: {detail}"))
            }
        }
    }
    true
}

/// The user has left the chat, whether for the session list, another
/// conversation, or the shell. Clears the interaction state that should not
/// follow them back in and reports how far they have now read, which becomes
/// the session's read receipt.
fn detach_chat(state: &mut ChatState) -> u64 {
    let last_seen_event_ordinal = state.latest_seq();
    state.reset_interaction();
    last_seen_event_ordinal
}

/// A chat view and every background feed behind it.
///
/// The dashboard owns one of these while a session is open and keeps it after
/// the user returns to the session list, so the view stays current off screen
/// and reopening the same session is only a redraw. Dropping it detaches the
/// proxy and leaves the target worker alive.
pub struct ActiveChat {
    state: ChatState,
    session: ManagedSessionHandle,
    bundle_id: String,
    recovery: Option<RecoveryContext>,
    remote: ChatRemoteSupervisor,
    /// Held so the receiver never reports the feed closed, and so spawned
    /// clipboard and history tasks have somewhere to report.
    chat_io_tx: tokio::sync::mpsc::UnboundedSender<ChatIoUpdate>,
    chat_io_rx: tokio::sync::mpsc::UnboundedReceiver<ChatIoUpdate>,
    diffstats_in_flight: usize,
    /// Held for the same reason, and cloned into each dictation thread.
    voice_updates_tx: tokio::sync::mpsc::UnboundedSender<VoiceUpdate>,
    voice_updates_rx: tokio::sync::mpsc::UnboundedReceiver<VoiceUpdate>,
    voice_cancel: Option<std::sync::mpsc::Sender<()>>,
    voice_prefix: String,
    /// A closed feed reports `None` for ever, which would leave its arm
    /// permanently ready. Each flag retires its own arm instead.
    remote_open: bool,
    session_open: bool,
}

impl ActiveChat {
    /// Builds the view from the session's current snapshot and starts its
    /// background feeds. Cheap enough to call from the dashboard loop: the
    /// only work done here is converting the tail of the transcript, a bounded
    /// number of items. Every other step, including converting the history in
    /// front of that tail, is a spawned task.
    ///
    /// `draft` is the unsent input saved when this session was last detached.
    /// Only a fresh view takes it: a warm chat the dashboard kept alive already
    /// holds newer input than the database copy.
    ///
    /// `notices` is the process-wide notifications bar; it is installed on the
    /// new state before any notice is raised below, so recovery and connection
    /// notices land in the same shared slot the dashboard reads.
    ///
    #[allow(clippy::too_many_arguments)]
    pub fn open(
        session: ManagedSessionHandle,
        bundle_id: &str,
        recovery: Option<RecoveryContext>,
        control: SessionManagerControl,
        header: SessionHeaderIdentity,
        draft: String,
        notices: Notices,
    ) -> Self {
        let view = session.view();
        let needs_initial_sync = view.snapshot.is_none();
        // The history a tail-first open leaves behind is converted off the
        // loop; those entries arrive over the I/O feed and are spliced in front
        // of the tail.
        let (mut state, pending_prefix) = {
            let empty = MaterializedSession::empty(session.session_id());
            let snapshot = view.snapshot;
            let materialized = snapshot
                .as_ref()
                .map_or(&empty, |snapshot| &snapshot.materialized);
            let mut state = ChatState::from_materialized_tail(
                materialized,
                snapshot
                    .as_ref()
                    .map_or(&[][..], |snapshot| &snapshot.operational.config_options),
                snapshot
                    .as_ref()
                    .map_or(&[][..], |snapshot| &snapshot.operational.available_commands),
            );
            if let Some(harness_kind) = recovery
                .as_ref()
                .map(|recovery| recovery.session.harness_kind)
            {
                state.set_harness_kind(harness_kind);
            }
            state.set_session_modes(
                snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot.operational.modes.clone()),
            );
            if let Some(snapshot) = snapshot.as_ref() {
                state.set_active_user_shells(&snapshot.operational.active_user_shells);
                state.set_active_agent_terminals(
                    &snapshot.operational.active_agent_terminals,
                    &snapshot.materialized,
                );
            }
            let pending = PendingPrefix::of(materialized, state.unconverted_prefix());
            (state, pending)
        };
        state.set_history_context(bundle_id);
        state.set_header_position(header.position);
        state.restore_draft(draft);
        state.notices = notices;
        let (chat_io_tx, chat_io_rx) = tokio::sync::mpsc::unbounded_channel::<ChatIoUpdate>();
        {
            let updates = chat_io_tx.clone();
            let session_id = session.session_id().to_owned();
            let bundle_id = bundle_id.to_owned();
            tokio::spawn(async move {
                let result = match tokio::task::spawn_blocking(move || {
                    crate::hel_database::search_prompts(
                        &session_id,
                        &bundle_id,
                        HistoryScope::Project,
                        "",
                    )
                    .map_err(|error| format!("{error:#}"))
                })
                .await
                {
                    Ok(result) => result,
                    Err(error) => Err(format!("history prefetch task failed: {error}")),
                };
                if let Err(error) = updates.send(ChatIoUpdate::ProjectHistoryPrefetched(result)) {
                    tracing::debug!(%error, "project history result dropped because the chat closed");
                }
            });
        }
        if let Some(pending) = pending_prefix {
            spawn_transcript_prefix(pending, 1, chat_io_tx.clone());
        }
        spawn_other_session_poller(control, header.others, chat_io_tx.clone());
        if let Some(detail) = recovery
            .as_ref()
            .and_then(|recovery| recovery.session.last_checkpoint_error.as_deref())
        {
            state.set_notice(format!("Recovery copy failed: {detail}"));
        }
        let (voice_updates_tx, voice_updates_rx) =
            tokio::sync::mpsc::unbounded_channel::<VoiceUpdate>();
        let remote = ChatRemoteSupervisor::spawn(session.clone());
        if needs_initial_sync {
            state.set_transcript_loading(true);
            state.set_notice("Connecting to session relay…");
            queue_chat_remote_operation(remote.operations(), ChatRemoteOperation::Sync, &mut state);
        }
        let mut diffstats_in_flight = 0;
        dispatch_diffstat_requests(&mut state, &chat_io_tx, &mut diffstats_in_flight);
        Self {
            state,
            session,
            bundle_id: bundle_id.to_owned(),
            recovery,
            remote,
            chat_io_tx,
            chat_io_rx,
            diffstats_in_flight,
            voice_updates_tx,
            voice_updates_rx,
            voice_cancel: None,
            voice_prefix: String::new(),
            remote_open: true,
            session_open: true,
        }
    }

    pub fn session_id(&self) -> &str {
        self.session.session_id()
    }

    /// Whether this view is still attached to a live session actor.
    ///
    /// Pause, destroy, and a replaced target retire the actor and close this
    /// feed. A warm chat whose feed is closed still holds the last snapshot,
    /// including a Closing/Closed phase, so the dashboard must open a new view
    /// instead of redrawing this one.
    pub fn session_feed_open(&self) -> bool {
        self.session_open
    }

    /// The composer's current text. The dashboard saves this on detach so
    /// unsent input survives a quit or a crash while the view is off screen.
    pub fn draft(&self) -> &str {
        &self.state.input
    }

    pub fn latest_event_ordinal(&self) -> u64 {
        self.state.latest_seq()
    }

    /// Waits for the next background message, applies it, and drains whatever
    /// queued behind it, so one wakeup costs one redraw.
    ///
    /// `None` means no chat is warm, and the feed never wakes the caller. Cancel
    /// safe: every arm is a cancel-safe receive, and a message is applied only
    /// once its arm has won.
    pub async fn pump(chat: Option<&mut Self>) {
        let Some(chat) = chat else {
            return std::future::pending().await;
        };
        enum Wakeup {
            Remote(Option<ChatRemoteResult>),
            Io(ChatIoUpdate),
            Voice(VoiceUpdate),
            // Boxed: a view carries the whole session snapshot, and the enum
            // is built on every wakeup.
            View(Box<Result<ManagedSessionView>>),
        }
        // The senders for the I/O and voice feeds live in this struct, so those
        // receivers cannot report a closed channel and need no retirement flag.
        let wakeup = tokio::select! {
            result = chat.remote.recv(), if chat.remote_open => Wakeup::Remote(result),
            Some(update) = chat.chat_io_rx.recv() => Wakeup::Io(update),
            Some(update) = chat.voice_updates_rx.recv() => Wakeup::Voice(update),
            view = chat.session.changed(), if chat.session_open => Wakeup::View(Box::new(view)),
        };
        match wakeup {
            Wakeup::Remote(Some(result)) => apply_chat_remote_result(&mut chat.state, result),
            Wakeup::Remote(None) => chat.remote_open = false,
            Wakeup::Io(update) => chat.apply_io_update(update),
            Wakeup::Voice(update) => chat.apply_voice_update(update),
            Wakeup::View(view) => chat.apply_session_view(*view),
        }
        chat.drain().await;
        chat.report_worker_death().await;
    }

    async fn drain(&mut self) {
        while let Ok(result) = self.remote.try_recv() {
            apply_chat_remote_result(&mut self.state, result);
        }
        while let Ok(update) = self.chat_io_rx.try_recv() {
            self.apply_io_update(update);
        }
        while let Ok(update) = self.voice_updates_rx.try_recv() {
            self.apply_voice_update(update);
        }
        while self.session_open && self.session.has_changed().unwrap_or(false) {
            let view = self.session.changed().await;
            self.apply_session_view(view);
        }
    }

    /// Reports a background worker that stopped on its own. Cheap enough to
    /// check on every wakeup: it only joins a handle that already finished.
    async fn report_worker_death(&mut self) {
        let Some(result) = self.remote.take_finished().await else {
            return;
        };
        if let Err(error) = result {
            self.state
                .set_notice(format!("Chat background worker failed: {error}"));
        } else {
            self.state
                .set_notice("Chat background worker stopped unexpectedly");
        }
    }

    fn apply_io_update(&mut self, update: ChatIoUpdate) {
        if matches!(&update, ChatIoUpdate::ToolDiffstats { .. }) {
            self.diffstats_in_flight = self.diffstats_in_flight.saturating_sub(1);
        }
        if let PrefixRebuild::Needed { attempt } = apply_chat_io_update(&mut self.state, update) {
            self.rebuild_transcript_prefix(attempt);
        }
        dispatch_history_search_request(&mut self.state, &self.chat_io_tx);
        dispatch_diffstat_requests(
            &mut self.state,
            &self.chat_io_tx,
            &mut self.diffstats_in_flight,
        );
    }

    /// Restarts the history conversion against the session's current snapshot,
    /// after the transcript changed under the last one. Only the spawn happens
    /// here; the conversion itself stays off the event loop.
    fn rebuild_transcript_prefix(&mut self, attempt: u32) {
        let view = self.session.view();
        let Some(snapshot) = view.snapshot else {
            return;
        };
        let Some(pending) =
            PendingPrefix::of(&snapshot.materialized, self.state.unconverted_prefix())
        else {
            return;
        };
        spawn_transcript_prefix(pending, attempt, self.chat_io_tx.clone());
    }

    fn apply_voice_update(&mut self, update: VoiceUpdate) {
        match update {
            VoiceUpdate::Partial(text) => self
                .state
                .set_input(append_dictation(&self.voice_prefix, &text)),
            VoiceUpdate::Status(status) => self.state.set_notice(status),
            VoiceUpdate::Finished(result) => {
                self.state.voice_active = false;
                self.voice_cancel = None;
                match result {
                    Ok(text) => {
                        self.state
                            .set_input(append_dictation(&self.voice_prefix, &text));
                        self.state.notices.clear();
                    }
                    Err(error) => self
                        .state
                        .set_notice(crate::speech::dictation_error_message(&error)),
                }
            }
        }
    }

    fn apply_session_view(&mut self, view: Result<ManagedSessionView>) {
        self.session_open = apply_session_view(&mut self.state, view);
        dispatch_diffstat_requests(
            &mut self.state,
            &self.chat_io_tx,
            &mut self.diffstats_in_flight,
        );
    }

    /// Applies one terminal event and reports what it asked for.
    pub fn handle_event(&mut self, event: Event) -> ChatEventOutcome {
        let action = match event {
            Event::Key(key) => self.state.handle_key(key),
            Event::Paste(pasted) => {
                self.state.handle_paste(&pasted);
                ChatAction::None
            }
            Event::Mouse(mouse) => self.state.handle_mouse(mouse),
            // Resize and focus changes only need the redraw.
            _ => ChatAction::None,
        };
        let outcome = self.dispatch(action);
        dispatch_history_search_request(&mut self.state, &self.chat_io_tx);
        outcome
    }

    /// A command ID for one remote operation. `None` means the system random
    /// source failed, which leaves the command unsent rather than closing the
    /// view the user is reading.
    fn command_id(&mut self, prefix: &str) -> Option<String> {
        match new_command_id(prefix) {
            Ok(command_id) => Some(command_id),
            Err(error) => {
                self.state
                    .set_notice(format!("Could not identify the command: {error:#}"));
                None
            }
        }
    }

    fn dispatch(&mut self, action: ChatAction) -> ChatEventOutcome {
        match action {
            ChatAction::None => return ChatEventOutcome::None,
            ChatAction::Prompt(text) => {
                let Some(command_id) = self.command_id("prompt") else {
                    restore_unsent_input(&mut self.state, &text);
                    return ChatEventOutcome::Handled;
                };
                self.state.set_notice("Prompt queued for delivery…");
                queue_chat_remote_operation(
                    self.remote.operations(),
                    ChatRemoteOperation::Prompt {
                        command_id,
                        text,
                        session_id: self.session.session_id().to_owned(),
                        bundle_id: self.bundle_id.clone(),
                    },
                    &mut self.state,
                );
            }
            ChatAction::RunShell(command) => {
                let Some(command_id) = self.command_id("shell") else {
                    restore_unsent_input(&mut self.state, &format!("!{command}"));
                    return ChatEventOutcome::Handled;
                };
                self.state.set_notice("Shell command queued…");
                queue_chat_remote_operation(
                    self.remote.operations(),
                    ChatRemoteOperation::RunShell {
                        command_id,
                        command,
                    },
                    &mut self.state,
                );
            }
            ChatAction::RemoveQueuedPrompt { id, text, kind } => {
                let Some(command_id) = self.command_id("remove-prompt") else {
                    return ChatEventOutcome::Handled;
                };
                self.state.set_notice("Removing queued prompt…");
                queue_chat_remote_operation(
                    self.remote.operations(),
                    ChatRemoteOperation::RemoveQueuedPrompt {
                        command_id,
                        id,
                        text,
                        kind,
                    },
                    &mut self.state,
                );
            }
            ChatAction::SetConfig { key, value } => {
                let Some(command_id) = self.command_id("set-config") else {
                    restore_unsent_input(&mut self.state, &config_command_text(&key, &value));
                    return ChatEventOutcome::Handled;
                };
                self.state.set_notice("Sending configuration update…");
                queue_chat_remote_operation(
                    self.remote.operations(),
                    ChatRemoteOperation::SetConfig {
                        command_id,
                        key,
                        value,
                    },
                    &mut self.state,
                );
            }
            ChatAction::SetSessionMode { mode_id } => {
                let Some(command_id) = self.command_id("set-session-mode") else {
                    self.state.current_mode = None;
                    return ChatEventOutcome::Handled;
                };
                queue_chat_remote_operation(
                    self.remote.operations(),
                    ChatRemoteOperation::SetSessionMode {
                        command_id,
                        mode_id,
                    },
                    &mut self.state,
                );
            }
            ChatAction::PlanCommand {
                original,
                control,
                requested_active,
                prompt,
            } => {
                let Some(command_id) = self.command_id("plan-mode") else {
                    self.state.plan_command_pending = false;
                    restore_unsent_input(&mut self.state, &original);
                    return ChatEventOutcome::Handled;
                };
                queue_chat_remote_operation(
                    self.remote.operations(),
                    ChatRemoteOperation::PlanCommand {
                        command_id,
                        original,
                        control,
                        requested_active,
                        prompt,
                        session_id: self.session.session_id().to_owned(),
                        bundle_id: self.bundle_id.clone(),
                    },
                    &mut self.state,
                );
            }
            ChatAction::Cancel => {
                let Some(command_id) = self.command_id("cancel") else {
                    return ChatEventOutcome::Handled;
                };
                self.state.set_notice("Sending cancellation request…");
                queue_chat_remote_operation(
                    self.remote.operations(),
                    ChatRemoteOperation::Cancel {
                        command_id,
                        cancel_agent: self.state.phase == crate::hel_worker::WorkerPhase::Running,
                        shell_command_ids: self.state.active_user_shell_ids(),
                    },
                    &mut self.state,
                );
            }
            ChatAction::RespondElicitation { request, response } => {
                let plan_followup = self.state.plan_review_followup(&request, &response);
                self.state.set_notice("Sending answer…");
                queue_chat_remote_operation(
                    self.remote.operations(),
                    ChatRemoteOperation::RespondElicitation {
                        request,
                        response,
                        plan_followup,
                        session_id: self.session.session_id().to_owned(),
                        bundle_id: self.bundle_id.clone(),
                    },
                    &mut self.state,
                );
            }
            ChatAction::PasteFromClipboard => {
                let updates = self.chat_io_tx.clone();
                tokio::spawn(async move {
                    let result = match tokio::task::spawn_blocking(|| {
                        crate::hel_clipboard::read_text().map_err(|error| format!("{error:#}"))
                    })
                    .await
                    {
                        Ok(result) => result,
                        Err(error) => Err(format!("clipboard task failed: {error}")),
                    };
                    if let Err(error) = updates.send(ChatIoUpdate::ClipboardText(result)) {
                        tracing::debug!(%error, "clipboard result dropped because the chat closed");
                    }
                });
            }
            ChatAction::ToggleVoice => {
                if let Some(cancel) = self.voice_cancel.as_ref() {
                    let _ = cancel.send(());
                    self.state.set_notice("Stopping voice dictation…");
                } else if !crate::speech::voice_input_supported() {
                    self.state.set_notice(
                        "Voice helper unavailable; install hel-voice-worker beside hel or set HEL_VOICE_WORKER",
                    );
                } else {
                    let (cancel_tx, cancel_rx) = std::sync::mpsc::channel();
                    self.voice_cancel = Some(cancel_tx);
                    self.voice_prefix.clone_from(&self.state.input);
                    self.state.voice_active = true;
                    self.state
                        .set_notice("Listening… press Alt-V again to stop");
                    spawn_dictation(self.voice_updates_tx.clone(), cancel_rx);
                }
            }
            // Every exit detaches the same way; only the label the caller acts
            // on differs.
            ChatAction::Back => {
                self.cancel_dictation();
                return ChatEventOutcome::Back {
                    last_seen_event_ordinal: detach_chat(&mut self.state),
                };
            }
            ChatAction::QuitDetach => {
                self.cancel_dictation();
                return ChatEventOutcome::QuitDetach {
                    last_seen_event_ordinal: detach_chat(&mut self.state),
                };
            }
            ChatAction::SwitchSession { session_id } => {
                self.cancel_dictation();
                return ChatEventOutcome::SwitchSession {
                    session_id,
                    last_seen_event_ordinal: detach_chat(&mut self.state),
                };
            }
        }
        ChatEventOutcome::Handled
    }

    /// Stops any dictation thread. The thread reports `Finished`, which clears
    /// the view's voice state, so this only asks it to stop.
    fn cancel_dictation(&mut self) {
        if let Some(cancel) = self.voice_cancel.take() {
            let _ = cancel.send(());
        }
    }

    /// Opens the view with the conversations pane focused. The caller uses this
    /// when the user arrived by walking the list, so the next key walks on from
    /// here instead of landing in the composer.
    pub fn focus_conversations(&mut self) {
        self.state.focus_conversations();
    }

    /// Draws the view. The recovery flag is read here rather than tracked,
    /// because the checkpoint gate moves without telling the dashboard.
    pub fn draw(&mut self, frame: &mut Frame) {
        self.state.recovery_busy = self.recovery_busy();
        render(frame, &mut self.state);
    }

    fn recovery_busy(&self) -> bool {
        self.recovery.as_ref().is_some_and(RecoveryContext::is_busy)
    }

    /// Whether the checkpoint title on screen is stale.
    pub fn recovery_title_is_stale(&self) -> bool {
        self.recovery_busy() != self.state.recovery_busy
    }

    /// Whether a clock tick has anything to redraw for: a running turn in the
    /// header, whose clock counts up once a second, or a checkpoint title that
    /// has gone stale.
    pub fn needs_clock_tick(&self) -> bool {
        self.recovery_title_is_stale()
            || self.state.turn_started_at_epoch_seconds.is_some()
            || !self.state.active_agent_terminals.is_empty()
            || self
                .state
                .other_sessions
                .iter()
                .any(|session| session.turn_started_at_epoch_seconds.is_some())
    }
}

impl Drop for ActiveChat {
    fn drop(&mut self) {
        self.cancel_dictation();
    }
}

pub(super) fn render(frame: &mut Frame, chat: &mut ChatState) {
    let inner = frame.area();
    // The pane never takes more than a third of the screen. Its border eats
    // two columns, so the text inside wraps to that narrower width.
    let pane_width = usize::from(inner.width.saturating_sub(2)).max(1);
    let pane = conversations_pane(
        chat,
        epoch_seconds(),
        pane_width,
        usize::from(inner.height / 3).clamp(1, CONVERSATIONS_PANE_MAX_ROWS),
    );
    // The pane always lists at least the current session, so this always has
    // room for its own top and bottom border rows.
    let conversations_height = pane.lines.len() as u16 + 2;
    let visible_queued = chat.queued_prompts.len().min(3) as u16;
    let prompt_width = usize::from(inner.width.saturating_sub(2)).max(1);
    let input_rows = input_visual_rows(&chat.input, prompt_width) as u16;
    let desired_prompt_height = input_rows
        .saturating_add(visible_queued)
        .saturating_add(2)
        .max(4);
    let maximum_prompt_height = inner.height.saturating_sub(6 + conversations_height).max(3);
    let prompt_height = desired_prompt_height.min(maximum_prompt_height);
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(conversations_height),
            Constraint::Min(5),
            Constraint::Length(prompt_height),
            Constraint::Length(1),
        ])
        .split(inner);
    let (conversations_area, transcript_area, prompt_area, footer_area) =
        (chunks[0], chunks[1], chunks[2], chunks[3]);
    // Focus shows as a double border on whichever pane owns the keyboard, so
    // the split stays obvious without the eye following a moving band.
    let conversations_border = if chat.focus == ChatFocus::Conversations {
        BorderType::Double
    } else {
        BorderType::Plain
    };
    let prompt_border = if chat.focus == ChatFocus::Prompt {
        BorderType::Double
    } else {
        BorderType::Plain
    };
    let conversations_block = Block::default()
        .borders(Borders::ALL)
        .border_type(conversations_border)
        .title("Active");
    let conversations_inner = conversations_block.inner(conversations_area);
    // Consumers (mouse hover/click/scroll) map a screen row against this
    // rect, so it must be the pane's inner area, not the bordered outline.
    chat.conversations_area = Some(conversations_inner);
    frame.render_widget(conversations_block, conversations_area);
    frame.render_widget(Paragraph::new(pane.lines), conversations_inner);
    if chat.focus == ChatFocus::Conversations
        && let Some(row) = pane.current_row
        && let Some(y) = conversations_inner
            .y
            .checked_add(row as u16)
            .filter(|y| *y < conversations_inner.bottom())
    {
        // A band behind the current row, the way the session list marks its
        // selection, so the focused pane is obvious. Only the background moves:
        // each row keeps the colours that say what its session is doing.
        let buffer = frame.buffer_mut();
        for x in conversations_inner.x..conversations_inner.right() {
            buffer[(x, y)].set_bg(Color::DarkGray);
        }
    }
    render_transcript(frame, transcript_area, chat);
    let queued = chat.queued_prompts.len();
    let prompt_title = prompt_title(chat, queued);
    let prompt_block = Block::default()
        .borders(Borders::ALL)
        .border_type(prompt_border)
        .title(prompt_title);
    let prompt_inner = prompt_block.inner(prompt_area);
    let mut prompt_lines = chat
        .queued_prompts
        .iter()
        .rev()
        .take(3)
        .rev()
        .enumerate()
        .map(|(index, queued)| {
            Line::from(Span::styled(
                truncate_to_width(
                    &format!(
                        "{} {}: {}",
                        queued.queue_label(),
                        index + 1,
                        queued_prompt_preview(&queued.text)
                    ),
                    usize::from(prompt_inner.width),
                ),
                Style::default().fg(Color::DarkGray),
            ))
        })
        .collect::<Vec<_>>();
    let queue_rows = prompt_lines.len();
    prompt_lines.extend(if let Some(search) = chat.history_search.as_ref() {
        highlighted_input_lines(&chat.input, &search.query)
    } else {
        chat.input
            .split('\n')
            .map(|line| Line::raw(line.to_owned()))
            .collect()
    });
    let cursor_row =
        input_cursor_visual_position(&chat.input, chat.input_cursor, prompt_width).1 + queue_rows;
    let content_height = usize::from(prompt_inner.height).max(1);
    let input_scroll = cursor_row.saturating_add(1).saturating_sub(content_height);
    frame.render_widget(
        Paragraph::new(prompt_lines)
            .wrap(Wrap { trim: false })
            .scroll((input_scroll as u16, 0))
            .block(prompt_block),
        prompt_area,
    );
    // The cursor belongs to whatever has focus, so the composer only shows one
    // while the keyboard is driving it.
    if chat.history_search.is_none() && chat.focus == ChatFocus::Prompt {
        set_input_cursor(
            frame,
            prompt_inner,
            &chat.input,
            chat.input_cursor,
            queue_rows,
            input_scroll,
        );
    }
    let default_footer = if chat.focus == ChatFocus::Conversations {
        "j/k or ↑/↓ switch conversation · Enter/Tab prompt · Ctrl-G dashboard"
    } else if chat.voice_active {
        "Listening… Alt-V stop · Ctrl-G dashboard"
    } else if !chat.queued_prompts.is_empty() {
        "Up/Ctrl-P edit last queued · Enter send/queue · Shift-Enter newline · Ctrl-R history · Esc cancel · Ctrl-G dashboard"
    } else {
        "Enter send/queue · Shift-Enter newline · Ctrl-R history · Ctrl-T transcript · Esc cancel · Ctrl-G dashboard"
    };
    let search_footer = chat.history_search.as_ref().map(history_search_footer);
    let notice = chat.notices.current();
    let footer = search_footer
        .as_deref()
        .or(notice.as_deref())
        .unwrap_or(default_footer);
    // The shared notice bar is yellow wherever it shows; a search prompt or
    // the default hotkey hints stay the quieter dark gray.
    let footer_color = if search_footer.is_none() && notice.is_some() {
        Color::Yellow
    } else {
        Color::DarkGray
    };
    frame.render_widget(
        Paragraph::new(footer).style(Style::default().fg(footer_color)),
        footer_area,
    );
    if let Some(search) = chat.history_search.as_ref()
        && footer_area.width > 0
    {
        let prefix = format!("reverse-i-search [{}]: ", history_scope_name(search.scope));
        let column = display_width(&prefix) + display_width(&search.query);
        frame.set_cursor_position((
            footer_area.x + column.min(usize::from(footer_area.width.saturating_sub(1))) as u16,
            footer_area.y,
        ));
    }
    render_autocomplete(frame, prompt_area, chat);
    if let Some(dialog) = chat.elicitation.as_ref() {
        render_elicitation(frame, dialog);
    }
}

fn prompt_title(chat: &ChatState, queued: usize) -> String {
    let mut parts = [
        chat.current_model.as_deref(),
        chat.current_effort.as_deref(),
    ]
    .into_iter()
    .flatten()
    .map(str::to_owned)
    .collect::<Vec<_>>();
    if chat.recovery_busy {
        parts.push("Saving checkpoint…".into());
        parts.push(format!("{queued} queued"));
    } else {
        if chat.plan_mode_active() {
            parts.push("Prompt — PLAN MODE".into());
        } else {
            match chat.phase {
                WorkerPhase::Idle => parts.push("Prompt".into()),
                WorkerPhase::Running if chat.pursuing_goal() => parts.push("Pursuing goal".into()),
                WorkerPhase::Running => parts.push("Running".into()),
                WorkerPhase::Closing => parts.push("Closing".into()),
                WorkerPhase::Closed => parts.push("Closed".into()),
            }
        }
        if queued > 0 {
            parts.push(format!("{queued} queued"));
        }
        if chat.phase == WorkerPhase::Running {
            parts.push("Esc cancels".into());
        }
    }
    format!(" {} ", parts.join(" · "))
}

enum VoiceUpdate {
    Partial(String),
    Status(String),
    Finished(anyhow::Result<String>),
}

fn spawn_dictation(
    updates: tokio::sync::mpsc::UnboundedSender<VoiceUpdate>,
    cancel: std::sync::mpsc::Receiver<()>,
) {
    std::thread::spawn(move || {
        let partial_updates = updates.clone();
        let status_updates = updates.clone();
        let result = crate::speech::run_dictation(
            move |text| {
                if let Err(error) = partial_updates.send(VoiceUpdate::Partial(text)) {
                    tracing::debug!(%error, "voice partial result dropped because the chat closed");
                }
            },
            |_| {},
            move |status| {
                if let Err(error) = status_updates.send(VoiceUpdate::Status(status)) {
                    tracing::debug!(%error, "voice status dropped because the chat closed");
                }
            },
            cancel,
        );
        if let Err(error) = updates.send(VoiceUpdate::Finished(result)) {
            tracing::debug!(%error, "voice result dropped because the chat closed");
        }
    });
}

fn append_dictation(prefix: &str, transcript: &str) -> String {
    match (prefix.trim_end(), transcript.trim()) {
        ("", transcript) => transcript.to_owned(),
        (prefix, "") => prefix.to_owned(),
        (prefix, transcript) => format!("{prefix} {transcript}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conversation_rows_show_hour_clock_and_agent_fragment_without_project() {
        let row = ConversationRow {
            session_id: "session-1".into(),
            position: 0,
            current: true,
            turn_started_at_epoch_seconds: Some(1_000),
            last_agent_line: Some("most recent answer".into()),
        };
        let line = conversation_line(&row, 4_725, 80);
        let text = line
            .spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect::<String>();
        assert_eq!(text, "› 01:02:05 most recent answer");
        assert!(!text.contains("project"));
        assert_eq!(line.style.fg, Some(Color::Yellow));
    }
    use crate::hel_chat::test_support::{
        agent_message_item, agent_transcript_item, ctrl, drawn_transcript, key, mouse_at_row,
        mouse_in, queued, snapshot,
    };
    use crate::hel_state::MaterializedExecutionState;
    use crate::hel_worker::RELAY_EVENT_GENESIS_DIGEST;
    use crate::hel_worker::{SequencedEvent, WorkerEvent};
    use agent_client_protocol::schema::v1::{SessionConfigOption, SessionConfigOptionCategory};
    use crossterm::event::{MouseButton, MouseEvent, MouseEventKind};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use ratatui::layout::Rect;
    use std::collections::BTreeMap;

    fn managed_view(session: MaterializedSession) -> ManagedSessionView {
        let session_id = session.session_id.clone();
        let latest_ordinal = session.applied_event_ordinal;
        let latest_digest = session.applied_event_digest.clone();
        ManagedSessionView {
            snapshot: Some(crate::hel_state::ManagedSessionSnapshot {
                materialized: session,
                latest_credential_sync_signal: None,
                operational: crate::hel_worker::RelayOperationalState {
                    session_id,
                    execution: crate::hel_worker::RelayExecutionState::Idle,
                    latest_ordinal,
                    latest_digest: latest_digest.clone(),
                    acknowledged_through: latest_ordinal,
                    acknowledged_digest: latest_digest,
                    recovery_floor_ordinal: 0,
                    recovery_floor_digest: RELAY_EVENT_GENESIS_DIGEST.into(),
                    native_session_id: None,
                    agent_capabilities: None,
                    agent_info: None,
                    config_options: Vec::new(),
                    modes: None,
                    available_commands: Vec::new(),
                    config: BTreeMap::new(),
                    active_prompt: None,
                    queued_prompts: Vec::new(),
                    active_user_shells: Vec::new(),
                    active_agent_terminals: Vec::new(),
                    checkpoint_barrier: None,
                    checkpoint_ready: None,
                    last_acp_activity_at_ms: None,
                },
            }),
            connected: true,
            error: None,
        }
    }

    fn other_session(
        position: usize,
        _project: &str,
        turn_started_at_epoch_seconds: Option<u64>,
        last_agent_line: &str,
    ) -> OtherSessionActivity {
        OtherSessionActivity {
            session_id: other_session_id(position),
            position,
            turn_started_at_epoch_seconds,
            last_agent_line: (!last_agent_line.is_empty()).then(|| last_agent_line.to_owned()),
        }
    }

    /// The id `other_session` gives the row at `position`, so a test that walks
    /// the list knows which session a key landed on.
    fn other_session_id(position: usize) -> String {
        format!("session-{position}")
    }

    fn header_chat(
        _project: &str,
        position: usize,
        others: Vec<OtherSessionActivity>,
    ) -> ChatState {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.set_header_position(position);
        chat.other_sessions = others;
        chat
    }

    fn header_text(chat: &ChatState, now_epoch_seconds: u64, width: usize) -> Vec<String> {
        pane_text(&conversations_pane(chat, now_epoch_seconds, width, 10))
    }

    fn pane_text(pane: &ConversationsPane) -> Vec<String> {
        pane.lines
            .iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect::<String>()
            })
            .collect()
    }

    fn header_colors(chat: &ChatState, now_epoch_seconds: u64) -> Vec<Option<Color>> {
        conversations_pane(chat, now_epoch_seconds, 80, 10)
            .lines
            .iter()
            .map(|line| line.style.fg)
            .collect()
    }

    /// A chat among `count` active sessions with the current one at `current`.
    /// Each project names its position, so a window reads as its own bounds.
    fn windowed_chat(count: usize, current: usize) -> ChatState {
        header_chat(
            &format!("project-{current}"),
            current,
            (0..count)
                .filter(|position| *position != current)
                .map(|position| other_session(position, &format!("project-{position}"), None, ""))
                .collect(),
        )
    }

    fn window_projects(chat: &ChatState, max_rows: usize) -> Vec<String> {
        let rows = conversation_rows(chat);
        let current = rows.iter().position(|row| row.current).unwrap_or(0);
        let start = conversations_window_start(
            rows.len(),
            current,
            max_rows.max(1).min(rows.len()),
            chat.conversations_window_start,
        );
        rows.iter()
            .skip(start)
            .take(max_rows)
            .map(|row| format!("project-{}", row.position))
            .collect()
    }

    #[test]
    fn dictation_appends_to_existing_prompt_cleanly() {
        assert_eq!(append_dictation("please", "fix this"), "please fix this");
        assert_eq!(append_dictation("", "fix this"), "fix this");
        assert_eq!(append_dictation("please ", ""), "please");
    }

    #[test]
    fn detaching_leaves_the_unsent_input_where_the_dashboard_saves_it_from() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.set_input("half typed thought".into());

        // Detaching keeps the composer intact: the warm chat goes on holding
        // it, and the dashboard reads it here to write it to the session row.
        detach_chat(&mut chat);
        assert_eq!(chat.input, "half typed thought");

        detach_chat(&mut chat);
        assert_eq!(chat.input, "half typed thought");
    }

    #[test]
    fn detaching_an_empty_composer_leaves_an_empty_draft_to_save() {
        let mut chat = ChatState::new(&snapshot(), &[]);

        detach_chat(&mut chat);

        assert_eq!(chat.input, "");
    }

    #[test]
    fn an_off_screen_chat_follows_the_session_view() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.set_transcript_loading(true);
        let mut session = MaterializedSession::empty("session-warm");
        session.applied_event_ordinal = 5;
        session.applied_event_digest = "a".repeat(64);
        session.transcript = vec![agent_transcript_item("first", 5)];

        assert!(apply_session_view(
            &mut chat,
            Ok(managed_view(session.clone()))
        ));
        assert_eq!(chat.latest_seq(), 5);
        assert_eq!(chat.entries.len(), 1);
        assert!(!chat.transcript_loading);

        session.applied_event_ordinal = 8;
        session.transcript.push(agent_transcript_item("second", 8));
        assert!(apply_session_view(&mut chat, Ok(managed_view(session))));
        assert_eq!(chat.latest_seq(), 8);
        assert_eq!(chat.entries.len(), 2);
    }

    #[test]
    fn a_stopped_session_manager_retires_its_feed_and_says_so() {
        let mut chat = ChatState::new(&snapshot(), &[]);

        let open = apply_session_view(&mut chat, Err(anyhow::anyhow!("session manager stopped")));

        assert!(!open);
        assert_eq!(
            chat.notice().as_deref(),
            Some("connection lost: session manager stopped")
        );
    }

    #[test]
    fn retiring_the_session_feed_keeps_a_closing_phase_in_place() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.phase = WorkerPhase::Closed;

        assert!(!apply_session_view(
            &mut chat,
            Err(anyhow::anyhow!("session manager stopped"))
        ));
        assert_eq!(chat.phase(), WorkerPhase::Closed);
    }

    #[test]
    fn leaving_the_chat_reports_the_ordinal_the_user_has_read() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        let mut session = MaterializedSession::empty("session-read");
        session.applied_event_ordinal = 12;
        session.transcript = vec![agent_transcript_item("first", 12)];
        apply_session_view(&mut chat, Ok(managed_view(session)));
        chat.queued_prompts.push_back(queued("queued-1", "queued"));

        assert_eq!(detach_chat(&mut chat), 12);
        // The transcript stays warm for the next visit; the interaction state
        // that belonged to the visit does not.
        assert_eq!(chat.entries.len(), 1);
        assert!(chat.queued_prompts.is_empty());
        assert_eq!(detach_chat(&mut chat), 12);
    }

    #[test]
    fn a_notice_set_through_a_shared_handle_shows_in_the_chat_footer_in_yellow() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        let shared = Notices::default();
        chat.notices = shared.clone();
        // Wide enough that the default hint line (over 100 columns) is not
        // truncated, so the footer text comparisons below are meaningful.
        let mut terminal = Terminal::new(TestBackend::new(120, 24)).expect("terminal");

        // Set from "outside", the way the dashboard's clone of the same handle
        // would.
        shared.set("Background import finished");
        terminal
            .draw(|frame| render(frame, &mut chat))
            .expect("draw chat");
        let buffer = terminal.backend().buffer();
        let footer_row = buffer.area.bottom() - 1;
        let footer_text = (buffer.area.x..buffer.area.right())
            .map(|x| buffer[(x, footer_row)].symbol())
            .collect::<String>();
        assert!(footer_text.contains("Background import finished"));
        assert_eq!(buffer[(buffer.area.x, footer_row)].fg, Color::Yellow);

        shared.clear();
        terminal
            .draw(|frame| render(frame, &mut chat))
            .expect("draw chat");
        let buffer = terminal.backend().buffer();
        let footer_text = (buffer.area.x..buffer.area.right())
            .map(|x| buffer[(x, footer_row)].symbol())
            .collect::<String>();
        assert!(footer_text.contains("Ctrl-G dashboard"));
        assert_eq!(buffer[(buffer.area.x, footer_row)].fg, Color::DarkGray);
    }

    #[test]
    fn composer_title_shows_live_model_and_effort_without_outer_session_frame() {
        use agent_client_protocol::schema::v1::{
            SessionConfigSelectOption, SessionConfigSelectOptions,
        };

        let options = vec![
            SessionConfigOption::select(
                "model",
                "Model",
                "gpt-5.6-sol",
                SessionConfigSelectOptions::Ungrouped(vec![SessionConfigSelectOption::new(
                    "gpt-5.6-sol",
                    "Sol",
                )]),
            )
            .category(SessionConfigOptionCategory::Model),
            SessionConfigOption::select(
                "effort",
                "Effort",
                "high",
                SessionConfigSelectOptions::Ungrouped(vec![SessionConfigSelectOption::new(
                    "high", "High",
                )]),
            )
            .category(SessionConfigOptionCategory::ThoughtLevel),
        ];
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.phase = WorkerPhase::Running;
        chat.set_config_options(&options);
        let mut terminal = Terminal::new(TestBackend::new(100, 24)).expect("terminal");

        terminal
            .draw(|frame| render(frame, &mut chat))
            .expect("draw chat");
        let buffer = terminal.backend().buffer();
        let rendered = buffer
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(rendered.contains("gpt-5.6-sol · high · Running · Esc cancels"));
        // No outer frame wraps the whole session (that's what the corner
        // border belongs to now: the conversations pane's own "Active" box,
        // not a re-introduced session title bar).
        assert!(!rendered.contains("HEL /"));
        assert_eq!(buffer[(buffer.area.x, buffer.area.y)].symbol(), "┌");
    }

    #[test]
    fn composer_title_names_plan_mode_even_during_a_turn() {
        let mut chat = crate::hel_chat::test_support::grok_chat();
        chat.current_mode = Some("plan".into());
        chat.phase = WorkerPhase::Running;

        assert!(prompt_title(&chat, 0).contains("Prompt — PLAN MODE"));
    }

    #[test]
    fn composer_title_identifies_an_active_advertised_goal() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.apply_session_update(
            1,
            &serde_json::json!({
                "sessionUpdate": "available_commands_update",
                "availableCommands": [
                    {"name": "goal", "description": "set a persistent goal"}
                ]
            }),
        );
        chat.apply_event(&SequencedEvent {
            seq: 2,
            recorded_at_ms: None,
            request_id: Some("goal".into()),
            event: WorkerEvent::PromptAccepted {
                request_id: "goal".into(),
                text: "/goal ship the release".into(),
                attachments: Vec::new(),
            },
        });

        assert!(prompt_title(&chat, 0).contains("Pursuing goal"));
        assert!(!prompt_title(&chat, 0).contains("Running"));

        chat.apply_event(&SequencedEvent {
            seq: 3,
            recorded_at_ms: None,
            request_id: None,
            event: WorkerEvent::TurnCompleted,
        });
        assert!(prompt_title(&chat, 0).contains("Prompt"));
        assert!(!prompt_title(&chat, 0).contains("Pursuing goal"));
    }

    #[test]
    fn composer_title_does_not_label_ordinary_or_unadvertised_prompts_as_goals() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.mark_prompt_submitted("/goal ship the release");
        assert!(prompt_title(&chat, 0).contains("Running"));
        assert!(!prompt_title(&chat, 0).contains("Pursuing goal"));

        chat.apply_session_update(
            1,
            &serde_json::json!({
                "sessionUpdate": "available_commands_update",
                "availableCommands": [
                    {"name": "goal", "description": "set a persistent goal"}
                ]
            }),
        );
        chat.mark_prompt_submitted("please ship the release");
        assert!(prompt_title(&chat, 0).contains("Running"));
        assert!(!prompt_title(&chat, 0).contains("Pursuing goal"));
    }

    #[test]
    fn session_header_lists_every_session_in_order_and_marks_the_current_one() {
        let chat = header_chat(
            "middle",
            1,
            vec![
                other_session(2, "last", None, "later work"),
                other_session(0, "first", None, "earlier work"),
            ],
        );

        assert_eq!(
            header_text(&chat, 0, 80),
            ["  [idle] earlier work", "› [idle]", "  [idle] later work",]
        );
    }

    #[test]
    fn session_header_shows_a_turn_clock_for_running_sessions_and_idle_for_the_rest() {
        let chat = header_chat(
            "current",
            0,
            vec![other_session(1, "other", Some(1_000), "still going")],
        );

        assert_eq!(
            header_text(&chat, 1_125, 80),
            ["› [idle]", "  00:02:05 still going"]
        );
        assert_eq!(
            header_colors(&chat, 1_125),
            [Some(Color::LightYellow), Some(Color::Yellow)]
        );
    }

    #[test]
    fn conversations_pane_centres_its_window_on_the_current_session() {
        let chat = windowed_chat(10, 5);

        assert_eq!(
            window_projects(&chat, 7),
            [
                "project-2",
                "project-3",
                "project-4",
                "project-5",
                "project-6",
                "project-7",
                "project-8",
            ]
        );
        // The current session sits in the middle of the window it centres.
        assert_eq!(conversations_pane(&chat, 0, 80, 7).current_row, Some(3));
    }

    #[test]
    fn conversations_pane_clamps_its_window_at_both_ends_of_the_list() {
        let first = windowed_chat(10, 0);
        assert_eq!(
            window_projects(&first, 7),
            [
                "project-0",
                "project-1",
                "project-2",
                "project-3",
                "project-4",
                "project-5",
                "project-6",
            ]
        );
        assert_eq!(conversations_pane(&first, 0, 80, 7).current_row, Some(0));

        let last = windowed_chat(10, 9);
        assert_eq!(
            window_projects(&last, 7),
            [
                "project-3",
                "project-4",
                "project-5",
                "project-6",
                "project-7",
                "project-8",
                "project-9",
            ]
        );
        assert_eq!(conversations_pane(&last, 0, 80, 7).current_row, Some(6));
    }

    #[test]
    fn conversations_pane_lists_every_session_when_they_all_fit() {
        let chat = windowed_chat(3, 1);

        assert_eq!(
            window_projects(&chat, 7),
            ["project-0", "project-1", "project-2"]
        );
    }

    #[test]
    fn tab_moves_focus_between_the_prompt_and_the_conversations_pane() {
        let mut chat = windowed_chat(3, 1);

        assert_eq!(chat.handle_key(key(KeyCode::Tab)), ChatAction::None);
        assert_eq!(chat.focus, ChatFocus::Conversations);

        assert_eq!(chat.handle_key(key(KeyCode::Tab)), ChatAction::None);
        assert_eq!(chat.focus, ChatFocus::Prompt);
    }

    #[test]
    fn tab_completes_an_open_popup_before_it_reaches_the_conversations_pane() {
        let mut chat = windowed_chat(3, 1);
        chat.set_input("/hel".into());

        chat.handle_key(key(KeyCode::Tab));

        assert_eq!(chat.input, "/help ");
        assert_eq!(chat.focus, ChatFocus::Prompt);
    }

    #[test]
    fn the_conversation_list_switches_to_the_neighbouring_session() {
        let mut chat = windowed_chat(10, 5);
        chat.focus_conversations();

        for previous in [key(KeyCode::Up), key(KeyCode::Char('k')), ctrl('p')] {
            assert_eq!(
                chat.handle_key(previous),
                ChatAction::SwitchSession {
                    session_id: other_session_id(4)
                }
            );
        }
        for next in [key(KeyCode::Down), key(KeyCode::Char('j')), ctrl('n')] {
            assert_eq!(
                chat.handle_key(next),
                ChatAction::SwitchSession {
                    session_id: other_session_id(6)
                }
            );
        }
        // The caller makes the switch, so the pane keeps its focus for the
        // session that opens next.
        assert_eq!(chat.focus, ChatFocus::Conversations);
    }

    #[test]
    fn the_conversation_list_does_not_wrap_at_either_end() {
        let mut first = windowed_chat(3, 0);
        first.focus_conversations();
        assert_eq!(first.handle_key(key(KeyCode::Char('k'))), ChatAction::None);
        assert_eq!(first.handle_key(key(KeyCode::Up)), ChatAction::None);
        assert_eq!(first.handle_key(ctrl('p')), ChatAction::None);

        let mut last = windowed_chat(3, 2);
        last.focus_conversations();
        assert_eq!(last.handle_key(key(KeyCode::Char('j'))), ChatAction::None);
        assert_eq!(last.handle_key(key(KeyCode::Down)), ChatAction::None);
        assert_eq!(last.handle_key(ctrl('n')), ChatAction::None);

        // A list of one has nowhere to go in either direction.
        let mut alone = windowed_chat(1, 0);
        alone.focus_conversations();
        assert_eq!(alone.handle_key(key(KeyCode::Char('j'))), ChatAction::None);
        assert_eq!(alone.handle_key(key(KeyCode::Char('k'))), ChatAction::None);
    }

    #[test]
    fn enter_and_escape_leave_the_conversation_list_for_the_prompt() {
        for code in [KeyCode::Enter, KeyCode::Esc] {
            let mut chat = windowed_chat(3, 1);
            chat.focus_conversations();

            assert_eq!(chat.handle_key(key(code)), ChatAction::None);
            assert_eq!(chat.focus, ChatFocus::Prompt);
        }
    }

    #[test]
    fn escape_in_the_conversation_list_does_not_cancel_a_running_turn() {
        let mut chat = windowed_chat(3, 1);
        chat.phase = WorkerPhase::Running;
        chat.focus_conversations();

        assert_eq!(chat.handle_key(key(KeyCode::Esc)), ChatAction::None);

        // Back at the prompt, Esc means what it has always meant.
        assert_eq!(chat.focus, ChatFocus::Prompt);
        assert_eq!(chat.handle_key(key(KeyCode::Esc)), ChatAction::Cancel);
    }

    #[test]
    fn typing_in_the_conversation_list_leaves_the_composer_alone() {
        let mut chat = windowed_chat(3, 1);
        chat.set_input("half typed".into());
        chat.focus_conversations();

        for code in [
            KeyCode::Char('x'),
            KeyCode::Char(' '),
            KeyCode::Backspace,
            KeyCode::Delete,
        ] {
            assert_eq!(chat.handle_key(key(code)), ChatAction::None);
        }

        assert_eq!(chat.input, "half typed");
        assert_eq!(chat.focus, ChatFocus::Conversations);
    }

    #[test]
    fn the_wheel_moves_the_conversations_window_without_switching_or_taking_focus() {
        let mut chat = windowed_chat(10, 5);
        let _ = drawn_transcript(&mut chat, 60, 24);
        let pane = chat
            .conversations_area
            .expect("the pane records its hitbox");
        // The border consumes the outer top row, so the hitbox other
        // consumers (mouse mapping, the band) use starts one row lower.
        assert_eq!(pane.y, 1, "the pane's border pushes its hitbox down a row");

        chat.handle_mouse(mouse_in(MouseEventKind::ScrollDown, pane));

        assert_eq!(window_projects(&chat, 7)[0], "project-3");
        assert_eq!(conversations_pane(&chat, 0, 80, 7).current_row, Some(2));
        // Hover decides what scrolls, so the wheel leaves focus where Tab put
        // it, and the current session is still this one.
        assert_eq!(chat.focus, ChatFocus::Prompt);
        assert_eq!(chat.session_id, snapshot().session_id);

        // The window stops at the end of the list, and at its start.
        chat.handle_mouse(mouse_in(MouseEventKind::ScrollDown, pane));
        assert_eq!(window_projects(&chat, 7)[0], "project-3");
        for _ in 0..5 {
            chat.handle_mouse(mouse_in(MouseEventKind::ScrollUp, pane));
        }
        assert_eq!(window_projects(&chat, 7)[0], "project-0");

        // The border row just above the hitbox belongs to the frame, not the
        // pane, so hovering it does not move the pane's window.
        chat.handle_mouse(MouseEvent {
            kind: MouseEventKind::ScrollDown,
            column: pane.x,
            row: pane.y - 1,
            modifiers: KeyModifiers::NONE,
        });
        assert_eq!(window_projects(&chat, 7)[0], "project-0");

        // Below the pane the wheel is the transcript's again.
        let transcript = Rect::new(0, pane.bottom(), 60, 1);
        chat.handle_mouse(mouse_in(MouseEventKind::ScrollDown, transcript));
        assert_eq!(window_projects(&chat, 7)[0], "project-0");
    }

    #[test]
    fn clicking_a_conversation_line_switches_to_it_and_resets_the_window() {
        let mut chat = windowed_chat(10, 5);
        let _ = drawn_transcript(&mut chat, 60, 24);
        let pane = chat
            .conversations_area
            .expect("the pane records its hitbox");
        // Centred on session 5, the window shows project-2..project-8, so row
        // 0 is project-2 (session id "session-2").
        assert_eq!(window_projects(&chat, 7)[0], "project-2");

        // Scroll the window first, so a successful click has to reset it.
        chat.handle_mouse(mouse_in(MouseEventKind::ScrollDown, pane));
        assert!(chat.conversations_window_start.is_some());

        let action = chat.handle_mouse(mouse_at_row(
            MouseEventKind::Down(MouseButton::Left),
            pane,
            0,
        ));

        assert_eq!(
            action,
            ChatAction::SwitchSession {
                session_id: other_session_id(3)
            },
            "row 0 after the scroll is project-3 (session-3)"
        );
        assert!(
            chat.conversations_window_start.is_none(),
            "a click resets the window like keyboard selection does"
        );
        assert_eq!(
            chat.focus,
            ChatFocus::Prompt,
            "a click selects without taking focus, same as the wheel"
        );
    }

    #[test]
    fn clicking_the_current_conversation_line_is_a_no_op() {
        let mut chat = windowed_chat(10, 5);
        let _ = drawn_transcript(&mut chat, 60, 24);
        let pane = chat
            .conversations_area
            .expect("the pane records its hitbox");
        // The current session (position 5) sits at window row 3.
        assert_eq!(conversations_pane(&chat, 0, 80, 7).current_row, Some(3));

        let action = chat.handle_mouse(mouse_at_row(
            MouseEventKind::Down(MouseButton::Left),
            pane,
            3,
        ));

        assert_eq!(action, ChatAction::None);
        assert_eq!(chat.focus, ChatFocus::Prompt);
    }

    #[test]
    fn walking_the_conversation_list_recentres_a_wheel_scrolled_window() {
        let mut chat = windowed_chat(10, 5);
        chat.focus_conversations();
        let _ = drawn_transcript(&mut chat, 60, 24);
        let pane = chat
            .conversations_area
            .expect("the pane records its hitbox");
        chat.handle_mouse(mouse_in(MouseEventKind::ScrollUp, pane));
        assert_eq!(window_projects(&chat, 7)[0], "project-1");

        assert_eq!(
            chat.handle_key(key(KeyCode::Char('j'))),
            ChatAction::SwitchSession {
                session_id: other_session_id(6)
            }
        );

        assert!(chat.conversations_window_start.is_none());
        assert_eq!(window_projects(&chat, 7)[0], "project-2");
    }

    #[test]
    fn a_focused_conversations_pane_bands_its_row_and_leaves_the_cursor_out_of_the_composer() {
        use ratatui::backend::Backend as _;

        let mut chat = windowed_chat(3, 1);
        chat.focus_conversations();
        let mut terminal = Terminal::new(TestBackend::new(60, 24)).expect("terminal");

        terminal
            .draw(|frame| render(frame, &mut chat))
            .expect("draw chat");
        // The band only covers the pane's inner hitbox: the border columns to
        // either side keep drawing border characters, not the band.
        let pane = chat
            .conversations_area
            .expect("the pane records its hitbox");
        let banded = |terminal: &Terminal<TestBackend>, y: u16| {
            let buffer = terminal.backend().buffer();
            (pane.x..pane.right()).all(|x| buffer[(x, y)].bg == Color::DarkGray)
        };
        // Row 0 is the pane's top border; rows 1-3 are the three sessions,
        // with the current one (index 1 in the list) at y = 2.
        assert!(!banded(&terminal, 0), "the top border draws no band");
        assert!(!banded(&terminal, 1));
        assert!(banded(&terminal, 2), "the current session's row is banded");
        assert!(!banded(&terminal, 3));
        assert!(!banded(&terminal, 4), "the bottom border draws no band");
        // Nothing places a cursor while the list has focus, so it stays where
        // the terminal left it.
        terminal.backend_mut().assert_cursor_position((0, 0));

        chat.handle_key(key(KeyCode::Enter));
        terminal
            .draw(|frame| render(frame, &mut chat))
            .expect("draw chat");
        assert!(!banded(&terminal, 2), "an unfocused pane draws no band");
        let cursor = terminal
            .backend_mut()
            .get_cursor_position()
            .expect("cursor position");
        assert!(
            cursor.y > 4,
            "the prompt's cursor sits in the composer, below the bordered pane"
        );
    }

    #[test]
    fn the_conversations_pane_border_is_titled_active() {
        let mut chat = windowed_chat(3, 1);
        let mut terminal = Terminal::new(TestBackend::new(60, 24)).expect("terminal");

        terminal
            .draw(|frame| render(frame, &mut chat))
            .expect("draw chat");

        let buffer = terminal.backend().buffer();
        let top_border: String = (buffer.area.x..buffer.area.right())
            .map(|x| buffer[(x, 0)].symbol())
            .collect();
        assert!(
            top_border.contains("Active"),
            "the pane's border carries the title: {top_border:?}"
        );
    }

    #[test]
    fn focus_shows_as_a_double_border_and_tab_swaps_which_pane_has_it() {
        let mut chat = windowed_chat(3, 1);
        chat.focus_conversations();
        let mut terminal = Terminal::new(TestBackend::new(60, 24)).expect("terminal");

        terminal
            .draw(|frame| render(frame, &mut chat))
            .expect("draw chat");
        // Both panes sit at the left edge, so their top border's row is
        // identified by which corner glyph it draws, not a hardcoded y. The
        // right corner is read from the same row, clear of the title text
        // that starts right after the left one.
        let (pane_top, prompt_top, right) = {
            let buffer = terminal.backend().buffer();
            let pane_top = (buffer.area.y..buffer.area.bottom())
                .find(|&y| buffer[(0, y)].symbol() == "╔")
                .expect("the focused conversations pane draws a double border");
            let prompt_top = (buffer.area.y..buffer.area.bottom())
                .find(|&y| buffer[(0, y)].symbol() == "┌")
                .expect("the unfocused composer draws a single border");
            (pane_top, prompt_top, buffer.area.right() - 1)
        };
        let left_corner = |terminal: &Terminal<TestBackend>, y: u16| -> String {
            terminal.backend().buffer()[(0, y)].symbol().to_owned()
        };
        let right_corner = |terminal: &Terminal<TestBackend>, y: u16| -> String {
            terminal.backend().buffer()[(right, y)].symbol().to_owned()
        };
        assert_eq!(right_corner(&terminal, pane_top), "╗");
        assert_eq!(right_corner(&terminal, prompt_top), "┐");

        chat.handle_key(key(KeyCode::Tab));
        assert_eq!(chat.focus, ChatFocus::Prompt);
        terminal
            .draw(|frame| render(frame, &mut chat))
            .expect("draw chat");

        assert_eq!(
            left_corner(&terminal, pane_top),
            "┌",
            "focus left the pane, so its border goes single"
        );
        assert_eq!(right_corner(&terminal, pane_top), "┐");
        assert_eq!(
            left_corner(&terminal, prompt_top),
            "╔",
            "focus landed on the composer, so its border goes double"
        );
        assert_eq!(right_corner(&terminal, prompt_top), "╗");
    }

    #[test]
    fn session_header_applies_the_turn_band_to_the_whole_collapsed_line() {
        let chat = header_chat(
            "current",
            0,
            vec![other_session(1, "other", Some(1_000), "still going")],
        );

        let lines = conversations_pane(&chat, 1_125, 80, 10).lines;
        let tail = &lines[1];
        assert_eq!(tail.spans[0].content.as_ref(), "  00:02:05 still going");
        assert_eq!(tail.style.fg, Some(Color::Yellow));
    }

    #[test]
    fn session_header_truncates_each_line_to_the_available_width() {
        let chat = header_chat(
            "current",
            0,
            vec![other_session(1, "other", None, "a tail that will not fit")],
        );

        assert_eq!(
            header_text(&chat, 0, 20),
            ["› [idle]", "  [idle] a tail tha…"]
        );
    }

    #[test]
    fn other_session_activity_reads_the_turn_clock_and_last_agent_line() {
        let identity = OtherSessionIdentity {
            // The activity carries the id the pane switches to, so it is the
            // identity's, not the materialized session's.
            session_id: other_session_id(3),
            position: 3,
        };
        let mut session = MaterializedSession::empty("other");
        session.transcript = vec![
            agent_message_item("first", 3, "earlier answer"),
            agent_message_item("second", 5, "opening line\n\nclosing line\n"),
        ];

        assert_eq!(
            other_session_activity(&identity, &session),
            other_session(3, "api-fix", None, "closing line")
        );

        session.execution = MaterializedExecutionState::Running {
            started_at_ms: 7_000,
        };
        assert_eq!(
            other_session_activity(&identity, &session),
            other_session(3, "api-fix", Some(7), "closing line")
        );
    }

    #[test]
    fn current_session_header_line_follows_the_materialized_turn_and_transcript() {
        let mut chat = header_chat("current", 0, Vec::new());
        let mut session = MaterializedSession::empty("1234567890");
        session.applied_event_ordinal = 5;
        session.transcript = vec![
            agent_message_item("first", 3, "earlier answer"),
            agent_message_item("second", 5, "opening line\nclosing line"),
        ];
        session.execution = MaterializedExecutionState::Running {
            started_at_ms: 60_000,
        };
        chat.apply_materialized(&session, &[], &[]);

        assert_eq!(header_text(&chat, 125, 80), ["› 00:01:05 closing line"]);

        session.applied_event_ordinal = 6;
        session.execution = MaterializedExecutionState::Idle;
        chat.apply_materialized(&session, &[], &[]);
        assert_eq!(header_text(&chat, 125, 80), ["› [idle] closing line"]);
    }

    #[test]
    fn chat_view_draws_one_header_row_per_session_above_the_transcript() {
        let mut chat = header_chat("current", 0, Vec::new());
        let mut terminal = Terminal::new(TestBackend::new(100, 24)).expect("terminal");
        let row = |terminal: &Terminal<TestBackend>, offset: u16| {
            let buffer = terminal.backend().buffer();
            (buffer.area.x..buffer.area.right())
                .map(|x| buffer[(x, buffer.area.y + offset)].symbol())
                .collect::<String>()
        };
        // Strips exactly the pane's left/right border columns (not the
        // caret's own leading spaces), so a row's session text compares the
        // same way it did before the border wrapped it.
        let bordered_row = |terminal: &Terminal<TestBackend>, offset: u16| -> String {
            let full = row(terminal, offset);
            let mut chars = full.chars();
            chars.next();
            chars.next_back();
            chars.as_str().trim_end().to_owned()
        };

        terminal
            .draw(|frame| render(frame, &mut chat))
            .expect("draw chat");
        // Row 0 is the pane's own top border; the header row sits inside it.
        assert_eq!(bordered_row(&terminal, 1), "› [idle]");
        // Row 2 is the pane's bottom border, so the transcript's chrome
        // starts at row 3.
        assert!(row(&terminal, 3).contains("Conversation"));

        apply_chat_io_update(
            &mut chat,
            ChatIoUpdate::OtherSessions(vec![other_session(1, "docs", None, "wrote the guide")]),
        );
        terminal
            .draw(|frame| render(frame, &mut chat))
            .expect("draw chat");
        assert_eq!(bordered_row(&terminal, 1), "› [idle]");
        assert_eq!(bordered_row(&terminal, 2), "  [idle] wrote the guide");
        // The transcript keeps its own chrome, below the header and the
        // pane's now-taller bottom border.
        assert!(row(&terminal, 4).contains("Conversation"));
    }

    /// A conversation long enough that opening it converts the tail only.
    fn long_session() -> MaterializedSession {
        let mut session = MaterializedSession::empty("session-long");
        session.transcript = (1..=300)
            .map(|position| {
                agent_message_item(
                    &format!("agent:{position}"),
                    position,
                    &format!("message {position}"),
                )
            })
            .collect();
        session.applied_event_ordinal = 301;
        session
    }

    #[test]
    fn the_converted_history_completes_a_chat_opened_on_its_tail() {
        let session = long_session();
        let mut chat = ChatState::from_materialized_tail(&session, &[], &[]);
        let pending = chat.unconverted_prefix();
        assert!(pending > 0);
        let prefix = materialized_prefix_entries(
            &session.transcript[..pending],
            session.applied_event_ordinal,
        );

        let rebuild = apply_chat_io_update(
            &mut chat,
            ChatIoUpdate::TranscriptPrefix {
                attempt: 1,
                result: Ok((prefix, Vec::new())),
            },
        );

        assert_eq!(rebuild, PrefixRebuild::NotNeeded);
        assert_eq!(chat.unconverted_prefix(), 0);
        assert_eq!(chat.entries.len(), session.transcript.len());
        assert_eq!(chat.entries[0].text, "message 1");
        assert_eq!(chat.notice(), None);
    }

    #[test]
    fn history_that_no_longer_fits_the_tail_is_rebuilt_and_then_gives_up_with_a_notice() {
        let session = long_session();
        let mut chat = ChatState::from_materialized_tail(&session, &[], &[]);
        let pending = chat.unconverted_prefix();
        // History from a transcript compaction rewrote: it overlaps the tail.
        let stale = materialized_prefix_entries(
            &session.transcript[session.transcript.len() - pending..],
            session.applied_event_ordinal,
        );

        let rebuild = apply_chat_io_update(
            &mut chat,
            ChatIoUpdate::TranscriptPrefix {
                attempt: 1,
                result: Ok((stale.clone(), Vec::new())),
            },
        );

        assert_eq!(rebuild, PrefixRebuild::Needed { attempt: 2 });
        assert_eq!(chat.unconverted_prefix(), pending);
        assert_eq!(chat.notice(), None);

        let exhausted = apply_chat_io_update(
            &mut chat,
            ChatIoUpdate::TranscriptPrefix {
                attempt: MAX_PREFIX_CONVERSION_ATTEMPTS,
                result: Ok((stale, Vec::new())),
            },
        );

        assert_eq!(exhausted, PrefixRebuild::NotNeeded);
        assert_eq!(chat.unconverted_prefix(), pending);
        assert!(
            chat.notice()
                .is_some_and(|notice| notice.contains("Earlier messages")),
            "giving up on the history has to be reported"
        );
    }

    #[test]
    fn a_failed_history_conversion_is_reported_instead_of_dropped() {
        let session = long_session();
        let mut chat = ChatState::from_materialized_tail(&session, &[], &[]);

        let rebuild = apply_chat_io_update(
            &mut chat,
            ChatIoUpdate::TranscriptPrefix {
                attempt: 1,
                result: Err("worker panicked".into()),
            },
        );

        assert_eq!(rebuild, PrefixRebuild::NotNeeded);
        assert_eq!(
            chat.notice().as_deref(),
            Some("Earlier messages failed to load: worker panicked")
        );
    }
}
