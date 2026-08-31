//! Pure state for the workspace-level advisory manager.
//!
//! The manager classifies projections the dashboard already owns and turns
//! keys into ordinary `DashboardAction`s. Model and lifecycle I/O remain in
//! the CLI dashboard.

use std::time::{SystemTime, UNIX_EPOCH};

use chrono::DateTime;
use crossterm::event::{KeyCode, KeyEvent};
use hel::hel_state::{SessionRecord, SessionState};
use hel::hel_text_input::TextInput;

use crate::dialogs::{ConfirmDialog, Confirmation};
use crate::{DashboardAction, DashboardState, Mode, move_index};

pub const IDLE_STOP_AFTER_SECONDS: u64 = 2 * 60 * 60;
pub const ARCHIVE_CLEANUP_AFTER_SECONDS: u64 = 30 * 24 * 60 * 60;
const MANAGER_PROMPT_CHARS: usize = 4 * 1024;
const MANAGER_CONTEXT_BYTES: usize = 24 * 1024;
const MANAGER_CONTEXT_MESSAGES: usize = 12;
const SESSION_TRANSCRIPT_TAIL_LINES: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ManagerFocus {
    Sessions,
    Transcript,
    Prompt,
}

impl ManagerFocus {
    fn next(self, reverse: bool) -> Self {
        match (self, reverse) {
            (Self::Sessions, false) | (Self::Prompt, true) => Self::Transcript,
            (Self::Transcript, false) | (Self::Sessions, true) => Self::Prompt,
            (Self::Prompt, false) | (Self::Transcript, true) => Self::Sessions,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ManagerMessageRole {
    User,
    Manager,
    System,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManagerMessage {
    pub(crate) role: ManagerMessageRole,
    pub(crate) text: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ManagerRecommendation {
    Stop,
    Destroy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ManagerSessionStatus {
    Working,
    Queued(usize),
    Idle,
    Ready,
    Unreachable,
    NeedsAttention,
    Inactive,
    Archived,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManagerSessionRow {
    pub(crate) session_id: String,
    pub(crate) title: String,
    pub(crate) project: String,
    pub(crate) profile: String,
    pub(crate) target: String,
    pub(crate) status: ManagerSessionStatus,
    pub(crate) age_seconds: Option<u64>,
    pub(crate) recommendation: Option<ManagerRecommendation>,
    pub(crate) active: bool,
    pub(crate) archived: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagerQuery {
    pub request_id: u64,
    pub prompt: String,
    pub model_prompt: String,
    pub backend_session_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManagerState {
    pub(crate) messages: Vec<ManagerMessage>,
    pub(crate) input: TextInput,
    pub(crate) focus: ManagerFocus,
    pub(crate) session_index: usize,
    pub(crate) transcript_scroll: usize,
    pub(crate) in_flight: Option<u64>,
    pub(crate) last_provider: Option<String>,
    next_request_id: u64,
}

impl Default for ManagerState {
    fn default() -> Self {
        Self {
            messages: vec![ManagerMessage {
                role: ManagerMessageRole::System,
                text: "I can assess idle sessions, summarize recent work, and point out safe cleanup candidates. Lifecycle changes always require your explicit dashboard confirmation."
                    .into(),
            }],
            input: TextInput::new().with_max_chars(MANAGER_PROMPT_CHARS),
            focus: ManagerFocus::Sessions,
            session_index: 0,
            transcript_scroll: 0,
            in_flight: None,
            last_provider: None,
            next_request_id: 1,
        }
    }
}

impl DashboardState {
    pub(crate) fn clamp_manager_selection(&mut self) {
        self.manager.session_index = self
            .manager
            .session_index
            .min(self.state.sessions.len().saturating_sub(1));
    }

    pub(crate) fn open_manager(&mut self) {
        self.clamp_manager_selection();
        self.mode = Mode::Manager;
    }

    pub(crate) fn manager_rows(&self) -> Vec<ManagerSessionRow> {
        self.manager_rows_at(now_epoch_seconds())
    }

    fn manager_rows_at(&self, now_seconds: u64) -> Vec<ManagerSessionRow> {
        let mut sessions = self.state.sessions.values().collect::<Vec<_>>();
        sessions.sort_by(|left, right| left.compare_by_creation(right));
        sessions
            .into_iter()
            .map(|session| self.assess_manager_session(session, now_seconds))
            .collect()
    }

    fn assess_manager_session(
        &self,
        session: &SessionRecord,
        now_seconds: u64,
    ) -> ManagerSessionRow {
        let detail = self.session_details.get(&session.id);
        let active = session.state.is_active();
        let age_seconds = if active {
            detail
                .and_then(|detail| detail.last_activity_at_ms)
                .map(|milliseconds| now_seconds.saturating_sub(milliseconds / 1_000))
        } else {
            timestamp_seconds(&session.updated_at)
                .and_then(|updated| u64::try_from(updated).ok())
                .map(|updated| now_seconds.saturating_sub(updated))
        };
        let queued = detail.map_or(0, |detail| detail.queued_prompts.len());
        let working = detail.is_some_and(|detail| detail.current_turn_started_at.is_some());
        let operation = self.session_operations.contains_key(&session.id);
        let unreachable = self.unreachable_sessions.contains(&session.id);
        let (status, recommendation) = if !active {
            let cleanup = session.archived
                && age_seconds.is_some_and(|age| age >= ARCHIVE_CLEANUP_AFTER_SECONDS);
            (
                if session.archived {
                    ManagerSessionStatus::Archived
                } else {
                    ManagerSessionStatus::Inactive
                },
                cleanup.then_some(ManagerRecommendation::Destroy),
            )
        } else if matches!(session.state, SessionState::Error) {
            (ManagerSessionStatus::NeedsAttention, None)
        } else if unreachable || matches!(session.state, SessionState::Disconnected) {
            (ManagerSessionStatus::Unreachable, None)
        } else if operation || working {
            (ManagerSessionStatus::Working, None)
        } else if queued > 0 {
            (ManagerSessionStatus::Queued(queued), None)
        } else if session.state == SessionState::Running
            && age_seconds.is_some_and(|age| age >= IDLE_STOP_AFTER_SECONDS)
        {
            (
                ManagerSessionStatus::Idle,
                Some(ManagerRecommendation::Stop),
            )
        } else {
            (ManagerSessionStatus::Ready, None)
        };
        ManagerSessionRow {
            session_id: session.id.clone(),
            title: session.display_title().to_owned(),
            project: session.project_name(&self.config),
            profile: session.last_profile.clone(),
            target: session.target_template_id.clone(),
            status,
            age_seconds,
            recommendation,
            active,
            archived: session.archived,
        }
    }

    pub(crate) fn handle_manager_key(&mut self, key: KeyEvent) -> DashboardAction {
        match key.code {
            KeyCode::Esc => {
                self.mode = Mode::Dashboard;
                return DashboardAction::None;
            }
            KeyCode::Tab => {
                self.manager.focus = self.manager.focus.next(false);
                return DashboardAction::None;
            }
            KeyCode::BackTab => {
                self.manager.focus = self.manager.focus.next(true);
                return DashboardAction::None;
            }
            _ => {}
        }
        match self.manager.focus {
            ManagerFocus::Sessions => self.handle_manager_sessions_key(key),
            ManagerFocus::Transcript => {
                match key.code {
                    KeyCode::Up | KeyCode::Char('k') => {
                        self.manager.transcript_scroll =
                            self.manager.transcript_scroll.saturating_add(1);
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        self.manager.transcript_scroll =
                            self.manager.transcript_scroll.saturating_sub(1);
                    }
                    KeyCode::End => self.manager.transcript_scroll = 0,
                    _ => {}
                }
                DashboardAction::None
            }
            ManagerFocus::Prompt => {
                if key.code == KeyCode::Enter {
                    return self.submit_manager_prompt();
                }
                self.manager.input.handle_key(key);
                DashboardAction::None
            }
        }
    }

    fn handle_manager_sessions_key(&mut self, key: KeyEvent) -> DashboardAction {
        let rows = self.manager_rows();
        match key.code {
            KeyCode::Up | KeyCode::Char('k') => {
                move_index(&mut self.manager.session_index, rows.len(), -1);
                DashboardAction::None
            }
            KeyCode::Down | KeyCode::Char('j') => {
                move_index(&mut self.manager.session_index, rows.len(), 1);
                DashboardAction::None
            }
            KeyCode::Char('s') => {
                let Some(row) = rows.get(self.manager.session_index) else {
                    return DashboardAction::None;
                };
                if row.recommendation != Some(ManagerRecommendation::Stop) {
                    self.set_notice("Only sessions marked idle are offered for manager stop.");
                    return DashboardAction::None;
                }
                let reviewer_conversation = self.sessions_with_review.contains(&row.session_id);
                self.mode = Mode::Confirm(ConfirmDialog::new(Confirmation::Close {
                    session_id: row.session_id.clone(),
                    reviewer_conversation,
                }));
                DashboardAction::None
            }
            KeyCode::Char('d') => {
                let Some(row) = rows.get(self.manager.session_index) else {
                    return DashboardAction::None;
                };
                if row.recommendation != Some(ManagerRecommendation::Destroy) {
                    self.set_notice("Only old archived sessions are offered for manager cleanup.");
                    return DashboardAction::None;
                }
                self.mode = Mode::Confirm(ConfirmDialog::new(Confirmation::DestroyStopped {
                    session_id: row.session_id.clone(),
                    reopen: None,
                }));
                DashboardAction::None
            }
            KeyCode::Char('a') => {
                let Some(row) = rows.get(self.manager.session_index) else {
                    return DashboardAction::None;
                };
                if row.active {
                    self.set_notice("Stop a live session before archiving it.");
                    return DashboardAction::None;
                }
                let archived = !row.archived;
                let session_id = row.session_id.clone();
                self.set_session_archived(&session_id, archived);
                DashboardAction::SetSessionArchived {
                    session_id,
                    archived,
                }
            }
            _ => DashboardAction::None,
        }
    }

    fn submit_manager_prompt(&mut self) -> DashboardAction {
        if self.manager.in_flight.is_some() {
            self.set_notice("The dashboard manager is still answering.");
            return DashboardAction::None;
        }
        let prompt = self.manager.input.value().trim().to_owned();
        if prompt.is_empty() {
            self.set_notice("Enter a question for the dashboard manager.");
            return DashboardAction::None;
        }
        self.manager.input.clear();
        self.manager.messages.push(ManagerMessage {
            role: ManagerMessageRole::User,
            text: prompt.clone(),
        });
        self.manager.transcript_scroll = 0;
        let backend_session_ids = self.manager_backend_session_ids();
        if backend_session_ids.is_empty() {
            self.manager.messages.push(ManagerMessage {
                role: ManagerMessageRole::System,
                text: "No connected, idle session is available to provide a scratch model. Wait for a turn and its queue to finish, then retry."
                    .into(),
            });
            return DashboardAction::None;
        }
        let request_id = self.manager.next_request_id;
        self.manager.next_request_id = self.manager.next_request_id.wrapping_add(1).max(1);
        self.manager.in_flight = Some(request_id);
        let model_prompt = self.manager_model_prompt(&prompt);
        DashboardAction::AskManager(ManagerQuery {
            request_id,
            prompt,
            model_prompt,
            backend_session_ids,
        })
    }

    fn manager_backend_session_ids(&self) -> Vec<String> {
        let rows = self.manager_rows();
        let selected = rows
            .get(self.manager.session_index)
            .map(|row| row.session_id.clone());
        let mut candidates = rows
            .into_iter()
            .filter(|row| {
                row.active
                    && matches!(
                        row.status,
                        ManagerSessionStatus::Ready | ManagerSessionStatus::Idle
                    )
                    && !self.unreachable_sessions.contains(&row.session_id)
                    && !self.session_operations.contains_key(&row.session_id)
            })
            .map(|row| row.session_id)
            .collect::<Vec<_>>();
        candidates.sort_by_key(|session_id| usize::from(Some(session_id) != selected.as_ref()));
        candidates
    }

    fn manager_model_prompt(&self, prompt: &str) -> String {
        let mut context = format!(
            "You are Hel's advisory dashboard manager. Analyze only the supplied redacted inventory and recent conversation. Never claim that you stopped, archived, destroyed, resumed, or messaged a session. Those actions require explicit confirmation in the dashboard. Distinguish observed facts from suggestions. Be concise and identify sessions by title and short Hel id.\n\nCURRENT REQUEST\n{}\n\nSESSION INVENTORY\n",
            bounded_line(prompt, MANAGER_PROMPT_CHARS),
        );
        for row in self.manager_rows() {
            let short_id = row.session_id.chars().take(8).collect::<String>();
            let status = manager_status_label(row.status);
            let age = row
                .age_seconds
                .map(format_age)
                .unwrap_or_else(|| "unknown".into());
            let recommendation = match row.recommendation {
                Some(ManagerRecommendation::Stop) => "stop candidate",
                Some(ManagerRecommendation::Destroy) => "cleanup candidate",
                None => "none",
            };
            push_bounded(
                &mut context,
                &format!(
                    "- {} [{}]: status={status}; project={}; profile={}; target={}; last_activity_age={age}; recommendation={recommendation}\n",
                    single_line(&row.title),
                    short_id,
                    single_line(&row.project),
                    single_line(&row.profile),
                    single_line(&row.target),
                ),
            );
            if let Some(detail) = self.session_details.get(&row.session_id) {
                let recent = detail
                    .transcript
                    .as_ref()
                    .map(|transcript| transcript.browser_tail(SESSION_TRANSCRIPT_TAIL_LINES))
                    .unwrap_or_else(|| {
                        [
                            detail
                                .last_user_message
                                .as_deref()
                                .map(|text| format!("You: {text}")),
                            detail
                                .last_agent_message
                                .as_deref()
                                .map(|text| format!("Agent: {text}")),
                        ]
                        .into_iter()
                        .flatten()
                        .collect()
                    });
                for line in recent {
                    push_bounded(&mut context, &format!("    {}\n", bounded_line(&line, 500)));
                }
            }
        }
        push_bounded(&mut context, "\nMANAGER CONVERSATION\n");
        let start = self
            .manager
            .messages
            .len()
            .saturating_sub(MANAGER_CONTEXT_MESSAGES);
        for message in &self.manager.messages[start..] {
            let role = match message.role {
                ManagerMessageRole::User => "User",
                ManagerMessageRole::Manager => "Manager",
                ManagerMessageRole::System => "System",
            };
            push_bounded(
                &mut context,
                &format!("{role}: {}\n", bounded_line(&message.text, 2_000)),
            );
        }
        push_bounded(
            &mut context,
            &format!("\nAnswer the current request: {prompt}"),
        );
        context
    }

    pub fn apply_manager_reply(
        &mut self,
        request_id: u64,
        source_session_id: Option<String>,
        result: Result<String, String>,
    ) {
        if self.manager.in_flight != Some(request_id) {
            return;
        }
        self.manager.in_flight = None;
        self.manager.last_provider = source_session_id;
        let (role, text) = match result {
            Ok(answer) if !answer.trim().is_empty() => (
                ManagerMessageRole::Manager,
                sanitize_manager_text(answer.trim()),
            ),
            Ok(_) => (
                ManagerMessageRole::System,
                "The scratch model returned an empty answer.".into(),
            ),
            Err(error) => (
                ManagerMessageRole::System,
                format!("The dashboard manager could not answer: {error}"),
            ),
        };
        self.manager.messages.push(ManagerMessage { role, text });
        self.manager.transcript_scroll = 0;
    }
}

fn now_epoch_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn timestamp_seconds(timestamp: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(timestamp)
        .ok()
        .map(|timestamp| timestamp.timestamp())
}

pub(crate) fn manager_status_label(status: ManagerSessionStatus) -> String {
    match status {
        ManagerSessionStatus::Working => "working".into(),
        ManagerSessionStatus::Queued(count) => format!("{count} queued"),
        ManagerSessionStatus::Idle => "idle · stop candidate".into(),
        ManagerSessionStatus::Ready => "ready".into(),
        ManagerSessionStatus::Unreachable => "unreachable".into(),
        ManagerSessionStatus::NeedsAttention => "needs attention".into(),
        ManagerSessionStatus::Inactive => "stopped".into(),
        ManagerSessionStatus::Archived => "archived".into(),
    }
}

pub(crate) fn format_age(seconds: u64) -> String {
    if seconds >= 24 * 60 * 60 {
        format!("{}d", seconds / (24 * 60 * 60))
    } else if seconds >= 60 * 60 {
        format!("{}h", seconds / (60 * 60))
    } else if seconds >= 60 {
        format!("{}m", seconds / 60)
    } else {
        format!("{seconds}s")
    }
}

fn single_line(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn sanitize_manager_text(text: &str) -> String {
    text.chars()
        .filter(|character| !character.is_control() || matches!(character, '\n' | '\t'))
        .collect()
}

fn bounded_line(text: &str, maximum_chars: usize) -> String {
    let line = single_line(text);
    let mut chars = line.chars();
    let bounded = chars.by_ref().take(maximum_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{bounded}…")
    } else {
        bounded
    }
}

fn push_bounded(context: &mut String, addition: &str) {
    let remaining = MANAGER_CONTEXT_BYTES.saturating_sub(context.len());
    if remaining == 0 {
        return;
    }
    if addition.len() <= remaining {
        context.push_str(addition);
        return;
    }
    let mut end = remaining.min(addition.len());
    while !addition.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    context.push_str(&addition[..end]);
}

#[cfg(test)]
mod tests {
    use crossterm::event::{KeyEvent, KeyModifiers};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::test_support::{buffer_lines, dashboard_with_session, stopped_session};

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    #[test]
    fn old_projected_idle_session_is_a_stop_candidate() {
        let mut session = stopped_session();
        session.state = SessionState::Running;
        let mut dashboard = dashboard_with_session(session);
        dashboard
            .session_details
            .get_mut("session-1")
            .unwrap()
            .last_activity_at_ms = Some(1_000_000);

        let row = dashboard.assess_manager_session(
            &dashboard.state.sessions["session-1"],
            1_000 + IDLE_STOP_AFTER_SECONDS,
        );

        assert_eq!(row.status, ManagerSessionStatus::Idle);
        assert_eq!(row.recommendation, Some(ManagerRecommendation::Stop));
    }

    #[test]
    fn missing_activity_never_recommends_stopping() {
        let mut session = stopped_session();
        session.state = SessionState::Running;
        let dashboard = dashboard_with_session(session);

        let row = dashboard.assess_manager_session(
            &dashboard.state.sessions["session-1"],
            IDLE_STOP_AFTER_SECONDS * 2,
        );

        assert_eq!(row.status, ManagerSessionStatus::Ready);
        assert_eq!(row.recommendation, None);
    }

    #[test]
    fn working_and_queued_sessions_are_not_model_providers() {
        let mut session = stopped_session();
        session.state = SessionState::Running;
        let mut dashboard = dashboard_with_session(session);
        dashboard
            .session_details
            .get_mut("session-1")
            .unwrap()
            .current_turn_started_at = Some(10);

        assert!(dashboard.manager_backend_session_ids().is_empty());

        let detail = dashboard.session_details.get_mut("session-1").unwrap();
        detail.current_turn_started_at = None;
        detail.queued_prompts.push(hel::hel_worker::QueuedPrompt {
            id: "queued-1".into(),
            text: "continue later".into(),
            attachments: Vec::new(),
            created_at_ms: 10,
        });

        assert!(dashboard.manager_backend_session_ids().is_empty());
    }

    #[test]
    fn old_archive_is_a_cleanup_candidate() {
        let mut session = stopped_session();
        session.archived = true;
        let mut dashboard = dashboard_with_session(session);
        dashboard
            .state
            .sessions
            .get_mut("session-1")
            .unwrap()
            .updated_at = "2026-01-01T00:00:00Z".into();
        let now = u64::try_from(
            DateTime::parse_from_rfc3339("2026-02-01T00:00:00Z")
                .unwrap()
                .timestamp(),
        )
        .unwrap();

        let row = dashboard.assess_manager_session(&dashboard.state.sessions["session-1"], now);

        assert_eq!(row.recommendation, Some(ManagerRecommendation::Destroy));
    }

    #[test]
    fn archive_with_invalid_timestamp_is_not_a_cleanup_candidate() {
        let mut session = stopped_session();
        session.archived = true;
        let mut dashboard = dashboard_with_session(session);
        dashboard
            .state
            .sessions
            .get_mut("session-1")
            .unwrap()
            .updated_at = "not-a-timestamp".into();

        let row = dashboard.assess_manager_session(
            &dashboard.state.sessions["session-1"],
            ARCHIVE_CLEANUP_AFTER_SECONDS * 2,
        );

        assert_eq!(row.age_seconds, None);
        assert_eq!(row.recommendation, None);
    }

    #[test]
    fn manager_prompt_becomes_a_bounded_background_action() {
        let mut session = stopped_session();
        session.state = SessionState::Running;
        let mut dashboard = dashboard_with_session(session);
        dashboard.open_manager();
        dashboard.manager.focus = ManagerFocus::Prompt;
        dashboard.manager.input.push_str("Summarize the work");

        let DashboardAction::AskManager(query) = dashboard.handle_key(key(KeyCode::Enter)) else {
            panic!("manager prompt did not request background work");
        };

        assert_eq!(query.prompt, "Summarize the work");
        assert_eq!(query.backend_session_ids, ["session-1"]);
        assert!(query.model_prompt.len() <= MANAGER_CONTEXT_BYTES);
        assert!(
            query
                .model_prompt
                .contains("CURRENT REQUEST\nSummarize the work")
        );
        assert_eq!(dashboard.manager.in_flight, Some(query.request_id));
    }

    #[test]
    fn paste_populates_the_manager_prompt() {
        let mut dashboard = dashboard_with_session(stopped_session());
        dashboard.open_manager();
        dashboard.manager.focus = ManagerFocus::Prompt;

        dashboard.handle_paste("summarize all\nsessions");

        assert_eq!(dashboard.manager.input.value(), "summarize all sessions");
    }

    #[test]
    fn stop_key_uses_the_existing_confirmation_for_idle_candidates() {
        let mut session = stopped_session();
        session.state = SessionState::Running;
        let mut dashboard = dashboard_with_session(session);
        dashboard
            .session_details
            .get_mut("session-1")
            .unwrap()
            .last_activity_at_ms = Some(1);
        dashboard.open_manager();

        assert_eq!(
            dashboard.handle_manager_sessions_key(key(KeyCode::Char('s'))),
            DashboardAction::None
        );
        assert!(matches!(
            dashboard.mode,
            Mode::Confirm(ConfirmDialog {
                confirmation: Confirmation::Close { .. },
                ..
            })
        ));
    }

    #[test]
    fn archive_key_is_an_optimistic_typed_action() {
        let mut dashboard = dashboard_with_session(stopped_session());
        dashboard.open_manager();

        assert_eq!(
            dashboard.handle_manager_sessions_key(key(KeyCode::Char('a'))),
            DashboardAction::SetSessionArchived {
                session_id: "session-1".into(),
                archived: true,
            }
        );
        assert!(dashboard.state.sessions["session-1"].archived);
    }

    #[test]
    fn manager_view_renders_assessment_transcript_and_prompt() {
        let mut dashboard = dashboard_with_session(stopped_session());
        dashboard.open_manager();
        let mut terminal = Terminal::new(TestBackend::new(120, 28)).unwrap();

        terminal
            .draw(|frame| crate::render::render(frame, &mut dashboard))
            .unwrap();
        let rendered = buffer_lines(terminal.backend().buffer()).join("\n");

        assert!(rendered.contains("Dashboard manager"));
        assert!(rendered.contains("Sessions · 1"));
        assert!(rendered.contains("Manager transcript"));
        assert!(rendered.contains("Prompt · Enter to send"));
    }

    #[test]
    fn manager_reply_is_retained_after_the_view_closes() {
        let mut dashboard = dashboard_with_session(stopped_session());
        dashboard.manager.in_flight = Some(4);
        dashboard.mode = Mode::Dashboard;

        dashboard.apply_manager_reply(4, Some("session-1".into()), Ok("All clear".into()));

        assert_eq!(
            dashboard.manager.messages.last().unwrap(),
            &ManagerMessage {
                role: ManagerMessageRole::Manager,
                text: "All clear".into(),
            }
        );
        assert_eq!(
            dashboard.manager.last_provider.as_deref(),
            Some("session-1")
        );
    }

    #[test]
    fn stale_manager_reply_does_not_replace_a_newer_turn() {
        let mut dashboard = dashboard_with_session(stopped_session());
        dashboard.manager.in_flight = Some(5);
        let before = dashboard.manager.messages.len();

        dashboard.apply_manager_reply(4, None, Ok("stale".into()));

        assert_eq!(dashboard.manager.messages.len(), before);
        assert_eq!(dashboard.manager.in_flight, Some(5));
    }
}
