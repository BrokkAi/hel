//! Minimal full-screen chat for one persistent Hel worker.

use std::io;
use std::time::Duration;

use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::Frame;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};

use crate::hel_acp::RuntimeEvent;
use crate::hel_worker::{SequencedEvent, WorkerEvent, WorkerPhase, WorkerSnapshot};
use crate::hel_worker_client::WorkerClient;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatExit {
    Detached,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChatAction {
    None,
    Prompt(String),
    Cancel,
    Checkpoint,
    ToggleVoice,
    Back,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatRole {
    User,
    Agent,
    /// Agent reasoning stream, rendered dimmed.
    Thought,
    /// Tool invocation titles.
    Tool,
    System,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatEntry {
    pub seq: u64,
    pub role: ChatRole,
    pub text: String,
}

pub struct ChatState {
    session_id: String,
    phase: WorkerPhase,
    latest_seq: u64,
    entries: Vec<ChatEntry>,
    input: String,
    scroll_back: u16,
    notice: Option<String>,
    voice_active: bool,
}

impl ChatState {
    pub fn new(snapshot: &WorkerSnapshot, events: &[SequencedEvent]) -> Self {
        let mut state = Self {
            session_id: snapshot.session_id.clone(),
            phase: snapshot.phase,
            latest_seq: 0,
            entries: Vec::new(),
            input: String::new(),
            scroll_back: 0,
            notice: None,
            voice_active: false,
        };
        state.apply_events(events);
        state.latest_seq = state.latest_seq.max(snapshot.latest_seq);
        state
    }

    pub fn phase(&self) -> WorkerPhase {
        self.phase
    }

    pub fn latest_seq(&self) -> u64 {
        self.latest_seq
    }

    pub fn entries(&self) -> &[ChatEntry] {
        &self.entries
    }

    pub fn set_notice(&mut self, notice: impl Into<String>) {
        self.notice = Some(notice.into());
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

    pub fn handle_key(&mut self, key: KeyEvent) -> ChatAction {
        if key.kind != KeyEventKind::Press && key.kind != KeyEventKind::Repeat {
            return ChatAction::None;
        }
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            return if self.phase == WorkerPhase::Running {
                ChatAction::Cancel
            } else {
                ChatAction::Back
            };
        }
        match key.code {
            KeyCode::Esc => ChatAction::Back,
            KeyCode::Enter if key.modifiers.contains(KeyModifiers::SHIFT) => {
                self.input.push('\n');
                ChatAction::None
            }
            KeyCode::Enter => {
                let prompt = self.input.trim().to_owned();
                if prompt.is_empty() || self.phase != WorkerPhase::Idle {
                    ChatAction::None
                } else {
                    self.input.clear();
                    ChatAction::Prompt(prompt)
                }
            }
            KeyCode::Backspace => {
                self.input.pop();
                ChatAction::None
            }
            KeyCode::Char('p') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                ChatAction::Checkpoint
            }
            KeyCode::Char('v') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                ChatAction::ToggleVoice
            }
            KeyCode::Char(character)
                if !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
            {
                self.input.push(character);
                ChatAction::None
            }
            KeyCode::PageUp => {
                self.scroll_back = self.scroll_back.saturating_add(8);
                ChatAction::None
            }
            KeyCode::PageDown => {
                self.scroll_back = self.scroll_back.saturating_sub(8);
                ChatAction::None
            }
            KeyCode::End => {
                self.scroll_back = 0;
                ChatAction::None
            }
            _ => ChatAction::None,
        }
    }

    fn apply_event(&mut self, event: &SequencedEvent) {
        match &event.event {
            WorkerEvent::PromptAccepted { text, .. } => {
                self.phase = WorkerPhase::Running;
                self.entries.push(ChatEntry {
                    seq: event.seq,
                    role: ChatRole::User,
                    text: text.clone(),
                });
            }
            WorkerEvent::TurnCompleted | WorkerEvent::Cancelled => {
                self.phase = WorkerPhase::Idle;
            }
            WorkerEvent::Closing => self.phase = WorkerPhase::Closing,
            WorkerEvent::Closed => self.phase = WorkerPhase::Closed,
            WorkerEvent::Checkpointed { reason } => self.entries.push(ChatEntry {
                seq: event.seq,
                role: ChatRole::System,
                text: reason.as_deref().map_or_else(
                    || "checkpoint created".into(),
                    |reason| format!("checkpoint: {reason}"),
                ),
            }),
            WorkerEvent::Adapter { payload, .. } => self.apply_adapter(event.seq, payload),
            WorkerEvent::ConfigChanged { .. } => {}
        }
    }

    fn apply_adapter(&mut self, seq: u64, payload: &serde_json::Value) {
        let Ok(runtime) = serde_json::from_value::<RuntimeEvent>(payload.clone()) else {
            return;
        };
        match runtime {
            RuntimeEvent::SessionUpdate { update } => self.apply_session_update(seq, &update),
            RuntimeEvent::Warning { message } => self.entries.push(ChatEntry {
                seq,
                role: ChatRole::System,
                text: format!("warning: {message}"),
            }),
            RuntimeEvent::SessionStarted { resumed, .. } => self.entries.push(ChatEntry {
                seq,
                role: ChatRole::System,
                text: if resumed {
                    "harness session resumed".into()
                } else {
                    "harness session started".into()
                },
            }),
            _ => {}
        }
    }

    /// Project one ACP `session/update` notification into the transcript.
    /// Streamed message and thought chunks coalesce into the previous entry of
    /// the same role so tokens don't each become their own transcript line.
    fn apply_session_update(&mut self, seq: u64, update: &serde_json::Value) {
        let Some(object) = update.as_object() else {
            return;
        };
        for (kind, body) in [
            ("agent_message_chunk", ChatRole::Agent),
            ("agent_thought_chunk", ChatRole::Thought),
        ] {
            if let Some(chunk) = object.get(kind) {
                let text = chunk
                    .get("content")
                    .and_then(|content| content.get("text"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default();
                if !text.is_empty() {
                    self.push_streamed(seq, body, text);
                }
                return;
            }
        }
        if let Some(call) = object.get("tool_call") {
            let title = call
                .get("title")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("tool call");
            self.entries.push(ChatEntry {
                seq,
                role: ChatRole::Tool,
                text: title.to_owned(),
            });
            return;
        }
        if object.contains_key("tool_call_update")
            || object.contains_key("plan")
            || object.contains_key("available_commands_update")
            || object.contains_key("current_mode_update")
        {
            return;
        }
        // Unknown update shapes keep the permissive text projection.
        for text in extract_text(update) {
            self.entries.push(ChatEntry {
                seq,
                role: ChatRole::Agent,
                text,
            });
        }
    }

    fn push_streamed(&mut self, seq: u64, role: ChatRole, text: &str) {
        if let Some(last) = self.entries.last_mut()
            && last.role == role
        {
            last.seq = seq;
            last.text.push_str(text);
            return;
        }
        self.entries.push(ChatEntry {
            seq,
            role,
            text: text.to_owned(),
        });
    }
}

/// Run chat until the user presses Escape. This detaches the proxy and leaves
/// the target worker alive.
pub async fn run_chat(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    mut client: WorkerClient,
) -> Result<ChatExit> {
    let bootstrap = client.bootstrap().await?;
    let mut chat = ChatState::new(&bootstrap.snapshot, &bootstrap.events);
    let (voice_updates_tx, mut voice_updates_rx) =
        tokio::sync::mpsc::unbounded_channel::<VoiceUpdate>();
    let mut voice_cancel: Option<std::sync::mpsc::Sender<()>> = None;
    let mut voice_prefix = String::new();
    loop {
        while let Ok(update) = voice_updates_rx.try_recv() {
            match update {
                VoiceUpdate::Partial(text) => chat.input = append_dictation(&voice_prefix, &text),
                VoiceUpdate::Status(status) => chat.set_notice(status),
                VoiceUpdate::Finished(result) => {
                    chat.voice_active = false;
                    voice_cancel = None;
                    match result {
                        Ok(text) => {
                            chat.input = append_dictation(&voice_prefix, &text);
                            chat.notice = None;
                        }
                        Err(error) => {
                            chat.set_notice(crate::speech::dictation_error_message(&error))
                        }
                    }
                }
            }
        }
        terminal.draw(|frame| render(frame, &chat))?;
        if event::poll(Duration::from_millis(150))?
            && let Event::Key(key) = event::read()?
        {
            let action = chat.handle_key(key);
            let result = match action {
                ChatAction::None => None,
                ChatAction::Prompt(text) => match client.prompt(text.clone(), Vec::new()).await {
                    Ok(_) => None,
                    Err(error) => {
                        chat.input = text;
                        Some(error)
                    }
                },
                ChatAction::Cancel => client.cancel().await.err(),
                ChatAction::Checkpoint => client
                    .checkpoint(Some("manual chat checkpoint".into()))
                    .await
                    .err(),
                ChatAction::ToggleVoice => {
                    if let Some(cancel) = voice_cancel.as_ref() {
                        let _ = cancel.send(());
                        chat.set_notice("Stopping voice dictation…");
                        None
                    } else if !crate::speech::voice_input_supported() {
                        chat.set_notice(
                            "Voice helper unavailable; install hel-voice-worker beside hel or set HEL_VOICE_WORKER",
                        );
                        None
                    } else {
                        let (cancel_tx, cancel_rx) = std::sync::mpsc::channel();
                        voice_cancel = Some(cancel_tx);
                        voice_prefix.clone_from(&chat.input);
                        chat.voice_active = true;
                        chat.set_notice("Listening… press Ctrl-V again to stop");
                        spawn_dictation(voice_updates_tx.clone(), cancel_rx);
                        None
                    }
                }
                ChatAction::Back => {
                    if let Some(cancel) = voice_cancel.take() {
                        let _ = cancel.send(());
                    }
                    client.detach().await?;
                    return Ok(ChatExit::Detached);
                }
            };
            if let Some(error) = result {
                chat.set_notice(format!("{error:#}"));
            }
        }
        match client.sync().await {
            Ok(events) => chat.apply_events(&events),
            Err(error) => chat.set_notice(format!("connection lost: {error:#}")),
        }
    }
}

pub fn render(frame: &mut Frame, chat: &ChatState) {
    let area = frame.area();
    let outer = Block::default()
        .borders(Borders::ALL)
        .title(format!(" HEL / {} ", short_id(&chat.session_id)))
        .border_style(Style::default().fg(Color::DarkGray));
    let inner = outer.inner(area);
    frame.render_widget(outer, area);
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(5),
            Constraint::Length(4),
            Constraint::Length(1),
        ])
        .split(inner);
    render_transcript(frame, chunks[0], chat);
    let prompt_title = match chat.phase {
        WorkerPhase::Idle => " Prompt ",
        WorkerPhase::Running => " Running (Ctrl-C cancels) ",
        WorkerPhase::Closing => " Closing ",
        WorkerPhase::Closed => " Closed ",
    };
    frame.render_widget(
        Paragraph::new(chat.input.as_str())
            .wrap(Wrap { trim: false })
            .block(Block::default().borders(Borders::ALL).title(prompt_title)),
        chunks[1],
    );
    let default_footer = if chat.voice_active {
        "Listening… Ctrl-V stop · Esc back (worker keeps running)"
    } else {
        "Enter send · Shift-Enter newline · Ctrl-V dictate · Ctrl-P checkpoint · Esc back"
    };
    let footer = chat.notice.as_deref().unwrap_or(default_footer);
    frame.render_widget(
        Paragraph::new(footer).style(Style::default().fg(Color::DarkGray)),
        chunks[2],
    );
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
                let _ = partial_updates.send(VoiceUpdate::Partial(text));
            },
            |_| {},
            move |status| {
                let _ = status_updates.send(VoiceUpdate::Status(status));
            },
            cancel,
        );
        let _ = updates.send(VoiceUpdate::Finished(result));
    });
}

fn append_dictation(prefix: &str, transcript: &str) -> String {
    match (prefix.trim_end(), transcript.trim()) {
        ("", transcript) => transcript.to_owned(),
        (prefix, "") => prefix.to_owned(),
        (prefix, transcript) => format!("{prefix} {transcript}"),
    }
}

fn render_transcript(frame: &mut Frame, area: Rect, chat: &ChatState) {
    let mut lines = Vec::new();
    for entry in &chat.entries {
        let (label, style) = match entry.role {
            ChatRole::User => (
                "you",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            ChatRole::Agent => (
                "agent",
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            ),
            ChatRole::Thought => (
                "think",
                Style::default()
                    .fg(Color::DarkGray)
                    .add_modifier(Modifier::ITALIC),
            ),
            ChatRole::Tool => ("tool", Style::default().fg(Color::Yellow)),
            ChatRole::System => ("hel", Style::default().fg(Color::DarkGray)),
        };
        let mut text_lines = entry.text.lines();
        lines.push(Line::from(vec![
            Span::styled(format!("{label}> "), style),
            Span::raw(text_lines.next().unwrap_or_default().to_owned()),
        ]));
        for continuation in text_lines {
            lines.push(Line::raw(continuation.to_owned()));
        }
        lines.push(Line::raw(""));
    }
    let line_count = u16::try_from(lines.len()).unwrap_or(u16::MAX);
    let viewport_height = area.height.saturating_sub(1);
    let bottom = line_count.saturating_sub(viewport_height);
    let offset = bottom.saturating_sub(chat.scroll_back);
    frame.render_widget(
        Paragraph::new(lines)
            .scroll((offset, 0))
            .wrap(Wrap { trim: false })
            .block(Block::default().borders(Borders::BOTTOM)),
        area,
    );
}

fn extract_text(value: &serde_json::Value) -> Vec<String> {
    let mut texts = Vec::new();
    collect_text(value, &mut texts);
    texts
}

fn collect_text(value: &serde_json::Value, texts: &mut Vec<String>) {
    match value {
        serde_json::Value::Object(map) => {
            if let Some(text) = map.get("text").and_then(serde_json::Value::as_str)
                && !text.is_empty()
            {
                texts.push(text.to_owned());
                return;
            }
            for child in map.values() {
                collect_text(child, texts);
            }
        }
        serde_json::Value::Array(values) => {
            for child in values {
                collect_text(child, texts);
            }
        }
        _ => {}
    }
}

fn short_id(id: &str) -> &str {
    id.get(..8).unwrap_or(id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hel_worker::{ActivePrompt, WorkerSnapshot};

    fn snapshot() -> WorkerSnapshot {
        serde_json::from_value(serde_json::json!({
            "session_id": "1234567890",
            "phase": "idle",
            "latest_seq": 0,
            "last_checkpoint_seq": null,
            "active_prompt": null,
            "config": {},
            "handled_requests": {}
        }))
        .unwrap()
    }

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    #[test]
    fn control_v_toggles_voice_without_editing_prompt() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        let action = chat.handle_key(KeyEvent::new(KeyCode::Char('v'), KeyModifiers::CONTROL));
        assert_eq!(action, ChatAction::ToggleVoice);
        assert!(chat.input.is_empty());
    }

    #[test]
    fn dictation_appends_to_existing_prompt_cleanly() {
        assert_eq!(append_dictation("please", "fix this"), "please fix this");
        assert_eq!(append_dictation("", "fix this"), "fix this");
        assert_eq!(append_dictation("please ", ""), "please");
    }

    #[test]
    fn escape_detaches_without_emitting_close() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        assert_eq!(chat.handle_key(key(KeyCode::Esc)), ChatAction::Back);
    }

    #[test]
    fn enter_submits_only_while_idle() {
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
        assert_eq!(chat.handle_key(key(KeyCode::Enter)), ChatAction::None);
    }

    #[test]
    fn replay_projects_user_and_agent_text() {
        let runtime = RuntimeEvent::SessionUpdate {
            update: serde_json::json!({"content": {"text": "done"}}),
        };
        let events = vec![
            SequencedEvent {
                seq: 1,
                request_id: Some("p".into()),
                event: WorkerEvent::PromptAccepted {
                    request_id: "p".into(),
                    text: "work".into(),
                    attachments: vec![],
                },
            },
            SequencedEvent {
                seq: 2,
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
    fn streamed_message_chunks_coalesce_into_one_entry() {
        let mut initial = snapshot();
        initial.latest_seq = 0;
        let mut chat = ChatState::new(&initial, &[]);
        for (seq, text) in [(1, "gpt"), (2, "-5.6"), (3, "-terra")] {
            chat.apply_session_update(
                seq,
                &serde_json::json!({
                    "agent_message_chunk": {"content": {"type": "text", "text": text}}
                }),
            );
        }
        chat.apply_session_update(
            4,
            &serde_json::json!({
                "agent_thought_chunk": {"content": {"type": "text", "text": "hmm"}}
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
            &serde_json::json!({"tool_call": {"title": "grep config", "status": "pending"}}),
        );
        chat.apply_session_update(
            2,
            &serde_json::json!({"tool_call_update": {"status": "completed",
                "content": [{"type": "content", "content": {"type": "text", "text": "noise"}}]}}),
        );
        assert_eq!(chat.entries.len(), 1);
        assert_eq!(chat.entries[0].role, ChatRole::Tool);
        assert_eq!(chat.entries[0].text, "grep config");
    }

    #[test]
    fn text_extraction_ignores_non_text_scalars() {
        assert_eq!(
            extract_text(&serde_json::json!({"items": [{"text": "a"}, {"count": 3}]})),
            vec!["a"]
        );
    }
}
