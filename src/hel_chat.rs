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
    Detached { last_seen_event_sequence: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChatAction {
    None,
    Prompt(String),
    SetConfig { key: String, value: String },
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
    tool_call_id: Option<String>,
    tool_status: Option<ToolStatus>,
}

impl ChatEntry {
    fn plain(seq: u64, role: ChatRole, text: impl Into<String>) -> Self {
        Self {
            seq,
            role,
            text: text.into(),
            tool_call_id: None,
            tool_status: None,
        }
    }

    fn tool(
        seq: u64,
        title: impl Into<String>,
        tool_call_id: Option<String>,
        tool_status: ToolStatus,
    ) -> Self {
        Self {
            seq,
            role: ChatRole::Tool,
            text: title.into(),
            tool_call_id,
            tool_status: Some(tool_status),
        }
    }
}

/// The ACP tool states needed to keep a compact tool block visually useful.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ToolStatus {
    Pending,
    Running,
    Completed,
    Failed,
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
                } else if let Some((key, value)) = slash_config(&prompt) {
                    self.input.clear();
                    if value.is_empty() {
                        self.set_notice(format!("usage: /{key} <value>"));
                        ChatAction::None
                    } else {
                        ChatAction::SetConfig {
                            key: key.to_owned(),
                            value: value.to_owned(),
                        }
                    }
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
                self.entries
                    .push(ChatEntry::plain(event.seq, ChatRole::User, text));
            }
            WorkerEvent::TurnCompleted | WorkerEvent::Cancelled => {
                self.phase = WorkerPhase::Idle;
            }
            WorkerEvent::Closing => self.phase = WorkerPhase::Closing,
            WorkerEvent::Closed => self.phase = WorkerPhase::Closed,
            WorkerEvent::Checkpointed { reason } => self.entries.push(ChatEntry::plain(
                event.seq,
                ChatRole::System,
                reason.as_deref().map_or_else(
                    || "checkpoint created".into(),
                    |reason| format!("checkpoint: {reason}"),
                ),
            )),
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
            RuntimeEvent::Warning { message } => self.entries.push(ChatEntry::plain(
                seq,
                ChatRole::System,
                format!("warning: {message}"),
            )),
            RuntimeEvent::ConfigApplied { key, value } => self.entries.push(ChatEntry::plain(
                seq,
                ChatRole::System,
                format!("{key} set to {value}"),
            )),
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

    /// Project one ACP `session/update` notification into the transcript.
    /// Streamed message and thought chunks coalesce into the previous entry of
    /// the same role so tokens don't each become their own transcript line.
    fn apply_session_update(&mut self, seq: u64, update: &serde_json::Value) {
        // ACP serializes updates internally tagged:
        // {"sessionUpdate": "agent_message_chunk", "content": {...}}.
        let kind = update
            .get("sessionUpdate")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let content_text = || {
            update
                .get("content")
                .and_then(|content| content.get("text"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
        };
        match kind {
            "agent_message_chunk" => {
                let text = content_text();
                if !text.is_empty() {
                    self.push_streamed(seq, ChatRole::Agent, text);
                }
            }
            "agent_thought_chunk" => {
                let text = content_text();
                if !text.is_empty() {
                    self.push_streamed(seq, ChatRole::Thought, text);
                }
            }
            "tool_call" => {
                let title = update
                    .get("title")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("tool call");
                let tool_call_id = update
                    .get("toolCallId")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned);
                self.entries.push(ChatEntry::tool(
                    seq,
                    title,
                    tool_call_id,
                    tool_status(update.get("status")),
                ));
            }
            "tool_call_update" => self.update_tool_call(seq, update),
            "plan" | "available_commands_update" | "current_mode_update" | "user_message_chunk" => {
            }
            _ => {
                // Unknown update shapes keep the permissive text projection.
                for text in extract_text(update) {
                    self.entries
                        .push(ChatEntry::plain(seq, ChatRole::Agent, text));
                }
            }
        }
    }

    fn update_tool_call(&mut self, seq: u64, update: &serde_json::Value) {
        let Some(tool_call_id) = update.get("toolCallId").and_then(serde_json::Value::as_str)
        else {
            return;
        };
        let Some(entry) = self.entries.iter_mut().rev().find(|entry| {
            entry.role == ChatRole::Tool && entry.tool_call_id.as_deref() == Some(tool_call_id)
        }) else {
            return;
        };
        entry.seq = seq;
        if let Some(title) = update.get("title").and_then(serde_json::Value::as_str) {
            entry.text = title.to_owned();
        }
        if update.get("status").is_some() {
            entry.tool_status = Some(tool_status(update.get("status")));
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
        self.entries.push(ChatEntry::plain(seq, role, text));
    }
}

fn tool_status(status: Option<&serde_json::Value>) -> ToolStatus {
    match status.and_then(serde_json::Value::as_str) {
        Some("in_progress" | "running") => ToolStatus::Running,
        Some("completed" | "done") => ToolStatus::Completed,
        Some("failed") => ToolStatus::Failed,
        _ => ToolStatus::Pending,
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
        // Drain every queued input event before redrawing or syncing: a paste
        // delivers thousands of key events, and one draw + worker sync per
        // character would lag the trailing Enter by minutes.
        let mut pending = event::poll(Duration::from_millis(150))?;
        while pending {
            if let Event::Key(key) = event::read()? {
                let action = chat.handle_key(key);
                let result = match action {
                    ChatAction::None => None,
                    ChatAction::Prompt(text) => match client.prompt(text.clone(), Vec::new()).await
                    {
                        Ok(_) => None,
                        Err(error) => {
                            chat.input = text;
                            Some(error)
                        }
                    },
                    ChatAction::SetConfig { key, value } => {
                        client.set_config(key, value).await.err()
                    }
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
                        let last_seen_event_sequence = chat.latest_seq();
                        client.detach().await?;
                        return Ok(ChatExit::Detached {
                            last_seen_event_sequence,
                        });
                    }
                };
                if let Some(error) = result {
                    chat.set_notice(format!("{error:#}"));
                }
            }
            pending = event::poll(Duration::from_millis(0))?;
        }
        match client.sync().await {
            Ok(events) => chat.apply_events(&events),
            Err(error) => chat.set_notice(format!("connection lost: {error:#}")),
        }
    }
}

fn slash_config(prompt: &str) -> Option<(&str, &str)> {
    for key in ["model", "effort"] {
        let command = format!("/{key}");
        if prompt == command {
            return Some((key, ""));
        }
        if let Some(value) = prompt.strip_prefix(&command)
            && value.starts_with(char::is_whitespace)
        {
            return Some((key, value.trim()));
        }
    }
    None
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
    // The original mj transcript pre-wrapped role and tool blocks before
    // handing them to `Paragraph`: otherwise ratatui would only put a role
    // marker or tool rail on the first visual row. Keep that treatment here so
    // long responses remain easy to scan in a narrow terminal.
    let lines = transcript_lines(&chat.entries, area.width);
    let viewport_height = area.height.saturating_sub(2);
    let max_scroll_back = lines.len().saturating_sub(usize::from(viewport_height));
    let scroll_back = usize::from(chat.scroll_back).min(max_scroll_back);
    let title = if scroll_back == 0 {
        " Conversation ".to_owned()
    } else {
        format!(" Conversation · scrolled +{scroll_back} · End to follow ")
    };
    let block = Block::default()
        .borders(Borders::TOP | Borders::BOTTOM)
        .title(title)
        .border_style(Style::default().fg(Color::DarkGray));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let offset = lines
        .len()
        .saturating_sub(usize::from(inner.height))
        .saturating_sub(scroll_back);
    frame.render_widget(
        Paragraph::new(lines).scroll((u16::try_from(offset).unwrap_or(u16::MAX), 0)),
        inner,
    );
}

const ROLE_GUTTER: &str = "│ ";
const ROLE_GUTTER_WIDTH: usize = 2;

/// Render the complete transcript into already-wrapped visual rows. Keeping
/// layout separate from painting makes scrolling a count of actual terminal
/// rows rather than logical message lines.
fn transcript_lines(entries: &[ChatEntry], width: u16) -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    for entry in entries {
        push_transcript_entry(&mut lines, entry, usize::from(width));
    }
    if lines.is_empty() {
        lines.push(Line::from(Span::styled(
            "No messages yet — send a prompt to begin.",
            Style::default()
                .fg(Color::DarkGray)
                .add_modifier(Modifier::ITALIC),
        )));
    }
    lines
}

fn push_transcript_entry(out: &mut Vec<Line<'static>>, entry: &ChatEntry, width: usize) {
    let visual = entry_visual(entry);
    let header = Line::from(vec![
        Span::styled(
            format!("{} ", visual.glyph),
            visual.header_style.add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            visual.label,
            visual.header_style.add_modifier(Modifier::BOLD),
        ),
    ]);
    out.extend(wrap_styled_line(header, width, ROLE_GUTTER_WIDTH));

    let content_width = width.saturating_sub(ROLE_GUTTER_WIDTH).max(1);
    for (raw, line) in markdownish_lines(&entry.text, visual.body_style, visual.header_style) {
        let continuation = markdown_continuation_width(raw).min(content_width.saturating_sub(1));
        for row in wrap_styled_line(line, content_width, continuation) {
            if line_is_empty(&row) {
                out.push(Line::from(""));
            } else {
                out.push(with_role_gutter(row, visual.rail_style));
            }
        }
    }
    out.push(Line::from(""));
}

struct EntryVisual {
    glyph: &'static str,
    label: String,
    header_style: Style,
    body_style: Style,
    rail_style: Style,
}

fn entry_visual(entry: &ChatEntry) -> EntryVisual {
    match entry.role {
        ChatRole::User => {
            let style = Style::default().fg(Color::Cyan);
            EntryVisual {
                glyph: "❯",
                label: "You".into(),
                header_style: style,
                body_style: Style::default(),
                rail_style: style,
            }
        }
        ChatRole::Agent => {
            let style = Style::default().fg(Color::Green);
            EntryVisual {
                glyph: "●",
                label: "Agent".into(),
                header_style: style,
                body_style: Style::default(),
                rail_style: style,
            }
        }
        ChatRole::Thought => {
            let style = Style::default()
                .fg(Color::DarkGray)
                .add_modifier(Modifier::ITALIC);
            EntryVisual {
                glyph: "○",
                label: "Thinking".into(),
                header_style: style,
                body_style: style,
                rail_style: style,
            }
        }
        ChatRole::Tool => {
            let status = entry.tool_status.unwrap_or(ToolStatus::Pending);
            let (glyph, label, style) = tool_presentation(status);
            EntryVisual {
                glyph,
                label: format!("Tool · {label}"),
                header_style: style,
                body_style: Style::default(),
                rail_style: style,
            }
        }
        ChatRole::System => {
            let style = Style::default().fg(Color::DarkGray);
            EntryVisual {
                glyph: "─",
                label: "Hel".into(),
                header_style: style,
                body_style: style,
                rail_style: style,
            }
        }
    }
}

fn tool_presentation(status: ToolStatus) -> (&'static str, &'static str, Style) {
    match status {
        ToolStatus::Pending => ("•", "waiting", Style::default().fg(Color::DarkGray)),
        ToolStatus::Running => ("●", "running", Style::default().fg(Color::Yellow)),
        ToolStatus::Completed => ("✓", "done", Style::default().fg(Color::Green)),
        ToolStatus::Failed => ("×", "failed", Style::default().fg(Color::Red)),
    }
}

fn with_role_gutter(line: Line<'static>, style: Style) -> Line<'static> {
    let mut spans = Vec::with_capacity(line.spans.len() + 1);
    spans.push(Span::styled(ROLE_GUTTER, style));
    spans.extend(line.spans);
    Line::from(spans)
}

fn line_is_empty(line: &Line<'_>) -> bool {
    line.spans.iter().all(|span| span.content.trim().is_empty())
}

/// Render just enough Markdown to make common agent output readable without
/// turning Hel's intentionally small chat state into a full Markdown engine.
fn markdownish_lines(
    text: &str,
    body_style: Style,
    accent_style: Style,
) -> Vec<(&str, Line<'static>)> {
    let mut lines = Vec::new();
    let mut in_code_block = false;
    for raw in text.split('\n') {
        let trimmed = raw.trim_start();
        if trimmed.starts_with("```") || trimmed.starts_with("~~~") {
            in_code_block = !in_code_block;
            let language = trimmed[3..].trim();
            let label = if language.is_empty() {
                "code".to_owned()
            } else {
                format!("code · {language}")
            };
            lines.push((
                raw,
                Line::from(Span::styled(
                    label,
                    Style::default()
                        .fg(Color::DarkGray)
                        .add_modifier(Modifier::BOLD),
                )),
            ));
            continue;
        }
        if in_code_block {
            lines.push((
                raw,
                Line::from(Span::styled(
                    raw.to_owned(),
                    Style::default().fg(Color::Gray),
                )),
            ));
            continue;
        }
        if let Some((level, heading)) = markdown_heading(raw) {
            lines.push((
                raw,
                Line::from(vec![
                    Span::styled(
                        format!("{} ", "#".repeat(level)),
                        Style::default().fg(Color::DarkGray),
                    ),
                    Span::styled(
                        heading.to_owned(),
                        accent_style.add_modifier(Modifier::BOLD),
                    ),
                ]),
            ));
            continue;
        }
        if markdown_rule(raw) {
            lines.push((
                raw,
                Line::from(Span::styled(
                    "────────────────────",
                    Style::default().fg(Color::DarkGray),
                )),
            ));
            continue;
        }
        if let Some(quoted) = trimmed.strip_prefix("> ") {
            lines.push((
                raw,
                Line::from(vec![
                    Span::styled("> ", Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        quoted.to_owned(),
                        body_style
                            .fg(Color::DarkGray)
                            .add_modifier(Modifier::ITALIC),
                    ),
                ]),
            ));
            continue;
        }
        if let Some((prefix, item)) = markdown_list_item(raw) {
            let mut spans = vec![Span::styled(prefix, Style::default().fg(Color::DarkGray))];
            spans.extend(inline_markdown_spans(item, body_style));
            lines.push((raw, Line::from(spans)));
            continue;
        }
        lines.push((raw, Line::from(inline_markdown_spans(raw, body_style))));
    }
    lines
}

fn markdown_heading(raw: &str) -> Option<(usize, &str)> {
    let trimmed = raw.trim_start();
    let level = trimmed.chars().take_while(|ch| *ch == '#').count();
    (1..=6)
        .contains(&level)
        .then(|| trimmed.get(level..))
        .flatten()
        .and_then(|rest| {
            rest.strip_prefix(' ')
                .map(|heading| (level, heading.trim()))
        })
}

fn markdown_rule(raw: &str) -> bool {
    let trimmed = raw.trim();
    trimmed.len() >= 3
        && (trimmed.chars().all(|ch| ch == '-')
            || trimmed.chars().all(|ch| ch == '*')
            || trimmed.chars().all(|ch| ch == '_'))
}

fn markdown_list_item(raw: &str) -> Option<(String, &str)> {
    let indent = &raw[..raw.len() - raw.trim_start().len()];
    let trimmed = raw.trim_start();
    if let Some(item) = trimmed
        .strip_prefix("- ")
        .or_else(|| trimmed.strip_prefix("* "))
    {
        return Some((format!("{indent}• "), item));
    }
    let digits = trimmed.chars().take_while(char::is_ascii_digit).count();
    (digits > 0 && trimmed[digits..].starts_with(". ")).then(|| {
        (
            format!("{indent}{}. ", &trimmed[..digits]),
            &trimmed[digits + 2..],
        )
    })
}

fn inline_markdown_spans(text: &str, base_style: Style) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    let mut rest = text;
    let mut bold = false;
    let mut code = false;
    while !rest.is_empty() {
        let bold_at = rest.find("**");
        let code_at = rest.find('`');
        let next = match (bold_at, code_at) {
            (Some(bold_at), Some(code_at)) if bold_at <= code_at => Some((bold_at, 2, true)),
            (Some(_), Some(code_at)) => Some((code_at, 1, false)),
            (Some(bold_at), None) => Some((bold_at, 2, true)),
            (None, Some(code_at)) => Some((code_at, 1, false)),
            (None, None) => None,
        };
        let Some((index, marker_len, is_bold)) = next else {
            spans.push(Span::styled(
                rest.to_owned(),
                markdown_style(base_style, bold, code),
            ));
            break;
        };
        if index > 0 {
            spans.push(Span::styled(
                rest[..index].to_owned(),
                markdown_style(base_style, bold, code),
            ));
        }
        if is_bold {
            bold = !bold;
        } else {
            code = !code;
        }
        rest = &rest[index + marker_len..];
    }
    spans
}

fn markdown_style(base_style: Style, bold: bool, code: bool) -> Style {
    let mut style = base_style;
    if bold {
        style = style.add_modifier(Modifier::BOLD);
    }
    if code {
        style = style.fg(Color::Yellow);
    }
    style
}

/// Wrap a styled line ourselves, preserving styles and a hanging indent for
/// list items. `Paragraph` wrapping cannot retain either on continuation rows.
fn wrap_styled_line(
    line: Line<'static>,
    width: usize,
    continuation_width: usize,
) -> Vec<Line<'static>> {
    let width = width.max(1);
    let continuation_width = continuation_width.min(width.saturating_sub(1));
    let continuation_style = line
        .spans
        .first()
        .map(|span| span.style)
        .unwrap_or_default();
    let continuation = vec![(' ', continuation_style); continuation_width];
    let mut tokens: Vec<Vec<(char, Style)>> = Vec::new();
    let mut token = Vec::new();
    let mut whitespace = None;
    for span in &line.spans {
        for ch in span.content.chars() {
            let is_whitespace = ch.is_whitespace();
            if whitespace != Some(is_whitespace) {
                if !token.is_empty() {
                    tokens.push(std::mem::take(&mut token));
                }
                whitespace = Some(is_whitespace);
            }
            token.push((ch, span.style));
        }
    }
    if !token.is_empty() {
        tokens.push(token);
    }

    let mut rows = Vec::new();
    let mut current = Vec::new();
    let mut current_width = 0;
    for token in tokens {
        let token_width = styled_token_width(&token);
        let is_whitespace = token.first().is_some_and(|(ch, _)| ch.is_whitespace());
        if current_width + token_width <= width {
            current.extend(token);
            current_width += token_width;
        } else if is_whitespace {
            rows.push(std::mem::take(&mut current));
            current = continuation.clone();
            current_width = continuation_width;
        } else if token_width + continuation_width <= width {
            if current.len() > continuation.len() {
                trim_trailing_whitespace(&mut current);
                rows.push(std::mem::take(&mut current));
            }
            current = continuation.clone();
            current.extend(token);
            current_width = continuation_width + token_width;
        } else {
            for (ch, style) in token {
                let char_width = display_width(&ch.to_string());
                if current_width + char_width > width && !current.is_empty() {
                    trim_trailing_whitespace(&mut current);
                    rows.push(std::mem::take(&mut current));
                    current = continuation.clone();
                    current_width = continuation_width;
                }
                current.push((ch, style));
                current_width += char_width;
            }
        }
    }
    if !current.is_empty() || rows.is_empty() {
        rows.push(current);
    }
    rows.into_iter().map(styled_chars_line).collect()
}

fn markdown_continuation_width(raw: &str) -> usize {
    let leading = &raw[..raw.len() - raw.trim_start().len()];
    let trimmed = raw.trim_start();
    if trimmed.starts_with("- ") || trimmed.starts_with("* ") || trimmed.starts_with("> ") {
        return display_width(leading) + 2;
    }
    let digits = trimmed.chars().take_while(char::is_ascii_digit).count();
    if digits > 0 && trimmed[digits..].starts_with(". ") {
        return display_width(leading) + digits + 2;
    }
    display_width(leading)
}

fn styled_token_width(token: &[(char, Style)]) -> usize {
    token
        .iter()
        .map(|(ch, _)| display_width(&ch.to_string()))
        .sum()
}

fn trim_trailing_whitespace(chars: &mut Vec<(char, Style)>) {
    while chars.last().is_some_and(|(ch, _)| ch.is_whitespace()) {
        chars.pop();
    }
}

fn styled_chars_line(chars: Vec<(char, Style)>) -> Line<'static> {
    let mut spans = Vec::new();
    let mut text = String::new();
    let mut style = None;
    for (ch, next_style) in chars {
        if style != Some(next_style) {
            if let Some(style) = style {
                spans.push(Span::styled(std::mem::take(&mut text), style));
            }
            style = Some(next_style);
        }
        text.push(ch);
    }
    if let Some(style) = style {
        spans.push(Span::styled(text, style));
    }
    Line::from(spans)
}

fn display_width(text: &str) -> usize {
    Line::raw(text.to_owned()).width()
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
    fn config_slash_command_without_value_shows_usage() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.input = "/model".into();

        assert_eq!(chat.handle_key(key(KeyCode::Enter)), ChatAction::None);
        assert_eq!(chat.notice.as_deref(), Some("usage: /model <value>"));
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
                "title": "grep config", "status": "pending"}),
        );
        chat.apply_session_update(
            2,
            &serde_json::json!({"sessionUpdate": "tool_call_update", "status": "completed",
                "content": [{"type": "content", "content": {"type": "text", "text": "noise"}}]}),
        );
        assert_eq!(chat.entries.len(), 1);
        assert_eq!(chat.entries[0].role, ChatRole::Tool);
        assert_eq!(chat.entries[0].text, "grep config");
    }

    #[test]
    fn tool_call_updates_refresh_the_rendered_status() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.apply_session_update(
            1,
            &serde_json::json!({
                "sessionUpdate": "tool_call",
                "toolCallId": "read-config",
                "title": "read config",
                "status": "pending"
            }),
        );
        chat.apply_session_update(
            2,
            &serde_json::json!({
                "sessionUpdate": "tool_call_update",
                "toolCallId": "read-config",
                "status": "completed"
            }),
        );

        assert_eq!(chat.entries.len(), 1);
        assert_eq!(chat.entries[0].tool_status, Some(ToolStatus::Completed));
        assert_eq!(tool_presentation(ToolStatus::Completed).1, "done");
    }

    #[test]
    fn transcript_blocks_keep_role_headers_and_wrapped_body_indented() {
        let entry = ChatEntry::plain(1, ChatRole::User, "alpha beta gamma");
        let lines = transcript_lines(&[entry], 12);
        let text = lines
            .iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect::<String>()
            })
            .collect::<Vec<_>>();

        assert_eq!(text, ["❯ You", "│ alpha beta", "│ gamma", ""]);
    }

    #[test]
    fn markdown_list_wrapping_uses_a_hanging_indent() {
        let entry = ChatEntry::plain(1, ChatRole::Agent, "- alpha beta gamma");
        let text = transcript_lines(&[entry], 13)
            .into_iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect::<String>()
            })
            .collect::<Vec<_>>();

        assert!(text.iter().any(|line| line == "│ • alpha"));
        assert!(text.iter().any(|line| line == "│   beta"));
        assert!(text.iter().any(|line| line == "│   gamma"));
    }

    #[test]
    fn page_navigation_keeps_end_attached_to_the_latest_message() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.handle_key(key(KeyCode::PageUp));
        assert_eq!(chat.scroll_back, 8);
        chat.handle_key(key(KeyCode::PageDown));
        assert_eq!(chat.scroll_back, 0);
        chat.handle_key(key(KeyCode::PageUp));
        chat.handle_key(key(KeyCode::End));
        assert_eq!(chat.scroll_back, 0);
    }

    #[test]
    fn text_extraction_ignores_non_text_scalars() {
        assert_eq!(
            extract_text(&serde_json::json!({"items": [{"text": "a"}, {"count": 3}]})),
            vec!["a"]
        );
    }
}
