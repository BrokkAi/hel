//! Minimal full-screen chat for one persistent Hel worker.

mod rendering;

use std::collections::VecDeque;
use std::io;
use std::time::Duration;

use agent_client_protocol::schema::v1::{
    AvailableCommand, AvailableCommandInput, ContentBlock, EmbeddedResourceResource,
    PlanEntryStatus, SessionConfigKind, SessionConfigOption, SessionConfigOptionCategory,
    SessionConfigSelectOptions, SessionUpdate, ToolCallContent, ToolCallLocation, ToolCallStatus,
};
use anyhow::Result;
use crossterm::event::{
    self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseEvent, MouseEventKind,
};
use ratatui::Frame;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, Paragraph, Wrap};
use similar::{ChangeTag, TextDiff};
use unicode_segmentation::UnicodeSegmentation;

use crate::hel_acp::RuntimeEvent;
use crate::hel_database::{HistoryScope, PromptHistoryEntry};
use crate::hel_worker::{SequencedEvent, WorkerEvent, WorkerPhase, WorkerSnapshot};
use crate::hel_worker_client::WorkerClient;
use rendering::{
    LogicalLine, TranscriptRenderMode, append_omitted_character_count, display_width,
    line_character_count, markdown_lines, raw_lines, sanitize_terminal_text, wrap_styled_line,
};

const MOUSE_SCROLL_ROWS: usize = 3;

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
    Checkpoint(Option<String>),
    ToggleVoice,
    Back,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalCommand {
    Help,
    Detach,
    Checkpoint,
    Model,
    Effort,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommandSource {
    Hel,
    Agent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CommandChoice {
    name: String,
    description: String,
    input_hint: Option<String>,
    source: CommandSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ConfigValueChoice {
    value: String,
    name: String,
    description: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AutocompleteKind {
    Commands,
    ConfigValues { key: &'static str },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Autocomplete {
    kind: AutocompleteKind,
    selected: usize,
    matches: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueuedPrompt {
    text: String,
}

#[derive(Debug, Clone)]
struct HistorySearch {
    original_input: String,
    original_cursor: usize,
    query: String,
    scope: HistoryScope,
    matches: Vec<PromptHistoryEntry>,
    selected: Option<usize>,
    unavailable: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatRole {
    User,
    Agent,
    /// Agent reasoning stream, rendered dimmed.
    Thought,
    /// Tool invocation titles.
    Tool,
    /// Current agent plan.
    Plan,
    System,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatEntry {
    pub seq: u64,
    pub role: ChatRole,
    pub text: String,
    revision: u64,
    message_id: Option<String>,
    tool_call_id: Option<String>,
    tool_status: Option<ToolStatus>,
    tool_content: Vec<String>,
    tool_diffstats: Vec<String>,
    tool_locations: Vec<String>,
    plan: Vec<PlanLine>,
}

impl ChatEntry {
    fn plain(seq: u64, role: ChatRole, text: impl Into<String>) -> Self {
        Self {
            seq,
            role,
            text: sanitize_terminal_text(&text.into()),
            revision: 0,
            message_id: None,
            tool_call_id: None,
            tool_status: None,
            tool_content: Vec::new(),
            tool_diffstats: Vec::new(),
            tool_locations: Vec::new(),
            plan: Vec::new(),
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
            text: sanitize_terminal_text(&title.into()),
            revision: 0,
            message_id: None,
            tool_call_id,
            tool_status: Some(tool_status),
            tool_content: Vec::new(),
            tool_diffstats: Vec::new(),
            tool_locations: Vec::new(),
            plan: Vec::new(),
        }
    }

    fn plan(seq: u64, plan: Vec<PlanLine>) -> Self {
        Self {
            seq,
            role: ChatRole::Plan,
            text: String::new(),
            revision: 0,
            message_id: None,
            tool_call_id: None,
            tool_status: None,
            tool_content: Vec::new(),
            tool_diffstats: Vec::new(),
            tool_locations: Vec::new(),
            plan,
        }
    }

    fn touch(&mut self, seq: u64) {
        self.seq = seq;
        self.revision = self.revision.wrapping_add(1);
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlanStatus {
    Pending,
    Running,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlanLine {
    text: String,
    status: PlanStatus,
}

#[derive(Debug, Clone)]
struct CachedEntry {
    revision: u64,
    lines: Vec<Line<'static>>,
}

#[derive(Debug)]
struct TranscriptRenderCache {
    width: u16,
    mode: TranscriptRenderMode,
    entries: Vec<Option<CachedEntry>>,
}

/// A read-only copy of the projected conversation that can be rendered by
/// surfaces other than the interactive chat view.
#[derive(Debug)]
pub struct TranscriptSnapshot {
    entries: Vec<ChatEntry>,
    render_cache: TranscriptRenderCache,
}

impl TranscriptSnapshot {
    pub(crate) fn rich_tail(&mut self, width: u16, maximum_lines: usize) -> Vec<Line<'static>> {
        let lines = transcript_entry_lines(
            &self.entries,
            &mut self.render_cache,
            width,
            TranscriptRenderMode::Rich,
        );
        let start = lines.len().saturating_sub(maximum_lines);
        lines.into_iter().skip(start).collect()
    }
}

impl Default for TranscriptRenderCache {
    fn default() -> Self {
        Self {
            width: 0,
            mode: TranscriptRenderMode::Rich,
            entries: Vec::new(),
        }
    }
}

pub struct ChatState {
    session_id: String,
    bundle_id: Option<String>,
    phase: WorkerPhase,
    latest_seq: u64,
    entries: Vec<ChatEntry>,
    input: String,
    input_cursor: usize,
    prompt_history: Vec<String>,
    history_index: Option<usize>,
    history_draft: String,
    kill_buffer: String,
    preferred_column: Option<usize>,
    history_search: Option<HistorySearch>,
    queued_prompts: VecDeque<QueuedPrompt>,
    agent_commands: Vec<AvailableCommand>,
    command_choices: Vec<CommandChoice>,
    model_values: Vec<ConfigValueChoice>,
    effort_values: Vec<ConfigValueChoice>,
    autocomplete: Option<Autocomplete>,
    scroll_top: usize,
    follow_bottom: bool,
    last_content_height: usize,
    last_viewport_height: usize,
    render_mode: TranscriptRenderMode,
    render_cache: TranscriptRenderCache,
    notice: Option<String>,
    voice_active: bool,
}

impl ChatState {
    pub fn new(snapshot: &WorkerSnapshot, events: &[SequencedEvent]) -> Self {
        let mut state = Self {
            session_id: snapshot.session_id.clone(),
            bundle_id: None,
            phase: snapshot.phase,
            latest_seq: 0,
            entries: Vec::new(),
            input: String::new(),
            input_cursor: 0,
            prompt_history: Vec::new(),
            history_index: None,
            history_draft: String::new(),
            kill_buffer: String::new(),
            preferred_column: None,
            history_search: None,
            queued_prompts: VecDeque::new(),
            agent_commands: Vec::new(),
            command_choices: builtin_command_choices(),
            model_values: Vec::new(),
            effort_values: Vec::new(),
            autocomplete: None,
            scroll_top: 0,
            follow_bottom: true,
            last_content_height: 0,
            last_viewport_height: 0,
            render_mode: TranscriptRenderMode::Rich,
            render_cache: TranscriptRenderCache::default(),
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

    pub fn set_history_context(&mut self, bundle_id: impl Into<String>) {
        self.bundle_id = Some(bundle_id.into());
    }

    fn record_accepted_prompt(&mut self, sequence: u64, text: &str) {
        let Some(bundle_id) = self.bundle_id.as_deref() else {
            return;
        };
        if let Err(error) =
            crate::hel_database::record_prompt(&self.session_id, bundle_id, sequence, None, text)
        {
            self.set_notice(format!("Prompt sent, but history was not saved: {error:#}"));
        }
    }

    pub fn latest_seq(&self) -> u64 {
        self.latest_seq
    }

    fn mark_prompt_submitted(&mut self) {
        self.phase = WorkerPhase::Running;
        self.notice = None;
    }

    pub fn entries(&self) -> &[ChatEntry] {
        &self.entries
    }

    pub fn transcript_snapshot(&self) -> TranscriptSnapshot {
        TranscriptSnapshot {
            entries: self.entries.clone(),
            render_cache: TranscriptRenderCache::default(),
        }
    }

    pub fn set_notice(&mut self, notice: impl Into<String>) {
        self.notice = Some(sanitize_terminal_text(&notice.into()));
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
        self.input.clear();
        self.input_cursor = 0;
        self.prompt_history.clear();
        self.history_index = None;
        self.history_draft.clear();
        self.preferred_column = None;
        self.history_search = None;
        self.queued_prompts.clear();
        self.autocomplete = None;
        self.scroll_top = 0;
        self.follow_bottom = true;
        self.last_content_height = 0;
        self.last_viewport_height = 0;
        self.render_mode = TranscriptRenderMode::Rich;
        self.notice = None;
        self.voice_active = false;
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

    fn insert_character(&mut self, character: char) {
        self.input.insert(self.input_cursor, character);
        self.input_cursor += character.len_utf8();
        self.history_index = None;
        self.preferred_column = None;
        self.update_autocomplete();
    }

    fn backspace(&mut self) {
        if self.input_cursor == 0 {
            return;
        }
        let start = previous_grapheme_boundary(&self.input, self.input_cursor);
        self.input.replace_range(start..self.input_cursor, "");
        self.input_cursor = start;
        self.history_index = None;
        self.preferred_column = None;
        self.update_autocomplete();
    }

    fn delete(&mut self) {
        if self.input_cursor >= self.input.len() {
            return;
        }
        let end = next_grapheme_boundary(&self.input, self.input_cursor);
        self.input.replace_range(self.input_cursor..end, "");
        self.history_index = None;
        self.preferred_column = None;
        self.update_autocomplete();
    }

    fn move_input_cursor(&mut self, delta: isize) {
        self.input_cursor = if delta.is_negative() {
            previous_grapheme_boundary(&self.input, self.input_cursor)
        } else {
            next_grapheme_boundary(&self.input, self.input_cursor)
        };
        self.preferred_column = None;
        self.update_autocomplete();
    }

    fn line_start(&self) -> usize {
        self.input[..self.input_cursor]
            .rfind('\n')
            .map_or(0, |index| index + 1)
    }

    fn line_end(&self) -> usize {
        self.input[self.input_cursor..]
            .find('\n')
            .map_or(self.input.len(), |index| self.input_cursor + index)
    }

    fn move_to_line_start(&mut self, cross_boundary: bool) {
        let start = self.line_start();
        self.input_cursor = if cross_boundary && self.input_cursor == start && start > 0 {
            self.input[..start - 1]
                .rfind('\n')
                .map_or(0, |index| index + 1)
        } else {
            start
        };
        self.preferred_column = None;
        self.update_autocomplete();
    }

    fn move_to_line_end(&mut self, cross_boundary: bool) {
        let end = self.line_end();
        self.input_cursor = if cross_boundary && self.input_cursor == end && end < self.input.len()
        {
            let next = end + 1;
            self.input[next..]
                .find('\n')
                .map_or(self.input.len(), |index| next + index)
        } else {
            end
        };
        self.preferred_column = None;
        self.update_autocomplete();
    }

    fn move_vertical(&mut self, direction: isize) {
        let start = self.line_start();
        let column = self
            .preferred_column
            .unwrap_or_else(|| self.input[start..self.input_cursor].graphemes(true).count());
        let target_start = if direction.is_negative() {
            if start == 0 {
                self.input_cursor = 0;
                self.preferred_column = None;
                self.update_autocomplete();
                return;
            }
            self.input[..start - 1]
                .rfind('\n')
                .map_or(0, |index| index + 1)
        } else {
            let end = self.line_end();
            if end == self.input.len() {
                self.input_cursor = self.input.len();
                self.preferred_column = None;
                self.update_autocomplete();
                return;
            }
            end + 1
        };
        let target_end = self.input[target_start..]
            .find('\n')
            .map_or(self.input.len(), |index| target_start + index);
        self.input_cursor = self.input[target_start..target_end]
            .grapheme_indices(true)
            .nth(column)
            .map_or(target_end, |(offset, _)| target_start + offset);
        self.preferred_column = Some(column);
        self.update_autocomplete();
    }

    fn previous_word_start(&self) -> usize {
        let prefix = &self.input[..self.input_cursor];
        let trimmed = prefix.trim_end_matches(char::is_whitespace);
        if trimmed.is_empty() {
            return 0;
        }
        let run_start = trimmed
            .char_indices()
            .rev()
            .find(|(_, character)| character.is_whitespace())
            .map_or(0, |(index, character)| index + character.len_utf8());
        let run = &trimmed[run_start..];
        let mut start = run_start + run.len();
        let mut class = None;
        for (index, character) in run.char_indices().rev() {
            let next_class = word_class(character);
            if class.is_some_and(|class| class != next_class) {
                break;
            }
            class = Some(next_class);
            start = run_start + index;
        }
        start
    }

    fn next_word_end(&self) -> usize {
        let suffix = &self.input[self.input_cursor..];
        let Some(non_space) = suffix.find(|character: char| !character.is_whitespace()) else {
            return self.input.len();
        };
        let run = &suffix[non_space..];
        let mut end = 0;
        let mut class = None;
        for (index, character) in run.char_indices() {
            if character.is_whitespace() {
                break;
            }
            let next_class = word_class(character);
            if class.is_some_and(|class| class != next_class) {
                break;
            }
            class = Some(next_class);
            end = index + character.len_utf8();
        }
        self.input_cursor + non_space + end
    }

    fn move_word(&mut self, direction: isize) {
        self.input_cursor = if direction.is_negative() {
            self.previous_word_start()
        } else {
            self.next_word_end()
        };
        self.preferred_column = None;
        self.update_autocomplete();
    }

    fn kill_range(&mut self, range: std::ops::Range<usize>) {
        if range.is_empty() {
            return;
        }
        self.kill_buffer = self.input[range.clone()].to_owned();
        self.input.replace_range(range.clone(), "");
        self.input_cursor = range.start;
        self.history_index = None;
        self.preferred_column = None;
        self.update_autocomplete();
    }

    fn kill_to_line_start(&mut self) {
        let start = self.line_start();
        if start == self.input_cursor && start > 0 {
            self.kill_range(start - 1..start);
        } else {
            self.kill_range(start..self.input_cursor);
        }
    }

    fn kill_to_line_end(&mut self) {
        let end = self.line_end();
        if end == self.input_cursor && end < self.input.len() {
            self.kill_range(end..end + 1);
        } else {
            self.kill_range(self.input_cursor..end);
        }
    }

    fn yank(&mut self) {
        if self.kill_buffer.is_empty() {
            return;
        }
        let text = self.kill_buffer.clone();
        self.input.insert_str(self.input_cursor, &text);
        self.input_cursor += text.len();
        self.history_index = None;
        self.update_autocomplete();
    }

    fn begin_history_search(&mut self) {
        self.autocomplete = None;
        self.history_search = Some(HistorySearch {
            original_input: self.input.clone(),
            original_cursor: self.input_cursor,
            query: String::new(),
            scope: HistoryScope::Project,
            matches: Vec::new(),
            selected: None,
            unavailable: None,
        });
    }

    fn refresh_history_search(&mut self) {
        let Some(search) = self.history_search.as_ref() else {
            return;
        };
        let query = search.query.clone();
        let scope = search.scope;
        let original = (search.original_input.clone(), search.original_cursor);
        let result = if query.is_empty() {
            Ok(Vec::new())
        } else if let Some(bundle_id) = self.bundle_id.as_deref() {
            crate::hel_database::search_prompts(&self.session_id, bundle_id, scope, &query)
        } else {
            Ok(self
                .prompt_history
                .iter()
                .rev()
                .enumerate()
                .filter(|(_, text)| text.to_lowercase().contains(&query.to_lowercase()))
                .map(|(index, text)| PromptHistoryEntry {
                    id: -(index as i64) - 1,
                    session_id: self.session_id.clone(),
                    text: text.clone(),
                })
                .collect())
        };
        match result {
            Ok(matches) => {
                if let Some(search) = self.history_search.as_mut() {
                    search.matches = matches;
                    search.selected = (!search.matches.is_empty()).then_some(0);
                    search.unavailable = None;
                }
                if let Some(text) = self
                    .history_search
                    .as_ref()
                    .and_then(|search| search.matches.first())
                    .map(|entry| entry.text.clone())
                {
                    self.input_cursor = text.len();
                    self.input = text;
                } else {
                    self.input = original.0;
                    self.input_cursor = original.1;
                }
            }
            Err(error) => {
                self.input = original.0;
                self.input_cursor = original.1;
                self.history_search = None;
                self.notice = Some(format!("History unavailable: {error:#}"));
                self.update_autocomplete();
            }
        }
    }

    fn step_history_search(&mut self, direction: isize) {
        let Some(search) = self.history_search.as_mut() else {
            return;
        };
        if search.matches.is_empty() {
            return;
        }
        let current = search.selected.unwrap_or(0);
        let next = if direction.is_negative() {
            current.saturating_sub(1)
        } else {
            (current + 1).min(search.matches.len() - 1)
        };
        search.selected = Some(next);
        self.input = search.matches[next].text.clone();
        self.input_cursor = self.input.len();
    }

    fn cycle_history_scope(&mut self) {
        let Some(search) = self.history_search.as_mut() else {
            return;
        };
        search.scope = match search.scope {
            HistoryScope::Project => HistoryScope::Session,
            HistoryScope::Session => HistoryScope::All,
            HistoryScope::All => HistoryScope::Project,
        };
        self.refresh_history_search();
    }

    fn cancel_history_search(&mut self) {
        let Some(search) = self.history_search.take() else {
            return;
        };
        self.input = search.original_input;
        self.input_cursor = search.original_cursor;
        self.update_autocomplete();
    }

    fn accept_history_search(&mut self) {
        if self
            .history_search
            .as_ref()
            .is_some_and(|search| search.selected.is_some())
        {
            self.history_search = None;
            self.history_index = None;
            self.update_autocomplete();
        }
    }

    fn move_autocomplete(&mut self, delta: isize) {
        let Some(autocomplete) = self.autocomplete.as_mut() else {
            return;
        };
        let len = autocomplete.matches.len();
        if len == 0 {
            return;
        }
        autocomplete.selected = if delta.is_negative() {
            autocomplete.selected.checked_sub(1).unwrap_or(len - 1)
        } else {
            (autocomplete.selected + 1) % len
        };
    }

    fn accept_autocomplete(&mut self) -> bool {
        let Some(autocomplete) = self.autocomplete.clone() else {
            return false;
        };
        let Some(&index) = autocomplete.matches.get(autocomplete.selected) else {
            return false;
        };
        let value = match autocomplete.kind {
            AutocompleteKind::Commands => self
                .command_choices
                .get(index)
                .map(|command| format!("/{} ", command.name)),
            AutocompleteKind::ConfigValues { key: "model" } => self
                .model_values
                .get(index)
                .map(|choice| format!("/model {}", choice.value)),
            AutocompleteKind::ConfigValues { key: "effort" } => self
                .effort_values
                .get(index)
                .map(|choice| format!("/effort {}", choice.value)),
            AutocompleteKind::ConfigValues { .. } => None,
        };
        let Some(value) = value else {
            return false;
        };
        self.set_input(value);
        self.autocomplete = None;
        true
    }

    fn update_autocomplete(&mut self) {
        if self.history_search.is_some() || self.input_cursor != self.input.len() {
            self.autocomplete = None;
            return;
        }
        for (prefix, key, values) in [
            ("/model ", "model", &self.model_values),
            ("/effort ", "effort", &self.effort_values),
        ] {
            if let Some(query) = self.input.strip_prefix(prefix) {
                let matches = matching_indices(values, query, |choice| {
                    (&choice.value, Some(choice.name.as_str()))
                });
                self.autocomplete = (!matches.is_empty()).then_some(Autocomplete {
                    kind: AutocompleteKind::ConfigValues { key },
                    selected: 0,
                    matches,
                });
                return;
            }
        }
        let Some(query) = self.input.strip_prefix('/') else {
            self.autocomplete = None;
            return;
        };
        if query.contains(char::is_whitespace) {
            self.autocomplete = None;
            return;
        }
        let matches = matching_indices(&self.command_choices, query, |command| {
            (&command.name, Some(command.description.as_str()))
        });
        self.autocomplete = (!matches.is_empty()).then_some(Autocomplete {
            kind: AutocompleteKind::Commands,
            selected: 0,
            matches,
        });
    }

    fn rebuild_command_choices(&mut self) {
        let mut commands = builtin_command_choices();
        for command in &self.agent_commands {
            let name = command.name.trim();
            if name.is_empty()
                || commands
                    .iter()
                    .any(|existing| existing.name.eq_ignore_ascii_case(name))
            {
                continue;
            }
            let input_hint = command.input.as_ref().and_then(|input| match input {
                AvailableCommandInput::Unstructured(input) => Some(input.hint.clone()),
                _ => None,
            });
            commands.push(CommandChoice {
                name: name.to_owned(),
                description: command.description.trim().to_owned(),
                input_hint,
                source: CommandSource::Agent,
            });
        }
        self.command_choices = commands;
        self.update_autocomplete();
    }

    fn set_config_options(&mut self, options: &[SessionConfigOption]) {
        self.model_values = config_values(options, "model");
        self.effort_values = config_values(options, "effort");
        self.update_autocomplete();
    }

    fn record_prompt_history(&mut self, prompt: &str) {
        if self.prompt_history.last().is_none_or(|last| last != prompt) {
            self.prompt_history.push(prompt.to_owned());
        }
        self.history_index = None;
        self.history_draft.clear();
    }

    fn move_history(&mut self, delta: isize) {
        let history = if let Some(bundle_id) = self.bundle_id.as_deref() {
            crate::hel_database::search_prompts(
                &self.session_id,
                bundle_id,
                HistoryScope::Project,
                "",
            )
            .map(|entries| {
                entries
                    .into_iter()
                    .rev()
                    .map(|entry| entry.text)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|error| {
                self.notice = Some(format!("History unavailable: {error:#}"));
                self.prompt_history.clone()
            })
        } else {
            self.prompt_history.clone()
        };
        if history.is_empty() {
            return;
        }
        let next = match (self.history_index, delta.is_negative()) {
            (None, true) => {
                self.history_draft.clone_from(&self.input);
                Some(history.len() - 1)
            }
            (None, false) => None,
            (Some(index), true) => Some(index.saturating_sub(1)),
            (Some(index), false) if index + 1 < history.len() => Some(index + 1),
            (Some(_), false) => None,
        };
        self.history_index = next;
        let input = next
            .and_then(|index| history.get(index).cloned())
            .unwrap_or_else(|| self.history_draft.clone());
        self.input = input;
        self.input_cursor = self.input.len();
        self.preferred_column = None;
        self.update_autocomplete();
    }

    fn edit_latest_queued_prompt(&mut self) {
        let Some(queued) = self.queued_prompts.pop_back() else {
            return;
        };
        self.set_input(queued.text);
        self.set_notice("Editing the most recently queued prompt");
    }

    fn show_help(&mut self) {
        let commands = self
            .command_choices
            .iter()
            .map(|command| {
                let hint = command
                    .input_hint
                    .as_deref()
                    .map(|hint| format!(" <{hint}>"))
                    .unwrap_or_default();
                let source = match command.source {
                    CommandSource::Hel => "hel",
                    CommandSource::Agent => "agent",
                };
                format!(
                    "/{name}{hint} — {description} [{source}]",
                    name = command.name,
                    description = command.description
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        self.entries.push(ChatEntry::plain(
            self.latest_seq,
            ChatRole::System,
            format!("Available commands:\n{commands}"),
        ));
    }

    fn submit_input(&mut self) -> ChatAction {
        let prompt = self.input.trim().to_owned();
        if prompt.is_empty() {
            return ChatAction::None;
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
                LocalCommand::Checkpoint => {
                    self.clear_input();
                    ChatAction::Checkpoint((!args.is_empty()).then(|| args.to_owned()))
                }
                LocalCommand::Model | LocalCommand::Effort => {
                    let key = if command == LocalCommand::Model {
                        "model"
                    } else {
                        "effort"
                    };
                    if self.phase != WorkerPhase::Idle {
                        self.set_notice(format!(
                            "/{key} is only available while the agent is idle"
                        ));
                        return ChatAction::None;
                    }
                    if args.is_empty() {
                        self.set_notice(format!("usage: /{key} <value>"));
                        return ChatAction::None;
                    }
                    self.clear_input();
                    ChatAction::SetConfig {
                        key: key.to_owned(),
                        value: args.to_owned(),
                    }
                }
            };
        }
        if matches!(self.phase, WorkerPhase::Closing | WorkerPhase::Closed) {
            self.set_notice("The worker is closing; this prompt was not sent");
            return ChatAction::None;
        }
        self.record_prompt_history(&prompt);
        self.clear_input();
        if self.phase == WorkerPhase::Running {
            self.queued_prompts.push_back(QueuedPrompt {
                text: prompt.clone(),
            });
            self.set_notice(format!(
                "Queued {}: {}",
                self.queued_prompts.len(),
                queued_prompt_preview(&prompt)
            ));
            ChatAction::None
        } else {
            ChatAction::Prompt(prompt)
        }
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> ChatAction {
        if key.kind != KeyEventKind::Press && key.kind != KeyEventKind::Repeat {
            return ChatAction::None;
        }
        let (code, modifiers) = normalize_key(key.code, key.modifiers);

        if modifiers.contains(KeyModifiers::CONTROL) && code == KeyCode::Char('g') {
            return ChatAction::Back;
        }

        if self.history_search.is_some() {
            if modifiers.contains(KeyModifiers::ALT) && code == KeyCode::Char('r') {
                self.cycle_history_scope();
                return ChatAction::None;
            }
            if modifiers.contains(KeyModifiers::CONTROL) && code == KeyCode::Char('r')
                || code == KeyCode::Up
            {
                self.step_history_search(1);
                return ChatAction::None;
            }
            if modifiers.contains(KeyModifiers::CONTROL) && code == KeyCode::Char('s')
                || code == KeyCode::Down
            {
                self.step_history_search(-1);
                return ChatAction::None;
            }
            if code == KeyCode::Esc
                || modifiers.contains(KeyModifiers::CONTROL) && code == KeyCode::Char('c')
            {
                self.cancel_history_search();
                return ChatAction::None;
            }
            if code == KeyCode::Enter {
                self.accept_history_search();
                return ChatAction::None;
            }
            if code == KeyCode::Backspace
                || modifiers.contains(KeyModifiers::CONTROL) && code == KeyCode::Char('h')
            {
                if let Some(search) = self.history_search.as_mut() {
                    let end = search.query.len();
                    let start = previous_grapheme_boundary(&search.query, end);
                    search.query.replace_range(start..end, "");
                }
                self.refresh_history_search();
                return ChatAction::None;
            }
            if modifiers.contains(KeyModifiers::CONTROL) && code == KeyCode::Char('u') {
                if let Some(search) = self.history_search.as_mut() {
                    search.query.clear();
                }
                self.refresh_history_search();
                return ChatAction::None;
            }
            if let KeyCode::Char(character) = code
                && !character.is_ascii_control()
                && !modifiers.intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
            {
                if let Some(search) = self.history_search.as_mut() {
                    search.query.push(character);
                }
                self.refresh_history_search();
            }
            return ChatAction::None;
        }

        if code == KeyCode::Esc {
            return if self.phase == WorkerPhase::Running {
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
                    self.render_mode = self.render_mode.toggled();
                    self.notice = Some(
                        match self.render_mode {
                            TranscriptRenderMode::Rich => "Rich transcript rendering enabled",
                            TranscriptRenderMode::Raw => "Raw transcript source enabled",
                        }
                        .into(),
                    );
                    return ChatAction::None;
                }
                KeyCode::Char('v') => return ChatAction::ToggleVoice,
                KeyCode::Char('a') => self.move_to_line_start(true),
                KeyCode::Char('e') => self.move_to_line_end(true),
                KeyCode::Char('b') => self.move_input_cursor(-1),
                KeyCode::Char('f') => self.move_input_cursor(1),
                KeyCode::Char('h') => self.backspace(),
                KeyCode::Char('d') => self.delete(),
                KeyCode::Char('u') => self.kill_to_line_start(),
                KeyCode::Char('k') => self.kill_to_line_end(),
                KeyCode::Char('w') => {
                    let start = self.previous_word_start();
                    self.kill_range(start..self.input_cursor);
                }
                KeyCode::Char('y') => self.yank(),
                KeyCode::Char('j') | KeyCode::Char('m') => self.insert_character('\n'),
                KeyCode::Char('p') => {
                    if self.input.is_empty() || self.history_index.is_some() {
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
                    self.scroll_top = 0;
                    self.follow_bottom = false;
                }
                KeyCode::End => self.follow_bottom = true,
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
                KeyCode::Up if !self.queued_prompts.is_empty() => self.edit_latest_queued_prompt(),
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
            KeyCode::Tab => {
                self.accept_autocomplete();
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
                if self.input.is_empty() || self.history_index.is_some() {
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
                self.edit_latest_queued_prompt();
                ChatAction::None
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

    pub fn handle_mouse(&mut self, mouse: MouseEvent) {
        match mouse.kind {
            MouseEventKind::ScrollUp => self.scroll_history_up(MOUSE_SCROLL_ROWS),
            MouseEventKind::ScrollDown => self.scroll_history_down(MOUSE_SCROLL_ROWS),
            _ => {}
        }
    }

    fn scroll_history_up(&mut self, rows: usize) {
        if self.follow_bottom {
            self.scroll_top = self
                .last_content_height
                .saturating_sub(self.last_viewport_height);
        }
        self.follow_bottom = false;
        self.scroll_top = self.scroll_top.saturating_sub(rows);
    }

    fn scroll_history_down(&mut self, rows: usize) {
        let maximum = self
            .last_content_height
            .saturating_sub(self.last_viewport_height);
        self.scroll_top = self.scroll_top.saturating_add(rows);
        if self.scroll_top >= maximum {
            self.scroll_top = maximum;
            self.follow_bottom = true;
        }
    }

    fn apply_event(&mut self, event: &SequencedEvent) {
        match &event.event {
            WorkerEvent::PromptAccepted { text, .. } => {
                self.phase = WorkerPhase::Running;
                self.entries
                    .push(ChatEntry::plain(event.seq, ChatRole::User, text));
            }
            WorkerEvent::TurnCompleted => {
                self.phase = WorkerPhase::Idle;
            }
            // The durable worker records cancellation acceptance before the
            // ACP prompt future resolves. Keep the chat busy until the later
            // TurnCompleted event so a queued prompt cannot race the runtime.
            WorkerEvent::Cancelled => {
                self.phase = WorkerPhase::Running;
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
            RuntimeEvent::ConfigApplied { key, value, .. } => self.entries.push(ChatEntry::plain(
                seq,
                ChatRole::System,
                format!("{key} set to {value}"),
            )),
            RuntimeEvent::SessionConfigured { config_options } => {
                self.set_config_options(&config_options)
            }
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
    fn apply_session_update(&mut self, seq: u64, update: &serde_json::Value) {
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
                    self.push_streamed(seq, ChatRole::Agent, message_id, &text);
                }
            }
            SessionUpdate::AgentThoughtChunk(chunk) => {
                let message_id = chunk.message_id.map(|id| id.to_string());
                if let Some(text) = content_block_text(&chunk.content) {
                    self.push_streamed(seq, ChatRole::Thought, message_id, &text);
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
                entry.tool_content = tool_content_details(&call.content);
                entry.tool_diffstats = tool_diffstats(&call.content);
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
                    entry.tool_content = tool_content_details(&content);
                    entry.tool_diffstats = tool_diffstats(&content);
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
            _ => {}
        }
    }

    fn push_streamed(&mut self, seq: u64, role: ChatRole, message_id: Option<String>, text: &str) {
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
        let mut entry = ChatEntry::plain(seq, role, text);
        entry.message_id = message_id;
        self.entries.push(entry);
    }
}

fn previous_grapheme_boundary(input: &str, cursor: usize) -> usize {
    input[..cursor]
        .grapheme_indices(true)
        .next_back()
        .map_or(0, |(index, _)| index)
}

fn next_grapheme_boundary(input: &str, cursor: usize) -> usize {
    input[cursor..]
        .grapheme_indices(true)
        .nth(1)
        .map_or(input.len(), |(index, _)| cursor + index)
}

fn word_class(character: char) -> bool {
    const SEPARATORS: &str = "`~!@#$%^&*()-=+[{]}\\|;:'\",.<>/?";
    SEPARATORS.contains(character)
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
        return (KeyCode::Char(character.to_ascii_lowercase()), modifiers);
    }
    (code, modifiers)
}

fn matching_indices<T>(
    values: &[T],
    query: &str,
    fields: impl Fn(&T) -> (&str, Option<&str>),
) -> Vec<usize> {
    let query = query.to_lowercase();
    let prefix = values
        .iter()
        .enumerate()
        .filter_map(|(index, value)| {
            fields(value)
                .0
                .to_lowercase()
                .starts_with(&query)
                .then_some(index)
        })
        .collect::<Vec<_>>();
    if !prefix.is_empty() {
        return prefix;
    }
    values
        .iter()
        .enumerate()
        .filter_map(|(index, value)| {
            let (primary, secondary) = fields(value);
            (primary.to_lowercase().contains(&query)
                || secondary.is_some_and(|secondary| secondary.to_lowercase().contains(&query)))
            .then_some(index)
        })
        .collect()
}

fn builtin_command_choices() -> Vec<CommandChoice> {
    [
        ("help", "show available Hel and agent commands", None),
        (
            "detach",
            "return to the dashboard without stopping the worker",
            None,
        ),
        (
            "checkpoint",
            "checkpoint the current session",
            Some("reason"),
        ),
        ("model", "change the active model while idle", Some("value")),
        (
            "effort",
            "change the active reasoning effort while idle",
            Some("value"),
        ),
    ]
    .into_iter()
    .map(|(name, description, input_hint)| CommandChoice {
        name: name.to_owned(),
        description: description.to_owned(),
        input_hint: input_hint.map(str::to_owned),
        source: CommandSource::Hel,
    })
    .collect()
}

fn parse_local_command(prompt: &str) -> Option<(LocalCommand, &str)> {
    let command = prompt.strip_prefix('/')?;
    let (name, args) = command
        .split_once(char::is_whitespace)
        .map_or((command, ""), |(name, args)| (name, args.trim()));
    let command = match name {
        "help" => LocalCommand::Help,
        "detach" => LocalCommand::Detach,
        "checkpoint" => LocalCommand::Checkpoint,
        "model" => LocalCommand::Model,
        "effort" => LocalCommand::Effort,
        _ => return None,
    };
    Some((command, args))
}

fn config_values(options: &[SessionConfigOption], key: &str) -> Vec<ConfigValueChoice> {
    let option = match key {
        "model" => options
            .iter()
            .find(|option| option.id.to_string() == "model")
            .or_else(|| {
                options.iter().find(|option| {
                    option.category == Some(SessionConfigOptionCategory::Model)
                        && !matches!(
                            option.id.to_string().as_str(),
                            "effort" | "reasoning_effort"
                        )
                })
            }),
        "effort" => options
            .iter()
            .find(|option| option.category == Some(SessionConfigOptionCategory::ThoughtLevel))
            .or_else(|| {
                options.iter().find(|option| {
                    matches!(
                        option.id.to_string().as_str(),
                        "effort" | "reasoning_effort"
                    )
                })
            }),
        _ => None,
    };
    let Some(option) = option else {
        return Vec::new();
    };
    let SessionConfigKind::Select(select) = &option.kind else {
        return Vec::new();
    };
    let choices = match &select.options {
        SessionConfigSelectOptions::Ungrouped(options) => options.iter().collect::<Vec<_>>(),
        SessionConfigSelectOptions::Grouped(groups) => {
            groups.iter().flat_map(|group| &group.options).collect()
        }
        _ => Vec::new(),
    };
    choices
        .into_iter()
        .map(|choice| ConfigValueChoice {
            value: choice.value.to_string(),
            name: choice.name.clone(),
            description: choice.description.clone(),
        })
        .collect()
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

fn tool_status(status: &ToolCallStatus) -> ToolStatus {
    match status {
        ToolCallStatus::InProgress => ToolStatus::Running,
        ToolCallStatus::Completed => ToolStatus::Completed,
        ToolCallStatus::Failed => ToolStatus::Failed,
        _ => ToolStatus::Pending,
    }
}

fn plan_status(status: &PlanEntryStatus) -> PlanStatus {
    match status {
        PlanEntryStatus::InProgress => PlanStatus::Running,
        PlanEntryStatus::Completed => PlanStatus::Completed,
        _ => PlanStatus::Pending,
    }
}

fn content_block_text(content: &ContentBlock) -> Option<String> {
    match content {
        ContentBlock::Text(text) => Some(text.text.clone()),
        ContentBlock::Image(_) => Some("[image]".into()),
        ContentBlock::Audio(_) => Some("[audio]".into()),
        ContentBlock::ResourceLink(link) => Some(format!("[{}]({})", link.name, link.uri)),
        ContentBlock::Resource(resource) => Some(match &resource.resource {
            EmbeddedResourceResource::TextResourceContents(resource) => resource.text.clone(),
            EmbeddedResourceResource::BlobResourceContents(resource) => {
                format!("[embedded resource: {}]", resource.uri)
            }
            _ => "[embedded resource]".into(),
        }),
        _ => None,
    }
}

fn tool_content_details(content: &[ToolCallContent]) -> Vec<String> {
    const MAX_DETAILS: usize = 8;
    let mut details = Vec::new();
    for item in content {
        let detail = match item {
            ToolCallContent::Content(content) => content_block_text(&content.content),
            ToolCallContent::Diff(diff) => Some(format_diffstat(diff)),
            ToolCallContent::Terminal(terminal) => {
                Some(format!("terminal {}", terminal.terminal_id))
            }
            _ => None,
        };
        if let Some(detail) = detail {
            details.push(sanitize_terminal_text(&detail));
        }
        if details.len() == MAX_DETAILS {
            return details;
        }
    }
    details
}

fn tool_diffstats(content: &[ToolCallContent]) -> Vec<String> {
    content
        .iter()
        .filter_map(|item| match item {
            ToolCallContent::Diff(diff) => Some(format_diffstat(diff)),
            _ => None,
        })
        .collect()
}

fn format_diffstat(diff: &agent_client_protocol::schema::v1::Diff) -> String {
    let old_text = diff.old_text.as_deref().unwrap_or_default();
    let changes = TextDiff::from_lines(old_text, &diff.new_text);
    let (insertions, deletions) =
        changes
            .iter_all_changes()
            .fold((0, 0), |(insertions, deletions), change| {
                match change.tag() {
                    ChangeTag::Insert => (insertions + 1, deletions),
                    ChangeTag::Delete => (insertions, deletions + 1),
                    ChangeTag::Equal => (insertions, deletions),
                }
            });
    format!("{}  +{insertions} −{deletions}", diff.path.display())
}

fn tool_location_details(locations: &[ToolCallLocation]) -> Vec<String> {
    locations
        .iter()
        .take(8)
        .map(|location| match location.line {
            Some(line) => format!("{}:{line}", location.path.display()),
            None => location.path.display().to_string(),
        })
        .collect()
}

/// Run chat until the user presses Escape. This detaches the proxy and leaves
/// the target worker alive.
pub async fn run_chat(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    mut client: WorkerClient,
    chat: Option<ChatState>,
    bundle_id: &str,
) -> Result<(ChatExit, WorkerClient, ChatState)> {
    let mut chat = match chat {
        Some(chat) => chat,
        None => {
            let bootstrap = client.bootstrap().await?;
            ChatState::new(&bootstrap.snapshot, &bootstrap.events)
        }
    };
    chat.set_history_context(bundle_id);
    let (voice_updates_tx, mut voice_updates_rx) =
        tokio::sync::mpsc::unbounded_channel::<VoiceUpdate>();
    let mut voice_cancel: Option<std::sync::mpsc::Sender<()>> = None;
    let mut voice_prefix = String::new();
    loop {
        while let Ok(update) = voice_updates_rx.try_recv() {
            match update {
                VoiceUpdate::Partial(text) => {
                    chat.set_input(append_dictation(&voice_prefix, &text))
                }
                VoiceUpdate::Status(status) => chat.set_notice(status),
                VoiceUpdate::Finished(result) => {
                    chat.voice_active = false;
                    voice_cancel = None;
                    match result {
                        Ok(text) => {
                            chat.set_input(append_dictation(&voice_prefix, &text));
                            chat.notice = None;
                        }
                        Err(error) => {
                            chat.set_notice(crate::speech::dictation_error_message(&error))
                        }
                    }
                }
            }
        }
        if chat.phase == WorkerPhase::Idle
            && let Some(queued) = chat.queued_prompts.pop_front()
        {
            match client.prompt(queued.text.clone(), Vec::new()).await {
                Ok(sequence) => {
                    chat.record_accepted_prompt(sequence, &queued.text);
                    chat.mark_prompt_submitted();
                }
                Err(error) => {
                    let dropped = chat.queued_prompts.len();
                    chat.queued_prompts.clear();
                    chat.set_input(queued.text);
                    chat.set_notice(if dropped == 0 {
                        format!("Queued prompt failed: {error:#}")
                    } else {
                        format!(
                            "Queued prompt failed: {error:#}; dropped {dropped} later prompt(s)"
                        )
                    });
                }
            }
        }
        terminal.draw(|frame| render(frame, &mut chat))?;
        // Drain every queued input event before redrawing or syncing: a paste
        // delivers thousands of key events, and one draw + worker sync per
        // character would lag the trailing Enter by minutes.
        let mut pending = event::poll(Duration::from_millis(150))?;
        while pending {
            let action = match event::read()? {
                Event::Key(key) => Some(chat.handle_key(key)),
                Event::Mouse(mouse) => {
                    chat.handle_mouse(mouse);
                    None
                }
                _ => None,
            };
            if let Some(action) = action {
                let result = match action {
                    ChatAction::None => None,
                    ChatAction::Prompt(text) => match client.prompt(text.clone(), Vec::new()).await
                    {
                        Ok(sequence) => {
                            chat.record_accepted_prompt(sequence, &text);
                            chat.mark_prompt_submitted();
                            None
                        }
                        Err(error) => {
                            chat.set_input(text);
                            Some(error)
                        }
                    },
                    ChatAction::SetConfig { key, value } => {
                        client.set_config(key, value).await.err()
                    }
                    ChatAction::Cancel => client.cancel().await.err(),
                    ChatAction::Checkpoint(reason) => client
                        .checkpoint(Some(
                            reason.unwrap_or_else(|| "manual chat checkpoint".into()),
                        ))
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
                        chat.reset_interaction();
                        return Ok((
                            ChatExit::Detached {
                                last_seen_event_sequence,
                            },
                            client,
                            chat,
                        ));
                    }
                };
                if let Some(error) = result {
                    chat.set_notice(format!("{error:#}"));
                }
            }
            pending = event::poll(Duration::from_millis(0))?;
        }
        match client.sync().await {
            Ok(events) => {
                chat.apply_events(&events);
            }
            Err(error) => chat.set_notice(format!("connection lost: {error:#}")),
        }
    }
}

pub fn render(frame: &mut Frame, chat: &mut ChatState) {
    let area = frame.area();
    let outer = Block::default()
        .borders(Borders::ALL)
        .title(format!(" HEL / {} ", short_id(&chat.session_id)))
        .border_style(Style::default().fg(Color::DarkGray));
    let inner = outer.inner(area);
    frame.render_widget(outer, area);
    let visible_queued = chat.queued_prompts.len().min(3) as u16;
    let prompt_width = usize::from(inner.width.saturating_sub(2)).max(1);
    let input_rows = input_visual_rows(&chat.input, prompt_width) as u16;
    let desired_prompt_height = input_rows
        .saturating_add(visible_queued)
        .saturating_add(2)
        .max(4);
    let maximum_prompt_height = inner.height.saturating_sub(6).max(3);
    let prompt_height = desired_prompt_height.min(maximum_prompt_height);
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(5),
            Constraint::Length(prompt_height),
            Constraint::Length(1),
        ])
        .split(inner);
    render_transcript(frame, chunks[0], chat);
    let queued = chat.queued_prompts.len();
    let prompt_title = match (chat.phase, queued) {
        (WorkerPhase::Idle, 0) => " Prompt ".to_owned(),
        (WorkerPhase::Idle, queued) => format!(" Prompt · {queued} queued "),
        (WorkerPhase::Running, 0) => " Running · Esc cancels ".to_owned(),
        (WorkerPhase::Running, queued) => {
            format!(" Running · {queued} queued · Esc cancels ")
        }
        (WorkerPhase::Closing, _) => " Closing ".to_owned(),
        (WorkerPhase::Closed, _) => " Closed ".to_owned(),
    };
    let prompt_block = Block::default().borders(Borders::ALL).title(prompt_title);
    let prompt_inner = prompt_block.inner(chunks[1]);
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
                        "queued {}: {}",
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
        chunks[1],
    );
    if chat.history_search.is_none() {
        set_input_cursor(
            frame,
            prompt_inner,
            &chat.input,
            chat.input_cursor,
            queue_rows,
            input_scroll,
        );
    }
    let default_footer = if chat.voice_active {
        "Listening… Ctrl-V stop · Ctrl-G dashboard"
    } else {
        "Enter send/queue · Shift-Enter newline · Ctrl-R history · Ctrl-T transcript · Esc cancel · Ctrl-G dashboard"
    };
    let search_footer = chat.history_search.as_ref().map(history_search_footer);
    let footer = search_footer
        .as_deref()
        .or(chat.notice.as_deref())
        .unwrap_or(default_footer);
    frame.render_widget(
        Paragraph::new(footer).style(Style::default().fg(Color::DarkGray)),
        chunks[2],
    );
    if let Some(search) = chat.history_search.as_ref()
        && chunks[2].width > 0
    {
        let prefix = format!("reverse-i-search [{}]: ", history_scope_name(search.scope));
        let column = display_width(&prefix) + display_width(&search.query);
        frame.set_cursor_position((
            chunks[2].x + column.min(usize::from(chunks[2].width.saturating_sub(1))) as u16,
            chunks[2].y,
        ));
    }
    render_autocomplete(frame, chunks[1], chat);
}

fn set_input_cursor(
    frame: &mut Frame,
    area: Rect,
    input: &str,
    cursor: usize,
    queue_rows: usize,
    scroll: usize,
) {
    if area.width == 0 || area.height == 0 {
        return;
    }
    let width = usize::from(area.width);
    let (column, input_row) = input_cursor_visual_position(input, cursor, width);
    let row = queue_rows.saturating_add(input_row).saturating_sub(scroll);
    if row < usize::from(area.height) {
        frame.set_cursor_position((
            area.x + column.min(width.saturating_sub(1)) as u16,
            area.y + row as u16,
        ));
    }
}

fn input_visual_rows(input: &str, width: usize) -> usize {
    input_cursor_visual_position(input, input.len(), width).1 + 1
}

fn input_cursor_visual_position(input: &str, cursor: usize, width: usize) -> (usize, usize) {
    let width = width.max(1);
    let mut row = 0;
    let mut column = 0;
    for grapheme in input[..cursor].graphemes(true) {
        if grapheme == "\n" {
            row += 1;
            column = 0;
            continue;
        }
        let grapheme_width = display_width(grapheme).max(1);
        if column + grapheme_width > width {
            row += 1;
            column = 0;
        }
        column += grapheme_width;
        if column >= width {
            row += 1;
            column = 0;
        }
    }
    (column, row)
}

fn history_scope_name(scope: HistoryScope) -> &'static str {
    match scope {
        HistoryScope::Project => "project",
        HistoryScope::Session => "session",
        HistoryScope::All => "all projects",
    }
}

fn history_search_footer(search: &HistorySearch) -> String {
    let mut footer = format!(
        "reverse-i-search [{}]: {}",
        history_scope_name(search.scope),
        search.query
    );
    if let Some(error) = search.unavailable.as_deref() {
        footer.push_str(&format!("  unavailable: {error}"));
    } else if !search.query.is_empty() && search.matches.is_empty() {
        footer.push_str("  no match");
    } else if let Some(selected) = search.selected {
        footer.push_str(&format!(
            "  {}/{} · Enter accept · Esc cancel · Alt-R scope",
            selected + 1,
            search.matches.len()
        ));
    } else {
        footer.push_str("  type to search · Alt-R scope · Esc cancel");
    }
    footer
}

fn highlighted_input_lines(input: &str, query: &str) -> Vec<Line<'static>> {
    let ranges = case_insensitive_match_ranges(input, query);
    let mut lines = Vec::new();
    let mut line_start = 0;
    for line in input.split('\n') {
        let line_end = line_start + line.len();
        let mut spans = Vec::new();
        let mut cursor = line_start;
        for range in ranges
            .iter()
            .filter(|range| range.start < line_end && range.end > line_start)
        {
            let start = range.start.max(line_start);
            let end = range.end.min(line_end);
            if cursor < start {
                spans.push(Span::raw(input[cursor..start].to_owned()));
            }
            spans.push(Span::styled(
                input[start..end].to_owned(),
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ));
            cursor = end;
        }
        if cursor < line_end {
            spans.push(Span::raw(input[cursor..line_end].to_owned()));
        }
        lines.push(Line::from(spans));
        line_start = line_end.saturating_add(1);
    }
    lines
}

fn case_insensitive_match_ranges(text: &str, query: &str) -> Vec<std::ops::Range<usize>> {
    if query.is_empty() {
        return Vec::new();
    }
    let query = query
        .chars()
        .flat_map(char::to_lowercase)
        .collect::<String>();
    let mut folded = String::new();
    let mut spans = Vec::new();
    for (start, character) in text.char_indices() {
        let original = start..start + character.len_utf8();
        for lower in character.to_lowercase() {
            let folded_start = folded.len();
            folded.push(lower);
            spans.push((folded_start..folded.len(), original.clone()));
        }
    }
    let mut ranges = Vec::new();
    let mut from = 0;
    while from <= folded.len()
        && let Some(relative) = folded[from..].find(&query)
    {
        let start = from + relative;
        let end = start + query.len();
        let first = spans
            .iter()
            .find(|(range, _)| range.end > start && range.start < end)
            .map(|(_, original)| original.start);
        let last = spans
            .iter()
            .rev()
            .find(|(range, _)| range.end > start && range.start < end)
            .map(|(_, original)| original.end);
        if let (Some(first), Some(last)) = (first, last) {
            ranges.push(first..last);
        }
        from = end;
    }
    ranges
}

fn render_autocomplete(frame: &mut Frame, prompt_area: Rect, chat: &ChatState) {
    let Some(autocomplete) = chat.autocomplete.as_ref() else {
        return;
    };
    let visible = autocomplete.matches.len().min(8);
    if visible == 0 {
        return;
    }
    let height = (visible as u16).saturating_add(2);
    let area = Rect::new(
        prompt_area.x,
        prompt_area.y.saturating_sub(height),
        prompt_area.width,
        height,
    );
    frame.render_widget(Clear, area);
    let title = match autocomplete.kind {
        AutocompleteKind::Commands => " commands · ↑/↓ select · Tab/Enter accept ",
        AutocompleteKind::ConfigValues { .. } => " values · ↑/↓ select · Tab/Enter accept ",
    };
    let block = Block::default().borders(Borders::ALL).title(title);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let start = autocomplete
        .selected
        .saturating_sub(visible.saturating_sub(1));
    let items = autocomplete.matches[start..]
        .iter()
        .take(visible)
        .enumerate()
        .filter_map(|(offset, index)| {
            let selected = start + offset == autocomplete.selected;
            autocomplete_row(chat, autocomplete.kind, *index).map(|row| {
                ListItem::new(truncate_to_width(&row, usize::from(inner.width))).style(
                    if selected {
                        Style::default()
                            .fg(Color::Black)
                            .bg(Color::White)
                            .add_modifier(Modifier::BOLD)
                    } else {
                        Style::default()
                    },
                )
            })
        })
        .collect::<Vec<_>>();
    frame.render_widget(List::new(items), inner);
}

fn autocomplete_row(chat: &ChatState, kind: AutocompleteKind, index: usize) -> Option<String> {
    match kind {
        AutocompleteKind::Commands => {
            let command = chat.command_choices.get(index)?;
            let hint = command
                .input_hint
                .as_deref()
                .map(|hint| format!(" <{hint}>"))
                .unwrap_or_default();
            let source = match command.source {
                CommandSource::Hel => "hel",
                CommandSource::Agent => "agent",
            };
            Some(format!(
                "/{}{hint}  — {} [{source}]",
                command.name, command.description
            ))
        }
        AutocompleteKind::ConfigValues { key: "model" } => {
            config_value_row(chat.model_values.get(index)?)
        }
        AutocompleteKind::ConfigValues { key: "effort" } => {
            config_value_row(chat.effort_values.get(index)?)
        }
        AutocompleteKind::ConfigValues { .. } => None,
    }
}

fn config_value_row(choice: &ConfigValueChoice) -> Option<String> {
    let description = choice
        .description
        .as_deref()
        .filter(|description| !description.trim().is_empty())
        .map(|description| format!(" — {description}"))
        .unwrap_or_default();
    Some(format!("{} ({}){description}", choice.name, choice.value))
}

fn truncate_to_width(text: &str, width: usize) -> String {
    if text.chars().count() <= width {
        return text.to_owned();
    }
    if width <= 1 {
        return "…".chars().take(width).collect();
    }
    let mut truncated = text.chars().take(width - 1).collect::<String>();
    truncated.push('…');
    truncated
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

fn render_transcript(frame: &mut Frame, area: Rect, chat: &mut ChatState) {
    let lines = transcript_lines(chat, area.width);
    let viewport_height = usize::from(area.height.saturating_sub(2));
    let maximum = lines.len().saturating_sub(viewport_height);
    chat.last_content_height = lines.len();
    chat.last_viewport_height = viewport_height;
    if chat.follow_bottom {
        chat.scroll_top = maximum;
    } else {
        chat.scroll_top = chat.scroll_top.min(maximum);
    }
    let title = if chat.follow_bottom {
        match chat.render_mode {
            TranscriptRenderMode::Rich => " Conversation ".to_owned(),
            TranscriptRenderMode::Raw => " Conversation · raw source ".to_owned(),
        }
    } else {
        format!(
            " Conversation · rows {}–{} of {} · End to follow ",
            chat.scroll_top.saturating_add(1),
            (chat.scroll_top + viewport_height).min(lines.len()),
            lines.len()
        )
    };
    let block = Block::default()
        .borders(Borders::TOP | Borders::BOTTOM)
        .title(title)
        .border_style(Style::default().fg(Color::DarkGray));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let visible = lines
        .into_iter()
        .skip(chat.scroll_top)
        .take(usize::from(inner.height))
        .collect::<Vec<_>>();
    frame.render_widget(Paragraph::new(visible), inner);
}

const ROLE_GUTTER: &str = "│ ";
const ROLE_GUTTER_WIDTH: usize = 2;

pub(crate) fn render_agent_message_preview(
    source: &str,
    width: usize,
    maximum_lines: usize,
) -> Vec<Line<'static>> {
    if width == 0 || maximum_lines == 0 {
        return Vec::new();
    }
    let agent_style = Style::default().fg(Color::Green);
    let content_width = width.saturating_sub(ROLE_GUTTER_WIDTH).max(1);
    let mut lines = markdown_lines(
        &sanitize_terminal_text(source),
        Style::default(),
        agent_style,
        content_width,
    )
    .into_iter()
    .flat_map(|logical| wrap_styled_line(logical.line, content_width, logical.continuation_indent))
    .map(|line| {
        if line_is_empty(&line) {
            Line::from("")
        } else {
            with_role_gutter(line, agent_style)
        }
    })
    .collect::<Vec<_>>();
    if lines.len() > maximum_lines {
        let omitted_characters = lines[maximum_lines..]
            .iter()
            .map(|line| {
                line_character_count(line).saturating_sub(if !line_is_empty(line) {
                    ROLE_GUTTER_WIDTH
                } else {
                    0
                })
            })
            .sum();
        lines.truncate(maximum_lines);
        if let Some(last) = lines.last_mut() {
            *last = append_omitted_character_count(
                std::mem::take(last),
                width,
                omitted_characters,
                agent_style,
            );
        }
    }
    lines
}

/// Render the complete transcript into already-wrapped visual rows. Keeping
/// layout separate from painting makes scrolling a count of actual terminal
/// rows rather than logical message lines.
fn transcript_lines(chat: &mut ChatState, width: u16) -> Vec<Line<'static>> {
    transcript_entry_lines(
        &chat.entries,
        &mut chat.render_cache,
        width,
        chat.render_mode,
    )
}

fn transcript_entry_lines(
    entries: &[ChatEntry],
    render_cache: &mut TranscriptRenderCache,
    width: u16,
    mode: TranscriptRenderMode,
) -> Vec<Line<'static>> {
    if render_cache.width != width || render_cache.mode != mode {
        render_cache.width = width;
        render_cache.mode = mode;
        render_cache.entries.clear();
    }
    render_cache.entries.resize(entries.len(), None);
    let mut lines = Vec::new();
    for (index, entry) in entries.iter().enumerate() {
        let cached = render_cache.entries[index]
            .as_ref()
            .filter(|cached| cached.revision == entry.revision)
            .map(|cached| cached.lines.clone());
        let entry_lines = cached.unwrap_or_else(|| {
            let rendered = render_transcript_entry(entry, usize::from(width), mode);
            render_cache.entries[index] = Some(CachedEntry {
                revision: entry.revision,
                lines: rendered.clone(),
            });
            rendered
        });
        lines.extend(entry_lines);
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

fn render_transcript_entry(
    entry: &ChatEntry,
    width: usize,
    mode: TranscriptRenderMode,
) -> Vec<Line<'static>> {
    let mut out = Vec::new();
    let visual = entry_visual(entry);
    let header = Line::from(vec![
        Span::styled(
            format!("{} ", visual.glyph),
            visual.header_style.add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            visual.label.clone(),
            visual.header_style.add_modifier(Modifier::BOLD),
        ),
    ]);
    out.extend(wrap_styled_line(header, width, ROLE_GUTTER_WIDTH));

    let content_width = width.saturating_sub(ROLE_GUTTER_WIDTH).max(1);
    let logical_lines = entry_logical_lines(entry, mode, &visual, content_width);
    for logical in logical_lines {
        for row in wrap_styled_line(logical.line, content_width, logical.continuation_indent) {
            if line_is_empty(&row) {
                out.push(Line::from(""));
            } else {
                out.push(with_role_gutter(row, visual.rail_style));
            }
        }
    }
    out.push(Line::from(""));
    out
}

fn entry_logical_lines(
    entry: &ChatEntry,
    mode: TranscriptRenderMode,
    visual: &EntryVisual,
    width: usize,
) -> Vec<LogicalLine> {
    if entry.role == ChatRole::Plan {
        return entry
            .plan
            .iter()
            .map(|item| {
                let (glyph, style) = match item.status {
                    PlanStatus::Pending => ("○", Style::default().fg(Color::DarkGray)),
                    PlanStatus::Running => ("●", Style::default().fg(Color::Yellow)),
                    PlanStatus::Completed => ("✓", Style::default().fg(Color::Green)),
                };
                LogicalLine {
                    line: Line::from(vec![
                        Span::styled(format!("{glyph} "), style),
                        Span::styled(item.text.clone(), visual.body_style),
                    ]),
                    continuation_indent: 2,
                }
            })
            .collect();
    }

    let details = entry
        .tool_content
        .iter()
        .chain(&entry.tool_locations)
        .take(8)
        .cloned()
        .collect::<Vec<_>>();
    let source = match mode {
        TranscriptRenderMode::Raw if !details.is_empty() => {
            format!("{}\n{}", entry.text, details.join("\n"))
        }
        TranscriptRenderMode::Rich if !entry.tool_diffstats.is_empty() => {
            format!("{}\n{}", entry.text, entry.tool_diffstats.join("\n"))
        }
        _ => entry.text.clone(),
    };
    match mode {
        TranscriptRenderMode::Rich => {
            markdown_lines(&source, visual.body_style, visual.header_style, width)
        }
        TranscriptRenderMode::Raw => raw_lines(&source, visual.body_style),
    }
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
        ChatRole::Plan => {
            let style = Style::default().fg(Color::Magenta);
            EntryVisual {
                glyph: "◇",
                label: "Plan".into(),
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

    fn mouse(kind: MouseEventKind) -> MouseEvent {
        MouseEvent {
            kind,
            column: 0,
            row: 0,
            modifiers: KeyModifiers::NONE,
        }
    }

    fn ctrl(character: char) -> KeyEvent {
        KeyEvent::new(KeyCode::Char(character), KeyModifiers::CONTROL)
    }

    fn transcript_text(chat: &mut ChatState, width: u16) -> Vec<String> {
        transcript_lines(chat, width)
            .into_iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect()
            })
            .collect()
    }

    fn line_text(lines: Vec<Line<'static>>) -> Vec<String> {
        lines
            .into_iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect()
            })
            .collect()
    }

    #[test]
    fn agent_preview_reports_omitted_character_count_after_four_rendered_rows() {
        let lines = render_agent_message_preview(
            "**one** two three four five six seven eight nine ten eleven twelve",
            12,
            4,
        );
        let rendered = lines
            .iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect::<String>()
            })
            .collect::<Vec<_>>();

        assert_eq!(rendered.len(), 4);
        assert!(rendered[0].starts_with(ROLE_GUTTER));
        assert_eq!(lines[3].spans.last().unwrap().content, "[31 more]");
        assert!(!rendered[3].contains('…'));
        assert_eq!(
            lines[3].spans.last().and_then(|span| span.style.fg),
            Some(Color::Green)
        );
        assert!(
            lines[0]
                .spans
                .iter()
                .any(|span| span.style.add_modifier.contains(Modifier::BOLD))
        );
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
    fn reset_interaction_preserves_projected_transcript_and_render_cache() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.entries
            .push(ChatEntry::plain(1, ChatRole::Agent, "cached response"));
        let _ = transcript_text(&mut chat, 80);
        chat.set_input("draft".into());
        chat.prompt_history.push("previous".into());
        chat.queued_prompts.push_back(QueuedPrompt {
            text: "queued".into(),
        });
        chat.scroll_top = 4;
        chat.follow_bottom = false;
        chat.notice = Some("temporary".into());
        chat.voice_active = true;

        chat.reset_interaction();

        assert_eq!(chat.entries.len(), 1);
        assert!(chat.render_cache.entries[0].is_some());
        assert!(chat.input.is_empty());
        assert!(chat.prompt_history.is_empty());
        assert!(chat.queued_prompts.is_empty());
        assert_eq!(chat.scroll_top, 0);
        assert!(chat.follow_bottom);
        assert!(chat.notice.is_none());
        assert!(!chat.voice_active);
    }

    #[test]
    fn control_g_detaches_without_emitting_close() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        let control_g = KeyEvent::new(KeyCode::Char('g'), KeyModifiers::CONTROL);
        assert_eq!(chat.handle_key(control_g), ChatAction::Back);
    }

    #[test]
    fn enter_sends_while_idle_and_queues_while_running() {
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
        assert_eq!(chat.queued_prompts.len(), 1);
        assert_eq!(chat.queued_prompts[0].text, "x");
        assert!(chat.entries.is_empty());
    }

    #[test]
    fn submitting_a_prompt_clears_a_stale_queue_notice() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.set_notice("Queued 1: next");

        chat.mark_prompt_submitted();

        assert_eq!(chat.phase, WorkerPhase::Running);
        assert!(chat.notice.is_none());
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
        chat.queued_prompts.push_back(QueuedPrompt {
            text: "next".into(),
        });
        chat.apply_event(&SequencedEvent {
            seq: 1,
            request_id: Some("cancel".into()),
            event: WorkerEvent::Cancelled,
        });
        assert_eq!(chat.phase, WorkerPhase::Running);

        chat.apply_event(&SequencedEvent {
            seq: 2,
            request_id: None,
            event: WorkerEvent::TurnCompleted,
        });
        assert_eq!(chat.phase, WorkerPhase::Idle);
        assert_eq!(chat.queued_prompts.front().unwrap().text, "next");
    }

    #[test]
    fn alt_up_recovers_the_latest_queued_prompt_for_editing() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.queued_prompts.push_back(QueuedPrompt {
            text: "first".into(),
        });
        chat.queued_prompts.push_back(QueuedPrompt {
            text: "second".into(),
        });

        chat.handle_key(KeyEvent::new(KeyCode::Up, KeyModifiers::ALT));

        assert_eq!(chat.input, "second");
        assert_eq!(chat.queued_prompts.len(), 1);
        assert_eq!(chat.queued_prompts[0].text, "first");
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
    fn local_command_parser_requires_an_exact_command_boundary() {
        assert_eq!(
            parse_local_command("/checkpoint before refactor"),
            Some((LocalCommand::Checkpoint, "before refactor"))
        );
        assert_eq!(parse_local_command("/checkpointing"), None);
        assert_eq!(parse_local_command("explain /checkpoint"), None);
    }

    #[test]
    fn autocomplete_merges_agent_commands_without_overriding_hel_commands() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.apply_session_update(
            1,
            &serde_json::json!({
                "sessionUpdate": "available_commands_update",
                "availableCommands": [
                    {"name": "review", "description": "agent review", "input": {"hint": "scope"}},
                    {"name": "help", "description": "agent help"}
                ]
            }),
        );
        assert!(
            chat.command_choices.iter().any(|command| {
                command.name == "review" && command.source == CommandSource::Agent
            })
        );
        assert_eq!(
            chat.command_choices
                .iter()
                .filter(|command| command.name == "help")
                .count(),
            1
        );

        chat.set_input("/rev".into());
        assert!(chat.accept_autocomplete());
        assert_eq!(chat.input, "/review ");
    }

    #[test]
    fn config_value_autocomplete_uses_advertised_acp_choices() {
        use agent_client_protocol::schema::v1::{
            SessionConfigSelectOption, SessionConfigSelectOptions,
        };

        let options = vec![
            SessionConfigOption::select(
                "model",
                "Model",
                "auto",
                SessionConfigSelectOptions::Ungrouped(vec![
                    SessionConfigSelectOption::new("auto", "Auto"),
                    SessionConfigSelectOption::new("gpt-5.6-luna", "Luna"),
                ]),
            )
            .category(SessionConfigOptionCategory::Model),
        ];
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.set_config_options(&options);
        chat.set_input("/model lun".into());

        assert!(chat.accept_autocomplete());
        assert_eq!(chat.input, "/model gpt-5.6-luna");
    }

    #[test]
    fn editor_supports_cursor_insertion_deletion_and_prompt_history() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.set_input("ac".into());
        chat.handle_key(key(KeyCode::Left));
        chat.handle_key(key(KeyCode::Char('b')));
        assert_eq!(chat.input, "abc");
        chat.handle_key(key(KeyCode::Backspace));
        assert_eq!(chat.input, "ac");
        chat.handle_key(key(KeyCode::Delete));
        assert_eq!(chat.input, "a");

        chat.set_input("remember me".into());
        assert_eq!(
            chat.handle_key(key(KeyCode::Enter)),
            ChatAction::Prompt("remember me".into())
        );
        chat.phase = WorkerPhase::Idle;
        chat.handle_key(key(KeyCode::Up));
        assert_eq!(chat.input, "remember me");
        chat.handle_key(key(KeyCode::Down));
        assert!(chat.input.is_empty());
    }

    #[test]
    fn readline_line_movement_kill_and_yank_match_codex() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.set_input("alpha beta\ngamma".into());
        chat.handle_key(ctrl('a'));
        assert_eq!(&chat.input[chat.input_cursor..], "gamma");
        chat.handle_key(ctrl('a'));
        assert_eq!(chat.input_cursor, 0);
        chat.handle_key(ctrl('e'));
        assert_eq!(&chat.input[..chat.input_cursor], "alpha beta");
        chat.handle_key(ctrl('k'));
        assert_eq!(chat.input, "alpha betagamma");
        chat.handle_key(ctrl('y'));
        assert_eq!(chat.input, "alpha beta\ngamma");
    }

    #[test]
    fn readline_word_edits_and_grapheme_cursor_are_atomic() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.set_input("one two 👩‍💻".into());
        chat.handle_key(key(KeyCode::Left));
        assert_eq!(&chat.input[chat.input_cursor..], "👩‍💻");
        chat.handle_key(ctrl('w'));
        assert_eq!(chat.input, "one 👩‍💻");
        chat.handle_key(ctrl('y'));
        assert_eq!(chat.input, "one two 👩‍💻");
        chat.handle_key(KeyEvent::new(KeyCode::Char('b'), KeyModifiers::ALT));
        assert_eq!(&chat.input[chat.input_cursor..], "two 👩‍💻");
    }

    #[test]
    fn reverse_search_previews_steps_accepts_and_restores_draft() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.prompt_history = vec!["fix parser".into(), "fix renderer".into()];
        chat.set_input("unfinished".into());
        chat.handle_key(ctrl('r'));
        chat.handle_key(key(KeyCode::Char('f')));
        chat.handle_key(key(KeyCode::Char('i')));
        chat.handle_key(key(KeyCode::Char('x')));
        assert_eq!(chat.input, "fix renderer");
        chat.handle_key(ctrl('r'));
        assert_eq!(chat.input, "fix parser");
        chat.handle_key(key(KeyCode::Esc));
        assert_eq!(chat.input, "unfinished");

        chat.handle_key(ctrl('r'));
        for character in "renderer".chars() {
            chat.handle_key(key(KeyCode::Char(character)));
        }
        chat.handle_key(key(KeyCode::Enter));
        assert_eq!(chat.input, "fix renderer");
        assert!(chat.history_search.is_none());
    }

    #[test]
    fn reverse_search_accepts_raw_control_events_and_alt_r_cycles_scope() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.handle_key(key(KeyCode::Char('\u{12}')));
        assert!(chat.history_search.is_some());
        chat.handle_key(KeyEvent::new(KeyCode::Char('r'), KeyModifiers::ALT));
        assert_eq!(
            chat.history_search.as_ref().unwrap().scope,
            HistoryScope::Session
        );
        chat.handle_key(ctrl('c'));
        assert!(chat.history_search.is_none());
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
    fn acp_diffs_render_diffstats_and_replacement_updates_replace_them() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.apply_session_update(
            1,
            &serde_json::json!({
                "sessionUpdate": "tool_call",
                "toolCallId": "edit-lib",
                "title": "Edit src/lib.rs",
                "status": "in_progress",
                "content": [{
                    "type": "diff",
                    "path": "/workspace/src/lib.rs",
                    "oldText": "alpha\n",
                    "newText": "alpha\nbeta\n"
                }]
            }),
        );

        assert_eq!(
            transcript_text(&mut chat, 80),
            [
                "● Tool · running",
                "│ Edit src/lib.rs",
                "│ /workspace/src/lib.rs  +1 −0",
                ""
            ]
        );

        chat.apply_session_update(
            2,
            &serde_json::json!({
                "sessionUpdate": "tool_call_update",
                "toolCallId": "edit-lib",
                "status": "completed",
                "content": [{
                    "type": "diff",
                    "path": "/workspace/src/lib.rs",
                    "oldText": "alpha\n",
                    "newText": "gamma\n"
                }]
            }),
        );

        assert_eq!(
            chat.entries[0].tool_diffstats,
            ["/workspace/src/lib.rs  +1 −1"]
        );
        assert_eq!(
            transcript_text(&mut chat, 80),
            [
                "✓ Tool · done",
                "│ Edit src/lib.rs",
                "│ /workspace/src/lib.rs  +1 −1",
                ""
            ]
        );
    }

    #[test]
    fn acp_new_file_diff_counts_each_inserted_line() {
        let diff = agent_client_protocol::schema::v1::Diff::new("/workspace/new.txt", "one\ntwo\n");

        assert_eq!(format_diffstat(&diff), "/workspace/new.txt  +2 −0");
    }

    #[test]
    fn transcript_blocks_keep_role_headers_and_wrapped_body_indented() {
        let entry = ChatEntry::plain(1, ChatRole::User, "alpha beta gamma");
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.entries.push(entry);
        let text = transcript_text(&mut chat, 12);

        assert_eq!(text, ["❯ You", "│ alpha beta", "│ gamma", ""]);
    }

    #[test]
    fn transcript_snapshot_tail_matches_rich_conversation_rows() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.entries
            .push(ChatEntry::plain(1, ChatRole::User, "inspect the renderer"));
        chat.entries.push(ChatEntry::plain(
            2,
            ChatRole::Agent,
            "**Done.**\n\n- shared renderer\n- live tail",
        ));
        let expected = transcript_text(&mut chat, 32);
        let expected = expected[expected.len().saturating_sub(6)..].to_vec();

        let mut snapshot = chat.transcript_snapshot();
        assert_eq!(line_text(snapshot.rich_tail(32, 6)), expected);
    }

    #[test]
    fn markdown_list_wrapping_uses_a_hanging_indent() {
        let entry = ChatEntry::plain(1, ChatRole::Agent, "- alpha beta gamma");
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.entries.push(entry);
        let text = transcript_text(&mut chat, 13);

        assert!(text.iter().any(|line| line == "│ • alpha"));
        assert!(text.iter().any(|line| line == "│   beta"));
        assert!(text.iter().any(|line| line == "│   gamma"));
    }

    #[test]
    fn page_navigation_keeps_end_attached_to_the_latest_message() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.last_content_height = 30;
        chat.last_viewport_height = 10;
        chat.handle_key(key(KeyCode::PageUp));
        assert_eq!(chat.scroll_top, 10);
        assert!(!chat.follow_bottom);
        chat.handle_key(key(KeyCode::PageDown));
        assert_eq!(chat.scroll_top, 20);
        assert!(chat.follow_bottom);
        chat.handle_key(key(KeyCode::PageUp));
        chat.handle_key(KeyEvent::new(KeyCode::End, KeyModifiers::CONTROL));
        assert!(chat.follow_bottom);
    }

    #[test]
    fn mouse_wheel_scrolls_chat_history_and_resumes_following_at_bottom() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.last_content_height = 30;
        chat.last_viewport_height = 10;

        chat.handle_mouse(mouse(MouseEventKind::ScrollUp));
        assert_eq!(chat.scroll_top, 17);
        assert!(!chat.follow_bottom);

        chat.handle_mouse(mouse(MouseEventKind::ScrollDown));
        assert_eq!(chat.scroll_top, 20);
        assert!(chat.follow_bottom);
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
    fn adjacent_thought_messages_coalesce_without_an_extra_separator() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        for (seq, id, text) in [(1, "one", "first thought"), (2, "two", "second thought")] {
            chat.apply_session_update(
                seq,
                &serde_json::json!({
                    "sessionUpdate": "agent_thought_chunk",
                    "messageId": id,
                    "content": {"type": "text", "text": text}
                }),
            );
        }

        assert_eq!(chat.entries.len(), 1);
        assert_eq!(chat.entries[0].text, "first thought\nsecond thought");
        let rendered = transcript_text(&mut chat, 80);
        assert_eq!(
            rendered
                .iter()
                .filter(|line| line.contains("Thinking"))
                .count(),
            1
        );
        assert_eq!(
            rendered,
            ["○ Thinking", "│ first thought", "│ second thought", ""]
        );
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
    fn raw_mode_preserves_markdown_markers_and_exposes_tool_details() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.entries
            .push(ChatEntry::plain(1, ChatRole::Agent, "**bold**"));
        chat.render_mode = TranscriptRenderMode::Raw;
        assert!(transcript_text(&mut chat, 30).contains(&"│ **bold**".into()));
    }
}
