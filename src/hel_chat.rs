//! Minimal full-screen chat for one persistent Hel worker.

mod rendering;

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agent_client_protocol::schema::v1::{
    AvailableCommand, AvailableCommandInput, ContentBlock, ContentChunk, EmbeddedResourceResource,
    Plan, PlanEntry, PlanEntryPriority, PlanEntryStatus, SessionConfigKind, SessionConfigOption,
    SessionConfigOptionCategory, SessionConfigSelectOptions, SessionUpdate, TextContent, ToolCall,
    ToolCallContent, ToolCallLocation, ToolCallStatus,
};
use anyhow::Result;
use crossterm::event::{
    Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseEvent, MouseEventKind,
};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, Paragraph, Wrap};
use serde::Serialize;
use sha2::{Digest, Sha256};
use similar::{ChangeTag, TextDiff};
use unicode_segmentation::UnicodeSegmentation;

use crate::hel_acp::RuntimeEvent;
use crate::hel_database::{HistoryScope, PromptHistoryEntry};
use crate::hel_recovery::RecoveryContext;
use crate::hel_session_manager::{
    ManagedSessionHandle, ManagedSessionView, SessionManagerControl, new_command_id,
};
use crate::hel_state::{
    MaterializedExecutionState, MaterializedQueuedPrompt, MaterializedSession, TranscriptBody,
    TranscriptItem,
};
use crate::hel_worker::{
    RELAY_EVENT_GENESIS_DIGEST, RelayCommand, SequencedEvent, WorkerEvent, WorkerPhase,
    WorkerSnapshot,
};
use rendering::{
    LogicalLine, TranscriptRenderMode, display_width, markdown_lines, raw_lines,
    sanitize_terminal_text, wrap_styled_line,
};

const MOUSE_SCROLL_ROWS: usize = 3;

/// What one terminal event asked the chat to do.
///
/// `None` means the event only changed local state, which lets the caller keep
/// draining a paste burst before it redraws. Both exits report the ordinal the
/// user has now seen, which becomes the session's read receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatEventOutcome {
    None,
    Handled,
    Back { last_seen_event_ordinal: u64 },
    QuitDetach { last_seen_event_ordinal: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChatAction {
    None,
    Prompt(String),
    RemoveQueuedPrompt { id: String, text: String },
    SetConfig { key: String, value: String },
    Cancel,
    PasteFromClipboard,
    ToggleVoice,
    Back,
    QuitDetach,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalCommand {
    Help,
    Detach,
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
    id: String,
    text: String,
}

#[derive(Debug, Clone)]
struct HistorySearch {
    original_input: String,
    original_cursor: usize,
    generation: u64,
    query: String,
    scope: HistoryScope,
    matches: Vec<PromptHistoryEntry>,
    selected: Option<usize>,
    unavailable: Option<String>,
}

#[derive(Debug, Clone)]
struct HistorySearchRequest {
    generation: u64,
    session_id: String,
    bundle_id: Option<String>,
    scope: HistoryScope,
    query: String,
    local_history: Vec<String>,
}

#[derive(Debug)]
enum ChatIoUpdate {
    ProjectHistoryPrefetched(std::result::Result<Vec<PromptHistoryEntry>, String>),
    HistorySearchResults {
        generation: u64,
        result: std::result::Result<Vec<PromptHistoryEntry>, String>,
    },
    ClipboardText(std::result::Result<String, String>),
    OtherSessions(Vec<OtherSessionActivity>),
}

/// What the chat's session header shows when it opens: where this session sits
/// among the active sessions, and the other sessions it lists.
#[derive(Debug, Clone, Default)]
pub struct SessionHeaderIdentity {
    pub project: String,
    pub position: usize,
    pub others: Vec<OtherSessionIdentity>,
}

/// Identity of another session, snapshotted when the chat opens. Both fields
/// stay fixed for the visit: `position` is the session's place in the ordered
/// list of active sessions at that moment, not a live value.
#[derive(Debug, Clone)]
pub struct OtherSessionIdentity {
    pub session_id: String,
    pub position: usize,
    pub project: String,
}

/// What the session header says about one other session.
#[derive(Debug, Clone, PartialEq, Eq)]
struct OtherSessionActivity {
    position: usize,
    project: String,
    turn_started_at_epoch_seconds: Option<u64>,
    last_agent_line: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChatEntry {
    #[serde(default)]
    start_seq: u64,
    pub seq: u64,
    pub role: ChatRole,
    pub text: String,
    recorded_at_ms: Option<i64>,
    revision: u64,
    message_id: Option<String>,
    tool_call_id: Option<String>,
    tool_status: Option<ToolStatus>,
    tool_content: Vec<String>,
    tool_diffstats: Vec<String>,
    tool_locations: Vec<String>,
    plan: Vec<PlanLine>,
    #[serde(default, skip_serializing_if = "is_false")]
    leading_omitted: bool,
    /// The materialized transcript item this entry was derived from, when it
    /// came from the controller's projection. Provenance only, so it is
    /// neither serialized nor part of the entry's value.
    #[serde(skip)]
    source: TranscriptSource,
}

/// Handle on the transcript item an entry was derived from. Unchanged items
/// keep the same `Arc` from one projection to the next, so a pointer
/// comparison replaces re-reading the item and re-parsing its JSON.
///
/// The handle records where an entry came from, not what it says, so two
/// entries with equal content are equal whatever they were derived from.
#[derive(Debug, Clone, Default)]
struct TranscriptSource(Option<Arc<TranscriptItem>>);

impl TranscriptSource {
    fn is(&self, item: &Arc<TranscriptItem>) -> bool {
        self.0
            .as_ref()
            .is_some_and(|source| Arc::ptr_eq(source, item))
    }
}

impl PartialEq for TranscriptSource {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for TranscriptSource {}

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
            source: TranscriptSource::default(),
        }
    }

    fn plan(seq: u64, plan: Vec<PlanLine>) -> Self {
        Self {
            start_seq: seq,
            seq,
            role: ChatRole::Plan,
            text: String::new(),
            recorded_at_ms: None,
            revision: 0,
            message_id: None,
            tool_call_id: None,
            tool_status: None,
            tool_content: Vec::new(),
            tool_diffstats: Vec::new(),
            tool_locations: Vec::new(),
            plan,
            leading_omitted: false,
            source: TranscriptSource::default(),
        }
    }

    fn touch(&mut self, seq: u64) {
        self.seq = seq;
        self.revision = self.revision.wrapping_add(1);
    }

    #[cfg(test)]
    fn bounded_for_dashboard(mut self) -> Self {
        self.bound_dashboard_content();
        self
    }

    #[cfg(test)]
    fn bound_dashboard_content(&mut self) {
        const TEXT_BYTES: usize = 64 * 1024;
        const DETAIL_BYTES: usize = 2 * 1024;
        const DETAIL_COUNT: usize = 8;

        self.leading_omitted |= truncate_string_start(&mut self.text, TEXT_BYTES);
        for values in [
            &mut self.tool_content,
            &mut self.tool_diffstats,
            &mut self.tool_locations,
        ] {
            values.truncate(DETAIL_COUNT);
            for value in values {
                truncate_string_start(value, DETAIL_BYTES);
            }
        }
        self.plan.truncate(DETAIL_COUNT);
        for line in &mut self.plan {
            truncate_string_start(&mut line.text, DETAIL_BYTES);
        }
    }

    fn with_recorded_at(mut self, recorded_at_ms: Option<i64>) -> Self {
        self.recorded_at_ms = recorded_at_ms;
        self
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[cfg(test)]
fn truncate_string_start(value: &mut String, maximum_bytes: usize) -> bool {
    if value.len() <= maximum_bytes {
        return false;
    }
    let mut start = value.len() - maximum_bytes;
    while !value.is_char_boundary(start) {
        start += 1;
    }
    value.drain(..start);
    true
}

/// The ACP tool states needed to keep a compact tool block visually useful.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
enum ToolStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
enum PlanStatus {
    Pending,
    Running,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
    collapse: Vec<ToolCollapse>,
}

/// Whether an entry renders on its own, heads a collapsed run of completed
/// tools, or is folded into the run above it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ToolCollapse {
    None,
    /// Member of a collapsed run whose summary renders on the run head.
    Hidden,
    /// Head of a collapsed run spanning `self..end` (end exclusive).
    Summary {
        end: usize,
        fingerprint: u64,
    },
}

/// A read-only copy of the projected conversation that can be rendered by
/// surfaces other than the interactive chat view.
#[derive(Debug)]
pub struct TranscriptSnapshot {
    entries: Vec<ChatEntry>,
    latest_seq: u64,
    last_compaction_seq: u64,
    render_cache: TranscriptRenderCache,
}

const BROWSER_TRANSCRIPT_LINES: usize = 1_000;
const BROWSER_LINE_BYTES: usize = 4 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BrowserTranscript {
    pub latest_seq: u64,
    pub window_start_seq: u64,
    pub reset: bool,
    pub entries: Vec<BrowserTranscriptEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BrowserTranscriptEntry {
    pub id: u64,
    pub updated_seq: u64,
    pub role: &'static str,
    pub label: String,
    pub recorded_at_ms: Option<i64>,
    pub lines: Vec<String>,
}

impl TranscriptSnapshot {
    pub fn from_entries(entries: Vec<ChatEntry>) -> Self {
        let latest_seq = entries.last().map_or(0, |entry| entry.seq);
        Self::from_entries_at(entries, latest_seq)
    }

    pub fn from_entries_at(entries: Vec<ChatEntry>, latest_seq: u64) -> Self {
        Self {
            entries,
            latest_seq,
            last_compaction_seq: 0,
            render_cache: TranscriptRenderCache::default(),
        }
    }

    /// Render the controller's durable logical-session projection. Relay
    /// ordinals remain the public cursor: an item's first ordinal is its
    /// stable browser identity, while the current projection frontier is a
    /// conservative update cursor for rebuilt snapshots.
    pub fn from_materialized(session: &MaterializedSession) -> Self {
        let entries = materialized_chat_entries(session);
        Self::from_entries_at(entries, session.applied_event_ordinal)
    }

    pub fn has_assistant_messages(&self) -> bool {
        self.entries
            .iter()
            .any(|entry| entry.role == ChatRole::Agent && !entry.text.trim().is_empty())
    }

    #[cfg(test)]
    fn rich_tail(&mut self, width: u16, maximum_lines: usize) -> Vec<Line<'static>> {
        self.rich_tail_scrolled(width, maximum_lines, 0).0
    }

    /// The last `maximum_lines` non-empty rows, skipping `scroll` rows above the
    /// live tail. Renders only the entries the window touches. Returns the rows
    /// and the scroll actually applied, clamped to the history available.
    pub fn rich_tail_scrolled(
        &mut self,
        width: u16,
        maximum_lines: usize,
        scroll: usize,
    ) -> (Vec<Line<'static>>, usize) {
        prepare_render_cache(
            &self.entries,
            &mut self.render_cache,
            width,
            TranscriptRenderMode::Rich,
        );
        let wanted = maximum_lines.saturating_add(scroll);
        let mut collected: VecDeque<Line<'static>> = VecDeque::new();
        'entries: for index in (0..self.entries.len()).rev() {
            let lines = cached_entry_lines(&self.entries, &mut self.render_cache, index);
            for line in lines.iter().rev().filter(|line| !line_is_empty(line)) {
                if collected.len() >= wanted {
                    break 'entries;
                }
                collected.push_front(line.clone());
            }
        }
        let applied = scroll.min(collected.len().saturating_sub(maximum_lines));
        let end = collected.len() - applied;
        let start = end.saturating_sub(maximum_lines);
        (collected.drain(start..end).collect(), applied)
    }

    pub fn browser_transcript(&self, after_seq: Option<u64>) -> BrowserTranscript {
        let mut entries = self
            .entries
            .iter()
            .filter(|entry| entry.start_seq > self.last_compaction_seq)
            .map(browser_entry)
            .collect::<Vec<_>>();
        let mut remaining = BROWSER_TRANSCRIPT_LINES;
        for entry in entries.iter_mut().rev() {
            if entry.lines.len() > remaining {
                let omitted = entry
                    .lines
                    .len()
                    .saturating_sub(remaining.saturating_sub(1));
                if remaining == 0 {
                    entry.lines.clear();
                } else {
                    entry.lines.drain(..omitted);
                    entry
                        .lines
                        .insert(0, format!("[… {omitted} earlier lines omitted …]"));
                    entry.lines.truncate(remaining);
                }
            }
            remaining = remaining.saturating_sub(entry.lines.len());
        }
        entries.retain(|entry| !entry.lines.is_empty());
        if remaining == 0 {
            while entries.first().is_some_and(|entry| entry.lines.is_empty()) {
                entries.remove(0);
            }
        }
        let window_start_seq = entries.first().map_or(self.latest_seq, |entry| entry.id);
        let reset = after_seq.is_some_and(|after| after < window_start_seq);
        if let Some(after) = after_seq.filter(|_| !reset) {
            entries.retain(|entry| entry.updated_seq > after);
        }
        BrowserTranscript {
            latest_seq: self.latest_seq,
            window_start_seq,
            reset,
            entries,
        }
    }

    pub fn browser_tail(&self, maximum_lines: usize) -> Vec<String> {
        let mut lines = self
            .browser_transcript(None)
            .entries
            .into_iter()
            .flat_map(|entry| {
                entry
                    .lines
                    .into_iter()
                    .enumerate()
                    .filter_map(move |(index, line)| {
                        let line = line.trim().to_owned();
                        (!line.is_empty()).then(|| {
                            if index == 0 {
                                format!("{}: {line}", entry.label)
                            } else {
                                line
                            }
                        })
                    })
            })
            .collect::<Vec<_>>();
        let start = lines.len().saturating_sub(maximum_lines);
        lines.drain(..start);
        lines
    }
}

fn materialized_chat_entries(session: &MaterializedSession) -> Vec<ChatEntry> {
    session
        .transcript
        .iter()
        .map(|item| materialized_chat_entry(item, session.applied_event_ordinal))
        .collect()
}

/// Rebuilds the entry list, keeping the entries whose transcript item did not
/// change. An unchanged item is the same `Arc` as in the previous projection,
/// so pointer identity settles reuse without reading a single field. The
/// field comparison is the fallback for items that were rebuilt with equal
/// content, which is what a restore from the canonical log produces.
fn materialized_chat_entries_reusing(
    session: &MaterializedSession,
    previous: Vec<ChatEntry>,
) -> Vec<ChatEntry> {
    let mut previous = previous.into_iter();
    session
        .transcript
        .iter()
        .map(|item| {
            let Some(mut entry) = previous.next() else {
                return materialized_chat_entry(item, session.applied_event_ordinal);
            };
            if entry.source.is(item) {
                entry.seq = session.applied_event_ordinal.max(item.position);
                return entry;
            }
            if entry_matches_transcript_item(&entry, item) {
                entry.seq = session.applied_event_ordinal.max(item.position);
                entry.source = TranscriptSource(Some(item.clone()));
                return entry;
            }
            materialized_chat_entry(item, session.applied_event_ordinal)
        })
        .collect()
}

fn entry_matches_transcript_item(entry: &ChatEntry, item: &TranscriptItem) -> bool {
    entry.start_seq == item.position
        && entry.recorded_at_ms == Some(item.created_at_ms)
        && entry.revision == u64::try_from(item.last_changed_at_ms).unwrap_or_default()
        && matches!(
            (&entry.role, &item.body),
            (ChatRole::User, TranscriptBody::User { .. })
                | (ChatRole::Agent, TranscriptBody::Agent { .. })
                | (ChatRole::Thought, TranscriptBody::Thought { .. })
                | (ChatRole::Tool, TranscriptBody::Tool { .. })
                | (ChatRole::Plan, TranscriptBody::Plan { .. })
                | (ChatRole::System, TranscriptBody::System { .. })
        )
        && match &item.body {
            TranscriptBody::Agent { .. } | TranscriptBody::Thought { .. } => {
                entry.message_id.as_deref() == Some(item.stable_id.as_str())
            }
            TranscriptBody::Tool { .. } => {
                entry.tool_call_id.as_deref() == Some(item.stable_id.as_str())
            }
            _ => true,
        }
}

fn materialized_chat_entry(item: &Arc<TranscriptItem>, frontier: u64) -> ChatEntry {
    let mut entry = match &item.body {
        TranscriptBody::User { content } => ChatEntry::plain(
            item.position,
            ChatRole::User,
            materialized_content_text(content),
        ),
        TranscriptBody::Agent { chunks, .. } => ChatEntry::plain(
            item.position,
            ChatRole::Agent,
            materialized_chunks_text(chunks),
        ),
        TranscriptBody::Thought { chunks, .. } => ChatEntry::plain(
            item.position,
            ChatRole::Thought,
            materialized_chunks_text(chunks),
        ),
        TranscriptBody::Tool { call } => {
            let call = serde_json::from_value::<ToolCall>(call.clone()).ok();
            let mut entry = ChatEntry::tool(
                item.position,
                call.as_ref()
                    .map_or("[invalid tool call]", |call| call.title.as_str()),
                Some(item.stable_id.clone()),
                call.as_ref()
                    .map_or(ToolStatus::Pending, |call| tool_status(&call.status)),
            );
            if let Some(call) = call {
                entry.tool_content = tool_content_details(&call.content);
                entry.tool_diffstats = tool_diffstats(&call.content);
                entry.tool_locations = tool_location_details(&call.locations);
            }
            entry
        }
        TranscriptBody::Plan { plan } => ChatEntry::plan(
            item.position,
            serde_json::from_value::<Plan>(plan.clone())
                .map(|plan| plan.entries)
                .unwrap_or_default()
                .into_iter()
                .map(|line| PlanLine {
                    text: sanitize_terminal_text(&line.content),
                    status: plan_status(&line.status),
                })
                .collect(),
        ),
        TranscriptBody::System { text } => ChatEntry::plain(item.position, ChatRole::System, text),
    };
    entry.seq = frontier.max(item.position);
    entry.recorded_at_ms = Some(item.created_at_ms);
    entry.revision = u64::try_from(item.last_changed_at_ms).unwrap_or_default();
    if matches!(
        &item.body,
        TranscriptBody::Agent { .. } | TranscriptBody::Thought { .. }
    ) {
        entry.message_id = Some(item.stable_id.clone());
    }
    entry.source = TranscriptSource(Some(item.clone()));
    entry
}

pub fn materialized_content_text(content: &[serde_json::Value]) -> String {
    content
        .iter()
        .map(materialized_value_text)
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn materialized_chunks_text(chunks: &[serde_json::Value]) -> String {
    chunks
        .iter()
        .filter_map(|value| serde_json::from_value::<ContentChunk>(value.clone()).ok())
        .filter_map(|chunk| content_block_text(&chunk.content))
        .map(|text| sanitize_terminal_text(&text))
        .collect::<Vec<_>>()
        .join("")
}

fn materialized_value_text(value: &serde_json::Value) -> String {
    if let Ok(block) = serde_json::from_value::<ContentBlock>(value.clone())
        && let Some(text) = content_block_text(&block)
    {
        return sanitize_terminal_text(&text);
    }
    if let Some(text) = value.as_str() {
        return sanitize_terminal_text(text);
    }
    sanitize_terminal_text(&serde_json::to_string(value).unwrap_or_else(|_| "[content]".into()))
}

fn browser_entry(entry: &ChatEntry) -> BrowserTranscriptEntry {
    let (role, label) = match entry.role {
        ChatRole::User => ("user", "You".to_owned()),
        ChatRole::Agent => ("agent", "Agent".to_owned()),
        ChatRole::Thought => ("thought", "Thinking".to_owned()),
        ChatRole::Tool => (
            "tool",
            format!(
                "Tool · {}",
                tool_status_name(entry.tool_status.unwrap_or(ToolStatus::Pending))
            ),
        ),
        ChatRole::Plan => ("plan", "Plan".to_owned()),
        ChatRole::System => ("system", "Hel".to_owned()),
    };
    let source = if entry.role == ChatRole::Plan {
        entry
            .plan
            .iter()
            .map(|line| {
                let marker = match line.status {
                    PlanStatus::Pending => "○",
                    PlanStatus::Running => "●",
                    PlanStatus::Completed => "✓",
                };
                format!("{marker} {}", line.text)
            })
            .collect::<Vec<_>>()
    } else if entry.role == ChatRole::Tool {
        std::iter::once(entry.text.clone())
            .chain(entry.tool_content.clone())
            .chain(entry.tool_locations.clone())
            .collect()
    } else {
        entry.text.lines().map(str::to_owned).collect()
    };
    BrowserTranscriptEntry {
        id: entry.start_seq,
        updated_seq: entry.seq,
        role,
        label,
        recorded_at_ms: entry.recorded_at_ms,
        lines: source
            .into_iter()
            .map(|line| truncate_browser_line(&line))
            .collect(),
    }
}

fn truncate_browser_line(line: &str) -> String {
    if line.len() <= BROWSER_LINE_BYTES {
        return line.to_owned();
    }
    const SUFFIX: &str = "… [truncated]";
    let mut end = BROWSER_LINE_BYTES - SUFFIX.len();
    while !line.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{SUFFIX}", &line[..end])
}

const fn tool_status_name(status: ToolStatus) -> &'static str {
    match status {
        ToolStatus::Pending => "waiting",
        ToolStatus::Running => "running",
        ToolStatus::Completed => "done",
        ToolStatus::Failed => "failed",
    }
}

/// Where the transcript viewport is pinned. Anchoring to an entry rather than an
/// absolute row keeps the view stable while the agent appends new rows below,
/// and lets the renderer touch only the entries the viewport covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TranscriptAnchor {
    /// Follow the newest rows.
    Bottom,
    /// The top visible row is `row` rows into `entry`.
    Row { entry: usize, row: usize },
}

/// Drop cached rows that a width or mode change invalidated, and size the cache
/// to the current entry count.
fn prepare_render_cache(
    entries: &[ChatEntry],
    cache: &mut TranscriptRenderCache,
    width: u16,
    mode: TranscriptRenderMode,
) {
    if cache.width != width || cache.mode != mode {
        cache.width = width;
        cache.mode = mode;
        cache.entries.clear();
    }
    cache.entries.resize(entries.len(), None);
    let collapse = tool_collapse_states(entries, mode);
    for (index, state) in collapse.iter().enumerate() {
        if cache.collapse.get(index) != Some(state) {
            cache.entries[index] = None;
        }
    }
    cache.collapse = collapse;
}

fn is_completed_tool(entry: &ChatEntry) -> bool {
    entry.role == ChatRole::Tool && entry.tool_status == Some(ToolStatus::Completed)
}

/// The newest completed tool keeps its full detail until the next request
/// starts. Scanning backwards, the first `User` entry ends protection and the
/// first completed tool claims it; every other entry is transparent.
fn protected_tool_index(entries: &[ChatEntry]) -> Option<usize> {
    entries
        .iter()
        .rposition(|entry| entry.role == ChatRole::User || is_completed_tool(entry))
        .filter(|&index| is_completed_tool(&entries[index]))
}

/// Collapse state per entry. In rich mode a maximal run of two or more
/// consecutive completed tools renders as one summary cell; every other entry,
/// including a pending, running, or failed tool, breaks the run. The protected
/// newest result breaks runs too and never joins one. Raw mode never collapses
/// so the full commands stay inspectable.
fn tool_collapse_states(entries: &[ChatEntry], mode: TranscriptRenderMode) -> Vec<ToolCollapse> {
    let mut states = vec![ToolCollapse::None; entries.len()];
    if mode != TranscriptRenderMode::Rich {
        return states;
    }
    let protected = protected_tool_index(entries);
    let collapsible = |index: usize| is_completed_tool(&entries[index]) && Some(index) != protected;
    let mut start = 0;
    while start < entries.len() {
        if !collapsible(start) {
            start += 1;
            continue;
        }
        let mut end = start + 1;
        while end < entries.len() && collapsible(end) {
            end += 1;
        }
        if end - start > 1 {
            // A member's update does not bump the head's revision, so fold the
            // members' revisions into the head's state: the summary's cached
            // rows then drop whenever any member changes.
            let fingerprint = entries[start..end].iter().fold(0u64, |accumulated, entry| {
                accumulated.wrapping_mul(31).wrapping_add(entry.revision)
            });
            states[start] = ToolCollapse::Summary { end, fingerprint };
            states[start + 1..end].fill(ToolCollapse::Hidden);
        }
        start = end;
    }
    states
}

/// The single cell that stands in for a run of completed tools: the first word
/// of each member's title, in order.
fn collapsed_tool_entry(members: &[ChatEntry]) -> ChatEntry {
    let titles = members
        .iter()
        .map(|member| member.text.split_whitespace().next().unwrap_or("tool"))
        .collect::<Vec<_>>()
        .join(", ");
    ChatEntry::tool(members[0].seq, titles, None, ToolStatus::Completed)
}

/// Rendered rows for one entry, rendering and caching it on first use.
/// `prepare_render_cache` must have run for the current width and mode.
fn cached_entry_lines<'cache>(
    entries: &[ChatEntry],
    cache: &'cache mut TranscriptRenderCache,
    index: usize,
) -> &'cache [Line<'static>] {
    let entry = &entries[index];
    let stale = cache.entries[index]
        .as_ref()
        .is_none_or(|cached| cached.revision != entry.revision);
    if stale {
        let width = usize::from(cache.width);
        let lines = match cache.collapse[index] {
            ToolCollapse::None => render_transcript_entry(entry, width, cache.mode),
            ToolCollapse::Hidden => Vec::new(),
            ToolCollapse::Summary { end, .. } => {
                let summary = collapsed_tool_entry(&entries[index..end]);
                render_transcript_entry(&summary, width, cache.mode)
            }
        };
        cache.entries[index] = Some(CachedEntry {
            revision: entry.revision,
            lines,
        });
    }
    &cache.entries[index]
        .as_ref()
        .expect("the entry was just rendered")
        .lines
}

impl Default for TranscriptRenderCache {
    fn default() -> Self {
        Self {
            width: 0,
            mode: TranscriptRenderMode::Rich,
            entries: Vec::new(),
            collapse: Vec::new(),
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
    recovery_busy: bool,
    goal_prompt_active: bool,
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
    project: String,
    position: usize,
    turn_started_at_epoch_seconds: Option<u64>,
    other_sessions: Vec<OtherSessionActivity>,
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
            recovery_busy: false,
            goal_prompt_active: snapshot
                .active_prompt
                .as_ref()
                .is_some_and(|prompt| is_goal_prompt(&prompt.text)),
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
            project: String::new(),
            position: 0,
            turn_started_at_epoch_seconds: None,
            other_sessions: Vec::new(),
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
        if rebuild_projection {
            self.entries =
                materialized_chat_entries_reusing(session, std::mem::take(&mut self.entries));
            self.queued_prompts = session
                .queued_prompts
                .iter()
                .map(|prompt| QueuedPrompt {
                    id: prompt.command_id.clone(),
                    text: materialized_content_text(&prompt.content),
                })
                .collect();
        }
        self.set_config_options(config_options);
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

    /// Names this session in the header and places its line among the other
    /// sessions. Both are fixed for the visit.
    pub fn set_header_identity(&mut self, project: impl Into<String>, position: usize) {
        self.project = project.into();
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

    fn set_project_history(&mut self, entries: Vec<PromptHistoryEntry>) {
        self.project_history_error = None;
        // Entries arrive newest-first; split this session's prompts from the
        // rest of the project so history navigation reaches them first.
        let (session, project): (Vec<_>, Vec<_>) = entries
            .into_iter()
            .partition(|entry| entry.session_id == self.session_id);
        self.session_history = session.into_iter().rev().map(|entry| entry.text).collect();
        self.project_history = project.into_iter().rev().map(|entry| entry.text).collect();
        if let Some(index) = self.history_index
            && index >= self.navigation_history().len()
        {
            self.history_index = None;
            self.history_draft.clear();
        }
    }

    fn set_project_history_unavailable(&mut self, error: String) {
        self.project_history_error = Some(error);
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
        self.turn_started_at_epoch_seconds = Some(now_epoch_seconds());
    }

    /// Starts the header clock for a turn the event log just reported. An
    /// event with no recorded time falls back to now, because the turn is
    /// running either way.
    fn start_turn_clock(&mut self, recorded_at_ms: Option<i64>) {
        self.turn_started_at_epoch_seconds = recorded_at_ms
            .and_then(|recorded_at_ms| u64::try_from(recorded_at_ms).ok())
            .map(|recorded_at_ms| recorded_at_ms / 1_000)
            .or_else(|| Some(now_epoch_seconds()));
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

    pub fn transcript_snapshot(&self) -> TranscriptSnapshot {
        TranscriptSnapshot {
            entries: self.entries.clone(),
            latest_seq: self.latest_seq,
            last_compaction_seq: self.last_compaction_seq,
            render_cache: TranscriptRenderCache::default(),
        }
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
                    content: vec![serde_json::json!({
                        "type": "text",
                        "text": prompt.text,
                    })],
                    queued_at_ms: 0,
                })
                .collect(),
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

    fn insert_character(&mut self, character: char) {
        self.input.insert(self.input_cursor, character);
        self.input_cursor += character.len_utf8();
        self.history_index = None;
        self.preferred_column = None;
        self.update_autocomplete();
    }

    fn handle_paste(&mut self, pasted: &str) {
        let pasted = sanitize_terminal_text(pasted);
        if pasted.is_empty() {
            return;
        }
        if let Some(search) = self.history_search.as_mut() {
            search.query.push_str(&pasted.replace(['\r', '\n'], " "));
            self.refresh_history_search();
            return;
        }
        self.input.insert_str(self.input_cursor, &pasted);
        self.input_cursor += pasted.len();
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

    /// Kill to the end of the line. A `chained` kill appends to the kill
    /// buffer, in Emacs order, so a later yank restores the whole block.
    fn kill_to_line_end(&mut self, chained: bool) {
        let end = self.line_end();
        let range = if end == self.input_cursor && end < self.input.len() {
            end..end + 1
        } else {
            self.input_cursor..end
        };
        if range.is_empty() {
            // Nothing was killed, so leave any chained buffer intact.
            return;
        }
        let previous = if chained {
            std::mem::take(&mut self.kill_buffer)
        } else {
            String::new()
        };
        self.kill_range(range);
        if !previous.is_empty() {
            self.kill_buffer.insert_str(0, &previous);
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
            generation: self.next_history_search_generation,
            query: String::new(),
            scope: HistoryScope::Project,
            matches: Vec::new(),
            selected: None,
            unavailable: None,
        });
        self.pending_history_search = None;
    }

    fn refresh_history_search(&mut self) {
        let Some(search) = self.history_search.as_ref() else {
            return;
        };
        let generation = self.next_history_search_generation.wrapping_add(1);
        self.next_history_search_generation = generation;
        let query = search.query.clone();
        let scope = search.scope;
        if let Some(search) = self.history_search.as_mut() {
            search.generation = generation;
        }
        if query.is_empty() {
            self.pending_history_search = None;
            self.apply_history_search_results(generation, Ok(Vec::new()));
        } else {
            self.pending_history_search = Some(HistorySearchRequest {
                generation,
                session_id: self.session_id.clone(),
                bundle_id: self.bundle_id.clone(),
                scope,
                query,
                local_history: self.prompt_history.clone(),
            });
        }
    }

    fn take_history_search_request(&mut self) -> Option<HistorySearchRequest> {
        self.pending_history_search.take()
    }

    fn apply_history_search_results(
        &mut self,
        generation: u64,
        result: std::result::Result<Vec<PromptHistoryEntry>, String>,
    ) {
        let Some(search) = self.history_search.as_ref() else {
            return;
        };
        if search.generation != generation {
            return;
        }
        let original = (search.original_input.clone(), search.original_cursor);
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
                self.notices.set(format!("History unavailable: {error}"));
                self.update_autocomplete();
            }
        }
    }

    fn local_history_search_results(request: &HistorySearchRequest) -> Vec<PromptHistoryEntry> {
        request
            .local_history
            .iter()
            .rev()
            .enumerate()
            .filter(|(_, text)| text.to_lowercase().contains(&request.query.to_lowercase()))
            .map(|(index, text)| PromptHistoryEntry {
                id: -(index as i64) - 1,
                session_id: request.session_id.clone(),
                text: text.clone(),
            })
            .collect()
    }

    fn resolve_history_search_request(
        request: HistorySearchRequest,
    ) -> std::result::Result<Vec<PromptHistoryEntry>, String> {
        if let Some(bundle_id) = request.bundle_id.as_deref() {
            crate::hel_database::search_prompts(
                &request.session_id,
                bundle_id,
                request.scope,
                &request.query,
            )
            .map_err(|error| format!("{error:#}"))
        } else {
            Ok(Self::local_history_search_results(&request))
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
        self.current_model = config_current_value(options, "model");
        self.current_effort = config_current_value(options, "effort");
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

    fn navigation_history(&self) -> Vec<String> {
        let mut history = self.project_history.clone();
        history.extend(self.session_history.iter().cloned());
        history.extend(self.prompt_history.iter().cloned());
        history
    }

    fn move_history(&mut self, delta: isize) {
        let history = self.navigation_history();
        if let Some(error) = self
            .bundle_id
            .as_ref()
            .and(self.project_history_error.as_ref())
        {
            self.notices.set(format!("History unavailable: {error}"));
        }
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

    fn edit_latest_queued_prompt(&mut self) -> ChatAction {
        let Some(queued) = self.queued_prompts.pop_back() else {
            return ChatAction::None;
        };
        self.set_input(queued.text.clone());
        self.set_notice("Editing the most recently queued prompt");
        ChatAction::RemoveQueuedPrompt {
            id: queued.id,
            text: queued.text,
        }
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
        ChatAction::Prompt(prompt)
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> ChatAction {
        if key.kind != KeyEventKind::Press && key.kind != KeyEventKind::Repeat {
            return ChatAction::None;
        }
        // Any key breaks a Ctrl-K chain; only the Ctrl-K arm sets it again.
        let chained = std::mem::take(&mut self.chain_kill);
        let (code, modifiers) = normalize_key(key.code, key.modifiers);

        if code == KeyCode::Char('v')
            && modifiers.intersects(KeyModifiers::CONTROL | KeyModifiers::SUPER)
        {
            return ChatAction::PasteFromClipboard;
        }

        if modifiers.contains(KeyModifiers::ALT) && code == KeyCode::Char('v') {
            return ChatAction::ToggleVoice;
        }

        if modifiers.contains(KeyModifiers::CONTROL) && code == KeyCode::Char('g') {
            return ChatAction::Back;
        }
        if modifiers.contains(KeyModifiers::CONTROL) && code == KeyCode::Char('q') {
            return ChatAction::QuitDetach;
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
                    self.notices.set(match self.render_mode {
                        TranscriptRenderMode::Rich => "Rich transcript rendering enabled",
                        TranscriptRenderMode::Raw => "Raw transcript source enabled",
                    });
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

    pub fn handle_mouse(&mut self, mouse: MouseEvent) {
        match mouse.kind {
            MouseEventKind::ScrollUp => self.scroll_history_up(MOUSE_SCROLL_ROWS),
            MouseEventKind::ScrollDown => self.scroll_history_down(MOUSE_SCROLL_ROWS),
            _ => {}
        }
    }

    /// Rows for the current anchor, rendering only the entries the viewport
    /// covers rather than the whole transcript.
    fn viewport(&mut self, width: u16, height: usize) -> TranscriptViewport {
        prepare_render_cache(
            &self.entries,
            &mut self.render_cache,
            width,
            self.render_mode,
        );
        let top = TranscriptAnchor::Row { entry: 0, row: 0 };
        if self.entries.is_empty() {
            return TranscriptViewport {
                rows: vec![empty_transcript_row()],
                anchor: TranscriptAnchor::Bottom,
                top,
            };
        }
        if let TranscriptAnchor::Row { entry, row } = self.anchor
            && entry < self.entries.len()
        {
            let mut rows = Vec::with_capacity(height);
            let mut skip = row;
            for index in entry..self.entries.len() {
                let lines = cached_entry_lines(&self.entries, &mut self.render_cache, index);
                for line in lines.iter().skip(skip) {
                    if rows.len() == height {
                        break;
                    }
                    rows.push(line.clone());
                }
                skip = 0;
                if rows.len() == height {
                    break;
                }
            }
            // Anchors inside the final screenful cannot fill the viewport; those
            // views already reach the newest row, so follow the tail instead of
            // painting a short page.
            if rows.len() == height {
                let anchor = TranscriptAnchor::Row { entry, row };
                return TranscriptViewport {
                    rows,
                    anchor,
                    top: anchor,
                };
            }
        }
        self.tail_viewport(height)
    }

    /// The last `height` rows, walking backwards from the newest entry. These
    /// rows always reach the newest row, so the anchor to store is `Bottom`.
    fn tail_viewport(&mut self, height: usize) -> TranscriptViewport {
        let mut rows: VecDeque<Line<'static>> = VecDeque::with_capacity(height);
        let mut top = TranscriptAnchor::Row { entry: 0, row: 0 };
        if height == 0 {
            return TranscriptViewport {
                rows: rows.into(),
                anchor: TranscriptAnchor::Bottom,
                top,
            };
        }
        for index in (0..self.entries.len()).rev() {
            let lines = cached_entry_lines(&self.entries, &mut self.render_cache, index);
            let take = height.saturating_sub(rows.len());
            let start = lines.len().saturating_sub(take);
            for line in lines[start..].iter().rev() {
                rows.push_front(line.clone());
            }
            top = TranscriptAnchor::Row {
                entry: index,
                row: start,
            };
            if rows.len() >= height {
                break;
            }
        }
        TranscriptViewport {
            rows: rows.into(),
            anchor: TranscriptAnchor::Bottom,
            top,
        }
    }

    /// Rendered row count for one entry, filling the cache on demand.
    fn entry_rows(&mut self, index: usize) -> usize {
        let width = self.render_cache.width;
        prepare_render_cache(
            &self.entries,
            &mut self.render_cache,
            width,
            self.render_mode,
        );
        cached_entry_lines(&self.entries, &mut self.render_cache, index).len()
    }

    /// The anchor the current view is showing, resolving `Bottom` into the
    /// concrete entry and row at the top of the viewport. `None` before the
    /// first draw, when no width is known to wrap against.
    fn resolved_anchor(&mut self) -> Option<TranscriptAnchor> {
        if self.render_cache.width == 0 {
            return None;
        }
        Some(match self.anchor {
            TranscriptAnchor::Row { entry, row } if entry < self.entries.len() => {
                TranscriptAnchor::Row { entry, row }
            }
            _ => self.tail_viewport(self.last_viewport_height.max(1)).top,
        })
    }

    fn scroll_history_up(&mut self, rows: usize) {
        let Some(TranscriptAnchor::Row { mut entry, mut row }) = self.resolved_anchor() else {
            // Either no draw has happened yet, or the transcript is shorter than
            // the viewport and has nothing above it.
            return;
        };
        let mut remaining = rows;
        while remaining > 0 {
            if row > 0 {
                let step = remaining.min(row);
                row -= step;
                remaining -= step;
            } else if entry > 0 {
                entry -= 1;
                row = self.entry_rows(entry);
            } else {
                break;
            }
        }
        self.anchor = TranscriptAnchor::Row { entry, row };
    }

    fn scroll_history_down(&mut self, rows: usize) {
        let Some(TranscriptAnchor::Row { mut entry, mut row }) = self.resolved_anchor() else {
            return;
        };
        let mut remaining = rows;
        while remaining > 0 {
            let below = self.entry_rows(entry).saturating_sub(row + 1);
            if below >= remaining {
                row += remaining;
                break;
            }
            if entry + 1 >= self.entries.len() {
                self.anchor = TranscriptAnchor::Bottom;
                return;
            }
            remaining -= below + 1;
            entry += 1;
            row = 0;
        }
        self.anchor = TranscriptAnchor::Row { entry, row };
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
        if modifiers.intersects(KeyModifiers::CONTROL | KeyModifiers::ALT | KeyModifiers::SUPER) {
            return (KeyCode::Char(character.to_ascii_lowercase()), modifiers);
        }
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
        "model" => LocalCommand::Model,
        "effort" => LocalCommand::Effort,
        _ => return None,
    };
    Some((command, args))
}

fn is_goal_prompt(prompt: &str) -> bool {
    prompt
        .strip_prefix("/goal")
        .is_some_and(|rest| rest.is_empty() || rest.starts_with(char::is_whitespace))
}

fn find_config_option<'a>(
    options: &'a [SessionConfigOption],
    key: &str,
) -> Option<&'a SessionConfigOption> {
    match key {
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
    }
}

fn config_current_value(options: &[SessionConfigOption], key: &str) -> Option<String> {
    let option = find_config_option(options, key)?;
    let SessionConfigKind::Select(select) = &option.kind else {
        return None;
    };
    Some(select.current_value.to_string())
}

fn config_values(options: &[SessionConfigOption], key: &str) -> Vec<ConfigValueChoice> {
    let option = find_config_option(options, key);
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
        .map(|location| match location.line {
            Some(line) => format!("{}:{line}", location.path.display()),
            None => location.path.display().to_string(),
        })
        .collect()
}

const CHAT_REMOTE_QUEUE_CAPACITY: usize = 32;

#[derive(Debug)]
enum ChatRemoteOperation {
    Sync,
    Prompt {
        command_id: String,
        text: String,
        session_id: String,
        bundle_id: String,
    },
    RemoveQueuedPrompt {
        command_id: String,
        id: String,
        text: String,
    },
    SetConfig {
        command_id: String,
        key: String,
        value: String,
    },
    Cancel {
        command_id: String,
    },
}

#[derive(Debug)]
enum ChatRemoteResult {
    Sync(std::result::Result<(), String>),
    Prompt {
        text: String,
        result: std::result::Result<(u64, Option<String>), String>,
    },
    RemoveQueuedPrompt {
        id: String,
        text: String,
        result: std::result::Result<(), String>,
    },
    SetConfig {
        key: String,
        value: String,
        result: std::result::Result<(), String>,
    },
    Cancel(std::result::Result<(), String>),
    WorkerFailed(String),
}

impl ChatRemoteResult {
    fn failure_message(&self) -> Option<&str> {
        match self {
            Self::Sync(Err(error))
            | Self::Prompt {
                result: Err(error), ..
            }
            | Self::RemoveQueuedPrompt {
                result: Err(error), ..
            }
            | Self::SetConfig {
                result: Err(error), ..
            }
            | Self::Cancel(Err(error))
            | Self::WorkerFailed(error) => Some(error),
            Self::Prompt {
                result: Ok((_, Some(error))),
                ..
            } => Some(error),
            _ => None,
        }
    }
}

fn publish_chat_remote_result(
    results: &tokio::sync::mpsc::UnboundedSender<ChatRemoteResult>,
    attached: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    result: ChatRemoteResult,
) {
    if !attached.load(std::sync::atomic::Ordering::Acquire) {
        if let Some(error) = result.failure_message() {
            tracing::error!(%error, "detached chat operation failed");
        }
        return;
    }
    if let Err(error) = results.send(result)
        && let Some(error) = error.0.failure_message()
    {
        tracing::error!(%error, "chat operation failed after its UI closed");
    }
}

struct ChatRemoteSupervisor {
    operations: Option<tokio::sync::mpsc::Sender<ChatRemoteOperation>>,
    results: tokio::sync::mpsc::UnboundedReceiver<ChatRemoteResult>,
    attached: std::sync::Arc<std::sync::atomic::AtomicBool>,
    worker: Option<tokio::task::JoinHandle<()>>,
}

impl ChatRemoteSupervisor {
    fn spawn(session: ManagedSessionHandle) -> Self {
        let (operations_tx, operations_rx) = tokio::sync::mpsc::channel(CHAT_REMOTE_QUEUE_CAPACITY);
        let (results_tx, results_rx) = tokio::sync::mpsc::unbounded_channel();
        let attached = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let worker_attached = attached.clone();
        let worker = tokio::spawn(run_chat_remote_worker(
            session,
            operations_rx,
            results_tx,
            worker_attached,
        ));
        Self {
            operations: Some(operations_tx),
            results: results_rx,
            attached,
            worker: Some(worker),
        }
    }

    fn operations(&self) -> &tokio::sync::mpsc::Sender<ChatRemoteOperation> {
        self.operations
            .as_ref()
            .expect("chat remote supervisor is attached")
    }

    fn try_recv(
        &mut self,
    ) -> std::result::Result<ChatRemoteResult, tokio::sync::mpsc::error::TryRecvError> {
        self.results.try_recv()
    }

    /// Waits for the next result. `None` means the worker is gone and no
    /// further result can arrive, so the caller must stop awaiting this feed.
    /// Cancel safe: an unfinished `recv` takes no message.
    async fn recv(&mut self) -> Option<ChatRemoteResult> {
        self.results.recv().await
    }

    async fn take_finished(&mut self) -> Option<std::result::Result<(), tokio::task::JoinError>> {
        if !self
            .worker
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            return None;
        }
        Some(
            self.worker
                .take()
                .expect("finished chat worker exists")
                .await,
        )
    }
}

impl Drop for ChatRemoteSupervisor {
    fn drop(&mut self) {
        self.attached
            .store(false, std::sync::atomic::Ordering::Release);
        self.results.close();
        while let Ok(result) = self.results.try_recv() {
            if let Some(error) = result.failure_message() {
                tracing::error!(%error, "chat operation failed while detaching");
            }
        }
        drop(self.operations.take());
        let Some(worker) = self.worker.take() else {
            return;
        };
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(async move {
                    if let Err(error) = worker.await {
                        tracing::error!(%error, "detached chat background worker failed");
                    }
                });
            }
            Err(error) => {
                worker.abort();
                tracing::error!(%error, "could not supervise detached chat background worker");
            }
        }
    }
}

async fn run_chat_remote_worker(
    session: ManagedSessionHandle,
    mut operations: tokio::sync::mpsc::Receiver<ChatRemoteOperation>,
    results: tokio::sync::mpsc::UnboundedSender<ChatRemoteResult>,
    attached: std::sync::Arc<std::sync::atomic::AtomicBool>,
) {
    let mut pending = tokio::task::JoinSet::new();
    let mut accepting = true;
    loop {
        if !accepting && pending.is_empty() {
            break;
        }
        tokio::select! {
            operation = operations.recv(), if accepting => {
                let Some(operation) = operation else {
                    accepting = false;
                    continue;
                };
                enqueue_chat_remote_operation(
                    &session,
                    operation,
                    &mut pending,
                    &results,
                    &attached,
                ).await;
            }
            joined = pending.join_next(), if !pending.is_empty() => {
                if let Some(Err(error)) = joined {
                    publish_chat_remote_result(
                        &results,
                        &attached,
                        ChatRemoteResult::WorkerFailed(format!(
                            "chat background operation failed: {error}"
                        )),
                    );
                }
            }
        }
    }
}

async fn enqueue_chat_remote_operation(
    session: &ManagedSessionHandle,
    operation: ChatRemoteOperation,
    pending: &mut tokio::task::JoinSet<()>,
    results: &tokio::sync::mpsc::UnboundedSender<ChatRemoteResult>,
    attached: &std::sync::Arc<std::sync::atomic::AtomicBool>,
) {
    match operation {
        ChatRemoteOperation::Sync => match session.enqueue_sync().await {
            Ok(response) => {
                let results = results.clone();
                let attached = attached.clone();
                pending.spawn(async move {
                    let result = response.wait().await.map_err(|error| format!("{error:#}"));
                    publish_chat_remote_result(&results, &attached, ChatRemoteResult::Sync(result));
                });
            }
            Err(error) => {
                publish_chat_remote_result(
                    results,
                    attached,
                    ChatRemoteResult::Sync(Err(format!("{error:#}"))),
                );
            }
        },
        ChatRemoteOperation::Prompt {
            command_id,
            text,
            session_id,
            bundle_id,
        } => {
            let response = session
                .enqueue_submit(
                    command_id,
                    RelayCommand::Prompt {
                        prompt: vec![ContentBlock::Text(TextContent::new(text.clone()))],
                    },
                )
                .await;
            match response {
                Ok(response) => {
                    let results = results.clone();
                    let attached = attached.clone();
                    pending.spawn(async move {
                        let result = match response.wait().await {
                            Ok(ordinal) => {
                                let history_text = text.clone();
                                let history = tokio::task::spawn_blocking(move || {
                                    crate::hel_database::record_prompt(
                                        &session_id,
                                        &bundle_id,
                                        ordinal,
                                        None,
                                        &history_text,
                                    )
                                })
                                .await;
                                let warning = match history {
                                    Ok(Ok(())) => None,
                                    Ok(Err(error)) => Some(format!("{error:#}")),
                                    Err(error) => Some(format!("history task failed: {error}")),
                                };
                                Ok((ordinal, warning))
                            }
                            Err(error) => Err(format!("{error:#}")),
                        };
                        publish_chat_remote_result(
                            &results,
                            &attached,
                            ChatRemoteResult::Prompt { text, result },
                        );
                    });
                }
                Err(error) => {
                    publish_chat_remote_result(
                        results,
                        attached,
                        ChatRemoteResult::Prompt {
                            text,
                            result: Err(format!("{error:#}")),
                        },
                    );
                }
            }
        }
        ChatRemoteOperation::RemoveQueuedPrompt {
            command_id,
            id,
            text,
        } => {
            let response = session
                .enqueue_submit(
                    command_id,
                    RelayCommand::RemoveQueuedPrompt {
                        queued_command_id: id.clone(),
                    },
                )
                .await;
            match response {
                Ok(response) => {
                    let results = results.clone();
                    let attached = attached.clone();
                    pending.spawn(async move {
                        let result = response
                            .wait()
                            .await
                            .map(|_| ())
                            .map_err(|error| format!("{error:#}"));
                        publish_chat_remote_result(
                            &results,
                            &attached,
                            ChatRemoteResult::RemoveQueuedPrompt { id, text, result },
                        );
                    });
                }
                Err(error) => {
                    publish_chat_remote_result(
                        results,
                        attached,
                        ChatRemoteResult::RemoveQueuedPrompt {
                            id,
                            text,
                            result: Err(format!("{error:#}")),
                        },
                    );
                }
            }
        }
        ChatRemoteOperation::SetConfig {
            command_id,
            key,
            value,
        } => {
            let response = session
                .enqueue_submit(
                    command_id,
                    RelayCommand::SetConfig {
                        key: key.clone(),
                        value: value.clone(),
                    },
                )
                .await;
            match response {
                Ok(response) => {
                    let results = results.clone();
                    let attached = attached.clone();
                    pending.spawn(async move {
                        let result = response
                            .wait()
                            .await
                            .map(|_| ())
                            .map_err(|error| format!("{error:#}"));
                        publish_chat_remote_result(
                            &results,
                            &attached,
                            ChatRemoteResult::SetConfig { key, value, result },
                        );
                    });
                }
                Err(error) => {
                    publish_chat_remote_result(
                        results,
                        attached,
                        ChatRemoteResult::SetConfig {
                            key,
                            value,
                            result: Err(format!("{error:#}")),
                        },
                    );
                }
            }
        }
        ChatRemoteOperation::Cancel { command_id } => {
            let response = session
                .enqueue_submit(command_id, RelayCommand::Cancel)
                .await;
            match response {
                Ok(response) => {
                    let results = results.clone();
                    let attached = attached.clone();
                    pending.spawn(async move {
                        let result = response
                            .wait()
                            .await
                            .map(|_| ())
                            .map_err(|error| format!("{error:#}"));
                        publish_chat_remote_result(
                            &results,
                            &attached,
                            ChatRemoteResult::Cancel(result),
                        );
                    });
                }
                Err(error) => {
                    publish_chat_remote_result(
                        results,
                        attached,
                        ChatRemoteResult::Cancel(Err(format!("{error:#}"))),
                    );
                }
            }
        }
    }
}

fn restore_unsent_input(chat: &mut ChatState, input: &str) {
    if chat.input.is_empty() {
        chat.set_input(input.to_owned());
    } else if chat.input != input {
        chat.set_input(format!("{input}\n\n{}", chat.input));
    }
}

fn apply_chat_remote_result(chat: &mut ChatState, result: ChatRemoteResult) {
    match result {
        ChatRemoteResult::Sync(Ok(())) => chat.set_notice("Connected to session relay"),
        ChatRemoteResult::Sync(Err(error)) => chat.set_notice(format!("Connection failed: {error}")),
        ChatRemoteResult::Prompt {
            text,
            result: Ok((ordinal, None)),
        } => chat.set_notice(format!(
            "Prompt accepted by relay at {ordinal}: {}",
            queued_prompt_preview(&text)
        )),
        ChatRemoteResult::Prompt {
            text,
            result: Ok((ordinal, Some(history_error))),
        } => chat.set_notice(format!(
            "Prompt accepted by relay at {ordinal} ({}), but history was not saved: {history_error}",
            queued_prompt_preview(&text)
        )),
        ChatRemoteResult::Prompt {
            text,
            result: Err(error),
        } => {
            restore_unsent_input(chat, &text);
            chat.set_notice(format!("Prompt was not sent: {error}"));
        }
        ChatRemoteResult::RemoveQueuedPrompt {
            result: Ok(()), ..
        } => chat.set_notice("Queued prompt removed"),
        ChatRemoteResult::RemoveQueuedPrompt {
            id,
            text,
            result: Err(error),
        } => {
            if !chat.queued_prompts.iter().any(|prompt| prompt.id == id) {
                chat.queued_prompts.push_back(QueuedPrompt { id, text });
            }
            chat.set_notice(format!("Queued prompt was not removed: {error}"));
        }
        ChatRemoteResult::SetConfig {
            result: Ok(()), ..
        } => chat.set_notice("Configuration update accepted"),
        ChatRemoteResult::SetConfig {
            key,
            value,
            result: Err(error),
        } => {
            restore_unsent_input(chat, &format!("/{key} {value}"));
            chat.set_notice(format!("Configuration was not changed: {error}"));
        }
        ChatRemoteResult::Cancel(Ok(())) => chat.set_notice("Cancellation requested"),
        ChatRemoteResult::Cancel(Err(error)) => {
            chat.set_notice(format!("Cancellation failed: {error}"))
        }
        ChatRemoteResult::WorkerFailed(error) => chat.set_notice(error),
    }
}

fn queue_chat_remote_operation(
    operations: &tokio::sync::mpsc::Sender<ChatRemoteOperation>,
    operation: ChatRemoteOperation,
    chat: &mut ChatState,
) {
    if let Err(error) = operations.try_send(operation) {
        let operation = error.into_inner();
        match operation {
            ChatRemoteOperation::Prompt { text, .. } => restore_unsent_input(chat, &text),
            ChatRemoteOperation::RemoveQueuedPrompt { id, text, .. } => {
                if !chat.queued_prompts.iter().any(|prompt| prompt.id == id) {
                    chat.queued_prompts.push_back(QueuedPrompt { id, text });
                }
            }
            ChatRemoteOperation::SetConfig { key, value, .. } => {
                restore_unsent_input(chat, &format!("/{key} {value}"));
            }
            ChatRemoteOperation::Sync | ChatRemoteOperation::Cancel { .. } => {}
        }
        chat.set_notice("The session command queue is full; the command was not sent");
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
        let _ = updates.send(ChatIoUpdate::HistorySearchResults { generation, result });
    });
}

fn apply_chat_io_update(chat: &mut ChatState, update: ChatIoUpdate) {
    match update {
        ChatIoUpdate::ProjectHistoryPrefetched(Ok(entries)) => chat.set_project_history(entries),
        ChatIoUpdate::ProjectHistoryPrefetched(Err(error)) => {
            chat.set_project_history_unavailable(error);
        }
        ChatIoUpdate::HistorySearchResults { generation, result } => {
            chat.apply_history_search_results(generation, result);
        }
        ChatIoUpdate::ClipboardText(Ok(text)) => chat.handle_paste(&text),
        ChatIoUpdate::ClipboardText(Err(error)) => {
            chat.set_notice(format!("Paste failed: {error}"));
        }
        ChatIoUpdate::OtherSessions(sessions) => chat.other_sessions = sessions,
    }
}

/// The one-line notifications bar shared by every view. Cloning shares the
/// same underlying slot; the latest notice wins and a clear in one view
/// clears it for all.
#[derive(Debug, Clone, Default)]
pub struct Notices(std::sync::Arc<std::sync::Mutex<Option<String>>>);

impl Notices {
    /// Sets the notice, replacing whatever is showing. Sanitizes the text so
    /// escape sequences or stray carriage returns from background work
    /// cannot corrupt the footer row.
    pub fn set(&self, notice: impl Into<String>) {
        let sanitized = sanitize_terminal_text(&notice.into());
        *self.0.lock().expect("notices lock poisoned") = Some(sanitized);
    }

    /// Replaces the notice only if it still reads `expected`, so a
    /// background task can upgrade its own "in progress" notice to a result
    /// without clobbering whatever replaced it in the meantime. Returns
    /// whether the replacement happened.
    pub fn replace_if(&self, expected: &str, replacement: impl Into<String>) -> bool {
        let mut current = self.0.lock().expect("notices lock poisoned");
        if current.as_deref() != Some(expected) {
            return false;
        }
        *current = Some(sanitize_terminal_text(&replacement.into()));
        true
    }

    /// Clears the notice everywhere it is shown.
    pub fn clear(&self) {
        *self.0.lock().expect("notices lock poisoned") = None;
    }

    /// The current notice, if any.
    pub fn current(&self) -> Option<String> {
        self.0.lock().expect("notices lock poisoned").clone()
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

/// Marker on the current session's header line. The session list uses the same
/// glyph for the row the user has selected.
const CURRENT_SESSION_CARET: &str = "› ";

/// One session's row in the header, whichever session it belongs to.
#[derive(Debug)]
struct SessionHeaderEntry {
    position: usize,
    current: bool,
    project: String,
    turn_started_at_epoch_seconds: Option<u64>,
    last_agent_line: Option<String>,
}

/// One line per active session, the current one included, in the order the
/// session list shows them. `max_rows` caps the block: the last row then
/// counts the sessions it left out.
fn session_header_lines(
    chat: &ChatState,
    now_epoch_seconds: u64,
    width: usize,
    max_rows: usize,
) -> Vec<Line<'static>> {
    let mut entries = vec![SessionHeaderEntry {
        position: chat.position,
        current: true,
        project: chat.project.clone(),
        turn_started_at_epoch_seconds: chat.turn_started_at_epoch_seconds,
        last_agent_line: chat.last_agent_line(),
    }];
    entries.extend(
        chat.other_sessions
            .iter()
            .map(|session| SessionHeaderEntry {
                position: session.position,
                current: false,
                project: session.project.clone(),
                turn_started_at_epoch_seconds: session.turn_started_at_epoch_seconds,
                last_agent_line: session.last_agent_line.clone(),
            }),
    );
    entries.sort_by_key(|entry| entry.position);

    let max_rows = max_rows.max(1);
    // When the sessions do not fit, the last row counts the rest instead.
    let shown = if entries.len() > max_rows {
        max_rows - 1
    } else {
        entries.len()
    };
    let hidden = entries.len() - shown;
    let mut lines = entries
        .iter()
        .take(shown)
        .map(|entry| {
            let caret = if entry.current {
                CURRENT_SESSION_CARET
            } else {
                "  "
            };
            let clock = crate::usage_format::format_turn_clock(
                now_epoch_seconds,
                entry.turn_started_at_epoch_seconds,
            );
            let band = Style::default().fg(turn_band_color(
                entry.turn_started_at_epoch_seconds.is_some(),
            ));
            let last_line = entry.last_agent_line.as_deref().unwrap_or_default().trim();
            let prefix = if last_line.is_empty() {
                format!("{caret}{} {clock}", entry.project)
            } else {
                format!("{caret}{} {clock} ", entry.project)
            };
            let mut spans = vec![Span::styled(prefix, band)];
            // The tail is agent text, so style it the way the conversation does.
            spans.extend(agent_text_spans(last_line));
            truncate_line_to_width(Line::from(spans), width)
        })
        .collect::<Vec<_>>();
    if hidden > 0 {
        lines.push(Line::from(Span::styled(
            format!("  +{hidden} more"),
            Style::default().fg(Color::DarkGray),
        )));
    }
    lines
}

fn other_session_activity(
    identity: &OtherSessionIdentity,
    session: &MaterializedSession,
) -> OtherSessionActivity {
    OtherSessionActivity {
        position: identity.position,
        project: identity.project.clone(),
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

fn now_epoch_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Last line of a message that has any text on it, trimmed. `None` means the
/// message is blank.
fn last_nonempty_line(text: &str) -> Option<String> {
    text.lines()
        .map(str::trim)
        .rfind(|line| !line.is_empty())
        .map(str::to_owned)
}

/// How often the poller retries sessions with no live actor. Resolving costs a
/// manager round trip, and an archived or lost session never resolves.
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
                    // No live actor is normal for archived or lost sessions.
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
                if updates
                    .send(ChatIoUpdate::OtherSessions(summaries.clone()))
                    .is_err()
                {
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
            state.set_notice(format!("connection lost: {error:#}"));
            return false;
        }
    };
    if let Some(snapshot) = view.snapshot {
        state.apply_materialized(
            &snapshot.materialized,
            &snapshot.operational.config_options,
            &snapshot.operational.available_commands,
        );
    }
    if let Some(error) = view.error {
        state.set_notice(format!("connection lost: {error}"));
    }
    true
}

/// The user has left the chat. Reports how far they have now read and clears
/// the interaction state that should not follow them back in.
fn detach_chat(state: &mut ChatState, quit: bool) -> ChatEventOutcome {
    let last_seen_event_ordinal = state.latest_seq();
    state.reset_interaction();
    if quit {
        ChatEventOutcome::QuitDetach {
            last_seen_event_ordinal,
        }
    } else {
        ChatEventOutcome::Back {
            last_seen_event_ordinal,
        }
    }
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
    /// background feeds. Cheap enough to call from the dashboard loop: every
    /// slow step is a spawned task.
    ///
    /// `draft` is the unsent input saved when this session was last detached.
    /// Only a fresh view takes it: a warm chat the dashboard kept alive already
    /// holds newer input than the database copy.
    ///
    /// `notices` is the process-wide notifications bar; it is installed on the
    /// new state before any notice is raised below, so recovery and connection
    /// notices land in the same shared slot the dashboard reads.
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
        let mut state = view.snapshot.map_or_else(
            || {
                ChatState::from_materialized(
                    &MaterializedSession::empty(session.session_id()),
                    &[],
                    &[],
                )
            },
            |snapshot| {
                ChatState::from_materialized(
                    &snapshot.materialized,
                    &snapshot.operational.config_options,
                    &snapshot.operational.available_commands,
                )
            },
        );
        state.set_history_context(bundle_id);
        state.set_header_identity(header.project, header.position);
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
                let _ = updates.send(ChatIoUpdate::ProjectHistoryPrefetched(result));
            });
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
            state.set_notice("Connecting to session relay…");
            queue_chat_remote_operation(remote.operations(), ChatRemoteOperation::Sync, &mut state);
        }
        Self {
            state,
            session,
            bundle_id: bundle_id.to_owned(),
            recovery,
            remote,
            chat_io_tx,
            chat_io_rx,
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

    /// The composer's current text. The dashboard saves this on detach so
    /// unsent input survives a quit or a crash while the view is off screen.
    pub fn draft(&self) -> &str {
        &self.state.input
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
        apply_chat_io_update(&mut self.state, update);
        dispatch_history_search_request(&mut self.state, &self.chat_io_tx);
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
    }

    /// Applies one terminal event and reports what it asked for.
    pub fn handle_event(&mut self, event: Event) -> ChatEventOutcome {
        let action = match event {
            Event::Key(key) => self.state.handle_key(key),
            Event::Paste(pasted) => {
                self.state.handle_paste(&pasted);
                ChatAction::None
            }
            Event::Mouse(mouse) => {
                self.state.handle_mouse(mouse);
                ChatAction::None
            }
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
                self.state.set_notice("Sending prompt…");
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
            ChatAction::RemoveQueuedPrompt { id, text } => {
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
                    },
                    &mut self.state,
                );
            }
            ChatAction::SetConfig { key, value } => {
                let Some(command_id) = self.command_id("set-config") else {
                    restore_unsent_input(&mut self.state, &format!("/{key} {value}"));
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
            ChatAction::Cancel => {
                let Some(command_id) = self.command_id("cancel") else {
                    return ChatEventOutcome::Handled;
                };
                self.state.set_notice("Sending cancellation request…");
                queue_chat_remote_operation(
                    self.remote.operations(),
                    ChatRemoteOperation::Cancel { command_id },
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
                    let _ = updates.send(ChatIoUpdate::ClipboardText(result));
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
            action @ (ChatAction::Back | ChatAction::QuitDetach) => {
                self.cancel_dictation();
                return detach_chat(&mut self.state, action == ChatAction::QuitDetach);
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

fn render(frame: &mut Frame, chat: &mut ChatState) {
    let inner = frame.area();
    // The header never takes more than a third of the screen.
    let header_lines = session_header_lines(
        chat,
        now_epoch_seconds(),
        usize::from(inner.width),
        usize::from(inner.height / 3).max(1),
    );
    let header_rows = header_lines.len() as u16;
    let visible_queued = chat.queued_prompts.len().min(3) as u16;
    let prompt_width = usize::from(inner.width.saturating_sub(2)).max(1);
    let input_rows = input_visual_rows(&chat.input, prompt_width) as u16;
    let desired_prompt_height = input_rows
        .saturating_add(visible_queued)
        .saturating_add(2)
        .max(4);
    let maximum_prompt_height = inner.height.saturating_sub(6 + header_rows).max(3);
    let prompt_height = desired_prompt_height.min(maximum_prompt_height);
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(header_rows),
            Constraint::Min(5),
            Constraint::Length(prompt_height),
            Constraint::Length(1),
        ])
        .split(inner);
    let (transcript_area, prompt_area, footer_area) = (chunks[1], chunks[2], chunks[3]);
    frame.render_widget(Paragraph::new(header_lines), chunks[0]);
    render_transcript(frame, transcript_area, chat);
    let queued = chat.queued_prompts.len();
    let prompt_title = prompt_title(chat, queued);
    let prompt_block = Block::default().borders(Borders::ALL).title(prompt_title);
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
        prompt_area,
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
        match chat.phase {
            WorkerPhase::Idle => parts.push("Prompt".into()),
            WorkerPhase::Running if chat.pursuing_goal() => parts.push("Pursuing goal".into()),
            WorkerPhase::Running => parts.push("Running".into()),
            WorkerPhase::Closing => parts.push("Closing".into()),
            WorkerPhase::Closed => parts.push("Closed".into()),
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
    let cursor = cursor.min(input.len());
    let mut line_offset = 0;
    let mut row_offset = 0;

    for line in input.split('\n') {
        let line_end = line_offset + line.len();
        let wrapped = wrap_input_line(line, line_offset, width);
        if cursor <= line_end {
            let mut previous = (0, row_offset);
            for (wrapped_row, graphemes) in wrapped.iter().enumerate() {
                let row = row_offset + wrapped_row;
                let mut column = 0;
                for grapheme in graphemes {
                    let start = (column, row);
                    if cursor <= grapheme.start {
                        return start;
                    }
                    column += grapheme.width;
                    let end = if column >= width {
                        (0, row + 1)
                    } else {
                        (column, row)
                    };
                    if cursor <= grapheme.end {
                        return end;
                    }
                    previous = end;
                }
            }
            return previous;
        }
        row_offset += wrapped.len();
        line_offset = line_end + 1;
    }

    (0, row_offset)
}

#[derive(Clone, Copy)]
struct InputGrapheme {
    start: usize,
    end: usize,
    width: usize,
    whitespace: bool,
}

/// Mirror ratatui's `WordWrapper` layout so the terminal cursor follows the
/// word-wrapped `Paragraph`, including whitespace discarded at wrap points.
fn wrap_input_line(line: &str, offset: usize, width: usize) -> Vec<Vec<InputGrapheme>> {
    let mut wrapped = Vec::new();
    let mut pending_line = Vec::new();
    let mut pending_word = Vec::new();
    let mut pending_whitespace = VecDeque::<InputGrapheme>::new();
    let mut line_width = 0;
    let mut word_width = 0;
    let mut whitespace_width = 0;
    let mut previous_was_non_whitespace = false;

    for (start, symbol) in line.grapheme_indices(true) {
        let symbol_width = display_width(symbol);
        if symbol_width > width {
            continue;
        }
        let whitespace = symbol == "\u{200b}"
            || (symbol.chars().all(char::is_whitespace) && symbol != "\u{00a0}");
        let grapheme = InputGrapheme {
            start: offset + start,
            end: offset + start + symbol.len(),
            width: symbol_width,
            whitespace,
        };
        let word_found = previous_was_non_whitespace && whitespace;
        let untrimmed_overflow =
            pending_line.is_empty() && word_width + whitespace_width + symbol_width > width;

        if word_found || untrimmed_overflow {
            pending_line.extend(pending_whitespace.drain(..));
            line_width += whitespace_width;
            pending_line.append(&mut pending_word);
            line_width += word_width;
            whitespace_width = 0;
            word_width = 0;
        }

        let line_full = line_width >= width;
        let pending_word_overflow =
            symbol_width > 0 && line_width + whitespace_width + word_width >= width;
        if line_full || pending_word_overflow {
            let mut remaining_width = width.saturating_sub(line_width);
            wrapped.push(std::mem::take(&mut pending_line));
            line_width = 0;

            while let Some(pending) = pending_whitespace.front() {
                if pending.width > remaining_width {
                    break;
                }
                whitespace_width -= pending.width;
                remaining_width -= pending.width;
                pending_whitespace.pop_front();
            }
            if whitespace && pending_whitespace.is_empty() {
                previous_was_non_whitespace = false;
                continue;
            }
        }

        if grapheme.whitespace {
            whitespace_width += grapheme.width;
            pending_whitespace.push_back(grapheme);
        } else {
            word_width += grapheme.width;
            pending_word.push(grapheme);
        }
        previous_was_non_whitespace = !whitespace;
    }

    if pending_line.is_empty() && pending_word.is_empty() && !pending_whitespace.is_empty() {
        wrapped.push(Vec::new());
    }
    pending_line.extend(pending_whitespace);
    pending_line.append(&mut pending_word);
    if !pending_line.is_empty() {
        wrapped.push(pending_line);
    }
    if wrapped.is_empty() {
        wrapped.push(Vec::new());
    }
    wrapped
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

/// Width used when rendering a one-line agent tail through the markdown
/// pipeline. Wide enough that no terminal line wraps before truncation.
const HEADER_TAIL_RENDER_WIDTH: usize = 4096;

/// Style a single line of agent text the way the conversation view does.
fn agent_text_spans(text: &str) -> Vec<Span<'static>> {
    if text.is_empty() {
        return Vec::new();
    }
    let visual = entry_visual(&ChatEntry::plain(0, ChatRole::Agent, text));
    markdown_lines(
        text,
        visual.body_style,
        visual.header_style,
        HEADER_TAIL_RENDER_WIDTH,
    )
    .into_iter()
    .next()
    .map(|logical| logical.line.spans)
    .unwrap_or_default()
}

/// Truncate a styled line to `width` characters, keeping each span's style and
/// marking the cut with `…` in the style of the span it landed in.
fn truncate_line_to_width(line: Line<'static>, width: usize) -> Line<'static> {
    let total = line
        .spans
        .iter()
        .map(|span| span.content.chars().count())
        .sum::<usize>();
    if total <= width {
        return line;
    }
    let budget = width.saturating_sub(1);
    let mut spans: Vec<Span<'static>> = Vec::new();
    let mut used = 0;
    for span in line.spans {
        if used >= budget {
            if width > 0 {
                spans.push(Span::styled("…", span.style));
            }
            return Line::from(spans);
        }
        let count = span.content.chars().count();
        if used + count <= budget {
            used += count;
            spans.push(span);
        } else {
            let kept = span.content.chars().take(budget - used).collect::<String>();
            let style = span.style;
            if !kept.is_empty() {
                spans.push(Span::styled(kept, style));
            }
            if width > 0 {
                spans.push(Span::styled("…", style));
            }
            return Line::from(spans);
        }
    }
    Line::from(spans)
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
    let viewport_height = usize::from(area.height.saturating_sub(2));
    chat.last_viewport_height = viewport_height;
    let window = chat.viewport(area.width, viewport_height);
    // The window resolves and clamps the anchor: an anchor inside the last
    // screenful snaps back to following the tail.
    chat.anchor = window.anchor;
    let title = match (chat.anchor, chat.render_mode) {
        (TranscriptAnchor::Bottom, TranscriptRenderMode::Rich) => " Conversation ".to_owned(),
        (TranscriptAnchor::Bottom, TranscriptRenderMode::Raw) => {
            " Conversation · raw source ".to_owned()
        }
        (TranscriptAnchor::Row { entry, .. }, _) => format!(
            " Conversation · message {} of {} · End to follow ",
            entry.saturating_add(1),
            chat.entries.len()
        ),
    };
    let block = Block::default()
        .borders(Borders::TOP | Borders::BOTTOM)
        .title(title)
        .border_style(Style::default().fg(Color::DarkGray));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let visible = window
        .rows
        .into_iter()
        .take(usize::from(inner.height))
        .collect::<Vec<_>>();
    frame.render_widget(Paragraph::new(visible), inner);
}

const ROLE_GUTTER: &str = "│ ";
const ROLE_GUTTER_WIDTH: usize = 2;

/// The last rows of an agent message for a small preview viewport, rendered
/// by the same pipeline as the conversation view. Only viewport concerns
/// differ: no header row, blank rows dropped, at most `maximum_lines` kept.
pub fn render_agent_message_tail(
    source: &str,
    width: usize,
    maximum_lines: usize,
) -> Vec<Line<'static>> {
    if width == 0 || maximum_lines == 0 {
        return Vec::new();
    }
    let entry = ChatEntry::plain(0, ChatRole::Agent, source);
    let lines = entry_body_rows(&entry, width, TranscriptRenderMode::Rich)
        .into_iter()
        .filter(|line| !line_is_empty(line))
        .collect::<Vec<_>>();
    let start = lines.len().saturating_sub(maximum_lines);
    lines.into_iter().skip(start).collect()
}

/// Render every row of the transcript. Rendering surfaces are all incremental
/// now, so this exists only for tests that assert on the whole projection.
#[cfg(test)]
fn transcript_lines(chat: &mut ChatState, width: u16) -> Vec<Line<'static>> {
    prepare_render_cache(
        &chat.entries,
        &mut chat.render_cache,
        width,
        chat.render_mode,
    );
    let mut lines = Vec::new();
    for index in 0..chat.entries.len() {
        lines.extend_from_slice(cached_entry_lines(
            &chat.entries,
            &mut chat.render_cache,
            index,
        ));
    }
    if lines.is_empty() {
        lines.push(empty_transcript_row());
    }
    lines
}

fn empty_transcript_row() -> Line<'static> {
    Line::from(Span::styled(
        "No messages yet — send a prompt to begin.",
        Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::ITALIC),
    ))
}

/// The rows a viewport shows, with both the anchor to persist and the concrete
/// position the rows start at.
struct TranscriptViewport {
    rows: Vec<Line<'static>>,
    /// The anchor to store: `Bottom` whenever these rows reach the newest row.
    anchor: TranscriptAnchor,
    /// The entry and row the first visible row came from.
    top: TranscriptAnchor,
}

fn render_transcript_entry(
    entry: &ChatEntry,
    width: usize,
    mode: TranscriptRenderMode,
) -> Vec<Line<'static>> {
    let mut out = Vec::new();
    let visual = entry_visual(entry);
    let label = match entry.role {
        ChatRole::User | ChatRole::Agent => format_event_time(entry.recorded_at_ms).map_or_else(
            || visual.label.clone(),
            |time| format!("{} · {time}", visual.label),
        ),
        _ => visual.label.clone(),
    };
    let header = Line::from(vec![
        Span::styled(
            format!("{} ", visual.glyph),
            visual.header_style.add_modifier(Modifier::BOLD),
        ),
        Span::styled(label, visual.header_style.add_modifier(Modifier::BOLD)),
    ]);
    out.extend(wrap_styled_line(header, width, ROLE_GUTTER_WIDTH));
    out.extend(entry_body_rows(entry, width, mode));
    out.push(Line::from(""));
    out
}

/// The gutter-prefixed body rows of one entry, shared between the full
/// conversation view and the dashboard preview tail.
fn entry_body_rows(
    entry: &ChatEntry,
    width: usize,
    mode: TranscriptRenderMode,
) -> Vec<Line<'static>> {
    let visual = entry_visual(entry);
    let content_width = width.saturating_sub(ROLE_GUTTER_WIDTH).max(1);
    entry_logical_lines(entry, mode, &visual, content_width)
        .into_iter()
        .flat_map(|logical| {
            wrap_styled_line(logical.line, content_width, logical.continuation_indent)
        })
        .map(|row| with_role_gutter(row, visual.rail_style))
        .collect()
}

fn format_event_time(recorded_at_ms: Option<i64>) -> Option<String> {
    chrono::DateTime::from_timestamp_millis(recorded_at_ms?).map(|time| {
        time.with_timezone(&chrono::Local)
            .format("%H:%M")
            .to_string()
    })
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
        .cloned()
        .collect::<Vec<_>>();
    let mut source = match mode {
        TranscriptRenderMode::Raw if !details.is_empty() => {
            format!("{}\n{}", entry.text, details.join("\n"))
        }
        TranscriptRenderMode::Rich if !entry.tool_diffstats.is_empty() => {
            format!("{}\n{}", entry.text, entry.tool_diffstats.join("\n"))
        }
        _ => entry.text.clone(),
    };
    if entry.leading_omitted {
        source.insert_str(0, "[… earlier content omitted …]\n");
    }
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
            let style = Style::default().fg(Color::Yellow);
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
    line.spans
        .iter()
        .all(|span| span.content.trim().is_empty() || span.content.as_ref() == ROLE_GUTTER)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hel_worker::{ActivePrompt, WorkerSnapshot};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

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

    fn apply_pending_history_search(chat: &mut ChatState) {
        let request = chat
            .take_history_search_request()
            .expect("history search request was queued");
        let generation = request.generation;
        let result = ChatState::resolve_history_search_request(request);
        chat.apply_history_search_results(generation, result);
    }

    fn queued(id: &str, text: &str) -> QueuedPrompt {
        QueuedPrompt {
            id: id.into(),
            text: text.into(),
        }
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
    fn alt_v_toggles_voice_without_editing_prompt() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        let action = chat.handle_key(KeyEvent::new(KeyCode::Char('v'), KeyModifiers::ALT));
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
    fn detaching_leaves_the_unsent_input_where_the_dashboard_saves_it_from() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.set_input("half typed thought".into());

        // Both detaches keep the composer intact: the warm chat goes on holding
        // it, and the dashboard reads it here to write it to the session row.
        assert!(matches!(
            detach_chat(&mut chat, false),
            ChatEventOutcome::Back { .. }
        ));
        assert_eq!(chat.input, "half typed thought");

        assert!(matches!(
            detach_chat(&mut chat, true),
            ChatEventOutcome::QuitDetach { .. }
        ));
        assert_eq!(chat.input, "half typed thought");
    }

    #[test]
    fn detaching_an_empty_composer_leaves_an_empty_draft_to_save() {
        let mut chat = ChatState::new(&snapshot(), &[]);

        detach_chat(&mut chat, false);

        assert_eq!(chat.input, "");
    }

    #[test]
    fn reset_interaction_preserves_projected_transcript_and_render_cache() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.entries
            .push(ChatEntry::plain(1, ChatRole::Agent, "cached response"));
        let _ = transcript_text(&mut chat, 80);
        chat.set_input("draft".into());
        chat.prompt_history.push("previous".into());
        chat.queued_prompts.push_back(queued("queued-1", "queued"));
        chat.anchor = TranscriptAnchor::Row { entry: 0, row: 4 };
        chat.set_notice("temporary");
        chat.voice_active = true;

        chat.reset_interaction();

        assert_eq!(chat.entries.len(), 1);
        assert!(chat.render_cache.entries[0].is_some());
        assert_eq!(chat.input, "draft");
        assert_eq!(chat.input_cursor, "draft".len());
        assert!(chat.prompt_history.is_empty());
        assert!(chat.queued_prompts.is_empty());
        assert_eq!(chat.anchor, TranscriptAnchor::Bottom);
        assert!(chat.notice().is_none());
        assert!(!chat.voice_active);
    }

    fn managed_view(session: MaterializedSession) -> ManagedSessionView {
        let session_id = session.session_id.clone();
        let latest_ordinal = session.applied_event_ordinal;
        let latest_digest = session.applied_event_digest.clone();
        ManagedSessionView {
            snapshot: Some(crate::hel_session_manager::ManagedSessionSnapshot {
                materialized: session,
                latest_auth_failure_ordinal: None,
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
                    available_commands: Vec::new(),
                    config: BTreeMap::new(),
                    active_prompt: None,
                    queued_prompts: Vec::new(),
                    checkpoint_barrier: None,
                    checkpoint_ready: None,
                },
            }),
            connected: true,
            error: None,
        }
    }

    #[test]
    fn an_off_screen_chat_follows_the_session_view() {
        let mut chat = ChatState::new(&snapshot(), &[]);
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
    fn leaving_the_chat_reports_the_ordinal_the_user_has_read() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        let mut session = MaterializedSession::empty("session-read");
        session.applied_event_ordinal = 12;
        session.transcript = vec![agent_transcript_item("first", 12)];
        apply_session_view(&mut chat, Ok(managed_view(session)));
        chat.queued_prompts.push_back(queued("queued-1", "queued"));

        assert_eq!(
            detach_chat(&mut chat, false),
            ChatEventOutcome::Back {
                last_seen_event_ordinal: 12
            }
        );
        // The transcript stays warm for the next visit; the interaction state
        // that belonged to the visit does not.
        assert_eq!(chat.entries.len(), 1);
        assert!(chat.queued_prompts.is_empty());
        assert_eq!(
            detach_chat(&mut chat, true),
            ChatEventOutcome::QuitDetach {
                last_seen_event_ordinal: 12
            }
        );
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
    fn multiline_paste_is_one_draft_and_one_queued_prompt() {
        let mut running = snapshot();
        running.phase = WorkerPhase::Running;
        running.active_prompt = Some(ActivePrompt {
            request_id: "p".into(),
            text: "busy".into(),
            attachments: vec![],
        });
        let mut chat = ChatState::new(&running, &[]);

        chat.handle_paste("first\r\nsecond\rthird");

        assert_eq!(chat.input, "first\nsecond\nthird");
        assert!(chat.queued_prompts.is_empty());
        assert_eq!(
            chat.handle_key(key(KeyCode::Enter)),
            ChatAction::Prompt("first\nsecond\nthird".into())
        );
        assert!(chat.queued_prompts.is_empty());
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
    fn cloned_notices_share_one_slot() {
        let notices = Notices::default();
        let clone = notices.clone();

        notices.set("set through the original");
        assert_eq!(clone.current().as_deref(), Some("set through the original"));

        clone.clear();
        assert_eq!(notices.current(), None);
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
    fn control_c_stashes_the_typed_prompt_into_history_and_clears_the_input() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        for character in "draft prompt".chars() {
            chat.handle_key(key(KeyCode::Char(character)));
        }

        assert_eq!(chat.handle_key(ctrl('c')), ChatAction::None);
        assert!(chat.input.is_empty());
        assert_eq!(chat.input_cursor, 0);

        chat.handle_key(key(KeyCode::Up));
        assert_eq!(chat.input, "draft prompt");
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
        assert!(!rendered.contains("HEL /"));
        assert_ne!(buffer[(buffer.area.x, buffer.area.y)].symbol(), "┌");
    }

    fn other_session(
        position: usize,
        project: &str,
        turn_started_at_epoch_seconds: Option<u64>,
        last_agent_line: &str,
    ) -> OtherSessionActivity {
        OtherSessionActivity {
            position,
            project: project.to_owned(),
            turn_started_at_epoch_seconds,
            last_agent_line: (!last_agent_line.is_empty()).then(|| last_agent_line.to_owned()),
        }
    }

    fn header_chat(project: &str, position: usize, others: Vec<OtherSessionActivity>) -> ChatState {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.set_header_identity(project, position);
        chat.other_sessions = others;
        chat
    }

    fn header_text(chat: &ChatState, now_epoch_seconds: u64, width: usize) -> Vec<String> {
        session_header_lines(chat, now_epoch_seconds, width, 10)
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
        session_header_lines(chat, now_epoch_seconds, 80, 10)
            .iter()
            .map(|line| line.spans.first().and_then(|span| span.style.fg))
            .collect()
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
            [
                "  first [idle] earlier work",
                "› middle [idle]",
                "  last [idle] later work",
            ]
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
            ["› current [idle]", "  other 02:05 still going"]
        );
        assert_eq!(
            header_colors(&chat, 1_125),
            [Some(Color::LightYellow), Some(Color::Yellow)]
        );
    }

    #[test]
    fn session_header_counts_the_sessions_that_do_not_fit() {
        let chat = header_chat(
            "current",
            0,
            (1..5)
                .map(|position| other_session(position, "other", None, ""))
                .collect(),
        );

        let lines = session_header_lines(&chat, 0, 80, 3);
        let text = lines
            .iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect::<String>()
            })
            .collect::<Vec<_>>();
        assert_eq!(text, ["› current [idle]", "  other [idle]", "  +3 more"]);
        assert_eq!(
            lines[2].spans.first().and_then(|span| span.style.fg),
            Some(Color::DarkGray)
        );
    }

    #[test]
    fn session_header_styles_the_last_line_like_agent_text_not_the_band() {
        let chat = header_chat(
            "current",
            0,
            vec![other_session(1, "other", Some(1_000), "still going")],
        );

        let lines = session_header_lines(&chat, 1_125, 80, 10);
        let tail = &lines[1];
        assert_eq!(tail.spans[0].content.as_ref(), "  other 02:05 ");
        assert_eq!(tail.spans[0].style.fg, Some(Color::Yellow));
        assert_eq!(tail.spans[1].content.as_ref(), "still going");
        assert_eq!(tail.spans[1].style.fg, None);
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
            ["› current [idle]", "  other [idle] a ta…"]
        );
    }

    #[test]
    fn other_session_activity_reads_the_turn_clock_and_last_agent_line() {
        let identity = OtherSessionIdentity {
            session_id: "other".into(),
            position: 3,
            project: "api-fix".into(),
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

        assert_eq!(
            header_text(&chat, 125, 80),
            ["› current 01:05 closing line"]
        );

        session.applied_event_ordinal = 6;
        session.execution = MaterializedExecutionState::Idle;
        chat.apply_materialized(&session, &[], &[]);
        assert_eq!(
            header_text(&chat, 125, 80),
            ["› current [idle] closing line"]
        );
    }

    fn agent_transcript_item(stable_id: &str, ordinal: u64) -> Arc<TranscriptItem> {
        agent_message_item(stable_id, ordinal, "hello")
    }

    fn agent_message_item(stable_id: &str, ordinal: u64, text: &str) -> Arc<TranscriptItem> {
        Arc::new(TranscriptItem {
            stable_id: stable_id.to_owned(),
            position: ordinal,
            latest_content_event_ordinal: Some(ordinal),
            created_at_ms: 0,
            last_changed_at_ms: 0,
            body: TranscriptBody::Agent {
                chunks: vec![serde_json::json!({
                    "content": {"type": "text", "text": text}
                })],
                streaming: false,
            },
        })
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

        terminal
            .draw(|frame| render(frame, &mut chat))
            .expect("draw chat");
        assert_eq!(row(&terminal, 0).trim_end(), "› current [idle]");
        assert!(row(&terminal, 1).contains("Conversation"));

        apply_chat_io_update(
            &mut chat,
            ChatIoUpdate::OtherSessions(vec![other_session(1, "docs", None, "wrote the guide")]),
        );
        terminal
            .draw(|frame| render(frame, &mut chat))
            .expect("draw chat");
        assert_eq!(row(&terminal, 0).trim_end(), "› current [idle]");
        assert_eq!(
            row(&terminal, 1).trim_end(),
            "  docs [idle] wrote the guide"
        );
        // The transcript keeps its own chrome, below the header.
        assert!(row(&terminal, 2).contains("Conversation"));
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
    fn composer_cursor_follows_a_word_moved_to_the_next_visual_row() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.set_input("abcdefgh ijkl".into());
        let mut terminal = Terminal::new(TestBackend::new(14, 12)).expect("terminal");

        terminal
            .draw(|frame| render(frame, &mut chat))
            .expect("draw chat");
        let (word_end_x, word_row) = {
            let buffer = terminal.backend().buffer();
            let word_row = (buffer.area.y..buffer.area.bottom())
                .find(|&y| {
                    (buffer.area.x..buffer.area.right())
                        .map(|x| buffer[(x, y)].symbol())
                        .collect::<String>()
                        .contains("ijkl")
                })
                .expect("wrapped word");
            let word_start_x = (buffer.area.x..buffer.area.right())
                .find(|&x| buffer[(x, word_row)].symbol() == "i")
                .expect("wrapped word start");
            (word_start_x + 4, word_row)
        };

        terminal
            .backend_mut()
            .assert_cursor_position((word_end_x, word_row));
        assert_eq!(
            input_cursor_visual_position(&chat.input, chat.input.len(), 12),
            (4, 1)
        );
        assert_eq!(input_cursor_visual_position(&chat.input, 9, 12), (0, 1));
        assert_eq!(input_cursor_visual_position(&chat.input, 10, 12), (1, 1));
    }

    #[test]
    fn config_slash_command_without_value_shows_usage() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.input = "/model".into();

        assert_eq!(chat.handle_key(key(KeyCode::Enter)), ChatAction::None);
        assert_eq!(chat.notice().as_deref(), Some("usage: /model <value>"));
    }

    #[test]
    fn local_command_parser_requires_an_exact_command_boundary() {
        assert_eq!(parse_local_command("/checkpoint before refactor"), None);
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
    fn advertised_plan_and_goal_commands_are_forwarded_unchanged() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.apply_session_update(
            1,
            &serde_json::json!({
                "sessionUpdate": "available_commands_update",
                "availableCommands": [
                    {"name": "plan", "description": "toggle plan mode"},
                    {"name": "goal", "description": "set a persistent goal", "input": {"hint": "objective"}}
                ]
            }),
        );
        assert!(
            chat.command_choices
                .iter()
                .any(|command| command.name == "plan")
        );
        assert!(
            chat.command_choices
                .iter()
                .any(|command| command.name == "goal")
        );

        chat.input = "/plan".into();
        assert_eq!(
            chat.handle_key(key(KeyCode::Enter)),
            ChatAction::Prompt("/plan".into())
        );

        chat.input = "/goal ship the release".into();
        assert_eq!(
            chat.handle_key(key(KeyCode::Enter)),
            ChatAction::Prompt("/goal ship the release".into())
        );
    }

    #[test]
    fn command_updates_replace_stale_adapter_capabilities() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        for available_commands in [
            serde_json::json!([
                {"name": "plan", "description": "toggle plan mode"},
                {"name": "goal", "description": "set a persistent goal"}
            ]),
            serde_json::json!([
                {"name": "plan", "description": "toggle plan mode"}
            ]),
        ] {
            chat.apply_session_update(
                1,
                &serde_json::json!({
                    "sessionUpdate": "available_commands_update",
                    "availableCommands": available_commands
                }),
            );
        }

        assert!(
            chat.command_choices
                .iter()
                .any(|command| command.name == "plan")
        );
        assert!(
            !chat
                .command_choices
                .iter()
                .any(|command| command.name == "goal")
        );
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
    fn sequential_control_k_accumulates_one_yankable_block() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.set_input("line1\nline2".into());
        chat.handle_key(ctrl('a'));
        chat.handle_key(ctrl('a'));
        chat.handle_key(ctrl('k'));
        chat.handle_key(ctrl('k'));
        assert_eq!(chat.input, "line2");
        chat.handle_key(ctrl('y'));
        assert_eq!(chat.input, "line1\nline2");
    }

    #[test]
    fn any_key_between_control_k_presses_restarts_the_kill_buffer() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.set_input("line1\nline2".into());
        chat.handle_key(ctrl('a'));
        chat.handle_key(ctrl('a'));
        chat.handle_key(ctrl('k'));
        chat.handle_key(key(KeyCode::Right));
        chat.handle_key(key(KeyCode::Left));
        chat.handle_key(ctrl('k'));
        assert_eq!(chat.kill_buffer, "\n");

        chat.set_input("line1\nline2".into());
        chat.handle_key(ctrl('a'));
        chat.handle_key(ctrl('a'));
        chat.handle_key(ctrl('k'));
        chat.handle_key(key(KeyCode::Char('x')));
        chat.handle_key(ctrl('a'));
        chat.handle_key(ctrl('k'));
        assert_eq!(chat.kill_buffer, "x");
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
        apply_pending_history_search(&mut chat);
        chat.handle_key(key(KeyCode::Char('i')));
        apply_pending_history_search(&mut chat);
        chat.handle_key(key(KeyCode::Char('x')));
        apply_pending_history_search(&mut chat);
        assert_eq!(chat.input, "fix renderer");
        chat.handle_key(ctrl('r'));
        assert_eq!(chat.input, "fix parser");
        chat.handle_key(key(KeyCode::Esc));
        assert_eq!(chat.input, "unfinished");

        chat.handle_key(ctrl('r'));
        for character in "renderer".chars() {
            chat.handle_key(key(KeyCode::Char(character)));
            apply_pending_history_search(&mut chat);
        }
        chat.handle_key(key(KeyCode::Enter));
        assert_eq!(chat.input, "fix renderer");
        assert!(chat.history_search.is_none());
    }

    #[test]
    fn stale_history_search_results_are_dropped() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.prompt_history = vec!["fix parser".into(), "fix renderer".into()];
        chat.set_input("draft".into());

        chat.handle_key(ctrl('r'));
        chat.handle_key(key(KeyCode::Char('f')));
        let stale = chat
            .take_history_search_request()
            .expect("first search request");
        chat.handle_key(key(KeyCode::Char('i')));
        let current = chat
            .take_history_search_request()
            .expect("second search request");

        chat.apply_history_search_results(
            stale.generation,
            Ok(vec![PromptHistoryEntry {
                id: 1,
                session_id: "1234567890".into(),
                text: "fix parser".into(),
            }]),
        );
        assert_eq!(chat.input, "draft");

        let generation = current.generation;
        let result = ChatState::resolve_history_search_request(current);
        chat.apply_history_search_results(generation, result);
        assert_eq!(chat.input, "fix renderer");
    }

    #[test]
    fn ctrl_v_returns_paste_request_action() {
        let mut chat = ChatState::new(&snapshot(), &[]);

        assert_eq!(chat.handle_key(ctrl('v')), ChatAction::PasteFromClipboard);
        assert!(chat.input.is_empty());
    }

    #[test]
    fn move_history_uses_prefetched_project_history_and_local_prompts() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.set_history_context("bundle");
        chat.set_project_history(vec![
            PromptHistoryEntry {
                id: 2,
                session_id: "other".into(),
                text: "project newest".into(),
            },
            PromptHistoryEntry {
                id: 1,
                session_id: "other".into(),
                text: "project oldest".into(),
            },
        ]);
        chat.record_prompt_history("local prompt");

        chat.handle_key(key(KeyCode::Up));
        assert_eq!(chat.input, "local prompt");
        chat.handle_key(key(KeyCode::Up));
        assert_eq!(chat.input, "project newest");
        chat.handle_key(key(KeyCode::Up));
        assert_eq!(chat.input, "project oldest");
    }

    #[test]
    fn move_history_walks_this_session_before_the_rest_of_the_project() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.set_history_context("bundle");
        // Newest-first, with this session's prompts interleaved with another's.
        chat.set_project_history(vec![
            PromptHistoryEntry {
                id: 4,
                session_id: "other".into(),
                text: "other newest".into(),
            },
            PromptHistoryEntry {
                id: 3,
                session_id: "1234567890".into(),
                text: "mine newest".into(),
            },
            PromptHistoryEntry {
                id: 2,
                session_id: "other".into(),
                text: "other oldest".into(),
            },
            PromptHistoryEntry {
                id: 1,
                session_id: "1234567890".into(),
                text: "mine oldest".into(),
            },
        ]);
        chat.record_prompt_history("this visit");

        let mut recalled = Vec::new();
        for _ in 0..5 {
            chat.handle_key(key(KeyCode::Up));
            recalled.push(chat.input.clone());
        }
        assert_eq!(
            recalled,
            vec![
                "this visit",
                "mine newest",
                "mine oldest",
                "other newest",
                "other oldest",
            ]
        );
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
    fn user_and_agent_headers_show_first_event_time_as_local_hours_and_minutes() {
        let expected = format_event_time(Some(0)).unwrap();
        let runtime = |text| RuntimeEvent::SessionUpdate {
            update: serde_json::json!({
                "sessionUpdate": "agent_message_chunk",
                "messageId": "message-1",
                "content": {"type": "text", "text": text}
            }),
        };
        let events = vec![
            SequencedEvent {
                seq: 1,
                recorded_at_ms: Some(0),
                request_id: Some("p".into()),
                event: WorkerEvent::PromptAccepted {
                    request_id: "p".into(),
                    text: "work".into(),
                    attachments: vec![],
                },
            },
            SequencedEvent {
                seq: 2,
                recorded_at_ms: Some(0),
                request_id: None,
                event: WorkerEvent::Adapter {
                    kind: "session_update".into(),
                    payload: serde_json::to_value(runtime("do")).unwrap(),
                },
            },
            SequencedEvent {
                seq: 3,
                recorded_at_ms: Some(60_000),
                request_id: None,
                event: WorkerEvent::Adapter {
                    kind: "session_update".into(),
                    payload: serde_json::to_value(runtime("ne")).unwrap(),
                },
            },
        ];
        let mut initial = snapshot();
        initial.latest_seq = 3;
        let mut chat = ChatState::new(&initial, &events);
        let lines = transcript_text(&mut chat, 80);

        assert!(lines.contains(&format!("❯ You · {expected}")));
        assert!(lines.contains(&format!("● Agent · {expected}")));
        assert_eq!(chat.entries[1].text, "done");
        assert_eq!(chat.entries[1].recorded_at_ms, Some(0));
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

    fn completed_tool(seq: u64, title: &str) -> ChatEntry {
        ChatEntry::tool(seq, title, None, ToolStatus::Completed)
    }

    #[test]
    fn completed_tool_run_collapses_to_single_summary_cell() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.entries.push(completed_tool(1, "grep -rn alpha src"));
        chat.entries.push(completed_tool(2, "grep -rn beta src"));
        chat.entries.push(completed_tool(3, "cat notes.md"));

        let text = transcript_text(&mut chat, 80);

        assert_eq!(
            text,
            [
                "✓ Tool · done",
                "│ grep, grep",
                "",
                "✓ Tool · done",
                "│ cat notes.md",
                "",
            ]
        );
    }

    #[test]
    fn completed_tool_run_collapses_fully_once_a_new_request_starts() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.entries.push(completed_tool(1, "grep -rn alpha src"));
        chat.entries.push(completed_tool(2, "grep -rn beta src"));
        chat.entries.push(completed_tool(3, "cat notes.md"));
        chat.entries
            .push(ChatEntry::plain(4, ChatRole::User, "now ship it"));

        let text = transcript_text(&mut chat, 80);

        assert_eq!(
            text,
            [
                "✓ Tool · done",
                "│ grep, grep, cat",
                "",
                "❯ You",
                "│ now ship it",
                "",
            ]
        );
    }

    #[test]
    fn newest_completed_tool_leaves_a_lone_predecessor_expanded() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.entries.push(completed_tool(1, "grep -rn alpha src"));
        chat.entries.push(completed_tool(2, "cat notes.md"));

        let text = transcript_text(&mut chat, 80);

        assert_eq!(
            text,
            [
                "✓ Tool · done",
                "│ grep -rn alpha src",
                "",
                "✓ Tool · done",
                "│ cat notes.md",
                "",
            ]
        );
    }

    #[test]
    fn a_later_completed_tool_collapses_the_earlier_run_entirely() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.entries.push(completed_tool(1, "grep -rn alpha src"));
        chat.entries.push(completed_tool(2, "grep -rn beta src"));
        chat.entries.push(completed_tool(3, "cat notes.md"));
        chat.entries
            .push(ChatEntry::plain(4, ChatRole::Agent, "found it"));
        chat.entries.push(completed_tool(5, "rg gamma src"));

        let text = transcript_text(&mut chat, 80);

        assert_eq!(
            text,
            [
                "✓ Tool · done",
                "│ grep, grep, cat",
                "",
                "● Agent",
                "│ found it",
                "",
                "✓ Tool · done",
                "│ rg gamma src",
                "",
            ]
        );
    }

    #[test]
    fn agent_message_between_completed_tools_prevents_collapsing() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.entries.push(completed_tool(1, "grep -rn alpha src"));
        chat.entries
            .push(ChatEntry::plain(2, ChatRole::Agent, "found it"));
        chat.entries.push(completed_tool(3, "cat notes.md"));

        let text = transcript_text(&mut chat, 80);

        assert_eq!(
            text,
            [
                "✓ Tool · done",
                "│ grep -rn alpha src",
                "",
                "● Agent",
                "│ found it",
                "",
                "✓ Tool · done",
                "│ cat notes.md",
                "",
            ]
        );
    }

    #[test]
    fn failed_tool_renders_alone_and_breaks_the_collapsed_run() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.entries.push(completed_tool(1, "grep -rn alpha src"));
        chat.entries.push(completed_tool(2, "grep -rn beta src"));
        chat.entries.push(ChatEntry::tool(
            3,
            "cat missing.md",
            None,
            ToolStatus::Failed,
        ));
        chat.entries.push(completed_tool(4, "rg gamma src"));
        chat.entries.push(completed_tool(5, "rg delta src"));

        let text = transcript_text(&mut chat, 80);

        // The trailing run's last member is the newest result, so it stays
        // expanded and leaves its single predecessor alone.
        assert_eq!(
            text,
            [
                "✓ Tool · done",
                "│ grep, grep",
                "",
                "× Tool · failed",
                "│ cat missing.md",
                "",
                "✓ Tool · done",
                "│ rg gamma src",
                "",
                "✓ Tool · done",
                "│ rg delta src",
                "",
            ]
        );

        chat.entries
            .push(ChatEntry::plain(6, ChatRole::User, "now ship it"));

        assert_eq!(
            transcript_text(&mut chat, 80),
            [
                "✓ Tool · done",
                "│ grep, grep",
                "",
                "× Tool · failed",
                "│ cat missing.md",
                "",
                "✓ Tool · done",
                "│ rg, rg",
                "",
                "❯ You",
                "│ now ship it",
                "",
            ]
        );
    }

    #[test]
    fn raw_mode_renders_every_completed_tool_in_full() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.render_mode = TranscriptRenderMode::Raw;
        chat.entries.push(completed_tool(1, "grep -rn alpha src"));
        chat.entries.push(completed_tool(2, "grep -rn beta src"));
        chat.entries.push(completed_tool(3, "cat notes.md"));

        let text = transcript_text(&mut chat, 80);

        assert_eq!(
            text,
            [
                "✓ Tool · done",
                "│ grep -rn alpha src",
                "",
                "✓ Tool · done",
                "│ grep -rn beta src",
                "",
                "✓ Tool · done",
                "│ cat notes.md",
                "",
            ]
        );
    }

    #[test]
    fn completing_a_running_tool_moves_protection_and_collapses_the_earlier_run() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.entries.push(completed_tool(1, "grep -rn alpha src"));
        chat.entries.push(completed_tool(2, "grep -rn beta src"));
        chat.entries.push(ChatEntry::tool(
            3,
            "cat notes.md",
            None,
            ToolStatus::Running,
        ));

        assert_eq!(
            transcript_text(&mut chat, 80),
            [
                "✓ Tool · done",
                "│ grep -rn alpha src",
                "",
                "✓ Tool · done",
                "│ grep -rn beta src",
                "",
                "● Tool · running",
                "│ cat notes.md",
                "",
            ]
        );

        chat.entries[2].touch(4);
        chat.entries[2].tool_status = Some(ToolStatus::Completed);

        // Protection moved to the newly completed tool, so the two entries
        // above must re-render even though neither revision changed.
        assert_eq!(
            transcript_text(&mut chat, 80),
            [
                "✓ Tool · done",
                "│ grep, grep",
                "",
                "✓ Tool · done",
                "│ cat notes.md",
                "",
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
    fn agent_preview_tail_matches_the_conversation_body_rows() {
        let text = "# heading\n\nfirst paragraph with some words to wrap\n\n- alpha\n- beta";
        let entry = ChatEntry::plain(0, ChatRole::Agent, text);
        let body = render_transcript_entry(&entry, 40, TranscriptRenderMode::Rich)
            .into_iter()
            .skip(1) // header row
            .filter(|line| !line_is_empty(line))
            .collect::<Vec<_>>();
        assert!(!body.is_empty());

        assert_eq!(render_agent_message_tail(text, 40, usize::MAX), body);
        assert_eq!(
            render_agent_message_tail(text, 40, 2),
            body[body.len() - 2..].to_vec()
        );
    }

    #[test]
    fn blank_rows_inside_messages_keep_the_role_gutter() {
        for (role, color) in [
            (ChatRole::User, Color::Cyan),
            (ChatRole::Agent, Color::Yellow),
        ] {
            let entry = ChatEntry::plain(1, role, "1. first\n\n2. second");
            let lines = render_transcript_entry(&entry, 80, TranscriptRenderMode::Rich);
            let blank = lines
                .iter()
                .find(|line| {
                    line.spans
                        .iter()
                        .map(|span| span.content.as_ref())
                        .collect::<String>()
                        == ROLE_GUTTER
                })
                .expect("blank row with role gutter");

            assert_eq!(blank.spans[0].style.fg, Some(color));
            assert!(lines.last().is_some_and(line_is_empty));
            assert!(lines.last().is_some_and(|line| line.spans.is_empty()));
        }
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
        let expected = transcript_lines(&mut chat, 32)
            .into_iter()
            .filter(|line| !line_is_empty(line))
            .collect::<Vec<_>>();
        let expected = line_text(expected);
        let expected = expected[expected.len().saturating_sub(6)..].to_vec();

        let mut snapshot = chat.transcript_snapshot();
        assert_eq!(line_text(snapshot.rich_tail(32, 6)), expected);
    }

    #[test]
    fn transcript_snapshot_tail_counts_only_nonempty_rows() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.entries.push(ChatEntry::plain(
            1,
            ChatRole::Agent,
            "one\n\ntwo\n\nthree\n\nfour\n\nfive",
        ));

        let mut snapshot = chat.transcript_snapshot();
        let tail = line_text(snapshot.rich_tail(80, 4));

        assert_eq!(tail.len(), 4);
        assert!(tail.iter().all(|line| !line.trim().is_empty()));
        assert_eq!(tail, ["│ two", "│ three", "│ four", "│ five"]);
    }

    #[test]
    fn browser_transcript_is_bounded_utf8_safe_and_supports_deltas() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.entries.push(ChatEntry::plain(
            1,
            ChatRole::Agent,
            (0..1_005)
                .map(|line| format!("line {line}"))
                .collect::<Vec<_>>()
                .join("\n"),
        ));
        chat.entries.push(ChatEntry::plain(
            2,
            ChatRole::Thought,
            "🦀".repeat(BROWSER_LINE_BYTES),
        ));
        chat.latest_seq = 2;

        let full = chat.transcript_snapshot().browser_transcript(None);
        assert_eq!(
            full.entries
                .iter()
                .map(|entry| entry.lines.len())
                .sum::<usize>(),
            BROWSER_TRANSCRIPT_LINES
        );
        assert_eq!(full.entries.last().unwrap().role, "thought");
        assert!(
            full.entries[0]
                .lines
                .first()
                .is_some_and(|line| line.contains("earlier lines omitted"))
        );
        let truncated = &full.entries.last().unwrap().lines[0];
        assert!(truncated.ends_with("… [truncated]"));
        assert!(truncated.len() <= BROWSER_LINE_BYTES);
        assert!(!full.reset);

        let delta = chat.transcript_snapshot().browser_transcript(Some(1));
        assert!(!delta.reset);
        assert_eq!(delta.entries.len(), 1);
        assert_eq!(delta.entries[0].updated_seq, 2);
        assert!(chat.transcript_snapshot().browser_transcript(Some(0)).reset);
    }

    #[test]
    fn browser_transcript_excludes_entries_before_provider_compaction() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.entries
            .push(ChatEntry::plain(1, ChatRole::User, "old"));
        chat.entries
            .push(ChatEntry::plain(3, ChatRole::Agent, "current"));
        chat.latest_seq = 3;
        chat.last_compaction_seq = 2;

        let browser = chat.transcript_snapshot().browser_transcript(None);
        assert_eq!(browser.entries.len(), 1);
        assert_eq!(browser.entries[0].lines, ["current"]);
        assert_eq!(browser_tail_label(&browser.entries[0]), "Agent: current");
    }

    fn browser_tail_label(entry: &BrowserTranscriptEntry) -> String {
        format!("{}: {}", entry.label, entry.lines[0])
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

    /// A chat with `count` single-line user messages, each naming its index so
    /// scroll assertions can name the row they expect to see.
    fn numbered_chat(count: usize) -> ChatState {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.entries = (0..count)
            .map(|index| ChatEntry::plain(index as u64, ChatRole::User, format!("message {index}")))
            .collect();
        chat
    }

    /// Draw the chat and return the transcript rows currently on screen.
    fn drawn_transcript(chat: &mut ChatState, width: u16, height: u16) -> Vec<String> {
        let mut terminal = Terminal::new(TestBackend::new(width, height)).expect("terminal");
        terminal
            .draw(|frame| render(frame, chat))
            .expect("draw chat");
        let buffer = terminal.backend().buffer();
        (buffer.area.y..buffer.area.bottom())
            .map(|y| {
                (buffer.area.x..buffer.area.right())
                    .map(|x| buffer[(x, y)].symbol())
                    .collect::<String>()
                    .trim_end()
                    .to_owned()
            })
            .collect()
    }

    fn shows(rows: &[String], needle: &str) -> bool {
        rows.iter().any(|row| row.contains(needle))
    }

    /// The message bodies on screen, ignoring the title and composer chrome.
    fn visible_messages(rows: &[String]) -> Vec<String> {
        rows.iter()
            .filter(|row| row.starts_with("│ message "))
            .cloned()
            .collect()
    }

    #[test]
    fn page_navigation_keeps_end_attached_to_the_latest_message() {
        let mut chat = numbered_chat(40);
        let rows = drawn_transcript(&mut chat, 60, 24);
        assert!(shows(&rows, "message 39"), "opens on the newest message");
        assert!(!shows(&rows, "End to follow"), "the tail needs no hint");

        chat.handle_key(key(KeyCode::PageUp));
        let rows = drawn_transcript(&mut chat, 60, 24);
        assert!(!shows(&rows, "message 39"), "page up leaves the tail");
        assert!(
            shows(&rows, "End to follow"),
            "scrolled back says how to return"
        );

        chat.handle_key(key(KeyCode::PageDown));
        let rows = drawn_transcript(&mut chat, 60, 24);
        assert!(shows(&rows, "message 39"), "page down returns to the tail");

        chat.handle_key(key(KeyCode::PageUp));
        chat.handle_key(KeyEvent::new(KeyCode::End, KeyModifiers::CONTROL));
        let rows = drawn_transcript(&mut chat, 60, 24);
        assert!(
            shows(&rows, "message 39"),
            "Ctrl-End follows the tail again"
        );
        assert!(!shows(&rows, "End to follow"));
    }

    #[test]
    fn control_home_and_end_reach_both_ends_of_a_long_transcript() {
        let mut chat = numbered_chat(200);
        let _ = drawn_transcript(&mut chat, 40, 24);

        chat.handle_key(KeyEvent::new(KeyCode::Home, KeyModifiers::CONTROL));
        let rows = drawn_transcript(&mut chat, 40, 24);
        assert!(shows(&rows, "message 0"), "Ctrl-Home reaches the first row");
        assert!(!shows(&rows, "message 199"));

        chat.handle_key(KeyEvent::new(KeyCode::End, KeyModifiers::CONTROL));
        let rows = drawn_transcript(&mut chat, 40, 24);
        assert!(shows(&rows, "message 199"), "Ctrl-End reaches the last row");
    }

    #[test]
    fn mouse_wheel_scrolls_chat_history_and_resumes_following_at_bottom() {
        let mut chat = numbered_chat(40);
        let _ = drawn_transcript(&mut chat, 40, 24);

        chat.handle_mouse(mouse(MouseEventKind::ScrollUp));
        let scrolled = drawn_transcript(&mut chat, 40, 24);
        assert!(!shows(&scrolled, "message 39"), "wheel up leaves the tail");

        chat.handle_mouse(mouse(MouseEventKind::ScrollDown));
        let rows = drawn_transcript(&mut chat, 40, 24);
        assert!(shows(&rows, "message 39"), "wheel down resumes following");
        assert!(!rows.iter().any(|row| row.contains("End to follow")));
    }

    #[test]
    fn scrolled_history_stays_put_while_new_messages_stream_in() {
        let mut chat = numbered_chat(40);
        let _ = drawn_transcript(&mut chat, 40, 24);
        chat.handle_key(key(KeyCode::PageUp));
        let before = drawn_transcript(&mut chat, 40, 24);

        for index in 40..50 {
            chat.entries.push(ChatEntry::plain(
                index as u64,
                ChatRole::User,
                format!("message {index}"),
            ));
        }
        let after = drawn_transcript(&mut chat, 40, 24);

        assert_eq!(
            visible_messages(&before),
            visible_messages(&after),
            "appending messages must not move a scrolled-back viewport"
        );
        assert!(!visible_messages(&after).is_empty());
    }

    #[test]
    fn a_transcript_shorter_than_the_viewport_cannot_scroll() {
        let mut chat = numbered_chat(2);
        let rows = drawn_transcript(&mut chat, 40, 24);

        chat.handle_mouse(mouse(MouseEventKind::ScrollUp));
        chat.handle_key(key(KeyCode::PageUp));

        assert_eq!(rows, drawn_transcript(&mut chat, 40, 24));
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
    fn materialized_tool_and_plan_conversion_preserves_more_than_eight_details() {
        let tool_content = (0..12)
            .map(|index| {
                serde_json::json!({
                    "type": "content",
                    "content": {"type": "text", "text": format!("result-{index}")}
                })
            })
            .collect::<Vec<_>>();
        let locations = (0..12)
            .map(|index| {
                serde_json::json!({
                    "path": format!("src/file-{index}.rs"),
                    "line": index + 1
                })
            })
            .collect::<Vec<_>>();
        let plan = (0..12)
            .map(|index| {
                serde_json::json!({
                    "content": format!("step-{index}"),
                    "priority": "medium",
                    "status": "pending"
                })
            })
            .collect::<Vec<_>>();
        let mut session = MaterializedSession::empty("session-rich-details");
        session.applied_event_ordinal = 2;
        session.applied_event_digest = "a".repeat(64);
        session.transcript = vec![
            Arc::new(TranscriptItem {
                stable_id: "tool:inspect".into(),
                position: 1,
                latest_content_event_ordinal: None,
                created_at_ms: 1,
                last_changed_at_ms: 1,
                body: TranscriptBody::Tool {
                    call: serde_json::json!({
                        "toolCallId": "inspect",
                        "title": "inspect",
                        "status": "completed",
                        "content": tool_content,
                        "locations": locations
                    }),
                },
            }),
            Arc::new(TranscriptItem {
                stable_id: "plan:current".into(),
                position: 2,
                latest_content_event_ordinal: None,
                created_at_ms: 2,
                last_changed_at_ms: 2,
                body: TranscriptBody::Plan {
                    plan: serde_json::json!({"entries": plan}),
                },
            }),
        ];

        let entries = materialized_chat_entries(&session);
        assert_eq!(entries[0].tool_content.len(), 12);
        assert_eq!(entries[0].tool_locations.len(), 12);
        assert_eq!(entries[1].plan.len(), 12);

        let browser = TranscriptSnapshot::from_materialized(&session).browser_transcript(None);
        assert!(
            browser.entries[0]
                .lines
                .iter()
                .any(|line| line == "result-11")
        );
        assert!(
            browser.entries[0]
                .lines
                .iter()
                .any(|line| line == "src/file-11.rs:12")
        );
        assert!(
            browser.entries[1]
                .lines
                .iter()
                .any(|line| line == "○ step-11")
        );
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
            content: vec![serde_json::json!("queued prompt")],
            queued_at_ms: 20,
        });
        chat.apply_materialized(&session, &[], &[]);

        assert_eq!(chat.entries[0].text, "first");
        assert!(chat.queued_prompts.is_empty());
    }

    fn user_transcript_item(position: u64, text: &str) -> Arc<TranscriptItem> {
        Arc::new(TranscriptItem {
            stable_id: format!("user:{position}"),
            position,
            latest_content_event_ordinal: None,
            created_at_ms: position as i64 * 10,
            last_changed_at_ms: position as i64 * 10,
            body: TranscriptBody::User {
                content: vec![serde_json::json!(text)],
            },
        })
    }

    #[test]
    fn appending_a_chunk_reuses_earlier_entries_by_pointer_identity() {
        let mut session = MaterializedSession::empty("session-pointer-reuse");
        session.applied_event_ordinal = 3;
        session.transcript = vec![
            user_transcript_item(1, "first"),
            user_transcript_item(2, "second"),
            agent_transcript_item("agent:3", 3),
        ];

        let mut chat = ChatState::from_materialized(&session, &[], &[]);
        // Nothing about these entries matches their item any more, so only a
        // pointer comparison can reuse them.
        for (index, entry) in chat.entries.iter_mut().take(2).enumerate() {
            entry.text = format!("reused {index}");
            entry.revision = u64::MAX;
            entry.recorded_at_ms = None;
        }

        let tail = Arc::make_mut(&mut session.transcript[2]);
        let TranscriptBody::Agent { chunks, .. } = &mut tail.body else {
            panic!("expected an agent message");
        };
        chunks.push(serde_json::json!({
            "content": {"type": "text", "text": " again"}
        }));
        tail.last_changed_at_ms = 40;
        tail.latest_content_event_ordinal = Some(4);
        session.applied_event_ordinal = 4;
        chat.apply_materialized(&session, &[], &[]);

        assert_eq!(chat.entries.len(), 3);
        assert_eq!(chat.entries[0].text, "reused 0");
        assert_eq!(chat.entries[1].text, "reused 1");
        assert!(chat.entries[0].source.is(&session.transcript[0]));
        assert!(chat.entries[1].source.is(&session.transcript[1]));
        assert_eq!(chat.entries[2].text, "hello again");
        assert!(chat.entries[2].source.is(&session.transcript[2]));
    }

    #[test]
    fn restored_transcript_reuses_entries_through_the_field_fallback() {
        let mut session = MaterializedSession::empty("session-restored");
        session.applied_event_ordinal = 2;
        session.transcript = vec![
            user_transcript_item(1, "first"),
            user_transcript_item(2, "second"),
        ];
        let mut chat = ChatState::from_materialized(&session, &[], &[]);
        chat.entries[0].text = "reused".into();

        // A restore rebuilds every item, so nothing is pointer-identical even
        // though the content is unchanged.
        let mut restored = MaterializedSession::empty("session-restored");
        restored.applied_event_ordinal = 3;
        restored.transcript = vec![
            user_transcript_item(1, "first"),
            user_transcript_item(2, "second"),
            agent_transcript_item("agent:3", 3),
        ];
        chat.apply_materialized(&restored, &[], &[]);

        assert_eq!(
            chat.entries
                .iter()
                .map(|entry| entry.text.as_str())
                .collect::<Vec<_>>(),
            ["reused", "second", "hello"]
        );
        // Reuse re-points the entry at the item it now stands for, so the next
        // projection can take the pointer path again.
        for (entry, item) in chat.entries.iter().zip(&restored.transcript) {
            assert!(entry.source.is(item));
        }
    }

    #[test]
    fn full_remote_queue_restores_unsent_input_without_blocking() {
        let (operations, _receiver) = tokio::sync::mpsc::channel(1);
        operations.try_send(ChatRemoteOperation::Sync).unwrap();
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.set_input("new draft".into());

        queue_chat_remote_operation(
            &operations,
            ChatRemoteOperation::Prompt {
                command_id: "prompt-1".into(),
                text: "unsent prompt".into(),
                session_id: "session-1".into(),
                bundle_id: "bundle-1".into(),
            },
            &mut chat,
        );

        assert_eq!(chat.input, "unsent prompt\n\nnew draft");
        assert!(
            chat.notice()
                .as_deref()
                .is_some_and(|notice| notice.contains("queue is full"))
        );
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
