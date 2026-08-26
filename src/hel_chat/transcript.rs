//! The projected conversation: entries built from the controller's logical
//! session, the render cache and collapse rules behind them, the scrollable
//! viewport, and the rows every surface draws.

use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::Arc;

use agent_client_protocol::schema::v1::{
    ContentBlock, ContentChunk, EmbeddedResourceResource, Plan, PlanEntryStatus, ToolCall,
    ToolCallContent, ToolCallLocation, ToolCallStatus,
};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
use serde::{Deserialize, Serialize};
use similar::{ChangeTag, TextDiff};

use crate::hel_state::{MaterializedSession, TerminalOutputRecord, TranscriptBody, TranscriptItem};
use crate::hel_transcript::{
    ChatEntry, ChatRole, PlanLine, PlanStatus, ToolStatus, TranscriptSource,
};

use super::ChatState;
use super::rendering::{
    LogicalLine, TranscriptRenderMode, append_trimmed_ellipsis, markdown_lines, raw_lines,
    sanitize_terminal_text, wrap_styled_line,
};

#[derive(Debug, Clone)]
struct CachedEntry {
    revision: u64,
    lines: Vec<Line<'static>>,
}

#[derive(Debug)]
pub(super) struct TranscriptRenderCache {
    width: u16,
    mode: TranscriptRenderMode,
    entries: Vec<Option<CachedEntry>>,
    collapse: Vec<EntryCollapse>,
}

/// Whether an entry renders on its own, renders nothing, or heads a collapsed
/// streak of completed tools and thoughts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntryCollapse {
    None,
    /// Member of a collapsed run whose summary renders on the run head.
    Hidden,
    /// Detail the decluttered feed leaves out.
    Omitted,
    /// Head of a collapsed streak spanning `self..end` (end exclusive).
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
    /// stable browser identity, and its update cursor is the latest ordinal
    /// the projection records for it (see `item_update_ordinal`), so a delta
    /// carries the entries that changed rather than the whole window.
    pub fn from_materialized(session: &MaterializedSession) -> Self {
        Self::from_materialized_with_diffstats(session, &BTreeMap::new())
    }

    /// Render a materialized session with exact diffstats computed by the
    /// caller's background projection. Missing entries deliberately fall back
    /// to path-only labels; rendering never performs the expensive diff.
    pub fn from_materialized_with_diffstats(
        session: &MaterializedSession,
        diffstats: &BTreeMap<String, Vec<String>>,
    ) -> Self {
        let entries = materialized_chat_entries_with_diffstats(session, diffstats);
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
            // The remote viewer mirrors the Rich feed, so detail Rich leaves
            // out never reaches it.
            .filter(|entry| !entry.raw_only)
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

#[cfg(test)]
fn materialized_chat_entries(session: &MaterializedSession) -> Vec<ChatEntry> {
    materialized_chat_entries_with_diffstats(session, &BTreeMap::new())
}

fn materialized_chat_entries_with_diffstats(
    session: &MaterializedSession,
    diffstats: &BTreeMap<String, Vec<String>>,
) -> Vec<ChatEntry> {
    session
        .transcript
        .iter()
        .map(|item| {
            materialized_chat_entry_with_diffstats(
                item,
                session.applied_event_ordinal,
                diffstats.get(&item.stable_id),
            )
        })
        .collect()
}

/// How many transcript items a freshly opened chat converts on the caller's
/// thread. Several screens of scrollback are ready in the first frame, and the
/// rest of the history is converted off the event loop; a long session has
/// thousands of items, and converting them all inline costs seconds.
pub(super) const TAIL_SEED_ITEMS: usize = 256;

/// Entries for the transcript items in `items`, which is a prefix of a
/// session's transcript. Runs off the event loop, so it takes the items by
/// slice rather than the session.
pub(super) fn materialized_prefix_entries(
    items: &[Arc<TranscriptItem>],
    frontier: u64,
) -> Vec<ChatEntry> {
    items
        .iter()
        .map(|item| materialized_chat_entry(item, frontier))
        .collect()
}

/// Rebuilds the entry list, keeping the entries whose transcript item did not
/// change. An unchanged item is the same `Arc` as in the previous projection,
/// so pointer identity settles reuse without reading a single field. The
/// field comparison is the fallback for items that were rebuilt with equal
/// content, which is what a restore from the canonical log produces.
///
/// `skip` is the number of leading transcript items that are not converted
/// yet, so `previous` lines up with `session.transcript[skip..]`. It is zero
/// for a complete projection.
pub(super) fn materialized_chat_entries_reusing(
    session: &MaterializedSession,
    skip: usize,
    previous: Vec<ChatEntry>,
) -> Vec<ChatEntry> {
    let mut previous = previous.into_iter();
    session
        .transcript
        .iter()
        .skip(skip)
        .map(|item| {
            let Some(mut entry) = previous.next() else {
                return materialized_chat_entry(item, session.applied_event_ordinal);
            };
            if entry.source.is(item) {
                entry.seq = item_update_ordinal(item, session.applied_event_ordinal);
                return entry;
            }
            if entry_matches_transcript_item(&entry, item) {
                entry.seq = item_update_ordinal(item, session.applied_event_ordinal);
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
                | (
                    ChatRole::System,
                    TranscriptBody::System { .. } | TranscriptBody::TerminalOutput { .. }
                )
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
    materialized_chat_entry_with_diffstats(item, frontier, None)
}

/// The latest relay ordinal at which this item's rendered form can have
/// changed. It is the entry's update cursor, so a remote viewer polling with
/// `after_seq` receives an entry again only when the projection says the entry
/// moved: stamping every entry with the frontier retransmits the whole window
/// on every event.
///
/// Overshooting is safe and undershooting is not, so an item whose changes the
/// projection records no ordinal for keeps the frontier. Those are the bodies
/// the projection edits in place — thoughts, tool calls, plans, and loose
/// terminal output — and they stay conservative until the projection records a
/// change ordinal for them the way it already does for agent messages.
fn item_update_ordinal(item: &TranscriptItem, frontier: u64) -> u64 {
    let latest = match &item.body {
        // Created once and never revisited, so the creating event is exact.
        TranscriptBody::User { .. } | TranscriptBody::System { .. } => item.position,
        // Every appended chunk records the ordinal that appended it. Closing
        // the stream is the only other edit and nothing rendered reads it.
        TranscriptBody::Agent { .. } => item.latest_content_event_ordinal.unwrap_or(frontier),
        TranscriptBody::Thought { .. }
        | TranscriptBody::Tool { .. }
        | TranscriptBody::Plan { .. }
        | TranscriptBody::TerminalOutput { .. } => frontier,
    };
    latest.max(item.position)
}

fn materialized_chat_entry_with_diffstats(
    item: &Arc<TranscriptItem>,
    frontier: u64,
    exact_diffstats: Option<&Vec<String>>,
) -> ChatEntry {
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
        TranscriptBody::Tool {
            call,
            terminal_outputs,
            ..
        } => {
            let call = ToolCall::deserialize(call).ok();
            let mut entry = ChatEntry::tool(
                item.position,
                call.as_ref()
                    .map_or("[invalid tool call]", |call| call.title.as_str()),
                Some(item.stable_id.clone()),
                call.as_ref()
                    .map_or(ToolStatus::Pending, |call| tool_status(&call.status)),
            );
            if let Some(call) = call {
                entry.tool_content =
                    tool_content_details(&call.content, terminal_outputs, call.raw_output.as_ref());
                entry.tool_diffstats = exact_diffstats
                    .cloned()
                    .unwrap_or_else(|| tool_diff_paths(&call.content));
                entry.tool_locations = tool_location_details(&call.locations);
            }
            entry
        }
        TranscriptBody::TerminalOutput { record } => {
            let mut entry = ChatEntry::plain(
                item.position,
                ChatRole::System,
                sanitize_terminal_text(&terminal_output_detail(record)),
            );
            // Output no tool call refers to is a whole block per command. A
            // command that ended cleanly says nothing the decluttered feed
            // needs, so only the raw transcript carries it; anything abnormal
            // stays visible everywhere.
            entry.raw_only = record.exited_cleanly();
            entry
        }
        TranscriptBody::Plan { plan } => ChatEntry::plan(
            item.position,
            Plan::deserialize(plan)
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
    entry.seq = item_update_ordinal(item, frontier);
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
    let text = content
        .iter()
        .map(materialized_value_text)
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    crate::hel_worker::strip_hidden_prompt_context(&text).to_owned()
}

pub fn materialized_chunks_text(chunks: &[serde_json::Value]) -> String {
    chunks
        .iter()
        .filter_map(|value| ContentChunk::deserialize(value).ok())
        .filter_map(|chunk| content_block_text(&chunk.content))
        .map(|text| sanitize_terminal_text(&text))
        .collect::<Vec<_>>()
        .join("")
}

fn materialized_value_text(value: &serde_json::Value) -> String {
    if let Ok(block) = ContentBlock::deserialize(value)
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
        // The remote viewer mirrors the TUI's Rich feed: the tool title plus
        // any diffstat, not the full Raw detail.
        std::iter::once(entry.text.clone())
            .chain(entry.tool_diffstats.clone())
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
pub(super) enum TranscriptAnchor {
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
    let collapse = entry_collapse_states(entries, mode);
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

/// The newest completed tool keeps its full detail until a later user request,
/// thought, or tool appears. Agent, plan, and system entries stay transparent
/// so the existing protection behavior across those entries does not change.
fn protected_tool_index(entries: &[ChatEntry]) -> Option<usize> {
    entries
        .iter()
        .rposition(|entry| {
            matches!(
                entry.role,
                ChatRole::User | ChatRole::Thought | ChatRole::Tool
            )
        })
        .filter(|&index| is_completed_tool(&entries[index]))
}

/// What every entry renders as, which is the one place the decluttered feed is
/// decided. In rich mode a maximal streak of completed tools and thoughts
/// renders its newest thought followed by its tools. Earlier thoughts are
/// hidden, and two or more tools use the existing first-word summary. Every
/// other entry, including a pending, running, or failed tool, breaks the
/// streak. The protected newest result breaks streaks too and never joins one.
/// A `raw_only` entry renders nothing at all and is transparent to a streak
/// rather than breaking it, since nothing of it is on screen to separate the
/// surrounding entries. Raw mode neither collapses nor omits, so the complete
/// source stays inspectable.
fn entry_collapse_states(entries: &[ChatEntry], mode: TranscriptRenderMode) -> Vec<EntryCollapse> {
    let mut states = vec![EntryCollapse::None; entries.len()];
    if mode != TranscriptRenderMode::Rich {
        return states;
    }
    for (index, entry) in entries.iter().enumerate() {
        if entry.raw_only {
            states[index] = EntryCollapse::Omitted;
        }
    }
    let protected = protected_tool_index(entries);
    let streak_member = |index: usize| {
        entries[index].role == ChatRole::Thought
            || (is_completed_tool(&entries[index]) && Some(index) != protected)
    };
    let mut start = 0;
    while start < entries.len() {
        if !streak_member(start) {
            start += 1;
            continue;
        }
        // `end` stops at the last visible member, so an omitted entry the
        // streak reached across is only inside it when another member follows.
        let mut end = start + 1;
        let mut cursor = start + 1;
        while cursor < entries.len() {
            if streak_member(cursor) {
                cursor += 1;
                end = cursor;
            } else if entries[cursor].raw_only {
                cursor += 1;
            } else {
                break;
            }
        }
        let members = &entries[start..end];
        let thoughts = members
            .iter()
            .filter(|entry| entry.role == ChatRole::Thought)
            .count();
        let tools = members
            .iter()
            .filter(|entry| is_completed_tool(entry))
            .count();
        let tool_precedes_thought = members
            .iter()
            .find(|entry| !entry.raw_only)
            .is_some_and(|entry| entry.role == ChatRole::Tool)
            && thoughts > 0;
        if thoughts > 1 || tools > 1 || tool_precedes_thought {
            // A member's update does not bump the head's revision, so fold the
            // streak's revisions into the head's state: its cached rows then
            // drop whenever any member changes.
            let fingerprint = members.iter().fold(0u64, |accumulated, entry| {
                accumulated.wrapping_mul(31).wrapping_add(entry.revision)
            });
            states[start] = EntryCollapse::Summary { end, fingerprint };
            // Omitted members render nothing either way, so one state covers
            // everything the head speaks for.
            states[start + 1..end].fill(EntryCollapse::Hidden);
        }
        start = end;
    }
    states
}

/// The compact label for one completed tool. Kimi describes shell calls as
/// `Running: <command>` (or `Starting background: <command>`); the lifecycle
/// verb says nothing once the call is complete, so summarize those by command.
fn collapsed_tool_label(title: &str) -> &str {
    let title = title.trim();
    let subject = title
        .strip_prefix("Running:")
        .or_else(|| title.strip_prefix("Starting background:"))
        .map(str::trim_start)
        .filter(|subject| !subject.is_empty())
        .unwrap_or(title);
    subject.split_whitespace().next().unwrap_or("tool")
}

/// The single cell that stands in for a streak of completed tools: the compact
/// label of each member's title, in order. Non-tool entries contribute none.
fn collapsed_tool_entry(members: &[ChatEntry]) -> ChatEntry {
    let tools = members
        .iter()
        .filter(|member| is_completed_tool(member))
        .collect::<Vec<_>>();
    let titles = tools
        .iter()
        .map(|member| collapsed_tool_label(&member.text))
        .collect::<Vec<_>>()
        .join(", ");
    ChatEntry::tool(tools[0].seq, titles, None, ToolStatus::Completed)
}

/// Render one collapsed streak in the requested fixed order: newest thought,
/// then the tools. A lone tool keeps its detail; only a real tool run uses CDL.
fn collapsed_streak_lines(
    members: &[ChatEntry],
    width: usize,
    mode: TranscriptRenderMode,
) -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    if let Some(thought) = members
        .iter()
        .rev()
        .find(|member| member.role == ChatRole::Thought)
    {
        lines.extend(render_transcript_entry(thought, width, mode));
    }
    let tools = members
        .iter()
        .filter(|member| is_completed_tool(member))
        .collect::<Vec<_>>();
    match tools.as_slice() {
        [] => {}
        [tool] => lines.extend(render_transcript_entry(tool, width, mode)),
        _ => {
            let summary = collapsed_tool_entry(members);
            lines.extend(render_transcript_entry(&summary, width, mode));
        }
    }
    lines
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
            EntryCollapse::None => render_transcript_entry(entry, width, cache.mode),
            EntryCollapse::Hidden | EntryCollapse::Omitted => Vec::new(),
            EntryCollapse::Summary { end, .. } => {
                collapsed_streak_lines(&entries[index..end], width, cache.mode)
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

impl TranscriptRenderCache {
    /// Drops every cached row. The cache is indexed by entry position, so any
    /// change that moves entries between positions has to clear it: a cached
    /// row whose revision happens to match the entry that moved into its slot
    /// would otherwise be served for the wrong entry. Rows are re-rendered
    /// lazily, so only the visible window pays for the refill.
    fn clear(&mut self) {
        self.entries.clear();
        self.collapse.clear();
    }
}

impl ChatState {
    pub fn transcript_snapshot(&self) -> TranscriptSnapshot {
        TranscriptSnapshot {
            entries: self.entries.clone(),
            latest_seq: self.latest_seq,
            last_compaction_seq: self.last_compaction_seq,
            render_cache: TranscriptRenderCache::default(),
        }
    }

    /// Drops the cached rows, for a change that moved entries between
    /// positions rather than editing one in place.
    pub(super) fn invalidate_render_cache(&mut self) {
        self.render_cache.clear();
    }

    /// How many leading transcript items this view has not converted yet. Zero
    /// once the conversation is complete, which is the normal case.
    pub(super) fn unconverted_prefix(&self) -> usize {
        self.unconverted_prefix
    }

    /// Puts the history built off the event loop in front of the tail this view
    /// opened with. Reports whether the prefix still fits: the transcript can
    /// be rewritten by compaction while the conversion runs, and a prefix that
    /// no longer meets the tail is refused rather than spliced into the wrong
    /// place.
    ///
    /// Fitting is an identity question, not a counting one. A rewrite that
    /// leaves the transcript at or above the pending prefix's length keeps
    /// every count valid, so the seam decides: the prefix's last entry has to
    /// be the projection item that still sits immediately in front of the
    /// tail. Anything else is history the projection has replaced.
    pub(super) fn splice_transcript_prefix(&mut self, mut prefix: Vec<ChatEntry>) -> bool {
        if self.unconverted_prefix == 0 || prefix.len() != self.unconverted_prefix {
            return false;
        }
        let meets_the_tail = match (prefix.last(), self.prefix_seam.as_ref()) {
            // Pointer identity settles the common case; the field comparison
            // covers items a restore from the canonical log rebuilt with equal
            // content.
            (Some(last), Some(seam)) => {
                last.source.is(seam) || entry_matches_transcript_item(last, seam)
            }
            _ => false,
        };
        if !meets_the_tail {
            return false;
        }
        // The prefix was converted against the frontier the view opened at, so
        // its update cursors are restamped against the current one.
        for entry in &mut prefix {
            entry.seq = match &entry.source.0 {
                Some(item) => item_update_ordinal(item, self.latest_seq),
                None => self.latest_seq.max(entry.start_seq),
            };
        }
        let shift = prefix.len();
        let tail = std::mem::replace(&mut self.entries, prefix);
        self.entries.extend(tail);
        self.unconverted_prefix = 0;
        self.prefix_seam = None;
        if let TranscriptAnchor::Row { entry, row } = self.anchor {
            self.anchor = TranscriptAnchor::Row {
                entry: entry.saturating_add(shift),
                row,
            };
        }
        self.invalidate_render_cache();
        true
    }

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
        // An empty transcript has no rows to anchor to, and its render cache
        // has no entry to read.
        if self.render_cache.width == 0 || self.entries.is_empty() {
            return None;
        }
        Some(match self.anchor {
            TranscriptAnchor::Row { entry, row } if entry < self.entries.len() => {
                TranscriptAnchor::Row { entry, row }
            }
            _ => self.tail_viewport(self.last_viewport_height.max(1)).top,
        })
    }

    pub(super) fn scroll_history_up(&mut self, rows: usize) {
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

    pub(super) fn scroll_history_down(&mut self, rows: usize) {
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
}

pub(super) fn tool_status(status: &ToolCallStatus) -> ToolStatus {
    match status {
        ToolCallStatus::InProgress => ToolStatus::Running,
        ToolCallStatus::Completed => ToolStatus::Completed,
        ToolCallStatus::Failed => ToolStatus::Failed,
        _ => ToolStatus::Pending,
    }
}

pub(super) fn plan_status(status: &PlanEntryStatus) -> PlanStatus {
    match status {
        PlanEntryStatus::InProgress => PlanStatus::Running,
        PlanEntryStatus::Completed => PlanStatus::Completed,
        _ => PlanStatus::Pending,
    }
}

pub(super) fn content_block_text(content: &ContentBlock) -> Option<String> {
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

pub(super) fn tool_content_details(
    content: &[ToolCallContent],
    terminal_outputs: &[TerminalOutputRecord],
    raw_output: Option<&serde_json::Value>,
) -> Vec<String> {
    let mut details = Vec::new();
    let mut referenced: Vec<&str> = Vec::new();
    for item in content {
        let detail = match item {
            ToolCallContent::Content(content) => content_block_text(&content.content),
            ToolCallContent::Diff(_) => None,
            // Kimi-style agents send a terminal reference and no textual copy
            // of the output, so the record hel captured is the only thing a
            // reader ever sees. Until the terminal is reaped there is none.
            ToolCallContent::Terminal(terminal) => {
                let terminal_id = terminal.terminal_id.0.as_ref();
                referenced.push(terminal_id);
                Some(
                    terminal_outputs
                        .iter()
                        .find(|record| record.terminal_id.as_str() == terminal_id)
                        .map(terminal_output_detail)
                        .or_else(|| raw_output.and_then(raw_output_terminal_detail))
                        .unwrap_or_else(|| format!("terminal {}", terminal.terminal_id)),
                )
            }
            _ => None,
        };
        if let Some(detail) = detail {
            details.push(sanitize_terminal_text(&detail));
        }
    }
    // Grok-style agents name the terminal on a mid-flight update and then
    // replace `content` wholesale without it, so the output hel captured has
    // nothing in the final call pointing at it. Show it rather than lose it.
    for record in terminal_outputs {
        if referenced.contains(&record.terminal_id.as_str()) {
            continue;
        }
        details.push(sanitize_terminal_text(&terminal_output_detail(record)));
    }
    details
}

/// The output codex reports for a terminal it ran itself. Codex names its own
/// server-side terminal, which hel never opened and has no record for, and
/// puts the text in `rawOutput`; reading it here keeps such a call from
/// rendering as a bare terminal id.
fn raw_output_terminal_detail(raw_output: &serde_json::Value) -> Option<String> {
    let output = raw_output.get("formatted_output")?.as_str()?;
    let Some(exit_code) = raw_output
        .get("exit_code")
        .and_then(serde_json::Value::as_i64)
    else {
        return Some(output.to_owned());
    };
    let summary = format!("exited {exit_code}");
    if output.is_empty() {
        return Some(summary);
    }
    Some(format!("{output}\n{summary}"))
}

/// One terminal's output followed by how it ended.
fn terminal_output_detail(record: &TerminalOutputRecord) -> String {
    let summary = terminal_exit_summary(record);
    if record.output.is_empty() {
        return summary;
    }
    format!("{}\n{summary}", record.output)
}

/// How a terminal ended, in one line.
fn terminal_exit_summary(record: &TerminalOutputRecord) -> String {
    let mut summary = match (record.exit_code, &record.signal) {
        (_, Some(signal)) => format!("killed by {signal}"),
        (Some(code), None) => format!("exited {code}"),
        (None, None) => "released before exit".to_owned(),
    };
    if record.truncated {
        summary.push_str(" · output truncated");
    }
    summary
}

pub(super) fn tool_diff_paths(content: &[ToolCallContent]) -> Vec<String> {
    content
        .iter()
        .filter_map(|item| match item {
            ToolCallContent::Diff(diff) => Some(diff.path.display().to_string()),
            _ => None,
        })
        .collect()
}

pub(super) fn compute_tool_diffstats(content: &[ToolCallContent]) -> Vec<String> {
    content
        .iter()
        .filter_map(|item| match item {
            ToolCallContent::Diff(diff) => Some(format_diffstat(diff)),
            _ => None,
        })
        .collect()
}

pub fn materialized_tool_diffstats(item: &TranscriptItem) -> Option<Vec<String>> {
    let TranscriptBody::Tool { call, .. } = &item.body else {
        return None;
    };
    let call = ToolCall::deserialize(call).ok()?;
    if !matches!(
        tool_status(&call.status),
        ToolStatus::Completed | ToolStatus::Failed
    ) {
        return None;
    }
    let diffstats = compute_tool_diffstats(&call.content);
    (!diffstats.is_empty()).then_some(diffstats)
}

#[derive(Debug, Clone)]
pub(super) struct ToolDiffstatRequest {
    pub(super) tool_call_id: String,
    pub(super) revision: u64,
    item: Arc<TranscriptItem>,
}

impl ToolDiffstatRequest {
    pub(super) fn from_item(item: &Arc<TranscriptItem>) -> Option<Self> {
        let TranscriptBody::Tool { call, .. } = &item.body else {
            return None;
        };
        let call = ToolCall::deserialize(call).ok()?;
        let terminal = matches!(
            tool_status(&call.status),
            ToolStatus::Completed | ToolStatus::Failed
        );
        let has_diff = call
            .content
            .iter()
            .any(|item| matches!(item, ToolCallContent::Diff(_)));
        (terminal && has_diff).then(|| Self {
            tool_call_id: item.stable_id.clone(),
            revision: u64::try_from(item.last_changed_at_ms).unwrap_or_default(),
            item: Arc::clone(item),
        })
    }

    pub(super) fn compute(self) -> Result<Vec<String>, String> {
        materialized_tool_diffstats(&self.item)
            .ok_or_else(|| format!("tool {} no longer has a final diff", self.tool_call_id))
    }
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

pub(super) fn tool_location_details(locations: &[ToolCallLocation]) -> Vec<String> {
    locations
        .iter()
        .map(|location| match location.line {
            Some(line) => format!("{}:{line}", location.path.display()),
            None => location.path.display().to_string(),
        })
        .collect()
}

pub(super) fn render_transcript(frame: &mut Frame, area: Rect, chat: &mut ChatState) {
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

/// First rows of an agent message for dashboard summaries. Rich formatting is
/// retained, but the final visible row announces omitted content.
pub fn render_agent_message_head(
    source: &str,
    width: usize,
    maximum_lines: usize,
) -> Vec<Line<'static>> {
    if width == 0 || maximum_lines == 0 {
        return Vec::new();
    }
    let entry = ChatEntry::plain(0, ChatRole::Agent, source);
    let mut lines = entry_body_rows(&entry, width, TranscriptRenderMode::Rich)
        .into_iter()
        .filter(|line| !line_is_empty(line))
        .collect::<Vec<_>>();
    let truncated = lines.len() > maximum_lines;
    lines.truncate(maximum_lines);
    if truncated && let Some(last) = lines.last_mut() {
        let preserved_spans = usize::from(
            last.spans
                .first()
                .is_some_and(|span| span.content == ROLE_GUTTER),
        );
        append_trimmed_ellipsis(last, preserved_spans);
    }
    lines
}

/// Render every row of the transcript. Rendering surfaces are all incremental
/// now, so this exists only for tests that assert on the whole projection.
#[cfg(test)]
pub(super) fn transcript_lines(chat: &mut ChatState, width: u16) -> Vec<Line<'static>> {
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
        .chain(&entry.tool_diffstats)
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
    use crate::hel_acp::RuntimeEvent;
    use crate::hel_chat::test_support::{
        agent_message_item, agent_transcript_item, drawn_transcript, key, line_text, mouse_in,
        queued, snapshot, transcript_text,
    };
    use crate::hel_worker::{SequencedEvent, WorkerEvent};
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseEvent, MouseEventKind};

    fn completed_tool(seq: u64, title: &str) -> ChatEntry {
        ChatEntry::tool(seq, title, None, ToolStatus::Completed)
    }

    fn thought(seq: u64, text: &str) -> ChatEntry {
        ChatEntry::plain(seq, ChatRole::Thought, text)
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

    fn mouse(kind: MouseEventKind) -> MouseEvent {
        MouseEvent {
            kind,
            column: 0,
            row: 0,
            modifiers: KeyModifiers::NONE,
        }
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

    /// Transcript items for the tail-first tests. Every item carries the same
    /// timestamps, so entries share a revision and a row cached at one position
    /// would be served at any other position the cache still believes in.
    const FIXTURE_MS: i64 = 7;

    fn fixture_item(position: u64, stable_id: String, body: TranscriptBody) -> Arc<TranscriptItem> {
        Arc::new(TranscriptItem {
            stable_id,
            position,
            // The projection requires an agent message to carry the ordinal of
            // its latest content chunk, and carries none for anything else.
            latest_content_event_ordinal: matches!(body, TranscriptBody::Agent { .. })
                .then_some(position),
            created_at_ms: FIXTURE_MS,
            last_changed_at_ms: FIXTURE_MS,
            body,
        })
    }

    fn fixture_user_item(position: u64) -> Arc<TranscriptItem> {
        fixture_item(
            position,
            format!("user:{position}"),
            TranscriptBody::User {
                content: vec![serde_json::json!(format!("question {position}"))],
            },
        )
    }

    fn fixture_agent_item(position: u64) -> Arc<TranscriptItem> {
        fixture_item(
            position,
            format!("agent:{position}"),
            TranscriptBody::Agent {
                // Multi-kilobyte, so the conversion cost is realistic.
                chunks: (0..8)
                    .map(|chunk| {
                        serde_json::json!({
                            "content": {
                                "type": "text",
                                "text": format!("answer {position}.{chunk} ").repeat(40)
                            }
                        })
                    })
                    .collect(),
                streaming: false,
            },
        )
    }

    fn fixture_thought_item(position: u64) -> Arc<TranscriptItem> {
        fixture_item(
            position,
            format!("thought:{position}"),
            TranscriptBody::Thought {
                chunks: vec![serde_json::json!({
                    "content": {"type": "text", "text": format!("thinking about {position}")}
                })],
                streaming: false,
            },
        )
    }

    fn fixture_tool_item(position: u64) -> Arc<TranscriptItem> {
        fixture_item(
            position,
            format!("tool:{position}"),
            TranscriptBody::Tool {
                call: serde_json::json!({
                    "toolCallId": format!("call-{position}"),
                    "title": format!("read file-{position}"),
                    "status": "completed",
                    "content": [{
                        "type": "content",
                        "content": {"type": "text", "text": "output ".repeat(600)}
                    }],
                    "locations": [{"path": format!("src/file-{position}.rs"), "line": 3}]
                }),
                terminal_outputs: Vec::new(),
                terminal_refs: Vec::new(),
            },
        )
    }

    fn fixture_plan_item(position: u64) -> Arc<TranscriptItem> {
        fixture_item(
            position,
            format!("plan:{position}"),
            TranscriptBody::Plan {
                plan: serde_json::json!({
                    "entries": [{
                        "content": format!("step {position}"),
                        "priority": "medium",
                        "status": "in_progress"
                    }]
                }),
            },
        )
    }

    fn fixture_system_item(position: u64) -> Arc<TranscriptItem> {
        fixture_item(
            position,
            format!("system:{position}"),
            TranscriptBody::System {
                text: format!("notice {position}"),
            },
        )
    }

    /// A conversation with the mix of bodies a real session carries, its first
    /// item at `first_position`. A compaction rewrite replaces the history in
    /// place, so it produces the same shape of transcript at fresh ordinals.
    fn materialized_session_from(first_position: u64, items: u64) -> MaterializedSession {
        let mut session = MaterializedSession::empty("session-long");
        session.transcript = (first_position..first_position + items)
            .map(|position| match position % 6 {
                0 => fixture_tool_item(position),
                1 => fixture_user_item(position),
                2 => fixture_agent_item(position),
                3 => fixture_thought_item(position),
                4 => fixture_plan_item(position),
                _ => fixture_system_item(position),
            })
            .collect();
        session.applied_event_ordinal = first_position + items;
        session
    }

    /// A conversation with the mix of bodies a real session carries.
    fn long_materialized_session(items: u64) -> MaterializedSession {
        materialized_session_from(1, items)
    }

    fn entry_texts(entries: &[ChatEntry]) -> Vec<&str> {
        entries.iter().map(|entry| entry.text.as_str()).collect()
    }

    fn converted_prefix(session: &MaterializedSession, chat: &ChatState) -> Vec<ChatEntry> {
        materialized_prefix_entries(
            &session.transcript[..chat.unconverted_prefix()],
            session.applied_event_ordinal,
        )
    }

    #[test]
    fn materialized_conversion_preserves_each_transcript_body() {
        let mut session = MaterializedSession::empty("session-bodies");
        session.applied_event_ordinal = 9;
        session.transcript = vec![
            fixture_user_item(1),
            fixture_agent_item(2),
            fixture_thought_item(3),
            fixture_tool_item(4),
            fixture_plan_item(5),
            fixture_system_item(6),
        ];

        let entries = materialized_chat_entries(&session);

        let roles = entries.iter().map(|entry| entry.role).collect::<Vec<_>>();
        assert_eq!(
            roles,
            [
                ChatRole::User,
                ChatRole::Agent,
                ChatRole::Thought,
                ChatRole::Tool,
                ChatRole::Plan,
                ChatRole::System,
            ]
        );
        assert_eq!(entries[0].text, "question 1");
        assert!(entries[1].text.starts_with("answer 2.0 "));
        assert_eq!(entries[1].text.len(), 8 * 40 * "answer 2.0 ".len());
        assert_eq!(entries[1].message_id.as_deref(), Some("agent:2"));
        assert_eq!(entries[2].text, "thinking about 3");
        assert_eq!(entries[3].text, "read file-4");
        assert_eq!(entries[3].tool_status, Some(ToolStatus::Completed));
        assert_eq!(entries[3].tool_call_id.as_deref(), Some("tool:4"));
        assert_eq!(entries[3].tool_content.len(), 1);
        assert_eq!(entries[3].tool_locations, ["src/file-4.rs:3"]);
        assert_eq!(entries[4].plan.len(), 1);
        assert_eq!(entries[4].plan[0].text, "step 5");
        assert_eq!(entries[4].plan[0].status, PlanStatus::Running);
        assert_eq!(entries[5].text, "notice 6");
        for (index, entry) in entries.iter().enumerate() {
            assert_eq!(entry.start_seq, index as u64 + 1);
            assert_eq!(entry.recorded_at_ms, Some(FIXTURE_MS));
            assert_eq!(entry.revision, FIXTURE_MS as u64);
        }
        // The bodies the projection records a change ordinal for keep their
        // own cursor; the ones it edits in place without one keep the frontier.
        assert_eq!(
            entries.iter().map(|entry| entry.seq).collect::<Vec<_>>(),
            [1, 2, 9, 9, 9, 6]
        );
    }

    #[test]
    fn opening_a_long_session_converts_only_the_tail() {
        let items = TAIL_SEED_ITEMS as u64 + 400;
        let session = long_materialized_session(items);

        let chat = ChatState::from_materialized_tail(&session, &[], &[]);

        assert_eq!(chat.entries.len(), TAIL_SEED_ITEMS);
        assert_eq!(chat.unconverted_prefix(), 400);
        let eager = materialized_chat_entries(&session);
        assert_eq!(chat.entries, eager[400..]);
    }

    #[test]
    fn opening_a_short_session_converts_the_whole_transcript() {
        let session = long_materialized_session(TAIL_SEED_ITEMS as u64);

        let chat = ChatState::from_materialized_tail(&session, &[], &[]);

        assert_eq!(chat.unconverted_prefix(), 0);
        assert_eq!(chat.entries, materialized_chat_entries(&session));
    }

    #[test]
    fn splicing_the_converted_prefix_matches_the_eager_projection() {
        let session = long_materialized_session(TAIL_SEED_ITEMS as u64 + 500);
        let mut chat = ChatState::from_materialized_tail(&session, &[], &[]);
        let prefix = converted_prefix(&session, &chat);

        assert!(chat.splice_transcript_prefix(prefix));

        assert_eq!(chat.unconverted_prefix(), 0);
        assert_eq!(chat.entries, materialized_chat_entries(&session));
    }

    #[test]
    fn an_update_while_the_prefix_is_pending_keeps_the_tail_and_still_splices() {
        let mut session = long_materialized_session(TAIL_SEED_ITEMS as u64 + 300);
        let mut chat = ChatState::from_materialized_tail(&session, &[], &[]);
        let prefix = converted_prefix(&session, &chat);
        let pending = chat.unconverted_prefix();

        let appended = session.transcript.len() as u64 + 1;
        session.transcript.push(fixture_user_item(appended));
        session.transcript.push(fixture_agent_item(appended + 1));
        session.applied_event_ordinal = appended + 2;
        chat.apply_materialized(&session, &[], &[]);

        assert_eq!(chat.unconverted_prefix(), pending);
        assert_eq!(chat.entries.len(), session.transcript.len() - pending);
        assert_eq!(
            entry_texts(&chat.entries),
            entry_texts(&materialized_chat_entries(&session)[pending..])
        );

        assert!(chat.splice_transcript_prefix(prefix));
        assert_eq!(
            entry_texts(&chat.entries),
            entry_texts(&materialized_chat_entries(&session))
        );
        assert!(
            chat.entries
                .windows(2)
                .all(|pair| pair[0].start_seq < pair[1].start_seq)
        );
    }

    #[test]
    fn splicing_the_prefix_drops_render_rows_cached_at_the_old_positions() {
        let session = long_materialized_session(TAIL_SEED_ITEMS as u64 + 120);
        let mut chat = ChatState::from_materialized_tail(&session, &[], &[]);
        let prefix = converted_prefix(&session, &chat);
        // Fill the cache while the entries still stand for the tail only.
        chat.anchor = TranscriptAnchor::Row { entry: 0, row: 0 };
        let tail_top = drawn_transcript(&mut chat, 60, 24);
        assert!(shows(&tail_top, "question 121"));

        assert!(chat.splice_transcript_prefix(prefix));
        chat.anchor = TranscriptAnchor::Row { entry: 0, row: 0 };
        let spliced_top = drawn_transcript(&mut chat, 60, 24);

        let mut eager = ChatState::from_materialized(&session, &[], &[]);
        eager.anchor = TranscriptAnchor::Row { entry: 0, row: 0 };
        assert_eq!(spliced_top, drawn_transcript(&mut eager, 60, 24));
        assert!(shows(&spliced_top, "question 1"));
        assert!(!shows(&spliced_top, "question 121"));
    }

    #[test]
    fn a_prefix_that_no_longer_meets_the_tail_is_refused() {
        let session = long_materialized_session(TAIL_SEED_ITEMS as u64 + 60);
        let mut chat = ChatState::from_materialized_tail(&session, &[], &[]);
        let pending = chat.unconverted_prefix();
        // History from a compacted transcript: the right length, but it runs
        // past the first entry the tail holds.
        let stale = materialized_prefix_entries(
            &session.transcript[session.transcript.len() - pending..],
            session.applied_event_ordinal,
        );

        assert!(!chat.splice_transcript_prefix(stale));

        assert_eq!(chat.unconverted_prefix(), pending);
        assert_eq!(chat.entries.len(), TAIL_SEED_ITEMS);
    }

    #[test]
    fn a_prefix_from_replaced_history_is_refused_when_the_rewrite_keeps_the_length() {
        let session = long_materialized_session(TAIL_SEED_ITEMS as u64 + 60);
        let mut chat = ChatState::from_materialized_tail(&session, &[], &[]);
        let stale = converted_prefix(&session, &chat);
        let pending = chat.unconverted_prefix();
        assert_eq!(stale.len(), pending);

        // Compaction rewrites the whole conversation at fresh ordinals and
        // leaves it exactly as long, so counting alone still lines up.
        let rewritten = materialized_session_from(1_000, session.transcript.len() as u64);
        chat.apply_materialized(&rewritten, &[], &[]);
        assert_eq!(chat.unconverted_prefix(), pending);
        assert!(
            stale.last().unwrap().start_seq < chat.entries[0].start_seq,
            "the replaced history still sorts in front of the rewritten tail"
        );

        assert!(!chat.splice_transcript_prefix(stale));

        assert_eq!(chat.unconverted_prefix(), pending);
        assert_eq!(
            entry_texts(&chat.entries),
            entry_texts(&materialized_chat_entries(&rewritten)[pending..])
        );
    }

    #[test]
    fn compaction_below_the_pending_prefix_reseats_the_tail() {
        let session = long_materialized_session(TAIL_SEED_ITEMS as u64 + 500);
        let mut chat = ChatState::from_materialized_tail(&session, &[], &[]);
        let prefix = converted_prefix(&session, &chat);

        // Compaction leaves a transcript shorter than the pending prefix.
        let mut compacted = long_materialized_session(TAIL_SEED_ITEMS as u64 + 100);
        compacted.applied_event_ordinal = session.applied_event_ordinal + 1;
        chat.apply_materialized(&compacted, &[], &[]);

        assert_eq!(chat.unconverted_prefix(), 100);
        assert_eq!(chat.entries.len(), TAIL_SEED_ITEMS);
        assert_eq!(
            entry_texts(&chat.entries),
            entry_texts(&materialized_chat_entries(&compacted)[100..])
        );
        // The history built against the old transcript no longer fits.
        assert!(!chat.splice_transcript_prefix(prefix));
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

    fn browser_tail_label(entry: &BrowserTranscriptEntry) -> String {
        format!("{}: {}", entry.label, entry.lines[0])
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
    fn live_acp_diffs_render_paths_without_counting_lines_on_the_event_loop() {
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
                "│ /workspace/src/lib.rs",
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

        assert_eq!(chat.entries[0].tool_diffstats, ["/workspace/src/lib.rs"]);
        assert_eq!(
            transcript_text(&mut chat, 80),
            [
                "✓ Tool · done",
                "│ Edit src/lib.rs",
                "│ /workspace/src/lib.rs",
                ""
            ]
        );
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
    fn kimi_shell_tool_run_collapses_to_command_names() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.entries
            .push(completed_tool(1, "Running: rg -n project_memory src"));
        chat.entries
            .push(completed_tool(2, "Running: cargo test --lib"));
        chat.entries
            .push(completed_tool(3, "Starting background: npm run preview"));
        chat.entries
            .push(ChatEntry::plain(4, ChatRole::User, "continue"));

        assert_eq!(
            transcript_text(&mut chat, 80),
            [
                "✓ Tool · done",
                "│ rg, cargo, npm",
                "",
                "❯ You",
                "│ continue",
                "",
            ]
        );
    }

    #[test]
    fn interleaved_tools_and_thoughts_render_latest_thinking_then_tool_cdl() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.entries.extend([
            completed_tool(1, "sed -n 1,260p .agents/PLANS.md"),
            thought(2, "Planning coverage analysis with cargo llvm-cov"),
            completed_tool(3, "cargo llvm-cov nextest --help"),
            thought(4, "Requesting full main help information"),
            completed_tool(5, "cargo llvm-cov --help"),
            thought(6, "Planning durable coverage storage"),
            completed_tool(7, "cargo llvm-cov report --help"),
            thought(8, "Planning optimized coverage reporting"),
            completed_tool(9, "Editing files"),
            thought(10, "Preparing coverage environment cleanup"),
        ]);

        assert_eq!(
            transcript_text(&mut chat, 80),
            [
                "○ Thinking",
                "│ Preparing coverage environment cleanup",
                "",
                "✓ Tool · done",
                "│ sed, cargo, cargo, cargo, Editing",
                "",
            ]
        );
    }

    #[test]
    fn thought_only_streak_keeps_only_the_most_recent_block() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.entries.extend([
            thought(1, "first approach"),
            thought(2, "second approach"),
            thought(3, "final approach"),
        ]);

        assert_eq!(
            transcript_text(&mut chat, 80),
            ["○ Thinking", "│ final approach", ""]
        );
    }

    #[test]
    fn visible_nonmembers_break_tool_thought_streaks() {
        let separators = [
            ChatEntry::plain(3, ChatRole::User, "user boundary"),
            ChatEntry::plain(3, ChatRole::Agent, "agent boundary"),
            ChatEntry::plan(3, Vec::new()),
            ChatEntry::plain(3, ChatRole::System, "system boundary"),
            ChatEntry::tool(3, "waiting tool", None, ToolStatus::Pending),
            ChatEntry::tool(3, "running tool", None, ToolStatus::Running),
            ChatEntry::tool(3, "failed tool", None, ToolStatus::Failed),
        ];

        for separator in separators {
            let mut chat = ChatState::new(&snapshot(), &[]);
            chat.entries.extend([
                thought(1, "thought before boundary"),
                completed_tool(2, "grep -rn alpha src"),
                separator,
                thought(4, "thought after boundary"),
                completed_tool(5, "cat notes.md"),
                ChatEntry::plain(6, ChatRole::User, "release trailing tool"),
            ]);

            let rendered = transcript_text(&mut chat, 80);
            assert!(rendered.contains(&"│ thought before boundary".to_owned()));
            assert!(rendered.contains(&"│ thought after boundary".to_owned()));
            assert!(rendered.contains(&"│ grep -rn alpha src".to_owned()));
            assert!(rendered.contains(&"│ cat notes.md".to_owned()));
            assert!(!rendered.contains(&"│ grep, cat".to_owned()));
        }
    }

    #[test]
    fn trailing_tool_stays_detailed_until_a_later_thought_appears() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.entries.extend([
            completed_tool(1, "grep -rn alpha src"),
            thought(2, "checking the first result"),
            completed_tool(3, "cat notes.md"),
        ]);

        assert_eq!(
            transcript_text(&mut chat, 80),
            [
                "○ Thinking",
                "│ checking the first result",
                "",
                "✓ Tool · done",
                "│ grep -rn alpha src",
                "",
                "✓ Tool · done",
                "│ cat notes.md",
                "",
            ]
        );

        chat.entries
            .push(thought(4, "checking the combined result"));

        assert_eq!(
            transcript_text(&mut chat, 80),
            [
                "○ Thinking",
                "│ checking the combined result",
                "",
                "✓ Tool · done",
                "│ grep, cat",
                "",
            ]
        );
    }

    #[test]
    fn updating_the_latest_collapsed_thought_invalidates_the_summary_cache() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.entries.extend([
            completed_tool(1, "grep -rn alpha src"),
            thought(2, "old thought"),
            completed_tool(3, "cat notes.md"),
            thought(4, "latest thought"),
        ]);
        assert!(transcript_text(&mut chat, 80).contains(&"│ latest thought".to_owned()));

        chat.entries[3].text = "revised latest thought".into();
        chat.entries[3].touch(5);

        let rendered = transcript_text(&mut chat, 80);
        assert!(rendered.contains(&"│ revised latest thought".to_owned()));
        assert!(!rendered.contains(&"│ latest thought".to_owned()));
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
    fn raw_mode_preserves_interleaved_tools_and_thoughts_in_source_order() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.render_mode = TranscriptRenderMode::Raw;
        chat.entries.extend([
            completed_tool(1, "grep -rn alpha src"),
            thought(2, "first thought"),
            completed_tool(3, "cat notes.md"),
            thought(4, "latest thought"),
        ]);

        assert_eq!(
            transcript_text(&mut chat, 80),
            [
                "✓ Tool · done",
                "│ grep -rn alpha src",
                "",
                "○ Thinking",
                "│ first thought",
                "",
                "✓ Tool · done",
                "│ cat notes.md",
                "",
                "○ Thinking",
                "│ latest thought",
                "",
            ]
        );
    }

    #[test]
    fn a_later_running_tool_releases_earlier_results_and_stays_expanded_when_completed() {
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
                "│ grep, grep",
                "",
                "● Tool · running",
                "│ cat notes.md",
                "",
            ]
        );

        chat.entries[2].touch(4);
        chat.entries[2].tool_status = Some(ToolStatus::Completed);

        // Once completed, the trailing tool protects its own full result.
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
    fn exact_diffstats_are_available_only_after_the_tool_finishes() {
        let item = |status: &str| TranscriptItem {
            stable_id: "tool:edit".into(),
            position: 1,
            latest_content_event_ordinal: None,
            created_at_ms: 1,
            last_changed_at_ms: 2,
            body: TranscriptBody::Tool {
                call: serde_json::json!({
                    "toolCallId": "edit",
                    "title": "Edit src/lib.rs",
                    "status": status,
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
        };

        assert_eq!(materialized_tool_diffstats(&item("in_progress")), None);
        assert_eq!(
            materialized_tool_diffstats(&item("completed")),
            Some(vec!["/workspace/src/lib.rs  +1 −0".into()])
        );
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
    fn agent_preview_head_removes_punctuation_before_its_ellipsis() {
        let lines = render_agent_message_head(
            "first line\n**late-corpus diagnostics,**\nthird line",
            80,
            2,
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

        assert_eq!(rendered, ["│ first line", "│ late-corpus diagnostics…"]);
        assert!(
            lines[1]
                .spans
                .last()
                .unwrap()
                .style
                .add_modifier
                .contains(Modifier::BOLD)
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

    /// A delta has to be proportional to what changed, not to the window. The
    /// bodies the projection records no change ordinal for still overshoot, so
    /// this asserts a large reduction rather than a minimal one.
    #[test]
    fn a_delta_costs_a_fraction_of_the_window_it_updates() {
        let mut session = long_materialized_session(600);
        let frontier = session.applied_event_ordinal;
        let bytes = |transcript: &BrowserTranscript| {
            serde_json::to_string(&transcript.entries).unwrap().len()
        };
        let window =
            bytes(&TranscriptSnapshot::from_materialized(&session).browser_transcript(None));

        let appended = frontier + 1;
        session.transcript.push(fixture_agent_item(appended));
        session.applied_event_ordinal = appended;
        let delta =
            TranscriptSnapshot::from_materialized(&session).browser_transcript(Some(frontier));

        println!("window {window} bytes, delta {} bytes", bytes(&delta));
        assert!(
            bytes(&delta) * 4 < window,
            "one appended message resent {} of {window} bytes",
            bytes(&delta)
        );
    }

    /// The conversation a delta test needs: settled messages the projection
    /// records an exact update cursor for.
    fn message_session(items: u64) -> MaterializedSession {
        let mut session = MaterializedSession::empty("session-delta");
        session.transcript = (1..=items)
            .map(|position| match position % 2 {
                1 => fixture_user_item(position),
                _ => fixture_agent_item(position),
            })
            .collect();
        session.applied_event_ordinal = items;
        session
    }

    fn delta_ids(session: &MaterializedSession, after_seq: u64) -> Vec<u64> {
        let delta =
            TranscriptSnapshot::from_materialized(session).browser_transcript(Some(after_seq));
        assert!(!delta.reset, "the window still covers the viewer's cursor");
        delta.entries.iter().map(|entry| entry.id).collect()
    }

    #[test]
    fn appending_one_message_marks_only_that_entry_changed() {
        let mut session = message_session(8);
        let opened = TranscriptSnapshot::from_materialized(&session).browser_transcript(None);
        assert_eq!(opened.entries.len(), 8);
        let cursor = opened.latest_seq;

        session.transcript.push(fixture_agent_item(9));
        session.applied_event_ordinal = 9;

        assert_eq!(delta_ids(&session, cursor), [9]);
    }

    #[test]
    fn a_growing_agent_message_is_the_only_entry_its_delta_carries() {
        let mut session = message_session(6);
        let cursor = TranscriptSnapshot::from_materialized(&session)
            .browser_transcript(None)
            .latest_seq;

        let streaming = Arc::make_mut(&mut session.transcript[5]);
        let TranscriptBody::Agent { chunks, .. } = &mut streaming.body else {
            panic!("expected an agent message");
        };
        chunks.push(serde_json::json!({
            "content": {"type": "text", "text": " and one more thing"}
        }));
        streaming.latest_content_event_ordinal = Some(7);
        streaming.last_changed_at_ms = FIXTURE_MS + 1;
        session.applied_event_ordinal = 7;

        assert_eq!(delta_ids(&session, cursor), [6]);
        let delta =
            TranscriptSnapshot::from_materialized(&session).browser_transcript(Some(cursor));
        assert!(delta.entries[0].lines[0].ends_with(" and one more thing"));
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
        // Away from the conversations pane, so the wheel reaches the transcript.
        let mouse = |kind| mouse_in(kind, Rect::new(0, 10, 40, 1));

        chat.handle_mouse(mouse(MouseEventKind::ScrollUp));
        let scrolled = drawn_transcript(&mut chat, 40, 24);
        assert!(!shows(&scrolled, "message 39"), "wheel up leaves the tail");

        chat.handle_mouse(mouse(MouseEventKind::ScrollDown));
        let rows = drawn_transcript(&mut chat, 40, 24);
        assert!(shows(&rows, "message 39"), "wheel down resumes following");
        assert!(!rows.iter().any(|row| row.contains("End to follow")));
    }

    #[test]
    fn the_wheel_over_an_empty_transcript_has_nothing_to_scroll() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        let rows = drawn_transcript(&mut chat, 40, 24);

        chat.handle_mouse(mouse_in(MouseEventKind::ScrollUp, Rect::new(0, 10, 40, 1)));
        chat.handle_mouse(mouse_in(
            MouseEventKind::ScrollDown,
            Rect::new(0, 10, 40, 1),
        ));

        assert_eq!(drawn_transcript(&mut chat, 40, 24), rows);
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
                    terminal_outputs: Vec::new(),
                    terminal_refs: Vec::new(),
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
        // The remote viewer mirrors the TUI's Rich feed, so a tool entry is its
        // title alone: neither the content details nor the locations belong
        // there, however many the projection kept for Raw mode.
        assert_eq!(browser.entries[0].lines, ["inspect"]);
        assert!(
            browser.entries[1]
                .lines
                .iter()
                .any(|line| line == "○ step-11")
        );
    }

    #[test]
    fn materialized_terminal_content_renders_output_and_exit_summary() {
        let mut session = MaterializedSession::empty("session-terminal");
        session.applied_event_ordinal = 1;
        session.applied_event_digest = "a".repeat(64);
        session.transcript = vec![Arc::new(TranscriptItem {
            stable_id: "tool:bash".into(),
            position: 1,
            latest_content_event_ordinal: None,
            created_at_ms: 1,
            last_changed_at_ms: 1,
            body: TranscriptBody::Tool {
                call: serde_json::json!({
                    "toolCallId": "bash",
                    "title": "Bash",
                    "status": "completed",
                    "content": [{"type": "terminal", "terminalId": "term-1"}]
                }),
                terminal_outputs: vec![TerminalOutputRecord {
                    terminal_id: "term-1".into(),
                    // Colored output from a real build tool: the escape must
                    // not survive into the terminal hel is drawing on.
                    output: "\u{1b}[32mtests passed\u{1b}[0m".into(),
                    truncated: false,
                    exit_code: Some(0),
                    signal: None,
                }],
                terminal_refs: vec!["term-1".into()],
            },
        })];

        let entries = materialized_chat_entries(&session);
        assert_eq!(entries[0].tool_content, ["tests passed\nexited 0"]);

        let mut chat = ChatState::from_materialized(&session, &[], &[]);
        chat.render_mode = TranscriptRenderMode::Raw;
        let rendered = transcript_text(&mut chat, 80);
        assert!(
            rendered.iter().any(|line| line.contains("tests passed")),
            "raw rows show the captured output: {rendered:?}"
        );
        assert!(
            rendered.iter().any(|line| line.contains("exited 0")),
            "raw rows show how the terminal ended: {rendered:?}"
        );
        assert!(
            !rendered.iter().any(|line| line.contains("terminal term-1")),
            "the id placeholder is replaced once output exists: {rendered:?}"
        );
        assert!(
            !rendered.iter().any(|line| line.contains('\u{1b}')),
            "escape sequences are sanitized out: {rendered:?}"
        );

        let browser = TranscriptSnapshot::from_materialized(&session).browser_transcript(None);
        assert_eq!(
            browser.entries[0].lines,
            ["Bash"],
            "the remote viewer shows the decluttered title, not the output"
        );
    }

    const STANDALONE_OUTPUT: &str = "cargo build finished";

    fn terminal_record(exit_code: Option<u32>, signal: Option<&str>) -> TerminalOutputRecord {
        TerminalOutputRecord {
            terminal_id: "term-1".into(),
            output: STANDALONE_OUTPUT.into(),
            truncated: false,
            exit_code,
            signal: signal.map(str::to_owned),
        }
    }

    fn terminal_output_item(position: u64, record: TerminalOutputRecord) -> Arc<TranscriptItem> {
        Arc::new(TranscriptItem {
            stable_id: format!("terminal:{}", record.terminal_id),
            position,
            latest_content_event_ordinal: None,
            created_at_ms: position as i64,
            last_changed_at_ms: position as i64,
            body: TranscriptBody::TerminalOutput { record },
        })
    }

    /// A hel-hosted command whose output no tool call refers to, after an agent
    /// message so the feed has something else to show.
    fn standalone_terminal_session(record: TerminalOutputRecord) -> MaterializedSession {
        let mut session = MaterializedSession::empty("session-standalone-terminal");
        session.applied_event_ordinal = 2;
        session.transcript = vec![
            agent_message_item("agent:1", 1, "running the build"),
            terminal_output_item(2, record),
        ];
        session
    }

    fn browser_lines(session: &MaterializedSession) -> Vec<String> {
        TranscriptSnapshot::from_materialized(session)
            .browser_transcript(None)
            .entries
            .into_iter()
            .flat_map(|entry| entry.lines)
            .collect()
    }

    #[test]
    fn a_cleanly_exited_standalone_terminal_item_renders_only_in_raw_mode() {
        let session = standalone_terminal_session(terminal_record(Some(0), None));

        let mut chat = ChatState::from_materialized(&session, &[], &[]);
        let rich = transcript_text(&mut chat, 80);
        assert!(
            rich.iter().any(|line| line.contains("running the build")),
            "the rest of the conversation still renders: {rich:?}"
        );
        assert!(
            !rich.iter().any(|line| line.contains(STANDALONE_OUTPUT)),
            "a clean command's output is left out of the rich feed: {rich:?}"
        );
        assert!(
            !rich.iter().any(|line| line.contains("exited 0")),
            "and so is its exit summary: {rich:?}"
        );

        let browser = browser_lines(&session);
        assert!(
            browser
                .iter()
                .any(|line| line.contains("running the build")),
            "the rest of the conversation still reaches the remote viewer: {browser:?}"
        );
        assert!(
            !browser.iter().any(|line| line.contains(STANDALONE_OUTPUT)),
            "the remote viewer mirrors the rich feed: {browser:?}"
        );

        chat.render_mode = TranscriptRenderMode::Raw;
        let raw = transcript_text(&mut chat, 80);
        assert!(
            raw.iter().any(|line| line.contains(STANDALONE_OUTPUT)),
            "raw rows keep the captured output: {raw:?}"
        );
        assert!(
            raw.iter().any(|line| line.contains("exited 0")),
            "raw rows keep how the terminal ended: {raw:?}"
        );
    }

    #[test]
    fn an_abnormally_ended_standalone_terminal_item_renders_everywhere() {
        for (record, summary) in [
            (terminal_record(Some(3), None), "exited 3"),
            (terminal_record(None, Some("SIGKILL")), "killed by SIGKILL"),
            (
                terminal_record(Some(0), Some("SIGKILL")),
                "killed by SIGKILL",
            ),
            (terminal_record(None, None), "released before exit"),
        ] {
            let session = standalone_terminal_session(record);

            let mut chat = ChatState::from_materialized(&session, &[], &[]);
            let rich = transcript_text(&mut chat, 80);
            assert!(
                rich.iter().any(|line| line.contains(STANDALONE_OUTPUT)),
                "{summary}: the rich feed keeps the output: {rich:?}"
            );
            assert!(
                rich.iter().any(|line| line.contains(summary)),
                "{summary}: the rich feed says how it ended: {rich:?}"
            );

            let browser = browser_lines(&session);
            assert!(
                browser.iter().any(|line| line.contains(STANDALONE_OUTPUT)),
                "{summary}: the remote viewer keeps the output: {browser:?}"
            );
            assert!(
                browser.iter().any(|line| line.contains(summary)),
                "{summary}: the remote viewer says how it ended: {browser:?}"
            );
        }
    }

    #[test]
    fn a_clean_standalone_terminal_item_between_completed_tools_keeps_one_run() {
        let mut session = MaterializedSession::empty("session-terminal-between-tools");
        session.applied_event_ordinal = 3;
        session.transcript = vec![
            fixture_tool_item(1),
            terminal_output_item(2, terminal_record(Some(0), None)),
            fixture_tool_item(3),
        ];
        let mut chat = ChatState::from_materialized(&session, &[], &[]);
        // Ends the newest result's protection, so both tools can collapse.
        chat.entries
            .push(ChatEntry::plain(4, ChatRole::User, "now ship it"));

        let text = transcript_text(&mut chat, 80);

        assert_eq!(
            text,
            [
                "✓ Tool · done",
                "│ read, read",
                "",
                "❯ You",
                "│ now ship it",
                "",
            ],
            "the omitted entry neither renders nor splits the run"
        );
    }

    /// Grok Build's final update replaces `content` with plain text, so the
    /// output hel captured is attached to the item with nothing in the call
    /// pointing at it. It is still the only copy of what the command printed.
    #[test]
    fn attached_terminal_output_renders_when_the_call_no_longer_refers_to_it() {
        let mut session = MaterializedSession::empty("session-dropped-terminal");
        session.applied_event_ordinal = 1;
        session.applied_event_digest = "a".repeat(64);
        session.transcript = vec![Arc::new(TranscriptItem {
            stable_id: "tool:bash".into(),
            position: 1,
            latest_content_event_ordinal: None,
            created_at_ms: 1,
            last_changed_at_ms: 1,
            body: TranscriptBody::Tool {
                call: serde_json::json!({
                    "toolCallId": "bash",
                    "title": "Bash",
                    "status": "completed",
                    "content": [{
                        "type": "content",
                        "content": {"type": "text", "text": "ran the build"}
                    }]
                }),
                terminal_outputs: vec![TerminalOutputRecord {
                    terminal_id: "term-1".into(),
                    output: "build finished".into(),
                    truncated: false,
                    exit_code: Some(0),
                    signal: None,
                }],
                terminal_refs: vec!["term-1".into()],
            },
        })];

        let entries = materialized_chat_entries(&session);
        assert_eq!(
            entries[0].tool_content,
            ["ran the build", "build finished\nexited 0"],
            "the captured output follows the content the call still carries"
        );
    }

    /// Codex runs the command in its own terminal, which hel never opened, and
    /// reports the text in `rawOutput` beside the reference.
    #[test]
    fn codex_raw_output_renders_for_a_terminal_hel_has_no_record_for() {
        let call = |raw_output: serde_json::Value| {
            serde_json::json!({
                "toolCallId": "exec",
                "title": "Shell",
                "status": "completed",
                "content": [{"type": "terminal", "terminalId": "exec-1"}],
                "rawOutput": raw_output
            })
        };
        let details = |raw_output: serde_json::Value| {
            let call = ToolCall::deserialize(&call(raw_output)).expect("valid ACP tool call");
            tool_content_details(&call.content, &[], call.raw_output.as_ref())
        };

        assert_eq!(
            details(serde_json::json!({"formatted_output": "tests passed", "exit_code": 0})),
            ["tests passed\nexited 0"]
        );
        assert_eq!(
            details(serde_json::json!({"formatted_output": "still running"})),
            ["still running"],
            "an exit line needs an exit code to report"
        );
        assert_eq!(
            details(serde_json::json!({"exit_code": 0})),
            ["terminal exec-1"],
            "without output there is nothing to show but the id"
        );
    }

    #[test]
    fn browser_tool_entries_show_the_title_and_diffstats_only() {
        let mut session = MaterializedSession::empty("session-browser-tool");
        session.applied_event_ordinal = 1;
        session.applied_event_digest = "a".repeat(64);
        session.transcript = vec![Arc::new(TranscriptItem {
            stable_id: "tool:edit".into(),
            position: 1,
            latest_content_event_ordinal: None,
            created_at_ms: 1,
            last_changed_at_ms: 1,
            body: TranscriptBody::Tool {
                call: serde_json::json!({
                    "toolCallId": "edit",
                    "title": "Edit src/lib.rs",
                    "status": "completed",
                    "content": [
                        {
                            "type": "content",
                            "content": {"type": "text", "text": "wrote the file"}
                        },
                        {
                            "type": "diff",
                            "path": "/workspace/src/lib.rs",
                            "oldText": "alpha\n",
                            "newText": "alpha\nbeta\n"
                        }
                    ],
                    "locations": [{"path": "/workspace/src/lib.rs", "line": 2}]
                }),
                terminal_outputs: Vec::new(),
                terminal_refs: Vec::new(),
            },
        })];

        let entries = materialized_chat_entries(&session);
        assert!(entries[0].tool_content.contains(&"wrote the file".into()));
        assert_eq!(entries[0].tool_locations, ["/workspace/src/lib.rs:2"]);

        let exact_diffstats = BTreeMap::from([(
            "tool:edit".to_owned(),
            materialized_tool_diffstats(&session.transcript[0]).unwrap(),
        )]);
        let browser =
            TranscriptSnapshot::from_materialized_with_diffstats(&session, &exact_diffstats)
                .browser_transcript(None);
        assert_eq!(
            browser.entries[0].lines,
            ["Edit src/lib.rs", "/workspace/src/lib.rs  +1 −0"],
            "the remote viewer carries the Rich feed's title and diffstat, \
             not the Raw content or locations"
        );
    }

    #[test]
    fn terminal_exit_summary_names_signal_release_and_truncation() {
        let record = |exit_code, signal: Option<&str>, truncated| TerminalOutputRecord {
            terminal_id: "term-1".into(),
            output: "out".into(),
            truncated,
            exit_code,
            signal: signal.map(str::to_owned),
        };

        assert_eq!(
            terminal_exit_summary(&record(Some(0), None, false)),
            "exited 0"
        );
        assert_eq!(
            terminal_exit_summary(&record(Some(1), None, true)),
            "exited 1 · output truncated"
        );
        assert_eq!(
            terminal_exit_summary(&record(None, Some("SIGKILL"), false)),
            "killed by SIGKILL"
        );
        assert_eq!(
            terminal_exit_summary(&record(None, None, false)),
            "released before exit"
        );

        // A terminal that produced nothing is still worth a line: the summary
        // is all a reader has to go on.
        let mut silent = record(None, Some("SIGTERM"), false);
        silent.output.clear();
        assert_eq!(terminal_output_detail(&silent), "killed by SIGTERM");
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
    fn raw_mode_preserves_markdown_markers_and_exposes_tool_details() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.entries
            .push(ChatEntry::plain(1, ChatRole::Agent, "**bold**"));
        chat.render_mode = TranscriptRenderMode::Raw;
        assert!(transcript_text(&mut chat, 30).contains(&"│ **bold**".into()));
    }
}
