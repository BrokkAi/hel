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
    let mut entries = session
        .transcript
        .iter()
        .map(|item| {
            materialized_chat_entry_with_diffstats(
                item,
                session.applied_event_ordinal,
                diffstats.get(&item.stable_id),
            )
        })
        .collect::<Vec<_>>();
    suppress_duplicate_standalone_terminal_output(&mut entries);
    entries
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
    let mut entries = items
        .iter()
        .map(|item| materialized_chat_entry(item, frontier))
        .collect::<Vec<_>>();
    suppress_duplicate_standalone_terminal_output(&mut entries);
    entries
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
    let mut entries = session
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
        .collect::<Vec<_>>();
    suppress_duplicate_standalone_terminal_output(&mut entries);
    entries
}

/// Hide a legacy standalone result from Rich surfaces when a completed tool
/// already carries the exact same raw result. New projections attach and
/// remove this duplicate at the source; this pass keeps transcripts projected
/// by an older Hel release equally quiet after an upgrade.
fn suppress_duplicate_standalone_terminal_output(entries: &mut [ChatEntry]) {
    if !entries.iter().any(|entry| {
        entry
            .source
            .0
            .as_ref()
            .is_some_and(|item| matches!(&item.body, TranscriptBody::TerminalOutput { .. }))
    }) {
        return;
    }
    let tools = entries
        .iter()
        .filter_map(|entry| entry.source.0.as_ref())
        .filter(|item| matches!(&item.body, TranscriptBody::Tool { .. }))
        .cloned()
        .collect::<Vec<_>>();
    for entry in entries {
        let Some(item) = entry.source.0.as_ref() else {
            continue;
        };
        let TranscriptBody::TerminalOutput { record } = &item.body else {
            continue;
        };
        entry.raw_only = record.exited_cleanly()
            || tools.iter().any(|tool| {
                let TranscriptBody::Tool { call, .. } = &tool.body else {
                    unreachable!("filtered to tool transcript items above");
                };
                record.matches_tool_raw_result(call)
            });
    }
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
            let call = match ToolCall::deserialize(call) {
                Ok(call) => Some(call),
                Err(error) => {
                    tracing::warn!(
                        stable_id = %item.stable_id,
                        %error,
                        "could not decode a stored tool call; rendering it as invalid"
                    );
                    None
                }
            };
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
                .unwrap_or_else(|error| {
                    tracing::warn!(
                        stable_id = %item.stable_id,
                        %error,
                        "could not decode a stored plan; rendering it empty"
                    );
                    Vec::new()
                })
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
        .filter_map(|value| match ContentChunk::deserialize(value) {
            Ok(chunk) => Some(chunk),
            Err(error) => {
                tracing::warn!(%error, "could not decode a stored content chunk");
                None
            }
        })
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
        suppress_duplicate_standalone_terminal_output(&mut self.entries);
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
        let fallback = self.active_terminal_fallback();
        prepare_render_cache(
            &self.entries,
            &mut self.render_cache,
            width,
            self.render_mode,
        );
        let top = TranscriptAnchor::Row { entry: 0, row: 0 };
        if self.entries.is_empty() && fallback.is_none() {
            return TranscriptViewport {
                rows: vec![empty_transcript_row(self.transcript_loading)],
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
            if let Some(fallback) = fallback.as_ref() {
                for line in render_transcript_entry(fallback, usize::from(width), self.render_mode)
                {
                    if rows.len() == height {
                        break;
                    }
                    rows.push(line);
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
        if let Some(fallback) = self.active_terminal_fallback() {
            let lines = render_transcript_entry(
                &fallback,
                usize::from(self.render_cache.width),
                self.render_mode,
            );
            let start = lines.len().saturating_sub(height);
            for line in lines[start..].iter().rev() {
                rows.push_front(line.clone());
            }
        }
        for index in (0..self.entries.len()).rev() {
            if rows.len() >= height {
                break;
            }
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

    /// A live, non-durable tool card for ACP terminals whose agent omitted the
    /// matching tool-call update. It disappears on exit and yields immediately
    /// when a real transcript tool claims an active terminal.
    fn active_terminal_fallback(&self) -> Option<ChatEntry> {
        let mut unclaimed = self
            .active_agent_terminals
            .iter()
            .filter(|terminal| {
                self.claimed_agent_terminals
                    .get(&terminal.terminal_id)
                    .is_none_or(|claimed_at_ms| *claimed_at_ms < terminal.started_at_ms)
            })
            .collect::<Vec<_>>();
        unclaimed.sort_by_key(|terminal| terminal.started_at_ms);
        let oldest = unclaimed.first()?;
        let elapsed = crate::usage_format::format_turn_clock(
            crate::clock::epoch_seconds(),
            u64::try_from(oldest.started_at_ms / 1_000).ok(),
        );
        let command = compact_terminal_command(&oldest.command);
        let text = if unclaimed.len() == 1 {
            format!("{command}\nRunning · {elapsed}")
        } else {
            format!(
                "{} shell commands active\nOldest: {command}\nRunning · {elapsed}",
                unclaimed.len()
            )
        };
        Some(ChatEntry::tool(
            self.latest_seq,
            text,
            None,
            ToolStatus::Running,
        ))
    }

    /// Bring the row cache in line with the current entries before a scroll
    /// traversal. A collapsed run can cross hundreds of entries, so doing the
    /// whole collapse pass again for every zero-row member would be quadratic.
    fn prepare_entry_rows(&mut self) {
        let width = self.render_cache.width;
        prepare_render_cache(
            &self.entries,
            &mut self.render_cache,
            width,
            self.render_mode,
        );
    }

    /// Rendered row count for one entry after [`Self::prepare_entry_rows`].
    fn entry_rows(&mut self, index: usize) -> usize {
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
        self.prepare_entry_rows();
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
        self.prepare_entry_rows();
        let mut remaining = rows;
        while remaining > 0 {
            let entry_rows = self.entry_rows(entry);
            if entry_rows == 0 || row >= entry_rows {
                // Rich rendering collapses a tool run into one summary entry
                // and gives its other entries zero rows. They are not scroll
                // distance: upward traversal already skips them, and charging
                // for them here can strand the viewport in a long tool run.
                if entry + 1 >= self.entries.len() {
                    self.anchor = TranscriptAnchor::Bottom;
                    return;
                }
                entry += 1;
                row = 0;
                continue;
            }
            let below = entry_rows - row - 1;
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
        let anchor = TranscriptAnchor::Row { entry, row };
        // A wheel step can land exactly on the first row of the final
        // screenful. That page fills the viewport, but it is still the live
        // tail and must resume following new output. Resolve this on input so
        // ordinary render frames do not have to scan the tail twice.
        self.anchor = if self.tail_viewport(self.last_viewport_height.max(1)).top == anchor {
            TranscriptAnchor::Bottom
        } else {
            anchor
        };
    }
}

const TERMINAL_COMMAND_PREVIEW_CHARACTERS: usize = 160;

fn compact_terminal_command(command: &str) -> String {
    let command = sanitize_terminal_text(command)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if command.chars().count() <= TERMINAL_COMMAND_PREVIEW_CHARACTERS {
        return command;
    }
    let mut preview = command
        .chars()
        .take(TERMINAL_COMMAND_PREVIEW_CHARACTERS - 1)
        .collect::<String>();
    preview.push('…');
    preview
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
        let output = sanitize_terminal_text(&record.output);
        if !output.is_empty() && details.iter().any(|detail| detail == &output) {
            // Kimi sends the captured stdout as ordinary tool content and in
            // its raw result. Keep the exit summary without printing those
            // same bytes a second time in Raw mode.
            details.push(terminal_exit_summary(record));
        } else {
            details.push(sanitize_terminal_text(&terminal_output_detail(record)));
        }
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
    let call = match ToolCall::deserialize(call) {
        Ok(call) => call,
        Err(error) => {
            tracing::warn!(
                stable_id = %item.stable_id,
                %error,
                "could not decode a stored tool call while reading diff summary"
            );
            return None;
        }
    };
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
        let call = match ToolCall::deserialize(call) {
            Ok(call) => call,
            Err(error) => {
                tracing::warn!(
                    stable_id = %item.stable_id,
                    %error,
                    "could not decode a stored tool call while scheduling diff summary"
                );
                return None;
            }
        };
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
    let title = transcript_title(chat, crate::clock::epoch_seconds());
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

fn transcript_title(chat: &ChatState, now_epoch_seconds: u64) -> String {
    let summary = if chat.header_target.is_empty() || chat.header_profile.is_empty() {
        "Conversation".to_owned()
    } else {
        crate::usage_format::format_session_summary(
            &chat.header_target,
            chat.queued_prompts.len(),
            now_epoch_seconds,
            chat.turn_started_at_epoch_seconds,
            chat.last_acp_activity_at_ms,
            &chat.header_profile,
        )
    };
    match (chat.anchor, chat.render_mode) {
        (TranscriptAnchor::Bottom, TranscriptRenderMode::Rich) => format!(" {summary} "),
        (TranscriptAnchor::Bottom, TranscriptRenderMode::Raw) => {
            format!(" {summary} · raw source ")
        }
        (TranscriptAnchor::Row { entry, .. }, _) => format!(
            " {summary} · message {} of {} · End to follow ",
            entry.saturating_add(1),
            chat.entries.len()
        ),
    }
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
    if let Some(fallback) = chat.active_terminal_fallback() {
        lines.extend(render_transcript_entry(
            &fallback,
            usize::from(width),
            chat.render_mode,
        ));
    }
    if lines.is_empty() {
        lines.push(empty_transcript_row(chat.transcript_loading));
    }
    lines
}

fn empty_transcript_row(loading: bool) -> Line<'static> {
    Line::from(Span::styled(
        if loading {
            "[Loading]"
        } else {
            "No messages yet — send a prompt to begin."
        },
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
mod tests;
