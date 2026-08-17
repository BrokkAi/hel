//! Wire-format chat transcript entries shared by the worker and the chat UI.
//!
//! [`ChatEntry`] is what a worker snapshot carries in its transcript tail and
//! what the chat view renders, so it lives below both rather than inside
//! either.

use std::sync::Arc;

use crate::hel_state::TranscriptItem;

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
    pub(crate) start_seq: u64,
    pub seq: u64,
    pub role: ChatRole,
    pub text: String,
    pub(crate) recorded_at_ms: Option<i64>,
    pub(crate) revision: u64,
    pub(crate) message_id: Option<String>,
    pub(crate) tool_call_id: Option<String>,
    pub(crate) tool_status: Option<ToolStatus>,
    pub(crate) tool_content: Vec<String>,
    pub(crate) tool_diffstats: Vec<String>,
    pub(crate) tool_locations: Vec<String>,
    pub(crate) plan: Vec<PlanLine>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub(crate) leading_omitted: bool,
    /// The materialized transcript item this entry was derived from, when it
    /// came from the controller's projection. Provenance only, so it is
    /// neither serialized nor part of the entry's value.
    #[serde(skip)]
    pub(crate) source: TranscriptSource,
}

/// Handle on the transcript item an entry was derived from. Unchanged items
/// keep the same `Arc` from one projection to the next, so a pointer
/// comparison replaces re-reading the item and re-parsing its JSON.
///
/// The handle records where an entry came from, not what it says, so two
/// entries with equal content are equal whatever they were derived from.
#[derive(Debug, Clone, Default)]
pub(crate) struct TranscriptSource(pub(crate) Option<Arc<TranscriptItem>>);

impl TranscriptSource {
    pub(crate) fn is(&self, item: &Arc<TranscriptItem>) -> bool {
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
    pub(crate) fn plan(seq: u64, plan: Vec<PlanLine>) -> Self {
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

    pub(crate) fn touch(&mut self, seq: u64) {
        self.seq = seq;
        self.revision = self.revision.wrapping_add(1);
    }

    #[cfg(test)]
    pub(crate) fn bounded_for_dashboard(mut self) -> Self {
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

    pub(crate) fn with_recorded_at(mut self, recorded_at_ms: Option<i64>) -> Self {
        self.recorded_at_ms = recorded_at_ms;
        self
    }
}

pub(crate) fn is_false(value: &bool) -> bool {
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
pub(crate) enum ToolStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum PlanStatus {
    Pending,
    Running,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct PlanLine {
    pub(crate) text: String,
    pub(crate) status: PlanStatus,
}
