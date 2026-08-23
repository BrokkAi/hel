//! Controller state ingestion: projections, quotas, capacity, and notices.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use hel::hel_chat::{
    Notices, TranscriptSnapshot, materialized_content_text, materialized_tool_diffstats,
};
use hel::hel_config::HelConfig;
use hel::hel_quota::ProfileQuota;
use hel::hel_state::{
    HelState, MaterializedExecutionState, MaterializedSession, SessionRecord,
    SessionResourceAllocation, SessionState, TranscriptBody, TranscriptItem,
};
use hel::hel_targets::{
    DeploymentCapacityTarget, DeploymentCapacityUsage, ProvisionStage, SessionResourceUsage,
};

use crate::wizards::clamp_resources;
use crate::{DashboardState, Mode, SessionOperationKind, nth_key};

#[derive(Debug, Clone)]
pub(crate) struct SessionOperationDisplay {
    pub(crate) kind: SessionOperationKind,
    pub(crate) started_at_epoch_seconds: u64,
    pub(crate) placeholder: Option<SessionRecord>,
    pub(crate) stage: Option<ProvisionStage>,
    /// When the current `stage` began, so the clock can count that stage's
    /// progress instead of the whole operation's.
    pub(crate) stage_started_at_epoch_seconds: Option<u64>,
    /// The (profile, target) a resume is moving the session TO. The
    /// controller updates the session record's own profile/target as soon as
    /// a resume starts, but that update lands in a separate, disk-persisted
    /// `Controller` inside the background task; the dashboard's local
    /// session snapshot is not refreshed until the operation finishes. This
    /// field lets the in-flight row show the destination instead of the
    /// stale snapshot's pre-resume profile/target.
    pub(crate) resume_destination: Option<(String, String)>,
}

#[derive(Debug, Default)]
pub(crate) struct SessionDetail {
    pub(crate) materialized_applied_event_ordinal: Option<u64>,
    pub(crate) current_turn_started_at: Option<u64>,
    pub(crate) last_activity_at_ms: Option<u64>,
    pub(crate) last_agent_message: Option<Arc<str>>,
    /// Latest agent-content ordinals retained so a state-only read-cursor
    /// update can recompute the single unread count exactly.
    pub(crate) agent_message_latest_content_ordinals: Vec<u64>,
    pub(crate) unread_agent_messages: usize,
    pub(crate) resource_usage: Option<SessionResourceUsage>,
    pub(crate) transcript: Option<TranscriptSnapshot>,
    pub(crate) transcript_hydration: TranscriptHydration,
    pub(crate) queued_prompts: Vec<hel::hel_worker::QueuedPrompt>,
    /// What the last projection derived, so the next one only rescans the
    /// transcript items that changed.
    pub(crate) projection: MaterializedProjectionCache,
}

/// Per-item results the previous session projection derived, kept so the next
/// projection can reuse them.
///
/// Transcript items are shared by pointer and copied on write, so the items
/// two consecutive projections agree on are the ones that are pointer-equal.
/// Everything before the first difference keeps its cached result, and the
/// per-item JSON work is spent only on the changed tail.
#[derive(Debug, Default, Clone)]
pub struct MaterializedProjectionCache {
    /// The transcript these results were derived from.
    pub(crate) transcript: Vec<Arc<TranscriptItem>>,
    /// Transcript index and latest content ordinal of every agent message that
    /// has content, in transcript order.
    agent_messages: Vec<(usize, u64)>,
    /// Transcript index and text of the last agent message with text.
    pub(crate) last_agent_message: Option<(usize, Arc<str>)>,
    /// Exact stats for terminal tool items, keyed by logical identity and
    /// revision so unrelated transcript updates never repeat their diff.
    tool_diffstats: BTreeMap<(String, i64), Vec<String>>,
}

impl MaterializedProjectionCache {
    /// How many leading items this cache and `transcript` share by pointer.
    fn unchanged_prefix(&self, transcript: &[Arc<TranscriptItem>]) -> usize {
        self.transcript
            .iter()
            .zip(transcript)
            .take_while(|(cached, current)| Arc::ptr_eq(cached, current))
            .count()
    }
}

/// The last agent message with text in `transcript[range]`, searched from the
/// end so it stops at the first one it finds.
fn last_agent_message_in(
    transcript: &[Arc<TranscriptItem>],
    range: std::ops::Range<usize>,
) -> Option<(usize, Arc<str>)> {
    let start = range.start;
    transcript[range]
        .iter()
        .enumerate()
        .rev()
        .find_map(|(offset, item)| {
            let TranscriptBody::Agent { chunks, .. } = &item.body else {
                return None;
            };
            let text = hel::hel_chat::materialized_chunks_text(chunks);
            (!text.trim().is_empty()).then(|| (start + offset, Arc::from(text)))
        })
}

/// The last agent message with text, scanning the changed tail first and
/// reusing the previous answer when it still holds.
///
/// The previous answer holds when it came from an item inside the unchanged
/// prefix: nothing after that item had a message, or the previous scan would
/// have stopped later. "No message at all" holds outright, because the
/// previous scan covered every item the prefix is made of. Only an answer
/// that came from an item that changed forces a rescan of the prefix, and
/// that rescan still stops at the first message it finds.
pub(crate) fn last_agent_message(
    transcript: &[Arc<TranscriptItem>],
    unchanged_prefix: usize,
    previous: &MaterializedProjectionCache,
) -> Option<(usize, Arc<str>)> {
    if let Some(found) = last_agent_message_in(transcript, unchanged_prefix..transcript.len()) {
        return Some(found);
    }
    match &previous.last_agent_message {
        Some((index, text)) if *index < unchanged_prefix => Some((*index, text.clone())),
        Some(_) => last_agent_message_in(transcript, 0..unchanged_prefix),
        None => None,
    }
}

pub struct PreparedMaterializedSessionDetail {
    pub(crate) session_id: String,
    pub(crate) applied_event_ordinal: u64,
    pub(crate) session_title: Option<String>,
    pub(crate) current_turn_started_at: Option<u64>,
    pub(crate) last_activity_at_ms: Option<u64>,
    pub(crate) last_agent_message: Option<Arc<str>>,
    agent_message_latest_content_ordinals: Vec<u64>,
    pub(crate) unread_agent_messages: usize,
    pub(crate) transcript: TranscriptSnapshot,
    pub(crate) queued_prompts: Vec<hel::hel_worker::QueuedPrompt>,
    pub(crate) projection: MaterializedProjectionCache,
}

impl PreparedMaterializedSessionDetail {
    /// Projects one session for the dashboard, reusing what `previous`
    /// derived for the transcript items that did not change.
    pub fn from_materialized(
        session: MaterializedSession,
        detached_after_event_ordinal: u64,
        previous: MaterializedProjectionCache,
    ) -> Self {
        let current_turn_started_at = match session.execution {
            MaterializedExecutionState::Running { started_at_ms } => {
                u64::try_from(started_at_ms).ok().map(|value| value / 1_000)
            }
            MaterializedExecutionState::Idle
            | MaterializedExecutionState::Closing
            | MaterializedExecutionState::Closed => None,
        };
        let unchanged_prefix = previous.unchanged_prefix(&session.transcript);
        let last_agent_message =
            last_agent_message(&session.transcript, unchanged_prefix, &previous);
        let mut cached_tool_diffstats = previous.tool_diffstats;
        let mut tool_diffstats = BTreeMap::new();
        let mut current_tool_diffstats = BTreeMap::new();
        for item in &session.transcript {
            let key = (item.stable_id.clone(), item.last_changed_at_ms);
            let stats = cached_tool_diffstats
                .remove(&key)
                .or_else(|| materialized_tool_diffstats(item));
            if let Some(stats) = stats {
                current_tool_diffstats.insert(item.stable_id.clone(), stats.clone());
                tool_diffstats.insert(key, stats);
            }
        }
        // Unread counting needs every agent message, so the list is carried
        // forward and only its changed tail is rebuilt.
        let mut agent_messages = previous.agent_messages;
        agent_messages
            .truncate(agent_messages.partition_point(|(index, _)| *index < unchanged_prefix));
        for (index, item) in session.transcript.iter().enumerate().skip(unchanged_prefix) {
            if item.is_nonempty_agent_message()
                && let Some(ordinal) = item.latest_content_event_ordinal
            {
                agent_messages.push((index, ordinal));
            }
        }
        let agent_message_latest_content_ordinals = agent_messages
            .iter()
            .map(|(_, ordinal)| *ordinal)
            .collect::<Vec<_>>();
        let unread_agent_messages = agent_message_latest_content_ordinals
            .iter()
            .filter(|ordinal| **ordinal > detached_after_event_ordinal)
            .count();
        let queued_prompts = session
            .queued_prompts
            .iter()
            .map(|prompt| hel::hel_worker::QueuedPrompt {
                id: prompt.command_id.clone(),
                text: materialized_content_text(&prompt.content),
                attachments: Vec::new(),
                created_at_ms: prompt.queued_at_ms,
            })
            .collect();
        let session_id = session.session_id.clone();
        let applied_event_ordinal = session.applied_event_ordinal;
        let session_title = session.session_title.clone();
        let last_activity_at_ms = session
            .last_activity_at_ms()
            .and_then(|value| u64::try_from(value).ok());
        let transcript =
            TranscriptSnapshot::from_materialized_with_diffstats(&session, &current_tool_diffstats);
        Self {
            session_id,
            applied_event_ordinal,
            session_title,
            current_turn_started_at,
            last_activity_at_ms,
            last_agent_message: last_agent_message
                .as_ref()
                .map(|(_, text)| Arc::clone(text)),
            agent_message_latest_content_ordinals,
            unread_agent_messages,
            transcript,
            queued_prompts,
            projection: MaterializedProjectionCache {
                transcript: session.transcript,
                agent_messages,
                last_agent_message,
                tool_diffstats,
            },
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TranscriptHydration {
    #[default]
    Loading,
    Ready,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CapacityDetail {
    pub(crate) target: DeploymentCapacityTarget,
    pub(crate) usage: Option<DeploymentCapacityUsage>,
    pub(crate) on_demand: bool,
    /// When the reading in `usage` was taken. A sample that stopped refreshing
    /// must not read as current, so the clock travels with the reading.
    pub(crate) sampled_at_epoch_seconds: Option<u64>,
    /// Why the most recent probe failed, if it did. The last good reading stays
    /// on screen beside it rather than vanishing on one failed probe.
    pub(crate) probe_error: Option<String>,
}

impl DashboardState {
    pub fn set_greeting(&mut self, greeting: String) {
        self.greeting = greeting;
    }

    pub fn set_config(&mut self, config: HelConfig) {
        self.config = config;
        // Closing the modal drops the resume dialog, and with it its rows.
        self.cancel_modal();
        self.clamp_selections();
    }

    pub fn set_state(&mut self, state: HelState) {
        self.state = state;
        self.session_details
            .retain(|session_id, _| self.state.sessions.contains_key(session_id));
        for session_id in self.state.sessions.keys() {
            self.session_details.entry(session_id.clone()).or_default();
        }
        self.apply_operation_projection();
        for (session_id, detail) in &mut self.session_details {
            let detached_after_event_ordinal = self
                .state
                .sessions
                .get(session_id)
                .map_or(0, |session| session.detached_after_event_ordinal);
            detail.unread_agent_messages = detail
                .agent_message_latest_content_ordinals
                .iter()
                .filter(|ordinal| **ordinal > detached_after_event_ordinal)
                .count();
        }
        // After the projection, so the rows see the records the dashboard does.
        self.rebuild_resume_rows();
        self.clamp_selections();
    }

    pub fn begin_session_operation(
        &mut self,
        session_id: String,
        kind: SessionOperationKind,
        placeholder: Option<SessionRecord>,
    ) {
        self.session_operations.insert(
            session_id,
            SessionOperationDisplay {
                kind,
                started_at_epoch_seconds: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                placeholder,
                stage: None,
                stage_started_at_epoch_seconds: None,
                resume_destination: None,
            },
        );
        self.apply_operation_projection();
        self.rebuild_resume_rows();
        self.clamp_selections();
    }

    /// Record the profile/target a resume is moving `session_id` to, so its
    /// in-flight "Resuming" row shows the destination rather than the
    /// session's pre-resume profile/target. A finished or unknown operation
    /// is left alone.
    pub fn set_resume_destination(
        &mut self,
        session_id: &str,
        profile_id: String,
        target_template_id: String,
    ) {
        if let Some(operation) = self.session_operations.get_mut(session_id) {
            operation.resume_destination = Some((profile_id, target_template_id));
        }
    }

    /// Name the launch phase in flight; a finished or unknown operation is
    /// left alone. Only a stage change resets the per-stage clock, so a
    /// repeated report of the same stage can't restart its counter.
    pub fn set_session_operation_stage(&mut self, session_id: &str, stage: ProvisionStage) {
        if let Some(operation) = self.session_operations.get_mut(session_id)
            && operation.stage != Some(stage)
        {
            operation.stage = Some(stage);
            operation.stage_started_at_epoch_seconds = Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            );
        }
    }

    pub fn rekey_session_operation(&mut self, previous: &str, session_id: String) {
        if let Some(mut operation) = self.session_operations.remove(previous) {
            operation.placeholder = None;
            self.session_operations.insert(session_id, operation);
        }
        self.apply_operation_projection();
        self.rebuild_resume_rows();
        self.clamp_selections();
    }

    pub fn finish_session_operation(&mut self, session_id: &str) {
        self.session_operations.remove(session_id);
        if self
            .state
            .sessions
            .get(session_id)
            .is_some_and(|session| session.id.starts_with("pending-"))
        {
            self.state.sessions.remove(session_id);
        }
        self.rebuild_resume_rows();
        self.clamp_selections();
    }

    fn apply_operation_projection(&mut self) {
        for (session_id, operation) in &self.session_operations {
            if let Some(placeholder) = &operation.placeholder {
                self.state
                    .sessions
                    .entry(session_id.clone())
                    .or_insert_with(|| placeholder.clone());
            }
            if matches!(
                operation.kind,
                SessionOperationKind::Launching
                    | SessionOperationKind::Resuming
                    | SessionOperationKind::Importing
            ) && let Some(session) = self.state.sessions.get_mut(session_id)
            {
                session.state = SessionState::Provisioning;
            }
        }
    }

    pub fn set_quotas(&mut self, quotas: BTreeMap<String, ProfileQuota>) {
        self.quota_refreshing.retain(|id| !quotas.contains_key(id));
        self.quotas = quotas;
        self.clamp_selections();
    }

    pub fn begin_quota_refresh(&mut self, profile_ids: impl IntoIterator<Item = String>) {
        self.quota_refreshing.extend(profile_ids);
    }

    pub fn apply_quota(&mut self, quota: ProfileQuota) {
        self.quota_refreshing.remove(&quota.profile_id);
        self.quotas.insert(quota.profile_id.clone(), quota);
    }

    pub fn apply_resource_usage(&mut self, session_id: &str, usage: SessionResourceUsage) {
        self.session_details
            .entry(session_id.to_string())
            .or_default()
            .resource_usage = Some(usage);
    }

    pub fn set_deployment_capacity_targets(&mut self, targets: Vec<DeploymentCapacityTarget>) {
        let mut previous = std::mem::take(&mut self.capacity_details);
        self.capacity_details = targets
            .into_iter()
            .map(|target| {
                let id = target.id.clone();
                let detail = previous.remove(&id).map_or(
                    CapacityDetail {
                        target: target.clone(),
                        usage: None,
                        on_demand: false,
                        sampled_at_epoch_seconds: None,
                        probe_error: None,
                    },
                    |mut detail| {
                        detail.target = target;
                        detail
                    },
                );
                (id, detail)
            })
            .collect();
        self.capacity_index = self
            .capacity_index
            .min(self.capacity_details.len().saturating_sub(1));
    }

    /// Folds in one capacity sample. A failed probe keeps the last reading and
    /// records why the probe failed, so the pane can mark the row stale instead
    /// of showing an hours-old sample as if it were current.
    pub fn apply_deployment_capacity(
        &mut self,
        target_id: &str,
        result: std::result::Result<Option<DeploymentCapacityUsage>, String>,
        sampled_at_epoch_seconds: u64,
    ) {
        let Some(detail) = self.capacity_details.get_mut(target_id) else {
            return;
        };
        match result {
            Ok(usage) => {
                detail.on_demand = usage.is_none();
                detail.usage = usage;
                detail.sampled_at_epoch_seconds = Some(sampled_at_epoch_seconds);
                detail.probe_error = None;
            }
            Err(error) => detail.probe_error = Some(error),
        }
        let affected_targets = detail.target.target_ids.clone();
        let limits = detail
            .usage
            .as_ref()
            .map(|usage| (usage.logical_cores, usage.memory_total_bytes));
        if let Some(limits) = limits {
            match &mut self.mode {
                Mode::New(wizard) => {
                    let selected = nth_key(&self.config.targets, wizard.target);
                    if affected_targets.contains(&selected)
                        && let Some(SessionResourceAllocation::Container { cpus, memory_bytes }) =
                            &wizard.resource_allocation
                    {
                        let (cpus, memory_bytes) =
                            clamp_resources(*cpus, *memory_bytes, Some(limits));
                        wizard.resource_allocation =
                            Some(SessionResourceAllocation::Container { cpus, memory_bytes });
                        wizard.sizing_error = None;
                    }
                }
                Mode::Resume(wizard) => {
                    let selected = nth_key(&self.config.targets, wizard.target);
                    if affected_targets.contains(&selected)
                        && let Some(SessionResourceAllocation::Container { cpus, memory_bytes }) =
                            &wizard.resource_allocation
                    {
                        let (cpus, memory_bytes) =
                            clamp_resources(*cpus, *memory_bytes, Some(limits));
                        wizard.resource_allocation =
                            Some(SessionResourceAllocation::Container { cpus, memory_bytes });
                        wizard.sizing_error = None;
                    }
                }
                _ => {}
            }
        }
    }

    /// Replace dashboard detail with the controller's durable logical-session
    /// projection. Unread is a count of logical agent messages with content
    /// added after the last detach cursor, never a count of stream chunks.
    pub fn apply_materialized_session(&mut self, session: &MaterializedSession) {
        let detached_after_event_ordinal = self
            .state
            .sessions
            .get(&session.session_id)
            .map_or(0, |record| record.detached_after_event_ordinal);
        let previous = self.take_projection_cache(&session.session_id);
        self.apply_prepared_materialized_session(
            PreparedMaterializedSessionDetail::from_materialized(
                session.clone(),
                detached_after_event_ordinal,
                previous,
            ),
        );
    }

    /// Hands the last projection's per-item results to the next projection,
    /// which runs off the UI task. A projection that never comes back, or one
    /// that arrives too late to apply, only costs the next one a full rescan.
    pub fn take_projection_cache(&mut self, session_id: &str) -> MaterializedProjectionCache {
        self.session_details
            .get_mut(session_id)
            .map(|detail| std::mem::take(&mut detail.projection))
            .unwrap_or_default()
    }

    pub fn apply_prepared_materialized_session(
        &mut self,
        prepared: PreparedMaterializedSessionDetail,
    ) -> bool {
        let detail = self
            .session_details
            .entry(prepared.session_id.clone())
            .or_default();
        if detail
            .materialized_applied_event_ordinal
            .is_some_and(|current| prepared.applied_event_ordinal < current)
        {
            return false;
        }
        detail.materialized_applied_event_ordinal = Some(prepared.applied_event_ordinal);
        detail.current_turn_started_at = prepared.current_turn_started_at;
        detail.last_activity_at_ms = prepared.last_activity_at_ms;
        detail.last_agent_message = prepared.last_agent_message;
        detail.agent_message_latest_content_ordinals =
            prepared.agent_message_latest_content_ordinals;
        detail.unread_agent_messages = prepared.unread_agent_messages;
        detail.transcript = Some(prepared.transcript);
        detail.transcript_hydration = TranscriptHydration::Ready;
        detail.queued_prompts = prepared.queued_prompts;
        detail.projection = prepared.projection;
        if let Some(title) = prepared.session_title.as_ref()
            && let Some(record) = self.state.sessions.get_mut(&prepared.session_id)
        {
            record.acp_session_title = Some(title.clone());
            self.rebuild_resume_rows();
        }
        true
    }

    pub fn mark_transcript_unavailable(&mut self, session_id: &str) {
        self.session_details
            .entry(session_id.to_string())
            .or_default()
            .transcript_hydration = TranscriptHydration::Unavailable;
    }

    pub fn apply_queued_prompts(
        &mut self,
        session_id: &str,
        queued_prompts: Vec<hel::hel_worker::QueuedPrompt>,
    ) {
        self.session_details
            .entry(session_id.to_owned())
            .or_default()
            .queued_prompts = queued_prompts;
    }

    pub fn apply_checkpoint_archive_sizes(&mut self, sizes: BTreeMap<String, Option<u64>>) {
        self.checkpoint_archive_sizes = sizes;
        self.rebuild_resume_rows();
    }

    /// Installs the process-wide notifications bar, so every view reports
    /// through one shared slot.
    pub fn share_notices(&mut self, notices: Notices) {
        self.notices = notices;
    }

    pub fn set_notice(&mut self, notice: impl Into<String>) {
        self.notices.set(notice);
    }

    pub fn replace_notice_if(&mut self, expected: &str, replacement: impl Into<String>) -> bool {
        self.notices.replace_if(expected, replacement)
    }

    pub fn clear_notice(&mut self) {
        self.notices.clear();
    }

    /// The current shared notice, if any.
    pub fn notice(&self) -> Option<String> {
        self.notices.current()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use ratatui::style::{Color, Modifier, Style};

    use hel::hel_chat::Notices;
    use hel::hel_state::{
        HelState, MaterializedExecutionState, MaterializedSession, SessionState, TranscriptBody,
        TranscriptItem,
    };
    use hel::hel_targets::ProvisionStage;

    use super::*;
    use crate::test_support::*;

    use crate::render::unread_line;
    use crate::{DashboardState, SessionOperationKind};

    #[test]
    fn resume_is_projected_into_active_while_background_work_runs() {
        let mut dashboard = dashboard_with_session(stopped_session());
        dashboard.begin_session_operation("session-1".into(), SessionOperationKind::Resuming, None);

        assert_eq!(
            dashboard.state.sessions["session-1"].state,
            SessionState::Provisioning
        );
        assert_eq!(
            dashboard.session_operations["session-1"].kind,
            SessionOperationKind::Resuming
        );
    }

    #[test]
    fn notice_replacement_does_not_overwrite_a_newer_notice() {
        let mut dashboard = DashboardState::new(config(), HelState::default(), BTreeMap::new());
        dashboard.set_notice("Refreshing profile quotas…");
        assert!(
            dashboard.replace_notice_if("Refreshing profile quotas…", "Profile quotas refreshed.")
        );
        assert_eq!(
            dashboard.notice().as_deref(),
            Some("Profile quotas refreshed.")
        );

        dashboard.set_notice("A later operation failed");
        assert!(
            !dashboard.replace_notice_if("Refreshing profile quotas…", "Profile quotas refreshed.")
        );
        assert_eq!(
            dashboard.notice().as_deref(),
            Some("A later operation failed")
        );
    }

    /// The dashboard and every other view (chat, background workers) share
    /// one notifications bar: a clone installed with `share_notices` sees
    /// what the dashboard sets, and the dashboard sees what the clone sets.
    #[test]
    fn a_shared_notice_is_visible_through_every_clone_of_the_handle() {
        let mut dashboard = DashboardState::new(config(), HelState::default(), BTreeMap::new());
        let shared = Notices::default();
        dashboard.share_notices(shared.clone());

        dashboard.set_notice("Background import finished");
        assert_eq!(
            shared.current().as_deref(),
            Some("Background import finished")
        );

        shared.clear();
        assert_eq!(dashboard.notice(), None);

        shared.set("Quota refresh finished");
        assert_eq!(
            dashboard.notice().as_deref(),
            Some("Quota refresh finished")
        );
    }

    #[test]
    fn unread_count_uses_logical_agent_positions_after_the_detach_cursor() {
        let mut session = stopped_session();
        session.state = SessionState::Running;
        let mut dashboard = dashboard_with_session(session);
        apply_materialized_transcript(
            &mut dashboard,
            vec![
                agent_message(1, "first message"),
                thought(3, "thinking"),
                agent_message(4, "second message"),
            ],
        );

        let detail = dashboard.session_details.get("session-1").unwrap();
        assert_eq!(detail.unread_agent_messages, 2);
        let badge = unread_line(2);
        assert_eq!(badge.spans[0].content.as_ref(), "2 unread");
        assert_eq!(
            badge.spans[0].style,
            Style::default()
                .fg(Color::LightYellow)
                .add_modifier(Modifier::BOLD)
        );

        let mut state = dashboard.state.clone();
        state
            .sessions
            .get_mut("session-1")
            .unwrap()
            .detached_after_event_ordinal = 1;
        dashboard.set_state(state);
        assert_eq!(
            dashboard.session_details["session-1"].unread_agent_messages,
            1
        );

        let mut state = dashboard.state.clone();
        state
            .sessions
            .get_mut("session-1")
            .unwrap()
            .detached_after_event_ordinal = 4;
        dashboard.set_state(state);
        assert_eq!(
            dashboard.session_details["session-1"].unread_agent_messages,
            0
        );
    }

    #[test]
    fn materialized_message_update_does_not_duplicate_unread_count() {
        let mut session = stopped_session();
        session.state = SessionState::Running;
        let mut dashboard = dashboard_with_session(session);
        let mut initial = materialized_session_for("session-1", vec![agent_message(1, "first ")]);
        initial
            .queued_prompts
            .push(hel::hel_state::MaterializedQueuedPrompt {
                command_id: "queued-1".into(),
                kind: hel::hel_state::QueuedCommandKind::Prompt,
                content: vec![serde_json::json!({ "type": "text", "text": "next task" })],
                queued_at_ms: 0,
            });
        dashboard.apply_materialized_session(&initial);

        let mut state = dashboard.state.clone();
        state
            .sessions
            .get_mut("session-1")
            .unwrap()
            .detached_after_event_ordinal = 1;
        dashboard.set_state(state);
        assert_eq!(
            dashboard.session_details["session-1"].unread_agent_messages,
            0
        );

        let mut updated = agent_message(1, "first continuation");
        Arc::make_mut(&mut updated).latest_content_event_ordinal = Some(2);
        Arc::make_mut(&mut updated).last_changed_at_ms = 2_000;
        let mut projection = materialized_session_for("session-1", vec![updated]);
        projection.applied_event_ordinal = 2;
        dashboard.apply_materialized_session(&projection);

        let detail = &dashboard.session_details["session-1"];
        assert_eq!(detail.unread_agent_messages, 1);
        assert_eq!(
            detail.last_agent_message.as_deref(),
            Some("first continuation")
        );
        assert!(detail.queued_prompts.is_empty());

        let mut state = dashboard.state.clone();
        state
            .sessions
            .get_mut("session-1")
            .unwrap()
            .detached_after_event_ordinal = 2;
        dashboard.set_state(state);
        assert_eq!(
            dashboard.session_details["session-1"].unread_agent_messages,
            0
        );
    }

    #[test]
    fn prepared_materialized_session_drops_stale_ordinals() {
        let mut session = stopped_session();
        session.state = SessionState::Running;
        let mut dashboard = dashboard_with_session(session);
        let mut latest = materialized_session_for("session-1", vec![agent_message(2, "latest")]);
        latest.applied_event_ordinal = 2;
        let mut stale = materialized_session_for("session-1", vec![agent_message(1, "stale")]);
        stale.applied_event_ordinal = 1;

        assert!(dashboard.apply_prepared_materialized_session(
            PreparedMaterializedSessionDetail::from_materialized(
                latest,
                0,
                MaterializedProjectionCache::default(),
            ),
        ));
        assert!(!dashboard.apply_prepared_materialized_session(
            PreparedMaterializedSessionDetail::from_materialized(
                stale,
                0,
                MaterializedProjectionCache::default(),
            ),
        ));

        assert_eq!(
            dashboard.session_details["session-1"]
                .last_agent_message
                .as_deref(),
            Some("latest")
        );
    }

    /// Rewrites one agent message the way the projection does: the item is
    /// copied, so every other handle in the transcript survives.
    fn set_agent_text(item: &mut Arc<TranscriptItem>, text: &str, content_ordinal: u64) {
        let item = Arc::make_mut(item);
        item.body = TranscriptBody::Agent {
            chunks: vec![serde_json::json!({
                "content": {"type": "text", "text": text}
            })],
            streaming: false,
        };
        item.latest_content_event_ordinal = Some(content_ordinal);
        item.last_changed_at_ms = i64::try_from(content_ordinal).unwrap() * 1_000;
    }

    /// The projection reuses per-item results across updates, so every shape
    /// of transcript change must land where a full rescan would.
    #[test]
    fn incremental_projection_matches_a_full_rescan_through_transcript_changes() {
        let detached_after_event_ordinal = 1;
        // One transcript, changed the way the projection changes it: items are
        // appended, and an item that changes is replaced by a copy while the
        // rest keep their handles.
        let mut transcript: Vec<Arc<TranscriptItem>> = Vec::new();
        let mut updates = vec![transcript.clone()];
        transcript.push(agent_message(1, "first"));
        transcript.push(thought(2, "thinking"));
        updates.push(transcript.clone());
        transcript.push(agent_message(3, "answer"));
        updates.push(transcript.clone());
        // More content streams into the tail message.
        set_agent_text(&mut transcript[2], "answer, at length", 4);
        updates.push(transcript.clone());
        // The tail message loses its text, so the previous answer no longer
        // holds and the earlier items have to decide it.
        set_agent_text(&mut transcript[2], "   ", 5);
        updates.push(transcript.clone());
        // An item inside the unchanged prefix changes.
        set_agent_text(&mut transcript[0], "first, corrected", 6);
        updates.push(transcript.clone());
        // A restore rebuilds every item, sharing no handles.
        transcript = vec![agent_message(1, "restored"), agent_message(2, "and again")];
        updates.push(transcript.clone());
        // A checkpoint restore leaves a shorter transcript.
        transcript.truncate(1);
        updates.push(transcript);

        let mut cache = MaterializedProjectionCache::default();
        for (index, transcript) in updates.into_iter().enumerate() {
            let session = materialized_session_for("session-1", transcript);
            let incremental = PreparedMaterializedSessionDetail::from_materialized(
                session.clone(),
                detached_after_event_ordinal,
                cache,
            );
            let rescanned = PreparedMaterializedSessionDetail::from_materialized(
                session,
                detached_after_event_ordinal,
                MaterializedProjectionCache::default(),
            );
            assert_eq!(
                incremental.last_agent_message, rescanned.last_agent_message,
                "last agent message after update {index}"
            );
            assert_eq!(
                incremental.agent_message_latest_content_ordinals,
                rescanned.agent_message_latest_content_ordinals,
                "agent ordinals after update {index}"
            );
            assert_eq!(
                incremental.unread_agent_messages, rescanned.unread_agent_messages,
                "unread count after update {index}"
            );
            cache = incremental.projection;
        }
    }

    #[test]
    fn projection_cache_keeps_terminal_diffstats_across_unrelated_updates() {
        let tool = Arc::new(TranscriptItem {
            stable_id: "tool:edit".into(),
            position: 1,
            latest_content_event_ordinal: None,
            created_at_ms: 1,
            last_changed_at_ms: 2,
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
        });
        let first = PreparedMaterializedSessionDetail::from_materialized(
            materialized_session_for("session-1", vec![tool.clone()]),
            0,
            MaterializedProjectionCache::default(),
        );
        assert_eq!(first.projection.tool_diffstats.len(), 1);

        let second = PreparedMaterializedSessionDetail::from_materialized(
            materialized_session_for(
                "session-1",
                vec![tool, agent_message(2, "unrelated update")],
            ),
            0,
            first.projection,
        );
        assert_eq!(second.projection.tool_diffstats.len(), 1);
        assert_eq!(
            second.transcript.browser_transcript(None).entries[0].lines,
            ["Edit src/lib.rs", "/workspace/src/lib.rs  +1 −0"]
        );
    }

    /// Unchanged items keep their handles, so a projection that follows one
    /// only reads the items that changed.
    #[test]
    fn projection_rereads_only_the_changed_tail() {
        let head = vec![agent_message(1, "first"), thought(2, "thinking")];
        let mut transcript = head.clone();
        transcript.push(agent_message(3, "answer"));
        let first = PreparedMaterializedSessionDetail::from_materialized(
            materialized_session_for("session-1", transcript.clone()),
            0,
            MaterializedProjectionCache::default(),
        );

        transcript.push(agent_message(4, "and more"));
        assert_eq!(
            first.projection.unchanged_prefix(&transcript),
            3,
            "appending leaves the earlier items untouched"
        );

        let mut streamed = transcript.clone();
        Arc::make_mut(&mut streamed[3]).last_changed_at_ms = 9_000;
        assert_eq!(
            first.projection.unchanged_prefix(&streamed),
            3,
            "a copy-on-write update only breaks the item it touches"
        );

        let restored = vec![agent_message(1, "first"), thought(2, "thinking")];
        assert_eq!(
            first.projection.unchanged_prefix(&restored),
            0,
            "rebuilt items share nothing, so everything is read again"
        );
    }

    #[test]
    fn later_non_agent_items_do_not_replace_the_last_agent_response() {
        let mut session = stopped_session();
        session.state = SessionState::Running;
        let mut dashboard = dashboard_with_session(session);
        apply_materialized_transcript(
            &mut dashboard,
            vec![
                agent_message(
                    1,
                    "The container lacked uv, so validation used Python 3 directly.",
                ),
                thought(2, "Checking the result"),
            ],
        );

        assert_eq!(
            dashboard.session_details["session-1"]
                .last_agent_message
                .as_deref(),
            Some("The container lacked uv, so validation used Python 3 directly.")
        );
    }

    #[test]
    fn materialized_idle_state_clears_a_stale_turn_clock() {
        let mut dashboard = dashboard_with_session(stopped_session());
        let mut running = MaterializedSession::empty("session-1");
        running.execution = MaterializedExecutionState::Running {
            started_at_ms: 1_000_000,
        };
        dashboard.apply_materialized_session(&running);
        let idle = MaterializedSession::empty("session-1");
        dashboard.apply_materialized_session(&idle);

        assert_eq!(
            dashboard.session_details["session-1"].current_turn_started_at,
            None
        );
    }

    #[test]
    fn materialized_running_state_starts_clock_without_transcript_events() {
        let mut dashboard = dashboard_with_session(stopped_session());
        let mut running = MaterializedSession::empty("session-1");
        running.execution = MaterializedExecutionState::Running {
            started_at_ms: 1_000_000,
        };
        dashboard.apply_materialized_session(&running);

        assert_eq!(
            dashboard.session_details["session-1"].current_turn_started_at,
            Some(1_000)
        );
    }

    #[test]
    fn setting_a_stage_for_an_unknown_session_is_ignored() {
        let mut dashboard = DashboardState::new(config(), HelState::default(), BTreeMap::new());
        dashboard.set_session_operation_stage("missing", ProvisionStage::Booting);
        assert!(dashboard.session_operations.is_empty());
    }

    #[test]
    fn set_resume_destination_updates_the_in_flight_operation() {
        let mut dashboard = dashboard_with_session(stopped_session());
        dashboard.begin_session_operation("session-1".into(), SessionOperationKind::Resuming, None);
        dashboard.set_resume_destination("session-1", "grok-1".into(), "raw-localhost".into());

        assert_eq!(
            dashboard.session_operations["session-1"].resume_destination,
            Some(("grok-1".to_string(), "raw-localhost".to_string()))
        );
    }

    #[test]
    fn set_resume_destination_for_an_unknown_session_is_ignored() {
        let mut dashboard = DashboardState::new(config(), HelState::default(), BTreeMap::new());
        dashboard.set_resume_destination("missing", "grok-1".into(), "raw-localhost".into());
        assert!(dashboard.session_operations.is_empty());
    }

    #[test]
    fn repeating_a_stage_report_does_not_reset_its_clock() {
        let mut dashboard = DashboardState::new(config(), HelState::default(), BTreeMap::new());
        dashboard.begin_session_operation(
            "session-1".into(),
            SessionOperationKind::Launching,
            None,
        );
        dashboard.set_session_operation_stage("session-1", ProvisionStage::Booting);
        dashboard
            .session_operations
            .get_mut("session-1")
            .expect("operation")
            .stage_started_at_epoch_seconds = Some(1_000);

        dashboard.set_session_operation_stage("session-1", ProvisionStage::Booting);

        assert_eq!(
            dashboard.session_operations["session-1"].stage_started_at_epoch_seconds,
            Some(1_000)
        );
    }
}
