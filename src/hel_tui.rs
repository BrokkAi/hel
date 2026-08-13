//! Full-screen dashboard and session picker for Hel.
//!
//! This module deliberately has no provisioning or persistence side effects.
//! Input is reduced to [`DashboardAction`] values for the controller to run.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseEvent, MouseEventKind};
use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Margin, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, BorderType, Borders, Cell, Clear, HighlightSpacing, List, ListItem, ListState,
    Paragraph, Row, Scrollbar, ScrollbarOrientation, ScrollbarState, Table, TableState, Wrap,
};
use sha2::{Digest, Sha256};

use crate::hel_chat::{TranscriptSnapshot, render_agent_message_preview};
use crate::hel_config::{HarnessKind, HelConfig, TargetTemplate};
use crate::hel_quota::ProfileQuota;
use crate::hel_state::{HelState, SessionRecord, SessionResourceAllocation, SessionState};
use crate::hel_targets::{
    AdditionalMount, DeploymentCapacityKind, DeploymentCapacityTarget, DeploymentCapacityUsage,
    SessionResourceUsage, default_mount_destination, path_completion,
};
use crate::hel_worker::{SequencedEvent, WorkerEvent, WorkerPhase};

const FORCE_CONFIRMATION: &str = "DESTROY";
const BASELINE_CPUS: u64 = 8;
const BASELINE_MEMORY_BYTES: u64 = 32 * 1024 * 1024 * 1024;
const ACTIVE_MESSAGE_LINES: usize = 4;
const SELECTED_TRANSCRIPT_LINES: usize = 10;
const SESSION_TABLE_CHROME_HEIGHT: u16 = 3;
const DASHBOARD_FIXED_HEIGHT: u16 = 3;
const DASHBOARD_PANE_COUNT: usize = 4;
const MOUSE_SCROLL_ROWS: isize = 3;
const IMPORT_STALL_WARNING_AFTER: Duration = Duration::from_secs(10);

/// A side effect requested by the dashboard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DashboardAction {
    None,
    Open {
        session_id: String,
    },
    CreateSession {
        profile_id: String,
        bundle_id: String,
        project_directory: Option<std::path::PathBuf>,
        target_template_id: String,
        additional_mounts: Vec<AdditionalMount>,
        allow_dirty_local: bool,
        resource_allocation: Option<SessionResourceAllocation>,
    },
    CompleteMountSource {
        target_template_id: String,
        prefix: String,
    },
    ValidateMountSource {
        target_template_id: String,
        source: String,
    },
    ValidateProjectDirectory {
        target_template_id: String,
        directory: String,
    },
    ResumeSession {
        session_id: String,
        profile_id: String,
        target_template_id: String,
        additional_mounts: Vec<AdditionalMount>,
        resource_allocation: Option<SessionResourceAllocation>,
    },
    ResolveAwsResourceOptions {
        target_template_ids: Vec<String>,
    },
    CreateBundle {
        source: String,
    },
    Close {
        session_id: String,
    },
    ForceDestroy {
        session_id: String,
    },
    DeleteActive {
        session_id: String,
    },
    DeleteArchived {
        session_id: String,
    },
    RenameSession {
        session_id: String,
        title: String,
    },
    RefreshQuotas,
    OpenImport,
    ImportSession {
        profile_id: String,
        native_session_id: String,
        display_title: String,
    },
    CancelImport,
    ConfirmImportBundle {
        accepted: bool,
    },
    OpenConfig,
    QuitDetach,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportSessionOption {
    pub native_session_id: String,
    pub title: String,
    pub details: String,
    pub unavailable_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportProfileOption {
    pub profile_id: String,
    pub harness_kind: HarnessKind,
    pub sessions: Vec<ImportSessionOption>,
    pub scan_progress: Option<(usize, usize)>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Focus {
    Active,
    Archived,
    Capacity,
    Quotas,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WizardStep {
    Profile,
    Target,
    Bundle,
    ProjectDirectory,
    Review,
    Mounts,
    NewBundle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WizardFocus {
    Content,
    Cancel,
    Back,
    Next,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NewWizard {
    step: WizardStep,
    focus: WizardFocus,
    profile: usize,
    bundle: usize,
    target: usize,
    mounts: MountWizard,
    review_focus: ReviewFocus,
    new_bundle_source: String,
    project_directory: String,
    project_directory_error: Option<String>,
    project_history: Vec<std::path::PathBuf>,
    project_history_index: usize,
    resource_allocation: Option<SessionResourceAllocation>,
    aws_options: BTreeMap<String, Vec<SessionResourceAllocation>>,
    sizing_error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MountFocus {
    Source,
    Destination,
    Cancel,
    Back,
    Add,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReviewFocus {
    Attachments,
    Cancel,
    Back,
    Add,
    Submit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MountWizard {
    source: String,
    destination: String,
    focus: MountFocus,
    mounts: Vec<AdditionalMount>,
    history: Vec<std::path::PathBuf>,
    history_index: usize,
    completion_cache: BTreeMap<String, Vec<String>>,
    completion_candidates: Vec<String>,
    completion_index: usize,
    error: Option<String>,
    editing_mount: Option<usize>,
}

impl MountWizard {
    fn new(history: Vec<std::path::PathBuf>) -> Self {
        Self {
            source: String::new(),
            destination: String::new(),
            focus: MountFocus::Source,
            mounts: Vec::new(),
            history,
            history_index: 0,
            completion_cache: BTreeMap::new(),
            completion_candidates: Vec::new(),
            completion_index: 0,
            error: None,
            editing_mount: None,
        }
    }

    fn with_mounts(history: Vec<std::path::PathBuf>, mounts: Vec<AdditionalMount>) -> Self {
        let mut wizard = Self::new(history);
        wizard.mounts = mounts;
        wizard
    }

    fn add_validated_mount(&mut self) {
        let mount = AdditionalMount {
            source: self.source.clone().into(),
            destination: self.destination.clone().into(),
        };
        if let Some(index) = self.editing_mount.take() {
            self.mounts[index] = mount;
        } else {
            self.mounts.push(mount);
        }
        self.source.clear();
        self.destination.clear();
        self.focus = MountFocus::Source;
        self.completion_candidates.clear();
        self.error = None;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResumeWizard {
    session_id: String,
    step: WizardStep,
    focus: WizardFocus,
    profile: usize,
    target: usize,
    mounts: MountWizard,
    review_focus: ReviewFocus,
    resource_allocation: Option<SessionResourceAllocation>,
    aws_options: BTreeMap<String, Vec<SessionResourceAllocation>>,
    sizing_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RenameEditor {
    session_id: String,
    title: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Confirmation {
    DirtyLocal {
        action: DashboardAction,
        repositories: Vec<String>,
    },
    Close {
        session_id: String,
    },
    CloseFailed {
        session_id: String,
        error: String,
    },
    ForceDestroy {
        session_id: String,
        typed: String,
    },
    DeleteActive {
        session_id: String,
        typed: String,
    },
    DeleteArchived {
        session_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Mode {
    Dashboard,
    New(NewWizard),
    Resume(ResumeWizard),
    Rename(RenameEditor),
    Import(ImportDialog),
    Importing(ImportProgress),
    ConfirmImportBundle(ImportBundleConfirmation),
    Confirm(Confirmation),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ImportProgress {
    session_title: String,
    step: usize,
    total: Option<usize>,
    message: String,
    last_updated: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ImportBundleConfirmation {
    dirty_git_roots: Vec<String>,
    omitted_non_git_dirs: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ImportFocus {
    Profiles,
    Sessions,
    Cancel,
    Import,
}

fn cycle_control<T: Copy + PartialEq>(current: T, order: &[T], reverse: bool) -> T {
    let index = order
        .iter()
        .position(|candidate| *candidate == current)
        .unwrap_or(0);
    let next = if reverse {
        index.checked_sub(1).unwrap_or(order.len() - 1)
    } else {
        (index + 1) % order.len()
    };
    order[next]
}

fn cycle_wizard_focus(current: WizardFocus, has_back: bool, reverse: bool) -> WizardFocus {
    if has_back {
        cycle_control(
            current,
            &[
                WizardFocus::Content,
                WizardFocus::Cancel,
                WizardFocus::Back,
                WizardFocus::Next,
            ],
            reverse,
        )
    } else {
        cycle_control(
            current,
            &[WizardFocus::Content, WizardFocus::Cancel, WizardFocus::Next],
            reverse,
        )
    }
}

fn review_focus_order(can_attach: bool, has_attachments: bool) -> Vec<ReviewFocus> {
    let mut order = Vec::new();
    if has_attachments {
        order.push(ReviewFocus::Attachments);
    }
    order.extend([ReviewFocus::Cancel, ReviewFocus::Back]);
    if can_attach {
        order.push(ReviewFocus::Add);
    }
    order.push(ReviewFocus::Submit);
    order
}

fn remove_selected_mount(mounts: &mut MountWizard) {
    if mounts.mounts.is_empty() {
        return;
    }
    mounts.mounts.remove(mounts.history_index);
    mounts.history_index = mounts
        .history_index
        .min(mounts.mounts.len().saturating_sub(1));
}

fn prepare_mount_editor(step: &mut WizardStep, mounts: &mut MountWizard) {
    mounts.source.clear();
    mounts.destination.clear();
    mounts.focus = MountFocus::Source;
    mounts.error = None;
    mounts.editing_mount = None;
    mounts.completion_candidates.clear();
    *step = WizardStep::Mounts;
}

fn prepare_selected_mount_editor(step: &mut WizardStep, mounts: &mut MountWizard) {
    if mounts.mounts.is_empty() {
        return;
    }
    let index = mounts.history_index;
    let mount = &mounts.mounts[index];
    mounts.source = mount.source.to_string_lossy().into_owned();
    mounts.destination = mount.destination.to_string_lossy().into_owned();
    mounts.focus = MountFocus::Source;
    mounts.error = None;
    mounts.editing_mount = Some(index);
    mounts.completion_candidates.clear();
    *step = WizardStep::Mounts;
}

fn begin_mount_editor(wizard: &mut NewWizard) {
    prepare_mount_editor(&mut wizard.step, &mut wizard.mounts);
}

fn edit_selected_mount(wizard: &mut NewWizard) {
    prepare_selected_mount_editor(&mut wizard.step, &mut wizard.mounts);
}

fn begin_resume_mount_editor(wizard: &mut ResumeWizard) {
    prepare_mount_editor(&mut wizard.step, &mut wizard.mounts);
}

fn edit_selected_resume_mount(wizard: &mut ResumeWizard) {
    prepare_selected_mount_editor(&mut wizard.step, &mut wizard.mounts);
}

fn validate_mount_entry(mounts: &MountWizard) -> Option<String> {
    let mount = AdditionalMount {
        source: mounts.source.clone().into(),
        destination: mounts.destination.clone().into(),
    };
    if let Err(error) = crate::hel_targets::validate_additional_mounts(std::slice::from_ref(&mount))
    {
        return Some(error.to_string());
    }
    let duplicate = mounts.mounts.iter().enumerate().any(|(index, existing)| {
        Some(index) != mounts.editing_mount && existing.destination == mount.destination
    });
    duplicate.then(|| {
        format!(
            "{} is already an attached directory destination.",
            mount.destination.display()
        )
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ImportDialog {
    discovery_id: u64,
    profiles: Vec<ImportProfileOption>,
    profile_index: usize,
    session_index: usize,
    focus: ImportFocus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActivityKind {
    Thinking,
    AgentText,
    ToolCall,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SessionActivity {
    kind: ActivityKind,
    text: String,
}

#[derive(Debug, Default)]
struct SessionDetail {
    last_event_sequence: u64,
    current_turn_started_at: Option<u64>,
    last_agent_text_at: Option<u64>,
    agent_text_stream_open: bool,
    last_agent_message_id: Option<String>,
    last_agent_message: Option<String>,
    unread_agent_message_sequences: Vec<u64>,
    resource_usage: Option<SessionResourceUsage>,
    transcript: Option<TranscriptSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CapacityDetail {
    target: DeploymentCapacityTarget,
    usage: Option<DeploymentCapacityUsage>,
    on_demand: bool,
}

/// Stateful, renderable projection of controller configuration and state.
pub struct DashboardState {
    config: HelConfig,
    state: HelState,
    quotas: BTreeMap<String, ProfileQuota>,
    quota_refreshing: BTreeSet<String>,
    session_details: BTreeMap<String, SessionDetail>,
    capacity_details: BTreeMap<String, CapacityDetail>,
    session_index: usize,
    capacity_index: usize,
    quota_index: usize,
    focus: Focus,
    pane_areas: Option<[Rect; DASHBOARD_PANE_COUNT]>,
    mode: Mode,
    notice: Option<String>,
    greeting: String,
}

impl DashboardState {
    pub fn new(config: HelConfig, state: HelState, quotas: BTreeMap<String, ProfileQuota>) -> Self {
        let mut dashboard = Self {
            config,
            state,
            quotas,
            quota_refreshing: BTreeSet::new(),
            session_details: BTreeMap::new(),
            capacity_details: BTreeMap::new(),
            session_index: 0,
            capacity_index: 0,
            quota_index: 0,
            focus: Focus::Active,
            pane_areas: None,
            mode: Mode::Dashboard,
            notice: None,
            greeting: "Welcome to Hel".into(),
        };
        dashboard.clamp_selections();
        dashboard
    }

    pub fn set_greeting(&mut self, greeting: String) {
        self.greeting = greeting;
    }

    pub fn set_config(&mut self, config: HelConfig) {
        self.config = config;
        self.cancel_modal();
        self.clamp_selections();
    }

    pub fn set_state(&mut self, state: HelState) {
        self.state = state;
        for (session_id, detail) in &mut self.session_details {
            let viewed_through = self
                .state
                .sessions
                .get(session_id)
                .map_or(0, |session| session.last_viewed_event_sequence);
            detail
                .unread_agent_message_sequences
                .retain(|seq| *seq > viewed_through);
        }
        self.clamp_selections();
    }

    pub fn select_active_session(&mut self, session_id: &str) {
        let (active, _) = partition_sessions(self.state.sessions.values(), &self.session_details);
        if let Some(index) = active.iter().position(|session| session.id == session_id) {
            self.focus = Focus::Active;
            self.session_index = index;
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

    /// Incorporate the newly replayed worker events for one session.
    pub fn apply_worker_events(
        &mut self,
        session_id: &str,
        events: &[SequencedEvent],
        observed_at_epoch_seconds: u64,
    ) -> bool {
        let mut updated_latest_message = false;
        let viewed_through = self
            .state
            .sessions
            .get(session_id)
            .map_or(0, |session| session.last_viewed_event_sequence);
        let detail = self
            .session_details
            .entry(session_id.to_string())
            .or_default();
        for event in events {
            if event.seq <= detail.last_event_sequence {
                continue;
            }
            detail.last_event_sequence = event.seq;
            match &event.event {
                WorkerEvent::PromptAccepted { .. } => {
                    detail.agent_text_stream_open = false;
                    detail.current_turn_started_at = Some(observed_at_epoch_seconds);
                }
                WorkerEvent::TurnCompleted => {
                    detail.agent_text_stream_open = false;
                    detail.current_turn_started_at = None;
                }
                WorkerEvent::Cancelled => detail.agent_text_stream_open = false,
                WorkerEvent::Adapter { payload, .. } => {
                    if let Some(activity) = activity_from_adapter(payload) {
                        if activity.kind == ActivityKind::AgentText {
                            let message_id = adapter_message_id(payload);
                            let continues_message = detail.agent_text_stream_open
                                && detail.last_agent_message_id == message_id;
                            if !continues_message && event.seq > viewed_through {
                                detail.unread_agent_message_sequences.push(event.seq);
                            }
                            if continues_message {
                                detail
                                    .last_agent_message
                                    .get_or_insert_default()
                                    .push_str(&activity.text);
                            } else {
                                detail.last_agent_message = Some(activity.text);
                            }
                            detail.last_agent_message_id = message_id;
                            detail.agent_text_stream_open = true;
                            detail.last_agent_text_at = Some(observed_at_epoch_seconds);
                            updated_latest_message = true;
                        } else {
                            detail.agent_text_stream_open = false;
                        }
                    }
                }
                WorkerEvent::ConfigChanged { .. }
                | WorkerEvent::Checkpointed { .. }
                | WorkerEvent::Closing
                | WorkerEvent::Closed => {
                    detail.agent_text_stream_open = false;
                }
            }
        }
        updated_latest_message
    }

    /// Apply transcript changes and reconcile transient state with the
    /// worker's authoritative snapshot-derived phase.
    pub fn apply_worker_update(
        &mut self,
        session_id: &str,
        events: &[SequencedEvent],
        phase: WorkerPhase,
        observed_at_epoch_seconds: u64,
    ) -> bool {
        let updated = self.apply_worker_events(session_id, events, observed_at_epoch_seconds);
        let detail = self
            .session_details
            .entry(session_id.to_string())
            .or_default();
        if phase == WorkerPhase::Running {
            detail
                .current_turn_started_at
                .get_or_insert(observed_at_epoch_seconds);
        } else {
            detail.current_turn_started_at = None;
        }
        updated
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

    pub fn apply_deployment_capacity(
        &mut self,
        target_id: &str,
        result: std::result::Result<Option<DeploymentCapacityUsage>, String>,
        _sampled_at_epoch_seconds: u64,
    ) {
        let Some(detail) = self.capacity_details.get_mut(target_id) else {
            return;
        };
        if let Ok(usage) = result {
            detail.on_demand = usage.is_none();
            detail.usage = usage;
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

    pub fn apply_transcript(&mut self, session_id: &str, transcript: TranscriptSnapshot) {
        self.session_details
            .entry(session_id.to_string())
            .or_default()
            .transcript = Some(transcript);
    }

    pub fn set_notice(&mut self, notice: impl Into<String>) {
        self.notice = Some(notice.into());
    }

    pub fn clear_notice(&mut self) {
        self.notice = None;
    }

    /// Show the recovery choices after a checkpointed close could not finish.
    pub fn show_close_failure(&mut self, session_id: String, error: impl Into<String>) {
        self.mode = Mode::Confirm(Confirmation::CloseFailed {
            session_id,
            error: error.into(),
        });
    }

    pub fn show_import_dialog(&mut self, discovery_id: u64, profiles: Vec<ImportProfileOption>) {
        self.mode = Mode::Import(ImportDialog {
            discovery_id,
            profiles,
            profile_index: 0,
            session_index: 0,
            focus: ImportFocus::Profiles,
        });
    }

    pub fn apply_import_profiles(&mut self, discovery_id: u64, profiles: Vec<ImportProfileOption>) {
        let Mode::Import(dialog) = &mut self.mode else {
            return;
        };
        if dialog.discovery_id != discovery_id {
            return;
        }
        let selected_profile = dialog
            .profiles
            .get(dialog.profile_index)
            .map(|profile| profile.profile_id.clone());
        let selected_session = dialog
            .profiles
            .get(dialog.profile_index)
            .and_then(|profile| profile.sessions.get(dialog.session_index))
            .map(|session| session.native_session_id.clone());
        dialog.profiles = profiles;
        dialog.profile_index = selected_profile
            .and_then(|selected| {
                dialog
                    .profiles
                    .iter()
                    .position(|profile| profile.profile_id == selected)
            })
            .unwrap_or(0);
        let sessions = dialog
            .profiles
            .get(dialog.profile_index)
            .map(|profile| profile.sessions.as_slice())
            .unwrap_or_default();
        dialog.session_index = selected_session
            .and_then(|selected| {
                sessions
                    .iter()
                    .position(|session| session.native_session_id == selected)
            })
            .unwrap_or_else(|| dialog.session_index.min(sessions.len().saturating_sub(1)));
    }

    pub fn apply_import_profile(&mut self, discovery_id: u64, profile: ImportProfileOption) {
        let Mode::Import(dialog) = &mut self.mode else {
            return;
        };
        if dialog.discovery_id != discovery_id {
            return;
        }
        let Some(profile_index) = dialog
            .profiles
            .iter()
            .position(|candidate| candidate.profile_id == profile.profile_id)
        else {
            return;
        };
        let selected_native_session_id = (dialog.profile_index == profile_index)
            .then(|| {
                dialog.profiles[profile_index]
                    .sessions
                    .get(dialog.session_index)
                    .map(|session| session.native_session_id.clone())
            })
            .flatten();
        dialog.profiles[profile_index] = profile;
        if dialog.profile_index != profile_index {
            return;
        }
        let sessions = &dialog.profiles[profile_index].sessions;
        dialog.session_index = selected_native_session_id
            .and_then(|selected| {
                sessions
                    .iter()
                    .position(|session| session.native_session_id == selected)
            })
            .unwrap_or_else(|| dialog.session_index.min(sessions.len().saturating_sub(1)));
    }

    pub fn show_import_progress(&mut self, session_title: String) {
        self.mode = Mode::Importing(ImportProgress {
            session_title,
            step: 1,
            total: None,
            message: "Locating native session…".into(),
            last_updated: Instant::now(),
        });
    }

    pub fn update_import_progress(&mut self, step: usize, total: Option<usize>, message: String) {
        let Mode::Importing(progress) = &mut self.mode else {
            return;
        };
        progress.step = step;
        progress.total = total;
        progress.message = message;
        progress.last_updated = Instant::now();
    }

    pub fn show_import_bundle_confirmation(
        &mut self,
        dirty_git_roots: Vec<String>,
        omitted_non_git_dirs: Vec<String>,
    ) {
        self.mode = Mode::ConfirmImportBundle(ImportBundleConfirmation {
            dirty_git_roots,
            omitted_non_git_dirs,
        });
    }

    pub fn show_dirty_local_confirmation(
        &mut self,
        action: DashboardAction,
        repositories: Vec<String>,
    ) {
        self.mode = Mode::Confirm(Confirmation::DirtyLocal {
            action,
            repositories,
        });
    }

    pub fn finish_import(&mut self) {
        self.cancel_modal();
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> DashboardAction {
        if key.kind != KeyEventKind::Press && key.kind != KeyEventKind::Repeat {
            return DashboardAction::None;
        }
        if is_paste_shortcut(key) {
            match crate::hel_clipboard::read_text() {
                Ok(text) => self.handle_paste(&text),
                Err(error) => self.notice = Some(format!("Paste failed: {error:#}")),
            }
            return DashboardAction::None;
        }
        if !matches!(self.mode, Mode::Dashboard)
            && dashboard_accelerator(key.modifiers)
            && key.code == KeyCode::Char('c')
        {
            return DashboardAction::QuitDetach;
        }

        self.notice = None;
        match self.mode.clone() {
            Mode::Dashboard => self.handle_dashboard_key(key),
            Mode::New(wizard) => self.handle_new_key(key.code, wizard),
            Mode::Resume(wizard) => self.handle_resume_key(key.code, wizard),
            Mode::Rename(editor) => self.handle_rename_key(key.code, editor),
            Mode::Import(dialog) => self.handle_import_key(key.code, dialog),
            Mode::Importing(_) => match key.code {
                KeyCode::Esc => DashboardAction::CancelImport,
                _ => DashboardAction::None,
            },
            Mode::ConfirmImportBundle(_) => match key.code {
                KeyCode::Char('y') | KeyCode::Enter => {
                    DashboardAction::ConfirmImportBundle { accepted: true }
                }
                KeyCode::Char('n') | KeyCode::Esc => {
                    DashboardAction::ConfirmImportBundle { accepted: false }
                }
                _ => DashboardAction::None,
            },
            Mode::Confirm(confirmation) => self.handle_confirmation_key(key.code, confirmation),
        }
    }

    pub fn handle_paste(&mut self, pasted: &str) {
        let pasted = single_line_paste(pasted);
        if pasted.is_empty() {
            return;
        }
        match &mut self.mode {
            Mode::Rename(editor) => {
                let remaining = 64_usize.saturating_sub(editor.title.chars().count());
                editor.title.extend(pasted.chars().take(remaining));
            }
            Mode::New(wizard) => match wizard.step {
                WizardStep::ProjectDirectory => {
                    wizard.project_directory.push_str(&pasted);
                    wizard.project_directory_error = None;
                }
                WizardStep::NewBundle => wizard.new_bundle_source.push_str(&pasted),
                WizardStep::Mounts => match wizard.mounts.focus {
                    MountFocus::Source => wizard.mounts.source.push_str(&pasted),
                    MountFocus::Destination => wizard.mounts.destination.push_str(&pasted),
                    _ => {}
                },
                _ => {}
            },
            Mode::Resume(wizard) if wizard.step == WizardStep::Mounts => {
                match wizard.mounts.focus {
                    MountFocus::Source => wizard.mounts.source.push_str(&pasted),
                    MountFocus::Destination => wizard.mounts.destination.push_str(&pasted),
                    _ => {}
                }
            }
            Mode::Confirm(Confirmation::ForceDestroy { typed, .. })
            | Mode::Confirm(Confirmation::DeleteActive { typed, .. }) => {
                let remaining = FORCE_CONFIRMATION.len().saturating_sub(typed.len());
                typed.extend(
                    pasted
                        .chars()
                        .filter(char::is_ascii_alphabetic)
                        .take(remaining)
                        .map(|character| character.to_ascii_uppercase()),
                );
            }
            _ => {}
        }
    }

    pub fn handle_mouse(&mut self, mouse: MouseEvent) {
        if !matches!(self.mode, Mode::Dashboard) {
            return;
        }
        let hovered = self.pane_areas.and_then(|areas| {
            areas
                .into_iter()
                .position(|area| rect_contains(area, mouse.column, mouse.row))
                .map(|index| match index {
                    0 => Focus::Active,
                    1 => Focus::Archived,
                    2 => Focus::Capacity,
                    3 => Focus::Quotas,
                    _ => unreachable!("dashboard has exactly four panes"),
                })
        });
        let Some(hovered) = hovered else {
            return;
        };
        match mouse.kind {
            MouseEventKind::ScrollUp => self.scroll_selection_for(hovered, -MOUSE_SCROLL_ROWS),
            MouseEventKind::ScrollDown => self.scroll_selection_for(hovered, MOUSE_SCROLL_ROWS),
            _ => {}
        }
    }

    fn handle_dashboard_key(&mut self, key: KeyEvent) -> DashboardAction {
        let command = dashboard_accelerator(key.modifiers);
        match (key.code, command) {
            (KeyCode::Char('q') | KeyCode::Char('c'), true) | (KeyCode::Esc, _) => {
                DashboardAction::QuitDetach
            }
            (KeyCode::Tab, _) => {
                self.cycle_focus(false);
                DashboardAction::None
            }
            (KeyCode::BackTab, _) => {
                self.cycle_focus(true);
                DashboardAction::None
            }
            (KeyCode::Up | KeyCode::Char('k'), false) => {
                self.move_selection(-1);
                DashboardAction::None
            }
            (KeyCode::Down | KeyCode::Char('j'), false) => {
                self.move_selection(1);
                DashboardAction::None
            }
            (KeyCode::Home, _) => {
                self.set_selection(0);
                DashboardAction::None
            }
            (KeyCode::End, _) => {
                let len = self.focus_len();
                self.set_selection(len.saturating_sub(1));
                DashboardAction::None
            }
            (KeyCode::Char('n'), true) => self.begin_new(),
            (KeyCode::Char('i'), true) => DashboardAction::OpenImport,
            (KeyCode::Char('r'), true) => {
                if self.focus == Focus::Quotas {
                    DashboardAction::RefreshQuotas
                } else if matches!(self.focus, Focus::Active | Focus::Archived) {
                    self.begin_rename();
                    DashboardAction::None
                } else {
                    DashboardAction::None
                }
            }
            (KeyCode::Char('u'), true) => DashboardAction::RefreshQuotas,
            (KeyCode::Char('e'), true) if self.config_is_empty() => DashboardAction::OpenConfig,
            (KeyCode::Char('p'), true) if self.focus == Focus::Active => {
                if let Some(session) = self.selected_session() {
                    self.mode = Mode::Confirm(Confirmation::Close {
                        session_id: session.id.clone(),
                    });
                }
                DashboardAction::None
            }
            (KeyCode::Char('d'), true) | (KeyCode::Delete, _) if self.focus == Focus::Archived => {
                if let Some(session) = self.selected_session() {
                    self.mode = Mode::Confirm(Confirmation::DeleteArchived {
                        session_id: session.id.clone(),
                    });
                }
                DashboardAction::None
            }
            (KeyCode::Char('d'), true) | (KeyCode::Delete, _) if self.focus == Focus::Active => {
                if let Some(session) = self.selected_session() {
                    let session_id = session.id.clone();
                    let has_assistant_messages =
                        self.session_details.get(&session_id).is_some_and(|detail| {
                            detail.last_agent_message.is_some()
                                || detail
                                    .transcript
                                    .as_ref()
                                    .is_some_and(TranscriptSnapshot::has_assistant_messages)
                        });
                    if !has_assistant_messages {
                        return DashboardAction::DeleteActive { session_id };
                    }
                    self.mode = Mode::Confirm(Confirmation::DeleteActive {
                        session_id,
                        typed: String::new(),
                    });
                }
                DashboardAction::None
            }
            (KeyCode::Enter, _) | (KeyCode::Char('o'), true) => self.open_or_resume(),
            _ => DashboardAction::None,
        }
    }

    fn handle_import_key(&mut self, code: KeyCode, mut dialog: ImportDialog) -> DashboardAction {
        match code {
            KeyCode::Esc => {
                self.cancel_modal();
                DashboardAction::None
            }
            KeyCode::Left if dialog.focus == ImportFocus::Sessions => {
                dialog.focus = ImportFocus::Profiles;
                self.mode = Mode::Import(dialog);
                DashboardAction::None
            }
            KeyCode::Right if dialog.focus == ImportFocus::Profiles => {
                dialog.focus = ImportFocus::Sessions;
                self.mode = Mode::Import(dialog);
                DashboardAction::None
            }
            KeyCode::Tab => {
                dialog.focus = cycle_control(
                    dialog.focus,
                    &[
                        ImportFocus::Profiles,
                        ImportFocus::Sessions,
                        ImportFocus::Cancel,
                        ImportFocus::Import,
                    ],
                    false,
                );
                self.mode = Mode::Import(dialog);
                DashboardAction::None
            }
            KeyCode::BackTab => {
                dialog.focus = cycle_control(
                    dialog.focus,
                    &[
                        ImportFocus::Profiles,
                        ImportFocus::Sessions,
                        ImportFocus::Cancel,
                        ImportFocus::Import,
                    ],
                    true,
                );
                self.mode = Mode::Import(dialog);
                DashboardAction::None
            }
            KeyCode::Up | KeyCode::Char('k') => {
                match dialog.focus {
                    ImportFocus::Profiles => {
                        move_index(&mut dialog.profile_index, dialog.profiles.len(), -1);
                        dialog.session_index = 0;
                    }
                    ImportFocus::Sessions => {
                        let len = dialog
                            .profiles
                            .get(dialog.profile_index)
                            .map_or(0, |profile| profile.sessions.len());
                        move_index(&mut dialog.session_index, len, -1);
                    }
                    ImportFocus::Cancel | ImportFocus::Import => {}
                }
                self.mode = Mode::Import(dialog);
                DashboardAction::None
            }
            KeyCode::Down | KeyCode::Char('j') => {
                match dialog.focus {
                    ImportFocus::Profiles => {
                        move_index(&mut dialog.profile_index, dialog.profiles.len(), 1);
                        dialog.session_index = 0;
                    }
                    ImportFocus::Sessions => {
                        let len = dialog
                            .profiles
                            .get(dialog.profile_index)
                            .map_or(0, |profile| profile.sessions.len());
                        move_index(&mut dialog.session_index, len, 1);
                    }
                    ImportFocus::Cancel | ImportFocus::Import => {}
                }
                self.mode = Mode::Import(dialog);
                DashboardAction::None
            }
            KeyCode::Enter if dialog.focus == ImportFocus::Profiles => {
                dialog.focus = ImportFocus::Sessions;
                self.mode = Mode::Import(dialog);
                DashboardAction::None
            }
            KeyCode::Enter if dialog.focus == ImportFocus::Sessions => {
                let available = dialog
                    .profiles
                    .get(dialog.profile_index)
                    .and_then(|profile| profile.sessions.get(dialog.session_index))
                    .is_some_and(|session| session.unavailable_reason.is_none());
                if available {
                    dialog.focus = ImportFocus::Import;
                }
                self.mode = Mode::Import(dialog);
                DashboardAction::None
            }
            KeyCode::Enter if dialog.focus == ImportFocus::Cancel => {
                self.cancel_modal();
                DashboardAction::None
            }
            KeyCode::Enter if dialog.focus == ImportFocus::Import => {
                let Some(profile) = dialog.profiles.get(dialog.profile_index) else {
                    self.mode = Mode::Import(dialog);
                    return DashboardAction::None;
                };
                let Some(session) = profile.sessions.get(dialog.session_index) else {
                    self.mode = Mode::Import(dialog);
                    return DashboardAction::None;
                };
                if session.unavailable_reason.is_some() {
                    self.mode = Mode::Import(dialog);
                    return DashboardAction::None;
                }
                let action = DashboardAction::ImportSession {
                    profile_id: profile.profile_id.clone(),
                    native_session_id: session.native_session_id.clone(),
                    display_title: session.title.clone(),
                };
                self.cancel_modal();
                action
            }
            _ => {
                self.mode = Mode::Import(dialog);
                DashboardAction::None
            }
        }
    }

    fn begin_rename(&mut self) {
        let Some(session) = self.selected_session() else {
            return;
        };
        self.mode = Mode::Rename(RenameEditor {
            session_id: session.id.clone(),
            title: session
                .session_title_override
                .as_ref()
                .or(session.acp_session_title.as_ref())
                .cloned()
                .unwrap_or_default(),
        });
    }

    fn handle_rename_key(&mut self, code: KeyCode, mut editor: RenameEditor) -> DashboardAction {
        match code {
            KeyCode::Esc => {
                self.cancel_modal();
                DashboardAction::None
            }
            KeyCode::Enter if editor.title.trim().is_empty() => {
                self.notice = Some("Session name cannot be empty.".into());
                self.mode = Mode::Rename(editor);
                DashboardAction::None
            }
            KeyCode::Enter => {
                self.cancel_modal();
                DashboardAction::RenameSession {
                    session_id: editor.session_id,
                    title: editor.title,
                }
            }
            KeyCode::Backspace => {
                editor.title.pop();
                self.mode = Mode::Rename(editor);
                DashboardAction::None
            }
            KeyCode::Char(character) if editor.title.chars().count() < 64 => {
                editor.title.push(character);
                self.mode = Mode::Rename(editor);
                DashboardAction::None
            }
            _ => {
                self.mode = Mode::Rename(editor);
                DashboardAction::None
            }
        }
    }

    fn handle_new_key(&mut self, code: KeyCode, mut wizard: NewWizard) -> DashboardAction {
        if code == KeyCode::Esc {
            self.cancel_modal();
            return DashboardAction::None;
        }
        if wizard.step == WizardStep::Mounts {
            return self.handle_mount_key(code, wizard);
        }
        if wizard.step == WizardStep::Review {
            return self.handle_new_review_key(code, wizard);
        }
        if wizard.step == WizardStep::ProjectDirectory {
            return match code {
                KeyCode::Up if !wizard.project_history.is_empty() => {
                    wizard.project_history_index = wizard
                        .project_history_index
                        .checked_sub(1)
                        .unwrap_or(wizard.project_history.len() - 1);
                    wizard.project_directory = wizard.project_history[wizard.project_history_index]
                        .to_string_lossy()
                        .into_owned();
                    wizard.project_directory_error = None;
                    self.mode = Mode::New(wizard);
                    DashboardAction::None
                }
                KeyCode::Down if !wizard.project_history.is_empty() => {
                    wizard.project_history_index =
                        (wizard.project_history_index + 1) % wizard.project_history.len();
                    wizard.project_directory = wizard.project_history[wizard.project_history_index]
                        .to_string_lossy()
                        .into_owned();
                    wizard.project_directory_error = None;
                    self.mode = Mode::New(wizard);
                    DashboardAction::None
                }
                KeyCode::Backspace if wizard.project_directory.is_empty() => {
                    wizard.step = WizardStep::Target;
                    self.mode = Mode::New(wizard);
                    DashboardAction::None
                }
                KeyCode::Backspace => {
                    wizard.project_directory.pop();
                    wizard.project_directory_error = None;
                    self.mode = Mode::New(wizard);
                    DashboardAction::None
                }
                KeyCode::Enter if wizard.project_directory.trim().is_empty() => {
                    wizard.project_directory_error =
                        Some("Project directory cannot be empty.".into());
                    self.mode = Mode::New(wizard);
                    DashboardAction::None
                }
                KeyCode::Enter => {
                    let path = std::path::Path::new(wizard.project_directory.trim());
                    if !path.is_absolute() {
                        wizard.project_directory_error =
                            Some("Project directory must be an absolute remote path.".into());
                        self.mode = Mode::New(wizard);
                        DashboardAction::None
                    } else if path
                        .components()
                        .any(|part| part == std::path::Component::ParentDir)
                    {
                        wizard.project_directory_error =
                            Some("Project directory must not contain '..'.".into());
                        self.mode = Mode::New(wizard);
                        DashboardAction::None
                    } else {
                        let target_template_id = nth_key(&self.config.targets, wizard.target);
                        let directory = wizard.project_directory.trim().to_owned();
                        wizard.project_directory_error = None;
                        self.mode = Mode::New(wizard);
                        DashboardAction::ValidateProjectDirectory {
                            target_template_id,
                            directory,
                        }
                    }
                }
                KeyCode::Char(character) if wizard.focus == WizardFocus::Content => {
                    wizard.project_directory.push(character);
                    wizard.project_directory_error = None;
                    self.mode = Mode::New(wizard);
                    DashboardAction::None
                }
                _ => {
                    self.mode = Mode::New(wizard);
                    DashboardAction::None
                }
            };
        }
        let has_back = wizard.step != WizardStep::Profile;
        if matches!(code, KeyCode::Tab | KeyCode::BackTab) {
            wizard.focus = cycle_wizard_focus(wizard.focus, has_back, code == KeyCode::BackTab);
            self.mode = Mode::New(wizard);
            return DashboardAction::None;
        }
        if code == KeyCode::Enter && wizard.focus == WizardFocus::Cancel {
            self.cancel_modal();
            return DashboardAction::None;
        }
        if code == KeyCode::Enter && wizard.focus == WizardFocus::Back {
            wizard.step = match wizard.step {
                WizardStep::Target => WizardStep::Profile,
                WizardStep::Bundle => WizardStep::Target,
                WizardStep::ProjectDirectory => WizardStep::Target,
                WizardStep::Review => {
                    if matches!(
                        self.config.targets[&nth_key(&self.config.targets, wizard.target)],
                        TargetTemplate::LocalBare | TargetTemplate::SshBare { .. }
                    ) {
                        WizardStep::ProjectDirectory
                    } else {
                        WizardStep::Bundle
                    }
                }
                WizardStep::NewBundle => WizardStep::Bundle,
                WizardStep::Profile => WizardStep::Profile,
                WizardStep::Mounts => unreachable!("mount input is handled above"),
            };
            wizard.focus = WizardFocus::Content;
            self.mode = Mode::New(wizard);
            return DashboardAction::None;
        }
        if wizard.step == WizardStep::NewBundle {
            return match code {
                KeyCode::Backspace if wizard.new_bundle_source.is_empty() => {
                    wizard.step = WizardStep::Bundle;
                    self.mode = Mode::New(wizard);
                    DashboardAction::None
                }
                KeyCode::Backspace => {
                    wizard.new_bundle_source.pop();
                    self.mode = Mode::New(wizard);
                    DashboardAction::None
                }
                KeyCode::Enter if wizard.new_bundle_source.trim().is_empty() => {
                    self.notice = Some("Repository source cannot be empty.".into());
                    self.mode = Mode::New(wizard);
                    DashboardAction::None
                }
                KeyCode::Enter => {
                    let source = wizard.new_bundle_source.trim().to_owned();
                    self.mode = Mode::New(wizard);
                    DashboardAction::CreateBundle { source }
                }
                KeyCode::Char(character) if wizard.focus == WizardFocus::Content => {
                    wizard.new_bundle_source.push(character);
                    self.mode = Mode::New(wizard);
                    DashboardAction::None
                }
                _ => {
                    self.mode = Mode::New(wizard);
                    DashboardAction::None
                }
            };
        }
        if wizard.step == WizardStep::Target
            && wizard.focus == WizardFocus::Content
            && matches!(
                code,
                KeyCode::Char('+')
                    | KeyCode::Char('-')
                    | KeyCode::Char('r')
                    | KeyCode::Char('c')
                    | KeyCode::Char('m')
            )
        {
            self.adjust_new_resources(&mut wizard, code);
            self.mode = Mode::New(wizard);
            return DashboardAction::None;
        }
        let len = match wizard.step {
            WizardStep::Profile => self.config.profiles.len(),
            WizardStep::Bundle => self.config.bundles.len() + 1,
            WizardStep::Target => self.config.targets.len(),
            WizardStep::ProjectDirectory => {
                unreachable!("project directory input is handled above")
            }
            WizardStep::Review => unreachable!("review input is handled above"),
            WizardStep::Mounts => unreachable!("mount input is handled before picker navigation"),
            WizardStep::NewBundle => unreachable!("bundle input is handled above"),
        };
        if wizard.focus == WizardFocus::Content && matches!(code, KeyCode::Up | KeyCode::Char('k'))
        {
            move_index(wizard.active_index_mut(), len, -1);
            let action = if wizard.step == WizardStep::Target {
                self.prepare_new_target(&mut wizard)
            } else {
                DashboardAction::None
            };
            self.mode = Mode::New(wizard);
            return action;
        }
        if wizard.focus == WizardFocus::Content
            && matches!(code, KeyCode::Down | KeyCode::Char('j'))
        {
            move_index(wizard.active_index_mut(), len, 1);
            let action = if wizard.step == WizardStep::Target {
                self.prepare_new_target(&mut wizard)
            } else {
                DashboardAction::None
            };
            self.mode = Mode::New(wizard);
            return action;
        }
        if code == KeyCode::Backspace {
            wizard.step = match wizard.step {
                WizardStep::Profile => {
                    self.cancel_modal();
                    return DashboardAction::None;
                }
                WizardStep::Target => WizardStep::Profile,
                WizardStep::Bundle => WizardStep::Target,
                WizardStep::ProjectDirectory => WizardStep::Target,
                WizardStep::Review => WizardStep::Target,
                WizardStep::Mounts => {
                    unreachable!("mount input is handled before picker navigation")
                }
                WizardStep::NewBundle => unreachable!("bundle input is handled above"),
            };
            self.mode = Mode::New(wizard);
            return DashboardAction::None;
        }
        if code != KeyCode::Enter
            || !matches!(wizard.focus, WizardFocus::Content | WizardFocus::Next)
        {
            self.mode = Mode::New(wizard);
            return DashboardAction::None;
        }

        match wizard.step {
            WizardStep::Profile => {
                wizard.step = WizardStep::Target;
                wizard.focus = WizardFocus::Content;
                let action = self.prepare_new_target(&mut wizard);
                self.mode = Mode::New(wizard);
                action
            }
            WizardStep::Bundle => {
                if wizard.bundle == self.config.bundles.len() {
                    wizard.step = WizardStep::NewBundle;
                    wizard.focus = WizardFocus::Content;
                    wizard.new_bundle_source.clear();
                    self.mode = Mode::New(wizard);
                    return DashboardAction::None;
                }
                wizard.step = WizardStep::Review;
                wizard.review_focus = ReviewFocus::Submit;
                self.mode = Mode::New(wizard);
                DashboardAction::None
            }
            WizardStep::Target => {
                let target_template_id = nth_key(&self.config.targets, wizard.target);
                let target = self
                    .config
                    .targets
                    .get(&target_template_id)
                    .expect("selected target index is present in config");
                if matches!(target, TargetTemplate::AwsEc2 { .. })
                    && wizard.resource_allocation.is_none()
                {
                    self.notice = Some(
                        wizard
                            .sizing_error
                            .clone()
                            .unwrap_or_else(|| "EC2 sizes are still loading.".into()),
                    );
                    self.mode = Mode::New(wizard);
                    return DashboardAction::None;
                }
                wizard.step = if is_bare_project_target(target) {
                    wizard.mounts = MountWizard::new(Vec::new());
                    let history_host = match target {
                        TargetTemplate::LocalBare => "local",
                        TargetTemplate::SshBare { ssh, .. } => &ssh.host,
                        _ => unreachable!(),
                    };
                    wizard.project_history = self.state.project_directories(history_host).to_vec();
                    wizard.project_history_index = 0;
                    if wizard.project_directory.is_empty()
                        && let Some(directory) = wizard.project_history.first()
                    {
                        wizard.project_directory = directory.to_string_lossy().into_owned();
                    }
                    WizardStep::ProjectDirectory
                } else {
                    wizard.mounts = MountWizard::new(
                        mount_history_host(target)
                            .and_then(|host| self.state.mount_history.get(host))
                            .cloned()
                            .unwrap_or_default(),
                    );
                    WizardStep::Bundle
                };
                self.mode = Mode::New(wizard);
                DashboardAction::None
            }
            WizardStep::Review => unreachable!("review input is handled before picker navigation"),
            WizardStep::Mounts => unreachable!("mount input is handled before picker navigation"),
            WizardStep::NewBundle => unreachable!("bundle input is handled above"),
            WizardStep::ProjectDirectory => {
                unreachable!("project directory input is handled above")
            }
        }
    }

    fn handle_new_review_key(&mut self, code: KeyCode, mut wizard: NewWizard) -> DashboardAction {
        let can_attach =
            mount_history_host(&self.config.targets[&nth_key(&self.config.targets, wizard.target)])
                .is_some();
        let order = review_focus_order(can_attach, !wizard.mounts.mounts.is_empty());
        match code {
            KeyCode::Tab | KeyCode::BackTab => {
                wizard.review_focus =
                    cycle_control(wizard.review_focus, &order, code == KeyCode::BackTab);
            }
            KeyCode::Up if wizard.review_focus == ReviewFocus::Attachments => {
                move_index(
                    &mut wizard.mounts.history_index,
                    wizard.mounts.mounts.len(),
                    -1,
                );
            }
            KeyCode::Down if wizard.review_focus == ReviewFocus::Attachments => {
                move_index(
                    &mut wizard.mounts.history_index,
                    wizard.mounts.mounts.len(),
                    1,
                );
            }
            KeyCode::Delete if wizard.review_focus == ReviewFocus::Attachments => {
                remove_selected_mount(&mut wizard.mounts);
                wizard.review_focus = if wizard.mounts.mounts.is_empty() {
                    ReviewFocus::Submit
                } else {
                    ReviewFocus::Attachments
                };
            }
            KeyCode::Enter => match wizard.review_focus {
                ReviewFocus::Attachments => edit_selected_mount(&mut wizard),
                ReviewFocus::Cancel => {
                    self.cancel_modal();
                    return DashboardAction::None;
                }
                ReviewFocus::Back => {
                    let target =
                        &self.config.targets[&nth_key(&self.config.targets, wizard.target)];
                    wizard.step = if is_bare_project_target(target) {
                        WizardStep::ProjectDirectory
                    } else {
                        WizardStep::Bundle
                    };
                    wizard.focus = WizardFocus::Content;
                }
                ReviewFocus::Add if can_attach => begin_mount_editor(&mut wizard),
                ReviewFocus::Add => {}
                ReviewFocus::Submit => return self.create_session_action(&wizard),
            },
            KeyCode::Esc => {
                self.cancel_modal();
                return DashboardAction::None;
            }
            _ => {}
        }
        self.mode = Mode::New(wizard);
        DashboardAction::None
    }

    fn handle_mount_key(&mut self, code: KeyCode, mut wizard: NewWizard) -> DashboardAction {
        let target_template_id = nth_key(&self.config.targets, wizard.target);
        match code {
            KeyCode::Tab
                if wizard.mounts.focus == MountFocus::Source
                    && !wizard.mounts.source.is_empty() =>
            {
                self.complete_new_mount_source(wizard, target_template_id)
            }
            KeyCode::Tab | KeyCode::BackTab => {
                wizard.mounts.focus = cycle_control(
                    wizard.mounts.focus,
                    &[
                        MountFocus::Source,
                        MountFocus::Destination,
                        MountFocus::Cancel,
                        MountFocus::Back,
                        MountFocus::Add,
                    ],
                    code == KeyCode::BackTab,
                );
                self.mode = Mode::New(wizard);
                DashboardAction::None
            }
            KeyCode::F(2) if wizard.mounts.focus == MountFocus::Source => {
                self.complete_new_mount_source(wizard, target_template_id)
            }
            KeyCode::Up
                if wizard.mounts.focus == MountFocus::Source
                    && !wizard.mounts.completion_candidates.is_empty() =>
            {
                move_index(
                    &mut wizard.mounts.completion_index,
                    wizard.mounts.completion_candidates.len(),
                    -1,
                );
                self.mode = Mode::New(wizard);
                DashboardAction::None
            }
            KeyCode::Down
                if wizard.mounts.focus == MountFocus::Source
                    && !wizard.mounts.completion_candidates.is_empty() =>
            {
                move_index(
                    &mut wizard.mounts.completion_index,
                    wizard.mounts.completion_candidates.len(),
                    1,
                );
                self.mode = Mode::New(wizard);
                DashboardAction::None
            }
            KeyCode::Up
                if wizard.mounts.focus == MountFocus::Source
                    && wizard.mounts.source.is_empty()
                    && !wizard.mounts.history.is_empty() =>
            {
                move_index(
                    &mut wizard.mounts.history_index,
                    wizard.mounts.history.len(),
                    -1,
                );
                wizard.mounts.source = wizard.mounts.history[wizard.mounts.history_index]
                    .to_string_lossy()
                    .into_owned();
                self.mode = Mode::New(wizard);
                DashboardAction::None
            }
            KeyCode::Down
                if wizard.mounts.focus == MountFocus::Source
                    && wizard.mounts.source.is_empty()
                    && !wizard.mounts.history.is_empty() =>
            {
                move_index(
                    &mut wizard.mounts.history_index,
                    wizard.mounts.history.len(),
                    1,
                );
                wizard.mounts.source = wizard.mounts.history[wizard.mounts.history_index]
                    .to_string_lossy()
                    .into_owned();
                self.mode = Mode::New(wizard);
                DashboardAction::None
            }
            KeyCode::Backspace => {
                match wizard.mounts.focus {
                    MountFocus::Source => {
                        wizard.mounts.source.pop();
                        wizard.mounts.completion_candidates.clear();
                    }
                    MountFocus::Destination => {
                        wizard.mounts.destination.pop();
                    }
                    MountFocus::Cancel | MountFocus::Back | MountFocus::Add => {}
                }
                wizard.mounts.error = None;
                self.mode = Mode::New(wizard);
                DashboardAction::None
            }
            KeyCode::Enter => match wizard.mounts.focus {
                MountFocus::Source if !wizard.mounts.completion_candidates.is_empty() => {
                    wizard.mounts.source =
                        wizard.mounts.completion_candidates[wizard.mounts.completion_index].clone();
                    wizard.mounts.completion_candidates.clear();
                    self.mode = Mode::New(wizard);
                    DashboardAction::None
                }
                MountFocus::Source if wizard.mounts.source.is_empty() => {
                    wizard.mounts.error =
                        Some("Choose or type a directory on the controller.".into());
                    self.mode = Mode::New(wizard);
                    DashboardAction::None
                }
                MountFocus::Source => {
                    if wizard.mounts.destination.is_empty() {
                        wizard.mounts.destination = default_resource_destination(
                            &self.config.targets[&target_template_id],
                            std::path::Path::new(&wizard.mounts.source),
                            &wizard.mounts.mounts,
                        )
                        .to_string_lossy()
                        .into_owned();
                    }
                    wizard.mounts.focus = MountFocus::Destination;
                    self.mode = Mode::New(wizard);
                    DashboardAction::None
                }
                MountFocus::Destination | MountFocus::Add => {
                    self.validate_new_mount(wizard, target_template_id)
                }
                MountFocus::Cancel => {
                    self.cancel_modal();
                    DashboardAction::None
                }
                MountFocus::Back => {
                    wizard.step = WizardStep::Review;
                    wizard.review_focus = ReviewFocus::Add;
                    self.mode = Mode::New(wizard);
                    DashboardAction::None
                }
            },
            KeyCode::Char(character) => {
                match wizard.mounts.focus {
                    MountFocus::Source => {
                        wizard.mounts.source.push(character);
                        wizard.mounts.completion_candidates.clear();
                    }
                    MountFocus::Destination => wizard.mounts.destination.push(character),
                    MountFocus::Cancel | MountFocus::Back | MountFocus::Add => {}
                }
                wizard.mounts.error = None;
                self.mode = Mode::New(wizard);
                DashboardAction::None
            }
            _ => {
                self.mode = Mode::New(wizard);
                DashboardAction::None
            }
        }
    }

    fn complete_new_mount_source(
        &mut self,
        mut wizard: NewWizard,
        target_template_id: String,
    ) -> DashboardAction {
        let prefix = wizard.mounts.source.clone();
        if prefix.is_empty() {
            self.mode = Mode::New(wizard);
            return DashboardAction::None;
        }
        if let Some(candidates) = wizard.mounts.completion_cache.get(&prefix).cloned() {
            apply_mount_completions(&mut wizard.mounts, &prefix, candidates);
            self.mode = Mode::New(wizard);
            DashboardAction::None
        } else {
            self.mode = Mode::New(wizard);
            DashboardAction::CompleteMountSource {
                target_template_id,
                prefix,
            }
        }
    }

    fn validate_new_mount(
        &mut self,
        mut wizard: NewWizard,
        target_template_id: String,
    ) -> DashboardAction {
        if let Some(error) = validate_mount_entry(&wizard.mounts) {
            wizard.mounts.error = Some(error);
            wizard.mounts.focus = MountFocus::Source;
            self.mode = Mode::New(wizard);
            return DashboardAction::None;
        }
        let source = wizard.mounts.source.clone();
        self.mode = Mode::New(wizard);
        DashboardAction::ValidateMountSource {
            target_template_id,
            source,
        }
    }

    fn create_session_action(&mut self, wizard: &NewWizard) -> DashboardAction {
        let target_template_id = nth_key(&self.config.targets, wizard.target);
        let raw_project = is_bare_project_target(&self.config.targets[&target_template_id]);
        let action = DashboardAction::CreateSession {
            profile_id: nth_key(&self.config.profiles, wizard.profile),
            bundle_id: if raw_project {
                raw_project_context_id(&wizard.project_directory)
            } else {
                nth_bundle_key(&self.config, &self.state, wizard.bundle)
            },
            project_directory: raw_project
                .then(|| std::path::PathBuf::from(wizard.project_directory.trim())),
            target_template_id,
            additional_mounts: if raw_project {
                Vec::new()
            } else {
                wizard.mounts.mounts.clone()
            },
            allow_dirty_local: false,
            resource_allocation: wizard.resource_allocation.clone(),
        };
        self.cancel_modal();
        action
    }

    pub fn apply_created_bundle(&mut self, config: HelConfig, bundle_id: &str) -> DashboardAction {
        let Mode::New(mut wizard) = self.mode.clone() else {
            return DashboardAction::None;
        };
        self.config = config;
        let Some(index) = bundle_ids_by_recent_creation(&self.config, &self.state)
            .iter()
            .position(|id| *id == bundle_id)
        else {
            self.notice = Some(format!("Created bundle {bundle_id:?} was not found."));
            return DashboardAction::None;
        };
        wizard.bundle = index;
        wizard.step = WizardStep::Review;
        self.mode = Mode::New(wizard);
        DashboardAction::None
    }

    pub fn apply_aws_resource_options(
        &mut self,
        target_id: &str,
        result: std::result::Result<Vec<SessionResourceAllocation>, String>,
    ) {
        match self.mode.clone() {
            Mode::New(mut wizard) => {
                if nth_key(&self.config.targets, wizard.target) != target_id {
                    if let Ok(options) = result {
                        wizard.aws_options.insert(target_id.to_string(), options);
                        self.mode = Mode::New(wizard);
                    }
                    return;
                }
                apply_aws_options(
                    target_id,
                    result,
                    &mut wizard.aws_options,
                    &mut wizard.resource_allocation,
                    &mut wizard.sizing_error,
                    None,
                );
                self.mode = Mode::New(wizard);
            }
            Mode::Resume(mut wizard) => {
                if nth_key(&self.config.targets, wizard.target) != target_id {
                    if let Ok(options) = result {
                        wizard.aws_options.insert(target_id.to_string(), options);
                        self.mode = Mode::Resume(wizard);
                    }
                    return;
                }
                let previous = self
                    .state
                    .sessions
                    .get(&wizard.session_id)
                    .and_then(|session| session.resource_allocation.as_ref());
                apply_aws_options(
                    target_id,
                    result,
                    &mut wizard.aws_options,
                    &mut wizard.resource_allocation,
                    &mut wizard.sizing_error,
                    previous,
                );
                self.mode = Mode::Resume(wizard);
            }
            _ => {}
        }
    }

    fn prepare_new_target(&self, wizard: &mut NewWizard) -> DashboardAction {
        self.prepare_target(
            wizard.target,
            &wizard.aws_options,
            &mut wizard.resource_allocation,
            &mut wizard.sizing_error,
            None,
        )
    }

    fn prepare_resume_target(&self, wizard: &mut ResumeWizard) -> DashboardAction {
        let previous = self
            .state
            .sessions
            .get(&wizard.session_id)
            .and_then(|session| session.resource_allocation.as_ref());
        self.prepare_target(
            wizard.target,
            &wizard.aws_options,
            &mut wizard.resource_allocation,
            &mut wizard.sizing_error,
            previous,
        )
    }

    fn prepare_target(
        &self,
        target_index: usize,
        aws_options: &BTreeMap<String, Vec<SessionResourceAllocation>>,
        allocation: &mut Option<SessionResourceAllocation>,
        sizing_error: &mut Option<String>,
        previous: Option<&SessionResourceAllocation>,
    ) -> DashboardAction {
        let target_id = nth_key(&self.config.targets, target_index);
        let target = &self.config.targets[&target_id];
        *sizing_error = None;
        match target {
            TargetTemplate::LocalBare => {
                *allocation = None;
                DashboardAction::None
            }
            TargetTemplate::LocalPodman { .. }
            | TargetTemplate::AppleContainer { .. }
            | TargetTemplate::SshPodman { .. } => {
                let limits = self.host_limits(&target_id);
                if limits.is_none() {
                    *sizing_error = Some("host totals unavailable; + disabled".into());
                }
                let (cpus, memory_bytes) = match previous {
                    Some(SessionResourceAllocation::Container { cpus, memory_bytes }) => {
                        clamp_resources(*cpus, *memory_bytes, limits)
                    }
                    _ => clamp_resources(BASELINE_CPUS, BASELINE_MEMORY_BYTES, limits),
                };
                *allocation = Some(SessionResourceAllocation::Container { cpus, memory_bytes });
                DashboardAction::None
            }
            TargetTemplate::AwsEc2 { .. } => {
                if let Some(options) = aws_options.get(&target_id) {
                    *allocation = preferred_aws_option(options, previous).cloned();
                    DashboardAction::None
                } else {
                    *allocation = None;
                    DashboardAction::ResolveAwsResourceOptions {
                        target_template_ids: vec![target_id],
                    }
                }
            }
            TargetTemplate::SshBare { .. } => {
                *allocation = None;
                DashboardAction::None
            }
        }
    }

    fn host_limits(&self, target_id: &str) -> Option<(u64, u64)> {
        self.capacity_details
            .values()
            .find(|detail| detail.target.target_ids.iter().any(|id| id == target_id))
            .and_then(|detail| detail.usage.as_ref())
            .map(|usage| (usage.logical_cores, usage.memory_total_bytes))
    }

    fn adjust_new_resources(&self, wizard: &mut NewWizard, code: KeyCode) {
        let target_id = nth_key(&self.config.targets, wizard.target);
        adjust_resources(
            &mut wizard.resource_allocation,
            wizard.aws_options.get(&target_id),
            self.host_limits(&target_id),
            code,
        );
    }

    fn adjust_resume_resources(&self, wizard: &mut ResumeWizard, code: KeyCode) {
        let target_id = nth_key(&self.config.targets, wizard.target);
        adjust_resources(
            &mut wizard.resource_allocation,
            wizard.aws_options.get(&target_id),
            self.host_limits(&target_id),
            code,
        );
    }

    /// Apply a completion response only when the source text has not changed
    /// since the request left the UI. Typed input always outranks suggestions.
    pub fn apply_mount_source_completions(&mut self, prefix: &str, candidates: Vec<String>) {
        match self.mode.clone() {
            Mode::New(mut wizard)
                if wizard.step == WizardStep::Mounts
                    && wizard.mounts.focus == MountFocus::Source
                    && wizard.mounts.source == prefix =>
            {
                apply_mount_completions(&mut wizard.mounts, prefix, candidates);
                self.mode = Mode::New(wizard);
            }
            Mode::Resume(mut wizard)
                if wizard.step == WizardStep::Mounts
                    && wizard.mounts.focus == MountFocus::Source
                    && wizard.mounts.source == prefix =>
            {
                apply_mount_completions(&mut wizard.mounts, prefix, candidates);
                self.mode = Mode::Resume(wizard);
            }
            _ => {}
        }
    }

    pub fn apply_mount_source_validation(&mut self, source: &str, result: Result<(), String>) {
        match &mut self.mode {
            Mode::New(wizard)
                if wizard.step == WizardStep::Mounts && wizard.mounts.source == source =>
            {
                match result {
                    Ok(()) => {
                        wizard.mounts.add_validated_mount();
                        wizard.mounts.history_index = wizard.mounts.mounts.len().saturating_sub(1);
                        wizard.review_focus = ReviewFocus::Attachments;
                        wizard.step = WizardStep::Review;
                    }
                    Err(error) => {
                        wizard.mounts.error = Some(error);
                        wizard.mounts.focus = MountFocus::Source;
                    }
                }
            }
            Mode::Resume(wizard)
                if wizard.step == WizardStep::Mounts && wizard.mounts.source == source =>
            {
                match result {
                    Ok(()) => {
                        wizard.mounts.add_validated_mount();
                        wizard.mounts.history_index = wizard.mounts.mounts.len().saturating_sub(1);
                        wizard.review_focus = ReviewFocus::Attachments;
                        wizard.step = WizardStep::Review;
                    }
                    Err(error) => {
                        wizard.mounts.error = Some(error);
                        wizard.mounts.focus = MountFocus::Source;
                    }
                }
            }
            _ => {}
        }
    }

    pub fn apply_project_directory_validation(
        &mut self,
        directory: &str,
        result: Result<(), String>,
    ) {
        let Mode::New(wizard) = &mut self.mode else {
            return;
        };
        if wizard.step != WizardStep::ProjectDirectory
            || wizard.project_directory.trim() != directory
        {
            return;
        }
        match result {
            Ok(()) => {
                wizard.project_directory_error = None;
                wizard.step = WizardStep::Review;
                wizard.review_focus = ReviewFocus::Submit;
            }
            Err(error) => wizard.project_directory_error = Some(error),
        }
    }

    fn handle_resume_key(&mut self, code: KeyCode, mut wizard: ResumeWizard) -> DashboardAction {
        if code == KeyCode::Esc {
            self.cancel_modal();
            return DashboardAction::None;
        }
        if wizard.step == WizardStep::Mounts {
            return self.handle_resume_mount_key(code, wizard);
        }
        if wizard.step == WizardStep::Review {
            return self.handle_resume_review_key(code, wizard);
        }
        let has_back = wizard.step != WizardStep::Profile;
        if matches!(code, KeyCode::Tab | KeyCode::BackTab) {
            wizard.focus = cycle_wizard_focus(wizard.focus, has_back, code == KeyCode::BackTab);
            self.mode = Mode::Resume(wizard);
            return DashboardAction::None;
        }
        if code == KeyCode::Enter && wizard.focus == WizardFocus::Cancel {
            self.cancel_modal();
            return DashboardAction::None;
        }
        if code == KeyCode::Enter && wizard.focus == WizardFocus::Back {
            wizard.step = match wizard.step {
                WizardStep::Target => WizardStep::Profile,
                WizardStep::Profile => WizardStep::Profile,
                WizardStep::Review => WizardStep::Target,
                WizardStep::Bundle | WizardStep::NewBundle | WizardStep::Mounts => {
                    unreachable!("invalid resume wizard step")
                }
                WizardStep::ProjectDirectory => {
                    unreachable!("resume does not select a project directory")
                }
            };
            wizard.focus = WizardFocus::Content;
            self.mode = Mode::Resume(wizard);
            return DashboardAction::None;
        }
        let profiles = self.compatible_profiles(&wizard.session_id);
        if wizard.step == WizardStep::Target
            && wizard.focus == WizardFocus::Content
            && matches!(
                code,
                KeyCode::Char('+')
                    | KeyCode::Char('-')
                    | KeyCode::Char('r')
                    | KeyCode::Char('c')
                    | KeyCode::Char('m')
            )
        {
            self.adjust_resume_resources(&mut wizard, code);
            self.mode = Mode::Resume(wizard);
            return DashboardAction::None;
        }
        let len = match wizard.step {
            WizardStep::Profile => profiles.len(),
            WizardStep::Target => self.config.targets.len(),
            WizardStep::Review => unreachable!("review input is handled above"),
            WizardStep::Bundle => unreachable!("resume does not select a bundle"),
            WizardStep::Mounts => unreachable!("mount input is handled before picker navigation"),
            WizardStep::NewBundle => unreachable!("resume does not create bundles"),
            WizardStep::ProjectDirectory => {
                unreachable!("resume does not select a project directory")
            }
        };
        if wizard.focus == WizardFocus::Content && matches!(code, KeyCode::Up | KeyCode::Char('k'))
        {
            move_index(wizard.active_index_mut(), len, -1);
            let action = if wizard.step == WizardStep::Target {
                self.prepare_resume_target(&mut wizard)
            } else {
                DashboardAction::None
            };
            self.mode = Mode::Resume(wizard);
            return action;
        }
        if wizard.focus == WizardFocus::Content
            && matches!(code, KeyCode::Down | KeyCode::Char('j'))
        {
            move_index(wizard.active_index_mut(), len, 1);
            let action = if wizard.step == WizardStep::Target {
                self.prepare_resume_target(&mut wizard)
            } else {
                DashboardAction::None
            };
            self.mode = Mode::Resume(wizard);
            return action;
        }
        if code == KeyCode::Backspace {
            match wizard.step {
                WizardStep::Profile => self.cancel_modal(),
                WizardStep::Target => {
                    wizard.step = WizardStep::Profile;
                    self.mode = Mode::Resume(wizard);
                }
                WizardStep::Review => {
                    wizard.step = WizardStep::Target;
                    self.mode = Mode::Resume(wizard);
                }
                WizardStep::Bundle => unreachable!("resume does not select a bundle"),
                WizardStep::Mounts => {
                    unreachable!("mount input is handled before picker navigation")
                }
                WizardStep::NewBundle => unreachable!("resume does not create bundles"),
                WizardStep::ProjectDirectory => {
                    unreachable!("resume does not select a project directory")
                }
            }
            return DashboardAction::None;
        }
        if code != KeyCode::Enter
            || !matches!(wizard.focus, WizardFocus::Content | WizardFocus::Next)
        {
            self.mode = Mode::Resume(wizard);
            return DashboardAction::None;
        }
        match wizard.step {
            WizardStep::Profile => {
                wizard.step = WizardStep::Target;
                wizard.focus = WizardFocus::Content;
                let action = self.prepare_resume_target(&mut wizard);
                self.mode = Mode::Resume(wizard);
                action
            }
            WizardStep::Target => {
                let target_id = nth_key(&self.config.targets, wizard.target);
                if matches!(
                    self.config.targets[&target_id],
                    TargetTemplate::AwsEc2 { .. }
                ) && wizard.resource_allocation.is_none()
                {
                    self.notice = Some(
                        wizard
                            .sizing_error
                            .clone()
                            .unwrap_or_else(|| "EC2 sizes are still loading.".into()),
                    );
                    self.mode = Mode::Resume(wizard);
                    return DashboardAction::None;
                }
                wizard.mounts.history = mount_history_host(&self.config.targets[&target_id])
                    .and_then(|host| self.state.mount_history.get(host))
                    .cloned()
                    .unwrap_or_default();
                wizard.mounts.history_index = 0;
                wizard.step = WizardStep::Review;
                wizard.review_focus = ReviewFocus::Submit;
                self.mode = Mode::Resume(wizard);
                DashboardAction::None
            }
            WizardStep::Bundle => unreachable!("resume does not select a bundle"),
            WizardStep::Review => unreachable!("review input is handled before picker navigation"),
            WizardStep::Mounts => unreachable!("mount input is handled before picker navigation"),
            WizardStep::NewBundle => unreachable!("resume does not create bundles"),
            WizardStep::ProjectDirectory => {
                unreachable!("resume does not select a project directory")
            }
        }
    }

    fn handle_resume_review_key(
        &mut self,
        code: KeyCode,
        mut wizard: ResumeWizard,
    ) -> DashboardAction {
        let can_attach =
            mount_history_host(&self.config.targets[&nth_key(&self.config.targets, wizard.target)])
                .is_some();
        let order = review_focus_order(can_attach, !wizard.mounts.mounts.is_empty());
        match code {
            KeyCode::Tab | KeyCode::BackTab => {
                wizard.review_focus =
                    cycle_control(wizard.review_focus, &order, code == KeyCode::BackTab);
            }
            KeyCode::Up if wizard.review_focus == ReviewFocus::Attachments => move_index(
                &mut wizard.mounts.history_index,
                wizard.mounts.mounts.len(),
                -1,
            ),
            KeyCode::Down if wizard.review_focus == ReviewFocus::Attachments => move_index(
                &mut wizard.mounts.history_index,
                wizard.mounts.mounts.len(),
                1,
            ),
            KeyCode::Delete if wizard.review_focus == ReviewFocus::Attachments => {
                remove_selected_mount(&mut wizard.mounts);
                wizard.review_focus = if wizard.mounts.mounts.is_empty() {
                    ReviewFocus::Submit
                } else {
                    ReviewFocus::Attachments
                };
            }
            KeyCode::Enter => match wizard.review_focus {
                ReviewFocus::Attachments => edit_selected_resume_mount(&mut wizard),
                ReviewFocus::Cancel => {
                    self.cancel_modal();
                    return DashboardAction::None;
                }
                ReviewFocus::Back => {
                    wizard.step = WizardStep::Target;
                    wizard.focus = WizardFocus::Content;
                }
                ReviewFocus::Add if can_attach => begin_resume_mount_editor(&mut wizard),
                ReviewFocus::Add => {}
                ReviewFocus::Submit => {
                    let profile_id = self
                        .compatible_profiles(&wizard.session_id)
                        .get(wizard.profile)
                        .map(|(id, _)| (*id).clone())
                        .expect("resume wizard is only opened with a compatible profile");
                    return self.resume_session_action(wizard, profile_id);
                }
            },
            KeyCode::Esc => {
                self.cancel_modal();
                return DashboardAction::None;
            }
            _ => {}
        }
        self.mode = Mode::Resume(wizard);
        DashboardAction::None
    }

    fn handle_resume_mount_key(
        &mut self,
        code: KeyCode,
        mut wizard: ResumeWizard,
    ) -> DashboardAction {
        let target_template_id = nth_key(&self.config.targets, wizard.target);
        match code {
            KeyCode::Tab
                if wizard.mounts.focus == MountFocus::Source
                    && !wizard.mounts.source.is_empty() =>
            {
                self.complete_resume_mount_source(wizard, target_template_id)
            }
            KeyCode::Tab | KeyCode::BackTab => {
                wizard.mounts.focus = cycle_control(
                    wizard.mounts.focus,
                    &[
                        MountFocus::Source,
                        MountFocus::Destination,
                        MountFocus::Cancel,
                        MountFocus::Back,
                        MountFocus::Add,
                    ],
                    code == KeyCode::BackTab,
                );
                self.mode = Mode::Resume(wizard);
                DashboardAction::None
            }
            KeyCode::F(2) if wizard.mounts.focus == MountFocus::Source => {
                self.complete_resume_mount_source(wizard, target_template_id)
            }
            KeyCode::Up
                if wizard.mounts.focus == MountFocus::Source
                    && !wizard.mounts.completion_candidates.is_empty() =>
            {
                move_index(
                    &mut wizard.mounts.completion_index,
                    wizard.mounts.completion_candidates.len(),
                    -1,
                );
                self.mode = Mode::Resume(wizard);
                DashboardAction::None
            }
            KeyCode::Down
                if wizard.mounts.focus == MountFocus::Source
                    && !wizard.mounts.completion_candidates.is_empty() =>
            {
                move_index(
                    &mut wizard.mounts.completion_index,
                    wizard.mounts.completion_candidates.len(),
                    1,
                );
                self.mode = Mode::Resume(wizard);
                DashboardAction::None
            }
            KeyCode::Up
                if wizard.mounts.focus == MountFocus::Source
                    && wizard.mounts.source.is_empty()
                    && !wizard.mounts.history.is_empty() =>
            {
                move_index(
                    &mut wizard.mounts.history_index,
                    wizard.mounts.history.len(),
                    -1,
                );
                wizard.mounts.source = wizard.mounts.history[wizard.mounts.history_index]
                    .to_string_lossy()
                    .into_owned();
                self.mode = Mode::Resume(wizard);
                DashboardAction::None
            }
            KeyCode::Down
                if wizard.mounts.focus == MountFocus::Source
                    && wizard.mounts.source.is_empty()
                    && !wizard.mounts.history.is_empty() =>
            {
                move_index(
                    &mut wizard.mounts.history_index,
                    wizard.mounts.history.len(),
                    1,
                );
                wizard.mounts.source = wizard.mounts.history[wizard.mounts.history_index]
                    .to_string_lossy()
                    .into_owned();
                self.mode = Mode::Resume(wizard);
                DashboardAction::None
            }
            KeyCode::Backspace => {
                match wizard.mounts.focus {
                    MountFocus::Source => {
                        wizard.mounts.source.pop();
                        wizard.mounts.completion_candidates.clear();
                    }
                    MountFocus::Destination => {
                        wizard.mounts.destination.pop();
                    }
                    MountFocus::Cancel | MountFocus::Back | MountFocus::Add => {}
                }
                wizard.mounts.error = None;
                self.mode = Mode::Resume(wizard);
                DashboardAction::None
            }
            KeyCode::Enter => match wizard.mounts.focus {
                MountFocus::Source if !wizard.mounts.completion_candidates.is_empty() => {
                    wizard.mounts.source =
                        wizard.mounts.completion_candidates[wizard.mounts.completion_index].clone();
                    wizard.mounts.completion_candidates.clear();
                    self.mode = Mode::Resume(wizard);
                    DashboardAction::None
                }
                MountFocus::Source if wizard.mounts.source.is_empty() => {
                    wizard.mounts.error =
                        Some("Choose or type a directory on the controller.".into());
                    self.mode = Mode::Resume(wizard);
                    DashboardAction::None
                }
                MountFocus::Source => {
                    if wizard.mounts.destination.is_empty() {
                        wizard.mounts.destination = default_resource_destination(
                            &self.config.targets[&target_template_id],
                            std::path::Path::new(&wizard.mounts.source),
                            &wizard.mounts.mounts,
                        )
                        .to_string_lossy()
                        .into_owned();
                    }
                    wizard.mounts.focus = MountFocus::Destination;
                    self.mode = Mode::Resume(wizard);
                    DashboardAction::None
                }
                MountFocus::Destination | MountFocus::Add => {
                    self.validate_resume_mount(wizard, target_template_id)
                }
                MountFocus::Cancel => {
                    self.cancel_modal();
                    DashboardAction::None
                }
                MountFocus::Back => {
                    wizard.step = WizardStep::Review;
                    wizard.review_focus = ReviewFocus::Add;
                    self.mode = Mode::Resume(wizard);
                    DashboardAction::None
                }
            },
            KeyCode::Char(character) => {
                match wizard.mounts.focus {
                    MountFocus::Source => {
                        wizard.mounts.source.push(character);
                        wizard.mounts.completion_candidates.clear();
                    }
                    MountFocus::Destination => wizard.mounts.destination.push(character),
                    MountFocus::Cancel | MountFocus::Back | MountFocus::Add => {}
                }
                wizard.mounts.error = None;
                self.mode = Mode::Resume(wizard);
                DashboardAction::None
            }
            _ => {
                self.mode = Mode::Resume(wizard);
                DashboardAction::None
            }
        }
    }

    fn complete_resume_mount_source(
        &mut self,
        mut wizard: ResumeWizard,
        target_template_id: String,
    ) -> DashboardAction {
        let prefix = wizard.mounts.source.clone();
        if prefix.is_empty() {
            self.mode = Mode::Resume(wizard);
            return DashboardAction::None;
        }
        if let Some(candidates) = wizard.mounts.completion_cache.get(&prefix).cloned() {
            apply_mount_completions(&mut wizard.mounts, &prefix, candidates);
            self.mode = Mode::Resume(wizard);
            DashboardAction::None
        } else {
            self.mode = Mode::Resume(wizard);
            DashboardAction::CompleteMountSource {
                target_template_id,
                prefix,
            }
        }
    }

    fn validate_resume_mount(
        &mut self,
        mut wizard: ResumeWizard,
        target_template_id: String,
    ) -> DashboardAction {
        if let Some(error) = validate_mount_entry(&wizard.mounts) {
            wizard.mounts.error = Some(error);
            wizard.mounts.focus = MountFocus::Source;
            self.mode = Mode::Resume(wizard);
            return DashboardAction::None;
        }
        let source = wizard.mounts.source.clone();
        self.mode = Mode::Resume(wizard);
        DashboardAction::ValidateMountSource {
            target_template_id,
            source,
        }
    }

    fn resume_session_action(
        &mut self,
        wizard: ResumeWizard,
        profile_id: String,
    ) -> DashboardAction {
        let action = DashboardAction::ResumeSession {
            session_id: wizard.session_id,
            profile_id,
            target_template_id: nth_key(&self.config.targets, wizard.target),
            additional_mounts: wizard.mounts.mounts,
            resource_allocation: wizard.resource_allocation,
        };
        self.cancel_modal();
        action
    }

    fn handle_confirmation_key(
        &mut self,
        code: KeyCode,
        confirmation: Confirmation,
    ) -> DashboardAction {
        match confirmation {
            Confirmation::DirtyLocal {
                mut action,
                repositories,
            } => match code {
                KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Enter => {
                    if let DashboardAction::CreateSession {
                        allow_dirty_local, ..
                    } = &mut action
                    {
                        *allow_dirty_local = true;
                    }
                    self.cancel_modal();
                    action
                }
                KeyCode::Esc | KeyCode::Char('n') | KeyCode::Char('N') => {
                    self.cancel_modal();
                    DashboardAction::None
                }
                _ => {
                    self.mode = Mode::Confirm(Confirmation::DirtyLocal {
                        action,
                        repositories,
                    });
                    DashboardAction::None
                }
            },
            Confirmation::Close { session_id } => match code {
                KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Enter => {
                    self.cancel_modal();
                    DashboardAction::Close { session_id }
                }
                KeyCode::Esc | KeyCode::Char('n') | KeyCode::Char('N') => {
                    self.cancel_modal();
                    DashboardAction::None
                }
                _ => DashboardAction::None,
            },
            Confirmation::DeleteArchived { session_id } => match code {
                KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Enter => {
                    self.cancel_modal();
                    DashboardAction::DeleteArchived { session_id }
                }
                KeyCode::Esc | KeyCode::Char('n') | KeyCode::Char('N') => {
                    self.cancel_modal();
                    DashboardAction::None
                }
                _ => DashboardAction::None,
            },
            Confirmation::CloseFailed { session_id, error } => match code {
                KeyCode::Char('r') | KeyCode::Char('R') => {
                    self.cancel_modal();
                    DashboardAction::Close { session_id }
                }
                KeyCode::Char('f') | KeyCode::Char('F') => {
                    self.mode = Mode::Confirm(Confirmation::ForceDestroy {
                        session_id,
                        typed: String::new(),
                    });
                    DashboardAction::None
                }
                KeyCode::Esc | KeyCode::Char('c') | KeyCode::Char('C') => {
                    self.cancel_modal();
                    DashboardAction::None
                }
                _ => {
                    self.mode = Mode::Confirm(Confirmation::CloseFailed { session_id, error });
                    DashboardAction::None
                }
            },
            Confirmation::ForceDestroy {
                session_id,
                mut typed,
            } => match code {
                KeyCode::Esc => {
                    self.cancel_modal();
                    DashboardAction::None
                }
                KeyCode::Backspace => {
                    typed.pop();
                    self.mode = Mode::Confirm(Confirmation::ForceDestroy { session_id, typed });
                    DashboardAction::None
                }
                KeyCode::Char(c) => {
                    if typed.len() < FORCE_CONFIRMATION.len() {
                        typed.push(c.to_ascii_uppercase());
                    }
                    self.mode = Mode::Confirm(Confirmation::ForceDestroy { session_id, typed });
                    DashboardAction::None
                }
                KeyCode::Enter if typed == FORCE_CONFIRMATION => {
                    self.cancel_modal();
                    DashboardAction::ForceDestroy { session_id }
                }
                _ => {
                    self.mode = Mode::Confirm(Confirmation::ForceDestroy { session_id, typed });
                    DashboardAction::None
                }
            },
            Confirmation::DeleteActive {
                session_id,
                mut typed,
            } => match code {
                KeyCode::Esc => {
                    self.cancel_modal();
                    DashboardAction::None
                }
                KeyCode::Backspace => {
                    typed.pop();
                    self.mode = Mode::Confirm(Confirmation::DeleteActive { session_id, typed });
                    DashboardAction::None
                }
                KeyCode::Char(c) => {
                    if typed.len() < FORCE_CONFIRMATION.len() {
                        typed.push(c.to_ascii_uppercase());
                    }
                    self.mode = Mode::Confirm(Confirmation::DeleteActive { session_id, typed });
                    DashboardAction::None
                }
                KeyCode::Enter if typed == FORCE_CONFIRMATION => {
                    self.cancel_modal();
                    DashboardAction::DeleteActive { session_id }
                }
                _ => {
                    self.mode = Mode::Confirm(Confirmation::DeleteActive { session_id, typed });
                    DashboardAction::None
                }
            },
        }
    }

    fn begin_new(&mut self) -> DashboardAction {
        if self.config.profiles.is_empty() || self.config.targets.is_empty() {
            self.notice = Some("Configure at least one profile and target first.".into());
            return DashboardAction::None;
        }
        let recent = most_recent_configured_session(&self.config, &self.state);
        let profile = recent
            .and_then(|session| {
                self.config
                    .profiles
                    .keys()
                    .position(|id| id == &session.last_profile)
            })
            .unwrap_or(0);
        let bundle = recent
            .and_then(|session| {
                bundle_ids_by_recent_creation(&self.config, &self.state)
                    .iter()
                    .position(|id| *id == session.bundle_id)
            })
            .unwrap_or(0);
        let target = recent
            .and_then(|session| {
                self.config
                    .targets
                    .keys()
                    .position(|id| id == &session.target_template_id)
            })
            .unwrap_or(0);
        self.mode = Mode::New(NewWizard {
            step: WizardStep::Profile,
            focus: WizardFocus::Content,
            profile,
            bundle,
            target,
            mounts: MountWizard::new(Vec::new()),
            review_focus: ReviewFocus::Submit,
            new_bundle_source: String::new(),
            project_directory: String::new(),
            project_directory_error: None,
            project_history: Vec::new(),
            project_history_index: 0,
            resource_allocation: None,
            aws_options: BTreeMap::new(),
            sizing_error: None,
        });
        self.resolve_all_aws_resource_options_action()
    }

    fn begin_resume(&mut self) -> DashboardAction {
        let Some(session) = self.selected_session() else {
            return DashboardAction::None;
        };
        if session.state.is_active() && session.state != SessionState::Error {
            self.notice = Some("This session is active; press Enter to open it.".into());
            return DashboardAction::None;
        }
        if session.checkpoint.is_none() {
            self.notice = Some("This session has no verified recovery copy to resume.".into());
            return DashboardAction::None;
        }
        if self.compatible_profiles(&session.id).is_empty() || self.config.targets.is_empty() {
            self.notice = Some("Resume needs a profile and a target template.".into());
            return DashboardAction::None;
        }
        let profile = self
            .compatible_profiles(&session.id)
            .iter()
            .position(|(profile_id, _)| profile_id.as_str() == session.last_profile)
            .unwrap_or(0);
        let target = self
            .config
            .targets
            .keys()
            .position(|target_id| target_id == &session.target_template_id)
            .unwrap_or(0);
        self.mode = Mode::Resume(ResumeWizard {
            session_id: session.id.clone(),
            step: WizardStep::Profile,
            focus: WizardFocus::Content,
            profile,
            target,
            mounts: MountWizard::with_mounts(Vec::new(), session.additional_mounts.clone()),
            review_focus: ReviewFocus::Submit,
            resource_allocation: None,
            aws_options: BTreeMap::new(),
            sizing_error: None,
        });
        self.resolve_all_aws_resource_options_action()
    }

    fn resolve_all_aws_resource_options_action(&self) -> DashboardAction {
        let target_template_ids = self
            .config
            .targets
            .iter()
            .filter_map(|(id, target)| {
                matches!(target, TargetTemplate::AwsEc2 { .. }).then_some(id.clone())
            })
            .collect::<Vec<_>>();
        if target_template_ids.is_empty() {
            DashboardAction::None
        } else {
            DashboardAction::ResolveAwsResourceOptions {
                target_template_ids,
            }
        }
    }

    fn open_or_resume(&mut self) -> DashboardAction {
        let Some(session) = self.selected_session() else {
            return DashboardAction::None;
        };
        if session.state == SessionState::Error {
            if session.checkpoint.is_some() {
                return self.begin_resume();
            } else {
                self.notice = Some(
                    session
                        .last_error
                        .clone()
                        .unwrap_or_else(|| "Session failed without a recorded error.".into()),
                );
            }
            return DashboardAction::None;
        }
        if session.state.is_active() {
            DashboardAction::Open {
                session_id: session.id.clone(),
            }
        } else {
            self.begin_resume()
        }
    }

    fn selected_session(&self) -> Option<&SessionRecord> {
        let session = self.ordered_sessions().get(self.session_index).copied()?;
        match self.focus {
            Focus::Active if session.state.is_active() => Some(session),
            Focus::Archived if !session.state.is_active() => Some(session),
            Focus::Active | Focus::Archived | Focus::Capacity | Focus::Quotas => None,
        }
    }

    fn ordered_sessions(&self) -> Vec<&SessionRecord> {
        let (active, archived) =
            partition_sessions(self.state.sessions.values(), &self.session_details);
        active.into_iter().chain(archived).collect()
    }

    fn compatible_profiles(&self, session_id: &str) -> Vec<(&String, HarnessKind)> {
        if !self.state.sessions.contains_key(session_id) {
            return Vec::new();
        }
        self.config
            .profiles
            .iter()
            .map(|(id, profile)| (id, profile.kind))
            .collect()
    }

    fn profile_choice(&self, id: &str, harness: HarnessKind) -> String {
        let quota = if self.quota_refreshing.contains(id) {
            "refreshing".to_string()
        } else {
            self.quotas
                .get(id)
                .map(ProfileQuota::compact)
                .unwrap_or_else(|| "refreshing".to_string())
        };
        let danger = if harness == HarnessKind::Kimi {
            "  ⚠ DANGER: auto mode allows commands without approval"
        } else {
            ""
        };
        format!("{id}  {}  ·  {quota}{danger}", harness_label(harness))
    }

    fn config_is_empty(&self) -> bool {
        self.config.profiles.is_empty() || self.config.targets.is_empty()
    }

    fn cancel_modal(&mut self) {
        self.mode = Mode::Dashboard;
    }

    fn focus_len(&self) -> usize {
        self.focus_len_for(self.focus)
    }

    fn focus_len_for(&self, focus: Focus) -> usize {
        let (active, archived) =
            partition_sessions(self.state.sessions.values(), &self.session_details);
        match focus {
            Focus::Active => active.len(),
            Focus::Archived => archived.len(),
            Focus::Capacity => self.capacity_details.len(),
            Focus::Quotas => self.config.profiles.len(),
        }
    }

    fn set_selection(&mut self, index: usize) {
        self.set_selection_for(self.focus, index);
    }

    fn set_selection_for(&mut self, focus: Focus, index: usize) {
        let active_len = partition_sessions(self.state.sessions.values(), &self.session_details)
            .0
            .len();
        match focus {
            Focus::Active => self.session_index = index,
            Focus::Archived => self.session_index = active_len + index,
            Focus::Capacity => self.capacity_index = index,
            Focus::Quotas => self.quota_index = index,
        }
    }

    fn move_selection(&mut self, delta: isize) {
        let len = self.focus_len();
        match self.focus {
            Focus::Active => move_index(&mut self.session_index, len, delta),
            Focus::Archived => {
                let active_len =
                    partition_sessions(self.state.sessions.values(), &self.session_details)
                        .0
                        .len();
                let mut index = self.session_index.saturating_sub(active_len);
                move_index(&mut index, len, delta);
                self.session_index = active_len + index;
            }
            Focus::Capacity => move_index(&mut self.capacity_index, len, delta),
            Focus::Quotas => move_index(&mut self.quota_index, len, delta),
        }
    }

    fn scroll_selection_for(&mut self, focus: Focus, delta: isize) {
        let len = self.focus_len_for(focus);
        if len == 0 {
            self.set_selection_for(focus, 0);
            return;
        }
        let active_len = partition_sessions(self.state.sessions.values(), &self.session_details)
            .0
            .len();
        let current = match focus {
            Focus::Active => self.session_index,
            Focus::Archived => self.session_index.saturating_sub(active_len),
            Focus::Capacity => self.capacity_index,
            Focus::Quotas => self.quota_index,
        };
        let next = if delta.is_negative() {
            current.saturating_sub(delta.unsigned_abs())
        } else {
            current
                .saturating_add(delta as usize)
                .min(len.saturating_sub(1))
        };
        self.set_selection_for(focus, next);
    }

    fn cycle_focus(&mut self, reverse: bool) {
        self.focus = match (self.focus, reverse) {
            (Focus::Active, false) | (Focus::Capacity, true) => Focus::Archived,
            (Focus::Archived, false) | (Focus::Quotas, true) => Focus::Capacity,
            (Focus::Capacity, false) | (Focus::Active, true) => Focus::Quotas,
            (Focus::Quotas, false) | (Focus::Archived, true) => Focus::Active,
        };
        let active_len = partition_sessions(self.state.sessions.values(), &self.session_details)
            .0
            .len();
        match self.focus {
            Focus::Active => {
                self.session_index = self.session_index.min(active_len.saturating_sub(1));
            }
            Focus::Archived => self.session_index = self.session_index.max(active_len),
            Focus::Capacity => {}
            Focus::Quotas => {}
        }
    }

    fn clamp_selections(&mut self) {
        let (active, archived) =
            partition_sessions(self.state.sessions.values(), &self.session_details);
        if self.focus == Focus::Active && active.is_empty() && !archived.is_empty() {
            self.focus = Focus::Archived;
        } else if self.focus == Focus::Archived && archived.is_empty() && !active.is_empty() {
            self.focus = Focus::Active;
        }
        self.session_index = match self.focus {
            Focus::Active => self.session_index.min(active.len().saturating_sub(1)),
            Focus::Archived => {
                active.len()
                    + self
                        .session_index
                        .saturating_sub(active.len())
                        .min(archived.len().saturating_sub(1))
            }
            Focus::Capacity => self
                .session_index
                .min(self.state.sessions.len().saturating_sub(1)),
            Focus::Quotas => self
                .session_index
                .min(self.state.sessions.len().saturating_sub(1)),
        };
        self.quota_index = self
            .quota_index
            .min(self.config.profiles.len().saturating_sub(1));
        self.capacity_index = self
            .capacity_index
            .min(self.capacity_details.len().saturating_sub(1));
    }
}

fn partition_sessions<'a>(
    sessions: impl IntoIterator<Item = &'a SessionRecord>,
    session_details: &BTreeMap<String, SessionDetail>,
) -> (Vec<&'a SessionRecord>, Vec<&'a SessionRecord>) {
    let mut active = Vec::new();
    let mut archived = Vec::new();
    for session in sessions {
        if session.state.is_active() {
            active.push(session);
        } else {
            archived.push(session);
        }
    }
    active.sort_by(|left, right| {
        let last_agent_text_at = |session: &SessionRecord| {
            session_details
                .get(&session.id)
                .and_then(|detail| detail.last_agent_text_at)
        };
        last_agent_text_at(right)
            .cmp(&last_agent_text_at(left))
            .then_with(|| left.id.cmp(&right.id))
    });
    archived.sort_by(|left, right| {
        checkpoint_time(right)
            .cmp(&checkpoint_time(left))
            .then_with(|| left.id.cmp(&right.id))
    });
    (active, archived)
}

fn clamp_resources(cpus: u64, memory_bytes: u64, limits: Option<(u64, u64)>) -> (u64, u64) {
    let Some((max_cpus, max_memory)) = limits else {
        return (cpus.max(1), memory_bytes.max(1));
    };
    (
        cpus.min(max_cpus.max(1)),
        memory_bytes.min(max_memory.max(1)),
    )
}

fn preferred_aws_option<'a>(
    options: &'a [SessionResourceAllocation],
    previous: Option<&SessionResourceAllocation>,
) -> Option<&'a SessionResourceAllocation> {
    if let Some(SessionResourceAllocation::AwsEc2 { instance_type, .. }) = previous
        && let Some(option) = options.iter().find(|option| {
            matches!(option, SessionResourceAllocation::AwsEc2 { instance_type: candidate, .. } if candidate == instance_type)
        })
    {
        return Some(option);
    }
    options.iter().find(|option| allocation_cpus(option) == 8)
}

fn apply_aws_options(
    target_id: &str,
    result: std::result::Result<Vec<SessionResourceAllocation>, String>,
    options_by_target: &mut BTreeMap<String, Vec<SessionResourceAllocation>>,
    allocation: &mut Option<SessionResourceAllocation>,
    sizing_error: &mut Option<String>,
    previous: Option<&SessionResourceAllocation>,
) {
    match result {
        Ok(options) => {
            *allocation = preferred_aws_option(&options, previous).cloned();
            options_by_target.insert(target_id.to_owned(), options);
            *sizing_error = None;
        }
        Err(error) => {
            *allocation = None;
            *sizing_error = Some(error);
        }
    }
}

fn allocation_cpus(allocation: &SessionResourceAllocation) -> u64 {
    match allocation {
        SessionResourceAllocation::Container { cpus, .. } => *cpus,
        SessionResourceAllocation::AwsEc2 { vcpus, .. } => *vcpus,
    }
}

fn allocation_memory(allocation: &SessionResourceAllocation) -> u64 {
    match allocation {
        SessionResourceAllocation::Container { memory_bytes, .. }
        | SessionResourceAllocation::AwsEc2 { memory_bytes, .. } => *memory_bytes,
    }
}

fn adjust_resources(
    allocation: &mut Option<SessionResourceAllocation>,
    aws_options: Option<&Vec<SessionResourceAllocation>>,
    limits: Option<(u64, u64)>,
    code: KeyCode,
) {
    let Some(current) = allocation.clone() else {
        return;
    };
    match current {
        SessionResourceAllocation::Container { cpus, memory_bytes } => {
            let next = match code {
                KeyCode::Char('r') => clamp_resources(BASELINE_CPUS, BASELINE_MEMORY_BYTES, limits),
                KeyCode::Char('+') => {
                    let Some((max_cpus, max_memory)) = limits else {
                        return;
                    };
                    (
                        cpus.saturating_mul(2).min(max_cpus.max(1)),
                        memory_bytes.saturating_mul(2).min(max_memory.max(1)),
                    )
                }
                KeyCode::Char('c') => {
                    let Some((max_cpus, _)) = limits else {
                        return;
                    };
                    (cpus.saturating_mul(2).min(max_cpus.max(1)), memory_bytes)
                }
                KeyCode::Char('m') => {
                    let Some((_, max_memory)) = limits else {
                        return;
                    };
                    (cpus, memory_bytes.saturating_mul(2).min(max_memory.max(1)))
                }
                KeyCode::Char('-') if cpus > 1 => (cpus / 2, (memory_bytes / 2).max(1)),
                _ => return,
            };
            *allocation = Some(SessionResourceAllocation::Container {
                cpus: next.0,
                memory_bytes: next.1,
            });
        }
        SessionResourceAllocation::AwsEc2 {
            vcpus,
            memory_bytes,
            ..
        } => {
            let Some(options) = aws_options else {
                return;
            };
            let desired = match code {
                KeyCode::Char('+') => (Some(vcpus.saturating_mul(2)), None),
                KeyCode::Char('-') if vcpus > 1 => (Some(vcpus / 2), None),
                KeyCode::Char('r') => (Some(BASELINE_CPUS), None),
                KeyCode::Char('c') => (Some(vcpus.saturating_mul(2)), Some(memory_bytes)),
                KeyCode::Char('m') => (Some(vcpus), Some(memory_bytes.saturating_mul(2))),
                _ => return,
            };
            if let Some(next) = options.iter().find(|option| {
                desired.0.is_none_or(|cpus| allocation_cpus(option) == cpus)
                    && desired
                        .1
                        .is_none_or(|memory| allocation_memory(option) == memory)
            }) {
                *allocation = Some(next.clone());
            }
        }
    }
}

fn checkpoint_time(session: &SessionRecord) -> Option<chrono::DateTime<chrono::FixedOffset>> {
    session
        .checkpoint
        .as_ref()
        .and_then(|checkpoint| chrono::DateTime::parse_from_rfc3339(&checkpoint.created_at).ok())
}

fn activity_from_adapter(payload: &serde_json::Value) -> Option<SessionActivity> {
    let update = payload.get("update").filter(|_| {
        payload.get("type").and_then(serde_json::Value::as_str) == Some("session_update")
    })?;
    let kind = update
        .get("sessionUpdate")
        .and_then(serde_json::Value::as_str)?;
    let text = || {
        update
            .get("content")
            .and_then(|content| content.get("text"))
            .and_then(serde_json::Value::as_str)
            .filter(|text| !text.trim().is_empty())
    };
    match kind {
        "agent_thought_chunk" => text().map(|text| SessionActivity {
            kind: ActivityKind::Thinking,
            text: text.to_string(),
        }),
        "agent_message_chunk" => text().map(|text| SessionActivity {
            kind: ActivityKind::AgentText,
            text: text.to_string(),
        }),
        "tool_call" => update
            .get("title")
            .and_then(serde_json::Value::as_str)
            .filter(|title| !title.trim().is_empty())
            .map(|text| SessionActivity {
                kind: ActivityKind::ToolCall,
                text: text.to_string(),
            }),
        _ => None,
    }
}

fn adapter_message_id(payload: &serde_json::Value) -> Option<String> {
    payload
        .get("update")
        .and_then(|update| update.get("messageId"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}

fn truncate_text(text: &str, width: usize) -> String {
    let text = collapse_whitespace(text);
    if text.chars().count() <= width {
        return text;
    }
    if width <= 1 {
        return "…".chars().take(width).collect();
    }
    let mut truncated = text.chars().take(width - 1).collect::<String>();
    truncated.push('…');
    truncated
}

fn collapse_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

impl NewWizard {
    fn active_index_mut(&mut self) -> &mut usize {
        match self.step {
            WizardStep::Profile => &mut self.profile,
            WizardStep::Bundle => &mut self.bundle,
            WizardStep::Target => &mut self.target,
            WizardStep::ProjectDirectory => unreachable!("project directory has no picker index"),
            WizardStep::Review => unreachable!("review input has no picker index"),
            WizardStep::Mounts => unreachable!("mount input has no picker index"),
            WizardStep::NewBundle => unreachable!("bundle input has no picker index"),
        }
    }
}

impl ResumeWizard {
    fn active_index_mut(&mut self) -> &mut usize {
        match self.step {
            WizardStep::Profile => &mut self.profile,
            WizardStep::Target => &mut self.target,
            WizardStep::Review => unreachable!("review input has no picker index"),
            WizardStep::Bundle => unreachable!("resume does not select a bundle"),
            WizardStep::Mounts => unreachable!("resume does not select mounts"),
            WizardStep::NewBundle => unreachable!("resume does not create bundles"),
            WizardStep::ProjectDirectory => {
                unreachable!("resume does not select a project directory")
            }
        }
    }
}

pub fn render(frame: &mut Frame, dashboard: &mut DashboardState) {
    dashboard.pane_areas = None;
    let area = frame.area();

    if !dashboard.config_is_empty() {
        render_adaptive_dashboard(frame, area, area, dashboard);
        return;
    }

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(8),
            Constraint::Length(5),
            Constraint::Length(8),
            Constraint::Length(2),
        ])
        .split(area);
    render_dashboard_title(frame, layout[0], &dashboard.greeting);

    render_onboarding(frame, layout[1], dashboard);
    render_capacity(frame, layout[2], dashboard);
    render_quotas(frame, layout[3], dashboard);
    render_footer(frame, layout[4], dashboard);

    match &dashboard.mode {
        Mode::New(wizard) => render_new_wizard(frame, area, dashboard, wizard),
        Mode::Resume(wizard) => render_resume_wizard(frame, area, dashboard, wizard),
        Mode::Rename(editor) => render_rename_editor(frame, area, editor),
        Mode::Import(dialog) => render_import_dialog(frame, area, dialog),
        Mode::Importing(progress) => render_import_progress(frame, area, progress),
        Mode::ConfirmImportBundle(confirmation) => {
            render_import_bundle_confirmation(frame, area, confirmation)
        }
        Mode::Confirm(confirmation) => render_confirmation(frame, area, confirmation),
        Mode::Dashboard => {}
    }
}

fn render_dashboard_title(frame: &mut Frame, area: Rect, greeting: &str) {
    frame.render_widget(
        Paragraph::new(Span::styled(
            greeting,
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ))
        .alignment(Alignment::Center),
        area,
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PaneHeights {
    active: u16,
    archived: u16,
    capacity: u16,
    quotas: u16,
}

impl PaneHeights {
    fn as_array(self) -> [u16; DASHBOARD_PANE_COUNT] {
        [self.active, self.archived, self.capacity, self.quotas]
    }

    fn from_array(heights: [u16; DASHBOARD_PANE_COUNT]) -> Self {
        Self {
            active: heights[0],
            archived: heights[1],
            capacity: heights[2],
            quotas: heights[3],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PaneAllocation {
    Fits(PaneHeights),
    TooSmall { required_frame_height: u16 },
}

fn allocate_pane_heights(
    frame_height: u16,
    full: PaneHeights,
    minimized: PaneHeights,
    focus: Focus,
) -> PaneAllocation {
    let pane_space = frame_height.saturating_sub(DASHBOARD_FIXED_HEIGHT);
    let full = full.as_array();
    let minimized = minimized.as_array();
    let minimum_total = minimized.iter().copied().fold(0_u16, u16::saturating_add);
    if pane_space < minimum_total {
        return PaneAllocation::TooSmall {
            required_frame_height: DASHBOARD_FIXED_HEIGHT.saturating_add(minimum_total),
        };
    }

    let full_total = full.iter().copied().fold(0_u16, u16::saturating_add);
    let mut allocated = if full_total <= pane_space {
        full
    } else {
        minimized
    };
    let allocated_total = allocated.iter().copied().fold(0_u16, u16::saturating_add);
    let mut remaining = pane_space.saturating_sub(allocated_total);
    if full_total > pane_space {
        let focused = match focus {
            Focus::Active => 0,
            Focus::Archived => 1,
            Focus::Capacity => 2,
            Focus::Quotas => 3,
        };
        let growth = remaining.min(full[focused].saturating_sub(allocated[focused]));
        allocated[focused] = allocated[focused].saturating_add(growth);
        remaining = remaining.saturating_sub(growth);
    }
    allocated[0] = allocated[0].saturating_add(remaining);
    PaneAllocation::Fits(PaneHeights::from_array(allocated))
}

fn render_adaptive_dashboard(
    frame: &mut Frame,
    frame_area: Rect,
    inner: Rect,
    dashboard: &mut DashboardState,
) {
    let preview_width = inner.width.saturating_sub(4);
    let full_active_previews =
        prepare_active_previews(dashboard, preview_width, SELECTED_TRANSCRIPT_LINES);
    let (active_count, archived_count) = {
        let (active, archived) = partition_sessions(
            dashboard.state.sessions.values(),
            &dashboard.session_details,
        );
        (active.len(), archived.len())
    };
    let active_row_heights = full_active_previews
        .iter()
        .map(|preview| preview.len() as u16 + 1)
        .collect::<Vec<_>>();
    let full = PaneHeights {
        active: active_pane_height(&active_row_heights, active_count),
        archived: plain_table_height(archived_count),
        capacity: plain_table_height(dashboard.capacity_details.len()),
        quotas: plain_table_height(dashboard.config.profiles.len()),
    };
    let minimized = PaneHeights {
        active: if dashboard.focus == Focus::Active {
            SESSION_TABLE_CHROME_HEIGHT
        } else {
            active_pane_height(&active_row_heights, active_count.min(2))
        },
        archived: focused_or_minimized_table_height(
            dashboard.focus == Focus::Archived,
            archived_count,
        ),
        capacity: focused_or_minimized_table_height(
            dashboard.focus == Focus::Capacity,
            dashboard.capacity_details.len(),
        ),
        quotas: focused_or_minimized_table_height(
            dashboard.focus == Focus::Quotas,
            dashboard.config.profiles.len(),
        ),
    };
    let allocation = allocate_pane_heights(frame_area.height, full, minimized, dashboard.focus);
    let PaneAllocation::Fits(heights) = allocation else {
        let PaneAllocation::TooSmall {
            required_frame_height,
        } = allocation
        else {
            unreachable!()
        };
        render_terminal_too_small(frame, frame_area, required_frame_height);
        return;
    };

    let fixed = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(0),
            Constraint::Length(2),
        ])
        .split(inner);
    render_dashboard_title(frame, fixed[0], &dashboard.greeting);
    let panes = Layout::default()
        .direction(Direction::Vertical)
        .constraints(
            heights
                .as_array()
                .into_iter()
                .map(Constraint::Length)
                .collect::<Vec<_>>(),
        )
        .split(fixed[1]);
    dashboard.pane_areas = Some([panes[0], panes[1], panes[2], panes[3]]);
    let selected_lines = usize::from(
        panes[0]
            .height
            .saturating_sub(SESSION_TABLE_CHROME_HEIGHT + 1),
    )
    .min(SELECTED_TRANSCRIPT_LINES);
    let active_previews = prepare_active_previews(dashboard, preview_width, selected_lines);
    render_sessions(frame, panes[0], panes[1], dashboard, &active_previews);
    render_capacity(frame, panes[2], dashboard);
    render_quotas(frame, panes[3], dashboard);
    render_footer(frame, fixed[2], dashboard);

    match &dashboard.mode {
        Mode::New(wizard) => render_new_wizard(frame, frame_area, dashboard, wizard),
        Mode::Resume(wizard) => render_resume_wizard(frame, frame_area, dashboard, wizard),
        Mode::Rename(editor) => render_rename_editor(frame, frame_area, editor),
        Mode::Import(dialog) => render_import_dialog(frame, frame_area, dialog),
        Mode::Importing(progress) => render_import_progress(frame, frame_area, progress),
        Mode::ConfirmImportBundle(confirmation) => {
            render_import_bundle_confirmation(frame, frame_area, confirmation)
        }
        Mode::Confirm(confirmation) => render_confirmation(frame, frame_area, confirmation),
        Mode::Dashboard => {}
    }
}

fn plain_table_height(rows: usize) -> u16 {
    SESSION_TABLE_CHROME_HEIGHT.saturating_add(rows.min(u16::MAX as usize) as u16)
}

fn rect_contains(area: Rect, column: u16, row: u16) -> bool {
    column >= area.x && column < area.right() && row >= area.y && row < area.bottom()
}

fn focused_or_minimized_table_height(focused: bool, rows: usize) -> u16 {
    if focused {
        SESSION_TABLE_CHROME_HEIGHT
    } else {
        plain_table_height(rows.min(2))
    }
}

fn active_pane_height(row_heights: &[u16], rows: usize) -> u16 {
    let rows = rows.min(row_heights.len());
    let row_height = row_heights[..rows]
        .iter()
        .copied()
        .fold(0_u16, u16::saturating_add);
    let spacers = rows.saturating_sub(1).min(u16::MAX as usize) as u16;
    SESSION_TABLE_CHROME_HEIGHT
        .saturating_add(row_height)
        .saturating_add(spacers)
}

fn prepare_active_previews(
    dashboard: &mut DashboardState,
    preview_width: u16,
    maximum_selected_lines: usize,
) -> Vec<Vec<Line<'static>>> {
    let active_ids = partition_sessions(
        dashboard.state.sessions.values(),
        &dashboard.session_details,
    )
    .0
    .into_iter()
    .map(|session| session.id.clone())
    .collect::<Vec<_>>();
    let selected_active = (dashboard.focus == Focus::Active)
        .then_some(dashboard.session_index)
        .filter(|index| *index < active_ids.len());
    active_ids
        .iter()
        .enumerate()
        .map(|(index, session_id)| {
            let detail = dashboard.session_details.get_mut(session_id);
            if selected_active == Some(index) {
                active_transcript_tail(detail, preview_width, maximum_selected_lines)
            } else {
                active_message_preview(detail.as_deref(), usize::from(preview_width))
            }
        })
        .collect()
}

fn render_terminal_too_small(frame: &mut Frame, area: Rect, required_height: u16) {
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(vec![
            Line::styled(
                "Terminal too small",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Line::raw(format!(
                "Increase height to at least {required_height} rows (currently {}).",
                area.height
            )),
        ])
        .alignment(Alignment::Center),
        area,
    );
}

fn render_import_progress(frame: &mut Frame, area: Rect, progress: &ImportProgress) {
    let popup = centered_rect(76, 10, area);
    frame.render_widget(Clear, popup);
    let total = progress
        .total
        .map_or_else(|| "?".into(), |total| total.to_string());
    let stalled_for = progress.last_updated.elapsed();
    let status = if stalled_for >= IMPORT_STALL_WARNING_AFTER {
        Line::styled(
            format!(
                "No progress for {}s; the filesystem may be stalled.",
                stalled_for.as_secs()
            ),
            Style::default().fg(Color::Yellow),
        )
    } else {
        Line::styled(
            "The dashboard remains responsive while the import runs.",
            Style::default().fg(Color::Gray),
        )
    };
    frame.render_widget(
        Paragraph::new(vec![
            Line::styled(
                truncate_text(&progress.session_title, 60),
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Line::raw(""),
            Line::raw(progress.message.clone()),
            status,
            Line::styled("Esc cancels this import.", Style::default().fg(Color::Gray)),
        ])
        .block(Block::default().borders(Borders::ALL).title(format!(
            " Importing session · progress {}/{total} ",
            progress.step
        )))
        .wrap(Wrap { trim: true }),
        popup,
    );
}

fn render_import_bundle_confirmation(
    frame: &mut Frame,
    area: Rect,
    confirmation: &ImportBundleConfirmation,
) {
    let height =
        (confirmation.dirty_git_roots.len() + confirmation.omitted_non_git_dirs.len() + 10) as u16;
    let popup = centered_rect(76, height.clamp(12, 24), area);
    frame.render_widget(Clear, popup);
    let mut lines = Vec::new();
    if !confirmation.dirty_git_roots.is_empty() {
        lines.push(Line::raw(
            "These Git roots are dirty; Hel will archive their complete current state:",
        ));
        lines.extend(
            confirmation
                .dirty_git_roots
                .iter()
                .map(|root| Line::styled(root.clone(), Style::default().fg(Color::Yellow))),
        );
    }
    if !confirmation.omitted_non_git_dirs.is_empty() {
        if !lines.is_empty() {
            lines.push(Line::raw(""));
        }
        lines.push(Line::raw(
            "These edited directories are outside Git and cannot be included:",
        ));
        lines.extend(
            confirmation.omitted_non_git_dirs.iter().map(|directory| {
                Line::styled(directory.clone(), Style::default().fg(Color::Yellow))
            }),
        );
    }
    lines.extend([
        Line::raw(""),
        Line::raw("Press y/Enter to continue, or n/Esc to cancel."),
    ]);
    frame.render_widget(
        Paragraph::new(lines)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" Import safety warning "),
            )
            .wrap(Wrap { trim: true }),
        popup,
    );
}

fn render_import_dialog(frame: &mut Frame, area: Rect, dialog: &ImportDialog) {
    let popup = centered_rect(82, 22, area);
    frame.render_widget(Clear, popup);
    let outer = Block::default()
        .borders(Borders::ALL)
        .title(" Import native session ");
    let inner = outer.inner(popup);
    frame.render_widget(outer, popup);
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(5), Constraint::Length(3)])
        .split(inner);
    let panes = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(36), Constraint::Percentage(64)])
        .split(rows[0]);

    let profile_items = dialog
        .profiles
        .iter()
        .map(|profile| {
            ListItem::new(format!(
                "{}  {}",
                profile.profile_id,
                harness_label(profile.harness_kind)
            ))
        })
        .collect::<Vec<_>>();
    let mut profile_state = ListState::default()
        .with_selected((!dialog.profiles.is_empty()).then_some(dialog.profile_index));
    let profiles_focused = dialog.focus == ImportFocus::Profiles;
    frame.render_stateful_widget(
        List::new(profile_items)
            .highlight_symbol(if profiles_focused { "› " } else { "  " })
            .highlight_style(if profiles_focused {
                Style::default().bg(Color::DarkGray).fg(Color::White)
            } else {
                Style::default()
            })
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_type(focus_border(profiles_focused))
                    .title(" Profiles "),
            ),
        panes[0],
        &mut profile_state,
    );

    let selected_profile = dialog.profiles.get(dialog.profile_index);
    let session_items = selected_profile
        .map(|profile| {
            if profile.sessions.is_empty() {
                if let Some(error) = &profile.error {
                    vec![ListItem::new(format!("Unavailable: {error}"))]
                } else if profile
                    .scan_progress
                    .is_none_or(|(scanned, total)| scanned < total)
                {
                    vec![ListItem::new("Scanning native sessions…")]
                } else {
                    vec![ListItem::new("No native sessions found")]
                }
            } else {
                profile
                    .sessions
                    .iter()
                    .map(|session| {
                        let title_style = if session.unavailable_reason.is_some() {
                            Style::default().fg(Color::DarkGray)
                        } else {
                            Style::default().add_modifier(Modifier::BOLD)
                        };
                        let details = if session.unavailable_reason.is_some() {
                            format!("{} · unavailable", session.details)
                        } else {
                            session.details.clone()
                        };
                        ListItem::new(vec![
                            Line::styled(
                                truncate_text(
                                    &session.title,
                                    panes[1].width.saturating_sub(4) as usize,
                                ),
                                title_style,
                            ),
                            Line::styled(details, Style::default().fg(Color::Gray)),
                        ])
                    })
                    .collect()
            }
        })
        .unwrap_or_default();
    let selectable_sessions = selected_profile.is_some_and(|profile| !profile.sessions.is_empty());
    let mut session_state =
        ListState::default().with_selected(selectable_sessions.then_some(dialog.session_index));
    let sessions_focused = dialog.focus == ImportFocus::Sessions;
    let sessions_title = selected_profile
        .and_then(|profile| profile.scan_progress)
        .map(|(scanned, total)| {
            format!(" Native sessions · newest first · {scanned}/{total} sessions scanned ")
        })
        .unwrap_or_else(|| " Native sessions · newest first · scanning… ".into());
    frame.render_stateful_widget(
        List::new(session_items)
            .highlight_symbol(if sessions_focused { "› " } else { "  " })
            .highlight_style(if sessions_focused {
                Style::default().bg(Color::DarkGray).fg(Color::White)
            } else {
                Style::default()
            })
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_type(focus_border(sessions_focused))
                    .title(sessions_title),
            ),
        panes[1],
        &mut session_state,
    );

    let unavailable_reason = selected_profile
        .and_then(|profile| profile.sessions.get(dialog.session_index))
        .and_then(|session| session.unavailable_reason.as_deref());
    let (status, status_style) = unavailable_reason.map_or_else(
        || {
            (
                "Tab moves focus · ↑/↓ selects · Enter activates".to_owned(),
                Style::default().fg(Color::Gray),
            )
        },
        |reason| {
            (
                format!("Cannot import: {reason}"),
                Style::default().fg(Color::Yellow),
            )
        },
    );
    frame.render_widget(
        Paragraph::new(vec![
            Line::styled(status, status_style),
            action_buttons(&[
                ("Cancel", dialog.focus == ImportFocus::Cancel),
                ("Import", dialog.focus == ImportFocus::Import),
            ]),
        ])
        .alignment(Alignment::Center)
        .wrap(Wrap { trim: true }),
        rows[1],
    );
}

fn render_onboarding(frame: &mut Frame, area: Rect, dashboard: &DashboardState) {
    let missing = [
        (dashboard.config.profiles.is_empty(), "a harness profile"),
        (dashboard.config.targets.is_empty(), "a target template"),
    ]
    .into_iter()
    .filter_map(|(missing, label)| missing.then_some(label))
    .collect::<Vec<_>>()
    .join(", ");
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(Span::styled(
                "Hel needs a little fuel.",
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Line::raw(""),
            Line::raw(format!("Setup can create {missing} from this machine.")),
            Line::raw("Press Ctrl+E to run setup, or edit Hel's TOML configuration by hand."),
        ])
        .alignment(Alignment::Center)
        .wrap(Wrap { trim: true })
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Get started "),
        ),
        area,
    );
}

fn render_sessions(
    frame: &mut Frame,
    active_area: Rect,
    archived_area: Rect,
    dashboard: &mut DashboardState,
    active_previews: &[Vec<Line<'static>>],
) {
    let (active, archived) = partition_sessions(
        dashboard.state.sessions.values(),
        &dashboard.session_details,
    );
    let now_epoch_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let preview_width = active_area.width.saturating_sub(4);
    let active_rows =
        active
            .iter()
            .zip(active_previews)
            .enumerate()
            .map(|(index, (session, preview))| {
                active_session_row(
                    session,
                    dashboard.session_details.get(&session.id),
                    now_epoch_seconds,
                    &dashboard.config,
                    preview.len() as u16 + 1,
                    u16::from(index > 0),
                )
            });
    let active_focused = dashboard.focus == Focus::Active;
    let active_table = Table::new(active_rows, session_column_constraints())
        .header(session_header())
        // Active rows reserve multiple lines for the conversation preview.
        // Selection styling is applied to the one-line session summary below.
        .row_highlight_style(Style::default())
        .highlight_symbol(if active_focused { "› " } else { "  " })
        .highlight_spacing(HighlightSpacing::Always)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(focus_border(active_focused))
                .title(" Active "),
        );
    let mut active_state = TableState::default()
        .with_selected((dashboard.session_index < active.len()).then_some(dashboard.session_index));
    frame.render_stateful_widget(active_table, active_area, &mut active_state);
    let active_offset = active_state.offset();
    let mut row_y = active_area.y + SESSION_TABLE_CHROME_HEIGHT;
    let mut visible_sessions = 0;
    for (index, _session) in active.iter().enumerate().skip(active_offset) {
        let preview = &active_previews[index];
        let spacer = u16::from(index > 0);
        let detail_y = row_y.saturating_add(spacer);
        if detail_y >= active_area.bottom().saturating_sub(1) {
            break;
        }
        visible_sessions += 1;
        let selected = dashboard.focus == Focus::Active && index == dashboard.session_index;
        let info_y = detail_y.saturating_sub(1);
        if selected && info_y < active_area.bottom().saturating_sub(1) {
            frame.buffer_mut().set_style(
                Rect::new(
                    active_area.x.saturating_add(1),
                    info_y,
                    active_area.width.saturating_sub(2),
                    1,
                ),
                Style::default().bg(Color::DarkGray).fg(Color::White),
            );
        }
        let preview_height = active_area
            .bottom()
            .saturating_sub(1)
            .saturating_sub(detail_y)
            .min(preview.len() as u16);
        if preview_height > 0 {
            frame.render_widget(
                Paragraph::new(preview.clone()),
                Rect::new(active_area.x + 3, detail_y, preview_width, preview_height),
            );
        }
        row_y = row_y.saturating_add(preview.len() as u16 + 1 + spacer);
    }
    render_session_scrollbar(
        frame,
        active_area,
        active.len(),
        active_offset,
        visible_sessions,
    );

    let archived_rows = archived.iter().map(|session| archived_session_row(session));
    let archived_focused = dashboard.focus == Focus::Archived;
    let archived_table = Table::new(archived_rows, archived_session_column_constraints())
        .header(archived_session_header())
        .row_highlight_style(if archived_focused {
            Style::default().bg(Color::DarkGray).fg(Color::White)
        } else {
            Style::default()
        })
        .highlight_symbol(if archived_focused { "› " } else { "  " })
        .highlight_spacing(HighlightSpacing::Always)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(focus_border(archived_focused))
                .title(" Paused "),
        );
    let mut archived_state = TableState::default().with_selected(
        Some(dashboard.session_index.saturating_sub(active.len()))
            .filter(|index| *index < archived.len()),
    );
    frame.render_stateful_widget(archived_table, archived_area, &mut archived_state);
    render_session_scrollbar(
        frame,
        archived_area,
        archived.len(),
        archived_state.offset(),
        usize::from(
            archived_area
                .height
                .saturating_sub(SESSION_TABLE_CHROME_HEIGHT),
        ),
    );
}

fn render_session_scrollbar(
    frame: &mut Frame,
    area: Rect,
    content_length: usize,
    position: usize,
    viewport_content_length: usize,
) {
    if content_length <= viewport_content_length {
        return;
    }
    let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
        .thumb_style(Style::default().fg(Color::Gray))
        .track_style(Style::default().fg(Color::DarkGray));
    let mut state = ScrollbarState::new(content_length)
        .position(position)
        .viewport_content_length(viewport_content_length.max(1));
    frame.render_stateful_widget(
        scrollbar,
        area.inner(Margin {
            vertical: 1,
            horizontal: 0,
        }),
        &mut state,
    );
}

fn focus_border(focused: bool) -> BorderType {
    if focused {
        BorderType::Double
    } else {
        BorderType::Plain
    }
}

fn session_column_constraints() -> [Constraint; 5] {
    [
        Constraint::Length(10),
        Constraint::Length(14),
        Constraint::Length(18),
        Constraint::Length(17),
        Constraint::Min(18),
    ]
}

fn archived_session_column_constraints() -> [Constraint; 4] {
    [
        Constraint::Length(14),
        Constraint::Length(14),
        Constraint::Length(17),
        Constraint::Min(18),
    ]
}

fn session_header() -> Row<'static> {
    Row::new([
        "Turn clock",
        "Profile",
        "Target",
        "Resources",
        "Session name",
    ])
    .style(Style::default().add_modifier(Modifier::BOLD))
}

fn archived_session_header() -> Row<'static> {
    Row::new(["Profile", "Archived", "Archive", "Session name"])
        .style(Style::default().add_modifier(Modifier::BOLD))
}

fn session_values(
    session: &SessionRecord,
    detail: Option<&SessionDetail>,
    now_epoch_seconds: u64,
    config: &HelConfig,
) -> (String, String, String, String, String) {
    let clock = if session.state == SessionState::Provisioning {
        let started_at = session_updated_at_epoch_seconds(session).unwrap_or(now_epoch_seconds);
        format!("Launch {}s", now_epoch_seconds.saturating_sub(started_at))
    } else {
        crate::usage_format::format_turn_clock(
            now_epoch_seconds,
            detail.and_then(|detail| detail.current_turn_started_at),
        )
    };
    (
        clock,
        session.last_profile.clone(),
        session_target(config, session),
        detail
            .and_then(|detail| detail.resource_usage.as_ref())
            .map(resource_summary)
            .unwrap_or_else(|| "—".into()),
        session_name(session).to_string(),
    )
}

fn session_updated_at_epoch_seconds(session: &SessionRecord) -> Option<u64> {
    chrono::DateTime::parse_from_rfc3339(&session.updated_at)
        .ok()?
        .timestamp()
        .try_into()
        .ok()
}

fn session_target(config: &HelConfig, session: &SessionRecord) -> String {
    let Some(bundle) = config.bundles.get(&session.bundle_id) else {
        return session.target_template_id.clone();
    };
    let project_dirs = bundle
        .repositories
        .iter()
        .map(|repository| repository.destination.display().to_string())
        .collect::<Vec<_>>()
        .join(", ");
    format!("{}: {project_dirs}", session.target_template_id)
}

fn resource_summary(usage: &SessionResourceUsage) -> String {
    let memory = match usage.memory_limit_bytes {
        Some(limit) if limit > 0 => format!(
            "M {}%",
            u128::from(usage.memory_current_bytes) * 100 / u128::from(limit)
        ),
        _ => format!("M {}", format_resource_bytes(usage.memory_current_bytes)),
    };
    let mut resources = Vec::new();
    if let Some(cpu) = usage.cpu_percent {
        resources.push(format!("C {cpu}%"));
    }
    resources.push(memory);
    if let Some(swap) = usage.swap_current_bytes.filter(|swap| *swap > 0) {
        resources.push(format!("S {}", format_resource_bytes(swap)));
    }
    if let Some(disk) = usage.writable_disk_bytes {
        resources.push(format!("D {}", format_resource_bytes(disk)));
    }
    resources.join(" · ")
}

fn checkpoint_archive_size(session: &SessionRecord) -> String {
    session
        .checkpoint
        .as_ref()
        .and_then(|checkpoint| std::fs::metadata(&checkpoint.archive_path).ok())
        .map(|metadata| format_resource_bytes(metadata.len()))
        .unwrap_or_else(|| "—".into())
}

fn format_resource_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    const TIB: f64 = GIB * 1024.0;

    if bytes < 1024 {
        format!("{bytes}B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1}K", bytes as f64 / KIB)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1}M", bytes as f64 / MIB)
    } else if bytes < 1024_u64.pow(4) {
        format!("{:.1}G", bytes as f64 / GIB)
    } else {
        format!("{:.1}T", bytes as f64 / TIB)
    }
}

fn session_name(session: &SessionRecord) -> &str {
    session.display_title()
}

fn session_name_line(session_name: String, unread_count: usize) -> Line<'static> {
    let mut spans = vec![Span::raw(session_name)];
    if unread_count > 0 {
        spans.push(Span::styled(
            format!("  {unread_count} unread"),
            Style::default()
                .fg(Color::Blue)
                .add_modifier(Modifier::BOLD),
        ));
    }
    Line::from(spans)
}

fn active_message_preview(detail: Option<&SessionDetail>, width: usize) -> Vec<Line<'static>> {
    detail
        .and_then(|detail| detail.last_agent_message.as_deref())
        .map(|message| {
            let single_line = message.lines().collect::<Vec<_>>().join(" ");
            render_agent_message_preview(&single_line, width, ACTIVE_MESSAGE_LINES)
        })
        .unwrap_or_default()
}

fn active_transcript_tail(
    detail: Option<&mut SessionDetail>,
    width: u16,
    maximum_lines: usize,
) -> Vec<Line<'static>> {
    let Some(detail) = detail else {
        return Vec::new();
    };
    match detail.transcript.as_mut() {
        Some(transcript) => transcript.rich_tail(width, maximum_lines),
        None => active_message_preview(Some(detail), usize::from(width)),
    }
}

fn active_session_row(
    session: &SessionRecord,
    detail: Option<&SessionDetail>,
    now_epoch_seconds: u64,
    config: &HelConfig,
    height: u16,
    top_margin: u16,
) -> Row<'static> {
    let (clock, profile, target, resources, session_name) =
        session_values(session, detail, now_epoch_seconds, config);
    let unread_count = detail.map_or(0, |detail| detail.unread_agent_message_sequences.len());
    Row::new([
        Cell::from(clock),
        Cell::from(profile),
        Cell::from(target),
        Cell::from(resources),
        Cell::from(session_name_line(
            recovery_warning_name(session, session_name, now_epoch_seconds),
            unread_count,
        )),
    ])
    .height(height)
    .top_margin(top_margin)
}

fn archived_session_row(session: &SessionRecord) -> Row<'static> {
    let checkpoint = session
        .checkpoint
        .as_ref()
        .map(|checkpoint| checkpoint_time_display(&checkpoint.created_at))
        .unwrap_or_else(|| "never".into());
    Row::new([
        session.last_profile.clone(),
        checkpoint,
        checkpoint_archive_size(session),
        session_name(session).to_string(),
    ])
}

fn checkpoint_age(now_epoch_seconds: u64, checkpointed_at: &str) -> String {
    let Ok(checkpointed_at) = chrono::DateTime::parse_from_rfc3339(checkpointed_at) else {
        return "unknown".into();
    };
    let checkpointed_at = checkpointed_at.timestamp().max(0) as u64;
    let age = now_epoch_seconds.saturating_sub(checkpointed_at);
    if age < 60 {
        format!("{age}s")
    } else if age < 3_600 {
        format!("{}m", age / 60)
    } else if age < 86_400 {
        format!("{}h", age / 3_600)
    } else {
        format!("{}d", age / 86_400)
    }
}

fn recovery_warning_name(session: &SessionRecord, name: String, now_epoch_seconds: u64) -> String {
    if session.last_checkpoint_error.is_none() {
        return name;
    }
    match &session.checkpoint {
        Some(checkpoint) => format!(
            "{name}  ⚠ Recovery copy {} old",
            checkpoint_age(now_epoch_seconds, &checkpoint.created_at)
        ),
        None => format!("{name}  ⚠ Recovery unavailable"),
    }
}

fn checkpoint_time_display(checkpointed_at: &str) -> String {
    chrono::DateTime::parse_from_rfc3339(checkpointed_at)
        .map(|checkpointed_at| checkpointed_at.format("%y-%m-%d %H:%M").to_string())
        .unwrap_or_else(|_| "unknown".into())
}

fn render_capacity(frame: &mut Frame, area: Rect, dashboard: &mut DashboardState) {
    let rows = dashboard.capacity_details.values().map(|detail| {
        let capacity = match (&detail.target.kind, &detail.usage) {
            (DeploymentCapacityKind::Host, Some(usage)) => {
                let memory_percent = if usage.memory_total_bytes == 0 {
                    0
                } else {
                    (u128::from(usage.memory_used_bytes) * 100
                        / u128::from(usage.memory_total_bytes))
                    .min(100)
                };
                format!(
                    "{}% CPU · {memory_percent}% RAM",
                    usage.cpu_percent.unwrap_or(0)
                )
            }
            (DeploymentCapacityKind::AwsFleet, Some(usage)) => format!(
                "{} cores · {} RAM · {} disk",
                usage.logical_cores,
                format_resource_bytes(usage.memory_total_bytes),
                format_resource_bytes(usage.disk_total_bytes.unwrap_or(0))
            ),
            (DeploymentCapacityKind::AwsFleet, None) if detail.on_demand => "on demand".into(),
            _ => "unavailable".into(),
        };
        Row::new([
            detail.target.host.clone(),
            detail.target.target_ids.join(", "),
            capacity,
        ])
    });
    let focused = dashboard.focus == Focus::Capacity;
    let table = Table::new(
        rows,
        [
            Constraint::Percentage(22),
            Constraint::Percentage(36),
            Constraint::Percentage(42),
        ],
    )
    .header(
        Row::new(["Host / fleet", "Targets", "Capacity"])
            .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .row_highlight_style(if focused {
        Style::default().bg(Color::DarkGray).fg(Color::White)
    } else {
        Style::default()
    })
    .highlight_symbol(if focused { "› " } else { "  " })
    .highlight_spacing(HighlightSpacing::Always)
    .block(
        Block::default()
            .borders(Borders::ALL)
            .border_type(focus_border(focused))
            .title(" Capacity in Use "),
    );
    let mut state = TableState::default().with_selected(
        (!dashboard.capacity_details.is_empty()).then_some(dashboard.capacity_index),
    );
    frame.render_stateful_widget(table, area, &mut state);
    render_session_scrollbar(
        frame,
        area,
        dashboard.capacity_details.len(),
        state.offset(),
        usize::from(area.height.saturating_sub(SESSION_TABLE_CHROME_HEIGHT)),
    );
}

fn render_quotas(frame: &mut Frame, area: Rect, dashboard: &mut DashboardState) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let rows = dashboard.config.profiles.iter().map(|(id, profile)| {
        let usage = if dashboard.quota_refreshing.contains(id) {
            "refreshing".into()
        } else {
            match dashboard.quotas.get(id) {
                Some(quota) => quota.compact(),
                None => "refreshing".into(),
            }
        };
        Row::new([
            Cell::from(id.clone()),
            Cell::from(harness_label(profile.kind)),
            Cell::from(usage),
        ])
    });
    let refresh_status = if !dashboard.quota_refreshing.is_empty() {
        "refreshing…".to_string()
    } else {
        dashboard
            .quotas
            .values()
            .map(|quota| quota.refreshed_at_epoch_seconds)
            .min()
            .map(|refreshed| format!("refreshed {}", refresh_age(now, refreshed)))
            .unwrap_or_else(|| "not refreshed".to_string())
    };
    let title = Line::from(vec![
        Span::raw(" Profile Quotas "),
        Span::styled(
            format!("({refresh_status}) "),
            Style::default().fg(Color::DarkGray),
        ),
    ]);
    let quotas_focused = dashboard.focus == Focus::Quotas;
    let border_type = if quotas_focused {
        BorderType::Double
    } else {
        BorderType::Plain
    };
    let table = Table::new(
        rows,
        [
            Constraint::Percentage(14),
            Constraint::Percentage(12),
            Constraint::Percentage(74),
        ],
    )
    .header(
        Row::new(["Profile", "Harness", "Quota / reset / error"])
            .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .row_highlight_style(if quotas_focused {
        Style::default().bg(Color::DarkGray).fg(Color::White)
    } else {
        Style::default()
    })
    .highlight_symbol(if quotas_focused { "› " } else { "  " })
    .highlight_spacing(HighlightSpacing::Always)
    .block(
        Block::default()
            .borders(Borders::ALL)
            .border_type(border_type)
            .title(title),
    );
    let mut state = TableState::default()
        .with_selected((!dashboard.config.profiles.is_empty()).then_some(dashboard.quota_index));
    frame.render_stateful_widget(table, area, &mut state);
    render_session_scrollbar(
        frame,
        area,
        dashboard.config.profiles.len(),
        state.offset(),
        usize::from(area.height.saturating_sub(SESSION_TABLE_CHROME_HEIGHT)),
    );
}

fn render_footer(frame: &mut Frame, area: Rect, dashboard: &DashboardState) {
    let accelerator = if cfg!(target_os = "macos") {
        "Cmd"
    } else {
        "Ctrl"
    };
    let actions = match dashboard.focus {
        Focus::Active => {
            "[N]ew · [I]mport · [R]ename · [P]ause · [D]elete · [U]pdate quotas · [Q]uit · Tab pane"
        }
        Focus::Archived => {
            "[N]ew · [I]mport · [R]ename · [D]elete permanently · [U]pdate quotas · [Q]uit · Tab pane"
        }
        Focus::Capacity => "[N]ew · [I]mport · [U]pdate quotas · [Q]uit · Tab pane",
        Focus::Quotas => "[N]ew · [I]mport · [R]efresh · [U]pdate quotas · [Q]uit · Tab pane",
    };
    frame.render_widget(
        Paragraph::new(vec![
            Line::styled(
                format!("{accelerator} for: {actions}"),
                Style::default().fg(Color::DarkGray),
            ),
            Line::styled(
                dashboard.notice.as_deref().unwrap_or_default(),
                Style::default().fg(Color::Yellow),
            ),
        ]),
        area,
    );
}

fn action_buttons(buttons: &[(&str, bool)]) -> Line<'static> {
    let mut spans = Vec::new();
    for (index, (label, focused)) in buttons.iter().enumerate() {
        if index > 0 {
            spans.push(Span::raw("  "));
        }
        let style = if *focused {
            Style::default()
                .bg(Color::Cyan)
                .fg(Color::Black)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().bg(Color::DarkGray).fg(Color::White)
        };
        spans.push(Span::styled(format!(" {label} "), style));
    }
    Line::from(spans).alignment(Alignment::Center)
}

fn wizard_buttons(focus: WizardFocus, has_back: bool, next_label: &str) -> Line<'static> {
    if has_back {
        action_buttons(&[
            ("Cancel", focus == WizardFocus::Cancel),
            ("Back", focus == WizardFocus::Back),
            (next_label, focus == WizardFocus::Next),
        ])
    } else {
        action_buttons(&[
            ("Cancel", focus == WizardFocus::Cancel),
            (next_label, focus == WizardFocus::Next),
        ])
    }
}

fn render_new_wizard(
    frame: &mut Frame,
    area: Rect,
    dashboard: &DashboardState,
    wizard: &NewWizard,
) {
    if wizard.step == WizardStep::Review {
        let target_id = nth_key(&dashboard.config.targets, wizard.target);
        let raw_project = is_bare_project_target(&dashboard.config.targets[&target_id]);
        let bundle_id = (!raw_project)
            .then(|| nth_bundle_key(&dashboard.config, &dashboard.state, wizard.bundle));
        render_review_wizard(
            frame,
            area,
            dashboard,
            ReviewWizardView {
                profile_id: &nth_key(&dashboard.config.profiles, wizard.profile),
                bundle_id: if raw_project {
                    wizard.project_directory.trim()
                } else {
                    bundle_id.as_deref().expect("bundle selected")
                },
                target_id: &target_id,
                allocation: wizard.resource_allocation.as_ref(),
                mounts: &wizard.mounts,
                focus: wizard.review_focus,
                title: " New session · 4/4 review ",
                submit_label: "Create",
            },
        );
        return;
    }
    if wizard.step == WizardStep::ProjectDirectory {
        let target_id = nth_key(&dashboard.config.targets, wizard.target);
        let local = matches!(
            dashboard.config.targets[&target_id],
            TargetTemplate::LocalBare
        );
        let mut lines = vec![
            Line::raw(if local {
                "Absolute project directory on this machine:"
            } else {
                "Absolute project directory on the remote machine:"
            }),
            Line::raw(""),
            Line::styled(
                format!("> {}▏", wizard.project_directory),
                Style::default().bg(Color::DarkGray).fg(Color::White),
            ),
        ];
        if !wizard.project_history.is_empty() {
            lines.push(Line::raw(""));
            lines.push(Line::styled(
                "Recent on this host (↑/↓ selects):",
                Style::default().fg(Color::Gray),
            ));
            lines.extend(wizard.project_history.iter().take(5).enumerate().map(
                |(index, directory)| {
                    Line::styled(
                        format!(
                            "{} {}",
                            if index == wizard.project_history_index {
                                "›"
                            } else {
                                " "
                            },
                            directory.display()
                        ),
                        if index == wizard.project_history_index {
                            Style::default().fg(Color::White)
                        } else {
                            Style::default().fg(Color::DarkGray)
                        },
                    )
                },
            ));
        }
        if let Some(error) = &wizard.project_directory_error {
            lines.push(Line::raw(""));
            lines.push(Line::styled(
                format!("Error: {error}"),
                Style::default().fg(Color::Red),
            ));
        }
        lines.push(Line::styled(
            "Enter validates · Backspace on empty goes back · Esc cancels",
            Style::default().fg(Color::Gray),
        ));
        let popup = centered_rect(76, (lines.len() as u16 + 2).clamp(9, 16), area);
        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new(lines).block(Block::default().borders(Borders::ALL).title(if local {
                " New session · 3/4 local project "
            } else {
                " New session · 3/4 remote project "
            })),
            popup,
        );
        return;
    }
    if wizard.step == WizardStep::Mounts {
        render_mount_wizard(
            frame,
            area,
            dashboard,
            wizard.target,
            &wizard.mounts,
            " Add attached directory ",
        );
        return;
    }
    if wizard.step == WizardStep::NewBundle {
        let popup = centered_rect(76, 9, area);
        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new(vec![
                Line::raw("Local Git path or GitHub owner/repository:"),
                Line::raw(""),
                Line::styled(
                    format!(
                        "> {}{}",
                        wizard.new_bundle_source,
                        if wizard.focus == WizardFocus::Content {
                            "▏"
                        } else {
                            ""
                        }
                    ),
                    if wizard.focus == WizardFocus::Content {
                        Style::default().bg(Color::DarkGray).fg(Color::White)
                    } else {
                        Style::default()
                    },
                ),
                Line::styled(
                    "Tab moves focus · Enter activates · Esc cancels",
                    Style::default().fg(Color::Gray),
                ),
                wizard_buttons(wizard.focus, true, "Create repository"),
            ])
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" New repository bundle "),
            ),
            popup,
        );
        return;
    }
    let (title, choices, selected) = match wizard.step {
        WizardStep::Profile => (
            " New session · 1/4 profile ",
            dashboard
                .config
                .profiles
                .iter()
                .map(|(id, profile)| dashboard.profile_choice(id, profile.kind))
                .collect(),
            wizard.profile,
        ),
        WizardStep::Bundle => (
            " New session · 3/4 project bundle ",
            bundle_ids_by_recent_creation(&dashboard.config, &dashboard.state)
                .into_iter()
                .map(|id| {
                    let bundle = &dashboard.config.bundles[id];
                    format!("{id}  {} repositories", bundle.repositories.len())
                })
                .chain(["New repository…".to_owned()])
                .collect(),
            wizard.bundle,
        ),
        WizardStep::Target => (
            " New session · 2/4 target ",
            dashboard
                .config
                .targets
                .iter()
                .map(|(id, target)| {
                    let size = if id == &nth_key(&dashboard.config.targets, wizard.target) {
                        resource_allocation_label(
                            wizard.resource_allocation.as_ref(),
                            wizard.sizing_error.as_deref(),
                        )
                    } else {
                        String::new()
                    };
                    format!("{id}  {}{size}", target_label(target))
                })
                .collect(),
            wizard.target,
        ),
        WizardStep::Review => unreachable!("review was rendered above"),
        WizardStep::Mounts => unreachable!("mount input was rendered above"),
        WizardStep::NewBundle => unreachable!("bundle input was rendered above"),
        WizardStep::ProjectDirectory => unreachable!("project directory input was rendered above"),
    };
    let help = if wizard.step == WizardStep::Target {
        "+ both · c CPU · m memory · - halve · r reset"
    } else {
        "↑/↓ select · Tab moves focus · Enter activates"
    };
    render_picker(
        frame,
        area,
        title,
        choices,
        selected,
        &[help],
        PickerNavigation {
            focus: wizard.focus,
            has_back: wizard.step != WizardStep::Profile,
        },
    );
}

struct ReviewWizardView<'a> {
    profile_id: &'a str,
    bundle_id: &'a str,
    target_id: &'a str,
    allocation: Option<&'a SessionResourceAllocation>,
    mounts: &'a MountWizard,
    focus: ReviewFocus,
    title: &'a str,
    submit_label: &'a str,
}

fn render_review_wizard(
    frame: &mut Frame,
    area: Rect,
    dashboard: &DashboardState,
    view: ReviewWizardView<'_>,
) {
    let ReviewWizardView {
        profile_id,
        bundle_id,
        target_id,
        allocation,
        mounts,
        focus,
        title,
        submit_label,
    } = view;
    let target = &dashboard.config.targets[target_id];
    let can_attach = mount_history_host(target).is_some();
    let mut lines = vec![
        Line::raw(format!("Profile: {profile_id}")),
        Line::raw(format!("Project: {bundle_id}")),
        Line::raw(format!("Target: {target_id} ({})", target_label(target))),
        Line::raw(format!(
            "Compute:{}",
            resource_allocation_label(allocation, None)
        )),
    ];
    if matches!(target, TargetTemplate::LocalBare)
        && dashboard
            .config
            .profiles
            .get(profile_id)
            .is_some_and(|profile| profile.kind == HarnessKind::Kimi)
    {
        lines.push(Line::styled(
            "⚠ DANGER: Kimi auto mode on raw localhost can modify this machine without approval.",
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        ));
    }
    if can_attach {
        lines.push(Line::raw(""));
        lines.push(Line::raw(format!(
            "Attached directories: {}",
            mounts.mounts.len()
        )));
    }
    if can_attach && mounts.mounts.is_empty() {
        lines.push(Line::styled(
            "  None (optional)",
            Style::default().fg(Color::DarkGray),
        ));
    } else if can_attach {
        lines.extend(
            mounts
                .mounts
                .iter()
                .enumerate()
                .take(6)
                .map(|(index, mount)| {
                    let selected =
                        focus == ReviewFocus::Attachments && index == mounts.history_index;
                    Line::styled(
                        format!(
                            "{}{} → {}",
                            if selected { "› " } else { "  " },
                            mount.source.display(),
                            mount.destination.display()
                        ),
                        if selected {
                            Style::default().bg(Color::DarkGray).fg(Color::White)
                        } else {
                            Style::default()
                        },
                    )
                }),
        );
    }
    lines.push(Line::raw(""));
    lines.push(Line::styled(
        if can_attach {
            "Tab moves focus · Enter edits selected directory · Del removes it"
        } else {
            "Tab moves focus · Enter activates"
        },
        Style::default().fg(Color::DarkGray),
    ));
    let mut buttons = vec![
        ("Cancel", focus == ReviewFocus::Cancel),
        ("Back", focus == ReviewFocus::Back),
    ];
    if can_attach {
        buttons.push(("Add directory…", focus == ReviewFocus::Add));
    }
    buttons.push((submit_label, focus == ReviewFocus::Submit));
    lines.push(action_buttons(&buttons));
    let popup = centered_rect(84, (lines.len() as u16 + 2).clamp(13, 24), area);
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(lines)
            .block(Block::default().borders(Borders::ALL).title(title))
            .wrap(Wrap { trim: false }),
        popup,
    );
}

fn render_mount_wizard(
    frame: &mut Frame,
    area: Rect,
    dashboard: &DashboardState,
    target_index: usize,
    mounts: &MountWizard,
    title: &str,
) {
    let target_id = nth_key(&dashboard.config.targets, target_index);
    let target = dashboard
        .config
        .targets
        .get(&target_id)
        .expect("selected target index is present in config");
    let protection = match target {
        TargetTemplate::AppleContainer { .. } => {
            "Apple Container has no :O overlay mode; each extra bind is read-only."
        }
        TargetTemplate::LocalPodman { .. } | TargetTemplate::SshPodman { .. } => {
            "Podman uses :O copy-on-write overlays; container writes never change the source."
        }
        TargetTemplate::AwsEc2 { .. } => {
            "EC2 directories stream as tar.gz through one SSH connection into the destination."
        }
        TargetTemplate::LocalBare | TargetTemplate::SshBare { .. } => {
            unreachable!("bare targets do not attach resources")
        }
    };
    let source_marker = if mounts.focus == MountFocus::Source {
        "› "
    } else {
        "  "
    };
    let destination_marker = if mounts.focus == MountFocus::Destination {
        "› "
    } else {
        "  "
    };
    let source_caret = if mounts.focus == MountFocus::Source {
        "▏"
    } else {
        ""
    };
    let destination_caret = if mounts.focus == MountFocus::Destination {
        "▏"
    } else {
        ""
    };
    let mut lines = vec![
        Line::raw(format!("Target: {target_id} ({})", target_label(target))),
        Line::styled(protection, Style::default().fg(Color::Yellow)),
        Line::raw(""),
        Line::styled(
            format!("{source_marker}Source: {}{source_caret}", mounts.source),
            if mounts.focus == MountFocus::Source {
                Style::default().bg(Color::DarkGray).fg(Color::White)
            } else {
                Style::default()
            },
        ),
        Line::styled(
            format!(
                "{destination_marker}Destination: {}{destination_caret}",
                mounts.destination
            ),
            if mounts.focus == MountFocus::Destination {
                Style::default().bg(Color::DarkGray).fg(Color::White)
            } else {
                Style::default()
            },
        ),
    ];
    if !mounts.mounts.is_empty() {
        lines.push(Line::raw(""));
        lines.push(Line::raw("Already attached:"));
        lines.extend(mounts.mounts.iter().map(|mount| {
            Line::raw(format!(
                "  {} → {}",
                mount.source.display(),
                mount.destination.display()
            ))
        }));
    }
    if mounts.focus == MountFocus::Source && mounts.source.is_empty() && !mounts.history.is_empty()
    {
        lines.push(Line::raw(""));
        lines.push(Line::styled(
            "Recent sources (↑/↓ when Source is empty):",
            Style::default().fg(Color::DarkGray),
        ));
        lines.extend(
            mounts
                .history
                .iter()
                .take(5)
                .enumerate()
                .map(|(index, source)| {
                    let marker = if index == mounts.history_index {
                        "› "
                    } else {
                        "  "
                    };
                    Line::raw(format!("{marker}{}", source.display()))
                }),
        );
    }
    if !mounts.completion_candidates.is_empty() {
        lines.push(Line::raw(""));
        lines.push(Line::styled(
            "Matches (↑/↓ select · Enter choose):",
            Style::default().fg(Color::DarkGray),
        ));
        lines.extend(mounts.completion_candidates.iter().take(5).enumerate().map(
            |(index, candidate)| {
                Line::raw(format!(
                    "{}{}",
                    if index == mounts.completion_index {
                        "› "
                    } else {
                        "  "
                    },
                    candidate
                ))
            },
        ));
    }
    if let Some(error) = &mounts.error {
        lines.push(Line::raw(""));
        lines.push(Line::styled(error, Style::default().fg(Color::Red)));
    }
    lines.extend([
        Line::raw(""),
        Line::styled(
            "Tab completes source · Shift-Tab moves focus · Enter continues/adds",
            Style::default().fg(Color::DarkGray),
        ),
        action_buttons(&[
            ("Cancel", mounts.focus == MountFocus::Cancel),
            ("Back", mounts.focus == MountFocus::Back),
            ("Add directory", mounts.focus == MountFocus::Add),
        ]),
    ]);
    let popup = centered_rect(84, (lines.len() as u16 + 2).clamp(12, 24), area);
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(lines)
            .block(Block::default().borders(Borders::ALL).title(title))
            .wrap(Wrap { trim: false }),
        popup,
    );
}

fn render_resume_wizard(
    frame: &mut Frame,
    area: Rect,
    dashboard: &DashboardState,
    wizard: &ResumeWizard,
) {
    if wizard.step == WizardStep::Review {
        let profile_id = dashboard
            .compatible_profiles(&wizard.session_id)
            .get(wizard.profile)
            .map(|(id, _)| id.as_str())
            .unwrap_or("unknown");
        let bundle_id = dashboard
            .state
            .sessions
            .get(&wizard.session_id)
            .map(|session| session.bundle_id.as_str())
            .unwrap_or("unknown");
        render_review_wizard(
            frame,
            area,
            dashboard,
            ReviewWizardView {
                profile_id,
                bundle_id,
                target_id: &nth_key(&dashboard.config.targets, wizard.target),
                allocation: wizard.resource_allocation.as_ref(),
                mounts: &wizard.mounts,
                focus: wizard.review_focus,
                title: " Resume · 3/3 review ",
                submit_label: "Resume",
            },
        );
        return;
    }
    if wizard.step == WizardStep::Mounts {
        render_mount_wizard(
            frame,
            area,
            dashboard,
            wizard.target,
            &wizard.mounts,
            " Add attached directory ",
        );
        return;
    }
    let (title, choices, selected, help) = match wizard.step {
        WizardStep::Profile => (
            " Resume · 1/3 profile (cross-harness supported) ",
            dashboard
                .compatible_profiles(&wizard.session_id)
                .into_iter()
                .map(|(id, harness)| {
                    let mut choice = dashboard.profile_choice(id, harness);
                    if dashboard
                        .state
                        .sessions
                        .get(&wizard.session_id)
                        .is_some_and(|session| session.harness_kind != harness)
                    {
                        choice.insert_str(id.len(), "  (lossy: text-only transcript)");
                    }
                    choice
                })
                .collect(),
            wizard.profile,
            &[
                "↑/↓ select · Tab moves focus · Enter activates",
                "Lossy: text only; tool calls + reasoning dropped.",
            ][..],
        ),
        WizardStep::Target => (
            " Resume · 2/3 new target ",
            dashboard
                .config
                .targets
                .iter()
                .map(|(id, target)| {
                    let size = if id == &nth_key(&dashboard.config.targets, wizard.target) {
                        resource_allocation_label(
                            wizard.resource_allocation.as_ref(),
                            wizard.sizing_error.as_deref(),
                        )
                    } else {
                        String::new()
                    };
                    format!("{id}  {}{size}", target_label(target))
                })
                .collect(),
            wizard.target,
            &["+ both · c CPU · m memory · - halve · r reset"][..],
        ),
        WizardStep::Bundle => unreachable!("resume does not select a bundle"),
        WizardStep::Review => unreachable!("review was rendered above"),
        WizardStep::Mounts => unreachable!("mount input was rendered above"),
        WizardStep::NewBundle => unreachable!("resume does not create bundles"),
        WizardStep::ProjectDirectory => unreachable!("resume does not select a project directory"),
    };
    render_picker(
        frame,
        area,
        title,
        choices,
        selected,
        help,
        PickerNavigation {
            focus: wizard.focus,
            has_back: wizard.step != WizardStep::Profile,
        },
    );
}

#[derive(Debug, Clone, Copy)]
struct PickerNavigation {
    focus: WizardFocus,
    has_back: bool,
}

fn render_picker(
    frame: &mut Frame,
    area: Rect,
    title: &str,
    choices: Vec<String>,
    selected: usize,
    help: &[&str],
    navigation: PickerNavigation,
) {
    let popup = centered_rect(
        68,
        (choices.len() as u16 + help.len() as u16 + 6).clamp(9, 19),
        area,
    );
    frame.render_widget(Clear, popup);
    let lines = choices
        .into_iter()
        .enumerate()
        .map(|(index, choice)| {
            let marker = if index == selected && navigation.focus == WizardFocus::Content {
                "› "
            } else {
                "  "
            };
            let style = if index == selected && navigation.focus == WizardFocus::Content {
                Style::default().bg(Color::DarkGray).fg(Color::White)
            } else {
                Style::default()
            };
            Line::styled(format!("{marker}{choice}"), style)
        })
        .chain([Line::raw("")])
        .chain(
            help.iter()
                .map(|line| Line::styled(*line, Style::default().fg(Color::DarkGray))),
        )
        .chain([
            Line::raw(""),
            wizard_buttons(navigation.focus, navigation.has_back, "Next"),
        ])
        .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(lines)
            .block(Block::default().borders(Borders::ALL).title(title))
            .wrap(Wrap { trim: false }),
        popup,
    );
}

fn render_rename_editor(frame: &mut Frame, area: Rect, editor: &RenameEditor) {
    let popup = centered_rect(60, 7, area);
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(vec![
            Line::raw(format!("Session: {}", editor.session_id)),
            Line::raw(""),
            Line::styled(editor.title.clone(), Style::default().fg(Color::Cyan)),
            Line::styled("Enter save · Esc cancel", Style::default().fg(Color::Gray)),
        ])
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Rename session "),
        ),
        popup,
    );
}

fn render_confirmation(frame: &mut Frame, area: Rect, confirmation: &Confirmation) {
    let popup = centered_rect(
        72,
        match confirmation {
            Confirmation::DirtyLocal { repositories, .. } => {
                (repositories.len() as u16 + 8).clamp(10, 18)
            }
            Confirmation::CloseFailed { .. } => 12,
            Confirmation::Close { .. }
            | Confirmation::ForceDestroy { .. }
            | Confirmation::DeleteActive { .. }
            | Confirmation::DeleteArchived { .. } => 9,
        },
        area,
    );
    frame.render_widget(Clear, popup);
    let (title, lines) = match confirmation {
        Confirmation::DirtyLocal { repositories, .. } => {
            let mut lines = vec![
                Line::raw("The initial worker will include these uncommitted changes:"),
                Line::raw(""),
            ];
            lines.extend(repositories.iter().map(|repository| {
                Line::styled(repository.clone(), Style::default().fg(Color::Yellow))
            }));
            lines.extend([
                Line::raw(""),
                Line::raw("Pushes back to origin are rejected until the local checkout is clean."),
                Line::raw("Press y/Enter to continue, or n/Esc to cancel."),
            ]);
            (" Local repository has uncommitted changes ", lines)
        }
        Confirmation::Close { session_id } => (
            " Pause session? ",
            vec![
                Line::raw(format!("Session: {session_id}")),
                Line::raw(""),
                Line::raw("Hel will verify a recovery copy before destroying the target."),
                Line::raw("Press y/Enter to pause, or n/Esc to cancel."),
            ],
        ),
        Confirmation::DeleteArchived { session_id } => (
            " Permanently delete paused session? ",
            vec![
                Line::raw(format!("Session: {session_id}")),
                Line::raw(""),
                Line::raw("Hel will permanently delete the recovery archive and session record."),
                Line::raw("Press y/Enter to delete, or n/Esc to cancel."),
            ],
        ),
        Confirmation::CloseFailed { session_id, error } => (
            " Pause could not complete ",
            vec![
                Line::raw(format!("Session: {session_id}")),
                Line::raw(""),
                Line::styled(
                    format!("Pause failed: {error}"),
                    Style::default().fg(Color::Yellow),
                ),
                Line::raw(""),
                Line::raw("r retry pause · f force destroy · Esc cancel"),
            ],
        ),
        Confirmation::ForceDestroy { session_id, typed } => (
            " FORCE DESTROY · DATA MAY BE LOST ",
            vec![
                Line::raw(format!("Session: {session_id}")),
                Line::raw(""),
                Line::raw(format!("Type {FORCE_CONFIRMATION}, then press Enter:")),
                Line::styled(typed.clone(), Style::default().fg(Color::Red)),
            ],
        ),
        Confirmation::DeleteActive { session_id, typed } => (
            " DELETE ACTIVE SESSION · NO CHECKPOINT ",
            vec![
                Line::raw(format!("Session: {session_id}")),
                Line::raw(""),
                Line::raw(format!("Type {FORCE_CONFIRMATION}, then press Enter:")),
                Line::styled(typed.clone(), Style::default().fg(Color::Red)),
            ],
        ),
    };
    frame.render_widget(
        Paragraph::new(lines)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(Color::Red))
                    .title(title),
            )
            .wrap(Wrap { trim: true }),
        popup,
    );
}

fn centered_rect(width_percent: u16, height: u16, area: Rect) -> Rect {
    let vertical_margin = area.height.saturating_sub(height) / 2;
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(vertical_margin),
            Constraint::Length(height.min(area.height)),
            Constraint::Min(0),
        ])
        .split(area);
    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - width_percent) / 2),
            Constraint::Percentage(width_percent),
            Constraint::Min(0),
        ])
        .split(vertical[1]);
    horizontal[1]
}

fn move_index(index: &mut usize, len: usize, delta: isize) {
    if len == 0 {
        *index = 0;
        return;
    }
    *index = ((*index as isize + delta).rem_euclid(len as isize)) as usize;
}

fn nth_key<T>(map: &BTreeMap<String, T>, index: usize) -> String {
    map.keys()
        .nth(index)
        .cloned()
        .expect("wizard is only opened for non-empty configuration")
}

fn nth_bundle_key(config: &HelConfig, state: &HelState, index: usize) -> String {
    bundle_ids_by_recent_creation(config, state)
        .get(index)
        .expect("wizard is only opened for non-empty configuration")
        .to_string()
}

fn most_recent_configured_session<'a>(
    config: &HelConfig,
    state: &'a HelState,
) -> Option<&'a SessionRecord> {
    state
        .sessions
        .values()
        .filter(|session| {
            config.profiles.contains_key(&session.last_profile)
                && config.bundles.contains_key(&session.bundle_id)
                && config.targets.contains_key(&session.target_template_id)
        })
        .max_by_key(|session| {
            chrono::DateTime::parse_from_rfc3339(&session.created_at)
                .ok()
                .map(|timestamp| timestamp.timestamp_millis())
        })
}

fn bundle_ids_by_recent_creation<'a>(config: &'a HelConfig, state: &HelState) -> Vec<&'a str> {
    let mut latest_created_at = BTreeMap::<&str, i64>::new();
    for session in state.sessions.values() {
        if !config.bundles.contains_key(&session.bundle_id) {
            continue;
        }
        let Some(created_at) = chrono::DateTime::parse_from_rfc3339(&session.created_at)
            .ok()
            .map(|timestamp| timestamp.timestamp_millis())
        else {
            continue;
        };
        latest_created_at
            .entry(&session.bundle_id)
            .and_modify(|latest| *latest = (*latest).max(created_at))
            .or_insert(created_at);
    }

    let mut bundle_ids = config
        .bundles
        .keys()
        .map(String::as_str)
        .collect::<Vec<_>>();
    bundle_ids.sort_by(|left, right| {
        latest_created_at
            .get(right)
            .cmp(&latest_created_at.get(left))
            .then_with(|| left.cmp(right))
    });
    bundle_ids
}

fn harness_label(kind: HarnessKind) -> &'static str {
    match kind {
        HarnessKind::Codex => "Codex",
        HarnessKind::Claude => "Claude Code",
        HarnessKind::Kimi => "Kimi Code",
    }
}

fn target_label(target: &TargetTemplate) -> &'static str {
    match target {
        TargetTemplate::LocalBare => "raw localhost",
        TargetTemplate::LocalPodman { .. } => "local Podman",
        TargetTemplate::AppleContainer { .. } => "Apple container",
        TargetTemplate::AwsEc2 { .. } => "AWS EC2",
        TargetTemplate::SshBare { .. } => "named SSH machine",
        TargetTemplate::SshPodman { .. } => "Podman over SSH",
    }
}

fn is_bare_project_target(target: &TargetTemplate) -> bool {
    matches!(
        target,
        TargetTemplate::LocalBare | TargetTemplate::SshBare { .. }
    )
}

fn resource_allocation_label(
    allocation: Option<&SessionResourceAllocation>,
    error: Option<&str>,
) -> String {
    let allocation = match allocation {
        Some(SessionResourceAllocation::Container { cpus, memory_bytes }) => {
            format!(" · {cpus} CPU / {}", format_resource_bytes(*memory_bytes))
        }
        Some(SessionResourceAllocation::AwsEc2 {
            instance_type,
            vcpus,
            memory_bytes,
        }) => format!(
            " · {instance_type} · {vcpus} CPU / {}",
            format_resource_bytes(*memory_bytes)
        ),
        None => " · fixed/default resources".into(),
    };
    match error {
        Some(error) => format!("{allocation} · {error}"),
        None => allocation,
    }
}

fn mount_history_host(target: &TargetTemplate) -> Option<&str> {
    match target {
        TargetTemplate::LocalBare => None,
        TargetTemplate::LocalPodman { .. }
        | TargetTemplate::AppleContainer { .. }
        | TargetTemplate::AwsEc2 { .. } => Some("local"),
        TargetTemplate::SshPodman { ssh, .. } => Some(&ssh.host),
        TargetTemplate::SshBare { .. } => None,
    }
}

fn raw_project_context_id(project_directory: &str) -> String {
    let digest = Sha256::digest(project_directory.trim().as_bytes());
    let suffix = digest[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("remote-project-{suffix}")
}

fn is_paste_shortcut(key: KeyEvent) -> bool {
    key.code == KeyCode::Char('v')
        && key
            .modifiers
            .intersects(KeyModifiers::CONTROL | KeyModifiers::SUPER)
}

#[cfg(target_os = "macos")]
fn dashboard_accelerator(modifiers: KeyModifiers) -> bool {
    modifiers.contains(KeyModifiers::SUPER)
}

#[cfg(not(target_os = "macos"))]
fn dashboard_accelerator(modifiers: KeyModifiers) -> bool {
    modifiers.contains(KeyModifiers::CONTROL)
}

fn single_line_paste(pasted: &str) -> String {
    pasted.trim_matches(['\r', '\n']).replace(['\r', '\n'], " ")
}

fn default_resource_destination(
    target: &TargetTemplate,
    source: &std::path::Path,
    existing: &[AdditionalMount],
) -> std::path::PathBuf {
    let default = default_mount_destination(source, existing);
    let TargetTemplate::AwsEc2 { ssh_user, .. } = target else {
        return default;
    };
    let basename = default
        .file_name()
        .unwrap_or_else(|| std::ffi::OsStr::new("resource"));
    let home = if ssh_user == "root" {
        std::path::PathBuf::from("/root")
    } else {
        std::path::PathBuf::from("/home").join(ssh_user)
    };
    let base = home.join("hel-resources").join(basename);
    if !existing.iter().any(|resource| resource.destination == base) {
        return base;
    }
    for number in 2.. {
        let candidate = home
            .join("hel-resources")
            .join(format!("{}-{number}", basename.to_string_lossy()));
        if !existing
            .iter()
            .any(|resource| resource.destination == candidate)
        {
            return candidate;
        }
    }
    unreachable!()
}

fn apply_mount_completions(wizard: &mut MountWizard, prefix: &str, candidates: Vec<String>) {
    wizard
        .completion_cache
        .insert(prefix.to_owned(), candidates.clone());
    if let Some(completed) = path_completion(prefix, &candidates) {
        wizard.source = completed;
    }
    if candidates.len() > 1 {
        wizard.completion_candidates = candidates.into_iter().take(5).collect();
        wizard.completion_index = 0;
    } else {
        wizard.completion_candidates.clear();
    }
}

fn refresh_age(now: u64, refreshed: u64) -> String {
    if refreshed == 0 {
        return "unknown".into();
    }
    let age = now.saturating_sub(refreshed);
    let (value, unit) = if age < 60 {
        (age, "s")
    } else if age < 3_600 {
        (age / 60, "m")
    } else {
        (age / 3_600, "h")
    };
    format!("{value}{unit} ago")
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::hel_config::{
        CONFIG_VERSION, ContainerTemplate, HarnessProfile, ProjectBundle, ProjectRepository,
        SshConnection,
    };
    use crate::hel_state::{CheckpointMetadata, STATE_VERSION};
    use crate::hel_worker::WorkerSnapshot;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn ctrl_key(character: char) -> KeyEvent {
        KeyEvent::new(
            KeyCode::Char(character),
            if cfg!(target_os = "macos") {
                KeyModifiers::SUPER
            } else {
                KeyModifiers::CONTROL
            },
        )
    }

    fn mouse_in(kind: MouseEventKind, area: Rect) -> MouseEvent {
        MouseEvent {
            kind,
            column: area.x.saturating_add(1),
            row: area.y.saturating_add(1),
            modifiers: KeyModifiers::NONE,
        }
    }

    fn config() -> HelConfig {
        HelConfig {
            version: CONFIG_VERSION,
            profiles: BTreeMap::from([
                (
                    "claude-1".into(),
                    HarnessProfile {
                        context_window_bytes: None,
                        kind: HarnessKind::Claude,
                        home: PathBuf::from("/profiles/claude"),
                        executable: None,
                        environment: BTreeMap::new(),
                    },
                ),
                (
                    "codex-1".into(),
                    HarnessProfile {
                        context_window_bytes: None,
                        kind: HarnessKind::Codex,
                        home: PathBuf::from("/profiles/codex"),
                        executable: None,
                        environment: BTreeMap::new(),
                    },
                ),
                (
                    "codex-2".into(),
                    HarnessProfile {
                        context_window_bytes: None,
                        kind: HarnessKind::Codex,
                        home: PathBuf::from("/profiles/codex-two"),
                        executable: None,
                        environment: BTreeMap::new(),
                    },
                ),
            ]),
            bundles: BTreeMap::from([(
                "hel".into(),
                ProjectBundle {
                    primary_repo: "hel".into(),
                    repositories: vec![ProjectRepository {
                        id: "hel".into(),
                        github: Some("BrokkAi/hel".into()),
                        local: None,
                        destination: PathBuf::from("hel"),
                        git_ref: None,
                    }],
                },
            )]),
            targets: BTreeMap::from([(
                "podman".into(),
                TargetTemplate::LocalPodman {
                    container: ContainerTemplate {
                        image: "ubuntu:24.04".into(),
                        platform: None,
                        cpus: None,
                        memory: None,
                        environment: BTreeMap::new(),
                    },
                },
            )]),
        }
    }

    fn archived_session() -> SessionRecord {
        SessionRecord {
            id: "session-1".into(),
            title: "Raise the dead".into(),
            harness_kind: HarnessKind::Codex,
            last_profile: "codex-1".into(),
            bundle_id: "hel".into(),
            project_directory: None,
            target_template_id: "podman".into(),
            resource_allocation: None,
            additional_mounts: vec![],
            state: SessionState::Archived,
            target: None,
            native_session_id: Some("native-1".into()),
            acp_session_title: Some("ACP pretty name".into()),
            session_title_override: None,
            created_at: "2026-08-09T00:00:00Z".into(),
            updated_at: "2026-08-09T01:00:00Z".into(),
            last_viewed_event_sequence: 0,
            last_error: None,
            last_checkpoint_error: None,
            checkpoint: Some(CheckpointMetadata {
                archive_path: PathBuf::from("sessions/session-1.hel.zip"),
                sha256: "a".repeat(64),
                created_at: "2026-08-09T01:00:00Z".into(),
                event_sequence: 2,
            }),
        }
    }

    fn dashboard_with_session(mut session: SessionRecord) -> DashboardState {
        session.updated_at = "2026-08-09T01:00:00Z".into();
        DashboardState::new(
            config(),
            HelState {
                version: STATE_VERSION,
                sessions: BTreeMap::from([(session.id.clone(), session)]),
                mount_history: BTreeMap::new(),
            },
            BTreeMap::new(),
        )
    }

    #[test]
    fn pane_allocator_fills_complete_tables_then_gives_surplus_to_active() {
        let allocation = allocate_pane_heights(
            37,
            PaneHeights {
                active: 10,
                archived: 5,
                capacity: 5,
                quotas: 5,
            },
            PaneHeights {
                active: 4,
                archived: 4,
                capacity: 4,
                quotas: 4,
            },
            Focus::Quotas,
        );

        assert_eq!(
            allocation,
            PaneAllocation::Fits(PaneHeights {
                active: 19,
                archived: 5,
                capacity: 5,
                quotas: 5,
            })
        );
    }

    #[test]
    fn pane_allocator_grows_focus_then_active_when_tables_do_not_fit() {
        let full = PaneHeights {
            active: 20,
            archived: 10,
            capacity: 10,
            quotas: 10,
        };
        let minimized = PaneHeights {
            active: 5,
            archived: 5,
            capacity: 5,
            quotas: 5,
        };
        for (focus, expected) in [
            (
                Focus::Active,
                PaneHeights {
                    active: 24,
                    archived: 5,
                    capacity: 5,
                    quotas: 5,
                },
            ),
            (
                Focus::Archived,
                PaneHeights {
                    active: 19,
                    archived: 10,
                    capacity: 5,
                    quotas: 5,
                },
            ),
            (
                Focus::Capacity,
                PaneHeights {
                    active: 19,
                    archived: 5,
                    capacity: 10,
                    quotas: 5,
                },
            ),
            (
                Focus::Quotas,
                PaneHeights {
                    active: 19,
                    archived: 5,
                    capacity: 5,
                    quotas: 10,
                },
            ),
        ] {
            assert_eq!(
                allocate_pane_heights(42, full, minimized, focus),
                PaneAllocation::Fits(expected)
            );
        }
    }

    #[test]
    fn active_two_row_minimum_counts_header_rows_spacer_and_borders() {
        assert_eq!(active_pane_height(&[5, 5], 2), 14);
        assert_eq!(active_pane_height(&[5], 1), 8);
        assert_eq!(active_pane_height(&[], 0), 3);
    }

    #[test]
    fn pane_allocator_reports_content_sensitive_minimum_height() {
        let heights = PaneHeights {
            active: 5,
            archived: 5,
            capacity: 5,
            quotas: 5,
        };
        assert_eq!(
            allocate_pane_heights(22, heights, heights, Focus::Active),
            PaneAllocation::TooSmall {
                required_frame_height: 23,
            }
        );
        assert!(matches!(
            allocate_pane_heights(23, heights, heights, Focus::Active),
            PaneAllocation::Fits(_)
        ));
    }

    #[test]
    fn dashboard_replaces_too_short_layout_with_required_height() {
        let mut dashboard = DashboardState::new(config(), HelState::default(), BTreeMap::new());
        let mut terminal = Terminal::new(TestBackend::new(120, 16)).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw short dashboard");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("Terminal too small"));
        assert!(rendered.contains("at least 17 rows (currently 16)"));

        let mut terminal = Terminal::new(TestBackend::new(120, 17)).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw exact minimum dashboard");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(!rendered.contains("Terminal too small"));
        assert!(rendered.contains("Active"));
        assert!(rendered.contains("Profile Quotas"));
    }

    #[test]
    fn dashboard_uses_separate_hotkey_and_notice_rows_without_an_outer_border() {
        let mut dashboard = DashboardState::new(config(), HelState::default(), BTreeMap::new());
        dashboard.set_notice("Transient dashboard message");
        let mut terminal = Terminal::new(TestBackend::new(120, 24)).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw dashboard");
        let buffer = terminal.backend().buffer();
        let line = |y| {
            (buffer.area.x..buffer.area.right())
                .map(|x| buffer[(x, y)].symbol())
                .collect::<String>()
        };

        assert_eq!(buffer[(buffer.area.x, buffer.area.y)].symbol(), " ");
        assert!(line(buffer.area.y).contains("Welcome to Hel"));
        assert!(!line(buffer.area.y).contains("ACP sessions"));
        assert!(line(buffer.area.y + 1).contains("Active"));
        let accelerator = if cfg!(target_os = "macos") {
            "Cmd"
        } else {
            "Ctrl"
        };
        assert!(line(buffer.area.bottom() - 2).contains(&format!("{accelerator} for: [N]ew")));
        assert!(line(buffer.area.bottom() - 1).contains("Transient dashboard message"));
    }

    #[test]
    fn dashboard_actions_require_control_while_navigation_does_not() {
        let mut session = archived_session();
        session.state = SessionState::Running;
        let mut dashboard = dashboard_with_session(session);

        assert_eq!(
            dashboard.handle_key(key(KeyCode::Char('n'))),
            DashboardAction::None
        );
        assert!(matches!(dashboard.mode, Mode::Dashboard));
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Char('q'))),
            DashboardAction::None
        );
        assert_eq!(dashboard.handle_key(ctrl_key('k')), DashboardAction::None);
        assert_eq!(
            dashboard.handle_key(ctrl_key('u')),
            DashboardAction::RefreshQuotas
        );
        assert_eq!(
            dashboard.handle_key(ctrl_key('q')),
            DashboardAction::QuitDetach
        );

        assert_eq!(
            dashboard.handle_key(key(KeyCode::Tab)),
            DashboardAction::None
        );
        assert_eq!(dashboard.focus, Focus::Archived);
    }

    fn test_capacity_target() -> DeploymentCapacityTarget {
        DeploymentCapacityTarget {
            id: "local".into(),
            host: "local".into(),
            target_ids: vec!["podman".into()],
            kind: DeploymentCapacityKind::Host,
            local: true,
            probes: Vec::new(),
            probe_error: None,
        }
    }

    fn adapter_text_event(seq: u64, kind: &str, text: &str) -> SequencedEvent {
        SequencedEvent {
            seq,
            request_id: None,
            event: WorkerEvent::Adapter {
                kind: "session_update".into(),
                payload: serde_json::json!({
                    "type": "session_update",
                    "update": {
                        "sessionUpdate": kind,
                        "content": { "type": "text", "text": text }
                    }
                }),
            },
        }
    }

    fn apply_transcript(dashboard: &mut DashboardState, events: &[SequencedEvent]) {
        let snapshot: WorkerSnapshot = serde_json::from_value(serde_json::json!({
            "session_id": "session-1",
            "phase": "running",
            "latest_seq": events.last().map_or(0, |event| event.seq),
            "last_checkpoint_seq": null,
            "active_prompt": null,
            "config": {},
            "handled_requests": {}
        }))
        .unwrap();
        let chat = crate::hel_chat::ChatState::new(&snapshot, events);
        dashboard.apply_worker_events("session-1", events, 100);
        dashboard.apply_transcript("session-1", chat.transcript_snapshot());
    }

    #[test]
    fn archived_pane_includes_terminal_sessions_with_pane_local_navigation() {
        let mut running = archived_session();
        running.id = "session-0".into();
        running.state = SessionState::Running;
        let archived = archived_session();
        let mut lost = archived_session();
        lost.id = "session-2".into();
        lost.state = SessionState::Lost;
        let state = HelState {
            version: STATE_VERSION,
            sessions: BTreeMap::from([
                (running.id.clone(), running),
                (archived.id.clone(), archived),
                (lost.id.clone(), lost),
            ]),
            mount_history: BTreeMap::new(),
        };
        let (active, archived) = partition_sessions(state.sessions.values(), &BTreeMap::new());
        assert_eq!(
            active
                .iter()
                .map(|session| session.id.as_str())
                .collect::<Vec<_>>(),
            ["session-0"]
        );
        assert_eq!(
            archived
                .iter()
                .map(|session| session.id.as_str())
                .collect::<Vec<_>>(),
            ["session-1", "session-2"]
        );

        let mut dashboard = DashboardState::new(config(), state, BTreeMap::new());
        dashboard.handle_key(key(KeyCode::Down));
        assert_eq!(
            dashboard
                .selected_session()
                .map(|session| session.id.as_str()),
            Some("session-0")
        );
        dashboard.handle_key(key(KeyCode::Tab));
        assert_eq!(
            dashboard
                .selected_session()
                .map(|session| session.id.as_str()),
            Some("session-1")
        );
    }

    #[test]
    fn archived_sessions_are_ordered_by_checkpoint_time_descending() {
        let mut oldest = archived_session();
        oldest.id = "session-z".into();
        oldest.checkpoint.as_mut().unwrap().created_at = "2026-08-09T01:00:00Z".into();
        let mut newest = archived_session();
        newest.id = "session-y".into();
        newest.checkpoint.as_mut().unwrap().created_at = "2026-08-09T00:30:00-02:00".into();
        let mut without_checkpoint = archived_session();
        without_checkpoint.id = "session-a".into();
        without_checkpoint.checkpoint = None;

        let (_, archived) =
            partition_sessions([&without_checkpoint, &oldest, &newest], &BTreeMap::new());

        assert_eq!(
            archived
                .iter()
                .map(|session| session.id.as_str())
                .collect::<Vec<_>>(),
            ["session-y", "session-z", "session-a"]
        );
    }

    #[test]
    fn active_sessions_are_sorted_by_most_recent_agent_text() {
        let active_session = |id: &str| {
            let mut session = archived_session();
            session.id = id.into();
            session.state = SessionState::Running;
            session
        };
        let sessions = [
            active_session("session-a"),
            active_session("session-b"),
            active_session("session-c"),
        ];
        let state = HelState {
            version: STATE_VERSION,
            sessions: sessions
                .into_iter()
                .map(|session| (session.id.clone(), session))
                .collect(),
            mount_history: BTreeMap::new(),
        };
        let mut dashboard = DashboardState::new(config(), state, BTreeMap::new());
        let ordered_ids = |dashboard: &DashboardState| {
            dashboard
                .ordered_sessions()
                .into_iter()
                .map(|session| session.id.clone())
                .collect::<Vec<_>>()
        };

        dashboard.apply_worker_events(
            "session-b",
            &[adapter_text_event(1, "agent_message_chunk", "second")],
            100,
        );
        assert_eq!(
            ordered_ids(&dashboard),
            ["session-b", "session-a", "session-c"]
        );

        dashboard.apply_worker_events(
            "session-c",
            &[adapter_text_event(
                1,
                "agent_thought_chunk",
                "later thought",
            )],
            200,
        );
        assert_eq!(
            ordered_ids(&dashboard),
            ["session-b", "session-a", "session-c"]
        );

        dashboard.apply_worker_events(
            "session-a",
            &[adapter_text_event(1, "agent_message_chunk", "newest")],
            300,
        );
        assert_eq!(
            ordered_ids(&dashboard),
            ["session-a", "session-b", "session-c"]
        );

        dashboard.apply_worker_events(
            "session-b",
            &[SequencedEvent {
                seq: 2,
                request_id: None,
                event: WorkerEvent::Adapter {
                    kind: "session_update".into(),
                    payload: serde_json::json!({
                        "type": "session_update",
                        "update": { "sessionUpdate": "tool_call", "title": "later tool" }
                    }),
                },
            }],
            400,
        );
        assert_eq!(
            ordered_ids(&dashboard),
            ["session-a", "session-b", "session-c"]
        );
    }

    #[test]
    fn unread_badge_counts_messages_not_chunks_and_clears_through_viewed_sequence() {
        let mut session = archived_session();
        session.state = SessionState::Running;
        let mut dashboard = dashboard_with_session(session);
        dashboard.apply_worker_events(
            "session-1",
            &[
                adapter_text_event(1, "agent_message_chunk", "first "),
                adapter_text_event(2, "agent_message_chunk", "message"),
                adapter_text_event(3, "agent_thought_chunk", "thinking"),
                adapter_text_event(4, "agent_message_chunk", "second message"),
            ],
            100,
        );

        let detail = dashboard.session_details.get("session-1").unwrap();
        assert_eq!(detail.unread_agent_message_sequences, [1, 4]);
        let badge = session_name_line("session".into(), 2);
        assert_eq!(badge.spans[1].content.as_ref(), "  2 unread");
        assert_eq!(
            badge.spans[1].style,
            Style::default()
                .fg(Color::Blue)
                .add_modifier(Modifier::BOLD)
        );

        let mut state = dashboard.state.clone();
        state
            .sessions
            .get_mut("session-1")
            .unwrap()
            .last_viewed_event_sequence = 1;
        dashboard.set_state(state);
        assert_eq!(
            dashboard.session_details["session-1"].unread_agent_message_sequences,
            [4]
        );

        let mut state = dashboard.state.clone();
        state
            .sessions
            .get_mut("session-1")
            .unwrap()
            .last_viewed_event_sequence = 4;
        dashboard.set_state(state);
        assert!(
            dashboard.session_details["session-1"]
                .unread_agent_message_sequences
                .is_empty()
        );
    }

    #[test]
    fn unrelated_adapter_updates_do_not_truncate_the_streamed_agent_response() {
        let mut session = archived_session();
        session.state = SessionState::Running;
        let mut dashboard = dashboard_with_session(session);
        let unrelated = SequencedEvent {
            seq: 2,
            request_id: None,
            event: WorkerEvent::Adapter {
                kind: "usage_update".into(),
                payload: serde_json::json!({
                    "type": "usage_update",
                    "used": 42
                }),
            },
        };

        dashboard.apply_worker_events(
            "session-1",
            &[
                adapter_text_event(1, "agent_message_chunk", "The container lacked "),
                unrelated,
                adapter_text_event(3, "agent_message_chunk", "uv, so validation used Python "),
                adapter_text_event(4, "agent_message_chunk", "3 directly."),
            ],
            100,
        );

        assert_eq!(
            dashboard.session_details["session-1"]
                .last_agent_message
                .as_deref(),
            Some("The container lacked uv, so validation used Python 3 directly.")
        );
    }

    #[test]
    fn new_session_wizard_returns_all_three_choices() {
        let mut dashboard = DashboardState::new(config(), HelState::default(), BTreeMap::new());
        assert_eq!(dashboard.handle_key(ctrl_key('n')), DashboardAction::None);
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Down)),
            DashboardAction::None
        );
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::None
        );
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::None
        );
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::None
        );
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::CreateSession {
                profile_id: "codex-1".into(),
                bundle_id: "hel".into(),
                project_directory: None,
                target_template_id: "podman".into(),
                additional_mounts: vec![],
                allow_dirty_local: false,
                resource_allocation: Some(SessionResourceAllocation::Container {
                    cpus: BASELINE_CPUS,
                    memory_bytes: BASELINE_MEMORY_BYTES,
                }),
            }
        );
    }

    #[test]
    fn new_session_wizard_renders_and_focuses_explicit_navigation_buttons() {
        let mut dashboard = DashboardState::new(config(), HelState::default(), BTreeMap::new());
        dashboard.handle_key(ctrl_key('n'));
        let mut terminal = Terminal::new(TestBackend::new(100, 24)).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw wizard");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("Cancel"));
        assert!(rendered.contains("Next"));

        dashboard.handle_key(key(KeyCode::Tab));
        let Mode::New(wizard) = &dashboard.mode else {
            panic!("expected new-session wizard");
        };
        assert_eq!(wizard.focus, WizardFocus::Cancel);
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::None
        );
        assert!(matches!(dashboard.mode, Mode::Dashboard));
    }

    #[test]
    fn opening_session_wizards_prefetches_all_aws_sizes() {
        let aws_target = || TargetTemplate::AwsEc2 {
            aws_profile: None,
            region: "us-east-1".into(),
            launch_template: "hel".into(),
            launch_template_version: None,
            ssh_user: "ubuntu".into(),
            address_source: crate::hel_config::AwsAddressSource::PublicIp,
            identity_file: None,
            ssh_args: Vec::new(),
        };
        let mut config = config();
        config.targets.insert("aws-a".into(), aws_target());
        config.targets.insert("aws-b".into(), aws_target());
        let mut dashboard =
            DashboardState::new(config.clone(), HelState::default(), BTreeMap::new());

        assert_eq!(
            dashboard.handle_key(ctrl_key('n')),
            DashboardAction::ResolveAwsResourceOptions {
                target_template_ids: vec!["aws-a".into(), "aws-b".into()],
            }
        );
        let aws_b_options = vec![SessionResourceAllocation::AwsEc2 {
            instance_type: "m7i.2xlarge".into(),
            vcpus: 8,
            memory_bytes: 32 * 1024 * 1024 * 1024,
        }];
        dashboard.apply_aws_resource_options("aws-b", Ok(aws_b_options.clone()));
        let Mode::New(wizard) = &dashboard.mode else {
            panic!("expected new-session wizard");
        };
        assert_eq!(wizard.aws_options["aws-b"], aws_b_options);

        let mut dashboard = DashboardState::new(
            config,
            HelState {
                version: STATE_VERSION,
                sessions: BTreeMap::from([("session-1".into(), archived_session())]),
                mount_history: BTreeMap::new(),
            },
            BTreeMap::new(),
        );
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::ResolveAwsResourceOptions {
                target_template_ids: vec!["aws-a".into(), "aws-b".into()],
            }
        );
    }

    #[test]
    fn new_session_can_request_a_repository_when_no_bundle_exists() {
        let mut config = config();
        config.bundles.clear();
        let mut dashboard = DashboardState::new(config, HelState::default(), BTreeMap::new());
        dashboard.handle_key(ctrl_key('n'));
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Enter));
        for character in "example/new-repo".chars() {
            dashboard.handle_key(key(KeyCode::Char(character)));
        }
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::CreateBundle {
                source: "example/new-repo".into(),
            }
        );
    }

    #[test]
    fn bare_ssh_new_session_selects_target_then_raw_project_without_attachments() {
        let mut config = config();
        config.targets = BTreeMap::from([(
            "machine".into(),
            TargetTemplate::SshBare {
                ssh: SshConnection {
                    host: "builder.example.com".into(),
                    user: None,
                    identity_file: None,
                    extra_args: Vec::new(),
                },
                workspace_prefix: ".local/share/hel/workspaces".into(),
            },
        )]);
        let mut state = HelState::default();
        state
            .remember_project_directory("builder.example.com", std::path::Path::new("/srv/recent"));
        state.remember_project_directory("builder.example.com", std::path::Path::new("/srv/older"));
        let mut dashboard = DashboardState::new(config, state, BTreeMap::new());

        dashboard.handle_key(ctrl_key('n'));
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Enter));
        let Mode::New(wizard) = &dashboard.mode else {
            panic!("expected new-session wizard")
        };
        assert_eq!(wizard.project_directory, "/srv/older");
        dashboard.handle_key(key(KeyCode::Down));
        let Mode::New(wizard) = &dashboard.mode else {
            panic!("expected new-session wizard")
        };
        assert_eq!(wizard.project_directory, "/srv/recent");
        while let Mode::New(wizard) = &dashboard.mode
            && !wizard.project_directory.is_empty()
        {
            dashboard.handle_key(key(KeyCode::Backspace));
        }
        for character in "/srv/project".chars() {
            dashboard.handle_key(key(KeyCode::Char(character)));
        }
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::ValidateProjectDirectory {
                target_template_id: "machine".into(),
                directory: "/srv/project".into(),
            }
        );
        dashboard.apply_project_directory_validation(
            "/srv/project",
            Err(
                "remote project directory /srv/project does not exist or is not a directory".into(),
            ),
        );

        let mut terminal = Terminal::new(TestBackend::new(100, 28)).unwrap();
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .unwrap();
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("Error: remote project directory /srv/project does not exist"));

        dashboard.apply_project_directory_validation("/srv/project", Ok(()));

        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .unwrap();
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("Project: /srv/project"));
        assert!(!rendered.contains("Attached directories"));

        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::CreateSession {
                profile_id: "claude-1".into(),
                bundle_id: raw_project_context_id("/srv/project"),
                project_directory: Some("/srv/project".into()),
                target_template_id: "machine".into(),
                additional_mounts: Vec::new(),
                allow_dirty_local: false,
                resource_allocation: None,
            }
        );
    }

    #[test]
    fn raw_localhost_uses_local_project_history_and_warns_for_kimi() {
        let mut config = config();
        config.profiles = BTreeMap::from([(
            "kimi".into(),
            HarnessProfile {
                context_window_bytes: None,
                kind: HarnessKind::Kimi,
                home: PathBuf::from("/profiles/kimi"),
                executable: None,
                environment: BTreeMap::new(),
            },
        )]);
        config.targets = BTreeMap::from([("raw-localhost".into(), TargetTemplate::LocalBare)]);
        let mut state = HelState::default();
        state.remember_project_directory("local", std::path::Path::new("/home/me/project"));
        let mut dashboard = DashboardState::new(config, state, BTreeMap::new());

        dashboard.handle_key(ctrl_key('n'));
        let mut terminal = Terminal::new(TestBackend::new(140, 28)).unwrap();
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .unwrap();
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("DANGER"));

        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Enter));
        let Mode::New(wizard) = &dashboard.mode else {
            panic!("expected local project directory step")
        };
        assert_eq!(wizard.project_directory, "/home/me/project");
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::ValidateProjectDirectory {
                target_template_id: "raw-localhost".into(),
                directory: "/home/me/project".into(),
            }
        );
        dashboard.apply_project_directory_validation("/home/me/project", Ok(()));
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::CreateSession {
                profile_id: "kimi".into(),
                bundle_id: raw_project_context_id("/home/me/project"),
                project_directory: Some("/home/me/project".into()),
                target_template_id: "raw-localhost".into(),
                additional_mounts: Vec::new(),
                allow_dirty_local: false,
                resource_allocation: None,
            }
        );
    }

    #[test]
    fn bracketed_paste_populates_dashboard_text_editors() {
        let mut session = archived_session();
        session.state = SessionState::Running;
        let mut dashboard = dashboard_with_session(session);

        dashboard.handle_key(ctrl_key('r'));
        let Mode::Rename(editor) = &mut dashboard.mode else {
            panic!("expected rename editor")
        };
        editor.title.clear();
        dashboard.handle_paste("pasted title\n");

        let Mode::Rename(editor) = &dashboard.mode else {
            panic!("expected rename editor")
        };
        assert_eq!(editor.title, "pasted title");
    }

    #[test]
    fn delete_active_is_immediate_without_assistant_messages_and_guarded_after_one() {
        let mut session = archived_session();
        session.state = SessionState::Running;
        session.checkpoint = None;
        let mut dashboard = dashboard_with_session(session);

        assert_eq!(
            dashboard.handle_key(key(KeyCode::Delete)),
            DashboardAction::DeleteActive {
                session_id: "session-1".into()
            }
        );

        dashboard.apply_worker_events(
            "session-1",
            &[adapter_text_event(1, "agent_message_chunk", "hello")],
            1,
        );
        assert_eq!(dashboard.handle_key(ctrl_key('d')), DashboardAction::None);
        assert!(matches!(
            dashboard.mode,
            Mode::Confirm(Confirmation::DeleteActive { .. })
        ));
    }

    #[test]
    fn new_session_bundles_are_ordered_by_latest_session_creation() {
        let mut config = config();
        let bundle = config.bundles["hel"].clone();
        config.bundles.insert("alpha-unused".into(), bundle.clone());
        config.bundles.insert("zebra-recent".into(), bundle);

        let mut older = archived_session();
        older.id = "older".into();
        older.created_at = "2026-08-10T12:00:00Z".into();
        let mut recent = archived_session();
        recent.id = "recent".into();
        recent.bundle_id = "zebra-recent".into();
        recent.created_at = "2026-08-11T12:00:00Z".into();
        let state = HelState {
            version: STATE_VERSION,
            sessions: BTreeMap::from([(older.id.clone(), older), (recent.id.clone(), recent)]),
            mount_history: BTreeMap::new(),
        };
        assert_eq!(
            bundle_ids_by_recent_creation(&config, &state),
            vec!["zebra-recent", "hel", "alpha-unused"]
        );

        let mut dashboard = DashboardState::new(config, state, BTreeMap::new());
        dashboard.handle_key(ctrl_key('n'));
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Enter));
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::CreateSession {
                profile_id: "codex-1".into(),
                bundle_id: "zebra-recent".into(),
                project_directory: None,
                target_template_id: "podman".into(),
                additional_mounts: vec![],
                allow_dirty_local: false,
                resource_allocation: Some(SessionResourceAllocation::Container {
                    cpus: BASELINE_CPUS,
                    memory_bytes: BASELINE_MEMORY_BYTES,
                }),
            }
        );
    }

    #[test]
    fn new_session_defaults_to_the_most_recent_configured_choices() {
        let mut config = config();
        config
            .bundles
            .insert("recent-project".into(), config.bundles["hel"].clone());
        config
            .targets
            .insert("recent-target".into(), config.targets["podman"].clone());
        let mut recent = archived_session();
        recent.last_profile = "codex-1".into();
        recent.bundle_id = "recent-project".into();
        recent.target_template_id = "recent-target".into();
        recent.created_at = "2026-08-12T12:00:00Z".into();
        let state = HelState {
            version: STATE_VERSION,
            sessions: BTreeMap::from([(recent.id.clone(), recent)]),
            mount_history: BTreeMap::new(),
        };
        let mut dashboard = DashboardState::new(config, state, BTreeMap::new());

        dashboard.handle_key(ctrl_key('n'));
        let Mode::New(wizard) = &dashboard.mode else {
            panic!("expected new-session wizard");
        };
        assert_eq!(
            nth_key(&dashboard.config.profiles, wizard.profile),
            "codex-1"
        );
        assert_eq!(
            nth_bundle_key(&dashboard.config, &dashboard.state, wizard.bundle),
            "recent-project"
        );
        assert_eq!(
            nth_key(&dashboard.config.targets, wizard.target),
            "recent-target"
        );
    }

    #[test]
    fn new_session_mount_wizard_adds_mount_and_preserves_typed_source() {
        let mut dashboard = DashboardState::new(config(), HelState::default(), BTreeMap::new());
        dashboard.handle_key(ctrl_key('n'));
        dashboard.handle_key(key(KeyCode::Down));
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::BackTab));
        dashboard.handle_key(key(KeyCode::Enter));
        let mut terminal = Terminal::new(TestBackend::new(120, 30)).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw resource wizard");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("Source: ▏"));
        assert!(rendered.contains("Add directory"));
        for character in "/opt/cache".chars() {
            dashboard.handle_key(key(KeyCode::Char(character)));
        }
        dashboard.apply_mount_source_completions("/opt/ca", vec!["/opt/cache/".into()]);
        dashboard.handle_key(key(KeyCode::Enter));
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::ValidateMountSource {
                target_template_id: "podman".into(),
                source: "/opt/cache".into(),
            }
        );
        dashboard.apply_mount_source_validation("/opt/cache", Ok(()));
        dashboard.handle_key(key(KeyCode::BackTab));

        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::CreateSession {
                profile_id: "codex-1".into(),
                bundle_id: "hel".into(),
                project_directory: None,
                target_template_id: "podman".into(),
                additional_mounts: vec![AdditionalMount {
                    source: "/opt/cache".into(),
                    destination: "/mnt/cache".into(),
                }],
                allow_dirty_local: false,
                resource_allocation: Some(SessionResourceAllocation::Container {
                    cpus: BASELINE_CPUS,
                    memory_bytes: BASELINE_MEMORY_BYTES,
                }),
            }
        );
    }

    #[test]
    fn directory_completion_is_bounded_and_keyboard_selectable() {
        let mut dashboard = DashboardState::new(config(), HelState::default(), BTreeMap::new());
        dashboard.handle_key(ctrl_key('n'));
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::BackTab));
        dashboard.handle_key(key(KeyCode::Enter));
        for character in "/opt/".chars() {
            dashboard.handle_key(key(KeyCode::Char(character)));
        }
        let candidates = (0..12)
            .map(|index| format!("/opt/directory-{index}/"))
            .collect::<Vec<_>>();
        dashboard.apply_mount_source_completions("/opt/", candidates);

        let Mode::New(wizard) = &dashboard.mode else {
            panic!("expected directory editor");
        };
        assert_eq!(wizard.mounts.completion_candidates.len(), 5);
        dashboard.handle_key(key(KeyCode::Down));
        dashboard.handle_key(key(KeyCode::Enter));
        let Mode::New(wizard) = &dashboard.mode else {
            panic!("expected directory editor");
        };
        assert_eq!(wizard.mounts.source, "/opt/directory-1/");

        let mut terminal = Terminal::new(TestBackend::new(100, 30)).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw bounded directory editor");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("Add directory"));
        assert!(rendered.contains("Cancel"));
    }

    #[test]
    fn failed_source_validation_does_not_add_new_or_resume_mounts() {
        let mut dashboard = DashboardState::new(config(), HelState::default(), BTreeMap::new());
        dashboard.handle_key(ctrl_key('n'));
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::BackTab));
        dashboard.handle_key(key(KeyCode::Enter));
        for character in "/missing".chars() {
            dashboard.handle_key(key(KeyCode::Char(character)));
        }
        dashboard.handle_key(key(KeyCode::Enter));
        assert!(matches!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::ValidateMountSource { .. }
        ));
        dashboard.apply_mount_source_validation(
            "/missing",
            Err("source path /missing does not exist or is not a directory".into()),
        );
        let Mode::New(wizard) = &dashboard.mode else {
            panic!("expected new-session resource dialog");
        };
        assert!(wizard.mounts.mounts.is_empty());
        assert_eq!(wizard.mounts.source, "/missing");
        assert_eq!(wizard.mounts.focus, MountFocus::Source);
        assert_eq!(
            wizard.mounts.error.as_deref(),
            Some("source path /missing does not exist or is not a directory")
        );

        let mut dashboard = dashboard_with_session(archived_session());
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::BackTab));
        dashboard.handle_key(key(KeyCode::Enter));
        for character in "/missing".chars() {
            dashboard.handle_key(key(KeyCode::Char(character)));
        }
        dashboard.handle_key(key(KeyCode::Enter));
        assert!(matches!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::ValidateMountSource { .. }
        ));
        dashboard.apply_mount_source_validation(
            "/missing",
            Err("source path /missing does not exist or is not a directory".into()),
        );
        let Mode::Resume(wizard) = &dashboard.mode else {
            panic!("expected resume resource dialog");
        };
        assert!(wizard.mounts.mounts.is_empty());
        assert_eq!(wizard.mounts.source, "/missing");
        assert_eq!(wizard.mounts.focus, MountFocus::Source);
    }

    #[test]
    fn resume_can_convert_to_another_harness() {
        let mut dashboard = dashboard_with_session(archived_session());
        dashboard.set_deployment_capacity_targets(vec![test_capacity_target()]);
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Up));
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Enter));
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::ResumeSession {
                session_id: "session-1".into(),
                profile_id: "claude-1".into(),
                target_template_id: "podman".into(),
                additional_mounts: vec![],
                resource_allocation: Some(SessionResourceAllocation::Container {
                    cpus: BASELINE_CPUS,
                    memory_bytes: BASELINE_MEMORY_BYTES,
                }),
            }
        );
    }

    #[test]
    fn resume_defaults_to_the_session_profile() {
        let mut dashboard = dashboard_with_session(archived_session());
        dashboard.handle_key(key(KeyCode::Enter));

        let Mode::Resume(wizard) = &dashboard.mode else {
            panic!("expected resume wizard");
        };
        let profiles = dashboard.compatible_profiles(&wizard.session_id);
        assert_eq!(profiles[wizard.profile].0, "codex-1");
    }

    #[test]
    fn resume_defaults_to_the_previously_used_target() {
        let mut dashboard = dashboard_with_session(archived_session());
        let target = dashboard.config.targets["podman"].clone();
        dashboard.config.targets.insert("alternate".into(), target);

        dashboard.handle_key(key(KeyCode::Enter));

        let Mode::Resume(wizard) = &dashboard.mode else {
            panic!("expected resume wizard");
        };
        assert_eq!(nth_key(&dashboard.config.targets, wizard.target), "podman");
    }

    #[test]
    fn resume_dialog_attaches_an_additional_resource() {
        let mut dashboard = dashboard_with_session(archived_session());
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::BackTab));
        dashboard.handle_key(key(KeyCode::Enter));
        for character in "/opt/cache".chars() {
            dashboard.handle_key(key(KeyCode::Char(character)));
        }
        dashboard.handle_key(key(KeyCode::Enter));
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::ValidateMountSource {
                target_template_id: "podman".into(),
                source: "/opt/cache".into(),
            }
        );
        dashboard.apply_mount_source_validation("/opt/cache", Ok(()));
        dashboard.handle_key(key(KeyCode::BackTab));

        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::ResumeSession {
                session_id: "session-1".into(),
                profile_id: "codex-1".into(),
                target_template_id: "podman".into(),
                additional_mounts: vec![AdditionalMount {
                    source: "/opt/cache".into(),
                    destination: "/mnt/cache".into(),
                }],
                resource_allocation: Some(SessionResourceAllocation::Container {
                    cpus: BASELINE_CPUS,
                    memory_bytes: BASELINE_MEMORY_BYTES,
                }),
            }
        );
    }

    #[test]
    fn resume_dialog_can_remove_a_previous_resource() {
        let mut session = archived_session();
        session.additional_mounts = vec![AdditionalMount {
            source: "/opt/old-cache".into(),
            destination: "/mnt/old-cache".into(),
        }];
        let mut dashboard = dashboard_with_session(session);
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Tab));
        dashboard.handle_key(key(KeyCode::Delete));

        let Mode::Resume(wizard) = &dashboard.mode else {
            panic!("expected resume resource dialog");
        };
        assert!(wizard.mounts.mounts.is_empty());
    }

    #[test]
    fn resume_review_edits_an_existing_attached_directory_in_place() {
        let mut session = archived_session();
        session.additional_mounts = vec![AdditionalMount {
            source: "/opt/cache".into(),
            destination: "/mnt/cache".into(),
        }];
        let mut dashboard = dashboard_with_session(session);
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Tab));
        dashboard.handle_key(key(KeyCode::Enter));

        let Mode::Resume(wizard) = &dashboard.mode else {
            panic!("expected attached-directory editor");
        };
        assert_eq!(wizard.mounts.source, "/opt/cache");
        assert_eq!(wizard.mounts.destination, "/mnt/cache");
        assert_eq!(wizard.mounts.editing_mount, Some(0));

        dashboard.handle_key(key(KeyCode::Enter));
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::ValidateMountSource {
                target_template_id: "podman".into(),
                source: "/opt/cache".into(),
            }
        );
        dashboard.apply_mount_source_validation("/opt/cache", Ok(()));
        let Mode::Resume(wizard) = &dashboard.mode else {
            panic!("expected resume review");
        };
        assert_eq!(wizard.step, WizardStep::Review);
        assert_eq!(wizard.mounts.mounts.len(), 1);
    }

    #[test]
    fn aws_resource_destinations_default_under_the_ssh_users_home() {
        let target = TargetTemplate::AwsEc2 {
            aws_profile: None,
            region: "us-east-1".into(),
            launch_template: "hel".into(),
            launch_template_version: None,
            ssh_user: "ubuntu".into(),
            address_source: crate::hel_config::AwsAddressSource::PublicIp,
            identity_file: None,
            ssh_args: Vec::new(),
        };

        assert_eq!(
            default_resource_destination(&target, std::path::Path::new("/opt/cache"), &[]),
            std::path::PathBuf::from("/home/ubuntu/hel-resources/cache")
        );
    }

    #[test]
    fn rename_uses_acp_title_as_the_initial_value() {
        let mut dashboard = dashboard_with_session(archived_session());
        dashboard.handle_key(ctrl_key('r'));
        for character in " v2".chars() {
            dashboard.handle_key(key(KeyCode::Char(character)));
        }
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::RenameSession {
                session_id: "session-1".into(),
                title: "ACP pretty name v2".into(),
            }
        );
    }

    #[test]
    fn session_name_prefers_override_then_acp_title_then_hel_uuid() {
        let mut session = archived_session();
        assert_eq!(session_name(&session), "ACP pretty name");

        session.acp_session_title = None;
        assert_eq!(session_name(&session), "session-1");

        session.session_title_override = Some("My name".into());
        assert_eq!(session_name(&session), "My name");

        session.session_title_override = None;
        session.native_session_id = None;
        assert_eq!(session_name(&session), "session-1");
        assert_ne!(session_name(&session), session.title);
    }

    #[test]
    fn dashboard_navigation_keeps_four_distinct_panes() {
        let mut active = archived_session();
        active.id = "session-0".into();
        active.state = SessionState::Running;
        let archived = archived_session();
        let mut dashboard = DashboardState::new(
            config(),
            HelState {
                version: STATE_VERSION,
                sessions: BTreeMap::from([
                    (active.id.clone(), active),
                    (archived.id.clone(), archived),
                ]),
                mount_history: BTreeMap::new(),
            },
            BTreeMap::new(),
        );
        dashboard.set_deployment_capacity_targets(vec![test_capacity_target()]);

        assert_eq!(dashboard.focus, Focus::Active);
        assert_eq!(dashboard.selected_session().unwrap().id, "session-0");
        dashboard.handle_key(key(KeyCode::Down));
        assert_eq!(dashboard.selected_session().unwrap().id, "session-0");

        dashboard.handle_key(key(KeyCode::Tab));
        assert_eq!(dashboard.focus, Focus::Archived);
        assert_eq!(dashboard.selected_session().unwrap().id, "session-1");
        dashboard.handle_key(key(KeyCode::Down));
        assert_eq!(dashboard.selected_session().unwrap().id, "session-1");

        dashboard.handle_key(key(KeyCode::Tab));
        assert_eq!(dashboard.focus, Focus::Capacity);
        assert!(dashboard.selected_session().is_none());
        dashboard.handle_key(key(KeyCode::Tab));
        assert_eq!(dashboard.focus, Focus::Quotas);
        assert!(dashboard.selected_session().is_none());
        dashboard.handle_key(key(KeyCode::Tab));
        assert_eq!(dashboard.focus, Focus::Active);

        dashboard.handle_key(key(KeyCode::BackTab));
        assert_eq!(dashboard.focus, Focus::Quotas);
        dashboard.handle_key(key(KeyCode::BackTab));
        assert_eq!(dashboard.focus, Focus::Capacity);
        dashboard.handle_key(key(KeyCode::BackTab));
        assert_eq!(dashboard.focus, Focus::Archived);
    }

    #[test]
    fn capacity_pane_renders_grouped_host_load_without_sample_clock() {
        let mut dashboard = DashboardState::new(config(), HelState::default(), BTreeMap::new());
        let mut target = test_capacity_target();
        target.target_ids = vec!["podman".into(), "mac-container".into()];
        dashboard.set_deployment_capacity_targets(vec![target]);
        dashboard.apply_deployment_capacity(
            "local",
            Ok(Some(DeploymentCapacityUsage {
                cpu_percent: Some(37),
                memory_used_bytes: 3,
                memory_total_bytes: 4,
                logical_cores: 8,
                disk_total_bytes: None,
            })),
            1,
        );
        dashboard.apply_deployment_capacity("local", Err("probe failed".into()), 2);
        let backend = TestBackend::new(120, 40);
        let mut terminal = Terminal::new(backend).expect("terminal");

        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw dashboard");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(rendered.contains("podman, mac-container"));
        assert!(rendered.contains("37% CPU · 75% RAM"));
        assert!(!rendered.contains("Sample"));
        assert!(!rendered.contains("stale"));
    }

    #[test]
    fn mouse_wheel_scrolls_the_hovered_pane_without_changing_focus() {
        let sessions = (0..5)
            .map(|index| {
                let mut session = archived_session();
                session.id = format!("session-{index}");
                session.state = SessionState::Running;
                (session.id.clone(), session)
            })
            .collect();
        let mut dashboard = DashboardState::new(
            config(),
            HelState {
                version: STATE_VERSION,
                sessions,
                mount_history: BTreeMap::new(),
            },
            BTreeMap::new(),
        );
        let mut terminal = Terminal::new(TestBackend::new(120, 40)).expect("test terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw pane hitboxes");
        let pane_areas = dashboard.pane_areas.expect("dashboard pane hitboxes");

        dashboard.handle_mouse(mouse_in(MouseEventKind::ScrollDown, pane_areas[0]));
        assert_eq!(dashboard.session_index, 3);
        dashboard.handle_mouse(mouse_in(MouseEventKind::ScrollUp, pane_areas[0]));
        assert_eq!(dashboard.session_index, 0);

        assert_eq!(dashboard.focus, Focus::Active);
        dashboard.handle_mouse(mouse_in(MouseEventKind::ScrollDown, pane_areas[3]));
        assert_eq!(dashboard.quota_index, 2);
        assert_eq!(dashboard.session_index, 0);
        assert_eq!(dashboard.focus, Focus::Active);
    }

    #[test]
    fn newly_ready_session_can_be_selected_after_state_refresh() {
        let mut new_session = archived_session();
        new_session.id = "new-session".into();
        new_session.state = SessionState::Running;
        let mut other = archived_session();
        other.id = "other".into();
        other.state = SessionState::Running;
        let mut dashboard = DashboardState::new(
            config(),
            HelState {
                version: STATE_VERSION,
                sessions: BTreeMap::from([(other.id.clone(), other)]),
                mount_history: BTreeMap::new(),
            },
            BTreeMap::new(),
        );
        dashboard.focus = Focus::Archived;

        let mut refreshed = dashboard.state.clone();
        refreshed
            .sessions
            .insert(new_session.id.clone(), new_session);
        dashboard.set_state(refreshed);
        dashboard.select_active_session("new-session");

        assert_eq!(dashboard.focus, Focus::Active);
        assert_eq!(dashboard.selected_session().unwrap().id, "new-session");
    }

    #[test]
    fn closing_last_active_session_moves_focus_to_archived_then_cycles_all_panes() {
        let mut session = archived_session();
        session.state = SessionState::Running;
        let mut dashboard = dashboard_with_session(session);
        dashboard.set_deployment_capacity_targets(vec![test_capacity_target()]);
        assert_eq!(dashboard.focus, Focus::Active);

        let mut state = dashboard.state.clone();
        state.sessions.get_mut("session-1").unwrap().state = SessionState::Archived;
        dashboard.set_state(state);
        assert_eq!(dashboard.focus, Focus::Archived);

        dashboard.handle_key(key(KeyCode::Tab));
        assert_eq!(dashboard.focus, Focus::Capacity);
        dashboard.handle_key(key(KeyCode::Tab));
        assert_eq!(dashboard.focus, Focus::Quotas);
        dashboard.handle_key(key(KeyCode::Tab));
        assert_eq!(dashboard.focus, Focus::Active);
        dashboard.handle_key(key(KeyCode::Tab));
        assert_eq!(dashboard.focus, Focus::Archived);
    }

    #[test]
    fn opening_an_active_session_returns_controller_action() {
        let mut session = archived_session();
        session.state = SessionState::Running;
        session.checkpoint = None;
        let mut dashboard = dashboard_with_session(session);
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::Open {
                session_id: "session-1".into()
            }
        );
    }

    #[test]
    fn opening_a_failed_session_with_a_checkpoint_starts_resume() {
        let mut session = archived_session();
        session.state = SessionState::Error;
        session.last_error = Some("resume failed: worker upload source was replaced".into());
        let mut dashboard = dashboard_with_session(session);

        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::None
        );
        assert!(matches!(dashboard.mode, Mode::Resume(_)));
    }

    #[test]
    fn opening_a_failed_session_without_a_checkpoint_preserves_the_actionable_error() {
        let mut session = archived_session();
        session.state = SessionState::Error;
        session.checkpoint = None;
        session.last_error = Some("worker bootstrap failed: upload failed".into());
        let mut dashboard = dashboard_with_session(session);

        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::None
        );
        assert_eq!(
            dashboard.notice.as_deref(),
            Some("worker bootstrap failed: upload failed")
        );
    }

    #[test]
    fn import_dialog_selects_a_session_from_the_chosen_profile() {
        let mut dashboard = dashboard_with_session(archived_session());
        assert_eq!(
            dashboard.handle_key(ctrl_key('i')),
            DashboardAction::OpenImport
        );
        let profiles = vec![
            ImportProfileOption {
                profile_id: "codex-1".into(),
                harness_kind: HarnessKind::Codex,
                sessions: vec![ImportSessionOption {
                    native_session_id: "codex-session".into(),
                    title: "Codex title".into(),
                    details: "2m ago · master · 1.0MB · ~/Projects/hel".into(),
                    unavailable_reason: None,
                }],
                scan_progress: Some((1, 1)),
                error: None,
            },
            ImportProfileOption {
                profile_id: "claude-1".into(),
                harness_kind: HarnessKind::Claude,
                sessions: vec![ImportSessionOption {
                    native_session_id: "claude-session".into(),
                    title: "Claude title".into(),
                    details: "4m ago · master · 2.0MB · ~/Projects/hel".into(),
                    unavailable_reason: None,
                }],
                scan_progress: Some((1, 1)),
                error: None,
            },
        ];
        dashboard.show_import_dialog(1, profiles.clone());
        dashboard.apply_import_profiles(1, profiles);

        dashboard.handle_key(key(KeyCode::Down));
        dashboard.handle_key(key(KeyCode::Right));
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::None
        );
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::ImportSession {
                profile_id: "claude-1".into(),
                native_session_id: "claude-session".into(),
                display_title: "Claude title".into(),
            }
        );
    }

    #[test]
    fn import_dialog_explains_and_blocks_unavailable_sessions() {
        let mut dashboard = dashboard_with_session(archived_session());
        let profiles = vec![ImportProfileOption {
            profile_id: "codex-1".into(),
            harness_kind: HarnessKind::Codex,
            sessions: vec![ImportSessionOption {
                native_session_id: "legacy-session".into(),
                title: "Legacy Codex session".into(),
                details: "2d ago · master · 1.0MB · ~/Projects/hel".into(),
                unavailable_reason: Some(
                    "Legacy Codex history cannot be imported; run codex migrate-rollouts --apply"
                        .into(),
                ),
            }],
            scan_progress: Some((1, 1)),
            error: None,
        }];
        dashboard.show_import_dialog(1, profiles.clone());
        dashboard.apply_import_profiles(1, profiles);
        dashboard.handle_key(key(KeyCode::Right));

        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::None
        );
        assert!(matches!(dashboard.mode, Mode::Import(_)));

        let mut terminal = Terminal::new(TestBackend::new(120, 30)).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw dashboard");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("unavailable"));
        assert!(rendered.contains("Cannot import: Legacy Codex history"));
        assert!(rendered.contains("codex migrate-rollouts --apply"));
    }

    #[test]
    fn incremental_import_results_preserve_the_selected_session() {
        let mut dashboard = dashboard_with_session(archived_session());
        let session = |id: &str| ImportSessionOption {
            native_session_id: id.into(),
            title: "Same title".into(),
            details: "just now · master · 1.0KB · ~/Projects/hel".into(),
            unavailable_reason: None,
        };
        let profile = |sessions: Vec<ImportSessionOption>, progress| ImportProfileOption {
            profile_id: "codex-1".into(),
            harness_kind: HarnessKind::Codex,
            sessions,
            scan_progress: Some(progress),
            error: None,
        };
        let initial = vec![profile(vec![session("a"), session("b")], (2, 3))];
        dashboard.show_import_dialog(1, initial.clone());
        dashboard.apply_import_profiles(1, initial);
        dashboard.handle_key(key(KeyCode::Tab));
        dashboard.handle_key(key(KeyCode::Down));

        dashboard.apply_import_profile(
            1,
            profile(vec![session("a"), session("b"), session("c")], (3, 3)),
        );

        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::None
        );
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::ImportSession {
                profile_id: "codex-1".into(),
                native_session_id: "b".into(),
                display_title: "Same title".into(),
            }
        );
    }

    #[test]
    fn import_dialog_renders_profile_and_session_panes() {
        let mut dashboard = dashboard_with_session(archived_session());
        let profiles = vec![ImportProfileOption {
            profile_id: "codex-1".into(),
            harness_kind: HarnessKind::Codex,
            sessions: vec![ImportSessionOption {
                native_session_id: "native-session-1".into(),
                title: "Native session title".into(),
                details: "2m ago · master · 1.0MB · ~/Projects/hel".into(),
                unavailable_reason: None,
            }],
            scan_progress: Some((1, 1)),
            error: None,
        }];
        dashboard.show_import_dialog(1, profiles.clone());
        dashboard.apply_import_profiles(1, profiles);
        let backend = TestBackend::new(120, 30);
        let mut terminal = Terminal::new(backend).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw dashboard");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(rendered.contains("Profiles"));
        assert!(rendered.contains("Native sessions"));
        assert!(rendered.contains("codex-1"));
        assert!(rendered.contains("Native session title"));
        assert!(rendered.contains("1.0MB"));
        assert!(rendered.contains("~/Projects/hel"));
        assert!(rendered.contains("1/1 sessions scanned"));
    }

    #[test]
    fn import_dialog_tab_cycles_through_panes_and_buttons() {
        let mut dashboard = dashboard_with_session(archived_session());
        let profiles = vec![ImportProfileOption {
            profile_id: "codex-1".into(),
            harness_kind: HarnessKind::Codex,
            sessions: vec![ImportSessionOption {
                native_session_id: "native-session-1".into(),
                title: "Native session title".into(),
                details: "2m ago · master · 1.0MB · ~/Projects/hel".into(),
                unavailable_reason: None,
            }],
            scan_progress: Some((1, 1)),
            error: None,
        }];
        dashboard.show_import_dialog(1, profiles.clone());
        dashboard.apply_import_profiles(1, profiles);
        let mut terminal = Terminal::new(TestBackend::new(120, 30)).expect("terminal");

        for (expected_focus, expected_cursors) in [
            (ImportFocus::Profiles, 1),
            (ImportFocus::Sessions, 1),
            (ImportFocus::Cancel, 0),
            (ImportFocus::Import, 0),
        ] {
            let Mode::Import(dialog) = &dashboard.mode else {
                panic!("expected import dialog");
            };
            assert_eq!(dialog.focus, expected_focus);
            terminal
                .draw(|frame| render_import_dialog(frame, frame.area(), dialog))
                .expect("draw import dialog");
            assert_eq!(
                terminal
                    .backend()
                    .buffer()
                    .content()
                    .iter()
                    .filter(|cell| cell.symbol() == "›")
                    .count(),
                expected_cursors
            );
            dashboard.handle_key(key(KeyCode::Tab));
        }
    }

    #[test]
    fn importing_session_renders_unknown_then_known_progress_and_ignores_navigation() {
        let mut dashboard = dashboard_with_session(archived_session());
        dashboard.show_import_progress("Chosen session".into());
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Down)),
            DashboardAction::None
        );
        let mut terminal = Terminal::new(TestBackend::new(100, 24)).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw import progress");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("Importing session · progress 1/?"));

        dashboard.update_import_progress(2, Some(4), "Native session parsed.".into());
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw known import progress");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("Importing session · progress 2/4"));
        assert!(rendered.contains("Native session parsed."));

        let Mode::Importing(progress) = &mut dashboard.mode else {
            panic!("expected import progress");
        };
        progress.last_updated = Instant::now() - IMPORT_STALL_WARNING_AFTER;
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw stalled import progress");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("filesystem may be stalled"));
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Esc)),
            DashboardAction::CancelImport
        );
    }

    #[test]
    fn import_dialog_shows_profiles_while_sessions_are_still_loading() {
        let mut dashboard = dashboard_with_session(archived_session());
        dashboard.show_import_dialog(
            7,
            vec![ImportProfileOption {
                profile_id: "codex-1".into(),
                harness_kind: HarnessKind::Codex,
                sessions: Vec::new(),
                scan_progress: None,
                error: None,
            }],
        );
        let backend = TestBackend::new(120, 30);
        let mut terminal = Terminal::new(backend).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw dashboard");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(rendered.contains("codex-1"));
        assert!(rendered.contains("Scanning native sessions"));
    }

    #[test]
    fn resume_profile_step_marks_cross_harness_profiles_as_lossy() {
        let mut dashboard = dashboard_with_session(archived_session());
        dashboard.handle_key(key(KeyCode::Enter));
        let backend = TestBackend::new(120, 24);
        let mut terminal = Terminal::new(backend).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw dashboard");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(rendered.contains("(lossy: text-only transcript)"));
        assert!(rendered.contains("Resume · 1/3"));
        assert!(rendered.contains("Lossy: text only; tool calls + reasoning dropped."));

        dashboard.handle_key(key(KeyCode::Enter));
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw resume target step");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("Resume · 2/3"));

        dashboard.handle_key(key(KeyCode::Enter));
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw resume resource step");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("Resume · 3/3"));
    }

    #[test]
    fn agent_message_update_requests_a_resource_refresh() {
        let mut session = archived_session();
        session.state = SessionState::Running;
        let mut dashboard = dashboard_with_session(session);

        assert!(dashboard.apply_worker_events(
            "session-1",
            &[adapter_text_event(1, "agent_message_chunk", "updated")],
            100,
        ));
        assert!(!dashboard.apply_worker_events(
            "session-1",
            &[adapter_text_event(
                2,
                "agent_thought_chunk",
                "not a message"
            )],
            101,
        ));
    }

    #[test]
    fn active_session_renders_the_complete_last_agent_message() {
        let mut session = archived_session();
        session.state = SessionState::Running;
        let mut dashboard = dashboard_with_session(session);
        dashboard.apply_resource_usage(
            "session-1",
            SessionResourceUsage {
                cpu_percent: Some(37),
                memory_current_bytes: 1_073_741_824,
                memory_limit_bytes: Some(2_147_483_648),
                swap_current_bytes: Some(4_096),
                swap_limit_bytes: None,
                writable_disk_bytes: Some(8_192),
            },
        );
        dashboard.apply_worker_events(
            "session-1",
            &[
                adapter_text_event(1, "agent_message_chunk", "**a b**\n"),
                adapter_text_event(2, "agent_message_chunk", "c"),
                adapter_text_event(3, "agent_thought_chunk", "later thought"),
            ],
            100,
        );
        assert_eq!(
            dashboard.session_details["session-1"]
                .last_agent_message
                .as_deref(),
            Some("**a b**\nc")
        );
        let backend = TestBackend::new(120, 36);
        let mut terminal = Terminal::new(backend).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw dashboard");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(rendered.contains("a b"));
        assert!(rendered.contains('c'));
        assert!(!rendered.contains("later thought"));
        assert!(rendered.contains("Turn clock"));
        assert!(rendered.contains("Profile"));
        assert!(rendered.contains("Target"));
        assert!(!rendered.contains("Checkpoint"));
        assert!(rendered.contains("Resources"));
        assert!(rendered.contains("C 37% · M 50%"));
        assert!(!rendered.contains("S 4.0K · D 8.0K"));
        assert!(rendered.contains("Session name"));
        assert!(rendered.contains("ACP pretty name"));
        assert!(!rendered.contains("native-1"));
        assert!(!rendered.contains("Raise the dead"));
    }

    #[test]
    fn highlighted_active_session_renders_rich_transcript_tail_and_collapses_on_blur() {
        let mut session = archived_session();
        session.state = SessionState::Running;
        let mut dashboard = dashboard_with_session(session);
        let events = vec![
            SequencedEvent {
                seq: 1,
                request_id: None,
                event: WorkerEvent::PromptAccepted {
                    request_id: "request-1".into(),
                    text: "inspect the dashboard".into(),
                    attachments: Vec::new(),
                },
            },
            adapter_text_event(2, "agent_message_chunk", "**Rendered answer**"),
            SequencedEvent {
                seq: 3,
                request_id: None,
                event: WorkerEvent::Adapter {
                    kind: "session_update".into(),
                    payload: serde_json::json!({
                        "type": "session_update",
                        "update": {
                            "sessionUpdate": "tool_call",
                            "toolCallId": "tool-1",
                            "title": "Run dashboard tests",
                            "kind": "execute",
                            "status": "completed",
                            "content": [],
                            "locations": []
                        }
                    }),
                },
            },
        ];
        apply_transcript(&mut dashboard, &events);
        let mut terminal = Terminal::new(TestBackend::new(120, 36)).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw selected dashboard");
        let selected = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(selected.contains("❯ You"));
        assert!(selected.contains("inspect the dashboard"));
        assert!(selected.contains("● Agent"));
        assert!(selected.contains("Rendered answer"));
        assert!(selected.contains("✓ Tool · done"));
        assert!(selected.contains("Run dashboard tests"));
        let buffer = terminal.backend().buffer();
        let row_text = |y| {
            (buffer.area.x..buffer.area.right())
                .map(|x| buffer[(x, y)].symbol())
                .collect::<String>()
        };
        let info_y = (buffer.area.y..buffer.area.bottom())
            .find(|y| row_text(*y).contains("ACP pretty name"))
            .expect("session info row");
        let conversation_y = (buffer.area.y..buffer.area.bottom())
            .find(|y| row_text(*y).contains("Rendered answer"))
            .expect("conversation row");
        assert!(
            (buffer.area.x + 1..buffer.area.right() - 1)
                .all(|x| buffer[(x, info_y)].bg == Color::DarkGray)
        );
        assert!(
            (buffer.area.x + 1..buffer.area.right() - 1)
                .all(|x| buffer[(x, conversation_y)].bg != Color::DarkGray)
        );

        dashboard.handle_key(key(KeyCode::Tab));
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw blurred dashboard");
        let blurred = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(blurred.contains("Rendered answer"));
        assert!(!blurred.contains("❯ You"));
        assert!(!blurred.contains("Run dashboard tests"));
    }

    #[test]
    fn selected_transcript_tail_adapts_to_a_constrained_terminal() {
        let mut session = archived_session();
        session.state = SessionState::Running;
        let mut dashboard = dashboard_with_session(session);
        let events = (1..=20)
            .map(|seq| adapter_text_event(seq, "agent_message_chunk", &format!("line {seq}\n")))
            .collect::<Vec<_>>();
        apply_transcript(&mut dashboard, &events);
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).expect("terminal");

        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw constrained dashboard");

        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("Active"));
        assert!(rendered.contains("Paused"));
        assert!(rendered.contains("Profile Quotas"));
    }

    #[test]
    fn overflowing_session_pane_shows_a_scrollbar() {
        let mut sessions = BTreeMap::new();
        for index in 0..6 {
            let mut session = archived_session();
            session.id = format!("active-{index:02}");
            session.state = SessionState::Running;
            sessions.insert(session.id.clone(), session);
        }
        for index in 0..20 {
            let mut session = archived_session();
            session.id = format!("archived-{index:02}");
            sessions.insert(session.id.clone(), session);
        }
        let state = HelState {
            version: STATE_VERSION,
            sessions,
            mount_history: BTreeMap::new(),
        };
        let mut dashboard = DashboardState::new(config(), state, BTreeMap::new());
        for index in 0..6 {
            dashboard.apply_worker_events(
                &format!("active-{index:02}"),
                &[adapter_text_event(
                    1,
                    "agent_message_chunk",
                    "one\ntwo\nthree\nfour",
                )],
                100,
            );
        }
        let backend = TestBackend::new(120, 36);
        let mut terminal = Terminal::new(backend).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw dashboard");
        let symbols = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<Vec<_>>();

        let up = symbols.iter().filter(|symbol| **symbol == "▲").count();
        let down = symbols.iter().filter(|symbol| **symbol == "▼").count();
        assert!(up >= 1, "expected an upper arrow, rendered {up}");
        assert!(down >= 1, "expected a lower arrow, rendered {down}");
    }

    #[test]
    fn fully_visible_tables_do_not_show_scrollbars() {
        let mut dashboard = dashboard_with_session(archived_session());
        let mut terminal = Terminal::new(TestBackend::new(120, 36)).expect("test terminal");

        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw fully visible tables");
        let symbols = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<Vec<_>>();

        assert!(!symbols.iter().any(|symbol| matches!(*symbol, "▲" | "▼")));
    }

    #[test]
    fn overflowing_quota_pane_uses_the_shared_scrollbar() {
        let mut config = config();
        let profile = config.profiles["codex-1"].clone();
        for index in 0..20 {
            config
                .profiles
                .insert(format!("profile-{index:02}"), profile.clone());
        }
        let mut dashboard = DashboardState::new(config, HelState::default(), BTreeMap::new());
        dashboard.focus = Focus::Quotas;
        let mut terminal = Terminal::new(TestBackend::new(120, 24)).expect("test terminal");

        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw overflowing quotas");
        let symbols = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<Vec<_>>();

        assert!(symbols.contains(&"▲"));
        assert!(symbols.contains(&"▼"));
    }

    #[test]
    fn archived_sessions_omit_turn_clock_and_target_columns() {
        let mut dashboard = dashboard_with_session(archived_session());
        let backend = TestBackend::new(120, 36);
        let mut terminal = Terminal::new(backend).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw dashboard");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert_eq!(rendered.matches("Turn clock").count(), 1);
        assert!(!rendered.contains("podman: hel"));
        assert!(rendered.contains("26-08-09 01:00"));
        assert!(!rendered.contains("2026-08-09T01:00:00Z"));
        assert!(!rendered.contains("idle"));
    }

    #[test]
    fn archived_resources_show_the_checkpoint_archive_size() {
        let directory = tempfile::tempdir().unwrap();
        let archive = directory.path().join("session.hel.zip");
        std::fs::write(&archive, vec![0_u8; 1_536]).unwrap();
        let mut session = archived_session();
        session.checkpoint.as_mut().unwrap().archive_path = archive;

        assert_eq!(checkpoint_archive_size(&session), "1.5K");
        session.checkpoint.as_mut().unwrap().archive_path = directory.path().join("missing.zip");
        assert_eq!(checkpoint_archive_size(&session), "—");
    }

    #[test]
    fn archived_checkpoint_time_preserves_its_reported_timezone() {
        assert_eq!(
            checkpoint_time_display("2026-08-09T01:02:03-05:00"),
            "26-08-09 01:02"
        );
        assert_eq!(checkpoint_time_display("not-a-timestamp"), "unknown");
    }

    #[test]
    fn active_checkpoint_age_uses_compact_seconds_minutes_hours_and_days() {
        let checkpointed_at = "2026-08-09T01:00:00Z";
        let base = chrono::DateTime::parse_from_rfc3339(checkpointed_at)
            .unwrap()
            .timestamp() as u64;

        assert_eq!(checkpoint_age(base + 12, checkpointed_at), "12s");
        assert_eq!(checkpoint_age(base + 8 * 60, checkpointed_at), "8m");
        assert_eq!(checkpoint_age(base + 3 * 3_600, checkpointed_at), "3h");
        assert_eq!(checkpoint_age(base + 2 * 86_400, checkpointed_at), "2d");
    }

    #[test]
    fn recovery_state_is_hidden_until_a_failure_needs_attention() {
        let mut session = archived_session();
        session.state = SessionState::Running;
        assert_eq!(
            recovery_warning_name(&session, "Build Hel".into(), 0),
            "Build Hel"
        );

        session.last_checkpoint_error = Some("copy failed".into());
        session.checkpoint = None;
        assert_eq!(
            recovery_warning_name(&session, "Build Hel".into(), 0),
            "Build Hel  ⚠ Recovery unavailable"
        );
    }

    #[test]
    fn active_idle_clock_is_blank() {
        let mut session = archived_session();
        session.state = SessionState::Running;
        let detail = SessionDetail {
            last_agent_text_at: Some(1_000),
            ..SessionDetail::default()
        };

        let (clock, _, _, _, _) = session_values(&session, Some(&detail), 1_480, &config());
        assert_eq!(clock, "");
    }

    #[test]
    fn worker_phase_clears_a_stale_replayed_turn_clock() {
        let mut dashboard = dashboard_with_session(archived_session());
        let prompt = SequencedEvent {
            seq: 1,
            request_id: None,
            event: WorkerEvent::PromptAccepted {
                request_id: "request-1".into(),
                text: "hello".into(),
                attachments: Vec::new(),
            },
        };

        dashboard.apply_worker_update("session-1", &[prompt], WorkerPhase::Idle, 1_000);

        assert_eq!(
            dashboard.session_details["session-1"].current_turn_started_at,
            None
        );
    }

    #[test]
    fn checked_in_running_worker_starts_clock_without_unseen_events() {
        let mut dashboard = dashboard_with_session(archived_session());

        dashboard.apply_worker_update("session-1", &[], WorkerPhase::Running, 1_000);

        assert_eq!(
            dashboard.session_details["session-1"].current_turn_started_at,
            Some(1_000)
        );
    }

    #[test]
    fn active_message_preview_uses_only_the_wrapped_lines_it_needs() {
        let short = SessionDetail {
            last_agent_message: Some("one line".into()),
            ..SessionDetail::default()
        };
        assert_eq!(active_message_preview(Some(&short), 80).len(), 1);

        let long = SessionDetail {
            last_agent_message: Some("one\ntwo\nthree\nfour\nfive".into()),
            ..SessionDetail::default()
        };
        assert_eq!(active_message_preview(Some(&long), 80).len(), 1);
        assert!(active_message_preview(None, 80).is_empty());
    }

    #[test]
    fn active_message_preview_flattens_final_message_newlines_before_capping() {
        let detail = SessionDetail {
            last_agent_message: Some(
                "Fixed and pushed.\n\nDuplicate LinkedIn URLs now use last-write-wins behavior.\n\nCommit: b6cb3e8 Keep the last duplicate connection record".into(),
            ),
            ..SessionDetail::default()
        };

        let rendered = active_message_preview(Some(&detail), 80)
            .iter()
            .flat_map(|line| line.spans.iter())
            .map(|span| span.content.as_ref())
            .collect::<String>();

        assert!(!rendered.contains("more]"));
        assert!(rendered.contains("Fixed and pushed."));
        assert!(rendered.contains("Commit: b6cb3e8"));
    }

    #[test]
    fn provisioning_clock_uses_elapsed_seconds_since_state_update() {
        let mut session = archived_session();
        session.state = SessionState::Provisioning;
        session.updated_at = "1970-01-01T00:16:40Z".into();

        let (clock, _, _, _, _) = session_values(&session, None, 1_012, &config());
        assert_eq!(clock, "Launch 12s");
    }

    #[test]
    fn target_cell_combines_infrastructure_and_project_directories() {
        let mut config = config();
        config
            .bundles
            .get_mut("hel")
            .unwrap()
            .repositories
            .push(ProjectRepository {
                id: "anvil".into(),
                github: Some("BrokkAi/anvil".into()),
                local: None,
                destination: "anvil".into(),
                git_ref: None,
            });

        assert_eq!(
            session_target(&config, &archived_session()),
            "podman: hel, anvil"
        );
    }

    #[test]
    fn container_size_controls_clamp_independently_halves_current_ratio_and_reset() {
        let gib = 1024 * 1024 * 1024;
        let mut allocation = Some(SessionResourceAllocation::Container {
            cpus: 8,
            memory_bytes: 32 * gib,
        });
        let limits = Some((64, 64 * gib));

        adjust_resources(&mut allocation, None, limits, KeyCode::Char('+'));
        adjust_resources(&mut allocation, None, limits, KeyCode::Char('+'));
        assert_eq!(
            allocation,
            Some(SessionResourceAllocation::Container {
                cpus: 32,
                memory_bytes: 64 * gib,
            })
        );

        adjust_resources(&mut allocation, None, limits, KeyCode::Char('-'));
        assert_eq!(
            allocation,
            Some(SessionResourceAllocation::Container {
                cpus: 16,
                memory_bytes: 32 * gib,
            })
        );
        adjust_resources(&mut allocation, None, limits, KeyCode::Char('r'));
        assert_eq!(
            allocation,
            Some(SessionResourceAllocation::Container {
                cpus: 8,
                memory_bytes: 32 * gib,
            })
        );

        adjust_resources(&mut allocation, None, limits, KeyCode::Char('c'));
        assert_eq!(
            allocation,
            Some(SessionResourceAllocation::Container {
                cpus: 16,
                memory_bytes: 32 * gib,
            })
        );
        adjust_resources(&mut allocation, None, limits, KeyCode::Char('m'));
        assert_eq!(
            allocation,
            Some(SessionResourceAllocation::Container {
                cpus: 16,
                memory_bytes: 64 * gib,
            })
        );
    }

    #[test]
    fn ec2_size_controls_use_exact_doubling_steps() {
        let options = [8_u64, 16, 32]
            .into_iter()
            .map(|vcpus| SessionResourceAllocation::AwsEc2 {
                instance_type: format!("family.{vcpus}"),
                vcpus,
                memory_bytes: vcpus * 4 * 1024 * 1024 * 1024,
            })
            .collect::<Vec<_>>();
        let mut allocation = Some(options[0].clone());
        adjust_resources(&mut allocation, Some(&options), None, KeyCode::Char('+'));
        assert_eq!(allocation_cpus(allocation.as_ref().unwrap()), 16);
        adjust_resources(&mut allocation, Some(&options), None, KeyCode::Char('r'));
        assert_eq!(allocation_cpus(allocation.as_ref().unwrap()), 8);
    }

    #[test]
    fn failed_archive_dialog_offers_retry_or_explicit_force_destroy() {
        let mut session = archived_session();
        session.state = SessionState::Running;
        let mut dashboard = dashboard_with_session(session);
        dashboard.handle_key(ctrl_key('p'));
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Char('y'))),
            DashboardAction::Close {
                session_id: "session-1".into()
            }
        );

        dashboard.show_close_failure("session-1".into(), "archive unavailable");
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Char('r'))),
            DashboardAction::Close {
                session_id: "session-1".into()
            }
        );

        dashboard.show_close_failure("session-1".into(), "archive unavailable");
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Char('x'))),
            DashboardAction::None
        );
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Char('f'))),
            DashboardAction::None
        );
        for character in FORCE_CONFIRMATION.chars() {
            assert_eq!(
                dashboard.handle_key(key(KeyCode::Char(character))),
                DashboardAction::None
            );
        }
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::ForceDestroy {
                session_id: "session-1".into()
            }
        );
    }

    #[test]
    fn archived_pane_replaces_checkpoint_and_archive_with_permanent_delete() {
        let mut dashboard = dashboard_with_session(archived_session());
        assert_eq!(dashboard.focus, Focus::Archived);
        assert_eq!(dashboard.handle_key(ctrl_key('k')), DashboardAction::None);
        assert_eq!(dashboard.handle_key(ctrl_key('p')), DashboardAction::None);
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Char('d'))),
            DashboardAction::None
        );
        assert_eq!(dashboard.handle_key(ctrl_key('d')), DashboardAction::None);
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::DeleteArchived {
                session_id: "session-1".into()
            }
        );

        let mut dashboard = dashboard_with_session(archived_session());
        let mut terminal = Terminal::new(TestBackend::new(120, 24)).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw archived actions");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("[D]elete permanently"));
        assert!(!rendered.contains("Chec[K]point"));
        assert!(!rendered.contains("[P]ause"));
    }

    #[test]
    fn focused_panes_use_double_borders_without_focus_title_text() {
        let mut dashboard = dashboard_with_session(archived_session());
        dashboard.set_deployment_capacity_targets(vec![test_capacity_target()]);
        let backend = TestBackend::new(120, 40);
        let mut terminal = Terminal::new(backend).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw dashboard");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("╔ Paused"));
        assert!(rendered.contains("┌ Active"));
        assert!(!rendered.contains("[focused]"));

        dashboard.handle_key(key(KeyCode::Tab));
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw dashboard");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("╔ Capacity in Use"));

        dashboard.handle_key(key(KeyCode::Tab));
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw dashboard");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("╔ Profile Quotas"));
        assert!(!rendered.contains("[focused]"));
    }

    #[test]
    fn only_focused_pane_draws_caret_without_shifting_table_columns() {
        let mut active = archived_session();
        active.id = "session-0".into();
        active.state = SessionState::Running;
        let archived = archived_session();
        let mut dashboard = DashboardState::new(
            config(),
            HelState {
                version: STATE_VERSION,
                sessions: BTreeMap::from([
                    (active.id.clone(), active),
                    (archived.id.clone(), archived),
                ]),
                mount_history: BTreeMap::new(),
            },
            BTreeMap::new(),
        );
        dashboard.set_deployment_capacity_targets(vec![test_capacity_target()]);
        let backend = TestBackend::new(120, 40);
        let mut terminal = Terminal::new(backend).expect("terminal");
        let mut initial_name_columns = None;

        for expected_focus in [
            Focus::Active,
            Focus::Archived,
            Focus::Capacity,
            Focus::Quotas,
        ] {
            assert_eq!(dashboard.focus, expected_focus);
            terminal
                .draw(|frame| render(frame, &mut dashboard))
                .expect("draw dashboard");
            let buffer = terminal.backend().buffer();
            let lines = (buffer.area.y..buffer.area.bottom())
                .map(|y| {
                    (buffer.area.x..buffer.area.right())
                        .map(|x| buffer[(x, y)].symbol())
                        .collect::<String>()
                })
                .collect::<Vec<_>>();
            assert_eq!(
                lines
                    .iter()
                    .flat_map(|line| line.chars())
                    .filter(|character| *character == '›')
                    .count(),
                1
            );
            let name_columns = lines
                .iter()
                .filter_map(|line| {
                    line.find("ACP pretty name")
                        .map(|byte| line[..byte].chars().count())
                })
                .collect::<Vec<_>>();
            assert_eq!(name_columns.len(), 2);
            if let Some(initial_name_columns) = &initial_name_columns {
                assert_eq!(&name_columns, initial_name_columns);
            } else {
                assert_ne!(name_columns[0], name_columns[1]);
                initial_name_columns = Some(name_columns);
            }

            dashboard.handle_key(key(KeyCode::Tab));
        }
    }

    #[test]
    fn empty_config_renders_onboarding_and_exact_welcome() {
        let mut dashboard =
            DashboardState::new(HelConfig::default(), HelState::default(), BTreeMap::new());
        let backend = TestBackend::new(100, 24);
        let mut terminal = Terminal::new(backend).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw dashboard");
        let buffer = terminal.backend().buffer();
        let rendered = buffer
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("Welcome to Hel"));
        assert!(rendered.contains("Hel needs a little fuel."));
        assert_eq!(
            dashboard.handle_key(ctrl_key('e')),
            DashboardAction::OpenConfig
        );
    }

    #[test]
    fn startup_greeting_does_not_change_with_dashboard_updates() {
        let mut dashboard = DashboardState::new(config(), HelState::default(), BTreeMap::new());
        dashboard.set_greeting("A fixed greeting".into());
        dashboard.set_state(HelState::default());
        dashboard.set_quotas(BTreeMap::new());

        let mut terminal = Terminal::new(TestBackend::new(100, 24)).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw dashboard");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("A fixed greeting"));
    }

    #[test]
    fn quota_render_includes_errors_and_refresh_age_in_title() {
        let mut dashboard = DashboardState::new(
            config(),
            HelState::default(),
            BTreeMap::from([(
                "codex-1".into(),
                ProfileQuota {
                    profile_id: "codex-1".into(),
                    harness: HarnessKind::Codex,
                    windows: vec![],
                    extra: None,
                    error: Some("offline".into()),
                    refreshed_at_epoch_seconds: 1,
                },
            )]),
        );
        let backend = TestBackend::new(120, 28);
        let mut terminal = Terminal::new(backend).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw dashboard");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("unavailable: offline"));
        assert!(rendered.contains("Profile Quotas (refreshed"));
        assert!(!rendered.contains("Refreshed"));
        assert!(!rendered.contains("Access"));
        assert!(!rendered.contains("agent-full-access"));
    }
}
