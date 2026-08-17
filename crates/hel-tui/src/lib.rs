//! Full-screen dashboard and session picker for Hel.
//!
//! This module deliberately has no provisioning or persistence side effects.
//! Input is reduced to [`DashboardAction`] values for the controller to run.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
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

use hel::hel_chat::{
    Notices, TranscriptSnapshot, materialized_content_text, render_agent_message_tail,
};
use hel::hel_config::{HarnessKind, HelConfig, TargetTemplate};
use hel::hel_quota::{ProfileQuota, QuotaWindow};
use hel::hel_state::{
    HelState, MaterializedExecutionState, MaterializedSession, SessionRecord,
    SessionResourceAllocation, SessionState, TranscriptBody, TranscriptItem,
};
use hel::hel_targets::{
    AdditionalMount, DeploymentCapacityKind, DeploymentCapacityTarget, DeploymentCapacityUsage,
    ProvisionStage, SessionResourceUsage, default_mount_destination, path_completion,
};

const FORCE_CONFIRMATION: &str = "DESTROY";
const BASELINE_CPUS: u64 = 8;
const BASELINE_MEMORY_BYTES: u64 = 32 * 1024 * 1024 * 1024;
const FLOOR_CPUS: u64 = 2;
const FLOOR_MEMORY_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const ACTIVE_MESSAGE_LINES: usize = 4;
const SELECTED_TRANSCRIPT_LINES: usize = 10;
const SESSION_TABLE_CHROME_HEIGHT: u16 = 3;
const SUMMARY_RULE: &str = "─";
const DASHBOARD_FIXED_HEIGHT: u16 = 3;
const DASHBOARD_PANE_COUNT: usize = 4;
const MOUSE_SCROLL_ROWS: isize = 3;
/// Rows one wheel notch moves the selected session's conversation preview.
const PREVIEW_SCROLL_ROWS: usize = 3;
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
        discard_queue: bool,
    },
    CancelOperation {
        session_id: String,
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
        include_untracked: bool,
    },
    OpenConfig,
    QuitDetach,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionOperationKind {
    Launching,
    Resuming,
    Pausing,
    Destroying,
    Deleting,
    Connecting,
    Importing,
}

impl SessionOperationKind {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Launching => "Launch",
            Self::Resuming => "Resuming",
            Self::Pausing => "Pausing",
            Self::Destroying => "Destroying",
            Self::Deleting => "Deleting",
            Self::Connecting => "Connecting",
            Self::Importing => "Importing",
        }
    }
}

#[derive(Debug, Clone)]
struct SessionOperationDisplay {
    kind: SessionOperationKind,
    started_at_epoch_seconds: u64,
    placeholder: Option<SessionRecord>,
    stage: Option<ProvisionStage>,
    /// When the current `stage` began, so the clock can count that stage's
    /// progress instead of the whole operation's.
    stage_started_at_epoch_seconds: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportSessionOption {
    pub native_session_id: String,
    pub title: String,
    pub project_directory: String,
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
enum SessionOrder {
    Sequence,
    RecentActivity,
    Profile,
}

impl SessionOrder {
    fn label(self) -> &'static str {
        match self {
            Self::Sequence => "sequence",
            Self::RecentActivity => "recent activity",
            Self::Profile => "profile, then sequence",
        }
    }

    fn next(self) -> Self {
        match self {
            Self::Sequence => Self::RecentActivity,
            Self::RecentActivity => Self::Profile,
            Self::Profile => Self::Sequence,
        }
    }
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
    discard_queue: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RenameEditor {
    session_id: String,
    title: String,
    focus: RenameFocus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RenameFocus {
    Field,
    Cancel,
    Save,
}

const RENAME_BUTTONS: &[&str] = &["Cancel", "Save"];
const RENAME_FOCUS_ORDER: [RenameFocus; 3] =
    [RenameFocus::Field, RenameFocus::Cancel, RenameFocus::Save];

impl RenameFocus {
    /// The button Enter would press. Typing in the field also submits, so Save
    /// stays highlighted there and exactly one button is ever highlighted.
    fn button_index(self) -> usize {
        match self {
            RenameFocus::Cancel => 0,
            RenameFocus::Field | RenameFocus::Save => 1,
        }
    }

    fn from_button_index(index: usize) -> Self {
        if index == 0 {
            RenameFocus::Cancel
        } else {
            RenameFocus::Save
        }
    }
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

/// A confirmation dialog plus the index of its focused button.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ConfirmDialog {
    confirmation: Confirmation,
    focus: usize,
}

impl ConfirmDialog {
    fn new(confirmation: Confirmation) -> Self {
        let focus = primary_button(confirmation_buttons(&confirmation));
        Self {
            confirmation,
            focus,
        }
    }
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
    Confirm(ConfirmDialog),
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
    scratch_git_roots: Vec<String>,
    has_untracked_files: bool,
    ignore_untracked: bool,
    focus: usize,
}

const IMPORT_BUNDLE_BUTTONS: &[&str] = &["Cancel", "Continue"];
const IMPORT_PROGRESS_BUTTONS: &[&str] = &["Cancel"];

/// Button labels for a confirmation dialog, ordered Cancel first and the primary
/// action last. Typed-confirmation dialogs have no buttons. This is the single
/// declaration used by both key handling and rendering.
fn confirmation_buttons(confirmation: &Confirmation) -> &'static [&'static str] {
    match confirmation {
        Confirmation::DirtyLocal { .. } => &["Cancel", "Continue"],
        Confirmation::Close { .. } => &["Cancel", "Pause"],
        Confirmation::DeleteArchived { .. } => &["Cancel", "Delete"],
        Confirmation::CloseFailed { .. } => &["Cancel", "Force destroy", "Retry pause"],
        Confirmation::ForceDestroy { .. } | Confirmation::DeleteActive { .. } => &[],
    }
}

/// Index of the primary (rightmost) button, which is focused when a dialog opens.
fn primary_button(labels: &[&str]) -> usize {
    labels.len().saturating_sub(1)
}

/// What a key press means for a focusable button row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ButtonKey {
    Focus(usize),
    Activate(usize),
    Cancel,
    Ignored,
}

fn button_row_key(code: KeyCode, focus: usize, count: usize) -> ButtonKey {
    match code {
        KeyCode::Tab | KeyCode::Right => ButtonKey::Focus(cycle_button_focus(focus, count, false)),
        KeyCode::BackTab | KeyCode::Left => {
            ButtonKey::Focus(cycle_button_focus(focus, count, true))
        }
        KeyCode::Enter => ButtonKey::Activate(focus),
        KeyCode::Esc => ButtonKey::Cancel,
        _ => ButtonKey::Ignored,
    }
}

fn cycle_button_focus(focus: usize, count: usize, reverse: bool) -> usize {
    if count == 0 {
        return 0;
    }
    if reverse {
        focus.min(count - 1).checked_sub(1).unwrap_or(count - 1)
    } else {
        (focus + 1) % count
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ImportFocus {
    Filter,
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
    if let Err(error) = hel::hel_targets::validate_additional_mounts(std::slice::from_ref(&mount)) {
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
    filter: String,
    focus: ImportFocus,
    opened_at: Instant,
}

impl ImportDialog {
    fn filtered_sessions(&self) -> Vec<&ImportSessionOption> {
        let needle = self.filter.to_lowercase();
        self.profiles
            .get(self.profile_index)
            .into_iter()
            .flat_map(|profile| &profile.sessions)
            .filter(|session| {
                needle.is_empty() || session.project_directory.to_lowercase().contains(&needle)
            })
            .collect()
    }

    fn selected_session(&self) -> Option<&ImportSessionOption> {
        self.filtered_sessions().get(self.session_index).copied()
    }

    fn is_scanning(&self) -> bool {
        self.profiles.iter().any(|profile| {
            profile.error.is_none()
                && profile
                    .scan_progress
                    .is_none_or(|(scanned, total)| scanned < total)
        })
    }
}

#[derive(Debug, Default)]
struct SessionDetail {
    materialized_applied_event_ordinal: Option<u64>,
    current_turn_started_at: Option<u64>,
    last_activity_at_ms: Option<u64>,
    last_agent_message: Option<Arc<str>>,
    /// Latest agent-content ordinals retained so a state-only read-cursor
    /// update can recompute the single unread count exactly.
    agent_message_latest_content_ordinals: Vec<u64>,
    unread_agent_messages: usize,
    resource_usage: Option<SessionResourceUsage>,
    transcript: Option<TranscriptSnapshot>,
    transcript_hydration: TranscriptHydration,
    queued_prompts: Vec<hel::hel_worker::QueuedPrompt>,
    /// What the last projection derived, so the next one only rescans the
    /// transcript items that changed.
    projection: MaterializedProjectionCache,
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
    transcript: Vec<Arc<TranscriptItem>>,
    /// Transcript index and latest content ordinal of every agent message that
    /// has content, in transcript order.
    agent_messages: Vec<(usize, u64)>,
    /// Transcript index and text of the last agent message with text.
    last_agent_message: Option<(usize, Arc<str>)>,
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
fn last_agent_message(
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
    session_id: String,
    applied_event_ordinal: u64,
    session_title: Option<String>,
    current_turn_started_at: Option<u64>,
    last_activity_at_ms: Option<u64>,
    last_agent_message: Option<Arc<str>>,
    agent_message_latest_content_ordinals: Vec<u64>,
    unread_agent_messages: usize,
    transcript: TranscriptSnapshot,
    queued_prompts: Vec<hel::hel_worker::QueuedPrompt>,
    projection: MaterializedProjectionCache,
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
        let transcript = TranscriptSnapshot::from_materialized(&session);
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
            },
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum TranscriptHydration {
    #[default]
    Loading,
    Ready,
    Unavailable,
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
    checkpoint_archive_sizes: BTreeMap<String, Option<u64>>,
    session_operations: BTreeMap<String, SessionOperationDisplay>,
    capacity_details: BTreeMap<String, CapacityDetail>,
    session_index: usize,
    session_order: SessionOrder,
    capacity_index: usize,
    quota_index: usize,
    focus: Focus,
    pane_areas: Option<[Rect; DASHBOARD_PANE_COUNT]>,
    import_sessions_area: Option<Rect>,
    /// Hitbox of the selected session's conversation preview, so the wheel can
    /// scroll that preview instead of moving the selection.
    selected_preview_area: Option<Rect>,
    /// Rows the selected session's preview sits above its live tail. Only one
    /// preview scrolls at a time; selecting another session snaps this back to
    /// the tail, which is why the owning session is tracked alongside it.
    preview_scroll: usize,
    preview_scroll_session: Option<String>,
    mode: Mode,
    notices: Notices,
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
            checkpoint_archive_sizes: BTreeMap::new(),
            session_operations: BTreeMap::new(),
            capacity_details: BTreeMap::new(),
            session_index: 0,
            session_order: SessionOrder::Sequence,
            capacity_index: 0,
            quota_index: 0,
            focus: Focus::Active,
            pane_areas: None,
            import_sessions_area: None,
            selected_preview_area: None,
            preview_scroll: 0,
            preview_scroll_session: None,
            mode: Mode::Dashboard,
            notices: Notices::default(),
            greeting: "Welcome to Hel".into(),
        };
        dashboard.session_details = dashboard
            .state
            .sessions
            .keys()
            .map(|id| (id.clone(), SessionDetail::default()))
            .collect();
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
            },
        );
        self.apply_operation_projection();
        self.clamp_selections();
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

    pub fn select_active_session(&mut self, session_id: &str) {
        let (active, _) = partition_sessions(
            self.state.sessions.values(),
            &self.session_details,
            self.session_order,
        );
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

    /// Show the recovery choices after a checkpointed close could not finish.
    pub fn show_close_failure(&mut self, session_id: String, error: impl Into<String>) {
        self.mode = Mode::Confirm(ConfirmDialog::new(Confirmation::CloseFailed {
            session_id,
            error: error.into(),
        }));
    }

    pub fn show_import_dialog(&mut self, discovery_id: u64, profiles: Vec<ImportProfileOption>) {
        self.mode = Mode::Import(ImportDialog {
            discovery_id,
            profiles,
            profile_index: 0,
            session_index: 0,
            filter: String::new(),
            focus: ImportFocus::Profiles,
            opened_at: Instant::now(),
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
            .selected_session()
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
        let sessions = dialog.filtered_sessions();
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
                dialog
                    .selected_session()
                    .map(|session| session.native_session_id.clone())
            })
            .flatten();
        dialog.profiles[profile_index] = profile;
        if dialog.profile_index != profile_index {
            return;
        }
        let sessions = dialog.filtered_sessions();
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
        scratch_git_roots: Vec<String>,
        has_untracked_files: bool,
    ) {
        self.mode = Mode::ConfirmImportBundle(ImportBundleConfirmation {
            dirty_git_roots,
            omitted_non_git_dirs,
            scratch_git_roots,
            has_untracked_files,
            ignore_untracked: has_untracked_files,
            focus: primary_button(IMPORT_BUNDLE_BUTTONS),
        });
    }

    pub fn show_dirty_local_confirmation(
        &mut self,
        action: DashboardAction,
        repositories: Vec<String>,
    ) {
        self.mode = Mode::Confirm(ConfirmDialog::new(Confirmation::DirtyLocal {
            action,
            repositories,
        }));
    }

    pub fn finish_import(&mut self) {
        self.cancel_modal();
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> DashboardAction {
        if key.kind != KeyEventKind::Press && key.kind != KeyEventKind::Repeat {
            return DashboardAction::None;
        }
        if is_paste_shortcut(key) {
            match hel::hel_clipboard::read_text() {
                Ok(text) => self.handle_paste(&text),
                Err(error) => self.notices.set(format!("Paste failed: {error:#}")),
            }
            return DashboardAction::None;
        }
        if dashboard_accelerator(key.modifiers)
            && matches!(key.code, KeyCode::Char('c') | KeyCode::Char('q'))
        {
            return DashboardAction::QuitDetach;
        }

        self.notices.clear();
        match self.mode.clone() {
            Mode::Dashboard => self.handle_dashboard_key(key),
            Mode::New(wizard) => self.handle_new_key(key.code, wizard),
            Mode::Resume(wizard) => self.handle_resume_key(key.code, wizard),
            Mode::Rename(editor) => self.handle_rename_key(key.code, editor),
            Mode::Import(dialog) => self.handle_import_key(key, dialog),
            // The only control is the Cancel button, so Enter presses it too.
            Mode::Importing(_) => match key.code {
                KeyCode::Esc | KeyCode::Enter => DashboardAction::CancelImport,
                _ => DashboardAction::None,
            },
            Mode::ConfirmImportBundle(confirmation) => {
                self.handle_import_bundle_key(key.code, confirmation)
            }
            Mode::Confirm(dialog) => self.handle_confirmation_key(key.code, dialog),
        }
    }

    pub fn handle_paste(&mut self, pasted: &str) {
        let pasted = single_line_paste(pasted);
        if pasted.is_empty() {
            return;
        }
        match &mut self.mode {
            Mode::Rename(editor) if editor.focus == RenameFocus::Field => {
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
            Mode::Import(dialog) if dialog.focus == ImportFocus::Filter => {
                dialog.filter.push_str(&pasted);
                dialog.session_index = 0;
            }
            Mode::Confirm(ConfirmDialog {
                confirmation:
                    Confirmation::ForceDestroy { typed, .. } | Confirmation::DeleteActive { typed, .. },
                ..
            }) => {
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
        if let Mode::Import(dialog) = &mut self.mode {
            let Some(area) = self.import_sessions_area else {
                return;
            };
            if !rect_contains(area, mouse.column, mouse.row) {
                return;
            }
            let delta = match mouse.kind {
                MouseEventKind::ScrollUp => -MOUSE_SCROLL_ROWS,
                MouseEventKind::ScrollDown => MOUSE_SCROLL_ROWS,
                _ => return,
            };
            let len = dialog.filtered_sessions().len();
            dialog.focus = ImportFocus::Sessions;
            dialog.session_index = offset_index(dialog.session_index, len, delta);
            return;
        }
        if !matches!(self.mode, Mode::Dashboard) {
            return;
        }
        // The selected session's conversation preview scrolls its own history;
        // anywhere else the wheel moves the hovered list's selection.
        if let Some(area) = self.selected_preview_area
            && rect_contains(area, mouse.column, mouse.row)
        {
            match mouse.kind {
                MouseEventKind::ScrollUp => {
                    self.preview_scroll = self.preview_scroll.saturating_add(PREVIEW_SCROLL_ROWS);
                }
                MouseEventKind::ScrollDown => {
                    self.preview_scroll = self.preview_scroll.saturating_sub(PREVIEW_SCROLL_ROWS);
                }
                _ => {}
            }
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
            (KeyCode::Char('s'), false) => {
                self.cycle_session_order();
                DashboardAction::None
            }
            (KeyCode::Char('n'), true) => self.begin_new(),
            (KeyCode::Char('x'), true) => {
                let session_id = self.selected_session().and_then(|session| {
                    self.session_operations
                        .contains_key(&session.id)
                        .then(|| session.id.clone())
                });
                session_id.map_or(DashboardAction::None, |session_id| {
                    DashboardAction::CancelOperation { session_id }
                })
            }
            (KeyCode::Char('i') | KeyCode::Char('t'), true) => DashboardAction::OpenImport,
            (KeyCode::Char('r'), true) => {
                if self.focus == Focus::Quotas {
                    DashboardAction::RefreshQuotas
                } else if matches!(self.focus, Focus::Active | Focus::Archived) {
                    if !self.reject_selected_operation() {
                        self.begin_rename();
                    }
                    DashboardAction::None
                } else {
                    DashboardAction::None
                }
            }
            (KeyCode::Char('u'), true) => DashboardAction::RefreshQuotas,
            (KeyCode::Char('e'), true) if self.config_is_empty() => DashboardAction::OpenConfig,
            (KeyCode::Char('p'), true) if self.focus == Focus::Active => {
                if self.reject_selected_operation() {
                    return DashboardAction::None;
                }
                if let Some(session) = self.selected_session() {
                    self.mode = Mode::Confirm(ConfirmDialog::new(Confirmation::Close {
                        session_id: session.id.clone(),
                    }));
                }
                DashboardAction::None
            }
            (KeyCode::Char('d'), true) | (KeyCode::Delete, _) if self.focus == Focus::Archived => {
                if self.reject_selected_operation() {
                    return DashboardAction::None;
                }
                if let Some(session) = self.selected_session() {
                    self.mode = Mode::Confirm(ConfirmDialog::new(Confirmation::DeleteArchived {
                        session_id: session.id.clone(),
                    }));
                }
                DashboardAction::None
            }
            (KeyCode::Char('d'), true) | (KeyCode::Delete, _) if self.focus == Focus::Active => {
                if self.reject_selected_operation() {
                    return DashboardAction::None;
                }
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
                    self.mode = Mode::Confirm(ConfirmDialog::new(Confirmation::DeleteActive {
                        session_id,
                        typed: String::new(),
                    }));
                }
                DashboardAction::None
            }
            (KeyCode::Enter, _) | (KeyCode::Char('o'), true) => self.open_or_resume(),
            _ => DashboardAction::None,
        }
    }

    fn handle_import_key(&mut self, key: KeyEvent, mut dialog: ImportDialog) -> DashboardAction {
        match key.code {
            KeyCode::Esc => {
                self.cancel_modal();
                DashboardAction::None
            }
            KeyCode::Left
                if matches!(dialog.focus, ImportFocus::Filter | ImportFocus::Sessions) =>
            {
                dialog.focus = ImportFocus::Profiles;
                self.mode = Mode::Import(dialog);
                DashboardAction::None
            }
            KeyCode::Right if dialog.focus == ImportFocus::Profiles => {
                dialog.focus = ImportFocus::Filter;
                self.mode = Mode::Import(dialog);
                DashboardAction::None
            }
            KeyCode::Tab => {
                dialog.focus = cycle_control(
                    dialog.focus,
                    &[
                        ImportFocus::Profiles,
                        ImportFocus::Filter,
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
                        ImportFocus::Filter,
                        ImportFocus::Sessions,
                        ImportFocus::Cancel,
                        ImportFocus::Import,
                    ],
                    true,
                );
                self.mode = Mode::Import(dialog);
                DashboardAction::None
            }
            KeyCode::Backspace if dialog.focus == ImportFocus::Filter => {
                dialog.filter.pop();
                dialog.session_index = 0;
                self.mode = Mode::Import(dialog);
                DashboardAction::None
            }
            KeyCode::Char(character)
                if dialog.focus == ImportFocus::Filter
                    && !key.modifiers.intersects(
                        KeyModifiers::CONTROL | KeyModifiers::ALT | KeyModifiers::SUPER,
                    ) =>
            {
                dialog.filter.push(character);
                dialog.session_index = 0;
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
                        if dialog.session_index == 0 {
                            dialog.focus = ImportFocus::Filter;
                        } else {
                            dialog.session_index -= 1;
                        }
                    }
                    ImportFocus::Filter | ImportFocus::Cancel | ImportFocus::Import => {}
                }
                self.mode = Mode::Import(dialog);
                DashboardAction::None
            }
            KeyCode::Down | KeyCode::Char('j') => {
                match dialog.focus {
                    ImportFocus::Filter => {
                        dialog.focus = ImportFocus::Sessions;
                    }
                    ImportFocus::Profiles => {
                        move_index(&mut dialog.profile_index, dialog.profiles.len(), 1);
                        dialog.session_index = 0;
                    }
                    ImportFocus::Sessions => {
                        let len = dialog.filtered_sessions().len();
                        move_index(&mut dialog.session_index, len, 1);
                    }
                    ImportFocus::Cancel | ImportFocus::Import => {}
                }
                self.mode = Mode::Import(dialog);
                DashboardAction::None
            }
            KeyCode::Enter if dialog.focus == ImportFocus::Profiles => {
                dialog.focus = ImportFocus::Filter;
                self.mode = Mode::Import(dialog);
                DashboardAction::None
            }
            KeyCode::Enter if dialog.focus == ImportFocus::Filter => {
                dialog.focus = ImportFocus::Sessions;
                self.mode = Mode::Import(dialog);
                DashboardAction::None
            }
            KeyCode::Enter if dialog.focus == ImportFocus::Sessions => {
                let available = dialog
                    .selected_session()
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
                let Some(profile_id) = dialog
                    .profiles
                    .get(dialog.profile_index)
                    .map(|profile| profile.profile_id.clone())
                else {
                    self.mode = Mode::Import(dialog);
                    return DashboardAction::None;
                };
                let Some(session) = dialog.selected_session() else {
                    self.mode = Mode::Import(dialog);
                    return DashboardAction::None;
                };
                if session.unavailable_reason.is_some() {
                    self.mode = Mode::Import(dialog);
                    return DashboardAction::None;
                }
                let action = DashboardAction::ImportSession {
                    profile_id,
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
            focus: RenameFocus::Field,
        });
    }

    fn reject_selected_operation(&mut self) -> bool {
        let operation = self.selected_session().and_then(|session| {
            self.session_operations
                .get(&session.id)
                .map(|operation| operation.kind)
        });
        if let Some(operation) = operation {
            self.notices.set(format!(
                "{} is in progress; press Ctrl+X to cancel it.",
                operation.label()
            ));
            true
        } else {
            false
        }
    }

    fn handle_rename_key(&mut self, code: KeyCode, mut editor: RenameEditor) -> DashboardAction {
        match code {
            KeyCode::Esc => {
                self.cancel_modal();
                DashboardAction::None
            }
            KeyCode::Tab | KeyCode::BackTab => {
                editor.focus =
                    cycle_control(editor.focus, &RENAME_FOCUS_ORDER, code == KeyCode::BackTab);
                self.mode = Mode::Rename(editor);
                DashboardAction::None
            }
            // The field has no cursor, so horizontal keys only move between buttons.
            KeyCode::Left | KeyCode::Right if editor.focus != RenameFocus::Field => {
                editor.focus = RenameFocus::from_button_index(cycle_button_focus(
                    editor.focus.button_index(),
                    RENAME_BUTTONS.len(),
                    code == KeyCode::Left,
                ));
                self.mode = Mode::Rename(editor);
                DashboardAction::None
            }
            KeyCode::Enter if editor.focus == RenameFocus::Cancel => {
                self.cancel_modal();
                DashboardAction::None
            }
            KeyCode::Enter if editor.title.trim().is_empty() => {
                self.notices.set("Session name cannot be empty.");
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
            KeyCode::Backspace if editor.focus == RenameFocus::Field => {
                editor.title.pop();
                self.mode = Mode::Rename(editor);
                DashboardAction::None
            }
            KeyCode::Char(character)
                if editor.focus == RenameFocus::Field && editor.title.chars().count() < 64 =>
            {
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
                    self.notices.set("Repository source cannot be empty.");
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
                    self.notices.set(
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
            self.notices
                .set(format!("Created bundle {bundle_id:?} was not found."));
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

    /// Why this session cannot resume on `target_id`, or `None` when it can.
    fn resume_target_rejection(&self, session_id: &str, target_id: &str) -> Option<String> {
        let session = self.state.sessions.get(session_id)?;
        hel::hel_controller::resume_compatibility(session, &self.config, target_id).err()
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
                if let Some(reason) = self.resume_target_rejection(&wizard.session_id, &target_id) {
                    self.notices.set(reason);
                    self.mode = Mode::Resume(wizard);
                    return DashboardAction::None;
                }
                if matches!(
                    self.config.targets[&target_id],
                    TargetTemplate::AwsEc2 { .. }
                ) && wizard.resource_allocation.is_none()
                {
                    self.notices.set(
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
        if code == KeyCode::Char('q')
            && self
                .session_details
                .get(&wizard.session_id)
                .is_some_and(|detail| !detail.queued_prompts.is_empty())
        {
            wizard.discard_queue = !wizard.discard_queue;
            self.mode = Mode::Resume(wizard);
            return DashboardAction::None;
        }
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
            discard_queue: wizard.discard_queue,
        };
        self.cancel_modal();
        action
    }

    fn handle_import_bundle_key(
        &mut self,
        code: KeyCode,
        mut confirmation: ImportBundleConfirmation,
    ) -> DashboardAction {
        // The checkbox toggle is independent of which button has focus.
        if code == KeyCode::Char(' ') && confirmation.has_untracked_files {
            confirmation.ignore_untracked = !confirmation.ignore_untracked;
            self.mode = Mode::ConfirmImportBundle(confirmation);
            return DashboardAction::None;
        }
        let cancelled = DashboardAction::ConfirmImportBundle {
            accepted: false,
            include_untracked: false,
        };
        confirmation.focus =
            match button_row_key(code, confirmation.focus, IMPORT_BUNDLE_BUTTONS.len()) {
                ButtonKey::Activate(index) if index == primary_button(IMPORT_BUNDLE_BUTTONS) => {
                    return DashboardAction::ConfirmImportBundle {
                        accepted: true,
                        include_untracked: !confirmation.ignore_untracked,
                    };
                }
                ButtonKey::Activate(_) | ButtonKey::Cancel => return cancelled,
                ButtonKey::Focus(focus) => focus,
                ButtonKey::Ignored => confirmation.focus,
            };
        self.mode = Mode::ConfirmImportBundle(confirmation);
        DashboardAction::None
    }

    fn handle_confirmation_key(&mut self, code: KeyCode, dialog: ConfirmDialog) -> DashboardAction {
        let ConfirmDialog {
            confirmation,
            focus,
        } = dialog;
        let buttons = confirmation_buttons(&confirmation);
        if buttons.is_empty() {
            return self.handle_typed_confirmation_key(code, confirmation);
        }
        let focus = match button_row_key(code, focus, buttons.len()) {
            ButtonKey::Activate(index) => {
                return self.activate_confirmation_button(confirmation, index);
            }
            ButtonKey::Cancel => {
                self.cancel_modal();
                return DashboardAction::None;
            }
            ButtonKey::Focus(next) => next,
            ButtonKey::Ignored => focus,
        };
        self.mode = Mode::Confirm(ConfirmDialog {
            confirmation,
            focus,
        });
        DashboardAction::None
    }

    /// Runs the button at `index` of `confirmation_buttons`, where index 0 is always Cancel.
    fn activate_confirmation_button(
        &mut self,
        confirmation: Confirmation,
        index: usize,
    ) -> DashboardAction {
        match (confirmation, index) {
            (Confirmation::DirtyLocal { mut action, .. }, 1) => {
                if let DashboardAction::CreateSession {
                    allow_dirty_local, ..
                } = &mut action
                {
                    *allow_dirty_local = true;
                }
                self.cancel_modal();
                action
            }
            (Confirmation::Close { session_id }, 1) => {
                self.cancel_modal();
                DashboardAction::Close { session_id }
            }
            (Confirmation::DeleteArchived { session_id }, 1) => {
                self.cancel_modal();
                DashboardAction::DeleteArchived { session_id }
            }
            (Confirmation::CloseFailed { session_id, .. }, 1) => {
                self.mode = Mode::Confirm(ConfirmDialog::new(Confirmation::ForceDestroy {
                    session_id,
                    typed: String::new(),
                }));
                DashboardAction::None
            }
            (Confirmation::CloseFailed { session_id, .. }, 2) => {
                self.cancel_modal();
                DashboardAction::Close { session_id }
            }
            _ => {
                self.cancel_modal();
                DashboardAction::None
            }
        }
    }

    fn handle_typed_confirmation_key(
        &mut self,
        code: KeyCode,
        confirmation: Confirmation,
    ) -> DashboardAction {
        match confirmation {
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
                    self.mode = Mode::Confirm(ConfirmDialog::new(Confirmation::ForceDestroy {
                        session_id,
                        typed,
                    }));
                    DashboardAction::None
                }
                KeyCode::Char(c) => {
                    if typed.len() < FORCE_CONFIRMATION.len() {
                        typed.push(c.to_ascii_uppercase());
                    }
                    self.mode = Mode::Confirm(ConfirmDialog::new(Confirmation::ForceDestroy {
                        session_id,
                        typed,
                    }));
                    DashboardAction::None
                }
                KeyCode::Enter if typed == FORCE_CONFIRMATION => {
                    self.cancel_modal();
                    DashboardAction::ForceDestroy { session_id }
                }
                _ => {
                    self.mode = Mode::Confirm(ConfirmDialog::new(Confirmation::ForceDestroy {
                        session_id,
                        typed,
                    }));
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
                    self.mode = Mode::Confirm(ConfirmDialog::new(Confirmation::DeleteActive {
                        session_id,
                        typed,
                    }));
                    DashboardAction::None
                }
                KeyCode::Char(c) => {
                    if typed.len() < FORCE_CONFIRMATION.len() {
                        typed.push(c.to_ascii_uppercase());
                    }
                    self.mode = Mode::Confirm(ConfirmDialog::new(Confirmation::DeleteActive {
                        session_id,
                        typed,
                    }));
                    DashboardAction::None
                }
                KeyCode::Enter if typed == FORCE_CONFIRMATION => {
                    self.cancel_modal();
                    DashboardAction::DeleteActive { session_id }
                }
                _ => {
                    self.mode = Mode::Confirm(ConfirmDialog::new(Confirmation::DeleteActive {
                        session_id,
                        typed,
                    }));
                    DashboardAction::None
                }
            },
            // Button dialogs are handled by `handle_confirmation_key`.
            other => {
                self.mode = Mode::Confirm(ConfirmDialog::new(other));
                DashboardAction::None
            }
        }
    }

    fn begin_new(&mut self) -> DashboardAction {
        if self.config.profiles.is_empty() || self.config.targets.is_empty() {
            self.notices
                .set("Configure at least one profile and target first.");
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
            self.notices
                .set("This session is active; press Enter to open it.");
            return DashboardAction::None;
        }
        if session.checkpoint.is_none() {
            self.notices
                .set("This session has no verified recovery copy to resume.");
            return DashboardAction::None;
        }
        if self.compatible_profiles(&session.id).is_empty() || self.config.targets.is_empty() {
            self.notices
                .set("Resume needs a profile and a target template.");
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
            discard_queue: false,
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
        if let Some(operation) = self.session_operations.get(&session.id) {
            self.notices.set(format!(
                "{} is in progress; press Ctrl+X to cancel it.",
                operation.kind.label()
            ));
            return DashboardAction::None;
        }
        if session.state == SessionState::Error {
            if session.checkpoint.is_some() {
                return self.begin_resume();
            } else {
                self.notices.set(
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
        let (active, archived) = partition_sessions(
            self.state.sessions.values(),
            &self.session_details,
            self.session_order,
        );
        active.into_iter().chain(archived).collect()
    }

    fn cycle_session_order(&mut self) {
        let selected_id = self.selected_session().map(|session| session.id.clone());
        self.session_order = self.session_order.next();
        if let Some(selected_id) = selected_id
            && let Some(index) = self
                .ordered_sessions()
                .iter()
                .position(|session| session.id == selected_id)
        {
            self.session_index = index;
        }
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
        format!("{id}  {}  ·  {quota}{danger}", harness.display_name())
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
        let (active, archived) = partition_sessions(
            self.state.sessions.values(),
            &self.session_details,
            self.session_order,
        );
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
        let active_len = partition_sessions(
            self.state.sessions.values(),
            &self.session_details,
            self.session_order,
        )
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
                let active_len = partition_sessions(
                    self.state.sessions.values(),
                    &self.session_details,
                    self.session_order,
                )
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
        let active_len = partition_sessions(
            self.state.sessions.values(),
            &self.session_details,
            self.session_order,
        )
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
        let active_len = partition_sessions(
            self.state.sessions.values(),
            &self.session_details,
            self.session_order,
        )
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
        let (active, archived) = partition_sessions(
            self.state.sessions.values(),
            &self.session_details,
            self.session_order,
        );
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
    order: SessionOrder,
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
    let activity_timestamp = |session: &SessionRecord| match session_details.get(&session.id) {
        Some(detail) if detail.last_activity_at_ms.is_some() => detail
            .last_activity_at_ms
            .and_then(|timestamp| i64::try_from(timestamp).ok()),
        Some(detail) if detail.transcript_hydration == TranscriptHydration::Ready => None,
        Some(_) | None => session_timestamp(&session.updated_at)
            .and_then(|timestamp| timestamp.checked_mul(1_000)),
    };
    let sequence = |left: &&SessionRecord, right: &&SessionRecord| left.compare_by_creation(right);
    let sort = |left: &&SessionRecord, right: &&SessionRecord| match order {
        SessionOrder::Sequence => sequence(left, right),
        SessionOrder::RecentActivity => activity_timestamp(right)
            .cmp(&activity_timestamp(left))
            .then_with(|| sequence(left, right)),
        SessionOrder::Profile => left
            .last_profile
            .cmp(&right.last_profile)
            .then_with(|| sequence(left, right)),
    };
    active.sort_by(sort);
    archived.sort_by(sort);
    (active, archived)
}

fn session_timestamp(timestamp: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(timestamp)
        .ok()
        .map(|timestamp| timestamp.timestamp())
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
                    (cpus.saturating_add(8).min(max_cpus.max(1)), memory_bytes)
                }
                KeyCode::Char('m') => {
                    let Some((_, max_memory)) = limits else {
                        return;
                    };
                    (
                        cpus,
                        memory_bytes
                            .saturating_add(memory_bytes / 2)
                            .min(max_memory.max(1)),
                    )
                }
                KeyCode::Char('-') => {
                    let next_cpus = if cpus > FLOOR_CPUS {
                        (cpus / 2).max(FLOOR_CPUS)
                    } else {
                        cpus
                    };
                    let next_memory = if memory_bytes > FLOOR_MEMORY_BYTES {
                        (memory_bytes / 2).max(FLOOR_MEMORY_BYTES)
                    } else {
                        memory_bytes
                    };
                    (next_cpus, next_memory)
                }
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
    dashboard.selected_preview_area = None;
    let area = frame.area();
    if area.width < MINIMUM_TERMINAL_WIDTH {
        render_terminal_too_small(
            frame,
            area,
            TerminalSizeRequirement::Width(MINIMUM_TERMINAL_WIDTH),
        );
        return;
    }
    dashboard.import_sessions_area =
        matches!(dashboard.mode, Mode::Import(_)).then(|| import_sessions_pane(area));

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
        Mode::Confirm(dialog) => render_confirmation(frame, area, dialog),
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
    // One ordering for the whole frame: the panes, the previews, and the row
    // widgets all read this partition.
    let (active, archived) = partition_sessions(
        dashboard.state.sessions.values(),
        &dashboard.session_details,
        dashboard.session_order,
    );
    let (active_count, archived_count) = (active.len(), archived.len());
    let selected_active = (dashboard.focus == Focus::Active)
        .then_some(dashboard.session_index)
        .filter(|index| *index < active_count);
    // Only the selected preview scrolls; moving the selection snaps the one
    // left behind back to its live tail.
    let selected_id = selected_active.map(|index| active[index].id.clone());
    if dashboard.preview_scroll_session != selected_id {
        dashboard.preview_scroll_session = selected_id;
        dashboard.preview_scroll = 0;
    }
    // A session mid-launch, mid-resume, or mid-pause has nothing worth
    // previewing, so its row collapses to just the summary line.
    let active_collapsed = active
        .iter()
        .map(|session| {
            active_row_collapses_to_summary(
                dashboard.session_operations.get(&session.id),
                session.state,
            )
        })
        .collect::<Vec<_>>();
    // Row heights need the previews, and the selected session's line budget
    // needs the allocated pane height. Every unselected preview is the same in
    // both passes, so the transcript tails are walked once here and only the
    // selected preview is rebuilt below when the pane came up short.
    let mut active_previews = prepare_active_previews(
        &active,
        &mut dashboard.session_details,
        PreviewRequest {
            width: preview_width,
            selected: selected_active,
            scroll: dashboard.preview_scroll,
            selected_lines: SELECTED_TRANSCRIPT_LINES,
        },
        &active_collapsed,
    );
    let active_row_heights = active_previews
        .previews
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
        render_terminal_too_small(
            frame,
            frame_area,
            TerminalSizeRequirement::Height(required_frame_height),
        );
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
    // The sizing pass gave the selected preview the full line budget; redo just
    // that row when the allocated pane grants it fewer lines, so its scroll is
    // clamped against the height it actually renders at.
    if selected_lines != SELECTED_TRANSCRIPT_LINES
        && let Some(index) = selected_active
        && !active_collapsed[index]
    {
        let (preview, applied) = active_transcript_tail(
            dashboard.session_details.get_mut(&active[index].id),
            preview_width,
            selected_lines,
            dashboard.preview_scroll,
        );
        active_previews.previews[index] = preview;
        active_previews.applied_scroll = applied;
    }
    dashboard.preview_scroll = active_previews.applied_scroll;
    if let Some(preview_area) = render_sessions(
        frame,
        panes[0],
        panes[1],
        dashboard,
        &active,
        &archived,
        &active_previews.previews,
    ) {
        dashboard.selected_preview_area = Some(preview_area);
    }
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
        Mode::Confirm(dialog) => render_confirmation(frame, frame_area, dialog),
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

/// A launching, resuming, or pausing session — or one caught mid-provisioning
/// after an interrupted launch with no operation record — has nothing worth
/// previewing yet, so its Active row collapses to just the summary line.
fn active_row_collapses_to_summary(
    operation: Option<&SessionOperationDisplay>,
    state: SessionState,
) -> bool {
    match operation {
        Some(operation) => matches!(
            operation.kind,
            SessionOperationKind::Launching
                | SessionOperationKind::Resuming
                | SessionOperationKind::Pausing
        ),
        None => state == SessionState::Provisioning,
    }
}

/// What one frame asks of the active previews: the width they wrap to, which
/// row is selected, how far that row's preview is scrolled, and the line budget
/// the selected row may use.
struct PreviewRequest {
    width: u16,
    selected: Option<usize>,
    scroll: usize,
    selected_lines: usize,
}

fn prepare_active_previews(
    active: &[&SessionRecord],
    session_details: &mut BTreeMap<String, SessionDetail>,
    request: PreviewRequest,
    collapsed: &[bool],
) -> ActivePreviews {
    let mut previews = Vec::with_capacity(active.len());
    let mut applied_scroll = 0;
    for (index, session) in active.iter().enumerate() {
        if collapsed.get(index).copied().unwrap_or(false) {
            previews.push(Vec::new());
            continue;
        }
        let selected = request.selected == Some(index);
        let (maximum_lines, scroll) = if selected {
            (request.selected_lines, request.scroll)
        } else {
            (ACTIVE_MESSAGE_LINES, 0)
        };
        let detail = session_details.get_mut(&session.id);
        let (preview, applied) =
            active_transcript_tail(detail, request.width, maximum_lines, scroll);
        if selected {
            applied_scroll = applied;
        }
        previews.push(preview);
    }
    ActivePreviews {
        previews,
        applied_scroll,
    }
}

/// Preview rows for every active session, plus the scroll the selected
/// session's preview settled on. The caller decides whether to keep the clamped
/// scroll, because the pass that measures row heights runs against a provisional
/// line budget and would otherwise narrow how far the preview can scroll.
struct ActivePreviews {
    previews: Vec<Vec<Line<'static>>>,
    applied_scroll: usize,
}

const MINIMUM_TERMINAL_WIDTH: u16 = 32;

enum TerminalSizeRequirement {
    Width(u16),
    Height(u16),
}

fn render_terminal_too_small(frame: &mut Frame, area: Rect, requirement: TerminalSizeRequirement) {
    let instructions = match requirement {
        TerminalSizeRequirement::Width(required_width) => vec![
            Line::raw(format!("Need at least {required_width} columns.")),
            Line::raw(format!("Current width: {}.", area.width)),
        ],
        TerminalSizeRequirement::Height(required_height) => vec![Line::raw(format!(
            "Increase height to at least {required_height} rows (currently {}).",
            area.height
        ))],
    };
    frame.render_widget(Clear, area);
    let mut lines = vec![Line::styled(
        "Terminal too small",
        Style::default().add_modifier(Modifier::BOLD),
    )];
    lines.extend(instructions);
    frame.render_widget(Paragraph::new(lines).alignment(Alignment::Center), area);
}

fn render_import_progress(frame: &mut Frame, area: Rect, progress: &ImportProgress) {
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
    let paragraph = Paragraph::new(vec![
        Line::styled(
            truncate_text(&progress.session_title, 60),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Line::raw(""),
        Line::raw(progress.message.clone()),
        status,
        Line::raw(""),
        focused_buttons(IMPORT_PROGRESS_BUTTONS, 0),
    ])
    .block(Block::default().borders(Borders::ALL).title(format!(
        " Importing session · progress {}/{total} ",
        progress.step
    )))
    // `trim: false` keeps the padding inside the leftmost button background.
    .wrap(Wrap { trim: false });
    let popup = centered_rect(76, popup_height(&paragraph, 76, 10, area), area);
    frame.render_widget(Clear, popup);
    frame.render_widget(paragraph, popup);
}

fn render_import_bundle_confirmation(
    frame: &mut Frame,
    area: Rect,
    confirmation: &ImportBundleConfirmation,
) {
    let mut lines = Vec::new();
    if !confirmation.dirty_git_roots.is_empty() {
        lines.push(Line::raw(
            "These Git roots have local changes; Hel will archive tracked changes:",
        ));
        lines.extend(
            confirmation
                .dirty_git_roots
                .iter()
                .map(|root| Line::styled(root.clone(), Style::default().fg(Color::Yellow))),
        );
        if confirmation.has_untracked_files {
            lines.push(Line::raw(""));
            lines.push(Line::styled(
                format!(
                    "{} Ignore untracked files",
                    if confirmation.ignore_untracked {
                        "[x]"
                    } else {
                        "[ ]"
                    }
                ),
                Style::default().fg(Color::Cyan),
            ));
        }
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
    if !confirmation.scratch_git_roots.is_empty() {
        if !lines.is_empty() {
            lines.push(Line::raw(""));
        }
        lines.push(Line::raw(
            "These scratch repositories are under temporary directories and stay out of the workspace:",
        ));
        lines.extend(
            confirmation
                .scratch_git_roots
                .iter()
                .map(|root| Line::styled(root.clone(), Style::default().fg(Color::Yellow))),
        );
    }
    lines.push(Line::raw(""));
    if confirmation.has_untracked_files {
        lines.push(Line::raw("Space toggles the checkbox."));
        lines.push(Line::raw(""));
    }
    lines.push(focused_buttons(IMPORT_BUNDLE_BUTTONS, confirmation.focus));
    let paragraph = Paragraph::new(lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Import safety warning "),
        )
        // `trim: false` keeps the padding inside the leftmost button background.
        .wrap(Wrap { trim: false });
    let popup = centered_rect(76, popup_height(&paragraph, 76, 12, area), area);
    frame.render_widget(Clear, popup);
    frame.render_widget(paragraph, popup);
}

/// Popup height that keeps every wrapped line of `paragraph` visible, never
/// shrinking below the dialog's nominal height.
fn popup_height(paragraph: &Paragraph, width_percent: u16, nominal: u16, area: Rect) -> u16 {
    let inner_width = centered_rect(width_percent, 1, area)
        .width
        .saturating_sub(2);
    let wrapped = u16::try_from(paragraph.line_count(inner_width)).unwrap_or(u16::MAX);
    nominal.max(wrapped)
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
                profile.harness_kind.display_name()
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
    let filtered_sessions = dialog.filtered_sessions();
    let session_items = if filtered_sessions.is_empty() {
        selected_profile.map_or_else(Vec::new, |profile| {
            if let Some(error) = &profile.error {
                vec![ListItem::new(format!("Unavailable: {error}"))]
            } else if !dialog.filter.is_empty() && !profile.sessions.is_empty() {
                let message = if profile
                    .scan_progress
                    .is_none_or(|(scanned, total)| scanned < total)
                {
                    "No matches yet · scanning…"
                } else {
                    "No matching native sessions"
                };
                vec![ListItem::new(message)]
            } else if profile
                .scan_progress
                .is_none_or(|(scanned, total)| scanned < total)
            {
                vec![ListItem::new("Scanning native sessions…")]
            } else {
                vec![ListItem::new("No native sessions found")]
            }
        })
    } else {
        filtered_sessions
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
                        truncate_text(&session.title, panes[1].width.saturating_sub(4) as usize),
                        title_style,
                    ),
                    Line::styled(details, Style::default().fg(Color::Gray)),
                ])
            })
            .collect()
    };
    let selectable_sessions = !filtered_sessions.is_empty();
    let mut session_state =
        ListState::default().with_selected(selectable_sessions.then_some(dialog.session_index));
    let sessions_focused = dialog.focus == ImportFocus::Sessions;
    let filter_focused = dialog.focus == ImportFocus::Filter;
    let sessions_title = selected_profile
        .and_then(|profile| profile.scan_progress)
        .map(|(scanned, total)| {
            format!(" Native sessions · newest first · {scanned}/{total} sessions scanned ")
        })
        .unwrap_or_else(|| " Native sessions · newest first · scanning… ".into());
    let sessions_block = Block::default()
        .borders(Borders::ALL)
        .border_type(focus_border(sessions_focused || filter_focused))
        .title(sessions_title);
    let sessions_inner = sessions_block.inner(panes[1]);
    frame.render_widget(sessions_block, panes[1]);
    let session_rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Min(1)])
        .split(sessions_inner);
    let filter_cursor = if filter_focused { "▏" } else { "" };
    frame.render_widget(
        Paragraph::new(format!("Filter: {}{filter_cursor}", dialog.filter)).style(
            if filter_focused {
                Style::default().bg(Color::DarkGray).fg(Color::White)
            } else {
                Style::default().fg(Color::Gray)
            },
        ),
        session_rows[0],
    );
    frame.render_stateful_widget(
        List::new(session_items)
            .highlight_symbol(if sessions_focused { "› " } else { "  " })
            .highlight_style(if sessions_focused {
                Style::default().bg(Color::DarkGray).fg(Color::White)
            } else {
                Style::default()
            }),
        session_rows[1],
        &mut session_state,
    );
    let visible_sessions = usize::from(session_rows[1].height) / 2;
    render_session_scrollbar(
        frame,
        panes[1],
        filtered_sessions.len(),
        session_state.offset(),
        visible_sessions.max(1),
    );

    let unavailable_reason = dialog
        .selected_session()
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
    if dialog.is_scanning() {
        const SPINNER: [char; 4] = ['|', '/', '-', '\\'];
        let frame_index = (dialog.opened_at.elapsed().as_millis() / 125) as usize;
        frame.render_widget(
            Paragraph::new(format!(
                "{} Parsing sessions…",
                SPINNER[frame_index % SPINNER.len()]
            ))
            .style(Style::default().fg(Color::Gray)),
            Rect::new(rows[1].x, rows[1].y + rows[1].height - 1, 22, 1),
        );
    }
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

/// Draws the Active and Paused panes and reports the selected session's
/// preview hitbox, if one was drawn.
fn render_sessions(
    frame: &mut Frame,
    active_area: Rect,
    archived_area: Rect,
    dashboard: &DashboardState,
    active: &[&SessionRecord],
    archived: &[&SessionRecord],
    active_previews: &[Vec<Line<'static>>],
) -> Option<Rect> {
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
                    dashboard.session_operations.get(&session.id),
                    now_epoch_seconds,
                    &dashboard.config,
                    ActiveSessionRowLayout {
                        height: preview.len() as u16 + 1,
                        top_margin: u16::from(index > 0),
                        rule_width: usize::from(active_area.width),
                    },
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
    let mut selected_preview_area = None;
    for (index, session) in active.iter().enumerate().skip(active_offset) {
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
            let summary_area = Rect::new(
                active_area.x.saturating_add(1),
                info_y,
                active_area.width.saturating_sub(2),
                1,
            );
            let band = session_band_color(dashboard.session_details.get(&session.id));
            let buffer = frame.buffer_mut();
            buffer.set_style(summary_area, Style::default().bg(Color::DarkGray).fg(band));
            for x in summary_area.x..summary_area.right() {
                let cell = &mut buffer[(x, info_y)];
                if cell.symbol() == SUMMARY_RULE {
                    cell.fg = Color::Reset;
                }
            }
        }
        let preview_height = active_area
            .bottom()
            .saturating_sub(1)
            .saturating_sub(detail_y)
            .min(preview.len() as u16);
        if preview_height > 0 {
            let preview_area =
                Rect::new(active_area.x + 3, detail_y, preview_width, preview_height);
            if selected {
                selected_preview_area = Some(preview_area);
            }
            frame.render_widget(Paragraph::new(preview.clone()), preview_area);
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

    let archived_rows = archived.iter().map(|session| {
        archived_session_row(
            session,
            &dashboard.config,
            dashboard.session_operations.get(&session.id),
            dashboard
                .checkpoint_archive_sizes
                .get(&session.id)
                .copied()
                .flatten(),
        )
    });
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
    selected_preview_area
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

fn session_column_constraints() -> [Constraint; 6] {
    [
        Constraint::Length(18),
        Constraint::Length(10),
        // Leave a little air before the profile column.
        Constraint::Length(11),
        Constraint::Length(14),
        Constraint::Length(18),
        Constraint::Min(18),
    ]
}

fn archived_session_column_constraints() -> [Constraint; 5] {
    [
        Constraint::Length(18),
        Constraint::Length(14),
        Constraint::Length(15),
        Constraint::Length(7),
        Constraint::Min(18),
    ]
}

fn session_header() -> Row<'static> {
    Row::new([
        "Project",
        "Unread",
        "Turn clock",
        "Profile",
        "Target",
        "Session name",
    ])
    .style(Style::default().add_modifier(Modifier::BOLD))
}

fn archived_session_header() -> Row<'static> {
    Row::new(["Project", "Profile", "Archived", "Archive", "Session name"])
        .style(Style::default().add_modifier(Modifier::BOLD))
}

fn session_values(
    session: &SessionRecord,
    detail: Option<&SessionDetail>,
    operation: Option<&SessionOperationDisplay>,
    now_epoch_seconds: u64,
    config: &HelConfig,
) -> (String, String, String, String, String) {
    let clock = if let Some(operation) = operation {
        let (label, started_at) = match (operation.stage, operation.kind) {
            (Some(stage), SessionOperationKind::Launching | SessionOperationKind::Resuming) => (
                stage.label(),
                operation
                    .stage_started_at_epoch_seconds
                    .unwrap_or(operation.started_at_epoch_seconds),
            ),
            _ => (operation.kind.label(), operation.started_at_epoch_seconds),
        };
        let elapsed = now_epoch_seconds.saturating_sub(started_at);
        format!("{label} {elapsed}s")
    } else if session.state == SessionState::Provisioning {
        let started_at = session_updated_at_epoch_seconds(session).unwrap_or(now_epoch_seconds);
        format!("Launch {}s", now_epoch_seconds.saturating_sub(started_at))
    } else {
        hel::usage_format::format_turn_clock(
            now_epoch_seconds,
            detail.and_then(|detail| detail.current_turn_started_at),
        )
    };
    (
        clock,
        session.last_profile.clone(),
        session.target_template_id.clone(),
        session.project_name(config),
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

fn checkpoint_archive_size(size: Option<u64>) -> String {
    size.map(format_resource_bytes)
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

/// Color of an active session's summary band. A session whose detail has not
/// loaded yet keeps the default.
fn session_band_color(detail: Option<&SessionDetail>) -> Color {
    hel::hel_chat::turn_band_color(
        detail.is_none_or(|detail| detail.current_turn_started_at.is_some()),
    )
}

fn unread_line(unread_count: usize) -> Line<'static> {
    if unread_count > 0 {
        Line::from(Span::styled(
            format!("{unread_count} unread"),
            Style::default()
                .fg(Color::LightYellow)
                .add_modifier(Modifier::BOLD),
        ))
    } else {
        Line::default()
    }
}

fn active_message_tail(
    detail: Option<&SessionDetail>,
    width: usize,
    maximum_lines: usize,
) -> Vec<Line<'static>> {
    detail
        .and_then(|detail| detail.last_agent_message.as_deref())
        .map(|message| render_agent_message_tail(message, width, maximum_lines))
        .unwrap_or_default()
}

/// The preview rows for one active session, plus the scroll actually applied
/// after clamping to the history available.
fn active_transcript_tail(
    detail: Option<&mut SessionDetail>,
    width: u16,
    maximum_lines: usize,
    scroll: usize,
) -> (Vec<Line<'static>>, usize) {
    let Some(detail) = detail else {
        return (
            vec![Line::styled(
                "Loading conversation…",
                Style::default().fg(Color::DarkGray),
            )],
            0,
        );
    };
    match detail.transcript.as_mut() {
        Some(transcript) => transcript.rich_tail_scrolled(width, maximum_lines, scroll),
        None if detail.last_agent_message.is_some() => (
            active_message_tail(Some(detail), usize::from(width), maximum_lines),
            0,
        ),
        None => (
            vec![Line::styled(
                match detail.transcript_hydration {
                    TranscriptHydration::Loading => "Loading conversation…",
                    TranscriptHydration::Unavailable => "Conversation unavailable",
                    TranscriptHydration::Ready => "No messages yet",
                },
                Style::default().fg(Color::DarkGray),
            )],
            0,
        ),
    }
}

struct ActiveSessionRowLayout {
    height: u16,
    top_margin: u16,
    rule_width: usize,
}

fn active_session_row(
    session: &SessionRecord,
    detail: Option<&SessionDetail>,
    operation: Option<&SessionOperationDisplay>,
    now_epoch_seconds: u64,
    config: &HelConfig,
    layout: ActiveSessionRowLayout,
) -> Row<'static> {
    let (clock, profile, target, project, session_name) =
        session_values(session, detail, operation, now_epoch_seconds, config);
    let unread_count = detail.map_or(0, |detail| detail.unread_agent_messages);
    let band = session_band_color(detail);
    Row::new([
        summary_rule_cell(Line::raw(project), layout.rule_width, band),
        summary_rule_cell(unread_line(unread_count), layout.rule_width, band),
        summary_rule_cell(Line::raw(clock), layout.rule_width, band),
        summary_rule_cell(Line::raw(profile), layout.rule_width, band),
        summary_rule_cell(Line::raw(target), layout.rule_width, band),
        summary_rule_cell(
            Line::raw(recovery_warning_name(
                session,
                session_name,
                now_epoch_seconds,
            )),
            layout.rule_width,
            band,
        ),
    ])
    .height(layout.height)
    .top_margin(layout.top_margin)
}

/// Trails each summary column with the block's rule glyph so every active
/// session reads as its own band. Table cells clip the fill at the column edge,
/// so one pane-wide run works for every column.
fn summary_rule_cell(content: Line<'static>, rule_width: usize, band: Color) -> Cell<'static> {
    let mut spans = content.spans;
    for span in &mut spans {
        span.style = span.style.fg(band);
    }
    let gap = if spans.iter().all(|span| span.content.is_empty()) {
        ""
    } else {
        " "
    };
    spans.push(Span::styled(
        format!("{gap}{}", SUMMARY_RULE.repeat(rule_width)),
        // Reset keeps the rule the same weight as the surrounding block border.
        Style::default().fg(Color::Reset),
    ));
    Cell::from(Line::from(spans))
}

fn archived_session_row(
    session: &SessionRecord,
    config: &HelConfig,
    operation: Option<&SessionOperationDisplay>,
    archive_size: Option<u64>,
) -> Row<'static> {
    let checkpoint = session
        .checkpoint
        .as_ref()
        .map(|checkpoint| checkpoint_time_display(&checkpoint.created_at))
        .unwrap_or_else(|| "never".into());
    Row::new([
        session.project_name(config),
        session.last_profile.clone(),
        checkpoint,
        checkpoint_archive_size(archive_size),
        operation.map_or_else(
            || session_name(session).to_string(),
            |operation| format!("{}… {}", operation.kind.label(), session_name(session)),
        ),
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
        Row::new(["Host / fleet", "Targets", "In Use"])
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
            .title(" Capacity "),
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

fn quota_remaining_percent(window: &QuotaWindow) -> Option<u8> {
    window
        .remaining_percent
        .map(|value| value.min(100))
        .or_else(|| {
            let (Some(used), Some(limit)) = (window.used, window.limit) else {
                return None;
            };
            if limit <= 0 {
                return None;
            }
            let remaining = i128::from(limit.saturating_sub(used).clamp(0, limit));
            Some((remaining * 100 / i128::from(limit)) as u8)
        })
}

fn quota_bar(window: Option<&QuotaWindow>) -> Line<'static> {
    const CELLS: usize = 10;
    const EIGHTHS_PER_CELL: usize = 8;
    let Some(remaining) = window.and_then(quota_remaining_percent) else {
        return Line::default();
    };
    let eighths = (usize::from(remaining) * CELLS * EIGHTHS_PER_CELL + 50) / 100;
    let full_cells = eighths / EIGHTHS_PER_CELL;
    let partial_eighths = eighths % EIGHTHS_PER_CELL;
    let partial = ["", "▏", "▎", "▍", "▌", "▋", "▊", "▉"][partial_eighths];
    let empty_cells = CELLS
        .saturating_sub(full_cells)
        .saturating_sub(usize::from(partial_eighths > 0));
    let color = match remaining {
        0..=20 => Color::Red,
        21..=50 => Color::Yellow,
        _ => Color::Green,
    };
    let bar_style = Style::default().fg(color).add_modifier(Modifier::BOLD);
    Line::from(vec![
        Span::styled("█".repeat(full_cells), bar_style),
        Span::styled(partial.to_string(), bar_style),
        Span::styled(
            "░".repeat(empty_cells),
            Style::default().fg(Color::DarkGray),
        ),
        Span::styled(
            format!(" {remaining:>3}%"),
            Style::default().fg(color).add_modifier(Modifier::BOLD),
        ),
    ])
}

fn quota_reset_summary(quota: &ProfileQuota) -> String {
    let weekly = quota
        .weekly_window()
        .and_then(|window| window.resets.as_deref());
    let five_hour = quota
        .five_hour_projects_exhaustion()
        .then(|| quota.five_hour_window())
        .flatten()
        .and_then(|window| window.resets.as_deref());
    let mut summary = match (weekly, five_hour) {
        (Some(weekly), Some(five_hour)) => format!("{weekly} / {five_hour}"),
        (Some(weekly), None) => weekly.to_string(),
        (None, Some(five_hour)) => five_hour.to_string(),
        (None, None) => String::new(),
    };
    if let Some(extra) = quota.extra.as_deref() {
        if !summary.is_empty() {
            summary.push_str(" · ");
        }
        summary.push_str(extra);
    }
    summary
}

fn render_quotas(frame: &mut Frame, area: Rect, dashboard: &mut DashboardState) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let rows = dashboard.config.profiles.iter().map(|(id, profile)| {
        let (weekly, five_hour, resets) = if dashboard.quota_refreshing.contains(id) {
            (Line::raw("refreshing…"), Line::default(), String::new())
        } else {
            match dashboard.quotas.get(id) {
                Some(quota) if quota.error.is_none() => (
                    quota_bar(quota.weekly_window()),
                    quota_bar(quota.five_hour_window()),
                    quota_reset_summary(quota),
                ),
                Some(quota) => (
                    Line::raw(format!(
                        "unavailable: {}",
                        quota.error.as_deref().unwrap_or("unknown error")
                    )),
                    Line::default(),
                    String::new(),
                ),
                None => (Line::raw("refreshing…"), Line::default(), String::new()),
            }
        };
        Row::new([
            Cell::from(id.clone()),
            Cell::from(profile.kind.display_name()),
            Cell::from(weekly),
            Cell::from(five_hour),
            Cell::from(resets),
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
            Constraint::Percentage(10),
            Constraint::Percentage(22),
            Constraint::Percentage(22),
            Constraint::Percentage(32),
        ],
    )
    .header(
        Row::new(["Profile", "Harness", "Weekly", "5H", "Resets"])
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
            "[N]ew · impor[T] · [R]ename · [P]ause · [D]elete · [U]pdate quotas · [Q]uit · Tab pane"
        }
        Focus::Archived => {
            "[N]ew · impor[T] · [R]ename · [D]elete permanently · [U]pdate quotas · [Q]uit · Tab pane"
        }
        Focus::Capacity => "[N]ew · impor[T] · [U]pdate quotas · [Q]uit · Tab pane",
        Focus::Quotas => "[N]ew · impor[T] · [R]efresh · [U]pdate quotas · [Q]uit · Tab pane",
    };
    frame.render_widget(
        Paragraph::new(vec![
            Line::styled(
                format!(
                    "s sort: {} · {accelerator} for: {actions}",
                    dashboard.session_order.label()
                ),
                Style::default().fg(Color::DarkGray),
            ),
            Line::styled(
                dashboard.notices.current().unwrap_or_default(),
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

/// Renders a button row from its label list, highlighting the focused index.
fn focused_buttons(labels: &[&'static str], focus: usize) -> Line<'static> {
    let buttons = labels
        .iter()
        .enumerate()
        .map(|(index, label)| (*label, index == focus))
        .collect::<Vec<_>>();
    action_buttons(&buttons)
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
                queue: None,
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
    let (title, choices, selected): (_, Vec<String>, _) = match wizard.step {
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
        "+ double · - halve · c +8 CPU · m +50% memory · r reset"
    } else {
        "↑/↓ select · Tab moves focus · Enter activates"
    };
    render_picker(
        frame,
        area,
        title,
        choices.into_iter().map(PickerChoice::from).collect(),
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
    queue: Option<(usize, bool)>,
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
        queue,
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
    if let Some((count, discard)) = queue {
        lines.push(Line::raw(format!(
            "Queued prompts: {count} · {} (q toggles)",
            if discard {
                "discard on resume"
            } else {
                "start after resume"
            }
        )));
    }
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
                queue: dashboard
                    .session_details
                    .get(&wizard.session_id)
                    .map(|detail| detail.queued_prompts.len())
                    .filter(|count| *count > 0)
                    .map(|count| (count, wizard.discard_queue)),
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
                    PickerChoice::from(choice)
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
                    match dashboard.resume_target_rejection(&wizard.session_id, id) {
                        Some(reason) => PickerChoice {
                            text: format!("{id}  {}  · {reason}", target_label(target)),
                            disabled: true,
                        },
                        None => PickerChoice::from(format!("{id}  {}{size}", target_label(target))),
                    }
                })
                .collect(),
            wizard.target,
            &["+ double · - halve · c +8 CPU · m +50% memory · r reset"][..],
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

/// One picker row. A disabled row stays in the list so row numbers keep
/// matching the underlying map order; it is greyed out and refuses Enter.
#[derive(Debug, Clone)]
struct PickerChoice {
    text: String,
    disabled: bool,
}

impl From<String> for PickerChoice {
    fn from(text: String) -> Self {
        Self {
            text,
            disabled: false,
        }
    }
}

fn render_picker(
    frame: &mut Frame,
    area: Rect,
    title: &str,
    choices: Vec<PickerChoice>,
    selected: usize,
    help: &[&str],
    navigation: PickerNavigation,
) {
    let width_percent = if area.width < 64 { 100 } else { 68 };
    let popup = centered_rect(
        width_percent,
        (choices.len() as u16 + help.len() as u16 + 6).clamp(9, 19),
        area,
    );
    frame.render_widget(Clear, popup);
    let lines = choices
        .into_iter()
        .enumerate()
        .map(|(index, choice)| {
            let focused = index == selected && navigation.focus == WizardFocus::Content;
            let marker = if focused { "› " } else { "  " };
            let style = match (focused, choice.disabled) {
                (true, _) => Style::default().bg(Color::DarkGray).fg(Color::White),
                (false, true) => Style::default().fg(Color::DarkGray),
                (false, false) => Style::default(),
            };
            Line::styled(format!("{marker}{}", choice.text), style)
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
        Paragraph::new(lines).block(Block::default().borders(Borders::ALL).title(title)),
        popup,
    );
}

fn render_rename_editor(frame: &mut Frame, area: Rect, editor: &RenameEditor) {
    let paragraph = Paragraph::new(vec![
        Line::raw(format!("Session: {}", editor.session_id)),
        Line::raw(""),
        Line::styled(editor.title.clone(), Style::default().fg(Color::Cyan)),
        Line::raw(""),
        focused_buttons(RENAME_BUTTONS, editor.focus.button_index()),
    ])
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Rename session "),
    );
    let popup = centered_rect(60, popup_height(&paragraph, 60, 8, area), area);
    frame.render_widget(Clear, popup);
    frame.render_widget(paragraph, popup);
}

fn render_confirmation(frame: &mut Frame, area: Rect, dialog: &ConfirmDialog) {
    let confirmation = &dialog.confirmation;
    // Minimum height per dialog; `popup_height` grows it to fit wrapped content.
    let nominal = match confirmation {
        Confirmation::DirtyLocal { .. } => 11,
        Confirmation::CloseFailed { .. } => 12,
        Confirmation::Close { .. } | Confirmation::DeleteArchived { .. } => 10,
        Confirmation::ForceDestroy { .. } | Confirmation::DeleteActive { .. } => 9,
    };
    let (title, mut lines) = match confirmation {
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
            ]);
            (" Local repository has uncommitted changes ", lines)
        }
        Confirmation::Close { session_id } => (
            " Pause session? ",
            vec![
                Line::raw(format!("Session: {session_id}")),
                Line::raw(""),
                Line::raw("Hel will verify a recovery copy before destroying the target."),
            ],
        ),
        Confirmation::DeleteArchived { session_id } => (
            " Permanently delete paused session? ",
            vec![
                Line::raw(format!("Session: {session_id}")),
                Line::raw(""),
                Line::raw("Hel will permanently delete the recovery archive and session record."),
                Line::raw("Any Hel-managed worktree and generated branch will also be deleted."),
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
            ],
        ),
        Confirmation::ForceDestroy { session_id, typed } => (
            " FORCE DESTROY · DATA MAY BE LOST ",
            vec![
                Line::raw(format!("Session: {session_id}")),
                Line::raw(""),
                Line::raw("The Hel-managed worktree and generated branch will be deleted."),
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
    let buttons = confirmation_buttons(confirmation);
    if !buttons.is_empty() {
        lines.push(Line::raw(""));
        lines.push(focused_buttons(buttons, dialog.focus));
    }
    let paragraph = Paragraph::new(lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Red))
                .title(title),
        )
        // `trim: false` keeps the padding inside the leftmost button background.
        .wrap(Wrap { trim: false });
    let popup = centered_rect(72, popup_height(&paragraph, 72, nominal, area), area);
    frame.render_widget(Clear, popup);
    frame.render_widget(paragraph, popup);
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

fn import_sessions_pane(area: Rect) -> Rect {
    let popup = centered_rect(82, 22, area);
    let inner = popup.inner(Margin {
        vertical: 1,
        horizontal: 1,
    });
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(5), Constraint::Length(3)])
        .split(inner);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(36), Constraint::Percentage(64)])
        .split(rows[0])[1]
}

fn offset_index(index: usize, len: usize, delta: isize) -> usize {
    if len == 0 {
        return 0;
    }
    if delta.is_negative() {
        index.saturating_sub(delta.unsigned_abs())
    } else {
        index
            .saturating_add(delta as usize)
            .min(len.saturating_sub(1))
    }
}

fn move_index(index: &mut usize, len: usize, delta: isize) {
    if len == 0 {
        *index = 0;
        return;
    }
    if delta.is_negative() {
        *index = index.saturating_sub(delta.unsigned_abs());
    } else {
        *index = index
            .saturating_add(delta as usize)
            .min(len.saturating_sub(1));
    }
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
    use std::sync::Arc;

    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use hel::hel_config::{
        CONFIG_VERSION, ContainerTemplate, HarnessProfile, ProjectBundle, ProjectRepository,
        SshConnection,
    };
    use hel::hel_state::{CheckpointMetadata, STATE_VERSION, TranscriptItem};

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    /// The drawn buffer as one string per row.
    fn buffer_lines(buffer: &ratatui::buffer::Buffer) -> Vec<String> {
        (buffer.area.y..buffer.area.bottom())
            .map(|y| {
                (buffer.area.x..buffer.area.right())
                    .map(|x| buffer[(x, y)].symbol())
                    .collect::<String>()
            })
            .collect()
    }

    /// Column of `needle` within a drawn row, counted in cells rather than bytes.
    fn cell_column(line: &str, needle: &str) -> u16 {
        let byte = line
            .find(needle)
            .unwrap_or_else(|| panic!("missing {needle} in {line:?}"));
        line[..byte].chars().count() as u16
    }

    /// A drawn summary cell that holds session text rather than the rule fill.
    fn summary_text_cell(cell: &ratatui::buffer::Cell) -> bool {
        let symbol = cell.symbol();
        !symbol.trim().is_empty() && symbol != SUMMARY_RULE
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
            managed_worktree: None,
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
            detached_after_event_ordinal: 0,
            draft_input: String::new(),
            last_error: None,
            last_checkpoint_error: None,
            checkpoint: Some(CheckpointMetadata {
                archive_path: PathBuf::from("sessions/session-1.hel.zip"),
                sha256: "a".repeat(64),
                created_at: "2026-08-09T01:00:00Z".into(),
                event_frontier: 2,
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
    fn resume_is_projected_into_active_while_background_work_runs() {
        let mut dashboard = dashboard_with_session(archived_session());
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
    fn resuming_session_with_a_transcript_collapses_to_its_summary_line() {
        let mut session = archived_session();
        session.state = SessionState::Running;
        let mut dashboard = dashboard_with_session(session);
        apply_materialized_transcript(&mut dashboard, vec![agent_message(1, "Rendered answer")]);

        let mut terminal = Terminal::new(TestBackend::new(120, 36)).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw with preview");
        let rendered = buffer_lines(terminal.backend().buffer()).join("\n");
        assert!(rendered.contains("Rendered answer"));

        dashboard.begin_session_operation("session-1".into(), SessionOperationKind::Resuming, None);
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw collapsed");
        let collapsed = buffer_lines(terminal.backend().buffer()).join("\n");
        assert!(!collapsed.contains("Rendered answer"));
        assert!(
            dashboard.selected_preview_area.is_none(),
            "a collapsed row has no preview to scroll"
        );

        dashboard.finish_session_operation("session-1");
        // Finishing the operation only drops its record; the session state
        // itself flips back to Running through a later relay update, which a
        // real resume delivers via `spawn_lifecycle_reload` in main.rs.
        dashboard
            .state
            .sessions
            .get_mut("session-1")
            .expect("session")
            .state = SessionState::Running;
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw restored");
        let restored = buffer_lines(terminal.backend().buffer()).join("\n");
        assert!(restored.contains("Rendered answer"));
    }

    #[test]
    fn pausing_session_with_a_transcript_collapses_to_its_summary_line() {
        let mut session = archived_session();
        session.state = SessionState::Running;
        let mut dashboard = dashboard_with_session(session);
        apply_materialized_transcript(&mut dashboard, vec![agent_message(1, "Rendered answer")]);

        dashboard.begin_session_operation("session-1".into(), SessionOperationKind::Pausing, None);
        let mut terminal = Terminal::new(TestBackend::new(120, 36)).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw collapsed");
        let collapsed = buffer_lines(terminal.backend().buffer()).join("\n");
        assert!(!collapsed.contains("Rendered answer"));

        dashboard.finish_session_operation("session-1");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw restored");
        let restored = buffer_lines(terminal.backend().buffer()).join("\n");
        assert!(restored.contains("Rendered answer"));
    }

    #[test]
    fn provisioning_without_an_operation_record_collapses_to_its_summary_line() {
        // Interrupted-launch recovery: the session comes back as Provisioning
        // with no in-flight operation to track it.
        let mut session = archived_session();
        session.state = SessionState::Provisioning;
        let mut dashboard = dashboard_with_session(session);
        apply_materialized_transcript(&mut dashboard, vec![agent_message(1, "Rendered answer")]);
        assert!(dashboard.session_operations.is_empty());

        let mut terminal = Terminal::new(TestBackend::new(120, 36)).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw collapsed");
        let collapsed = buffer_lines(terminal.backend().buffer()).join("\n");
        assert!(!collapsed.contains("Rendered answer"));
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
    fn dashboard_replaces_layouts_narrower_than_32_columns() {
        let mut dashboard = DashboardState::new(config(), HelState::default(), BTreeMap::new());
        let mut terminal = Terminal::new(TestBackend::new(31, 24)).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw narrow dashboard");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("Terminal too small"));
        assert!(rendered.contains("Need at least 32 columns"));
        assert!(rendered.contains("Current width: 31"));

        let mut terminal = Terminal::new(TestBackend::new(32, 24)).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw exact minimum-width dashboard");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(!rendered.contains("Terminal too small"));
        assert!(rendered.contains("Active"));
    }

    #[test]
    fn new_session_picker_keeps_choices_and_controls_visible_at_minimum_width() {
        let mut dashboard = DashboardState::new(config(), HelState::default(), BTreeMap::new());
        assert_eq!(dashboard.handle_key(ctrl_key('n')), DashboardAction::None);
        let mut terminal = Terminal::new(TestBackend::new(32, 24)).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw minimum-width new-session picker");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(rendered.contains("claude-1"));
        assert!(rendered.contains("codex-2"));
        assert!(rendered.contains("Cancel"));
        assert!(rendered.contains("Next"));
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
            dashboard.handle_key(ctrl_key('t')),
            DashboardAction::OpenImport
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

    #[test]
    fn ctrl_q_quits_without_mutating_any_dashboard_modal() {
        let mut new_session = DashboardState::new(config(), HelState::default(), BTreeMap::new());
        assert_eq!(new_session.handle_key(ctrl_key('n')), DashboardAction::None);

        let mut resume = dashboard_with_session(archived_session());
        assert_eq!(
            resume.handle_key(key(KeyCode::Enter)),
            DashboardAction::None
        );

        let mut running = archived_session();
        running.state = SessionState::Running;
        running.checkpoint = None;
        let mut rename = dashboard_with_session(running);
        assert_eq!(rename.handle_key(ctrl_key('r')), DashboardAction::None);

        let mut import = dashboard_with_session(archived_session());
        import.show_import_dialog(1, Vec::new());

        let mut importing = dashboard_with_session(archived_session());
        importing.show_import_progress("Chosen session".into());

        let mut confirm_import = dashboard_with_session(archived_session());
        confirm_import.show_import_bundle_confirmation(Vec::new(), Vec::new(), Vec::new(), false);

        let mut confirm = dashboard_with_session(archived_session());
        confirm.show_dirty_local_confirmation(DashboardAction::None, vec!["project".into()]);

        for (label, mut dashboard) in [
            ("new session", new_session),
            ("resume", resume),
            ("rename", rename),
            ("import", import),
            ("import progress", importing),
            ("import confirmation", confirm_import),
            ("confirmation", confirm),
        ] {
            assert!(!matches!(dashboard.mode, Mode::Dashboard), "{label}");
            let mode_before_quit = dashboard.mode.clone();

            assert_eq!(
                dashboard.handle_key(ctrl_key('q')),
                DashboardAction::QuitDetach,
                "{label}"
            );
            assert_eq!(dashboard.mode, mode_before_quit, "{label}");
        }
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

    fn transcript_item(position: u64, body: TranscriptBody) -> Arc<TranscriptItem> {
        let at_ms = i64::try_from(position).unwrap() * 1_000;
        let latest_content_event_ordinal =
            matches!(&body, TranscriptBody::Agent { .. }).then_some(position);
        Arc::new(TranscriptItem {
            stable_id: format!("item-{position}"),
            position,
            latest_content_event_ordinal,
            created_at_ms: at_ms,
            last_changed_at_ms: at_ms,
            body,
        })
    }

    fn agent_message(position: u64, text: impl Into<String>) -> Arc<TranscriptItem> {
        transcript_item(
            position,
            TranscriptBody::Agent {
                chunks: vec![serde_json::json!({
                    "content": {"type": "text", "text": text.into()}
                })],
                streaming: false,
            },
        )
    }

    fn thought(position: u64, text: impl Into<String>) -> Arc<TranscriptItem> {
        transcript_item(
            position,
            TranscriptBody::Thought {
                chunks: vec![serde_json::json!({
                    "content": {"type": "text", "text": text.into()}
                })],
                streaming: false,
            },
        )
    }

    fn materialized_session_for(
        session_id: &str,
        transcript: Vec<Arc<TranscriptItem>>,
    ) -> MaterializedSession {
        let frontier = transcript
            .iter()
            .map(|item| item.position)
            .max()
            .unwrap_or(0);
        let mut session = MaterializedSession::empty(session_id);
        session.applied_event_ordinal = frontier;
        if frontier > 0 {
            session.applied_event_digest = "a".repeat(64);
        }
        session.execution = MaterializedExecutionState::Running {
            started_at_ms: 100_000,
        };
        session.last_activity_at_ms = transcript
            .iter()
            .map(|item| item.last_changed_at_ms)
            .max()
            .or(Some(100_000));
        session.transcript = transcript;
        session
    }

    fn apply_materialized_transcript(
        dashboard: &mut DashboardState,
        transcript: Vec<Arc<TranscriptItem>>,
    ) {
        apply_materialized_transcript_for(dashboard, "session-1", transcript);
    }

    fn apply_materialized_transcript_for(
        dashboard: &mut DashboardState,
        session_id: &str,
        transcript: Vec<Arc<TranscriptItem>>,
    ) {
        dashboard.apply_materialized_session(&materialized_session_for(session_id, transcript));
    }

    /// A conversation of `count` numbered exchanges, so preview scroll
    /// assertions can name the message they expect to see. Agent chunks
    /// coalesce unless separated, so each pairs with its own prompt.
    fn numbered_conversation(count: u64) -> Vec<Arc<TranscriptItem>> {
        (0..count)
            .flat_map(|index| {
                [
                    transcript_item(
                        index * 2 + 1,
                        TranscriptBody::User {
                            content: vec![serde_json::json!({
                                "type": "text",
                                "text": format!("question {index}"),
                            })],
                        },
                    ),
                    agent_message(index * 2 + 2, format!("answer {index}")),
                ]
            })
            .collect()
    }

    /// The text inside one rect, row by row. Session summary rows repeat the
    /// newest agent message, so preview assertions must look only at the
    /// preview itself.
    fn rows_in(terminal: &Terminal<TestBackend>, area: Rect) -> Vec<String> {
        let buffer = terminal.backend().buffer();
        (area.y..area.bottom())
            .map(|y| {
                (area.x..area.right())
                    .map(|x| buffer[(x, y)].symbol())
                    .collect::<String>()
                    .trim_end()
                    .to_owned()
            })
            .collect()
    }

    fn rect_shows(terminal: &Terminal<TestBackend>, area: Rect, needle: &str) -> bool {
        rows_in(terminal, area)
            .iter()
            .any(|row| row.contains(needle))
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
        let (active, archived) = partition_sessions(
            state.sessions.values(),
            &BTreeMap::new(),
            SessionOrder::Sequence,
        );
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
    fn sessions_are_ordered_by_creation_sequence_ascending_by_default() {
        let mut oldest = archived_session();
        oldest.id = "session-z".into();
        oldest.created_at = "2026-08-09T01:00:00Z".into();
        let mut newest = archived_session();
        newest.id = "session-y".into();
        newest.created_at = "2026-08-09T00:30:00-02:00".into();
        let mut invalid_timestamp = archived_session();
        invalid_timestamp.id = "session-a".into();
        invalid_timestamp.created_at = "unknown".into();

        let (_, archived) = partition_sessions(
            [&invalid_timestamp, &oldest, &newest],
            &BTreeMap::new(),
            SessionOrder::Sequence,
        );

        assert_eq!(
            archived
                .iter()
                .map(|session| session.id.as_str())
                .collect::<Vec<_>>(),
            ["session-z", "session-y", "session-a"]
        );
    }

    #[test]
    fn recent_activity_uses_projection_milliseconds_without_metadata_override() {
        let mut first = archived_session();
        first.id = "first".into();
        first.state = SessionState::Running;
        first.updated_at = "2099-01-01T00:00:00Z".into();
        let mut second = archived_session();
        second.id = "second".into();
        second.state = SessionState::Running;
        second.updated_at = "1970-01-01T00:00:00Z".into();
        let details = BTreeMap::from([
            (
                first.id.clone(),
                SessionDetail {
                    last_activity_at_ms: Some(10_001),
                    transcript_hydration: TranscriptHydration::Ready,
                    ..SessionDetail::default()
                },
            ),
            (
                second.id.clone(),
                SessionDetail {
                    last_activity_at_ms: Some(10_002),
                    transcript_hydration: TranscriptHydration::Ready,
                    ..SessionDetail::default()
                },
            ),
        ]);

        let (active, _) =
            partition_sessions([&first, &second], &details, SessionOrder::RecentActivity);

        assert_eq!(
            active
                .iter()
                .map(|session| session.id.as_str())
                .collect::<Vec<_>>(),
            ["second", "first"]
        );
    }

    #[test]
    fn hydrated_session_without_activity_does_not_sort_by_lifecycle_timestamp() {
        let mut hydrated = archived_session();
        hydrated.id = "hydrated".into();
        hydrated.state = SessionState::Running;
        hydrated.updated_at = "2099-01-01T00:00:00Z".into();
        let mut loading = archived_session();
        loading.id = "loading".into();
        loading.state = SessionState::Running;
        loading.updated_at = "2026-01-01T00:00:00Z".into();
        let details = BTreeMap::from([
            (
                hydrated.id.clone(),
                SessionDetail {
                    transcript_hydration: TranscriptHydration::Ready,
                    ..SessionDetail::default()
                },
            ),
            (loading.id.clone(), SessionDetail::default()),
        ]);

        let (active, _) = partition_sessions(
            [&hydrated, &loading],
            &details,
            SessionOrder::RecentActivity,
        );

        assert_eq!(active[0].id, "loading");
    }

    #[test]
    fn unavailable_session_keeps_its_committed_activity_watermark() {
        let mut disconnected = archived_session();
        disconnected.id = "disconnected".into();
        disconnected.state = SessionState::Disconnected;
        disconnected.updated_at = "2099-01-01T00:00:00Z".into();
        let mut connected = archived_session();
        connected.id = "connected".into();
        connected.state = SessionState::Running;
        connected.updated_at = "1970-01-01T00:00:00Z".into();
        let details = BTreeMap::from([
            (
                disconnected.id.clone(),
                SessionDetail {
                    last_activity_at_ms: Some(10_001),
                    transcript_hydration: TranscriptHydration::Unavailable,
                    ..SessionDetail::default()
                },
            ),
            (
                connected.id.clone(),
                SessionDetail {
                    last_activity_at_ms: Some(10_002),
                    transcript_hydration: TranscriptHydration::Ready,
                    ..SessionDetail::default()
                },
            ),
        ]);

        let (active, _) = partition_sessions(
            [&disconnected, &connected],
            &details,
            SessionOrder::RecentActivity,
        );

        assert_eq!(active[0].id, "connected");
    }

    #[test]
    fn sort_hotkey_round_robins_sequence_activity_and_profile() {
        let active_session = |id: &str| {
            let mut session = archived_session();
            session.id = id.into();
            session.state = SessionState::Running;
            session
        };
        let mut sessions = [
            active_session("session-a"),
            active_session("session-b"),
            active_session("session-c"),
        ];
        sessions[0].last_profile = "z-profile".into();
        sessions[1].last_profile = "a-profile".into();
        sessions[2].last_profile = "a-profile".into();
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

        assert_eq!(dashboard.session_order, SessionOrder::Sequence);
        assert_eq!(
            ordered_ids(&dashboard),
            ["session-a", "session-b", "session-c"]
        );
        dashboard.handle_key(key(KeyCode::Down));
        assert_eq!(dashboard.selected_session().unwrap().id, "session-b");

        let mut second = agent_message(1, "second");
        Arc::make_mut(&mut second).last_changed_at_ms = 2_000_000_100_000;
        apply_materialized_transcript_for(&mut dashboard, "session-b", vec![second]);
        dashboard.handle_key(key(KeyCode::Char('s')));
        assert_eq!(dashboard.session_order, SessionOrder::RecentActivity);
        assert_eq!(
            ordered_ids(&dashboard),
            ["session-b", "session-a", "session-c"]
        );
        assert_eq!(dashboard.selected_session().unwrap().id, "session-b");

        let mut later_thought = thought(1, "later thought");
        Arc::make_mut(&mut later_thought).last_changed_at_ms = 2_000_000_200_000;
        apply_materialized_transcript_for(&mut dashboard, "session-c", vec![later_thought]);
        assert_eq!(
            ordered_ids(&dashboard),
            ["session-c", "session-b", "session-a"]
        );

        let mut newest = agent_message(1, "newest");
        Arc::make_mut(&mut newest).last_changed_at_ms = 2_000_000_300_000;
        apply_materialized_transcript_for(&mut dashboard, "session-a", vec![newest]);
        assert_eq!(
            ordered_ids(&dashboard),
            ["session-a", "session-c", "session-b"]
        );

        dashboard.handle_key(key(KeyCode::Char('s')));
        assert_eq!(dashboard.session_order, SessionOrder::Profile);
        assert_eq!(
            ordered_ids(&dashboard),
            ["session-b", "session-c", "session-a"]
        );
        assert_eq!(dashboard.selected_session().unwrap().id, "session-a");

        dashboard.handle_key(key(KeyCode::Char('s')));
        assert_eq!(dashboard.session_order, SessionOrder::Sequence);
        assert_eq!(
            ordered_ids(&dashboard),
            ["session-a", "session-b", "session-c"]
        );
        assert_eq!(dashboard.selected_session().unwrap().id, "session-a");

        let mut later_tool = transcript_item(
            2,
            TranscriptBody::Tool {
                call: serde_json::json!({
                    "toolCallId": "later-tool",
                    "title": "later tool",
                    "status": "in_progress"
                }),
            },
        );
        Arc::make_mut(&mut later_tool).last_changed_at_ms = 2_000_000_400_000;
        apply_materialized_transcript_for(&mut dashboard, "session-b", vec![later_tool]);
        assert_eq!(
            ordered_ids(&dashboard),
            ["session-a", "session-b", "session-c"]
        );
    }

    #[test]
    fn unread_count_uses_logical_agent_positions_after_the_detach_cursor() {
        let mut session = archived_session();
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
        let mut session = archived_session();
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
        let mut session = archived_session();
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
    fn active_status_row_leads_with_project_and_separates_clock_from_profile() {
        let mut session = archived_session();
        session.state = SessionState::Running;
        let mut dashboard = dashboard_with_session(session);
        dashboard.focus = Focus::Quotas;
        apply_materialized_transcript(&mut dashboard, vec![agent_message(1, "hidden response")]);
        let mut terminal = Terminal::new(TestBackend::new(140, 28)).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw dashboard");
        let lines = (terminal.backend().buffer().area.y..terminal.backend().buffer().area.bottom())
            .map(|y| {
                (terminal.backend().buffer().area.x..terminal.backend().buffer().area.right())
                    .map(|x| terminal.backend().buffer()[(x, y)].symbol())
                    .collect::<String>()
            })
            .collect::<Vec<_>>();
        let header = lines
            .iter()
            .find(|line| line.contains("Turn clock") && line.contains("Profile"))
            .expect("active table header");
        assert!(header.find("Project") < header.find("Unread"));
        let clock_end = header.find("Turn clock").unwrap() + "Turn clock".len();
        let profile_start = header.find("Profile").unwrap();
        assert!(profile_start.saturating_sub(clock_end) >= 2);

        let status_y = lines
            .iter()
            .position(|line| line.contains("1 unread") && line.contains("codex-1"))
            .expect("active status row");
        let status = &lines[status_y];
        assert!(status.find("hel") < status.find("1 unread"));
        assert!(status.find("1 unread") < status.find("codex-1"));
        let buffer = terminal.backend().buffer();
        let status_y = buffer.area.y + status_y as u16;
        // The fixture's turn is still running, so the band keeps the default.
        assert!(
            (buffer.area.x + 1..buffer.area.right() - 1)
                .filter(|x| summary_text_cell(&buffer[(*x, status_y)]))
                .all(|x| buffer[(x, status_y)].fg == Color::Yellow)
        );
        assert!(
            (buffer.area.x + 1..buffer.area.right() - 1)
                .all(|x| buffer[(x, status_y)].bg != Color::DarkGray)
        );
        assert!(status.contains(SUMMARY_RULE));
        assert!(
            (buffer.area.x + 1..buffer.area.right() - 1)
                .filter(|x| buffer[(*x, status_y)].symbol() == SUMMARY_RULE)
                .all(|x| buffer[(x, status_y)].fg == Color::Reset)
        );
    }

    #[test]
    fn ended_turn_brightens_the_summary_band_even_after_output_is_read() {
        let mut session = archived_session();
        session.state = SessionState::Running;
        // The detach cursor sits past the only agent message, so nothing is
        // unread; the band still brightens because no turn is in flight.
        session.detached_after_event_ordinal = 1;
        let mut dashboard = dashboard_with_session(session);
        dashboard.focus = Focus::Quotas;
        let mut materialized =
            materialized_session_for("session-1", vec![agent_message(1, "seen response")]);
        materialized.execution = MaterializedExecutionState::Idle;
        dashboard.apply_materialized_session(&materialized);
        let mut terminal = Terminal::new(TestBackend::new(140, 28)).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw dashboard");
        let buffer = terminal.backend().buffer();
        let status_y = (buffer.area.y..buffer.area.bottom())
            .find(|y| {
                let row = (buffer.area.x..buffer.area.right())
                    .map(|x| buffer[(x, *y)].symbol())
                    .collect::<String>();
                row.contains("ACP pretty name")
            })
            .expect("active status row");
        let status = (buffer.area.x..buffer.area.right())
            .map(|x| buffer[(x, status_y)].symbol())
            .collect::<String>();
        assert!(!status.contains("unread"));
        assert!(
            (buffer.area.x + 1..buffer.area.right() - 1)
                .filter(|x| summary_text_cell(&buffer[(*x, status_y)]))
                .all(|x| buffer[(x, status_y)].fg == Color::LightYellow)
        );
    }

    #[test]
    fn later_non_agent_items_do_not_replace_the_last_agent_response() {
        let mut session = archived_session();
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
            address_source: hel::hel_config::AwsAddressSource::PublicIp,
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

        apply_materialized_transcript(&mut dashboard, vec![agent_message(1, "hello")]);
        assert_eq!(dashboard.handle_key(ctrl_key('d')), DashboardAction::None);
        assert!(matches!(
            dashboard.mode,
            Mode::Confirm(ConfirmDialog {
                confirmation: Confirmation::DeleteActive { .. },
                ..
            })
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
                discard_queue: false,
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
    fn resume_refuses_a_target_the_session_cannot_use_and_says_why() {
        let mut dashboard = dashboard_with_session(archived_session());
        dashboard
            .config
            .targets
            .insert("bare".into(), TargetTemplate::LocalBare);

        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Up));
        assert_eq!(
            nth_key(&dashboard.config.targets, resume_wizard(&dashboard).target),
            "bare"
        );

        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::None
        );

        assert_eq!(resume_wizard(&dashboard).step, WizardStep::Target);
        let notice = dashboard.notices.current().unwrap_or_default();
        assert!(notice.contains("created from a project bundle"), "{notice}");
    }

    #[test]
    fn resume_marks_an_unusable_target_row_as_disabled() {
        let mut dashboard = dashboard_with_session(archived_session());
        dashboard
            .config
            .targets
            .insert("bare".into(), TargetTemplate::LocalBare);
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Enter));

        let mut terminal =
            ratatui::Terminal::new(ratatui::backend::TestBackend::new(120, 30)).unwrap();
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .unwrap();
        let rendered = buffer_lines(terminal.backend().buffer()).join("\n");

        assert!(
            rendered.contains("created from a project bundle"),
            "{rendered}"
        );
    }

    fn resume_wizard(dashboard: &DashboardState) -> &ResumeWizard {
        let Mode::Resume(wizard) = &dashboard.mode else {
            panic!("expected resume wizard");
        };
        wizard
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
                discard_queue: false,
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
            address_source: hel::hel_config::AwsAddressSource::PublicIp,
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
        let Mode::Rename(editor) = &dashboard.mode else {
            panic!("expected rename editor");
        };
        assert_eq!(editor.focus, RenameFocus::Field);
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
        assert!(matches!(dashboard.mode, Mode::Dashboard));
    }

    fn dashboard_with_rename_editor() -> DashboardState {
        let mut dashboard = dashboard_with_session(archived_session());
        dashboard.handle_key(ctrl_key('r'));
        assert!(matches!(dashboard.mode, Mode::Rename(_)));
        dashboard
    }

    fn rename_focus(dashboard: &DashboardState) -> RenameFocus {
        let Mode::Rename(editor) = &dashboard.mode else {
            panic!("expected rename editor");
        };
        editor.focus
    }

    #[test]
    fn rename_editor_cycles_focus_from_the_field_through_both_buttons() {
        let mut dashboard = dashboard_with_rename_editor();
        for expected in [
            RenameFocus::Cancel,
            RenameFocus::Save,
            RenameFocus::Field,
            RenameFocus::Cancel,
        ] {
            assert_eq!(
                dashboard.handle_key(key(KeyCode::Tab)),
                DashboardAction::None
            );
            assert_eq!(rename_focus(&dashboard), expected);
        }

        let mut dashboard = dashboard_with_rename_editor();
        for expected in [RenameFocus::Save, RenameFocus::Cancel, RenameFocus::Field] {
            assert_eq!(
                dashboard.handle_key(key(KeyCode::BackTab)),
                DashboardAction::None
            );
            assert_eq!(rename_focus(&dashboard), expected);
        }
    }

    #[test]
    fn rename_editor_arrows_move_between_buttons_but_never_edit_the_field() {
        let mut dashboard = dashboard_with_rename_editor();
        // The field has no cursor, so arrows there change nothing.
        for arrow in [KeyCode::Left, KeyCode::Right] {
            assert_eq!(dashboard.handle_key(key(arrow)), DashboardAction::None);
            assert_eq!(rename_focus(&dashboard), RenameFocus::Field);
        }

        dashboard.handle_key(key(KeyCode::Tab));
        assert_eq!(rename_focus(&dashboard), RenameFocus::Cancel);
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Right)),
            DashboardAction::None
        );
        assert_eq!(rename_focus(&dashboard), RenameFocus::Save);
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Left)),
            DashboardAction::None
        );
        assert_eq!(rename_focus(&dashboard), RenameFocus::Cancel);
    }

    #[test]
    fn rename_editor_buttons_ignore_typing_and_backspace() {
        let mut dashboard = dashboard_with_rename_editor();
        dashboard.handle_key(key(KeyCode::Tab));
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Char('x'))),
            DashboardAction::None
        );
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Backspace)),
            DashboardAction::None
        );
        let Mode::Rename(editor) = &dashboard.mode else {
            panic!("expected rename editor");
        };
        assert_eq!(editor.title, "ACP pretty name");
        assert_eq!(editor.focus, RenameFocus::Cancel);
    }

    #[test]
    fn rename_editor_cancel_button_closes_without_renaming() {
        let mut dashboard = dashboard_with_rename_editor();
        dashboard.handle_key(key(KeyCode::Tab));
        assert_eq!(rename_focus(&dashboard), RenameFocus::Cancel);
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::None
        );
        assert!(matches!(dashboard.mode, Mode::Dashboard));
    }

    #[test]
    fn rename_editor_save_button_renames_like_the_field() {
        let mut dashboard = dashboard_with_rename_editor();
        dashboard.handle_key(key(KeyCode::Char('!')));
        dashboard.handle_key(key(KeyCode::Tab));
        dashboard.handle_key(key(KeyCode::Tab));
        assert_eq!(rename_focus(&dashboard), RenameFocus::Save);
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::RenameSession {
                session_id: "session-1".into(),
                title: "ACP pretty name!".into(),
            }
        );
    }

    #[test]
    fn rename_editor_rejects_an_empty_title_from_the_field_and_the_save_button() {
        for focus_moves in [0, 2] {
            let mut dashboard = dashboard_with_rename_editor();
            let Mode::Rename(editor) = &mut dashboard.mode else {
                panic!("expected rename editor");
            };
            editor.title.clear();
            for _ in 0..focus_moves {
                dashboard.handle_key(key(KeyCode::Tab));
            }
            assert_eq!(
                dashboard.handle_key(key(KeyCode::Enter)),
                DashboardAction::None,
                "{focus_moves} focus moves"
            );
            assert_eq!(
                dashboard.notice().as_deref(),
                Some("Session name cannot be empty."),
                "{focus_moves} focus moves"
            );
            assert!(matches!(dashboard.mode, Mode::Rename(_)), "{focus_moves}");
        }
    }

    #[test]
    fn rename_editor_escape_cancels_from_any_focus() {
        for focus_moves in 0..3 {
            let mut dashboard = dashboard_with_rename_editor();
            for _ in 0..focus_moves {
                dashboard.handle_key(key(KeyCode::Tab));
            }
            assert_eq!(
                dashboard.handle_key(key(KeyCode::Esc)),
                DashboardAction::None,
                "{focus_moves} focus moves"
            );
            assert!(matches!(dashboard.mode, Mode::Dashboard), "{focus_moves}");
        }
    }

    #[test]
    fn rename_editor_highlights_save_until_cancel_takes_focus() {
        let mut dashboard = dashboard_with_rename_editor();
        let mut terminal = Terminal::new(TestBackend::new(100, 24)).expect("terminal");
        let mut button_styles = |dashboard: &mut DashboardState| {
            terminal
                .draw(|frame| render(frame, dashboard))
                .expect("draw rename editor");
            let buffer = terminal.backend().buffer();
            let lines = buffer_lines(buffer);
            let row = lines
                .iter()
                .position(|line| line.contains(" Cancel ") && line.contains(" Save "))
                .expect("button row");
            let y = buffer.area.y + row as u16;
            assert!(!lines.iter().any(|line| line.contains("Enter save")));
            (
                buffer[(buffer.area.x + cell_column(&lines[row], "Cancel"), y)].bg,
                buffer[(buffer.area.x + cell_column(&lines[row], "Save"), y)].bg,
            )
        };

        // The field submits, so Save stays lit while the field has focus.
        assert_eq!(
            button_styles(&mut dashboard),
            (Color::DarkGray, Color::Cyan)
        );

        dashboard.handle_key(key(KeyCode::Tab));
        assert_eq!(
            button_styles(&mut dashboard),
            (Color::Cyan, Color::DarkGray)
        );

        dashboard.handle_key(key(KeyCode::Tab));
        assert_eq!(
            button_styles(&mut dashboard),
            (Color::DarkGray, Color::Cyan)
        );
    }

    #[test]
    fn import_progress_renders_a_focused_cancel_button_that_enter_presses() {
        let mut dashboard = dashboard_with_session(archived_session());
        dashboard.show_import_progress("Chosen session".into());
        let mut terminal = Terminal::new(TestBackend::new(100, 24)).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw import progress");
        let buffer = terminal.backend().buffer();
        let lines = buffer_lines(buffer);
        let row = lines
            .iter()
            .position(|line| line.contains(" Cancel "))
            .expect("button row");
        let y = buffer.area.y + row as u16;
        let cancel_x = buffer.area.x + cell_column(&lines[row], "Cancel");
        assert_eq!(buffer[(cancel_x, y)].bg, Color::Cyan);
        assert_eq!(buffer[(cancel_x - 1, y)].bg, Color::Cyan);
        assert!(!lines.iter().any(|line| line.contains("Esc cancels this")));
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::CancelImport
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
    fn keyboard_selection_stops_at_the_active_panes_ends_instead_of_wrapping() {
        let sessions = (0..3)
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

        assert_eq!(dashboard.session_index, 0);
        dashboard.handle_key(key(KeyCode::Up));
        assert_eq!(dashboard.session_index, 0, "Up at the first row stays put");

        dashboard.handle_key(key(KeyCode::Down));
        dashboard.handle_key(key(KeyCode::Down));
        assert_eq!(dashboard.session_index, 2);
        dashboard.handle_key(key(KeyCode::Down));
        assert_eq!(dashboard.session_index, 2, "Down at the last row stays put");
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
        let buffer = terminal.backend().buffer();
        let header = (buffer.area.y..buffer.area.bottom())
            .map(|y| {
                (buffer.area.x..buffer.area.right())
                    .map(|x| buffer[(x, y)].symbol())
                    .collect::<String>()
            })
            .find(|line| line.contains("Host / fleet") && line.contains("Targets"))
            .expect("capacity header");
        assert!(header.contains("In Use"));
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

    /// A dashboard with `count` running sessions, each carrying a numbered
    /// conversation long enough to scroll.
    fn dashboard_with_conversations(count: usize) -> DashboardState {
        let sessions = (0..count)
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
        let transcript = numbered_conversation(14);
        for index in 0..count {
            apply_materialized_transcript_for(
                &mut dashboard,
                &format!("session-{index}"),
                transcript.clone(),
            );
        }
        dashboard
    }

    #[test]
    fn wheel_over_the_selected_preview_scrolls_its_conversation_not_the_session_list() {
        let mut dashboard = dashboard_with_conversations(3);
        let mut terminal = Terminal::new(TestBackend::new(120, 40)).expect("test terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw previews");
        let preview = dashboard
            .selected_preview_area
            .expect("the selected session exposes a preview hitbox");
        assert!(
            rect_shows(&terminal, preview, "answer 13"),
            "opens on the tail"
        );

        dashboard.handle_mouse(mouse_in(MouseEventKind::ScrollUp, preview));
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw scrolled preview");

        assert!(
            !rect_shows(&terminal, preview, "answer 13"),
            "the wheel scrolled the preview above its live tail"
        );
        assert!(
            rect_shows(&terminal, preview, "question 12"),
            "older rows came into view"
        );
        assert_eq!(
            dashboard.session_index, 0,
            "scrolling a preview must not move the selection"
        );
        assert_eq!(dashboard.focus, Focus::Active);
    }

    #[test]
    fn wheel_below_the_selected_preview_still_moves_the_session_selection() {
        let mut dashboard = dashboard_with_conversations(3);
        let mut terminal = Terminal::new(TestBackend::new(120, 40)).expect("test terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw previews");
        let pane_areas = dashboard.pane_areas.expect("dashboard pane hitboxes");
        assert!(dashboard.selected_preview_area.is_some());

        // The Active pane's own header sits outside every preview hitbox.
        dashboard.handle_mouse(mouse_in(MouseEventKind::ScrollDown, pane_areas[0]));

        assert_eq!(dashboard.session_index, 2);
        assert_eq!(dashboard.preview_scroll, 0);
    }

    #[test]
    fn selecting_another_session_snaps_the_previous_preview_back_to_its_tail() {
        let mut dashboard = dashboard_with_conversations(3);
        let mut terminal = Terminal::new(TestBackend::new(120, 40)).expect("test terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw previews");
        let preview = dashboard.selected_preview_area.expect("preview hitbox");
        dashboard.handle_mouse(mouse_in(MouseEventKind::ScrollUp, preview));
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw scrolled preview");
        assert!(dashboard.preview_scroll > 0, "the preview is scrolled back");

        dashboard.move_selection(1);
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw after moving the selection");
        assert_eq!(
            dashboard.preview_scroll, 0,
            "the new selection starts at its own tail"
        );

        dashboard.move_selection(-1);
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw after returning to the first session");

        assert_eq!(dashboard.preview_scroll, 0);
        let preview = dashboard.selected_preview_area.expect("preview hitbox");
        assert!(
            rect_shows(&terminal, preview, "answer 13"),
            "the preview left behind snapped back to its live tail"
        );
    }

    #[test]
    fn preview_scroll_reaches_the_oldest_row_in_a_constrained_terminal() {
        // A short pane gives the selected preview fewer lines than
        // SELECTED_TRANSCRIPT_LINES, so the provisional sizing pass must not cap
        // how far the preview can scroll.
        let mut dashboard = dashboard_with_conversations(1);
        let mut terminal = Terminal::new(TestBackend::new(120, 26)).expect("test terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw previews");
        let preview = dashboard
            .selected_preview_area
            .expect("preview hitbox in a short terminal");
        assert!(
            preview.height < SELECTED_TRANSCRIPT_LINES as u16,
            "the preview must be line-constrained for this test to mean anything"
        );

        for _ in 0..60 {
            dashboard.handle_mouse(mouse_in(MouseEventKind::ScrollUp, preview));
            terminal
                .draw(|frame| render(frame, &mut dashboard))
                .expect("draw scrolled preview");
        }

        assert!(
            rect_shows(&terminal, preview, "question 0"),
            "a constrained preview still reaches the oldest message"
        );
    }

    #[test]
    fn preview_scroll_stops_at_the_oldest_row_of_the_conversation() {
        let mut dashboard = dashboard_with_conversations(1);
        let mut terminal = Terminal::new(TestBackend::new(120, 40)).expect("test terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw previews");
        let preview = dashboard.selected_preview_area.expect("preview hitbox");

        for _ in 0..40 {
            dashboard.handle_mouse(mouse_in(MouseEventKind::ScrollUp, preview));
            terminal
                .draw(|frame| render(frame, &mut dashboard))
                .expect("draw scrolled preview");
        }
        let clamped = dashboard.preview_scroll;

        assert!(
            rect_shows(&terminal, preview, "question 0"),
            "reaches the oldest message"
        );
        dashboard.handle_mouse(mouse_in(MouseEventKind::ScrollUp, preview));
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw at the top");
        assert_eq!(
            dashboard.preview_scroll, clamped,
            "scrolling past the oldest row is clamped"
        );
    }

    /// Active sessions whose previews differ in length, plus paused sessions,
    /// capacity rows, and quota rows, so every pane contributes to the layout.
    fn mixed_fleet_dashboard() -> DashboardState {
        let sessions = (0..4)
            .map(|index| {
                let mut session = archived_session();
                session.id = format!("session-{index}");
                if index < 2 {
                    session.state = SessionState::Running;
                }
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
        apply_materialized_transcript_for(&mut dashboard, "session-0", numbered_conversation(14));
        apply_materialized_transcript_for(&mut dashboard, "session-1", numbered_conversation(1));
        dashboard.set_deployment_capacity_targets(vec![test_capacity_target()]);
        dashboard
    }

    /// Pane rectangles, the selected preview hitbox, and the row each session
    /// summary and preview line lands on. Volatile text (turn clocks, message
    /// timestamps) is reduced to a tag so the fingerprint is pure geometry.
    fn layout_fingerprint(terminal: &Terminal<TestBackend>, dashboard: &DashboardState) -> String {
        let mut lines = Vec::new();
        for (index, area) in dashboard
            .pane_areas
            .expect("dashboard pane hitboxes")
            .iter()
            .enumerate()
        {
            lines.push(format!(
                "pane {index}: {},{} {}x{}",
                area.x, area.y, area.width, area.height
            ));
        }
        let preview = dashboard
            .selected_preview_area
            .expect("selected preview hitbox");
        lines.push(format!(
            "preview: {},{} {}x{}",
            preview.x, preview.y, preview.width, preview.height
        ));
        let buffer = terminal.backend().buffer();
        for y in buffer.area.y..buffer.area.bottom() {
            let text = (buffer.area.x..buffer.area.right())
                .map(|x| buffer[(x, y)].symbol())
                .collect::<String>();
            let text = text.trim_end();
            if text.contains("unread") {
                lines.push(format!("row {y}: session summary"));
            } else if let Some(start) = text.find("question ").or_else(|| text.find("answer ")) {
                // Trailing pane border and scrollbar glyphs are not part of the
                // message text.
                let message = text[start..].trim_end_matches(['║', '│', '█', '▲', '▼', ' ']);
                lines.push(format!("row {y}: {message}"));
            }
        }
        lines.join("\n")
    }

    /// Locks the Active pane's layout for a fleet that mixes preview lengths
    /// with paused, capacity, and quota rows. Both the row-sizing pass and the
    /// pass that renders previews feed these numbers.
    #[test]
    fn dashboard_layout_is_stable_for_a_mixed_fleet() {
        let mut dashboard = mixed_fleet_dashboard();
        let mut terminal = Terminal::new(TestBackend::new(120, 40)).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw the mixed fleet");

        assert_eq!(
            layout_fingerprint(&terminal, &dashboard),
            concat!(
                "pane 0: 0,1 120x22\n",
                "pane 1: 0,23 120x5\n",
                "pane 2: 0,28 120x4\n",
                "pane 3: 0,32 120x6\n",
                "preview: 3,4 116x10\n",
                "row 3: session summary\n",
                "row 5: answer 11\n",
                "row 7: question 12\n",
                "row 9: answer 12\n",
                "row 11: question 13\n",
                "row 13: answer 13\n",
                "row 15: session summary\n",
                "row 17: question 0\n",
                "row 19: answer 0",
            )
        );
    }

    /// The same fleet in a terminal too short for the full preview budget, so
    /// the selected session's line allowance comes from the allocated pane.
    #[test]
    fn dashboard_layout_is_stable_when_the_active_pane_is_squeezed() {
        let mut dashboard = mixed_fleet_dashboard();
        let mut terminal = Terminal::new(TestBackend::new(120, 26)).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw the squeezed fleet");

        assert_eq!(
            layout_fingerprint(&terminal, &dashboard),
            concat!(
                "pane 0: 0,1 120x9\n",
                "pane 1: 0,10 120x5\n",
                "pane 2: 0,15 120x4\n",
                "pane 3: 0,19 120x5\n",
                "preview: 3,4 116x5\n",
                "row 3: session summary\n",
                "row 4: answer 12\n",
                "row 6: question 13\n",
                "row 8: answer 13",
            )
        );
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
            dashboard.notice().as_deref(),
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
                    project_directory: "/home/user/Projects/hel".into(),
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
                    project_directory: "/home/user/Projects/hel".into(),
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
        dashboard.handle_key(key(KeyCode::Enter));
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
    fn import_dialog_focuses_profiles_then_filters_each_selected_profile() {
        let mut dashboard = dashboard_with_session(archived_session());
        let session = |id: &str, title: &str, project_directory: &str| ImportSessionOption {
            native_session_id: id.into(),
            title: title.into(),
            project_directory: project_directory.into(),
            details: format!("just now · master · 1.0KB · {project_directory}"),
            unavailable_reason: None,
        };
        let profiles = vec![
            ImportProfileOption {
                profile_id: "codex-1".into(),
                harness_kind: HarnessKind::Codex,
                sessions: vec![
                    session("title-only", "Hel project", "/work/other"),
                    session("codex-match", "Matching cwd", "/work/Projects/HEL"),
                ],
                scan_progress: Some((2, 2)),
                error: None,
            },
            ImportProfileOption {
                profile_id: "claude-1".into(),
                harness_kind: HarnessKind::Claude,
                sessions: vec![session(
                    "claude-match",
                    "Claude matching cwd",
                    "/Users/me/projects/hel",
                )],
                scan_progress: Some((1, 1)),
                error: None,
            },
        ];
        dashboard.show_import_dialog(1, profiles);

        let Mode::Import(dialog) = &dashboard.mode else {
            panic!("expected import dialog");
        };
        assert_eq!(dialog.focus, ImportFocus::Profiles);

        dashboard.handle_key(key(KeyCode::Tab));
        dashboard.handle_paste("hEl\n");
        let Mode::Import(dialog) = &dashboard.mode else {
            panic!("expected import dialog");
        };
        assert_eq!(dialog.focus, ImportFocus::Filter);
        assert_eq!(dialog.filter, "hEl");
        assert_eq!(
            dialog
                .filtered_sessions()
                .iter()
                .map(|session| session.native_session_id.as_str())
                .collect::<Vec<_>>(),
            vec!["codex-match"]
        );

        dashboard.handle_key(key(KeyCode::Down));
        let Mode::Import(dialog) = &dashboard.mode else {
            panic!("expected import dialog");
        };
        assert_eq!(dialog.focus, ImportFocus::Sessions);
        dashboard.handle_key(key(KeyCode::Up));
        let Mode::Import(dialog) = &dashboard.mode else {
            panic!("expected import dialog");
        };
        assert_eq!(dialog.focus, ImportFocus::Filter);

        dashboard.handle_key(key(KeyCode::Left));
        dashboard.handle_key(key(KeyCode::Down));
        dashboard.handle_key(key(KeyCode::Right));
        let Mode::Import(dialog) = &dashboard.mode else {
            panic!("expected import dialog");
        };
        assert_eq!(dialog.focus, ImportFocus::Filter);
        assert_eq!(dialog.filter, "hEl");
        assert_eq!(
            dialog.filtered_sessions()[0].native_session_id,
            "claude-match"
        );
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Enter));
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::ImportSession {
                profile_id: "claude-1".into(),
                native_session_id: "claude-match".into(),
                display_title: "Claude matching cwd".into(),
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
                project_directory: "/home/user/Projects/hel".into(),
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
        dashboard.handle_key(key(KeyCode::Enter));

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
    fn import_safety_defaults_to_ignoring_untracked_files_and_can_include_them() {
        let mut dashboard = dashboard_with_session(archived_session());
        dashboard.show_import_bundle_confirmation(
            vec!["/work/repo — 1 tracked change · 222561 untracked paths".into()],
            Vec::new(),
            Vec::new(),
            true,
        );
        let mut terminal = Terminal::new(TestBackend::new(120, 30)).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw safety warning");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("[x] Ignore untracked files"));
        assert!(rendered.contains(" Cancel "));
        assert!(rendered.contains(" Continue "));
        assert!(rendered.contains("Space toggles the checkbox."));
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::ConfirmImportBundle {
                accepted: true,
                include_untracked: false,
            }
        );

        dashboard.show_import_bundle_confirmation(
            vec!["/work/repo — 222561 untracked paths".into()],
            Vec::new(),
            Vec::new(),
            true,
        );
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Char(' '))),
            DashboardAction::None
        );
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::ConfirmImportBundle {
                accepted: true,
                include_untracked: true,
            }
        );
    }

    #[test]
    fn import_safety_lists_scratch_repositories_left_out_of_the_workspace() {
        let mut dashboard = dashboard_with_session(archived_session());
        dashboard.show_import_bundle_confirmation(
            Vec::new(),
            Vec::new(),
            vec!["/tmp/claude-1000/scratch".into()],
            false,
        );
        let mut terminal = Terminal::new(TestBackend::new(120, 30)).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw safety warning");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(rendered.contains("temporary directories"), "{rendered}");
        assert!(rendered.contains("/tmp/claude-1000/scratch"), "{rendered}");
    }

    #[test]
    fn import_safety_buttons_toggle_the_checkbox_and_cancel_from_the_cancel_button() {
        let mut dashboard = dashboard_with_session(archived_session());
        dashboard.show_import_bundle_confirmation(
            vec!["/work/repo — 1 tracked change · 3 untracked paths".into()],
            Vec::new(),
            Vec::new(),
            true,
        );

        // Focus starts on Continue; moving to Cancel does not disturb the checkbox.
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Tab)),
            DashboardAction::None
        );
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Char(' '))),
            DashboardAction::None
        );
        let Mode::ConfirmImportBundle(confirmation) = &dashboard.mode else {
            panic!("expected import safety confirmation");
        };
        assert!(!confirmation.ignore_untracked);
        assert_eq!(confirmation.focus, 0);
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::ConfirmImportBundle {
                accepted: false,
                include_untracked: false,
            }
        );

        dashboard.show_import_bundle_confirmation(Vec::new(), Vec::new(), Vec::new(), false);
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Char('y'))),
            DashboardAction::None
        );
        assert!(matches!(dashboard.mode, Mode::ConfirmImportBundle(_)));
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Esc)),
            DashboardAction::ConfirmImportBundle {
                accepted: false,
                include_untracked: false,
            }
        );
    }

    #[test]
    fn incremental_import_results_preserve_the_selected_session() {
        let mut dashboard = dashboard_with_session(archived_session());
        let session = |id: &str| ImportSessionOption {
            native_session_id: id.into(),
            title: "Same title".into(),
            project_directory: "/home/user/Projects/hel".into(),
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
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Down));
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
                project_directory: "/home/user/Projects/hel".into(),
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
        assert!(rendered.contains("Filter:"));
        assert!(rendered.contains("codex-1"));
        assert!(rendered.contains("Native session title"));
        assert!(rendered.contains("1.0MB"));
        assert!(rendered.contains("~/Projects/hel"));
        assert!(rendered.contains("1/1 sessions scanned"));
        assert!(!rendered.contains("Parsing sessions"));
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
                project_directory: "/home/user/Projects/hel".into(),
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
            (ImportFocus::Filter, 0),
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
    fn import_session_list_renders_a_scrollbar_and_accepts_mouse_wheel() {
        let mut dashboard = dashboard_with_session(archived_session());
        let sessions = (0..20)
            .map(|index| ImportSessionOption {
                native_session_id: format!("native-session-{index}"),
                title: format!("Native session {index}"),
                project_directory: "/home/user/Projects/hel".into(),
                details: "2m ago · master · 1.0MB · ~/Projects/hel".into(),
                unavailable_reason: None,
            })
            .collect();
        dashboard.show_import_dialog(
            1,
            vec![ImportProfileOption {
                profile_id: "codex-1".into(),
                harness_kind: HarnessKind::Codex,
                sessions,
                scan_progress: Some((20, 20)),
                error: None,
            }],
        );
        let mut terminal = Terminal::new(TestBackend::new(120, 30)).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw import dialog");
        let symbols = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<Vec<_>>();
        assert!(symbols.contains(&"▲"));
        assert!(symbols.contains(&"▼"));

        let sessions_area = dashboard
            .import_sessions_area
            .expect("import session pane hitbox");
        dashboard.handle_mouse(mouse_in(MouseEventKind::ScrollDown, sessions_area));
        let Mode::Import(dialog) = &dashboard.mode else {
            panic!("expected import dialog");
        };
        assert_eq!(dialog.focus, ImportFocus::Sessions);
        assert_eq!(dialog.session_index, MOUSE_SCROLL_ROWS as usize);

        dashboard.handle_mouse(mouse_in(MouseEventKind::ScrollUp, sessions_area));
        let Mode::Import(dialog) = &dashboard.mode else {
            panic!("expected import dialog");
        };
        assert_eq!(dialog.session_index, 0);
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
        assert!(rendered.contains("Parsing sessions"));
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
    fn active_session_renders_the_complete_last_agent_message() {
        let mut session = archived_session();
        session.state = SessionState::Running;
        let mut dashboard = dashboard_with_session(session);
        dashboard.focus = Focus::Quotas;
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
        apply_materialized_transcript(&mut dashboard, vec![agent_message(1, "**a b**\nc")]);
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
        assert!(rendered.contains("Turn clock"));
        assert!(rendered.contains("Profile"));
        assert!(rendered.contains("Target"));
        assert!(!rendered.contains("Checkpoint"));
        assert!(rendered.contains("Project"));
        assert!(rendered.contains("hel"));
        assert!(!rendered.contains("C 37% · M 50%"));
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
        let transcript = vec![
            transcript_item(
                1,
                TranscriptBody::User {
                    content: vec![serde_json::json!({
                        "type": "text",
                        "text": "inspect the dashboard",
                    })],
                },
            ),
            agent_message(2, "**Rendered answer**"),
            transcript_item(
                3,
                TranscriptBody::Tool {
                    call: serde_json::json!({
                        "toolCallId": "dashboard-tests",
                        "title": "Run dashboard tests",
                        "status": "completed"
                    }),
                },
            ),
        ];
        apply_materialized_transcript(&mut dashboard, transcript);
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
        // The fixture's turn is still running, so the band keeps the default.
        assert!(
            (buffer.area.x + 1..buffer.area.right() - 1)
                .filter(|x| summary_text_cell(&buffer[(*x, info_y)]))
                .all(|x| buffer[(x, info_y)].fg == Color::Yellow)
        );
        // The rule fills the gaps and stays the color of the block border.
        assert!(
            (buffer.area.x + 1..buffer.area.right() - 1)
                .any(|x| buffer[(x, info_y)].symbol() == SUMMARY_RULE)
        );
        assert!(
            (buffer.area.x + 1..buffer.area.right() - 1)
                .filter(|x| buffer[(*x, info_y)].symbol() == SUMMARY_RULE)
                .all(|x| buffer[(x, info_y)].fg == Color::Reset
                    && buffer[(x, info_y)].bg == Color::DarkGray)
        );
        assert!(
            (buffer.area.x + 1..buffer.area.right() - 1)
                .all(|x| buffer[(x, conversation_y)].bg != Color::DarkGray)
        );
        let answer_x = row_text(conversation_y)
            .find("Rendered answer")
            .expect("conversation text") as u16;
        assert_ne!(
            buffer[(buffer.area.x + answer_x, conversation_y)].fg,
            Color::Yellow
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
        assert!(blurred.contains("Run dashboard tests"));
    }

    #[test]
    fn selected_transcript_tail_adapts_to_a_constrained_terminal() {
        let mut session = archived_session();
        session.state = SessionState::Running;
        let mut dashboard = dashboard_with_session(session);
        let message = (1..=20)
            .map(|line| format!("line {line}"))
            .collect::<Vec<_>>()
            .join("\n");
        apply_materialized_transcript(&mut dashboard, vec![agent_message(1, message)]);
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
            apply_materialized_transcript_for(
                &mut dashboard,
                &format!("active-{index:02}"),
                vec![agent_message(1, "one\ntwo\nthree\nfour")],
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
    fn archived_sessions_omit_turn_clock_and_target_infrastructure() {
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
        assert!(rendered.contains("Project"));
        assert!(rendered.contains("hel"));
        assert!(!rendered.contains("podman: hel"));
        assert!(rendered.contains("26-08-09 01:00"));
        assert!(!rendered.contains("2026-08-09T01:00:00Z"));
        assert!(!rendered.contains("idle"));
    }

    #[test]
    fn archived_columns_leave_all_remaining_width_for_the_complete_session_name() {
        let long_name =
            "A session name that is deliberately longer than sixty-four characters but still fits";
        let mut session = archived_session();
        session.acp_session_title = Some(long_name.into());
        let mut dashboard = dashboard_with_session(session);
        let mut terminal = Terminal::new(TestBackend::new(160, 36)).expect("terminal");

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
        let header = lines
            .iter()
            .find(|line| {
                line.contains("Project")
                    && line.contains("Profile")
                    && line.contains("Archived")
                    && line.contains("Session name")
            })
            .expect("archived header");
        let project = header.find("Project").unwrap();
        let profile = header.find("Profile").unwrap();
        let archived = header.find("Archived").unwrap();
        let archive = header[archived + "Archived".len()..]
            .find("Archive")
            .map(|offset| offset + archived + "Archived".len())
            .unwrap();
        let session_name = header.find("Session name").unwrap();

        assert_eq!(profile - project, 19);
        assert_eq!(archived - profile, 15);
        assert_eq!(archive - archived, 16);
        assert_eq!(session_name - archive, 8);
        assert!(lines.iter().any(|line| line.contains(long_name)));
    }

    #[test]
    fn archived_resources_show_the_checkpoint_archive_size() {
        assert_eq!(checkpoint_archive_size(Some(1_536)), "1.5K");
        assert_eq!(checkpoint_archive_size(None), "—");
    }

    #[test]
    fn archived_resources_use_cached_checkpoint_archive_size() {
        let mut dashboard = dashboard_with_session(archived_session());
        dashboard.apply_checkpoint_archive_sizes(BTreeMap::from([(
            "session-1".to_string(),
            Some(1_536),
        )]));

        assert_eq!(
            dashboard
                .checkpoint_archive_sizes
                .get("session-1")
                .copied()
                .flatten(),
            Some(1_536)
        );
        assert_eq!(
            checkpoint_archive_size(
                dashboard
                    .checkpoint_archive_sizes
                    .get("session-1")
                    .copied()
                    .flatten()
            ),
            "1.5K"
        );
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
    fn active_session_with_no_turn_in_flight_reads_idle() {
        let mut session = archived_session();
        session.state = SessionState::Running;
        let detail = SessionDetail {
            last_activity_at_ms: Some(1_000_000),
            ..SessionDetail::default()
        };

        let (clock, _, _, _, _) = session_values(&session, Some(&detail), None, 1_480, &config());
        assert_eq!(clock, "[idle]");
    }

    #[test]
    fn materialized_idle_state_clears_a_stale_turn_clock() {
        let mut dashboard = dashboard_with_session(archived_session());
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
        let mut dashboard = dashboard_with_session(archived_session());
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
    fn active_message_tail_uses_the_last_four_nonempty_lines() {
        let short = SessionDetail {
            last_agent_message: Some("one line".into()),
            ..SessionDetail::default()
        };
        assert_eq!(
            active_message_tail(Some(&short), 80, ACTIVE_MESSAGE_LINES).len(),
            1
        );

        let long = SessionDetail {
            last_agent_message: Some("one\ntwo\nthree\nfour\nfive".into()),
            ..SessionDetail::default()
        };
        let lines = active_message_tail(Some(&long), 80, ACTIVE_MESSAGE_LINES)
            .into_iter()
            .map(|line| {
                line.spans
                    .into_iter()
                    .map(|span| span.content.into_owned())
                    .collect::<String>()
            })
            .collect::<Vec<_>>();
        assert_eq!(lines, ["│ two", "│ three", "│ four", "│ five"]);
        assert!(active_message_tail(None, 80, ACTIVE_MESSAGE_LINES).is_empty());
    }

    #[test]
    fn active_message_tail_removes_blank_lines_before_capping() {
        let detail = SessionDetail {
            last_agent_message: Some(
                "Fixed and pushed.\n\nDuplicate LinkedIn URLs now use last-write-wins behavior.\n\nCommit: b6cb3e8 Keep the last duplicate connection record".into(),
            ),
            ..SessionDetail::default()
        };

        let rendered = active_message_tail(Some(&detail), 80, ACTIVE_MESSAGE_LINES)
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

        let (clock, _, _, _, _) = session_values(&session, None, None, 1_012, &config());
        assert_eq!(clock, "Launch 12s");
    }

    fn operation(
        kind: SessionOperationKind,
        stage: Option<ProvisionStage>,
    ) -> SessionOperationDisplay {
        SessionOperationDisplay {
            kind,
            started_at_epoch_seconds: 1_000,
            placeholder: None,
            stage,
            stage_started_at_epoch_seconds: stage.map(|_| 1_000),
        }
    }

    #[test]
    fn launch_clock_names_the_reported_stage() {
        let session = archived_session();
        let operation = operation(
            SessionOperationKind::Launching,
            Some(ProvisionStage::Booting),
        );

        let (clock, _, _, _, _) =
            session_values(&session, None, Some(&operation), 1_012, &config());
        assert_eq!(clock, "Boot 12s");
    }

    #[test]
    fn launch_clock_falls_back_to_the_kind_label_without_a_stage() {
        let session = archived_session();
        let operation = operation(SessionOperationKind::Launching, None);

        let (clock, _, _, _, _) =
            session_values(&session, None, Some(&operation), 1_012, &config());
        assert_eq!(clock, "Launch 12s");
    }

    #[test]
    fn a_stage_does_not_rename_a_non_launch_operation() {
        let session = archived_session();
        let operation = operation(SessionOperationKind::Pausing, Some(ProvisionStage::Syncing));

        let (clock, _, _, _, _) =
            session_values(&session, None, Some(&operation), 1_012, &config());
        assert_eq!(clock, "Pausing 12s");
    }

    #[test]
    fn setting_a_stage_for_an_unknown_session_is_ignored() {
        let mut dashboard = DashboardState::new(config(), HelState::default(), BTreeMap::new());
        dashboard.set_session_operation_stage("missing", ProvisionStage::Booting);
        assert!(dashboard.session_operations.is_empty());
    }

    #[test]
    fn stage_clock_counts_from_when_the_stage_began_not_the_operation() {
        let session = archived_session();
        let mut operation = operation(
            SessionOperationKind::Launching,
            Some(ProvisionStage::Booting),
        );
        // The operation started at 1_000 but the stage only began at 1_040;
        // the clock must count from the stage, not the whole operation.
        operation.stage_started_at_epoch_seconds = Some(1_040);

        let (clock, _, _, _, _) =
            session_values(&session, None, Some(&operation), 1_052, &config());
        assert_eq!(clock, "Boot 12s");
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

    #[test]
    fn active_target_and_project_are_separate_cells() {
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

        let (_, _, target, project, _) =
            session_values(&archived_session(), None, None, 0, &config);
        assert_eq!(target, "podman");
        assert_eq!(project, "hel");
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
                memory_bytes: 48 * gib,
            })
        );
    }

    #[test]
    fn container_minus_clamps_cpu_at_floor_and_keeps_halving_memory() {
        let gib = 1024 * 1024 * 1024;
        let mut allocation = Some(SessionResourceAllocation::Container {
            cpus: 2,
            memory_bytes: 32 * gib,
        });
        let limits = Some((64, 64 * gib));

        adjust_resources(&mut allocation, None, limits, KeyCode::Char('-'));
        assert_eq!(
            allocation,
            Some(SessionResourceAllocation::Container {
                cpus: 2,
                memory_bytes: 16 * gib,
            })
        );
        adjust_resources(&mut allocation, None, limits, KeyCode::Char('-'));
        assert_eq!(
            allocation,
            Some(SessionResourceAllocation::Container {
                cpus: 2,
                memory_bytes: 8 * gib,
            })
        );
    }

    #[test]
    fn container_minus_clamps_memory_at_floor_and_keeps_halving_cpu() {
        let gib = 1024 * 1024 * 1024;
        let mut allocation = Some(SessionResourceAllocation::Container {
            cpus: 16,
            memory_bytes: 8 * gib,
        });
        let limits = Some((64, 64 * gib));

        adjust_resources(&mut allocation, None, limits, KeyCode::Char('-'));
        assert_eq!(
            allocation,
            Some(SessionResourceAllocation::Container {
                cpus: 8,
                memory_bytes: 8 * gib,
            })
        );
        adjust_resources(&mut allocation, None, limits, KeyCode::Char('-'));
        assert_eq!(
            allocation,
            Some(SessionResourceAllocation::Container {
                cpus: 4,
                memory_bytes: 8 * gib,
            })
        );
    }

    #[test]
    fn container_minus_is_a_no_op_once_both_are_at_their_floors() {
        let gib = 1024 * 1024 * 1024;
        let mut allocation = Some(SessionResourceAllocation::Container {
            cpus: 2,
            memory_bytes: 8 * gib,
        });
        let limits = Some((64, 64 * gib));

        adjust_resources(&mut allocation, None, limits, KeyCode::Char('-'));
        assert_eq!(
            allocation,
            Some(SessionResourceAllocation::Container {
                cpus: 2,
                memory_bytes: 8 * gib,
            })
        );
    }

    #[test]
    fn container_minus_leaves_values_already_below_floor_unchanged() {
        let gib = 1024 * 1024 * 1024;
        let mut allocation = Some(SessionResourceAllocation::Container {
            cpus: 1,
            memory_bytes: 4 * gib,
        });
        let limits = Some((64, 64 * gib));

        adjust_resources(&mut allocation, None, limits, KeyCode::Char('-'));
        assert_eq!(
            allocation,
            Some(SessionResourceAllocation::Container {
                cpus: 1,
                memory_bytes: 4 * gib,
            })
        );
    }

    #[test]
    fn container_c_clamps_at_cpu_ceiling() {
        let gib = 1024 * 1024 * 1024;
        let mut allocation = Some(SessionResourceAllocation::Container {
            cpus: 60,
            memory_bytes: 32 * gib,
        });
        let limits = Some((64, 64 * gib));

        adjust_resources(&mut allocation, None, limits, KeyCode::Char('c'));
        assert_eq!(
            allocation,
            Some(SessionResourceAllocation::Container {
                cpus: 64,
                memory_bytes: 32 * gib,
            })
        );
    }

    #[test]
    fn container_m_clamps_at_memory_ceiling() {
        let gib = 1024 * 1024 * 1024;
        let mut allocation = Some(SessionResourceAllocation::Container {
            cpus: 8,
            memory_bytes: 60 * gib,
        });
        let limits = Some((64, 64 * gib));

        adjust_resources(&mut allocation, None, limits, KeyCode::Char('m'));
        assert_eq!(
            allocation,
            Some(SessionResourceAllocation::Container {
                cpus: 8,
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
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::Close {
                session_id: "session-1".into()
            }
        );

        // "Retry pause" is the primary button, so it is focused when the dialog opens.
        dashboard.show_close_failure("session-1".into(), "archive unavailable");
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::Close {
                session_id: "session-1".into()
            }
        );

        dashboard.show_close_failure("session-1".into(), "archive unavailable");
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Char('x'))),
            DashboardAction::None
        );
        // "Force destroy" sits between Cancel and Retry pause.
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Left)),
            DashboardAction::None
        );
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::None
        );
        assert!(matches!(
            dashboard.mode,
            Mode::Confirm(ConfirmDialog {
                confirmation: Confirmation::ForceDestroy { .. },
                ..
            })
        ));
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
    fn close_failure_cancel_button_closes_the_dialog_without_acting() {
        let mut session = archived_session();
        session.state = SessionState::Running;
        let mut dashboard = dashboard_with_session(session);
        dashboard.show_close_failure("session-1".into(), "archive unavailable");

        // Tab from the rightmost button (Retry pause) wraps to Cancel.
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Tab)),
            DashboardAction::None
        );
        let Mode::Confirm(dialog) = &dashboard.mode else {
            panic!("expected close failure dialog");
        };
        assert_eq!(dialog.focus, 0);
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::None
        );
        assert!(matches!(dashboard.mode, Mode::Dashboard));
    }

    fn running_dashboard_with_pause_dialog() -> DashboardState {
        let mut session = archived_session();
        session.state = SessionState::Running;
        let mut dashboard = dashboard_with_session(session);
        dashboard.handle_key(ctrl_key('p'));
        assert!(matches!(
            dashboard.mode,
            Mode::Confirm(ConfirmDialog {
                confirmation: Confirmation::Close { .. },
                ..
            })
        ));
        dashboard
    }

    #[test]
    fn pause_confirmation_focuses_the_primary_button_so_enter_pauses() {
        let mut dashboard = running_dashboard_with_pause_dialog();
        let Mode::Confirm(dialog) = &dashboard.mode else {
            panic!("expected pause confirmation");
        };
        assert_eq!(
            confirmation_buttons(&dialog.confirmation),
            &["Cancel", "Pause"]
        );
        assert_eq!(dialog.focus, 1);
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::Close {
                session_id: "session-1".into()
            }
        );
        assert!(matches!(dashboard.mode, Mode::Dashboard));
    }

    #[test]
    fn pause_confirmation_cycles_focus_and_cancels_from_the_cancel_button() {
        for cycle_key in [
            KeyCode::Tab,
            KeyCode::Left,
            KeyCode::Right,
            KeyCode::BackTab,
        ] {
            let mut dashboard = running_dashboard_with_pause_dialog();
            assert_eq!(dashboard.handle_key(key(cycle_key)), DashboardAction::None);
            let Mode::Confirm(dialog) = &dashboard.mode else {
                panic!("expected pause confirmation to stay open for {cycle_key:?}");
            };
            assert_eq!(dialog.focus, 0, "{cycle_key:?}");
            assert_eq!(
                dashboard.handle_key(key(KeyCode::Enter)),
                DashboardAction::None,
                "{cycle_key:?}"
            );
            assert!(matches!(dashboard.mode, Mode::Dashboard), "{cycle_key:?}");
        }
    }

    #[test]
    fn pause_confirmation_wraps_focus_back_to_the_primary_button() {
        let mut dashboard = running_dashboard_with_pause_dialog();
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Tab)),
            DashboardAction::None
        );
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Tab)),
            DashboardAction::None
        );
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::Close {
                session_id: "session-1".into()
            }
        );
    }

    #[test]
    fn pause_confirmation_escape_cancels_from_any_button() {
        for presses in 0..3 {
            let mut dashboard = running_dashboard_with_pause_dialog();
            for _ in 0..presses {
                dashboard.handle_key(key(KeyCode::Tab));
            }
            assert_eq!(
                dashboard.handle_key(key(KeyCode::Esc)),
                DashboardAction::None,
                "after {presses} focus moves"
            );
            assert!(matches!(dashboard.mode, Mode::Dashboard), "{presses}");
        }
    }

    #[test]
    fn pause_confirmation_ignores_the_removed_letter_accelerators() {
        for accelerator in ['y', 'Y', 'n', 'N'] {
            let mut dashboard = running_dashboard_with_pause_dialog();
            assert_eq!(
                dashboard.handle_key(key(KeyCode::Char(accelerator))),
                DashboardAction::None,
                "{accelerator}"
            );
            assert!(
                matches!(
                    dashboard.mode,
                    Mode::Confirm(ConfirmDialog {
                        confirmation: Confirmation::Close { .. },
                        ..
                    })
                ),
                "{accelerator}"
            );
        }
    }

    #[test]
    fn pause_confirmation_renders_cancel_and_pause_buttons_with_pause_focused() {
        let mut dashboard = running_dashboard_with_pause_dialog();
        let mut terminal = Terminal::new(TestBackend::new(100, 24)).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw pause confirmation");
        let buffer = terminal.backend().buffer();
        let lines = buffer_lines(buffer);
        let row = lines
            .iter()
            .position(|line| line.contains(" Cancel ") && line.contains(" Pause "))
            .expect("button row");
        let button_y = buffer.area.y + row as u16;
        let cancel_x = buffer.area.x + cell_column(&lines[row], "Cancel");
        let pause_x = buffer.area.x + cell_column(&lines[row], "Pause");
        assert_eq!(buffer[(pause_x, button_y)].bg, Color::Cyan);
        assert_eq!(buffer[(cancel_x, button_y)].bg, Color::DarkGray);
        // Each label keeps its one-cell padding inside the button background.
        assert_eq!(buffer[(cancel_x - 1, button_y)].bg, Color::DarkGray);
        assert_eq!(buffer[(pause_x - 1, button_y)].bg, Color::Cyan);
        assert!(!lines.iter().any(|line| line.contains("Press y/Enter")));
    }

    #[test]
    fn button_confirmations_keep_their_button_row_visible() {
        let confirmations = [
            Confirmation::Close {
                session_id: "session-1".into(),
            },
            Confirmation::DeleteArchived {
                session_id: "session-1".into(),
            },
            Confirmation::CloseFailed {
                session_id: "session-1".into(),
                error: "archive unavailable".into(),
            },
            Confirmation::DirtyLocal {
                action: DashboardAction::None,
                repositories: vec!["/work/repo".into(), "/work/other".into()],
            },
        ];
        for confirmation in confirmations {
            for (width, height) in [(120, 30), (100, 24), (72, 22)] {
                let mut dashboard = dashboard_with_session(archived_session());
                dashboard.mode = Mode::Confirm(ConfirmDialog::new(confirmation.clone()));
                let mut terminal =
                    Terminal::new(TestBackend::new(width, height)).expect("terminal");
                terminal
                    .draw(|frame| render(frame, &mut dashboard))
                    .expect("draw confirmation");
                let rendered = buffer_lines(terminal.backend().buffer()).join("\n");
                for label in confirmation_buttons(&confirmation) {
                    assert!(
                        rendered.contains(&format!(" {label} ")),
                        "{confirmation:?} at {width}x{height} hides {label}"
                    );
                }
            }
        }
    }

    #[test]
    fn delete_archived_confirmation_deletes_from_its_primary_button() {
        let mut dashboard = dashboard_with_session(archived_session());
        dashboard.focus = Focus::Archived;
        dashboard.handle_key(key(KeyCode::Delete));
        let Mode::Confirm(dialog) = &dashboard.mode else {
            panic!("expected delete confirmation");
        };
        assert_eq!(
            confirmation_buttons(&dialog.confirmation),
            &["Cancel", "Delete"]
        );
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::DeleteArchived {
                session_id: "session-1".into()
            }
        );
    }

    #[test]
    fn dirty_local_confirmation_continues_or_cancels_from_its_buttons() {
        let create = |allow_dirty_local| DashboardAction::CreateSession {
            profile_id: "codex-1".into(),
            bundle_id: "hel".into(),
            project_directory: None,
            target_template_id: "podman".into(),
            additional_mounts: Vec::new(),
            allow_dirty_local,
            resource_allocation: None,
        };

        let mut dashboard = dashboard_with_session(archived_session());
        dashboard.show_dirty_local_confirmation(create(false), vec!["project".into()]);
        assert_eq!(dashboard.handle_key(key(KeyCode::Enter)), create(true));
        assert!(matches!(dashboard.mode, Mode::Dashboard));

        let mut dashboard = dashboard_with_session(archived_session());
        dashboard.show_dirty_local_confirmation(create(false), vec!["project".into()]);
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Char('y'))),
            DashboardAction::None
        );
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Tab)),
            DashboardAction::None
        );
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::None
        );
        assert!(matches!(dashboard.mode, Mode::Dashboard));
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
        assert!(rendered.contains("╔ Capacity"));
        assert!(!rendered.contains("╔ Capacity in Use"));

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

    #[test]
    fn quota_bars_show_fractional_remaining_capacity_and_blank_missing_windows() {
        let window = QuotaWindow {
            label: "Week".into(),
            remaining_percent: Some(73),
            used: None,
            limit: None,
            resets: None,
            resets_at_epoch_seconds: None,
        };

        let bar = quota_bar(Some(&window));
        assert_eq!(
            bar.spans
                .iter()
                .map(|span| span.content.as_ref())
                .collect::<String>(),
            "███████▎░░  73%"
        );
        assert_eq!(bar.spans[0].style.fg, Some(Color::Green));
        assert_eq!(bar.spans[2].style.fg, Some(Color::DarkGray));
        assert!(quota_bar(None).spans.is_empty());
    }

    #[test]
    fn quota_resets_add_five_hour_only_when_projected_to_exhaust() {
        let mut quota = ProfileQuota {
            profile_id: "codex-1".into(),
            harness: HarnessKind::Codex,
            windows: vec![
                QuotaWindow {
                    label: "Week".into(),
                    remaining_percent: Some(73),
                    used: None,
                    limit: None,
                    resets: Some("09:00 Aug 20".into()),
                    resets_at_epoch_seconds: Some(604_800),
                },
                QuotaWindow {
                    label: "5H".into(),
                    remaining_percent: Some(80),
                    used: None,
                    limit: None,
                    resets: Some("14:00 Aug 13".into()),
                    resets_at_epoch_seconds: Some(14_400),
                },
            ],
            extra: None,
            error: None,
            refreshed_at_epoch_seconds: 0,
        };

        assert_eq!(quota_reset_summary(&quota), "09:00 Aug 20");
        quota.windows[1].remaining_percent = Some(70);
        assert_eq!(quota_reset_summary(&quota), "09:00 Aug 20 / 14:00 Aug 13");
    }

    #[test]
    fn quota_render_uses_weekly_five_hour_and_reset_columns() {
        let quota = ProfileQuota {
            profile_id: "codex-1".into(),
            harness: HarnessKind::Codex,
            windows: vec![
                QuotaWindow {
                    label: "Week".into(),
                    remaining_percent: Some(73),
                    used: None,
                    limit: None,
                    resets: Some("09:00 Aug 20".into()),
                    resets_at_epoch_seconds: Some(604_800),
                },
                QuotaWindow {
                    label: "5H".into(),
                    remaining_percent: Some(70),
                    used: None,
                    limit: None,
                    resets: Some("14:00 Aug 13".into()),
                    resets_at_epoch_seconds: Some(14_400),
                },
            ],
            extra: None,
            error: None,
            refreshed_at_epoch_seconds: 0,
        };
        let mut dashboard = DashboardState::new(
            config(),
            HelState::default(),
            BTreeMap::from([("codex-1".into(), quota)]),
        );
        let mut terminal = Terminal::new(TestBackend::new(140, 28)).expect("terminal");
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

        assert!(rendered.contains("Weekly"));
        assert!(rendered.contains("5H"));
        assert!(rendered.contains("Resets"));
        assert!(rendered.contains("73%"));
        assert!(rendered.contains("70%"));
        assert!(rendered.contains("09:00 Aug 20 / 14:00 Aug 13"));
    }
}
