//! Full-screen dashboard and session picker for Hel.
//!
//! This module deliberately has no provisioning or persistence side effects.
//! Input is reduced to [`DashboardAction`] values for the controller to run.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use crossterm::event::{
    KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};
use ratatui::layout::Rect;

use hel::hel_chat::{Notices, TranscriptSnapshot};
use hel::hel_config::{HarnessKind, HelConfig, TargetTemplate as HelTargetTemplate};
use hel::hel_quota::ProfileQuota;
use hel::hel_state::{HelState, SessionRecord, SessionResourceAllocation, SessionState};
use hel::hel_targets::AdditionalMount;

use crate::dialogs::{
    ConfirmDialog, Confirmation, ContainerEditor, FORCE_CONFIRMATION, ImportBundleConfirmation,
    ImportProgress, RenameEditor, RenameFocus,
};
use crate::ingest::{CapacityDetail, SessionDetail, SessionOperationDisplay, TranscriptHydration};
use crate::resume::ResumeDialog;
use crate::wizards::{MountFocus, NewWizard, ResumeWizard, WizardStep};

mod dialogs;
mod ingest;
mod render;
mod resume;
mod widgets;
mod wizards;

#[cfg(test)]
mod test_support;

pub use crate::dialogs::{ImportProfileOption, ImportSessionOption};
pub use crate::ingest::{MaterializedProjectionCache, PreparedMaterializedSessionDetail};
pub use crate::render::render;
pub use crate::resume::resume_profile_placeholders;

/// Active sessions, capacity, and quotas. Sessions that are not live live in
/// the resume dialog instead of a dashboard pane.
pub(crate) const DASHBOARD_PANE_COUNT: usize = 3;

pub(crate) const MOUSE_SCROLL_ROWS: isize = 3;

/// Maximum gap between two left clicks on the same session row for the pair
/// to count as a double click.
const DOUBLE_CLICK_INTERVAL: Duration = Duration::from_millis(500);

/// Rows one wheel notch moves the selected session's conversation preview.
const PREVIEW_SCROLL_ROWS: usize = 3;

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
    DeleteStopped {
        session_id: String,
    },
    RenameSession {
        session_id: String,
        title: String,
    },
    RefreshQuotas,
    OpenResumeDialog,
    /// Hide or reveal one Hel session record in the resume dialog.
    SetSessionArchived {
        session_id: String,
        archived: bool,
    },
    /// Hide or reveal one native session in the resume dialog. Hel records the
    /// choice in its own database; the harness home is never written.
    SetNativeSessionHidden {
        harness_kind: HarnessKind,
        native_session_id: String,
        hidden: bool,
    },
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
    /// Per-session container provisioning inputs, taking effect the next time
    /// the container is created.
    SaveContainerSettings {
        session_id: String,
        cpus: Option<String>,
        memory: Option<String>,
        additional_mounts: Vec<AdditionalMount>,
        mount_history: Vec<std::path::PathBuf>,
    },
    QuitDetach,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionOperationKind {
    Launching,
    Resuming,
    Stopping,
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
            Self::Stopping => "Stopping",
            Self::Destroying => "Destroying",
            Self::Deleting => "Deleting",
            Self::Connecting => "Connecting",
            Self::Importing => "Importing",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Focus {
    Active,
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
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Sequence => "sequence",
            Self::RecentActivity => "recent activity",
            Self::Profile => "profile, then sequence",
        }
    }

    pub(crate) fn next(self) -> Self {
        match self {
            Self::Sequence => Self::RecentActivity,
            Self::RecentActivity => Self::Profile,
            Self::Profile => Self::Sequence,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Mode {
    Dashboard,
    New(NewWizard),
    Resume(ResumeWizard),
    /// The unified picker for every session that is not live.
    ResumeDialog(ResumeDialog),
    Rename(RenameEditor),
    EditContainer(ContainerEditor),
    Importing(ImportProgress),
    ConfirmImportBundle(ImportBundleConfirmation),
    Confirm(ConfirmDialog),
}

/// What a key press means for a focusable button row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ButtonKey {
    Focus(usize),
    Activate(usize),
    Cancel,
    Ignored,
}

pub(crate) fn button_row_key(code: KeyCode, focus: usize, count: usize) -> ButtonKey {
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

pub(crate) fn cycle_button_focus(focus: usize, count: usize, reverse: bool) -> usize {
    if count == 0 {
        return 0;
    }
    if reverse {
        focus.min(count - 1).checked_sub(1).unwrap_or(count - 1)
    } else {
        (focus + 1) % count
    }
}

pub(crate) fn cycle_control<T: Copy + PartialEq>(current: T, order: &[T], reverse: bool) -> T {
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

/// Stateful, renderable projection of controller configuration and state.
pub struct DashboardState {
    pub(crate) config: HelConfig,
    pub(crate) state: HelState,
    pub(crate) quotas: BTreeMap<String, ProfileQuota>,
    pub(crate) quota_refreshing: BTreeSet<String>,
    pub(crate) session_details: BTreeMap<String, SessionDetail>,
    pub(crate) checkpoint_archive_sizes: BTreeMap<String, Option<u64>>,
    pub(crate) session_operations: BTreeMap<String, SessionOperationDisplay>,
    pub(crate) capacity_details: BTreeMap<String, CapacityDetail>,
    pub(crate) session_index: usize,
    pub(crate) session_order: SessionOrder,
    pub(crate) capacity_index: usize,
    pub(crate) quota_index: usize,
    pub(crate) focus: Focus,
    pub(crate) pane_areas: Option<[Rect; DASHBOARD_PANE_COUNT]>,
    pub(crate) resume_sessions_area: Option<Rect>,
    /// Native sessions the resume dialog hides, loaded from Hel's database.
    pub(crate) hidden_native_sessions: BTreeSet<(HarnessKind, String)>,
    /// Hitbox of the selected session's conversation preview, so the wheel can
    /// scroll that preview instead of moving the selection.
    pub(crate) selected_preview_area: Option<Rect>,
    /// Row hitboxes for the Active pane, keyed by the row's index into the
    /// active session list. Each rect spans the summary line and every
    /// visible preview line beneath it, so a click anywhere on the row
    /// selects it.
    pub(crate) active_row_areas: Vec<(usize, Rect)>,
    /// Rows the selected session's preview sits above its live tail. Only one
    /// preview scrolls at a time; selecting another session snaps this back to
    /// the tail, which is why the owning session is tracked alongside it.
    pub(crate) preview_scroll: usize,
    pub(crate) preview_scroll_session: Option<String>,
    /// The pane, row index, and time of the most recent left click on a
    /// session row, so the next click can be recognized as a double click.
    last_row_click: Option<(Focus, usize, Instant)>,
    pub(crate) mode: Mode,
    pub(crate) notices: Notices,
    pub(crate) greeting: String,
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
            resume_sessions_area: None,
            hidden_native_sessions: BTreeSet::new(),
            selected_preview_area: None,
            active_row_areas: Vec::new(),
            preview_scroll: 0,
            preview_scroll_session: None,
            last_row_click: None,
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
            Mode::ResumeDialog(dialog) => self.handle_resume_dialog_key(key, dialog),
            Mode::Rename(editor) => self.handle_rename_key(key.code, editor),
            Mode::EditContainer(editor) => self.handle_container_edit_key(key.code, editor),
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

    pub fn handle_mouse(&mut self, mouse: MouseEvent) -> DashboardAction {
        if let Mode::ResumeDialog(dialog) = &self.mode {
            let Some(area) = self.resume_sessions_area else {
                return DashboardAction::None;
            };
            if !rect_contains(area, mouse.column, mouse.row) {
                return DashboardAction::None;
            }
            let delta = match mouse.kind {
                MouseEventKind::ScrollUp => -MOUSE_SCROLL_ROWS,
                MouseEventKind::ScrollDown => MOUSE_SCROLL_ROWS,
                _ => return DashboardAction::None,
            };
            let len = self.resume_rows(dialog).len();
            let index = offset_index(dialog.row_index, len, delta);
            if let Mode::ResumeDialog(dialog) = &mut self.mode {
                dialog.focus = crate::resume::ResumeFocus::Sessions;
            }
            self.select_resume_row(index);
            return DashboardAction::None;
        }
        if !matches!(self.mode, Mode::Dashboard) {
            return DashboardAction::None;
        }
        if mouse.kind == MouseEventKind::Down(MouseButton::Left) {
            if let Some(&(index, _)) = self
                .active_row_areas
                .iter()
                .find(|(_, area)| rect_contains(*area, mouse.column, mouse.row))
            {
                return self.handle_row_click(Focus::Active, index);
            }
            // The click missed every row; forget any pending double click so
            // a stray click elsewhere can't pair up with the next row click.
            self.last_row_click = None;
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
            return DashboardAction::None;
        }
        let hovered = self.pane_areas.and_then(|areas| {
            areas
                .into_iter()
                .position(|area| rect_contains(area, mouse.column, mouse.row))
                .map(|index| match index {
                    0 => Focus::Active,
                    1 => Focus::Capacity,
                    2 => Focus::Quotas,
                    _ => unreachable!("dashboard has exactly three panes"),
                })
        });
        let Some(hovered) = hovered else {
            return DashboardAction::None;
        };
        match mouse.kind {
            MouseEventKind::ScrollUp => self.scroll_selection_for(hovered, -MOUSE_SCROLL_ROWS),
            MouseEventKind::ScrollDown => self.scroll_selection_for(hovered, MOUSE_SCROLL_ROWS),
            _ => {}
        }
        DashboardAction::None
    }

    /// Selects the clicked row and, if it's the second click on the same row
    /// within `DOUBLE_CLICK_INTERVAL`, performs the same action Enter would.
    fn handle_row_click(&mut self, focus: Focus, index: usize) -> DashboardAction {
        self.focus = focus;
        self.set_selection_for(focus, index);
        let now = Instant::now();
        let is_double_click = matches!(
            self.last_row_click,
            Some((last_focus, last_index, last_time))
                if last_focus == focus
                    && last_index == index
                    && now.saturating_duration_since(last_time) <= DOUBLE_CLICK_INTERVAL
        );
        if is_double_click {
            self.last_row_click = None;
            self.open_or_resume()
        } else {
            self.last_row_click = Some((focus, index, now));
            DashboardAction::None
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
            (KeyCode::Char('s'), true) => {
                self.cycle_session_order();
                self.set_notice(format!("Sort by {}", self.session_order.label()));
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
            (KeyCode::Char('i') | KeyCode::Char('t'), true) => DashboardAction::OpenResumeDialog,
            (KeyCode::Char('r'), true) => {
                if self.focus == Focus::Quotas {
                    DashboardAction::RefreshQuotas
                } else if self.focus == Focus::Active {
                    if !self.reject_selected_operation() {
                        self.begin_rename();
                    }
                    DashboardAction::None
                } else {
                    DashboardAction::None
                }
            }
            (KeyCode::Char('u'), true) => DashboardAction::RefreshQuotas,
            // Setup and the container editor never both apply: setup only
            // opens while the config is empty, and an empty config has no
            // sessions to select.
            (KeyCode::Char('e'), true) if self.config_is_empty() => DashboardAction::OpenConfig,
            (KeyCode::Char('e'), true) if self.focus == Focus::Active => {
                self.begin_container_edit();
                DashboardAction::None
            }
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

    pub(crate) fn selected_session(&self) -> Option<&SessionRecord> {
        if self.focus != Focus::Active {
            return None;
        }
        self.ordered_sessions().get(self.session_index).copied()
    }

    /// The sessions the dashboard lists, in the current sort order. Only live
    /// sessions appear here; everything else belongs to the resume dialog.
    pub(crate) fn ordered_sessions(&self) -> Vec<&SessionRecord> {
        partition_sessions(
            self.state.sessions.values(),
            &self.session_details,
            self.session_order,
        )
        .0
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

    pub(crate) fn compatible_profiles(&self, session_id: &str) -> Vec<(&String, HarnessKind)> {
        if !self.state.sessions.contains_key(session_id) {
            return Vec::new();
        }
        self.config
            .profiles
            .iter()
            .map(|(id, profile)| (id, profile.kind))
            .collect()
    }

    pub(crate) fn profile_choice(&self, id: &str, harness: HarnessKind) -> String {
        let quota = if self.quota_refreshing.contains(id) {
            "refreshing".to_string()
        } else {
            self.quotas
                .get(id)
                .map(ProfileQuota::compact)
                .unwrap_or_else(|| "refreshing".to_string())
        };
        let danger = match harness.bare_target_auto_approval() {
            Some(mechanism) => format!("  ⚠ DANGER: {mechanism} approves every command"),
            None => String::new(),
        };
        format!("{id}  {}  ·  {quota}{danger}", harness.display_name())
    }

    /// The selected session, if its target template creates a container.
    pub(crate) fn selected_container_session(&self) -> Option<&SessionRecord> {
        let session = self.selected_session()?;
        matches!(
            self.config.targets.get(&session.target_template_id)?,
            HelTargetTemplate::LocalPodman { .. }
                | HelTargetTemplate::AppleContainer { .. }
                | HelTargetTemplate::SshPodman { .. }
        )
        .then_some(session)
    }

    pub(crate) fn config_is_empty(&self) -> bool {
        self.config.profiles.is_empty() || self.config.targets.is_empty()
    }

    pub(crate) fn cancel_modal(&mut self) {
        self.mode = Mode::Dashboard;
    }

    fn focus_len(&self) -> usize {
        self.focus_len_for(self.focus)
    }

    fn focus_len_for(&self, focus: Focus) -> usize {
        match focus {
            Focus::Active => self.ordered_sessions().len(),
            Focus::Capacity => self.capacity_details.len(),
            Focus::Quotas => self.config.profiles.len(),
        }
    }

    fn set_selection(&mut self, index: usize) {
        self.set_selection_for(self.focus, index);
    }

    fn set_selection_for(&mut self, focus: Focus, index: usize) {
        match focus {
            Focus::Active => self.session_index = index,
            Focus::Capacity => self.capacity_index = index,
            Focus::Quotas => self.quota_index = index,
        }
    }

    fn move_selection(&mut self, delta: isize) {
        let len = self.focus_len();
        match self.focus {
            Focus::Active => move_index(&mut self.session_index, len, delta),
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
        let current = match focus {
            Focus::Active => self.session_index,
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
            (Focus::Active, false) | (Focus::Quotas, true) => Focus::Capacity,
            (Focus::Capacity, false) | (Focus::Active, true) => Focus::Quotas,
            (Focus::Quotas, false) | (Focus::Capacity, true) => Focus::Active,
        };
        let active_len = self.ordered_sessions().len();
        if self.focus == Focus::Active {
            self.session_index = self.session_index.min(active_len.saturating_sub(1));
        }
    }

    pub(crate) fn clamp_selections(&mut self) {
        let active_len = self.ordered_sessions().len();
        self.session_index = self.session_index.min(active_len.saturating_sub(1));
        self.quota_index = self
            .quota_index
            .min(self.config.profiles.len().saturating_sub(1));
        self.capacity_index = self
            .capacity_index
            .min(self.capacity_details.len().saturating_sub(1));
    }
}

/// Split sessions into the ones the dashboard lists and the ones the resume
/// dialog lists. The dashboard shows only live sessions; every other state
/// belongs to the dialog, and nothing appears in both.
pub(crate) fn partition_sessions<'a>(
    sessions: impl IntoIterator<Item = &'a SessionRecord>,
    session_details: &BTreeMap<String, SessionDetail>,
    order: SessionOrder,
) -> (Vec<&'a SessionRecord>, Vec<&'a SessionRecord>) {
    let mut active = Vec::new();
    let mut terminal = Vec::new();
    for session in sessions {
        if session.state.is_active() {
            active.push(session);
        } else {
            terminal.push(session);
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
    terminal.sort_by(sort);
    (active, terminal)
}

fn session_timestamp(timestamp: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(timestamp)
        .ok()
        .map(|timestamp| timestamp.timestamp())
}

fn rect_contains(area: Rect, column: u16, row: u16) -> bool {
    column >= area.x && column < area.right() && row >= area.y && row < area.bottom()
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

pub(crate) fn move_index(index: &mut usize, len: usize, delta: isize) {
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

pub(crate) fn nth_key<T>(map: &BTreeMap<String, T>, index: usize) -> String {
    map.keys()
        .nth(index)
        .cloned()
        .expect("wizard is only opened for non-empty configuration")
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use crossterm::event::{KeyCode, MouseButton, MouseEventKind};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use hel::hel_state::{HelState, STATE_VERSION, SessionState, TranscriptBody};

    use super::*;
    use crate::test_support::*;

    use crate::ingest::{SessionDetail, TranscriptHydration};
    use crate::render::{SELECTED_TRANSCRIPT_LINES, render};

    #[test]
    fn dashboard_actions_require_control_while_navigation_does_not() {
        let mut session = stopped_session();
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
            DashboardAction::OpenResumeDialog
        );
        assert_eq!(
            dashboard.handle_key(ctrl_key('q')),
            DashboardAction::QuitDetach
        );

        assert_eq!(
            dashboard.handle_key(key(KeyCode::Tab)),
            DashboardAction::None
        );
        assert_eq!(dashboard.focus, Focus::Capacity);
    }

    #[test]
    fn ctrl_q_quits_without_mutating_any_dashboard_modal() {
        let mut new_session = DashboardState::new(config(), HelState::default(), BTreeMap::new());
        assert_eq!(new_session.handle_key(ctrl_key('n')), DashboardAction::None);

        let mut resume = dashboard_with_session(stopped_session());
        assert_eq!(open_resume_wizard(&mut resume), DashboardAction::None);

        let mut running = stopped_session();
        running.state = SessionState::Running;
        running.checkpoint = None;
        let mut rename = dashboard_with_session(running);
        assert_eq!(rename.handle_key(ctrl_key('r')), DashboardAction::None);

        let mut importing = dashboard_with_session(stopped_session());
        importing.show_import_progress("Chosen session".into());

        let mut confirm_import = dashboard_with_session(stopped_session());
        confirm_import.show_import_bundle_confirmation(Vec::new(), Vec::new(), Vec::new(), false);

        let mut confirm = dashboard_with_session(stopped_session());
        confirm.show_dirty_local_confirmation(DashboardAction::None, vec!["project".into()]);

        let mut resume_dialog = dashboard_with_session(stopped_session());
        resume_dialog.show_resume_dialog(1, Vec::new());

        for (label, mut dashboard) in [
            ("new session", new_session),
            ("resume", resume),
            ("resume dialog", resume_dialog),
            ("rename", rename),
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

    #[test]
    fn the_partition_keeps_terminal_states_off_the_dashboard() {
        let mut running = stopped_session();
        running.id = "session-0".into();
        running.state = SessionState::Running;
        let stopped = stopped_session();
        let mut lost = stopped_session();
        lost.id = "session-2".into();
        lost.state = SessionState::Lost;
        let state = HelState {
            version: STATE_VERSION,
            sessions: BTreeMap::from([
                (running.id.clone(), running),
                (stopped.id.clone(), stopped),
                (lost.id.clone(), lost),
            ]),
            mount_history: BTreeMap::new(),
        };
        let (active, terminal) = partition_sessions(
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
            terminal
                .iter()
                .map(|session| session.id.as_str())
                .collect::<Vec<_>>(),
            ["session-1", "session-2"]
        );

        // Only the live session is on the dashboard; Tab leaves the session
        // pane rather than walking into a second one.
        let mut dashboard = DashboardState::new(config(), state, BTreeMap::new());
        dashboard.handle_key(key(KeyCode::Down));
        assert_eq!(
            dashboard
                .selected_session()
                .map(|session| session.id.as_str()),
            Some("session-0")
        );
        dashboard.handle_key(key(KeyCode::Tab));
        assert_eq!(dashboard.focus, Focus::Capacity);
        assert!(dashboard.selected_session().is_none());
    }

    #[test]
    fn sessions_are_ordered_by_creation_sequence_ascending_by_default() {
        let mut oldest = stopped_session();
        oldest.id = "session-z".into();
        oldest.created_at = "2026-08-09T01:00:00Z".into();
        let mut newest = stopped_session();
        newest.id = "session-y".into();
        newest.created_at = "2026-08-09T00:30:00-02:00".into();
        let mut invalid_timestamp = stopped_session();
        invalid_timestamp.id = "session-a".into();
        invalid_timestamp.created_at = "unknown".into();

        let (_, terminal) = partition_sessions(
            [&invalid_timestamp, &oldest, &newest],
            &BTreeMap::new(),
            SessionOrder::Sequence,
        );

        assert_eq!(
            terminal
                .iter()
                .map(|session| session.id.as_str())
                .collect::<Vec<_>>(),
            ["session-z", "session-y", "session-a"]
        );
    }

    #[test]
    fn recent_activity_uses_projection_milliseconds_without_metadata_override() {
        let mut first = stopped_session();
        first.id = "first".into();
        first.state = SessionState::Running;
        first.updated_at = "2099-01-01T00:00:00Z".into();
        let mut second = stopped_session();
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
        let mut hydrated = stopped_session();
        hydrated.id = "hydrated".into();
        hydrated.state = SessionState::Running;
        hydrated.updated_at = "2099-01-01T00:00:00Z".into();
        let mut loading = stopped_session();
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
        let mut disconnected = stopped_session();
        disconnected.id = "disconnected".into();
        disconnected.state = SessionState::Disconnected;
        disconnected.updated_at = "2099-01-01T00:00:00Z".into();
        let mut connected = stopped_session();
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
            let mut session = stopped_session();
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
        assert_eq!(dashboard.session_order, SessionOrder::Sequence);
        assert_eq!(dashboard.notices.current(), None);

        dashboard.handle_key(ctrl_key('s'));
        assert_eq!(dashboard.session_order, SessionOrder::RecentActivity);
        assert_eq!(
            dashboard.notices.current().as_deref(),
            Some("Sort by recent activity")
        );
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

        dashboard.handle_key(ctrl_key('s'));
        assert_eq!(dashboard.session_order, SessionOrder::Profile);
        assert_eq!(
            ordered_ids(&dashboard),
            ["session-b", "session-c", "session-a"]
        );
        assert_eq!(dashboard.selected_session().unwrap().id, "session-a");

        dashboard.handle_key(ctrl_key('s'));
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
                terminal_outputs: Vec::new(),
                terminal_refs: Vec::new(),
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
    fn bracketed_paste_populates_dashboard_text_editors() {
        let mut session = stopped_session();
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
    fn dashboard_navigation_keeps_four_distinct_panes() {
        let mut active = stopped_session();
        active.id = "session-0".into();
        active.state = SessionState::Running;
        let archived = stopped_session();
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
        assert_eq!(dashboard.focus, Focus::Active);
    }

    #[test]
    fn keyboard_selection_stops_at_the_active_panes_ends_instead_of_wrapping() {
        let sessions = (0..3)
            .map(|index| {
                let mut session = stopped_session();
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
    fn mouse_wheel_scrolls_the_hovered_pane_without_changing_focus() {
        let sessions = (0..5)
            .map(|index| {
                let mut session = stopped_session();
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
        dashboard.handle_mouse(mouse_in(MouseEventKind::ScrollDown, pane_areas[2]));
        assert_eq!(dashboard.quota_index, 2);
        assert_eq!(dashboard.session_index, 0);
        assert_eq!(dashboard.focus, Focus::Active);
    }

    #[test]
    fn clicking_an_active_rows_tail_line_selects_that_session() {
        let mut dashboard = dashboard_with_conversations(3);
        let mut terminal = Terminal::new(TestBackend::new(120, 40)).expect("test terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw active row hitboxes");
        assert_eq!(dashboard.session_index, 0, "starts on the first session");

        let (_, row) = *dashboard
            .active_row_areas
            .iter()
            .find(|(index, _)| *index == 2)
            .expect("the third active row has a recorded hitbox");
        assert!(
            row.height > 1,
            "an unselected row still spans its preview lines"
        );
        // Click the row's bottom line, i.e. its conversation tail, not the
        // one-line summary at the top.
        dashboard.handle_mouse(mouse_at_row(
            MouseEventKind::Down(MouseButton::Left),
            row,
            row.height - 1,
        ));

        assert_eq!(
            dashboard.session_index, 2,
            "clicking the tail line selected the row, not just its summary line"
        );
        assert_eq!(dashboard.focus, Focus::Active);
    }

    #[test]
    fn a_single_click_on_a_row_selects_but_reports_no_action() {
        let mut dashboard = dashboard_with_conversations(3);
        let mut terminal = Terminal::new(TestBackend::new(120, 40)).expect("test terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw active row hitboxes");

        let (_, row) = *dashboard
            .active_row_areas
            .iter()
            .find(|(index, _)| *index == 1)
            .expect("the second active row has a recorded hitbox");
        let action = dashboard.handle_mouse(mouse_at_row(
            MouseEventKind::Down(MouseButton::Left),
            row,
            0,
        ));

        assert_eq!(action, DashboardAction::None);
        assert_eq!(dashboard.session_index, 1);
    }

    #[test]
    fn a_double_click_on_an_active_row_opens_it_like_enter() {
        let mut dashboard = dashboard_with_conversations(3);
        let mut terminal = Terminal::new(TestBackend::new(120, 40)).expect("test terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw active row hitboxes");

        let (_, row) = *dashboard
            .active_row_areas
            .iter()
            .find(|(index, _)| *index == 1)
            .expect("the second active row has a recorded hitbox");
        let click = || mouse_at_row(MouseEventKind::Down(MouseButton::Left), row, 0);

        let first = dashboard.handle_mouse(click());
        assert_eq!(first, DashboardAction::None, "the first click just selects");

        let second = dashboard.handle_mouse(click());
        assert_eq!(
            second,
            DashboardAction::Open {
                session_id: "session-1".into(),
            },
            "a quick second click on the same row opens it, matching Enter"
        );
        assert_eq!(dashboard.session_index, 1);
    }

    #[test]
    fn clicks_on_different_rows_do_not_count_as_a_double_click() {
        let mut dashboard = dashboard_with_conversations(3);
        let mut terminal = Terminal::new(TestBackend::new(120, 40)).expect("test terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw active row hitboxes");

        let row_for = |index: usize| {
            *dashboard
                .active_row_areas
                .iter()
                .find(|(row_index, _)| *row_index == index)
                .map(|(_, area)| area)
                .expect("row has a recorded hitbox")
        };
        let first_row = row_for(0);
        let second_row = row_for(1);

        let first = dashboard.handle_mouse(mouse_at_row(
            MouseEventKind::Down(MouseButton::Left),
            first_row,
            0,
        ));
        assert_eq!(first, DashboardAction::None);

        // A click on a different row is a fresh first click, not the second
        // half of a double click on row 0.
        let second = dashboard.handle_mouse(mouse_at_row(
            MouseEventKind::Down(MouseButton::Left),
            second_row,
            0,
        ));
        assert_eq!(second, DashboardAction::None);
        assert_eq!(
            dashboard.session_index, 1,
            "the second click's row is selected"
        );
    }

    /// A dashboard with `count` running sessions, each carrying a numbered
    /// conversation long enough to scroll.
    fn dashboard_with_conversations(count: usize) -> DashboardState {
        let sessions = (0..count)
            .map(|index| {
                let mut session = stopped_session();
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
        let mut terminal = Terminal::new(TestBackend::new(120, 20)).expect("test terminal");
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

    #[test]
    fn newly_ready_session_can_be_selected_after_state_refresh() {
        let mut new_session = stopped_session();
        new_session.id = "new-session".into();
        new_session.state = SessionState::Running;
        let mut other = stopped_session();
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
        dashboard.focus = Focus::Quotas;

        let mut refreshed = dashboard.state.clone();
        refreshed
            .sessions
            .insert(new_session.id.clone(), new_session);
        dashboard.set_state(refreshed);
        dashboard.select_active_session("new-session");

        assert_eq!(dashboard.focus, Focus::Active);
        assert_eq!(dashboard.selected_session().unwrap().id, "new-session");
    }

    /// Stopping the last session empties the dashboard rather than moving the
    /// row to another pane: it belongs to the resume dialog now.
    #[test]
    fn stopping_the_last_session_empties_the_dashboard_and_panes_still_cycle() {
        let mut session = stopped_session();
        session.state = SessionState::Running;
        let mut dashboard = dashboard_with_session(session);
        dashboard.set_deployment_capacity_targets(vec![test_capacity_target()]);
        assert_eq!(dashboard.focus, Focus::Active);

        let mut state = dashboard.state.clone();
        state.sessions.get_mut("session-1").unwrap().state = SessionState::Stopped;
        dashboard.set_state(state);
        assert_eq!(dashboard.focus, Focus::Active);
        assert!(dashboard.ordered_sessions().is_empty());
        assert!(dashboard.selected_session().is_none());

        dashboard.handle_key(key(KeyCode::Tab));
        assert_eq!(dashboard.focus, Focus::Capacity);
        dashboard.handle_key(key(KeyCode::Tab));
        assert_eq!(dashboard.focus, Focus::Quotas);
        dashboard.handle_key(key(KeyCode::Tab));
        assert_eq!(dashboard.focus, Focus::Active);
    }

    #[test]
    fn opening_an_active_session_returns_controller_action() {
        let mut session = stopped_session();
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
        let mut session = stopped_session();
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
        let mut session = stopped_session();
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
}
