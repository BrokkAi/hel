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

use hel::hel_chat::Notices;
use hel::hel_config::{HarnessKind, HelConfig, TargetTemplate as HelTargetTemplate};
use hel::hel_quota::ProfileQuota;
use hel::hel_state::{
    HelState, ProjectSourceIdentity, SessionRecord, SessionResourceAllocation, SessionState,
};
use hel::hel_targets::AdditionalMount;

use crate::dialogs::{
    ConfirmDialog, Confirmation, ContainerEditor, FORCE_CONFIRMATION, ImportBundleConfirmation,
    ImportProgress, RenameEditor, RenameFocus, RepositoryOriginDialog,
};
use crate::ingest::{CapacityDetail, SessionDetail, SessionOperationDisplay};
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
pub use crate::ingest::{
    MaterializedProjectionCache, PreparedMaterializedSessionDetail,
    PreparedMaterializedSessionSummary,
};
pub use crate::render::render;
pub use crate::resume::resume_profile_placeholders;

/// Active sessions, capacity, and quotas. Sessions that are not live live in
/// the resume dialog instead of a dashboard pane.
pub(crate) const DASHBOARD_PANE_COUNT: usize = 3;

pub(crate) const MOUSE_SCROLL_ROWS: isize = 3;

/// Maximum gap between two left clicks on the same session row for the pair
/// to count as a double click.
const DOUBLE_CLICK_INTERVAL: Duration = Duration::from_millis(500);

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
    ValidateSessionMounts {
        target_template_id: String,
        mounts: Vec<AdditionalMount>,
        launch: Box<DashboardAction>,
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
    PreflightResumeRepositories {
        launch: Box<DashboardAction>,
    },
    ReplaceResumeRepositoryOrigin {
        session_id: String,
        repository_id: String,
        replacement: String,
        launch: Box<DashboardAction>,
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
    MarkAllRead {
        receipts: Vec<(String, u64)>,
    },
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Mode {
    Dashboard,
    New(NewWizard),
    Resume(ResumeWizard),
    /// The unified picker for every session that is not live.
    ResumeDialog(ResumeDialog),
    RepositoryOrigin(RepositoryOriginDialog),
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
    pub(crate) project_sources: BTreeMap<String, ProjectSourceIdentity>,
    pub(crate) checkpoint_archive_sizes: BTreeMap<String, Option<u64>>,
    pub(crate) session_operations: BTreeMap<String, SessionOperationDisplay>,
    pub(crate) capacity_details: BTreeMap<String, CapacityDetail>,
    pub(crate) session_index: usize,
    pub(crate) capacity_index: usize,
    pub(crate) quota_index: usize,
    pub(crate) focus: Focus,
    pub(crate) pane_areas: Option<[Rect; DASHBOARD_PANE_COUNT]>,
    pub(crate) resume_sessions_area: Option<Rect>,
    /// Native sessions the resume dialog hides, loaded from Hel's database.
    pub(crate) hidden_native_sessions: BTreeSet<(HarnessKind, String)>,
    /// The rows the open resume dialog shows, derived from the records, the
    /// scans, and the dialog's own search. Rebuilt where those change and once
    /// a second for the activity labels; empty when no dialog is open.
    pub(crate) resume_rows: Vec<crate::resume::ResumeRow>,
    /// Row hitboxes for the Active pane, keyed by the row's index into the
    /// active session list. Each rect spans the summary line and every
    /// visible preview line beneath it, so a click anywhere on the row
    /// selects it.
    pub(crate) active_row_areas: Vec<(usize, Rect)>,
    pub(crate) project_heading_areas: Vec<(String, Rect)>,
    pub(crate) expanded_project_key: Option<String>,
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
            project_sources: BTreeMap::new(),
            checkpoint_archive_sizes: BTreeMap::new(),
            session_operations: BTreeMap::new(),
            capacity_details: BTreeMap::new(),
            session_index: 0,
            capacity_index: 0,
            quota_index: 0,
            focus: Focus::Active,
            pane_areas: None,
            resume_sessions_area: None,
            hidden_native_sessions: BTreeSet::new(),
            resume_rows: Vec::new(),
            active_row_areas: Vec::new(),
            project_heading_areas: Vec::new(),
            expanded_project_key: None,
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
        let (active, _) = partition_sessions(self.state.sessions.values());
        if let Some(index) = active.iter().position(|session| session.id == session_id) {
            self.focus = Focus::Active;
            self.session_index = index;
        }
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> DashboardAction {
        self.handle_key_at(key, Instant::now())
    }

    /// Handles one key with an explicit reading of the clock. `now` decides
    /// whether the notice on screen has been readable long enough for this
    /// key press to dismiss it.
    pub fn handle_key_at(&mut self, key: KeyEvent, now: Instant) -> DashboardAction {
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

        // Retire the notice this key press is stepping past, but only once it
        // has been on screen long enough to read: for a background failure
        // this bar is the only report there is.
        self.notices.dismiss(now);
        // The resume dialog carries every scanned native session, so it is
        // handled where it lives rather than through a copy of the mode.
        if matches!(self.mode, Mode::ResumeDialog(_)) {
            return self.handle_resume_dialog_key(key);
        }
        match self.mode.clone() {
            Mode::Dashboard => self.handle_dashboard_key(key),
            Mode::New(wizard) => self.handle_new_key(key.code, wizard),
            Mode::Resume(wizard) => self.handle_resume_key(key.code, wizard),
            Mode::ResumeDialog(_) => unreachable!("the resume dialog is handled in place"),
            Mode::RepositoryOrigin(dialog) => self.handle_repository_origin_key(key.code, dialog),
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
            Mode::RepositoryOrigin(dialog)
                if dialog.focus == dialogs::RepositoryOriginFocus::Field =>
            {
                dialog.replacement.push_str(&pasted);
                dialog.error = None;
            }
            _ => {}
        }
    }

    pub fn handle_mouse(&mut self, mouse: MouseEvent) -> DashboardAction {
        if matches!(self.mode, Mode::ResumeDialog(_)) {
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
            let len = self.resume_rows().len();
            let Mode::ResumeDialog(dialog) = &mut self.mode else {
                return DashboardAction::None;
            };
            dialog.focus = crate::resume::ResumeFocus::Sessions;
            let index = offset_index(dialog.row_index, len, delta);
            self.select_resume_row(index);
            return DashboardAction::None;
        }
        if !matches!(self.mode, Mode::Dashboard) {
            return DashboardAction::None;
        }
        if mouse.kind == MouseEventKind::Down(MouseButton::Left) {
            if let Some((project_key, _)) = self
                .project_heading_areas
                .iter()
                .find(|(_, area)| rect_contains(*area, mouse.column, mouse.row))
            {
                let project_key = project_key.clone();
                self.expand_project(&project_key);
                return DashboardAction::None;
            }
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
            (KeyCode::Char('a'), true) => self.mark_all_read(),
            (KeyCode::Char(digit @ '1'..='9'), false) if self.focus == Focus::Active => {
                self.expand_project_number(digit.to_digit(10).unwrap_or(0) as usize);
                DashboardAction::None
            }
            (KeyCode::Char(' '), _) if self.focus == Focus::Active => {
                self.expand_selected_project();
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
            (KeyCode::Char('s'), true) => DashboardAction::OpenResumeDialog,
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
        if self.focus == Focus::Active && !self.selected_project_is_expanded() {
            self.expand_selected_project();
            return DashboardAction::None;
        }
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

    /// The sessions the dashboard lists, in creation order. Only live
    /// sessions appear here; everything else belongs to the resume dialog.
    pub(crate) fn ordered_sessions(&self) -> Vec<&SessionRecord> {
        let active = partition_sessions(self.state.sessions.values()).0;
        let mut groups = BTreeMap::<(String, String, String), Vec<&SessionRecord>>::new();
        for session in active {
            let source = self.project_source(session);
            groups
                .entry((source.short.to_lowercase(), source.full, source.key))
                .or_default()
                .push(session);
        }
        groups.into_values().flatten().collect()
    }

    pub fn project_source(&self, session: &SessionRecord) -> ProjectSourceIdentity {
        self.project_sources
            .get(&session.id)
            .cloned()
            .unwrap_or_else(|| session.project_source(&self.config))
    }

    pub fn has_resolved_project_source(&self, session_id: &str) -> bool {
        self.project_sources.contains_key(session_id)
    }

    pub fn set_project_source(&mut self, session_id: &str, source: ProjectSourceIdentity) {
        if self.state.sessions.contains_key(session_id) {
            self.project_sources.insert(session_id.to_owned(), source);
            self.clamp_selections();
        }
    }

    pub(crate) fn project_keys(&self) -> Vec<String> {
        let mut keys = Vec::new();
        for session in self.ordered_sessions() {
            let key = self.project_source(session).key;
            if keys.last() != Some(&key) {
                keys.push(key);
            }
        }
        keys
    }

    pub(crate) fn project_is_expanded(&self, session: &SessionRecord) -> bool {
        let keys = self.project_keys();
        keys.len() <= 1
            || self.expanded_project_key.as_ref() == Some(&self.project_source(session).key)
    }

    fn selected_project_is_expanded(&self) -> bool {
        self.selected_session()
            .is_none_or(|session| self.project_is_expanded(session))
    }

    fn expand_project(&mut self, project_key: &str) {
        self.expanded_project_key = Some(project_key.to_owned());
        if let Some(index) = self
            .ordered_sessions()
            .iter()
            .position(|session| self.project_source(session).key == project_key)
        {
            self.focus = Focus::Active;
            self.session_index = index;
        }
    }

    fn expand_selected_project(&mut self) {
        let key = self
            .selected_session()
            .map(|session| self.project_source(session).key);
        if let Some(key) = key {
            self.expanded_project_key = Some(key);
        }
    }

    fn expand_project_number(&mut self, number: usize) {
        if number == 0 {
            return;
        }
        if let Some(key) = self.project_keys().get(number - 1).cloned() {
            self.expand_project(&key);
        }
    }

    fn mark_all_read(&mut self) -> DashboardAction {
        let mut receipts = Vec::new();
        for (session_id, detail) in &mut self.session_details {
            if detail.unread_agent_messages == 0 {
                continue;
            }
            let Some(through) = detail.materialized_applied_event_ordinal else {
                continue;
            };
            let Some(session) = self.state.sessions.get_mut(session_id) else {
                continue;
            };
            if through > session.viewed_through_event_ordinal {
                session.viewed_through_event_ordinal = through;
                detail.unread_agent_messages = 0;
                receipts.push((session_id.clone(), through));
            }
        }
        if receipts.is_empty() {
            self.set_notice("No unread sessions.");
            DashboardAction::None
        } else {
            self.set_notice("Marked all sessions read.");
            DashboardAction::MarkAllRead { receipts }
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
        let danger = match harness.unsandboxed_guardian_warning() {
            Some(warning) => format!("  ⚠ {warning}"),
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
        self.rebuild_resume_rows();
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
        let project_keys = self.project_keys();
        if project_keys.len() == 1 {
            self.expanded_project_key = project_keys.first().cloned();
        } else if !self
            .expanded_project_key
            .as_ref()
            .is_some_and(|key| project_keys.contains(key))
        {
            self.expanded_project_key = self
                .selected_session()
                .map(|session| self.project_source(session).key)
                .or_else(|| project_keys.first().cloned());
        }
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
    let sequence = |left: &&SessionRecord, right: &&SessionRecord| left.compare_by_creation(right);
    active.sort_by(sequence);
    terminal.sort_by(sequence);
    (active, terminal)
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

    use crossterm::event::{KeyCode, MouseButton, MouseEventKind};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use hel::hel_state::{HelState, STATE_VERSION, SessionState};

    use super::*;
    use crate::test_support::*;

    use crate::render::render;

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
            dashboard.handle_key(ctrl_key('s')),
            DashboardAction::OpenResumeDialog
        );
        assert_eq!(dashboard.handle_key(ctrl_key('t')), DashboardAction::None);
        assert_eq!(dashboard.handle_key(ctrl_key('i')), DashboardAction::None);
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

    /// The notice bar is the only report a background failure gets, so a key
    /// press that happens to arrive while one is fresh must not wipe it.
    #[test]
    fn a_fresh_notice_survives_a_key_press_and_clears_once_it_has_been_readable() {
        let mut session = stopped_session();
        session.state = SessionState::Running;
        let mut dashboard = dashboard_with_session(session);

        dashboard.set_notice("Rename failed: relay unreachable");
        let shown_at = Instant::now();

        assert_eq!(
            dashboard.handle_key_at(key(KeyCode::Down), shown_at),
            DashboardAction::None
        );
        assert_eq!(
            dashboard.notice().as_deref(),
            Some("Rename failed: relay unreachable")
        );

        assert_eq!(
            dashboard.handle_key_at(
                key(KeyCode::Down),
                shown_at + hel::hel_chat::NOTICE_MINIMUM_DISPLAY
            ),
            DashboardAction::None
        );
        assert_eq!(dashboard.notice(), None);
    }

    /// A key press that reports something of its own replaces the notice
    /// whatever its age; the display period only defends against incidental
    /// keys.
    #[test]
    fn a_key_press_with_its_own_notice_replaces_a_fresh_one() {
        let mut session = stopped_session();
        session.state = SessionState::Running;
        let mut dashboard = dashboard_with_session(session);

        dashboard.set_notice("Rename failed: relay unreachable");
        let shown_at = Instant::now();

        assert_eq!(
            dashboard.handle_key_at(ctrl_key('a'), shown_at),
            DashboardAction::None
        );
        assert_eq!(dashboard.notice().as_deref(), Some("No unread sessions."));
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
        let (active, terminal) = partition_sessions(state.sessions.values());
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
    fn sessions_are_ordered_by_creation_sequence_ascending() {
        let mut oldest = stopped_session();
        oldest.id = "session-z".into();
        oldest.created_at = "2026-08-09T01:00:00Z".into();
        let mut newest = stopped_session();
        newest.id = "session-y".into();
        newest.created_at = "2026-08-09T00:30:00-02:00".into();
        let mut invalid_timestamp = stopped_session();
        invalid_timestamp.id = "session-a".into();
        invalid_timestamp.created_at = "unknown".into();

        let (_, terminal) = partition_sessions([&invalid_timestamp, &oldest, &newest]);

        assert_eq!(
            terminal
                .iter()
                .map(|session| session.id.as_str())
                .collect::<Vec<_>>(),
            ["session-z", "session-y", "session-a"]
        );
    }

    #[test]
    fn resolved_git_origin_groups_differently_named_raw_worktrees() {
        let mut first = stopped_session();
        first.id = "bifrost-fird".into();
        first.state = SessionState::Running;
        first.project_directory = Some("/mnt/optane/bifrost-fird".into());
        let mut second = stopped_session();
        second.id = "bifrost-fuzz".into();
        second.state = SessionState::Running;
        second.project_directory = Some("/home/dev/bifrost-fuzz".into());
        let state = HelState {
            version: STATE_VERSION,
            sessions: [first, second]
                .into_iter()
                .map(|session| (session.id.clone(), session))
                .collect(),
            mount_history: BTreeMap::new(),
        };
        let mut dashboard = DashboardState::new(config(), state, BTreeMap::new());
        let source =
            ProjectSourceIdentity::git_remote("git@github.com:BrokkAi/bifrost-dev.git").unwrap();

        dashboard.set_project_source("bifrost-fird", source.clone());
        dashboard.set_project_source("bifrost-fuzz", source);

        assert_eq!(dashboard.project_keys(), ["github:brokkai/bifrost-dev"]);
        assert!(
            dashboard
                .ordered_sessions()
                .iter()
                .all(|session| dashboard.project_is_expanded(session))
        );
    }

    #[test]
    fn mark_all_read_advances_a_materialized_session_and_returns_its_receipt() {
        let mut dashboard = dashboard_with_session(running_session());
        apply_materialized_transcript(&mut dashboard, vec![agent_message(4, "unread response")]);
        assert_eq!(
            dashboard.session_details["session-1"].unread_agent_messages,
            1
        );

        assert_eq!(
            dashboard.handle_key(ctrl_key('a')),
            DashboardAction::MarkAllRead {
                receipts: vec![("session-1".into(), 4)]
            }
        );
        assert_eq!(
            dashboard.session_details["session-1"].unread_agent_messages,
            0
        );
        assert_eq!(
            dashboard.state.sessions["session-1"].viewed_through_event_ordinal,
            4
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
    fn opening_the_selected_project_group_preserves_the_selected_session() {
        let sessions = (0..3)
            .map(|index| {
                let mut session = stopped_session();
                session.id = format!("session-{index}");
                session.state = SessionState::Running;
                session.project_directory = Some(if index < 2 {
                    "/projects/shared".into()
                } else {
                    "/projects/other".into()
                });
                session.created_at = format!("2026-08-1{}T00:00:00Z", index + 1);
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
        let other_key = dashboard
            .project_source(&dashboard.state.sessions["session-2"])
            .key;
        dashboard.expand_project(&other_key);
        dashboard.session_index = dashboard
            .ordered_sessions()
            .iter()
            .position(|session| session.id == "session-1")
            .unwrap();
        let selected_index = dashboard.session_index;
        assert!(!dashboard.selected_project_is_expanded());

        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::None
        );

        assert_eq!(dashboard.session_index, selected_index);
        assert_eq!(dashboard.selected_session().unwrap().id, "session-1");
        assert!(dashboard.selected_project_is_expanded());
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
