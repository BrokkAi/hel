//! Full-screen dashboard and session picker for Hel.
//!
//! This module deliberately has no provisioning or persistence side effects.
//! Input is reduced to [`DashboardAction`] values for the controller to run.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, TableState, Wrap};

use crate::hel_config::{HarnessKind, HelConfig, TargetTemplate};
use crate::hel_quota::ProfileQuota;
use crate::hel_state::{HelState, SessionRecord, SessionState};
use crate::hel_targets::{AdditionalMount, default_mount_destination, path_completion};
use crate::hel_worker::{SequencedEvent, WorkerEvent};

const FORCE_CONFIRMATION: &str = "DESTROY";

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
        target_template_id: String,
        additional_mounts: Vec<AdditionalMount>,
    },
    CompleteMountSource {
        target_template_id: String,
        prefix: String,
    },
    ResumeSession {
        session_id: String,
        profile_id: String,
        target_template_id: String,
    },
    Checkpoint {
        session_id: String,
    },
    Close {
        session_id: String,
    },
    ForceDestroy {
        session_id: String,
    },
    RefreshQuotas,
    OpenConfig,
    QuitDetach,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Focus {
    Sessions,
    Quotas,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WizardStep {
    Profile,
    Bundle,
    Target,
    Mounts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NewWizard {
    step: WizardStep,
    profile: usize,
    bundle: usize,
    target: usize,
    mounts: MountWizard,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MountField {
    Source,
    Destination,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MountWizard {
    source: String,
    destination: String,
    field: MountField,
    mounts: Vec<AdditionalMount>,
    history: Vec<std::path::PathBuf>,
    history_index: usize,
    completion_cache: BTreeMap<String, Vec<String>>,
}

impl MountWizard {
    fn new(history: Vec<std::path::PathBuf>) -> Self {
        Self {
            source: String::new(),
            destination: String::new(),
            field: MountField::Source,
            mounts: Vec::new(),
            history,
            history_index: 0,
            completion_cache: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResumeWizard {
    session_id: String,
    step: WizardStep,
    profile: usize,
    target: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Confirmation {
    Close { session_id: String },
    ForceDestroy { session_id: String, typed: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Mode {
    Dashboard,
    New(NewWizard),
    Resume(ResumeWizard),
    Confirm(Confirmation),
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

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct SessionDetail {
    last_event_sequence: u64,
    current_turn_started_at: Option<u64>,
    last_turn_completed_at: Option<u64>,
    activity: Option<SessionActivity>,
}

/// Stateful, renderable projection of controller configuration and state.
pub struct DashboardState {
    config: HelConfig,
    state: HelState,
    quotas: BTreeMap<String, ProfileQuota>,
    quota_refreshing: BTreeSet<String>,
    session_details: BTreeMap<String, SessionDetail>,
    session_index: usize,
    quota_index: usize,
    focus: Focus,
    mode: Mode,
    notice: Option<String>,
}

impl DashboardState {
    pub fn new(config: HelConfig, state: HelState, quotas: BTreeMap<String, ProfileQuota>) -> Self {
        let mut dashboard = Self {
            config,
            state,
            quotas,
            quota_refreshing: BTreeSet::new(),
            session_details: BTreeMap::new(),
            session_index: 0,
            quota_index: 0,
            focus: Focus::Sessions,
            mode: Mode::Dashboard,
            notice: None,
        };
        dashboard.clamp_selections();
        dashboard
    }

    pub fn set_config(&mut self, config: HelConfig) {
        self.config = config;
        self.cancel_modal();
        self.clamp_selections();
    }

    pub fn set_state(&mut self, state: HelState) {
        self.state = state;
        self.clamp_selections();
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

    /// Incorporate the newly replayed worker events for one session. Details
    /// are intentionally dashboard-local; only harness titles need durable
    /// controller state.
    pub fn apply_worker_events(
        &mut self,
        session_id: &str,
        events: &[SequencedEvent],
        observed_at_epoch_seconds: u64,
    ) {
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
                    detail.current_turn_started_at = Some(observed_at_epoch_seconds);
                }
                WorkerEvent::TurnCompleted | WorkerEvent::Cancelled => {
                    detail.current_turn_started_at = None;
                    detail.last_turn_completed_at = Some(observed_at_epoch_seconds);
                }
                WorkerEvent::Adapter { payload, .. } => {
                    if let Some(activity) = activity_from_adapter(payload) {
                        update_activity(detail, activity);
                    }
                }
                WorkerEvent::ConfigChanged { .. }
                | WorkerEvent::Checkpointed { .. }
                | WorkerEvent::Closing
                | WorkerEvent::Closed => {}
            }
        }
    }

    pub fn set_notice(&mut self, notice: impl Into<String>) {
        self.notice = Some(notice.into());
    }

    pub fn clear_notice(&mut self) {
        self.notice = None;
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> DashboardAction {
        if key.kind != KeyEventKind::Press && key.kind != KeyEventKind::Repeat {
            return DashboardAction::None;
        }
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            return DashboardAction::QuitDetach;
        }

        self.notice = None;
        match self.mode.clone() {
            Mode::Dashboard => self.handle_dashboard_key(key.code),
            Mode::New(wizard) => self.handle_new_key(key.code, wizard),
            Mode::Resume(wizard) => self.handle_resume_key(key.code, wizard),
            Mode::Confirm(confirmation) => self.handle_confirmation_key(key.code, confirmation),
        }
    }

    fn handle_dashboard_key(&mut self, code: KeyCode) -> DashboardAction {
        match code {
            KeyCode::Char('q') | KeyCode::Esc => DashboardAction::QuitDetach,
            KeyCode::Tab | KeyCode::BackTab => {
                self.focus = match self.focus {
                    Focus::Sessions => Focus::Quotas,
                    Focus::Quotas => Focus::Sessions,
                };
                DashboardAction::None
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.move_selection(-1);
                DashboardAction::None
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.move_selection(1);
                DashboardAction::None
            }
            KeyCode::Home => {
                self.set_selection(0);
                DashboardAction::None
            }
            KeyCode::End => {
                let len = self.focus_len();
                self.set_selection(len.saturating_sub(1));
                DashboardAction::None
            }
            KeyCode::Char('n') => {
                self.begin_new();
                DashboardAction::None
            }
            KeyCode::Char('r') => {
                if self.focus == Focus::Quotas {
                    DashboardAction::RefreshQuotas
                } else {
                    self.begin_resume();
                    DashboardAction::None
                }
            }
            KeyCode::Char('u') => DashboardAction::RefreshQuotas,
            KeyCode::Char('e') if self.config_is_empty() => DashboardAction::OpenConfig,
            KeyCode::Char('p') => self
                .selected_session()
                .map(|session| DashboardAction::Checkpoint {
                    session_id: session.id.clone(),
                })
                .unwrap_or(DashboardAction::None),
            KeyCode::Char('c') => {
                if let Some(session) = self.selected_session() {
                    self.mode = Mode::Confirm(Confirmation::Close {
                        session_id: session.id.clone(),
                    });
                }
                DashboardAction::None
            }
            KeyCode::Char('x') => {
                if let Some(session) = self.selected_session() {
                    self.mode = Mode::Confirm(Confirmation::ForceDestroy {
                        session_id: session.id.clone(),
                        typed: String::new(),
                    });
                }
                DashboardAction::None
            }
            KeyCode::Enter | KeyCode::Char('o') => self.open_or_resume(),
            _ => DashboardAction::None,
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
        let len = match wizard.step {
            WizardStep::Profile => self.config.profiles.len(),
            WizardStep::Bundle => self.config.bundles.len(),
            WizardStep::Target => self.config.targets.len(),
            WizardStep::Mounts => unreachable!("mount input is handled before picker navigation"),
        };
        if matches!(code, KeyCode::Up | KeyCode::Char('k')) {
            move_index(wizard.active_index_mut(), len, -1);
            self.mode = Mode::New(wizard);
            return DashboardAction::None;
        }
        if matches!(code, KeyCode::Down | KeyCode::Char('j')) {
            move_index(wizard.active_index_mut(), len, 1);
            self.mode = Mode::New(wizard);
            return DashboardAction::None;
        }
        if matches!(code, KeyCode::Backspace | KeyCode::Left) {
            wizard.step = match wizard.step {
                WizardStep::Profile => {
                    self.cancel_modal();
                    return DashboardAction::None;
                }
                WizardStep::Bundle => WizardStep::Profile,
                WizardStep::Target => WizardStep::Bundle,
                WizardStep::Mounts => {
                    unreachable!("mount input is handled before picker navigation")
                }
            };
            self.mode = Mode::New(wizard);
            return DashboardAction::None;
        }
        if !matches!(code, KeyCode::Enter | KeyCode::Right) {
            self.mode = Mode::New(wizard);
            return DashboardAction::None;
        }

        match wizard.step {
            WizardStep::Profile => {
                wizard.step = WizardStep::Bundle;
                self.mode = Mode::New(wizard);
                DashboardAction::None
            }
            WizardStep::Bundle => {
                wizard.step = WizardStep::Target;
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
                if let Some(host) = mount_history_host(target) {
                    wizard.step = WizardStep::Mounts;
                    wizard.mounts = MountWizard::new(
                        self.state
                            .mount_history
                            .get(host)
                            .cloned()
                            .unwrap_or_default(),
                    );
                    self.mode = Mode::New(wizard);
                    DashboardAction::None
                } else {
                    self.create_session_action(&wizard)
                }
            }
            WizardStep::Mounts => unreachable!("mount input is handled before picker navigation"),
        }
    }

    fn handle_mount_key(&mut self, code: KeyCode, mut wizard: NewWizard) -> DashboardAction {
        let target_template_id = nth_key(&self.config.targets, wizard.target);
        match code {
            KeyCode::Tab if wizard.mounts.field == MountField::Source => {
                let prefix = wizard.mounts.source.clone();
                if prefix.is_empty() {
                    self.mode = Mode::New(wizard);
                    return DashboardAction::None;
                }
                if let Some(candidates) = wizard.mounts.completion_cache.get(&prefix) {
                    if let Some(completed) = path_completion(&prefix, candidates) {
                        wizard.mounts.source = completed;
                    }
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
            KeyCode::Up
                if wizard.mounts.field == MountField::Source
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
                if wizard.mounts.field == MountField::Source
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
                match wizard.mounts.field {
                    MountField::Source if !wizard.mounts.source.is_empty() => {
                        wizard.mounts.source.pop();
                    }
                    MountField::Source => wizard.step = WizardStep::Target,
                    MountField::Destination if !wizard.mounts.destination.is_empty() => {
                        wizard.mounts.destination.pop();
                    }
                    MountField::Destination => wizard.mounts.field = MountField::Source,
                }
                self.mode = Mode::New(wizard);
                DashboardAction::None
            }
            KeyCode::Left => {
                match wizard.mounts.field {
                    MountField::Source => wizard.step = WizardStep::Target,
                    MountField::Destination => wizard.mounts.field = MountField::Source,
                }
                self.mode = Mode::New(wizard);
                DashboardAction::None
            }
            KeyCode::Enter | KeyCode::Right => match wizard.mounts.field {
                MountField::Source if wizard.mounts.source.is_empty() => {
                    self.create_session_action(&wizard)
                }
                MountField::Source => {
                    wizard.mounts.destination = default_mount_destination(
                        std::path::Path::new(&wizard.mounts.source),
                        &wizard.mounts.mounts,
                    )
                    .to_string_lossy()
                    .into_owned();
                    wizard.mounts.field = MountField::Destination;
                    self.mode = Mode::New(wizard);
                    DashboardAction::None
                }
                MountField::Destination => {
                    let mount = AdditionalMount {
                        source: wizard.mounts.source.clone().into(),
                        destination: wizard.mounts.destination.clone().into(),
                    };
                    if let Err(error) =
                        crate::hel_targets::validate_additional_mounts(std::slice::from_ref(&mount))
                    {
                        self.notice = Some(format!("Invalid mount: {error}"));
                        self.mode = Mode::New(wizard);
                        return DashboardAction::None;
                    }
                    if wizard
                        .mounts
                        .mounts
                        .iter()
                        .any(|existing| existing.destination == mount.destination)
                    {
                        self.notice = Some(format!(
                            "{} is already a mount destination.",
                            mount.destination.display()
                        ));
                        self.mode = Mode::New(wizard);
                        return DashboardAction::None;
                    }
                    wizard.mounts.mounts.push(mount);
                    wizard.mounts.source.clear();
                    wizard.mounts.destination.clear();
                    wizard.mounts.field = MountField::Source;
                    self.mode = Mode::New(wizard);
                    DashboardAction::None
                }
            },
            KeyCode::Char(character) => {
                match wizard.mounts.field {
                    MountField::Source => wizard.mounts.source.push(character),
                    MountField::Destination => wizard.mounts.destination.push(character),
                }
                self.mode = Mode::New(wizard);
                DashboardAction::None
            }
            _ => {
                self.mode = Mode::New(wizard);
                DashboardAction::None
            }
        }
    }

    fn create_session_action(&mut self, wizard: &NewWizard) -> DashboardAction {
        let action = DashboardAction::CreateSession {
            profile_id: nth_key(&self.config.profiles, wizard.profile),
            bundle_id: nth_key(&self.config.bundles, wizard.bundle),
            target_template_id: nth_key(&self.config.targets, wizard.target),
            additional_mounts: wizard.mounts.mounts.clone(),
        };
        self.cancel_modal();
        action
    }

    /// Apply a completion response only when the source text has not changed
    /// since the request left the UI. Typed input always outranks suggestions.
    pub fn apply_mount_source_completions(&mut self, prefix: &str, candidates: Vec<String>) {
        let Mode::New(mut wizard) = self.mode.clone() else {
            return;
        };
        if wizard.step != WizardStep::Mounts
            || wizard.mounts.field != MountField::Source
            || wizard.mounts.source != prefix
        {
            return;
        }
        wizard
            .mounts
            .completion_cache
            .insert(prefix.to_owned(), candidates.clone());
        if let Some(completed) = path_completion(prefix, &candidates) {
            wizard.mounts.source = completed;
        }
        self.mode = Mode::New(wizard);
    }

    fn handle_resume_key(&mut self, code: KeyCode, mut wizard: ResumeWizard) -> DashboardAction {
        if code == KeyCode::Esc {
            self.cancel_modal();
            return DashboardAction::None;
        }
        let profiles = self.compatible_profiles(&wizard.session_id);
        let len = match wizard.step {
            WizardStep::Profile => profiles.len(),
            WizardStep::Target => self.config.targets.len(),
            WizardStep::Bundle => unreachable!("resume does not select a bundle"),
            WizardStep::Mounts => unreachable!("resume does not select mounts"),
        };
        if matches!(code, KeyCode::Up | KeyCode::Char('k')) {
            move_index(wizard.active_index_mut(), len, -1);
            self.mode = Mode::Resume(wizard);
            return DashboardAction::None;
        }
        if matches!(code, KeyCode::Down | KeyCode::Char('j')) {
            move_index(wizard.active_index_mut(), len, 1);
            self.mode = Mode::Resume(wizard);
            return DashboardAction::None;
        }
        if matches!(code, KeyCode::Backspace | KeyCode::Left) {
            match wizard.step {
                WizardStep::Profile => self.cancel_modal(),
                WizardStep::Target => {
                    wizard.step = WizardStep::Profile;
                    self.mode = Mode::Resume(wizard);
                }
                WizardStep::Bundle => unreachable!("resume does not select a bundle"),
                WizardStep::Mounts => unreachable!("resume does not select mounts"),
            }
            return DashboardAction::None;
        }
        if !matches!(code, KeyCode::Enter | KeyCode::Right) {
            self.mode = Mode::Resume(wizard);
            return DashboardAction::None;
        }
        match wizard.step {
            WizardStep::Profile => {
                wizard.step = WizardStep::Target;
                self.mode = Mode::Resume(wizard);
                DashboardAction::None
            }
            WizardStep::Target => {
                let action = DashboardAction::ResumeSession {
                    session_id: wizard.session_id,
                    profile_id: profiles
                        .get(wizard.profile)
                        .map(|(id, _)| (*id).clone())
                        .expect("resume wizard is only opened with a compatible profile"),
                    target_template_id: nth_key(&self.config.targets, wizard.target),
                };
                self.cancel_modal();
                action
            }
            WizardStep::Bundle => unreachable!("resume does not select a bundle"),
            WizardStep::Mounts => unreachable!("resume does not select mounts"),
        }
    }

    fn handle_confirmation_key(
        &mut self,
        code: KeyCode,
        confirmation: Confirmation,
    ) -> DashboardAction {
        match confirmation {
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
        }
    }

    fn begin_new(&mut self) {
        if self.config.profiles.is_empty()
            || self.config.bundles.is_empty()
            || self.config.targets.is_empty()
        {
            self.notice = Some("Configure at least one profile, bundle, and target first.".into());
            return;
        }
        self.mode = Mode::New(NewWizard {
            step: WizardStep::Profile,
            profile: 0,
            bundle: 0,
            target: 0,
            mounts: MountWizard::new(Vec::new()),
        });
    }

    fn begin_resume(&mut self) {
        let Some(session) = self.selected_session() else {
            return;
        };
        if session.state.is_active() {
            self.notice = Some("This session is active; press Enter to open it.".into());
            return;
        }
        if session.checkpoint.is_none() {
            self.notice = Some("This session has no verified checkpoint to resume.".into());
            return;
        }
        if self.compatible_profiles(&session.id).is_empty() || self.config.targets.is_empty() {
            self.notice = Some("Resume needs a profile and a target template.".into());
            return;
        }
        self.mode = Mode::Resume(ResumeWizard {
            session_id: session.id.clone(),
            step: WizardStep::Profile,
            profile: 0,
            target: 0,
        });
    }

    fn open_or_resume(&mut self) -> DashboardAction {
        let Some(session) = self.selected_session() else {
            return DashboardAction::None;
        };
        if session.state.is_active() {
            DashboardAction::Open {
                session_id: session.id.clone(),
            }
        } else {
            self.begin_resume();
            DashboardAction::None
        }
    }

    fn selected_session(&self) -> Option<&SessionRecord> {
        self.ordered_sessions().get(self.session_index).copied()
    }

    fn ordered_sessions(&self) -> Vec<&SessionRecord> {
        let (active, archived) = partition_sessions(self.state.sessions.values());
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
        format!(
            "{id}  {}  [{}]  ·  {quota}",
            harness_label(harness),
            harness.unrestricted_mode(),
        )
    }

    fn config_is_empty(&self) -> bool {
        self.config.profiles.is_empty()
            || self.config.bundles.is_empty()
            || self.config.targets.is_empty()
    }

    fn cancel_modal(&mut self) {
        self.mode = Mode::Dashboard;
    }

    fn focus_len(&self) -> usize {
        match self.focus {
            Focus::Sessions => self.state.sessions.len(),
            Focus::Quotas => self.config.profiles.len(),
        }
    }

    fn set_selection(&mut self, index: usize) {
        match self.focus {
            Focus::Sessions => self.session_index = index,
            Focus::Quotas => self.quota_index = index,
        }
    }

    fn move_selection(&mut self, delta: isize) {
        let len = self.focus_len();
        match self.focus {
            Focus::Sessions => move_index(&mut self.session_index, len, delta),
            Focus::Quotas => move_index(&mut self.quota_index, len, delta),
        }
    }

    fn clamp_selections(&mut self) {
        self.session_index = self
            .session_index
            .min(self.state.sessions.len().saturating_sub(1));
        self.quota_index = self
            .quota_index
            .min(self.config.profiles.len().saturating_sub(1));
    }
}

fn partition_sessions<'a>(
    sessions: impl IntoIterator<Item = &'a SessionRecord>,
) -> (Vec<&'a SessionRecord>, Vec<&'a SessionRecord>) {
    let mut active = Vec::new();
    let mut archived = Vec::new();
    for session in sessions {
        if session.state == SessionState::Archived {
            archived.push(session);
        } else {
            active.push(session);
        }
    }
    (active, archived)
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
            .map(str::trim)
            .filter(|text| !text.is_empty())
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
            .map(str::trim)
            .filter(|title| !title.is_empty())
            .map(|text| SessionActivity {
                kind: ActivityKind::ToolCall,
                text: text.to_string(),
            }),
        _ => None,
    }
}

fn update_activity(detail: &mut SessionDetail, activity: SessionActivity) {
    if let Some(previous) = detail.activity.as_mut()
        && previous.kind == activity.kind
        && matches!(
            activity.kind,
            ActivityKind::Thinking | ActivityKind::AgentText
        )
    {
        previous.text.push_str(&activity.text);
        previous.text = truncate_text(&previous.text, 512);
        return;
    }
    detail.activity = Some(activity);
}

fn activity_label(activity: &SessionActivity) -> String {
    let label = match activity.kind {
        ActivityKind::Thinking => "thinking",
        ActivityKind::AgentText => "agent",
        ActivityKind::ToolCall => "tool",
    };
    format!("{label}: {}", activity.text)
}

fn truncate_text(text: &str, width: usize) -> String {
    let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
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

impl NewWizard {
    fn active_index_mut(&mut self) -> &mut usize {
        match self.step {
            WizardStep::Profile => &mut self.profile,
            WizardStep::Bundle => &mut self.bundle,
            WizardStep::Target => &mut self.target,
            WizardStep::Mounts => unreachable!("mount input has no picker index"),
        }
    }
}

impl ResumeWizard {
    fn active_index_mut(&mut self) -> &mut usize {
        match self.step {
            WizardStep::Profile => &mut self.profile,
            WizardStep::Target => &mut self.target,
            WizardStep::Bundle => unreachable!("resume does not select a bundle"),
            WizardStep::Mounts => unreachable!("resume does not select mounts"),
        }
    }
}

pub fn render(frame: &mut Frame, dashboard: &mut DashboardState) {
    let area = frame.area();
    let outer = Block::default()
        .borders(Borders::ALL)
        .title(" HEL ")
        .border_style(Style::default().fg(Color::DarkGray));
    let inner = outer.inner(area);
    frame.render_widget(outer, area);

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(8),
            Constraint::Length(8),
            Constraint::Length(2),
        ])
        .split(inner);
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(
                "Welcome to Hel.",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("  ACP sessions, wherever they run."),
        ]))
        .alignment(Alignment::Center),
        layout[0],
    );

    if dashboard.config_is_empty() {
        render_onboarding(frame, layout[1], dashboard);
    } else {
        render_sessions(frame, layout[1], dashboard);
    }
    render_quotas(frame, layout[2], dashboard);
    render_footer(frame, layout[3], dashboard);

    match &dashboard.mode {
        Mode::New(wizard) => render_new_wizard(frame, area, dashboard, wizard),
        Mode::Resume(wizard) => render_resume_wizard(frame, area, dashboard, wizard),
        Mode::Confirm(confirmation) => render_confirmation(frame, area, confirmation),
        Mode::Dashboard => {}
    }
}

fn render_onboarding(frame: &mut Frame, area: Rect, dashboard: &DashboardState) {
    let missing = [
        (dashboard.config.profiles.is_empty(), "a harness profile"),
        (dashboard.config.bundles.is_empty(), "a project bundle"),
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
            Line::raw("Press e to run setup, or edit Hel's TOML configuration by hand."),
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

fn render_sessions(frame: &mut Frame, area: Rect, dashboard: &mut DashboardState) {
    let (active, archived) = partition_sessions(dashboard.state.sessions.values());
    let now_epoch_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let width = area.width.saturating_sub(8) as usize;
    let mut rows = Vec::new();
    let mut selected_row = None;
    let mut session_index = 0;

    rows.push(session_section_row("Active", active.len()));
    for session in active {
        if session_index == dashboard.session_index {
            selected_row = Some(rows.len());
        }
        rows.push(session_row(
            session,
            dashboard.session_details.get(&session.id),
            now_epoch_seconds,
            width,
        ));
        session_index += 1;
    }
    if !archived.is_empty() {
        rows.push(session_section_row("Archived", archived.len()));
        for session in archived {
            if session_index == dashboard.session_index {
                selected_row = Some(rows.len());
            }
            rows.push(session_row(
                session,
                dashboard.session_details.get(&session.id),
                now_epoch_seconds,
                width,
            ));
            session_index += 1;
        }
    }
    let title = if dashboard.focus == Focus::Sessions {
        " Sessions [focused] "
    } else {
        " Sessions "
    };
    let table = Table::new(rows, [Constraint::Percentage(100)])
        .row_highlight_style(Style::default().bg(Color::DarkGray).fg(Color::White))
        .highlight_symbol("› ")
        .block(Block::default().borders(Borders::ALL).title(title));
    let mut state = TableState::default().with_selected(selected_row);
    frame.render_stateful_widget(table, area, &mut state);
}

fn session_section_row(label: &str, count: usize) -> Row<'static> {
    Row::new([Cell::from(format!(" {label} ({count}) "))])
        .style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
        .height(1)
}

fn session_row(
    session: &SessionRecord,
    detail: Option<&SessionDetail>,
    now_epoch_seconds: u64,
    width: usize,
) -> Row<'static> {
    let checkpoint = session
        .checkpoint
        .as_ref()
        .map(|checkpoint| checkpoint.created_at.as_str())
        .unwrap_or("never");
    let summary = format!(
        "{}  ·  {} / {}  ·  {}  ·  {}  ·  checkpoint {}",
        session.title,
        harness_label(session.harness_kind),
        session.last_profile,
        session.target_template_id,
        state_label(session.state),
        checkpoint,
    );
    let clock = crate::usage_format::format_turn_clock(
        now_epoch_seconds,
        detail.and_then(|detail| detail.current_turn_started_at),
        detail.and_then(|detail| detail.last_turn_completed_at),
    );
    let activity = detail
        .and_then(|detail| detail.activity.as_ref())
        .map(activity_label);
    let detail = match activity {
        Some(activity) => format!("{clock}  ·  {activity}"),
        None => clock,
    };
    Row::new([Cell::from(vec![
        Line::raw(truncate_text(&summary, width)),
        Line::styled(
            truncate_text(&detail, width),
            Style::default().fg(Color::DarkGray),
        ),
    ])])
    .height(2)
}

fn render_quotas(frame: &mut Frame, area: Rect, dashboard: &mut DashboardState) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let rows = dashboard.config.profiles.iter().map(|(id, profile)| {
        let (usage, refreshed) = if dashboard.quota_refreshing.contains(id) {
            ("refreshing".into(), "…".into())
        } else {
            match dashboard.quotas.get(id) {
                Some(quota) => (
                    quota.compact(),
                    quota_age(now, quota.refreshed_at_epoch_seconds),
                ),
                None => ("refreshing".into(), "…".into()),
            }
        };
        Row::new([
            Cell::from(id.clone()),
            Cell::from(harness_label(profile.kind)),
            Cell::from(profile.unrestricted_mode()),
            Cell::from(usage),
            Cell::from(refreshed),
        ])
    });
    let title = if dashboard.focus == Focus::Quotas {
        " Profile quotas [focused] "
    } else {
        " Profile quotas "
    };
    let table = Table::new(
        rows,
        [
            Constraint::Percentage(14),
            Constraint::Percentage(12),
            Constraint::Percentage(18),
            Constraint::Percentage(44),
            Constraint::Percentage(12),
        ],
    )
    .header(
        Row::new([
            "Profile",
            "Harness",
            "Access",
            "Quota / reset / error",
            "Refreshed",
        ])
        .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .row_highlight_style(Style::default().bg(Color::DarkGray).fg(Color::White))
    .highlight_symbol("› ")
    .block(Block::default().borders(Borders::ALL).title(title));
    let mut state = TableState::default()
        .with_selected((!dashboard.config.profiles.is_empty()).then_some(dashboard.quota_index));
    frame.render_stateful_widget(table, area, &mut state);
}

fn render_footer(frame: &mut Frame, area: Rect, dashboard: &DashboardState) {
    let text = dashboard.notice.as_deref().unwrap_or(
        "n new · Enter open/resume · p checkpoint · c close · x force · u quota · Tab pane · q detach",
    );
    let style = if dashboard.notice.is_some() {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    frame.render_widget(Paragraph::new(text).style(style), area);
}

fn render_new_wizard(
    frame: &mut Frame,
    area: Rect,
    dashboard: &DashboardState,
    wizard: &NewWizard,
) {
    if wizard.step == WizardStep::Mounts {
        render_mount_wizard(frame, area, dashboard, wizard);
        return;
    }
    let (title, choices, selected) = match wizard.step {
        WizardStep::Profile => (
            " New session · 1/3 profile ",
            dashboard
                .config
                .profiles
                .iter()
                .map(|(id, profile)| dashboard.profile_choice(id, profile.kind))
                .collect(),
            wizard.profile,
        ),
        WizardStep::Bundle => (
            " New session · 2/3 project bundle ",
            dashboard
                .config
                .bundles
                .iter()
                .map(|(id, bundle)| format!("{id}  {} repositories", bundle.repositories.len()))
                .collect(),
            wizard.bundle,
        ),
        WizardStep::Target => (
            " New session · 3/3 target ",
            dashboard
                .config
                .targets
                .iter()
                .map(|(id, target)| format!("{id}  {}", target_label(target)))
                .collect(),
            wizard.target,
        ),
        WizardStep::Mounts => unreachable!("mount input was rendered above"),
    };
    render_picker(
        frame,
        area,
        title,
        choices,
        selected,
        "Enter next · ← back · Esc cancel",
    );
}

fn render_mount_wizard(
    frame: &mut Frame,
    area: Rect,
    dashboard: &DashboardState,
    wizard: &NewWizard,
) {
    let target_id = nth_key(&dashboard.config.targets, wizard.target);
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
        TargetTemplate::AwsEc2 { .. } | TargetTemplate::SshBare { .. } => {
            unreachable!("only container targets enter the mount wizard")
        }
    };
    let source_marker = if wizard.mounts.field == MountField::Source {
        "› "
    } else {
        "  "
    };
    let destination_marker = if wizard.mounts.field == MountField::Destination {
        "› "
    } else {
        "  "
    };
    let mut lines = vec![
        Line::raw(format!("Target: {target_id} ({})", target_label(target))),
        Line::styled(protection, Style::default().fg(Color::Yellow)),
        Line::raw(""),
        Line::styled(
            format!("{source_marker}Source: {}", wizard.mounts.source),
            if wizard.mounts.field == MountField::Source {
                Style::default().bg(Color::DarkGray).fg(Color::White)
            } else {
                Style::default()
            },
        ),
        Line::styled(
            format!(
                "{destination_marker}Destination: {}",
                wizard.mounts.destination
            ),
            if wizard.mounts.field == MountField::Destination {
                Style::default().bg(Color::DarkGray).fg(Color::White)
            } else {
                Style::default()
            },
        ),
    ];
    if !wizard.mounts.mounts.is_empty() {
        lines.push(Line::raw(""));
        lines.push(Line::raw("Additional mount points:"));
        lines.extend(wizard.mounts.mounts.iter().map(|mount| {
            Line::raw(format!(
                "  {} → {}",
                mount.source.display(),
                mount.destination.display()
            ))
        }));
    }
    if wizard.mounts.field == MountField::Source && !wizard.mounts.history.is_empty() {
        lines.push(Line::raw(""));
        lines.push(Line::styled(
            "Recent sources (↑/↓ when Source is empty):",
            Style::default().fg(Color::DarkGray),
        ));
        lines.extend(
            wizard
                .mounts
                .history
                .iter()
                .take(5)
                .enumerate()
                .map(|(index, source)| {
                    let marker = if index == wizard.mounts.history_index {
                        "› "
                    } else {
                        "  "
                    };
                    Line::raw(format!("{marker}{}", source.display()))
                }),
        );
    }
    if let Some(candidates) = wizard.mounts.completion_cache.get(&wizard.mounts.source) {
        lines.push(Line::raw(""));
        lines.push(Line::styled(
            format!("Tab matches: {}", candidates.join("  ")),
            Style::default().fg(Color::DarkGray),
        ));
    }
    lines.extend([
        Line::raw(""),
        Line::styled(
            "Enter accepts a field; empty Source continues · Tab completes · ← back · Esc cancel",
            Style::default().fg(Color::DarkGray),
        ),
    ]);
    let popup = centered_rect(84, (lines.len() as u16 + 2).clamp(12, 24), area);
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(lines)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" New session · 4/4 additional mount points "),
            )
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
    let (title, choices, selected) = match wizard.step {
        WizardStep::Profile => (
            " Resume · 1/2 profile (cross-harness supported) ",
            dashboard
                .compatible_profiles(&wizard.session_id)
                .into_iter()
                .map(|(id, harness)| dashboard.profile_choice(id, harness))
                .collect(),
            wizard.profile,
        ),
        WizardStep::Target => (
            " Resume · 2/2 new target ",
            dashboard
                .config
                .targets
                .iter()
                .map(|(id, target)| format!("{id}  {}", target_label(target)))
                .collect(),
            wizard.target,
        ),
        WizardStep::Bundle => unreachable!("resume does not select a bundle"),
        WizardStep::Mounts => unreachable!("resume does not select mounts"),
    };
    render_picker(
        frame,
        area,
        title,
        choices,
        selected,
        "Enter next · ← back · Esc cancel",
    );
}

fn render_picker(
    frame: &mut Frame,
    area: Rect,
    title: &str,
    choices: Vec<String>,
    selected: usize,
    help: &str,
) {
    let popup = centered_rect(68, (choices.len() as u16 + 5).clamp(8, 18), area);
    frame.render_widget(Clear, popup);
    let lines = choices
        .into_iter()
        .enumerate()
        .map(|(index, choice)| {
            let marker = if index == selected { "› " } else { "  " };
            let style = if index == selected {
                Style::default().bg(Color::DarkGray).fg(Color::White)
            } else {
                Style::default()
            };
            Line::styled(format!("{marker}{choice}"), style)
        })
        .chain([
            Line::raw(""),
            Line::styled(help, Style::default().fg(Color::DarkGray)),
        ])
        .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(lines)
            .block(Block::default().borders(Borders::ALL).title(title))
            .wrap(Wrap { trim: false }),
        popup,
    );
}

fn render_confirmation(frame: &mut Frame, area: Rect, confirmation: &Confirmation) {
    let popup = centered_rect(64, 9, area);
    frame.render_widget(Clear, popup);
    let (title, lines) = match confirmation {
        Confirmation::Close { session_id } => (
            " Close and archive session? ",
            vec![
                Line::raw(format!("Session: {session_id}")),
                Line::raw(""),
                Line::raw("Hel will verify the checkpoint before destroying the target."),
                Line::raw("Press y/Enter to close, or n/Esc to cancel."),
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

fn harness_label(kind: HarnessKind) -> &'static str {
    match kind {
        HarnessKind::Codex => "Codex",
        HarnessKind::Claude => "Claude Code",
        HarnessKind::Kimi => "Kimi Code",
    }
}

fn target_label(target: &TargetTemplate) -> &'static str {
    match target {
        TargetTemplate::LocalPodman { .. } => "local Podman",
        TargetTemplate::AppleContainer { .. } => "Apple container",
        TargetTemplate::AwsEc2 { .. } => "AWS EC2",
        TargetTemplate::SshBare { .. } => "named SSH machine",
        TargetTemplate::SshPodman { .. } => "Podman over SSH",
    }
}

fn mount_history_host(target: &TargetTemplate) -> Option<&str> {
    match target {
        TargetTemplate::LocalPodman { .. } | TargetTemplate::AppleContainer { .. } => Some("local"),
        TargetTemplate::SshPodman { ssh, .. } => Some(&ssh.host),
        TargetTemplate::AwsEc2 { .. } | TargetTemplate::SshBare { .. } => None,
    }
}

fn state_label(state: SessionState) -> &'static str {
    match state {
        SessionState::Provisioning => "provisioning",
        SessionState::Running => "running",
        SessionState::Disconnected => "disconnected",
        SessionState::Checkpointing => "checkpointing",
        SessionState::Closing => "closing",
        SessionState::Archived => "archived",
        SessionState::Lost => "lost",
        SessionState::Error => "error",
        SessionState::DestroyedWithDataLoss => "data lost",
    }
}

fn quota_age(now: u64, refreshed: u64) -> String {
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
    if age > 15 * 60 {
        format!("stale · {value}{unit}")
    } else {
        format!("{value}{unit} ago")
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::hel_config::{
        CONFIG_VERSION, ContainerTemplate, HarnessProfile, ProjectBundle, ProjectRepository,
    };
    use crate::hel_state::{CheckpointMetadata, STATE_VERSION};

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn config() -> HelConfig {
        HelConfig {
            version: CONFIG_VERSION,
            profiles: BTreeMap::from([
                (
                    "claude-1".into(),
                    HarnessProfile {
                        model: None,
                        reasoning_effort: None,
                        kind: HarnessKind::Claude,
                        home: PathBuf::from("/profiles/claude"),
                        executable: None,
                        environment: BTreeMap::new(),
                    },
                ),
                (
                    "codex-1".into(),
                    HarnessProfile {
                        model: None,
                        reasoning_effort: None,
                        kind: HarnessKind::Codex,
                        home: PathBuf::from("/profiles/codex"),
                        executable: None,
                        environment: BTreeMap::new(),
                    },
                ),
                (
                    "codex-2".into(),
                    HarnessProfile {
                        model: None,
                        reasoning_effort: None,
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
                        github: "BrokkAi/hel".into(),
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
            target_template_id: "podman".into(),
            additional_mounts: vec![],
            state: SessionState::Archived,
            target: None,
            native_session_id: Some("native-1".into()),
            created_at: "2026-08-09T00:00:00Z".into(),
            updated_at: "2026-08-09T01:00:00Z".into(),
            last_error: None,
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
    fn archived_section_partitions_sessions_without_losing_selection_order() {
        let mut running = archived_session();
        running.id = "session-0".into();
        running.state = SessionState::Running;
        let archived = archived_session();
        let state = HelState {
            version: STATE_VERSION,
            sessions: BTreeMap::from([
                (running.id.clone(), running),
                (archived.id.clone(), archived),
            ]),
            mount_history: BTreeMap::new(),
        };
        let (active, archived) = partition_sessions(state.sessions.values());
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
            ["session-1"]
        );

        let mut dashboard = DashboardState::new(config(), state, BTreeMap::new());
        dashboard.handle_key(key(KeyCode::Down));
        assert_eq!(
            dashboard
                .selected_session()
                .map(|session| session.id.as_str()),
            Some("session-1")
        );
    }

    #[test]
    fn new_session_wizard_returns_all_three_choices() {
        let mut dashboard = DashboardState::new(config(), HelState::default(), BTreeMap::new());
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Char('n'))),
            DashboardAction::None
        );
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
                target_template_id: "podman".into(),
                additional_mounts: vec![],
            }
        );
    }

    #[test]
    fn new_session_mount_wizard_adds_mount_and_preserves_typed_source() {
        let mut dashboard = DashboardState::new(config(), HelState::default(), BTreeMap::new());
        dashboard.handle_key(key(KeyCode::Char('n')));
        dashboard.handle_key(key(KeyCode::Down));
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Enter));
        for character in "/opt/cache".chars() {
            dashboard.handle_key(key(KeyCode::Char(character)));
        }
        dashboard.apply_mount_source_completions("/opt/ca", vec!["/opt/cache/".into()]);
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Enter));

        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::CreateSession {
                profile_id: "codex-1".into(),
                bundle_id: "hel".into(),
                target_template_id: "podman".into(),
                additional_mounts: vec![AdditionalMount {
                    source: "/opt/cache".into(),
                    destination: "/mnt/cache".into(),
                }],
            }
        );
    }

    #[test]
    fn resume_can_convert_to_another_harness() {
        let mut dashboard = dashboard_with_session(archived_session());
        dashboard.handle_key(key(KeyCode::Char('r')));
        dashboard.handle_key(key(KeyCode::Enter));
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::ResumeSession {
                session_id: "session-1".into(),
                profile_id: "claude-1".into(),
                target_template_id: "podman".into(),
            }
        );
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
    fn close_and_force_destroy_have_separate_confirmation_strengths() {
        let mut dashboard = dashboard_with_session(archived_session());
        dashboard.handle_key(key(KeyCode::Char('c')));
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Char('y'))),
            DashboardAction::Close {
                session_id: "session-1".into()
            }
        );

        dashboard.handle_key(key(KeyCode::Char('x')));
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
        assert!(rendered.contains("Welcome to Hel."));
        assert!(rendered.contains("Hel needs a little fuel."));
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Char('e'))),
            DashboardAction::OpenConfig
        );
    }

    #[test]
    fn quota_render_includes_errors_and_stale_refresh_age() {
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
        assert!(rendered.contains("stale"));
    }
}
