//! Full-screen dashboard and session picker for Hel.
//!
//! This module deliberately has no provisioning or persistence side effects.
//! Input is reduced to [`DashboardAction`] values for the controller to run.

use std::collections::BTreeMap;
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NewWizard {
    step: WizardStep,
    profile: usize,
    bundle: usize,
    target: usize,
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

/// Stateful, renderable projection of controller configuration and state.
pub struct DashboardState {
    config: HelConfig,
    state: HelState,
    quotas: BTreeMap<String, ProfileQuota>,
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
        self.quotas = quotas;
        self.clamp_selections();
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
        let len = match wizard.step {
            WizardStep::Profile => self.config.profiles.len(),
            WizardStep::Bundle => self.config.bundles.len(),
            WizardStep::Target => self.config.targets.len(),
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
                let action = DashboardAction::CreateSession {
                    profile_id: nth_key(&self.config.profiles, wizard.profile),
                    bundle_id: nth_key(&self.config.bundles, wizard.bundle),
                    target_template_id: nth_key(&self.config.targets, wizard.target),
                };
                self.cancel_modal();
                action
            }
        }
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
        self.state.sessions.values().nth(self.session_index)
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

impl NewWizard {
    fn active_index_mut(&mut self) -> &mut usize {
        match self.step {
            WizardStep::Profile => &mut self.profile,
            WizardStep::Bundle => &mut self.bundle,
            WizardStep::Target => &mut self.target,
        }
    }
}

impl ResumeWizard {
    fn active_index_mut(&mut self) -> &mut usize {
        match self.step {
            WizardStep::Profile => &mut self.profile,
            WizardStep::Target => &mut self.target,
            WizardStep::Bundle => unreachable!("resume does not select a bundle"),
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
    let rows = dashboard.state.sessions.values().map(|session| {
        Row::new([
            Cell::from(session.title.clone()),
            Cell::from(format!(
                "{} / {}",
                harness_label(session.harness_kind),
                session.last_profile
            )),
            Cell::from(session.target_template_id.clone()),
            Cell::from(state_label(session.state)),
            Cell::from(
                session
                    .checkpoint
                    .as_ref()
                    .map(|checkpoint| checkpoint.created_at.clone())
                    .unwrap_or_else(|| "never".into()),
            ),
        ])
    });
    let title = if dashboard.focus == Focus::Sessions {
        " Sessions [focused] "
    } else {
        " Sessions "
    };
    let table = Table::new(
        rows,
        [
            Constraint::Percentage(28),
            Constraint::Percentage(23),
            Constraint::Percentage(18),
            Constraint::Percentage(14),
            Constraint::Percentage(17),
        ],
    )
    .header(
        Row::new([
            "Title",
            "Harness / profile",
            "Target",
            "State",
            "Checkpoint",
        ])
        .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .row_highlight_style(Style::default().bg(Color::DarkGray).fg(Color::White))
    .highlight_symbol("› ")
    .block(Block::default().borders(Borders::ALL).title(title));
    let mut state = TableState::default()
        .with_selected((!dashboard.state.sessions.is_empty()).then_some(dashboard.session_index));
    frame.render_stateful_widget(table, area, &mut state);
}

fn render_quotas(frame: &mut Frame, area: Rect, dashboard: &mut DashboardState) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let rows = dashboard.config.profiles.iter().map(|(id, profile)| {
        let (usage, refreshed) = match dashboard.quotas.get(id) {
            Some(quota) => (
                quota.compact(),
                quota_age(now, quota.refreshed_at_epoch_seconds),
            ),
            None => ("not refreshed".into(), "never".into()),
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
    let (title, choices, selected) = match wizard.step {
        WizardStep::Profile => (
            " New session · 1/3 profile ",
            dashboard
                .config
                .profiles
                .iter()
                .map(|(id, profile)| {
                    format!(
                        "{id}  {}  [{}]",
                        harness_label(profile.kind),
                        profile.unrestricted_mode()
                    )
                })
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
                .map(|(id, harness)| format!("{id}  {}", harness_label(harness)))
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
            },
            BTreeMap::new(),
        )
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
            DashboardAction::CreateSession {
                profile_id: "codex-1".into(),
                bundle_id: "hel".into(),
                target_template_id: "podman".into(),
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
