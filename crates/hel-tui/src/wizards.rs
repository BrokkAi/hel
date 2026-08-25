//! New-session and resume wizards, including their mount and review steps.

use std::collections::BTreeMap;

use crossterm::event::KeyCode;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use sha2::{Digest, Sha256};

use hel::hel_config::{HelConfig, TargetTemplate, is_bare_project_target, mount_history_host};
use hel::hel_state::{
    HelState, SessionRecord, SessionResourceAllocation, SessionState, allocation_cpus,
    allocation_memory,
};
use hel::hel_targets::{AdditionalMount, default_mount_destination, path_completion};

use crate::widgets::{action_buttons, centered_rect, format_resource_bytes};
use crate::{DashboardAction, DashboardState, Mode, cycle_control, move_index, nth_key};

const BASELINE_CPUS: u64 = 8;
const BASELINE_MEMORY_BYTES: u64 = 32 * 1024 * 1024 * 1024;
const FLOOR_CPUS: u64 = 2;
const FLOOR_MEMORY_BYTES: u64 = 8 * 1024 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WizardStep {
    Profile,
    Target,
    Bundle,
    ProjectDirectory,
    Review,
    Mounts,
    NewBundle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WizardFocus {
    Content,
    Cancel,
    Back,
    Next,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NewWizard {
    pub(crate) step: WizardStep,
    pub(crate) focus: WizardFocus,
    pub(crate) profile: usize,
    bundle: usize,
    pub(crate) target: usize,
    pub(crate) mounts: MountWizard,
    review_focus: ReviewFocus,
    pub(crate) new_bundle_source: String,
    pub(crate) project_directory: String,
    pub(crate) project_directory_error: Option<String>,
    project_history: Vec<std::path::PathBuf>,
    project_history_index: usize,
    pub(crate) resource_allocation: Option<SessionResourceAllocation>,
    aws_options: BTreeMap<String, Vec<SessionResourceAllocation>>,
    pub(crate) sizing_error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MountFocus {
    Source,
    Destination,
    ReadOnly,
    Cancel,
    Back,
    Add,
}

/// Tab order for the mount editor, shared by the new-session and resume paths.
const MOUNT_FOCUS_ORDER: [MountFocus; 6] = [
    MountFocus::Source,
    MountFocus::Destination,
    MountFocus::ReadOnly,
    MountFocus::Cancel,
    MountFocus::Back,
    MountFocus::Add,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReviewFocus {
    Attachments,
    Cancel,
    Back,
    Add,
    Submit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MountWizard {
    pub(crate) source: String,
    pub(crate) destination: String,
    pub(crate) focus: MountFocus,
    pub(crate) read_only: bool,
    pub(crate) mounts: Vec<AdditionalMount>,
    pub(crate) history: Vec<std::path::PathBuf>,
    history_index: usize,
    completion_cache: BTreeMap<String, Vec<String>>,
    completion_candidates: Vec<String>,
    completion_index: usize,
    /// Sources the target's host reported as unable to hold Podman's overlay,
    /// keyed by the typed source and holding the `filesystem (reason)` label.
    forced_sources: BTreeMap<String, String>,
    pub(crate) error: Option<String>,
    editing_mount: Option<usize>,
}

impl MountWizard {
    pub(crate) fn new(history: Vec<std::path::PathBuf>) -> Self {
        Self {
            source: String::new(),
            destination: String::new(),
            focus: MountFocus::Source,
            read_only: false,
            mounts: Vec::new(),
            history,
            history_index: 0,
            completion_cache: BTreeMap::new(),
            completion_candidates: Vec::new(),
            completion_index: 0,
            forced_sources: BTreeMap::new(),
            error: None,
            editing_mount: None,
        }
    }

    fn with_mounts(history: Vec<std::path::PathBuf>, mounts: Vec<AdditionalMount>) -> Self {
        let mut wizard = Self::new(history);
        wizard.mounts = mounts;
        wizard
    }

    /// Why the source under edit can only be attached read-only, if it can.
    pub(crate) fn forced_read_only(&self) -> Option<&str> {
        self.forced_sources
            .get(self.source.trim())
            .map(String::as_str)
    }

    /// Space and Enter toggle the checkbox, except where the host's filesystem
    /// has already settled the answer.
    fn toggle_read_only(&mut self) {
        if self.forced_read_only().is_some() {
            return;
        }
        self.read_only = !self.read_only;
    }

    fn add_validated_mount(&mut self) {
        let mount = AdditionalMount {
            source: self.source.clone().into(),
            destination: self.destination.clone().into(),
            read_only: self.read_only,
        };
        if let Some(index) = self.editing_mount.take() {
            self.mounts[index] = mount;
        } else {
            self.mounts.push(mount);
        }
        self.source.clear();
        self.destination.clear();
        self.read_only = false;
        self.focus = MountFocus::Source;
        self.completion_candidates.clear();
        self.error = None;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResumeWizard {
    pub(crate) session_id: String,
    pub(crate) step: WizardStep,
    pub(crate) focus: WizardFocus,
    pub(crate) profile: usize,
    pub(crate) target: usize,
    pub(crate) mounts: MountWizard,
    review_focus: ReviewFocus,
    pub(crate) resource_allocation: Option<SessionResourceAllocation>,
    aws_options: BTreeMap<String, Vec<SessionResourceAllocation>>,
    pub(crate) sizing_error: Option<String>,
    pub(crate) discard_queue: bool,
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
    mounts.read_only = false;
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
    let mount = mounts.mounts[index].clone();
    mounts.source = mount.source.to_string_lossy().into_owned();
    mounts.destination = mount.destination.to_string_lossy().into_owned();
    mounts.read_only = mount.read_only || mounts.forced_read_only().is_some();
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
        read_only: mounts.read_only,
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

pub(crate) fn clamp_resources(
    cpus: u64,
    memory_bytes: u64,
    limits: Option<(u64, u64)>,
) -> (u64, u64) {
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

#[derive(Debug, Clone, Copy)]
pub(crate) struct PickerNavigation {
    pub(crate) focus: WizardFocus,
    pub(crate) has_back: bool,
}

/// One picker row. A disabled row stays in the list so row numbers keep
/// matching the underlying map order; it is greyed out and refuses Enter.
#[derive(Debug, Clone)]
pub(crate) struct PickerChoice {
    pub(crate) text: String,
    pub(crate) disabled: bool,
}

impl From<String> for PickerChoice {
    fn from(text: String) -> Self {
        Self {
            text,
            disabled: false,
        }
    }
}

pub(crate) fn render_picker(
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

pub(crate) fn wizard_buttons(
    focus: WizardFocus,
    has_back: bool,
    next_label: &str,
) -> Line<'static> {
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

pub(crate) fn render_new_wizard(
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
                project_label: if raw_project {
                    "Project directory"
                } else {
                    "Project"
                },
                project: if raw_project {
                    wizard.project_directory.trim()
                } else {
                    bundle_id.as_deref().expect("bundle selected")
                },
                project_note: "",
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
    pub(crate) profile_id: &'a str,
    pub(crate) project_label: &'a str,
    pub(crate) project: &'a str,
    pub(crate) project_note: &'a str,
    pub(crate) target_id: &'a str,
    pub(crate) allocation: Option<&'a SessionResourceAllocation>,
    pub(crate) mounts: &'a MountWizard,
    pub(crate) focus: ReviewFocus,
    pub(crate) title: &'a str,
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
        project_label,
        project,
        project_note,
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
        Line::raw(format!("{project_label}: {project}{project_note}")),
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
    // Not a permission mode Hel picked, but the state the session runs in:
    // every command is approved, and on raw localhost that is this machine.
    if matches!(target, TargetTemplate::LocalBare)
        && let Some(kind) = dashboard
            .config
            .profiles
            .get(profile_id)
            .map(|profile| profile.kind)
        && let Some(mechanism) = kind.bare_target_auto_approval()
    {
        lines.push(Line::styled(
            format!(
                "⚠ DANGER: {} approves every command through {mechanism}. On raw localhost it can \
                 change this machine without asking.",
                kind.display_name()
            ),
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
                            "{}{} → {}{}",
                            if selected { "› " } else { "  " },
                            mount.source.display(),
                            mount.destination.display(),
                            read_only_marker(mount.read_only)
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

/// Suffix that marks an attached directory as read-only in a list row.
pub(crate) fn read_only_marker(read_only: bool) -> &'static str {
    if read_only { " · ro" } else { "" }
}

/// The read-only checkbox, locked when the host's filesystem has settled it.
fn read_only_line(mounts: &MountWizard) -> Line<'static> {
    let marker = if mounts.focus == MountFocus::ReadOnly {
        "› "
    } else {
        "  "
    };
    let box_text = if mounts.read_only { "[x]" } else { "[ ]" };
    match mounts.forced_read_only() {
        Some(reason) => Line::styled(
            format!("{marker}Read-only: [x] locked · {reason}"),
            Style::default().fg(Color::Yellow),
        ),
        None => Line::styled(
            format!("{marker}Read-only: {box_text} (Space toggles)"),
            if mounts.focus == MountFocus::ReadOnly {
                Style::default().bg(Color::DarkGray).fg(Color::White)
            } else {
                Style::default()
            },
        ),
    }
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
            "Podman uses :O copy-on-write overlays; read-only skips the overlay."
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
        read_only_line(mounts),
    ];
    if !mounts.mounts.is_empty() {
        lines.push(Line::raw(""));
        lines.push(Line::raw("Already attached:"));
        lines.extend(mounts.mounts.iter().map(|mount| {
            Line::raw(format!(
                "  {} → {}{}",
                mount.source.display(),
                mount.destination.display(),
                read_only_marker(mount.read_only)
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
            "Tab completes · Shift-Tab moves · Space toggles read-only · Enter continues/adds",
            Style::default().fg(Color::DarkGray),
        ),
        action_buttons(&[
            ("Cancel", mounts.focus == MountFocus::Cancel),
            ("Back", mounts.focus == MountFocus::Back),
            ("Add directory", mounts.focus == MountFocus::Add),
        ]),
    ]);
    // One row taller than before the read-only checkbox joined the editor.
    let popup = centered_rect(84, (lines.len() as u16 + 2).clamp(13, 25), area);
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(lines)
            .block(Block::default().borders(Borders::ALL).title(title))
            .wrap(Wrap { trim: false }),
        popup,
    );
}

pub(crate) fn render_resume_wizard(
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
        let session = dashboard.state.sessions.get(&wizard.session_id);
        let bundle_id = session
            .map(|session| session.bundle_id.as_str())
            .unwrap_or("unknown");
        let target_id = nth_key(&dashboard.config.targets, wizard.target);
        let reused_project_directory = session
            .filter(|session| {
                hel::hel_controller::resume_compatibility(session, &dashboard.config, &target_id)
                    == Ok(hel::hel_controller::ResumePlan::InPlace)
            })
            .and_then(|session| session.project_directory.as_deref())
            .map(|directory| directory.display().to_string());
        let (project_label, project, project_note) =
            if let Some(directory) = reused_project_directory.as_deref() {
                ("Project directory", directory, " (reused)")
            } else {
                ("Project", bundle_id, "")
            };
        render_review_wizard(
            frame,
            area,
            dashboard,
            ReviewWizardView {
                profile_id,
                project_label,
                project,
                project_note,
                target_id: &target_id,
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

fn raw_project_context_id(project_directory: &str) -> String {
    let digest = Sha256::digest(project_directory.trim().as_bytes());
    let suffix = digest[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("remote-project-{suffix}")
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

impl DashboardState {
    pub(crate) fn handle_new_key(
        &mut self,
        code: KeyCode,
        mut wizard: NewWizard,
    ) -> DashboardAction {
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
                ReviewFocus::Submit => return self.preflight_create_session_action(&wizard),
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
                    &MOUNT_FOCUS_ORDER,
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
                    MountFocus::ReadOnly
                    | MountFocus::Cancel
                    | MountFocus::Back
                    | MountFocus::Add => {}
                }
                wizard.mounts.error = None;
                self.mode = Mode::New(wizard);
                DashboardAction::None
            }
            KeyCode::Char(' ') if wizard.mounts.focus == MountFocus::ReadOnly => {
                wizard.mounts.toggle_read_only();
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
                MountFocus::ReadOnly => {
                    wizard.mounts.toggle_read_only();
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
                    MountFocus::ReadOnly
                    | MountFocus::Cancel
                    | MountFocus::Back
                    | MountFocus::Add => {}
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
        let action = self.create_session_action_without_closing(wizard);
        self.cancel_modal();
        action
    }

    fn preflight_create_session_action(&mut self, wizard: &NewWizard) -> DashboardAction {
        if wizard.mounts.mounts.is_empty() {
            return self.create_session_action(wizard);
        }
        let launch = self.create_session_action_without_closing(wizard);
        DashboardAction::ValidateSessionMounts {
            target_template_id: nth_key(&self.config.targets, wizard.target),
            mounts: wizard.mounts.mounts.clone(),
            launch: Box::new(launch),
        }
    }

    fn create_session_action_without_closing(&self, wizard: &NewWizard) -> DashboardAction {
        let target_template_id = nth_key(&self.config.targets, wizard.target);
        let raw_project = is_bare_project_target(&self.config.targets[&target_template_id]);
        DashboardAction::CreateSession {
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
        }
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

    /// Apply the host's answer about one mount source. A source whose
    /// filesystem cannot hold the overlay is remembered, so the entry is
    /// attached read-only and the editor locks the checkbox from then on.
    pub fn apply_mount_source_validation(
        &mut self,
        source: &str,
        result: Result<Option<String>, String>,
    ) {
        let (mounts, review_focus, step) = match &mut self.mode {
            Mode::New(wizard)
                if wizard.step == WizardStep::Mounts && wizard.mounts.source == source =>
            {
                (
                    &mut wizard.mounts,
                    &mut wizard.review_focus,
                    &mut wizard.step,
                )
            }
            Mode::Resume(wizard)
                if wizard.step == WizardStep::Mounts && wizard.mounts.source == source =>
            {
                (
                    &mut wizard.mounts,
                    &mut wizard.review_focus,
                    &mut wizard.step,
                )
            }
            _ => return,
        };
        match result {
            Ok(forced) => {
                if let Some(reason) = forced {
                    mounts
                        .forced_sources
                        .insert(source.trim().to_owned(), reason);
                    mounts.read_only = true;
                }
                mounts.add_validated_mount();
                mounts.history_index = mounts.mounts.len().saturating_sub(1);
                *review_focus = ReviewFocus::Attachments;
                *step = WizardStep::Review;
            }
            Err(error) => {
                mounts.error = Some(error);
                mounts.focus = MountFocus::Source;
            }
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

    pub(crate) fn handle_resume_key(
        &mut self,
        code: KeyCode,
        mut wizard: ResumeWizard,
    ) -> DashboardAction {
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
                    return self.preflight_resume_session_action(wizard, profile_id);
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
                    &MOUNT_FOCUS_ORDER,
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
                    MountFocus::ReadOnly
                    | MountFocus::Cancel
                    | MountFocus::Back
                    | MountFocus::Add => {}
                }
                wizard.mounts.error = None;
                self.mode = Mode::Resume(wizard);
                DashboardAction::None
            }
            KeyCode::Char(' ') if wizard.mounts.focus == MountFocus::ReadOnly => {
                wizard.mounts.toggle_read_only();
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
                MountFocus::ReadOnly => {
                    wizard.mounts.toggle_read_only();
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
                    MountFocus::ReadOnly
                    | MountFocus::Cancel
                    | MountFocus::Back
                    | MountFocus::Add => {}
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

    fn preflight_resume_session_action(
        &mut self,
        wizard: ResumeWizard,
        profile_id: String,
    ) -> DashboardAction {
        let target_template_id = nth_key(&self.config.targets, wizard.target);
        let mounts = wizard.mounts.mounts.clone();
        let launch = DashboardAction::ResumeSession {
            session_id: wizard.session_id.clone(),
            profile_id,
            target_template_id: target_template_id.clone(),
            additional_mounts: mounts.clone(),
            resource_allocation: wizard.resource_allocation.clone(),
            discard_queue: wizard.discard_queue,
        };
        self.mode = Mode::Resume(wizard);
        let preflight = DashboardAction::PreflightResumeRepositories {
            launch: Box::new(launch),
        };
        if mounts.is_empty() {
            preflight
        } else {
            DashboardAction::ValidateSessionMounts {
                target_template_id,
                mounts,
                launch: Box::new(preflight),
            }
        }
    }

    pub fn apply_session_mount_preflight_failure(&mut self, source: &str, error: String) {
        match &mut self.mode {
            Mode::New(wizard) => {
                if let Some(index) = wizard
                    .mounts
                    .mounts
                    .iter()
                    .position(|mount| mount.source == std::path::Path::new(source))
                {
                    wizard.mounts.history_index = index;
                    prepare_selected_mount_editor(&mut wizard.step, &mut wizard.mounts);
                }
                wizard.mounts.error = Some(error);
            }
            Mode::Resume(wizard) => {
                if let Some(index) = wizard
                    .mounts
                    .mounts
                    .iter()
                    .position(|mount| mount.source == std::path::Path::new(source))
                {
                    wizard.mounts.history_index = index;
                    prepare_selected_mount_editor(&mut wizard.step, &mut wizard.mounts);
                }
                wizard.mounts.error = Some(error);
            }
            _ => {}
        }
    }

    pub fn finish_session_mount_preflight(&mut self) {
        self.cancel_modal();
    }

    pub(crate) fn begin_new(&mut self) -> DashboardAction {
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

    pub(crate) fn begin_resume(&mut self) -> DashboardAction {
        let Some(session_id) = self.selected_session().map(|session| session.id.clone()) else {
            return DashboardAction::None;
        };
        self.begin_resume_for(&session_id)
    }

    /// Open the resume wizard for one session by id. The dashboard reaches
    /// this for a failed but checkpointed session; the resume dialog reaches it
    /// for a stopped one.
    pub(crate) fn begin_resume_for(&mut self, session_id: &str) -> DashboardAction {
        let Some(session) = self.state.sessions.get(session_id).cloned() else {
            return DashboardAction::None;
        };
        let session = &session;
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
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use crossterm::event::KeyCode;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use hel::hel_config::{HarnessKind, HarnessProfile, SshConnection, TargetTemplate};
    use hel::hel_state::{HelState, STATE_VERSION, SessionResourceAllocation};
    use hel::hel_targets::AdditionalMount;

    use super::*;
    use crate::test_support::*;

    use crate::render::render;
    use crate::{DashboardAction, DashboardState, Mode, nth_key};

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
                sessions: BTreeMap::from([("session-1".into(), stopped_session())]),
                mount_history: BTreeMap::new(),
            },
            BTreeMap::new(),
        );
        assert_eq!(
            open_resume_wizard(&mut dashboard),
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
        assert!(rendered.contains("Project directory: /srv/project"));
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

    /// The review pane names the harness and the mechanism that approves
    /// everything, so the two ways of arriving there are both visible.
    #[test]
    fn raw_localhost_names_the_blanket_approval_mechanism_per_harness() {
        let review_text = |kind: HarnessKind| {
            let mut config = config();
            config.profiles = BTreeMap::from([(
                "profile".into(),
                HarnessProfile {
                    context_window_bytes: None,
                    kind,
                    home: PathBuf::from("/profiles/harness"),
                    executable: None,
                    environment: BTreeMap::new(),
                },
            )]);
            config.targets = BTreeMap::from([("localhost".into(), TargetTemplate::LocalBare)]);
            let mut state = HelState::default();
            state.remember_project_directory("local", std::path::Path::new("/home/me/project"));
            let mut dashboard = DashboardState::new(config, state, BTreeMap::new());
            dashboard.handle_key(ctrl_key('n'));
            let mut terminal = Terminal::new(TestBackend::new(180, 32)).unwrap();
            terminal
                .draw(|frame| render(frame, &mut dashboard))
                .unwrap();
            terminal
                .backend()
                .buffer()
                .content()
                .iter()
                .map(|cell| cell.symbol())
                .collect::<String>()
        };

        let grok = review_text(HarnessKind::Grok);
        assert!(grok.contains("DANGER"), "{grok}");
        assert!(grok.contains("Grok Build"), "{grok}");
        assert!(grok.contains("--always-approve launch flag"), "{grok}");

        let kimi = review_text(HarnessKind::Kimi);
        assert!(kimi.contains("DANGER"), "{kimi}");
        assert!(kimi.contains("default auto mode"), "{kimi}");

        // Codex and Claude Code ask on a bare target, so there is no warning.
        for kind in [HarnessKind::Codex, HarnessKind::Claude] {
            let quiet = review_text(kind);
            assert!(!quiet.contains("DANGER"), "{kind:?}: {quiet}");
        }
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
        config.targets = BTreeMap::from([("localhost".into(), TargetTemplate::LocalBare)]);
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
                target_template_id: "localhost".into(),
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
                target_template_id: "localhost".into(),
                additional_mounts: Vec::new(),
                allow_dirty_local: false,
                resource_allocation: None,
            }
        );
    }

    #[test]
    fn new_session_bundles_are_ordered_by_latest_session_creation() {
        let mut config = config();
        let bundle = config.bundles["hel"].clone();
        config.bundles.insert("alpha-unused".into(), bundle.clone());
        config.bundles.insert("zebra-recent".into(), bundle);

        let mut older = stopped_session();
        older.id = "older".into();
        older.created_at = "2026-08-10T12:00:00Z".into();
        let mut recent = stopped_session();
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
        let mut recent = stopped_session();
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

    /// Walk the new-session wizard as far as an open mount editor with the
    /// source already typed and the destination filled in.
    fn dashboard_at_mount_editor(source: &str) -> DashboardState {
        let mut dashboard = DashboardState::new(config(), HelState::default(), BTreeMap::new());
        dashboard.handle_key(ctrl_key('n'));
        dashboard.handle_key(key(KeyCode::Down));
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::BackTab));
        dashboard.handle_key(key(KeyCode::Enter));
        for character in source.chars() {
            dashboard.handle_key(key(KeyCode::Char(character)));
        }
        // Enter on the source fills the default destination and moves on.
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard
    }

    fn wizard_mounts(dashboard: &DashboardState) -> &MountWizard {
        let Mode::New(wizard) = &dashboard.mode else {
            panic!("expected the new-session wizard");
        };
        &wizard.mounts
    }

    #[test]
    fn the_read_only_checkbox_rides_the_mount_into_the_created_session() {
        let mut dashboard = dashboard_at_mount_editor("/opt/cache");

        dashboard.handle_key(key(KeyCode::Tab));
        assert_eq!(wizard_mounts(&dashboard).focus, MountFocus::ReadOnly);
        dashboard.handle_key(key(KeyCode::Char(' ')));
        assert!(wizard_mounts(&dashboard).read_only);

        // Tab past Cancel and Back to the add button, then commit.
        for _ in 0..3 {
            dashboard.handle_key(key(KeyCode::Tab));
        }
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::ValidateMountSource {
                target_template_id: "podman".into(),
                source: "/opt/cache".into(),
            }
        );
        dashboard.apply_mount_source_validation("/opt/cache", Ok(None));

        assert_eq!(
            wizard_mounts(&dashboard).mounts,
            vec![AdditionalMount {
                source: "/opt/cache".into(),
                destination: "/mnt/cache".into(),
                read_only: true,
            }]
        );
        // The next entry starts unchecked again.
        assert!(!wizard_mounts(&dashboard).read_only);
    }

    #[test]
    fn a_source_the_host_forces_read_only_cannot_be_unchecked() {
        let mut dashboard = dashboard_at_mount_editor("/nfs/share");

        assert!(!wizard_mounts(&dashboard).read_only);
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.apply_mount_source_validation(
            "/nfs/share",
            Ok(Some("nfs (network filesystem)".into())),
        );
        assert_eq!(
            wizard_mounts(&dashboard).mounts,
            vec![AdditionalMount {
                source: "/nfs/share".into(),
                destination: "/mnt/share".into(),
                read_only: true,
            }]
        );

        // Reopen the entry: the checkbox is checked, locked, and named.
        dashboard.handle_key(key(KeyCode::Enter));
        assert!(wizard_mounts(&dashboard).read_only);
        assert_eq!(
            wizard_mounts(&dashboard).forced_read_only(),
            Some("nfs (network filesystem)")
        );
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Tab));
        assert_eq!(wizard_mounts(&dashboard).focus, MountFocus::ReadOnly);
        dashboard.handle_key(key(KeyCode::Char(' ')));
        dashboard.handle_key(key(KeyCode::Enter));
        assert!(
            wizard_mounts(&dashboard).read_only,
            "a forced source must stay read-only"
        );

        let mut terminal = Terminal::new(TestBackend::new(120, 30)).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw the mount editor");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("Read-only: [x] locked · nfs (network filesystem)"));
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
        dashboard.apply_mount_source_validation("/opt/cache", Ok(None));
        dashboard.handle_key(key(KeyCode::BackTab));

        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::ValidateSessionMounts {
                target_template_id: "podman".into(),
                mounts: vec![AdditionalMount {
                    source: "/opt/cache".into(),
                    destination: "/mnt/cache".into(),
                    read_only: false,
                }],
                launch: Box::new(DashboardAction::CreateSession {
                    profile_id: "codex-1".into(),
                    bundle_id: "hel".into(),
                    project_directory: None,
                    target_template_id: "podman".into(),
                    additional_mounts: vec![AdditionalMount {
                        source: "/opt/cache".into(),
                        destination: "/mnt/cache".into(),
                        read_only: false,
                    }],
                    allow_dirty_local: false,
                    resource_allocation: Some(SessionResourceAllocation::Container {
                        cpus: BASELINE_CPUS,
                        memory_bytes: BASELINE_MEMORY_BYTES,
                    }),
                }),
            }
        );
    }

    #[test]
    fn failed_submit_preflight_reopens_the_invalid_mount() {
        let mut dashboard = dashboard_at_mount_editor("/opt/cache");
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.apply_mount_source_validation("/opt/cache", Ok(None));
        dashboard.handle_key(key(KeyCode::BackTab));
        assert!(matches!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::ValidateSessionMounts { .. }
        ));

        dashboard.apply_session_mount_preflight_failure(
            "/opt/cache",
            "source path /opt/cache does not exist or is not a directory".into(),
        );

        let Mode::New(wizard) = &dashboard.mode else {
            panic!("preflight failure should keep the new-session dialog open");
        };
        assert_eq!(wizard.step, WizardStep::Mounts);
        assert_eq!(wizard.mounts.source, "/opt/cache");
        assert_eq!(
            wizard.mounts.error.as_deref(),
            Some("source path /opt/cache does not exist or is not a directory")
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

        let mut dashboard = dashboard_with_session(stopped_session());
        open_resume_wizard(&mut dashboard);
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
        let mut dashboard = dashboard_with_session(stopped_session());
        dashboard.set_deployment_capacity_targets(vec![test_capacity_target()]);
        open_resume_wizard(&mut dashboard);
        dashboard.handle_key(key(KeyCode::Up));
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Enter));
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::PreflightResumeRepositories {
                launch: Box::new(DashboardAction::ResumeSession {
                    session_id: "session-1".into(),
                    profile_id: "claude-1".into(),
                    target_template_id: "podman".into(),
                    additional_mounts: vec![],
                    resource_allocation: Some(SessionResourceAllocation::Container {
                        cpus: BASELINE_CPUS,
                        memory_bytes: BASELINE_MEMORY_BYTES,
                    }),
                    discard_queue: false,
                }),
            }
        );
    }

    #[test]
    fn resume_defaults_to_the_session_profile() {
        let mut dashboard = dashboard_with_session(stopped_session());
        open_resume_wizard(&mut dashboard);

        let Mode::Resume(wizard) = &dashboard.mode else {
            panic!("expected resume wizard");
        };
        let profiles = dashboard.compatible_profiles(&wizard.session_id);
        assert_eq!(profiles[wizard.profile].0, "codex-1");
    }

    #[test]
    fn resume_defaults_to_the_previously_used_target() {
        let mut dashboard = dashboard_with_session(stopped_session());
        let target = dashboard.config.targets["podman"].clone();
        dashboard.config.targets.insert("alternate".into(), target);

        open_resume_wizard(&mut dashboard);

        let Mode::Resume(wizard) = &dashboard.mode else {
            panic!("expected resume wizard");
        };
        assert_eq!(nth_key(&dashboard.config.targets, wizard.target), "podman");
    }

    #[test]
    fn resume_refuses_a_target_the_session_cannot_use_and_says_why() {
        let mut dashboard = dashboard_with_session(stopped_session());
        dashboard
            .config
            .targets
            .insert("bare".into(), TargetTemplate::LocalBare);

        open_resume_wizard(&mut dashboard);
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
        assert!(notice.contains("came from GitHub"), "{notice}");
    }

    #[test]
    fn resume_marks_an_unusable_target_row_as_disabled() {
        let mut dashboard = dashboard_with_session(stopped_session());
        dashboard
            .config
            .targets
            .insert("bare".into(), TargetTemplate::LocalBare);
        open_resume_wizard(&mut dashboard);
        dashboard.handle_key(key(KeyCode::Enter));

        let mut terminal =
            ratatui::Terminal::new(ratatui::backend::TestBackend::new(120, 30)).unwrap();
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .unwrap();
        let rendered = buffer_lines(terminal.backend().buffer()).join("\n");

        assert!(rendered.contains("came from GitHub"), "{rendered}");
    }

    fn resume_wizard(dashboard: &DashboardState) -> &ResumeWizard {
        let Mode::Resume(wizard) = &dashboard.mode else {
            panic!("expected resume wizard");
        };
        wizard
    }

    #[test]
    fn resume_dialog_attaches_an_additional_resource() {
        let mut dashboard = dashboard_with_session(stopped_session());
        open_resume_wizard(&mut dashboard);
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
        dashboard.apply_mount_source_validation("/opt/cache", Ok(None));
        dashboard.handle_key(key(KeyCode::BackTab));

        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::ValidateSessionMounts {
                target_template_id: "podman".into(),
                mounts: vec![AdditionalMount {
                    source: "/opt/cache".into(),
                    destination: "/mnt/cache".into(),
                    read_only: false,
                }],
                launch: Box::new(DashboardAction::PreflightResumeRepositories {
                    launch: Box::new(DashboardAction::ResumeSession {
                        session_id: "session-1".into(),
                        profile_id: "codex-1".into(),
                        target_template_id: "podman".into(),
                        additional_mounts: vec![AdditionalMount {
                            source: "/opt/cache".into(),
                            destination: "/mnt/cache".into(),
                            read_only: false,
                        }],
                        resource_allocation: Some(SessionResourceAllocation::Container {
                            cpus: BASELINE_CPUS,
                            memory_bytes: BASELINE_MEMORY_BYTES,
                        }),
                        discard_queue: false,
                    }),
                }),
            }
        );
    }

    #[test]
    fn resume_dialog_can_remove_a_previous_resource() {
        let mut session = stopped_session();
        session.additional_mounts = vec![AdditionalMount {
            source: "/opt/old-cache".into(),
            destination: "/mnt/old-cache".into(),
            read_only: false,
        }];
        let mut dashboard = dashboard_with_session(session);
        open_resume_wizard(&mut dashboard);
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
        let mut session = stopped_session();
        session.additional_mounts = vec![AdditionalMount {
            source: "/opt/cache".into(),
            destination: "/mnt/cache".into(),
            read_only: false,
        }];
        let mut dashboard = dashboard_with_session(session);
        open_resume_wizard(&mut dashboard);
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
        dashboard.apply_mount_source_validation("/opt/cache", Ok(None));
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
    fn resume_profile_step_marks_cross_harness_profiles_as_lossy() {
        let mut dashboard = dashboard_with_session(stopped_session());
        open_resume_wizard(&mut dashboard);
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
    fn raw_resume_review_names_the_exact_reused_project_directory() {
        let mut session = stopped_session();
        session.target_template_id = "localhost".into();
        session.project_directory = Some("/mnt/optane/bifrost-fird".into());
        session.bundle_id = "remote-project-a66373eef659f856".into();
        let mut config = config();
        config
            .targets
            .insert("localhost".into(), TargetTemplate::LocalBare);
        let mut dashboard = DashboardState::new(
            config,
            HelState {
                version: STATE_VERSION,
                sessions: BTreeMap::from([(session.id.clone(), session)]),
                mount_history: BTreeMap::new(),
            },
            BTreeMap::new(),
        );
        open_resume_wizard(&mut dashboard);
        dashboard.handle_key(key(KeyCode::Enter));
        dashboard.handle_key(key(KeyCode::Enter));

        let mut terminal = Terminal::new(TestBackend::new(120, 24)).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw resume review");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(
            rendered.contains("Project directory: /mnt/optane/bifrost-fird (reused)"),
            "{rendered}"
        );
        assert!(!rendered.contains("Project: remote-project-a66373eef659f856"));
    }

    #[test]
    fn resume_target_step_minus_halves_container_size_through_the_key_path() {
        let mut config = config();
        // Mirror the real config: an EC2 target that sorts before podman.
        config.targets.insert(
            "aws-runson".into(),
            TargetTemplate::AwsEc2 {
                aws_profile: None,
                region: "us-east-1".into(),
                launch_template: "lt-123".into(),
                launch_template_version: None,
                ssh_user: "ubuntu".into(),
                address_source: Default::default(),
                identity_file: None,
                ssh_args: Vec::new(),
            },
        );
        let mut dashboard = DashboardState::new(
            config,
            HelState {
                version: STATE_VERSION,
                sessions: BTreeMap::from([("session-1".into(), stopped_session())]),
                mount_history: BTreeMap::new(),
            },
            BTreeMap::new(),
        );

        dashboard.begin_resume_for("session-1");
        let Mode::Resume(wizard) = &dashboard.mode else {
            panic!("expected resume wizard, got {:?}", dashboard.mode);
        };
        assert_eq!(wizard.step, WizardStep::Profile);

        // 1/3 -> 2/3 target step; podman is the session's target.
        dashboard.handle_key(key(KeyCode::Enter));
        let Mode::Resume(wizard) = &dashboard.mode else {
            panic!("expected resume wizard on target step");
        };
        assert_eq!(wizard.step, WizardStep::Target);
        assert_eq!(
            nth_key(&dashboard.config.targets, wizard.target),
            "podman".to_string()
        );
        let gib = 1024 * 1024 * 1024;
        assert_eq!(
            wizard.resource_allocation,
            Some(SessionResourceAllocation::Container {
                cpus: 8,
                memory_bytes: 32 * gib,
            })
        );

        dashboard.handle_key(key(KeyCode::Char('-')));
        let Mode::Resume(wizard) = &dashboard.mode else {
            panic!("expected resume wizard after '-'");
        };
        assert_eq!(
            wizard.resource_allocation,
            Some(SessionResourceAllocation::Container {
                cpus: 4,
                memory_bytes: 16 * gib,
            })
        );
    }

    #[test]
    fn new_target_step_minus_halves_container_size_when_focus_is_off_content() {
        let mut dashboard = DashboardState::new(config(), HelState::default(), BTreeMap::new());
        dashboard.handle_key(ctrl_key('n'));
        let Mode::New(wizard) = &dashboard.mode else {
            panic!("expected new wizard, got {:?}", dashboard.mode);
        };
        assert_eq!(wizard.step, WizardStep::Profile);

        dashboard.handle_key(key(KeyCode::Enter));
        let Mode::New(wizard) = &dashboard.mode else {
            panic!("expected new wizard on target step");
        };
        assert_eq!(wizard.step, WizardStep::Target);
        let gib = 1024 * 1024 * 1024;
        assert_eq!(
            wizard.resource_allocation,
            Some(SessionResourceAllocation::Container {
                cpus: 8,
                memory_bytes: 32 * gib,
            })
        );

        dashboard.handle_key(key(KeyCode::Tab));
        let Mode::New(wizard) = &dashboard.mode else {
            panic!("expected new wizard after tab");
        };
        assert_ne!(wizard.focus, WizardFocus::Content);

        dashboard.handle_key(key(KeyCode::Char('-')));
        let Mode::New(wizard) = &dashboard.mode else {
            panic!("expected new wizard after '-'");
        };
        assert_eq!(
            wizard.resource_allocation,
            Some(SessionResourceAllocation::Container {
                cpus: 4,
                memory_bytes: 16 * gib,
            })
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
}
