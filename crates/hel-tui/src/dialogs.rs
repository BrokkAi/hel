//! Modal dialogs: session import, confirmations, and the rename editor.

use std::time::{Duration, Instant};

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Margin, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph, Wrap};

use std::path::PathBuf;

use hel::hel_config::{HarnessKind, mount_history_host};
use hel::hel_targets::{AdditionalMount, default_mount_destination, validate_additional_mounts};

use crate::render::render_session_scrollbar;
use crate::widgets::{
    action_buttons, centered_rect, focus_border, focused_buttons, popup_height, truncate_text,
};
use crate::wizards::read_only_marker;
use crate::{
    ButtonKey, DashboardAction, DashboardState, Mode, button_row_key, cycle_button_focus,
    cycle_control, move_index,
};

pub(crate) const FORCE_CONFIRMATION: &str = "DESTROY";

const IMPORT_STALL_WARNING_AFTER: Duration = Duration::from_secs(10);

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RenameEditor {
    pub(crate) session_id: String,
    pub(crate) title: String,
    pub(crate) focus: RenameFocus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RenameFocus {
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
pub(crate) enum Confirmation {
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
pub(crate) struct ConfirmDialog {
    pub(crate) confirmation: Confirmation,
    pub(crate) focus: usize,
}

impl ConfirmDialog {
    pub(crate) fn new(confirmation: Confirmation) -> Self {
        let focus = primary_button(confirmation_buttons(&confirmation));
        Self {
            confirmation,
            focus,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ImportProgress {
    pub(crate) session_title: String,
    pub(crate) step: usize,
    total: Option<usize>,
    pub(crate) message: String,
    last_updated: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ImportBundleConfirmation {
    dirty_git_roots: Vec<String>,
    omitted_non_git_dirs: Vec<String>,
    scratch_git_roots: Vec<String>,
    has_untracked_files: bool,
    ignore_untracked: bool,
    pub(crate) focus: usize,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ImportFocus {
    Filter,
    Profiles,
    Sessions,
    Cancel,
    Import,
}

/// Tab order for the import dialog. Profiles lead because the chosen profile
/// decides which sessions the filter and session pane can show.
const IMPORT_FOCUS_ORDER: [ImportFocus; 5] = [
    ImportFocus::Profiles,
    ImportFocus::Filter,
    ImportFocus::Sessions,
    ImportFocus::Cancel,
    ImportFocus::Import,
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ImportDialog {
    discovery_id: u64,
    pub(crate) profiles: Vec<ImportProfileOption>,
    profile_index: usize,
    pub(crate) session_index: usize,
    pub(crate) filter: String,
    pub(crate) focus: ImportFocus,
    opened_at: Instant,
}

impl ImportDialog {
    pub(crate) fn filtered_sessions(&self) -> Vec<&ImportSessionOption> {
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

    pub(crate) fn selected_session(&self) -> Option<&ImportSessionOption> {
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

pub(crate) fn render_import_progress(frame: &mut Frame, area: Rect, progress: &ImportProgress) {
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

pub(crate) fn render_import_bundle_confirmation(
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

pub(crate) fn render_import_dialog(frame: &mut Frame, area: Rect, dialog: &ImportDialog) {
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

/// Editable per-session container provisioning inputs: the size overrides and
/// the attached host directories. Nothing here is written to config.toml.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ContainerEditor {
    pub(crate) session_id: String,
    pub(crate) cpus: String,
    pub(crate) memory: String,
    pub(crate) mounts: Vec<AdditionalMount>,
    /// Remembered mount sources for this session's host, offered as
    /// suggestions and editable so a stale directory can be forgotten.
    pub(crate) suggestions: Vec<PathBuf>,
    pub(crate) source: String,
    pub(crate) destination: String,
    /// Read-only setting for the directory being typed, carried into the list
    /// when it is attached.
    pub(crate) read_only: bool,
    pub(crate) focus: ContainerEditFocus,
    pub(crate) mount_index: usize,
    pub(crate) suggestion_index: usize,
    pub(crate) error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ContainerEditFocus {
    Cpus,
    Memory,
    Source,
    Destination,
    ReadOnly,
    Mounts,
    Suggestions,
    Cancel,
    Save,
}

const CONTAINER_EDIT_BUTTONS: &[&str] = &["Cancel", "Save"];

/// One line, so the dialog never implies a live resize.
pub(crate) const CONTAINER_EDIT_SCOPE: &str = "Applies when the container is next recreated.";

impl ContainerEditor {
    /// Focus order, skipping the lists that have nothing to select.
    fn focus_order(&self) -> Vec<ContainerEditFocus> {
        let mut order = vec![
            ContainerEditFocus::Cpus,
            ContainerEditFocus::Memory,
            ContainerEditFocus::Source,
            ContainerEditFocus::Destination,
            ContainerEditFocus::ReadOnly,
        ];
        if !self.mounts.is_empty() {
            order.push(ContainerEditFocus::Mounts);
        }
        if !self.suggestions.is_empty() {
            order.push(ContainerEditFocus::Suggestions);
        }
        order.extend([ContainerEditFocus::Cancel, ContainerEditFocus::Save]);
        order
    }

    fn field_mut(&mut self) -> Option<&mut String> {
        match self.focus {
            ContainerEditFocus::Cpus => Some(&mut self.cpus),
            ContainerEditFocus::Memory => Some(&mut self.memory),
            ContainerEditFocus::Source => Some(&mut self.source),
            ContainerEditFocus::Destination => Some(&mut self.destination),
            ContainerEditFocus::ReadOnly
            | ContainerEditFocus::Mounts
            | ContainerEditFocus::Suggestions
            | ContainerEditFocus::Cancel
            | ContainerEditFocus::Save => None,
        }
    }

    fn button_index(&self) -> usize {
        match self.focus {
            ContainerEditFocus::Cancel => 0,
            _ => 1,
        }
    }

    /// Add the typed mount, filling in a default destination. Returns the
    /// reason it was rejected, if it was.
    fn add_mount(&mut self) -> Option<String> {
        let source = PathBuf::from(self.source.trim());
        if source.as_os_str().is_empty() {
            return Some("Enter a host directory to attach.".into());
        }
        let destination = if self.destination.trim().is_empty() {
            default_mount_destination(&source, &self.mounts)
        } else {
            PathBuf::from(self.destination.trim())
        };
        let mount = AdditionalMount {
            source,
            destination,
            read_only: self.read_only,
        };
        let mut mounts = self.mounts.clone();
        mounts.push(mount);
        if let Err(error) = validate_additional_mounts(&mounts) {
            return Some(error.to_string());
        }
        self.mounts = mounts;
        self.source.clear();
        self.destination.clear();
        self.read_only = false;
        self.mount_index = self.mounts.len() - 1;
        None
    }

    /// Toggle read-only for the entry being typed, or for the selected row.
    fn toggle_read_only(&mut self) {
        match self.focus {
            ContainerEditFocus::ReadOnly => self.read_only = !self.read_only,
            ContainerEditFocus::Mounts => {
                if let Some(mount) = self.mounts.get_mut(self.mount_index) {
                    mount.read_only = !mount.read_only;
                }
            }
            _ => {}
        }
    }

    fn take_suggestion(&mut self) {
        let Some(source) = self.suggestions.get(self.suggestion_index) else {
            return;
        };
        self.source = source.to_string_lossy().into_owned();
        self.destination = default_mount_destination(source, &self.mounts)
            .to_string_lossy()
            .into_owned();
        self.focus = ContainerEditFocus::Source;
    }

    fn remove_selected(&mut self) {
        match self.focus {
            ContainerEditFocus::Mounts if !self.mounts.is_empty() => {
                self.mounts.remove(self.mount_index);
                self.mount_index = self.mount_index.min(self.mounts.len().saturating_sub(1));
                if self.mounts.is_empty() {
                    self.focus = ContainerEditFocus::Source;
                }
            }
            ContainerEditFocus::Suggestions if !self.suggestions.is_empty() => {
                self.suggestions.remove(self.suggestion_index);
                self.suggestion_index = self
                    .suggestion_index
                    .min(self.suggestions.len().saturating_sub(1));
                if self.suggestions.is_empty() {
                    self.focus = ContainerEditFocus::Source;
                }
            }
            _ => {}
        }
    }

    fn save(&self) -> Result<DashboardAction, String> {
        validate_additional_mounts(&self.mounts).map_err(|error| error.to_string())?;
        let value = |text: &str| {
            let trimmed = text.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_owned())
        };
        Ok(DashboardAction::SaveContainerSettings {
            session_id: self.session_id.clone(),
            cpus: value(&self.cpus),
            memory: value(&self.memory),
            additional_mounts: self.mounts.clone(),
            mount_history: self.suggestions.clone(),
        })
    }
}

pub(crate) fn render_container_editor(frame: &mut Frame, area: Rect, editor: &ContainerEditor) {
    let field = |label: &str, value: &str, focused: bool| {
        let style = if focused {
            Style::default().fg(Color::Black).bg(Color::Cyan)
        } else {
            Style::default().fg(Color::Cyan)
        };
        Line::from(vec![
            ratatui::text::Span::raw(format!("{label}: ")),
            ratatui::text::Span::styled(format!("{value} "), style),
        ])
    };
    let mut lines = vec![
        Line::raw(format!("Session: {}", editor.session_id)),
        Line::styled(CONTAINER_EDIT_SCOPE, Style::default().fg(Color::DarkGray)),
        Line::raw(""),
        field(
            "CPUs",
            &editor.cpus,
            editor.focus == ContainerEditFocus::Cpus,
        ),
        field(
            "Memory",
            &editor.memory,
            editor.focus == ContainerEditFocus::Memory,
        ),
        Line::styled(
            "Empty keeps the target's value.",
            Style::default().fg(Color::DarkGray),
        ),
        Line::raw(""),
        Line::raw("Attached directories"),
    ];
    if editor.mounts.is_empty() {
        lines.push(Line::styled("  none", Style::default().fg(Color::DarkGray)));
    }
    for (index, mount) in editor.mounts.iter().enumerate() {
        let selected = editor.focus == ContainerEditFocus::Mounts && index == editor.mount_index;
        lines.push(Line::styled(
            format!(
                "{} {} -> {}{}",
                if selected { "›" } else { " " },
                mount.source.display(),
                mount.destination.display(),
                read_only_marker(mount.read_only)
            ),
            if selected {
                Style::default().fg(Color::Black).bg(Color::Cyan)
            } else {
                Style::default()
            },
        ));
    }
    lines.extend([
        Line::raw(""),
        field(
            "Attach host directory",
            &editor.source,
            editor.focus == ContainerEditFocus::Source,
        ),
        field(
            "Container destination",
            &editor.destination,
            editor.focus == ContainerEditFocus::Destination,
        ),
        field(
            "Read-only",
            if editor.read_only { "[x]" } else { "[ ]" },
            editor.focus == ContainerEditFocus::ReadOnly,
        ),
    ]);
    if !editor.suggestions.is_empty() {
        lines.push(Line::raw(""));
        lines.push(Line::raw("Remembered directories"));
        for (index, source) in editor.suggestions.iter().enumerate() {
            let selected =
                editor.focus == ContainerEditFocus::Suggestions && index == editor.suggestion_index;
            lines.push(Line::styled(
                format!("{} {}", if selected { "›" } else { " " }, source.display()),
                if selected {
                    Style::default().fg(Color::Black).bg(Color::Cyan)
                } else {
                    Style::default()
                },
            ));
        }
    }
    if let Some(error) = &editor.error {
        lines.push(Line::styled(
            error.clone(),
            Style::default().fg(Color::Yellow),
        ));
    }
    lines.extend([
        Line::raw(""),
        Line::styled(
            "Enter attaches or takes the selected row · Space toggles read-only · d forgets it · \
             Tab moves",
            Style::default().fg(Color::DarkGray),
        ),
        focused_buttons(CONTAINER_EDIT_BUTTONS, editor.button_index()),
    ]);
    let paragraph = Paragraph::new(lines).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Edit container size and mounts "),
    );
    let popup = centered_rect(70, popup_height(&paragraph, 70, 18, area), area);
    frame.render_widget(Clear, popup);
    frame.render_widget(paragraph, popup);
}

pub(crate) fn render_rename_editor(frame: &mut Frame, area: Rect, editor: &RenameEditor) {
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

pub(crate) fn render_confirmation(frame: &mut Frame, area: Rect, dialog: &ConfirmDialog) {
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

pub(crate) fn import_sessions_pane(area: Rect) -> Rect {
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

impl DashboardState {
    /// Open the container editor for the selected session, if that session
    /// runs on a container-backed target.
    pub(crate) fn begin_container_edit(&mut self) {
        let Some(session) = self.selected_container_session() else {
            self.notices
                .set("Container size and mounts apply to container targets only.");
            return;
        };
        let suggestions = self
            .config
            .targets
            .get(&session.target_template_id)
            .and_then(mount_history_host)
            .and_then(|host| self.state.mount_history.get(host))
            .cloned()
            .unwrap_or_default();
        self.mode = Mode::EditContainer(ContainerEditor {
            session_id: session.id.clone(),
            cpus: session.container_cpus.clone().unwrap_or_default(),
            memory: session.container_memory.clone().unwrap_or_default(),
            mounts: session.additional_mounts.clone(),
            suggestions,
            source: String::new(),
            destination: String::new(),
            read_only: false,
            focus: ContainerEditFocus::Cpus,
            mount_index: 0,
            suggestion_index: 0,
            error: None,
        });
    }

    pub(crate) fn handle_container_edit_key(
        &mut self,
        code: KeyCode,
        mut editor: ContainerEditor,
    ) -> DashboardAction {
        let action = match code {
            KeyCode::Esc => {
                self.cancel_modal();
                return DashboardAction::None;
            }
            KeyCode::Tab | KeyCode::BackTab => {
                editor.focus = cycle_control(
                    editor.focus,
                    &editor.focus_order(),
                    code == KeyCode::BackTab,
                );
                DashboardAction::None
            }
            KeyCode::Up | KeyCode::Down => {
                let reverse = code == KeyCode::Up;
                match editor.focus {
                    ContainerEditFocus::Mounts => move_index(
                        &mut editor.mount_index,
                        editor.mounts.len(),
                        if reverse { -1 } else { 1 },
                    ),
                    ContainerEditFocus::Suggestions => move_index(
                        &mut editor.suggestion_index,
                        editor.suggestions.len(),
                        if reverse { -1 } else { 1 },
                    ),
                    _ => {
                        editor.focus = cycle_control(editor.focus, &editor.focus_order(), reverse);
                    }
                }
                DashboardAction::None
            }
            KeyCode::Left | KeyCode::Right
                if matches!(
                    editor.focus,
                    ContainerEditFocus::Cancel | ContainerEditFocus::Save
                ) =>
            {
                editor.focus = if editor.focus == ContainerEditFocus::Cancel {
                    ContainerEditFocus::Save
                } else {
                    ContainerEditFocus::Cancel
                };
                DashboardAction::None
            }
            KeyCode::Enter if editor.focus == ContainerEditFocus::Cancel => {
                self.cancel_modal();
                return DashboardAction::None;
            }
            KeyCode::Enter if editor.focus == ContainerEditFocus::Suggestions => {
                editor.take_suggestion();
                editor.error = None;
                DashboardAction::None
            }
            // Enter belongs to Save everywhere else, so only the checkbox
            // itself answers to it; Space also toggles the selected row.
            KeyCode::Enter if editor.focus == ContainerEditFocus::ReadOnly => {
                editor.toggle_read_only();
                DashboardAction::None
            }
            KeyCode::Char(' ')
                if matches!(
                    editor.focus,
                    ContainerEditFocus::ReadOnly | ContainerEditFocus::Mounts
                ) =>
            {
                editor.toggle_read_only();
                DashboardAction::None
            }
            KeyCode::Enter
                if matches!(
                    editor.focus,
                    ContainerEditFocus::Source | ContainerEditFocus::Destination
                ) =>
            {
                editor.error = editor.add_mount();
                DashboardAction::None
            }
            KeyCode::Enter => match editor.save() {
                Ok(action) => {
                    self.cancel_modal();
                    return action;
                }
                Err(error) => {
                    editor.error = Some(error);
                    DashboardAction::None
                }
            },
            KeyCode::Delete | KeyCode::Char('d')
                if matches!(
                    editor.focus,
                    ContainerEditFocus::Mounts | ContainerEditFocus::Suggestions
                ) =>
            {
                editor.remove_selected();
                DashboardAction::None
            }
            KeyCode::Backspace => {
                if let Some(field) = editor.field_mut() {
                    field.pop();
                }
                DashboardAction::None
            }
            KeyCode::Char(character) => {
                if let Some(field) = editor.field_mut() {
                    field.push(character);
                    editor.error = None;
                }
                DashboardAction::None
            }
            _ => DashboardAction::None,
        };
        self.mode = Mode::EditContainer(editor);
        action
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

    pub(crate) fn handle_import_key(
        &mut self,
        key: KeyEvent,
        mut dialog: ImportDialog,
    ) -> DashboardAction {
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
            KeyCode::Tab | KeyCode::BackTab => {
                dialog.focus = cycle_control(
                    dialog.focus,
                    &IMPORT_FOCUS_ORDER,
                    key.code == KeyCode::BackTab,
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

    pub(crate) fn begin_rename(&mut self) {
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

    pub(crate) fn handle_rename_key(
        &mut self,
        code: KeyCode,
        mut editor: RenameEditor,
    ) -> DashboardAction {
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

    pub(crate) fn handle_import_bundle_key(
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

    pub(crate) fn handle_confirmation_key(
        &mut self,
        code: KeyCode,
        dialog: ConfirmDialog,
    ) -> DashboardAction {
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
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use crossterm::event::{KeyCode, MouseEventKind};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use ratatui::style::Color;

    use hel::hel_config::HarnessKind;
    use hel::hel_state::SessionState;

    use super::*;
    use crate::test_support::*;

    use crate::render::render;
    use crate::{DashboardAction, DashboardState, Focus, MOUSE_SCROLL_ROWS, Mode};

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

    fn dashboard_with_container_session() -> DashboardState {
        let mut session = archived_session();
        session.additional_mounts = vec![AdditionalMount {
            source: PathBuf::from("/srv/data"),
            destination: PathBuf::from("/mnt/data"),
            read_only: false,
        }];
        let mut dashboard = dashboard_with_session(session);
        dashboard
            .state
            .mount_history
            .insert("local".into(), vec![PathBuf::from("/srv/models")]);
        dashboard
    }

    fn container_editor(dashboard: &DashboardState) -> &ContainerEditor {
        let Mode::EditContainer(editor) = &dashboard.mode else {
            panic!("expected the container editor");
        };
        editor
    }

    #[test]
    fn ctrl_e_opens_the_container_editor_only_once_setup_is_done() {
        let mut empty = DashboardState::new(
            hel::hel_config::HelConfig {
                version: hel::hel_config::CONFIG_VERSION,
                profiles: Default::default(),
                bundles: Default::default(),
                targets: Default::default(),
            },
            hel::hel_state::HelState::default(),
            Default::default(),
        );
        assert_eq!(empty.handle_key(ctrl_key('e')), DashboardAction::OpenConfig);
        assert!(matches!(empty.mode, Mode::Dashboard));

        let mut dashboard = dashboard_with_container_session();
        assert_eq!(dashboard.handle_key(ctrl_key('e')), DashboardAction::None);
        let editor = container_editor(&dashboard);
        assert_eq!(editor.session_id, "session-1");
        assert_eq!(editor.mounts.len(), 1);
        assert_eq!(editor.suggestions, vec![PathBuf::from("/srv/models")]);
    }

    #[test]
    fn container_editor_saves_edited_size_mounts_and_remembered_sources() {
        let mut dashboard = dashboard_with_container_session();
        dashboard.handle_key(ctrl_key('e'));
        for character in "4".chars() {
            dashboard.handle_key(key(KeyCode::Char(character)));
        }
        dashboard.handle_key(key(KeyCode::Tab));
        for character in "6g".chars() {
            dashboard.handle_key(key(KeyCode::Char(character)));
        }
        assert_eq!(
            container_editor(&dashboard).focus,
            ContainerEditFocus::Memory
        );

        // Take the remembered directory as the next mount.
        while container_editor(&dashboard).focus != ContainerEditFocus::Suggestions {
            dashboard.handle_key(key(KeyCode::Tab));
        }
        dashboard.handle_key(key(KeyCode::Enter));
        assert_eq!(container_editor(&dashboard).source, "/srv/models");
        dashboard.handle_key(key(KeyCode::Enter));
        assert_eq!(
            container_editor(&dashboard).mounts,
            vec![
                AdditionalMount {
                    source: PathBuf::from("/srv/data"),
                    destination: PathBuf::from("/mnt/data"),
                    read_only: false,
                },
                AdditionalMount {
                    source: PathBuf::from("/srv/models"),
                    destination: PathBuf::from("/mnt/models"),
                    read_only: false,
                },
            ]
        );

        // Forget the remembered directory, then drop the original mount.
        while container_editor(&dashboard).focus != ContainerEditFocus::Suggestions {
            dashboard.handle_key(key(KeyCode::Tab));
        }
        dashboard.handle_key(key(KeyCode::Char('d')));
        assert!(container_editor(&dashboard).suggestions.is_empty());
        while container_editor(&dashboard).focus != ContainerEditFocus::Mounts {
            dashboard.handle_key(key(KeyCode::Tab));
        }
        dashboard.handle_key(key(KeyCode::Up));
        assert_eq!(container_editor(&dashboard).mount_index, 0);
        dashboard.handle_key(key(KeyCode::Char('d')));

        while container_editor(&dashboard).focus != ContainerEditFocus::Save {
            dashboard.handle_key(key(KeyCode::Tab));
        }
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::SaveContainerSettings {
                session_id: "session-1".into(),
                cpus: Some("4".into()),
                memory: Some("6g".into()),
                additional_mounts: vec![AdditionalMount {
                    source: PathBuf::from("/srv/models"),
                    destination: PathBuf::from("/mnt/models"),
                    read_only: false,
                }],
                mount_history: Vec::new(),
            }
        );
        assert!(matches!(dashboard.mode, Mode::Dashboard));
    }

    #[test]
    fn container_editor_marks_new_and_existing_mounts_read_only() {
        let mut dashboard = dashboard_with_container_session();
        dashboard.handle_key(ctrl_key('e'));

        // Space on the checkbox attaches the next directory read-only.
        while container_editor(&dashboard).focus != ContainerEditFocus::Source {
            dashboard.handle_key(key(KeyCode::Tab));
        }
        for character in "/nfs/share".chars() {
            dashboard.handle_key(key(KeyCode::Char(character)));
        }
        dashboard.handle_key(key(KeyCode::Tab));
        dashboard.handle_key(key(KeyCode::Tab));
        assert_eq!(
            container_editor(&dashboard).focus,
            ContainerEditFocus::ReadOnly
        );
        dashboard.handle_key(key(KeyCode::Char(' ')));
        assert!(container_editor(&dashboard).read_only);
        while container_editor(&dashboard).focus != ContainerEditFocus::Source {
            dashboard.handle_key(key(KeyCode::Tab));
        }
        dashboard.handle_key(key(KeyCode::Enter));

        // Space on a listed row toggles that row, and the flag is saved.
        while container_editor(&dashboard).focus != ContainerEditFocus::Mounts {
            dashboard.handle_key(key(KeyCode::Tab));
        }
        dashboard.handle_key(key(KeyCode::Up));
        assert_eq!(container_editor(&dashboard).mount_index, 0);
        dashboard.handle_key(key(KeyCode::Char(' ')));

        while container_editor(&dashboard).focus != ContainerEditFocus::Save {
            dashboard.handle_key(key(KeyCode::Tab));
        }
        assert_eq!(
            dashboard.handle_key(key(KeyCode::Enter)),
            DashboardAction::SaveContainerSettings {
                session_id: "session-1".into(),
                cpus: None,
                memory: None,
                additional_mounts: vec![
                    AdditionalMount {
                        source: PathBuf::from("/srv/data"),
                        destination: PathBuf::from("/mnt/data"),
                        read_only: true,
                    },
                    AdditionalMount {
                        source: PathBuf::from("/nfs/share"),
                        destination: PathBuf::from("/mnt/share"),
                        read_only: true,
                    },
                ],
                mount_history: vec![PathBuf::from("/srv/models")],
            }
        );
    }

    #[test]
    fn container_editor_says_when_the_change_takes_effect() {
        let mut dashboard = dashboard_with_container_session();
        dashboard.handle_key(ctrl_key('e'));
        let mut terminal = Terminal::new(TestBackend::new(100, 40)).expect("terminal");
        terminal
            .draw(|frame| crate::render::render(frame, &mut dashboard))
            .expect("draw editor");
        let rendered = buffer_lines(terminal.backend().buffer()).join("\n");
        assert!(rendered.contains("Applies when the container is next recreated"));
        assert!(rendered.contains("/srv/data"));
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
}
