//! Dashboard rendering: pane layout, session tables, capacity, quotas, footer.
use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Margin, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{
    Block, BorderType, Borders, Cell, Clear, HighlightSpacing, Paragraph, Row, Scrollbar,
    ScrollbarOrientation, ScrollbarState, Table, TableState, Wrap,
};

use hel::hel_chat::render_agent_message_head;
#[cfg(test)]
use hel::hel_chat::render_agent_message_tail;
use hel::hel_config::{HarnessKind, HelConfig, PermissionMode};
use hel::hel_quota::{ProfileQuota, QuotaWindow};
use hel::hel_selection::{SurfaceFrame, SurfaceId};
use hel::hel_state::{SessionRecord, SessionState};
use hel::hel_targets::DeploymentCapacityKind;

use crate::dialogs::{
    render_config_id_editor, render_confirmation, render_container_editor,
    render_import_bundle_confirmation, render_import_progress, render_rename_editor,
    render_repository_origin, render_session_edit, render_target_actions, render_web_dialog,
};
use crate::ingest::{CapacityDetail, SessionDetail, SessionOperationDisplay};
use crate::manager::{
    ManagerFocus, ManagerMessageRole, ManagerRecommendation, format_age, manager_status_label,
};
use crate::resume::{render_resume_dialog, resume_sessions_pane};
use crate::widgets::{bordered_content, focus_border, format_resource_bytes};
use crate::wizards::{render_new_wizard, render_resume_wizard};
use crate::{DASHBOARD_PANE_COUNT, DashboardState, Focus, Mode, SessionOperationKind};

#[cfg(test)]
const ACTIVE_MESSAGE_LINES: usize = 4;

const SESSION_TABLE_CHROME_HEIGHT: u16 = 3;
const ACTIVE_PANE_CHROME_HEIGHT: u16 = 2;

#[cfg(test)]
pub(crate) const SUMMARY_RULE: &str = "─";

const DASHBOARD_FIXED_HEIGHT: u16 = 3;

pub fn render(frame: &mut Frame, dashboard: &mut DashboardState) {
    dashboard.pane_areas = None;
    dashboard.active_row_areas.clear();
    dashboard.project_heading_areas.clear();
    dashboard.frame_surfaces.clear();
    let area = frame.area();
    if area.width < MINIMUM_TERMINAL_WIDTH {
        render_terminal_too_small(
            frame,
            area,
            TerminalSizeRequirement::Width(MINIMUM_TERMINAL_WIDTH),
        );
        return;
    }
    if matches!(dashboard.mode, Mode::Manager) {
        render_manager(frame, area, dashboard);
        return;
    }
    dashboard.resume_sessions_area =
        matches!(dashboard.mode, Mode::ResumeDialog(_)).then(|| resume_sessions_pane(area));
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
    render_modal(frame, area, dashboard);
}

/// Draws the active modal over the dashboard already on the frame. Each modal
/// clears its own centered rect, so the panes stay visible around it.
///
/// The registry moves out for the call because the modal renderers read the
/// rest of the dashboard while they register their own surfaces.
fn render_modal(frame: &mut Frame, area: Rect, dashboard: &mut DashboardState) {
    let mut surfaces = std::mem::take(&mut dashboard.frame_surfaces);
    match &dashboard.mode {
        Mode::New(wizard) => render_new_wizard(frame, area, dashboard, wizard, &mut surfaces),
        Mode::Resume(wizard) => render_resume_wizard(frame, area, dashboard, wizard, &mut surfaces),
        Mode::ResumeDialog(dialog) => {
            render_resume_dialog(frame, area, dashboard, dialog, &mut surfaces)
        }
        Mode::RepositoryOrigin(dialog) => {
            render_repository_origin(frame, area, dialog, &mut surfaces)
        }
        Mode::SessionEdit(dialog) => render_session_edit(frame, area, dialog, &mut surfaces),
        Mode::ConfigId(editor) => render_config_id_editor(frame, area, editor, &mut surfaces),
        Mode::TargetActions(dialog) => {
            render_target_actions(frame, area, dashboard, dialog, &mut surfaces)
        }
        Mode::Web(dialog) => render_web_dialog(frame, area, dialog, &mut surfaces),
        Mode::Rename(editor) => render_rename_editor(frame, area, editor, &mut surfaces),
        Mode::EditContainer(editor) => render_container_editor(frame, area, editor, &mut surfaces),
        Mode::Importing(progress) => render_import_progress(frame, area, progress, &mut surfaces),
        Mode::ConfirmImportBundle(confirmation) => {
            render_import_bundle_confirmation(frame, area, confirmation, &mut surfaces)
        }
        Mode::Confirm(dialog) => render_confirmation(frame, area, dialog, &mut surfaces),
        Mode::Manager => {}
        Mode::Dashboard => {}
    }
    dashboard.frame_surfaces = surfaces;
}

fn render_manager(frame: &mut Frame, area: Rect, dashboard: &DashboardState) {
    let fixed = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(8),
            Constraint::Length(3),
            Constraint::Length(2),
        ])
        .split(area);
    render_dashboard_title(frame, fixed[0], "Dashboard manager");
    let body = if area.width >= 90 {
        Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(43), Constraint::Percentage(57)])
            .split(fixed[1])
    } else {
        Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(45), Constraint::Percentage(55)])
            .split(fixed[1])
    };
    render_manager_sessions(frame, body[0], dashboard);
    render_manager_transcript(frame, body[1], dashboard);
    render_manager_prompt(frame, fixed[2], dashboard);
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled("Tab", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(" focus  "),
            Span::styled("s", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(" stop idle  "),
            Span::styled("a", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(" archive  "),
            Span::styled("d", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(" clean up  "),
            Span::styled("Esc/Ctrl+M", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(" close"),
        ])),
        fixed[3],
    );
}

fn render_manager_sessions(frame: &mut Frame, area: Rect, dashboard: &DashboardState) {
    let rows = dashboard.manager_rows();
    let visible = usize::from(area.height.saturating_sub(2) / 2).max(1);
    let selected = dashboard
        .manager
        .session_index
        .min(rows.len().saturating_sub(1));
    let start = selected
        .saturating_sub(visible.saturating_sub(1))
        .min(rows.len().saturating_sub(visible));
    let mut lines = Vec::new();
    if rows.is_empty() {
        lines.push(Line::styled(
            "No sessions in this workspace.",
            Style::default().fg(Color::DarkGray),
        ));
    }
    for (index, row) in rows.iter().enumerate().skip(start).take(visible) {
        let selected = index == selected;
        let style = if selected {
            Style::default()
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default()
        };
        let short_id = row.session_id.chars().take(8).collect::<String>();
        lines.push(Line::styled(
            format!(
                "{} {}  [{short_id}]",
                if selected { "›" } else { " " },
                row.title
            ),
            style,
        ));
        let age = row
            .age_seconds
            .map(|age| format!(" · {} ago", format_age(age)))
            .unwrap_or_default();
        let recommendation = match row.recommendation {
            Some(ManagerRecommendation::Stop) => " · press s to stop",
            Some(ManagerRecommendation::Destroy) => " · press d to clean up",
            None => "",
        };
        lines.push(Line::styled(
            format!(
                "  {} · {} → {}{age}{recommendation}",
                manager_status_label(row.status),
                row.project,
                row.target,
            ),
            style,
        ));
    }
    frame.render_widget(
        Paragraph::new(lines).block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(focus_border(
                    dashboard.manager.focus == ManagerFocus::Sessions,
                ))
                .title(format!(" Sessions · {} ", rows.len())),
        ),
        area,
    );
}

fn render_manager_transcript(frame: &mut Frame, area: Rect, dashboard: &DashboardState) {
    let messages = &dashboard.manager.messages;
    let end = messages
        .len()
        .saturating_sub(dashboard.manager.transcript_scroll)
        .max(1)
        .min(messages.len());
    let visible_messages = usize::from(area.height.saturating_sub(2) / 2).max(2);
    let start = end.saturating_sub(visible_messages);
    let mut lines = Vec::new();
    for message in &messages[start..end] {
        let (label, style) = match message.role {
            ManagerMessageRole::User => (
                "You",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            ManagerMessageRole::Manager => (
                "Manager",
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            ),
            ManagerMessageRole::System => (
                "Hel",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
        };
        lines.push(Line::styled(format!("{label}:"), style));
        lines.extend(
            message
                .text
                .lines()
                .map(|line| Line::raw(format!("  {line}"))),
        );
        lines.push(Line::raw(""));
    }
    if dashboard.manager.in_flight.is_some() {
        lines.push(Line::styled(
            "Manager is thinking…",
            Style::default().fg(Color::Yellow),
        ));
    }
    let provider = dashboard
        .manager
        .last_provider
        .as_deref()
        .map(|id| format!(" · via {}", id.chars().take(8).collect::<String>()))
        .unwrap_or_default();
    frame.render_widget(
        Paragraph::new(lines).wrap(Wrap { trim: false }).block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(focus_border(
                    dashboard.manager.focus == ManagerFocus::Transcript,
                ))
                .title(format!(" Manager transcript{provider} ")),
        ),
        area,
    );
}

fn render_manager_prompt(frame: &mut Frame, area: Rect, dashboard: &DashboardState) {
    let focused = dashboard.manager.focus == ManagerFocus::Prompt;
    let value = if focused {
        dashboard.manager.input.with_cursor_marker("▏")
    } else if dashboard.manager.input.is_empty() {
        "Ask for a status summary…".into()
    } else {
        dashboard.manager.input.value().to_owned()
    };
    frame.render_widget(
        Paragraph::new(value).block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(focus_border(focused))
                .title(" Prompt · Enter to send "),
        ),
        area,
    );
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
    pub(crate) active: u16,
    pub(crate) capacity: u16,
    pub(crate) quotas: u16,
}

impl PaneHeights {
    fn as_array(self) -> [u16; DASHBOARD_PANE_COUNT] {
        [self.active, self.capacity, self.quotas]
    }

    fn from_array(heights: [u16; DASHBOARD_PANE_COUNT]) -> Self {
        Self {
            active: heights[0],
            capacity: heights[1],
            quotas: heights[2],
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
            Focus::Capacity => 1,
            Focus::Quotas => 2,
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
    let active = dashboard
        .ordered_sessions()
        .into_iter()
        .map(|session| session.id.clone())
        .collect::<Vec<_>>();
    let active_count = active.len();
    let mut previous_project = None;
    let active_row_heights = active
        .iter()
        .enumerate()
        .map(|(index, id)| {
            let session = &dashboard.state.sessions[id];
            let source = dashboard.project_source(session);
            let heading = u16::from(previous_project.as_ref() != Some(&source.key));
            let expanded = dashboard.project_is_expanded(session);
            let spacing = session_row_spacing(dashboard, &active, index, &source.key, expanded);
            previous_project = Some(source.key);
            heading + if expanded { 4 } else { 1 } + spacing
        })
        .collect::<Vec<_>>();
    let full = PaneHeights {
        active: active_pane_height(&active_row_heights, active_count),
        capacity: plain_table_height(dashboard.capacity_details.len()),
        quotas: plain_table_height(dashboard.config.profiles.len()),
    };
    let minimized = PaneHeights {
        active: if dashboard.focus == Focus::Active {
            ACTIVE_PANE_CHROME_HEIGHT
        } else {
            active_pane_height(&active_row_heights, active_count.min(2))
        },
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
    dashboard.pane_areas = Some([panes[0], panes[1], panes[2]]);
    for (index, pane) in panes.iter().take(DASHBOARD_PANE_COUNT).enumerate() {
        // The selectable area is the text inside the border, so a selection
        // never picks up border glyphs or the scrollbar column.
        dashboard.frame_surfaces.push(SurfaceFrame::fixed(
            SurfaceId::DashboardPane(index as u8),
            bordered_content(*pane),
        ));
    }
    let rendered_rows = render_sessions(frame, panes[0], dashboard, &active);
    dashboard.active_row_areas = rendered_rows.active_row_areas;
    dashboard.project_heading_areas = rendered_rows.project_heading_areas;
    render_capacity(frame, panes[1], dashboard);
    render_quotas(frame, panes[2], dashboard);
    render_footer(frame, fixed[2], dashboard);
    render_modal(frame, frame_area, dashboard);
}

fn plain_table_height(rows: usize) -> u16 {
    SESSION_TABLE_CHROME_HEIGHT.saturating_add(rows.min(u16::MAX as usize) as u16)
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
    ACTIVE_PANE_CHROME_HEIGHT.saturating_add(row_height)
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

/// Session-row rendering results that the caller folds back into the
/// dashboard's mouse hitboxes once the borrow of `active` (which aliases
/// `dashboard.state.sessions`) has ended.
struct SessionRowsRendered {
    active_row_areas: Vec<(usize, Rect)>,
    project_heading_areas: Vec<(String, Rect)>,
}

fn session_row_spacing(
    dashboard: &DashboardState,
    active: &[String],
    index: usize,
    project_key: &str,
    expanded: bool,
) -> u16 {
    u16::from(
        active
            .get(index + 1)
            .and_then(|next| dashboard.state.sessions.get(next))
            .is_some_and(|next| expanded || dashboard.project_source(next).key != project_key),
    )
}

/// Draws the Active pane and reports the selected session's preview hitbox and
/// the per-row mouse hitboxes.
fn render_sessions(
    frame: &mut Frame,
    active_area: Rect,
    dashboard: &DashboardState,
    active: &[String],
) -> SessionRowsRendered {
    let now_epoch_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let project_count = dashboard.project_keys().len();
    let mut short_projects = BTreeMap::<String, BTreeSet<String>>::new();
    for session in active
        .iter()
        .filter_map(|id| dashboard.state.sessions.get(id))
    {
        short_projects
            .entry(dashboard.project_source(session).short)
            .or_default()
            .insert(dashboard.project_source(session).key);
    }
    let mut target_counts = BTreeMap::<(String, String), usize>::new();
    for id in active {
        let Some(session) = dashboard.state.sessions.get(id) else {
            continue;
        };
        let project_key = dashboard.project_source(session).key;
        let target = session_target_label(
            session,
            dashboard.session_operations.get(id),
            &dashboard.config,
        );
        *target_counts.entry((project_key, target)).or_default() += 1;
    }
    let mut target_occurrences = BTreeMap::<(String, String), usize>::new();
    let mut previous_project = None;
    let mut project_number = 0;
    let mut row_meta = Vec::new();
    let active_rows = active.iter().enumerate().filter_map(|(index, id)| {
        let session = dashboard.state.sessions.get(id)?;
        let source = dashboard.project_source(session);
        let first = previous_project.as_ref() != Some(&source.key);
        if first {
            project_number += 1;
            previous_project = Some(source.key.clone());
        }
        let mut lines = Vec::new();
        if first {
            let label = if short_projects
                .get(&source.short)
                .is_some_and(|projects| projects.len() > 1)
            {
                &source.full
            } else {
                &source.short
            };
            let hotkey = if project_count > 1 && project_number <= 9 {
                format!("[{}] ", project_number)
            } else {
                String::new()
            };
            lines.push(Line::styled(
                format!("{hotkey}{label}"),
                Style::default().add_modifier(Modifier::BOLD),
            ));
        }
        let detail = dashboard.session_details.get(id);
        let unreachable = dashboard.unreachable_sessions.contains(id);
        let target = session_target_label(
            session,
            dashboard.session_operations.get(id),
            &dashboard.config,
        );
        let target_key = (source.key.clone(), target.clone());
        let occurrence = target_occurrences.entry(target_key.clone()).or_default();
        *occurrence += 1;
        let target = if target_counts.get(&target_key).copied().unwrap_or_default() > 1 {
            format!("{target} [{}]", *occurrence)
        } else {
            target
        };
        let expanded = dashboard.project_is_expanded(session);
        let selected = dashboard.focus == Focus::Active && index == dashboard.session_index;
        let prefix = if selected { "› " } else { "  " };
        let permission = session_permission_badge(
            session,
            dashboard.session_operations.get(id),
            &dashboard.config,
        );
        if expanded {
            lines.push(session_top_line(
                prefix,
                session,
                detail,
                unreachable,
                dashboard.session_operations.get(id),
                now_epoch_seconds,
                &target,
                permission,
            ));
            lines.push(prefixed_summary_line(
                "  ",
                "You: ",
                detail.and_then(|detail| detail.last_user_message.as_deref()),
                usize::from(active_area.width.saturating_sub(4)),
                detail.is_some_and(|detail| detail.last_agent_message_follows_last_user),
            ));
            let agent_excerpt = detail.and_then(|detail| {
                if detail.last_user_message.is_none() || detail.last_agent_message_follows_last_user
                {
                    detail.last_agent_message.as_deref()
                } else {
                    detail.latest_agent_activity_after_last_user.as_deref()
                }
            });
            let show_agent_excerpt = detail.is_none_or(|detail| {
                detail.last_user_message.is_none()
                    || detail.last_agent_message_follows_last_user
                    || detail.latest_agent_activity_after_last_user.is_some()
            });
            if show_agent_excerpt {
                let prefixes = dashboard_agent_prefixes(now_epoch_seconds, detail);
                let prefix_width = prefixes.iter().map(String::len).max().unwrap_or_default();
                let mut agent = agent_excerpt
                    .map(|message| {
                        render_agent_message_head(
                            message,
                            usize::from(active_area.width.saturating_sub(
                                u16::try_from(prefix_width + 5).unwrap_or(u16::MAX),
                            )),
                            2,
                        )
                    })
                    .unwrap_or_default();
                if agent.is_empty() {
                    agent.push(Line::raw("No messages yet"));
                }
                agent.resize(2, Line::default());
                for (agent_index, mut line) in agent.into_iter().take(2).enumerate() {
                    let mut spans = vec![Span::raw("  ")];
                    spans.push(Span::styled(
                        format!("{} ", prefixes[agent_index]),
                        Style::default().add_modifier(Modifier::BOLD),
                    ));
                    spans.append(&mut line.spans);
                    lines.push(Line::from(spans));
                }
            }
        } else {
            lines.push(collapsed_session_line(
                prefix,
                &target,
                detail,
                unreachable,
                now_epoch_seconds,
                usize::from(active_area.width.saturating_sub(4)),
                permission,
            ));
        }
        let heading = usize::from(first);
        let content_height = lines.len() as u16;
        let spacing = session_row_spacing(dashboard, active, index, &source.key, expanded);
        row_meta.push((
            source.key,
            heading,
            content_height,
            content_height + spacing,
        ));
        Some(
            Row::new([Cell::from(Text::from(lines))])
                .height(content_height)
                .bottom_margin(spacing),
        )
    });
    let active_focused = dashboard.focus == Focus::Active;
    let active_table = Table::new(active_rows, [Constraint::Min(1)]).block(
        Block::default()
            .borders(Borders::ALL)
            .border_type(focus_border(active_focused))
            .title(" Active "),
    );
    let mut active_state = TableState::default()
        .with_selected((dashboard.session_index < active.len()).then_some(dashboard.session_index));
    frame.render_stateful_widget(active_table, active_area, &mut active_state);
    let active_offset = active_state.offset();
    let mut row_y = active_area.y + 1;
    let mut visible_sessions = 0;
    let mut active_row_areas = Vec::new();
    let mut project_heading_areas = Vec::new();
    for (index, (project_key, heading, content_height, total_height)) in
        row_meta.iter().enumerate().skip(active_offset)
    {
        if row_y >= active_area.bottom().saturating_sub(1) {
            break;
        }
        visible_sessions += 1;
        if *heading > 0 {
            project_heading_areas.push((
                project_key.clone(),
                Rect::new(
                    active_area.x + 1,
                    row_y,
                    active_area.width.saturating_sub(2),
                    1,
                ),
            ));
        }
        let session_y = row_y.saturating_add(*heading as u16);
        let session_height = content_height.saturating_sub(*heading as u16);
        let row_rect = Rect::new(
            active_area.x.saturating_add(1),
            session_y,
            active_area.width.saturating_sub(2),
            session_height.min(
                active_area
                    .bottom()
                    .saturating_sub(1)
                    .saturating_sub(session_y),
            ),
        );
        active_row_areas.push((index, row_rect));
        row_y = row_y.saturating_add(*total_height);
    }
    render_session_scrollbar(
        frame,
        active_area,
        active.len(),
        active_offset,
        visible_sessions,
    );

    SessionRowsRendered {
        active_row_areas,
        project_heading_areas,
    }
}

fn collapsed_session_line(
    prefix: &str,
    target: &str,
    detail: Option<&SessionDetail>,
    unreachable: bool,
    now_epoch_seconds: u64,
    width: usize,
    permission: Option<Span<'static>>,
) -> Line<'static> {
    let clock = hel::usage_format::format_turn_clock(
        now_epoch_seconds,
        detail.and_then(|detail| detail.current_turn_started_at),
    );
    let fragment = detail
        .and_then(|detail| detail.last_agent_message.as_deref())
        .and_then(|message| message.lines().rev().find(|line| !line.trim().is_empty()))
        .unwrap_or("No messages yet")
        .trim();
    let style = Style::default().fg(session_band_color(detail, unreachable));
    let mut lead_width = prefix.chars().count() + target.chars().count() + 2;
    let mut spans = vec![Span::styled(format!("{prefix}{target}"), style)];
    if let Some(permission) = permission {
        spans.push(Span::styled("  ", style));
        lead_width += permission.width() + 2;
        spans.push(permission);
    }
    spans.push(Span::styled("  ", style));
    lead_width += clock.chars().count() + 1;
    spans.push(Span::styled(
        format!(
            "{clock} {}",
            crate::widgets::truncate_text(fragment, width.saturating_sub(lead_width))
        ),
        style,
    ));
    Line::from(spans).style(style)
}

#[allow(clippy::too_many_arguments)]
fn session_top_line(
    prefix: &str,
    session: &SessionRecord,
    detail: Option<&SessionDetail>,
    unreachable: bool,
    operation: Option<&SessionOperationDisplay>,
    now_epoch_seconds: u64,
    target: &str,
    permission: Option<Span<'static>>,
) -> Line<'static> {
    let (profile, _) = operation
        .and_then(|operation| operation.resume_destination.clone())
        .unwrap_or_else(|| {
            (
                session.last_profile.clone(),
                session.target_template_id.clone(),
            )
        });
    let status_columns = if let Some(operation) = operation {
        let (label, started_at) = operation_status(operation);
        Some(vec![format!(
            "{label} {}",
            format_elapsed(now_epoch_seconds.saturating_sub(started_at))
        )])
    } else if session.state == SessionState::Provisioning {
        let started_at = session_updated_at_epoch_seconds(session).unwrap_or(now_epoch_seconds);
        Some(vec![format!(
            "Launch {}",
            format_elapsed(now_epoch_seconds.saturating_sub(started_at))
        )])
    } else {
        None
    };
    let queued_prompts = detail.map_or(0, |detail| detail.queued_prompts.len());
    let mut columns = vec![target.to_owned()];
    if queued_prompts > 0 {
        columns.push(format!("[Q {queued_prompts}]"));
    }
    let summary = if let Some(status_columns) = status_columns {
        columns.extend(status_columns);
        columns.push(profile.clone());
        columns.join("  ")
    } else {
        columns.push(profile.clone());
        columns.join("  ")
    };
    let session_name =
        recovery_warning_name(session, session_name(session).to_owned(), now_epoch_seconds);
    let summary_tail = summary
        .strip_prefix(target)
        .expect("session summary starts with its target");
    let style = Style::default().fg(session_band_color(detail, unreachable));
    let mut spans = vec![Span::styled(format!("{prefix}{target}"), style)];
    if let Some(permission) = permission {
        spans.push(Span::styled("  ", style));
        spans.push(permission);
    }
    spans.push(Span::styled(
        format!("{summary_tail}  {session_name}"),
        style,
    ));
    Line::from(spans).style(style)
}

const DASHBOARD_CLOCK_WIDTH: usize = 6;

fn compact_dashboard_clock(elapsed_seconds: u64) -> String {
    let minutes = elapsed_seconds / 60;
    if minutes > 99 {
        format!("{minutes}m")
    } else if minutes > 0 {
        format!("{minutes}m{:02}s", elapsed_seconds % 60)
    } else {
        format!("{}s", elapsed_seconds % 60)
    }
}

fn dashboard_agent_prefixes(now_epoch_seconds: u64, detail: Option<&SessionDetail>) -> [String; 2] {
    let Some(turn_started) = detail.and_then(|detail| detail.current_turn_started_at) else {
        let time = detail
            .and_then(|detail| detail.last_activity_at_ms)
            .and_then(|value| i64::try_from(value).ok())
            .and_then(|value| hel::hel_chat::format_event_time(Some(value)))
            .unwrap_or_default();
        return ["Agent:".into(), format!("{time:<6}")];
    };
    let step_started = detail
        .and_then(|detail| detail.last_acp_activity_at_ms)
        .map(|value| value / 1_000)
        .unwrap_or(turn_started)
        .max(turn_started);
    [
        format!(
            "T {:>DASHBOARD_CLOCK_WIDTH$}",
            compact_dashboard_clock(now_epoch_seconds.saturating_sub(turn_started))
        ),
        format!(
            "S {:>DASHBOARD_CLOCK_WIDTH$}",
            compact_dashboard_clock(now_epoch_seconds.saturating_sub(step_started))
        ),
    ]
}

fn session_target_label(
    session: &SessionRecord,
    operation: Option<&SessionOperationDisplay>,
    config: &HelConfig,
) -> String {
    let target_id = operation
        .and_then(|operation| operation.resume_destination.as_ref())
        .map(|(_, target_id)| target_id)
        .unwrap_or(&session.target_template_id);
    session.project_target(config, target_id)
}

fn session_permission_badge(
    session: &SessionRecord,
    operation: Option<&SessionOperationDisplay>,
    config: &HelConfig,
) -> Option<Span<'static>> {
    let target_id = operation
        .and_then(|operation| operation.resume_destination.as_ref())
        .map(|(_, target_id)| target_id)
        .unwrap_or(&session.target_template_id);
    config
        .targets
        .get(target_id)
        .and_then(|target| permission_badge(target.permission_mode()))
}

fn permission_badge(mode: Option<PermissionMode>) -> Option<Span<'static>> {
    mode.map(|mode| match mode {
        PermissionMode::Guardian => Span::styled(
            "[G]",
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        ),
        PermissionMode::Yolo => Span::styled(
            "[Y]",
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        ),
    })
}

fn capacity_target_labels(target_ids: &[String], config: &HelConfig) -> Line<'static> {
    let mut spans = Vec::new();
    for (index, target_id) in target_ids.iter().enumerate() {
        if index > 0 {
            spans.push(Span::raw(", "));
        }
        spans.push(Span::raw(target_id.clone()));
        if let Some(badge) = config
            .targets
            .get(target_id)
            .and_then(|target| permission_badge(target.permission_mode()))
        {
            spans.push(Span::raw(" "));
            spans.push(badge);
        }
    }
    Line::from(spans)
}

fn prefixed_summary_line(
    prefix: &str,
    label: &str,
    message: Option<&str>,
    width: usize,
    muted: bool,
) -> Line<'static> {
    let message = message.unwrap_or("No messages yet");
    let flattened = message.split_whitespace().collect::<Vec<_>>().join(" ");
    let lead = format!("{prefix}{label}");
    let line = Line::from(vec![
        Span::raw(prefix.to_owned()),
        Span::styled(
            label.to_owned(),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw(crate::widgets::truncate_text(
            &flattened,
            width.saturating_sub(lead.chars().count()),
        )),
    ]);
    if muted {
        line.style(Style::default().fg(Color::DarkGray))
    } else {
        line
    }
}

fn format_elapsed(elapsed: u64) -> String {
    format!(
        "{:02}:{:02}:{:02}",
        elapsed / 3_600,
        (elapsed % 3_600) / 60,
        elapsed % 60
    )
}

pub(crate) fn render_session_scrollbar(
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

#[cfg(test)]
fn session_values(
    session: &SessionRecord,
    detail: Option<&SessionDetail>,
    operation: Option<&SessionOperationDisplay>,
    now_epoch_seconds: u64,
    config: &HelConfig,
) -> (String, String, String, String, String) {
    let clock = if let Some(operation) = operation {
        let (label, started_at) = operation_status(operation);
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
    // An in-flight resume already told the controller its destination; show
    // that instead of the session record, which the dashboard won't refresh
    // until the operation finishes (see `SessionOperationDisplay::resume_destination`).
    let (profile_id, target_template_id) = operation
        .and_then(|operation| operation.resume_destination.clone())
        .unwrap_or_else(|| {
            (
                session.last_profile.clone(),
                session.target_template_id.clone(),
            )
        });
    (
        clock,
        profile_id,
        target_template_id,
        session.project_name(config),
        session_name(session).to_string(),
    )
}

fn operation_status(operation: &SessionOperationDisplay) -> (String, u64) {
    if matches!(
        operation.kind,
        SessionOperationKind::Launching | SessionOperationKind::Resuming
    ) && !operation.active_stages.is_empty()
    {
        let label = operation
            .active_stages
            .keys()
            .map(|stage| stage.label())
            .collect::<Vec<_>>()
            .join(", ");
        let started_at = operation
            .active_stages
            .values()
            .copied()
            .min()
            .unwrap_or(operation.started_at_epoch_seconds);
        (label, started_at)
    } else {
        (
            operation.kind.label().to_owned(),
            operation.started_at_epoch_seconds,
        )
    }
}

fn session_updated_at_epoch_seconds(session: &SessionRecord) -> Option<u64> {
    chrono::DateTime::parse_from_rfc3339(&session.updated_at)
        .ok()?
        .timestamp()
        .try_into()
        .ok()
}

fn session_name(session: &SessionRecord) -> &str {
    session.display_title()
}

/// Color of an active session's summary band. An unreachable target is red so
/// it stands out; otherwise unread sessions are highlighted and the rest keep
/// the default. A session whose detail has not loaded yet keeps the default.
fn session_band_color(detail: Option<&SessionDetail>, unreachable: bool) -> Color {
    if unreachable {
        return Color::Red;
    }
    match detail {
        Some(detail) if detail.has_unread() && detail.current_turn_started_at.is_none() => {
            Color::LightBlue
        }
        Some(detail) if detail.has_unread() => Color::LightYellow,
        // ANSI yellow is the orange/amber ink in common terminal palettes;
        // bright yellow remains distinct for unread sessions.
        _ => Color::Yellow,
    }
}

#[cfg(test)]
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

#[cfg(test)]
pub(crate) fn unread_line(unread_count: usize) -> Line<'static> {
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

/// A reading older than this stopped tracking the host: the poller samples
/// every 30 seconds, so three missed rounds mean the number on screen is no
/// longer what the host is doing.
const CAPACITY_SAMPLE_STALE_AFTER_SECONDS: u64 = 90;

/// Why the row's reading cannot be trusted, if it cannot: a probe that failed,
/// or a sample that stopped refreshing. `None` means the reading is current.
fn capacity_staleness(detail: &CapacityDetail, now_epoch_seconds: u64) -> Option<String> {
    if let Some(error) = &detail.probe_error {
        return Some(format!("stale: {error}"));
    }
    let sampled_at = detail.sampled_at_epoch_seconds?;
    (now_epoch_seconds.saturating_sub(sampled_at) > CAPACITY_SAMPLE_STALE_AFTER_SECONDS).then(
        || {
            format!(
                "stale: sampled {}",
                refresh_age(now_epoch_seconds, sampled_at)
            )
        },
    )
}

fn render_capacity(frame: &mut Frame, area: Rect, dashboard: &mut DashboardState) {
    let now_epoch_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let rows = dashboard.capacity_details.values().map(|detail| {
        let capacity = if detail.refreshing {
            "refreshing…".into()
        } else {
            match (&detail.target.kind, &detail.usage) {
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
            }
        };
        let mut in_use = vec![Span::raw(capacity)];
        if let Some(staleness) = capacity_staleness(detail, now_epoch_seconds) {
            in_use.push(Span::styled(
                format!("  · {staleness}"),
                Style::default().fg(Color::DarkGray),
            ));
        }
        Row::new([
            Cell::from(detail.target.host.clone()),
            Cell::from(capacity_target_labels(
                &detail.target.target_ids,
                &dashboard.config,
            )),
            Cell::from(Line::from(in_use)),
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

const EMPTY_QUOTA_COLOR: Color = Color::DarkGray;
const EMPTY_QUOTA_CELL: &str = "░";
// Both bar kinds occupy the same column, so they must agree on the cell count.
const QUOTA_BAR_CELLS: usize = 10;

fn quota_bar(window: Option<&QuotaWindow>) -> Line<'static> {
    const CELLS: usize = QUOTA_BAR_CELLS;
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
            EMPTY_QUOTA_CELL.repeat(empty_cells),
            Style::default().fg(EMPTY_QUOTA_COLOR),
        ),
        Span::styled(
            format!(" {remaining:>3}%"),
            Style::default().fg(color).add_modifier(Modifier::BOLD),
        ),
    ])
}

/// Renders the API label centered in a field of the same depleted-quota
/// shading the bars use, with the label cells left unshaded.
///
/// The depleted bar has no background color to copy: it is `EMPTY_QUOTA_COLOR`
/// seen through a glyph that covers about a quarter of each cell, so its
/// apparent shade exists only in the eye. Reusing the glyph reproduces that
/// shade exactly under any terminal theme or font, which a fixed color cannot.
fn api_quota_bar() -> Line<'static> {
    let label = hel::hel_quota::API_LABEL;
    let label_cells = label.chars().count().min(QUOTA_BAR_CELLS);
    let left = (QUOTA_BAR_CELLS - label_cells) / 2;
    let right = QUOTA_BAR_CELLS - label_cells - left;
    let shading = Style::default().fg(EMPTY_QUOTA_COLOR);
    Line::from(vec![
        Span::styled(EMPTY_QUOTA_CELL.repeat(left), shading),
        Span::styled(label, Style::default().add_modifier(Modifier::BOLD)),
        Span::styled(EMPTY_QUOTA_CELL.repeat(right), shading),
    ])
}

fn weekly_quota_exhausted(quota: &ProfileQuota) -> bool {
    quota
        .weekly_window()
        .and_then(quota_remaining_percent)
        .is_some_and(|remaining| remaining < 1)
}

fn five_hour_quota_bar(quota: &ProfileQuota) -> Line<'static> {
    let five_hour = if weekly_quota_exhausted(quota) {
        None
    } else {
        quota.five_hour_window()
    };
    quota_bar(five_hour)
}

fn quota_reset_countdown(now: u64, reset_at_epoch_seconds: i64) -> String {
    let Ok(reset) = u64::try_from(reset_at_epoch_seconds) else {
        return "now".into();
    };
    let remaining = reset.saturating_sub(now);
    if remaining == 0 {
        return "now".into();
    }

    const MINUTE: u64 = 60;
    const HOUR: u64 = 60 * MINUTE;
    const DAY: u64 = 24 * HOUR;
    if remaining >= DAY {
        let days = remaining / DAY;
        let hours = remaining % DAY / HOUR;
        if days == 1 && hours > 0 {
            format!("{days}d{hours}h")
        } else {
            format!("{days}d")
        }
    } else if remaining >= HOUR {
        let hours = remaining / HOUR;
        let minutes = remaining % HOUR / MINUTE;
        if hours == 1 && minutes > 0 {
            format!("{hours}h{minutes}m")
        } else {
            format!("{hours}h")
        }
    } else if remaining >= MINUTE {
        format!("{}m", remaining / MINUTE)
    } else {
        "<1m".into()
    }
}

fn quota_reset_cell(window: Option<&QuotaWindow>, now: u64) -> String {
    let Some(window) = window else {
        return String::new();
    };
    window
        .resets_at_epoch_seconds
        .map(|reset| quota_reset_countdown(now, reset))
        .or_else(|| window.resets.clone())
        .unwrap_or_default()
}

fn quota_reset_cells(quota: &ProfileQuota, now: u64) -> (String, String) {
    let mut weekly = quota_reset_cell(quota.weekly_window(), now);
    if let Some(extra) = quota.extra.as_deref() {
        if !weekly.is_empty() {
            weekly.push_str(" · ");
        }
        weekly.push_str(extra);
    }
    let five_hour = if weekly_quota_exhausted(quota) {
        String::new()
    } else {
        quota_reset_cell(quota.five_hour_window(), now)
    };
    (weekly, five_hour)
}

struct QuotaTableRow {
    profile: String,
    harness: String,
    weekly: Line<'static>,
    weekly_reset: String,
    five_hour: Line<'static>,
    five_hour_reset: String,
}

impl QuotaTableRow {
    fn into_row(self) -> Row<'static> {
        Row::new([
            Cell::from(self.profile),
            Cell::from(self.harness),
            Cell::from(self.weekly),
            Cell::from(self.weekly_reset),
            Cell::from(self.five_hour),
            Cell::from(self.five_hour_reset),
        ])
    }
}

fn quota_column_width(
    header: &str,
    content_widths: impl Iterator<Item = usize>,
    maximum: u16,
) -> u16 {
    let width = content_widths.fold(Line::raw(header).width(), usize::max);
    u16::try_from(width).unwrap_or(u16::MAX).min(maximum)
}

fn render_quotas(frame: &mut Frame, area: Rect, dashboard: &mut DashboardState) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let rows = dashboard
        .config
        .profiles
        .iter()
        .map(|(id, profile)| {
            let (weekly, weekly_reset, five_hour, five_hour_reset) =
                if profile.kind == HarnessKind::Deepseek {
                    (
                        api_quota_bar(),
                        String::new(),
                        Line::default(),
                        String::new(),
                    )
                } else if dashboard.quota_refreshing.contains(id) {
                    (
                        Line::raw("refreshing…"),
                        String::new(),
                        Line::default(),
                        String::new(),
                    )
                } else {
                    match dashboard.quotas.get(id) {
                        Some(quota) if quota.error.is_none() => {
                            let (weekly_reset, five_hour_reset) = quota_reset_cells(quota, now);
                            (
                                quota_bar(quota.weekly_window()),
                                weekly_reset,
                                five_hour_quota_bar(quota),
                                five_hour_reset,
                            )
                        }
                        Some(quota) => (
                            Line::raw(
                                quota
                                    .error_label()
                                    .unwrap_or_else(|| "unavailable: unknown error".into()),
                            ),
                            String::new(),
                            Line::default(),
                            String::new(),
                        ),
                        None => (
                            Line::raw("refreshing…"),
                            String::new(),
                            Line::default(),
                            String::new(),
                        ),
                    }
                };
            QuotaTableRow {
                profile: id.clone(),
                harness: profile.kind.display_name().into(),
                weekly,
                weekly_reset,
                five_hour,
                five_hour_reset,
            }
        })
        .collect::<Vec<_>>();
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
    let widths = [
        quota_column_width(
            "Profile",
            rows.iter()
                .map(|row| Line::raw(row.profile.as_str()).width()),
            24,
        ),
        quota_column_width(
            "Harness",
            rows.iter()
                .map(|row| Line::raw(row.harness.as_str()).width()),
            12,
        ),
        quota_column_width("Weekly", rows.iter().map(|row| row.weekly.width()), 32),
        quota_column_width(
            "Resets",
            rows.iter()
                .map(|row| Line::raw(row.weekly_reset.as_str()).width()),
            24,
        ),
        quota_column_width("5H", rows.iter().map(|row| row.five_hour.width()), 15),
        quota_column_width(
            "Resets",
            rows.iter()
                .map(|row| Line::raw(row.five_hour_reset.as_str()).width()),
            24,
        ),
    ]
    .map(Constraint::Length);
    let table = Table::new(rows.into_iter().map(QuotaTableRow::into_row), widths)
        .column_spacing(2)
        .header(
            Row::new(["Profile", "Harness", "Weekly", "Resets", "5H", "Resets"])
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
            "[N]ew · [S] Resume · [W]orkspaces · [E]dit · We[b] · mark [A]ll read · [Q]uit · Tab pane"
        }
        Focus::Capacity => "[W]orkspaces · [E]dit targets · [R]efresh · We[b] · [Q]uit · Tab pane",
        Focus::Quotas => "[W]orkspaces · [E]dit profile · [R]efresh · We[b] · [Q]uit · Tab pane",
    };
    frame.render_widget(
        Paragraph::new(vec![
            Line::styled(
                format!("{accelerator} for: {actions}"),
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
    use std::collections::BTreeMap;

    use crossterm::event::KeyCode;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use ratatui::style::Color;

    use hel::hel_config::{HarnessKind, HelConfig, ProjectRepository};
    use hel::hel_quota::{ProfileQuota, QuotaWindow};
    use hel::hel_state::{
        HelState, MaterializedExecutionState, STATE_VERSION, SessionState, TranscriptBody,
    };
    use hel::hel_targets::{DeploymentCapacityUsage, ProvisionStage};

    use super::*;
    use crate::test_support::*;

    use crate::ingest::SessionDetail;
    use crate::{DashboardAction, DashboardState, Focus, SessionOperationKind};

    #[test]
    fn grouped_dashboard_has_no_column_header_and_uses_fixed_session_summaries() {
        let mut dashboard = dashboard_with_session(running_session());
        apply_materialized_transcript(&mut dashboard, numbered_conversation(2));
        dashboard
            .session_details
            .get_mut("session-1")
            .unwrap()
            .queued_prompts
            .push(hel::hel_worker::QueuedPrompt {
                id: "queued-1".into(),
                text: "later".into(),
                attachments: Vec::new(),
                created_at_ms: 1,
            });
        let mut terminal = Terminal::new(TestBackend::new(120, 30)).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw dashboard");
        let rendered = buffer_lines(terminal.backend().buffer()).join("\n");

        assert!(rendered.contains("hel"));
        assert!(!rendered.contains("[1] hel"));
        assert!(!rendered.contains("Turn clock"));
        assert!(!rendered.contains("Session name"));
        assert!(rendered.contains("podman  [Q 1]  codex-1  ACP pretty name"));
        assert!(!rendered.contains("  Turn "));
        assert!(!rendered.contains("  Step "));
        assert!(rendered.contains("  codex-1  ACP pretty name"));
        assert!(!rendered.contains("queued]"));
        assert!(rendered.contains("You: question 1"));
        assert!(rendered.contains("T "));
        assert!(rendered.contains("S "));
        assert!(rendered.contains("answer 1"));

        let buffer = terminal.backend().buffer();
        let lines = buffer_lines(buffer);
        let (user_row, user_line) = lines
            .iter()
            .enumerate()
            .find(|(_, line)| line.contains("You: question 1"))
            .expect("user transcript line");
        let user_column = cell_column(user_line, "You: question 1");
        assert!((user_column..user_column + 15).all(|column| {
            buffer[(buffer.area.x + column, buffer.area.y + user_row as u16)].fg == Color::DarkGray
        }));
    }

    #[test]
    fn a_modal_overlays_the_dashboard_instead_of_replacing_it() {
        let mut dashboard = dashboard_with_session(running_session());
        dashboard.set_greeting("UNDERLYING DASHBOARD SENTINEL".into());
        assert_eq!(
            dashboard.handle_key(crate::test_support::ctrl_key('e')),
            DashboardAction::None
        );
        assert_eq!(
            dashboard.handle_key(crate::test_support::key(KeyCode::Enter)),
            DashboardAction::None
        );
        let mut terminal = Terminal::new(TestBackend::new(120, 30)).expect("terminal");

        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw rename dialog");
        let lines = buffer_lines(terminal.backend().buffer());

        let row_of = |needle: &str| {
            lines
                .iter()
                .position(|line| line.contains(needle))
                .unwrap_or_else(|| panic!("missing {needle} in {lines:#?}"))
        };
        let popup_top = row_of("Rename session");
        // The dashboard underneath still shows through every row the modal's
        // centred popup does not cover.
        assert!(row_of("UNDERLYING DASHBOARD SENTINEL") < popup_top);
        assert!(row_of(" Active ") < popup_top);
    }

    #[test]
    fn drawing_the_dashboard_registers_each_pane_interior_for_selection() {
        let mut dashboard = dashboard_with_session(running_session());
        let mut terminal = Terminal::new(TestBackend::new(120, 30)).expect("terminal");

        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw dashboard");

        let panes = dashboard.pane_areas.expect("dashboard pane hitboxes");
        let surfaces = dashboard.frame_surfaces();
        for (index, pane) in panes.iter().enumerate() {
            let id = SurfaceId::DashboardPane(index as u8);
            let surface = surfaces
                .surface(id)
                .unwrap_or_else(|| panic!("pane {index} registered"));
            assert_eq!(surface.rect, bordered_content(*pane));
            assert_eq!(
                surfaces
                    .surface_at(surface.rect.x, surface.rect.y)
                    .map(|surface| surface.id),
                Some(id)
            );
        }
        // The border rows and the scrollbar column stay out of every surface,
        // so a selection can never pick up their glyphs.
        assert!(surfaces.surface_at(panes[0].x, panes[0].y).is_none());
        assert!(
            surfaces
                .surface_at(panes[0].right() - 1, panes[0].y + 1)
                .is_none()
        );
    }

    #[test]
    fn an_open_dialog_registers_its_body_and_list_above_the_panes() {
        let mut dashboard = dashboard_with_session(stopped_session());
        dashboard.show_resume_dialog(1, Vec::new());
        let mut terminal = Terminal::new(TestBackend::new(120, 30)).expect("terminal");

        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw resume dialog");

        let surfaces = dashboard.frame_surfaces();
        let body = surfaces.surface(SurfaceId::ModalBody).expect("dialog body");
        let list = surfaces
            .surface(SurfaceId::ResumeList)
            .expect("session list");
        // The dialog covers the panes, and its list covers the dialog.
        assert_eq!(
            surfaces
                .surface_at(body.rect.x, body.rect.y)
                .map(|surface| surface.id),
            Some(SurfaceId::ModalBody)
        );
        assert_eq!(
            surfaces
                .surface_at(list.rect.x, list.rect.y)
                .map(|surface| surface.id),
            Some(SurfaceId::ResumeList)
        );
        // Beside the popup the pane underneath still owns its cells.
        assert_eq!(
            surfaces
                .surface_at(body.rect.x - 2, body.rect.y)
                .map(|surface| surface.id),
            Some(SurfaceId::DashboardPane(0))
        );
    }

    #[test]
    fn unanswered_user_line_stays_bright_and_shows_the_latest_agent_activity() {
        let mut dashboard = dashboard_with_session(running_session());
        let mut transcript = numbered_conversation(1);
        transcript.push(transcript_item(
            3,
            TranscriptBody::User {
                content: vec![serde_json::json!({
                    "type": "text",
                    "text": "unanswered follow-up"
                })],
            },
        ));
        transcript.push(thought(4, "Checking the workspace"));
        apply_materialized_transcript(&mut dashboard, transcript);
        let mut terminal = Terminal::new(TestBackend::new(120, 30)).expect("terminal");

        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw dashboard");
        let buffer = terminal.backend().buffer();
        let lines = buffer_lines(buffer);
        let rendered = lines.join("\n");

        assert!(rendered.contains("You: unanswered follow-up"));
        assert!(rendered.contains("│ Checking the workspace"), "{rendered}");
        assert!(!rendered.contains("Agent:"));
        assert!(!rendered.contains("answer 0"));
        let (user_row, user_line) = lines
            .iter()
            .enumerate()
            .find(|(_, line)| line.contains("You: unanswered follow-up"))
            .expect("user transcript line");
        let user_column = cell_column(user_line, "You: unanswered follow-up");
        assert_ne!(
            buffer[(buffer.area.x + user_column, buffer.area.y + user_row as u16)].fg,
            Color::DarkGray
        );
    }

    #[test]
    fn dashboard_agent_prefixes_show_active_clocks_and_idle_activity_time() {
        let detail = SessionDetail {
            current_turn_started_at: Some(1_000),
            last_acp_activity_at_ms: Some(1_297_000),
            ..SessionDetail::default()
        };

        assert_eq!(
            dashboard_agent_prefixes(1_330, Some(&detail)),
            ["T  5m30s", "S    33s"]
        );
        assert_eq!(compact_dashboard_clock(99 * 60 + 59), "99m59s");
        assert_eq!(compact_dashboard_clock(100 * 60 + 59), "100m");

        let idle = SessionDetail {
            last_activity_at_ms: Some(1_297_000),
            ..SessionDetail::default()
        };
        let activity_time = hel::hel_chat::format_event_time(Some(1_297_000)).unwrap();
        assert_eq!(
            dashboard_agent_prefixes(1_330, Some(&idle)),
            ["Agent:".to_owned(), format!("{activity_time:<6}")]
        );
    }

    #[test]
    fn idle_dashboard_moves_state_and_activity_time_beside_the_agent_excerpt() {
        let mut dashboard = dashboard_with_session(running_session());
        let mut materialized =
            materialized_session_for("session-1", vec![agent_message(2, "Finished work")]);
        materialized.execution = MaterializedExecutionState::Idle;
        dashboard.apply_materialized_session(&materialized);
        let activity_time = hel::hel_chat::format_event_time(Some(2_000)).unwrap();
        let mut terminal = Terminal::new(TestBackend::new(120, 30)).expect("terminal");

        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw dashboard");
        let rendered = buffer_lines(terminal.backend().buffer()).join("\n");

        assert!(!rendered.contains("[idle]"), "{rendered}");
        assert!(rendered.contains("Agent: │ Finished work"), "{rendered}");
        assert!(
            rendered.contains(&format!("{activity_time}  ")),
            "{rendered}"
        );
    }

    #[test]
    fn sessions_in_an_expanded_project_have_a_blank_row_and_only_the_caret_marks_selection() {
        let mut first = running_session();
        first.id = "session-first".into();
        first.project_directory = Some("/projects/shared".into());
        first.session_title_override = Some("First session".into());
        let mut second = running_session();
        second.id = "session-second".into();
        second.project_directory = Some("/projects/shared".into());
        second.session_title_override = Some("Second session".into());
        second.created_at = "2026-08-10T00:00:00Z".into();
        let state = HelState {
            version: STATE_VERSION,
            sessions: BTreeMap::from([(first.id.clone(), first), (second.id.clone(), second)]),
            mount_history: BTreeMap::new(),
            container_sizes: BTreeMap::new(),
        };
        let mut dashboard = DashboardState::new(config(), state, BTreeMap::new());
        let mut terminal = Terminal::new(TestBackend::new(120, 30)).expect("terminal");

        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw dashboard");

        let buffer = terminal.backend().buffer();
        let lines = buffer_lines(buffer);
        let first_y = lines
            .iter()
            .position(|line| line.contains("podman [1]"))
            .expect("first session row") as u16;
        let second_y = lines
            .iter()
            .position(|line| line.contains("podman [2]"))
            .expect("second session row") as u16;
        assert!(
            (first_y..first_y + 4).all(|y| {
                (buffer.area.x + 1..buffer.area.right() - 1)
                    .all(|x| buffer[(x, y)].bg != Color::DarkGray)
            }),
            "selection must not paint a background"
        );
        assert!(lines[first_y as usize].contains("› podman [1]"));
        assert_eq!(
            second_y,
            first_y + 5,
            "sessions in an expanded project have one blank row between them"
        );
        assert!(
            (buffer.area.x + 1..buffer.area.right() - 1)
                .all(|x| buffer[(x, first_y + 4)].symbol().trim().is_empty())
        );
    }

    #[test]
    fn project_groups_have_one_blank_row_between_them() {
        let mut first = running_session();
        first.id = "session-alpha".into();
        first.project_directory = Some("/projects/alpha".into());
        let mut second = running_session();
        second.id = "session-beta".into();
        second.project_directory = Some("/projects/beta".into());
        second.created_at = "2026-08-10T00:00:00Z".into();
        let state = HelState {
            version: STATE_VERSION,
            sessions: BTreeMap::from([(first.id.clone(), first), (second.id.clone(), second)]),
            mount_history: BTreeMap::new(),
            container_sizes: BTreeMap::new(),
        };
        let mut dashboard = DashboardState::new(config(), state, BTreeMap::new());
        let mut terminal = Terminal::new(TestBackend::new(120, 30)).expect("terminal");

        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw dashboard");

        let buffer = terminal.backend().buffer();
        let lines = buffer_lines(buffer);
        let first_y = lines
            .iter()
            .position(|line| line.contains("› podman"))
            .expect("first session row") as u16;
        let second_heading_y = lines
            .iter()
            .position(|line| line.contains("beta"))
            .expect("second project heading") as u16;
        let first_bottom = first_y + 4;
        assert_eq!(second_heading_y, first_bottom + 1);
        assert!(
            (buffer.area.x + 1..buffer.area.right() - 1)
                .all(|x| buffer[(x, first_bottom)].symbol().trim().is_empty())
        );
    }

    #[test]
    fn project_hotkey_expands_one_group_and_collapses_the_other() {
        let mut first = running_session();
        first.id = "session-alpha".into();
        first.project_directory = Some("/projects/alpha".into());
        let mut second = running_session();
        second.id = "session-beta".into();
        second.project_directory = Some("/projects/beta".into());
        second.created_at = "2026-08-10T00:00:00Z".into();
        let state = HelState {
            version: STATE_VERSION,
            sessions: BTreeMap::from([(first.id.clone(), first), (second.id.clone(), second)]),
            mount_history: BTreeMap::new(),
            container_sizes: BTreeMap::new(),
        };
        let mut dashboard = DashboardState::new(config(), state, BTreeMap::new());
        dashboard.apply_materialized_session(&materialized_session_for(
            "session-alpha",
            numbered_conversation(1),
        ));
        dashboard.apply_materialized_session(&materialized_session_for(
            "session-beta",
            vec![
                transcript_item(
                    1,
                    TranscriptBody::User {
                        content: vec![serde_json::json!({"type":"text","text":"beta question"})],
                    },
                ),
                agent_message(2, "beta answer"),
            ],
        ));
        let mut terminal = Terminal::new(TestBackend::new(120, 34)).expect("terminal");
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw first project");
        let first_draw = buffer_lines(terminal.backend().buffer()).join("\n");
        assert!(first_draw.contains("[1] alpha"));
        assert!(first_draw.contains("[2] beta"));
        assert!(first_draw.contains("You: question 0"));
        assert!(!first_draw.contains("You: beta question"));
        assert!(
            first_draw
                .lines()
                .any(|line| line.contains("podman  ") && line.contains("beta answer")),
            "{first_draw}"
        );

        assert_eq!(
            dashboard.handle_key(crate::test_support::key(KeyCode::Char('2'))),
            DashboardAction::None
        );
        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw second project");
        let second_draw = buffer_lines(terminal.backend().buffer()).join("\n");
        assert!(!second_draw.contains("You: question 0"));
        assert!(second_draw.contains("You: beta question"));
        assert_eq!(dashboard.selected_session().unwrap().id, "session-beta");
    }

    #[test]
    fn collapsed_duplicate_targets_are_numbered_within_their_project() {
        let mut alpha = running_session();
        alpha.id = "session-alpha".into();
        alpha.project_directory = Some("/projects/alpha".into());
        let mut beta_first = running_session();
        beta_first.id = "session-beta-first".into();
        beta_first.project_directory = Some("/projects/beta".into());
        beta_first.created_at = "2026-08-10T00:00:00Z".into();
        let mut beta_second = beta_first.clone();
        beta_second.id = "session-beta-second".into();
        beta_second.created_at = "2026-08-11T00:00:00Z".into();
        let state = HelState {
            version: STATE_VERSION,
            sessions: [alpha, beta_first, beta_second]
                .into_iter()
                .map(|session| (session.id.clone(), session))
                .collect(),
            mount_history: BTreeMap::new(),
            container_sizes: BTreeMap::new(),
        };
        let mut dashboard = DashboardState::new(config(), state, BTreeMap::new());
        dashboard.apply_materialized_session(&materialized_session_for(
            "session-beta-first",
            vec![agent_message(1, "first tail")],
        ));
        dashboard.apply_materialized_session(&materialized_session_for(
            "session-beta-second",
            vec![agent_message(1, "second tail")],
        ));
        let mut terminal = Terminal::new(TestBackend::new(120, 34)).expect("terminal");

        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw collapsed duplicate targets");
        let rendered = buffer_lines(terminal.backend().buffer()).join("\n");

        assert!(
            rendered
                .lines()
                .any(|line| line.contains("podman [1]  ") && line.contains("first tail")),
            "{rendered}"
        );
        assert!(
            rendered
                .lines()
                .any(|line| line.contains("podman [2]  ") && line.contains("second tail")),
            "{rendered}"
        );
    }

    #[test]
    fn summary_band_colors_distinguish_normal_unread_and_unread_idle() {
        let normal = SessionDetail {
            current_turn_started_at: Some(1),
            ..SessionDetail::default()
        };
        assert_eq!(session_band_color(Some(&normal), false), Color::Yellow);

        let unread = SessionDetail {
            current_turn_started_at: Some(1),
            unread_agent_messages: 1,
            ..SessionDetail::default()
        };
        assert_eq!(session_band_color(Some(&unread), false), Color::LightYellow);

        let unread_idle = SessionDetail {
            unread_agent_messages: 1,
            ..SessionDetail::default()
        };
        assert_eq!(
            session_band_color(Some(&unread_idle), false),
            Color::LightBlue
        );

        let collapsed =
            collapsed_session_line("› ", "podman", Some(&unread_idle), false, 1, 80, None);
        assert_eq!(collapsed.style.fg, Some(Color::LightBlue));

        let restarted_idle = SessionDetail {
            unread_session_restarts: 1,
            ..SessionDetail::default()
        };
        assert_eq!(
            session_band_color(Some(&restarted_idle), false),
            Color::LightBlue
        );

        let restarted_running = SessionDetail {
            current_turn_started_at: Some(1),
            unread_session_restarts: 1,
            ..SessionDetail::default()
        };
        assert_eq!(
            session_band_color(Some(&restarted_running), false),
            Color::LightYellow
        );

        // An unreachable target is red, overriding every other state.
        assert_eq!(session_band_color(Some(&unread), true), Color::Red);
        assert_eq!(session_band_color(None, true), Color::Red);
        let unreachable_line =
            collapsed_session_line("› ", "podman", Some(&unread_idle), true, 1, 80, None);
        assert_eq!(unreachable_line.style.fg, Some(Color::Red));
    }

    #[test]
    fn pane_allocator_fills_complete_tables_then_gives_surplus_to_active() {
        let allocation = allocate_pane_heights(
            32,
            PaneHeights {
                active: 10,
                capacity: 5,
                quotas: 5,
            },
            PaneHeights {
                active: 4,
                capacity: 4,
                quotas: 4,
            },
            Focus::Quotas,
        );

        assert_eq!(
            allocation,
            PaneAllocation::Fits(PaneHeights {
                active: 19,
                capacity: 5,
                quotas: 5,
            })
        );
    }

    #[test]
    fn pane_allocator_grows_focus_then_active_when_tables_do_not_fit() {
        let full = PaneHeights {
            active: 20,
            capacity: 10,
            quotas: 10,
        };
        let minimized = PaneHeights {
            active: 5,
            capacity: 5,
            quotas: 5,
        };
        for (focus, expected) in [
            (
                Focus::Active,
                PaneHeights {
                    active: 22,
                    capacity: 5,
                    quotas: 5,
                },
            ),
            (
                Focus::Capacity,
                PaneHeights {
                    active: 17,
                    capacity: 10,
                    quotas: 5,
                },
            ),
            (
                Focus::Quotas,
                PaneHeights {
                    active: 17,
                    capacity: 5,
                    quotas: 10,
                },
            ),
        ] {
            assert_eq!(
                allocate_pane_heights(35, full, minimized, focus),
                PaneAllocation::Fits(expected)
            );
        }
    }

    #[test]
    fn active_two_row_minimum_counts_header_rows_spacer_and_borders() {
        assert_eq!(active_pane_height(&[5, 5], 2), 12);
        assert_eq!(active_pane_height(&[5], 1), 7);
        assert_eq!(active_pane_height(&[], 0), 2);
    }

    #[test]
    fn pane_allocator_reports_content_sensitive_minimum_height() {
        let heights = PaneHeights {
            active: 5,
            capacity: 5,
            quotas: 5,
        };
        assert_eq!(
            allocate_pane_heights(17, heights, heights, Focus::Active),
            PaneAllocation::TooSmall {
                required_frame_height: 18,
            }
        );
        assert!(matches!(
            allocate_pane_heights(18, heights, heights, Focus::Active),
            PaneAllocation::Fits(_)
        ));
    }

    #[test]
    fn dashboard_replaces_too_short_layout_with_required_height() {
        let mut dashboard = DashboardState::new(config(), HelState::default(), BTreeMap::new());
        let mut terminal = Terminal::new(TestBackend::new(120, 10)).expect("terminal");
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
        assert!(
            rendered.contains("at least 13 rows (currently 10)"),
            "{rendered:?}"
        );

        let mut terminal = Terminal::new(TestBackend::new(120, 13)).expect("terminal");
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
        let hotkeys = line(buffer.area.bottom() - 2);
        assert!(hotkeys.contains(&format!("{accelerator} for: [N]ew")));
        assert!(hotkeys.contains("mark [A]ll read"));
        assert!(!hotkeys.contains("[S]ort"));
        assert!(!hotkeys.contains("[D]elete"));
        assert!(line(buffer.area.bottom() - 1).contains("Transient dashboard message"));
    }

    #[test]
    fn read_idle_session_uses_the_normal_summary_color() {
        let mut session = stopped_session();
        session.state = SessionState::Running;
        // The detach cursor sits past the only agent message, so nothing is
        // unread; the band still brightens because no turn is in flight.
        session.viewed_through_event_ordinal = 1;
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
                .all(|x| buffer[(x, status_y)].fg == Color::Yellow)
        );
    }

    #[test]
    fn session_name_prefers_override_then_acp_title_then_hel_uuid() {
        let mut session = stopped_session();
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

    /// A capacity sample the poller keeps refreshing carries no clock column
    /// and no staleness marker: the number on screen is the current one.
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
            now_epoch_seconds(),
        );
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
    fn dashboard_colors_named_host_permission_badges() {
        let mut config = config();
        let container = match config.targets["podman"].clone() {
            hel::hel_config::TargetTemplate::LocalPodman { container } => container,
            _ => unreachable!(),
        };
        let ssh = |host: &str| hel::hel_config::SshConnection {
            host: host.into(),
            user: None,
            identity_file: None,
            extra_args: Vec::new(),
        };
        config.targets.insert(
            "precision-3260".into(),
            hel::hel_config::TargetTemplate::SshBare {
                ssh: ssh("precision-3260"),
                permissions: PermissionMode::Yolo,
                workspace_prefix: ".local/share/hel/workspaces".into(),
            },
        );
        config.targets.insert(
            "morannon-podman".into(),
            hel::hel_config::TargetTemplate::SshPodman {
                ssh: ssh("morannon"),
                container,
            },
        );
        config.targets.insert(
            "morannon-raw".into(),
            hel::hel_config::TargetTemplate::SshBare {
                ssh: ssh("morannon"),
                permissions: PermissionMode::Guardian,
                workspace_prefix: ".local/share/hel/workspaces".into(),
            },
        );
        let mut session = running_session();
        session.target_template_id = "precision-3260".into();
        session.project_directory = Some("/home/dev/hel".into());
        let state = HelState {
            version: STATE_VERSION,
            sessions: BTreeMap::from([(session.id.clone(), session)]),
            mount_history: BTreeMap::new(),
            container_sizes: BTreeMap::new(),
        };
        let mut dashboard = DashboardState::new(config, state, BTreeMap::new());
        let capacity_target =
            |host: &str, target_ids: &[&str]| hel::hel_targets::DeploymentCapacityTarget {
                id: format!("ssh:{host}"),
                host: host.into(),
                target_ids: target_ids.iter().map(|id| (*id).into()).collect(),
                kind: DeploymentCapacityKind::Host,
                local: false,
                probes: Vec::new(),
                probe_error: None,
            };
        dashboard.set_deployment_capacity_targets(vec![
            capacity_target("precision-3260", &["precision-3260"]),
            capacity_target("morannon", &["morannon-podman", "morannon-raw"]),
        ]);
        let mut terminal = Terminal::new(TestBackend::new(140, 40)).expect("terminal");

        terminal
            .draw(|frame| render(frame, &mut dashboard))
            .expect("draw dashboard");

        let buffer = terminal.backend().buffer();
        let lines = buffer_lines(buffer);
        let badge_has_color = |needle: &str, color: Color| {
            lines.iter().enumerate().any(|(row, line)| {
                let Some(byte) = line.find(needle) else {
                    return false;
                };
                let x = buffer.area.x + line[..byte].chars().count() as u16;
                (x..x + 3).all(|x| buffer[(x, buffer.area.y + row as u16)].fg == color)
            })
        };
        let rendered = lines.join("\n");
        assert!(rendered.contains("precision-3260 [Y]"), "{rendered}");
        assert!(
            rendered.contains("morannon-podman, morannon-raw [G]"),
            "{rendered}"
        );
        assert!(!rendered.contains("morannon-podman [G]"), "{rendered}");
        assert!(badge_has_color("[Y]", Color::Red), "{rendered}");
        assert!(badge_has_color("[G]", Color::Green), "{rendered}");
    }

    fn now_epoch_seconds() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    fn host_capacity_usage() -> DeploymentCapacityUsage {
        DeploymentCapacityUsage {
            cpu_percent: Some(37),
            memory_used_bytes: 3,
            memory_total_bytes: 4,
            logical_cores: 8,
            disk_total_bytes: None,
        }
    }

    fn drawn_dashboard(dashboard: &mut DashboardState, width: u16) -> String {
        let mut terminal = Terminal::new(TestBackend::new(width, 40)).expect("terminal");
        terminal
            .draw(|frame| render(frame, dashboard))
            .expect("draw dashboard");
        buffer_lines(terminal.backend().buffer()).join("\n")
    }

    /// A probe that failed and a reading that stopped refreshing both keep the
    /// last numbers on screen and say why they cannot be trusted, instead of
    /// rendering exactly like a reading taken a moment ago.
    #[test]
    fn capacity_rows_mark_a_failed_probe_and_a_sample_that_stopped_refreshing() {
        let mut failed = DashboardState::new(config(), HelState::default(), BTreeMap::new());
        failed.set_deployment_capacity_targets(vec![test_capacity_target()]);
        failed.apply_deployment_capacity(
            "local",
            Ok(Some(host_capacity_usage())),
            now_epoch_seconds(),
        );
        failed.apply_deployment_capacity(
            "local",
            Err("probe timed out".into()),
            now_epoch_seconds(),
        );
        let rendered = drawn_dashboard(&mut failed, 200);
        assert!(rendered.contains("37% CPU · 75% RAM"), "{rendered}");
        assert!(rendered.contains("stale: probe timed out"), "{rendered}");

        let mut aged = DashboardState::new(config(), HelState::default(), BTreeMap::new());
        aged.set_deployment_capacity_targets(vec![test_capacity_target()]);
        aged.apply_deployment_capacity(
            "local",
            Ok(Some(host_capacity_usage())),
            now_epoch_seconds().saturating_sub(3_600),
        );
        let rendered = drawn_dashboard(&mut aged, 200);
        assert!(rendered.contains("stale: sampled 1h ago"), "{rendered}");
    }

    #[test]
    fn selected_transcript_tail_adapts_to_a_constrained_terminal() {
        let mut session = stopped_session();
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
        assert!(rendered.contains("Capacity"));
        assert!(rendered.contains("Profile Quotas"));
    }

    #[test]
    fn overflowing_session_pane_shows_a_scrollbar() {
        let mut sessions = BTreeMap::new();
        for index in 0..6 {
            let mut session = stopped_session();
            session.id = format!("active-{index:02}");
            session.state = SessionState::Running;
            sessions.insert(session.id.clone(), session);
        }
        for index in 0..20 {
            let mut session = stopped_session();
            session.id = format!("archived-{index:02}");
            sessions.insert(session.id.clone(), session);
        }
        let state = HelState {
            version: STATE_VERSION,
            sessions,
            mount_history: BTreeMap::new(),
            container_sizes: BTreeMap::new(),
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
        let mut dashboard = dashboard_with_session(stopped_session());
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
        let mut session = stopped_session();
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
        let mut session = stopped_session();
        session.state = SessionState::Running;
        let detail = SessionDetail {
            last_activity_at_ms: Some(1_000_000),
            ..SessionDetail::default()
        };

        let (clock, _, _, _, _) = session_values(&session, Some(&detail), None, 1_480, &config());
        assert_eq!(clock, "[idle]");
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
        let mut session = stopped_session();
        session.state = SessionState::Provisioning;
        session.updated_at = "1970-01-01T00:16:40Z".into();

        let (clock, _, _, _, _) = session_values(&session, None, None, 1_012, &config());
        assert_eq!(clock, "Launch 12s");
    }

    #[test]
    fn launch_clock_names_the_reported_stage() {
        let session = stopped_session();
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
        let session = stopped_session();
        let operation = operation(SessionOperationKind::Launching, None);

        let (clock, _, _, _, _) =
            session_values(&session, None, Some(&operation), 1_012, &config());
        assert_eq!(clock, "Launch 12s");
    }

    #[test]
    fn a_stage_does_not_rename_a_non_launch_operation() {
        let session = stopped_session();
        let operation = operation(
            SessionOperationKind::Stopping,
            Some(ProvisionStage::Syncing),
        );

        let (clock, _, _, _, _) =
            session_values(&session, None, Some(&operation), 1_012, &config());
        assert_eq!(clock, "Stopping 12s");
    }

    #[test]
    fn resuming_row_shows_the_destination_profile_and_target_not_the_stale_record() {
        // The controller updates the session's own last_profile/target as
        // soon as a resume starts, but the dashboard's local session
        // snapshot only refreshes once the operation finishes. The in-flight
        // row must show where the resume is going, not where it came from.
        let session = stopped_session();
        assert_eq!(session.last_profile, "codex-1");
        assert_eq!(session.target_template_id, "podman");
        let mut resuming = operation(SessionOperationKind::Resuming, None);
        resuming.resume_destination = Some(("grok-1".into(), "localhost".into()));

        let (_, profile_id, target_template_id, _, _) =
            session_values(&session, None, Some(&resuming), 1_012, &config());

        assert_eq!(profile_id, "grok-1");
        assert_eq!(target_template_id, "localhost");
    }

    #[test]
    fn without_a_resume_destination_the_row_falls_back_to_the_session_record() {
        let session = stopped_session();
        let resuming = operation(SessionOperationKind::Resuming, None);

        let (_, profile_id, target_template_id, _, _) =
            session_values(&session, None, Some(&resuming), 1_012, &config());

        assert_eq!(profile_id, session.last_profile);
        assert_eq!(target_template_id, session.target_template_id);
    }

    #[test]
    fn stage_clock_counts_from_when_the_stage_began_not_the_operation() {
        let session = stopped_session();
        let mut operation = operation(
            SessionOperationKind::Launching,
            Some(ProvisionStage::Booting),
        );
        // The operation started at 1_000 but the stage only began at 1_040;
        // the clock must count from the stage, not the whole operation.
        operation
            .active_stages
            .insert(ProvisionStage::Booting, 1_040);

        let (clock, _, _, _, _) =
            session_values(&session, None, Some(&operation), 1_052, &config());
        assert_eq!(clock, "Boot 12s");
    }

    #[test]
    fn launch_clock_names_concurrent_stages_in_lifecycle_order() {
        let session = stopped_session();
        let mut operation = operation(SessionOperationKind::Launching, None);
        operation
            .active_stages
            .insert(ProvisionStage::Syncing, 1_003);
        operation
            .active_stages
            .insert(ProvisionStage::Cloning, 1_002);

        let (clock, _, _, _, _) =
            session_values(&session, None, Some(&operation), 1_012, &config());
        assert_eq!(clock, "Clone, Sync 10s");
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

        let (_, _, target, project, _) = session_values(&stopped_session(), None, None, 0, &config);
        assert_eq!(target, "podman");
        assert_eq!(project, "hel");
    }

    #[test]
    fn focused_panes_use_double_borders_without_focus_title_text() {
        let mut dashboard = dashboard_with_session(running_session());
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
        assert!(rendered.contains("╔ Active"));
        assert!(rendered.contains("┌ Capacity"));
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
        let mut first = stopped_session();
        first.id = "session-0".into();
        first.state = SessionState::Running;
        let mut second = stopped_session();
        second.state = SessionState::Running;
        let mut dashboard = DashboardState::new(
            config(),
            HelState {
                version: STATE_VERSION,
                sessions: BTreeMap::from([(first.id.clone(), first), (second.id.clone(), second)]),
                mount_history: BTreeMap::new(),
                container_sizes: BTreeMap::new(),
            },
            BTreeMap::new(),
        );
        dashboard.set_deployment_capacity_targets(vec![test_capacity_target()]);
        let backend = TestBackend::new(120, 40);
        let mut terminal = Terminal::new(backend).expect("terminal");
        let mut initial_name_columns = None;

        for expected_focus in [Focus::Active, Focus::Capacity, Focus::Quotas] {
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
                assert_eq!(name_columns[0], name_columns[1]);
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
    fn quota_render_shows_login_expired_without_unavailable_prefix() {
        let mut dashboard = DashboardState::new(
            config(),
            HelState::default(),
            BTreeMap::from([(
                "claude-1".into(),
                ProfileQuota {
                    profile_id: "claude-1".into(),
                    harness: HarnessKind::Claude,
                    windows: vec![],
                    extra: None,
                    error: Some("login expired".into()),
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
        assert!(rendered.contains("login expired"));
        assert!(!rendered.contains("unavailable: login expired"));
    }

    #[test]
    fn deepseek_quota_row_shows_api_without_bars_or_reset_dates() {
        let mut config = config();
        config.profiles.get_mut("codex-1").unwrap().kind = HarnessKind::Deepseek;
        let mut dashboard = DashboardState::new(config, HelState::default(), BTreeMap::new());
        dashboard.quota_refreshing.insert("codex-1".into());
        let mut terminal = Terminal::new(TestBackend::new(120, 28)).expect("terminal");
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

        assert!(rendered.contains("API"));
        assert!(!rendered.contains("API Pricing"));
        assert!(rendered.contains("DSH"));
        assert!(!rendered.contains("DeepSeek Harness"));
        assert!(!rendered.contains("unavailable"));
        assert!(!rendered.contains('%'));
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
        assert_eq!(bar.spans[2].style.bg, None);
        assert!(quota_bar(None).spans.is_empty());
    }

    #[test]
    fn api_quota_label_is_punched_into_the_depleted_bar_shading() {
        let api = api_quota_bar();
        let exhausted = quota_bar(Some(&QuotaWindow {
            label: "Week".into(),
            remaining_percent: Some(0),
            used: None,
            limit: None,
            resets: None,
            resets_at_epoch_seconds: None,
        }));
        let shading = &exhausted.spans[2];
        assert_eq!(shading.content, "░░░░░░░░░░");

        let rendered: String = api.spans.iter().map(|span| span.content.as_ref()).collect();
        assert_eq!(rendered, "░░░API░░░░");
        // The padding must be the bar's own glyph and color, so the two match
        // whatever the terminal maps them to.
        for padding in [&api.spans[0], &api.spans[2]] {
            assert_eq!(padding.style.fg, shading.style.fg);
            assert_eq!(padding.style.bg, shading.style.bg);
        }
        // A painted background would not match a glyph-shaded cell.
        assert!(api.spans.iter().all(|span| span.style.bg.is_none()));
    }

    #[test]
    fn quota_render_hides_five_hour_bar_and_reset_when_weekly_quota_is_exhausted() {
        let quota = ProfileQuota {
            profile_id: "codex-1".into(),
            harness: HarnessKind::Codex,
            windows: vec![
                QuotaWindow {
                    label: "Week".into(),
                    remaining_percent: Some(0),
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

        assert!(rendered.contains("0%"));
        assert!(!rendered.contains("70%"));
        assert!(!rendered.contains("4h"));
    }

    #[test]
    fn quota_reset_countdowns_use_a_second_unit_only_after_one_first_unit() {
        const MINUTE: u64 = 60;
        const HOUR: u64 = 60 * MINUTE;
        const DAY: u64 = 24 * HOUR;
        let now = 100;

        assert_eq!(
            quota_reset_countdown(now, (now + 2 * DAY + 5 * HOUR) as i64),
            "2d"
        );
        assert_eq!(
            quota_reset_countdown(now, (now + DAY + 5 * HOUR) as i64),
            "1d5h"
        );
        assert_eq!(
            quota_reset_countdown(now, (now + 2 * HOUR + 5 * MINUTE) as i64),
            "2h"
        );
        assert_eq!(
            quota_reset_countdown(now, (now + HOUR + 5 * MINUTE) as i64),
            "1h5m"
        );
        assert_eq!(
            quota_reset_countdown(now, (now + 35 * MINUTE) as i64),
            "35m"
        );
        assert_eq!(quota_reset_countdown(now, (now + 30) as i64), "<1m");
        assert_eq!(quota_reset_countdown(now, now as i64), "now");
    }

    #[test]
    fn weekly_and_five_hour_resets_are_independent() {
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

        assert_eq!(quota_reset_cells(&quota, 0), ("7d".into(), "4h".into()));
    }

    #[test]
    fn quota_render_uses_weekly_five_hour_and_reset_columns() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let now = i64::try_from(now).unwrap();
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
                    resets_at_epoch_seconds: Some(now + 2 * 24 * 60 * 60 + 30),
                },
                QuotaWindow {
                    label: "5H".into(),
                    remaining_percent: Some(70),
                    used: None,
                    limit: None,
                    resets: Some("14:00 Aug 13".into()),
                    resets_at_epoch_seconds: Some(now + 60 * 60 + 5 * 60 + 30),
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
        let lines = buffer_lines(terminal.backend().buffer());
        let rendered = lines.join("\n");

        assert!(rendered.contains("Weekly"));
        assert!(rendered.contains("5H"));
        assert_eq!(rendered.matches("Resets").count(), 2);
        assert!(rendered.contains("73%"));
        assert!(rendered.contains("70%"));
        assert!(rendered.contains("2d"));
        assert!(rendered.contains("1h5m"));
        assert!(!rendered.contains("09:00 Aug 20"));

        let row = lines
            .iter()
            .find(|line| line.contains("codex-1"))
            .expect("quota row");
        let weekly_percent = cell_column(row, "73%");
        let weekly_reset = cell_column(row, "2d");
        let five_hour_percent = cell_column(row, "70%");
        let five_hour_reset = cell_column(row, "1h5m");
        assert_eq!(weekly_reset, weekly_percent + 3 + 2);
        assert_eq!(five_hour_percent - 12, weekly_reset + 6 + 2);
        assert_eq!(five_hour_reset, five_hour_percent + 3 + 2);
    }
}
