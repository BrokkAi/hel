//! Dashboard rendering: pane layout, session tables, capacity, quotas, footer.

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Margin, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, BorderType, Borders, Cell, Clear, HighlightSpacing, Paragraph, Row, Scrollbar,
    ScrollbarOrientation, ScrollbarState, Table, TableState, Wrap,
};

use hel::hel_chat::render_agent_message_tail;
use hel::hel_config::HelConfig;
use hel::hel_quota::{ProfileQuota, QuotaWindow};
use hel::hel_state::{SessionRecord, SessionState};
use hel::hel_targets::DeploymentCapacityKind;

use crate::dialogs::{
    render_confirmation, render_container_editor, render_import_bundle_confirmation,
    render_import_progress, render_rename_editor, render_repository_origin,
};
use crate::ingest::{SessionDetail, SessionOperationDisplay, TranscriptHydration};
use crate::resume::{render_resume_dialog, resume_sessions_pane};
use crate::widgets::{focus_border, format_resource_bytes};
use crate::wizards::{render_new_wizard, render_resume_wizard};
use crate::{
    DASHBOARD_PANE_COUNT, DashboardState, Focus, Mode, SessionOperationKind, partition_sessions,
};

const ACTIVE_MESSAGE_LINES: usize = 4;

pub(crate) const SELECTED_TRANSCRIPT_LINES: usize = 10;

const SESSION_TABLE_CHROME_HEIGHT: u16 = 3;

pub(crate) const SUMMARY_RULE: &str = "─";

const DASHBOARD_FIXED_HEIGHT: u16 = 3;

pub fn render(frame: &mut Frame, dashboard: &mut DashboardState) {
    dashboard.pane_areas = None;
    dashboard.selected_preview_area = None;
    dashboard.active_row_areas.clear();
    let area = frame.area();
    if area.width < MINIMUM_TERMINAL_WIDTH {
        render_terminal_too_small(
            frame,
            area,
            TerminalSizeRequirement::Width(MINIMUM_TERMINAL_WIDTH),
        );
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

    match &dashboard.mode {
        Mode::New(wizard) => render_new_wizard(frame, area, dashboard, wizard),
        Mode::Resume(wizard) => render_resume_wizard(frame, area, dashboard, wizard),
        Mode::ResumeDialog(dialog) => render_resume_dialog(frame, area, dashboard, dialog),
        Mode::RepositoryOrigin(dialog) => render_repository_origin(frame, area, dialog),
        Mode::Rename(editor) => render_rename_editor(frame, area, editor),
        Mode::EditContainer(editor) => render_container_editor(frame, area, editor),
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
    let preview_width = inner.width.saturating_sub(4);
    // One ordering for the whole frame: the pane, the previews, and the row
    // widgets all read this list. Only live sessions are on the dashboard;
    // everything else is in the resume dialog.
    let active = partition_sessions(
        dashboard.state.sessions.values(),
        &dashboard.session_details,
        dashboard.session_order,
    )
    .0;
    let active_count = active.len();
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
    // A session mid-launch, mid-resume, or mid-stop has nothing worth
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
        capacity: plain_table_height(dashboard.capacity_details.len()),
        quotas: plain_table_height(dashboard.config.profiles.len()),
    };
    let minimized = PaneHeights {
        active: if dashboard.focus == Focus::Active {
            SESSION_TABLE_CHROME_HEIGHT
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
    let rendered_rows = render_sessions(
        frame,
        panes[0],
        dashboard,
        &active,
        &active_previews.previews,
    );
    dashboard.selected_preview_area = rendered_rows.selected_preview_area;
    dashboard.active_row_areas = rendered_rows.active_row_areas;
    render_capacity(frame, panes[1], dashboard);
    render_quotas(frame, panes[2], dashboard);
    render_footer(frame, fixed[2], dashboard);

    match &dashboard.mode {
        Mode::New(wizard) => render_new_wizard(frame, frame_area, dashboard, wizard),
        Mode::Resume(wizard) => render_resume_wizard(frame, frame_area, dashboard, wizard),
        Mode::ResumeDialog(dialog) => render_resume_dialog(frame, frame_area, dashboard, dialog),
        Mode::RepositoryOrigin(dialog) => render_repository_origin(frame, frame_area, dialog),
        Mode::Rename(editor) => render_rename_editor(frame, frame_area, editor),
        Mode::EditContainer(editor) => render_container_editor(frame, frame_area, editor),
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

/// A launching, resuming, or stopping session — or one caught mid-provisioning
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
                | SessionOperationKind::Stopping
        ),
        None => state == SessionState::Provisioning,
    }
}

/// What one frame asks of the active previews: the width they wrap to, which
/// row is selected, how far that row's preview is scrolled, and the line budget
/// the selected row may use.
struct PreviewRequest {
    pub(crate) width: u16,
    pub(crate) selected: Option<usize>,
    pub(crate) scroll: usize,
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
    pub(crate) previews: Vec<Vec<Line<'static>>>,
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
    selected_preview_area: Option<Rect>,
    active_row_areas: Vec<(usize, Rect)>,
}

/// Draws the Active pane and reports the selected session's preview hitbox and
/// the per-row mouse hitboxes.
fn render_sessions(
    frame: &mut Frame,
    active_area: Rect,
    dashboard: &DashboardState,
    active: &[&SessionRecord],
    active_previews: &[Vec<Line<'static>>],
) -> SessionRowsRendered {
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
    let mut active_row_areas = Vec::new();
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
        let row_rect = Rect::new(
            active_area.x.saturating_add(1),
            info_y,
            active_area.width.saturating_sub(2),
            detail_y
                .saturating_sub(info_y)
                .saturating_add(preview_height),
        );
        active_row_areas.push((index, row_rect));
        row_y = row_y.saturating_add(preview.len() as u16 + 1 + spacer);
    }
    render_session_scrollbar(
        frame,
        active_area,
        active.len(),
        active_offset,
        visible_sessions,
    );

    SessionRowsRendered {
        selected_preview_area,
        active_row_areas,
    }
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

/// Color of an active session's summary band. A session whose detail has not
/// loaded yet keeps the default.
fn session_band_color(detail: Option<&SessionDetail>) -> Color {
    hel::hel_chat::turn_band_color(
        detail.is_none_or(|detail| detail.current_turn_started_at.is_some()),
    )
}

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
    pub(crate) height: u16,
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
                    Line::raw(
                        quota
                            .error_label()
                            .unwrap_or_else(|| "unavailable: unknown error".into()),
                    ),
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
            "[N]ew · [T] Resume · [R]ename · [E]dit container · [S]ort · sto[P] · [D]elete · [U]pdate quotas · [Q]uit · Tab pane"
        }
        Focus::Capacity => "[N]ew · [T] Resume · [S]ort · [U]pdate quotas · [Q]uit · Tab pane",
        Focus::Quotas => {
            "[N]ew · [T] Resume · [R]efresh · [S]ort · [U]pdate quotas · [Q]uit · Tab pane"
        }
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
    use hel::hel_targets::{DeploymentCapacityUsage, ProvisionStage, SessionResourceUsage};

    use super::*;
    use crate::test_support::*;

    use crate::ingest::SessionDetail;
    use crate::{DashboardAction, DashboardState, Focus, SessionOperationKind};

    #[test]
    fn resuming_session_with_a_transcript_collapses_to_its_summary_line() {
        let mut session = stopped_session();
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
        let mut session = stopped_session();
        session.state = SessionState::Running;
        let mut dashboard = dashboard_with_session(session);
        apply_materialized_transcript(&mut dashboard, vec![agent_message(1, "Rendered answer")]);

        dashboard.begin_session_operation("session-1".into(), SessionOperationKind::Stopping, None);
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
        let mut session = stopped_session();
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
        assert_eq!(active_pane_height(&[5, 5], 2), 14);
        assert_eq!(active_pane_height(&[5], 1), 8);
        assert_eq!(active_pane_height(&[], 0), 3);
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
        let mut terminal = Terminal::new(TestBackend::new(120, 12)).expect("terminal");
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
        assert!(rendered.contains("at least 14 rows (currently 12)"));

        let mut terminal = Terminal::new(TestBackend::new(120, 14)).expect("terminal");
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
        // Sort is a Ctrl command now, so no order is displayed permanently.
        assert!(hotkeys.contains("[S]ort"));
        assert!(!hotkeys.contains("sort: "));
        assert!(line(buffer.area.bottom() - 1).contains("Transient dashboard message"));
    }

    #[test]
    fn active_status_row_leads_with_project_and_separates_clock_from_profile() {
        let mut session = stopped_session();
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
        let mut session = stopped_session();
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

    /// Active sessions whose previews differ in length, plus stopped sessions,
    /// capacity rows, and quota rows, so every pane contributes to the layout.
    fn mixed_fleet_dashboard() -> DashboardState {
        let sessions = (0..4)
            .map(|index| {
                let mut session = stopped_session();
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
    /// with capacity and quota rows. Both the row-sizing pass and the pass
    /// that renders previews feed these numbers.
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
                "pane 0: 0,1 120x27\n",
                "pane 1: 0,28 120x4\n",
                "pane 2: 0,32 120x6\n",
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
                "pane 0: 0,1 120x14\n",
                "pane 1: 0,15 120x4\n",
                "pane 2: 0,19 120x5\n",
                "preview: 3,4 116x10\n",
                "row 3: session summary\n",
                "row 5: answer 11\n",
                "row 7: question 12\n",
                "row 9: answer 12\n",
                "row 11: question 13\n",
                "row 13: answer 13",
            )
        );
    }

    #[test]
    fn active_session_renders_the_complete_last_agent_message() {
        let mut session = stopped_session();
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
        let mut session = stopped_session();
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
                    terminal_outputs: Vec::new(),
                    terminal_refs: Vec::new(),
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
        resuming.resume_destination = Some(("grok-1".into(), "raw-localhost".into()));

        let (_, profile_id, target_template_id, _, _) =
            session_values(&session, None, Some(&resuming), 1_012, &config());

        assert_eq!(profile_id, "grok-1");
        assert_eq!(target_template_id, "raw-localhost");
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
        operation.stage_started_at_epoch_seconds = Some(1_040);

        let (clock, _, _, _, _) =
            session_values(&session, None, Some(&operation), 1_052, &config());
        assert_eq!(clock, "Boot 12s");
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
