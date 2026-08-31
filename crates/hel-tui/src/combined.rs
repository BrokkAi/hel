//! The combined conversation surface.
//!
//! One screen holds all of Hel's terminal UI: a Sessions pane, the transcript
//! of the conversation on screen, the Prompt composer, and Targets and Quota
//! summaries under it, with a shared one-row footer. There is no second screen
//! to switch to, so nothing is ever hidden behind a navigation step.

use hel::hel_chat::{ActiveChat, ChatRegions};
use hel::hel_selection::{SurfaceFrame, SurfaceId};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, BorderType, Borders, Paragraph, Wrap};

use crate::render::{
    MINIMUM_TERMINAL_WIDTH, TerminalSizeRequirement, minimized_quota_line, minimized_targets_line,
    render_capacity, render_footer, render_modal, render_onboarding_surface, render_quotas,
    render_sessions, render_terminal_too_small, sessions_content_height,
};
use crate::resume::resume_sessions_pane;
use crate::widgets::bordered_content;
use crate::{DashboardState, Focus, Mode};

/// Rows the footer always keeps.
const FOOTER_HEIGHT: u16 = 1;
/// The fewest rows the transcript is worth drawing in.
const TRANSCRIPT_MINIMUM: u16 = 3;
/// A bordered composer with one row of text.
const PROMPT_MINIMUM: u16 = 3;
/// A border plus the two lines of create-or-resume guidance.
const EMPTY_PROMPT_HEIGHT: u16 = 4;
/// A bordered pane with one row of content.
const PANE_MINIMUM: u16 = 3;
/// The one-row form Targets and Quota collapse to.
const SUMMARY_ROW: u16 = 1;

/// How tall one band wants to be and how short it may get.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PaneBand {
    minimum: u16,
    full: u16,
    cap: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CombinedHeights {
    sessions: u16,
    transcript: u16,
    prompt: u16,
    targets: u16,
    quota: u16,
    footer: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CombinedAllocation {
    Fits(CombinedHeights),
    TooSmall { required_frame_height: u16 },
}

/// Divides the frame between the six bands.
///
/// Every band starts at its minimum, and the surplus is then spent in a fixed
/// order: the composer first, because it is where the user is typing; then
/// whichever pane has the keyboard; then Sessions, Targets and Quota; and the
/// transcript takes whatever is left. The caps stop any one pane from eating
/// a small screen — a project with thirty live sessions must not push the
/// conversation off it.
fn allocate_combined_heights(
    frame_height: u16,
    sessions: PaneBand,
    targets: PaneBand,
    quota: PaneBand,
    desired_prompt: u16,
    focus: Focus,
) -> CombinedAllocation {
    let required = sessions
        .minimum
        .saturating_add(TRANSCRIPT_MINIMUM)
        .saturating_add(PROMPT_MINIMUM)
        .saturating_add(targets.minimum)
        .saturating_add(quota.minimum)
        .saturating_add(FOOTER_HEIGHT);
    if frame_height < required {
        return CombinedAllocation::TooSmall {
            required_frame_height: required,
        };
    }
    let mut heights = CombinedHeights {
        sessions: sessions.minimum,
        transcript: TRANSCRIPT_MINIMUM,
        prompt: PROMPT_MINIMUM,
        targets: targets.minimum,
        quota: quota.minimum,
        footer: FOOTER_HEIGHT,
    };
    let mut surplus = frame_height.saturating_sub(required);
    let grow = |current: &mut u16, want: u16, surplus: &mut u16| {
        let step = (*surplus).min(want.saturating_sub(*current));
        *current = current.saturating_add(step);
        *surplus = surplus.saturating_sub(step);
    };
    grow(
        &mut heights.prompt,
        desired_prompt.min((frame_height / 3).max(PROMPT_MINIMUM)),
        &mut surplus,
    );
    match focus {
        Focus::Sessions => grow(
            &mut heights.sessions,
            sessions.full.min(sessions.cap),
            &mut surplus,
        ),
        Focus::Targets => grow(
            &mut heights.targets,
            targets.full.min(targets.cap),
            &mut surplus,
        ),
        Focus::Quota => grow(&mut heights.quota, quota.full.min(quota.cap), &mut surplus),
        Focus::Prompt => {}
    }
    grow(
        &mut heights.sessions,
        sessions.full.min(sessions.cap),
        &mut surplus,
    );
    grow(
        &mut heights.targets,
        targets.full.min(targets.cap),
        &mut surplus,
    );
    grow(&mut heights.quota, quota.full.min(quota.cap), &mut surplus);
    heights.transcript = heights.transcript.saturating_add(surplus);
    CombinedAllocation::Fits(heights)
}

fn support_band(full: u16, focused: bool, frame_height: u16, minimized: bool) -> PaneBand {
    if minimized {
        return PaneBand {
            minimum: SUMMARY_ROW,
            full: SUMMARY_ROW,
            cap: SUMMARY_ROW,
        };
    }
    let divisor = if focused { 3 } else { 4 };
    PaneBand {
        minimum: PANE_MINIMUM,
        full,
        cap: (frame_height / divisor).max(PANE_MINIMUM),
    }
}

/// Draws the whole combined surface: Sessions, the conversation, Prompt,
/// Targets, Quota, the footer, and any modal over the top.
///
/// `chat` is the conversation on screen, or `None` when the workspace has no
/// live session. `transcript_selected` says the selection engine still owns a
/// selection on the transcript, so its row space has to stay frozen for this
/// frame.
pub fn render_combined(
    frame: &mut Frame,
    dashboard: &mut DashboardState,
    chat: Option<&mut ActiveChat>,
    transcript_selected: bool,
) {
    dashboard.pane_areas = None;
    dashboard.session_row_areas.clear();
    dashboard.project_heading_areas.clear();
    dashboard.frame_surfaces.clear();
    dashboard.chat_transcript_area = None;
    dashboard.chat_prompt_area = None;
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
    if dashboard.config_is_empty() {
        render_onboarding_surface(frame, dashboard);
        return;
    }

    let minimized = dashboard.support_minimized();
    let focus = dashboard.focus();
    let sessions_cap = if focus == Focus::Sessions {
        area.height / 2
    } else {
        area.height / 3
    };
    let sessions = PaneBand {
        minimum: PANE_MINIMUM,
        full: sessions_content_height(dashboard, area.width).saturating_add(2),
        cap: sessions_cap.max(PANE_MINIMUM),
    };
    let targets = support_band(
        table_height(dashboard.capacity_details.len()),
        focus == Focus::Targets,
        area.height,
        minimized,
    );
    let quota = support_band(
        table_height(dashboard.config.profiles.len()),
        focus == Focus::Quota,
        area.height,
        minimized,
    );
    // With no conversation the prompt band holds the two-line guidance that
    // stands in for a composer, so it asks for the rows to show both.
    let desired_prompt = chat.as_ref().map_or(EMPTY_PROMPT_HEIGHT, |chat| {
        chat.desired_prompt_height(area.width)
    });
    let allocation =
        allocate_combined_heights(area.height, sessions, targets, quota, desired_prompt, focus);
    let heights = match allocation {
        CombinedAllocation::Fits(heights) => heights,
        CombinedAllocation::TooSmall {
            required_frame_height,
        } => {
            render_terminal_too_small(
                frame,
                area,
                TerminalSizeRequirement::Height(required_frame_height),
            );
            return;
        }
    };

    let bands = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(heights.sessions),
            Constraint::Length(heights.transcript),
            Constraint::Length(heights.prompt),
            Constraint::Length(heights.targets),
            Constraint::Length(heights.quota),
            Constraint::Length(heights.footer),
        ])
        .split(area);
    let (sessions_area, transcript_area, prompt_area, targets_area, quota_area, footer_area) =
        (bands[0], bands[1], bands[2], bands[3], bands[4], bands[5]);
    dashboard.pane_areas = Some([sessions_area, targets_area, quota_area]);

    let rendered = render_sessions(frame, sessions_area, dashboard);
    dashboard.session_row_areas = rendered.session_row_areas;
    dashboard.project_heading_areas = rendered.project_heading_areas;
    dashboard.frame_surfaces.push(SurfaceFrame::fixed(
        SurfaceId::DashboardPane(0),
        bordered_content(sessions_area),
    ));

    dashboard.chat_transcript_area = Some(transcript_area);
    dashboard.chat_prompt_area = Some(prompt_area);
    let prompt_focused = dashboard.prompt_has_focus();
    let chat_drew_footer = match chat {
        Some(chat) => {
            chat.draw_in(
                frame,
                ChatRegions {
                    transcript: transcript_area,
                    prompt: prompt_area,
                    footer: prompt_focused.then_some(footer_area),
                    overlay: area,
                },
                prompt_focused,
                transcript_selected,
            );
            // A modal inside the conversation owns the frame's interaction, so
            // the panes behind it stop being selectable rather than staying
            // reachable underneath.
            if chat.frame_surfaces_exclusive() {
                dashboard.frame_surfaces.replace_with(chat.frame_surfaces());
            } else {
                dashboard.frame_surfaces.append(chat.frame_surfaces());
            }
            prompt_focused
        }
        None => {
            render_empty_conversation(frame, transcript_area, prompt_area, prompt_focused);
            false
        }
    };

    // Targets and Quota keep their pane numbers whether they are full tables
    // or one-row summaries, so a click resolves to the same pane either way.
    if minimized {
        frame.render_widget(
            Paragraph::new(minimized_targets_line(dashboard, targets_area.width)),
            targets_area,
        );
        frame.render_widget(
            Paragraph::new(minimized_quota_line(dashboard, quota_area.width)),
            quota_area,
        );
        dashboard.frame_surfaces.push(SurfaceFrame::fixed(
            SurfaceId::DashboardPane(1),
            targets_area,
        ));
        dashboard
            .frame_surfaces
            .push(SurfaceFrame::fixed(SurfaceId::DashboardPane(2), quota_area));
    } else {
        render_capacity(frame, targets_area, dashboard);
        render_quotas(frame, quota_area, dashboard);
        dashboard.frame_surfaces.push(SurfaceFrame::fixed(
            SurfaceId::DashboardPane(1),
            bordered_content(targets_area),
        ));
        dashboard.frame_surfaces.push(SurfaceFrame::fixed(
            SurfaceId::DashboardPane(2),
            bordered_content(quota_area),
        ));
    }

    if !chat_drew_footer {
        render_footer(frame, footer_area, dashboard);
    }
    render_modal(frame, area, dashboard);
}

/// The bordered chrome that stands in for a conversation while the workspace
/// has no live session, with the two things the user can do about it.
fn render_empty_conversation(
    frame: &mut Frame,
    transcript_area: Rect,
    prompt_area: Rect,
    prompt_focused: bool,
) {
    frame.render_widget(
        Block::default()
            .borders(Borders::ALL)
            .title(" Conversation "),
        transcript_area,
    );
    let border = if prompt_focused {
        BorderType::Double
    } else {
        BorderType::Plain
    };
    frame.render_widget(
        Paragraph::new(vec![
            Line::raw("No live session in this workspace."),
            Line::raw("Press Tab for Sessions, then n to create or s to resume."),
        ])
        .style(Style::default().fg(Color::DarkGray))
        .wrap(Wrap { trim: true })
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(border)
                .title(" Prompt (no live session) "),
        ),
        prompt_area,
    );
}

/// A bordered table with a header row and `rows` data rows.
fn table_height(rows: usize) -> u16 {
    u16::try_from(rows)
        .unwrap_or(u16::MAX)
        .saturating_add(PANE_MINIMUM)
}
