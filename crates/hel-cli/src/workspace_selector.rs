//! Terminal workspace picker with a compact read-only dashboard preview.

use std::collections::BTreeMap;

use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Wrap};

use crate::TerminalGuard;
use crate::daemon::{WorkspaceListing, WorkspaceSnapshot};

pub(crate) struct SelectorWorkspace {
    pub listing: WorkspaceListing,
    pub snapshot: WorkspaceSnapshot,
}

pub(crate) enum SelectorOutcome {
    Select(String),
    Create(String),
    Rename { workspace_id: String, name: String },
    Delete(String),
    RecoverDraft(String),
    Cancel,
}

enum EditMode {
    Create,
    Rename { workspace_id: String },
}

pub(crate) fn select_workspace(
    workspaces: &[SelectorWorkspace],
    suggested_name: &str,
) -> Result<SelectorOutcome> {
    let mut terminal = TerminalGuard::enter()?;
    let mut selected = 0_usize;
    let mut editing: Option<EditMode> = None;
    let mut input = String::new();

    loop {
        terminal.terminal.draw(|frame| {
            let [body, footer] = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Min(4), Constraint::Length(1)])
                .areas(frame.area());
            let [left, right] = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(34), Constraint::Percentage(66)])
                .areas(body);

            let mut items = workspaces
                .iter()
                .map(|candidate| {
                    let attached = if candidate.listing.attached_pids.is_empty() {
                        String::new()
                    } else {
                        format!(
                            " [attached to {}]",
                            candidate
                                .listing
                                .attached_pids
                                .iter()
                                .map(u32::to_string)
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    };
                    ListItem::new(format!("{}{}", candidate.listing.workspace.name, attached))
                })
                .collect::<Vec<_>>();
            items.push(ListItem::new("＋ Create new"));
            let mut list_state = ListState::default().with_selected(Some(selected));
            frame.render_stateful_widget(
                List::new(items)
                    .block(Block::default().title(" Workspaces ").borders(Borders::ALL))
                    .highlight_symbol("› ")
                    .highlight_style(Style::default().add_modifier(Modifier::REVERSED)),
                left,
                &mut list_state,
            );

            let preview = if let Some(candidate) = workspaces.get(selected) {
                preview_lines(candidate)
            } else {
                vec![
                    Line::from("Create a durable workspace for a group of sessions."),
                    Line::from(""),
                    Line::from(format!("Suggested name: {suggested_name}")),
                ]
            };
            frame.render_widget(
                Paragraph::new(preview)
                    .block(Block::default().title(" Preview ").borders(Borders::ALL))
                    .wrap(Wrap { trim: false }),
                right,
            );

            let footer_text = match &editing {
                Some(EditMode::Create) => format!("Create workspace: {input}▌"),
                Some(EditMode::Rename { .. }) => format!("Rename workspace: {input}▌"),
                None => {
                    "↑↓ select  Enter open  N new  R rename  V recover draft  D delete  Esc cancel"
                        .into()
                }
            };
            frame.render_widget(Paragraph::new(footer_text), footer);
        })?;

        let Event::Key(key) = event::read()? else {
            continue;
        };
        if key.kind != KeyEventKind::Press {
            continue;
        }
        if let Some(mode) = &editing {
            match key.code {
                KeyCode::Esc => {
                    editing = None;
                    input.clear();
                }
                KeyCode::Enter if !input.trim().is_empty() => {
                    return Ok(match mode {
                        EditMode::Create => SelectorOutcome::Create(input),
                        EditMode::Rename { workspace_id } => SelectorOutcome::Rename {
                            workspace_id: workspace_id.clone(),
                            name: input,
                        },
                    });
                }
                KeyCode::Backspace => {
                    input.pop();
                }
                KeyCode::Char(character)
                    if !key
                        .modifiers
                        .intersects(crossterm::event::KeyModifiers::CONTROL)
                        && input.chars().count() < 64
                        && !character.is_control() =>
                {
                    input.push(character);
                }
                _ => {}
            }
            continue;
        }

        match key.code {
            KeyCode::Esc => return Ok(SelectorOutcome::Cancel),
            KeyCode::Up | KeyCode::Char('k') => {
                selected = selected.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                selected = (selected + 1).min(workspaces.len());
            }
            KeyCode::Enter if selected < workspaces.len() => {
                return Ok(SelectorOutcome::Select(
                    workspaces[selected].listing.workspace.id.clone(),
                ));
            }
            KeyCode::Enter | KeyCode::Char('n' | 'N') => {
                editing = Some(EditMode::Create);
                input = suggested_name.to_owned();
            }
            KeyCode::Char('r' | 'R') if selected < workspaces.len() => {
                editing = Some(EditMode::Rename {
                    workspace_id: workspaces[selected].listing.workspace.id.clone(),
                });
                input = workspaces[selected].listing.workspace.name.clone();
            }
            KeyCode::Char('d' | 'D') if selected < workspaces.len() => {
                return Ok(SelectorOutcome::Delete(
                    workspaces[selected].listing.workspace.id.clone(),
                ));
            }
            KeyCode::Char('v' | 'V') if selected < workspaces.len() => {
                if let Some(draft) = workspaces[selected].snapshot.drafts.first() {
                    return Ok(SelectorOutcome::RecoverDraft(draft.id.clone()));
                }
            }
            _ => {}
        }
    }
}

fn preview_lines(candidate: &SelectorWorkspace) -> Vec<Line<'static>> {
    let mut lines = vec![Line::from(vec![
        Span::styled(
            candidate.listing.workspace.name.clone(),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!(
            "  {} session{}",
            candidate.snapshot.sessions.len(),
            if candidate.snapshot.sessions.len() == 1 {
                ""
            } else {
                "s"
            }
        )),
    ])];
    if !candidate.listing.attached_pids.is_empty() {
        lines.push(Line::from(format!(
            "attached to {}",
            candidate
                .listing
                .attached_pids
                .iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }
    lines.push(Line::from(""));
    if !candidate.snapshot.drafts.is_empty() {
        lines.push(Line::from(format!(
            "{} recoverable draft{}  (V restores newest)",
            candidate.snapshot.drafts.len(),
            if candidate.snapshot.drafts.len() == 1 {
                ""
            } else {
                "s"
            }
        )));
        lines.push(Line::from(""));
    }

    let mut projects = BTreeMap::<&str, Vec<_>>::new();
    for session in candidate
        .snapshot
        .sessions
        .iter()
        .filter(|session| session.active)
    {
        projects
            .entry(session.project.as_str())
            .or_default()
            .push(session);
    }
    if projects.is_empty() {
        lines.push(Line::from("No active sessions"));
    } else {
        for (project, sessions) in projects {
            lines.push(Line::from(format!(
                "▸ {project}  {} active session{}",
                sessions.len(),
                if sessions.len() == 1 { "" } else { "s" }
            )));
        }
    }
    lines
}
