//! Modal editor for ACP form elicitations.

use std::collections::{BTreeMap, BTreeSet};

use crossterm::event::{KeyCode, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};

use crate::hel_elicitation::{
    ElicitationField, ElicitationFieldKind, ElicitationRequest, ElicitationResponse,
    ElicitationValue,
};

use super::rendering::sanitize_terminal_text;

#[derive(Debug, Clone)]
enum FieldValue {
    Text { value: String, cursor: usize },
    Single(Option<usize>),
    Multi(BTreeSet<usize>),
    Boolean(bool),
}

#[derive(Debug, Clone, Copy)]
struct DisplayField {
    field: usize,
    custom: Option<usize>,
}

#[derive(Debug, Clone)]
pub(super) struct ElicitationDialog {
    request: ElicitationRequest,
    values: Vec<FieldValue>,
    option_cursors: Vec<usize>,
    display_fields: Vec<DisplayField>,
    active_custom_fields: BTreeSet<usize>,
    focus: usize,
    error: Option<String>,
}

impl ElicitationDialog {
    pub(super) fn new(request: ElicitationRequest) -> Self {
        let values = request.fields.iter().map(default_value).collect::<Vec<_>>();
        let mut option_cursors = values
            .iter()
            .map(|value| match value {
                FieldValue::Single(Some(index)) => *index,
                FieldValue::Multi(selected) => selected.first().copied().unwrap_or(0),
                _ => 0,
            })
            .collect::<Vec<_>>();
        let (display_fields, active_custom_fields) = display_fields(&request, &values);
        for display in &display_fields {
            let Some(custom) = display.custom else {
                continue;
            };
            if active_custom_fields.contains(&custom)
                && let Some(option_count) = select_option_count(&request.fields[display.field])
            {
                option_cursors[display.field] = option_count;
            }
        }
        Self {
            request,
            values,
            option_cursors,
            display_fields,
            active_custom_fields,
            focus: 0,
            error: None,
        }
    }

    pub(super) fn id(&self) -> &str {
        &self.request.id
    }

    pub(super) fn request(&self) -> &ElicitationRequest {
        &self.request
    }

    pub(super) fn paste(&mut self, text: &str) {
        let text = sanitize_terminal_text(text);
        let Some((field, custom)) = self.editable_field() else {
            return;
        };
        if custom {
            self.active_custom_fields.insert(field);
        }
        if let Some(FieldValue::Text { value, cursor }) = self.values.get_mut(field) {
            value.insert_str(*cursor, &text);
            *cursor += text.len();
            self.error = None;
        }
    }

    pub(super) fn handle_key(
        &mut self,
        code: KeyCode,
        modifiers: KeyModifiers,
    ) -> Option<ElicitationResponse> {
        self.error = None;
        if code == KeyCode::Esc {
            return Some(ElicitationResponse::Cancel);
        }
        let field_count = self.display_fields.len();
        let focus_count = field_count + 3;
        match code {
            KeyCode::Tab if modifiers.contains(KeyModifiers::SHIFT) => {
                self.focus = self.focus.checked_sub(1).unwrap_or(focus_count - 1);
            }
            KeyCode::BackTab => {
                self.focus = self.focus.checked_sub(1).unwrap_or(focus_count - 1);
            }
            KeyCode::Tab => self.focus = (self.focus + 1) % focus_count,
            KeyCode::Enter if self.focus == field_count => {
                return self.accept();
            }
            KeyCode::Enter if self.focus == field_count + 1 => {
                return Some(ElicitationResponse::Decline);
            }
            KeyCode::Enter if self.focus == field_count + 2 => {
                return Some(ElicitationResponse::Cancel);
            }
            KeyCode::Enter => self.focus = (self.focus + 1).min(field_count),
            KeyCode::Up => self.move_option(-1),
            KeyCode::Down => self.move_option(1),
            KeyCode::Char(' ') => self.toggle_current(),
            KeyCode::Backspace => self.edit_text(TextEdit::Backspace),
            KeyCode::Delete => self.edit_text(TextEdit::Delete),
            KeyCode::Left => self.edit_text(TextEdit::MoveLeft),
            KeyCode::Right => self.edit_text(TextEdit::MoveRight),
            KeyCode::Home => self.edit_text(TextEdit::Home),
            KeyCode::End => self.edit_text(TextEdit::End),
            KeyCode::Char(character)
                if !modifiers.intersects(KeyModifiers::CONTROL | KeyModifiers::SUPER) =>
            {
                self.edit_text(TextEdit::Insert(character));
            }
            _ => {}
        }
        None
    }

    fn move_option(&mut self, delta: isize) {
        let Some(display) = self.display_fields.get(self.focus).copied() else {
            return;
        };
        let Some(option_count) = select_option_count(&self.request.fields[display.field]) else {
            return;
        };
        let row_count = option_count + usize::from(display.custom.is_some());
        if row_count == 0 {
            return;
        }
        let cursor = &mut self.option_cursors[display.field];
        *cursor = if delta.is_negative() {
            cursor.checked_sub(1).unwrap_or(row_count - 1)
        } else {
            (*cursor + 1) % row_count
        };
        if let FieldValue::Single(selected) = &mut self.values[display.field] {
            if *cursor == option_count {
                *selected = None;
                if let Some(custom) = display.custom {
                    self.active_custom_fields.insert(custom);
                }
            } else {
                *selected = Some(*cursor);
                if let Some(custom) = display.custom {
                    self.active_custom_fields.remove(&custom);
                }
            }
        }
    }

    fn toggle_current(&mut self) {
        if self.editable_field().is_some() {
            self.edit_text(TextEdit::Insert(' '));
            return;
        }
        let Some(display) = self.display_fields.get(self.focus).copied() else {
            return;
        };
        let value = &mut self.values[display.field];
        match value {
            FieldValue::Single(selected) => {
                *selected = Some(self.option_cursors[display.field]);
                if let Some(custom) = display.custom {
                    self.active_custom_fields.remove(&custom);
                }
            }
            FieldValue::Multi(selected) => {
                let index = self.option_cursors[display.field];
                if !selected.remove(&index) {
                    selected.insert(index);
                }
                if let Some(custom) = display.custom {
                    self.active_custom_fields.remove(&custom);
                }
            }
            FieldValue::Boolean(selected) => *selected = !*selected,
            FieldValue::Text { .. } => unreachable!("text fields are handled above"),
        }
    }

    fn edit_text(&mut self, edit: TextEdit) {
        let Some((field, custom)) = self.editable_field() else {
            return;
        };
        if custom {
            self.active_custom_fields.insert(field);
        }
        let Some(FieldValue::Text { value, cursor }) = self.values.get_mut(field) else {
            unreachable!("editable fields contain text values")
        };
        match edit {
            TextEdit::Insert(character) => {
                value.insert(*cursor, character);
                *cursor += character.len_utf8();
            }
            TextEdit::Backspace if *cursor > 0 => {
                let previous = previous_char_boundary(value, *cursor);
                value.replace_range(previous..*cursor, "");
                *cursor = previous;
            }
            TextEdit::Delete if *cursor < value.len() => {
                let next = next_char_boundary(value, *cursor);
                value.replace_range(*cursor..next, "");
            }
            TextEdit::MoveLeft => *cursor = previous_char_boundary(value, *cursor),
            TextEdit::MoveRight => *cursor = next_char_boundary(value, *cursor),
            TextEdit::Home => *cursor = 0,
            TextEdit::End => *cursor = value.len(),
            TextEdit::Backspace | TextEdit::Delete => {}
        }
    }

    fn accept(&mut self) -> Option<ElicitationResponse> {
        let mut content = BTreeMap::new();
        for (display_index, display) in self.display_fields.iter().copied().enumerate() {
            let field_index = display
                .custom
                .filter(|custom| self.active_custom_fields.contains(custom))
                .unwrap_or(display.field);
            let field = &self.request.fields[field_index];
            let value = &self.values[field_index];
            match validated_value(field, value) {
                Ok(Some(value)) => {
                    content.insert(field.id.clone(), value);
                }
                Ok(None) => {}
                Err(error) => {
                    self.error = Some(error);
                    self.focus = display_index;
                    if field_index != display.field
                        && let Some(option_count) =
                            select_option_count(&self.request.fields[display.field])
                    {
                        self.option_cursors[display.field] = option_count;
                    }
                    return None;
                }
            }
        }
        Some(ElicitationResponse::Accept { content })
    }

    fn editable_field(&self) -> Option<(usize, bool)> {
        let display = self.display_fields.get(self.focus)?;
        if matches!(self.values[display.field], FieldValue::Text { .. }) {
            return Some((display.field, false));
        }
        let custom = display.custom?;
        let option_count = select_option_count(&self.request.fields[display.field])?;
        (self.option_cursors[display.field] == option_count).then_some((custom, true))
    }
}

fn display_fields(
    request: &ElicitationRequest,
    values: &[FieldValue],
) -> (Vec<DisplayField>, BTreeSet<usize>) {
    let fields_by_id = request
        .fields
        .iter()
        .enumerate()
        .map(|(index, field)| (field.id.as_str(), index))
        .collect::<BTreeMap<_, _>>();
    let mut custom_by_owner = BTreeMap::new();
    let mut attached_custom = BTreeSet::new();
    for (custom, field) in request.fields.iter().enumerate() {
        let Some(owner) = field
            .custom_answer_for
            .as_deref()
            .and_then(|owner| fields_by_id.get(owner))
            .copied()
        else {
            continue;
        };
        if !matches!(field.kind, ElicitationFieldKind::Text { .. })
            || select_option_count(&request.fields[owner]).is_none()
            || custom_by_owner.contains_key(&owner)
        {
            continue;
        }
        custom_by_owner.insert(owner, custom);
        attached_custom.insert(custom);
    }
    let display_fields = request
        .fields
        .iter()
        .enumerate()
        .filter(|(index, _)| !attached_custom.contains(index))
        .map(|(field, _)| DisplayField {
            field,
            custom: custom_by_owner.get(&field).copied(),
        })
        .collect::<Vec<_>>();
    let active_custom_fields = attached_custom
        .into_iter()
        .filter(|index| {
            matches!(
                &values[*index],
                FieldValue::Text { value, .. } if !value.is_empty()
            )
        })
        .collect();
    (display_fields, active_custom_fields)
}

fn select_option_count(field: &ElicitationField) -> Option<usize> {
    match &field.kind {
        ElicitationFieldKind::SingleSelect { options, .. }
        | ElicitationFieldKind::MultiSelect { options, .. } => Some(options.len()),
        _ => None,
    }
}

enum TextEdit {
    Insert(char),
    Backspace,
    Delete,
    MoveLeft,
    MoveRight,
    Home,
    End,
}

fn default_value(field: &ElicitationField) -> FieldValue {
    match &field.kind {
        ElicitationFieldKind::Text { default, .. } => FieldValue::Text {
            value: default.clone().unwrap_or_default(),
            cursor: default.as_ref().map_or(0, String::len),
        },
        ElicitationFieldKind::SingleSelect { options, default } => FieldValue::Single(
            default
                .as_ref()
                .and_then(|default| options.iter().position(|option| option.value == *default)),
        ),
        ElicitationFieldKind::MultiSelect {
            options, default, ..
        } => FieldValue::Multi(
            options
                .iter()
                .enumerate()
                .filter_map(|(index, option)| default.contains(&option.value).then_some(index))
                .collect(),
        ),
        ElicitationFieldKind::Boolean { default } => FieldValue::Boolean(default.unwrap_or(false)),
        ElicitationFieldKind::Integer { default, .. } => FieldValue::Text {
            value: default.map(|value| value.to_string()).unwrap_or_default(),
            cursor: default.map(|value| value.to_string().len()).unwrap_or(0),
        },
        ElicitationFieldKind::Number { default, .. } => FieldValue::Text {
            value: default.map(|value| value.to_string()).unwrap_or_default(),
            cursor: default.map(|value| value.to_string().len()).unwrap_or(0),
        },
    }
}

fn validated_value(
    field: &ElicitationField,
    value: &FieldValue,
) -> Result<Option<ElicitationValue>, String> {
    let missing = || Err(format!("{} is required", field.title));
    match (&field.kind, value) {
        (
            ElicitationFieldKind::Text {
                min_length,
                max_length,
                pattern,
                format,
                ..
            },
            FieldValue::Text { value, .. },
        ) => {
            if value.is_empty() {
                return if field.required { missing() } else { Ok(None) };
            }
            let length = value.chars().count();
            if min_length.is_some_and(|minimum| length < minimum) {
                return Err(format!("{} is too short", field.title));
            }
            if max_length.is_some_and(|maximum| length > maximum) {
                return Err(format!("{} is too long", field.title));
            }
            if pattern.as_ref().is_some_and(|pattern| {
                !regex::Regex::new(pattern)
                    .expect("validated pattern")
                    .is_match(value)
            }) {
                return Err(format!(
                    "{} does not match the required format",
                    field.title
                ));
            }
            if let Some(format) = format {
                validate_text_format(value, format)
                    .map_err(|message| format!("{} {message}", field.title))?;
            }
            Ok(Some(ElicitationValue::String(value.clone())))
        }
        (ElicitationFieldKind::SingleSelect { options, .. }, FieldValue::Single(selected)) => {
            let Some(index) = selected else {
                return if field.required { missing() } else { Ok(None) };
            };
            Ok(Some(ElicitationValue::String(
                options[*index].value.clone(),
            )))
        }
        (
            ElicitationFieldKind::MultiSelect {
                options,
                min_items,
                max_items,
                ..
            },
            FieldValue::Multi(selected),
        ) => {
            if selected.is_empty() && field.required {
                return missing();
            }
            if min_items.is_some_and(|minimum| selected.len() < minimum) {
                return Err(format!("{} needs more selections", field.title));
            }
            if max_items.is_some_and(|maximum| selected.len() > maximum) {
                return Err(format!("{} has too many selections", field.title));
            }
            if selected.is_empty() {
                return Ok(None);
            }
            Ok(Some(ElicitationValue::StringArray(
                selected
                    .iter()
                    .map(|index| options[*index].value.clone())
                    .collect(),
            )))
        }
        (ElicitationFieldKind::Boolean { .. }, FieldValue::Boolean(value)) => {
            Ok(Some(ElicitationValue::Boolean(*value)))
        }
        (
            ElicitationFieldKind::Integer {
                minimum, maximum, ..
            },
            FieldValue::Text { value, .. },
        ) => {
            if value.is_empty() {
                return if field.required { missing() } else { Ok(None) };
            }
            let value = value
                .parse::<i64>()
                .map_err(|_| format!("{} must be an integer", field.title))?;
            if minimum.is_some_and(|minimum| value < minimum)
                || maximum.is_some_and(|maximum| value > maximum)
            {
                return Err(format!("{} is outside the allowed range", field.title));
            }
            Ok(Some(ElicitationValue::Integer(value)))
        }
        (
            ElicitationFieldKind::Number {
                minimum, maximum, ..
            },
            FieldValue::Text { value, .. },
        ) => {
            if value.is_empty() {
                return if field.required { missing() } else { Ok(None) };
            }
            let value = value
                .parse::<f64>()
                .map_err(|_| format!("{} must be a number", field.title))?;
            if !value.is_finite()
                || minimum.is_some_and(|minimum| value < minimum)
                || maximum.is_some_and(|maximum| value > maximum)
            {
                return Err(format!("{} is outside the allowed range", field.title));
            }
            Ok(Some(ElicitationValue::Number(value)))
        }
        _ => Err(format!("{} has an incompatible value", field.title)),
    }
}

fn validate_text_format(value: &str, format: &str) -> Result<(), &'static str> {
    match format {
        "email"
            if !value.split_once('@').is_some_and(|(local, domain)| {
                !local.is_empty() && domain.contains('.') && !domain.starts_with('.')
            }) =>
        {
            Err("must be an email address")
        }
        "uri" if url::Url::parse(value).is_err() => Err("must be a URI"),
        "date" if chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").is_err() => {
            Err("must be a YYYY-MM-DD date")
        }
        "date-time" if chrono::DateTime::parse_from_rfc3339(value).is_err() => {
            Err("must be an RFC 3339 date-time")
        }
        _ => Ok(()),
    }
}

pub(super) fn render_elicitation(frame: &mut Frame, dialog: &ElicitationDialog) {
    let area = centered_rect(frame.area(), 82, 78);
    frame.render_widget(Clear, area);
    let title = dialog.request.title.as_deref().unwrap_or("Agent question");
    let block = Block::default()
        .borders(Borders::ALL)
        .title(format!(" {title} "))
        .border_style(Style::default().fg(Color::Cyan));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let message_height = if dialog.request.id.starts_with("plan-review-") {
        Constraint::Percentage(45)
    } else {
        Constraint::Length(3)
    };
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            message_height,
            Constraint::Min(4),
            Constraint::Length(2),
            Constraint::Length(1),
        ])
        .split(inner);
    frame.render_widget(
        Paragraph::new(dialog.request.message.as_str()).wrap(Wrap { trim: true }),
        chunks[0],
    );
    render_focus(frame, chunks[1], dialog);
    let field_count = dialog.display_fields.len();
    let buttons = ["Submit", "Skip", "Cancel"]
        .into_iter()
        .enumerate()
        .flat_map(|(index, label)| {
            let selected = dialog.focus == field_count + index;
            [
                Span::styled(
                    format!(" {label} "),
                    if selected {
                        Style::default()
                            .fg(Color::Black)
                            .bg(Color::Cyan)
                            .add_modifier(Modifier::BOLD)
                    } else {
                        Style::default().fg(Color::Gray)
                    },
                ),
                Span::raw("  "),
            ]
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(Line::from(buttons)).alignment(Alignment::Center),
        chunks[2],
    );
    let footer = dialog
        .error
        .as_deref()
        .unwrap_or("Tab fields/buttons · ↑/↓ choose · Space toggle · Enter continue · Esc cancel");
    frame.render_widget(
        Paragraph::new(footer).style(Style::default().fg(if dialog.error.is_some() {
            Color::Red
        } else {
            Color::DarkGray
        })),
        chunks[3],
    );
}

fn render_focus(frame: &mut Frame, area: Rect, dialog: &ElicitationDialog) {
    let Some(display) = dialog.display_fields.get(dialog.focus).copied() else {
        let label = match dialog.focus.saturating_sub(dialog.display_fields.len()) {
            0 => "Submit these answers",
            1 => "Skip this question and let the agent continue",
            _ => "Cancel this question",
        };
        frame.render_widget(Paragraph::new(label).alignment(Alignment::Center), area);
        return;
    };
    let field = &dialog.request.fields[display.field];
    let required = if field.required { " (required)" } else { "" };
    let mut lines = vec![Line::from(vec![
        Span::styled(
            format!("{}/{}  ", dialog.focus + 1, dialog.display_fields.len()),
            Style::default().fg(Color::DarkGray),
        ),
        Span::styled(
            format!("{}{}", field.title, required),
            Style::default().add_modifier(Modifier::BOLD),
        ),
    ])];
    if let Some(description) = &field.description {
        lines.push(Line::styled(
            description.as_str(),
            Style::default().fg(Color::Gray),
        ));
    }
    let mut text_cursor = None;
    match (&field.kind, &dialog.values[display.field]) {
        (_, FieldValue::Text { value, .. }) => {
            let shown = if field.secret {
                "•".repeat(value.chars().count())
            } else {
                value.clone()
            };
            lines.push(Line::raw(""));
            let input_line = lines.len() as u16;
            lines.push(Line::styled(
                format!("> {shown}"),
                Style::default().fg(Color::Cyan),
            ));
            let FieldValue::Text { cursor, .. } = &dialog.values[display.field] else {
                unreachable!("text fields contain text values")
            };
            text_cursor = Some((input_line, value[..*cursor].chars().count()));
        }
        (ElicitationFieldKind::SingleSelect { options, .. }, FieldValue::Single(selected)) => {
            let custom_active = display
                .custom
                .is_some_and(|custom| dialog.active_custom_fields.contains(&custom));
            for (index, option) in options.iter().enumerate() {
                let cursor = dialog.option_cursors[display.field] == index;
                let marker = if !custom_active && *selected == Some(index) {
                    "●"
                } else {
                    "○"
                };
                let style = if cursor {
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                lines.push(Line::styled(format!("{marker} {}", option.title), style));
                if cursor {
                    if let Some(description) = &option.description {
                        lines.push(Line::styled(
                            format!("    {description}"),
                            Style::default().fg(Color::Gray),
                        ));
                    }
                    if let Some(preview) = &option.preview {
                        lines.push(Line::styled(
                            format!("    {preview}"),
                            Style::default().fg(Color::DarkGray),
                        ));
                    }
                }
            }
            render_custom_answer(
                &mut lines,
                &mut text_cursor,
                dialog,
                display,
                options.len(),
                "○",
                "●",
            );
        }
        (ElicitationFieldKind::MultiSelect { options, .. }, FieldValue::Multi(selected)) => {
            let custom_active = display
                .custom
                .is_some_and(|custom| dialog.active_custom_fields.contains(&custom));
            for (index, option) in options.iter().enumerate() {
                let marker = if !custom_active && selected.contains(&index) {
                    "☑"
                } else {
                    "☐"
                };
                let style = if dialog.option_cursors[display.field] == index {
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                lines.push(Line::styled(format!("{marker} {}", option.title), style));
            }
            render_custom_answer(
                &mut lines,
                &mut text_cursor,
                dialog,
                display,
                options.len(),
                "☐",
                "☑",
            );
        }
        (ElicitationFieldKind::Boolean { .. }, FieldValue::Boolean(selected)) => {
            lines.push(Line::styled(
                if *selected { "☑ Yes" } else { "☐ No" },
                Style::default().fg(Color::Cyan),
            ));
        }
        _ => {}
    }
    frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), area);
    if let Some((line, column)) = text_cursor
        && area.width > 2
        && area.height > 0
    {
        frame.set_cursor_position((
            area.x + 2 + (column as u16).min(area.width.saturating_sub(3)),
            area.y + line.min(area.height.saturating_sub(1)),
        ));
    }
}

fn render_custom_answer(
    lines: &mut Vec<Line<'_>>,
    text_cursor: &mut Option<(u16, usize)>,
    dialog: &ElicitationDialog,
    display: DisplayField,
    option_count: usize,
    unselected_marker: &str,
    selected_marker: &str,
) {
    let Some(custom_index) = display.custom else {
        return;
    };
    let custom = &dialog.request.fields[custom_index];
    let FieldValue::Text { value, cursor } = &dialog.values[custom_index] else {
        unreachable!("custom answer fields contain text values")
    };
    let focused = dialog.option_cursors[display.field] == option_count;
    let active = dialog.active_custom_fields.contains(&custom_index);
    let marker = if active {
        selected_marker
    } else {
        unselected_marker
    };
    let style = if focused {
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    };
    lines.push(Line::styled(format!("{marker} {}", custom.title), style));
    if !focused {
        return;
    }
    if let Some(description) = &custom.description {
        lines.push(Line::styled(
            format!("    {description}"),
            Style::default().fg(Color::Gray),
        ));
    }
    let shown = if custom.secret {
        "•".repeat(value.chars().count())
    } else {
        value.clone()
    };
    let input_line = lines.len() as u16;
    lines.push(Line::styled(
        format!("> {shown}"),
        Style::default().fg(Color::Cyan),
    ));
    *text_cursor = Some((input_line, value[..*cursor].chars().count()));
}

fn centered_rect(area: Rect, width_percent: u16, height_percent: u16) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - height_percent) / 2),
            Constraint::Percentage(height_percent),
            Constraint::Percentage((100 - height_percent) / 2),
        ])
        .split(area);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - width_percent) / 2),
            Constraint::Percentage(width_percent),
            Constraint::Percentage((100 - width_percent) / 2),
        ])
        .split(vertical[1])[1]
}

fn previous_char_boundary(value: &str, cursor: usize) -> usize {
    value[..cursor]
        .char_indices()
        .next_back()
        .map_or(0, |(index, _)| index)
}

fn next_char_boundary(value: &str, cursor: usize) -> usize {
    value[cursor..]
        .char_indices()
        .nth(1)
        .map_or(value.len(), |(index, _)| cursor + index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hel_elicitation::ElicitationOption;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    fn request(kind: ElicitationFieldKind, required: bool) -> ElicitationRequest {
        ElicitationRequest {
            id: "ask-1".into(),
            message: "Choose an architecture".into(),
            title: None,
            description: None,
            fields: vec![ElicitationField {
                id: "question_0".into(),
                title: "Architecture".into(),
                description: None,
                required,
                secret: false,
                custom_answer_for: None,
                kind,
            }],
        }
    }

    fn paired_request(question_count: usize, multi_select: bool) -> ElicitationRequest {
        let mut fields = Vec::new();
        for index in 0..question_count {
            let id = format!("question_{index}");
            let options = vec![
                ElicitationOption {
                    value: "alpha".into(),
                    title: "Alpha".into(),
                    description: Some("Choose alpha".into()),
                    preview: None,
                },
                ElicitationOption {
                    value: "beta".into(),
                    title: "Beta".into(),
                    description: Some("Choose beta".into()),
                    preview: None,
                },
            ];
            fields.push(ElicitationField {
                id: id.clone(),
                title: format!("Question {}", index + 1),
                description: Some(format!("Prompt {}", index + 1)),
                required: false,
                secret: false,
                custom_answer_for: None,
                kind: if multi_select {
                    ElicitationFieldKind::MultiSelect {
                        options,
                        default: Vec::new(),
                        min_items: None,
                        max_items: None,
                    }
                } else {
                    ElicitationFieldKind::SingleSelect {
                        options,
                        default: None,
                    }
                },
            });
            fields.push(ElicitationField {
                id: format!("{id}__other"),
                title: "Other".into(),
                description: Some(
                    "Type your own answer instead of choosing an option above.".into(),
                ),
                required: false,
                secret: false,
                custom_answer_for: Some(id),
                kind: ElicitationFieldKind::Text {
                    default: None,
                    min_length: None,
                    max_length: None,
                    pattern: None,
                    format: None,
                },
            });
        }
        ElicitationRequest {
            id: "ask-paired".into(),
            message: "Input requested".into(),
            title: None,
            description: None,
            fields,
        }
    }

    fn rendered(dialog: &ElicitationDialog) -> String {
        let mut terminal = Terminal::new(TestBackend::new(100, 30)).expect("terminal");
        terminal
            .draw(|frame| render_elicitation(frame, dialog))
            .expect("render elicitation");
        let buffer = terminal.backend().buffer();
        (buffer.area.y..buffer.area.bottom())
            .map(|y| {
                (buffer.area.x..buffer.area.right())
                    .map(|x| buffer[(x, y)].symbol())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn selecting_an_option_returns_its_wire_value() {
        let mut dialog = ElicitationDialog::new(request(
            ElicitationFieldKind::SingleSelect {
                options: vec![
                    ElicitationOption {
                        value: "thin".into(),
                        title: "Thin callers".into(),
                        description: None,
                        preview: None,
                    },
                    ElicitationOption {
                        value: "dynamic".into(),
                        title: "Dynamic matrix".into(),
                        description: None,
                        preview: None,
                    },
                ],
                default: None,
            },
            true,
        ));
        dialog.handle_key(KeyCode::Down, KeyModifiers::NONE);
        dialog.handle_key(KeyCode::Enter, KeyModifiers::NONE);

        assert_eq!(
            dialog.handle_key(KeyCode::Enter, KeyModifiers::NONE),
            Some(ElicitationResponse::Accept {
                content: BTreeMap::from([(
                    "question_0".into(),
                    ElicitationValue::String("dynamic".into())
                )])
            })
        );
    }

    #[test]
    fn paired_custom_answers_share_their_question_page() {
        let mut dialog = ElicitationDialog::new(paired_request(3, false));

        assert_eq!(dialog.display_fields.len(), 3);
        let first = rendered(&dialog);
        assert!(first.contains("1/3"));
        assert!(first.contains("○ Other"));
        assert!(!first.contains("1/6"));

        dialog.handle_key(KeyCode::Tab, KeyModifiers::NONE);
        let second = rendered(&dialog);
        assert!(second.contains("2/3"));
        assert!(second.contains("Question 2"));
    }

    #[test]
    fn custom_answer_uses_the_adapter_field_instead_of_the_stale_selection() {
        let mut dialog = ElicitationDialog::new(paired_request(1, false));
        dialog.handle_key(KeyCode::Down, KeyModifiers::NONE);
        dialog.handle_key(KeyCode::Down, KeyModifiers::NONE);
        for character in "custom answer".chars() {
            dialog.handle_key(KeyCode::Char(character), KeyModifiers::NONE);
        }
        dialog.handle_key(KeyCode::Enter, KeyModifiers::NONE);

        assert_eq!(
            dialog.handle_key(KeyCode::Enter, KeyModifiers::NONE),
            Some(ElicitationResponse::Accept {
                content: BTreeMap::from([(
                    "question_0__other".into(),
                    ElicitationValue::String("custom answer".into())
                )])
            })
        );
    }

    #[test]
    fn choosing_an_option_after_typing_other_omits_the_custom_draft() {
        let mut dialog = ElicitationDialog::new(paired_request(1, false));
        dialog.handle_key(KeyCode::Up, KeyModifiers::NONE);
        dialog.paste("custom draft");
        dialog.handle_key(KeyCode::Up, KeyModifiers::NONE);
        dialog.handle_key(KeyCode::Enter, KeyModifiers::NONE);

        assert_eq!(
            dialog.handle_key(KeyCode::Enter, KeyModifiers::NONE),
            Some(ElicitationResponse::Accept {
                content: BTreeMap::from([(
                    "question_0".into(),
                    ElicitationValue::String("beta".into())
                )])
            })
        );
    }

    #[test]
    fn toggling_a_multi_select_option_deactivates_other() {
        let mut dialog = ElicitationDialog::new(paired_request(1, true));
        dialog.handle_key(KeyCode::Up, KeyModifiers::NONE);
        dialog.paste("custom draft");
        dialog.handle_key(KeyCode::Up, KeyModifiers::NONE);
        dialog.handle_key(KeyCode::Char(' '), KeyModifiers::NONE);
        dialog.handle_key(KeyCode::Enter, KeyModifiers::NONE);

        assert_eq!(
            dialog.handle_key(KeyCode::Enter, KeyModifiers::NONE),
            Some(ElicitationResponse::Accept {
                content: BTreeMap::from([(
                    "question_0".into(),
                    ElicitationValue::StringArray(vec!["beta".into()])
                )])
            })
        );
    }

    #[test]
    fn dangling_custom_metadata_remains_a_standalone_page() {
        let mut request = paired_request(1, false);
        request.fields[1].custom_answer_for = Some("missing".into());
        let mut dialog = ElicitationDialog::new(request);

        assert_eq!(dialog.display_fields.len(), 2);
        dialog.handle_key(KeyCode::Tab, KeyModifiers::NONE);
        assert!(rendered(&dialog).contains("2/2"));
        assert!(rendered(&dialog).contains("Other"));
    }

    #[test]
    fn required_text_blocks_submit_until_answered() {
        let mut dialog = ElicitationDialog::new(request(
            ElicitationFieldKind::Text {
                default: None,
                min_length: None,
                max_length: None,
                pattern: None,
                format: None,
            },
            true,
        ));
        dialog.focus = 1;
        assert_eq!(dialog.handle_key(KeyCode::Enter, KeyModifiers::NONE), None);
        assert_eq!(dialog.focus, 0);
        assert_eq!(dialog.error.as_deref(), Some("Architecture is required"));
    }

    #[test]
    fn escape_cancels_the_elicitation() {
        let mut dialog = ElicitationDialog::new(request(
            ElicitationFieldKind::Boolean { default: None },
            false,
        ));
        assert_eq!(
            dialog.handle_key(KeyCode::Esc, KeyModifiers::NONE),
            Some(ElicitationResponse::Cancel)
        );
    }
}
