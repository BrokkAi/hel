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

#[derive(Debug, Clone)]
pub(super) struct ElicitationDialog {
    request: ElicitationRequest,
    values: Vec<FieldValue>,
    option_cursors: Vec<usize>,
    focus: usize,
    error: Option<String>,
}

impl ElicitationDialog {
    pub(super) fn new(request: ElicitationRequest) -> Self {
        let values = request.fields.iter().map(default_value).collect::<Vec<_>>();
        let option_cursors = values
            .iter()
            .map(|value| match value {
                FieldValue::Single(Some(index)) => *index,
                FieldValue::Multi(selected) => selected.first().copied().unwrap_or(0),
                _ => 0,
            })
            .collect();
        Self {
            request,
            values,
            option_cursors,
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
        if let Some(FieldValue::Text { value, cursor }) = self.values.get_mut(self.focus) {
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
        let focus_count = self.request.fields.len() + 3;
        match code {
            KeyCode::Tab if modifiers.contains(KeyModifiers::SHIFT) => {
                self.focus = self.focus.checked_sub(1).unwrap_or(focus_count - 1);
            }
            KeyCode::BackTab => {
                self.focus = self.focus.checked_sub(1).unwrap_or(focus_count - 1);
            }
            KeyCode::Tab => self.focus = (self.focus + 1) % focus_count,
            KeyCode::Enter if self.focus == self.request.fields.len() => {
                return self.accept();
            }
            KeyCode::Enter if self.focus == self.request.fields.len() + 1 => {
                return Some(ElicitationResponse::Decline);
            }
            KeyCode::Enter if self.focus == self.request.fields.len() + 2 => {
                return Some(ElicitationResponse::Cancel);
            }
            KeyCode::Enter => self.focus = (self.focus + 1).min(self.request.fields.len()),
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
        let Some(field) = self.request.fields.get(self.focus) else {
            return;
        };
        let option_count = match &field.kind {
            ElicitationFieldKind::SingleSelect { options, .. }
            | ElicitationFieldKind::MultiSelect { options, .. } => options.len(),
            _ => return,
        };
        if option_count == 0 {
            return;
        }
        let cursor = &mut self.option_cursors[self.focus];
        *cursor = if delta.is_negative() {
            cursor.checked_sub(1).unwrap_or(option_count - 1)
        } else {
            (*cursor + 1) % option_count
        };
        if let FieldValue::Single(selected) = &mut self.values[self.focus] {
            *selected = Some(*cursor);
        }
    }

    fn toggle_current(&mut self) {
        let Some(value) = self.values.get_mut(self.focus) else {
            return;
        };
        match value {
            FieldValue::Single(selected) => {
                *selected = Some(self.option_cursors[self.focus]);
            }
            FieldValue::Multi(selected) => {
                let index = self.option_cursors[self.focus];
                if !selected.remove(&index) {
                    selected.insert(index);
                }
            }
            FieldValue::Boolean(selected) => *selected = !*selected,
            FieldValue::Text { value, cursor } => {
                value.insert(*cursor, ' ');
                *cursor += 1;
            }
        }
    }

    fn edit_text(&mut self, edit: TextEdit) {
        let Some(FieldValue::Text { value, cursor }) = self.values.get_mut(self.focus) else {
            return;
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
        for (field, value) in self.request.fields.iter().zip(&self.values) {
            match validated_value(field, value) {
                Ok(Some(value)) => {
                    content.insert(field.id.clone(), value);
                }
                Ok(None) => {}
                Err(error) => {
                    self.error = Some(error);
                    self.focus = self
                        .request
                        .fields
                        .iter()
                        .position(|candidate| candidate.id == field.id)
                        .unwrap_or(0);
                    return None;
                }
            }
        }
        Some(ElicitationResponse::Accept { content })
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
    let field_count = dialog.request.fields.len();
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
    let Some(field) = dialog.request.fields.get(dialog.focus) else {
        let label = match dialog.focus.saturating_sub(dialog.request.fields.len()) {
            0 => "Submit these answers",
            1 => "Skip this question and let the agent continue",
            _ => "Cancel this question",
        };
        frame.render_widget(Paragraph::new(label).alignment(Alignment::Center), area);
        return;
    };
    let required = if field.required { " (required)" } else { "" };
    let mut lines = vec![Line::from(vec![
        Span::styled(
            format!("{}/{}  ", dialog.focus + 1, dialog.request.fields.len()),
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
    match (&field.kind, &dialog.values[dialog.focus]) {
        (_, FieldValue::Text { value, .. }) => {
            let shown = if field.secret {
                "•".repeat(value.chars().count())
            } else {
                value.clone()
            };
            lines.push(Line::raw(""));
            lines.push(Line::styled(
                format!("> {shown}"),
                Style::default().fg(Color::Cyan),
            ));
        }
        (ElicitationFieldKind::SingleSelect { options, .. }, FieldValue::Single(selected)) => {
            for (index, option) in options.iter().enumerate() {
                let cursor = dialog.option_cursors[dialog.focus] == index;
                let marker = if *selected == Some(index) {
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
        }
        (ElicitationFieldKind::MultiSelect { options, .. }, FieldValue::Multi(selected)) => {
            for (index, option) in options.iter().enumerate() {
                let marker = if selected.contains(&index) {
                    "☑"
                } else {
                    "☐"
                };
                let style = if dialog.option_cursors[dialog.focus] == index {
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                lines.push(Line::styled(format!("{marker} {}", option.title), style));
            }
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
    if let FieldValue::Text { value, cursor } = &dialog.values[dialog.focus]
        && area.width > 2
        && area.height > 3
    {
        let column = value[..*cursor].chars().count();
        frame.set_cursor_position((
            area.x + 2 + (column as u16).min(area.width.saturating_sub(3)),
            area.y + 3.min(area.height.saturating_sub(1)),
        ));
    }
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
