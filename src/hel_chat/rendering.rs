//! Markdown and width-aware transcript rendering.

use pulldown_cmark::{CodeBlockKind, Event, HeadingLevel, Options, Parser, Tag, TagEnd};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use textwrap::WordSplitter;
use unicode_segmentation::UnicodeSegmentation;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TranscriptRenderMode {
    Rich,
    Raw,
}

impl TranscriptRenderMode {
    pub(super) fn toggled(self) -> Self {
        match self {
            Self::Rich => Self::Raw,
            Self::Raw => Self::Rich,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct LogicalLine {
    pub(super) line: Line<'static>,
    pub(super) continuation_indent: usize,
}

#[derive(Debug, Clone)]
struct ListState {
    next: Option<u64>,
}

#[derive(Debug, Default)]
struct TableState {
    rows: Vec<Vec<String>>,
    row: Vec<String>,
    cell: String,
}

struct MarkdownWriter {
    lines: Vec<LogicalLine>,
    spans: Vec<Span<'static>>,
    style: Style,
    quote_depth: usize,
    lists: Vec<ListState>,
    item_prefix: Option<String>,
    table: Option<TableState>,
    width: usize,
}

impl MarkdownWriter {
    fn new(width: usize, style: Style) -> Self {
        Self {
            lines: Vec::new(),
            spans: Vec::new(),
            style,
            quote_depth: 0,
            lists: Vec::new(),
            item_prefix: None,
            table: None,
            width: width.max(1),
        }
    }

    fn push_text(&mut self, text: &str) {
        if let Some(table) = &mut self.table {
            table.cell.push_str(text);
            return;
        }
        let mut parts = text.split('\n').peekable();
        while let Some(part) = parts.next() {
            if !part.is_empty() {
                self.spans.push(Span::styled(part.to_owned(), self.style));
            }
            if parts.peek().is_some() {
                self.finish_line();
            }
        }
    }

    fn finish_line(&mut self) {
        let quote = "> ".repeat(self.quote_depth);
        let item = self.item_prefix.take().unwrap_or_default();
        let continuation_indent = display_width(&quote) + display_width(&item);
        let mut spans = Vec::with_capacity(self.spans.len() + 2);
        if !quote.is_empty() {
            spans.push(Span::styled(quote, Style::default().fg(Color::DarkGray)));
        }
        if !item.is_empty() {
            spans.push(Span::styled(item, Style::default().fg(Color::DarkGray)));
        }
        spans.append(&mut self.spans);
        self.lines.push(LogicalLine {
            line: Line::from(spans),
            continuation_indent,
        });
    }

    fn finish_block(&mut self) {
        if !self.spans.is_empty() || self.item_prefix.is_some() {
            self.finish_line();
        }
        if self
            .lines
            .last()
            .is_some_and(|line| !line.line.spans.is_empty())
        {
            self.lines.push(LogicalLine {
                line: Line::default(),
                continuation_indent: 0,
            });
        }
    }

    fn finish(mut self) -> Vec<LogicalLine> {
        if !self.spans.is_empty() || self.item_prefix.is_some() {
            self.finish_line();
        }
        while self
            .lines
            .last()
            .is_some_and(|line| line.line.spans.is_empty())
        {
            self.lines.pop();
        }
        self.lines
    }

    fn render_table(&mut self, mut table: TableState) {
        if !table.cell.is_empty() || !table.row.is_empty() {
            table.row.push(std::mem::take(&mut table.cell));
        }
        if !table.row.is_empty() {
            table.rows.push(std::mem::take(&mut table.row));
        }
        let Some(header) = table.rows.first().cloned() else {
            return;
        };
        let columns = header.len().max(1);
        let grid_width = table
            .rows
            .iter()
            .map(|row| {
                row.iter().map(|cell| display_width(cell)).sum::<usize>()
                    + columns.saturating_sub(1) * 3
                    + 4
            })
            .max()
            .unwrap_or(0);
        if grid_width <= self.width {
            for (index, row) in table.rows.into_iter().enumerate() {
                let text = format!("│ {} │", row.join(" │ "));
                self.lines.push(LogicalLine {
                    line: if index == 0 {
                        Line::from(Span::styled(
                            text,
                            Style::default().add_modifier(Modifier::BOLD),
                        ))
                    } else {
                        Line::from(text)
                    },
                    continuation_indent: 2,
                });
            }
        } else {
            for (row_index, row) in table.rows.into_iter().skip(1).enumerate() {
                if row_index > 0 {
                    self.lines.push(LogicalLine {
                        line: Line::from(Span::styled(
                            "────────────────────",
                            Style::default().fg(Color::DarkGray),
                        )),
                        continuation_indent: 0,
                    });
                }
                for (column, value) in row.into_iter().enumerate() {
                    let label = header
                        .get(column)
                        .filter(|label| !label.is_empty())
                        .cloned()
                        .unwrap_or_else(|| format!("Column {}", column + 1));
                    self.lines.push(LogicalLine {
                        line: Line::from(vec![
                            Span::styled(
                                format!("{label}: "),
                                Style::default().add_modifier(Modifier::BOLD),
                            ),
                            Span::from(value),
                        ]),
                        continuation_indent: display_width(&label) + 2,
                    });
                }
            }
        }
    }
}

/// Remove terminal controls while preserving user-visible whitespace.
pub(super) fn sanitize_terminal_text(text: &str) -> String {
    let mut sanitized = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\x1b' && chars.next_if_eq(&'[').is_some() {
            let _ = chars.find(|ch| ('@'..='~').contains(ch));
        } else if ch == '\r' {
            if chars.peek() != Some(&'\n') {
                sanitized.push('\n');
            }
        } else if matches!(ch, '\n' | '\t') || !ch.is_control() {
            sanitized.push(ch);
        }
    }
    sanitized
}

pub(super) fn markdown_lines(
    source: &str,
    body_style: Style,
    accent_style: Style,
    width: usize,
) -> Vec<LogicalLine> {
    let mut options = Options::empty();
    options.insert(Options::ENABLE_STRIKETHROUGH);
    options.insert(Options::ENABLE_TABLES);
    let parser = Parser::new_ext(source, options);
    let mut writer = MarkdownWriter::new(width, body_style);
    let mut style_stack = Vec::new();

    for event in parser {
        match event {
            Event::Start(tag) => match tag {
                Tag::Paragraph => {}
                Tag::Heading { level, .. } => {
                    let count = match level {
                        HeadingLevel::H1 => 1,
                        HeadingLevel::H2 => 2,
                        HeadingLevel::H3 => 3,
                        HeadingLevel::H4 => 4,
                        HeadingLevel::H5 => 5,
                        HeadingLevel::H6 => 6,
                    };
                    writer.spans.push(Span::styled(
                        format!("{} ", "#".repeat(count)),
                        Style::default().fg(Color::DarkGray),
                    ));
                    style_stack.push(writer.style);
                    writer.style = accent_style.add_modifier(Modifier::BOLD);
                }
                Tag::BlockQuote => writer.quote_depth += 1,
                Tag::CodeBlock(kind) => {
                    writer.finish_block();
                    let language = match kind {
                        CodeBlockKind::Fenced(language) if !language.is_empty() => {
                            format!("code · {language}")
                        }
                        _ => "code".to_owned(),
                    };
                    writer.lines.push(LogicalLine {
                        line: Line::from(Span::styled(
                            language,
                            Style::default()
                                .fg(Color::DarkGray)
                                .add_modifier(Modifier::BOLD),
                        )),
                        continuation_indent: 0,
                    });
                    style_stack.push(writer.style);
                    writer.style = Style::default().fg(Color::Gray);
                }
                Tag::List(start) => writer.lists.push(ListState { next: start }),
                Tag::Item => {
                    let depth = writer.lists.len().saturating_sub(1);
                    let marker = writer
                        .lists
                        .last_mut()
                        .and_then(|list| list.next.as_mut())
                        .map_or_else(
                            || "• ".to_owned(),
                            |next| {
                                let marker = format!("{next}. ");
                                *next += 1;
                                marker
                            },
                        );
                    writer.item_prefix = Some(format!("{}{marker}", "  ".repeat(depth)));
                }
                Tag::Emphasis => {
                    style_stack.push(writer.style);
                    writer.style = writer.style.add_modifier(Modifier::ITALIC);
                }
                Tag::Strong => {
                    style_stack.push(writer.style);
                    writer.style = writer.style.add_modifier(Modifier::BOLD);
                }
                Tag::Strikethrough => {
                    style_stack.push(writer.style);
                    writer.style = writer.style.add_modifier(Modifier::CROSSED_OUT);
                }
                Tag::Link { .. } => {
                    style_stack.push(writer.style);
                    writer.style = writer
                        .style
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::UNDERLINED);
                }
                Tag::Table(_) => writer.table = Some(TableState::default()),
                Tag::TableHead | Tag::TableRow | Tag::TableCell | Tag::Image { .. } => {}
                _ => {}
            },
            Event::End(tag) => match tag {
                TagEnd::Paragraph => writer.finish_block(),
                TagEnd::Heading(_) => {
                    writer.style = style_stack.pop().unwrap_or(body_style);
                    writer.finish_block();
                }
                TagEnd::BlockQuote => {
                    writer.finish_block();
                    writer.quote_depth = writer.quote_depth.saturating_sub(1);
                }
                TagEnd::CodeBlock => {
                    if !writer.spans.is_empty() {
                        writer.finish_line();
                    }
                    writer.style = style_stack.pop().unwrap_or(body_style);
                    writer.finish_block();
                }
                TagEnd::List(_) => {
                    writer.finish_block();
                    writer.lists.pop();
                }
                TagEnd::Item => writer.finish_block(),
                TagEnd::Emphasis | TagEnd::Strong | TagEnd::Strikethrough | TagEnd::Link => {
                    writer.style = style_stack.pop().unwrap_or(body_style);
                }
                TagEnd::TableCell => {
                    if let Some(table) = &mut writer.table {
                        table.row.push(std::mem::take(&mut table.cell));
                    }
                }
                TagEnd::TableHead | TagEnd::TableRow => {
                    if let Some(table) = &mut writer.table {
                        table.rows.push(std::mem::take(&mut table.row));
                    }
                }
                TagEnd::Table => {
                    if let Some(table) = writer.table.take() {
                        writer.render_table(table);
                    }
                    writer.finish_block();
                }
                _ => {}
            },
            Event::Text(text) => writer.push_text(&text),
            Event::Code(code) => {
                if writer.table.is_some() {
                    writer.push_text(&code);
                } else {
                    writer.spans.push(Span::styled(
                        code.into_string(),
                        writer.style.fg(Color::Yellow),
                    ));
                }
            }
            Event::SoftBreak | Event::HardBreak => writer.finish_line(),
            Event::Rule => {
                writer.finish_block();
                writer.lines.push(LogicalLine {
                    line: Line::from(Span::styled(
                        "────────────────────",
                        Style::default().fg(Color::DarkGray),
                    )),
                    continuation_indent: 0,
                });
            }
            // Rich mode intentionally does not interpret or display raw HTML.
            // Ctrl-R still exposes it as sanitized source when needed.
            Event::Html(_) | Event::InlineHtml(_) => {}
            Event::FootnoteReference(reference) => writer.push_text(&format!("[{reference}]")),
            Event::TaskListMarker(checked) => {
                writer.push_text(if checked { "[x] " } else { "[ ] " });
            }
        }
    }
    writer.finish()
}

pub(super) fn raw_lines(source: &str, style: Style) -> Vec<LogicalLine> {
    source
        .split('\n')
        .map(|line| LogicalLine {
            line: Line::from(Span::styled(line.to_owned(), style)),
            continuation_indent: display_width(
                &line[..line.len().saturating_sub(line.trim_start().len())],
            ),
        })
        .collect()
}

pub(super) fn wrap_styled_line(
    line: Line<'static>,
    width: usize,
    continuation_indent: usize,
) -> Vec<Line<'static>> {
    let width = width.max(1);
    let continuation_indent = continuation_indent.min(width.saturating_sub(1));
    if line.spans.len() == 1 {
        return wrap_single_span(line, width, continuation_indent);
    }
    wrap_styled_graphemes(line, width, continuation_indent)
}

pub(super) fn line_character_count(line: &Line<'_>) -> usize {
    line.spans
        .iter()
        .map(|span| span.content.chars().count())
        .sum()
}

fn styled_line_prefix(line: &Line<'static>, width: usize) -> (Vec<Span<'static>>, usize) {
    let mut spans = Vec::new();
    let mut used_width = 0;
    let mut kept_characters = 0;

    'spans: for span in &line.spans {
        let mut content = String::new();
        for grapheme in span.content.graphemes(true) {
            let grapheme_width = display_width(grapheme);
            if used_width + grapheme_width > width {
                if !content.is_empty() {
                    spans.push(Span::styled(content, span.style));
                }
                break 'spans;
            }
            content.push_str(grapheme);
            used_width += grapheme_width;
            kept_characters += grapheme.chars().count();
        }
        if !content.is_empty() {
            spans.push(Span::styled(content, span.style));
        }
    }
    (spans, kept_characters)
}

pub(super) fn append_omitted_character_count(
    line: Line<'static>,
    width: usize,
    additional_omitted_characters: usize,
    style: Style,
) -> Line<'static> {
    let line_characters = line_character_count(&line);
    let mut omitted_characters = additional_omitted_characters;

    loop {
        let marker = format!("[{omitted_characters} more]");
        let marker_width = display_width(&marker);
        let prefix_width = width.saturating_sub(marker_width.saturating_add(1));
        let (_, kept_characters) = styled_line_prefix(&line, prefix_width);
        let next_omitted = additional_omitted_characters + line_characters - kept_characters;
        if next_omitted != omitted_characters {
            omitted_characters = next_omitted;
            continue;
        }

        let (mut spans, _) = styled_line_prefix(&line, prefix_width);
        if !spans.is_empty() {
            spans.push(Span::raw(" "));
        }
        spans.push(Span::styled(marker, style));
        return Line::from(spans);
    }
}

fn wrap_single_span(
    line: Line<'static>,
    width: usize,
    continuation_indent: usize,
) -> Vec<Line<'static>> {
    let span = &line.spans[0];
    if span.content.is_empty() {
        return vec![Line::default()];
    }
    let indent = " ".repeat(continuation_indent);
    let options = textwrap::Options::new(width)
        .subsequent_indent(&indent)
        .break_words(false)
        .word_splitter(WordSplitter::NoHyphenation);
    let wrapped = textwrap::wrap(span.content.as_ref(), options);
    let mut out = Vec::new();
    for wrapped_line in wrapped {
        let styled = Line::from(Span::styled(wrapped_line.into_owned(), span.style));
        if styled.width() <= width {
            out.push(styled);
        } else {
            out.extend(wrap_styled_graphemes(styled, width, continuation_indent));
        }
    }
    out
}

fn wrap_styled_graphemes(
    line: Line<'static>,
    width: usize,
    continuation_indent: usize,
) -> Vec<Line<'static>> {
    let continuation_style = line
        .spans
        .first()
        .map(|span| span.style)
        .unwrap_or_default();
    let continuation = vec![(" ".to_owned(), continuation_style); continuation_indent];
    let mut tokens: Vec<Vec<(String, Style)>> = Vec::new();
    let mut token = Vec::new();
    let mut whitespace = None;
    for span in &line.spans {
        for grapheme in span.content.graphemes(true) {
            let is_whitespace = grapheme.chars().all(char::is_whitespace);
            if whitespace != Some(is_whitespace) {
                if !token.is_empty() {
                    tokens.push(std::mem::take(&mut token));
                }
                whitespace = Some(is_whitespace);
            }
            token.push((grapheme.to_owned(), span.style));
        }
    }
    if !token.is_empty() {
        tokens.push(token);
    }

    let mut rows = Vec::new();
    let mut current = Vec::new();
    let mut current_width = 0;
    for token in tokens {
        let token_width = styled_token_width(&token);
        let is_whitespace = token
            .first()
            .is_some_and(|(text, _)| text.chars().all(char::is_whitespace));
        if current_width + token_width <= width {
            current.extend(token);
            current_width += token_width;
        } else if is_whitespace {
            trim_trailing_whitespace(&mut current);
            if !current.is_empty() {
                rows.push(std::mem::take(&mut current));
            }
            current = continuation.clone();
            current_width = continuation_indent;
        } else if token_width + continuation_indent <= width {
            if current.len() > continuation.len() {
                trim_trailing_whitespace(&mut current);
                rows.push(std::mem::take(&mut current));
            }
            current = continuation.clone();
            current.extend(token);
            current_width = continuation_indent + token_width;
        } else {
            for (grapheme, style) in token {
                let grapheme_width = display_width(&grapheme);
                if current_width + grapheme_width > width && !current.is_empty() {
                    trim_trailing_whitespace(&mut current);
                    rows.push(std::mem::take(&mut current));
                    current = continuation.clone();
                    current_width = continuation_indent;
                }
                current.push((grapheme, style));
                current_width += grapheme_width;
            }
        }
    }
    trim_trailing_whitespace(&mut current);
    if !current.is_empty() || rows.is_empty() {
        rows.push(current);
    }
    rows.into_iter().map(styled_graphemes_line).collect()
}

fn styled_token_width(token: &[(String, Style)]) -> usize {
    token.iter().map(|(text, _)| display_width(text)).sum()
}

fn trim_trailing_whitespace(graphemes: &mut Vec<(String, Style)>) {
    while graphemes
        .last()
        .is_some_and(|(text, _)| text.chars().all(char::is_whitespace))
    {
        graphemes.pop();
    }
}

fn styled_graphemes_line(graphemes: Vec<(String, Style)>) -> Line<'static> {
    let mut spans = Vec::new();
    let mut text = String::new();
    let mut style = None;
    for (grapheme, next_style) in graphemes {
        if style != Some(next_style) {
            if let Some(style) = style {
                spans.push(Span::styled(std::mem::take(&mut text), style));
            }
            style = Some(next_style);
        }
        text.push_str(&grapheme);
    }
    if let Some(style) = style {
        spans.push(Span::styled(text, style));
    }
    Line::from(spans)
}

pub(super) fn display_width(text: &str) -> usize {
    Line::raw(text.to_owned()).width()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(lines: &[Line<'_>]) -> Vec<String> {
        lines
            .iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect()
            })
            .collect()
    }

    #[test]
    fn sanitizer_removes_terminal_controls_and_normalizes_carriage_returns() {
        assert_eq!(
            sanitize_terminal_text("safe\x1b[31mred\x1b[0m\rnext\u{7}"),
            "safered\nnext"
        );
    }

    #[test]
    fn grapheme_wrapper_never_splits_joined_or_combining_characters() {
        let wrapped = wrap_styled_line(Line::from("a 👩‍💻 e\u{301} ｶﾞ z"), 4, 0);
        let rendered = text(&wrapped);
        assert!(rendered.iter().any(|line| line.contains("👩‍💻")));
        assert!(rendered.iter().any(|line| line.contains("e\u{301}")));
        assert!(rendered.iter().any(|line| line.contains("ｶﾞ")));
    }

    #[test]
    fn markdown_parser_handles_styles_lists_and_incomplete_fences() {
        let lines = markdown_lines(
            "# Heading\n\n- **bold** and `code`\n\n```rust\nfn main() {}",
            Style::default(),
            Style::default().fg(Color::Green),
            40,
        );
        let rendered = lines.into_iter().map(|line| line.line).collect::<Vec<_>>();
        assert_eq!(
            text(&rendered).join("\n"),
            "# Heading\n\n• bold and code\n\ncode · rust\nfn main() {}"
        );
    }

    #[test]
    fn narrow_markdown_table_falls_back_to_records() {
        let lines = markdown_lines(
            "| Name | Description |\n| --- | --- |\n| alpha | a long explanation |",
            Style::default(),
            Style::default(),
            18,
        );
        let rendered = lines.into_iter().map(|line| line.line).collect::<Vec<_>>();
        assert_eq!(
            text(&rendered),
            ["Name: alpha", "Description: a long explanation"]
        );
    }
}
