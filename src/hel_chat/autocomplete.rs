//! Slash commands: what they parse to, what the popup offers, and how a
//! chosen completion lands back in the composer.

use agent_client_protocol::schema::v1::{
    AvailableCommandInput, SessionConfigKind, SessionConfigOption, SessionConfigOptionCategory,
    SessionConfigSelectOptions,
};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem};

use crate::hel_transcript::{ChatEntry, ChatRole};

use super::ChatState;
use super::rendering::truncate_to_width;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LocalCommand {
    Help,
    Detach,
    Model,
    Effort,
    Plan,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommandSource {
    Hel,
    Agent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CommandChoice {
    name: String,
    description: String,
    input_hint: Option<String>,
    source: CommandSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ConfigValueChoice {
    value: String,
    name: String,
    description: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AutocompleteKind {
    Commands,
    ConfigValues { key: &'static str },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Autocomplete {
    kind: AutocompleteKind,
    selected: usize,
    matches: Vec<usize>,
}

impl ChatState {
    pub(super) fn move_autocomplete(&mut self, delta: isize) {
        let Some(autocomplete) = self.autocomplete.as_mut() else {
            return;
        };
        let len = autocomplete.matches.len();
        if len == 0 {
            return;
        }
        autocomplete.selected = if delta.is_negative() {
            autocomplete.selected.checked_sub(1).unwrap_or(len - 1)
        } else {
            (autocomplete.selected + 1) % len
        };
    }

    pub(super) fn accept_autocomplete(&mut self) -> bool {
        let Some(autocomplete) = self.autocomplete.clone() else {
            return false;
        };
        let Some(&index) = autocomplete.matches.get(autocomplete.selected) else {
            return false;
        };
        let value = match autocomplete.kind {
            AutocompleteKind::Commands => self
                .command_choices
                .get(index)
                .map(|command| format!("/{} ", command.name)),
            AutocompleteKind::ConfigValues { key: "model" } => self
                .model_values
                .get(index)
                .map(|choice| format!("/model {}", choice.value)),
            AutocompleteKind::ConfigValues { key: "effort" } => self
                .effort_values
                .get(index)
                .map(|choice| format!("/effort {}", choice.value)),
            AutocompleteKind::ConfigValues { .. } => None,
        };
        let Some(value) = value else {
            return false;
        };
        self.set_input(value);
        self.autocomplete = None;
        true
    }

    pub(super) fn update_autocomplete(&mut self) {
        if self.history_search.is_some() || self.input_cursor != self.input.len() {
            self.autocomplete = None;
            return;
        }
        for (prefix, key, values) in [
            ("/model ", "model", &self.model_values),
            ("/effort ", "effort", &self.effort_values),
        ] {
            if let Some(query) = self.input.strip_prefix(prefix) {
                let matches = matching_indices(values, query, |choice| {
                    (&choice.value, Some(choice.name.as_str()))
                });
                self.autocomplete = (!matches.is_empty()).then_some(Autocomplete {
                    kind: AutocompleteKind::ConfigValues { key },
                    selected: 0,
                    matches,
                });
                return;
            }
        }
        let Some(query) = self.input.strip_prefix('/') else {
            self.autocomplete = None;
            return;
        };
        if query.contains(char::is_whitespace) {
            self.autocomplete = None;
            return;
        }
        let matches = matching_indices(&self.command_choices, query, |command| {
            (&command.name, Some(command.description.as_str()))
        });
        self.autocomplete = (!matches.is_empty()).then_some(Autocomplete {
            kind: AutocompleteKind::Commands,
            selected: 0,
            matches,
        });
    }

    pub(super) fn rebuild_command_choices(&mut self) {
        let mut commands = builtin_command_choices();
        if self.plan_mode_ids().is_some() {
            commands.push(CommandChoice {
                name: "plan".to_owned(),
                description: "toggle plan mode".to_owned(),
                input_hint: Some("on|off".to_owned()),
                source: CommandSource::Hel,
            });
        }
        for command in &self.agent_commands {
            let name = command.name.trim();
            if name.is_empty()
                || commands
                    .iter()
                    .any(|existing| existing.name.eq_ignore_ascii_case(name))
            {
                continue;
            }
            let input_hint = command.input.as_ref().and_then(|input| match input {
                AvailableCommandInput::Unstructured(input) => Some(input.hint.clone()),
                _ => None,
            });
            commands.push(CommandChoice {
                name: name.to_owned(),
                description: command.description.trim().to_owned(),
                input_hint,
                source: CommandSource::Agent,
            });
        }
        self.command_choices = commands;
        self.update_autocomplete();
    }

    pub(super) fn set_config_options(&mut self, options: &[SessionConfigOption]) {
        self.current_model = config_current_value(options, "model");
        self.current_effort = config_current_value(options, "effort");
        self.model_values = config_values(options, "model");
        self.effort_values = config_values(options, "effort");
        self.update_autocomplete();
    }

    pub(super) fn show_help(&mut self) {
        let commands = self
            .command_choices
            .iter()
            .map(|command| {
                let hint = command
                    .input_hint
                    .as_deref()
                    .map(|hint| format!(" <{hint}>"))
                    .unwrap_or_default();
                let source = match command.source {
                    CommandSource::Hel => "hel",
                    CommandSource::Agent => "agent",
                };
                format!(
                    "/{name}{hint} — {description} [{source}]",
                    name = command.name,
                    description = command.description
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        self.entries.push(ChatEntry::plain(
            self.latest_seq,
            ChatRole::System,
            format!("Available commands:\n{commands}"),
        ));
    }
}

fn matching_indices<T>(
    values: &[T],
    query: &str,
    fields: impl Fn(&T) -> (&str, Option<&str>),
) -> Vec<usize> {
    let query = query.to_lowercase();
    let prefix = values
        .iter()
        .enumerate()
        .filter_map(|(index, value)| {
            fields(value)
                .0
                .to_lowercase()
                .starts_with(&query)
                .then_some(index)
        })
        .collect::<Vec<_>>();
    if !prefix.is_empty() {
        return prefix;
    }
    values
        .iter()
        .enumerate()
        .filter_map(|(index, value)| {
            let (primary, secondary) = fields(value);
            (primary.to_lowercase().contains(&query)
                || secondary.is_some_and(|secondary| secondary.to_lowercase().contains(&query)))
            .then_some(index)
        })
        .collect()
}

pub(super) fn builtin_command_choices() -> Vec<CommandChoice> {
    [
        ("help", "show available Hel and agent commands", None),
        (
            "detach",
            "return to the dashboard without stopping the worker",
            None,
        ),
        (
            "model",
            "change the active model, queued while the agent is busy",
            Some("value"),
        ),
        (
            "effort",
            "change the active reasoning effort, queued while the agent is busy",
            Some("value"),
        ),
    ]
    .into_iter()
    .map(|(name, description, input_hint)| CommandChoice {
        name: name.to_owned(),
        description: description.to_owned(),
        input_hint: input_hint.map(str::to_owned),
        source: CommandSource::Hel,
    })
    .collect()
}

pub(super) fn parse_local_command(prompt: &str) -> Option<(LocalCommand, &str)> {
    let command = prompt.strip_prefix('/')?;
    let (name, args) = command
        .split_once(char::is_whitespace)
        .map_or((command, ""), |(name, args)| (name, args.trim()));
    let command = match name {
        "help" => LocalCommand::Help,
        "detach" => LocalCommand::Detach,
        "model" => LocalCommand::Model,
        "effort" => LocalCommand::Effort,
        "plan" => LocalCommand::Plan,
        _ => return None,
    };
    Some((command, args))
}

pub(super) fn is_goal_prompt(prompt: &str) -> bool {
    prompt
        .strip_prefix("/goal")
        .is_some_and(|rest| rest.is_empty() || rest.starts_with(char::is_whitespace))
}

fn find_config_option<'a>(
    options: &'a [SessionConfigOption],
    key: &str,
) -> Option<&'a SessionConfigOption> {
    match key {
        "model" => options
            .iter()
            .find(|option| option.id.to_string() == "model")
            .or_else(|| {
                options.iter().find(|option| {
                    option.category == Some(SessionConfigOptionCategory::Model)
                        && !matches!(
                            option.id.to_string().as_str(),
                            "effort" | "reasoning_effort"
                        )
                })
            }),
        "effort" => options
            .iter()
            .find(|option| option.category == Some(SessionConfigOptionCategory::ThoughtLevel))
            .or_else(|| {
                options.iter().find(|option| {
                    matches!(
                        option.id.to_string().as_str(),
                        "effort" | "reasoning_effort"
                    )
                })
            }),
        _ => None,
    }
}

pub(super) fn config_current_value(options: &[SessionConfigOption], key: &str) -> Option<String> {
    let option = find_config_option(options, key)?;
    let SessionConfigKind::Select(select) = &option.kind else {
        return None;
    };
    Some(select.current_value.to_string())
}

fn config_values(options: &[SessionConfigOption], key: &str) -> Vec<ConfigValueChoice> {
    let option = find_config_option(options, key);
    let Some(option) = option else {
        return Vec::new();
    };
    let SessionConfigKind::Select(select) = &option.kind else {
        return Vec::new();
    };
    let choices = match &select.options {
        SessionConfigSelectOptions::Ungrouped(options) => options.iter().collect::<Vec<_>>(),
        SessionConfigSelectOptions::Grouped(groups) => {
            groups.iter().flat_map(|group| &group.options).collect()
        }
        _ => Vec::new(),
    };
    choices
        .into_iter()
        .map(|choice| ConfigValueChoice {
            value: choice.value.to_string(),
            name: choice.name.clone(),
            description: choice.description.clone(),
        })
        .collect()
}

pub(super) fn render_autocomplete(frame: &mut Frame, prompt_area: Rect, chat: &ChatState) {
    let Some(autocomplete) = chat.autocomplete.as_ref() else {
        return;
    };
    let visible = autocomplete.matches.len().min(8);
    if visible == 0 {
        return;
    }
    let height = (visible as u16).saturating_add(2);
    let area = Rect::new(
        prompt_area.x,
        prompt_area.y.saturating_sub(height),
        prompt_area.width,
        height,
    );
    frame.render_widget(Clear, area);
    let title = match autocomplete.kind {
        AutocompleteKind::Commands => " commands · ↑/↓ select · Tab/Enter accept ",
        AutocompleteKind::ConfigValues { .. } => " values · ↑/↓ select · Tab/Enter accept ",
    };
    let block = Block::default().borders(Borders::ALL).title(title);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let start = autocomplete
        .selected
        .saturating_sub(visible.saturating_sub(1));
    let items = autocomplete.matches[start..]
        .iter()
        .take(visible)
        .enumerate()
        .filter_map(|(offset, index)| {
            let selected = start + offset == autocomplete.selected;
            autocomplete_row(chat, autocomplete.kind, *index).map(|row| {
                ListItem::new(truncate_to_width(&row, usize::from(inner.width))).style(
                    if selected {
                        Style::default()
                            .fg(Color::Black)
                            .bg(Color::White)
                            .add_modifier(Modifier::BOLD)
                    } else {
                        Style::default()
                    },
                )
            })
        })
        .collect::<Vec<_>>();
    frame.render_widget(List::new(items), inner);
}

fn autocomplete_row(chat: &ChatState, kind: AutocompleteKind, index: usize) -> Option<String> {
    match kind {
        AutocompleteKind::Commands => {
            let command = chat.command_choices.get(index)?;
            let hint = command
                .input_hint
                .as_deref()
                .map(|hint| format!(" <{hint}>"))
                .unwrap_or_default();
            let source = match command.source {
                CommandSource::Hel => "hel",
                CommandSource::Agent => "agent",
            };
            Some(format!(
                "/{}{hint}  — {} [{source}]",
                command.name, command.description
            ))
        }
        AutocompleteKind::ConfigValues { key: "model" } => {
            config_value_row(chat.model_values.get(index)?)
        }
        AutocompleteKind::ConfigValues { key: "effort" } => {
            config_value_row(chat.effort_values.get(index)?)
        }
        AutocompleteKind::ConfigValues { .. } => None,
    }
}

fn config_value_row(choice: &ConfigValueChoice) -> Option<String> {
    let description = choice
        .description
        .as_deref()
        .filter(|description| !description.trim().is_empty())
        .map(|description| format!(" — {description}"))
        .unwrap_or_default();
    Some(format!("{} ({}){description}", choice.name, choice.value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hel_chat::ChatAction;
    use crate::hel_chat::test_support::{advertise, grok_chat, key, snapshot};
    use crate::hel_config::HarnessKind;
    use crossterm::event::KeyCode;

    #[test]
    fn local_command_parser_requires_an_exact_command_boundary() {
        assert_eq!(parse_local_command("/checkpoint before refactor"), None);
        assert_eq!(parse_local_command("/checkpointing"), None);
        assert_eq!(parse_local_command("explain /checkpoint"), None);
    }

    #[test]
    fn autocomplete_merges_agent_commands_without_overriding_hel_commands() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.apply_session_update(
            1,
            &serde_json::json!({
                "sessionUpdate": "available_commands_update",
                "availableCommands": [
                    {"name": "review", "description": "agent review", "input": {"hint": "scope"}},
                    {"name": "help", "description": "agent help"}
                ]
            }),
        );
        assert!(
            chat.command_choices.iter().any(|command| {
                command.name == "review" && command.source == CommandSource::Agent
            })
        );
        assert_eq!(
            chat.command_choices
                .iter()
                .filter(|command| command.name == "help")
                .count(),
            1
        );

        chat.set_input("/rev".into());
        assert!(chat.accept_autocomplete());
        assert_eq!(chat.input, "/review ");
    }

    #[test]
    fn command_updates_replace_stale_adapter_capabilities() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        for available_commands in [
            serde_json::json!([
                {"name": "plan", "description": "toggle plan mode"},
                {"name": "goal", "description": "set a persistent goal"}
            ]),
            serde_json::json!([
                {"name": "plan", "description": "toggle plan mode"}
            ]),
        ] {
            chat.apply_session_update(
                1,
                &serde_json::json!({
                    "sessionUpdate": "available_commands_update",
                    "availableCommands": available_commands
                }),
            );
        }

        assert!(
            chat.command_choices
                .iter()
                .any(|command| command.name == "plan")
        );
        assert!(
            !chat
                .command_choices
                .iter()
                .any(|command| command.name == "goal")
        );
    }

    #[test]
    fn advertised_plan_and_goal_commands_are_forwarded_unchanged() {
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.apply_session_update(
            1,
            &serde_json::json!({
                "sessionUpdate": "available_commands_update",
                "availableCommands": [
                    {"name": "plan", "description": "toggle plan mode"},
                    {"name": "goal", "description": "set a persistent goal", "input": {"hint": "objective"}}
                ]
            }),
        );
        assert!(
            chat.command_choices
                .iter()
                .any(|command| command.name == "plan")
        );
        assert!(
            chat.command_choices
                .iter()
                .any(|command| command.name == "goal")
        );

        chat.input = "/plan".into();
        assert_eq!(
            chat.handle_key(key(KeyCode::Enter)),
            ChatAction::Prompt("/plan".into())
        );

        chat.input = "/goal ship the release".into();
        assert_eq!(
            chat.handle_key(key(KeyCode::Enter)),
            ChatAction::Prompt("/goal ship the release".into())
        );
    }

    #[test]
    fn config_value_autocomplete_uses_advertised_acp_choices() {
        use agent_client_protocol::schema::v1::{
            SessionConfigSelectOption, SessionConfigSelectOptions,
        };

        let options = vec![
            SessionConfigOption::select(
                "model",
                "Model",
                "auto",
                SessionConfigSelectOptions::Ungrouped(vec![
                    SessionConfigSelectOption::new("auto", "Auto"),
                    SessionConfigSelectOption::new("gpt-5.6-luna", "Luna"),
                ]),
            )
            .category(SessionConfigOptionCategory::Model),
        ];
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.set_config_options(&options);
        chat.set_input("/model lun".into());

        assert!(chat.accept_autocomplete());
        assert_eq!(chat.input, "/model gpt-5.6-luna");
    }

    #[test]
    fn plan_is_listed_as_a_hel_command_only_where_it_is_a_shim() {
        let lists_plan = |chat: &ChatState| {
            chat.command_choices
                .iter()
                .any(|command| command.name == "plan" && command.source == CommandSource::Hel)
        };

        let mut chat = grok_chat();
        assert!(lists_plan(&chat));

        // Once the agent advertises `plan`, Hel steps out of the way.
        advertise(&mut chat, 1, &["plan"]);
        assert!(!lists_plan(&chat));

        let mut codex = ChatState::new(&snapshot(), &[]);
        codex.set_harness(Some(HarnessKind::Codex));
        assert!(!lists_plan(&codex));
    }
}
