//! Normalized session controls exposed by an ACP agent.
//!
//! ACP agents can advertise the same user-facing control through configuration
//! options, legacy session modes, or provider extensions.  This module is the
//! single place where Hel turns those protocol surfaces into chat semantics.

use std::collections::BTreeMap;

use agent_client_protocol::schema::v1::{
    AvailableCommand, SessionConfigKind, SessionConfigOption, SessionModeState,
};
use serde_json::Value;

use crate::hel_config::HarnessKind;

use super::{find_session_config_option, select_contains};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanControl {
    SetConfig { key: String, value: String },
    SetSessionMode { mode_id: String },
}

/// ACP controls and commands available to one live session.
#[derive(Debug, Clone, Default)]
pub(crate) struct AcpSessionSurface {
    harness_kind: Option<HarnessKind>,
    config_options: Vec<SessionConfigOption>,
    session_modes: Option<SessionModeState>,
    current_mode: Option<String>,
    agent_commands: Vec<AvailableCommand>,
    current_model: Option<String>,
    current_effort: Option<String>,
}

impl AcpSessionSurface {
    pub(crate) fn from_configuration(configuration: &BTreeMap<String, Value>) -> Self {
        Self {
            current_mode: configuration
                .get("mode")
                .and_then(Value::as_str)
                .map(str::to_owned),
            current_model: configuration
                .get("model")
                .and_then(Value::as_str)
                .map(str::to_owned),
            current_effort: configuration
                .get("effort")
                .and_then(Value::as_str)
                .map(str::to_owned),
            ..Self::default()
        }
    }

    pub(crate) fn set_harness_kind(&mut self, harness_kind: HarnessKind) {
        self.harness_kind = Some(harness_kind);
        self.sync_plan_mode();
    }

    pub(crate) fn set_config_options(&mut self, options: &[SessionConfigOption]) -> bool {
        let changed = self.config_options != options;
        self.config_options = options.to_vec();
        self.current_model = config_current_value(options, "model");
        self.current_effort = config_current_value(options, "effort");
        if changed || self.current_mode.is_none() {
            self.sync_plan_mode();
        }
        changed
    }

    pub(crate) fn set_session_modes(&mut self, modes: Option<SessionModeState>) -> bool {
        let changed = self.session_modes != modes;
        self.session_modes = modes;
        if changed || self.current_mode.is_none() {
            self.sync_plan_mode();
        }
        changed
    }

    pub(crate) fn apply_current_mode_update(&mut self, mode: String) {
        if self.harness_kind == Some(HarnessKind::Codex) {
            return;
        }
        self.current_mode = Some(mode.clone());
        if let Some(modes) = self.session_modes.as_mut() {
            modes.current_mode_id = mode.into();
        }
    }

    pub(crate) fn apply_projected_configuration(
        &mut self,
        configuration: &BTreeMap<String, Value>,
    ) {
        let plan_mode_key = if self.harness_kind == Some(HarnessKind::Codex) {
            "collaboration_mode"
        } else {
            "mode"
        };
        if let Some(mode) = configuration.get(plan_mode_key).and_then(Value::as_str) {
            self.current_mode = Some(mode.to_owned());
        }
        self.current_model = configuration
            .get("model")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| config_current_value(&self.config_options, "model"));
        self.current_effort = configuration
            .get("effort")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| config_current_value(&self.config_options, "effort"));
    }

    pub(crate) fn set_agent_commands(&mut self, commands: Vec<AvailableCommand>) {
        self.agent_commands = commands;
    }

    pub(crate) fn agent_commands(&self) -> &[AvailableCommand] {
        &self.agent_commands
    }

    pub(crate) fn advertises_command(&self, name: &str) -> bool {
        self.agent_commands
            .iter()
            .any(|command| command.name.eq_ignore_ascii_case(name))
    }

    pub(crate) fn prompt_invokes(prompt: &str, command: &str) -> bool {
        prompt
            .strip_prefix('/')
            .and_then(|prompt| prompt.strip_prefix(command))
            .is_some_and(|rest| rest.is_empty() || rest.starts_with(char::is_whitespace))
    }

    pub(crate) fn current_model(&self) -> Option<&str> {
        self.current_model.as_deref()
    }

    pub(crate) fn current_effort(&self) -> Option<&str> {
        self.current_effort.as_deref()
    }

    pub(crate) fn current_mode(&self) -> Option<&str> {
        self.current_mode.as_deref()
    }

    pub(crate) fn set_optimistic_plan_mode(&mut self, active: bool) {
        self.current_mode = Some(if active { "plan" } else { "default" }.into());
    }

    pub(crate) fn clear_current_mode(&mut self) {
        self.current_mode = None;
    }

    pub(crate) fn supports_plan_mode(&self) -> bool {
        self.plan_control(true).is_ok()
    }

    pub(crate) fn plan_mode_active(&self) -> bool {
        self.supports_plan_mode() && self.current_mode() == Some("plan")
    }

    pub(crate) fn plan_control(&self, active: bool) -> Result<PlanControl, &'static str> {
        let value = if active { "plan" } else { "default" };
        match self.harness_kind {
            Some(HarnessKind::Deepseek) => Err("Plan mode is unsupported in DSH."),
            Some(HarnessKind::Codex) => self
                .exact_config_has_plan_pair("collaboration_mode")
                .then(|| PlanControl::SetConfig {
                    key: "collaboration_mode".into(),
                    value: value.into(),
                })
                .ok_or(
                    "This Codex ACP version does not expose collaboration_mode with plan/default values.",
                ),
            Some(HarnessKind::Claude | HarnessKind::Kimi) => {
                if self.exact_config_has_plan_pair("mode") {
                    Ok(PlanControl::SetConfig {
                        key: "mode".into(),
                        value: value.into(),
                    })
                } else if self.advertised_plan_modes() {
                    Ok(PlanControl::SetSessionMode {
                        mode_id: value.into(),
                    })
                } else {
                    Err("This ACP harness does not expose compatible plan/default modes.")
                }
            }
            Some(HarnessKind::Grok) => Ok(PlanControl::SetSessionMode {
                mode_id: value.into(),
            }),
            None => {
                if self.config_has_plan_pair("mode") {
                    Ok(PlanControl::SetConfig {
                        key: "mode".into(),
                        value: value.into(),
                    })
                } else if self.advertised_plan_modes() {
                    Ok(PlanControl::SetSessionMode {
                        mode_id: value.into(),
                    })
                } else {
                    Err("This ACP harness does not expose compatible plan/default modes.")
                }
            }
        }
    }

    fn sync_plan_mode(&mut self) {
        let config_key = if self.harness_kind == Some(HarnessKind::Codex) {
            "collaboration_mode"
        } else {
            "mode"
        };
        if let Some(value) = config_current_value(&self.config_options, config_key) {
            self.current_mode = Some(value);
        } else if self.harness_kind != Some(HarnessKind::Codex) {
            self.current_mode = self
                .session_modes
                .as_ref()
                .map(|modes| modes.current_mode_id.to_string());
        }
    }

    fn advertised_plan_modes(&self) -> bool {
        self.session_modes.as_ref().is_some_and(|modes| {
            ["plan", "default"].into_iter().all(|desired| {
                modes
                    .available_modes
                    .iter()
                    .any(|mode| mode.id.to_string() == desired)
            })
        })
    }

    fn config_has_plan_pair(&self, key: &str) -> bool {
        find_session_config_option(&self.config_options, key).is_some_and(|option| {
            select_contains(&option.kind, "plan") && select_contains(&option.kind, "default")
        })
    }

    fn exact_config_has_plan_pair(&self, key: &str) -> bool {
        self.config_options.iter().any(|option| {
            option.id.to_string() == key
                && select_contains(&option.kind, "plan")
                && select_contains(&option.kind, "default")
        })
    }
}

pub(crate) fn config_current_value(options: &[SessionConfigOption], key: &str) -> Option<String> {
    let option = find_session_config_option(options, key)?;
    let SessionConfigKind::Select(select) = &option.kind else {
        return None;
    };
    Some(select.current_value.to_string())
}
