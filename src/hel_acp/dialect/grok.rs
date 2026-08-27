//! Grok Build's legacy ACP extensions.

use agent_client_protocol::schema::v1::{
    SessionConfigId, SessionConfigKind, SessionConfigOption, SessionConfigOptionCategory,
    SessionConfigSelect, SessionConfigSelectOption, SessionConfigSelectOptions,
    SessionConfigValueId, SessionId,
};
use agent_client_protocol::{Agent, ConnectionTo};
use anyhow::{Context, Result, bail, ensure};

use crate::hel_elicitation::{ElicitationResponse, ElicitationValue};

const EXIT_PLAN_MODE_METHOD: &str = "x.ai/exit_plan_mode";
const SET_MODEL_METHOD: &str = "session/set_model";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct GrokModelState {
    pub(crate) current_model_id: String,
    pub(crate) current_effort: Option<String>,
    pub(crate) models: Vec<GrokModel>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GrokModel {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) description: Option<String>,
    pub(crate) efforts: Vec<GrokChoice>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GrokChoice {
    pub(crate) id: String,
    name: String,
    description: Option<String>,
}

impl GrokModelState {
    fn current_model(&self) -> Option<&GrokModel> {
        self.models
            .iter()
            .find(|model| model.id == self.current_model_id)
    }
}

pub(crate) fn is_exit_plan_mode_method(method: &str) -> bool {
    method.strip_prefix('_').unwrap_or(method) == EXIT_PLAN_MODE_METHOD
}

pub(crate) fn plan_response(response: ElicitationResponse) -> serde_json::Value {
    let ElicitationResponse::Accept { content } = response else {
        return serde_json::json!({ "outcome": "cancelled" });
    };
    let action = match content.get("action") {
        Some(ElicitationValue::String(action)) => action.as_str(),
        _ => "keep_planning",
    };
    let feedback = match content.get("feedback") {
        Some(ElicitationValue::String(feedback)) if !feedback.trim().is_empty() => {
            Some(feedback.clone())
        }
        _ => None,
    };
    match action {
        "implement" => serde_json::json!({ "outcome": "approved" }),
        "exit" => serde_json::json!({ "outcome": "abandoned" }),
        "revise" => serde_json::json!({ "outcome": "cancelled", "feedback": feedback }),
        _ => serde_json::json!({ "outcome": "cancelled" }),
    }
}

/// Read Grok's model catalogue from ACP initialization or session metadata.
pub(crate) fn model_state(
    meta: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Option<GrokModelState> {
    model_state_from_value(meta?.get("modelState")?)
}

pub(crate) fn model_state_from_value(state: &serde_json::Value) -> Option<GrokModelState> {
    let current_model_id = state.get("currentModelId")?.as_str()?.to_owned();
    let models = state
        .get("availableModels")?
        .as_array()?
        .iter()
        .filter_map(|model| {
            let id = model.get("modelId")?.as_str()?.to_owned();
            let efforts = model
                .pointer("/_meta/reasoningEfforts")
                .and_then(serde_json::Value::as_array)
                .map(|efforts| {
                    efforts
                        .iter()
                        .filter_map(|effort| {
                            let id = effort
                                .get("value")
                                .or_else(|| effort.get("id"))?
                                .as_str()?
                                .to_owned();
                            Some(GrokChoice {
                                name: effort
                                    .get("label")
                                    .and_then(serde_json::Value::as_str)
                                    .unwrap_or(&id)
                                    .to_owned(),
                                description: effort
                                    .get("description")
                                    .and_then(serde_json::Value::as_str)
                                    .map(ToOwned::to_owned),
                                id,
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();
            Some(GrokModel {
                name: model
                    .get("name")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or(&id)
                    .to_owned(),
                description: model
                    .get("description")
                    .and_then(serde_json::Value::as_str)
                    .map(ToOwned::to_owned),
                id,
                efforts,
            })
        })
        .collect::<Vec<_>>();
    let current_effort = state
        .get("availableModels")?
        .as_array()?
        .iter()
        .find(|model| {
            model.get("modelId").and_then(serde_json::Value::as_str) == Some(&current_model_id)
        })
        .and_then(|model| model.pointer("/_meta/reasoningEffort"))
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    (!models.is_empty()).then_some(GrokModelState {
        current_model_id,
        current_effort,
        models,
    })
}

/// Present Grok's catalogue as the standard selectors consumed by Hel.
pub(crate) fn config_options(state: &GrokModelState) -> Vec<SessionConfigOption> {
    let choice = |choice: &GrokChoice| {
        let mut option = SessionConfigSelectOption::new(
            SessionConfigValueId::new(choice.id.clone()),
            choice.name.clone(),
        );
        option.description = choice.description.clone();
        option
    };
    let mut options = vec![{
        let models = state
            .models
            .iter()
            .map(|model| {
                choice(&GrokChoice {
                    id: model.id.clone(),
                    name: model.name.clone(),
                    description: model.description.clone(),
                })
            })
            .collect::<Vec<_>>();
        let mut option = SessionConfigOption::new(
            SessionConfigId::new("model"),
            "Model",
            SessionConfigKind::Select(SessionConfigSelect::new(
                SessionConfigValueId::new(state.current_model_id.clone()),
                SessionConfigSelectOptions::Ungrouped(models),
            )),
        );
        option.category = Some(SessionConfigOptionCategory::Model);
        option
    }];
    let efforts = state
        .current_model()
        .map(|model| model.efforts.as_slice())
        .unwrap_or_default();
    if !efforts.is_empty() {
        let current = state
            .current_effort
            .clone()
            .unwrap_or_else(|| efforts[0].id.clone());
        let mut option = SessionConfigOption::new(
            SessionConfigId::new("effort"),
            "Reasoning effort",
            SessionConfigKind::Select(SessionConfigSelect::new(
                SessionConfigValueId::new(current),
                SessionConfigSelectOptions::Ungrouped(
                    efforts.iter().map(choice).collect::<Vec<_>>(),
                ),
            )),
        );
        option.category = Some(SessionConfigOptionCategory::ThoughtLevel);
        options.push(option);
    }
    options
}

pub(crate) fn set_model_request(
    session_id: &SessionId,
    state: &GrokModelState,
    key: &str,
    value: &str,
) -> Result<(serde_json::Value, GrokModelState)> {
    let mut updated = state.clone();
    let model_id = match key {
        "model" => {
            ensure!(
                state.models.iter().any(|model| model.id == value),
                "{value:?} is not an available model value"
            );
            updated.current_model_id = value.to_owned();
            updated.current_effort = None;
            value.to_owned()
        }
        "effort" => {
            ensure!(
                state
                    .current_model()
                    .is_some_and(|model| model.efforts.iter().any(|effort| effort.id == value)),
                "{value:?} is not an available effort value"
            );
            updated.current_effort = Some(value.to_owned());
            state.current_model_id.clone()
        }
        _ => bail!("Grok Build has no {key} selector"),
    };
    let mut params = serde_json::Map::new();
    params.insert("sessionId".into(), session_id.to_string().into());
    params.insert("modelId".into(), model_id.into());
    if key == "effort" {
        params.insert(
            "_meta".into(),
            serde_json::json!({ "reasoningEffort": value }),
        );
    }
    Ok((serde_json::Value::Object(params), updated))
}

pub(crate) async fn apply_model_change(
    connection: &ConnectionTo<Agent>,
    session_id: &SessionId,
    state: &mut GrokModelState,
    key: &str,
    value: &str,
) -> Result<()> {
    let (params, updated) = set_model_request(session_id, state, key, value)?;
    connection
        .send_request(
            agent_client_protocol::UntypedMessage::new(SET_MODEL_METHOD, params)
                .context("build Grok Build set-model request")?,
        )
        .block_task()
        .await
        .with_context(|| format!("set session {key} to {value}"))?;
    *state = updated;
    Ok(())
}

pub(crate) fn permits_unadvertised_mode(mode_id: &str) -> bool {
    matches!(mode_id, "plan" | "default")
}
