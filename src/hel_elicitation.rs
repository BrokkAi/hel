//! ACP form elicitation shapes shared by the runtime, relay, and chat UI.
//!
//! The ACP crate version Hel currently uses predates enum-option descriptions,
//! so requests are parsed from their raw JSON here. Keeping that interpretation
//! in one place also lets Claude and Codex metadata conventions share one UI.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

pub const MAX_ELICITATION_BYTES: usize = 64 * 1024;
const MAX_FIELDS: usize = 32;
const MAX_OPTIONS: usize = 100;
const MAX_TEXT_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ElicitationRequest {
    pub id: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub fields: Vec<ElicitationField>,
}

impl ElicitationRequest {
    pub fn from_acp_params(id: impl Into<String>, params: Value) -> Result<Self> {
        let encoded = serde_json::to_vec(&params).context("serialize ACP elicitation request")?;
        ensure!(
            encoded.len() <= MAX_ELICITATION_BYTES,
            "elicitation request exceeds {MAX_ELICITATION_BYTES} bytes"
        );
        let params = object(&params, "elicitation params")?;
        ensure!(
            string(params, "mode")? == "form",
            "Hel only supports form elicitations"
        );
        ensure!(
            params.get("sessionId").and_then(Value::as_str).is_some()
                || params.get("requestId").is_some(),
            "elicitation has no sessionId or requestId scope"
        );
        let message = string(params, "message")?.to_owned();
        ensure_text(&message, "elicitation message")?;
        let schema = object(
            params
                .get("requestedSchema")
                .context("form elicitation has no requestedSchema")?,
            "requestedSchema",
        )?;
        if let Some(kind) = schema.get("type").and_then(Value::as_str) {
            ensure!(kind == "object", "requestedSchema type must be object");
        }
        let title = optional_text(schema, "title")?;
        let description = optional_text(schema, "description")?;
        let properties = schema
            .get("properties")
            .map(|value| object(value, "requestedSchema.properties"))
            .transpose()?
            .cloned()
            .unwrap_or_default();
        ensure!(
            properties.len() <= MAX_FIELDS,
            "elicitation has more than {MAX_FIELDS} fields"
        );
        let required = string_set(schema.get("required"), "requestedSchema.required")?;
        let fields = properties
            .iter()
            .map(|(field_id, schema)| parse_field(field_id, schema, required.contains(field_id)))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            id: id.into(),
            message,
            title,
            description,
            fields,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ElicitationField {
    pub id: String,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub required: bool,
    #[serde(default)]
    pub secret: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_answer_for: Option<String>,
    #[serde(flatten)]
    pub kind: ElicitationFieldKind,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ElicitationFieldKind {
    Text {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        min_length: Option<usize>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_length: Option<usize>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pattern: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        format: Option<String>,
    },
    SingleSelect {
        options: Vec<ElicitationOption>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default: Option<String>,
    },
    MultiSelect {
        options: Vec<ElicitationOption>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        default: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        min_items: Option<usize>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_items: Option<usize>,
    },
    Boolean {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default: Option<bool>,
    },
    Integer {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        minimum: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        maximum: Option<i64>,
    },
    Number {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default: Option<f64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        minimum: Option<f64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        maximum: Option<f64>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ElicitationOption {
    pub value: String,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ElicitationResponse {
    Accept {
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        content: BTreeMap<String, ElicitationValue>,
    },
    Decline,
    Cancel,
}

impl ElicitationResponse {
    pub const fn action_name(&self) -> &'static str {
        match self {
            Self::Accept { .. } => "accept",
            Self::Decline => "decline",
            Self::Cancel => "cancel",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ElicitationValue {
    String(String),
    Integer(i64),
    Number(f64),
    Boolean(bool),
    StringArray(Vec<String>),
}

fn parse_field(field_id: &str, value: &Value, required: bool) -> Result<ElicitationField> {
    ensure_text(field_id, "elicitation field id")?;
    let schema = object(value, &format!("schema for field {field_id:?}"))?;
    let title = optional_text(schema, "title")?.unwrap_or_else(|| field_id.to_owned());
    let description = optional_text(schema, "description")?;
    let meta = schema.get("_meta").and_then(Value::as_object);
    let codex = meta
        .and_then(|meta| meta.get("codex"))
        .and_then(Value::as_object);
    let secret = codex
        .and_then(|meta| meta.get("isSecret"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let claude_custom = meta
        .and_then(|meta| meta.get("_askUserQuestionCustomAnswer"))
        .and_then(Value::as_object);
    let custom_answer_for = codex
        .filter(|meta| meta.get("isOtherAnswer").and_then(Value::as_bool) == Some(true))
        .and_then(|meta| meta.get("questionId"))
        .and_then(Value::as_str)
        .or_else(|| {
            claude_custom
                .filter(|meta| meta.get("isCustomAnswer").and_then(Value::as_bool) == Some(true))
                .and_then(|meta| meta.get("questionId"))
                .and_then(Value::as_str)
        })
        .map(str::to_owned);
    let kind = match string(schema, "type")? {
        "string" => {
            let options = parse_string_options(schema)?;
            if options.is_empty() {
                let pattern = optional_text(schema, "pattern")?;
                if let Some(pattern) = &pattern {
                    regex::Regex::new(pattern)
                        .with_context(|| format!("field {field_id:?} has an invalid pattern"))?;
                }
                ElicitationFieldKind::Text {
                    default: optional_text(schema, "default")?,
                    min_length: optional_usize(schema, "minLength")?,
                    max_length: optional_usize(schema, "maxLength")?,
                    pattern,
                    format: optional_text(schema, "format")?,
                }
            } else {
                ElicitationFieldKind::SingleSelect {
                    options,
                    default: optional_text(schema, "default")?,
                }
            }
        }
        "array" => {
            let items = object(
                schema
                    .get("items")
                    .context("multi-select field has no items schema")?,
                "multi-select items",
            )?;
            let options = parse_options(items, "anyOf").or_else(|_| parse_enum_options(items))?;
            ensure!(!options.is_empty(), "multi-select field has no options");
            ElicitationFieldKind::MultiSelect {
                options,
                default: optional_string_array(schema, "default")?.unwrap_or_default(),
                min_items: optional_usize(schema, "minItems")?,
                max_items: optional_usize(schema, "maxItems")?,
            }
        }
        "boolean" => ElicitationFieldKind::Boolean {
            default: optional_bool(schema, "default")?,
        },
        "integer" => ElicitationFieldKind::Integer {
            default: optional_i64(schema, "default")?,
            minimum: optional_i64(schema, "minimum")?,
            maximum: optional_i64(schema, "maximum")?,
        },
        "number" => ElicitationFieldKind::Number {
            default: optional_f64(schema, "default")?,
            minimum: optional_f64(schema, "minimum")?,
            maximum: optional_f64(schema, "maximum")?,
        },
        other => bail!("field {field_id:?} has unsupported type {other:?}"),
    };
    Ok(ElicitationField {
        id: field_id.to_owned(),
        title,
        description,
        required,
        secret,
        custom_answer_for,
        kind,
    })
}

fn parse_string_options(schema: &Map<String, Value>) -> Result<Vec<ElicitationOption>> {
    if schema.contains_key("oneOf") {
        parse_options(schema, "oneOf")
    } else if schema.contains_key("enum") {
        parse_enum_options(schema)
    } else {
        Ok(Vec::new())
    }
}

fn parse_options(schema: &Map<String, Value>, key: &str) -> Result<Vec<ElicitationOption>> {
    let values = schema
        .get(key)
        .and_then(Value::as_array)
        .with_context(|| format!("{key} must be an array"))?;
    ensure!(values.len() <= MAX_OPTIONS, "{key} has too many options");
    values
        .iter()
        .map(|value| {
            let option = object(value, "elicitation option")?;
            let value = string(option, "const")?.to_owned();
            let title = optional_text(option, "title")?.unwrap_or_else(|| value.clone());
            let description = optional_text(option, "description")?;
            let preview = option
                .get("_meta")
                .and_then(Value::as_object)
                .and_then(|meta| meta.get("_claude/askUserQuestionOption"))
                .and_then(Value::as_object)
                .and_then(|meta| meta.get("preview"))
                .and_then(Value::as_str)
                .map(str::to_owned);
            if let Some(preview) = &preview {
                ensure_text(preview, "elicitation option preview")?;
            }
            Ok(ElicitationOption {
                value,
                title,
                description,
                preview,
            })
        })
        .collect()
}

fn parse_enum_options(schema: &Map<String, Value>) -> Result<Vec<ElicitationOption>> {
    let values = schema
        .get("enum")
        .and_then(Value::as_array)
        .context("enum must be an array")?;
    ensure!(values.len() <= MAX_OPTIONS, "enum has too many options");
    values
        .iter()
        .map(|value| {
            let value = value
                .as_str()
                .context("elicitation enum values must be strings")?
                .to_owned();
            Ok(ElicitationOption {
                title: value.clone(),
                value,
                description: None,
                preview: None,
            })
        })
        .collect()
}

fn object<'a>(value: &'a Value, what: &str) -> Result<&'a Map<String, Value>> {
    value
        .as_object()
        .with_context(|| format!("{what} must be an object"))
}

fn string<'a>(object: &'a Map<String, Value>, key: &str) -> Result<&'a str> {
    object
        .get(key)
        .and_then(Value::as_str)
        .with_context(|| format!("{key} must be a string"))
}

fn ensure_text(value: &str, what: &str) -> Result<()> {
    ensure!(value.len() <= MAX_TEXT_BYTES, "{what} is too long");
    Ok(())
}

fn optional_text(object: &Map<String, Value>, key: &str) -> Result<Option<String>> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => {
            ensure_text(value, key)?;
            Ok(Some(value.clone()))
        }
        Some(_) => bail!("{key} must be a string"),
    }
}

fn optional_usize(object: &Map<String, Value>, key: &str) -> Result<Option<usize>> {
    optional_u64(object, key)?
        .map(|value| usize::try_from(value).context("numeric constraint does not fit usize"))
        .transpose()
}

fn optional_u64(object: &Map<String, Value>, key: &str) -> Result<Option<u64>> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_u64()
            .with_context(|| format!("{key} must be a non-negative integer"))
            .map(Some),
    }
}

fn optional_i64(object: &Map<String, Value>, key: &str) -> Result<Option<i64>> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_i64()
            .with_context(|| format!("{key} must be an integer"))
            .map(Some),
    }
}

fn optional_f64(object: &Map<String, Value>, key: &str) -> Result<Option<f64>> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => {
            let value = value
                .as_f64()
                .with_context(|| format!("{key} must be a number"))?;
            ensure!(value.is_finite(), "{key} must be finite");
            Ok(Some(value))
        }
    }
}

fn optional_bool(object: &Map<String, Value>, key: &str) -> Result<Option<bool>> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_bool()
            .with_context(|| format!("{key} must be a boolean"))
            .map(Some),
    }
}

fn optional_string_array(object: &Map<String, Value>, key: &str) -> Result<Option<Vec<String>>> {
    let Some(value) = object.get(key) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let values = value
        .as_array()
        .with_context(|| format!("{key} must be an array"))?;
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .with_context(|| format!("{key} entries must be strings"))
                .map(str::to_owned)
        })
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

fn string_set(value: Option<&Value>, what: &str) -> Result<BTreeSet<String>> {
    let Some(value) = value else {
        return Ok(BTreeSet::new());
    };
    value
        .as_array()
        .with_context(|| format!("{what} must be an array"))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .with_context(|| format!("{what} entries must be strings"))
                .map(str::to_owned)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_claude_choices_preview_and_custom_answer_marker() {
        let request = ElicitationRequest::from_acp_params(
            "elicit-1",
            json!({
                "sessionId": "session-1",
                "toolCallId": "tool-1",
                "mode": "form",
                "message": "Which architecture?",
                "requestedSchema": {
                    "type": "object",
                    "properties": {
                        "question_0": {
                            "type": "string",
                            "title": "CI architecture",
                            "oneOf": [{
                                "const": "reusable",
                                "title": "Reusable workflow",
                                "description": "Thin callers",
                                "_meta": {"_claude/askUserQuestionOption": {"preview": "ci.yml -> reusable"}}
                            }]
                        },
                        "question_0_custom": {
                            "type": "string",
                            "title": "Other",
                            "_meta": {"_askUserQuestionCustomAnswer": {
                                "questionId": "question_0",
                                "isCustomAnswer": true
                            }}
                        }
                    }
                }
            }),
        )
        .unwrap();
        assert_eq!(request.message, "Which architecture?");
        let ElicitationFieldKind::SingleSelect { options, .. } = &request.fields[0].kind else {
            panic!("expected single-select field");
        };
        assert_eq!(options[0].description.as_deref(), Some("Thin callers"));
        assert_eq!(options[0].preview.as_deref(), Some("ci.yml -> reusable"));
        assert_eq!(
            request.fields[1].custom_answer_for.as_deref(),
            Some("question_0")
        );
    }

    #[test]
    fn parses_codex_secret_other_field() {
        let request = ElicitationRequest::from_acp_params(
            "elicit-2",
            json!({
                "sessionId": "session-1",
                "mode": "form",
                "message": "Input requested",
                "requestedSchema": {
                    "properties": {
                        "token__other": {
                            "type": "string",
                            "_meta": {"codex": {
                                "questionId": "token",
                                "isOtherAnswer": true,
                                "isSecret": true
                            }}
                        }
                    }
                }
            }),
        )
        .unwrap();
        assert!(request.fields[0].secret);
        assert_eq!(
            request.fields[0].custom_answer_for.as_deref(),
            Some("token")
        );
    }

    #[test]
    fn rejects_url_and_nested_object_elicitations() {
        let url = ElicitationRequest::from_acp_params(
            "bad-url",
            json!({"requestId": 1, "mode": "url", "message": "Open", "url": "https://example.com"}),
        )
        .unwrap_err();
        assert!(url.to_string().contains("only supports form"));

        let nested = ElicitationRequest::from_acp_params(
            "bad-nested",
            json!({
                "sessionId": "session-1",
                "mode": "form",
                "message": "Nested",
                "requestedSchema": {"properties": {"nested": {"type": "object"}}}
            }),
        )
        .unwrap_err();
        assert!(nested.to_string().contains("unsupported type"));
    }
}
