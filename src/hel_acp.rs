//! Minimal ACP runtime used by a Hel session worker.
//!
//! The worker owns exactly one harness process and one foreground session.  It
//! deliberately does not know about orchestration, review lanes, or subagents.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex};

use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::schema::v1::{
    CancelNotification, ClientCapabilities, CloseSessionRequest, ContentBlock, Implementation,
    InitializeRequest, LoadSessionRequest, NewSessionRequest, PermissionOptionKind, PromptRequest,
    RequestPermissionOutcome, RequestPermissionRequest, RequestPermissionResponse,
    SelectedPermissionOutcome, SessionConfigKind, SessionConfigOption, SessionConfigOptionCategory,
    SessionConfigValueId, SessionId, SessionNotification, SessionUpdate,
    SetSessionConfigOptionRequest, SetSessionModeRequest, TextContent,
};
use agent_client_protocol::{Agent, ByteStreams, Client, ConnectTo, ConnectionTo};
use anyhow::{Context, Result, anyhow, bail, ensure};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::{mpsc, oneshot};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::hel_config::HarnessKind;

#[derive(Debug, Clone)]
pub struct LaunchSpec {
    pub command: PathBuf,
    pub args: Vec<String>,
    pub environment: BTreeMap<String, String>,
    pub cwd: PathBuf,
    pub additional_directories: Vec<PathBuf>,
    pub resume_session: Option<String>,
    pub harness: HarnessKind,
    pub force_unrestricted_mode: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProductionCompactionConfig {
    model: &'static str,
    effort_option: &'static str,
    effort: &'static str,
}

fn production_compaction_config(harness: HarnessKind) -> Option<ProductionCompactionConfig> {
    match harness {
        HarnessKind::Codex => Some(ProductionCompactionConfig {
            model: "gpt-5.6-luna",
            effort_option: "reasoning_effort",
            effort: "high",
        }),
        HarnessKind::Claude => Some(ProductionCompactionConfig {
            model: "sonnet 5",
            effort_option: "effort",
            effort: "high",
        }),
        HarnessKind::Kimi => None,
    }
}

#[derive(Debug)]
pub enum CommandRequest {
    Prompt {
        request_id: String,
        text: String,
    },
    SetConfig {
        request_id: String,
        key: String,
        value: String,
    },
    Compact {
        prompt: String,
        response: oneshot::Sender<std::result::Result<String, String>>,
    },
    Cancel {
        request_id: String,
    },
    Close {
        request_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RuntimeEvent {
    Connected {
        agent_name: Option<String>,
        agent_version: Option<String>,
    },
    SessionStarted {
        native_session_id: String,
        resumed: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        unrestricted_mode: Option<String>,
    },
    SessionConfigured {
        config_options: Vec<SessionConfigOption>,
    },
    SessionUpdate {
        update: serde_json::Value,
    },
    PermissionAutoApproved {
        option_id: String,
        option_name: String,
    },
    PromptFinished {
        #[serde(default, skip_serializing_if = "String::is_empty")]
        request_id: String,
        stop_reason: String,
    },
    Warning {
        message: String,
    },
    ConfigApplied {
        #[serde(default, skip_serializing_if = "String::is_empty")]
        request_id: String,
        key: String,
        value: String,
    },
    Stopped,
}

pub async fn run(
    spec: LaunchSpec,
    requests: mpsc::Receiver<CommandRequest>,
    events: mpsc::UnboundedSender<RuntimeEvent>,
) -> Result<()> {
    let mut child = Command::new(&spec.command)
        .args(&spec.args)
        .envs(&spec.environment)
        .current_dir(&spec.cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("launch ACP bridge {}", spec.command.display()))?;
    let stdin = child.stdin.take().context("ACP bridge stdin unavailable")?;
    let stdout = child
        .stdout
        .take()
        .context("ACP bridge stdout unavailable")?;
    let stderr = child
        .stderr
        .take()
        .context("ACP bridge stderr unavailable")?;
    let stderr_task = tokio::spawn(read_stderr_tail(stderr));
    let transport = ByteStreams::new(stdin.compat_write(), stdout.compat());

    let (mut result, child_reaped) = {
        let drive = drive(transport, spec, requests, events.clone());
        tokio::pin!(drive);
        tokio::select! {
            biased;
            result = &mut drive => (result, false),
            waited = child.wait() => {
                let result = match waited {
                    Ok(status) => Err(anyhow!(
                        "ACP bridge exited before the protocol runtime completed with {status}; \
                         bridge stdout must contain only JSON-RPC frames and login-shell startup must be silent"
                    )),
                    Err(error) => Err(error).context("wait for ACP bridge"),
                };
                (result, true)
            }
        }
    };
    // Dropping the transport closes the supervisor's stdin. Give it time to
    // terminate and reap the complete bridge process group before killing the
    // supervisor itself as a last resort.
    if !child_reaped {
        match tokio::time::timeout(std::time::Duration::from_secs(3), child.wait()).await {
            Ok(waited) => {
                let _ = waited;
            }
            Err(_) => {
                let _ = child.kill().await;
                let _ = child.wait().await;
            }
        }
    }
    let stderr_tail = stderr_task
        .await
        .unwrap_or_else(|error| format!("failed to collect ACP bridge stderr: {error}"));
    if !stderr_tail.trim().is_empty() {
        result =
            result.map_err(|error| error.context(format!("ACP bridge stderr:\n{stderr_tail}")));
    }
    if let Err(error) = &result {
        let _ = events.send(RuntimeEvent::Warning {
            message: format!("ACP runtime failed: {error:#}"),
        });
    }
    let _ = events.send(RuntimeEvent::Stopped);
    result
}

const ACP_STDERR_TAIL_BYTES: usize = 16 * 1024;
const UNEXPECTED_PERMISSION_REQUEST_WARNING: &str = "The agent made a permission request, which means its permission policy is misconfigured. Hel is designed to run in either auto-review or YOLO mode.";

async fn read_stderr_tail(mut stderr: tokio::process::ChildStderr) -> String {
    let mut tail = Vec::new();
    let mut buffer = [0_u8; 4096];
    loop {
        match stderr.read(&mut buffer).await {
            Ok(0) => break,
            Ok(read) => {
                tail.extend_from_slice(&buffer[..read]);
                if tail.len() > ACP_STDERR_TAIL_BYTES {
                    tail.drain(..tail.len() - ACP_STDERR_TAIL_BYTES);
                }
            }
            Err(error) => {
                return format!("failed to read ACP bridge stderr: {error}");
            }
        }
    }
    String::from_utf8_lossy(&tail).trim().to_owned()
}

async fn drive<T>(
    transport: T,
    spec: LaunchSpec,
    mut requests: mpsc::Receiver<CommandRequest>,
    events: mpsc::UnboundedSender<RuntimeEvent>,
) -> Result<()>
where
    T: ConnectTo<Client>,
{
    let notification_events = events.clone();
    let permission_events = events.clone();
    let auto_approve_permissions = spec.force_unrestricted_mode;
    let scratch_outputs = Arc::new(Mutex::new(BTreeMap::<String, String>::new()));
    let notification_scratch_outputs = scratch_outputs.clone();
    Client
        .builder()
        .on_receive_notification(
            async move |notification: SessionNotification, _cx| {
                let scratch_id = notification.session_id.to_string();
                let mut scratch = notification_scratch_outputs
                    .lock()
                    .expect("scratch output lock poisoned");
                if let Some(output) = scratch.get_mut(&scratch_id) {
                    if let SessionUpdate::AgentMessageChunk(chunk) = &notification.update
                        && let ContentBlock::Text(text) = &chunk.content
                    {
                        output.push_str(&text.text);
                    }
                    return Ok(());
                }
                drop(scratch);
                let update = serde_json::to_value(notification.update).unwrap_or_else(
                    |error| serde_json::json!({"serialization_error": error.to_string()}),
                );
                let _ = notification_events.send(RuntimeEvent::SessionUpdate { update });
                Ok(())
            },
            agent_client_protocol::on_receive_notification!(),
        )
        .on_receive_request(
            async move |request: RequestPermissionRequest, responder, _cx| {
                let _ = permission_events.send(RuntimeEvent::Warning {
                    message: UNEXPECTED_PERMISSION_REQUEST_WARNING.to_owned(),
                });
                if !auto_approve_permissions {
                    return responder.respond(RequestPermissionResponse::new(
                        RequestPermissionOutcome::Cancelled,
                    ));
                }
                let selected = request
                    .options
                    .iter()
                    .find(|option| option.kind == PermissionOptionKind::AllowAlways)
                    .or_else(|| {
                        request
                            .options
                            .iter()
                            .find(|option| option.kind == PermissionOptionKind::AllowOnce)
                    });
                let Some(selected) = selected else {
                    return responder.respond(RequestPermissionResponse::new(
                        RequestPermissionOutcome::Cancelled,
                    ));
                };
                let _ = permission_events.send(RuntimeEvent::PermissionAutoApproved {
                    option_id: selected.option_id.to_string(),
                    option_name: selected.name.clone(),
                });
                responder.respond(RequestPermissionResponse::new(
                    RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new(
                        selected.option_id.clone(),
                    )),
                ))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .connect_with(transport, |connection: ConnectionTo<Agent>| async move {
            drive_connection(connection, &spec, &mut requests, &events, scratch_outputs)
                .await
                .map_err(|error| {
                    agent_client_protocol::Error::internal_error()
                        .data(serde_json::Value::String(format!("{error:#}")))
                })
        })
        .await
        .map_err(|error| {
            anyhow!(
                "ACP protocol failed: {error}; bridge stdout must contain only JSON-RPC frames \
                 and login-shell startup must be silent"
            )
        })
}

async fn drive_connection(
    connection: ConnectionTo<Agent>,
    spec: &LaunchSpec,
    requests: &mut mpsc::Receiver<CommandRequest>,
    events: &mpsc::UnboundedSender<RuntimeEvent>,
    scratch_outputs: Arc<Mutex<BTreeMap<String, String>>>,
) -> Result<()> {
    let mut meta = serde_json::Map::new();
    meta.insert("terminal_output".into(), serde_json::Value::Bool(true));
    let capabilities = ClientCapabilities::new().meta(meta);
    let initialized = connection
        .send_request(
            InitializeRequest::new(ProtocolVersion::V1)
                .client_info(
                    Implementation::new(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"))
                        .title("Hel"),
                )
                .client_capabilities(capabilities),
        )
        .block_task()
        .await
        .context("initialize ACP bridge")?;
    if initialized.protocol_version != ProtocolVersion::V1 {
        bail!(
            "ACP bridge negotiated unsupported protocol {:?}",
            initialized.protocol_version
        );
    }
    let _ = events.send(RuntimeEvent::Connected {
        agent_name: initialized
            .agent_info
            .as_ref()
            .map(|info| info.name.clone()),
        agent_version: initialized
            .agent_info
            .as_ref()
            .map(|info| info.version.clone()),
    });

    let loaded_session = if let Some(existing) = &spec.resume_session {
        let loaded = connection
            .send_request(
                LoadSessionRequest::new(SessionId::from(existing.clone()), spec.cwd.clone())
                    .additional_directories(spec.additional_directories.clone()),
            )
            .block_task()
            .await
            .with_context(|| format!("load ACP session {existing}"))?;
        Some((
            SessionId::from(existing.clone()),
            loaded.config_options,
            loaded.modes,
        ))
    } else {
        None
    };
    let (session_id, config_options, modes, resumed) =
        if let Some((id, options, modes)) = loaded_session {
            (id, options, modes, true)
        } else {
            let created = connection
                .send_request(
                    NewSessionRequest::new(spec.cwd.clone())
                        .additional_directories(spec.additional_directories.clone()),
                )
                .block_task()
                .await
                .context("create ACP session")?;
            (
                created.session_id,
                created.config_options,
                created.modes,
                false,
            )
        };

    let desired_mode = spec
        .force_unrestricted_mode
        .then(|| spec.harness.unrestricted_mode());
    if let Some(desired_mode) = desired_mode {
        enforce_unrestricted_mode(
            &connection,
            &session_id,
            desired_mode,
            config_options.as_deref().unwrap_or_default(),
            modes.as_ref(),
        )
        .await?;
    }
    let mut config_options = config_options.unwrap_or_default();
    let _ = events.send(RuntimeEvent::SessionStarted {
        native_session_id: session_id.to_string(),
        resumed,
        unrestricted_mode: desired_mode.map(str::to_owned),
    });
    let _ = events.send(RuntimeEvent::SessionConfigured {
        config_options: config_options.clone(),
    });

    while let Some(request) = requests.recv().await {
        match request {
            CommandRequest::Prompt { request_id, text } => {
                if text.trim().is_empty() {
                    continue;
                }
                let prompt = connection
                    .send_request(PromptRequest::new(
                        session_id.clone(),
                        vec![ContentBlock::Text(TextContent::new(text))],
                    ))
                    .block_task();
                tokio::pin!(prompt);
                loop {
                    tokio::select! {
                        response = &mut prompt => {
                            let response = response.context("send ACP prompt")?;
                            let _ = events.send(RuntimeEvent::PromptFinished {
                                request_id,
                                stop_reason: format!("{:?}", response.stop_reason),
                            });
                            break;
                        }
                        command = requests.recv() => match command {
                            Some(CommandRequest::Cancel { .. }) => {
                                connection.send_notification(CancelNotification::new(session_id.clone()))?;
                            }
                            Some(CommandRequest::Close { .. }) | None => {
                                connection.send_notification(CancelNotification::new(session_id.clone()))?;
                                let _ = connection
                                    .send_request(CloseSessionRequest::new(session_id.clone()))
                                    .block_task()
                                    .await;
                                return Ok(());
                            }
                            Some(CommandRequest::Prompt { .. }) => {
                                let _ = events.send(RuntimeEvent::Warning {
                                    message: "a prompt is already running".into(),
                                });
                            }
                            Some(CommandRequest::SetConfig { .. }) => {
                                let _ = events.send(RuntimeEvent::Warning {
                                    message: "model and effort can only be changed while the agent is idle".into(),
                                });
                            }
                            Some(CommandRequest::Compact { response, .. }) => {
                                let _ = response.send(Err(
                                    "cannot compact while the destination prompt is running".into(),
                                ));
                            }
                        }
                    }
                }
            }
            CommandRequest::SetConfig {
                request_id,
                key,
                value,
            } => {
                match set_session_config(
                    &connection,
                    &session_id,
                    &mut config_options,
                    &key,
                    &value,
                )
                .await
                {
                    Ok(()) => {
                        let _ = events.send(RuntimeEvent::ConfigApplied {
                            request_id,
                            key,
                            value,
                        });
                        let _ = events.send(RuntimeEvent::SessionConfigured {
                            config_options: config_options.clone(),
                        });
                    }
                    Err(error) => {
                        let _ = events.send(RuntimeEvent::Warning {
                            message: format!("{error:#}"),
                        });
                    }
                }
            }
            CommandRequest::Compact { prompt, response } => {
                let result =
                    compact_in_scratch_session(&connection, spec, prompt, &scratch_outputs)
                        .await
                        .map_err(|error| format!("{error:#}"));
                let _ = response.send(result);
            }
            CommandRequest::Cancel { .. } => {
                connection.send_notification(CancelNotification::new(session_id.clone()))?;
            }
            CommandRequest::Close { .. } => {
                let _ = connection
                    .send_request(CloseSessionRequest::new(session_id.clone()))
                    .block_task()
                    .await;
                break;
            }
        }
    }
    Ok(())
}

async fn set_session_config(
    connection: &ConnectionTo<Agent>,
    session_id: &SessionId,
    options: &mut Vec<SessionConfigOption>,
    key: &str,
    value: &str,
) -> Result<()> {
    let option = find_session_config_option(options, key)
        .with_context(|| format!("ACP bridge does not expose a {key} selector"))?;
    ensure!(
        select_contains(&option.kind, value),
        "{value:?} is not an available {key} value"
    );
    let response = connection
        .send_request(SetSessionConfigOptionRequest::new(
            session_id.clone(),
            option.id.clone(),
            SessionConfigValueId::new(value.to_owned()),
        ))
        .block_task()
        .await
        .with_context(|| format!("set session {key} to {value}"))?;
    *options = response.config_options;
    Ok(())
}

fn find_session_config_option<'a>(
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

async fn compact_in_scratch_session(
    connection: &ConnectionTo<Agent>,
    spec: &LaunchSpec,
    prompt: String,
    scratch_outputs: &Arc<Mutex<BTreeMap<String, String>>>,
) -> Result<String> {
    let created = connection
        .send_request(
            NewSessionRequest::new(spec.cwd.clone())
                .additional_directories(spec.additional_directories.clone()),
        )
        .block_task()
        .await
        .context("create scratch ACP session")?;
    let session_id = created.session_id;
    if spec.force_unrestricted_mode {
        enforce_unrestricted_mode(
            connection,
            &session_id,
            spec.harness.unrestricted_mode(),
            created.config_options.as_deref().unwrap_or_default(),
            created.modes.as_ref(),
        )
        .await?;
    }
    configure_production_compactor(
        connection,
        &session_id,
        spec.harness,
        created.config_options.unwrap_or_default(),
    )
    .await?;
    scratch_outputs
        .lock()
        .expect("scratch output lock poisoned")
        .insert(session_id.to_string(), String::new());
    let prompt_result = connection
        .send_request(PromptRequest::new(
            session_id.clone(),
            vec![ContentBlock::Text(TextContent::new(prompt))],
        ))
        .block_task()
        .await;
    let _ = connection
        .send_request(CloseSessionRequest::new(session_id.clone()))
        .block_task()
        .await;
    let output = scratch_outputs
        .lock()
        .expect("scratch output lock poisoned")
        .remove(&session_id.to_string())
        .unwrap_or_default();
    prompt_result.context("run scratch ACP compaction prompt")?;
    if output.trim().is_empty() {
        bail!("scratch ACP compaction returned no agent text");
    }
    Ok(output)
}

async fn configure_production_compactor(
    connection: &ConnectionTo<Agent>,
    session_id: &SessionId,
    harness: HarnessKind,
    options: Vec<SessionConfigOption>,
) -> Result<()> {
    let Some(config) = production_compaction_config(harness) else {
        return Ok(());
    };
    let model_option = options
        .iter()
        .find(|option| {
            option.id.to_string() == "model"
                || option.category == Some(SessionConfigOptionCategory::Model)
        })
        .context("ACP bridge does not expose a model selector for compaction")?;
    let response = connection
        .send_request(SetSessionConfigOptionRequest::new(
            session_id.clone(),
            model_option.id.clone(),
            SessionConfigValueId::new(config.model.to_owned()),
        ))
        .block_task()
        .await
        .with_context(|| format!("select production compaction model {}", config.model))?;
    let effort_option = response
        .config_options
        .iter()
        .find(|option| option.id.to_string() == config.effort_option)
        .with_context(|| {
            format!(
                "ACP bridge does not expose compaction effort option {:?}",
                config.effort_option
            )
        })?;
    ensure!(
        select_contains(&effort_option.kind, config.effort),
        "ACP compaction model {} does not support effort {}",
        config.model,
        config.effort
    );
    connection
        .send_request(SetSessionConfigOptionRequest::new(
            session_id.clone(),
            effort_option.id.clone(),
            SessionConfigValueId::new(config.effort.to_owned()),
        ))
        .block_task()
        .await
        .with_context(|| {
            format!(
                "select production compaction effort {} for {}",
                config.effort, config.model
            )
        })?;
    Ok(())
}

async fn enforce_unrestricted_mode(
    connection: &ConnectionTo<Agent>,
    session_id: &SessionId,
    desired: &str,
    config_options: &[SessionConfigOption],
    legacy_modes: Option<&agent_client_protocol::schema::v1::SessionModeState>,
) -> Result<()> {
    if let Some(option) = config_options.iter().find(|option| {
        option.category == Some(SessionConfigOptionCategory::Mode)
            && select_contains(&option.kind, desired)
    }) {
        connection
            .send_request(SetSessionConfigOptionRequest::new(
                session_id.clone(),
                option.id.clone(),
                SessionConfigValueId::new(desired.to_string()),
            ))
            .block_task()
            .await
            .with_context(|| format!("select unrestricted ACP mode {desired}"))?;
        return Ok(());
    }
    if legacy_modes.is_some_and(|modes| {
        modes
            .available_modes
            .iter()
            .any(|mode| mode.id.to_string() == desired)
    }) {
        connection
            .send_request(SetSessionModeRequest::new(
                session_id.clone(),
                desired.to_string(),
            ))
            .block_task()
            .await
            .with_context(|| format!("select unrestricted ACP mode {desired}"))?;
        return Ok(());
    }
    bail!("ACP bridge does not expose required unrestricted mode {desired}")
}

fn select_contains(kind: &SessionConfigKind, desired: &str) -> bool {
    let SessionConfigKind::Select(select) = kind else {
        return false;
    };
    match &select.options {
        agent_client_protocol::schema::v1::SessionConfigSelectOptions::Ungrouped(options) => {
            options
                .iter()
                .any(|option| option.value.to_string() == desired)
        }
        agent_client_protocol::schema::v1::SessionConfigSelectOptions::Grouped(groups) => groups
            .iter()
            .flat_map(|group| &group.options)
            .any(|option| option.value.to_string() == desired),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_client_protocol::schema::v1::{
        SessionConfigSelectGroup, SessionConfigSelectOption, SessionConfigSelectOptions,
    };

    #[test]
    fn finds_modes_in_flat_and_grouped_options() {
        let flat =
            SessionConfigKind::Select(agent_client_protocol::schema::v1::SessionConfigSelect::new(
                "default",
                vec![SessionConfigSelectOption::new("auto", "Auto")],
            ));
        assert!(select_contains(&flat, "auto"));

        let grouped =
            SessionConfigKind::Select(agent_client_protocol::schema::v1::SessionConfigSelect::new(
                "default",
                SessionConfigSelectOptions::Grouped(vec![SessionConfigSelectGroup::new(
                    "permissions",
                    "Permissions",
                    vec![SessionConfigSelectOption::new(
                        "bypassPermissions",
                        "Bypass",
                    )],
                )]),
            ));
        assert!(select_contains(&grouped, "bypassPermissions"));
    }

    #[test]
    fn production_compactors_are_fixed_independently_of_target_profiles() {
        assert_eq!(
            production_compaction_config(HarnessKind::Codex),
            Some(ProductionCompactionConfig {
                model: "gpt-5.6-luna",
                effort_option: "reasoning_effort",
                effort: "high",
            })
        );
        assert_eq!(
            production_compaction_config(HarnessKind::Claude),
            Some(ProductionCompactionConfig {
                model: "sonnet 5",
                effort_option: "effort",
                effort: "high",
            })
        );
        assert_eq!(production_compaction_config(HarnessKind::Kimi), None);
    }

    #[test]
    fn live_config_finds_model_and_anvil_reasoning_effort_separately() {
        let model = SessionConfigOption::select(
            "model",
            "Model",
            "gpt-5.6-sol",
            vec![SessionConfigSelectOption::new("gpt-5.6-sol", "Sol")],
        )
        .category(SessionConfigOptionCategory::Model);
        let effort = SessionConfigOption::select(
            "reasoning_effort",
            "Reasoning effort",
            "high",
            vec![SessionConfigSelectOption::new("high", "High")],
        )
        .category(SessionConfigOptionCategory::Model);
        let options = vec![model, effort];

        assert_eq!(
            find_session_config_option(&options, "model")
                .unwrap()
                .id
                .to_string(),
            "model"
        );
        assert_eq!(
            find_session_config_option(&options, "effort")
                .unwrap()
                .id
                .to_string(),
            "reasoning_effort"
        );
    }

    #[test]
    fn permission_request_warning_explains_required_permission_modes() {
        assert!(UNEXPECTED_PERMISSION_REQUEST_WARNING.contains("misconfigured"));
        assert!(UNEXPECTED_PERMISSION_REQUEST_WARNING.contains("auto-review"));
        assert!(UNEXPECTED_PERMISSION_REQUEST_WARNING.contains("YOLO"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bridge_exit_during_initialize_returns_an_actionable_error() {
        let (_request_tx, request_rx) = mpsc::channel(1);
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        let spec = LaunchSpec {
            command: "sh".into(),
            args: vec![
                "-c".into(),
                "echo 'specific supervisor failure' >&2; exit 17".into(),
            ],
            environment: BTreeMap::new(),
            cwd: std::env::current_dir().unwrap(),
            additional_directories: Vec::new(),
            resume_session: None,
            harness: HarnessKind::Kimi,
            force_unrestricted_mode: true,
        };

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            run(spec, request_rx, event_tx),
        )
        .await
        .expect("an exited bridge must not leave ACP initialization hanging")
        .unwrap_err();
        let complete_error = format!("{error:#}");
        assert!(
            complete_error.contains("bridge stdout must contain only JSON-RPC frames"),
            "unexpected error: {error:#}"
        );
        assert!(complete_error.contains("specific supervisor failure"));

        let events = std::iter::from_fn(|| event_rx.try_recv().ok()).collect::<Vec<_>>();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, RuntimeEvent::Warning { message } if
            message.contains("ACP runtime failed")))
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, RuntimeEvent::Stopped))
        );
    }
}
