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
    AgentCapabilities, CancelNotification, ClientCapabilities, CloseSessionRequest, ContentBlock,
    Implementation, InitializeRequest, LoadSessionRequest, NewSessionRequest, PermissionOptionKind,
    PromptRequest, RequestPermissionOutcome, RequestPermissionRequest, RequestPermissionResponse,
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
        prompt: Vec<ContentBlock>,
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
        #[serde(default, skip_serializing_if = "Option::is_none")]
        protocol_version: Option<ProtocolVersion>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        capabilities: Option<Box<AgentCapabilities>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        agent_info: Option<Implementation>,
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
        /// The complete configuration returned by ACP for this change. Keep
        /// it in the same runtime event as command completion so the relay
        /// cannot publish a checkpoint between the two durable observations.
        #[serde(default)]
        config_options: Vec<SessionConfigOption>,
    },
    CommandRejected {
        request_id: String,
        message: String,
    },
    CommandInterrupted {
        request_id: String,
        message: String,
    },
    CancelApplied {
        request_id: String,
    },
    CloseApplied {
        request_id: String,
    },
    Stopped,
}

pub async fn run(
    spec: LaunchSpec,
    requests: mpsc::Receiver<CommandRequest>,
    events: mpsc::Sender<RuntimeEvent>,
) -> Result<()> {
    let result = run_inner(spec, requests, events.clone()).await;
    if let Err(error) = &result {
        emit_runtime_event(
            &events,
            RuntimeEvent::Warning {
                message: format!("ACP runtime failed: {error:#}"),
            },
        )
        .await
        .with_context(|| format!("report ACP runtime failure: {error:#}"))?;
    }
    emit_runtime_event(&events, RuntimeEvent::Stopped).await?;
    result
}

async fn run_inner(
    spec: LaunchSpec,
    requests: mpsc::Receiver<CommandRequest>,
    events: mpsc::Sender<RuntimeEvent>,
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
        let cleanup =
            match tokio::time::timeout(std::time::Duration::from_secs(3), child.wait()).await {
                Ok(Ok(status)) if status.success() => Ok(()),
                Ok(Ok(status)) => Err(anyhow!(
                    "ACP bridge exited with {status} after the protocol runtime completed"
                )),
                Ok(Err(error)) => Err(error).context("wait for ACP bridge shutdown"),
                Err(_) => {
                    let killed = child.kill().await.context("kill unresponsive ACP bridge");
                    let waited = child
                        .wait()
                        .await
                        .context("reap killed ACP bridge")
                        .map(|_| ());
                    match (killed, waited) {
                        (Ok(()), Ok(())) => Ok(()),
                        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
                        (Err(error), Err(wait_error)) => Err(error.context(format!(
                            "also failed to reap killed ACP bridge: {wait_error:#}"
                        ))),
                    }
                }
            };
        if let Err(error) = cleanup {
            merge_runtime_error(&mut result, error);
        }
    }
    let stderr_tail = match stderr_task.await {
        Ok(Ok(tail)) => tail,
        Ok(Err(error)) => {
            merge_runtime_error(&mut result, error);
            String::new()
        }
        Err(error) => {
            merge_runtime_error(
                &mut result,
                anyhow!("ACP stderr collector task failed: {error}"),
            );
            String::new()
        }
    };
    if let Some(stderr_tail) = actionable_stderr_tail(&stderr_tail) {
        result =
            result.map_err(|error| error.context(format!("ACP bridge stderr:\n{stderr_tail}")));
    }
    result
}

const ACP_STDERR_TAIL_BYTES: usize = 16 * 1024;
const UNEXPECTED_PERMISSION_REQUEST_WARNING: &str = "The agent made a permission request, which means its permission policy is misconfigured. Hel is designed to run in either auto-review or YOLO mode.";
/// Chatter the Claude bridge logs for SDK events it does not model, for example
/// `Unexpected case: {"type":"vcs_state_changed"}`. It arrives often enough to
/// fill the whole stderr tail and bury the real failure in worker exit records.
const ADAPTER_CHATTER_PREFIX: &str = "Unexpected case: ";

/// The part of a bridge stderr tail worth attaching to a failing result.
/// Returns `None` when only adapter chatter was captured, so a failure keeps
/// its own error text instead of gaining misleading context.
fn actionable_stderr_tail(tail: &str) -> Option<String> {
    let kept = tail
        .lines()
        .filter(|line| !line.trim_start().starts_with(ADAPTER_CHATTER_PREFIX))
        .collect::<Vec<_>>()
        .join("\n");
    let kept = kept.trim();
    (!kept.is_empty()).then(|| kept.to_owned())
}

async fn emit_runtime_event(
    events: &mpsc::Sender<RuntimeEvent>,
    event: RuntimeEvent,
) -> Result<()> {
    events
        .send(event)
        .await
        .map_err(|_| anyhow!("relay event coordinator stopped"))
}

fn relay_event_channel_error() -> agent_client_protocol::Error {
    agent_client_protocol::Error::internal_error().data(serde_json::Value::String(
        "relay event coordinator stopped".into(),
    ))
}

fn merge_runtime_error(result: &mut Result<()>, additional: anyhow::Error) {
    let previous = std::mem::replace(result, Ok(()));
    *result = match previous {
        Ok(()) => Err(additional),
        Err(error) => Err(error.context(format!("additional ACP runtime error: {additional:#}"))),
    };
}

async fn read_stderr_tail(mut stderr: tokio::process::ChildStderr) -> Result<String> {
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
                return Err(error).context("read ACP bridge stderr");
            }
        }
    }
    Ok(String::from_utf8_lossy(&tail).trim().to_owned())
}

async fn drive<T>(
    transport: T,
    spec: LaunchSpec,
    mut requests: mpsc::Receiver<CommandRequest>,
    events: mpsc::Sender<RuntimeEvent>,
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
                {
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
                }
                let update = serde_json::to_value(notification.update).map_err(|error| {
                    agent_client_protocol::Error::internal_error().data(serde_json::Value::String(
                        format!("serialize ACP session update for relay: {error}"),
                    ))
                })?;
                notification_events
                    .send(RuntimeEvent::SessionUpdate { update })
                    .await
                    .map_err(|_| relay_event_channel_error())?;
                Ok(())
            },
            agent_client_protocol::on_receive_notification!(),
        )
        .on_receive_request(
            async move |request: RequestPermissionRequest, responder, _cx| {
                permission_events
                    .send(RuntimeEvent::Warning {
                        message: UNEXPECTED_PERMISSION_REQUEST_WARNING.to_owned(),
                    })
                    .await
                    .map_err(|_| relay_event_channel_error())?;
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
                permission_events
                    .send(RuntimeEvent::PermissionAutoApproved {
                        option_id: selected.option_id.to_string(),
                        option_name: selected.name.clone(),
                    })
                    .await
                    .map_err(|_| relay_event_channel_error())?;
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

/// Stop reason reported for a turn the bridge rejected instead of finishing.
const PROMPT_ERROR_STOP_REASON: &str = "error";

/// Marker Hel adds to the warning for a prompt the bridge failed with ACP's
/// `auth_required`. The wire message is a bare "Authentication required", too
/// generic for `hel_credentials` to match on text alone, so the error code —
/// not the bridge's wording — decides whether the credential heuristic fires.
pub const PROMPT_AUTH_REQUIRED_MARKER: &str = "ACP auth_required";

fn prompt_failure_warning(error: &agent_client_protocol::Error) -> String {
    if error.code == agent_client_protocol::ErrorCode::AuthRequired {
        format!("prompt failed ({PROMPT_AUTH_REQUIRED_MARKER}): {error}")
    } else {
        format!("prompt failed: {error}")
    }
}

async fn drive_connection(
    connection: ConnectionTo<Agent>,
    spec: &LaunchSpec,
    requests: &mut mpsc::Receiver<CommandRequest>,
    events: &mpsc::Sender<RuntimeEvent>,
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
    emit_runtime_event(
        events,
        RuntimeEvent::Connected {
            agent_name: initialized
                .agent_info
                .as_ref()
                .map(|info| info.name.clone()),
            agent_version: initialized
                .agent_info
                .as_ref()
                .map(|info| info.version.clone()),
            protocol_version: Some(initialized.protocol_version),
            capabilities: Some(Box::new(initialized.agent_capabilities.clone())),
            agent_info: initialized.agent_info.clone(),
        },
    )
    .await?;

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
    emit_runtime_event(
        events,
        RuntimeEvent::SessionStarted {
            native_session_id: session_id.to_string(),
            resumed,
            unrestricted_mode: desired_mode.map(str::to_owned),
        },
    )
    .await?;
    emit_runtime_event(
        events,
        RuntimeEvent::SessionConfigured {
            config_options: config_options.clone(),
        },
    )
    .await?;

    while let Some(request) = requests.recv().await {
        match request {
            CommandRequest::Prompt { request_id, prompt } => {
                if prompt.is_empty() {
                    emit_runtime_event(
                        events,
                        RuntimeEvent::CommandRejected {
                            request_id,
                            message: "ACP prompt has no content blocks".into(),
                        },
                    )
                    .await?;
                    continue;
                }
                let prompt = connection
                    .send_request(PromptRequest::new(session_id.clone(), prompt))
                    .block_task();
                tokio::pin!(prompt);
                loop {
                    tokio::select! {
                        response = &mut prompt => {
                            // A rejected prompt fails the turn, not the worker: the
                            // bridge can still serve later prompts, and a bridge that
                            // really died is caught by the `child.wait()` arm in `run`.
                            let stop_reason = match response {
                                Ok(response) => format!("{:?}", response.stop_reason),
                                Err(error) => {
                                    emit_runtime_event(
                                        events,
                                        RuntimeEvent::Warning {
                                            message: prompt_failure_warning(&error),
                                        },
                                    )
                                    .await?;
                                    PROMPT_ERROR_STOP_REASON.to_owned()
                                }
                            };
                            emit_runtime_event(
                                events,
                                RuntimeEvent::PromptFinished {
                                    request_id,
                                    stop_reason,
                                },
                            )
                            .await?;
                            break;
                        }
                        command = requests.recv() => match command {
                            Some(CommandRequest::Cancel { request_id: cancel_id }) => {
                                match connection.send_notification(CancelNotification::new(session_id.clone())) {
                                    Ok(()) => {
                                        emit_runtime_event(
                                            events,
                                            RuntimeEvent::CancelApplied {
                                                request_id: cancel_id,
                                            },
                                        )
                                        .await?;
                                    }
                                    Err(error) => {
                                        emit_runtime_event(
                                            events,
                                            RuntimeEvent::CommandRejected {
                                                request_id: cancel_id,
                                                message: format!("cancel ACP prompt: {error}"),
                                            },
                                        )
                                        .await?;
                                    }
                                }
                            }
                            Some(CommandRequest::Close { request_id: close_id }) => {
                                if let Err(error) = connection.send_notification(CancelNotification::new(session_id.clone())) {
                                    emit_runtime_event(
                                        events,
                                        RuntimeEvent::Warning {
                                            message: format!("cancel ACP prompt before close: {error}"),
                                        },
                                    )
                                    .await?;
                                }
                                emit_runtime_event(
                                    events,
                                    RuntimeEvent::CommandInterrupted {
                                        request_id: request_id.clone(),
                                        message: "prompt interrupted because the session was closed".into(),
                                    },
                                )
                                .await?;
                                match connection
                                    .send_request(CloseSessionRequest::new(session_id.clone()))
                                    .block_task()
                                    .await
                                {
                                    Ok(_) => {
                                        emit_runtime_event(
                                            events,
                                            RuntimeEvent::CloseApplied {
                                                request_id: close_id,
                                            },
                                        )
                                        .await?;
                                    }
                                    Err(error) => {
                                        emit_runtime_event(
                                            events,
                                            RuntimeEvent::CommandRejected {
                                                request_id: close_id,
                                                message: format!("close ACP session: {error}"),
                                            },
                                        )
                                        .await?;
                                    }
                                }
                                return Ok(());
                            }
                            None => {
                                let cancellation = connection
                                    .send_notification(CancelNotification::new(session_id.clone()));
                                emit_runtime_event(
                                    events,
                                    RuntimeEvent::CommandInterrupted {
                                        request_id: request_id.clone(),
                                        message: "ACP command channel closed while the prompt was running".into(),
                                    },
                                )
                                .await?;
                                cancellation.context("cancel ACP prompt during runtime shutdown")?;
                                return Ok(());
                            }
                            Some(CommandRequest::Prompt { request_id, .. }) => {
                                emit_runtime_event(
                                    events,
                                    RuntimeEvent::CommandRejected {
                                        request_id,
                                        message: "a prompt is already running".into(),
                                    },
                                )
                                .await?;
                            }
                            Some(CommandRequest::SetConfig { request_id, .. }) => {
                                emit_runtime_event(
                                    events,
                                    RuntimeEvent::CommandRejected {
                                        request_id,
                                        message: "configuration can only be changed while the agent is idle".into(),
                                    },
                                )
                                .await?;
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
                        emit_runtime_event(
                            events,
                            RuntimeEvent::ConfigApplied {
                                request_id,
                                key,
                                value,
                                config_options: config_options.clone(),
                            },
                        )
                        .await?;
                    }
                    Err(error) => {
                        emit_runtime_event(
                            events,
                            RuntimeEvent::CommandRejected {
                                request_id,
                                message: format!("{error:#}"),
                            },
                        )
                        .await?;
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
            CommandRequest::Cancel { request_id } => {
                match connection.send_notification(CancelNotification::new(session_id.clone())) {
                    Ok(()) => {
                        emit_runtime_event(events, RuntimeEvent::CancelApplied { request_id })
                            .await?;
                    }
                    Err(error) => {
                        emit_runtime_event(
                            events,
                            RuntimeEvent::CommandRejected {
                                request_id,
                                message: format!("cancel ACP prompt: {error}"),
                            },
                        )
                        .await?;
                    }
                }
            }
            CommandRequest::Close { request_id } => {
                match connection
                    .send_request(CloseSessionRequest::new(session_id.clone()))
                    .block_task()
                    .await
                {
                    Ok(_) => {
                        emit_runtime_event(events, RuntimeEvent::CloseApplied { request_id })
                            .await?;
                    }
                    Err(error) => {
                        emit_runtime_event(
                            events,
                            RuntimeEvent::CommandRejected {
                                request_id,
                                message: format!("close ACP session: {error}"),
                            },
                        )
                        .await?;
                    }
                }
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
    let close_result = connection
        .send_request(CloseSessionRequest::new(session_id.clone()))
        .block_task()
        .await
        .context("close scratch ACP compaction session");
    let output = scratch_outputs
        .lock()
        .expect("scratch output lock poisoned")
        .remove(&session_id.to_string())
        .unwrap_or_default();
    let prompt_result = prompt_result.context("run scratch ACP compaction prompt");
    match (prompt_result, close_result) {
        (Ok(_), Ok(_)) => {}
        (Err(error), Ok(_)) | (Ok(_), Err(error)) => return Err(error),
        (Err(error), Err(close_error)) => {
            return Err(error.context(format!(
                "also failed to close scratch ACP compaction session: {close_error:#}"
            )));
        }
    }
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

    #[tokio::test]
    async fn runtime_event_delivery_waits_for_bounded_channel_capacity() {
        let (events_tx, mut events_rx) = mpsc::channel(1);
        emit_runtime_event(
            &events_tx,
            RuntimeEvent::Warning {
                message: "first".into(),
            },
        )
        .await
        .unwrap();

        let blocked_tx = events_tx.clone();
        let blocked = tokio::spawn(async move {
            emit_runtime_event(
                &blocked_tx,
                RuntimeEvent::Warning {
                    message: "second".into(),
                },
            )
            .await
        });
        tokio::task::yield_now().await;
        assert!(
            !blocked.is_finished(),
            "event producer bypassed bounded-channel backpressure"
        );

        assert!(matches!(
            events_rx.recv().await,
            Some(RuntimeEvent::Warning { message }) if message == "first"
        ));
        blocked.await.unwrap().unwrap();
        assert!(matches!(
            events_rx.recv().await,
            Some(RuntimeEvent::Warning { message }) if message == "second"
        ));
    }

    #[test]
    fn adapter_chatter_never_becomes_error_context() {
        assert_eq!(
            actionable_stderr_tail(
                "Unexpected case: {\"type\":\"vcs_state_changed\"}\nUnexpected case: {\"type\":\"other\"}"
            ),
            None
        );
        assert_eq!(
            actionable_stderr_tail(
                "Unexpected case: {\"type\":\"vcs_state_changed\"}\nnode: out of memory\nUnexpected case: {\"type\":\"other\"}"
            ),
            Some("node: out of memory".to_owned())
        );
        assert_eq!(actionable_stderr_tail("   "), None);
    }

    #[test]
    fn an_auth_required_prompt_failure_carries_the_credential_marker() {
        let auth = prompt_failure_warning(&agent_client_protocol::Error::auth_required());
        assert!(auth.contains("prompt failed"), "{auth}");
        assert!(crate::hel_credentials::auth_failure_signature(
            HarnessKind::Claude,
            &auth
        ));

        let other = prompt_failure_warning(&agent_client_protocol::Error::internal_error());
        assert!(other.contains("prompt failed"), "{other}");
        assert!(!crate::hel_credentials::auth_failure_signature(
            HarnessKind::Claude,
            &other
        ));
    }

    /// Answers `initialize` and `session/new`, then fails the first
    /// `session/prompt` with a JSON-RPC error and completes the second.
    async fn scripted_bridge(stream: tokio::io::DuplexStream) -> usize {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

        let (read, mut write) = tokio::io::split(stream);
        let mut lines = BufReader::new(read).lines();
        let mut prompts = 0_usize;
        while let Some(line) = lines.next_line().await.expect("read scripted bridge input") {
            let request: serde_json::Value =
                serde_json::from_str(&line).expect("bridge input must be JSON-RPC");
            let Some(method) = request.get("method").and_then(serde_json::Value::as_str) else {
                continue;
            };
            let id = request
                .get("id")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            let response = match method {
                "initialize" => {
                    serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {"protocolVersion": 1}})
                }
                "session/new" => {
                    serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {"sessionId": "scripted"}})
                }
                "session/prompt" => {
                    prompts += 1;
                    if prompts == 1 {
                        serde_json::json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "error": {"code": -32000, "message": "Authentication required"},
                        })
                    } else {
                        serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {"stopReason": "end_turn"}})
                    }
                }
                _ => continue,
            };
            if write
                .write_all(format!("{response}\n").as_bytes())
                .await
                .is_err()
            {
                break;
            }
        }
        prompts
    }

    #[tokio::test]
    async fn a_failed_prompt_fails_the_turn_and_the_runtime_keeps_serving() {
        let (client_stream, bridge_stream) = tokio::io::duplex(64 * 1024);
        let bridge = tokio::spawn(scripted_bridge(bridge_stream));
        let (client_read, client_write) = tokio::io::split(client_stream);
        let transport = ByteStreams::new(client_write.compat_write(), client_read.compat());

        let (request_tx, request_rx) = mpsc::channel(4);
        let (event_tx, mut event_rx) = mpsc::channel(16);
        let spec = LaunchSpec {
            command: "scripted".into(),
            args: Vec::new(),
            environment: BTreeMap::new(),
            cwd: std::env::current_dir().unwrap(),
            additional_directories: Vec::new(),
            resume_session: None,
            harness: HarnessKind::Claude,
            force_unrestricted_mode: false,
        };
        let driver = tokio::spawn(drive(transport, spec, request_rx, event_tx));

        let next_event = async |events: &mut mpsc::Receiver<RuntimeEvent>| {
            tokio::time::timeout(std::time::Duration::from_secs(5), events.recv())
                .await
                .expect("the runtime must keep emitting events after a failed prompt")
                .expect("the runtime must not drop its event channel")
        };

        request_tx
            .send(CommandRequest::Prompt {
                request_id: "first".into(),
                prompt: vec![ContentBlock::Text(TextContent::new("hello"))],
            })
            .await
            .unwrap();
        let mut warning = None;
        let failed = loop {
            match next_event(&mut event_rx).await {
                RuntimeEvent::Warning { message } => warning = Some(message),
                RuntimeEvent::PromptFinished {
                    request_id,
                    stop_reason,
                } => break (request_id, stop_reason),
                _ => {}
            }
        };
        assert_eq!(failed, ("first".to_owned(), "error".to_owned()));
        let warning = warning.expect("a failed prompt must warn before it finishes the turn");
        assert!(warning.contains("Authentication required"), "{warning}");
        assert!(crate::hel_credentials::auth_failure_signature(
            HarnessKind::Claude,
            &warning
        ));

        request_tx
            .send(CommandRequest::Prompt {
                request_id: "second".into(),
                prompt: vec![ContentBlock::Text(TextContent::new("still there?"))],
            })
            .await
            .unwrap();
        let completed = loop {
            if let RuntimeEvent::PromptFinished {
                request_id,
                stop_reason,
            } = next_event(&mut event_rx).await
            {
                break (request_id, stop_reason);
            }
        };
        assert_eq!(completed, ("second".to_owned(), "EndTurn".to_owned()));

        drop(request_tx);
        tokio::time::timeout(std::time::Duration::from_secs(5), driver)
            .await
            .expect("closing the command channel must end the runtime")
            .expect("the runtime task must not panic")
            .expect("a failed prompt must not fail the runtime");
        assert_eq!(bridge.await.unwrap(), 2);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bridge_exit_during_initialize_returns_an_actionable_error() {
        let (_request_tx, request_rx) = mpsc::channel(1);
        let (event_tx, mut event_rx) = mpsc::channel(16);
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

    #[cfg(unix)]
    #[tokio::test]
    async fn bridge_launch_failure_is_reported_before_the_runtime_stops() {
        let temp = tempfile::tempdir().unwrap();
        let (_request_tx, request_rx) = mpsc::channel(1);
        let (event_tx, mut event_rx) = mpsc::channel(16);
        let missing_bridge = temp.path().join("missing-acp-bridge");
        let spec = LaunchSpec {
            command: missing_bridge.clone(),
            args: Vec::new(),
            environment: BTreeMap::new(),
            cwd: temp.path().to_path_buf(),
            additional_directories: Vec::new(),
            resume_session: None,
            harness: HarnessKind::Kimi,
            force_unrestricted_mode: true,
        };

        let error = run(spec, request_rx, event_tx).await.unwrap_err();
        assert!(
            format!("{error:#}")
                .contains(&format!("launch ACP bridge {}", missing_bridge.display()))
        );
        assert!(matches!(
            event_rx.recv().await,
            Some(RuntimeEvent::Warning { message }) if message.contains("ACP runtime failed")
        ));
        assert!(matches!(event_rx.recv().await, Some(RuntimeEvent::Stopped)));
    }
}
