//! Minimal ACP runtime used by a Hel session worker.
//!
//! The worker owns exactly one harness process and one foreground session.  It
//! deliberately does not know about orchestration, review lanes, or subagents.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Stdio;

use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::schema::v1::{
    CancelNotification, ClientCapabilities, CloseSessionRequest, ContentBlock, Implementation,
    InitializeRequest, LoadSessionRequest, NewSessionRequest, PermissionOptionKind, PromptRequest,
    RequestPermissionOutcome, RequestPermissionRequest, RequestPermissionResponse,
    SelectedPermissionOutcome, SessionConfigKind, SessionConfigOption, SessionConfigOptionCategory,
    SessionConfigValueId, SessionId, SessionNotification, SetSessionConfigOptionRequest,
    SetSessionModeRequest, TextContent,
};
use agent_client_protocol::{Agent, ByteStreams, Client, ConnectTo, ConnectionTo};
use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use tokio::process::Command;
use tokio::sync::mpsc;
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
}

#[derive(Debug)]
pub enum CommandRequest {
    Prompt(String),
    Cancel,
    Close,
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
        unrestricted_mode: String,
    },
    SessionUpdate {
        update: serde_json::Value,
    },
    PermissionAutoApproved {
        option_id: String,
        option_name: String,
    },
    PromptFinished {
        stop_reason: String,
    },
    Warning {
        message: String,
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
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("launch ACP bridge {}", spec.command.display()))?;
    let stdin = child.stdin.take().context("ACP bridge stdin unavailable")?;
    let stdout = child
        .stdout
        .take()
        .context("ACP bridge stdout unavailable")?;
    let transport = ByteStreams::new(stdin.compat_write(), stdout.compat());

    let result = drive(transport, spec, requests, events.clone()).await;
    let _ = child.kill().await;
    let _ = child.wait().await;
    let _ = events.send(RuntimeEvent::Stopped);
    result
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
    Client
        .builder()
        .on_receive_notification(
            async move |notification: SessionNotification, _cx| {
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
            drive_connection(connection, &spec, &mut requests, &events)
                .await
                .map_err(|error| {
                    agent_client_protocol::Error::internal_error()
                        .data(serde_json::Value::String(format!("{error:#}")))
                })
        })
        .await
        .map_err(|error| anyhow!("ACP connection failed: {error}"))
}

async fn drive_connection(
    connection: ConnectionTo<Agent>,
    spec: &LaunchSpec,
    requests: &mut mpsc::Receiver<CommandRequest>,
    events: &mpsc::UnboundedSender<RuntimeEvent>,
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
        // A session that never ran a prompt may have no harness-side history
        // (Codex writes its rollout lazily), so a failed load falls back to a
        // fresh native session instead of killing the worker.
        match connection
            .send_request(
                LoadSessionRequest::new(SessionId::from(existing.clone()), spec.cwd.clone())
                    .additional_directories(spec.additional_directories.clone()),
            )
            .block_task()
            .await
        {
            Ok(loaded) => Some((
                SessionId::from(existing.clone()),
                loaded.config_options,
                loaded.modes,
            )),
            Err(error) => {
                let _ = events.send(RuntimeEvent::Warning {
                    message: format!(
                        "could not load ACP session {existing}; starting a fresh session: {error:#}"
                    ),
                });
                None
            }
        }
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

    let desired_mode = spec.harness.unrestricted_mode();
    enforce_unrestricted_mode(
        &connection,
        &session_id,
        desired_mode,
        config_options.as_deref().unwrap_or_default(),
        modes.as_ref(),
    )
    .await?;
    let _ = events.send(RuntimeEvent::SessionStarted {
        native_session_id: session_id.to_string(),
        resumed,
        unrestricted_mode: desired_mode.to_string(),
    });

    while let Some(request) = requests.recv().await {
        match request {
            CommandRequest::Prompt(text) => {
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
                                stop_reason: format!("{:?}", response.stop_reason),
                            });
                            break;
                        }
                        command = requests.recv() => match command {
                            Some(CommandRequest::Cancel) => {
                                connection.send_notification(CancelNotification::new(session_id.clone()))?;
                            }
                            Some(CommandRequest::Close) | None => {
                                connection.send_notification(CancelNotification::new(session_id.clone()))?;
                                let _ = connection
                                    .send_request(CloseSessionRequest::new(session_id.clone()))
                                    .block_task()
                                    .await;
                                return Ok(());
                            }
                            Some(CommandRequest::Prompt(_)) => {
                                let _ = events.send(RuntimeEvent::Warning {
                                    message: "a prompt is already running".into(),
                                });
                            }
                        }
                    }
                }
            }
            CommandRequest::Cancel => {
                connection.send_notification(CancelNotification::new(session_id.clone()))?;
            }
            CommandRequest::Close => {
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
}
