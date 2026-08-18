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
    CreateTerminalRequest, CreateTerminalResponse, Implementation, InitializeRequest,
    KillTerminalRequest, KillTerminalResponse, LoadSessionRequest, NewSessionRequest,
    PermissionOptionKind, PromptRequest, ReleaseTerminalRequest, ReleaseTerminalResponse,
    RequestPermissionOutcome, RequestPermissionRequest, RequestPermissionResponse,
    SelectedPermissionOutcome, SessionConfigId, SessionConfigKind, SessionConfigOption,
    SessionConfigOptionCategory, SessionConfigValueId, SessionId, SessionNotification,
    SessionUpdate, SetSessionConfigOptionRequest, SetSessionModeRequest, TerminalExitStatus,
    TerminalId, TerminalOutputRequest, TerminalOutputResponse, TextContent,
    WaitForTerminalExitRequest, WaitForTerminalExitResponse,
};
use agent_client_protocol::{Agent, ByteStreams, Client, ConnectTo, ConnectionTo};
use anyhow::{Context, Result, anyhow, bail, ensure};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::{mpsc, oneshot};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::hel_config::{HarnessKind, UnrestrictedEnforcement};
use crate::hel_terminal::{
    DEFAULT_TERMINAL_OUTPUT_BYTES, TerminalExit, TerminalRegistry, TerminalSpawn,
};

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
        // Both auto-compact and expose a native `/compact`.
        HarnessKind::Kimi | HarnessKind::Grok => None,
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
    /// Opaque ACP `session/set_mode`. Used for harnesses whose plan mode is a
    /// session mode rather than an advertised slash command.
    SetSessionMode {
        request_id: String,
        mode_id: String,
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
    /// A client-run terminal was reaped. Exactly one of these is emitted per
    /// terminal, by the supervisor that waits on the child, so kill and
    /// release flow through the same report.
    TerminalClosed {
        terminal_id: String,
        output: String,
        #[serde(default)]
        truncated: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exit_code: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        signal: Option<String>,
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
    SessionModeApplied {
        #[serde(default, skip_serializing_if = "String::is_empty")]
        request_id: String,
        mode_id: String,
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

/// Grok Build's `exit_plan_mode` tool asks the client whether the agent may
/// leave plan mode. It is an ACP ext method, so it reaches Hel untyped, and
/// the ext framing prefixes the method name with `_`. Both spellings are
/// accepted so a bridge that sends the bare name still works.
const EXIT_PLAN_MODE_METHOD: &str = "x.ai/exit_plan_mode";

fn is_exit_plan_mode_method(method: &str) -> bool {
    method.strip_prefix('_').unwrap_or(method) == EXIT_PLAN_MODE_METHOD
}

/// Hel answers `exit_plan_mode` the way it answers a permission request: an
/// unrestricted target approves, so the agent leaves plan mode and proceeds; a
/// restricted target declines, so plan mode stays active and the person
/// decides. `feedback` belongs with `cancelled` only, and Hel has none to
/// give — nobody has read the plan yet.
///
/// The asymmetry with tool permissions on a bare target is deliberate. Grok
/// Build always launches with `--always-approve`, so its writes never ask; but
/// leaving plan mode is a decision about the plan, not a tool permission, so it
/// stays with the person until the target itself is disposable.
fn exit_plan_mode_outcome(auto_approve: bool) -> &'static str {
    if auto_approve {
        "approved"
    } else {
        "cancelled"
    }
}

fn exit_plan_mode_response(auto_approve: bool) -> serde_json::Value {
    serde_json::json!({ "outcome": exit_plan_mode_outcome(auto_approve) })
}

fn exit_plan_mode_report(auto_approve: bool) -> String {
    if auto_approve {
        "The agent asked to leave plan mode. Hel approved it, because this target runs unrestricted.".to_owned()
    } else {
        "The agent asked to leave plan mode. Hel declined, so plan mode stays active and the plan is yours to accept.".to_owned()
    }
}

fn unsupported_client_request_report(method: &str) -> String {
    format!(
        "The agent sent the client request {method}, which Hel does not implement. \
         Hel answered with a method-not-found error rather than leaving the agent waiting."
    )
}

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

/// Answer for a `terminal/*` request naming a terminal this connection does
/// not have, most often one the agent already released.
fn unknown_terminal_error(terminal_id: &str) -> agent_client_protocol::Error {
    agent_client_protocol::Error::invalid_params().data(serde_json::Value::String(format!(
        "unknown terminal {terminal_id}"
    )))
}

fn terminal_exit_status(exit: &TerminalExit) -> TerminalExitStatus {
    TerminalExitStatus::new()
        .exit_code(exit.exit_code)
        .signal(exit.signal.clone())
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
    let ext_events = events.clone();
    let auto_approve_permissions = spec.force_unrestricted_mode;
    let scratch_outputs = Arc::new(Mutex::new(BTreeMap::<String, String>::new()));
    let notification_scratch_outputs = scratch_outputs.clone();
    let terminals = TerminalRegistry::new();
    let create_terminals = terminals.clone();
    let output_terminals = terminals.clone();
    let wait_terminals = terminals.clone();
    let kill_terminals = terminals.clone();
    let release_terminals = terminals.clone();
    let create_events = events.clone();
    let wait_events = events.clone();
    let release_events = events.clone();
    // A terminal runs where the session runs unless the agent names a
    // directory of its own.
    let session_cwd = spec.cwd.clone();
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
        .on_receive_request(
            async move |request: CreateTerminalRequest, responder, _cx| {
                let spawn = TerminalSpawn {
                    command: request.command.clone(),
                    args: request.args.clone(),
                    // Additions, not a replacement: the child inherits the
                    // daemon environment it needs to reach the toolchain.
                    env: request
                        .env
                        .iter()
                        .map(|variable| (variable.name.clone(), variable.value.clone()))
                        .collect(),
                    cwd: request.cwd.clone().unwrap_or_else(|| session_cwd.clone()),
                    output_byte_limit: request
                        .output_byte_limit
                        .and_then(|limit| usize::try_from(limit).ok())
                        .unwrap_or(DEFAULT_TERMINAL_OUTPUT_BYTES),
                };
                match create_terminals.create(spawn, create_events.clone()) {
                    Ok(terminal_id) => responder
                        .respond(CreateTerminalResponse::new(TerminalId::from(terminal_id))),
                    Err(error) => {
                        create_events
                            .send(RuntimeEvent::Warning {
                                message: format!("a client terminal failed to start: {error:#}"),
                            })
                            .await
                            .map_err(|_| relay_event_channel_error())?;
                        responder.respond_with_error(
                            agent_client_protocol::Error::internal_error()
                                .data(serde_json::Value::String(format!("{error:#}"))),
                        )
                    }
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |request: TerminalOutputRequest, responder, _cx| {
                let terminal_id = request.terminal_id.to_string();
                let Some(snapshot) = output_terminals.output(&terminal_id) else {
                    return responder.respond_with_error(unknown_terminal_error(&terminal_id));
                };
                let mut response = TerminalOutputResponse::new(snapshot.output, snapshot.truncated);
                if let Some(exit) = &snapshot.exit {
                    response = response.exit_status(terminal_exit_status(exit));
                }
                responder.respond(response)
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |request: WaitForTerminalExitRequest, responder, _cx| {
                let terminal_id = request.terminal_id.to_string();
                let Some(exit) = wait_terminals.exit_receiver(&terminal_id) else {
                    return responder.respond_with_error(unknown_terminal_error(&terminal_id));
                };
                // Handlers run on the dispatch loop, so awaiting the child here
                // would stop every other message until it exits.
                let events = wait_events.clone();
                tokio::spawn(async move {
                    let exit = crate::hel_terminal::wait_for_exit(exit).await;
                    if let Err(error) = responder.respond(WaitForTerminalExitResponse::new(
                        terminal_exit_status(&exit),
                    )) {
                        // A closed channel means the relay already stopped, so
                        // this warning has nowhere left to go.
                        let _ = events
                            .send(RuntimeEvent::Warning {
                                message: format!(
                                    "report the exit of client terminal {terminal_id}: {error}"
                                ),
                            })
                            .await;
                    }
                });
                Ok(())
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |request: KillTerminalRequest, responder, _cx| {
                let terminal_id = request.terminal_id.to_string();
                // The terminal stays valid: output and wait_for_exit still
                // answer for it until the agent releases it.
                if !kill_terminals.kill(&terminal_id) {
                    return responder.respond_with_error(unknown_terminal_error(&terminal_id));
                }
                responder.respond(KillTerminalResponse::new())
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |request: ReleaseTerminalRequest, responder, _cx| {
                let terminal_id = request.terminal_id.to_string();
                let Some(supervisor) = release_terminals.release(&terminal_id) else {
                    return responder.respond_with_error(unknown_terminal_error(&terminal_id));
                };
                // Reap off the dispatch loop: the supervisor still has to watch
                // the killed child exit before it reports the terminal closed.
                let events = release_events.clone();
                tokio::spawn(async move {
                    if let Err(error) = supervisor.await {
                        let _ = events
                            .send(RuntimeEvent::Warning {
                                message: format!(
                                    "reap released client terminal {terminal_id}: {error}"
                                ),
                            })
                            .await;
                    }
                });
                responder.respond(ReleaseTerminalResponse::new())
            },
            agent_client_protocol::on_receive_request!(),
        )
        // Catch-all, registered last so the typed handlers above win. The ACP
        // crate parks an unhandled request that carries a session id instead of
        // rejecting it, so without this an agent that sends an ext request Hel
        // does not know waits for a reply that never comes, and its turn never
        // ends. Hel answers every incoming request, always.
        .on_receive_request(
            async move |request: agent_client_protocol::UntypedMessage, responder, _cx| {
                let method = request.method().to_owned();
                if is_exit_plan_mode_method(&method) {
                    ext_events
                        .send(RuntimeEvent::Warning {
                            message: exit_plan_mode_report(auto_approve_permissions),
                        })
                        .await
                        .map_err(|_| relay_event_channel_error())?;
                    return responder.respond(exit_plan_mode_response(auto_approve_permissions));
                }
                ext_events
                    .send(RuntimeEvent::Warning {
                        message: unsupported_client_request_report(&method),
                    })
                    .await
                    .map_err(|_| relay_event_channel_error())?;
                responder.respond_with_error(
                    agent_client_protocol::Error::method_not_found()
                        .data(serde_json::Value::String(method)),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .connect_with(transport, |connection: ConnectionTo<Agent>| async move {
            drive_connection(
                connection,
                &spec,
                &mut requests,
                &events,
                scratch_outputs,
                terminals,
            )
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

/// Grok Build speaks an older ACP dialect. It never returns `configOptions`,
/// and model and reasoning effort move through the legacy `session/set_model`
/// method: `{sessionId, modelId}` with the effort as
/// `_meta.reasoningEffort`. Hel synthesizes the same `SessionConfigOption`
/// list every other harness returns, so `/model` and `/effort` behave
/// identically everywhere.
const GROK_SET_MODEL_METHOD: &str = "session/set_model";

/// Grok Build's model catalogue, from `_meta.modelState` on the `initialize`
/// response and `_meta["x.ai/modelState"]`-shaped payloads after a switch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct GrokModelState {
    current_model_id: String,
    current_effort: Option<String>,
    models: Vec<GrokModel>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GrokModel {
    id: String,
    name: String,
    description: Option<String>,
    /// Reasoning tiers this model accepts. Empty when it has none.
    efforts: Vec<GrokChoice>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GrokChoice {
    id: String,
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

/// Read `modelState` out of a `_meta` object. `None` when the agent is not
/// speaking this dialect, which is every harness except Grok Build.
fn grok_model_state(
    meta: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Option<GrokModelState> {
    let state = meta?.get("modelState")?;
    grok_model_state_from_value(state)
}

fn grok_model_state_from_value(state: &serde_json::Value) -> Option<GrokModelState> {
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

/// Present Grok Build's catalogue as the option list the rest of Hel already
/// understands: a `model` selector, plus an `effort` selector carrying the
/// current model's reasoning tiers.
fn grok_config_options(state: &GrokModelState) -> Vec<SessionConfigOption> {
    use agent_client_protocol::schema::v1::{
        SessionConfigSelect, SessionConfigSelectOption, SessionConfigSelectOptions,
    };

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

/// Translate a Hel `/model` or `/effort` change into Grok Build's
/// `session/set_model` request. An effort change re-sends the current model,
/// because effort only travels as meta on that same request.
fn grok_set_model_request(
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
            // A different model has its own tiers, so the old effort no longer
            // applies; the agent reports the new one.
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
    terminals: TerminalRegistry,
) -> Result<()> {
    // Terminals belong to the connection. However the session ends — closed,
    // failed, or with its command channel dropped — their process groups must
    // not outlive it.
    let result = serve_session(&connection, spec, requests, events, scratch_outputs).await;
    terminals.shutdown(events).await;
    result
}

async fn serve_session(
    connection: &ConnectionTo<Agent>,
    spec: &LaunchSpec,
    requests: &mut mpsc::Receiver<CommandRequest>,
    events: &mpsc::Sender<RuntimeEvent>,
    scratch_outputs: Arc<Mutex<BTreeMap<String, String>>>,
) -> Result<()> {
    let mut meta = serde_json::Map::new();
    meta.insert("terminal_output".into(), serde_json::Value::Bool(true));
    // Kimi routes every shell call through the client's terminal surface and
    // has no local fallback, so this capability is what makes Bash work.
    let capabilities = ClientCapabilities::new().terminal(true).meta(meta);
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
    // Grok Build publishes its catalogue here rather than as `configOptions`.
    let mut grok_models = (spec.harness == HarnessKind::Grok)
        .then(|| grok_model_state(initialized.meta.as_ref()))
        .flatten();
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
            // A session may open on a different model than the agent-wide
            // default, so a fresher catalogue on the session wins.
            if let Some(state) = grok_models.as_mut()
                && let Some(fresh) = grok_model_state(created.meta.as_ref())
            {
                *state = fresh;
            }
            (
                created.session_id,
                created.config_options,
                created.modes,
                false,
            )
        };

    // A launch-flag harness was already put in its unrestricted mode when the
    // bridge process started; there is nothing to select over ACP, but the
    // session still reports which mode it runs in.
    let enforcement = spec
        .force_unrestricted_mode
        .then(|| spec.harness.unrestricted_enforcement());
    if let Some(desired_mode) = enforcement.and_then(UnrestrictedEnforcement::acp_mode) {
        enforce_unrestricted_mode(
            connection,
            &session_id,
            desired_mode,
            config_options.as_deref().unwrap_or_default(),
            modes.as_ref(),
        )
        .await?;
    }
    let mut config_options = config_options.unwrap_or_default();
    // Grok Build never returns `configOptions`; present its catalogue in the
    // shape the rest of Hel reads so `/model` and `/effort` work unchanged.
    if let Some(state) = &grok_models
        && config_options.is_empty()
    {
        config_options = grok_config_options(state);
    }
    emit_runtime_event(
        events,
        RuntimeEvent::SessionStarted {
            native_session_id: session_id.to_string(),
            resumed,
            // A launch flag Hel passes on every target is genuinely on even
            // when the target did not force unrestricted mode, so the session
            // reports the mode it really runs in rather than nothing.
            unrestricted_mode: enforcement
                .or_else(|| {
                    spec.harness
                        .launch_flag_for(spec.force_unrestricted_mode)
                        .map(|_| spec.harness.unrestricted_enforcement())
                })
                .map(|enforcement| enforcement.label().to_owned()),
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
                            Some(CommandRequest::SetSessionMode { request_id, .. }) => {
                                emit_runtime_event(
                                    events,
                                    RuntimeEvent::CommandRejected {
                                        request_id,
                                        message: "the session mode can only be changed while the agent is idle".into(),
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
                let applied = match grok_models.as_mut() {
                    Some(state) => set_grok_model(connection, &session_id, state, &key, &value)
                        .await
                        .inspect(|()| config_options = grok_config_options(state)),
                    None => {
                        set_session_config(
                            connection,
                            &session_id,
                            &mut config_options,
                            &key,
                            &value,
                        )
                        .await
                    }
                };
                match applied {
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
            CommandRequest::SetSessionMode {
                request_id,
                mode_id,
            } => {
                match connection
                    .send_request(SetSessionModeRequest::new(
                        session_id.clone(),
                        mode_id.clone(),
                    ))
                    .block_task()
                    .await
                {
                    Ok(_) => {
                        emit_runtime_event(
                            events,
                            RuntimeEvent::SessionModeApplied {
                                request_id,
                                mode_id,
                            },
                        )
                        .await?;
                    }
                    Err(error) => {
                        emit_runtime_event(
                            events,
                            RuntimeEvent::CommandRejected {
                                request_id,
                                message: format!("set session mode to {mode_id}: {error}"),
                            },
                        )
                        .await?;
                    }
                }
            }
            CommandRequest::Compact { prompt, response } => {
                // Compaction is several model turns long. It runs in a scratch
                // session, so the destination stays idle and the coordinator
                // keeps serving cancels, closes and rejections meanwhile.
                let compaction =
                    compact_in_scratch_session(connection, spec, prompt, &scratch_outputs);
                tokio::pin!(compaction);
                let mut response = Some(response);
                loop {
                    tokio::select! {
                        result = &mut compaction => {
                            if let Some(response) = response.take() {
                                let _ = response.send(result.map_err(|error| format!("{error:#}")));
                            }
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
                                if let Some(response) = response.take() {
                                    let _ = response.send(Err("session closed during compaction".into()));
                                }
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
                                    RuntimeEvent::Warning {
                                        message: "a compaction was abandoned because the session was closed".into(),
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
                                if let Some(response) = response.take() {
                                    let _ = response.send(Err(
                                        "ACP command channel closed while a compaction was running".into(),
                                    ));
                                }
                                connection
                                    .send_notification(CancelNotification::new(session_id.clone()))
                                    .context("cancel ACP prompt during runtime shutdown")?;
                                return Ok(());
                            }
                            Some(CommandRequest::Prompt { request_id, .. }) => {
                                emit_runtime_event(
                                    events,
                                    RuntimeEvent::CommandRejected {
                                        request_id,
                                        message: "a compaction is running; retry when it finishes".into(),
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
                            Some(CommandRequest::SetSessionMode { request_id, .. }) => {
                                emit_runtime_event(
                                    events,
                                    RuntimeEvent::CommandRejected {
                                        request_id,
                                        message: "the session mode can only be changed while the agent is idle".into(),
                                    },
                                )
                                .await?;
                            }
                            Some(CommandRequest::Compact { response, .. }) => {
                                let _ = response.send(Err(
                                    "a compaction is already running".into(),
                                ));
                            }
                        }
                    }
                }
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

/// Apply a `/model` or `/effort` change through Grok Build's legacy
/// `session/set_model` method. The ACP crate has no type for it, so the
/// request goes out as an untyped JSON-RPC message.
async fn set_grok_model(
    connection: &ConnectionTo<Agent>,
    session_id: &SessionId,
    state: &mut GrokModelState,
    key: &str,
    value: &str,
) -> Result<()> {
    let (params, updated) = grok_set_model_request(session_id, state, key, value)?;
    connection
        .send_request(
            agent_client_protocol::UntypedMessage::new(GROK_SET_MODEL_METHOD, params)
                .context("build Grok Build set-model request")?,
        )
        .block_task()
        .await
        .with_context(|| format!("set session {key} to {value}"))?;
    *state = updated;
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
    if let Some(desired_mode) = spec
        .force_unrestricted_mode
        .then(|| spec.harness.unrestricted_enforcement())
        .and_then(UnrestrictedEnforcement::acp_mode)
    {
        enforce_unrestricted_mode(
            connection,
            &session_id,
            desired_mode,
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
        assert_eq!(production_compaction_config(HarnessKind::Grok), None);
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

    /// Answers `initialize` and `session/new`, then — while the prompt is in
    /// flight — sends the client an ext request and publishes the answer as
    /// soon as it arrives, so a silent client shows up as a timeout.
    async fn ext_request_bridge(
        stream: tokio::io::DuplexStream,
        method: &'static str,
        answered: tokio::sync::oneshot::Sender<serde_json::Value>,
    ) {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

        let (read, mut write) = tokio::io::split(stream);
        let mut lines = BufReader::new(read).lines();
        let mut answered = Some(answered);
        while let Some(line) = lines.next_line().await.expect("read bridge input") {
            let message: serde_json::Value =
                serde_json::from_str(&line).expect("bridge input must be JSON-RPC");
            if message.get("id").and_then(serde_json::Value::as_str) == Some("ext-1") {
                if let Some(answered) = answered.take() {
                    let _ = answered.send(message);
                }
                continue;
            }
            let Some(request_method) = message.get("method").and_then(serde_json::Value::as_str)
            else {
                continue;
            };
            let id = message
                .get("id")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            let response = match request_method {
                "initialize" => {
                    serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {"protocolVersion": 1}})
                }
                "session/new" => {
                    serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {"sessionId": "scripted"}})
                }
                // Ask the client to leave plan mode without answering the
                // prompt: the turn only ends once the client replies, which is
                // exactly the hang this guards against.
                "session/prompt" => serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": "ext-1",
                    "method": method,
                    "params": {
                        "sessionId": "scripted",
                        "toolCallId": "call-1",
                        "planContent": "1. do the thing",
                    },
                }),
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
    }

    async fn answer_to_ext_request(
        method: &'static str,
        force_unrestricted_mode: bool,
    ) -> serde_json::Value {
        let (client_stream, bridge_stream) = tokio::io::duplex(64 * 1024);
        let (answered_tx, answered_rx) = tokio::sync::oneshot::channel();
        let bridge = tokio::spawn(ext_request_bridge(bridge_stream, method, answered_tx));
        let (client_read, client_write) = tokio::io::split(client_stream);
        let transport = ByteStreams::new(client_write.compat_write(), client_read.compat());

        let (request_tx, request_rx) = mpsc::channel(4);
        let (event_tx, mut event_rx) = mpsc::channel(64);
        // Drain events so a full channel can never be mistaken for silence.
        let events = tokio::spawn(async move { while event_rx.recv().await.is_some() {} });
        let spec = LaunchSpec {
            command: "scripted".into(),
            args: Vec::new(),
            environment: BTreeMap::new(),
            cwd: std::env::current_dir().unwrap(),
            additional_directories: Vec::new(),
            resume_session: None,
            harness: HarnessKind::Grok,
            force_unrestricted_mode,
        };
        let driver = tokio::spawn(drive(transport, spec, request_rx, event_tx));
        request_tx
            .send(CommandRequest::Prompt {
                request_id: "first".into(),
                prompt: vec![ContentBlock::Text(TextContent::new("plan it"))],
            })
            .await
            .unwrap();

        let answer = tokio::time::timeout(std::time::Duration::from_secs(5), answered_rx)
            .await
            .expect("Hel must answer every incoming request instead of leaving the agent waiting")
            .expect("the bridge must publish the answer");

        drop(request_tx);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), driver).await;
        bridge.abort();
        events.abort();
        answer
    }

    /// Modeled on the `_meta.modelState` a signed-in `grok agent stdio`
    /// returns from `initialize`.
    fn grok_model_meta() -> serde_json::Map<String, serde_json::Value> {
        let state = serde_json::json!({
            "currentModelId": "grok-4.6",
            "availableModels": [
                {
                    "modelId": "grok-4.6",
                    "name": "Grok 4.6",
                    "description": "SpaceXAI's latest frontier model",
                    "_meta": {
                        "totalContextTokens": 500_000,
                        "supportsReasoningEffort": true,
                        "reasoningEffort": "high",
                        "reasoningEfforts": [
                            {"id": "xhigh", "value": "xhigh", "label": "Extra High Effort", "description": "Highest effort and reasoning level", "default": true},
                            {"id": "high", "value": "high", "label": "High Effort", "default": true},
                            {"id": "medium", "value": "medium", "label": "Medium Effort", "default": false},
                            {"id": "low", "value": "low", "label": "Low Effort", "default": false}
                        ]
                    }
                },
                {
                    "modelId": "grok-4.5",
                    "name": "Grok 4.5",
                    "_meta": {
                        "supportsReasoningEffort": true,
                        "reasoningEffort": "high",
                        "reasoningEfforts": [
                            {"id": "high", "value": "high", "label": "High Effort", "default": true},
                            {"id": "low", "value": "low", "label": "Low Effort", "default": false}
                        ]
                    }
                }
            ]
        });
        let mut meta = serde_json::Map::new();
        meta.insert("modelState".into(), state);
        meta
    }

    fn select_values(option: &SessionConfigOption) -> Vec<String> {
        let SessionConfigKind::Select(select) = &option.kind else {
            panic!("expected a select option");
        };
        let agent_client_protocol::schema::v1::SessionConfigSelectOptions::Ungrouped(options) =
            &select.options
        else {
            panic!("expected ungrouped options");
        };
        options
            .iter()
            .map(|option| option.value.to_string())
            .collect()
    }

    #[test]
    fn grok_model_state_reads_the_catalogue_out_of_initialize_meta() {
        let state = grok_model_state(Some(&grok_model_meta())).unwrap();

        assert_eq!(state.current_model_id, "grok-4.6");
        assert_eq!(state.current_effort.as_deref(), Some("high"));
        assert_eq!(state.models.len(), 2);
        assert_eq!(state.models[0].name, "Grok 4.6");
        assert_eq!(
            state.models[0]
                .efforts
                .iter()
                .map(|effort| effort.id.as_str())
                .collect::<Vec<_>>(),
            ["xhigh", "high", "medium", "low"]
        );
        assert_eq!(state.models[1].efforts.len(), 2);

        // Every other harness returns real `configOptions`, so nothing is
        // synthesized for them.
        assert_eq!(grok_model_state(None), None);
        assert_eq!(
            grok_model_state(Some(&serde_json::Map::new())),
            None,
            "an agent without a catalogue must not get a synthesized one"
        );
    }

    #[test]
    fn grok_config_options_carry_the_model_and_its_reasoning_tiers() {
        let state = grok_model_state(Some(&grok_model_meta())).unwrap();

        let options = grok_config_options(&state);

        assert_eq!(options.len(), 2);
        assert_eq!(options[0].id.to_string(), "model");
        assert_eq!(
            options[0].category,
            Some(SessionConfigOptionCategory::Model)
        );
        assert_eq!(select_values(&options[0]), ["grok-4.6", "grok-4.5"]);
        assert!(select_contains(&options[0].kind, "grok-4.5"));

        assert_eq!(options[1].id.to_string(), "effort");
        assert_eq!(
            options[1].category,
            Some(SessionConfigOptionCategory::ThoughtLevel)
        );
        // Effort tiers belong to the selected model, not the whole catalogue.
        assert_eq!(
            select_values(&options[1]),
            ["xhigh", "high", "medium", "low"]
        );

        // Hel's existing /model and /effort lookups find both selectors.
        assert_eq!(
            find_session_config_option(&options, "model").map(|option| option.id.to_string()),
            Some("model".to_owned())
        );
        assert_eq!(
            find_session_config_option(&options, "effort").map(|option| option.id.to_string()),
            Some("effort".to_owned())
        );
    }

    #[test]
    fn a_grok_model_change_sends_the_new_id_without_an_effort_meta() {
        let state = grok_model_state(Some(&grok_model_meta())).unwrap();

        let (params, updated) =
            grok_set_model_request(&SessionId::from("s-1"), &state, "model", "grok-4.5").unwrap();

        assert_eq!(
            params,
            serde_json::json!({"sessionId": "s-1", "modelId": "grok-4.5"})
        );
        assert_eq!(updated.current_model_id, "grok-4.5");
        // The new model has its own tiers, so the agent reports the effort.
        assert_eq!(updated.current_effort, None);
        // The effort selector now offers the new model's tiers.
        assert_eq!(
            select_values(&grok_config_options(&updated)[1]),
            ["high", "low"]
        );
    }

    #[test]
    fn a_grok_effort_change_resends_the_current_model_with_the_effort_meta() {
        let state = grok_model_state(Some(&grok_model_meta())).unwrap();

        let (params, updated) =
            grok_set_model_request(&SessionId::from("s-1"), &state, "effort", "low").unwrap();

        assert_eq!(
            params,
            serde_json::json!({
                "sessionId": "s-1",
                "modelId": "grok-4.6",
                "_meta": {"reasoningEffort": "low"},
            })
        );
        assert_eq!(updated.current_model_id, "grok-4.6");
        assert_eq!(updated.current_effort.as_deref(), Some("low"));
        let SessionConfigKind::Select(select) = &grok_config_options(&updated)[1].kind else {
            panic!("expected a select option");
        };
        assert_eq!(select.current_value.to_string(), "low");
    }

    #[test]
    fn grok_rejects_values_and_keys_it_has_no_selector_for() {
        let state = grok_model_state(Some(&grok_model_meta())).unwrap();
        let session = SessionId::from("s-1");

        for (key, value) in [("model", "grok-9"), ("effort", "ludicrous")] {
            let error = grok_set_model_request(&session, &state, key, value).unwrap_err();
            assert!(
                format!("{error:#}").contains(&format!("is not an available {key} value")),
                "{error:#}"
            );
        }
        let error = grok_set_model_request(&session, &state, "verbosity", "high").unwrap_err();
        assert!(
            format!("{error:#}").contains("no verbosity selector"),
            "{error:#}"
        );
    }

    #[test]
    fn exit_plan_mode_is_recognized_with_and_without_the_ext_prefix() {
        assert!(is_exit_plan_mode_method("_x.ai/exit_plan_mode"));
        assert!(is_exit_plan_mode_method("x.ai/exit_plan_mode"));
        assert!(!is_exit_plan_mode_method("_x.ai/other"));
        assert!(!is_exit_plan_mode_method("session/request_permission"));
    }

    #[test]
    fn exit_plan_mode_is_approved_when_unrestricted_and_cancelled_when_restricted() {
        assert_eq!(
            exit_plan_mode_response(true),
            serde_json::json!({"outcome": "approved"})
        );
        // No `feedback`: it is only meaningful with `cancelled`, and Hel has
        // none — the plan has not been read by anyone yet.
        assert_eq!(
            exit_plan_mode_response(false),
            serde_json::json!({"outcome": "cancelled"})
        );
    }

    #[tokio::test]
    async fn an_unrestricted_session_approves_the_agents_exit_plan_mode_request() {
        let answer = answer_to_ext_request("_x.ai/exit_plan_mode", true).await;
        assert_eq!(answer["result"], serde_json::json!({"outcome": "approved"}));
    }

    #[tokio::test]
    async fn a_restricted_session_cancels_the_agents_exit_plan_mode_request() {
        let answer = answer_to_ext_request("_x.ai/exit_plan_mode", false).await;
        assert_eq!(
            answer["result"],
            serde_json::json!({"outcome": "cancelled"})
        );
    }

    #[tokio::test]
    async fn an_unknown_client_request_is_answered_with_an_error_rather_than_silence() {
        let answer = answer_to_ext_request("_someone.example/unknown", true).await;
        assert!(
            answer.get("result").is_none(),
            "an unimplemented request must not be answered with a result: {answer}"
        );
        assert_eq!(
            answer["error"]["code"], -32601,
            "expected a method-not-found error: {answer}"
        );
    }

    /// Answers `initialize` (with or without Grok Build's model catalogue) and
    /// `session/new`, then records the request Hel sends for a config change.
    async fn config_change_bridge(
        stream: tokio::io::DuplexStream,
        model_catalogue: bool,
        observed: tokio::sync::oneshot::Sender<serde_json::Value>,
    ) {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

        let (read, mut write) = tokio::io::split(stream);
        let mut lines = BufReader::new(read).lines();
        let mut observed = Some(observed);
        while let Some(line) = lines.next_line().await.expect("read bridge input") {
            let message: serde_json::Value =
                serde_json::from_str(&line).expect("bridge input must be JSON-RPC");
            let Some(method) = message.get("method").and_then(serde_json::Value::as_str) else {
                continue;
            };
            let id = message
                .get("id")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            let response = match method {
                "initialize" => {
                    let mut result = serde_json::json!({"protocolVersion": 1});
                    if model_catalogue {
                        result["_meta"] = serde_json::Value::Object(grok_model_meta());
                    }
                    serde_json::json!({"jsonrpc": "2.0", "id": id, "result": result})
                }
                "session/new" => serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {
                        "sessionId": "scripted",
                        // A plain harness answers with real config options.
                        "configOptions": (!model_catalogue).then(|| serde_json::json!([{
                            "id": "model",
                            "name": "Model",
                            "category": "model",
                            "type": "select",
                            "currentValue": "sonnet",
                            "options": [{"value": "sonnet", "name": "Sonnet"},
                                        {"value": "opus", "name": "Opus"}],
                        }])),
                    },
                }),
                _ => {
                    if let Some(observed) = observed.take() {
                        let _ = observed.send(message.clone());
                    }
                    serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {}})
                }
            };
            if write
                .write_all(format!("{response}\n").as_bytes())
                .await
                .is_err()
            {
                break;
            }
        }
    }

    async fn config_change_request(
        harness: HarnessKind,
        model_catalogue: bool,
        key: &str,
        value: &str,
    ) -> serde_json::Value {
        let (client_stream, bridge_stream) = tokio::io::duplex(64 * 1024);
        let (observed_tx, observed_rx) = tokio::sync::oneshot::channel();
        let bridge = tokio::spawn(config_change_bridge(
            bridge_stream,
            model_catalogue,
            observed_tx,
        ));
        let (client_read, client_write) = tokio::io::split(client_stream);
        let transport = ByteStreams::new(client_write.compat_write(), client_read.compat());

        let (request_tx, request_rx) = mpsc::channel(4);
        let (event_tx, mut event_rx) = mpsc::channel(64);
        let events = tokio::spawn(async move { while event_rx.recv().await.is_some() {} });
        let spec = LaunchSpec {
            command: "scripted".into(),
            args: Vec::new(),
            environment: BTreeMap::new(),
            cwd: std::env::current_dir().unwrap(),
            additional_directories: Vec::new(),
            resume_session: None,
            harness,
            force_unrestricted_mode: false,
        };
        let driver = tokio::spawn(drive(transport, spec, request_rx, event_tx));
        request_tx
            .send(CommandRequest::SetConfig {
                request_id: "config-1".into(),
                key: key.to_owned(),
                value: value.to_owned(),
            })
            .await
            .unwrap();

        let observed = tokio::time::timeout(std::time::Duration::from_secs(5), observed_rx)
            .await
            .expect("Hel must send a configuration request")
            .expect("the bridge must publish the request");

        drop(request_tx);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), driver).await;
        bridge.abort();
        events.abort();
        observed
    }

    #[tokio::test]
    async fn a_grok_effort_change_goes_out_as_a_legacy_set_model_request() {
        let request = config_change_request(HarnessKind::Grok, true, "effort", "low").await;

        assert_eq!(request["method"], "session/set_model");
        assert_eq!(request["params"]["sessionId"], "scripted");
        assert_eq!(request["params"]["modelId"], "grok-4.6");
        assert_eq!(request["params"]["_meta"]["reasoningEffort"], "low");
    }

    #[tokio::test]
    async fn a_grok_model_change_goes_out_as_a_legacy_set_model_request() {
        let request = config_change_request(HarnessKind::Grok, true, "model", "grok-4.5").await;

        assert_eq!(request["method"], "session/set_model");
        assert_eq!(request["params"]["modelId"], "grok-4.5");
        assert!(
            request["params"].get("_meta").is_none(),
            "a model change carries no effort meta: {request}"
        );
    }

    #[tokio::test]
    async fn a_harness_with_real_config_options_still_uses_the_standard_acp_request() {
        let request = config_change_request(HarnessKind::Claude, false, "model", "opus").await;

        assert_eq!(request["method"], "session/set_config_option");
        assert_eq!(request["params"]["configId"], "model");
        assert_eq!(request["params"]["value"], "opus");
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

    /// Answers `initialize` and both `session/new` calls, then leaves the
    /// scratch `session/prompt` unanswered so a compaction stays in flight.
    /// Every method it sees is republished so a test can wait for one.
    async fn stalled_compaction_bridge(
        stream: tokio::io::DuplexStream,
        observed: mpsc::UnboundedSender<String>,
    ) {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

        let (read, mut write) = tokio::io::split(stream);
        let mut lines = BufReader::new(read).lines();
        let mut sessions = 0_usize;
        while let Some(line) = lines.next_line().await.expect("read bridge input") {
            let message: serde_json::Value =
                serde_json::from_str(&line).expect("bridge input must be JSON-RPC");
            let Some(method) = message.get("method").and_then(serde_json::Value::as_str) else {
                continue;
            };
            let _ = observed.send(method.to_owned());
            let id = message
                .get("id")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            let response = match method {
                "initialize" => {
                    serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {"protocolVersion": 1}})
                }
                "session/new" => {
                    sessions += 1;
                    let session = if sessions == 1 {
                        "destination"
                    } else {
                        "scratch"
                    };
                    serde_json::json!({"jsonrpc": "2.0", "id": id, "result": {"sessionId": session}})
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
    }

    #[tokio::test]
    async fn a_cancel_is_served_while_a_compaction_is_in_flight() {
        let (client_stream, bridge_stream) = tokio::io::duplex(64 * 1024);
        let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
        let bridge = tokio::spawn(stalled_compaction_bridge(bridge_stream, observed_tx));
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
            // Kimi has no production compactor, so the scratch session goes
            // straight from `session/new` to the prompt that stalls.
            harness: HarnessKind::Kimi,
            force_unrestricted_mode: false,
        };
        let driver = tokio::spawn(drive(transport, spec, request_rx, event_tx));

        let (compacted_tx, mut compacted_rx) = oneshot::channel();
        request_tx
            .send(CommandRequest::Compact {
                prompt: "summarize the transcript".into(),
                response: compacted_tx,
            })
            .await
            .unwrap();
        loop {
            let method =
                tokio::time::timeout(std::time::Duration::from_secs(5), observed_rx.recv())
                    .await
                    .expect("the compaction must reach the scratch prompt")
                    .expect("the bridge must keep reporting methods");
            if method == "session/prompt" {
                break;
            }
        }

        request_tx
            .send(CommandRequest::Cancel {
                request_id: "cancel-1".into(),
            })
            .await
            .unwrap();
        let applied = loop {
            let event = tokio::time::timeout(std::time::Duration::from_secs(5), event_rx.recv())
                .await
                .expect("the coordinator must keep serving while a compaction runs")
                .expect("the runtime must not drop its event channel");
            if let RuntimeEvent::CancelApplied { request_id } = event {
                break request_id;
            }
        };
        assert_eq!(applied, "cancel-1");
        assert!(
            compacted_rx.try_recv().is_err(),
            "the cancel must be served without ending the compaction"
        );

        drop(request_tx);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), driver).await;
        bridge.abort();
    }

    /// Terminals run real children in real process groups, which only Unix has.
    #[cfg(unix)]
    mod terminals {
        use super::*;

        /// Every wait carries this bound, so a handler that stalls the dispatch
        /// loop or a child that deadlocks on a full pipe fails the test in
        /// seconds instead of hanging the suite.
        const ANSWER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

        /// Answers `initialize` and `session/new`, writes the requests a test
        /// scripts, and republishes every answer Hel sends back.
        async fn client_request_bridge(
            stream: tokio::io::DuplexStream,
            mut scripted: mpsc::UnboundedReceiver<serde_json::Value>,
            answers: mpsc::UnboundedSender<serde_json::Value>,
        ) {
            use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

            let (read, mut write) = tokio::io::split(stream);
            let mut lines = BufReader::new(read).lines();
            loop {
                let outgoing = tokio::select! {
                    line = lines.next_line() => {
                        let Some(line) = line.expect("read bridge input") else {
                            break;
                        };
                        let message: serde_json::Value =
                            serde_json::from_str(&line).expect("bridge input must be JSON-RPC");
                        let Some(method) =
                            message.get("method").and_then(serde_json::Value::as_str)
                        else {
                            // No method: an answer to one of the scripted requests.
                            if answers.send(message).is_err() {
                                break;
                            }
                            continue;
                        };
                        let id = message
                            .get("id")
                            .cloned()
                            .unwrap_or(serde_json::Value::Null);
                        match method {
                            "initialize" => serde_json::json!({
                                "jsonrpc": "2.0", "id": id, "result": {"protocolVersion": 1},
                            }),
                            "session/new" => serde_json::json!({
                                "jsonrpc": "2.0", "id": id, "result": {"sessionId": "scripted"},
                            }),
                            _ => continue,
                        }
                    }
                    request = scripted.recv() => {
                        let Some(request) = request else {
                            break;
                        };
                        request
                    }
                };
                if write
                    .write_all(format!("{outgoing}\n").as_bytes())
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }

        /// The agent side of a scripted connection. Answers are collected by
        /// id, so a test can keep one request in flight while it sends others.
        struct ScriptedAgent {
            scripted: mpsc::UnboundedSender<serde_json::Value>,
            answers: mpsc::UnboundedReceiver<serde_json::Value>,
            received: BTreeMap<String, serde_json::Value>,
            sent: usize,
        }

        impl ScriptedAgent {
            fn send(&mut self, method: &str, params: serde_json::Value) -> String {
                self.sent += 1;
                let id = format!("agent-{}", self.sent);
                self.scripted
                    .send(serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "method": method,
                        "params": params,
                    }))
                    .expect("the scripted bridge must accept requests");
                id
            }

            async fn answer(&mut self, id: &str) -> serde_json::Value {
                loop {
                    if let Some(answer) = self.received.remove(id) {
                        return answer;
                    }
                    let answer = tokio::time::timeout(ANSWER_TIMEOUT, self.answers.recv())
                        .await
                        .expect(
                            "Hel must answer every terminal request instead of leaving the agent waiting",
                        )
                        .expect("the bridge must keep publishing answers");
                    let answer_id = answer
                        .get("id")
                        .and_then(serde_json::Value::as_str)
                        .expect("an answer must carry the request id")
                        .to_owned();
                    self.received.insert(answer_id, answer);
                }
            }

            async fn call(&mut self, method: &str, params: serde_json::Value) -> serde_json::Value {
                let id = self.send(method, params);
                self.answer(&id).await
            }
        }

        struct ScriptedRuntime {
            agent: ScriptedAgent,
            observed: Arc<Mutex<Vec<RuntimeEvent>>>,
            requests: mpsc::Sender<CommandRequest>,
            driver: tokio::task::JoinHandle<Result<()>>,
            bridge: tokio::task::JoinHandle<()>,
            events: tokio::task::JoinHandle<()>,
        }

        impl ScriptedRuntime {
            /// Close the command channel and wait for the runtime to finish,
            /// which is also what tears the terminals down.
            async fn stop(self) {
                drop(self.requests);
                tokio::time::timeout(ANSWER_TIMEOUT, self.driver)
                    .await
                    .expect("closing the command channel must end the runtime")
                    .expect("the runtime task must not panic")
                    .expect("terminal work must not fail the runtime");
                self.bridge.abort();
                self.events.abort();
            }
        }

        fn start_scripted_runtime() -> ScriptedRuntime {
            let (client_stream, bridge_stream) = tokio::io::duplex(64 * 1024);
            let (scripted_tx, scripted_rx) = mpsc::unbounded_channel();
            let (answers_tx, answers_rx) = mpsc::unbounded_channel();
            let bridge = tokio::spawn(client_request_bridge(
                bridge_stream,
                scripted_rx,
                answers_tx,
            ));
            let (client_read, client_write) = tokio::io::split(client_stream);
            let transport = ByteStreams::new(client_write.compat_write(), client_read.compat());

            let (request_tx, request_rx) = mpsc::channel(4);
            let (event_tx, mut event_rx) = mpsc::channel(64);
            // Drain events so a full channel can never be mistaken for silence,
            // and keep them so a test can read what the runtime reported.
            let observed = Arc::new(Mutex::new(Vec::new()));
            let recorder = observed.clone();
            let events = tokio::spawn(async move {
                while let Some(event) = event_rx.recv().await {
                    recorder
                        .lock()
                        .expect("observed events lock poisoned")
                        .push(event);
                }
            });
            let spec = LaunchSpec {
                command: "scripted".into(),
                args: Vec::new(),
                environment: BTreeMap::new(),
                cwd: std::env::current_dir().unwrap(),
                additional_directories: Vec::new(),
                resume_session: None,
                harness: HarnessKind::Kimi,
                force_unrestricted_mode: false,
            };
            let driver = tokio::spawn(drive(transport, spec, request_rx, event_tx));
            ScriptedRuntime {
                agent: ScriptedAgent {
                    scripted: scripted_tx,
                    answers: answers_rx,
                    received: BTreeMap::new(),
                    sent: 0,
                },
                observed,
                requests: request_tx,
                driver,
                bridge,
                events,
            }
        }

        fn terminal_params(terminal_id: &str) -> serde_json::Value {
            serde_json::json!({"sessionId": "scripted", "terminalId": terminal_id})
        }

        /// Every close report a terminal made. Waits for the first, then keeps
        /// watching: a second report would arrive right behind it.
        async fn terminal_close_reports(
            observed: &Arc<Mutex<Vec<RuntimeEvent>>>,
            terminal_id: &str,
        ) -> Vec<RuntimeEvent> {
            let reports = || {
                observed
                    .lock()
                    .expect("observed events lock poisoned")
                    .iter()
                    .filter(|event| {
                        matches!(event, RuntimeEvent::TerminalClosed { terminal_id: id, .. }
                            if id == terminal_id)
                    })
                    .cloned()
                    .collect::<Vec<_>>()
            };
            for _ in 0..100 {
                if !reports().is_empty() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            reports()
        }

        async fn create_terminal(agent: &mut ScriptedAgent, params: serde_json::Value) -> String {
            let created = agent.call("terminal/create", params).await;
            assert!(
                created.get("result").is_some(),
                "terminal/create must be answered with a result, not the catch-all's \
                 method-not-found error: {created}"
            );
            created["result"]["terminalId"]
                .as_str()
                .unwrap_or_else(|| panic!("terminal/create must return a terminal id: {created}"))
                .to_owned()
        }

        #[tokio::test]
        async fn terminal_create_output_wait_and_release_round_trip() {
            let mut runtime = start_scripted_runtime();
            let terminal_id = create_terminal(
                &mut runtime.agent,
                serde_json::json!({
                    "sessionId": "scripted",
                    "command": "/bin/sh",
                    // `PATH` proves the daemon environment is inherited rather
                    // than replaced by the agent's additions.
                    "args": ["-c", "printf 'ran %s %s' \"$HEL_TERMINAL_TEST\" \"${PATH:+inherited}\""],
                    "env": [{"name": "HEL_TERMINAL_TEST", "value": "overlaid"}],
                }),
            )
            .await;

            let exited = runtime
                .agent
                .call("terminal/wait_for_exit", terminal_params(&terminal_id))
                .await;
            assert_eq!(exited["result"]["exitCode"], 0, "{exited}");

            let output = runtime
                .agent
                .call("terminal/output", terminal_params(&terminal_id))
                .await;
            assert_eq!(output["result"]["output"], "ran overlaid inherited");
            assert_eq!(output["result"]["truncated"], false);
            assert_eq!(output["result"]["exitStatus"]["exitCode"], 0);

            let released = runtime
                .agent
                .call("terminal/release", terminal_params(&terminal_id))
                .await;
            assert!(released.get("result").is_some(), "{released}");

            // A released terminal is gone, and Hel says so rather than hanging.
            let stale = runtime
                .agent
                .call("terminal/output", terminal_params(&terminal_id))
                .await;
            assert_eq!(stale["error"]["code"], -32602, "{stale}");
            assert!(
                stale["error"]["data"]
                    .as_str()
                    .is_some_and(|data| data.contains(&terminal_id)),
                "the error must name the terminal: {stale}"
            );

            runtime.stop().await;
        }

        #[tokio::test]
        async fn terminal_output_keeps_the_last_bytes_when_a_child_exceeds_the_limit() {
            let mut runtime = start_scripted_runtime();
            // 512 KiB is far past the 64 KiB pipe buffer: a supervisor that did
            // not drain the pipes while the child ran would block it forever,
            // and the answer timeouts would report that as a failure.
            let script = "data=0123456789abcdef; \
                          while [ ${#data} -lt 524288 ]; do data=\"$data$data\"; done; \
                          printf '%s' \"$data\"; printf 'TAIL-MARKER'";
            let limit = 8 * 1024;
            let terminal_id = create_terminal(
                &mut runtime.agent,
                serde_json::json!({
                    "sessionId": "scripted",
                    "command": "/bin/sh",
                    "args": ["-c", script],
                    "outputByteLimit": limit,
                }),
            )
            .await;

            let exited = runtime
                .agent
                .call("terminal/wait_for_exit", terminal_params(&terminal_id))
                .await;
            assert_eq!(exited["result"]["exitCode"], 0, "{exited}");

            let output = runtime
                .agent
                .call("terminal/output", terminal_params(&terminal_id))
                .await;
            let text = output["result"]["output"]
                .as_str()
                .unwrap_or_else(|| panic!("terminal/output must serve text: {output}"));
            assert!(
                text.len() <= limit,
                "served {} bytes for a {limit} byte limit",
                text.len()
            );
            assert!(
                text.ends_with("TAIL-MARKER"),
                "the retained output must be the tail, ended with {:?}",
                &text[text.len().saturating_sub(32)..]
            );
            assert_eq!(output["result"]["truncated"], true, "{output}");

            runtime.stop().await;
        }

        #[tokio::test]
        async fn terminal_kill_reports_the_signal_and_keeps_output_readable() {
            let mut runtime = start_scripted_runtime();
            let terminal_id = create_terminal(
                &mut runtime.agent,
                serde_json::json!({
                    "sessionId": "scripted",
                    "command": "/bin/sh",
                    "args": ["-c", "printf running; sleep 300"],
                }),
            )
            .await;

            // The wait stays outstanding while the terminal runs: an inline
            // wait would stall the dispatch loop and nothing below could be
            // answered.
            let waiting = runtime
                .agent
                .send("terminal/wait_for_exit", terminal_params(&terminal_id));
            let mut running = String::new();
            for _ in 0..100 {
                let polled = runtime
                    .agent
                    .call("terminal/output", terminal_params(&terminal_id))
                    .await;
                running = polled["result"]["output"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned();
                if running == "running" {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            assert_eq!(running, "running", "a live terminal must serve its output");

            let killed = runtime
                .agent
                .call("terminal/kill", terminal_params(&terminal_id))
                .await;
            assert!(killed.get("result").is_some(), "{killed}");

            let exited = runtime.agent.answer(&waiting).await;
            assert_eq!(exited["result"]["signal"], "SIGKILL", "{exited}");
            assert!(
                exited["result"].get("exitCode").is_none(),
                "a killed terminal has no exit code: {exited}"
            );

            // A kill does not release the terminal.
            let after = runtime
                .agent
                .call("terminal/output", terminal_params(&terminal_id))
                .await;
            assert_eq!(after["result"]["output"], "running");
            assert_eq!(after["result"]["exitStatus"]["signal"], "SIGKILL");

            let released = runtime
                .agent
                .call("terminal/release", terminal_params(&terminal_id))
                .await;
            assert!(released.get("result").is_some(), "{released}");

            // The transcript gets one report per terminal, from whichever of
            // kill, release, or teardown reaped the child.
            let observed = runtime.observed.clone();
            runtime.stop().await;
            let reports = terminal_close_reports(&observed, &terminal_id).await;
            assert_eq!(
                reports.len(),
                1,
                "a killed and released terminal must report its close once: {reports:?}"
            );
            let RuntimeEvent::TerminalClosed { output, signal, .. } = &reports[0] else {
                panic!("expected a terminal close report: {reports:?}");
            };
            assert_eq!(output, "running");
            assert_eq!(signal.as_deref(), Some("SIGKILL"));
        }

        #[tokio::test]
        async fn terminal_create_accepts_a_grok_style_single_string_command() {
            let mut runtime = start_scripted_runtime();
            // Grok Build puts the whole shell line in `command` and sends no
            // arguments at all.
            let terminal_id = create_terminal(
                &mut runtime.agent,
                serde_json::json!({
                    "sessionId": "scripted",
                    "command": "/bin/sh -c 'printf grok-ok'",
                    "args": [],
                }),
            )
            .await;

            let exited = runtime
                .agent
                .call("terminal/wait_for_exit", terminal_params(&terminal_id))
                .await;
            assert_eq!(exited["result"]["exitCode"], 0, "{exited}");

            let output = runtime
                .agent
                .call("terminal/output", terminal_params(&terminal_id))
                .await;
            assert_eq!(output["result"]["output"], "grok-ok", "{output}");

            runtime.stop().await;
        }

        /// A process still visible but already dead — a zombie waiting for its
        /// parent — counts as gone; the parent died with it.
        fn process_is_gone(pid: i32) -> bool {
            // SAFETY: signal 0 only probes whether the process exists.
            if unsafe { libc::kill(pid, 0) } != 0 {
                return true;
            }
            std::fs::read_to_string(format!("/proc/{pid}/stat"))
                .ok()
                .and_then(|stat| {
                    stat.rsplit(')')
                        .next()
                        .map(|rest| rest.trim_start().starts_with('Z'))
                })
                .unwrap_or(false)
        }

        /// A shell that keeps a grandchild alive and publishes both pids, so a
        /// test can prove a kill reached the whole process group rather than
        /// only the shell Hel spawned.
        async fn start_terminal_with_a_grandchild(
            runtime: &mut ScriptedRuntime,
            pids_path: &std::path::Path,
        ) -> Vec<i32> {
            let script = format!(
                "sleep 300 & printf '%s %s' \"$$\" \"$!\" > '{}'; wait",
                pids_path.display()
            );
            create_terminal(
                &mut runtime.agent,
                serde_json::json!({
                    "sessionId": "scripted",
                    "command": "/bin/sh",
                    "args": ["-c", script],
                }),
            )
            .await;

            let mut pids = Vec::new();
            for _ in 0..250 {
                if let Ok(recorded) = std::fs::read_to_string(pids_path) {
                    pids = recorded
                        .split_whitespace()
                        .filter_map(|pid| pid.parse::<i32>().ok())
                        .collect();
                    if pids.len() == 2 {
                        break;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            assert_eq!(pids.len(), 2, "the terminal must report both of its pids");
            pids
        }

        async fn assert_processes_are_gone(pids: &[i32]) {
            for pid in pids {
                let mut gone = false;
                for _ in 0..250 {
                    if process_is_gone(*pid) {
                        gone = true;
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
                assert!(gone, "process {pid} survived the runtime that started it");
            }
        }

        #[tokio::test]
        async fn runtime_teardown_kills_terminal_process_groups() {
            let temp = tempfile::tempdir().unwrap();
            let pids_path = temp.path().join("pids");
            let mut runtime = start_scripted_runtime();
            // Nothing killed or released this terminal: teardown owns it.
            let pids = start_terminal_with_a_grandchild(&mut runtime, &pids_path).await;

            runtime.stop().await;

            assert_processes_are_gone(&pids).await;
        }

        #[tokio::test]
        async fn dropping_the_connection_kills_terminal_process_groups() {
            let temp = tempfile::tempdir().unwrap();
            let pids_path = temp.path().join("pids");
            let mut runtime = start_scripted_runtime();
            let pids = start_terminal_with_a_grandchild(&mut runtime, &pids_path).await;

            // A bridge that dies mid-session leaves the runtime dropping the
            // whole connection rather than ending its command loop, so orderly
            // teardown never runs and the terminals still must not survive.
            runtime.driver.abort();

            assert_processes_are_gone(&pids).await;
            runtime.bridge.abort();
            runtime.events.abort();
        }
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
