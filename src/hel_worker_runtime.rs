//! Target-side daemon and stdio proxy for the durable worker protocol.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::hel_config::HarnessKind;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerLaunchConfig {
    pub session_id: String,
    pub harness: HarnessKind,
    pub bridge_command: PathBuf,
    #[serde(default)]
    pub bridge_args: Vec<String>,
    #[serde(default)]
    pub environment: std::collections::BTreeMap<String, String>,
    pub cwd: PathBuf,
    #[serde(default)]
    pub additional_directories: Vec<PathBuf>,
    pub native_session_id: Option<String>,
}

impl WorkerLaunchConfig {
    pub fn read(path: &Path) -> Result<Self> {
        let body = std::fs::read(path)
            .with_context(|| format!("read worker launch config {}", path.display()))?;
        serde_json::from_slice(&body)
            .with_context(|| format!("parse worker launch config {}", path.display()))
    }

    pub fn write(&self, path: &Path) -> Result<()> {
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent)?;
        let body = serde_json::to_vec_pretty(self)?;
        std::fs::write(path, body)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        }
        Ok(())
    }
}

#[cfg(unix)]
mod unix {
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use anyhow::{Context, Result, bail};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{UnixListener, UnixStream};
    use tokio::sync::mpsc;

    use super::WorkerLaunchConfig;
    use crate::hel_acp::{self, CommandRequest, LaunchSpec, RuntimeEvent};
    use crate::hel_worker::{
        DurableWorker, ErrorCode, PROTOCOL_VERSION, ProtocolError, RequestEnvelope, ResponseBody,
        ResponseEnvelope, ResponsePayload, WorkerRequest,
    };

    pub async fn run_daemon(root: PathBuf, mut config: WorkerLaunchConfig) -> Result<()> {
        super::resolve_relative_harness_home(&mut config, &std::env::current_dir()?);
        std::fs::create_dir_all(&root)
            .with_context(|| format!("create worker root {}", root.display()))?;
        let socket = root.join("control.sock");
        if socket.exists() {
            match UnixStream::connect(&socket).await {
                Ok(_) => bail!("a worker is already running at {}", socket.display()),
                Err(_) => std::fs::remove_file(&socket)
                    .with_context(|| format!("remove stale socket {}", socket.display()))?,
            }
        }
        let listener = UnixListener::bind(&socket)
            .with_context(|| format!("bind worker socket {}", socket.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
        }

        let worker = Arc::new(Mutex::new(DurableWorker::open(
            &root,
            &config.session_id,
            env!("CARGO_PKG_VERSION"),
        )?));
        let (acp_commands_tx, acp_commands_rx) = mpsc::channel(32);
        let (acp_events_tx, mut acp_events_rx) = mpsc::unbounded_channel();
        let acp_spec = LaunchSpec {
            command: config.bridge_command,
            args: config.bridge_args,
            environment: config.environment,
            cwd: config.cwd,
            additional_directories: config.additional_directories,
            resume_session: config.native_session_id,
            harness: config.harness,
        };
        let mut acp_task = tokio::spawn(hel_acp::run(acp_spec, acp_commands_rx, acp_events_tx));

        let event_worker = worker.clone();
        let mut event_task = tokio::spawn(async move {
            while let Some(event) = acp_events_rx.recv().await {
                let value = serde_json::to_value(&event)?;
                let mut worker = event_worker.lock().expect("worker state lock poisoned");
                worker.record_adapter_event(runtime_event_kind(&event), value)?;
                match event {
                    RuntimeEvent::PromptFinished { .. } => {
                        worker.record_turn_completed()?;
                    }
                    RuntimeEvent::Stopped
                        if worker.snapshot().phase == crate::hel_worker::WorkerPhase::Closing =>
                    {
                        worker.record_closed()?;
                    }
                    _ => {}
                }
            }
            Ok::<_, anyhow::Error>(())
        });

        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (stream, _) = accepted.context("accept worker proxy")?;
                    let client_worker = worker.clone();
                    let client_commands = acp_commands_tx.clone();
                    tokio::spawn(async move {
                        if let Err(error) = serve_client(stream, client_worker, client_commands).await {
                            tracing::warn!(%error, "worker proxy client disconnected");
                        }
                    });
                }
                result = &mut event_task => {
                    result.context("worker event task stopped")??;
                    break;
                }
                result = &mut acp_task => {
                    result.context("ACP runtime task stopped")??;
                    break;
                }
            }
        }
        let _ = std::fs::remove_file(socket);
        Ok(())
    }

    async fn serve_client(
        stream: UnixStream,
        worker: Arc<Mutex<DurableWorker>>,
        commands: mpsc::Sender<CommandRequest>,
    ) -> Result<()> {
        let (reader, mut writer) = stream.into_split();
        let mut lines = BufReader::new(reader).lines();
        while let Some(line) = lines.next_line().await? {
            if line.len() > crate::hel_worker::MAX_FRAME_BYTES {
                bail!("worker request frame is too large");
            }
            let envelope: RequestEnvelope =
                serde_json::from_str(&line).context("decode worker request")?;
            let request = envelope.request.clone();
            if let WorkerRequest::Compact { text } = request {
                let response = compact_response(envelope, text, &commands).await;
                let mut encoded = serde_json::to_vec(&response)?;
                encoded.push(b'\n');
                writer.write_all(&encoded).await?;
                writer.flush().await?;
                continue;
            }
            let response = worker
                .lock()
                .expect("worker state lock poisoned")
                .handle(envelope);
            let accepted = accepted(&response);
            let mut encoded = serde_json::to_vec(&response)?;
            encoded.push(b'\n');
            writer.write_all(&encoded).await?;
            writer.flush().await?;
            if accepted && let Some(command) = acp_command(request) {
                commands
                    .send(command)
                    .await
                    .context("ACP runtime stopped")?;
            }
        }
        Ok(())
    }

    async fn compact_response(
        envelope: RequestEnvelope,
        text: String,
        commands: &mpsc::Sender<CommandRequest>,
    ) -> ResponseEnvelope {
        let body = if envelope.protocol_version != PROTOCOL_VERSION {
            ResponseBody::Error {
                error: ProtocolError {
                    code: ErrorCode::IncompatibleProtocol,
                    message: format!(
                        "request uses protocol {}, worker requires {}",
                        envelope.protocol_version, PROTOCOL_VERSION
                    ),
                    retryable: false,
                },
            }
        } else if text.trim().is_empty() {
            ResponseBody::Error {
                error: ProtocolError {
                    code: ErrorCode::InvalidRequest,
                    message: "compaction prompt is empty".into(),
                    retryable: false,
                },
            }
        } else {
            let (response_tx, response_rx) = tokio::sync::oneshot::channel();
            match commands
                .send(CommandRequest::Compact {
                    prompt: text,
                    response: response_tx,
                })
                .await
            {
                Ok(()) => match response_rx.await {
                    Ok(Ok(text)) => ResponseBody::Ok {
                        payload: ResponsePayload::Compacted { text },
                    },
                    Ok(Err(message)) => runtime_compaction_error(&message),
                    Err(_) => runtime_compaction_error("ACP runtime stopped"),
                },
                Err(_) => runtime_compaction_error("ACP runtime stopped"),
            }
        };
        ResponseEnvelope {
            request_id: envelope.request_id,
            protocol_version: PROTOCOL_VERSION,
            body,
        }
    }

    fn runtime_compaction_error(message: &str) -> ResponseBody {
        ResponseBody::Error {
            error: ProtocolError {
                code: ErrorCode::Internal,
                message: message.into(),
                retryable: false,
            },
        }
    }

    fn accepted(response: &ResponseEnvelope) -> bool {
        matches!(
            response.body,
            ResponseBody::Ok {
                payload: ResponsePayload::Accepted { .. }
            }
        )
    }

    fn acp_command(request: WorkerRequest) -> Option<CommandRequest> {
        match request {
            WorkerRequest::Prompt { text, .. } => Some(CommandRequest::Prompt(text)),
            WorkerRequest::SetConfig { key, value } => {
                value.as_str().map(|value| CommandRequest::SetConfig {
                    key,
                    value: value.to_owned(),
                })
            }
            WorkerRequest::Cancel => Some(CommandRequest::Cancel),
            WorkerRequest::Close => Some(CommandRequest::Close),
            _ => None,
        }
    }

    fn runtime_event_kind(event: &RuntimeEvent) -> &'static str {
        match event {
            RuntimeEvent::Connected { .. } => "connected",
            RuntimeEvent::SessionStarted { .. } => "session_started",
            RuntimeEvent::SessionUpdate { .. } => "session_update",
            RuntimeEvent::PermissionAutoApproved { .. } => "permission_auto_approved",
            RuntimeEvent::PromptFinished { .. } => "prompt_finished",
            RuntimeEvent::Warning { .. } => "warning",
            RuntimeEvent::ConfigApplied { .. } => "config_applied",
            RuntimeEvent::Stopped => "stopped",
        }
    }

    pub async fn proxy(root: PathBuf) -> Result<()> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let stream = UnixStream::connect(root.join("control.sock"))
            .await
            .with_context(|| format!("connect worker at {}", root.display()))?;
        let (mut socket_read, mut socket_write) = stream.into_split();
        let mut stdin = tokio::io::stdin();
        let mut stdout = tokio::io::stdout();
        // The proxy must die with its client. Joining both copy directions
        // left the process alive forever after stdin EOF (a killed `podman
        // exec` client), leaking one thread-heavy process per poll inside the
        // container. Exit as soon as either side closes, with an idle
        // watchdog for transports that never deliver EOF.
        const IDLE_LIMIT: std::time::Duration = std::time::Duration::from_secs(15 * 60);
        let mut stdin_buf = [0_u8; 64 * 1024];
        let mut socket_buf = [0_u8; 64 * 1024];
        let idle = tokio::time::sleep(IDLE_LIMIT);
        tokio::pin!(idle);
        loop {
            tokio::select! {
                read = stdin.read(&mut stdin_buf) => {
                    let count = read.context("read proxy stdin")?;
                    if count == 0 {
                        // Client is gone; flush any final in-flight response
                        // briefly, then exit.
                        let _ = socket_write.shutdown().await;
                        let _ = tokio::time::timeout(
                            std::time::Duration::from_millis(500),
                            tokio::io::copy(&mut socket_read, &mut stdout),
                        )
                        .await;
                        return Ok(());
                    }
                    socket_write
                        .write_all(&stdin_buf[..count])
                        .await
                        .context("forward request to worker")?;
                    idle.as_mut().reset(tokio::time::Instant::now() + IDLE_LIMIT);
                }
                read = socket_read.read(&mut socket_buf) => {
                    let count = read.context("read worker socket")?;
                    if count == 0 {
                        return Ok(());
                    }
                    stdout
                        .write_all(&socket_buf[..count])
                        .await
                        .context("forward response to client")?;
                    stdout.flush().await.context("flush proxy stdout")?;
                    idle.as_mut().reset(tokio::time::Instant::now() + IDLE_LIMIT);
                }
                _ = &mut idle => {
                    return Ok(());
                }
            }
        }
    }
}

#[cfg(unix)]
fn resolve_relative_harness_home(config: &mut WorkerLaunchConfig, base: &Path) {
    let key = config.harness.home_env();
    let Some(value) = config.environment.get_mut(key) else {
        return;
    };
    let path = Path::new(value);
    if path.is_relative() {
        *value = base.join(path).to_string_lossy().into_owned();
    }
}

#[cfg(unix)]
pub use unix::{proxy, run_daemon};

#[cfg(not(unix))]
pub async fn run_daemon(
    _root: std::path::PathBuf,
    _config: WorkerLaunchConfig,
) -> anyhow::Result<()> {
    anyhow::bail!("target workers require Unix")
}

#[cfg(not(unix))]
pub async fn proxy(_root: std::path::PathBuf) -> anyhow::Result<()> {
    anyhow::bail!("target workers require Unix")
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn launch_config(profile_home: &str) -> WorkerLaunchConfig {
        WorkerLaunchConfig {
            session_id: "session".into(),
            harness: HarnessKind::Codex,
            bridge_command: "codex-acp".into(),
            bridge_args: Vec::new(),
            environment: BTreeMap::from([("CODEX_HOME".into(), profile_home.into())]),
            cwd: ".local/share/hel/workspaces/session/repo".into(),
            additional_directories: Vec::new(),
            native_session_id: None,
        }
    }

    #[test]
    fn relative_harness_home_is_resolved_before_bridge_changes_directory() {
        let mut config = launch_config(".local/share/hel/profiles/session");

        resolve_relative_harness_home(&mut config, Path::new("/home/ubuntu"));

        assert_eq!(
            config.environment["CODEX_HOME"],
            "/home/ubuntu/.local/share/hel/profiles/session"
        );
    }

    #[test]
    fn absolute_harness_home_is_preserved() {
        let mut config = launch_config("/var/lib/hel/profiles/session");

        resolve_relative_harness_home(&mut config, Path::new("/home/ubuntu"));

        assert_eq!(
            config.environment["CODEX_HOME"],
            "/var/lib/hel/profiles/session"
        );
    }
}
