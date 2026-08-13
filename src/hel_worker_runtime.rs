//! Target-side daemon and stdio proxy for the durable worker protocol.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::hel_config::HarnessKind;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerOwnership {
    pub version: u32,
    pub session_id: String,
    pub profile_id: String,
    pub bundle_id: String,
    pub target_template_id: String,
}

impl WorkerOwnership {
    pub const VERSION: u32 = 1;

    pub fn write(&self, path: &Path) -> Result<()> {
        let body = serde_json::to_vec(self)?;
        crate::hel_config::atomic_write(path, &body)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcpSupervisorSpec {
    pub command: PathBuf,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub environment: std::collections::BTreeMap<String, String>,
    pub cwd: PathBuf,
}

impl AcpSupervisorSpec {
    pub fn read(path: &Path) -> Result<Self> {
        let body = std::fs::read(path)
            .with_context(|| format!("read ACP supervisor spec {}", path.display()))?;
        serde_json::from_slice(&body)
            .with_context(|| format!("parse ACP supervisor spec {}", path.display()))
    }

    #[cfg(unix)]
    fn write(&self, path: &Path) -> Result<()> {
        let body = serde_json::to_vec_pretty(self)?;
        crate::hel_config::atomic_write(path, &body)
    }
}

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
    /// Recover a legacy native identity from canonical events when neither
    /// the controller nor the worker has persisted one. Cross-harness resume
    /// disables this because restored history belongs to the source harness.
    #[serde(default = "default_recover_native_session")]
    pub recover_native_session: bool,
}

const fn default_recover_native_session() -> bool {
    true
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
    use std::collections::{BTreeSet, VecDeque};
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use agent_client_protocol::schema::v1::{SessionUpdate, ToolCallStatus};
    use anyhow::{Context, Result, bail};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{UnixListener, UnixStream};
    use tokio::sync::{mpsc, oneshot};

    use super::{AcpSupervisorSpec, WorkerLaunchConfig};
    use crate::hel_acp::{self, CommandRequest, LaunchSpec, RuntimeEvent};
    use crate::hel_worker::{
        DurableWorker, ErrorCode, PROTOCOL_VERSION, ProtocolError, RequestEnvelope, ResponseBody,
        ResponseEnvelope, ResponsePayload, WorkerRequest,
    };
    use crate::hel_worker_protocol::DecodedRequest;

    struct SocketGuard(PathBuf);

    pub(super) struct CheckpointBarrier {
        pub(super) envelope: RequestEnvelope,
        pub(super) response: oneshot::Sender<ResponseEnvelope>,
    }

    #[derive(Default)]
    struct ToolActivity {
        open: BTreeSet<String>,
    }

    impl ToolActivity {
        fn observe(&mut self, event: &RuntimeEvent) {
            let RuntimeEvent::SessionUpdate { update } = event else {
                return;
            };
            let Ok(update) = serde_json::from_value::<SessionUpdate>(update.clone()) else {
                return;
            };
            match update {
                SessionUpdate::ToolCall(call) => {
                    self.set_status(call.tool_call_id.to_string(), call.status);
                }
                SessionUpdate::ToolCallUpdate(update) => {
                    if let Some(status) = update.fields.status {
                        self.set_status(update.tool_call_id.to_string(), status);
                    }
                }
                _ => {}
            }
        }

        fn set_status(&mut self, id: String, status: ToolCallStatus) {
            if matches!(status, ToolCallStatus::Completed | ToolCallStatus::Failed) {
                self.open.remove(&id);
            } else {
                self.open.insert(id);
            }
        }

        fn is_quiescent(&self) -> bool {
            self.open.is_empty()
        }
    }

    impl Drop for SocketGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    pub async fn run_daemon(root: PathBuf, mut config: WorkerLaunchConfig) -> Result<()> {
        super::resolve_relative_harness_home(&mut config, &std::env::current_dir()?);
        std::fs::create_dir_all(&root)
            .with_context(|| format!("create worker root {}", root.display()))?;
        // Validate and recover durable state before publishing a socket. A
        // failed startup must never leave a fresh endpoint that looks live.
        let durable_worker =
            DurableWorker::open(&root, &config.session_id, env!("CARGO_PKG_VERSION"))?;
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
        let _socket_guard = SocketGuard(socket.clone());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
        }

        let resume_session = select_resume_session(&config, &durable_worker);
        let worker = Arc::new(Mutex::new(durable_worker));
        let (acp_commands_tx, acp_commands_rx) = mpsc::channel(32);
        let (acp_events_tx, acp_events_rx) = mpsc::unbounded_channel();
        let (checkpoint_tx, checkpoint_rx) = mpsc::unbounded_channel();
        let supervisor_path = root.join("acp-supervisor.json");
        AcpSupervisorSpec {
            command: config.bridge_command,
            args: config.bridge_args,
            environment: config.environment,
            cwd: config.cwd.clone(),
        }
        .write(&supervisor_path)?;
        let acp_spec = LaunchSpec {
            command: std::env::current_exe().context("locate Hel worker executable")?,
            args: vec![
                "worker".into(),
                "acp-supervisor".into(),
                "--spec".into(),
                supervisor_path.to_string_lossy().into_owned(),
            ],
            environment: Default::default(),
            cwd: config.cwd,
            additional_directories: config.additional_directories,
            resume_session,
            harness: config.harness,
        };
        let mut acp_task = tokio::spawn(hel_acp::run(acp_spec, acp_commands_rx, acp_events_tx));

        dispatch_pending(&worker, &acp_commands_tx).await?;

        let event_worker = worker.clone();
        let mut event_task = tokio::spawn(run_event_coordinator(
            event_worker,
            acp_events_rx,
            checkpoint_rx,
        ));

        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (stream, _) = accepted.context("accept worker proxy")?;
                    let client_worker = worker.clone();
                    let client_commands = acp_commands_tx.clone();
                    let client_checkpoints = checkpoint_tx.clone();
                    tokio::spawn(async move {
                        if let Err(error) = serve_client(
                            stream,
                            client_worker,
                            client_commands,
                            client_checkpoints,
                        ).await {
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
        Ok(())
    }

    pub(super) async fn run_event_coordinator(
        worker: Arc<Mutex<DurableWorker>>,
        mut events: mpsc::UnboundedReceiver<RuntimeEvent>,
        mut checkpoints: mpsc::UnboundedReceiver<CheckpointBarrier>,
    ) -> Result<()> {
        const DRAIN_BATCH: usize = 256;

        let mut activity = ToolActivity::default();
        let mut pending = VecDeque::new();
        let mut checkpoints_open = true;
        loop {
            tokio::select! {
                event = events.recv() => {
                    let Some(event) = event else { return Ok(()) };
                    record_runtime_event(&worker, &mut activity, &event)?;
                }
                checkpoint = checkpoints.recv(), if checkpoints_open => {
                    match checkpoint {
                        Some(checkpoint) => pending.push_back(checkpoint),
                        None => checkpoints_open = false,
                    }
                }
            }

            let mut drained = 0;
            loop {
                match events.try_recv() {
                    Ok(event) => {
                        record_runtime_event(&worker, &mut activity, &event)?;
                        drained += 1;
                        if drained == DRAIN_BATCH {
                            drained = 0;
                            tokio::task::yield_now().await;
                        }
                    }
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => return Ok(()),
                }
            }

            if activity.is_quiescent() {
                while let Some(checkpoint) = pending.pop_front() {
                    if checkpoint.response.is_closed() {
                        continue;
                    }
                    let response = worker
                        .lock()
                        .expect("worker state lock poisoned")
                        .handle(checkpoint.envelope);
                    let _ = checkpoint.response.send(response);
                }
            }
        }
    }

    fn record_runtime_event(
        worker: &Arc<Mutex<DurableWorker>>,
        activity: &mut ToolActivity,
        event: &RuntimeEvent,
    ) -> Result<()> {
        activity.observe(event);
        let value = serde_json::to_value(event)?;
        let mut worker = worker.lock().expect("worker state lock poisoned");
        if let RuntimeEvent::SessionStarted {
            native_session_id, ..
        } = event
        {
            worker.record_native_session_started(
                runtime_event_kind(event),
                value,
                native_session_id,
            )?;
        } else {
            worker.record_adapter_event(runtime_event_kind(event), value)?;
        }
        match event {
            RuntimeEvent::PromptFinished { request_id, .. } => {
                worker.record_turn_completed()?;
                worker.complete_dispatch(request_id)?;
            }
            RuntimeEvent::ConfigApplied { request_id, .. } => {
                worker.complete_dispatch(request_id)?;
            }
            RuntimeEvent::Stopped
                if worker.snapshot().phase == crate::hel_worker::WorkerPhase::Closing =>
            {
                worker.record_closed()?;
            }
            _ => {}
        }
        Ok(())
    }

    pub(super) fn select_resume_session(
        config: &WorkerLaunchConfig,
        worker: &crate::hel_worker::DurableWorker,
    ) -> Option<String> {
        if let Some(native_session_id) = &config.native_session_id {
            return Some(native_session_id.clone());
        }
        if let Some(native_session_id) = worker.native_session_id() {
            return Some(native_session_id.to_owned());
        }
        if !config.recover_native_session {
            return None;
        }
        worker.recover_native_session_id_from_events()
    }

    async fn serve_client(
        stream: UnixStream,
        worker: Arc<Mutex<DurableWorker>>,
        commands: mpsc::Sender<CommandRequest>,
        checkpoints: mpsc::UnboundedSender<CheckpointBarrier>,
    ) -> Result<()> {
        let (reader, mut writer) = stream.into_split();
        let mut lines = BufReader::new(reader).lines();
        while let Some(line) = lines.next_line().await? {
            if line.len() > crate::hel_worker::MAX_FRAME_BYTES {
                bail!("worker request frame is too large");
            }
            let envelope = match crate::hel_worker_protocol::decode_request(line.as_bytes())? {
                DecodedRequest::Known(envelope) => envelope,
                DecodedRequest::Unknown {
                    request_id,
                    protocol_version,
                    method,
                } => {
                    let response = crate::hel_worker::unsupported_method_response(
                        request_id,
                        protocol_version,
                        method,
                    );
                    write_response(&mut writer, &response).await?;
                    continue;
                }
                DecodedRequest::Invalid {
                    request_id,
                    protocol_version,
                    message,
                } => {
                    let response = crate::hel_worker::invalid_request_response(
                        request_id,
                        protocol_version,
                        message,
                    );
                    write_response(&mut writer, &response).await?;
                    continue;
                }
            };
            let request = envelope.request.clone();
            if let WorkerRequest::Compact { text } = request {
                let response = compact_response(envelope, text, &commands).await;
                write_response(&mut writer, &response).await?;
                continue;
            }
            if matches!(
                request,
                WorkerRequest::Checkpoint { .. } | WorkerRequest::CheckpointWhenQuiescent { .. }
            ) {
                let request_id = envelope.request_id.clone();
                let protocol_version = envelope.protocol_version;
                let (response_tx, response_rx) = oneshot::channel();
                let response = if checkpoints
                    .send(CheckpointBarrier {
                        envelope,
                        response: response_tx,
                    })
                    .is_ok()
                {
                    response_rx.await.unwrap_or_else(|_| {
                        runtime_request_error(
                            request_id,
                            protocol_version,
                            "worker event coordinator stopped",
                        )
                    })
                } else {
                    runtime_request_error(
                        request_id,
                        protocol_version,
                        "worker event coordinator stopped",
                    )
                };
                write_response(&mut writer, &response).await?;
                continue;
            }
            let response = worker
                .lock()
                .expect("worker state lock poisoned")
                .handle(envelope);
            dispatch_pending(&worker, &commands).await?;
            write_response(&mut writer, &response).await?;
        }
        Ok(())
    }

    fn runtime_request_error(
        request_id: String,
        protocol_version: u32,
        message: &str,
    ) -> ResponseEnvelope {
        ResponseEnvelope {
            request_id,
            protocol_version,
            body: ResponseBody::Error {
                error: ProtocolError {
                    code: ErrorCode::Internal,
                    message: message.into(),
                    retryable: true,
                    detail: None,
                },
            },
        }
    }

    async fn write_response(
        writer: &mut tokio::net::unix::OwnedWriteHalf,
        response: &ResponseEnvelope,
    ) -> Result<()> {
        let mut encoded = serde_json::to_vec(response)?;
        encoded.push(b'\n');
        writer.write_all(&encoded).await?;
        writer.flush().await?;
        Ok(())
    }

    async fn compact_response(
        envelope: RequestEnvelope,
        text: String,
        commands: &mpsc::Sender<CommandRequest>,
    ) -> ResponseEnvelope {
        let body = if !(crate::hel_worker::MIN_PROTOCOL_VERSION..=PROTOCOL_VERSION)
            .contains(&envelope.protocol_version)
        {
            ResponseBody::Error {
                error: ProtocolError {
                    code: ErrorCode::IncompatibleProtocol,
                    message: format!(
                        "request uses protocol {}, worker supports {}-{}",
                        envelope.protocol_version,
                        crate::hel_worker::MIN_PROTOCOL_VERSION,
                        PROTOCOL_VERSION
                    ),
                    retryable: false,
                    detail: None,
                },
            }
        } else if text.trim().is_empty() {
            ResponseBody::Error {
                error: ProtocolError {
                    code: ErrorCode::InvalidRequest,
                    message: "compaction prompt is empty".into(),
                    retryable: false,
                    detail: None,
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
            protocol_version: envelope.protocol_version,
            body,
        }
    }

    fn runtime_compaction_error(message: &str) -> ResponseBody {
        ResponseBody::Error {
            error: ProtocolError {
                code: ErrorCode::Internal,
                message: message.into(),
                retryable: false,
                detail: None,
            },
        }
    }

    async fn dispatch_pending(
        worker: &Arc<Mutex<DurableWorker>>,
        commands: &mpsc::Sender<CommandRequest>,
    ) -> Result<()> {
        let pending = worker
            .lock()
            .expect("worker state lock poisoned")
            .claim_pending_dispatches()?;
        for (request_id, request) in pending {
            if let Some(command) = acp_command(request_id, request) {
                commands
                    .send(command)
                    .await
                    .context("ACP runtime stopped")?;
            }
        }
        Ok(())
    }

    fn acp_command(request_id: String, request: WorkerRequest) -> Option<CommandRequest> {
        match request {
            WorkerRequest::Prompt { text, .. } => Some(CommandRequest::Prompt { request_id, text }),
            WorkerRequest::SetConfig { key, value } => {
                value.as_str().map(|value| CommandRequest::SetConfig {
                    request_id,
                    key,
                    value: value.to_owned(),
                })
            }
            WorkerRequest::Cancel => Some(CommandRequest::Cancel { request_id }),
            WorkerRequest::Close => Some(CommandRequest::Close { request_id }),
            _ => None,
        }
    }

    fn runtime_event_kind(event: &RuntimeEvent) -> &'static str {
        match event {
            RuntimeEvent::Connected { .. } => "connected",
            RuntimeEvent::SessionStarted { .. } => "session_started",
            RuntimeEvent::SessionConfigured { .. } => "session_configured",
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

    /// Own the ACP bridge's process group.  The daemon communicates only with
    /// this supervisor; if the daemon is killed, stdin reaches EOF and the
    /// complete bridge process tree is terminated and reaped.
    pub async fn run_acp_supervisor(spec: AcpSupervisorSpec) -> Result<()> {
        use tokio::io::AsyncWriteExt;

        let mut command = tokio::process::Command::new(&spec.command);
        command
            .args(&spec.args)
            .envs(&spec.environment)
            .current_dir(&spec.cwd)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit());
        command.process_group(0);
        let mut child = command
            .spawn()
            .with_context(|| format!("launch supervised ACP bridge {}", spec.command.display()))?;
        let pid = child
            .id()
            .context("supervised ACP bridge has no process ID")? as i32;
        let mut child_stdin = child.stdin.take().context("ACP bridge stdin unavailable")?;
        let mut child_stdout = child
            .stdout
            .take()
            .context("ACP bridge stdout unavailable")?;
        let mut parent_stdin = tokio::io::stdin();
        let mut parent_stdout = tokio::io::stdout();

        {
            let input = tokio::io::copy(&mut parent_stdin, &mut child_stdin);
            let output = tokio::io::copy(&mut child_stdout, &mut parent_stdout);
            tokio::pin!(input, output);
            tokio::select! {
                result = &mut input => { result.context("forward ACP supervisor input")?; }
                result = &mut output => { result.context("forward ACP supervisor output")?; }
            }
        }
        let _ = child_stdin.shutdown().await;
        terminate_process_group(pid, libc::SIGTERM);
        match tokio::time::timeout(std::time::Duration::from_secs(2), child.wait()).await {
            Ok(status) => {
                let _ = status.context("wait for supervised ACP bridge")?;
            }
            Err(_) => {
                terminate_process_group(pid, libc::SIGKILL);
                child.wait().await.context("reap supervised ACP bridge")?;
            }
        }
        Ok(())
    }

    fn terminate_process_group(pid: i32, signal: i32) {
        // SAFETY: a negative, validated child PID targets only the process
        // group created for this supervisor's child.
        unsafe {
            libc::kill(-pid, signal);
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
pub use unix::{proxy, run_acp_supervisor, run_daemon};

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

#[cfg(not(unix))]
pub async fn run_acp_supervisor(_spec: AcpSupervisorSpec) -> anyhow::Result<()> {
    anyhow::bail!("ACP supervision requires Unix")
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use crate::hel_acp;
    use tokio::sync::{mpsc, oneshot};

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
            recover_native_session: true,
        }
    }

    fn tool_event(id: &str, status: &str) -> hel_acp::RuntimeEvent {
        hel_acp::RuntimeEvent::SessionUpdate {
            update: serde_json::json!({
                "sessionUpdate": "tool_call",
                "toolCallId": id,
                "title": id,
                "status": status,
            }),
        }
    }

    fn tool_update(id: &str, status: &str) -> hel_acp::RuntimeEvent {
        hel_acp::RuntimeEvent::SessionUpdate {
            update: serde_json::json!({
                "sessionUpdate": "tool_call_update",
                "toolCallId": id,
                "status": status,
            }),
        }
    }

    #[tokio::test]
    async fn checkpoint_waits_for_the_observed_tool_cohort_to_finish() {
        let temp = tempfile::tempdir().unwrap();
        let worker = Arc::new(Mutex::new(
            crate::hel_worker::DurableWorker::open(
                temp.path(),
                "018f9dd2-a3b4-7c8d-9000-123456789abc",
                "1.0.0",
            )
            .unwrap(),
        ));
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (checkpoint_tx, checkpoint_rx) = mpsc::unbounded_channel();
        let coordinator = tokio::spawn(unix::run_event_coordinator(
            worker.clone(),
            event_rx,
            checkpoint_rx,
        ));

        event_tx.send(tool_event("one", "in_progress")).unwrap();
        event_tx.send(tool_event("two", "pending")).unwrap();
        let (response_tx, mut response_rx) = oneshot::channel();
        checkpoint_tx
            .send(unix::CheckpointBarrier {
                envelope: crate::hel_worker::RequestEnvelope {
                    request_id: "checkpoint-request".into(),
                    protocol_version: crate::hel_worker::PROTOCOL_VERSION,
                    request: crate::hel_worker::WorkerRequest::CheckpointWhenQuiescent {
                        reason: Some("test".into()),
                    },
                },
                response: response_tx,
            })
            .unwrap();

        event_tx.send(tool_update("one", "completed")).unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(25), &mut response_rx)
                .await
                .is_err(),
            "one remaining tool call must keep the checkpoint parked"
        );

        event_tx.send(tool_update("two", "failed")).unwrap();
        let response = tokio::time::timeout(std::time::Duration::from_secs(1), response_rx)
            .await
            .unwrap()
            .unwrap();
        let crate::hel_worker::ResponseBody::Ok {
            payload: crate::hel_worker::ResponsePayload::Accepted { seq },
        } = &response.body
        else {
            panic!("checkpoint was not accepted: {response:?}");
        };
        assert_eq!(*seq, 5);
        assert_eq!(
            worker.lock().unwrap().snapshot().last_checkpoint_seq,
            Some(5)
        );

        drop(event_tx);
        drop(checkpoint_tx);
        coordinator.await.unwrap().unwrap();
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

    #[test]
    fn legacy_launch_config_enables_event_identity_recovery() {
        let mut value = serde_json::to_value(launch_config("/profile")).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .remove("recover_native_session");

        let config: WorkerLaunchConfig = serde_json::from_value(value).unwrap();
        assert!(config.recover_native_session);
    }

    #[test]
    fn persisted_native_identity_is_reused_for_every_harness() {
        let native = "019feb39-865b-7392-b358-96932c672a42";
        for harness in [HarnessKind::Codex, HarnessKind::Claude, HarnessKind::Kimi] {
            let temp = tempfile::tempdir().unwrap();
            let mut worker = crate::hel_worker::DurableWorker::open(
                temp.path(),
                "018f9dd2-a3b4-7c8d-9000-123456789abc",
                "1.0.0",
            )
            .unwrap();
            worker
                .record_native_session_started(
                    "session_started",
                    serde_json::json!({
                        "type": "session_started",
                        "native_session_id": native,
                        "resumed": false,
                    }),
                    native,
                )
                .unwrap();
            let mut config = launch_config("/var/lib/hel/profiles/session");
            config.harness = harness;

            assert_eq!(
                unix::select_resume_session(&config, &worker).as_deref(),
                Some(native),
                "{harness:?} must resume the worker-owned native identity"
            );
        }
    }

    #[test]
    fn intentional_fresh_session_ignores_restored_native_history() {
        let temp = tempfile::tempdir().unwrap();
        let native = "019feb39-865b-7392-b358-96932c672a42";
        let mut worker = crate::hel_worker::DurableWorker::open(
            temp.path(),
            "018f9dd2-a3b4-7c8d-9000-123456789abc",
            "1.0.0",
        )
        .unwrap();
        worker
            .record_adapter_event(
                "session_started",
                serde_json::json!({
                    "type": "session_started",
                    "native_session_id": native,
                    "resumed": false,
                }),
            )
            .unwrap();
        let mut config = launch_config("/var/lib/hel/profiles/session");
        config.recover_native_session = false;

        assert_eq!(unix::select_resume_session(&config, &worker), None);
    }

    #[test]
    fn fresh_session_policy_reuses_identity_after_destination_starts() {
        let temp = tempfile::tempdir().unwrap();
        let native = "019feb39-865b-7392-b358-96932c672a42";
        let mut worker = crate::hel_worker::DurableWorker::open(
            temp.path(),
            "018f9dd2-a3b4-7c8d-9000-123456789abc",
            "1.0.0",
        )
        .unwrap();
        worker
            .record_native_session_started(
                "session_started",
                serde_json::json!({
                    "type": "session_started",
                    "native_session_id": native,
                    "resumed": false,
                }),
                native,
            )
            .unwrap();
        let mut config = launch_config("/var/lib/hel/profiles/session");
        config.recover_native_session = false;

        assert_eq!(
            unix::select_resume_session(&config, &worker).as_deref(),
            Some(native)
        );
    }
}
