//! Target-side daemon and stdio proxy for the durable worker protocol.

#[cfg(unix)]
mod unix {
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    use anyhow::{Context, Result, bail};
    use serde::{Deserialize, Serialize};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{UnixListener, UnixStream};
    use tokio::sync::mpsc;

    use crate::hel_acp::{self, CommandRequest, LaunchSpec, RuntimeEvent};
    use crate::hel_config::HarnessKind;
    use crate::hel_worker::{
        DurableWorker, RequestEnvelope, ResponseBody, ResponseEnvelope, ResponsePayload,
        WorkerRequest,
    };

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

    pub async fn run_daemon(root: PathBuf, config: WorkerLaunchConfig) -> Result<()> {
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
            RuntimeEvent::Stopped => "stopped",
        }
    }

    pub async fn proxy(root: PathBuf) -> Result<()> {
        let stream = UnixStream::connect(root.join("control.sock"))
            .await
            .with_context(|| format!("connect worker at {}", root.display()))?;
        let (mut socket_read, mut socket_write) = stream.into_split();
        let mut stdin = tokio::io::stdin();
        let mut stdout = tokio::io::stdout();
        let upload = tokio::io::copy(&mut stdin, &mut socket_write);
        let download = tokio::io::copy(&mut socket_read, &mut stdout);
        let (_, _) = tokio::try_join!(upload, download)?;
        Ok(())
    }
}

#[cfg(unix)]
pub use unix::{WorkerLaunchConfig, proxy, run_daemon};

#[cfg(not(unix))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkerLaunchConfig;

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
