//! Persistent per-user controller daemon and its authenticated local protocol.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail, ensure};
use hel::hel_config::{HelConfig, data_dir};
use hel::hel_controller::{Controller, ControllerStoreGuard};
use hel::hel_state::SessionState;
use hel::hel_workspace::WorkspaceRecord;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

const PROTOCOL_VERSION: u32 = 1;
const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
const START_TIMEOUT: Duration = Duration::from_secs(8);
const RETRY_DELAY: Duration = Duration::from_millis(40);

fn metadata_path() -> PathBuf {
    data_dir().join("daemon.json")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DaemonMetadata {
    protocol_version: u32,
    pid: u32,
    address: SocketAddr,
    token: String,
    started_at: String,
    build_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkspaceListing {
    pub workspace: WorkspaceRecord,
    pub attached_pids: Vec<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SessionPreview {
    pub id: String,
    pub title: String,
    pub project: String,
    pub harness: String,
    pub state: String,
    pub active: bool,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkspaceSnapshot {
    pub workspace: WorkspaceRecord,
    pub sessions: Vec<SessionPreview>,
    pub drafts: Vec<DraftPreview>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DraftPreview {
    pub id: String,
    pub session_id: Option<String>,
    pub source: String,
    pub owner_pid: Option<u32>,
    pub saved_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "action", content = "arguments")]
enum DaemonAction {
    Ping,
    Status,
    ListWorkspaces,
    CreateWorkspace {
        name: String,
    },
    RenameWorkspace {
        workspace_id: String,
        name: String,
    },
    DeleteWorkspace {
        workspace_id: String,
    },
    Attach {
        workspace_id: String,
        client_id: String,
        pid: u32,
    },
    Detach {
        client_id: String,
    },
    Snapshot {
        workspace_id: String,
    },
    RecoverDraft {
        draft_id: String,
    },
    Stop,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestEnvelope {
    protocol_version: u32,
    request_id: u64,
    token: String,
    action: DaemonAction,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResponseEnvelope {
    protocol_version: u32,
    request_id: u64,
    result: std::result::Result<DaemonReply, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "reply", content = "value")]
enum DaemonReply {
    Pong,
    Status(DaemonStatus),
    Workspaces(Vec<WorkspaceListing>),
    Workspace(WorkspaceRecord),
    Snapshot(WorkspaceSnapshot),
    Done,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DaemonStatus {
    pub pid: u32,
    pub started_at: String,
    pub build_version: String,
    pub attached_clients: usize,
    pub phone_status: String,
}

#[derive(Debug, Clone)]
struct Attachment {
    workspace_id: String,
    pid: u32,
}

#[derive(Default)]
struct RuntimeState {
    attachments: Mutex<BTreeMap<String, Attachment>>,
    phone_status: Mutex<String>,
    ever_attached: AtomicBool,
}

impl RuntimeState {
    fn attachments(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, Attachment>> {
        self.attachments
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn prune_dead_clients(&self) {
        self.attachments()
            .retain(|_, attachment| process_is_alive(attachment.pid));
    }

    fn set_phone_status(&self, status: impl Into<String>) {
        *self
            .phone_status
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = status.into();
    }

    fn phone_status(&self) -> String {
        self.phone_status
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

fn process_is_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        // SAFETY: kill(pid, 0) sends no signal and is the standard existence
        // probe. EPERM still means the process exists.
        let result = unsafe { libc::kill(pid as libc::pid_t, 0) };
        result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        true
    }
}

fn random_hex<const N: usize>() -> Result<String> {
    let mut bytes = [0_u8; N];
    getrandom::fill(&mut bytes).map_err(|error| anyhow!("generate daemon secret: {error}"))?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn write_metadata(path: &Path, metadata: &DaemonMetadata) -> Result<()> {
    let parent = path
        .parent()
        .context("daemon metadata path has no parent")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("create daemon data directory {}", parent.display()))?;
    let temporary = parent.join(format!(".daemon.{}.tmp", std::process::id()));
    let body = serde_json::to_vec_pretty(metadata)?;
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&temporary)
        .with_context(|| format!("create {}", temporary.display()))?;
    file.write_all(&body)?;
    file.sync_all()?;
    fs::rename(&temporary, path)
        .with_context(|| format!("publish daemon metadata {}", path.display()))?;
    Ok(())
}

fn read_metadata() -> Result<DaemonMetadata> {
    let metadata = read_metadata_any()?;
    ensure!(
        metadata.protocol_version == PROTOCOL_VERSION,
        "daemon protocol {} is incompatible with client protocol {}",
        metadata.protocol_version,
        PROTOCOL_VERSION
    );
    Ok(metadata)
}

fn read_metadata_any() -> Result<DaemonMetadata> {
    let path = metadata_path();
    let body = fs::read(&path).with_context(|| format!("read {}", path.display()))?;
    let metadata: DaemonMetadata =
        serde_json::from_slice(&body).with_context(|| format!("parse {}", path.display()))?;
    Ok(metadata)
}

async fn write_frame<T: Serialize>(stream: &mut TcpStream, value: &T) -> Result<()> {
    let body = serde_json::to_vec(value)?;
    ensure!(body.len() <= MAX_FRAME_BYTES, "daemon frame is too large");
    stream.write_u32(body.len() as u32).await?;
    stream.write_all(&body).await?;
    stream.flush().await?;
    Ok(())
}

async fn read_frame<T: for<'de> Deserialize<'de>>(stream: &mut TcpStream) -> Result<T> {
    let length = stream.read_u32().await? as usize;
    ensure!(
        length <= MAX_FRAME_BYTES,
        "daemon frame exceeds {MAX_FRAME_BYTES} bytes"
    );
    let mut body = vec![0_u8; length];
    stream.read_exact(&mut body).await?;
    serde_json::from_slice(&body).context("decode daemon frame")
}

pub(crate) struct DaemonClient {
    metadata: DaemonMetadata,
    stream: TcpStream,
    next_request_id: u64,
}

impl DaemonClient {
    async fn connect(metadata: DaemonMetadata) -> Result<Self> {
        let stream =
            tokio::time::timeout(Duration::from_secs(1), TcpStream::connect(metadata.address))
                .await
                .context("time out connecting to Hel daemon")??;
        Ok(Self {
            metadata,
            stream,
            next_request_id: 1,
        })
    }

    async fn request(&mut self, action: DaemonAction) -> Result<DaemonReply> {
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        write_frame(
            &mut self.stream,
            &RequestEnvelope {
                protocol_version: PROTOCOL_VERSION,
                request_id,
                token: self.metadata.token.clone(),
                action,
            },
        )
        .await?;
        let response: ResponseEnvelope = read_frame(&mut self.stream).await?;
        ensure!(
            response.protocol_version == PROTOCOL_VERSION,
            "daemon changed protocol"
        );
        ensure!(
            response.request_id == request_id,
            "daemon crossed request IDs"
        );
        response.result.map_err(anyhow::Error::msg)
    }

    pub(crate) async fn status(&mut self) -> Result<DaemonStatus> {
        match self.request(DaemonAction::Status).await? {
            DaemonReply::Status(status) => Ok(status),
            reply => bail!("unexpected daemon status reply {reply:?}"),
        }
    }

    pub(crate) async fn list_workspaces(&mut self) -> Result<Vec<WorkspaceListing>> {
        match self.request(DaemonAction::ListWorkspaces).await? {
            DaemonReply::Workspaces(workspaces) => Ok(workspaces),
            reply => bail!("unexpected daemon workspace reply {reply:?}"),
        }
    }

    pub(crate) async fn create_workspace(&mut self, name: String) -> Result<WorkspaceRecord> {
        match self.request(DaemonAction::CreateWorkspace { name }).await? {
            DaemonReply::Workspace(workspace) => Ok(workspace),
            reply => bail!("unexpected create-workspace reply {reply:?}"),
        }
    }

    pub(crate) async fn rename_workspace(
        &mut self,
        workspace_id: String,
        name: String,
    ) -> Result<()> {
        match self
            .request(DaemonAction::RenameWorkspace { workspace_id, name })
            .await?
        {
            DaemonReply::Done => Ok(()),
            reply => bail!("unexpected rename-workspace reply {reply:?}"),
        }
    }

    pub(crate) async fn delete_workspace(&mut self, workspace_id: String) -> Result<()> {
        match self
            .request(DaemonAction::DeleteWorkspace { workspace_id })
            .await?
        {
            DaemonReply::Done => Ok(()),
            reply => bail!("unexpected delete-workspace reply {reply:?}"),
        }
    }

    pub(crate) async fn attach(
        &mut self,
        workspace_id: String,
        client_id: String,
        pid: u32,
    ) -> Result<()> {
        match self
            .request(DaemonAction::Attach {
                workspace_id,
                client_id,
                pid,
            })
            .await?
        {
            DaemonReply::Done => Ok(()),
            reply => bail!("unexpected attach reply {reply:?}"),
        }
    }

    pub(crate) async fn detach(&mut self, client_id: String) -> Result<()> {
        match self.request(DaemonAction::Detach { client_id }).await? {
            DaemonReply::Done => Ok(()),
            reply => bail!("unexpected detach reply {reply:?}"),
        }
    }

    pub(crate) async fn snapshot(&mut self, workspace_id: String) -> Result<WorkspaceSnapshot> {
        match self
            .request(DaemonAction::Snapshot { workspace_id })
            .await?
        {
            DaemonReply::Snapshot(snapshot) => Ok(snapshot),
            reply => bail!("unexpected snapshot reply {reply:?}"),
        }
    }

    pub(crate) async fn recover_draft(&mut self, draft_id: String) -> Result<()> {
        match self
            .request(DaemonAction::RecoverDraft { draft_id })
            .await?
        {
            DaemonReply::Done => Ok(()),
            reply => bail!("unexpected recover-draft reply {reply:?}"),
        }
    }

    pub(crate) async fn stop(&mut self) -> Result<()> {
        match self.request(DaemonAction::Stop).await? {
            DaemonReply::Done => Ok(()),
            reply => bail!("unexpected stop reply {reply:?}"),
        }
    }
}

pub(crate) async fn connect_existing() -> Result<DaemonClient> {
    DaemonClient::connect(read_metadata()?).await
}

pub(crate) async fn connect_or_start() -> Result<DaemonClient> {
    if let Ok(metadata) = read_metadata_any()
        && metadata.protocol_version != PROTOCOL_VERSION
    {
        stop_incompatible_daemon(&metadata).await?;
    }
    if let Ok(mut client) = connect_existing().await
        && matches!(
            client.request(DaemonAction::Ping).await,
            Ok(DaemonReply::Pong)
        )
    {
        return Ok(client);
    }

    let executable = std::env::current_exe().context("find current Hel executable")?;
    let mut command = std::process::Command::new(executable);
    command.arg("daemon-run");
    let _pid = hel::hel_subprocess::spawn_detached(&mut command, &data_dir().join("daemon.log"))?;

    let deadline = Instant::now() + START_TIMEOUT;
    let mut last_error = None;
    while Instant::now() < deadline {
        match connect_existing().await {
            Ok(mut client) => match client.request(DaemonAction::Ping).await {
                Ok(DaemonReply::Pong) => return Ok(client),
                Ok(reply) => last_error = Some(anyhow!("unexpected startup reply {reply:?}")),
                Err(error) => last_error = Some(error),
            },
            Err(error) => last_error = Some(error),
        }
        tokio::time::sleep(RETRY_DELAY).await;
    }
    Err(last_error.unwrap_or_else(|| anyhow!("Hel daemon did not become ready"))).with_context(
        || {
            format!(
                "start Hel daemon; details are in {}",
                data_dir().join("daemon.log").display()
            )
        },
    )
}

async fn stop_incompatible_daemon(metadata: &DaemonMetadata) -> Result<()> {
    #[cfg(unix)]
    {
        let mut system = sysinfo::System::new();
        system.refresh_processes(
            sysinfo::ProcessesToUpdate::Some(&[sysinfo::Pid::from_u32(metadata.pid)]),
            true,
        );
        let daemon_executable = system
            .process(sysinfo::Pid::from_u32(metadata.pid))
            .and_then(sysinfo::Process::exe)
            .and_then(|path| fs::canonicalize(path).ok());
        let current_executable = std::env::current_exe()
            .ok()
            .and_then(|path| fs::canonicalize(path).ok());
        ensure!(
            daemon_executable.is_some() && daemon_executable == current_executable,
            "refusing to signal stale daemon PID {} because it is not this Hel executable",
            metadata.pid
        );
        // SAFETY: the PID comes from owner-only daemon metadata and SIGTERM is
        // handled as graceful cancellation by every supported daemon.
        let result = unsafe { libc::kill(metadata.pid as libc::pid_t, libc::SIGTERM) };
        if result != 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::NotFound {
                return Err(error).context("stop incompatible Hel daemon");
            }
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline && process_is_alive(metadata.pid) {
            tokio::time::sleep(RETRY_DELAY).await;
        }
        ensure!(
            !process_is_alive(metadata.pid),
            "incompatible Hel daemon {} did not stop",
            metadata.pid
        );
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        bail!("stop the incompatible Hel daemon, then retry")
    }
}

pub(crate) fn maintain_attachment(
    workspace_id: String,
    client_id: String,
    pid: u32,
    cancellation: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(2));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => return,
                _ = interval.tick() => {
                    match connect_or_start().await {
                        Ok(mut daemon) => {
                            if let Err(error) = daemon
                                .attach(workspace_id.clone(), client_id.clone(), pid)
                                .await
                            {
                                tracing::warn!(%error, "could not refresh daemon workspace attachment");
                            }
                        }
                        Err(error) => {
                            tracing::warn!(%error, "could not reconnect dashboard to Hel daemon");
                        }
                    }
                }
            }
        }
    })
}

pub(crate) async fn run_daemon_process() -> Result<()> {
    let _guard = ControllerStoreGuard::acquire()?;
    HelConfig::migrate_legacy_localhost_target()?;
    let config = HelConfig::load()?;
    hel::hel_database::recover_interrupted_checkpointing_sessions(
        &chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    )?;
    hel::hel_controller::reconcile_managed_checkpoint_archives()?;

    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .await
        .context("bind Hel daemon loopback endpoint")?;
    let metadata = DaemonMetadata {
        protocol_version: PROTOCOL_VERSION,
        pid: std::process::id(),
        address: listener.local_addr()?,
        token: random_hex::<32>()?,
        started_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        build_version: env!("CARGO_PKG_VERSION").to_owned(),
    };
    write_metadata(&metadata_path(), &metadata)?;

    let state = Arc::new(RuntimeState::default());
    let cancellation = hel::termination::Coordinator::install().token();
    let exit_when_idle = std::env::var_os("HEL_DAEMON_EXIT_WHEN_IDLE").is_some();
    let mut idle_tick = tokio::time::interval(Duration::from_millis(100));
    idle_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    if config.phone.enabled {
        spawn_phone_server(config.phone, cancellation.clone(), state.clone());
    } else {
        state.set_phone_status("disabled");
    }
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => break,
            _ = idle_tick.tick(), if exit_when_idle && state.ever_attached.load(Ordering::Acquire) => {
                state.prune_dead_clients();
                if state.attachments().is_empty() {
                    break;
                }
            }
            accepted = listener.accept() => {
                let (stream, peer) = accepted.context("accept Hel daemon client")?;
                if !peer.ip().is_loopback() {
                    tracing::warn!(%peer, "rejected non-loopback daemon client");
                    continue;
                }
                let metadata = metadata.clone();
                let state = state.clone();
                let cancellation = cancellation.clone();
                tokio::spawn(async move {
                    if let Err(error) = serve_client(stream, metadata, state, cancellation).await {
                        tracing::debug!(error = format!("{error:#}"), "daemon client disconnected");
                    }
                });
            }
        }
    }
    if let Err(error) = fs::remove_file(metadata_path())
        && error.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(%error, "could not remove daemon metadata");
    }
    Ok(())
}

fn spawn_phone_server(
    config: hel::hel_config::PhoneConfig,
    cancellation: CancellationToken,
    state: Arc<RuntimeState>,
) {
    tokio::spawn(async move {
        loop {
            let reporter = {
                let state = state.clone();
                move |status| state.set_phone_status(status)
            };
            match crate::server::run_server((&config).into(), cancellation.clone(), reporter).await
            {
                Ok(()) if cancellation.is_cancelled() => return,
                Ok(()) => state.set_phone_status("stopped unexpectedly"),
                Err(error) => {
                    tracing::warn!(error = format!("{error:#}"), "phone server stopped");
                    state.set_phone_status(format!("error: {error:#}"));
                }
            }
            tokio::select! {
                _ = cancellation.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_secs(10)) => {}
            }
        }
    });
}

async fn serve_client(
    mut stream: TcpStream,
    metadata: DaemonMetadata,
    state: Arc<RuntimeState>,
    cancellation: CancellationToken,
) -> Result<()> {
    loop {
        let request: RequestEnvelope = match read_frame(&mut stream).await {
            Ok(request) => request,
            Err(error)
                if error.downcast_ref::<std::io::Error>().is_some_and(|io| {
                    matches!(
                        io.kind(),
                        std::io::ErrorKind::UnexpectedEof
                            | std::io::ErrorKind::ConnectionReset
                            | std::io::ErrorKind::BrokenPipe
                    )
                }) =>
            {
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let request_id = request.request_id;
        let result = if request.protocol_version != PROTOCOL_VERSION {
            Err(format!(
                "incompatible daemon protocol {}; expected {}",
                request.protocol_version, PROTOCOL_VERSION
            ))
        } else if request.token != metadata.token {
            Err("daemon authentication failed".to_owned())
        } else {
            handle_action(request.action, &metadata, &state, &cancellation)
                .await
                .map_err(|error| format!("{error:#}"))
        };
        write_frame(
            &mut stream,
            &ResponseEnvelope {
                protocol_version: PROTOCOL_VERSION,
                request_id,
                result,
            },
        )
        .await?;
    }
}

async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(work)
        .await
        .context("daemon background database task panicked")?
}

async fn handle_action(
    action: DaemonAction,
    metadata: &DaemonMetadata,
    state: &RuntimeState,
    cancellation: &CancellationToken,
) -> Result<DaemonReply> {
    match action {
        DaemonAction::Ping => Ok(DaemonReply::Pong),
        DaemonAction::Status => {
            state.prune_dead_clients();
            Ok(DaemonReply::Status(DaemonStatus {
                pid: metadata.pid,
                started_at: metadata.started_at.clone(),
                build_version: metadata.build_version.clone(),
                attached_clients: state.attachments().len(),
                phone_status: state.phone_status(),
            }))
        }
        DaemonAction::ListWorkspaces => {
            state.prune_dead_clients();
            let workspaces = blocking(hel::hel_database::list_workspaces).await?;
            let attachments = state.attachments();
            Ok(DaemonReply::Workspaces(
                workspaces
                    .into_iter()
                    .map(|workspace| {
                        let attached_pids = attachments
                            .values()
                            .filter(|attachment| attachment.workspace_id == workspace.id)
                            .map(|attachment| attachment.pid)
                            .collect::<BTreeSet<_>>()
                            .into_iter()
                            .collect();
                        WorkspaceListing {
                            workspace,
                            attached_pids,
                        }
                    })
                    .collect(),
            ))
        }
        DaemonAction::CreateWorkspace { name } => Ok(DaemonReply::Workspace(
            blocking(move || hel::hel_database::create_workspace(&name)).await?,
        )),
        DaemonAction::RenameWorkspace { workspace_id, name } => {
            blocking(move || hel::hel_database::rename_workspace(&workspace_id, &name)).await?;
            Ok(DaemonReply::Done)
        }
        DaemonAction::DeleteWorkspace { workspace_id } => {
            ensure!(
                !state
                    .attachments()
                    .values()
                    .any(|attachment| attachment.workspace_id == workspace_id),
                "workspace still has attached clients"
            );
            blocking(move || hel::hel_database::delete_empty_workspace(&workspace_id)).await?;
            Ok(DaemonReply::Done)
        }
        DaemonAction::Attach {
            workspace_id,
            client_id,
            pid,
        } => {
            let exists = blocking({
                let workspace_id = workspace_id.clone();
                move || {
                    Ok(hel::hel_database::list_workspaces()?
                        .iter()
                        .any(|workspace| workspace.id == workspace_id))
                }
            })
            .await?;
            ensure!(exists, "unknown workspace {workspace_id:?}");
            let changed = state
                .attachments()
                .insert(
                    client_id,
                    Attachment {
                        workspace_id: workspace_id.clone(),
                        pid,
                    },
                )
                .is_none_or(|previous| {
                    previous.workspace_id != workspace_id || previous.pid != pid
                });
            state.ever_attached.store(true, Ordering::Release);
            if changed {
                blocking(move || hel::hel_database::touch_workspace(&workspace_id)).await?;
            }
            Ok(DaemonReply::Done)
        }
        DaemonAction::Detach { client_id } => {
            state.attachments().remove(&client_id);
            Ok(DaemonReply::Done)
        }
        DaemonAction::Snapshot { workspace_id } => {
            let snapshot = blocking(move || workspace_snapshot(&workspace_id)).await?;
            Ok(DaemonReply::Snapshot(snapshot))
        }
        DaemonAction::RecoverDraft { draft_id } => {
            blocking(move || hel::hel_database::recover_detached_draft(&draft_id)).await?;
            Ok(DaemonReply::Done)
        }
        DaemonAction::Stop => {
            cancellation.cancel();
            Ok(DaemonReply::Done)
        }
    }
}

fn workspace_snapshot(workspace_id: &str) -> Result<WorkspaceSnapshot> {
    let workspace = hel::hel_database::list_workspaces()?
        .into_iter()
        .find(|workspace| workspace.id == workspace_id)
        .with_context(|| format!("unknown workspace {workspace_id:?}"))?;
    let ids = hel::hel_database::session_ids_for_workspace(workspace_id)?
        .into_iter()
        .collect::<BTreeSet<_>>();
    let controller = Controller::load()?;
    let sessions = controller
        .state
        .sessions
        .values()
        .filter(|session| ids.contains(&session.id))
        .map(|session| SessionPreview {
            id: session.id.clone(),
            title: session.display_title().to_owned(),
            project: session.project_name(&controller.config),
            harness: session.harness_kind.display_name().to_owned(),
            state: session_state_label(session.state).to_owned(),
            active: session.state.is_active(),
            updated_at: session.updated_at.clone(),
        })
        .collect();
    let drafts = hel::hel_database::list_detached_drafts(workspace_id)?
        .into_iter()
        .map(|draft| DraftPreview {
            id: draft.id,
            session_id: draft.session_id,
            source: draft.source,
            owner_pid: draft.owner_pid,
            saved_at: draft.saved_at,
        })
        .collect();
    Ok(WorkspaceSnapshot {
        workspace,
        sessions,
        drafts,
    })
}

fn session_state_label(state: SessionState) -> &'static str {
    match state {
        SessionState::Provisioning => "provisioning",
        SessionState::Running => "running",
        SessionState::Disconnected => "disconnected",
        SessionState::Checkpointing => "checkpointing",
        SessionState::Closing => "closing",
        SessionState::Destroying => "destroying",
        SessionState::Stopped => "stopped",
        SessionState::Lost => "lost",
        SessionState::Error => "error",
        SessionState::DestroyedWithDataLoss => "destroyed-with-data-loss",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn framing_round_trips_payloads_larger_than_a_pipe_buffer() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let sender = tokio::spawn(async move {
            let mut stream = TcpStream::connect(address).await.unwrap();
            write_frame(&mut stream, &"x".repeat(512 * 1024))
                .await
                .unwrap();
        });
        let (mut stream, _) = listener.accept().await.unwrap();
        let received: String = read_frame(&mut stream).await.unwrap();
        sender.await.unwrap();
        assert_eq!(received.len(), 512 * 1024);
    }

    #[tokio::test]
    async fn framing_rejects_an_oversized_frame_before_allocating_it() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let sender = tokio::spawn(async move {
            let mut stream = TcpStream::connect(address).await.unwrap();
            stream
                .write_u32((MAX_FRAME_BYTES + 1) as u32)
                .await
                .unwrap();
        });
        let (mut stream, _) = listener.accept().await.unwrap();
        assert!(read_frame::<String>(&mut stream).await.is_err());
        sender.await.unwrap();
    }

    #[tokio::test]
    async fn daemon_rejects_a_request_with_the_wrong_owner_token() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let metadata = DaemonMetadata {
            protocol_version: PROTOCOL_VERSION,
            pid: 1,
            address,
            token: "right-token".into(),
            started_at: "now".into(),
            build_version: "test".into(),
        };
        let server_metadata = metadata.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            serve_client(
                stream,
                server_metadata,
                Arc::new(RuntimeState::default()),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        });
        let mut stream = TcpStream::connect(address).await.unwrap();
        write_frame(
            &mut stream,
            &RequestEnvelope {
                protocol_version: PROTOCOL_VERSION,
                request_id: 42,
                token: "wrong-token".into(),
                action: DaemonAction::Ping,
            },
        )
        .await
        .unwrap();
        let response: ResponseEnvelope = read_frame(&mut stream).await.unwrap();
        assert_eq!(response.request_id, 42);
        assert_eq!(response.result.unwrap_err(), "daemon authentication failed");
        drop(stream);
        server.await.unwrap();
    }
}
