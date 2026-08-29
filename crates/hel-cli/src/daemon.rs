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
use hel::hel_controller::{
    Controller, ControllerStoreGuard, ResumeRepositorySourceReceipt, SessionResumeOptions,
};
use hel::hel_credentials::CredentialSyncSignal;
use hel::hel_elicitation::ElicitationResponse;
use hel::hel_session_manager::{
    ManagedSessionView, RemoteSessionPublisher, RemoteSessionRequest, SessionManagerChannels,
    SessionManagerControl, ViewError, spawn_remote_session_manager, spawn_session_manager,
};
use hel::hel_state::{
    MaterializedSession, RecoveryObservation, RecoveryObserver, SessionResourceAllocation,
    SessionState,
};
use hel::hel_targets::{AdditionalMount, CancellableProcessExecutor};
use hel::hel_worker::{RelayCommand, RelayOperationalState};
use hel::hel_workspace::WorkspaceRecord;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use crate::pollers::{
    dashboard_worker_targets, interrupted_close_session_ids, reserve_recovery_or_cancel,
    spawn_interrupted_close_recovery,
};

const PROTOCOL_VERSION: u32 = 2;
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
pub(crate) struct RuntimeSessionView {
    pub session_id: String,
    pub projection_ordinal: u64,
    pub projection_digest: String,
    pub operational: Option<RelayOperationalState>,
    pub latest_credential_sync_signal: Option<CredentialSyncSignal>,
    pub connected: bool,
    pub error: Option<ViewError>,
}

impl RuntimeSessionView {
    fn from_managed(session_id: String, view: ManagedSessionView) -> Self {
        let (projection_ordinal, projection_digest, operational, signal) =
            view.snapshot
                .map_or((0, String::new(), None, None), |snapshot| {
                    (
                        snapshot.materialized.applied_event_ordinal,
                        snapshot.materialized.applied_event_digest,
                        Some(snapshot.operational),
                        snapshot.latest_credential_sync_signal,
                    )
                });
        Self {
            session_id,
            projection_ordinal,
            projection_digest,
            operational,
            latest_credential_sync_signal: signal,
            connected: view.connected,
            error: view.error,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RuntimeSnapshot {
    pub revision: u64,
    pub sessions: Vec<RuntimeSessionView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResumeSessionRequest {
    pub session_id: String,
    pub profile_id: String,
    pub target_template_id: String,
    pub additional_mounts: Option<Vec<AdditionalMount>>,
    pub resource_allocation: Option<SessionResourceAllocation>,
    pub discard_queue: bool,
    pub repository_preflight: Option<ResumeRepositorySourceReceipt>,
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
    RuntimeSnapshot {
        workspace_id: String,
        after_revision: u64,
    },
    SubmitSessionCommand {
        session_id: String,
        command_id: String,
        command: RelayCommand,
    },
    SyncSession {
        session_id: String,
    },
    RespondElicitation {
        session_id: String,
        elicitation_id: String,
        response: ElicitationResponse,
    },
    CloseSession {
        session_id: String,
    },
    ResumeSession(ResumeSessionRequest),
    ForceStopSession {
        session_id: String,
    },
    DestroyStoppedSession {
        session_id: String,
    },
    CancelLifecycle {
        session_id: String,
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
    RuntimeSnapshot(RuntimeSnapshot),
    MaterializedSession(MaterializedSession),
    Ordinal(u64),
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

pub(crate) struct RuntimeState {
    attachments: Mutex<BTreeMap<String, Attachment>>,
    phone_status: Mutex<String>,
    ever_attached: AtomicBool,
    sessions: Mutex<BTreeMap<String, RuntimeSessionView>>,
    revision: std::sync::atomic::AtomicU64,
    revision_tx: tokio::sync::watch::Sender<u64>,
    session_manager: SessionManagerControl,
    lifecycle: Mutex<BTreeMap<String, ActiveLifecycle>>,
    controller: Mutex<Controller>,
    recovery_observer: RecoveryObserver,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LifecycleKind {
    Close,
    Resume,
    ForceStop,
    DestroyStopped,
}

struct ActiveLifecycle {
    kind: LifecycleKind,
    cancelled: Arc<AtomicBool>,
    result:
        tokio::sync::watch::Receiver<Option<std::result::Result<DaemonLifecycleResult, String>>>,
}

#[derive(Debug, Clone)]
enum DaemonLifecycleResult {
    Done,
    Materialized(Box<MaterializedSession>),
}

impl RuntimeState {
    fn new(
        session_manager: SessionManagerControl,
        controller: Controller,
        recovery_observer: RecoveryObserver,
    ) -> Self {
        let (revision_tx, _) = tokio::sync::watch::channel(0);
        Self {
            attachments: Mutex::new(BTreeMap::new()),
            phone_status: Mutex::new(String::new()),
            ever_attached: AtomicBool::new(false),
            sessions: Mutex::new(BTreeMap::new()),
            revision: std::sync::atomic::AtomicU64::new(0),
            revision_tx,
            session_manager,
            lifecycle: Mutex::new(BTreeMap::new()),
            controller: Mutex::new(controller),
            recovery_observer,
        }
    }

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

    fn publish_session(&self, session_id: String, view: ManagedSessionView) {
        if let Some(snapshot) = view.snapshot.as_ref() {
            let controller = self
                .controller
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(session) = controller.state.sessions.get(&session_id).cloned() {
                self.recovery_observer.observe(RecoveryObservation {
                    session,
                    config: controller.config.clone(),
                    latest_completed_turn_ordinal: hel::hel_state::latest_completed_turn_ordinal(
                        &snapshot.materialized,
                    ),
                    execution: snapshot.materialized.execution,
                });
            }
        }
        self.sessions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(
                session_id.clone(),
                RuntimeSessionView::from_managed(session_id, view),
            );
        let revision = self.revision.fetch_add(1, Ordering::AcqRel) + 1;
        self.revision_tx.send_replace(revision);
    }

    async fn runtime_snapshot(
        &self,
        workspace_id: &str,
        after_revision: u64,
    ) -> Result<RuntimeSnapshot> {
        let mut revisions = self.revision_tx.subscribe();
        if *revisions.borrow_and_update() <= after_revision {
            let _ = tokio::time::timeout(Duration::from_secs(30), revisions.changed()).await;
        }
        let revision = self.revision.load(Ordering::Acquire);
        let session_ids = blocking({
            let workspace_id = workspace_id.to_owned();
            move || hel::hel_database::session_ids_for_workspace(&workspace_id)
        })
        .await?
        .into_iter()
        .collect::<BTreeSet<_>>();
        let sessions = self
            .sessions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|(session_id, _)| session_ids.contains(*session_id))
            .map(|(_, view)| view.clone())
            .collect();
        Ok(RuntimeSnapshot { revision, sessions })
    }

    async fn run_lifecycle<F, Fut>(
        self: &Arc<Self>,
        session_id: String,
        kind: LifecycleKind,
        work: F,
    ) -> Result<DaemonLifecycleResult>
    where
        F: FnOnce(Arc<Self>, String, Arc<AtomicBool>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<DaemonLifecycleResult>> + Send + 'static,
    {
        let mut work = Some(work);
        let mut result = {
            let mut lifecycle = self
                .lifecycle
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(active) = lifecycle.get(&session_id) {
                ensure!(
                    active.kind == kind,
                    "another lifecycle operation is already running for session {session_id}"
                );
                active.result.clone()
            } else {
                let cancelled = Arc::new(AtomicBool::new(false));
                let (result_tx, result_rx) = tokio::sync::watch::channel(None);
                lifecycle.insert(
                    session_id.clone(),
                    ActiveLifecycle {
                        kind,
                        cancelled: cancelled.clone(),
                        result: result_rx.clone(),
                    },
                );
                let state = Arc::clone(self);
                let operation_session_id = session_id.clone();
                let operation = work.take().expect("new lifecycle operation has work");
                tokio::spawn(async move {
                    let result = operation(state.clone(), operation_session_id.clone(), cancelled)
                        .await
                        .map_err(|error| format!("{error:#}"));
                    result_tx.send_replace(Some(result));
                    state
                        .lifecycle
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .remove(&operation_session_id);
                    refresh_runtime_controller(&state).await;
                });
                result_rx
            }
        };

        loop {
            if let Some(result) = result.borrow_and_update().clone() {
                return result.map_err(anyhow::Error::msg);
            }
            result
                .changed()
                .await
                .context("daemon lifecycle operation stopped without a result")?;
        }
    }

    pub(crate) async fn close_session(self: &Arc<Self>, session_id: String) -> Result<()> {
        let already_stopped = blocking({
            let session_id = session_id.clone();
            move || {
                let controller = Controller::load()?;
                Ok(controller
                    .state
                    .sessions
                    .get(&session_id)
                    .is_some_and(|session| session.state == SessionState::Stopped))
            }
        })
        .await?;
        if already_stopped {
            return Ok(());
        }
        self.run_lifecycle(
            session_id,
            LifecycleKind::Close,
            |state, session_id, cancelled| async move {
                let _recovery_reservation = tokio::task::spawn_blocking({
                    let observer = state.recovery_observer.clone();
                    let session_id = session_id.clone();
                    let cancelled = cancelled.clone();
                    move || reserve_recovery_or_cancel(&observer, &session_id, &cancelled)
                })
                .await
                .context("reserve recovery for daemon close task")??;
                let mut controller = tokio::task::spawn_blocking(Controller::load)
                    .await
                    .context("load controller for daemon close task")??;
                let executor = CancellableProcessExecutor::new(cancelled);
                controller
                    .close_session_managed_controlled(
                        &session_id,
                        &executor,
                        &state.session_manager,
                    )
                    .await?;
                Ok(DaemonLifecycleResult::Done)
            },
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn resume_session(
        self: &Arc<Self>,
        request: ResumeSessionRequest,
    ) -> Result<MaterializedSession> {
        let session_id = request.session_id.clone();
        let already_running = blocking({
            let session_id = session_id.clone();
            move || {
                let controller = Controller::load()?;
                if controller
                    .state
                    .sessions
                    .get(&session_id)
                    .is_some_and(|session| session.state == SessionState::Running)
                {
                    Ok(Some(
                        hel::hel_database::load_materialized_session(&session_id)?.with_context(
                            || {
                                format!(
                                    "running session {session_id} has no materialized projection"
                                )
                            },
                        )?,
                    ))
                } else {
                    Ok(None)
                }
            }
        })
        .await?;
        if let Some(materialized) = already_running {
            return Ok(materialized);
        }
        let result = self
            .run_lifecycle(
                session_id,
                LifecycleKind::Resume,
                move |state, session_id, cancelled| async move {
                    let _recovery_reservation = tokio::task::spawn_blocking({
                        let observer = state.recovery_observer.clone();
                        let session_id = session_id.clone();
                        let cancelled = cancelled.clone();
                        move || reserve_recovery_or_cancel(&observer, &session_id, &cancelled)
                    })
                    .await
                    .context("reserve recovery for daemon resume task")??;
                    let mut controller = tokio::task::spawn_blocking(Controller::load)
                        .await
                        .context("load controller for daemon resume task")??;
                    let executor = CancellableProcessExecutor::new(cancelled);
                    let materialized = controller
                        .resume_session_controlled_with_repository_preflight(
                            &session_id,
                            &request.profile_id,
                            &request.target_template_id,
                            SessionResumeOptions {
                                additional_mounts: request.additional_mounts,
                                resource_allocation: request.resource_allocation,
                                discard_queue: request.discard_queue,
                            },
                            request.repository_preflight,
                            &executor,
                        )
                        .await?;
                    Ok(DaemonLifecycleResult::Materialized(Box::new(materialized)))
                },
            )
            .await?;
        match result {
            DaemonLifecycleResult::Materialized(materialized) => Ok(*materialized),
            DaemonLifecycleResult::Done => bail!("daemon resume completed without a projection"),
        }
    }

    async fn force_stop_session(self: &Arc<Self>, session_id: String) -> Result<()> {
        self.run_lifecycle(
            session_id,
            LifecycleKind::ForceStop,
            |_state, session_id, cancelled| async move {
                blocking(move || {
                    let mut controller = Controller::load()?;
                    let executor = CancellableProcessExecutor::new(cancelled);
                    controller.force_stop(&session_id, &executor)?;
                    Ok(DaemonLifecycleResult::Done)
                })
                .await
            },
        )
        .await?;
        Ok(())
    }

    async fn destroy_stopped_session(self: &Arc<Self>, session_id: String) -> Result<()> {
        let exists = blocking({
            let session_id = session_id.clone();
            move || Ok(Controller::load()?.state.sessions.contains_key(&session_id))
        })
        .await?;
        if !exists {
            return Ok(());
        }
        self.run_lifecycle(
            session_id,
            LifecycleKind::DestroyStopped,
            |_state, session_id, cancelled| async move {
                blocking(move || {
                    let mut controller = Controller::load()?;
                    let executor = CancellableProcessExecutor::new(cancelled);
                    controller.destroy_session_controlled(&session_id, &executor)?;
                    Ok(DaemonLifecycleResult::Done)
                })
                .await
            },
        )
        .await?;
        Ok(())
    }

    fn cancel_lifecycle(&self, session_id: &str) -> Result<()> {
        let lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let active = lifecycle.get(session_id).with_context(|| {
            format!("no lifecycle operation is running for session {session_id}")
        })?;
        active.cancelled.store(true, Ordering::Release);
        Ok(())
    }

    pub(crate) fn cancel_lifecycle_if_active(&self, session_id: &str) {
        if let Some(active) = self
            .lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(session_id)
        {
            active.cancelled.store(true, Ordering::Release);
        }
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

    pub(crate) async fn runtime_snapshot(
        &mut self,
        workspace_id: String,
        after_revision: u64,
    ) -> Result<RuntimeSnapshot> {
        match self
            .request(DaemonAction::RuntimeSnapshot {
                workspace_id,
                after_revision,
            })
            .await?
        {
            DaemonReply::RuntimeSnapshot(snapshot) => Ok(snapshot),
            reply => bail!("unexpected runtime snapshot reply {reply:?}"),
        }
    }

    pub(crate) async fn submit_session_command(
        &mut self,
        session_id: String,
        command_id: String,
        command: RelayCommand,
    ) -> Result<u64> {
        match self
            .request(DaemonAction::SubmitSessionCommand {
                session_id,
                command_id,
                command,
            })
            .await?
        {
            DaemonReply::Ordinal(ordinal) => Ok(ordinal),
            reply => bail!("unexpected session command reply {reply:?}"),
        }
    }

    pub(crate) async fn sync_session(&mut self, session_id: String) -> Result<()> {
        match self
            .request(DaemonAction::SyncSession { session_id })
            .await?
        {
            DaemonReply::Done => Ok(()),
            reply => bail!("unexpected session sync reply {reply:?}"),
        }
    }

    pub(crate) async fn respond_elicitation(
        &mut self,
        session_id: String,
        elicitation_id: String,
        response: ElicitationResponse,
    ) -> Result<()> {
        match self
            .request(DaemonAction::RespondElicitation {
                session_id,
                elicitation_id,
                response,
            })
            .await?
        {
            DaemonReply::Done => Ok(()),
            reply => bail!("unexpected elicitation reply {reply:?}"),
        }
    }

    pub(crate) async fn close_session(&mut self, session_id: String) -> Result<()> {
        match self
            .request(DaemonAction::CloseSession { session_id })
            .await?
        {
            DaemonReply::Done => Ok(()),
            reply => bail!("unexpected close-session reply {reply:?}"),
        }
    }

    pub(crate) async fn resume_session(
        &mut self,
        request: ResumeSessionRequest,
    ) -> Result<MaterializedSession> {
        match self.request(DaemonAction::ResumeSession(request)).await? {
            DaemonReply::MaterializedSession(materialized) => Ok(materialized),
            reply => bail!("unexpected resume-session reply {reply:?}"),
        }
    }

    pub(crate) async fn force_stop_session(&mut self, session_id: String) -> Result<()> {
        match self
            .request(DaemonAction::ForceStopSession { session_id })
            .await?
        {
            DaemonReply::Done => Ok(()),
            reply => bail!("unexpected force-stop reply {reply:?}"),
        }
    }

    pub(crate) async fn destroy_stopped_session(&mut self, session_id: String) -> Result<()> {
        match self
            .request(DaemonAction::DestroyStoppedSession { session_id })
            .await?
        {
            DaemonReply::Done => Ok(()),
            reply => bail!("unexpected destroy-stopped reply {reply:?}"),
        }
    }

    pub(crate) async fn cancel_lifecycle(&mut self, session_id: String) -> Result<()> {
        match self
            .request(DaemonAction::CancelLifecycle { session_id })
            .await?
        {
            DaemonReply::Done => Ok(()),
            reply => bail!("unexpected cancel-lifecycle reply {reply:?}"),
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

    let controller = Controller::load()?;
    let manager = spawn_session_manager()?;
    let manager_targets = manager.targets;
    manager_targets.send_replace(dashboard_worker_targets(&controller));
    let mut manager_updates = manager.updates;
    let manager_control = manager.control.clone();
    let manager_shutdown = manager.shutdown;
    let mut recovery = hel::hel_recovery::RecoveryCoordinator::spawn(manager_control.clone());
    let recovery_observer = recovery.observer();

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

    let state = Arc::new(RuntimeState::new(
        manager_control.clone(),
        Controller {
            config: controller.config.clone(),
            state: controller.state.clone(),
        },
        recovery_observer.clone(),
    ));
    let cancellation = hel::termination::Coordinator::install().token();
    let target_refresh = spawn_manager_target_refresher(
        manager_targets.clone(),
        cancellation.clone(),
        state.clone(),
    );
    let exit_when_idle = std::env::var_os("HEL_DAEMON_EXIT_WHEN_IDLE").is_some();
    let mut idle_tick = tokio::time::interval(Duration::from_millis(100));
    idle_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut recovery_tick = tokio::time::interval(Duration::from_millis(250));
    recovery_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let (interrupted_close_tx, mut interrupted_close_rx) = tokio::sync::mpsc::unbounded_channel();
    for session_id in interrupted_close_session_ids(&controller) {
        spawn_interrupted_close_recovery(
            session_id,
            manager_control.clone(),
            recovery_observer.clone(),
            Arc::new(AtomicBool::new(false)),
            interrupted_close_tx.clone(),
            None,
        );
    }
    let mut phone_publisher: Option<RemoteSessionPublisher> = None;
    if config.phone.enabled {
        let remote = spawn_remote_session_manager()?;
        remote
            .targets
            .send_replace(dashboard_worker_targets(&controller));
        phone_publisher = Some(remote.publisher.clone());
        spawn_remote_request_bridge(remote.requests, manager_control.clone());
        spawn_phone_server(
            config.phone,
            cancellation.clone(),
            state.clone(),
            SessionManagerChannels {
                targets: remote.targets,
                control: remote.control,
                updates: remote.updates,
                shutdown: remote.shutdown,
            },
        );
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
            _ = recovery_tick.tick() => {
                while let Some(result) = recovery.try_result() {
                    if let Err(error) = &result.outcome {
                        tracing::warn!(session_id = %result.session_id, %error, "daemon recovery checkpoint failed");
                    }
                    refresh_runtime_controller(&state).await;
                }
            }
            completed = interrupted_close_rx.recv() => {
                if let Some(completed) = completed {
                    if let Err(error) = completed.result {
                        tracing::warn!(session_id = %completed.session_id, %error, "daemon could not resume interrupted close");
                    }
                    refresh_runtime_controller(&state).await;
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
            update = manager_updates.recv() => {
                let Some(update) = update else {
                    bail!("controller daemon session manager stopped");
                };
                if let Some(publisher) = phone_publisher.as_ref()
                    && let Err(error) = publisher.try_publish(
                        update.session_id.clone(),
                        update.view.clone(),
                    )
                {
                    tracing::warn!(%error, "phone session view bridge stopped");
                    phone_publisher = None;
                }
                state.publish_session(update.session_id, update.view);
            }
        }
    }
    // The idle-exit path does not arrive through the termination coordinator,
    // so explicitly stop every daemon-owned background task before awaiting it.
    cancellation.cancel();
    manager_shutdown
        .shutdown()
        .await
        .context("shut down controller daemon session manager")?;
    if let Err(error) = target_refresh.await {
        tracing::warn!(%error, "controller target refresher failed while shutting down");
    }
    if let Err(error) = fs::remove_file(metadata_path())
        && error.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(%error, "could not remove daemon metadata");
    }
    Ok(())
}

fn spawn_manager_target_refresher(
    targets: tokio::sync::watch::Sender<Vec<hel::hel_session_manager::RelaySessionTarget>>,
    cancellation: CancellationToken,
    state: Arc<RuntimeState>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => return,
                _ = interval.tick() => {
                    match tokio::task::spawn_blocking(|| {
                        Controller::load().map(|controller| {
                            let targets = dashboard_worker_targets(&controller);
                            (controller, targets)
                        })
                    }).await {
                        Ok(Ok((controller, refreshed))) => {
                            *state.controller.lock().unwrap_or_else(PoisonError::into_inner) = controller;
                            targets.send_replace(refreshed);
                        }
                        Ok(Err(error)) => {
                            tracing::warn!(error = format!("{error:#}"), "could not refresh daemon session targets");
                        }
                        Err(error) => {
                            tracing::error!(%error, "daemon target refresh task failed");
                            return;
                        }
                    }
                }
            }
        }
    })
}

async fn refresh_runtime_controller(state: &RuntimeState) {
    match tokio::task::spawn_blocking(Controller::load).await {
        Ok(Ok(controller)) => {
            *state
                .controller
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = controller;
            let revision = state.revision.fetch_add(1, Ordering::AcqRel) + 1;
            state.revision_tx.send_replace(revision);
        }
        Ok(Err(error)) => {
            tracing::warn!(
                error = format!("{error:#}"),
                "could not refresh daemon controller state"
            );
        }
        Err(error) => tracing::error!(%error, "daemon controller refresh task failed"),
    }
}

fn spawn_phone_server(
    config: hel::hel_config::PhoneConfig,
    cancellation: CancellationToken,
    state: Arc<RuntimeState>,
    worker: SessionManagerChannels,
) {
    tokio::spawn(async move {
        let reporter = {
            let state = state.clone();
            move |status| state.set_phone_status(status)
        };
        match crate::server::run_server(
            (&config).into(),
            cancellation.clone(),
            reporter,
            worker,
            state.clone(),
        )
        .await
        {
            Ok(()) if cancellation.is_cancelled() => {}
            Ok(()) => state.set_phone_status("stopped unexpectedly"),
            Err(error) => {
                tracing::warn!(error = format!("{error:#}"), "phone server stopped");
                state.set_phone_status(format!("error: {error:#}"));
            }
        }
    });
}

fn spawn_remote_request_bridge(
    mut requests: hel::hel_session_manager::RemoteSessionRequests,
    manager: SessionManagerControl,
) {
    tokio::spawn(async move {
        while let Some(request) = requests.recv().await {
            let manager = manager.clone();
            tokio::spawn(async move {
                forward_in_process_session_request(request, manager).await;
            });
        }
    });
}

async fn forward_in_process_session_request(
    request: RemoteSessionRequest,
    manager: SessionManagerControl,
) {
    match request {
        RemoteSessionRequest::Submit {
            session_id,
            command_id,
            command,
            reply,
        } => {
            let result = async {
                manager
                    .session(session_id)
                    .await?
                    .submit(command_id, command)
                    .await
            }
            .await
            .map_err(|error| format!("{error:#}"));
            let _ = reply.send(result);
        }
        RemoteSessionRequest::Sync { session_id, reply } => {
            let result = async { manager.session(session_id).await?.sync_now().await }
                .await
                .map_err(|error| format!("{error:#}"));
            let _ = reply.send(result);
        }
        RemoteSessionRequest::RespondElicitation {
            session_id,
            elicitation_id,
            response,
            reply,
        } => {
            let result = async {
                manager
                    .session(session_id)
                    .await?
                    .respond_elicitation(elicitation_id, response)
                    .await
            }
            .await
            .map_err(|error| format!("{error:#}"));
            let _ = reply.send(result);
        }
    }
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
    state: &Arc<RuntimeState>,
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
        DaemonAction::RuntimeSnapshot {
            workspace_id,
            after_revision,
        } => Ok(DaemonReply::RuntimeSnapshot(
            state
                .runtime_snapshot(&workspace_id, after_revision)
                .await?,
        )),
        DaemonAction::SubmitSessionCommand {
            session_id,
            command_id,
            command,
        } => {
            let session = state.session_manager.session(session_id).await?;
            Ok(DaemonReply::Ordinal(
                session.submit(command_id, command).await?,
            ))
        }
        DaemonAction::SyncSession { session_id } => {
            state
                .session_manager
                .session(session_id)
                .await?
                .sync_now()
                .await?;
            Ok(DaemonReply::Done)
        }
        DaemonAction::RespondElicitation {
            session_id,
            elicitation_id,
            response,
        } => {
            state
                .session_manager
                .session(session_id)
                .await?
                .respond_elicitation(elicitation_id, response)
                .await?;
            Ok(DaemonReply::Done)
        }
        DaemonAction::CloseSession { session_id } => {
            state.close_session(session_id).await?;
            Ok(DaemonReply::Done)
        }
        DaemonAction::ResumeSession(request) => Ok(DaemonReply::MaterializedSession(
            state.resume_session(request).await?,
        )),
        DaemonAction::ForceStopSession { session_id } => {
            state.force_stop_session(session_id).await?;
            Ok(DaemonReply::Done)
        }
        DaemonAction::DestroyStoppedSession { session_id } => {
            state.destroy_stopped_session(session_id).await?;
            Ok(DaemonReply::Done)
        }
        DaemonAction::CancelLifecycle { session_id } => {
            state.cancel_lifecycle(&session_id)?;
            Ok(DaemonReply::Done)
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
        let remote = spawn_remote_session_manager().unwrap();
        let recovery = hel::hel_recovery::RecoveryCoordinator::spawn(remote.control.clone());
        let state = Arc::new(RuntimeState::new(
            remote.control,
            Controller {
                config: HelConfig::default(),
                state: hel::hel_state::HelState::default(),
            },
            recovery.observer(),
        ));
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
            serve_client(stream, server_metadata, state, CancellationToken::new())
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
