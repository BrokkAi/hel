//! Persistent per-user controller daemon and its authenticated local protocol.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail, ensure};
use hel::hel_config::{HelConfig, data_dir};
use hel::hel_controller::{
    Controller, ControllerStoreGuard, ResumeRepositorySourceReceipt, SessionLaunchOptions,
    SessionResumeOptions,
};
use hel::hel_credentials::CredentialSyncSignal;
use hel::hel_elicitation::ElicitationResponse;
use hel::hel_session_manager::{
    ManagedSessionView, RemoteSessionPublisher, RemoteSessionRequest, SessionManagerChannels,
    SessionManagerControl, ViewError, spawn_remote_session_manager, spawn_session_manager,
};
use hel::hel_state::{
    HostContainerSize, MaterializedSession, RecoveryObservation, RecoveryObserver, SessionRecord,
    SessionResourceAllocation, SessionState,
};
use hel::hel_targets::{
    AdditionalMount, CancellableProcessExecutor, CommandExecutor, CommandOutput, CommandSpec,
    ProvisionStage, ProvisionStageGuard,
};
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

const PROTOCOL_VERSION: u32 = 3;
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
    pub config: HelConfig,
    pub records: Vec<SessionRecord>,
    pub sessions: Vec<RuntimeSessionView>,
    pub lifecycles: Vec<RuntimeLifecycleView>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RuntimeLifecycleKind {
    Create,
    Close,
    Resume,
    ForceStop,
    DestroyStopped,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RuntimeLifecycleView {
    pub session_id: String,
    pub kind: RuntimeLifecycleKind,
    pub started_at_epoch_seconds: u64,
    pub active_stages: Vec<(ProvisionStage, u64)>,
    pub resume_destination: Option<(String, String)>,
    pub notice: Option<String>,
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
pub(crate) struct CreateSessionRequest {
    pub workspace_id: String,
    pub profile_id: String,
    pub bundle_id: String,
    pub project_directory: Option<PathBuf>,
    pub target_template_id: String,
    pub additional_mounts: Vec<AdditionalMount>,
    pub allow_dirty_local: bool,
    pub resource_allocation: Option<SessionResourceAllocation>,
    pub title: String,
    pub session_title_override: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RegisteredSession {
    pub session: SessionRecord,
    pub remembered_container_size: Option<(String, HostContainerSize)>,
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
    RenameProfile {
        old_id: String,
        new_id: String,
    },
    RenameTarget {
        old_id: String,
        new_id: String,
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
    StartCreateSession(CreateSessionRequest),
    WaitCreateSession {
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
    RegisteredSession(Box<RegisteredSession>),
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
    pub phone_status: WebViewerStatus,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub(crate) enum WebViewerStatus {
    Disabled,
    Starting,
    Ready {
        viewer_url: String,
        viewer_code: String,
        qr_login_url: Option<String>,
        fallback_reason: Option<String>,
    },
    Stopped,
    Error {
        message: String,
    },
}

impl std::fmt::Debug for WebViewerStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ready {
                viewer_url,
                viewer_code,
                fallback_reason,
                ..
            } => formatter
                .debug_struct("Ready")
                .field("viewer_url", viewer_url)
                .field("viewer_code", viewer_code)
                .field("qr_login_url", &"[redacted]")
                .field("fallback_reason", fallback_reason)
                .finish(),
            Self::Disabled => formatter.write_str("Disabled"),
            Self::Starting => formatter.write_str("Starting"),
            Self::Stopped => formatter.write_str("Stopped"),
            Self::Error { message } => formatter.debug_tuple("Error").field(message).finish(),
        }
    }
}

impl std::fmt::Display for WebViewerStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Disabled => formatter.write_str("disabled"),
            Self::Starting => formatter.write_str("starting"),
            Self::Stopped => formatter.write_str("stopped unexpectedly"),
            Self::Error { message } => write!(formatter, "error: {message}"),
            Self::Ready {
                viewer_url,
                viewer_code,
                fallback_reason,
                ..
            } => {
                write!(formatter, "{viewer_url}; viewer code {viewer_code}")?;
                if let Some(reason) = fallback_reason {
                    write!(
                        formatter,
                        "; local only because Tailscale HTTPS is unavailable: {reason}"
                    )?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone)]
struct Attachment {
    workspace_id: String,
    pid: u32,
}

pub(crate) struct RuntimeState {
    attachments: Mutex<BTreeMap<String, Attachment>>,
    phone_status: Mutex<WebViewerStatus>,
    ever_attached: AtomicBool,
    sessions: Mutex<BTreeMap<String, RuntimeSessionView>>,
    revision: std::sync::atomic::AtomicU64,
    revision_tx: tokio::sync::watch::Sender<u64>,
    workspaces_tx: tokio::sync::watch::Sender<Vec<WorkspaceRecord>>,
    session_manager: SessionManagerControl,
    lifecycle: Mutex<BTreeMap<String, ActiveLifecycle>>,
    controller: Mutex<Controller>,
    config_mutation: tokio::sync::Mutex<()>,
    recovery_observer: RecoveryObserver,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LifecycleKind {
    Create,
    Close,
    Resume,
    ForceStop,
    DestroyStopped,
}

struct ActiveLifecycle {
    kind: LifecycleKind,
    cancelled: Arc<AtomicBool>,
    started_at_epoch_seconds: u64,
    active_stages: BTreeMap<ProvisionStage, (usize, u64)>,
    resume_destination: Option<(String, String)>,
    notice: Option<String>,
    result:
        tokio::sync::watch::Receiver<Option<std::result::Result<DaemonLifecycleResult, String>>>,
}

#[derive(Debug, Clone)]
enum DaemonLifecycleResult {
    Done,
    Materialized(Box<MaterializedSession>),
}

impl From<LifecycleKind> for RuntimeLifecycleKind {
    fn from(kind: LifecycleKind) -> Self {
        match kind {
            LifecycleKind::Create => Self::Create,
            LifecycleKind::Close => Self::Close,
            LifecycleKind::Resume => Self::Resume,
            LifecycleKind::ForceStop => Self::ForceStop,
            LifecycleKind::DestroyStopped => Self::DestroyStopped,
        }
    }
}

impl RuntimeState {
    fn new(
        session_manager: SessionManagerControl,
        controller: Controller,
        recovery_observer: RecoveryObserver,
        workspaces: Vec<WorkspaceRecord>,
    ) -> Self {
        // Revisions are opaque cursors, so give every daemon incarnation a
        // fresh high-water mark. Clients that survive a daemon restart must
        // never wait on, or render, a cursor from the previous process as if
        // it belonged to the new feed.
        let initial_revision = u64::try_from(chrono::Utc::now().timestamp_micros()).unwrap_or(1);
        let (revision_tx, _) = tokio::sync::watch::channel(initial_revision);
        let (workspaces_tx, _) = tokio::sync::watch::channel(workspaces);
        Self {
            attachments: Mutex::new(BTreeMap::new()),
            phone_status: Mutex::new(WebViewerStatus::Starting),
            ever_attached: AtomicBool::new(false),
            sessions: Mutex::new(BTreeMap::new()),
            revision: std::sync::atomic::AtomicU64::new(initial_revision),
            revision_tx,
            workspaces_tx,
            session_manager,
            lifecycle: Mutex::new(BTreeMap::new()),
            controller: Mutex::new(controller),
            config_mutation: tokio::sync::Mutex::new(()),
            recovery_observer,
        }
    }

    pub(crate) fn allocate_revision(&self) -> u64 {
        self.revision.fetch_add(1, Ordering::AcqRel) + 1
    }

    fn publish_revision(&self) -> u64 {
        let revision = self.allocate_revision();
        self.revision_tx.send_replace(revision);
        revision
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

    fn set_phone_status(&self, status: WebViewerStatus) {
        *self
            .phone_status
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = status;
    }

    fn phone_status(&self) -> WebViewerStatus {
        self.phone_status
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn workspaces(&self) -> tokio::sync::watch::Receiver<Vec<WorkspaceRecord>> {
        self.workspaces_tx.subscribe()
    }

    pub(crate) fn revisions(&self) -> tokio::sync::watch::Receiver<u64> {
        self.revision_tx.subscribe()
    }

    fn publish_workspaces(&self, workspaces: Vec<WorkspaceRecord>) {
        self.workspaces_tx.send_replace(workspaces);
    }

    pub(crate) async fn reload_controller(&self) -> Result<()> {
        // Serialize installs so an earlier phone publication cannot overwrite
        // a later completed lifecycle with the controller snapshot it loaded.
        let _mutation = self.config_mutation.lock().await;
        let controller = tokio::task::spawn_blocking(Controller::load)
            .await
            .context("daemon controller reload task panicked")??;
        let session_count = controller.state.sessions.len();
        *self
            .controller
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = controller;
        let revision = self.publish_revision();
        tracing::debug!(revision, session_count, "daemon controller state reloaded");
        Ok(())
    }

    async fn publish_session(&self, session_id: String, view: ManagedSessionView) -> Result<()> {
        let connected = view.connected;
        let has_snapshot = view.snapshot.is_some();
        tracing::debug!(
            %session_id,
            connected,
            has_snapshot,
            "daemon received a session view"
        );
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
        reach_test_hook("relay_projection_before_revision_publication").await?;
        self.publish_revision();
        Ok(())
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
        let lifecycles = self
            .lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|(session_id, active)| {
                session_ids.contains(*session_id) && active.result.borrow().is_none()
            })
            .map(|(session_id, active)| RuntimeLifecycleView {
                session_id: session_id.clone(),
                kind: active.kind.into(),
                started_at_epoch_seconds: active.started_at_epoch_seconds,
                active_stages: active
                    .active_stages
                    .iter()
                    .map(|(stage, (_, started_at))| (*stage, *started_at))
                    .collect(),
                resume_destination: active.resume_destination.clone(),
                notice: active.notice.clone(),
            })
            .collect();
        let controller = self
            .controller
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let records = runtime_records_for_sessions(&controller, &session_ids);
        Ok(RuntimeSnapshot {
            revision,
            config: controller.config.clone(),
            records,
            sessions,
            lifecycles,
        })
    }

    fn start_or_join_lifecycle<F, Fut>(
        self: &Arc<Self>,
        session_id: String,
        kind: LifecycleKind,
        work: F,
    ) -> Result<
        tokio::sync::watch::Receiver<Option<std::result::Result<DaemonLifecycleResult, String>>>,
    >
    where
        F: FnOnce(Arc<Self>, String, Arc<AtomicBool>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<DaemonLifecycleResult>> + Send + 'static,
    {
        let mut work = Some(work);
        let result = {
            let mut lifecycle = self
                .lifecycle
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let completed_other_kind = lifecycle
                .get(&session_id)
                .is_some_and(|active| active.kind != kind && active.result.borrow().is_some());
            if completed_other_kind {
                lifecycle.remove(&session_id);
            }
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
                        started_at_epoch_seconds: epoch_seconds(),
                        active_stages: BTreeMap::new(),
                        resume_destination: None,
                        notice: None,
                        result: result_rx.clone(),
                    },
                );
                self.publish_revision();
                let state = Arc::clone(self);
                let operation_session_id = session_id.clone();
                let operation = work.take().expect("new lifecycle operation has work");
                tokio::spawn(async move {
                    let mut result =
                        operation(state.clone(), operation_session_id.clone(), cancelled)
                            .await
                            .map_err(|error| format!("{error:#}"));
                    if let Err(error) = state.reload_controller().await {
                        let reload_error = format!(
                            "reload daemon state after lifecycle operation for {operation_session_id}: {error:#}"
                        );
                        if result.is_ok() {
                            result = Err(reload_error);
                        } else {
                            tracing::warn!(
                                session_id = %operation_session_id,
                                error = reload_error,
                                "lifecycle failed and its durable state could not be reloaded"
                            );
                        }
                    }
                    if let Err(error) =
                        reach_test_hook("lifecycle_reservation_before_result_publication").await
                    {
                        result = Err(format!("test lifecycle publication hook failed: {error:#}"));
                    }
                    result_tx.send_replace(Some(result));
                    state.publish_revision();
                });
                result_rx
            }
        };
        Ok(result)
    }

    async fn wait_lifecycle_result(
        mut result: tokio::sync::watch::Receiver<
            Option<std::result::Result<DaemonLifecycleResult, String>>,
        >,
    ) -> Result<DaemonLifecycleResult> {
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
        let result = self.start_or_join_lifecycle(session_id, kind, work)?;
        let channel = result.clone();
        let outcome = Self::wait_lifecycle_result(result).await;
        self.remove_completed_lifecycle(&channel);
        outcome
    }

    fn remove_completed_lifecycle(
        &self,
        channel: &tokio::sync::watch::Receiver<
            Option<std::result::Result<DaemonLifecycleResult, String>>,
        >,
    ) {
        self.lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|_, active| {
                !active.result.same_channel(channel) || active.result.borrow().is_none()
            });
    }

    async fn start_create_session(
        self: &Arc<Self>,
        request: CreateSessionRequest,
    ) -> Result<RegisteredSession> {
        let registered = blocking(move || {
            let mut controller = Controller::load()?;
            let session_id = controller.register_session_with_resources(
                &request.profile_id,
                &request.bundle_id,
                &request.target_template_id,
                request.title,
                SessionLaunchOptions {
                    workspace_id: request.workspace_id,
                    additional_mounts: request.additional_mounts,
                    allow_dirty_local: request.allow_dirty_local,
                    resource_allocation: request.resource_allocation,
                    project_directory: request.project_directory,
                    session_title_override: request.session_title_override,
                },
            )?;
            let session = controller
                .state
                .sessions
                .get(&session_id)
                .expect("newly registered session exists")
                .clone();
            let remembered_container_size = controller
                .config
                .targets
                .get(&request.target_template_id)
                .and_then(hel::hel_config::container_size_host)
                .and_then(|host| {
                    controller
                        .state
                        .container_sizes
                        .get(host)
                        .copied()
                        .map(|size| (host.to_owned(), size))
                });
            Ok(RegisteredSession {
                session,
                remembered_container_size,
            })
        })
        .await?;
        let session_id = registered.session.id.clone();
        self.start_or_join_lifecycle(
            session_id,
            LifecycleKind::Create,
            |state, session_id, cancelled| async move {
                let mut controller = tokio::task::spawn_blocking(Controller::load)
                    .await
                    .context("load controller for daemon create task")??;
                let executor = DaemonStageReportingExecutor::new(
                    CancellableProcessExecutor::new(cancelled),
                    state,
                    session_id.clone(),
                );
                controller
                    .provision_session_controlled(&session_id, &executor)
                    .await?;
                Ok(DaemonLifecycleResult::Done)
            },
        )?;
        Ok(registered)
    }

    async fn wait_create_session(&self, session_id: &str) -> Result<()> {
        let result = {
            let lifecycle = self
                .lifecycle
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let active = lifecycle
                .get(session_id)
                .with_context(|| format!("no create operation exists for session {session_id}"))?;
            ensure!(
                active.kind == LifecycleKind::Create,
                "session {session_id} is no longer being created"
            );
            active.result.clone()
        };
        let channel = result.clone();
        let outcome = Self::wait_lifecycle_result(result).await;
        self.remove_completed_lifecycle(&channel);
        match outcome? {
            DaemonLifecycleResult::Done => Ok(()),
            DaemonLifecycleResult::Materialized(_) => {
                bail!("daemon create completed with an unexpected projection")
            }
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
                let executor = DaemonStageReportingExecutor::new(
                    CancellableProcessExecutor::new(cancelled),
                    state.clone(),
                    session_id.clone(),
                );
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
        let profile_id = request.profile_id.clone();
        let target_template_id = request.target_template_id.clone();
        let operation_session_id = session_id.clone();
        let result = self.start_or_join_lifecycle(
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
                let executor = DaemonStageReportingExecutor::new(
                    CancellableProcessExecutor::new(cancelled),
                    state.clone(),
                    session_id.clone(),
                );
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
        )?;
        self.set_lifecycle_resume_destination(
            &operation_session_id,
            profile_id,
            target_template_id,
        );
        let channel = result.clone();
        let result = Self::wait_lifecycle_result(result).await;
        self.remove_completed_lifecycle(&channel);
        let result = result?;
        match result {
            DaemonLifecycleResult::Materialized(materialized) => Ok(*materialized),
            DaemonLifecycleResult::Done => bail!("daemon resume completed without a projection"),
        }
    }

    async fn force_stop_session(self: &Arc<Self>, session_id: String) -> Result<()> {
        self.run_lifecycle(
            session_id,
            LifecycleKind::ForceStop,
            |state, session_id, cancelled| async move {
                blocking(move || {
                    let mut controller = Controller::load()?;
                    let executor = DaemonStageReportingExecutor::new(
                        CancellableProcessExecutor::new(cancelled),
                        state,
                        session_id.clone(),
                    );
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
            |state, session_id, cancelled| async move {
                blocking(move || {
                    let mut controller = Controller::load()?;
                    let executor = DaemonStageReportingExecutor::new(
                        CancellableProcessExecutor::new(cancelled),
                        state,
                        session_id.clone(),
                    );
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

    fn set_lifecycle_resume_destination(
        &self,
        session_id: &str,
        profile_id: String,
        target_id: String,
    ) {
        if let Some(active) = self
            .lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_mut(session_id)
        {
            active.resume_destination = Some((profile_id, target_id));
            self.publish_revision();
        }
    }

    fn change_lifecycle_stage(&self, session_id: &str, stage: ProvisionStage, active: bool) {
        let changed = {
            let mut lifecycle = self
                .lifecycle
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let Some(operation) = lifecycle.get_mut(session_id) else {
                return;
            };
            if active {
                let entry = operation
                    .active_stages
                    .entry(stage)
                    .or_insert_with(|| (0, epoch_seconds()));
                entry.0 += 1;
                entry.0 == 1
            } else {
                let Some((count, _)) = operation.active_stages.get_mut(&stage) else {
                    return;
                };
                *count -= 1;
                if *count == 0 {
                    operation.active_stages.remove(&stage);
                    true
                } else {
                    false
                }
            }
        };
        if changed {
            self.publish_revision();
        }
    }

    fn set_lifecycle_notice(&self, session_id: &str, notice: &str) {
        if let Some(active) = self
            .lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_mut(session_id)
        {
            active.notice = Some(notice.to_owned());
            self.publish_revision();
        }
    }
}

fn runtime_records_for_sessions(
    controller: &Controller,
    session_ids: &BTreeSet<String>,
) -> Vec<SessionRecord> {
    controller
        .state
        .sessions
        .iter()
        .filter(|(session_id, _)| session_ids.contains(*session_id))
        .map(|(_, session)| session.clone())
        .collect()
}

struct DaemonStageReportingExecutor<E> {
    inner: E,
    state: Arc<RuntimeState>,
    session_id: String,
}

impl<E> DaemonStageReportingExecutor<E> {
    fn new(inner: E, state: Arc<RuntimeState>, session_id: String) -> Self {
        Self {
            inner,
            state,
            session_id,
        }
    }
}

impl<E: CommandExecutor> CommandExecutor for DaemonStageReportingExecutor<E> {
    fn execute(&self, command: &CommandSpec) -> Result<CommandOutput> {
        let _stage = command
            .stage
            .map(|stage| ProvisionStageGuard::new(self, stage));
        self.inner.execute(command)
    }

    fn execute_with_stdin(
        &self,
        command: &CommandSpec,
        input: &mut (dyn std::io::Read + Send),
    ) -> Result<CommandOutput> {
        let _stage = command
            .stage
            .map(|stage| ProvisionStageGuard::new(self, stage));
        self.inner.execute_with_stdin(command, input)
    }

    fn cancellation_requested(&self) -> bool {
        self.inner.cancellation_requested()
    }

    fn stage_started(&self, stage: ProvisionStage) {
        self.state
            .change_lifecycle_stage(&self.session_id, stage, true);
    }

    fn stage_finished(&self, stage: ProvisionStage) {
        self.state
            .change_lifecycle_stage(&self.session_id, stage, false);
    }

    fn notify_notice(&self, notice: &str) {
        self.state.set_lifecycle_notice(&self.session_id, notice);
    }
}

fn epoch_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
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

    pub(crate) async fn rename_profile(&mut self, old_id: String, new_id: String) -> Result<()> {
        match self
            .request(DaemonAction::RenameProfile { old_id, new_id })
            .await?
        {
            DaemonReply::Done => Ok(()),
            reply => bail!("unexpected rename-profile reply {reply:?}"),
        }
    }

    pub(crate) async fn rename_target(&mut self, old_id: String, new_id: String) -> Result<()> {
        match self
            .request(DaemonAction::RenameTarget { old_id, new_id })
            .await?
        {
            DaemonReply::Done => Ok(()),
            reply => bail!("unexpected rename-target reply {reply:?}"),
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

    pub(crate) async fn start_create_session(
        &mut self,
        request: CreateSessionRequest,
    ) -> Result<RegisteredSession> {
        match self
            .request(DaemonAction::StartCreateSession(request))
            .await?
        {
            DaemonReply::RegisteredSession(registered) => Ok(*registered),
            reply => bail!("unexpected start-create reply {reply:?}"),
        }
    }

    pub(crate) async fn wait_create_session(&mut self, session_id: String) -> Result<()> {
        match self
            .request(DaemonAction::WaitCreateSession { session_id })
            .await?
        {
            DaemonReply::Done => Ok(()),
            reply => bail!("unexpected wait-create reply {reply:?}"),
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
    Controller::recover_config_id_rename()?;
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
    reach_test_hook("daemon_metadata_before_listening").await?;

    let workspaces = tokio::task::spawn_blocking(hel::hel_database::list_workspaces)
        .await
        .context("daemon workspace load task panicked")??;
    let state = Arc::new(RuntimeState::new(
        manager_control.clone(),
        Controller {
            config: controller.config.clone(),
            state: controller.state.clone(),
        },
        recovery_observer.clone(),
        workspaces,
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
        state.set_phone_status(WebViewerStatus::Disabled);
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
                state.publish_session(update.session_id, update.view).await?;
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
                    // Keep a controller loaded from the old config from being
                    // installed after a concurrent id rename has committed.
                    let _config_mutation = state.config_mutation.lock().await;
                    match tokio::task::spawn_blocking(|| {
                        Controller::load().map(|controller| {
                            let targets = dashboard_worker_targets(&controller);
                            (controller, targets)
                        })
                    }).await {
                        Ok(Ok((controller, refreshed))) => {
                            let changed = {
                                let mut current = state
                                    .controller
                                    .lock()
                                    .unwrap_or_else(PoisonError::into_inner);
                                let changed = current.config != controller.config;
                                *current = controller;
                                changed
                            };
                            targets.send_replace(refreshed);
                            if changed {
                                state.publish_revision();
                            }
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
    if let Err(error) = state.reload_controller().await {
        tracing::warn!(
            error = format!("{error:#}"),
            "could not refresh daemon controller state"
        );
    }
}

async fn refresh_runtime_workspaces(state: &RuntimeState) -> Result<()> {
    let workspaces = tokio::task::spawn_blocking(hel::hel_database::list_workspaces)
        .await
        .context("daemon workspace refresh task panicked")??;
    state.publish_workspaces(workspaces);
    Ok(())
}

fn spawn_phone_server(
    config: hel::hel_config::PhoneConfig,
    cancellation: CancellationToken,
    state: Arc<RuntimeState>,
    worker: SessionManagerChannels,
) {
    state.set_phone_status(WebViewerStatus::Starting);
    let workspaces = state.workspaces();
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
            workspaces,
        )
        .await
        {
            Ok(()) if cancellation.is_cancelled() => {}
            Ok(()) => state.set_phone_status(WebViewerStatus::Stopped),
            Err(error) => {
                tracing::warn!(error = format!("{error:#}"), "phone server stopped");
                state.set_phone_status(WebViewerStatus::Error {
                    message: format!("{error:#}"),
                });
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

async fn reach_test_hook(name: &'static str) -> Result<()> {
    #[cfg(feature = "test-hooks")]
    {
        tokio::task::spawn_blocking(move || hel::hel_test_hooks::reach_test_hook(name))
            .await
            .context("test hook task panicked")??;
    }
    #[cfg(not(feature = "test-hooks"))]
    let _ = name;
    Ok(())
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
        DaemonAction::CreateWorkspace { name } => {
            let workspace = blocking(move || hel::hel_database::create_workspace(&name)).await?;
            refresh_runtime_workspaces(state).await?;
            Ok(DaemonReply::Workspace(workspace))
        }
        DaemonAction::RenameWorkspace { workspace_id, name } => {
            blocking(move || hel::hel_database::rename_workspace(&workspace_id, &name)).await?;
            refresh_runtime_workspaces(state).await?;
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
            refresh_runtime_workspaces(state).await?;
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
        DaemonAction::RenameProfile { old_id, new_id } => {
            let _config_mutation = state.config_mutation.lock().await;
            ensure_no_active_lifecycle(state)?;
            let controller = blocking(move || {
                let mut controller = Controller::load()?;
                controller.rename_profile_id(&old_id, &new_id)?;
                Ok(controller)
            })
            .await?;
            install_renamed_controller(state, controller);
            Ok(DaemonReply::Done)
        }
        DaemonAction::RenameTarget { old_id, new_id } => {
            let _config_mutation = state.config_mutation.lock().await;
            ensure_no_active_lifecycle(state)?;
            let controller = blocking(move || {
                let mut controller = Controller::load()?;
                controller.rename_target_id(&old_id, &new_id)?;
                Ok(controller)
            })
            .await?;
            install_renamed_controller(state, controller);
            Ok(DaemonReply::Done)
        }
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
        DaemonAction::StartCreateSession(request) => Ok(DaemonReply::RegisteredSession(Box::new(
            state.start_create_session(request).await?,
        ))),
        DaemonAction::WaitCreateSession { session_id } => {
            state.wait_create_session(&session_id).await?;
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

fn ensure_no_active_lifecycle(state: &RuntimeState) -> Result<()> {
    ensure!(
        !state
            .lifecycle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .any(|active| active.result.borrow().is_none()),
        "cannot rename configuration while a session lifecycle operation is active"
    );
    Ok(())
}

fn install_renamed_controller(state: &RuntimeState, controller: Controller) {
    *state
        .controller
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = controller;
    state.publish_revision();
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

    fn test_runtime_state() -> Arc<RuntimeState> {
        let remote = spawn_remote_session_manager().unwrap();
        let recovery = hel::hel_recovery::RecoveryCoordinator::spawn(remote.control.clone());
        Arc::new(RuntimeState::new(
            remote.control,
            Controller {
                config: HelConfig::default(),
                state: hel::hel_state::HelState::default(),
            },
            recovery.observer(),
            Vec::new(),
        ))
    }

    #[tokio::test]
    async fn workspace_publication_reaches_existing_phone_subscriber() {
        let state = test_runtime_state();
        let mut workspaces = state.workspaces();
        let expected = WorkspaceRecord {
            id: "workspace-1".into(),
            name: "Reliability".into(),
            created_at: "2026-08-30T00:00:00Z".into(),
            last_opened_at: "2026-08-30T00:00:00Z".into(),
            session_count: 0,
        };

        state.publish_workspaces(vec![expected.clone()]);
        tokio::time::timeout(Duration::from_secs(1), workspaces.changed())
            .await
            .expect("workspace publication timed out")
            .expect("workspace publisher stopped");

        assert_eq!(workspaces.borrow_and_update().as_slice(), &[expected]);
    }

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
        let state = test_runtime_state();
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

    #[tokio::test]
    async fn equivalent_lifecycle_requests_join_one_daemon_operation() {
        let state = test_runtime_state();
        let starts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let release = Arc::new(tokio::sync::Notify::new());
        let first = state
            .start_or_join_lifecycle("session-1".into(), LifecycleKind::Close, {
                let starts = starts.clone();
                let release = release.clone();
                move |_state, _session_id, _cancelled| async move {
                    starts.fetch_add(1, Ordering::AcqRel);
                    release.notified().await;
                    Ok(DaemonLifecycleResult::Done)
                }
            })
            .unwrap();
        tokio::task::yield_now().await;
        let second = state
            .start_or_join_lifecycle(
                "session-1".into(),
                LifecycleKind::Close,
                |_state, _session_id, _cancelled| async move {
                    panic!("joined lifecycle request started duplicate work")
                },
            )
            .unwrap();
        assert_eq!(starts.load(Ordering::Acquire), 1);
        assert!(
            state
                .start_or_join_lifecycle(
                    "session-1".into(),
                    LifecycleKind::Resume,
                    |_state, _session_id, _cancelled| async move {
                        Ok(DaemonLifecycleResult::Done)
                    },
                )
                .is_err()
        );

        // The daemon task is independent of either client waiter.
        drop(first);
        release.notify_one();
        assert!(matches!(
            RuntimeState::wait_lifecycle_result(second).await.unwrap(),
            DaemonLifecycleResult::Done
        ));
        assert_eq!(starts.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn daemon_lifecycle_reports_balanced_concurrent_stages() {
        struct UnusedExecutor;

        impl CommandExecutor for UnusedExecutor {
            fn execute(&self, command: &CommandSpec) -> Result<CommandOutput> {
                panic!("a stage notification must not run {}", command.program)
            }
        }

        let state = test_runtime_state();
        let release = Arc::new(tokio::sync::Notify::new());
        let result = state
            .start_or_join_lifecycle("session-1".into(), LifecycleKind::Create, {
                let release = release.clone();
                move |_state, _session_id, _cancelled| async move {
                    release.notified().await;
                    Ok(DaemonLifecycleResult::Done)
                }
            })
            .unwrap();
        let executor =
            DaemonStageReportingExecutor::new(UnusedExecutor, state.clone(), "session-1".into());
        executor.stage_started(ProvisionStage::Cloning);
        executor.stage_started(ProvisionStage::Cloning);
        executor.stage_started(ProvisionStage::Syncing);
        executor.stage_finished(ProvisionStage::Cloning);
        {
            let lifecycle = state
                .lifecycle
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let stages = &lifecycle.get("session-1").unwrap().active_stages;
            assert_eq!(stages.get(&ProvisionStage::Cloning).unwrap().0, 1);
            assert_eq!(stages.get(&ProvisionStage::Syncing).unwrap().0, 1);
        }
        executor.stage_finished(ProvisionStage::Cloning);
        executor.stage_finished(ProvisionStage::Syncing);
        assert!(
            state
                .lifecycle
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get("session-1")
                .unwrap()
                .active_stages
                .is_empty()
        );
        release.notify_one();
        assert!(matches!(
            RuntimeState::wait_lifecycle_result(result).await.unwrap(),
            DaemonLifecycleResult::Done
        ));
    }
}
