//! One-shot background jobs the dashboard starts and the updates they report.
//!
//! Each job runs on a blocking task and answers over the dashboard's single
//! [`DashboardIoUpdate`] channel, so no filesystem, database, or process work
//! ever happens on the render loop. Failures travel as `Err` payloads rather
//! than being dropped.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use hel::hel_config::{HarnessKind, HelConfig, ProjectBundle, ProjectRepository};
use hel::hel_controller::ResumeRepositorySourcePreflight;
use hel::hel_controller::{Controller, SessionLaunchOptions};
use hel::hel_import::{configured_bundle_for_local, configured_bundle_for_origin};
use hel::hel_setup::github_repository_from_origin;
use hel::hel_state::{
    HelState, MaterializedSession, ProjectSourceIdentity, RecoveryObserver, SessionRecord,
    SessionState,
};
use hel::hel_targets::{
    BoundedProcessExecutor, CancellableProcessExecutor, CommandExecutor, CommandOutput,
    CommandSpec, ProvisionStage,
};
use hel_tui::{DashboardAction, PreparedMaterializedSessionDetail, SessionOperationKind};
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;

use crate::dashboard::DashboardContext;
use crate::import::{DashboardImportSuccess, PendingDashboardImport, persist_imported_session};
use crate::pollers::{
    LifecycleSuccess, LifecycleUpdate, WorkerRecordPersistence, reserve_recovery_or_cancel,
};
use crate::short_id;

/// Longest the quit path waits for the detach write before giving up.
pub(crate) const DETACH_PERSIST_QUIT_TIMEOUT: Duration = Duration::from_secs(5);

/// Everything the dashboard learns from a background job.
pub(crate) enum DashboardIoUpdate {
    WorkerRecordPersistence {
        operation: WorkerRecordPersistence,
        result: std::result::Result<(), String>,
    },
    MaterializedSessionProjection {
        session_id: String,
        result: std::result::Result<Box<PreparedMaterializedSessionDetail>, String>,
    },
    ProjectSource {
        session_id: String,
        result: std::result::Result<ProjectSourceIdentity, String>,
    },
    CreateSession(Box<DashboardCreateSessionUpdate>),
    RenameSession {
        session_id: String,
        title: String,
        result: std::result::Result<String, String>,
    },
    ContainerSettings {
        session_id: String,
        result: std::result::Result<(), String>,
    },
    DetachedSessionState {
        session_id: String,
        result: std::result::Result<(), String>,
    },
    ReadReceipt {
        session_id: String,
        result: std::result::Result<u64, String>,
    },
    CreatedBundle {
        result: Box<std::result::Result<CreatedBundleUpdate, String>>,
    },
    ImportedSessionApplied {
        result: Box<std::result::Result<ImportedDashboardSessionApply, String>>,
    },
    LifecycleReloaded(Box<LifecycleReloaded>),
    CheckpointArchiveSizes {
        generation: u64,
        sizes: BTreeMap<String, Option<u64>>,
    },
    WorkerDiagnosis {
        session_id: String,
        episode_id: u64,
        result: std::result::Result<Option<String>, String>,
    },
    MountCompletions {
        prefix: String,
        result: std::result::Result<Vec<String>, String>,
    },
    MountValidation {
        source: String,
        result: std::result::Result<Option<String>, String>,
    },
    SessionMountValidation {
        launch: Box<DashboardAction>,
        result: std::result::Result<Option<(String, String)>, String>,
    },
    ResumeRepositoryPreflight {
        launch: Box<DashboardAction>,
        submitted_repository_id: Option<String>,
        result: Box<std::result::Result<ResumeRepositoryPreflightApply, String>>,
    },
    ProjectValidation {
        directory: String,
        result: std::result::Result<(), String>,
    },
    LifecycleStage {
        session_id: String,
        stage: ProvisionStage,
    },
    /// Something a lifecycle operation decided on the user's behalf, such as
    /// attaching a directory read-only because its filesystem cannot overlay.
    LifecycleNotice {
        notice: String,
    },
    /// The set of native sessions the resume dialog hides, read from Hel's
    /// database.
    HiddenNativeSessions {
        result: std::result::Result<BTreeSet<(HarnessKind, String)>, String>,
    },
    /// A hide or reveal that has already been applied optimistically. Only a
    /// failure needs handling; `target` says what to put back so no row can
    /// stay out of step with what is stored.
    ArchiveWrite {
        what: String,
        target: ArchiveWriteTarget,
        result: std::result::Result<(), String>,
    },
}

/// Which hidden-row store an archive write was aimed at, and what the record
/// held before the optimistic update overwrote it.
pub(crate) enum ArchiveWriteTarget {
    /// A Hel session record. Its archived flag lives in two in-memory copies —
    /// the controller's state and the dashboard's — and both are restored.
    Session { session_id: String, archived: bool },
    /// The hidden native session set, which is re-read from the database
    /// rather than reconstructed.
    HiddenNativeSessions,
}

/// Puts back what an archive write did not manage to store. Returns whether
/// the hidden native set still has to be re-read from the database.
pub(crate) fn revert_archive_write(
    target: &ArchiveWriteTarget,
    state: &mut HelState,
    dashboard: &mut hel_tui::DashboardState,
) -> bool {
    match target {
        ArchiveWriteTarget::Session {
            session_id,
            archived,
        } => {
            if let Some(session) = state.sessions.get_mut(session_id) {
                session.archived = *archived;
            }
            dashboard.set_session_archived(session_id, *archived);
            false
        }
        ArchiveWriteTarget::HiddenNativeSessions => true,
    }
}

pub(crate) struct ActiveLifecycleOperation {
    pub(crate) cancelled: Arc<AtomicBool>,
    pub(crate) kind: SessionOperationKind,
}

pub(crate) struct RegisteredDashboardSession {
    session: SessionRecord,
    cancelled: Arc<AtomicBool>,
}

pub(crate) enum DashboardCreateSessionUpdate {
    DirtyLocal {
        action: DashboardAction,
        repositories: Vec<String>,
    },
    Registered(Box<RegisteredDashboardSession>),
    Failed(String),
}

pub(crate) struct ImportedDashboardSessionApply {
    harness: &'static str,
    native_session_id: String,
    session: SessionRecord,
    bundle_id: String,
    bundle: ProjectBundle,
}

pub(crate) struct CreatedBundleUpdate {
    config: HelConfig,
    bundle_id: String,
}

pub(crate) struct ResumeRepositoryPreflightApply {
    pub(crate) config: Option<HelConfig>,
    pub(crate) preflight: ResumeRepositorySourcePreflight,
}

pub(crate) struct LifecycleReload {
    pub(crate) update: LifecycleUpdate,
    pub(crate) operation: Option<ActiveLifecycleOperation>,
}

pub(crate) struct LifecycleReloaded {
    reload: LifecycleReload,
    result: std::result::Result<Controller, String>,
}

/// Runs one blocking job off the loop and reports its outcome on the
/// dashboard's I/O channel. Errors are formatted once, here, so no caller can
/// quietly drop one.
pub(crate) fn spawn_io<T>(
    updates: UnboundedSender<DashboardIoUpdate>,
    work: impl FnOnce() -> Result<T> + Send + 'static,
    report: impl FnOnce(std::result::Result<T, String>) -> DashboardIoUpdate + Send + 'static,
) -> JoinHandle<()>
where
    T: Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let result = work().map_err(|error| format!("{error:#}"));
        let _ = updates.send(report(result));
    })
}

/// Reads the hidden-session set out of Hel's own database. Called when the
/// resume dialog opens and again whenever a hide or reveal fails to commit.
pub(crate) fn spawn_hidden_native_sessions_load(
    updates: UnboundedSender<DashboardIoUpdate>,
) -> JoinHandle<()> {
    spawn_io(
        updates,
        hel::hel_database::hidden_native_sessions,
        |result| DashboardIoUpdate::HiddenNativeSessions { result },
    )
}

/// Resolves one raw checkout's Git origin off the event loop. Each session is
/// independent, so callers can launch these concurrently and redraw as the
/// answers arrive.
pub(crate) fn spawn_project_source_resolution(
    controller: &Controller,
    session_id: String,
    updates: UnboundedSender<DashboardIoUpdate>,
) -> JoinHandle<()> {
    let config = controller.config.clone();
    let session = controller.state.sessions.get(&session_id).cloned();
    let source_controller = Controller {
        config,
        state: HelState {
            sessions: session
                .map(|session| [(session_id.clone(), session)].into_iter().collect())
                .unwrap_or_default(),
            ..HelState::default()
        },
    };
    let reported_session_id = session_id.clone();
    spawn_io(
        updates,
        move || {
            source_controller.resolve_session_project_source(
                &session_id,
                &BoundedProcessExecutor::new(Duration::from_secs(8)),
            )
        },
        move |result| DashboardIoUpdate::ProjectSource {
            session_id: reported_session_id,
            result,
        },
    )
}

/// Persists one archive or unarchive. The dashboard already moved the row, so
/// only the failure path matters here: `target` carries what to restore.
pub(crate) fn spawn_archive_write(
    what: String,
    target: ArchiveWriteTarget,
    write: impl FnOnce() -> Result<()> + Send + 'static,
    updates: UnboundedSender<DashboardIoUpdate>,
) -> JoinHandle<()> {
    spawn_io(updates, write, move |result| {
        DashboardIoUpdate::ArchiveWrite {
            what,
            target,
            result,
        }
    })
}

/// A controller that answers target questions from configuration alone, for
/// the completions and validations the launch dialog asks for.
pub(crate) fn config_only_controller(config: HelConfig) -> Controller {
    Controller {
        config,
        state: HelState::default(),
    }
}

/// What every session lifecycle operation needs to run off the loop.
pub(crate) struct LifecycleOperationRequest {
    pub(crate) session_id: String,
    pub(crate) cancelled: Arc<AtomicBool>,
    /// Set when the operation must preempt a recovery copy already running for
    /// the session.
    pub(crate) recovery: Option<RecoveryObserver>,
    pub(crate) updates: UnboundedSender<LifecycleUpdate>,
}

/// Runs one session lifecycle operation on a blocking task.
///
/// Every one of them takes the same three steps around its own work: hold the
/// session against recovery copies, reload the controller so it acts on
/// durable state, and answer on the lifecycle channel whatever happens.
pub(crate) fn spawn_lifecycle_operation(
    request: LifecycleOperationRequest,
    work: impl FnOnce(&mut Controller, Arc<AtomicBool>) -> Result<LifecycleSuccess> + Send + 'static,
) {
    let LifecycleOperationRequest {
        session_id,
        cancelled,
        recovery,
        updates,
    } = request;
    let operation_session_id = session_id.clone();
    tokio::task::spawn_blocking(move || {
        let result = (|| -> Result<LifecycleSuccess> {
            let _recovery_reservation = recovery
                .map(|observer| {
                    reserve_recovery_or_cancel(&observer, &operation_session_id, &cancelled)
                })
                .transpose()?;
            let mut controller = Controller::load()?;
            work(&mut controller, cancelled)
        })()
        .map_err(|error| format!("{error:#}"));
        let _ = updates.send(LifecycleUpdate { session_id, result });
    });
}

pub(crate) fn spawn_materialized_session_projection(
    materialized: MaterializedSession,
    viewed_through_event_ordinal: u64,
    previous: hel_tui::MaterializedProjectionCache,
    updates: UnboundedSender<DashboardIoUpdate>,
    permits: Arc<tokio::sync::Semaphore>,
) {
    let session_id = materialized.session_id.clone();
    tokio::spawn(async move {
        let result = match permits.acquire_owned().await {
            Ok(permit) => {
                let result = tokio::task::spawn_blocking(move || {
                    PreparedMaterializedSessionDetail::from_materialized(
                        materialized,
                        viewed_through_event_ordinal,
                        previous,
                    )
                })
                .await
                .map(Box::new)
                .map_err(|error| format!("session projection task failed: {error}"));
                drop(permit);
                result
            }
            Err(error) => Err(format!("session projection worker stopped: {error}")),
        };
        let _ =
            updates.send(DashboardIoUpdate::MaterializedSessionProjection { session_id, result });
    });
}

pub(crate) fn spawn_lifecycle_reload(
    reload: LifecycleReload,
    updates: UnboundedSender<DashboardIoUpdate>,
) {
    spawn_io(updates, Controller::load, move |result| {
        DashboardIoUpdate::LifecycleReloaded(Box::new(LifecycleReloaded { reload, result }))
    });
}

pub(crate) fn spawn_dashboard_rename(
    session_id: String,
    title: String,
    updates: UnboundedSender<DashboardIoUpdate>,
) {
    let renamed_session_id = session_id.clone();
    let requested_title = title.clone();
    spawn_io(
        updates,
        move || Controller::load()?.rename_session(&renamed_session_id, &requested_title),
        move |result| DashboardIoUpdate::RenameSession {
            session_id,
            title,
            result,
        },
    );
}

/// What the container editor asks the controller to persist.
pub(crate) struct ContainerSettingsRequest {
    pub(crate) session_id: String,
    pub(crate) cpus: Option<String>,
    pub(crate) memory: Option<String>,
    pub(crate) additional_mounts: Vec<hel::hel_targets::AdditionalMount>,
    pub(crate) mount_history: Vec<std::path::PathBuf>,
}

pub(crate) fn spawn_dashboard_container_settings(
    request: ContainerSettingsRequest,
    updates: UnboundedSender<DashboardIoUpdate>,
) {
    let session_id = request.session_id.clone();
    spawn_io(
        updates,
        move || {
            Controller::load()?.update_session_container_settings(
                &request.session_id,
                request.cpus,
                request.memory,
                request.additional_mounts,
                request.mount_history,
            )
        },
        move |result| DashboardIoUpdate::ContainerSettings { session_id, result },
    );
}

/// Persist everything one detach produces: the read receipt and the unsent
/// draft. They describe the same moment and the same row, so one task keeps
/// them together and gives the quit path a single handle to await.
pub(crate) fn spawn_detached_session_state_persist(
    session_id: String,
    event_ordinal: u64,
    draft: String,
    updates: UnboundedSender<DashboardIoUpdate>,
) -> JoinHandle<()> {
    let persisted_session_id = session_id.clone();
    spawn_io(
        updates,
        move || {
            let receipt = hel::hel_database::advance_viewed_through_event_ordinal(
                &persisted_session_id,
                event_ordinal,
            )
            .map(|_| ());
            // Save the draft even when the receipt was rejected: losing typed
            // text is worse than an out-of-date read marker.
            let saved_draft =
                hel::hel_database::set_session_draft_input(&persisted_session_id, &draft);
            receipt.and(saved_draft)
        },
        move |result| DashboardIoUpdate::DetachedSessionState { session_id, result },
    )
}

pub(crate) fn spawn_read_receipt_persist(
    session_id: String,
    through: u64,
    updates: UnboundedSender<DashboardIoUpdate>,
) {
    let persisted_session_id = session_id.clone();
    spawn_io(
        updates,
        move || {
            hel::hel_database::advance_viewed_through_event_ordinal(&persisted_session_id, through)
        },
        move |result| DashboardIoUpdate::ReadReceipt { session_id, result },
    );
}

pub(crate) fn spawn_create_bundle(source: String, updates: UnboundedSender<DashboardIoUpdate>) {
    spawn_io(
        updates,
        move || {
            // Load fresh so a concurrent background save (e.g. an import
            // apply) is not clobbered by a stale UI-time config snapshot.
            let mut config = Controller::load()?.config;
            let bundle_id = create_quick_bundle(&mut config, &source)?;
            config.save()?;
            Ok(CreatedBundleUpdate { config, bundle_id })
        },
        |result| DashboardIoUpdate::CreatedBundle {
            result: Box::new(result),
        },
    );
}

pub(crate) fn spawn_imported_session_apply(
    mut imported: DashboardImportSuccess,
    pending: PendingDashboardImport,
    updates: UnboundedSender<DashboardIoUpdate>,
) {
    spawn_io(
        updates,
        move || {
            let session = imported
                .controller
                .state
                .sessions
                .remove(&imported.session_id)
                .context("import worker did not return its new session")?;
            let bundle = imported
                .controller
                .config
                .bundles
                .get(&session.bundle_id)
                .cloned()
                .context("import worker did not return its session bundle")?;
            let mut config = Controller::load()?.config;
            config
                .bundles
                .insert(session.bundle_id.clone(), bundle.clone());
            config.save()?;
            persist_imported_session(&session)?;
            Ok(ImportedDashboardSessionApply {
                harness: imported.harness,
                native_session_id: pending.native_session_id,
                bundle_id: session.bundle_id.clone(),
                bundle,
                session,
            })
        },
        |result| DashboardIoUpdate::ImportedSessionApplied {
            result: Box::new(result),
        },
    );
}

pub(crate) fn checkpoint_archive_targets(controller: &Controller) -> BTreeMap<String, PathBuf> {
    controller
        .state
        .sessions
        .values()
        .filter(|session| session.state == SessionState::Stopped)
        .filter_map(|session| {
            session
                .checkpoint
                .as_ref()
                .map(|checkpoint| (session.id.clone(), checkpoint.archive_path.clone()))
        })
        .collect()
}

pub(crate) fn spawn_checkpoint_archive_size_refresh(
    generation: u64,
    targets: BTreeMap<String, PathBuf>,
    updates: UnboundedSender<DashboardIoUpdate>,
) {
    tokio::task::spawn_blocking(move || {
        let sizes = targets
            .into_iter()
            .map(|(session_id, path)| {
                let size = std::fs::metadata(path).ok().map(|metadata| metadata.len());
                (session_id, size)
            })
            .collect();
        let _ = updates.send(DashboardIoUpdate::CheckpointArchiveSizes { generation, sizes });
    });
}

/// Registering a session and provisioning it are one job with two answers: the
/// dashboard shows the session as soon as it exists, then follows the launch,
/// so this stays separate from [`spawn_lifecycle_operation`].
pub(crate) fn spawn_dashboard_create_session(
    action: DashboardAction,
    updates: UnboundedSender<DashboardIoUpdate>,
    lifecycle_updates: UnboundedSender<LifecycleUpdate>,
    runtime: tokio::runtime::Handle,
) {
    tokio::task::spawn_blocking(move || {
        let DashboardAction::CreateSession {
            profile_id,
            bundle_id,
            project_directory,
            target_template_id,
            additional_mounts,
            allow_dirty_local,
            resource_allocation,
        } = action.clone()
        else {
            return;
        };
        let registered = (|| -> Result<Option<RegisteredDashboardSession>> {
            let mut controller = Controller::load()?;
            if !allow_dirty_local && project_directory.is_none() {
                let dirty = controller
                    .config
                    .bundles
                    .get(&bundle_id)
                    .with_context(|| format!("unknown bundle {bundle_id:?}"))
                    .and_then(hel::hel_local_git::dirty_local_repositories)?;
                if !dirty.is_empty() {
                    let repositories = dirty
                        .into_iter()
                        .map(|repository| {
                            format!("{}: {}", repository.path.display(), repository.summary)
                        })
                        .collect();
                    let _ = updates.send(DashboardIoUpdate::CreateSession(Box::new(
                        DashboardCreateSessionUpdate::DirtyLocal {
                            action,
                            repositories,
                        },
                    )));
                    return Ok(None);
                }
            }
            let title = format!(
                "{} via {profile_id}",
                project_directory
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| bundle_id.clone())
            );
            let session_id = controller.register_session_with_resources(
                &profile_id,
                &bundle_id,
                &target_template_id,
                title,
                SessionLaunchOptions {
                    additional_mounts,
                    allow_dirty_local,
                    resource_allocation,
                    project_directory,
                    session_title_override: None,
                },
            )?;
            let session = controller
                .state
                .sessions
                .get(&session_id)
                .expect("newly registered session exists")
                .clone();
            let cancelled = Arc::new(AtomicBool::new(false));
            Ok(Some(RegisteredDashboardSession { session, cancelled }))
        })();
        let Some(registered) = (match registered {
            Ok(registered) => registered,
            Err(error) => {
                let _ = updates.send(DashboardIoUpdate::CreateSession(Box::new(
                    DashboardCreateSessionUpdate::Failed(format!("{error:#}")),
                )));
                None
            }
        }) else {
            return;
        };
        let session_id = registered.session.id.clone();
        let cancelled = registered.cancelled.clone();
        if updates
            .send(DashboardIoUpdate::CreateSession(Box::new(
                DashboardCreateSessionUpdate::Registered(Box::new(registered)),
            )))
            .is_err()
        {
            cancelled.store(true, Ordering::Release);
        }
        let result = (|| -> Result<()> {
            let mut controller = Controller::load()?;
            let executor = StageReportingExecutor::new(
                CancellableProcessExecutor::new(cancelled),
                session_id.clone(),
                updates,
            );
            runtime.block_on(controller.provision_session_controlled(&session_id, &executor))
        })()
        .map(|()| LifecycleSuccess::Created)
        .map_err(|error| format!("{error:#}"));
        let _ = lifecycle_updates.send(LifecycleUpdate { session_id, result });
    });
}

/// Reports the launch stage of each command a lifecycle operation runs, so the
/// session clock can name the work in flight.
pub(crate) struct StageReportingExecutor<E: CommandExecutor> {
    inner: E,
    session_id: String,
    updates: UnboundedSender<DashboardIoUpdate>,
    reported: std::sync::Mutex<Option<ProvisionStage>>,
}

impl<E: CommandExecutor> StageReportingExecutor<E> {
    pub(crate) fn new(
        inner: E,
        session_id: String,
        updates: UnboundedSender<DashboardIoUpdate>,
    ) -> Self {
        Self {
            inner,
            session_id,
            updates,
            reported: std::sync::Mutex::new(None),
        }
    }

    fn report(&self, command: &CommandSpec) {
        let Some(stage) = command.stage else {
            return;
        };
        self.report_stage(stage);
    }

    /// The single place a stage reaches the dashboard, so command-driven and
    /// explicit reports dedupe against the same last-reported stage.
    fn report_stage(&self, stage: ProvisionStage) {
        let mut reported = self
            .reported
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if *reported == Some(stage) {
            return;
        }
        *reported = Some(stage);
        let _ = self.updates.send(DashboardIoUpdate::LifecycleStage {
            session_id: self.session_id.clone(),
            stage,
        });
    }
}

impl<E: CommandExecutor> CommandExecutor for StageReportingExecutor<E> {
    fn execute(&self, command: &CommandSpec) -> Result<CommandOutput> {
        self.report(command);
        self.inner.execute(command)
    }

    fn cancellation_requested(&self) -> bool {
        self.inner.cancellation_requested()
    }

    fn notify_stage(&self, stage: ProvisionStage) {
        self.report_stage(stage);
    }

    fn notify_notice(&self, notice: &str) {
        let _ = self.updates.send(DashboardIoUpdate::LifecycleNotice {
            notice: notice.to_owned(),
        });
    }

    fn execute_with_stdin(
        &self,
        command: &CommandSpec,
        input: &mut (dyn std::io::Read + Send),
    ) -> Result<CommandOutput> {
        self.report(command);
        self.inner.execute_with_stdin(command, input)
    }
}

impl DashboardContext {
    /// Folds one finished background job into dashboard and controller state.
    pub(super) fn apply_dashboard_io_update(&mut self, update: DashboardIoUpdate) {
        match update {
            DashboardIoUpdate::WorkerRecordPersistence { operation, result } => {
                if let Err(error) = result {
                    match operation {
                        WorkerRecordPersistence::AcpTitle { .. } => self
                            .dashboard
                            .set_notice(format!("Could not save harness title: {error}")),
                    }
                }
            }
            DashboardIoUpdate::LifecycleStage { session_id, stage } => {
                self.dashboard
                    .set_session_operation_stage(&session_id, stage);
            }
            DashboardIoUpdate::LifecycleNotice { notice } => self.dashboard.set_notice(notice),
            DashboardIoUpdate::HiddenNativeSessions { result } => match result {
                Ok(hidden) => self.dashboard.set_hidden_native_sessions(hidden),
                Err(error) => self
                    .dashboard
                    .set_notice(format!("Could not read archived sessions: {error}")),
            },
            DashboardIoUpdate::ArchiveWrite {
                what,
                target,
                result,
            } => {
                if let Err(error) = result {
                    self.dashboard
                        .set_notice(format!("Could not archive {what}: {error}"));
                    // The optimistic update no longer matches storage, so the
                    // row it moved goes back where storage still has it.
                    if revert_archive_write(
                        &target,
                        &mut self.controller.state,
                        &mut self.dashboard,
                    ) {
                        spawn_hidden_native_sessions_load(self.dashboard_io_tx.clone());
                    }
                    self.dirty = true;
                }
            }
            DashboardIoUpdate::MaterializedSessionProjection { session_id, result } => {
                self.finish_materialized_projection(session_id, result);
            }
            DashboardIoUpdate::ProjectSource { session_id, result } => {
                self.project_sources_in_flight.remove(&session_id);
                match result {
                    Ok(source) => self.dashboard.set_project_source(&session_id, source),
                    Err(error) => tracing::warn!(
                        %session_id,
                        "could not resolve canonical project source: {error}"
                    ),
                }
            }
            DashboardIoUpdate::CreateSession(update) => self.apply_create_session_update(*update),
            DashboardIoUpdate::RenameSession {
                session_id,
                title,
                result,
            } => match result {
                Ok(title) => {
                    if let Some(session) = self.controller.state.sessions.get_mut(&session_id) {
                        session.session_title_override = Some(title.clone());
                        session.updated_at = chrono::Utc::now().to_rfc3339();
                    }
                    self.dashboard.set_state(self.controller.state.clone());
                    self.dashboard
                        .set_notice(format!("Renamed session to {title}"));
                }
                Err(error) => {
                    self.dashboard
                        .set_notice(format!("Rename failed for {title}: {error}"));
                }
            },
            DashboardIoUpdate::ContainerSettings { session_id, result } => match result {
                Ok(()) => {
                    // Reload so the saved mounts, overrides, and mount history
                    // all come back from one durable read.
                    match Controller::load() {
                        Ok(controller) => {
                            self.controller = controller;
                            self.dashboard.set_state(self.controller.state.clone());
                            self.dashboard.set_notice(format!(
                                "Container settings saved for {}; applies when it is next recreated.",
                                short_id(&session_id)
                            ));
                        }
                        Err(error) => self.dashboard.set_notice(format!(
                            "Container settings saved for {}, but reloading state failed: {error:#}",
                            short_id(&session_id)
                        )),
                    }
                }
                Err(error) => self.dashboard.set_notice(format!(
                    "Container settings failed for {}: {error}",
                    short_id(&session_id)
                )),
            },
            DashboardIoUpdate::DetachedSessionState { session_id, result } => {
                if let Err(error) = result {
                    self.dashboard.set_notice(format!(
                        "Could not save draft and read status for {}: {error}",
                        short_id(&session_id)
                    ));
                }
            }
            DashboardIoUpdate::ReadReceipt { session_id, result } => {
                self.finish_read_receipt(session_id, result);
            }
            DashboardIoUpdate::CreatedBundle { result } => match *result {
                Ok(created) => {
                    self.controller.config = created.config;
                    let followup = self
                        .dashboard
                        .apply_created_bundle(self.controller.config.clone(), &created.bundle_id);
                    if let DashboardAction::ResolveAwsResourceOptions {
                        target_template_ids,
                    } = followup
                    {
                        self.resolve_aws_resource_options(target_template_ids);
                    }
                }
                Err(error) => {
                    self.dashboard
                        .set_notice(format!("Could not create bundle: {error}"));
                }
            },
            DashboardIoUpdate::ImportedSessionApplied { result } => match *result {
                Ok(applied) => {
                    self.controller
                        .config
                        .bundles
                        .insert(applied.bundle_id, applied.bundle);
                    self.controller
                        .state
                        .sessions
                        .insert(applied.session.id.clone(), applied.session);
                    self.dashboard.set_config(self.controller.config.clone());
                    self.dashboard.set_state(self.controller.state.clone());
                    self.resolve_project_sources();
                    self.refresh_poll_targets();
                    self.dashboard.set_notice(format!(
                        "Imported {} session {}.",
                        applied.harness, applied.native_session_id
                    ));
                }
                Err(error) => self.dashboard.set_notice(format!("Import failed: {error}")),
            },
            DashboardIoUpdate::LifecycleReloaded(reloaded) => {
                self.apply_lifecycle_reloaded(*reloaded)
            }
            DashboardIoUpdate::CheckpointArchiveSizes { generation, sizes } => {
                if generation == self.checkpoint_archive_generation {
                    self.dashboard.apply_checkpoint_archive_sizes(sizes);
                }
            }
            DashboardIoUpdate::WorkerDiagnosis {
                session_id,
                episode_id,
                result,
            } => self.apply_worker_diagnosis(session_id, episode_id, result),
            DashboardIoUpdate::MountCompletions { prefix, result } => match result {
                Ok(candidates) => self
                    .dashboard
                    .apply_mount_source_completions(&prefix, candidates),
                Err(error) => self
                    .dashboard
                    .set_notice(format!("Path completion failed: {error}")),
            },
            DashboardIoUpdate::MountValidation { source, result } => self
                .dashboard
                .apply_mount_source_validation(&source, result),
            DashboardIoUpdate::SessionMountValidation { launch, result } => match result {
                Ok(None) => {
                    self.dashboard.finish_session_mount_preflight();
                    match *launch {
                        DashboardAction::PreflightResumeRepositories { launch } => {
                            if let Err(error) =
                                super::actions::start_resume_repository_preflight(self, launch)
                            {
                                self.dashboard.set_notice(format!(
                                    "Could not check checkpoint repositories: {error:#}"
                                ));
                            }
                        }
                        launch => super::actions::start_session_launch(self, launch),
                    }
                }
                Ok(Some((source, error))) => {
                    self.dashboard
                        .apply_session_mount_preflight_failure(&source, error);
                }
                Err(error) => self
                    .dashboard
                    .set_notice(format!("Could not check attached directories: {error}")),
            },
            DashboardIoUpdate::ResumeRepositoryPreflight {
                launch,
                submitted_repository_id,
                result,
            } => match *result {
                Ok(applied) => {
                    if let Some(config) = applied.config {
                        self.controller.config = config.clone();
                        self.dashboard.set_config(config);
                    }
                    match applied.preflight {
                        ResumeRepositorySourcePreflight::Ready(receipt) => {
                            self.dashboard.finish_resume_repository_preflight();
                            super::actions::start_preflighted_session_launch(
                                self, *launch, receipt,
                            );
                        }
                        ResumeRepositorySourcePreflight::RepositoryMoved(mismatch) => {
                            if submitted_repository_id.as_deref()
                                == Some(mismatch.repository_id.as_str())
                            {
                                self.dashboard.apply_repository_origin_failure(
                                    &mismatch.repository_id,
                                    format!(
                                        "That origin does not contain checkpoint base {}.",
                                        mismatch.missing_commit
                                    ),
                                );
                            } else {
                                self.dashboard.show_repository_origin_dialog(
                                    mismatch.session_id,
                                    mismatch.repository_id,
                                    mismatch.missing_commit,
                                    mismatch.archived_origin,
                                    mismatch.configured_origin,
                                    *launch,
                                );
                            }
                        }
                    }
                }
                Err(error) => {
                    if let Some(repository_id) = submitted_repository_id {
                        self.dashboard
                            .apply_repository_origin_failure(&repository_id, error);
                    } else {
                        self.dashboard.set_notice(format!(
                            "Could not check checkpoint repositories: {error}"
                        ));
                    }
                }
            },
            DashboardIoUpdate::ProjectValidation { directory, result } => self
                .dashboard
                .apply_project_directory_validation(&directory, result),
        }
    }

    fn apply_create_session_update(&mut self, update: DashboardCreateSessionUpdate) {
        match update {
            DashboardCreateSessionUpdate::DirtyLocal {
                action,
                repositories,
            } => self
                .dashboard
                .show_dirty_local_confirmation(action, repositories),
            DashboardCreateSessionUpdate::Registered(registered) => {
                let registered = *registered;
                let session_id = registered.session.id.clone();
                self.controller
                    .state
                    .sessions
                    .insert(session_id.clone(), registered.session);
                self.dashboard.set_state(self.controller.state.clone());
                self.resolve_project_sources();
                self.dashboard.begin_session_operation(
                    session_id.clone(),
                    SessionOperationKind::Launching,
                    None,
                );
                self.dashboard
                    .set_notice(format!("Launching {}…", short_id(&session_id)));
                self.lifecycle_operations.insert(
                    session_id,
                    ActiveLifecycleOperation {
                        cancelled: registered.cancelled,
                        kind: SessionOperationKind::Launching,
                    },
                );
            }
            DashboardCreateSessionUpdate::Failed(error) => {
                self.dashboard
                    .set_notice(format!("Could not create session: {error}"));
            }
        }
    }

    fn apply_lifecycle_reloaded(&mut self, reloaded: LifecycleReloaded) {
        let LifecycleReload { update, operation } = reloaded.reload;
        let session_id = update.session_id;
        let loaded = match reloaded.result {
            Ok(loaded) => loaded,
            Err(error) => {
                self.dashboard
                    .set_notice(format!("Could not reload completed operation: {error}"));
                return;
            }
        };
        self.controller = loaded;
        self.dashboard.set_state(self.controller.state.clone());
        self.resolve_project_sources();
        if update.result.is_ok() {
            self.drop_warm_chat_for(&session_id);
        }
        match update.result {
            Ok(LifecycleSuccess::Created) => {
                self.dashboard.select_active_session(&session_id);
                self.dashboard.set_notice(format!(
                    "Session {} is ready; press Enter to open it",
                    short_id(&session_id)
                ));
                self.request_quota_refresh();
            }
            Ok(LifecycleSuccess::Resumed {
                profile_id,
                target_id,
                materialized,
            }) => {
                let viewed_through_event_ordinal = self
                    .controller
                    .state
                    .sessions
                    .get(&session_id)
                    .map_or(0, |session| session.viewed_through_event_ordinal);
                self.request_materialized_projection(*materialized, viewed_through_event_ordinal);
                self.dashboard.select_active_session(&session_id);
                self.dashboard.set_notice(format!(
                    "Resumed {} with {profile_id} on {target_id}",
                    short_id(&session_id)
                ));
                self.request_quota_refresh();
            }
            Ok(LifecycleSuccess::Closed) => {
                self.dashboard
                    .set_notice(format!("Stopped {}", short_id(&session_id)));
            }
            Ok(LifecycleSuccess::Destroyed) => self.dashboard.set_notice(format!(
                "Destroyed {} without an archive",
                short_id(&session_id)
            )),
            Ok(LifecycleSuccess::DeletedActive) => self.dashboard.set_notice(format!(
                "Deleted active session {} without checkpointing",
                short_id(&session_id)
            )),
            Ok(LifecycleSuccess::DeletedStopped) => self.dashboard.set_notice(format!(
                "Permanently deleted stopped session {}",
                short_id(&session_id)
            )),
            Err(error) => {
                if operation
                    .as_ref()
                    .is_some_and(|operation| operation.kind == SessionOperationKind::Stopping)
                {
                    self.dashboard.show_close_failure(session_id.clone(), error);
                } else {
                    let label = operation
                        .as_ref()
                        .map_or("Operation", |operation| operation.kind.label());
                    self.dashboard
                        .set_notice(format!("{label} failed: {error}"));
                }
            }
        }
        self.refresh_poll_targets();
    }

    fn apply_worker_diagnosis(
        &mut self,
        session_id: String,
        episode_id: u64,
        result: std::result::Result<Option<String>, String>,
    ) {
        let completion = self.worker_diagnoses.finish(&session_id, episode_id);
        if let Some(error) = completion.display_error {
            let mut message = format!("relay unreachable: {error}");
            match &result {
                Ok(Some(diagnosis)) => {
                    message.push_str("; ");
                    message.push_str(diagnosis);
                }
                Ok(None) => {}
                Err(failure) => {
                    message.push_str("; worker diagnostics failed: ");
                    message.push_str(failure);
                }
            }
            self.dashboard
                .set_notice(format!("Session {}: {message}", short_id(&session_id)));
        } else if let Err(error) = &result {
            tracing::warn!(%session_id, "stale worker diagnosis task failed: {error}");
        }
        if let Some(restart_episode) = completion.restart_episode {
            crate::pollers::spawn_worker_diagnosis(
                &self.controller,
                session_id,
                restart_episode,
                self.dashboard_io_tx.clone(),
            );
        }
    }
}

fn create_quick_bundle(config: &mut HelConfig, source: &str) -> Result<String> {
    let source = source.trim();
    if source.is_empty() {
        bail!("repository source cannot be empty");
    }
    let candidate = Path::new(source);
    let (name, github, local) = if candidate.exists() {
        let root = hel::hel_local_git::canonical_repository(candidate)?;
        if let Some(existing) = configured_bundle_for_local(config, &root) {
            return Ok(existing);
        }
        let name = root
            .file_name()
            .and_then(|name| name.to_str())
            .context("local repository has no usable directory name")?
            .to_owned();
        (name, None, Some(root))
    } else {
        if candidate.is_absolute() || source.starts_with('.') || source.starts_with('~') {
            bail!("local repository path {source:?} does not exist");
        }
        let repository = github_repository_from_origin(source)
            .with_context(|| format!("{source:?} is not a GitHub owner/repository or URL"))?;
        if let Some(existing) = configured_bundle_for_origin(config, &repository) {
            return Ok(existing);
        }
        let name = repository.repository.clone();
        let github = format!("{}/{}", repository.owner, repository.repository);
        (name, Some(github), None)
    };
    let repository_id = quick_config_id(&name);
    let mut bundle_id = repository_id.clone();
    for suffix in 2_u32.. {
        if !config.bundles.contains_key(&bundle_id) {
            break;
        }
        bundle_id = format!("{repository_id}-{suffix}");
    }
    config.bundles.insert(
        bundle_id.clone(),
        ProjectBundle {
            primary_repo: repository_id.clone(),
            repositories: vec![ProjectRepository {
                id: repository_id.clone(),
                github,
                local,
                destination: PathBuf::from(repository_id),
                git_ref: None,
            }],
        },
    );
    config.validate()?;
    Ok(bundle_id)
}

fn quick_config_id(value: &str) -> String {
    let id = value
        .chars()
        .filter(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.')
        })
        .take(64)
        .collect::<String>();
    if id.is_empty() || matches!(id.as_str(), "." | "..") {
        "repository".into()
    } else {
        id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Compaction is not a command execution, so the resume path names its
    /// stage explicitly. The explicit report must reach the dashboard and
    /// dedupe against the same last-reported stage a staged command sets.
    #[test]
    fn notify_stage_reports_a_lifecycle_stage_and_dedupes_a_repeat() {
        struct UnusedExecutor;

        impl CommandExecutor for UnusedExecutor {
            fn execute(&self, command: &CommandSpec) -> Result<CommandOutput> {
                panic!("a stage notification must not run {}", command.program)
            }
        }

        let (updates, mut reported) = tokio::sync::mpsc::unbounded_channel();
        let executor = StageReportingExecutor::new(UnusedExecutor, "session-1".to_owned(), updates);

        executor.notify_stage(ProvisionStage::Compacting);
        executor.notify_stage(ProvisionStage::Compacting);
        executor.notify_stage(ProvisionStage::Starting);
        drop(executor);

        let stages = std::iter::from_fn(|| reported.try_recv().ok())
            .map(|update| match update {
                DashboardIoUpdate::LifecycleStage { session_id, stage } => (session_id, stage),
                _ => panic!("a stage notification must publish a lifecycle stage"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            stages,
            vec![
                ("session-1".to_owned(), ProvisionStage::Compacting),
                ("session-1".to_owned(), ProvisionStage::Starting),
            ]
        );
    }

    #[test]
    fn quick_github_bundle_uses_collision_suffix_and_reuses_matching_source() {
        let mut config = HelConfig::default();
        config.bundles.insert(
            "app".into(),
            ProjectBundle {
                primary_repo: "app".into(),
                repositories: vec![ProjectRepository {
                    id: "app".into(),
                    github: Some("other/app".into()),
                    local: None,
                    destination: "app".into(),
                    git_ref: None,
                }],
            },
        );

        let created =
            create_quick_bundle(&mut config, "https://github.com/example/app.git").unwrap();
        assert_eq!(created, "app-2");
        assert_eq!(
            create_quick_bundle(&mut config, "example/app").unwrap(),
            "app-2"
        );
        assert_eq!(config.bundles.len(), 2);
    }

    fn archivable_session(id: &str) -> SessionRecord {
        SessionRecord {
            archived: false,
            container_cpus: None,
            container_memory: None,
            id: id.into(),
            title: "Raise the dead".into(),
            harness_kind: HarnessKind::Codex,
            last_profile: "codex-1".into(),
            bundle_id: "hel".into(),
            project_directory: None,
            managed_worktree: None,
            target_template_id: "podman".into(),
            resource_allocation: None,
            additional_mounts: Vec::new(),
            state: SessionState::Stopped,
            target: None,
            native_session_id: None,
            acp_session_title: None,
            session_title_override: Some("Raise the dead".into()),
            created_at: "2026-08-14T00:00:00Z".into(),
            updated_at: "2026-08-14T00:00:00Z".into(),
            viewed_through_event_ordinal: 0,
            draft_input: String::new(),
            last_error: None,
            last_checkpoint_error: None,
            checkpoint: None,
        }
    }

    /// An archive write that never reached the database must not leave the
    /// dashboard and the controller holding a row the database still shows.
    /// Both in-memory copies go back, and the row returns to the dialog.
    #[test]
    fn a_failed_session_archive_write_puts_both_copies_of_the_record_back() {
        let mut state = HelState::default();
        state
            .sessions
            .insert("session-1".into(), archivable_session("session-1"));
        let mut dashboard =
            hel_tui::DashboardState::new(HelConfig::default(), state.clone(), BTreeMap::new());
        dashboard.show_resume_dialog(1, Vec::new());

        // What pressing `a` applies before the write is even scheduled.
        dashboard.set_session_archived("session-1", true);
        state
            .sessions
            .get_mut("session-1")
            .expect("the session")
            .archived = true;

        let reload_hidden_native = revert_archive_write(
            &ArchiveWriteTarget::Session {
                session_id: "session-1".into(),
                archived: false,
            },
            &mut state,
            &mut dashboard,
        );

        assert!(
            !reload_hidden_native,
            "a hel record is restored from what the write knew, not from the native set"
        );
        assert!(!state.sessions["session-1"].archived);
        // The row is listed again, so archiving it asks for the same write the
        // failed one attempted rather than an unarchive.
        assert_eq!(
            dashboard.handle_key(crossterm::event::KeyEvent::new(
                crossterm::event::KeyCode::Char('a'),
                crossterm::event::KeyModifiers::NONE,
            )),
            DashboardAction::SetSessionArchived {
                session_id: "session-1".into(),
                archived: true,
            }
        );
    }

    /// The native hidden set has no per-row memory to restore, so a failed
    /// hide is repaired by re-reading what the database holds.
    #[test]
    fn a_failed_native_hide_write_asks_for_the_stored_set() {
        let mut state = HelState::default();
        let mut dashboard =
            hel_tui::DashboardState::new(HelConfig::default(), state.clone(), BTreeMap::new());
        assert!(revert_archive_write(
            &ArchiveWriteTarget::HiddenNativeSessions,
            &mut state,
            &mut dashboard,
        ));
    }
}
