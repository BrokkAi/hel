//! The interactive session dashboard.
//!
//! One loop owns the terminal. It waits on every background feed at once, then
//! batches whatever queued behind the message that woke it, so a burst of
//! updates costs one draw. Nothing in this loop blocks: filesystem, database,
//! process, and network work all run as the tasks in [`io`] and
//! [`crate::pollers`], and answer over channels.
//!
//! [`DashboardContext`] holds everything the loop owns, which is what lets the
//! wait, the drains, and the action handling in [`actions`] be separate
//! functions over the same state.

pub(crate) mod actions;
pub(crate) mod io;

use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use crossterm::event::{self, Event};
use hel::hel_config::{HelConfig, config_path};
use hel::hel_controller::Controller;
use hel::hel_credentials::CredentialSyncHandle;
use hel::hel_recovery::RecoveryCoordinator;
use hel::hel_session_manager::{SessionManagerControl, SessionManagerUpdates, ViewError};
use hel::hel_setup::{SetupOutcome, run_setup_dialog};
use hel::hel_state::{RecoveryObserver, SessionResourceAllocation, SessionState};
use hel::hel_targets::DeploymentCapacityTarget;
use hel::hel_worker_client::CredentialSyncCoordinator;
use hel_tui::{
    DashboardAction, DashboardState, ImportProfileOption, SessionOperationKind, render,
    resume_profile_placeholders,
};
use tokio::sync::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tokio::sync::watch;
use tokio_stream::StreamExt as _;

use crate::dashboard::io::{
    ActiveLifecycleOperation, DashboardIoUpdate, LifecycleReload, checkpoint_archive_targets,
    spawn_checkpoint_archive_size_refresh, spawn_lifecycle_reload,
    spawn_materialized_session_projection,
};
use crate::import::{
    DashboardImportTaskResult, DashboardImportUpdate, PendingDashboardImport,
    spawn_dashboard_import,
};
use crate::pollers::{
    AuthFailureSyncTracker, CapacityPollUpdate, CredentialSyncNotices, Feed, LifecycleUpdate,
    QuotaRefreshBatch, QuotaUpdate, ResourcePollTarget, ResourcePollUpdate, WorkerDiagnosisTracker,
    WorkerPollTarget, apply_worker_poll_update, complete_manual_quota_refresh,
    dashboard_worker_targets, interrupted_close_session_ids, merge_recovery_result,
    projected_queued_prompts, quota_refresh_profiles, refresh_dashboard_poll_targets,
    schedule_due_auth_failure_syncs, spawn_dashboard_capacity_poller,
    spawn_dashboard_quota_refresher, spawn_dashboard_resource_poller,
    spawn_dashboard_worker_poller, spawn_interrupted_close_recovery, spawn_worker_diagnosis,
};
use crate::{TerminalGuard, short_id, startup_greeting};

/// Redraw cadence for displays that move with the wall clock: turn timers,
/// countdowns, and elapsed times.
const DASHBOARD_CLOCK_TICK: Duration = Duration::from_secs(1);
/// Redraw cadence while a dialog animates on its own.
const IMPORT_PROGRESS_TICK: Duration = Duration::from_millis(125);
pub(crate) const QUOTA_REFRESH_NOTICE: &str = "Refreshing profile quotas…";
pub(crate) const QUOTA_REFRESHED_NOTICE: &str = "Profile quotas refreshed.";

/// Which view the one loop is drawing. The chat view is data rather than a
/// nested loop, so its background feeds stay live while the session list is on
/// screen and returning to a session is only a redraw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum View {
    Dashboard,
    /// Only valid while the loop holds an `ActiveChat`.
    Chat,
}

pub(crate) struct ActiveDashboardImport {
    task_id: u64,
    pub(crate) cancelled: Arc<AtomicBool>,
}

/// Everything one dashboard run owns.
pub(crate) struct DashboardContext {
    terminal: TerminalGuard,
    pub(crate) controller: Controller,
    pub(crate) dashboard: DashboardState,
    /// One notifications bar for the whole process: the dashboard and every
    /// chat view opened from it report through this shared handle.
    notices: hel::hel_chat::Notices,
    /// Absent only while the setup dialog owns the terminal; see
    /// [`DashboardContext::run_setup_dialog`].
    events: Option<event::EventStream>,
    pub(crate) view: View,
    /// At most one chat stays warm: the session opened last. Its feeds keep
    /// running off screen, so reopening it costs a draw rather than a rebuild.
    pub(crate) active_chat: Option<hel::hel_chat::ActiveChat>,
    /// The first pass always draws; after that a redraw needs a wakeup.
    pub(crate) dirty: bool,
    /// The poll targets are recomputed only after the controller may have
    /// changed.
    pub(crate) controller_changed: bool,
    pub(crate) quit_detached: bool,

    quota_profiles_tx: watch::Sender<QuotaRefreshBatch>,
    quota: Feed<Receiver<QuotaUpdate>>,
    pub(crate) manual_quota_refresh_generation: Option<u64>,

    worker_targets_tx: watch::Sender<Vec<WorkerPollTarget>>,
    worker: Feed<SessionManagerUpdates>,
    pub(crate) worker_commands_tx: SessionManagerControl,
    worker_diagnoses: WorkerDiagnosisTracker,

    recovery: Feed<RecoveryCoordinator>,
    pub(crate) recovery_observer: RecoveryObserver,

    pub(crate) lifecycle_updates_tx: UnboundedSender<LifecycleUpdate>,
    lifecycle: Feed<UnboundedReceiver<LifecycleUpdate>>,
    pub(crate) lifecycle_operations: BTreeMap<String, ActiveLifecycleOperation>,

    credential_sync: Feed<CredentialSyncCoordinator>,
    credential_sync_handle: CredentialSyncHandle,
    auth_failure_syncs: AuthFailureSyncTracker,
    credential_sync_notices: CredentialSyncNotices,

    resource_targets_tx: watch::Sender<Vec<ResourcePollTarget>>,
    resource_triggers_tx: Sender<String>,
    resource: Feed<Receiver<ResourcePollUpdate>>,

    capacity_targets_tx: watch::Sender<Vec<DeploymentCapacityTarget>>,
    capacity: Feed<Receiver<CapacityPollUpdate>>,

    pub(crate) aws_resource_options_tx: AwsResourceOptionsSender,
    aws_options: Feed<UnboundedReceiver<AwsResourceOptions>>,
    pub(crate) resolving_aws_resource_options: BTreeSet<String>,

    import_updates_tx: Sender<(u64, ImportProfileOption)>,
    import_profiles: Feed<Receiver<(u64, ImportProfileOption)>>,
    pub(crate) import_task_tx: Sender<DashboardImportUpdate>,
    import_tasks: Feed<Receiver<DashboardImportUpdate>>,
    pub(crate) pending_import: Option<PendingDashboardImport>,
    pub(crate) import_discovery_id: u64,
    pub(crate) next_import_task_id: u64,
    pub(crate) active_import: Option<ActiveDashboardImport>,

    pub(crate) dashboard_io_tx: UnboundedSender<DashboardIoUpdate>,
    dashboard_io: Feed<UnboundedReceiver<DashboardIoUpdate>>,

    checkpoint_archive_targets_seen: BTreeMap<String, std::path::PathBuf>,
    checkpoint_archive_generation: u64,
}

/// One deployment target's resolved instance sizes, or why they could not be
/// resolved.
type AwsResourceOptions = (
    String,
    std::result::Result<Vec<SessionResourceAllocation>, String>,
);
type AwsResourceOptionsSender = UnboundedSender<AwsResourceOptions>;

pub(crate) async fn run_dashboard() -> Result<()> {
    if !std::io::IsTerminal::is_terminal(&std::io::stdin())
        || !std::io::IsTerminal::is_terminal(&std::io::stdout())
    {
        println!("Welcome to Hel");
        println!("Run `hel doctor` for non-interactive validation.");
        return Ok(());
    }

    let Some(mut context) = DashboardContext::open()? else {
        return Ok(());
    };
    let termination = hel::termination::Coordinator::install().token();
    // `interval_at` so the first tick is a period away rather than immediate,
    // and `Delay` so a tick that was gated off does not fire a burst to catch
    // up when it comes back.
    let mut clock_tick = tokio::time::interval_at(
        tokio::time::Instant::now() + DASHBOARD_CLOCK_TICK,
        DASHBOARD_CLOCK_TICK,
    );
    clock_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut import_tick = tokio::time::interval_at(
        tokio::time::Instant::now() + IMPORT_PROGRESS_TICK,
        IMPORT_PROGRESS_TICK,
    );
    import_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        context.refresh_controller_derived_state();
        context.draw()?;
        let mut action = DashboardAction::None;
        let mut chat_outcome = hel::hel_chat::ChatEventOutcome::None;
        // The winning arm takes the message that woke the loop; the drains
        // below batch whatever is queued behind it, so one wakeup is one draw.
        tokio::select! {
            _ = termination.cancelled() => break,
            event = next_terminal_event(&mut context.events) => {
                let Some(event) = event else { break };
                let mut event = event?;
                // Key repeats and pastes arrive as several ready events. The
                // buffered ones are handled before drawing, but the first event
                // that asks for work ends the batch so that dispatch still
                // follows input order.
                loop {
                    let batched = match (context.view, context.active_chat.as_mut()) {
                        (View::Chat, Some(chat)) => {
                            chat_outcome = chat.handle_event(event);
                            matches!(chat_outcome, hel::hel_chat::ChatEventOutcome::None)
                        }
                        _ => {
                            action = dashboard_event_action(&mut context.dashboard, event);
                            context.controller_changed = true;
                            matches!(action, DashboardAction::None)
                        }
                    };
                    if !batched {
                        break;
                    }
                    // A zero timeout rather than a no-op waker: `EventStream`
                    // arms its reader thread with the waker it was last polled
                    // with, so a no-op waker here would swallow the next wakeup.
                    let Ok(Some(next)) = tokio::time::timeout(
                        Duration::ZERO,
                        next_terminal_event(&mut context.events),
                    )
                    .await
                    else {
                        break;
                    };
                    event = next?;
                }
                context.dirty = true;
            }
            // The warm chat's own feeds: remote command results, its clipboard
            // and history I/O, dictation, and the session view. They run
            // whether or not the chat is on screen, which is what keeps an
            // off-screen chat current.
            () = hel::hel_chat::ActiveChat::pump(context.active_chat.as_mut()) => {
                context.dirty |= context.view == View::Chat;
            }
            update = context.quota.wait(), if context.quota.is_open() => {
                let woke = context.quota.accept(update);
                context.dirty |= woke;
            }
            update = context.worker.wait(), if context.worker.is_open() => {
                let woke = context.worker.accept(update);
                context.dirty |= woke;
            }
            result = context.recovery.wait(), if context.recovery.is_open() => {
                let woke = context.recovery.accept(result);
                context.dirty |= woke;
            }
            result = context.credential_sync.wait(), if context.credential_sync.is_open() => {
                let woke = context.credential_sync.accept(result);
                context.dirty |= woke;
            }
            update = context.resource.wait(), if context.resource.is_open() => {
                let woke = context.resource.accept(update);
                context.dirty |= woke;
            }
            update = context.capacity.wait(), if context.capacity.is_open() => {
                let woke = context.capacity.accept(update);
                context.dirty |= woke;
            }
            options = context.aws_options.wait(), if context.aws_options.is_open() => {
                let woke = context.aws_options.accept(options);
                context.dirty |= woke;
            }
            profile = context.import_profiles.wait(), if context.import_profiles.is_open() => {
                let woke = context.import_profiles.accept(profile);
                context.dirty |= woke;
            }
            update = context.import_tasks.wait(), if context.import_tasks.is_open() => {
                let woke = context.import_tasks.accept(update);
                context.dirty |= woke;
            }
            update = context.lifecycle.wait(), if context.lifecycle.is_open() => {
                let woke = context.lifecycle.accept(update);
                context.dirty |= woke;
            }
            update = context.dashboard_io.wait(), if context.dashboard_io.is_open() => {
                let woke = context.dashboard_io.accept(update);
                context.dirty |= woke;
            }
            // Turn clocks, countdowns, and credential-sync backoffs move on
            // their own, so the dashboard redraws once a second regardless.
            // The chat redraws only when its own time-driven text has moved:
            // a running turn clock in the session header, or the checkpoint
            // title.
            _ = clock_tick.tick() => {
                context.dirty |= match (context.view, context.active_chat.as_ref()) {
                    (View::Chat, Some(chat)) => chat.needs_clock_tick(),
                    _ => true,
                };
            }
            // The import progress dialog reports how long a step has stalled
            // and the resume dialog spins while it scans; both need a faster
            // tick, and only while they are on screen.
            _ = import_tick.tick(),
                if context.dashboard.needs_fast_tick() && context.view == View::Dashboard =>
            {
                context.dirty = true;
            }
        }
        context.drain_feeds();
        if context.apply_chat_outcome(chat_outcome).await.is_break() {
            break;
        }
        if actions::apply_dashboard_action(&mut context, action)
            .await?
            .is_break()
        {
            break;
        }
    }
    context.cancel_background_work();
    let quit_detached = context.quit_detached;
    // Hand the terminal back before saying anything on it; the warm chat and
    // the background feeds are torn down after, as the rest of the context
    // drops.
    drop(context.terminal);
    if quit_detached {
        println!(
            "Active sessions will continue working; Hel will reattach to them on your next invocation."
        );
    }
    Ok(())
}

impl DashboardContext {
    /// Loads state, takes the terminal, and starts every background feed.
    /// `Ok(None)` means first-run setup was cancelled and there is nothing to
    /// run.
    fn open() -> Result<Option<Self>> {
        let mut controller = Controller::load()?;
        let greeting = startup_greeting(&controller);
        let mut dashboard = DashboardState::new(
            controller.config.clone(),
            controller.state.clone(),
            BTreeMap::new(),
        );
        let notices = hel::hel_chat::Notices::default();
        dashboard.share_notices(notices.clone());
        for (session_id, queued) in projected_queued_prompts(&controller)? {
            dashboard.apply_queued_prompts(&session_id, queued);
        }
        dashboard.set_greeting(greeting);
        let mut terminal = TerminalGuard::enter()?;
        if configuration_needs_setup(&controller.config) {
            terminal.suspend()?;
            let setup_result = run_setup_dialog(&config_path());
            terminal.resume()?;
            match setup_result? {
                SetupOutcome::Written => {
                    controller.reload()?;
                    dashboard.set_config(controller.config.clone());
                    dashboard.set_state(controller.state.clone());
                    dashboard
                        .set_notice("Setup complete. Press Ctrl+N to start your first session.");
                }
                SetupOutcome::Cancelled => return Ok(None),
            }
        }

        let (quota_profiles_tx, quota_updates_rx) = spawn_dashboard_quota_refresher();
        let (worker_targets_tx, worker_updates_rx, worker_commands_tx) =
            spawn_dashboard_worker_poller()?;
        worker_targets_tx.send_replace(dashboard_worker_targets(&controller));
        let recovery = RecoveryCoordinator::spawn(worker_commands_tx.clone());
        let recovery_observer = recovery.observer();
        let (lifecycle_updates_tx, lifecycle_updates_rx) =
            tokio::sync::mpsc::unbounded_channel::<LifecycleUpdate>();
        let mut lifecycle_operations = BTreeMap::<String, ActiveLifecycleOperation>::new();
        for session_id in interrupted_close_session_ids(&controller) {
            let state = controller.state.sessions[&session_id].state;
            let kind = if state == SessionState::Destroying {
                SessionOperationKind::Destroying
            } else {
                SessionOperationKind::Stopping
            };
            let cancelled = Arc::new(AtomicBool::new(false));
            lifecycle_operations.insert(
                session_id.clone(),
                ActiveLifecycleOperation {
                    cancelled: cancelled.clone(),
                    kind,
                },
            );
            dashboard.begin_session_operation(session_id.clone(), kind, None);
            spawn_interrupted_close_recovery(
                session_id,
                worker_commands_tx.clone(),
                recovery_observer.clone(),
                cancelled,
                lifecycle_updates_tx.clone(),
            );
        }
        let credential_sync = CredentialSyncCoordinator::spawn();
        let credential_sync_handle = credential_sync.handle();
        let (resource_targets_tx, resource_triggers_tx, resource_updates_rx) =
            spawn_dashboard_resource_poller();
        let (capacity_targets_tx, capacity_updates_rx) = spawn_dashboard_capacity_poller();
        let (aws_resource_options_tx, aws_resource_options_rx) =
            tokio::sync::mpsc::unbounded_channel::<AwsResourceOptions>();
        refresh_dashboard_poll_targets(
            &controller,
            &worker_targets_tx,
            &resource_targets_tx,
            &credential_sync_handle,
            &lifecycle_operations.keys().cloned().collect(),
        );
        let capacity_targets = controller.deployment_capacity_targets();
        capacity_targets_tx.send_replace(capacity_targets.clone());
        dashboard.set_deployment_capacity_targets(capacity_targets);
        let (import_updates_tx, import_updates_rx) =
            tokio::sync::mpsc::channel::<(u64, ImportProfileOption)>(32);
        let (import_task_tx, import_task_rx) =
            tokio::sync::mpsc::channel::<DashboardImportUpdate>(8);
        let (dashboard_io_tx, dashboard_io_rx) =
            tokio::sync::mpsc::unbounded_channel::<DashboardIoUpdate>();

        let mut context = Self {
            terminal,
            controller,
            dashboard,
            notices,
            events: Some(event::EventStream::new()),
            view: View::Dashboard,
            active_chat: None,
            dirty: true,
            controller_changed: true,
            quit_detached: false,
            quota_profiles_tx,
            quota: Feed::new(quota_updates_rx),
            manual_quota_refresh_generation: None,
            worker_targets_tx,
            worker: Feed::new(worker_updates_rx),
            worker_commands_tx,
            worker_diagnoses: WorkerDiagnosisTracker::default(),
            recovery: Feed::new(recovery),
            recovery_observer,
            lifecycle_updates_tx,
            lifecycle: Feed::new(lifecycle_updates_rx),
            lifecycle_operations,
            credential_sync: Feed::new(credential_sync),
            credential_sync_handle,
            auth_failure_syncs: AuthFailureSyncTracker::default(),
            credential_sync_notices: CredentialSyncNotices::default(),
            resource_targets_tx,
            resource_triggers_tx,
            resource: Feed::new(resource_updates_rx),
            capacity_targets_tx,
            capacity: Feed::new(capacity_updates_rx),
            aws_resource_options_tx,
            aws_options: Feed::new(aws_resource_options_rx),
            resolving_aws_resource_options: BTreeSet::new(),
            import_updates_tx,
            import_profiles: Feed::new(import_updates_rx),
            import_task_tx,
            import_tasks: Feed::new(import_task_rx),
            pending_import: None,
            import_discovery_id: 0,
            next_import_task_id: 0,
            active_import: None,
            dashboard_io_tx,
            dashboard_io: Feed::new(dashboard_io_rx),
            checkpoint_archive_targets_seen: BTreeMap::new(),
            checkpoint_archive_generation: 0,
        };
        context.request_quota_refresh();
        Ok(Some(context))
    }

    /// Redraws the view on screen, if anything asked for a redraw.
    fn draw(&mut self) -> Result<()> {
        if !self.dirty {
            return Ok(());
        }
        self.dirty = false;
        let Self {
            terminal,
            dashboard,
            active_chat,
            view,
            ..
        } = self;
        match (*view, active_chat.as_mut()) {
            (View::Chat, Some(chat)) => {
                terminal.terminal.draw(|frame| chat.draw(frame))?;
            }
            _ => {
                terminal.terminal.draw(|frame| render(frame, dashboard))?;
            }
        }
        Ok(())
    }

    /// Recomputes what depends on controller state after it may have changed.
    fn refresh_controller_derived_state(&mut self) {
        if !self.controller_changed {
            return;
        }
        self.controller_changed = false;
        let archive_targets = checkpoint_archive_targets(&self.controller);
        if archive_targets != self.checkpoint_archive_targets_seen {
            self.checkpoint_archive_targets_seen = archive_targets.clone();
            self.checkpoint_archive_generation =
                self.checkpoint_archive_generation.wrapping_add(1).max(1);
            spawn_checkpoint_archive_size_refresh(
                self.checkpoint_archive_generation,
                archive_targets,
                self.dashboard_io_tx.clone(),
            );
        }
        let capacity_targets = self.controller.deployment_capacity_targets();
        if *self.capacity_targets_tx.borrow() != capacity_targets {
            self.capacity_targets_tx
                .send_replace(capacity_targets.clone());
            self.dashboard
                .set_deployment_capacity_targets(capacity_targets);
            self.dirty = true;
        }
    }

    /// Asks the poller for fresh quotas and reports the refresh in the UI.
    /// Returns the generation, so a manual refresh can recognize its own
    /// completion.
    pub(crate) fn request_quota_refresh(&mut self) -> u64 {
        let profiles = quota_refresh_profiles(&self.controller);
        self.dashboard
            .begin_quota_refresh(profiles.iter().map(|profile| profile.profile_id.clone()));
        let generation = self
            .quota_profiles_tx
            .borrow()
            .generation
            .wrapping_add(1)
            .max(1);
        self.quota_profiles_tx.send_replace(QuotaRefreshBatch {
            generation,
            profiles,
        });
        generation
    }

    /// Republishes what the pollers should watch, leaving out sessions a
    /// lifecycle operation currently owns.
    pub(crate) fn refresh_poll_targets(&self) {
        refresh_dashboard_poll_targets(
            &self.controller,
            &self.worker_targets_tx,
            &self.resource_targets_tx,
            &self.credential_sync_handle,
            &self.lifecycle_operations.keys().cloned().collect(),
        );
    }

    /// Drops the warm chat when it belongs to `session_id`.
    ///
    /// Pause and destroy retire that session's actor. Resume starts a new one,
    /// often on a different profile. Keeping the old view would redraw a
    /// Closing/Closed snapshot and refuse prompts.
    pub(crate) fn drop_warm_chat_for(&mut self, session_id: &str) {
        if self
            .active_chat
            .as_ref()
            .is_some_and(|chat| chat.session_id() == session_id)
        {
            self.active_chat = None;
        }
    }

    /// Opens or reveals the chat for `session_id`, reporting a failure as a
    /// notice rather than ending the run.
    pub(crate) async fn hold_chat_session(&mut self, session_id: &str) -> Result<(), ()> {
        match hold_chat_session(
            &self.controller,
            &mut self.active_chat,
            session_id,
            &self.worker_commands_tx,
            &self.recovery_observer,
            self.notices.clone(),
        )
        .await
        {
            Ok(()) => Ok(()),
            Err(error) => {
                self.dashboard
                    .set_notice(format!("Could not open session: {error:#}"));
                Err(())
            }
        }
    }

    /// Suspends the terminal for the setup dialog and takes back what it
    /// changed.
    ///
    /// The dialog reads the terminal itself, and once polled an `EventStream`
    /// leaves a reader thread inside crossterm's internal reader, where it
    /// holds the reader lock and consumes terminal input. So the live stream is
    /// dropped before the replacement is built, which takes that same lock. The
    /// replacement reads nothing until the loop polls it again.
    pub(crate) fn run_setup_dialog(&mut self) -> Result<SetupOutcome> {
        drop(self.events.take());
        self.terminal.suspend()?;
        let outcome = run_setup_dialog(&config_path());
        self.terminal.resume()?;
        self.events = Some(event::EventStream::new());
        outcome
    }

    /// Tells every operation still in flight to stop. Cancellation is
    /// cooperative, so this only requests it.
    fn cancel_background_work(&self) {
        for operation in self.lifecycle_operations.values() {
            operation.cancelled.store(true, Ordering::Release);
        }
        if let Some(active) = self.active_import.as_ref() {
            active.cancelled.store(true, Ordering::Release);
        }
    }

    /// Takes every message queued behind the one that woke the loop, feed by
    /// feed, in the order the UI depends on.
    fn drain_feeds(&mut self) {
        self.drain_quota_updates();
        self.drain_worker_updates();
        schedule_due_auth_failure_syncs(
            &mut self.auth_failure_syncs,
            &self.credential_sync_handle,
            Instant::now(),
        );
        self.drain_recovery_results();
        self.drain_credential_results();
        self.drain_resource_updates();
        self.drain_capacity_updates();
        self.drain_aws_resource_options();
        self.drain_import_profiles();
        self.drain_import_tasks();
        self.drain_lifecycle_updates();
        self.drain_dashboard_io();
    }

    fn drain_quota_updates(&mut self) {
        while let Some(update) = self.quota.next_ready() {
            match update {
                QuotaUpdate::Refreshing { profile_ids } => {
                    self.dashboard.begin_quota_refresh(profile_ids)
                }
                QuotaUpdate::Report(quota) => self.dashboard.apply_quota(quota),
                QuotaUpdate::Finished { generation } => {
                    if complete_manual_quota_refresh(
                        &mut self.manual_quota_refresh_generation,
                        generation,
                    ) {
                        self.dashboard
                            .replace_notice_if(QUOTA_REFRESH_NOTICE, QUOTA_REFRESHED_NOTICE);
                    }
                }
            }
        }
    }

    fn drain_worker_updates(&mut self) {
        while let Some(update) = self.worker.next_ready() {
            self.controller_changed = true;
            let session_id = update.session_id.clone();
            let connected = update.view.connected;
            // Only unreachable relays drive the worker diagnostics flow.
            let connection_error = match update.view.error.as_ref() {
                Some(ViewError::Unreachable(detail)) => Some(detail.clone()),
                Some(ViewError::ProjectionIntegrity(_)) | None => None,
            };
            if let Some(snapshot) = update.view.snapshot.as_ref()
                && let Some(session) = self.controller.state.sessions.get(&session_id).cloned()
            {
                if let Some(ordinal) = snapshot.latest_auth_failure_ordinal {
                    self.auth_failure_syncs
                        .observe(&session_id, &session.last_profile, ordinal);
                }
                // Queued, never awaited: the copy decision belongs to the
                // coordinator, and the dashboard loop must stay free to draw.
                self.recovery_observer
                    .observe(hel::hel_state::RecoveryObservation {
                        session,
                        config: self.controller.config.clone(),
                        latest_completed_turn_ordinal:
                            hel::hel_state::latest_completed_turn_ordinal(&snapshot.materialized),
                        execution: snapshot.materialized.execution,
                    });
            }
            let materialized = update
                .view
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.materialized.clone());
            match apply_worker_poll_update(
                &mut self.controller,
                &mut self.dashboard,
                update,
                &self.dashboard_io_tx,
            ) {
                Ok(true) => {
                    let _ = self.resource_triggers_tx.try_send(session_id.clone());
                    if let Some(materialized) = materialized {
                        let detached_after_event_ordinal = self
                            .controller
                            .state
                            .sessions
                            .get(&session_id)
                            .map_or(0, |session| session.detached_after_event_ordinal);
                        let previous = self.dashboard.take_projection_cache(&session_id);
                        spawn_materialized_session_projection(
                            materialized,
                            detached_after_event_ordinal,
                            previous,
                            self.dashboard_io_tx.clone(),
                        );
                    }
                }
                Ok(false) => {}
                Err(error) => {
                    self.dashboard
                        .set_notice(format!("Could not save harness title: {error:#}"));
                }
            }
            if let Some(episode_id) =
                self.worker_diagnoses
                    .observe(&session_id, connected, connection_error)
            {
                spawn_worker_diagnosis(
                    &self.controller,
                    session_id,
                    episode_id,
                    self.dashboard_io_tx.clone(),
                );
            }
        }
    }

    fn drain_recovery_results(&mut self) {
        while let Some(result) = self.recovery.next_ready() {
            self.controller_changed = true;
            apply_recovery_result(&mut self.controller, &mut self.dashboard, result);
        }
    }

    fn drain_credential_results(&mut self) {
        while let Some(result) = self.credential_sync.next_ready() {
            if let Some(notice) = self.credential_sync_notices.notice(&result) {
                self.dashboard.set_notice(notice);
            }
        }
    }

    fn drain_resource_updates(&mut self) {
        while let Some(update) = self.resource.next_ready() {
            self.dashboard
                .apply_resource_usage(&update.session_id, update.usage);
        }
    }

    fn drain_capacity_updates(&mut self) {
        while let Some(update) = self.capacity.next_ready() {
            self.dashboard.apply_deployment_capacity(
                &update.target_id,
                update.result,
                update.sampled_at_epoch_seconds,
            );
        }
    }

    fn drain_aws_resource_options(&mut self) {
        while let Some((target_id, result)) = self.aws_options.next_ready() {
            self.resolving_aws_resource_options.remove(&target_id);
            self.dashboard
                .apply_aws_resource_options(&target_id, result);
        }
    }

    fn drain_import_profiles(&mut self) {
        while let Some((discovery_id, profile)) = self.import_profiles.next_ready() {
            self.dashboard.apply_resume_profile(discovery_id, profile);
        }
    }

    fn drain_import_tasks(&mut self) {
        while let Some(update) = self.import_tasks.next_ready() {
            match update {
                DashboardImportUpdate::Progress {
                    task_id,
                    step,
                    total,
                    message,
                } => {
                    if self
                        .active_import
                        .as_ref()
                        .is_some_and(|active| active.task_id == task_id)
                    {
                        self.dashboard.update_import_progress(step, total, message);
                    }
                }
                DashboardImportUpdate::Finished {
                    task_id,
                    pending,
                    result,
                } => {
                    if self
                        .active_import
                        .as_ref()
                        .is_none_or(|active| active.task_id != task_id)
                    {
                        continue;
                    }
                    self.active_import = None;
                    match result {
                        Ok(DashboardImportTaskResult::NeedsBundle(prompt)) => {
                            self.pending_import = Some(pending);
                            self.dashboard.show_import_bundle_confirmation(
                                prompt.dirty_git_roots,
                                prompt.omitted_non_git_dirs,
                                prompt.scratch_git_roots,
                                prompt.has_untracked_files,
                            );
                        }
                        Ok(DashboardImportTaskResult::Imported(imported)) => {
                            self.dashboard.finish_import();
                            self.dashboard.set_notice("Saving imported session…");
                            io::spawn_imported_session_apply(
                                imported,
                                pending,
                                self.dashboard_io_tx.clone(),
                            );
                        }
                        Ok(DashboardImportTaskResult::Cancelled) => {
                            self.dashboard.finish_import();
                            self.dashboard
                                .set_notice("Import cancelled; no Hel files were changed.");
                        }
                        Err(error) => {
                            self.dashboard.finish_import();
                            self.dashboard
                                .set_notice(format!("Import failed: {error:#}"));
                        }
                    }
                }
            }
        }
    }

    fn drain_lifecycle_updates(&mut self) {
        while let Some(update) = self.lifecycle.next_ready() {
            self.controller_changed = true;
            let session_id = update.session_id.clone();
            let operation = self.lifecycle_operations.remove(&session_id);
            self.dashboard.finish_session_operation(&session_id);
            spawn_lifecycle_reload(
                LifecycleReload { update, operation },
                self.dashboard_io_tx.clone(),
            );
        }
    }

    fn drain_dashboard_io(&mut self) {
        while let Some(update) = self.dashboard_io.next_ready() {
            self.controller_changed = true;
            self.apply_dashboard_io_update(update);
        }
    }

    /// Opens the resume dialog and starts one background scan per profile.
    /// Every profile appears immediately as a placeholder, so the dialog is
    /// usable while the scans are still running, and the scans run
    /// concurrently rather than one after another.
    pub(crate) fn start_resume_discovery(&mut self) {
        self.import_discovery_id = self.import_discovery_id.wrapping_add(1);
        self.dashboard.show_resume_dialog(
            self.import_discovery_id,
            resume_profile_placeholders(
                self.controller
                    .config
                    .profiles
                    .iter()
                    .map(|(id, profile)| (id.clone(), profile.kind)),
            ),
        );
        io::spawn_hidden_native_sessions_load(self.dashboard_io_tx.clone());
        let discovery_id = self.import_discovery_id;
        for (profile_id, profile) in self.controller.config.profiles.clone() {
            let updates = self.import_updates_tx.clone();
            tokio::task::spawn_blocking(move || {
                let completed = crate::import::discover_import_profile(
                    profile_id,
                    profile.kind,
                    profile.home,
                    |profile| {
                        if let Ok(permit) = updates.try_reserve() {
                            permit.send((discovery_id, profile.clone()));
                        }
                    },
                );
                let _ = updates.blocking_send((discovery_id, completed));
            });
        }
    }

    /// Starts a background import and shows its progress dialog.
    pub(crate) fn start_import(
        &mut self,
        pending: PendingDashboardImport,
        safety: crate::import::DashboardImportSafety,
    ) {
        self.dashboard
            .show_import_progress(pending.display_title.clone());
        self.next_import_task_id = self.next_import_task_id.wrapping_add(1);
        let cancelled = Arc::new(AtomicBool::new(false));
        self.active_import = Some(ActiveDashboardImport {
            task_id: self.next_import_task_id,
            cancelled: cancelled.clone(),
        });
        spawn_dashboard_import(
            &self.controller,
            pending,
            safety,
            self.next_import_task_id,
            cancelled,
            self.import_task_tx.clone(),
        );
    }

    /// Applies what the chat view asked for after handling its own input.
    /// Breaking means the user quit from the chat.
    async fn apply_chat_outcome(
        &mut self,
        outcome: hel::hel_chat::ChatEventOutcome,
    ) -> ControlFlow<()> {
        match outcome {
            hel::hel_chat::ChatEventOutcome::None | hel::hel_chat::ChatEventOutcome::Handled => {}
            hel::hel_chat::ChatEventOutcome::SwitchSession {
                session_id,
                last_seen_event_ordinal,
            } => {
                // The session being left is saved exactly as `Back` saves it;
                // only the view it hands over to differs.
                self.record_detach(last_seen_event_ordinal);
                // Keep the session list on the conversation now on screen, so
                // returning to it lands where the user left off.
                self.dashboard.select_active_session(&session_id);
                // The view stays on a chat either way: the session that opened,
                // or the one still here with the notice saying why it did not.
                //
                // The user arrived by walking the list, so the pane keeps focus
                // and the next key walks on from here.
                if self.hold_chat_session(&session_id).await.is_ok()
                    && let Some(chat) = self.active_chat.as_mut()
                {
                    chat.focus_conversations();
                }
                self.dirty = true;
            }
            hel::hel_chat::ChatEventOutcome::Back {
                last_seen_event_ordinal,
            } => {
                self.leave_chat(last_seen_event_ordinal);
            }
            hel::hel_chat::ChatEventOutcome::QuitDetach {
                last_seen_event_ordinal,
            } => {
                let persist = self.leave_chat(last_seen_event_ordinal);
                // Quitting leaves this loop for process exit, so the detach
                // write has to land first. Bounded so a stuck database cannot
                // hang the quit.
                if let Some(persist) = persist {
                    let _ = tokio::time::timeout(io::DETACH_PERSIST_QUIT_TIMEOUT, persist).await;
                }
                self.quit_detached = true;
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    }

    /// Returns to the session list. The chat stays warm behind it, so its feeds
    /// keep following the worker and reopening it costs a draw.
    fn leave_chat(&mut self, last_seen_event_ordinal: u64) -> Option<tokio::task::JoinHandle<()>> {
        self.view = View::Dashboard;
        self.dirty = true;
        // The warm chat goes on holding this input in memory, so save it here:
        // a quit or a crash while it is off screen would otherwise lose it.
        self.record_detach(last_seen_event_ordinal)
    }

    /// Persists how far the warm chat has been read and the draft it holds.
    fn record_detach(
        &mut self,
        last_seen_event_ordinal: u64,
    ) -> Option<tokio::task::JoinHandle<()>> {
        let detached = self
            .active_chat
            .as_ref()
            .map(|chat| (chat.session_id().to_owned(), chat.draft().to_owned()))?;
        let (session_id, draft) = detached;
        record_chat_detach_state(
            &mut self.controller,
            &mut self.dashboard,
            &session_id,
            last_seen_event_ordinal,
            &draft,
            &self.dashboard_io_tx,
        )
    }
}

/// The next terminal event. Cancel-safe, so losing the `select!` race cannot
/// drop one. A missing stream is never ready: it is absent only while the setup
/// dialog owns the terminal, which is not while this loop is waiting.
async fn next_terminal_event(
    events: &mut Option<event::EventStream>,
) -> Option<std::io::Result<Event>> {
    match events {
        Some(events) => events.next().await,
        None => std::future::pending().await,
    }
}

/// Builds a chat view for one session: its identity, the other sessions it
/// reports activity for, and its recovery context.
async fn open_chat_view(
    controller: &Controller,
    session_id: &str,
    sessions: &SessionManagerControl,
    recovery_observer: &RecoveryObserver,
    notices: hel::hel_chat::Notices,
) -> Result<hel::hel_chat::ActiveChat> {
    let session_record = controller
        .state
        .sessions
        .get(session_id)
        .with_context(|| format!("unknown session {session_id}"))?
        .clone();
    let bundle_id = session_record.bundle_id.clone();
    // Only a fresh view is seeded: a warm chat the loop kept alive holds newer
    // input than this saved copy.
    let saved_draft = session_record.draft_input.clone();
    // The header lists every active session in the order the session list
    // shows them, so each one keeps the same place in both views.
    let mut active = controller
        .state
        .sessions
        .values()
        .filter(|record| record.state.is_active())
        .collect::<Vec<_>>();
    active.sort_by(|left, right| left.compare_by_creation(right));
    let mut header = hel::hel_chat::SessionHeaderIdentity {
        project: session_record.project_name(&controller.config),
        ..hel::hel_chat::SessionHeaderIdentity::default()
    };
    for (position, record) in active.into_iter().enumerate() {
        if record.id == session_id {
            header.position = position;
            continue;
        }
        header.others.push(hel::hel_chat::OtherSessionIdentity {
            session_id: record.id.clone(),
            position,
            project: record.project_name(&controller.config),
        });
    }
    let harness = controller
        .config
        .profiles
        .get(&session_record.last_profile)
        .map(|profile| profile.kind);
    let recovery_context = hel::hel_state::RecoveryContext {
        observer: recovery_observer.clone(),
        session: session_record,
        config: controller.config.clone(),
    };
    let managed = sessions.session(session_id.to_owned()).await?;
    Ok(hel::hel_chat::ActiveChat::open(
        managed,
        &bundle_id,
        Some(recovery_context),
        sessions.clone(),
        header,
        saved_draft,
        notices,
        harness,
    ))
}

/// Makes `session_id` the chat the loop holds. A warm chat for the same
/// session is left as it is, so showing it costs a draw; any other session is
/// opened fresh, which drops the previous chat and detaches its proxy.
///
/// A warm chat whose session actor has already been retired is not reused.
/// Pause closes that actor after sealing the worker, and the view keeps the
/// Closing/Closed snapshot; reopening attaches to the worker resume started.
async fn hold_chat_session(
    controller: &Controller,
    active_chat: &mut Option<hel::hel_chat::ActiveChat>,
    session_id: &str,
    sessions: &SessionManagerControl,
    recovery_observer: &RecoveryObserver,
    notices: hel::hel_chat::Notices,
) -> Result<()> {
    if active_chat
        .as_ref()
        .is_some_and(|chat| chat.session_id() == session_id && chat.session_feed_open())
    {
        // The warm chat is this session and still follows its worker, so
        // showing it is only a redraw.
        return Ok(());
    }
    let chat = open_chat_view(controller, session_id, sessions, recovery_observer, notices).await?;
    // Only one chat stays warm, so the previous one is dropped here; its
    // supervisor detaches on drop.
    *active_chat = Some(chat);
    Ok(())
}

/// Records what leaving a chat produced — how far the user has read and the
/// input they left unsent — and persists both in the background. A missing
/// session is reported rather than fatal: the session itself is unaffected.
///
/// The returned handle lets the quit path wait for the write. `None` means
/// nothing was queued.
fn record_chat_detach_state(
    controller: &mut Controller,
    dashboard: &mut DashboardState,
    session_id: &str,
    event_ordinal: u64,
    draft: &str,
    updates: &UnboundedSender<DashboardIoUpdate>,
) -> Option<tokio::task::JoinHandle<()>> {
    let Some(session) = controller.state.sessions.get_mut(session_id) else {
        dashboard.set_notice(format!(
            "Could not save draft and read status for {}: unknown session",
            short_id(session_id)
        ));
        return None;
    };
    session.detached_after_event_ordinal = session.detached_after_event_ordinal.max(event_ordinal);
    session.draft_input = draft.to_owned();
    dashboard.set_state(controller.state.clone());
    dashboard.clear_notice();
    Some(io::spawn_detached_session_state_persist(
        session_id.to_owned(),
        event_ordinal,
        draft.to_owned(),
        updates.clone(),
    ))
}

/// Applies one terminal event to the dashboard and reports the work it asks
/// for. Every event redraws, so events that carry no action still return
/// `None` rather than being skipped.
fn dashboard_event_action(dashboard: &mut DashboardState, event: Event) -> DashboardAction {
    match event {
        Event::Key(key) => dashboard.handle_key(key),
        Event::Paste(pasted) => {
            dashboard.handle_paste(&pasted);
            DashboardAction::None
        }
        Event::Mouse(mouse) => dashboard.handle_mouse(mouse),
        // Resize and focus changes only need the redraw.
        _ => DashboardAction::None,
    }
}

fn apply_recovery_result(
    controller: &mut Controller,
    dashboard: &mut DashboardState,
    result: hel::hel_recovery::RecoveryResult,
) {
    let session_id = result.session_id.clone();
    let failure = if result.cancelled {
        // A copy the user preempted is not a failure worth reporting.
        None
    } else {
        result.outcome.as_ref().err().cloned()
    };
    if merge_recovery_result(controller, result) {
        dashboard.set_state(controller.state.clone());
        if let Some(detail) = failure {
            dashboard.set_notice(format!(
                "Recovery copy for {} failed: {detail}",
                short_id(&session_id)
            ));
        }
    }
}

pub(crate) fn resume_progress_notice(
    session_id: &str,
    profile_id: &str,
    target_id: &str,
) -> String {
    format!(
        "Preparing {}: verifying checkpoint, provisioning {target_id}, and restoring {profile_id}…",
        short_id(session_id)
    )
}

fn configuration_needs_setup(config: &HelConfig) -> bool {
    config.profiles.is_empty() && config.bundles.is_empty() && config.targets.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;
    use hel::hel_state::HelState;

    /// The dashboard loop batches buffered input and stops at the first event
    /// that asks for work, so events that only need a redraw must report no
    /// action and actionable keys must report theirs.
    #[test]
    fn only_events_that_ask_for_work_end_an_input_batch() {
        let mut dashboard = DashboardState::new(
            HelConfig::default(),
            HelState::default(),
            std::collections::BTreeMap::new(),
        );

        assert!(matches!(
            dashboard_event_action(&mut dashboard, Event::Resize(80, 24)),
            DashboardAction::None
        ));
        assert!(matches!(
            dashboard_event_action(
                &mut dashboard,
                Event::Key(crossterm::event::KeyEvent::new(
                    crossterm::event::KeyCode::Esc,
                    crossterm::event::KeyModifiers::NONE,
                )),
            ),
            DashboardAction::QuitDetach
        ));
    }

    #[test]
    fn resume_progress_explains_the_blocking_work() {
        assert_eq!(
            resume_progress_notice("0123456789", "codex-1", "podman"),
            "Preparing 01234567: verifying checkpoint, provisioning podman, and restoring codex-1…"
        );
    }

    #[test]
    fn only_a_fully_empty_config_triggers_automatic_setup() {
        let mut config = hel::hel_config::HelConfig::default();
        assert!(configuration_needs_setup(&config));
        config.targets.insert(
            "podman".into(),
            hel::hel_config::TargetTemplate::LocalPodman {
                container: hel::hel_config::ContainerTemplate {
                    image: "ubuntu:24.04".into(),
                    platform: None,
                    cpus: None,
                    memory: None,
                    environment: std::collections::BTreeMap::new(),
                },
            },
        );
        assert!(!configuration_needs_setup(&config));
    }
}
