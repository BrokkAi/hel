//! Multiplexed controller-side ownership of durable ACP relay sessions.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use tokio::sync::{mpsc, oneshot, watch};

use crate::hel_archive::verify_archive_streaming;
use crate::hel_credentials::relay_event_reports_auth_failure;
use crate::hel_database::{
    ProjectionApplyOutcome, apply_projection_event, save_materialized_session,
};
use crate::hel_projection::{
    apply_committed_projection_event, materialized_session_from_canonical, project_relay_event,
};
use crate::hel_state::MaterializedSession;
use crate::hel_targets::CommandSpec;
use crate::hel_worker::{RelayCommand, RelayCursor, RelayOperationalState, validate_relay_event};
use crate::hel_worker_client::{RelayClient, RelayEventPage, RelayRejected};

const SESSION_SYNC_INTERVAL: Duration = Duration::from_millis(150);
const RECONNECT_INTERVAL: Duration = Duration::from_secs(1);
const UNREACHABLE_FAILURE_THRESHOLD: u32 = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelaySessionTarget {
    pub session_id: String,
    pub spec: CommandSpec,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ManagedSessionSnapshot {
    pub materialized: MaterializedSession,
    pub operational: RelayOperationalState,
    /// Newest relay event observed by this live actor that reports an
    /// authentication failure. This is intentionally ephemeral: it avoids
    /// retaining raw replay pages or rescanning projected history.
    pub latest_auth_failure_ordinal: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ManagedSessionView {
    pub snapshot: Option<ManagedSessionSnapshot>,
    pub connected: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SessionManagerUpdate {
    pub session_id: String,
    pub view: ManagedSessionView,
}

pub struct SessionManagerChannels {
    pub targets: watch::Sender<Vec<RelaySessionTarget>>,
    pub control: SessionManagerControl,
    pub updates: SessionManagerUpdates,
}

#[derive(Clone)]
struct CoalescedUpdateSender {
    pending: Arc<Mutex<BTreeMap<String, SessionManagerUpdate>>>,
    wake: mpsc::Sender<()>,
}

/// Bounded latest-state feed for the dashboard. At most one snapshot per
/// session is retained while the consumer is busy.
pub struct SessionManagerUpdates {
    pending: Arc<Mutex<BTreeMap<String, SessionManagerUpdate>>>,
    wake: mpsc::Receiver<()>,
}

impl CoalescedUpdateSender {
    fn send(&self, update: SessionManagerUpdate) {
        if self.wake.is_closed() {
            return;
        }
        self.pending
            .lock()
            .expect("session update coalescer poisoned")
            .insert(update.session_id.clone(), update);
        let _ = self.wake.try_send(());
    }
}

impl SessionManagerUpdates {
    fn pop_pending(&self) -> Option<SessionManagerUpdate> {
        self.pending
            .lock()
            .expect("session update coalescer poisoned")
            .pop_first()
            .map(|(_, update)| update)
    }

    pub async fn recv(&mut self) -> Option<SessionManagerUpdate> {
        loop {
            if let Some(update) = self.pop_pending() {
                return Some(update);
            }
            self.wake.recv().await?;
        }
    }

    pub fn try_recv(
        &mut self,
    ) -> std::result::Result<SessionManagerUpdate, mpsc::error::TryRecvError> {
        if let Some(update) = self.pop_pending() {
            return Ok(update);
        }
        self.wake.try_recv()?;
        self.pop_pending().ok_or(mpsc::error::TryRecvError::Empty)
    }
}

fn coalesced_update_channel() -> (CoalescedUpdateSender, SessionManagerUpdates) {
    let pending = Arc::new(Mutex::new(BTreeMap::new()));
    let (wake_tx, wake_rx) = mpsc::channel(1);
    (
        CoalescedUpdateSender {
            pending: pending.clone(),
            wake: wake_tx,
        },
        SessionManagerUpdates {
            pending,
            wake: wake_rx,
        },
    )
}

#[derive(Clone)]
pub struct SessionManagerControl {
    commands: mpsc::Sender<ManagerCommand>,
}

#[derive(Clone)]
pub struct ManagedSessionHandle {
    session_id: String,
    commands: mpsc::Sender<ActorCommand>,
    releases: mpsc::UnboundedSender<ReturnedConnection>,
    view: watch::Receiver<ManagedSessionView>,
}

/// Exclusive ownership of a session actor's existing relay connection.
///
/// Lifecycle operations use this instead of opening a competing projection
/// client. Dropping an unreleased lease drops the proxy connection, which in
/// turn cancels any ordinary relay checkpoint barrier.
pub struct ManagedSessionLease {
    lease_id: Option<u64>,
    connection: Option<StandaloneSession>,
    releases: mpsc::UnboundedSender<ReturnedConnection>,
}

impl ManagedSessionLease {
    pub fn connection_mut(&mut self) -> &mut StandaloneSession {
        self.connection
            .as_mut()
            .expect("managed session lease has already been released")
    }

    pub fn release(mut self) {
        let lease_id = self
            .lease_id
            .take()
            .expect("managed session lease has already been released");
        let connection = self.connection.take();
        let _ = self.releases.send(ReturnedConnection {
            lease_id,
            connection,
        });
    }
}

impl Drop for ManagedSessionLease {
    fn drop(&mut self) {
        let Some(lease_id) = self.lease_id.take() else {
            return;
        };
        // Drop the proxy before telling the actor to reconnect so the relay
        // observes EOF and releases any abandoned checkpoint barrier first.
        drop(self.connection.take());
        let _ = self.releases.send(ReturnedConnection {
            lease_id,
            connection: None,
        });
    }
}

impl ManagedSessionHandle {
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub fn view(&self) -> ManagedSessionView {
        self.view.borrow().clone()
    }

    pub fn has_changed(&self) -> Result<bool> {
        self.view.has_changed().context("session manager stopped")
    }

    pub async fn changed(&mut self) -> Result<ManagedSessionView> {
        self.view
            .changed()
            .await
            .context("session manager stopped")?;
        Ok(self.view())
    }

    pub async fn submit(&self, command_id: String, command: RelayCommand) -> Result<u64> {
        self.enqueue_submit(command_id, command).await?.wait().await
    }

    pub(crate) async fn enqueue_submit(
        &self,
        command_id: String,
        command: RelayCommand,
    ) -> Result<PendingRelaySubmit> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(ActorCommand::Submit {
                command_id,
                command,
                reply,
            })
            .await
            .context("session manager stopped")?;
        Ok(PendingRelaySubmit { response })
    }

    pub async fn sync_now(&self) -> Result<()> {
        self.enqueue_sync().await?.wait().await
    }

    pub(crate) async fn enqueue_sync(&self) -> Result<PendingRelaySync> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(ActorCommand::Sync { reply })
            .await
            .context("session manager stopped")?;
        Ok(PendingRelaySync { response })
    }

    pub async fn lease_connection(&self) -> Result<ManagedSessionLease> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(ActorCommand::Lease { reply })
            .await
            .context("session manager stopped")?;
        let (lease_id, connection) = response
            .await
            .context("session manager stopped")?
            .map_err(anyhow::Error::msg)?;
        Ok(ManagedSessionLease {
            lease_id: Some(lease_id),
            connection: Some(connection),
            releases: self.releases.clone(),
        })
    }
}

pub(crate) struct PendingRelaySubmit {
    response: oneshot::Receiver<std::result::Result<u64, String>>,
}

impl PendingRelaySubmit {
    pub(crate) async fn wait(self) -> Result<u64> {
        self.response
            .await
            .context("session manager stopped")?
            .map_err(anyhow::Error::msg)
    }
}

pub(crate) struct PendingRelaySync {
    response: oneshot::Receiver<std::result::Result<(), String>>,
}

impl PendingRelaySync {
    pub(crate) async fn wait(self) -> Result<()> {
        self.response
            .await
            .context("session manager stopped")?
            .map_err(anyhow::Error::msg)
    }
}

impl SessionManagerControl {
    pub async fn session(&self, session_id: impl Into<String>) -> Result<ManagedSessionHandle> {
        let session_id = session_id.into();
        let (reply, response) = oneshot::channel();
        self.commands
            .send(ManagerCommand::Session {
                session_id: session_id.clone(),
                reply,
            })
            .await
            .context("session manager stopped")?;
        response
            .await
            .context("session manager stopped")?
            .with_context(|| format!("session {session_id} is not managed"))
    }

    pub async fn wait_for_session(
        &self,
        session_id: &str,
        timeout: Duration,
    ) -> Result<ManagedSessionHandle> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            match self.session(session_id.to_owned()).await {
                Ok(handle) => return Ok(handle),
                Err(error) if tokio::time::Instant::now() < deadline => {
                    tracing::trace!(session_id, "waiting for session actor: {error:#}");
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }
}

enum ManagerCommand {
    Session {
        session_id: String,
        reply: oneshot::Sender<Option<ManagedSessionHandle>>,
    },
}

enum ActorCommand {
    Submit {
        command_id: String,
        command: RelayCommand,
        reply: oneshot::Sender<std::result::Result<u64, String>>,
    },
    Sync {
        reply: oneshot::Sender<std::result::Result<(), String>>,
    },
    Lease {
        reply: oneshot::Sender<std::result::Result<(u64, StandaloneSession), String>>,
    },
}

impl ActorCommand {
    fn reject(self, message: &str) {
        match self {
            Self::Submit { reply, .. } => {
                let _ = reply.send(Err(message.to_owned()));
            }
            Self::Sync { reply } => {
                let _ = reply.send(Err(message.to_owned()));
            }
            Self::Lease { reply } => {
                let _ = reply.send(Err(message.to_owned()));
            }
        }
    }
}

struct ReturnedConnection {
    lease_id: u64,
    connection: Option<StandaloneSession>,
}

#[derive(Debug, Default)]
struct ActorLifecycle {
    active_lease: Option<u64>,
    retirement_requested: bool,
}

impl ActorLifecycle {
    fn set_retirement_requested(&mut self, requested: bool) {
        self.retirement_requested = requested;
    }

    fn is_leased(&self) -> bool {
        self.active_lease.is_some()
    }

    fn should_stop(&self) -> bool {
        self.retirement_requested && !self.is_leased()
    }

    fn accepts_new_work(&self) -> bool {
        !self.retirement_requested
    }

    fn activate_lease(&mut self, lease_id: u64) {
        debug_assert!(self.active_lease.is_none());
        self.active_lease = Some(lease_id);
    }

    fn return_lease(&mut self, lease_id: u64) -> bool {
        if self.active_lease != Some(lease_id) {
            return false;
        }
        self.active_lease = None;
        true
    }
}

struct ActorRegistration {
    target: RelaySessionTarget,
    commands: mpsc::Sender<ActorCommand>,
    releases: mpsc::UnboundedSender<ReturnedConnection>,
    retirement: watch::Sender<bool>,
    view: watch::Receiver<ManagedSessionView>,
    abort: tokio::task::AbortHandle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconcileAction {
    Idle,
    Spawn,
    Keep,
    Retire,
}

fn reconcile_action(
    actor: Option<&RelaySessionTarget>,
    desired: Option<&RelaySessionTarget>,
) -> ReconcileAction {
    match (actor, desired) {
        (None, None) => ReconcileAction::Idle,
        (None, Some(_)) => ReconcileAction::Spawn,
        (Some(actor), Some(desired)) if actor == desired => ReconcileAction::Keep,
        (Some(_), Some(_) | None) => ReconcileAction::Retire,
    }
}

fn target_map(targets: &[RelaySessionTarget]) -> BTreeMap<String, RelaySessionTarget> {
    targets
        .iter()
        .cloned()
        .map(|target| (target.session_id.clone(), target))
        .collect()
}

fn reconcile_actors(
    targets: &BTreeMap<String, RelaySessionTarget>,
    actors: &mut BTreeMap<String, ActorRegistration>,
    tasks: &mut tokio::task::JoinSet<String>,
    updates: &CoalescedUpdateSender,
) {
    for (session_id, actor) in actors.iter() {
        let retiring = matches!(
            reconcile_action(Some(&actor.target), targets.get(session_id)),
            ReconcileAction::Retire
        );
        actor.retirement.send_replace(retiring);
    }

    for (session_id, target) in targets {
        if !matches!(
            reconcile_action(
                actors.get(session_id).map(|actor| &actor.target),
                Some(target)
            ),
            ReconcileAction::Spawn
        ) {
            continue;
        }
        let (actor_tx, actor_rx) = mpsc::channel(32);
        let (release_tx, release_rx) = mpsc::unbounded_channel();
        let (retirement_tx, retirement_rx) = watch::channel(false);
        let (view_tx, view_rx) = watch::channel(ManagedSessionView::default());
        let actor_updates = updates.clone();
        let task_target = target.clone();
        let task_id = session_id.clone();
        let abort = tasks.spawn(async move {
            run_session_actor(
                task_target,
                actor_rx,
                release_rx,
                retirement_rx,
                view_tx,
                actor_updates,
            )
            .await;
            task_id
        });
        actors.insert(
            session_id.clone(),
            ActorRegistration {
                target: target.clone(),
                commands: actor_tx,
                releases: release_tx,
                retirement: retirement_tx,
                view: view_rx,
                abort,
            },
        );
    }
}

pub fn spawn_session_manager() -> Result<SessionManagerChannels> {
    let (targets_tx, mut targets_rx) = watch::channel(Vec::<RelaySessionTarget>::new());
    let (commands_tx, mut commands_rx) = mpsc::channel(32);
    let (updates_tx, updates_rx) = coalesced_update_channel();
    tokio::spawn(async move {
        let mut actors = BTreeMap::<String, ActorRegistration>::new();
        let mut tasks = tokio::task::JoinSet::<String>::new();
        let mut desired_targets = BTreeMap::<String, RelaySessionTarget>::new();
        loop {
            tokio::select! {
                changed = targets_rx.changed() => {
                    if changed.is_err() {
                        for (_, actor) in actors {
                            actor.abort.abort();
                        }
                        break;
                    }
                    desired_targets = target_map(&targets_rx.borrow_and_update());
                    reconcile_actors(
                        &desired_targets,
                        &mut actors,
                        &mut tasks,
                        &updates_tx,
                    );
                }
                command = commands_rx.recv() => {
                    let Some(ManagerCommand::Session { session_id, reply }) = command else {
                        for (_, actor) in actors {
                            actor.abort.abort();
                        }
                        break;
                    };
                    let handle = actors
                        .get(&session_id)
                        .filter(|actor| !actor.commands.is_closed())
                        .filter(|actor| desired_targets.get(&session_id) == Some(&actor.target))
                        .map(|actor| ManagedSessionHandle {
                            session_id,
                            commands: actor.commands.clone(),
                            releases: actor.releases.clone(),
                            view: actor.view.clone(),
                        });
                    let _ = reply.send(handle);
                }
                joined = tasks.join_next_with_id(), if !tasks.is_empty() => {
                    match joined {
                        Some(Ok((_task_id, session_id))) => {
                            actors.remove(&session_id);
                            // A watch sender may have published another target while this
                            // completion was already ready. Reconcile against its newest
                            // value so an intermediate replacement is never started.
                            desired_targets = target_map(&targets_rx.borrow());
                            reconcile_actors(
                                &desired_targets,
                                &mut actors,
                                &mut tasks,
                                &updates_tx,
                            );
                        }
                        Some(Err(error)) if error.is_cancelled() => {}
                        Some(Err(error)) => {
                            let failed_task = error.id();
                            if let Some(session_id) = actors.iter().find_map(|(session_id, actor)| {
                                (actor.abort.id() == failed_task).then(|| session_id.clone())
                            }) {
                                actors.remove(&session_id);
                                desired_targets = target_map(&targets_rx.borrow());
                                reconcile_actors(
                                    &desired_targets,
                                    &mut actors,
                                    &mut tasks,
                                    &updates_tx,
                                );
                            }
                            tracing::error!(%error, "session relay actor failed");
                        }
                        None => {}
                    }
                }
            }
        }
    });
    Ok(SessionManagerChannels {
        targets: targets_tx,
        control: SessionManagerControl {
            commands: commands_tx,
        },
        updates: updates_rx,
    })
}

async fn run_session_actor(
    target: RelaySessionTarget,
    mut commands: mpsc::Receiver<ActorCommand>,
    mut releases: mpsc::UnboundedReceiver<ReturnedConnection>,
    mut retirement: watch::Receiver<bool>,
    view_tx: watch::Sender<ManagedSessionView>,
    updates: CoalescedUpdateSender,
) {
    let mut connection: Option<StandaloneSession> = None;
    let mut failures = 0_u32;
    let mut lifecycle = ActorLifecycle::default();
    let mut next_lease_id = 1_u64;
    let mut interval = tokio::time::interval(SESSION_SYNC_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        lifecycle.set_retirement_requested(*retirement.borrow_and_update());
        if lifecycle.should_stop() {
            break;
        }
        tokio::select! {
            _ = interval.tick() => {
                lifecycle.set_retirement_requested(*retirement.borrow());
                if lifecycle.should_stop() {
                    break;
                }
                if lifecycle.is_leased() {
                    continue;
                }
                let result = sync_actor_connection(
                    &target,
                    &mut connection,
                ).await;
                match result {
                    Ok(snapshot) => {
                        failures = 0;
                        if let Some(snapshot) = snapshot {
                            publish_view(&target.session_id, ManagedSessionView {
                                snapshot: Some(snapshot),
                                connected: true,
                                error: None,
                            }, &view_tx, &updates);
                        }
                    }
                    Err(error) => {
                        connection = None;
                        failures = failures.saturating_add(1);
                        if failures >= UNREACHABLE_FAILURE_THRESHOLD {
                            publish_view(&target.session_id, ManagedSessionView {
                                snapshot: view_tx.borrow().snapshot.clone(),
                                connected: false,
                                error: Some(format!("{error:#}")),
                            }, &view_tx, &updates);
                        }
                        interval.reset_after(RECONNECT_INTERVAL);
                    }
                }
            }
            command = commands.recv() => {
                let Some(command) = command else { break };
                lifecycle.set_retirement_requested(*retirement.borrow());
                if !lifecycle.accepts_new_work() {
                    command.reject("session target is changing");
                    continue;
                }
                match command {
                    ActorCommand::Submit { command_id, command, reply } => {
                        if lifecycle.is_leased() {
                            let _ = reply.send(Err(
                                "session is reserved for a lifecycle operation".into(),
                            ));
                            continue;
                        }
                        let result = submit_actor_command(
                            &target,
                            &mut connection,
                            &command_id,
                            &command,
                        )
                        .await;
                        if let Ok(ordinal) = result.as_ref() {
                            let snapshot = connection
                                .as_ref()
                                .expect("successful submission retained its connection")
                                .snapshot();
                            publish_view(&target.session_id, ManagedSessionView {
                                snapshot: Some(snapshot),
                                connected: true,
                                error: None,
                            }, &view_tx, &updates);
                            tracing::trace!(%ordinal, %command_id, "relay command accepted");
                        }
                        if result.is_err() {
                            connection = None;
                        }
                        let _ = reply.send(result.map_err(|error| format!("{error:#}")));
                    }
                    ActorCommand::Sync { reply } => {
                        if lifecycle.is_leased() {
                            let _ = reply.send(Err(
                                "session is reserved for a lifecycle operation".into(),
                            ));
                            continue;
                        }
                        let result = sync_actor_connection(
                            &target,
                            &mut connection,
                        ).await.map(|snapshot| {
                            if let Some(snapshot) = snapshot {
                                publish_view(&target.session_id, ManagedSessionView {
                                    snapshot: Some(snapshot),
                                    connected: true,
                                    error: None,
                                }, &view_tx, &updates);
                            }
                        });
                        if result.is_err() {
                            connection = None;
                        }
                        let _ = reply.send(result.map_err(|error| format!("{error:#}")));
                    }
                    ActorCommand::Lease { reply } => {
                        if lifecycle.is_leased() {
                            let _ = reply.send(Err(
                                "session already has a lifecycle operation".into(),
                            ));
                            continue;
                        }
                        let lease_id = next_lease_id;
                        let result = sync_actor_connection(
                            &target,
                            &mut connection,
                        )
                        .await
                        .map(|_| {
                            next_lease_id = next_lease_id.wrapping_add(1).max(1);
                            (
                                lease_id,
                                connection
                                    .take()
                                    .expect("successful sync retained its connection"),
                            )
                        })
                        .map_err(|error| format!("{error:#}"));
                        if result.is_err() {
                            connection = None;
                        }
                        let acquired = result.is_ok();
                        match reply.send(result) {
                            Ok(()) if acquired => lifecycle.activate_lease(lease_id),
                            Ok(()) => {}
                            Err(Ok((_lease_id, returned))) => connection = Some(returned),
                            Err(Err(_)) => {}
                        }
                    }
                }
            }
            returned = releases.recv() => {
                let Some(returned) = returned else { continue };
                if lifecycle.return_lease(returned.lease_id) {
                    connection = returned.connection;
                    failures = 0;
                    interval.reset();
                }
            }
            changed = retirement.changed() => {
                if changed.is_err() {
                    break;
                }
            }
        }
    }
}

async fn submit_actor_command(
    target: &RelaySessionTarget,
    connection: &mut Option<StandaloneSession>,
    command_id: &str,
    command: &RelayCommand,
) -> Result<u64> {
    let mut first_error = None;
    for _ in 0..2 {
        if connection.is_none() {
            sync_actor_connection(target, connection).await?;
        }
        let result = connection
            .as_mut()
            .context("relay is disconnected")?
            .submit(command_id.to_owned(), command.clone())
            .await;
        match result {
            Ok(ordinal) => return Ok(ordinal),
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(format!("{error:#}"));
                }
                *connection = None;
            }
        }
    }
    let detail = first_error.unwrap_or_else(|| "relay submission failed".into());
    bail!("relay command {command_id} failed after an idempotent reconnect: {detail}")
}

async fn sync_actor_connection(
    target: &RelaySessionTarget,
    connection: &mut Option<StandaloneSession>,
) -> Result<Option<ManagedSessionSnapshot>> {
    if connection.is_none() {
        *connection = Some(StandaloneSession::connect(target).await?);
        return Ok(Some(
            connection
                .as_ref()
                .expect("connection was initialized")
                .snapshot(),
        ));
    }
    let connection = connection.as_mut().expect("connection was initialized");
    if connection.sync_in_place().await? {
        Ok(Some(connection.snapshot()))
    } else {
        Ok(None)
    }
}

fn publish_view(
    session_id: &str,
    view: ManagedSessionView,
    watch: &watch::Sender<ManagedSessionView>,
    updates: &CoalescedUpdateSender,
) {
    if *watch.borrow() == view {
        return;
    }
    watch.send_replace(view.clone());
    updates.send(SessionManagerUpdate {
        session_id: session_id.to_owned(),
        view,
    });
}

pub struct StandaloneSession {
    client: RelayClient,
    materialized: MaterializedSession,
    operational: RelayOperationalState,
    latest_auth_failure_ordinal: Option<u64>,
}

impl StandaloneSession {
    pub async fn connect(target: &RelaySessionTarget) -> Result<Self> {
        let materialized = crate::hel_database::load_materialized_session(&target.session_id)?
            .unwrap_or_else(|| MaterializedSession::empty(target.session_id.clone()));
        let mut client = RelayClient::connect(&target.spec, &target.session_id).await?;
        let operational = client.status().await?;
        let mut connection = Self {
            client,
            materialized,
            operational,
            latest_auth_failure_ordinal: None,
        };
        connection.sync_in_place().await?;
        Ok(connection)
    }

    pub async fn connect_command(spec: &CommandSpec, session_id: &str) -> Result<Self> {
        Self::connect(&RelaySessionTarget {
            session_id: session_id.to_owned(),
            spec: spec.clone(),
        })
        .await
    }

    pub async fn sync(&mut self) -> Result<ManagedSessionSnapshot> {
        self.sync_in_place().await?;
        Ok(self.snapshot())
    }

    async fn sync_in_place(&mut self) -> Result<bool> {
        let original_ordinal = self.materialized.applied_event_ordinal;
        let original_digest = self.materialized.applied_event_digest.clone();
        let original_operational = self.operational.clone();
        let mut repaired = false;
        loop {
            let after_ordinal = self.materialized.applied_event_ordinal;
            match self.catch_up_fixed_frontier().await {
                Ok(()) => break,
                Err(error) if relay_desynchronized(&error) => {
                    self.repair_projection()
                        .await
                        .with_context(|| {
                            format!(
                                "controller projection for {} cannot catch up from ordinal {after_ordinal}: {error:#}",
                                self.materialized.session_id
                            )
                        })?;
                    repaired = true;
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(repaired
            || self.materialized.applied_event_ordinal != original_ordinal
            || self.materialized.applied_event_digest != original_digest
            || self.operational != original_operational)
    }

    /// Apply and acknowledge one relay page at a time through the exact
    /// frontier captured by the first response. This bounds memory by the
    /// relay frame size and makes every completed page durable progress.
    async fn catch_up_fixed_frontier(&mut self) -> Result<()> {
        let after = RelayCursor {
            ordinal: self.materialized.applied_event_ordinal,
            digest: self.materialized.applied_event_digest.clone(),
        };
        let catch_up = self
            .client
            .begin_catch_up(after.ordinal, &after.digest)
            .await?;
        let mut cursor = self.apply_event_page(catch_up.first_page).await?;
        let mut pages_remaining = catch_up.frontier.ordinal.saturating_sub(cursor.ordinal);
        while cursor.ordinal < catch_up.frontier.ordinal {
            ensure!(
                pages_remaining > 0,
                "relay catch-up exceeded its fixed page bound"
            );
            pages_remaining -= 1;
            let page = self
                .client
                .next_catch_up_page(&cursor, &catch_up.frontier)
                .await?;
            cursor = self.apply_event_page(page).await?;
        }
        ensure!(
            cursor == catch_up.frontier,
            "controller projection did not reach the captured relay frontier"
        );
        let mut operational = catch_up.state;
        operational.acknowledged_through = cursor.ordinal;
        operational.acknowledged_digest = cursor.digest;
        self.operational = operational;
        Ok(())
    }

    async fn repair_projection(&mut self) -> Result<()> {
        let state = crate::hel_database::load_state()?;
        let record = state
            .sessions
            .get(&self.materialized.session_id)
            .context("controller session disappeared while repairing its projection")?;
        let Some(checkpoint) = record.checkpoint.as_ref() else {
            let replacement = MaterializedSession::empty(&self.materialized.session_id);
            self.client
                .attach(
                    replacement.applied_event_ordinal,
                    &replacement.applied_event_digest,
                )
                .await
                .context("relay cannot rebuild the projection from its genesis")?;
            save_materialized_session(&replacement)?;
            self.materialized = replacement;
            return Ok(());
        };
        let checkpoint_path = checkpoint.archive_path.clone();
        let archive = tokio::task::spawn_blocking(move || {
            verify_archive_streaming(&checkpoint_path).with_context(|| {
                format!(
                    "verify projection repair checkpoint {}",
                    checkpoint_path.display()
                )
            })
        })
        .await
        .context("projection repair archive verification task failed")??;
        ensure!(
            archive.archive_sha256 == checkpoint.sha256,
            "projection repair checkpoint checksum does not match controller metadata"
        );
        ensure!(
            archive.manifest.session.id == self.materialized.session_id,
            "projection repair checkpoint belongs to session {}, not {}",
            archive.manifest.session.id,
            self.materialized.session_id
        );
        let canonical = archive.canonical_session;
        ensure!(
            canonical.event_frontier == checkpoint.event_frontier,
            "projection repair checkpoint metadata frontier {} does not match archive frontier {}",
            checkpoint.event_frontier,
            canonical.event_frontier
        );

        // Prove that the relay recognizes this exact event-chain cursor before
        // replacing any controller state. A matching ordinal alone is not a
        // repair proof.
        self.client
            .attach(canonical.event_frontier, &canonical.event_frontier_digest)
            .await
            .context("relay rejected the verified checkpoint repair cursor")?;
        let replacement =
            materialized_session_from_canonical(&self.materialized.session_id, &canonical)?;
        save_materialized_session(&replacement)?;
        self.materialized = replacement;
        Ok(())
    }

    pub fn snapshot(&self) -> ManagedSessionSnapshot {
        ManagedSessionSnapshot {
            materialized: self.materialized.clone(),
            operational: self.operational.clone(),
            latest_auth_failure_ordinal: self.latest_auth_failure_ordinal,
        }
    }

    pub async fn submit(&mut self, command_id: String, command: RelayCommand) -> Result<u64> {
        let ordinal = self.client.submit(command_id, command).await?;
        self.sync_in_place().await?;
        Ok(ordinal)
    }

    async fn apply_event_page(&mut self, page: RelayEventPage) -> Result<RelayCursor> {
        let mut delivered_through = self.materialized.applied_event_ordinal;
        let mut delivered_digest = self.materialized.applied_event_digest.clone();
        for event in &page.events {
            validate_relay_event(delivered_through, &delivered_digest, event)?;
            delivered_through = event.ordinal;
            delivered_digest.clone_from(&event.digest);
            let projected = project_relay_event(&self.materialized, event)?;
            match apply_projection_event(
                &self.materialized.session_id,
                event.ordinal,
                &event.previous_digest,
                &event.digest,
                &projected.mutation,
            )? {
                ProjectionApplyOutcome::Applied => {
                    apply_committed_projection_event(
                        &mut self.materialized,
                        event,
                        &projected.mutation,
                    )?;
                    if relay_event_reports_auth_failure(event) {
                        self.latest_auth_failure_ordinal = Some(event.ordinal);
                    }
                }
                ProjectionApplyOutcome::AlreadyApplied => {
                    bail!(
                        "session actor received relay event {} after its projection had already applied it",
                        event.ordinal
                    );
                }
            }
        }
        ensure!(
            delivered_through == page.through_ordinal,
            "relay page claimed frontier {} but delivered through {delivered_through}",
            page.through_ordinal
        );
        ensure!(
            delivered_digest == page.through_digest,
            "relay page digest does not match its claimed frontier"
        );
        ensure!(
            self.materialized.applied_event_ordinal == page.through_ordinal,
            "controller projection frontier {} does not match committed relay page {}",
            self.materialized.applied_event_ordinal,
            page.through_ordinal,
        );
        if page.through_ordinal > 0 {
            let acknowledged = self
                .client
                .acknowledge(page.through_ordinal, &page.through_digest)
                .await?;
            ensure!(
                acknowledged.ordinal == page.through_ordinal
                    && acknowledged.digest == page.through_digest,
                "relay acknowledged cursor {}:{} instead of {}:{}",
                acknowledged.ordinal,
                acknowledged.digest,
                page.through_ordinal,
                page.through_digest,
            );
        }
        Ok(RelayCursor {
            ordinal: delivered_through,
            digest: delivered_digest,
        })
    }
}

fn relay_desynchronized(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<RelayRejected>()
            .is_some_and(RelayRejected::is_desynchronized)
    })
}

pub fn new_command_id(prefix: &str) -> Result<String> {
    ensure!(!prefix.trim().is_empty(), "command ID prefix is required");
    let mut random = [0_u8; 16];
    getrandom::fill(&mut random)
        .map_err(|error| anyhow::anyhow!("generate command ID: {error}"))?;
    Ok(format!("{prefix}-{}", hex(&random)))
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(program: &str) -> RelaySessionTarget {
        RelaySessionTarget {
            session_id: "session-1".to_owned(),
            spec: CommandSpec::new(program, std::iter::empty::<&str>()),
        }
    }

    #[test]
    fn command_ids_are_namespaced_and_unique() {
        let first = new_command_id("prompt").unwrap();
        let second = new_command_id("prompt").unwrap();
        assert!(first.starts_with("prompt-"));
        assert_ne!(first, second);
    }

    #[test]
    fn leased_actor_defers_replacement_and_uses_latest_queued_target() {
        let original = target("relay-v1");
        let intermediate = target("relay-v2");
        let latest = target("relay-v3");
        let mut lifecycle = ActorLifecycle::default();
        lifecycle.activate_lease(7);

        assert_eq!(
            reconcile_action(Some(&original), Some(&intermediate)),
            ReconcileAction::Retire
        );
        lifecycle.set_retirement_requested(true);
        assert!(!lifecycle.accepts_new_work());
        assert!(!lifecycle.should_stop());

        assert_eq!(
            reconcile_action(Some(&original), Some(&latest)),
            ReconcileAction::Retire
        );
        assert!(lifecycle.return_lease(7));
        assert!(lifecycle.should_stop());

        assert_eq!(
            reconcile_action(None, Some(&latest)),
            ReconcileAction::Spawn
        );
    }

    #[test]
    fn leased_actor_defers_removal_until_its_connection_returns() {
        let original = target("relay-v1");
        let mut lifecycle = ActorLifecycle::default();
        lifecycle.activate_lease(11);

        assert_eq!(
            reconcile_action(Some(&original), None),
            ReconcileAction::Retire
        );
        lifecycle.set_retirement_requested(true);
        assert!(!lifecycle.should_stop());
        assert!(!lifecycle.return_lease(10));
        assert!(!lifecycle.should_stop());
        assert!(lifecycle.return_lease(11));
        assert!(lifecycle.should_stop());
        assert_eq!(reconcile_action(None, None), ReconcileAction::Idle);
    }

    #[test]
    fn queued_change_back_to_current_target_cancels_retirement() {
        let original = target("relay-v1");
        let replacement = target("relay-v2");
        let mut lifecycle = ActorLifecycle::default();
        lifecycle.activate_lease(3);

        assert_eq!(
            reconcile_action(Some(&original), Some(&replacement)),
            ReconcileAction::Retire
        );
        lifecycle.set_retirement_requested(true);
        assert_eq!(
            reconcile_action(Some(&original), Some(&original)),
            ReconcileAction::Keep
        );
        lifecycle.set_retirement_requested(false);

        assert!(lifecycle.return_lease(3));
        assert!(!lifecycle.should_stop());
        assert!(lifecycle.accepts_new_work());
    }

    #[test]
    fn dashboard_updates_keep_only_the_latest_view_per_session() {
        let (sender, mut receiver) = coalesced_update_channel();
        for revision in 0..1_000 {
            sender.send(SessionManagerUpdate {
                session_id: "session-1".into(),
                view: ManagedSessionView {
                    error: Some(format!("revision-{revision}")),
                    ..ManagedSessionView::default()
                },
            });
        }
        sender.send(SessionManagerUpdate {
            session_id: "session-2".into(),
            view: ManagedSessionView {
                error: Some("other".into()),
                ..ManagedSessionView::default()
            },
        });

        assert_eq!(
            sender
                .pending
                .lock()
                .expect("session update coalescer poisoned")
                .len(),
            2
        );
        let updates = [receiver.try_recv().unwrap(), receiver.try_recv().unwrap()]
            .into_iter()
            .map(|update| (update.session_id, update.view.error.unwrap()))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(updates["session-1"], "revision-999");
        assert_eq!(updates["session-2"], "other");
        assert!(receiver.try_recv().is_err());
    }
}
