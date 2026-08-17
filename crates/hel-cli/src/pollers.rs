//! Background feeds for the control surfaces.
//!
//! Everything here runs off the event loop and reports back over a channel:
//! harness quota refreshes, worker session polling, per-session resource and
//! deployment capacity probes, credential-sync scheduling, and the one-shot
//! tasks that recover interrupted closes. The loop that consumes them never
//! blocks; see [`Feed`] for the wait-then-drain shape they all share.

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use hel::clock::epoch_seconds;
use hel::hel_config::HelConfig;
use hel::hel_controller::Controller;
use hel::hel_credentials::{CredentialSyncHandle, CredentialSyncTarget};
use hel::hel_quota::{ProfileQuota, QuotaManager, QuotaRefreshRequest};
use hel::hel_recovery::{RecoveryCoordinator, RecoveryResult};
use hel::hel_session_manager::{
    RelaySessionTarget, SessionManagerControl, SessionManagerUpdate, SessionManagerUpdates,
    ViewError, spawn_session_manager,
};
use hel::hel_state::{HelState, MaterializedSession, SessionResourceAllocation, SessionState};
use hel::hel_targets::{
    CancellableProcessExecutor, CommandOutput, CommandSpec, DeploymentCapacityKind,
    DeploymentCapacityTarget, DeploymentCapacityUsage, ProcessExecutor, SessionResourceProbe,
    SessionResourceUsage,
};
use hel::hel_worker_client::CredentialSyncCoordinator;
use hel_tui::DashboardState;

use crate::dashboard::io::DashboardIoUpdate;
use crate::short_id;

const QUOTA_REFRESH_INTERVAL: Duration = Duration::from_secs(60 * 60);
pub(crate) const RESOURCE_POLL_INTERVAL: Duration = Duration::from_secs(60);
const RESOURCE_POLL_TIMEOUT: Duration = Duration::from_secs(15);
pub(crate) const CAPACITY_POLL_INTERVAL: Duration = Duration::from_secs(30);
const WORKER_DIAGNOSIS_TIMEOUT: Duration = Duration::from_secs(15);

/// Something a control loop waits on and then drains: one awaited receive for
/// the `select!` arm, and a non-blocking receive for the batch that follows.
pub(crate) trait FeedSource {
    type Item;

    /// Cancel-safe: a wait that loses the race must not drop a message.
    fn wait(&mut self) -> impl Future<Output = Option<Self::Item>>;

    fn poll_now(&mut self) -> Option<Self::Item>;
}

impl<T> FeedSource for tokio::sync::mpsc::Receiver<T> {
    type Item = T;

    fn wait(&mut self) -> impl Future<Output = Option<T>> {
        self.recv()
    }

    fn poll_now(&mut self) -> Option<T> {
        self.try_recv().ok()
    }
}

impl<T> FeedSource for tokio::sync::mpsc::UnboundedReceiver<T> {
    type Item = T;

    fn wait(&mut self) -> impl Future<Output = Option<T>> {
        self.recv()
    }

    fn poll_now(&mut self) -> Option<T> {
        self.try_recv().ok()
    }
}

impl FeedSource for SessionManagerUpdates {
    type Item = SessionManagerUpdate;

    fn wait(&mut self) -> impl Future<Output = Option<SessionManagerUpdate>> {
        self.recv()
    }

    fn poll_now(&mut self) -> Option<SessionManagerUpdate> {
        self.try_recv().ok()
    }
}

impl FeedSource for RecoveryCoordinator {
    type Item = RecoveryResult;

    fn wait(&mut self) -> impl Future<Output = Option<RecoveryResult>> {
        self.result()
    }

    fn poll_now(&mut self) -> Option<RecoveryResult> {
        self.try_result()
    }
}

impl FeedSource for CredentialSyncCoordinator {
    type Item = hel::hel_credentials::CredentialSyncResult;

    fn wait(&mut self) -> impl Future<Output = Option<Self::Item>> {
        self.result()
    }

    fn poll_now(&mut self) -> Option<Self::Item> {
        self.try_result()
    }
}

/// One background feed as a control loop uses it.
///
/// The `select!` arm hands the message that woke the loop to [`Feed::accept`],
/// and the drain that follows walks [`Feed::next_ready`] until the feed is
/// empty, so a burst of updates costs one draw. A closed channel reports `None`
/// for ever, which would leave its arm permanently ready; `accept` retires the
/// feed instead, and [`Feed::is_open`] gates the arm.
pub(crate) struct Feed<S: FeedSource> {
    source: S,
    pending: Option<S::Item>,
    open: bool,
}

impl<S: FeedSource> Feed<S> {
    pub(crate) fn new(source: S) -> Self {
        Self {
            source,
            pending: None,
            open: true,
        }
    }

    pub(crate) fn is_open(&self) -> bool {
        self.open
    }

    pub(crate) fn wait(&mut self) -> impl Future<Output = Option<S::Item>> {
        self.source.wait()
    }

    /// Latches the message that won the select and reports whether the loop
    /// must redraw.
    pub(crate) fn accept(&mut self, message: Option<S::Item>) -> bool {
        match message {
            Some(message) => {
                self.pending = Some(message);
                true
            }
            None => {
                self.open = false;
                false
            }
        }
    }

    /// The next message for the batch drain: the one that won the select
    /// first, then whatever queued behind it.
    pub(crate) fn next_ready(&mut self) -> Option<S::Item> {
        self.pending.take().or_else(|| self.source.poll_now())
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct QuotaRefreshBatch {
    pub(crate) generation: u64,
    pub(crate) profiles: Vec<QuotaRefreshRequest>,
}

#[derive(Debug)]
pub(crate) enum QuotaUpdate {
    Refreshing { profile_ids: Vec<String> },
    Report(ProfileQuota),
    Finished { generation: u64 },
}

pub(crate) type WorkerPollTarget = RelaySessionTarget;
pub(crate) type WorkerPollUpdate = SessionManagerUpdate;

#[derive(Debug)]
struct WorkerDiagnosisEpisode {
    id: u64,
    error: String,
    diagnosed: bool,
}

#[derive(Debug, Default)]
pub(crate) struct WorkerDiagnosisTracker {
    next_episode: u64,
    current: std::collections::BTreeMap<String, WorkerDiagnosisEpisode>,
    pending: std::collections::BTreeMap<String, u64>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct WorkerDiagnosisCompletion {
    pub(crate) display_error: Option<String>,
    pub(crate) restart_episode: Option<u64>,
}

impl WorkerDiagnosisTracker {
    pub(crate) fn observe(
        &mut self,
        session_id: &str,
        connected: bool,
        error: Option<String>,
    ) -> Option<u64> {
        if connected {
            self.current.remove(session_id);
        }
        let error = error?;
        let episode = self
            .current
            .entry(session_id.to_owned())
            .or_insert_with(|| {
                self.next_episode = self.next_episode.wrapping_add(1).max(1);
                WorkerDiagnosisEpisode {
                    id: self.next_episode,
                    error: error.clone(),
                    diagnosed: false,
                }
            });
        episode.error = error;
        if episode.diagnosed || self.pending.contains_key(session_id) {
            return None;
        }
        self.pending.insert(session_id.to_owned(), episode.id);
        Some(episode.id)
    }

    pub(crate) fn finish(
        &mut self,
        session_id: &str,
        episode_id: u64,
    ) -> WorkerDiagnosisCompletion {
        if self.pending.get(session_id) != Some(&episode_id) {
            return WorkerDiagnosisCompletion::default();
        }
        self.pending.remove(session_id);
        let Some(current) = self.current.get_mut(session_id) else {
            return WorkerDiagnosisCompletion::default();
        };
        if current.id == episode_id {
            current.diagnosed = true;
            return WorkerDiagnosisCompletion {
                display_error: Some(current.error.clone()),
                restart_episode: None,
            };
        }
        if !current.diagnosed {
            self.pending.insert(session_id.to_owned(), current.id);
            return WorkerDiagnosisCompletion {
                display_error: None,
                restart_episode: Some(current.id),
            };
        }
        WorkerDiagnosisCompletion::default()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ResourcePollTarget {
    session_id: String,
    probe: SessionResourceProbe,
}

#[derive(Debug)]
pub(crate) struct ResourcePollUpdate {
    pub(crate) session_id: String,
    pub(crate) usage: SessionResourceUsage,
}

#[derive(Debug)]
pub(crate) struct CapacityPollUpdate {
    pub(crate) target_id: String,
    pub(crate) result: std::result::Result<Option<DeploymentCapacityUsage>, String>,
    pub(crate) sampled_at_epoch_seconds: u64,
}

pub(crate) fn projected_queued_prompts(
    controller: &Controller,
) -> Result<std::collections::BTreeMap<String, Vec<hel::hel_worker::QueuedPrompt>>> {
    let queues = hel::hel_database::load_materialized_queued_prompts()?;
    Ok(controller
        .state
        .sessions
        .keys()
        .filter_map(|session_id| {
            queues
                .get(session_id)
                .map(|queue| (session_id.clone(), queued_prompt_entries(queue)))
        })
        .collect())
}

pub(crate) fn quota_refresh_profiles(controller: &Controller) -> Vec<QuotaRefreshRequest> {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    controller
        .config
        .profiles
        .iter()
        .map(|(id, profile)| {
            let mut environment = profile.environment.clone();
            environment.insert(
                profile.home_env().to_string(),
                profile.home.to_string_lossy().into_owned(),
            );
            QuotaRefreshRequest {
                profile_id: id.clone(),
                harness: profile.kind,
                source_home: profile.home.clone(),
                executable: profile.executable.clone(),
                environment,
                cwd: cwd.clone(),
            }
        })
        .collect()
}

pub(crate) fn spawn_dashboard_quota_refresher() -> (
    tokio::sync::watch::Sender<QuotaRefreshBatch>,
    tokio::sync::mpsc::Receiver<QuotaUpdate>,
) {
    let (profiles_tx, mut profiles_rx) = tokio::sync::watch::channel(QuotaRefreshBatch::default());
    let (updates_tx, updates_rx) = tokio::sync::mpsc::channel(32);
    tokio::spawn(async move {
        let mut quotas = QuotaManager::default();
        let mut batch = QuotaRefreshBatch::default();
        let mut interval = tokio::time::interval(QUOTA_REFRESH_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;
        loop {
            tokio::select! {
                _ = interval.tick(), if !batch.profiles.is_empty() => {
                    if !refresh_profile_quotas(
                        &mut quotas,
                        batch.generation,
                        &batch.profiles,
                        &updates_tx,
                    ).await {
                        break;
                    }
                }
                changed = profiles_rx.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    batch = profiles_rx.borrow_and_update().clone();
                    if !refresh_profile_quotas(
                        &mut quotas,
                        batch.generation,
                        &batch.profiles,
                        &updates_tx,
                    ).await {
                        break;
                    }
                }
            }
        }
        quotas.shutdown().await;
    });
    (profiles_tx, updates_rx)
}

async fn refresh_profile_quotas(
    quotas: &mut QuotaManager,
    generation: u64,
    profiles: &[QuotaRefreshRequest],
    updates: &tokio::sync::mpsc::Sender<QuotaUpdate>,
) -> bool {
    let ids = profiles
        .iter()
        .map(|profile| profile.profile_id.clone())
        .collect::<Vec<_>>();
    if updates
        .send(QuotaUpdate::Refreshing { profile_ids: ids })
        .await
        .is_err()
    {
        return false;
    }
    // Keep draining even if the UI is gone so codex clients return to the
    // manager for a clean shutdown; just stop sending.
    let delivered = AtomicBool::new(true);
    quotas
        .refresh_profiles(profiles.to_vec(), |quota| {
            let delivered = &delivered;
            async move {
                if delivered.load(Ordering::Acquire)
                    && updates.send(QuotaUpdate::Report(quota)).await.is_err()
                {
                    delivered.store(false, Ordering::Release);
                }
            }
        })
        .await;
    if !delivered.into_inner() {
        return false;
    }
    updates
        .send(QuotaUpdate::Finished { generation })
        .await
        .is_ok()
}

pub(crate) fn complete_manual_quota_refresh(
    pending_generation: &mut Option<u64>,
    completed_generation: u64,
) -> bool {
    if *pending_generation != Some(completed_generation) {
        return false;
    }
    *pending_generation = None;
    true
}

pub(crate) fn dashboard_worker_targets(controller: &Controller) -> Vec<WorkerPollTarget> {
    controller
        .state
        .sessions
        .values()
        .filter(|session| session.state.is_active() && session.target.is_some())
        .filter_map(|session| {
            controller
                .reconnect_command(&session.id)
                .ok()
                .map(|spec| WorkerPollTarget {
                    session_id: session.id.clone(),
                    spec,
                })
        })
        .collect()
}

/// Sessions whose worker can answer credential requests right now. Sessions
/// still provisioning or already disconnected would only produce connection
/// errors, so they stay out.
pub(crate) fn credential_sync_targets(controller: &Controller) -> Vec<CredentialSyncTarget> {
    controller
        .state
        .sessions
        .values()
        .filter(|session| {
            matches!(
                session.state,
                SessionState::Running | SessionState::Checkpointing
            ) && session.target.is_some()
        })
        .filter_map(|session| {
            let profile = controller.config.profiles.get(&session.last_profile)?;
            let spec = controller.reconnect_command(&session.id).ok()?;
            Some(CredentialSyncTarget {
                session_id: session.id.clone(),
                profile_id: session.last_profile.clone(),
                harness: profile.kind,
                profile_home: profile.home.clone(),
                spec,
            })
        })
        .collect()
}

/// One automatic sync and notice per session per cooldown, so a harness that
/// fails authentication on every retry does not flood the UI.
pub(crate) const AUTH_FAILURE_SYNC_COOLDOWN: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingAuthFailure {
    ordinal: u64,
    profile_id: String,
}

/// Deduplicates the actor's sticky failure marker while retaining a newer
/// failure until its session cooldown expires.
#[derive(Debug, Default)]
pub(crate) struct AuthFailureSyncTracker {
    handled_ordinals: std::collections::BTreeMap<String, u64>,
    last_attempts: std::collections::BTreeMap<String, Instant>,
    pending: std::collections::BTreeMap<String, PendingAuthFailure>,
}

impl AuthFailureSyncTracker {
    pub(crate) fn observe(&mut self, session_id: &str, profile_id: &str, ordinal: u64) {
        if self
            .handled_ordinals
            .get(session_id)
            .is_some_and(|handled| *handled >= ordinal)
        {
            return;
        }
        let pending = PendingAuthFailure {
            ordinal,
            profile_id: profile_id.to_owned(),
        };
        match self.pending.entry(session_id.to_owned()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(pending);
            }
            std::collections::btree_map::Entry::Occupied(mut entry)
                if entry.get().ordinal <= ordinal =>
            {
                entry.insert(pending);
            }
            std::collections::btree_map::Entry::Occupied(_) => {}
        }
    }

    fn drain_due(&mut self, now: Instant) -> Vec<(String, String)> {
        let due = self
            .pending
            .keys()
            .filter(|session_id| {
                self.last_attempts.get(*session_id).is_none_or(|previous| {
                    now.saturating_duration_since(*previous) >= AUTH_FAILURE_SYNC_COOLDOWN
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        due.into_iter()
            .map(|session_id| {
                let pending = self
                    .pending
                    .remove(&session_id)
                    .expect("due authentication failure disappeared");
                self.handled_ordinals
                    .insert(session_id.clone(), pending.ordinal);
                self.last_attempts.insert(session_id.clone(), now);
                (session_id, pending.profile_id)
            })
            .collect()
    }
}

pub(crate) fn schedule_due_auth_failure_syncs(
    tracker: &mut AuthFailureSyncTracker,
    credential_sync: &CredentialSyncHandle,
    now: Instant,
) {
    for (session_id, profile_id) in tracker.drain_due(now) {
        credential_sync.sync_profile_now(&profile_id, Some(&session_id));
    }
}

/// Turns finished credential syncs into UI notices.
///
/// The periodic cycle revisits every profile, so a session that keeps failing
/// the same way would post the same notice forever. The last failure message
/// per key is remembered and only a changed one speaks up again. Keys are the
/// profile for a whole-sync failure and the profile plus session for a
/// per-session failure.
#[derive(Debug, Default)]
pub(crate) struct CredentialSyncNotices {
    last_failures: std::collections::BTreeMap<(String, Option<String>), String>,
}

impl CredentialSyncNotices {
    /// Healthy no-op cycles stay out of the UI; only actions, new failures, and
    /// answers to an authentication failure are worth a notice.
    pub(crate) fn notice(
        &mut self,
        result: &hel::hel_credentials::CredentialSyncResult,
    ) -> Option<String> {
        // Authentication-triggered syncs always speak: the upstream per-session
        // cooldown, not this dedup, is what keeps them rare.
        if let Some(session_id) = &result.triggered_by {
            return Some(if result.pushed_to(session_id) {
                format!(
                    "Session {} hit an authentication failure; refreshed credentials were pushed. Retry the prompt, and if it repeats run `hel login --profile {}`.",
                    short_id(session_id),
                    result.profile_id
                )
            } else {
                format!(
                    "Session {} hit an authentication failure and Hel has nothing fresher to push. Run `hel login --profile {}`.",
                    short_id(session_id),
                    result.profile_id
                )
            });
        }

        let mut failures = std::collections::BTreeMap::new();
        if let Some(detail) = &result.failure {
            failures.insert(
                (result.profile_id.clone(), None),
                format!(
                    "Credential sync for profile {} failed: {detail}",
                    result.profile_id
                ),
            );
        }
        for (session_id, detail) in result.failures() {
            failures.insert(
                (result.profile_id.clone(), Some(session_id.to_owned())),
                format!(
                    "Credential sync for {} failed: {detail}",
                    short_id(session_id)
                ),
            );
        }
        // A key that stopped failing is forgotten silently, so the same failure
        // after a clean cycle is reported again.
        self.last_failures
            .retain(|key, _| key.0 != result.profile_id || failures.contains_key(key));
        let mut notice = None;
        for (key, message) in failures {
            if self.last_failures.get(&key) != Some(&message) {
                notice.get_or_insert_with(|| message.clone());
            }
            self.last_failures.insert(key, message);
        }
        if notice.is_some() {
            return notice;
        }

        let mut parts = Vec::new();
        let credentials = result.credential_sessions();
        if credentials > 0 {
            parts.push(format!(
                "Refreshed harness credentials for profile {} across {credentials} session(s).",
                result.profile_id
            ));
        }
        let skills = result.skills_sessions();
        if skills > 0 {
            parts.push(format!(
                "Synced skills for profile {} to {skills} session(s).",
                result.profile_id
            ));
        }
        (!parts.is_empty()).then(|| parts.join(" "))
    }
}

fn dashboard_resource_targets(controller: &Controller) -> Vec<ResourcePollTarget> {
    controller
        .state
        .sessions
        .values()
        .filter(|session| session.state.is_active() && session.target.is_some())
        .filter_map(|session| {
            controller
                .resource_probe(&session.id)
                .ok()
                .map(|probe| ResourcePollTarget {
                    session_id: session.id.clone(),
                    probe,
                })
        })
        .collect()
}

pub(crate) fn refresh_dashboard_poll_targets(
    controller: &Controller,
    worker_targets_tx: &tokio::sync::watch::Sender<Vec<WorkerPollTarget>>,
    resource_targets_tx: &tokio::sync::watch::Sender<Vec<ResourcePollTarget>>,
    credential_sync: &CredentialSyncHandle,
    excluded_sessions: &std::collections::BTreeSet<String>,
) {
    let mut worker_targets = dashboard_worker_targets(controller);
    worker_targets.retain(|target| !excluded_sessions.contains(&target.session_id));
    worker_targets_tx.send_replace(worker_targets);
    let mut resource_targets = dashboard_resource_targets(controller);
    resource_targets.retain(|target| !excluded_sessions.contains(&target.session_id));
    resource_targets_tx.send_replace(resource_targets);
    let mut credential_targets = credential_sync_targets(controller);
    credential_targets.retain(|target| !excluded_sessions.contains(&target.session_id));
    credential_sync.set_targets(credential_targets);
}

pub(crate) fn spawn_aws_resource_options_resolution(
    config: HelConfig,
    target_id: String,
    updates: tokio::sync::mpsc::UnboundedSender<(
        String,
        std::result::Result<Vec<SessionResourceAllocation>, String>,
    )>,
) {
    let _task = tokio::task::spawn_blocking(move || {
        let controller = Controller {
            config,
            state: HelState::default(),
        };
        let result = controller
            .resolve_aws_resource_options(&target_id, &ProcessExecutor)
            .map_err(|error| format!("{error:#}"));
        let _ = updates.send((target_id, result));
    });
}

pub(crate) fn spawn_dashboard_resource_poller() -> (
    tokio::sync::watch::Sender<Vec<ResourcePollTarget>>,
    tokio::sync::mpsc::Sender<String>,
    tokio::sync::mpsc::Receiver<ResourcePollUpdate>,
) {
    let (targets_tx, mut targets_rx) =
        tokio::sync::watch::channel(Vec::<ResourcePollTarget>::new());
    let (triggers_tx, mut triggers_rx) = tokio::sync::mpsc::channel(64);
    let (updates_tx, updates_rx) = tokio::sync::mpsc::channel(64);
    tokio::spawn(async move {
        let mut targets = std::collections::BTreeMap::new();
        let mut last_started = std::collections::BTreeMap::new();
        let mut interval = tokio::time::interval(RESOURCE_POLL_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let due = targets.values().cloned().collect::<Vec<_>>();
                    for target in due {
                        schedule_resource_sample(target, &mut last_started, &updates_tx);
                    }
                }
                changed = targets_rx.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    targets = targets_rx
                        .borrow_and_update()
                        .iter()
                        .cloned()
                        .map(|target| (target.session_id.clone(), target))
                        .collect();
                    last_started.retain(|session_id, _| targets.contains_key(session_id));
                    let due = targets.values().cloned().collect::<Vec<_>>();
                    for target in due {
                        schedule_resource_sample(target, &mut last_started, &updates_tx);
                    }
                }
                session_id = triggers_rx.recv() => {
                    let Some(session_id) = session_id else {
                        break;
                    };
                    if let Some(target) = targets.get(&session_id).cloned() {
                        schedule_resource_sample(target, &mut last_started, &updates_tx);
                    }
                }
            }
        }
    });
    (targets_tx, triggers_tx, updates_rx)
}

fn resource_sample_is_due(
    last_started: Option<&tokio::time::Instant>,
    now: tokio::time::Instant,
) -> bool {
    last_started.is_none_or(|started| now.duration_since(*started) >= RESOURCE_POLL_INTERVAL)
}

fn schedule_resource_sample(
    target: ResourcePollTarget,
    last_started: &mut std::collections::BTreeMap<String, tokio::time::Instant>,
    updates: &tokio::sync::mpsc::Sender<ResourcePollUpdate>,
) {
    let now = tokio::time::Instant::now();
    if !resource_sample_is_due(last_started.get(&target.session_id), now) {
        return;
    }
    last_started.insert(target.session_id.clone(), now);
    let updates = updates.clone();
    tokio::spawn(async move {
        let usage = tokio::time::timeout(
            RESOURCE_POLL_TIMEOUT,
            collect_session_resource_usage(&target.probe),
        )
        .await
        .ok()
        .and_then(Result::ok);
        let Some(usage) = usage else {
            return;
        };
        let _ = updates
            .send(ResourcePollUpdate {
                session_id: target.session_id,
                usage,
            })
            .await;
    });
}

async fn collect_session_resource_usage(
    probe: &SessionResourceProbe,
) -> Result<SessionResourceUsage> {
    let memory = execute_resource_command(&probe.memory).await?;
    let disk = match &probe.disk {
        Some(command) => execute_resource_command(command).await.ok(),
        None => None,
    };
    hel::hel_targets::parse_resource_usage(
        &memory.stdout,
        disk.as_ref().map(|output| output.stdout.as_slice()),
    )
}

pub(crate) fn spawn_dashboard_capacity_poller() -> (
    tokio::sync::watch::Sender<Vec<DeploymentCapacityTarget>>,
    tokio::sync::mpsc::Receiver<CapacityPollUpdate>,
) {
    let (targets_tx, mut targets_rx) =
        tokio::sync::watch::channel(Vec::<DeploymentCapacityTarget>::new());
    let (updates_tx, updates_rx) = tokio::sync::mpsc::channel(64);
    tokio::spawn(async move {
        let mut targets = Vec::new();
        let mut interval = tokio::time::interval(CAPACITY_POLL_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    schedule_capacity_samples(&targets, &updates_tx);
                }
                changed = targets_rx.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    targets = targets_rx.borrow_and_update().clone();
                    schedule_capacity_samples(&targets, &updates_tx);
                }
            }
        }
    });
    (targets_tx, updates_rx)
}

fn schedule_capacity_samples(
    targets: &[DeploymentCapacityTarget],
    updates: &tokio::sync::mpsc::Sender<CapacityPollUpdate>,
) {
    for target in targets.iter().cloned() {
        let updates = updates.clone();
        tokio::spawn(async move {
            let result = tokio::time::timeout(RESOURCE_POLL_TIMEOUT, collect_capacity(&target))
                .await
                .map_err(|_| "capacity probe timed out".to_string())
                .and_then(|result| result.map_err(|error| format!("{error:#}")));
            let _ = updates
                .send(CapacityPollUpdate {
                    target_id: target.id,
                    result,
                    sampled_at_epoch_seconds: epoch_seconds(),
                })
                .await;
        });
    }
}

async fn collect_capacity(
    target: &DeploymentCapacityTarget,
) -> Result<Option<DeploymentCapacityUsage>> {
    if let Some(error) = &target.probe_error {
        anyhow::bail!("capacity probe is unavailable: {error}");
    }
    if target.local {
        return tokio::task::spawn_blocking(collect_local_capacity)
            .await
            .context("join local capacity probe")?
            .map(Some);
    }
    match target.kind {
        DeploymentCapacityKind::Host => {
            let mut last_error = None;
            for command in &target.probes {
                match execute_resource_command(command).await {
                    Ok(output) => {
                        return hel::hel_targets::parse_host_capacity(&output.stdout).map(Some);
                    }
                    Err(error) => last_error = Some(error),
                }
            }
            Err(last_error.unwrap_or_else(|| anyhow::anyhow!("no host probe is configured")))
        }
        DeploymentCapacityKind::AwsFleet => {
            if target.probes.is_empty() {
                return Ok(None);
            }
            let mut tasks = tokio::task::JoinSet::new();
            for command in target.probes.clone() {
                tasks.spawn(async move {
                    let output = execute_resource_command(&command).await?;
                    hel::hel_targets::parse_aws_allocated_capacity(&output.stdout)
                });
            }
            let mut usages = Vec::new();
            while let Some(result) = tasks.join_next().await {
                usages.push(result.context("join EC2 capacity probe")??);
            }
            aggregate_aws_capacity(&usages).map(Some)
        }
    }
}

pub(crate) fn aggregate_aws_capacity(
    usages: &[DeploymentCapacityUsage],
) -> Result<DeploymentCapacityUsage> {
    let mut total = DeploymentCapacityUsage {
        cpu_percent: None,
        memory_used_bytes: 0,
        memory_total_bytes: 0,
        logical_cores: 0,
        disk_total_bytes: Some(0),
    };
    for usage in usages {
        total.memory_total_bytes = total
            .memory_total_bytes
            .checked_add(usage.memory_total_bytes)
            .context("aggregate EC2 RAM overflow")?;
        total.logical_cores = total
            .logical_cores
            .checked_add(usage.logical_cores)
            .context("aggregate EC2 core count overflow")?;
        total.disk_total_bytes = Some(
            total
                .disk_total_bytes
                .unwrap_or(0)
                .checked_add(usage.disk_total_bytes.unwrap_or(0))
                .context("aggregate EC2 disk overflow")?,
        );
    }
    Ok(total)
}

fn collect_local_capacity() -> Result<DeploymentCapacityUsage> {
    let mut system = sysinfo::System::new();
    system.refresh_memory();
    system.refresh_cpu_all();
    std::thread::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);
    system.refresh_cpu_usage();
    Ok(DeploymentCapacityUsage {
        cpu_percent: Some(system.global_cpu_usage().round().clamp(0.0, 100.0) as u8),
        memory_used_bytes: system
            .total_memory()
            .saturating_sub(system.available_memory()),
        memory_total_bytes: system.total_memory(),
        logical_cores: system
            .cpus()
            .len()
            .try_into()
            .context("logical CPU count overflow")?,
        disk_total_bytes: None,
    })
}

async fn execute_resource_command(command: &CommandSpec) -> Result<CommandOutput> {
    let mut process = tokio::process::Command::new(&command.program);
    process
        .args(&command.args)
        .envs(&command.env)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let child = process
        .spawn()
        .with_context(|| format!("start {} for {}", command.program, command.purpose))?;
    // stdin is null; nothing writes while output drains, so this cannot hit
    // the write-then-wait deadlock the disallowed_methods lint guards against.
    #[allow(clippy::disallowed_methods)]
    let output = child
        .wait_with_output()
        .await
        .with_context(|| format!("wait for {}", command.purpose))?;
    let command_output = CommandOutput {
        status: output.status.code().unwrap_or(-1),
        stdout: output.stdout,
        stderr: output.stderr,
    };
    if command_output.status != 0 {
        bail!(
            "{} failed with status {}: {}",
            command.purpose,
            command_output.status,
            String::from_utf8_lossy(&command_output.stderr).trim()
        );
    }
    Ok(command_output)
}

pub(crate) fn spawn_dashboard_worker_poller() -> Result<(
    tokio::sync::watch::Sender<Vec<WorkerPollTarget>>,
    SessionManagerUpdates,
    SessionManagerControl,
)> {
    let channels = spawn_session_manager()?;
    Ok((channels.targets, channels.updates, channels.control))
}

pub(crate) fn apply_worker_poll_update(
    controller: &mut Controller,
    dashboard: &mut DashboardState,
    update: WorkerPollUpdate,
    dashboard_io_tx: &tokio::sync::mpsc::UnboundedSender<DashboardIoUpdate>,
) -> Result<bool> {
    if apply_worker_record_update(controller, &update, Some(dashboard_io_tx))? {
        dashboard.set_state(controller.state.clone());
    }
    match update.view.error {
        Some(ViewError::Unreachable(detail)) => {
            dashboard.mark_transcript_unavailable(&update.session_id);
            dashboard.set_notice(format!(
                "Session {}: relay unreachable: {detail}; collecting worker diagnostics…",
                &update.session_id[..update.session_id.len().min(8)]
            ));
        }
        Some(ViewError::ProjectionIntegrity(detail)) => {
            // Deterministic failure: no worker diagnostics, and no
            // "relay unreachable:" last_error, which reconnect handling
            // reserves for genuinely unreachable relays.
            dashboard.mark_transcript_unavailable(&update.session_id);
            dashboard.set_notice(format!(
                "Session {}: transcript projection failed: {detail}",
                &update.session_id[..update.session_id.len().min(8)]
            ));
        }
        None => {}
    }
    Ok(update.view.snapshot.is_some())
}

pub(crate) fn apply_worker_record_update(
    controller: &mut Controller,
    update: &WorkerPollUpdate,
    dashboard_io_tx: Option<&tokio::sync::mpsc::UnboundedSender<DashboardIoUpdate>>,
) -> Result<bool> {
    let Some(snapshot) = update.view.snapshot.as_ref() else {
        return Ok(false);
    };
    let Some(session) = controller.state.sessions.get(&update.session_id) else {
        return Ok(false);
    };
    let changed_title = (session.acp_session_title != snapshot.materialized.session_title)
        .then(|| snapshot.materialized.session_title.clone());
    let reconnect_observed = update.view.connected
        && session.state == SessionState::Error
        && session
            .last_error
            .as_deref()
            .is_some_and(|message| message.starts_with("relay unreachable:"));
    let mut changed = false;
    if let Some(title) = changed_title {
        if dashboard_io_tx.is_none() {
            hel::hel_database::set_session_acp_title(&update.session_id, title.as_deref())?;
        }
        controller
            .state
            .sessions
            .get_mut(&update.session_id)
            .expect("session disappeared while updating its ACP title")
            .acp_session_title = title;
        if let Some(dashboard_io_tx) = dashboard_io_tx {
            spawn_worker_record_persistence(
                update.session_id.clone(),
                WorkerRecordPersistence::AcpTitle {
                    title: snapshot.materialized.session_title.clone(),
                },
                dashboard_io_tx.clone(),
            );
        }
        changed = true;
    }
    let reconnect_applies = if dashboard_io_tx.is_some() {
        reconnect_observed
    } else {
        reconnect_observed && hel::hel_database::mark_session_relay_reconnected(&update.session_id)?
    };
    if reconnect_applies {
        let session = controller
            .state
            .sessions
            .get_mut(&update.session_id)
            .expect("session disappeared while recording relay reconnection");
        session.state = SessionState::Running;
        session.last_error = None;
        if let Some(dashboard_io_tx) = dashboard_io_tx {
            spawn_worker_record_persistence(
                update.session_id.clone(),
                WorkerRecordPersistence::RelayReconnect,
                dashboard_io_tx.clone(),
            );
        }
        changed = true;
    }
    Ok(changed)
}

pub(crate) enum WorkerRecordPersistence {
    AcpTitle { title: Option<String> },
    RelayReconnect,
}

fn spawn_worker_record_persistence(
    session_id: String,
    operation: WorkerRecordPersistence,
    updates: tokio::sync::mpsc::UnboundedSender<DashboardIoUpdate>,
) {
    tokio::task::spawn_blocking(move || {
        let result = match &operation {
            WorkerRecordPersistence::AcpTitle { title } => {
                hel::hel_database::set_session_acp_title(&session_id, title.as_deref())
            }
            WorkerRecordPersistence::RelayReconnect => {
                hel::hel_database::mark_session_relay_reconnected(&session_id).map(|_| ())
            }
        }
        .map_err(|error| format!("{error:#}"));
        let _ = updates.send(DashboardIoUpdate::WorkerRecordPersistence {
            session_id,
            operation,
            result,
        });
    });
}

pub(crate) fn spawn_worker_diagnosis(
    controller: &Controller,
    session_id: String,
    episode_id: u64,
    updates: tokio::sync::mpsc::UnboundedSender<DashboardIoUpdate>,
) {
    let diagnostic_controller = Controller {
        config: controller.config.clone(),
        state: controller.state.clone(),
    };
    tokio::spawn(async move {
        let task_session_id = session_id.clone();
        let joined = tokio::task::spawn_blocking(move || {
            let executor = CancellableProcessExecutor::with_timeout(WORKER_DIAGNOSIS_TIMEOUT);
            diagnostic_controller.diagnose_worker_controlled(&task_session_id, &executor)
        })
        .await;
        let result = joined.map_err(|error| format!("worker diagnosis task failed: {error}"));
        if updates
            .send(DashboardIoUpdate::WorkerDiagnosis {
                session_id: session_id.clone(),
                episode_id,
                result,
            })
            .is_err()
        {
            tracing::debug!(%session_id, "worker diagnosis finished after the dashboard stopped");
        }
    });
}

pub(crate) fn queued_prompt_projection(
    session: &MaterializedSession,
) -> Vec<hel::hel_worker::QueuedPrompt> {
    queued_prompt_entries(&session.queued_prompts)
}

fn queued_prompt_entries(
    prompts: &[hel::hel_state::MaterializedQueuedPrompt],
) -> Vec<hel::hel_worker::QueuedPrompt> {
    prompts
        .iter()
        .map(|prompt| hel::hel_worker::QueuedPrompt {
            id: prompt.command_id.clone(),
            text: hel::hel_chat::materialized_content_text(&prompt.content),
            attachments: Vec::new(),
            created_at_ms: prompt.queued_at_ms,
        })
        .collect()
}

pub(crate) fn merge_recovery_result(
    controller: &mut Controller,
    result: hel::hel_recovery::RecoveryResult,
) -> bool {
    let hel::hel_recovery::RecoveryResult {
        session_id,
        expected_target,
        outcome,
        cancelled,
    } = result;
    if let Err(error) = controller.reload() {
        tracing::warn!(%session_id, "could not reload a completed recovery checkpoint: {error:#}");
        return false;
    }
    let Some(session) = controller.state.sessions.get_mut(&session_id) else {
        return false;
    };
    if session.target.as_ref() != Some(&expected_target) || !session.state.is_active() {
        return false;
    }
    match outcome {
        Ok(artifact) => {
            if session.checkpoint.as_ref() != Some(&artifact.metadata) {
                tracing::warn!(
                    %session_id,
                    "recovery checkpoint result no longer matches the durable session record; retaining both archives"
                );
                return false;
            }
        }
        Err(_) if cancelled => {
            // A preempted copy was never judged, so it must not leave a
            // checkpoint error on the session.
            return false;
        }
        Err(detail) => {
            // record_recovery_failure normally made this durable before the
            // result was published. Preserve the diagnostic in this view if
            // that write itself failed; later reloads remain authoritative.
            session.last_checkpoint_error = Some(detail);
        }
    }
    true
}

pub(crate) enum LifecycleSuccess {
    Created,
    Resumed {
        profile_id: String,
        target_id: String,
        materialized: Box<MaterializedSession>,
    },
    Closed,
    Destroyed,
    DeletedActive,
    DeletedArchived,
}

pub(crate) struct LifecycleUpdate {
    pub(crate) session_id: String,
    pub(crate) result: std::result::Result<LifecycleSuccess, String>,
}

pub(crate) fn interrupted_close_session_ids(controller: &Controller) -> Vec<String> {
    controller
        .state
        .sessions
        .values()
        .filter(|session| {
            matches!(
                session.state,
                SessionState::Closing | SessionState::Destroying
            ) && session.target.is_some()
        })
        .map(|session| session.id.clone())
        .collect()
}

pub(crate) fn spawn_interrupted_close_recovery(
    session_id: String,
    session_manager: SessionManagerControl,
    recovery_observer: hel::hel_state::RecoveryObserver,
    cancelled: Arc<AtomicBool>,
    updates: tokio::sync::mpsc::UnboundedSender<LifecycleUpdate>,
) {
    let runtime = tokio::runtime::Handle::current();
    tokio::spawn(async move {
        let operation_session_id = session_id.clone();
        let joined = tokio::task::spawn_blocking(move || {
            (|| -> Result<()> {
                let _recovery_reservation = reserve_recovery_or_cancel(
                    &recovery_observer,
                    &operation_session_id,
                    &cancelled,
                )?;
                let mut controller = Controller::load()?;
                let executor = CancellableProcessExecutor::new(cancelled);
                runtime.block_on(controller.recover_interrupted_close_managed(
                    &operation_session_id,
                    &executor,
                    &session_manager,
                ))
            })()
            .map(|()| LifecycleSuccess::Closed)
            .map_err(|error| format!("{error:#}"))
        })
        .await;
        let result = match joined {
            Ok(result) => result,
            Err(error) => Err(format!("interrupted close recovery task failed: {error}")),
        };
        if updates
            .send(LifecycleUpdate {
                session_id: session_id.clone(),
                result,
            })
            .is_err()
        {
            tracing::debug!(%session_id, "interrupted close finished after its controller stopped");
        }
    });
}

pub(crate) fn reserve_recovery_or_cancel(
    observer: &hel::hel_state::RecoveryObserver,
    session_id: &str,
    cancelled: &AtomicBool,
) -> Result<hel::hel_state::RecoveryReservation> {
    let reservation = observer.reserve(session_id);
    // The reservation stops the next copy; cancelling preempts the one already
    // running so a lifecycle operation never queues behind a long or wedged
    // copy.
    observer.cancel_busy(session_id);
    while observer.is_busy(session_id) {
        if cancelled.load(Ordering::Acquire) {
            bail!("operation cancelled while waiting for recovery copy");
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    Ok(reservation)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_diagnosis_is_coalesced_for_one_unreachable_episode() {
        let mut tracker = WorkerDiagnosisTracker::default();
        let episode = tracker
            .observe("session-1", false, Some("connection refused".into()))
            .unwrap();

        assert_eq!(
            tracker.observe("session-1", false, Some("still unreachable".into())),
            None
        );
        assert_eq!(
            tracker.finish("session-1", episode),
            WorkerDiagnosisCompletion {
                display_error: Some("still unreachable".into()),
                restart_episode: None,
            }
        );
        assert_eq!(
            tracker.observe("session-1", false, Some("third poll".into())),
            None
        );
    }

    #[test]
    fn stale_worker_diagnosis_is_not_published_after_reconnect() {
        let mut tracker = WorkerDiagnosisTracker::default();
        let first = tracker
            .observe("session-1", false, Some("first outage".into()))
            .unwrap();
        assert_eq!(tracker.observe("session-1", true, None), None);
        assert_eq!(
            tracker.observe("session-1", false, Some("new outage".into())),
            None
        );

        let completion = tracker.finish("session-1", first);
        assert_eq!(completion.display_error, None);
        let second = completion.restart_episode.unwrap();
        assert_eq!(
            tracker.finish("session-1", second).display_error.as_deref(),
            Some("new outage")
        );
    }

    #[tokio::test]
    async fn quota_refresh_completion_keeps_its_generation() {
        let mut quotas = QuotaManager::default();
        let (updates, mut received) = tokio::sync::mpsc::channel(4);
        assert!(refresh_profile_quotas(&mut quotas, 42, &[], &updates).await);
        assert!(matches!(
            received.recv().await,
            Some(QuotaUpdate::Refreshing {
                profile_ids,
            }) if profile_ids.is_empty()
        ));
        assert!(matches!(
            received.recv().await,
            Some(QuotaUpdate::Finished { generation: 42 })
        ));

        let mut pending = Some(43);
        assert!(!complete_manual_quota_refresh(&mut pending, 42));
        assert_eq!(pending, Some(43));
        assert!(complete_manual_quota_refresh(&mut pending, 43));
        assert_eq!(pending, None);
        quotas.shutdown().await;
    }

    #[test]
    fn resource_samples_are_throttled_to_one_per_minute() {
        let started = tokio::time::Instant::now();
        assert!(!resource_sample_is_due(
            Some(&started),
            started + Duration::from_secs(59),
        ));
        assert!(resource_sample_is_due(
            Some(&started),
            started + RESOURCE_POLL_INTERVAL,
        ));
    }

    #[test]
    fn capacity_samples_refresh_every_thirty_seconds() {
        assert_eq!(CAPACITY_POLL_INTERVAL, Duration::from_secs(30));
    }

    #[test]
    fn a_new_auth_failure_waits_out_the_cooldown_without_being_lost() {
        let mut tracker = AuthFailureSyncTracker::default();
        let started = Instant::now();
        tracker.observe("session", "work", 41);
        assert_eq!(
            tracker.drain_due(started),
            vec![("session".into(), "work".into())]
        );

        tracker.observe("session", "work", 42);
        assert!(
            tracker
                .drain_due(started + Duration::from_secs(60))
                .is_empty()
        );
        tracker.observe("session", "new-profile", 43);
        assert_eq!(tracker.pending["session"].ordinal, 43);

        // No repeated observation is needed: the loop timer drains the sticky
        // failure once its cooldown expires.
        assert_eq!(
            tracker.drain_due(started + AUTH_FAILURE_SYNC_COOLDOWN),
            vec![("session".into(), "new-profile".into())]
        );
        tracker.observe("session", "new-profile", 43);
        assert!(
            tracker
                .drain_due(started + (AUTH_FAILURE_SYNC_COOLDOWN * 2))
                .is_empty()
        );

        tracker.observe("other", "personal", 1);
        assert_eq!(
            tracker.drain_due(started + Duration::from_secs(60)),
            vec![("other".into(), "personal".into())]
        );
    }

    #[test]
    fn a_healthy_credential_cycle_stays_out_of_the_ui() {
        let result = hel::hel_credentials::CredentialSyncResult {
            profile_id: "work".into(),
            triggered_by: None,
            failure: None,
            outcomes: Vec::new(),
        };
        assert_eq!(CredentialSyncNotices::default().notice(&result), None);
    }

    #[test]
    fn an_authentication_failure_notice_says_whether_anything_was_pushed() {
        use hel::hel_credentials::{
            CredentialSyncAction, CredentialSyncOutcome, CredentialSyncResult,
        };

        let mut notices = CredentialSyncNotices::default();
        let pushed = CredentialSyncResult {
            profile_id: "work".into(),
            triggered_by: Some("018f9dd2-a3b4".into()),
            failure: None,
            outcomes: vec![CredentialSyncOutcome {
                session_id: "018f9dd2-a3b4".into(),
                outcome: Ok(vec![CredentialSyncAction::Pushed]),
            }],
        };
        let notice = notices.notice(&pushed).unwrap();
        assert!(notice.contains("were pushed"), "{notice}");
        assert!(notice.contains("hel login --profile work"), "{notice}");

        let nothing_to_push = CredentialSyncResult {
            triggered_by: Some("018f9dd2-a3b4".into()),
            outcomes: Vec::new(),
            ..pushed
        };
        let notice = notices.notice(&nothing_to_push).unwrap();
        assert!(notice.contains("nothing fresher"), "{notice}");
        assert!(notice.contains("hel login --profile work"), "{notice}");
        // The per-session cooldown upstream limits these; the dedup must not.
        assert_eq!(notices.notice(&nothing_to_push), Some(notice));
    }

    #[test]
    fn a_failed_credential_sync_is_reported() {
        use hel::hel_credentials::{CredentialSyncOutcome, CredentialSyncResult};

        let result = CredentialSyncResult {
            profile_id: "work".into(),
            triggered_by: None,
            failure: None,
            outcomes: vec![CredentialSyncOutcome {
                session_id: "018f9dd2-a3b4".into(),
                outcome: Err("worker proxy disconnected".into()),
            }],
        };
        let notice = CredentialSyncNotices::default().notice(&result).unwrap();
        assert!(notice.contains("worker proxy disconnected"), "{notice}");
    }

    #[test]
    fn a_repeated_credential_failure_is_reported_once_until_it_changes() {
        use hel::hel_credentials::{
            CredentialSyncAction, CredentialSyncOutcome, CredentialSyncResult,
        };

        let failed = |detail: &str| CredentialSyncResult {
            profile_id: "work".into(),
            triggered_by: None,
            failure: None,
            outcomes: vec![CredentialSyncOutcome {
                session_id: "018f9dd2-a3b4".into(),
                outcome: Err(detail.to_owned()),
            }],
        };
        let mut notices = CredentialSyncNotices::default();

        assert!(
            notices
                .notice(&failed("worker proxy disconnected"))
                .is_some()
        );
        assert_eq!(notices.notice(&failed("worker proxy disconnected")), None);

        let changed = notices.notice(&failed("container is gone")).unwrap();
        assert!(changed.contains("container is gone"), "{changed}");
        assert_eq!(notices.notice(&failed("container is gone")), None);

        // A clean cycle forgets the failure, so a recurrence is reported again.
        let healthy = CredentialSyncResult {
            profile_id: "work".into(),
            triggered_by: None,
            failure: None,
            outcomes: vec![CredentialSyncOutcome {
                session_id: "018f9dd2-a3b4".into(),
                outcome: Ok(vec![CredentialSyncAction::Pushed]),
            }],
        };
        let refreshed = notices.notice(&healthy).unwrap();
        assert!(
            refreshed.contains("Refreshed harness credentials"),
            "{refreshed}"
        );
        assert!(notices.notice(&failed("container is gone")).is_some());
    }

    #[test]
    fn a_repeated_whole_sync_failure_is_reported_once_per_profile() {
        use hel::hel_credentials::CredentialSyncResult;

        let failed = |profile_id: &str| CredentialSyncResult {
            profile_id: profile_id.to_owned(),
            triggered_by: None,
            failure: Some("controller home is unreadable".into()),
            outcomes: Vec::new(),
        };
        let mut notices = CredentialSyncNotices::default();

        let notice = notices.notice(&failed("work")).unwrap();
        assert!(notice.contains("profile work"), "{notice}");
        assert_eq!(notices.notice(&failed("work")), None);
        // Another profile failing the same way is its own key.
        assert!(notices.notice(&failed("personal")).is_some());
        assert_eq!(notices.notice(&failed("work")), None);
    }

    #[test]
    fn skills_and_credential_syncs_each_speak_in_the_notice() {
        use hel::hel_credentials::{
            CredentialSyncAction, CredentialSyncOutcome, CredentialSyncResult,
        };

        let result = CredentialSyncResult {
            profile_id: "work".into(),
            triggered_by: None,
            failure: None,
            outcomes: vec![
                CredentialSyncOutcome {
                    session_id: "018f9dd2-a3b4".into(),
                    outcome: Ok(vec![
                        CredentialSyncAction::Pushed,
                        CredentialSyncAction::SkillsPushed,
                    ]),
                },
                CredentialSyncOutcome {
                    session_id: "018f9dd2-bbbb".into(),
                    outcome: Ok(vec![CredentialSyncAction::SkillsPushed]),
                },
            ],
        };
        let notice = CredentialSyncNotices::default().notice(&result).unwrap();
        assert!(
            notice.contains("Refreshed harness credentials for profile work across 1 session(s)."),
            "{notice}"
        );
        assert!(
            notice.contains("Synced skills for profile work to 2 session(s)."),
            "{notice}"
        );
    }

    #[test]
    fn aws_capacity_sums_live_instance_allocations() {
        let total = aggregate_aws_capacity(&[
            DeploymentCapacityUsage {
                cpu_percent: None,
                memory_used_bytes: 0,
                memory_total_bytes: 8,
                logical_cores: 2,
                disk_total_bytes: Some(100),
            },
            DeploymentCapacityUsage {
                cpu_percent: None,
                memory_used_bytes: 0,
                memory_total_bytes: 16,
                logical_cores: 4,
                disk_total_bytes: Some(200),
            },
        ])
        .unwrap();

        assert_eq!(total.memory_total_bytes, 24);
        assert_eq!(total.logical_cores, 6);
        assert_eq!(total.disk_total_bytes, Some(300));
    }
}
