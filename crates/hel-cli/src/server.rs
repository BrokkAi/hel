//! The phone-oriented remote-control server: its HTTP surface, the controller
//! actions phones request, and the concurrency limits that keep them safe.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use clap::Args;
use hel::hel_controller::{Controller, SessionLaunchOptions, SessionResumeOptions};
use hel::hel_quota::QuotaManager;
use hel::hel_server::{
    ControllerAction, ControllerRequest, ResumeQueueDisposition, ServerOptions, ViewerQueuedPrompt,
    ViewerQuota, ViewerSnapshot,
};
use hel::hel_session_manager::{SessionManagerControl, new_command_id};
use hel::hel_state::{HelState, SessionRecord};
use hel::hel_targets::{CancellableProcessExecutor, CommandExecutor};
use hel::hel_worker::RelayCommand;
use hel::hel_worker_client::CredentialSyncCoordinator;

use crate::pollers::{
    AuthFailureSyncTracker, CredentialSyncNotices, LifecycleUpdate, apply_worker_record_update,
    credential_sync_targets, dashboard_worker_targets, interrupted_close_session_ids,
    merge_recovery_result, projected_queued_prompts, queued_prompt_projection,
    quota_refresh_profiles, reserve_recovery_or_cancel, schedule_due_auth_failure_syncs,
    spawn_dashboard_worker_poller, spawn_interrupted_close_recovery,
};

#[derive(Debug, Args)]
pub(crate) struct ServerArgs {
    /// Address exposed by the explicit phone-control server.
    #[arg(long, default_value = "127.0.0.1:3765")]
    bind: String,
    /// PEM certificate for direct HTTPS (for example, from Tailscale).
    #[arg(long, requires = "tls_key")]
    tls_cert: Option<PathBuf>,
    /// PEM private key for direct HTTPS.
    #[arg(long, requires = "tls_cert")]
    tls_key: Option<PathBuf>,
}

const MAX_CONCURRENT_PHONE_ACTIONS: usize = 4;

struct PhoneActionStarted {
    action_id: u64,
    session: SessionRecord,
    published: tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
}

#[derive(Clone, Copy)]
#[repr(u8)]
enum PhoneNewActionState {
    Active = 0,
    CancelRequested = 1,
    CommitGranted = 2,
}

struct PhoneNewActionGate {
    state: AtomicU8,
}

impl PhoneNewActionGate {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(PhoneNewActionState::Active as u8),
        }
    }

    fn request_cancel(&self) -> bool {
        self.state
            .compare_exchange(
                PhoneNewActionState::Active as u8,
                PhoneNewActionState::CancelRequested as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn grant_commit(&self) -> bool {
        self.state
            .compare_exchange(
                PhoneNewActionState::Active as u8,
                PhoneNewActionState::CommitGranted as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }
}

#[derive(Clone)]
struct PhoneActionControl {
    cancelled: Arc<AtomicBool>,
    new_gate: Option<Arc<PhoneNewActionGate>>,
}

impl PhoneActionControl {
    fn for_action(action: &ControllerAction) -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            new_gate: matches!(action, ControllerAction::New { .. })
                .then(|| Arc::new(PhoneNewActionGate::new())),
        }
    }

    fn request_cancel(&self) -> bool {
        let accepted = self.new_gate.as_ref().map_or_else(
            || {
                self.cancelled
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            },
            |gate| gate.request_cancel(),
        );
        if accepted {
            self.cancelled.store(true, Ordering::Release);
        }
        accepted
    }

    fn grant_new_commit(&self) -> bool {
        self.new_gate
            .as_ref()
            .is_some_and(|gate| gate.grant_commit())
    }
}

pub(crate) async fn run_server(args: ServerArgs) -> Result<()> {
    let bind = args.bind.parse().context("parse --bind socket address")?;
    let mut controller = Controller::load()?;
    let mut quotas = QuotaManager::default();
    refresh_all_quotas(&controller, &mut quotas).await;
    let mut revision = 1;
    let mut conversations = std::collections::BTreeMap::new();
    let mut queued_prompts = projected_queued_prompts(&controller)?;
    let (snapshot_tx, snapshot_rx) = tokio::sync::watch::channel(viewer_snapshot(
        &controller,
        &quotas,
        &conversations,
        &queued_prompts,
        revision,
    ));
    let (conversation_tx, conversation_rx) = tokio::sync::watch::channel(conversations.clone());
    let (action_tx, mut action_rx) = tokio::sync::mpsc::channel(32);
    let (worker_targets_tx, mut worker_updates_rx, worker_commands_tx) =
        spawn_dashboard_worker_poller()?;
    worker_targets_tx.send_replace(dashboard_worker_targets(&controller));
    let mut recovery = hel::hel_recovery::RecoveryCoordinator::spawn(worker_commands_tx.clone());
    let recovery_observer = recovery.observer();
    let interrupted_close_ids = interrupted_close_session_ids(&controller);
    let (interrupted_close_tx, mut interrupted_close_rx) =
        tokio::sync::mpsc::unbounded_channel::<LifecycleUpdate>();
    let mut interrupted_close_cancellations = std::collections::BTreeMap::new();
    for session_id in &interrupted_close_ids {
        let cancelled = Arc::new(AtomicBool::new(false));
        interrupted_close_cancellations.insert(session_id.clone(), cancelled.clone());
        spawn_interrupted_close_recovery(
            session_id.clone(),
            worker_commands_tx.clone(),
            recovery_observer.clone(),
            cancelled,
            interrupted_close_tx.clone(),
        );
    }
    let mut credential_sync = CredentialSyncCoordinator::spawn();
    let credential_sync_handle = credential_sync.handle();
    credential_sync_handle.set_targets(credential_sync_targets(&controller));
    let mut auth_failure_syncs = AuthFailureSyncTracker::default();
    let mut credential_sync_notices = CredentialSyncNotices::default();
    let termination = hel::termination::Coordinator::install().token();
    let mut options = ServerOptions::new(bind, snapshot_rx, conversation_rx, action_tx)?;
    options.shutdown = termination.clone();
    if let (Some(cert), Some(key)) = (args.tls_cert, args.tls_key) {
        options.set_tls_config(
            axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key)
                .await
                .context("load phone-server TLS certificate")?,
        );
    } else if bind.ip().is_loopback() {
        options.secure_cookie = false;
    } else {
        anyhow::bail!("non-loopback phone server requires --tls-cert and --tls-key");
    }

    let serve = hel::hel_server::run_server(options);
    let control = async {
        let mut recovery_tick = tokio::time::interval(Duration::from_millis(250));
        let (action_done_tx, mut action_done_rx) = tokio::sync::mpsc::unbounded_channel::<(
            u64,
            Option<String>,
            tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
            std::result::Result<(), String>,
        )>();
        let (action_started_tx, mut action_started_rx) =
            tokio::sync::mpsc::unbounded_channel::<PhoneActionStarted>();
        let mut active_actions = interrupted_close_ids
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        let mut next_action_id = 0_u64;
        let mut action_cancellations = std::collections::BTreeMap::<u64, PhoneActionControl>::new();
        let mut action_sessions = std::collections::BTreeMap::<u64, String>::new();
        loop {
            tokio::select! {
                _ = termination.cancelled() => {
                    for cancelled in interrupted_close_cancellations.values() {
                        cancelled.store(true, Ordering::Release);
                    }
                    for control in action_cancellations.values() {
                        control.request_cancel();
                    }
                    break;
                },
                update = worker_updates_rx.recv() => {
                    let Some(update) = update else { break };
                    if let Some(snapshot) = update.view.snapshot.as_ref()
                        && let Some(session) = controller.state.sessions.get(&update.session_id)
                        && let Some(ordinal) = snapshot.latest_auth_failure_ordinal
                    {
                        auth_failure_syncs.observe(
                            &update.session_id,
                            &session.last_profile,
                            ordinal,
                        );
                    }
                    schedule_due_auth_failure_syncs(
                        &mut auth_failure_syncs,
                        &credential_sync_handle,
                        Instant::now(),
                    );
                    if let Err(error) =
                        apply_worker_record_update(&mut controller, &update, None)
                    {
                        tracing::warn!(session_id = %update.session_id, "could not persist relay session metadata: {error:#}");
                    }
                    if let Some(snapshot) = update.view.snapshot {
                        if let Some(session) = controller.state.sessions.get(&update.session_id).cloned() {
                            recovery_observer.observe(hel::hel_state::RecoveryObservation {
                                session,
                                config: controller.config.clone(),
                                latest_completed_turn_ordinal:
                                    hel::hel_state::latest_completed_turn_ordinal(
                                        &snapshot.materialized,
                                    ),
                                execution: snapshot.materialized.execution,
                            });
                        }
                        conversations.insert(
                            update.session_id.clone(),
                            hel::hel_chat::TranscriptSnapshot::from_materialized(
                                &snapshot.materialized,
                            )
                            .browser_transcript(None),
                        );
                        queued_prompts.insert(
                            update.session_id.clone(),
                            queued_prompt_projection(&snapshot.materialized),
                        );
                        revision += 1;
                        conversation_tx.send_replace(conversations.clone());
                        let _ = snapshot_tx.send(viewer_snapshot(
                            &controller,
                            &quotas,
                            &conversations,
                            &queued_prompts,
                            revision,
                        ));
                    }
                }
                _ = recovery_tick.tick() => {
                    schedule_due_auth_failure_syncs(
                        &mut auth_failure_syncs,
                        &credential_sync_handle,
                        Instant::now(),
                    );
                    let mut changed = false;
                    while let Some(result) = recovery.try_result() {
                        changed |= merge_recovery_result(&mut controller, result);
                    }
                    while let Some(result) = credential_sync.try_result() {
                        if let Some(notice) = credential_sync_notices.notice(&result) {
                            eprintln!("Hel: {notice}");
                        }
                    }
                    if changed {
                        revision += 1;
                        let _ = snapshot_tx.send(viewer_snapshot(&controller, &quotas, &conversations, &queued_prompts, revision));
                    }
                }
                completed = interrupted_close_rx.recv() => {
                    let Some(completed) = completed else { continue; };
                    active_actions.remove(&completed.session_id);
                    interrupted_close_cancellations.remove(&completed.session_id);
                    if let Err(error) = &completed.result {
                        tracing::warn!(session_id = %completed.session_id, "could not resume interrupted close: {error}");
                    }
                    if let Err(error) = controller.reload() {
                        tracing::warn!(%error, "interrupted close completed but controller state could not be reloaded");
                        continue;
                    }
                    worker_targets_tx.send_replace(dashboard_worker_targets(&controller));
                    credential_sync_handle.set_targets(credential_sync_targets(&controller));
                    queued_prompts.retain(|session_id, _| {
                        controller.state.sessions.contains_key(session_id)
                    });
                    conversations.retain(|id, _| {
                        controller.state.sessions.get(id).is_some_and(|session| session.state.is_active())
                    });
                    revision += 1;
                    conversation_tx.send_replace(conversations.clone());
                    let _ = snapshot_tx.send(viewer_snapshot(&controller, &quotas, &conversations, &queued_prompts, revision));
                }
                action = action_rx.recv() => {
                    let Some(request) = action else { break };
                    if let ControllerAction::Cancel { session_id } = &request.action {
                        let result = request_phone_action_cancellation(
                            session_id,
                            &action_sessions,
                            &action_cancellations,
                            &interrupted_close_cancellations,
                        )
                        .then_some(())
                        .ok_or_else(|| "the session has no cancellable operation".into());
                        let _ = request.reply.send(result);
                        continue;
                    }
                    if !phone_action_capacity_available(action_cancellations.len()) {
                        let _ = request.reply.send(Err(
                            "the controller is at its concurrent phone-action limit; retry shortly".into()
                        ));
                        continue;
                    }
                    let session_id = controller_action_session_id(&request.action);
                    if session_id.as_ref().is_some_and(|id| !active_actions.insert(id.clone())) {
                        let _ = request.reply.send(Err("another operation is already running for this session".into()));
                        continue;
                    }
                    let ControllerRequest { action, reply } = request;
                    let done = action_done_tx.clone();
                    let observer = recovery_observer.clone();
                    let session_control = worker_commands_tx.clone();
                    let started = action_started_tx.clone();
                    next_action_id = next_action_id.wrapping_add(1).max(1);
                    let action_id = next_action_id;
                    let control = PhoneActionControl::for_action(&action);
                    action_cancellations.insert(action_id, control.clone());
                    if let Some(session_id) = &session_id {
                        action_sessions.insert(action_id, session_id.clone());
                    }
                    let runtime = tokio::runtime::Handle::current();
                    tokio::spawn(async move {
                        let joined = tokio::task::spawn_blocking(move || {
                            let result = (|| -> Result<()> {
                                let _recovery_reservation = match &action {
                                    ControllerAction::Prompt { session_id, .. }
                                    | ControllerAction::Close { session_id }
                                    | ControllerAction::Resume { session_id, .. }
                                    | ControllerAction::RemoveQueuedPrompt { session_id, .. } => {
                                        Some(reserve_recovery_or_cancel(
                                            &observer,
                                            session_id,
                                            &control.cancelled,
                                        )?)
                                    }
                                    ControllerAction::New { .. }
                                    | ControllerAction::Open { .. }
                                    | ControllerAction::Read { .. }
                                    | ControllerAction::Cancel { .. } => None,
                                };
                                if control.cancelled.load(Ordering::Acquire) {
                                    bail!("phone action cancelled");
                                }
                                let mut operation_controller = Controller::load()?;
                                let executor =
                                    CancellableProcessExecutor::new(control.cancelled.clone());
                                runtime.block_on(apply_phone_action(
                                    &mut operation_controller,
                                    &session_control,
                                    action,
                                    &executor,
                                    action_id,
                                    &started,
                                    &control,
                                ))
                            })();
                            result.map_err(|error| format!("{error:#}"))
                        })
                        .await;
                        let result = match joined {
                            Ok(result) => result,
                            Err(error) => Err(format!("phone action task failed: {error}")),
                        };
                        if done.send((action_id, session_id, reply, result)).is_err() {
                            tracing::debug!(action_id, "phone action finished after the server stopped");
                        }
                    });
                }
                started = action_started_rx.recv() => {
                    let Some(started) = started else { continue; };
                    let publication = if !action_cancellations.contains_key(&started.action_id) {
                        Err("phone action completed before its provisional session was published".into())
                    } else {
                        track_started_phone_session(
                            &mut controller.state,
                            &mut active_actions,
                            &mut action_sessions,
                            started.action_id,
                            started.session,
                        )
                    };
                    if publication.is_ok() {
                        revision += 1;
                        let _ = snapshot_tx.send(viewer_snapshot(
                            &controller,
                            &quotas,
                            &conversations,
                            &queued_prompts,
                            revision,
                        ));
                    };
                    if publication.is_err()
                        && let Some(control) = action_cancellations.get(&started.action_id)
                    {
                        control.request_cancel();
                    }
                    let _ = started.published.send(publication);
                }
                completed = action_done_rx.recv() => {
                    let Some((action_id, session_id, reply, result)) = completed else { break };
                    action_cancellations.remove(&action_id);
                    if let Some(session_id) = action_sessions.remove(&action_id).or(session_id) {
                        active_actions.remove(&session_id);
                    }
                    if let Err(error) = &result {
                        eprintln!("Hel phone action failed: {error}");
                    }
                    if let Err(error) = controller.reload() {
                        let _ = reply.send(Err(format!("action completed but state reload failed: {error:#}")));
                        continue;
                    }
                    worker_targets_tx.send_replace(dashboard_worker_targets(&controller));
                    credential_sync_handle.set_targets(credential_sync_targets(&controller));
                    conversations.retain(|id, _| {
                        controller.state.sessions.get(id).is_some_and(|session| session.state.is_active())
                    });
                    revision += 1;
                    conversation_tx.send_replace(conversations.clone());
                    let _ = snapshot_tx.send(viewer_snapshot(&controller, &quotas, &conversations, &queued_prompts, revision));
                    let _ = reply.send(result);
                }
            }
        }
        Ok::<(), anyhow::Error>(())
    };
    tokio::select! {
        result = serve => result,
        result = control => result,
    }?;
    quotas.shutdown().await;
    Ok(())
}

fn controller_action_session_id(action: &ControllerAction) -> Option<String> {
    match action {
        ControllerAction::New { .. } => None,
        ControllerAction::Prompt { session_id, .. }
        | ControllerAction::Close { session_id }
        | ControllerAction::Resume { session_id, .. }
        | ControllerAction::Open { session_id }
        | ControllerAction::Read { session_id, .. }
        | ControllerAction::Cancel { session_id }
        | ControllerAction::RemoveQueuedPrompt { session_id, .. } => Some(session_id.clone()),
    }
}

fn phone_action_capacity_available(active_actions: usize) -> bool {
    active_actions < MAX_CONCURRENT_PHONE_ACTIONS
}

fn request_phone_action_cancellation(
    session_id: &str,
    action_sessions: &std::collections::BTreeMap<u64, String>,
    action_cancellations: &std::collections::BTreeMap<u64, PhoneActionControl>,
    interrupted_cancellations: &std::collections::BTreeMap<String, Arc<AtomicBool>>,
) -> bool {
    let control = action_sessions
        .iter()
        .find_map(|(action_id, active_session_id)| {
            (active_session_id == session_id)
                .then(|| action_cancellations.get(action_id))
                .flatten()
        });
    if let Some(control) = control {
        return control.request_cancel();
    }
    interrupted_cancellations
        .get(session_id)
        .is_some_and(|cancelled| {
            cancelled
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        })
}

fn track_started_phone_session(
    state: &mut HelState,
    active_actions: &mut std::collections::BTreeSet<String>,
    action_sessions: &mut std::collections::BTreeMap<u64, String>,
    action_id: u64,
    session: SessionRecord,
) -> std::result::Result<(), String> {
    let session_id = session.id.clone();
    if !active_actions.insert(session_id.clone()) {
        return Err("another operation is already running for the new session".into());
    }
    action_sessions.insert(action_id, session_id.clone());
    state.sessions.insert(session_id, session);
    Ok(())
}

async fn apply_phone_action(
    controller: &mut Controller,
    sessions: &SessionManagerControl,
    action: ControllerAction,
    executor: &(impl CommandExecutor + Sync),
    action_id: u64,
    started: &tokio::sync::mpsc::UnboundedSender<PhoneActionStarted>,
    control: &PhoneActionControl,
) -> Result<()> {
    match action {
        ControllerAction::New {
            profile_id,
            bundle_id,
            target_id,
            title,
            project_directory,
        } => {
            let session_title_override = Some(title.clone());
            let session_id = controller.register_session_with_resources(
                &profile_id,
                &bundle_id,
                &target_id,
                title,
                SessionLaunchOptions {
                    additional_mounts: Vec::new(),
                    allow_dirty_local: false,
                    resource_allocation: None,
                    project_directory,
                    session_title_override,
                },
            )?;
            let session = controller
                .state
                .sessions
                .get(&session_id)
                .expect("newly registered phone session exists")
                .clone();
            let (published, publication) = tokio::sync::oneshot::channel();
            let publish_result = started
                .send(PhoneActionStarted {
                    action_id,
                    session,
                    published,
                })
                .map_err(|_| anyhow::anyhow!("phone server stopped before publishing session"));
            let publish_result = match publish_result {
                Ok(()) => publication
                    .await
                    .map_err(|_| anyhow::anyhow!("phone server stopped before publishing session"))?
                    .map_err(anyhow::Error::msg),
                Err(error) => Err(error),
            };
            if let Err(error) = publish_result {
                control.request_cancel();
                let rollback = controller
                    .provision_session_controlled(&session_id, executor)
                    .await;
                return match rollback {
                    Ok(()) => Err(error),
                    Err(rollback) => Err(error.context(format!(
                        "discard provisional session after publication failure: {rollback:#}"
                    ))),
                };
            }
            controller
                .provision_session_controlled_with_commit(&session_id, executor, || {
                    if control.grant_new_commit() {
                        Ok(())
                    } else {
                        bail!("phone action cancelled before session commit")
                    }
                })
                .await
        }
        ControllerAction::Prompt { session_id, text } => {
            sessions
                .session(&session_id)
                .await?
                .submit(
                    new_command_id("phone-prompt")?,
                    RelayCommand::Prompt {
                        prompt: vec![agent_client_protocol::schema::v1::ContentBlock::Text(
                            agent_client_protocol::schema::v1::TextContent::new(text),
                        )],
                    },
                )
                .await?;
            Ok(())
        }
        ControllerAction::Close { session_id } => {
            controller
                .close_session_managed_controlled(&session_id, executor, sessions)
                .await
        }
        ControllerAction::Resume {
            session_id,
            profile_id,
            target_id,
            queue,
        } => controller
            .resume_session_controlled(
                &session_id,
                &profile_id,
                &target_id,
                SessionResumeOptions {
                    additional_mounts: None,
                    resource_allocation: None,
                    discard_queue: queue == ResumeQueueDisposition::Discard,
                },
                executor,
            )
            .await
            .map(|_| ()),
        ControllerAction::Open { .. } => Ok(()),
        ControllerAction::Cancel { .. } => {
            bail!("cancel actions must be handled by the phone control loop")
        }
        ControllerAction::Read {
            session_id,
            through,
        } => controller.mark_session_detached_after(&session_id, through),
        ControllerAction::RemoveQueuedPrompt {
            session_id,
            queue_id,
        } => {
            sessions
                .session(&session_id)
                .await?
                .submit(
                    new_command_id("phone-remove-prompt")?,
                    RelayCommand::RemoveQueuedPrompt {
                        queued_command_id: queue_id,
                    },
                )
                .await?;
            Ok(())
        }
    }
}

fn viewer_snapshot(
    controller: &Controller,
    quotas: &QuotaManager,
    conversations: &std::collections::BTreeMap<String, hel::hel_chat::BrowserTranscript>,
    queued_prompts: &std::collections::BTreeMap<String, Vec<hel::hel_worker::QueuedPrompt>>,
    revision: u64,
) -> ViewerSnapshot {
    let mut snapshot =
        ViewerSnapshot::from_config_state(&controller.config, &controller.state, revision);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    for profile in &mut snapshot.profiles {
        let Some(quota) = quotas.reports().get(&profile.id) else {
            continue;
        };
        profile.quota = Some(ViewerQuota {
            summary: quota.compact(),
            resets_at: quota
                .windows
                .iter()
                .find_map(|window| window.resets.clone()),
            stale: now.saturating_sub(quota.refreshed_at_epoch_seconds) > 300,
            has_error: quota.error.is_some(),
        });
    }
    for session in &mut snapshot.sessions {
        session.queued_prompts = queued_prompts
            .get(&session.id)
            .into_iter()
            .flatten()
            .map(|prompt| ViewerQueuedPrompt {
                id: prompt.id.clone(),
                text: prompt.text.clone(),
                created_at: prompt.created_at_ms.to_string(),
            })
            .collect();
        if let Some(transcript) = conversations.get(&session.id) {
            session.conversation_available = true;
            let mut lines = transcript
                .entries
                .iter()
                .flat_map(|entry| {
                    entry
                        .lines
                        .iter()
                        .enumerate()
                        .filter_map(move |(index, line)| {
                            let line = line.trim();
                            (!line.is_empty()).then(|| {
                                if index == 0 {
                                    format!("{}: {line}", entry.label)
                                } else {
                                    line.to_owned()
                                }
                            })
                        })
                })
                .collect::<Vec<_>>();
            session.preview = lines.split_off(lines.len().saturating_sub(4));
        }
    }
    snapshot
}

async fn refresh_all_quotas(controller: &Controller, quotas: &mut QuotaManager) {
    quotas
        .refresh_profiles(quota_refresh_profiles(controller), |_| {
            std::future::ready(())
        })
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use hel::hel_state::SessionState;

    #[test]
    fn phone_action_capacity_is_bounded() {
        assert!(phone_action_capacity_available(
            MAX_CONCURRENT_PHONE_ACTIONS - 1
        ));
        assert!(!phone_action_capacity_available(
            MAX_CONCURRENT_PHONE_ACTIONS
        ));
    }

    #[test]
    fn started_phone_session_is_visible_and_mapped_before_provisioning() {
        let session_id = "0123456789abcdef0123456789abcdef";
        let session = SessionRecord {
            id: session_id.into(),
            title: "Phone launch".into(),
            harness_kind: hel::hel_config::HarnessKind::Codex,
            last_profile: "codex".into(),
            bundle_id: "project".into(),
            project_directory: None,
            managed_worktree: None,
            target_template_id: "podman".into(),
            resource_allocation: None,
            additional_mounts: Vec::new(),
            state: SessionState::Provisioning,
            target: None,
            native_session_id: None,
            acp_session_title: None,
            session_title_override: Some("Phone launch".into()),
            created_at: "2026-08-14T00:00:00Z".into(),
            updated_at: "2026-08-14T00:00:00Z".into(),
            detached_after_event_ordinal: 0,
            draft_input: String::new(),
            last_error: None,
            last_checkpoint_error: None,
            checkpoint: None,
        };
        let mut state = HelState::default();
        let mut active_actions = std::collections::BTreeSet::new();
        let mut action_sessions = std::collections::BTreeMap::new();

        track_started_phone_session(
            &mut state,
            &mut active_actions,
            &mut action_sessions,
            7,
            session,
        )
        .unwrap();

        assert_eq!(state.sessions[session_id].state, SessionState::Provisioning);
        assert_eq!(state.sessions[session_id].display_title(), "Phone launch");
        assert!(active_actions.contains(session_id));
        assert_eq!(
            action_sessions.get(&7).map(String::as_str),
            Some(session_id)
        );
    }

    #[test]
    fn phone_cancel_targets_the_matching_background_action() {
        let first = PhoneActionControl {
            cancelled: Arc::new(AtomicBool::new(false)),
            new_gate: None,
        };
        let second = PhoneActionControl {
            cancelled: Arc::new(AtomicBool::new(false)),
            new_gate: None,
        };
        let action_sessions =
            std::collections::BTreeMap::from([(1, "session-1".into()), (2, "session-2".into())]);
        let cancellations =
            std::collections::BTreeMap::from([(1, first.clone()), (2, second.clone())]);

        assert!(request_phone_action_cancellation(
            "session-2",
            &action_sessions,
            &cancellations,
            &std::collections::BTreeMap::new(),
        ));
        assert!(!first.cancelled.load(Ordering::Acquire));
        assert!(second.cancelled.load(Ordering::Acquire));
        assert!(!request_phone_action_cancellation(
            "missing",
            &action_sessions,
            &cancellations,
            &std::collections::BTreeMap::new(),
        ));
    }

    #[test]
    fn phone_new_cancel_and_running_commit_have_one_atomic_winner() {
        for _ in 0..100 {
            let control = PhoneActionControl {
                cancelled: Arc::new(AtomicBool::new(false)),
                new_gate: Some(Arc::new(PhoneNewActionGate::new())),
            };
            let cancelling = control.clone();
            let committing = control.clone();
            let (cancelled, committed) = std::thread::scope(|scope| {
                let cancel = scope.spawn(move || cancelling.request_cancel());
                let commit = scope.spawn(move || committing.grant_new_commit());
                (cancel.join().unwrap(), commit.join().unwrap())
            });

            assert_ne!(cancelled, committed);
            assert_eq!(control.cancelled.load(Ordering::Acquire), cancelled);
            assert!(!control.request_cancel());
            assert!(!control.grant_new_commit());
        }
    }
}
