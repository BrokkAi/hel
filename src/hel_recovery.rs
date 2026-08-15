//! Background recovery-copy policy and coordination.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use tokio::sync::{mpsc, watch};

use crate::hel_config::HelConfig;
use crate::hel_controller::{CheckpointArtifact, Controller};
use crate::hel_database::record_recovery_failure;
use crate::hel_session_manager::SessionManagerControl;
use crate::hel_state::{
    CheckpointMetadata, HelState, MaterializedExecutionState, MaterializedSession, SessionRecord,
    TranscriptBody,
};
use crate::hel_targets::CancellableProcessExecutor;

pub const AUTO_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(10 * 60);

#[derive(Debug, Clone)]
pub struct RecoveryObservation {
    pub session: SessionRecord,
    pub config: HelConfig,
    pub latest_completed_turn_ordinal: Option<u64>,
    pub execution: MaterializedExecutionState,
}

pub fn latest_completed_turn_ordinal(session: &MaterializedSession) -> Option<u64> {
    if session.execution != MaterializedExecutionState::Idle {
        return None;
    }
    session
        .transcript
        .iter()
        .rev()
        .find(|item| matches!(item.body, TranscriptBody::User { .. }))
        .map(|item| item.position)
}

struct RecoveryRequest {
    observation: RecoveryObservation,
    acknowledged: tokio::sync::oneshot::Sender<()>,
}

#[derive(Debug, Clone)]
pub struct RecoveryResult {
    pub session_id: String,
    pub expected_target: crate::hel_state::TargetLocator,
    pub outcome: Result<CheckpointArtifact, String>,
}

#[derive(Clone)]
pub struct RecoveryObserver {
    observations: mpsc::UnboundedSender<RecoveryRequest>,
    busy: watch::Receiver<BTreeSet<String>>,
    gate: Arc<RecoveryGate>,
}

/// A per-session reservation held by a foreground lifecycle operation. The
/// coordinator cannot start another recovery copy until this value is dropped.
pub struct RecoveryReservation {
    session_id: String,
    gate: Arc<RecoveryGate>,
}

impl Drop for RecoveryReservation {
    fn drop(&mut self) {
        self.gate.release(&self.session_id);
    }
}

#[derive(Default)]
struct RecoveryGate {
    state: Mutex<RecoveryGateState>,
}

#[derive(Default)]
struct RecoveryGateState {
    busy: BTreeSet<String>,
    reservations: BTreeMap<String, usize>,
}

impl RecoveryGate {
    fn reserve(self: &Arc<Self>, session_id: &str) -> RecoveryReservation {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        *state.reservations.entry(session_id.to_owned()).or_default() += 1;
        RecoveryReservation {
            session_id: session_id.to_owned(),
            gate: self.clone(),
        }
    }

    fn release(&self, session_id: &str) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let Some(count) = state.reservations.get_mut(session_id) else {
            return;
        };
        *count -= 1;
        if *count == 0 {
            state.reservations.remove(session_id);
        }
    }

    fn try_start(&self, session_id: &str) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.busy.contains(session_id) || state.reservations.contains_key(session_id) {
            return false;
        }
        state.busy.insert(session_id.to_owned());
        true
    }

    fn finish(&self, session_id: &str) {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .busy
            .remove(session_id);
    }

    fn is_busy(&self, session_id: &str) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .busy
            .contains(session_id)
    }

    fn busy_sessions(&self) -> BTreeSet<String> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .busy
            .clone()
    }
}

#[derive(Clone)]
pub struct RecoveryContext {
    pub observer: RecoveryObserver,
    pub session: SessionRecord,
    pub config: HelConfig,
}

impl RecoveryContext {
    pub async fn observe(&self, materialized: &MaterializedSession) {
        self.observer
            .observe(RecoveryObservation {
                session: self.session.clone(),
                config: self.config.clone(),
                latest_completed_turn_ordinal: latest_completed_turn_ordinal(materialized),
                execution: materialized.execution,
            })
            .await;
    }

    pub fn is_busy(&self) -> bool {
        self.observer.is_busy(&self.session.id)
    }
}

impl RecoveryObserver {
    pub async fn observe(&self, observation: RecoveryObservation) {
        let (acknowledged, received) = tokio::sync::oneshot::channel();
        if self
            .observations
            .send(RecoveryRequest {
                observation,
                acknowledged,
            })
            .is_ok()
        {
            let _ = received.await;
        }
    }

    pub fn is_busy(&self, session_id: &str) -> bool {
        self.gate.is_busy(session_id)
    }

    pub fn reserve(&self, session_id: &str) -> RecoveryReservation {
        self.gate.reserve(session_id)
    }

    pub async fn wait_idle(&self, session_id: &str) {
        let mut busy = self.busy.clone();
        while self.is_busy(session_id) {
            if busy.changed().await.is_err() {
                break;
            }
        }
    }
}

pub struct RecoveryCoordinator {
    observer: RecoveryObserver,
    results: mpsc::UnboundedReceiver<RecoveryResult>,
    cancelled: Arc<AtomicBool>,
}

impl Drop for RecoveryCoordinator {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Release);
    }
}

impl RecoveryCoordinator {
    pub fn spawn(session_manager: SessionManagerControl) -> Self {
        let (observations_tx, mut observations_rx) = mpsc::unbounded_channel::<RecoveryRequest>();
        let (completed_tx, mut completed_rx) = mpsc::unbounded_channel::<RecoveryResult>();
        let (results_tx, results_rx) = mpsc::unbounded_channel();
        let (busy_tx, busy_rx) = watch::channel(BTreeSet::new());
        let gate = Arc::new(RecoveryGate::default());
        let coordinator_gate = gate.clone();
        let cancelled = Arc::new(AtomicBool::new(false));
        let coordinator_cancelled = cancelled.clone();
        tokio::spawn(async move {
            let mut policies = BTreeMap::<String, PolicyState>::new();
            loop {
                tokio::select! {
                    request = observations_rx.recv() => {
                        let Some(request) = request else { break };
                        let RecoveryRequest { observation, acknowledged } = request;
                        if coordinator_cancelled.load(Ordering::Acquire) {
                            let _ = acknowledged.send(());
                            break;
                        }
                        let session_id = observation.session.id.clone();
                        let policy = policies.entry(session_id.clone()).or_default();
                        policy.observe_checkpoint(observation.session.checkpoint.as_ref());
                        policy.observe_completed_turn(observation.latest_completed_turn_ordinal);
                        if policy.due(observation.execution, Utc::now())
                            && let Some(expected_target) = observation.session.target.clone()
                            && coordinator_gate.try_start(&session_id)
                        {
                            policy.last_attempted_turn = Some(policy.latest_completed_turn);
                            busy_tx.send_replace(coordinator_gate.busy_sessions());
                            let completed_tx = completed_tx.clone();
                            let session_manager = session_manager.clone();
                            let cancelled = coordinator_cancelled.clone();
                            let handle = tokio::runtime::Handle::current();
                            let task_session_id = session_id.clone();
                            tokio::spawn(async move {
                                let joined = tokio::task::spawn_blocking(move || {
                                    let mut state = HelState::default();
                                    state.sessions.insert(
                                        task_session_id.clone(),
                                        observation.session,
                                    );
                                    let controller = Controller {
                                        config: observation.config,
                                        state,
                                    };
                                    let executor = CancellableProcessExecutor::new(cancelled);
                                    handle
                                        .block_on(
                                            controller
                                                .create_recovery_checkpoint_managed_controlled(
                                                    &task_session_id,
                                                    &session_manager,
                                                    &executor,
                                                ),
                                        )
                                        .map_err(|error| format!("{error:#}"))
                                })
                                .await;
                                let outcome = match joined {
                                    Ok(outcome) => outcome,
                                    Err(error) => Err(format!(
                                        "recovery checkpoint task failed: {error}"
                                    )),
                                };
                                let result = RecoveryResult {
                                    session_id,
                                    expected_target,
                                    outcome,
                                };
                                let _ = completed_tx.send(result);
                            });
                        }
                        let _ = acknowledged.send(());
                    }
                    completed = completed_rx.recv() => {
                        let Some(result) = completed else { break };
                        coordinator_gate.finish(&result.session_id);
                        busy_tx.send_replace(coordinator_gate.busy_sessions());
                        let policy = policies.entry(result.session_id.clone()).or_default();
                        match &result.outcome {
                            Ok(artifact) => {
                                policy.checkpoint = Some(artifact.metadata.clone());
                            }
                            Err(detail) => {
                                let session_id = result.session_id.clone();
                                let detail = detail.clone();
                                let persisted = tokio::task::spawn_blocking(move || {
                                    record_recovery_failure(&session_id, &detail)
                                })
                                .await
                                .map_err(anyhow::Error::from)
                                .and_then(|result| result);
                                if let Err(error) = persisted {
                                    tracing::warn!(session_id = %result.session_id, "could not persist recovery failure: {error:#}");
                                }
                            }
                        }
                        let _ = results_tx.send(result);
                    }
                }
            }
        });
        Self {
            observer: RecoveryObserver {
                observations: observations_tx,
                busy: busy_rx,
                gate,
            },
            results: results_rx,
            cancelled,
        }
    }

    pub fn observer(&self) -> RecoveryObserver {
        self.observer.clone()
    }

    pub fn try_result(&mut self) -> Option<RecoveryResult> {
        self.results.try_recv().ok()
    }

    /// Waits for the next finished recovery copy.
    ///
    /// Event-driven loops select on this instead of polling; `None` means the
    /// coordinator task has stopped. Cancel-safe, so a lost `select!` race
    /// keeps the result queued.
    pub async fn result(&mut self) -> Option<RecoveryResult> {
        self.results.recv().await
    }
}

#[derive(Default)]
struct PolicyState {
    latest_completed_turn: u64,
    last_attempted_turn: Option<u64>,
    checkpoint: Option<CheckpointMetadata>,
}

impl PolicyState {
    fn observe_completed_turn(&mut self, sequence: Option<u64>) {
        if let Some(sequence) = sequence {
            self.latest_completed_turn = self.latest_completed_turn.max(sequence);
        }
    }

    fn observe_checkpoint(&mut self, checkpoint: Option<&CheckpointMetadata>) {
        if checkpoint.is_some_and(|candidate| {
            self.checkpoint
                .as_ref()
                .is_none_or(|current| candidate.event_frontier > current.event_frontier)
        }) {
            self.checkpoint = checkpoint.cloned();
        }
    }

    fn due(&self, execution: MaterializedExecutionState, now: chrono::DateTime<Utc>) -> bool {
        if execution != MaterializedExecutionState::Idle
            || self.latest_completed_turn == 0
            || self
                .checkpoint
                .as_ref()
                .is_some_and(|checkpoint| checkpoint.event_frontier >= self.latest_completed_turn)
            || self.last_attempted_turn == Some(self.latest_completed_turn)
        {
            return false;
        }
        self.checkpoint.as_ref().is_none_or(|checkpoint| {
            chrono::DateTime::parse_from_rfc3339(&checkpoint.created_at)
                .map(|created| now.signed_duration_since(created).num_seconds() >= 600)
                .unwrap_or(true)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn completed(position: u64) -> MaterializedSession {
        let mut session = MaterializedSession::empty("session-1");
        session.applied_event_ordinal = position;
        session.transcript.push(crate::hel_state::TranscriptItem {
            stable_id: format!("user:{position}"),
            position,
            latest_content_event_ordinal: None,
            created_at_ms: 1,
            last_changed_at_ms: 1,
            body: TranscriptBody::User {
                content: vec![serde_json::json!({"type": "text", "text": "go"})],
            },
        });
        session
    }

    #[test]
    fn lifecycle_reservation_closes_the_recovery_start_race() {
        let gate = Arc::new(RecoveryGate::default());
        let reservation = gate.reserve("session-1");

        assert!(!gate.try_start("session-1"));

        drop(reservation);
        assert!(gate.try_start("session-1"));
        assert!(gate.is_busy("session-1"));
        gate.finish("session-1");
        assert!(!gate.is_busy("session-1"));
    }

    #[tokio::test]
    async fn dropping_coordinator_cancels_its_background_copies() {
        let channels = crate::hel_session_manager::spawn_session_manager().unwrap();
        let coordinator = RecoveryCoordinator::spawn(channels.control);
        let cancelled = coordinator.cancelled.clone();

        drop(coordinator);

        assert!(cancelled.load(Ordering::Acquire));
    }

    #[test]
    fn first_completed_idle_turn_is_due() {
        let mut policy = PolicyState::default();
        policy.observe_completed_turn(latest_completed_turn_ordinal(&completed(3)));
        assert!(policy.due(MaterializedExecutionState::Idle, Utc::now()));
        assert!(!policy.due(
            MaterializedExecutionState::Running { started_at_ms: 1 },
            Utc::now()
        ));
    }

    #[test]
    fn checkpoint_must_be_ten_minutes_old_and_behind_the_turn() {
        let now = Utc::now();
        let mut policy = PolicyState {
            latest_completed_turn: 8,
            checkpoint: Some(CheckpointMetadata {
                archive_path: "copy.hel.zip".into(),
                sha256: "a".repeat(64),
                created_at: (now - chrono::Duration::minutes(9)).to_rfc3339(),
                event_frontier: 4,
            }),
            ..Default::default()
        };
        assert!(!policy.due(MaterializedExecutionState::Idle, now));
        policy.checkpoint.as_mut().unwrap().created_at =
            (now - chrono::Duration::minutes(10)).to_rfc3339();
        assert!(policy.due(MaterializedExecutionState::Idle, now));
        policy.checkpoint.as_mut().unwrap().event_frontier = 8;
        assert!(!policy.due(MaterializedExecutionState::Idle, now));
    }

    #[test]
    fn failed_boundary_waits_for_another_completed_turn() {
        let now = Utc::now();
        let policy = PolicyState {
            latest_completed_turn: 8,
            last_attempted_turn: Some(8),
            ..Default::default()
        };
        assert!(!policy.due(MaterializedExecutionState::Idle, now));
    }
}
