//! Background recovery-copy policy and coordination.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use chrono::Utc;
use tokio::sync::{mpsc, watch};

use crate::hel_config::HelConfig;
use crate::hel_controller::{CheckpointArtifact, Controller};
use crate::hel_database::{record_recovery_failure, record_recovery_success};
use crate::hel_state::{CheckpointMetadata, HelState, SessionRecord};
use crate::hel_worker::{SequencedEvent, WorkerEvent, WorkerPhase};

pub const AUTO_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(10 * 60);

#[derive(Debug, Clone)]
pub struct RecoveryObservation {
    pub session: SessionRecord,
    pub config: HelConfig,
    pub latest_completed_turn_seq: Option<u64>,
    pub phase: WorkerPhase,
}

pub fn latest_completed_turn_seq(events: &[SequencedEvent]) -> Option<u64> {
    events
        .iter()
        .rev()
        .find(|event| matches!(event.event, WorkerEvent::TurnCompleted))
        .map(|event| event.seq)
}

struct RecoveryRequest {
    observation: RecoveryObservation,
    acknowledged: tokio::sync::oneshot::Sender<()>,
}

#[derive(Debug, Clone)]
pub struct RecoveryResult {
    pub session_id: String,
    pub expected_target: crate::hel_state::TargetLocator,
    pub previous_checkpoint: Option<CheckpointMetadata>,
    pub outcome: Result<CheckpointArtifact, String>,
}

#[derive(Clone)]
pub struct RecoveryObserver {
    observations: mpsc::UnboundedSender<RecoveryRequest>,
    busy: watch::Receiver<BTreeSet<String>>,
}

#[derive(Clone)]
pub struct RecoveryContext {
    pub observer: RecoveryObserver,
    pub session: SessionRecord,
    pub config: HelConfig,
}

impl RecoveryContext {
    pub async fn observe(&self, events: &[SequencedEvent], phase: WorkerPhase) {
        self.observer
            .observe(RecoveryObservation {
                session: self.session.clone(),
                config: self.config.clone(),
                latest_completed_turn_seq: latest_completed_turn_seq(events),
                phase,
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
        self.busy.borrow().contains(session_id)
    }

    pub async fn wait_idle(&self, session_id: &str) {
        let mut busy = self.busy.clone();
        while busy.borrow().contains(session_id) {
            if busy.changed().await.is_err() {
                break;
            }
        }
    }
}

pub struct RecoveryCoordinator {
    observer: RecoveryObserver,
    results: mpsc::UnboundedReceiver<RecoveryResult>,
}

impl RecoveryCoordinator {
    pub fn spawn() -> Self {
        let (observations_tx, mut observations_rx) = mpsc::unbounded_channel::<RecoveryRequest>();
        let (completed_tx, mut completed_rx) = mpsc::unbounded_channel::<RecoveryResult>();
        let (results_tx, results_rx) = mpsc::unbounded_channel();
        let (busy_tx, busy_rx) = watch::channel(BTreeSet::new());
        tokio::spawn(async move {
            let mut policies = BTreeMap::<String, PolicyState>::new();
            let mut busy = BTreeSet::new();
            loop {
                tokio::select! {
                    request = observations_rx.recv() => {
                        let Some(request) = request else { break };
                        let RecoveryRequest { observation, acknowledged } = request;
                        let session_id = observation.session.id.clone();
                        let policy = policies.entry(session_id.clone()).or_default();
                        policy.observe_checkpoint(observation.session.checkpoint.as_ref());
                        policy.observe_completed_turn(observation.latest_completed_turn_seq);
                        if !busy.contains(&session_id)
                            && policy.due(observation.phase, Utc::now())
                            && let Some(expected_target) = observation.session.target.clone()
                        {
                            policy.last_attempted_turn = Some(policy.latest_completed_turn);
                            busy.insert(session_id.clone());
                            busy_tx.send_replace(busy.clone());
                            let completed_tx = completed_tx.clone();
                            let previous_checkpoint = policy.checkpoint.clone();
                            let handle = tokio::runtime::Handle::current();
                            tokio::task::spawn_blocking(move || {
                                let mut state = HelState::default();
                                state.sessions.insert(session_id.clone(), observation.session);
                                let controller = Controller { config: observation.config, state };
                                let outcome = handle
                                    .block_on(controller.create_recovery_checkpoint(&session_id))
                                    .map_err(|error| format!("{error:#}"));
                                let result = RecoveryResult {
                                    session_id,
                                    expected_target,
                                    previous_checkpoint,
                                    outcome,
                                };
                                let _ = completed_tx.send(result);
                            });
                        }
                        let _ = acknowledged.send(());
                    }
                    completed = completed_rx.recv() => {
                        let Some(result) = completed else { break };
                        busy.remove(&result.session_id);
                        busy_tx.send_replace(busy.clone());
                        let policy = policies.entry(result.session_id.clone()).or_default();
                        match &result.outcome {
                            Ok(artifact) => {
                                policy.checkpoint = Some(artifact.metadata.clone());
                                if let Err(error) = record_recovery_success(
                                    &result.session_id,
                                    &artifact.native_session_id,
                                    &artifact.metadata,
                                ) {
                                    tracing::warn!(session_id = %result.session_id, "could not persist recovery copy: {error:#}");
                                }
                            }
                            Err(detail) => {
                                if let Err(error) = record_recovery_failure(&result.session_id, detail) {
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
            },
            results: results_rx,
        }
    }

    pub fn observer(&self) -> RecoveryObserver {
        self.observer.clone()
    }

    pub fn try_result(&mut self) -> Option<RecoveryResult> {
        self.results.try_recv().ok()
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
                .is_none_or(|current| candidate.event_sequence > current.event_sequence)
        }) {
            self.checkpoint = checkpoint.cloned();
        }
    }

    fn due(&self, phase: WorkerPhase, now: chrono::DateTime<Utc>) -> bool {
        if phase != WorkerPhase::Idle
            || self.latest_completed_turn == 0
            || self
                .checkpoint
                .as_ref()
                .is_some_and(|checkpoint| checkpoint.event_sequence >= self.latest_completed_turn)
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

    fn completed(seq: u64) -> SequencedEvent {
        SequencedEvent {
            seq,
            recorded_at_ms: None,
            request_id: None,
            event: WorkerEvent::TurnCompleted,
        }
    }

    #[test]
    fn first_completed_idle_turn_is_due() {
        let mut policy = PolicyState::default();
        policy.observe_completed_turn(latest_completed_turn_seq(&[completed(3)]));
        assert!(policy.due(WorkerPhase::Idle, Utc::now()));
        assert!(!policy.due(WorkerPhase::Running, Utc::now()));
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
                event_sequence: 4,
            }),
            ..Default::default()
        };
        assert!(!policy.due(WorkerPhase::Idle, now));
        policy.checkpoint.as_mut().unwrap().created_at =
            (now - chrono::Duration::minutes(10)).to_rfc3339();
        assert!(policy.due(WorkerPhase::Idle, now));
        policy.checkpoint.as_mut().unwrap().event_sequence = 8;
        assert!(!policy.due(WorkerPhase::Idle, now));
    }

    #[test]
    fn failed_boundary_waits_for_another_completed_turn() {
        let now = Utc::now();
        let policy = PolicyState {
            latest_completed_turn: 8,
            last_attempted_turn: Some(8),
            ..Default::default()
        };
        assert!(!policy.due(WorkerPhase::Idle, now));
    }
}
