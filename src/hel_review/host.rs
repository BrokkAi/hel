//! Where turn review actually runs.
//!
//! The review driver is a pure state machine (`super::driver`); this is the
//! process that feeds it. It lives in the controller daemon, which is the one
//! process that owns every session whether or not anyone is watching: it pumps
//! each session's relay every 150 ms, it is the only SQLite writer, and it
//! hosts the phone server. That is why review lives here and not in a UI. A
//! review started from the terminal survives the terminal closing; a session
//! driven only from a phone is reviewed on the same terms; a session nobody is
//! attached to is reviewed too.
//!
//! Every surface is a projection: the terminal and the phone both render
//! [`RuntimeReviewView`] and both resolve a review by asking the host. Neither
//! owns any part of the review.
//!
//! Shape: one task owns all review state and processes [`HostEvent`]s in
//! order. Everything slow -- capturing a delta, staging a reviewer profile,
//! reading a role's journal -- happens in a spawned task that sends its result
//! back as another event. Nothing here holds a lock across an await, and no
//! two reviews can interleave their state.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use crate::hel_config::ReviewConfig;
use crate::hel_database::TurnReviewState;
use crate::hel_session_manager::{
    ManagedSessionHandle, ManagedSessionView, ReviewerAction, ReviewerOutcome,
    SessionManagerControl, new_command_id,
};
use crate::hel_state::{MaterializedExecutionState, MaterializedSession};
use crate::hel_worker::{RelayCommand, RelayEvent, RelayObservation};

use super::driver::{
    INTENT_ROLE, Resolution, ReviewRequest, RoleStatus, SUPERVISOR_ROLE, TurnReviewDriver,
    TurnReviewPhase, TurnReviewSeed,
};
use super::lanes::{ReviewTier, UserMessage};
use super::verdict::ReviewVerdict;

/// How long an idle reviewing role waits before reading its journal again. An
/// attach answers immediately even when nothing has been journaled, so without
/// this a review with several roles would spin on empty pages.
const ROLE_POLL_IDLE_INTERVAL: Duration = Duration::from_millis(200);

/// What the host tells a surface about one running review.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeReviewView {
    pub session_id: String,
    pub tier: ReviewTier,
    pub phase: TurnReviewPhase,
    pub roles: Vec<RoleStatus>,
    /// What the review is doing, in one line.
    pub status: String,
    /// Present once the review has reached a verdict the user must answer.
    pub verdict: Option<VerdictView>,
    pub started_at_epoch_seconds: u64,
}

/// A verdict as a surface renders it.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerdictView {
    pub kind: VerdictKind,
    /// The findings, or the failure's reason. Empty for a clean verdict, which
    /// resolves itself and is never on screen.
    pub text: String,
    /// Which resolutions this verdict accepts right now. A surface shows the
    /// rest disabled rather than hiding them, so the buttons do not move.
    pub allowed: Vec<Resolution>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerdictKind {
    Clean,
    Findings,
    Failed,
}

/// Why a review could not start. Every variant is something a person can act
/// on, which is why they carry their own sentences rather than a code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartRefusal(pub String);

impl std::fmt::Display for StartRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// The message every surface gives for a prompt held by an open review.
pub const PROMPT_HELD_MESSAGE: &str =
    "a review of the last turn is open; forward, dismiss or cancel it first";

/// Sessions whose prompts an unresolved review is holding.
///
/// This is the authoritative lock, and it is in memory on purpose: the process
/// that owns the review owns the lock, so a lock can never outlive the review
/// that set it. The shipped design kept it in a database row written by the
/// terminal, which is how a killed terminal could hold a session's prompts for
/// ever.
static PROMPT_LOCK: LazyLock<Mutex<BTreeSet<String>>> = LazyLock::new(Mutex::default);

/// Whether a prompt for `session_id` must be refused, and why.
#[must_use]
pub fn prompt_refusal(session_id: &str) -> Option<&'static str> {
    PROMPT_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .contains(session_id)
        .then_some(PROMPT_HELD_MESSAGE)
}

fn hold_prompts(session_id: &str) {
    PROMPT_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(session_id.to_owned());
}

fn release_prompts(session_id: &str) {
    PROMPT_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(session_id);
}

/// Where the host reads the arming configuration. The daemon reloads
/// `config.toml` every 500 ms already, so this closure just reads whatever it
/// last installed.
pub type ReviewConfigSource = Arc<dyn Fn() -> ReviewConfig + Send + Sync>;

/// A handle on the review host. Cheap to clone; every method is a message.
#[derive(Clone)]
pub struct TurnReviewHost {
    events: mpsc::Sender<HostEvent>,
    shared: Arc<HostShared>,
}

/// What surfaces read without waiting for the host's task.
#[derive(Default)]
struct HostShared {
    views: Mutex<BTreeMap<String, RuntimeReviewView>>,
}

impl std::fmt::Debug for TurnReviewHost {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("TurnReviewHost")
    }
}

impl TurnReviewHost {
    /// Starts the host's task. `config` is read at each trigger decision.
    #[must_use]
    pub fn spawn(control: SessionManagerControl, config: ReviewConfigSource) -> Self {
        let (events, receiver) = mpsc::channel(256);
        let shared = Arc::new(HostShared::default());
        let host = Self {
            events: events.clone(),
            shared: shared.clone(),
        };
        tokio::spawn(host_loop(
            HostState {
                control,
                config,
                shared,
                events,
                reviews: BTreeMap::new(),
                preparing: BTreeSet::new(),
                next_epoch: 0,
                sessions: BTreeMap::new(),
                missing_reviewer_reported: BTreeSet::new(),
            },
            receiver,
        ));
        host
    }

    /// Reports one session's latest view. This is the trigger's only input.
    pub fn observe(&self, session_id: &str, view: &ManagedSessionView) {
        // A dropped observation is not worth blocking the daemon's update loop
        // for: the next view arrives within 150 ms and carries the same phase.
        let _ = self.events.try_send(HostEvent::View {
            session_id: session_id.to_owned(),
            snapshot: view
                .snapshot
                .as_ref()
                .map(|snapshot| Box::new(snapshot.materialized.clone())),
        });
    }

    /// Reviews the turn that just finished, on request.
    pub async fn start(&self, session_id: &str, manual: bool) -> Result<(), StartRefusal> {
        let (reply, answer) = oneshot::channel();
        self.events
            .send(HostEvent::Start {
                session_id: session_id.to_owned(),
                manual,
                reply: Some(reply),
            })
            .await
            .map_err(|_| StartRefusal("the review host stopped".to_owned()))?;
        answer
            .await
            .map_err(|_| StartRefusal("the review host stopped".to_owned()))?
    }

    /// Forwards, dismisses, or cancels the open review.
    pub async fn resolve(&self, session_id: &str, resolution: Resolution) -> Result<(), String> {
        let (reply, answer) = oneshot::channel();
        self.events
            .send(HostEvent::Resolve {
                session_id: session_id.to_owned(),
                resolution,
                reply,
            })
            .await
            .map_err(|_| "the review host stopped".to_owned())?;
        answer
            .await
            .map_err(|_| "the review host stopped".to_owned())?
    }

    /// Every open review, for a snapshot a surface renders.
    #[must_use]
    pub fn views(&self) -> Vec<RuntimeReviewView> {
        self.shared
            .views
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }

    /// One session's review, if it has one.
    #[must_use]
    pub fn view(&self, session_id: &str) -> Option<RuntimeReviewView> {
        self.shared
            .views
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(session_id)
            .cloned()
    }

    /// Whether an unresolved review is holding this session's prompts.
    #[must_use]
    pub fn refuses_prompt(&self, session_id: &str) -> bool {
        prompt_refusal(session_id).is_some()
    }
}

/// What the host's task processes, in order.
enum HostEvent {
    View {
        session_id: String,
        snapshot: Option<Box<MaterializedSession>>,
    },
    Start {
        session_id: String,
        manual: bool,
        reply: Option<oneshot::Sender<Result<(), StartRefusal>>>,
    },
    Prepared {
        session_id: String,
        manual: bool,
        reply: Option<oneshot::Sender<Result<(), StartRefusal>>>,
        prepared: Result<Prepared, StartRefusal>,
    },
    /// One asynchronous step of a review that was open when it started.
    ///
    /// `epoch` is which review asked. mjolnir's orchestrator tags every review
    /// outcome with one and drops the ones that no longer match
    /// (`mj-core/src/orchestrator.rs`, `review_outcome_rx`), because a result
    /// arriving after its review was cancelled would otherwise be applied to
    /// whatever review is open now. Session id alone is not enough: a session
    /// can start its next review immediately.
    Step {
        session_id: String,
        epoch: u64,
        step: ReviewStep,
    },
    Resolve {
        session_id: String,
        resolution: Resolution,
        reply: oneshot::Sender<Result<(), String>>,
    },
}

/// One asynchronous step's result, belonging to exactly one review.
enum ReviewStep {
    Delta(Result<Vec<crate::hel_worker::RepoDelta>, String>),
    Analysis(Result<String, String>),
    RoleStarted {
        role: String,
        result: Result<(), String>,
    },
    RoleEvents {
        role: String,
        result: Result<Vec<RelayEvent>, String>,
    },
    Dispatches(Result<Vec<super::lanes::ReviewSubagentRequest>, String>),
}

/// Everything one blocking preparation gathered before a review can start.
struct Prepared {
    state: TurnReviewState,
    reviewer: ReviewerIdentity,
    tier: ReviewTier,
}

/// Which harness reviews, and how it is configured. Read from `[review]`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ReviewerIdentity {
    profile: String,
    model: Option<String>,
    effort: Option<String>,
}

/// One open review and its execution context.
struct ReviewSlot {
    /// Which review this is. Asynchronous results name it, and results that
    /// name another are dropped.
    epoch: u64,
    driver: TurnReviewDriver,
    /// One transcript projection per reviewing role, which is how the host
    /// reads a role's answer out of its own relay journal.
    roles: BTreeMap<String, RoleTranscript>,
    reviewer: ReviewerIdentity,
    state: TurnReviewState,
    /// Bumped per role launch: the sidecar reads a new generation as "this is
    /// a different reviewer" and refuses to reuse the running one.
    generation: u64,
    started_at_epoch_seconds: u64,
}

/// One role's journal, folded far enough to read its final answer.
#[derive(Default)]
struct RoleTranscript {
    session: Option<MaterializedSession>,
    cursor_ordinal: u64,
    cursor_digest: String,
}

impl RoleTranscript {
    fn apply(&mut self, session_id: &str, events: &[RelayEvent]) {
        let session = self
            .session
            .get_or_insert_with(|| MaterializedSession::empty(session_id));
        for event in events {
            let Ok(projected) = crate::hel_projection::project_relay_event(session, event) else {
                continue;
            };
            if crate::hel_projection::apply_committed_projection_event(
                session,
                event,
                projected.mutation,
            )
            .is_err()
            {
                continue;
            }
            self.cursor_ordinal = event.ordinal;
            self.cursor_digest.clone_from(&event.digest);
        }
    }

    /// The role's latest complete answer, which is what the driver reads. Tool
    /// logs and reasoning are deliberately not part of it.
    fn latest_answer(&self) -> Option<String> {
        let session = self.session.as_ref()?;
        session
            .transcript
            .iter()
            .rev()
            .find(|item| item.is_nonempty_agent_message())
            .and_then(|item| {
                let crate::hel_state::TranscriptBody::Agent { chunks, .. } = &item.body else {
                    return None;
                };
                Some(crate::hel_chat::materialized_chunks_text(chunks))
            })
            .filter(|text| !text.trim().is_empty())
    }
}

struct HostState {
    control: SessionManagerControl,
    config: ReviewConfigSource,
    shared: Arc<HostShared>,
    events: mpsc::Sender<HostEvent>,
    reviews: BTreeMap<String, ReviewSlot>,
    /// Sessions whose review is being prepared. Preparation is asynchronous,
    /// so without this an automatic trigger and a manual `/review` racing each
    /// other would both create a review and the second would overwrite the
    /// first.
    preparing: BTreeSet<String>,
    /// Distinguishes reviews. Every asynchronous step carries the epoch of the
    /// review that asked for it, so a late result cannot land on its
    /// successor.
    next_epoch: u64,
    /// The last view seen per session: its execution state, for the
    /// Running→Idle edge, and its materialized transcript, for the seed.
    sessions: BTreeMap<String, SessionWatch>,
    /// Sessions already told that no reviewer is configured. One notice per
    /// session, not one per turn.
    missing_reviewer_reported: BTreeSet<String>,
}

struct SessionWatch {
    execution: MaterializedExecutionState,
    materialized: Option<Box<MaterializedSession>>,
}

async fn host_loop(mut state: HostState, mut events: mpsc::Receiver<HostEvent>) {
    // A review interrupted by a daemon restart is not resumed: the baseline
    // never advanced, so the next review covers the same change, and half a
    // multi-agent fan-out is not worth rebuilding. Clearing the stored flag
    // here is also what keeps a stale row from reading as an open review.
    if let Err(error) =
        tokio::task::spawn_blocking(crate::hel_database::clear_interrupted_turn_reviews).await
    {
        tracing::warn!(%error, "the interrupted-review sweep did not run");
    }
    while let Some(event) = events.recv().await {
        state.handle(event).await;
    }
}

impl HostState {
    async fn handle(&mut self, event: HostEvent) {
        match event {
            HostEvent::View {
                session_id,
                snapshot,
            } => self.observe(session_id, snapshot).await,
            HostEvent::Start {
                session_id,
                manual,
                reply,
            } => self.begin(session_id, manual, reply),
            HostEvent::Prepared {
                session_id,
                manual,
                reply,
                prepared,
            } => self.prepared(session_id, manual, reply, prepared),
            HostEvent::Resolve {
                session_id,
                resolution,
                reply,
            } => {
                let answer = self.resolve(&session_id, resolution);
                let _ = reply.send(answer);
            }
            HostEvent::Step {
                session_id,
                epoch,
                step,
            } => self.step(session_id, epoch, step),
        }
    }

    /// Applies one asynchronous result to the review that asked for it.
    fn step(&mut self, session_id: String, epoch: u64, step: ReviewStep) {
        // A result from a review that has since been cancelled, resolved, or
        // replaced is not this review's business.
        if self.reviews.get(&session_id).map(|slot| slot.epoch) != Some(epoch) {
            return;
        }
        match step {
            ReviewStep::Delta(result) => {
                let requests = match result {
                    Ok(deltas) => self
                        .reviews
                        .get_mut(&session_id)
                        .map(|slot| slot.driver.delta_captured(deltas))
                        .unwrap_or_default(),
                    Err(error) => {
                        self.fail(
                            &session_id,
                            format!("the change could not be captured: {error}"),
                        );
                        return;
                    }
                };
                self.run(&session_id, requests);
            }
            ReviewStep::Analysis(result) => {
                let requests = self
                    .reviews
                    .get_mut(&session_id)
                    .map(|slot| slot.driver.analysis_completed(result))
                    .unwrap_or_default();
                self.run(&session_id, requests);
            }
            ReviewStep::RoleStarted { role, result } => {
                let requests = match self.reviews.get_mut(&session_id) {
                    Some(slot) => match result {
                        Ok(()) => slot.driver.role_started(&role),
                        // A lane that cannot start is a coverage gap the
                        // supervisor is told about; any other role failing to
                        // start fails the review.
                        Err(error) if super::lanes::lane_by_id(&role).is_some() => {
                            slot.driver.lane_failed(&role, error)
                        }
                        Err(error) => slot.driver.request_failed(error),
                    },
                    None => return,
                };
                self.run(&session_id, requests);
                if self.reviews.contains_key(&session_id) {
                    self.poll_role(&session_id, &role, Duration::ZERO);
                }
            }
            ReviewStep::RoleEvents { role, result } => self.role_events(session_id, role, result),
            ReviewStep::Dispatches(result) => {
                let requests = match result {
                    Ok(requests) => self
                        .reviews
                        .get_mut(&session_id)
                        .map(|slot| slot.driver.lanes_dispatched(requests))
                        .unwrap_or_default(),
                    // A dropped dispatch would leave the supervisor waiting for
                    // lanes that never run, so it fails the review rather than
                    // stalling it.
                    Err(error) => {
                        self.fail(
                            &session_id,
                            format!(
                                "the review could not collect the supervisor's specialists: {error}"
                            ),
                        );
                        return;
                    }
                };
                self.run(&session_id, requests);
            }
        }
    }

    /// Watches one session for the edge that arms an automatic review.
    async fn observe(&mut self, session_id: String, snapshot: Option<Box<MaterializedSession>>) {
        let execution = snapshot
            .as_ref()
            .map_or(MaterializedExecutionState::Idle, |snapshot| {
                snapshot.execution
            });
        let previous = self.sessions.insert(
            session_id.clone(),
            SessionWatch {
                execution,
                materialized: snapshot,
            },
        );
        let finished_turn = matches!(
            previous.as_ref().map(|watch| watch.execution),
            Some(MaterializedExecutionState::Running { .. })
        ) && matches!(execution, MaterializedExecutionState::Idle);
        if !finished_turn || !(self.config)().enabled {
            return;
        }
        self.begin(session_id, false, None);
    }

    /// Decides whether a review can start, and prepares one if it can.
    ///
    /// The cheap gates are answered here; the ones that need the database or
    /// the worker are answered in the preparation task, so this never blocks
    /// the host's loop.
    fn begin(
        &mut self,
        session_id: String,
        manual: bool,
        reply: Option<oneshot::Sender<Result<(), StartRefusal>>>,
    ) {
        if let Some(refusal) = self.refuse_start(&session_id) {
            answer(reply, Err(refusal));
            return;
        }
        if self.preparing.contains(&session_id) {
            answer(
                reply,
                Err(StartRefusal("a review is already starting".to_owned())),
            );
            return;
        }
        let config = (self.config)();
        let Some(profile) = config.reviewer_profile().map(str::to_owned) else {
            // Configuration is the only place this can be fixed, so the
            // message names the key. A session hears it once, not once a turn.
            let refusal = StartRefusal(
                "turn review needs a reviewer: set [review] profile in config.toml".to_owned(),
            );
            if self.missing_reviewer_reported.insert(session_id.clone()) {
                self.record_notice(&session_id, refusal.0.clone());
            }
            answer(reply, Err(refusal));
            return;
        };
        let reviewer = ReviewerIdentity {
            profile,
            model: config.model.clone(),
            effort: config.effort.clone(),
        };
        let tier = config.tier;
        let control = self.control.clone();
        let events = self.events.clone();
        let prepare_session = session_id.clone();
        self.preparing.insert(session_id);
        tokio::spawn(async move {
            let prepared = prepare(&control, &prepare_session, &reviewer, tier).await;
            let _ = events
                .send(HostEvent::Prepared {
                    session_id: prepare_session,
                    manual,
                    reply,
                    prepared,
                })
                .await;
        });
    }

    /// The gates that need nothing but the host's own state.
    fn refuse_start(&self, session_id: &str) -> Option<StartRefusal> {
        if self.reviews.contains_key(session_id) {
            return Some(StartRefusal("a review is already open".to_owned()));
        }
        let Some(watch) = self.sessions.get(session_id) else {
            return Some(StartRefusal("this session is not connected".to_owned()));
        };
        if !matches!(watch.execution, MaterializedExecutionState::Idle) {
            return Some(StartRefusal(
                "a review runs between turns; this one is still working".to_owned(),
            ));
        }
        let queued = watch
            .materialized
            .as_ref()
            .is_some_and(|materialized| !materialized.queued_prompts.is_empty());
        if queued {
            // Reviewing now would hold prompts the user has already sent. The
            // review after the queue drains covers the whole batch instead.
            return Some(StartRefusal(
                "prompts are queued; the review waits for them".to_owned(),
            ));
        }
        None
    }

    fn prepared(
        &mut self,
        session_id: String,
        manual: bool,
        reply: Option<oneshot::Sender<Result<(), StartRefusal>>>,
        prepared: Result<Prepared, StartRefusal>,
    ) {
        self.preparing.remove(&session_id);
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(refusal) => {
                answer(reply, Err(refusal));
                return;
            }
        };
        // The session may have started another turn while preparation ran.
        if let Some(refusal) = self.refuse_start(&session_id) {
            answer(reply, Err(refusal));
            return;
        }
        let Some(materialized) = self
            .sessions
            .get(&session_id)
            .and_then(|watch| watch.materialized.as_deref())
        else {
            answer(
                reply,
                Err(StartRefusal(
                    "this session has no transcript yet".to_owned(),
                )),
            );
            return;
        };
        let seed = seed_from_session(
            materialized,
            prepared.tier,
            &prepared.state,
            if manual { "manual" } else { "automatic" },
        );
        let (driver, requests) = TurnReviewDriver::start(seed);
        self.next_epoch = self.next_epoch.saturating_add(1);
        let epoch = self.next_epoch;
        self.reviews.insert(
            session_id.clone(),
            ReviewSlot {
                epoch,
                driver,
                roles: BTreeMap::new(),
                reviewer: prepared.reviewer,
                state: prepared.state,
                generation: 0,
                started_at_epoch_seconds: crate::clock::epoch_seconds(),
            },
        );
        // The lock goes on before any agent starts: a review the user cannot
        // see yet still holds the turn it is about to review.
        hold_prompts(&session_id);
        answer(reply, Ok(()));
        self.run(&session_id, requests);
    }

    /// Forwards, dismisses, or cancels an open review on a surface's request.
    fn resolve(&mut self, session_id: &str, resolution: Resolution) -> Result<(), String> {
        let Some(slot) = self.reviews.get_mut(session_id) else {
            return Err("no review is open for that session".to_owned());
        };
        let requests = match resolution {
            Resolution::Forwarded => {
                if !slot.driver.can_forward() {
                    return Err("there are no findings to forward".to_owned());
                }
                slot.driver.forward()
            }
            Resolution::Dismissed => {
                if slot.driver.verdict().is_none() {
                    return Err("the review has not reached a verdict yet".to_owned());
                }
                slot.driver.dismiss()
            }
            Resolution::Cancelled => slot.driver.cancel(),
            Resolution::NothingToReview | Resolution::CoverageStarted => {
                return Err("that is not a resolution a surface can ask for".to_owned());
            }
        };
        if requests.is_empty() {
            return Err("the review could not be resolved that way".to_owned());
        }
        self.run(session_id, requests);
        Ok(())
    }

    /// Ends a review that cannot continue. Every failure path is the same: a
    /// verdict the user dismisses, and a baseline that stays where it was, so
    /// the change is reviewed again rather than silently skipped.
    fn fail(&mut self, session_id: &str, message: impl Into<String>) {
        let Some(slot) = self.reviews.get_mut(session_id) else {
            return;
        };
        let requests = slot.driver.request_failed(message);
        self.run(session_id, requests);
    }

    fn run(&mut self, session_id: &str, requests: Vec<ReviewRequest>) {
        for request in requests {
            self.run_one(session_id, request);
        }
        self.publish(session_id);
    }

    fn run_one(&mut self, session_id: &str, request: ReviewRequest) {
        match request {
            ReviewRequest::CaptureDelta { baselines } => {
                self.review_step(
                    session_id,
                    ReviewerAction::CaptureDelta { baselines },
                    |outcome| {
                        ReviewStep::Delta(match outcome {
                            Ok(ReviewerOutcome::Delta { repositories }) => Ok(repositories),
                            other => Err(unexpected(other)),
                        })
                    },
                );
            }
            ReviewRequest::AnalyzeDelta { repositories } => {
                self.review_step(
                    session_id,
                    ReviewerAction::AnalyzeDelta { repositories },
                    |outcome| {
                        ReviewStep::Analysis(match outcome {
                            Ok(ReviewerOutcome::ChangedFunctions { packet }) => Ok(packet),
                            other => Err(unexpected(other)),
                        })
                    },
                );
            }
            ReviewRequest::StartRole { role, fresh } => self.start_role(session_id, role, fresh),
            ReviewRequest::PromptRole {
                role,
                command_id,
                prompt,
            } => {
                self.prompt_role(session_id, &role, command_id, prompt);
                self.poll_role(session_id, &role, Duration::ZERO);
            }
            ReviewRequest::PromptPrimary { command_id, prompt } => {
                // The review's own corrective prompt must not be held by the
                // review's own lock, and by this point the review has
                // resolved, so the lock is already released below.
                self.prompt_primary(session_id, command_id, prompt);
            }
            ReviewRequest::PauseRole { role } => {
                let session_id = session_id.to_owned();
                self.spawn_reviewer(
                    session_id.clone(),
                    Some(role),
                    ReviewerAction::Pause,
                    move |outcome| {
                        if let Err(error) = outcome {
                            tracing::debug!(
                                session_id = %session_id,
                                %error,
                                "pausing a review role failed"
                            );
                        }
                        None
                    },
                );
            }
            ReviewRequest::AdvanceBaseline {
                trees,
                reviewed_through_ordinal,
            } => {
                if let Some(slot) = self.reviews.get_mut(session_id) {
                    slot.state.baselines = trees.clone();
                    slot.state.reviewed_through_ordinal = reviewed_through_ordinal;
                    slot.state.active = None;
                    persist(session_id, &slot.state);
                }
                let session_id = session_id.to_owned();
                self.spawn_reviewer(
                    session_id.clone(),
                    None,
                    ReviewerAction::AdvanceBaseline { trees },
                    move |outcome| {
                        if let Err(error) = outcome {
                            // The controller's copy is what the next capture is
                            // taken against; the worker-side ref is only a gc
                            // pin, so a failure here costs nothing but the pin.
                            tracing::debug!(
                                session_id = %session_id,
                                %error,
                                "the review baseline ref could not be pinned"
                            );
                        }
                        None
                    },
                );
            }
            ReviewRequest::RecordPriorReview { prior } => {
                if let Some(slot) = self.reviews.get_mut(session_id) {
                    slot.state.prior_review = Some(prior);
                    persist(session_id, &slot.state);
                }
            }
            ReviewRequest::ClearPriorReview => {
                if let Some(slot) = self.reviews.get_mut(session_id) {
                    slot.state.prior_review = None;
                    persist(session_id, &slot.state);
                }
            }
            ReviewRequest::Close => {
                let notice = self
                    .reviews
                    .get(session_id)
                    .and_then(|slot| resolution_notice(slot.driver.phase()));
                if let Some(mut slot) = self.reviews.remove(session_id) {
                    slot.state.active = None;
                    persist(session_id, &slot.state);
                }
                release_prompts(session_id);
                if let Some(notice) = notice {
                    self.record_notice(session_id, notice);
                }
            }
        }
    }

    /// Stages the configured reviewer profile and starts one role under it.
    fn start_role(&mut self, session_id: &str, role: String, fresh: bool) {
        let Some(slot) = self.reviews.get_mut(session_id) else {
            return;
        };
        // A fresh role must not reuse the running harness session: the
        // validator judges the reviewer's claims against source, so it must
        // not inherit them. Bumping the generation is what the sidecar reads
        // as "this is a different reviewer".
        if fresh {
            slot.generation = slot.generation.saturating_add(1);
        }
        let generation = slot.generation;
        let epoch = slot.epoch;
        let reviewer = slot.reviewer.clone();
        let repositories = slot.driver.repository_roots();
        let control = self.control.clone();
        let events = self.events.clone();
        let session_id = session_id.to_owned();
        tokio::spawn(async move {
            let result = launch_role(
                &control,
                &session_id,
                &role,
                &reviewer,
                generation,
                &repositories,
            )
            .await;
            let _ = events
                .send(HostEvent::Step {
                    session_id,
                    epoch,
                    step: ReviewStep::RoleStarted { role, result },
                })
                .await;
        });
    }

    fn prompt_role(&mut self, session_id: &str, role: &str, command_id: String, prompt: String) {
        let session_id_for_log = session_id.to_owned();
        self.spawn_reviewer(
            session_id.to_owned(),
            Some(role.to_owned()),
            ReviewerAction::Submit {
                command_id,
                command: prompt_command(prompt),
            },
            move |outcome| {
                if let Err(error) = outcome {
                    tracing::warn!(
                        session_id = %session_id_for_log,
                        %error,
                        "a reviewing role could not be prompted"
                    );
                }
                None
            },
        );
    }

    /// Sends the review's corrective prompt to the primary agent.
    fn prompt_primary(&mut self, session_id: &str, command_id: String, prompt: String) {
        let control = self.control.clone();
        let session_id = session_id.to_owned();
        tokio::spawn(async move {
            let submitted = async {
                let handle = control
                    .session(session_id.clone())
                    .await
                    .map_err(|error| format!("{error:#}"))?;
                handle
                    .submit(command_id, prompt_command(prompt))
                    .await
                    .map(|_| ())
                    .map_err(|error| format!("{error:#}"))
            }
            .await;
            if let Err(error) = submitted {
                tracing::warn!(
                    session_id = %session_id,
                    %error,
                    "the review's findings could not be sent to the agent"
                );
            }
        });
    }

    /// Reads one role's journal from where the host left off.
    fn poll_role(&mut self, session_id: &str, role: &str, delay: Duration) {
        let Some(slot) = self.reviews.get_mut(session_id) else {
            return;
        };
        let transcript = slot.roles.entry(role.to_owned()).or_default();
        let after_ordinal = transcript.cursor_ordinal;
        let after_digest = if transcript.cursor_digest.is_empty() {
            crate::hel_worker::RELAY_EVENT_GENESIS_DIGEST.to_owned()
        } else {
            transcript.cursor_digest.clone()
        };
        let epoch = slot.epoch;
        let control = self.control.clone();
        let events = self.events.clone();
        let session_id = session_id.to_owned();
        let role = role.to_owned();
        tokio::spawn(async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            let result = reviewer_action(
                &control,
                &session_id,
                Some(role.clone()),
                ReviewerAction::Attach {
                    after_ordinal,
                    after_digest,
                },
            )
            .await;
            let result = match result {
                Ok(ReviewerOutcome::Attached(attachment)) => Ok(attachment.events),
                other => Err(unexpected(other)),
            };
            let _ = events
                .send(HostEvent::Step {
                    session_id,
                    epoch,
                    step: ReviewStep::RoleEvents { role, result },
                })
                .await;
        });
    }

    fn role_events(
        &mut self,
        session_id: String,
        role: String,
        result: Result<Vec<RelayEvent>, String>,
    ) {
        let events = match result {
            Ok(events) => events,
            Err(error) => {
                self.fail(&session_id, error);
                return;
            }
        };
        let Some(slot) = self.reviews.get_mut(&session_id) else {
            return;
        };
        let idle = events.is_empty();
        let relay_session = role_session_id(&session_id, &role);
        let transcript = slot.roles.entry(role.clone()).or_default();
        transcript.apply(&relay_session, &events);
        // The newest agent message is not enough on its own: after the
        // validator starts, the reviewer's own findings are still the newest
        // message in that role's journal. The relay's completion record for
        // the exact command the driver submitted is what settles it.
        let awaited = slot
            .driver
            .awaited_commands()
            .into_iter()
            .find(|(awaited_role, _)| *awaited_role == role)
            .map(|(_, command_id)| command_id);
        let completed = awaited.as_ref().is_some_and(|awaited| {
            events.iter().any(|event| {
                matches!(
                    &event.observation,
                    RelayObservation::CommandCompleted { command_id, outcome }
                        if command_id == awaited
                            && matches!(
                                outcome,
                                crate::hel_worker::RelayCommandOutcome::Prompt { .. }
                            )
                )
            })
        });
        let requests = match (completed, awaited) {
            (true, Some(awaited)) => {
                let answer = slot
                    .roles
                    .get(&role)
                    .and_then(RoleTranscript::latest_answer)
                    .unwrap_or_default();
                let slot = self.reviews.get_mut(&session_id).expect("the slot is open");
                slot.driver.role_turn_completed(&awaited, &answer)
            }
            _ => Vec::new(),
        };
        self.run(&session_id, requests);
        let Some(slot) = self.reviews.get(&session_id) else {
            return;
        };
        if slot.driver.active_roles().contains(&role) {
            self.poll_role(
                &session_id,
                &role,
                if idle {
                    ROLE_POLL_IDLE_INTERVAL
                } else {
                    Duration::ZERO
                },
            );
        }
        if role == SUPERVISOR_ROLE
            && self
                .reviews
                .get(&session_id)
                .is_some_and(|slot| slot.driver.supervisor_running())
        {
            self.poll_dispatches(&session_id);
        }
    }

    /// Collects the specialist lanes the supervisor asked for through its MCP
    /// tool. The tool answers the supervisor at once and leaves the request in
    /// the worker; this is where the host picks it up and launches them.
    fn poll_dispatches(&mut self, session_id: &str) {
        self.review_step(session_id, ReviewerAction::TakeLaneDispatches, |outcome| {
            ReviewStep::Dispatches(match outcome {
                Ok(ReviewerOutcome::LaneDispatches { requests }) => Ok(requests),
                other => Err(unexpected(other)),
            })
        });
    }

    /// Puts one controller-authored line into the session's conversation, so a
    /// resolution is visible on every surface rather than in one UI's notice
    /// bar.
    fn record_notice(&self, session_id: &str, text: String) {
        let control = self.control.clone();
        let session_id = session_id.to_owned();
        tokio::spawn(async move {
            let recorded = async {
                let handle = control
                    .session(session_id.clone())
                    .await
                    .map_err(|error| format!("{error:#}"))?;
                let command_id =
                    new_command_id("turn-review-notice").map_err(|error| format!("{error:#}"))?;
                handle
                    .submit(command_id, RelayCommand::RecordNotice { text })
                    .await
                    .map(|_| ())
                    .map_err(|error| format!("{error:#}"))
            }
            .await;
            // The conversation line is a courtesy; a relay that refuses it has
            // not damaged the review.
            if let Err(error) = recorded {
                tracing::debug!(
                    session_id = %session_id,
                    %error,
                    "could not record a review notice in the conversation"
                );
            }
        });
    }

    /// Runs one reviewer action for the default role and feeds its outcome
    /// back to the review that asked for it.
    fn review_step(
        &mut self,
        session_id: &str,
        action: ReviewerAction,
        into_step: impl FnOnce(Result<ReviewerOutcome, String>) -> ReviewStep + Send + 'static,
    ) {
        let Some(epoch) = self.reviews.get(session_id).map(|slot| slot.epoch) else {
            return;
        };
        let owner = session_id.to_owned();
        self.spawn_reviewer(session_id.to_owned(), None, action, move |outcome| {
            Some(HostEvent::Step {
                session_id: owner,
                epoch,
                step: into_step(outcome),
            })
        });
    }

    fn spawn_reviewer(
        &self,
        session_id: String,
        role: Option<String>,
        action: ReviewerAction,
        into_event: impl FnOnce(Result<ReviewerOutcome, String>) -> Option<HostEvent> + Send + 'static,
    ) {
        let control = self.control.clone();
        let events = self.events.clone();
        tokio::spawn(async move {
            let outcome = reviewer_action(&control, &session_id, role, action).await;
            if let Some(event) = into_event(outcome) {
                let _ = events.send(event).await;
            }
        });
    }

    /// Republishes what surfaces read. Called after every state change, so a
    /// snapshot poll and a phone request see the same review.
    fn publish(&self, session_id: &str) {
        let mut views = self
            .shared
            .views
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match self.reviews.get(session_id) {
            Some(slot) => {
                views.insert(session_id.to_owned(), slot.view(session_id));
            }
            None => {
                views.remove(session_id);
            }
        }
    }
}

impl ReviewSlot {
    fn view(&self, session_id: &str) -> RuntimeReviewView {
        RuntimeReviewView {
            session_id: session_id.to_owned(),
            tier: self.driver.tier(),
            phase: self.driver.phase().clone(),
            roles: self.driver.roles(),
            status: self.driver.status().to_owned(),
            verdict: self.driver.verdict().map(|verdict| match verdict {
                ReviewVerdict::Clean => VerdictView {
                    kind: VerdictKind::Clean,
                    text: String::new(),
                    allowed: Vec::new(),
                },
                ReviewVerdict::Findings { synthesis, .. } => VerdictView {
                    kind: VerdictKind::Findings,
                    text: synthesis.clone(),
                    allowed: vec![
                        Resolution::Forwarded,
                        Resolution::Dismissed,
                        Resolution::Cancelled,
                    ],
                },
                ReviewVerdict::Failed { reason } => VerdictView {
                    kind: VerdictKind::Failed,
                    text: reason.clone(),
                    // A failed review has nothing to forward, and dismissing
                    // it does not advance the baseline: the change stays
                    // unreviewed either way.
                    allowed: vec![Resolution::Dismissed, Resolution::Cancelled],
                },
            }),
            started_at_epoch_seconds: self.started_at_epoch_seconds,
        }
    }
}

fn answer(
    reply: Option<oneshot::Sender<Result<(), StartRefusal>>>,
    result: Result<(), StartRefusal>,
) {
    if let Some(reply) = reply {
        let _ = reply.send(result);
    }
}

fn unexpected(outcome: Result<ReviewerOutcome, String>) -> String {
    match outcome {
        Ok(other) => format!("unexpected reviewer response {other:?}"),
        Err(error) => error,
    }
}

fn prompt_command(prompt: String) -> RelayCommand {
    RelayCommand::Prompt {
        prompt: vec![agent_client_protocol::schema::v1::ContentBlock::Text(
            agent_client_protocol::schema::v1::TextContent::new(prompt),
        )],
    }
}

/// The relay session id one reviewing role journals under. The default role
/// keeps the plan reviewer's id, which is the one the worker uses.
#[must_use]
pub fn role_session_id(primary_session_id: &str, role: &str) -> String {
    if role == super::driver::REVIEWER_ROLE {
        format!("{primary_session_id}-reviewer")
    } else {
        format!("{primary_session_id}-review-{role}")
    }
}

async fn reviewer_action(
    control: &SessionManagerControl,
    session_id: &str,
    role: Option<String>,
    action: ReviewerAction,
) -> Result<ReviewerOutcome, String> {
    let handle: ManagedSessionHandle = control
        .session(session_id.to_owned())
        .await
        .map_err(|error| format!("{error:#}"))?;
    handle
        .reviewer_as(role, action)
        .await
        .map_err(|error| format!("{error:#}"))
}

/// Stages the configured reviewer profile and starts one role under it.
async fn launch_role(
    control: &SessionManagerControl,
    session_id: &str,
    role: &str,
    reviewer: &ReviewerIdentity,
    generation: u64,
    repositories: &[PathBuf],
) -> Result<(), String> {
    // A specialist lane's analyzers are its identity, so it gets the `slopcop`
    // set as well as navigation; every other role navigates and reads rather
    // than running analyzers. The intent analyst gets no tools at all: it
    // reads the user's messages, not the code.
    let lane = super::lanes::lane_by_id(role).is_some();
    let mcp_servers = if role == INTENT_ROLE {
        Vec::new()
    } else {
        super::bifrost::review_mcp_servers(
            repositories,
            if lane {
                super::lanes::LANE_BIFROST_TOOLSET
            } else {
                super::lanes::SUPERVISOR_BIFROST_TOOLSET
            },
        )
    };
    // Only the supervisor may launch specialists.
    let dispatch_tool = role == SUPERVISOR_ROLE;
    let staged = {
        let session_id = session_id.to_owned();
        let profile = reviewer.profile.clone();
        tokio::task::spawn_blocking(move || {
            let controller = crate::hel_controller::Controller::load()?;
            controller.stage_reviewer_profile_with_mcp(
                &session_id,
                &profile,
                generation,
                &mcp_servers,
                dispatch_tool,
            )
        })
        .await
        .map_err(|error| format!("staging the reviewer stopped: {error}"))?
        .map_err(|error| format!("{error:#}"))?
    };
    let mut config = staged;
    config.model = reviewer.model.clone();
    config.effort = reviewer.effort.clone();
    match reviewer_action(
        control,
        session_id,
        Some(role.to_owned()),
        ReviewerAction::Start {
            config: Box::new(config),
        },
    )
    .await
    {
        Ok(ReviewerOutcome::Started(_)) => Ok(()),
        other => Err(unexpected(other)),
    }
}

/// Everything a review needs that only the database and the worker can answer.
async fn prepare(
    control: &SessionManagerControl,
    session_id: &str,
    reviewer: &ReviewerIdentity,
    tier: ReviewTier,
) -> Result<Prepared, StartRefusal> {
    let profile = reviewer.profile.clone();
    let session = session_id.to_owned();
    let checked = tokio::task::spawn_blocking(move || -> Result<TurnReviewState, String> {
        let controller =
            crate::hel_controller::Controller::load().map_err(|error| format!("{error:#}"))?;
        if !controller.config.profiles.contains_key(&profile) {
            return Err(format!(
                "turn review needs a reviewer: [review] profile {profile:?} is not a profile in config.toml"
            ));
        }
        if controller
            .state
            .sessions
            .get(&session)
            .is_some_and(|record| record.archived)
        {
            return Err("this session is archived".to_owned());
        }
        crate::hel_database::turn_review_state(&session).map_err(|error| format!("{error:#}"))
    })
    .await
    .map_err(|error| StartRefusal(format!("preparing the review stopped: {error}")))?;
    let state = checked.map_err(StartRefusal)?;
    // Mutual exclusion with a plan-review second opinion: they share the
    // default reviewer role, and the running one keeps the slot. Checked
    // against the worker rather than against any UI's state, because the
    // worker is the only place that knows.
    match reviewer_action(control, session_id, None, ReviewerAction::Status).await {
        Ok(ReviewerOutcome::Status(state)) if state.active_prompt.is_some() => {
            return Err(StartRefusal(
                "the reviewer is busy with a second opinion".to_owned(),
            ));
        }
        Ok(_) => {}
        Err(error) => return Err(StartRefusal(error)),
    }
    Ok(Prepared {
        state,
        reviewer: reviewer.clone(),
        tier,
    })
}

fn persist(session_id: &str, state: &TurnReviewState) {
    if let Err(error) = crate::hel_database::save_turn_review_state(session_id, state) {
        tracing::warn!(
            session_id = %session_id,
            error = %format!("{error:#}"),
            "could not record how far this session has been reviewed"
        );
    }
}

/// The transcript line a resolution leaves behind, on every surface.
#[must_use]
pub fn resolution_notice(phase: &TurnReviewPhase) -> Option<String> {
    let TurnReviewPhase::Resolved(resolution) = phase else {
        return None;
    };
    Some(match resolution {
        Resolution::Forwarded => "Review findings sent to the agent".to_owned(),
        Resolution::Dismissed => "Review dismissed".to_owned(),
        Resolution::Cancelled => "Review cancelled".to_owned(),
        Resolution::NothingToReview => "Nothing to review: the turn changed no files".to_owned(),
        Resolution::CoverageStarted => {
            "Review coverage starts here; the next completed turn is reviewed".to_owned()
        }
    })
}

/// Builds the review's seed from the session's own projection.
///
/// This is the daemon-side twin of what the chat used to read out of its view
/// state: the opening prompt is the task, the user messages after the last
/// completed review are the intent, the agent's closing message is the result,
/// and a compact trajectory says what it did.
fn seed_from_session(
    session: &MaterializedSession,
    tier: ReviewTier,
    state: &TurnReviewState,
    _trigger: &str,
) -> TurnReviewSeed {
    let reviewed_through = state.reviewed_through_ordinal;
    let mut task = String::new();
    let mut user_messages = Vec::new();
    let mut initial_result = String::new();
    let mut trajectory = Vec::new();
    for item in &session.transcript {
        match &item.body {
            crate::hel_state::TranscriptBody::User { content } => {
                let text = crate::hel_chat::materialized_content_text(content);
                let text = text.trim();
                if text.is_empty() {
                    continue;
                }
                if task.is_empty() {
                    task = text.to_owned();
                }
                if item.position > reviewed_through
                    && !crate::hel_second_opinion::is_control_origin_prompt(text)
                {
                    // Hel's own generated prompts -- a forwarded review, a
                    // plan-review context request -- are not user intent, and
                    // reading them as intent would let a review grade the
                    // agent against Hel's words.
                    user_messages.push(UserMessage::prompt(text));
                    trajectory.push(format!("user: {text}"));
                }
            }
            crate::hel_state::TranscriptBody::Agent { chunks, .. } => {
                if !item.is_nonempty_agent_message() {
                    continue;
                }
                let text = crate::hel_chat::materialized_chunks_text(chunks);
                let text = text.trim();
                if text.is_empty() {
                    continue;
                }
                initial_result = text.to_owned();
                if item.position > reviewed_through {
                    trajectory.push(format!("agent: {text}"));
                }
            }
            crate::hel_state::TranscriptBody::Tool { call, .. } => {
                // The tool's own title, straight out of the stored ACP call:
                // the trajectory says what the agent did, and the captured
                // patch already carries what it changed.
                let title = call
                    .get("title")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .trim();
                if item.position > reviewed_through && !title.is_empty() {
                    trajectory.push(format!("tool: {title}"));
                }
            }
            _ => {}
        }
    }
    if task.is_empty() {
        task = user_messages
            .first()
            .map(|message| message.text.clone())
            .unwrap_or_default();
    }
    TurnReviewSeed {
        tier,
        task,
        user_messages,
        initial_result,
        trajectory: trajectory.join("\n"),
        baselines: state.baselines.clone(),
        through_ordinal: session.applied_event_ordinal,
        prior_review: state.prior_review.clone(),
    }
}
