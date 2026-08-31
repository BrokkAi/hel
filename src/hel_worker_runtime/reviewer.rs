//! The second-opinion reviewer that runs beside a primary session.
//!
//! A reviewer is a sidecar, not a session. It shares the primary's target and
//! working directory and owns nothing else: its harness home is a fresh copy
//! of the chosen profile, staged under the primary worker root, and it keeps
//! its own native session and durable relay there. Hel never gives it a
//! session record, a target, a repository checkout, or a lifecycle operation.
//!
//! Reusing [`DurableRelay`] for the reviewer is deliberate. It makes the
//! reviewer's conversation journaled, replayable and recoverable on exactly
//! the terms the primary's is, so the controller projects and renders it with
//! the code it already has instead of a parallel transcript pipeline.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::unix::{ACP_EVENT_CHANNEL_CAPACITY, run_relay_coordinator};
use super::{AcpSupervisorSpec, REVIEWER_DIR, REVIEWER_PROFILE_DIR, ReviewerLaunchConfig};
use crate::hel_acp::{self, CommandRequest, LaunchSpec};
use crate::hel_worker::{
    DurableRelay, RelayCommand, RelayCursor, RelayEvent, RelayObservation, RelayOperationalState,
    RelayRequest, RelayRequestEnvelope, RelayResponseBody, RelayResponseEnvelope,
    RelayResponsePayload,
};

/// How long a reviewer may take to open its native session and advertise its
/// configuration. A harness that has to authenticate or warm a large profile
/// is slow, but a harness that never answers must not hang the controller.
const START_TIMEOUT: Duration = Duration::from_secs(180);
/// How long one configuration change may take to apply.
const CONFIGURE_TIMEOUT: Duration = Duration::from_secs(60);
/// How long a paused reviewer's runtime is given to terminate its harness
/// process group before the pause gives up and reports the leak.
const PAUSE_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a cancelled reviewer turn is given to leave the relay idle before
/// the pause stops waiting for it.
const CANCEL_TIMEOUT: Duration = Duration::from_secs(5);
/// Interval between reads of the reviewer relay's durable state while waiting.
const POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Everything a reviewer inherits from the primary session it reviews for.
#[derive(Debug, Clone)]
pub struct ReviewerPlacement {
    /// The primary worker root. The reviewer lives in a subdirectory of it.
    pub worker_root: PathBuf,
    /// Primary session id, used to name the reviewer's relay session.
    pub session_id: String,
    /// The primary's working directory. The reviewer reads the same tree.
    pub cwd: PathBuf,
    /// The primary's additional workspace roots.
    pub additional_directories: Vec<PathBuf>,
    /// This worker's own executable, which supervises the reviewer's bridge
    /// exactly as it supervises the primary's.
    pub worker_executable: PathBuf,
}

impl ReviewerPlacement {
    #[must_use]
    pub fn root(&self) -> PathBuf {
        self.worker_root.join(REVIEWER_DIR)
    }

    #[must_use]
    pub fn profile_home(&self) -> PathBuf {
        self.root().join(REVIEWER_PROFILE_DIR)
    }

    /// Relay session id for the reviewer. It is derived from the primary's so
    /// worker logs name the pair, and it is distinct so the two relays can
    /// never be mistaken for one another.
    #[must_use]
    fn relay_session_id(&self) -> String {
        format!("{}-reviewer", self.session_id)
    }
}

/// The reviewer's live process and the tasks driving it.
struct RunningReviewer {
    config: ReviewerLaunchConfig,
    commands: mpsc::Sender<CommandRequest>,
    dispatch_wake: mpsc::Sender<()>,
    acp: JoinHandle<Result<()>>,
    coordinator: JoinHandle<Result<()>>,
}

/// Owns the reviewer sidecar for one primary worker.
pub struct ReviewerSidecar {
    placement: ReviewerPlacement,
    relay: Option<Arc<Mutex<DurableRelay>>>,
    running: Option<RunningReviewer>,
    /// Distinguishes the configuration commands this sidecar submits itself.
    config_sequence: u64,
}

impl ReviewerSidecar {
    #[must_use]
    pub fn new(placement: ReviewerPlacement) -> Self {
        Self {
            placement,
            relay: None,
            running: None,
            config_sequence: 0,
        }
    }

    /// Serves one reviewer request. Every variant answers with an ordinary
    /// relay response, so the controller decodes reviewer replies with the
    /// code it already uses for the primary.
    pub async fn handle(
        &mut self,
        envelope: RelayRequestEnvelope,
        request: crate::hel_worker::ReviewerRequest,
    ) -> RelayResponseEnvelope {
        let request_id = envelope.request_id;
        let protocol_version = envelope.protocol_version;
        let body = match self.dispatch(request).await {
            Ok(body) => body,
            Err(error) => reviewer_error(format!("{error:#}")),
        };
        RelayResponseEnvelope {
            request_id,
            protocol_version,
            body,
        }
    }

    async fn dispatch(
        &mut self,
        request: crate::hel_worker::ReviewerRequest,
    ) -> Result<RelayResponseBody> {
        use crate::hel_worker::ReviewerRequest;

        match request {
            ReviewerRequest::Start { config } => self.start(*config).await,
            ReviewerRequest::Pause => {
                self.pause().await;
                Ok(RelayResponseBody::Ok {
                    payload: RelayResponsePayload::ReviewerPaused,
                })
            }
            ReviewerRequest::Attach {
                after_ordinal,
                after_digest,
            } => self.forward(RelayRequest::Attach {
                after_ordinal,
                after_digest,
            }),
            ReviewerRequest::Acknowledge {
                through_ordinal,
                through_digest,
            } => self.forward(RelayRequest::Acknowledge {
                through_ordinal,
                through_digest,
            }),
            ReviewerRequest::Submit {
                command_id,
                command,
            } => {
                let response = self.forward(RelayRequest::Submit {
                    command_id,
                    command,
                })?;
                self.wake_dispatch();
                Ok(response)
            }
            ReviewerRequest::Status => self.forward(RelayRequest::Status),
            ReviewerRequest::RespondElicitation {
                elicitation_id,
                response,
            } => self.respond_elicitation(elicitation_id, response).await,
        }
    }

    /// Answers a form the reviewer's harness is waiting on.
    ///
    /// The answer goes straight to the reviewer's ACP runtime, never through
    /// its command queue: form content is the user's, and the primary's
    /// answers are kept out of the durable ledger for the same reason.
    async fn respond_elicitation(
        &mut self,
        elicitation_id: String,
        response: crate::hel_elicitation::ElicitationResponse,
    ) -> Result<RelayResponseBody> {
        let Some(running) = self.running.as_ref() else {
            bail!("no reviewer is running to answer that form");
        };
        let (resolved, resolution) = tokio::sync::oneshot::channel();
        running
            .commands
            .send(CommandRequest::ResolveElicitation {
                elicitation_id: elicitation_id.clone(),
                response,
                resolved,
            })
            .await
            .map_err(|_| anyhow::anyhow!("the reviewer runtime stopped before it could answer"))?;
        match resolution.await {
            Ok(Ok(())) => Ok(RelayResponseBody::Ok {
                payload: RelayResponsePayload::ElicitationResolved { elicitation_id },
            }),
            Ok(Err(message)) => bail!("{message}"),
            Err(_) => bail!("the reviewer runtime stopped before it answered"),
        }
    }

    /// Starts the reviewer, or reports the running one when it already matches
    /// `config`. A configuration that names a different profile or a newer
    /// generation replaces the running reviewer rather than reusing it.
    async fn start(&mut self, config: ReviewerLaunchConfig) -> Result<RelayResponseBody> {
        let profile_home = self.placement.profile_home();
        if !profile_home.is_dir() {
            bail!(
                "reviewer profile has not been staged at {}",
                profile_home.display()
            );
        }
        let reused = match self.running.as_ref() {
            Some(running) if running.config.reusable_for(&config) => true,
            Some(_) => {
                // A different profile or a new generation is a different
                // reviewer. Stop the old process group before its replacement
                // touches the same staged directory.
                self.pause().await;
                false
            }
            None => false,
        };
        if !reused {
            self.launch(&config).await?;
        }
        self.request_plan_mode(&config).await;
        self.apply_configuration(&config).await?;
        let state = self.state()?;
        Ok(RelayResponseBody::Ok {
            payload: RelayResponsePayload::ReviewerStarted {
                native_session_id: state.native_session_id.clone(),
                config_options: state.config_options.clone(),
                reused,
                state: Box::new(state),
            },
        })
    }

    /// Spawns the reviewer's harness and waits for it to open a session and
    /// advertise its configuration.
    async fn launch(&mut self, config: &ReviewerLaunchConfig) -> Result<()> {
        let root = self.placement.root();
        std::fs::create_dir_all(&root)
            .with_context(|| format!("create reviewer root {}", root.display()))?;
        let relay = self.open_relay()?;

        let mut environment = config.environment.clone();
        // The worker fixes the harness home itself: a controller must never be
        // able to aim a reviewer at the primary's credentials.
        environment.insert(
            config.harness.home_env().into(),
            self.placement.profile_home().to_string_lossy().into_owned(),
        );
        config
            .harness
            .configure_execution_environment(config.execution_policy, &mut environment);

        let supervisor_path = root.join("acp-supervisor.json");
        AcpSupervisorSpec {
            command: config.bridge_command.clone(),
            args: config.bridge_args.clone(),
            environment,
            cwd: self.placement.cwd.clone(),
        }
        .write_spec(&supervisor_path)?;

        // Captured before the harness starts, so a later scan sees only what
        // this launch produced and never an earlier one's events.
        let cursor = {
            let relay = relay.lock().expect("reviewer relay lock poisoned");
            RelayCursor {
                ordinal: relay.latest_ordinal(),
                digest: relay.latest_digest().to_owned(),
            }
        };
        let resume_session = relay
            .lock()
            .expect("reviewer relay lock poisoned")
            .operational_state()
            .native_session_id;
        let spec = LaunchSpec {
            command: self.placement.worker_executable.clone(),
            args: vec![
                "worker".into(),
                "acp-supervisor".into(),
                "--spec".into(),
                supervisor_path.to_string_lossy().into_owned(),
            ],
            environment: Default::default(),
            cwd: self.placement.cwd.clone(),
            additional_directories: self.placement.additional_directories.clone(),
            // A reviewer reads the workspace; it never syncs project memory,
            // which belongs to the primary session alone.
            project_memory: None,
            resume_session,
            harness: config.harness,
            execution_policy: config.execution_policy,
            acp_activity: relay
                .lock()
                .expect("reviewer relay lock poisoned")
                .acp_activity_clock(),
        };

        let (commands_tx, commands_rx) = mpsc::channel(32);
        let (events_tx, events_rx) = mpsc::channel(ACP_EVENT_CHANNEL_CAPACITY);
        let (wake_tx, wake_rx) = mpsc::channel(1);
        let acp = tokio::spawn(hel_acp::run(spec, commands_rx, events_tx));
        let coordinator = tokio::spawn(run_relay_coordinator(
            relay.clone(),
            events_rx,
            wake_rx,
            commands_tx.clone(),
        ));
        self.running = Some(RunningReviewer {
            config: config.clone(),
            commands: commands_tx,
            dispatch_wake: wake_tx,
            acp,
            coordinator,
        });

        // The relay is the durable truth about the harness, so readiness is
        // read from it rather than from a side channel that a restart would
        // not reproduce. `SessionConfigured` is the event to wait for, not a
        // non-empty option list: a harness that advertises no selectors is
        // ready too, and the waterfall offers it a harness default.
        let ready = self
            .wait_for_observation(START_TIMEOUT, &cursor, |observation| {
                matches!(observation, RelayObservation::SessionConfigured { .. })
            })
            .await;
        if ready.is_err() {
            let failure = self.failure_since(&cursor).unwrap_or_else(|| {
                "the reviewer harness did not open a session in time".to_owned()
            });
            self.pause().await;
            bail!("{failure}");
        }
        Ok(())
    }

    /// Asks the reviewer's harness for plan mode when it has one.
    ///
    /// This is a request, not a guarantee: Hel does not claim the reviewer is
    /// read-only, and its prompt says not to implement for the same reason. A
    /// harness with no plan mode simply keeps the one it has.
    async fn request_plan_mode(&mut self, config: &ReviewerLaunchConfig) {
        let Ok(state) = self.state() else {
            return;
        };
        // The same harness-aware decision the primary's /plan uses, so a
        // reviewer asks for plan mode exactly the way a session does.
        let mut surface = crate::hel_acp::surface::AcpSessionSurface::default();
        surface.set_harness_kind(config.harness);
        surface.set_config_options(&state.config_options);
        surface.set_session_modes(state.modes.clone());
        let Ok(control) = surface.plan_control(true) else {
            return;
        };
        let command = match control {
            crate::hel_acp::PlanControl::SetConfig { key, value } => {
                RelayCommand::SetConfig { key, value }
            }
            crate::hel_acp::PlanControl::SetSessionMode { mode_id } => {
                RelayCommand::SetSessionMode { mode_id }
            }
        };
        self.config_sequence += 1;
        let command_id = format!("reviewer-plan-mode-{}", self.config_sequence);
        let cursor = match self.cursor() {
            Ok(cursor) => cursor,
            Err(_) => return,
        };
        if self
            .forward(RelayRequest::Submit {
                command_id: command_id.clone(),
                command,
            })
            .is_err()
        {
            return;
        }
        self.wake_dispatch();
        // A harness that refuses plan mode is not a failure: the review still
        // runs, and the prompt is what actually asks the reviewer not to act.
        let _ = self
            .wait_for_observation(CONFIGURE_TIMEOUT, &cursor, |observation| {
                matches!(
                    observation,
                    RelayObservation::CommandCompleted { command_id: done, .. }
                        | RelayObservation::CommandRejected { command_id: done, .. }
                        | RelayObservation::CommandInterrupted { command_id: done, .. }
                    if *done == command_id
                )
            })
            .await;
    }

    /// Applies the chosen model and effort on the live reviewer. A `None`
    /// choice means the harness advertises no such selector, so nothing is
    /// sent: the reviewer keeps whatever its profile configures.
    async fn apply_configuration(&mut self, config: &ReviewerLaunchConfig) -> Result<()> {
        for (key, value) in [("model", &config.model), ("effort", &config.effort)] {
            let Some(value) = value else {
                continue;
            };
            if self
                .state()?
                .config
                .get(key)
                .is_some_and(|current| current == value)
            {
                continue;
            }
            self.config_sequence += 1;
            let command_id = format!("reviewer-{key}-{}", self.config_sequence);
            let cursor = self.cursor()?;
            let body = self.forward(RelayRequest::Submit {
                command_id: command_id.clone(),
                command: RelayCommand::SetConfig {
                    key: key.to_owned(),
                    value: value.clone(),
                },
            })?;
            if let RelayResponseBody::Error { error } = body {
                bail!(
                    "reviewer could not accept {key} {value:?}: {}",
                    error.message
                );
            }
            self.wake_dispatch();
            // Waiting for the command's own completion, not for the value in
            // the relay's configuration map, is what makes the refreshed
            // option list part of the answer: the runtime records the value,
            // then the refreshed options, then the completion.
            let settled = self
                .wait_for_observation(CONFIGURE_TIMEOUT, &cursor, |observation| {
                    matches!(
                        observation,
                        RelayObservation::CommandCompleted { command_id: done, .. }
                            | RelayObservation::CommandRejected { command_id: done, .. }
                            | RelayObservation::CommandInterrupted { command_id: done, .. }
                        if *done == command_id
                    )
                })
                .await;
            let applied = self.state()?.config.get(key) == Some(value);
            if settled.is_err() || !applied {
                let failure = self
                    .failure_since(&cursor)
                    .unwrap_or_else(|| format!("the reviewer did not apply {key} {value:?}"));
                bail!("{failure}");
            }
        }
        Ok(())
    }

    /// Cancels any turn in flight and stops the reviewer's process group,
    /// keeping its staged profile, native session and journal.
    pub async fn pause(&mut self) {
        let Some(running) = self.running.take() else {
            return;
        };
        // Ask the harness to stop the turn first, so a paused reviewer is not
        // reloaded mid-answer next time.
        self.config_sequence += 1;
        let command_id = format!("reviewer-cancel-{}", self.config_sequence);
        if self
            .forward(RelayRequest::Submit {
                command_id,
                command: RelayCommand::Cancel,
            })
            .is_ok()
        {
            let _ = running.dispatch_wake.try_send(());
            let _ = self
                .wait_for(CANCEL_TIMEOUT, |state| state.active_prompt.is_none())
                .await;
        }

        // The coordinator holds the only other command sender. Stopping it
        // first is what lets the runtime see a closed channel, shut its bridge
        // down gracefully, and terminate the harness process group. Killing
        // the runtime instead would strand that group.
        running.coordinator.abort();
        let _ = running.coordinator.await;
        drop(running.commands);
        drop(running.dispatch_wake);
        match tokio::time::timeout(PAUSE_TIMEOUT, running.acp).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => {
                tracing::debug!(
                    operation = "reviewer_pause",
                    error = format!("{error:#}"),
                    "the reviewer runtime reported a failure while stopping"
                );
            }
            Ok(Err(error)) => {
                tracing::warn!(
                    operation = "reviewer_pause",
                    %error,
                    "the reviewer runtime task stopped abnormally"
                );
            }
            Err(_) => {
                // Reported rather than dropped: the harness process group may
                // still be alive, and only a report makes that visible.
                tracing::error!(
                    operation = "reviewer_pause",
                    "the reviewer runtime did not stop within {PAUSE_TIMEOUT:?}; \
                     its harness process group may still be running"
                );
            }
        }
    }

    /// Opens the reviewer's relay, creating its journal on first use.
    fn open_relay(&mut self) -> Result<Arc<Mutex<DurableRelay>>> {
        if let Some(relay) = &self.relay {
            return Ok(relay.clone());
        }
        let relay = DurableRelay::open(
            self.placement.root(),
            self.placement.relay_session_id(),
            env!("CARGO_PKG_VERSION"),
        )
        .context("open the reviewer relay")?;
        let relay = Arc::new(Mutex::new(relay));
        self.relay = Some(relay.clone());
        Ok(relay)
    }

    /// Hands one request to the reviewer's own relay.
    fn forward(&mut self, request: RelayRequest) -> Result<RelayResponseBody> {
        let relay = self.open_relay()?;
        let envelope = RelayRequestEnvelope {
            request_id: "reviewer".to_owned(),
            protocol_version: crate::hel_worker::RELAY_PROTOCOL_VERSION,
            request,
        };
        let response = relay
            .lock()
            .expect("reviewer relay lock poisoned")
            .handle(envelope);
        Ok(response.body)
    }

    fn state(&mut self) -> Result<RelayOperationalState> {
        let relay = self.open_relay()?;
        let state = relay
            .lock()
            .expect("reviewer relay lock poisoned")
            .operational_state();
        Ok(state)
    }

    fn wake_dispatch(&self) {
        if let Some(running) = &self.running {
            let _ = running.dispatch_wake.try_send(());
        }
    }

    /// Waits until the reviewer's durable state satisfies `ready`, or until
    /// the runtime stops or the deadline passes.
    async fn wait_for(
        &mut self,
        timeout: Duration,
        ready: impl Fn(&RelayOperationalState) -> bool,
    ) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if ready(&self.state()?) {
                return Ok(());
            }
            if self
                .running
                .as_ref()
                .is_none_or(|running| running.acp.is_finished())
            {
                bail!("the reviewer runtime stopped");
            }
            if tokio::time::Instant::now() >= deadline {
                bail!("timed out waiting for the reviewer");
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// A cursor at the reviewer relay's current frontier, for scanning only
    /// the events an operation is about to produce.
    fn cursor(&mut self) -> Result<RelayCursor> {
        let relay = self.open_relay()?;
        let relay = relay.lock().expect("reviewer relay lock poisoned");
        Ok(RelayCursor {
            ordinal: relay.latest_ordinal(),
            digest: relay.latest_digest().to_owned(),
        })
    }

    /// Everything the reviewer journaled after `cursor`.
    fn events_since(&mut self, cursor: &RelayCursor) -> Vec<RelayEvent> {
        let Some(relay) = self.relay.as_ref().cloned() else {
            return Vec::new();
        };
        let relay = relay.lock().expect("reviewer relay lock poisoned");
        relay
            .events_after(cursor.ordinal, &cursor.digest)
            .unwrap_or_default()
    }

    /// Why the operation that started at `cursor` failed, as the reviewer's
    /// own runtime recorded it. Reporting the harness's words beats reporting
    /// that Hel gave up waiting.
    fn failure_since(&mut self, cursor: &RelayCursor) -> Option<String> {
        self.events_since(cursor)
            .iter()
            .rev()
            .find_map(|event| match &event.observation {
                RelayObservation::Warning { message } => Some(message.clone()),
                RelayObservation::CommandRejected { message, .. }
                | RelayObservation::CommandInterrupted { message, .. } => Some(message.clone()),
                _ => None,
            })
    }

    /// Waits until the reviewer journals an observation matching `wanted`
    /// after `cursor`, or until the runtime stops or the deadline passes.
    async fn wait_for_observation(
        &mut self,
        timeout: Duration,
        cursor: &RelayCursor,
        wanted: impl Fn(&RelayObservation) -> bool,
    ) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self
                .events_since(cursor)
                .iter()
                .any(|event| wanted(&event.observation))
            {
                return Ok(());
            }
            if self
                .running
                .as_ref()
                .is_none_or(|running| running.acp.is_finished())
            {
                bail!("the reviewer runtime stopped");
            }
            if tokio::time::Instant::now() >= deadline {
                bail!("timed out waiting for the reviewer");
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }
}

fn reviewer_error(message: String) -> RelayResponseBody {
    RelayResponseBody::Error {
        error: crate::hel_worker::RelayProtocolError {
            code: crate::hel_worker::RelayErrorCode::InvalidState,
            message,
            retryable: false,
            detail: None,
        },
    }
}
