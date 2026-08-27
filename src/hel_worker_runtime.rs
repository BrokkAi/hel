//! Target-side daemon and stdio proxy for the durable ACP relay protocol.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::hel_config::{ExecutionPolicy, HarnessKind};

pub use crate::hel_worker::WORKER_PID_FILE;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerOwnership {
    pub version: u32,
    pub session_id: String,
    pub profile_id: String,
    pub bundle_id: String,
    pub target_template_id: String,
}

impl WorkerOwnership {
    pub const VERSION: u32 = 1;

    pub fn write(&self, path: &Path) -> Result<()> {
        let body = serde_json::to_vec(self)?;
        crate::hel_config::atomic_write(path, &body)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcpSupervisorSpec {
    pub command: PathBuf,
    pub args: Vec<String>,
    pub environment: std::collections::BTreeMap<String, String>,
    pub cwd: PathBuf,
}

impl AcpSupervisorSpec {
    pub fn read(path: &Path) -> Result<Self> {
        let body = std::fs::read(path)
            .with_context(|| format!("read ACP supervisor spec {}", path.display()))?;
        serde_json::from_slice(&body)
            .with_context(|| format!("parse ACP supervisor spec {}", path.display()))
    }

    #[cfg(unix)]
    fn write(&self, path: &Path) -> Result<()> {
        let body = serde_json::to_vec_pretty(self)?;
        crate::hel_config::atomic_write(path, &body)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerLaunchConfig {
    pub session_id: String,
    pub harness: HarnessKind,
    pub bridge_command: PathBuf,
    pub bridge_args: Vec<String>,
    pub environment: std::collections::BTreeMap<String, String>,
    pub cwd: PathBuf,
    #[serde(default)]
    pub additional_directories: Vec<PathBuf>,
    #[serde(default)]
    pub native_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_memory: Option<ProjectMemoryLaunchConfig>,
    /// Target-level policy translated into harness-specific controls by the
    /// worker. Raw localhost and guardian SSH targets preserve configured
    /// approvals; other targets run unconstrained.
    #[serde(
        alias = "force_unrestricted_mode",
        deserialize_with = "deserialize_execution_policy"
    )]
    pub execution_policy: ExecutionPolicy,
}

fn deserialize_execution_policy<'de, D>(deserializer: D) -> Result<ExecutionPolicy, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum WirePolicy {
        Current(ExecutionPolicy),
        Legacy(bool),
    }

    Ok(match WirePolicy::deserialize(deserializer)? {
        WirePolicy::Current(policy) => policy,
        WirePolicy::Legacy(true) => ExecutionPolicy::Unconstrained,
        WirePolicy::Legacy(false) => ExecutionPolicy::ConfiguredApprovals,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectMemoryLaunchConfig {
    /// Stable controller-derived identity for this repository or bundle.
    pub project_key: String,
    /// Target-side replica used by native Claude and the MCP server.
    pub root: PathBuf,
    /// Session-private copy of the canonical tree from the last successful
    /// synchronization, used as the three-way merge base.
    #[serde(default)]
    pub baseline_root: PathBuf,
    /// Bundle repository IDs mapped to the roots presented over ACP.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub repository_roots: std::collections::BTreeMap<String, PathBuf>,
    /// How the harness learns about the project-memory MCP server. Most ACP
    /// adapters accept a stdio server in `session/new`; adapters that need
    /// harness-specific runtime metadata receive it through their staged
    /// profile instead.
    #[serde(default, skip_serializing_if = "ProjectMemoryMcpDelivery::is_acp")]
    pub mcp_delivery: ProjectMemoryMcpDelivery,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectMemoryMcpDelivery {
    #[default]
    Acp,
    HarnessProfile,
}

impl ProjectMemoryMcpDelivery {
    fn is_acp(&self) -> bool {
        *self == Self::Acp
    }
}

impl WorkerLaunchConfig {
    #[cfg(unix)]
    fn enforce_execution_policy(&mut self) {
        self.harness
            .configure_execution_environment(self.execution_policy, &mut self.environment);
    }

    pub fn read(path: &Path) -> Result<Self> {
        let body = std::fs::read(path)
            .with_context(|| format!("read worker launch config {}", path.display()))?;
        serde_json::from_slice(&body)
            .with_context(|| format!("parse worker launch config {}", path.display()))
    }

    pub fn write(&self, path: &Path) -> Result<()> {
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent)?;
        let body = serde_json::to_vec_pretty(self)?;
        std::fs::write(path, body)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        }
        Ok(())
    }
}

#[cfg(unix)]
mod unix {
    use std::collections::{BTreeMap, BTreeSet};
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use agent_client_protocol::schema::v1::SessionUpdate;
    use anyhow::{Context, Result, bail};
    use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{UnixListener, UnixStream};
    use tokio::sync::mpsc;

    use super::{AcpSupervisorSpec, CredentialEndpoint, WorkerLaunchConfig};

    pub(super) const PROXY_INITIAL_INPUT_TIMEOUT: std::time::Duration =
        std::time::Duration::from_secs(30);
    use crate::hel_acp::{self, CommandRequest, LaunchSpec, RuntimeEvent};
    use crate::hel_config::HarnessKind;
    use crate::hel_worker::{
        ClaimedRelayCommand, DeferredRelayAttach, DurableRelay, RelayCommand, RelayCommandOutcome,
        RelayErrorCode, RelayObservation, RelayProtocolError, RelayRequest, RelayRequestEnvelope,
        RelayResponseBody, RelayResponseEnvelope, RelayResponsePayload,
        incompatible_request_protocol_response, invalid_relay_request_response,
        unsupported_relay_method_response,
    };
    use crate::hel_worker_protocol::{DecodedRelayRequest, decode_relay_request};

    pub(super) const ACP_EVENT_CHANNEL_CAPACITY: usize = 256;

    #[derive(Clone)]
    pub(super) struct ProjectMemoryEndpoint {
        config: Option<super::ProjectMemoryLaunchConfig>,
        io: Arc<tokio::sync::Semaphore>,
    }

    impl ProjectMemoryEndpoint {
        fn new(config: Option<super::ProjectMemoryLaunchConfig>) -> Self {
            Self {
                config,
                io: Arc::new(tokio::sync::Semaphore::new(1)),
            }
        }
    }

    impl Default for ProjectMemoryEndpoint {
        fn default() -> Self {
            Self::new(None)
        }
    }

    struct SocketGuard(PathBuf);

    impl Drop for SocketGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    use super::WORKER_PID_FILE;

    /// Record this daemon's PID where session teardown can find it. Teardown
    /// must stop the daemon before deleting the worker root; without this file
    /// it can only guess from process command lines.
    pub(super) fn write_worker_pidfile(root: &std::path::Path, pid: u32) -> Result<()> {
        let path = root.join(WORKER_PID_FILE);
        std::fs::write(&path, format!("{pid}\n"))
            .with_context(|| format!("write worker pidfile {}", path.display()))
    }

    pub async fn run_daemon(root: PathBuf, mut config: WorkerLaunchConfig) -> Result<()> {
        let startup_directory = std::env::current_dir()?;
        let root = super::resolve_relative_worker_root(root, &startup_directory);
        super::resolve_relative_harness_home(&mut config, &startup_directory);
        config.enforce_execution_policy();
        // Resolve this before the launch config's environment is consumed by
        // the ACP supervisor specification below.
        let credentials = super::credential_endpoint(&config);
        std::fs::create_dir_all(&root)
            .with_context(|| format!("create worker root {}", root.display()))?;
        configure_github_cli(&root, &mut config.environment)?;
        let socket = root.join("control.sock");
        // Refuse a second daemon before touching durable state: opening the
        // relay recovers the journal in place, so getting that far would
        // corrupt the files a live worker is still writing.
        if socket.exists() && UnixStream::connect(&socket).await.is_ok() {
            bail!("a worker is already running at {}", socket.display());
        }
        // A dead daemon can leave its socket inode behind. Remove it before
        // journal recovery so controller liveness can distinguish a worker
        // that is still starting from one that has published its endpoint.
        if socket.exists() {
            std::fs::remove_file(&socket)
                .with_context(|| format!("remove stale socket {}", socket.display()))?;
        }
        // Validate and recover durable state before publishing a socket. A
        // failed startup must never leave a fresh endpoint that looks live.
        let mut durable_relay =
            DurableRelay::open(&root, &config.session_id, env!("CARGO_PKG_VERSION"))?;
        let resume_session = select_resume_session(&config, &durable_relay);
        let project_memory = ProjectMemoryEndpoint::new(config.project_memory.clone());
        if resume_session.is_none()
            && config.harness != HarnessKind::Claude
            && let Some(memory) = &config.project_memory
        {
            let store = crate::hel_project_memory::ProjectMemoryStore::new(&memory.root);
            durable_relay.install_prompt_context(
                crate::hel_project_memory::startup_prompt_context(
                    &store,
                    &memory.repository_roots,
                )?,
            )?;
        }
        // Startup succeeded far enough to own this root, so claim it. A failed
        // open leaves any previous pidfile alone rather than pointing teardown
        // at a process that never took over.
        write_worker_pidfile(&root, std::process::id())?;
        // Durable state recovered, so any exit record belongs to a previous
        // life of this worker. Leaving it would make the controller read this
        // startup as another death.
        let exit_record = root.join("worker-exit.json");
        if exit_record.exists() {
            std::fs::remove_file(&exit_record)
                .with_context(|| format!("clear stale exit record {}", exit_record.display()))?;
        }
        let listener = UnixListener::bind(&socket)
            .with_context(|| format!("bind worker socket {}", socket.display()))?;
        let _socket_guard = SocketGuard(socket.clone());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
        }

        let relay = Arc::new(Mutex::new(durable_relay));
        // Client tasks are detached, so a durable failure they cannot recover
        // from has to travel back here to stop the daemon.
        let (fatal_tx, mut fatal_rx) = mpsc::channel(1);
        if relay
            .lock()
            .expect("relay state lock poisoned")
            .operational_state()
            .execution
            == crate::hel_worker::RelayExecutionState::Closed
        {
            // A durable close is a seal, including across target or daemon
            // restarts. Keep the relay attachable so the controller can catch
            // up and complete its checkpoint, but never reopen the ACP session.
            let (dispatch_wake_tx, dispatch_wake_rx) = mpsc::channel(1);
            drop(dispatch_wake_rx);
            return serve_terminal_relay(
                listener,
                relay,
                dispatch_wake_tx,
                credentials,
                project_memory,
                fatal_tx,
                fatal_rx,
            )
            .await;
        }

        let (acp_commands_tx, acp_commands_rx) = mpsc::channel(32);
        let (acp_events_tx, acp_events_rx) = mpsc::channel(ACP_EVENT_CHANNEL_CAPACITY);
        let (dispatch_wake_tx, dispatch_wake_rx) = mpsc::channel(1);
        let user_shells = crate::hel_user_shell::UserShellRegistry::new(
            config.cwd.clone(),
            config.environment.clone(),
            acp_events_tx.clone(),
        );
        let supervisor_path = root.join("acp-supervisor.json");
        AcpSupervisorSpec {
            command: config.bridge_command,
            args: config.bridge_args,
            environment: config.environment,
            cwd: config.cwd.clone(),
        }
        .write(&supervisor_path)?;
        let acp_spec = LaunchSpec {
            command: std::env::current_exe().context("locate Hel worker executable")?,
            args: vec![
                "worker".into(),
                "acp-supervisor".into(),
                "--spec".into(),
                supervisor_path.to_string_lossy().into_owned(),
            ],
            environment: Default::default(),
            cwd: config.cwd,
            additional_directories: config.additional_directories,
            project_memory: config.project_memory,
            resume_session,
            harness: config.harness,
            execution_policy: config.execution_policy,
            acp_activity: relay
                .lock()
                .expect("relay lock poisoned")
                .acp_activity_clock(),
        };
        let mut acp_task = tokio::spawn(hel_acp::run(acp_spec, acp_commands_rx, acp_events_tx));

        let event_relay = relay.clone();
        let mut event_task = tokio::spawn(run_relay_coordinator_with_shells(
            event_relay,
            acp_events_rx,
            dispatch_wake_rx,
            acp_commands_tx.clone(),
            user_shells,
        ));

        let acp_join = loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (stream, _) = accepted.context("accept worker proxy")?;
                    let client_relay = relay.clone();
                    let client_dispatch_wake = dispatch_wake_tx.clone();
                    let client_credentials = credentials.clone();
                    let client_fatal = fatal_tx.clone();
                    let client_commands = acp_commands_tx.clone();
                    let client_project_memory = project_memory.clone();
                    tokio::spawn(async move {
                        if let Err(error) = serve_client_with_memory(
                            stream,
                            client_relay,
                            client_dispatch_wake,
                            client_credentials,
                            client_project_memory,
                            Some(client_commands),
                            client_fatal,
                        ).await {
                            tracing::warn!(%error, "relay proxy client disconnected");
                        }
                    });
                }
                fatal = fatal_rx.recv() => {
                    let error = fatal
                        .unwrap_or_else(|| anyhow::anyhow!("relay failure report was lost"));
                    event_task.abort();
                    drop(acp_commands_tx);
                    return abort_peer_and_return(
                        &mut acp_task,
                        error,
                        "relay durable state became unwritable",
                    ).await;
                }
                result = &mut event_task => {
                    match result {
                        Ok(Ok(())) => break acp_task.await,
                        Ok(Err(error)) => {
                            drop(acp_commands_tx);
                            return abort_peer_and_return(
                                &mut acp_task,
                                error,
                                "relay coordinator failed",
                            ).await;
                        }
                        Err(error) => {
                            drop(acp_commands_tx);
                            return abort_peer_and_return(
                                &mut acp_task,
                                anyhow::anyhow!(error),
                                "relay coordinator task stopped",
                            ).await;
                        }
                    }
                }
                result = &mut acp_task => {
                    event_task.await.context("relay event task stopped")??;
                    break result;
                }
            }
        };
        let acp_result = match acp_join {
            Ok(result) => result,
            Err(error) => {
                let error = anyhow::anyhow!("ACP runtime task stopped: {error}");
                relay
                    .lock()
                    .expect("relay state lock poisoned")
                    .record_observation(RelayObservation::Warning {
                        message: format!("{error:#}"),
                    })?;
                Err(error)
            }
        };
        let closed = relay
            .lock()
            .expect("relay state lock poisoned")
            .operational_state()
            .execution
            == crate::hel_worker::RelayExecutionState::Closed;
        if !closed {
            return acp_result;
        }
        if let Err(error) = &acp_result {
            tracing::warn!(%error, "ACP runtime failed after the relay closed");
        }
        serve_terminal_relay(
            listener,
            relay,
            dispatch_wake_tx,
            credentials,
            project_memory,
            fatal_tx,
            fatal_rx,
        )
        .await
    }

    pub(super) async fn abort_peer_and_return<T>(
        peer: &mut tokio::task::JoinHandle<T>,
        error: anyhow::Error,
        context: &'static str,
    ) -> Result<()> {
        peer.abort();
        let _ = peer.await;
        Err(error.context(context))
    }

    pub(super) async fn serve_terminal_relay(
        listener: UnixListener,
        relay: Arc<Mutex<DurableRelay>>,
        dispatch_wake: mpsc::Sender<()>,
        credentials: std::result::Result<CredentialEndpoint, String>,
        project_memory: ProjectMemoryEndpoint,
        fatal: mpsc::Sender<anyhow::Error>,
        mut fatal_reports: mpsc::Receiver<anyhow::Error>,
    ) -> Result<()> {
        loop {
            let stream = tokio::select! {
                accepted = listener.accept() => {
                    accepted.context("accept closed relay proxy")?.0
                }
                report = fatal_reports.recv() => {
                    return Err(report
                        .unwrap_or_else(|| anyhow::anyhow!("relay failure report was lost"))
                        .context("relay durable state became unwritable"));
                }
            };
            let client_relay = relay.clone();
            let client_dispatch_wake = dispatch_wake.clone();
            let client_credentials = credentials.clone();
            let client_fatal = fatal.clone();
            let client_project_memory = project_memory.clone();
            tokio::spawn(async move {
                // A sealed session has no ACP runtime left, so compaction
                // cannot be served here.
                if let Err(error) = serve_client_with_memory(
                    stream,
                    client_relay,
                    client_dispatch_wake,
                    client_credentials,
                    client_project_memory,
                    None,
                    client_fatal,
                )
                .await
                {
                    tracing::warn!(%error, "closed relay proxy client disconnected");
                }
            });
        }
    }

    async fn run_relay_coordinator_with_shells(
        relay: Arc<Mutex<DurableRelay>>,
        mut events: mpsc::Receiver<RuntimeEvent>,
        mut dispatch_wakes: mpsc::Receiver<()>,
        commands: mpsc::Sender<CommandRequest>,
        mut user_shells: crate::hel_user_shell::UserShellRegistry,
    ) -> Result<()> {
        let mut in_flight = BTreeMap::new();
        let mut session_configured = false;
        dispatch_pending(
            &relay,
            &commands,
            &mut in_flight,
            session_configured,
            &mut user_shells,
        )?;
        let mut wakes_open = true;
        loop {
            tokio::select! {
                biased;
                wake = dispatch_wakes.recv(), if wakes_open => {
                    if wake.is_none() {
                        wakes_open = false;
                    } else {
                        // A wake only says durable work may now be available.
                        // Runtime events already in the channel always belong
                        // before any newly admitted checkpoint cut.
                        let queued = events.len();
                        if record_queued_runtime_events(
                            &relay,
                            &mut in_flight,
                            &mut events,
                            &mut session_configured,
                            &mut user_shells,
                            queued,
                        ).await? {
                            return Ok(());
                        }
                        dispatch_pending(
                            &relay,
                            &commands,
                            &mut in_flight,
                            session_configured,
                            &mut user_shells,
                        )?;
                    }
                }
                event = events.recv() => {
                    let Some(event) = event else {
                        interrupt_in_flight(
                            &relay,
                            &mut in_flight,
                            "ACP runtime stopped before the command completed",
                        )?;
                        return Ok(());
                    };
                    if record_runtime_event_batch(
                        &relay,
                        &mut in_flight,
                        event,
                        &mut events,
                        &mut session_configured,
                        &mut user_shells,
                    ).await? {
                        return Ok(());
                    }
                    dispatch_pending(
                        &relay,
                        &commands,
                        &mut in_flight,
                        session_configured,
                        &mut user_shells,
                    )?;
                }
            }
        }
    }

    #[cfg(test)]
    pub(super) async fn run_relay_coordinator(
        relay: Arc<Mutex<DurableRelay>>,
        events: mpsc::Receiver<RuntimeEvent>,
        dispatch_wakes: mpsc::Receiver<()>,
        commands: mpsc::Sender<CommandRequest>,
    ) -> Result<()> {
        let (shell_events, _shell_events_rx) = mpsc::channel(1);
        let user_shells = crate::hel_user_shell::UserShellRegistry::new(
            std::env::current_dir()?,
            BTreeMap::new(),
            shell_events,
        );
        run_relay_coordinator_with_shells(relay, events, dispatch_wakes, commands, user_shells)
            .await
    }

    /// Record the complete batch already emitted by the ACP runtime before
    /// admitting a checkpoint barrier. A command event may itself materialize
    /// several durable observations, and queued notification events belong to
    /// the cut ahead of any waiting barrier.
    async fn record_runtime_event_batch(
        relay: &Arc<Mutex<DurableRelay>>,
        in_flight: &mut BTreeMap<String, RelayCommand>,
        first: RuntimeEvent,
        events: &mut mpsc::Receiver<RuntimeEvent>,
        session_configured: &mut bool,
        user_shells: &mut crate::hel_user_shell::UserShellRegistry,
    ) -> Result<bool> {
        track_user_shell_completion(user_shells, &first);
        if record_runtime_event_and_track_configuration(
            relay,
            in_flight,
            first,
            session_configured,
        )? {
            return Ok(true);
        }
        let queued = events.len();
        record_queued_runtime_events(
            relay,
            in_flight,
            events,
            session_configured,
            user_shells,
            queued,
        )
        .await
    }

    async fn record_queued_runtime_events(
        relay: &Arc<Mutex<DurableRelay>>,
        in_flight: &mut BTreeMap<String, RelayCommand>,
        events: &mut mpsc::Receiver<RuntimeEvent>,
        session_configured: &mut bool,
        user_shells: &mut crate::hel_user_shell::UserShellRegistry,
        maximum: usize,
    ) -> Result<bool> {
        for recorded in 0..maximum {
            match events.try_recv() {
                Ok(event) => {
                    track_user_shell_completion(user_shells, &event);
                    if record_runtime_event_and_track_configuration(
                        relay,
                        in_flight,
                        event,
                        session_configured,
                    )? {
                        return Ok(true);
                    }
                    if (recorded + 1) % 256 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
                Err(mpsc::error::TryRecvError::Empty) => return Ok(false),
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    interrupt_in_flight(
                        relay,
                        in_flight,
                        "ACP runtime stopped before the command completed",
                    )?;
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    fn track_user_shell_completion(
        user_shells: &mut crate::hel_user_shell::UserShellRegistry,
        event: &RuntimeEvent,
    ) {
        if let RuntimeEvent::UserShellFinished { request_id, .. } = event {
            user_shells.completed(request_id);
        }
    }

    fn record_runtime_event_and_track_configuration(
        relay: &Arc<Mutex<DurableRelay>>,
        in_flight: &mut BTreeMap<String, RelayCommand>,
        event: RuntimeEvent,
        session_configured: &mut bool,
    ) -> Result<bool> {
        *session_configured |= matches!(event, RuntimeEvent::SessionConfigured { .. });
        record_runtime_event(relay, in_flight, event)
    }

    pub(super) fn record_runtime_event(
        relay: &Arc<Mutex<DurableRelay>>,
        in_flight: &mut BTreeMap<String, RelayCommand>,
        event: RuntimeEvent,
    ) -> Result<bool> {
        let stopped = matches!(event, RuntimeEvent::Stopped);
        let mut relay = relay.lock().expect("relay state lock poisoned");
        match event {
            RuntimeEvent::Connected {
                protocol_version: Some(protocol_version),
                capabilities: Some(capabilities),
                agent_info,
                ..
            } => {
                relay.record_observation(RelayObservation::AgentInitialized {
                    protocol_version,
                    capabilities,
                    agent_info,
                })?;
            }
            RuntimeEvent::Connected { .. } => {
                relay.record_observation(RelayObservation::Warning {
                    message: "ACP initialized without capability metadata".into(),
                })?;
            }
            RuntimeEvent::SessionStarted {
                native_session_id,
                resumed,
                ..
            } => {
                relay.record_observation(RelayObservation::SessionOpened {
                    native_session_id,
                    resumed,
                })?;
            }
            RuntimeEvent::SessionConfigured { config_options } => {
                relay.record_observation(RelayObservation::SessionConfigured { config_options })?;
            }
            RuntimeEvent::SessionModesConfigured { modes } => {
                relay.record_observation(RelayObservation::SessionModesConfigured { modes })?;
            }
            RuntimeEvent::SessionUpdate { update } => {
                let typed = serde_json::from_value::<SessionUpdate>(update).map_err(|error| {
                    anyhow::anyhow!("decode ACP session update for relay journal: {error}")
                });
                match typed {
                    Ok(update) => {
                        relay.record_session_update(update)?;
                    }
                    Err(error) => {
                        relay.record_observation(RelayObservation::Warning {
                            message: format!("{error:#}"),
                        })?;
                        return Err(error);
                    }
                }
            }
            RuntimeEvent::ElicitationRequested { request } => {
                relay.record_observation(RelayObservation::ElicitationRequested { request })?;
            }
            RuntimeEvent::ElicitationResolved {
                elicitation_id,
                action,
            } => {
                relay.record_observation(RelayObservation::ElicitationResolved {
                    elicitation_id,
                    action,
                })?;
            }
            RuntimeEvent::PromptFinished {
                request_id,
                stop_reason,
            } => {
                in_flight.remove(&request_id);
                relay.record_command_completed(
                    &request_id,
                    RelayCommandOutcome::Prompt { stop_reason },
                )?;
            }
            RuntimeEvent::ConfigApplied {
                request_id,
                key,
                value,
                config_options,
            } => {
                relay.record_observation(RelayObservation::ConfigurationUpdated { key, value })?;
                relay.record_observation(RelayObservation::SessionConfigured { config_options })?;
                relay.record_command_completed(&request_id, RelayCommandOutcome::Configured)?;
                in_flight.remove(&request_id);
            }
            RuntimeEvent::SessionModeApplied {
                request_id,
                mode_id,
                config_options,
                modes,
            } => {
                relay.record_observation(RelayObservation::ConfigurationUpdated {
                    key: "mode".to_owned(),
                    value: mode_id,
                })?;
                relay.record_observation(RelayObservation::SessionConfigured { config_options })?;
                relay.record_observation(RelayObservation::SessionModesConfigured { modes })?;
                relay.record_command_completed(&request_id, RelayCommandOutcome::SessionModeSet)?;
                in_flight.remove(&request_id);
            }
            RuntimeEvent::CommandRejected {
                request_id,
                message,
            } => {
                in_flight.remove(&request_id);
                relay.record_command_rejected(&request_id, message)?;
            }
            RuntimeEvent::CommandInterrupted {
                request_id,
                message,
            } => {
                in_flight.remove(&request_id);
                relay.record_command_interrupted(&request_id, message)?;
            }
            RuntimeEvent::CancelApplied { request_id } => {
                in_flight.remove(&request_id);
                relay.record_command_completed(&request_id, RelayCommandOutcome::Cancelled)?;
            }
            RuntimeEvent::CloseApplied { request_id } => {
                in_flight.remove(&request_id);
                relay.record_command_completed(&request_id, RelayCommandOutcome::Closed)?;
                relay.record_observation(RelayObservation::Closed)?;
            }
            RuntimeEvent::Warning { message } => {
                relay.record_observation(RelayObservation::Warning { message })?;
            }
            RuntimeEvent::HarnessRestarting { message } => {
                relay.clear_agent_terminals();
                relay.record_observation(RelayObservation::Warning {
                    message: message.clone(),
                })?;
                for (command_id, _) in std::mem::take(in_flight) {
                    relay.record_command_interrupted(&command_id, message.clone())?;
                }
            }
            RuntimeEvent::TerminalClosed {
                terminal_id,
                mut output,
                mut truncated,
                exit_code,
                signal,
            } => {
                relay.agent_terminal_closed(&terminal_id);
                // Cap here rather than letting `clamp_observation` fire: that
                // keeps the head of a string, and a terminal's tail is what
                // says how the command ended.
                truncated |= crate::hel_worker::truncate_start_with_marker(
                    &mut output,
                    crate::hel_worker::TERMINAL_JOURNAL_OUTPUT_BYTES,
                );
                relay.record_observation(RelayObservation::TerminalOutput {
                    terminal_id,
                    output,
                    truncated,
                    exit_code,
                    signal,
                })?;
            }
            RuntimeEvent::TerminalStarted {
                terminal_id,
                command,
                started_at_ms,
            } => {
                relay.agent_terminal_started(crate::hel_worker::ActiveAgentTerminal {
                    terminal_id,
                    command,
                    started_at_ms,
                });
            }
            RuntimeEvent::UserShellOutput {
                request_id,
                command,
                stdout,
                stderr,
                stdout_truncated,
                stderr_truncated,
            } => {
                relay.record_observation(RelayObservation::UserShellOutput {
                    command_id: request_id,
                    command,
                    stdout,
                    stderr,
                    stdout_truncated,
                    stderr_truncated,
                })?;
            }
            RuntimeEvent::UserShellFinished { request_id, result } => {
                relay.record_command_completed(
                    &request_id,
                    RelayCommandOutcome::UserShell { result },
                )?;
            }
            RuntimeEvent::Stopped => {
                relay.clear_agent_terminals();
                relay.record_observation(RelayObservation::ElicitationsCleared)?;
                if relay.operational_state().execution
                    != crate::hel_worker::RelayExecutionState::Closed
                {
                    relay.record_observation(RelayObservation::Warning {
                        message: "ACP runtime stopped".into(),
                    })?;
                }
                for (command_id, _) in std::mem::take(in_flight) {
                    relay.record_command_interrupted(
                        &command_id,
                        "ACP runtime stopped before the command completed",
                    )?;
                }
            }
        }
        Ok(stopped)
    }

    fn interrupt_in_flight(
        relay: &Arc<Mutex<DurableRelay>>,
        in_flight: &mut BTreeMap<String, RelayCommand>,
        message: &str,
    ) -> Result<()> {
        let mut relay = relay.lock().expect("relay state lock poisoned");
        for (command_id, _) in std::mem::take(in_flight) {
            relay.record_command_interrupted(&command_id, message)?;
        }
        Ok(())
    }

    /// Hand durable work to the ACP runtime through capacity reserved before
    /// the claim, so dispatch never waits. The command channel is shared with
    /// out-of-band senders (compaction, elicitation answers); they now compete
    /// for the same permits instead of stealing capacity a claim already
    /// counted on. That matters because a coordinator parked on a send stops
    /// draining ACP's bounded event channel, which stops the runtime that
    /// would have drained these commands: a cycle nothing breaks.
    ///
    /// This function deliberately holds no await point. A reserved permit
    /// carries no durable state, so reserve -> durable claim -> permit send
    /// keeps the claim-before-dispatch contract: a crash between the claim and
    /// the send leaves the command in flight for restart interruption, exactly
    /// as before.
    fn dispatch_pending(
        relay: &Arc<Mutex<DurableRelay>>,
        commands: &mpsc::Sender<CommandRequest>,
        in_flight: &mut BTreeMap<String, RelayCommand>,
        session_configured: bool,
        user_shells: &mut crate::hel_user_shell::UserShellRegistry,
    ) -> Result<()> {
        let mut permits = Vec::new();
        // `Full` means dispatch what fits now and reserve again on the next
        // runtime event or wake. `Closed` means the ACP runtime is gone, so
        // claim nothing: leaving the commands pending lets the next run
        // dispatch them instead of interrupting work that never started, and
        // the closing events channel is what ends this coordinator.
        while let Ok(permit) = commands.try_reserve() {
            permits.push(permit);
        }
        dispatch_user_shells(relay, user_shells)?;
        let mut pending = relay
            .lock()
            .expect("relay state lock poisoned")
            .claim_pending_commands_up_to(session_configured, permits.len())?;
        pending.sort_by_key(|claimed| claimed.accepted_ordinal);
        // Hand back capacity the claim did not need before doing anything else.
        permits.truncate(pending.len());
        let mut permits = permits.into_iter();
        for claimed in pending {
            if matches!(&claimed.command, RelayCommand::BeginCheckpoint { .. }) {
                relay
                    .lock()
                    .expect("relay state lock poisoned")
                    .record_checkpoint_ready(&claimed.command_id)?;
                continue;
            }
            let Some(command) = acp_command(&claimed) else {
                relay
                    .lock()
                    .expect("relay state lock poisoned")
                    .record_command_rejected(
                        &claimed.command_id,
                        "relay-local command was unexpectedly claimed for ACP dispatch",
                    )?;
                continue;
            };
            permits
                .next()
                .expect("every claimed command holds a reserved ACP command permit")
                .send(command);
            in_flight.insert(claimed.command_id, claimed.command);
        }
        Ok(())
    }

    fn dispatch_user_shells(
        relay: &Arc<Mutex<DurableRelay>>,
        user_shells: &mut crate::hel_user_shell::UserShellRegistry,
    ) -> Result<()> {
        let claimed = relay
            .lock()
            .expect("relay state lock poisoned")
            .claim_pending_user_shell_commands_up_to(user_shells.available_slots())?;
        for claimed in claimed {
            match claimed.command {
                RelayCommand::RunUserShell { command } => {
                    if let Err(error) =
                        user_shells.start(claimed.command_id.clone(), command.clone())
                    {
                        relay
                            .lock()
                            .expect("relay state lock poisoned")
                            .record_command_completed(
                                &claimed.command_id,
                                RelayCommandOutcome::UserShell {
                                    result: crate::hel_worker::UserShellResult {
                                        command,
                                        stdout: String::new(),
                                        stderr: String::new(),
                                        stdout_truncated: false,
                                        stderr_truncated: false,
                                        exit_code: None,
                                        signal: None,
                                        duration_ms: 0,
                                        status: crate::hel_worker::UserShellStatus::Failed,
                                        error: Some(format!("{error:#}")),
                                    },
                                },
                            )?;
                    }
                }
                RelayCommand::CancelUserShell { shell_command_id } => {
                    let cancellation = user_shells.cancel(&shell_command_id);
                    let mut relay = relay.lock().expect("relay state lock poisoned");
                    if cancellation == crate::hel_user_shell::UserShellCancelOutcome::NotRunning
                        && relay
                            .operational_state()
                            .active_user_shells
                            .iter()
                            .any(|shell| shell.command_id == shell_command_id)
                    {
                        relay.record_command_interrupted(
                            &shell_command_id,
                            "shell command was cancelled before it started",
                        )?;
                    }
                    relay.record_command_completed(
                        &claimed.command_id,
                        RelayCommandOutcome::UserShellCancelled,
                    )?;
                }
                _ => unreachable!("only user shell commands are claimed here"),
            }
        }
        Ok(())
    }

    fn acp_command(claimed: &ClaimedRelayCommand) -> Option<CommandRequest> {
        let request_id = claimed.command_id.clone();
        match &claimed.command {
            RelayCommand::Prompt { prompt } => {
                let mut prompt = prompt.clone();
                if let Some(context) = &claimed.hidden_prompt_context {
                    prompt.insert(
                        0,
                        agent_client_protocol::schema::v1::ContentBlock::Text(
                            agent_client_protocol::schema::v1::TextContent::new(context.clone()),
                        ),
                    );
                }
                Some(CommandRequest::Prompt { request_id, prompt })
            }
            RelayCommand::SetConfig { key, value } => Some(CommandRequest::SetConfig {
                request_id,
                key: key.clone(),
                value: value.clone(),
            }),
            RelayCommand::SetSessionMode { mode_id } => Some(CommandRequest::SetSessionMode {
                request_id,
                mode_id: mode_id.clone(),
            }),
            RelayCommand::Cancel => Some(CommandRequest::Cancel { request_id }),
            RelayCommand::Close { .. } => Some(CommandRequest::Close { request_id }),
            RelayCommand::BeginCheckpoint { .. }
            | RelayCommand::RunUserShell { .. }
            | RelayCommand::CancelUserShell { .. }
            | RelayCommand::RemoveQueuedPrompt { .. }
            | RelayCommand::ClearQueuedPrompts
            | RelayCommand::CompleteCheckpoint { .. }
            | RelayCommand::ReleaseCheckpoint { .. }
            | RelayCommand::AdvanceRecoveryFloor { .. }
            | RelayCommand::RecordNotice { .. } => None,
        }
    }

    pub(super) fn select_resume_session(
        config: &WorkerLaunchConfig,
        relay: &DurableRelay,
    ) -> Option<String> {
        config
            .native_session_id
            .clone()
            .or_else(|| relay.operational_state().native_session_id)
    }

    pub(super) async fn read_bounded_line(
        reader: &mut (impl AsyncBufRead + Unpin),
        maximum_bytes: usize,
    ) -> Result<Option<String>> {
        let mut line = Vec::new();
        loop {
            let available = reader.fill_buf().await.context("read relay request")?;
            if available.is_empty() {
                if line.is_empty() {
                    return Ok(None);
                }
                return String::from_utf8(line)
                    .context("relay request is not UTF-8")
                    .map(Some);
            }
            let newline = available.iter().position(|byte| *byte == b'\n');
            let content_bytes = newline.unwrap_or(available.len());
            let next_len = line
                .len()
                .checked_add(content_bytes)
                .context("relay request frame length overflow")?;
            if next_len > maximum_bytes {
                bail!("relay request frame is too large");
            }
            line.extend_from_slice(&available[..content_bytes]);
            let consumed = content_bytes + usize::from(newline.is_some());
            reader.consume(consumed);
            if newline.is_some() {
                return String::from_utf8(line)
                    .context("relay request is not UTF-8")
                    .map(Some);
            }
        }
    }

    /// A durable write can only fail permanently because the worker root is
    /// gone: session teardown removed it under this daemon. Nothing served
    /// afterwards could ever be persisted, so the daemon has to stop.
    pub(super) fn worker_root_was_removed(
        body: &RelayResponseBody,
        root: &std::path::Path,
    ) -> bool {
        matches!(
            body,
            RelayResponseBody::Error {
                error: RelayProtocolError {
                    code: RelayErrorCode::Internal,
                    ..
                }
            }
        ) && !root.is_dir()
    }

    /// Answer one relay request, keeping the relay lock for exactly as long as
    /// the request needs it.
    ///
    /// Every request but attach is a bounded mutation of durable state and is
    /// served under the lock. Attach is not: validating a controller's cursor
    /// can decompress a sealed segment and the reply carries up to a
    /// [`crate::hel_worker::RELAY_REPLAY_BYTE_BUDGET`] page read from disk, and
    /// a controller catching up over a long offline history asks for page
    /// after page. Holding the lock across that stalls the coordinator's
    /// `record_runtime_event`, and once its bounded event channel fills the
    /// agent's turn stalls with it. So attach captures a plan under the lock
    /// and does its reading on a blocking thread with the lock released.
    pub(super) async fn handle_request(
        relay: &Arc<Mutex<DurableRelay>>,
        envelope: RelayRequestEnvelope,
    ) -> Result<RelayResponseEnvelope> {
        // Sealing and garbage collection can move the segments a plan named
        // while it is being read. That is not the controller's fault and not
        // its problem: plan again against the journal as it now stands. The
        // journal only moves that way once per sealed megabyte or per
        // acknowledgement, so a couple of attempts always outrun it.
        const REPLAN_ATTEMPTS: usize = 3;

        let mut deferred = {
            let mut guard = relay.lock().expect("relay state lock poisoned");
            match guard.take_deferred_attach(&envelope) {
                Some(deferred) => deferred,
                None => return Ok(guard.handle(envelope)),
            }
        };
        for _ in 0..REPLAN_ATTEMPTS {
            let generation = deferred.journal_generation();
            let response = tokio::task::spawn_blocking(move || deferred.finish())
                .await
                .context("assemble relay replay page")?;
            if !matches!(response.body, RelayResponseBody::Error { .. }) {
                let mut guard = relay.lock().expect("relay state lock poisoned");
                if guard.journal_generation() == generation {
                    guard.remember_replay_cursor(&response);
                }
                return Ok(response);
            }
            let replanned = {
                let guard = relay.lock().expect("relay state lock poisoned");
                if guard.journal_generation() == generation {
                    // The journal never moved, so this really is the answer.
                    return Ok(response);
                }
                guard.take_deferred_attach(&envelope)
            };
            match replanned {
                Some(next) => deferred = next,
                None => break,
            }
        }
        Ok(DeferredRelayAttach::stale_journal_response(
            envelope.request_id,
            envelope.protocol_version,
        ))
    }

    /// `commands` is the ACP coordinator's command channel, or `None` once the
    /// session is sealed and no ACP runtime is left to serve scratch prompts.
    #[cfg(test)]
    pub(super) async fn serve_client(
        stream: UnixStream,
        relay: Arc<Mutex<DurableRelay>>,
        dispatch_wake: mpsc::Sender<()>,
        credentials: std::result::Result<CredentialEndpoint, String>,
        commands: Option<mpsc::Sender<CommandRequest>>,
        fatal: mpsc::Sender<anyhow::Error>,
    ) -> Result<()> {
        serve_client_with_memory(
            stream,
            relay,
            dispatch_wake,
            credentials,
            ProjectMemoryEndpoint::default(),
            commands,
            fatal,
        )
        .await
    }

    pub(super) async fn serve_client_with_memory(
        stream: UnixStream,
        relay: Arc<Mutex<DurableRelay>>,
        dispatch_wake: mpsc::Sender<()>,
        credentials: std::result::Result<CredentialEndpoint, String>,
        project_memory: ProjectMemoryEndpoint,
        commands: Option<mpsc::Sender<CommandRequest>>,
        fatal: mpsc::Sender<anyhow::Error>,
    ) -> Result<()> {
        let relay_root = relay
            .lock()
            .expect("relay state lock poisoned")
            .root()
            .to_path_buf();
        let session_id = relay
            .lock()
            .expect("relay state lock poisoned")
            .operational_state()
            .session_id;
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut checkpoint_barriers = BTreeSet::new();
        let serving_result = async {
            while let Some(line) =
                read_bounded_line(&mut reader, crate::hel_worker::MAX_FRAME_BYTES).await?
            {
                // One decoder owns this boundary: an unknown method is a
                // protocol error the controller can act on, and an unreadable
                // frame is answered rather than dropping the connection.
                let envelope = match decode_relay_request(line.as_bytes()) {
                    DecodedRelayRequest::Known(envelope) => envelope,
                    DecodedRelayRequest::Unknown {
                        request_id,
                        protocol_version,
                        method,
                    } => {
                        let response =
                            unsupported_relay_method_response(request_id, protocol_version, method);
                        write_logged_response(&mut writer, &response, &session_id, "unknown")
                            .await?;
                        continue;
                    }
                    DecodedRelayRequest::Invalid {
                        request_id,
                        protocol_version,
                        message,
                    } => {
                        let response =
                            invalid_relay_request_response(request_id, protocol_version, message);
                        write_logged_response(&mut writer, &response, &session_id, "invalid")
                            .await?;
                        continue;
                    }
                };
                if matches!(
                    &envelope.request,
                    RelayRequest::CredentialState
                        | RelayRequest::ReadCredentials
                        | RelayRequest::InstallCredentials { .. }
                        | RelayRequest::SkillsState
                        | RelayRequest::InstallSkills { .. }
                        | RelayRequest::GithubTokenState
                        | RelayRequest::InstallGithubToken { .. }
                        | RelayRequest::RemoveGithubToken
                ) {
                    // Credential, token, and skills bytes stay on this connection.
                    // They never reach DurableRelay, its journal, or its
                    // command ledger.
                    let operation = envelope.request.method_name();
                    let response = credential_response(envelope, &credentials, &relay_root).await;
                    write_logged_response(&mut writer, &response, &session_id, operation).await?;
                    continue;
                }
                if matches!(
                    &envelope.request,
                    RelayRequest::ProjectMemorySnapshot
                        | RelayRequest::InstallProjectMemorySnapshot { .. }
                ) {
                    let operation = envelope.request.method_name();
                    let response = project_memory_response(envelope, &project_memory).await;
                    write_logged_response(&mut writer, &response, &session_id, operation).await?;
                    continue;
                }
                if let RelayRequest::Compact { .. } = &envelope.request {
                    // A scratch compaction prompt is not session history, so
                    // it never reaches DurableRelay, its journal, or its
                    // command ledger. Awaiting the model turn stalls only this
                    // connection; the controller drives it as a single
                    // sequential RPC.
                    let response = compaction_response(envelope, commands.as_ref()).await;
                    write_logged_response(&mut writer, &response, &session_id, "compact").await?;
                    continue;
                }
                if let RelayRequest::RespondElicitation { .. } = &envelope.request {
                    // Form answers can contain private user input. They travel
                    // directly to the ACP runtime and never touch relay state.
                    let response = elicitation_response(envelope, commands.as_ref()).await;
                    write_logged_response(
                        &mut writer,
                        &response,
                        &session_id,
                        "respond_elicitation",
                    )
                    .await?;
                    continue;
                }
                let wakes_dispatch = matches!(&envelope.request, RelayRequest::Submit { .. });
                let checkpoint_change = checkpoint_change(&envelope.request);
                let operation = envelope.request.method_name();
                let response = match handle_request(&relay, envelope).await {
                    Ok(response) => response,
                    Err(error) => {
                        tracing::error!(
                            %session_id,
                            %operation,
                            "relay request handling failed: {error:#}"
                        );
                        return Err(error);
                    }
                };
                if worker_root_was_removed(&response.body, &relay_root) {
                    // One report is enough; the daemon is already winding down.
                    report_fatal(
                        &fatal,
                        anyhow::anyhow!(
                            "worker root {} was removed while the relay was serving",
                            relay_root.display()
                        ),
                        &session_id,
                        "worker root removed",
                    );
                }
                let accepted = matches!(
                    &response.body,
                    RelayResponseBody::Ok {
                        payload: RelayResponsePayload::Accepted { .. }
                    }
                );
                if accepted {
                    match checkpoint_change {
                        Some(CheckpointChange::Begin(command_id)) => {
                            checkpoint_barriers.insert(command_id);
                        }
                        Some(CheckpointChange::Ended(command_id)) => {
                            checkpoint_barriers.remove(&command_id);
                        }
                        None => {}
                    }
                }
                if wakes_dispatch && accepted {
                    wake_dispatch(&relay, &dispatch_wake)?;
                }
                // Dispatch is driven from durable state, not from delivery of
                // the acknowledgement. A controller can disappear after its
                // request reaches the relay but before the response write.
                write_logged_response(&mut writer, &response, &session_id, operation).await?;
            }
            Ok(())
        }
        .await;

        let cleanup_result =
            release_checkpoint_barriers(&relay, &dispatch_wake, checkpoint_barriers);
        let outcome = match (serving_result, cleanup_result) {
            (Ok(()), cleanup_result) => cleanup_result,
            (Err(error), Ok(())) => Err(error),
            (Err(error), Err(cleanup_error)) => Err(error.context(format!(
                "also failed to release checkpoint barriers: {cleanup_error:#}"
            ))),
        };
        if outcome.is_err() && !relay_root.is_dir() {
            report_fatal(
                &fatal,
                anyhow::anyhow!(
                    "worker root {} was removed while the relay was serving",
                    relay_root.display()
                ),
                &session_id,
                "worker root removed",
            );
        }
        outcome
    }

    /// Run a compaction prompt in a disposable ACP session on the connection.
    /// A compaction failure is never retryable: the transcript that produced
    /// it does not change between attempts.
    async fn compaction_response(
        envelope: RelayRequestEnvelope,
        commands: Option<&mpsc::Sender<CommandRequest>>,
    ) -> RelayResponseEnvelope {
        if !envelope.request.supported_at(envelope.protocol_version) {
            return incompatible_request_protocol_response(
                envelope.request_id,
                envelope.protocol_version,
            );
        }
        let RelayRequest::Compact { prompt } = envelope.request else {
            unreachable!("compaction_response only serves compact requests");
        };
        let body = if prompt.trim().is_empty() {
            compaction_error(RelayErrorCode::InvalidRequest, "compaction prompt is empty")
        } else {
            match commands {
                None => compaction_error(
                    RelayErrorCode::InvalidState,
                    "session is closed; no ACP runtime can compact",
                ),
                Some(commands) => {
                    let (response_tx, response_rx) = tokio::sync::oneshot::channel();
                    match commands
                        .send(CommandRequest::Compact {
                            prompt,
                            response: response_tx,
                        })
                        .await
                    {
                        Ok(()) => match response_rx.await {
                            Ok(Ok(text)) => RelayResponseBody::Ok {
                                payload: RelayResponsePayload::Compacted { text },
                            },
                            Ok(Err(message)) => {
                                compaction_error(RelayErrorCode::InvalidState, &message)
                            }
                            Err(_) => compaction_error(
                                RelayErrorCode::Internal,
                                "ACP runtime stopped before it compacted",
                            ),
                        },
                        Err(_) => compaction_error(
                            RelayErrorCode::Internal,
                            "ACP runtime stopped before accepting the compaction prompt",
                        ),
                    }
                }
            }
        };
        RelayResponseEnvelope {
            request_id: envelope.request_id,
            protocol_version: envelope.protocol_version,
            body,
        }
    }

    fn compaction_error(code: RelayErrorCode, message: &str) -> RelayResponseBody {
        RelayResponseBody::Error {
            error: RelayProtocolError {
                code,
                message: message.to_owned(),
                retryable: false,
                detail: None,
            },
        }
    }

    async fn elicitation_response(
        envelope: RelayRequestEnvelope,
        commands: Option<&mpsc::Sender<CommandRequest>>,
    ) -> RelayResponseEnvelope {
        if !envelope.request.supported_at(envelope.protocol_version) {
            return incompatible_request_protocol_response(
                envelope.request_id,
                envelope.protocol_version,
            );
        }
        let protocol_version = envelope.protocol_version;
        let request_id = envelope.request_id;
        let RelayRequest::RespondElicitation {
            elicitation_id,
            response,
        } = envelope.request
        else {
            unreachable!("elicitation_response only serves form answers")
        };
        let body = match commands {
            None => compaction_error(
                RelayErrorCode::InvalidState,
                "session is closed; no ACP runtime can answer the elicitation",
            ),
            Some(commands) => {
                let (resolved, resolution) = tokio::sync::oneshot::channel();
                match commands
                    .send(CommandRequest::ResolveElicitation {
                        elicitation_id: elicitation_id.clone(),
                        response,
                        resolved,
                    })
                    .await
                {
                    Ok(()) => match resolution.await {
                        Ok(Ok(())) => RelayResponseBody::Ok {
                            payload: RelayResponsePayload::ElicitationResolved { elicitation_id },
                        },
                        Ok(Err(message)) => {
                            compaction_error(RelayErrorCode::InvalidState, &message)
                        }
                        Err(_) => compaction_error(
                            RelayErrorCode::Internal,
                            "ACP runtime stopped before resolving the elicitation",
                        ),
                    },
                    Err(_) => compaction_error(
                        RelayErrorCode::Internal,
                        "ACP runtime stopped before accepting the elicitation answer",
                    ),
                }
            }
        };
        RelayResponseEnvelope {
            request_id,
            protocol_version,
            body,
        }
    }

    /// Serve a credential or skills request against this relay's own harness
    /// home. File work runs on a blocking thread so neither the socket task
    /// nor the ACP coordinator is stalled by filesystem I/O.
    async fn credential_response(
        envelope: RelayRequestEnvelope,
        credentials: &std::result::Result<CredentialEndpoint, String>,
        relay_root: &std::path::Path,
    ) -> RelayResponseEnvelope {
        if !envelope.request.supported_at(envelope.protocol_version) {
            return incompatible_request_protocol_response(
                envelope.request_id,
                envelope.protocol_version,
            );
        }
        let body = match credentials {
            Err(message) => RelayResponseBody::Error {
                error: RelayProtocolError {
                    code: RelayErrorCode::InvalidState,
                    message: message.clone(),
                    retryable: false,
                    detail: None,
                },
            },
            Ok(endpoint) => {
                let endpoint = endpoint.clone();
                let github_token_path = relay_root.join("github-token");
                let request = envelope.request.clone();
                match tokio::task::spawn_blocking(move || {
                    apply_credential_request_at(&endpoint, &github_token_path, &request)
                })
                .await
                {
                    Ok(Ok(payload)) => RelayResponseBody::Ok { payload },
                    Ok(Err(error)) => RelayResponseBody::Error {
                        error: RelayProtocolError {
                            code: RelayErrorCode::InvalidRequest,
                            message: format!("{error:#}"),
                            retryable: false,
                            detail: None,
                        },
                    },
                    Err(error) => RelayResponseBody::Error {
                        error: RelayProtocolError {
                            code: RelayErrorCode::Internal,
                            message: format!("credential task stopped: {error}"),
                            retryable: true,
                            detail: None,
                        },
                    },
                }
            }
        };
        RelayResponseEnvelope {
            request_id: envelope.request_id,
            protocol_version: envelope.protocol_version,
            body,
        }
    }

    /// Snapshot and install project memory off the socket task. These payloads
    /// are connection-only and are never journaled as conversation history.
    pub(super) async fn run_serialized_project_memory_io<T, F>(
        project_memory_io: &Arc<tokio::sync::Semaphore>,
        operation: F,
    ) -> std::result::Result<Result<T>, tokio::task::JoinError>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T> + Send + 'static,
    {
        // A timed-out client can abandon this future, but Tokio cannot cancel
        // blocking filesystem work after it starts. Keep the permit inside
        // that work so reconnects wait instead of piling more reads and fsyncs
        // onto the degraded storage device.
        let permit = project_memory_io
            .clone()
            .acquire_owned()
            .await
            .expect("project memory I/O semaphore is never closed");
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            operation()
        })
        .await
    }

    async fn project_memory_response(
        envelope: RelayRequestEnvelope,
        endpoint: &ProjectMemoryEndpoint,
    ) -> RelayResponseEnvelope {
        if !envelope.request.supported_at(envelope.protocol_version) {
            return incompatible_request_protocol_response(
                envelope.request_id,
                envelope.protocol_version,
            );
        }
        let body = match endpoint.config.as_ref() {
            None => RelayResponseBody::Error {
                error: RelayProtocolError {
                    code: RelayErrorCode::InvalidState,
                    message: "this session has no project memory endpoint".into(),
                    retryable: false,
                    detail: None,
                },
            },
            Some(memory) => {
                let memory = memory.clone();
                let request = envelope.request.clone();
                match run_serialized_project_memory_io(&endpoint.io, move || {
                    apply_project_memory_request(&memory, &request)
                })
                .await
                {
                    Ok(Ok(payload)) => RelayResponseBody::Ok { payload },
                    Ok(Err(error)) => RelayResponseBody::Error {
                        error: RelayProtocolError {
                            code: RelayErrorCode::InvalidRequest,
                            message: format!("{error:#}"),
                            retryable: false,
                            detail: None,
                        },
                    },
                    Err(error) => RelayResponseBody::Error {
                        error: RelayProtocolError {
                            code: RelayErrorCode::Internal,
                            message: format!("project memory task stopped: {error}"),
                            retryable: true,
                            detail: None,
                        },
                    },
                }
            }
        };
        RelayResponseEnvelope {
            request_id: envelope.request_id,
            protocol_version: envelope.protocol_version,
            body,
        }
    }

    pub(super) fn apply_project_memory_request(
        memory: &super::ProjectMemoryLaunchConfig,
        request: &RelayRequest,
    ) -> Result<RelayResponsePayload> {
        let replica = crate::hel_project_memory::ProjectMemoryStore::new(&memory.root);
        let baseline = crate::hel_project_memory::ProjectMemoryStore::new(&memory.baseline_root);
        match request {
            RelayRequest::ProjectMemorySnapshot => {
                Ok(RelayResponsePayload::ProjectMemorySnapshot {
                    baseline: baseline.snapshot()?,
                    replica: replica.snapshot()?,
                })
            }
            RelayRequest::InstallProjectMemorySnapshot { snapshot } => {
                replica.install_snapshot(snapshot)?;
                baseline.install_snapshot(snapshot)?;
                Ok(RelayResponsePayload::ProjectMemorySnapshotInstalled)
            }
            other => bail!("{} is not a project memory request", other.method_name()),
        }
    }

    #[cfg(test)]
    pub(super) fn apply_credential_request(
        endpoint: &CredentialEndpoint,
        request: &RelayRequest,
    ) -> Result<RelayResponsePayload> {
        apply_credential_request_at(endpoint, &endpoint.home.join("github-token"), request)
    }

    fn apply_credential_request_at(
        endpoint: &CredentialEndpoint,
        github_token_path: &std::path::Path,
        request: &RelayRequest,
    ) -> Result<RelayResponsePayload> {
        use crate::hel_credentials::{
            CredentialSnapshot, MAX_CREDENTIAL_BYTES, MAX_GITHUB_TOKEN_BYTES, read_credential_file,
            read_github_token, remove_github_token, write_credential_file, write_github_token,
        };
        use crate::hel_skills::{
            MAX_SKILLS_ARCHIVE_BYTES, SkillsArchive, collect_skills, install_skills,
        };
        use base64::Engine as _;
        use base64::engine::general_purpose::STANDARD as BASE64;

        match request {
            RelayRequest::CredentialState => {
                let (snapshot, _) = read_credential_file(endpoint.harness, &endpoint.marker)?;
                Ok(credential_state_payload(&snapshot))
            }
            RelayRequest::ReadCredentials => {
                let (snapshot, bytes) = read_credential_file(endpoint.harness, &endpoint.marker)?;
                if !snapshot.present {
                    bail!("session has no {} credentials", endpoint.marker.display());
                }
                Ok(RelayResponsePayload::Credentials {
                    data: BASE64.encode(&bytes),
                })
            }
            RelayRequest::InstallCredentials { data } => {
                if data.len() > MAX_CREDENTIAL_BYTES * 2 {
                    bail!("credential payload is above the {MAX_CREDENTIAL_BYTES} byte limit");
                }
                let bytes = BASE64
                    .decode(data.as_bytes())
                    .context("decode credential payload")?;
                write_credential_file(endpoint.harness, &endpoint.marker, &bytes)?;
                Ok(credential_state_payload(&CredentialSnapshot::of(
                    endpoint.harness,
                    &bytes,
                )))
            }
            RelayRequest::SkillsState => {
                let archive = collect_skills(endpoint.harness, &endpoint.home)?;
                Ok(skills_state_payload(&archive.state()))
            }
            RelayRequest::InstallSkills { data } => {
                // Base64 inflates by a third; rejecting early keeps a hostile
                // controller from making the worker buffer an endless frame.
                if data.len() > MAX_SKILLS_ARCHIVE_BYTES * 2 {
                    bail!(
                        "skills payload is above the {MAX_SKILLS_ARCHIVE_BYTES} byte archive limit"
                    );
                }
                let bytes = BASE64
                    .decode(data.as_bytes())
                    .context("decode skills payload")?;
                let archive = SkillsArchive::decode(&bytes)?;
                install_skills(endpoint.harness, &endpoint.home, &archive)?;
                let installed = collect_skills(endpoint.harness, &endpoint.home)?;
                Ok(skills_state_payload(&installed.state()))
            }
            RelayRequest::GithubTokenState => {
                let (snapshot, _) = read_github_token(github_token_path)?;
                Ok(github_token_state_payload(&snapshot))
            }
            RelayRequest::InstallGithubToken { data } => {
                if data.len() > MAX_GITHUB_TOKEN_BYTES * 2 {
                    bail!("GitHub token is above the {MAX_GITHUB_TOKEN_BYTES} byte limit");
                }
                let bytes = BASE64
                    .decode(data.as_bytes())
                    .context("decode GitHub token payload")?;
                let snapshot = write_github_token(github_token_path, &bytes)?;
                Ok(github_token_state_payload(&snapshot))
            }
            RelayRequest::RemoveGithubToken => {
                remove_github_token(github_token_path)?;
                Ok(github_token_state_payload(
                    &crate::hel_credentials::GithubTokenSnapshot::absent(),
                ))
            }
            other => bail!(
                "{} is not a credential, GitHub token, or skills request",
                other.method_name()
            ),
        }
    }

    fn skills_state_payload(state: &crate::hel_skills::SkillsSyncState) -> RelayResponsePayload {
        RelayResponsePayload::SkillsState {
            present: state.present,
            fingerprint: state.fingerprint.clone(),
        }
    }

    fn credential_state_payload(
        snapshot: &crate::hel_credentials::CredentialSnapshot,
    ) -> RelayResponsePayload {
        RelayResponsePayload::CredentialState {
            present: snapshot.present,
            fingerprint: snapshot.fingerprint.clone(),
            freshness_epoch_ms: snapshot.freshness_epoch_ms,
        }
    }

    fn github_token_state_payload(
        snapshot: &crate::hel_credentials::GithubTokenSnapshot,
    ) -> RelayResponsePayload {
        RelayResponsePayload::GithubTokenState {
            present: snapshot.present,
            fingerprint: snapshot.fingerprint.clone(),
        }
    }

    pub(super) fn configure_github_cli(
        root: &std::path::Path,
        environment: &mut BTreeMap<String, String>,
    ) -> Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let bin = root.join("bin");
        if std::fs::symlink_metadata(&bin).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
            bail!(
                "GitHub CLI wrapper directory {} is a symbolic link",
                bin.display()
            );
        }
        std::fs::create_dir_all(&bin)
            .with_context(|| format!("create GitHub CLI wrapper directory {}", bin.display()))?;
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o700))?;

        let wrapper = bin.join("gh");
        if std::fs::symlink_metadata(&wrapper)
            .is_ok_and(|metadata| metadata.file_type().is_symlink())
        {
            bail!(
                "GitHub CLI wrapper {} is a symbolic link",
                wrapper.display()
            );
        }
        const WRAPPER: &str = r#"#!/bin/sh
set -eu
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
clean_path=
old_ifs=$IFS
IFS=:
for entry in $PATH; do
    if [ "$entry" != "$script_dir" ]; then
        if [ -z "$clean_path" ]; then clean_path=$entry; else clean_path=$clean_path:$entry; fi
    fi
done
IFS=$old_ifs
PATH=$clean_path
export PATH
token_file=$script_dir/../github-token
if [ -f "$token_file" ]; then
    IFS= read -r GH_TOKEN < "$token_file"
    export GH_TOKEN
    unset GITHUB_TOKEN
else
    unset GH_TOKEN GITHUB_TOKEN
fi
exec gh "$@"
"#;
        crate::hel_config::atomic_write_existing(&wrapper, WRAPPER.as_bytes())?;
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700))?;

        let inherited_path = environment
            .get("PATH")
            .cloned()
            .or_else(|| std::env::var("PATH").ok())
            .unwrap_or_default();
        environment.insert(
            "PATH".into(),
            if inherited_path.is_empty() {
                bin.to_string_lossy().into_owned()
            } else {
                format!("{}:{inherited_path}", bin.to_string_lossy())
            },
        );

        let inherited_token = std::env::var("GH_TOKEN")
            .ok()
            .or_else(|| std::env::var("GITHUB_TOKEN").ok());
        if let Some(token) = inherited_token
            && crate::hel_credentials::validate_github_token(token.as_bytes()).is_ok()
        {
            crate::hel_credentials::write_github_token(
                &root.join("github-token"),
                token.as_bytes(),
            )?;
        }
        Ok(())
    }

    enum CheckpointChange {
        Begin(String),
        /// The barrier no longer belongs to this connection, whether it ended
        /// through a full completion or an early dispatch release.
        Ended(String),
    }

    fn checkpoint_change(request: &RelayRequest) -> Option<CheckpointChange> {
        let RelayRequest::Submit {
            command_id,
            command,
        } = request
        else {
            return None;
        };
        match command {
            RelayCommand::BeginCheckpoint { .. } => {
                Some(CheckpointChange::Begin(command_id.clone()))
            }
            RelayCommand::CompleteCheckpoint { barrier_command_id }
            | RelayCommand::ReleaseCheckpoint { barrier_command_id } => {
                Some(CheckpointChange::Ended(barrier_command_id.clone()))
            }
            _ => None,
        }
    }

    fn report_fatal(
        fatal: &mpsc::Sender<anyhow::Error>,
        error: anyhow::Error,
        session_id: &str,
        reason: &str,
    ) {
        let detail = format!("{error:#}");
        tracing::error!(
            %session_id,
            %reason,
            error = %detail,
            "relay daemon reported a fatal failure"
        );
        if let Err(send_error) = fatal.try_send(error) {
            tracing::error!(
                %session_id,
                %reason,
                error = %send_error,
                "could not deliver relay fatal failure to daemon"
            );
        }
    }

    fn release_checkpoint_barriers(
        relay: &Arc<Mutex<DurableRelay>>,
        dispatch_wake: &mpsc::Sender<()>,
        checkpoint_barriers: BTreeSet<String>,
    ) -> Result<()> {
        let mut released = false;
        {
            let mut relay = relay.lock().expect("relay state lock poisoned");
            for command_id in checkpoint_barriers {
                released |= relay
                    .cancel_checkpoint_barrier_on_disconnect(&command_id)
                    .with_context(|| {
                        format!("release disconnected checkpoint barrier {command_id}")
                    })?
                    .is_some();
            }
        }
        if released {
            wake_dispatch(relay, dispatch_wake)
                .context("wake relay after releasing checkpoint barrier")?;
        }
        Ok(())
    }

    pub(super) fn wake_dispatch(
        relay: &Arc<Mutex<DurableRelay>>,
        dispatch_wake: &mpsc::Sender<()>,
    ) -> Result<()> {
        match dispatch_wake.try_send(()) {
            Ok(()) | Err(mpsc::error::TrySendError::Full(())) => Ok(()),
            Err(mpsc::error::TrySendError::Closed(()))
                if relay
                    .lock()
                    .expect("relay state lock poisoned")
                    .operational_state()
                    .execution
                    == crate::hel_worker::RelayExecutionState::Closed =>
            {
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(())) => bail!("relay coordinator stopped"),
        }
    }

    pub(super) async fn write_response(
        writer: &mut tokio::net::unix::OwnedWriteHalf,
        response: &RelayResponseEnvelope,
    ) -> Result<()> {
        let mut encoded = serde_json::to_vec(response)?;
        if encoded.len().saturating_add(1) > crate::hel_worker::MAX_FRAME_BYTES {
            bail!("relay response frame is too large");
        }
        encoded.push(b'\n');
        writer.write_all(&encoded).await?;
        writer.flush().await?;
        Ok(())
    }

    /// Protocol rejections are ordinary responses rather than Rust errors, so
    /// the socket loop must log them explicitly. Keep this next to the write
    /// boundary so every request route (including credentials and old
    /// protocol methods) gets the same session and operation context.
    async fn write_logged_response(
        writer: &mut tokio::net::unix::OwnedWriteHalf,
        response: &RelayResponseEnvelope,
        session_id: &str,
        operation: &str,
    ) -> Result<()> {
        if let RelayResponseBody::Error { error } = &response.body {
            tracing::warn!(
                %session_id,
                %operation,
                request_id = %response.request_id,
                protocol_version = response.protocol_version,
                relay_error_code = ?error.code,
                relay_retryable = error.retryable,
                error_message = %error.message,
                "relay request returned an error"
            );
        }
        write_response(writer, response).await
    }

    pub(super) async fn forward_proxy_streams(
        mut client_read: impl tokio::io::AsyncRead + Unpin,
        mut client_write: impl tokio::io::AsyncWrite + Unpin,
        mut relay_read: impl tokio::io::AsyncRead + Unpin,
        mut relay_write: impl tokio::io::AsyncWrite + Unpin,
    ) -> Result<()> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // The proxy must die with its client. Joining both copy directions
        // left the process alive forever after stdin EOF (a killed `podman
        // exec` client), leaking one thread-heavy process per poll inside the
        // container. Exit as soon as either side closes. An idle connection
        // is intentional: it may own a checkpoint barrier while the
        // controller transfers a large archive. Before the first request,
        // however, a bounded deadline prevents a detached Podman conmon from
        // holding the target-side proxy forever after the controller kills a
        // launcher whose handshake timed out.
        let mut client_buf = [0_u8; 64 * 1024];
        let mut relay_buf = [0_u8; 64 * 1024];
        let first_count = match tokio::time::timeout(
            PROXY_INITIAL_INPUT_TIMEOUT,
            client_read.read(&mut client_buf),
        )
        .await
        {
            Ok(read) => read.context("read initial proxy stdin")?,
            Err(_) => {
                tracing::debug!(
                    operation = "proxy_initial_input",
                    "relay proxy client sent no initial frame before the idle deadline"
                );
                return Ok(());
            }
        };
        if first_count == 0 {
            if let Err(error) = relay_write.shutdown().await {
                tracing::debug!(
                    operation = "proxy_shutdown",
                    %error,
                    "could not close relay socket after proxy client EOF"
                );
            }
            return Ok(());
        }
        relay_write
            .write_all(&client_buf[..first_count])
            .await
            .context("forward initial request to worker")?;

        loop {
            tokio::select! {
                read = client_read.read(&mut client_buf) => {
                    let count = read.context("read proxy stdin")?;
                    if count == 0 {
                        // Client is gone; flush any final in-flight response
                        // briefly, then exit.
                        if let Err(error) = relay_write.shutdown().await {
                            tracing::debug!(
                                operation = "proxy_shutdown",
                                %error,
                                "could not close relay socket after proxy client EOF"
                            );
                        }
                        if let Err(error) = tokio::time::timeout(
                            std::time::Duration::from_millis(500),
                            tokio::io::copy(&mut relay_read, &mut client_write),
                        )
                        .await
                        {
                            tracing::debug!(
                                operation = "proxy_final_response",
                                %error,
                                "could not forward the final relay response before proxy shutdown"
                            );
                        }
                        return Ok(());
                    }
                    relay_write
                        .write_all(&client_buf[..count])
                        .await
                        .context("forward request to worker")?;
                }
                read = relay_read.read(&mut relay_buf) => {
                    let count = read.context("read worker socket")?;
                    if count == 0 {
                        return Ok(());
                    }
                    client_write
                        .write_all(&relay_buf[..count])
                        .await
                        .context("forward response to client")?;
                    client_write.flush().await.context("flush proxy stdout")?;
                }
            }
        }
    }

    pub async fn proxy(root: PathBuf) -> Result<()> {
        let stream = UnixStream::connect(root.join("control.sock"))
            .await
            .with_context(|| format!("connect worker at {}", root.display()))?;
        let (socket_read, socket_write) = stream.into_split();
        forward_proxy_streams(
            tokio::io::stdin(),
            tokio::io::stdout(),
            socket_read,
            socket_write,
        )
        .await
    }

    /// Own the ACP bridge's process group.  The daemon communicates only with
    /// this supervisor; if the daemon is killed, stdin reaches EOF and the
    /// complete bridge process tree is terminated and reaped.
    pub async fn run_acp_supervisor(spec: AcpSupervisorSpec) -> Result<()> {
        use tokio::io::AsyncWriteExt;

        let mut command = tokio::process::Command::new(&spec.command);
        command
            .args(&spec.args)
            .envs(&spec.environment)
            .current_dir(&spec.cwd)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit());
        command.process_group(0);
        let mut child = command
            .spawn()
            .with_context(|| format!("launch supervised ACP bridge {}", spec.command.display()))?;
        let pid = child
            .id()
            .context("supervised ACP bridge has no process ID")? as i32;
        let mut child_stdin = child.stdin.take().context("ACP bridge stdin unavailable")?;
        let mut child_stdout = child
            .stdout
            .take()
            .context("ACP bridge stdout unavailable")?;
        let mut parent_stdin = tokio::io::stdin();
        let mut parent_stdout = tokio::io::stdout();

        let child_output_ended = {
            let input = tokio::io::copy(&mut parent_stdin, &mut child_stdin);
            let output = tokio::io::copy(&mut child_stdout, &mut parent_stdout);
            tokio::pin!(input, output);
            tokio::select! {
                result = &mut input => {
                    result.context("forward ACP supervisor input")?;
                    false
                }
                result = &mut output => {
                    result.context("forward ACP supervisor output")?;
                    true
                }
            }
        };
        if let Err(error) = child_stdin.shutdown().await {
            tracing::debug!(
                operation = "acp_supervisor_shutdown",
                %error,
                "could not close ACP bridge stdin before termination"
            );
        }
        terminate_process_group(pid, libc::SIGTERM);
        match tokio::time::timeout(std::time::Duration::from_secs(2), child.wait()).await {
            Ok(status) => {
                let status = status.context("wait for supervised ACP bridge")?;
                if child_output_ended && !status.success() {
                    bail!("supervised ACP bridge exited with {status}");
                }
            }
            Err(_) => {
                terminate_process_group(pid, libc::SIGKILL);
                child.wait().await.context("reap supervised ACP bridge")?;
            }
        }
        Ok(())
    }

    /// Signal a whole process group. Terminals reuse this so process-group
    /// termination lives in one place.
    pub(crate) fn terminate_process_group(pid: i32, signal: i32) {
        // SAFETY: a negative, validated child PID targets only the process
        // group created for this supervisor's child.
        unsafe {
            libc::kill(-pid, signal);
        }
    }

    /// Make this process lead its own session, so session teardown can stop
    /// the whole worker tree with a single process-group signal. Failing means
    /// the process already leads its group, which is the state we wanted.
    ///
    /// Only the real daemon entry point may call this: it detaches the caller
    /// from its controlling terminal.
    pub fn lead_process_group() {
        // SAFETY: setsid takes no arguments and changes only this process's
        // own session and process-group membership.
        unsafe {
            libc::setsid();
        }
    }
}

#[cfg(not(unix))]
pub fn lead_process_group() {}

/// Where this relay's harness keeps its home, resolved solely from the launch
/// config. Credential and skills requests carry no path, so a caller cannot
/// steer a read or write outside the session's harness home.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialEndpoint {
    pub harness: HarnessKind,
    /// The session's harness home; skills trees sync under it.
    pub home: PathBuf,
    pub marker: PathBuf,
}

#[cfg(unix)]
fn credential_endpoint(
    config: &WorkerLaunchConfig,
) -> std::result::Result<CredentialEndpoint, String> {
    let key = config.harness.home_env();
    let home = config.environment.get(key).ok_or_else(|| {
        format!("worker launch config has no {key} entry, so it cannot locate harness credentials")
    })?;
    Ok(CredentialEndpoint {
        harness: config.harness,
        home: PathBuf::from(home.as_str()),
        marker: crate::hel_setup::harness_authentication_marker(
            config.harness,
            Path::new(home.as_str()),
        ),
    })
}

#[cfg(unix)]
fn resolve_relative_harness_home(config: &mut WorkerLaunchConfig, base: &Path) {
    let key = config.harness.home_env();
    if let Some(value) = config.environment.get_mut(key) {
        let path = Path::new(value);
        if path.is_relative() {
            *value = base.join(path).to_string_lossy().into_owned();
        }
    }
    if let Some(memory) = config.project_memory.as_mut() {
        if memory.root.is_relative() {
            memory.root = base.join(&memory.root);
        }
        if memory.baseline_root.as_os_str().is_empty() {
            memory.baseline_root = memory
                .root
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join(".hel-memory-baseline");
        }
        if memory.baseline_root.is_relative() {
            memory.baseline_root = base.join(&memory.baseline_root);
        }
    }
}

#[cfg(unix)]
fn resolve_relative_worker_root(root: PathBuf, base: &Path) -> PathBuf {
    if root.is_relative() {
        base.join(root)
    } else {
        root
    }
}

#[cfg(unix)]
pub use unix::{lead_process_group, proxy, run_acp_supervisor, run_daemon};

#[cfg(unix)]
pub(crate) use unix::terminate_process_group;

#[cfg(not(unix))]
pub async fn run_daemon(
    _root: std::path::PathBuf,
    _config: WorkerLaunchConfig,
) -> anyhow::Result<()> {
    anyhow::bail!("target workers require Unix")
}

#[cfg(not(unix))]
pub async fn proxy(_root: std::path::PathBuf) -> anyhow::Result<()> {
    anyhow::bail!("target workers require Unix")
}

#[cfg(not(unix))]
pub async fn run_acp_supervisor(_spec: AcpSupervisorSpec) -> anyhow::Result<()> {
    anyhow::bail!("ACP supervision requires Unix")
}

#[cfg(all(test, unix))]
mod relay_tests {
    use std::collections::BTreeMap;
    use std::path::Path;
    use std::sync::{Arc, Mutex};

    use agent_client_protocol::schema::ProtocolVersion;
    use agent_client_protocol::schema::v1::{
        AgentCapabilities, ContentBlock, ContentChunk, Implementation, SessionUpdate, TextContent,
    };
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::sync::mpsc;

    use super::*;
    use crate::hel_acp::{CommandRequest, RuntimeEvent};
    use crate::hel_worker::{
        DurableRelay, RELAY_EVENT_GENESIS_DIGEST, RELAY_PROTOCOL_VERSION, RelayCommand,
        RelayErrorCode, RelayExecutionState, RelayObservation, RelayProtocolError, RelayRequest,
        RelayRequestEnvelope, RelayResponseBody, RelayResponseEnvelope, RelayResponsePayload,
    };

    const SESSION_ID: &str = "018f9dd2-a3b4-7c8d-9000-123456789abc";

    struct TestRuntimeEventSender(mpsc::Sender<RuntimeEvent>);

    impl TestRuntimeEventSender {
        fn send(&self, event: RuntimeEvent) -> std::result::Result<(), ()> {
            self.0.try_send(event).map_err(|_| ())
        }
    }

    fn runtime_event_channel() -> (TestRuntimeEventSender, mpsc::Receiver<RuntimeEvent>) {
        let (sender, receiver) = mpsc::channel(unix::ACP_EVENT_CHANNEL_CAPACITY);
        (TestRuntimeEventSender(sender), receiver)
    }

    /// Channel a served client uses to report that durable state became
    /// unwritable. Most fixtures only need somewhere for that report to go.
    fn fatal_reports() -> (mpsc::Sender<anyhow::Error>, mpsc::Receiver<anyhow::Error>) {
        mpsc::channel(1)
    }

    fn launch_config(profile_home: &str) -> WorkerLaunchConfig {
        WorkerLaunchConfig {
            session_id: SESSION_ID.into(),
            harness: HarnessKind::Codex,
            bridge_command: "codex-acp".into(),
            bridge_args: Vec::new(),
            environment: BTreeMap::from([("CODEX_HOME".into(), profile_home.into())]),
            cwd: ".local/share/hel/workspaces/session/repo".into(),
            additional_directories: Vec::new(),
            native_session_id: None,
            project_memory: None,
            execution_policy: ExecutionPolicy::Unconstrained,
        }
    }

    fn test_credentials() -> std::result::Result<CredentialEndpoint, String> {
        credential_endpoint(&launch_config("/profile"))
    }

    fn codex_credentials(last_refresh: &str) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "auth_mode": "chatgpt",
            "tokens": { "access_token": "access", "refresh_token": "refresh" },
            "last_refresh": last_refresh,
        }))
        .unwrap()
    }

    fn install_request(bytes: &[u8]) -> RelayRequest {
        use base64::Engine as _;
        RelayRequest::InstallCredentials {
            data: base64::engine::general_purpose::STANDARD.encode(bytes),
        }
    }

    fn skills_install_request(bytes: &[u8]) -> RelayRequest {
        use base64::Engine as _;
        RelayRequest::InstallSkills {
            data: base64::engine::general_purpose::STANDARD.encode(bytes),
        }
    }

    fn skills_state_of(payload: RelayResponsePayload) -> crate::hel_skills::SkillsSyncState {
        let RelayResponsePayload::SkillsState {
            present,
            fingerprint,
        } = payload
        else {
            panic!("expected a skills state payload, got {payload:?}");
        };
        crate::hel_skills::SkillsSyncState {
            present,
            fingerprint,
        }
    }

    fn github_token_state_of(
        payload: RelayResponsePayload,
    ) -> crate::hel_credentials::GithubTokenSnapshot {
        let RelayResponsePayload::GithubTokenState {
            present,
            fingerprint,
        } = payload
        else {
            panic!("expected a GitHub token state payload, got {payload:?}");
        };
        crate::hel_credentials::GithubTokenSnapshot {
            present,
            fingerprint,
        }
    }

    #[test]
    fn github_token_requests_install_and_remove_connection_only_state() {
        use base64::Engine as _;

        let home = tempfile::tempdir().unwrap();
        let endpoint = credential_endpoint(&launch_config(&home.path().to_string_lossy())).unwrap();
        let absent = github_token_state_of(
            unix::apply_credential_request(&endpoint, &RelayRequest::GithubTokenState).unwrap(),
        );
        assert!(!absent.present);

        let installed = github_token_state_of(
            unix::apply_credential_request(
                &endpoint,
                &RelayRequest::InstallGithubToken {
                    data: base64::engine::general_purpose::STANDARD.encode(b"fresh-token"),
                },
            )
            .unwrap(),
        );
        assert_eq!(
            installed,
            crate::hel_credentials::GithubTokenSnapshot::of("fresh-token")
        );
        let removed = github_token_state_of(
            unix::apply_credential_request(&endpoint, &RelayRequest::RemoveGithubToken).unwrap(),
        );
        assert!(!removed.present);
    }

    #[test]
    fn github_cli_wrapper_reads_each_live_token_and_clears_stale_environment() {
        use std::os::unix::fs::PermissionsExt;

        let worker = tempfile::tempdir().unwrap();
        let real = tempfile::tempdir().unwrap();
        let real_bin = real.path().join("bin");
        std::fs::create_dir(&real_bin).unwrap();
        let real_gh = real_bin.join("gh");
        std::fs::write(
            &real_gh,
            b"#!/bin/sh\nprintf '%s|%s\\n' \"${GH_TOKEN-unset}\" \"${GITHUB_TOKEN-unset}\"\n",
        )
        .unwrap();
        std::fs::set_permissions(&real_gh, std::fs::Permissions::from_mode(0o700)).unwrap();

        let mut environment = BTreeMap::from([(
            "PATH".into(),
            format!("{}:/usr/bin:/bin", real_bin.display()),
        )]);
        unix::configure_github_cli(worker.path(), &mut environment).unwrap();
        let token_path = worker.path().join("github-token");
        crate::hel_credentials::remove_github_token(&token_path).unwrap();

        let invoke = |expected_token: Option<&str>| {
            match expected_token {
                Some(token) => {
                    crate::hel_credentials::write_github_token(&token_path, token.as_bytes())
                        .unwrap();
                }
                None => crate::hel_credentials::remove_github_token(&token_path).unwrap(),
            }
            let mut command = std::process::Command::new(worker.path().join("bin/gh"));
            command
                .env_clear()
                .env("PATH", environment.get("PATH").unwrap())
                .env("GH_TOKEN", "stale-token")
                .env("GITHUB_TOKEN", "also-stale");
            let output = crate::hel_subprocess::run_with_input(&mut command, &[]).unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            String::from_utf8(output.stdout).unwrap()
        };

        assert_eq!(invoke(Some("first-token")), "first-token|unset\n");
        assert_eq!(invoke(Some("rotated-token")), "rotated-token|unset\n");
        assert_eq!(invoke(None), "unset|unset\n");
    }

    #[test]
    fn skills_state_reports_an_empty_home_then_a_synced_tree() {
        let home = tempfile::tempdir().unwrap();
        let endpoint = credential_endpoint(&launch_config(&home.path().to_string_lossy())).unwrap();

        let empty = skills_state_of(
            unix::apply_credential_request(&endpoint, &RelayRequest::SkillsState).unwrap(),
        );
        assert!(!empty.present);

        std::fs::create_dir_all(home.path().join("skills/review")).unwrap();
        std::fs::write(home.path().join("skills/review/SKILL.md"), b"review").unwrap();
        let state = skills_state_of(
            unix::apply_credential_request(&endpoint, &RelayRequest::SkillsState).unwrap(),
        );
        let expected = crate::hel_skills::collect_skills(HarnessKind::Codex, home.path()).unwrap();
        assert!(state.present);
        assert_eq!(state.fingerprint, expected.fingerprint());
    }

    #[test]
    fn install_skills_replaces_the_session_tree_and_reports_the_new_state() {
        let canonical = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(canonical.path().join("skills/review")).unwrap();
        std::fs::write(canonical.path().join("skills/review/SKILL.md"), b"v1").unwrap();
        let archive =
            crate::hel_skills::collect_skills(HarnessKind::Codex, canonical.path()).unwrap();

        let home = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(home.path().join("skills/stale")).unwrap();
        std::fs::write(home.path().join("skills/stale/SKILL.md"), b"old").unwrap();
        let endpoint = credential_endpoint(&launch_config(&home.path().to_string_lossy())).unwrap();

        let state = skills_state_of(
            unix::apply_credential_request(&endpoint, &skills_install_request(&archive.encode()))
                .unwrap(),
        );
        assert_eq!(state, archive.state());
        assert_eq!(
            std::fs::read(home.path().join("skills/review/SKILL.md")).unwrap(),
            b"v1"
        );
        assert!(!home.path().join("skills/stale").exists());

        let empty = crate::hel_skills::SkillsArchive::default();
        let state = skills_state_of(
            unix::apply_credential_request(&endpoint, &skills_install_request(&empty.encode()))
                .unwrap(),
        );
        assert!(!state.present);
        assert!(!home.path().join("skills").exists());
    }

    #[test]
    fn install_skills_rejects_garbage_and_leaves_the_tree_untouched() {
        let home = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(home.path().join("skills")).unwrap();
        std::fs::write(home.path().join("skills/keep.md"), b"keep").unwrap();
        let endpoint = credential_endpoint(&launch_config(&home.path().to_string_lossy())).unwrap();

        let error = unix::apply_credential_request(
            &endpoint,
            &skills_install_request(b"garbage-with-enough-length"),
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("magic"), "{error:#}");
        assert_eq!(
            std::fs::read(home.path().join("skills/keep.md")).unwrap(),
            b"keep"
        );
    }

    #[test]
    fn non_credential_requests_are_not_served_by_the_home_handler() {
        let error =
            unix::apply_credential_request(&test_credentials().unwrap(), &RelayRequest::Status)
                .unwrap_err();
        assert!(
            format!("{error:#}").contains("credential, GitHub token, or skills"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn credential_exchange_stays_on_the_connection_and_out_of_relay_state() {
        use base64::Engine as _;

        let temp = tempfile::tempdir().unwrap();
        let relay_root = temp.path().join("relay");
        let relay = Arc::new(Mutex::new(
            DurableRelay::open(&relay_root, SESSION_ID, "1.0.0").unwrap(),
        ));
        let endpoint = credential_endpoint(&launch_config(&temp.path().to_string_lossy())).unwrap();
        let (wake_tx, _wake_rx) = mpsc::channel(1);
        let (server, client) = tokio::net::UnixStream::pair().unwrap();
        let server_task = tokio::spawn(unix::serve_client(
            server,
            relay.clone(),
            wake_tx,
            Ok(endpoint),
            None,
            fatal_reports().0,
        ));
        let (reader, mut writer) = client.into_split();
        let mut lines = BufReader::new(reader).lines();
        let bytes = codex_credentials("2026-08-05T02:51:00.864587231Z");
        let request = RelayRequestEnvelope {
            request_id: "install-credentials".into(),
            protocol_version: RELAY_PROTOCOL_VERSION,
            request: install_request(&bytes),
        };
        let mut encoded = serde_json::to_vec(&request).unwrap();
        encoded.push(b'\n');
        writer.write_all(&encoded).await.unwrap();

        let response: RelayResponseEnvelope =
            serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        let RelayResponseBody::Ok {
            payload:
                RelayResponsePayload::CredentialState {
                    present,
                    fingerprint,
                    freshness_epoch_ms,
                },
        } = response.body
        else {
            panic!("credential install failed: {:?}", response.body);
        };
        assert!(present);
        assert_eq!(
            fingerprint,
            crate::hel_credentials::credential_fingerprint(&bytes)
        );
        assert_eq!(freshness_epoch_ms, Some(1_785_898_260_864));
        assert_eq!(std::fs::read(temp.path().join("auth.json")).unwrap(), bytes);

        let read = RelayRequestEnvelope {
            request_id: "read-credentials".into(),
            protocol_version: RELAY_PROTOCOL_VERSION,
            request: RelayRequest::ReadCredentials,
        };
        let mut encoded = serde_json::to_vec(&read).unwrap();
        encoded.push(b'\n');
        writer.write_all(&encoded).await.unwrap();
        let response: RelayResponseEnvelope =
            serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        let RelayResponseBody::Ok {
            payload: RelayResponsePayload::Credentials { data },
        } = response.body
        else {
            panic!("credential read failed: {:?}", response.body);
        };
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(data.as_bytes())
                .unwrap(),
            bytes
        );

        {
            let relay = relay.lock().unwrap();
            assert_eq!(relay.latest_ordinal(), 0);
            assert!(
                relay
                    .events_after(0, RELAY_EVENT_GENESIS_DIGEST)
                    .unwrap()
                    .is_empty()
            );
            let persisted = std::fs::read_to_string(relay_root.join("relay-state.json")).unwrap();
            assert!(!persisted.contains(&request.request_id));
        }

        drop(writer);
        drop(lines);
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn compaction_reaches_the_acp_runtime_and_stays_out_of_relay_state() {
        let temp = tempfile::tempdir().unwrap();
        let relay_root = temp.path().join("relay");
        let relay = Arc::new(Mutex::new(
            DurableRelay::open(&relay_root, SESSION_ID, "1.0.0").unwrap(),
        ));
        let (wake_tx, _wake_rx) = mpsc::channel(1);
        let (commands_tx, mut commands_rx) = mpsc::channel(1);
        let (server, client) = tokio::net::UnixStream::pair().unwrap();
        let server_task = tokio::spawn(unix::serve_client(
            server,
            relay.clone(),
            wake_tx,
            test_credentials(),
            Some(commands_tx),
            fatal_reports().0,
        ));
        let runtime = tokio::spawn(async move {
            let Some(CommandRequest::Compact { prompt, response }) = commands_rx.recv().await
            else {
                panic!("the relay must route compaction to the ACP runtime");
            };
            assert!(prompt.contains("summarize the history"));
            let _ = response.send(Ok("<state_snapshot>kept</state_snapshot>".into()));
        });

        let (reader, mut writer) = client.into_split();
        let mut lines = BufReader::new(reader).lines();
        let request = RelayRequestEnvelope {
            request_id: "compact-request".into(),
            protocol_version: RELAY_PROTOCOL_VERSION,
            request: RelayRequest::Compact {
                prompt: "please summarize the history".into(),
            },
        };
        let mut encoded = serde_json::to_vec(&request).unwrap();
        encoded.push(b'\n');
        writer.write_all(&encoded).await.unwrap();

        let response: RelayResponseEnvelope =
            serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        let RelayResponseBody::Ok {
            payload: RelayResponsePayload::Compacted { text },
        } = response.body
        else {
            panic!("compaction failed: {:?}", response.body);
        };
        assert_eq!(text, "<state_snapshot>kept</state_snapshot>");
        runtime.await.unwrap();

        {
            let mut relay = relay.lock().unwrap();
            assert_eq!(relay.latest_ordinal(), 0);
            assert!(
                relay
                    .events_after(0, RELAY_EVENT_GENESIS_DIGEST)
                    .unwrap()
                    .is_empty()
            );
            assert!(relay.claim_pending_commands(true).unwrap().is_empty());
            let persisted = std::fs::read_to_string(relay_root.join("relay-state.json")).unwrap();
            assert!(!persisted.contains("summarize the history"));
        }

        drop(writer);
        drop(lines);
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn compaction_fails_when_no_acp_runtime_can_serve_it() {
        let temp = tempfile::tempdir().unwrap();
        let relay = Arc::new(Mutex::new(
            DurableRelay::open(temp.path().join("relay"), SESSION_ID, "1.0.0").unwrap(),
        ));
        let (wake_tx, _wake_rx) = mpsc::channel(1);
        let (server, client) = tokio::net::UnixStream::pair().unwrap();
        let server_task = tokio::spawn(unix::serve_client(
            server,
            relay.clone(),
            wake_tx,
            test_credentials(),
            None,
            fatal_reports().0,
        ));
        let (reader, mut writer) = client.into_split();
        let mut lines = BufReader::new(reader).lines();
        let request = RelayRequestEnvelope {
            request_id: "compact-sealed".into(),
            protocol_version: RELAY_PROTOCOL_VERSION,
            request: RelayRequest::Compact {
                prompt: "summarize".into(),
            },
        };
        let mut encoded = serde_json::to_vec(&request).unwrap();
        encoded.push(b'\n');
        writer.write_all(&encoded).await.unwrap();

        let response: RelayResponseEnvelope =
            serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        let RelayResponseBody::Error { error } = response.body else {
            panic!("a sealed session cannot compact");
        };
        assert_eq!(error.code, RelayErrorCode::InvalidState);
        assert!(!error.retryable);

        drop(writer);
        drop(lines);
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn incompatible_protocol_cannot_compact() {
        let temp = tempfile::tempdir().unwrap();
        let relay = Arc::new(Mutex::new(
            DurableRelay::open(temp.path().join("relay"), SESSION_ID, "1.0.0").unwrap(),
        ));
        let (wake_tx, _wake_rx) = mpsc::channel(1);
        let (commands_tx, mut commands_rx) = mpsc::channel(1);
        let (server, client) = tokio::net::UnixStream::pair().unwrap();
        let server_task = tokio::spawn(unix::serve_client(
            server,
            relay.clone(),
            wake_tx,
            test_credentials(),
            Some(commands_tx),
            fatal_reports().0,
        ));
        let (reader, mut writer) = client.into_split();
        let mut lines = BufReader::new(reader).lines();
        let request = RelayRequestEnvelope {
            request_id: "compact-old-protocol".into(),
            protocol_version: RELAY_PROTOCOL_VERSION + 1,
            request: RelayRequest::Compact {
                prompt: "summarize".into(),
            },
        };
        let mut encoded = serde_json::to_vec(&request).unwrap();
        encoded.push(b'\n');
        writer.write_all(&encoded).await.unwrap();

        let response: RelayResponseEnvelope =
            serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        let RelayResponseBody::Error { error } = response.body else {
            panic!("an incompatible protocol cannot compact");
        };
        assert_eq!(error.code, RelayErrorCode::IncompatibleProtocol);
        assert!(commands_rx.try_recv().is_err());

        drop(writer);
        drop(lines);
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn protocol_v1_can_compact() {
        let temp = tempfile::tempdir().unwrap();
        let relay = Arc::new(Mutex::new(
            DurableRelay::open(temp.path().join("relay"), SESSION_ID, "1.0.0").unwrap(),
        ));
        let (wake_tx, _wake_rx) = mpsc::channel(1);
        let (server, client) = tokio::net::UnixStream::pair().unwrap();
        let server_task = tokio::spawn(unix::serve_client(
            server,
            relay,
            wake_tx,
            test_credentials(),
            None,
            fatal_reports().0,
        ));
        let (reader, mut writer) = client.into_split();
        let mut lines = BufReader::new(reader).lines();
        let request = RelayRequestEnvelope {
            request_id: "compact-v1".into(),
            protocol_version: 1,
            request: RelayRequest::Compact {
                prompt: "summarize".into(),
            },
        };
        let mut encoded = serde_json::to_vec(&request).unwrap();
        encoded.push(b'\n');
        writer.write_all(&encoded).await.unwrap();

        let response: RelayResponseEnvelope =
            serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        let RelayResponseBody::Error { error } = response.body else {
            panic!("expected a closed-session compact error, got {response:?}");
        };
        assert_eq!(error.code, RelayErrorCode::InvalidState);
        assert!(error.message.contains("session is closed"), "{error:?}");

        drop(writer);
        drop(lines);
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn protocol_v1_cannot_respond_to_elicitation() {
        let temp = tempfile::tempdir().unwrap();
        let relay = Arc::new(Mutex::new(
            DurableRelay::open(temp.path().join("relay"), SESSION_ID, "1.0.0").unwrap(),
        ));
        let (wake_tx, _wake_rx) = mpsc::channel(1);
        let (commands_tx, mut commands_rx) = mpsc::channel(1);
        let (server, client) = tokio::net::UnixStream::pair().unwrap();
        let server_task = tokio::spawn(unix::serve_client(
            server,
            relay,
            wake_tx,
            test_credentials(),
            Some(commands_tx),
            fatal_reports().0,
        ));
        let (reader, mut writer) = client.into_split();
        let mut lines = BufReader::new(reader).lines();
        let request = RelayRequestEnvelope {
            request_id: "elicit-v1".into(),
            protocol_version: 1,
            request: RelayRequest::RespondElicitation {
                elicitation_id: "form-1".into(),
                response: crate::hel_elicitation::ElicitationResponse::Cancel,
            },
        };
        let mut encoded = serde_json::to_vec(&request).unwrap();
        encoded.push(b'\n');
        writer.write_all(&encoded).await.unwrap();

        let response: RelayResponseEnvelope =
            serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        let RelayResponseBody::Error { error } = response.body else {
            panic!("protocol v1 must not answer elicitations, got {response:?}");
        };
        assert_eq!(error.code, RelayErrorCode::IncompatibleProtocol);
        assert!(commands_rx.try_recv().is_err());

        drop(writer);
        drop(lines);
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn incompatible_protocol_cannot_read_or_mutate_credentials() {
        let home = tempfile::tempdir().unwrap();
        let relay = Arc::new(Mutex::new(
            DurableRelay::open(home.path().join("relay"), SESSION_ID, "1.0.0").unwrap(),
        ));
        let endpoint = credential_endpoint(&launch_config(&home.path().to_string_lossy())).unwrap();
        let original = codex_credentials("2026-08-05T02:51:00Z");
        crate::hel_credentials::write_credential_file(
            endpoint.harness,
            &endpoint.marker,
            &original,
        )
        .unwrap();
        let replacement = codex_credentials("2026-08-06T02:51:00Z");
        let (wake_tx, _wake_rx) = mpsc::channel(1);
        let (server, client) = tokio::net::UnixStream::pair().unwrap();
        let server_task = tokio::spawn(unix::serve_client(
            server,
            relay,
            wake_tx,
            Ok(endpoint),
            None,
            fatal_reports().0,
        ));
        let (reader, mut writer) = client.into_split();
        let mut lines = BufReader::new(reader).lines();

        for (request_id, request) in [
            ("read-with-v0", RelayRequest::ReadCredentials),
            ("install-with-v0", install_request(&replacement)),
        ] {
            let envelope = RelayRequestEnvelope {
                request_id: request_id.into(),
                protocol_version: 0,
                request,
            };
            let mut encoded = serde_json::to_vec(&envelope).unwrap();
            encoded.push(b'\n');
            writer.write_all(&encoded).await.unwrap();
            let response: RelayResponseEnvelope =
                serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
            assert_eq!(response.protocol_version, 0);
            assert!(matches!(
                response.body,
                RelayResponseBody::Error {
                    error: RelayProtocolError {
                        code: RelayErrorCode::IncompatibleProtocol,
                        retryable: false,
                        ..
                    }
                }
            ));
        }

        assert_eq!(
            std::fs::read(home.path().join("auth.json")).unwrap(),
            original
        );
        drop(writer);
        drop(lines);
        server_task.await.unwrap().unwrap();
    }

    #[test]
    fn installed_credentials_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let home = tempfile::tempdir().unwrap();
        let endpoint = credential_endpoint(&launch_config(&home.path().to_string_lossy())).unwrap();
        unix::apply_credential_request(
            &endpoint,
            &install_request(&codex_credentials("2026-08-05T02:51:00Z")),
        )
        .unwrap();

        let mode = std::fs::metadata(home.path().join("auth.json"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    #[test]
    fn installing_kimi_credentials_uses_its_fixed_nested_marker() {
        let home = tempfile::tempdir().unwrap();
        let mut config = launch_config(&home.path().to_string_lossy());
        config.harness = HarnessKind::Kimi;
        config.environment = BTreeMap::from([(
            "KIMI_CODE_HOME".to_owned(),
            home.path().to_string_lossy().into_owned(),
        )]);
        let endpoint = credential_endpoint(&config).unwrap();
        let bytes = serde_json::to_vec(&serde_json::json!({
            "access_token": "access",
            "expires_at": 1_755_000_000,
        }))
        .unwrap();

        unix::apply_credential_request(&endpoint, &install_request(&bytes)).unwrap();

        assert_eq!(
            std::fs::read(home.path().join("credentials/kimi-code.json")).unwrap(),
            bytes
        );
    }

    #[test]
    fn absent_reads_and_invalid_installs_are_refused() {
        let home = tempfile::tempdir().unwrap();
        let endpoint = credential_endpoint(&launch_config(&home.path().to_string_lossy())).unwrap();

        let error =
            unix::apply_credential_request(&endpoint, &RelayRequest::ReadCredentials).unwrap_err();
        assert!(format!("{error:#}").contains("no"), "{error:#}");

        let error =
            unix::apply_credential_request(&endpoint, &install_request(b"not json")).unwrap_err();
        assert!(format!("{error:#}").contains("JSON"), "{error:#}");

        let oversized = vec![b'a'; crate::hel_credentials::MAX_CREDENTIAL_BYTES + 1];
        let error =
            unix::apply_credential_request(&endpoint, &install_request(&oversized)).unwrap_err();
        assert!(format!("{error:#}").contains("limit"), "{error:#}");
    }

    #[test]
    fn installing_over_a_symlink_leaves_the_link_target_untouched() {
        let home = tempfile::tempdir().unwrap();
        let elsewhere = home.path().join("stolen.json");
        std::fs::write(&elsewhere, b"{}").unwrap();
        std::os::unix::fs::symlink(&elsewhere, home.path().join("auth.json")).unwrap();
        let endpoint = credential_endpoint(&launch_config(&home.path().to_string_lossy())).unwrap();

        let error = unix::apply_credential_request(
            &endpoint,
            &install_request(&codex_credentials("2026-08-05T02:51:00Z")),
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("symbolic link"), "{error:#}");
        assert_eq!(std::fs::read(&elsewhere).unwrap(), b"{}");
    }

    #[test]
    fn a_launch_config_without_a_harness_home_cannot_serve_credentials() {
        let mut config = launch_config("/profile");
        config.environment.clear();

        let error = credential_endpoint(&config).unwrap_err();

        assert!(error.contains("CODEX_HOME"), "{error}");
    }

    #[test]
    fn launch_wires_require_the_new_baseline_shape() {
        let launch = launch_config("profile-home");
        let mut retired = serde_json::to_value(&launch).unwrap();
        retired
            .as_object_mut()
            .unwrap()
            .insert("recover_native_session".into(), serde_json::json!(true));
        assert!(serde_json::from_value::<WorkerLaunchConfig>(retired).is_err());

        let mut incomplete = serde_json::to_value(&launch).unwrap();
        incomplete.as_object_mut().unwrap().remove("bridge_args");
        assert!(serde_json::from_value::<WorkerLaunchConfig>(incomplete).is_err());

        let mut missing_policy = serde_json::to_value(&launch).unwrap();
        missing_policy
            .as_object_mut()
            .unwrap()
            .remove("execution_policy");
        assert!(serde_json::from_value::<WorkerLaunchConfig>(missing_policy).is_err());

        let mut legacy_policy = serde_json::to_value(&launch).unwrap();
        let legacy_policy_object = legacy_policy.as_object_mut().unwrap();
        legacy_policy_object.remove("execution_policy");
        legacy_policy_object.insert("force_unrestricted_mode".into(), serde_json::json!(true));
        let mut legacy_policy =
            serde_json::from_value::<WorkerLaunchConfig>(legacy_policy).unwrap();
        assert_eq!(
            legacy_policy.execution_policy,
            ExecutionPolicy::Unconstrained
        );
        assert!(!legacy_policy.environment.contains_key("INITIAL_AGENT_MODE"));
        legacy_policy.enforce_execution_policy();
        assert_eq!(
            legacy_policy
                .environment
                .get("INITIAL_AGENT_MODE")
                .map(String::as_str),
            Some("agent-full-access")
        );

        let mut legacy = serde_json::to_value(&launch).unwrap();
        for field in ["additional_directories", "native_session_id"] {
            legacy.as_object_mut().unwrap().remove(field);
        }
        let parsed = serde_json::from_value::<WorkerLaunchConfig>(legacy).unwrap();
        assert!(parsed.additional_directories.is_empty());
        assert!(parsed.native_session_id.is_none());
        assert_eq!(parsed.execution_policy, ExecutionPolicy::Unconstrained);

        let supervisor = AcpSupervisorSpec {
            command: "codex-acp".into(),
            args: Vec::new(),
            environment: BTreeMap::new(),
            cwd: ".".into(),
        };
        let mut incomplete = serde_json::to_value(&supervisor).unwrap();
        incomplete.as_object_mut().unwrap().remove("environment");
        assert!(serde_json::from_value::<AcpSupervisorSpec>(incomplete).is_err());
    }

    fn prompt(text: &str) -> RelayCommand {
        RelayCommand::Prompt {
            prompt: vec![ContentBlock::Text(TextContent::new(text))],
        }
    }

    fn submit(relay: &mut DurableRelay, command_id: &str, command: RelayCommand) {
        let response = relay.handle(RelayRequestEnvelope {
            request_id: format!("submit-{command_id}"),
            protocol_version: RELAY_PROTOCOL_VERSION,
            request: RelayRequest::Submit {
                command_id: command_id.into(),
                command,
            },
        });
        assert!(
            matches!(
                &response.body,
                RelayResponseBody::Ok {
                    payload: RelayResponsePayload::Accepted { .. }
                }
            ),
            "relay command was not accepted: {response:?}"
        );
    }

    /// An out-of-band ACP command: it shares the dispatch channel but never
    /// reaches durable relay state.
    fn compact_request() -> CommandRequest {
        let (response, _answer) = tokio::sync::oneshot::channel();
        CommandRequest::Compact {
            prompt: "out of band".into(),
            response,
        }
    }

    /// Whether a runtime warning reached durable state. A coordinator that
    /// parked on a command send stops draining runtime events, so this stays
    /// false forever once that happens.
    fn recorded_warning(relay: &Arc<Mutex<DurableRelay>>, message: &str) -> bool {
        relay
            .lock()
            .unwrap()
            .events_after(0, RELAY_EVENT_GENESIS_DIGEST)
            .unwrap()
            .iter()
            .any(|event| {
                matches!(
                    &event.observation,
                    RelayObservation::Warning { message: recorded } if recorded == message
                )
            })
    }

    async fn next_command(commands: &mut mpsc::Receiver<CommandRequest>) -> CommandRequest {
        tokio::time::timeout(std::time::Duration::from_secs(5), commands.recv())
            .await
            .expect("the relay coordinator stopped dispatching commands")
            .expect("the ACP command channel closed")
    }

    /// Wait for a coordinator-side condition. Everything this guards against
    /// wedges permanently, so a generous deadline still fails fast enough.
    async fn wait_until(mut condition: impl FnMut() -> bool, blocked: &str) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !condition() {
            assert!(std::time::Instant::now() < deadline, "{blocked}");
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }

    /// Line a test up with the coordinator without asserting anything: a
    /// condition that never holds must fail the test on what it broke rather
    /// than on the rendezvous.
    async fn wait_for_rendezvous(mut condition: impl FnMut() -> bool) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !condition() && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }

    fn assert_prompt(command: CommandRequest, expected_id: &str, expected_text: &str) {
        let CommandRequest::Prompt { request_id, prompt } = command else {
            panic!("expected ACP prompt command");
        };
        assert_eq!(request_id, expected_id);
        assert!(matches!(
            prompt.as_slice(),
            [ContentBlock::Text(text)] if text.text == expected_text
        ));
    }

    #[tokio::test]
    async fn offline_prompt_queue_runs_serially_without_a_controller() {
        let temp = tempfile::tempdir().unwrap();
        let mut durable = DurableRelay::open(temp.path(), SESSION_ID, "1.0.0").unwrap();
        submit(&mut durable, "prompt-1", prompt("first"));
        submit(&mut durable, "prompt-2", prompt("second"));
        let relay = Arc::new(Mutex::new(durable));
        let (event_tx, event_rx) = runtime_event_channel();
        let (wake_tx, wake_rx) = mpsc::channel(1);
        let (command_tx, mut command_rx) = mpsc::channel(4);
        let coordinator = tokio::spawn(unix::run_relay_coordinator(
            relay.clone(),
            event_rx,
            wake_rx,
            command_tx,
        ));
        event_tx
            .send(RuntimeEvent::SessionConfigured {
                config_options: Vec::new(),
            })
            .unwrap();

        let first = tokio::time::timeout(std::time::Duration::from_secs(1), command_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_prompt(first, "prompt-1", "first");
        event_tx
            .send(RuntimeEvent::PromptFinished {
                request_id: "prompt-1".into(),
                stop_reason: "end_turn".into(),
            })
            .unwrap();
        let second = tokio::time::timeout(std::time::Duration::from_secs(1), command_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_prompt(second, "prompt-2", "second");
        assert_eq!(
            relay
                .lock()
                .unwrap()
                .operational_state()
                .active_prompt
                .as_ref()
                .map(|prompt| prompt.command_id.as_str()),
            Some("prompt-2")
        );

        event_tx
            .send(RuntimeEvent::CommandRejected {
                request_id: "prompt-2".into(),
                message: "test shutdown".into(),
            })
            .unwrap();
        drop(event_tx);
        drop(wake_tx);
        coordinator.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn config_during_a_prompt_waits_but_cancel_dispatches_immediately() {
        let temp = tempfile::tempdir().unwrap();
        let mut durable = DurableRelay::open(temp.path(), SESSION_ID, "1.0.0").unwrap();
        submit(&mut durable, "active-prompt", prompt("running"));
        let relay = Arc::new(Mutex::new(durable));
        let (event_tx, event_rx) = runtime_event_channel();
        let (wake_tx, wake_rx) = mpsc::channel(1);
        let (command_tx, mut command_rx) = mpsc::channel(4);
        let coordinator = tokio::spawn(unix::run_relay_coordinator(
            relay.clone(),
            event_rx,
            wake_rx,
            command_tx,
        ));
        event_tx
            .send(RuntimeEvent::SessionConfigured {
                config_options: Vec::new(),
            })
            .unwrap();
        assert_prompt(command_rx.recv().await.unwrap(), "active-prompt", "running");

        submit(
            &mut relay.lock().unwrap(),
            "config-while-running",
            RelayCommand::SetConfig {
                key: "model".into(),
                value: "later".into(),
            },
        );
        submit(
            &mut relay.lock().unwrap(),
            "cancel-while-running",
            RelayCommand::Cancel,
        );
        wake_tx.try_send(()).unwrap();
        assert!(matches!(
            tokio::time::timeout(std::time::Duration::from_secs(1), command_rx.recv())
                .await
                .unwrap()
                .unwrap(),
            CommandRequest::Cancel { request_id } if request_id == "cancel-while-running"
        ));

        event_tx
            .send(RuntimeEvent::CancelApplied {
                request_id: "cancel-while-running".into(),
            })
            .unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(25), command_rx.recv())
                .await
                .is_err(),
            "configuration dispatched before the active prompt finished"
        );

        event_tx
            .send(RuntimeEvent::PromptFinished {
                request_id: "active-prompt".into(),
                stop_reason: "cancelled".into(),
            })
            .unwrap();
        assert!(matches!(
            tokio::time::timeout(std::time::Duration::from_secs(1), command_rx.recv())
                .await
                .unwrap()
                .unwrap(),
            CommandRequest::SetConfig { request_id, .. } if request_id == "config-while-running"
        ));
        event_tx
            .send(RuntimeEvent::CommandRejected {
                request_id: "config-while-running".into(),
                message: "test shutdown".into(),
            })
            .unwrap();
        event_tx.send(RuntimeEvent::Stopped).unwrap();
        drop(event_tx);
        drop(wake_tx);
        coordinator.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn proxy_transport_does_not_expire_an_idle_connection() {
        let (mut controller, proxy_client) = tokio::io::duplex(1024);
        let (proxy_relay, mut relay_peer) = tokio::io::duplex(1024);
        let (client_read, client_write) = tokio::io::split(proxy_client);
        let (relay_read, relay_write) = tokio::io::split(proxy_relay);
        let proxy = tokio::spawn(unix::forward_proxy_streams(
            client_read,
            client_write,
            relay_read,
            relay_write,
        ));

        controller.write_all(b"request").await.unwrap();
        let mut request = [0_u8; 7];
        relay_peer.read_exact(&mut request).await.unwrap();
        assert_eq!(&request, b"request");
        relay_peer.write_all(b"response").await.unwrap();
        let mut response = [0_u8; 8];
        controller.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"response");

        tokio::time::advance(std::time::Duration::from_secs(16 * 60)).await;
        tokio::task::yield_now().await;
        assert!(!proxy.is_finished(), "idle proxy connection expired");

        controller.write_all(b"another").await.unwrap();
        let mut request = [0_u8; 7];
        relay_peer.read_exact(&mut request).await.unwrap();
        assert_eq!(&request, b"another");
        relay_peer.write_all(b"answer!!").await.unwrap();
        let mut response = [0_u8; 8];
        controller.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"answer!!");

        drop(relay_peer);
        drop(controller);
        proxy.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn proxy_transport_expires_while_waiting_for_its_first_request() {
        let (_controller, proxy_client) = tokio::io::duplex(1024);
        let (proxy_relay, _relay_peer) = tokio::io::duplex(1024);
        let (client_read, client_write) = tokio::io::split(proxy_client);
        let (relay_read, relay_write) = tokio::io::split(proxy_relay);
        let proxy = tokio::spawn(unix::forward_proxy_streams(
            client_read,
            client_write,
            relay_read,
            relay_write,
        ));

        tokio::time::advance(unix::PROXY_INITIAL_INPUT_TIMEOUT).await;
        proxy
            .await
            .expect("proxy task stopped cleanly")
            .expect("pre-handshake proxy timeout is clean shutdown");
    }

    #[tokio::test]
    async fn prompt_dispatch_preserves_the_complete_acp_content_vector() {
        let temp = tempfile::tempdir().unwrap();
        let content = vec![
            ContentBlock::Text(TextContent::new("first block")),
            ContentBlock::Text(TextContent::new("second block")),
        ];
        let mut durable = DurableRelay::open(temp.path(), SESSION_ID, "1.0.0").unwrap();
        submit(
            &mut durable,
            "prompt-blocks",
            RelayCommand::Prompt {
                prompt: content.clone(),
            },
        );
        let relay = Arc::new(Mutex::new(durable));
        let (event_tx, event_rx) = runtime_event_channel();
        let (wake_tx, wake_rx) = mpsc::channel(1);
        let (command_tx, mut command_rx) = mpsc::channel(1);
        let coordinator = tokio::spawn(unix::run_relay_coordinator(
            relay, event_rx, wake_rx, command_tx,
        ));
        event_tx
            .send(RuntimeEvent::SessionConfigured {
                config_options: Vec::new(),
            })
            .unwrap();

        let CommandRequest::Prompt { request_id, prompt } = command_rx.recv().await.unwrap() else {
            panic!("expected ACP prompt command");
        };
        assert_eq!(request_id, "prompt-blocks");
        assert_eq!(prompt, content);

        event_tx
            .send(RuntimeEvent::CommandRejected {
                request_id: "prompt-blocks".into(),
                message: "test shutdown".into(),
            })
            .unwrap();
        drop(event_tx);
        drop(wake_tx);
        coordinator.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn same_priority_queue_entries_dispatch_in_acceptance_order() {
        let temp = tempfile::tempdir().unwrap();
        let mut durable = DurableRelay::open(temp.path(), SESSION_ID, "1.0.0").unwrap();
        submit(
            &mut durable,
            "z-accepted-first",
            RelayCommand::SetConfig {
                key: "model".into(),
                value: "first".into(),
            },
        );
        submit(
            &mut durable,
            "a-accepted-second",
            RelayCommand::SetConfig {
                key: "model".into(),
                value: "second".into(),
            },
        );
        let relay = Arc::new(Mutex::new(durable));
        let (event_tx, event_rx) = runtime_event_channel();
        let (wake_tx, wake_rx) = mpsc::channel(1);
        let (command_tx, mut command_rx) = mpsc::channel(2);
        let coordinator = tokio::spawn(unix::run_relay_coordinator(
            relay, event_rx, wake_rx, command_tx,
        ));
        event_tx
            .send(RuntimeEvent::SessionConfigured {
                config_options: Vec::new(),
            })
            .unwrap();

        // Queue entries run one at a time, so the second change reaches ACP
        // only after the first is terminal.
        assert!(matches!(
            command_rx.recv().await.unwrap(),
            CommandRequest::SetConfig { request_id, .. }
                if request_id == "z-accepted-first"
        ));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(25), command_rx.recv())
                .await
                .is_err(),
            "the queued change dispatched while an earlier one was in flight"
        );
        event_tx
            .send(RuntimeEvent::CommandRejected {
                request_id: "z-accepted-first".into(),
                message: "advance the queue".into(),
            })
            .unwrap();
        assert!(matches!(
            tokio::time::timeout(std::time::Duration::from_secs(1), command_rx.recv())
                .await
                .unwrap()
                .unwrap(),
            CommandRequest::SetConfig { request_id, .. }
                if request_id == "a-accepted-second"
        ));

        event_tx
            .send(RuntimeEvent::CommandRejected {
                request_id: "a-accepted-second".into(),
                message: "test shutdown".into(),
            })
            .unwrap();
        event_tx.send(RuntimeEvent::Stopped).unwrap();
        drop(event_tx);
        drop(wake_tx);
        coordinator.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn dispatch_batch_does_not_outgrow_the_bounded_acp_command_channel() {
        let temp = tempfile::tempdir().unwrap();
        let mut durable = DurableRelay::open(temp.path(), SESSION_ID, "1.0.0").unwrap();
        // A prompt and the cancel that targets it are both claimable at once,
        // so only the bounded channel limits the durable batch.
        submit(&mut durable, "prompt-first", prompt("running"));
        submit(&mut durable, "cancel-second", RelayCommand::Cancel);
        let relay = Arc::new(Mutex::new(durable));
        let (event_tx, event_rx) = runtime_event_channel();
        let (wake_tx, wake_rx) = mpsc::channel(1);
        let (command_tx, mut command_rx) = mpsc::channel(1);
        let coordinator = tokio::spawn(unix::run_relay_coordinator(
            relay, event_rx, wake_rx, command_tx,
        ));
        event_tx
            .send(RuntimeEvent::SessionConfigured {
                config_options: Vec::new(),
            })
            .unwrap();

        assert_prompt(command_rx.recv().await.unwrap(), "prompt-first", "running");
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(25), command_rx.recv(),)
                .await
                .is_err(),
            "the second command was claimed beyond the channel's durable dispatch capacity"
        );

        event_tx
            .send(RuntimeEvent::CommandRejected {
                request_id: "prompt-first".into(),
                message: "advance the bounded batch".into(),
            })
            .unwrap();
        assert!(matches!(
            tokio::time::timeout(std::time::Duration::from_secs(1), command_rx.recv())
                .await
                .unwrap()
                .unwrap(),
            CommandRequest::Cancel { request_id } if request_id == "cancel-second"
        ));
        event_tx
            .send(RuntimeEvent::CommandRejected {
                request_id: "cancel-second".into(),
                message: "test shutdown".into(),
            })
            .unwrap();
        event_tx.send(RuntimeEvent::Stopped).unwrap();
        drop(event_tx);
        drop(wake_tx);
        coordinator.await.unwrap().unwrap();
    }

    /// The ACP command channel is shared: compaction prompts and elicitation
    /// answers ride it beside dispatched commands. Dispatch therefore holds
    /// the transport capacity it claims against instead of counting free
    /// slots, because a coordinator parked on a command send stops draining
    /// ACP events, which stops the runtime that would have made room for the
    /// command it is waiting on.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn out_of_band_sends_cannot_park_the_dispatching_coordinator() {
        const COMMAND_CAPACITY: usize = 2;
        let temp = tempfile::tempdir().unwrap();
        let relay = Arc::new(Mutex::new(
            DurableRelay::open(temp.path(), SESSION_ID, "1.0.0").unwrap(),
        ));
        let (event_tx, event_rx) = runtime_event_channel();
        let (wake_tx, wake_rx) = mpsc::channel(1);
        let (command_tx, mut command_rx) = mpsc::channel(COMMAND_CAPACITY);
        let out_of_band = command_tx.clone();
        let coordinator = tokio::spawn(unix::run_relay_coordinator(
            relay.clone(),
            event_rx,
            wake_rx,
            command_tx,
        ));
        event_tx
            .send(RuntimeEvent::SessionConfigured {
                config_options: Vec::new(),
            })
            .unwrap();
        submit(
            &mut relay.lock().unwrap(),
            "prompt-warm-up",
            prompt("warm up"),
        );
        unix::wake_dispatch(&relay, &wake_tx).unwrap();
        assert_prompt(
            next_command(&mut command_rx).await,
            "prompt-warm-up",
            "warm up",
        );
        event_tx
            .send(RuntimeEvent::PromptFinished {
                request_id: "prompt-warm-up".into(),
                stop_reason: "end_turn".into(),
            })
            .unwrap();
        // Idle: the warm-up turn is durable and dispatch holds no capacity.
        wait_until(
            || {
                relay
                    .lock()
                    .unwrap()
                    .operational_state()
                    .active_prompt
                    .is_none()
                    && out_of_band.capacity() == COMMAND_CAPACITY
            },
            "the coordinator never finished the warm-up turn",
        )
        .await;

        // Hold the relay state lock to stop dispatch inside its claim: it has
        // already decided how much transport it may use, and nothing is
        // durable yet. That is the window an out-of-band send used to steal.
        let (claiming_tx, claiming_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let holder_relay = relay.clone();
        let holder = tokio::task::spawn_blocking(move || {
            let mut held = holder_relay.lock().expect("relay state lock poisoned");
            submit(&mut held, "prompt-batched", prompt("second turn"));
            submit(&mut held, "cancel-batched", RelayCommand::Cancel);
            claiming_tx
                .send(())
                .expect("the test stopped waiting for the claim");
            let _ = release_rx.blocking_recv();
        });
        claiming_rx.await.unwrap();
        // Wake dispatch by hand: the relay lock this test holds is exactly
        // what `wake_dispatch` would need to report a stopped coordinator.
        assert!(
            !matches!(
                wake_tx.try_send(()),
                Err(mpsc::error::TrySendError::Closed(()))
            ),
            "the relay coordinator stopped before the claim"
        );
        wait_for_rendezvous(|| out_of_band.capacity() == 0).await;

        // Out-of-band senders now compete for permits at reservation time.
        let out_of_band_attempts = [
            out_of_band.try_send(compact_request()),
            out_of_band.try_send(compact_request()),
        ];
        release_tx.send(()).unwrap();
        holder.await.unwrap();

        event_tx
            .send(RuntimeEvent::Warning {
                message: "still draining".into(),
            })
            .unwrap();
        wait_until(
            || recorded_warning(&relay, "still draining"),
            "an out-of-band send parked dispatch: the coordinator stopped draining ACP events",
        )
        .await;
        assert!(
            out_of_band_attempts
                .iter()
                .all(|attempt| matches!(attempt, Err(mpsc::error::TrySendError::Full(_)))),
            "dispatch must reserve transport capacity before it claims durable work"
        );
        assert_prompt(
            next_command(&mut command_rx).await,
            "prompt-batched",
            "second turn",
        );
        assert!(matches!(
            next_command(&mut command_rx).await,
            CommandRequest::Cancel { request_id } if request_id == "cancel-batched"
        ));

        event_tx.send(RuntimeEvent::Stopped).unwrap();
        drop(event_tx);
        drop(wake_tx);
        coordinator.await.unwrap().unwrap();
    }

    /// Capacity another sender already holds shrinks the durable batch instead
    /// of queueing behind it, and dispatch resumes once that sender's message
    /// is drained.
    #[tokio::test]
    async fn out_of_band_traffic_shrinks_the_durable_dispatch_batch() {
        let temp = tempfile::tempdir().unwrap();
        let mut durable = DurableRelay::open(temp.path(), SESSION_ID, "1.0.0").unwrap();
        // The prompt and the cancel that targets it are both claimable at
        // once, so only the transport limits the durable batch.
        submit(&mut durable, "prompt-first", prompt("running"));
        submit(&mut durable, "cancel-second", RelayCommand::Cancel);
        let relay = Arc::new(Mutex::new(durable));
        let (event_tx, event_rx) = runtime_event_channel();
        let (wake_tx, wake_rx) = mpsc::channel(1);
        let (command_tx, mut command_rx) = mpsc::channel(2);
        let out_of_band = command_tx.clone();
        // A compaction prompt occupies one of the two slots throughout.
        out_of_band.try_send(compact_request()).unwrap();
        let coordinator = tokio::spawn(unix::run_relay_coordinator(
            relay.clone(),
            event_rx,
            wake_rx,
            command_tx,
        ));
        event_tx
            .send(RuntimeEvent::SessionConfigured {
                config_options: Vec::new(),
            })
            .unwrap();

        wait_until(
            || {
                relay
                    .lock()
                    .unwrap()
                    .operational_state()
                    .active_prompt
                    .is_some()
            },
            "the queued prompt never reached the ACP runtime",
        )
        .await;
        // The coordinator keeps draining events with the cancel undispatched,
        // and only the prompt joined the compaction message on the transport.
        event_tx
            .send(RuntimeEvent::Warning {
                message: "still draining".into(),
            })
            .unwrap();
        wait_until(
            || recorded_warning(&relay, "still draining"),
            "dispatch parked on a transport slot the compaction prompt already held",
        )
        .await;
        assert_eq!(
            command_rx.len(),
            2,
            "the cancel was claimed beyond the capacity dispatch reserved"
        );

        assert!(matches!(
            next_command(&mut command_rx).await,
            CommandRequest::Compact { .. }
        ));
        assert_prompt(
            next_command(&mut command_rx).await,
            "prompt-first",
            "running",
        );
        // The compaction message is drained, so the retried batch fits.
        unix::wake_dispatch(&relay, &wake_tx).unwrap();
        assert!(matches!(
            next_command(&mut command_rx).await,
            CommandRequest::Cancel { request_id } if request_id == "cancel-second"
        ));

        event_tx.send(RuntimeEvent::Stopped).unwrap();
        drop(event_tx);
        drop(wake_tx);
        coordinator.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn different_command_types_dispatch_in_acceptance_order() {
        let temp = tempfile::tempdir().unwrap();
        let mut durable = DurableRelay::open(temp.path(), SESSION_ID, "1.0.0").unwrap();
        submit(
            &mut durable,
            "config-first",
            RelayCommand::SetConfig {
                key: "model".into(),
                value: "before-prompt".into(),
            },
        );
        submit(&mut durable, "prompt-second", prompt("after config"));
        let relay = Arc::new(Mutex::new(durable));
        let (event_tx, event_rx) = runtime_event_channel();
        let (wake_tx, wake_rx) = mpsc::channel(1);
        let (command_tx, mut command_rx) = mpsc::channel(2);
        let coordinator = tokio::spawn(unix::run_relay_coordinator(
            relay, event_rx, wake_rx, command_tx,
        ));
        event_tx
            .send(RuntimeEvent::SessionConfigured {
                config_options: Vec::new(),
            })
            .unwrap();

        assert!(matches!(
            command_rx.recv().await.unwrap(),
            CommandRequest::SetConfig { request_id, .. } if request_id == "config-first"
        ));
        // The prompt waits for the configuration change accepted before it.
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(25), command_rx.recv())
                .await
                .is_err(),
            "the prompt dispatched before the earlier configuration change finished"
        );
        event_tx
            .send(RuntimeEvent::ConfigApplied {
                request_id: "config-first".into(),
                key: "model".into(),
                value: "before-prompt".into(),
                config_options: Vec::new(),
            })
            .unwrap();
        assert_prompt(
            tokio::time::timeout(std::time::Duration::from_secs(1), command_rx.recv())
                .await
                .unwrap()
                .unwrap(),
            "prompt-second",
            "after config",
        );

        event_tx
            .send(RuntimeEvent::CommandRejected {
                request_id: "prompt-second".into(),
                message: "test shutdown".into(),
            })
            .unwrap();
        event_tx.send(RuntimeEvent::Stopped).unwrap();
        drop(event_tx);
        drop(wake_tx);
        coordinator.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn rejected_prompt_is_durable_and_does_not_stall_the_queue() {
        let temp = tempfile::tempdir().unwrap();
        let mut durable = DurableRelay::open(temp.path(), SESSION_ID, "1.0.0").unwrap();
        submit(&mut durable, "prompt-1", prompt("first"));
        submit(&mut durable, "prompt-2", prompt("second"));
        let relay = Arc::new(Mutex::new(durable));
        let (event_tx, event_rx) = runtime_event_channel();
        let (wake_tx, wake_rx) = mpsc::channel(1);
        let (command_tx, mut command_rx) = mpsc::channel(4);
        let coordinator = tokio::spawn(unix::run_relay_coordinator(
            relay.clone(),
            event_rx,
            wake_rx,
            command_tx,
        ));
        event_tx
            .send(RuntimeEvent::SessionConfigured {
                config_options: Vec::new(),
            })
            .unwrap();

        assert_prompt(command_rx.recv().await.unwrap(), "prompt-1", "first");
        event_tx
            .send(RuntimeEvent::CommandRejected {
                request_id: "prompt-1".into(),
                message: "agent rejected prompt".into(),
            })
            .unwrap();
        assert_prompt(command_rx.recv().await.unwrap(), "prompt-2", "second");
        let observations = relay
            .lock()
            .unwrap()
            .events_after(0, RELAY_EVENT_GENESIS_DIGEST)
            .unwrap();
        assert!(observations.iter().any(|event| matches!(
            &event.observation,
            RelayObservation::CommandRejected {
                command_id,
                message,
                ..
            }
                if command_id == "prompt-1" && message == "agent rejected prompt"
        )));

        event_tx
            .send(RuntimeEvent::CommandRejected {
                request_id: "prompt-2".into(),
                message: "test shutdown".into(),
            })
            .unwrap();
        drop(event_tx);
        drop(wake_tx);
        coordinator.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn set_session_mode_waits_for_idle_then_records_a_durable_outcome() {
        let temp = tempfile::tempdir().unwrap();
        let relay = Arc::new(Mutex::new(
            DurableRelay::open(temp.path(), SESSION_ID, "1.0.0").unwrap(),
        ));
        let (event_tx, event_rx) = runtime_event_channel();
        let (wake_tx, wake_rx) = mpsc::channel(1);
        let (command_tx, mut command_rx) = mpsc::channel(4);
        let coordinator = tokio::spawn(unix::run_relay_coordinator(
            relay.clone(),
            event_rx,
            wake_rx,
            command_tx,
        ));
        event_tx
            .send(RuntimeEvent::SessionConfigured {
                config_options: Vec::new(),
            })
            .unwrap();

        submit(&mut relay.lock().unwrap(), "prompt-1", prompt("running"));
        wake_tx.try_send(()).unwrap();
        assert_prompt(command_rx.recv().await.unwrap(), "prompt-1", "running");

        submit(
            &mut relay.lock().unwrap(),
            "session-mode-1",
            RelayCommand::SetSessionMode {
                mode_id: "plan".into(),
            },
        );
        wake_tx.try_send(()).unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(25), command_rx.recv())
                .await
                .is_err(),
            "the session mode dispatched before the active prompt finished"
        );

        event_tx
            .send(RuntimeEvent::PromptFinished {
                request_id: "prompt-1".into(),
                stop_reason: "end_turn".into(),
            })
            .unwrap();
        assert!(matches!(
            tokio::time::timeout(std::time::Duration::from_secs(1), command_rx.recv())
                .await
                .unwrap()
                .unwrap(),
            CommandRequest::SetSessionMode { request_id, mode_id }
                if request_id == "session-mode-1" && mode_id == "plan"
        ));
        event_tx
            .send(RuntimeEvent::SessionModeApplied {
                request_id: "session-mode-1".into(),
                mode_id: "plan".into(),
                config_options: Vec::new(),
                modes: None,
            })
            .unwrap();
        wait_for_relay_state(&relay, |state| {
            state.config.get("mode").map(String::as_str) == Some("plan")
        })
        .await;

        event_tx.send(RuntimeEvent::Stopped).unwrap();
        drop(event_tx);
        drop(wake_tx);
        coordinator.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_rejected_session_mode_change_reports_the_failure_and_leaves_the_mode_alone() {
        let temp = tempfile::tempdir().unwrap();
        let relay = Arc::new(Mutex::new(
            DurableRelay::open(temp.path(), SESSION_ID, "1.0.0").unwrap(),
        ));
        let (event_tx, event_rx) = runtime_event_channel();
        let (wake_tx, wake_rx) = mpsc::channel(1);
        let (command_tx, mut command_rx) = mpsc::channel(4);
        let coordinator = tokio::spawn(unix::run_relay_coordinator(
            relay.clone(),
            event_rx,
            wake_rx,
            command_tx,
        ));
        event_tx
            .send(RuntimeEvent::SessionConfigured {
                config_options: Vec::new(),
            })
            .unwrap();

        submit(
            &mut relay.lock().unwrap(),
            "session-mode-1",
            RelayCommand::SetSessionMode {
                mode_id: "plan".into(),
            },
        );
        wake_tx.try_send(()).unwrap();
        assert!(matches!(
            command_rx.recv().await.unwrap(),
            CommandRequest::SetSessionMode { request_id, mode_id }
                if request_id == "session-mode-1" && mode_id == "plan"
        ));
        event_tx
            .send(RuntimeEvent::CommandRejected {
                request_id: "session-mode-1".into(),
                message: "set session mode to plan: no such mode".into(),
            })
            .unwrap();
        wait_for_relay_state(&relay, |state| !state.config.contains_key("mode")).await;

        event_tx.send(RuntimeEvent::Stopped).unwrap();
        drop(event_tx);
        drop(wake_tx);
        coordinator.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn config_cancel_and_close_commands_have_durable_terminal_outcomes() {
        let temp = tempfile::tempdir().unwrap();
        let relay = Arc::new(Mutex::new(
            DurableRelay::open(temp.path(), SESSION_ID, "1.0.0").unwrap(),
        ));
        let (event_tx, event_rx) = runtime_event_channel();
        let (wake_tx, wake_rx) = mpsc::channel(1);
        let (command_tx, mut command_rx) = mpsc::channel(4);
        let coordinator = tokio::spawn(unix::run_relay_coordinator(
            relay.clone(),
            event_rx,
            wake_rx,
            command_tx,
        ));
        event_tx
            .send(RuntimeEvent::SessionConfigured {
                config_options: Vec::new(),
            })
            .unwrap();

        submit(
            &mut relay.lock().unwrap(),
            "config-1",
            RelayCommand::SetConfig {
                key: "model".into(),
                value: "test-model".into(),
            },
        );
        wake_tx.try_send(()).unwrap();
        assert!(matches!(
            command_rx.recv().await.unwrap(),
            CommandRequest::SetConfig { request_id, key, value }
                if request_id == "config-1" && key == "model" && value == "test-model"
        ));
        event_tx
            .send(RuntimeEvent::ConfigApplied {
                request_id: "config-1".into(),
                key: "model".into(),
                value: "test-model".into(),
                config_options: Vec::new(),
            })
            .unwrap();
        wait_for_relay_state(&relay, |state| {
            state.config.get("model").map(String::as_str) == Some("test-model")
        })
        .await;

        submit(&mut relay.lock().unwrap(), "prompt-1", prompt("running"));
        wake_tx.try_send(()).unwrap();
        assert_prompt(command_rx.recv().await.unwrap(), "prompt-1", "running");
        submit(&mut relay.lock().unwrap(), "cancel-1", RelayCommand::Cancel);
        wake_tx.try_send(()).unwrap();
        assert!(matches!(
            command_rx.recv().await.unwrap(),
            CommandRequest::Cancel { request_id } if request_id == "cancel-1"
        ));
        event_tx
            .send(RuntimeEvent::CancelApplied {
                request_id: "cancel-1".into(),
            })
            .unwrap();
        event_tx
            .send(RuntimeEvent::PromptFinished {
                request_id: "prompt-1".into(),
                stop_reason: "cancelled".into(),
            })
            .unwrap();
        wait_for_relay_state(&relay, |state| {
            state.execution == RelayExecutionState::Idle && state.active_prompt.is_none()
        })
        .await;

        submit(
            &mut relay.lock().unwrap(),
            "barrier-before-close",
            RelayCommand::BeginCheckpoint {
                reason: Some("close test".into()),
            },
        );
        wake_tx.try_send(()).unwrap();
        wait_for_relay_state(&relay, |state| state.checkpoint_ready.is_some()).await;
        let expected = relay
            .lock()
            .unwrap()
            .operational_state()
            .checkpoint_ready
            .unwrap();
        submit(
            &mut relay.lock().unwrap(),
            "close-01",
            RelayCommand::Close {
                barrier_command_id: "barrier-before-close".into(),
                expected,
            },
        );
        submit(
            &mut relay.lock().unwrap(),
            "complete-before-close",
            RelayCommand::CompleteCheckpoint {
                barrier_command_id: "barrier-before-close".into(),
            },
        );
        wake_tx.try_send(()).unwrap();
        assert!(matches!(
            command_rx.recv().await.unwrap(),
            CommandRequest::Close { request_id } if request_id == "close-01"
        ));
        event_tx
            .send(RuntimeEvent::CloseApplied {
                request_id: "close-01".into(),
            })
            .unwrap();
        wait_for_relay_state(&relay, |state| {
            state.execution == RelayExecutionState::Closed
        })
        .await;

        event_tx.send(RuntimeEvent::Stopped).unwrap();
        drop(event_tx);
        drop(wake_tx);
        coordinator.await.unwrap().unwrap();
        let observations = relay
            .lock()
            .unwrap()
            .events_after(0, RELAY_EVENT_GENESIS_DIGEST)
            .unwrap();
        assert!(observations.iter().any(|event| matches!(
            &event.observation,
            RelayObservation::CommandCompleted {
                command_id,
                outcome: crate::hel_worker::RelayCommandOutcome::Cancelled,
            } if command_id == "cancel-1"
        )));
        assert!(
            observations
                .iter()
                .any(|event| matches!(&event.observation, RelayObservation::Closed))
        );
    }

    async fn wait_for_relay_state(
        relay: &Arc<Mutex<DurableRelay>>,
        predicate: impl Fn(&crate::hel_worker::RelayOperationalState) -> bool,
    ) {
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if predicate(&relay.lock().unwrap().operational_state()) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("relay state did not reach the expected condition");
    }

    #[test]
    fn typed_acp_observations_are_journaled() {
        let temp = tempfile::tempdir().unwrap();
        let relay = Arc::new(Mutex::new(
            DurableRelay::open(temp.path(), SESSION_ID, "1.0.0").unwrap(),
        ));
        let mut in_flight = BTreeMap::new();
        unix::record_runtime_event(
            &relay,
            &mut in_flight,
            RuntimeEvent::Connected {
                agent_name: Some("test-agent".into()),
                agent_version: Some("1".into()),
                protocol_version: Some(ProtocolVersion::V1),
                capabilities: Some(Box::new(AgentCapabilities::default())),
                agent_info: Some(Implementation::new("test-agent", "1")),
            },
        )
        .unwrap();
        let update = SessionUpdate::AgentMessageChunk(
            ContentChunk::new(ContentBlock::Text(TextContent::new("hello")))
                .message_id("message-1"),
        );
        unix::record_runtime_event(
            &relay,
            &mut in_flight,
            RuntimeEvent::SessionUpdate {
                update: serde_json::to_value(update).unwrap(),
            },
        )
        .unwrap();

        let events = relay
            .lock()
            .unwrap()
            .events_after(0, RELAY_EVENT_GENESIS_DIGEST)
            .unwrap();
        assert!(
            events.iter().any(|event| matches!(
                event.observation,
                RelayObservation::AgentInitialized { .. }
            ))
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event.observation, RelayObservation::SessionUpdate { .. }))
        );
    }

    #[test]
    fn harness_restarting_interrupts_in_flight_commands() {
        let temp = tempfile::tempdir().unwrap();
        let mut durable = DurableRelay::open(temp.path(), SESSION_ID, "1.0.0").unwrap();
        submit(&mut durable, "prompt-1", prompt("go"));
        let relay = Arc::new(Mutex::new(durable));
        let mut in_flight = BTreeMap::new();
        in_flight.insert("prompt-1".into(), prompt("go"));

        unix::record_runtime_event(
            &relay,
            &mut in_flight,
            RuntimeEvent::HarnessRestarting {
                message: "ACP bridge exited; reloading the native session".into(),
            },
        )
        .unwrap();

        assert!(
            in_flight.is_empty(),
            "the in-flight prompt must be interrupted"
        );
        let events = relay
            .lock()
            .unwrap()
            .events_after(0, RELAY_EVENT_GENESIS_DIGEST)
            .unwrap();
        assert!(events.iter().any(|event| matches!(
            &event.observation,
            RelayObservation::Warning { message } if message.contains("reloading the native session")
        )));
        assert!(events.iter().any(|event| matches!(
            &event.observation,
            RelayObservation::CommandInterrupted { command_id, .. } if command_id == "prompt-1"
        )));
    }

    #[test]
    fn terminal_close_journals_a_tail_capped_terminal_output_observation() {
        let temp = tempfile::tempdir().unwrap();
        let relay = Arc::new(Mutex::new(
            DurableRelay::open(temp.path(), SESSION_ID, "1.0.0").unwrap(),
        ));
        let mut in_flight = BTreeMap::new();

        let ordinal_before_start = relay.lock().unwrap().operational_state().latest_ordinal;
        unix::record_runtime_event(
            &relay,
            &mut in_flight,
            RuntimeEvent::TerminalStarted {
                terminal_id: "term-1".into(),
                command: "cargo test".into(),
                started_at_ms: 1_000,
            },
        )
        .unwrap();
        let operational = relay.lock().unwrap().operational_state();
        assert_eq!(operational.latest_ordinal, ordinal_before_start);
        assert_eq!(
            operational.active_agent_terminals,
            [crate::hel_worker::ActiveAgentTerminal {
                terminal_id: "term-1".into(),
                command: "cargo test".into(),
                started_at_ms: 1_000,
            }],
            "starting a terminal is visible but never journaled"
        );

        // A build log the size of a real one: far past both the pipe buffer and
        // the journal cap, so only the tail can survive.
        let mut output = String::from("first line of the build log\n");
        while output.len() < 512 * 1024 {
            output.push_str("compiling something that says nothing useful\n");
        }
        output.push_str("error: the last line is the one that matters\n");
        let produced = output.len();

        unix::record_runtime_event(
            &relay,
            &mut in_flight,
            RuntimeEvent::TerminalClosed {
                terminal_id: "term-1".into(),
                output,
                truncated: false,
                exit_code: Some(101),
                signal: None,
            },
        )
        .unwrap();

        assert!(
            relay
                .lock()
                .unwrap()
                .operational_state()
                .active_agent_terminals
                .is_empty(),
            "the provisional activity disappears as soon as the child exits"
        );

        let events = relay
            .lock()
            .unwrap()
            .events_after(0, RELAY_EVENT_GENESIS_DIGEST)
            .unwrap();
        let event = events
            .iter()
            .find(|event| matches!(event.observation, RelayObservation::TerminalOutput { .. }))
            .expect("the closed terminal is journaled");
        let RelayObservation::TerminalOutput {
            terminal_id,
            output,
            truncated,
            exit_code,
            signal,
        } = &event.observation
        else {
            unreachable!("matched a terminal observation above");
        };

        assert_eq!(terminal_id, "term-1");
        assert_eq!(*exit_code, Some(101));
        assert_eq!(*signal, None);
        assert!(*truncated, "dropping the head must be disclosed");
        assert!(
            output.ends_with("error: the last line is the one that matters\n"),
            "the tail of the output is what says how the command ended"
        );
        assert!(
            !output.contains("first line of the build log"),
            "the head is what gets dropped, not the tail"
        );
        assert!(output.contains("[hel dropped"), "the drop is disclosed");
        assert!(
            output.len() < produced,
            "the journal copy is capped below what the terminal produced"
        );
        assert!(
            serde_json::to_vec(event).unwrap().len() <= crate::hel_worker::RELAY_EVENT_BYTE_BUDGET,
            "the capped event fits a replay page without further clamping"
        );
    }

    #[test]
    fn a_fast_terminal_cannot_be_resurrected_by_a_late_start_event() {
        let temp = tempfile::tempdir().unwrap();
        let relay = Arc::new(Mutex::new(
            DurableRelay::open(temp.path(), SESSION_ID, "1.0.0").unwrap(),
        ));
        let mut in_flight = BTreeMap::new();

        unix::record_runtime_event(
            &relay,
            &mut in_flight,
            RuntimeEvent::TerminalClosed {
                terminal_id: "term-1".into(),
                output: String::new(),
                truncated: false,
                exit_code: Some(0),
                signal: None,
            },
        )
        .unwrap();
        unix::record_runtime_event(
            &relay,
            &mut in_flight,
            RuntimeEvent::TerminalStarted {
                terminal_id: "term-1".into(),
                command: "true".into(),
                started_at_ms: 1_000,
            },
        )
        .unwrap();

        assert!(
            relay
                .lock()
                .unwrap()
                .operational_state()
                .active_agent_terminals
                .is_empty()
        );
    }

    #[tokio::test]
    async fn checkpoint_waits_for_current_session_configuration_then_stays_local() {
        let temp = tempfile::tempdir().unwrap();
        let mut durable = DurableRelay::open(temp.path(), SESSION_ID, "1.0.0").unwrap();
        submit(
            &mut durable,
            "checkpoint-1",
            RelayCommand::BeginCheckpoint {
                reason: Some("test".into()),
            },
        );
        let relay = Arc::new(Mutex::new(durable));
        let (event_tx, event_rx) = runtime_event_channel();
        let (wake_tx, wake_rx) = mpsc::channel(1);
        let (command_tx, mut command_rx) = mpsc::channel(1);
        let coordinator = tokio::spawn(unix::run_relay_coordinator(
            relay.clone(),
            event_rx,
            wake_rx,
            command_tx,
        ));

        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        assert!(
            relay
                .lock()
                .unwrap()
                .operational_state()
                .checkpoint_barrier
                .is_none(),
            "checkpoint became ready before the current ACP session was configured"
        );
        event_tx
            .send(RuntimeEvent::SessionConfigured {
                config_options: Vec::new(),
            })
            .unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if relay
                    .lock()
                    .unwrap()
                    .operational_state()
                    .checkpoint_barrier
                    .as_deref()
                    == Some("checkpoint-1")
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(25), command_rx.recv())
                .await
                .is_err()
        );
        drop(event_tx);
        drop(wake_tx);
        coordinator.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn checkpoint_waits_for_an_in_flight_config_command() {
        let temp = tempfile::tempdir().unwrap();
        let mut durable = DurableRelay::open(temp.path(), SESSION_ID, "1.0.0").unwrap();
        submit(
            &mut durable,
            "config-before",
            RelayCommand::SetConfig {
                key: "model".into(),
                value: "test-model".into(),
            },
        );
        submit(
            &mut durable,
            "barrier-after-config",
            RelayCommand::BeginCheckpoint {
                reason: Some("after config".into()),
            },
        );
        let relay = Arc::new(Mutex::new(durable));
        let (event_tx, event_rx) = runtime_event_channel();
        let (wake_tx, wake_rx) = mpsc::channel(1);
        let (command_tx, mut command_rx) = mpsc::channel(2);
        let coordinator = tokio::spawn(unix::run_relay_coordinator(
            relay.clone(),
            event_rx,
            wake_rx,
            command_tx,
        ));

        event_tx
            .send(RuntimeEvent::SessionConfigured {
                config_options: Vec::new(),
            })
            .unwrap();
        assert!(matches!(
            command_rx.recv().await.unwrap(),
            CommandRequest::SetConfig { request_id, .. } if request_id == "config-before"
        ));
        assert!(
            relay
                .lock()
                .unwrap()
                .operational_state()
                .checkpoint_barrier
                .is_none(),
            "checkpoint became ready before the ACP config command completed"
        );
        event_tx
            .send(RuntimeEvent::ConfigApplied {
                request_id: "config-before".into(),
                key: "model".into(),
                value: "test-model".into(),
                config_options: Vec::new(),
            })
            .unwrap();
        // ConfigApplied carries the ACP response's complete configuration.
        // The coordinator must durably materialize its SessionConfigured
        // observation before admitting the waiting checkpoint.
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let (state, configured_events) = {
                    let relay = relay.lock().unwrap();
                    let state = relay.operational_state();
                    let configured_events = relay
                        .events_after(0, RELAY_EVENT_GENESIS_DIGEST)
                        .unwrap()
                        .iter()
                        .filter(|event| {
                            matches!(
                                event.observation,
                                RelayObservation::SessionConfigured { .. }
                            )
                        })
                        .count();
                    (state, configured_events)
                };
                if state.checkpoint_barrier.as_deref() == Some("barrier-after-config")
                    && configured_events == 2
                {
                    let ready = state.checkpoint_ready.expect("checkpoint is ready");
                    assert_eq!(ready.ordinal, state.latest_ordinal);
                    assert_eq!(ready.digest, state.latest_digest);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        relay
            .lock()
            .unwrap()
            .cancel_checkpoint_barrier_on_disconnect("barrier-after-config")
            .unwrap();
        event_tx.send(RuntimeEvent::Stopped).unwrap();
        drop(event_tx);
        drop(wake_tx);
        coordinator.await.unwrap().unwrap();
    }

    /// A checkpoint hands ACP dispatch back as soon as its archive exists, then
    /// keeps using the same connection for the transfer. When that connection
    /// finally drops, the released barrier is already terminal: cancelling it
    /// again would push a spurious interruption into the transcript.
    #[tokio::test]
    async fn a_released_checkpoint_barrier_is_not_cancelled_when_its_connection_drops() {
        let temp = tempfile::tempdir().unwrap();
        let mut durable = DurableRelay::open(temp.path(), SESSION_ID, "1.0.0").unwrap();
        submit(
            &mut durable,
            "released-barrier",
            RelayCommand::BeginCheckpoint {
                reason: Some("early release".into()),
            },
        );
        submit(
            &mut durable,
            "queued-during-transfer",
            RelayCommand::Prompt {
                prompt: vec![ContentBlock::Text(TextContent::new("later"))],
            },
        );
        assert_eq!(durable.claim_pending_commands(true).unwrap().len(), 1);
        durable.record_checkpoint_ready("released-barrier").unwrap();
        let floor_before = durable.operational_state().recovery_floor_ordinal;
        let relay = Arc::new(Mutex::new(durable));

        let (mut client, server) = tokio::net::UnixStream::pair().unwrap();
        let (wake_tx, _wake_rx) = mpsc::channel(1);
        let served = tokio::spawn(unix::serve_client(
            server,
            relay.clone(),
            wake_tx,
            test_credentials(),
            None,
            fatal_reports().0,
        ));
        let request = RelayRequestEnvelope {
            request_id: "release-request".into(),
            protocol_version: RELAY_PROTOCOL_VERSION,
            request: RelayRequest::Submit {
                command_id: "release-command".into(),
                command: RelayCommand::ReleaseCheckpoint {
                    barrier_command_id: "released-barrier".into(),
                },
            },
        };
        let mut encoded = serde_json::to_vec(&request).unwrap();
        encoded.push(b'\n');
        client.write_all(&encoded).await.unwrap();
        let response = BufReader::new(&mut client)
            .lines()
            .next_line()
            .await
            .unwrap()
            .unwrap();
        let response: RelayResponseEnvelope = serde_json::from_str(&response).unwrap();
        assert!(
            matches!(
                &response.body,
                RelayResponseBody::Ok {
                    payload: RelayResponsePayload::Accepted { command_id, .. }
                } if command_id == "release-command"
            ),
            "relay did not accept the release: {:?}",
            response.body
        );

        // Dropping the connection is exactly what the worker treats as the
        // controller disappearing.
        drop(client);
        served.await.unwrap().unwrap();

        let mut relay = relay.lock().unwrap();
        let state = relay.operational_state();
        assert!(state.checkpoint_barrier.is_none());
        assert_eq!(state.recovery_floor_ordinal, floor_before);
        assert!(
            !relay
                .events_after(0, RELAY_EVENT_GENESIS_DIGEST)
                .unwrap()
                .iter()
                .any(|event| matches!(
                    &event.observation,
                    RelayObservation::CommandInterrupted { command_id, .. }
                        if command_id == "released-barrier"
                )),
            "the dropped connection cancelled a barrier it had already released"
        );
        let next = relay.claim_pending_commands(true).unwrap();
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].command_id, "queued-during-transfer");
    }

    #[tokio::test]
    async fn checkpoint_wake_records_already_queued_runtime_events_first() {
        let temp = tempfile::tempdir().unwrap();
        let relay = Arc::new(Mutex::new(
            DurableRelay::open(temp.path(), SESSION_ID, "1.0.0").unwrap(),
        ));
        let (event_tx, event_rx) = runtime_event_channel();
        let (wake_tx, wake_rx) = mpsc::channel(1);
        let (command_tx, _command_rx) = mpsc::channel(1);
        let coordinator = tokio::spawn(unix::run_relay_coordinator(
            relay.clone(),
            event_rx,
            wake_rx,
            command_tx,
        ));
        event_tx
            .send(RuntimeEvent::SessionConfigured {
                config_options: Vec::new(),
            })
            .unwrap();
        wait_for_relay_state(&relay, |state| state.latest_ordinal >= 1).await;

        event_tx
            .send(RuntimeEvent::Warning {
                message: "queued before checkpoint wake".into(),
            })
            .unwrap();
        submit(
            &mut relay.lock().unwrap(),
            "checkpoint-after-queued-event",
            RelayCommand::BeginCheckpoint {
                reason: Some("ordering test".into()),
            },
        );
        wake_tx.try_send(()).unwrap();
        wait_for_relay_state(&relay, |state| state.checkpoint_ready.is_some()).await;

        {
            let relay_state = relay.lock().unwrap();
            let state = relay_state.operational_state();
            let ready = state.checkpoint_ready.expect("checkpoint is ready");
            assert_eq!(ready.ordinal, state.latest_ordinal);
            assert_eq!(ready.digest, state.latest_digest);
            let events = relay_state
                .events_after(0, RELAY_EVENT_GENESIS_DIGEST)
                .unwrap();
            let warning_ordinal = events
                .iter()
                .find_map(|event| match &event.observation {
                    RelayObservation::Warning { message }
                        if message == "queued before checkpoint wake" =>
                    {
                        Some(event.ordinal)
                    }
                    _ => None,
                })
                .expect("queued warning was recorded");
            assert!(warning_ordinal < ready.ordinal);
        }

        relay
            .lock()
            .unwrap()
            .cancel_checkpoint_barrier_on_disconnect("checkpoint-after-queued-event")
            .unwrap();
        event_tx.send(RuntimeEvent::Stopped).unwrap();
        drop(event_tx);
        drop(wake_tx);
        coordinator.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn checkpoint_wake_is_not_starved_by_a_runtime_event_flood() {
        let temp = tempfile::tempdir().unwrap();
        let mut durable = DurableRelay::open(temp.path(), SESSION_ID, "1.0.0").unwrap();
        submit(
            &mut durable,
            "checkpoint-during-event-flood",
            RelayCommand::BeginCheckpoint {
                reason: Some("event flood fairness".into()),
            },
        );
        let relay = Arc::new(Mutex::new(durable));
        let (event_tx, event_rx) = mpsc::channel(8);
        event_tx
            .try_send(RuntimeEvent::SessionConfigured {
                config_options: Vec::new(),
            })
            .unwrap();
        for sequence in 0..7 {
            event_tx
                .try_send(RuntimeEvent::Warning {
                    message: format!("queued flood event {sequence}"),
                })
                .unwrap();
        }
        let (wake_tx, wake_rx) = mpsc::channel(1);
        wake_tx.try_send(()).unwrap();
        let (command_tx, _command_rx) = mpsc::channel(1);
        let flood_tx = event_tx.clone();
        let flood = tokio::spawn(async move {
            let mut sequence = 7_u64;
            loop {
                if flood_tx
                    .send(RuntimeEvent::Warning {
                        message: format!("live flood event {sequence}"),
                    })
                    .await
                    .is_err()
                {
                    return;
                }
                sequence += 1;
            }
        });
        let coordinator = tokio::spawn(unix::run_relay_coordinator(
            relay.clone(),
            event_rx,
            wake_rx,
            command_tx,
        ));

        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if relay
                    .lock()
                    .unwrap()
                    .operational_state()
                    .checkpoint_ready
                    .is_some()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("runtime event flood starved the checkpoint wake");

        flood.abort();
        let _ = flood.await;
        relay
            .lock()
            .unwrap()
            .cancel_checkpoint_barrier_on_disconnect("checkpoint-during-event-flood")
            .unwrap();
        event_tx.send(RuntimeEvent::Stopped).await.unwrap();
        drop(event_tx);
        drop(wake_tx);
        coordinator.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn checkpoint_freezes_effectful_commands_submitted_after_the_barrier() {
        let temp = tempfile::tempdir().unwrap();
        let mut durable = DurableRelay::open(temp.path(), SESSION_ID, "1.0.0").unwrap();
        submit(
            &mut durable,
            "barrier-before-config",
            RelayCommand::BeginCheckpoint {
                reason: Some("freeze later work".into()),
            },
        );
        submit(
            &mut durable,
            "config-after",
            RelayCommand::SetConfig {
                key: "model".into(),
                value: "later-model".into(),
            },
        );
        let relay = Arc::new(Mutex::new(durable));
        let (event_tx, event_rx) = runtime_event_channel();
        let (wake_tx, wake_rx) = mpsc::channel(1);
        let (command_tx, mut command_rx) = mpsc::channel(2);
        let coordinator = tokio::spawn(unix::run_relay_coordinator(
            relay.clone(),
            event_rx,
            wake_rx,
            command_tx,
        ));

        event_tx
            .send(RuntimeEvent::SessionConfigured {
                config_options: Vec::new(),
            })
            .unwrap();
        wait_for_relay_state(&relay, |state| {
            state.checkpoint_barrier.as_deref() == Some("barrier-before-config")
        })
        .await;
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(25), command_rx.recv())
                .await
                .is_err(),
            "a post-barrier ACP command was dispatched before checkpoint completion"
        );

        submit(
            &mut relay.lock().unwrap(),
            "complete-checkpoint",
            RelayCommand::CompleteCheckpoint {
                barrier_command_id: "barrier-before-config".into(),
            },
        );
        wake_tx.try_send(()).unwrap();
        assert!(matches!(
            command_rx.recv().await.unwrap(),
            CommandRequest::SetConfig { request_id, .. } if request_id == "config-after"
        ));

        event_tx
            .send(RuntimeEvent::CommandRejected {
                request_id: "config-after".into(),
                message: "test shutdown".into(),
            })
            .unwrap();
        event_tx.send(RuntimeEvent::Stopped).unwrap();
        drop(event_tx);
        drop(wake_tx);
        coordinator.await.unwrap().unwrap();
    }

    /// A served request that cannot be persisted because teardown removed the
    /// worker root has to stop the daemon; answering on from memory would keep
    /// a closed session apparently alive.
    #[tokio::test]
    async fn a_removed_worker_root_reports_a_fatal_failure_to_the_daemon() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("worker-root");
        let mut durable = DurableRelay::open(&root, SESSION_ID, "1.0.0").unwrap();
        durable
            .record_observation(RelayObservation::Warning {
                message: "before teardown".into(),
            })
            .unwrap();
        let through = durable.latest_ordinal();
        let digest = durable.latest_digest().to_owned();
        let relay = Arc::new(Mutex::new(durable));
        let (wake_tx, _wake_rx) = mpsc::channel(1);
        let (fatal_tx, mut fatal_rx) = fatal_reports();
        let (server, client) = tokio::net::UnixStream::pair().unwrap();
        let served = tokio::spawn(unix::serve_client(
            server,
            relay.clone(),
            wake_tx,
            test_credentials(),
            None,
            fatal_tx,
        ));
        std::fs::remove_dir_all(&root).unwrap();

        let (reader, mut writer) = client.into_split();
        let mut lines = BufReader::new(reader).lines();
        let request = RelayRequestEnvelope {
            request_id: "acknowledge-after-teardown".into(),
            protocol_version: RELAY_PROTOCOL_VERSION,
            request: RelayRequest::Acknowledge {
                through_ordinal: through,
                through_digest: digest,
            },
        };
        let mut encoded = serde_json::to_vec(&request).unwrap();
        encoded.push(b'\n');
        writer.write_all(&encoded).await.unwrap();
        let response: crate::hel_worker::RelayResponseEnvelope =
            serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        assert!(
            matches!(
                response.body,
                RelayResponseBody::Error {
                    error: RelayProtocolError {
                        code: RelayErrorCode::Internal,
                        ..
                    }
                }
            ),
            "{:?}",
            response.body
        );

        let report = fatal_rx.recv().await.expect("the daemon must be told");
        assert!(format!("{report:#}").contains("was removed"), "{report:#}");
        assert!(
            !root.exists(),
            "serving a request recreated the worker root"
        );

        drop(writer);
        drop(lines);
        served.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn client_disconnect_releases_checkpoint_and_runs_queued_prompt() {
        let temp = tempfile::tempdir().unwrap();
        let relay = Arc::new(Mutex::new(
            DurableRelay::open(temp.path(), SESSION_ID, "1.0.0").unwrap(),
        ));
        let (event_tx, event_rx) = runtime_event_channel();
        let (wake_tx, wake_rx) = mpsc::channel(1);
        let (command_tx, mut command_rx) = mpsc::channel(1);
        let coordinator = tokio::spawn(unix::run_relay_coordinator(
            relay.clone(),
            event_rx,
            wake_rx,
            command_tx,
        ));
        event_tx
            .send(RuntimeEvent::SessionConfigured {
                config_options: Vec::new(),
            })
            .unwrap();
        let (server, client) = tokio::net::UnixStream::pair().unwrap();
        let server_task = tokio::spawn(unix::serve_client(
            server,
            relay.clone(),
            wake_tx.clone(),
            test_credentials(),
            None,
            fatal_reports().0,
        ));
        let (reader, mut writer) = client.into_split();
        let mut lines = BufReader::new(reader).lines();

        let begin = RelayRequestEnvelope {
            request_id: "begin-checkpoint".into(),
            protocol_version: RELAY_PROTOCOL_VERSION,
            request: RelayRequest::Submit {
                command_id: "checkpoint-1".into(),
                command: RelayCommand::BeginCheckpoint {
                    reason: Some("test disconnect".into()),
                },
            },
        };
        let mut encoded = serde_json::to_vec(&begin).unwrap();
        encoded.push(b'\n');
        writer.write_all(&encoded).await.unwrap();
        let response = lines.next_line().await.unwrap().unwrap();
        let response: crate::hel_worker::RelayResponseEnvelope =
            serde_json::from_str(&response).unwrap();
        assert!(matches!(response.body, RelayResponseBody::Ok { .. }));
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if relay
                    .lock()
                    .unwrap()
                    .operational_state()
                    .checkpoint_barrier
                    .as_deref()
                    == Some("checkpoint-1")
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let queued_prompt = RelayRequestEnvelope {
            request_id: "queue-prompt".into(),
            protocol_version: RELAY_PROTOCOL_VERSION,
            request: RelayRequest::Submit {
                command_id: "prompt-1".into(),
                command: prompt("runs after disconnect"),
            },
        };
        let mut encoded = serde_json::to_vec(&queued_prompt).unwrap();
        encoded.push(b'\n');
        writer.write_all(&encoded).await.unwrap();
        let response = lines.next_line().await.unwrap().unwrap();
        let response: crate::hel_worker::RelayResponseEnvelope =
            serde_json::from_str(&response).unwrap();
        assert!(matches!(response.body, RelayResponseBody::Ok { .. }));

        writer.shutdown().await.unwrap();
        drop(writer);
        drop(lines);
        server_task.await.unwrap().unwrap();

        assert!(
            relay
                .lock()
                .unwrap()
                .operational_state()
                .checkpoint_barrier
                .is_none()
        );
        assert_prompt(
            tokio::time::timeout(std::time::Duration::from_secs(1), command_rx.recv())
                .await
                .unwrap()
                .unwrap(),
            "prompt-1",
            "runs after disconnect",
        );
        assert!(
            relay
                .lock()
                .unwrap()
                .events_after(0, RELAY_EVENT_GENESIS_DIGEST)
                .unwrap()
                .iter()
                .any(|event| matches!(
                    &event.observation,
                    RelayObservation::CommandInterrupted { command_id, .. }
                        if command_id == "checkpoint-1"
                ))
        );

        event_tx
            .send(RuntimeEvent::CommandRejected {
                request_id: "prompt-1".into(),
                message: "test shutdown".into(),
            })
            .unwrap();
        drop(event_tx);
        drop(wake_tx);
        coordinator.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn relay_client_rejects_unknown_envelope_fields_without_disconnect() {
        let temp = tempfile::tempdir().unwrap();
        let relay = Arc::new(Mutex::new(
            DurableRelay::open(temp.path(), SESSION_ID, "1.0.0").unwrap(),
        ));
        let (wake_tx, _wake_rx) = mpsc::channel(1);
        let (server, client) = tokio::net::UnixStream::pair().unwrap();
        let server_task = tokio::spawn(unix::serve_client(
            server,
            relay,
            wake_tx,
            test_credentials(),
            None,
            fatal_reports().0,
        ));
        let (reader, mut writer) = client.into_split();
        let mut lines = BufReader::new(reader).lines();

        writer
            .write_all(
                b"{\"request_id\":\"retired\",\"protocol_version\":1,\"controller_store_id\":\"old\",\"request\":{\"method\":\"status\"}}\n",
            )
            .await
            .unwrap();
        let rejected: RelayResponseEnvelope =
            serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        assert!(matches!(
            rejected.body,
            RelayResponseBody::Error {
                error: RelayProtocolError {
                    code: RelayErrorCode::InvalidRequest,
                    ..
                }
            }
        ));

        let status = RelayRequestEnvelope {
            request_id: "valid-after-invalid".into(),
            protocol_version: RELAY_PROTOCOL_VERSION,
            request: RelayRequest::Status,
        };
        let mut encoded = serde_json::to_vec(&status).unwrap();
        encoded.push(b'\n');
        writer.write_all(&encoded).await.unwrap();
        let accepted: RelayResponseEnvelope =
            serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        assert!(matches!(
            accepted.body,
            RelayResponseBody::Ok {
                payload: RelayResponsePayload::Status(_)
            }
        ));

        writer.shutdown().await.unwrap();
        drop(writer);
        drop(lines);
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn an_unknown_relay_method_is_named_in_its_rejection() {
        let temp = tempfile::tempdir().unwrap();
        let relay = Arc::new(Mutex::new(
            DurableRelay::open(temp.path(), SESSION_ID, "1.0.0").unwrap(),
        ));
        let (wake_tx, _wake_rx) = mpsc::channel(1);
        let (server, client) = tokio::net::UnixStream::pair().unwrap();
        let server_task = tokio::spawn(unix::serve_client(
            server,
            relay,
            wake_tx,
            test_credentials(),
            None,
            fatal_reports().0,
        ));
        let (reader, mut writer) = client.into_split();
        let mut lines = BufReader::new(reader).lines();

        writer
            .write_all(
                b"{\"request_id\":\"future\",\"protocol_version\":1,\"request\":{\"method\":\"subscribe\",\"params\":{\"after_seq\":0}}}\n",
            )
            .await
            .unwrap();
        let rejected: RelayResponseEnvelope =
            serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        assert_eq!(rejected.request_id, "future");
        let RelayResponseBody::Error { error } = rejected.body else {
            panic!("an unknown method must be rejected");
        };
        assert_eq!(error.code, RelayErrorCode::InvalidRequest);
        assert!(
            error.message.contains("does not support method")
                && error.message.contains("subscribe"),
            "{}",
            error.message
        );

        // The connection is a protocol boundary, not a casualty of the
        // rejection: the next request is still served.
        let status = RelayRequestEnvelope {
            request_id: "after-unknown-method".into(),
            protocol_version: RELAY_PROTOCOL_VERSION,
            request: RelayRequest::Status,
        };
        let mut encoded = serde_json::to_vec(&status).unwrap();
        encoded.push(b'\n');
        writer.write_all(&encoded).await.unwrap();
        let accepted: RelayResponseEnvelope =
            serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        assert!(matches!(
            accepted.body,
            RelayResponseBody::Ok {
                payload: RelayResponsePayload::Status(_)
            }
        ));

        writer.shutdown().await.unwrap();
        drop(writer);
        drop(lines);
        server_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn disconnect_between_close_and_checkpoint_completion_dispatches_close() {
        let temp = tempfile::tempdir().unwrap();
        let relay = Arc::new(Mutex::new(
            DurableRelay::open(temp.path(), SESSION_ID, "1.0.0").unwrap(),
        ));
        let (event_tx, event_rx) = runtime_event_channel();
        let (wake_tx, wake_rx) = mpsc::channel(1);
        let (command_tx, mut command_rx) = mpsc::channel(1);
        let coordinator = tokio::spawn(unix::run_relay_coordinator(
            relay.clone(),
            event_rx,
            wake_rx,
            command_tx,
        ));
        event_tx
            .send(RuntimeEvent::SessionConfigured {
                config_options: Vec::new(),
            })
            .unwrap();
        let (server, client) = tokio::net::UnixStream::pair().unwrap();
        let server_task = tokio::spawn(unix::serve_client(
            server,
            relay.clone(),
            wake_tx.clone(),
            test_credentials(),
            None,
            fatal_reports().0,
        ));
        let (reader, mut writer) = client.into_split();
        let mut lines = BufReader::new(reader).lines();

        let begin = RelayRequestEnvelope {
            request_id: "begin-close-checkpoint".into(),
            protocol_version: RELAY_PROTOCOL_VERSION,
            request: RelayRequest::Submit {
                command_id: "barrier-before-close".into(),
                command: RelayCommand::BeginCheckpoint {
                    reason: Some("disconnect race".into()),
                },
            },
        };
        let mut encoded = serde_json::to_vec(&begin).unwrap();
        encoded.push(b'\n');
        writer.write_all(&encoded).await.unwrap();
        let response = lines.next_line().await.unwrap().unwrap();
        let response: crate::hel_worker::RelayResponseEnvelope =
            serde_json::from_str(&response).unwrap();
        assert!(matches!(response.body, RelayResponseBody::Ok { .. }));
        wait_for_relay_state(&relay, |state| state.checkpoint_ready.is_some()).await;
        let expected = relay
            .lock()
            .unwrap()
            .operational_state()
            .checkpoint_ready
            .unwrap();

        let close = RelayRequestEnvelope {
            request_id: "queue-exact-close".into(),
            protocol_version: RELAY_PROTOCOL_VERSION,
            request: RelayRequest::Submit {
                command_id: "close-after-barrier".into(),
                command: RelayCommand::Close {
                    barrier_command_id: "barrier-before-close".into(),
                    expected,
                },
            },
        };
        let mut encoded = serde_json::to_vec(&close).unwrap();
        encoded.push(b'\n');
        writer.write_all(&encoded).await.unwrap();
        let response = lines.next_line().await.unwrap().unwrap();
        let response: crate::hel_worker::RelayResponseEnvelope =
            serde_json::from_str(&response).unwrap();
        assert!(matches!(response.body, RelayResponseBody::Ok { .. }));

        writer.shutdown().await.unwrap();
        drop(writer);
        drop(lines);
        server_task.await.unwrap().unwrap();

        assert!(matches!(
            tokio::time::timeout(std::time::Duration::from_secs(1), command_rx.recv())
                .await
                .unwrap()
                .unwrap(),
            CommandRequest::Close { request_id } if request_id == "close-after-barrier"
        ));
        event_tx
            .send(RuntimeEvent::CloseApplied {
                request_id: "close-after-barrier".into(),
            })
            .unwrap();
        wait_for_relay_state(&relay, |state| {
            state.execution == RelayExecutionState::Closed
        })
        .await;
        event_tx.send(RuntimeEvent::Stopped).unwrap();
        drop(event_tx);
        drop(wake_tx);
        coordinator.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn relay_v1_client_disconnect_does_not_own_command_execution() {
        let temp = tempfile::tempdir().unwrap();
        let relay = Arc::new(Mutex::new(
            DurableRelay::open(temp.path(), SESSION_ID, "1.0.0").unwrap(),
        ));
        let (event_tx, event_rx) = runtime_event_channel();
        let (wake_tx, wake_rx) = mpsc::channel(1);
        let (command_tx, mut command_rx) = mpsc::channel(1);
        let coordinator = tokio::spawn(unix::run_relay_coordinator(
            relay.clone(),
            event_rx,
            wake_rx,
            command_tx,
        ));
        event_tx
            .send(RuntimeEvent::SessionConfigured {
                config_options: Vec::new(),
            })
            .unwrap();
        let (server, client) = tokio::net::UnixStream::pair().unwrap();
        let server_task = tokio::spawn(unix::serve_client(
            server,
            relay,
            wake_tx.clone(),
            test_credentials(),
            None,
            fatal_reports().0,
        ));
        let (reader, mut writer) = client.into_split();
        let request = RelayRequestEnvelope {
            request_id: "submit".into(),
            protocol_version: RELAY_PROTOCOL_VERSION,
            request: RelayRequest::Submit {
                command_id: "prompt-1".into(),
                command: prompt("continues offline"),
            },
        };
        let mut encoded = serde_json::to_vec(&request).unwrap();
        encoded.push(b'\n');
        writer.write_all(&encoded).await.unwrap();
        drop(writer);
        drop(reader);
        // The response write may win or lose the close race; command
        // execution must not depend on it.
        let _ = server_task.await.unwrap();

        assert_prompt(
            tokio::time::timeout(std::time::Duration::from_secs(1), command_rx.recv())
                .await
                .unwrap()
                .unwrap(),
            "prompt-1",
            "continues offline",
        );
        event_tx
            .send(RuntimeEvent::CommandRejected {
                request_id: "prompt-1".into(),
                message: "test shutdown".into(),
            })
            .unwrap();
        drop(event_tx);
        drop(wake_tx);
        coordinator.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn closed_relay_stays_attachable_after_the_acp_runtime_stops() {
        let temp = tempfile::tempdir().unwrap();
        let mut durable = DurableRelay::open(temp.path(), SESSION_ID, "1.0.0").unwrap();
        submit(
            &mut durable,
            "barrier-before-close",
            RelayCommand::BeginCheckpoint {
                reason: Some("close test".into()),
            },
        );
        let claimed = durable.claim_pending_commands(true).unwrap();
        assert!(matches!(
            claimed.as_slice(),
            [crate::hel_worker::ClaimedRelayCommand {
                command_id,
                command: RelayCommand::BeginCheckpoint { .. },
                ..
            }] if command_id == "barrier-before-close"
        ));
        durable
            .record_checkpoint_ready("barrier-before-close")
            .unwrap();
        let expected = durable
            .operational_state()
            .checkpoint_ready
            .expect("checkpoint is ready");
        submit(
            &mut durable,
            "close-command",
            RelayCommand::Close {
                barrier_command_id: "barrier-before-close".into(),
                expected,
            },
        );
        submit(
            &mut durable,
            "complete-checkpoint",
            RelayCommand::CompleteCheckpoint {
                barrier_command_id: "barrier-before-close".into(),
            },
        );
        let claimed = durable.claim_pending_commands(true).unwrap();
        assert!(matches!(
            claimed.as_slice(),
            [crate::hel_worker::ClaimedRelayCommand {
                command_id,
                command: RelayCommand::Close { .. },
                ..
            }] if command_id == "close-command"
        ));
        durable
            .record_command_completed(
                "close-command",
                crate::hel_worker::RelayCommandOutcome::Closed,
            )
            .unwrap();
        durable
            .record_observation(RelayObservation::Closed)
            .unwrap();
        let relay = Arc::new(Mutex::new(durable));

        let socket = temp.path().join("closed-relay.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let (wake_tx, wake_rx) = mpsc::channel(1);
        drop(wake_rx);
        let (fatal_tx, fatal_rx) = fatal_reports();
        let terminal = tokio::spawn(unix::serve_terminal_relay(
            listener,
            relay.clone(),
            wake_tx,
            test_credentials(),
            unix::ProjectMemoryEndpoint::default(),
            fatal_tx,
            fatal_rx,
        ));
        let stream = tokio::net::UnixStream::connect(&socket).await.unwrap();
        let (reader, mut writer) = stream.into_split();
        let request = RelayRequestEnvelope {
            request_id: "attach-closed-relay".into(),
            protocol_version: RELAY_PROTOCOL_VERSION,
            request: RelayRequest::Attach {
                after_ordinal: 0,
                after_digest: RELAY_EVENT_GENESIS_DIGEST.into(),
            },
        };
        let mut encoded = serde_json::to_vec(&request).unwrap();
        encoded.push(b'\n');
        writer.write_all(&encoded).await.unwrap();
        let response = BufReader::new(reader)
            .lines()
            .next_line()
            .await
            .unwrap()
            .unwrap();
        let response: crate::hel_worker::RelayResponseEnvelope =
            serde_json::from_str(&response).unwrap();
        let state = match response.body {
            RelayResponseBody::Ok {
                payload: RelayResponsePayload::Attached { state, .. },
            } => state,
            body => panic!("closed relay did not accept attach: {body:?}"),
        };
        assert_eq!(state.execution, RelayExecutionState::Closed);

        writer.shutdown().await.unwrap();
        terminal.abort();
        assert!(terminal.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn daemon_restart_serves_a_closed_relay_without_starting_acp() {
        let temp = tempfile::tempdir().unwrap();
        let mut durable = DurableRelay::open(temp.path(), SESSION_ID, "1.0.0").unwrap();
        submit(
            &mut durable,
            "barrier-before-restart-close",
            RelayCommand::BeginCheckpoint {
                reason: Some("closed restart test".into()),
            },
        );
        let barrier = durable.claim_pending_commands(true).unwrap();
        assert!(matches!(
            barrier.as_slice(),
            [crate::hel_worker::ClaimedRelayCommand {
                command_id,
                command: RelayCommand::BeginCheckpoint { .. },
                ..
            }] if command_id == "barrier-before-restart-close"
        ));
        durable
            .record_checkpoint_ready("barrier-before-restart-close")
            .unwrap();
        let expected = durable
            .operational_state()
            .checkpoint_ready
            .expect("checkpoint is ready");
        submit(
            &mut durable,
            "close-before-daemon-restart",
            RelayCommand::Close {
                barrier_command_id: "barrier-before-restart-close".into(),
                expected,
            },
        );
        submit(
            &mut durable,
            "complete-before-daemon-restart",
            RelayCommand::CompleteCheckpoint {
                barrier_command_id: "barrier-before-restart-close".into(),
            },
        );
        let close = durable.claim_pending_commands(true).unwrap();
        assert!(matches!(
            close.as_slice(),
            [crate::hel_worker::ClaimedRelayCommand {
                command_id,
                command: RelayCommand::Close { .. },
                ..
            }] if command_id == "close-before-daemon-restart"
        ));
        durable
            .record_command_completed(
                "close-before-daemon-restart",
                crate::hel_worker::RelayCommandOutcome::Closed,
            )
            .unwrap();
        let closed_frontier = durable.latest_ordinal();
        drop(durable);

        let mut config = launch_config("profile-home-that-must-not-be-used");
        config.bridge_command = temp.path().join("missing-acp-bridge");
        config.cwd = temp.path().to_owned();
        let root = temp.path().to_owned();
        let daemon = tokio::spawn(unix::run_daemon(root.clone(), config));
        let stream = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                match tokio::net::UnixStream::connect(root.join("control.sock")).await {
                    Ok(stream) => break stream,
                    Err(_) if daemon.is_finished() => {
                        panic!("closed relay daemon stopped during startup")
                    }
                    Err(_) => tokio::task::yield_now().await,
                }
            }
        })
        .await
        .unwrap();
        let (reader, mut writer) = stream.into_split();
        let request = RelayRequestEnvelope {
            request_id: "status-after-closed-restart".into(),
            protocol_version: RELAY_PROTOCOL_VERSION,
            request: RelayRequest::Status,
        };
        let mut encoded = serde_json::to_vec(&request).unwrap();
        encoded.push(b'\n');
        writer.write_all(&encoded).await.unwrap();
        let response = BufReader::new(reader)
            .lines()
            .next_line()
            .await
            .unwrap()
            .unwrap();
        let response: RelayResponseEnvelope = serde_json::from_str(&response).unwrap();
        let state = match response.body {
            RelayResponseBody::Ok {
                payload: RelayResponsePayload::Status(state),
            } => state,
            body => panic!("closed relay did not serve status after restart: {body:?}"),
        };
        assert_eq!(state.execution, RelayExecutionState::Closed);
        assert_eq!(state.latest_ordinal, closed_frontier);
        assert!(!root.join("acp-supervisor.json").exists());
        // Session teardown reads this file to stop the daemon before it
        // deletes the root out from under it.
        assert_eq!(
            std::fs::read_to_string(root.join(WORKER_PID_FILE))
                .unwrap()
                .trim(),
            std::process::id().to_string()
        );

        writer.shutdown().await.unwrap();
        daemon.abort();
        assert!(daemon.await.unwrap_err().is_cancelled());
    }

    /// Opening the relay recovers its journal in place, so a second daemon has
    /// to detect the live one before it can rewrite files the first is using.
    #[tokio::test]
    async fn a_live_worker_stops_a_second_daemon_before_it_touches_durable_state() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().to_owned();
        let mut relay = DurableRelay::open(&root, SESSION_ID, "1.0.0").unwrap();
        relay
            .record_observation(RelayObservation::Warning {
                message: "recorded by the live worker".into(),
            })
            .unwrap();
        drop(relay);

        // A torn tail is exactly what a startup recovery would rewrite.
        let journal = root
            .join(crate::hel_worker::RELAY_JOURNAL_DIR)
            .join("active.jsonl");
        let mut torn = std::fs::read(&journal).unwrap();
        torn.extend_from_slice(b"{\"ordinal\":2,\"truncated\"");
        std::fs::write(&journal, &torn).unwrap();
        let exit_record = root.join("worker-exit.json");
        std::fs::write(&exit_record, b"{\"reason\":\"earlier life\"}").unwrap();
        let _live = tokio::net::UnixListener::bind(root.join("control.sock")).unwrap();

        let error = unix::run_daemon(root.clone(), launch_config("/profile"))
            .await
            .unwrap_err();

        assert!(
            format!("{error:#}").contains("a worker is already running"),
            "{error:#}"
        );
        assert_eq!(
            std::fs::read(&journal).unwrap(),
            torn,
            "the second daemon recovered a live worker's journal"
        );
        assert!(
            exit_record.exists(),
            "the live worker's exit record was cleared"
        );
        assert!(
            !root.join(WORKER_PID_FILE).exists(),
            "the second daemon claimed a root it does not own"
        );
    }

    /// Teardown needs the PID of the daemon that owns the root right now, not
    /// of one that died earlier.
    #[test]
    fn the_worker_pidfile_replaces_a_previous_daemons_claim() {
        let temp = tempfile::tempdir().unwrap();
        let pidfile = temp.path().join(WORKER_PID_FILE);
        std::fs::write(&pidfile, "999999999\n").unwrap();

        unix::write_worker_pidfile(temp.path(), std::process::id()).unwrap();

        assert_eq!(
            std::fs::read_to_string(&pidfile).unwrap().trim(),
            std::process::id().to_string()
        );
    }

    #[tokio::test]
    async fn oversized_response_is_rejected_before_writing() {
        let (server, _client) = tokio::net::UnixStream::pair().unwrap();
        let (_, mut writer) = server.into_split();
        let response = RelayResponseEnvelope {
            request_id: "oversized".into(),
            protocol_version: RELAY_PROTOCOL_VERSION,
            body: RelayResponseBody::Error {
                error: RelayProtocolError {
                    code: RelayErrorCode::Internal,
                    message: "x".repeat(crate::hel_worker::MAX_FRAME_BYTES),
                    retryable: false,
                    detail: None,
                },
            },
        };

        let error = unix::write_response(&mut writer, &response)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("response frame is too large"));
    }

    #[tokio::test]
    async fn request_line_limit_is_enforced_while_the_line_is_read() {
        let mut exact = BufReader::new(&b"12345678\nnext\n"[..]);
        assert_eq!(
            unix::read_bounded_line(&mut exact, 8).await.unwrap(),
            Some("12345678".into())
        );
        assert_eq!(
            unix::read_bounded_line(&mut exact, 8).await.unwrap(),
            Some("next".into())
        );

        let mut oversized = BufReader::with_capacity(4, &b"123456789-without-a-newline"[..]);
        let error = unix::read_bounded_line(&mut oversized, 8)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("request frame is too large"));
    }

    #[test]
    fn repeated_dispatch_wakes_coalesce_to_one_pending_token() {
        let temp = tempfile::tempdir().unwrap();
        let relay = Arc::new(Mutex::new(
            DurableRelay::open(temp.path(), SESSION_ID, "1.0.0").unwrap(),
        ));
        let (wake_tx, mut wake_rx) = mpsc::channel(1);

        for _ in 0..10_000 {
            unix::wake_dispatch(&relay, &wake_tx).unwrap();
        }
        assert_eq!(wake_rx.try_recv(), Ok(()));
        assert!(matches!(
            wake_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn coordinator_failure_aborts_peer_and_preserves_the_cause() {
        let mut peer = tokio::spawn(std::future::pending::<()>());
        let error = unix::abort_peer_and_return(
            &mut peer,
            anyhow::anyhow!("original coordinator failure"),
            "relay coordinator failed",
        )
        .await
        .unwrap_err();

        assert!(peer.is_finished());
        assert!(format!("{error:#}").contains("original coordinator failure"));
    }

    #[test]
    fn resume_prefers_explicit_identity_then_relay_identity() {
        let temp = tempfile::tempdir().unwrap();
        let mut relay = DurableRelay::open(temp.path(), SESSION_ID, "1.0.0").unwrap();
        relay
            .record_observation(RelayObservation::SessionOpened {
                native_session_id: "native-relay".into(),
                resumed: false,
            })
            .unwrap();
        let mut config = launch_config("/var/lib/hel/profiles/session");
        assert_eq!(
            unix::select_resume_session(&config, &relay).as_deref(),
            Some("native-relay")
        );
        config.native_session_id = Some("native-explicit".into());
        assert_eq!(
            unix::select_resume_session(&config, &relay).as_deref(),
            Some("native-explicit")
        );
    }

    /// A history that seals several journal segments and overflows one replay
    /// page, so an attach against it really reads and decompresses from disk.
    fn paged_relay_history(root: &Path, events: usize) -> DurableRelay {
        let mut relay = DurableRelay::open(root, SESSION_ID, "1.0.0").unwrap();
        for index in 0..events {
            relay
                .record_observation(RelayObservation::Warning {
                    message: format!("{index:04}:{}", "x".repeat(64 * 1024)),
                })
                .unwrap();
        }
        relay
    }

    fn attach_frame(request_id: &str, after_ordinal: u64, after_digest: &str) -> Vec<u8> {
        let mut encoded = serde_json::to_vec(&RelayRequestEnvelope {
            request_id: request_id.to_owned(),
            protocol_version: RELAY_PROTOCOL_VERSION,
            request: RelayRequest::Attach {
                after_ordinal,
                after_digest: after_digest.to_owned(),
            },
        })
        .unwrap();
        encoded.push(b'\n');
        encoded
    }

    /// Records small observations through the relay lock until it is stopped,
    /// reporting how many landed and the worst time it ever waited for the
    /// lock. The wait is the contention this test is about; the append's own
    /// fsync is deliberately left out of it.
    struct LiveRecorder {
        stop: Arc<std::sync::atomic::AtomicBool>,
        task: tokio::task::JoinHandle<(u64, std::time::Duration)>,
    }

    impl LiveRecorder {
        fn start(relay: Arc<Mutex<DurableRelay>>) -> Self {
            // Sample the lock often enough that a page read cannot hide in the
            // gaps, but append rarely enough that the fsync per event neither
            // dominates the sampling nor grows the active segment far enough
            // to seal it under the reader.
            const SAMPLES_PER_RECORD: u64 = 8;
            const RECORD_LIMIT: u64 = 512;
            let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let recorder_stop = stop.clone();
            let task = tokio::task::spawn_blocking(move || {
                let mut samples = 0_u64;
                let mut recorded = 0_u64;
                let mut worst_wait = std::time::Duration::ZERO;
                while !recorder_stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let waiting = std::time::Instant::now();
                    let mut guard = relay.lock().expect("relay state lock poisoned");
                    worst_wait = worst_wait.max(waiting.elapsed());
                    if samples.is_multiple_of(SAMPLES_PER_RECORD) && recorded < RECORD_LIMIT {
                        guard
                            .record_observation(RelayObservation::Warning {
                                message: format!("live-{recorded}"),
                            })
                            .unwrap();
                        recorded += 1;
                    }
                    drop(guard);
                    samples += 1;
                    std::thread::sleep(std::time::Duration::from_micros(200));
                }
                (recorded, worst_wait)
            });
            Self { stop, task }
        }

        async fn stop(self) -> (u64, std::time::Duration) {
            self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
            self.task.await.unwrap()
        }
    }

    /// Controllers are supposed to come and go without perturbing the session.
    /// A catch-up over a long offline history reads page after page from disk
    /// and decompresses sealed segments; doing that under the relay lock stops
    /// the coordinator from recording ACP events, and once its bounded channel
    /// fills the agent's turn stops with it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_replaying_controller_does_not_stall_live_event_recording() {
        let temp = tempfile::tempdir().unwrap();
        let durable = paged_relay_history(temp.path(), 80);
        let frontier = durable.latest_ordinal();
        let relay = Arc::new(Mutex::new(durable));

        let (mut client, server) = tokio::net::UnixStream::pair().unwrap();
        let (wake_tx, _wake_rx) = mpsc::channel(1);
        let served = tokio::spawn(unix::serve_client(
            server,
            relay.clone(),
            wake_tx,
            test_credentials(),
            None,
            fatal_reports().0,
        ));

        let recorder = LiveRecorder::start(relay.clone());
        client
            .write_all(&attach_frame("catch-up", 0, RELAY_EVENT_GENESIS_DIGEST))
            .await
            .unwrap();
        let response = BufReader::new(&mut client)
            .lines()
            .next_line()
            .await
            .unwrap()
            .unwrap();
        let (recorded, worst_wait) = recorder.stop().await;

        let response: RelayResponseEnvelope = serde_json::from_str(&response).unwrap();
        let RelayResponseBody::Ok {
            payload:
                RelayResponsePayload::Attached {
                    events,
                    through_ordinal,
                    ..
                },
        } = response.body
        else {
            panic!("catch-up attach failed: {:?}", response.body);
        };
        assert!(
            !events.is_empty() && through_ordinal < frontier,
            "this history should need several pages: {through_ordinal} of {frontier}"
        );

        // What one page costs to assemble, measured on this machine with
        // nothing else running. Serving it under the relay lock would make a
        // recorder wait about this long; serving it off the lock costs a
        // recorder only the plan capture.
        let page_cost = std::time::Instant::now();
        let _ = unix::handle_request(
            &relay,
            RelayRequestEnvelope {
                request_id: "page-cost".into(),
                protocol_version: RELAY_PROTOCOL_VERSION,
                request: RelayRequest::Attach {
                    after_ordinal: 0,
                    after_digest: RELAY_EVENT_GENESIS_DIGEST.into(),
                },
            },
        )
        .await
        .unwrap();
        let page_cost = page_cost.elapsed();
        assert!(
            page_cost >= std::time::Duration::from_millis(10),
            "a page assembled in {page_cost:?}; too cheap to say anything about contention"
        );
        assert!(recorded > 0, "no event was recorded during the replay");
        assert!(
            worst_wait * 3 < page_cost,
            "recording waited {worst_wait:?} for a page that takes {page_cost:?} to assemble: \
             the page is being read under the relay lock"
        );

        drop(client);
        served.await.unwrap().unwrap();
    }

    /// Sealing the active segment moves events into a file no captured replay
    /// plan names, and a busy session seals one per megabyte of transcript. A
    /// controller catching up through that must still get its pages: the relay
    /// plans again against the journal as it now stands instead of handing the
    /// controller a failure it did nothing to cause.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_catch_up_completes_while_the_journal_keeps_sealing_under_it() {
        let temp = tempfile::tempdir().unwrap();
        let durable = paged_relay_history(temp.path(), 96);
        let target = durable.latest_ordinal();
        let generation = durable.journal_generation();
        let relay = Arc::new(Mutex::new(durable));

        // Events large enough to seal a segment every few appends, written as
        // fast as the durable path allows.
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writer_relay = relay.clone();
        let writer_stop = stop.clone();
        let writer = tokio::task::spawn_blocking(move || {
            let mut written = 0_u64;
            while !writer_stop.load(std::sync::atomic::Ordering::Relaxed) && written < 24 {
                writer_relay
                    .lock()
                    .expect("relay state lock poisoned")
                    .record_observation(RelayObservation::Warning {
                        message: format!("{written:04}:{}", "y".repeat(256 * 1024)),
                    })
                    .unwrap();
                written += 1;
            }
            written
        });

        let mut cursor = (0_u64, RELAY_EVENT_GENESIS_DIGEST.to_owned());
        let mut pages = 0_usize;
        while cursor.0 < target {
            let body = unix::handle_request(
                &relay,
                RelayRequestEnvelope {
                    request_id: format!("sealing-page-{pages}"),
                    protocol_version: RELAY_PROTOCOL_VERSION,
                    request: RelayRequest::Attach {
                        after_ordinal: cursor.0,
                        after_digest: cursor.1.clone(),
                    },
                },
            )
            .await
            .unwrap()
            .body;
            let RelayResponseBody::Ok {
                payload:
                    RelayResponsePayload::Attached {
                        through_ordinal,
                        through_digest,
                        ..
                    },
            } = body
            else {
                panic!("page {pages} of a catch-up under a sealing journal failed: {body:?}");
            };
            assert!(
                through_ordinal > cursor.0,
                "page {pages} made no progress from event {}",
                cursor.0
            );
            cursor = (through_ordinal, through_digest);
            pages += 1;
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        writer.await.unwrap();

        assert!(
            relay
                .lock()
                .expect("relay state lock poisoned")
                .journal_generation()
                > generation,
            "the journal never sealed, so this run proved nothing"
        );
    }

    /// Recording throughput during a full catch-up, measured with the page
    /// read inside and outside the relay lock so both policies are timed in
    /// one process. Run with
    /// `cargo test --lib
    /// hel_worker_runtime::relay_tests::catch_up_recording_throughput
    /// -- --ignored --nocapture`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "timing measurement, not a behavior assertion"]
    async fn catch_up_recording_throughput() {
        const HISTORY_EVENTS: usize = 200;

        async fn catch_up(defer_page_reads: bool) -> (u64, std::time::Duration, usize) {
            let temp = tempfile::tempdir().unwrap();
            let durable = paged_relay_history(temp.path(), HISTORY_EVENTS);
            let frontier = durable.latest_ordinal();
            let genesis = RELAY_EVENT_GENESIS_DIGEST.to_owned();
            let relay = Arc::new(Mutex::new(durable));
            let recorder = LiveRecorder::start(relay.clone());

            let started = std::time::Instant::now();
            let mut cursor = (0_u64, genesis);
            let mut pages = 0_usize;
            while cursor.0 < frontier {
                let envelope = RelayRequestEnvelope {
                    request_id: format!("page-{pages}"),
                    protocol_version: RELAY_PROTOCOL_VERSION,
                    request: RelayRequest::Attach {
                        after_ordinal: cursor.0,
                        after_digest: cursor.1.clone(),
                    },
                };
                let body = if defer_page_reads {
                    unix::handle_request(&relay, envelope).await.unwrap().body
                } else {
                    // The coupled policy: the whole read runs under the lock.
                    relay
                        .lock()
                        .expect("relay state lock poisoned")
                        .handle(envelope)
                        .body
                };
                let RelayResponseBody::Ok {
                    payload:
                        RelayResponsePayload::Attached {
                            through_ordinal,
                            through_digest,
                            ..
                        },
                } = body
                else {
                    panic!("catch-up page {pages} (defer={defer_page_reads}) failed: {body:?}");
                };
                cursor = (through_ordinal, through_digest);
                pages += 1;
            }
            let elapsed = started.elapsed();
            let (recorded, _) = recorder.stop().await;
            (recorded, elapsed, pages)
        }

        for round in 0..2 {
            let (coupled, coupled_elapsed, pages) = catch_up(false).await;
            let (deferred, deferred_elapsed, _) = catch_up(true).await;
            println!(
                "round {round}: {pages} pages | under the lock {coupled} events in \
                 {coupled_elapsed:?} | off the lock {deferred} events in {deferred_elapsed:?}"
            );
        }
    }

    #[test]
    fn relative_paths_are_resolved_before_the_bridge_changes_directory() {
        let mut config = launch_config(".local/share/hel/profiles/session");
        resolve_relative_harness_home(&mut config, Path::new("/home/ubuntu"));
        assert_eq!(
            config.environment["CODEX_HOME"],
            "/home/ubuntu/.local/share/hel/profiles/session"
        );
        assert_eq!(
            resolve_relative_worker_root(
                ".local/share/hel/workers/session".into(),
                Path::new("/home/ubuntu"),
            ),
            Path::new("/home/ubuntu/.local/share/hel/workers/session")
        );
    }

    #[test]
    fn project_memory_connection_requests_round_trip_replica_and_baseline() {
        let directory = tempfile::tempdir().unwrap();
        let memory = ProjectMemoryLaunchConfig {
            project_key: "project".into(),
            root: directory.path().join("replica"),
            baseline_root: directory.path().join("baseline"),
            repository_roots: BTreeMap::new(),
            mcp_delivery: ProjectMemoryMcpDelivery::Acp,
        };
        let baseline = crate::hel_project_memory::ProjectMemoryStore::new(&memory.baseline_root);
        baseline
            .install_snapshot(&crate::hel_project_memory::ProjectMemorySnapshot {
                files: BTreeMap::from([("/MEMORY.md".into(), "base".into())]),
            })
            .unwrap();
        let replica = crate::hel_project_memory::ProjectMemoryStore::new(&memory.root);
        replica
            .install_snapshot(&crate::hel_project_memory::ProjectMemorySnapshot {
                files: BTreeMap::from([("/MEMORY.md".into(), "changed".into())]),
            })
            .unwrap();

        let payload =
            unix::apply_project_memory_request(&memory, &RelayRequest::ProjectMemorySnapshot)
                .unwrap();
        let RelayResponsePayload::ProjectMemorySnapshot {
            baseline: captured_baseline,
            replica: captured_replica,
        } = payload
        else {
            panic!("unexpected project-memory payload")
        };
        assert_eq!(captured_baseline.files["/MEMORY.md"], "base");
        assert_eq!(captured_replica.files["/MEMORY.md"], "changed");

        unix::apply_project_memory_request(
            &memory,
            &RelayRequest::InstallProjectMemorySnapshot {
                snapshot: captured_replica.clone(),
            },
        )
        .unwrap();
        assert_eq!(baseline.snapshot().unwrap(), captured_replica);
    }

    #[tokio::test]
    async fn abandoned_project_memory_requests_do_not_overlap_blocking_io() {
        let gate = Arc::new(tokio::sync::Semaphore::new(1));
        let (first_started_tx, first_started_rx) = tokio::sync::oneshot::channel();
        let (release_first_tx, release_first_rx) = std::sync::mpsc::channel();
        let first_gate = gate.clone();
        let first = tokio::spawn(async move {
            unix::run_serialized_project_memory_io(&first_gate, move || {
                first_started_tx.send(()).unwrap();
                release_first_rx.recv().unwrap();
                Ok(())
            })
            .await
        });
        first_started_rx.await.unwrap();

        // Model a controller dropping its request future after its transport
        // timeout. The blocking operation survives that cancellation.
        first.abort();
        let (second_started_tx, second_started_rx) = tokio::sync::oneshot::channel();
        let second_gate = gate.clone();
        let second = tokio::spawn(async move {
            unix::run_serialized_project_memory_io(&second_gate, move || {
                second_started_tx.send(()).unwrap();
                Ok(())
            })
            .await
        });
        let mut second_started_rx = std::pin::pin!(second_started_rx);
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(100),
                &mut second_started_rx,
            )
            .await
            .is_err(),
            "abandoning a request released its in-flight filesystem permit"
        );

        release_first_tx.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), &mut second_started_rx)
            .await
            .expect("the queued request did not start after prior I/O completed")
            .unwrap();
        second.await.unwrap().unwrap().unwrap();
    }
}
