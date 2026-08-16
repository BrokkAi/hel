//! Hel: a session control plane for ACP coding agents.

use std::io::{self, IsTerminal};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use clap::{ArgGroup, Args, Parser, Subcommand, ValueEnum};
use crossterm::event::{
    self, DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture,
    Event, KeyboardEnhancementFlags, PopKeyboardEnhancementFlags, PushKeyboardEnhancementFlags,
};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use hel::hel_archive::verify_archive_streaming;
use hel::hel_config::{HelConfig, ProjectBundle, ProjectRepository, config_path, sessions_dir};
use hel::hel_controller::{
    Controller, ControllerStoreGuard, SessionLaunchOptions, SessionResumeOptions,
};
use hel::hel_credentials::{CredentialSyncCoordinator, CredentialSyncHandle, CredentialSyncTarget};
use hel::hel_greeting::{GreetingFacts, RepositoryGreetingFacts};
use hel::hel_import::{
    BundleResolution, ClaudeImportRequest, ClaudeSessionSelection, CodexImportRequest,
    CodexSessionSelection, ImportArchiveProgress, ImportControl, KimiImportRequest,
    KimiSessionSelection, claude_config_home, codex_config_home, configured_bundle_for_local,
    configured_bundle_for_origin, import_claude_session, import_claude_session_with_control,
    import_codex_session, import_codex_session_with_control, import_kimi_session,
    import_kimi_session_with_control, import_safety_issues, kimi_config_home,
    locate_claude_session, locate_codex_session, locate_kimi_session, read_claude_transcript,
    read_codex_transcript, read_kimi_transcript, resolve_bundle, scan_claude_sessions,
    scan_codex_sessions, scan_kimi_sessions, session_edit_targets,
};
use hel::hel_projection::materialized_session_from_canonical;
use hel::hel_quota::{ProfileQuota, QuotaManager, QuotaRefreshRequest};
use hel::hel_server::{
    ControllerAction, ControllerRequest, ResumeQueueDisposition, ServerOptions, ViewerQueuedPrompt,
    ViewerQuota, ViewerSnapshot,
};
use hel::hel_session_manager::{
    RelaySessionTarget, SessionManagerControl, SessionManagerUpdate, SessionManagerUpdates,
    ViewError, new_command_id, spawn_session_manager,
};
use hel::hel_setup::{SetupOutcome, github_repository_from_origin, run_setup_dialog};
use hel::hel_state::{
    HelState, MaterializedSession, SessionRecord, SessionResourceAllocation, SessionState,
    TargetLocator,
};
use hel::hel_targets::{
    CancellableProcessExecutor, CommandExecutor, CommandOutput, CommandSpec,
    DeploymentCapacityKind, DeploymentCapacityTarget, DeploymentCapacityUsage, ProcessExecutor,
    SessionResourceProbe, SessionResourceUsage,
};
use hel::hel_worker::RelayCommand;
use hel::hel_worker_runtime::{
    AcpSupervisorSpec, WorkerLaunchConfig, proxy, run_acp_supervisor, run_daemon,
};
use hel_tui::{
    DashboardAction, DashboardState, ImportProfileOption, ImportSessionOption,
    PreparedMaterializedSessionDetail, SessionOperationKind, render,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use tokio_stream::StreamExt as _;

#[derive(Debug, Parser)]
#[command(name = "hel", version, about = "ACP session control plane")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start the phone-oriented remote-control server.
    Server(ServerArgs),
    /// Internal target-side worker commands.
    #[command(hide = true)]
    Worker(WorkerArgs),
    /// Internal controller-side local Git broker.
    #[command(hide = true)]
    Broker(BrokerArgs),
    /// Diagnose platform and configuration prerequisites.
    Doctor(DoctorArgs),
    /// Discover local agent homes and create an initial Hel configuration.
    Setup(SetupArgs),
    /// Adopt a native coding-agent session as an archived Hel session.
    Import(ImportArgs),
    /// Find, adopt, or explicitly destroy managed workers missing from state.
    Recover(RecoverArgs),
    /// Create a verified recovery copy for an active session.
    Checkpoint(CheckpointArgs),
    /// Run a harness login for a profile so live sessions pick up fresh credentials.
    Login(LoginArgs),
}

#[derive(Debug, Args)]
struct CheckpointArgs {
    #[arg(long)]
    session: String,
}

#[derive(Debug, Args)]
struct LoginArgs {
    /// Profile to authenticate. Optional when exactly one profile exists.
    #[arg(long)]
    profile: Option<String>,
}

#[derive(Debug, Args)]
struct RecoverArgs {
    #[command(subcommand)]
    command: RecoverCommand,
}

#[derive(Debug, Subcommand)]
enum RecoverCommand {
    /// List managed worker resources not present in controller state.
    Scan {
        #[arg(long)]
        json: bool,
    },
    /// Probe a managed worker and add it back to controller state.
    Adopt {
        #[arg(long)]
        session: String,
        #[arg(long)]
        target: String,
        /// Required only for current-v1 workers created before ownership markers.
        #[arg(long)]
        profile: Option<String>,
        /// Required only for current-v1 workers created before ownership markers.
        #[arg(long)]
        bundle: Option<String>,
    },
    /// Destroy an untracked managed resource after exact-ID confirmation.
    Destroy {
        #[arg(long)]
        session: String,
        #[arg(long)]
        target: String,
        #[arg(long)]
        confirm: String,
    },
}

#[derive(Debug, Args)]
struct BrokerArgs {
    #[arg(long)]
    spec: PathBuf,
}

#[derive(Debug, Args)]
struct DoctorArgs {
    /// Emit a machine-readable array of prerequisite checks.
    #[arg(long)]
    json: bool,
    /// Run disposable container smoke tests where supported.
    #[arg(long)]
    smoke: bool,
}

#[derive(Debug, Args)]
struct SetupArgs {
    #[command(subcommand)]
    command: Option<SetupCommand>,
}

#[derive(Debug, Args)]
struct ImportArgs {
    #[command(subcommand)]
    command: ImportCommand,
}

#[derive(Debug, Subcommand)]
enum ImportCommand {
    /// Import a session created by vanilla Claude Code.
    Claude(ClaudeImportArgs),
    /// Import a session created by vanilla Codex.
    Codex(CodexImportArgs),
    /// Import a session created by vanilla Kimi Code.
    Kimi(KimiImportArgs),
}

#[derive(Debug, Args)]
#[command(group(
    ArgGroup::new("codex-session-selection")
        .required(true)
        .args(["session", "latest"])
))]
struct CodexImportArgs {
    /// Native Codex session UUID to import.
    #[arg(long)]
    session: Option<String>,
    /// Import the most recently modified Codex session.
    #[arg(long)]
    latest: bool,
    /// Existing configured bundle to associate with the imported session.
    #[arg(long)]
    bundle: Option<String>,
    /// Title displayed in Hel's dashboard.
    #[arg(long)]
    title: Option<String>,
    /// Proceed after acknowledging dirty detected Git roots.
    #[arg(long = "allow-dirty", visible_alias = "allow-dirty-local")]
    allow_dirty_local: bool,
    /// Proceed after acknowledging edited non-Git directories will be omitted.
    #[arg(long)]
    allow_omitted_non_git: bool,
}

#[derive(Debug, Args)]
#[command(group(
    ArgGroup::new("kimi-session-selection")
        .required(true)
        .args(["session", "latest"])
))]
struct KimiImportArgs {
    /// Native Kimi session UUID to import.
    #[arg(long)]
    session: Option<String>,
    /// Import the most recently modified Kimi session.
    #[arg(long)]
    latest: bool,
    /// Existing configured bundle to associate with the imported session.
    #[arg(long)]
    bundle: Option<String>,
    /// Title displayed in Hel's dashboard.
    #[arg(long)]
    title: Option<String>,
    /// Proceed after acknowledging dirty detected Git roots.
    #[arg(long = "allow-dirty", visible_alias = "allow-dirty-local")]
    allow_dirty_local: bool,
    /// Proceed after acknowledging edited non-Git directories will be omitted.
    #[arg(long)]
    allow_omitted_non_git: bool,
}

#[derive(Debug, Args)]
#[command(group(
    ArgGroup::new("claude-session-selection")
        .required(true)
        .args(["session", "latest"])
))]
struct ClaudeImportArgs {
    /// Native Claude session UUID to import.
    #[arg(long)]
    session: Option<String>,
    /// Import the most recently modified Claude session across all projects.
    #[arg(long)]
    latest: bool,
    /// Existing configured bundle to associate with the imported session.
    #[arg(long)]
    bundle: Option<String>,
    /// Title displayed in Hel's dashboard.
    #[arg(long)]
    title: Option<String>,
    /// Proceed after acknowledging dirty detected Git roots.
    #[arg(long = "allow-dirty", visible_alias = "allow-dirty-local")]
    allow_dirty_local: bool,
    /// Proceed after acknowledging edited non-Git directories will be omitted.
    #[arg(long)]
    allow_omitted_non_git: bool,
}

#[derive(Debug, Subcommand)]
enum SetupCommand {
    /// Print coding-agent instructions for preparing a host.
    Instructions {
        #[arg(long, value_enum)]
        platform: SetupPlatform,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum SetupPlatform {
    Linux,
    Macos,
}

#[derive(Debug, Args)]
struct ServerArgs {
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

#[derive(Debug, Args)]
struct WorkerArgs {
    #[command(subcommand)]
    command: WorkerCommand,
}

const QUOTA_REFRESH_INTERVAL: Duration = Duration::from_secs(60 * 60);
const RESOURCE_POLL_INTERVAL: Duration = Duration::from_secs(60);
const RESOURCE_POLL_TIMEOUT: Duration = Duration::from_secs(15);
const CAPACITY_POLL_INTERVAL: Duration = Duration::from_secs(30);
const WORKER_DIAGNOSIS_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_CONCURRENT_PHONE_ACTIONS: usize = 4;
/// Redraw cadence for displays that move with the wall clock: turn timers,
/// countdowns, and elapsed times.
const DASHBOARD_CLOCK_TICK: Duration = Duration::from_secs(1);
/// Redraw cadence while the import progress dialog is on screen.
const IMPORT_PROGRESS_TICK: Duration = Duration::from_millis(125);
const QUOTA_REFRESH_NOTICE: &str = "Refreshing profile quotas…";
const QUOTA_REFRESHED_NOTICE: &str = "Profile quotas refreshed.";

#[derive(Debug, Clone, Default)]
struct QuotaRefreshBatch {
    generation: u64,
    profiles: Vec<QuotaRefreshRequest>,
}

#[derive(Debug)]
enum QuotaUpdate {
    Refreshing { profile_ids: Vec<String> },
    Report(ProfileQuota),
    Finished { generation: u64 },
}

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

type WorkerPollTarget = RelaySessionTarget;
type WorkerPollUpdate = SessionManagerUpdate;

#[derive(Debug)]
struct WorkerDiagnosisEpisode {
    id: u64,
    error: String,
    diagnosed: bool,
}

#[derive(Debug, Default)]
struct WorkerDiagnosisTracker {
    next_episode: u64,
    current: std::collections::BTreeMap<String, WorkerDiagnosisEpisode>,
    pending: std::collections::BTreeMap<String, u64>,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct WorkerDiagnosisCompletion {
    display_error: Option<String>,
    restart_episode: Option<u64>,
}

impl WorkerDiagnosisTracker {
    fn observe(&mut self, session_id: &str, connected: bool, error: Option<String>) -> Option<u64> {
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

    fn finish(&mut self, session_id: &str, episode_id: u64) -> WorkerDiagnosisCompletion {
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
struct ResourcePollTarget {
    session_id: String,
    probe: SessionResourceProbe,
}

#[derive(Debug)]
struct ResourcePollUpdate {
    session_id: String,
    usage: SessionResourceUsage,
}

#[derive(Debug)]
struct CapacityPollUpdate {
    target_id: String,
    result: std::result::Result<Option<DeploymentCapacityUsage>, String>,
    sampled_at_epoch_seconds: u64,
}

#[derive(Debug, Subcommand)]
enum WorkerCommand {
    /// Own an ACP bridge and durable session event log.
    Run {
        #[arg(long)]
        root: PathBuf,
        #[arg(long)]
        config: PathBuf,
    },
    /// Proxy JSON-lines between stdio and a detached worker.
    Proxy {
        #[arg(long)]
        root: PathBuf,
    },
    /// Supervise the ACP bridge process tree for a worker daemon.
    AcpSupervisor {
        #[arg(long)]
        spec: PathBuf,
    },
    /// Build a target-side archive for verified controller transfer.
    ExportCheckpoint {
        #[arg(long)]
        spec: PathBuf,
    },
    /// Restore a verified archive into a freshly cloned target.
    RestoreCheckpoint {
        #[arg(long)]
        spec: PathBuf,
    },
    /// Restore controller-side local repository bootstrap snapshots.
    RestoreRepositories {
        #[arg(long)]
        spec: PathBuf,
    },
    /// Install one streamed resource directory on a remote target.
    InstallResource {
        #[arg(long)]
        destination: PathBuf,
    },
    /// Bridge controller Git services to this worker over stdio.
    GitBridge {
        #[arg(long)]
        root: PathBuf,
    },
    /// Expose one bridged repository as a Git ext transport.
    GitProxy {
        #[arg(long)]
        root: PathBuf,
        #[arg(long)]
        repository: String,
        service: String,
    },
}

/// Record why a worker died where the controller can find it. The daemon's
/// stdout/stderr go to worker.log; this file is the structured summary read
/// by `Controller` diagnosis when a worker becomes unreachable.
fn write_worker_exit_record(root: &std::path::Path, reason: &str) {
    let record = serde_json::json!({
        "reason": reason,
        "at": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        "version": env!("CARGO_PKG_VERSION"),
    });
    let _ = std::fs::write(
        root.join("worker-exit.json"),
        serde_json::to_vec_pretty(&record).unwrap_or_default(),
    );
}

/// Capture panics as last words too; the default hook then prints the
/// backtrace to stderr, which the launch redirect lands in worker.log.
fn install_worker_last_words(root: &std::path::Path) {
    let root = root.to_path_buf();
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        write_worker_exit_record(&root, &format!("panic: {info}"));
        default_hook(info);
    }));
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let is_controller_process = matches!(
        &cli.command,
        None | Some(
            Command::Server(_)
                | Command::Setup(_)
                | Command::Import(_)
                | Command::Recover(_)
                | Command::Checkpoint(_)
        )
    );
    let _controller_guard = is_controller_process
        .then(ControllerStoreGuard::acquire)
        .transpose()?;
    if is_controller_process {
        hel::hel_database::recover_interrupted_checkpointing_sessions(
            &chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        )?;
        hel::hel_controller::reconcile_managed_checkpoint_archives()?;
    }
    match cli.command {
        None => run_dashboard().await,
        Some(Command::Server(args)) => run_server(args).await,
        Some(Command::Worker(args)) => match args.command {
            WorkerCommand::Run { root, config } => {
                install_worker_last_words(&root);
                let result = run_daemon(root.clone(), WorkerLaunchConfig::read(&config)?).await;
                if let Err(error) = &result {
                    write_worker_exit_record(&root, &format!("{error:#}"));
                }
                result
            }
            WorkerCommand::Proxy { root } => proxy(root).await,
            WorkerCommand::AcpSupervisor { spec } => {
                run_acp_supervisor(AcpSupervisorSpec::read(&spec)?).await
            }
            WorkerCommand::ExportCheckpoint { spec } => {
                let checkpoint = hel::hel_checkpoint::export_from_spec_file(&spec)?;
                println!("{}", serde_json::to_string(&checkpoint)?);
                Ok(())
            }
            WorkerCommand::RestoreCheckpoint { spec } => {
                hel::hel_checkpoint::restore_from_spec_file(&spec)
            }
            WorkerCommand::RestoreRepositories { spec } => {
                hel::hel_checkpoint::restore_repositories_from_spec_file(&spec)
            }
            WorkerCommand::InstallResource { destination } => {
                hel::hel_resources::install_resource_stream(std::io::stdin(), &destination)
            }
            WorkerCommand::GitBridge { root } => hel::hel_git_proxy::run_worker_bridge(&root).await,
            WorkerCommand::GitProxy {
                root,
                repository,
                service,
            } => hel::hel_git_proxy::run_worker_proxy(&root, &repository, &service).await,
        },
        Some(Command::Broker(args)) => hel::hel_git_proxy::run_broker(&args.spec).await,
        Some(Command::Doctor(args)) => doctor(args),
        Some(Command::Setup(args)) => setup(args),
        Some(Command::Import(args)) => import(args),
        Some(Command::Recover(args)) => recover(args).await,
        Some(Command::Checkpoint(args)) => {
            let mut controller = Controller::load()?;
            let checkpoint = controller.checkpoint_session(&args.session).await?;
            println!(
                "saved recovery copy for {} at event {}",
                args.session, checkpoint.event_frontier
            );
            Ok(())
        }
        Some(Command::Login(args)) => login(args).await,
    }
}

/// Run the harness's own interactive login against a profile's canonical home.
/// Hel never sees the credential contents; it compares fingerprints before and
/// after so it can tell the operator whether anything changed.
async fn login(args: LoginArgs) -> Result<()> {
    let controller = Controller::load()?;
    let profile_id = resolve_login_profile(&controller.config, args.profile.as_deref())?;
    let profile = controller
        .config
        .profiles
        .get(&profile_id)
        .with_context(|| {
            format!(
                "unknown profile {profile_id:?}; configured profiles: {}",
                profile_ids(&controller.config)
            )
        })?;
    let marker = hel::hel_setup::harness_authentication_marker(profile.kind, &profile.home);
    let (before, _) = hel::hel_credentials::read_credential_file(profile.kind, &marker)?;
    let (program, arguments) = hel::hel_credentials::login_command(profile);

    println!(
        "Running `{program} {}` against {}.",
        arguments.join(" "),
        profile.home.display()
    );
    let status = tokio::process::Command::new(&program)
        .args(&arguments)
        .envs(&profile.environment)
        .env(profile.home_env(), &profile.home)
        .status()
        .await
        .with_context(|| {
            format!(
                "run `{program} {}` for profile {profile_id}",
                arguments.join(" ")
            )
        })?;

    let (after, _) = hel::hel_credentials::read_credential_file(profile.kind, &marker)?;
    if after.present && after.fingerprint != before.fingerprint {
        println!(
            "Credentials updated for profile {profile_id}. Live sessions pick them up within about a minute while the Hel TUI or server is running."
        );
    } else {
        println!("Credentials for profile {profile_id} are unchanged.");
    }
    if !status.success() {
        std::process::exit(status.code().unwrap_or(1));
    }
    Ok(())
}

fn profile_ids(config: &HelConfig) -> String {
    config
        .profiles
        .keys()
        .cloned()
        .collect::<Vec<_>>()
        .join(", ")
}

fn resolve_login_profile(config: &HelConfig, requested: Option<&str>) -> Result<String> {
    if let Some(profile) = requested {
        return Ok(profile.to_owned());
    }
    let mut profiles = config.profiles.keys();
    match (profiles.next(), profiles.next()) {
        (Some(only), None) => Ok(only.clone()),
        (Some(_), Some(_)) => bail!(
            "several profiles are configured; pass --profile with one of: {}",
            profile_ids(config)
        ),
        (None, _) => bail!("no harness profiles are configured; run `hel setup` first"),
    }
}

async fn recover(args: RecoverArgs) -> Result<()> {
    let mut controller = Controller::load()?;
    match args.command {
        RecoverCommand::Scan { json } => {
            let scan = controller.scan_orphan_workers(&ProcessExecutor);
            if json {
                println!("{}", serde_json::to_string_pretty(&scan)?);
            } else {
                for candidate in &scan.candidates {
                    let metadata = if candidate.ownership.is_some() {
                        "ownership verified"
                    } else {
                        "v1 resource; profile and bundle unknown"
                    };
                    println!(
                        "{}\t{}\t{}",
                        candidate.session_id, candidate.target_template_id, metadata
                    );
                }
                for warning in &scan.warnings {
                    eprintln!("warning: {warning}");
                }
            }
            Ok(())
        }
        RecoverCommand::Adopt {
            session,
            target,
            profile,
            bundle,
        } => {
            controller
                .adopt_orphan_worker(
                    &session,
                    &target,
                    profile.as_deref(),
                    bundle.as_deref(),
                    &ProcessExecutor,
                )
                .await?;
            println!("adopted worker {session}");
            Ok(())
        }
        RecoverCommand::Destroy {
            session,
            target,
            confirm,
        } => {
            controller.destroy_orphan_worker(&session, &target, &confirm, &ProcessExecutor)?;
            println!("destroyed orphan worker resource {session}");
            Ok(())
        }
    }
}

fn setup(args: SetupArgs) -> Result<()> {
    match args.command {
        Some(SetupCommand::Instructions { platform }) => {
            let platform = match platform {
                SetupPlatform::Linux => hel::hel_doctor::InstructionsPlatform::Linux,
                SetupPlatform::Macos => hel::hel_doctor::InstructionsPlatform::Macos,
            };
            print!("{}", hel::hel_doctor::setup_instructions(platform));
            Ok(())
        }
        None => match run_setup_dialog(&config_path())? {
            SetupOutcome::Written | SetupOutcome::Cancelled => Ok(()),
        },
    }
}

fn import(args: ImportArgs) -> Result<()> {
    match args.command {
        ImportCommand::Claude(args) => import_claude(args),
        ImportCommand::Codex(args) => import_codex(args),
        ImportCommand::Kimi(args) => import_kimi(args),
    }
}

fn import_claude(args: ClaudeImportArgs) -> Result<()> {
    let claude_home = claude_config_home()?;
    let selection = match args.session {
        Some(session) => ClaudeSessionSelection::NativeSessionId(session),
        None => ClaudeSessionSelection::Latest,
    };
    let source = locate_claude_session(&claude_home, &selection)?;
    println!(
        "Selected Claude session {} at {}",
        source.native_session_id,
        source.jsonl_path.display()
    );
    let transcript = read_claude_transcript(&source.jsonl_path)?;
    println!("Original cwd: {}", transcript.cwd.display());

    let mut config = HelConfig::load()?;
    let mut state = HelState::load()?;
    state.validate_against_config(&config)?;
    let targets = session_edit_targets(&transcript, &claude_home)?;
    let bundle_id =
        resolve_import_bundle(&mut config, &transcript, &targets, args.bundle.as_deref())?;
    if !confirm_import_safety(&targets, args.allow_dirty_local, args.allow_omitted_non_git)? {
        println!("Import cancelled; no Hel files were changed.");
        return Ok(());
    }
    let imported = import_claude_session(
        &config,
        &mut state,
        ClaudeImportRequest {
            claude_home: &claude_home,
            source: &source,
            transcript: &transcript,
            bundle_id: &bundle_id,
            profile_id: None,
            title: args.title.as_deref(),
            archive_directory: &sessions_dir(),
        },
    )?;
    // Both writes are atomic. The archive was already written and reopened by
    // `write_archive_atomic`; persist a synthesized config before the state
    // record that references it.
    config.save()?;
    persist_imported_session(
        state
            .sessions
            .get(&imported.session_id)
            .context("import did not add its session to controller state")?,
    )?;
    println!(
        "Imported {} as Hel session {} (bundle {}, archive {})",
        imported.native_session_id,
        imported.session_id,
        imported.bundle_id,
        imported.archive_path.display()
    );
    Ok(())
}

fn import_codex(args: CodexImportArgs) -> Result<()> {
    let codex_home = codex_config_home()?;
    let selection = match args.session {
        Some(session) => CodexSessionSelection::NativeSessionId(session),
        None => CodexSessionSelection::Latest,
    };
    let source = locate_codex_session(&codex_home, &selection)?;
    println!(
        "Selected Codex session {} at {}",
        source.native_session_id,
        source.jsonl_path.display()
    );
    let transcript = read_codex_transcript(&source.jsonl_path)?;
    println!("Original cwd: {}", transcript.cwd.display());

    let mut config = HelConfig::load()?;
    let mut state = HelState::load()?;
    state.validate_against_config(&config)?;
    let targets = session_edit_targets(&transcript, &codex_home)?;
    let bundle_id =
        resolve_import_bundle(&mut config, &transcript, &targets, args.bundle.as_deref())?;
    if !confirm_import_safety(&targets, args.allow_dirty_local, args.allow_omitted_non_git)? {
        println!("Import cancelled; no Hel files were changed.");
        return Ok(());
    }
    let imported = import_codex_session(
        &config,
        &mut state,
        CodexImportRequest {
            codex_home: &codex_home,
            source: &source,
            transcript: &transcript,
            bundle_id: &bundle_id,
            profile_id: None,
            title: args.title.as_deref(),
            archive_directory: &sessions_dir(),
        },
    )?;
    config.save()?;
    persist_imported_session(
        state
            .sessions
            .get(&imported.session_id)
            .context("import did not add its session to controller state")?,
    )?;
    println!(
        "Imported {} as Hel session {} (bundle {}, archive {})",
        imported.native_session_id,
        imported.session_id,
        imported.bundle_id,
        imported.archive_path.display()
    );
    Ok(())
}

fn import_kimi(args: KimiImportArgs) -> Result<()> {
    let kimi_home = kimi_config_home()?;
    let selection = match args.session {
        Some(session) => KimiSessionSelection::NativeSessionId(session),
        None => KimiSessionSelection::Latest,
    };
    let source = locate_kimi_session(&kimi_home, &selection)?;
    println!(
        "Selected Kimi session {} at {}",
        source.native_session_id,
        source.session_path.display()
    );
    let transcript = read_kimi_transcript(&source.session_path)?;
    println!("Original cwd: {}", transcript.cwd.display());

    let mut config = HelConfig::load()?;
    let mut state = HelState::load()?;
    state.validate_against_config(&config)?;
    let targets = session_edit_targets(&transcript, &kimi_home)?;
    let bundle_id =
        resolve_import_bundle(&mut config, &transcript, &targets, args.bundle.as_deref())?;
    if !confirm_import_safety(&targets, args.allow_dirty_local, args.allow_omitted_non_git)? {
        println!("Import cancelled; no Hel files were changed.");
        return Ok(());
    }
    let imported = import_kimi_session(
        &config,
        &mut state,
        KimiImportRequest {
            kimi_home: &kimi_home,
            source: &source,
            transcript: &transcript,
            bundle_id: &bundle_id,
            profile_id: None,
            title: args.title.as_deref(),
            archive_directory: &sessions_dir(),
        },
    )?;
    config.save()?;
    persist_imported_session(
        state
            .sessions
            .get(&imported.session_id)
            .context("import did not add its session to controller state")?,
    )?;
    println!(
        "Imported {} as Hel session {} (bundle {}, archive {})",
        imported.native_session_id,
        imported.session_id,
        imported.bundle_id,
        imported.archive_path.display()
    );
    Ok(())
}

fn resolve_import_bundle(
    config: &mut HelConfig,
    transcript: &hel::hel_import::ClaudeTranscript,
    targets: &hel::hel_import::SessionEditTargets,
    requested_bundle: Option<&str>,
) -> Result<String> {
    match resolve_bundle(config, &transcript.cwd, targets, requested_bundle)? {
        BundleResolution::Existing(bundle_id) => Ok(bundle_id),
        BundleResolution::Synthesized { id, bundle } => {
            config.bundles.insert(id.clone(), bundle);
            Ok(id)
        }
    }
}

fn confirm_import_safety(
    targets: &hel::hel_import::SessionEditTargets,
    allow_dirty: bool,
    allow_omitted_non_git: bool,
) -> Result<bool> {
    let issues = import_safety_issues(targets)?;
    let needs_dirty = !issues.dirty_git_roots.is_empty() && !allow_dirty;
    let needs_omitted = !issues.omitted_non_git_dirs.is_empty() && !allow_omitted_non_git;
    if !needs_dirty && !needs_omitted {
        return Ok(true);
    }
    if needs_dirty {
        eprintln!("These Git roots are dirty; Hel will archive their complete current state:");
        for (root, summary) in &issues.dirty_git_roots {
            eprintln!("  {} — {summary}", root.display());
        }
    }
    if needs_omitted {
        eprintln!("These edited directories are outside Git and cannot be included:");
        for directory in &issues.omitted_non_git_dirs {
            eprintln!("  {}", directory.display());
        }
    }
    if !io::stdin().is_terminal() {
        let flags = match (needs_dirty, needs_omitted) {
            (true, true) => "--allow-dirty and --allow-omitted-non-git",
            (true, false) => "--allow-dirty",
            (false, true) => "--allow-omitted-non-git",
            (false, false) => unreachable!(),
        };
        bail!("pass {flags} to acknowledge import safety warnings");
    }
    print!("Proceed? [y/N]: ");
    use std::io::Write as _;
    io::stdout().flush()?;
    let mut answer = String::new();
    io::stdin().read_line(&mut answer)?;
    Ok(matches!(
        answer.trim().to_ascii_lowercase().as_str(),
        "y" | "yes"
    ))
}

fn doctor(args: DoctorArgs) -> Result<()> {
    let checks = hel::hel_doctor::run_current(hel::hel_doctor::DoctorOptions { smoke: args.smoke });
    if args.json {
        println!("{}", serde_json::to_string_pretty(&checks)?);
    } else {
        hel::hel_doctor::render_human(&checks, &mut io::stdout())?;
    }
    if hel::hel_doctor::all_ready(&checks) {
        Ok(())
    } else {
        bail!("Hel has fixable prerequisites; run `hel doctor --json` and follow its remediations.")
    }
}

async fn run_server(args: ServerArgs) -> Result<()> {
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
                            recovery_observer.observe(hel::hel_recovery::RecoveryObservation {
                                session,
                                config: controller.config.clone(),
                                latest_completed_turn_ordinal:
                                    hel::hel_recovery::latest_completed_turn_ordinal(
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

fn persist_imported_session(session: &SessionRecord) -> Result<()> {
    hel::hel_database::save_session(session)?;
    let checkpoint = session
        .checkpoint
        .as_ref()
        .context("imported session has no checkpoint")?;
    let canonical = verify_archive_streaming(&checkpoint.archive_path)?.canonical_session;
    let materialized = materialized_session_from_canonical(session.id.clone(), &canonical)?;
    hel::hel_database::save_materialized_session(&materialized)
}

fn projected_queued_prompts(
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
        .refresh_profiles(quota_refresh_profiles(controller))
        .await;
}

fn quota_refresh_profiles(controller: &Controller) -> Vec<QuotaRefreshRequest> {
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
                environment,
                cwd: cwd.clone(),
            }
        })
        .collect()
}

fn spawn_dashboard_quota_refresher() -> (
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
    for quota in quotas.refresh_profiles(profiles.to_vec()).await {
        if updates.send(QuotaUpdate::Report(quota)).await.is_err() {
            return false;
        }
    }
    updates
        .send(QuotaUpdate::Finished { generation })
        .await
        .is_ok()
}

fn complete_manual_quota_refresh(
    pending_generation: &mut Option<u64>,
    completed_generation: u64,
) -> bool {
    if *pending_generation != Some(completed_generation) {
        return false;
    }
    *pending_generation = None;
    true
}

fn dashboard_worker_targets(controller: &Controller) -> Vec<WorkerPollTarget> {
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
fn credential_sync_targets(controller: &Controller) -> Vec<CredentialSyncTarget> {
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
const AUTH_FAILURE_SYNC_COOLDOWN: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingAuthFailure {
    ordinal: u64,
    profile_id: String,
}

/// Deduplicates the actor's sticky failure marker while retaining a newer
/// failure until its session cooldown expires.
#[derive(Debug, Default)]
struct AuthFailureSyncTracker {
    handled_ordinals: std::collections::BTreeMap<String, u64>,
    last_attempts: std::collections::BTreeMap<String, Instant>,
    pending: std::collections::BTreeMap<String, PendingAuthFailure>,
}

impl AuthFailureSyncTracker {
    fn observe(&mut self, session_id: &str, profile_id: &str, ordinal: u64) {
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

fn schedule_due_auth_failure_syncs(
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
struct CredentialSyncNotices {
    last_failures: std::collections::BTreeMap<(String, Option<String>), String>,
}

impl CredentialSyncNotices {
    /// Healthy no-op cycles stay out of the UI; only actions, new failures, and
    /// answers to an authentication failure are worth a notice.
    fn notice(&mut self, result: &hel::hel_credentials::CredentialSyncResult) -> Option<String> {
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

fn refresh_dashboard_poll_targets(
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

fn spawn_aws_resource_options_resolution(
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

fn spawn_dashboard_resource_poller() -> (
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

fn spawn_dashboard_capacity_poller() -> (
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
                    sampled_at_epoch_seconds: current_epoch_seconds(),
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

fn aggregate_aws_capacity(usages: &[DeploymentCapacityUsage]) -> Result<DeploymentCapacityUsage> {
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

fn spawn_dashboard_worker_poller() -> Result<(
    tokio::sync::watch::Sender<Vec<WorkerPollTarget>>,
    SessionManagerUpdates,
    SessionManagerControl,
)> {
    let channels = spawn_session_manager()?;
    Ok((channels.targets, channels.updates, channels.control))
}

fn current_epoch_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn request_dashboard_quota_refresh(
    controller: &Controller,
    dashboard: &mut DashboardState,
    profiles_tx: &tokio::sync::watch::Sender<QuotaRefreshBatch>,
) -> u64 {
    let profiles = quota_refresh_profiles(controller);
    dashboard.begin_quota_refresh(profiles.iter().map(|profile| profile.profile_id.clone()));
    let generation = profiles_tx.borrow().generation.wrapping_add(1).max(1);
    profiles_tx.send_replace(QuotaRefreshBatch {
        generation,
        profiles,
    });
    generation
}

fn apply_worker_poll_update(
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

fn apply_worker_record_update(
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

enum WorkerRecordPersistence {
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

fn spawn_materialized_session_projection(
    materialized: MaterializedSession,
    detached_after_event_ordinal: u64,
    previous: hel_tui::MaterializedProjectionCache,
    updates: tokio::sync::mpsc::UnboundedSender<DashboardIoUpdate>,
) {
    tokio::task::spawn_blocking(move || {
        let detail = PreparedMaterializedSessionDetail::from_materialized(
            materialized,
            detached_after_event_ordinal,
            previous,
        );
        let _ = updates.send(DashboardIoUpdate::MaterializedSessionProjection {
            detail: Box::new(detail),
        });
    });
}

fn spawn_lifecycle_reload(
    reload: LifecycleReload,
    updates: tokio::sync::mpsc::UnboundedSender<DashboardIoUpdate>,
) {
    tokio::task::spawn_blocking(move || {
        let result = Controller::load().map_err(|error| format!("{error:#}"));
        let _ = updates.send(DashboardIoUpdate::LifecycleReloaded(Box::new(
            LifecycleReloaded { reload, result },
        )));
    });
}

fn spawn_dashboard_rename(
    session_id: String,
    title: String,
    updates: tokio::sync::mpsc::UnboundedSender<DashboardIoUpdate>,
) {
    tokio::task::spawn_blocking(move || {
        let result = (|| -> Result<String> {
            let mut controller = Controller::load()?;
            controller.rename_session(&session_id, &title)
        })()
        .map_err(|error| format!("{error:#}"));
        let _ = updates.send(DashboardIoUpdate::RenameSession {
            session_id,
            title,
            result,
        });
    });
}

/// Longest the quit path waits for the detach write before giving up.
const DETACH_PERSIST_QUIT_TIMEOUT: Duration = Duration::from_secs(5);

/// Persist everything one detach produces: the read receipt and the unsent
/// draft. They describe the same moment and the same row, so one task keeps
/// them together and gives the quit path a single handle to await.
fn spawn_detached_session_state_persist(
    session_id: String,
    event_ordinal: u64,
    draft: String,
    updates: tokio::sync::mpsc::UnboundedSender<DashboardIoUpdate>,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_blocking(move || {
        let receipt =
            hel::hel_database::advance_detached_after_event_ordinal(&session_id, event_ordinal)
                .map(|_| ());
        // Save the draft even when the receipt was rejected: losing typed text
        // is worse than an out-of-date read marker.
        let saved_draft = hel::hel_database::set_session_draft_input(&session_id, &draft);
        let result = receipt
            .and(saved_draft)
            .map_err(|error| format!("{error:#}"));
        let _ = updates.send(DashboardIoUpdate::DetachedSessionState { session_id, result });
    })
}

fn spawn_create_bundle(
    source: String,
    updates: tokio::sync::mpsc::UnboundedSender<DashboardIoUpdate>,
) {
    tokio::task::spawn_blocking(move || {
        let result = (|| -> Result<CreatedBundleUpdate> {
            // Load fresh so a concurrent background save (e.g. an import
            // apply) is not clobbered by a stale UI-time config snapshot.
            let mut config = Controller::load()?.config;
            let bundle_id = create_quick_bundle(&mut config, &source)?;
            config.save()?;
            Ok(CreatedBundleUpdate { config, bundle_id })
        })()
        .map_err(|error| format!("{error:#}"));
        let _ = updates.send(DashboardIoUpdate::CreatedBundle {
            result: Box::new(result),
        });
    });
}

fn spawn_imported_session_apply(
    mut imported: DashboardImportSuccess,
    pending: PendingDashboardImport,
    updates: tokio::sync::mpsc::UnboundedSender<DashboardIoUpdate>,
) {
    tokio::task::spawn_blocking(move || {
        let result = (|| -> Result<ImportedDashboardSessionApply> {
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
        })()
        .map_err(|error| format!("{error:#}"));
        let _ = updates.send(DashboardIoUpdate::ImportedSessionApplied {
            result: Box::new(result),
        });
    });
}

fn checkpoint_archive_targets(
    controller: &Controller,
) -> std::collections::BTreeMap<String, PathBuf> {
    controller
        .state
        .sessions
        .values()
        .filter(|session| session.state == SessionState::Archived)
        .filter_map(|session| {
            session
                .checkpoint
                .as_ref()
                .map(|checkpoint| (session.id.clone(), checkpoint.archive_path.clone()))
        })
        .collect()
}

fn spawn_checkpoint_archive_size_refresh(
    generation: u64,
    targets: std::collections::BTreeMap<String, PathBuf>,
    updates: tokio::sync::mpsc::UnboundedSender<DashboardIoUpdate>,
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

fn spawn_dashboard_create_session(
    action: DashboardAction,
    updates: tokio::sync::mpsc::UnboundedSender<DashboardIoUpdate>,
    lifecycle_updates: tokio::sync::mpsc::UnboundedSender<LifecycleUpdate>,
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
            let executor = CancellableProcessExecutor::new(cancelled);
            runtime.block_on(controller.provision_session_controlled(&session_id, &executor))
        })()
        .map(|()| LifecycleSuccess::Created)
        .map_err(|error| format!("{error:#}"));
        let _ = lifecycle_updates.send(LifecycleUpdate { session_id, result });
    });
}

fn spawn_worker_diagnosis(
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

fn queued_prompt_projection(session: &MaterializedSession) -> Vec<hel::hel_worker::QueuedPrompt> {
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

fn apply_recovery_result(
    controller: &mut Controller,
    dashboard: &mut DashboardState,
    result: hel::hel_recovery::RecoveryResult,
) {
    let session_id = result.session_id.clone();
    let failure = result.outcome.as_ref().err().cloned();
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

fn merge_recovery_result(
    controller: &mut Controller,
    result: hel::hel_recovery::RecoveryResult,
) -> bool {
    let hel::hel_recovery::RecoveryResult {
        session_id,
        expected_target,
        outcome,
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
        Err(detail) => {
            // record_recovery_failure normally made this durable before the
            // result was published. Preserve the diagnostic in this view if
            // that write itself failed; later reloads remain authoritative.
            session.last_checkpoint_error = Some(detail);
        }
    }
    true
}

#[derive(Clone)]
struct PendingDashboardImport {
    profile_id: String,
    native_session_id: String,
    display_title: String,
}

#[derive(Clone, Copy)]
struct DashboardImportSafety {
    accepted: bool,
    include_untracked: bool,
}

struct ImportBundlePrompt {
    dirty_git_roots: Vec<String>,
    omitted_non_git_dirs: Vec<String>,
    has_untracked_files: bool,
}

struct DashboardImportSuccess {
    harness: &'static str,
    session_id: String,
    controller: Controller,
}

enum DashboardImportTaskResult {
    NeedsBundle(ImportBundlePrompt),
    Imported(DashboardImportSuccess),
    Cancelled,
}

enum DashboardImportUpdate {
    Progress {
        task_id: u64,
        step: usize,
        total: Option<usize>,
        message: String,
    },
    Finished {
        task_id: u64,
        pending: PendingDashboardImport,
        result: Result<DashboardImportTaskResult>,
    },
}

struct ActiveDashboardImport {
    task_id: u64,
    cancelled: Arc<AtomicBool>,
}

struct ActiveLifecycleOperation {
    cancelled: Arc<AtomicBool>,
    kind: SessionOperationKind,
}

enum LifecycleSuccess {
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

struct LifecycleUpdate {
    session_id: String,
    result: std::result::Result<LifecycleSuccess, String>,
}

struct RegisteredDashboardSession {
    session: SessionRecord,
    cancelled: Arc<AtomicBool>,
}

enum DashboardCreateSessionUpdate {
    DirtyLocal {
        action: DashboardAction,
        repositories: Vec<String>,
    },
    Registered(Box<RegisteredDashboardSession>),
    Failed(String),
}

struct ImportedDashboardSessionApply {
    harness: &'static str,
    native_session_id: String,
    session: SessionRecord,
    bundle_id: String,
    bundle: ProjectBundle,
}

struct CreatedBundleUpdate {
    config: HelConfig,
    bundle_id: String,
}

struct LifecycleReload {
    update: LifecycleUpdate,
    operation: Option<ActiveLifecycleOperation>,
}

struct LifecycleReloaded {
    reload: LifecycleReload,
    result: std::result::Result<Controller, String>,
}

fn interrupted_close_session_ids(controller: &Controller) -> Vec<String> {
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

fn spawn_interrupted_close_recovery(
    session_id: String,
    session_manager: SessionManagerControl,
    recovery_observer: hel::hel_recovery::RecoveryObserver,
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

enum DashboardIoUpdate {
    WorkerRecordPersistence {
        session_id: String,
        operation: WorkerRecordPersistence,
        result: std::result::Result<(), String>,
    },
    MaterializedSessionProjection {
        detail: Box<PreparedMaterializedSessionDetail>,
    },
    CreateSession(Box<DashboardCreateSessionUpdate>),
    RenameSession {
        session_id: String,
        title: String,
        result: std::result::Result<String, String>,
    },
    DetachedSessionState {
        session_id: String,
        result: std::result::Result<(), String>,
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
        sizes: std::collections::BTreeMap<String, Option<u64>>,
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
        result: std::result::Result<(), String>,
    },
    ProjectValidation {
        directory: String,
        result: std::result::Result<(), String>,
    },
}

fn startup_greeting(controller: &Controller) -> String {
    let active = controller
        .state
        .sessions
        .values()
        .filter(|session| session.state.is_active())
        .collect::<Vec<_>>();
    let raw_localhost_active = active
        .iter()
        .any(|session| matches!(session.target, Some(TargetLocator::LocalBare { .. })));
    let container_active = active.iter().any(|session| {
        matches!(
            session.target,
            Some(
                TargetLocator::LocalPodman { .. }
                    | TargetLocator::AppleContainer { .. }
                    | TargetLocator::SshPodman { .. }
            )
        )
    });
    let remote_active = active.iter().any(|session| {
        matches!(
            session.target,
            Some(
                TargetLocator::AwsEc2 { .. }
                    | TargetLocator::SshBare { .. }
                    | TargetLocator::SshPodman { .. }
            )
        )
    });
    let facts = GreetingFacts {
        first_name: git_output(&["config", "--get", "user.name"])
            .and_then(|name| name.split_whitespace().next().map(str::to_owned)),
        returning: !controller.state.sessions.is_empty(),
        profile_count: controller.config.profiles.len(),
        active_sessions: active.len(),
        paused_sessions: controller
            .state
            .sessions
            .values()
            .filter(|session| session.state == SessionState::Archived)
            .count(),
        raw_localhost_active,
        container_active,
        remote_active,
        repository: repository_greeting_facts(),
        ..GreetingFacts::default()
    };
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    hel::hel_greeting::select(&facts, seed)
}

fn repository_greeting_facts() -> Option<RepositoryGreetingFacts> {
    git_output(&["rev-parse", "--is-inside-work-tree"]).filter(|answer| answer == "true")?;
    let status = git_output(&["status", "--porcelain=v1"])?;
    let conflicted = git_output(&["diff", "--name-only", "--diff-filter=U"])
        .is_some_and(|paths| !paths.is_empty());
    let (ahead, behind) =
        git_output(&["rev-list", "--left-right", "--count", "HEAD...@{upstream}"])
            .and_then(|counts| {
                let mut counts = counts.split_whitespace();
                Some((
                    counts.next()?.parse::<u64>().ok()?,
                    counts.next()?.parse::<u64>().ok()?,
                ))
            })
            .unwrap_or_default();
    Some(RepositoryGreetingFacts {
        clean: status.is_empty(),
        dirty: !status.is_empty(),
        ahead: ahead > 0 && behind == 0,
        behind: behind > 0 && ahead == 0,
        diverged_or_conflicted: conflicted || (ahead > 0 && behind > 0),
    })
}

fn git_output(arguments: &[&str]) -> Option<String> {
    let output = std::process::Command::new("git")
        .args(arguments)
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// Replaces the dashboard's event stream, dropping the old one first.
///
/// Once polled, an `EventStream` leaves a reader thread inside crossterm's
/// internal reader, where it holds the reader lock and consumes terminal input.
/// Anything else that reads the terminal needs that thread gone first. Building
/// the replacement takes the same lock, which is why the old stream goes first.
fn restart_event_stream(events: event::EventStream) -> event::EventStream {
    drop(events);
    event::EventStream::new()
}

/// Which view the one loop is drawing. The chat view is data rather than a
/// nested loop, so its background feeds stay live while the session list is on
/// screen and returning to a session is only a redraw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum View {
    Dashboard,
    /// Only valid while the loop holds an `ActiveChat`.
    Chat,
}

/// Builds a chat view for one session: its identity, the other sessions it
/// reports activity for, and its recovery context.
async fn open_chat_view(
    controller: &Controller,
    session_id: &str,
    sessions: &SessionManagerControl,
    recovery_observer: &hel::hel_recovery::RecoveryObserver,
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
    let recovery_context = hel::hel_recovery::RecoveryContext {
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
    ))
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
    updates: &tokio::sync::mpsc::UnboundedSender<DashboardIoUpdate>,
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
    Some(spawn_detached_session_state_persist(
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
        Event::Mouse(mouse) => {
            dashboard.handle_mouse(mouse);
            DashboardAction::None
        }
        // Resize and focus changes only need the redraw.
        _ => DashboardAction::None,
    }
}

async fn run_dashboard() -> Result<()> {
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        println!("Welcome to Hel");
        println!("Run `hel doctor` for non-interactive validation.");
        return Ok(());
    }

    let mut controller = Controller::load()?;
    let greeting = startup_greeting(&controller);
    let mut dashboard = DashboardState::new(
        controller.config.clone(),
        controller.state.clone(),
        std::collections::BTreeMap::new(),
    );
    // One notifications bar for the whole process: the dashboard and every
    // chat view opened below report through the same shared handle.
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
                dashboard.set_notice("Setup complete. Press Ctrl+N to start your first session.");
            }
            SetupOutcome::Cancelled => return Ok(()),
        }
    }
    let (quota_profiles_tx, mut quota_updates_rx) = spawn_dashboard_quota_refresher();
    request_dashboard_quota_refresh(&controller, &mut dashboard, &quota_profiles_tx);
    let (worker_targets_tx, mut worker_updates_rx, worker_commands_tx) =
        spawn_dashboard_worker_poller()?;
    worker_targets_tx.send_replace(dashboard_worker_targets(&controller));
    let mut recovery = hel::hel_recovery::RecoveryCoordinator::spawn(worker_commands_tx.clone());
    let recovery_observer = recovery.observer();
    let (lifecycle_updates_tx, mut lifecycle_updates_rx) =
        tokio::sync::mpsc::unbounded_channel::<LifecycleUpdate>();
    let mut lifecycle_operations =
        std::collections::BTreeMap::<String, ActiveLifecycleOperation>::new();
    for session_id in interrupted_close_session_ids(&controller) {
        let state = controller.state.sessions[&session_id].state;
        let kind = if state == SessionState::Destroying {
            SessionOperationKind::Destroying
        } else {
            SessionOperationKind::Pausing
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
    let mut credential_sync = CredentialSyncCoordinator::spawn();
    let credential_sync_handle = credential_sync.handle();
    let mut auth_failure_syncs = AuthFailureSyncTracker::default();
    let mut credential_sync_notices = CredentialSyncNotices::default();
    let (resource_targets_tx, resource_triggers_tx, mut resource_updates_rx) =
        spawn_dashboard_resource_poller();
    let (capacity_targets_tx, mut capacity_updates_rx) = spawn_dashboard_capacity_poller();
    let (aws_resource_options_tx, mut aws_resource_options_rx) =
        tokio::sync::mpsc::unbounded_channel::<(
            String,
            std::result::Result<Vec<SessionResourceAllocation>, String>,
        )>();
    let mut resolving_aws_resource_options = std::collections::BTreeSet::new();
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
    let (import_updates_tx, mut import_updates_rx) =
        tokio::sync::mpsc::channel::<(u64, ImportProfileOption)>(32);
    let (import_task_tx, mut import_task_rx) =
        tokio::sync::mpsc::channel::<DashboardImportUpdate>(8);
    let mut pending_import = None;
    let mut import_discovery_id = 0_u64;
    let mut quit_detached = false;
    let mut next_import_task_id = 0_u64;
    let mut active_import: Option<ActiveDashboardImport> = None;
    let (dashboard_io_tx, mut dashboard_io_rx) =
        tokio::sync::mpsc::unbounded_channel::<DashboardIoUpdate>();
    let mut worker_diagnoses = WorkerDiagnosisTracker::default();
    let termination = hel::termination::Coordinator::install().token();
    let mut manual_quota_refresh_generation = None;
    let mut checkpoint_archive_targets_seen = std::collections::BTreeMap::new();
    let mut checkpoint_archive_generation = 0_u64;
    let mut events = event::EventStream::new();
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
    // A closed feed reports `None` for ever, which would leave its arm
    // permanently ready. Each flag retires its own arm instead.
    let (mut quota_open, mut sessions_open, mut recovery_open, mut credentials_open) =
        (true, true, true, true);
    let (mut resource_open, mut capacity_open, mut aws_options_open, mut import_profiles_open) =
        (true, true, true, true);
    let (mut import_tasks_open, mut lifecycle_open, mut dashboard_io_open) = (true, true, true);
    // The first pass always draws; after that a redraw needs a wakeup, and the
    // poll targets are recomputed only after the controller may have changed.
    let mut dirty = true;
    let mut controller_changed = true;
    let mut view = View::Dashboard;
    // At most one chat stays warm: the session opened last. Its feeds keep
    // running off screen, so reopening it costs a draw rather than a rebuild.
    let mut active_chat: Option<hel::hel_chat::ActiveChat> = None;

    loop {
        if controller_changed {
            controller_changed = false;
            let checkpoint_archive_targets = checkpoint_archive_targets(&controller);
            if checkpoint_archive_targets != checkpoint_archive_targets_seen {
                checkpoint_archive_targets_seen = checkpoint_archive_targets.clone();
                checkpoint_archive_generation =
                    checkpoint_archive_generation.wrapping_add(1).max(1);
                spawn_checkpoint_archive_size_refresh(
                    checkpoint_archive_generation,
                    checkpoint_archive_targets,
                    dashboard_io_tx.clone(),
                );
            }
            let capacity_targets = controller.deployment_capacity_targets();
            if *capacity_targets_tx.borrow() != capacity_targets {
                capacity_targets_tx.send_replace(capacity_targets.clone());
                dashboard.set_deployment_capacity_targets(capacity_targets);
                dirty = true;
            }
        }
        if dirty {
            dirty = false;
            match (view, active_chat.as_mut()) {
                (View::Chat, Some(chat)) => {
                    terminal.terminal.draw(|frame| chat.draw(frame))?;
                }
                _ => {
                    terminal
                        .terminal
                        .draw(|frame| render(frame, &mut dashboard))?;
                }
            }
        }
        let mut action = DashboardAction::None;
        let mut chat_outcome = hel::hel_chat::ChatEventOutcome::None;
        let mut quota_update = None;
        let mut session_update = None;
        let mut recovery_result = None;
        let mut credential_result = None;
        let mut resource_update = None;
        let mut capacity_update = None;
        let mut aws_options = None;
        let mut import_profile = None;
        let mut import_task_update = None;
        let mut lifecycle_update = None;
        let mut dashboard_io_update = None;
        // The winning arm takes the message that woke the loop; the drains
        // below batch whatever is queued behind it, so one wakeup is one draw.
        tokio::select! {
            _ = termination.cancelled() => break,
            event = events.next() => {
                let Some(event) = event else { break };
                let mut event = event?;
                // Key repeats and pastes arrive as several ready events. The
                // buffered ones are handled before drawing, but the first event
                // that asks for work ends the batch so that dispatch still
                // follows input order.
                loop {
                    let batched = match (view, active_chat.as_mut()) {
                        (View::Chat, Some(chat)) => {
                            chat_outcome = chat.handle_event(event);
                            matches!(chat_outcome, hel::hel_chat::ChatEventOutcome::None)
                        }
                        _ => {
                            action = dashboard_event_action(&mut dashboard, event);
                            controller_changed = true;
                            matches!(action, DashboardAction::None)
                        }
                    };
                    if !batched {
                        break;
                    }
                    // A zero timeout rather than a no-op waker: `EventStream`
                    // arms its reader thread with the waker it was last polled
                    // with, so a no-op waker here would swallow the next wakeup.
                    let Ok(Some(next)) =
                        tokio::time::timeout(Duration::ZERO, events.next()).await
                    else {
                        break;
                    };
                    event = next?;
                }
                dirty = true;
            }
            // The warm chat's own feeds: remote command results, its clipboard
            // and history I/O, dictation, and the session view. They run
            // whether or not the chat is on screen, which is what keeps an
            // off-screen chat current.
            () = hel::hel_chat::ActiveChat::pump(active_chat.as_mut()) => {
                dirty |= view == View::Chat;
            }
            update = quota_updates_rx.recv(), if quota_open => match update {
                Some(update) => {
                    quota_update = Some(update);
                    dirty = true;
                }
                None => quota_open = false,
            },
            update = worker_updates_rx.recv(), if sessions_open => match update {
                Some(update) => {
                    session_update = Some(update);
                    dirty = true;
                }
                None => sessions_open = false,
            },
            result = recovery.result(), if recovery_open => match result {
                Some(result) => {
                    recovery_result = Some(result);
                    dirty = true;
                }
                None => recovery_open = false,
            },
            result = credential_sync.result(), if credentials_open => match result {
                Some(result) => {
                    credential_result = Some(result);
                    dirty = true;
                }
                None => credentials_open = false,
            },
            update = resource_updates_rx.recv(), if resource_open => match update {
                Some(update) => {
                    resource_update = Some(update);
                    dirty = true;
                }
                None => resource_open = false,
            },
            update = capacity_updates_rx.recv(), if capacity_open => match update {
                Some(update) => {
                    capacity_update = Some(update);
                    dirty = true;
                }
                None => capacity_open = false,
            },
            options = aws_resource_options_rx.recv(), if aws_options_open => match options {
                Some(options) => {
                    aws_options = Some(options);
                    dirty = true;
                }
                None => aws_options_open = false,
            },
            profile = import_updates_rx.recv(), if import_profiles_open => match profile {
                Some(profile) => {
                    import_profile = Some(profile);
                    dirty = true;
                }
                None => import_profiles_open = false,
            },
            update = import_task_rx.recv(), if import_tasks_open => match update {
                Some(update) => {
                    import_task_update = Some(update);
                    dirty = true;
                }
                None => import_tasks_open = false,
            },
            update = lifecycle_updates_rx.recv(), if lifecycle_open => match update {
                Some(update) => {
                    lifecycle_update = Some(update);
                    dirty = true;
                }
                None => lifecycle_open = false,
            },
            update = dashboard_io_rx.recv(), if dashboard_io_open => match update {
                Some(update) => {
                    dashboard_io_update = Some(update);
                    dirty = true;
                }
                None => dashboard_io_open = false,
            },
            // Turn clocks, countdowns, and credential-sync backoffs move on
            // their own, so the dashboard redraws once a second regardless.
            // The chat redraws only when its own time-driven text has moved:
            // a running turn clock in the session header, or the checkpoint
            // title.
            _ = clock_tick.tick() => {
                dirty |= match (view, active_chat.as_ref()) {
                    (View::Chat, Some(chat)) => chat.needs_clock_tick(),
                    _ => true,
                };
            }
            // The import dialog reports how long a step has stalled; it needs a
            // faster tick, and only while it is on screen.
            _ = import_tick.tick(), if active_import.is_some() && view == View::Dashboard => {
                dirty = true;
            }
        }
        while let Some(update) = quota_update
            .take()
            .or_else(|| quota_updates_rx.try_recv().ok())
        {
            match update {
                QuotaUpdate::Refreshing { profile_ids } => {
                    dashboard.begin_quota_refresh(profile_ids)
                }
                QuotaUpdate::Report(quota) => dashboard.apply_quota(quota),
                QuotaUpdate::Finished { generation } => {
                    if complete_manual_quota_refresh(
                        &mut manual_quota_refresh_generation,
                        generation,
                    ) {
                        dashboard.replace_notice_if(QUOTA_REFRESH_NOTICE, QUOTA_REFRESHED_NOTICE);
                    }
                }
            }
        }
        while let Some(update) = session_update
            .take()
            .or_else(|| worker_updates_rx.try_recv().ok())
        {
            controller_changed = true;
            let session_id = update.session_id.clone();
            let connected = update.view.connected;
            // Only unreachable relays drive the worker diagnostics flow.
            let connection_error = match update.view.error.as_ref() {
                Some(ViewError::Unreachable(detail)) => Some(detail.clone()),
                Some(ViewError::ProjectionIntegrity(_)) | None => None,
            };
            if let Some(snapshot) = update.view.snapshot.as_ref()
                && let Some(session) = controller.state.sessions.get(&session_id).cloned()
            {
                if let Some(ordinal) = snapshot.latest_auth_failure_ordinal {
                    auth_failure_syncs.observe(&session_id, &session.last_profile, ordinal);
                }
                // Queued, never awaited: the copy decision belongs to the
                // coordinator, and the dashboard loop must stay free to draw.
                recovery_observer.observe(hel::hel_recovery::RecoveryObservation {
                    session,
                    config: controller.config.clone(),
                    latest_completed_turn_ordinal: hel::hel_recovery::latest_completed_turn_ordinal(
                        &snapshot.materialized,
                    ),
                    execution: snapshot.materialized.execution,
                });
            }
            let materialized = update
                .view
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.materialized.clone());
            match apply_worker_poll_update(
                &mut controller,
                &mut dashboard,
                update,
                &dashboard_io_tx,
            ) {
                Ok(true) => {
                    let _ = resource_triggers_tx.try_send(session_id.clone());
                    if let Some(materialized) = materialized {
                        let detached_after_event_ordinal = controller
                            .state
                            .sessions
                            .get(&session_id)
                            .map_or(0, |session| session.detached_after_event_ordinal);
                        let previous = dashboard.take_projection_cache(&session_id);
                        spawn_materialized_session_projection(
                            materialized,
                            detached_after_event_ordinal,
                            previous,
                            dashboard_io_tx.clone(),
                        );
                    }
                }
                Ok(false) => {}
                Err(error) => {
                    dashboard.set_notice(format!("Could not save harness title: {error:#}"));
                }
            }
            if let Some(episode_id) =
                worker_diagnoses.observe(&session_id, connected, connection_error)
            {
                spawn_worker_diagnosis(
                    &controller,
                    session_id,
                    episode_id,
                    dashboard_io_tx.clone(),
                );
            }
        }
        schedule_due_auth_failure_syncs(
            &mut auth_failure_syncs,
            &credential_sync_handle,
            Instant::now(),
        );
        while let Some(result) = recovery_result.take().or_else(|| recovery.try_result()) {
            controller_changed = true;
            apply_recovery_result(&mut controller, &mut dashboard, result);
        }
        while let Some(result) = credential_result
            .take()
            .or_else(|| credential_sync.try_result())
        {
            if let Some(notice) = credential_sync_notices.notice(&result) {
                dashboard.set_notice(notice);
            }
        }
        while let Some(update) = resource_update
            .take()
            .or_else(|| resource_updates_rx.try_recv().ok())
        {
            dashboard.apply_resource_usage(&update.session_id, update.usage);
        }
        while let Some(update) = capacity_update
            .take()
            .or_else(|| capacity_updates_rx.try_recv().ok())
        {
            dashboard.apply_deployment_capacity(
                &update.target_id,
                update.result,
                update.sampled_at_epoch_seconds,
            );
        }
        while let Some((target_id, result)) = aws_options
            .take()
            .or_else(|| aws_resource_options_rx.try_recv().ok())
        {
            resolving_aws_resource_options.remove(&target_id);
            dashboard.apply_aws_resource_options(&target_id, result);
        }
        while let Some((discovery_id, profile)) = import_profile
            .take()
            .or_else(|| import_updates_rx.try_recv().ok())
        {
            dashboard.apply_import_profile(discovery_id, profile);
        }
        while let Some(update) = import_task_update
            .take()
            .or_else(|| import_task_rx.try_recv().ok())
        {
            match update {
                DashboardImportUpdate::Progress {
                    task_id,
                    step,
                    total,
                    message,
                } => {
                    if active_import
                        .as_ref()
                        .is_some_and(|active| active.task_id == task_id)
                    {
                        dashboard.update_import_progress(step, total, message);
                    }
                }
                DashboardImportUpdate::Finished {
                    task_id,
                    pending,
                    result,
                } => {
                    if active_import
                        .as_ref()
                        .is_none_or(|active| active.task_id != task_id)
                    {
                        continue;
                    }
                    active_import = None;
                    match result {
                        Ok(DashboardImportTaskResult::NeedsBundle(prompt)) => {
                            pending_import = Some(pending);
                            dashboard.show_import_bundle_confirmation(
                                prompt.dirty_git_roots,
                                prompt.omitted_non_git_dirs,
                                prompt.has_untracked_files,
                            );
                        }
                        Ok(DashboardImportTaskResult::Imported(imported)) => {
                            dashboard.finish_import();
                            dashboard.set_notice("Saving imported session…");
                            spawn_imported_session_apply(
                                imported,
                                pending,
                                dashboard_io_tx.clone(),
                            );
                        }
                        Ok(DashboardImportTaskResult::Cancelled) => {
                            dashboard.finish_import();
                            dashboard.set_notice("Import cancelled; no Hel files were changed.");
                        }
                        Err(error) => {
                            dashboard.finish_import();
                            dashboard.set_notice(format!("Import failed: {error:#}"));
                        }
                    }
                }
            }
        }
        while let Some(update) = lifecycle_update
            .take()
            .or_else(|| lifecycle_updates_rx.try_recv().ok())
        {
            controller_changed = true;
            let session_id = update.session_id.clone();
            let operation = lifecycle_operations.remove(&session_id);
            dashboard.finish_session_operation(&session_id);
            spawn_lifecycle_reload(
                LifecycleReload { update, operation },
                dashboard_io_tx.clone(),
            );
        }
        while let Some(update) = dashboard_io_update
            .take()
            .or_else(|| dashboard_io_rx.try_recv().ok())
        {
            controller_changed = true;
            match update {
                DashboardIoUpdate::WorkerRecordPersistence {
                    session_id,
                    operation,
                    result,
                } => {
                    if let Err(error) = result {
                        match operation {
                            WorkerRecordPersistence::AcpTitle { .. } => dashboard
                                .set_notice(format!("Could not save harness title: {error}")),
                            WorkerRecordPersistence::RelayReconnect => {
                                dashboard.set_notice(format!(
                                    "Could not save relay reconnect for {}: {error}",
                                    short_id(&session_id)
                                ))
                            }
                        }
                    }
                }
                DashboardIoUpdate::MaterializedSessionProjection { detail } => {
                    dashboard.apply_prepared_materialized_session(*detail);
                }
                DashboardIoUpdate::CreateSession(update) => match *update {
                    DashboardCreateSessionUpdate::DirtyLocal {
                        action,
                        repositories,
                    } => dashboard.show_dirty_local_confirmation(action, repositories),
                    DashboardCreateSessionUpdate::Registered(registered) => {
                        let registered = *registered;
                        let session_id = registered.session.id.clone();
                        controller
                            .state
                            .sessions
                            .insert(session_id.clone(), registered.session);
                        dashboard.set_state(controller.state.clone());
                        dashboard.begin_session_operation(
                            session_id.clone(),
                            SessionOperationKind::Launching,
                            None,
                        );
                        dashboard.set_notice(format!("Launching {}…", short_id(&session_id)));
                        lifecycle_operations.insert(
                            session_id,
                            ActiveLifecycleOperation {
                                cancelled: registered.cancelled,
                                kind: SessionOperationKind::Launching,
                            },
                        );
                    }
                    DashboardCreateSessionUpdate::Failed(error) => {
                        dashboard.set_notice(format!("Could not create session: {error}"));
                    }
                },
                DashboardIoUpdate::RenameSession {
                    session_id,
                    title,
                    result,
                } => match result {
                    Ok(title) => {
                        if let Some(session) = controller.state.sessions.get_mut(&session_id) {
                            session.session_title_override = Some(title.clone());
                            session.updated_at = chrono::Utc::now().to_rfc3339();
                        }
                        dashboard.set_state(controller.state.clone());
                        dashboard.set_notice(format!("Renamed session to {title}"));
                    }
                    Err(error) => {
                        dashboard.set_notice(format!("Rename failed for {title}: {error}"));
                    }
                },
                DashboardIoUpdate::DetachedSessionState { session_id, result } => {
                    if let Err(error) = result {
                        dashboard.set_notice(format!(
                            "Could not save draft and read status for {}: {error}",
                            short_id(&session_id)
                        ));
                    }
                }
                DashboardIoUpdate::CreatedBundle { result } => match *result {
                    Ok(created) => {
                        controller.config = created.config;
                        let followup = dashboard
                            .apply_created_bundle(controller.config.clone(), &created.bundle_id);
                        if let DashboardAction::ResolveAwsResourceOptions {
                            target_template_ids,
                        } = followup
                        {
                            for target_template_id in target_template_ids {
                                if resolving_aws_resource_options.insert(target_template_id.clone())
                                {
                                    spawn_aws_resource_options_resolution(
                                        controller.config.clone(),
                                        target_template_id,
                                        aws_resource_options_tx.clone(),
                                    );
                                }
                            }
                        }
                    }
                    Err(error) => {
                        dashboard.set_notice(format!("Could not create bundle: {error}"));
                    }
                },
                DashboardIoUpdate::ImportedSessionApplied { result } => match *result {
                    Ok(applied) => {
                        controller
                            .config
                            .bundles
                            .insert(applied.bundle_id, applied.bundle);
                        controller
                            .state
                            .sessions
                            .insert(applied.session.id.clone(), applied.session);
                        dashboard.set_config(controller.config.clone());
                        dashboard.set_state(controller.state.clone());
                        refresh_dashboard_poll_targets(
                            &controller,
                            &worker_targets_tx,
                            &resource_targets_tx,
                            &credential_sync_handle,
                            &lifecycle_operations.keys().cloned().collect(),
                        );
                        dashboard.set_notice(format!(
                            "Imported {} session {}.",
                            applied.harness, applied.native_session_id
                        ));
                    }
                    Err(error) => dashboard.set_notice(format!("Import failed: {error}")),
                },
                DashboardIoUpdate::LifecycleReloaded(reloaded) => {
                    let reloaded = *reloaded;
                    let LifecycleReload { update, operation } = reloaded.reload;
                    let session_id = update.session_id;
                    match reloaded.result {
                        Ok(loaded) => {
                            controller = loaded;
                            dashboard.set_state(controller.state.clone());
                            match update.result {
                                Ok(LifecycleSuccess::Created) => {
                                    dashboard.select_active_session(&session_id);
                                    dashboard.set_notice(format!(
                                        "Session {} is ready; press Enter to open it",
                                        short_id(&session_id)
                                    ));
                                    request_dashboard_quota_refresh(
                                        &controller,
                                        &mut dashboard,
                                        &quota_profiles_tx,
                                    );
                                }
                                Ok(LifecycleSuccess::Resumed {
                                    profile_id,
                                    target_id,
                                    materialized,
                                }) => {
                                    dashboard.apply_materialized_session(&materialized);
                                    dashboard.select_active_session(&session_id);
                                    dashboard.set_notice(format!(
                                        "Resumed {} with {profile_id} on {target_id}",
                                        short_id(&session_id)
                                    ));
                                    request_dashboard_quota_refresh(
                                        &controller,
                                        &mut dashboard,
                                        &quota_profiles_tx,
                                    );
                                }
                                Ok(LifecycleSuccess::Closed) => {
                                    dashboard
                                        .set_notice(format!("Paused {}", short_id(&session_id)));
                                }
                                Ok(LifecycleSuccess::Destroyed) => dashboard.set_notice(format!(
                                    "Destroyed {} without an archive",
                                    short_id(&session_id)
                                )),
                                Ok(LifecycleSuccess::DeletedActive) => {
                                    dashboard.set_notice(format!(
                                        "Deleted active session {} without checkpointing",
                                        short_id(&session_id)
                                    ))
                                }
                                Ok(LifecycleSuccess::DeletedArchived) => {
                                    dashboard.set_notice(format!(
                                        "Permanently deleted paused session {}",
                                        short_id(&session_id)
                                    ))
                                }
                                Err(error) => {
                                    if operation.as_ref().is_some_and(|operation| {
                                        operation.kind == SessionOperationKind::Pausing
                                    }) {
                                        dashboard.show_close_failure(session_id.clone(), error);
                                    } else {
                                        let label =
                                            operation.as_ref().map_or("Operation", |operation| {
                                                operation.kind.label()
                                            });
                                        dashboard.set_notice(format!("{label} failed: {error}"));
                                    }
                                }
                            }
                            refresh_dashboard_poll_targets(
                                &controller,
                                &worker_targets_tx,
                                &resource_targets_tx,
                                &credential_sync_handle,
                                &lifecycle_operations.keys().cloned().collect(),
                            );
                        }
                        Err(error) => dashboard
                            .set_notice(format!("Could not reload completed operation: {error}")),
                    }
                }
                DashboardIoUpdate::CheckpointArchiveSizes { generation, sizes } => {
                    if generation == checkpoint_archive_generation {
                        dashboard.apply_checkpoint_archive_sizes(sizes);
                    }
                }
                DashboardIoUpdate::WorkerDiagnosis {
                    session_id,
                    episode_id,
                    result,
                } => {
                    let completion = worker_diagnoses.finish(&session_id, episode_id);
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
                        dashboard
                            .set_notice(format!("Session {}: {message}", short_id(&session_id)));
                    } else if let Err(error) = &result {
                        tracing::warn!(%session_id, "stale worker diagnosis task failed: {error}");
                    }
                    if let Some(restart_episode) = completion.restart_episode {
                        spawn_worker_diagnosis(
                            &controller,
                            session_id,
                            restart_episode,
                            dashboard_io_tx.clone(),
                        );
                    }
                }
                DashboardIoUpdate::MountCompletions { prefix, result } => match result {
                    Ok(candidates) => dashboard.apply_mount_source_completions(&prefix, candidates),
                    Err(error) => dashboard.set_notice(format!("Path completion failed: {error}")),
                },
                DashboardIoUpdate::MountValidation { source, result } => {
                    dashboard.apply_mount_source_validation(&source, result)
                }
                DashboardIoUpdate::ProjectValidation { directory, result } => {
                    dashboard.apply_project_directory_validation(&directory, result)
                }
            }
        }
        match chat_outcome {
            hel::hel_chat::ChatEventOutcome::None | hel::hel_chat::ChatEventOutcome::Handled => {}
            hel::hel_chat::ChatEventOutcome::Back {
                last_seen_event_ordinal,
            }
            | hel::hel_chat::ChatEventOutcome::QuitDetach {
                last_seen_event_ordinal,
            } => {
                // The chat stays warm behind the session list, so its feeds
                // keep following the worker and reopening it costs a draw.
                view = View::Dashboard;
                dirty = true;
                // The warm chat goes on holding this input in memory, so save
                // it here: a quit or a crash while it is off screen would
                // otherwise lose it.
                let detached = active_chat
                    .as_ref()
                    .map(|chat| (chat.session_id().to_owned(), chat.draft().to_owned()));
                let persist = detached.map(|(session_id, draft)| {
                    record_chat_detach_state(
                        &mut controller,
                        &mut dashboard,
                        &session_id,
                        last_seen_event_ordinal,
                        &draft,
                        &dashboard_io_tx,
                    )
                });
                if matches!(
                    chat_outcome,
                    hel::hel_chat::ChatEventOutcome::QuitDetach { .. }
                ) {
                    // Quitting leaves this loop for process exit, so the detach
                    // write has to land first. Bounded so a stuck database
                    // cannot hang the quit.
                    if let Some(persist) = persist.flatten() {
                        let _ = tokio::time::timeout(DETACH_PERSIST_QUIT_TIMEOUT, persist).await;
                    }
                    quit_detached = true;
                    break;
                }
            }
        }
        match action {
            DashboardAction::None => {}
            DashboardAction::QuitDetach => {
                for operation in lifecycle_operations.values() {
                    operation.cancelled.store(true, Ordering::Release);
                }
                if let Some(active) = active_import.as_ref() {
                    active.cancelled.store(true, Ordering::Release);
                }
                quit_detached = true;
                break;
            }
            DashboardAction::OpenConfig => {
                // The setup dialog reads the terminal itself, and a live event
                // stream's reader thread consumes terminal input. Retire the
                // stream first; the replacement reads nothing until it is polled
                // again at the top of the loop.
                events = restart_event_stream(events);
                terminal.suspend()?;
                let setup_result = run_setup_dialog(&config_path());
                terminal.resume()?;
                match setup_result? {
                    SetupOutcome::Written => {
                        controller.reload()?;
                        dashboard.set_config(controller.config.clone());
                        dashboard.set_state(controller.state.clone());
                        request_dashboard_quota_refresh(
                            &controller,
                            &mut dashboard,
                            &quota_profiles_tx,
                        );
                        refresh_dashboard_poll_targets(
                            &controller,
                            &worker_targets_tx,
                            &resource_targets_tx,
                            &credential_sync_handle,
                            &lifecycle_operations.keys().cloned().collect(),
                        );
                        dashboard.set_notice(
                            "Setup complete. Press Ctrl+N to start your first session.",
                        );
                    }
                    SetupOutcome::Cancelled => dashboard.set_notice("Setup cancelled."),
                }
            }
            DashboardAction::RefreshQuotas => {
                manual_quota_refresh_generation = Some(request_dashboard_quota_refresh(
                    &controller,
                    &mut dashboard,
                    &quota_profiles_tx,
                ));
                dashboard.set_notice(QUOTA_REFRESH_NOTICE);
            }
            DashboardAction::OpenImport => {
                import_discovery_id = import_discovery_id.wrapping_add(1);
                dashboard.show_import_dialog(
                    import_discovery_id,
                    import_profile_placeholders(&controller.config),
                );
                let discovery_id = import_discovery_id;
                for (profile_id, profile) in controller.config.profiles.clone() {
                    let updates = import_updates_tx.clone();
                    tokio::task::spawn_blocking(move || {
                        let completed = discover_import_profile(
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
            DashboardAction::ImportSession {
                profile_id,
                native_session_id,
                display_title,
            } => {
                let pending = PendingDashboardImport {
                    profile_id,
                    native_session_id,
                    display_title,
                };
                dashboard.show_import_progress(pending.display_title.clone());
                next_import_task_id = next_import_task_id.wrapping_add(1);
                let cancelled = Arc::new(AtomicBool::new(false));
                active_import = Some(ActiveDashboardImport {
                    task_id: next_import_task_id,
                    cancelled: cancelled.clone(),
                });
                spawn_dashboard_import(
                    &controller,
                    pending,
                    DashboardImportSafety {
                        accepted: false,
                        include_untracked: true,
                    },
                    next_import_task_id,
                    cancelled,
                    import_task_tx.clone(),
                );
            }
            DashboardAction::CancelImport => {
                if let Some(active) = active_import.take() {
                    active.cancelled.store(true, Ordering::Release);
                    dashboard.finish_import();
                    dashboard
                        .set_notice("Import cancellation requested; no Hel state will be changed.");
                }
            }
            DashboardAction::ConfirmImportBundle {
                accepted,
                include_untracked,
            } => {
                let Some(pending) = pending_import.take() else {
                    dashboard.finish_import();
                    dashboard.set_notice("Import confirmation expired.");
                    continue;
                };
                if accepted {
                    dashboard.show_import_progress(pending.display_title.clone());
                    next_import_task_id = next_import_task_id.wrapping_add(1);
                    let cancelled = Arc::new(AtomicBool::new(false));
                    active_import = Some(ActiveDashboardImport {
                        task_id: next_import_task_id,
                        cancelled: cancelled.clone(),
                    });
                    spawn_dashboard_import(
                        &controller,
                        pending,
                        DashboardImportSafety {
                            accepted: true,
                            include_untracked,
                        },
                        next_import_task_id,
                        cancelled,
                        import_task_tx.clone(),
                    );
                } else {
                    dashboard.finish_import();
                    dashboard.set_notice("Import cancelled; no Hel files were changed.");
                }
            }
            DashboardAction::RenameSession { session_id, title } => {
                dashboard.set_notice("Renaming session…");
                spawn_dashboard_rename(session_id, title, dashboard_io_tx.clone());
            }
            DashboardAction::CompleteMountSource {
                target_template_id,
                prefix,
            } => {
                let config = controller.config.clone();
                let updates = dashboard_io_tx.clone();
                tokio::task::spawn_blocking(move || {
                    let operation_controller = Controller {
                        config,
                        state: HelState::default(),
                    };
                    let result = operation_controller
                        .complete_mount_source(&target_template_id, &prefix, &ProcessExecutor)
                        .map_err(|error| format!("{error:#}"));
                    let _ = updates.send(DashboardIoUpdate::MountCompletions { prefix, result });
                });
            }
            DashboardAction::ValidateMountSource {
                target_template_id,
                source,
            } => {
                let config = controller.config.clone();
                let updates = dashboard_io_tx.clone();
                tokio::task::spawn_blocking(move || {
                    let operation_controller = Controller {
                        config,
                        state: HelState::default(),
                    };
                    let result = operation_controller
                        .validate_mount_source(
                            &target_template_id,
                            std::path::Path::new(&source),
                            &ProcessExecutor,
                        )
                        .map_err(|error| format!("{error:#}"));
                    let _ = updates.send(DashboardIoUpdate::MountValidation { source, result });
                });
            }
            DashboardAction::ValidateProjectDirectory {
                target_template_id,
                directory,
            } => {
                let config = controller.config.clone();
                let updates = dashboard_io_tx.clone();
                tokio::task::spawn_blocking(move || {
                    let operation_controller = Controller {
                        config,
                        state: HelState::default(),
                    };
                    let result = operation_controller
                        .validate_project_directory(
                            &target_template_id,
                            std::path::Path::new(&directory),
                            &ProcessExecutor,
                        )
                        .map_err(|error| format!("{error:#}"));
                    let _ =
                        updates.send(DashboardIoUpdate::ProjectValidation { directory, result });
                });
            }
            DashboardAction::CreateSession {
                profile_id,
                bundle_id,
                project_directory,
                target_template_id,
                additional_mounts,
                allow_dirty_local,
                resource_allocation,
            } => {
                dashboard.set_notice("Preparing session launch…");
                spawn_dashboard_create_session(
                    DashboardAction::CreateSession {
                        profile_id,
                        bundle_id,
                        project_directory,
                        target_template_id,
                        additional_mounts,
                        allow_dirty_local,
                        resource_allocation,
                    },
                    dashboard_io_tx.clone(),
                    lifecycle_updates_tx.clone(),
                    tokio::runtime::Handle::current(),
                );
            }
            DashboardAction::Open { session_id } => {
                if active_chat
                    .as_ref()
                    .is_some_and(|chat| chat.session_id() == session_id)
                {
                    // The warm chat is this session: it has been following the
                    // worker off screen, so showing it is only a redraw.
                    view = View::Chat;
                    dirty = true;
                } else {
                    match open_chat_view(
                        &controller,
                        &session_id,
                        &worker_commands_tx,
                        &recovery_observer,
                        notices.clone(),
                    )
                    .await
                    {
                        Ok(chat) => {
                            // Only one chat stays warm, so the previous one is
                            // dropped here; its supervisor detaches on drop.
                            active_chat = Some(chat);
                            view = View::Chat;
                            dirty = true;
                        }
                        Err(error) => {
                            dashboard.set_notice(format!("Could not open session: {error:#}"));
                        }
                    }
                }
            }
            DashboardAction::ResumeSession {
                session_id,
                profile_id,
                target_template_id,
                additional_mounts,
                resource_allocation,
                discard_queue,
            } => {
                dashboard.set_notice(resume_progress_notice(
                    &session_id,
                    &profile_id,
                    &target_template_id,
                ));
                dashboard.begin_session_operation(
                    session_id.clone(),
                    SessionOperationKind::Resuming,
                    None,
                );
                let cancelled = Arc::new(AtomicBool::new(false));
                lifecycle_operations.insert(
                    session_id.clone(),
                    ActiveLifecycleOperation {
                        cancelled: cancelled.clone(),
                        kind: SessionOperationKind::Resuming,
                    },
                );
                let updates = lifecycle_updates_tx.clone();
                let observer = recovery_observer.clone();
                let operation_session_id = session_id.clone();
                let operation_profile_id = profile_id.clone();
                let operation_target_id = target_template_id.clone();
                let runtime = tokio::runtime::Handle::current();
                tokio::task::spawn_blocking(move || {
                    let result = (|| -> Result<MaterializedSession> {
                        let _recovery_reservation = reserve_recovery_or_cancel(
                            &observer,
                            &operation_session_id,
                            &cancelled,
                        )?;
                        let mut controller = Controller::load()?;
                        let executor = CancellableProcessExecutor::new(cancelled);
                        runtime.block_on(controller.resume_session_controlled(
                            &operation_session_id,
                            &operation_profile_id,
                            &operation_target_id,
                            SessionResumeOptions {
                                additional_mounts: Some(additional_mounts),
                                resource_allocation,
                                discard_queue,
                            },
                            &executor,
                        ))
                    })()
                    .map(|materialized| LifecycleSuccess::Resumed {
                        profile_id: operation_profile_id,
                        target_id: operation_target_id,
                        materialized: Box::new(materialized),
                    })
                    .map_err(|error| format!("{error:#}"));
                    let _ = updates.send(LifecycleUpdate {
                        session_id: operation_session_id,
                        result,
                    });
                });
            }
            DashboardAction::Close { session_id } => {
                dashboard.begin_session_operation(
                    session_id.clone(),
                    SessionOperationKind::Pausing,
                    None,
                );
                dashboard.set_notice(format!("Pausing {}…", short_id(&session_id)));
                let cancelled = Arc::new(AtomicBool::new(false));
                lifecycle_operations.insert(
                    session_id.clone(),
                    ActiveLifecycleOperation {
                        cancelled: cancelled.clone(),
                        kind: SessionOperationKind::Pausing,
                    },
                );
                let observer = recovery_observer.clone();
                let updates = lifecycle_updates_tx.clone();
                let session_manager = worker_commands_tx.clone();
                let operation_session_id = session_id.clone();
                let runtime = tokio::runtime::Handle::current();
                tokio::task::spawn_blocking(move || {
                    let result = (|| -> Result<()> {
                        let _recovery_reservation = reserve_recovery_or_cancel(
                            &observer,
                            &operation_session_id,
                            &cancelled,
                        )?;
                        let mut controller = Controller::load()?;
                        let executor = CancellableProcessExecutor::new(cancelled.clone());
                        runtime.block_on(controller.close_session_managed_controlled(
                            &operation_session_id,
                            &executor,
                            &session_manager,
                        ))
                    })()
                    .map(|()| LifecycleSuccess::Closed)
                    .map_err(|error| format!("{error:#}"));
                    let _ = updates.send(LifecycleUpdate {
                        session_id: operation_session_id,
                        result,
                    });
                });
            }
            DashboardAction::ResolveAwsResourceOptions {
                target_template_ids,
            } => {
                for target_template_id in target_template_ids {
                    if resolving_aws_resource_options.insert(target_template_id.clone()) {
                        spawn_aws_resource_options_resolution(
                            controller.config.clone(),
                            target_template_id,
                            aws_resource_options_tx.clone(),
                        );
                    }
                }
            }
            DashboardAction::CreateBundle { source } => {
                dashboard.set_notice("Creating bundle…");
                spawn_create_bundle(source, dashboard_io_tx.clone());
            }
            DashboardAction::ForceDestroy { session_id } => {
                dashboard.begin_session_operation(
                    session_id.clone(),
                    SessionOperationKind::Destroying,
                    None,
                );
                let cancelled = Arc::new(AtomicBool::new(false));
                lifecycle_operations.insert(
                    session_id.clone(),
                    ActiveLifecycleOperation {
                        cancelled: cancelled.clone(),
                        kind: SessionOperationKind::Destroying,
                    },
                );
                let observer = recovery_observer.clone();
                let updates = lifecycle_updates_tx.clone();
                let operation_session_id = session_id.clone();
                tokio::task::spawn_blocking(move || {
                    let result = (|| -> Result<()> {
                        let _recovery_reservation = reserve_recovery_or_cancel(
                            &observer,
                            &operation_session_id,
                            &cancelled,
                        )?;
                        let mut controller = Controller::load()?;
                        let executor = CancellableProcessExecutor::new(cancelled);
                        controller.force_destroy(&operation_session_id, &executor)
                    })()
                    .map(|()| LifecycleSuccess::Destroyed)
                    .map_err(|error| format!("{error:#}"));
                    let _ = updates.send(LifecycleUpdate {
                        session_id: operation_session_id,
                        result,
                    });
                });
            }
            DashboardAction::DeleteActive { session_id } => {
                dashboard.begin_session_operation(
                    session_id.clone(),
                    SessionOperationKind::Deleting,
                    None,
                );
                let cancelled = Arc::new(AtomicBool::new(false));
                lifecycle_operations.insert(
                    session_id.clone(),
                    ActiveLifecycleOperation {
                        cancelled: cancelled.clone(),
                        kind: SessionOperationKind::Deleting,
                    },
                );
                let observer = recovery_observer.clone();
                let updates = lifecycle_updates_tx.clone();
                let operation_session_id = session_id.clone();
                tokio::task::spawn_blocking(move || {
                    let result = (|| -> Result<()> {
                        let _recovery_reservation = reserve_recovery_or_cancel(
                            &observer,
                            &operation_session_id,
                            &cancelled,
                        )?;
                        let mut controller = Controller::load()?;
                        let executor = CancellableProcessExecutor::new(cancelled);
                        controller.force_destroy(&operation_session_id, &executor)?;
                        controller.delete_session_controlled(&operation_session_id, &executor)
                    })()
                    .map(|()| LifecycleSuccess::DeletedActive)
                    .map_err(|error| format!("{error:#}"));
                    let _ = updates.send(LifecycleUpdate {
                        session_id: operation_session_id,
                        result,
                    });
                });
            }
            DashboardAction::DeleteArchived { session_id } => {
                dashboard.begin_session_operation(
                    session_id.clone(),
                    SessionOperationKind::Deleting,
                    None,
                );
                let cancelled = Arc::new(AtomicBool::new(false));
                lifecycle_operations.insert(
                    session_id.clone(),
                    ActiveLifecycleOperation {
                        cancelled: cancelled.clone(),
                        kind: SessionOperationKind::Deleting,
                    },
                );
                let updates = lifecycle_updates_tx.clone();
                let operation_session_id = session_id.clone();
                tokio::task::spawn_blocking(move || {
                    let result = (|| -> Result<()> {
                        let mut controller = Controller::load()?;
                        if cancelled.load(Ordering::Acquire) {
                            bail!("operation cancelled");
                        }
                        let executor = CancellableProcessExecutor::new(cancelled);
                        controller.delete_session_controlled(&operation_session_id, &executor)
                    })()
                    .map(|()| LifecycleSuccess::DeletedArchived)
                    .map_err(|error| format!("{error:#}"));
                    let _ = updates.send(LifecycleUpdate {
                        session_id: operation_session_id,
                        result,
                    });
                });
            }
            DashboardAction::CancelOperation { session_id } => {
                if let Some(operation) = lifecycle_operations.get(&session_id) {
                    operation.cancelled.store(true, Ordering::Release);
                    dashboard.set_notice(format!(
                        "Cancelling {} for {}…",
                        operation.kind.label().to_ascii_lowercase(),
                        short_id(&session_id)
                    ));
                }
            }
        }
    }
    for operation in lifecycle_operations.values() {
        operation.cancelled.store(true, Ordering::Release);
    }
    if let Some(active) = active_import.as_ref() {
        active.cancelled.store(true, Ordering::Release);
    }
    drop(terminal);
    if quit_detached {
        println!(
            "Active sessions will continue working; Hel will reattach to them on your next invocation."
        );
    }
    Ok(())
}

fn reserve_recovery_or_cancel(
    observer: &hel::hel_recovery::RecoveryObserver,
    session_id: &str,
    cancelled: &AtomicBool,
) -> Result<hel::hel_recovery::RecoveryReservation> {
    let reservation = observer.reserve(session_id);
    while observer.is_busy(session_id) {
        if cancelled.load(Ordering::Acquire) {
            bail!("operation cancelled while waiting for recovery copy");
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    Ok(reservation)
}

fn discover_import_profile(
    profile_id: String,
    harness_kind: hel::hel_config::HarnessKind,
    home: PathBuf,
    mut publish: impl FnMut(&ImportProfileOption),
) -> ImportProfileOption {
    let mut profile = ImportProfileOption {
        profile_id,
        harness_kind,
        sessions: Vec::new(),
        scan_progress: None,
        error: None,
    };
    let discovered = match harness_kind {
        hel::hel_config::HarnessKind::Codex => scan_codex_sessions(&home, |progress| {
            profile.scan_progress = Some((progress.scanned, progress.total));
            if let Some(session) = progress.session {
                let unavailable_reason = session.history_mode.import_issue().map(ToOwned::to_owned);
                profile.sessions.push(import_session_option(
                    session.native_session_id,
                    session.title,
                    session.modified_at,
                    session.git_branch,
                    session.size_bytes,
                    session.cwd,
                    unavailable_reason,
                ));
            }
            publish(&profile);
        }),
        hel::hel_config::HarnessKind::Claude => scan_claude_sessions(&home, |progress| {
            profile.scan_progress = Some((progress.scanned, progress.total));
            if let Some(session) = progress.session {
                profile.sessions.push(import_session_option(
                    session.native_session_id,
                    session.title,
                    session.modified_at,
                    session.git_branch,
                    session.size_bytes,
                    session.cwd,
                    None,
                ));
            }
            publish(&profile);
        }),
        hel::hel_config::HarnessKind::Kimi => scan_kimi_sessions(&home, |progress| {
            profile.scan_progress = Some((progress.scanned, progress.total));
            if let Some(session) = progress.session {
                profile.sessions.push(import_session_option(
                    session.native_session_id,
                    session.title,
                    session.modified_at,
                    session.git_branch,
                    session.size_bytes,
                    session.cwd,
                    None,
                ));
            }
            publish(&profile);
        }),
    };
    if let Err(error) = discovered {
        profile.error = Some(format!("{error:#}"));
        publish(&profile);
    }
    profile
}

fn import_session_option(
    native_session_id: String,
    title: String,
    modified_at: SystemTime,
    branch: String,
    size: u64,
    cwd: PathBuf,
    unavailable_reason: Option<String>,
) -> ImportSessionOption {
    let project_directory = display_home_relative(&cwd);
    let details = format!(
        "{} · {} · {} · {}",
        system_time_age(modified_at),
        branch,
        format_byte_size(size),
        project_directory
    );
    ImportSessionOption {
        native_session_id,
        title,
        project_directory,
        details,
        unavailable_reason,
    }
}

fn import_profile_placeholders(config: &HelConfig) -> Vec<ImportProfileOption> {
    config
        .profiles
        .iter()
        .map(|(profile_id, profile)| ImportProfileOption {
            profile_id: profile_id.clone(),
            harness_kind: profile.kind,
            sessions: Vec::new(),
            scan_progress: None,
            error: None,
        })
        .collect()
}

fn system_time_age(time: SystemTime) -> String {
    let age = SystemTime::now().duration_since(time).unwrap_or_default();
    let seconds = age.as_secs();
    if seconds < 60 {
        format!("{seconds}s ago")
    } else if seconds < 3_600 {
        format!("{}m ago", seconds / 60)
    } else if seconds < 86_400 {
        format!("{}h ago", seconds / 3_600)
    } else {
        format!("{}d ago", seconds / 86_400)
    }
}

fn format_byte_size(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    if bytes < 1024 {
        format!("{bytes}B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1}KB", bytes as f64 / KIB)
    } else {
        format!("{:.1}MB", bytes as f64 / MIB)
    }
}

fn display_home_relative(path: &std::path::Path) -> String {
    dirs::home_dir()
        .and_then(|home| path.strip_prefix(home).ok().map(PathBuf::from))
        .map(|relative| format!("~/{}", relative.display()))
        .unwrap_or_else(|| path.display().to_string())
}

fn spawn_dashboard_import(
    controller: &Controller,
    pending: PendingDashboardImport,
    safety: DashboardImportSafety,
    task_id: u64,
    cancelled: Arc<AtomicBool>,
    updates: tokio::sync::mpsc::Sender<DashboardImportUpdate>,
) {
    let worker_controller = Controller {
        config: controller.config.clone(),
        state: controller.state.clone(),
    };
    tokio::task::spawn_blocking(move || {
        let last_detail_update = Mutex::new(Instant::now() - Duration::from_secs(1));
        let report = |step: usize, total: Option<usize>, message: &str, force: bool| {
            if cancelled.load(Ordering::Acquire) {
                return;
            }
            if force {
                let _ = updates.blocking_send(DashboardImportUpdate::Progress {
                    task_id,
                    step,
                    total,
                    message: message.into(),
                });
                return;
            }
            let mut last_update = last_detail_update.lock().expect("import progress lock");
            let now = Instant::now();
            if now.duration_since(*last_update) < Duration::from_millis(250) {
                return;
            }
            if updates
                .try_send(DashboardImportUpdate::Progress {
                    task_id,
                    step,
                    total,
                    message: message.into(),
                })
                .is_ok()
            {
                *last_update = now;
            }
        };
        let mut result = import_session_from_profile(
            worker_controller,
            &pending.profile_id,
            &pending.native_session_id,
            &pending.display_title,
            safety,
            &cancelled,
            report,
        );
        if cancelled.load(Ordering::Acquire) {
            if let Ok(DashboardImportTaskResult::Imported(imported)) = &result
                && let Some(path) = imported
                    .controller
                    .state
                    .sessions
                    .get(&imported.session_id)
                    .and_then(|session| session.checkpoint.as_ref())
                    .map(|checkpoint| checkpoint.archive_path.clone())
            {
                let _ = std::fs::remove_file(path);
            }
            result = Ok(DashboardImportTaskResult::Cancelled);
        }
        let _ = updates.blocking_send(DashboardImportUpdate::Finished {
            task_id,
            pending,
            result,
        });
    });
}

enum BackgroundBundleResolution {
    Ready(String),
    NeedsConfirmation(ImportBundlePrompt),
}

fn report_import_archive_progress(
    progress: ImportArchiveProgress,
    report: &(impl Fn(usize, Option<usize>, &str, bool) + Sync),
) {
    match progress {
        ImportArchiveProgress::Repository { current, total, id } => report(
            current,
            Some(total),
            &format!("Snapshotting repository {current}/{total}: {id}"),
            true,
        ),
        ImportArchiveProgress::UntrackedFile {
            repository_id,
            current,
            total,
            path,
        } => report(
            current,
            Some(total),
            &format!(
                "Repository {repository_id}: archiving untracked file {current}/{total}: {}",
                path.display()
            ),
            current == 1 || current == total,
        ),
        ImportArchiveProgress::WritingArchive => report(
            1,
            None,
            "Writing, syncing, and verifying the archive…",
            true,
        ),
    }
}

fn resolve_background_import_bundle(
    config: &mut HelConfig,
    transcript: &hel::hel_import::ClaudeTranscript,
    profile_home: &std::path::Path,
    safety_accepted: bool,
) -> Result<BackgroundBundleResolution> {
    let targets = session_edit_targets(transcript, profile_home)?;
    let bundle_id = match resolve_bundle(config, &transcript.cwd, &targets, None)? {
        BundleResolution::Existing(bundle_id) => bundle_id,
        BundleResolution::Synthesized { id, bundle } => {
            config.bundles.insert(id.clone(), bundle);
            id
        }
    };
    let issues = import_safety_issues(&targets)?;
    if !safety_accepted
        && (!issues.dirty_git_roots.is_empty() || !issues.omitted_non_git_dirs.is_empty())
    {
        return Ok(BackgroundBundleResolution::NeedsConfirmation(
            ImportBundlePrompt {
                dirty_git_roots: issues
                    .dirty_git_roots
                    .into_iter()
                    .map(|(root, summary)| format!("{} — {summary}", root.display()))
                    .collect(),
                omitted_non_git_dirs: issues
                    .omitted_non_git_dirs
                    .into_iter()
                    .map(|path| path.display().to_string())
                    .collect(),
                has_untracked_files: issues.has_untracked_files,
            },
        ));
    }
    Ok(BackgroundBundleResolution::Ready(bundle_id))
}

fn import_session_from_profile(
    mut controller: Controller,
    profile_id: &str,
    native_session_id: &str,
    display_title: &str,
    safety: DashboardImportSafety,
    cancelled: &AtomicBool,
    report: impl Fn(usize, Option<usize>, &str, bool) + Sync,
) -> Result<DashboardImportTaskResult> {
    report(1, None, "Locating native session…", true);
    let profile = controller
        .config
        .profiles
        .get(profile_id)
        .with_context(|| format!("unknown profile {profile_id:?}"))?
        .clone();
    match profile.kind {
        hel::hel_config::HarnessKind::Codex => {
            let source = locate_codex_session(
                &profile.home,
                &CodexSessionSelection::NativeSessionId(native_session_id.into()),
            )?;
            let transcript = read_codex_transcript(&source.jsonl_path)?;
            report(2, Some(4), "Native session parsed.", true);
            let bundle_id = match resolve_background_import_bundle(
                &mut controller.config,
                &transcript,
                &profile.home,
                safety.accepted,
            )? {
                BackgroundBundleResolution::Ready(bundle_id) => bundle_id,
                BackgroundBundleResolution::NeedsConfirmation(prompt) => {
                    return Ok(DashboardImportTaskResult::NeedsBundle(prompt));
                }
            };
            let archive_progress = |progress| report_import_archive_progress(progress, &report);
            let control = ImportControl {
                cancelled,
                progress: &archive_progress,
                include_untracked: safety.include_untracked,
            };
            let imported = import_codex_session_with_control(
                &controller.config,
                &mut controller.state,
                CodexImportRequest {
                    codex_home: &profile.home,
                    source: &source,
                    transcript: &transcript,
                    bundle_id: &bundle_id,
                    profile_id: Some(profile_id),
                    title: Some(display_title),
                    archive_directory: &sessions_dir(),
                },
                &control,
            )?;
            report(4, Some(4), "Finalizing imported session…", true);
            Ok(DashboardImportTaskResult::Imported(
                DashboardImportSuccess {
                    harness: "Codex",
                    session_id: imported.session_id,
                    controller,
                },
            ))
        }
        hel::hel_config::HarnessKind::Claude => {
            let source = locate_claude_session(
                &profile.home,
                &ClaudeSessionSelection::NativeSessionId(native_session_id.into()),
            )?;
            let transcript = read_claude_transcript(&source.jsonl_path)?;
            report(2, Some(4), "Native session parsed.", true);
            let bundle_id = match resolve_background_import_bundle(
                &mut controller.config,
                &transcript,
                &profile.home,
                safety.accepted,
            )? {
                BackgroundBundleResolution::Ready(bundle_id) => bundle_id,
                BackgroundBundleResolution::NeedsConfirmation(prompt) => {
                    return Ok(DashboardImportTaskResult::NeedsBundle(prompt));
                }
            };
            let archive_progress = |progress| report_import_archive_progress(progress, &report);
            let control = ImportControl {
                cancelled,
                progress: &archive_progress,
                include_untracked: safety.include_untracked,
            };
            let imported = import_claude_session_with_control(
                &controller.config,
                &mut controller.state,
                ClaudeImportRequest {
                    claude_home: &profile.home,
                    source: &source,
                    transcript: &transcript,
                    bundle_id: &bundle_id,
                    profile_id: Some(profile_id),
                    title: Some(display_title),
                    archive_directory: &sessions_dir(),
                },
                &control,
            )?;
            report(4, Some(4), "Finalizing imported session…", true);
            Ok(DashboardImportTaskResult::Imported(
                DashboardImportSuccess {
                    harness: "Claude",
                    session_id: imported.session_id,
                    controller,
                },
            ))
        }
        hel::hel_config::HarnessKind::Kimi => {
            let source = locate_kimi_session(
                &profile.home,
                &KimiSessionSelection::NativeSessionId(native_session_id.into()),
            )?;
            let transcript = read_kimi_transcript(&source.session_path)?;
            report(2, Some(4), "Native session parsed.", true);
            let bundle_id = match resolve_background_import_bundle(
                &mut controller.config,
                &transcript,
                &profile.home,
                safety.accepted,
            )? {
                BackgroundBundleResolution::Ready(bundle_id) => bundle_id,
                BackgroundBundleResolution::NeedsConfirmation(prompt) => {
                    return Ok(DashboardImportTaskResult::NeedsBundle(prompt));
                }
            };
            let archive_progress = |progress| report_import_archive_progress(progress, &report);
            let control = ImportControl {
                cancelled,
                progress: &archive_progress,
                include_untracked: safety.include_untracked,
            };
            let imported = import_kimi_session_with_control(
                &controller.config,
                &mut controller.state,
                KimiImportRequest {
                    kimi_home: &profile.home,
                    source: &source,
                    transcript: &transcript,
                    bundle_id: &bundle_id,
                    profile_id: Some(profile_id),
                    title: Some(display_title),
                    archive_directory: &sessions_dir(),
                },
                &control,
            )?;
            report(4, Some(4), "Finalizing imported session…", true);
            Ok(DashboardImportTaskResult::Imported(
                DashboardImportSuccess {
                    harness: "Kimi",
                    session_id: imported.session_id,
                    controller,
                },
            ))
        }
    }
}

fn short_id(id: &str) -> &str {
    id.get(..8).unwrap_or(id)
}

fn resume_progress_notice(session_id: &str, profile_id: &str, target_id: &str) -> String {
    format!(
        "Preparing {}: verifying checkpoint, provisioning {target_id}, and restoring {profile_id}…",
        short_id(session_id)
    )
}

fn configuration_needs_setup(config: &hel::hel_config::HelConfig) -> bool {
    config.profiles.is_empty() && config.bundles.is_empty() && config.targets.is_empty()
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

struct TerminalGuard {
    terminal: Terminal<CrosstermBackend<io::Stdout>>,
    keyboard_enhancement: bool,
}

impl TerminalGuard {
    fn enter() -> Result<Self> {
        enable_raw_mode().context("enable terminal raw mode")?;
        let mut stdout = io::stdout();
        // Legacy terminal input encodes Ctrl+I as the same byte as Tab. Ask
        // capable terminals to report them distinctly so both bindings work.
        let keyboard_enhancement = matches!(
            crossterm::terminal::supports_keyboard_enhancement(),
            Ok(true)
        );
        if keyboard_enhancement {
            execute!(
                stdout,
                PushKeyboardEnhancementFlags(KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES)
            )
            .context("enable unambiguous terminal key reporting")?;
        }
        execute!(
            stdout,
            EnterAlternateScreen,
            EnableMouseCapture,
            EnableBracketedPaste
        )
        .context("enter alternate screen and enable terminal input modes")?;
        let terminal = Terminal::new(CrosstermBackend::new(stdout))?;
        Ok(Self {
            terminal,
            keyboard_enhancement,
        })
    }

    fn suspend(&mut self) -> Result<()> {
        if self.keyboard_enhancement {
            execute!(self.terminal.backend_mut(), PopKeyboardEnhancementFlags)
                .context("restore terminal key reporting for setup")?;
        }
        execute!(
            self.terminal.backend_mut(),
            DisableBracketedPaste,
            DisableMouseCapture,
            LeaveAlternateScreen
        )
        .context("disable terminal input modes and leave alternate screen for setup")?;
        disable_raw_mode().context("disable terminal raw mode for setup")?;
        self.terminal
            .show_cursor()
            .context("show cursor for setup")?;
        Ok(())
    }

    fn resume(&mut self) -> Result<()> {
        enable_raw_mode().context("re-enable terminal raw mode after setup")?;
        if self.keyboard_enhancement {
            execute!(
                self.terminal.backend_mut(),
                PushKeyboardEnhancementFlags(KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES)
            )
            .context("re-enable unambiguous terminal key reporting after setup")?;
        }
        execute!(
            self.terminal.backend_mut(),
            EnterAlternateScreen,
            EnableMouseCapture,
            EnableBracketedPaste
        )
        .context("re-enter alternate screen and enable terminal input modes after setup")?;
        self.terminal
            .clear()
            .context("clear dashboard after setup")?;
        Ok(())
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        if self.keyboard_enhancement {
            let _ = execute!(self.terminal.backend_mut(), PopKeyboardEnhancementFlags);
        }
        let _ = execute!(
            self.terminal.backend_mut(),
            DisableBracketedPaste,
            DisableMouseCapture,
            LeaveAlternateScreen
        );
        let _ = disable_raw_mode();
        let _ = self.terminal.show_cursor();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn login_uses_the_sole_profile_and_otherwise_demands_a_choice() {
        let mut config = HelConfig::default();
        assert!(resolve_login_profile(&config, None).is_err());

        config.profiles.insert(
            "work".into(),
            hel::hel_config::HarnessProfile {
                kind: hel::hel_config::HarnessKind::Claude,
                home: PathBuf::from("/home/user/.claude"),
                executable: None,
                environment: Default::default(),
                context_window_bytes: None,
            },
        );
        assert_eq!(resolve_login_profile(&config, None).unwrap(), "work");

        config
            .profiles
            .insert("personal".into(), config.profiles["work"].clone());
        let error = resolve_login_profile(&config, None)
            .unwrap_err()
            .to_string();
        assert!(error.contains("personal, work"), "{error}");
        assert_eq!(
            resolve_login_profile(&config, Some("personal")).unwrap(),
            "personal"
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

    #[test]
    fn short_session_ids_are_safe() {
        assert_eq!(short_id("0123456789"), "01234567");
        assert_eq!(short_id("tiny"), "tiny");
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

    #[test]
    fn resume_progress_explains_the_blocking_work() {
        assert_eq!(
            resume_progress_notice("0123456789", "codex-1", "podman"),
            "Preparing 01234567: verifying checkpoint, provisioning podman, and restoring codex-1…"
        );
    }

    #[test]
    fn cli_name_and_worker_shape_are_stable() {
        use clap::CommandFactory;
        let command = Cli::command();
        assert_eq!(command.get_name(), "hel");
        assert!(
            command
                .get_subcommands()
                .any(|sub| sub.get_name() == "worker")
        );
        assert!(
            command
                .get_subcommands()
                .any(|sub| sub.get_name() == "setup")
        );
        let login = command
            .get_subcommands()
            .find(|sub| sub.get_name() == "login")
            .expect("hel login is a visible command");
        assert!(!login.is_hide_set());
    }

    #[test]
    fn doctor_json_and_setup_instructions_are_parseable() {
        let doctor = Cli::try_parse_from(["hel", "doctor", "--json"]).unwrap();
        assert!(matches!(
            doctor.command,
            Some(Command::Doctor(DoctorArgs {
                json: true,
                smoke: false
            }))
        ));

        let setup =
            Cli::try_parse_from(["hel", "setup", "instructions", "--platform", "linux"]).unwrap();
        assert!(matches!(
            setup.command,
            Some(Command::Setup(SetupArgs {
                command: Some(SetupCommand::Instructions {
                    platform: SetupPlatform::Linux
                })
            }))
        ));
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

    #[test]
    fn failed_archive_removal_retains_session_metadata_for_retry() {
        let directory = tempfile::tempdir().unwrap();
        let archive_path = directory.path().join("checkpoint.hel.zip");
        std::fs::create_dir(&archive_path).unwrap();
        let session_id = "1123456789abcdef0123456789abcdef";
        let mut state = HelState::default();
        state.sessions.insert(
            session_id.into(),
            SessionRecord {
                id: session_id.into(),
                title: "paused".into(),
                harness_kind: hel::hel_config::HarnessKind::Codex,
                last_profile: "codex".into(),
                bundle_id: "project".into(),
                project_directory: None,
                managed_worktree: None,
                target_template_id: "podman".into(),
                resource_allocation: None,
                additional_mounts: Vec::new(),
                state: SessionState::Archived,
                target: None,
                native_session_id: Some("native-session".into()),
                acp_session_title: None,
                session_title_override: None,
                created_at: "2026-08-12T00:00:00Z".into(),
                updated_at: "2026-08-12T00:00:00Z".into(),
                detached_after_event_ordinal: 0,
                draft_input: String::new(),
                last_error: None,
                last_checkpoint_error: None,
                checkpoint: Some(hel::hel_state::CheckpointMetadata {
                    archive_path,
                    sha256: "a".repeat(64),
                    created_at: "2026-08-12T00:00:00Z".into(),
                    event_frontier: 7,
                }),
            },
        );
        let mut controller = Controller {
            config: HelConfig::default(),
            state,
        };

        assert!(
            controller
                .delete_session_controlled(session_id, &ProcessExecutor)
                .is_err()
        );
        assert!(controller.state.sessions.contains_key(session_id));
    }
}
