//! Hel: a session control plane for ACP coding agents.

use std::io::{self, IsTerminal};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
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
use hel::hel_archive::{PayloadRole, read_archive_verified};
use hel::hel_config::{HelConfig, ProjectBundle, ProjectRepository, config_path, sessions_dir};
use hel::hel_controller::{Controller, SessionLaunchOptions, SessionResumeOptions};
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
use hel::hel_quota::{ProfileQuota, QuotaManager, QuotaRefreshRequest};
use hel::hel_server::{
    ControllerAction, ControllerRequest, ResumeQueueDisposition, ServerOptions, ViewerQueuedPrompt,
    ViewerQuota, ViewerSnapshot,
};
use hel::hel_setup::{SetupOutcome, github_repository_from_origin, run_setup_dialog};
use hel::hel_state::{
    HelState, SessionResourceAllocation, SessionState, TargetLocator, harness_session_title,
};
use hel::hel_targets::{
    CancellableProcessExecutor, CommandOutput, CommandSpec, DeploymentCapacityKind,
    DeploymentCapacityTarget, DeploymentCapacityUsage, ProcessExecutor, SessionResourceProbe,
    SessionResourceUsage,
};
use hel::hel_tui::{
    DashboardAction, DashboardState, ImportProfileOption, ImportSessionOption,
    SessionOperationKind, render,
};
use hel::hel_worker::{SequencedEvent, WorkerPhase};
use hel::hel_worker_client::{WorkerBootstrap, WorkerClient};
use hel::hel_worker_runtime::{
    AcpSupervisorSpec, WorkerLaunchConfig, proxy, run_acp_supervisor, run_daemon,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

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
const WORKER_POLL_INTERVAL: Duration = Duration::from_secs(1);
const WORKER_POLL_TIMEOUT: Duration = Duration::from_secs(3);
const RESOURCE_POLL_INTERVAL: Duration = Duration::from_secs(60);
const RESOURCE_POLL_TIMEOUT: Duration = Duration::from_secs(15);
const CAPACITY_POLL_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug)]
enum QuotaUpdate {
    Refreshing(Vec<String>),
    Report(ProfileQuota),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkerPollTarget {
    session_id: String,
    spec: CommandSpec,
}

#[derive(Debug)]
struct WorkerPollUpdate {
    session_id: String,
    payload: WorkerPollPayload,
}

#[derive(Debug)]
enum WorkerPollPayload {
    Connected,
    Events {
        events: Vec<SequencedEvent>,
        phase: WorkerPhase,
        transcript: hel::hel_chat::TranscriptSnapshot,
        queued_prompts: Vec<hel::hel_worker::QueuedPrompt>,
        /// These events predate the connection established by this poll.
        received_while_detached: bool,
    },
    /// The worker failed several consecutive polls; the session needs
    /// attention and a diagnosis.
    Unreachable {
        detail: String,
    },
}

struct WarmWorker {
    spec: CommandSpec,
    client: WorkerClient,
    chat: hel::hel_chat::ChatState,
    queued_prompts: Vec<hel::hel_worker::QueuedPrompt>,
    opening_events: Vec<SequencedEvent>,
}

enum WorkerPollCommand {
    Checkout {
        session_id: String,
        reply: tokio::sync::oneshot::Sender<Option<WarmWorker>>,
    },
    Checkin {
        session_id: String,
        worker: Option<Box<WarmWorker>>,
    },
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

/// Consecutive failed polls before a running session is declared unreachable.
/// One failure is routinely a transient exec hiccup.
const WORKER_POLL_FAILURE_THRESHOLD: u32 = 3;

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
                args.session, checkpoint.event_sequence
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
    hel::hel_database::save_session(
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
    hel::hel_database::save_session(
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
    hel::hel_database::save_session(
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
    let mut queued_prompts = archived_queued_prompts(&controller);
    let (snapshot_tx, snapshot_rx) = tokio::sync::watch::channel(viewer_snapshot(
        &controller,
        &quotas,
        &conversations,
        &queued_prompts,
        revision,
    ));
    let (conversation_tx, conversation_rx) = tokio::sync::watch::channel(conversations.clone());
    let (action_tx, mut action_rx) = tokio::sync::mpsc::channel(32);
    let (worker_targets_tx, mut worker_updates_rx, _worker_commands_tx) =
        spawn_dashboard_worker_poller();
    worker_targets_tx.send_replace(dashboard_worker_targets(&controller));
    let mut recovery = hel::hel_recovery::RecoveryCoordinator::spawn();
    let recovery_observer = recovery.observer();
    let mut credential_sync = CredentialSyncCoordinator::spawn();
    let credential_sync_handle = credential_sync.handle();
    credential_sync_handle.set_targets(credential_sync_targets(&controller));
    let mut auth_failure_syncs = std::collections::BTreeMap::<String, Instant>::new();
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
            Option<String>,
            tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
            std::result::Result<(), String>,
        )>();
        let mut active_actions = std::collections::BTreeSet::new();
        loop {
            tokio::select! {
                _ = termination.cancelled() => break,
                update = worker_updates_rx.recv() => {
                    let Some(update) = update else { break };
                    if let WorkerPollPayload::Events { events, phase, transcript, queued_prompts: worker_queue, .. } = update.payload {
                        if let Some(session) = controller.state.sessions.get(&update.session_id).cloned() {
                            if hel::hel_credentials::events_report_auth_failure(session.harness_kind, &events)
                                && auth_failure_sync_is_due(&mut auth_failure_syncs, &update.session_id, Instant::now())
                            {
                                credential_sync_handle.sync_profile_now(&session.last_profile, Some(&update.session_id));
                            }
                            recovery_observer
                                .observe(hel::hel_recovery::RecoveryObservation {
                                    session,
                                    config: controller.config.clone(),
                                    events,
                                    phase,
                                })
                                .await;
                        }
                        conversations.insert(
                            update.session_id.clone(),
                            transcript.browser_transcript(None),
                        );
                        queued_prompts.insert(update.session_id.clone(), worker_queue);
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
                    let mut changed = false;
                    while let Some(result) = recovery.try_result() {
                        changed |= merge_recovery_result(&mut controller, result);
                    }
                    while let Some(result) = credential_sync.try_result() {
                        if let Some(notice) = credential_sync_notice(&result) {
                            eprintln!("Hel: {notice}");
                        }
                    }
                    if changed {
                        revision += 1;
                        let _ = snapshot_tx.send(viewer_snapshot(&controller, &quotas, &conversations, &queued_prompts, revision));
                    }
                }
                action = action_rx.recv() => {
                    let Some(request) = action else { break };
                    let session_id = controller_action_session_id(&request.action);
                    if session_id.as_ref().is_some_and(|id| !active_actions.insert(id.clone())) {
                        let _ = request.reply.send(Err("another operation is already running for this session".into()));
                        continue;
                    }
                    let ControllerRequest { action, reply } = request;
                    let done = action_done_tx.clone();
                    let observer = recovery_observer.clone();
                    tokio::spawn(async move {
                        if let ControllerAction::Prompt { session_id, .. }
                            | ControllerAction::Close { session_id } = &action
                        {
                            observer.wait_idle(session_id).await;
                        }
                        let result = async {
                            let mut operation_controller = Controller::load()?;
                            apply_phone_action(&mut operation_controller, action).await
                        }
                        .await
                        .map_err(|error| format!("{error:#}"));
                        let _ = done.send((session_id, reply, result));
                    });
                }
                completed = action_done_rx.recv() => {
                    let Some((session_id, reply, result)) = completed else { break };
                    if let Some(session_id) = session_id {
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
        | ControllerAction::RemoveQueuedPrompt { session_id, .. } => Some(session_id.clone()),
    }
}

async fn apply_phone_action(controller: &mut Controller, action: ControllerAction) -> Result<()> {
    match action {
        ControllerAction::New {
            profile_id,
            bundle_id,
            target_id,
            title,
        } => match controller.register_session(&profile_id, &bundle_id, &target_id, title) {
            Ok(session_id) => controller.provision_session(&session_id).await,
            Err(error) => Err(error),
        },
        ControllerAction::Prompt { session_id, text } => {
            let spec = controller.reconnect_command(&session_id)?;
            let mut client = WorkerClient::connect(&spec, &session_id).await?;
            client.enqueue_prompt(text, Vec::new()).await?;
            client.detach().await
        }
        ControllerAction::Close { session_id } => controller.close_session(&session_id).await,
        ControllerAction::Resume {
            session_id,
            profile_id,
            target_id,
            queue,
        } => {
            controller
                .resume_session_with_queue_disposition(
                    &session_id,
                    &profile_id,
                    &target_id,
                    queue == ResumeQueueDisposition::Discard,
                )
                .await
        }
        ControllerAction::Open { .. } => Ok(()),
        ControllerAction::RemoveQueuedPrompt {
            session_id,
            queue_id,
        } => {
            let spec = controller.reconnect_command(&session_id)?;
            let mut client = WorkerClient::connect(&spec, &session_id).await?;
            client.remove_queued_prompt(queue_id).await?;
            client.detach().await
        }
    }
}

fn archived_queued_prompts(
    controller: &Controller,
) -> std::collections::BTreeMap<String, Vec<hel::hel_worker::QueuedPrompt>> {
    controller
        .state
        .sessions
        .values()
        .filter_map(|session| {
            let checkpoint = session.checkpoint.as_ref()?;
            let archive = read_archive_verified(&checkpoint.archive_path).ok()?;
            let bytes = archive
                .payload_by_role(&PayloadRole::CanonicalEvents)
                .ok()?;
            let events = bytes
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.is_empty())
                .map(serde_json::from_slice)
                .collect::<serde_json::Result<Vec<SequencedEvent>>>()
                .ok()?;
            Some((
                session.id.clone(),
                hel::hel_worker::queued_prompts_from_events(&events),
            ))
        })
        .collect()
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
    tokio::sync::watch::Sender<Vec<QuotaRefreshRequest>>,
    tokio::sync::mpsc::Receiver<QuotaUpdate>,
) {
    let (profiles_tx, mut profiles_rx) = tokio::sync::watch::channel(Vec::new());
    let (updates_tx, updates_rx) = tokio::sync::mpsc::channel(32);
    tokio::spawn(async move {
        let mut quotas = QuotaManager::default();
        let mut profiles = Vec::new();
        let mut interval = tokio::time::interval(QUOTA_REFRESH_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;
        loop {
            tokio::select! {
                _ = interval.tick(), if !profiles.is_empty() => {
                    if !refresh_profile_quotas(&mut quotas, &profiles, &updates_tx).await {
                        break;
                    }
                }
                changed = profiles_rx.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    profiles = profiles_rx.borrow_and_update().clone();
                    if !profiles.is_empty()
                        && !refresh_profile_quotas(&mut quotas, &profiles, &updates_tx).await
                    {
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
    profiles: &[QuotaRefreshRequest],
    updates: &tokio::sync::mpsc::Sender<QuotaUpdate>,
) -> bool {
    let ids = profiles
        .iter()
        .map(|profile| profile.profile_id.clone())
        .collect::<Vec<_>>();
    if updates.send(QuotaUpdate::Refreshing(ids)).await.is_err() {
        return false;
    }
    for quota in quotas.refresh_profiles(profiles.to_vec()).await {
        if updates.send(QuotaUpdate::Report(quota)).await.is_err() {
            return false;
        }
    }
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

fn auth_failure_sync_is_due(
    last_attempts: &mut std::collections::BTreeMap<String, Instant>,
    session_id: &str,
    now: Instant,
) -> bool {
    if last_attempts
        .get(session_id)
        .is_some_and(|previous| now.duration_since(*previous) < AUTH_FAILURE_SYNC_COOLDOWN)
    {
        return false;
    }
    last_attempts.insert(session_id.to_owned(), now);
    true
}

/// Healthy no-op cycles stay out of the UI; only actions, failures, and
/// answers to an authentication failure are worth a notice.
fn credential_sync_notice(result: &hel::hel_credentials::CredentialSyncResult) -> Option<String> {
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
    if let Some(detail) = &result.failure {
        return Some(format!(
            "Credential sync for profile {} failed: {detail}",
            result.profile_id
        ));
    }
    if let Some((session_id, detail)) = result.failures().next() {
        return Some(format!(
            "Credential sync for {} failed: {detail}",
            short_id(session_id)
        ));
    }
    let actions = result.actions();
    (actions > 0).then(|| {
        format!(
            "Refreshed harness credentials for profile {} across {actions} session(s).",
            result.profile_id
        )
    })
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

fn spawn_dashboard_worker_poller() -> (
    tokio::sync::watch::Sender<Vec<WorkerPollTarget>>,
    tokio::sync::mpsc::UnboundedReceiver<WorkerPollUpdate>,
    tokio::sync::mpsc::Sender<WorkerPollCommand>,
) {
    spawn_dashboard_worker_poller_with_interval(WORKER_POLL_INTERVAL)
}

struct CompletedWorkerPoll {
    worker: WarmWorker,
    events: Vec<SequencedEvent>,
    phase: WorkerPhase,
    transcript: Option<hel::hel_chat::TranscriptSnapshot>,
    connected: bool,
}

struct WorkerPollCompletion {
    target: WorkerPollTarget,
    result: std::result::Result<CompletedWorkerPoll, String>,
}

fn spawn_dashboard_worker_poller_with_interval(
    poll_interval: Duration,
) -> (
    tokio::sync::watch::Sender<Vec<WorkerPollTarget>>,
    tokio::sync::mpsc::UnboundedReceiver<WorkerPollUpdate>,
    tokio::sync::mpsc::Sender<WorkerPollCommand>,
) {
    let (targets_tx, mut targets_rx) = tokio::sync::watch::channel(Vec::<WorkerPollTarget>::new());
    let (updates_tx, updates_rx) = tokio::sync::mpsc::unbounded_channel();
    let (commands_tx, mut commands_rx) = tokio::sync::mpsc::channel(8);
    let (completions_tx, mut completions_rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut targets: std::collections::BTreeMap<String, WorkerPollTarget> =
            std::collections::BTreeMap::new();
        let mut clients: std::collections::BTreeMap<String, WarmWorker> =
            std::collections::BTreeMap::new();
        let mut checked_out = std::collections::BTreeSet::new();
        let mut polling = std::collections::BTreeMap::<String, CommandSpec>::new();
        let mut pending_checkouts = std::collections::BTreeMap::new();
        let mut failures: std::collections::BTreeMap<String, (u32, String)> =
            std::collections::BTreeMap::new();
        let mut interval = tokio::time::interval(poll_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    for target in targets.values() {
                        if checked_out.contains(&target.session_id)
                            || polling.contains_key(&target.session_id)
                        {
                            continue;
                        }
                        let worker = clients.remove(&target.session_id).filter(|worker| worker.spec == target.spec);
                        polling.insert(target.session_id.clone(), target.spec.clone());
                        let target = target.clone();
                        let completions = completions_tx.clone();
                        tokio::spawn(async move {
                            let completion = poll_dashboard_worker(target, worker).await;
                            let _ = completions.send(completion);
                        });
                    }
                }
                command = commands_rx.recv() => {
                    match command {
                        Some(WorkerPollCommand::Checkout { session_id, reply }) => {
                            if polling.contains_key(&session_id) {
                                if let Some(previous) = pending_checkouts.insert(session_id, reply) {
                                    let _ = previous.send(None);
                                }
                            } else {
                                checked_out.insert(session_id.clone());
                                let _ = reply.send(clients.remove(&session_id));
                            }
                        }
                        Some(WorkerPollCommand::Checkin { session_id, worker }) => {
                            checked_out.remove(&session_id);
                            if let Some(worker) = worker
                                && targets.get(&session_id).is_some_and(|target| target.spec == worker.spec)
                            {
                                clients.insert(session_id, *worker);
                            }
                        }
                        None => break,
                    }
                }
                completion = completions_rx.recv() => {
                    let Some(completion) = completion else { break };
                    let session_id = completion.target.session_id.clone();
                    let current_poll = polling
                        .get(&session_id)
                        .is_some_and(|spec| spec == &completion.target.spec);
                    if current_poll {
                        polling.remove(&session_id);
                    }
                    let target_is_current = current_poll
                        && targets
                            .get(&session_id)
                            .is_some_and(|target| target.spec == completion.target.spec);
                    if !target_is_current {
                        if let Some(reply) = pending_checkouts.remove(&session_id) {
                            if targets.contains_key(&session_id) {
                                checked_out.insert(session_id);
                            }
                            let _ = reply.send(None);
                        }
                        continue;
                    }
                    match completion.result {
                        Ok(completed) => {
                            let recovered = failures.remove(&session_id).is_some();
                            if let Some(reply) = pending_checkouts.remove(&session_id) {
                                checked_out.insert(session_id);
                                let mut worker = completed.worker;
                                worker.opening_events = completed.events;
                                let _ = reply.send(Some(worker));
                                continue;
                            }
                            if completed.connected || recovered {
                                let _ = updates_tx.send(WorkerPollUpdate {
                                    session_id: session_id.clone(),
                                    payload: WorkerPollPayload::Connected,
                                });
                            }
                            if !completed.events.is_empty() || completed.connected {
                                let _ = updates_tx.send(WorkerPollUpdate {
                                    session_id: session_id.clone(),
                                    payload: WorkerPollPayload::Events {
                                        events: completed.events,
                                        phase: completed.phase,
                                        transcript: completed
                                            .transcript
                                            .expect("changed worker poll includes transcript"),
                                        queued_prompts: completed.worker.queued_prompts.clone(),
                                        received_while_detached: completed.connected,
                                    },
                                });
                            }
                            clients.insert(session_id, completed.worker);
                        }
                        Err(detail) => {
                            if let Some(reply) = pending_checkouts.remove(&session_id) {
                                checked_out.insert(session_id.clone());
                                let _ = reply.send(None);
                            }
                            let entry = failures.entry(session_id.clone()).or_insert((0, String::new()));
                            entry.0 += 1;
                            entry.1 = detail;
                            if entry.0 == WORKER_POLL_FAILURE_THRESHOLD {
                                let _ = updates_tx.send(WorkerPollUpdate {
                                    session_id,
                                    payload: WorkerPollPayload::Unreachable {
                                        detail: entry.1.clone(),
                                    },
                                });
                            }
                        }
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
                    clients.retain(|id, worker| {
                        targets.get(id).is_some_and(|target| target.spec == worker.spec)
                    });
                    checked_out.retain(|id| targets.contains_key(id));
                    failures.retain(|id, _| targets.contains_key(id));
                    let stale_pending = pending_checkouts
                        .keys()
                        .filter(|id| {
                            polling
                                .get(*id)
                                .zip(targets.get(*id))
                                .is_none_or(|(polling, target)| polling != &target.spec)
                        })
                        .cloned()
                        .collect::<Vec<_>>();
                    for session_id in stale_pending {
                        if targets.contains_key(&session_id) {
                            checked_out.insert(session_id.clone());
                        }
                        if let Some(reply) = pending_checkouts.remove(&session_id) {
                            let _ = reply.send(None);
                        }
                    }
                }
            }
        }
    });
    (targets_tx, updates_rx, commands_tx)
}

async fn poll_dashboard_worker(
    target: WorkerPollTarget,
    worker: Option<WarmWorker>,
) -> WorkerPollCompletion {
    let connected = worker.is_none();
    let result = tokio::time::timeout(WORKER_POLL_TIMEOUT, async {
        let (worker, events) = match worker {
            Some(mut worker) => {
                let events = worker.client.sync().await?;
                worker.chat.apply_events(&events);
                apply_queued_prompt_events(&mut worker.queued_prompts, &events);
                (worker, events)
            }
            None => {
                let mut client = WorkerClient::connect(&target.spec, &target.session_id).await?;
                let bootstrap = client.bootstrap().await?;
                let events = bootstrap.events.clone();
                let chat = hel::hel_chat::ChatState::new(&bootstrap.snapshot, &bootstrap.events);
                (
                    WarmWorker {
                        spec: target.spec.clone(),
                        client,
                        chat,
                        queued_prompts: bootstrap.snapshot.queued_prompts.clone(),
                        opening_events: Vec::new(),
                    },
                    events,
                )
            }
        };
        let phase = worker.chat.phase();
        let transcript =
            (connected || !events.is_empty()).then(|| worker.chat.transcript_snapshot());
        Ok::<_, anyhow::Error>(CompletedWorkerPoll {
            worker,
            events,
            phase,
            transcript,
            connected,
        })
    })
    .await;
    let result = match result {
        Ok(Ok(completed)) => Ok(completed),
        Ok(Err(error)) => Err(format!("{error:#}")),
        Err(_) if connected => Err("worker connect timed out".to_string()),
        Err(_) => Err("worker poll timed out".to_string()),
    };
    WorkerPollCompletion { target, result }
}

fn apply_queued_prompt_events(
    queued: &mut Vec<hel::hel_worker::QueuedPrompt>,
    events: &[SequencedEvent],
) {
    for event in events {
        match &event.event {
            hel::hel_worker::WorkerEvent::QueuedPromptAdded { prompt } => {
                queued.push(prompt.clone());
            }
            hel::hel_worker::WorkerEvent::QueuedPromptRemoved { queue_id } => {
                queued.retain(|prompt| prompt.id != *queue_id);
            }
            hel::hel_worker::WorkerEvent::QueuedPromptPromoted { prompt, .. } => {
                queued.retain(|queued| queued.id != prompt.id);
            }
            hel::hel_worker::WorkerEvent::QueuedPromptsCleared => queued.clear(),
            _ => {}
        }
    }
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
    profiles_tx: &tokio::sync::watch::Sender<Vec<QuotaRefreshRequest>>,
) {
    let profiles = quota_refresh_profiles(controller);
    dashboard.begin_quota_refresh(profiles.iter().map(|profile| profile.profile_id.clone()));
    profiles_tx.send_replace(profiles);
}

fn apply_worker_poll_update(
    controller: &mut Controller,
    dashboard: &mut DashboardState,
    update: WorkerPollUpdate,
) -> Result<bool> {
    let mut latest_message_updated = false;
    match update.payload {
        WorkerPollPayload::Connected => {
            if let Some(session) = controller.state.sessions.get_mut(&update.session_id)
                && session.state == SessionState::Error
                && session
                    .last_error
                    .as_deref()
                    .is_some_and(|message| message.starts_with("worker unreachable:"))
            {
                session.state = SessionState::Running;
                session.last_error = None;
                hel::hel_database::save_session(session)?;
                dashboard.set_state(controller.state.clone());
            }
        }
        WorkerPollPayload::Events {
            events,
            phase,
            transcript,
            received_while_detached,
            queued_prompts,
        } => {
            if let Some(title) = harness_session_title(&events)
                && let Some(session) = controller.state.sessions.get_mut(&update.session_id)
                && session.acp_session_title.as_deref() != Some(&title)
            {
                session.acp_session_title = Some(title);
                hel::hel_database::save_session(session)?;
                dashboard.set_state(controller.state.clone());
            }
            latest_message_updated = dashboard.apply_worker_update(
                &update.session_id,
                &events,
                phase,
                current_epoch_seconds(),
                received_while_detached,
            );
            dashboard.apply_transcript(&update.session_id, transcript);
            dashboard.apply_queued_prompts(&update.session_id, queued_prompts);
        }
        WorkerPollPayload::Unreachable { detail } => {
            let diagnosis = controller.diagnose_worker(&update.session_id);
            let mut message = format!("worker unreachable: {detail}");
            if let Some(diagnosis) = diagnosis {
                message.push_str("; ");
                message.push_str(&diagnosis);
            }
            dashboard.set_notice(format!(
                "Session {}: {message}",
                &update.session_id[..update.session_id.len().min(8)]
            ));
        }
    }
    Ok(latest_message_updated)
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
    let Some(session) = controller.state.sessions.get_mut(&result.session_id) else {
        return false;
    };
    if session.target.as_ref() != Some(&result.expected_target) || !session.state.is_active() {
        return false;
    }
    match result.outcome {
        Ok(artifact) => {
            session.native_session_id = Some(artifact.native_session_id);
            session.checkpoint = Some(artifact.metadata.clone());
            session.last_checkpoint_error = None;
            if let Some(previous) = result
                .previous_checkpoint
                .filter(|previous| previous.archive_path != artifact.metadata.archive_path)
                && let Err(error) = std::fs::remove_file(&previous.archive_path)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                tracing::warn!(path = %previous.archive_path.display(), "could not remove superseded recovery copy: {error}");
            }
        }
        Err(detail) => session.last_checkpoint_error = Some(detail),
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
        bootstrap: Box<WorkerBootstrap>,
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

enum DashboardIoUpdate {
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
    for (session_id, queued) in archived_queued_prompts(&controller) {
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
        spawn_dashboard_worker_poller();
    let mut recovery = hel::hel_recovery::RecoveryCoordinator::spawn();
    let recovery_observer = recovery.observer();
    let mut credential_sync = CredentialSyncCoordinator::spawn();
    let credential_sync_handle = credential_sync.handle();
    let mut auth_failure_syncs = std::collections::BTreeMap::<String, Instant>::new();
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
        &std::collections::BTreeSet::new(),
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
    let (lifecycle_updates_tx, mut lifecycle_updates_rx) =
        tokio::sync::mpsc::unbounded_channel::<LifecycleUpdate>();
    let (dashboard_io_tx, mut dashboard_io_rx) =
        tokio::sync::mpsc::unbounded_channel::<DashboardIoUpdate>();
    let mut lifecycle_operations =
        std::collections::BTreeMap::<String, ActiveLifecycleOperation>::new();
    let termination = hel::termination::Coordinator::install().token();

    loop {
        let capacity_targets = controller.deployment_capacity_targets();
        if *capacity_targets_tx.borrow() != capacity_targets {
            capacity_targets_tx.send_replace(capacity_targets.clone());
            dashboard.set_deployment_capacity_targets(capacity_targets);
        }
        while let Ok(update) = quota_updates_rx.try_recv() {
            match update {
                QuotaUpdate::Refreshing(ids) => dashboard.begin_quota_refresh(ids),
                QuotaUpdate::Report(quota) => dashboard.apply_quota(quota),
            }
        }
        while let Ok(update) = worker_updates_rx.try_recv() {
            let session_id = update.session_id.clone();
            if let WorkerPollPayload::Events { events, phase, .. } = &update.payload
                && let Some(session) = controller.state.sessions.get(&session_id).cloned()
            {
                if hel::hel_credentials::events_report_auth_failure(session.harness_kind, events)
                    && auth_failure_sync_is_due(
                        &mut auth_failure_syncs,
                        &session_id,
                        Instant::now(),
                    )
                {
                    credential_sync_handle
                        .sync_profile_now(&session.last_profile, Some(&session_id));
                }
                recovery_observer
                    .observe(hel::hel_recovery::RecoveryObservation {
                        session,
                        config: controller.config.clone(),
                        events: events.clone(),
                        phase: *phase,
                    })
                    .await;
            }
            match apply_worker_poll_update(&mut controller, &mut dashboard, update) {
                Ok(true) => {
                    let _ = resource_triggers_tx.try_send(session_id);
                }
                Ok(false) => {}
                Err(error) => {
                    dashboard.set_notice(format!("Could not save harness title: {error:#}"));
                }
            }
        }
        while let Some(result) = recovery.try_result() {
            apply_recovery_result(&mut controller, &mut dashboard, result);
        }
        while let Some(result) = credential_sync.try_result() {
            if let Some(notice) = credential_sync_notice(&result) {
                dashboard.set_notice(notice);
            }
        }
        while let Ok(update) = resource_updates_rx.try_recv() {
            dashboard.apply_resource_usage(&update.session_id, update.usage);
        }
        while let Ok(update) = capacity_updates_rx.try_recv() {
            dashboard.apply_deployment_capacity(
                &update.target_id,
                update.result,
                update.sampled_at_epoch_seconds,
            );
        }
        while let Ok((target_id, result)) = aws_resource_options_rx.try_recv() {
            resolving_aws_resource_options.remove(&target_id);
            dashboard.apply_aws_resource_options(&target_id, result);
        }
        while let Ok((discovery_id, profile)) = import_updates_rx.try_recv() {
            dashboard.apply_import_profile(discovery_id, profile);
        }
        while let Ok(update) = import_task_rx.try_recv() {
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
                        Ok(DashboardImportTaskResult::Imported(mut imported)) => {
                            let applied = (|| -> Result<()> {
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
                                controller
                                    .config
                                    .bundles
                                    .insert(session.bundle_id.clone(), bundle);
                                controller
                                    .state
                                    .sessions
                                    .insert(session.id.clone(), session);
                                controller.config.save()?;
                                hel::hel_database::save_session(
                                    controller
                                        .state
                                        .sessions
                                        .get(&imported.session_id)
                                        .context("imported session disappeared before save")?,
                                )?;
                                Ok(())
                            })();
                            dashboard.finish_import();
                            match applied {
                                Ok(()) => {
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
                                        imported.harness, pending.native_session_id
                                    ));
                                }
                                Err(error) => {
                                    dashboard.set_notice(format!("Import failed: {error:#}"));
                                }
                            }
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
        while let Ok(update) = lifecycle_updates_rx.try_recv() {
            let session_id = update.session_id;
            let operation = lifecycle_operations.remove(&session_id);
            dashboard.finish_session_operation(&session_id);
            if let Err(error) = controller.reload() {
                dashboard.set_notice(format!("Could not reload completed operation: {error:#}"));
                continue;
            }
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
                    bootstrap,
                }) => {
                    let chat =
                        hel::hel_chat::ChatState::new(&bootstrap.snapshot, &bootstrap.events);
                    dashboard.apply_transcript(&session_id, chat.transcript_snapshot());
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
                    dashboard.set_notice(format!("Paused {}", short_id(&session_id)));
                }
                Ok(LifecycleSuccess::Destroyed) => dashboard.set_notice(format!(
                    "Destroyed {} without an archive",
                    short_id(&session_id)
                )),
                Ok(LifecycleSuccess::DeletedActive) => dashboard.set_notice(format!(
                    "Deleted active session {} without checkpointing",
                    short_id(&session_id)
                )),
                Ok(LifecycleSuccess::DeletedArchived) => dashboard.set_notice(format!(
                    "Permanently deleted paused session {}",
                    short_id(&session_id)
                )),
                Err(error) => {
                    if operation
                        .as_ref()
                        .is_some_and(|operation| operation.kind == SessionOperationKind::Pausing)
                    {
                        dashboard.show_close_failure(session_id.clone(), error);
                    } else {
                        let label =
                            operation.map_or("Operation", |operation| operation.kind.label());
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
        while let Ok(update) = dashboard_io_rx.try_recv() {
            match update {
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
        terminal
            .terminal
            .draw(|frame| render(frame, &mut dashboard))?;
        if termination.is_cancelled() {
            break;
        }
        if !event::poll(Duration::from_millis(250))? {
            continue;
        }
        let action = match event::read()? {
            Event::Key(key) => dashboard.handle_key(key),
            Event::Paste(pasted) => {
                dashboard.handle_paste(&pasted);
                DashboardAction::None
            }
            Event::Mouse(mouse) => {
                dashboard.handle_mouse(mouse);
                DashboardAction::None
            }
            _ => continue,
        };
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
                request_dashboard_quota_refresh(&controller, &mut dashboard, &quota_profiles_tx);
                dashboard.set_notice("Refreshing profile quotas…");
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
                match controller.rename_session(&session_id, &title) {
                    Ok(title) => {
                        dashboard.set_state(controller.state.clone());
                        dashboard.set_notice(format!("Renamed session to {title}"));
                    }
                    Err(error) => dashboard.set_notice(format!("Rename failed: {error:#}")),
                }
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
                if !allow_dirty_local && project_directory.is_none() {
                    let dirty = controller
                        .config
                        .bundles
                        .get(&bundle_id)
                        .with_context(|| format!("unknown bundle {bundle_id:?}"))
                        .and_then(hel::hel_local_git::dirty_local_repositories);
                    match dirty {
                        Ok(dirty) if !dirty.is_empty() => {
                            let repositories = dirty
                                .into_iter()
                                .map(|repository| {
                                    format!("{}: {}", repository.path.display(), repository.summary)
                                })
                                .collect();
                            dashboard.show_dirty_local_confirmation(
                                DashboardAction::CreateSession {
                                    profile_id,
                                    bundle_id,
                                    project_directory,
                                    target_template_id,
                                    additional_mounts,
                                    allow_dirty_local: false,
                                    resource_allocation,
                                },
                                repositories,
                            );
                            continue;
                        }
                        Err(error) => {
                            dashboard.set_notice(format!(
                                "Could not inspect local repository: {error:#}"
                            ));
                            continue;
                        }
                        _ => {}
                    }
                }
                let title = format!(
                    "{} via {profile_id}",
                    project_directory
                        .as_ref()
                        .map(|path| path.display().to_string())
                        .unwrap_or_else(|| bundle_id.clone())
                );
                match controller.register_session_with_resources(
                    &profile_id,
                    &bundle_id,
                    &target_template_id,
                    title,
                    SessionLaunchOptions {
                        additional_mounts,
                        allow_dirty_local,
                        resource_allocation,
                        project_directory,
                    },
                ) {
                    Ok(session_id) => {
                        dashboard.set_state(controller.state.clone());
                        dashboard.begin_session_operation(
                            session_id.clone(),
                            SessionOperationKind::Launching,
                            None,
                        );
                        dashboard.set_notice(format!("Launching {}…", short_id(&session_id)));
                        let cancelled = Arc::new(AtomicBool::new(false));
                        lifecycle_operations.insert(
                            session_id.clone(),
                            ActiveLifecycleOperation {
                                cancelled: cancelled.clone(),
                                kind: SessionOperationKind::Launching,
                            },
                        );
                        let updates = lifecycle_updates_tx.clone();
                        let operation_session_id = session_id.clone();
                        let runtime = tokio::runtime::Handle::current();
                        tokio::task::spawn_blocking(move || {
                            let result =
                                (|| -> Result<()> {
                                    let mut controller = Controller::load()?;
                                    let executor = CancellableProcessExecutor::new(cancelled);
                                    runtime.block_on(controller.provision_session_controlled(
                                        &operation_session_id,
                                        &executor,
                                    ))
                                })()
                                .map(|()| LifecycleSuccess::Created)
                                .map_err(|error| format!("{error:#}"));
                            let _ = updates.send(LifecycleUpdate {
                                session_id: operation_session_id,
                                result,
                            });
                        });
                    }
                    Err(error) => {
                        dashboard.set_notice(format!("Could not create session: {error:#}"))
                    }
                }
            }
            DashboardAction::Open { session_id } => {
                let session = controller
                    .state
                    .sessions
                    .get(&session_id)
                    .with_context(|| format!("unknown session {session_id}"))?
                    .clone();
                let bundle_id = session.bundle_id.clone();
                let recovery_context = hel::hel_recovery::RecoveryContext {
                    observer: recovery_observer.clone(),
                    session,
                    config: controller.config.clone(),
                };
                let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
                worker_commands_tx
                    .send(WorkerPollCommand::Checkout {
                        session_id: session_id.clone(),
                        reply: reply_tx,
                    })
                    .await
                    .context("dashboard worker poller stopped")?;
                let warm = reply_rx.await.context("dashboard worker poller stopped")?;
                let result = match warm {
                    Some(worker) => {
                        let spec = worker.spec;
                        if let Err(error) = apply_worker_poll_update(
                            &mut controller,
                            &mut dashboard,
                            WorkerPollUpdate {
                                session_id: session_id.clone(),
                                payload: WorkerPollPayload::Connected,
                            },
                        ) {
                            dashboard.set_notice(format!("Could not save worker state: {error:#}"));
                        }
                        if !worker.opening_events.is_empty() {
                            recovery_context
                                .observe(&worker.opening_events, worker.chat.phase())
                                .await;
                            let update = WorkerPollUpdate {
                                session_id: session_id.clone(),
                                payload: WorkerPollPayload::Events {
                                    events: worker.opening_events,
                                    phase: worker.chat.phase(),
                                    transcript: worker.chat.transcript_snapshot(),
                                    queued_prompts: worker.queued_prompts.clone(),
                                    received_while_detached: true,
                                },
                            };
                            if let Err(error) =
                                apply_worker_poll_update(&mut controller, &mut dashboard, update)
                            {
                                dashboard
                                    .set_notice(format!("Could not save harness title: {error:#}"));
                            }
                        }
                        hel::hel_chat::run_chat(
                            &mut terminal.terminal,
                            worker.client,
                            Some(worker.chat),
                            &bundle_id,
                            Some(recovery_context.clone()),
                        )
                        .await
                        .map(|(exit, client, chat)| {
                            let queued_prompts = chat.queued_prompt_snapshot();
                            (
                                exit,
                                Some(WarmWorker {
                                    spec,
                                    client,
                                    chat,
                                    queued_prompts,
                                    opening_events: Vec::new(),
                                }),
                            )
                        })
                    }
                    None => {
                        async {
                            let spec = controller.reconnect_command(&session_id)?;
                            let client = WorkerClient::connect(&spec, &session_id).await?;
                            let (exit, client, chat) = hel::hel_chat::run_chat(
                                &mut terminal.terminal,
                                client,
                                None,
                                &bundle_id,
                                Some(recovery_context),
                            )
                            .await?;
                            let queued_prompts = chat.queued_prompt_snapshot();
                            Ok((
                                exit,
                                Some(WarmWorker {
                                    spec,
                                    client,
                                    chat,
                                    queued_prompts,
                                    opening_events: Vec::new(),
                                }),
                            ))
                        }
                        .await
                    }
                };
                match result {
                    Ok((exit, worker)) => {
                        let (last_seen_event_sequence, quit_after_detach) = match exit {
                            hel::hel_chat::ChatExit::Detached {
                                last_seen_event_sequence,
                            } => (last_seen_event_sequence, false),
                            hel::hel_chat::ChatExit::QuitDetached {
                                last_seen_event_sequence,
                            } => (last_seen_event_sequence, true),
                        };
                        if let Some(worker) = worker.as_ref() {
                            dashboard.apply_worker_update(
                                &session_id,
                                &[],
                                worker.chat.phase(),
                                current_epoch_seconds(),
                                false,
                            );
                            dashboard
                                .apply_transcript(&session_id, worker.chat.transcript_snapshot());
                            dashboard
                                .apply_queued_prompts(&session_id, worker.queued_prompts.clone());
                        }
                        worker_commands_tx
                            .send(WorkerPollCommand::Checkin {
                                session_id: session_id.clone(),
                                worker: worker.map(Box::new),
                            })
                            .await
                            .context("dashboard worker poller stopped")?;
                        while let Some(result) = recovery.try_result() {
                            apply_recovery_result(&mut controller, &mut dashboard, result);
                        }
                        let read_result = controller
                            .mark_session_viewed_through(&session_id, last_seen_event_sequence);
                        dashboard.set_state(controller.state.clone());
                        match read_result {
                            Ok(()) => dashboard.clear_notice(),
                            Err(error) => dashboard.set_notice(format!(
                                "Could not save read status for {}: {error:#}",
                                short_id(&session_id)
                            )),
                        }
                        if quit_after_detach {
                            quit_detached = true;
                            break;
                        }
                    }
                    Err(error) => {
                        worker_commands_tx
                            .send(WorkerPollCommand::Checkin {
                                session_id: session_id.clone(),
                                worker: None,
                            })
                            .await
                            .context("dashboard worker poller stopped")?;
                        dashboard.set_notice(format!("Could not open session: {error:#}"))
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
                let operation_session_id = session_id.clone();
                let operation_profile_id = profile_id.clone();
                let operation_target_id = target_template_id.clone();
                let runtime = tokio::runtime::Handle::current();
                tokio::task::spawn_blocking(move || {
                    let result = (|| -> Result<WorkerBootstrap> {
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
                    .map(|bootstrap| LifecycleSuccess::Resumed {
                        profile_id: operation_profile_id,
                        target_id: operation_target_id,
                        bootstrap: Box::new(bootstrap),
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
                let operation_session_id = session_id.clone();
                let runtime = tokio::runtime::Handle::current();
                tokio::task::spawn_blocking(move || {
                    let result = (|| -> Result<()> {
                        while observer.is_busy(&operation_session_id) {
                            if cancelled.load(Ordering::Acquire) {
                                bail!("operation cancelled while waiting for recovery copy");
                            }
                            std::thread::sleep(Duration::from_millis(25));
                        }
                        let mut controller = Controller::load()?;
                        let executor = CancellableProcessExecutor::new(cancelled.clone());
                        runtime.block_on(
                            controller.close_session_controlled(&operation_session_id, &executor),
                        )
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
                match create_quick_bundle(&mut controller.config, &source) {
                    Ok(bundle_id) => {
                        controller.config.save()?;
                        let followup =
                            dashboard.apply_created_bundle(controller.config.clone(), &bundle_id);
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
                        dashboard.set_notice(format!("Could not create bundle: {error:#}"))
                    }
                }
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
                let updates = lifecycle_updates_tx.clone();
                let operation_session_id = session_id.clone();
                tokio::task::spawn_blocking(move || {
                    let result = (|| -> Result<()> {
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
                let updates = lifecycle_updates_tx.clone();
                let operation_session_id = session_id.clone();
                tokio::task::spawn_blocking(move || {
                    let result = (|| -> Result<()> {
                        let mut controller = Controller::load()?;
                        let executor = CancellableProcessExecutor::new(cancelled);
                        controller.force_destroy(&operation_session_id, &executor)?;
                        delete_archived_session(&mut controller, &operation_session_id)
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
                        delete_archived_session(&mut controller, &operation_session_id)
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

fn delete_archived_session(controller: &mut Controller, session_id: &str) -> Result<()> {
    let session = controller.state.remove_archived_session(session_id)?;
    let archive_path = session
        .checkpoint
        .as_ref()
        .map(|checkpoint| checkpoint.archive_path.clone());
    if let Err(error) = hel::hel_database::delete_session(session_id) {
        controller
            .state
            .sessions
            .insert(session.id.clone(), session);
        return Err(error).context("delete paused session from state");
    }
    if let Some(archive_path) = &archive_path
        && let Err(error) = std::fs::remove_file(archive_path)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        return Err(error).with_context(|| {
            format!(
                "session was deleted, but its recovery archive could not be removed: {}",
                archive_path.display()
            )
        });
    }
    Ok(())
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

    #[tokio::test]
    async fn slow_worker_poll_does_not_block_another_session_checkout() {
        let (targets, _updates, commands) =
            spawn_dashboard_worker_poller_with_interval(Duration::from_millis(10));
        targets.send_replace(vec![
            WorkerPollTarget {
                session_id: "a-slow".into(),
                spec: CommandSpec::new("sh", ["-c", "sleep 5"]),
            },
            WorkerPollTarget {
                session_id: "b-fast".into(),
                spec: CommandSpec::new("false", std::iter::empty::<&str>()),
            },
        ]);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let (reply, received) = tokio::sync::oneshot::channel();
        commands
            .send(WorkerPollCommand::Checkout {
                session_id: "b-fast".into(),
                reply,
            })
            .await
            .unwrap();

        let checked_out = tokio::time::timeout(Duration::from_millis(250), received)
            .await
            .expect("checkout should not wait for the unrelated slow worker")
            .expect("worker poller should answer checkout");
        assert!(checked_out.is_none());
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
    fn a_retry_loop_triggers_at_most_one_credential_sync_per_cooldown() {
        let mut attempts = std::collections::BTreeMap::new();
        let started = Instant::now();
        assert!(auth_failure_sync_is_due(&mut attempts, "session", started));
        assert!(!auth_failure_sync_is_due(
            &mut attempts,
            "session",
            started + Duration::from_secs(60)
        ));
        assert!(auth_failure_sync_is_due(
            &mut attempts,
            "other",
            started + Duration::from_secs(60)
        ));
        assert!(auth_failure_sync_is_due(
            &mut attempts,
            "session",
            started + AUTH_FAILURE_SYNC_COOLDOWN
        ));
    }

    #[test]
    fn a_healthy_credential_cycle_stays_out_of_the_ui() {
        let result = hel::hel_credentials::CredentialSyncResult {
            profile_id: "work".into(),
            triggered_by: None,
            failure: None,
            outcomes: Vec::new(),
        };
        assert_eq!(credential_sync_notice(&result), None);
    }

    #[test]
    fn an_authentication_failure_notice_says_whether_anything_was_pushed() {
        use hel::hel_credentials::{
            CredentialSyncAction, CredentialSyncOutcome, CredentialSyncResult,
        };

        let pushed = CredentialSyncResult {
            profile_id: "work".into(),
            triggered_by: Some("018f9dd2-a3b4".into()),
            failure: None,
            outcomes: vec![CredentialSyncOutcome {
                session_id: "018f9dd2-a3b4".into(),
                outcome: Ok(CredentialSyncAction::Pushed),
            }],
        };
        let notice = credential_sync_notice(&pushed).unwrap();
        assert!(notice.contains("were pushed"), "{notice}");
        assert!(notice.contains("hel login --profile work"), "{notice}");

        let nothing_to_push = CredentialSyncResult {
            triggered_by: Some("018f9dd2-a3b4".into()),
            outcomes: Vec::new(),
            ..pushed
        };
        let notice = credential_sync_notice(&nothing_to_push).unwrap();
        assert!(notice.contains("nothing fresher"), "{notice}");
        assert!(notice.contains("hel login --profile work"), "{notice}");
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
        let notice = credential_sync_notice(&result).unwrap();
        assert!(notice.contains("worker proxy disconnected"), "{notice}");
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
}
