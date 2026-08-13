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
    Event,
};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use hel::hel_config::{HelConfig, ProjectBundle, ProjectRepository, config_path, sessions_dir};
use hel::hel_controller::{Controller, SessionLaunchOptions};
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
use hel::hel_server::{ControllerAction, ServerOptions, ViewerQuota, ViewerSnapshot};
use hel::hel_setup::{SetupOutcome, github_repository_from_origin, run_setup_dialog};
use hel::hel_state::{HelState, SessionResourceAllocation, SessionState, harness_session_title};
use hel::hel_targets::{
    CommandOutput, CommandSpec, DeploymentCapacityKind, DeploymentCapacityTarget,
    DeploymentCapacityUsage, ProcessExecutor, SessionResourceProbe, SessionResourceUsage,
};
use hel::hel_tui::{
    DashboardAction, DashboardState, ImportProfileOption, ImportSessionOption, render,
};
use hel::hel_worker::{SequencedEvent, WorkerPhase};
use hel::hel_worker_client::WorkerClient;
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
    state.save()?;
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
    state.save()?;
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
    state.save()?;
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
    let (snapshot_tx, snapshot_rx) =
        tokio::sync::watch::channel(viewer_snapshot(&controller, &quotas, revision));
    let (action_tx, mut action_rx) = tokio::sync::mpsc::channel(32);
    let termination = hel::termination::Coordinator::install().token();
    let mut options = ServerOptions::new(bind, snapshot_rx, action_tx)?;
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
        loop {
            tokio::select! {
                _ = termination.cancelled() => break,
                action = action_rx.recv() => {
                    let Some(action) = action else { break };
                    apply_phone_action(&mut controller, action).await;
                    revision += 1;
                    let _ = snapshot_tx.send(viewer_snapshot(&controller, &quotas, revision));
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

async fn apply_phone_action(controller: &mut Controller, action: ControllerAction) {
    let result = match action {
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
            worker_prompt(controller, &session_id, text).await
        }
        ControllerAction::Checkpoint { session_id } => {
            controller.checkpoint_session(&session_id).await.map(|_| ())
        }
        ControllerAction::Close { session_id } => controller.close_session(&session_id).await,
        ControllerAction::Resume {
            session_id,
            profile_id,
            target_id,
        } => {
            controller
                .resume_session(&session_id, &profile_id, &target_id)
                .await
        }
        ControllerAction::Open { .. } => Ok(()),
    };
    if let Err(error) = result {
        eprintln!("Hel phone action failed: {error:#}");
    }
}

fn viewer_snapshot(
    controller: &Controller,
    quotas: &QuotaManager,
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
    snapshot
}

async fn refresh_all_quotas(controller: &Controller, quotas: &mut QuotaManager) {
    quotas
        .refresh_profiles(quota_refresh_profiles(controller))
        .await;
}

async fn worker_prompt(controller: &Controller, session_id: &str, text: String) -> Result<()> {
    let bundle_id = controller
        .state
        .sessions
        .get(session_id)
        .with_context(|| format!("unknown session {session_id}"))?
        .bundle_id
        .clone();
    let spec = controller.reconnect_command(session_id)?;
    let mut client = WorkerClient::connect(&spec, session_id).await?;
    let sequence = client.prompt(text.clone(), Vec::new()).await?;
    if let Err(error) =
        hel::hel_database::record_prompt(session_id, &bundle_id, sequence, None, &text)
    {
        tracing::warn!(
            session_id,
            "prompt sent but history was not saved: {error:#}"
        );
    }
    client.detach().await
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
) {
    worker_targets_tx.send_replace(dashboard_worker_targets(controller));
    resource_targets_tx.send_replace(dashboard_resource_targets(controller));
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
    tokio::sync::mpsc::Receiver<WorkerPollUpdate>,
    tokio::sync::mpsc::Sender<WorkerPollCommand>,
) {
    let (targets_tx, mut targets_rx) = tokio::sync::watch::channel(Vec::<WorkerPollTarget>::new());
    let (updates_tx, updates_rx) = tokio::sync::mpsc::channel(64);
    let (commands_tx, mut commands_rx) = tokio::sync::mpsc::channel(8);
    tokio::spawn(async move {
        let mut targets: std::collections::BTreeMap<String, WorkerPollTarget> =
            std::collections::BTreeMap::new();
        let mut clients: std::collections::BTreeMap<String, WarmWorker> =
            std::collections::BTreeMap::new();
        let mut checked_out = std::collections::BTreeSet::new();
        let mut failures: std::collections::BTreeMap<String, (u32, String)> =
            std::collections::BTreeMap::new();
        let mut interval = tokio::time::interval(WORKER_POLL_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if !poll_dashboard_workers(&targets, &mut clients, &checked_out, &mut failures, &updates_tx).await {
                        break;
                    }
                }
                command = commands_rx.recv() => {
                    match command {
                        Some(WorkerPollCommand::Checkout { session_id, reply }) => {
                            checked_out.insert(session_id.clone());
                            let _ = reply.send(clients.remove(&session_id));
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
                }
            }
        }
    });
    (targets_tx, updates_rx, commands_tx)
}

async fn poll_dashboard_workers(
    targets: &std::collections::BTreeMap<String, WorkerPollTarget>,
    clients: &mut std::collections::BTreeMap<String, WarmWorker>,
    checked_out: &std::collections::BTreeSet<String>,
    failures: &mut std::collections::BTreeMap<String, (u32, String)>,
    updates: &tokio::sync::mpsc::Sender<WorkerPollUpdate>,
) -> bool {
    for target in targets.values() {
        if checked_out.contains(&target.session_id) {
            continue;
        }
        let synced = match clients.get_mut(&target.session_id) {
            Some(worker) if worker.spec == target.spec => {
                Some(tokio::time::timeout(WORKER_POLL_TIMEOUT, worker.client.sync()).await)
            }
            _ => None,
        };
        let failure = match synced {
            Some(Ok(Ok(events))) => {
                let recovered = failures.remove(&target.session_id).is_some();
                let worker = clients
                    .get_mut(&target.session_id)
                    .expect("a successfully synced worker remains cached");
                worker.chat.apply_events(&events);
                let phase = worker.chat.phase();
                let transcript = worker.chat.transcript_snapshot();
                if recovered
                    && updates
                        .send(WorkerPollUpdate {
                            session_id: target.session_id.clone(),
                            payload: WorkerPollPayload::Connected,
                        })
                        .await
                        .is_err()
                {
                    return false;
                }
                if !events.is_empty()
                    && updates
                        .send(WorkerPollUpdate {
                            session_id: target.session_id.clone(),
                            payload: WorkerPollPayload::Events {
                                events,
                                phase,
                                transcript,
                            },
                        })
                        .await
                        .is_err()
                {
                    return false;
                }
                None
            }
            Some(Ok(Err(error))) => {
                clients.remove(&target.session_id);
                Some(format!("{error:#}"))
            }
            Some(Err(_)) => {
                clients.remove(&target.session_id);
                Some("worker poll timed out".to_string())
            }
            None => {
                let connected = tokio::time::timeout(WORKER_POLL_TIMEOUT, async {
                    let mut client =
                        WorkerClient::connect(&target.spec, &target.session_id).await?;
                    let bootstrap = client.bootstrap().await?;
                    Ok::<_, anyhow::Error>((client, bootstrap))
                })
                .await;
                match connected {
                    Ok(Ok((client, bootstrap))) => {
                        failures.remove(&target.session_id);
                        let events = bootstrap.events.clone();
                        let chat =
                            hel::hel_chat::ChatState::new(&bootstrap.snapshot, &bootstrap.events);
                        let phase = chat.phase();
                        let transcript = chat.transcript_snapshot();
                        clients.insert(
                            target.session_id.clone(),
                            WarmWorker {
                                spec: target.spec.clone(),
                                client,
                                chat,
                            },
                        );
                        if updates
                            .send(WorkerPollUpdate {
                                session_id: target.session_id.clone(),
                                payload: WorkerPollPayload::Connected,
                            })
                            .await
                            .is_err()
                        {
                            return false;
                        }
                        if updates
                            .send(WorkerPollUpdate {
                                session_id: target.session_id.clone(),
                                payload: WorkerPollPayload::Events {
                                    events,
                                    phase,
                                    transcript,
                                },
                            })
                            .await
                            .is_err()
                        {
                            return false;
                        }
                        None
                    }
                    Ok(Err(error)) => Some(format!("{error:#}")),
                    Err(_) => Some("worker connect timed out".to_string()),
                }
            }
        };
        if let Some(detail) = failure {
            let entry = failures
                .entry(target.session_id.clone())
                .or_insert((0, String::new()));
            entry.0 += 1;
            entry.1 = detail;
            if entry.0 == WORKER_POLL_FAILURE_THRESHOLD {
                let detail = entry.1.clone();
                if updates
                    .send(WorkerPollUpdate {
                        session_id: target.session_id.clone(),
                        payload: WorkerPollPayload::Unreachable { detail },
                    })
                    .await
                    .is_err()
                {
                    return false;
                }
            }
        }
    }
    true
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
                controller.state.save()?;
                dashboard.set_state(controller.state.clone());
            }
        }
        WorkerPollPayload::Events {
            events,
            phase,
            transcript,
        } => {
            if let Some(title) = harness_session_title(&events)
                && let Some(session) = controller.state.sessions.get_mut(&update.session_id)
                && session.acp_session_title.as_deref() != Some(&title)
            {
                session.acp_session_title = Some(title);
                controller.state.save()?;
                dashboard.set_state(controller.state.clone());
            }
            latest_message_updated = dashboard.apply_worker_update(
                &update.session_id,
                &events,
                phase,
                current_epoch_seconds(),
            );
            dashboard.apply_transcript(&update.session_id, transcript);
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

#[derive(Clone)]
struct PendingDashboardImport {
    profile_id: String,
    native_session_id: String,
    display_title: String,
}

struct ImportBundlePrompt {
    dirty_git_roots: Vec<String>,
    omitted_non_git_dirs: Vec<String>,
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

async fn run_dashboard() -> Result<()> {
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        println!("Welcome to Hel.");
        println!("Run `hel doctor` for non-interactive validation.");
        return Ok(());
    }

    let mut controller = Controller::load()?;
    let mut dashboard = DashboardState::new(
        controller.config.clone(),
        controller.state.clone(),
        std::collections::BTreeMap::new(),
    );
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
    let (resource_targets_tx, resource_triggers_tx, mut resource_updates_rx) =
        spawn_dashboard_resource_poller();
    let (capacity_targets_tx, mut capacity_updates_rx) = spawn_dashboard_capacity_poller();
    let (aws_resource_options_tx, mut aws_resource_options_rx) =
        tokio::sync::mpsc::unbounded_channel::<(
            String,
            std::result::Result<Vec<SessionResourceAllocation>, String>,
        )>();
    let mut resolving_aws_resource_options = std::collections::BTreeSet::new();
    refresh_dashboard_poll_targets(&controller, &worker_targets_tx, &resource_targets_tx);
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
                                controller.state.save()?;
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
            Event::Mouse(mouse) => {
                dashboard.handle_mouse(mouse);
                DashboardAction::None
            }
            _ => continue,
        };
        match action {
            DashboardAction::None => {}
            DashboardAction::QuitDetach => {
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
                    false,
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
            DashboardAction::ConfirmImportBundle { accepted } => {
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
                        true,
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
            } => match controller.complete_mount_source(
                &target_template_id,
                &prefix,
                &ProcessExecutor,
            ) {
                Ok(candidates) => dashboard.apply_mount_source_completions(&prefix, candidates),
                Err(error) => dashboard.set_notice(format!("Path completion failed: {error:#}")),
            },
            DashboardAction::ValidateMountSource {
                target_template_id,
                source,
            } => {
                let result = controller
                    .validate_mount_source(
                        &target_template_id,
                        std::path::Path::new(&source),
                        &ProcessExecutor,
                    )
                    .map_err(|error| format!("{error:#}"));
                dashboard.apply_mount_source_validation(&source, result);
            }
            DashboardAction::CreateSession {
                profile_id,
                bundle_id,
                target_template_id,
                additional_mounts,
                allow_dirty_local,
                resource_allocation,
            } => {
                if !allow_dirty_local {
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
                let title = format!("{bundle_id} via {profile_id}");
                match controller.register_session_with_resources(
                    &profile_id,
                    &bundle_id,
                    &target_template_id,
                    title,
                    SessionLaunchOptions {
                        additional_mounts,
                        allow_dirty_local,
                        resource_allocation,
                    },
                ) {
                    Ok(session_id) => {
                        dashboard.set_state(controller.state.clone());
                        dashboard.set_notice(format!("Provisioning {}…", short_id(&session_id)));
                        terminal
                            .terminal
                            .draw(|frame| render(frame, &mut dashboard))?;
                        let mut provisioning_controller = Controller {
                            config: controller.config.clone(),
                            state: controller.state.clone(),
                        };
                        let provisioning_session_id = session_id.clone();
                        let mut provisioning = tokio::spawn(async move {
                            let result = provisioning_controller
                                .provision_session(&provisioning_session_id)
                                .await;
                            (provisioning_controller, result)
                        });
                        let (updated_controller, provision_result) = loop {
                            tokio::select! {
                                result = &mut provisioning => {
                                    break result.context("provisioning task failed")?;
                                }
                                _ = tokio::time::sleep(Duration::from_millis(250)) => {
                                    terminal
                                        .terminal
                                        .draw(|frame| render(frame, &mut dashboard))?;
                                }
                            }
                        };
                        controller = updated_controller;
                        dashboard.set_state(controller.state.clone());
                        match provision_result {
                            Ok(()) => {
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
                            Err(error) => {
                                dashboard.set_notice(format!("Provisioning failed: {error:#}"))
                            }
                        }
                        refresh_dashboard_poll_targets(
                            &controller,
                            &worker_targets_tx,
                            &resource_targets_tx,
                        );
                    }
                    Err(error) => {
                        dashboard.set_notice(format!("Could not create session: {error:#}"))
                    }
                }
            }
            DashboardAction::Open { session_id } => {
                let bundle_id = controller
                    .state
                    .sessions
                    .get(&session_id)
                    .with_context(|| format!("unknown session {session_id}"))?
                    .bundle_id
                    .clone();
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
                        hel::hel_chat::run_chat(
                            &mut terminal.terminal,
                            worker.client,
                            Some(worker.chat),
                            &bundle_id,
                        )
                        .await
                        .map(|(exit, client, chat)| (exit, Some(WarmWorker { spec, client, chat })))
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
                            )
                            .await?;
                            Ok((exit, Some(WarmWorker { spec, client, chat })))
                        }
                        .await
                    }
                };
                match result {
                    Ok((
                        hel::hel_chat::ChatExit::Detached {
                            last_seen_event_sequence,
                        },
                        worker,
                    )) => {
                        if let Some(worker) = worker.as_ref() {
                            dashboard
                                .apply_transcript(&session_id, worker.chat.transcript_snapshot());
                        }
                        worker_commands_tx
                            .send(WorkerPollCommand::Checkin {
                                session_id: session_id.clone(),
                                worker: worker.map(Box::new),
                            })
                            .await
                            .context("dashboard worker poller stopped")?;
                        let read_result = controller
                            .mark_session_viewed_through(&session_id, last_seen_event_sequence);
                        dashboard.set_state(controller.state.clone());
                        match read_result {
                            Ok(()) => dashboard
                                .set_notice(format!("Detached from {}", short_id(&session_id))),
                            Err(error) => dashboard.set_notice(format!(
                                "Detached from {}; could not save read status: {error:#}",
                                short_id(&session_id)
                            )),
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
            } => {
                dashboard.set_notice(resume_progress_notice(
                    &session_id,
                    &profile_id,
                    &target_template_id,
                ));
                terminal
                    .terminal
                    .draw(|frame| render(frame, &mut dashboard))?;
                let resumed_chat = match controller
                    .resume_session_with_options(
                        &session_id,
                        &profile_id,
                        &target_template_id,
                        Some(additional_mounts),
                        resource_allocation,
                    )
                    .await
                {
                    Ok(bootstrap) => {
                        dashboard.set_notice(format!(
                            "Resumed {} with {profile_id} on {target_template_id}",
                            short_id(&session_id)
                        ));
                        request_dashboard_quota_refresh(
                            &controller,
                            &mut dashboard,
                            &quota_profiles_tx,
                        );
                        Some(hel::hel_chat::ChatState::new(
                            &bootstrap.snapshot,
                            &bootstrap.events,
                        ))
                    }
                    Err(error) => {
                        dashboard.set_notice(format!("Resume failed: {error:#}"));
                        None
                    }
                };
                dashboard.set_state(controller.state.clone());
                if let Some(chat) = resumed_chat {
                    dashboard.apply_transcript(&session_id, chat.transcript_snapshot());
                    dashboard.select_active_session(&session_id);
                }
                refresh_dashboard_poll_targets(
                    &controller,
                    &worker_targets_tx,
                    &resource_targets_tx,
                );
            }
            DashboardAction::Checkpoint { session_id } => {
                match controller.checkpoint_session(&session_id).await {
                    Ok(checkpoint) => dashboard.set_notice(format!(
                        "Checkpointed {} at event {}",
                        short_id(&session_id),
                        checkpoint.event_sequence
                    )),
                    Err(error) => dashboard.set_notice(format!("Checkpoint failed: {error:#}")),
                }
                dashboard.set_state(controller.state.clone());
                refresh_dashboard_poll_targets(
                    &controller,
                    &worker_targets_tx,
                    &resource_targets_tx,
                );
            }
            DashboardAction::Close { session_id } => {
                let (returned_controller, result) = close_session_with_redraw(
                    controller,
                    &session_id,
                    &mut dashboard,
                    &mut terminal,
                )
                .await?;
                controller = returned_controller;
                match result {
                    Ok(()) => dashboard.set_notice(format!("Paused {}", short_id(&session_id))),
                    Err(error) => {
                        dashboard.show_close_failure(session_id.clone(), format!("{error:#}"))
                    }
                }
                dashboard.set_state(controller.state.clone());
                refresh_dashboard_poll_targets(
                    &controller,
                    &worker_targets_tx,
                    &resource_targets_tx,
                );
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
                match controller.force_destroy(&session_id, &ProcessExecutor) {
                    Ok(()) => dashboard.set_notice(format!(
                        "Destroyed {} without an archive",
                        short_id(&session_id)
                    )),
                    Err(error) => dashboard.set_notice(format!("Destroy failed: {error:#}")),
                }
                dashboard.set_state(controller.state.clone());
                refresh_dashboard_poll_targets(
                    &controller,
                    &worker_targets_tx,
                    &resource_targets_tx,
                );
            }
            DashboardAction::DeleteArchived { session_id } => {
                match delete_archived_session(&mut controller, &session_id) {
                    Ok(()) => dashboard.set_notice(format!(
                        "Permanently deleted paused session {}",
                        short_id(&session_id)
                    )),
                    Err(error) => dashboard.set_notice(format!("Delete failed: {error:#}")),
                }
                dashboard.set_state(controller.state.clone());
                refresh_dashboard_poll_targets(
                    &controller,
                    &worker_targets_tx,
                    &resource_targets_tx,
                );
            }
        }
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
    if let Some(archive_path) = &archive_path
        && let Err(error) = std::fs::remove_file(archive_path)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        controller
            .state
            .sessions
            .insert(session.id.clone(), session);
        return Err(error)
            .with_context(|| format!("delete checkpoint archive {}", archive_path.display()));
    }
    if let Err(error) = controller.state.save() {
        controller
            .state
            .sessions
            .insert(session.id.clone(), session);
        return Err(error).context("save state after deleting paused session");
    }
    Ok(())
}

async fn close_session_with_redraw(
    mut controller: Controller,
    session_id: &str,
    dashboard: &mut DashboardState,
    terminal: &mut TerminalGuard,
) -> Result<(Controller, Result<()>)> {
    let mut visible_state = controller.state.clone();
    if let Some(session) = visible_state.sessions.get_mut(session_id) {
        session.state = SessionState::Checkpointing;
    }
    dashboard.set_state(visible_state);

    let session_id = session_id.to_string();
    let task_session_id = session_id.clone();
    let mut close = tokio::spawn(async move {
        let result = controller.close_session(&task_session_id).await;
        (controller, result)
    });
    let started = Instant::now();
    let mut interval = tokio::time::interval(Duration::from_millis(125));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut frame = 0;

    loop {
        tokio::select! {
            joined = &mut close => {
                return joined.context("close task failed");
            }
            _ = interval.tick() => {
                dashboard.set_notice(close_progress_notice(
                    &session_id,
                    started.elapsed(),
                    frame,
                ));
                frame += 1;
                terminal.terminal.draw(|frame| render(frame, dashboard))?;
            }
        }
    }
}

fn close_progress_notice(session_id: &str, elapsed: Duration, frame: usize) -> String {
    const SPINNER: [char; 4] = ['|', '/', '-', '\\'];
    format!(
        "{} Checkpointing {}… {}s",
        SPINNER[frame % SPINNER.len()],
        short_id(session_id),
        elapsed.as_secs()
    )
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
    ImportSessionOption {
        native_session_id,
        title,
        details: format!(
            "{} · {} · {} · {}",
            system_time_age(modified_at),
            branch,
            format_byte_size(size),
            display_home_relative(&cwd),
        ),
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
    safety_accepted: bool,
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
            safety_accepted,
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
    safety_accepted: bool,
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
                safety_accepted,
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
                safety_accepted,
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
                safety_accepted,
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
}

impl TerminalGuard {
    fn enter() -> Result<Self> {
        enable_raw_mode().context("enable terminal raw mode")?;
        let mut stdout = io::stdout();
        execute!(
            stdout,
            EnterAlternateScreen,
            EnableMouseCapture,
            EnableBracketedPaste
        )
        .context("enter alternate screen and enable terminal input modes")?;
        let terminal = Terminal::new(CrosstermBackend::new(stdout))?;
        Ok(Self { terminal })
    }

    fn suspend(&mut self) -> Result<()> {
        disable_raw_mode().context("disable terminal raw mode for setup")?;
        execute!(
            self.terminal.backend_mut(),
            DisableBracketedPaste,
            DisableMouseCapture,
            LeaveAlternateScreen
        )
        .context("disable terminal input modes and leave alternate screen for setup")?;
        self.terminal
            .show_cursor()
            .context("show cursor for setup")?;
        Ok(())
    }

    fn resume(&mut self) -> Result<()> {
        enable_raw_mode().context("re-enable terminal raw mode after setup")?;
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
        let _ = disable_raw_mode();
        let _ = execute!(
            self.terminal.backend_mut(),
            DisableBracketedPaste,
            DisableMouseCapture,
            LeaveAlternateScreen
        );
        let _ = self.terminal.show_cursor();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn close_progress_notice_animates_and_reports_elapsed_time() {
        assert_eq!(
            close_progress_notice("0123456789", Duration::from_secs(7), 1),
            "/ Checkpointing 01234567… 7s"
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
