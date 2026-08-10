//! Hel: a session control plane for ACP coding agents.

use std::io::{self, IsTerminal};
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use clap::{ArgGroup, Args, Parser, Subcommand, ValueEnum};
use crossterm::event::{self, Event};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use hel::hel_config::{HelConfig, config_path, sessions_dir};
use hel::hel_controller::Controller;
use hel::hel_import::{
    BundleResolution, ClaudeImportRequest, ClaudeSessionSelection, CodexImportRequest,
    CodexSessionSelection, KimiImportRequest, KimiSessionSelection, claude_config_home,
    codex_config_home, import_claude_session, import_codex_session, import_kimi_session,
    kimi_config_home, locate_claude_session, locate_codex_session, locate_kimi_session,
    read_claude_transcript, read_codex_transcript, read_kimi_transcript, resolve_bundle,
};
use hel::hel_quota::{ProfileQuota, QuotaManager};
use hel::hel_server::{ControllerAction, ServerOptions, ViewerQuota, ViewerSnapshot};
use hel::hel_setup::{SetupOutcome, run_setup_dialog};
use hel::hel_state::{HelState, SessionState, harness_session_title};
use hel::hel_targets::{CommandSpec, ProcessExecutor};
use hel::hel_tui::{DashboardAction, DashboardState, render};
use hel::hel_worker::SequencedEvent;
use hel::hel_worker_client::WorkerClient;
use hel::hel_worker_runtime::{WorkerLaunchConfig, proxy, run_daemon};
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
    /// Diagnose platform and configuration prerequisites.
    Doctor(DoctorArgs),
    /// Discover local agent homes and create an initial Hel configuration.
    Setup(SetupArgs),
    /// Adopt a native coding-agent session as an archived Hel session.
    Import(ImportArgs),
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

#[derive(Debug, Clone)]
struct QuotaRefreshProfile {
    id: String,
    harness: hel::hel_config::HarnessKind,
    home: PathBuf,
    environment: std::collections::BTreeMap<String, String>,
    cwd: PathBuf,
}

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
    Events(Vec<SequencedEvent>),
    /// The worker failed several consecutive polls; the session needs
    /// attention and a diagnosis.
    Unreachable {
        detail: String,
    },
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
            WorkerCommand::ExportCheckpoint { spec } => {
                let checkpoint = hel::hel_checkpoint::export_from_spec_file(&spec)?;
                println!("{}", serde_json::to_string(&checkpoint)?);
                Ok(())
            }
            WorkerCommand::RestoreCheckpoint { spec } => {
                hel::hel_checkpoint::restore_from_spec_file(&spec)
            }
        },
        Some(Command::Doctor(args)) => doctor(args),
        Some(Command::Setup(args)) => setup(args),
        Some(Command::Import(args)) => import(args),
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
        Some(session) => ClaudeSessionSelection::Session(session),
        None => ClaudeSessionSelection::Latest,
    };
    let source = locate_claude_session(&claude_home, &selection)?;
    println!(
        "Selected Claude session {} at {}",
        source.session_id,
        source.jsonl_path.display()
    );
    let transcript = read_claude_transcript(&source.jsonl_path)?;
    println!("Original cwd: {}", transcript.cwd.display());

    let mut config = HelConfig::load()?;
    let mut state = HelState::load()?;
    state.validate_against_config(&config)?;
    let resolution = resolve_bundle(&config, &transcript.cwd, args.bundle.as_deref())?;
    let bundle_id = match resolution {
        BundleResolution::Existing(bundle_id) => bundle_id,
        BundleResolution::Synthesized { id, bundle } => {
            let repository = bundle.primary().expect("synthesized bundle has a primary");
            if !confirm_synthesized_bundle(&id, &repository.github, &repository.destination)? {
                println!("Import cancelled; no Hel files were changed.");
                return Ok(());
            }
            config.bundles.insert(id.clone(), bundle);
            id
        }
    };
    let imported = import_claude_session(
        &config,
        &mut state,
        ClaudeImportRequest {
            claude_home: &claude_home,
            source: &source,
            transcript: &transcript,
            bundle_id: &bundle_id,
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
        Some(session) => CodexSessionSelection::Session(session),
        None => CodexSessionSelection::Latest,
    };
    let source = locate_codex_session(&codex_home, &selection)?;
    println!(
        "Selected Codex session {} at {}",
        source.session_id,
        source.jsonl_path.display()
    );
    let transcript = read_codex_transcript(&source.jsonl_path)?;
    println!("Original cwd: {}", transcript.cwd.display());

    let mut config = HelConfig::load()?;
    let mut state = HelState::load()?;
    state.validate_against_config(&config)?;
    let Some(bundle_id) =
        resolve_import_bundle(&mut config, &transcript.cwd, args.bundle.as_deref())?
    else {
        return Ok(());
    };
    let imported = import_codex_session(
        &config,
        &mut state,
        CodexImportRequest {
            codex_home: &codex_home,
            source: &source,
            transcript: &transcript,
            bundle_id: &bundle_id,
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
        Some(session) => KimiSessionSelection::Session(session),
        None => KimiSessionSelection::Latest,
    };
    let source = locate_kimi_session(&kimi_home, &selection)?;
    println!(
        "Selected Kimi session {} at {}",
        source.session_id,
        source.session_path.display()
    );
    let transcript = read_kimi_transcript(&source.session_path)?;
    println!("Original cwd: {}", transcript.cwd.display());

    let mut config = HelConfig::load()?;
    let mut state = HelState::load()?;
    state.validate_against_config(&config)?;
    let Some(bundle_id) =
        resolve_import_bundle(&mut config, &transcript.cwd, args.bundle.as_deref())?
    else {
        return Ok(());
    };
    let imported = import_kimi_session(
        &config,
        &mut state,
        KimiImportRequest {
            kimi_home: &kimi_home,
            source: &source,
            transcript: &transcript,
            bundle_id: &bundle_id,
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
    cwd: &std::path::Path,
    requested_bundle: Option<&str>,
) -> Result<Option<String>> {
    match resolve_bundle(config, cwd, requested_bundle)? {
        BundleResolution::Existing(bundle_id) => Ok(Some(bundle_id)),
        BundleResolution::Synthesized { id, bundle } => {
            let repository = bundle.primary().expect("synthesized bundle has a primary");
            if !confirm_synthesized_bundle(&id, &repository.github, &repository.destination)? {
                println!("Import cancelled; no Hel files were changed.");
                return Ok(None);
            }
            config.bundles.insert(id.clone(), bundle);
            Ok(Some(id))
        }
    }
}

fn confirm_synthesized_bundle(
    bundle_id: &str,
    github: &str,
    destination: &std::path::Path,
) -> Result<bool> {
    print!(
        "No configured bundle matches this repository. Create bundle {bundle_id:?} for {github} at {}? [y/N]: ",
        destination.display()
    );
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
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    for (id, profile) in &controller.config.profiles {
        let mut environment = profile.environment.clone();
        environment.insert(
            profile.home_env().to_string(),
            profile.home.to_string_lossy().into_owned(),
        );
        quotas
            .refresh(id, profile.kind, &profile.home, &environment, &cwd)
            .await;
    }
}

async fn worker_prompt(controller: &Controller, session_id: &str, text: String) -> Result<()> {
    let spec = controller.reconnect_command(session_id)?;
    let mut client = WorkerClient::connect(&spec, session_id).await?;
    client.prompt(text, Vec::new()).await?;
    client.detach().await
}

fn quota_refresh_profiles(controller: &Controller) -> Vec<QuotaRefreshProfile> {
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
            QuotaRefreshProfile {
                id: id.clone(),
                harness: profile.kind,
                home: profile.home.clone(),
                environment,
                cwd: cwd.clone(),
            }
        })
        .collect()
}

fn spawn_dashboard_quota_refresher() -> (
    tokio::sync::watch::Sender<Vec<QuotaRefreshProfile>>,
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
    profiles: &[QuotaRefreshProfile],
    updates: &tokio::sync::mpsc::Sender<QuotaUpdate>,
) -> bool {
    let ids = profiles
        .iter()
        .map(|profile| profile.id.clone())
        .collect::<Vec<_>>();
    if updates.send(QuotaUpdate::Refreshing(ids)).await.is_err() {
        return false;
    }
    for profile in profiles {
        let quota = quotas
            .refresh(
                &profile.id,
                profile.harness,
                &profile.home,
                &profile.environment,
                &profile.cwd,
            )
            .await;
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

fn spawn_dashboard_worker_poller() -> (
    tokio::sync::watch::Sender<Vec<WorkerPollTarget>>,
    tokio::sync::mpsc::Receiver<WorkerPollUpdate>,
) {
    let (targets_tx, mut targets_rx) = tokio::sync::watch::channel(Vec::<WorkerPollTarget>::new());
    let (updates_tx, updates_rx) = tokio::sync::mpsc::channel(64);
    tokio::spawn(async move {
        let mut targets: std::collections::BTreeMap<String, WorkerPollTarget> =
            std::collections::BTreeMap::new();
        let mut clients: std::collections::BTreeMap<String, (CommandSpec, WorkerClient)> =
            std::collections::BTreeMap::new();
        let mut failures: std::collections::BTreeMap<String, (u32, String)> =
            std::collections::BTreeMap::new();
        let mut interval = tokio::time::interval(WORKER_POLL_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if !poll_dashboard_workers(&targets, &mut clients, &mut failures, &updates_tx).await {
                        break;
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
                    clients.retain(|id, (spec, _)| {
                        targets.get(id).is_some_and(|target| spec == &target.spec)
                    });
                }
            }
        }
    });
    (targets_tx, updates_rx)
}

async fn poll_dashboard_workers(
    targets: &std::collections::BTreeMap<String, WorkerPollTarget>,
    clients: &mut std::collections::BTreeMap<String, (CommandSpec, WorkerClient)>,
    failures: &mut std::collections::BTreeMap<String, (u32, String)>,
    updates: &tokio::sync::mpsc::Sender<WorkerPollUpdate>,
) -> bool {
    for target in targets.values() {
        let synced = match clients.get_mut(&target.session_id) {
            Some((spec, client)) if spec == &target.spec => {
                Some(tokio::time::timeout(WORKER_POLL_TIMEOUT, client.sync()).await)
            }
            _ => None,
        };
        let failure = match synced {
            Some(Ok(Ok(events))) => {
                failures.remove(&target.session_id);
                if !events.is_empty()
                    && updates
                        .send(WorkerPollUpdate {
                            session_id: target.session_id.clone(),
                            payload: WorkerPollPayload::Events(events),
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
                    Ok::<_, anyhow::Error>((client, bootstrap.events))
                })
                .await;
                match connected {
                    Ok(Ok((client, events))) => {
                        failures.remove(&target.session_id);
                        clients.insert(target.session_id.clone(), (target.spec.clone(), client));
                        if !events.is_empty()
                            && updates
                                .send(WorkerPollUpdate {
                                    session_id: target.session_id.clone(),
                                    payload: WorkerPollPayload::Events(events),
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
    profiles_tx: &tokio::sync::watch::Sender<Vec<QuotaRefreshProfile>>,
) {
    let profiles = quota_refresh_profiles(controller);
    dashboard.begin_quota_refresh(profiles.iter().map(|profile| profile.id.clone()));
    profiles_tx.send_replace(profiles);
}

fn apply_worker_poll_update(
    controller: &mut Controller,
    dashboard: &mut DashboardState,
    update: WorkerPollUpdate,
) -> Result<()> {
    match update.payload {
        WorkerPollPayload::Events(events) => {
            if let Some(title) = harness_session_title(&events)
                && let Some(session) = controller.state.sessions.get_mut(&update.session_id)
                && session.acp_session_title.as_deref() != Some(&title)
            {
                session.acp_session_title = Some(title);
                controller.state.save()?;
                dashboard.set_state(controller.state.clone());
            }
            dashboard.apply_worker_events(&update.session_id, &events, current_epoch_seconds());
        }
        WorkerPollPayload::Unreachable { detail } => {
            let diagnosis = controller.diagnose_worker(&update.session_id);
            let mut message = format!("worker unreachable: {detail}");
            if let Some(diagnosis) = diagnosis {
                message.push_str("; ");
                message.push_str(&diagnosis);
            }
            if let Some(session) = controller.state.sessions.get_mut(&update.session_id)
                && session.state == SessionState::Running
            {
                session.state = SessionState::Error;
                session.last_error = Some(message.clone());
                controller.state.save()?;
                dashboard.set_state(controller.state.clone());
            }
            dashboard.set_notice(format!(
                "Session {}: {message}",
                &update.session_id[..update.session_id.len().min(8)]
            ));
        }
    }
    Ok(())
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
                dashboard.set_notice("Setup complete. Press n to start your first session.");
            }
            SetupOutcome::Cancelled => return Ok(()),
        }
    }
    let (quota_profiles_tx, mut quota_updates_rx) = spawn_dashboard_quota_refresher();
    request_dashboard_quota_refresh(&controller, &mut dashboard, &quota_profiles_tx);
    let (worker_targets_tx, mut worker_updates_rx) = spawn_dashboard_worker_poller();
    worker_targets_tx.send_replace(dashboard_worker_targets(&controller));
    let termination = hel::termination::Coordinator::install().token();

    loop {
        while let Ok(update) = quota_updates_rx.try_recv() {
            match update {
                QuotaUpdate::Refreshing(ids) => dashboard.begin_quota_refresh(ids),
                QuotaUpdate::Report(quota) => dashboard.apply_quota(quota),
            }
        }
        while let Ok(update) = worker_updates_rx.try_recv() {
            if let Err(error) = apply_worker_poll_update(&mut controller, &mut dashboard, update) {
                dashboard.set_notice(format!("Could not save harness title: {error:#}"));
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
        let Event::Key(key) = event::read()? else {
            continue;
        };
        let action = dashboard.handle_key(key);
        match action {
            DashboardAction::None => {}
            DashboardAction::QuitDetach => break,
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
                        worker_targets_tx.send_replace(dashboard_worker_targets(&controller));
                        dashboard
                            .set_notice("Setup complete. Press n to start your first session.");
                    }
                    SetupOutcome::Cancelled => dashboard.set_notice("Setup cancelled."),
                }
            }
            DashboardAction::RefreshQuotas => {
                request_dashboard_quota_refresh(&controller, &mut dashboard, &quota_profiles_tx);
                dashboard.set_notice("Refreshing profile quotas…");
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
            DashboardAction::CreateSession {
                profile_id,
                bundle_id,
                target_template_id,
                additional_mounts,
            } => {
                let title = format!("{bundle_id} via {profile_id}");
                match controller.register_session_with_mounts(
                    &profile_id,
                    &bundle_id,
                    &target_template_id,
                    title,
                    additional_mounts,
                ) {
                    Ok(session_id) => {
                        dashboard.set_state(controller.state.clone());
                        dashboard.set_notice(format!("Provisioning {}…", short_id(&session_id)));
                        terminal
                            .terminal
                            .draw(|frame| render(frame, &mut dashboard))?;
                        match controller.provision_session(&session_id).await {
                            Ok(()) => {
                                dashboard.set_notice(format!(
                                    "Target ready for {}; connecting worker",
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
                        dashboard.set_state(controller.state.clone());
                        worker_targets_tx.send_replace(dashboard_worker_targets(&controller));
                    }
                    Err(error) => {
                        dashboard.set_notice(format!("Could not create session: {error:#}"))
                    }
                }
            }
            DashboardAction::Open { session_id } => {
                let result = async {
                    let spec = controller.reconnect_command(&session_id)?;
                    let client = WorkerClient::connect(&spec, &session_id).await?;
                    hel::hel_chat::run_chat(&mut terminal.terminal, client).await?;
                    Ok::<_, anyhow::Error>(())
                }
                .await;
                match result {
                    Ok(()) => {
                        dashboard.set_notice(format!("Detached from {}", short_id(&session_id)))
                    }
                    Err(error) => {
                        dashboard.set_notice(format!("Could not open session: {error:#}"))
                    }
                }
            }
            DashboardAction::ResumeSession {
                session_id,
                profile_id,
                target_template_id,
            } => {
                match controller
                    .resume_session(&session_id, &profile_id, &target_template_id)
                    .await
                {
                    Ok(()) => {
                        dashboard.set_notice(format!(
                            "Resumed {} with {profile_id} on {target_template_id}",
                            short_id(&session_id)
                        ));
                        request_dashboard_quota_refresh(
                            &controller,
                            &mut dashboard,
                            &quota_profiles_tx,
                        );
                    }
                    Err(error) => dashboard.set_notice(format!("Resume failed: {error:#}")),
                }
                dashboard.set_state(controller.state.clone());
                worker_targets_tx.send_replace(dashboard_worker_targets(&controller));
            }
            DashboardAction::Checkpoint { session_id } => {
                match controller.checkpoint_session(&session_id).await {
                    Ok(checkpoint) => dashboard.set_notice(format!(
                        "Archived {} at event {}",
                        short_id(&session_id),
                        checkpoint.event_sequence
                    )),
                    Err(error) => dashboard.set_notice(format!("Checkpoint failed: {error:#}")),
                }
                dashboard.set_state(controller.state.clone());
                worker_targets_tx.send_replace(dashboard_worker_targets(&controller));
            }
            DashboardAction::Close { session_id } => {
                match controller.close_session(&session_id).await {
                    Ok(()) => dashboard
                        .set_notice(format!("Archived and closed {}", short_id(&session_id))),
                    Err(error) => {
                        dashboard.show_close_failure(session_id.clone(), format!("{error:#}"))
                    }
                }
                dashboard.set_state(controller.state.clone());
                worker_targets_tx.send_replace(dashboard_worker_targets(&controller));
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
                worker_targets_tx.send_replace(dashboard_worker_targets(&controller));
            }
        }
    }
    Ok(())
}

fn short_id(id: &str) -> &str {
    id.get(..8).unwrap_or(id)
}

fn configuration_needs_setup(config: &hel::hel_config::HelConfig) -> bool {
    config.profiles.is_empty() && config.bundles.is_empty() && config.targets.is_empty()
}

struct TerminalGuard {
    terminal: Terminal<CrosstermBackend<io::Stdout>>,
}

impl TerminalGuard {
    fn enter() -> Result<Self> {
        enable_raw_mode().context("enable terminal raw mode")?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen).context("enter alternate screen")?;
        let terminal = Terminal::new(CrosstermBackend::new(stdout))?;
        Ok(Self { terminal })
    }

    fn suspend(&mut self) -> Result<()> {
        disable_raw_mode().context("disable terminal raw mode for setup")?;
        execute!(self.terminal.backend_mut(), LeaveAlternateScreen)
            .context("leave alternate screen for setup")?;
        self.terminal
            .show_cursor()
            .context("show cursor for setup")?;
        Ok(())
    }

    fn resume(&mut self) -> Result<()> {
        enable_raw_mode().context("re-enable terminal raw mode after setup")?;
        execute!(self.terminal.backend_mut(), EnterAlternateScreen)
            .context("re-enter alternate screen after setup")?;
        self.terminal
            .clear()
            .context("clear dashboard after setup")?;
        Ok(())
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(self.terminal.backend_mut(), LeaveAlternateScreen);
        let _ = self.terminal.show_cursor();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_session_ids_are_safe() {
        assert_eq!(short_id("0123456789"), "01234567");
        assert_eq!(short_id("tiny"), "tiny");
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
