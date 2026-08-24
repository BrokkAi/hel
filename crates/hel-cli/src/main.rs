//! Hel: a session control plane for ACP coding agents.
//!
//! This file owns the command-line surface and the one-shot subcommands. The
//! long-running surfaces live beside it: [`dashboard`] drives the terminal UI,
//! [`server`] the phone-oriented remote control, [`pollers`] the background
//! feeds both of them read, and [`import`] session adoption.

mod dashboard;
mod import;
mod pollers;
mod server;

use std::io;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use crossterm::event::{
    DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture,
    KeyboardEnhancementFlags, PopKeyboardEnhancementFlags, PushKeyboardEnhancementFlags,
};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use hel::hel_config::{HelConfig, config_path};
use hel::hel_controller::{Controller, ControllerStoreGuard};
use hel::hel_greeting::{GreetingFacts, RepositoryGreetingFacts};
use hel::hel_setup::{SetupOutcome, run_setup_dialog};
use hel::hel_state::{SessionState, TargetLocator};
use hel::hel_targets::ProcessExecutor;
use hel::hel_worker_runtime::{
    AcpSupervisorSpec, WorkerLaunchConfig, lead_process_group, proxy, run_acp_supervisor,
    run_daemon,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

use crate::dashboard::run_dashboard;
use crate::import::{ImportArgs, import};
use crate::server::{ServerArgs, run_server};

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
    /// Adopt a native coding-agent session as a stopped Hel session.
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
struct WorkerArgs {
    #[command(subcommand)]
    command: WorkerCommand,
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
        /// Export specification path, or `-` to read it from standard input.
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
    /// Serve project memory tools over MCP stdio.
    MemoryMcp {
        #[arg(long)]
        root: PathBuf,
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
    // Session teardown removes the worker root; recreating it here would
    // resurrect a closed session's state directory.
    if !root.is_dir() {
        return;
    }
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
                lead_process_group();
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
            WorkerCommand::MemoryMcp { root } => hel::hel_project_memory::run_mcp_stdio(&root),
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

pub(crate) fn startup_greeting(controller: &Controller) -> String {
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
        stopped_sessions: controller
            .state
            .sessions
            .values()
            .filter(|session| session.state == SessionState::Stopped)
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

/// The prefix every message uses when it names a session, so notices stay
/// readable without losing which session they are about.
pub(crate) fn short_id(id: &str) -> &str {
    id.get(..8).unwrap_or(id)
}

pub(crate) struct TerminalGuard {
    pub(crate) terminal: Terminal<CrosstermBackend<io::Stdout>>,
    keyboard_enhancement: bool,
}

impl TerminalGuard {
    pub(crate) fn enter() -> Result<Self> {
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

    pub(crate) fn suspend(&mut self) -> Result<()> {
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

    pub(crate) fn resume(&mut self) -> Result<()> {
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
    use hel::hel_state::{HelState, SessionRecord};

    /// The controller streams a checkpoint spec by asking the worker to read
    /// `--spec -`, so that dash has to survive argument parsing as a value.
    #[test]
    fn export_checkpoint_accepts_a_dash_for_a_streamed_spec() {
        let cli = Cli::try_parse_from(["hel", "worker", "export-checkpoint", "--spec", "-"])
            .expect("a streamed spec is a valid export argument");
        let Some(Command::Worker(WorkerArgs {
            command: WorkerCommand::ExportCheckpoint { spec },
        })) = cli.command
        else {
            panic!("export-checkpoint did not parse as a worker command");
        };
        assert_eq!(spec, PathBuf::from("-"));
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
    fn failed_archive_removal_retains_session_metadata_for_retry() {
        let directory = tempfile::tempdir().unwrap();
        let archive_path = directory.path().join("checkpoint.hel.zip");
        std::fs::create_dir(&archive_path).unwrap();
        let session_id = "1123456789abcdef0123456789abcdef";
        let mut state = HelState::default();
        state.sessions.insert(
            session_id.into(),
            SessionRecord {
                archived: false,
                container_cpus: None,
                container_memory: None,
                id: session_id.into(),
                title: "stopped".into(),
                harness_kind: hel::hel_config::HarnessKind::Codex,
                last_profile: "codex".into(),
                bundle_id: "project".into(),
                project_directory: None,
                managed_worktree: None,
                target_template_id: "podman".into(),
                resource_allocation: None,
                additional_mounts: Vec::new(),
                state: SessionState::Stopped,
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
