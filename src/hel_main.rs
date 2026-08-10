//! Hel: a session control plane for ACP coding agents.

use std::io::{self, IsTerminal};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use crossterm::event::{self, Event};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use hel::hel_config::config_path;
use hel::hel_controller::Controller;
use hel::hel_quota::QuotaManager;
use hel::hel_server::{ControllerAction, ServerOptions, ViewerQuota, ViewerSnapshot};
use hel::hel_setup::{SetupOutcome, run_setup_dialog};
use hel::hel_targets::ProcessExecutor;
use hel::hel_tui::{DashboardAction, DashboardState, render};
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

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        None => run_dashboard().await,
        Some(Command::Server(args)) => run_server(args).await,
        Some(Command::Worker(args)) => match args.command {
            WorkerCommand::Run { root, config } => {
                run_daemon(root, WorkerLaunchConfig::read(&config)?).await
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

async fn run_dashboard() -> Result<()> {
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        println!("Welcome to Hel.");
        println!("Run `hel doctor` for non-interactive validation.");
        return Ok(());
    }

    let mut controller = Controller::load()?;
    let mut quotas = QuotaManager::default();
    let mut dashboard = DashboardState::new(
        controller.config.clone(),
        controller.state.clone(),
        quotas.reports().clone(),
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
    let termination = hel::termination::Coordinator::install().token();

    loop {
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
                        dashboard
                            .set_notice("Setup complete. Press n to start your first session.");
                    }
                    SetupOutcome::Cancelled => dashboard.set_notice("Setup cancelled."),
                }
            }
            DashboardAction::RefreshQuotas => {
                refresh_quotas(&controller, &mut quotas, &mut dashboard).await;
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
                            Ok(()) => dashboard.set_notice(format!(
                                "Target ready for {}; connecting worker",
                                short_id(&session_id)
                            )),
                            Err(error) => {
                                dashboard.set_notice(format!("Provisioning failed: {error:#}"))
                            }
                        }
                        dashboard.set_state(controller.state.clone());
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
                    Ok(()) => dashboard.set_notice(format!(
                        "Resumed {} with {profile_id} on {target_template_id}",
                        short_id(&session_id)
                    )),
                    Err(error) => dashboard.set_notice(format!("Resume failed: {error:#}")),
                }
                dashboard.set_state(controller.state.clone());
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
            }
            DashboardAction::Close { session_id } => {
                match controller.close_session(&session_id).await {
                    Ok(()) => dashboard
                        .set_notice(format!("Archived and closed {}", short_id(&session_id))),
                    Err(error) => dashboard.set_notice(format!("Close blocked: {error:#}")),
                }
                dashboard.set_state(controller.state.clone());
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
            }
        }
    }
    quotas.shutdown().await;
    Ok(())
}

async fn refresh_quotas(
    controller: &Controller,
    quotas: &mut QuotaManager,
    dashboard: &mut DashboardState,
) {
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
    dashboard.set_quotas(quotas.reports().clone());
    dashboard.set_notice("Quota dashboard refreshed");
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
