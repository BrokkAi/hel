//! Plain-stdio first-run configuration for Hel.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};

use crate::hel_config::{
    ContainerTemplate, HarnessKind, HarnessProfile, HelConfig, ProjectBundle, ProjectRepository,
    TargetTemplate,
};
use crate::hel_targets::{
    CommandExecutor, CommandOutput, CommandSpec, ContainerTemplate as RuntimeContainerTemplate,
    ProcessExecutor, TargetTemplate as RuntimeTargetTemplate, setup_smoke_plan,
    verify_local_podman,
};

const DEFAULT_IMAGE: &str = "ubuntu:24.04";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredHome {
    pub kind: HarnessKind,
    pub path: PathBuf,
    pub authenticated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GithubRepository {
    pub owner: String,
    pub repository: String,
}

impl GithubRepository {
    fn source(&self) -> String {
        format!("{}/{}", self.owner, self.repository)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeKind {
    Podman,
    AppleContainer,
}

impl RuntimeKind {
    fn id(self) -> &'static str {
        match self {
            Self::Podman => "podman",
            Self::AppleContainer => "apple-container",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Podman => "Podman",
            Self::AppleContainer => "Apple container",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "podman" => Some(Self::Podman),
            "apple-container" | "container" => Some(Self::AppleContainer),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeProbe {
    pub kind: RuntimeKind,
    pub usable: bool,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetupDiscovery {
    pub homes: Vec<DiscoveredHome>,
    pub repository: Option<GithubRepository>,
    pub runtimes: Vec<RuntimeProbe>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetupOutcome {
    Written,
    Cancelled,
}

/// Run the setup dialog using the user's normal standard input and output.
pub fn run_setup_dialog(config_path: &Path) -> Result<SetupOutcome> {
    let discovery = discover_current(&ProcessExecutor);
    let stdin = io::stdin();
    let stdout = io::stdout();
    run_setup_dialog_with(
        &mut stdin.lock(),
        &mut stdout.lock(),
        config_path,
        &discovery,
        &ProcessExecutor,
    )
}

pub fn discover_current(executor: &impl CommandExecutor) -> SetupDiscovery {
    let home = dirs::home_dir();
    let overrides = HarnessKind::ALL.into_iter().filter_map(|kind| {
        std::env::var_os(kind.home_env()).map(|path| (kind, PathBuf::from(path)))
    });
    let homes = discover_harness_homes(home.as_deref(), overrides);
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));

    SetupDiscovery {
        homes,
        repository: discover_github_repository(&cwd),
        runtimes: probe_local_runtimes(executor, cfg!(target_os = "macos")),
    }
}

pub fn discover_harness_homes(
    home: Option<&Path>,
    overrides: impl IntoIterator<Item = (HarnessKind, PathBuf)>,
) -> Vec<DiscoveredHome> {
    let mut candidates = Vec::new();
    if let Some(home) = home {
        candidates.extend([
            (HarnessKind::Codex, home.join(".codex")),
            (HarnessKind::Claude, home.join(".claude")),
            (HarnessKind::Kimi, home.join(".kimi-code")),
        ]);
    }
    candidates.extend(overrides);

    let mut seen = BTreeSet::new();
    candidates
        .into_iter()
        .filter(|(kind, path)| seen.insert((*kind, path.clone())) && path.is_dir())
        .map(|(kind, path)| DiscoveredHome {
            authenticated: harness_is_authenticated(kind, &path),
            kind,
            path,
        })
        .collect()
}

pub fn harness_is_authenticated(kind: HarnessKind, home: &Path) -> bool {
    harness_authentication_marker(kind, home).is_file()
        || (kind == HarnessKind::Kimi && home.join("credentials").is_file())
}

pub fn harness_authentication_marker(kind: HarnessKind, home: &Path) -> PathBuf {
    home.join(match kind {
        HarnessKind::Codex => "auth.json",
        HarnessKind::Claude => ".credentials.json",
        HarnessKind::Kimi => "credentials/kimi-code.json",
    })
}

pub fn github_repository_from_origin(origin: &str) -> Option<GithubRepository> {
    let origin = origin.trim();
    let path = origin
        .strip_prefix("https://github.com/")
        .or_else(|| origin.strip_prefix("http://github.com/"))
        .or_else(|| origin.strip_prefix("git@github.com:"))
        .or_else(|| origin.strip_prefix("ssh://git@github.com/"))
        // Config accepts owner/repository shorthand, and import uses the same
        // parser to compare that configured source with `git remote` output.
        .unwrap_or(origin);
    let path = path.trim_end_matches(".git");
    let mut parts = path.split('/');
    let owner = parts.next()?;
    let repository = parts.next()?;
    if owner.is_empty()
        || repository.is_empty()
        || parts.next().is_some()
        || owner.chars().any(char::is_whitespace)
        || repository.chars().any(char::is_whitespace)
    {
        return None;
    }
    Some(GithubRepository {
        owner: owner.to_owned(),
        repository: repository.to_owned(),
    })
}

fn discover_github_repository(cwd: &Path) -> Option<GithubRepository> {
    let output = Command::new("git")
        .args(["remote", "get-url", "origin"])
        .current_dir(cwd)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    github_repository_from_origin(&String::from_utf8_lossy(&output.stdout))
}

pub fn probe_local_runtimes(executor: &impl CommandExecutor, is_macos: bool) -> Vec<RuntimeProbe> {
    let mut probes = vec![probe_podman_runtime(executor)];
    if is_macos {
        probes.push(probe_runtime(
            executor,
            RuntimeKind::AppleContainer,
            CommandSpec::new("container", ["system", "status"])
                .purpose("check Apple container runtime"),
        ));
    }
    probes
}

fn probe_podman_runtime(executor: &impl CommandExecutor) -> RuntimeProbe {
    match verify_local_podman(executor) {
        Ok(preflight) => RuntimeProbe {
            kind: RuntimeKind::Podman,
            usable: true,
            detail: format!("Podman {} with a valid rootless UID map", preflight.version),
        },
        Err(error) => RuntimeProbe {
            kind: RuntimeKind::Podman,
            usable: false,
            detail: format!("{error:#}"),
        },
    }
}

fn probe_runtime(
    executor: &impl CommandExecutor,
    kind: RuntimeKind,
    command: CommandSpec,
) -> RuntimeProbe {
    match executor.execute(&command) {
        Ok(output) if output.status == 0 => RuntimeProbe {
            kind,
            usable: true,
            detail: String::from_utf8_lossy(&output.stdout).trim().to_owned(),
        },
        Ok(output) => RuntimeProbe {
            kind,
            usable: false,
            detail: command_failure_detail(&output),
        },
        Err(error) => RuntimeProbe {
            kind,
            usable: false,
            detail: error.to_string(),
        },
    }
}

fn command_failure_detail(output: &CommandOutput) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    if stderr.is_empty() {
        format!("command exited with status {}", output.status)
    } else {
        stderr
    }
}

pub fn recommended_runtime(runtimes: &[RuntimeProbe]) -> Option<RuntimeKind> {
    runtimes
        .iter()
        .find(|runtime| runtime.usable)
        .map(|runtime| runtime.kind)
}

pub fn build_config(
    homes: &[DiscoveredHome],
    repository: Option<&GithubRepository>,
    runtime: RuntimeKind,
    image: &str,
) -> HelConfig {
    let mut config = HelConfig::default();
    for home in homes {
        let base_id = match home.kind {
            HarnessKind::Codex => "codex",
            HarnessKind::Claude => "claude",
            HarnessKind::Kimi => "kimi",
        };
        let id = unique_profile_id(&config.profiles, base_id);
        config.profiles.insert(
            id,
            HarnessProfile {
                kind: home.kind,
                home: home.path.clone(),
                executable: None,
                environment: BTreeMap::new(),
                model: None,
                reasoning_effort: None,
                context_window_bytes: None,
            },
        );
    }

    if let Some(repository) = repository {
        let repository_id = config_id(&repository.repository);
        config.bundles.insert(
            "current-repository".to_owned(),
            ProjectBundle {
                primary_repo: repository_id.clone(),
                repositories: vec![ProjectRepository {
                    id: repository_id.clone(),
                    github: repository.source(),
                    destination: PathBuf::from(repository_id),
                    git_ref: None,
                }],
            },
        );
    }

    let container = ContainerTemplate {
        image: image.trim().to_owned(),
        platform: None,
        cpus: None,
        memory: None,
        environment: BTreeMap::new(),
    };
    let (target_id, target) = match runtime {
        RuntimeKind::Podman => ("podman", TargetTemplate::LocalPodman { container }),
        RuntimeKind::AppleContainer => (
            "apple-container",
            TargetTemplate::AppleContainer { container },
        ),
    };
    config.targets.insert(target_id.to_owned(), target);
    config
}

fn unique_profile_id(profiles: &BTreeMap<String, HarnessProfile>, base_id: &str) -> String {
    if !profiles.contains_key(base_id) {
        return base_id.to_owned();
    }
    let mut number = 2;
    loop {
        let candidate = format!("{base_id}-{number}");
        if !profiles.contains_key(&candidate) {
            return candidate;
        }
        number += 1;
    }
}

fn config_id(value: &str) -> String {
    let mut id = value
        .chars()
        .filter(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.')
        })
        .take(64)
        .collect::<String>();
    if id.is_empty() || matches!(id.as_str(), "." | "..") {
        id = "repository".to_owned();
    }
    id
}

pub fn run_setup_dialog_with(
    input: &mut impl BufRead,
    output: &mut impl Write,
    config_path: &Path,
    discovery: &SetupDiscovery,
    executor: &impl CommandExecutor,
) -> Result<SetupOutcome> {
    writeln!(output, "Welcome to Hel setup.")?;
    writeln!(output)?;
    write_discovered_homes(output, &discovery.homes)?;
    write_repository(output, discovery.repository.as_ref())?;
    write_runtimes(output, &discovery.runtimes)?;

    let Some(recommended) = recommended_runtime(&discovery.runtimes) else {
        let failures = discovery
            .runtimes
            .iter()
            .map(|runtime| format!("{}: {}", runtime.kind.label(), runtime.detail))
            .collect::<Vec<_>>()
            .join("\n");
        bail!("no usable local container runtime found:\n{failures}");
    };
    let runtime = select_runtime(input, output, &discovery.runtimes, recommended)?;
    let image = prompt(
        input,
        output,
        &format!("Container image [{DEFAULT_IMAGE}]: "),
    )?;
    let image = if image.is_empty() {
        DEFAULT_IMAGE.to_owned()
    } else {
        image
    };
    let config = build_config(
        &discovery.homes,
        discovery.repository.as_ref(),
        runtime,
        &image,
    );
    config.validate()?;

    writeln!(output)?;
    write_summary(output, config_path, &config, runtime)?;
    let confirmation = prompt(input, output, "Write this configuration? [y/N]: ")?;
    if !matches!(confirmation.to_ascii_lowercase().as_str(), "y" | "yes") {
        writeln!(output, "Setup cancelled.")?;
        return Ok(SetupOutcome::Cancelled);
    }

    writeln!(output, "Writing {}...", config_path.display())?;
    config.save_to(config_path)?;
    let smoke_target = smoke_target(runtime, &image);
    run_smoke_test(output, &smoke_target, executor)?;
    writeln!(
        output,
        "Advanced users can edit TOML for extra profiles, virtual monorepos, SSH, and AWS."
    )?;
    writeln!(output, "Press n to start your first session.")?;
    Ok(SetupOutcome::Written)
}

fn write_discovered_homes(output: &mut impl Write, homes: &[DiscoveredHome]) -> Result<()> {
    writeln!(output, "Harness homes:")?;
    if homes.is_empty() {
        writeln!(
            output,
            "  No existing Codex, Claude Code, or Kimi Code homes found."
        )?;
    }
    for home in homes {
        let authentication = if home.authenticated {
            "authenticated"
        } else {
            "not authenticated"
        };
        writeln!(
            output,
            "  {}: {} ({authentication})",
            harness_label(home.kind),
            home.path.display()
        )?;
    }
    Ok(())
}

fn write_repository(output: &mut impl Write, repository: Option<&GithubRepository>) -> Result<()> {
    match repository {
        Some(repository) => writeln!(
            output,
            "GitHub origin: {} (a one-repository bundle will be created)",
            repository.source()
        )?,
        None => writeln!(
            output,
            "GitHub origin: none detected in the current directory."
        )?,
    }
    Ok(())
}

fn write_runtimes(output: &mut impl Write, runtimes: &[RuntimeProbe]) -> Result<()> {
    writeln!(output, "Local runtimes:")?;
    for runtime in runtimes {
        let state = if runtime.usable {
            "usable"
        } else {
            "unavailable"
        };
        if runtime.detail.is_empty() {
            writeln!(output, "  {}: {state}", runtime.kind.label())?;
        } else {
            writeln!(
                output,
                "  {}: {state} ({})",
                runtime.kind.label(),
                runtime.detail
            )?;
        }
    }
    if let Some(runtime) = recommended_runtime(runtimes) {
        writeln!(output, "Recommended runtime: {}", runtime.label())?;
    }
    Ok(())
}

fn select_runtime(
    input: &mut impl BufRead,
    output: &mut impl Write,
    runtimes: &[RuntimeProbe],
    recommended: RuntimeKind,
) -> Result<RuntimeKind> {
    let choices = runtimes
        .iter()
        .filter(|runtime| runtime.usable)
        .map(|runtime| runtime.kind.id())
        .collect::<Vec<_>>()
        .join(", ");
    let selected = prompt(
        input,
        output,
        &format!("Runtime ({choices}) [{}]: ", recommended.id()),
    )?;
    let selected = if selected.is_empty() {
        recommended
    } else {
        RuntimeKind::parse(&selected).ok_or_else(|| {
            anyhow::anyhow!("unknown runtime {selected:?}; choose one of: {choices}")
        })?
    };
    if !runtimes
        .iter()
        .any(|runtime| runtime.kind == selected && runtime.usable)
    {
        bail!("{} is not usable on this machine", selected.label());
    }
    Ok(selected)
}

fn prompt(input: &mut impl BufRead, output: &mut impl Write, label: &str) -> Result<String> {
    write!(output, "{label}")?;
    output.flush()?;
    let mut answer = String::new();
    input
        .read_line(&mut answer)
        .context("read setup response")?;
    Ok(answer.trim().to_owned())
}

fn write_summary(
    output: &mut impl Write,
    config_path: &Path,
    config: &HelConfig,
    runtime: RuntimeKind,
) -> Result<()> {
    writeln!(output, "Hel will write {} with:", config_path.display())?;
    writeln!(output, "  {} profile(s)", config.profiles.len())?;
    writeln!(output, "  {} bundle(s)", config.bundles.len())?;
    let target = config
        .targets
        .get(runtime.id())
        .expect("selected target exists");
    let image = match target {
        TargetTemplate::LocalPodman { container }
        | TargetTemplate::AppleContainer { container } => &container.image,
        _ => unreachable!("setup only creates local container targets"),
    };
    writeln!(output, "  {} target using {image}", runtime.label())?;
    if config_path.exists() {
        writeln!(output, "  This replaces the existing configuration file.")?;
    }
    Ok(())
}

fn smoke_target(runtime: RuntimeKind, image: &str) -> RuntimeTargetTemplate {
    let container = RuntimeContainerTemplate {
        image: image.to_owned(),
        extra_run_args: vec![],
    };
    match runtime {
        RuntimeKind::Podman => RuntimeTargetTemplate::LocalPodman(container),
        RuntimeKind::AppleContainer => RuntimeTargetTemplate::AppleContainer(container),
    }
}

fn run_smoke_test(
    output: &mut impl Write,
    target: &RuntimeTargetTemplate,
    executor: &impl CommandExecutor,
) -> Result<()> {
    let smoke_id = format!(
        "setup-{}-{:x}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let plan = setup_smoke_plan(target, &smoke_id)?;
    let create = &plan.commands[0];
    let execute = &plan.commands[1];
    let cleanup = &plan.commands[2];

    writeln!(output, "Smoke test: creating a disposable container...")?;
    execute_smoke_command(executor, create)?;
    writeln!(output, "Smoke test: executing a trivial command in it...")?;
    let execution = execute_smoke_command(executor, execute);
    writeln!(output, "Smoke test: deleting the disposable container...")?;
    let cleanup_result = execute_smoke_command(executor, cleanup);
    execution?;
    cleanup_result
}

fn execute_smoke_command(executor: &impl CommandExecutor, command: &CommandSpec) -> Result<()> {
    let output = executor.execute(command)?;
    if output.status != 0 {
        bail!(
            "{} failed with status {}: {}",
            command.purpose,
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

fn harness_label(kind: HarnessKind) -> &'static str {
    match kind {
        HarnessKind::Codex => "Codex",
        HarnessKind::Claude => "Claude Code",
        HarnessKind::Kimi => "Kimi Code",
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::fs;

    use super::*;

    struct FakeExecutor {
        commands: RefCell<Vec<CommandSpec>>,
        statuses: Vec<i32>,
    }

    impl FakeExecutor {
        fn succeeds() -> Self {
            Self {
                commands: RefCell::new(vec![]),
                statuses: vec![0, 0, 0],
            }
        }
    }

    impl CommandExecutor for FakeExecutor {
        fn execute(&self, command: &CommandSpec) -> Result<CommandOutput> {
            let index = self.commands.borrow().len();
            self.commands.borrow_mut().push(command.clone());
            Ok(CommandOutput {
                status: self.statuses.get(index).copied().unwrap_or(0),
                stdout: b"available".to_vec(),
                stderr: b"failed".to_vec(),
            })
        }
    }

    struct RuntimeProbeExecutor {
        commands: RefCell<Vec<CommandSpec>>,
        outputs: RefCell<Vec<CommandOutput>>,
    }

    impl CommandExecutor for RuntimeProbeExecutor {
        fn execute(&self, command: &CommandSpec) -> Result<CommandOutput> {
            self.commands.borrow_mut().push(command.clone());
            Ok(self.outputs.borrow_mut().remove(0))
        }
    }

    #[test]
    fn discovers_default_and_overridden_homes_with_authentication_markers() {
        let directory = tempfile::tempdir().unwrap();
        let home = directory.path().join("home");
        let codex = home.join(".codex");
        let kimi = home.join(".kimi-code");
        let claude = directory.path().join("claude-override");
        fs::create_dir_all(&codex).unwrap();
        fs::create_dir_all(kimi.join("credentials")).unwrap();
        fs::create_dir_all(&claude).unwrap();
        fs::write(codex.join("auth.json"), "{}").unwrap();
        fs::write(kimi.join("credentials/kimi-code.json"), "{}").unwrap();
        fs::write(claude.join(".credentials.json"), "{}").unwrap();

        let homes = discover_harness_homes(Some(&home), [(HarnessKind::Claude, claude.clone())]);

        assert_eq!(homes.len(), 3);
        assert!(homes.iter().all(|home| home.authenticated));
        assert!(homes.iter().any(|home| home.path == codex));
        assert!(homes.iter().any(|home| home.path == claude));
        assert!(homes.iter().any(|home| home.path == kimi));
    }

    #[test]
    fn github_origin_parser_accepts_standard_https_and_ssh_forms() {
        for origin in [
            "https://github.com/BrokkAi/hel.git",
            "git@github.com:BrokkAi/hel.git",
            "ssh://git@github.com/BrokkAi/hel.git",
        ] {
            assert_eq!(
                github_repository_from_origin(origin),
                Some(GithubRepository {
                    owner: "BrokkAi".into(),
                    repository: "hel".into(),
                })
            );
        }
        assert_eq!(
            github_repository_from_origin("https://example.com/hel"),
            None
        );
    }

    #[test]
    fn config_contains_discovered_profiles_current_repository_and_selected_target() {
        let homes = vec![
            DiscoveredHome {
                kind: HarnessKind::Codex,
                path: PathBuf::from("/profiles/codex"),
                authenticated: true,
            },
            DiscoveredHome {
                kind: HarnessKind::Codex,
                path: PathBuf::from("/profiles/codex-two"),
                authenticated: false,
            },
        ];
        let repository = GithubRepository {
            owner: "BrokkAi".into(),
            repository: "hel".into(),
        };

        let config = build_config(
            &homes,
            Some(&repository),
            RuntimeKind::Podman,
            "ubuntu:24.04",
        );

        config.validate().unwrap();
        assert!(config.profiles.contains_key("codex"));
        assert!(config.profiles.contains_key("codex-2"));
        assert_eq!(
            config.bundles["current-repository"].repositories[0].github,
            "BrokkAi/hel"
        );
        assert!(matches!(
            config.targets["podman"],
            TargetTemplate::LocalPodman { .. }
        ));
    }

    #[test]
    fn runtime_probe_requires_podman_rootless_preflight_and_checks_apple_on_macos() {
        let executor = RuntimeProbeExecutor {
            commands: RefCell::new(vec![]),
            outputs: RefCell::new(vec![
                CommandOutput {
                    status: 0,
                    stdout: b"podman version 5.4.2\n".to_vec(),
                    stderr: vec![],
                },
                CommandOutput {
                    status: 0,
                    stdout: b"true\n".to_vec(),
                    stderr: vec![],
                },
                CommandOutput {
                    status: 0,
                    stdout: b"0 1000 1\n1 100000 65536\n".to_vec(),
                    stderr: vec![],
                },
                CommandOutput {
                    status: 0,
                    stdout: b"available".to_vec(),
                    stderr: vec![],
                },
            ]),
        };
        let runtimes = probe_local_runtimes(&executor, true);

        assert_eq!(runtimes.len(), 2);
        assert_eq!(recommended_runtime(&runtimes), Some(RuntimeKind::Podman));
        assert_eq!(executor.commands.borrow()[0].program, "podman");
        assert_eq!(executor.commands.borrow()[0].args, ["--version"]);
        assert_eq!(
            executor.commands.borrow()[1].args,
            ["info", "--format", "{{.Host.Security.Rootless}}"]
        );
        assert_eq!(
            executor.commands.borrow()[2].args,
            ["unshare", "cat", "/proc/self/uid_map"]
        );
        assert_eq!(executor.commands.borrow()[3].program, "container");
    }

    #[test]
    fn smoke_test_removes_the_container_after_a_failed_command() {
        let executor = FakeExecutor {
            commands: RefCell::new(vec![]),
            statuses: vec![0, 1, 0],
        };
        let mut output = Vec::new();

        assert!(
            run_smoke_test(
                &mut output,
                &smoke_target(RuntimeKind::Podman, "ubuntu:24.04"),
                &executor
            )
            .is_err()
        );
        let commands = executor.commands.borrow();
        assert_eq!(commands.len(), 3);
        assert_eq!(commands[2].args[0], "rm");
    }

    #[test]
    fn dialog_writes_config_runs_smoke_test_and_ends_with_first_session_prompt() {
        let directory = tempfile::tempdir().unwrap();
        let config_path = directory.path().join("config.toml");
        let discovery = SetupDiscovery {
            homes: vec![DiscoveredHome {
                kind: HarnessKind::Codex,
                path: PathBuf::from("/profiles/codex"),
                authenticated: true,
            }],
            repository: Some(GithubRepository {
                owner: "BrokkAi".into(),
                repository: "hel".into(),
            }),
            runtimes: vec![RuntimeProbe {
                kind: RuntimeKind::Podman,
                usable: true,
                detail: "podman version 5".into(),
            }],
        };
        let executor = FakeExecutor::succeeds();
        let mut input = b"\n\ny\n".as_slice();
        let mut output = Vec::new();

        assert_eq!(
            run_setup_dialog_with(&mut input, &mut output, &config_path, &discovery, &executor,)
                .unwrap(),
            SetupOutcome::Written
        );
        assert!(config_path.exists());
        assert_eq!(executor.commands.borrow().len(), 3);
        assert!(
            String::from_utf8(output)
                .unwrap()
                .ends_with("Press n to start your first session.\n")
        );
    }
}
