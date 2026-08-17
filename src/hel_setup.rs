//! Plain-stdio first-run configuration for Hel.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};

use crate::hel_config::{
    AwsAddressSource, ContainerTemplate, HarnessKind, HarnessProfile, HelConfig, ProjectBundle,
    ProjectRepository, TargetTemplate,
};
use crate::hel_doctor::{
    CheckStatus, DoctorOptions, all_ready, apple_container_daemon_check, current_apple_platform,
    local_podman_runtime_check, render_human, run_with_config_path,
};
use crate::hel_targets::{
    CancellableProcessExecutor, CommandExecutor, CommandSpec,
    ContainerTemplate as RuntimeContainerTemplate, ProcessExecutor,
    TargetTemplate as RuntimeTargetTemplate, setup_smoke_plan,
};

/// AWS credential detection must never stall an interactive first run, so the
/// probe commands share a bounded deadline.
const AWS_PROBE_TIMEOUT: Duration = Duration::from_secs(8);

/// The user every Hel launch template image boots with; see
/// scripts/update-runson-launch-template.sh.
const DEFAULT_AWS_SSH_USER: &str = "ubuntu";
const AWS_TARGET_ID: &str = "aws";

// Published from containers/Containerfile.agent-dev by
// .github/workflows/publish-agent-dev-image.yml. It already carries Node, Rust,
// Git, gh, and the pinned ACP bridges, so a first session does not have to
// install them.
const DEFAULT_IMAGE: &str = "ghcr.io/brokkai/hel/agent-dev:latest";

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
    /// The fix `hel doctor` would print for this runtime, carried through so
    /// setup never invents its own remediation wording.
    pub remediation: Option<String>,
}

/// An AWS identity that `aws sts get-caller-identity` confirmed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwsAccount {
    pub account: String,
    pub arn: String,
    /// The CLI's configured default region, when it has one.
    pub region: Option<String>,
}

/// The answers that become a `[targets.aws]` entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwsTargetInput {
    pub launch_template: String,
    pub region: String,
    pub ssh_user: String,
    pub identity_file: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetupDiscovery {
    pub homes: Vec<DiscoveredHome>,
    pub repository: Option<GithubRepository>,
    pub runtimes: Vec<RuntimeProbe>,
    /// `None` when this host has no working AWS CLI credentials, in which case
    /// setup never offers an AWS target.
    pub aws: Option<AwsAccount>,
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
        aws: detect_aws(&CancellableProcessExecutor::with_timeout(AWS_PROBE_TIMEOUT)),
    }
}

pub fn discover_harness_homes(
    home: Option<&Path>,
    overrides: impl IntoIterator<Item = (HarnessKind, PathBuf)>,
) -> Vec<DiscoveredHome> {
    let mut candidates = Vec::new();
    if let Some(home) = home {
        candidates.extend(
            HarnessKind::ALL
                .into_iter()
                .map(|kind| (kind, home.join(kind.default_home_leaf()))),
        );
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
        HarnessKind::Grok => "auth.json",
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

/// Probe the container runtimes setup can configure, reusing the doctor checks
/// so an unavailable runtime carries doctor's detail and remediation.
pub fn probe_local_runtimes(executor: &impl CommandExecutor, is_macos: bool) -> Vec<RuntimeProbe> {
    let mut probes = vec![runtime_probe_from_check(
        RuntimeKind::Podman,
        local_podman_runtime_check(executor),
    )];
    if is_macos {
        probes.push(runtime_probe_from_check(
            RuntimeKind::AppleContainer,
            apple_container_daemon_check(executor),
        ));
    }
    probes
}

fn runtime_probe_from_check(
    kind: RuntimeKind,
    check: crate::hel_doctor::DoctorCheck,
) -> RuntimeProbe {
    RuntimeProbe {
        kind,
        usable: check.status == CheckStatus::Ready,
        detail: check.detail,
        remediation: check.remediation,
    }
}

/// Detect a usable AWS CLI identity on this host.
///
/// Returns `None` whenever the CLI is missing or its credentials do not work,
/// so setup can skip the AWS step instead of prompting for a target that could
/// never launch.
pub fn detect_aws(executor: &impl CommandExecutor) -> Option<AwsAccount> {
    let identity = CommandSpec::new("aws", ["sts", "get-caller-identity", "--output", "json"])
        .purpose("detect AWS credentials");
    let output = executor.execute(&identity).ok()?;
    if output.status != 0 {
        return None;
    }
    let identity: serde_json::Value = serde_json::from_slice(&output.stdout).ok()?;
    let account = identity.get("Account")?.as_str()?.to_owned();
    let arn = identity.get("Arn")?.as_str()?.to_owned();
    Some(AwsAccount {
        account,
        arn,
        region: configured_aws_region(executor),
    })
}

fn configured_aws_region(executor: &impl CommandExecutor) -> Option<String> {
    let command = CommandSpec::new("aws", ["configure", "get", "region"])
        .purpose("read the default AWS region");
    let output = executor.execute(&command).ok()?;
    if output.status != 0 {
        return None;
    }
    let region = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    (!region.is_empty()).then_some(region)
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
    build_config_with_runtime(homes, repository, Some((runtime, image)), None)
}

fn build_config_with_runtime(
    homes: &[DiscoveredHome],
    repository: Option<&GithubRepository>,
    runtime: Option<(RuntimeKind, &str)>,
    aws: Option<&AwsTargetInput>,
) -> HelConfig {
    let mut config = HelConfig::default();
    for home in homes {
        let id = unique_profile_id(&config.profiles, home.kind.id());
        config.profiles.insert(
            id,
            HarnessProfile {
                kind: home.kind,
                home: home.path.clone(),
                executable: None,
                environment: BTreeMap::new(),
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
                    github: Some(repository.source()),
                    local: None,
                    destination: PathBuf::from(repository_id),
                    git_ref: None,
                }],
            },
        );
    }

    #[cfg(unix)]
    config
        .targets
        .insert("raw-localhost".to_owned(), TargetTemplate::LocalBare);
    if let Some((runtime, image)) = runtime {
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
    }
    if let Some(aws) = aws {
        config.targets.insert(
            AWS_TARGET_ID.to_owned(),
            TargetTemplate::AwsEc2 {
                aws_profile: None,
                region: aws.region.clone(),
                launch_template: aws.launch_template.clone(),
                launch_template_version: None,
                ssh_user: aws.ssh_user.clone(),
                address_source: AwsAddressSource::default(),
                identity_file: aws.identity_file.clone(),
                ssh_args: vec![],
            },
        );
    }
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

    let runtime = if let Some(recommended) = recommended_runtime(&discovery.runtimes) {
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
        Some((runtime, image))
    } else {
        writeln!(
            output,
            "No usable container runtime found; raw localhost will still be configured."
        )?;
        None
    };
    let aws = prompt_aws_target(input, output, discovery.aws.as_ref())?;
    let config = build_config_with_runtime(
        &discovery.homes,
        discovery.repository.as_ref(),
        runtime
            .as_ref()
            .map(|(runtime, image)| (*runtime, image.as_str())),
        aws.as_ref(),
    );
    config.validate()?;

    writeln!(output)?;
    write_summary(
        output,
        config_path,
        &config,
        runtime.as_ref().map(|(kind, _)| *kind),
    )?;
    let confirmation = prompt(input, output, "Write this configuration? [y/N]: ")?;
    if !matches!(confirmation.to_ascii_lowercase().as_str(), "y" | "yes") {
        writeln!(output, "Setup cancelled.")?;
        return Ok(SetupOutcome::Cancelled);
    }

    writeln!(output, "Writing {}...", config_path.display())?;
    config.save_to(config_path)?;
    if let Some((runtime, image)) = runtime {
        let smoke_target = smoke_target(runtime, &image);
        run_smoke_test(output, &smoke_target, executor)?;
    }
    write_doctor_report(output, config_path, executor)?;
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
            "  No existing Codex, Claude Code, Kimi Code, or Grok Build homes found."
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
            "  {}: {} ({authentication}){}",
            home.kind.display_name(),
            home.path.display(),
            match home.kind.bare_target_auto_approval() {
                Some(mechanism) => format!(
                    " — DANGER: {mechanism} approves every command, including on raw localhost"
                ),
                None => String::new(),
            }
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
        if let Some(remediation) = &runtime.remediation {
            writeln!(output, "    remediation: {remediation}")?;
        }
    }
    if let Some(runtime) = recommended_runtime(runtimes) {
        writeln!(output, "Recommended runtime: {}", runtime.label())?;
    }
    Ok(())
}

/// Offer an AWS EC2 target, but only when this host already has working AWS
/// credentials. Without them the step prints one line and asks nothing.
fn prompt_aws_target(
    input: &mut impl BufRead,
    output: &mut impl Write,
    account: Option<&AwsAccount>,
) -> Result<Option<AwsTargetInput>> {
    let Some(account) = account else {
        writeln!(
            output,
            "AWS: no working `aws` CLI credentials found; skipping the AWS target."
        )?;
        return Ok(None);
    };
    writeln!(
        output,
        "AWS: credentials are valid for account {} ({}).",
        account.account, account.arn
    )?;
    let answer = prompt(input, output, "Add an AWS EC2 target? [y/N]: ")?;
    if !matches!(answer.to_ascii_lowercase().as_str(), "y" | "yes") {
        return Ok(None);
    }

    let launch_template = prompt(input, output, "Launch template name: ")?;
    if launch_template.is_empty() {
        writeln!(
            output,
            "A launch template name is required; skipping the AWS target."
        )?;
        return Ok(None);
    }

    let region_label = match &account.region {
        Some(region) => format!("Region [{region}]: "),
        None => "Region: ".to_owned(),
    };
    let region = prompt(input, output, &region_label)?;
    let region = if region.is_empty() {
        match &account.region {
            Some(region) => region.clone(),
            None => {
                writeln!(output, "A region is required; skipping the AWS target.")?;
                return Ok(None);
            }
        }
    } else {
        region
    };

    let ssh_user = prompt(
        input,
        output,
        &format!("SSH user [{DEFAULT_AWS_SSH_USER}]: "),
    )?;
    let ssh_user = if ssh_user.is_empty() {
        DEFAULT_AWS_SSH_USER.to_owned()
    } else {
        ssh_user
    };
    let identity_file = prompt(input, output, "SSH identity file (optional): ")?;

    Ok(Some(AwsTargetInput {
        launch_template,
        region,
        ssh_user,
        identity_file: (!identity_file.is_empty()).then(|| PathBuf::from(identity_file)),
    }))
}

/// End setup with the same report `hel doctor` prints, so the user gets one
/// ready/fixable summary with remediations instead of two different signals.
fn write_doctor_report(
    output: &mut impl Write,
    config_path: &Path,
    executor: &impl CommandExecutor,
) -> Result<()> {
    writeln!(output)?;
    writeln!(output, "Running `hel doctor` checks on the new config...")?;
    let checks = run_with_config_path(
        config_path,
        executor,
        current_apple_platform(executor),
        DoctorOptions { smoke: false },
    );
    render_human(&checks, output)?;
    if all_ready(&checks) {
        writeln!(output, "Every check is ready.")?;
    } else {
        writeln!(
            output,
            "Apply the remediations above, then rerun `hel doctor`."
        )?;
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
    runtime: Option<RuntimeKind>,
) -> Result<()> {
    writeln!(output, "Hel will write {} with:", config_path.display())?;
    writeln!(output, "  {} profile(s)", config.profiles.len())?;
    writeln!(output, "  {} bundle(s)", config.bundles.len())?;
    writeln!(
        output,
        "  raw localhost target using configured harness homes directly"
    )?;
    if let Some(runtime) = runtime {
        let target = config
            .targets
            .get(runtime.id())
            .expect("selected target exists");
        let image = match target {
            TargetTemplate::LocalPodman { container }
            | TargetTemplate::AppleContainer { container } => &container.image,
            _ => unreachable!("setup runtime target is a local container"),
        };
        writeln!(output, "  {} target using {image}", runtime.label())?;
    }
    if let Some(TargetTemplate::AwsEc2 {
        launch_template,
        region,
        ..
    }) = config.targets.get(AWS_TARGET_ID)
    {
        writeln!(
            output,
            "  AWS EC2 target using launch template {launch_template} in {region}"
        )?;
    }
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

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::fs;

    use super::*;
    use crate::hel_targets::CommandOutput;

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

    impl RuntimeProbeExecutor {
        fn new(outputs: impl IntoIterator<Item = CommandOutput>) -> Self {
            Self {
                commands: RefCell::new(vec![]),
                outputs: RefCell::new(outputs.into_iter().collect()),
            }
        }
    }

    impl CommandExecutor for RuntimeProbeExecutor {
        fn execute(&self, command: &CommandSpec) -> Result<CommandOutput> {
            self.commands.borrow_mut().push(command.clone());
            if self.outputs.borrow().is_empty() {
                bail!("no canned output for {}", command.program);
            }
            Ok(self.outputs.borrow_mut().remove(0))
        }
    }

    fn ok(stdout: &[u8]) -> CommandOutput {
        CommandOutput {
            status: 0,
            stdout: stdout.to_vec(),
            stderr: vec![],
        }
    }

    fn failed(stderr: &[u8]) -> CommandOutput {
        CommandOutput {
            status: 1,
            stdout: vec![],
            stderr: stderr.to_vec(),
        }
    }

    const CALLER_IDENTITY: &[u8] =
        br#"{"UserId":"AIDA","Account":"123456789012","Arn":"arn:aws:iam::123456789012:user/dev"}"#;

    fn discovery_without_runtimes() -> SetupDiscovery {
        SetupDiscovery {
            homes: vec![],
            repository: None,
            runtimes: vec![],
            aws: None,
        }
    }

    #[test]
    fn discovers_default_and_overridden_homes_with_authentication_markers() {
        let directory = tempfile::tempdir().unwrap();
        let home = directory.path().join("home");
        let codex = home.join(".codex");
        let kimi = home.join(".kimi-code");
        let grok = home.join(".grok");
        let claude = directory.path().join("claude-override");
        fs::create_dir_all(&codex).unwrap();
        fs::create_dir_all(kimi.join("credentials")).unwrap();
        fs::create_dir_all(&grok).unwrap();
        fs::create_dir_all(&claude).unwrap();
        fs::write(codex.join("auth.json"), "{}").unwrap();
        fs::write(kimi.join("credentials/kimi-code.json"), "{}").unwrap();
        fs::write(grok.join("auth.json"), "{}").unwrap();
        fs::write(claude.join(".credentials.json"), "{}").unwrap();

        let homes = discover_harness_homes(Some(&home), [(HarnessKind::Claude, claude.clone())]);

        assert_eq!(homes.len(), 4);
        assert!(homes.iter().all(|home| home.authenticated));
        assert!(homes.iter().any(|home| home.path == codex));
        assert!(homes.iter().any(|home| home.path == claude));
        assert!(homes.iter().any(|home| home.path == kimi));
        assert!(
            homes
                .iter()
                .any(|home| home.path == grok && home.kind == HarnessKind::Grok)
        );
    }

    #[test]
    fn every_harness_has_a_discoverable_default_home() {
        let directory = tempfile::tempdir().unwrap();
        let home = directory.path().to_path_buf();
        for kind in HarnessKind::ALL {
            fs::create_dir_all(home.join(kind.default_home_leaf())).unwrap();
        }

        let homes = discover_harness_homes(Some(&home), []);

        assert_eq!(homes.len(), HarnessKind::ALL.len());
        for kind in HarnessKind::ALL {
            assert!(
                homes
                    .iter()
                    .any(|home| home.kind == kind && !home.authenticated),
                "{kind:?} default home"
            );
        }
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
            config.bundles["current-repository"].repositories[0]
                .github
                .as_deref(),
            Some("BrokkAi/hel")
        );
        assert!(matches!(
            config.targets["podman"],
            TargetTemplate::LocalPodman { .. }
        ));
        assert!(matches!(
            config.targets["raw-localhost"],
            TargetTemplate::LocalBare
        ));
    }

    #[test]
    fn runtime_probe_requires_podman_rootless_preflight_and_checks_apple_on_macos() {
        let executor = RuntimeProbeExecutor::new([
            ok(b"podman version 5.4.2\n"),
            ok(b"true\n"),
            ok(b"0 1000 1\n1 100000 65536\n"),
            ok(b"container version 1\n"),
            ok(b"running\n"),
        ]);
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
        assert!(runtimes.iter().all(|runtime| runtime.usable));
    }

    #[test]
    fn unusable_podman_carries_the_doctor_remediation_into_the_runtime_list() {
        let executor = RuntimeProbeExecutor::new([ok(b"podman version 3.4.7\n")]);

        let runtimes = probe_local_runtimes(&executor, false);

        assert_eq!(runtimes.len(), 1);
        assert!(!runtimes[0].usable);
        let remediation = runtimes[0].remediation.as_deref().unwrap();
        assert!(remediation.contains("Upgrade Podman"), "{remediation}");

        let mut output = Vec::new();
        write_runtimes(&mut output, &runtimes).unwrap();
        let output = String::from_utf8(output).unwrap();
        assert!(output.contains("Podman: unavailable"), "{output}");
        assert!(output.contains("remediation: Upgrade Podman"), "{output}");
    }

    #[test]
    fn aws_is_detected_only_when_the_caller_identity_call_succeeds() {
        let missing = RuntimeProbeExecutor::new([]);
        assert_eq!(detect_aws(&missing), None);

        let denied = RuntimeProbeExecutor::new([failed(b"ExpiredToken")]);
        assert_eq!(detect_aws(&denied), None);

        let working = RuntimeProbeExecutor::new([ok(CALLER_IDENTITY), ok(b"us-east-1\n")]);
        assert_eq!(
            detect_aws(&working),
            Some(AwsAccount {
                account: "123456789012".into(),
                arn: "arn:aws:iam::123456789012:user/dev".into(),
                region: Some("us-east-1".into()),
            })
        );
        assert_eq!(working.commands.borrow()[0].args[0], "sts");
        assert_eq!(
            working.commands.borrow()[1].args,
            ["configure", "get", "region"]
        );
    }

    #[test]
    fn aws_detection_without_a_configured_region_leaves_the_region_unset() {
        let executor = RuntimeProbeExecutor::new([ok(CALLER_IDENTITY), failed(b"")]);

        assert_eq!(detect_aws(&executor).unwrap().region, None);
    }

    #[test]
    fn the_aws_step_asks_nothing_when_no_aws_credentials_were_detected() {
        let mut input = b"".as_slice();
        let mut output = Vec::new();

        let aws = prompt_aws_target(&mut input, &mut output, None).unwrap();

        assert_eq!(aws, None);
        let output = String::from_utf8(output).unwrap();
        assert!(output.contains("skipping the AWS target"), "{output}");
        assert!(!output.contains("[y/N]"), "{output}");
    }

    #[test]
    fn the_aws_step_defaults_region_and_ssh_user_when_the_answers_are_blank() {
        let account = AwsAccount {
            account: "123456789012".into(),
            arn: "arn:aws:iam::123456789012:user/dev".into(),
            region: Some("us-east-1".into()),
        };
        let mut input = b"y\nhel-runson\n\n\n\n".as_slice();
        let mut output = Vec::new();

        let aws = prompt_aws_target(&mut input, &mut output, Some(&account))
            .unwrap()
            .unwrap();

        assert_eq!(
            aws,
            AwsTargetInput {
                launch_template: "hel-runson".into(),
                region: "us-east-1".into(),
                ssh_user: DEFAULT_AWS_SSH_USER.into(),
                identity_file: None,
            }
        );
        let config = build_config_with_runtime(&[], None, None, Some(&aws));
        let TargetTemplate::AwsEc2 {
            region,
            launch_template,
            ssh_user,
            ..
        } = &config.targets[AWS_TARGET_ID]
        else {
            panic!("setup must write an aws-ec2 target");
        };
        assert_eq!(region, "us-east-1");
        assert_eq!(launch_template, "hel-runson");
        assert_eq!(ssh_user, DEFAULT_AWS_SSH_USER);
        config.validate().unwrap();
    }

    #[test]
    fn declining_the_aws_step_writes_no_aws_target() {
        let account = AwsAccount {
            account: "123456789012".into(),
            arn: "arn:aws:iam::123456789012:user/dev".into(),
            region: None,
        };
        let mut input = b"\n".as_slice();
        let mut output = Vec::new();

        assert_eq!(
            prompt_aws_target(&mut input, &mut output, Some(&account)).unwrap(),
            None
        );
        let config = build_config_with_runtime(&[], None, None, None);
        assert!(!config.targets.contains_key(AWS_TARGET_ID));
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
                remediation: None,
            }],
            aws: None,
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
        let smoke = executor.commands.borrow()[..3]
            .iter()
            .map(|command| command.args[0].clone())
            .collect::<Vec<_>>();
        assert_eq!(smoke, ["run", "exec", "rm"]);
        assert!(
            String::from_utf8(output)
                .unwrap()
                .ends_with("Press n to start your first session.\n")
        );
    }

    #[test]
    fn setup_finishes_with_the_standard_doctor_report_for_the_config_it_wrote() {
        let directory = tempfile::tempdir().unwrap();
        let config_path = directory.path().join("config.toml");
        let discovery = SetupDiscovery {
            homes: vec![DiscoveredHome {
                kind: HarnessKind::Codex,
                path: directory.path().join("missing-codex-home"),
                authenticated: false,
            }],
            ..discovery_without_runtimes()
        };
        let executor = FakeExecutor::succeeds();
        let mut input = b"y\n".as_slice();
        let mut output = Vec::new();

        run_setup_dialog_with(&mut input, &mut output, &config_path, &discovery, &executor)
            .unwrap();

        let output = String::from_utf8(output).unwrap();
        // The report is doctor's own rendering: a status-prefixed line per
        // check, plus the remediation doctor would print for the missing home.
        assert!(
            output.contains(&format!(
                "ready Hel configuration: {} is valid",
                config_path.display()
            )),
            "{output}"
        );
        assert!(output.contains("fixable Harness profile codex"), "{output}");
        assert!(
            output.contains("  remediation: Create or select the Codex home"),
            "{output}"
        );
        assert!(
            output.contains("Apply the remediations above, then rerun `hel doctor`."),
            "{output}"
        );
    }

    #[test]
    fn dialog_configures_raw_localhost_without_a_container_runtime() {
        let directory = tempfile::tempdir().unwrap();
        let config_path = directory.path().join("config.toml");
        let discovery = SetupDiscovery {
            homes: vec![DiscoveredHome {
                kind: HarnessKind::Kimi,
                path: PathBuf::from("/profiles/kimi"),
                authenticated: true,
            }],
            repository: None,
            runtimes: vec![RuntimeProbe {
                kind: RuntimeKind::Podman,
                usable: false,
                detail: "not installed".into(),
                remediation: Some("Install Podman.".into()),
            }],
            aws: None,
        };
        let executor = FakeExecutor::succeeds();
        let mut input = b"y\n".as_slice();
        let mut output = Vec::new();

        assert_eq!(
            run_setup_dialog_with(&mut input, &mut output, &config_path, &discovery, &executor)
                .unwrap(),
            SetupOutcome::Written
        );
        let config = HelConfig::load_from(&config_path).unwrap();
        assert!(matches!(
            config.targets["raw-localhost"],
            TargetTemplate::LocalBare
        ));
        // No smoke test runs without a runtime; the trailing commands belong to
        // the doctor report.
        assert!(
            executor
                .commands
                .borrow()
                .iter()
                .all(|command| command.program != "podman" || command.args[0] != "run")
        );
        let output = String::from_utf8(output).unwrap();
        assert!(output.contains("DANGER"));
        assert!(output.contains("its default auto mode approves every command"));
        assert!(output.contains("raw localhost will still be configured"));
    }

    #[test]
    fn discovered_homes_warn_for_every_harness_that_approves_everything() {
        let warning = |kind: HarnessKind| {
            let mut output = Vec::new();
            write_discovered_homes(
                &mut output,
                &[DiscoveredHome {
                    kind,
                    path: PathBuf::from("/profiles/harness"),
                    authenticated: true,
                }],
            )
            .unwrap();
            String::from_utf8(output).unwrap()
        };

        // Both harnesses approve everything; the warning names how each does.
        let grok = warning(HarnessKind::Grok);
        assert!(grok.contains("Grok Build"), "{grok}");
        assert!(
            grok.contains("DANGER: Hel's --always-approve launch flag approves every command"),
            "{grok}"
        );
        let kimi = warning(HarnessKind::Kimi);
        assert!(
            kimi.contains("DANGER: its default auto mode approves every command"),
            "{kimi}"
        );

        for kind in [HarnessKind::Codex, HarnessKind::Claude] {
            assert!(!warning(kind).contains("DANGER"), "{kind:?}");
        }
    }
}
