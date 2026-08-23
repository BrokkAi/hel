//! Plain-stdio first-run configuration for Hel.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};

use crate::hel_config::{
    AwsAddressSource, ContainerTemplate, HarnessKind, HarnessProfile, HelConfig, ProjectBundle,
    ProjectRepository, SshConnection, TargetTemplate, validate_id,
};
use crate::hel_doctor::{
    CheckStatus, DoctorCheck, DoctorOptions, all_ready, apple_container_daemon_check,
    current_apple_platform, local_podman_runtime_check, probe_executor, render_human,
    run_with_config_path,
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

/// Which kind of SSH target the user chose in the SSH step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SshTargetKind {
    Bare,
    Podman { image: String },
}

/// The answers that become a `[targets.<name>]` SSH entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SshTargetInput {
    pub name: String,
    pub host: String,
    pub kind: SshTargetKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetupDiscovery {
    pub homes: Vec<DiscoveredHome>,
    pub repository: Option<GithubRepository>,
    pub runtimes: Vec<RuntimeProbe>,
    /// `None` when this host has no working AWS CLI credentials, in which case
    /// setup never offers an AWS target.
    pub aws: Option<AwsAccount>,
    /// Concrete `Host` aliases read from `~/.ssh/config`; empty when the file
    /// is absent or only defines wildcard blocks.
    pub ssh_hosts: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetupOutcome {
    Written,
    Cancelled,
}

/// Run the setup dialog using the user's normal standard input and output.
pub fn run_setup_dialog(config_path: &Path) -> Result<SetupOutcome> {
    // Prerequisite probes run under doctor's per-probe deadline, so a wedged
    // container socket cannot stall the first run; only the smoke test, which
    // may pull an image, is allowed to take as long as it needs.
    let probes = probe_executor();
    let discovery = discover_current(&probes);
    let stdin = io::stdin();
    let stdout = io::stdout();
    run_setup_dialog_with(
        &mut stdin.lock(),
        &mut stdout.lock(),
        config_path,
        &discovery,
        &ProcessExecutor,
        &probes,
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
        repository: discover_github_repository(executor, &cwd),
        runtimes: probe_local_runtimes(executor, cfg!(target_os = "macos")),
        aws: detect_aws(&CancellableProcessExecutor::with_timeout(AWS_PROBE_TIMEOUT)),
        ssh_hosts: discover_ssh_hosts(home.as_deref()),
    }
}

/// Read the concrete `Host` aliases from `~/.ssh/config`.
///
/// This is a pure read: setup never runs `ssh` while discovering. `Include`
/// directives are deliberately not followed, because resolving them correctly
/// means reimplementing OpenSSH's glob and relative-path rules; aliases that
/// live in an included file simply are not offered, and the user can still
/// type a host by hand.
pub fn discover_ssh_hosts(home: Option<&Path>) -> Vec<String> {
    let Some(home) = home else {
        return Vec::new();
    };
    let Ok(contents) = std::fs::read_to_string(home.join(".ssh").join("config")) else {
        return Vec::new();
    };
    ssh_config_aliases(&contents)
}

/// Extract the usable `Host` aliases from SSH config text.
///
/// Pattern entries (`*`, `?`, `!`) are skipped: they configure other hosts
/// rather than naming one Hel could connect to.
pub fn ssh_config_aliases(contents: &str) -> Vec<String> {
    let mut aliases: Vec<String> = Vec::new();
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((keyword, rest)) = line.split_once(char::is_whitespace) else {
            continue;
        };
        if !keyword.eq_ignore_ascii_case("host") {
            continue;
        }
        for alias in rest.split_whitespace() {
            let alias = alias.trim_matches('"');
            if alias.is_empty() || alias.contains(['*', '?', '!']) {
                continue;
            }
            if !aliases.iter().any(|existing| existing == alias) {
                aliases.push(alias.to_owned());
            }
        }
    }
    aliases
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
        HarnessKind::Deepseek => ".credentials.yaml",
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

/// Read the current directory's GitHub origin, through the same executor every
/// other discovery probe uses so it is bounded and can be faked in tests.
///
/// `git -C` selects the directory instead of a working-directory field on the
/// command, which no executor carries.
fn discover_github_repository(
    executor: &impl CommandExecutor,
    cwd: &Path,
) -> Option<GithubRepository> {
    let command = CommandSpec::new(
        "git",
        [
            "-C".to_owned(),
            cwd.to_string_lossy().into_owned(),
            "remote".to_owned(),
            "get-url".to_owned(),
            "origin".to_owned(),
        ],
    )
    .purpose("detect the current repository's GitHub origin");
    let output = executor.execute(&command).ok()?;
    if output.status != 0 {
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
    build_config_with_runtime(homes, repository, Some((runtime, image)), None, None)
}

fn build_config_with_runtime(
    homes: &[DiscoveredHome],
    repository: Option<&GithubRepository>,
    runtime: Option<(RuntimeKind, &str)>,
    aws: Option<&AwsTargetInput>,
    ssh: Option<&SshTargetInput>,
) -> HelConfig {
    let mut config = HelConfig::default();
    for home in homes {
        let id = unique_id(&config.profiles, home.kind.id());
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
    if let Some(ssh) = ssh {
        // Leave user and identity file unset: the SSH config alias already
        // carries whatever the user configured for this host.
        let connection = SshConnection {
            host: ssh.host.clone(),
            user: None,
            identity_file: None,
            extra_args: vec![],
        };
        let target = match &ssh.kind {
            SshTargetKind::Bare => TargetTemplate::SshBare {
                ssh: connection,
                workspace_prefix: default_ssh_workspace_prefix(),
            },
            SshTargetKind::Podman { image } => TargetTemplate::SshPodman {
                ssh: connection,
                container: ContainerTemplate {
                    image: image.clone(),
                    platform: None,
                    cpus: None,
                    memory: None,
                    environment: BTreeMap::new(),
                },
            },
        };
        // The dialog already refuses a name that collides, so this only guards
        // a caller that builds a config without asking: a chosen SSH name must
        // never silently replace a target configured moments earlier.
        config
            .targets
            .insert(unique_id(&config.targets, &ssh.name), target);
    }
    config
}

/// The same default `serde` applies to a hand-written `ssh-bare` target.
fn default_ssh_workspace_prefix() -> PathBuf {
    PathBuf::from(".local/share/hel/workspaces")
}

/// The first free id at or after `base_id`, so building a config never drops an
/// entry by inserting over one that is already there.
fn unique_id<T>(entries: &BTreeMap<String, T>, base_id: &str) -> String {
    if !entries.contains_key(base_id) {
        return base_id.to_owned();
    }
    let mut number = 2;
    loop {
        let candidate = format!("{base_id}-{number}");
        if !entries.contains_key(&candidate) {
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

/// Ask the setup questions, write the configuration, and report on it.
///
/// The smoke test and the closing doctor report run through different
/// executors on purpose: a smoke test may pull a multi-gigabyte image and must
/// not be given a deadline, while every prerequisite probe must answer quickly
/// or be reported as a fixable check.
pub fn run_setup_dialog_with(
    input: &mut impl BufRead,
    output: &mut impl Write,
    config_path: &Path,
    discovery: &SetupDiscovery,
    smoke_executor: &impl CommandExecutor,
    probe_executor: &impl CommandExecutor,
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
    let runtime_choice = runtime
        .as_ref()
        .map(|(runtime, image)| (*runtime, image.as_str()));
    // Build what the earlier answers already claimed, so the SSH step can
    // refuse a target name that would replace one of them.
    let configured = build_config_with_runtime(
        &discovery.homes,
        discovery.repository.as_ref(),
        runtime_choice,
        aws.as_ref(),
        None,
    );
    let ssh = prompt_ssh_target(input, output, &discovery.ssh_hosts, &configured.targets)?;
    let config = build_config_with_runtime(
        &discovery.homes,
        discovery.repository.as_ref(),
        runtime_choice,
        aws.as_ref(),
        ssh.as_ref(),
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
    // A failed smoke test is a fixable prerequisite, not a reason to abandon
    // the run: the configuration is already written, and this is exactly when
    // the closing report's remediations matter most.
    let smoke_failure = runtime.and_then(|(runtime, image)| {
        let target = smoke_target(runtime, &image);
        run_smoke_test(output, &target, smoke_executor)
            .err()
            .map(|error| smoke_failure_check(runtime, &image, &error))
    });
    write_doctor_report(output, config_path, probe_executor, smoke_failure)?;
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

/// Offer an SSH target built from the aliases in `~/.ssh/config`.
///
/// With no aliases the step prints one line and asks nothing, the same way the
/// AWS step reports skipping.
fn prompt_ssh_target(
    input: &mut impl BufRead,
    output: &mut impl Write,
    aliases: &[String],
    configured: &BTreeMap<String, TargetTemplate>,
) -> Result<Option<SshTargetInput>> {
    if aliases.is_empty() {
        writeln!(
            output,
            "SSH: no host aliases found in ~/.ssh/config; skipping the SSH target."
        )?;
        return Ok(None);
    }
    writeln!(output, "SSH: hosts found in ~/.ssh/config:")?;
    for (index, alias) in aliases.iter().enumerate() {
        writeln!(output, "  {}) {alias}", index + 1)?;
    }
    let answer = prompt(input, output, "Add an SSH target? [y/N]: ")?;
    if !matches!(answer.to_ascii_lowercase().as_str(), "y" | "yes") {
        return Ok(None);
    }

    let choice = prompt(
        input,
        output,
        &format!("Host number 1-{} or a host name: ", aliases.len()),
    )?;
    let host = match choice.parse::<usize>() {
        Ok(index) if (1..=aliases.len()).contains(&index) => aliases[index - 1].clone(),
        _ if !choice.is_empty() => choice,
        _ => {
            writeln!(output, "A host is required; skipping the SSH target.")?;
            return Ok(None);
        }
    };

    let kind = prompt(input, output, "Run agents in Podman on that host? [Y/n]: ")?;
    let kind = if matches!(kind.to_ascii_lowercase().as_str(), "n" | "no") {
        SshTargetKind::Bare
    } else {
        let image = prompt(
            input,
            output,
            &format!("Container image [{DEFAULT_IMAGE}]: "),
        )?;
        SshTargetKind::Podman {
            image: if image.is_empty() {
                DEFAULT_IMAGE.to_owned()
            } else {
                image
            },
        }
    };

    let Some(name) = prompt_ssh_target_name(input, output, &host, configured)? else {
        return Ok(None);
    };

    Ok(Some(SshTargetInput { name, host, kind }))
}

/// Ask for the SSH target's name until the answer is a usable target id.
///
/// Both failures are caught here rather than by `config.validate()` after every
/// question has been asked: an invalid id would otherwise discard the whole
/// dialog, and a name that is already taken would silently replace the target
/// it collides with.
fn prompt_ssh_target_name(
    input: &mut impl BufRead,
    output: &mut impl Write,
    host: &str,
    configured: &BTreeMap<String, TargetTemplate>,
) -> Result<Option<String>> {
    loop {
        let Some(answer) = prompt_line(input, output, &format!("Target name [{host}]: "))? else {
            writeln!(output, "Input ended; skipping the SSH target.")?;
            return Ok(None);
        };
        let name = if answer.is_empty() {
            host.to_owned()
        } else {
            answer
        };
        if let Err(error) = validate_id("target", &name) {
            writeln!(output, "{error}")?;
            continue;
        }
        if configured.contains_key(&name) {
            writeln!(
                output,
                "Target {name} is already configured; choose another name."
            )?;
            continue;
        }
        return Ok(Some(name));
    }
}

/// Phrase a failed setup smoke test the way `hel doctor --smoke` phrases the
/// same failure, so it joins the closing report instead of ending the run.
fn smoke_failure_check(runtime: RuntimeKind, image: &str, error: &anyhow::Error) -> DoctorCheck {
    DoctorCheck::fixable(
        format!("runtime.{}.smoke", runtime.id()),
        format!("{} smoke test", runtime.label()),
        format!("Disposable run/exec/remove smoke test failed for image {image}: {error:#}"),
        format!(
            "Fix the configured image or the {} runtime, then run `hel doctor --smoke` again.",
            runtime.label()
        ),
    )
}

/// End setup with the same report `hel doctor` prints, so the user gets one
/// ready/fixable summary with remediations instead of two different signals.
///
/// `extra` carries anything setup itself learned that doctor cannot repeat
/// without the opt-in smoke test.
fn write_doctor_report(
    output: &mut impl Write,
    config_path: &Path,
    executor: &impl CommandExecutor,
    extra: Option<DoctorCheck>,
) -> Result<()> {
    writeln!(output)?;
    writeln!(output, "Running `hel doctor` checks on the new config...")?;
    let mut checks = run_with_config_path(
        config_path,
        executor,
        current_apple_platform(executor),
        DoctorOptions { smoke: false },
    );
    checks.extend(extra);
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
    Ok(prompt_line(input, output, label)?.unwrap_or_default())
}

/// Read one answer, reporting `None` once the input has ended.
///
/// Every question but one treats the end of input as an empty answer and takes
/// its default. A question that must be asked again until it is answered needs
/// the difference, or it would loop forever against a closed stdin.
fn prompt_line(
    input: &mut impl BufRead,
    output: &mut impl Write,
    label: &str,
) -> Result<Option<String>> {
    write!(output, "{label}")?;
    output.flush()?;
    let mut answer = String::new();
    let read = input
        .read_line(&mut answer)
        .context("read setup response")?;
    Ok((read > 0).then(|| answer.trim().to_owned()))
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
    for (id, target) in &config.targets {
        match target {
            TargetTemplate::SshBare { ssh, .. } => {
                writeln!(output, "  SSH target {id} on {} (no container)", ssh.host)?;
            }
            TargetTemplate::SshPodman { ssh, container } => {
                writeln!(
                    output,
                    "  SSH target {id} on {} using Podman image {}",
                    ssh.host, container.image
                )?;
            }
            _ => {}
        }
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
            ssh_hosts: vec![],
        }
    }

    #[test]
    fn discovers_default_and_overridden_homes_with_authentication_markers() {
        let directory = tempfile::tempdir().unwrap();
        let home = directory.path().join("home");
        let codex = home.join(".codex");
        let kimi = home.join(".kimi-code");
        let grok = home.join(".grok");
        let deepseek = home.join(".dsh");
        let claude = directory.path().join("claude-override");
        fs::create_dir_all(&codex).unwrap();
        fs::create_dir_all(kimi.join("credentials")).unwrap();
        fs::create_dir_all(&grok).unwrap();
        fs::create_dir_all(&deepseek).unwrap();
        fs::create_dir_all(&claude).unwrap();
        fs::write(codex.join("auth.json"), "{}").unwrap();
        fs::write(kimi.join("credentials/kimi-code.json"), "{}").unwrap();
        fs::write(grok.join("auth.json"), "{}").unwrap();
        fs::write(deepseek.join(".credentials.yaml"), "version: 1\n").unwrap();
        fs::write(claude.join(".credentials.json"), "{}").unwrap();

        let homes = discover_harness_homes(Some(&home), [(HarnessKind::Claude, claude.clone())]);

        assert_eq!(homes.len(), 5);
        assert!(homes.iter().all(|home| home.authenticated));
        assert!(homes.iter().any(|home| home.path == codex));
        assert!(homes.iter().any(|home| home.path == claude));
        assert!(homes.iter().any(|home| home.path == kimi));
        assert!(
            homes
                .iter()
                .any(|home| { home.path == deepseek && home.kind == HarnessKind::Deepseek })
        );
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
        let config = build_config_with_runtime(&[], None, None, Some(&aws), None);
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

    const SSH_CONFIG_FIXTURE: &str = r#"
# Personal hosts
Host *
    ServerAliveInterval 60

Host builder build.example.com
    HostName build.example.com
    User dev

Host bastion
  HostName 10.0.0.1
  IdentityFile ~/.ssh/id_ed25519

Host prod-*
    User deploy

Host !staging *.internal
    User deploy

Host builder
    Compression yes
"#;

    #[test]
    fn ssh_config_parsing_keeps_concrete_aliases_and_drops_pattern_blocks() {
        let aliases = ssh_config_aliases(SSH_CONFIG_FIXTURE);

        assert_eq!(
            aliases,
            vec!["builder", "build.example.com", "bastion"],
            "wildcard, negated, and duplicate entries must not appear"
        );
    }

    #[test]
    fn ssh_config_parsing_returns_nothing_for_a_config_of_only_wildcards() {
        assert!(
            ssh_config_aliases(
                "Host *
  User dev
"
            )
            .is_empty()
        );
        assert!(ssh_config_aliases("").is_empty());
    }

    #[test]
    fn the_ssh_step_asks_nothing_when_the_ssh_config_has_no_aliases() {
        let mut input = b"".as_slice();
        let mut output = Vec::new();

        assert_eq!(
            prompt_ssh_target(&mut input, &mut output, &[], &BTreeMap::new()).unwrap(),
            None
        );
        let output = String::from_utf8(output).unwrap();
        assert!(output.contains("skipping the SSH target"), "{output}");
        assert!(!output.contains("[y/N]"), "{output}");
    }

    #[test]
    fn declining_the_ssh_step_writes_no_ssh_target() {
        let aliases = vec!["builder".to_owned()];
        let mut input = b"\n".as_slice();
        let mut output = Vec::new();

        assert_eq!(
            prompt_ssh_target(&mut input, &mut output, &aliases, &BTreeMap::new()).unwrap(),
            None
        );
        let config = build_config_with_runtime(&[], None, None, None, None);
        assert!(!config.targets.values().any(|target| matches!(
            target,
            TargetTemplate::SshBare { .. } | TargetTemplate::SshPodman { .. }
        )));
    }

    #[test]
    fn accepting_the_ssh_step_writes_an_ssh_podman_target_with_the_default_image() {
        let aliases = vec!["builder".to_owned(), "bastion".to_owned()];
        // yes, host 1, podman (default), default image, default name.
        let mut input = b"y\n1\n\n\n\n".as_slice();
        let mut output = Vec::new();

        let ssh = prompt_ssh_target(&mut input, &mut output, &aliases, &BTreeMap::new())
            .unwrap()
            .unwrap();

        assert_eq!(
            ssh,
            SshTargetInput {
                name: "builder".into(),
                host: "builder".into(),
                kind: SshTargetKind::Podman {
                    image: DEFAULT_IMAGE.into()
                },
            }
        );
        let config = build_config_with_runtime(&[], None, None, None, Some(&ssh));
        let TargetTemplate::SshPodman { ssh, container } = &config.targets["builder"] else {
            panic!("setup must write an ssh-podman target");
        };
        assert_eq!(ssh.host, "builder");
        assert_eq!(ssh.user, None);
        assert_eq!(ssh.identity_file, None);
        assert_eq!(container.image, DEFAULT_IMAGE);
        config.validate().unwrap();
    }

    #[test]
    fn accepting_the_ssh_step_writes_an_ssh_bare_target_under_a_chosen_name() {
        let aliases = vec!["builder".to_owned()];
        // yes, typed host, no podman, custom target name.
        let mut input = b"y\nother.example.com\nn\nremote\n".as_slice();
        let mut output = Vec::new();

        let ssh = prompt_ssh_target(&mut input, &mut output, &aliases, &BTreeMap::new())
            .unwrap()
            .unwrap();

        assert_eq!(
            ssh,
            SshTargetInput {
                name: "remote".into(),
                host: "other.example.com".into(),
                kind: SshTargetKind::Bare,
            }
        );
        let config = build_config_with_runtime(&[], None, None, None, Some(&ssh));
        let TargetTemplate::SshBare { ssh, .. } = &config.targets["remote"] else {
            panic!("setup must write an ssh-bare target");
        };
        assert_eq!(ssh.host, "other.example.com");
        config.validate().unwrap();
    }

    #[test]
    fn the_ssh_step_reasks_until_the_name_is_a_free_and_valid_target_id() {
        let aliases = vec!["builder".to_owned()];
        let configured = build_config_with_runtime(
            &[],
            None,
            Some((RuntimeKind::Podman, DEFAULT_IMAGE)),
            None,
            None,
        )
        .targets;
        // yes, host 1, no podman, then: a name that is already taken, a name
        // that is not a usable id, and finally a free one.
        let mut input = b"y\n1\nn\npodman\nbuild host\nbuilder\n".as_slice();
        let mut output = Vec::new();

        let ssh = prompt_ssh_target(&mut input, &mut output, &aliases, &configured)
            .unwrap()
            .unwrap();

        assert_eq!(ssh.name, "builder");
        let output = String::from_utf8(output).unwrap();
        assert!(
            output.contains("Target podman is already configured"),
            "{output}"
        );
        assert!(output.contains("invalid target id"), "{output}");
    }

    #[test]
    fn the_ssh_step_stops_asking_for_a_name_once_the_input_ends() {
        let aliases = vec!["podman".to_owned()];
        let configured = build_config_with_runtime(
            &[],
            None,
            Some((RuntimeKind::Podman, DEFAULT_IMAGE)),
            None,
            None,
        )
        .targets;
        // yes, host 1, no podman, then nothing: the default name collides, so
        // the question can never be answered.
        let mut input = b"y\n1\nn\n".as_slice();
        let mut output = Vec::new();

        assert_eq!(
            prompt_ssh_target(&mut input, &mut output, &aliases, &configured).unwrap(),
            None
        );
        let output = String::from_utf8(output).unwrap();
        assert!(output.contains("Input ended; skipping"), "{output}");
    }

    #[test]
    fn an_ssh_target_never_replaces_a_target_configured_earlier() {
        let ssh = SshTargetInput {
            name: "podman".into(),
            host: "builder".into(),
            kind: SshTargetKind::Bare,
        };

        let config = build_config_with_runtime(
            &[],
            None,
            Some((RuntimeKind::Podman, DEFAULT_IMAGE)),
            None,
            Some(&ssh),
        );

        assert!(matches!(
            config.targets["podman"],
            TargetTemplate::LocalPodman { .. }
        ));
        assert!(matches!(
            config.targets["podman-2"],
            TargetTemplate::SshBare { .. }
        ));
        config.validate().unwrap();
    }

    #[test]
    fn the_github_origin_is_discovered_through_the_shared_executor() {
        let executor = RuntimeProbeExecutor::new([ok(b"git@github.com:BrokkAi/hel.git\n")]);

        let repository = discover_github_repository(&executor, Path::new("/work/hel")).unwrap();

        assert_eq!(repository.source(), "BrokkAi/hel");
        let commands = executor.commands.borrow();
        assert_eq!(commands[0].program, "git");
        assert_eq!(
            commands[0].args,
            ["-C", "/work/hel", "remote", "get-url", "origin"]
        );
    }

    #[test]
    fn no_github_origin_is_reported_when_the_probe_fails() {
        let failing = RuntimeProbeExecutor::new([failed(b"not a git repository")]);
        assert_eq!(
            discover_github_repository(&failing, Path::new("/work/plain")),
            None
        );

        let missing = RuntimeProbeExecutor::new([]);
        assert_eq!(
            discover_github_repository(&missing, Path::new("/work/plain")),
            None
        );
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
        let config = build_config_with_runtime(&[], None, None, None, None);
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
            ssh_hosts: vec![],
        };
        let executor = FakeExecutor::succeeds();
        let mut input = b"\n\ny\n".as_slice();
        let mut output = Vec::new();

        assert_eq!(
            run_setup_dialog_with(
                &mut input,
                &mut output,
                &config_path,
                &discovery,
                &executor,
                &executor,
            )
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
    fn a_failed_smoke_test_becomes_a_fixable_line_in_the_closing_report() {
        let directory = tempfile::tempdir().unwrap();
        let config_path = directory.path().join("config.toml");
        let discovery = SetupDiscovery {
            runtimes: vec![RuntimeProbe {
                kind: RuntimeKind::Podman,
                usable: true,
                detail: "podman version 5".into(),
                remediation: None,
            }],
            ..discovery_without_runtimes()
        };
        // Create the container, fail the command inside it, remove it.
        let executor = FakeExecutor {
            commands: RefCell::new(vec![]),
            statuses: vec![0, 1, 0],
        };
        let mut input = b"\n\ny\n".as_slice();
        let mut output = Vec::new();

        let outcome = run_setup_dialog_with(
            &mut input,
            &mut output,
            &config_path,
            &discovery,
            &executor,
            &executor,
        )
        .unwrap();

        assert_eq!(outcome, SetupOutcome::Written);
        assert!(config_path.exists());
        let output = String::from_utf8(output).unwrap();
        assert!(output.contains("fixable Podman smoke test"), "{output}");
        assert!(
            output.contains("remediation: Fix the configured image or the Podman runtime"),
            "{output}"
        );
        // The report the user was promised still runs, and still ends with the
        // instruction to apply the remediations it just listed.
        assert!(
            output.contains("Running `hel doctor` checks on the new config..."),
            "{output}"
        );
        assert!(
            output.contains("Apply the remediations above, then rerun `hel doctor`."),
            "{output}"
        );
        assert!(
            output.ends_with("Press n to start your first session.\n"),
            "{output}"
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

        run_setup_dialog_with(
            &mut input,
            &mut output,
            &config_path,
            &discovery,
            &executor,
            &executor,
        )
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
            ssh_hosts: vec![],
        };
        let executor = FakeExecutor::succeeds();
        let mut input = b"y\n".as_slice();
        let mut output = Vec::new();

        assert_eq!(
            run_setup_dialog_with(
                &mut input,
                &mut output,
                &config_path,
                &discovery,
                &executor,
                &executor,
            )
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

        for kind in [
            HarnessKind::Codex,
            HarnessKind::Claude,
            HarnessKind::Deepseek,
        ] {
            assert!(!warning(kind).contains("DANGER"), "{kind:?}");
        }
    }
}
