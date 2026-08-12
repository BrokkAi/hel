//! Declarative execution plans for Hel session targets.
//!
//! Plans deliberately contain argv vectors instead of local shell strings.  A
//! shell is used only at the SSH boundary, where OpenSSH necessarily sends a
//! command string; every remotely supplied argument is POSIX-quoted there.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const SESSION_LABEL: &str = "dev.hel.session";
pub const MANAGED_LABEL: &str = "dev.hel.managed";
pub const SESSION_TAG: &str = "dev.hel.session";
pub const MANAGED_TAG: &str = "dev.hel.managed";
pub const CONTAINER_WORKSPACE: &str = "/workspace";
pub const PODMAN_DOCUMENTATION_PATH: &str = "docs/PODMAN.md";

const PODMAN_MINIMUM_MAJOR_VERSION: u32 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManagedResourceKind {
    Container,
    Ec2Instance,
}

/// Build command-line fragments that identify resources Hel owns for a session.
fn managed_resource_identity_args(kind: ManagedResourceKind, session_id: &str) -> Vec<String> {
    match kind {
        ManagedResourceKind::Container => vec![
            "--label".to_owned(),
            format!("{SESSION_LABEL}={session_id}"),
            "--label".to_owned(),
            format!("{MANAGED_LABEL}=true"),
        ],
        ManagedResourceKind::Ec2Instance => vec![
            "--tag-specifications".to_owned(),
            format!(
                "ResourceType=instance,Tags=[{{Key={SESSION_TAG},Value={session_id}}},{{Key={MANAGED_TAG},Value=true}}]"
            ),
        ],
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandSpec {
    pub program: String,
    pub args: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    pub purpose: String,
}

impl CommandSpec {
    pub fn new(
        program: impl Into<String>,
        args: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            program: program.into(),
            args: args.into_iter().map(Into::into).collect(),
            env: BTreeMap::new(),
            purpose: String::new(),
        }
    }

    pub fn purpose(mut self, purpose: impl Into<String>) -> Self {
        self.purpose = purpose.into();
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutput {
    pub status: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionResourceUsage {
    pub memory_current_bytes: u64,
    pub memory_limit_bytes: Option<u64>,
    pub swap_current_bytes: Option<u64>,
    pub swap_limit_bytes: Option<u64>,
    pub writable_disk_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionResourceProbe {
    pub memory: CommandSpec,
    pub disk: CommandSpec,
}

/// An additional host directory made available to a single container session.
///
/// The runtime selects the isolation mode: Podman uses a copy-on-write overlay
/// mount while Apple Container receives a read-only bind mount.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdditionalMount {
    pub source: PathBuf,
    pub destination: PathBuf,
}

pub fn validate_additional_mounts(mounts: &[AdditionalMount]) -> Result<()> {
    let mut destinations = BTreeSet::new();
    for mount in mounts {
        if !mount.source.is_absolute() || mount.source.as_os_str().is_empty() {
            bail!("additional mount source must be an absolute directory path");
        }
        if !mount.destination.is_absolute()
            || mount.destination.as_os_str().is_empty()
            || mount
                .destination
                .components()
                .any(|component| matches!(component, std::path::Component::ParentDir))
        {
            bail!("additional mount destination must be a safe absolute container path");
        }
        if !destinations.insert(mount.destination.clone()) {
            bail!(
                "additional mount destination {:?} is configured more than once",
                mount.destination
            );
        }
    }
    Ok(())
}

/// Choose the editable default destination for an additional host directory.
pub fn default_mount_destination(source: &Path, existing: &[AdditionalMount]) -> PathBuf {
    let basename = source
        .file_name()
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| std::ffi::OsStr::new("mount"));
    let base = PathBuf::from("/mnt").join(basename);
    if !existing.iter().any(|mount| mount.destination == base) {
        return base;
    }
    for number in 2.. {
        let candidate =
            PathBuf::from("/mnt").join(format!("{}-{number}", basename.to_string_lossy()));
        if !existing.iter().any(|mount| mount.destination == candidate) {
            return candidate;
        }
    }
    unreachable!("a finite mount list always has an unused numbered destination")
}

/// Complete an on-disk directory path without spawning a shell.
pub fn local_directory_completions(prefix: &str) -> Vec<String> {
    let (directory, fragment) = match prefix.rsplit_once('/') {
        Some((directory, fragment)) => (format!("{directory}/"), fragment),
        None => (String::new(), prefix),
    };
    let lookup = if directory.is_empty() {
        "."
    } else {
        &directory
    };
    let Ok(entries) = fs::read_dir(lookup) else {
        return Vec::new();
    };
    let mut matches = entries
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let name = entry.file_name();
            let name = name.to_str()?;
            (name.starts_with(fragment) && entry.path().is_dir())
                .then(|| format!("{directory}{name}/"))
        })
        .collect::<Vec<_>>();
    matches.sort();
    matches.dedup();
    matches
}

/// Return the single match or the extra shared path prefix that Tab can add.
pub fn path_completion(prefix: &str, candidates: &[String]) -> Option<String> {
    let first = candidates.first()?;
    if candidates.len() == 1 {
        return Some(first.clone());
    }
    let common = candidates
        .iter()
        .skip(1)
        .fold(first.clone(), |common, next| {
            common
                .chars()
                .zip(next.chars())
                .take_while(|(left, right)| left == right)
                .map(|(character, _)| character)
                .collect()
        });
    (common.len() > prefix.len() && common.starts_with(prefix)).then_some(common)
}

pub trait CommandExecutor {
    fn execute(&self, command: &CommandSpec) -> Result<CommandOutput>;
}

pub struct ProcessExecutor;

impl CommandExecutor for ProcessExecutor {
    fn execute(&self, command: &CommandSpec) -> Result<CommandOutput> {
        let output = Command::new(&command.program)
            .args(&command.args)
            .envs(&command.env)
            .stdin(Stdio::null())
            .output()
            .with_context(|| format!("run {} for {}", command.program, command.purpose))?;
        let status = output.status.code().unwrap_or(-1);
        Ok(CommandOutput {
            status,
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodmanPreflight {
    pub version: String,
}

/// Verify the fast local preconditions for Hel's rootless Podman target.
///
/// This intentionally never pulls an image. Image availability is verified by
/// `hel setup`'s smoke test and by the subsequent target creation command.
pub fn verify_local_podman(executor: &impl CommandExecutor) -> Result<PodmanPreflight> {
    let version = execute_podman_preflight(
        executor,
        CommandSpec::new("podman", ["--version"]).purpose("check Podman version"),
        "Postcondition `podman --version` succeeds with Podman 4.0.0 or newer",
        "Install or upgrade Podman: Debian/Ubuntu `sudo apt update && sudo apt install -y podman uidmap`; Fedora `sudo dnf install -y podman shadow-utils`.",
    )?;
    let version = parse_podman_version(&version.stdout)?;

    let rootless = execute_podman_preflight(
        executor,
        CommandSpec::new(
            "podman",
            ["info", "--format", "{{.Host.Security.Rootless}}"],
        )
        .purpose("check rootless Podman mode"),
        "Postcondition `podman info --format '{{.Host.Security.Rootless}}'` prints `true`",
        "Run Hel as the ordinary user without `sudo`; if a remote Podman connection is configured, unset `CONTAINER_HOST` or select the rootless local connection.",
    )?;
    let rootless_output = String::from_utf8_lossy(&rootless.stdout);
    if rootless_output.trim() != "true" {
        bail!(
            "Podman preflight failed: Postcondition `podman info --format '{{{{.Host.Security.Rootless}}}}'` prints `true` returned {:?}. Run Hel as the ordinary user without `sudo`; if a remote Podman connection is configured, unset `CONTAINER_HOST` or select the rootless local connection. See {PODMAN_DOCUMENTATION_PATH}.",
            rootless_output.trim()
        );
    }

    let uid_map = execute_podman_preflight(
        executor,
        CommandSpec::new("podman", ["unshare", "cat", "/proc/self/uid_map"])
            .purpose("check rootless Podman UID map"),
        "Postcondition `podman unshare cat /proc/self/uid_map` maps container UIDs 0 and 1",
        "Install UID-map helpers (`sudo apt install -y uidmap` on Debian/Ubuntu or `sudo dnf install -y shadow-utils` on Fedora), then add subordinate ranges with `sudo usermod --add-subuids 100000-165535 --add-subgids 100000-165535 \"$USER\"` and start a fresh login session.",
    )?;
    if !valid_rootless_uid_map(&uid_map.stdout) {
        bail!(
            "Podman preflight failed: Postcondition `podman unshare cat /proc/self/uid_map` maps container UIDs 0 and 1 was not met. Add subordinate ranges with `sudo usermod --add-subuids 100000-165535 --add-subgids 100000-165535 \"$USER\"`, verify `/etc/subuid` and `/etc/subgid`, then log out and back in. See {PODMAN_DOCUMENTATION_PATH}."
        );
    }

    Ok(PodmanPreflight { version })
}

fn execute_podman_preflight(
    executor: &impl CommandExecutor,
    command: CommandSpec,
    postcondition: &str,
    remediation: &str,
) -> Result<CommandOutput> {
    let output = executor.execute(&command).map_err(|error| {
        anyhow::anyhow!(
            "Podman preflight failed: {postcondition}. {remediation} See {PODMAN_DOCUMENTATION_PATH}. Underlying error: {error}"
        )
    })?;
    if output.status != 0 {
        bail!(
            "Podman preflight failed: {postcondition}. {remediation} See {PODMAN_DOCUMENTATION_PATH}. Podman reported: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(output)
}

fn parse_podman_version(stdout: &[u8]) -> Result<String> {
    let version = String::from_utf8_lossy(stdout).trim().to_owned();
    let Some(candidate) = version
        .split_whitespace()
        .find(|part| part.as_bytes().first().is_some_and(u8::is_ascii_digit))
    else {
        bail!(
            "Podman preflight failed: Postcondition `podman --version` succeeds with Podman 4.0.0 or newer returned {version:?}. Install or upgrade Podman: Debian/Ubuntu `sudo apt update && sudo apt install -y podman uidmap`; Fedora `sudo dnf install -y podman shadow-utils`. See {PODMAN_DOCUMENTATION_PATH}."
        );
    };
    let Some(major) = candidate
        .split('.')
        .next()
        .and_then(|part| part.parse::<u32>().ok())
    else {
        bail!(
            "Podman preflight failed: Postcondition `podman --version` succeeds with Podman 4.0.0 or newer returned {version:?}. Install or upgrade Podman: Debian/Ubuntu `sudo apt update && sudo apt install -y podman uidmap`; Fedora `sudo dnf install -y podman shadow-utils`. See {PODMAN_DOCUMENTATION_PATH}."
        );
    };
    if major < PODMAN_MINIMUM_MAJOR_VERSION {
        bail!(
            "Podman preflight failed: Postcondition `podman --version` succeeds with Podman 4.0.0 or newer was not met (found {candidate}). Upgrade Podman to 4.0.0 or newer: Debian/Ubuntu `sudo apt update && sudo apt install -y podman uidmap`; Fedora `sudo dnf install -y podman shadow-utils`. See {PODMAN_DOCUMENTATION_PATH}."
        );
    }
    Ok(candidate.to_owned())
}

fn valid_rootless_uid_map(stdout: &[u8]) -> bool {
    let mappings = String::from_utf8_lossy(stdout)
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            Some((
                fields.next()?.parse::<u64>().ok()?,
                fields.next()?.parse::<u64>().ok()?,
                fields.next()?.parse::<u64>().ok()?,
            ))
        })
        .collect::<Vec<_>>();
    [0, 1].into_iter().all(|container_id| {
        mappings.iter().any(|(inside, _outside, length)| {
            inside
                .checked_add(*length)
                .is_some_and(|end| *inside <= container_id && container_id < end)
        })
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandPlan {
    pub description: String,
    pub commands: Vec<CommandSpec>,
}

impl CommandPlan {
    pub fn execute(&self, executor: &impl CommandExecutor) -> Result<Vec<CommandOutput>> {
        let mut outputs = Vec::with_capacity(self.commands.len());
        for command in &self.commands {
            let output = executor.execute(command)?;
            if output.status != 0 {
                bail!(
                    "{} failed with status {}: {}",
                    command.purpose,
                    output.status,
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            outputs.push(output);
        }
        Ok(outputs)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositorySpec {
    /// Clone URL for network-backed repositories. `None` creates an empty
    /// repository which a verified local snapshot restores later.
    pub url: Option<String>,
    pub destination: String,
    pub git_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectBundleSpec {
    pub primary: String,
    pub repositories: Vec<RepositorySpec>,
}

impl ProjectBundleSpec {
    pub fn validate(&self) -> Result<()> {
        validate_relative_path(&self.primary)?;
        if self.repositories.is_empty() {
            bail!("a project bundle must contain at least one repository");
        }
        let mut destinations = std::collections::BTreeSet::new();
        for repository in &self.repositories {
            validate_relative_path(&repository.destination)?;
            if repository
                .url
                .as_deref()
                .is_some_and(|url| url.trim().is_empty() || url.starts_with('-'))
            {
                bail!("invalid repository URL");
            }
            if !destinations.insert(&repository.destination) {
                bail!(
                    "duplicate repository destination {}",
                    repository.destination
                );
            }
        }
        if !destinations.contains(&self.primary) {
            bail!("primary repository is not present in the bundle");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerTemplate {
    pub image: String,
    #[serde(default)]
    pub extra_run_args: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SshTarget {
    pub destination: String,
    #[serde(default)]
    pub ssh_args: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AwsTemplate {
    pub profile: String,
    pub region: String,
    pub launch_template: String,
    pub launch_template_version: Option<String>,
    pub ssh: SshTarget,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TargetTemplate {
    LocalPodman(ContainerTemplate),
    AppleContainer(ContainerTemplate),
    AwsEc2(AwsTemplate),
    SshBare {
        ssh: SshTarget,
        #[serde(default = "default_ssh_prefix")]
        workspace_prefix: String,
    },
    SshPodman {
        ssh: SshTarget,
        container: ContainerTemplate,
    },
}

fn default_ssh_prefix() -> String {
    ".local/share/hel/workspaces".to_owned()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TargetLocator {
    LocalPodman {
        container_id: String,
    },
    AppleContainer {
        container_id: String,
    },
    AwsEc2 {
        profile: String,
        region: String,
        instance_id: String,
        ssh: SshTarget,
        workspace: String,
    },
    SshBare {
        ssh: SshTarget,
        workspace: String,
    },
    SshPodman {
        ssh: SshTarget,
        container_id: String,
    },
}

pub fn resource_name(session_id: &str) -> Result<String> {
    validate_session_id(session_id)?;
    let readable: String = session_id
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .take(12)
        .map(|character| character.to_ascii_lowercase())
        .collect();
    let digest = Sha256::digest(session_id.as_bytes());
    Ok(format!(
        "hel-{readable}-{:02x}{:02x}{:02x}",
        digest[0], digest[1], digest[2]
    ))
}

pub fn workspace_for(template: &TargetTemplate, session_id: &str) -> Result<String> {
    validate_session_id(session_id)?;
    match template {
        TargetTemplate::LocalPodman(_)
        | TargetTemplate::AppleContainer(_)
        | TargetTemplate::SshPodman { .. } => Ok(CONTAINER_WORKSPACE.to_owned()),
        TargetTemplate::AwsEc2(_) => Ok(format!(".local/share/hel/workspaces/{session_id}")),
        TargetTemplate::SshBare {
            workspace_prefix, ..
        } => {
            validate_workspace_prefix(workspace_prefix)?;
            Ok(format!(
                "{}/{session_id}",
                workspace_prefix.trim_end_matches('/')
            ))
        }
    }
}

/// Create the initial resource. AWS address discovery and all SSH bootstrap
/// happen after parsing the `run-instances` response and constructing a locator.
pub fn provision_plan(
    template: &TargetTemplate,
    session_id: &str,
    bundle: &ProjectBundleSpec,
    additional_mounts: &[AdditionalMount],
) -> Result<CommandPlan> {
    bundle.validate()?;
    if !additional_mounts.is_empty()
        && !matches!(
            template,
            TargetTemplate::LocalPodman(_)
                | TargetTemplate::AppleContainer(_)
                | TargetTemplate::SshPodman { .. }
        )
    {
        bail!("additional mounts require a container-backed target");
    }
    let name = resource_name(session_id)?;
    let mut commands = Vec::new();
    match template {
        TargetTemplate::LocalPodman(container) => {
            validate_container_template(container)?;
            commands.push(container_run(
                "podman",
                container,
                &name,
                session_id,
                additional_mounts,
            )?);
            commands.extend(
                install_git_plan(ExecutionBoundary::Container {
                    engine: "podman",
                    container_id: &name,
                })
                .commands,
            );
            commands.extend(clone_commands(bundle, CONTAINER_WORKSPACE, |args| {
                container_exec("podman", &name, args)
            }));
        }
        TargetTemplate::AppleContainer(container) => {
            validate_container_template(container)?;
            commands.push(
                CommandSpec::new("container", ["system", "status"])
                    .purpose("check Apple container service"),
            );
            commands.push(container_run(
                "container",
                container,
                &name,
                session_id,
                additional_mounts,
            )?);
            commands.extend(
                install_git_plan(ExecutionBoundary::Container {
                    engine: "container",
                    container_id: &name,
                })
                .commands,
            );
            commands.extend(clone_commands(bundle, CONTAINER_WORKSPACE, |args| {
                container_exec("container", &name, args)
            }));
        }
        TargetTemplate::AwsEc2(aws) => {
            validate_aws(aws)?;
            let launch_key = if aws.launch_template.starts_with("lt-") {
                "LaunchTemplateId"
            } else {
                "LaunchTemplateName"
            };
            let mut launch = format!("{launch_key}={}", aws.launch_template);
            if let Some(version) = &aws.launch_template_version {
                launch.push_str(",Version=");
                launch.push_str(version);
            }
            let mut args = vec![
                "--profile".to_owned(),
                aws.profile.clone(),
                "--region".to_owned(),
                aws.region.clone(),
                "ec2".to_owned(),
                "run-instances".to_owned(),
                "--launch-template".to_owned(),
                launch,
            ];
            args.extend(managed_resource_identity_args(
                ManagedResourceKind::Ec2Instance,
                session_id,
            ));
            args.extend(["--output".to_owned(), "json".to_owned()]);
            commands.push(CommandSpec::new("aws", args).purpose("launch EC2 session instance"));
        }
        TargetTemplate::SshBare {
            ssh,
            workspace_prefix: _,
        } => {
            validate_ssh(ssh)?;
            let workspace = workspace_for(template, session_id)?;
            commands.push(
                ssh_command(ssh, ["mkdir", "-p", &workspace])
                    .purpose("create SSH session workspace"),
            );
            commands.extend(install_git_plan(ExecutionBoundary::Ssh(ssh)).commands);
            commands.extend(clone_commands(bundle, &workspace, |args| {
                ssh_command_owned(ssh, args)
            }));
        }
        TargetTemplate::SshPodman { ssh, container } => {
            validate_ssh(ssh)?;
            validate_container_template(container)?;
            let mut run = vec!["podman".to_owned()];
            run.extend(container_run_args(
                "podman",
                container,
                &name,
                session_id,
                additional_mounts,
            )?);
            commands.push(ssh_command_owned(ssh, run).purpose("start remote Podman container"));
            commands.extend(
                install_git_plan(ExecutionBoundary::SshPodman {
                    ssh,
                    container_id: &name,
                })
                .commands,
            );
            commands.extend(clone_commands(bundle, CONTAINER_WORKSPACE, |args| {
                let mut remote = vec!["podman".to_owned(), "exec".to_owned(), name.clone()];
                remote.extend(args);
                ssh_command_owned(ssh, remote)
            }));
        }
    }
    Ok(CommandPlan {
        description: format!("provision Hel session {session_id}"),
        commands,
    })
}

/// Create the short-lived local container used to verify a setup target.
///
/// This deliberately shares the same argv construction as session targets so
/// setup catches an unusable image or runtime before the first session exists.
pub fn setup_smoke_plan(template: &TargetTemplate, smoke_id: &str) -> Result<CommandPlan> {
    let name = resource_name(smoke_id)?;
    let (engine, container) = match template {
        TargetTemplate::LocalPodman(container) => ("podman", container),
        TargetTemplate::AppleContainer(container) => ("container", container),
        _ => bail!("setup smoke tests require a local container target"),
    };
    validate_container_template(container)?;

    Ok(CommandPlan {
        description: format!("smoke test Hel setup target {smoke_id}"),
        commands: vec![
            container_run(engine, container, &name, smoke_id, &[])?
                .purpose("create disposable setup container"),
            container_exec(engine, &name, ["true"]).purpose("execute setup smoke command"),
            CommandSpec::new(engine, ["rm", "--force", &name])
                .purpose("remove disposable setup container"),
        ],
    })
}

/// Run the disposable setup smoke test and always attempt container cleanup
/// after a successful create step.
pub fn run_setup_smoke_test(
    template: &TargetTemplate,
    smoke_id: &str,
    executor: &impl CommandExecutor,
) -> Result<()> {
    let plan = setup_smoke_plan(template, smoke_id)?;
    execute_checked(executor, &plan.commands[0])?;
    let smoke_result = execute_checked(executor, &plan.commands[1]);
    let cleanup_result = execute_checked(executor, &plan.commands[2]);
    smoke_result?;
    cleanup_result
}

fn execute_checked(executor: &impl CommandExecutor, command: &CommandSpec) -> Result<()> {
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

/// Clone/bootstrap commands for AWS once the exact instance ID and address are known.
pub fn provision_on_locator_plan(
    locator: &TargetLocator,
    session_id: &str,
    bundle: &ProjectBundleSpec,
) -> Result<CommandPlan> {
    bundle.validate()?;
    verify_locator(locator, session_id)?;
    let TargetLocator::AwsEc2 { ssh, workspace, .. } = locator else {
        bail!("post-launch provisioning is only required for AWS");
    };
    let mut commands =
        vec![ssh_command(ssh, ["mkdir", "-p", workspace]).purpose("create EC2 session workspace")];
    commands.extend(install_git_plan(ExecutionBoundary::Ssh(ssh)).commands);
    commands.extend(clone_commands(bundle, workspace, |args| {
        ssh_command_owned(ssh, args)
    }));
    Ok(CommandPlan {
        description: format!("initialize EC2 session {session_id}"),
        commands,
    })
}

pub fn reconnect_plan(locator: &TargetLocator, session_id: &str) -> Result<CommandPlan> {
    verify_locator(locator, session_id)?;
    let root = worker_root(locator, session_id)?;
    let binary = format!("{root}/hel");
    let command = match locator {
        TargetLocator::LocalPodman { container_id } => container_exec(
            "podman",
            container_id,
            [&binary, "worker", "proxy", "--root", &root],
        ),
        TargetLocator::AppleContainer { container_id } => container_exec(
            "container",
            container_id,
            [&binary, "worker", "proxy", "--root", &root],
        ),
        TargetLocator::AwsEc2 { ssh, .. } | TargetLocator::SshBare { ssh, .. } => {
            ssh_command(ssh, [&binary, "worker", "proxy", "--root", &root])
        }
        TargetLocator::SshPodman { ssh, container_id } => ssh_command(
            ssh,
            [
                "podman",
                "exec",
                "-i",
                container_id,
                &binary,
                "worker",
                "proxy",
                "--root",
                &root,
            ],
        ),
    }
    .purpose("connect to Hel worker");
    Ok(CommandPlan {
        description: format!("reconnect Hel session {session_id}"),
        commands: vec![command],
    })
}

/// Run the target-side half of the local Git bridge over the same trusted
/// execution boundary Hel uses for worker control.
pub fn git_bridge_command(locator: &TargetLocator, session_id: &str) -> Result<CommandSpec> {
    let root = worker_root(locator, session_id)?;
    let binary = format!("{root}/hel");
    command_on_locator(
        locator,
        session_id,
        vec![
            binary,
            "worker".into(),
            "git-bridge".into(),
            "--root".into(),
            root,
        ],
        "bridge local Git repositories",
    )
}

/// Wrap an argv vector for execution at a provisioned session target.
pub fn command_on_locator(
    locator: &TargetLocator,
    session_id: &str,
    args: Vec<String>,
    purpose: impl Into<String>,
) -> Result<CommandSpec> {
    verify_locator(locator, session_id)?;
    if args.is_empty() {
        bail!("target command must not be empty");
    }
    let command = match locator {
        TargetLocator::LocalPodman { container_id } => container_exec("podman", container_id, args),
        TargetLocator::AppleContainer { container_id } => {
            container_exec("container", container_id, args)
        }
        TargetLocator::AwsEc2 { ssh, .. } | TargetLocator::SshBare { ssh, .. } => {
            ssh_command_owned(ssh, args)
        }
        TargetLocator::SshPodman { ssh, container_id } => {
            let mut remote = vec![
                "podman".to_owned(),
                "exec".to_owned(),
                "-i".to_owned(),
                container_id.to_owned(),
            ];
            remote.extend(args);
            ssh_command_owned(ssh, remote)
        }
    };
    Ok(command.purpose(purpose))
}

const CGROUP_MEMORY_USAGE_SCRIPT: &str = r#"
for file in memory.current memory.max memory.swap.current memory.swap.max; do
    path="/sys/fs/cgroup/$file"
    if [ -r "$path" ]; then
        printf "%s=%s\n" "$file" "$(cat "$path")"
    fi
done
"#;

const HOST_MEMORY_USAGE_SCRIPT: &str = r#"
awk '
    /^MemTotal:/ { memory_total = $2 }
    /^MemAvailable:/ { memory_available = $2 }
    /^SwapTotal:/ { swap_total = $2 }
    /^SwapFree:/ { swap_free = $2 }
    END {
        printf "memory.current=%.0f\n", (memory_total - memory_available) * 1024
        printf "memory.max=%.0f\n", memory_total * 1024
        printf "memory.swap.current=%.0f\n", (swap_total - swap_free) * 1024
        printf "memory.swap.max=%.0f\n", swap_total * 1024
    }
' /proc/meminfo
"#;

const AWS_SESSION_DISK_USAGE_SCRIPT: &str = r#"
du -s -B1 -- "$@" 2>/dev/null | awk '{ total += $1 } END { print total + 0 }'
"#;

pub fn resource_probe(locator: &TargetLocator, session_id: &str) -> Result<SessionResourceProbe> {
    verify_locator(locator, session_id)?;
    let (memory, disk) = match locator {
        TargetLocator::LocalPodman { container_id } => (
            container_exec(
                "podman",
                container_id,
                ["sh", "-c", CGROUP_MEMORY_USAGE_SCRIPT],
            )
            .purpose("sample local Podman container memory"),
            CommandSpec::new(
                "podman",
                [
                    "container",
                    "inspect",
                    "--size",
                    "--format",
                    "{{.SizeRw}}",
                    container_id,
                ],
            )
            .purpose("sample local Podman container writable disk"),
        ),
        TargetLocator::SshPodman { ssh, container_id } => (
            ssh_command(
                ssh,
                [
                    "podman",
                    "exec",
                    container_id,
                    "sh",
                    "-c",
                    CGROUP_MEMORY_USAGE_SCRIPT,
                ],
            )
            .purpose("sample remote Podman container memory"),
            ssh_command(
                ssh,
                [
                    "podman",
                    "container",
                    "inspect",
                    "--size",
                    "--format",
                    "{{.SizeRw}}",
                    container_id,
                ],
            )
            .purpose("sample remote Podman container writable disk"),
        ),
        TargetLocator::AwsEc2 { ssh, workspace, .. } => {
            let worker_root = worker_root(locator, session_id)?;
            let profile_root = format!(".local/share/hel/profiles/{session_id}");
            (
                ssh_command(ssh, ["sh", "-c", HOST_MEMORY_USAGE_SCRIPT])
                    .purpose("sample EC2 session memory"),
                ssh_command(
                    ssh,
                    [
                        "sh",
                        "-c",
                        AWS_SESSION_DISK_USAGE_SCRIPT,
                        "sh",
                        workspace.as_str(),
                        worker_root.as_str(),
                        profile_root.as_str(),
                    ],
                )
                .purpose("sample EC2 session disk"),
            )
        }
        TargetLocator::AppleContainer { .. } | TargetLocator::SshBare { .. } => {
            bail!("resource sampling is unsupported for this target")
        }
    };
    Ok(SessionResourceProbe { memory, disk })
}

pub fn parse_resource_usage(
    memory_output: &[u8],
    disk_output: Option<&[u8]>,
) -> Result<SessionResourceUsage> {
    let mut values = BTreeMap::new();
    let memory_text = String::from_utf8_lossy(memory_output);
    for line in memory_text.lines() {
        let Some((name, value)) = line.split_once('=') else {
            continue;
        };
        values.insert(name, value.trim());
    }

    let memory_current_bytes = parse_cgroup_counter(
        values
            .get("memory.current")
            .context("resource probe did not expose memory.current")?,
    )?
    .context("resource probe reported memory.current as unlimited")?;
    let memory_limit_bytes = values
        .get("memory.max")
        .map(|value| parse_cgroup_counter(value))
        .transpose()?
        .flatten();
    let swap_current_bytes = values
        .get("memory.swap.current")
        .map(|value| parse_cgroup_counter(value))
        .transpose()?
        .flatten();
    let swap_limit_bytes = values
        .get("memory.swap.max")
        .map(|value| parse_cgroup_counter(value))
        .transpose()?
        .flatten();
    let writable_disk_bytes =
        disk_output.and_then(|output| String::from_utf8_lossy(output).trim().parse().ok());

    Ok(SessionResourceUsage {
        memory_current_bytes,
        memory_limit_bytes,
        swap_current_bytes,
        swap_limit_bytes,
        writable_disk_bytes,
    })
}

fn parse_cgroup_counter(value: &str) -> Result<Option<u64>> {
    if value == "max" {
        return Ok(None);
    }
    Ok(Some(value.parse().with_context(|| {
        format!("invalid memory counter {value:?}")
    })?))
}

pub fn worker_root(locator: &TargetLocator, session_id: &str) -> Result<String> {
    verify_locator(locator, session_id)?;
    Ok(match locator {
        TargetLocator::LocalPodman { .. }
        | TargetLocator::AppleContainer { .. }
        | TargetLocator::SshPodman { .. } => format!("/var/lib/hel/workers/{session_id}"),
        TargetLocator::AwsEc2 { .. } | TargetLocator::SshBare { .. } => {
            format!(".local/share/hel/workers/{session_id}")
        }
    })
}

pub fn close_plan(locator: &TargetLocator, session_id: &str) -> Result<CommandPlan> {
    verify_locator(locator, session_id)?;
    let session_worker_root = worker_root(locator, session_id)?;
    let session_profile_home = format!(".local/share/hel/profiles/{session_id}");
    let command = match locator {
        TargetLocator::LocalPodman { container_id } => {
            CommandSpec::new("podman", ["rm", "--force", container_id])
                .purpose("remove local Podman session container")
        }
        TargetLocator::AppleContainer { container_id } => {
            CommandSpec::new("container", ["rm", "--force", container_id])
                .purpose("remove Apple session container")
        }
        TargetLocator::AwsEc2 {
            profile,
            region,
            instance_id,
            ..
        } => CommandSpec::new(
            "aws",
            [
                "--profile",
                profile,
                "--region",
                region,
                "ec2",
                "terminate-instances",
                "--instance-ids",
                instance_id,
            ],
        )
        .purpose("terminate exact EC2 session instance"),
        TargetLocator::SshBare { ssh, workspace } => ssh_command(
            ssh,
            [
                "rm",
                "-rf",
                "--",
                workspace,
                &session_worker_root,
                &session_profile_home,
            ],
        )
        .purpose("remove exact SSH session workspace and runtime state"),
        TargetLocator::SshPodman { ssh, container_id } => {
            ssh_command(ssh, ["podman", "rm", "--force", container_id])
                .purpose("remove exact remote Podman session container")
        }
    };
    Ok(CommandPlan {
        description: format!("close Hel session {session_id}"),
        commands: vec![command],
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionBoundary<'a> {
    Direct,
    Container {
        engine: &'a str,
        container_id: &'a str,
    },
    Ssh(&'a SshTarget),
    SshPodman {
        ssh: &'a SshTarget,
        container_id: &'a str,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessProbe<'a> {
    pub executable: &'a str,
    pub version_args: &'a [&'a str],
    pub bridge_executable: Option<&'a str>,
}

/// Compatibility is intentionally interpreted by the controller. A successful
/// probe permits an image-baked tool to be reused; a missing/incompatible tool
/// causes the controller to upload/install its release-owned copy.
pub fn bootstrap_probe_plan(
    boundary: ExecutionBoundary<'_>,
    harness: HarnessProbe<'_>,
) -> Result<CommandPlan> {
    validate_executable(harness.executable)?;
    let mut commands = vec![
        at_boundary(
            boundary,
            std::iter::once(harness.executable)
                .chain(harness.version_args.iter().copied())
                .map(str::to_owned)
                .collect(),
        )
        .purpose("probe harness version"),
    ];
    if let Some(bridge) = harness.bridge_executable {
        validate_executable(bridge)?;
        commands.push(
            at_boundary(boundary, vec![bridge.to_owned(), "--version".to_owned()])
                .purpose("probe ACP bridge version"),
        );
    }
    commands.push(
        at_boundary(boundary, vec!["git".to_owned(), "--version".to_owned()]).purpose("probe Git"),
    );
    Ok(CommandPlan {
        description: "probe reusable target tools".to_owned(),
        commands,
    })
}

/// Thin Linux Git bootstrap. This is used only after `git --version` fails.
pub fn install_git_plan(boundary: ExecutionBoundary<'_>) -> CommandPlan {
    let script = "set -eu; if command -v git >/dev/null 2>&1; then exit 0; fi; SUDO=''; if [ \"$(id -u)\" != 0 ]; then command -v sudo >/dev/null 2>&1 && sudo -n true || { echo 'Git installation requires root or passwordless sudo' >&2; exit 1; }; SUDO='sudo -n'; fi; if command -v apt-get >/dev/null 2>&1; then $SUDO apt-get update; $SUDO apt-get install -y git ca-certificates curl; elif command -v dnf >/dev/null 2>&1; then $SUDO dnf install -y git ca-certificates curl; elif command -v yum >/dev/null 2>&1; then $SUDO yum install -y git ca-certificates curl; elif command -v apk >/dev/null 2>&1; then $SUDO apk add --no-cache git ca-certificates curl; else echo 'Unsupported package manager; install Git manually' >&2; exit 1; fi";
    CommandPlan {
        description: "install missing Git".to_owned(),
        commands: vec![
            at_boundary(
                boundary,
                vec!["sh".to_owned(), "-c".to_owned(), script.to_owned()],
            )
            .purpose("install Git"),
        ],
    }
}

fn clone_commands(
    bundle: &ProjectBundleSpec,
    workspace: &str,
    wrap: impl Fn(Vec<String>) -> CommandSpec,
) -> Vec<CommandSpec> {
    let mut commands = vec![
        wrap(vec![
            "mkdir".to_owned(),
            "-p".to_owned(),
            workspace.to_owned(),
        ])
        .purpose("create bundle workspace"),
    ];
    for repository in &bundle.repositories {
        let destination = format!("{workspace}/{}", repository.destination);
        let Some(url) = &repository.url else {
            commands.push(
                wrap(vec!["git".into(), "init".into(), "--".into(), destination])
                    .purpose(format!("initialize {}", repository.destination)),
            );
            continue;
        };
        let mut args = vec!["git".to_owned(), "clone".to_owned()];
        if let Some(git_ref) = &repository.git_ref {
            args.extend(["--branch".to_owned(), git_ref.clone()]);
        }
        args.push("--".to_owned());
        args.push(url.clone());
        args.push(destination);
        commands.push(wrap(args).purpose(format!("clone {}", repository.destination)));
    }
    commands
}

fn container_run(
    engine: &str,
    template: &ContainerTemplate,
    name: &str,
    session_id: &str,
    additional_mounts: &[AdditionalMount],
) -> Result<CommandSpec> {
    Ok(CommandSpec::new(
        engine,
        container_run_args(engine, template, name, session_id, additional_mounts)?,
    )
    .purpose("start session container"))
}

fn container_run_args(
    engine: &str,
    template: &ContainerTemplate,
    name: &str,
    session_id: &str,
    additional_mounts: &[AdditionalMount],
) -> Result<Vec<String>> {
    validate_additional_mounts(additional_mounts)?;
    let mut args = vec![
        "run".to_owned(),
        "--detach".to_owned(),
        "--name".to_owned(),
        name.to_owned(),
    ];
    args.extend(managed_resource_identity_args(
        ManagedResourceKind::Container,
        session_id,
    ));
    args.extend(template.extra_run_args.clone());
    for mount in additional_mounts {
        let source = mount.source.to_string_lossy();
        let destination = mount.destination.to_string_lossy();
        match engine {
            "podman" => args.extend(["--volume".to_owned(), format!("{source}:{destination}:O")]),
            "container" => args.extend([
                "--mount".to_owned(),
                format!("type=bind,source={source},target={destination},readonly"),
            ]),
            _ => bail!("additional mounts are unsupported for container engine {engine:?}"),
        }
    }
    args.extend([
        template.image.clone(),
        "sleep".to_owned(),
        "infinity".to_owned(),
    ]);
    Ok(args)
}

fn container_exec(
    engine: &str,
    container_id: &str,
    args: impl IntoIterator<Item = impl Into<String>>,
) -> CommandSpec {
    let mut command_args = vec!["exec".to_owned(), "-i".to_owned(), container_id.to_owned()];
    command_args.extend(args.into_iter().map(Into::into));
    CommandSpec::new(engine, command_args)
}

fn at_boundary(boundary: ExecutionBoundary<'_>, args: Vec<String>) -> CommandSpec {
    match boundary {
        ExecutionBoundary::Direct => CommandSpec::new(args[0].clone(), args[1..].iter().cloned()),
        ExecutionBoundary::Container {
            engine,
            container_id,
        } => container_exec(engine, container_id, args),
        ExecutionBoundary::Ssh(ssh) => ssh_command_owned(ssh, args),
        ExecutionBoundary::SshPodman { ssh, container_id } => {
            let mut remote = vec![
                "podman".to_owned(),
                "exec".to_owned(),
                "-i".to_owned(),
                container_id.to_owned(),
            ];
            remote.extend(args);
            ssh_command_owned(ssh, remote)
        }
    }
}

fn ssh_command(ssh: &SshTarget, args: impl IntoIterator<Item = impl AsRef<str>>) -> CommandSpec {
    ssh_command_owned(
        ssh,
        args.into_iter()
            .map(|arg| arg.as_ref().to_owned())
            .collect(),
    )
}

fn ssh_command_owned(ssh: &SshTarget, remote_args: Vec<String>) -> CommandSpec {
    let mut args = ssh.ssh_args.clone();
    args.push(ssh.destination.clone());
    args.push(join_remote_command(&remote_args));
    CommandSpec::new("ssh", args)
}

pub fn join_remote_command(args: &[String]) -> String {
    args.iter()
        .map(|arg| posix_quote(arg))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Complete remote directory paths through the configured SSH target.
///
/// The SSH connection timeout and noninteractive mode keep a Tab press from
/// blocking the wizard when a host is unavailable. The quoted prefix remains
/// literal while the trailing glob is expanded only by the remote shell.
pub fn ssh_directory_completions(
    ssh: &SshTarget,
    prefix: &str,
    executor: &impl CommandExecutor,
) -> Result<Vec<String>> {
    if prefix.is_empty() {
        return Ok(Vec::new());
    }
    let remote_command = format!("ls -d -- {}*/ 2>/dev/null", posix_quote(prefix));
    let mut args = ssh.ssh_args.clone();
    args.extend([
        "-o".into(),
        "BatchMode=yes".into(),
        "-o".into(),
        "ConnectTimeout=3".into(),
        "-o".into(),
        "ServerAliveInterval=2".into(),
        "-o".into(),
        "ServerAliveCountMax=1".into(),
        ssh.destination.clone(),
        remote_command,
    ]);
    let output = executor
        .execute(&CommandSpec::new("ssh", args).purpose("complete remote mount directory"))?;
    if output.status != 0 {
        return Ok(Vec::new());
    }
    let mut matches = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|path| path.starts_with(prefix) && path.ends_with('/'))
        .map(str::to_owned)
        .collect::<Vec<_>>();
    matches.sort();
    matches.dedup();
    Ok(matches)
}

fn posix_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn verify_locator(locator: &TargetLocator, session_id: &str) -> Result<()> {
    let expected_name = resource_name(session_id)?;
    match locator {
        TargetLocator::LocalPodman { container_id }
        | TargetLocator::AppleContainer { container_id }
        | TargetLocator::SshPodman { container_id, .. } => {
            if container_id != &expected_name && !is_runtime_container_id(container_id) {
                bail!(
                    "refusing cleanup: container locator is neither the generated name nor an immutable runtime ID"
                );
            }
        }
        TargetLocator::AwsEc2 {
            instance_id,
            workspace,
            ..
        } => {
            if !valid_ec2_instance_id(instance_id) {
                bail!("refusing cleanup: invalid EC2 instance ID");
            }
            verify_session_workspace(workspace, session_id)?;
        }
        TargetLocator::SshBare { workspace, .. } => {
            verify_session_workspace(workspace, session_id)?
        }
    }
    Ok(())
}

fn verify_session_workspace(workspace: &str, session_id: &str) -> Result<()> {
    validate_workspace_prefix(workspace)?;
    let final_component = workspace.trim_end_matches('/').rsplit('/').next();
    if final_component != Some(session_id) {
        bail!("refusing cleanup: workspace does not end in the exact session ID");
    }
    Ok(())
}

fn validate_session_id(value: &str) -> Result<()> {
    if value.len() < 8
        || value.len() > 128
        || !value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
    {
        bail!("session ID must be 8-128 ASCII letters, digits, '-' or '_'");
    }
    Ok(())
}

fn validate_relative_path(value: &str) -> Result<()> {
    let path = std::path::Path::new(value);
    if value.is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|part| !matches!(part, std::path::Component::Normal(_)))
    {
        bail!("unsafe relative bundle path {value:?}");
    }
    Ok(())
}

fn validate_workspace_prefix(value: &str) -> Result<()> {
    if value.is_empty()
        || value == "/"
        || value.contains('\0')
        || value.split('/').any(|part| part == "..")
    {
        bail!("unsafe workspace path");
    }
    Ok(())
}

fn validate_container_template(template: &ContainerTemplate) -> Result<()> {
    if template.image.trim().is_empty() || template.image.starts_with('-') {
        bail!("invalid container image");
    }
    if template
        .extra_run_args
        .iter()
        .any(|arg| arg == "--name" || arg.starts_with("--name="))
    {
        bail!("container template may not override the generated name");
    }
    if template.extra_run_args.iter().any(|arg| {
        arg == "--label"
            || [SESSION_LABEL, MANAGED_LABEL]
                .iter()
                .any(|label| arg.starts_with(&format!("--label={label}=")))
    }) {
        bail!("container template may not override Hel ownership labels");
    }
    Ok(())
}

fn validate_ssh(ssh: &SshTarget) -> Result<()> {
    if ssh.destination.trim().is_empty()
        || ssh.destination.starts_with('-')
        || ssh.destination.chars().any(char::is_whitespace)
    {
        bail!("invalid SSH destination");
    }
    Ok(())
}

fn validate_aws(aws: &AwsTemplate) -> Result<()> {
    validate_ssh(&aws.ssh)?;
    for (name, value) in [
        ("AWS profile", &aws.profile),
        ("AWS region", &aws.region),
        ("launch template", &aws.launch_template),
    ] {
        if value.is_empty()
            || value.starts_with('-')
            || !value
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '/'))
        {
            bail!("invalid {name}");
        }
    }
    Ok(())
}

fn validate_executable(value: &str) -> Result<()> {
    if value.is_empty() || value.starts_with('-') || value.chars().any(char::is_whitespace) {
        bail!("invalid executable name");
    }
    Ok(())
}

fn valid_ec2_instance_id(value: &str) -> bool {
    value
        .strip_prefix("i-")
        .is_some_and(|rest| rest.len() >= 8 && rest.chars().all(|c| c.is_ascii_hexdigit()))
}

fn is_runtime_container_id(value: &str) -> bool {
    value.len() >= 12 && value.len() <= 128 && value.chars().all(|c| c.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    const SESSION: &str = "018f9dd2-a3b4-7c8d-9000-123456789abc";

    fn bundle() -> ProjectBundleSpec {
        ProjectBundleSpec {
            primary: "app".to_owned(),
            repositories: vec![
                RepositorySpec {
                    url: Some("git@github.com:example/app.git".to_owned()),
                    destination: "app".to_owned(),
                    git_ref: Some("main".to_owned()),
                },
                RepositorySpec {
                    url: Some("https://github.com/example/lib.git".to_owned()),
                    destination: "libs/lib".to_owned(),
                    git_ref: None,
                },
            ],
        }
    }

    fn ssh() -> SshTarget {
        SshTarget {
            destination: "dev@example.test".to_owned(),
            ssh_args: vec!["-o".to_owned(), "BatchMode=yes".to_owned()],
        }
    }

    struct PodmanPreflightExecutor {
        seen: RefCell<Vec<CommandSpec>>,
        outputs: RefCell<Vec<CommandOutput>>,
    }

    impl PodmanPreflightExecutor {
        fn with_outputs(outputs: impl IntoIterator<Item = CommandOutput>) -> Self {
            Self {
                seen: RefCell::new(vec![]),
                outputs: RefCell::new(outputs.into_iter().collect()),
            }
        }
    }

    impl CommandExecutor for PodmanPreflightExecutor {
        fn execute(&self, command: &CommandSpec) -> Result<CommandOutput> {
            self.seen.borrow_mut().push(command.clone());
            Ok(self.outputs.borrow_mut().remove(0))
        }
    }

    fn podman_output(stdout: impl AsRef<[u8]>) -> CommandOutput {
        CommandOutput {
            status: 0,
            stdout: stdout.as_ref().to_vec(),
            stderr: vec![],
        }
    }

    #[test]
    fn podman_preflight_requires_supported_rootless_uid_mapped_runtime() {
        let executor = PodmanPreflightExecutor::with_outputs([
            podman_output(b"podman version 5.4.2\n"),
            podman_output(b"true\n"),
            podman_output(b"         0       1000          1\n         1     100000      65536\n"),
        ]);

        assert_eq!(
            verify_local_podman(&executor).unwrap(),
            PodmanPreflight {
                version: "5.4.2".into()
            }
        );
        let seen = executor.seen.borrow();
        assert_eq!(seen.len(), 3);
        assert_eq!(seen[0].args, ["--version"]);
        assert_eq!(
            seen[1].args,
            ["info", "--format", "{{.Host.Security.Rootless}}"]
        );
        assert_eq!(seen[2].args, ["unshare", "cat", "/proc/self/uid_map"]);
    }

    #[test]
    fn podman_preflight_rejects_unsupported_version_with_upgrade_remediation() {
        let executor =
            PodmanPreflightExecutor::with_outputs([podman_output(b"podman version 3.4.7\n")]);

        let error = verify_local_podman(&executor).unwrap_err().to_string();
        assert!(error.contains("Podman 4.0.0 or newer"));
        assert!(error.contains("apt install -y podman uidmap"));
        assert!(error.contains(PODMAN_DOCUMENTATION_PATH));
    }

    #[test]
    fn podman_preflight_reports_uidmap_helper_remediation() {
        let executor = PodmanPreflightExecutor::with_outputs([
            podman_output(b"podman version 5.4.2\n"),
            podman_output(b"true\n"),
            CommandOutput {
                status: 1,
                stdout: vec![],
                stderr: b"cannot find newuidmap executable".to_vec(),
            },
        ]);

        let error = verify_local_podman(&executor).unwrap_err().to_string();
        assert!(error.contains("podman unshare cat /proc/self/uid_map"));
        assert!(error.contains("apt install -y uidmap"));
        assert!(error.contains(PODMAN_DOCUMENTATION_PATH));
    }

    #[test]
    fn podman_preflight_rejects_a_uid_map_without_subordinate_ids() {
        let executor = PodmanPreflightExecutor::with_outputs([
            podman_output(b"podman version 5.4.2\n"),
            podman_output(b"true\n"),
            podman_output(b"         0       1000          1\n"),
        ]);

        let error = verify_local_podman(&executor).unwrap_err().to_string();
        assert!(error.contains("maps container UIDs 0 and 1"));
        assert!(error.contains("usermod --add-subuids"));
        assert!(error.contains(PODMAN_DOCUMENTATION_PATH));
    }

    #[test]
    fn managed_resource_identity_args_build_container_labels_and_ec2_tags() {
        assert_eq!(
            managed_resource_identity_args(ManagedResourceKind::Container, SESSION),
            vec![
                "--label",
                "dev.hel.session=018f9dd2-a3b4-7c8d-9000-123456789abc",
                "--label",
                "dev.hel.managed=true",
            ]
        );
        assert_eq!(
            managed_resource_identity_args(ManagedResourceKind::Ec2Instance, SESSION),
            vec![
                "--tag-specifications",
                "ResourceType=instance,Tags=[{Key=dev.hel.session,Value=018f9dd2-a3b4-7c8d-9000-123456789abc},{Key=dev.hel.managed,Value=true}]",
            ]
        );
    }

    #[test]
    fn podman_plan_uses_owned_name_label_and_argv_clones() {
        let plan = provision_plan(
            &TargetTemplate::LocalPodman(ContainerTemplate {
                image: "ubuntu:24.04".to_owned(),
                extra_run_args: vec!["--cpus=4".to_owned()],
            }),
            SESSION,
            &bundle(),
            &[],
        )
        .unwrap();
        let name = resource_name(SESSION).unwrap();
        assert_eq!(plan.commands[0].program, "podman");
        assert!(
            plan.commands[0]
                .args
                .windows(2)
                .any(|args| args == ["--name", &name])
        );
        assert!(
            plan.commands[0].args.windows(4).any(|args| args
                == managed_resource_identity_args(ManagedResourceKind::Container, SESSION))
        );
        let clone = plan
            .commands
            .iter()
            .find(|command| command.purpose == "clone app")
            .unwrap();
        assert_eq!(&clone.args[..4], ["exec", "-i", &name, "git"]);
        assert!(clone.args.contains(&"--".to_owned()));
        assert!(clone.args.contains(&"/workspace/app".to_owned()));
        let bootstrap = plan
            .commands
            .iter()
            .find(|command| command.purpose == "install Git")
            .unwrap();
        assert!(bootstrap.args.last().unwrap().contains("command -v git"));
    }

    #[test]
    fn podman_additional_mounts_use_copy_on_write_overlay_volumes() {
        let mounts = [AdditionalMount {
            source: PathBuf::from("/host/cache"),
            destination: PathBuf::from("/mnt/cache"),
        }];
        let plan = provision_plan(
            &TargetTemplate::LocalPodman(ContainerTemplate {
                image: "ubuntu:24.04".to_owned(),
                extra_run_args: vec![],
            }),
            SESSION,
            &bundle(),
            &mounts,
        )
        .unwrap();

        assert!(
            plan.commands[0]
                .args
                .windows(2)
                .any(|args| args == ["--volume", "/host/cache:/mnt/cache:O"])
        );
    }

    #[test]
    fn apple_additional_mounts_use_read_only_bind_fallback() {
        let mounts = [AdditionalMount {
            source: PathBuf::from("/Users/me/assets"),
            destination: PathBuf::from("/mnt/assets"),
        }];
        let plan = provision_plan(
            &TargetTemplate::AppleContainer(ContainerTemplate {
                image: "ubuntu:24.04".to_owned(),
                extra_run_args: vec![],
            }),
            SESSION,
            &bundle(),
            &mounts,
        )
        .unwrap();

        assert!(plan.commands[1].args.windows(2).any(|args| {
            args == [
                "--mount",
                "type=bind,source=/Users/me/assets,target=/mnt/assets,readonly",
            ]
        }));
    }

    #[test]
    fn apple_plan_preflights_and_uses_container_cli() {
        let plan = provision_plan(
            &TargetTemplate::AppleContainer(ContainerTemplate {
                image: "ghcr.io/example/dev:latest".to_owned(),
                extra_run_args: vec![],
            }),
            SESSION,
            &bundle(),
            &[],
        )
        .unwrap();
        assert_eq!(
            plan.commands[0],
            CommandSpec::new("container", ["system", "status"])
                .purpose("check Apple container service")
        );
        assert_eq!(plan.commands[1].program, "container");
        assert!(plan.commands[1].args.windows(4).any(|args| {
            args == managed_resource_identity_args(ManagedResourceKind::Container, SESSION)
        }));
    }

    #[test]
    fn setup_smoke_plan_uses_the_configured_local_runtime_and_cleans_up() {
        let plan = setup_smoke_plan(
            &TargetTemplate::LocalPodman(ContainerTemplate {
                image: "ubuntu:24.04".to_owned(),
                extra_run_args: vec![],
            }),
            "setup-123",
        )
        .unwrap();

        assert_eq!(plan.commands.len(), 3);
        assert_eq!(plan.commands[0].program, "podman");
        assert!(plan.commands[0].args.contains(&"ubuntu:24.04".to_owned()));
        assert_eq!(plan.commands[1].args.last().unwrap(), "true");
        assert_eq!(plan.commands[2].args[0], "rm");
        assert_eq!(
            plan.commands[2].purpose,
            "remove disposable setup container"
        );
    }

    #[test]
    fn setup_smoke_test_removes_a_container_after_a_failed_exec() {
        let executor = FakeExecutor {
            seen: RefCell::new(vec![]),
            fail_at: Some(1),
        };

        assert!(
            run_setup_smoke_test(
                &TargetTemplate::AppleContainer(ContainerTemplate {
                    image: "ubuntu:24.04".to_owned(),
                    extra_run_args: vec![],
                }),
                "setup-123",
                &executor,
            )
            .is_err()
        );
        assert_eq!(executor.seen.borrow().len(), 3);
        assert_eq!(executor.seen.borrow()[2].args[0], "rm");
    }

    #[test]
    fn remote_podman_is_ssh_plus_podman_not_remote_api() {
        let plan = provision_plan(
            &TargetTemplate::SshPodman {
                ssh: ssh(),
                container: ContainerTemplate {
                    image: "dev:1".to_owned(),
                    extra_run_args: vec![],
                },
            },
            SESSION,
            &bundle(),
            &[],
        )
        .unwrap();
        assert!(plan.commands.iter().all(|command| command.program == "ssh"));
        assert!(
            plan.commands[0]
                .args
                .last()
                .unwrap()
                .contains("'podman' 'run'")
        );
        assert!(plan.commands[0].args.last().unwrap().contains(&format!(
            "'--label' '{SESSION_LABEL}={SESSION}' '--label' '{MANAGED_LABEL}=true'"
        )));
        assert!(
            !plan
                .commands
                .iter()
                .flat_map(|command| &command.args)
                .any(|arg| arg.contains("CONTAINER_HOST") || arg == "--remote")
        );
    }

    #[test]
    fn remote_podman_resource_probe_uses_ssh_and_container_cgroups() {
        let locator = TargetLocator::SshPodman {
            ssh: ssh(),
            container_id: resource_name(SESSION).unwrap(),
        };

        let probe = resource_probe(&locator, SESSION).unwrap();

        assert_eq!(probe.memory.program, "ssh");
        assert!(
            probe
                .memory
                .args
                .last()
                .unwrap()
                .contains("memory.swap.current")
        );
        assert_eq!(probe.disk.program, "ssh");
        assert!(
            probe
                .disk
                .args
                .last()
                .unwrap()
                .contains("'podman' 'container' 'inspect' '--size'")
        );
    }

    #[test]
    fn ec2_resource_probe_reads_host_pressure_and_session_disk() {
        let locator = TargetLocator::AwsEc2 {
            profile: "default".to_owned(),
            region: "us-east-1".to_owned(),
            instance_id: "i-0123456789abcdef0".to_owned(),
            ssh: ssh(),
            workspace: format!(".local/share/hel/workspaces/{SESSION}"),
        };

        let probe = resource_probe(&locator, SESSION).unwrap();

        assert_eq!(probe.memory.program, "ssh");
        assert!(probe.memory.args.last().unwrap().contains("MemAvailable"));
        assert_eq!(probe.disk.program, "ssh");
        assert!(
            probe
                .disk
                .args
                .last()
                .unwrap()
                .contains(&format!(".local/share/hel/workspaces/{SESSION}"))
        );
    }

    #[test]
    fn parses_cgroup_memory_swap_and_writable_disk_usage() {
        let usage = parse_resource_usage(
            b"memory.current=1073741824\nmemory.max=2147483648\nmemory.swap.current=4096\nmemory.swap.max=max\n",
            Some(b"8192\n"),
        )
        .unwrap();

        assert_eq!(usage.memory_current_bytes, 1_073_741_824);
        assert_eq!(usage.memory_limit_bytes, Some(2_147_483_648));
        assert_eq!(usage.swap_current_bytes, Some(4_096));
        assert_eq!(usage.swap_limit_bytes, None);
        assert_eq!(usage.writable_disk_bytes, Some(8_192));
    }

    #[test]
    fn local_path_completion_returns_directory_components_and_tab_prefix() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::create_dir(directory.path().join("data")).unwrap();
        std::fs::create_dir(directory.path().join("dashboard")).unwrap();
        std::fs::write(directory.path().join("data.txt"), "not a directory").unwrap();
        let prefix = format!("{}/da", directory.path().display());

        let matches = local_directory_completions(&prefix);

        assert_eq!(
            matches,
            vec![
                format!("{}/dashboard/", directory.path().display()),
                format!("{}/data/", directory.path().display()),
            ]
        );
        assert_eq!(path_completion(&prefix, &matches), None);
        assert_eq!(
            path_completion("/srv/da", &["/srv/data/".into(), "/srv/database/".into()],),
            Some("/srv/data".into())
        );
        assert_eq!(
            path_completion("/srv/da", &["/srv/data/".into()]),
            Some("/srv/data/".into())
        );
    }

    #[test]
    fn ssh_path_completion_uses_short_timeout_and_fake_executor() {
        let executor = PodmanPreflightExecutor::with_outputs([CommandOutput {
            status: 0,
            stdout: b"/srv/projects/\n/srv/prompts/\n".to_vec(),
            stderr: vec![],
        }]);

        let matches = ssh_directory_completions(&ssh(), "/srv/pr", &executor).unwrap();

        assert_eq!(matches, vec!["/srv/projects/", "/srv/prompts/"]);
        let command = &executor.seen.borrow()[0];
        assert_eq!(command.program, "ssh");
        assert!(command.args.contains(&"ConnectTimeout=3".to_owned()));
        assert!(
            command
                .args
                .last()
                .unwrap()
                .contains("ls -d -- '/srv/pr'*/")
        );
    }

    #[test]
    fn aws_plan_tags_instance_and_close_uses_recorded_id() {
        let template = TargetTemplate::AwsEc2(AwsTemplate {
            profile: "work".to_owned(),
            region: "us-east-2".to_owned(),
            launch_template: "hel-dev".to_owned(),
            launch_template_version: Some("3".to_owned()),
            ssh: ssh(),
        });
        let provision = provision_plan(&template, SESSION, &bundle(), &[]).unwrap();
        assert_eq!(provision.commands.len(), 1);
        assert!(provision.commands[0].args.windows(2).any(|args| args
            == managed_resource_identity_args(ManagedResourceKind::Ec2Instance, SESSION)));
        let close = close_plan(
            &TargetLocator::AwsEc2 {
                profile: "work".to_owned(),
                region: "us-east-2".to_owned(),
                instance_id: "i-0123456789abcdef0".to_owned(),
                ssh: ssh(),
                workspace: format!(".local/share/hel/workspaces/{SESSION}"),
            },
            SESSION,
        )
        .unwrap();
        assert_eq!(
            close.commands[0].args.last().unwrap(),
            "i-0123456789abcdef0"
        );
    }

    #[test]
    fn shell_arguments_are_single_quoted_at_ssh_boundary() {
        let hostile = "repo'; touch /tmp/pwned; echo '";
        let command = ssh_command(&ssh(), ["git", "clone", "--", hostile]);
        assert_eq!(
            command.args.last().unwrap(),
            "'git' 'clone' '--' 'repo'\\''; touch /tmp/pwned; echo '\\'''"
        );
    }

    #[test]
    fn close_rejects_broad_or_mismatched_targets() {
        let broad = TargetLocator::SshBare {
            ssh: ssh(),
            workspace: ".local/share/hel/workspaces".to_owned(),
        };
        assert!(close_plan(&broad, SESSION).is_err());
        let mismatch = TargetLocator::LocalPodman {
            container_id: "hel-someone-abcdef".to_owned(),
        };
        assert!(close_plan(&mismatch, SESSION).is_err());
        let root = TargetLocator::SshBare {
            ssh: ssh(),
            workspace: "/".to_owned(),
        };
        assert!(close_plan(&root, SESSION).is_err());
    }

    #[test]
    fn bundle_rejects_traversal_and_duplicate_destinations() {
        let mut invalid = bundle();
        invalid.repositories[0].destination = "../escape".to_owned();
        assert!(invalid.validate().is_err());
        let mut duplicate = bundle();
        duplicate.repositories[1].destination = "app".to_owned();
        assert!(duplicate.validate().is_err());
    }

    struct FakeExecutor {
        seen: RefCell<Vec<CommandSpec>>,
        fail_at: Option<usize>,
    }

    impl CommandExecutor for FakeExecutor {
        fn execute(&self, command: &CommandSpec) -> Result<CommandOutput> {
            let index = self.seen.borrow().len();
            self.seen.borrow_mut().push(command.clone());
            Ok(CommandOutput {
                status: i32::from(self.fail_at == Some(index)),
                stdout: vec![],
                stderr: b"failure".to_vec(),
            })
        }
    }

    #[test]
    fn executor_stops_at_first_failed_command() {
        let executor = FakeExecutor {
            seen: RefCell::new(vec![]),
            fail_at: Some(1),
        };
        let plan = CommandPlan {
            description: "test".to_owned(),
            commands: vec![
                CommandSpec::new("one", std::iter::empty::<String>()).purpose("one"),
                CommandSpec::new("two", std::iter::empty::<String>()).purpose("two"),
                CommandSpec::new("three", std::iter::empty::<String>()).purpose("three"),
            ],
        };
        assert!(plan.execute(&executor).is_err());
        assert_eq!(executor.seen.borrow().len(), 2);
    }

    #[test]
    fn bootstrap_probes_at_remote_container_boundary() {
        let plan = bootstrap_probe_plan(
            ExecutionBoundary::SshPodman {
                ssh: &ssh(),
                container_id: "abcdef012345",
            },
            HarnessProbe {
                executable: "codex",
                version_args: &["--version"],
                bridge_executable: Some("codex-acp"),
            },
        )
        .unwrap();
        assert_eq!(plan.commands.len(), 3);
        assert!(
            plan.commands[0]
                .args
                .last()
                .unwrap()
                .contains("'podman' 'exec' '-i' 'abcdef012345' 'codex' '--version'")
        );
    }
}
