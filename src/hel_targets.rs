//! Declarative execution plans for Hel session targets.
//!
//! Plans deliberately contain argv vectors instead of local shell strings.  A
//! shell is used only at the SSH boundary, where OpenSSH necessarily sends a
//! command string; every remotely supplied argument is POSIX-quoted there.

use std::collections::BTreeMap;
use std::process::{Command, Stdio};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const SESSION_LABEL: &str = "dev.hel.session";
pub const SESSION_TAG: &str = "dev.hel.session";
pub const CONTAINER_WORKSPACE: &str = "/workspace";

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
    pub url: String,
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
            if repository.url.trim().is_empty() || repository.url.starts_with('-') {
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
) -> Result<CommandPlan> {
    bundle.validate()?;
    let name = resource_name(session_id)?;
    let mut commands = Vec::new();
    match template {
        TargetTemplate::LocalPodman(container) => {
            validate_container_template(container)?;
            commands.push(container_run("podman", container, &name, session_id));
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
            commands.push(container_run("container", container, &name, session_id));
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
            commands.push(
                CommandSpec::new(
                    "aws",
                    [
                        "--profile".to_owned(),
                        aws.profile.clone(),
                        "--region".to_owned(),
                        aws.region.clone(),
                        "ec2".to_owned(),
                        "run-instances".to_owned(),
                        "--launch-template".to_owned(),
                        launch,
                        "--tag-specifications".to_owned(),
                        format!(
                            "ResourceType=instance,Tags=[{{Key={SESSION_TAG},Value={session_id}}}]"
                        ),
                        "--output".to_owned(),
                        "json".to_owned(),
                    ],
                )
                .purpose("launch EC2 session instance"),
            );
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
            run.extend(container_run_args(container, &name, session_id));
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
            container_run(engine, container, &name, smoke_id)
                .purpose("create disposable setup container"),
            container_exec(engine, &name, ["true"]).purpose("execute setup smoke command"),
            CommandSpec::new(engine, ["rm", "--force", &name])
                .purpose("remove disposable setup container"),
        ],
    })
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
        let mut args = vec!["git".to_owned(), "clone".to_owned()];
        if let Some(git_ref) = &repository.git_ref {
            args.extend(["--branch".to_owned(), git_ref.clone()]);
        }
        args.push("--".to_owned());
        args.push(repository.url.clone());
        args.push(format!("{workspace}/{}", repository.destination));
        commands.push(wrap(args).purpose(format!("clone {}", repository.destination)));
    }
    commands
}

fn container_run(
    engine: &str,
    template: &ContainerTemplate,
    name: &str,
    session_id: &str,
) -> CommandSpec {
    CommandSpec::new(engine, container_run_args(template, name, session_id))
        .purpose("start session container")
}

fn container_run_args(template: &ContainerTemplate, name: &str, session_id: &str) -> Vec<String> {
    let mut args = vec![
        "run".to_owned(),
        "--detach".to_owned(),
        "--name".to_owned(),
        name.to_owned(),
        "--label".to_owned(),
        format!("{SESSION_LABEL}={session_id}"),
    ];
    args.extend(template.extra_run_args.clone());
    args.extend([
        template.image.clone(),
        "sleep".to_owned(),
        "infinity".to_owned(),
    ]);
    args
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
    if template
        .extra_run_args
        .iter()
        .any(|arg| arg == "--label" || arg.starts_with(&format!("--label={SESSION_LABEL}")))
    {
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
                    url: "git@github.com:example/app.git".to_owned(),
                    destination: "app".to_owned(),
                    git_ref: Some("main".to_owned()),
                },
                RepositorySpec {
                    url: "https://github.com/example/lib.git".to_owned(),
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

    #[test]
    fn podman_plan_uses_owned_name_label_and_argv_clones() {
        let plan = provision_plan(
            &TargetTemplate::LocalPodman(ContainerTemplate {
                image: "ubuntu:24.04".to_owned(),
                extra_run_args: vec!["--cpus=4".to_owned()],
            }),
            SESSION,
            &bundle(),
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
            plan.commands[0]
                .args
                .contains(&format!("{SESSION_LABEL}={SESSION}"))
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
    fn apple_plan_preflights_and_uses_container_cli() {
        let plan = provision_plan(
            &TargetTemplate::AppleContainer(ContainerTemplate {
                image: "ghcr.io/example/dev:latest".to_owned(),
                extra_run_args: vec![],
            }),
            SESSION,
            &bundle(),
        )
        .unwrap();
        assert_eq!(
            plan.commands[0],
            CommandSpec::new("container", ["system", "status"])
                .purpose("check Apple container service")
        );
        assert_eq!(plan.commands[1].program, "container");
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
        assert!(
            !plan
                .commands
                .iter()
                .flat_map(|command| &command.args)
                .any(|arg| arg.contains("CONTAINER_HOST") || arg == "--remote")
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
        let provision = provision_plan(&template, SESSION, &bundle()).unwrap();
        assert_eq!(provision.commands.len(), 1);
        assert!(
            provision.commands[0]
                .args
                .iter()
                .any(|arg| arg.contains(&format!("Key={SESSION_TAG},Value={SESSION}")))
        );
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
