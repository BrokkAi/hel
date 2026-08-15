//! Declarative execution plans for Hel session targets.
//!
//! Plans deliberately contain argv vectors instead of local shell strings.  A
//! shell is used only at the SSH boundary, where OpenSSH necessarily sends a
//! command string; every remotely supplied argument is POSIX-quoted there.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

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
    pub cpu_percent: Option<u8>,
    pub memory_current_bytes: u64,
    pub memory_limit_bytes: Option<u64>,
    pub swap_current_bytes: Option<u64>,
    pub swap_limit_bytes: Option<u64>,
    pub writable_disk_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionResourceProbe {
    pub memory: CommandSpec,
    pub disk: Option<CommandSpec>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeploymentCapacityKind {
    Host,
    AwsFleet,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentCapacityTarget {
    pub id: String,
    pub host: String,
    pub target_ids: Vec<String>,
    pub kind: DeploymentCapacityKind,
    pub local: bool,
    /// Alternative commands for a host, or one command per live AWS instance.
    pub probes: Vec<CommandSpec>,
    /// Prevents a partial AWS fleet sample when one live instance cannot be probed yet.
    pub probe_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentCapacityUsage {
    pub cpu_percent: Option<u8>,
    pub memory_used_bytes: u64,
    pub memory_total_bytes: u64,
    pub logical_cores: u64,
    pub disk_total_bytes: Option<u64>,
}

/// An additional directory made available to one session.
///
/// Containers use isolated mounts. Remote targets may instead receive a
/// controller-packed snapshot at the destination while retaining this shared
/// persisted shape.
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

    /// Whether the operation supervising this executor has requested
    /// cancellation. Test executors and ordinary process execution are not
    /// cancellable unless they opt in.
    fn cancellation_requested(&self) -> bool {
        false
    }

    fn execute_with_stdin(
        &self,
        _command: &CommandSpec,
        _input: &mut (dyn Read + Send),
    ) -> Result<CommandOutput> {
        bail!("this command executor does not support streamed stdin")
    }
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

    fn execute_with_stdin(
        &self,
        command: &CommandSpec,
        input: &mut (dyn Read + Send),
    ) -> Result<CommandOutput> {
        let mut child = Command::new(&command.program)
            .args(&command.args)
            .envs(&command.env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("run {} for {}", command.program, command.purpose))?;
        let mut stdin = child
            .stdin
            .take()
            .context("streamed command stdin missing")?;
        let mut stdout = child
            .stdout
            .take()
            .context("streamed command stdout missing")?;
        let mut stderr = child
            .stderr
            .take()
            .context("streamed command stderr missing")?;
        let stdout_reader = std::thread::spawn(move || {
            let mut bytes = Vec::new();
            std::io::copy(&mut stdout, &mut bytes).map(|_| bytes)
        });
        let stderr_reader = std::thread::spawn(move || {
            let mut bytes = Vec::new();
            std::io::copy(&mut stderr, &mut bytes).map(|_| bytes)
        });
        let copy = std::io::copy(input, &mut stdin);
        let flush = stdin.flush();
        drop(stdin);
        let status = child
            .wait()
            .with_context(|| format!("wait for {} for {}", command.program, command.purpose))?;
        let stdout = stdout_reader
            .join()
            .map_err(|_| anyhow::anyhow!("streamed command stdout reader panicked"))??;
        let stderr = stderr_reader
            .join()
            .map_err(|_| anyhow::anyhow!("streamed command stderr reader panicked"))??;
        if status.success() {
            copy.context("stream command input")?;
            flush.context("flush command input")?;
        }
        Ok(CommandOutput {
            status: status.code().unwrap_or(-1),
            stdout,
            stderr,
        })
    }
}

#[derive(Clone)]
pub struct CancellableProcessExecutor {
    cancelled: Arc<AtomicBool>,
    deadline: Option<Instant>,
}

impl CancellableProcessExecutor {
    pub fn new(cancelled: Arc<AtomicBool>) -> Self {
        Self {
            cancelled,
            deadline: None,
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
            || self
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
    }

    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            deadline: Some(Instant::now() + timeout),
        }
    }

    fn check_cancelled(&self) -> Result<()> {
        if self.is_cancelled() {
            bail!("operation cancelled");
        }
        Ok(())
    }
}

fn cancellable_command(command: &CommandSpec) -> Command {
    let mut process = Command::new(&command.program);
    process.args(&command.args).envs(&command.env);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;
        process.process_group(0);
    }
    process
}

fn terminate_cancellable_child(child: &mut std::process::Child) {
    #[cfg(unix)]
    // The child owns a fresh process group, so descendants such as an SSH or
    // shell helper cannot keep its output pipes open after cancellation.
    unsafe {
        libc::kill(-(child.id() as i32), libc::SIGKILL);
    }
    #[cfg(not(unix))]
    let _ = child.kill();
    let _ = child.wait();
}

impl CommandExecutor for CancellableProcessExecutor {
    fn cancellation_requested(&self) -> bool {
        self.is_cancelled()
    }

    fn execute(&self, command: &CommandSpec) -> Result<CommandOutput> {
        self.check_cancelled()?;
        let mut child = cancellable_command(command)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("run {} for {}", command.program, command.purpose))?;
        let mut stdout = child.stdout.take().context("command stdout missing")?;
        let mut stderr = child.stderr.take().context("command stderr missing")?;
        let stdout_reader = std::thread::spawn(move || {
            let mut bytes = Vec::new();
            std::io::copy(&mut stdout, &mut bytes).map(|_| bytes)
        });
        let stderr_reader = std::thread::spawn(move || {
            let mut bytes = Vec::new();
            std::io::copy(&mut stderr, &mut bytes).map(|_| bytes)
        });
        let status = loop {
            if self.is_cancelled() {
                terminate_cancellable_child(&mut child);
                let _ = stdout_reader.join();
                let _ = stderr_reader.join();
                bail!("operation cancelled while {}", command.purpose);
            }
            if let Some(status) = child
                .try_wait()
                .with_context(|| format!("wait for {}", command.purpose))?
            {
                break status;
            }
            std::thread::sleep(Duration::from_millis(25));
        };
        let stdout = stdout_reader
            .join()
            .map_err(|_| anyhow::anyhow!("command stdout reader panicked"))??;
        let stderr = stderr_reader
            .join()
            .map_err(|_| anyhow::anyhow!("command stderr reader panicked"))??;
        Ok(CommandOutput {
            status: status.code().unwrap_or(-1),
            stdout,
            stderr,
        })
    }

    fn execute_with_stdin(
        &self,
        command: &CommandSpec,
        input: &mut (dyn Read + Send),
    ) -> Result<CommandOutput> {
        self.check_cancelled()?;
        // Streamed transfers already isolate stdout/stderr reader threads.
        // Checking before each input chunk makes large checkpoint copies
        // cooperatively cancellable without changing the executor interface.
        let mut child = cancellable_command(command)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("run {} for {}", command.program, command.purpose))?;
        let mut stdin = child
            .stdin
            .take()
            .context("streamed command stdin missing")?;
        let mut stdout = child
            .stdout
            .take()
            .context("streamed command stdout missing")?;
        let mut stderr = child
            .stderr
            .take()
            .context("streamed command stderr missing")?;
        let stdout_reader = std::thread::spawn(move || {
            let mut bytes = Vec::new();
            std::io::copy(&mut stdout, &mut bytes).map(|_| bytes)
        });
        let stderr_reader = std::thread::spawn(move || {
            let mut bytes = Vec::new();
            std::io::copy(&mut stderr, &mut bytes).map(|_| bytes)
        });
        let process_result = std::thread::scope(|scope| -> Result<_> {
            // Pipe writes can block forever when a remote helper stops reading.
            // Keep the writer off the supervising thread so cancellation can
            // kill the process group and thereby close the blocked pipe.
            let input_writer = scope.spawn(|| -> Result<()> {
                let mut buffer = [0_u8; 64 * 1024];
                loop {
                    self.check_cancelled()?;
                    let count = input.read(&mut buffer).context("read command input")?;
                    if count == 0 {
                        break;
                    }
                    stdin
                        .write_all(&buffer[..count])
                        .context("stream command input")?;
                }
                stdin.flush().context("flush command input")
            });
            let status = loop {
                if self.is_cancelled() {
                    terminate_cancellable_child(&mut child);
                    let _ = input_writer.join();
                    bail!("operation cancelled while {}", command.purpose);
                }
                match child.try_wait() {
                    Ok(Some(status)) => break status,
                    Ok(None) => std::thread::sleep(Duration::from_millis(25)),
                    Err(error) => {
                        terminate_cancellable_child(&mut child);
                        let _ = input_writer.join();
                        return Err(error).with_context(|| format!("wait for {}", command.purpose));
                    }
                }
            };
            let input_result = input_writer
                .join()
                .map_err(|_| anyhow::anyhow!("streamed command input writer panicked"))?;
            Ok((status, input_result))
        });
        let stdout = stdout_reader
            .join()
            .map_err(|_| anyhow::anyhow!("streamed command stdout reader panicked"))??;
        let stderr = stderr_reader
            .join()
            .map_err(|_| anyhow::anyhow!("streamed command stderr reader panicked"))??;
        let (status, input_result) = process_result?;
        input_result?;
        Ok(CommandOutput {
            status: status.code().unwrap_or(-1),
            stdout,
            stderr,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodmanPreflight {
    pub version: String,
}

/// Where the Podman prerequisite probes run.
///
/// The same postconditions apply locally and over SSH; only the command
/// wrapping and the wording of a failure differ.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PodmanHost<'a> {
    Local,
    Ssh(&'a SshTarget),
}

impl PodmanHost<'_> {
    /// Sentence opener for every failure raised by these probes.
    fn failure(self) -> String {
        match self {
            Self::Local => "Podman preflight failed".to_owned(),
            Self::Ssh(ssh) => format!("Remote Podman preflight failed on {}", ssh.destination),
        }
    }

    /// Prefix that says where a remediation must be applied.
    fn remediation_scope(self) -> String {
        match self {
            Self::Local => String::new(),
            Self::Ssh(ssh) => format!("On {}: ", ssh.destination),
        }
    }

    fn command(self, args: &[&str], purpose: &'static str) -> CommandSpec {
        match self {
            Self::Local => CommandSpec::new(args[0], args[1..].iter().copied()).purpose(purpose),
            Self::Ssh(ssh) => ssh_validation_command(
                ssh,
                args.iter().map(|arg| (*arg).to_owned()).collect(),
                purpose,
            ),
        }
    }
}

/// Verify the fast local preconditions for Hel's rootless Podman target.
///
/// This intentionally never pulls an image. Image availability is verified by
/// `hel setup`'s smoke test and by the subsequent target creation command.
pub fn verify_local_podman(executor: &impl CommandExecutor) -> Result<PodmanPreflight> {
    verify_podman(PodmanHost::Local, executor)
}

/// Verify the same rootless Podman preconditions on an SSH host.
///
/// The probes run through the noninteractive SSH options, so an unreachable
/// host fails fast instead of blocking doctor or session preflight.
pub fn verify_ssh_podman(
    ssh: &SshTarget,
    executor: &impl CommandExecutor,
) -> Result<PodmanPreflight> {
    let host = PodmanHost::Ssh(ssh);
    validate_ssh(ssh).map_err(|error| {
        anyhow::anyhow!(
            "{}: the configured SSH destination is unusable ({error}). Set a valid `host` (and optional `user`) for this ssh-podman target. See {PODMAN_DOCUMENTATION_PATH}.",
            host.failure()
        )
    })?;
    verify_podman(host, executor)
}

fn verify_podman(host: PodmanHost<'_>, executor: &impl CommandExecutor) -> Result<PodmanPreflight> {
    let version = execute_podman_preflight(
        executor,
        host,
        &["podman", "--version"],
        "check Podman version",
        "Postcondition `podman --version` succeeds with Podman 4.0.0 or newer",
        "Install or upgrade Podman: Debian/Ubuntu `sudo apt update && sudo apt install -y podman uidmap`; Fedora `sudo dnf install -y podman shadow-utils`.",
    )?;
    let version = parse_podman_version(host, &version.stdout)?;

    let rootless = execute_podman_preflight(
        executor,
        host,
        &["podman", "info", "--format", "{{.Host.Security.Rootless}}"],
        "check rootless Podman mode",
        "Postcondition `podman info --format '{{.Host.Security.Rootless}}'` prints `true`",
        "Run Hel as the ordinary user without `sudo`; if a remote Podman connection is configured, unset `CONTAINER_HOST` or select the rootless local connection.",
    )?;
    let rootless_output = String::from_utf8_lossy(&rootless.stdout);
    if rootless_output.trim() != "true" {
        bail!(
            "{}: Postcondition `podman info --format '{{{{.Host.Security.Rootless}}}}'` prints `true` returned {:?}. {}Run Hel as the ordinary user without `sudo`; if a remote Podman connection is configured, unset `CONTAINER_HOST` or select the rootless local connection. See {PODMAN_DOCUMENTATION_PATH}.",
            host.failure(),
            rootless_output.trim(),
            host.remediation_scope(),
        );
    }

    let uid_map = execute_podman_preflight(
        executor,
        host,
        &["podman", "unshare", "cat", "/proc/self/uid_map"],
        "check rootless Podman UID map",
        "Postcondition `podman unshare cat /proc/self/uid_map` maps container UIDs 0 and 1",
        "Install UID-map helpers (`sudo apt install -y uidmap` on Debian/Ubuntu or `sudo dnf install -y shadow-utils` on Fedora), then add subordinate ranges with `sudo usermod --add-subuids 100000-165535 --add-subgids 100000-165535 \"$USER\"` and start a fresh login session.",
    )?;
    if !valid_rootless_uid_map(&uid_map.stdout) {
        bail!(
            "{}: Postcondition `podman unshare cat /proc/self/uid_map` maps container UIDs 0 and 1 was not met. {}Add subordinate ranges with `sudo usermod --add-subuids 100000-165535 --add-subgids 100000-165535 \"$USER\"`, verify `/etc/subuid` and `/etc/subgid`, then log out and back in. See {PODMAN_DOCUMENTATION_PATH}.",
            host.failure(),
            host.remediation_scope(),
        );
    }

    Ok(PodmanPreflight { version })
}

fn execute_podman_preflight(
    executor: &impl CommandExecutor,
    host: PodmanHost<'_>,
    args: &[&str],
    purpose: &'static str,
    postcondition: &str,
    remediation: &str,
) -> Result<CommandOutput> {
    let command = host.command(args, purpose);
    let failure = host.failure();
    let scope = host.remediation_scope();
    let output = match executor.execute(&command) {
        Ok(output) => output,
        Err(error) => match ssh_transport_failure(host, &error.to_string()) {
            Some(message) => bail!("{message}"),
            None => bail!(
                "{failure}: {postcondition}. {scope}{remediation} See {PODMAN_DOCUMENTATION_PATH}. Underlying error: {error}"
            ),
        },
    };
    if output.status == SSH_TRANSPORT_EXIT_STATUS
        && let Some(message) =
            ssh_transport_failure(host, String::from_utf8_lossy(&output.stderr).trim())
    {
        bail!("{message}");
    }
    if output.status != 0 {
        bail!(
            "{failure}: {postcondition}. {scope}{remediation} See {PODMAN_DOCUMENTATION_PATH}. Podman reported: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(output)
}

/// `ssh` reserves exit status 255 for its own connection failures; the Podman
/// probes never produce it. Reporting that case separately keeps an
/// unreachable host from being mistaken for a broken Podman installation.
const SSH_TRANSPORT_EXIT_STATUS: i32 = 255;

fn ssh_transport_failure(host: PodmanHost<'_>, reported: &str) -> Option<String> {
    let PodmanHost::Ssh(ssh) = host else {
        return None;
    };
    let destination = &ssh.destination;
    Some(format!(
        "{}: SSH could not run the probes on {destination}. Verify that `ssh {destination}` succeeds noninteractively from this host. See {PODMAN_DOCUMENTATION_PATH}. ssh reported: {reported}",
        host.failure()
    ))
}

fn parse_podman_version(host: PodmanHost<'_>, stdout: &[u8]) -> Result<String> {
    let failure = host.failure();
    let scope = host.remediation_scope();
    let version = String::from_utf8_lossy(stdout).trim().to_owned();
    let Some(candidate) = version
        .split_whitespace()
        .find(|part| part.as_bytes().first().is_some_and(u8::is_ascii_digit))
    else {
        bail!(
            "{failure}: Postcondition `podman --version` succeeds with Podman 4.0.0 or newer returned {version:?}. {scope}Install or upgrade Podman: Debian/Ubuntu `sudo apt update && sudo apt install -y podman uidmap`; Fedora `sudo dnf install -y podman shadow-utils`. See {PODMAN_DOCUMENTATION_PATH}."
        );
    };
    let Some(major) = candidate
        .split('.')
        .next()
        .and_then(|part| part.parse::<u32>().ok())
    else {
        bail!(
            "{failure}: Postcondition `podman --version` succeeds with Podman 4.0.0 or newer returned {version:?}. {scope}Install or upgrade Podman: Debian/Ubuntu `sudo apt update && sudo apt install -y podman uidmap`; Fedora `sudo dnf install -y podman shadow-utils`. See {PODMAN_DOCUMENTATION_PATH}."
        );
    };
    if major < PODMAN_MINIMUM_MAJOR_VERSION {
        bail!(
            "{failure}: Postcondition `podman --version` succeeds with Podman 4.0.0 or newer was not met (found {candidate}). {scope}Upgrade Podman to 4.0.0 or newer: Debian/Ubuntu `sudo apt update && sudo apt install -y podman uidmap`; Fedora `sudo dnf install -y podman shadow-utils`. See {PODMAN_DOCUMENTATION_PATH}."
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
    pub instance_type: Option<String>,
    pub ssh: SshTarget,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TargetTemplate {
    LocalBare,
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
    LocalBare {
        worker_root: String,
    },
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
        TargetTemplate::LocalBare => bail!("local bare projects use their selected directory"),
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
        TargetTemplate::LocalBare => {
            bail!("local bare projects must use the existing-project provisioning path")
        }
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
            if let Some(instance_type) = &aws.instance_type {
                args.extend(["--instance-type".to_owned(), instance_type.clone()]);
            }
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

/// Build the no-op infrastructure plan for an existing bare project.
/// The wizard validates the project for early feedback; worker/ACP startup is
/// authoritative if it changes before launch. Worker state is installed later
/// under the dedicated worker and profile roots, not under a cloned workspace.
pub fn provision_bare_project_plan(
    template: &TargetTemplate,
    session_id: &str,
    project_directory: &str,
) -> Result<CommandPlan> {
    let project = std::path::Path::new(project_directory);
    validate_bare_project_path(project)?;
    match template {
        TargetTemplate::LocalBare => {}
        TargetTemplate::SshBare { ssh, .. } => {
            validate_ssh(ssh)?;
            workspace_for(template, session_id)?;
        }
        _ => bail!("raw project directories require a bare target"),
    }
    Ok(CommandPlan {
        description: format!("provision Hel session {session_id}"),
        commands: Vec::new(),
    })
}

/// Create the short-lived local container used to verify a setup target.
///
/// This deliberately shares the same argv construction as session targets so
/// setup catches an unusable image or runtime before the first session exists.
pub fn setup_smoke_plan(template: &TargetTemplate, smoke_id: &str) -> Result<CommandPlan> {
    let name = resource_name(smoke_id)?;
    let (engine, container, boundary) = match template {
        TargetTemplate::LocalPodman(container) => ("podman", container, ExecutionBoundary::Direct),
        TargetTemplate::AppleContainer(container) => {
            ("container", container, ExecutionBoundary::Direct)
        }
        TargetTemplate::SshPodman { ssh, container } => {
            validate_ssh(ssh)?;
            ("podman", container, ExecutionBoundary::Ssh(ssh))
        }
        _ => bail!("setup smoke tests require a local container or ssh-podman target"),
    };
    validate_container_template(container)?;

    let mut run = vec![engine.to_owned()];
    run.extend(container_run_args(engine, container, &name, smoke_id, &[])?);
    let exec = vec![
        engine.to_owned(),
        "exec".to_owned(),
        "-i".to_owned(),
        name.clone(),
        "true".to_owned(),
    ];
    let remove = vec![
        engine.to_owned(),
        "rm".to_owned(),
        "--force".to_owned(),
        name,
    ];

    Ok(CommandPlan {
        description: format!("smoke test Hel setup target {smoke_id}"),
        commands: vec![
            at_boundary(boundary, run).purpose("create disposable setup container"),
            at_boundary(boundary, exec).purpose("execute setup smoke command"),
            at_boundary(boundary, remove).purpose("remove disposable setup container"),
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
        TargetLocator::LocalBare { .. } => {
            CommandSpec::new(binary, ["worker", "proxy", "--root", root.as_str()])
        }
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
        TargetLocator::LocalBare { .. } => {
            let mut args = args.into_iter();
            let program = args.next().expect("checked non-empty target command");
            CommandSpec::new(program, args)
        }
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

const CGROUP_RESOURCE_USAGE_SCRIPT: &str = r#"
for file in memory.current memory.max memory.swap.current memory.swap.max; do
    path="/sys/fs/cgroup/$file"
    if [ -r "$path" ]; then
        printf "%s=%s\n" "$file" "$(cat "$path")"
    fi
done
if [ -r /sys/fs/cgroup/cpu.stat ]; then
    before=$(awk '/^usage_usec / { print $2 }' /sys/fs/cgroup/cpu.stat)
    sleep 0.25
    after=$(awk '/^usage_usec / { print $2 }' /sys/fs/cgroup/cpu.stat)
    set -- $(cat /sys/fs/cgroup/cpu.max 2>/dev/null || printf 'max 100000')
    if [ "$1" = max ]; then
        cores=$(getconf _NPROCESSORS_ONLN 2>/dev/null || printf '1')
    else
        cores=$(awk -v quota="$1" -v period="$2" 'BEGIN { print quota / period }')
    fi
    awk -v used="$((after - before))" -v cores="$cores" \
        'BEGIN { if (cores > 0) printf "cpu.percent=%.0f\n", used / 250000 / cores * 100 }'
fi
"#;

const HOST_RESOURCE_USAGE_SCRIPT: &str = r#"
read_cpu() { awk '/^cpu / { total=0; for (i=2; i<=NF; i++) total += $i; print total, $5 + $6 }' /proc/stat; }
set -- $(read_cpu); total_before=$1; idle_before=$2
sleep 0.25
set -- $(read_cpu); total_after=$1; idle_after=$2
awk -v total="$((total_after - total_before))" -v idle="$((idle_after - idle_before))" \
    'BEGIN { if (total > 0) printf "cpu.percent=%.0f\n", (total - idle) * 100 / total }'
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
printf 'logical.cores=%s\n' "$(getconf _NPROCESSORS_ONLN 2>/dev/null || nproc)"
"#;

const AWS_ALLOCATED_CAPACITY_SCRIPT: &str = r#"
awk '/^MemTotal:/ { printf "memory.total=%.0f\n", $2 * 1024 }' /proc/meminfo
printf 'logical.cores=%s\n' "$(getconf _NPROCESSORS_ONLN 2>/dev/null || nproc)"
df -B1 -P -- "$1" | awk 'NR == 2 { print "disk.total=" $2 }'
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
                ["sh", "-c", CGROUP_RESOURCE_USAGE_SCRIPT],
            )
            .purpose("sample local Podman container resources"),
            Some(
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
                    CGROUP_RESOURCE_USAGE_SCRIPT,
                ],
            )
            .purpose("sample remote Podman container resources"),
            Some(
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
        ),
        TargetLocator::AwsEc2 { ssh, workspace, .. } => {
            let worker_root = worker_root(locator, session_id)?;
            let profile_root = format!(".local/share/hel/profiles/{session_id}");
            (
                ssh_command(ssh, ["sh", "-c", HOST_RESOURCE_USAGE_SCRIPT])
                    .purpose("sample EC2 session resources"),
                Some(
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
                ),
            )
        }
        TargetLocator::AppleContainer { container_id } => (
            container_exec(
                "container",
                container_id,
                ["sh", "-c", CGROUP_RESOURCE_USAGE_SCRIPT],
            )
            .purpose("sample Apple container resources"),
            None,
        ),
        TargetLocator::LocalBare { .. } | TargetLocator::SshBare { .. } => {
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
    let cpu_percent = values
        .get("cpu.percent")
        .map(|value| parse_percent(value))
        .transpose()?;

    Ok(SessionResourceUsage {
        cpu_percent,
        memory_current_bytes,
        memory_limit_bytes,
        swap_current_bytes,
        swap_limit_bytes,
        writable_disk_bytes,
    })
}

pub fn ssh_host_capacity_command(ssh: &SshTarget) -> CommandSpec {
    ssh_command(ssh, ["sh", "-c", HOST_RESOURCE_USAGE_SCRIPT])
        .purpose("sample deployment host capacity")
}

pub fn aws_allocated_capacity_command(
    locator: &TargetLocator,
    session_id: &str,
) -> Result<CommandSpec> {
    let TargetLocator::AwsEc2 { workspace, .. } = locator else {
        bail!("AWS allocated-capacity probes require an EC2 locator");
    };
    command_on_locator(
        locator,
        session_id,
        vec![
            "sh".into(),
            "-c".into(),
            AWS_ALLOCATED_CAPACITY_SCRIPT.into(),
            "sh".into(),
            workspace.clone(),
        ],
        "sample EC2 allocated capacity",
    )
}

pub fn parse_host_capacity(output: &[u8]) -> Result<DeploymentCapacityUsage> {
    let values = parse_key_values(output);
    let total = parse_required_u64(&values, "memory.max")?;
    Ok(DeploymentCapacityUsage {
        cpu_percent: Some(parse_percent(required_value(&values, "cpu.percent")?)?),
        memory_used_bytes: parse_required_u64(&values, "memory.current")?,
        memory_total_bytes: total,
        logical_cores: parse_required_u64(&values, "logical.cores")?,
        disk_total_bytes: None,
    })
}

pub fn parse_aws_allocated_capacity(output: &[u8]) -> Result<DeploymentCapacityUsage> {
    let values = parse_key_values(output);
    let memory_total_bytes = parse_required_u64(&values, "memory.total")?;
    Ok(DeploymentCapacityUsage {
        cpu_percent: None,
        memory_used_bytes: 0,
        memory_total_bytes,
        logical_cores: parse_required_u64(&values, "logical.cores")?,
        disk_total_bytes: Some(parse_required_u64(&values, "disk.total")?),
    })
}

fn parse_key_values(output: &[u8]) -> BTreeMap<String, String> {
    String::from_utf8_lossy(output)
        .lines()
        .filter_map(|line| line.split_once('='))
        .map(|(key, value)| (key.to_owned(), value.trim().to_owned()))
        .collect()
}

fn required_value<'a>(values: &'a BTreeMap<String, String>, key: &str) -> Result<&'a str> {
    values
        .get(key)
        .map(String::as_str)
        .with_context(|| format!("capacity probe did not expose {key}"))
}

fn parse_required_u64(values: &BTreeMap<String, String>, key: &str) -> Result<u64> {
    required_value(values, key)?
        .parse()
        .with_context(|| format!("capacity probe reported invalid {key}"))
}

fn parse_percent(value: &str) -> Result<u8> {
    let value: f64 = value
        .parse()
        .with_context(|| format!("invalid percentage {value:?}"))?;
    if !value.is_finite() {
        bail!("invalid percentage {value:?}");
    }
    Ok(value.round().clamp(0.0, 100.0) as u8)
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
        TargetLocator::LocalBare { worker_root } => worker_root.clone(),
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
        TargetLocator::LocalBare { .. } => {
            CommandSpec::new("rm", ["-rf", "--", session_worker_root.as_str()])
                .purpose("remove exact local Hel worker state")
        }
        TargetLocator::LocalPodman { container_id } => {
            CommandSpec::new("podman", ["rm", "--force", "--ignore", container_id])
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
        } => {
            // EC2 TerminateInstances is explicitly idempotent, including a
            // repeated request for an already-terminated instance.
            CommandSpec::new(
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
            .purpose("terminate exact EC2 session instance")
        }
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
            ssh_command(ssh, ["podman", "rm", "--force", "--ignore", container_id])
                .purpose("remove exact remote Podman session container")
        }
    };
    Ok(CommandPlan {
        description: format!("close Hel session {session_id}"),
        commands: vec![command],
    })
}

/// Confirm that an Apple container is absent after its exact delete command
/// failed. Other target deletion commands are already idempotent: filesystem
/// removal uses `rm -rf`, Podman uses `--ignore`, and EC2 termination is an
/// idempotent API operation. Apple defines the value passed to `run --name` as
/// the container ID, and `list --quiet` emits those IDs, so this is an exact
/// identity check rather than a display-name comparison.
pub fn cleanup_target_is_confirmed_absent(
    locator: &TargetLocator,
    session_id: &str,
    executor: &impl CommandExecutor,
) -> Result<bool> {
    verify_locator(locator, session_id)?;
    let TargetLocator::AppleContainer { container_id } = locator else {
        return Ok(false);
    };
    let command = CommandSpec::new("container", ["list", "--all", "--quiet"])
        .purpose("confirm exact Apple session container is absent");
    let output = executor.execute(&command)?;
    if output.status != 0 {
        bail!(
            "{} failed with status {}: {}",
            command.purpose,
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let listed = String::from_utf8(output.stdout).context("decode Apple container list")?;
    Ok(!listed.lines().any(|id| id.trim() == container_id))
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

/// Thin Linux Git bootstrap. Managed containers also receive GitHub CLI and
/// its HTTPS credential helper so an injected `GH_TOKEN` works before clone.
pub fn install_git_plan(boundary: ExecutionBoundary<'_>) -> CommandPlan {
    let managed_container = matches!(
        boundary,
        ExecutionBoundary::Container { .. } | ExecutionBoundary::SshPodman { .. }
    );
    let script = if managed_container {
        "set -eu; if ! command -v git >/dev/null 2>&1 || ! command -v gh >/dev/null 2>&1; then SUDO=''; if [ \"$(id -u)\" != 0 ]; then command -v sudo >/dev/null 2>&1 && sudo -n true || { echo 'Git and GitHub CLI installation requires root or passwordless sudo' >&2; exit 1; }; SUDO='sudo -n'; fi; if command -v apt-get >/dev/null 2>&1; then $SUDO apt-get update; $SUDO apt-get install -y git gh ca-certificates curl; elif command -v dnf >/dev/null 2>&1; then $SUDO dnf install -y git gh ca-certificates curl; elif command -v yum >/dev/null 2>&1; then $SUDO yum install -y git gh ca-certificates curl; elif command -v apk >/dev/null 2>&1; then $SUDO apk add --no-cache git github-cli ca-certificates curl; else echo 'Unsupported package manager; install Git and GitHub CLI in the image' >&2; exit 1; fi; fi; git config --global credential.https://github.com.helper '!gh auth git-credential'; git config --global credential.https://gist.github.com.helper '!gh auth git-credential'"
    } else {
        "set -eu; if command -v git >/dev/null 2>&1; then exit 0; fi; SUDO=''; if [ \"$(id -u)\" != 0 ]; then command -v sudo >/dev/null 2>&1 && sudo -n true || { echo 'Git installation requires root or passwordless sudo' >&2; exit 1; }; SUDO='sudo -n'; fi; if command -v apt-get >/dev/null 2>&1; then $SUDO apt-get update; $SUDO apt-get install -y git ca-certificates curl; elif command -v dnf >/dev/null 2>&1; then $SUDO dnf install -y git ca-certificates curl; elif command -v yum >/dev/null 2>&1; then $SUDO yum install -y git ca-certificates curl; elif command -v apk >/dev/null 2>&1; then $SUDO apk add --no-cache git ca-certificates curl; else echo 'Unsupported package manager; install Git manually' >&2; exit 1; fi"
    };
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
    let mut args = vec!["run".to_owned()];
    if engine == "podman" {
        // PID 1 is `sleep infinity`, which reaps nothing, so every exec that
        // outlives its parent leaves a zombie behind. Apple's `container`
        // engine is left alone: its support for the flag is unverified.
        args.push("--init".to_owned());
    }
    args.extend(["--detach".to_owned(), "--name".to_owned(), name.to_owned()]);
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

/// Check whether a directory exists on the configured SSH host.
pub fn ssh_directory_exists(
    ssh: &SshTarget,
    path: &Path,
    executor: &impl CommandExecutor,
) -> Result<bool> {
    let command = ssh_validation_command(
        ssh,
        vec![
            "test".into(),
            "-d".into(),
            path.to_string_lossy().into_owned(),
        ],
        "validate remote directory",
    );
    let output = executor.execute(&command)?;
    match output.status {
        0 => Ok(true),
        1 => Ok(false),
        status => bail!(
            "remote directory check failed with status {status}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ),
    }
}

/// Verify that a bare-SSH project path exists and has a committed Git HEAD.
pub fn validate_bare_project_directory(
    ssh: &SshTarget,
    path: &Path,
    executor: &impl CommandExecutor,
) -> Result<()> {
    validate_bare_project_path(path)?;
    if !ssh_directory_exists(ssh, path, executor)? {
        bail!(
            "remote project directory {} does not exist or is not a directory",
            path.display()
        );
    }
    let output = executor.execute(&ssh_validation_command(
        ssh,
        vec![
            "git".into(),
            "-C".into(),
            path.to_string_lossy().into_owned(),
            "rev-parse".into(),
            "--verify".into(),
            "HEAD".into(),
        ],
        "validate bare SSH Git project",
    ))?;
    if output.status != 0 {
        let detail = String::from_utf8_lossy(&output.stderr);
        let detail = detail.trim();
        if detail.is_empty() {
            bail!(
                "remote project directory {} has no valid Git HEAD",
                path.display()
            );
        }
        bail!(
            "remote project directory {} has no valid Git HEAD: {detail}",
            path.display()
        );
    }
    Ok(())
}

fn validate_bare_project_path(path: &Path) -> Result<()> {
    if !path.is_absolute()
        || path
            .components()
            .any(|part| part == std::path::Component::ParentDir)
    {
        bail!("bare project directory must be an absolute safe path");
    }
    Ok(())
}

fn ssh_validation_command(
    ssh: &SshTarget,
    remote_args: Vec<String>,
    purpose: &'static str,
) -> CommandSpec {
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
        join_remote_command(&remote_args),
    ]);
    CommandSpec::new("ssh", args).purpose(purpose)
}

fn posix_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn verify_locator(locator: &TargetLocator, session_id: &str) -> Result<()> {
    let expected_name = resource_name(session_id)?;
    match locator {
        TargetLocator::LocalBare { worker_root } => {
            let path = Path::new(worker_root);
            if !path.is_absolute()
                || path
                    .components()
                    .any(|part| part == std::path::Component::ParentDir)
                || !path.ends_with(session_id)
            {
                bail!("refusing cleanup: invalid local bare worker root");
            }
        }
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

    #[test]
    fn process_executor_streams_stdin_and_captures_output() {
        let mut input = std::io::Cursor::new(b"streamed input".to_vec());
        let output = ProcessExecutor
            .execute_with_stdin(
                &CommandSpec::new("sh", ["-c", "cat"]).purpose("echo streamed input"),
                &mut input,
            )
            .unwrap();

        assert_eq!(output.status, 0);
        assert_eq!(output.stdout, b"streamed input");
        assert!(output.stderr.is_empty());
    }

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
    fn ssh_podman_preflight_runs_every_probe_through_noninteractive_ssh() {
        let executor = PodmanPreflightExecutor::with_outputs([
            podman_output(b"podman version 5.4.2\n"),
            podman_output(b"true\n"),
            podman_output(b"         0       1000          1\n         1     100000      65536\n"),
        ]);

        let preflight = verify_ssh_podman(&ssh(), &executor).unwrap();

        assert_eq!(preflight.version, "5.4.2");
        let seen = executor.seen.borrow();
        assert_eq!(seen.len(), 3);
        for command in seen.iter() {
            assert_eq!(command.program, "ssh");
            assert!(command.args.contains(&"BatchMode=yes".to_owned()));
            assert!(command.args.contains(&"ConnectTimeout=3".to_owned()));
            assert!(command.args.contains(&"dev@example.test".to_owned()));
            assert!(command.args.last().unwrap().starts_with("'podman'"));
        }
        assert!(
            seen[2]
                .args
                .last()
                .unwrap()
                .contains("'/proc/self/uid_map'")
        );
    }

    #[test]
    fn ssh_podman_preflight_failures_name_the_destination_and_remote_scope() {
        let executor =
            PodmanPreflightExecutor::with_outputs([podman_output(b"podman version 3.4.7\n")]);

        let error = verify_ssh_podman(&ssh(), &executor)
            .unwrap_err()
            .to_string();

        assert!(error.contains("Remote Podman preflight failed on dev@example.test"));
        assert!(error.contains("On dev@example.test: Upgrade Podman"));
        assert!(error.contains(PODMAN_DOCUMENTATION_PATH));
    }

    #[test]
    fn ssh_podman_preflight_reports_an_unreachable_host_separately_from_podman() {
        let executor = PodmanPreflightExecutor::with_outputs([CommandOutput {
            status: 255,
            stdout: vec![],
            stderr: b"ssh: connect to host example.test port 22: Connection timed out".to_vec(),
        }]);

        let error = verify_ssh_podman(&ssh(), &executor)
            .unwrap_err()
            .to_string();

        assert!(error.contains("SSH could not run the probes on dev@example.test"));
        assert!(error.contains("Connection timed out"));
        assert!(!error.contains("Podman 4.0.0"));
    }

    #[test]
    fn ssh_podman_preflight_rejects_an_unusable_destination_without_running_ssh() {
        let executor = PodmanPreflightExecutor::with_outputs([]);
        let target = SshTarget {
            destination: "--oProxyCommand=touch /tmp/pwn".to_owned(),
            ssh_args: vec![],
        };

        let error = verify_ssh_podman(&target, &executor)
            .unwrap_err()
            .to_string();

        assert!(error.contains("SSH destination is unusable"));
        assert!(executor.seen.borrow().is_empty());
    }

    #[test]
    fn setup_smoke_plan_wraps_every_ssh_podman_command_in_ssh() {
        let plan = setup_smoke_plan(
            &TargetTemplate::SshPodman {
                ssh: ssh(),
                container: ContainerTemplate {
                    image: "ubuntu:24.04".to_owned(),
                    extra_run_args: vec![],
                },
            },
            "setup-123",
        )
        .unwrap();

        assert_eq!(plan.commands.len(), 3);
        for command in &plan.commands {
            assert_eq!(command.program, "ssh");
            assert!(command.args.contains(&"dev@example.test".to_owned()));
            assert!(command.args.last().unwrap().starts_with("'podman'"));
        }
        assert!(
            plan.commands[0]
                .args
                .last()
                .unwrap()
                .contains("'run' '--init'")
        );
        assert!(plan.commands[1].args.last().unwrap().ends_with("'true'"));
        assert!(
            plan.commands[2]
                .args
                .last()
                .unwrap()
                .contains("'rm' '--force'")
        );
        assert_eq!(
            plan.commands[2].purpose,
            "remove disposable setup container"
        );
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
        assert!(bootstrap.args.last().unwrap().contains("command -v gh"));
        assert!(
            bootstrap
                .args
                .last()
                .unwrap()
                .contains("gh auth git-credential")
        );
    }

    #[test]
    fn podman_containers_reap_zombies_and_apple_containers_keep_their_defaults() {
        let podman = provision_plan(
            &TargetTemplate::LocalPodman(ContainerTemplate {
                image: "ubuntu:24.04".to_owned(),
                extra_run_args: vec![],
            }),
            SESSION,
            &bundle(),
            &[],
        )
        .unwrap();
        assert_eq!(podman.commands[0].args[0], "run");
        assert_eq!(podman.commands[0].args[1], "--init");

        let remote = provision_plan(
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
        assert!(
            remote.commands[0]
                .args
                .last()
                .unwrap()
                .contains("'podman' 'run' '--init'")
        );

        let apple = provision_plan(
            &TargetTemplate::AppleContainer(ContainerTemplate {
                image: "ubuntu:24.04".to_owned(),
                extra_run_args: vec![],
            }),
            SESSION,
            &bundle(),
            &[],
        )
        .unwrap();
        assert_eq!(apple.commands[1].args[0], "run");
        assert!(!apple.commands[1].args.contains(&"--init".to_owned()));
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
        let name = resource_name(SESSION).unwrap();
        assert!(
            plan.commands[1]
                .args
                .windows(2)
                .any(|args| args == ["--name", &name])
        );
        assert!(plan.commands[1].args.windows(4).any(|args| {
            args == managed_resource_identity_args(ManagedResourceKind::Container, SESSION)
        }));
    }

    #[test]
    fn apple_cleanup_confirms_absence_by_the_exact_provisioned_container_id() {
        let container_id = resource_name(SESSION).unwrap();
        let locator = TargetLocator::AppleContainer {
            container_id: container_id.clone(),
        };
        let still_live = PodmanPreflightExecutor::with_outputs([podman_output(format!(
            "unrelated-id\n{container_id}\n"
        ))]);

        assert!(!cleanup_target_is_confirmed_absent(&locator, SESSION, &still_live).unwrap());
        assert_eq!(
            still_live.seen.borrow()[0].args,
            ["list", "--all", "--quiet"]
        );

        let absent = PodmanPreflightExecutor::with_outputs([podman_output("unrelated-id\n")]);
        assert!(cleanup_target_is_confirmed_absent(&locator, SESSION, &absent).unwrap());

        let failed = PodmanPreflightExecutor::with_outputs([CommandOutput {
            status: 1,
            stdout: Vec::new(),
            stderr: b"service unavailable".to_vec(),
        }]);
        assert!(cleanup_target_is_confirmed_absent(&locator, SESSION, &failed).is_err());
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
        assert_eq!(probe.disk.as_ref().unwrap().program, "ssh");
        assert!(
            probe
                .disk
                .as_ref()
                .unwrap()
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
        assert_eq!(probe.disk.as_ref().unwrap().program, "ssh");
        assert!(
            probe
                .disk
                .as_ref()
                .unwrap()
                .args
                .last()
                .unwrap()
                .contains(&format!(".local/share/hel/workspaces/{SESSION}"))
        );
    }

    #[test]
    fn parses_cgroup_memory_swap_and_writable_disk_usage() {
        let usage = parse_resource_usage(
            b"cpu.percent=37.4\nmemory.current=1073741824\nmemory.max=2147483648\nmemory.swap.current=4096\nmemory.swap.max=max\n",
            Some(b"8192\n"),
        )
        .unwrap();

        assert_eq!(usage.cpu_percent, Some(37));
        assert_eq!(usage.memory_current_bytes, 1_073_741_824);
        assert_eq!(usage.memory_limit_bytes, Some(2_147_483_648));
        assert_eq!(usage.swap_current_bytes, Some(4_096));
        assert_eq!(usage.swap_limit_bytes, None);
        assert_eq!(usage.writable_disk_bytes, Some(8_192));
    }

    #[test]
    fn parses_host_and_aws_capacity_outputs() {
        let host = parse_host_capacity(
            b"cpu.percent=62.6\nmemory.current=300\nmemory.max=1000\nlogical.cores=8\n",
        )
        .unwrap();
        assert_eq!(host.cpu_percent, Some(63));
        assert_eq!(host.memory_used_bytes, 300);
        assert_eq!(host.memory_total_bytes, 1_000);
        assert_eq!(host.logical_cores, 8);

        let aws = parse_aws_allocated_capacity(
            b"memory.total=34359738368\nlogical.cores=16\ndisk.total=214748364800\n",
        )
        .unwrap();
        assert_eq!(aws.cpu_percent, None);
        assert_eq!(aws.memory_total_bytes, 34_359_738_368);
        assert_eq!(aws.logical_cores, 16);
        assert_eq!(aws.disk_total_bytes, Some(214_748_364_800));

        assert!(parse_host_capacity(b"cpu.percent=nan\n").is_err());
        assert!(parse_aws_allocated_capacity(b"memory.total=nope\n").is_err());
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
    fn ssh_directory_check_quotes_the_source_and_distinguishes_missing() {
        let exists = FakeExecutor {
            seen: RefCell::new(vec![]),
            fail_at: None,
        };
        assert!(ssh_directory_exists(&ssh(), Path::new("/srv/user's data"), &exists).unwrap());
        let command = &exists.seen.borrow()[0];
        assert_eq!(command.program, "ssh");
        let remote_command = command.args.last().unwrap();
        assert!(remote_command.starts_with("'test' '-d' "));
        assert!(remote_command.contains("'/srv/user'\\''s data'"));

        let missing = FakeExecutor {
            seen: RefCell::new(vec![]),
            fail_at: Some(0),
        };
        assert!(!ssh_directory_exists(&ssh(), Path::new("/missing"), &missing).unwrap());
    }

    #[test]
    fn bare_project_validation_checks_directory_and_git_repository() {
        let valid = FakeExecutor {
            seen: RefCell::new(vec![]),
            fail_at: None,
        };
        validate_bare_project_directory(&ssh(), Path::new("/srv/project"), &valid).unwrap();
        let seen = valid.seen.borrow();
        assert_eq!(seen.len(), 2);
        assert!(
            seen[0]
                .args
                .last()
                .unwrap()
                .contains("'test' '-d' '/srv/project'")
        );
        assert!(
            seen[1]
                .args
                .last()
                .unwrap()
                .contains("'git' '-C' '/srv/project' 'rev-parse' '--verify' 'HEAD'")
        );
        assert!(seen[0].args.contains(&"ConnectTimeout=3".to_owned()));

        let missing = FakeExecutor {
            seen: RefCell::new(vec![]),
            fail_at: Some(0),
        };
        let error =
            validate_bare_project_directory(&ssh(), Path::new("/missing"), &missing).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("does not exist or is not a directory")
        );
        assert_eq!(missing.seen.borrow().len(), 1);

        let not_git = FakeExecutor {
            seen: RefCell::new(vec![]),
            fail_at: Some(1),
        };
        let error =
            validate_bare_project_directory(&ssh(), Path::new("/srv/plain"), &not_git).unwrap_err();
        assert!(error.to_string().contains("has no valid Git HEAD"));
        assert_eq!(not_git.seen.borrow().len(), 2);
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
            instance_type: Some("m8i-flex.2xlarge".into()),
            ssh: ssh(),
        });
        let provision = provision_plan(&template, SESSION, &bundle(), &[]).unwrap();
        assert_eq!(provision.commands.len(), 1);
        assert!(provision.commands[0].args.windows(2).any(|args| args
            == managed_resource_identity_args(ManagedResourceKind::Ec2Instance, SESSION)));
        assert!(
            provision.commands[0]
                .args
                .windows(2)
                .any(|args| { args == ["--instance-type", "m8i-flex.2xlarge"] })
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
    fn bare_project_plan_leaves_project_validation_to_dialog_and_launch() {
        let local =
            provision_bare_project_plan(&TargetTemplate::LocalBare, SESSION, "/home/me/project")
                .unwrap();
        assert!(local.commands.is_empty());

        let template = TargetTemplate::SshBare {
            ssh: ssh(),
            workspace_prefix: ".local/share/hel/workspaces".into(),
        };
        let provision = provision_bare_project_plan(&template, SESSION, "/srv/project").unwrap();
        let commands = provision
            .commands
            .iter()
            .map(|command| command.args.last().unwrap().as_str())
            .collect::<Vec<_>>();
        assert!(commands.is_empty());

        let locator = TargetLocator::SshBare {
            ssh: ssh(),
            workspace: format!(".local/share/hel/workspaces/{SESSION}"),
        };
        let close = close_plan(&locator, SESSION).unwrap();
        assert!(
            !close.commands[0]
                .args
                .last()
                .unwrap()
                .contains("/srv/project")
        );
    }

    #[test]
    fn local_bare_worker_commands_are_direct_and_cleanup_is_exact() {
        let worker_root = format!("/var/lib/hel/workers/{SESSION}");
        let locator = TargetLocator::LocalBare {
            worker_root: worker_root.clone(),
        };

        let reconnect = reconnect_plan(&locator, SESSION).unwrap();
        assert_eq!(reconnect.commands[0].program, format!("{worker_root}/hel"));
        assert_eq!(
            reconnect.commands[0].args,
            ["worker", "proxy", "--root", worker_root.as_str()]
        );
        let close = close_plan(&locator, SESSION).unwrap();
        assert_eq!(close.commands[0].program, "rm");
        assert_eq!(close.commands[0].args, ["-rf", "--", worker_root.as_str()]);
    }

    #[test]
    fn podman_cleanup_ignores_an_already_absent_container() {
        let name = resource_name(SESSION).unwrap();
        let local = close_plan(
            &TargetLocator::LocalPodman {
                container_id: name.clone(),
            },
            SESSION,
        )
        .unwrap();
        assert_eq!(
            local.commands[0].args,
            ["rm", "--force", "--ignore", name.as_str()]
        );

        let remote = close_plan(
            &TargetLocator::SshPodman {
                ssh: ssh(),
                container_id: name,
            },
            SESSION,
        )
        .unwrap();
        assert!(
            remote.commands[0]
                .args
                .last()
                .unwrap()
                .contains("'podman' 'rm' '--force' '--ignore'")
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
    fn cancellable_executor_terminates_a_running_process() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let executor = CancellableProcessExecutor::new(cancelled.clone());
        let cancel = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            cancelled.store(true, Ordering::Release);
        });
        let started = std::time::Instant::now();
        let error = executor
            .execute(
                &CommandSpec::new("sh", ["-c", "sleep 30"]).purpose("test cancellable process"),
            )
            .unwrap_err();
        cancel.join().unwrap();
        assert!(error.to_string().contains("operation cancelled"));
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn cancellable_executor_enforces_its_inline_deadline() {
        let executor = CancellableProcessExecutor::with_timeout(Duration::from_millis(50));
        let started = std::time::Instant::now();

        let error = executor
            .execute(
                &CommandSpec::new("sh", ["-c", "sleep 30"])
                    .purpose("test bounded process execution"),
            )
            .unwrap_err();

        assert!(error.to_string().contains("operation cancelled"));
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn cancellable_executor_interrupts_a_blocked_stdin_pipe() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let executor = CancellableProcessExecutor::new(cancelled.clone());
        let cancel = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            cancelled.store(true, Ordering::Release);
        });
        let mut input = std::io::Cursor::new(vec![0_u8; 16 * 1024 * 1024]);
        let started = std::time::Instant::now();

        let error = executor
            .execute_with_stdin(
                &CommandSpec::new("sh", ["-c", "sleep 30"])
                    .purpose("test cancellation while streaming"),
                &mut input,
            )
            .unwrap_err();

        cancel.join().unwrap();
        assert!(error.to_string().contains("operation cancelled"));
        assert!(started.elapsed() < Duration::from_secs(2));
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
