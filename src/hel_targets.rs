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

use anyhow::{Context, Result, bail, ensure};
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

/// The launch phase a command belongs to, reported as launch progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProvisionStage {
    Provisioning,
    Booting,
    Syncing,
    Starting,
    Compacting,
}

impl ProvisionStage {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Provisioning => "Provision",
            Self::Booting => "Boot",
            Self::Syncing => "Sync",
            Self::Starting => "Start",
            Self::Compacting => "Compact",
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
struct SensitiveCommandInput(Vec<u8>);

impl std::fmt::Debug for SensitiveCommandInput {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("<redacted>")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandSpec {
    pub program: String,
    pub args: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    pub purpose: String,
    #[serde(default)]
    pub stage: Option<ProvisionStage>,
    /// Commands that share this marker and appear consecutively in a plan's
    /// command list may run concurrently under
    /// [`CommandPlan::execute_concurrent`]. Commands without a marker, or
    /// whose neighbors do not share it, keep running strictly in plan order.
    #[serde(default)]
    pub parallel_group: Option<u32>,
    /// Whether this command brings the session's target into existence. Every
    /// command after it in a provisioning plan runs against a target that
    /// already exists, so a later failure owes that target's teardown.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub creates_target: bool,
    /// Input that must reach the child without becoming part of its arguments,
    /// environment, serialized plan, or debug representation.
    #[serde(skip)]
    sensitive_stdin: Option<SensitiveCommandInput>,
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
            stage: None,
            parallel_group: None,
            creates_target: false,
            sensitive_stdin: None,
        }
    }

    pub fn purpose(mut self, purpose: impl Into<String>) -> Self {
        self.purpose = purpose.into();
        self
    }

    pub fn stage(mut self, stage: ProvisionStage) -> Self {
        self.stage = Some(stage);
        self
    }

    /// Mark this command as eligible to run concurrently with its
    /// plan-adjacent siblings that share the same group.
    pub fn parallel_group(mut self, group: u32) -> Self {
        self.parallel_group = Some(group);
        self
    }

    /// Mark this command as the one that creates the session's target.
    pub fn creates_target(mut self) -> Self {
        self.creates_target = true;
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
    /// Attach the source read-only instead of behind Podman's copy-on-write
    /// overlay. Defaults to false so archives and records written before the
    /// option existed keep the overlay they were provisioned with.
    #[serde(default)]
    pub read_only: bool,
}

/// Why a filesystem cannot host Podman's `:O` copy-on-write overlay, or `None`
/// when it can. Unknown types are allowed: the overlay is the better mount and
/// only a filesystem known to break it is downgraded.
///
/// The names are those `stat -f -c %T` reports, matched case-insensitively.
pub fn overlay_unsupported_filesystem(filesystem: &str) -> Option<&'static str> {
    let name = filesystem.trim().to_ascii_lowercase();
    // FUSE reports the backing driver as `fuse.sshfs`, `fuse.s3fs`, and so on.
    if name == "fuse" || name == "fuseblk" || name.starts_with("fuse.") {
        return Some("FUSE filesystem");
    }
    match name.as_str() {
        "nfs" | "nfs4" | "cifs" | "smb2" | "smb3" | "9p" | "v9fs" | "virtiofs" | "ceph"
        | "lustre" | "afs" | "glusterfs" | "ocfs2" | "gfs" | "gfs2" => Some("network filesystem"),
        "msdos" | "vfat" | "fat" | "exfat" | "ntfs" | "ntfs3" => Some("no POSIX metadata"),
        "overlayfs" => Some("overlay stacking limit"),
        _ => None,
    }
}

/// Filesystem type of each directory, probed on the host that runs the
/// container engine. `ssh` names that host for a remote Podman target; `None`
/// probes this machine.
///
/// The reply is positional, so the whole batch fails unless `stat` answered for
/// every directory in order.
pub fn probe_filesystem_types(
    ssh: Option<&SshTarget>,
    paths: &[PathBuf],
    executor: &impl CommandExecutor,
) -> Result<Vec<String>> {
    if paths.is_empty() {
        return Ok(Vec::new());
    }
    let mut args = vec![
        "stat".to_owned(),
        "-f".to_owned(),
        "-c".to_owned(),
        "%T".to_owned(),
        "--".to_owned(),
    ];
    args.extend(paths.iter().map(|path| path.to_string_lossy().into_owned()));
    let host = match ssh {
        Some(ssh) => PodmanHost::Ssh(ssh),
        None => PodmanHost::Local,
    };
    let output = executor.execute(&host.command_owned(args, "probe mount source filesystem"))?;
    if output.status != 0 {
        bail!(
            "filesystem probe failed with status {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let types = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|line| line.trim().to_owned())
        .collect::<Vec<_>>();
    if types.len() != paths.len() {
        bail!(
            "filesystem probe named {} filesystems for {} directories",
            types.len(),
            paths.len()
        );
    }
    Ok(types)
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

    /// Report a lifecycle stage for work that is not a command execution, so
    /// long in-process phases still name themselves on the session clock.
    fn notify_stage(&self, _stage: ProvisionStage) {}

    /// Report a decision an operation made on the user's behalf. This is not a
    /// failure: the work continues, and the user is told what changed.
    fn notify_notice(&self, _notice: &str) {}

    fn execute_with_stdin(
        &self,
        _command: &CommandSpec,
        _input: &mut (dyn Read + Send),
    ) -> Result<CommandOutput> {
        bail!("this command executor does not support streamed stdin")
    }
}

pub struct ProcessExecutor;

/// One debug line per finished target command, so a slow launch or resume
/// phase can be attributed from logs instead of re-profiled by hand.
fn trace_command_duration(command: &CommandSpec, started: Instant, status: i32) {
    tracing::debug!(
        purpose = command.purpose.as_str(),
        program = command.program.as_str(),
        status,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "target command finished"
    );
}

impl CommandExecutor for ProcessExecutor {
    fn execute(&self, command: &CommandSpec) -> Result<CommandOutput> {
        if let Some(input) = &command.sensitive_stdin {
            let mut input = std::io::Cursor::new(input.0.as_slice());
            return self.execute_with_stdin(command, &mut input);
        }
        let started = Instant::now();
        let output = Command::new(&command.program)
            .args(&command.args)
            .envs(&command.env)
            .stdin(Stdio::null())
            .output()
            .with_context(|| format!("run {} for {}", command.program, command.purpose))?;
        let status = output.status.code().unwrap_or(-1);
        trace_command_duration(command, started, status);
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
        let mut process = Command::new(&command.program);
        process.args(&command.args).envs(&command.env);
        // Plain process execution is not cancellable, so the transfer only
        // ends when the child does.
        stream_command_with_stdin(process, command, input, &|| false)
    }
}

/// Streams `input` into a freshly spawned child and collects its output.
///
/// Both executors share this one implementation because the pipe edge cases
/// below are easy to get subtly wrong in a second copy.
///
/// `is_cancelled` reports whether the supervising operation wants the transfer
/// abandoned; [`ProcessExecutor`] passes a check that is never true, which also
/// makes the kill path below unreachable for it.
fn stream_command_with_stdin(
    mut process: Command,
    command: &CommandSpec,
    input: &mut (dyn Read + Send),
    is_cancelled: &(dyn Fn() -> bool + Sync),
) -> Result<CommandOutput> {
    let started = Instant::now();
    if is_cancelled() {
        bail!("operation cancelled");
    }
    let mut child = process
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("run {} for {}", command.program, command.purpose))?;
    let stdin = child
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
    // Reader threads keep the child's output pipes drained; a child that fills
    // one while nobody reads would block instead of exiting.
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
        // Keep the writer off the supervising thread so cancellation can kill
        // the process group and thereby close the blocked pipe.
        let input_writer = scope.spawn(move || -> Result<()> {
            // Owning `stdin` here is what closes the pipe's write end once the
            // transfer finishes. A child that reads to EOF, such as
            // `hel worker export-checkpoint --spec -`, never exits while any
            // copy of the write end is still open.
            let mut stdin = stdin;
            let mut buffer = [0_u8; 64 * 1024];
            loop {
                // Checking before each chunk makes large checkpoint copies
                // cooperatively cancellable without changing the executor
                // interface.
                if is_cancelled() {
                    bail!("operation cancelled");
                }
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
            if is_cancelled() {
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
    if status.success() {
        // A child that exited first explains the failure through its own
        // status and stderr; the broken pipe that exit caused would only hide
        // it. A successful child must not hide an input error.
        input_result?;
    }
    let status = status.code().unwrap_or(-1);
    trace_command_duration(command, started, status);
    Ok(CommandOutput {
        status,
        stdout,
        stderr,
    })
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

    /// Bounds an existing flag-based executor with a deadline, so a wedged
    /// child becomes a reported failure instead of running forever.
    pub fn with_deadline(mut self, timeout: Duration) -> Self {
        self.deadline = Some(Instant::now() + timeout);
        self
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
        if let Some(input) = &command.sensitive_stdin {
            let mut input = std::io::Cursor::new(input.0.as_slice());
            return self.execute_with_stdin(command, &mut input);
        }
        let started = Instant::now();
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
        let status = status.code().unwrap_or(-1);
        trace_command_duration(command, started, status);
        Ok(CommandOutput {
            status,
            stdout,
            stderr,
        })
    }

    fn execute_with_stdin(
        &self,
        command: &CommandSpec,
        input: &mut (dyn Read + Send),
    ) -> Result<CommandOutput> {
        // The child runs in its own process group so cancellation can kill the
        // whole group, which is what releases a writer blocked on a full pipe.
        stream_command_with_stdin(cancellable_command(command), command, input, &|| {
            self.is_cancelled()
        })
    }
}

/// Runs every command with its own deadline.
///
/// [`CancellableProcessExecutor::with_timeout`] bounds a whole operation from a
/// single shared deadline, which suits one provisioning run. Prerequisite
/// probes are different: each one is expected to answer quickly, and a wedged
/// socket or blackholed network must not stall the probes that follow it. A
/// timeout here names the probe that hung, so the caller can report it the same
/// way it reports any other probe failure.
#[derive(Debug, Clone, Copy)]
pub struct BoundedProcessExecutor {
    timeout: Duration,
}

impl BoundedProcessExecutor {
    pub const fn new(timeout: Duration) -> Self {
        Self { timeout }
    }
}

impl CommandExecutor for BoundedProcessExecutor {
    fn execute(&self, command: &CommandSpec) -> Result<CommandOutput> {
        let executor = CancellableProcessExecutor::with_timeout(self.timeout);
        executor.execute(command).map_err(|error| {
            if executor.is_cancelled() {
                anyhow::anyhow!(
                    "`{}` did not answer within {} seconds while trying to {}",
                    command.program,
                    self.timeout.as_secs(),
                    command.purpose
                )
            } else {
                error
            }
        })
    }

    fn execute_with_stdin(
        &self,
        command: &CommandSpec,
        input: &mut (dyn Read + Send),
    ) -> Result<CommandOutput> {
        CancellableProcessExecutor::with_timeout(self.timeout).execute_with_stdin(command, input)
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
        self.command_owned(args.iter().map(|arg| (*arg).to_owned()).collect(), purpose)
    }

    fn command_owned(self, args: Vec<String>, purpose: &'static str) -> CommandSpec {
        match self {
            Self::Local => {
                CommandSpec::new(args[0].clone(), args[1..].iter().cloned()).purpose(purpose)
            }
            Self::Ssh(ssh) => ssh_validation_command(ssh, args, purpose),
        }
        .stage(ProvisionStage::Provisioning)
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
    /// Supply one container environment value without placing it in the
    /// Podman/SSH argument vector. The target launcher reads the value from
    /// stdin, exports it, and asks the container engine to inherit it by name.
    pub fn provide_target_environment_secret(
        &mut self,
        target: &TargetTemplate,
        name: &str,
        value: &str,
    ) -> Result<()> {
        ensure!(
            !name.is_empty()
                && name.bytes().enumerate().all(|(index, byte)| byte == b'_'
                    || byte.is_ascii_alphabetic()
                    || (index > 0 && byte.is_ascii_digit())),
            "invalid secret environment variable name"
        );
        ensure!(
            !value.as_bytes().contains(&b'\n') && !value.as_bytes().contains(&b'\r'),
            "secret environment value cannot contain a newline"
        );
        let command = self
            .commands
            .iter_mut()
            .find(|command| command.creates_target)
            .context("provisioning plan has no target creation command")?;
        let read_and_export = format!("IFS= read -r {name} || exit 1; export {name};");
        match target {
            TargetTemplate::LocalPodman(_) | TargetTemplate::AppleContainer(_) => {
                let program = std::mem::replace(&mut command.program, "sh".to_owned());
                let args = std::mem::take(&mut command.args);
                command.args = vec![
                    "-c".to_owned(),
                    format!("{read_and_export} exec \"$@\""),
                    "hel-secret-env".to_owned(),
                    program,
                ];
                command.args.extend(args);
            }
            TargetTemplate::SshPodman { .. } => {
                let remote = command
                    .args
                    .last_mut()
                    .context("remote Podman command has no SSH command argument")?;
                *remote = format!("{read_and_export} exec {remote}");
            }
            TargetTemplate::LocalBare
            | TargetTemplate::AwsEc2(_)
            | TargetTemplate::SshBare { .. } => {
                bail!("target does not support inherited container environment")
            }
        }
        let mut input = value.as_bytes().to_vec();
        input.push(b'\n');
        command.sensitive_stdin = Some(SensitiveCommandInput(input));
        Ok(())
    }

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

    /// Execute the plan the same way [`Self::execute`] does, except that
    /// commands sharing a [`CommandSpec::parallel_group`] marker and
    /// appearing consecutively in `commands` run concurrently as one batch.
    ///
    /// A batch starts only once every earlier command has succeeded, and a
    /// batch that fails reports the first failure in plan order regardless
    /// of which command finished first — the same fail-fast contract
    /// [`Self::execute`] provides between individual commands. This method
    /// requires a `Sync` executor because a batch shares it across threads;
    /// [`Self::execute`] keeps working with non-`Sync` executors such as
    /// test fakes built on `RefCell`.
    pub fn execute_concurrent(
        &self,
        executor: &(impl CommandExecutor + Sync),
    ) -> Result<Vec<CommandOutput>> {
        let mut outputs = Vec::with_capacity(self.commands.len());
        let mut index = 0;
        while index < self.commands.len() {
            let group = self.commands[index].parallel_group;
            let mut end = index + 1;
            if group.is_some() {
                while end < self.commands.len() && self.commands[end].parallel_group == group {
                    end += 1;
                }
            }
            let batch = &self.commands[index..end];
            if let [command] = batch {
                outputs.push(checked_command_output(command, executor.execute(command)?)?);
            } else {
                let results: Vec<Result<CommandOutput>> = std::thread::scope(|scope| {
                    let handles: Vec<_> = batch
                        .iter()
                        .map(|command| scope.spawn(|| executor.execute(command)))
                        .collect();
                    handles
                        .into_iter()
                        .map(|handle| match handle.join() {
                            Ok(result) => result,
                            Err(panic) => Err(anyhow::anyhow!(
                                "concurrent command thread panicked: {}",
                                command_thread_panic_message(panic.as_ref())
                            )),
                        })
                        .collect()
                });
                for (command, result) in batch.iter().zip(results) {
                    outputs.push(checked_command_output(command, result?)?);
                }
            }
            index = end;
        }
        Ok(outputs)
    }

    /// Split the plan around the command that creates the session's target:
    /// the commands through that one, then the commands that run against a
    /// target which already exists.
    ///
    /// A plan that creates nothing — an existing project directory, say —
    /// splits into nothing, so a caller never arms a teardown for a target it
    /// did not bring into existence.
    pub fn split_at_target_creation(&self) -> Option<(Self, Self)> {
        let created = self
            .commands
            .iter()
            .position(|command| command.creates_target)?;
        let (creation, remainder) = self.commands.split_at(created + 1);
        Some((
            Self {
                description: self.description.clone(),
                commands: creation.to_vec(),
            },
            Self {
                description: self.description.clone(),
                commands: remainder.to_vec(),
            },
        ))
    }
}

/// Fail the same way [`CommandPlan::execute`] does for a non-zero exit
/// status; kept as a shared helper so [`CommandPlan::execute_concurrent`]
/// reports identical error text.
fn checked_command_output(command: &CommandSpec, output: CommandOutput) -> Result<CommandOutput> {
    if output.status != 0 {
        bail!(
            "{} failed with status {}: {}",
            command.purpose,
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(output)
}

/// Describe a spawned command thread's panic payload for error context.
pub(crate) fn command_thread_panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_owned()
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

/// Commands and identity needed to bring a stopped managed target back online.
/// Only runtimes whose stopped resources retain their durable files provide
/// one; callers leave every other target kind alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetRecoveryPlan {
    pub inspect: CommandSpec,
    pub start: CommandSpec,
    pub session_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetRecoveryOutcome {
    NotRequired,
    AlreadyRunning,
    Started,
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
            // Interpret a leading "~/" as home-relative. Remote commands are
            // single-quoted, so a literal tilde would name a directory called
            // "~"; a relative path resolves against the login home for ssh
            // and scp alike.
            let prefix = workspace_prefix
                .strip_prefix("~/")
                .unwrap_or(workspace_prefix);
            Ok(format!("{}/{session_id}", prefix.trim_end_matches('/')))
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
                    .purpose("check Apple container service")
                    .stage(ProvisionStage::Provisioning),
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
            commands.push(
                CommandSpec::new("aws", args)
                    .purpose("launch EC2 session instance")
                    .stage(ProvisionStage::Provisioning)
                    .creates_target(),
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
                    .purpose("create SSH session workspace")
                    .stage(ProvisionStage::Provisioning)
                    .creates_target(),
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
            commands.push(
                ssh_command_owned(ssh, run)
                    .purpose("start remote Podman container")
                    .stage(ProvisionStage::Provisioning)
                    .creates_target(),
            );
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
    let mut commands = vec![
        ssh_command(ssh, ["mkdir", "-p", workspace])
            .purpose("create EC2 session workspace")
            .stage(ProvisionStage::Syncing),
    ];
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
    .purpose("connect to Hel worker")
    .stage(ProvisionStage::Starting);
    Ok(CommandPlan {
        description: format!("reconnect Hel session {session_id}"),
        commands: vec![command],
    })
}

/// Describe safe recovery for a Podman container that belongs to an active
/// session. The inspect command is deliberately separate from `exec`: a host
/// crash can leave the container present but stopped, where `exec` cannot
/// distinguish that state from other transport failures.
pub fn target_recovery_plan(
    locator: &TargetLocator,
    session_id: &str,
) -> Result<Option<TargetRecoveryPlan>> {
    verify_locator(locator, session_id)?;
    let (inspect, start) = match locator {
        TargetLocator::LocalPodman { container_id } => (
            CommandSpec::new("podman", ["container", "inspect", container_id])
                .purpose("inspect Hel session container"),
            CommandSpec::new("podman", ["start", container_id])
                .purpose("start stopped Hel session container"),
        ),
        TargetLocator::SshPodman { ssh, container_id } => (
            ssh_command(ssh, ["podman", "container", "inspect", container_id])
                .purpose("inspect remote Hel session container"),
            ssh_command(ssh, ["podman", "start", container_id])
                .purpose("start stopped remote Hel session container"),
        ),
        TargetLocator::LocalBare { .. }
        | TargetLocator::AppleContainer { .. }
        | TargetLocator::AwsEc2 { .. }
        | TargetLocator::SshBare { .. } => return Ok(None),
    };
    Ok(Some(TargetRecoveryPlan {
        inspect,
        start,
        session_id: session_id.to_owned(),
    }))
}

/// Start a confirmed stopped Podman target and verify it reached `running`.
/// Missing or foreign containers, transport failures, and transitional states
/// fail without running the start command.
pub fn ensure_recovery_target_running(
    executor: &impl CommandExecutor,
    plan: Option<&TargetRecoveryPlan>,
) -> Result<TargetRecoveryOutcome> {
    let Some(plan) = plan else {
        return Ok(TargetRecoveryOutcome::NotRequired);
    };
    let status = inspect_recovery_target(executor, plan)?;
    match status.as_str() {
        "running" => Ok(TargetRecoveryOutcome::AlreadyRunning),
        "created" | "initialized" | "stopped" | "exited" => {
            let output = executor.execute(&plan.start)?;
            checked_command_output(&plan.start, output)
                .context("start confirmed stopped Podman session target")?;
            let after = inspect_recovery_target(executor, plan)
                .context("verify Podman session target after starting it")?;
            ensure!(
                after == "running",
                "Podman session target reported {after:?} after start"
            );
            Ok(TargetRecoveryOutcome::Started)
        }
        "paused" | "removing" | "stopping" | "unknown" => {
            bail!("refusing to start Podman session target in {status:?} state")
        }
        _ => bail!("Podman session target reported unexpected state {status:?}"),
    }
}

fn inspect_recovery_target(
    executor: &impl CommandExecutor,
    plan: &TargetRecoveryPlan,
) -> Result<String> {
    let output = executor.execute(&plan.inspect)?;
    let output = checked_command_output(&plan.inspect, output)
        .context("inspect Podman session target for recovery")?;
    let values: Vec<serde_json::Value> =
        serde_json::from_slice(&output.stdout).context("parse Podman session target inspection")?;
    ensure!(
        values.len() == 1,
        "Podman inspection returned {} targets instead of one",
        values.len()
    );
    let target = &values[0];
    let labels = target
        .pointer("/Config/Labels")
        .and_then(serde_json::Value::as_object)
        .context("Podman session target has no ownership labels")?;
    ensure!(
        labels
            .get(MANAGED_LABEL)
            .and_then(serde_json::Value::as_str)
            == Some("true"),
        "refusing to start a Podman target Hel does not own"
    );
    ensure!(
        labels
            .get(SESSION_LABEL)
            .and_then(serde_json::Value::as_str)
            == Some(plan.session_id.as_str()),
        "refusing to start a Podman target owned by another session"
    );
    target
        .pointer("/State/Status")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .context("Podman session target inspection has no state")
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

// `du` is run on its own so a path it cannot measure fails the probe instead of
// being silently dropped from the total: a session that reports less disk than
// it uses is worse than one that reports none. Its stderr is deliberately left
// attached, so the caller's failure message names the path that could not be
// read.
const AWS_SESSION_DISK_USAGE_SCRIPT: &str = r#"
usage=$(du -s -B1 -- "$@") || exit 1
printf '%s\n' "$usage" | awk '{ total += $1 } END { print total + 0 }'
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
    let writable_disk_bytes = disk_output.map(parse_disk_usage).transpose()?;
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

/// Read the single byte count every writable-disk probe answers with.
///
/// A probe that ran and answered something else measured nothing, which must be
/// reported as a failure rather than silently becoming "disk usage unknown":
/// only a probe that was never run leaves the value unknown.
fn parse_disk_usage(output: &[u8]) -> Result<u64> {
    let text = String::from_utf8_lossy(output);
    let text = text.trim();
    text.parse()
        .with_context(|| format!("disk usage probe answered {text:?} instead of a byte count"))
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

/// POSIX shell helpers that identify the daemon for one exact worker root.
/// The match is assembled at run time so the script's own command line cannot
/// select itself, and `worker proxy` command lines cannot match either.
fn worker_daemon_identity_script(worker_root: &str) -> String {
    format!(
        r#"hel_root={root}
hel_match="hel worker run --root $hel_root"
hel_match_home="hel worker run --root $HOME/$hel_root"
hel_ps() {{
    ps -ww "$@" 2>/dev/null || ps "$@" 2>/dev/null
}}
hel_is_worker() {{
    hel_args=$(hel_ps -o args= -p "$1") || return 1
    case "$hel_args" in
        *"$hel_match"*|*"$hel_match_home"*) return 0 ;;
    esac
    return 1
}}
hel_recorded_worker() {{
    [ -f "$hel_root/{pid_file}" ] || return 1
    hel_pid=$(cat "$hel_root/{pid_file}" 2>/dev/null)
    case "$hel_pid" in
        '' | *[!0-9]*) return 1 ;;
    esac
    hel_is_worker "$hel_pid" || return 1
    printf '%s\n' "$hel_pid"
}}"#,
        root = posix_quote(worker_root),
        pid_file = crate::hel_worker::WORKER_PID_FILE,
    )
}

/// Report whether the exact session worker is alive without signaling it.
/// A successful probe prints one stable token; transport or shell failures
/// stay distinguishable from a confirmed absent worker.
pub(crate) fn worker_daemon_liveness_script(worker_root: &str) -> String {
    let mut script = worker_daemon_identity_script(worker_root);
    script.push_str(
        r#"
if hel_recorded_worker >/dev/null; then
    printf 'alive\n'
    exit 0
fi
while read -r hel_pid hel_args; do
    case "$hel_pid" in
        '' | *[!0-9]*) continue ;;
    esac
    [ "$hel_pid" -eq $$ ] && continue
    case "$hel_args" in
        *"$hel_match"*|*"$hel_match_home"*) printf 'alive\n'; exit 0 ;;
    esac
done <<HEL_PS
$(hel_ps -eo pid=,args=)
HEL_PS
printf 'dead\n'
"#,
    );
    script
}

/// Stop the detached worker daemon rooted at `worker_root`.
///
/// The daemon leads its own process group, so the signal goes to the group
/// first to take the agent down with it. Shells disagree about how to write a
/// negative PID (`dash` rejects `--`), hence the two forms before the
/// single-process fallback for daemons predating the group leadership.
pub(crate) fn stop_worker_daemon_script(worker_root: &str) -> String {
    let mut script = worker_daemon_identity_script(worker_root);
    script.push_str(
        r#"
hel_signal() {
    kill -"$1" -- "-$2" 2>/dev/null && return 0
    kill -"$1" "-$2" 2>/dev/null && return 0
    kill -"$1" "$2" 2>/dev/null
}
hel_stop() {
    hel_signal TERM "$1" || return 0
    hel_waited=0
    while [ "$hel_waited" -lt 2 ]; do
        kill -0 "$1" 2>/dev/null || return 0
        sleep 1
        hel_waited=$((hel_waited + 1))
    done
    kill -0 "$1" 2>/dev/null || return 0
    hel_signal KILL "$1" || true
    hel_waited=0
    while [ "$hel_waited" -lt 3 ]; do
        kill -0 "$1" 2>/dev/null || return 0
        sleep 1
        hel_waited=$((hel_waited + 1))
    done
}
if hel_pid=$(hel_recorded_worker); then
    hel_stop "$hel_pid"
fi
hel_ps -eo pid=,args= | while read -r hel_pid hel_args; do
    case "$hel_pid" in
        '' | *[!0-9]*) continue ;;
    esac
    [ "$hel_pid" -eq $$ ] && continue
    case "$hel_args" in
        *"$hel_match"*|*"$hel_match_home"*) hel_stop "$hel_pid" ;;
    esac
done
hel_left=0
while read -r hel_pid hel_args; do
    case "$hel_pid" in
        '' | *[!0-9]*) continue ;;
    esac
    [ "$hel_pid" -eq $$ ] && continue
    case "$hel_args" in
        *"$hel_match"*|*"$hel_match_home"*) hel_left=1 ;;
    esac
done <<HEL_PS
$(hel_ps -eo pid=,args=)
HEL_PS
if [ "$hel_left" -ne 0 ]; then
    echo "worker still running after stop: $hel_root" >&2
    exit 1
fi
"#,
    );
    script
}

/// Stop a leaked worker and delete the durable relay state under its root.
///
/// A resume seeds fresh relay state into the same root a closed session used.
/// Leftover state wins over that seed at startup, so it has to go, and
/// whatever might still be writing it has to go first. Container and instance
/// targets are rebuilt from scratch on resume, so they need nothing here.
pub fn clear_relay_state_plan(
    locator: &TargetLocator,
    session_id: &str,
) -> Result<Option<CommandSpec>> {
    verify_locator(locator, session_id)?;
    let session_worker_root = worker_root(locator, session_id)?;
    let script = format!(
        "{}\nrm -rf -- {} {}\n",
        stop_worker_daemon_script(&session_worker_root),
        posix_quote(&format!(
            "{session_worker_root}/{}",
            crate::hel_worker::RELAY_STATE_FILE
        )),
        posix_quote(&format!(
            "{session_worker_root}/{}",
            crate::hel_worker::RELAY_JOURNAL_DIR
        )),
    );
    Ok(match locator {
        TargetLocator::LocalBare { .. } => Some(
            CommandSpec::new("sh", ["-c", script.as_str()])
                .purpose("stop a leaked local Hel worker and clear its relay state"),
        ),
        TargetLocator::SshBare { ssh, .. } => Some(
            ssh_command(ssh, ["sh", "-c", script.as_str()])
                .purpose("stop a leaked remote Hel worker and clear its relay state"),
        ),
        TargetLocator::LocalPodman { .. }
        | TargetLocator::AppleContainer { .. }
        | TargetLocator::SshPodman { .. }
        | TargetLocator::AwsEc2 { .. } => None,
    })
}

pub fn close_plan(locator: &TargetLocator, session_id: &str) -> Result<CommandPlan> {
    verify_locator(locator, session_id)?;
    let session_worker_root = worker_root(locator, session_id)?;
    let session_profile_home = format!(".local/share/hel/profiles/{session_id}");
    let command = match locator {
        TargetLocator::LocalBare { .. } => {
            // The daemon dies before its root does: a survivor's next durable
            // write would recreate the directory this command removes.
            let script = format!(
                "{}\nrm -rf -- {}\n",
                stop_worker_daemon_script(&session_worker_root),
                posix_quote(&session_worker_root),
            );
            CommandSpec::new("sh", ["-c", script.as_str()])
                .purpose("stop the local Hel worker and remove exact local Hel worker state")
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
        TargetLocator::SshBare { ssh, workspace } => {
            // Same ordering constraint as the local bare target: stop the
            // daemon before deleting the root it keeps writing to.
            let script = format!(
                "{}\nrm -rf -- {} {} {}\n",
                stop_worker_daemon_script(&session_worker_root),
                posix_quote(workspace),
                posix_quote(&session_worker_root),
                posix_quote(&session_profile_home),
            );
            ssh_command(ssh, ["sh", "-c", script.as_str()]).purpose(
                "stop the remote Hel worker and remove exact SSH session workspace and runtime state",
            )
        }
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
            .purpose("install Git")
            .stage(ProvisionStage::Syncing),
        ],
    }
}

/// Shared [`CommandSpec::parallel_group`] marker for one bundle's per-repository
/// clone/init commands. Every `clone_commands` call builds its own
/// [`CommandPlan`], so a single fixed marker never mixes batches across plans.
const BUNDLE_REPOSITORIES_PARALLEL_GROUP: u32 = 1;

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
        .purpose("create bundle workspace")
        .stage(ProvisionStage::Syncing),
    ];
    for repository in &bundle.repositories {
        let destination = format!("{workspace}/{}", repository.destination);
        let Some(url) = &repository.url else {
            commands.push(
                wrap(vec!["git".into(), "init".into(), "--".into(), destination])
                    .purpose(format!("initialize {}", repository.destination))
                    .stage(ProvisionStage::Syncing)
                    .parallel_group(BUNDLE_REPOSITORIES_PARALLEL_GROUP),
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
        commands.push(
            wrap(args)
                .purpose(format!("clone {}", repository.destination))
                .stage(ProvisionStage::Syncing)
                .parallel_group(BUNDLE_REPOSITORIES_PARALLEL_GROUP),
        );
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
    .purpose("start session container")
    .stage(ProvisionStage::Provisioning)
    .creates_target())
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
            "podman" => {
                let mode = if mount.read_only { "ro" } else { "O" };
                args.extend([
                    "--volume".to_owned(),
                    format!("{source}:{destination}:{mode}"),
                ]);
            }
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

/// The connectivity probe `hel doctor` runs against an SSH target.
///
/// It reuses the provisioning argument order so the probe fails exactly where
/// a real session would, with two deliberate overrides prepended. OpenSSH
/// honours the first occurrence of an option, so these win over the
/// provisioning defaults: `BatchMode=yes` never prompts for a password, and
/// `StrictHostKeyChecking=yes` never accepts an unknown host key. Doctor
/// diagnoses; the user decides whether to trust a key.
pub fn ssh_connectivity_probe(ssh: &SshTarget) -> CommandSpec {
    let mut probe = ssh.clone();
    probe.ssh_args.splice(
        0..0,
        [
            "-o".to_owned(),
            "BatchMode=yes".to_owned(),
            "-o".to_owned(),
            "StrictHostKeyChecking=yes".to_owned(),
        ],
    );
    ssh_command(&probe, ["true"]).purpose("verify SSH connectivity")
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

/// Wrap a value so a POSIX shell reads it as one literal argument. Used at the
/// SSH boundary here and when Hel rebuilds an agent's terminal command line
/// (`hel_terminal::shell_line`).
pub(crate) fn posix_quote(value: &str) -> String {
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
        || value == "~"
        || value == "~/"
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
    use std::sync::{Barrier, Mutex};

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

    /// A child that reads its stdin to end of file, as the checkpoint export
    /// worker does, only exits once the write end of the pipe is closed. Every
    /// executor that streams stdin has to close it after the last chunk.
    fn assert_streams_to_eof(executor: &dyn CommandExecutor) {
        let payload = vec![b'x'; 256 * 1024];
        let mut input = std::io::Cursor::new(payload.clone());
        let started = std::time::Instant::now();

        let output = executor
            .execute_with_stdin(
                &CommandSpec::new("sh", ["-c", "cat"]).purpose("echo a stream read to eof"),
                &mut input,
            )
            .unwrap();

        assert_eq!(output.status, 0);
        assert_eq!(output.stdout, payload);
        assert!(output.stderr.is_empty());
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn process_executor_completes_a_stream_against_a_child_that_reads_to_eof() {
        assert_streams_to_eof(&ProcessExecutor);
    }

    #[test]
    fn cancellable_executor_completes_a_stream_against_a_child_that_reads_to_eof() {
        assert_streams_to_eof(&CancellableProcessExecutor::new(Arc::new(AtomicBool::new(
            false,
        ))));
    }

    #[cfg(unix)]
    #[test]
    fn cancellable_executor_drains_large_stdout_and_stderr_concurrently() {
        let output = CancellableProcessExecutor::with_timeout(Duration::from_secs(5))
            .execute(
                &CommandSpec::new(
                    "sh",
                    [
                        "-c",
                        "head -c 131072 /dev/zero | tr '\\000' x; head -c 131072 /dev/zero | tr '\\000' y >&2",
                    ],
                )
                .purpose("emit large command output"),
            )
            .unwrap();

        assert_eq!(output.status, 0);
        assert_eq!(output.stdout, vec![b'x'; 128 * 1024]);
        assert_eq!(output.stderr, vec![b'y'; 128 * 1024]);
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

    fn podman_inspection(status: &str, session_id: &str, managed: &str) -> CommandOutput {
        podman_output(
            serde_json::to_vec(&serde_json::json!([{
                "Id": "0123456789abcdef",
                "Config": {
                    "Labels": {
                        (MANAGED_LABEL): managed,
                        (SESSION_LABEL): session_id,
                    }
                },
                "State": { "Status": status },
            }]))
            .unwrap(),
        )
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
    fn podman_target_recovery_uses_the_exact_local_or_remote_container() {
        let name = resource_name(SESSION).unwrap();
        let local = target_recovery_plan(
            &TargetLocator::LocalPodman {
                container_id: name.clone(),
            },
            SESSION,
        )
        .unwrap()
        .unwrap();
        assert_eq!(local.inspect.args, ["container", "inspect", name.as_str()]);
        assert_eq!(local.start.args, ["start", name.as_str()]);

        let remote = target_recovery_plan(
            &TargetLocator::SshPodman {
                ssh: ssh(),
                container_id: name.clone(),
            },
            SESSION,
        )
        .unwrap()
        .unwrap();
        assert_eq!(remote.inspect.program, "ssh");
        assert_eq!(
            remote.inspect.args.last().unwrap(),
            &format!("'podman' 'container' 'inspect' '{name}'")
        );
        assert_eq!(
            remote.start.args.last().unwrap(),
            &format!("'podman' 'start' '{name}'")
        );
    }

    #[test]
    fn stopped_owned_podman_target_is_started_and_reinspected() {
        let name = resource_name(SESSION).unwrap();
        let plan =
            target_recovery_plan(&TargetLocator::LocalPodman { container_id: name }, SESSION)
                .unwrap();
        let executor = PodmanPreflightExecutor::with_outputs([
            podman_inspection("exited", SESSION, "true"),
            podman_output("container-id\n"),
            podman_inspection("running", SESSION, "true"),
        ]);

        assert_eq!(
            ensure_recovery_target_running(&executor, plan.as_ref()).unwrap(),
            TargetRecoveryOutcome::Started
        );
        let seen = executor.seen.borrow();
        assert_eq!(seen.len(), 3);
        assert_eq!(seen[0].purpose, "inspect Hel session container");
        assert_eq!(seen[1].purpose, "start stopped Hel session container");
        assert_eq!(seen[2].purpose, "inspect Hel session container");
    }

    #[test]
    fn running_podman_target_is_not_started() {
        let plan = TargetRecoveryPlan {
            inspect: CommandSpec::new("inspect", std::iter::empty::<&str>()),
            start: CommandSpec::new("start", std::iter::empty::<&str>()),
            session_id: SESSION.into(),
        };
        let executor =
            PodmanPreflightExecutor::with_outputs([podman_inspection("running", SESSION, "true")]);

        assert_eq!(
            ensure_recovery_target_running(&executor, Some(&plan)).unwrap(),
            TargetRecoveryOutcome::AlreadyRunning
        );
        assert_eq!(executor.seen.borrow().len(), 1);
    }

    #[test]
    fn unsafe_podman_target_states_and_ownership_never_start() {
        for (status, session, managed, expected) in [
            ("paused", SESSION, "true", "paused"),
            ("stopping", SESSION, "true", "stopping"),
            ("exited", "another-session", "true", "another session"),
            ("exited", SESSION, "false", "does not own"),
        ] {
            let plan = TargetRecoveryPlan {
                inspect: CommandSpec::new("inspect", std::iter::empty::<&str>()),
                start: CommandSpec::new("start", std::iter::empty::<&str>()),
                session_id: SESSION.into(),
            };
            let executor = PodmanPreflightExecutor::with_outputs([podman_inspection(
                status, session, managed,
            )]);

            let error = ensure_recovery_target_running(&executor, Some(&plan))
                .unwrap_err()
                .to_string();
            assert!(error.contains(expected), "{error}");
            assert_eq!(executor.seen.borrow().len(), 1);
        }
    }

    #[test]
    fn podman_target_must_still_be_running_after_start() {
        let plan = TargetRecoveryPlan {
            inspect: CommandSpec::new("inspect", std::iter::empty::<&str>()),
            start: CommandSpec::new("start", std::iter::empty::<&str>()),
            session_id: SESSION.into(),
        };
        let executor = PodmanPreflightExecutor::with_outputs([
            podman_inspection("exited", SESSION, "true"),
            podman_output("container-id\n"),
            podman_inspection("exited", SESSION, "true"),
        ]);

        let error = ensure_recovery_target_running(&executor, Some(&plan))
            .unwrap_err()
            .to_string();
        assert!(error.contains("after start"), "{error}");
    }

    #[test]
    fn podman_inspect_or_start_failures_stop_recovery() {
        let plan = TargetRecoveryPlan {
            inspect: CommandSpec::new("inspect", std::iter::empty::<&str>())
                .purpose("inspect test target"),
            start: CommandSpec::new("start", std::iter::empty::<&str>())
                .purpose("start test target"),
            session_id: SESSION.into(),
        };
        let inspect_failed = PodmanPreflightExecutor::with_outputs([CommandOutput {
            status: 1,
            stdout: Vec::new(),
            stderr: b"container missing".to_vec(),
        }]);
        let error = ensure_recovery_target_running(&inspect_failed, Some(&plan))
            .unwrap_err()
            .to_string();
        assert!(error.contains("inspect Podman session target"), "{error}");
        assert_eq!(inspect_failed.seen.borrow().len(), 1);

        let start_failed = PodmanPreflightExecutor::with_outputs([
            podman_inspection("exited", SESSION, "true"),
            CommandOutput {
                status: 1,
                stdout: Vec::new(),
                stderr: b"start refused".to_vec(),
            },
        ]);
        let error = ensure_recovery_target_running(&start_failed, Some(&plan))
            .unwrap_err()
            .to_string();
        assert!(error.contains("start confirmed stopped"), "{error}");
        assert_eq!(start_failed.seen.borrow().len(), 2);
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
    fn container_secret_is_streamed_without_entering_local_command_arguments() {
        let secret = "github-token-that-must-not-reach-argv";
        let target = TargetTemplate::LocalPodman(ContainerTemplate {
            image: "ubuntu:24.04".to_owned(),
            extra_run_args: vec!["--env".to_owned(), "GH_TOKEN".to_owned()],
        });
        let mut plan = CommandPlan {
            description: "exercise secret launcher".to_owned(),
            commands: vec![
                CommandSpec::new("sh", ["-c", "printf %s \"$GH_TOKEN\""])
                    .purpose("read inherited secret")
                    .creates_target(),
            ],
        };

        plan.provide_target_environment_secret(&target, "GH_TOKEN", secret)
            .unwrap();

        let command = &plan.commands[0];
        assert_eq!(command.program, "sh");
        assert!(
            !command
                .args
                .iter()
                .any(|argument| argument.contains(secret))
        );
        assert!(!format!("{command:?}").contains(secret));
        assert!(format!("{command:?}").contains("<redacted>"));
        assert!(!serde_json::to_string(command).unwrap().contains(secret));
        let output = plan.execute(&ProcessExecutor).unwrap();
        assert_eq!(output[0].stdout, secret.as_bytes());
    }

    #[test]
    fn container_secret_is_streamed_without_entering_remote_ssh_arguments() {
        let secret = "remote-github-token-that-must-not-reach-argv";
        let target = TargetTemplate::SshPodman {
            ssh: ssh(),
            container: ContainerTemplate {
                image: "ubuntu:24.04".to_owned(),
                extra_run_args: vec!["--env".to_owned(), "GH_TOKEN".to_owned()],
            },
        };
        let mut plan = provision_plan(&target, SESSION, &bundle(), &[]).unwrap();

        plan.provide_target_environment_secret(&target, "GH_TOKEN", secret)
            .unwrap();

        let command = plan
            .commands
            .iter()
            .find(|command| command.creates_target)
            .unwrap();
        assert_eq!(command.program, "ssh");
        assert!(command.args.last().unwrap().contains("read -r GH_TOKEN"));
        assert!(command.args.last().unwrap().contains("'--env' 'GH_TOKEN'"));
        assert!(
            !command
                .args
                .iter()
                .any(|argument| argument.contains(secret))
        );
        assert!(!format!("{command:?}").contains(secret));
        assert!(format!("{command:?}").contains("<redacted>"));
        assert!(!serde_json::to_string(command).unwrap().contains(secret));
    }

    #[test]
    fn podman_plan_only_marks_per_repository_clone_commands_for_parallel_execution() {
        let plan = provision_plan(
            &TargetTemplate::LocalPodman(ContainerTemplate {
                image: "ubuntu:24.04".to_owned(),
                extra_run_args: vec![],
            }),
            SESSION,
            &bundle(),
            &[],
        )
        .unwrap();

        let bootstrap = plan
            .commands
            .iter()
            .find(|command| command.purpose == "install Git")
            .unwrap();
        assert_eq!(bootstrap.parallel_group, None);

        let mkdir = plan
            .commands
            .iter()
            .find(|command| command.purpose == "create bundle workspace")
            .unwrap();
        assert_eq!(mkdir.parallel_group, None);

        let clone_app = plan
            .commands
            .iter()
            .find(|command| command.purpose == "clone app")
            .unwrap();
        let clone_lib = plan
            .commands
            .iter()
            .find(|command| command.purpose == "clone libs/lib")
            .unwrap();
        assert!(clone_app.parallel_group.is_some());
        assert_eq!(clone_app.parallel_group, clone_lib.parallel_group);
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
        let mounts = [
            AdditionalMount {
                source: PathBuf::from("/host/cache"),
                destination: PathBuf::from("/mnt/cache"),
                read_only: false,
            },
            AdditionalMount {
                source: PathBuf::from("/host/models"),
                destination: PathBuf::from("/mnt/models"),
                read_only: true,
            },
        ];
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
        assert!(
            plan.commands[0]
                .args
                .windows(2)
                .any(|args| args == ["--volume", "/host/models:/mnt/models:ro"])
        );
    }

    #[test]
    fn overlay_denylist_covers_network_fuse_and_metadata_poor_filesystems() {
        assert_eq!(
            overlay_unsupported_filesystem("nfs"),
            Some("network filesystem")
        );
        assert_eq!(
            overlay_unsupported_filesystem("  NFS4 "),
            Some("network filesystem")
        );
        // FUSE names its backing driver, and the case comes from the kernel.
        assert_eq!(
            overlay_unsupported_filesystem("FUSE.sshfs"),
            Some("FUSE filesystem")
        );
        assert_eq!(
            overlay_unsupported_filesystem("fuseblk"),
            Some("FUSE filesystem")
        );
        assert_eq!(
            overlay_unsupported_filesystem("exfat"),
            Some("no POSIX metadata")
        );
        assert_eq!(
            overlay_unsupported_filesystem("overlayfs"),
            Some("overlay stacking limit")
        );
        // Anything else, known-good or unrecognized, keeps the overlay.
        assert_eq!(overlay_unsupported_filesystem("ext4"), None);
        assert_eq!(overlay_unsupported_filesystem("btrfs"), None);
        assert_eq!(overlay_unsupported_filesystem("futurefs"), None);
        assert_eq!(overlay_unsupported_filesystem(""), None);
    }

    #[test]
    fn filesystem_probe_answers_positionally_and_reaches_the_podman_host() {
        let executor = PodmanPreflightExecutor::with_outputs([podman_output(b"ext4\nnfs\n")]);
        let paths = [PathBuf::from("/host/cache"), PathBuf::from("/host/models")];

        assert_eq!(
            probe_filesystem_types(None, &paths, &executor).unwrap(),
            vec!["ext4".to_owned(), "nfs".to_owned()]
        );
        let seen = executor.seen.borrow();
        assert_eq!(seen[0].program, "stat");
        assert_eq!(
            seen[0].args,
            ["-f", "-c", "%T", "--", "/host/cache", "/host/models"]
        );

        let remote = PodmanPreflightExecutor::with_outputs([podman_output(b"ext4\nnfs\n")]);
        probe_filesystem_types(Some(&ssh()), &paths, &remote).unwrap();
        let seen = remote.seen.borrow();
        assert_eq!(seen[0].program, "ssh");
        assert!(
            seen[0].args.last().is_some_and(|remote| {
                remote == "'stat' '-f' '-c' '%T' '--' '/host/cache' '/host/models'"
            }),
            "{:?}",
            seen[0].args
        );
    }

    #[test]
    fn filesystem_probe_rejects_a_partial_or_failed_answer() {
        let short = PodmanPreflightExecutor::with_outputs([podman_output(b"ext4\n")]);
        let error = probe_filesystem_types(
            None,
            &[PathBuf::from("/host/cache"), PathBuf::from("/host/models")],
            &short,
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("named 1 filesystems for 2 directories"),
            "{error}"
        );

        let failed = PodmanPreflightExecutor::with_outputs([CommandOutput {
            status: 1,
            stdout: Vec::new(),
            stderr: b"stat: cannot read file system information".to_vec(),
        }]);
        let error = probe_filesystem_types(None, &[PathBuf::from("/host/cache")], &failed)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("cannot read file system information"),
            "{error}"
        );
    }

    #[test]
    fn apple_additional_mounts_use_read_only_bind_fallback() {
        let mounts = [AdditionalMount {
            source: PathBuf::from("/Users/me/assets"),
            destination: PathBuf::from("/mnt/assets"),
            read_only: false,
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
                .stage(ProvisionStage::Provisioning)
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
    fn a_disk_probe_that_answered_garbage_is_a_failure_not_unknown_usage() {
        let memory = b"memory.current=1073741824\n";

        // A probe that never ran leaves the usage unknown.
        let unknown = parse_resource_usage(memory, None).unwrap();
        assert_eq!(unknown.writable_disk_bytes, None);

        // A probe that ran and answered something that is not a byte count
        // measured nothing, and must not be reported as usage.
        let error = parse_resource_usage(memory, Some(b"du: cannot access\n")).unwrap_err();
        assert!(
            error.to_string().contains("instead of a byte count"),
            "{error}"
        );
        assert!(parse_resource_usage(memory, Some(b"")).is_err());
    }

    #[test]
    fn the_ec2_disk_probe_fails_instead_of_undercounting_an_unreadable_path() {
        let directory = tempfile::tempdir().unwrap();
        let measured = directory.path().join("workspace");
        fs::create_dir_all(&measured).unwrap();
        fs::write(measured.join("file"), vec![0_u8; 64 * 1024]).unwrap();
        let missing = directory.path().join("never-created");
        let disk_probe = |paths: &[&Path]| {
            let mut args = vec![
                "-c".to_owned(),
                AWS_SESSION_DISK_USAGE_SCRIPT.to_owned(),
                "sh".to_owned(),
            ];
            args.extend(paths.iter().map(|path| path.to_string_lossy().into_owned()));
            ProcessExecutor
                .execute(&CommandSpec::new("sh", args).purpose("test the EC2 session disk probe"))
                .unwrap()
        };

        let measured_only = disk_probe(&[&measured]);
        assert_eq!(measured_only.status, 0);
        let bytes = parse_disk_usage(&measured_only.stdout).unwrap();
        assert!(bytes >= 64 * 1024, "{bytes}");

        // One unreadable path must fail the probe rather than quietly reporting
        // the total of the paths that did answer.
        let with_missing = disk_probe(&[&measured, &missing]);
        assert_ne!(with_missing.status, 0);
        assert!(
            String::from_utf8_lossy(&with_missing.stderr).contains("never-created"),
            "the failure must name the path: {}",
            String::from_utf8_lossy(&with_missing.stderr)
        );
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

    /// Provisioning has to know exactly when its target came into existence,
    /// because every step after that owes the target's teardown on failure.
    #[test]
    fn every_provisioning_plan_names_the_command_that_creates_its_target() {
        let container = ContainerTemplate {
            image: "ubuntu:24.04".to_owned(),
            extra_run_args: vec![],
        };
        let creating = [
            (
                TargetTemplate::LocalPodman(container.clone()),
                "start session container",
            ),
            (
                TargetTemplate::AppleContainer(container.clone()),
                "start session container",
            ),
            (
                TargetTemplate::SshPodman {
                    ssh: ssh(),
                    container,
                },
                "start remote Podman container",
            ),
            (
                TargetTemplate::SshBare {
                    ssh: ssh(),
                    workspace_prefix: "/srv/hel".to_owned(),
                },
                "create SSH session workspace",
            ),
            (
                TargetTemplate::AwsEc2(AwsTemplate {
                    profile: "work".to_owned(),
                    region: "us-east-2".to_owned(),
                    launch_template: "hel-dev".to_owned(),
                    launch_template_version: None,
                    instance_type: None,
                    ssh: ssh(),
                }),
                "launch EC2 session instance",
            ),
        ];
        for (template, purpose) in creating {
            let plan = provision_plan(&template, SESSION, &bundle(), &[]).unwrap();
            let (creation, remainder) = plan.split_at_target_creation().unwrap();

            assert_eq!(creation.commands.last().unwrap().purpose, purpose);
            assert_eq!(
                creation
                    .commands
                    .iter()
                    .filter(|command| command.creates_target)
                    .count(),
                1
            );
            assert!(
                !remainder
                    .commands
                    .iter()
                    .any(|command| command.creates_target)
            );
            assert_eq!(
                creation.commands.len() + remainder.commands.len(),
                plan.commands.len()
            );
            // Cloning a bundle happens against a target that already exists.
            assert_eq!(
                remainder
                    .commands
                    .iter()
                    .any(|command| command.purpose.starts_with("clone ")),
                !matches!(template, TargetTemplate::AwsEc2(_)),
            );
        }

        // An existing project directory is never created, so nothing about it
        // can leak.
        assert!(
            provision_bare_project_plan(&TargetTemplate::LocalBare, SESSION, "/srv/project")
                .unwrap()
                .split_at_target_creation()
                .is_none()
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
    fn tilde_workspace_prefix_becomes_home_relative() {
        // Remote commands are single-quoted, so a literal "~" would name a
        // real directory instead of the login home.
        let template = TargetTemplate::SshBare {
            ssh: ssh(),
            workspace_prefix: "~/hel".into(),
        };
        assert_eq!(
            workspace_for(&template, SESSION).unwrap(),
            format!("hel/{SESSION}")
        );

        for degenerate in ["~", "~/"] {
            let template = TargetTemplate::SshBare {
                ssh: ssh(),
                workspace_prefix: degenerate.into(),
            };
            assert!(
                workspace_for(&template, SESSION).is_err(),
                "prefix {degenerate:?} must be rejected"
            );
        }
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
        assert_eq!(close.commands[0].program, "sh");
        assert_eq!(close.commands[0].args[0], "-c");
        let script = &close.commands[0].args[1];
        assert!(script.contains(&format!("hel_root='{worker_root}'")));
        assert!(script.ends_with(&format!("rm -rf -- '{worker_root}'\n")));
    }

    /// A leaked daemon that survives teardown recreates the root it is asked
    /// to forget, so the kill has to be part of the same cleanup command.
    #[test]
    fn bare_cleanup_stops_the_recorded_worker_before_removing_its_root() {
        let worker_root = format!("/var/lib/hel/workers/{SESSION}");
        let local = close_plan(
            &TargetLocator::LocalBare {
                worker_root: worker_root.clone(),
            },
            SESSION,
        )
        .unwrap();
        let remote = close_plan(
            &TargetLocator::SshBare {
                ssh: ssh(),
                workspace: format!(".local/share/hel/workspaces/{SESSION}"),
            },
            SESSION,
        )
        .unwrap();

        for script in [
            local.commands[0].args[1].clone(),
            remote.commands[0].args.last().unwrap().clone(),
        ] {
            let kill = script
                .find("hel_signal TERM")
                .expect("cleanup signals the worker");
            let remove = script.find("rm -rf").expect("cleanup removes the root");
            assert!(kill < remove, "the worker must die before its root does");
            // The pidfile is the identity check; a reused PID running
            // something else must survive.
            assert!(script.contains("worker.pid"));
            assert!(script.contains(r#"hel_match="hel worker run --root $hel_root""#));
            assert!(script.contains(r#"hel_match_home="hel worker run --root $HOME/$hel_root""#));
            assert!(script.contains("hel_is_worker"));
            assert!(script.contains("hel_signal KILL"));
            assert!(script.contains("worker still running after stop"));
            assert!(
                !script.contains("grep -F"),
                "leftover detection must not grep the match string; grep's own argv contains it"
            );
            assert!(!script.contains("pkill"));
        }
        assert!(
            remote.commands[0]
                .args
                .last()
                .unwrap()
                .contains(&format!(".local/share/hel/workers/{SESSION}")),
        );
    }

    #[cfg(unix)]
    #[test]
    fn stop_worker_script_succeeds_when_no_daemon_is_running() {
        let worker_root = format!("/tmp/hel-stop-absent-{}-{SESSION}", std::process::id());
        let script = stop_worker_daemon_script(&worker_root);
        let output = std::process::Command::new("sh")
            .args(["-c", &script])
            .output()
            .expect("run stop script");
        assert!(
            output.status.success(),
            "stop with no daemon must not false-positive leftover detection: status={} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[cfg(unix)]
    #[test]
    fn stop_worker_script_kills_a_matching_daemon_and_is_idempotent() {
        use std::os::unix::fs::PermissionsExt;
        use std::os::unix::process::ExitStatusExt;

        let directory = tempfile::tempdir().unwrap();
        let worker_root = directory.path().join(SESSION);
        std::fs::create_dir_all(&worker_root).unwrap();
        let fake_hel = worker_root.join("hel");
        std::fs::write(&fake_hel, "#!/bin/sh\nwhile true; do sleep 1; done\n").unwrap();
        std::fs::set_permissions(&fake_hel, std::fs::Permissions::from_mode(0o700)).unwrap();
        let root = worker_root.to_str().unwrap();
        let mut child = std::process::Command::new(&fake_hel)
            .args(["worker", "run", "--root", root, "--config", "launch.json"])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("start fake worker");
        std::fs::write(worker_root.join("worker.pid"), format!("{}\n", child.id())).unwrap();

        let liveness = std::process::Command::new("sh")
            .args(["-c", &worker_daemon_liveness_script(root)])
            .output()
            .expect("probe live fake worker");
        assert!(liveness.status.success());
        assert_eq!(liveness.stdout, b"alive\n");

        let script = stop_worker_daemon_script(root);
        let output = std::process::Command::new("sh")
            .args(["-c", &script])
            .output()
            .expect("run stop script");
        assert!(
            output.status.success(),
            "stop must kill the matching daemon: status={} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
        let status = child.wait().expect("reap fake worker");
        assert!(
            !status.success() || status.signal().is_some(),
            "fake worker should have been signaled, got {status:?}"
        );

        let output = std::process::Command::new("sh")
            .args(["-c", &script])
            .output()
            .expect("run stop script again");
        assert!(
            output.status.success(),
            "second stop must be a no-op: status={} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
        let liveness = std::process::Command::new("sh")
            .args(["-c", &worker_daemon_liveness_script(root)])
            .output()
            .expect("probe stopped fake worker");
        assert!(liveness.status.success());
        assert_eq!(liveness.stdout, b"dead\n");
    }

    /// Resume reuses a bare target's worker root, so anything left writing
    /// there and any stale relay state has to go before the restore seeds it.
    #[test]
    fn resume_cleanup_clears_relay_state_only_for_reused_bare_roots() {
        let local = clear_relay_state_plan(
            &TargetLocator::LocalBare {
                worker_root: format!("/var/lib/hel/workers/{SESSION}"),
            },
            SESSION,
        )
        .unwrap()
        .expect("raw localhost reuses its worker root");
        let script = &local.args[1];
        assert!(script.contains("hel_signal TERM"));
        assert!(script.contains(&format!(
            "rm -rf -- '/var/lib/hel/workers/{SESSION}/relay-state.json' \
             '/var/lib/hel/workers/{SESSION}/relay-journal'"
        )));

        let remote = clear_relay_state_plan(
            &TargetLocator::SshBare {
                ssh: ssh(),
                workspace: format!(".local/share/hel/workspaces/{SESSION}"),
            },
            SESSION,
        )
        .unwrap()
        .expect("an SSH host reuses its worker root");
        assert!(remote.args.last().unwrap().contains(&format!(
            ".local/share/hel/workers/{SESSION}/relay-state.json"
        )));

        // Containers and instances are rebuilt from nothing on resume.
        assert!(
            clear_relay_state_plan(
                &TargetLocator::LocalPodman {
                    container_id: resource_name(SESSION).unwrap(),
                },
                SESSION,
            )
            .unwrap()
            .is_none()
        );
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

    /// A `Sync` counterpart to [`FakeExecutor`], usable with
    /// [`CommandPlan::execute_concurrent`].
    struct SyncFakeExecutor {
        seen: Mutex<Vec<CommandSpec>>,
        fail_at: Option<usize>,
    }

    impl CommandExecutor for SyncFakeExecutor {
        fn execute(&self, command: &CommandSpec) -> Result<CommandOutput> {
            let mut seen = self.seen.lock().unwrap();
            let index = seen.len();
            seen.push(command.clone());
            Ok(CommandOutput {
                status: i32::from(self.fail_at == Some(index)),
                stdout: vec![],
                stderr: b"failure".to_vec(),
            })
        }
    }

    #[test]
    fn ungrouped_plans_behave_identically_under_both_execution_methods() {
        let commands = vec![
            CommandSpec::new("one", std::iter::empty::<String>()).purpose("one"),
            CommandSpec::new("two", std::iter::empty::<String>()).purpose("two"),
            CommandSpec::new("three", std::iter::empty::<String>()).purpose("three"),
        ];
        let sequential_plan = CommandPlan {
            description: "test".to_owned(),
            commands: commands.clone(),
        };
        let concurrent_plan = CommandPlan {
            description: "test".to_owned(),
            commands,
        };

        let sequential_executor = SyncFakeExecutor {
            seen: Mutex::new(vec![]),
            fail_at: None,
        };
        let sequential_outputs = sequential_plan.execute(&sequential_executor).unwrap();

        let concurrent_executor = SyncFakeExecutor {
            seen: Mutex::new(vec![]),
            fail_at: None,
        };
        let concurrent_outputs = concurrent_plan
            .execute_concurrent(&concurrent_executor)
            .unwrap();

        assert_eq!(sequential_outputs, concurrent_outputs);
        assert_eq!(
            sequential_executor.seen.into_inner().unwrap(),
            concurrent_executor.seen.into_inner().unwrap(),
            "an ungrouped plan runs its commands in the same order either way"
        );
    }

    /// Blocks every command on a barrier sized to the batch, so this only
    /// returns if [`CommandPlan::execute_concurrent`] actually starts the
    /// whole batch before any command in it completes.
    struct BarrierExecutor {
        seen: Mutex<Vec<CommandSpec>>,
        barrier: Barrier,
    }

    impl CommandExecutor for BarrierExecutor {
        fn execute(&self, command: &CommandSpec) -> Result<CommandOutput> {
            self.seen.lock().unwrap().push(command.clone());
            self.barrier.wait();
            Ok(CommandOutput {
                status: 0,
                stdout: vec![],
                stderr: vec![],
            })
        }
    }

    #[test]
    fn grouped_commands_run_concurrently() {
        let plan = CommandPlan {
            description: "test".to_owned(),
            commands: vec![
                CommandSpec::new("one", std::iter::empty::<String>())
                    .purpose("one")
                    .parallel_group(7),
                CommandSpec::new("two", std::iter::empty::<String>())
                    .purpose("two")
                    .parallel_group(7),
                CommandSpec::new("three", std::iter::empty::<String>())
                    .purpose("three")
                    .parallel_group(7),
            ],
        };
        let executor = BarrierExecutor {
            seen: Mutex::new(vec![]),
            barrier: Barrier::new(3),
        };

        let outputs = plan.execute_concurrent(&executor).unwrap();

        assert_eq!(outputs.len(), 3);
        assert_eq!(executor.seen.into_inner().unwrap().len(), 3);
    }

    /// Fails "first" slowly and "third" immediately, so a plan-order failure
    /// report (rather than a completion-order one) can only pick "first".
    struct OrderSensitiveFailureExecutor {
        seen: Mutex<Vec<CommandSpec>>,
    }

    impl CommandExecutor for OrderSensitiveFailureExecutor {
        fn execute(&self, command: &CommandSpec) -> Result<CommandOutput> {
            self.seen.lock().unwrap().push(command.clone());
            match command.purpose.as_str() {
                "first" => {
                    std::thread::sleep(Duration::from_millis(50));
                    Ok(CommandOutput {
                        status: 1,
                        stdout: vec![],
                        stderr: b"first failed".to_vec(),
                    })
                }
                "third" => Ok(CommandOutput {
                    status: 1,
                    stdout: vec![],
                    stderr: b"third failed".to_vec(),
                }),
                _ => Ok(CommandOutput {
                    status: 0,
                    stdout: vec![],
                    stderr: vec![],
                }),
            }
        }
    }

    #[test]
    fn batch_failure_reports_the_first_in_plan_order_failure_and_blocks_later_commands() {
        let plan = CommandPlan {
            description: "test".to_owned(),
            commands: vec![
                CommandSpec::new("a", std::iter::empty::<String>())
                    .purpose("first")
                    .parallel_group(3),
                CommandSpec::new("b", std::iter::empty::<String>())
                    .purpose("second")
                    .parallel_group(3),
                CommandSpec::new("c", std::iter::empty::<String>())
                    .purpose("third")
                    .parallel_group(3),
                CommandSpec::new("d", std::iter::empty::<String>()).purpose("fourth"),
            ],
        };
        let executor = OrderSensitiveFailureExecutor {
            seen: Mutex::new(vec![]),
        };

        let error = plan.execute_concurrent(&executor).unwrap_err();

        assert!(
            error.to_string().starts_with("first failed with status 1"),
            "expected the plan-order failure (\"first\"), got: {error}"
        );
        let seen = executor.seen.into_inner().unwrap();
        assert_eq!(
            seen.len(),
            3,
            "the whole failing batch starts even though it fails"
        );
        assert!(
            !seen.iter().any(|command| command.purpose == "fourth"),
            "a command after a failed batch must not start"
        );
    }

    #[test]
    fn failure_before_a_batch_prevents_the_batch_from_starting() {
        let plan = CommandPlan {
            description: "test".to_owned(),
            commands: vec![
                CommandSpec::new("gate", std::iter::empty::<String>()).purpose("gate"),
                CommandSpec::new("a", std::iter::empty::<String>())
                    .purpose("batch-a")
                    .parallel_group(9),
                CommandSpec::new("b", std::iter::empty::<String>())
                    .purpose("batch-b")
                    .parallel_group(9),
            ],
        };
        let executor = SyncFakeExecutor {
            seen: Mutex::new(vec![]),
            fail_at: Some(0),
        };

        assert!(plan.execute_concurrent(&executor).is_err());
        assert_eq!(
            executor.seen.into_inner().unwrap().len(),
            1,
            "a batch must not start once an earlier command has already failed"
        );
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
    fn bounded_executor_times_out_naming_the_probe_and_still_runs_the_next_one() {
        let executor = BoundedProcessExecutor::new(Duration::from_millis(100));
        let started = std::time::Instant::now();

        let error = executor
            .execute(
                &CommandSpec::new("sh", ["-c", "sleep 30"]).purpose("check a wedged prerequisite"),
            )
            .unwrap_err();

        assert!(started.elapsed() < Duration::from_secs(5));
        let message = error.to_string();
        assert!(message.contains("`sh`"), "{message}");
        assert!(
            message.contains("check a wedged prerequisite"),
            "the timeout must name the probe that hung: {message}"
        );

        // Each command gets its own deadline, so one hung probe does not
        // cancel every probe that follows it.
        let output = executor
            .execute(&CommandSpec::new("sh", ["-c", "printf ready"]).purpose("check the next one"))
            .unwrap();
        assert_eq!(output.status, 0);
        assert_eq!(output.stdout, b"ready");
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
