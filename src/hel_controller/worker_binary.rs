//! Worker binary acquisition, profile staging, and worker installation.

use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use rayon::prelude::*;
use sha2::{Digest, Sha256};

use crate::hel_config::{ProjectBundle, data_dir};
use crate::hel_targets::{self, CommandExecutor, CommandSpec, ProcessExecutor, SshTarget};
use crate::hel_worker_runtime::{WorkerLaunchConfig, WorkerOwnership};

use super::backend::backend_locator;
use super::provisioning::{force_unrestricted_mode, install_inherited_git_settings};
use super::readiness::WORKER_EXIT_RECORD_MARKER;
use super::{Controller, execute_checked, scp_command_spec, ssh_command_spec, target_profile_home};

impl Controller {
    pub(super) fn prepare_worker_files(
        &self,
        session_id: &str,
        executor: &impl CommandExecutor,
    ) -> Result<(hel_targets::TargetLocator, String)> {
        let session = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?;
        let profile = self
            .config
            .profiles
            .get(&session.last_profile)
            .context("session profile is missing")?;
        let bundle = session
            .project_directory
            .is_none()
            .then(|| self.config.bundles.get(&session.bundle_id))
            .flatten();
        let locator = session
            .target
            .as_ref()
            .context("session target is missing")?;
        let backend = backend_locator(locator, session, &self.config)?;
        let worker_root = hel_targets::worker_root(&backend, session_id)?;
        let target_profile_home = target_profile_home(&backend, session_id, profile);
        let workspace = if let Some(project_directory) = &session.project_directory {
            (project_directory.to_string_lossy().into_owned(), Vec::new())
        } else {
            workspace_paths(
                &backend,
                bundle.context("session bundle is missing")?,
                session_id,
            )?
        };
        let mut additional_directories = workspace
            .1
            .into_iter()
            .map(PathBuf::from)
            .collect::<Vec<_>>();
        additional_directories.extend(
            session
                .additional_mounts
                .iter()
                .map(|resource| resource.destination.clone()),
        );
        // The flag-enforced harnesses need the unrestricted decision on the
        // bridge command line, so it is taken before the launch config.
        let force_unrestricted = force_unrestricted_mode(&backend);
        let (bridge_command, bridge_args) = bridge_launch(
            profile.kind,
            profile.executable.as_deref(),
            force_unrestricted,
        );
        let mut environment = profile.environment.clone();
        environment.insert(profile.home_env().into(), target_profile_home.clone());
        let launch = WorkerLaunchConfig {
            session_id: session_id.to_string(),
            harness: profile.kind,
            bridge_command: PathBuf::from(bridge_command),
            bridge_args,
            environment,
            cwd: PathBuf::from(&workspace.0),
            additional_directories,
            native_session_id: session.native_session_id.clone(),
            force_unrestricted_mode: force_unrestricted,
        };

        let staging = tempfile::tempdir().context("create worker staging directory")?;
        let launch_path = staging.path().join("launch.json");
        launch.write(&launch_path)?;
        let ownership_path = staging.path().join("ownership.json");
        WorkerOwnership {
            version: WorkerOwnership::VERSION,
            session_id: session_id.to_string(),
            profile_id: session.last_profile.clone(),
            bundle_id: session.bundle_id.clone(),
            target_template_id: session.target_template_id.clone(),
        }
        .write(&ownership_path)?;
        let profile_stage = staging.path().join("profile");
        if !matches!(backend, hel_targets::TargetLocator::LocalBare { .. }) {
            let started = Instant::now();
            let result = stage_profile(profile, &profile_stage);
            tracing::debug!(
                session_id,
                elapsed_ms = started.elapsed().as_millis(),
                "profile staging completed"
            );
            result?;
        }
        let worker_binary = worker_binary_for(&backend, executor)?;

        install_worker_files(
            executor,
            &backend,
            session_id,
            &worker_root,
            &target_profile_home,
            &worker_binary,
            &launch_path,
            &ownership_path,
            &profile_stage,
        )?;
        install_inherited_git_settings(executor, &backend, session_id)?;
        Ok((backend, worker_root))
    }

    /// Collect the dead worker's exit record and log tail for a session whose
    /// worker has become unreachable. Best-effort; returns None when the
    /// target no longer exists or has no diagnostics.
    pub fn diagnose_worker(&self, session_id: &str) -> Option<String> {
        self.diagnose_worker_controlled(session_id, &ProcessExecutor)
    }

    pub fn diagnose_worker_controlled(
        &self,
        session_id: &str,
        executor: &impl CommandExecutor,
    ) -> Option<String> {
        let session = self.state.sessions.get(session_id)?;
        let locator = session.target.as_ref()?;
        let backend = backend_locator(locator, session, &self.config).ok()?;
        let worker_root = hel_targets::worker_root(&backend, session_id).ok()?;
        worker_last_words(executor, &backend, &worker_root)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerBinaryAvailability {
    Local {
        path: PathBuf,
        source: String,
    },
    Remote {
        url: String,
        sha256: String,
        triple: String,
    },
}

fn packaged_worker_binary_path(directory: &Path, triple: &str) -> PathBuf {
    directory.join(format!("hel-worker-{triple}"))
}

/// Find a worker source without downloading it.
///
/// Container provisioning resolves this after discovering the target
/// architecture. Doctor uses the same lookup with the selected container's
/// expected architecture, so it can recommend a fix without creating a
/// container or making a network request.
pub fn worker_binary_prerequisite_for_arch(arch: &str) -> Result<WorkerBinaryAvailability> {
    let triple = format!("{arch}-unknown-linux-musl");
    if let Some(path) = std::env::var_os("HEL_WORKER_BINARY").map(PathBuf::from) {
        if !path.is_file() {
            bail!("HEL_WORKER_BINARY is not a file: {}", path.display());
        }
        return Ok(WorkerBinaryAvailability::Local {
            path,
            source: "HEL_WORKER_BINARY".into(),
        });
    }
    let current = std::env::current_exe().context("resolve Hel controller binary")?;
    let mut candidates = Vec::new();
    if let Some(directory) = std::env::var_os("HEL_WORKER_DIR").map(PathBuf::from) {
        candidates.push((
            packaged_worker_binary_path(&directory, &triple),
            "HEL_WORKER_DIR",
        ));
        candidates.push((directory.join(&triple).join("hel"), "HEL_WORKER_DIR"));
    }
    if let Some(directory) = current.parent() {
        candidates.push((
            packaged_worker_binary_path(directory, &triple),
            "beside the Hel binary",
        ));
        // Development checkout: a controller at target/<profile>/hel finds its
        // musl sibling at target/<triple>/<profile>/hel. The static build is
        // preferred because the target's glibc may be older than the host's.
        if let (Some(profile), Some(target_dir)) = (
            directory.file_name().map(std::ffi::OsString::from),
            directory.parent(),
        ) {
            candidates.push((
                target_dir.join(&triple).join(profile).join("hel"),
                "development musl sibling",
            ));
        }
    }
    if let Some((path, source)) = candidates.into_iter().find(|(path, _)| path.is_file()) {
        return Ok(WorkerBinaryAvailability::Local {
            path,
            source: source.into(),
        });
    }
    if cfg!(target_os = "linux")
        && ((arch == "x86_64" && cfg!(target_arch = "x86_64"))
            || (arch == "aarch64" && cfg!(target_arch = "aarch64")))
    {
        return Ok(WorkerBinaryAvailability::Local {
            path: stable_running_executable(&current)?,
            source: "native Linux Hel binary".into(),
        });
    }
    if let Ok(template) = std::env::var("HEL_WORKER_URL") {
        let expected = std::env::var("HEL_WORKER_SHA256")
            .context("HEL_WORKER_URL requires HEL_WORKER_SHA256")?;
        validate_worker_sha256(&expected)?;
        return Ok(WorkerBinaryAvailability::Remote {
            url: template.replace("{target}", &triple),
            sha256: expected,
            triple,
        });
    }
    bail!(
        "no Linux worker for {triple}; install hel-worker-{triple} beside Hel, set HEL_WORKER_DIR/HEL_WORKER_BINARY, or configure HEL_WORKER_URL and HEL_WORKER_SHA256"
    )
}

fn stable_running_executable(current: &Path) -> Result<PathBuf> {
    if current.is_file() {
        return Ok(current.to_path_buf());
    }
    #[cfg(target_os = "linux")]
    {
        let proc_exe = PathBuf::from(format!("/proc/{}/exe", std::process::id()));
        let directory = data_dir().join("workers").join("running");
        let cached = directory.join(format!("hel-{}", std::process::id()));
        materialize_running_executable(current, &proc_exe, &cached)
    }
    #[cfg(not(target_os = "linux"))]
    bail!(
        "resolved Hel controller executable is no longer readable: {}",
        current.display()
    )
}

#[cfg(target_os = "linux")]
fn materialize_running_executable(
    current: &Path,
    proc_exe: &Path,
    cached: &Path,
) -> Result<PathBuf> {
    if !proc_exe.is_file() {
        bail!(
            "resolved Hel controller executable is no longer readable: {}",
            current.display()
        );
    }
    let parent = cached
        .parent()
        .context("worker executable cache has no parent")?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("create worker executable cache {}", parent.display()))?;
    std::fs::copy(proc_exe, cached).with_context(|| {
        format!(
            "copy running Hel executable from {} after {} was replaced",
            proc_exe.display(),
            current.display()
        )
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(cached, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(cached.to_path_buf())
}

fn worker_binary_for(
    locator: &hel_targets::TargetLocator,
    executor: &impl CommandExecutor,
) -> Result<PathBuf> {
    let arch = target_architecture(locator, executor)?;
    match worker_binary_prerequisite_for_arch(arch)? {
        WorkerBinaryAvailability::Local { path, .. } => Ok(path),
        WorkerBinaryAvailability::Remote {
            url,
            sha256,
            triple,
        } => download_worker(&url, &sha256, &triple),
    }
}

fn target_architecture(
    locator: &hel_targets::TargetLocator,
    executor: &impl CommandExecutor,
) -> Result<&'static str> {
    let command = match locator {
        hel_targets::TargetLocator::LocalBare { .. } => CommandSpec::new("uname", ["-m"]),
        hel_targets::TargetLocator::LocalPodman { container_id } => {
            CommandSpec::new("podman", ["exec", container_id, "uname", "-m"])
        }
        hel_targets::TargetLocator::AppleContainer { container_id } => {
            CommandSpec::new("container", ["exec", container_id, "uname", "-m"])
        }
        hel_targets::TargetLocator::AwsEc2 { ssh, .. }
        | hel_targets::TargetLocator::SshBare { ssh, .. } => ssh_command_spec(ssh, ["uname", "-m"]),
        hel_targets::TargetLocator::SshPodman { ssh, container_id } => {
            ssh_command_spec(ssh, ["podman", "exec", container_id, "uname", "-m"])
        }
    }
    .purpose("detect target architecture");
    let output = execute_checked(executor, command)?;
    match String::from_utf8(output.stdout)?.trim() {
        "x86_64" | "amd64" => Ok("x86_64"),
        "aarch64" | "arm64" => Ok("aarch64"),
        architecture => bail!("unsupported target architecture {architecture:?}"),
    }
}

fn download_worker(url: &str, expected_sha256: &str, triple: &str) -> Result<PathBuf> {
    validate_worker_sha256(expected_sha256)?;
    let directory = data_dir()
        .join("workers")
        .join(env!("CARGO_PKG_VERSION"))
        .join(triple);
    std::fs::create_dir_all(&directory)?;
    let destination = directory.join("hel");
    if destination.is_file() {
        let bytes = std::fs::read(&destination)?;
        if format!("{:x}", Sha256::digest(&bytes)).eq_ignore_ascii_case(expected_sha256) {
            return Ok(destination);
        }
    }
    let bytes = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .build()?
        .get(url)
        .send()?
        .error_for_status()?
        .bytes()?;
    let actual = format!("{:x}", Sha256::digest(&bytes));
    if !actual.eq_ignore_ascii_case(expected_sha256) {
        bail!("downloaded worker checksum mismatch: expected {expected_sha256}, got {actual}");
    }
    let mut temporary = tempfile::NamedTempFile::new_in(&directory)?;
    std::io::Write::write_all(&mut temporary, &bytes)?;
    temporary.as_file_mut().sync_all()?;
    temporary
        .persist(&destination)
        .map_err(|error| error.error)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&destination, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(destination)
}

fn validate_worker_sha256(expected_sha256: &str) -> Result<()> {
    if expected_sha256.len() != 64 || !expected_sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        bail!("HEL_WORKER_SHA256 must be a 64-character hexadecimal digest");
    }
    Ok(())
}

fn workspace_paths(
    locator: &hel_targets::TargetLocator,
    bundle: &ProjectBundle,
    session_id: &str,
) -> Result<(String, Vec<String>)> {
    let root = match locator {
        hel_targets::TargetLocator::LocalBare { .. } => {
            bail!("local bare projects use their selected directory")
        }
        hel_targets::TargetLocator::LocalPodman { .. }
        | hel_targets::TargetLocator::AppleContainer { .. }
        | hel_targets::TargetLocator::SshPodman { .. } => "/workspace".to_string(),
        hel_targets::TargetLocator::AwsEc2 { workspace, .. }
        | hel_targets::TargetLocator::SshBare { workspace, .. } => workspace.clone(),
    };
    if matches!(locator, hel_targets::TargetLocator::AwsEc2 { .. }) {
        let expected = format!(".local/share/hel/workspaces/{session_id}");
        if root != expected {
            bail!("AWS workspace does not match session")
        }
    }
    let primary = bundle.primary().context("bundle primary is missing")?;
    let primary_path = format!("{root}/{}", primary.destination.to_string_lossy());
    let additional = bundle
        .repositories
        .iter()
        .filter(|repository| repository.id != bundle.primary_repo)
        .map(|repository| format!("{root}/{}", repository.destination.to_string_lossy()))
        .collect();
    Ok((primary_path, additional))
}

// npx fallbacks for images that do not already carry an ACP bridge. Keep these
// in lockstep with the global npm installs in
// containers/Containerfile.agent-dev; bridge_pins_match_containerfile() below
// fails the build when they drift.
const CODEX_ACP_FALLBACK_VERSION: &str = "1.1.14";

const CLAUDE_AGENT_ACP_FALLBACK_VERSION: &str = "0.68.0";

fn bridge_launch(
    harness: crate::hel_config::HarnessKind,
    executable: Option<&Path>,
    unrestricted: bool,
) -> (String, Vec<String>) {
    if let Some(executable) = executable {
        let args = harness
            .bridge_override_args(unrestricted)
            .into_iter()
            .map(str::to_owned)
            .collect();
        return (executable.to_string_lossy().into_owned(), args);
    }
    match harness {
        crate::hel_config::HarnessKind::Codex => (
            "sh".into(),
            vec![
                "-lc".into(),
                format!("if command -v codex-acp >/dev/null 2>&1; then exec codex-acp; fi; {}; exec npx -y @agentclientprotocol/codex-acp@{CODEX_ACP_FALLBACK_VERSION}", ensure_node_script()),
            ],
        ),
        crate::hel_config::HarnessKind::Claude => (
            "sh".into(),
            vec![
                "-lc".into(),
                format!("if command -v claude-agent-acp >/dev/null 2>&1; then exec claude-agent-acp; fi; {}; exec npx -y @agentclientprotocol/claude-agent-acp@{CLAUDE_AGENT_ACP_FALLBACK_VERSION}", ensure_node_script()),
            ],
        ),
        crate::hel_config::HarnessKind::Kimi => (
            "sh".into(),
            vec![
                "-lc".into(),
                "if command -v kimi >/dev/null 2>&1; then exec kimi acp; elif [ -x \"$HOME/.kimi-code/bin/kimi\" ]; then exec \"$HOME/.kimi-code/bin/kimi\" acp; elif command -v curl >/dev/null 2>&1; then curl -fsSL https://code.kimi.com/kimi-code/install.sh | bash && exec \"$HOME/.kimi-code/bin/kimi\" acp; else echo 'Hel needs compatible Kimi Code or curl for its official installer' >&2; exit 127; fi".into(),
            ],
        ),
        crate::hel_config::HarnessKind::Grok => {
            let acp = crate::hel_config::HarnessKind::Grok
                .bridge_override_args(unrestricted)
                .join(" ");
            (
                "sh".into(),
                vec![
                    "-lc".into(),
                    format!(
                        "if command -v grok >/dev/null 2>&1; then exec grok {acp}; elif [ -x \"$GROK_HOME/bin/grok\" ]; then exec \"$GROK_HOME/bin/grok\" {acp}; elif [ -x \"$HOME/.grok/bin/grok\" ]; then exec \"$HOME/.grok/bin/grok\" {acp}; elif command -v curl >/dev/null 2>&1; then curl -fsSL https://x.ai/cli/install.sh | bash && exec \"$HOME/.grok/bin/grok\" {acp}; else echo 'Hel needs compatible Grok Build or curl for its official installer' >&2; exit 127; fi"
                    ),
                ],
            )
        }
    }
}

fn ensure_node_script() -> &'static str {
    "if ! command -v npx >/dev/null 2>&1; then if [ \"$(id -u)\" = 0 ]; then SUDO=''; elif command -v sudo >/dev/null 2>&1 && sudo -n true; then SUDO='sudo'; else echo 'Hel needs Node/npx or passwordless sudo to install it' >&2; exit 127; fi; if command -v apt-get >/dev/null 2>&1; then $SUDO apt-get update && $SUDO apt-get install -y nodejs npm; elif command -v dnf >/dev/null 2>&1; then $SUDO dnf install -y nodejs npm; elif command -v yum >/dev/null 2>&1; then $SUDO yum install -y nodejs npm; elif command -v apk >/dev/null 2>&1; then $SUDO apk add --no-cache nodejs npm; else echo 'Hel cannot install Node on this image; bake npx or a compatible ACP bridge into it' >&2; exit 127; fi; fi"
}

const HEL_CONTAINER_ENVIRONMENT: &str = "## Hel container environment\n\nThis session runs in a disposable Hel container. When the session closes, Hel checkpoints everything in project workspace directories (`/workspace/...`), including committed work, staged and unstaged changes, and untracked files.\n\nEverything outside the workspace, including installed packages, `$HOME`, and `/tmp`, is ephemeral and will be lost when the session ends. Keep durable results in the workspace or push them to a remote.\n";

fn stage_profile(profile: &crate::hel_config::HarnessProfile, destination: &Path) -> Result<()> {
    let harness = profile.kind;
    let source = profile.home.as_path();
    std::fs::create_dir_all(destination)?;
    let allowlist: &[&str] = match harness {
        crate::hel_config::HarnessKind::Codex => &[
            "auth.json",
            "config.toml",
            "AGENTS.md",
            "instructions.md",
            "rules",
            "skills",
        ],
        crate::hel_config::HarnessKind::Claude => &[
            ".claude.json",
            ".credentials.json",
            "settings.json",
            "CLAUDE.md",
            "skills",
            "plugins",
        ],
        crate::hel_config::HarnessKind::Kimi => &[
            "credentials",
            "config.toml",
            "device_id",
            "AGENTS.md",
            "SYSTEM.md",
            "mcp.json",
            "skills",
            "agents",
            "plugins",
        ],
        crate::hel_config::HarnessKind::Grok => &[
            "auth.json",
            "config.toml",
            "AGENTS.md",
            "agent_id",
            "skills",
            "plugins",
        ],
    };
    // Allowlist entries (and, within each, a copied directory's children) are
    // independent of one another, so copying them concurrently shortens the
    // stage step for profiles with large skills/plugins trees.
    allowlist.par_iter().try_for_each(|name| -> Result<()> {
        let from = source.join(name);
        if from.exists() {
            copy_profile_entry(&from, &destination.join(name))?;
        }
        Ok(())
    })?;
    append_hel_container_environment(profile.kind, destination)
}

/// Add the Hel lifecycle guidance only to the staged per-session profile.
fn append_hel_container_environment(
    harness: crate::hel_config::HarnessKind,
    destination: &Path,
) -> Result<()> {
    let instructions = match harness {
        crate::hel_config::HarnessKind::Codex => "AGENTS.md",
        crate::hel_config::HarnessKind::Claude => "CLAUDE.md",
        crate::hel_config::HarnessKind::Kimi => "SYSTEM.md",
        crate::hel_config::HarnessKind::Grok => "AGENTS.md",
    };
    let path = destination.join(instructions);
    let separator = match std::fs::read_to_string(&path) {
        Ok(contents) if !contents.is_empty() && !contents.ends_with('\n') => "\n\n",
        Ok(contents) if !contents.is_empty() => "\n",
        Ok(_) => "",
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => "",
        Err(error) => return Err(error.into()),
    };
    use std::io::Write;

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("open staged harness instructions {}", path.display()))?;
    file.write_all(separator.as_bytes())?;
    file.write_all(HEL_CONTAINER_ENVIRONMENT.as_bytes())?;
    Ok(())
}

fn copy_profile_entry(source: &Path, destination: &Path) -> Result<()> {
    let metadata = std::fs::symlink_metadata(source)
        .with_context(|| format!("read staged profile entry metadata {}", source.display()))?;
    if metadata.file_type().is_symlink() {
        return Ok(());
    }
    if metadata.is_file() {
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create staged profile directory {}", parent.display()))?;
        }
        std::fs::copy(source, destination).with_context(|| {
            format!(
                "copy staged profile file {} to {}",
                source.display(),
                destination.display()
            )
        })?;
        return Ok(());
    }
    if metadata.is_dir() {
        std::fs::create_dir_all(destination).with_context(|| {
            format!("create staged profile directory {}", destination.display())
        })?;
        let entries = std::fs::read_dir(source)
            .with_context(|| format!("list staged profile directory {}", source.display()))?
            .collect::<std::io::Result<Vec<_>>>()
            .with_context(|| {
                format!(
                    "read staged profile directory entries in {}",
                    source.display()
                )
            })?;
        // Sibling entries in one directory are independent, so recurse in
        // parallel; this is the level most likely to hold many files (e.g. a
        // skills or plugins tree).
        entries.par_iter().try_for_each(|entry| {
            copy_profile_entry(&entry.path(), &destination.join(entry.file_name()))
        })?;
        std::fs::set_permissions(destination, metadata.permissions()).with_context(|| {
            format!(
                "set permissions for staged profile directory {}",
                destination.display()
            )
        })?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn install_worker_files(
    executor: &impl CommandExecutor,
    locator: &hel_targets::TargetLocator,
    session_id: &str,
    worker_root: &str,
    profile_home: &str,
    worker_binary: &Path,
    launch_config: &Path,
    ownership: &Path,
    profile_stage: &Path,
) -> Result<()> {
    match locator {
        hel_targets::TargetLocator::LocalBare { .. } => {
            for command in [
                CommandSpec::new("mkdir", ["-p", worker_root])
                    .purpose("create local bare worker directory"),
                CommandSpec::new(
                    "cp",
                    [
                        worker_binary.to_string_lossy().into_owned(),
                        format!("{worker_root}/hel"),
                    ],
                )
                .purpose("install local Hel worker"),
                CommandSpec::new(
                    "cp",
                    [
                        launch_config.to_string_lossy().into_owned(),
                        format!("{worker_root}/launch.json"),
                    ],
                )
                .purpose("install local worker launch configuration"),
                CommandSpec::new(
                    "cp",
                    [
                        ownership.to_string_lossy().into_owned(),
                        format!("{worker_root}/ownership.json"),
                    ],
                )
                .purpose("install local worker ownership marker"),
                CommandSpec::new("chmod", ["700", &format!("{worker_root}/hel")])
                    .purpose("make local Hel worker executable"),
            ] {
                execute_checked(executor, command)?;
            }
        }
        hel_targets::TargetLocator::LocalPodman { container_id }
        | hel_targets::TargetLocator::AppleContainer { container_id } => {
            let engine = if matches!(locator, hel_targets::TargetLocator::LocalPodman { .. }) {
                "podman"
            } else {
                "container"
            };
            for command in [
                CommandSpec::new(
                    engine,
                    [
                        "exec".into(),
                        container_id.clone(),
                        "mkdir".into(),
                        "-p".into(),
                        worker_root.into(),
                        profile_home.into(),
                    ],
                )
                .purpose("create target worker directories"),
                CommandSpec::new(
                    engine,
                    [
                        "cp".into(),
                        worker_binary.to_string_lossy().into_owned(),
                        format!("{container_id}:{worker_root}/hel"),
                    ],
                )
                .purpose("upload Hel worker"),
                CommandSpec::new(
                    engine,
                    [
                        "cp".into(),
                        launch_config.to_string_lossy().into_owned(),
                        format!("{container_id}:{worker_root}/launch.json"),
                    ],
                )
                .purpose("upload worker launch configuration"),
                CommandSpec::new(
                    engine,
                    [
                        "cp".into(),
                        ownership.to_string_lossy().into_owned(),
                        format!("{container_id}:{worker_root}/ownership.json"),
                    ],
                )
                .purpose("upload worker ownership marker"),
                CommandSpec::new(
                    engine,
                    [
                        "cp".into(),
                        format!("{}/.", profile_stage.display()),
                        format!("{container_id}:{profile_home}"),
                    ],
                )
                .purpose("upload harness profile allowlist"),
                CommandSpec::new(
                    engine,
                    [
                        "exec".into(),
                        container_id.clone(),
                        "chmod".into(),
                        "700".into(),
                        format!("{worker_root}/hel"),
                    ],
                )
                .purpose("make Hel worker executable"),
                CommandSpec::new(
                    engine,
                    [
                        "exec".into(),
                        container_id.clone(),
                        "chmod".into(),
                        "-R".into(),
                        "go-rwx".into(),
                        profile_home.into(),
                    ],
                )
                .purpose("restrict harness profile permissions"),
            ] {
                execute_checked(executor, command)?;
            }
        }
        hel_targets::TargetLocator::AwsEc2 { ssh, .. }
        | hel_targets::TargetLocator::SshBare { ssh, .. } => {
            install_worker_over_ssh(
                executor,
                ssh,
                worker_root,
                profile_home,
                worker_binary,
                launch_config,
                ownership,
                profile_stage,
            )?;
        }
        hel_targets::TargetLocator::SshPodman { ssh, container_id } => {
            // The worker binary is 10-30 MB and identical across sessions, so
            // keep it in a content-addressed cache on the remote host and copy
            // it over the wire only once per unique binary.
            let digest = worker_binary_digest(worker_binary)?;
            // Home-relative, not "~/": ssh_command_spec single-quotes every
            // argument, so a tilde would stay literal in the remote shell
            // while scp expands it, and the two sides would disagree. Both
            // ssh commands (cwd is the login home) and scp resolve a relative
            // path against the remote home.
            let cache_dir = format!(".cache/hel/workers/{digest}");
            let cached_worker = format!("{cache_dir}/hel");
            let cached = matches!(
                executor.execute(
                    &ssh_command_spec(ssh, ["test", "-f", &cached_worker])
                        .purpose("probe cached remote Hel worker"),
                ),
                Ok(output) if output.status == 0
            );
            if !cached {
                execute_checked(
                    executor,
                    ssh_command_spec(ssh, ["mkdir", "-p", &cache_dir])
                        .purpose("create remote worker cache"),
                )?;
                let partial = format!("{cache_dir}/hel.partial-{session_id}");
                execute_checked(
                    executor,
                    scp_command_spec(ssh, worker_binary, &partial, false)
                        .purpose("upload remote Podman worker binary"),
                )?;
                // Rename within the cache directory so the final path only
                // ever names a complete upload.
                execute_checked(
                    executor,
                    ssh_command_spec(ssh, ["mv", &partial, &cached_worker])
                        .purpose("publish cached remote Hel worker"),
                )?;
            }
            let upload = format!(".cache/hel/uploads/{session_id}");
            execute_checked(
                executor,
                ssh_command_spec(ssh, ["mkdir", "-p", &upload])
                    .purpose("create remote upload staging"),
            )?;
            for (source, name) in [
                (launch_config, "launch.json"),
                (ownership, "ownership.json"),
            ] {
                execute_checked(
                    executor,
                    scp_command_spec(ssh, source, &format!("{upload}/{name}"), false)
                        .purpose("upload remote Podman worker file"),
                )?;
            }
            execute_checked(
                executor,
                scp_command_spec(ssh, profile_stage, &format!("{upload}/profile"), true)
                    .purpose("upload remote Podman profile allowlist"),
            )?;
            let remote = [
                vec![
                    "podman".into(),
                    "exec".into(),
                    container_id.clone(),
                    "mkdir".into(),
                    "-p".into(),
                    worker_root.into(),
                    profile_home.into(),
                ],
                vec![
                    "podman".into(),
                    "cp".into(),
                    cached_worker.clone(),
                    format!("{container_id}:{worker_root}/hel"),
                ],
                vec![
                    "podman".into(),
                    "cp".into(),
                    format!("{upload}/launch.json"),
                    format!("{container_id}:{worker_root}/launch.json"),
                ],
                vec![
                    "podman".into(),
                    "cp".into(),
                    format!("{upload}/ownership.json"),
                    format!("{container_id}:{worker_root}/ownership.json"),
                ],
                vec![
                    "podman".into(),
                    "cp".into(),
                    format!("{upload}/profile/."),
                    format!("{container_id}:{profile_home}"),
                ],
                vec![
                    "podman".into(),
                    "exec".into(),
                    container_id.clone(),
                    "chmod".into(),
                    "700".into(),
                    format!("{worker_root}/hel"),
                ],
                vec![
                    "podman".into(),
                    "exec".into(),
                    container_id.clone(),
                    "chmod".into(),
                    "-R".into(),
                    "go-rwx".into(),
                    profile_home.into(),
                ],
                vec!["rm".into(), "-rf".into(), "--".into(), upload.clone()],
            ];
            for args in remote {
                execute_checked(
                    executor,
                    ssh_command_spec(ssh, args).purpose("install remote Podman worker"),
                )?;
            }
        }
    }
    Ok(())
}

/// Content address for the worker binary, used as the remote cache key.
fn worker_binary_digest(worker_binary: &Path) -> Result<String> {
    let bytes = std::fs::read(worker_binary).with_context(|| {
        format!(
            "failed to read worker binary {} for cache addressing",
            worker_binary.display()
        )
    })?;
    Ok(format!("{:x}", Sha256::digest(&bytes)))
}

#[allow(clippy::too_many_arguments)]
fn install_worker_over_ssh(
    executor: &impl CommandExecutor,
    ssh: &SshTarget,
    worker_root: &str,
    profile_home: &str,
    worker_binary: &Path,
    launch_config: &Path,
    ownership: &Path,
    profile_stage: &Path,
) -> Result<()> {
    execute_checked(
        executor,
        ssh_command_spec(ssh, ["mkdir", "-p", worker_root, profile_home])
            .purpose("create SSH worker directories"),
    )?;
    for (source, remote, recursive) in [
        (worker_binary, format!("{worker_root}/hel"), false),
        (launch_config, format!("{worker_root}/launch.json"), false),
        (ownership, format!("{worker_root}/ownership.json"), false),
    ] {
        execute_checked(
            executor,
            scp_command_spec(ssh, source, &remote, recursive).purpose("upload SSH worker file"),
        )?;
    }
    let incoming_profile = format!("{profile_home}.incoming");
    execute_checked(
        executor,
        scp_command_spec(ssh, profile_stage, &incoming_profile, true)
            .purpose("upload SSH harness profile allowlist"),
    )?;
    execute_checked(
        executor,
        ssh_command_spec(
            ssh,
            ["cp", "-R", &format!("{incoming_profile}/."), profile_home],
        )
        .purpose("install SSH harness profile allowlist"),
    )?;
    execute_checked(
        executor,
        ssh_command_spec(ssh, ["rm", "-rf", "--", &incoming_profile])
            .purpose("remove SSH profile staging"),
    )?;
    execute_checked(
        executor,
        ssh_command_spec(ssh, ["chmod", "700", &format!("{worker_root}/hel")])
            .purpose("make SSH worker executable"),
    )?;
    execute_checked(
        executor,
        ssh_command_spec(ssh, ["chmod", "-R", "go-rwx", profile_home])
            .purpose("restrict SSH harness profile permissions"),
    )?;
    Ok(())
}

pub(super) fn start_worker(
    executor: &impl CommandExecutor,
    locator: &hel_targets::TargetLocator,
    worker_root: &str,
) -> Result<()> {
    let binary = format!("{worker_root}/hel");
    let config = format!("{worker_root}/launch.json");
    // The exit record describes the worker's previous life. Clear it as part
    // of the launch, before the new daemon can be probed: the startup connect
    // loop treats that file as proof the worker it just started has died, so
    // a stale record would abort every restart.
    let clear_exit_record = format!(
        "rm -f {}; ",
        hel_targets::join_remote_command(&[format!("{worker_root}/worker-exit.json")]),
    );
    let detached_script = format!(
        "{clear_exit_record}nohup {} >{} 2>&1 </dev/null &",
        hel_targets::join_remote_command(&[
            binary.clone(),
            "worker".into(),
            "run".into(),
            "--root".into(),
            worker_root.into(),
            "--config".into(),
            config.clone(),
        ]),
        hel_targets::join_remote_command(&[format!("{worker_root}/worker.log")]),
    );
    // Redirect daemon output to worker.log in every launch mode; an
    // unexplained dead worker is undebuggable without it.
    let exec_script = format!(
        "{clear_exit_record}exec {} >{} 2>&1",
        hel_targets::join_remote_command(&[
            binary.clone(),
            "worker".into(),
            "run".into(),
            "--root".into(),
            worker_root.into(),
            "--config".into(),
            config.clone(),
        ]),
        hel_targets::join_remote_command(&[format!("{worker_root}/worker.log")]),
    );
    let command = match locator {
        hel_targets::TargetLocator::LocalBare { .. } => {
            CommandSpec::new("sh", ["-c", &detached_script])
        }
        hel_targets::TargetLocator::LocalPodman { container_id } => CommandSpec::new(
            "podman",
            ["exec", "--detach", container_id, "sh", "-c", &exec_script],
        ),
        hel_targets::TargetLocator::AppleContainer { container_id } => CommandSpec::new(
            "container",
            ["exec", "--detach", container_id, "sh", "-c", &exec_script],
        ),
        hel_targets::TargetLocator::AwsEc2 { ssh, .. }
        | hel_targets::TargetLocator::SshBare { ssh, .. } => {
            ssh_command_spec(ssh, ["sh", "-lc", &detached_script])
        }
        hel_targets::TargetLocator::SshPodman { ssh, container_id } => ssh_command_spec(
            ssh,
            [
                "podman",
                "exec",
                "--detach",
                container_id,
                "sh",
                "-c",
                &exec_script,
            ],
        ),
    }
    .purpose("start detached Hel worker");
    execute_checked(executor, command)?;
    Ok(())
}

/// Enrich an opaque handshake failure by running the installed worker binary
/// directly in the target. This surfaces loader errors (for example a
/// glibc-linked worker inside an older-glibc container) that a detached start
/// swallows.
pub(super) fn worker_probe_diagnosis(
    executor: &impl CommandExecutor,
    locator: &hel_targets::TargetLocator,
    worker_root: &str,
    error: anyhow::Error,
) -> anyhow::Error {
    let binary = format!("{worker_root}/hel");
    let command = match locator {
        hel_targets::TargetLocator::LocalBare { .. } => {
            CommandSpec::new(binary.clone(), ["--version"])
        }
        hel_targets::TargetLocator::LocalPodman { container_id } => {
            CommandSpec::new("podman", ["exec", container_id, &binary, "--version"])
        }
        hel_targets::TargetLocator::AppleContainer { container_id } => {
            CommandSpec::new("container", ["exec", container_id, &binary, "--version"])
        }
        hel_targets::TargetLocator::AwsEc2 { ssh, .. }
        | hel_targets::TargetLocator::SshBare { ssh, .. } => {
            ssh_command_spec(ssh, [binary.as_str(), "--version"])
        }
        hel_targets::TargetLocator::SshPodman { ssh, container_id } => ssh_command_spec(
            ssh,
            ["podman", "exec", container_id, binary.as_str(), "--version"],
        ),
    }
    .purpose("probe installed worker binary");
    let error = match executor.execute(&command) {
        Ok(output) if output.status == 0 => error,
        Ok(output) => {
            let detail = String::from_utf8_lossy(&output.stderr).trim().to_string();
            error.context(format!(
                "worker binary {binary} fails to run in the target: {detail}; \
                 if this is a loader/glibc error, provide a musl worker \
                 (cargo build --release --target <arch>-unknown-linux-musl, \
                 or set HEL_WORKER_BINARY/HEL_WORKER_DIR)"
            ))
        }
        Err(probe_error) => error.context(format!("worker probe failed: {probe_error:#}")),
    };
    match worker_last_words(executor, locator, worker_root) {
        Some(last_words) => error.context(last_words),
        None => error,
    }
}

/// Fetch the dead worker's structured exit record and log tail from the
/// target, so unreachable-worker errors carry the root cause.
pub(super) fn worker_last_words(
    executor: &impl CommandExecutor,
    locator: &hel_targets::TargetLocator,
    worker_root: &str,
) -> Option<String> {
    let script = format!(
        "if [ -f {root}/worker-exit.json ]; then echo '{marker}'; cat {root}/worker-exit.json; fi; if [ -f {root}/worker.log ]; then echo '--- worker.log (tail) ---'; tail -n 20 {root}/worker.log; fi",
        root = worker_root,
        marker = WORKER_EXIT_RECORD_MARKER
    );
    let command = match locator {
        hel_targets::TargetLocator::LocalBare { .. } => CommandSpec::new("sh", ["-c", &script]),
        hel_targets::TargetLocator::LocalPodman { container_id } => {
            CommandSpec::new("podman", ["exec", container_id, "sh", "-c", &script])
        }
        hel_targets::TargetLocator::AppleContainer { container_id } => {
            CommandSpec::new("container", ["exec", container_id, "sh", "-c", &script])
        }
        hel_targets::TargetLocator::AwsEc2 { ssh, .. }
        | hel_targets::TargetLocator::SshBare { ssh, .. } => {
            ssh_command_spec(ssh, ["sh", "-lc", &script])
        }
        hel_targets::TargetLocator::SshPodman { ssh, container_id } => {
            ssh_command_spec(ssh, ["podman", "exec", container_id, "sh", "-c", &script])
        }
    }
    .purpose("collect worker last words");
    let output = executor.execute(&command).ok()?;
    let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
    (!text.is_empty()).then(|| format!("worker diagnostics:\n{text}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    use anyhow::Result;

    use crate::hel_targets::{self, CommandExecutor, CommandOutput, CommandSpec, SshTarget};

    use sha2::{Digest, Sha256};
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    use std::path::{Path, PathBuf};

    #[test]
    fn packaged_worker_names_match_release_archives() {
        let directory = Path::new("/opt/hel/bin");
        assert_eq!(
            packaged_worker_binary_path(directory, "x86_64-unknown-linux-musl"),
            directory.join("hel-worker-x86_64-unknown-linux-musl")
        );
        assert_eq!(
            packaged_worker_binary_path(directory, "aarch64-unknown-linux-musl"),
            directory.join("hel-worker-aarch64-unknown-linux-musl")
        );
    }
    #[cfg(target_os = "linux")]
    #[test]
    fn replaced_running_executable_is_materialized_for_worker_upload() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let replaced = directory.path().join("hel (deleted)");
        let proc_exe = directory.path().join("proc-exe");
        let cached = directory.path().join("workers/running/hel-1");
        std::fs::write(&proc_exe, b"running executable").unwrap();

        assert_eq!(
            materialize_running_executable(&replaced, &proc_exe, &cached).unwrap(),
            cached
        );
        assert_eq!(std::fs::read(&cached).unwrap(), b"running executable");
        assert_eq!(
            std::fs::metadata(&cached).unwrap().permissions().mode() & 0o777,
            0o700
        );
    }
    /// A worker that died leaves an exit record behind. Starting a new worker
    /// must clear it first, or the startup connect loop reads the previous
    /// death as this worker's and gives up on a healthy daemon.
    #[test]
    fn starting_a_worker_clears_the_previous_exit_record_before_launching() {
        struct RecordingExecutor {
            commands: RefCell<Vec<CommandSpec>>,
        }

        impl CommandExecutor for RecordingExecutor {
            fn execute(&self, command: &CommandSpec) -> Result<CommandOutput> {
                self.commands.borrow_mut().push(command.clone());
                Ok(CommandOutput {
                    status: 0,
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                })
            }
        }

        for locator in [
            hel_targets::TargetLocator::LocalBare {
                worker_root: "/worker/root".into(),
            },
            hel_targets::TargetLocator::LocalPodman {
                container_id: "container-1".into(),
            },
        ] {
            let executor = RecordingExecutor {
                commands: RefCell::new(Vec::new()),
            };
            start_worker(&executor, &locator, "/worker/root").unwrap();

            let commands = executor.commands.borrow();
            let script = commands
                .iter()
                .flat_map(|command| command.args.iter())
                .find(|argument| argument.contains("worker-exit.json"))
                .unwrap_or_else(|| {
                    panic!("no launch script cleared the exit record: {commands:?}")
                });
            let cleared = script.find("rm -f").expect("the exit record is removed");
            let launched = script.find("worker").expect("the daemon is launched");
            assert!(
                cleared < launched,
                "the exit record must be cleared before the daemon starts: {script}"
            );
        }
    }
    struct PodmanInstallExecutor {
        commands: RefCell<Vec<CommandSpec>>,
        worker_cached: bool,
    }
    impl CommandExecutor for PodmanInstallExecutor {
        fn execute(&self, command: &CommandSpec) -> Result<CommandOutput> {
            self.commands.borrow_mut().push(command.clone());
            let probing_cache = command
                .args
                .iter()
                .any(|argument| argument.contains("'test' '-f'"));
            let status = if probing_cache && !self.worker_cached {
                1
            } else {
                0
            };
            Ok(CommandOutput {
                status,
                stdout: Vec::new(),
                stderr: Vec::new(),
            })
        }
    }
    struct PodmanInstallFixture {
        _root: tempfile::TempDir,
        worker_binary: PathBuf,
        launch_config: PathBuf,
        ownership: PathBuf,
        profile_stage: PathBuf,
        locator: hel_targets::TargetLocator,
        digest: String,
    }
    fn podman_install_fixture() -> PodmanInstallFixture {
        let root = tempfile::tempdir().unwrap();
        let worker_binary = root.path().join("hel");
        std::fs::write(&worker_binary, b"worker-binary-bytes").unwrap();
        let launch_config = root.path().join("launch.json");
        std::fs::write(&launch_config, b"{}").unwrap();
        let ownership = root.path().join("ownership.json");
        std::fs::write(&ownership, b"{}").unwrap();
        let profile_stage = root.path().join("profile");
        std::fs::create_dir_all(&profile_stage).unwrap();
        let digest = format!("{:x}", Sha256::digest(b"worker-binary-bytes"));
        PodmanInstallFixture {
            _root: root,
            worker_binary,
            launch_config,
            ownership,
            profile_stage,
            locator: hel_targets::TargetLocator::SshPodman {
                ssh: SshTarget {
                    destination: "user@example.test".into(),
                    ssh_args: Vec::new(),
                },
                container_id: "container-1".into(),
            },
            digest,
        }
    }
    fn run_podman_install(worker_cached: bool) -> (Vec<CommandSpec>, PodmanInstallFixture) {
        let fixture = podman_install_fixture();
        let executor = PodmanInstallExecutor {
            commands: RefCell::new(Vec::new()),
            worker_cached,
        };
        install_worker_files(
            &executor,
            &fixture.locator,
            "0123456789abcdef0123456789abcdef",
            "/workspace/.hel/worker",
            "/workspace/.hel/profile",
            &fixture.worker_binary,
            &fixture.launch_config,
            &fixture.ownership,
            &fixture.profile_stage,
        )
        .unwrap();
        let commands = executor.commands.borrow().clone();
        (commands, fixture)
    }
    fn rendered(commands: &[CommandSpec]) -> Vec<String> {
        commands
            .iter()
            .map(|command| format!("{} {}", command.program, command.args.join(" ")))
            .collect()
    }
    #[test]
    fn ssh_podman_install_caches_the_worker_binary_on_a_cache_miss() {
        let (commands, fixture) = run_podman_install(false);
        let lines = rendered(&commands);
        let digest = &fixture.digest;
        let cache_dir = format!(".cache/hel/workers/{digest}");
        let session = "0123456789abcdef0123456789abcdef";

        assert!(
            lines
                .iter()
                .any(|line| line.starts_with("ssh") && line.contains("'test' '-f'")),
            "expected a cache probe, got {lines:#?}"
        );
        assert!(
            !lines.iter().any(|line| line.contains('~')),
            "remote staging paths must be home-relative: ssh arguments are \
                 single-quoted so a tilde stays literal in the remote shell while \
                 scp expands it, got {lines:#?}"
        );
        assert!(
            lines.iter().any(|line| line.starts_with("ssh")
                && line.contains(&format!("'mkdir' '-p' '{cache_dir}'"))),
            "expected the cache directory to be created, got {lines:#?}"
        );
        let partial = format!("{cache_dir}/hel.partial-{session}");
        assert!(
            lines.iter().any(|line| line
                == &format!(
                    "scp {} user@example.test:{partial}",
                    fixture.worker_binary.display()
                )),
            "expected the worker to be uploaded to the partial cache path, got {lines:#?}"
        );
        assert!(
            lines.iter().any(|line| line.starts_with("ssh")
                && line.contains(&format!("'mv' '{partial}' '{cache_dir}/hel'"))),
            "expected an atomic rename into the cache, got {lines:#?}"
        );
        assert!(
            lines.iter().any(|line| line.contains("'podman' 'cp'")
                && line.contains(&format!("'{cache_dir}/hel'"))),
            "expected podman cp to read the cached worker, got {lines:#?}"
        );
        assert!(
            !lines.iter().any(|line| line.starts_with("scp")
                && line.ends_with(&format!(
                    "user@example.test:.cache/hel/uploads/{session}/hel"
                ))),
            "the worker must not be staged in the per-session upload directory, got {lines:#?}"
        );
    }
    #[test]
    fn ssh_podman_install_skips_the_worker_upload_on_a_cache_hit() {
        let (commands, fixture) = run_podman_install(true);
        let lines = rendered(&commands);
        let digest = &fixture.digest;
        let cache_dir = format!(".cache/hel/workers/{digest}");
        let session = "0123456789abcdef0123456789abcdef";

        assert!(
            !lines.iter().any(|line| line.starts_with("scp")
                && line.contains(&fixture.worker_binary.display().to_string())),
            "a cached worker must not be re-uploaded, got {lines:#?}"
        );
        assert!(
            !lines.iter().any(|line| line.contains("'mv'")),
            "a cache hit must not rename anything, got {lines:#?}"
        );
        assert!(
            lines.iter().any(|line| line.contains("'podman' 'cp'")
                && line.contains(&format!("'{cache_dir}/hel'"))),
            "expected podman cp to read the cached worker, got {lines:#?}"
        );
        for name in ["launch.json", "ownership.json"] {
            assert!(
                lines.iter().any(|line| line.starts_with("scp")
                    && line.ends_with(&format!(
                        "user@example.test:.cache/hel/uploads/{session}/{name}"
                    ))),
                "expected {name} to still be uploaded per session, got {lines:#?}"
            );
        }
    }
    #[test]
    fn default_bridges_pin_command_capable_adapter_versions() {
        let (_, codex_arguments) = bridge_launch(crate::hel_config::HarnessKind::Codex, None, true);
        assert!(codex_arguments[1].contains("@agentclientprotocol/codex-acp@1.1.14"));

        let (_, claude_arguments) =
            bridge_launch(crate::hel_config::HarnessKind::Claude, None, true);
        assert!(claude_arguments[1].contains("@agentclientprotocol/claude-agent-acp@0.68.0"));
    }
    #[test]
    fn bridge_fallback_pins_match_the_agent_dev_containerfile() {
        const CONTAINERFILE: &str = include_str!("../../containers/Containerfile.agent-dev");

        let codex = format!("codex-acp@{CODEX_ACP_FALLBACK_VERSION}");
        assert!(
            CONTAINERFILE.contains(&codex),
            "containers/Containerfile.agent-dev must install {codex}. The image and the \
                 bridge_launch() npx fallbacks have to stay in lockstep, otherwise a container \
                 session and an npx session run different adapter versions."
        );

        let claude = format!("claude-agent-acp@{CLAUDE_AGENT_ACP_FALLBACK_VERSION}");
        assert!(
            CONTAINERFILE.contains(&claude),
            "containers/Containerfile.agent-dev must install {claude}. The image and the \
                 bridge_launch() npx fallbacks have to stay in lockstep, otherwise a container \
                 session and an npx session run different adapter versions."
        );
    }
    #[test]
    fn kimi_default_bridge_uses_bash_for_the_official_installer() {
        let (command, arguments) = bridge_launch(crate::hel_config::HarnessKind::Kimi, None, true);
        assert_eq!(command, "sh");
        assert_eq!(arguments[0], "-lc");
        assert!(arguments[1].contains("install.sh | bash &&"));
        assert!(arguments[1].contains("$HOME/.kimi-code/bin/kimi"));
    }
    #[test]
    fn grok_default_bridge_uses_bash_for_the_official_installer() {
        let (command, arguments) = bridge_launch(crate::hel_config::HarnessKind::Grok, None, false);
        assert_eq!(command, "sh");
        assert_eq!(arguments[0], "-lc");
        let script = &arguments[1];
        assert!(script.contains("https://x.ai/cli/install.sh | bash &&"));
        assert!(script.contains("command -v grok"));
        assert!(script.contains("[ -x \"$GROK_HOME/bin/grok\" ]"));
        assert!(script.contains("[ -x \"$HOME/.grok/bin/grok\" ]"));
        assert!(script.contains("exec grok agent stdio"));
        assert!(script.contains("exit 127"));
        assert!(
            !script.contains("--always-approve"),
            "a restricted session must not auto-approve: {script}"
        );
    }
    #[test]
    fn grok_default_bridge_adds_the_always_approve_flag_when_unrestricted() {
        let (_, arguments) = bridge_launch(crate::hel_config::HarnessKind::Grok, None, true);
        let script = &arguments[1];
        assert!(script.contains("exec grok agent --always-approve stdio"));
        assert!(script.contains("exec \"$GROK_HOME/bin/grok\" agent --always-approve stdio"));
        assert!(script.contains("exec \"$HOME/.grok/bin/grok\" agent --always-approve stdio"));
    }
    #[test]
    fn bridge_executable_override_carries_the_acp_subcommand_per_harness() {
        let executable = std::path::PathBuf::from("/opt/harness");
        for (kind, expected) in [
            (crate::hel_config::HarnessKind::Codex, Vec::new()),
            (crate::hel_config::HarnessKind::Claude, Vec::new()),
            (crate::hel_config::HarnessKind::Kimi, vec!["acp"]),
            (
                crate::hel_config::HarnessKind::Grok,
                vec!["agent", "--always-approve", "stdio"],
            ),
        ] {
            let (command, arguments) = bridge_launch(kind, Some(&executable), true);
            assert_eq!(command, "/opt/harness");
            assert_eq!(arguments, expected, "{kind:?} override arguments");
        }
        let (_, restricted) = bridge_launch(
            crate::hel_config::HarnessKind::Grok,
            Some(&executable),
            false,
        );
        assert_eq!(restricted, ["agent", "stdio"]);
    }
    #[test]
    fn stage_grok_profile_copies_authentication_and_agent_identity() {
        let home = tempfile::tempdir().unwrap();
        std::fs::write(
            home.path().join("auth.json"),
            "{\"https://auth.x.ai::1\":{}}",
        )
        .unwrap();
        std::fs::write(home.path().join("agent_id"), "stable-agent-id").unwrap();
        std::fs::write(home.path().join("config.toml"), "model = \"grok-4.6\"\n").unwrap();
        // Native session storage is checkpointed, never staged.
        std::fs::create_dir(home.path().join("sessions")).unwrap();
        std::fs::write(home.path().join("sessions/session_search.sqlite"), "x").unwrap();
        let staged = tempfile::tempdir().unwrap();
        let profile = crate::hel_config::HarnessProfile {
            kind: crate::hel_config::HarnessKind::Grok,
            home: home.path().to_path_buf(),
            executable: None,
            environment: BTreeMap::new(),
            context_window_bytes: None,
        };

        stage_profile(&profile, staged.path()).unwrap();

        assert_eq!(
            std::fs::read_to_string(staged.path().join("agent_id")).unwrap(),
            "stable-agent-id"
        );
        assert!(staged.path().join("auth.json").is_file());
        assert!(staged.path().join("config.toml").is_file());
        assert!(!staged.path().join("sessions").exists());
    }
    #[test]
    fn stage_claude_profile_preserves_rollout_identity() {
        let home = tempfile::tempdir().unwrap();
        let identity = r#"{
                "machineID": "stable-machine",
                "userID": "stable-user",
                "cachedGrowthBookFeatures": {
                    "tengu_velvet_mallet_fable_5": true
                }
            }"#;
        std::fs::write(home.path().join(".claude.json"), identity).unwrap();
        let staged = tempfile::tempdir().unwrap();
        let profile = crate::hel_config::HarnessProfile {
            kind: crate::hel_config::HarnessKind::Claude,
            home: home.path().to_path_buf(),
            executable: None,
            environment: BTreeMap::new(),
            context_window_bytes: None,
        };

        stage_profile(&profile, staged.path()).unwrap();

        assert_eq!(
            std::fs::read_to_string(staged.path().join(".claude.json")).unwrap(),
            identity
        );
    }
    #[test]
    fn stage_kimi_profile_preserves_device_identity() {
        let home = tempfile::tempdir().unwrap();
        std::fs::write(home.path().join("config.toml"), "default_model = \"k3\"\n").unwrap();
        std::fs::write(home.path().join("device_id"), "stable-device-id").unwrap();
        std::fs::create_dir(home.path().join("credentials")).unwrap();
        std::fs::write(
            home.path().join("credentials/kimi-code.json"),
            "{\"access_token\":\"secret\"}",
        )
        .unwrap();
        let staged = tempfile::tempdir().unwrap();
        let profile = crate::hel_config::HarnessProfile {
            kind: crate::hel_config::HarnessKind::Kimi,
            home: home.path().to_path_buf(),
            executable: None,
            environment: BTreeMap::new(),
            context_window_bytes: None,
        };

        stage_profile(&profile, staged.path()).unwrap();

        assert_eq!(
            std::fs::read_to_string(staged.path().join("device_id")).unwrap(),
            "stable-device-id"
        );
        assert!(staged.path().join("credentials/kimi-code.json").is_file());
    }
    #[test]
    fn stage_profile_appends_container_environment_for_each_harness_without_touching_home() {
        for (kind, instructions) in [
            (crate::hel_config::HarnessKind::Codex, "AGENTS.md"),
            (crate::hel_config::HarnessKind::Claude, "CLAUDE.md"),
            (crate::hel_config::HarnessKind::Kimi, "SYSTEM.md"),
            (crate::hel_config::HarnessKind::Grok, "AGENTS.md"),
        ] {
            let home = tempfile::tempdir().unwrap();
            let original = "# Controller instructions\n\nKeep this source unchanged.\n";
            let source_instructions = home.path().join(instructions);
            std::fs::write(&source_instructions, original).unwrap();
            let staged = tempfile::tempdir().unwrap();
            let profile = crate::hel_config::HarnessProfile {
                kind,
                home: home.path().to_path_buf(),
                executable: None,
                environment: std::collections::BTreeMap::new(),
                context_window_bytes: None,
            };

            stage_profile(&profile, staged.path()).unwrap();

            assert_eq!(
                std::fs::read_to_string(staged.path().join(instructions)).unwrap(),
                format!("{original}\n{HEL_CONTAINER_ENVIRONMENT}"),
                "{instructions} receives the section in the staged profile"
            );
            assert_eq!(
                std::fs::read_to_string(source_instructions).unwrap(),
                original,
                "{instructions} in the controller-side home stays untouched"
            );
        }
    }
    #[test]
    fn stage_profile_creates_missing_staged_container_instructions() {
        let home = tempfile::tempdir().unwrap();
        let staged = tempfile::tempdir().unwrap();
        let profile = crate::hel_config::HarnessProfile {
            kind: crate::hel_config::HarnessKind::Kimi,
            home: home.path().to_path_buf(),
            executable: None,
            environment: std::collections::BTreeMap::new(),
            context_window_bytes: None,
        };

        stage_profile(&profile, staged.path()).unwrap();

        assert_eq!(
            std::fs::read_to_string(staged.path().join("SYSTEM.md")).unwrap(),
            HEL_CONTAINER_ENVIRONMENT
        );
        assert!(!home.path().join("SYSTEM.md").exists());
    }
}
