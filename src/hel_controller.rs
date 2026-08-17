//! Controller-side lifecycle transitions and canonical-to-backend conversion.

mod backend;
mod checkpoint;
mod lifecycle;
mod provisioning;
mod readiness;
mod recovery_scan;
mod resume;
#[cfg(test)]
mod test_support;
mod worker_binary;
mod worktree;

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use chrono::Utc;

use crate::hel_config::{
    HelConfig, SshConnection, TargetTemplate, data_dir, is_bare_project_target, mount_history_host,
};
use crate::hel_database::advance_detached_after_event_ordinal;
use crate::hel_local_git::dirty_local_repositories;
use crate::hel_state::{
    HelState, SessionRecord, SessionResourceAllocation, SessionState, new_session_id,
    normalize_session_title,
};
use crate::hel_targets::{
    self, AdditionalMount, CommandExecutor, CommandOutput, CommandSpec, SshTarget,
};

use backend::validate_resource_allocation;
use provisioning::apply_failed_new_session_rollback;

pub use checkpoint::{CheckpointArtifact, reconcile_managed_checkpoint_archives};
pub use recovery_scan::{RecoveryCandidate, RecoveryScan};
pub use worker_binary::{WorkerBinaryAvailability, worker_binary_prerequisite_for_arch};
pub use worktree::{ResumePlan, resume_compatibility};

pub struct Controller {
    pub config: HelConfig,
    pub state: HelState,
}

/// Machine-wide advisory lock for one controller data store. This prevents a
/// dashboard, server, or CLI lifecycle command from concurrently acting as a
/// second controller against the same SQLite state and relay sessions.
#[derive(Debug)]
pub struct ControllerStoreGuard {
    file: File,
}

impl ControllerStoreGuard {
    pub fn acquire() -> Result<Self> {
        let directory = data_dir();
        Self::acquire_at(&directory)
    }

    fn acquire_at(directory: &Path) -> Result<Self> {
        std::fs::create_dir_all(directory)
            .with_context(|| format!("create controller data directory {}", directory.display()))?;
        let path = directory.join("controller.lock");
        let mut options = OpenOptions::new();
        options.create(true).read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options
            .open(&path)
            .with_context(|| format!("open controller lock {}", path.display()))?;
        match file.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => bail!(
                "another Hel controller is already using {}; stop it before starting this command",
                directory.display()
            ),
            Err(std::fs::TryLockError::Error(error)) => {
                return Err(error)
                    .with_context(|| format!("lock controller store {}", directory.display()));
            }
        }
        Ok(Self { file })
    }
}

impl Drop for ControllerStoreGuard {
    fn drop(&mut self) {
        // Make release explicit. `File` also unlocks on close, but an explicit
        // unlock keeps same-process handoff deterministic across platforms.
        let _ = self.file.unlock();
    }
}

pub struct SessionLaunchOptions {
    pub additional_mounts: Vec<AdditionalMount>,
    pub allow_dirty_local: bool,
    pub resource_allocation: Option<SessionResourceAllocation>,
    pub project_directory: Option<PathBuf>,
    pub session_title_override: Option<String>,
}

pub struct SessionResumeOptions {
    pub additional_mounts: Option<Vec<AdditionalMount>>,
    pub resource_allocation: Option<SessionResourceAllocation>,
    pub discard_queue: bool,
}

impl Controller {
    pub fn load() -> Result<Self> {
        let config = HelConfig::load()?;
        let state = HelState::load()?;
        state.validate_against_config(&config)?;
        Ok(Self { config, state })
    }

    pub fn reload(&mut self) -> Result<()> {
        *self = Self::load()?;
        Ok(())
    }

    fn persist_session_state(&self, session_id: &str) -> Result<()> {
        match self.state.sessions.get(session_id) {
            Some(session) => crate::hel_database::save_lifecycle_session(session),
            None => crate::hel_database::delete_session(session_id),
        }
    }

    fn persist_session_transition_or_restore(
        &mut self,
        session_id: &str,
        previous: &SessionRecord,
        context: &'static str,
    ) -> Result<()> {
        persist_session_record_transition_or_restore(
            &mut self.state,
            session_id,
            previous,
            context,
            &crate::hel_database::save_lifecycle_session,
        )
    }

    fn restore_prior_session_after_persistence_failure(
        &mut self,
        session_id: &str,
        previous: &SessionRecord,
        primary: anyhow::Error,
    ) -> anyhow::Error {
        restore_session_after_persistence_failure(
            &mut self.state,
            session_id,
            previous,
            primary,
            crate::hel_database::save_lifecycle_session,
        )
    }

    /// Complete a mount source at the same host that will run the container.
    pub fn complete_mount_source(
        &self,
        target_id: &str,
        prefix: &str,
        executor: &impl CommandExecutor,
    ) -> Result<Vec<String>> {
        let target = self
            .config
            .targets
            .get(target_id)
            .with_context(|| format!("unknown target template {target_id:?}"))?;
        match target {
            TargetTemplate::LocalPodman { .. }
            | TargetTemplate::AppleContainer { .. }
            | TargetTemplate::AwsEc2 { .. } => Ok(hel_targets::local_directory_completions(prefix)),
            TargetTemplate::SshPodman { ssh, .. } => {
                hel_targets::ssh_directory_completions(&backend_ssh(ssh), prefix, executor)
            }
            TargetTemplate::LocalBare | TargetTemplate::SshBare { .. } => {
                bail!("resource path completion is unsupported for bare targets")
            }
        }
    }

    /// Verify a mount source on the host where Hel will consume it.
    pub fn validate_mount_source(
        &self,
        target_id: &str,
        source: &Path,
        executor: &impl CommandExecutor,
    ) -> Result<()> {
        let target = self
            .config
            .targets
            .get(target_id)
            .with_context(|| format!("unknown target template {target_id:?}"))?;
        let exists = match target {
            TargetTemplate::LocalPodman { .. }
            | TargetTemplate::AppleContainer { .. }
            | TargetTemplate::AwsEc2 { .. } => std::fs::metadata(source)
                .map(|metadata| metadata.is_dir())
                .or_else(|error| {
                    if error.kind() == std::io::ErrorKind::NotFound {
                        Ok(false)
                    } else {
                        Err(error)
                    }
                })
                .with_context(|| format!("inspect resource source {}", source.display()))?,
            TargetTemplate::SshPodman { ssh, .. } => {
                hel_targets::ssh_directory_exists(&backend_ssh(ssh), source, executor)?
            }
            TargetTemplate::LocalBare | TargetTemplate::SshBare { .. } => {
                bail!("resource attachments are unsupported for bare targets")
            }
        };
        ensure!(
            exists,
            "source path {} does not exist or is not a directory",
            source.display()
        );
        Ok(())
    }

    fn fail_new_session_with_cleanup(
        &mut self,
        session_id: &str,
        error: anyhow::Error,
        executor: &impl CommandExecutor,
    ) -> Result<anyhow::Error> {
        let original = format!("{error:#}");
        let cleanup_error = self
            .cleanup_new_session_worktree_after_failure(session_id, executor)
            .err()
            .map(|cleanup_error| format!("{cleanup_error:#}"));
        let failure = apply_failed_new_session_rollback(
            &mut self.state,
            session_id,
            &original,
            cleanup_error,
        );
        self.persist_session_state(session_id)?;
        Ok(failure)
    }

    pub fn register_session_with_resources(
        &mut self,
        profile_id: &str,
        bundle_id: &str,
        target_id: &str,
        title: impl Into<String>,
        options: SessionLaunchOptions,
    ) -> Result<String> {
        let SessionLaunchOptions {
            additional_mounts,
            allow_dirty_local,
            resource_allocation,
            project_directory,
            session_title_override,
        } = options;
        let session_title_override = match session_title_override {
            Some(title) => {
                Some(normalize_session_title(&title).context("session name cannot be empty")?)
            }
            None => None,
        };
        let profile = self
            .config
            .profiles
            .get(profile_id)
            .with_context(|| format!("unknown profile {profile_id:?}"))?;
        let template = self
            .config
            .targets
            .get(target_id)
            .with_context(|| format!("unknown target template {target_id:?}"))?;
        if project_directory.is_some() != is_bare_project_target(template) {
            bail!("raw project directories require a bare target, and bare targets require one");
        }
        if let Some(path) = &project_directory
            && (!path.is_absolute()
                || path
                    .components()
                    .any(|part| part == std::path::Component::ParentDir))
        {
            bail!("bare project directory must be an absolute safe path");
        }
        let bundle = project_directory
            .is_none()
            .then(|| self.config.bundles.get(bundle_id))
            .flatten();
        if project_directory.is_none() && bundle.is_none() {
            bail!("unknown bundle {bundle_id:?}");
        }
        let dirty = bundle
            .map(dirty_local_repositories)
            .transpose()?
            .unwrap_or_default();
        if !allow_dirty_local && !dirty.is_empty() {
            let repositories = dirty
                .iter()
                .map(|repository| format!("{} ({})", repository.path.display(), repository.summary))
                .collect::<Vec<_>>()
                .join(", ");
            bail!(
                "local repositories have uncommitted changes: {repositories}; explicit confirmation is required"
            );
        }
        validate_resource_allocation(template, resource_allocation.as_ref())?;
        if !additional_mounts.is_empty() && mount_history_host(template).is_none() {
            bail!("attached resources are unsupported for this target");
        }
        hel_targets::validate_additional_mounts(&additional_mounts)?;
        let id = new_session_id()?;
        let now = now();
        let record = SessionRecord {
            id: id.clone(),
            title: title.into(),
            harness_kind: profile.kind,
            last_profile: profile_id.to_string(),
            bundle_id: bundle_id.to_string(),
            project_directory,
            managed_worktree: None,
            target_template_id: target_id.to_string(),
            resource_allocation,
            additional_mounts: additional_mounts.clone(),
            state: SessionState::Provisioning,
            target: None,
            native_session_id: None,
            acp_session_title: None,
            session_title_override,
            created_at: now.clone(),
            updated_at: now,
            detached_after_event_ordinal: 0,
            draft_input: String::new(),
            last_error: None,
            last_checkpoint_error: None,
            checkpoint: None,
        };
        self.state.sessions.insert(id.clone(), record);
        if let Some(host) = mount_history_host(template) {
            self.state.remember_mount_sources(host, &additional_mounts);
        }
        self.persist_session_state(&id)?;
        if let Some(host) = mount_history_host(template) {
            crate::hel_database::remember_mount_sources(host, &additional_mounts)?;
        }
        Ok(id)
    }

    pub fn rename_session(&mut self, session_id: &str, title: &str) -> Result<String> {
        let title = normalize_session_title(title).context("session name cannot be empty")?;
        ensure!(
            self.state.sessions.contains_key(session_id),
            "unknown session {session_id}"
        );
        let updated_at = now();
        crate::hel_database::set_session_title_override(session_id, &title, &updated_at)?;
        let record = self
            .state
            .sessions
            .get_mut(session_id)
            .expect("session was checked before updating its title");
        record.session_title_override = Some(title.clone());
        record.updated_at = updated_at;
        Ok(title)
    }

    pub fn mark_session_detached_after(
        &mut self,
        session_id: &str,
        event_ordinal: u64,
    ) -> Result<()> {
        ensure!(
            self.state.sessions.contains_key(session_id),
            "unknown session {session_id}"
        );
        let receipt = advance_detached_after_event_ordinal(session_id, event_ordinal)?;
        let record = self
            .state
            .sessions
            .get_mut(session_id)
            .expect("session was checked before advancing its read receipt");
        record.detached_after_event_ordinal = receipt;
        Ok(())
    }
}

fn target_kind(locator: &hel_targets::TargetLocator) -> &'static str {
    match locator {
        hel_targets::TargetLocator::LocalBare { .. } => "local-bare",
        hel_targets::TargetLocator::LocalPodman { .. } => "local-podman",
        hel_targets::TargetLocator::AppleContainer { .. } => "apple-container",
        hel_targets::TargetLocator::AwsEc2 { .. } => "aws-ec2",
        hel_targets::TargetLocator::SshBare { .. } => "ssh-bare",
        hel_targets::TargetLocator::SshPodman { .. } => "ssh-podman",
    }
}

fn target_profile_home(
    locator: &hel_targets::TargetLocator,
    session_id: &str,
    profile: &crate::hel_config::HarnessProfile,
) -> String {
    match locator {
        hel_targets::TargetLocator::LocalBare { .. } => profile.home.to_string_lossy().into_owned(),
        hel_targets::TargetLocator::LocalPodman { .. }
        | hel_targets::TargetLocator::AppleContainer { .. }
        | hel_targets::TargetLocator::SshPodman { .. } => {
            format!("/var/lib/hel/profiles/{session_id}")
        }
        hel_targets::TargetLocator::AwsEc2 { .. } | hel_targets::TargetLocator::SshBare { .. } => {
            format!(".local/share/hel/profiles/{session_id}")
        }
    }
}

pub(crate) fn backend_ssh(ssh: &SshConnection) -> SshTarget {
    let destination = match &ssh.user {
        Some(user) => format!("{user}@{}", ssh.host),
        None => ssh.host.clone(),
    };
    SshTarget {
        destination,
        ssh_args: ssh_args_with_identity(&ssh.extra_args, ssh.identity_file.as_deref()),
    }
}

fn ssh_command_spec(
    ssh: &SshTarget,
    args: impl IntoIterator<Item = impl AsRef<str>>,
) -> CommandSpec {
    let remote = args
        .into_iter()
        .map(|arg| arg.as_ref().to_string())
        .collect::<Vec<_>>();
    let mut command_args = ssh.ssh_args.clone();
    command_args.push(ssh.destination.clone());
    command_args.push(hel_targets::join_remote_command(&remote));
    CommandSpec::new("ssh", command_args)
}

fn scp_command_spec(ssh: &SshTarget, source: &Path, remote: &str, recursive: bool) -> CommandSpec {
    let mut args = ssh.ssh_args.clone();
    if recursive {
        args.push("-r".into());
    }
    args.push(source.to_string_lossy().into_owned());
    args.push(format!("{}:{remote}", ssh.destination));
    CommandSpec::new("scp", args)
}

fn ssh_args_with_identity(args: &[String], identity: Option<&Path>) -> Vec<String> {
    // Hel drives ssh non-interactively from a TUI; a host-key or password
    // prompt would steal the terminal and wedge provisioning. BatchMode fails
    // fast instead of prompting, and accept-new trusts a first-seen host key
    // (fresh EC2 instances are always first-seen) while still rejecting
    // changed keys. User-supplied ssh_args come last so they can override.
    let mut result = vec![
        "-o".into(),
        "BatchMode=yes".into(),
        "-o".into(),
        "StrictHostKeyChecking=accept-new".into(),
        "-o".into(),
        "ConnectTimeout=15".into(),
    ];
    result.extend(args.iter().cloned());
    if let Some(identity) = identity {
        result.push("-i".into());
        result.push(identity.to_string_lossy().into_owned());
    }
    result
}

fn execute_checked(executor: &impl CommandExecutor, command: CommandSpec) -> Result<CommandOutput> {
    let output = executor.execute(&command)?;
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

fn now() -> String {
    Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn restore_session_after_persistence_failure(
    state: &mut HelState,
    session_id: &str,
    previous: &SessionRecord,
    primary: anyhow::Error,
    persist: impl FnOnce(&SessionRecord) -> Result<()>,
) -> anyhow::Error {
    state
        .sessions
        .insert(session_id.to_owned(), previous.clone());
    let restored = state
        .sessions
        .get(session_id)
        .expect("restored session record disappeared");
    match persist(restored) {
        Ok(()) => primary,
        Err(error) => primary.context(format!(
            "restored prior session state in memory, but failed to persist the rollback: {error:#}"
        )),
    }
}

fn persist_session_record_transition_or_restore(
    state: &mut HelState,
    session_id: &str,
    previous: &SessionRecord,
    context: &'static str,
    persist: &impl Fn(&SessionRecord) -> Result<()>,
) -> Result<()> {
    let result = persist(
        state
            .sessions
            .get(session_id)
            .expect("checkpoint session disappeared before persistence"),
    );
    match result {
        Ok(()) => Ok(()),
        Err(error) => Err(restore_session_after_persistence_failure(
            state,
            session_id,
            previous,
            error.context(context),
            persist,
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::Path;

    use crate::hel_config::{ContainerTemplate as ConfigContainer, HelConfig, TargetTemplate};
    use crate::hel_state::HelState;
    use crate::hel_targets::ProcessExecutor;

    use super::*;

    #[test]
    fn controller_store_lock_excludes_a_second_process_owner() {
        let directory = tempfile::tempdir().unwrap();
        let first = ControllerStoreGuard::acquire_at(directory.path()).unwrap();
        run_controller_lock_probe(directory.path(), true);
        drop(first);
        run_controller_lock_probe(directory.path(), false);
    }
    fn run_controller_lock_probe(directory: &Path, expect_locked: bool) {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "hel_controller::tests::controller_store_lock_subprocess_probe",
                "--nocapture",
            ])
            .env("HEL_CONTROLLER_LOCK_PROBE", directory)
            .env(
                "HEL_CONTROLLER_LOCK_EXPECTED",
                if expect_locked { "locked" } else { "available" },
            )
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "controller lock subprocess failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    #[test]
    fn controller_store_lock_subprocess_probe() {
        let Some(directory) = std::env::var_os("HEL_CONTROLLER_LOCK_PROBE") else {
            return;
        };
        let expected = std::env::var("HEL_CONTROLLER_LOCK_EXPECTED").unwrap();
        let acquired = ControllerStoreGuard::acquire_at(Path::new(&directory));
        match expected.as_str() {
            "locked" => {
                let error = acquired.expect_err("a second process acquired the controller store");
                assert!(error.to_string().contains("another Hel controller"));
            }
            "available" => {
                acquired.expect("released controller store stayed locked");
            }
            value => panic!("unexpected lock probe expectation {value:?}"),
        }
    }
    #[test]
    fn local_mount_source_must_be_an_existing_directory() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("file");
        std::fs::write(&file, "not a directory").unwrap();
        let mut config = HelConfig::default();
        config.targets.insert(
            "local".into(),
            TargetTemplate::LocalPodman {
                container: ConfigContainer {
                    image: "ubuntu:24.04".into(),
                    platform: None,
                    cpus: None,
                    memory: None,
                    environment: BTreeMap::new(),
                },
            },
        );
        let controller = Controller {
            config,
            state: HelState::default(),
        };

        assert!(
            controller
                .validate_mount_source("local", directory.path(), &ProcessExecutor)
                .is_ok()
        );
        for invalid in [file, directory.path().join("missing")] {
            let error = controller
                .validate_mount_source("local", &invalid, &ProcessExecutor)
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("does not exist or is not a directory")
            );
        }
    }
}
