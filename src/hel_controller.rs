//! Controller-side lifecycle transitions and canonical-to-backend conversion.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use chrono::Utc;
use sha2::{Digest, Sha256};

use crate::hel_archive::{
    ArchiveInput, BundleManifest, GitCollectionSpec, PayloadRole, SessionManifest, SystemGit,
    TargetManifest, collect_git_snapshot, read_archive_verified, write_archive_atomic,
};
use crate::hel_checkpoint::{
    CheckpointExportSpec, CheckpointRepositorySpec, CheckpointRestoreSpec, CheckpointTransfer,
    RepositoryRestoreSpec, export_command, restore_command,
};
use crate::hel_config::{
    AwsAddressSource, HelConfig, ProjectBundle, SshConnection, TargetTemplate, data_dir,
    sessions_dir,
};
use crate::hel_git_proxy::{GitBrokerSpec, broker_is_alive};
use crate::hel_local_git::{canonical_repository, dirty_local_repositories};
use crate::hel_state::{
    CheckpointMetadata, HelState, SessionRecord, SessionResourceAllocation, SessionState,
    TargetLocator, new_session_id, normalize_session_title,
};
use crate::hel_targets::{
    self, AdditionalMount, AwsTemplate, CommandExecutor, CommandOutput, CommandSpec,
    ContainerTemplate, ProcessExecutor, ProjectBundleSpec, RepositorySpec, SshTarget,
};
use crate::hel_worker::{
    PROTOCOL_VERSION, RequestEnvelope, ResponseBody, ResponsePayload, VersionRange, WorkerEvent,
    WorkerRequest,
};
use crate::hel_worker_client::{WorkerBootstrap, WorkerClient};
use crate::hel_worker_runtime::{WorkerLaunchConfig, WorkerOwnership};

const INHERITED_GIT_SETTINGS: &[&str] = &[
    "diff.algorithm",
    "fetch.prune",
    "fetch.prunetags",
    "init.defaultbranch",
    "merge.conflictstyle",
    "pull.ff",
    "pull.rebase",
    "push.autosetupremote",
    "push.default",
    "rebase.autostash",
    "rerere.autoupdate",
    "rerere.enabled",
    "user.email",
    "user.name",
];

pub struct Controller {
    pub config: HelConfig,
    pub state: HelState,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct RecoveryCandidate {
    pub session_id: String,
    pub target_template_id: String,
    pub locator: TargetLocator,
    pub ownership: Option<WorkerOwnership>,
}

#[derive(Debug, Default, serde::Serialize)]
pub struct RecoveryScan {
    pub candidates: Vec<RecoveryCandidate>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct CheckpointArtifact {
    pub metadata: CheckpointMetadata,
    pub native_session_id: String,
}

pub struct SessionLaunchOptions {
    pub additional_mounts: Vec<AdditionalMount>,
    pub allow_dirty_local: bool,
    pub resource_allocation: Option<SessionResourceAllocation>,
}

impl Controller {
    pub fn resolve_aws_resource_options(
        &self,
        target_id: &str,
        executor: &impl CommandExecutor,
    ) -> Result<Vec<SessionResourceAllocation>> {
        let TargetTemplate::AwsEc2 {
            aws_profile,
            region,
            launch_template,
            launch_template_version,
            ..
        } = self
            .config
            .targets
            .get(target_id)
            .with_context(|| format!("unknown target template {target_id:?}"))?
        else {
            bail!("target {target_id:?} is not an AWS EC2 target");
        };
        let profile = aws_profile.as_deref().unwrap_or("default");
        let launch_key = if launch_template.starts_with("lt-") {
            "--launch-template-id"
        } else {
            "--launch-template-name"
        };
        let version = launch_template_version.as_deref().unwrap_or("$Default");
        let describe_template = CommandSpec::new(
            "aws",
            [
                "--profile",
                profile,
                "--region",
                region,
                "ec2",
                "describe-launch-template-versions",
                launch_key,
                launch_template,
                "--versions",
                version,
                "--output",
                "json",
            ],
        )
        .purpose("resolve EC2 launch template instance family");
        let output = executor.execute(&describe_template)?;
        if output.status != 0 {
            bail!(
                "{} failed with status {}: {}",
                describe_template.purpose,
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        let response: serde_json::Value =
            serde_json::from_slice(&output.stdout).context("parse EC2 launch template response")?;
        let instance_type = response
            .pointer("/LaunchTemplateVersions/0/LaunchTemplateData/InstanceType")
            .and_then(serde_json::Value::as_str)
            .context("launch template does not specify a concrete instance type")?;
        let family = instance_type
            .rsplit_once('.')
            .map(|(family, _)| family)
            .context("launch template instance type has no size suffix")?;
        let filter = format!("Name=instance-type,Values={family}.*");
        let describe_types = CommandSpec::new(
            "aws",
            [
                "--profile",
                profile,
                "--region",
                region,
                "ec2",
                "describe-instance-types",
                "--filters",
                &filter,
                "--output",
                "json",
            ],
        )
        .purpose("discover EC2 instance sizes");
        let output = executor.execute(&describe_types)?;
        if output.status != 0 {
            bail!(
                "{} failed with status {}: {}",
                describe_types.purpose,
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        let response: serde_json::Value =
            serde_json::from_slice(&output.stdout).context("parse EC2 instance type response")?;
        let mut options = response
            .get("InstanceTypes")
            .and_then(serde_json::Value::as_array)
            .context("EC2 instance type response omitted InstanceTypes")?
            .iter()
            .filter_map(|entry| {
                Some(SessionResourceAllocation::AwsEc2 {
                    instance_type: entry.get("InstanceType")?.as_str()?.to_owned(),
                    vcpus: entry.pointer("/VCpuInfo/DefaultVCpus")?.as_u64()?,
                    memory_bytes: entry
                        .pointer("/MemoryInfo/SizeInMiB")?
                        .as_u64()?
                        .checked_mul(1024 * 1024)?,
                })
            })
            .collect::<Vec<_>>();
        options.sort_by_key(allocation_vcpus);
        if !options.iter().any(|option| allocation_vcpus(option) == 8) {
            bail!("EC2 family {family:?} has no exact 8-vCPU baseline size");
        }
        Ok(options)
    }

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

    /// Find managed resources which are not represented by the controller's
    /// current state. Labels/tags establish Hel ownership; the worker marker
    /// supplies profile and bundle metadata when it is available.
    pub fn scan_orphan_workers(&self, executor: &impl CommandExecutor) -> RecoveryScan {
        let mut scan = RecoveryScan::default();
        for (target_id, template) in &self.config.targets {
            match scan_target_workers(target_id, template, executor) {
                Ok(candidates) => {
                    scan.candidates
                        .extend(candidates.into_iter().filter(|candidate| {
                            !self.state.sessions.contains_key(&candidate.session_id)
                        }))
                }
                Err(error) => scan.warnings.push(format!("target {target_id}: {error:#}")),
            }
        }
        scan.candidates.sort_by(|left, right| {
            (&left.session_id, &left.target_template_id)
                .cmp(&(&right.session_id, &right.target_template_id))
        });
        scan.candidates.dedup_by(|left, right| {
            left.session_id == right.session_id
                && left.target_template_id == right.target_template_id
        });
        scan
    }

    pub async fn adopt_orphan_worker(
        &mut self,
        session_id: &str,
        target_id: &str,
        profile_override: Option<&str>,
        bundle_override: Option<&str>,
        executor: &impl CommandExecutor,
    ) -> Result<()> {
        if self.state.sessions.contains_key(session_id) {
            bail!("session {session_id} is already tracked");
        }
        let candidate = self
            .scan_orphan_workers(executor)
            .candidates
            .into_iter()
            .find(|candidate| {
                candidate.session_id == session_id && candidate.target_template_id == target_id
            })
            .with_context(|| {
                format!("no managed orphan {session_id} was found on target {target_id:?}")
            })?;
        let profile_id = profile_override
            .map(str::to_owned)
            .or_else(|| {
                candidate
                    .ownership
                    .as_ref()
                    .map(|marker| marker.profile_id.clone())
            })
            .context("orphan has no ownership marker; pass --profile")?;
        let bundle_id = bundle_override
            .map(str::to_owned)
            .or_else(|| {
                candidate
                    .ownership
                    .as_ref()
                    .map(|marker| marker.bundle_id.clone())
            })
            .context("orphan has no ownership marker; pass --bundle")?;
        let profile = self
            .config
            .profiles
            .get(&profile_id)
            .with_context(|| format!("unknown profile {profile_id:?}"))?;
        self.config
            .bundles
            .get(&bundle_id)
            .with_context(|| format!("unknown bundle {bundle_id:?}"))?;
        let now = now();
        let record = SessionRecord {
            id: session_id.to_owned(),
            title: format!("Recovered {}", &session_id[..session_id.len().min(8)]),
            harness_kind: profile.kind,
            last_profile: profile_id,
            bundle_id,
            target_template_id: target_id.to_owned(),
            resource_allocation: None,
            additional_mounts: Vec::new(),
            state: SessionState::Disconnected,
            target: Some(candidate.locator),
            native_session_id: None,
            acp_session_title: None,
            session_title_override: None,
            created_at: now.clone(),
            updated_at: now,
            last_viewed_event_sequence: 0,
            last_error: None,
            last_checkpoint_error: None,
            checkpoint: None,
        };
        let backend = backend_locator(record.target.as_ref().unwrap(), &record, &self.config)?;
        let spec = hel_targets::reconnect_plan(&backend, session_id)?
            .commands
            .into_iter()
            .next()
            .context("reconnect plan is empty")?;
        let mut client = WorkerClient::connect(&spec, session_id)
            .await
            .context("orphan worker did not complete the v1 handshake")?;
        let bootstrap = client
            .bootstrap()
            .await
            .context("bootstrap orphan worker")?;
        let mut record = record;
        record.state = SessionState::Running;
        record.native_session_id = crate::hel_worker::recover_native_session_id(&bootstrap.events);
        record.acp_session_title = crate::hel_state::harness_session_title(&bootstrap.events);
        self.state.sessions.insert(session_id.to_owned(), record);
        self.state.save()
    }

    pub fn destroy_orphan_worker(
        &self,
        session_id: &str,
        target_id: &str,
        confirmation: &str,
        executor: &impl CommandExecutor,
    ) -> Result<()> {
        if confirmation != session_id {
            bail!("refusing destructive recovery: --confirm must exactly match the session ID");
        }
        let candidate = self
            .scan_orphan_workers(executor)
            .candidates
            .into_iter()
            .find(|candidate| {
                candidate.session_id == session_id && candidate.target_template_id == target_id
            })
            .with_context(|| {
                format!("no managed orphan {session_id} was found on target {target_id:?}")
            })?;
        let template = self.config.targets.get(target_id).unwrap();
        let backend = recovery_backend_locator(template, &candidate.locator, session_id)?;
        hel_targets::close_plan(&backend, session_id)?
            .execute(executor)
            .map(|_| ())
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
            TargetTemplate::SshBare { .. } => {
                bail!("resource path completion is unsupported for bare SSH targets")
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
            TargetTemplate::SshBare { .. } => {
                bail!("resource attachments are unsupported for bare SSH targets")
            }
        };
        ensure!(
            exists,
            "source path {} does not exist or is not a directory",
            source.display()
        );
        Ok(())
    }

    pub fn register_session(
        &mut self,
        profile_id: &str,
        bundle_id: &str,
        target_id: &str,
        title: impl Into<String>,
    ) -> Result<String> {
        self.register_session_with_mounts(profile_id, bundle_id, target_id, title, Vec::new())
    }

    pub fn register_session_with_mounts(
        &mut self,
        profile_id: &str,
        bundle_id: &str,
        target_id: &str,
        title: impl Into<String>,
        additional_mounts: Vec<AdditionalMount>,
    ) -> Result<String> {
        self.register_session_with_mounts_allow_dirty(
            profile_id,
            bundle_id,
            target_id,
            title,
            additional_mounts,
            false,
        )
    }

    pub fn register_session_with_mounts_allow_dirty(
        &mut self,
        profile_id: &str,
        bundle_id: &str,
        target_id: &str,
        title: impl Into<String>,
        additional_mounts: Vec<AdditionalMount>,
        allow_dirty_local: bool,
    ) -> Result<String> {
        self.register_session_with_resources(
            profile_id,
            bundle_id,
            target_id,
            title,
            SessionLaunchOptions {
                additional_mounts,
                allow_dirty_local,
                resource_allocation: None,
            },
        )
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
        } = options;
        let profile = self
            .config
            .profiles
            .get(profile_id)
            .with_context(|| format!("unknown profile {profile_id:?}"))?;
        let bundle = self
            .config
            .bundles
            .get(bundle_id)
            .with_context(|| format!("unknown bundle {bundle_id:?}"))?;
        let dirty = dirty_local_repositories(bundle)?;
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
        let template = self
            .config
            .targets
            .get(target_id)
            .with_context(|| format!("unknown target template {target_id:?}"))?;
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
            target_template_id: target_id.to_string(),
            resource_allocation,
            additional_mounts: additional_mounts.clone(),
            state: SessionState::Provisioning,
            target: None,
            native_session_id: None,
            acp_session_title: None,
            session_title_override: None,
            created_at: now.clone(),
            updated_at: now,
            last_viewed_event_sequence: 0,
            last_error: None,
            last_checkpoint_error: None,
            checkpoint: None,
        };
        self.state.sessions.insert(id.clone(), record);
        if let Some(host) = mount_history_host(template) {
            self.state.remember_mount_sources(&host, &additional_mounts);
        }
        self.state.save()?;
        Ok(id)
    }

    pub async fn provision_session(&mut self, session_id: &str) -> Result<()> {
        self.provision_session_with(session_id, &ProcessExecutor)
            .await?;
        match self
            .install_and_connect_worker(session_id, &ProcessExecutor)
            .await
        {
            Ok(native_session_id) => self.mark_worker_connected(session_id, native_session_id),
            Err(error) => {
                let record = self.state.sessions.get_mut(session_id).unwrap();
                record.state = SessionState::Error;
                record.updated_at = now();
                record.last_error = Some(format!("worker bootstrap failed: {error:#}"));
                self.state.save()?;
                Err(error)
            }
        }
    }

    pub fn rename_session(&mut self, session_id: &str, title: &str) -> Result<String> {
        let title = normalize_session_title(title).context("session name cannot be empty")?;
        let record = self
            .state
            .sessions
            .get_mut(session_id)
            .with_context(|| format!("unknown session {session_id}"))?;
        record.session_title_override = Some(title.clone());
        record.updated_at = now();
        self.state.save()?;
        Ok(title)
    }

    pub fn mark_session_viewed_through(
        &mut self,
        session_id: &str,
        event_sequence: u64,
    ) -> Result<()> {
        let record = self
            .state
            .sessions
            .get_mut(session_id)
            .with_context(|| format!("unknown session {session_id}"))?;
        if event_sequence > record.last_viewed_event_sequence {
            record.last_viewed_event_sequence = event_sequence;
            self.state.save()?;
        }
        Ok(())
    }

    pub async fn provision_session_with(
        &mut self,
        session_id: &str,
        executor: &(impl CommandExecutor + Sync),
    ) -> Result<()> {
        let session = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?
            .clone();
        if session.state != SessionState::Provisioning {
            bail!("session {session_id} is not provisioning");
        }
        // Once registration succeeds, every subsequent failure must remove the
        // provisional record. Keep planning, preflight, creation, and locator
        // discovery in one result so no early `?` can strand a session.
        let result = (|| {
            let template = self
                .config
                .targets
                .get(&session.target_template_id)
                .context("target template disappeared during provisioning")?;
            if matches!(template, TargetTemplate::AwsEc2 { .. }) {
                for resource in &session.additional_mounts {
                    ensure!(
                        resource.source.is_dir(),
                        "attached resource source is not a directory: {}",
                        resource.source.display()
                    );
                }
            }
            let bundle = self
                .config
                .bundles
                .get(&session.bundle_id)
                .context("project bundle disappeared during provisioning")?;
            let target = backend_target(template, session.resource_allocation.as_ref())?;
            let bundle = backend_bundle(bundle)?;
            let runtime_mounts = if matches!(target, hel_targets::TargetTemplate::AwsEc2(_)) {
                &[][..]
            } else {
                session.additional_mounts.as_slice()
            };
            let provision =
                hel_targets::provision_plan(&target, session_id, &bundle, runtime_mounts)?;

            let outputs =
                preflight_target(template, executor).and_then(|()| provision.execute(executor))?;
            locator_after_provision(
                template,
                &target,
                session_id,
                outputs.first(),
                executor,
                &bundle,
            )
            .map_err(|error| {
                match cleanup_failed_provision(template, session_id, outputs.first(), executor) {
                    Some(note) => error.context(note),
                    None => error,
                }
            })
        })();
        let result = apply_new_session_provisioning_result(&mut self.state, session_id, result);
        self.state.save()?;
        result
    }

    pub fn mark_worker_connected(
        &mut self,
        session_id: &str,
        native_session_id: Option<String>,
    ) -> Result<()> {
        let session = self
            .state
            .sessions
            .get_mut(session_id)
            .with_context(|| format!("unknown session {session_id}"))?;
        if session.target.is_none() {
            bail!("session {session_id} has no provisioned target");
        }
        session.state = SessionState::Running;
        if native_session_id.is_some() {
            session.native_session_id = native_session_id;
        }
        session.updated_at = now();
        session.last_error = None;
        self.state.save()
    }

    async fn install_and_connect_worker(
        &self,
        session_id: &str,
        executor: &impl CommandExecutor,
    ) -> Result<Option<String>> {
        let (backend, worker_root) = self.prepare_worker_files(session_id, executor, true)?;
        install_attached_resources(&self.state, session_id, &backend, &worker_root, executor)?;
        self.connect_local_repositories(session_id, &backend, &worker_root, executor)?;
        start_worker(executor, &backend, &worker_root)?;
        match handshake_worker(&hel_targets::reconnect_plan(&backend, session_id)?.commands[0])
            .await
        {
            Ok(native_session_id) => Ok(native_session_id),
            Err(error) => Err(worker_probe_diagnosis(
                executor,
                &backend,
                &worker_root,
                error,
            )),
        }
    }

    fn prepare_worker_files(
        &self,
        session_id: &str,
        executor: &impl CommandExecutor,
        recover_native_session: bool,
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
        let bundle = self
            .config
            .bundles
            .get(&session.bundle_id)
            .context("session bundle is missing")?;
        let locator = session
            .target
            .as_ref()
            .context("session target is missing")?;
        let backend = backend_locator(locator, session, &self.config)?;
        let worker_root = hel_targets::worker_root(&backend, session_id)?;
        let target_profile_home = match backend {
            hel_targets::TargetLocator::LocalPodman { .. }
            | hel_targets::TargetLocator::AppleContainer { .. }
            | hel_targets::TargetLocator::SshPodman { .. } => {
                format!("/var/lib/hel/profiles/{session_id}")
            }
            hel_targets::TargetLocator::AwsEc2 { .. }
            | hel_targets::TargetLocator::SshBare { .. } => {
                format!(".local/share/hel/profiles/{session_id}")
            }
        };
        let workspace = workspace_paths(&backend, bundle, session_id)?;
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
        let (bridge_command, bridge_args) =
            bridge_launch(profile.kind, profile.executable.as_deref());
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
            recover_native_session,
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
        stage_profile(profile, &profile_stage)?;
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

    fn connect_local_repositories(
        &self,
        session_id: &str,
        backend: &hel_targets::TargetLocator,
        worker_root: &str,
        executor: &impl CommandExecutor,
    ) -> Result<()> {
        let session = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?;
        let bundle = self
            .config
            .bundles
            .get(&session.bundle_id)
            .context("session bundle is missing")?;
        let local = bundle
            .repositories
            .iter()
            .filter_map(|repository| repository.local.as_ref().map(|path| (repository, path)))
            .collect::<Vec<_>>();
        if local.is_empty() {
            return Ok(());
        }

        let absolute_worker_root =
            absolute_target_path(executor, backend, session_id, worker_root)?;
        let repositories = local
            .iter()
            .map(|(repository, path)| Ok((repository.id.clone(), canonical_repository(path)?)))
            .collect::<Result<BTreeMap<_, _>>>()?;
        ensure_git_broker(session_id, backend, repositories)?;

        let workspace_root = match backend {
            hel_targets::TargetLocator::LocalPodman { .. }
            | hel_targets::TargetLocator::AppleContainer { .. }
            | hel_targets::TargetLocator::SshPodman { .. } => "/workspace".to_owned(),
            hel_targets::TargetLocator::AwsEc2 { workspace, .. }
            | hel_targets::TargetLocator::SshBare { workspace, .. } => workspace.clone(),
        };
        let mut missing = Vec::new();
        for &(repository, source) in &local {
            local_branch(source)?;
            let destination = format!(
                "{workspace_root}/{}",
                repository.destination.to_string_lossy()
            );
            let origin = local_origin_url(&absolute_worker_root, &repository.id);
            for (args, purpose) in [
                (
                    vec![
                        "git".into(),
                        "-C".into(),
                        destination.clone(),
                        "config".into(),
                        "protocol.ext.allow".into(),
                        "always".into(),
                    ],
                    "enable the confined local Git transport",
                ),
                (
                    vec![
                        "git".into(),
                        "-C".into(),
                        destination.clone(),
                        "config".into(),
                        "remote.origin.url".into(),
                        origin,
                    ],
                    "configure local Git origin",
                ),
                (
                    vec![
                        "git".into(),
                        "-C".into(),
                        destination.clone(),
                        "config".into(),
                        "remote.origin.fetch".into(),
                        "+refs/heads/*:refs/remotes/origin/*".into(),
                    ],
                    "configure local Git fetch refspec",
                ),
            ] {
                execute_checked(
                    executor,
                    hel_targets::command_on_locator(backend, session_id, args, purpose)?,
                )?;
            }
            let has_head = executor.execute(&hel_targets::command_on_locator(
                backend,
                session_id,
                vec![
                    "git".into(),
                    "-C".into(),
                    destination.clone(),
                    "rev-parse".into(),
                    "--verify".into(),
                    "HEAD".into(),
                ],
                "inspect local Git bootstrap state",
            )?)?;
            if has_head.status != 0 {
                missing.push((repository, source));
            }
        }
        if !missing.is_empty() {
            restore_local_repository_seed(
                executor,
                backend,
                session,
                bundle,
                &workspace_root,
                worker_root,
                &missing,
            )?;
        }
        for (repository, _) in local {
            let destination = format!(
                "{workspace_root}/{}",
                repository.destination.to_string_lossy()
            );
            execute_checked(
                executor,
                hel_targets::command_on_locator(
                    backend,
                    session_id,
                    vec![
                        "git".into(),
                        "-C".into(),
                        destination.clone(),
                        "fetch".into(),
                        "origin".into(),
                    ],
                    "fetch local Git origin",
                )?,
            )?;
        }
        Ok(())
    }

    /// Collect the dead worker's exit record and log tail for a session whose
    /// worker has become unreachable. Best-effort; returns None when the
    /// target no longer exists or has no diagnostics.
    pub fn diagnose_worker(&self, session_id: &str) -> Option<String> {
        let session = self.state.sessions.get(session_id)?;
        let locator = session.target.as_ref()?;
        let backend = backend_locator(locator, session, &self.config).ok()?;
        let worker_root = hel_targets::worker_root(&backend, session_id).ok()?;
        worker_last_words(&ProcessExecutor, &backend, &worker_root)
    }

    pub fn reconnect_command(&self, session_id: &str) -> Result<CommandSpec> {
        let session = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?;
        let locator = session.target.as_ref().context("session has no target")?;
        let backend = backend_locator(locator, session, &self.config)?;
        hel_targets::reconnect_plan(&backend, session_id)?
            .commands
            .into_iter()
            .next()
            .context("reconnect plan is empty")
    }

    pub fn resource_probe(&self, session_id: &str) -> Result<hel_targets::SessionResourceProbe> {
        let session = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?;
        let locator = session.target.as_ref().context("session has no target")?;
        let backend = backend_locator(locator, session, &self.config)?;
        hel_targets::resource_probe(&backend, session_id)
    }

    pub fn deployment_capacity_targets(&self) -> Vec<hel_targets::DeploymentCapacityTarget> {
        use hel_targets::{DeploymentCapacityKind, DeploymentCapacityTarget};

        let mut local_ids = Vec::new();
        let mut ssh_hosts: BTreeMap<String, (Vec<String>, Vec<CommandSpec>)> = BTreeMap::new();
        let mut targets = Vec::new();
        for (target_id, template) in &self.config.targets {
            match template {
                TargetTemplate::LocalPodman { .. } | TargetTemplate::AppleContainer { .. } => {
                    local_ids.push(target_id.clone());
                }
                TargetTemplate::SshBare { ssh, .. } | TargetTemplate::SshPodman { ssh, .. } => {
                    let entry = ssh_hosts.entry(ssh.host.clone()).or_default();
                    entry.0.push(target_id.clone());
                    let command = hel_targets::ssh_host_capacity_command(&backend_ssh(ssh));
                    if !entry.1.contains(&command) {
                        entry.1.push(command);
                    }
                }
                TargetTemplate::AwsEc2 { .. } => {
                    let mut probes = Vec::new();
                    let mut probe_error = None;
                    for session in self.state.sessions.values().filter(|session| {
                        session.target_template_id == *target_id
                            && session.state.is_active()
                            && session.target.is_some()
                    }) {
                        let result = backend_locator(
                            session.target.as_ref().expect("filtered target"),
                            session,
                            &self.config,
                        )
                        .and_then(|locator| {
                            hel_targets::aws_allocated_capacity_command(&locator, &session.id)
                        });
                        match result {
                            Ok(command) => probes.push(command),
                            Err(error) => probe_error = Some(format!("{error:#}")),
                        }
                    }
                    targets.push(DeploymentCapacityTarget {
                        id: format!("aws:{target_id}"),
                        host: target_id.clone(),
                        target_ids: vec![target_id.clone()],
                        kind: DeploymentCapacityKind::AwsFleet,
                        local: false,
                        probes,
                        probe_error,
                    });
                }
            }
        }
        if !local_ids.is_empty() {
            targets.push(DeploymentCapacityTarget {
                id: "local".into(),
                host: "local".into(),
                target_ids: local_ids,
                kind: DeploymentCapacityKind::Host,
                local: true,
                probes: Vec::new(),
                probe_error: None,
            });
        }
        targets.extend(ssh_hosts.into_iter().map(|(host, (target_ids, probes))| {
            DeploymentCapacityTarget {
                id: format!("ssh:{host}"),
                host,
                target_ids,
                kind: DeploymentCapacityKind::Host,
                local: false,
                probes,
                probe_error: None,
            }
        }));
        targets.sort_by(|left, right| left.id.cmp(&right.id));
        targets
    }

    /// Resume an archived logical session on any configured profile and
    /// target. Cross-harness resume restores Git and canonical history, starts
    /// a fresh native session, and supplies the prior transcript as its first
    /// context turn.
    pub async fn resume_session(
        &mut self,
        session_id: &str,
        profile_id: &str,
        target_id: &str,
    ) -> Result<()> {
        self.resume_session_with_resources(session_id, profile_id, target_id, None)
            .await
    }

    pub async fn resume_session_with_resources(
        &mut self,
        session_id: &str,
        profile_id: &str,
        target_id: &str,
        resource_allocation: Option<SessionResourceAllocation>,
    ) -> Result<()> {
        self.resume_session_with_options(
            session_id,
            profile_id,
            target_id,
            None,
            resource_allocation,
        )
        .await
        .map(|_| ())
    }

    pub async fn resume_session_with_options(
        &mut self,
        session_id: &str,
        profile_id: &str,
        target_id: &str,
        additional_mounts: Option<Vec<AdditionalMount>>,
        resource_allocation: Option<SessionResourceAllocation>,
    ) -> Result<WorkerBootstrap> {
        let previous = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?
            .clone();
        if !matches!(previous.state, SessionState::Archived | SessionState::Error) {
            bail!("session {session_id} is not archived or retryable");
        }
        let checkpoint = previous
            .checkpoint
            .as_ref()
            .context("session has no checkpoint")?;
        let archive = read_archive_verified(&checkpoint.archive_path)?;
        if archive.archive_sha256 != checkpoint.sha256 || archive.manifest.session.id != session_id
        {
            bail!("persisted checkpoint verification failed");
        }
        let profile = self
            .config
            .profiles
            .get(profile_id)
            .with_context(|| format!("unknown profile {profile_id:?}"))?;
        let target_template = self
            .config
            .targets
            .get(target_id)
            .with_context(|| format!("unknown target template {target_id:?}"))?;
        let resource_allocation =
            resource_allocation.or_else(|| previous.resource_allocation.clone());
        let additional_mounts =
            additional_mounts.unwrap_or_else(|| previous.additional_mounts.clone());
        validate_resource_allocation(target_template, resource_allocation.as_ref())?;
        if !additional_mounts.is_empty() && mount_history_host(target_template).is_none() {
            bail!("attached resources are unsupported for this target");
        }
        hel_targets::validate_additional_mounts(&additional_mounts)?;
        let history_host = mount_history_host(target_template);
        let history_mounts = additional_mounts.clone();
        if previous.state == SessionState::Error
            && let Some(locator) = &previous.target
        {
            let backend = backend_locator(locator, &previous, &self.config)?;
            hel_targets::close_plan(&backend, session_id)?
                .execute(&ProcessExecutor)
                .context("clean up target from failed resume")?;
        }
        let same_harness = profile.kind == archive.manifest.session.harness_kind;
        let canonical_events = archive.payload_by_role(&PayloadRole::CanonicalEvents)?;
        let source_latest_seq = canonical_latest_sequence(canonical_events)?;
        let context_bytes = profile
            .context_window_bytes
            .unwrap_or(crate::hel_compaction::DEFAULT_CONTEXT_BYTES);
        let portable_events = (!same_harness).then(|| canonical_events.to_vec());

        let record = self.state.sessions.get_mut(session_id).unwrap();
        record.harness_kind = profile.kind;
        record.last_profile = profile_id.to_string();
        record.target_template_id = target_id.to_string();
        record.resource_allocation = resource_allocation;
        record.additional_mounts = additional_mounts;
        record.target = None;
        record.native_session_id =
            same_harness.then(|| archive.manifest.session.native_session_id.clone());
        record.state = SessionState::Provisioning;
        record.updated_at = now();
        record.last_error = None;
        if let Some(host) = history_host {
            self.state.remember_mount_sources(&host, &history_mounts);
        }
        self.state.save()?;

        let result = async {
            self.provision_session_with(session_id, &ProcessExecutor)
                .await?;
            let (backend, worker_root) =
                self.prepare_worker_files(session_id, &ProcessExecutor, same_harness)?;
            let harness_home = target_profile_home(&backend, session_id);
            let workspace_root = match &backend {
                hel_targets::TargetLocator::LocalPodman { .. }
                | hel_targets::TargetLocator::AppleContainer { .. }
                | hel_targets::TargetLocator::SshPodman { .. } => "/workspace".to_string(),
                hel_targets::TargetLocator::AwsEc2 { workspace, .. }
                | hel_targets::TargetLocator::SshBare { workspace, .. } => workspace.clone(),
            };
            let target_path = |path: &str| match &backend {
                hel_targets::TargetLocator::AwsEc2 { .. }
                | hel_targets::TargetLocator::SshBare { .. } => PathBuf::from(format!("~/{path}")),
                _ => PathBuf::from(path),
            };
            let remote_archive = format!("{worker_root}/restore.hel.zip");
            let remote_spec = format!("{worker_root}/restore-spec.json");
            let restore = CheckpointRestoreSpec {
                archive_path: target_path(&remote_archive),
                workspace_root: target_path(&workspace_root),
                worker_root: target_path(&worker_root),
                harness_home: target_path(&harness_home),
                restore_native: same_harness,
            };
            let staging = tempfile::tempdir().context("create restore staging")?;
            let local_spec = staging.path().join("restore-spec.json");
            std::fs::write(&local_spec, serde_json::to_vec_pretty(&restore)?)?;
            upload_checkpoint_spec(
                &ProcessExecutor,
                &backend,
                session_id,
                &checkpoint.archive_path,
                &remote_archive,
            )?;
            upload_checkpoint_spec(
                &ProcessExecutor,
                &backend,
                session_id,
                &local_spec,
                &remote_spec,
            )?;
            execute_checked(
                &ProcessExecutor,
                restore_command(&backend, session_id, &remote_spec)?,
            )?;
            install_attached_resources(
                &self.state,
                session_id,
                &backend,
                &worker_root,
                &ProcessExecutor,
            )?;
            self.connect_local_repositories(
                session_id,
                &backend,
                &worker_root,
                &ProcessExecutor,
            )?;
            start_worker(&ProcessExecutor, &backend, &worker_root)?;
            handshake_worker(&hel_targets::reconnect_plan(&backend, session_id)?.commands[0])
                .await?;
            let spec = self.reconnect_command(session_id)?;
            let mut client = WorkerClient::connect(&spec, session_id).await?;
            let (native_session_id, resumed) =
                wait_for_session_started(&mut client, source_latest_seq).await?;
            if same_harness {
                if !resumed {
                    bail!("same-harness resume started a fresh ACP session instead of loading the copied native session");
                }
                if native_session_id != archive.manifest.session.native_session_id {
                    bail!(
                        "ACP loaded native session {native_session_id}, expected {}",
                        archive.manifest.session.native_session_id
                    );
                }
            } else {
                if resumed {
                    bail!("cross-harness resume unexpectedly loaded a native source session");
                }
                let events = portable_events
                    .as_deref()
                    .context("cross-harness resume is missing canonical events")?;
                let context = crate::hel_compaction::compact_events(
                    events,
                    context_bytes,
                    &mut client,
                )
                .await?;
                client
                    .prompt(
                        context,
                        vec![crate::hel_worker::Attachment {
                            name: "cross-harness-handoff".into(),
                            media_type: crate::hel_compaction::HANDOFF_MEDIA_TYPE.into(),
                            reference: "synthetic".into(),
                        }],
                    )
                    .await?;
            }
            self.mark_worker_connected(session_id, Some(native_session_id))?;
            let bootstrap = client.bootstrap().await?;
            client.detach().await?;
            Ok::<_, anyhow::Error>(bootstrap)
        }
        .await;
        match result {
            Ok(bootstrap) => Ok(bootstrap),
            Err(error) => {
                Err(self.rollback_failed_resume(session_id, &previous, error, &ProcessExecutor)?)
            }
        }
    }

    fn rollback_failed_resume(
        &mut self,
        session_id: &str,
        previous: &SessionRecord,
        error: anyhow::Error,
        executor: &impl CommandExecutor,
    ) -> Result<anyhow::Error> {
        let current = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?
            .clone();
        let cleanup = match current.target.as_ref() {
            Some(locator) => (|| -> Result<()> {
                let backend = backend_locator(locator, &current, &self.config)?;
                hel_targets::close_plan(&backend, session_id)?
                    .execute(executor)
                    .map(|_| ())
            })(),
            None => Ok(()),
        };
        let original = format!("{error:#}");
        let record = self.state.sessions.get_mut(session_id).unwrap();
        let failure = apply_failed_resume_rollback(
            record,
            previous,
            &original,
            cleanup
                .err()
                .map(|cleanup_error| format!("{cleanup_error:#}")),
        );
        self.state.save()?;
        Ok(failure)
    }

    /// Materialize and locally verify a complete session checkpoint while the
    /// target remains live. A failed export or transfer leaves the previous
    /// archive and target untouched.
    pub async fn checkpoint_session(&mut self, session_id: &str) -> Result<CheckpointMetadata> {
        let previous = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?
            .clone();
        let record = self.state.sessions.get_mut(session_id).unwrap();
        record.state = SessionState::Checkpointing;
        record.updated_at = now();
        record.last_checkpoint_error = None;
        self.state.save()?;

        match self
            .checkpoint_session_with(session_id, &ProcessExecutor)
            .await
        {
            Ok(artifact) => {
                let record = self.state.sessions.get_mut(session_id).unwrap();
                record.state = SessionState::Running;
                record.native_session_id = Some(artifact.native_session_id);
                record.checkpoint = Some(artifact.metadata.clone());
                record.updated_at = now();
                record.last_error = None;
                record.last_checkpoint_error = None;
                self.state.save()?;
                prune_replaced_checkpoint(previous.checkpoint.as_ref(), &artifact.metadata);
                Ok(artifact.metadata)
            }
            Err(error) => {
                if let Some(record) = self.state.sessions.get_mut(session_id) {
                    record.state = if previous.state == SessionState::Checkpointing {
                        SessionState::Running
                    } else {
                        previous.state
                    };
                    record.updated_at = now();
                    record.last_checkpoint_error = Some(format!("{error:#}"));
                    self.state.save()?;
                }
                Err(error)
            }
        }
    }

    /// Create and verify a recovery archive without mutating controller state.
    /// The caller is responsible for installing the returned metadata.
    pub async fn create_recovery_checkpoint(&self, session_id: &str) -> Result<CheckpointArtifact> {
        self.checkpoint_session_with(session_id, &ProcessExecutor)
            .await
    }

    async fn checkpoint_session_with(
        &self,
        session_id: &str,
        executor: &(impl CommandExecutor + Sync),
    ) -> Result<CheckpointArtifact> {
        let session = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?
            .clone();
        let locator = session
            .target
            .as_ref()
            .context("session has no live target")?;
        let backend = backend_locator(locator, &session, &self.config)?;
        let bundle = self
            .config
            .bundles
            .get(&session.bundle_id)
            .context("session bundle is missing")?;
        let reconnect = hel_targets::reconnect_plan(&backend, session_id)?
            .commands
            .into_iter()
            .next()
            .context("reconnect plan is empty")?;
        let mut client = WorkerClient::connect(&reconnect, session_id).await?;
        let expected_sequence = client
            .checkpoint(Some("controller archive checkpoint".into()))
            .await?;
        let bootstrap = client.bootstrap().await?;
        let native_session_id = native_session_id_from_events(&bootstrap.events)
            .or_else(|| session.native_session_id.clone())
            .context("harness did not report its native session ID")?;
        client.detach().await?;

        let worker_root = hel_targets::worker_root(&backend, session_id)?;
        let harness_home = target_profile_home(&backend, session_id);
        let workspace_root = match &backend {
            hel_targets::TargetLocator::LocalPodman { .. }
            | hel_targets::TargetLocator::AppleContainer { .. }
            | hel_targets::TargetLocator::SshPodman { .. } => "/workspace".to_string(),
            hel_targets::TargetLocator::AwsEc2 { workspace, .. }
            | hel_targets::TargetLocator::SshBare { workspace, .. } => workspace.clone(),
        };
        let target_path = |path: &str| match &backend {
            hel_targets::TargetLocator::AwsEc2 { .. }
            | hel_targets::TargetLocator::SshBare { .. } => PathBuf::from(format!("~/{path}")),
            _ => PathBuf::from(path),
        };
        let remote_spec = format!("{worker_root}/checkpoint-spec.json");
        let remote_archive = format!("{worker_root}/checkpoint.hel.zip");
        let checkpointed_at = now();
        let spec = CheckpointExportSpec {
            session: SessionManifest {
                id: session.id.clone(),
                title: session.title.clone(),
                harness_kind: session.harness_kind,
                profile_id: session.last_profile.clone(),
                native_session_id: native_session_id.clone(),
                created_at: session.created_at.clone(),
                checkpointed_at: checkpointed_at.clone(),
                hel_version: env!("CARGO_PKG_VERSION").into(),
                worker_version: env!("CARGO_PKG_VERSION").into(),
                adapter_version: "acp-v1".into(),
            },
            target: TargetManifest {
                template_id: session.target_template_id.clone(),
                target_kind: target_kind(&backend).into(),
                details: Default::default(),
            },
            bundle: BundleManifest {
                id: session.bundle_id.clone(),
                primary_repository: bundle.primary_repo.clone(),
            },
            worker_root: target_path(&worker_root),
            harness_home: target_path(&harness_home),
            workspace_root: target_path(&workspace_root),
            repositories: bundle
                .repositories
                .iter()
                .map(|repository| CheckpointRepositorySpec {
                    id: repository.id.clone(),
                    relative_destination: repository.destination.clone(),
                    base_commit: if repository.is_local() {
                        "refs/hel/base".into()
                    } else {
                        repository
                            .git_ref
                            .as_deref()
                            .map(|git_ref| format!("origin/{git_ref}"))
                            .unwrap_or_else(|| "origin/HEAD".into())
                    },
                    full_history: repository.is_local(),
                    origin_override: repository
                        .is_local()
                        .then(|| format!("hel-local:{}", repository.id)),
                })
                .collect(),
            event_sequence: expected_sequence,
            output_path: target_path(&remote_archive),
        };
        let staging = tempfile::tempdir().context("create checkpoint staging")?;
        let local_spec = staging.path().join("checkpoint-spec.json");
        spec.write(&local_spec)?;
        upload_checkpoint_spec(executor, &backend, session_id, &local_spec, &remote_spec)?;
        let exported = execute_checked(
            executor,
            export_command(&backend, session_id, &remote_spec)?,
        )?;
        let target_checkpoint: crate::hel_checkpoint::TargetCheckpoint =
            serde_json::from_slice(&exported.stdout).context("decode target checkpoint result")?;
        if target_checkpoint.event_sequence != expected_sequence {
            bail!(
                "target checkpoint event frontier changed: expected {expected_sequence}, found {}",
                target_checkpoint.event_sequence
            );
        }

        let destination = sessions_dir().join(format!(
            "{session_id}-{}.hel.zip",
            target_checkpoint.event_sequence
        ));
        let transfer = CheckpointTransfer {
            locator: &backend,
            session_id,
            remote_archive: &remote_archive,
            destination: &destination,
            expected_event_sequence: Some(target_checkpoint.event_sequence),
        };
        let verified = transfer.execute(executor)?;
        if verified.sha256() != target_checkpoint.sha256 {
            bail!("target and controller checkpoint checksums differ");
        }
        transfer.cleanup_plan(&verified)?.execute(executor)?;
        let metadata = CheckpointMetadata {
            archive_path: verified.archive_path().to_path_buf(),
            sha256: verified.sha256().to_string(),
            created_at: checkpointed_at,
            event_sequence: verified.event_sequence(),
        };
        Ok(CheckpointArtifact {
            metadata,
            native_session_id,
        })
    }

    /// Checkpoint, ask the harness to close, and only then tear down the exact
    /// provisioned target. Checkpoint failure is deliberately non-destructive.
    pub async fn close_session(&mut self, session_id: &str) -> Result<()> {
        self.checkpoint_session(session_id).await?;
        let spec = self.reconnect_command(session_id)?;
        let mut client = WorkerClient::connect(&spec, session_id).await?;
        client.close().await?;
        client.detach().await?;
        self.destroy_after_verified_checkpoint(session_id, &ProcessExecutor)
    }

    /// Execute cleanup only after the close state machine has installed a
    /// verified checkpoint on the record.
    pub fn destroy_after_verified_checkpoint(
        &mut self,
        session_id: &str,
        executor: &impl CommandExecutor,
    ) -> Result<()> {
        let session = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?
            .clone();
        if session.checkpoint.is_none() {
            bail!("refusing to destroy session {session_id}: no verified checkpoint");
        }
        let locator = session.target.as_ref().context("session has no target")?;
        let backend = backend_locator(locator, &session, &self.config)?;
        hel_targets::close_plan(&backend, session_id)?.execute(executor)?;
        let record = self.state.sessions.get_mut(session_id).unwrap();
        record.state = SessionState::Archived;
        record.target = None;
        record.updated_at = now();
        record.last_error = None;
        self.state.save()
    }

    pub fn force_destroy(
        &mut self,
        session_id: &str,
        executor: &impl CommandExecutor,
    ) -> Result<()> {
        let session = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?
            .clone();
        if let Some(locator) = &session.target {
            let backend = backend_locator(locator, &session, &self.config)?;
            hel_targets::close_plan(&backend, session_id)?.execute(executor)?;
        }
        let record = self.state.sessions.get_mut(session_id).unwrap();
        record.state = SessionState::DestroyedWithDataLoss;
        record.target = None;
        record.updated_at = now();
        self.state.save()
    }
}

fn apply_new_session_provisioning_result(
    state: &mut HelState,
    session_id: &str,
    result: Result<TargetLocator>,
) -> Result<()> {
    match result {
        Ok(locator) => {
            let record = state.sessions.get_mut(session_id).unwrap();
            record.target = Some(locator);
            // Provisioning has completed, but Running is reserved for a
            // successful worker handshake.
            record.state = SessionState::Disconnected;
            record.updated_at = now();
            record.last_error = None;
            Ok(())
        }
        Err(error) => {
            state.sessions.remove(session_id);
            Err(error)
        }
    }
}

fn apply_failed_resume_rollback(
    current: &mut SessionRecord,
    previous: &SessionRecord,
    original_error: &str,
    cleanup_error: Option<String>,
) -> anyhow::Error {
    match cleanup_error {
        None => {
            *current = previous.clone();
            current.state = SessionState::Archived;
            current.target = None;
            current.updated_at = now();
            let failure = format!(
                "{original_error}; partial target removed and session returned to archived"
            );
            current.last_error = Some(format!("resume failed: {failure}"));
            anyhow::anyhow!(failure)
        }
        Some(cleanup_error) => {
            let failure = format!(
                "{original_error}; cleanup of the partial resume target failed: {cleanup_error}"
            );
            current.state = SessionState::Error;
            current.updated_at = now();
            current.last_error = Some(format!("resume failed: {failure}"));
            anyhow::anyhow!(failure)
        }
    }
}

fn allocation_vcpus(allocation: &SessionResourceAllocation) -> u64 {
    match allocation {
        SessionResourceAllocation::Container { cpus, .. } => *cpus,
        SessionResourceAllocation::AwsEc2 { vcpus, .. } => *vcpus,
    }
}

fn preflight_target(template: &TargetTemplate, executor: &impl CommandExecutor) -> Result<()> {
    match template {
        TargetTemplate::LocalPodman { .. } => hel_targets::verify_local_podman(executor)
            .map(|_| ())
            .map_err(|error| {
                anyhow::anyhow!(
                    "local Podman preflight failed; run `hel doctor` for actionable prerequisites: {error:#}"
                )
            }),
        TargetTemplate::AppleContainer { .. } => {
            let command = CommandSpec::new("container", ["system", "status"])
                .purpose("preflight Apple container runtime");
            let output = executor.execute(&command).map_err(|error| {
                anyhow::anyhow!(
                    "Apple container preflight failed; run `hel doctor` for actionable prerequisites: {error}"
                )
            })?;
            if output.status != 0 {
                bail!(
                    "Apple container preflight failed; run `hel doctor` for actionable prerequisites: container system status exited {}: {}",
                    output.status,
                    String::from_utf8_lossy(&output.stderr).trim()
                );
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn native_session_id_from_events(events: &[crate::hel_worker::SequencedEvent]) -> Option<String> {
    crate::hel_worker::recover_native_session_id(events)
}

fn canonical_latest_sequence(bytes: &[u8]) -> Result<u64> {
    let mut latest = 0;
    for line in bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let event: crate::hel_worker::SequencedEvent = serde_json::from_slice(line)?;
        latest = latest.max(event.seq);
    }
    Ok(latest)
}

async fn wait_for_session_started(
    client: &mut WorkerClient,
    mut cursor: u64,
) -> Result<(String, bool)> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let events = client.replay_after(cursor).await?;
        for event in events {
            cursor = cursor.max(event.seq);
            let WorkerEvent::Adapter { payload, .. } = event.event else {
                continue;
            };
            match serde_json::from_value::<crate::hel_acp::RuntimeEvent>(payload) {
                Ok(crate::hel_acp::RuntimeEvent::SessionStarted {
                    native_session_id,
                    resumed,
                    ..
                }) => return Ok((native_session_id, resumed)),
                Ok(crate::hel_acp::RuntimeEvent::Stopped) => {
                    bail!("ACP runtime stopped before starting its session")
                }
                _ => {}
            }
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("ACP runtime did not report session startup");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
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

fn target_kind(locator: &hel_targets::TargetLocator) -> &'static str {
    match locator {
        hel_targets::TargetLocator::LocalPodman { .. } => "local-podman",
        hel_targets::TargetLocator::AppleContainer { .. } => "apple-container",
        hel_targets::TargetLocator::AwsEc2 { .. } => "aws-ec2",
        hel_targets::TargetLocator::SshBare { .. } => "ssh-bare",
        hel_targets::TargetLocator::SshPodman { .. } => "ssh-podman",
    }
}

fn target_profile_home(locator: &hel_targets::TargetLocator, session_id: &str) -> String {
    match locator {
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

fn upload_checkpoint_spec(
    executor: &impl CommandExecutor,
    locator: &hel_targets::TargetLocator,
    session_id: &str,
    local: &Path,
    remote: &str,
) -> Result<()> {
    match locator {
        hel_targets::TargetLocator::LocalPodman { container_id } => execute_checked(
            executor,
            CommandSpec::new(
                "podman",
                [
                    "cp".into(),
                    local.to_string_lossy().into_owned(),
                    format!("{container_id}:{remote}"),
                ],
            )
            .purpose("upload checkpoint specification"),
        ),
        hel_targets::TargetLocator::AppleContainer { container_id } => execute_checked(
            executor,
            CommandSpec::new(
                "container",
                [
                    "cp".into(),
                    local.to_string_lossy().into_owned(),
                    format!("{container_id}:{remote}"),
                ],
            )
            .purpose("upload checkpoint specification"),
        ),
        hel_targets::TargetLocator::AwsEc2 { ssh, .. }
        | hel_targets::TargetLocator::SshBare { ssh, .. } => execute_checked(
            executor,
            scp_command_spec(ssh, local, remote, false).purpose("upload checkpoint specification"),
        ),
        hel_targets::TargetLocator::SshPodman { ssh, container_id } => {
            let staging = format!(".local/share/hel/uploads/{session_id}-checkpoint.json");
            execute_checked(
                executor,
                ssh_command_spec(ssh, ["mkdir", "-p", ".local/share/hel/uploads"])
                    .purpose("create remote checkpoint staging"),
            )?;
            execute_checked(
                executor,
                scp_command_spec(ssh, local, &staging, false)
                    .purpose("upload remote Podman checkpoint specification"),
            )?;
            execute_checked(
                executor,
                ssh_command_spec(
                    ssh,
                    [
                        "podman",
                        "cp",
                        &staging,
                        &format!("{container_id}:{remote}"),
                    ],
                )
                .purpose("install remote Podman checkpoint specification"),
            )?;
            execute_checked(
                executor,
                ssh_command_spec(ssh, ["rm", "-f", "--", &staging])
                    .purpose("remove remote checkpoint staging"),
            )
        }
    }?;
    Ok(())
}

fn backend_bundle(bundle: &ProjectBundle) -> Result<ProjectBundleSpec> {
    let primary = bundle.primary().context("bundle primary is missing")?;
    Ok(ProjectBundleSpec {
        primary: primary.destination.to_string_lossy().into_owned(),
        repositories: bundle
            .repositories
            .iter()
            .map(|repository| RepositorySpec {
                url: repository.github.as_deref().map(github_url),
                destination: repository.destination.to_string_lossy().into_owned(),
                git_ref: repository.git_ref.clone(),
            })
            .collect(),
    })
}

fn github_url(source: &str) -> String {
    if source.contains("://") || source.starts_with("git@") {
        source.to_string()
    } else {
        format!("https://github.com/{}.git", source.trim_end_matches(".git"))
    }
}

fn scan_target_workers(
    target_id: &str,
    template: &TargetTemplate,
    executor: &impl CommandExecutor,
) -> Result<Vec<RecoveryCandidate>> {
    let mut candidates = match template {
        TargetTemplate::LocalPodman { .. } => scan_container_engine(
            target_id,
            template,
            "podman",
            vec![
                "ps".into(),
                "--all".into(),
                "--filter".into(),
                format!("label={}=true", hel_targets::MANAGED_LABEL),
                "--format".into(),
                "json".into(),
            ],
            executor,
        )?,
        TargetTemplate::AppleContainer { .. } => scan_container_engine(
            target_id,
            template,
            "container",
            vec![
                "list".into(),
                "--all".into(),
                "--format".into(),
                "json".into(),
            ],
            executor,
        )?,
        TargetTemplate::SshPodman { ssh, .. } => {
            let remote = hel_targets::join_remote_command(&[
                "podman".into(),
                "ps".into(),
                "--all".into(),
                "--filter".into(),
                format!("label={}=true", hel_targets::MANAGED_LABEL),
                "--format".into(),
                "json".into(),
            ]);
            let output = execute_scan(
                executor,
                ssh_spec(ssh, [remote]),
                "scan remote Podman workers",
            )?;
            candidates_from_container_json(target_id, template, &output.stdout)?
        }
        TargetTemplate::AwsEc2 {
            aws_profile,
            region,
            address_source,
            ..
        } => {
            let profile = aws_profile.clone().unwrap_or_else(|| "default".into());
            let output = execute_scan(
                executor,
                CommandSpec::new(
                    "aws",
                    [
                        "--profile".into(),
                        profile,
                        "--region".into(),
                        region.clone(),
                        "ec2".into(),
                        "describe-instances".into(),
                        "--filters".into(),
                        format!("Name=tag:{},Values=true", hel_targets::MANAGED_TAG),
                        "Name=instance-state-name,Values=pending,running,stopping,stopped".into(),
                        "--output".into(),
                        "json".into(),
                    ],
                )
                .purpose("scan managed EC2 workers"),
                "scan managed EC2 workers",
            )?;
            candidates_from_aws_json(target_id, address_source.clone(), &output.stdout)?
        }
        TargetTemplate::SshBare { ssh, .. } => {
            let output = execute_scan(
                executor,
                ssh_spec(
                    ssh,
                    [hel_targets::join_remote_command(&[
                        "find".into(),
                        ".local/share/hel/workers".into(),
                        "-mindepth".into(),
                        "2".into(),
                        "-maxdepth".into(),
                        "2".into(),
                        "-name".into(),
                        "ownership.json".into(),
                        "-print".into(),
                    ])],
                ),
                "scan bare SSH worker markers",
            )?;
            output
                .stdout
                .split(|byte| *byte == b'\n')
                .filter_map(|line| {
                    let path = std::str::from_utf8(line).ok()?.trim();
                    let session_id = Path::new(path).parent()?.file_name()?.to_str()?;
                    hel_targets::resource_name(session_id).ok()?;
                    let backend = backend_target(template, None).ok()?;
                    let workspace = hel_targets::workspace_for(&backend, session_id).ok()?;
                    Some(RecoveryCandidate {
                        session_id: session_id.to_owned(),
                        target_template_id: target_id.to_owned(),
                        locator: TargetLocator::SshBare {
                            host: ssh.host.clone(),
                            workspace: PathBuf::from(workspace),
                            worker_id: None,
                        },
                        ownership: None,
                    })
                })
                .collect()
        }
    };
    for candidate in &mut candidates {
        candidate.ownership = read_recovery_ownership(template, candidate, executor);
    }
    Ok(candidates)
}

fn scan_container_engine(
    target_id: &str,
    template: &TargetTemplate,
    engine: &str,
    args: Vec<String>,
    executor: &impl CommandExecutor,
) -> Result<Vec<RecoveryCandidate>> {
    let output = execute_scan(
        executor,
        CommandSpec::new(engine, args).purpose("scan managed container workers"),
        "scan managed container workers",
    )?;
    candidates_from_container_json(target_id, template, &output.stdout)
}

fn candidates_from_container_json(
    target_id: &str,
    template: &TargetTemplate,
    stdout: &[u8],
) -> Result<Vec<RecoveryCandidate>> {
    let value: serde_json::Value =
        serde_json::from_slice(stdout).context("parse container list JSON")?;
    let mut sessions = Vec::new();
    collect_managed_sessions(&value, &mut sessions);
    sessions.sort();
    sessions.dedup();
    Ok(sessions
        .into_iter()
        .filter_map(|session_id| {
            let generated = hel_targets::resource_name(&session_id).ok()?;
            let locator = match template {
                TargetTemplate::LocalPodman { .. } => TargetLocator::LocalPodman {
                    container_id: generated,
                },
                TargetTemplate::AppleContainer { .. } => TargetLocator::AppleContainer {
                    container_id: generated,
                },
                TargetTemplate::SshPodman { ssh, .. } => TargetLocator::SshPodman {
                    host: ssh.host.clone(),
                    container_id: generated,
                },
                _ => return None,
            };
            Some(RecoveryCandidate {
                session_id,
                target_template_id: target_id.to_owned(),
                locator,
                ownership: None,
            })
        })
        .collect())
}

fn collect_managed_sessions(value: &serde_json::Value, sessions: &mut Vec<String>) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                collect_managed_sessions(value, sessions);
            }
        }
        serde_json::Value::Object(object) => {
            for label_key in ["Labels", "labels"] {
                if let Some(labels) = object.get(label_key) {
                    let managed = label_value(labels, hel_targets::MANAGED_LABEL)
                        .is_some_and(|value| value == "true");
                    if managed
                        && let Some(session) = label_value(labels, hel_targets::SESSION_LABEL)
                    {
                        sessions.push(session);
                    }
                }
            }
            for value in object.values() {
                collect_managed_sessions(value, sessions);
            }
        }
        _ => {}
    }
}

fn label_value(labels: &serde_json::Value, key: &str) -> Option<String> {
    match labels {
        serde_json::Value::Object(object) => object.get(key)?.as_str().map(str::to_owned),
        serde_json::Value::String(text) => text
            .split(',')
            .find_map(|label| {
                label
                    .trim()
                    .split_once('=')
                    .filter(|(name, _)| *name == key)
            })
            .map(|(_, value)| value.to_owned()),
        _ => None,
    }
}

fn candidates_from_aws_json(
    target_id: &str,
    address_source: AwsAddressSource,
    stdout: &[u8],
) -> Result<Vec<RecoveryCandidate>> {
    let value: serde_json::Value =
        serde_json::from_slice(stdout).context("parse AWS instance JSON")?;
    let mut result = Vec::new();
    let reservations = value
        .get("Reservations")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    for instance in reservations.iter().flat_map(|reservation| {
        reservation
            .get("Instances")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
    }) {
        let tags = instance
            .get("Tags")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();
        let tag = |key: &str| {
            tags.iter()
                .find(|tag| tag.get("Key").and_then(serde_json::Value::as_str) == Some(key))
                .and_then(|tag| tag.get("Value"))
                .and_then(serde_json::Value::as_str)
        };
        if tag(hel_targets::MANAGED_TAG) != Some("true") {
            continue;
        }
        let Some(session_id) = tag(hel_targets::SESSION_TAG).map(str::to_owned) else {
            continue;
        };
        hel_targets::resource_name(&session_id)?;
        let instance_id = instance
            .get("InstanceId")
            .and_then(serde_json::Value::as_str)
            .context("managed EC2 instance omitted InstanceId")?
            .to_owned();
        let field = match address_source {
            AwsAddressSource::PublicDns => "PublicDnsName",
            AwsAddressSource::PublicIp => "PublicIpAddress",
            AwsAddressSource::PrivateDns => "PrivateDnsName",
            AwsAddressSource::PrivateIp => "PrivateIpAddress",
        };
        let address = instance
            .get(field)
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_owned);
        result.push(RecoveryCandidate {
            session_id,
            target_template_id: target_id.to_owned(),
            locator: TargetLocator::AwsEc2 {
                instance_id,
                address,
            },
            ownership: None,
        });
    }
    Ok(result)
}

fn execute_scan(
    executor: &impl CommandExecutor,
    command: CommandSpec,
    operation: &str,
) -> Result<CommandOutput> {
    let output = executor.execute(&command)?;
    if output.status != 0 {
        bail!(
            "{operation} failed with status {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(output)
}

fn ssh_spec(ssh: &SshConnection, remote: impl IntoIterator<Item = String>) -> CommandSpec {
    let backend = backend_ssh(ssh);
    let mut args = backend.ssh_args;
    args.push(backend.destination);
    args.extend(remote);
    CommandSpec::new("ssh", args)
}

fn read_recovery_ownership(
    template: &TargetTemplate,
    candidate: &RecoveryCandidate,
    executor: &impl CommandExecutor,
) -> Option<WorkerOwnership> {
    let backend =
        recovery_backend_locator(template, &candidate.locator, &candidate.session_id).ok()?;
    let root = hel_targets::worker_root(&backend, &candidate.session_id).ok()?;
    let command = hel_targets::command_on_locator(
        &backend,
        &candidate.session_id,
        vec!["cat".into(), format!("{root}/ownership.json")],
        "read worker ownership marker",
    )
    .ok()?;
    let output = executor.execute(&command).ok()?;
    if output.status != 0 {
        return None;
    }
    let marker: WorkerOwnership = serde_json::from_slice(&output.stdout).ok()?;
    (marker.version == WorkerOwnership::VERSION
        && marker.session_id == candidate.session_id
        && marker.target_template_id == candidate.target_template_id)
        .then_some(marker)
}

fn recovery_backend_locator(
    template: &TargetTemplate,
    locator: &TargetLocator,
    session_id: &str,
) -> Result<hel_targets::TargetLocator> {
    Ok(match (template, locator) {
        (TargetTemplate::LocalPodman { .. }, TargetLocator::LocalPodman { container_id }) => {
            hel_targets::TargetLocator::LocalPodman {
                container_id: container_id.clone(),
            }
        }
        (TargetTemplate::AppleContainer { .. }, TargetLocator::AppleContainer { container_id }) => {
            hel_targets::TargetLocator::AppleContainer {
                container_id: container_id.clone(),
            }
        }
        (TargetTemplate::SshPodman { ssh, .. }, TargetLocator::SshPodman { container_id, .. }) => {
            hel_targets::TargetLocator::SshPodman {
                ssh: backend_ssh(ssh),
                container_id: container_id.clone(),
            }
        }
        (TargetTemplate::SshBare { ssh, .. }, TargetLocator::SshBare { workspace, .. }) => {
            hel_targets::TargetLocator::SshBare {
                ssh: backend_ssh(ssh),
                workspace: workspace.to_string_lossy().into_owned(),
            }
        }
        (
            TargetTemplate::AwsEc2 {
                aws_profile,
                region,
                ssh_user,
                identity_file,
                ssh_args,
                ..
            },
            TargetLocator::AwsEc2 {
                instance_id,
                address,
            },
        ) => hel_targets::TargetLocator::AwsEc2 {
            profile: aws_profile.clone().unwrap_or_else(|| "default".into()),
            region: region.clone(),
            instance_id: instance_id.clone(),
            ssh: SshTarget {
                destination: format!(
                    "{ssh_user}@{}",
                    address.as_deref().unwrap_or("unavailable.invalid")
                ),
                ssh_args: ssh_args_with_identity(ssh_args, identity_file.as_deref()),
            },
            workspace: format!(".local/share/hel/workspaces/{session_id}"),
        },
        _ => bail!("recovery target locator does not match target template"),
    })
}

fn backend_target(
    template: &TargetTemplate,
    allocation: Option<&SessionResourceAllocation>,
) -> Result<hel_targets::TargetTemplate> {
    Ok(match template {
        TargetTemplate::LocalPodman { container } => {
            hel_targets::TargetTemplate::LocalPodman(backend_container(container, allocation))
        }
        TargetTemplate::AppleContainer { container } => {
            hel_targets::TargetTemplate::AppleContainer(backend_container(container, allocation))
        }
        TargetTemplate::AwsEc2 {
            aws_profile,
            region,
            launch_template,
            launch_template_version,
            ssh_user,
            identity_file,
            ssh_args,
            ..
        } => hel_targets::TargetTemplate::AwsEc2(AwsTemplate {
            profile: aws_profile.clone().unwrap_or_else(|| "default".into()),
            region: region.clone(),
            launch_template: launch_template.clone(),
            launch_template_version: launch_template_version.clone(),
            instance_type: match allocation {
                Some(SessionResourceAllocation::AwsEc2 { instance_type, .. }) => {
                    Some(instance_type.clone())
                }
                _ => None,
            },
            // The address is filled after describe-instances.
            ssh: SshTarget {
                destination: format!("{ssh_user}@pending.invalid"),
                ssh_args: ssh_args_with_identity(ssh_args, identity_file.as_deref()),
            },
        }),
        TargetTemplate::SshBare {
            ssh,
            workspace_prefix,
        } => hel_targets::TargetTemplate::SshBare {
            ssh: backend_ssh(ssh),
            workspace_prefix: workspace_prefix.to_string_lossy().into_owned(),
        },
        TargetTemplate::SshPodman { ssh, container } => hel_targets::TargetTemplate::SshPodman {
            ssh: backend_ssh(ssh),
            container: backend_container(container, allocation),
        },
    })
}

fn mount_history_host(template: &TargetTemplate) -> Option<String> {
    match template {
        TargetTemplate::LocalPodman { .. }
        | TargetTemplate::AppleContainer { .. }
        | TargetTemplate::AwsEc2 { .. } => Some("local".into()),
        TargetTemplate::SshPodman { ssh, .. } => Some(ssh.host.clone()),
        TargetTemplate::SshBare { .. } => None,
    }
}

fn backend_container(
    container: &crate::hel_config::ContainerTemplate,
    allocation: Option<&SessionResourceAllocation>,
) -> ContainerTemplate {
    let mut extra_run_args = Vec::new();
    if let Some(platform) = &container.platform {
        extra_run_args.push(format!("--platform={platform}"));
    }
    if let Some(SessionResourceAllocation::Container { cpus, memory_bytes }) = allocation {
        extra_run_args.push(format!("--cpus={cpus}"));
        extra_run_args.push(format!("--memory={memory_bytes}"));
    } else {
        if let Some(cpus) = &container.cpus {
            extra_run_args.push(format!("--cpus={cpus}"));
        }
        if let Some(memory) = &container.memory {
            extra_run_args.push(format!("--memory={memory}"));
        }
    }
    for (key, value) in &container.environment {
        extra_run_args.extend(["--env".to_string(), format!("{key}={value}")]);
    }
    ContainerTemplate {
        image: container.image.clone(),
        extra_run_args,
    }
}

fn validate_resource_allocation(
    template: &TargetTemplate,
    allocation: Option<&SessionResourceAllocation>,
) -> Result<()> {
    match (template, allocation) {
        (_, None)
        | (
            TargetTemplate::LocalPodman { .. }
            | TargetTemplate::AppleContainer { .. }
            | TargetTemplate::SshPodman { .. },
            Some(SessionResourceAllocation::Container { .. }),
        )
        | (TargetTemplate::AwsEc2 { .. }, Some(SessionResourceAllocation::AwsEc2 { .. })) => Ok(()),
        (TargetTemplate::SshBare { .. }, Some(_)) => {
            bail!("bare SSH targets have fixed host resources")
        }
        _ => bail!("resource allocation does not match the selected target kind"),
    }
}

fn backend_ssh(ssh: &SshConnection) -> SshTarget {
    let destination = match &ssh.user {
        Some(user) => format!("{user}@{}", ssh.host),
        None => ssh.host.clone(),
    };
    SshTarget {
        destination,
        ssh_args: ssh_args_with_identity(&ssh.extra_args, ssh.identity_file.as_deref()),
    }
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

fn install_attached_resources(
    state: &HelState,
    session_id: &str,
    backend: &hel_targets::TargetLocator,
    worker_root: &str,
    executor: &impl CommandExecutor,
) -> Result<()> {
    let hel_targets::TargetLocator::AwsEc2 { .. } = backend else {
        return Ok(());
    };
    let session = state
        .sessions
        .get(session_id)
        .with_context(|| format!("unknown session {session_id}"))?;
    if session.additional_mounts.is_empty() {
        return Ok(());
    }
    for resource in &session.additional_mounts {
        let install = hel_targets::command_on_locator(
            backend,
            session_id,
            vec![
                format!("{worker_root}/hel"),
                "worker".into(),
                "install-resource".into(),
                "--destination".into(),
                resource.destination.to_string_lossy().into_owned(),
            ],
            "stream attached resource",
        )?;
        crate::hel_resources::stream_resource(&resource.source, |stream| {
            execute_checked_with_stdin(executor, &install, stream).map(|_| ())
        })
        .with_context(|| format!("stream attached resource {}", resource.source.display()))?;
    }
    Ok(())
}

/// Best-effort teardown of a freshly created resource whose provisioning
/// failed before a locator was recorded. Returns a note describing what
/// happened for inclusion in the session error.
fn cleanup_failed_provision(
    canonical: &TargetTemplate,
    session_id: &str,
    first_output: Option<&CommandOutput>,
    executor: &impl CommandExecutor,
) -> Option<String> {
    let command = match canonical {
        TargetTemplate::LocalPodman { .. } => {
            let name = hel_targets::resource_name(session_id).ok()?;
            CommandSpec::new("podman", ["rm", "--force", &name])
                .purpose("remove container after failed provisioning")
        }
        TargetTemplate::AppleContainer { .. } => {
            let name = hel_targets::resource_name(session_id).ok()?;
            CommandSpec::new("container", ["rm", "--force", &name])
                .purpose("remove container after failed provisioning")
        }
        TargetTemplate::AwsEc2 {
            aws_profile,
            region,
            ..
        } => {
            let instance_id = serde_json::from_slice::<serde_json::Value>(&first_output?.stdout)
                .ok()?
                .pointer("/Instances/0/InstanceId")?
                .as_str()?
                .to_string();
            let profile = aws_profile.clone().unwrap_or_else(|| "default".into());
            CommandSpec::new(
                "aws",
                [
                    "--profile",
                    &profile,
                    "--region",
                    region,
                    "ec2",
                    "terminate-instances",
                    "--instance-ids",
                    &instance_id,
                ],
            )
            .purpose("terminate EC2 instance after failed provisioning")
        }
        // SSH machines are persistent; nothing was created that must die.
        TargetTemplate::SshBare { .. } | TargetTemplate::SshPodman { .. } => return None,
    };
    let purpose = command.purpose.clone();
    match executor.execute(&command) {
        Ok(output) if output.status == 0 => Some(format!("cleanup succeeded: {purpose}")),
        Ok(output) => Some(format!(
            "cleanup FAILED ({purpose}, status {}): the resource may still exist; find it via its dev.hel.session={session_id} label/tag",
            output.status
        )),
        Err(error) => Some(format!(
            "cleanup FAILED ({purpose}): {error:#}; the resource may still exist; find it via its dev.hel.session={session_id} label/tag"
        )),
    }
}

fn locator_after_provision(
    canonical: &TargetTemplate,
    backend: &hel_targets::TargetTemplate,
    session_id: &str,
    first_output: Option<&CommandOutput>,
    executor: &impl CommandExecutor,
    bundle: &ProjectBundleSpec,
) -> Result<TargetLocator> {
    let generated = hel_targets::resource_name(session_id)?;
    Ok(match canonical {
        TargetTemplate::LocalPodman { .. } => TargetLocator::LocalPodman {
            container_id: generated,
        },
        TargetTemplate::AppleContainer { .. } => TargetLocator::AppleContainer {
            container_id: generated,
        },
        TargetTemplate::SshBare { ssh, .. } => TargetLocator::SshBare {
            host: ssh.host.clone(),
            workspace: PathBuf::from(hel_targets::workspace_for(backend, session_id)?),
            worker_id: None,
        },
        TargetTemplate::SshPodman { ssh, .. } => TargetLocator::SshPodman {
            host: ssh.host.clone(),
            container_id: generated,
        },
        TargetTemplate::AwsEc2 {
            aws_profile,
            region,
            ssh_user,
            address_source,
            identity_file,
            ssh_args,
            ..
        } => {
            let output = first_output.context("AWS launch produced no output")?;
            let json: serde_json::Value = serde_json::from_slice(&output.stdout)
                .context("parse aws ec2 run-instances response")?;
            let instance_id = json
                .pointer("/Instances/0/InstanceId")
                .and_then(serde_json::Value::as_str)
                .context("AWS response omitted instance ID")?
                .to_string();
            let profile = aws_profile.clone().unwrap_or_else(|| "default".into());
            execute_checked(
                executor,
                CommandSpec::new(
                    "aws",
                    [
                        "--profile".into(),
                        profile.clone(),
                        "--region".into(),
                        region.clone(),
                        "ec2".into(),
                        "wait".into(),
                        "instance-status-ok".into(),
                        "--instance-ids".into(),
                        instance_id.clone(),
                    ],
                )
                .purpose("wait for EC2 session instance"),
            )?;
            let field = match address_source {
                AwsAddressSource::PublicDns => "PublicDnsName",
                AwsAddressSource::PublicIp => "PublicIpAddress",
                AwsAddressSource::PrivateDns => "PrivateDnsName",
                AwsAddressSource::PrivateIp => "PrivateIpAddress",
            };
            let address = execute_checked(
                executor,
                CommandSpec::new(
                    "aws",
                    [
                        "--profile".into(),
                        profile.clone(),
                        "--region".into(),
                        region.clone(),
                        "ec2".into(),
                        "describe-instances".into(),
                        "--instance-ids".into(),
                        instance_id.clone(),
                        "--query".into(),
                        format!("Reservations[0].Instances[0].{field}"),
                        "--output".into(),
                        "text".into(),
                    ],
                )
                .purpose("resolve EC2 session address"),
            )?;
            let address = String::from_utf8(address.stdout)
                .context("AWS address was not UTF-8")?
                .trim()
                .to_string();
            if address.is_empty() || address == "None" {
                bail!("AWS instance {instance_id} has no configured address");
            }
            let ssh = SshTarget {
                destination: format!("{ssh_user}@{address}"),
                ssh_args: ssh_args_with_identity(ssh_args, identity_file.as_deref()),
            };
            let backend_locator = hel_targets::TargetLocator::AwsEc2 {
                profile: profile.clone(),
                region: region.clone(),
                instance_id: instance_id.clone(),
                ssh,
                workspace: format!(".local/share/hel/workspaces/{session_id}"),
            };
            hel_targets::provision_on_locator_plan(&backend_locator, session_id, bundle)?
                .execute(executor)?;
            TargetLocator::AwsEc2 {
                instance_id,
                address: Some(address),
            }
        }
    })
}

fn backend_locator(
    locator: &TargetLocator,
    session: &SessionRecord,
    config: &HelConfig,
) -> Result<hel_targets::TargetLocator> {
    let template = config
        .targets
        .get(&session.target_template_id)
        .context("session target template is missing")?;
    Ok(match locator {
        TargetLocator::LocalPodman { container_id } => hel_targets::TargetLocator::LocalPodman {
            container_id: container_id.clone(),
        },
        TargetLocator::AppleContainer { container_id } => {
            hel_targets::TargetLocator::AppleContainer {
                container_id: container_id.clone(),
            }
        }
        TargetLocator::SshBare { workspace, .. } => {
            let TargetTemplate::SshBare { ssh, .. } = template else {
                bail!("session locator/template mismatch")
            };
            hel_targets::TargetLocator::SshBare {
                ssh: backend_ssh(ssh),
                workspace: workspace.to_string_lossy().into_owned(),
            }
        }
        TargetLocator::SshPodman { container_id, .. } => {
            let TargetTemplate::SshPodman { ssh, .. } = template else {
                bail!("session locator/template mismatch")
            };
            hel_targets::TargetLocator::SshPodman {
                ssh: backend_ssh(ssh),
                container_id: container_id.clone(),
            }
        }
        TargetLocator::AwsEc2 {
            instance_id,
            address,
        } => {
            let TargetTemplate::AwsEc2 {
                aws_profile,
                region,
                ssh_user,
                identity_file,
                ssh_args,
                ..
            } = template
            else {
                bail!("session locator/template mismatch")
            };
            let address = address.as_deref().context("AWS locator has no address")?;
            hel_targets::TargetLocator::AwsEc2 {
                profile: aws_profile.clone().unwrap_or_else(|| "default".into()),
                region: region.clone(),
                instance_id: instance_id.clone(),
                ssh: SshTarget {
                    destination: format!("{ssh_user}@{address}"),
                    ssh_args: ssh_args_with_identity(ssh_args, identity_file.as_deref()),
                },
                workspace: format!(".local/share/hel/workspaces/{}", session.id),
            }
        }
    })
}

fn absolute_target_path(
    executor: &impl CommandExecutor,
    locator: &hel_targets::TargetLocator,
    session_id: &str,
    path: &str,
) -> Result<String> {
    if path.starts_with('/') {
        return Ok(path.to_owned());
    }
    let output = execute_checked(
        executor,
        hel_targets::command_on_locator(
            locator,
            session_id,
            vec!["pwd".into()],
            "resolve target home directory",
        )?,
    )?;
    let directory = String::from_utf8(output.stdout).context("decode target working directory")?;
    let directory = directory.trim_end_matches(['\r', '\n', '/']);
    if directory.is_empty() || !directory.starts_with('/') {
        bail!("target returned an invalid working directory {directory:?}");
    }
    Ok(format!("{directory}/{path}"))
}

fn local_branch(repository: &Path) -> Result<String> {
    let output = Command::new("git")
        .args(["symbolic-ref", "--quiet", "--short", "HEAD"])
        .current_dir(repository)
        .output()
        .with_context(|| format!("read current branch in {}", repository.display()))?;
    if !output.status.success() {
        bail!(
            "local repository {} must have a branch checked out before Hel can expose it as origin",
            repository.display()
        );
    }
    let branch = String::from_utf8(output.stdout).context("decode local Git branch")?;
    let branch = branch.trim().to_owned();
    if branch.is_empty() {
        bail!("local repository has an empty current branch");
    }
    Ok(branch)
}

fn local_origin_url(worker_root: &str, repository_id: &str) -> String {
    fn ext_argument(value: &str) -> String {
        value.replace('%', "%%").replace(' ', "% ")
    }
    format!(
        "ext::{}/hel worker git-proxy --root {} --repository {} %S",
        ext_argument(worker_root),
        ext_argument(worker_root),
        repository_id,
    )
}

fn restore_local_repository_seed(
    executor: &impl CommandExecutor,
    locator: &hel_targets::TargetLocator,
    session: &SessionRecord,
    bundle: &ProjectBundle,
    workspace_root: &str,
    worker_root: &str,
    repositories: &[(&crate::hel_config::ProjectRepository, &PathBuf)],
) -> Result<()> {
    let snapshots = repositories
        .iter()
        .map(|(repository, source)| {
            collect_git_snapshot(
                &SystemGit,
                source,
                &GitCollectionSpec {
                    id: repository.id.clone(),
                    relative_destination: repository.destination.clone(),
                    base_commit: "HEAD".into(),
                    full_history: true,
                    origin_override: Some(format!("hel-local:{}", repository.id)),
                },
            )
            .with_context(|| format!("snapshot local repository {:?}", repository.id))
        })
        .collect::<Result<Vec<_>>>()?;
    let staging = data_dir().join("git-seeds");
    std::fs::create_dir_all(&staging)?;
    let archive_path = staging.join(format!("{}.hel.zip", session.id));
    write_archive_atomic(
        &archive_path,
        &ArchiveInput {
            session: SessionManifest {
                id: session.id.clone(),
                title: session.title.clone(),
                harness_kind: session.harness_kind,
                profile_id: session.last_profile.clone(),
                native_session_id: session.native_session_id.clone().unwrap_or_default(),
                created_at: session.created_at.clone(),
                checkpointed_at: now(),
                hel_version: env!("CARGO_PKG_VERSION").into(),
                worker_version: env!("CARGO_PKG_VERSION").into(),
                adapter_version: "acp-v1".into(),
            },
            target: TargetManifest {
                template_id: session.target_template_id.clone(),
                target_kind: target_kind(locator).into(),
                details: Default::default(),
            },
            bundle: BundleManifest {
                id: session.bundle_id.clone(),
                primary_repository: bundle.primary_repo.clone(),
            },
            canonical_events: Vec::new(),
            native_artifacts: Vec::new(),
            repositories: snapshots,
        },
    )?;

    let remote_archive = format!("{worker_root}/local-seed.hel.zip");
    let remote_spec = format!("{worker_root}/local-seed.json");
    let target_path = |path: &str| match locator {
        hel_targets::TargetLocator::AwsEc2 { .. } | hel_targets::TargetLocator::SshBare { .. } => {
            PathBuf::from(format!("~/{path}"))
        }
        _ => PathBuf::from(path),
    };
    let spec = RepositoryRestoreSpec {
        archive_path: target_path(&remote_archive),
        workspace_root: target_path(workspace_root),
    };
    let local_spec = staging.join(format!("{}.json", session.id));
    crate::hel_config::atomic_write(&local_spec, &serde_json::to_vec_pretty(&spec)?)?;
    upload_checkpoint_spec(
        executor,
        locator,
        &session.id,
        &archive_path,
        &remote_archive,
    )?;
    upload_checkpoint_spec(executor, locator, &session.id, &local_spec, &remote_spec)?;
    execute_checked(
        executor,
        hel_targets::command_on_locator(
            locator,
            &session.id,
            vec![
                format!("{worker_root}/hel"),
                "worker".into(),
                "restore-repositories".into(),
                "--spec".into(),
                remote_spec,
            ],
            "restore local repository bootstrap",
        )?,
    )?;
    Ok(())
}

fn ensure_git_broker(
    session_id: &str,
    locator: &hel_targets::TargetLocator,
    repositories: BTreeMap<String, PathBuf>,
) -> Result<()> {
    let directory = data_dir().join("git-brokers");
    std::fs::create_dir_all(&directory)?;
    let spec_path = directory.join(format!("{session_id}.json"));
    let ready_path = directory.join(format!("{session_id}.ready"));
    let pid_path = directory.join(format!("{session_id}.pid"));
    let log_path = directory.join(format!("{session_id}.log"));
    let spec = GitBrokerSpec {
        session_id: session_id.to_owned(),
        bridge: hel_targets::git_bridge_command(locator, session_id)?,
        repositories,
        ready_path: ready_path.clone(),
        pid_path: pid_path.clone(),
    };
    if broker_is_alive(&pid_path) {
        if GitBrokerSpec::read(&spec_path).is_ok_and(|existing| existing == spec)
            && ready_path.exists()
        {
            return Ok(());
        }
        bail!(
            "a different local Git broker is still active for session {session_id}; close its target before reconnecting"
        );
    }
    let _ = std::fs::remove_file(&ready_path);
    spec.write(&spec_path)?;

    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .with_context(|| format!("open Git broker log {}", log_path.display()))?;
    let stderr = log.try_clone()?;
    let executable = std::env::current_exe().context("locate Hel controller executable")?;
    let mut command = Command::new(executable);
    command
        .args(["broker", "--spec"])
        .arg(&spec_path)
        .stdin(Stdio::null())
        .stdout(log)
        .stderr(stderr);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let mut child = command.spawn().context("start local Git broker")?;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if ready_path.exists() && broker_is_alive(&pid_path) {
            std::thread::spawn(move || {
                let _ = child.wait();
            });
            return Ok(());
        }
        if let Some(status) = child.try_wait().context("poll local Git broker")? {
            bail!(
                "local Git broker exited with {status}; see {}",
                log_path.display()
            );
        }
        if Instant::now() >= deadline {
            bail!(
                "timed out starting local Git broker; see {}",
                log_path.display()
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    }
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

fn execute_checked_with_stdin(
    executor: &impl CommandExecutor,
    command: &CommandSpec,
    input: &mut dyn std::io::Read,
) -> Result<CommandOutput> {
    let output = executor.execute_with_stdin(command, input)?;
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

fn install_inherited_git_settings(
    executor: &impl CommandExecutor,
    locator: &hel_targets::TargetLocator,
    session_id: &str,
) -> Result<()> {
    let settings = if matches!(locator, hel_targets::TargetLocator::SshBare { .. }) {
        BTreeMap::new()
    } else {
        controller_git_settings()?
    };
    for command in inherited_git_setting_commands(locator, session_id, settings)? {
        execute_checked(executor, command)?;
    }
    Ok(())
}

fn inherited_git_setting_commands(
    locator: &hel_targets::TargetLocator,
    session_id: &str,
    settings: BTreeMap<String, String>,
) -> Result<Vec<CommandSpec>> {
    if matches!(locator, hel_targets::TargetLocator::SshBare { .. }) {
        return Ok(Vec::new());
    }
    settings
        .into_iter()
        .map(|(key, value)| {
            hel_targets::command_on_locator(
                locator,
                session_id,
                vec![
                    "git".into(),
                    "config".into(),
                    "--global".into(),
                    "--replace-all".into(),
                    "--".into(),
                    key.clone(),
                    value,
                ],
                format!("inherit Git setting {key}"),
            )
        })
        .collect()
}

fn controller_git_settings() -> Result<BTreeMap<String, String>> {
    let output = match Command::new("git")
        .args(["config", "--global", "--includes", "--null", "--list"])
        .stdin(Stdio::null())
        .output()
    {
        Ok(output) => output,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(error) => return Err(error).context("read controller Git configuration"),
    };
    if !output.status.success() {
        bail!(
            "read controller Git configuration failed with status {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    parse_inherited_git_settings(&output.stdout)
}

fn parse_inherited_git_settings(output: &[u8]) -> Result<BTreeMap<String, String>> {
    let mut settings = BTreeMap::new();
    for entry in output
        .split(|byte| *byte == 0)
        .filter(|entry| !entry.is_empty())
    {
        let entry = std::str::from_utf8(entry).context("decode controller Git configuration")?;
        let (key, value) = entry
            .split_once('\n')
            .with_context(|| format!("controller Git returned malformed entry {entry:?}"))?;
        let key = key.to_ascii_lowercase();
        if INHERITED_GIT_SETTINGS.contains(&key.as_str()) {
            settings.insert(key, value.to_owned());
        }
    }
    Ok(settings)
}

fn workspace_paths(
    locator: &hel_targets::TargetLocator,
    bundle: &ProjectBundle,
    session_id: &str,
) -> Result<(String, Vec<String>)> {
    let root = match locator {
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

fn bridge_launch(
    harness: crate::hel_config::HarnessKind,
    executable: Option<&Path>,
) -> (String, Vec<String>) {
    if let Some(executable) = executable {
        let args = if harness == crate::hel_config::HarnessKind::Kimi {
            vec!["acp".into()]
        } else {
            Vec::new()
        };
        return (executable.to_string_lossy().into_owned(), args);
    }
    match harness {
        crate::hel_config::HarnessKind::Codex => (
            "sh".into(),
            vec![
                "-lc".into(),
                format!("if command -v codex-acp >/dev/null 2>&1; then exec codex-acp; fi; {}; exec npx -y @agentclientprotocol/codex-acp@1.1.14", ensure_node_script()),
            ],
        ),
        crate::hel_config::HarnessKind::Claude => (
            "sh".into(),
            vec![
                "-lc".into(),
                format!("if command -v claude-agent-acp >/dev/null 2>&1; then exec claude-agent-acp; fi; {}; exec npx -y @agentclientprotocol/claude-agent-acp@0.66.0", ensure_node_script()),
            ],
        ),
        crate::hel_config::HarnessKind::Kimi => (
            "sh".into(),
            vec![
                "-lc".into(),
                "if command -v kimi >/dev/null 2>&1; then exec kimi acp; elif [ -x \"$HOME/.kimi-code/bin/kimi\" ]; then exec \"$HOME/.kimi-code/bin/kimi\" acp; elif command -v curl >/dev/null 2>&1; then curl -fsSL https://code.kimi.com/kimi-code/install.sh | bash && exec \"$HOME/.kimi-code/bin/kimi\" acp; else echo 'Hel needs compatible Kimi Code or curl for its official installer' >&2; exit 127; fi".into(),
            ],
        ),
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
            ".credentials.json",
            "settings.json",
            "CLAUDE.md",
            "skills",
            "plugins",
        ],
        crate::hel_config::HarnessKind::Kimi => &[
            "credentials",
            "config.toml",
            "AGENTS.md",
            "SYSTEM.md",
            "mcp.json",
            "skills",
            "agents",
            "plugins",
        ],
    };
    for name in allowlist {
        let from = source.join(name);
        if from.exists() {
            copy_profile_entry(&from, &destination.join(name))?;
        }
    }
    apply_profile_overrides(profile, destination)?;
    append_hel_container_environment(profile.kind, destination)
}

/// Apply `model`/`reasoning_effort` overrides to the staged per-session copy
/// of the harness configuration. The controller-side home stays untouched.
fn apply_profile_overrides(
    profile: &crate::hel_config::HarnessProfile,
    destination: &Path,
) -> Result<()> {
    if profile.model.is_none() && profile.reasoning_effort.is_none() {
        return Ok(());
    }
    match profile.kind {
        crate::hel_config::HarnessKind::Codex => {
            let path = destination.join("config.toml");
            let mut table: toml::Table = if path.is_file() {
                std::fs::read_to_string(&path)?
                    .parse()
                    .with_context(|| format!("parse staged codex config {}", path.display()))?
            } else {
                toml::Table::new()
            };
            if let Some(model) = &profile.model {
                table.insert("model".into(), toml::Value::String(model.clone()));
            }
            if let Some(effort) = &profile.reasoning_effort {
                table.insert(
                    "model_reasoning_effort".into(),
                    toml::Value::String(effort.clone()),
                );
            }
            std::fs::write(&path, toml::to_string_pretty(&table)?)?;
        }
        crate::hel_config::HarnessKind::Claude => {
            let path = destination.join("settings.json");
            let mut settings: serde_json::Value = if path.is_file() {
                serde_json::from_str(&std::fs::read_to_string(&path)?)
                    .with_context(|| format!("parse staged claude settings {}", path.display()))?
            } else {
                serde_json::json!({})
            };
            let object = settings
                .as_object_mut()
                .context("staged claude settings.json is not a JSON object")?;
            if let Some(model) = &profile.model {
                object.insert("model".into(), serde_json::Value::String(model.clone()));
            }
            std::fs::write(&path, serde_json::to_string_pretty(&settings)?)?;
        }
        // Config validation rejects overrides for Kimi profiles.
        crate::hel_config::HarnessKind::Kimi => {}
    }
    Ok(())
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
    let metadata = std::fs::symlink_metadata(source)?;
    if metadata.file_type().is_symlink() {
        return Ok(());
    }
    if metadata.is_file() {
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::copy(source, destination)?;
        return Ok(());
    }
    if metadata.is_dir() {
        std::fs::create_dir_all(destination)?;
        for entry in std::fs::read_dir(source)? {
            let entry = entry?;
            copy_profile_entry(&entry.path(), &destination.join(entry.file_name()))?;
        }
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
            let upload = format!("~/.cache/hel/uploads/{session_id}");
            execute_checked(
                executor,
                ssh_command_spec(ssh, ["mkdir", "-p", &upload])
                    .purpose("create remote upload staging"),
            )?;
            for (source, name) in [
                (worker_binary, "hel"),
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
                    format!("{upload}/hel"),
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
    Ok(())
}

fn start_worker(
    executor: &impl CommandExecutor,
    locator: &hel_targets::TargetLocator,
    worker_root: &str,
) -> Result<()> {
    let binary = format!("{worker_root}/hel");
    let config = format!("{worker_root}/launch.json");
    let detached_script = format!(
        "nohup {} >{} 2>&1 </dev/null &",
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
        "exec {} >{} 2>&1",
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

/// Enrich an opaque handshake failure by running the installed worker binary
/// directly in the target. This surfaces loader errors (for example a
/// glibc-linked worker inside an older-glibc container) that a detached start
/// swallows.
fn worker_probe_diagnosis(
    executor: &impl CommandExecutor,
    locator: &hel_targets::TargetLocator,
    worker_root: &str,
    error: anyhow::Error,
) -> anyhow::Error {
    let binary = format!("{worker_root}/hel");
    let command = match locator {
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
fn worker_last_words(
    executor: &impl CommandExecutor,
    locator: &hel_targets::TargetLocator,
    worker_root: &str,
) -> Option<String> {
    let script = format!(
        "if [ -f {root}/worker-exit.json ]; then echo '--- worker-exit.json ---'; cat {root}/worker-exit.json; fi; if [ -f {root}/worker.log ]; then echo '--- worker.log (tail) ---'; tail -n 20 {root}/worker.log; fi",
        root = worker_root
    );
    let command = match locator {
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

async fn handshake_worker(command: &CommandSpec) -> Result<Option<String>> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    for _ in 0..40 {
        let mut child = tokio::process::Command::new(&command.program)
            .args(&command.args)
            .envs(&command.env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn();
        if let Ok(ref mut child) = child {
            let request = RequestEnvelope {
                request_id: "controller-hello".into(),
                protocol_version: PROTOCOL_VERSION,
                request: WorkerRequest::Hello {
                    client_version: env!("CARGO_PKG_VERSION").into(),
                    supported: VersionRange::CURRENT,
                },
            };
            let mut stdin = child.stdin.take().context("worker proxy stdin missing")?;
            let stdout = child.stdout.take().context("worker proxy stdout missing")?;
            let mut encoded = serde_json::to_vec(&request)?;
            encoded.push(b'\n');
            if stdin.write_all(&encoded).await.is_ok() {
                let mut line = String::new();
                let read = tokio::time::timeout(
                    std::time::Duration::from_secs(2),
                    tokio::io::BufReader::new(stdout).read_line(&mut line),
                )
                .await;
                if matches!(read, Ok(Ok(value)) if value > 0) {
                    let response: crate::hel_worker::ResponseEnvelope =
                        serde_json::from_str(&line)?;
                    let _ = child.kill().await;
                    return match response.body {
                        ResponseBody::Ok {
                            payload: ResponsePayload::Hello { .. },
                        } => Ok(None),
                        ResponseBody::Error { error } => {
                            bail!("worker handshake rejected: {}", error.message)
                        }
                        _ => bail!("worker handshake returned an unexpected response"),
                    };
                }
            }
            let _ = child.kill().await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    bail!("target worker did not become reachable")
}

fn now() -> String {
    Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn prune_replaced_checkpoint(previous: Option<&CheckpointMetadata>, current: &CheckpointMetadata) {
    let Some(previous) = previous.filter(|old| old.archive_path != current.archive_path) else {
        return;
    };
    if let Err(error) = std::fs::remove_file(&previous.archive_path)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(
            path = %previous.archive_path.display(),
            "could not remove superseded recovery copy: {error}"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;
    use crate::hel_config::{ContainerTemplate as ConfigContainer, ProjectRepository};

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

    #[test]
    fn failed_new_session_provisioning_discards_provisional_record() {
        let session_id = "0123456789abcdef0123456789abcdef";
        let record = SessionRecord {
            id: session_id.into(),
            title: "new session".into(),
            harness_kind: crate::hel_config::HarnessKind::Codex,
            last_profile: "codex".into(),
            bundle_id: "project".into(),
            target_template_id: "podman".into(),
            resource_allocation: None,
            additional_mounts: Vec::new(),
            state: SessionState::Provisioning,
            target: None,
            native_session_id: None,
            acp_session_title: None,
            session_title_override: None,
            created_at: "2026-08-12T00:00:00Z".into(),
            updated_at: "2026-08-12T00:00:00Z".into(),
            last_viewed_event_sequence: 0,
            last_error: None,
            last_checkpoint_error: None,
            checkpoint: None,
        };
        let mut state = HelState::default();
        state.sessions.insert(session_id.into(), record);

        let result = apply_new_session_provisioning_result(
            &mut state,
            session_id,
            Err(anyhow::anyhow!("container creation failed")),
        );

        assert!(result.is_err());
        assert!(!state.sessions.contains_key(session_id));
    }

    #[test]
    fn failed_resume_rolls_back_only_after_target_cleanup() {
        let previous = SessionRecord {
            id: "0123456789abcdef0123456789abcdef".into(),
            title: "imported session".into(),
            harness_kind: crate::hel_config::HarnessKind::Codex,
            last_profile: "codex-old".into(),
            bundle_id: "project".into(),
            target_template_id: "podman-old".into(),
            resource_allocation: None,
            additional_mounts: Vec::new(),
            state: SessionState::Archived,
            target: None,
            native_session_id: Some("native-session".into()),
            acp_session_title: None,
            session_title_override: None,
            created_at: "2026-08-12T00:00:00Z".into(),
            updated_at: "2026-08-12T00:00:00Z".into(),
            last_viewed_event_sequence: 0,
            last_error: None,
            last_checkpoint_error: None,
            checkpoint: None,
        };
        let partial_target = TargetLocator::LocalPodman {
            container_id: "partial-container".into(),
        };
        let mut cleaned = previous.clone();
        cleaned.state = SessionState::Error;
        cleaned.last_profile = "codex-new".into();
        cleaned.target = Some(partial_target.clone());

        let failure =
            apply_failed_resume_rollback(&mut cleaned, &previous, "worker upload failed", None);

        assert_eq!(cleaned.state, SessionState::Archived);
        assert_eq!(cleaned.last_profile, "codex-old");
        assert_eq!(cleaned.target, None);
        assert!(failure.to_string().contains("returned to archived"));

        let mut cleanup_failed = previous.clone();
        cleanup_failed.state = SessionState::Error;
        cleanup_failed.last_profile = "codex-new".into();
        cleanup_failed.target = Some(partial_target.clone());

        let failure = apply_failed_resume_rollback(
            &mut cleanup_failed,
            &previous,
            "worker upload failed",
            Some("podman rm failed".into()),
        );

        assert_eq!(cleanup_failed.state, SessionState::Error);
        assert_eq!(cleanup_failed.last_profile, "codex-new");
        assert_eq!(cleanup_failed.target, Some(partial_target));
        assert!(failure.to_string().contains("cleanup"));
    }

    #[test]
    fn aws_resources_are_compressed_into_one_streamed_ssh_command() {
        struct RecordingExecutor {
            commands: RefCell<Vec<CommandSpec>>,
            streams: RefCell<Vec<Vec<u8>>>,
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

            fn execute_with_stdin(
                &self,
                command: &CommandSpec,
                input: &mut dyn std::io::Read,
            ) -> Result<CommandOutput> {
                self.commands.borrow_mut().push(command.clone());
                let mut stream = Vec::new();
                input.read_to_end(&mut stream)?;
                self.streams.borrow_mut().push(stream);
                Ok(CommandOutput {
                    status: 0,
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                })
            }
        }

        let source = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(source.path().join("many/files")).unwrap();
        std::fs::write(source.path().join("many/files/one"), b"one").unwrap();
        std::fs::write(source.path().join("many/files/two"), b"two").unwrap();
        let session_id = "0123456789abcdef0123456789abcdef";
        let record = SessionRecord {
            id: session_id.into(),
            title: "AWS resources".into(),
            harness_kind: crate::hel_config::HarnessKind::Codex,
            last_profile: "codex".into(),
            bundle_id: "project".into(),
            target_template_id: "aws".into(),
            resource_allocation: None,
            additional_mounts: vec![AdditionalMount {
                source: source.path().to_path_buf(),
                destination: "/home/ubuntu/hel-resources/data".into(),
            }],
            state: SessionState::Disconnected,
            target: None,
            native_session_id: None,
            acp_session_title: None,
            session_title_override: None,
            created_at: "2026-08-12T00:00:00Z".into(),
            updated_at: "2026-08-12T00:00:00Z".into(),
            last_viewed_event_sequence: 0,
            last_error: None,
            last_checkpoint_error: None,
            checkpoint: None,
        };
        let state = HelState {
            version: crate::hel_state::STATE_VERSION,
            sessions: BTreeMap::from([(session_id.into(), record)]),
            mount_history: BTreeMap::new(),
        };
        let backend = hel_targets::TargetLocator::AwsEc2 {
            profile: "default".into(),
            region: "us-east-1".into(),
            instance_id: "i-1234567890abcdef0".into(),
            ssh: SshTarget {
                destination: "ubuntu@example.test".into(),
                ssh_args: Vec::new(),
            },
            workspace: format!(".local/share/hel/workspaces/{session_id}"),
        };
        let executor = RecordingExecutor {
            commands: RefCell::new(Vec::new()),
            streams: RefCell::new(Vec::new()),
        };

        install_attached_resources(
            &state,
            session_id,
            &backend,
            ".local/share/hel/workers/session",
            &executor,
        )
        .unwrap();

        let commands = executor.commands.borrow();
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].program, "ssh");
        assert!(
            commands[0]
                .args
                .iter()
                .any(|argument| argument.contains("install-resource"))
        );
        let streams = executor.streams.borrow();
        assert_eq!(streams.len(), 1);
        assert_eq!(&streams[0][..2], &[0x1f, 0x8b]);
    }

    #[test]
    fn default_bridges_pin_command_capable_adapter_versions() {
        let (_, codex_arguments) = bridge_launch(crate::hel_config::HarnessKind::Codex, None);
        assert!(codex_arguments[1].contains("@agentclientprotocol/codex-acp@1.1.14"));

        let (_, claude_arguments) = bridge_launch(crate::hel_config::HarnessKind::Claude, None);
        assert!(claude_arguments[1].contains("@agentclientprotocol/claude-agent-acp@0.66.0"));
    }

    #[test]
    fn kimi_default_bridge_uses_bash_for_the_official_installer() {
        let (command, arguments) = bridge_launch(crate::hel_config::HarnessKind::Kimi, None);
        assert_eq!(command, "sh");
        assert_eq!(arguments[0], "-lc");
        assert!(arguments[1].contains("install.sh | bash &&"));
        assert!(arguments[1].contains("$HOME/.kimi-code/bin/kimi"));
    }

    #[test]
    fn stage_profile_applies_codex_model_and_effort_overrides() {
        let home = tempfile::tempdir().unwrap();
        std::fs::write(
            home.path().join("config.toml"),
            "model = \"gpt-old\"\nsandbox_mode = \"workspace-write\"\n",
        )
        .unwrap();
        let staged = tempfile::tempdir().unwrap();
        let profile = crate::hel_config::HarnessProfile {
            kind: crate::hel_config::HarnessKind::Codex,
            home: home.path().to_path_buf(),
            executable: None,
            environment: std::collections::BTreeMap::new(),
            model: Some("gpt-5.6-terra".into()),
            reasoning_effort: Some("xhigh".into()),
            context_window_bytes: None,
        };
        stage_profile(&profile, staged.path()).unwrap();
        let staged_config: toml::Table = std::fs::read_to_string(staged.path().join("config.toml"))
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(
            staged_config["model"].as_str().unwrap(),
            "gpt-5.6-terra",
            "override replaces the copied model"
        );
        assert_eq!(
            staged_config["model_reasoning_effort"].as_str().unwrap(),
            "xhigh"
        );
        assert_eq!(
            staged_config["sandbox_mode"].as_str().unwrap(),
            "workspace-write",
            "unrelated keys survive the rewrite"
        );
        let source_config = std::fs::read_to_string(home.path().join("config.toml")).unwrap();
        assert!(
            source_config.contains("gpt-old"),
            "controller-side home must stay untouched"
        );
    }

    #[test]
    fn stage_profile_appends_container_environment_for_each_harness_without_touching_home() {
        for (kind, instructions) in [
            (crate::hel_config::HarnessKind::Codex, "AGENTS.md"),
            (crate::hel_config::HarnessKind::Claude, "CLAUDE.md"),
            (crate::hel_config::HarnessKind::Kimi, "SYSTEM.md"),
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
                model: None,
                reasoning_effort: None,
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
            model: None,
            reasoning_effort: None,
            context_window_bytes: None,
        };

        stage_profile(&profile, staged.path()).unwrap();

        assert_eq!(
            std::fs::read_to_string(staged.path().join("SYSTEM.md")).unwrap(),
            HEL_CONTAINER_ENVIRONMENT
        );
        assert!(!home.path().join("SYSTEM.md").exists());
    }

    #[test]
    fn inherited_git_settings_allow_only_portable_non_executable_values() {
        let settings = parse_inherited_git_settings(
            b"user.name\nAgent User\0USER.EMAIL\nagent@example.test\0pull.rebase\ntrue\0alias.deploy\n!ship\0credential.helper\nstore\0core.editor\nvim\0include.path\n/host/config\0user.name\nFinal User\0",
        )
        .unwrap();

        assert_eq!(
            settings,
            BTreeMap::from([
                ("pull.rebase".into(), "true".into()),
                ("user.email".into(), "agent@example.test".into()),
                ("user.name".into(), "Final User".into()),
            ])
        );
    }

    #[test]
    fn inherited_git_settings_reject_malformed_or_non_utf8_output() {
        assert!(parse_inherited_git_settings(b"user.name\0").is_err());
        assert!(parse_inherited_git_settings(b"user.name\n\xff\0").is_err());
    }

    #[test]
    fn inherited_git_settings_target_every_ephemeral_worker_but_ssh_bare() {
        let ssh = SshTarget {
            destination: "worker@example.test".into(),
            ssh_args: vec!["-p".into(), "2222".into()],
        };
        let ephemeral = [
            hel_targets::TargetLocator::LocalPodman {
                container_id: "abcdef012345".into(),
            },
            hel_targets::TargetLocator::AppleContainer {
                container_id: "abcdef012346".into(),
            },
            hel_targets::TargetLocator::AwsEc2 {
                profile: "default".into(),
                region: "us-east-1".into(),
                instance_id: "i-1234567890abcdef0".into(),
                ssh: ssh.clone(),
                workspace: ".local/share/hel/workspaces/018f9dd2-a3b4-7c8d-9000-123456789abc"
                    .into(),
            },
            hel_targets::TargetLocator::SshPodman {
                ssh: ssh.clone(),
                container_id: "abcdef012347".into(),
            },
        ];
        for locator in ephemeral {
            let commands = inherited_git_setting_commands(
                &locator,
                "018f9dd2-a3b4-7c8d-9000-123456789abc",
                BTreeMap::from([("user.name".into(), "- Agent O'Brien 日本語".into())]),
            )
            .unwrap();
            assert_eq!(commands.len(), 1);
            assert!(
                commands[0]
                    .args
                    .iter()
                    .any(|argument| argument.contains("user.name"))
            );
            assert!(
                commands[0]
                    .args
                    .iter()
                    .any(|argument| argument.contains("- Agent O'"))
            );
        }

        let persistent = hel_targets::TargetLocator::SshBare {
            ssh,
            workspace: "/srv/hel/018f9dd2-a3b4-7c8d-9000-123456789abc".into(),
        };
        assert!(
            inherited_git_setting_commands(
                &persistent,
                "018f9dd2-a3b4-7c8d-9000-123456789abc",
                BTreeMap::from([("user.name".into(), "Agent".into())]),
            )
            .unwrap()
            .is_empty()
        );
    }

    #[test]
    fn canonical_bundle_maps_github_shorthand_and_primary_destination() {
        let bundle = ProjectBundle {
            primary_repo: "app".into(),
            repositories: vec![ProjectRepository {
                id: "app".into(),
                github: Some("example/app".into()),
                local: None,
                destination: PathBuf::from("services/app"),
                git_ref: Some("main".into()),
            }],
        };
        let backend = backend_bundle(&bundle).unwrap();
        assert_eq!(backend.primary, "services/app");
        assert_eq!(
            backend.repositories[0].url.as_deref(),
            Some("https://github.com/example/app.git")
        );
    }

    #[test]
    fn container_resources_and_environment_become_argv() {
        let template = TargetTemplate::LocalPodman {
            container: ConfigContainer {
                image: "dev:1".into(),
                platform: Some("linux/arm64".into()),
                cpus: Some("4".into()),
                memory: Some("8g".into()),
                environment: std::collections::BTreeMap::from([("A".into(), "b c".into())]),
            },
        };
        let hel_targets::TargetTemplate::LocalPodman(container) =
            backend_target(&template, None).unwrap()
        else {
            unreachable!()
        };
        assert!(container.extra_run_args.contains(&"--cpus=4".into()));
        assert!(container.extra_run_args.contains(&"A=b c".into()));
    }

    #[test]
    fn aws_resource_options_follow_the_launch_template_family() {
        let mut config = HelConfig::default();
        config.targets.insert(
            "aws".into(),
            TargetTemplate::AwsEc2 {
                aws_profile: None,
                region: "us-east-1".into(),
                launch_template: "hel-runson".into(),
                launch_template_version: None,
                ssh_user: "ubuntu".into(),
                address_source: AwsAddressSource::PublicIp,
                identity_file: None,
                ssh_args: Vec::new(),
            },
        );
        let executor = PreflightExecutor {
            outputs: RefCell::new(vec![
                CommandOutput {
                    status: 0,
                    stdout: br#"{"LaunchTemplateVersions":[{"LaunchTemplateData":{"InstanceType":"m8i-flex.large"}}]}"#.to_vec(),
                    stderr: Vec::new(),
                },
                CommandOutput {
                    status: 0,
                    stdout: br#"{"InstanceTypes":[{"InstanceType":"m8i-flex.4xlarge","VCpuInfo":{"DefaultVCpus":16},"MemoryInfo":{"SizeInMiB":65536}},{"InstanceType":"m8i-flex.2xlarge","VCpuInfo":{"DefaultVCpus":8},"MemoryInfo":{"SizeInMiB":32768}}]}"#.to_vec(),
                    stderr: Vec::new(),
                },
            ]),
        };
        let controller = Controller {
            config,
            state: HelState::default(),
        };

        let options = controller
            .resolve_aws_resource_options("aws", &executor)
            .unwrap();
        assert_eq!(
            options.iter().map(allocation_vcpus).collect::<Vec<_>>(),
            [8, 16]
        );
    }

    #[test]
    fn deployment_capacity_groups_local_and_same_host_targets() {
        let container = || ConfigContainer {
            image: "dev:1".into(),
            platform: None,
            cpus: None,
            memory: None,
            environment: BTreeMap::new(),
        };
        let ssh = |host: &str| SshConnection {
            host: host.into(),
            user: Some("builder".into()),
            identity_file: None,
            extra_args: Vec::new(),
        };
        let config = HelConfig {
            version: crate::hel_config::CONFIG_VERSION,
            profiles: BTreeMap::new(),
            bundles: BTreeMap::new(),
            targets: BTreeMap::from([
                (
                    "apple".into(),
                    TargetTemplate::AppleContainer {
                        container: container(),
                    },
                ),
                (
                    "local".into(),
                    TargetTemplate::LocalPodman {
                        container: container(),
                    },
                ),
                (
                    "bare".into(),
                    TargetTemplate::SshBare {
                        ssh: ssh("builder"),
                        workspace_prefix: ".local/share/hel/workspaces".into(),
                    },
                ),
                (
                    "remote-container".into(),
                    TargetTemplate::SshPodman {
                        ssh: ssh("builder"),
                        container: container(),
                    },
                ),
                (
                    "alias".into(),
                    TargetTemplate::SshBare {
                        ssh: ssh("builder-alias"),
                        workspace_prefix: ".local/share/hel/workspaces".into(),
                    },
                ),
            ]),
        };
        let controller = Controller {
            config,
            state: HelState::default(),
        };

        let targets = controller.deployment_capacity_targets();

        assert_eq!(targets.len(), 3);
        let local = targets.iter().find(|target| target.id == "local").unwrap();
        assert_eq!(local.target_ids, ["apple", "local"]);
        let builder = targets
            .iter()
            .find(|target| target.id == "ssh:builder")
            .unwrap();
        assert_eq!(builder.target_ids, ["bare", "remote-container"]);
        assert_eq!(builder.probes.len(), 1);
        assert!(
            targets
                .iter()
                .any(|target| target.id == "ssh:builder-alias")
        );
    }

    struct PreflightExecutor {
        outputs: RefCell<Vec<CommandOutput>>,
    }

    impl CommandExecutor for PreflightExecutor {
        fn execute(&self, _command: &CommandSpec) -> Result<CommandOutput> {
            Ok(self.outputs.borrow_mut().remove(0))
        }
    }

    #[test]
    fn local_podman_preflight_failures_recommend_doctor() {
        let template = TargetTemplate::LocalPodman {
            container: ConfigContainer {
                image: "ubuntu:24.04".into(),
                platform: None,
                cpus: None,
                memory: None,
                environment: std::collections::BTreeMap::new(),
            },
        };
        let executor = PreflightExecutor {
            outputs: RefCell::new(vec![CommandOutput {
                status: 0,
                stdout: b"podman version 3.4.7\n".to_vec(),
                stderr: vec![],
            }]),
        };

        let error = preflight_target(&template, &executor)
            .unwrap_err()
            .to_string();
        assert!(error.contains("hel doctor"));
        assert!(error.contains("Podman 4.0.0"));
    }

    #[test]
    fn apple_container_preflight_failures_recommend_doctor() {
        let template = TargetTemplate::AppleContainer {
            container: ConfigContainer {
                image: "ubuntu:24.04".into(),
                platform: None,
                cpus: None,
                memory: None,
                environment: std::collections::BTreeMap::new(),
            },
        };
        let executor = PreflightExecutor {
            outputs: RefCell::new(vec![CommandOutput {
                status: 1,
                stdout: vec![],
                stderr: b"daemon is not running".to_vec(),
            }]),
        };

        let error = preflight_target(&template, &executor)
            .unwrap_err()
            .to_string();
        assert!(error.contains("hel doctor"));
        assert!(error.contains("daemon is not running"));
    }

    #[test]
    fn recovery_container_scan_requires_both_managed_and_session_labels() {
        let template = TargetTemplate::LocalPodman {
            container: ConfigContainer {
                image: "ignored".into(),
                platform: None,
                cpus: None,
                memory: None,
                environment: BTreeMap::new(),
            },
        };
        let json = serde_json::json!([
            {"Labels": {"dev.hel.managed": "true", "dev.hel.session": "0123456789abcdef0123456789abcdef"}},
            {"Labels": {"dev.hel.managed": "false", "dev.hel.session": "not-owned"}},
            {"configuration": {"labels": "dev.hel.managed=true,dev.hel.session=abcdef0123456789abcdef0123456789"}}
        ]);
        let candidates = candidates_from_container_json(
            "local",
            &template,
            serde_json::to_string(&json).unwrap().as_bytes(),
        )
        .unwrap();
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].session_id, "0123456789abcdef0123456789abcdef");
    }

    #[test]
    fn recovery_aws_scan_uses_exact_tagged_instance_and_address() {
        let json = serde_json::json!({"Reservations": [{"Instances": [{
            "InstanceId": "i-exact",
            "PrivateIpAddress": "10.0.0.7",
            "Tags": [
                {"Key": "dev.hel.managed", "Value": "true"},
                {"Key": "dev.hel.session", "Value": "0123456789abcdef0123456789abcdef"}
            ]
        }]}]});
        let candidates = candidates_from_aws_json(
            "aws",
            AwsAddressSource::PrivateIp,
            serde_json::to_string(&json).unwrap().as_bytes(),
        )
        .unwrap();
        assert!(matches!(
            &candidates[0].locator,
            TargetLocator::AwsEc2 { instance_id, address }
                if instance_id == "i-exact" && address.as_deref() == Some("10.0.0.7")
        ));
    }
}
