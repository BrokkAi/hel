//! Controller-side lifecycle transitions and canonical-to-backend conversion.

use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{Context, Result, bail};
use chrono::Utc;
use sha2::{Digest, Sha256};

use crate::hel_archive::{
    BundleManifest, PayloadRole, SessionManifest, TargetManifest, read_archive_verified,
};
use crate::hel_checkpoint::{
    CheckpointExportSpec, CheckpointRepositorySpec, CheckpointRestoreSpec, CheckpointTransfer,
    export_command, restore_command,
};
use crate::hel_config::{
    AwsAddressSource, HelConfig, ProjectBundle, SshConnection, TargetTemplate, data_dir,
    sessions_dir,
};
use crate::hel_state::{
    CheckpointMetadata, HelState, SessionRecord, SessionState, TargetLocator, new_session_id,
    normalize_session_title,
};
use crate::hel_targets::{
    self, AdditionalMount, AwsTemplate, CommandExecutor, CommandOutput, CommandSpec,
    ContainerTemplate, ProcessExecutor, ProjectBundleSpec, RepositorySpec, SshTarget,
};
use crate::hel_worker::{
    PROTOCOL_VERSION, RequestEnvelope, ResponseBody, ResponsePayload, VersionRange, WorkerEvent,
    WorkerRequest,
};
use crate::hel_worker_client::WorkerClient;
use crate::hel_worker_runtime::WorkerLaunchConfig;

pub struct Controller {
    pub config: HelConfig,
    pub state: HelState,
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
            TargetTemplate::LocalPodman { .. } | TargetTemplate::AppleContainer { .. } => {
                Ok(hel_targets::local_directory_completions(prefix))
            }
            TargetTemplate::SshPodman { ssh, .. } => {
                hel_targets::ssh_directory_completions(&backend_ssh(ssh), prefix, executor)
            }
            _ => bail!("mount path completion requires a container-backed target"),
        }
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
        let profile = self
            .config
            .profiles
            .get(profile_id)
            .with_context(|| format!("unknown profile {profile_id:?}"))?;
        self.config
            .bundles
            .get(bundle_id)
            .with_context(|| format!("unknown bundle {bundle_id:?}"))?;
        let template = self
            .config
            .targets
            .get(target_id)
            .with_context(|| format!("unknown target template {target_id:?}"))?;
        if !additional_mounts.is_empty() && mount_history_host(template).is_none() {
            bail!("additional mounts require a container-backed target");
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
        let template = self
            .config
            .targets
            .get(&session.target_template_id)
            .context("target template disappeared during provisioning")?;
        let bundle = self
            .config
            .bundles
            .get(&session.bundle_id)
            .context("project bundle disappeared during provisioning")?;
        let target = backend_target(template)?;
        let bundle = backend_bundle(bundle)?;
        let provision =
            hel_targets::provision_plan(&target, session_id, &bundle, &session.additional_mounts)?;

        let result =
            preflight_target(template, executor).and_then(|()| provision.execute(executor));
        // Everything after resource creation must funnel through the error
        // branch below: an early `?` here once left records stuck in
        // Provisioning with a live, untracked cloud resource.
        let result = result.and_then(|outputs| {
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
        });
        match result {
            Ok(locator) => {
                let record = self.state.sessions.get_mut(session_id).unwrap();
                record.target = Some(locator);
                // Provisioning has completed, but Running is reserved for a
                // successful worker handshake.
                record.state = SessionState::Disconnected;
                record.updated_at = now();
                record.last_error = None;
                self.state.save()?;
                Ok(())
            }
            Err(error) => {
                let record = self.state.sessions.get_mut(session_id).unwrap();
                record.state = SessionState::Error;
                record.updated_at = now();
                record.last_error = Some(format!("{error:#}"));
                self.state.save()?;
                Err(error)
            }
        }
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
            additional_directories: workspace.1.into_iter().map(PathBuf::from).collect(),
            native_session_id: session.native_session_id.clone(),
            recover_native_session,
        };

        let staging = tempfile::tempdir().context("create worker staging directory")?;
        let launch_path = staging.path().join("launch.json");
        launch.write(&launch_path)?;
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
            &profile_stage,
        )?;
        Ok((backend, worker_root))
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
        let previous = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?
            .clone();
        if previous.state != SessionState::Archived {
            bail!("session {session_id} is not archived");
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
        if !previous.additional_mounts.is_empty() && mount_history_host(target_template).is_none() {
            bail!("resuming a session with additional mounts requires a container-backed target");
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
        record.target = None;
        record.native_session_id =
            same_harness.then(|| archive.manifest.session.native_session_id.clone());
        record.state = SessionState::Provisioning;
        record.updated_at = now();
        record.last_error = None;
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
            client.detach().await?;
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if let Err(error) = &result {
            let record = self.state.sessions.get_mut(session_id).unwrap();
            record.state = SessionState::Error;
            record.updated_at = now();
            record.last_error = Some(format!("resume failed: {error:#}"));
            self.state.save()?;
        }
        result
    }

    /// Materialize and locally verify a complete session checkpoint while the
    /// target remains live. A failed export or transfer leaves the previous
    /// archive and target untouched.
    pub async fn checkpoint_session(&mut self, session_id: &str) -> Result<CheckpointMetadata> {
        match self
            .checkpoint_session_with(session_id, &ProcessExecutor)
            .await
        {
            Ok(checkpoint) => Ok(checkpoint),
            Err(error) => {
                if let Some(record) = self.state.sessions.get_mut(session_id) {
                    record.state = SessionState::Error;
                    record.updated_at = now();
                    record.last_error = Some(format!("checkpoint failed: {error:#}"));
                    self.state.save()?;
                }
                Err(error)
            }
        }
    }

    async fn checkpoint_session_with(
        &mut self,
        session_id: &str,
        executor: &(impl CommandExecutor + Sync),
    ) -> Result<CheckpointMetadata> {
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
        let record = self.state.sessions.get_mut(session_id).unwrap();
        record.state = SessionState::Checkpointing;
        record.updated_at = now();
        record.last_error = None;
        self.state.save()?;

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
                    base_commit: repository
                        .git_ref
                        .as_deref()
                        .map(|git_ref| format!("origin/{git_ref}"))
                        .unwrap_or_else(|| "origin/HEAD".into()),
                })
                .collect(),
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
        if target_checkpoint.event_sequence < expected_sequence {
            bail!(
                "target checkpoint omitted the checkpoint event {expected_sequence}; ended at {}",
                target_checkpoint.event_sequence
            );
        }

        let destination = sessions_dir().join(format!("{session_id}.hel.zip"));
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
        let checkpoint = CheckpointMetadata {
            archive_path: verified.archive_path().to_path_buf(),
            sha256: verified.sha256().to_string(),
            created_at: checkpointed_at,
            event_sequence: verified.event_sequence(),
        };
        let record = self.state.sessions.get_mut(session_id).unwrap();
        record.state = SessionState::Running;
        record.native_session_id = Some(native_session_id);
        record.checkpoint = Some(checkpoint.clone());
        record.updated_at = now();
        record.last_error = None;
        self.state.save()?;
        Ok(checkpoint)
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
    let current = std::env::current_exe().context("resolve Hel controller binary")?;
    if let Some(path) = std::env::var_os("HEL_WORKER_BINARY").map(PathBuf::from) {
        if !path.is_file() {
            bail!("HEL_WORKER_BINARY is not a file: {}", path.display());
        }
        return Ok(WorkerBinaryAvailability::Local {
            path,
            source: "HEL_WORKER_BINARY".into(),
        });
    }
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
            path: current,
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
                url: github_url(&repository.github),
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

fn backend_target(template: &TargetTemplate) -> Result<hel_targets::TargetTemplate> {
    Ok(match template {
        TargetTemplate::LocalPodman { container } => {
            hel_targets::TargetTemplate::LocalPodman(backend_container(container))
        }
        TargetTemplate::AppleContainer { container } => {
            hel_targets::TargetTemplate::AppleContainer(backend_container(container))
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
            container: backend_container(container),
        },
    })
}

fn mount_history_host(template: &TargetTemplate) -> Option<String> {
    match template {
        TargetTemplate::LocalPodman { .. } | TargetTemplate::AppleContainer { .. } => {
            Some("local".into())
        }
        TargetTemplate::SshPodman { ssh, .. } => Some(ssh.host.clone()),
        TargetTemplate::AwsEc2 { .. } | TargetTemplate::SshBare { .. } => None,
    }
}

fn backend_container(container: &crate::hel_config::ContainerTemplate) -> ContainerTemplate {
    let mut extra_run_args = Vec::new();
    if let Some(platform) = &container.platform {
        extra_run_args.push(format!("--platform={platform}"));
    }
    if let Some(cpus) = &container.cpus {
        extra_run_args.push(format!("--cpus={cpus}"));
    }
    if let Some(memory) = &container.memory {
        extra_run_args.push(format!("--memory={memory}"));
    }
    for (key, value) in &container.environment {
        extra_run_args.extend(["--env".to_string(), format!("{key}={value}")]);
    }
    ContainerTemplate {
        image: container.image.clone(),
        extra_run_args,
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
                format!("if command -v claude-agent-acp >/dev/null 2>&1; then exec claude-agent-acp; fi; {}; exec npx -y @agentclientprotocol/claude-agent-acp@0.63.0", ensure_node_script()),
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
            for (source, name) in [(worker_binary, "hel"), (launch_config, "launch.json")] {
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

fn install_worker_over_ssh(
    executor: &impl CommandExecutor,
    ssh: &SshTarget,
    worker_root: &str,
    profile_home: &str,
    worker_binary: &Path,
    launch_config: &Path,
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

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;
    use crate::hel_config::{ContainerTemplate as ConfigContainer, ProjectRepository};

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
    fn canonical_bundle_maps_github_shorthand_and_primary_destination() {
        let bundle = ProjectBundle {
            primary_repo: "app".into(),
            repositories: vec![ProjectRepository {
                id: "app".into(),
                github: "example/app".into(),
                destination: PathBuf::from("services/app"),
                git_ref: Some("main".into()),
            }],
        };
        let backend = backend_bundle(&bundle).unwrap();
        assert_eq!(backend.primary, "services/app");
        assert_eq!(
            backend.repositories[0].url,
            "https://github.com/example/app.git"
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
            backend_target(&template).unwrap()
        else {
            unreachable!()
        };
        assert!(container.extra_run_args.contains(&"--cpus=4".into()));
        assert!(container.extra_run_args.contains(&"A=b c".into()));
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
}
