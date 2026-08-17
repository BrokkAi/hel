//! Session provisioning, rollback, and worker-side Git bootstrap.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};

use crate::hel_archive::{
    ArchiveInput, BundleManifest, GitCollectionSpec, GitHistoryMode, SessionManifest, SystemGit,
    TargetManifest, collect_git_snapshot, write_archive_atomic,
};
use crate::hel_checkpoint::RepositoryRestoreSpec;
use crate::hel_config::{ProjectBundle, TargetTemplate, atomic_write, data_dir};
use crate::hel_git_proxy::{GitBrokerSpec, broker_is_alive};
use crate::hel_local_git::canonical_repository;
use crate::hel_projection::canonical_session_from_materialized;
use crate::hel_state::{HelState, SessionRecord, SessionState, TargetLocator};
use crate::hel_targets::{
    self, CancellableProcessExecutor, CommandExecutor, CommandOutput, CommandSpec, ProvisionStage,
};

use super::backend::{
    absolute_target_path, backend_bundle, backend_locator, backend_target, controller_github_token,
    inject_github_token, locator_after_provision, preflight_target, use_github_https_urls,
};
use super::checkpoint::upload_checkpoint_spec;
use super::readiness::{connect_started_worker, wait_for_native_session};
use super::worker_binary::{start_worker, worker_probe_diagnosis};
use super::{Controller, execute_checked, now, target_kind};

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

/// Whether connecting local repositories may also carry the user's current
/// uncommitted changes into a still-empty target checkout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum LocalBootstrap {
    /// A fresh target starts from `git init`, so seed its branch and dirty
    /// state from the local repository.
    Seed,
    /// Seed from this checkout instead of the bundle's configured path. A
    /// resume that moves a raw session into a target carries the session's own
    /// worktree, not the user's primary checkout.
    SeedFrom(PathBuf),
    /// Resume restores the session's own dirty state from the checkpoint
    /// archive; seeding the local repository's would collide with it.
    Skip,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ProvisioningFailureDisposition {
    /// A freshly registered session has no durable history to retain.
    Discard,
    /// Resume owns rollback to the archived record and checkpoint lineage.
    Preserve,
}

impl Controller {
    pub async fn provision_session_controlled(
        &mut self,
        session_id: &str,
        executor: &(impl CommandExecutor + Sync),
    ) -> Result<()> {
        self.provision_session_controlled_with_commit(session_id, executor, || Ok(()))
            .await
    }

    pub async fn provision_session_controlled_with_commit(
        &mut self,
        session_id: &str,
        executor: &(impl CommandExecutor + Sync),
        grant_commit: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        let github_token = controller_github_token();
        self.provision_session_with_github_token(session_id, executor, github_token.as_deref())
            .await?;
        match self.install_and_connect_worker(session_id, executor).await {
            Ok(native_session_id) => {
                if let Err(error) = grant_commit() {
                    return Err(self.rollback_failed_new_session(session_id, error, executor)?);
                }
                self.mark_worker_connected(session_id, native_session_id)
            }
            Err(error) => Err(self.rollback_failed_new_session(session_id, error, executor)?),
        }
    }

    fn rollback_failed_new_session(
        &mut self,
        session_id: &str,
        error: anyhow::Error,
        executor: &impl CommandExecutor,
    ) -> Result<anyhow::Error> {
        let session = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?
            .clone();
        let target_cleanup = match session.target.as_ref() {
            Some(locator) => (|| -> Result<()> {
                let backend = backend_locator(locator, &session, &self.config)?;
                hel_targets::close_plan(&backend, session_id)?
                    // Rollback must remain possible after the foreground
                    // operation's cancellation token has been set.
                    .execute(&CancellableProcessExecutor::with_timeout(
                        Duration::from_secs(15),
                    ))
                    .map(|_| ())
            })(),
            None => Ok(()),
        };
        let worktree_cleanup =
            self.cleanup_new_session_worktree_after_failure(session_id, executor);
        let cleanup_error = [target_cleanup, worktree_cleanup]
            .into_iter()
            .filter_map(Result::err)
            .map(|error| format!("{error:#}"))
            .collect::<Vec<_>>()
            .join("; ");
        let original = format!("{error:#}");
        let original = match persist_launch_failure(session_id, &original) {
            Ok(path) => format!("{original}; full diagnostic saved to {}", path.display()),
            Err(save_error) => {
                format!("{original}; saving the local diagnostic failed: {save_error:#}")
            }
        };
        let failure = apply_failed_new_session_rollback(
            &mut self.state,
            session_id,
            &original,
            (!cleanup_error.is_empty()).then_some(cleanup_error),
        );
        self.persist_session_state(session_id)?;
        Ok(failure)
    }

    pub async fn provision_session_with(
        &mut self,
        session_id: &str,
        executor: &(impl CommandExecutor + Sync),
    ) -> Result<()> {
        self.provision_session_with_github_token(session_id, executor, None)
            .await
    }

    async fn provision_session_with_github_token(
        &mut self,
        session_id: &str,
        executor: &(impl CommandExecutor + Sync),
        github_token: Option<&str>,
    ) -> Result<()> {
        self.provision_session_with_failure_disposition(
            session_id,
            executor,
            github_token,
            ProvisioningFailureDisposition::Discard,
        )
        .await
    }

    pub(super) async fn provision_session_with_failure_disposition(
        &mut self,
        session_id: &str,
        executor: &(impl CommandExecutor + Sync),
        github_token: Option<&str>,
        failure_disposition: ProvisioningFailureDisposition,
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
        let created_worktree = match self.prepare_managed_raw_worktree(session_id, executor) {
            Ok(created) => created,
            Err(error) if failure_disposition == ProvisioningFailureDisposition::Discard => {
                return Err(self.fail_new_session_with_cleanup(session_id, error, executor)?);
            }
            Err(error) => return Err(error),
        };
        let session = self
            .state
            .sessions
            .get(session_id)
            .expect("session retained after managed worktree preparation")
            .clone();
        // Keep planning, preflight, creation, and locator discovery in one
        // result so the caller's failure disposition applies to every error.
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
            let mut target = backend_target(template, session.resource_allocation.as_ref())?;
            let runtime_mounts = if matches!(target, hel_targets::TargetTemplate::AwsEc2(_)) {
                &[][..]
            } else {
                session.additional_mounts.as_slice()
            };
            let mut bundle = session
                .project_directory
                .is_none()
                .then(|| self.config.bundles.get(&session.bundle_id))
                .flatten()
                .map(backend_bundle)
                .transpose()?;
            if let Some(token) = github_token
                && inject_github_token(&mut target, token)
                && let Some(bundle) = bundle.as_mut()
            {
                use_github_https_urls(bundle);
            }
            let provision = if let Some(project_directory) = &session.project_directory {
                hel_targets::provision_bare_project_plan(
                    &target,
                    session_id,
                    &project_directory.to_string_lossy(),
                )?
            } else {
                hel_targets::provision_plan(
                    &target,
                    session_id,
                    bundle
                        .as_ref()
                        .context("project bundle disappeared during provisioning")?,
                    runtime_mounts,
                )?
            };

            let outputs = preflight_target(template, executor).and_then(|()| {
                let started = Instant::now();
                let result = provision.execute_concurrent(executor);
                tracing::debug!(
                    session_id,
                    elapsed_ms = started.elapsed().as_millis(),
                    "provisioning plan execution completed"
                );
                result
            })?;
            locator_after_provision(
                template,
                &target,
                session_id,
                outputs.first(),
                executor,
                bundle.as_ref(),
            )
            .map_err(|error| {
                match cleanup_failed_provision(template, session_id, outputs.first(), executor) {
                    Some(note) => error.context(note),
                    None => error,
                }
            })
        })();
        let result = match result {
            Err(error)
                if created_worktree
                    && failure_disposition == ProvisioningFailureDisposition::Discard =>
            {
                return Err(self.fail_new_session_with_cleanup(session_id, error, executor)?);
            }
            Err(error) if failure_disposition == ProvisioningFailureDisposition::Preserve => {
                Err(error)
            }
            result => apply_new_session_provisioning_result(&mut self.state, session_id, result),
        };
        if result.is_ok()
            && let Some(session) = self.state.sessions.get(session_id)
            && let Some(directory) = session
                .managed_worktree
                .as_ref()
                .map(|worktree| worktree.source_project_directory.clone())
                .or_else(|| session.project_directory.clone())
            && let Some(template) = self.config.targets.get(&session.target_template_id)
        {
            let host = match template {
                TargetTemplate::LocalBare => Some("local"),
                TargetTemplate::SshBare { ssh, .. } => Some(ssh.host.as_str()),
                _ => None,
            };
            if let Some(host) = host {
                self.state.remember_project_directory(host, &directory);
                crate::hel_database::remember_project_directory(host, &directory)?;
            }
        }
        self.persist_session_state(session_id)?;
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
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?;
        if session.target.is_none() {
            bail!("session {session_id} has no provisioned target");
        }
        let updated_at = now();
        crate::hel_database::mark_session_worker_connected(
            session_id,
            native_session_id.as_deref(),
            &updated_at,
        )?;
        let session = self
            .state
            .sessions
            .get_mut(session_id)
            .expect("session disappeared after its worker connection was saved");
        session.state = SessionState::Running;
        if native_session_id.is_some() {
            session.native_session_id = native_session_id;
        }
        session.updated_at = updated_at;
        session.last_error = None;
        Ok(())
    }

    async fn install_and_connect_worker(
        &self,
        session_id: &str,
        executor: &impl CommandExecutor,
    ) -> Result<Option<String>> {
        // Installing the worker and connecting its repositories moves data into
        // the target, so it reports as Sync. Start begins at the daemon launch.
        let syncing = &StagedExecutor::new(executor, ProvisionStage::Syncing);
        let (backend, worker_root) = self.worker_placement(session_id)?;
        self.prepare_worker_files(session_id, &backend, &worker_root, syncing)?;
        install_attached_resources(&self.state, session_id, &backend, &worker_root, syncing)?;
        self.connect_local_repositories(
            session_id,
            &backend,
            &worker_root,
            syncing,
            LocalBootstrap::Seed,
        )?;
        let executor = &StagedExecutor::new(executor, ProvisionStage::Starting);
        start_worker(executor, &backend, &worker_root)?;
        let reconnect = &hel_targets::reconnect_plan(&backend, session_id)?.commands[0];
        let readiness = async {
            let mut relay =
                connect_started_worker(reconnect, session_id, executor, &backend, &worker_root)
                    .await?;
            let native_session_id = wait_for_native_session(&mut relay, executor).await?;
            Ok(Some(native_session_id))
        }
        .await;
        match readiness {
            Ok(native_session_id) => Ok(native_session_id),
            Err(error) => Err(worker_probe_diagnosis(
                executor,
                &backend,
                &worker_root,
                error,
            )),
        }
    }

    /// Point the target's checkouts at the `hel-local` Git proxy and fetch the
    /// committed history it serves. `bootstrap` decides whether a still-empty
    /// checkout is also seeded with the local repository's uncommitted changes.
    pub(super) fn connect_local_repositories(
        &self,
        session_id: &str,
        backend: &hel_targets::TargetLocator,
        worker_root: &str,
        executor: &impl CommandExecutor,
        bootstrap: LocalBootstrap,
    ) -> Result<()> {
        let session = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?;
        if session.project_directory.is_some() {
            return Ok(());
        }
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
            hel_targets::TargetLocator::LocalBare { worker_root } => worker_root.clone(),
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
        // Fetch before bootstrapping: the proxy delivers every branch, so the
        // bootstrap archive only has to carry identity and dirty state, and
        // the commit it checks out is already present.
        for (repository, _) in &local {
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
        if let Some(sources) = seed_sources(&missing, &bootstrap)
            && !sources.is_empty()
        {
            bootstrap_local_repositories(
                executor,
                backend,
                session,
                bundle,
                &workspace_root,
                worker_root,
                &sources,
            )?;
        }
        Ok(())
    }
}

const MAX_LAUNCH_DIAGNOSTIC_BYTES: usize = 64 * 1024;

const RETAINED_LAUNCH_DIAGNOSTICS: usize = 20;

fn persist_launch_failure(session_id: &str, detail: &str) -> Result<PathBuf> {
    persist_launch_failure_to(&data_dir().join("diagnostics"), session_id, detail)
}

fn persist_launch_failure_to(directory: &Path, session_id: &str, detail: &str) -> Result<PathBuf> {
    crate::hel_config::validate_id("session", session_id)?;
    std::fs::create_dir_all(directory).with_context(|| {
        format!(
            "create launch diagnostics directory {}",
            directory.display()
        )
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700))?;
    }
    let path = directory.join(format!("{session_id}-launch-error.txt"));
    let detail = bounded_launch_diagnostic(detail);
    let body = format!(
        "Hel session launch failure\nsession: {session_id}\nat: {}\n\n{detail}\n",
        now()
    );
    atomic_write(&path, body.as_bytes())?;
    prune_launch_diagnostics(directory)?;
    Ok(path)
}

fn bounded_launch_diagnostic(detail: &str) -> String {
    if detail.len() <= MAX_LAUNCH_DIAGNOSTIC_BYTES {
        return detail.to_owned();
    }
    let mut head_end = MAX_LAUNCH_DIAGNOSTIC_BYTES / 4;
    while !detail.is_char_boundary(head_end) {
        head_end -= 1;
    }
    let tail_bytes = MAX_LAUNCH_DIAGNOSTIC_BYTES - head_end;
    let mut tail_start = detail.len() - tail_bytes;
    while !detail.is_char_boundary(tail_start) {
        tail_start += 1;
    }
    format!(
        "{}\n\n[... launch diagnostic truncated ...]\n\n{}",
        &detail[..head_end],
        &detail[tail_start..]
    )
}

fn prune_launch_diagnostics(directory: &Path) -> Result<()> {
    let mut diagnostics = Vec::new();
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        if !entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.ends_with("-launch-error.txt"))
        {
            continue;
        }
        diagnostics.push((entry.metadata()?.modified()?, entry.path()));
    }
    diagnostics.sort_by_key(|entry| std::cmp::Reverse(entry.0));
    for (_, path) in diagnostics.into_iter().skip(RETAINED_LAUNCH_DIAGNOSTICS) {
        std::fs::remove_file(&path)
            .with_context(|| format!("prune old launch diagnostic {}", path.display()))?;
    }
    Ok(())
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

pub(super) fn apply_failed_new_session_rollback(
    state: &mut HelState,
    session_id: &str,
    original_error: &str,
    cleanup_error: Option<String>,
) -> anyhow::Error {
    match cleanup_error {
        None => {
            state.sessions.remove(session_id);
            anyhow::anyhow!(
                "{original_error}; partial target removed and provisional session discarded"
            )
        }
        Some(cleanup_error) => {
            let failure = format!(
                "{original_error}; cleanup of the failed session target failed: {cleanup_error}"
            );
            let record = state.sessions.get_mut(session_id).unwrap();
            record.state = SessionState::Error;
            record.updated_at = now();
            record.last_error = Some(format!("worker bootstrap failed: {failure}"));
            anyhow::anyhow!(failure)
        }
    }
}

pub(super) fn install_attached_resources(
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
        TargetTemplate::LocalBare
        | TargetTemplate::SshBare { .. }
        | TargetTemplate::SshPodman { .. } => return None,
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

fn seed_sources<'a>(
    missing: &[(&'a crate::hel_config::ProjectRepository, &'a PathBuf)],
    bootstrap: &'a LocalBootstrap,
) -> Option<Vec<(&'a crate::hel_config::ProjectRepository, &'a PathBuf)>> {
    let checkout = match bootstrap {
        LocalBootstrap::Skip => return None,
        LocalBootstrap::Seed => None,
        LocalBootstrap::SeedFrom(checkout) => Some(checkout),
    };
    Some(
        missing
            .iter()
            .map(|(repository, source)| (*repository, checkout.unwrap_or(source)))
            .collect(),
    )
}

/// Carry a local repository's identity and uncommitted changes into a freshly
/// initialized target checkout. Committed history is never bundled here: the
/// caller fetches it through the `hel-local` proxy first.
fn bootstrap_local_repositories(
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
                    history: GitHistoryMode::NoBundle,
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
                relay_version: env!("CARGO_PKG_VERSION").into(),
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
            canonical_session: canonical_session_from_materialized(
                &crate::hel_state::MaterializedSession::empty(session.id.clone()),
            )?,
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

/// Reports every command an installer issues as one launch stage, so progress
/// stays accurate without threading the stage through each `CommandSpec`.
/// A command that already names a stage keeps it.
pub(super) struct StagedExecutor<'a, E: CommandExecutor> {
    inner: &'a E,
    stage: ProvisionStage,
}

impl<'a, E: CommandExecutor> StagedExecutor<'a, E> {
    pub(super) fn new(inner: &'a E, stage: ProvisionStage) -> Self {
        Self { inner, stage }
    }

    fn staged(&self, command: &CommandSpec) -> CommandSpec {
        if command.stage.is_some() {
            return command.clone();
        }
        command.clone().stage(self.stage)
    }
}

impl<E: CommandExecutor> CommandExecutor for StagedExecutor<'_, E> {
    fn execute(&self, command: &CommandSpec) -> Result<CommandOutput> {
        self.inner.execute(&self.staged(command))
    }

    fn cancellation_requested(&self) -> bool {
        self.inner.cancellation_requested()
    }

    fn notify_stage(&self, stage: ProvisionStage) {
        self.inner.notify_stage(stage);
    }

    fn execute_with_stdin(
        &self,
        command: &CommandSpec,
        input: &mut (dyn std::io::Read + Send),
    ) -> Result<CommandOutput> {
        self.inner.execute_with_stdin(&self.staged(command), input)
    }
}

fn execute_checked_with_stdin(
    executor: &impl CommandExecutor,
    command: &CommandSpec,
    input: &mut (dyn std::io::Read + Send),
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

pub(super) fn install_inherited_git_settings(
    executor: &impl CommandExecutor,
    locator: &hel_targets::TargetLocator,
    session_id: &str,
) -> Result<()> {
    let settings = if inherits_controller_git_settings(locator) {
        controller_git_settings()?
    } else {
        BTreeMap::new()
    };
    for command in inherited_git_setting_commands(locator, session_id, settings)? {
        execute_checked(executor, command)?;
    }
    Ok(())
}

fn inherits_controller_git_settings(locator: &hel_targets::TargetLocator) -> bool {
    !matches!(
        locator,
        hel_targets::TargetLocator::LocalBare { .. } | hel_targets::TargetLocator::SshBare { .. }
    )
}

pub(super) fn force_unrestricted_mode(locator: &hel_targets::TargetLocator) -> bool {
    !matches!(locator, hel_targets::TargetLocator::LocalBare { .. })
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::hel_config::ProjectRepository;
    use crate::hel_state::{HelState, SessionRecord, SessionState, TargetLocator};
    use crate::hel_targets::{self, SshTarget};

    use super::*;

    #[test]
    fn failed_new_session_provisioning_discards_provisional_record() {
        let session_id = "0123456789abcdef0123456789abcdef";
        let record = SessionRecord {
            id: session_id.into(),
            title: "new session".into(),
            harness_kind: crate::hel_config::HarnessKind::Codex,
            last_profile: "codex".into(),
            bundle_id: "project".into(),
            project_directory: None,
            managed_worktree: None,
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
            detached_after_event_ordinal: 0,
            draft_input: String::new(),
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
    fn failed_new_worker_start_discards_session_only_after_target_cleanup() {
        let session_id = "0123456789abcdef0123456789abcdef";
        let mut session = SessionRecord {
            id: session_id.into(),
            title: "new session".into(),
            harness_kind: crate::hel_config::HarnessKind::Kimi,
            last_profile: "kimi".into(),
            bundle_id: "raw-project".into(),
            project_directory: Some("/srv/project".into()),
            managed_worktree: None,
            target_template_id: "remote".into(),
            resource_allocation: None,
            additional_mounts: Vec::new(),
            state: SessionState::Disconnected,
            target: Some(TargetLocator::SshBare {
                host: "builder".into(),
                workspace: format!(".local/share/hel/workspaces/{session_id}").into(),
                worker_id: None,
            }),
            native_session_id: None,
            acp_session_title: None,
            session_title_override: None,
            created_at: "2026-08-12T00:00:00Z".into(),
            updated_at: "2026-08-12T00:00:00Z".into(),
            detached_after_event_ordinal: 0,
            draft_input: String::new(),
            last_error: None,
            last_checkpoint_error: None,
            checkpoint: None,
        };
        let mut cleaned = HelState::default();
        cleaned.sessions.insert(session_id.into(), session.clone());

        let failure =
            apply_failed_new_session_rollback(&mut cleaned, session_id, "ACP startup failed", None);

        assert!(!cleaned.sessions.contains_key(session_id));
        assert!(
            failure
                .to_string()
                .contains("provisional session discarded")
        );

        session.state = SessionState::Disconnected;
        let mut cleanup_failed = HelState::default();
        cleanup_failed.sessions.insert(session_id.into(), session);
        let failure = apply_failed_new_session_rollback(
            &mut cleanup_failed,
            session_id,
            "ACP startup failed",
            Some("ssh unavailable".into()),
        );
        let retained = cleanup_failed.sessions.get(session_id).unwrap();
        assert_eq!(retained.state, SessionState::Error);
        assert!(retained.target.is_some());
        assert!(failure.to_string().contains("cleanup"));
    }
    #[test]
    fn launch_failure_is_persisted_separately_from_session_state() {
        let directory = tempfile::tempdir().unwrap();
        let session_id = "0123456789abcdef0123456789abcdef";
        let detail = format!(
            "specific startup cause\n{}\nstderr tail survives",
            "x".repeat(MAX_LAUNCH_DIAGNOSTIC_BYTES)
        );

        let path = persist_launch_failure_to(directory.path(), session_id, &detail).unwrap();
        let saved = std::fs::read_to_string(path).unwrap();

        assert!(saved.contains("specific startup cause"));
        assert!(saved.contains("launch diagnostic truncated"));
        assert!(saved.contains("stderr tail survives"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(directory.path())
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
        }
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
    fn inherited_git_settings_target_only_isolated_workers() {
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
        for locator in &ephemeral {
            assert!(inherits_controller_git_settings(locator));
            let commands = inherited_git_setting_commands(
                locator,
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
        let local = hel_targets::TargetLocator::LocalBare {
            worker_root: "/var/lib/hel/workers/018f9dd2-a3b4-7c8d-9000-123456789abc".into(),
        };
        assert!(!inherits_controller_git_settings(&persistent));
        assert!(!inherits_controller_git_settings(&local));
        assert!(!force_unrestricted_mode(&local));
        assert!(force_unrestricted_mode(&ephemeral[0]));
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
    fn a_converting_resume_seeds_from_its_own_checkout() {
        let repository = ProjectRepository {
            id: "project".into(),
            github: None,
            local: Some(PathBuf::from("/home/dev/project")),
            destination: PathBuf::from("project"),
            git_ref: None,
        };
        let configured = PathBuf::from("/home/dev/project");
        let missing = vec![(&repository, &configured)];
        let checkout = PathBuf::from("/home/dev/project/.hel/worktrees/session");

        assert_eq!(seed_sources(&missing, &LocalBootstrap::Skip), None);
        assert_eq!(
            seed_sources(&missing, &LocalBootstrap::Seed)
                .unwrap()
                .into_iter()
                .map(|(_, source)| source.clone())
                .collect::<Vec<_>>(),
            vec![configured.clone()]
        );
        assert_eq!(
            seed_sources(&missing, &LocalBootstrap::SeedFrom(checkout.clone()))
                .unwrap()
                .into_iter()
                .map(|(_, source)| source.clone())
                .collect::<Vec<_>>(),
            vec![checkout]
        );
    }
}
