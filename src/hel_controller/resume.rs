//! Resuming an archived session onto a profile and target.

use std::path::PathBuf;
use std::time::Duration;

use agent_client_protocol::schema::v1::{ContentBlock, Plan, TextContent, ToolCall};
use anyhow::{Context, Result, bail};

use crate::hel_archive::{
    CanonicalQueuedCommandKind, CanonicalSessionSnapshot, CanonicalTranscriptBody, SystemGit,
    verify_archive_streaming,
};
use crate::hel_checkpoint::{CheckpointRestoreSpec, restore_command};
use crate::hel_projection::materialized_session_from_canonical;
use crate::hel_session_manager::new_command_id;
use crate::hel_state::{
    MaterializedSession, SessionRecord, SessionResourceAllocation, SessionState,
};
use crate::hel_targets::{
    self, AdditionalMount, CancellableProcessExecutor, CommandExecutor, ProcessExecutor,
    ProvisionStage,
};
use crate::hel_worker::RelayCommand;

use super::backend::{
    backend_locator, controller_github_token, mount_history_host, validate_resource_allocation,
};
use super::checkpoint::upload_checkpoint_spec;
use super::provisioning::{
    LocalBootstrap, ProvisioningFailureDisposition, StagedExecutor, install_attached_resources,
};
use super::readiness::{connect_started_worker, wait_for_native_session};
use super::worker_binary::{start_worker, worker_probe_diagnosis};
use super::worktree::{
    PrimaryCheckoutRequirement, ResumeConversion, ResumePlan, apply_raw_to_workspace,
    apply_workspace_to_raw, cleanup_managed_worktree, create_managed_worktree,
    plan_raw_to_workspace, raw_checkout_divergence_notice, raw_checkout_position,
    resume_compatibility, retire_managed_worktree,
};
use super::{Controller, SessionResumeOptions, execute_checked, now, target_profile_home};

impl Controller {
    /// Resume an archived logical session on any configured profile and
    /// target. Cross-harness resume restores Git and canonical history, starts
    /// a fresh native session, and supplies the prior transcript as its first
    /// context turn.
    pub async fn resume_session_with_options(
        &mut self,
        session_id: &str,
        profile_id: &str,
        target_id: &str,
        additional_mounts: Option<Vec<AdditionalMount>>,
        resource_allocation: Option<SessionResourceAllocation>,
    ) -> Result<MaterializedSession> {
        self.resume_session_with_options_and_queue_disposition(
            session_id,
            profile_id,
            target_id,
            additional_mounts,
            resource_allocation,
            false,
        )
        .await
    }

    pub async fn resume_session_with_options_and_queue_disposition(
        &mut self,
        session_id: &str,
        profile_id: &str,
        target_id: &str,
        additional_mounts: Option<Vec<AdditionalMount>>,
        resource_allocation: Option<SessionResourceAllocation>,
        discard_queue: bool,
    ) -> Result<MaterializedSession> {
        self.resume_session_controlled(
            session_id,
            profile_id,
            target_id,
            SessionResumeOptions {
                additional_mounts,
                resource_allocation,
                discard_queue,
            },
            &ProcessExecutor,
        )
        .await
    }

    pub async fn resume_session_controlled(
        &mut self,
        session_id: &str,
        profile_id: &str,
        target_id: &str,
        options: SessionResumeOptions,
        executor: &(impl CommandExecutor + Sync),
    ) -> Result<MaterializedSession> {
        let SessionResumeOptions {
            additional_mounts,
            resource_allocation,
            discard_queue,
        } = options;
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
        let archive = verify_archive_streaming(&checkpoint.archive_path)?;
        if archive.archive_sha256 != checkpoint.sha256 || archive.manifest.session.id != session_id
        {
            bail!("persisted checkpoint verification failed");
        }
        let profile = self
            .config
            .profiles
            .get(profile_id)
            .with_context(|| format!("unknown profile {profile_id:?}"))?
            .clone();
        let target_template = self
            .config
            .targets
            .get(target_id)
            .with_context(|| format!("unknown target template {target_id:?}"))?;
        // Decide the representation before the record changes, so an
        // incompatible target fails here instead of during provisioning.
        let plan = resume_compatibility(&previous, &self.config, target_id)
            .map_err(|reason| anyhow::anyhow!("{reason}"))?;
        if plan == ResumePlan::InPlace
            && let Some(project_directory) = &previous.project_directory
        {
            self.validate_project_directory(target_id, project_directory, executor)
                .context("raw project is unavailable for resume")?;
        }
        let conversion = match plan {
            ResumePlan::InPlace => None,
            ResumePlan::RawToWorkspace => Some(ResumeConversion::RawToWorkspace(
                plan_raw_to_workspace(&previous, &self.config, executor)
                    .context("prepare the raw checkout for its new target")?,
            )),
            ResumePlan::WorkspaceToRaw => Some(ResumeConversion::WorkspaceToRaw(
                self.plan_workspace_to_raw(&previous, target_id, executor)
                    .context("prepare a checkout for this session")?,
            )),
        };
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
                .execute(executor)
                .context("clean up target from failed resume")?;
        }
        let mut resume_notices = Vec::new();
        if let Some(conversion) = conversion
            .as_ref()
            .and_then(ResumeConversion::raw_to_workspace)
            && let Some(project_directory) = &previous.project_directory
        {
            resume_notices.push(match &conversion.retire {
                Some(worktree) => format!(
                    "This session moved out of {} and into the {target_id} target. Its branch {} stays in {}.",
                    project_directory.display(),
                    worktree.branch,
                    worktree.source_repository.display()
                ),
                None => format!(
                    "This session moved out of {} and into the {target_id} target.",
                    project_directory.display()
                ),
            });
        }
        if let Some(conversion) = conversion
            .as_ref()
            .and_then(ResumeConversion::workspace_to_raw)
        {
            resume_notices.push(format!(
                "This session moved out of its {} target and into {}. Its branch {} is now {}.",
                previous.target_template_id,
                conversion.worktree.worktree_root.display(),
                archive
                    .manifest
                    .repositories
                    .first()
                    .and_then(|repository| repository.metadata.branch.as_deref())
                    .unwrap_or("a detached head"),
                conversion.worktree.branch,
            ));
        }
        // The live checkout, not the archive, is the truth for a raw session.
        // Say so in the conversation when the two disagree; never reconcile.
        if let Some(project_directory) = &previous.project_directory {
            match raw_checkout_position(&previous, &self.config, project_directory, executor) {
                Ok(live) => resume_notices.extend(raw_checkout_divergence_notice(
                    project_directory,
                    archive
                        .manifest
                        .repositories
                        .first()
                        .map(|repository| &repository.metadata),
                    &live,
                )),
                // Informational only: a resume must not fail because Hel could
                // not read where the checkout stands.
                Err(error) => tracing::warn!(
                    session_id,
                    error = format!("{error:#}"),
                    "could not read the raw checkout position for a resume notice"
                ),
            }
        }
        let same_harness = profile.kind == archive.manifest.session.harness_kind;
        let canonical_session = archive.canonical_session.clone();
        let context_bytes = profile
            .context_window_bytes
            .unwrap_or(crate::hel_compaction::DEFAULT_CONTEXT_BYTES);
        let portable_session = (!same_harness).then(|| canonical_session.clone());
        let github_token = controller_github_token();

        // The configuration gains the bundle before the record points at it, so
        // no persisted session ever names a bundle that is not there.
        if let Some(conversion) = conversion
            .as_ref()
            .and_then(ResumeConversion::raw_to_workspace)
            && let Some(bundle) = &conversion.new_bundle
        {
            self.config
                .bundles
                .insert(conversion.bundle_id.clone(), bundle.clone());
            self.config
                .save()
                .context("save the bundle for a converted raw session")?;
        }

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
        match &conversion {
            Some(ResumeConversion::RawToWorkspace(conversion)) => {
                apply_raw_to_workspace(record, conversion);
            }
            Some(ResumeConversion::WorkspaceToRaw(conversion)) => {
                apply_workspace_to_raw(record, conversion);
            }
            None => {}
        }
        let resumed_project_directory = record.project_directory.clone();
        if let Some(host) = history_host {
            self.state.remember_mount_sources(&host, &history_mounts);
            crate::hel_database::remember_mount_sources(&host, &history_mounts)?;
        }
        // The session's prompt history is filed under its bundle, so a
        // conversion moves the history with it before the record is persisted.
        if let Some(conversion) = conversion
            .as_ref()
            .and_then(ResumeConversion::raw_to_workspace)
        {
            crate::hel_database::rebind_session_bundle(session_id, &conversion.bundle_id)?;
        }
        self.persist_session_state(session_id)?;

        let result = async {
            // The record already names the worktree, so a failure here rolls
            // back through the same path that cleans up a new session's.
            if let Some(conversion) =
                conversion.as_ref().and_then(ResumeConversion::workspace_to_raw)
            {
                create_managed_worktree(
                    executor,
                    &conversion.worktree,
                    None,
                    PrimaryCheckoutRequirement::Any,
                )?;
                crate::hel_checkpoint::restore_single_repository_onto_branch(
                    &checkpoint.archive_path,
                    &conversion.worktree.worktree_root,
                    &conversion.worktree.branch,
                    &SystemGit,
                )
                .context("restore this session's checkout")?;
            }
            self.provision_session_with_failure_disposition(
                session_id,
                executor,
                github_token.as_deref(),
                ProvisioningFailureDisposition::Preserve,
            )
            .await?;
            let executor = &StagedExecutor::new(executor, ProvisionStage::Starting);
            let (backend, worker_root) = self.prepare_worker_files(session_id, executor)?;
            let harness_home = target_profile_home(&backend, session_id, &profile);
            let workspace_root = if let Some(project_directory) = &resumed_project_directory {
                project_directory
                    .parent()
                    .context("bare project directory has no parent")?
                    .to_string_lossy()
                    .into_owned()
            } else {
                match &backend {
                    hel_targets::TargetLocator::LocalPodman { .. }
                    | hel_targets::TargetLocator::AppleContainer { .. }
                    | hel_targets::TargetLocator::SshPodman { .. } => "/workspace".to_string(),
                    hel_targets::TargetLocator::AwsEc2 { workspace, .. }
                    | hel_targets::TargetLocator::SshBare { workspace, .. } => workspace.clone(),
                    hel_targets::TargetLocator::LocalBare { worker_root } => worker_root.clone(),
                }
            };
            let target_path = |path: &str| match &backend {
                hel_targets::TargetLocator::AwsEc2 { .. }
                | hel_targets::TargetLocator::SshBare { .. }
                    if !path.starts_with('/') =>
                {
                    PathBuf::from(format!("~/{path}"))
                }
                _ => PathBuf::from(path),
            };
            // The restore needs the fetched objects: a committed delta bundle
            // cannot be applied without its prerequisites, and a bundle-free
            // snapshot checks out a head commit only the proxy can supply. The
            // archive carries this session's dirty state, so nothing is seeded.
            self.connect_local_repositories(
                session_id,
                &backend,
                &worker_root,
                executor,
                LocalBootstrap::Skip,
            )?;
            let remote_archive = format!("{worker_root}/restore.hel.zip");
            let remote_spec = format!("{worker_root}/restore-spec.json");
            let restore = CheckpointRestoreSpec {
                archive_path: target_path(&remote_archive),
                workspace_root: target_path(&workspace_root),
                relay_root: target_path(&worker_root),
                harness_home: target_path(&harness_home),
                // A converted session's repository arrives as a seed from its
                // own checkout, not from the archive's metadata-only capture.
                restore_repositories: resumed_project_directory.is_none() && conversion.is_none(),
                restore_native: same_harness,
                // A conversion puts the checkout somewhere the archive could
                // not have named, so the restored harness session is pointed at
                // the real working directory instead of the archived one.
                primary_repository_root: conversion
                    .is_some()
                    .then(|| resumed_project_directory.clone())
                    .flatten()
                    .map(|directory| target_path(&directory.to_string_lossy())),
                discard_queued_prompts: discard_queue || !same_harness,
            };
            let staging = tempfile::tempdir().context("create restore staging")?;
            let local_spec = staging.path().join("restore-spec.json");
            std::fs::write(&local_spec, serde_json::to_vec_pretty(&restore)?)?;
            upload_checkpoint_spec(
                executor,
                &backend,
                session_id,
                &checkpoint.archive_path,
                &remote_archive,
            )?;
            upload_checkpoint_spec(executor, &backend, session_id, &local_spec, &remote_spec)?;
            execute_checked(
                executor,
                restore_command(&backend, session_id, &remote_spec)?,
            )?;
            install_attached_resources(&self.state, session_id, &backend, &worker_root, executor)?;
            self.connect_local_repositories(
                session_id,
                &backend,
                &worker_root,
                executor,
                match conversion.as_ref().and_then(ResumeConversion::raw_to_workspace) {
                    Some(conversion) => LocalBootstrap::SeedFrom(conversion.checkout.clone()),
                    None => LocalBootstrap::Seed,
                },
            )?;
            let mut restored_projection =
                materialized_session_from_canonical(session_id, &canonical_session)?;
            if discard_queue || !same_harness {
                restored_projection.queued_prompts.clear();
            }
            crate::hel_database::save_materialized_session(&restored_projection)?;
            start_worker(executor, &backend, &worker_root)?;
            let spec = self.reconnect_command(session_id)?;
            let readiness = async {
                let mut relay =
                    connect_started_worker(&spec, session_id, executor, &backend, &worker_root)
                        .await?;
                let native_session_id = wait_for_native_session(&mut relay, executor).await?;
                Ok::<_, anyhow::Error>((relay, native_session_id))
            }
            .await;
            let (mut relay, native_session_id) = readiness
                .map_err(|error| worker_probe_diagnosis(executor, &backend, &worker_root, error))?;
            if same_harness {
                if native_session_id != archive.manifest.session.native_session_id {
                    bail!(
                        "ACP loaded native session {native_session_id}, expected {}",
                        archive.manifest.session.native_session_id
                    );
                }
            } else {
                let portable = portable_session
                    .as_ref()
                    .context("cross-harness resume is missing canonical session")?;
                let context = canonical_handoff_text(portable, context_bytes);
                relay
                    .submit(
                        new_command_id("cross-harness-handoff")?,
                        RelayCommand::Prompt {
                            prompt: vec![ContentBlock::Text(TextContent::new(context))],
                        },
                    )
                    .await?;
                if !discard_queue {
                    for prompt in &canonical_session.queued_prompts {
                        // A queued configuration change is replayed as itself;
                        // rebuilding it as a prompt would send `/model x` to
                        // the agent as text.
                        let command = match &prompt.kind {
                            CanonicalQueuedCommandKind::Prompt => RelayCommand::Prompt {
                                prompt: prompt
                                    .content
                                    .iter()
                                    .cloned()
                                    .map(serde_json::from_value)
                                    .collect::<serde_json::Result<Vec<ContentBlock>>>()?,
                            },
                            CanonicalQueuedCommandKind::SetConfig { key, value } => {
                                RelayCommand::SetConfig {
                                    key: key.clone(),
                                    value: value.clone(),
                                }
                            }
                        };
                        relay.submit(prompt.command_id.clone(), command).await?;
                    }
                }
            }
            // Last, and only once the resume has otherwise succeeded: a failure
            // before this point rolls the record back to a session whose
            // worktree still has to be there.
            if let Some(worktree) = conversion
                .as_ref()
                .and_then(ResumeConversion::raw_to_workspace)
                .and_then(|plan| plan.retire.as_ref())
                && let Err(error) = retire_managed_worktree(executor, worktree)
            {
                resume_notices.push(format!(
                    "Hel could not remove the worktree at {}: {error:#}. Remove it with `git worktree remove --force {}`.",
                    worktree.worktree_root.display(),
                    worktree.worktree_root.display()
                ));
            }
            for notice in &resume_notices {
                let submitted = async {
                    let command_id = new_command_id("resume-notice")?;
                    relay
                        .submit(
                            command_id,
                            RelayCommand::RecordNotice {
                                text: notice.clone(),
                            },
                        )
                        .await
                }
                .await;
                // The conversation line is a courtesy. A relay that refuses it
                // has not damaged the resume, so report and carry on.
                if let Err(error) = submitted {
                    tracing::warn!(
                        session_id,
                        error = format!("{error:#}"),
                        "could not record a resume notice in the conversation"
                    );
                }
            }
            self.mark_worker_connected(session_id, Some(native_session_id))?;
            Ok::<_, anyhow::Error>(relay.sync().await?.materialized)
        }
        .await;
        match result {
            Ok(materialized) => Ok(materialized),
            Err(error) => {
                if let Ok(previous_projection) =
                    materialized_session_from_canonical(session_id, &canonical_session)
                {
                    let _ = crate::hel_database::save_materialized_session(&previous_projection);
                }
                Err(self.rollback_failed_resume(session_id, &previous, error, executor)?)
            }
        }
    }

    fn rollback_failed_resume(
        &mut self,
        session_id: &str,
        previous: &SessionRecord,
        error: anyhow::Error,
        _executor: &impl CommandExecutor,
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
                    // Use a fresh executor: cancellation applies to the
                    // requested operation, not to its compensating cleanup.
                    .execute(&CancellableProcessExecutor::with_timeout(
                        Duration::from_secs(15),
                    ))
                    .map(|_| ())
            })(),
            None => Ok(()),
        };
        let worktree_cleanup = match (
            current.managed_worktree.as_ref(),
            previous.managed_worktree.as_ref(),
        ) {
            (Some(current), Some(previous)) if current == previous => Ok(()),
            (Some(worktree), _) => cleanup_managed_worktree(
                &CancellableProcessExecutor::with_timeout(Duration::from_secs(15)),
                worktree,
            ),
            (None, _) => Ok(()),
        };
        let cleanup_error = [cleanup, worktree_cleanup]
            .into_iter()
            .filter_map(Result::err)
            .map(|cleanup_error| format!("{cleanup_error:#}"))
            .collect::<Vec<_>>()
            .join("; ");
        let original = format!("{error:#}");
        let record = self.state.sessions.get_mut(session_id).unwrap();
        let failure = apply_failed_resume_rollback(
            record,
            previous,
            &original,
            (!cleanup_error.is_empty()).then_some(cleanup_error),
        );
        // A conversion filed the session's prompt history under its new bundle.
        // The record went back, so the history goes back with it.
        if record.bundle_id != current.bundle_id {
            let bundle_id = record.bundle_id.clone();
            crate::hel_database::rebind_session_bundle(session_id, &bundle_id)?;
        }
        self.persist_session_state(session_id)?;
        Ok(failure)
    }
}

pub(super) fn apply_failed_resume_rollback(
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
            // The target locator stays so the leftover resource can still be
            // cleaned up, but the session's representation goes back: a resume
            // that converted the record never moved the checkout it names.
            current
                .project_directory
                .clone_from(&previous.project_directory);
            current
                .managed_worktree
                .clone_from(&previous.managed_worktree);
            current.bundle_id.clone_from(&previous.bundle_id);
            current.state = SessionState::Error;
            current.updated_at = now();
            current.last_error = Some(format!("resume failed: {failure}"));
            anyhow::anyhow!(failure)
        }
    }
}

fn canonical_handoff_text(snapshot: &CanonicalSessionSnapshot, maximum_bytes: usize) -> String {
    const PREFIX: &str = "Continue this coding session from the portable transcript below. Preserve the user's intent and the work already completed.\n\n";
    let mut transcript = String::new();
    for item in &snapshot.transcript {
        let (label, body) = match &item.body {
            CanonicalTranscriptBody::User { content } => {
                ("User", crate::hel_chat::materialized_content_text(content))
            }
            CanonicalTranscriptBody::Agent { chunks, .. } => {
                ("Agent", crate::hel_chat::materialized_chunks_text(chunks))
            }
            CanonicalTranscriptBody::Thought { chunks, .. } => (
                "Agent reasoning",
                crate::hel_chat::materialized_chunks_text(chunks),
            ),
            CanonicalTranscriptBody::Tool { call } => (
                "Tool",
                serde_json::from_value::<ToolCall>(call.clone())
                    .map(|call| format!("{} [{:?}]", call.title, call.status))
                    .unwrap_or_else(|_| "[invalid tool call]".into()),
            ),
            CanonicalTranscriptBody::Plan { plan } => (
                "Plan",
                serde_json::from_value::<Plan>(plan.clone())
                    .map(|plan| plan.entries)
                    .unwrap_or_default()
                    .into_iter()
                    .map(|entry| {
                        format!(
                            "- [{}] {}",
                            format!("{:?}", entry.status).to_ascii_lowercase(),
                            entry.content
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n"),
            ),
            CanonicalTranscriptBody::System { text } => ("System", text.clone()),
        };
        if !body.trim().is_empty() {
            transcript.push_str(label);
            transcript.push_str(":\n");
            transcript.push_str(&body);
            transcript.push_str("\n\n");
        }
    }
    let available = maximum_bytes.saturating_sub(PREFIX.len());
    if transcript.len() > available {
        let mut start = transcript.len() - available;
        while !transcript.is_char_boundary(start) {
            start += 1;
        }
        transcript.drain(..start);
    }
    format!("{PREFIX}{transcript}")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::process::Command;

    use anyhow::Result;

    use crate::hel_archive::verify_archive_streaming;
    use crate::hel_config::{
        ContainerTemplate as ConfigContainer, HarnessProfile, HelConfig, ProjectBundle,
        ProjectRepository, TargetTemplate,
    };
    use crate::hel_controller::test_support::{
        checkpoint_test_session, committed_repository, managed_worktree_session,
        resume_compatibility_config, write_checkpoint_gate_archive,
    };
    use crate::hel_controller::{Controller, SessionResumeOptions};
    use crate::hel_projection::materialized_session_from_canonical;
    use crate::hel_state::{HelState, SessionRecord, SessionState, TargetLocator};
    use crate::hel_targets::{CommandExecutor, CommandOutput, CommandSpec, ProcessExecutor};

    use super::*;

    const RESUME_ROLLBACK_TEST_CHILD: &str = "HEL_RESUME_ROLLBACK_TEST_CHILD";
    #[test]
    fn bundle_sessions_refuse_a_local_bare_resume_before_the_record_changes() {
        struct UnusedExecutor;

        impl CommandExecutor for UnusedExecutor {
            fn execute(&self, command: &CommandSpec) -> Result<CommandOutput> {
                panic!("resume ran {} before rejecting the target", command.program);
            }
        }

        let directory = tempfile::tempdir().unwrap();
        let session_id = "0123456789abcdef0123456789abcdef";
        let checkpoint = write_checkpoint_gate_archive(directory.path(), session_id, 3);
        let mut session = checkpoint_test_session(session_id);
        session.state = SessionState::Archived;
        session.checkpoint = Some(checkpoint);
        let previous = session.clone();
        let profile_home = directory.path().join("profile");
        std::fs::create_dir_all(&profile_home).unwrap();
        let mut config = HelConfig::default();
        config.profiles.insert(
            "codex".into(),
            HarnessProfile {
                kind: crate::hel_config::HarnessKind::Codex,
                home: profile_home,
                executable: None,
                environment: BTreeMap::new(),
                context_window_bytes: None,
            },
        );
        config
            .targets
            .insert("raw-localhost".into(), TargetTemplate::LocalBare);
        let mut controller = Controller {
            config,
            state: HelState {
                sessions: BTreeMap::from([(session_id.into(), session)]),
                ..HelState::default()
            },
        };

        let error = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(controller.resume_session_controlled(
                session_id,
                "codex",
                "raw-localhost",
                SessionResumeOptions {
                    additional_mounts: None,
                    resource_allocation: None,
                    discard_queue: false,
                },
                &UnusedExecutor,
            ))
            .unwrap_err();

        let detail = format!("{error:#}");
        assert!(detail.contains("created from a project bundle"), "{detail}");
        assert!(
            detail.contains("resume it on a container, SSH, or EC2 target"),
            "{detail}"
        );
        assert_eq!(controller.state.sessions[session_id], previous);
    }
    #[test]
    fn failed_resume_rolls_back_only_after_target_cleanup() {
        let previous = SessionRecord {
            id: "0123456789abcdef0123456789abcdef".into(),
            title: "imported session".into(),
            harness_kind: crate::hel_config::HarnessKind::Codex,
            last_profile: "codex-old".into(),
            bundle_id: "project".into(),
            project_directory: None,
            managed_worktree: None,
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
            detached_after_event_ordinal: 0,
            draft_input: String::new(),
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
    fn failed_resume_provisioning_preserves_checkpoint_and_projection_lineage() {
        // HEL_DATA_DIR is process-global, so run the database-backed half in an
        // exact child test instead of racing unrelated tests in this process.
        if std::env::var_os(RESUME_ROLLBACK_TEST_CHILD).is_none() {
            let directory = tempfile::tempdir().unwrap();
            let test_name = format!(
                "{}::failed_resume_provisioning_preserves_checkpoint_and_projection_lineage",
                module_path!()
                    .strip_prefix("hel::")
                    .unwrap_or(module_path!())
            );
            let output = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", &test_name, "--nocapture"])
                .env(RESUME_ROLLBACK_TEST_CHILD, "1")
                .env("HEL_DATA_DIR", directory.path())
                .env("GH_TOKEN", "test-token")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "isolated resume rollback test failed\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }

        struct FailingPreflightExecutor;

        impl CommandExecutor for FailingPreflightExecutor {
            fn execute(&self, command: &CommandSpec) -> Result<CommandOutput> {
                assert_eq!(command.program, "podman");
                Ok(CommandOutput {
                    status: 1,
                    stdout: Vec::new(),
                    stderr: b"podman is temporarily unavailable".to_vec(),
                })
            }
        }

        let data_directory = PathBuf::from(std::env::var_os("HEL_DATA_DIR").unwrap());
        let archive_directory = data_directory.join("archives");
        std::fs::create_dir_all(&archive_directory).unwrap();
        let session_id = "0123456789abcdef0123456789abcdef";
        let checkpoint = write_checkpoint_gate_archive(&archive_directory, session_id, 7);
        let archive = verify_archive_streaming(&checkpoint.archive_path).unwrap();
        let expected_projection =
            materialized_session_from_canonical(session_id, &archive.canonical_session).unwrap();

        let mut session = checkpoint_test_session(session_id);
        session.state = SessionState::Archived;
        session.checkpoint = Some(checkpoint.clone());
        let previous = session.clone();
        let profile_home = data_directory.join("profile");
        std::fs::create_dir_all(&profile_home).unwrap();
        let mut config = HelConfig::default();
        config.profiles.insert(
            "codex".into(),
            HarnessProfile {
                kind: crate::hel_config::HarnessKind::Codex,
                home: profile_home,
                executable: None,
                environment: BTreeMap::new(),
                context_window_bytes: None,
            },
        );
        config.bundles.insert(
            "project".into(),
            ProjectBundle {
                primary_repo: "project".into(),
                repositories: vec![ProjectRepository {
                    id: "project".into(),
                    github: Some("example/project".into()),
                    local: None,
                    destination: "project".into(),
                    git_ref: None,
                }],
            },
        );
        config.targets.insert(
            "podman".into(),
            TargetTemplate::LocalPodman {
                container: ConfigContainer {
                    image: "example.invalid/hel-test:latest".into(),
                    platform: None,
                    cpus: None,
                    memory: None,
                    environment: BTreeMap::new(),
                },
            },
        );
        let mut controller = Controller {
            config,
            state: HelState {
                sessions: BTreeMap::from([(session_id.into(), session)]),
                ..HelState::default()
            },
        };
        crate::hel_database::save_state(&controller.state).unwrap();
        crate::hel_database::save_materialized_session(&expected_projection).unwrap();

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let error = runtime
            .block_on(controller.resume_session_controlled(
                session_id,
                "codex",
                "podman",
                SessionResumeOptions {
                    additional_mounts: None,
                    resource_allocation: None,
                    discard_queue: false,
                },
                &FailingPreflightExecutor,
            ))
            .unwrap_err();
        let detail = format!("{error:#}");
        assert!(detail.contains("returned to archived"), "{detail}");
        assert!(!detail.contains("unknown session"), "{detail}");

        let retained = controller.state.sessions.get(session_id).unwrap();
        assert_eq!(retained.state, SessionState::Archived);
        assert_eq!(retained.checkpoint, previous.checkpoint);
        assert_eq!(retained.managed_worktree, previous.managed_worktree);
        assert!(checkpoint.archive_path.is_file());

        let durable = crate::hel_database::load_state().unwrap();
        let durable_session = durable.sessions.get(session_id).unwrap();
        assert_eq!(durable_session.state, SessionState::Archived);
        assert_eq!(durable_session.checkpoint, previous.checkpoint);
        assert_eq!(
            crate::hel_database::load_materialized_session(session_id).unwrap(),
            Some(expected_projection)
        );
    }
    const RAW_CONVERSION_TEST_CHILD: &str = "HEL_RAW_CONVERSION_TEST_CHILD";
    #[test]
    fn a_failed_raw_conversion_keeps_the_bundle_and_leaves_the_worktree_alone() {
        // HEL_DATA_DIR and HEL_CONFIG_DIR are process-global, so run the half
        // that writes them in an exact child test.
        if std::env::var_os(RAW_CONVERSION_TEST_CHILD).is_none() {
            let directory = tempfile::tempdir().unwrap();
            let test_name = format!(
                "{}::a_failed_raw_conversion_keeps_the_bundle_and_leaves_the_worktree_alone",
                module_path!()
                    .strip_prefix("hel::")
                    .unwrap_or(module_path!())
            );
            let output = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", &test_name, "--nocapture"])
                .env(RAW_CONVERSION_TEST_CHILD, "1")
                .env("HEL_DATA_DIR", directory.path().join("data"))
                .env("HEL_CONFIG_DIR", directory.path().join("config"))
                .env("GH_TOKEN", "test-token")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "isolated raw conversion test failed\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }

        /// Real Git, no container runtime. Provisioning fails at preflight,
        /// after the conversion has already reshaped the record.
        struct GitWithoutPodmanExecutor;

        impl CommandExecutor for GitWithoutPodmanExecutor {
            fn execute(&self, command: &CommandSpec) -> Result<CommandOutput> {
                if command.program == "git" {
                    return ProcessExecutor.execute(command);
                }
                Ok(CommandOutput {
                    status: 1,
                    stdout: Vec::new(),
                    stderr: b"podman is temporarily unavailable".to_vec(),
                })
            }
        }

        let data_directory = PathBuf::from(std::env::var_os("HEL_DATA_DIR").unwrap());
        let archive_directory = data_directory.join("archives");
        std::fs::create_dir_all(&archive_directory).unwrap();
        std::fs::create_dir_all(crate::hel_config::config_dir()).unwrap();
        let session_id = "0123456789abcdef0123456789abcdef";
        let checkpoint = write_checkpoint_gate_archive(&archive_directory, session_id, 7);

        let repository = committed_repository();
        let mut session = managed_worktree_session(repository.path(), session_id);
        session.checkpoint = Some(checkpoint);
        let worktree = session.managed_worktree.clone().unwrap();
        let previous = session.clone();

        let profile_home = data_directory.join("profile");
        std::fs::create_dir_all(&profile_home).unwrap();
        let mut config = resume_compatibility_config();
        config.profiles.insert(
            "codex".into(),
            HarnessProfile {
                kind: crate::hel_config::HarnessKind::Codex,
                home: profile_home,
                executable: None,
                environment: BTreeMap::new(),
                context_window_bytes: None,
            },
        );
        let mut controller = Controller {
            config,
            state: HelState {
                sessions: BTreeMap::from([(session_id.into(), session)]),
                ..HelState::default()
            },
        };
        crate::hel_database::save_state(&controller.state).unwrap();

        let error = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(controller.resume_session_controlled(
                session_id,
                "codex",
                "podman",
                SessionResumeOptions {
                    additional_mounts: None,
                    resource_allocation: None,
                    discard_queue: false,
                },
                &GitWithoutPodmanExecutor,
            ))
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("returned to archived"),
            "{error:#}"
        );

        // The bundle stays: it was saved before the record referenced it, and a
        // retry reuses it instead of adding another.
        let (_, bundle) = controller
            .config
            .bundles
            .iter()
            .find(|(_, bundle)| bundle.repositories[0].local.as_deref() == Some(repository.path()))
            .expect("the conversion added a bundle for the checkout");
        assert_eq!(
            bundle.repositories[0].destination,
            PathBuf::from(session_id)
        );
        let saved = crate::hel_config::HelConfig::load().unwrap();
        assert_eq!(saved.bundles, controller.config.bundles);

        let retained = controller.state.sessions.get(session_id).unwrap();
        assert_eq!(retained.state, SessionState::Archived);
        assert_eq!(retained.project_directory, previous.project_directory);
        assert_eq!(retained.managed_worktree, previous.managed_worktree);
        assert_eq!(retained.bundle_id, previous.bundle_id);
        assert!(worktree.worktree_root.is_dir(), "the checkout stays put");
    }
}
