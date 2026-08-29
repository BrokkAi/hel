//! What the dashboard does with the work its key handling asks for.
//!
//! Every arm here either updates UI state directly or starts a background job;
//! none of them block the loop.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Result, bail};
use hel::hel_controller::{Controller, ResumeRepositorySourceReceipt};
use hel::hel_setup::SetupOutcome;
use hel::hel_targets::CancellableProcessExecutor;
use hel_tui::{DashboardAction, SessionOperationKind};

use crate::daemon;
use crate::dashboard::io::{
    ArchiveWriteTarget, ContainerSettingsRequest, DashboardIoUpdate, LifecycleOperationRequest,
    ResumeRepositoryPreflightApply, config_only_controller, spawn_archive_write,
    spawn_cancellable_io, spawn_clipboard_read, spawn_create_bundle,
    spawn_dashboard_container_settings, spawn_dashboard_create_session, spawn_dashboard_rename,
    spawn_io, spawn_lifecycle_operation,
};
use crate::dashboard::{DashboardContext, QUOTA_REFRESH_NOTICE, resume_progress_notice};
use crate::import::{DashboardImportSafety, PendingDashboardImport};
use crate::pollers::{LifecycleSuccess, spawn_aws_resource_options_resolution};
use crate::short_id;

/// Carries out one dashboard action.
pub(crate) async fn apply_dashboard_action(
    context: &mut DashboardContext,
    action: DashboardAction,
) -> Result<()> {
    match action {
        DashboardAction::None => {}
        DashboardAction::QuitDetach => {
            context.request_shutdown();
        }
        DashboardAction::OpenWorkspacePicker => {
            context.request_workspace_switch();
        }
        DashboardAction::OpenConfig => match context.run_setup_dialog()? {
            SetupOutcome::Written => {
                context.dashboard.set_notice("Saving setup changes…");
                let workspace_id = context.workspace_id.clone();
                let client_id = context.client_id.clone();
                spawn_io(
                    "reload setup state",
                    context.dashboard_io_tx.clone(),
                    move || {
                        let mut controller = Controller::load()?;
                        super::retain_workspace_sessions(
                            &mut controller,
                            &workspace_id,
                            &client_id,
                        )?;
                        Ok(controller)
                    },
                    DashboardIoUpdate::SetupReloaded,
                );
            }
            SetupOutcome::Cancelled => context.dashboard.set_notice("Setup cancelled."),
        },
        DashboardAction::RefreshQuotas => {
            context.manual_quota_refresh_generation = Some(context.request_quota_refresh());
            context.dashboard.set_notice(QUOTA_REFRESH_NOTICE);
        }
        DashboardAction::PasteFromClipboard => {
            if !context.clipboard_read_in_flight {
                context.clipboard_read_in_flight = true;
                context.dashboard.set_notice("Reading clipboard…");
                spawn_clipboard_read(context.dashboard_io_tx.clone());
            }
        }
        DashboardAction::MarkAllRead { receipts } => {
            context.acknowledge_dashboard_sessions(receipts);
        }
        DashboardAction::OpenResumeDialog => context.start_resume_discovery(),
        DashboardAction::SetSessionArchived {
            session_id,
            archived,
        } => {
            let what = format!("session {}", short_id(&session_id));
            let id = session_id.clone();
            spawn_archive_write(
                what,
                ArchiveWriteTarget::Session {
                    session_id: session_id.clone(),
                    archived: !archived,
                },
                move || hel::hel_database::set_session_archived(&id, archived),
                context.dashboard_io_tx.clone(),
                context.critical_operations.clone(),
            );
            // The in-memory record the dashboard already updated is the one a
            // later reload compares against, so keep the controller in step.
            if let Some(session) = context.controller.state.sessions.get_mut(&session_id) {
                session.archived = archived;
            }
        }
        DashboardAction::SetNativeSessionHidden {
            harness_kind,
            native_session_id,
            hidden,
        } => {
            let what = format!("native session {}", short_id(&native_session_id));
            spawn_archive_write(
                what,
                ArchiveWriteTarget::HiddenNativeSessions,
                move || {
                    hel::hel_database::set_native_session_hidden(
                        harness_kind,
                        &native_session_id,
                        hidden,
                    )
                },
                context.dashboard_io_tx.clone(),
                context.critical_operations.clone(),
            );
        }
        DashboardAction::ImportSession {
            profile_id,
            native_session_id,
            display_title,
        } => context.start_import(
            PendingDashboardImport {
                profile_id,
                native_session_id,
                display_title,
            },
            DashboardImportSafety {
                accepted: false,
                include_untracked: true,
            },
        ),
        DashboardAction::CancelImport => {
            if let Some(active) = context.active_import.take() {
                active.cancelled.store(true, Ordering::Release);
                context.dashboard.finish_import();
                context
                    .dashboard
                    .set_notice("Import cancellation requested; no Hel state will be changed.");
            }
        }
        DashboardAction::ConfirmImportBundle {
            accepted,
            include_untracked,
        } => {
            let Some(pending) = context.pending_import.take() else {
                context.dashboard.finish_import();
                context.dashboard.set_notice("Import confirmation expired.");
                return Ok(());
            };
            if accepted {
                context.start_import(
                    pending,
                    DashboardImportSafety {
                        accepted: true,
                        include_untracked,
                    },
                );
            } else {
                context.dashboard.finish_import();
                context
                    .dashboard
                    .set_notice("Import cancelled; no Hel files were changed.");
            }
        }
        DashboardAction::RenameSession { session_id, title } => {
            context.dashboard.set_notice("Renaming session…");
            spawn_dashboard_rename(
                session_id,
                title,
                context.dashboard_io_tx.clone(),
                context.critical_operations.clone(),
            );
        }
        DashboardAction::SaveContainerSettings {
            session_id,
            cpus,
            memory,
            additional_mounts,
            mount_history,
        } => {
            context
                .dashboard
                .set_notice("Saving container size and mounts…");
            spawn_dashboard_container_settings(
                ContainerSettingsRequest {
                    session_id,
                    cpus,
                    memory,
                    additional_mounts,
                    mount_history,
                },
                context.dashboard_io_tx.clone(),
                context.critical_operations.clone(),
            );
        }
        DashboardAction::CompleteMountSource {
            target_template_id,
            prefix,
        } => {
            let config = context.controller.config.clone();
            let requested = prefix.clone();
            spawn_cancellable_io(
                context.critical_operations.clone(),
                "completing attached directory",
                context.dashboard_io_tx.clone(),
                move |cancelled| {
                    let executor = CancellableProcessExecutor::new(cancelled);
                    config_only_controller(config).complete_mount_source(
                        &target_template_id,
                        &requested,
                        &executor,
                    )
                },
                move |result| DashboardIoUpdate::MountCompletions { prefix, result },
            );
        }
        DashboardAction::ValidateMountSource {
            target_template_id,
            source,
        } => {
            let config = context.controller.config.clone();
            let requested = source.clone();
            spawn_cancellable_io(
                context.critical_operations.clone(),
                "validating attached directory",
                context.dashboard_io_tx.clone(),
                move |cancelled| {
                    let executor = CancellableProcessExecutor::new(cancelled);
                    config_only_controller(config).validate_mount_source(
                        &target_template_id,
                        std::path::Path::new(&requested),
                        &executor,
                    )
                },
                move |result| DashboardIoUpdate::MountValidation { source, result },
            );
        }
        DashboardAction::ValidateSessionMounts {
            target_template_id,
            mounts,
            launch,
        } => {
            context
                .dashboard
                .set_notice("Checking attached directories…");
            let config = context.controller.config.clone();
            spawn_cancellable_io(
                context.critical_operations.clone(),
                "validating session directories",
                context.dashboard_io_tx.clone(),
                move |cancelled| {
                    let controller = config_only_controller(config);
                    let executor = CancellableProcessExecutor::new(cancelled);
                    for mount in mounts {
                        if let Err(error) = controller.validate_mount_source(
                            &target_template_id,
                            std::path::Path::new(&mount.source),
                            &executor,
                        ) {
                            return Ok(Some((
                                mount.source.to_string_lossy().into_owned(),
                                format!("{error:#}"),
                            )));
                        }
                    }
                    Ok(None)
                },
                move |result| DashboardIoUpdate::SessionMountValidation { launch, result },
            );
        }
        DashboardAction::PreflightResumeRepositories { launch } => {
            start_resume_repository_preflight(context, launch)?;
        }
        DashboardAction::ReplaceResumeRepositoryOrigin {
            session_id,
            repository_id,
            replacement,
            launch,
        } => {
            context.dashboard.set_notice("Checking replacement origin…");
            let submitted_repository_id = repository_id.clone();
            spawn_cancellable_io(
                context.critical_operations.clone(),
                format!("updating repository for {}", short_id(&session_id)),
                context.dashboard_io_tx.clone(),
                move |cancelled| {
                    let executor = CancellableProcessExecutor::new(cancelled);
                    let mut controller = Controller::load()?;
                    let preflight = controller.replace_resume_repository_origin(
                        &session_id,
                        &repository_id,
                        &replacement,
                        &executor,
                    )?;
                    Ok(ResumeRepositoryPreflightApply {
                        config: Some(controller.config),
                        preflight,
                    })
                },
                move |result| DashboardIoUpdate::ResumeRepositoryPreflight {
                    launch,
                    submitted_repository_id: Some(submitted_repository_id),
                    result: Box::new(result),
                },
            );
        }
        DashboardAction::ValidateProjectDirectory {
            target_template_id,
            directory,
        } => {
            let config = context.controller.config.clone();
            let requested = directory.clone();
            spawn_cancellable_io(
                context.critical_operations.clone(),
                "validating project directory",
                context.dashboard_io_tx.clone(),
                move |cancelled| {
                    let executor = CancellableProcessExecutor::new(cancelled);
                    config_only_controller(config).validate_project_directory(
                        &target_template_id,
                        std::path::Path::new(&requested),
                        &executor,
                    )
                },
                move |result| DashboardIoUpdate::ProjectValidation { directory, result },
            );
        }
        action @ DashboardAction::CreateSession { .. } => start_session_launch(context, action),
        DashboardAction::Open { session_id } => {
            context.open_chat_session(&session_id);
        }
        action @ DashboardAction::ResumeSession { .. } => start_session_launch(context, action),
        DashboardAction::Close { session_id } => {
            context
                .dashboard
                .set_notice(format!("Stopping {}…", short_id(&session_id)));
            let request = context.begin_lifecycle_operation(
                &session_id,
                SessionOperationKind::Stopping,
                true,
            );
            let runtime = tokio::runtime::Handle::current();
            spawn_lifecycle_operation(
                request,
                context.critical_operations.clone(),
                move |_controller, _cancelled| {
                    runtime.block_on(async {
                        daemon::connect_or_start()
                            .await?
                            .close_session(session_id)
                            .await
                    })?;
                    Ok(LifecycleSuccess::Closed)
                },
            );
        }
        DashboardAction::ResolveAwsResourceOptions {
            target_template_ids,
        } => context.resolve_aws_resource_options(target_template_ids),
        DashboardAction::CreateBundle { source } => {
            context.dashboard.set_notice("Creating bundle…");
            spawn_create_bundle(
                source,
                context.dashboard_io_tx.clone(),
                context.critical_operations.clone(),
            );
        }
        DashboardAction::ForceStop { session_id } => {
            let request = context.begin_lifecycle_operation(
                &session_id,
                SessionOperationKind::Stopping,
                true,
            );
            spawn_lifecycle_operation(
                request,
                context.critical_operations.clone(),
                move |_controller, _cancelled| {
                    tokio::runtime::Handle::current().block_on(async {
                        daemon::connect_or_start()
                            .await?
                            .force_stop_session(session_id)
                            .await
                    })?;
                    Ok(LifecycleSuccess::ForceStopped)
                },
            );
        }
        DashboardAction::DestroyStopped { session_id } => {
            // A stopped session has no worker, so nothing is copying it and
            // no recovery reservation is needed.
            let request = context.begin_lifecycle_operation(
                &session_id,
                SessionOperationKind::Destroying,
                false,
            );
            spawn_lifecycle_operation(
                request,
                context.critical_operations.clone(),
                move |_controller, _cancelled| {
                    tokio::runtime::Handle::current().block_on(async {
                        daemon::connect_or_start()
                            .await?
                            .destroy_stopped_session(session_id)
                            .await
                    })?;
                    Ok(LifecycleSuccess::DestroyedStopped)
                },
            );
        }
        DashboardAction::CancelOperation { session_id } => {
            if let Some(operation) = context.lifecycle_operations.get(&session_id) {
                operation.cancelled.store(true, Ordering::Release);
                if operation.kind != SessionOperationKind::Launching {
                    let daemon_session_id = session_id.clone();
                    tokio::spawn(async move {
                        let result = async {
                            daemon::connect_or_start()
                                .await?
                                .cancel_lifecycle(daemon_session_id)
                                .await
                        }
                        .await;
                        if let Err(error) = result {
                            tracing::warn!(%error, "could not cancel daemon lifecycle operation");
                        }
                    });
                }
                context.dashboard.set_notice(format!(
                    "Cancelling {} for {}…",
                    operation.kind.label().to_ascii_lowercase(),
                    short_id(&session_id)
                ));
            }
        }
    }
    Ok(())
}

fn resume_launch_destination(action: &DashboardAction) -> Result<(&str, &str)> {
    match action {
        DashboardAction::ResumeSession {
            session_id,
            target_template_id,
            ..
        } => Ok((session_id, target_template_id)),
        _ => bail!("repository preflight did not receive a resume launch"),
    }
}

pub(crate) fn start_resume_repository_preflight(
    context: &mut DashboardContext,
    launch: Box<DashboardAction>,
) -> Result<()> {
    let (session_id, target_id) = resume_launch_destination(&launch)?;
    let session_id = session_id.to_owned();
    let target_id = target_id.to_owned();
    context
        .dashboard
        .set_notice("Checking checkpoint repository history…");
    spawn_cancellable_io(
        context.critical_operations.clone(),
        format!("checking repositories for {}", short_id(&session_id)),
        context.dashboard_io_tx.clone(),
        move |cancelled| {
            let executor = CancellableProcessExecutor::new(cancelled);
            let controller = Controller::load()?;
            Ok(ResumeRepositoryPreflightApply {
                config: None,
                preflight: controller.preflight_resume_repository_sources(
                    &session_id,
                    &target_id,
                    &executor,
                )?,
            })
        },
        move |result| DashboardIoUpdate::ResumeRepositoryPreflight {
            launch,
            submitted_repository_id: None,
            result: Box::new(result),
        },
    );
    Ok(())
}

pub(crate) fn start_session_launch(context: &mut DashboardContext, action: DashboardAction) {
    start_session_launch_with_repository_preflight(context, action, None);
}

pub(crate) fn start_preflighted_session_launch(
    context: &mut DashboardContext,
    action: DashboardAction,
    repository_preflight: ResumeRepositorySourceReceipt,
) {
    start_session_launch_with_repository_preflight(context, action, Some(repository_preflight));
}

fn start_session_launch_with_repository_preflight(
    context: &mut DashboardContext,
    action: DashboardAction,
    repository_preflight: Option<ResumeRepositorySourceReceipt>,
) {
    match action {
        action @ DashboardAction::CreateSession { .. } => {
            debug_assert!(repository_preflight.is_none());
            context.dashboard.set_notice("Preparing session launch…");
            spawn_dashboard_create_session(
                action,
                context.workspace_id.clone(),
                context.dashboard_io_tx.clone(),
                context.lifecycle_updates_tx.clone(),
                tokio::runtime::Handle::current(),
                context.critical_operations.clone(),
            );
        }
        DashboardAction::ResumeSession {
            session_id,
            profile_id,
            target_template_id,
            additional_mounts,
            resource_allocation,
            discard_queue,
        } => {
            context.dashboard.set_notice(resume_progress_notice(
                &session_id,
                &profile_id,
                &target_template_id,
            ));
            let request = context.begin_lifecycle_operation(
                &session_id,
                SessionOperationKind::Resuming,
                true,
            );
            context.dashboard.set_resume_destination(
                &session_id,
                profile_id.clone(),
                target_template_id.clone(),
            );
            let runtime = tokio::runtime::Handle::current();
            spawn_lifecycle_operation(
                request,
                context.critical_operations.clone(),
                move |_controller, _cancelled| {
                    let materialized = runtime.block_on(async {
                        daemon::connect_or_start()
                            .await?
                            .resume_session(daemon::ResumeSessionRequest {
                                session_id: session_id.clone(),
                                profile_id: profile_id.clone(),
                                target_template_id: target_template_id.clone(),
                                additional_mounts: Some(additional_mounts),
                                resource_allocation,
                                discard_queue,
                                repository_preflight,
                            })
                            .await
                    })?;
                    Ok(LifecycleSuccess::Resumed {
                        profile_id,
                        target_id: target_template_id,
                        materialized: Box::new(materialized),
                    })
                },
            );
        }
        _ => unreachable!("mount preflight only carries create or resume actions"),
    }
}

impl DashboardContext {
    /// Marks a session busy in the UI and hands back what runs its operation
    /// off the loop. `reserve_recovery` is for operations that must preempt a
    /// recovery copy already running for the session.
    fn begin_lifecycle_operation(
        &mut self,
        session_id: &str,
        kind: SessionOperationKind,
        reserve_recovery: bool,
    ) -> LifecycleOperationRequest {
        self.dashboard
            .begin_session_operation(session_id.to_owned(), kind, None);
        let cancelled = Arc::new(AtomicBool::new(false));
        self.lifecycle_operations.insert(
            session_id.to_owned(),
            crate::dashboard::io::ActiveLifecycleOperation {
                cancelled: cancelled.clone(),
                kind,
            },
        );
        LifecycleOperationRequest {
            session_id: session_id.to_owned(),
            kind,
            cancelled,
            recovery: reserve_recovery.then(|| self.recovery_observer.clone()),
            updates: self.lifecycle_updates_tx.clone(),
        }
    }

    /// Resolves the instance sizes a deployment target offers, once per target.
    pub(crate) fn resolve_aws_resource_options(&mut self, target_template_ids: Vec<String>) {
        for target_template_id in target_template_ids {
            if self
                .resolving_aws_resource_options
                .insert(target_template_id.clone())
            {
                spawn_aws_resource_options_resolution(
                    self.controller.config.clone(),
                    target_template_id,
                    self.aws_resource_options_tx.clone(),
                    self.critical_operations.clone(),
                );
            }
        }
    }
}
