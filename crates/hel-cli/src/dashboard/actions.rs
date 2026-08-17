//! What the dashboard does with the work its key handling asks for.
//!
//! Every arm here either updates UI state directly or starts a background job;
//! none of them block the loop. `ControlFlow::Break` means the run is over.

use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Result, bail};
use hel::hel_controller::SessionResumeOptions;
use hel::hel_setup::SetupOutcome;
use hel::hel_targets::{CancellableProcessExecutor, ProcessExecutor};
use hel_tui::{DashboardAction, SessionOperationKind};

use crate::dashboard::io::{
    ContainerSettingsRequest, DashboardIoUpdate, LifecycleOperationRequest, StageReportingExecutor,
    config_only_controller, spawn_create_bundle, spawn_dashboard_container_settings,
    spawn_dashboard_create_session, spawn_dashboard_rename, spawn_io, spawn_lifecycle_operation,
};
use crate::dashboard::{DashboardContext, QUOTA_REFRESH_NOTICE, View, resume_progress_notice};
use crate::import::{DashboardImportSafety, PendingDashboardImport};
use crate::pollers::{LifecycleSuccess, spawn_aws_resource_options_resolution};
use crate::short_id;

/// Carries out one dashboard action. Breaking ends the run.
pub(crate) async fn apply_dashboard_action(
    context: &mut DashboardContext,
    action: DashboardAction,
) -> Result<ControlFlow<()>> {
    match action {
        DashboardAction::None => {}
        DashboardAction::QuitDetach => {
            context.quit_detached = true;
            return Ok(ControlFlow::Break(()));
        }
        DashboardAction::OpenConfig => match context.run_setup_dialog()? {
            SetupOutcome::Written => {
                context.controller.reload()?;
                context
                    .dashboard
                    .set_config(context.controller.config.clone());
                context
                    .dashboard
                    .set_state(context.controller.state.clone());
                context.request_quota_refresh();
                context.refresh_poll_targets();
                context
                    .dashboard
                    .set_notice("Setup complete. Press Ctrl+N to start your first session.");
            }
            SetupOutcome::Cancelled => context.dashboard.set_notice("Setup cancelled."),
        },
        DashboardAction::RefreshQuotas => {
            context.manual_quota_refresh_generation = Some(context.request_quota_refresh());
            context.dashboard.set_notice(QUOTA_REFRESH_NOTICE);
        }
        DashboardAction::OpenImport => context.start_import_discovery(),
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
                return Ok(ControlFlow::Continue(()));
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
            spawn_dashboard_rename(session_id, title, context.dashboard_io_tx.clone());
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
            );
        }
        DashboardAction::CompleteMountSource {
            target_template_id,
            prefix,
        } => {
            let config = context.controller.config.clone();
            let requested = prefix.clone();
            spawn_io(
                context.dashboard_io_tx.clone(),
                move || {
                    config_only_controller(config).complete_mount_source(
                        &target_template_id,
                        &requested,
                        &ProcessExecutor,
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
            spawn_io(
                context.dashboard_io_tx.clone(),
                move || {
                    config_only_controller(config).validate_mount_source(
                        &target_template_id,
                        std::path::Path::new(&requested),
                        &ProcessExecutor,
                    )
                },
                move |result| DashboardIoUpdate::MountValidation { source, result },
            );
        }
        DashboardAction::ValidateProjectDirectory {
            target_template_id,
            directory,
        } => {
            let config = context.controller.config.clone();
            let requested = directory.clone();
            spawn_io(
                context.dashboard_io_tx.clone(),
                move || {
                    config_only_controller(config).validate_project_directory(
                        &target_template_id,
                        std::path::Path::new(&requested),
                        &ProcessExecutor,
                    )
                },
                move |result| DashboardIoUpdate::ProjectValidation { directory, result },
            );
        }
        DashboardAction::CreateSession {
            profile_id,
            bundle_id,
            project_directory,
            target_template_id,
            additional_mounts,
            allow_dirty_local,
            resource_allocation,
        } => {
            context.dashboard.set_notice("Preparing session launch…");
            spawn_dashboard_create_session(
                DashboardAction::CreateSession {
                    profile_id,
                    bundle_id,
                    project_directory,
                    target_template_id,
                    additional_mounts,
                    allow_dirty_local,
                    resource_allocation,
                },
                context.dashboard_io_tx.clone(),
                context.lifecycle_updates_tx.clone(),
                tokio::runtime::Handle::current(),
            );
        }
        DashboardAction::Open { session_id } => {
            if context.hold_chat_session(&session_id).await.is_ok() {
                context.view = View::Chat;
            }
            context.dirty = true;
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
            // The wizard's chosen profile/target become the record's
            // last_profile/target_template_id as soon as resume starts
            // (Controller::resume_session_controlled), but this dashboard's
            // session snapshot won't see that update until the background
            // operation finishes. Tell the in-flight row where the resume is
            // going so it doesn't show where the session resumed from.
            context.dashboard.set_resume_destination(
                &session_id,
                profile_id.clone(),
                target_template_id.clone(),
            );
            let stage_updates = context.dashboard_io_tx.clone();
            let runtime = tokio::runtime::Handle::current();
            spawn_lifecycle_operation(request, move |controller, cancelled| {
                let executor = StageReportingExecutor::new(
                    CancellableProcessExecutor::new(cancelled),
                    session_id.clone(),
                    stage_updates,
                );
                let materialized = runtime.block_on(controller.resume_session_controlled(
                    &session_id,
                    &profile_id,
                    &target_template_id,
                    SessionResumeOptions {
                        additional_mounts: Some(additional_mounts),
                        resource_allocation,
                        discard_queue,
                    },
                    &executor,
                ))?;
                Ok(LifecycleSuccess::Resumed {
                    profile_id,
                    target_id: target_template_id,
                    materialized: Box::new(materialized),
                })
            });
        }
        DashboardAction::Close { session_id } => {
            context
                .dashboard
                .set_notice(format!("Pausing {}…", short_id(&session_id)));
            let request =
                context.begin_lifecycle_operation(&session_id, SessionOperationKind::Pausing, true);
            let session_manager = context.worker_commands_tx.clone();
            let runtime = tokio::runtime::Handle::current();
            spawn_lifecycle_operation(request, move |controller, cancelled| {
                let executor = CancellableProcessExecutor::new(cancelled);
                runtime.block_on(controller.close_session_managed_controlled(
                    &session_id,
                    &executor,
                    &session_manager,
                ))?;
                Ok(LifecycleSuccess::Closed)
            });
        }
        DashboardAction::ResolveAwsResourceOptions {
            target_template_ids,
        } => context.resolve_aws_resource_options(target_template_ids),
        DashboardAction::CreateBundle { source } => {
            context.dashboard.set_notice("Creating bundle…");
            spawn_create_bundle(source, context.dashboard_io_tx.clone());
        }
        DashboardAction::ForceDestroy { session_id } => {
            let request = context.begin_lifecycle_operation(
                &session_id,
                SessionOperationKind::Destroying,
                true,
            );
            spawn_lifecycle_operation(request, move |controller, cancelled| {
                let executor = CancellableProcessExecutor::new(cancelled);
                controller.force_destroy(&session_id, &executor)?;
                Ok(LifecycleSuccess::Destroyed)
            });
        }
        DashboardAction::DeleteActive { session_id } => {
            let request = context.begin_lifecycle_operation(
                &session_id,
                SessionOperationKind::Deleting,
                true,
            );
            spawn_lifecycle_operation(request, move |controller, cancelled| {
                let executor = CancellableProcessExecutor::new(cancelled);
                controller.force_destroy(&session_id, &executor)?;
                controller.delete_session_controlled(&session_id, &executor)?;
                Ok(LifecycleSuccess::DeletedActive)
            });
        }
        DashboardAction::DeleteArchived { session_id } => {
            // An archived session has no worker, so nothing is copying it and
            // no recovery reservation is needed.
            let request = context.begin_lifecycle_operation(
                &session_id,
                SessionOperationKind::Deleting,
                false,
            );
            spawn_lifecycle_operation(request, move |controller, cancelled| {
                if cancelled.load(Ordering::Acquire) {
                    bail!("operation cancelled");
                }
                let executor = CancellableProcessExecutor::new(cancelled);
                controller.delete_session_controlled(&session_id, &executor)?;
                Ok(LifecycleSuccess::DeletedArchived)
            });
        }
        DashboardAction::CancelOperation { session_id } => {
            if let Some(operation) = context.lifecycle_operations.get(&session_id) {
                operation.cancelled.store(true, Ordering::Release);
                context.dashboard.set_notice(format!(
                    "Cancelling {} for {}…",
                    operation.kind.label().to_ascii_lowercase(),
                    short_id(&session_id)
                ));
            }
        }
    }
    Ok(ControlFlow::Continue(()))
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
                );
            }
        }
    }
}
