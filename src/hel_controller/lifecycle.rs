//! Session close, destroy, and delete transitions.

use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};

use crate::hel_session_manager::{SessionManagerControl, new_command_id};
use crate::hel_state::{CheckpointMetadata, SessionRecord, SessionState};
use crate::hel_targets::{self, CommandExecutor, ProcessExecutor};
use crate::hel_worker::{RelayCommand, RelayExecutionState};

use super::backend::backend_locator;
use super::checkpoint::{
    CheckpointExportPolicy, LatchExclusivity, prune_replaced_checkpoint,
    verify_installed_checkpoint_gate, wait_for_relay_closed,
};
use super::provisioning::retire_git_broker;
use super::worktree::cleanup_managed_worktree;
use super::{Controller, now, persist_session_record_transition_or_restore};

impl Controller {
    /// Checkpoint, ask the harness to close, and only then tear down the exact
    /// provisioned target. Checkpoint failure is deliberately non-destructive.
    pub async fn close_session(&mut self, session_id: &str) -> Result<()> {
        self.close_session_controlled(session_id, &ProcessExecutor)
            .await
    }

    pub async fn close_session_controlled(
        &mut self,
        session_id: &str,
        executor: &(impl CommandExecutor + Sync),
    ) -> Result<()> {
        self.close_session_controlled_with_manager(session_id, executor, None)
            .await
    }

    pub async fn close_session_managed_controlled(
        &mut self,
        session_id: &str,
        executor: &(impl CommandExecutor + Sync),
        manager: &SessionManagerControl,
    ) -> Result<()> {
        self.close_session_controlled_with_manager(session_id, executor, Some(manager))
            .await
    }

    async fn close_session_controlled_with_manager(
        &mut self,
        session_id: &str,
        executor: &(impl CommandExecutor + Sync),
        manager: Option<&SessionManagerControl>,
    ) -> Result<()> {
        let previous = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?
            .clone();
        let record = self.state.sessions.get_mut(session_id).unwrap();
        // Persist the close intent before beginning its checkpoint. A process
        // exit anywhere below must leave enough state for the next controller
        // to retry the close, even when no checkpoint has been installed yet.
        apply_close_checkpoint_started(record, now());
        self.persist_session_transition_or_restore(
            session_id,
            &previous,
            "persist closing state before checkpointing the session",
        )?;

        // Close seals the relay at the exact latched cursor, so this checkpoint
        // keeps its exclusive connection until the relay reports Closed.
        let mut latched = match self
            .checkpoint_session_latched(
                session_id,
                executor,
                manager,
                LatchExclusivity::HoldThroughClose,
                CheckpointExportPolicy::ReuseUnchangedArchive,
            )
            .await
        {
            Ok(latched) => latched,
            Err(error) => {
                let record = self.state.sessions.get_mut(session_id).unwrap();
                record.state = previous.state;
                record.updated_at = now();
                record.last_checkpoint_error = Some(format!("{error:#}"));
                return Err(
                    self.persist_failed_checkpoint_state_or_restore(session_id, &previous, error)
                );
            }
        };

        let artifact = latched.artifact.clone();
        let record = self.state.sessions.get_mut(session_id).unwrap();
        record.state = SessionState::Closing;
        record.native_session_id = Some(artifact.native_session_id.clone());
        record.checkpoint = Some(artifact.metadata.clone());
        record.updated_at = now();
        record.last_error = None;
        record.last_checkpoint_error = None;
        self.persist_checkpoint_transition_or_restore(
            session_id,
            &previous,
            "persist verified checkpoint and closing state before sealing the relay",
        )?;
        prune_replaced_checkpoint(previous.checkpoint.as_ref(), &artifact.metadata);

        let close_command_id = new_command_id("close")?;
        let barrier_command_id = latched.barrier_command_id.clone();
        if let Err(error) = latched
            .relay
            .connection_mut()
            .submit(
                close_command_id,
                RelayCommand::Close {
                    barrier_command_id: barrier_command_id.clone(),
                    expected: latched.cursor.clone(),
                },
            )
            .await
        {
            self.record_interrupted_close(session_id, &error)?;
            return Err(error.context("seal verified checkpoint for close"));
        }
        if let Err(error) = latched
            .relay
            .connection_mut()
            .submit(
                new_command_id("checkpoint-complete")?,
                RelayCommand::CompleteCheckpoint { barrier_command_id },
            )
            .await
        {
            self.record_interrupted_close(session_id, &error)?;
            return Err(error.context("release verified close checkpoint"));
        }
        if let Err(error) = wait_for_relay_closed(latched.relay.connection_mut()).await {
            self.record_interrupted_close(session_id, &error)?;
            return Err(error);
        }
        latched.relay.release();

        if let Err(error) =
            self.destroy_after_verified_checkpoint(session_id, &artifact.metadata, executor)
        {
            self.record_interrupted_close(session_id, &error)?;
            return Err(error);
        }
        Ok(())
    }

    /// Resume the durable closing state after a controller restart. If the
    /// relay had accepted Close, wait for it and destroy through the exact
    /// installed checkpoint gate. If it had not, take a fresh checkpoint;
    /// the previously installed archive may have become stale after EOF
    /// released its barrier.
    pub async fn recover_interrupted_close_managed(
        &mut self,
        session_id: &str,
        executor: &(impl CommandExecutor + Sync),
        manager: &SessionManagerControl,
    ) -> Result<()> {
        let (state, verified) = {
            let session = self
                .state
                .sessions
                .get(session_id)
                .with_context(|| format!("unknown session {session_id}"))?;
            ensure!(
                matches!(
                    session.state,
                    SessionState::Closing | SessionState::Destroying
                ),
                "session {session_id} has no interrupted close to recover"
            );
            (session.state, session.checkpoint.clone())
        };
        if state == SessionState::Destroying {
            let verified = verified.context("destroying session has no verified checkpoint")?;
            return self.destroy_after_verified_checkpoint(session_id, &verified, executor);
        }
        ensure!(
            state == SessionState::Closing,
            "session {session_id} has no relay close to recover"
        );
        let handle = manager
            .wait_for_session(session_id, Duration::from_secs(5))
            .await?;
        let mut lease = handle.lease_connection().await?;
        let execution = lease.connection_mut().sync().await?.operational.execution;
        match execution {
            RelayExecutionState::Closed => {}
            RelayExecutionState::Closing => {
                wait_for_relay_closed(lease.connection_mut()).await?;
            }
            RelayExecutionState::Idle | RelayExecutionState::Running => {
                lease.release();
                return self
                    .close_session_controlled_with_manager(session_id, executor, Some(manager))
                    .await;
            }
        }
        lease.release();
        let verified = verified.context("closed relay has no verified checkpoint")?;
        self.destroy_after_verified_checkpoint(session_id, &verified, executor)
    }

    fn record_interrupted_close(&mut self, session_id: &str, error: &anyhow::Error) -> Result<()> {
        let record = self.state.sessions.get_mut(session_id).unwrap();
        apply_interrupted_close_error(record, error, &now());
        self.persist_session_state(session_id)
    }

    /// Execute cleanup only after the close state machine has installed a
    /// verified checkpoint on the record.
    fn destroy_after_verified_checkpoint(
        &mut self,
        session_id: &str,
        verified: &CheckpointMetadata,
        executor: &impl CommandExecutor,
    ) -> Result<()> {
        self.destroy_after_verified_checkpoint_with(
            session_id,
            verified,
            executor,
            crate::hel_database::save_lifecycle_session,
        )
    }

    fn destroy_after_verified_checkpoint_with(
        &mut self,
        session_id: &str,
        verified: &CheckpointMetadata,
        executor: &impl CommandExecutor,
        persist: impl Fn(&SessionRecord) -> Result<()>,
    ) -> Result<()> {
        let session = self
            .state
            .sessions
            .get(session_id)
            .with_context(|| format!("unknown session {session_id}"))?
            .clone();
        ensure!(
            matches!(
                session.state,
                SessionState::Closing | SessionState::Destroying
            ),
            "refusing to destroy session {session_id}: it is not closing or destroying"
        );
        ensure!(
            session.checkpoint.as_ref() == Some(verified),
            "refusing to destroy session {session_id}: verified checkpoint gate is stale"
        );
        if session.state == SessionState::Closing {
            let record = self.state.sessions.get_mut(session_id).unwrap();
            record.state = SessionState::Destroying;
            record.updated_at = now();
            record.last_error = None;
            persist_session_record_transition_or_restore(
                &mut self.state,
                session_id,
                &session,
                "persist destroying state before target cleanup",
                &persist,
            )?;
        }

        let destroying = self
            .state
            .sessions
            .get(session_id)
            .expect("destroying session disappeared")
            .clone();
        verify_installed_checkpoint_gate(session_id, verified)?;
        // The session's local Git origin ends here. Stopping the broker before
        // the target it bridges into disappears is what keeps a normal close
        // from reading as an unexpected broker death.
        retire_git_broker(session_id).context("stop the session's local Git broker")?;
        let locator = destroying
            .target
            .as_ref()
            .context("session has no target")?;
        let backend = backend_locator(locator, &destroying, &self.config)?;
        if let Err(cleanup_error) = hel_targets::close_plan(&backend, session_id)?.execute(executor)
        {
            match hel_targets::cleanup_target_is_confirmed_absent(&backend, session_id, executor) {
                Ok(true) => {
                    tracing::warn!(
                        session_id,
                        error = format!("{cleanup_error:#}"),
                        "target cleanup command failed, but the target was confirmed absent"
                    );
                }
                Ok(false) => {
                    tracing::error!(
                        session_id,
                        error = format!("{cleanup_error:#}"),
                        "target cleanup failed and the target is still present"
                    );
                    return Err(cleanup_error);
                }
                Err(probe_error) => {
                    tracing::error!(
                        session_id,
                        cleanup_error = format!("{cleanup_error:#}"),
                        probe_error = format!("{probe_error:#}"),
                        "target cleanup failed and exact absence could not be confirmed"
                    );
                    return Err(cleanup_error.context(format!(
                        "target cleanup failed and exact absence could not be confirmed: {probe_error:#}"
                    )));
                }
            }
        }
        let record = self.state.sessions.get_mut(session_id).unwrap();
        record.state = SessionState::Stopped;
        record.target = None;
        record.updated_at = now();
        record.last_error = None;
        persist_session_record_transition_or_restore(
            &mut self.state,
            session_id,
            &destroying,
            "persist stopped state after target cleanup",
            &persist,
        )
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
        retire_git_broker(session_id).context("stop the session's local Git broker")?;
        if let Some(locator) = &session.target {
            let backend = backend_locator(locator, &session, &self.config)?;
            hel_targets::close_plan(&backend, session_id)?.execute(executor)?;
        }
        let record = self.state.sessions.get_mut(session_id).unwrap();
        record.state = SessionState::DestroyedWithDataLoss;
        record.target = None;
        record.updated_at = now();
        self.persist_session_state(session_id)
    }

    /// Permanently remove an inactive session and every artifact Hel owns for it.
    /// External cleanup happens before the durable record is dropped so failures
    /// remain visible and retryable.
    pub fn delete_session_controlled(
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
        if session.state.is_active() {
            bail!("refusing to delete active session {session_id}");
        }
        // A session deleted for good keeps nothing, including a broker an
        // earlier failure left running.
        retire_git_broker(session_id).context("stop the session's local Git broker")?;
        if let Some(worktree) = &session.managed_worktree {
            cleanup_managed_worktree(executor, worktree)
                .context("remove managed raw-session worktree")?;
        }
        if let Some(checkpoint) = &session.checkpoint
            && let Err(error) = std::fs::remove_file(&checkpoint.archive_path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            return Err(error).with_context(|| {
                format!(
                    "remove session recovery archive {}",
                    checkpoint.archive_path.display()
                )
            });
        }
        crate::hel_database::delete_session(session_id)
            .context("delete stopped session from database")?;
        self.state.remove_stopped_session(session_id)?;
        Ok(())
    }
}

fn apply_close_checkpoint_started(record: &mut SessionRecord, updated_at: String) {
    record.state = SessionState::Closing;
    record.updated_at = updated_at;
    record.last_checkpoint_error = None;
}

fn apply_interrupted_close_error(
    record: &mut SessionRecord,
    error: &anyhow::Error,
    updated_at: &str,
) {
    let destroying = record.state == SessionState::Destroying;
    if !destroying {
        record.state = SessionState::Closing;
    }
    record.updated_at = updated_at.to_owned();
    record.last_error = Some(if destroying {
        format!("target cleanup is safely retryable from its verified checkpoint: {error:#}")
    } else {
        format!("close is safely resumable from its verified checkpoint: {error:#}")
    });
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    use anyhow::Result;

    use crate::hel_config::{ContainerTemplate as ConfigContainer, HelConfig, TargetTemplate};
    use crate::hel_controller::Controller;
    use crate::hel_controller::test_support::{
        checkpoint_test_session, write_checkpoint_gate_archive,
    };
    use crate::hel_state::{HelState, SessionState, TargetLocator};
    use crate::hel_targets::{self, CommandExecutor, CommandOutput, CommandSpec};

    use super::*;

    #[test]
    fn starting_close_persists_its_intent_before_checkpointing() {
        let mut session = checkpoint_test_session("0123456789abcdef0123456789abcdef");
        session.state = SessionState::Running;
        session.last_checkpoint_error = Some("old failure".into());

        apply_close_checkpoint_started(&mut session, "2026-08-14T12:00:00Z".into());

        assert_eq!(session.state, SessionState::Closing);
        assert_eq!(session.updated_at, "2026-08-14T12:00:00Z");
        assert!(session.last_checkpoint_error.is_none());
    }
    #[test]
    fn target_cleanup_persists_destroying_and_rechecks_the_installed_archive() {
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

        let directory = tempfile::tempdir().unwrap();
        let session_id = "0123456789abcdef0123456789abcdef";
        let checkpoint = write_checkpoint_gate_archive(directory.path(), session_id, 7);
        let mut session = checkpoint_test_session(session_id);
        session.target_template_id = "local".into();
        session.state = SessionState::Closing;
        session.target = Some(TargetLocator::LocalBare {
            worker_root: directory.path().join(session_id),
        });
        session.checkpoint = Some(checkpoint.clone());
        let mut config = HelConfig::default();
        config
            .targets
            .insert("local".into(), TargetTemplate::LocalBare);
        let mut controller = Controller {
            config,
            state: HelState {
                sessions: BTreeMap::from([(session_id.into(), session)]),
                ..HelState::default()
            },
        };
        let executor = RecordingExecutor {
            commands: RefCell::new(Vec::new()),
        };
        let persisted = RefCell::new(Vec::new());

        controller
            .destroy_after_verified_checkpoint_with(session_id, &checkpoint, &executor, |record| {
                persisted.borrow_mut().push(record.state);
                Ok(())
            })
            .unwrap();

        assert_eq!(
            persisted.into_inner(),
            vec![SessionState::Destroying, SessionState::Stopped]
        );
        assert_eq!(executor.commands.borrow().len(), 1);
        let stopped = &controller.state.sessions[session_id];
        assert_eq!(stopped.state, SessionState::Stopped);
        assert!(stopped.target.is_none());
    }
    #[test]
    fn destroying_retry_blocks_cleanup_when_the_archive_gate_changed() {
        struct RecordingExecutor {
            calls: RefCell<usize>,
        }

        impl CommandExecutor for RecordingExecutor {
            fn execute(&self, _command: &CommandSpec) -> Result<CommandOutput> {
                *self.calls.borrow_mut() += 1;
                Ok(CommandOutput {
                    status: 0,
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                })
            }
        }

        let directory = tempfile::tempdir().unwrap();
        let session_id = "0123456789abcdef0123456789abcdef";
        let mut checkpoint = write_checkpoint_gate_archive(directory.path(), session_id, 7);
        checkpoint.event_frontier = 8;
        let mut session = checkpoint_test_session(session_id);
        session.target_template_id = "local".into();
        session.state = SessionState::Destroying;
        session.target = Some(TargetLocator::LocalBare {
            worker_root: directory.path().join(session_id),
        });
        session.checkpoint = Some(checkpoint.clone());
        let mut config = HelConfig::default();
        config
            .targets
            .insert("local".into(), TargetTemplate::LocalBare);
        let mut controller = Controller {
            config,
            state: HelState {
                sessions: BTreeMap::from([(session_id.into(), session)]),
                ..HelState::default()
            },
        };
        let executor = RecordingExecutor {
            calls: RefCell::new(0),
        };
        let persisted = RefCell::new(Vec::new());

        let error = controller
            .destroy_after_verified_checkpoint_with(session_id, &checkpoint, &executor, |record| {
                persisted.borrow_mut().push(record.state);
                Ok(())
            })
            .unwrap_err();

        assert!(error.to_string().contains("checkpoint frontier changed"));
        assert_eq!(*executor.calls.borrow(), 0);
        assert!(persisted.into_inner().is_empty());
        assert_eq!(
            controller.state.sessions[session_id].state,
            SessionState::Destroying
        );
    }
    #[test]
    fn destroying_retry_finalizes_when_apple_container_is_confirmed_absent() {
        struct AlreadyRemovedExecutor {
            commands: RefCell<Vec<CommandSpec>>,
        }

        impl CommandExecutor for AlreadyRemovedExecutor {
            fn execute(&self, command: &CommandSpec) -> Result<CommandOutput> {
                self.commands.borrow_mut().push(command.clone());
                if command
                    .args
                    .first()
                    .is_some_and(|argument| argument == "rm")
                {
                    Ok(CommandOutput {
                        status: 1,
                        stdout: Vec::new(),
                        stderr: b"container not found".to_vec(),
                    })
                } else {
                    Ok(CommandOutput {
                        status: 0,
                        stdout: Vec::new(),
                        stderr: Vec::new(),
                    })
                }
            }
        }

        let directory = tempfile::tempdir().unwrap();
        let session_id = "0123456789abcdef0123456789abcdef";
        let checkpoint = write_checkpoint_gate_archive(directory.path(), session_id, 7);
        let mut session = checkpoint_test_session(session_id);
        session.target_template_id = "apple".into();
        session.state = SessionState::Destroying;
        session.target = Some(TargetLocator::AppleContainer {
            container_id: hel_targets::resource_name(session_id).unwrap(),
        });
        session.checkpoint = Some(checkpoint.clone());
        let mut config = HelConfig::default();
        config.targets.insert(
            "apple".into(),
            TargetTemplate::AppleContainer {
                container: ConfigContainer {
                    image: "test:latest".into(),
                    pull_policy: Default::default(),
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
        let executor = AlreadyRemovedExecutor {
            commands: RefCell::new(Vec::new()),
        };
        let persisted = RefCell::new(Vec::new());

        controller
            .destroy_after_verified_checkpoint_with(session_id, &checkpoint, &executor, |record| {
                persisted.borrow_mut().push(record.state);
                Ok(())
            })
            .unwrap();

        let commands = executor.commands.borrow();
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0].args[0], "rm");
        assert_eq!(commands[1].args, ["list", "--all", "--quiet"]);
        assert_eq!(persisted.into_inner(), vec![SessionState::Stopped]);
        assert_eq!(
            controller.state.sessions[session_id].state,
            SessionState::Stopped
        );
    }
    /// Ending a session ends the local Git origin it was serving. Close,
    /// force-destroy, and permanent delete all retire the broker on purpose:
    /// its spec and lock file go, so nothing restarts it against a target
    /// that is being torn down, and its log stays for reading afterwards.
    #[cfg(unix)]
    #[test]
    fn every_session_ending_retires_its_local_git_broker() {
        const RETIREMENT_TEST_CHILD: &str = "HEL_TEST_BROKER_RETIREMENT_CHILD";

        struct SucceedingExecutor;

        impl CommandExecutor for SucceedingExecutor {
            fn execute(&self, _command: &CommandSpec) -> Result<CommandOutput> {
                Ok(CommandOutput {
                    status: 0,
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                })
            }
        }

        // HEL_DATA_DIR is process-global, so run the half that reads it in an
        // exact child test instead of racing unrelated tests in this process.
        if std::env::var_os(RETIREMENT_TEST_CHILD).is_none() {
            let directory = tempfile::tempdir().unwrap();
            let test_name = format!(
                "{}::every_session_ending_retires_its_local_git_broker",
                module_path!()
                    .strip_prefix("hel::")
                    .unwrap_or(module_path!())
            );
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", &test_name, "--nocapture"])
                .env(RETIREMENT_TEST_CHILD, "1")
                .env("HEL_DATA_DIR", directory.path())
                .output()
                .unwrap();
            let reported = String::from_utf8_lossy(&output.stdout).into_owned();
            assert!(
                output.status.success(),
                "isolated broker retirement test failed\nstdout:\n{reported}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
            // A filter that matches nothing also exits zero, so insist the
            // child really ran this test.
            assert!(
                reported.contains("1 passed"),
                "the isolated broker retirement test never ran\nstdout:\n{reported}"
            );
            return;
        }

        let brokers = crate::hel_config::data_dir().join("git-brokers");
        std::fs::create_dir_all(&brokers).unwrap();
        let seed_broker = |session_id: &str| {
            for (extension, contents) in [
                ("json", "{}"),
                // A PID file nobody holds a lock on: this session's broker is
                // already stopped, so ending the session only has to clear
                // what it would otherwise be restarted from.
                ("pid", "424242"),
                ("ready", "ready\n"),
                ("log", "broker log\n"),
            ] {
                std::fs::write(brokers.join(format!("{session_id}.{extension}")), contents)
                    .unwrap();
            }
        };
        let assert_retired = |session_id: &str| {
            for extension in ["json", "pid", "ready"] {
                let path = brokers.join(format!("{session_id}.{extension}"));
                assert!(!path.exists(), "{} outlived its session", path.display());
            }
            assert_eq!(
                std::fs::read_to_string(brokers.join(format!("{session_id}.log"))).unwrap(),
                "broker log\n",
                "the broker log must survive its session"
            );
        };

        let directory = tempfile::tempdir().unwrap();
        let closing = "0123456789abcdef0123456789abcdef";
        let forced = "0123456789abcdef0123456789abcdee";
        let deleted = "0123456789abcdef0123456789abcded";
        let checkpoint = write_checkpoint_gate_archive(directory.path(), closing, 7);
        let mut closing_session = checkpoint_test_session(closing);
        closing_session.target_template_id = "local".into();
        closing_session.state = SessionState::Closing;
        closing_session.target = Some(TargetLocator::LocalBare {
            worker_root: directory.path().join(closing),
        });
        closing_session.checkpoint = Some(checkpoint.clone());
        let mut forced_session = checkpoint_test_session(forced);
        forced_session.target_template_id = "local".into();
        forced_session.state = SessionState::Running;
        forced_session.target = Some(TargetLocator::LocalBare {
            worker_root: directory.path().join(forced),
        });
        // force_destroy persists through the database, so the row it updates
        // has to exist.
        crate::hel_database::save_session(&forced_session).unwrap();
        let mut deleted_session = checkpoint_test_session(deleted);
        deleted_session.target_template_id = "local".into();
        deleted_session.state = SessionState::Stopped;
        let mut config = HelConfig::default();
        config
            .targets
            .insert("local".into(), TargetTemplate::LocalBare);
        let mut controller = Controller {
            config,
            state: HelState {
                sessions: BTreeMap::from([
                    (closing.into(), closing_session),
                    (forced.into(), forced_session),
                    (deleted.into(), deleted_session),
                ]),
                ..HelState::default()
            },
        };
        for session_id in [closing, forced, deleted] {
            seed_broker(session_id);
        }

        controller
            .destroy_after_verified_checkpoint_with(
                closing,
                &checkpoint,
                &SucceedingExecutor,
                |_| Ok(()),
            )
            .unwrap();
        assert_retired(closing);

        controller
            .force_destroy(forced, &SucceedingExecutor)
            .unwrap();
        assert_retired(forced);

        controller
            .delete_session_controlled(deleted, &SucceedingExecutor)
            .unwrap();
        assert_retired(deleted);
    }
    #[test]
    fn interrupted_close_error_preserves_destroying_phase() {
        let session_id = "0123456789abcdef0123456789abcdef";
        let mut session = checkpoint_test_session(session_id);
        session.state = SessionState::Destroying;

        apply_interrupted_close_error(
            &mut session,
            &anyhow::anyhow!("podman unavailable"),
            "2026-08-14T12:00:00Z",
        );

        assert_eq!(session.state, SessionState::Destroying);
        assert_eq!(session.updated_at, "2026-08-14T12:00:00Z");
        assert!(
            session
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("cleanup is safely retryable"))
        );
    }
}
