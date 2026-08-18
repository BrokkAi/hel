//! The chat's background command channel: every relay operation the view can
//! ask for, the task that runs them, and how their results land back in state.

use agent_client_protocol::schema::v1::{ContentBlock, TextContent};

use crate::hel_elicitation::{ElicitationRequest, ElicitationResponse};
use crate::hel_session_manager::ManagedSessionHandle;
use crate::hel_state::{QueuedCommandKind, config_command_text};
use crate::hel_worker::RelayCommand;

use super::{ChatState, QueuedPrompt, queued_prompt_preview};

const CHAT_REMOTE_QUEUE_CAPACITY: usize = 32;

#[derive(Debug)]
pub(super) enum ChatRemoteOperation {
    Sync,
    Prompt {
        command_id: String,
        text: String,
        session_id: String,
        bundle_id: String,
    },
    RemoveQueuedPrompt {
        command_id: String,
        id: String,
        text: String,
        kind: QueuedCommandKind,
    },
    SetConfig {
        command_id: String,
        key: String,
        value: String,
    },
    SetSessionMode {
        command_id: String,
        mode_id: String,
    },
    Cancel {
        command_id: String,
    },
    RespondElicitation {
        request: ElicitationRequest,
        response: ElicitationResponse,
    },
}

#[derive(Debug)]
pub(super) enum ChatRemoteResult {
    Sync(std::result::Result<(), String>),
    Prompt {
        text: String,
        result: std::result::Result<(u64, Option<String>), String>,
    },
    RemoveQueuedPrompt {
        id: String,
        text: String,
        kind: QueuedCommandKind,
        result: std::result::Result<(), String>,
    },
    SetConfig {
        key: String,
        value: String,
        result: std::result::Result<(), String>,
    },
    SetSessionMode {
        mode_id: String,
        result: std::result::Result<(), String>,
    },
    Cancel(std::result::Result<(), String>),
    RespondElicitation {
        request: ElicitationRequest,
        result: std::result::Result<(), String>,
    },
    WorkerFailed(String),
}

impl ChatRemoteResult {
    fn failure_message(&self) -> Option<&str> {
        match self {
            Self::Sync(Err(error))
            | Self::Prompt {
                result: Err(error), ..
            }
            | Self::RemoveQueuedPrompt {
                result: Err(error), ..
            }
            | Self::SetConfig {
                result: Err(error), ..
            }
            | Self::SetSessionMode {
                result: Err(error), ..
            }
            | Self::Cancel(Err(error))
            | Self::RespondElicitation {
                result: Err(error), ..
            }
            | Self::WorkerFailed(error) => Some(error),
            Self::Prompt {
                result: Ok((_, Some(error))),
                ..
            } => Some(error),
            _ => None,
        }
    }
}

fn publish_chat_remote_result(
    results: &tokio::sync::mpsc::UnboundedSender<ChatRemoteResult>,
    attached: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    result: ChatRemoteResult,
) {
    if !attached.load(std::sync::atomic::Ordering::Acquire) {
        if let Some(error) = result.failure_message() {
            tracing::error!(%error, "detached chat operation failed");
        }
        return;
    }
    if let Err(error) = results.send(result)
        && let Some(error) = error.0.failure_message()
    {
        tracing::error!(%error, "chat operation failed after its UI closed");
    }
}

pub(super) struct ChatRemoteSupervisor {
    operations: Option<tokio::sync::mpsc::Sender<ChatRemoteOperation>>,
    results: tokio::sync::mpsc::UnboundedReceiver<ChatRemoteResult>,
    attached: std::sync::Arc<std::sync::atomic::AtomicBool>,
    worker: Option<tokio::task::JoinHandle<()>>,
}

impl ChatRemoteSupervisor {
    pub(super) fn spawn(session: ManagedSessionHandle) -> Self {
        let (operations_tx, operations_rx) = tokio::sync::mpsc::channel(CHAT_REMOTE_QUEUE_CAPACITY);
        let (results_tx, results_rx) = tokio::sync::mpsc::unbounded_channel();
        let attached = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let worker_attached = attached.clone();
        let worker = tokio::spawn(run_chat_remote_worker(
            session,
            operations_rx,
            results_tx,
            worker_attached,
        ));
        Self {
            operations: Some(operations_tx),
            results: results_rx,
            attached,
            worker: Some(worker),
        }
    }

    pub(super) fn operations(&self) -> &tokio::sync::mpsc::Sender<ChatRemoteOperation> {
        self.operations
            .as_ref()
            .expect("chat remote supervisor is attached")
    }

    pub(super) fn try_recv(
        &mut self,
    ) -> std::result::Result<ChatRemoteResult, tokio::sync::mpsc::error::TryRecvError> {
        self.results.try_recv()
    }

    /// Waits for the next result. `None` means the worker is gone and no
    /// further result can arrive, so the caller must stop awaiting this feed.
    /// Cancel safe: an unfinished `recv` takes no message.
    pub(super) async fn recv(&mut self) -> Option<ChatRemoteResult> {
        self.results.recv().await
    }

    pub(super) async fn take_finished(
        &mut self,
    ) -> Option<std::result::Result<(), tokio::task::JoinError>> {
        if !self
            .worker
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            return None;
        }
        Some(
            self.worker
                .take()
                .expect("finished chat worker exists")
                .await,
        )
    }
}

impl Drop for ChatRemoteSupervisor {
    fn drop(&mut self) {
        self.attached
            .store(false, std::sync::atomic::Ordering::Release);
        self.results.close();
        while let Ok(result) = self.results.try_recv() {
            if let Some(error) = result.failure_message() {
                tracing::error!(%error, "chat operation failed while detaching");
            }
        }
        drop(self.operations.take());
        let Some(worker) = self.worker.take() else {
            return;
        };
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(async move {
                    if let Err(error) = worker.await {
                        tracing::error!(%error, "detached chat background worker failed");
                    }
                });
            }
            Err(error) => {
                worker.abort();
                tracing::error!(%error, "could not supervise detached chat background worker");
            }
        }
    }
}

async fn run_chat_remote_worker(
    session: ManagedSessionHandle,
    mut operations: tokio::sync::mpsc::Receiver<ChatRemoteOperation>,
    results: tokio::sync::mpsc::UnboundedSender<ChatRemoteResult>,
    attached: std::sync::Arc<std::sync::atomic::AtomicBool>,
) {
    let mut pending = tokio::task::JoinSet::new();
    let mut accepting = true;
    loop {
        if !accepting && pending.is_empty() {
            break;
        }
        tokio::select! {
            operation = operations.recv(), if accepting => {
                let Some(operation) = operation else {
                    accepting = false;
                    continue;
                };
                enqueue_chat_remote_operation(
                    &session,
                    operation,
                    &mut pending,
                    &results,
                    &attached,
                ).await;
            }
            joined = pending.join_next(), if !pending.is_empty() => {
                if let Some(Err(error)) = joined {
                    publish_chat_remote_result(
                        &results,
                        &attached,
                        ChatRemoteResult::WorkerFailed(format!(
                            "chat background operation failed: {error}"
                        )),
                    );
                }
            }
        }
    }
}

async fn enqueue_chat_remote_operation(
    session: &ManagedSessionHandle,
    operation: ChatRemoteOperation,
    pending: &mut tokio::task::JoinSet<()>,
    results: &tokio::sync::mpsc::UnboundedSender<ChatRemoteResult>,
    attached: &std::sync::Arc<std::sync::atomic::AtomicBool>,
) {
    match operation {
        ChatRemoteOperation::Sync => match session.enqueue_sync().await {
            Ok(response) => {
                let results = results.clone();
                let attached = attached.clone();
                pending.spawn(async move {
                    let result = response.wait().await.map_err(|error| format!("{error:#}"));
                    publish_chat_remote_result(&results, &attached, ChatRemoteResult::Sync(result));
                });
            }
            Err(error) => {
                publish_chat_remote_result(
                    results,
                    attached,
                    ChatRemoteResult::Sync(Err(format!("{error:#}"))),
                );
            }
        },
        ChatRemoteOperation::Prompt {
            command_id,
            text,
            session_id,
            bundle_id,
        } => {
            let response = session
                .enqueue_submit(
                    command_id,
                    RelayCommand::Prompt {
                        prompt: vec![ContentBlock::Text(TextContent::new(text.clone()))],
                    },
                )
                .await;
            match response {
                Ok(response) => {
                    let results = results.clone();
                    let attached = attached.clone();
                    pending.spawn(async move {
                        let result = match response.wait().await {
                            Ok(ordinal) => {
                                let history_text = text.clone();
                                let history = tokio::task::spawn_blocking(move || {
                                    crate::hel_database::record_prompt(
                                        &session_id,
                                        &bundle_id,
                                        ordinal,
                                        None,
                                        &history_text,
                                    )
                                })
                                .await;
                                let warning = match history {
                                    Ok(Ok(())) => None,
                                    Ok(Err(error)) => Some(format!("{error:#}")),
                                    Err(error) => Some(format!("history task failed: {error}")),
                                };
                                Ok((ordinal, warning))
                            }
                            Err(error) => Err(format!("{error:#}")),
                        };
                        publish_chat_remote_result(
                            &results,
                            &attached,
                            ChatRemoteResult::Prompt { text, result },
                        );
                    });
                }
                Err(error) => {
                    publish_chat_remote_result(
                        results,
                        attached,
                        ChatRemoteResult::Prompt {
                            text,
                            result: Err(format!("{error:#}")),
                        },
                    );
                }
            }
        }
        ChatRemoteOperation::RemoveQueuedPrompt {
            command_id,
            id,
            text,
            kind,
        } => {
            let response = session
                .enqueue_submit(
                    command_id,
                    RelayCommand::RemoveQueuedPrompt {
                        queued_command_id: id.clone(),
                    },
                )
                .await;
            match response {
                Ok(response) => {
                    let results = results.clone();
                    let attached = attached.clone();
                    pending.spawn(async move {
                        let result = response
                            .wait()
                            .await
                            .map(|_| ())
                            .map_err(|error| format!("{error:#}"));
                        publish_chat_remote_result(
                            &results,
                            &attached,
                            ChatRemoteResult::RemoveQueuedPrompt {
                                id,
                                text,
                                kind,
                                result,
                            },
                        );
                    });
                }
                Err(error) => {
                    publish_chat_remote_result(
                        results,
                        attached,
                        ChatRemoteResult::RemoveQueuedPrompt {
                            id,
                            text,
                            kind,
                            result: Err(format!("{error:#}")),
                        },
                    );
                }
            }
        }
        ChatRemoteOperation::SetConfig {
            command_id,
            key,
            value,
        } => {
            let response = session
                .enqueue_submit(
                    command_id,
                    RelayCommand::SetConfig {
                        key: key.clone(),
                        value: value.clone(),
                    },
                )
                .await;
            match response {
                Ok(response) => {
                    let results = results.clone();
                    let attached = attached.clone();
                    pending.spawn(async move {
                        let result = response
                            .wait()
                            .await
                            .map(|_| ())
                            .map_err(|error| format!("{error:#}"));
                        publish_chat_remote_result(
                            &results,
                            &attached,
                            ChatRemoteResult::SetConfig { key, value, result },
                        );
                    });
                }
                Err(error) => {
                    publish_chat_remote_result(
                        results,
                        attached,
                        ChatRemoteResult::SetConfig {
                            key,
                            value,
                            result: Err(format!("{error:#}")),
                        },
                    );
                }
            }
        }
        ChatRemoteOperation::SetSessionMode {
            command_id,
            mode_id,
        } => {
            let response = session
                .enqueue_submit(
                    command_id,
                    RelayCommand::SetSessionMode {
                        mode_id: mode_id.clone(),
                    },
                )
                .await;
            match response {
                Ok(response) => {
                    let results = results.clone();
                    let attached = attached.clone();
                    pending.spawn(async move {
                        let result = response
                            .wait()
                            .await
                            .map(|_| ())
                            .map_err(|error| format!("{error:#}"));
                        publish_chat_remote_result(
                            &results,
                            &attached,
                            ChatRemoteResult::SetSessionMode { mode_id, result },
                        );
                    });
                }
                Err(error) => {
                    publish_chat_remote_result(
                        results,
                        attached,
                        ChatRemoteResult::SetSessionMode {
                            mode_id,
                            result: Err(format!("{error:#}")),
                        },
                    );
                }
            }
        }
        ChatRemoteOperation::Cancel { command_id } => {
            let response = session
                .enqueue_submit(command_id, RelayCommand::Cancel)
                .await;
            match response {
                Ok(response) => {
                    let results = results.clone();
                    let attached = attached.clone();
                    pending.spawn(async move {
                        let result = response
                            .wait()
                            .await
                            .map(|_| ())
                            .map_err(|error| format!("{error:#}"));
                        publish_chat_remote_result(
                            &results,
                            &attached,
                            ChatRemoteResult::Cancel(result),
                        );
                    });
                }
                Err(error) => {
                    publish_chat_remote_result(
                        results,
                        attached,
                        ChatRemoteResult::Cancel(Err(format!("{error:#}"))),
                    );
                }
            }
        }
        ChatRemoteOperation::RespondElicitation { request, response } => {
            let session = session.clone();
            let results = results.clone();
            let attached = attached.clone();
            pending.spawn(async move {
                let result = session
                    .respond_elicitation(request.id.clone(), response)
                    .await
                    .map_err(|error| format!("{error:#}"));
                publish_chat_remote_result(
                    &results,
                    &attached,
                    ChatRemoteResult::RespondElicitation { request, result },
                );
            });
        }
    }
}

pub(super) fn restore_unsent_input(chat: &mut ChatState, input: &str) {
    if chat.input.is_empty() {
        chat.set_input(input.to_owned());
    } else if chat.input != input {
        chat.set_input(format!("{input}\n\n{}", chat.input));
    }
}

pub(super) fn apply_chat_remote_result(chat: &mut ChatState, result: ChatRemoteResult) {
    match result {
        ChatRemoteResult::Sync(Ok(())) => chat.set_notice("Connected to session relay"),
        ChatRemoteResult::Sync(Err(error)) => chat.set_notice(format!("Connection failed: {error}")),
        ChatRemoteResult::Prompt {
            text,
            result: Ok((ordinal, None)),
        } => chat.set_notice(format!(
            "Prompt accepted by relay at {ordinal}: {}",
            queued_prompt_preview(&text)
        )),
        ChatRemoteResult::Prompt {
            text,
            result: Ok((ordinal, Some(history_error))),
        } => chat.set_notice(format!(
            "Prompt accepted by relay at {ordinal} ({}), but history was not saved: {history_error}",
            queued_prompt_preview(&text)
        )),
        ChatRemoteResult::Prompt {
            text,
            result: Err(error),
        } => {
            restore_unsent_input(chat, &text);
            chat.set_notice(format!("Prompt was not sent: {error}"));
        }
        ChatRemoteResult::RemoveQueuedPrompt {
            result: Ok(()), ..
        } => chat.set_notice("Queued prompt removed"),
        ChatRemoteResult::RemoveQueuedPrompt {
            id,
            text,
            kind,
            result: Err(error),
        } => {
            if !chat.queued_prompts.iter().any(|prompt| prompt.id == id) {
                chat.queued_prompts
                    .push_back(QueuedPrompt { id, text, kind });
            }
            chat.set_notice(format!("Queued prompt was not removed: {error}"));
        }
        ChatRemoteResult::SetConfig {
            result: Ok(()), ..
        } => chat.set_notice("Configuration update accepted"),
        ChatRemoteResult::SetConfig {
            key,
            value,
            result: Err(error),
        } => {
            restore_unsent_input(chat, &config_command_text(&key, &value));
            chat.set_notice(format!("Configuration was not changed: {error}"));
        }
        ChatRemoteResult::SetSessionMode {
            result: Ok(()), ..
        } => chat.set_notice("Session mode update accepted"),
        ChatRemoteResult::SetSessionMode {
            mode_id,
            result: Err(error),
        } => {
            // The optimistic toggle never happened, so drop it rather than
            // leave the status line claiming a mode the agent is not in.
            chat.current_mode = None;
            chat.set_notice(format!("Session mode was not changed to {mode_id}: {error}"));
        }
        ChatRemoteResult::Cancel(Ok(())) => chat.set_notice("Cancellation requested"),
        ChatRemoteResult::Cancel(Err(error)) => {
            chat.set_notice(format!("Cancellation failed: {error}"))
        }
        ChatRemoteResult::RespondElicitation { result: Ok(()), .. } => {
            chat.set_notice("Answer sent")
        }
        ChatRemoteResult::RespondElicitation {
            request,
            result: Err(error),
        } => {
            chat.restore_elicitation(request);
            chat.set_notice(format!("Answer was not sent: {error}"));
        }
        ChatRemoteResult::WorkerFailed(error) => chat.set_notice(error),
    }
}

pub(super) fn queue_chat_remote_operation(
    operations: &tokio::sync::mpsc::Sender<ChatRemoteOperation>,
    operation: ChatRemoteOperation,
    chat: &mut ChatState,
) {
    if let Err(error) = operations.try_send(operation) {
        let operation = error.into_inner();
        match operation {
            ChatRemoteOperation::Prompt { text, .. } => restore_unsent_input(chat, &text),
            ChatRemoteOperation::RemoveQueuedPrompt { id, text, kind, .. } => {
                if !chat.queued_prompts.iter().any(|prompt| prompt.id == id) {
                    chat.queued_prompts
                        .push_back(QueuedPrompt { id, text, kind });
                }
            }
            ChatRemoteOperation::SetConfig { key, value, .. } => {
                restore_unsent_input(chat, &config_command_text(&key, &value));
            }
            ChatRemoteOperation::SetSessionMode { .. } => chat.current_mode = None,
            ChatRemoteOperation::RespondElicitation { request, .. } => {
                chat.restore_elicitation(request)
            }
            ChatRemoteOperation::Sync | ChatRemoteOperation::Cancel { .. } => {}
        }
        chat.set_notice("The session command queue is full; the command was not sent");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hel_chat::test_support::snapshot;

    #[test]
    fn full_remote_queue_restores_unsent_input_without_blocking() {
        let (operations, _receiver) = tokio::sync::mpsc::channel(1);
        operations.try_send(ChatRemoteOperation::Sync).unwrap();
        let mut chat = ChatState::new(&snapshot(), &[]);
        chat.set_input("new draft".into());

        queue_chat_remote_operation(
            &operations,
            ChatRemoteOperation::Prompt {
                command_id: "prompt-1".into(),
                text: "unsent prompt".into(),
                session_id: "session-1".into(),
                bundle_id: "bundle-1".into(),
            },
            &mut chat,
        );

        assert_eq!(chat.input, "unsent prompt\n\nnew draft");
        assert!(
            chat.notice()
                .as_deref()
                .is_some_and(|notice| notice.contains("queue is full"))
        );
    }
}
