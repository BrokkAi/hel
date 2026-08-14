//! Controller-owned projection of the durable ACP relay stream.

use std::collections::BTreeMap;

use agent_client_protocol::schema::v1::{SessionUpdate, ToolCall};
use anyhow::{Context, Result, bail};
use serde_json::Value;

use crate::hel_archive::{
    CanonicalExecutionState, CanonicalQueuedPrompt, CanonicalSessionSnapshot,
    CanonicalSessionState, CanonicalTranscriptBody, CanonicalTranscriptItem,
};
use crate::hel_database::{MaterializedSessionMutation, TranscriptMutation};
use crate::hel_state::{
    MaterializedExecutionState, MaterializedQueuedPrompt, MaterializedSession, TranscriptBody,
    TranscriptItem, normalize_session_title,
};
use crate::hel_worker::{
    RelayCommand, RelayCommandKind, RelayEvent, RelayObservation, validate_relay_event,
};

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectedRelayEvent {
    pub mutation: MaterializedSessionMutation,
}

/// Derive the minimal mutation for exactly the next relay event. This clones
/// only logical items touched by the event; the actor-owned transcript is not
/// copied.
pub fn project_relay_event(
    current: &MaterializedSession,
    event: &RelayEvent,
) -> Result<ProjectedRelayEvent> {
    validate_relay_event(
        current.applied_event_ordinal,
        &current.applied_event_digest,
        event,
    )?;

    let mut mutation = MaterializedSessionMutation {
        last_activity_at_ms: Some(event.recorded_at_ms),
        ..MaterializedSessionMutation::default()
    };
    project_observation(current, event, &mut mutation)?;
    Ok(ProjectedRelayEvent { mutation })
}

/// Apply a mutation to the actor's sole in-memory projection after the same
/// mutation and frontier have committed atomically in SQLite.
pub fn apply_committed_projection_event(
    current: &mut MaterializedSession,
    event: &RelayEvent,
    mutation: &MaterializedSessionMutation,
) -> Result<()> {
    validate_relay_event(
        current.applied_event_ordinal,
        &current.applied_event_digest,
        event,
    )?;
    if let Some(execution) = mutation.execution {
        current.execution = execution;
    }
    if let Some(title) = &mutation.session_title {
        current.session_title.clone_from(title);
    }
    if let Some(configuration) = &mutation.configuration {
        current.configuration.clone_from(configuration);
    }
    for item_mutation in &mutation.transcript {
        match item_mutation {
            TranscriptMutation::Upsert(item) => {
                item.validate(event.ordinal)?;
                if let Some(existing) = current
                    .transcript
                    .iter_mut()
                    .find(|existing| existing.stable_id == item.stable_id)
                {
                    if existing.position != item.position
                        || existing.created_at_ms != item.created_at_ms
                    {
                        bail!(
                            "transcript item {:?} changed immutable identity fields",
                            item.stable_id
                        );
                    }
                    if item.last_changed_at_ms < existing.last_changed_at_ms {
                        bail!(
                            "transcript item {:?} moved its changed timestamp backwards",
                            item.stable_id
                        );
                    }
                    if existing
                        .latest_content_event_ordinal
                        .is_some_and(|existing| {
                            item.latest_content_event_ordinal
                                .is_none_or(|next| next < existing)
                        })
                    {
                        bail!(
                            "transcript item {:?} moved its latest content ordinal backwards",
                            item.stable_id
                        );
                    }
                    existing.clone_from(item);
                } else {
                    current.transcript.push(item.clone());
                }
            }
            TranscriptMutation::Remove { stable_id } => {
                current
                    .transcript
                    .retain(|item| item.stable_id != *stable_id);
            }
        }
    }
    if let Some(queued_prompts) = &mutation.queued_prompts {
        current.queued_prompts.clone_from(queued_prompts);
    }
    if let Some(activity) = mutation.last_activity_at_ms {
        current.last_activity_at_ms = Some(
            current
                .last_activity_at_ms
                .map_or(activity, |existing| existing.max(activity)),
        );
    }
    current.applied_event_ordinal = event.ordinal;
    current.applied_event_digest.clone_from(&event.digest);
    Ok(())
}

fn project_observation(
    current: &MaterializedSession,
    event: &RelayEvent,
    mutation: &mut MaterializedSessionMutation,
) -> Result<()> {
    match &event.observation {
        RelayObservation::AgentInitialized { .. } => {}
        RelayObservation::SessionOpened { resumed, .. } => push_system(
            mutation,
            event,
            if *resumed {
                "harness session resumed"
            } else {
                "harness session started"
            },
        ),
        RelayObservation::SessionConfigured { config_options } => {
            mutation.configuration = Some(configuration_values(config_options));
        }
        RelayObservation::SessionUpdate { update } => {
            project_session_update(current, event, update, mutation)?;
        }
        RelayObservation::PermissionAutoApproved {
            option_id,
            option_name,
        } => push_system(
            mutation,
            event,
            format!("permission auto-approved: {option_name} ({option_id})"),
        ),
        RelayObservation::CommandQueued {
            command_id,
            command,
            created_at_ms,
        } => match command {
            RelayCommand::Prompt { prompt } => {
                let mut queue = current.queued_prompts.clone();
                queue.retain(|queued| queued.command_id != *command_id);
                queue.push(MaterializedQueuedPrompt {
                    command_id: command_id.clone(),
                    content: prompt
                        .iter()
                        .map(serde_json::to_value)
                        .collect::<serde_json::Result<_>>()?,
                    queued_at_ms: *created_at_ms,
                });
                mutation.queued_prompts = Some(queue);
            }
            RelayCommand::RemoveQueuedPrompt { .. } | RelayCommand::ClearQueuedPrompts => {}
            RelayCommand::Close { .. } => {
                close_streams(current, mutation, event.recorded_at_ms);
                mutation.execution = Some(MaterializedExecutionState::Closing);
            }
            _ => {}
        },
        RelayObservation::CommandStarted {
            command_id,
            started_at_ms,
        } => {
            if let Some(index) = current
                .queued_prompts
                .iter()
                .position(|queued| queued.command_id == *command_id)
            {
                let mut queue = current.queued_prompts.clone();
                let prompt = queue.remove(index);
                mutation.queued_prompts = Some(queue);
                close_streams(current, mutation, event.recorded_at_ms);
                upsert(
                    mutation,
                    TranscriptItem {
                        stable_id: format!("user:{command_id}"),
                        position: event.ordinal,
                        latest_content_event_ordinal: None,
                        created_at_ms: *started_at_ms,
                        last_changed_at_ms: *started_at_ms,
                        body: TranscriptBody::User {
                            content: prompt.content,
                        },
                    },
                );
                mutation.execution = Some(MaterializedExecutionState::Running {
                    started_at_ms: *started_at_ms,
                });
            }
        }
        RelayObservation::CommandCompleted {
            command_id,
            outcome,
        } => {
            let mut queue = current.queued_prompts.clone();
            queue.retain(|queued| queued.command_id != *command_id);
            match outcome {
                crate::hel_worker::RelayCommandOutcome::Prompt { .. } => {
                    close_streams(current, mutation, event.recorded_at_ms);
                    mutation.execution = Some(MaterializedExecutionState::Idle);
                }
                crate::hel_worker::RelayCommandOutcome::Closed => {
                    close_streams(current, mutation, event.recorded_at_ms);
                    mutation.execution = Some(MaterializedExecutionState::Closed);
                }
                crate::hel_worker::RelayCommandOutcome::QueueChanged {
                    removed_command_ids,
                } => queue.retain(|queued| {
                    !removed_command_ids
                        .iter()
                        .any(|command_id| command_id == &queued.command_id)
                }),
                crate::hel_worker::RelayCommandOutcome::Configured
                | crate::hel_worker::RelayCommandOutcome::Cancelled
                | crate::hel_worker::RelayCommandOutcome::CheckpointCompleted => {}
            }
            if queue != current.queued_prompts {
                mutation.queued_prompts = Some(queue);
            }
        }
        RelayObservation::CommandRejected {
            command_id,
            command,
            message,
        }
        | RelayObservation::CommandInterrupted {
            command_id,
            command,
            message,
        } => {
            let prompt_was_started = current
                .transcript
                .iter()
                .any(|item| item.stable_id == format!("user:{command_id}"));
            let mut queue = current.queued_prompts.clone();
            queue.retain(|queued| queued.command_id != *command_id);
            if queue != current.queued_prompts {
                mutation.queued_prompts = Some(queue);
            }
            if prompt_was_started {
                close_streams(current, mutation, event.recorded_at_ms);
                mutation.execution = Some(MaterializedExecutionState::Idle);
            }
            if *command == RelayCommandKind::Close
                && current.execution == MaterializedExecutionState::Closing
            {
                mutation.execution = Some(MaterializedExecutionState::Idle);
            }
            push_system(mutation, event, format!("command {command_id}: {message}"));
        }
        RelayObservation::ConfigurationUpdated { key, value } => {
            let mut configuration = current.configuration.clone();
            configuration.insert(key.clone(), Value::String(value.clone()));
            mutation.configuration = Some(configuration);
        }
        RelayObservation::CheckpointReady { .. } => {}
        RelayObservation::Warning { message } => {
            push_system(mutation, event, format!("warning: {message}"));
        }
        RelayObservation::Closing => {
            close_streams(current, mutation, event.recorded_at_ms);
            mutation.execution = Some(MaterializedExecutionState::Closing);
        }
        RelayObservation::Closed => {
            close_streams(current, mutation, event.recorded_at_ms);
            mutation.execution = Some(MaterializedExecutionState::Closed);
        }
    }
    Ok(())
}

fn project_session_update(
    current: &MaterializedSession,
    event: &RelayEvent,
    update: &SessionUpdate,
    mutation: &mut MaterializedSessionMutation,
) -> Result<()> {
    match update {
        SessionUpdate::AgentMessageChunk(chunk) => {
            close_stream_kind(current, mutation, false, event.recorded_at_ms);
            push_stream_chunk(current, mutation, event, true, chunk)?;
        }
        SessionUpdate::AgentThoughtChunk(chunk) => {
            close_stream_kind(current, mutation, true, event.recorded_at_ms);
            push_stream_chunk(current, mutation, event, false, chunk)?;
        }
        // CommandStarted is the controller's canonical local user message.
        SessionUpdate::UserMessageChunk(_) => {}
        SessionUpdate::ToolCall(call) => {
            close_streams(current, mutation, event.recorded_at_ms);
            upsert(
                mutation,
                TranscriptItem {
                    stable_id: format!("tool:{}", call.tool_call_id),
                    position: event.ordinal,
                    latest_content_event_ordinal: None,
                    created_at_ms: event.recorded_at_ms,
                    last_changed_at_ms: event.recorded_at_ms,
                    body: TranscriptBody::Tool {
                        call: serde_json::to_value(call)?,
                    },
                },
            );
        }
        SessionUpdate::ToolCallUpdate(update) => {
            close_streams(current, mutation, event.recorded_at_ms);
            let stable_id = format!("tool:{}", update.tool_call_id);
            let mut item = current
                .transcript
                .iter()
                .find(|item| item.stable_id == stable_id)
                .cloned()
                .with_context(|| {
                    format!(
                        "ACP updated tool call {} before creating it",
                        update.tool_call_id
                    )
                })?;
            let TranscriptBody::Tool { call } = &mut item.body else {
                bail!(
                    "ACP tool call {} conflicts with transcript item {stable_id}",
                    update.tool_call_id
                );
            };
            let mut materialized_call: ToolCall = serde_json::from_value(call.clone())
                .with_context(|| {
                    format!("parse materialized ACP tool call {}", update.tool_call_id)
                })?;
            materialized_call.update(update.fields.clone());
            *call = serde_json::to_value(materialized_call)?;
            item.last_changed_at_ms = item.last_changed_at_ms.max(event.recorded_at_ms);
            upsert(mutation, item);
        }
        SessionUpdate::Plan(plan) => {
            close_streams(current, mutation, event.recorded_at_ms);
            let plan = serde_json::to_value(plan)?;
            let latest_user_position = current
                .transcript
                .iter()
                .rev()
                .find(|item| matches!(item.body, TranscriptBody::User { .. }))
                .map_or(0, |item| item.position);
            if let Some(mut item) = current
                .transcript
                .iter()
                .rev()
                .find(|item| {
                    item.position > latest_user_position
                        && matches!(item.body, TranscriptBody::Plan { .. })
                })
                .cloned()
            {
                item.body = TranscriptBody::Plan { plan };
                item.last_changed_at_ms = item.last_changed_at_ms.max(event.recorded_at_ms);
                upsert(mutation, item);
            } else {
                upsert(
                    mutation,
                    TranscriptItem {
                        stable_id: format!("plan:{}", event.ordinal),
                        position: event.ordinal,
                        latest_content_event_ordinal: None,
                        created_at_ms: event.recorded_at_ms,
                        last_changed_at_ms: event.recorded_at_ms,
                        body: TranscriptBody::Plan { plan },
                    },
                );
            }
        }
        SessionUpdate::ConfigOptionUpdate(update) => {
            mutation.configuration = Some(configuration_values(&update.config_options));
        }
        SessionUpdate::CurrentModeUpdate(update) => {
            let mut configuration = current.configuration.clone();
            configuration.insert(
                "mode".into(),
                Value::String(update.current_mode_id.to_string()),
            );
            mutation.configuration = Some(configuration);
        }
        SessionUpdate::SessionInfoUpdate(update) => {
            let value = serde_json::to_value(update)?;
            if let Some(title) = value.get("title") {
                mutation.session_title = Some(title.as_str().and_then(normalize_session_title));
            }
        }
        SessionUpdate::AvailableCommandsUpdate(_) | SessionUpdate::UsageUpdate(_) => {}
        _ => {}
    }
    Ok(())
}

fn push_stream_chunk(
    current: &MaterializedSession,
    mutation: &mut MaterializedSessionMutation,
    event: &RelayEvent,
    agent: bool,
    chunk: &agent_client_protocol::schema::v1::ContentChunk,
) -> Result<()> {
    let explicit_id = chunk.message_id.as_ref().map(|id| {
        if agent {
            format!("agent:{id}")
        } else {
            format!("thought:{id}")
        }
    });
    let existing = explicit_id
        .as_ref()
        .and_then(|id| {
            current
                .transcript
                .iter()
                .position(|item| item.stable_id == *id)
        })
        .or_else(|| {
            if explicit_id.is_none() {
                current
                    .transcript
                    .iter()
                    .rposition(|item| match &item.body {
                        TranscriptBody::Agent { streaming, .. } if agent => *streaming,
                        TranscriptBody::Thought { streaming, .. } if !agent => *streaming,
                        _ => false,
                    })
            } else {
                None
            }
        });
    if let Some(index) = existing {
        let mut item = current.transcript[index].clone();
        match &mut item.body {
            TranscriptBody::Agent { chunks, streaming } if agent => {
                chunks.push(serde_json::to_value(chunk)?);
                *streaming = true;
            }
            TranscriptBody::Thought { chunks, streaming } if !agent => {
                chunks.push(serde_json::to_value(chunk)?);
                *streaming = true;
            }
            _ => bail!(
                "ACP message ID conflicts with transcript item {}",
                item.stable_id
            ),
        }
        item.last_changed_at_ms = item.last_changed_at_ms.max(event.recorded_at_ms);
        if agent {
            item.latest_content_event_ordinal = Some(event.ordinal);
        }
        upsert(mutation, item);
        return Ok(());
    }
    upsert(
        mutation,
        TranscriptItem {
            stable_id: explicit_id.unwrap_or_else(|| {
                if agent {
                    format!("agent:{}", event.ordinal)
                } else {
                    format!("thought:{}", event.ordinal)
                }
            }),
            position: event.ordinal,
            latest_content_event_ordinal: agent.then_some(event.ordinal),
            created_at_ms: event.recorded_at_ms,
            last_changed_at_ms: event.recorded_at_ms,
            body: if agent {
                TranscriptBody::Agent {
                    chunks: vec![serde_json::to_value(chunk)?],
                    streaming: true,
                }
            } else {
                TranscriptBody::Thought {
                    chunks: vec![serde_json::to_value(chunk)?],
                    streaming: true,
                }
            },
        },
    );
    Ok(())
}

fn close_stream_kind(
    current: &MaterializedSession,
    mutation: &mut MaterializedSessionMutation,
    agent: bool,
    changed_at_ms: i64,
) {
    for item in &current.transcript {
        let streaming = match &item.body {
            TranscriptBody::Agent { streaming, .. } if agent => *streaming,
            TranscriptBody::Thought { streaming, .. } if !agent => *streaming,
            _ => continue,
        };
        if streaming {
            let mut closed = item.clone();
            match &mut closed.body {
                TranscriptBody::Agent { streaming, .. }
                | TranscriptBody::Thought { streaming, .. } => *streaming = false,
                _ => unreachable!(),
            }
            closed.last_changed_at_ms = closed.last_changed_at_ms.max(changed_at_ms);
            upsert(mutation, closed);
        }
    }
}

fn close_streams(
    current: &MaterializedSession,
    mutation: &mut MaterializedSessionMutation,
    changed_at_ms: i64,
) {
    close_stream_kind(current, mutation, true, changed_at_ms);
    close_stream_kind(current, mutation, false, changed_at_ms);
}

fn push_system(
    mutation: &mut MaterializedSessionMutation,
    event: &RelayEvent,
    text: impl Into<String>,
) {
    upsert(
        mutation,
        TranscriptItem {
            stable_id: format!("system:{}", event.ordinal),
            position: event.ordinal,
            latest_content_event_ordinal: None,
            created_at_ms: event.recorded_at_ms,
            last_changed_at_ms: event.recorded_at_ms,
            body: TranscriptBody::System { text: text.into() },
        },
    );
}

fn upsert(mutation: &mut MaterializedSessionMutation, item: TranscriptItem) {
    if let Some(existing) = mutation.transcript.iter_mut().find(|candidate| {
        matches!(candidate, TranscriptMutation::Upsert(current) if current.stable_id == item.stable_id)
    }) {
        *existing = TranscriptMutation::Upsert(item);
    } else {
        mutation.transcript.push(TranscriptMutation::Upsert(item));
    }
}

fn configuration_values(
    options: &[agent_client_protocol::schema::v1::SessionConfigOption],
) -> BTreeMap<String, Value> {
    options
        .iter()
        .filter_map(|option| {
            let value = serde_json::to_value(option).ok()?;
            let id = value.get("id")?.as_str()?.to_owned();
            let current = value
                .get("currentValue")
                .or_else(|| value.get("current_value"))?
                .clone();
            Some((id, current))
        })
        .collect()
}

pub fn canonical_session_from_materialized(
    materialized: &MaterializedSession,
) -> Result<CanonicalSessionSnapshot> {
    let transcript = materialized
        .transcript
        .iter()
        .map(|item| {
            let body = match &item.body {
                TranscriptBody::User { content } => CanonicalTranscriptBody::User {
                    content: content.clone(),
                },
                TranscriptBody::Agent { chunks, streaming } => CanonicalTranscriptBody::Agent {
                    chunks: chunks.clone(),
                    streaming: *streaming,
                },
                TranscriptBody::Thought { chunks, streaming } => CanonicalTranscriptBody::Thought {
                    chunks: chunks.clone(),
                    streaming: *streaming,
                },
                TranscriptBody::Tool { call } => {
                    CanonicalTranscriptBody::Tool { call: call.clone() }
                }
                TranscriptBody::Plan { plan } => {
                    CanonicalTranscriptBody::Plan { plan: plan.clone() }
                }
                TranscriptBody::System { text } => {
                    CanonicalTranscriptBody::System { text: text.clone() }
                }
            };
            Ok(CanonicalTranscriptItem {
                stable_id: item.stable_id.clone(),
                position: item.position,
                latest_content_event_ordinal: item.latest_content_event_ordinal,
                created_at_ms: item.created_at_ms,
                last_changed_at_ms: item.last_changed_at_ms,
                body,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(CanonicalSessionSnapshot {
        event_frontier: materialized.applied_event_ordinal,
        event_frontier_digest: materialized.applied_event_digest.clone(),
        session: CanonicalSessionState {
            execution: match materialized.execution {
                MaterializedExecutionState::Idle => CanonicalExecutionState::Idle,
                MaterializedExecutionState::Running { started_at_ms } => {
                    CanonicalExecutionState::Running { started_at_ms }
                }
                MaterializedExecutionState::Closing => CanonicalExecutionState::Closing,
                MaterializedExecutionState::Closed => CanonicalExecutionState::Closed,
            },
            last_activity_at_ms: materialized.last_activity_at_ms,
            session_title: materialized.session_title.clone(),
            configuration: materialized.configuration.clone(),
        },
        transcript,
        queued_prompts: materialized
            .queued_prompts
            .iter()
            .map(|prompt| CanonicalQueuedPrompt {
                command_id: prompt.command_id.clone(),
                content: prompt.content.clone(),
                queued_at_ms: prompt.queued_at_ms,
            })
            .collect(),
    })
}

pub fn materialized_session_from_canonical(
    session_id: impl Into<String>,
    canonical: &CanonicalSessionSnapshot,
) -> Result<MaterializedSession> {
    let transcript = canonical
        .transcript
        .iter()
        .map(|item| {
            let body = match &item.body {
                CanonicalTranscriptBody::User { content } => TranscriptBody::User {
                    content: content.clone(),
                },
                CanonicalTranscriptBody::Agent { chunks, streaming } => TranscriptBody::Agent {
                    chunks: chunks.clone(),
                    streaming: *streaming,
                },
                CanonicalTranscriptBody::Thought { chunks, streaming } => TranscriptBody::Thought {
                    chunks: chunks.clone(),
                    streaming: *streaming,
                },
                CanonicalTranscriptBody::Tool { call } => {
                    TranscriptBody::Tool { call: call.clone() }
                }
                CanonicalTranscriptBody::Plan { plan } => {
                    TranscriptBody::Plan { plan: plan.clone() }
                }
                CanonicalTranscriptBody::System { text } => {
                    TranscriptBody::System { text: text.clone() }
                }
            };
            Ok(TranscriptItem {
                stable_id: item.stable_id.clone(),
                position: item.position,
                latest_content_event_ordinal: item.latest_content_event_ordinal,
                created_at_ms: item.created_at_ms,
                last_changed_at_ms: item.last_changed_at_ms,
                body,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(MaterializedSession {
        session_id: session_id.into(),
        applied_event_ordinal: canonical.event_frontier,
        applied_event_digest: canonical.event_frontier_digest.clone(),
        last_activity_at_ms: canonical.session.last_activity_at_ms,
        execution: match canonical.session.execution {
            CanonicalExecutionState::Idle => MaterializedExecutionState::Idle,
            CanonicalExecutionState::Running { started_at_ms } => {
                MaterializedExecutionState::Running { started_at_ms }
            }
            CanonicalExecutionState::Closing => MaterializedExecutionState::Closing,
            CanonicalExecutionState::Closed => MaterializedExecutionState::Closed,
        },
        session_title: canonical.session.session_title.clone(),
        configuration: canonical.session.configuration.clone(),
        transcript,
        queued_prompts: canonical
            .queued_prompts
            .iter()
            .map(|prompt| MaterializedQueuedPrompt {
                command_id: prompt.command_id.clone(),
                content: prompt.content.clone(),
                queued_at_ms: prompt.queued_at_ms,
            })
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use agent_client_protocol::schema::v1::{
        ContentBlock, TextContent, ToolCallUpdate, ToolCallUpdateFields,
    };

    use super::*;
    use crate::hel_worker::{RelayCommandOutcome, RelayObservation, relay_event_digest};
    use serde_json::json;

    fn event(previous: &MaterializedSession, observation: RelayObservation) -> RelayEvent {
        let mut event = RelayEvent {
            ordinal: previous.applied_event_ordinal + 1,
            previous_digest: previous.applied_event_digest.clone(),
            digest: String::new(),
            recorded_at_ms: 0,
            command_id: None,
            observation,
        };
        event.recorded_at_ms = i64::try_from(event.ordinal).unwrap() * 100;
        event.digest = relay_event_digest(&event).unwrap();
        event
    }

    fn apply(session: &mut MaterializedSession, event: RelayEvent) {
        let projected = project_relay_event(session, &event).unwrap();
        apply_committed_projection_event(session, &event, &projected.mutation).unwrap();
    }

    fn apply_observation(session: &mut MaterializedSession, observation: RelayObservation) {
        let next = event(session, observation);
        apply(session, next);
    }

    #[test]
    fn streamed_chunks_are_one_unread_logical_agent_message() {
        let mut session = MaterializedSession::empty("session-1");
        apply_observation(
            &mut session,
            RelayObservation::SessionUpdate {
                update: Box::new(SessionUpdate::AgentMessageChunk(
                    agent_client_protocol::schema::v1::ContentChunk::new(ContentBlock::Text(
                        TextContent::new("hel"),
                    ))
                    .message_id("answer-1"),
                )),
            },
        );
        assert_eq!(session.transcript[0].latest_content_event_ordinal, Some(1));
        assert_eq!(session.unread_agent_messages_after(0), 1);
        assert_eq!(session.unread_agent_messages_after(1), 0);

        apply_observation(
            &mut session,
            RelayObservation::SessionUpdate {
                update: Box::new(SessionUpdate::AgentMessageChunk(
                    agent_client_protocol::schema::v1::ContentChunk::new(ContentBlock::Text(
                        TextContent::new("lo"),
                    ))
                    .message_id("answer-1"),
                )),
            },
        );
        assert_eq!(session.unread_agent_messages_after(0), 1);
        assert_eq!(session.unread_agent_messages_after(1), 1);
        assert!(matches!(
            &session.transcript[0].body,
            TranscriptBody::Agent { chunks, .. }
                if crate::hel_chat::materialized_chunks_text(chunks) == "hello"
        ));
        assert_eq!(session.transcript[0].position, 1);
        assert_eq!(session.transcript[0].latest_content_event_ordinal, Some(2));

        apply_observation(
            &mut session,
            RelayObservation::CommandCompleted {
                command_id: "prompt-1".into(),
                outcome: RelayCommandOutcome::Prompt {
                    stop_reason: "end_turn".into(),
                },
            },
        );
        assert_eq!(session.transcript[0].latest_content_event_ordinal, Some(2));
        assert_eq!(session.unread_agent_messages_after(2), 0);
    }

    #[test]
    fn backward_relay_clock_never_regresses_transcript_change_times() {
        let mut session = MaterializedSession::empty("session-1");
        let mut first = event(
            &session,
            RelayObservation::SessionUpdate {
                update: Box::new(SessionUpdate::AgentMessageChunk(
                    agent_client_protocol::schema::v1::ContentChunk::new(ContentBlock::Text(
                        TextContent::new("first"),
                    ))
                    .message_id("answer-1"),
                )),
            },
        );
        first.recorded_at_ms = 1_000;
        first.digest = relay_event_digest(&first).unwrap();
        apply(&mut session, first);

        let mut backward = event(
            &session,
            RelayObservation::SessionUpdate {
                update: Box::new(SessionUpdate::AgentMessageChunk(
                    agent_client_protocol::schema::v1::ContentChunk::new(ContentBlock::Text(
                        TextContent::new(" second"),
                    ))
                    .message_id("answer-1"),
                )),
            },
        );
        backward.recorded_at_ms = 500;
        backward.digest = relay_event_digest(&backward).unwrap();
        apply(&mut session, backward);
        assert_eq!(session.transcript[0].last_changed_at_ms, 1_000);

        let mut completion = event(
            &session,
            RelayObservation::CommandCompleted {
                command_id: "prompt-1".into(),
                outcome: RelayCommandOutcome::Prompt {
                    stop_reason: "end_turn".into(),
                },
            },
        );
        completion.recorded_at_ms = 250;
        completion.digest = relay_event_digest(&completion).unwrap();
        apply(&mut session, completion);
        assert_eq!(session.transcript[0].last_changed_at_ms, 1_000);
        assert_eq!(session.last_activity_at_ms(), Some(1_000));
    }

    #[test]
    fn tool_update_without_an_initial_call_is_rejected() {
        let session = MaterializedSession::empty("session-1");
        let update = SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(
            "missing-tool",
            ToolCallUpdateFields::new().title("updated"),
        ));

        let error = project_relay_event(
            &session,
            &event(
                &session,
                RelayObservation::SessionUpdate {
                    update: Box::new(update),
                },
            ),
        )
        .unwrap_err();
        assert!(error.to_string().contains("before creating it"));
    }

    #[test]
    fn queued_prompt_becomes_user_message_only_when_started() {
        let mut session = MaterializedSession::empty("session-1");
        apply_observation(
            &mut session,
            RelayObservation::CommandQueued {
                command_id: "prompt-1".into(),
                command: RelayCommand::Prompt {
                    prompt: vec![ContentBlock::Text(TextContent::new("hello"))],
                },
                created_at_ms: 100,
            },
        );
        assert!(session.transcript.is_empty());
        assert_eq!(session.queued_prompts.len(), 1);

        apply_observation(
            &mut session,
            RelayObservation::CommandStarted {
                command_id: "prompt-1".into(),
                started_at_ms: 200,
            },
        );
        assert!(session.queued_prompts.is_empty());
        assert!(matches!(
            session.transcript[0].body,
            TranscriptBody::User { .. }
        ));

        apply_observation(
            &mut session,
            RelayObservation::CommandCompleted {
                command_id: "prompt-1".into(),
                outcome: RelayCommandOutcome::Prompt {
                    stop_reason: "end_turn".into(),
                },
            },
        );
        assert_eq!(session.execution, MaterializedExecutionState::Idle);
    }

    #[test]
    fn queue_changes_project_only_from_their_completion_events() {
        let mut session = MaterializedSession::empty("session-1");
        session.queued_prompts.push(MaterializedQueuedPrompt {
            command_id: "queued-1".into(),
            content: vec![json!({"type": "text", "text": "later"})],
            queued_at_ms: 10,
        });

        apply_observation(
            &mut session,
            RelayObservation::CommandQueued {
                command_id: "remove-1".into(),
                command: RelayCommand::RemoveQueuedPrompt {
                    queued_command_id: "queued-1".into(),
                },
                created_at_ms: 100,
            },
        );
        assert_eq!(session.queued_prompts.len(), 1);

        apply_observation(
            &mut session,
            RelayObservation::CommandCompleted {
                command_id: "remove-1".into(),
                outcome: RelayCommandOutcome::QueueChanged {
                    removed_command_ids: vec!["queued-1".into()],
                },
            },
        );
        assert!(session.queued_prompts.is_empty());

        session.queued_prompts.extend([
            MaterializedQueuedPrompt {
                command_id: "queued-2".into(),
                content: vec![json!({"type": "text", "text": "two"})],
                queued_at_ms: 20,
            },
            MaterializedQueuedPrompt {
                command_id: "queued-3".into(),
                content: vec![json!({"type": "text", "text": "three"})],
                queued_at_ms: 30,
            },
        ]);
        apply_observation(
            &mut session,
            RelayObservation::CommandQueued {
                command_id: "clear-1".into(),
                command: RelayCommand::ClearQueuedPrompts,
                created_at_ms: 200,
            },
        );
        assert_eq!(session.queued_prompts.len(), 2);

        apply_observation(
            &mut session,
            RelayObservation::CommandCompleted {
                command_id: "clear-1".into(),
                outcome: RelayCommandOutcome::QueueChanged {
                    removed_command_ids: vec!["queued-2".into(), "queued-3".into()],
                },
            },
        );
        assert!(session.queued_prompts.is_empty());
    }

    #[test]
    fn rejected_close_rolls_closing_projection_back_to_idle() {
        let mut session = MaterializedSession::empty("session-1");
        apply_observation(
            &mut session,
            RelayObservation::CommandQueued {
                command_id: "close-1".into(),
                command: RelayCommand::Close {
                    barrier_command_id: "barrier-1".into(),
                    expected: crate::hel_worker::RelayCursor {
                        ordinal: 0,
                        digest: "0".repeat(64),
                    },
                },
                created_at_ms: 100,
            },
        );
        assert_eq!(session.execution, MaterializedExecutionState::Closing);

        apply_observation(
            &mut session,
            RelayObservation::CommandRejected {
                command_id: "close-1".into(),
                command: RelayCommandKind::Close,
                message: "ACP close failed".into(),
            },
        );
        assert_eq!(session.execution, MaterializedExecutionState::Idle);
    }

    #[test]
    fn control_command_outcomes_do_not_end_an_active_prompt() {
        let mut session = MaterializedSession::empty("session-1");
        session.applied_event_ordinal = 2;
        session.applied_event_digest = "a".repeat(64);
        session.execution = MaterializedExecutionState::Running { started_at_ms: 100 };
        session.transcript.push(TranscriptItem {
            stable_id: "agent:answer-1".into(),
            position: 2,
            latest_content_event_ordinal: Some(2),
            created_at_ms: 200,
            last_changed_at_ms: 200,
            body: TranscriptBody::Agent {
                chunks: vec![json!({
                    "content": {"type": "text", "text": "working"}
                })],
                streaming: true,
            },
        });

        apply_observation(
            &mut session,
            RelayObservation::CommandCompleted {
                command_id: "config-1".into(),
                outcome: RelayCommandOutcome::Configured,
            },
        );
        assert!(matches!(
            session.execution,
            MaterializedExecutionState::Running { .. }
        ));
        assert!(matches!(
            session.transcript[0].body,
            TranscriptBody::Agent {
                streaming: true,
                ..
            }
        ));

        apply_observation(
            &mut session,
            RelayObservation::CommandRejected {
                command_id: "cancel-1".into(),
                command: RelayCommandKind::Cancel,
                message: "not cancellable".into(),
            },
        );
        assert!(matches!(
            session.execution,
            MaterializedExecutionState::Running { .. }
        ));
        assert!(matches!(
            session.transcript[0].body,
            TranscriptBody::Agent {
                streaming: true,
                ..
            }
        ));
    }

    #[test]
    fn canonical_round_trip_preserves_cursor_and_logical_positions() {
        let mut session = MaterializedSession::empty("session-1");
        session.applied_event_ordinal = 4;
        session.applied_event_digest = "a".repeat(64);
        session.last_activity_at_ms = Some(40);
        session.session_title = Some("Build it".into());
        session.transcript.push(TranscriptItem {
            stable_id: "agent:a".into(),
            position: 2,
            latest_content_event_ordinal: Some(4),
            created_at_ms: 20,
            last_changed_at_ms: 40,
            body: TranscriptBody::Agent {
                chunks: vec![json!({
                    "content": {"type": "text", "text": "done"},
                    "messageId": "a",
                    "_meta": {"provider": "test"}
                })],
                streaming: false,
            },
        });
        session.transcript.push(TranscriptItem {
            stable_id: "thought:t".into(),
            position: 3,
            latest_content_event_ordinal: None,
            created_at_ms: 30,
            last_changed_at_ms: 30,
            body: TranscriptBody::Thought {
                chunks: vec![json!({
                    "content": {
                        "type": "text",
                        "text": "reasoning",
                        "_meta": {"contentProvider": "test"}
                    },
                    "messageId": "t",
                    "_meta": {"chunkProvider": "test"}
                })],
                streaming: false,
            },
        });
        session.transcript.push(TranscriptItem {
            stable_id: "tool:call-1".into(),
            position: 4,
            latest_content_event_ordinal: None,
            created_at_ms: 40,
            last_changed_at_ms: 40,
            body: TranscriptBody::Tool {
                call: json!({
                    "toolCallId": "call-1",
                    "title": "Read file",
                    "kind": "read",
                    "status": "completed",
                    "rawInput": {"path": "README.md"},
                    "rawOutput": {"bytes": 42},
                    "_meta": {"provider": "test"}
                }),
            },
        });
        session.transcript.push(TranscriptItem {
            stable_id: "plan:4".into(),
            position: 4,
            latest_content_event_ordinal: None,
            created_at_ms: 40,
            last_changed_at_ms: 40,
            body: TranscriptBody::Plan {
                plan: json!({
                    "entries": [{
                        "content": "finish",
                        "priority": "high",
                        "status": "in_progress",
                        "_meta": {"entryProvider": "test"}
                    }],
                    "_meta": {"planProvider": "test"}
                }),
            },
        });
        let canonical = canonical_session_from_materialized(&session).unwrap();
        canonical.validate().unwrap();
        let restored = materialized_session_from_canonical("session-1", &canonical).unwrap();
        assert_eq!(restored.applied_event_ordinal, 4);
        assert_eq!(restored.transcript[0].position, 2);
        assert_eq!(restored.unread_agent_messages_after(1), 1);
        assert_eq!(restored, session);
    }

    #[test]
    fn one_chunk_projects_only_the_touched_logical_item() {
        let mut session = MaterializedSession::empty("session-1");
        session.applied_event_ordinal = 10_000;
        session.applied_event_digest = "a".repeat(64);
        session.last_activity_at_ms = Some(10_000);
        session.transcript = (1..=10_000)
            .map(|position| TranscriptItem {
                stable_id: format!("system:{position}"),
                position,
                latest_content_event_ordinal: None,
                created_at_ms: i64::try_from(position).unwrap(),
                last_changed_at_ms: i64::try_from(position).unwrap(),
                body: TranscriptBody::System {
                    text: format!("event {position}"),
                },
            })
            .collect();
        let next = event(
            &session,
            RelayObservation::SessionUpdate {
                update: Box::new(SessionUpdate::AgentMessageChunk(
                    agent_client_protocol::schema::v1::ContentChunk::new(ContentBlock::Text(
                        TextContent::new("answer"),
                    ))
                    .message_id("answer-1"),
                )),
            },
        );

        let projected = project_relay_event(&session, &next).unwrap();

        assert_eq!(projected.mutation.transcript.len(), 1);
        assert!(projected.mutation.configuration.is_none());
        assert!(projected.mutation.queued_prompts.is_none());
        assert_eq!(session.transcript.len(), 10_000);
        apply_committed_projection_event(&mut session, &next, &projected.mutation).unwrap();
        assert_eq!(session.transcript.len(), 10_001);
    }
}
