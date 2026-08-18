//! Controller-owned projection of the durable ACP relay stream.

use std::collections::BTreeMap;
use std::sync::Arc;

use agent_client_protocol::schema::v1::{
    ContentBlock, SessionUpdate, TextContent, ToolCall, ToolCallContent,
};
use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::Value;

use crate::hel_archive::{
    CanonicalExecutionState, CanonicalQueuedCommandKind, CanonicalQueuedPrompt,
    CanonicalSessionSnapshot, CanonicalSessionState, CanonicalTerminalOutput,
    CanonicalTranscriptBody, CanonicalTranscriptItem,
};
use crate::hel_database::{
    MaterializedSessionMutation, ProjectionIntegrityError, TranscriptMutation,
};
use crate::hel_state::{
    MaterializedExecutionState, MaterializedQueuedPrompt, MaterializedSession, QueuedCommandKind,
    TerminalOutputRecord, TranscriptBody, TranscriptItem, config_command_text,
    normalize_session_title,
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
/// mutation and frontier have committed atomically in SQLite. The mutation is
/// consumed so its committed values move into the projection instead of being
/// copied a second time.
pub fn apply_committed_projection_event(
    current: &mut MaterializedSession,
    event: &RelayEvent,
    mutation: MaterializedSessionMutation,
) -> Result<()> {
    validate_relay_event(
        current.applied_event_ordinal,
        &current.applied_event_digest,
        event,
    )?;
    if let Some(execution) = mutation.execution {
        current.execution = execution;
    }
    if let Some(title) = mutation.session_title {
        current.session_title = title;
    }
    if let Some(configuration) = mutation.configuration {
        current.configuration = configuration;
    }
    for item_mutation in mutation.transcript {
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
                        return Err(ProjectionIntegrityError(format!(
                            "transcript item {:?} changed immutable identity fields",
                            item.stable_id
                        ))
                        .into());
                    }
                    if item.last_changed_at_ms < existing.last_changed_at_ms {
                        return Err(ProjectionIntegrityError(format!(
                            "transcript item {:?} moved its changed timestamp backwards",
                            item.stable_id
                        ))
                        .into());
                    }
                    if existing
                        .latest_content_event_ordinal
                        .is_some_and(|existing| {
                            item.latest_content_event_ordinal
                                .is_none_or(|next| next < existing)
                        })
                    {
                        return Err(ProjectionIntegrityError(format!(
                            "transcript item {:?} moved its latest content ordinal backwards",
                            item.stable_id
                        ))
                        .into());
                    }
                    // Reuse the item in place when no published snapshot shares
                    // it; otherwise publish a fresh item so snapshots taken
                    // earlier keep the value they were given.
                    if let Some(owned) = Arc::get_mut(existing) {
                        *owned = item;
                    } else {
                        *existing = Arc::new(item);
                    }
                } else {
                    current.transcript.push(Arc::new(item));
                }
            }
            TranscriptMutation::Remove { stable_id } => {
                current
                    .transcript
                    .retain(|item| item.stable_id != stable_id);
            }
        }
    }
    if let Some(queued_prompts) = mutation.queued_prompts {
        current.queued_prompts = queued_prompts;
    }
    if let Some(pending_elicitations) = mutation.pending_elicitations {
        current.pending_elicitations = pending_elicitations;
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
        RelayObservation::SessionOpened { resumed, .. } => {
            mutation.pending_elicitations = Some(Vec::new());
            push_system(
                mutation,
                event,
                if *resumed {
                    "harness session resumed"
                } else {
                    "harness session started"
                },
            );
        }
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
        RelayObservation::ElicitationRequested { request } => {
            let mut pending = current.pending_elicitations.clone();
            pending.retain(|existing| existing.id != request.id);
            pending.push(request.clone());
            mutation.pending_elicitations = Some(pending);
        }
        RelayObservation::ElicitationResolved { elicitation_id, .. } => {
            let mut pending = current.pending_elicitations.clone();
            pending.retain(|request| request.id != *elicitation_id);
            mutation.pending_elicitations = Some(pending);
        }
        RelayObservation::ElicitationsCleared => {
            mutation.pending_elicitations = Some(Vec::new());
        }
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
                    kind: QueuedCommandKind::Prompt,
                    content: prompt
                        .iter()
                        .map(serde_json::to_value)
                        .collect::<serde_json::Result<_>>()?,
                    queued_at_ms: *created_at_ms,
                });
                mutation.queued_prompts = Some(queue);
            }
            // A configuration change waits in the same queue as prompts and is
            // displayed as the composer text that produced it.
            RelayCommand::SetConfig { key, value } => {
                let mut queue = current.queued_prompts.clone();
                queue.retain(|queued| queued.command_id != *command_id);
                queue.push(MaterializedQueuedPrompt {
                    command_id: command_id.clone(),
                    kind: QueuedCommandKind::SetConfig {
                        key: key.clone(),
                        value: value.clone(),
                    },
                    content: vec![serde_json::to_value(ContentBlock::Text(TextContent::new(
                        config_command_text(key, value),
                    )))?],
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
                let entry = queue.remove(index);
                mutation.queued_prompts = Some(queue);
                // A configuration change applies between turns: it never
                // becomes a transcript turn and never starts the turn clock.
                if entry.kind.is_prompt() {
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
                                content: entry.content,
                            },
                        },
                    );
                    mutation.execution = Some(MaterializedExecutionState::Running {
                        started_at_ms: *started_at_ms,
                    });
                }
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
                | crate::hel_worker::RelayCommandOutcome::SessionModeSet
                | crate::hel_worker::RelayCommandOutcome::Cancelled
                | crate::hel_worker::RelayCommandOutcome::CheckpointCompleted
                | crate::hel_worker::RelayCommandOutcome::CheckpointReleased
                | crate::hel_worker::RelayCommandOutcome::RecoveryFloorAdvanced
                | crate::hel_worker::RelayCommandOutcome::NoticeRecorded => {}
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
        // Terminal output can land before or after the tool call that names the
        // terminal, so both orderings have to end in the same place: attached to
        // every referencing tool item, or parked in a standalone item that the
        // tool call consumes when it arrives.
        RelayObservation::TerminalOutput {
            terminal_id,
            output,
            truncated,
            exit_code,
            signal,
        } => {
            let record = TerminalOutputRecord {
                terminal_id: terminal_id.clone(),
                output: output.clone(),
                truncated: *truncated,
                exit_code: *exit_code,
                signal: signal.clone(),
            };
            let mut attached = false;
            for existing in &current.transcript {
                if !tool_refers_to_terminal(&existing.body, terminal_id) {
                    continue;
                }
                let mut item = TranscriptItem::clone(existing);
                let TranscriptBody::Tool {
                    terminal_outputs, ..
                } = &mut item.body
                else {
                    unreachable!("matched a tool body above");
                };
                replace_or_push_terminal_record(terminal_outputs, record.clone());
                item.last_changed_at_ms = item.last_changed_at_ms.max(event.recorded_at_ms);
                upsert(mutation, item);
                attached = true;
            }
            if !attached {
                let stable_id = terminal_item_id(terminal_id);
                match current
                    .transcript
                    .iter()
                    .find(|item| item.stable_id == stable_id)
                {
                    Some(existing) => {
                        let mut item = TranscriptItem::clone(existing);
                        item.body = TranscriptBody::TerminalOutput { record };
                        item.last_changed_at_ms = item.last_changed_at_ms.max(event.recorded_at_ms);
                        upsert(mutation, item);
                    }
                    None => upsert(
                        mutation,
                        TranscriptItem {
                            stable_id,
                            position: event.ordinal,
                            latest_content_event_ordinal: None,
                            created_at_ms: event.recorded_at_ms,
                            last_changed_at_ms: event.recorded_at_ms,
                            body: TranscriptBody::TerminalOutput { record },
                        },
                    ),
                }
            }
        }
        RelayObservation::Warning { message } => {
            push_system(mutation, event, format!("warning: {message}"));
        }
        // Keyed on the command rather than the event ordinal, and skipped once
        // the line exists: a relay that re-records the same notice after a
        // persistence retry leaves exactly one line in the conversation.
        RelayObservation::Notice { message } => {
            let stable_id = match &event.command_id {
                Some(command_id) => format!("system:notice:{command_id}"),
                None => format!("system:{}", event.ordinal),
            };
            if !current
                .transcript
                .iter()
                .any(|item| item.stable_id == stable_id)
            {
                push_system_with_id(mutation, event, stable_id, message.clone());
            }
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
    let running = matches!(
        mutation.execution.as_ref().unwrap_or(&current.execution),
        MaterializedExecutionState::Running { .. }
    );
    match update {
        SessionUpdate::AgentMessageChunk(chunk) => {
            close_stream_kind(current, mutation, false, event.recorded_at_ms);
            push_stream_chunk(current, mutation, event, true, running, chunk)?;
        }
        SessionUpdate::AgentThoughtChunk(chunk) => {
            close_stream_kind(current, mutation, true, event.recorded_at_ms);
            push_stream_chunk(current, mutation, event, false, running, chunk)?;
        }
        // CommandStarted is the controller's canonical local user message.
        SessionUpdate::UserMessageChunk(_) => {}
        SessionUpdate::ToolCall(call) => {
            close_streams(current, mutation, event.recorded_at_ms);
            let stable_id = format!("tool:{}", call.tool_call_id);
            // Agents re-send a whole `tool_call` for an id they already
            // reported, both when they revise a call and when a resumed
            // session replays its history. Merge into the existing item so the
            // immutable identity fields survive.
            if let Some(mut item) = current
                .transcript
                .iter()
                .find(|item| item.stable_id == stable_id)
                .map(|item| TranscriptItem::clone(item))
            {
                let TranscriptBody::Tool { call: existing, .. } = &mut item.body else {
                    bail!(
                        "ACP tool call {} conflicts with transcript item {stable_id}",
                        call.tool_call_id
                    );
                };
                *existing = serde_json::to_value(call)?;
                item.last_changed_at_ms = item.last_changed_at_ms.max(event.recorded_at_ms);
                attach_terminal_outputs(current, mutation, &mut item);
                upsert(mutation, item);
            } else {
                let mut item = TranscriptItem {
                    stable_id,
                    position: event.ordinal,
                    latest_content_event_ordinal: None,
                    created_at_ms: event.recorded_at_ms,
                    last_changed_at_ms: event.recorded_at_ms,
                    body: TranscriptBody::Tool {
                        call: serde_json::to_value(call)?,
                        terminal_outputs: Vec::new(),
                        terminal_refs: Vec::new(),
                    },
                };
                attach_terminal_outputs(current, mutation, &mut item);
                upsert(mutation, item);
            }
        }
        SessionUpdate::ToolCallUpdate(update) => {
            close_streams(current, mutation, event.recorded_at_ms);
            let stable_id = format!("tool:{}", update.tool_call_id);
            let mut item = current
                .transcript
                .iter()
                .find(|item| item.stable_id == stable_id)
                .map(|item| TranscriptItem::clone(item))
                .with_context(|| {
                    format!(
                        "ACP updated tool call {} before creating it",
                        update.tool_call_id
                    )
                })?;
            let TranscriptBody::Tool { call, .. } = &mut item.body else {
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
            attach_terminal_outputs(current, mutation, &mut item);
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
                .map(|item| TranscriptItem::clone(item))
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
    // ACP permits trailing session updates after a prompt completes or is cancelled (some
    // agents, like Grok Build's goal mode, stream an entire autonomous turn this way, one small
    // delta per chunk, and never set `message_id`). A chunk recorded while the session is not
    // running is complete by definition, so it must not (re)open a stream: checkpoint export
    // requires no open streams at an idle barrier. Instead, a no-message-id chunk recorded while
    // idle is coalesced into the transcript's last item when that item is the same kind (Agent
    // for an agent chunk, Thought for a thought chunk), staying closed (`streaming: false`).
    // This keeps a long run of trailing chunks from Grok Build a single transcript item instead
    // of thousands, while still segmenting the transcript naturally: an intervening item of
    // another kind (a tool call, a plan update, ...) or a thought/agent kind switch makes the
    // transcript's last item mismatch, so the next chunk starts a fresh item.
    running: bool,
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
        let mut item = TranscriptItem::clone(&current.transcript[index]);
        match &mut item.body {
            TranscriptBody::Agent { chunks, streaming } if agent => {
                chunks.push(serde_json::to_value(chunk)?);
                *streaming = running;
            }
            TranscriptBody::Thought { chunks, streaming } if !agent => {
                chunks.push(serde_json::to_value(chunk)?);
                *streaming = running;
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
    // No message ID and no open same-kind stream: while idle, coalesce into the transcript's
    // last item rather than opening a new item per chunk, as long as that last item is the same
    // kind. The item stays closed; see the `running` doc comment above.
    if explicit_id.is_none()
        && !running
        && let Some(last) = current.transcript.last()
    {
        let same_kind = match &last.body {
            TranscriptBody::Agent { .. } => agent,
            TranscriptBody::Thought { .. } => !agent,
            _ => false,
        };
        if same_kind {
            let mut item = TranscriptItem::clone(last);
            match &mut item.body {
                TranscriptBody::Agent { chunks, .. } if agent => {
                    chunks.push(serde_json::to_value(chunk)?);
                }
                TranscriptBody::Thought { chunks, .. } if !agent => {
                    chunks.push(serde_json::to_value(chunk)?);
                }
                _ => unreachable!("same_kind matched the item's body above"),
            }
            item.last_changed_at_ms = item.last_changed_at_ms.max(event.recorded_at_ms);
            if agent {
                item.latest_content_event_ordinal = Some(event.ordinal);
            }
            upsert(mutation, item);
            return Ok(());
        }
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
                    streaming: running,
                }
            } else {
                TranscriptBody::Thought {
                    chunks: vec![serde_json::to_value(chunk)?],
                    streaming: running,
                }
            },
        },
    );
    Ok(())
}

/// Stable id of the standalone item that holds a terminal's output until a
/// tool call refers to it.
fn terminal_item_id(terminal_id: &str) -> String {
    format!("terminal:{terminal_id}")
}

/// The terminal ids one stored ACP tool call refers to. This is the only place
/// that reads terminal content out of a call, so attaching output and
/// consuming a parked item cannot disagree about what a call refers to.
///
/// Content hel cannot read as an ACP block names no terminal; the renderer
/// already reports such a call as invalid, so this hides no failure.
fn tool_call_terminal_ids(call: &Value) -> Vec<String> {
    let Some(Value::Array(content)) = call.get("content") else {
        return Vec::new();
    };
    content
        .iter()
        .filter_map(|value| match ToolCallContent::deserialize(value).ok()? {
            ToolCallContent::Terminal(terminal) => Some(terminal.terminal_id.0.to_string()),
            _ => None,
        })
        .collect()
}

/// Whether one transcript item is a tool call that refers to `terminal_id`,
/// now or at any earlier point in its life. The current call is still consulted
/// because items restored from an archive written before terminal references
/// were remembered carry none.
fn tool_refers_to_terminal(body: &TranscriptBody, terminal_id: &str) -> bool {
    let TranscriptBody::Tool {
        call,
        terminal_refs,
        ..
    } = body
    else {
        return false;
    };
    terminal_refs.iter().any(|id| id == terminal_id)
        || tool_call_terminal_ids(call)
            .iter()
            .any(|id| id == terminal_id)
}

/// The output already recorded for `terminal_id`, wherever it is parked.
fn find_terminal_record(
    current: &MaterializedSession,
    terminal_id: &str,
) -> Option<TerminalOutputRecord> {
    current.transcript.iter().find_map(|item| match &item.body {
        TranscriptBody::TerminalOutput { record } if record.terminal_id == terminal_id => {
            Some(record.clone())
        }
        _ => None,
    })
}

fn replace_or_push_terminal_record(
    records: &mut Vec<TerminalOutputRecord>,
    record: TerminalOutputRecord,
) {
    match records
        .iter_mut()
        .find(|existing| existing.terminal_id == record.terminal_id)
    {
        Some(existing) => *existing = record,
        None => records.push(record),
    }
}

/// Remember every terminal the current call refers to, move any parked output
/// for the terminals `item` has ever referred to into the item, and remove the
/// standalone items it consumed. Output that arrives before the tool call
/// therefore ends up exactly where output that arrives after it does, and a
/// call that drops its terminal reference on a later content update still owns
/// the terminal it started.
fn attach_terminal_outputs(
    current: &MaterializedSession,
    mutation: &mut MaterializedSessionMutation,
    item: &mut TranscriptItem,
) {
    let TranscriptBody::Tool {
        call,
        terminal_outputs,
        terminal_refs,
    } = &mut item.body
    else {
        return;
    };
    for terminal_id in tool_call_terminal_ids(call) {
        if !terminal_refs.contains(&terminal_id) {
            terminal_refs.push(terminal_id);
        }
    }
    let mut consumed = Vec::new();
    for terminal_id in terminal_refs.iter() {
        let Some(record) = find_terminal_record(current, terminal_id) else {
            continue;
        };
        replace_or_push_terminal_record(terminal_outputs, record);
        consumed.push(terminal_item_id(terminal_id));
    }
    for stable_id in consumed {
        mutation
            .transcript
            .push(TranscriptMutation::Remove { stable_id });
    }
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
            let mut closed = TranscriptItem::clone(item);
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
    push_system_with_id(mutation, event, format!("system:{}", event.ordinal), text);
}

fn push_system_with_id(
    mutation: &mut MaterializedSessionMutation,
    event: &RelayEvent,
    stable_id: String,
    text: impl Into<String>,
) {
    upsert(
        mutation,
        TranscriptItem {
            stable_id,
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

fn canonical_terminal_output(record: &TerminalOutputRecord) -> CanonicalTerminalOutput {
    CanonicalTerminalOutput {
        terminal_id: record.terminal_id.clone(),
        output: record.output.clone(),
        truncated: record.truncated,
        exit_code: record.exit_code,
        signal: record.signal.clone(),
    }
}

fn materialized_terminal_output(record: &CanonicalTerminalOutput) -> TerminalOutputRecord {
    TerminalOutputRecord {
        terminal_id: record.terminal_id.clone(),
        output: record.output.clone(),
        truncated: record.truncated,
        exit_code: record.exit_code,
        signal: record.signal.clone(),
    }
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
                TranscriptBody::Tool {
                    call,
                    terminal_outputs,
                    terminal_refs,
                } => CanonicalTranscriptBody::Tool {
                    call: call.clone(),
                    terminal_outputs: terminal_outputs
                        .iter()
                        .map(canonical_terminal_output)
                        .collect(),
                    terminal_refs: terminal_refs.clone(),
                },
                TranscriptBody::TerminalOutput { record } => {
                    CanonicalTranscriptBody::TerminalOutput {
                        record: canonical_terminal_output(record),
                    }
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
                kind: match &prompt.kind {
                    QueuedCommandKind::Prompt => CanonicalQueuedCommandKind::Prompt,
                    QueuedCommandKind::SetConfig { key, value } => {
                        CanonicalQueuedCommandKind::SetConfig {
                            key: key.clone(),
                            value: value.clone(),
                        }
                    }
                },
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
                CanonicalTranscriptBody::Tool {
                    call,
                    terminal_outputs,
                    terminal_refs,
                } => TranscriptBody::Tool {
                    call: call.clone(),
                    terminal_outputs: terminal_outputs
                        .iter()
                        .map(materialized_terminal_output)
                        .collect(),
                    terminal_refs: terminal_refs.clone(),
                },
                CanonicalTranscriptBody::TerminalOutput { record } => {
                    TranscriptBody::TerminalOutput {
                        record: materialized_terminal_output(record),
                    }
                }
                CanonicalTranscriptBody::Plan { plan } => {
                    TranscriptBody::Plan { plan: plan.clone() }
                }
                CanonicalTranscriptBody::System { text } => {
                    TranscriptBody::System { text: text.clone() }
                }
            };
            Ok(Arc::new(TranscriptItem {
                stable_id: item.stable_id.clone(),
                position: item.position,
                latest_content_event_ordinal: item.latest_content_event_ordinal,
                created_at_ms: item.created_at_ms,
                last_changed_at_ms: item.last_changed_at_ms,
                body,
            }))
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
        queued_prompts: materialized_queued_prompts_from_canonical(&canonical.queued_prompts),
        pending_elicitations: Vec::new(),
    })
}

/// Project archived queued commands onto the durable queue shape. Kept apart
/// from the whole-projection build so a caller that only has to restore the
/// queue does not have to rebuild the transcript with it.
pub fn materialized_queued_prompts_from_canonical(
    queued_prompts: &[CanonicalQueuedPrompt],
) -> Vec<MaterializedQueuedPrompt> {
    queued_prompts
        .iter()
        .map(|prompt| MaterializedQueuedPrompt {
            command_id: prompt.command_id.clone(),
            kind: match &prompt.kind {
                CanonicalQueuedCommandKind::Prompt => QueuedCommandKind::Prompt,
                CanonicalQueuedCommandKind::SetConfig { key, value } => {
                    QueuedCommandKind::SetConfig {
                        key: key.clone(),
                        value: value.clone(),
                    }
                }
            },
            content: prompt.content.clone(),
            queued_at_ms: prompt.queued_at_ms,
        })
        .collect()
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
        apply_committed_projection_event(session, &event, projected.mutation).unwrap();
    }

    fn apply_observation(session: &mut MaterializedSession, observation: RelayObservation) {
        let next = event(session, observation);
        apply(session, next);
    }

    #[test]
    fn elicitation_projection_keeps_only_pending_request_metadata() {
        let mut session = MaterializedSession::empty("session-1");
        let request = crate::hel_elicitation::ElicitationRequest {
            id: "elicitation-1".into(),
            message: "Choose one".into(),
            title: None,
            description: None,
            fields: Vec::new(),
        };
        apply_observation(
            &mut session,
            RelayObservation::ElicitationRequested {
                request: request.clone(),
            },
        );
        assert_eq!(session.pending_elicitations, vec![request]);

        apply_observation(
            &mut session,
            RelayObservation::ElicitationResolved {
                elicitation_id: "elicitation-1".into(),
                action: "accept".into(),
            },
        );
        assert!(session.pending_elicitations.is_empty());
    }

    /// An agent message chunk with no `message_id`, as Grok Build's goal mode streams them.
    fn untagged_agent_chunk(text: &str) -> RelayObservation {
        RelayObservation::SessionUpdate {
            update: Box::new(SessionUpdate::AgentMessageChunk(
                agent_client_protocol::schema::v1::ContentChunk::new(ContentBlock::Text(
                    TextContent::new(text),
                )),
            )),
        }
    }

    /// An agent thought chunk with no `message_id`, mirroring [`untagged_agent_chunk`].
    fn untagged_thought_chunk(text: &str) -> RelayObservation {
        RelayObservation::SessionUpdate {
            update: Box::new(SessionUpdate::AgentThoughtChunk(
                agent_client_protocol::schema::v1::ContentChunk::new(ContentBlock::Text(
                    TextContent::new(text),
                )),
            )),
        }
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
    fn agent_chunk_while_idle_is_recorded_closed() {
        let mut session = MaterializedSession::empty("session-1");
        session.execution = MaterializedExecutionState::Idle;
        apply_observation(
            &mut session,
            RelayObservation::SessionUpdate {
                update: Box::new(SessionUpdate::AgentMessageChunk(
                    agent_client_protocol::schema::v1::ContentChunk::new(ContentBlock::Text(
                        TextContent::new("trailing"),
                    ))
                    .message_id("msg-1"),
                )),
            },
        );
        let item = session
            .transcript
            .iter()
            .find(|item| item.stable_id == "agent:msg-1")
            .expect("trailing chunk recorded");
        assert!(matches!(
            &item.body,
            TranscriptBody::Agent { chunks, streaming }
                if !*streaming
                    && crate::hel_chat::materialized_chunks_text(chunks) == "trailing"
        ));
    }

    #[test]
    fn thought_chunk_while_idle_is_recorded_closed() {
        let mut session = MaterializedSession::empty("session-1");
        session.execution = MaterializedExecutionState::Idle;
        apply_observation(
            &mut session,
            RelayObservation::SessionUpdate {
                update: Box::new(SessionUpdate::AgentThoughtChunk(
                    agent_client_protocol::schema::v1::ContentChunk::new(ContentBlock::Text(
                        TextContent::new("late thought"),
                    ))
                    .message_id("msg-1"),
                )),
            },
        );
        let item = session
            .transcript
            .iter()
            .find(|item| item.stable_id == "thought:msg-1")
            .expect("trailing thought recorded");
        assert!(matches!(
            &item.body,
            TranscriptBody::Thought { streaming, .. } if !*streaming
        ));
    }

    #[test]
    fn agent_chunk_while_running_still_streams() {
        let mut session = MaterializedSession::empty("session-1");
        session.execution = MaterializedExecutionState::Running { started_at_ms: 1 };
        apply_observation(
            &mut session,
            RelayObservation::SessionUpdate {
                update: Box::new(SessionUpdate::AgentMessageChunk(
                    agent_client_protocol::schema::v1::ContentChunk::new(ContentBlock::Text(
                        TextContent::new("live"),
                    ))
                    .message_id("msg-1"),
                )),
            },
        );
        let item = session
            .transcript
            .iter()
            .find(|item| item.stable_id == "agent:msg-1")
            .expect("live chunk recorded");
        assert!(matches!(
            &item.body,
            TranscriptBody::Agent { chunks, streaming }
                if *streaming && crate::hel_chat::materialized_chunks_text(chunks) == "live"
        ));
    }

    #[test]
    fn idle_untagged_agent_chunks_coalesce_into_one_closed_item() {
        let mut session = MaterializedSession::empty("session-1");
        session.execution = MaterializedExecutionState::Idle;
        for word in ["Grok ", "streams ", "one ", "word ", "at ", "a ", "time"] {
            apply_observation(&mut session, untagged_agent_chunk(word));
        }
        assert_eq!(session.transcript.len(), 1);
        let item = &session.transcript[0];
        assert!(matches!(
            &item.body,
            TranscriptBody::Agent { chunks, streaming }
                if !*streaming
                    && crate::hel_chat::materialized_chunks_text(chunks)
                        == "Grok streams one word at a time"
        ));
    }

    #[test]
    fn idle_untagged_thought_chunks_coalesce_into_one_closed_item() {
        let mut session = MaterializedSession::empty("session-1");
        session.execution = MaterializedExecutionState::Idle;
        for word in ["thinking ", "in ", "small ", "pieces"] {
            apply_observation(&mut session, untagged_thought_chunk(word));
        }
        assert_eq!(session.transcript.len(), 1);
        let item = &session.transcript[0];
        assert!(matches!(
            &item.body,
            TranscriptBody::Thought { chunks, streaming }
                if !*streaming
                    && crate::hel_chat::materialized_chunks_text(chunks)
                        == "thinking in small pieces"
        ));
    }

    #[test]
    fn idle_untagged_thought_then_agent_chunks_split_into_two_items() {
        let mut session = MaterializedSession::empty("session-1");
        session.execution = MaterializedExecutionState::Idle;
        apply_observation(&mut session, untagged_thought_chunk("pondering "));
        apply_observation(&mut session, untagged_thought_chunk("the goal"));
        apply_observation(&mut session, untagged_agent_chunk("here's "));
        apply_observation(&mut session, untagged_agent_chunk("the plan"));

        assert_eq!(session.transcript.len(), 2);
        assert!(matches!(
            &session.transcript[0].body,
            TranscriptBody::Thought { chunks, streaming }
                if !*streaming
                    && crate::hel_chat::materialized_chunks_text(chunks) == "pondering the goal"
        ));
        assert!(matches!(
            &session.transcript[1].body,
            TranscriptBody::Agent { chunks, streaming }
                if !*streaming
                    && crate::hel_chat::materialized_chunks_text(chunks) == "here's the plan"
        ));
    }

    #[test]
    fn idle_untagged_agent_chunks_split_around_an_intervening_tool_call() {
        let mut session = MaterializedSession::empty("session-1");
        session.execution = MaterializedExecutionState::Idle;
        apply_observation(&mut session, untagged_agent_chunk("checking "));
        apply_observation(&mut session, untagged_agent_chunk("the repo"));
        apply_observation(
            &mut session,
            RelayObservation::SessionUpdate {
                update: Box::new(SessionUpdate::ToolCall(ToolCall::new("call-1", "grep"))),
            },
        );
        apply_observation(&mut session, untagged_agent_chunk("found "));
        apply_observation(&mut session, untagged_agent_chunk("it"));

        let agent_items: Vec<&TranscriptItem> = session
            .transcript
            .iter()
            .filter(|item| matches!(item.body, TranscriptBody::Agent { .. }))
            .map(|item| item.as_ref())
            .collect();
        assert_eq!(agent_items.len(), 2, "transcript: {:?}", session.transcript);
        assert!(matches!(
            &agent_items[0].body,
            TranscriptBody::Agent { chunks, streaming }
                if !*streaming
                    && crate::hel_chat::materialized_chunks_text(chunks) == "checking the repo"
        ));
        assert!(matches!(
            &agent_items[1].body,
            TranscriptBody::Agent { chunks, streaming }
                if !*streaming
                    && crate::hel_chat::materialized_chunks_text(chunks) == "found it"
        ));
        assert!(
            session
                .transcript
                .iter()
                .any(|item| matches!(&item.body, TranscriptBody::Tool { .. })),
            "the tool call item survives between the two agent items"
        );
    }

    #[test]
    fn running_untagged_agent_chunks_still_merge_into_one_open_stream() {
        let mut session = MaterializedSession::empty("session-1");
        session.execution = MaterializedExecutionState::Running { started_at_ms: 1 };
        for word in ["live ", "streaming ", "text"] {
            apply_observation(&mut session, untagged_agent_chunk(word));
        }
        assert_eq!(session.transcript.len(), 1);
        let item = &session.transcript[0];
        assert!(matches!(
            &item.body,
            TranscriptBody::Agent { chunks, streaming }
                if *streaming
                    && crate::hel_chat::materialized_chunks_text(chunks) == "live streaming text"
        ));
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
    fn resent_tool_call_keeps_identity_and_replaces_the_call_payload() {
        let mut session = MaterializedSession::empty("session-1");
        apply_observation(
            &mut session,
            RelayObservation::SessionUpdate {
                update: Box::new(SessionUpdate::ToolCall(ToolCall::new(
                    "call-1",
                    "read file",
                ))),
            },
        );
        let created = TranscriptItem::clone(&session.transcript[0]);
        assert_eq!(created.position, 1);
        assert_eq!(created.created_at_ms, 100);

        let resend = event(
            &session,
            RelayObservation::SessionUpdate {
                update: Box::new(SessionUpdate::ToolCall(
                    ToolCall::new("call-1", "read file again")
                        .status(agent_client_protocol::schema::v1::ToolCallStatus::Completed),
                )),
            },
        );
        let projected = project_relay_event(&session, &resend).unwrap();
        let TranscriptMutation::Upsert(item) = projected
            .mutation
            .transcript
            .iter()
            .find(|mutation| {
                matches!(mutation, TranscriptMutation::Upsert(item) if item.stable_id == "tool:call-1")
            })
            .expect("the re-sent tool call upserts its existing item")
            .clone()
        else {
            unreachable!("matched an upsert above");
        };
        assert_eq!(item.position, created.position);
        assert_eq!(item.created_at_ms, created.created_at_ms);
        assert_eq!(item.last_changed_at_ms, resend.recorded_at_ms);
        assert_eq!(
            item.latest_content_event_ordinal,
            created.latest_content_event_ordinal
        );
        let TranscriptBody::Tool { call, .. } = &item.body else {
            panic!("re-sent tool call stayed a tool item");
        };
        assert_eq!(call["title"], json!("read file again"));

        apply_committed_projection_event(&mut session, &resend, projected.mutation)
            .expect("the merged item passes the projection integrity checks");
        assert_eq!(session.transcript.len(), 1);
        assert_eq!(session.transcript[0].position, created.position);
    }

    #[test]
    fn tool_call_update_then_resent_tool_call_survives_the_projection() {
        let mut session = MaterializedSession::empty("session-1");
        apply_observation(
            &mut session,
            RelayObservation::SessionUpdate {
                update: Box::new(SessionUpdate::ToolCall(ToolCall::new("call-1", "shell"))),
            },
        );
        apply_observation(
            &mut session,
            RelayObservation::SessionUpdate {
                update: Box::new(SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(
                    "call-1",
                    ToolCallUpdateFields::new()
                        .status(agent_client_protocol::schema::v1::ToolCallStatus::Completed),
                ))),
            },
        );
        apply_observation(
            &mut session,
            RelayObservation::SessionUpdate {
                update: Box::new(SessionUpdate::ToolCall(ToolCall::new(
                    "call-1",
                    "shell (retried)",
                ))),
            },
        );

        assert_eq!(session.transcript.len(), 1);
        let item = &session.transcript[0];
        assert_eq!(item.position, 1);
        assert_eq!(item.created_at_ms, 100);
        assert_eq!(item.last_changed_at_ms, 300);
        let TranscriptBody::Tool { call, .. } = &item.body else {
            panic!("the item stayed a tool item");
        };
        assert_eq!(call["title"], json!("shell (retried)"));
    }

    /// A tool call whose only content is a terminal reference, the shape
    /// kimi-code sends for every Bash call.
    fn terminal_tool_call(call_id: &'static str, terminal_id: &'static str) -> RelayObservation {
        RelayObservation::SessionUpdate {
            update: Box::new(SessionUpdate::ToolCall(
                ToolCall::new(call_id, "shell").content(vec![ToolCallContent::Terminal(
                    agent_client_protocol::schema::v1::Terminal::new(terminal_id),
                )]),
            )),
        }
    }

    fn terminal_output(terminal_id: &str) -> RelayObservation {
        RelayObservation::TerminalOutput {
            terminal_id: terminal_id.into(),
            output: "build finished\n".into(),
            truncated: false,
            exit_code: Some(0),
            signal: None,
        }
    }

    fn attached_terminal_outputs(item: &TranscriptItem) -> &[TerminalOutputRecord] {
        let TranscriptBody::Tool {
            terminal_outputs, ..
        } = &item.body
        else {
            panic!("expected a tool item, got {:?}", item.body);
        };
        terminal_outputs
    }

    #[test]
    fn terminal_output_after_the_tool_call_attaches_to_the_tool_item() {
        let mut session = MaterializedSession::empty("session-1");
        apply_observation(&mut session, terminal_tool_call("call-1", "term-1"));
        apply_observation(&mut session, terminal_output("term-1"));

        assert_eq!(session.transcript.len(), 1, "no standalone item is left");
        let outputs = attached_terminal_outputs(&session.transcript[0]);
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].terminal_id, "term-1");
        assert_eq!(outputs[0].output, "build finished\n");
        assert_eq!(outputs[0].exit_code, Some(0));
        assert_eq!(session.transcript[0].last_changed_at_ms, 200);
    }

    #[test]
    fn terminal_output_before_the_tool_call_attaches_when_the_call_arrives() {
        let mut session = MaterializedSession::empty("session-1");
        apply_observation(&mut session, terminal_output("term-1"));

        // Output nobody refers to yet is parked in its own item rather than
        // dropped, so a terminal a call never names still reaches the reader.
        assert_eq!(session.transcript.len(), 1);
        assert_eq!(session.transcript[0].stable_id, "terminal:term-1");
        assert!(matches!(
            &session.transcript[0].body,
            TranscriptBody::TerminalOutput { record } if record.terminal_id == "term-1"
        ));

        apply_observation(&mut session, terminal_tool_call("call-1", "term-1"));

        assert_eq!(
            session.transcript.len(),
            1,
            "the tool call consumes the parked item: {:?}",
            session.transcript
        );
        assert_eq!(session.transcript[0].stable_id, "tool:call-1");
        let outputs = attached_terminal_outputs(&session.transcript[0]);
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].output, "build finished\n");

        // Both orderings converge on the same tool body.
        let mut reversed = MaterializedSession::empty("session-1");
        apply_observation(&mut reversed, terminal_tool_call("call-1", "term-1"));
        apply_observation(&mut reversed, terminal_output("term-1"));
        assert_eq!(
            attached_terminal_outputs(&reversed.transcript[0]),
            outputs,
            "output arriving before or after the call must read the same"
        );
    }

    #[test]
    fn wholesale_tool_call_update_keeps_the_attached_terminal_output() {
        let mut session = MaterializedSession::empty("session-1");
        apply_observation(&mut session, terminal_tool_call("call-1", "term-1"));
        apply_observation(&mut session, terminal_output("term-1"));
        // `ToolCall::update` replaces `content` wholesale, which is why the
        // output lives beside the call rather than inside it.
        apply_observation(
            &mut session,
            RelayObservation::SessionUpdate {
                update: Box::new(SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(
                    "call-1",
                    ToolCallUpdateFields::new()
                        .status(agent_client_protocol::schema::v1::ToolCallStatus::Completed)
                        .content(vec![ToolCallContent::Terminal(
                            agent_client_protocol::schema::v1::Terminal::new("term-1"),
                        )]),
                ))),
            },
        );

        assert_eq!(session.transcript.len(), 1);
        let outputs = attached_terminal_outputs(&session.transcript[0]);
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].output, "build finished\n");
        let TranscriptBody::Tool { call, .. } = &session.transcript[0].body else {
            panic!("the item stayed a tool item");
        };
        assert_eq!(call["status"], json!("completed"));
    }

    /// Grok Build names the terminal on a mid-flight update and then replaces
    /// `content` wholesale with plain text before the terminal is reaped, so
    /// the close event arrives with nothing in the call pointing at it.
    #[test]
    fn a_tool_call_that_dropped_its_terminal_reference_still_attaches_the_output() {
        let mut session = MaterializedSession::empty("session-1");
        apply_observation(&mut session, terminal_tool_call("call-1", "term-1"));
        apply_observation(
            &mut session,
            RelayObservation::SessionUpdate {
                update: Box::new(SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(
                    "call-1",
                    ToolCallUpdateFields::new()
                        .status(agent_client_protocol::schema::v1::ToolCallStatus::Completed)
                        .content(vec![ToolCallContent::from(ContentBlock::Text(
                            TextContent::new("ran the build"),
                        ))]),
                ))),
            },
        );
        apply_observation(&mut session, terminal_output("term-1"));

        assert_eq!(
            session.transcript.len(),
            1,
            "the output attaches instead of parking in its own item: {:?}",
            session.transcript
        );
        assert_eq!(session.transcript[0].stable_id, "tool:call-1");
        let outputs = attached_terminal_outputs(&session.transcript[0]);
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].output, "build finished\n");
        let TranscriptBody::Tool {
            call,
            terminal_refs,
            ..
        } = &session.transcript[0].body
        else {
            panic!("the item stayed a tool item");
        };
        assert_eq!(terminal_refs, &["term-1".to_owned()]);
        assert_eq!(
            tool_call_terminal_ids(call),
            Vec::<String>::new(),
            "the final call really did drop the reference"
        );
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
    fn queued_config_change_starts_without_becoming_a_turn() {
        let mut session = MaterializedSession::empty("session-1");
        apply_observation(
            &mut session,
            RelayObservation::CommandQueued {
                command_id: "config-1".into(),
                command: RelayCommand::SetConfig {
                    key: "model".into(),
                    value: "sonnet".into(),
                },
                created_at_ms: 100,
            },
        );
        assert_eq!(session.queued_prompts.len(), 1);
        assert_eq!(
            session.queued_prompts[0].kind,
            QueuedCommandKind::SetConfig {
                key: "model".into(),
                value: "sonnet".into(),
            }
        );
        assert_eq!(
            crate::hel_chat::materialized_content_text(&session.queued_prompts[0].content),
            "/model sonnet"
        );

        apply_observation(
            &mut session,
            RelayObservation::CommandStarted {
                command_id: "config-1".into(),
                started_at_ms: 200,
            },
        );
        assert!(session.queued_prompts.is_empty());
        assert!(session.transcript.is_empty());
        assert_eq!(session.execution, MaterializedExecutionState::Idle);

        apply_observation(
            &mut session,
            RelayObservation::CommandCompleted {
                command_id: "config-1".into(),
                outcome: RelayCommandOutcome::Configured,
            },
        );
        assert_eq!(session.execution, MaterializedExecutionState::Idle);
        assert!(session.transcript.is_empty());
    }

    #[test]
    fn queue_changes_project_only_from_their_completion_events() {
        let mut session = MaterializedSession::empty("session-1");
        session.queued_prompts.push(MaterializedQueuedPrompt {
            command_id: "queued-1".into(),
            kind: QueuedCommandKind::Prompt,
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
                kind: QueuedCommandKind::Prompt,
                content: vec![json!({"type": "text", "text": "two"})],
                queued_at_ms: 20,
            },
            MaterializedQueuedPrompt {
                command_id: "queued-3".into(),
                kind: QueuedCommandKind::Prompt,
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
        session.transcript.push(Arc::new(TranscriptItem {
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
        }));

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
        session.transcript.push(Arc::new(TranscriptItem {
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
        }));
        session.transcript.push(Arc::new(TranscriptItem {
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
        }));
        session.transcript.push(Arc::new(TranscriptItem {
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
                    "content": [{"type": "terminal", "terminalId": "term-1"}],
                    "rawInput": {"path": "README.md"},
                    "rawOutput": {"bytes": 42},
                    "_meta": {"provider": "test"}
                }),
                terminal_outputs: vec![TerminalOutputRecord {
                    terminal_id: "term-1".into(),
                    output: "ok\n".into(),
                    truncated: true,
                    exit_code: Some(0),
                    signal: None,
                }],
                // "term-3" is a reference the call no longer carries, so only
                // the remembered list can survive the archive round trip.
                terminal_refs: vec!["term-1".into(), "term-3".into()],
            },
        }));
        session.transcript.push(Arc::new(TranscriptItem {
            stable_id: "terminal:term-2".into(),
            position: 4,
            latest_content_event_ordinal: None,
            created_at_ms: 40,
            last_changed_at_ms: 40,
            body: TranscriptBody::TerminalOutput {
                record: TerminalOutputRecord {
                    terminal_id: "term-2".into(),
                    output: "orphaned output\n".into(),
                    truncated: false,
                    exit_code: None,
                    signal: Some("SIGKILL".into()),
                },
            },
        }));
        session.transcript.push(Arc::new(TranscriptItem {
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
        }));
        session.queued_prompts.push(MaterializedQueuedPrompt {
            command_id: "queued-config".into(),
            kind: QueuedCommandKind::SetConfig {
                key: "model".into(),
                value: "sonnet".into(),
            },
            content: vec![json!({"type": "text", "text": "/model sonnet"})],
            queued_at_ms: 50,
        });
        let canonical = canonical_session_from_materialized(&session).unwrap();
        canonical.validate().unwrap();
        assert_eq!(
            canonical.queued_prompts[0].kind,
            CanonicalQueuedCommandKind::SetConfig {
                key: "model".into(),
                value: "sonnet".into(),
            }
        );
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
            .map(|position| {
                Arc::new(TranscriptItem {
                    stable_id: format!("system:{position}"),
                    position,
                    latest_content_event_ordinal: None,
                    created_at_ms: i64::try_from(position).unwrap(),
                    last_changed_at_ms: i64::try_from(position).unwrap(),
                    body: TranscriptBody::System {
                        text: format!("event {position}"),
                    },
                })
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
        apply_committed_projection_event(&mut session, &next, projected.mutation).unwrap();
        assert_eq!(session.transcript.len(), 10_001);
    }

    fn agent_chunk(text: &str, message_id: &str) -> RelayObservation {
        RelayObservation::SessionUpdate {
            update: Box::new(SessionUpdate::AgentMessageChunk(
                agent_client_protocol::schema::v1::ContentChunk::new(ContentBlock::Text(
                    TextContent::new(text),
                ))
                .message_id(message_id),
            )),
        }
    }

    fn end_turn() -> RelayObservation {
        RelayObservation::CommandCompleted {
            command_id: "prompt-1".into(),
            outcome: RelayCommandOutcome::Prompt {
                stop_reason: "end_turn".into(),
            },
        }
    }

    fn agent_text(item: &TranscriptItem) -> String {
        let TranscriptBody::Agent { chunks, .. } = &item.body else {
            panic!("expected an agent message, got {:?}", item.body);
        };
        crate::hel_chat::materialized_chunks_text(chunks)
    }

    #[test]
    fn appending_a_transcript_item_leaves_earlier_items_shared_with_older_snapshots() {
        let mut session = MaterializedSession::empty("session-1");
        apply_observation(&mut session, agent_chunk("answer", "answer-1"));
        apply_observation(&mut session, end_turn());
        let published = session.clone();

        apply_observation(
            &mut session,
            RelayObservation::Warning {
                message: "disk is nearly full".into(),
            },
        );

        assert_eq!(published.transcript.len(), 1);
        assert_eq!(session.transcript.len(), 2);
        assert!(
            Arc::ptr_eq(&session.transcript[0], &published.transcript[0]),
            "cloning a session must share earlier transcript items, not copy them"
        );
    }

    #[test]
    fn appending_a_chunk_replaces_only_the_streaming_tail_item() {
        let mut session = MaterializedSession::empty("session-1");
        apply_observation(&mut session, agent_chunk("finished", "answer-1"));
        apply_observation(&mut session, end_turn());
        apply_observation(&mut session, agent_chunk("hel", "answer-2"));
        let published = session.clone();

        apply_observation(&mut session, agent_chunk("lo", "answer-2"));

        assert_eq!(session.transcript.len(), 2);
        assert!(
            Arc::ptr_eq(&session.transcript[0], &published.transcript[0]),
            "finalized items stay shared while the tail streams"
        );
        assert!(
            !Arc::ptr_eq(&session.transcript[1], &published.transcript[1]),
            "the streaming tail must be replaced, not mutated in place"
        );
        assert_eq!(agent_text(&published.transcript[1]), "hel");
        assert_eq!(agent_text(&session.transcript[1]), "hello");
    }
}
