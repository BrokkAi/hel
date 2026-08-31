# Add durable mid-turn steering to Hel

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

This document must be maintained in accordance with `.agents/PLANS.md` from the repository root. A future contributor must be able to resume the work using only that file, this ExecPlan, and the current working tree.

## Purpose / Big Picture

Hel currently lets a user submit more instructions while an agent is working, but those instructions wait in a durable first-in, first-out queue until the active turn finishes. A user can cancel the active turn, after which the queued correction starts as a new turn, but cannot redirect a supported agent without discarding the current turn's progress.

After this change, a user can queue a correction during a running turn and press the existing turn-interrupt control. If the connected ACP agent advertises mid-turn steering, Hel removes the oldest eligible queued correction only after the agent confirms that it was injected into the running turn. The original turn remains active and continues under the corrected instruction. If the agent does not support steering, the control keeps Hel's present behavior: cancel the active turn and leave the correction queued to run next. The terminal and web viewer expose the same queue-first behavior.

The visible demonstration is a paused fake agent turn. Submit `first`, queue `change direction`, and activate the turn control. With steering advertised, the fake ACP log must contain one `_session/steering` request, the transcript must show `change direction` inside the still-running turn, and no `session/cancel` may be sent. With steering disabled, the log must instead contain `session/cancel`, and `change direction` must subsequently arrive as the next ordinary `session/prompt`.

## Progress

- [x] (2026-08-31 13:05Z) Inspected Hel's durable relay, ACP prompt loop, projection, terminal chat, web viewer, and reliability-lab paths.
- [x] (2026-08-31 13:05Z) Traced the reference steering implementation and recorded the exact capability advertisement, wire request, outcomes, and idle-race behavior in this plan.
- [ ] Milestone 1: add the capability and durable steering command without introducing a second queue.
- [ ] Milestone 2: send `_session/steering` concurrently with an active ACP prompt and settle every outcome before the turn ends.
- [ ] Milestone 3: expose queue-first steering through the terminal and web viewer with identical fallback behavior.
- [ ] Milestone 4: prove supported, unsupported, missed, restarted, and multi-client behavior end to end; document the feature and run the full validation suite.

## Surprises & Discoveries

- Observation: the provider-specific work already exists in the adapters bundled by Hel's agent-development image.
  Evidence: `containers/Containerfile.agent-dev` pins `@agentclientprotocol/codex-acp@1.6.2` and `@agentclientprotocol/claude-agent-acp@0.68.0`. Those adapter families advertise `InitializeResponse._meta.steering.supported`; Codex forwards the extension to its app-server turn steering and Claude maps it to immediate-priority input. Hel only lacks the client-side extension handling.

- Observation: `_session/steering` is an advertised ACP extension, not a typed ACP v1 request.
  Evidence: the request must use `agent_client_protocol::UntypedMessage` with method `_session/steering`, while support is a boolean at `InitializeResponse._meta.steering.supported`, a sibling of the standard agent capabilities.

- Observation: Hel's existing durable queue makes fallback materially simpler than an in-memory frontend.
  Evidence: the queued prompt remains in `RelaySnapshot.queued_prompts` until a steering success is journaled. A failed, missed, timed-out, or interrupted steer therefore needs no copied fallback payload: it completes the steering control while leaving the original prompt command queued.

- Observation: prompts and configuration changes share one ordered queue in Hel.
  Evidence: `RelayCommand::Prompt` and `RelayCommand::SetConfig` both become `StoredQueuedRelayCommand` entries, and `promote_next_queued_command` starts only the queue head. Steering must not skip a queued configuration change to reach a later prompt, because doing so would violate the order the user submitted.

- Observation: ACP adapter echoes do not create a second user entry in Hel today.
  Evidence: both `src/hel_projection.rs` and `src/hel_chat.rs` intentionally ignore `SessionUpdate::UserMessageChunk`. The durable steering-success projection must therefore create the user transcript item itself, and no echo-deduplication layer is required for this feature.

- Observation: a steering control can lose the race before it reaches the ACP command channel.
  Evidence: `claim_pending_commands_up_to` currently promotes the next queued prompt before it selects control commands. A steering command accepted during one active prompt could otherwise be claimed after that prompt ends and accidentally steer the newly promoted correction into itself. The relay must settle stale steering controls before promoting the queue.

## Decision Log

- Decision: reuse the existing durable prompt queue; do not create a steering queue.
  Rationale: the queue already supplies ordering, restart recovery, idempotent command identities, checkpoint inclusion, and visibility in every client. The new operation is a control that conditionally consumes the queue head, not a new kind of user content.
  Date/Author: 2026-08-31, Codex.

- Decision: keep prompt submission queue-first and make steering an explicit interrupt gesture.
  Rationale: pressing Enter while a turn runs must remain safe and reversible: it queues the instruction, which can still be edited or removed. Pressing Escape in the terminal, or the conversation's turn-control button in the web viewer, applies the oldest correction now when possible. This matches the requested workflow without making every mid-turn submission an irreversible steer.
  Date/Author: 2026-08-31, Codex.

- Decision: introduce `RelayCommand::SteerQueuedPrompt { queued_command_id }` as a durable ACP control.
  Rationale: naming the existing queue entry lets the relay validate first-in, first-out order and prevents two attached clients from consuming different interpretations of “oldest.” The queued prompt content remains owned by its original `RelayCommand::Prompt`; the steering command never duplicates it in the journal.
  Date/Author: 2026-08-31, Codex.

- Decision: only the first queue entry is steerable, and only when it is a prompt.
  Rationale: configuration changes deliberately share the queue with prompts. Skipping a configuration entry would reorder user intent. If the head is a configuration change, the interrupt gesture performs ordinary cancellation and leaves the complete queue intact.
  Date/Author: 2026-08-31, Codex.

- Decision: remove and terminalize the queued prompt only for the confirmed wire outcome `injected`.
  Rationale: `startedNewTurn`, `promptRequired`, `failed`, missing or unknown outcomes, transport errors, and timeouts do not prove delivery into the intended turn. Retaining the prompt favors a visible ordinary retry over silently losing an instruction.
  Date/Author: 2026-08-31, Codex.

- Decision: request `_meta.steering.idleBehavior = "promptRequired"` and reclaim `startedNewTurn` with `session/cancel` before allowing ordinary queue delivery.
  Rationale: `promptRequired` asks the adapter not to start an unowned turn if the original turn has already ended. Some Codex adapters can still answer `startedNewTurn`; cancelling that detached turn before the durable prompt is promoted prevents the same instruction from executing twice.
  Date/Author: 2026-08-31, Codex.

- Decision: settle an in-flight steer before publishing the original prompt's terminal event, with a two-second bounded wait when the prompt wins the race.
  Rationale: success must add the correction to the same running turn before `PromptFinished` changes the projection to idle and promotes later queue entries. A bound preserves runtime responsiveness if an adapter never answers the extension request.
  Date/Author: 2026-08-31, Codex.

- Decision: preserve Hel's existing post-error queue policy.
  Rationale: unlike the reference frontend's in-memory queue, Hel's durable relay currently proceeds to later queued prompts after an ACP prompt returns an error stop reason. A missed steer stays queued under the same policy; this feature must not silently introduce a separate drop-on-error rule.
  Date/Author: 2026-08-31, Codex.

- Decision: steering changes only the agent turn and does not cancel independent user shell commands.
  Rationale: a successful steer is not a cancellation. When steering is unavailable or no eligible prompt is queued, the existing Escape path remains unchanged and cancels the active agent turn together with the user shells it currently owns.
  Date/Author: 2026-08-31, Codex.

- Decision: add a distinct web action named `interrupt-turn`; do not overload the existing `cancel` action or dashboard Stop button.
  Rationale: `ControllerAction::Cancel` currently cancels an in-progress lifecycle action, while dashboard Stop checkpoints and destroys a session target. Mid-turn control is a different, non-destructive operation and must remain unambiguous at the API boundary.
  Date/Author: 2026-08-31, Codex.

- Decision: bump the relay protocol and snapshot schema when the new command and durable capability field land.
  Rationale: new controllers must not send an unknown command to an old worker, and an old worker must reject rather than misread a snapshot containing steering state. Additive wire fields retain serde defaults for reading old records, while explicit version increments protect downgrade behavior.
  Date/Author: 2026-08-31, Codex.

## Outcomes & Retrospective

The implementation has not started. Research confirms that no provider or model integration is needed: the bundled Codex and Claude ACP adapters already implement the extension. The planned production change is deliberately narrow—capability propagation, one durable control command, one untyped request in the active prompt loop, and two UI bindings—while the larger test matrix exists to prove that the small mechanism does not lose or duplicate a durable queued prompt at turn and process boundaries.

## Context and Orientation

Hel is a controller for long-lived coding-agent sessions. The root `hel-core` package owns session state and protocol behavior, `crates/hel-tui` owns the dashboard shell, and `crates/hel-cli` owns the daemon and web-server loops. The interactive chat itself is implemented in `src/hel_chat.rs` and `src/hel_chat/`; it is reused by the terminal dashboard rather than living in the `hel-tui` crate.

An ACP agent is the Codex, Claude, Kimi, Grok, or DeepSeek bridge process speaking Agent Client Protocol over JSON-RPC. `src/hel_acp.rs` initializes that process and owns its active `session/prompt` request. The standard ACP cancellation is a `session/cancel` notification. Mid-turn steering is an optional extension named `_session/steering`: it adds another user message to the currently running turn without completing that turn.

A relay is the target-side durable command ledger and event journal for one Hel session. `src/hel_worker/snapshot.rs` defines serialized commands, outcomes, observations, and the in-memory snapshot they fold into. `src/hel_worker.rs` validates submissions and promotes durable commands. `src/hel_worker_runtime/unix.rs` claims commands, converts them to ACP requests, records runtime results back into the relay, and interrupts in-flight commands if the harness restarts. A command is “terminalized” when its dispatch record is completed, rejected, or interrupted and its ledger entry has a terminal event ordinal.

The existing prompt path is store-and-forward. `RelayCommand::Prompt` is appended to `RelaySnapshot.queued_prompts`. `promote_next_queued_command` starts the head only when no prompt, configuration change, checkpoint barrier, or close owns execution. Starting a prompt removes it from the queue, installs it as `active_prompt`, and changes execution to running. `src/hel_projection.rs` turns those relay observations into a `MaterializedSession` containing the transcript and small queued-prompt projection stored by `src/hel_database.rs`.

The terminal path starts in `ChatState::handle_key` in `src/hel_chat.rs`. Enter produces `ChatAction::Prompt`; `ActiveChat::dispatch` in `src/hel_chat/active.rs` submits it through the background machinery in `src/hel_chat/remote.rs`. Escape currently produces `ChatAction::Cancel` while the session is running. `apply_session_view` already receives `RelayOperationalState`, so it can copy the new steering capability and any steering-control status into `ChatState` without a database migration.

The web viewer's public request and snapshot types live in `src/hel_server.rs`; the served HTML, CSS, and JavaScript are embedded there as constants. `crates/hel-cli/src/server.rs` subscribes to session-manager views, overlays live transcript and queue data into `ViewerSnapshot`, and executes `ControllerAction`s in background tasks. The existing web `cancel` action concerns lifecycle operations, and the existing dashboard Stop action closes the whole session. The new conversation-level action must therefore be `interrupt-turn`.

The reliability lab under `tests/e2e/reliability_lab.py` runs a fake ACP bridge, a real Hel daemon, terminal clients, and the web viewer against disposable state. `tests/e2e/run-reliability.sh` is its entry point. Extend this lab instead of creating a new crate or a credential-dependent test.

## Plan of Work

Milestone 1 adds durable steering semantics before any UI can invoke them. In `src/hel_worker/snapshot.rs`, add `steering_supported: bool` with serde defaults to `RelayObservation::AgentInitialized`, `RelaySnapshot`, and `RelayOperationalState`. Add `RelayCommand::SteerQueuedPrompt { queued_command_id: String }`, its command kind, and completion outcomes for `Steered { queued_command_id }` and `SteerDeferred { queued_command_id }`. A steered outcome means the target prompt entered the running turn; a deferred outcome means the control finished without consuming the target.

Update command classification so steering is an effectful ACP control but not a queue entry or relay-local command. Raise `RELAY_PROTOCOL_VERSION` from 6 to 7, return 7 from `minimum_protocol` for the steering command, and raise `RELAY_STATE_VERSION` from 2 to 3. Every newly added serialized field must have a default so v1/v2 journals and snapshots remain loadable by the new worker. Update snapshot fixtures explicitly where clarity is more valuable than relying on defaults.

In `DurableRelay::submit_command` in `src/hel_worker.rs`, accept a steering command only when an active prompt exists, the latest initialized agent advertises steering, the named command is exactly the first queued entry, that entry contains a prompt rather than a configuration change, and no queued, pending, or in-flight steering command already targets it. Return an `InvalidState` protocol error with actionable text for a stale target or unsupported agent. Keep idempotent resubmission by command ID unchanged.

Add a small pre-promotion pass in `claim_pending_commands_up_to`. Before `promote_next_queued_command`, complete any still-queued steering control as `SteerDeferred` when there is no longer an active prompt, steering support disappeared after a harness restart, or its named target is no longer the eligible queue head. This completion leaves the target prompt untouched. While the intended prompt is still active, let steering bypass it exactly as cancellation does. Do not let it bypass a checkpoint barrier or close.

Extend `ClaimedRelayCommand` with an optional steering payload built by the relay from the named queued prompt. Reuse the normal hidden-context attachment logic: attach pending memory and completed-shell context to the target prompt's command ID, not the steering control ID. On `Steered`, terminalize the target prompt as completed, remove it from the queue, and clear context attached to that target. On `SteerDeferred`, leave both the target and its attached context queued so ordinary promotion sends the same complete content later. If the worker or bridge dies while the steering control is in flight, the existing command-interruption path must terminalize only the control and leave its target queued.

Project a `Steered` completion in `src/hel_projection.rs` as a user transcript item with stable ID `user:<queued-command-id>` at the steering completion ordinal. Use the target's materialized queued content, close the open agent stream before inserting the user item, remove the target from the materialized queue, and leave `MaterializedExecutionState::Running` unchanged. A `SteerDeferred` completion changes neither the transcript nor the queued prompts. Add an optional active-steering summary to `RelayOperationalState`, derived from nonterminal steering dispatches, so clients can suppress duplicate gestures and render an in-flight label without storing it in the controller database.

Milestone 1 is complete when relay and projection unit tests demonstrate that a confirmed steer consumes exactly one FIFO prompt without ending the active turn, while every stale, unsupported, interrupted, or deferred path retains the prompt exactly once. Commit this milestone independently after focused tests pass.

Milestone 2 implements the small ACP extension. In `src/hel_acp.rs`, import `agent_client_protocol::UntypedMessage`, define the method constant `_session/steering`, and add a helper that reads only a literal boolean true from `InitializeResponse._meta.steering.supported`. Add the result to `RuntimeEvent::Connected` and record it through `RelayObservation::AgentInitialized` in `src/hel_worker_runtime/unix.rs`.

Add `CommandRequest::Steer { request_id, prompt }`, `RuntimeEvent::SteerApplied { request_id }`, and `RuntimeEvent::SteerDeferred { request_id, message }`. `acp_command` in `src/hel_worker_runtime/unix.rs` must create the steering request only from the relay-resolved steering payload; a missing payload is a relay invariant failure, not permission to fetch or guess another queue entry.

While `serve_session` owns a running `PromptRequest`, accept one steering command alongside cancellation, close, and elicitation responses. Send an untyped request with this exact shape, using the existing ACP `ContentBlock` values without translating them:

    method: _session/steering
    params:
      sessionId: <the active ACP session id>
      prompt: <the queued prompt content blocks, including attached hidden context>
      _meta:
        steering:
          idleBehavior: promptRequired

Keep the steering future in the prompt loop's `tokio::select!` rather than awaiting it inline, so prompt completion, cancellation, close, elicitation answers, and shutdown remain responsive. Only one steering request may be in flight; defensively defer a second request even though relay admission should prevent it.

Interpret a response object whose string `outcome` is `injected` as `SteerApplied`. Interpret `promptRequired`, `failed`, a missing or unknown outcome, and a request error as `SteerDeferred`, accompanied by a bounded warning that says the queued correction will run next. For `startedNewTurn`, first clear or cancel any connection-owned pending interaction that would outlive that detached turn, send `session/cancel`, record a warning, and then emit `SteerDeferred`. The cancellation must reach the adapter before the original queued prompt can be promoted.

If the ordinary prompt completes while a steering request is outstanding, wait no more than two seconds for the steering result, emit `SteerApplied` or `SteerDeferred`, and only then emit `PromptFinished`. On close or command-channel shutdown, emit `CommandInterrupted` for both the active prompt and any steering command before tearing down the connection. On a harness restart, rely on the coordinator's existing in-flight interruption to preserve the target queue entry.

In `record_runtime_event`, look up the steering command in the `in_flight` map, extract its target identity, and record the matching `RelayCommandOutcome`. Do not trust a target ID returned by the adapter. Remove the steering command from `in_flight` only after its durable terminal event is written.

Milestone 2 is complete when the mock ACP tests prove wire shape, capability gating, event order, timeout behavior, and `startedNewTurn` cancellation order. The existing prompt and cancellation tests must still pass for agents whose initialize metadata omits steering. Commit this milestone independently.

Milestone 3 exposes the behavior through both clients. Add `steering_supported` and the optional active steering target to `ChatState` in `src/hel_chat.rs`, and set them whenever `apply_session_view` in `src/hel_chat/active.rs` receives a live operational snapshot. Add `ChatAction::SteerQueuedPrompt` and the corresponding background operation in `src/hel_chat/remote.rs`. When the session is running, no steering control is already pending, steering is supported, and the first visible queue entry is a prompt, Escape submits `RelayCommand::SteerQueuedPrompt` for that exact ID. Otherwise Escape retains the existing cancellation action. Enter continues to queue under all circumstances; Ctrl-C retains Hel's current composer behavior.

Do not optimistically remove a correction from the terminal queue or add it to the transcript. Show a short `Steering queued correction…` notice after command acceptance, then let the durable `Steered` projection perform both changes on confirmed delivery. If submission loses a multi-client race, keep the correction visible and show the relay's actionable error. Update the chat footer and running-session header so the control reads `Esc steers next` only when it actually will; otherwise it reads `Esc cancels` as today.

In `src/hel_server.rs`, extend `ViewerSession` with defaulted live fields sufficient to render the turn control: whether an ACP prompt is running, whether steering is supported, whether a steering control is pending, and whether the FIFO head is a steerable prompt. Add `ControllerAction::InterruptTurn { session_id }`. In `crates/hel-cli/src/server.rs`, retain those live facts from each `ManagedSessionView` alongside the existing conversation, queue, and shell maps. When performing `interrupt-turn`, reacquire the session's latest view: submit `SteerQueuedPrompt` for its exact FIFO head only when all steering conditions still hold; otherwise submit `RelayCommand::Cancel`. This server-side recheck is the authority, not the browser's possibly stale snapshot.

Add a conversation-level button to the embedded viewer. Hide or disable it while no agent turn is active. Label it `Steer next` when the live snapshot says the FIFO correction can be steered; otherwise label it `Cancel turn`. Keep the dashboard's existing Stop button unchanged because it checkpoints and tears down the whole session. Disable the turn-control button while its HTTP request is outstanding and refresh the snapshot after acceptance. Phone API validation must distinguish the new action from lifecycle cancellation and return the existing safe public error form without exposing target or profile details.

Milestone 3 is complete when reducer tests and web-server tests prove the same matrix: supported plus eligible queue head steers; unsupported, empty queue, queued configuration head, already-pending steer, and stale turn fall back to or report ordinary cancellation exactly as specified. Commit this milestone independently.

Milestone 4 adds behavior-level proof and documentation. Extend the fake ACP bridge in `tests/e2e/reliability_lab.py` with a mode that advertises steering, holds an ordinary prompt open, records untyped steering requests, and returns each defined outcome on command from the scenario driver. Add a `mid-turn-steering` scenario to `tests/e2e/run-reliability.sh`. Use one real terminal client and the authenticated web viewer against the same session so either surface can queue and the other can steer.

The supported scenario must prove that the first of two queued prompts is injected exactly once, the second remains queued, the original prompt remains active until the driver releases it, every client shows the steered user message in the same turn, and no cancellation was sent. Repeat with steering omitted from initialize metadata and prove that the turn is cancelled and the oldest queued correction becomes the next ordinary prompt. Add variants for `promptRequired`, `startedNewTurn`, steering request timeout at turn completion, bridge death after dispatch but before response, a configuration entry at the queue head, and two clients attempting to steer the same target. Each variant must assert no lost prompt, no duplicate ordinary prompt, FIFO order, and converged transcript/queue state after reconnect.

Update `README.md` near the quickstart and durability sections. Explain the queue-first gesture, capability fallback, terminal key, web label, and the fact that a correction is removed only after confirmed injection. Do not claim all harnesses support steering; say that Hel uses it when the connected ACP agent advertises the extension. The Starlight index currently delegates general usage to the README, so no new human documentation page is required unless that documentation structure changes during implementation.

Milestone 4 and the feature are complete when focused tests, the new reliability scenario, the full Rust suite, Clippy, formatting, and diff checks pass. Record actual test counts and any adapter deviations in this plan's living sections, then commit the final validated documentation and test changes.

## Concrete Steps

Work from `/workspace/hel`. At every stopping point, update this file's `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective`, and append a change note at the bottom. Commit each coherent, validated milestone on the current branch. Stage only files changed for this feature.

Begin with the relay and projection tests, then implement Milestone 1:

    cargo test -p hel-core hel_worker
    cargo test -p hel-core hel_projection

The new focused tests should include names equivalent to:

    steer_claims_only_the_fifo_prompt_during_an_active_turn
    confirmed_steer_terminalizes_its_target_exactly_once
    deferred_or_interrupted_steer_leaves_its_target_queued
    stale_steer_is_settled_before_the_target_prompt_is_promoted
    queued_configuration_is_not_overtaken_by_steering
    steered_prompt_is_projected_inside_the_running_turn

Implement and validate the ACP layer with:

    cargo test -p hel-core hel_acp
    cargo test -p hel-core hel_worker_runtime

The ACP mock must make the following observable assertions: initialize metadata without a boolean true yields `steering_supported == false`; the request method and parameters exactly match the contract in this plan; `injected` precedes `PromptFinished`; `startedNewTurn` logs `session/cancel` before the prompt is allowed to run normally; and a missing steering response delays turn completion by no more than approximately two seconds.

Implement and validate the client surfaces with:

    cargo test -p hel-core hel_chat
    cargo test -p hel-core hel_server
    cargo test -p hel-cli server

Build the normal repository target before the system scenario:

    cargo build
    tests/e2e/run-reliability.sh --scenario mid-turn-steering --seed 1 ./target/x86_64-unknown-linux-musl/debug/hel

The scenario must end with a line equivalent to:

    reliability: passed scenario=mid-turn-steering seed=1 clients=3 leaks=0

Before each milestone commit, format the Rust files changed in that milestone and run `git diff --check`. Before declaring the feature complete, run all repository-required validation outside the restricted sandbox as required by `AGENTS.md`:

    cargo fmt --all -- --check
    cargo test
    cargo clippy --all-targets -- -D warnings
    git diff --check

If the host is not x86_64 Linux, pass the host target explicitly as described in `AGENTS.md`; the checked-in `.cargo/config.toml` otherwise builds the musl worker/controller binary used by the reliability lab.

## Validation and Acceptance

The feature is accepted only when a human can observe all of the following behavior, backed by automated tests.

With a supported agent, start a long-running prompt, submit two more prompts, and activate the turn control. The oldest queued prompt appears as a user message after the already-streamed portion of the agent response. The session still reports the original turn as running, the second prompt remains queued, and the fake ACP log contains exactly one `_session/steering` request with `idleBehavior: promptRequired` and no `session/cancel`.

With an unsupported agent, the same gesture sends one `session/cancel`, does not send `_session/steering`, retains both queued prompts until the cancelled turn settles, and then starts the oldest as the next ordinary `session/prompt`.

When a supported agent returns `promptRequired`, `failed`, an unknown response, or an error, or fails to answer before the active prompt completes, Hel leaves the correction queued and sends it once as the next ordinary prompt. When it returns `startedNewTurn`, Hel sends `session/cancel` before ordinary delivery; the correction still executes exactly once.

If the bridge or worker stops after the steering command is durable but before it confirms delivery, reopening the relay shows the steering control interrupted and the original correction still queued. If two clients race to steer the same queue head, at most one steering command is admitted and the correction appears only once. A queued configuration entry is never bypassed.

The terminal and web viewer converge on the same transcript, active-turn state, and queue after every case. Enter always queues. The terminal footer and web button never advertise steering when the connected agent omitted the capability, when no turn is active, when the queue head is not a prompt, or while another steering control owns the target. Dashboard Stop continues to checkpoint and close the session rather than steering it.

No event/render loop may block while a steering request is in flight. Cancellation, close, elicitation answers, detach, web refresh, and terminal quit remain responsive. The two-second settlement wait runs only in the ACP runtime task after the original prompt resolves; it never runs on a TUI or web loop.

## Idempotence and Recovery

All new relay submissions use caller-generated command IDs and inherit the existing idempotency rule: retrying the same ID with the same steering target returns the original acceptance, while reusing it for another target is rejected. A steering success and its target terminalization occur in one durable relay event fold, so reopening after snapshot persistence failure can replay the event without consuming the prompt twice.

A deferred, rejected, interrupted, or stale steering control never removes its target prompt. After restart, ordinary queue promotion is therefore the recovery path; do not synthesize a replacement prompt. Hidden prompt context stays attached to the original prompt command ID across the same failures and is cleared only on confirmed steering or ordinary prompt completion.

The system scenarios use newly created isolated state roots and the existing supervised process cleanup. Stop process groups before removing their working files. A failed scenario retains artifacts and prints its exact replay command. Do not test this feature against personal credentials or a real repository.

If implementation reveals that the pinned Codex or Claude adapter does not advertise the documented capability, record the exact initialize response in `Surprises & Discoveries` and update only the pinned adapter version in the agent-development image after proving the required extension in the fake bridge. Do not infer steering support from harness kind or agent name.

## Artifacts and Notes

The extension contract embedded here is the source needed to implement the feature; no external repository is required:

    InitializeResponse._meta:
      steering:
        supported: true

    Request:
      method: _session/steering
      params:
        sessionId: session-123
        prompt:
          - type: text
            text: change direction
        _meta:
          steering:
            idleBehavior: promptRequired

    Confirmed response:
      outcome: injected

    Non-delivery responses:
      outcome: promptRequired
      outcome: failed

    Idle-race response requiring cancellation before fallback:
      outcome: startedNewTurn

The reference implementation was introduced by commit `ac08da7d4ea83389aeb46c2235368e7e554b3f65` and changed to queue-first explicit steering by `9bc1ef516788a79e2ed17323a77e1a2d19443851`. Those identifiers are provenance only. All behavior required from them is restated in this plan.

The most important event ordering for a confirmed steer is:

    CommandQueued(steer-control)
    CommandStarted(steer-control)
    _session/steering -> outcome injected
    CommandCompleted(steer-control, Steered(target-prompt))
    PromptFinished(original-prompt)

For a missed steer, replace `Steered` with `SteerDeferred`; the target prompt remains in the queue and is promoted only after `PromptFinished(original-prompt)`.

## Interfaces and Dependencies

Do not add a crate or a new external dependency. The existing `agent-client-protocol` dependency already exports `UntypedMessage`, and the existing `serde_json`, Tokio, relay journal, session manager, and fake ACP infrastructure are sufficient.

At the end of Milestone 1, `src/hel_worker/snapshot.rs` must expose serialized shapes equivalent to:

    RelayCommand::SteerQueuedPrompt {
        queued_command_id: String,
    }

    RelayCommandOutcome::Steered {
        queued_command_id: String,
    }

    RelayCommandOutcome::SteerDeferred {
        queued_command_id: String,
    }

    RelayOperationalState {
        steering_supported: bool,
        active_steering_target: Option<String>,
        ...
    }

Use an explicit small struct instead of `Option<String>` for the operational active steering state if clients also need the steering control command ID. Keep all new operational fields defaulted for older worker responses.

At the end of Milestone 2, `src/hel_acp.rs` and `src/hel_worker_runtime/unix.rs` must contain interfaces equivalent to:

    CommandRequest::Steer {
        request_id: String,
        prompt: Vec<ContentBlock>,
    }

    RuntimeEvent::SteerApplied {
        request_id: String,
    }

    RuntimeEvent::SteerDeferred {
        request_id: String,
        message: String,
    }

    fn steering_supported_from_meta(meta: Option<&Meta>) -> bool;

The runtime's pending steering future needs only its steering command ID and response future. It must not retain a second copy of the fallback prompt because the durable relay still owns that prompt.

At the end of Milestone 3, both clients submit the same relay-level intent. The terminal interface is `ChatAction::SteerQueuedPrompt { queued_command_id }`. The public web interface is a `ControllerAction` serialized as:

    {
      "action": "interrupt-turn",
      "session_id": "..."
    }

The server resolves that intent against the latest live session view and submits either `SteerQueuedPrompt` or the existing `Cancel`. No browser-provided capability boolean or queue target is trusted.

Change note, 2026-08-31: Initial ExecPlan created after examining Hel master and the complete reference steering history. The design uses Hel's durable FIFO as the fallback authority, preserves interleaved configuration ordering, and limits the production mechanism to one capability flag, one relay control, and one untyped ACP request.
