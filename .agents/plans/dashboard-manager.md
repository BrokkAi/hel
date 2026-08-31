# Add an advisory dashboard manager

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds. This document follows `.agents/PLANS.md` from the repository root.

## Purpose / Big Picture

After this change, a person can press Ctrl+M from Hel's terminal dashboard and open a workspace-level manager. The manager shows every Hel session, identifies live sessions that have been idle long enough to consider stopping, identifies old archived sessions that can be permanently cleaned up, and offers the existing safe lifecycle confirmations for those operations. The right side of the view is a small conversation with its own prompt box. Questions are answered by a scratch model running through an idle live session, with a bounded snapshot of all current sessions and their recent work; the manager never mutates lifecycle state merely because a model suggested it.

The behavior is visible without a real model in focused TUI tests: session assessment and keyboard actions are pure state transitions. A real configured dashboard with at least one connected idle session demonstrates the model path, while the full test suite proves that its relay request remains off the terminal event loop and survives the dashboard-to-daemon boundary.

## Progress

- [x] (2026-08-31 00:00Z) Read the dashboard, session projection, lifecycle confirmation, relay manager, and daemon protocol boundaries.
- [x] (2026-08-31 00:00Z) Chose an advisory first version with explicit user confirmation and an idle-session scratch-model backend.
- [x] (2026-08-31 12:28Z) Added the manager assessment, retained conversation, prompt input, responsive rendering, and focused behavior tests to `hel-tui`.
- [x] (2026-08-31 12:31Z) Added the non-blocking manager query action and retained result path to the CLI dashboard.
- [x] (2026-08-31 12:39Z) Added scratch-model requests to local and daemon-backed session manager handles, including an actor-side idle recheck and remote-facade transport coverage.
- [x] (2026-08-31 13:00Z) Passed focused tests, the full default Rust suite, formatting, `git diff --check`, and clippy with warnings denied.
- [x] (2026-08-31 13:00Z) Updated the README and completed this plan's implementation record. The validated commit remains the final repository step.

## Surprises & Discoveries

- Observation: Hel already has the safe lifecycle operations the manager needs. Stopping checkpoints before teardown, destroying refuses active sessions, and both operations already have TUI confirmation flows.
  Evidence: `DashboardAction::Close` and `DashboardAction::DestroyStopped` in `crates/hel-tui/src/lib.rs` dispatch through `crates/hel-cli/src/dashboard/actions.rs` to daemon-owned lifecycle operations.

- Observation: The existing relay supports a scratch ACP request named `compact`, which starts a disposable native model session and returns only agent text. It is suitable for advisory manager answers without polluting a coding session's transcript.
  Evidence: `StandaloneSession::compact` in `src/hel_session_manager.rs` calls `WorkerClient::compact`, and `compact_in_scratch_session` in `src/hel_acp.rs` creates and closes the scratch ACP session.

- Observation: A scratch request shares the primary harness connection and therefore cannot be concurrent with a working turn on that session.
  Evidence: `CommandRequest::Compact` is consumed by the same sequential request loop as `CommandRequest::Prompt` in `src/hel_acp.rs`.

- Observation: The repository's default Cargo target is `x86_64-unknown-linux-musl`, but this disposable environment provides an AArch64 GNU Rust toolchain and not that target.
  Evidence: the unqualified focused test reported the missing musl target; validation succeeded with `--target aarch64-unknown-linux-gnu` after selecting `/usr/local/rustup/toolchains/1.98.0-aarch64-unknown-linux-gnu/bin` on `PATH`.

- Observation: The optional `voice-worker` workspace member needs the system ALSA development package, which is not installed in this image. It is not a default workspace member and is unrelated to the dashboard manager.
  Evidence: an exploratory `cargo test --workspace manager` stopped in `alsa-sys` because `alsa.pc` was unavailable. The required default-member suite subsequently completed successfully.

## Decision Log

- Decision: The first version is advisory. Model output is text, while stop, archive, and permanent cleanup remain typed UI actions with existing confirmation rules.
  Rationale: Natural-language model output is not a safe authorization boundary for destructive lifecycle operations. The UI can offer the useful operation immediately without allowing a hallucinated action to run.
  Date/Author: 2026-08-31 / Codex

- Decision: A manager query may use only a connected session that is currently idle and has no queued prompts or lifecycle operation.
  Rationale: Scratch requests share that session's ACP connection. Refusing when all sessions are busy preserves independent agent work and makes the limitation visible instead of silently delaying a coding prompt.
  Date/Author: 2026-08-31 / Codex

- Decision: Treat a live session as a stop candidate after two hours without projected activity, and treat an archived inactive session as a cleanup candidate after thirty days. Missing or invalid activity timestamps never qualify automatically.
  Rationale: Automatic classification should be conservative. The manager may explain other sessions, but only strong timestamp evidence produces a lifecycle recommendation.
  Date/Author: 2026-08-31 / Codex

- Decision: Keep the manager mini transcript across closing and reopening the manager within one dashboard run, but do not add a database migration in this slice.
  Rationale: The requested independent conversation is immediately useful and is passed back into every scratch query. Durable manager history is a separate retention-policy decision and should not be silently introduced as unbounded workspace data.
  Date/Author: 2026-08-31 / Codex

- Decision: Implement terminal-dashboard support first; do not expose a partial model conversation in the phone viewer.
  Rationale: The current web action channel returns mutation success rather than streaming model content. Adding server-owned conversation state is an independent protocol and persistence milestone; a half-parity implementation would imply that manager history is shared when it is not.
  Date/Author: 2026-08-31 / Codex

- Decision: Recheck materialized execution and the durable queue inside the session actor immediately before starting the scratch request.
  Rationale: The pure dashboard snapshot can become stale while an asynchronous request crosses the daemon boundary. The authoritative actor gate prevents a manager question from slipping in after real agent work has started.
  Date/Author: 2026-08-31 / Codex

## Outcomes & Retrospective

The terminal dashboard now has the planned advisory manager behind `Ctrl+M`. It derives status from the dashboard's existing projections, conservatively recommends stopping after two hours or permanent archived cleanup after thirty days, and routes those operations through Hel's existing typed actions and confirmations. Its prompt uses one available idle session for a bounded, redacted scratch-model request and retains replies in an independent mini transcript for the lifetime of the dashboard process. Work is asynchronous from the terminal loop, and the session actor closes the stale-snapshot race by checking idleness again at execution time.

Validation completed on the host target: the default-member `cargo test` suite passed (70 CLI tests, 1 logging test, 2 PTY termination tests, 1,434 core tests with 4 ignored, 211 TUI tests with 1 ignored, and credential-gated import cases ignored); focused daemon and remote-session-manager tests passed; `cargo fmt --all -- --check`, `cargo clippy --all-targets -- -D warnings`, and `git diff --check` passed. The exploratory all-workspace command could not compile the optional voice worker because this image lacks ALSA development metadata, as recorded above.

Durable manager transcript history and phone-viewer parity remain deliberately deferred. The delivered version neither introduces unbounded database retention nor implies that the terminal-local conversation is shared with the web surface.

## Context and Orientation

Hel is a Rust terminal control plane for long-lived coding-agent sessions. `crates/hel-cli/src/dashboard.rs` owns the one terminal event loop and starts all filesystem, network, process, and database work in background tasks. `crates/hel-tui/src/lib.rs` is the pure dashboard state machine: it turns keyboard and mouse events into `DashboardAction` values without performing I/O. `crates/hel-tui/src/render.rs` renders that state. This separation must remain intact.

A session record in `src/hel_state.rs` holds lifecycle state and timestamps. Live transcript projections are reduced into `SessionDetail` in `crates/hel-tui/src/ingest.rs`; this includes last activity, current turn state, queued prompts, recent messages, and an optional renderable transcript snapshot. The manager assessment will use only these already-loaded values and will never read the filesystem during rendering.

The dashboard talks to the controller daemon through a remote `SessionManagerControl`. `ManagedSessionHandle` in `src/hel_session_manager.rs` currently supports prompt submission, synchronization, elicitation responses, and second-opinion reviewer actions. A new `compact` method will travel through the same actor and daemon request bridge to `StandaloneSession::compact`. Here, “compact” means a scratch model call whose response is plain agent text and is not added to the coding session's history; for the manager it is a general bounded advisory call, not transcript compaction.

An “idle stop candidate” means a session whose lifecycle is active, whose projected agent execution is idle, whose durable prompt queue is empty, which has no lifecycle operation in flight, and whose most recent projected activity is at least two hours old. An “old archive cleanup candidate” means an inactive record with its display-only `archived` flag set and an `updated_at` timestamp at least thirty days old. “Destroy” is permanent cleanup: Hel removes the stopped record, checkpoint archive, and other owned artifacts. Existing confirmation copy must remain in front of this action.

## Plan of Work

Create `crates/hel-tui/src/manager.rs` for the manager-specific pure state. Define the manager focus, message role and message records, assessed session row, recommendation, and query payload there. Keep a `ManagerState` field on `DashboardState` rather than inside the modal enum so its mini transcript survives closing and reopening. Add `Mode::Manager`, Ctrl+M entry, prompt editing, focus navigation, selected-row stop/archive/destroy actions, paste handling, and stale-result rejection by monotonically increasing request id. Build model context from bounded session metadata, last messages, and a small transcript tail. Do not serialize raw resource locators, native session ids, errors, credentials, environment, or profile paths.

Extend `crates/hel-tui/src/render.rs` with a full-frame manager layout: assessed sessions on the left, the manager mini transcript on the right, and a one-line prompt at the bottom. On narrow terminals stack the session assessment over the conversation. Render exact key hints and explicitly label model availability and destructive recommendations.

Add `DashboardAction::AskManager` and a `DashboardIoUpdate::ManagerReply`. In `crates/hel-cli/src/dashboard/actions.rs`, choose only the idle backend candidates supplied by the pure dashboard state, spawn an asynchronous request, and return its result through the existing dashboard I/O channel. The result must name the source session so the transcript can say which profile/session provided the scratch model. Failure must become an assistant-visible error and clear the in-flight state. No query work may run on the render loop.

Extend `ManagedSessionHandle` with `compact(prompt)`. Add an actor command, local actor handling, remote actor request, daemon action and text reply, daemon client method, and in-process remote-request bridge handling. Update exhaustive test fakes and add focused tests that prove a compact request crosses the remote facade and returns text. Bump the private daemon protocol version because its serialized action and reply enums change.

Use the existing close, archive, and destroy actions. The manager should open the existing confirmation dialog for close and destroy, and should use `SetSessionArchived` for the reversible display flag. A manager result that arrives while the manager is closed must update its retained transcript without forcing the view open.

Update `README.md` to explain the manager and revise the old “Hel is not an agent” non-goal: Hel still does not autonomously write code or schedule work, but it now hosts an advisory control-plane model over explicitly confirmed operations.

## Concrete Steps

From `/workspace/hel`, edit the pure TUI model and run:

    cargo test -p hel-tui manager

Expect the new manager assessment and input tests to pass. These tests must cover the two-hour and thirty-day thresholds, missing timestamps, busy sessions, prompt submission, paste, stale replies, and typed lifecycle actions.

Then edit the session-manager and daemon transport and run:

    cargo test -p hel-core session_manager
    cargo test -p hel-cli daemon

Expect compact request tests to pass for both the in-process actor and the remote facade. The daemon protocol test fixtures must agree on the incremented protocol version.

After integrating the CLI dashboard result path, run from `/workspace/hel`:

    cargo fmt --all -- --check
    cargo test
    cargo clippy --all-targets -- -D warnings

All commands must succeed. This environment is unrestricted; do not redirect Cargo output into `/tmp`.

Finally inspect `git diff --check` and `git status --short`, stage only files changed for this feature, and commit directly to the current branch with a descriptive message.

## Validation and Acceptance

With at least one configured, connected, idle session, run `hel`, press Ctrl+M, and observe a manager view with a session assessment, an independent transcript, and a prompt. Type “Summarize what my sessions are doing” and press Enter. The prompt appears immediately, the UI stays responsive, a busy indicator appears, and the eventual reply cites the scratch provider session in the manager status without adding a turn to that session's visible chat.

A live session with no queued prompt, no running turn, and projected activity older than two hours is labeled as an idle stop candidate. Selecting it and pressing the displayed stop key opens the same recovery-copy confirmation used by the normal dashboard. Cancelling changes nothing; confirming starts the ordinary background stop operation.

An inactive archived session older than thirty days is labeled as a cleanup candidate. Selecting it and pressing the displayed destroy key opens the existing permanent-destruction confirmation. No model text can skip this confirmation. A recent archive and a record with an invalid timestamp are not automatically recommended for cleanup.

When every live session is working, queued, disconnected, or in a lifecycle operation, submitting a manager prompt fails promptly with a useful transcript message explaining that an idle model provider is required. Existing coding turns and queues remain unchanged.

Closing the manager with Escape and reopening it with Ctrl+M retains its messages for that dashboard run. Returning to a normal session chat and stopping or resuming sessions continues to work as before.

## Idempotence and Recovery

The assessment is derived from current in-memory projections on every access and is safe to repeat. Model queries have no lifecycle side effect and use scratch sessions that the ACP runtime closes after each answer. If a query fails, the in-flight marker clears and the user can retry. A stale reply whose request id no longer matches cannot overwrite a newer turn.

Stop and destroy reuse existing lifecycle operations and confirmations. Their established checkpoint and active-state gates are the recovery boundary; the manager adds no alternate destructive path. If implementation is interrupted, this plan's `Progress` section identifies the next incomplete milestone, and focused package tests can be rerun without cleanup.

## Artifacts and Notes

The manager system prompt must include language equivalent to:

    You are Hel's advisory dashboard manager. Analyze only the supplied redacted inventory.
    Never claim that you stopped, archived, destroyed, resumed, or messaged a session.
    Those actions require explicit confirmation in the dashboard. Distinguish facts from suggestions.

The inventory should identify sessions by display title and short Hel id, and include lifecycle state, working/idle/queued assessment, project label, profile id, target template id, last activity age, and bounded recent conversation text. It must not include target locators, host addresses, native session ids, profile homes, environment values, or raw errors.

## Interfaces and Dependencies

In `crates/hel-tui/src/manager.rs`, define manager-owned equivalents of:

    pub struct ManagerQuery {
        pub request_id: u64,
        pub prompt: String,
        pub model_prompt: String,
        pub backend_session_ids: Vec<String>,
    }

    impl DashboardState {
        pub fn apply_manager_reply(
            &mut self,
            request_id: u64,
            source_session_id: Option<String>,
            result: Result<String, String>,
        );
    }

Exact field visibility may remain crate-private where the CLI does not need it, but the `DashboardAction` payload and result application must cross the `hel-tui` crate boundary without exposing the rest of `DashboardState`.

In `src/hel_session_manager.rs`, add:

    impl ManagedSessionHandle {
        pub async fn compact(&self, prompt: String) -> anyhow::Result<String>;
    }

The local actor must call the existing `StandaloneSession::compact`; the remote actor must produce a `RemoteSessionRequest::Compact`, and the daemon must serialize a matching action and text reply. No new third-party dependency is needed.

Revision note (2026-08-31): Created the plan after repository research and recorded the advisory safety boundary, idle scratch-provider constraint, conservative time thresholds, in-process transcript retention, and terminal-first scope.

Revision note (2026-08-31 13:00Z): Recorded the completed TUI, asynchronous query path, daemon transport, actor-side idle race gate, documentation, validation evidence, host-target constraint, and optional ALSA dependency limitation.
