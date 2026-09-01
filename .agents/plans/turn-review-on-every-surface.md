# Turn review hosted by the daemon and usable from every surface

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds. Maintain this document in accordance with `.agents/PLANS.md` at the repository root.

This plan builds on `.agents/plans/cross-harness-turn-review.md` (checked in, complete), which ported mjolnir's discrete review into Hel and is incorporated here by reference for everything about lanes, prompts, tiers, verdicts, Bifrost, and the delta-capture protocol. This plan does not change any of that. It changes *where the review runs* and *who can see and resolve it*.

## Purpose / Big Picture

Turn review shipped working only for a user sitting at the terminal. The review driver runs inside the TUI's view object, so a session driven from the phone browser never gets reviewed; the phone's command palette does not know `/review`, so typing it on a phone sends the literal text to the coding agent; and worst, a review opened in the terminal locks the phone out — the server refuses phone prompts with "forward, dismiss or cancel it first" while the phone has no way to do any of those three, and a hard-killed TUI strands that lock until a terminal reattaches.

After this plan:

1. One global switch (`[review] enabled = true` in `config.toml`) arms automatic review for every session, and the review actually runs for every session — terminal-attached, phone-driven, or nobody-attached — because the driver moves into the controller daemon, the one process that always owns every session. This is the primary goal.
2. The review is visible and resolvable from both surfaces: the terminal keeps its split pane; the phone gets a review card with the role strip, the verdict synthesis, and Forward / Dismiss / Cancel buttons. A review can therefore never lock a surface out.
3. Bare `/review` runs a one-off review from either composer, whether or not automatic review is armed. The composer no longer mutates global configuration — that was a category error this plan removes.

To see it working: set `[review] enabled = true`, open a session from the phone with no terminal anywhere, ask the agent to edit a file, and watch the review card appear when the turn finishes; press Forward on the phone and watch the corrective turn start.

## Progress

- [ ] Milestone 1: the daemon hosts the driver (trigger, execution, lock, and state ownership move out of the TUI; the terminal experience is unchanged).
- [ ] Milestone 2: the phone surface (review projection on `ViewerSession`, review card with resolution buttons, `/review` and `/review status` on the phone palette). Milestones 1 and 2 must ship in the same release; between them a phone-driven armed session could hit the lock with no buttons.
- [ ] Milestone 3: global `[review]` config arms auto-review; the composer command becomes one-off + status only; the `turn_review_settings` workspace table is dropped.
- [ ] Milestone 4: recovery under daemon ownership, removal of the superseded TUI code paths, parity-note update, retrospective.

## Surprises & Discoveries

Findings from the research pass that shaped this plan (2026-09-01; all citations verified that day):

- Observation: the TUI process runs no session actors at all. `spawn_session_manager` has exactly one production call site — `crates/hel-cli/src/daemon.rs:2200` — and the TUI uses `spawn_remote_session_manager` (`src/hel_session_manager.rs:1264`), a pure command→RPC translator with no transport of its own. Moving the driver daemon-side removes a hop instead of adding a process.
- Observation: the shipped lock can strand the phone. `refuse_prompt_during_turn_review` (`src/hel_server.rs:1551`) reads `turn_review_state.active`, which only the TUI writes; a SIGKILL'd TUI leaves the row set, and the only thing that clears it is the next TUI attach (`src/hel_chat/active.rs:633`). Daemon ownership dissolves this: the process that owns the lock is the process that owns the review.
- Observation: `RuntimeLifecycleView` (`crates/hel-cli/src/daemon.rs:147-156`) is precedent for exactly what review phase needs — daemon-owned, in-flight, multi-stage progress published to the TUI through the 30-second `RuntimeSnapshot` long poll (`daemon.rs:699-756`, woken by `publish_revision`) and to the phone in-process. The relay journal is the wrong layer for phase: it is the protocol-versioned harness conversation, and a new `RelayObservation` variant costs a protocol bump; `RelayCommand::RecordNotice` (`src/hel_worker/snapshot.rs:79-85`, precedent at `src/hel_controller/resume.rs:1164`) already covers the one thing the journal *should* carry, the human-visible resolution line.
- Observation: the daemon hot-reloads `config.toml` every 500 ms (`spawn_manager_target_refresher`, `daemon.rs:2357-2409`) and many daemon handlers call `Controller::load()` per action (`daemon.rs:893` and ten siblings). Global review config needs no new reload machinery.
- Observation: the phone already has a generic structured-card channel — elicitations (`buildElicitationCard`, `src/web/viewer.js:1191`; plan review rides it via `normalized_plan_review`, `src/hel_acp.rs:745-790`) — but `pending_elicitations` is journal-derived, and review phase is daemon state, so review gets its own snapshot field and action instead of a synthetic elicitation.
- Observation: mjolnir hosts its review driver in the session-owning process for all of its surfaces (`mj-core/src/orchestrator.rs:544`, spawned by the TUI at `mjolnir/src/main.rs:2815`, headless at `src/headless.rs:228`, and the web server at `src/remote_host.rs:663`), and arms it from its global config file. This plan converges Hel on that shape, which matters for a possible re-merge.
- Observation (out of scope, recorded so it is not lost): phone-submitted prompts never enter prompt history — `record_prompt` has exactly one caller, on the TUI's daemon path (`crates/hel-cli/src/daemon.rs:2981`); the phone's in-process bridge skips it.

## Decision Log

- Decision: the turn-review driver, trigger, lock, and state writes move from `ActiveChat` into a daemon-side host that lives beside the session manager. The TUI becomes a projection of the review plus a sender of resolution actions, exactly like the phone.
  Rationale: the daemon is the only process that pumps every session (actor sync every 150 ms regardless of attachment), the only SQLite writer, and the process the phone server runs in. The shipped TUI-hosted driver made review a property of a view object, which is why headless sessions were never reviewed and why a dead TUI could strand the lock. mj's orchestrator proves the shape.
  Date/Author: 2026-09-01 / jbellis + Fable.
- Decision: automatic review is armed by global configuration — a `[review]` section in `config.toml` (`enabled`, `tier`) — not by a per-workspace SQLite row and not by a composer command. This supersedes the prior plan's decisions "two workspace-scoped values in SQLite, nothing in `config.toml`" and "`/review on|off` toggles automatic review". The maintainer's priorities: global auto-review per turn is the primary goal; `/review` as a manual one-off is the secondary goal; both are wanted.
  Rationale: a slash command is a session-scoped gesture and should not mutate shared configuration ("wiring a global config up there is not optimal"); arming review is user configuration, and `config.toml` is where Hel's durable global configuration lives (profiles, `[phone]`). mj arms review the same way. Two keys do not meaningfully complicate configuration.
  Date/Author: 2026-09-01 / jbellis.
- Decision: `/review` (bare) is the one-off manual trigger and `/review status` reports state; both work from the terminal and the phone. `/review on|off|quick|extended` are still recognized but only print a notice naming the config keys. A per-session override of the global arming (mj has one, as runtime atomics) is explicitly deferred — noted, not built.
  Rationale: keeps both maintainer goals with the smallest command surface; the override is not in the stated goals and scope discipline says do not add it unasked.
  Date/Author: 2026-09-01 / jbellis + Fable.
- Decision: review phase reaches the surfaces through daemon state, not the relay journal: a `reviews` field on `RuntimeSnapshot` for the TUI (mirroring `lifecycles`) and a `turn_review` field on `ViewerSession` for the phone. The journal carries only controller-authored `RecordNotice` lines for resolutions ("review forwarded/dismissed/cancelled/…"), reusing the existing observation with no protocol bump. Durable state stays in the per-session `turn_review_state` SQLite row.
  Rationale: the journal is the protocol-versioned harness conversation replicated to the target; phase is ephemeral controller progress. `RuntimeLifecycleView` and `RecordNotice` are the established homes for each half. A synthetic elicitation for the verdict was considered and rejected: `pending_elicitations` is journal-derived, and faking a journal-shaped object from daemon state would split one channel's meaning.
  Date/Author: 2026-09-01 / Fable.
- Decision: the authoritative prompt refusal moves to the daemon's submit path. The TUI's composer refusal and the server's HTTP 400 remain as immediate-feedback mirrors, but the gate that cannot be bypassed or go stale is in the session manager's submit handling, next to the host that owns the review.
  Rationale: the shipped design had three enforcement points synchronized through a TUI-written DB row; single ownership removes the stale-lock class entirely.
  Date/Author: 2026-09-01 / Fable.
- Decision: automatic review requires a remembered per-workspace reviewer (`ReviewerDefaults`). When armed and no reviewer is remembered, the host emits a one-time-per-session `RecordNotice` telling the user to run `/review` once from the terminal (whose waterfall selection persists the default), and skips — loudly, not silently, and without a degraded reviewer-less mode.
  Rationale: the selection waterfall is a UI; the daemon cannot run one. The notice makes the unconfigured state visible in every surface's transcript, and one terminal round trip fixes it forever for that workspace. No fallback path.
  Date/Author: 2026-09-01 / Fable.
- Decision: the phone learns available slash commands from the server (`available_commands` on `ViewerSession`) instead of a JS constant, and `/review` dispatches as a typed action. Migrating the five existing web commands' dispatch to a fully shared parser is a follow-up, not this plan.
  Rationale: the hand-synced `HEL_COMMANDS` whitelist is how `/review` went missing (`src/web/viewer.js:1631` admits the design debt). Publishing the list kills discovery drift now; rewiring working dispatch for model/effort/fast/plan/implement is riskier than the bug it prevents and is bounded out.
  Date/Author: 2026-09-01 / jbellis + Fable.
- Decision: an in-flight review still does not survive a daemon restart (cancelled, notice recorded, baseline kept — the cumulative rule makes it lossless). But a review now *survives the TUI closing*: detaching or killing the terminal mid-review leaves the review running and resolvable from the phone. The `ActiveChat::Drop` cancellation (commit 2a11e18) is deleted with the rest of the TUI ownership.
  Rationale: the Drop cancellation existed only because the driver died with the view. Under daemon ownership the view's death is a non-event, which is the whole point.
  Date/Author: 2026-09-01 / Fable.

## Outcomes & Retrospective

To be written at each milestone.

## Context and Orientation

Hel is a Rust workspace (library code in `src/`, the CLI crate in `crates/hel-cli/`). One machine runs exactly one *controller daemon* (`hel daemon-run`, `run_daemon_process` at `crates/hel-cli/src/daemon.rs:2188`), which holds the machine-wide controller lock, owns the only SQLite writer (`submit_database_write`, `src/hel_database.rs:288`), runs one *session actor* per session (`run_session_actor`, `src/hel_session_manager.rs:1518`) that syncs that session's relay journal every 150 ms whether or not any UI is attached, and — when `[phone]` is enabled — hosts the *phone server* (`src/hel_server.rs`, spawned at `daemon.rs:2428`), an axum app serving the web viewer (`src/web/viewer.js`).

The *TUI* is a separate process. It runs no actors and never writes SQLite: it consumes daemon state through a 30-second long poll (`RuntimeState::runtime_snapshot`, `daemon.rs:699-756`, driven from `crates/hel-cli/src/pollers.rs:1240-1378`) and forwards every mutation over the daemon's TCP protocol (`forward_remote_session_request`, `pollers.rs:1399`). Its per-session view object is `ActiveChat` (`src/hel_chat/active.rs`), created on attach and dropped on detach.

*Turn review* as shipped by the prior plan: when a turn completes, a driver captures the cumulative git delta since the last-reviewed baseline, runs a cross-harness reviewer (quick tier: reviewer then validator; extended tier: intent analyst, supervisor, and specialist lanes over Bifrost tools), parses a verdict, and offers Forward / Dismiss / Cancel; the baseline (`turn_review_state` row: `baselines` tree ids, `reviewed_through_ordinal`, `prior_review`, `active`) advances only on resolution. The pure state machine is `TurnReviewDriver` (`src/hel_review/driver.rs`, deliberately transport-free); the reviewer processes are roles in the worker's `ReviewerSidecar` (`src/hel_worker_runtime/reviewer.rs`), driven via `ReviewerAction` (`src/hel_session_manager.rs:333`, dispatched daemon-side at `:2195-2255`).

The defect: everything around the pure driver is TUI-hosted. The trigger is the Running→Idle edge check in `ActiveChat::advance_turn_review` (`src/hel_chat/active.rs:824-832`); the `ReviewRequest` interpreter is `run_review_request` (`active.rs:887-990`) plus `launch_review_role` (`:1079-1106`), the role-journal poller (`:2315-2361`), and the lane-dispatch collector (`:2366-2379`); arming is a per-workspace SQLite row written by `/review on` (`src/hel_chat.rs:1433-1472` → `set_turn_review_settings`, `active.rs:1249-1283`); and the lock is the `turn_review_state.active` row, TUI-written, server-read (`src/hel_server.rs:1551`). The phone can be refused by that lock but has no review UI at all, and its command palette is a hardcoded JS list (`HEL_COMMANDS`, `src/web/viewer.js:1640-1648`) that does not know `review`.

## Plan of Work

### Milestone 1 — the daemon hosts the driver

Goal: automatic and manual review run in the daemon for every session; the terminal behaves exactly as before (pane opens on trigger, lock holds, Forward/Dismiss/Cancel work), but the TUI is now a projection plus action sender. Arming still reads the existing workspace setting in this milestone; the config switch is Milestone 3.

Create `src/hel_review/host.rs`: a `TurnReviewHost` owned by the daemon, holding at most one review per session (`TurnReviewDriver` plus its execution context: captured trees, rendered prompts, role bookkeeping). Wire it in `run_daemon_process` next to `spawn_session_manager` (`daemon.rs:2200`), subscribed to `SessionManagerUpdates` so it observes every session's published view. Port the interpreter from `ActiveChat` — `run_review_request`, `launch_review_role`, the role-journal poll loop (keeping its 200 ms empty-page backoff), and the lane-dispatch collector — replacing the chat context with the host's: requests execute via the in-process `ManagedSessionHandle` (`session.reviewer(...)` / `reviewer_as`), and all persistence goes through `submit_database_write` directly, since the host lives in the writer's process. Delete the TUI's `ChatPersistenceRequest::{SaveTurnReviewState, SaveTurnReviewSettings}` plumbing once nothing sends it (Milestone 4 removes the remains).

Trigger: the host watches each session's view for the Running→Idle phase edge. Gates, evaluated in the host: arming (this milestone: the existing workspace `turn_review_settings`; Milestone 3: config), queued prompts empty, no review already open for the session, the default reviewer role idle (mutual exclusion with a plan-review second opinion, checked via `ReviewerAction::Status` rather than any UI state), session not archived. The review seed (task, user messages after `reviewed_through_ordinal`, trajectory) is built from the daemon's materialized session, not from `ChatState` entries — port `turn_review_seed`'s filtering (`src/hel_chat.rs:913-974`) onto `MaterializedSession`.

Reviewer selection: the host requires a remembered `ReviewerDefaults` for the workspace. Absent one, it records a one-time-per-session `RecordNotice` ("automatic review is armed but this workspace has no reviewer; run /review in the terminal once to choose one") and skips. The manual path from the terminal still opens the waterfall (below).

Surfacing: add `reviews: Vec<RuntimeReviewView>` to `RuntimeSnapshot` (`daemon.rs:128-135`), built from the host on every `publish_revision`, mirroring how `lifecycles` is built and drained. The TUI drains it beside `drain_runtime_lifecycles` (`crates/hel-cli/src/dashboard.rs:1700`) and populates the existing `TurnReview` view state from it: the split pane, role strip, Tab cycling, and action bar in `src/hel_chat/turn_review.rs` all keep their rendering, but their inputs come from the drained view and their actions become daemon requests. Role transcripts in the pane keep using cursor-paged `ReviewerAction::Attach` reads; only the host acknowledges role journals, the TUI displays without acknowledging.

Actions: add `DaemonAction::StartTurnReview { session_id }` and `DaemonAction::ResolveTurnReview { session_id, resolution }` (resolution: forward, dismiss, cancel), validated against host state (forward requires a findings verdict, dismiss any verdict, cancel always — the same gating `can_forward()` enforces today). Bare `/review` in the terminal sends `StartTurnReview`; a "no reviewer configured" refusal makes the TUI open the waterfall (`ReviewerSetup`), persist the chosen default, and retry. Esc in the pane sends cancel. Forward's corrective prompt is submitted by the host (`PromptPrimary` already flows through `ChatRemoteOperation::Prompt`-equivalent submit on the handle).

The lock: the host sets `turn_review_state.active` when a review opens and clears it on resolution, as today, but the authoritative refusal moves into the daemon's submit path (`deliver_submit`, `src/hel_session_manager.rs:2057`): a prompt for a session with an unresolved review is rejected with the existing message. The TUI's composer refusal (`src/hel_chat.rs:1523-1529`) and the server's 400 (`src/hel_server.rs:1551`) stay as immediate feedback; neither is load-bearing anymore. Resolution lines ("review forwarded", "review dismissed", "review cancelled", "nothing to review", failure text) are recorded with `RecordNotice` so they appear in every surface's transcript; the TUI's local notice plumbing for these goes away.

Delete from the TUI: the trigger in `advance_turn_review`, the driver invocation and `run_review_request`, the `Drop` cancellation (`active.rs:2668-2691`), baseline initialization, and the restart-clears-active logic at `active.rs:633` (recovery moves to Milestone 4's daemon path). `TurnReviewDriver::start`'s only caller becomes the host.

Validation: the driver's own tests are untouched. New host tests with a hand-written fake session manager: the Running→Idle edge with the gates (queued prompts, busy reviewer role, already-open review) starts or refuses correctly; a review started headless (no TUI drain ever runs) reaches a verdict and resolves; `ResolveTurnReview` gating matches the verdict; the submit path refuses prompts while unresolved and accepts after. Terminal walk (tmux dev loop): behavior indistinguishable from before this milestone; additionally, detach the TUI mid-review, reattach, and find the review still running.

### Milestone 2 — the phone surface

Goal: a phone user sees a review happen and can resolve it; `/review` and `/review status` work from the phone composer. This closes the lockout.

Projection: add `turn_review: Option<ViewerTurnReview>` to `ViewerSession` (`src/hel_server.rs:393-461`), filled by the phone control loop from the same host views the snapshot carries (the phone server runs in the daemon process; `crates/hel-cli/src/server.rs:700-762` is where the snapshot is assembled). `ViewerTurnReview` carries tier, phase, the role strip (label + state per role), and — once a verdict exists — the verdict kind and the synthesis text bounded to a sane size (the full text is already bounded by the driver's own limits), plus which resolutions are currently allowed.

Card: in `viewer.js`, render a review card above the composer when `turn_review` is present, following the elicitation card's structure and diffing discipline (`buildElicitationCard`, `viewer.js:1191-1283`): while running, the role strip with states; on verdict, the synthesis and enabled buttons Forward / Dismiss / Cancel. Buttons post `{action: "resolve-review", session_id, resolution}`. While `turn_review` is present the composer is disabled with a banner naming the state ("a review of the last turn is open"), so the 400 refusal becomes a backstop rather than the UX.

Actions: add `resolve-review` and `start-review` to `validate_action` (`src/hel_server.rs:1628`) and `apply_phone_action` (`crates/hel-cli/src/server.rs:1444`), applied against the host. `start-review` refuses (with the notice text) when no reviewer is remembered for the workspace — choosing one stays a terminal task in this plan.

Commands: publish `available_commands` on `ViewerSession` — the server-side list of Hel commands valid for that session — and have `availableCommands()` (`viewer.js:1662`) read it instead of the `HEL_COMMANDS` constant, which is deleted. Dispatch for the five existing commands is unchanged; `review` dispatches to `start-review`, `review status` renders the state client-side from `turn_review` plus the armed indicator. The palette hint for `/review` includes `status` (also fix the terminal hint, `src/hel_chat/autocomplete.rs:283-287`, which omits it).

Validation: server unit tests for the new action validation (resolution gating mirrors the daemon's, unknown resolutions rejected); snapshot serialization test for `ViewerTurnReview`. Manual phone walk (Tailscale, per the dev loop): drive a session from the phone with a terminal also attached, watch the same review appear in both, resolve it from the phone, and watch the terminal pane close; then repeat with no terminal at all; then open a review in the terminal, SIGKILL the TUI, and resolve from the phone.

### Milestone 3 — global config arms it; the composer stops configuring

Goal: `[review] enabled = true` in `config.toml` is the one switch; a fresh session in any workspace gets reviewed with no per-workspace setup beyond the remembered reviewer.

Config: add `ReviewConfig { enabled: bool (default false), tier: ReviewTier (default quick) }` as `[review]` in `src/hel_config.rs`, beside `[phone]` (struct at the top, field on `Config` near `:749`, salvage entry near `:854`, validation rejecting unknown tiers). The host reads the daemon's refresher-installed controller config (at most 500 ms stale — `spawn_manager_target_refresher`, `daemon.rs:2357`) at each trigger decision; the manual path may load fresh per action, matching the daemon handlers' `Controller::load()` precedent.

Arming semantics: when armed and a session has no `turn_review_state` row, the host initializes baselines at session adoption (idempotently — capture current trees, store, coverage starts there), so the first completed turn after arming is reviewed rather than swallowed as coverage start. The composer title's `review quick` indicator now derives from config plus session state, via the drained runtime config the TUI already adopts (`drain_runtime_config`, `dashboard.rs:1758`).

Command reshape: `/review` bare and `/review status` keep working everywhere; `on`, `off`, `quick`, `extended` print a notice naming the config keys ("automatic review is configured in config.toml: [review] enabled, tier") and change nothing. `set_turn_review_settings`, the settings mirror in `ChatState`, and the off→on baseline capture in the TUI are deleted.

Migration: schema migration 20 drops `turn_review_settings`; delete `TurnReviewSettings`, its accessors (`src/hel_database.rs:2655-2747`), and the daemon persistence arm. No data is migrated: the feature is days old, and a workspace-to-global mapping has no defensible merge rule; the release note for the next release states that auto-review must be re-armed in `config.toml`.

Validation: config parse tests (defaults, salvage, bad tier rejected); a host test that an armed config plus a fresh session initializes baselines once and reviews the first turn; `/review on` produces the notice and leaves config untouched. Manual: arm in config, restart nothing (the refresher picks it up), drive a phone-only session, watch the review.

### Milestone 4 — recovery, removal, retrospective

On daemon start, the host clears any `turn_review_state.active` row, records the "review cancelled by restart" `RecordNotice`, and leaves baselines alone (the cumulative rule makes it lossless) — this replaces the TUI-attach recovery deleted in Milestone 1. Verify the worker's `pause_all` backstop still reaps roles when a session closes mid-review. Remove everything Milestone 1 orphaned (grep for the deleted symbols; `cargo clippy --all-targets -- -D warnings` as the tripwire). Update `.agents/docs/turn-review-mj-parity.md`: the hosting shape now matches mj (driver in the session-owning process; every UI a projection), and arming matches mj's global-config model; note the deliberate differences that remain (synchronous resolution with a human verdict decision; no correction rounds). Append the retrospective to this plan and a closing note to the prior plan.

## Concrete Steps

Work from the repository root (`/home/jonathan/Projects/hel3`). Build and test as the repository prescribes — default target `x86_64-unknown-linux-musl`; every `cargo test` outside the restricted sandbox with elevated permissions (loopback TCP and Unix sockets):

    cargo build
    cargo test
    cargo clippy --all-targets -- -D warnings

All three must pass at every milestone boundary. Commit each validated milestone on the current branch; stage only files you changed; never `git add -A`. Manual verification uses the maintainer's dev loop: the built controller in tmux for the terminal walks, and the Tailscale phone viewer for the web walks.

## Validation and Acceptance

Acceptance is behavioral:

1. With `[review] enabled = true` and a remembered reviewer, a session driven only from the phone reviews every code-changing turn: the card appears when the turn finishes, streams role states, shows the verdict, and Forward from the phone starts the corrective turn — which is itself reviewed.
2. The terminal experience is unchanged from the prior plan's acceptance: pane on trigger, lock during review, Forward/Dismiss/Cancel, cumulative baseline on cancel.
3. A review open in the terminal is simultaneously visible on the phone, and either surface can resolve it. Killing the TUI (SIGKILL) mid-review leaves the review running and resolvable from the phone; no surface is ever locked out with no path to unlock.
4. `/review` typed on the phone triggers a one-off review (never reaches the coding agent as text); `/review status` reports armed state, tier, and any open review, on both surfaces.
5. `/review on` on either surface changes nothing and names the config keys.
6. A daemon restart mid-review yields the cancellation notice in the transcript, a released lock, and an unchanged baseline; the next review covers the same changes.
7. With auto-review armed and no remembered reviewer, exactly one transcript notice per session says how to fix it, and nothing else happens.
8. `cargo test` and `cargo clippy --all-targets -- -D warnings` are clean.

## Idempotence and Recovery

All steps are additive until Milestone 3's migration, which is guarded by the schema-version mechanism; dropping `turn_review_settings` loses only the arming bit, deliberately. Re-running any milestone's steps is safe: the host initializes baselines idempotently, `RecordNotice` projection is idempotent by command id, and snapshot fields are recomputed on every publish. If the plan is abandoned after Milestone 1, the feature still works from the terminal exactly as before, and better (reviews survive detach); after Milestone 2 the phone is a full surface; Milestone 3 only moves the switch.

## Interfaces and Dependencies

No new crates and no new dependencies. New code lives in `src/hel_review/host.rs`, plus edits in `crates/hel-cli/src/daemon.rs`, `crates/hel-cli/src/server.rs`, `src/hel_server.rs`, `src/hel_config.rs`, `src/hel_chat/*`, `src/web/viewer.js`.

In `src/hel_review/host.rs`:

    pub struct TurnReviewHost { /* per-session drivers + contexts */ }
    impl TurnReviewHost {
        pub fn observe(&self, view: &ManagedSessionView);          // trigger edges + gates
        pub fn start(&self, session_id: &str, manual: bool) -> Result<(), StartRefusal>;
        pub fn resolve(&self, session_id: &str, resolution: ReviewResolution) -> Result<()>;
        pub fn views(&self) -> Vec<RuntimeReviewView>;             // for RuntimeSnapshot
        pub fn refuses_prompt(&self, session_id: &str) -> bool;    // authoritative lock
    }

    pub struct RuntimeReviewView {
        pub session_id: String,
        pub tier: ReviewTier,
        pub phase: TurnReviewPhase,          // reused from hel_review::driver
        pub roles: Vec<RoleStatus>,          // reused from hel_review::driver
        pub verdict: Option<VerdictView>,    // kind + bounded synthesis + allowed resolutions
        pub started_at_epoch_seconds: u64,
    }

In `src/hel_config.rs`:

    #[derive(Debug, Clone, Deserialize, Serialize)]
    #[serde(deny_unknown_fields)]
    pub struct ReviewConfig {
        #[serde(default)] pub enabled: bool,
        #[serde(default)] pub tier: ReviewTier,
    }

Daemon protocol: `DaemonAction::{StartTurnReview, ResolveTurnReview}`; `RuntimeSnapshot.reviews`. Phone protocol: `ViewerSession.{turn_review, available_commands}`; actions `start-review`, `resolve-review`. The daemon and CLI are one binary with a version-checked handshake, so these need no compatibility shims.

Revision note (2026-09-01): initial version, authored after the prior plan's implementation shipped and its first real use exposed the three surface defects recorded in `Context and Orientation`. The architecture was fixed in conversation with the maintainer on 2026-09-01 (daemon hosting, global-config arming with `/review` as one-off, phone as first-class surface) from research passes over hel's daemon/session-manager/phone topology and mjolnir's orchestrator hosting; all decisions and their evidence are in the Decision Log and Surprises & Discoveries.
