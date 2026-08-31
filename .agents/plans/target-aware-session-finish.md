# Make session finishing obvious and target-aware

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

Maintain this document in accordance with `.agents/PLANS.md` from the repository root.

## Purpose / Big Picture

After this change, a person who has finished working in a Hel session will not need to understand workers, checkpoint barriers, Podman process ownership, or the difference between an internal `Closing` and `Stopped` state. The running-session screen will offer one prominent **Finish** action. Before Hel acts, it will say exactly what will happen for that session's target: remove a local container, remove a container while preserving its remote host, terminate a Hel-created EC2 instance, or stop Hel's worker while preserving a user-selected bare project directory. Hel will save and verify recovery state first. The completed session will then appear under **Saved sessions**, where it consumes disk but no running compute and may either be resumed or deleted permanently.

Navigation will be equally explicit. Ctrl+G and the primary `/dashboard` command will say that they return to the dashboard while leaving the worker running. Quitting a dashboard that still has live sessions will require acknowledging that those sessions continue to run. The terminal dashboard and phone web viewer will use the same user-facing lifecycle terms and the same target-aware consequences.

The visible proof is a local Podman session whose chat can be left without stopping it, whose Finish confirmation says that its container will be removed, whose finish operation removes the container only after a verified recovery copy exists, and whose resulting **Saved** row can be resumed onto a fresh target. Equivalent behavior will be covered without requiring paid cloud resources for SSH Podman, raw SSH, EC2, Apple containers, and local bare projects.

## Progress

- [x] (2026-08-31 12:51Z) Read the current chat navigation, dashboard lifecycle, checkpoint/close, target teardown, saved-session, phone viewer, documentation, and test paths.
- [x] (2026-08-31 12:51Z) Resolved the first-version product model: one Finish intent, target-specific consequences derived from ownership already encoded by the target kind, and no new per-session policy choice.
- [x] (2026-08-31 13:57Z) Implemented the shared finish-effect model and distinguished a finish checkpoint from an ordinary recovery checkpoint. Focused checkpoint tests passed on the container's `aarch64-unknown-linux-gnu` host target.
- [x] (2026-08-31 14:34Z) Replaced the terminal UI's hidden Stop workflow with target-aware Finish, Saved, and Delete language, and made leave-running behavior explicit. The complete TUI suite and termination PTY tests pass.
- [x] (2026-08-31 14:53Z) Brought the phone viewer and its controller action schema to the same lifecycle model without exposing private target locators. Rust projection/server tests, script syntax checks, and targeted clippy pass; the real browser run is updated but cannot launch Chromium in this container because its system libraries require unavailable sudo access.
- [x] (2026-08-31 15:00Z) Published lifecycle guidance, updated behavioral and end-to-end tests, and ran the complete locally available validation suite. Rust, lint, docs, links, and script syntax pass; real browser launch and manual Podman acceptance remain environment-skipped for the reasons recorded below.
- [x] (2026-08-31 15:28Z) Reconciled the feature with `origin/master` at `53dffa5`, preserving the newer phone viewer's image-prompt and elicitation flows alongside target-aware Finish. The post-merge full Rust suite, formatting, Clippy, documentation build, link check, JavaScript parse, Python compile, and Playwright test discovery pass.
- [x] (2026-08-31 15:36Z) Fixed the CI reliability scenario to wait for both TUI projections to converge after a phone-initiated Finish before asserting immediate quit. The exact three-client scenario now passes locally with zero leaked processes.

## Surprises & Discoveries

- Observation: `/detach` and Ctrl+G currently produce the same `ChatAction::Back`; they return to the dashboard and leave the worker alive. The slash command is only a text route to navigation, not a lifecycle operation.
  Evidence: `src/hel_chat.rs` maps both `LocalCommand::Detach` and Ctrl+G to `ChatAction::Back`, and `crates/hel-cli/src/dashboard.rs::leave_chat` keeps the active chat warm.

- Observation: the safe teardown already exists, but it is hidden under a generic edit dialog.
  Evidence: the active-pane footer in `crates/hel-tui/src/render.rs::render_footer` advertises `[E]dit`; `crates/hel-tui/src/dialogs.rs::SessionEditDialog::actions` then exposes Stop, and only its following confirmation explains that recovery is verified before target destruction.

- Observation: target teardown is already operationally target-specific. Local and remote Podman remove only the session container, Apple containers are removed, EC2 instances are terminated, and bare targets stop Hel runtime state rather than deleting the selected project directory.
  Evidence: `src/hel_targets.rs::close_plan` contains one exact-resource command per `TargetLocator` variant. For SSH bare sessions, the locator's `workspace` is the Hel-owned session path produced by `workspace_for`; the user's selected `SessionRecord::project_directory` is separate and is not passed to `close_plan`.

- Observation: checkpoint submission already freezes the unstarted prompt/configuration queue while allowing an active effect to settle. The missing distinction is that an ordinary recovery checkpoint and the checkpoint held through close carry the same command shape, so the relay cannot reject brand-new work specifically after Finish has been requested.
  Evidence: `src/hel_worker.rs::promote_next_queued_command` refuses promotion whenever `pending_checkpoint_barrier` is true, including while the barrier waits behind an active prompt. `RelayCommand::BeginCheckpoint` currently carries only a free-form reason, and the relay accepts queue entries behind an ordinary active barrier.

- Observation: “archive” already means hide, not retain or copy. A stopped session's verified recovery file exists independently of its `archived` flag.
  Evidence: `src/hel_state.rs::SessionRecord::archived` documents that it is display-only, and `crates/hel-tui/src/resume.rs` filters hidden rows while separately loading checkpoint file sizes.

- Observation: the phone snapshot intentionally excludes SSH hosts, filesystem paths, AWS details, and concrete resource locators.
  Evidence: `src/hel_server.rs::ViewerSnapshot::from_config_state` documents this privacy boundary. Target-aware copy sent to the browser must therefore use a safe effect category, never raw locator data.

- Observation: this disposable container includes an aarch64 Rust toolchain but not the repository's default `x86_64-unknown-linux-musl` standard library.
  Evidence: the default focused test failed with “can't find crate for `core`”; the same tests pass with `--target aarch64-unknown-linux-gnu`, as prescribed for non-x86_64 hosts in `AGENTS.md`.

- Observation: a chat reducer knows only the currently open conversation, while the dashboard reducer owns the workspace-wide live-session count required by the quit warning.
  Evidence: `src/hel_chat.rs` emits `ChatEventOutcome::QuitDetach` without a workspace snapshot; `crates/hel-cli/src/dashboard.rs` receives that outcome, persists the draft, returns to the dashboard state, and can then call `DashboardState::request_quit` with the full session map.

- Observation: Playwright's pinned Chromium downloads successfully in this container, but the image omits its native ATK, DBus, GBM, XKB, ALSA, and accessibility libraries. Playwright's dependency installer invokes sudo, which this disposable environment does not authorize.
  Evidence: `tests/e2e/run-browser-reliability.sh --seed 31082026 target/aarch64-unknown-linux-gnu/debug/hel` reached Playwright and exited before browser synchronization with the native-dependency diagnostic. The artifact is under `target/reliability-artifacts/browser-tui-convergence-seed-31082026-34463`; Rust HTTP/projection tests and standalone JavaScript/Python syntax checks pass.

- Observation: Podman is not installed in this disposable container, so the optional manual exact-container acceptance run cannot be performed here.
  Evidence: `command -v podman` returns no executable. Fake target-plan tests and the browser reliability lab's fake bare target remain the deterministic coverage for target release in this environment.

- Observation: `origin/master` added phone-viewer image attachments and elicitation forms after this branch was opened, touching the same embedded viewer as target-aware Finish.
  Evidence: the merge conflicts were confined to `src/hel_server.rs` and the public exports in `src/hel_worker.rs`. The resolved viewer maps all three projections (`finish`, `pending_elicitations`, and `prompt_images_supported`), and all 39 server tests pass together.

- Observation: the deterministic three-client CI scenario treated the phone snapshot reaching Saved as if both TUI clients had already ingested that revision.
  Evidence: the failed artifact showed a valid database and completed Finish, while the second TUI received Ctrl+Q just before its terminal projection update and correctly opened the new live-session warning. Waiting for the finished title to disappear stably from both terminal screens makes the scenario prove three-client terminal convergence before testing immediate quit.

## Decision Log

- Decision: Use **Finish session** as the primary user action, not Stop, Close, Detach, or the bare adjective Done.
  Rationale: Finish describes the user's intent without implying that every target performs the same physical operation. “Done” is useful conversationally but ambiguous as a button or state; “Stop” incorrectly suggests a paused container; “Close” is implementation language.
  Date/Author: 2026-08-31 / Codex

- Decision: Keep one user intent but render a target-specific consequence and target-specific confirmation button.
  Rationale: a person should not choose teardown mechanics on every session. The target kind already records the ownership boundary. The modal can say **Remove container and save**, **Terminate instance and save**, or **Stop worker and save** while the top-level action remains Finish.
  Date/Author: 2026-08-31 / Codex

- Decision: Do not add an `on_finish` configuration field in this iteration.
  Rationale: every current target has a deterministic ownership contract. Hel creates and owns session containers and EC2 instances; it does not own a raw host or the bare project directory selected by the user. A configuration choice would push the same lifecycle burden back onto users and permit unsafe combinations. A future pooled-worker target should add its own effect category, such as releasing a lease, when that target kind exists.
  Date/Author: 2026-08-31 / Codex

- Decision: Preserve the existing controller teardown implementation and durable state names while changing user-facing boundary names.
  Rationale: `Controller::close_session`, `SessionState::Closing`, `SessionState::Stopped`, and `hel_targets::close_plan` already implement crash-safe checkpoint and teardown semantics. Renaming persisted states or the internal daemon protocol adds migration risk without improving the interface. Reducer and phone actions that express user intent may be named `Finish` and `DeleteSaved`; rendered states will say **Finishing** and **Saved**.
  Date/Author: 2026-08-31 / Codex

- Decision: Finish waits for the currently active effect to become terminal, prevents unstarted queued prompts and configuration changes from beginning, captures those queued entries in the recovery archive, and then tears down the target.
  Rationale: silently draining a potentially long queue contradicts “I am finished” and can continue consuming remote resources. Abruptly cancelling the active turn risks losing the work the user is trying to preserve. The deterministic default is to finish the one already-running effect and save everything not yet started for an optional later resume.
  Date/Author: 2026-08-31 / Codex

- Decision: Represent the distinction as `CheckpointPurpose::Recovery` versus `CheckpointPurpose::Finish`, not as a queue policy.
  Rationale: the existing relay already freezes unstarted queue entries for every pending checkpoint. Purpose is the actual missing state: Finish additionally rejects new effectful submissions so the exact close checkpoint cannot drift, while Recovery continues accepting work behind a temporary barrier.
  Date/Author: 2026-08-31 / Codex

- Decision: Once a Finish barrier is durable, reject every new relay command except the exact Close, checkpoint completion, or checkpoint release operations.
  Rationale: even relay-local queue edits and notices advance the event frontier and would invalidate the checkpoint cursor that Close must seal. Retries of commands accepted before Finish remain idempotent because duplicate-command handling runs before this admission gate.
  Date/Author: 2026-08-31 / Codex

- Decision: Keep saved sessions indefinitely by default, show that they use disk but no running compute, and rename the existing archive control to Hide.
  Rationale: a recovery archive may be the only copy of uncommitted work, so automatic destructive retention is unsafe as a first step. Users should not have to choose a deletion date when finishing. Showing per-session and total saved storage makes eventual cleanup an informed, non-urgent choice. Hide is honest about the existing display-only flag.
  Date/Author: 2026-08-31 / Codex

- Decision: Make `/dashboard` the advertised slash command and retain `/detach` as an unadvertised input alias during this user-interface change.
  Rationale: existing users and prompt history may still contain `/detach`, while new users should learn a navigation verb. The alias must have no separate behavior and should be covered by one compatibility test; it is not a second lifecycle mode.
  Date/Author: 2026-08-31 / Codex

- Decision: A quit request with live sessions must show a confirmation that counts the sessions which will keep running. It must not offer an implicit “finish all” operation.
  Rationale: the warning prevents accidental resource leaks. Finishing multiple independent sessions is destructive, slow, and failure-prone, and must remain a set of supervised per-session operations rather than hidden work on the terminal event loop.
  Date/Author: 2026-08-31 / Codex

- Decision: Handle Ctrl+Q from chat at the dashboard driver boundary: first persist the draft and leave chat, then either quit immediately or display the workspace-wide keep-running confirmation.
  Rationale: this preserves the chat reducer's narrow responsibility and ensures the warning counts every live session rather than only the conversation that happened to be open. No checkpoint or lifecycle operation is started by this navigation path.
  Date/Author: 2026-08-31 / Codex

- Decision: Partition phone cards into Active sessions and Saved sessions, and use a page-owned Finish dialog rather than the browser's native confirm prompt.
  Rationale: a native confirm prompt cannot display the target-specific primary action label carried by `ViewerFinish`. The page-owned dialog can present active-work, queued-work, and target-effect copy together, while local pending state immediately renders Finishing and disables duplicate actions before the next asynchronous snapshot arrives.
  Date/Author: 2026-08-31 / Codex

## Outcomes & Retrospective

Milestone 1 established the core contract without changing an existing UI action. All six live target locators now map to a privacy-safe `SessionFinishEffect` and target-specific copy. Relay protocol 7 carries `CheckpointPurpose::Finish`; older durable commands still decode as Recovery, and a close against an older live worker first replaces that worker. A Finish barrier waits for the active effect, preserves previously accepted queued prompts in the canonical archive projection, rejects new work, and resumes the queue if its controller disconnects. A database-backed lifecycle test also proves that failed Finish export leaves the target and Running state intact and never invokes the target teardown command.

The focused checkpoint run passed 120 tests (one ignored), including relay runtime and controller latch coverage. The four named Finish/recovery state-machine tests, archive projection test, target-effect test, backward-serde test, and failed-export lifecycle path pass on `aarch64-unknown-linux-gnu`.

Milestone 2 made Finish a direct Ctrl+F action with a consequence and primary button derived from the exact live target. Stop no longer appears in Edit. Chat advertises `/dashboard` and Ctrl+G as navigation that leaves the worker running, while `/detach` remains a hidden compatibility alias. Quit now starts on Cancel whenever live sessions would remain. Inactive rows live under Saved sessions, which explains that they run no workers and retain local disk; Hide and Delete permanently now describe their actual effects. The complete `hel-tui` suite passed 199 tests (one ignored), the focused CLI dashboard tests passed 16 tests, and both termination PTY tests passed on `aarch64-unknown-linux-gnu`. The remaining work is phone convergence, documentation, browser/end-to-end coverage, and repository-wide validation.

Milestone 3 added an optional, privacy-safe `ViewerFinish` projection and changed the public phone action atomically from `close` to `finish`; the daemon still delegates to its internal close implementation. Viewer state now renders Closing as Finishing and Stopped as Saved. Active and Saved cards are separate, never expose Finish and Resume together, and the Finish dialog uses the projected consequence and target-specific primary label. Six-locator projection coverage proves raw hosts, paths, container IDs, worker IDs, instance IDs, and addresses stay out of serialized snapshots. The 28 `hel_server` tests, 16 CLI server tests, JavaScript syntax check, Python compile check, and targeted all-target clippy pass. The browser/TUI reliability scenario now exercises Finish and Saved, but its local execution is deferred to an environment with Playwright's native libraries because this container cannot install them without sudo.

Milestone 4 added the human-facing Session lifecycle guide, linked it from the docs landing page and container guide, and updated the README's first-run and durability language. The browser/TUI reliability scenario now enters chat, proves Ctrl+G leaves the session Running, opens and cancels the live-session quit warning, then completes target-aware Finish and observes Saved before the phone resumes and finishes it again. The full host-target Rust run passed 1,723 tests with 9 environment-dependent tests ignored; `cargo fmt --check`, workspace all-target clippy with warnings denied, Astro check/build, 228-link validation, JavaScript syntax, and Python compilation all pass. The real Playwright scenario and optional manual Podman run are the only environment-skipped checks, with no product-test failure observed.

## Context and Orientation

Hel has three relevant layers. The `hel-core` package at the repository root owns durable session records, the relay between the controller and each coding harness, recovery checkpoints, and target provisioning/teardown. The `hel-tui` package in `crates/hel-tui` is a pure terminal state reducer and renderer: it turns keystrokes into `DashboardAction` values but performs no filesystem, process, or network work. The `hel-cli` package in `crates/hel-cli` runs the terminal event loop, the persistent local daemon, and the phone web control loop; it executes TUI actions in supervised background tasks.

A **session** is the logical conversation, queued work, repository state, and recovery metadata represented by `src/hel_state.rs::SessionRecord`. A **target** is where its live worker runs. `src/hel_config.rs::TargetTemplate` describes target configuration, while `src/hel_state.rs::TargetLocator` records the exact live resource created or selected for one session. A **worker** is Hel's target-side daemon which owns the coding harness and durable command relay. A **checkpoint** is a verified archive copied back before target teardown so the logical session can later resume on a fresh target.

The current user-visible state names expose the controller implementation. Active sessions appear in the main dashboard. Ctrl+G or `/detach` leaves the chat but keeps the worker. Ctrl+E opens an Edit session dialog, whose Stop action calls `Controller::close_session`. A successful close changes the durable state to `SessionState::Stopped`, clears the live locator, and moves the row into the Ctrl+S Resume dialog. The existing Destroy action then deletes the recovery file and session record. Ctrl+Q or Escape at the dashboard quits the terminal client and leaves workers alive without confirmation.

The close implementation in `src/hel_controller/lifecycle.rs` is intentionally conservative. It persists the close intent, waits for a relay checkpoint barrier, exports and verifies the recovery archive, asks the harness to close at that exact archived event cursor, and only then calls `src/hel_targets.rs::close_plan`. A checkpoint failure restores the prior live state and refuses teardown. A later force-stop path is available only if an older verified archive exists and may lose recent work. These invariants must remain intact.

The target effects which the new interface must describe are:

- `LocalPodman`: remove the exact Hel-created local Podman session container and its per-session Git cache snapshot. The host and other containers remain.
- `AppleContainer`: remove the exact Hel-created Apple container and its per-session Git cache snapshot. The host and other containers remain.
- `SshPodman`: connect to the configured SSH host, remove the exact Hel-created Podman session container and its per-session Git cache snapshot, and preserve the host.
- `AwsEc2`: terminate the exact Hel-created EC2 session instance.
- `LocalBare`: stop the exact Hel worker process group and remove its Hel runtime root. Preserve the selected user project directory.
- `SshBare`: stop the exact remote Hel worker and remove Hel-owned per-session runtime paths. Preserve the SSH host and the user-selected remote project directory.

The term **finish effect** in this plan means that safe, privacy-preserving classification. It does not contain an SSH host, path, container identifier, or instance identifier. The term **Saved** means the rendered name for the existing inactive `SessionState::Stopped`; it does not introduce another durable state or database migration. A saved session has no live locator or worker, keeps one verified recovery archive, and can be resumed. The term **Hide** means toggling the existing `SessionRecord::archived` display flag; it does not free disk.

The phone UI is an HTML/JavaScript page embedded in `src/hel_server.rs`. `ViewerSnapshot` is its privacy-filtered JSON model, and `ControllerAction` is the accepted action schema. `crates/hel-cli/src/server.rs` receives those actions and delegates long work to the daemon. Phone actions return “accepted” immediately and converge through later snapshots, so Finish must continue using that asynchronous model.

## Plan of Work

### Milestone 1: encode finish semantics and seal new work

First make the core behavior precise without changing a button. In `src/hel_controller/lifecycle.rs`, add a public, copyable `SessionFinishEffect` enum with the six current effects: stopping a local bare worker, removing a local Podman container, removing an Apple container, stopping a remote bare worker, removing a remote Podman container, and terminating an EC2 instance. Export it from `src/hel_controller.rs`. Add `session_finish_effect(session: &SessionRecord) -> Result<SessionFinishEffect>`, which requires an active `session.target` and classifies the canonical locator. Give the enum privacy-safe methods returning the confirmation consequence and primary action label. Copy must name what is preserved as well as what is released; for example, the SSH Podman consequence says the session container will be removed and the SSH host will remain.

Do not place raw locator fields in these strings. Add unit tests that construct all locator variants and assert meaningful behavior: a local bare effect mentions preserving the project, an SSH effect mentions preserving the host, and EC2 says terminate. Strengthen existing `src/hel_targets/tests.rs` behavior tests so every effect remains paired with the actual teardown plan. Those tests must inspect commands produced for fake exact identifiers; they must never execute real Podman, SSH, Apple container, or AWS cleanup.

Next make the finish intent explicit at the relay. In `src/hel_worker/snapshot.rs`, extend `RelayCommand::BeginCheckpoint` with a serializable `CheckpointPurpose` whose values are `Recovery` and `Finish`. Existing periodic/manual checkpoints use Recovery. The checkpoint held through close in `src/hel_controller/lifecycle.rs` uses Finish. Bump the relay protocol version and make a Finish command require that version, because an older worker cannot safely interpret the new purpose.

Keep the existing queue-freezing behavior in `src/hel_worker.rs::promote_next_queued_command` and add a `finish_checkpoint_pending` predicate over nonterminal Finish barriers. Once such a barrier has been accepted, reject new prompts, user shells, and configuration or mode changes with a clear “session is finishing” invalid-state error; never allow post-finish work to drift past the exact checkpoint cut. The active effect may finish, and the materialized checkpoint must still contain queue entries accepted before Finish. If the controller connection disappears or the lifecycle operation is cancelled before the barrier completes, `cancel_checkpoint_barrier_on_disconnect` must interrupt either purpose and resume normal promotion. Ordinary Recovery barriers must continue accepting work behind the barrier and retain their existing command ordering after release.

Add state-machine tests named along the lines of `finish_checkpoint_waits_for_active_prompt_and_preserves_queue`, `finish_checkpoint_rejects_new_work`, `cancelled_finish_checkpoint_resumes_the_queue`, and `recovery_checkpoint_still_accepts_work_behind_the_barrier`. Add a controller checkpoint test proving the canonical archive contains a preserved queued prompt and a lifecycle test proving no target teardown is attempted when the finish checkpoint fails.

This milestone is complete when core tests show that Finish has one safe target effect, does not leak locator details into its presentation, allows a current effect to settle, preserves all unstarted work, and retains the existing verify-before-teardown invariant.

### Milestone 2: make the terminal lifecycle self-explanatory

Change the TUI boundary vocabulary while retaining internal controller names. In `crates/hel-tui/src/lib.rs`, replace the user-intent `DashboardAction::Close` variant with `DashboardAction::Finish` and `DashboardAction::DestroyStopped` with `DashboardAction::DeleteSaved`. Rename `SessionOperationKind::Stopping` to `Finishing` so the in-flight row reads **Finishing 12s**. Keep `DashboardAction::ForceStop` as the advanced recovery escape hatch, but render it as **Force finish** and continue to state that recent work may be lost.

Remove Stop from `SessionEditDialog`; that dialog should contain only Rename, optional Container settings, and Cancel. In `DashboardState::handle_dashboard_key`, bind Ctrl+F on an active-session row to a new target-aware Finish confirmation. The active-pane footer in `crates/hel-tui/src/render.rs::render_footer` must advertise `[F]inish` directly beside New and Saved sessions, while Ctrl+E remains Edit. The Finish modal obtains `SessionFinishEffect` from the selected live session and renders all of the following: the session name, “Hel will finish the current work, save and verify recovery, and preserve N queued items for resume” when applicable, the target-specific consequence, and a target-specific primary button. Opening the modal is pure TUI work; checkpointing and teardown continue only after `crates/hel-cli/src/dashboard/actions.rs` receives `DashboardAction::Finish` and starts its existing supervised lifecycle task.

On successful completion, `crates/hel-cli/src/dashboard/io.rs` must show a durable notice such as “Session saved; local Podman container removed. Open Saved sessions with Ctrl+S.” Failure must keep the row active and show the existing retry/force-finish choices with target-specific wording. Cancellation through Ctrl+X must restore the running row and allow its preserved queue to continue.

Change the chat command surface in `src/hel_chat/autocomplete.rs` and `src/hel_chat.rs`: advertise `/dashboard`, describe it as “return to the dashboard; the worker keeps running,” and parse it to `ChatAction::Back`. Keep `/detach` as an unadvertised synonym mapping to the same action. Update the chat footer in `src/hel_chat/active.rs` to say “Ctrl-G dashboard · worker keeps running” at normal terminal widths, with a compact but still explicit form at narrow widths. Ctrl+G must continue preserving an unsent composer draft.

Add a quit confirmation state to `crates/hel-tui`. Ctrl+Q from chat or Ctrl+Q/Escape from the dashboard should quit immediately only when this workspace has no live session. Otherwise return to or overlay the dashboard with “Quit Hel? N sessions will keep running.” The primary destructive-looking choice is not allowed; use **Cancel** as the initial focus and **Quit, keep running** as the explicit second button. Confirming still emits the existing `QuitDetach` outcome and must not start checkpoints or block the render loop. Update `crates/hel-cli/tests/termination_pty.rs` and reliability helpers so automated quitting explicitly confirms when their fixtures contain live sessions.

Rename the Ctrl+S surface in rendered copy from Resume to **Saved sessions** without renaming the internal `ResumeDialog` module. The main footer should say `[S]aved`; the dialog title and empty state should say **Saved sessions** and **No saved Hel sessions**. Render `SessionState::Stopped` as **saved** at user-facing boundaries. Change `a archives` to `a hides`, `s shows archived` to `s shows hidden`, and render the flag as `[hidden]`. Show a one-line explanation and the sum of loaded checkpoint sizes: “Saved sessions run no workers · 1.2 GiB stored locally.” Retain per-row archive sizes. Change the permanent action and confirmation from Destroy to **Delete permanently**, explicitly saying that the verified recovery archive and logical session will be removed and cannot be resumed.

Add reducer and renderer behavior tests. They must prove Ctrl+F opens the correct consequence for each effect category, Stop is absent from Edit, cancelling Finish performs no action, confirming produces one `DashboardAction::Finish`, Ctrl+G never produces Finish, a quit with a live session warns while a quit with none exits, Saved copy explains zero live compute and disk retention, and Delete is unavailable for active sessions.

This milestone is complete when a new user can discover Finish from the dashboard footer, understand its exact consequence before confirming, and see where the saved session went afterward without knowing the prior Stop/Resume/Destroy vocabulary.

### Milestone 3: give the phone viewer the same contract

Extend the privacy-filtered projection in `src/hel_server.rs` with a `ViewerFinish` object on active `ViewerSession` rows. It contains only a kebab-case effect category, the privacy-safe consequence, and the primary action label derived from `SessionFinishEffect`. It must not include host names, paths, container IDs, instance IDs, or raw errors. Saved rows omit this object.

Rename `ControllerAction::Close` to `ControllerAction::Finish`, changing the bundled page request from `{"action":"close"}` to `{"action":"finish"}`. This page and server ship in the same binary, so update the schema directly rather than keeping two public verbs. In `crates/hel-cli/src/server.rs`, Finish continues to call the internal `daemon_runtime.close_session`; internal mechanics remain named close. Update action validation so only active sessions accept Finish, while only saved sessions offer Resume. The phone surface must continue excluding force finish and permanent deletion because its existing security boundary deliberately omits destructive recovery actions.

Refactor the embedded JavaScript card rendering so active and saved rows are visibly separate. An active row offers Open and Finish; a saved row says **Saved · no worker running** and offers Resume. Do not render Resume and Finish together. Clicking Finish opens a target-aware confirmation using `ViewerFinish`, including the active-work and queued-count behavior. After the action is accepted, render **Finishing** and disable duplicate lifecycle actions until a later snapshot moves the row to Saved or reports attention needed.

Update `src/hel_server.rs` request/response tests and `tests/e2e/web/reliability.spec.js`. Tests must prove the snapshot does not leak a fixture host, project path, container ID, or EC2 instance ID; the browser renders the right consequence for remote Podman and EC2 categories; a saved row has no Finish action; and action acceptance remains asynchronous and converges through snapshots.

This milestone is complete when terminal and phone clients use the same lifecycle language and effect classification while the phone retains its stricter privacy and destructive-action boundary.

### Milestone 4: document and exercise the complete lifecycle

Add `docs/src/content/docs/session-lifecycle.md` as human-facing documentation and link it from `docs/src/content/docs/index.mdx` and the relevant container page. Begin with the ordinary workflow, not internals: Back leaves work running; Finish completes current work, saves queued work, verifies recovery, and releases the target; Saved uses disk but no live compute; Resume provisions a new target; Hide only organizes the list; Delete permanently removes recovery. Include one concise target-effect list and a warning that quit is not Finish. Explain that users do not need to choose a retention time immediately because saved sessions remain recoverable and incur no worker cost, while the Saved screen shows local disk usage.

Update `README.md` to use Finish and Saved in user-facing prose while retaining internal function names in code documentation. Update `docs/src/content/docs/containers.md` from “Closing a session” to “Finishing a session” and describe the remote-host preservation boundary.

Extend the deterministic PTY and fake-target tests rather than requiring real infrastructure in CI. A PTY scenario should open a chat, use Ctrl+G, verify the dashboard still reports the session running, open Finish, assert the target-specific text, confirm, observe Finishing, and finally find the row under Saved. Backend fake-executor tests prove the exact teardown commands for every target kind. Update the browser lab's current “Stop session?” expectation to Finish and prove terminal/web convergence.

Perform one optional manual local Podman acceptance run when Podman is available. Record the exact generated Hel container before confirmation, confirm Finish, verify only that container disappears, open Saved sessions, resume onto a newly generated container, and finish it again for cleanup. Do not make AWS, Apple container, or a remote SSH host prerequisites for acceptance; their effects are proven through pure classification, fake command execution, and web/TUI behavior tests.

This milestone is complete when product documentation, focused tests, the full Rust suite, linting, docs build, PTY behavior, and web behavior all describe and prove the same lifecycle.

## Concrete Steps

Run all commands from `/workspace/hel` unless a step explicitly changes directory. At the start of each milestone, inspect the working tree so unrelated user changes remain unstaged:

    git status --short

After Milestone 1, format and run the focused core tests:

    cargo fmt --all
    cargo test -p hel-core finish_effect
    cargo test -p hel-core finish_checkpoint
    cargo test -p hel-core lifecycle
    cargo test -p hel-core hel_targets

The expected evidence is that the named finish tests pass, a Finish barrier leaves pre-existing queued command IDs in the snapshot/archive and rejects new work, a cancelled barrier permits the next queued command to start, and fake target plans contain the exact target-specific teardown command without executing it.

After Milestone 2, run the TUI, CLI, and PTY behavior tests:

    cargo test -p hel-tui finish
    cargo test -p hel-tui saved
    cargo test -p hel-tui quit
    cargo test -p hel-cli dashboard
    cargo test -p hel-cli --test termination_pty

Expected rendered fragments include:

    Ctrl for: [N]ew · [S]aved · [F]inish
    Ctrl-G dashboard · worker keeps running
    Finish session?
    Saved sessions run no workers
    Delete saved session permanently?

After Milestone 3, run the phone projection and server tests, then the browser suite using its pinned package:

    cargo test -p hel-core hel_server
    cargo test -p hel-cli server
    cd tests/e2e/web
    npm ci
    npm test
    cd ../../..

The browser suite should observe an accepted Finish action, a later Finishing state, and eventual Saved state. Snapshot tests must search the serialized JSON for fixture secrets and find none.

After Milestone 4, build the human documentation:

    cd docs
    npm ci
    npm run check
    npm run build
    cd ..

Finish with the repository-wide checks required by `AGENTS.md`. Cargo tests need unrestricted loopback TCP and Unix socket access; a sandboxed `EPERM` or hang is not a valid result.

    cargo fmt --all -- --check
    cargo test
    cargo clippy --all-targets -- -D warnings
    git status --short

At each coherent, validated milestone, update this ExecPlan's Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective sections, append a revision note, stage only files changed for that milestone, and commit on the current branch. Do not use `git add -A` and do not push unless explicitly requested.

## Validation and Acceptance

Acceptance is behavioral and consists of all of the following observations.

In chat, the footer and `/dashboard` autocomplete state that the worker remains running. Ctrl+G returns to the dashboard, preserves a half-written draft, and causes no checkpoint or target command. The hidden `/detach` alias does exactly the same thing.

On a selected active session, Ctrl+F is directly discoverable. The confirmation describes one and only one exact effect appropriate to the live locator. Remote effects explicitly preserve their host; bare effects explicitly preserve the selected project; EC2 explicitly terminates the session instance. No confirmation or phone payload prints a private host, path, container ID, or instance ID.

When Finish is requested during a running turn, that turn becomes terminal before the checkpoint barrier is admitted, no queued prompt or queued configuration change starts, and the resulting archive retains the pre-existing queue. A new prompt submitted after the Finish barrier is accepted is rejected with a clear Finishing message; it must never enter the archived queue or run ahead of the finish checkpoint. Cancelling before the exact close is sealed releases the barrier and permits normal work to resume. A failed checkpoint leaves the target present. A successful checkpoint is verified before the target-specific teardown command runs.

After success, the active row disappears only because the same logical session appears under **Saved sessions**. The UI says no worker is running, shows its stored archive size when available, and offers Resume. Resume provisions a fresh disposable target or reconnects a bare project according to the existing resume rules. Hide removes the row from the default list without changing its file or lifecycle state. Delete permanently removes the archive and record only after confirmation.

Quitting with any live session names how many workers will remain. Cancelling the dialog leaves the UI and workers unchanged; explicitly choosing Quit exits promptly and leaves all workers unchanged. Quitting with no live sessions remains prompt.

The phone viewer distinguishes active and saved rows, uses Finish rather than Stop/Close, and never offers Finish for a saved row or Resume for an active row. Its action request is acknowledged without holding the HTTP connection through teardown, and later snapshots converge with the terminal dashboard.

All focused and full validation commands in Concrete Steps pass. A local Podman manual run, when available, proves that Finish removes the exact session container and not the host or unrelated containers. The absence of real AWS or SSH credentials does not weaken acceptance because fake command executors and effect-rendering tests cover those branches without external side effects.

## Idempotence and Recovery

The plan intentionally avoids a database migration. Existing `SessionState::Stopped`, `SessionRecord::archived`, checkpoints, and daemon close commands remain readable. Re-running formatters and tests is safe. The phone action rename is atomic because the HTML page and HTTP action enum are compiled into the same binary.

Finish remains retryable through the current durable Closing recovery. The controller persists intent before checkpointing, verifies the installed archive before cleanup, and uses exact, idempotent target identifiers. Preserve those properties. If an implementation fails after the checkpoint but before teardown, let the existing interrupted-close recovery finish it; do not mark the session Saved until target absence is confirmed. If it fails before a verified checkpoint, restore the prior active state and do not tear anything down.

A cancelled Finish checkpoint must be explicitly covered because a stuck barrier would pause a remote queue indefinitely. The connection-drop interruption path is the recovery mechanism; tests must simulate cancellation while the barrier is queued behind active work and while it is active.

Never test destructive target commands against an unresolved environment variable, broad path, arbitrary Podman list, or real cloud account. Use fake executors and generated fixture IDs. For the optional manual Podman run, record the exact session container from Hel state before acting and stop it through Hel. If manual cleanup is necessary after a product failure, use `hel recover scan` to identify the exact Hel-managed resource before any destroy command.

If the new terminology causes an unforeseen regression, the UI commits can be reverted independently without reverting the finish-purpose checkpoint or target-effect tests. Do not revert checkpoint safety to make copy changes pass.

## Artifacts and Notes

The intended terminal flow is:

    Running session
      Ctrl+G  -> Dashboard; worker keeps running
      Ctrl+F  -> Finish confirmation
                  current effect settles
                  queued work is preserved
                  recovery is verified
                  target-specific resource is released
               -> Saved session; no worker running
                    Enter -> Resume on a fresh target
                    a     -> Hide from the default list
                    d     -> Delete recovery permanently

The effect copy should remain short enough for the TUI and precise enough to stand alone. Representative wording is:

    Local Podman: The Hel session container will be removed. This computer and other containers remain unchanged.
    SSH Podman: The Hel session container will be removed from the remote host. The host and other containers remain unchanged.
    EC2: The Hel-created EC2 session instance will be terminated.
    SSH bare: The remote Hel worker will stop and Hel runtime files will be removed. The remote host and selected project directory remain unchanged.

The success notice must couple resource release to recoverability:

    Session saved; remote Podman container removed. Open Saved sessions with Ctrl+S.

Do not use “archived” as a synonym for Saved. In this repository it already names both a verified recovery file in some internal comments and a display-only hidden flag, which is a source of the current confusion.

## Interfaces and Dependencies

No new crate or third-party dependency is required.

In `src/hel_controller/lifecycle.rs`, define and export through `src/hel_controller.rs`:

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum SessionFinishEffect {
        StopLocalBareWorker,
        RemoveLocalPodmanContainer,
        RemoveAppleContainer,
        StopRemoteBareWorker,
        RemoveRemotePodmanContainer,
        TerminateAwsEc2Instance,
    }

    pub fn session_finish_effect(session: &SessionRecord) -> Result<SessionFinishEffect>;

`SessionFinishEffect` must expose privacy-safe consequence and primary-action methods. If implementation experience shows that a small `SessionFinishPresentation` value is clearer than two methods, record that change in the Decision Log; do not duplicate six independent target matches across TUI and web code.

In `src/hel_worker/snapshot.rs`, define:

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum CheckpointPurpose {
        Recovery,
        Finish,
    }

and carry it on `RelayCommand::BeginCheckpoint`. Give the purpose a default of Recovery and use `#[serde(default)]` on the field so existing durable journal and snapshot records still decode. Finish is used only for the checkpoint held through a close. It seals new effectful submissions while reusing the existing queue freeze; queued commands remain first-class durable state.

At the TUI boundary, the resulting action variants are:

    DashboardAction::Finish { session_id: String }
    DashboardAction::DeleteSaved { session_id: String }

The driver maps Finish to the existing daemon `close_session` call and DeleteSaved to `destroy_stopped_session`. The daemon and controller method names remain internal and unchanged.

At the phone boundary, add a privacy-safe projection:

    pub struct ViewerFinish {
        pub kind: String,
        pub consequence: String,
        pub primary_action: String,
    }

and make `ViewerSession::finish` optional. The controller action is:

    ControllerAction::Finish { session_id: String }

The serialized action verb is `finish`. Do not serialize or interpolate any field from `TargetLocator` into `ViewerFinish`.

Revision note (2026-08-31): Initial plan created after tracing the current detach, stop, destroy, queue, target-teardown, saved-session, and phone-viewer paths. It resolves the design around a single Finish intent with target-aware effects, rather than a uniform physical teardown or a new policy choice for every user.

Revision note (2026-08-31): Milestone 1 source inspection corrected the queue design. Pending checkpoint barriers already freeze unstarted queue entries; the plan now adds a Finish purpose solely to reject new work and protect the exact checkpoint cut.

Revision note (2026-08-31): Milestone 1 completed with protocol 7, backward-compatible Recovery decoding, automatic worker replacement for old Finish peers, privacy-safe target effects, canonical queue preservation, cancellation recovery, and verify-before-teardown coverage.

Revision note (2026-08-31): Milestone 2 completed with target-aware terminal Finish, explicit leave-running navigation and quit behavior, Saved/Hide/Delete vocabulary, and passing TUI, CLI dashboard, and PTY coverage.

Revision note (2026-08-31): Milestone 3 completed the privacy-safe phone projection, Finish action schema, Active/Saved rendering, target-aware web confirmation, and asynchronous Finishing state; browser reliability coverage was updated but cannot launch locally without privileged system-library installation.

Revision note (2026-08-31): Milestone 4 published the lifecycle documentation, extended the browser/TUI reliability flow through Dashboard, quit warning, Finish, and Saved, and completed all locally available full-suite validation.

Revision note (2026-08-31): Reconciled the completed feature with `origin/master` at `53dffa5`. The resolution retains both lifecycle Finish and the newer image/elicitation viewer capabilities; the merged tree passes 1,757 Rust tests with 9 environment or measurement tests ignored, plus formatting, Clippy, docs, links, and script checks.

Revision note (2026-08-31): GitHub's three-client smoke test exposed a projection race in the test sequence, not a lifecycle failure: one TUI was asked to quit before it had observed the phone-initiated terminal revision. The harness now waits for both terminal screens to remove the finished session, records that convergence, and then verifies both dashboards quit promptly.
