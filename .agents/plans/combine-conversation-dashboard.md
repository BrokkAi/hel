# Combine the conversation and the dashboard into one TUI surface

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

The canonical rules for ExecPlans live in `.agents/PLANS.md`, relative to the repository root. This document must be maintained in accordance with that file.

## Purpose / Big Picture

Today Hel's terminal UI has two screens. The first is a *dashboard*: a list of live agent sessions, a table of machines that can host sessions ("Capacity"), and a table of per-account usage limits ("Profile Quotas"). Pressing Enter on a session leaves the dashboard and opens the second screen, a *chat*: the conversation transcript for that one session with a text composer under it. `Ctrl-G` goes back. The two screens never appear at the same time, so watching a running agent means losing sight of every other session, and checking capacity means losing sight of the conversation you were reading.

After this change there is one screen. From top to bottom it shows: a **Sessions** pane, the **transcript** of the session you are talking to, the **Prompt** composer, a **Targets** pane (formerly "Capacity"), a **Quota** pane, and a one-row footer. Nothing is hidden behind a screen switch. `Ctrl-G` no longer means "go back"; it collapses Targets and Quota into single summary rows (and keeps Sessions in its compact form) so the transcript gets almost the whole screen, and pressing it again brings them back.

You can see it working like this. Run `hel`, pick a workspace, and the surface opens with the conversation that had the most recent agent activity already loaded and the cursor in Prompt — you can type a prompt as the first thing you do. Press `Tab` and focus moves to Sessions, which expands to show every project's sessions in full. Press `Tab` twice more and you are on Targets, then Prompt again. Press `Ctrl-G` and Targets and Quota shrink to one row each, reading something like `Targets  workstation 42%  builder 7%` and `Quota  claude-1 37%  codex-1 8%`. Press `Ctrl-G` again and the full tables return. At no point does the transcript disappear.

The user-visible payoff: you can read an agent's output, see what your other agents are doing, see whether a machine is saturated, and see how much weekly quota is left, without navigating anywhere.

## Progress

- [x] (2026-08-31) Researched the current split: `crates/hel-tui/src/lib.rs` (dashboard reducer), `crates/hel-tui/src/render.rs` (dashboard rendering), `src/hel_chat.rs` and `src/hel_chat/active.rs` (chat state and rendering), `crates/hel-cli/src/dashboard.rs` (the one event loop and the `View` switch). Wrote this ExecPlan.
- [x] (2026-08-31) Milestone 1 — Area-scoped chat rendering. `FrameSurfaces::append`/`replace_with` in `src/hel_selection.rs`; `ChatRegions` and `ChatState::frame_surfaces_exclusive` in `src/hel_chat.rs`; `render_in`, `render_chat_footer`, `ActiveChat::draw_in` and `ActiveChat::desired_prompt_height` in `src/hel_chat/active.rs`. The whole-frame `render` is now a thin caller of `render_in`, so the old two-screen behaviour is unchanged.
- [x] (2026-08-31) Milestone 2 — Combined focus model, minimize state, and the keymap. `Focus` is now public with four stops and `FOCUS_ORDER`; `DashboardState` gained `support_minimized`, `collapsed_project_keys`, `current_session_id` and `selected_session_id`, and lost `session_index` and `expanded_project_key`. `ChatEventOutcome` lost `Back` and `SwitchSession` and gained `CycleFocus`, `ToggleSupportPanes` and `OpenWebDialog`.
- [x] (2026-08-31) Milestone 3 — Sessions pane projection and rendering (completed: `SessionsRow`, `sessions_rows`, `current_project_key`, `visible_session_indices`, and a rewritten `render_sessions` drawing compact one-line, collapsed one-line, and four-row expanded forms; remaining: the behaviour tests for the five-session threshold and the Others aggregation, which land next).
- [x] (2026-08-31) Milestone 4 — The combined renderer. `crates/hel-tui/src/combined.rs` holds `render_combined` and `allocate_combined_heights`; `render.rs` gained `minimized_targets_line`, `minimized_quota_line` and `combined_footer_text`, and lost the old three-pane `allocate_pane_heights` and `render_adaptive_dashboard`. Pane titles now read Sessions, Targets and Quota.
- [x] (2026-08-31) Milestone 5 — Controller integration (completed: `View` deleted, one draw and one hitbox registry, mouse routed by pointer and keys by focus, F2 intercept, startup selection with its two-second bound and its user-input cancellation, the outgoing conversation saved on every switch; remaining: the controller behaviour tests, which land with the Milestone 3 tests).
- [x] (2026-08-31) Milestone 6 — Documentation and test harnesses. README gained a `The terminal surface` section and a corrected Quickstart; `docs/src/content/docs/containers.md` names the new first-session keys; `.agents/docs/parallel-luna-testing.md` describes the one surface, the plain pane keys, the Tab-first requirement, and that Escape no longer quits; `crates/hel-cli/tests/termination_pty.rs` waits for `Sessions` and quits with Ctrl-Q; the three Python labs wait for `Sessions` and `browser_lab.py` tabs to the pane before pressing `e`; the module docs of `crates/hel-tui/src/lib.rs` and `src/hel_chat/active.rs` describe the combined surface.
- [x] (2026-08-31) Behaviour tests. Sessions: the five-session threshold, the Others aggregation with its exact active/idle split, the current project following the conversation, the focused list ignoring the threshold, default-expanded and independent collapse, and the selection surviving the list changing under it. Rendering: the four-row expanded form, the compact row's project/target/clock/last-line, the minimized rows' CPU and weekly-percent-used, their explicit unavailable/refreshing/stale readings, and their truncation. Startup: the newest-activity pick, the creation-then-id fallback, the bounded wait, and the user taking the choice back.
- [x] (2026-08-31) Verification. The real binary was driven in a PTY at 140x32 and 60x20: the six bands draw in order, Tab walks Sessions to Quota with the footer following, Ctrl-G collapses Targets and Quota to `Targets  local 17%` and `Quota  claude-1 unavailable codex-1 unavailable` and hands the transcript the rows they freed, Tab restores them onto Sessions, 15 rows reports `Increase height to at least 16 rows`. The band order and the collapse arithmetic are now also render tests, so they cannot regress silently.

## Outstanding

A workspace with live sessions has been verified through the render tests
(compact rows, the five-session threshold, the Others aggregation, the
four-row expanded form, independent collapse) but not yet by hand against a
running agent, which needs a provisioned container target and harness
credentials. That is the one check from the original test plan that automation
here could not stand in for.

## Surprises & Discoveries

- Observation: the startup fallback ranked sessions backwards. `compare_by_creation` orders oldest first, and the comparator inverted it, so a workspace whose summaries had not loaded would have opened its *oldest* session rather than its newest. The test written for the documented behaviour caught it.
  Evidence: `startup_falls_back_to_the_newest_creation_then_the_larger_id` failed with `left: Some("session-a")` against `right: Some("session-b")` before the comparator was corrected to `left.compare_by_creation(right)`.

- Observation: Milestones 2 and 5 could not be separated. Changing `ChatEventOutcome` breaks every reference to the conversations pane, and the pane cannot compile without the outcomes it returns, so the pane, its background poller, `ChatFocus`, `OtherSessionIdentity` and `SurfaceId::Conversations` all had to go in the same change as the new focus model. Nothing was lost by doing it early: the Sessions pane already switched sessions through `DashboardAction::Open`, so the only capability missing in between was the in-chat switching shortcut.
  Evidence: `cargo check -p hel-core` after the outcome change reported `no variant named 'SwitchSession' found for enum 'ChatAction'` at `src/hel_chat/active.rs:161`, inside `neighbour_session`, which is conversations-pane code.

- Observation: `/detach` used to mean "return to the dashboard". With one surface there is nothing to return to, so it now quits and leaves the session running — which is what the word says, and what `Ctrl-Q` does.
  Evidence: `src/hel_chat.rs`, `LocalCommand::Detach` now yields `ChatAction::QuitDetach`; the command's description in `src/hel_chat/autocomplete.rs` changed from "return to the dashboard without stopping the worker" to "leave Hel without stopping the worker".

- Observation: the P and S legend in the Sessions title explains prefixes that only the expanded rows draw, so on a compact pane it was pure noise competing with the workspace name for title width. The title now carries the legend only while the pane has focus.
  Evidence: the first live capture at 140x32 read `Sessions · P=time since prompt · S=time since agent activity scratchpad · Hel is other people's agents`, running the whole width with no expanded row on screen.

- Observation: `Ctrl-W` is intercepted globally by the controller before any view sees it, so the chat composer's `Ctrl-W` (kill previous word) has never worked in Hel.
  Evidence: `crates/hel-cli/src/dashboard.rs`, function `workspace_picker_event`, called first inside the event batch loop in `run_dashboard_for_workspace`; `src/hel_chat.rs` `handle_key` has a `KeyCode::Char('w')` arm under `KeyModifiers::CONTROL` that is unreachable. Moving Workspaces to `F2` fixes this as a side effect.

- Observation: the chat's transcript block draws its title over the region's first row without a left corner glyph, so a render test must look for the title text rather than a box-drawing character; and the composer's border is the doubled variant whenever it has focus.
  Evidence: `draw_in_places_the_transcript_and_prompt_in_the_given_regions` first failed with `transcript border: " Conversation \u{2500}\u{2500}..."` and then with `the prompt's bottom border closes the region: "\u{255a}\u{2550}..."`.

- Observation: the PTY termination test and two end-to-end Python labs key off the literal pane title `Active` and off `Escape` as the quit key, so renaming the pane and making `Escape` non-quitting breaks them.
  Evidence: `crates/hel-cli/tests/termination_pty.rs` line 16 `const READY_MARKER: &[u8] = b"Active";` and line 367 `master.write_all(b"\x1b")`; `tests/e2e/browser_lab.py` line 30 and `tests/e2e/test_hook_chaos.py` lines 67, 71 and 188 all wait for `"Active"`.

## Decision Log

- Decision: `truncate_line_to_width` moved from `pub(super)` to `pub` in `src/hel_chat/rendering.rs` and is re-exported from `hel::hel_chat`, rather than being deleted as dead code or copied into `hel-tui`.
  Rationale: it became dead when the conversations pane went, but the Sessions pane needs exactly the same style-preserving truncation. One implementation in one place beats a second copy in the other crate.
  Date/Author: 2026-08-31, implementer.

- Decision: `select_active_session` no longer moves focus onto the Sessions pane.
  Rationale: it is called whenever a conversation opens, including from the startup pick and from a background arrival. Stealing the keyboard out of the composer because a session appeared is exactly the kind of surprise the combined surface exists to avoid. The caller decides where focus belongs.
  Date/Author: 2026-08-31, implementer.

- Decision: The combined renderer lives in `hel-tui` and takes `Option<&mut hel::hel_chat::ActiveChat>`, rather than the chat rendering the support panes.
  Rationale: `hel-tui` already depends on the `hel` library crate (`crates/hel-tui/Cargo.toml`, `hel.workspace = true`), and `ActiveChat` lives in `hel`. The reverse dependency does not exist and must not be created. Putting the whole-frame layout in one place keeps the pane heights, the modal overlay order, and the hitbox registry consistent.
  Date/Author: 2026-08-31, plan author.

- Decision: The chat keeps its own `FrameSurfaces` registry; the combined renderer merges it into the dashboard's registry with new `append` / `replace_with` helpers instead of threading one shared registry through every chat render function.
  Rationale: `ChatState` pushes surfaces from several modules (`src/hel_chat/transcript.rs`, `src/hel_chat/active.rs`, the elicitation dialog). Threading a borrow through all of them is wide, mechanical churn with no behavioural benefit. A merge at the end preserves render order, which is what the hit-testing depends on.
  Date/Author: 2026-08-31, plan author.

- Decision: `Ctrl-C` keeps its existing per-surface meaning: on the Sessions, Targets, and Quota panes it detaches (as the dashboard does today); in Prompt it clears the composer and stashes the text into history (as the chat does today).
  Rationale: The brief lists `Ctrl-Q` as the quit key and does not mention `Ctrl-C`. Preserving both existing behaviours is the least surprising outcome for existing users, and the two never collide because Prompt is a distinct focus.
  Date/Author: 2026-08-31, plan author.

- Decision: `F2` (Workspaces) is intercepted globally, before any view sees the event, exactly where `Ctrl-W` is intercepted today. `F3` (Web) is a normal-surface key handled by the focused surface.
  Rationale: `F2` only ever ends the current dashboard run and returns to the workspace picker, which is safe from any state. `F3` opens a modal on `DashboardState`; letting it fire while a wizard is open would replace that wizard's mode and lose typed input.
  Date/Author: 2026-08-31, plan author.

- Decision: A single project's expansion state is stored as a *collapsed* set (`BTreeSet<String>` of project keys), not an expanded set or a single expanded key.
  Rationale: The brief requires every project to default to expanded and requires several projects to be expanded at once. A collapsed set makes "default expanded" the empty state, so a newly discovered project is expanded without any extra bookkeeping.
  Date/Author: 2026-08-31, plan author.

- Decision: The Sessions selection is anchored by session id (`selected_session_id: Option<String>`), not by a positional index.
  Rationale: The pane shows different row sets in compact and focused mode, so a positional index would silently point at a different session when focus changes or when the five-session threshold flips. An id survives both.
  Date/Author: 2026-08-31, plan author.

- Decision: The workspace greeting moves from its own full-width title row into the Sessions pane title.
  Rationale: The brief lists the physical layout as Sessions, transcript, Prompt, Targets, Quota, footer — with no title row. Reclaiming that row gives it to the transcript, and the workspace name is still visible.
  Date/Author: 2026-08-31, plan author.

- Decision: While the support panes are minimized, focus is always Prompt; any click on the Sessions pane, the Targets row, or the Quota row restores the full panes and focuses the clicked pane.
  Rationale: The brief states minimizing always focuses Prompt and that clicking a minimized title restores and focuses that pane. Extending the same rule to a Sessions row click keeps one invariant ("minimized implies Prompt focus") instead of two competing ones.
  Date/Author: 2026-08-31, plan author.

## Outcomes & Retrospective

The feature is delivered. Hel's TUI is one screen — Sessions, transcript, Prompt, Targets, Quota, footer — with no view switch, no `View` enum, and no second renderer. `Ctrl-G` collapses the support panes instead of navigating back, `Tab` walks four focus stops, the panes take plain letters, `Escape` never quits, and Workspaces and the web viewer moved to `F2` and `F3`, which handed `Ctrl-W` and `Ctrl-B` back to the composer.

Three things are worth passing on.

The milestone boundaries in this plan did not survive contact with the compiler. Milestones 2 and 5 were one change: `ChatEventOutcome` and the conversations pane are the same knot, and pulling either unravels the other. The plan's own ordering hazard note was about the opposite risk — deleting the pane too early — and it turned out to be unfounded, because the Sessions pane already switched sessions through `DashboardAction::Open`. A plan can sequence work that the type system will not let you sequence; the honest response is to merge the milestones and say so, which is what the Progress and Surprises sections now do.

Two real defects came out of writing the tests the plan asked for rather than out of running the code. The startup fallback ranked sessions backwards, so a workspace whose summaries had not loaded would have opened its oldest conversation; and the first cut of the collapse test asserted a row count that was wrong in a way that hid a second effect (minimizing also compacts Sessions, because it moves focus to Prompt). Both are the kind of thing a screenshot would have passed.

Driving the real binary in a PTY was worth more than any single test. It is what surfaced the over-long Sessions title, the guidance line clipped out of the empty prompt, and the two pane titles that still said Capacity and Profile Quotas — none of which the unit tests were looking at, because none of them were wrong in a way a test had been written to notice.

## Context and Orientation

Read this section as if you have never opened this repository. Every path is relative to the repository root, `/home/jonathan/Projects/hel`.

### Terms used in this plan

- **Workspace**: a named grouping of Hel sessions, chosen at startup. All panes in this plan show only the selected workspace's sessions.
- **Session**: one long-running coding-agent conversation, owned by a Hel *controller* process and executed on a *target*. A session record is `hel::hel_state::SessionRecord`.
- **Live session**: a session whose `SessionRecord::state` satisfies `hel::hel_state::SessionState::is_active()`. Everything else (stopped, lost, errored) is shown only in the Resume dialog, never in the Sessions pane.
- **Target**: the machine or container a session runs on. The pane the brief calls **Targets** is the one currently titled `Capacity` and internally called `Focus::Capacity`.
- **Profile**: one harness account (a Codex account, a Claude account…) with its own usage limits. The **Quota** pane lists these.
- **Projection / materialized session**: Hel replays a session's durable event journal into a `hel::hel_state::MaterializedSession`. A cheaper `MaterializedSessionSummary` (last agent message, last user message, whether a turn is running, last activity timestamp) is persisted so the surface can show a session without replaying its whole history.
- **Surface / frame surface**: a rectangle registered during a draw so mouse hit-testing and text selection know what is where. See `src/hel_selection.rs`: `SurfaceId`, `SurfaceFrame`, `FrameSurfaces`.
- **Reducer**: in this codebase, the pure `handle_key` / `handle_mouse` functions that turn an input event into new state plus a `DashboardAction` or `ChatAction`. They perform no I/O; the controller runs the actions.

### The three crates

The workspace (`Cargo.toml` at the root) has three members that matter here.

- `hel` (library crate; sources under `src/`, package name `hel-core`). Owns session state, the chat view, and the selection engine. Relevant files: `src/hel_chat.rs` (`ChatState`, `ChatAction`, `ChatEventOutcome`), `src/hel_chat/active.rs` (`ActiveChat`, background feeds, the whole-frame chat renderer `render`), `src/hel_chat/transcript.rs` (transcript rows and `render_transcript`), `src/hel_selection.rs` (`FrameSurfaces`).
- `hel-tui` (`crates/hel-tui/`). Owns the dashboard: `src/lib.rs` (`DashboardState`, `Focus`, `Mode`, `DashboardAction`, the key and mouse reducers), `src/render.rs` (pane layout and drawing), `src/ingest.rs` (`SessionDetail`, `CapacityDetail`, and how projections are folded into the dashboard), `src/resume.rs`, `src/dialogs.rs`, `src/wizards.rs`. It depends on `hel`.
- `hel-cli` (`crates/hel-cli/`, produces the `hel` binary). Owns the one event loop: `src/dashboard.rs` (`DashboardContext`, `run_dashboard_for_workspace`, the `View` switch), `src/dashboard/actions.rs` (running a `DashboardAction`), `src/dashboard/io.rs` (background I/O tasks and their results). It depends on both `hel` and `hel-tui`.

### How the two screens work today

`crates/hel-cli/src/dashboard.rs` holds `enum View { Dashboard, Chat }` on `DashboardContext`. One `tokio::select!` loop drives everything. `DashboardContext::draw` branches on `View`: either `hel_tui::render(frame, dashboard)` or `chat.draw(frame, transcript_selected)`. `dispatch_to_view` branches the same way to decide whether a terminal event goes to `DashboardState::handle_key` or to `ActiveChat::handle_event`.

The chat view is *data*, not a nested loop: `DashboardContext::active_chat: Option<ActiveChat>` stays alive with its background feeds running even while the dashboard is on screen. That is the property this change relies on — the chat is already always warm, so drawing it in the same frame as the panes costs nothing new.

`ActiveChat::draw` calls the private `render` in `src/hel_chat/active.rs`, which takes `frame.area()` and splits it into four bands: a *conversations* pane listing same-project sessions, the transcript, the prompt, and a one-row footer. That conversations pane, its background poller (`spawn_other_session_poller`), and its focus (`ChatFocus::Conversations`) are all being deleted: the new Sessions pane replaces them and covers every project, not just the current one.

`crates/hel-tui/src/render.rs` `render` calls `render_adaptive_dashboard`, which computes three pane heights with `allocate_pane_heights`, draws a greeting title row, then `render_sessions`, `render_capacity`, `render_quotas`, a two-row footer, and finally `render_modal` over the top.

### Key facts you will need

- `DashboardState::ordered_sessions()` returns live sessions grouped by project (the group key is `hel::hel_state::ProjectSourceIdentity::key`, resolved asynchronously from the git origin so two differently named worktrees of one repository group together), ordered by creation within a group.
- `DashboardState::session_details: BTreeMap<String, SessionDetail>` holds the per-session projection results. `SessionDetail` (in `crates/hel-tui/src/ingest.rs`) carries `current_turn_started_at: Option<u64>` (set means an agent turn is running), `last_activity_at_ms: Option<u64>`, `last_agent_message`, `last_user_message`, `last_agent_message_follows_last_user`, `latest_agent_activity_after_last_user`, and unread counters.
- `DashboardState::capacity_details: BTreeMap<String, CapacityDetail>`. `CapacityDetail` carries `target: DeploymentCapacityTarget` (with `host: String` and `kind: DeploymentCapacityKind::{Host, AwsFleet}`), `usage: Option<DeploymentCapacityUsage>` (with `cpu_percent: Option<u8>`), `on_demand: bool`, `sampled_at_epoch_seconds: Option<u64>`, `probe_error: Option<String>`, and `refreshing: bool`.
- `DashboardState::quotas: BTreeMap<String, ProfileQuota>` and `quota_refreshing: BTreeSet<String>`. `ProfileQuota::weekly_window()` returns `Option<&QuotaWindow>`; `render.rs::quota_remaining_percent(window)` already converts a window into a remaining percentage.
- `dashboard_accelerator(modifiers)` in `crates/hel-tui/src/lib.rs` is `SUPER` on macOS and `CONTROL` elsewhere. Every "Ctrl-" key in this plan means that accelerator.
- The notice bar is a shared `hel::hel_chat::Notices` handle installed on both the dashboard and every chat, so one footer row can show notices from either side.

## Plan of Work

Six milestones. Each one compiles, keeps `cargo test` green, and leaves the product in a usable state.

### Milestone 1 — Area-scoped chat rendering

**Goal.** `ActiveChat` can draw its transcript and prompt into rectangles somebody else chose, and can report the surfaces it registered so a caller can merge them with its own. Nothing user-visible changes yet: the existing whole-frame `draw` is reimplemented on top of the new entry point and still draws the conversations pane.

**Work.**

In `src/hel_selection.rs`, add two methods to `FrameSurfaces`:

    /// Appends every surface `other` registered, preserving render order.
    /// Used where one frame is drawn by more than one renderer.
    pub fn append(&mut self, other: &FrameSurfaces) { ... }

    /// Replaces every registration with `other`'s. Used when a modal owns
    /// the frame's interaction and everything behind it stops being
    /// selectable.
    pub fn replace_with(&mut self, other: &FrameSurfaces) { ... }

In `src/hel_chat.rs`, add a public description of where the chat may draw:

    /// Where a host surface has told the chat to draw itself.
    ///
    /// `transcript` and `prompt` are the *outer* rectangles including each
    /// block's border. `footer` is `Some` only when the host wants the chat
    /// to own the footer row (that is, while the composer has focus).
    /// `overlay` is the whole frame: modals and the autocomplete popup are
    /// centred and clamped inside it.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ChatRegions {
        pub transcript: Rect,
        pub prompt: Rect,
        pub footer: Option<Rect>,
        pub overlay: Rect,
    }

In `src/hel_chat/active.rs`, split the existing private `render(frame, chat, transcript_selected)` into two functions. The new one takes the regions and a focus flag:

    pub(super) fn render_in(
        frame: &mut Frame,
        chat: &mut ChatState,
        regions: ChatRegions,
        prompt_focused: bool,
        transcript_selected: bool,
    )

`render_in` keeps everything the current `render` does from `let split = chat.second_opinion_split();` onward, with these substitutions: `transcript_area` becomes `regions.transcript`; `prompt_area` becomes `regions.prompt`; the footer block runs only when `regions.footer` is `Some`; `inner` (used to centre the second-opinion setup box) becomes `regions.overlay`; and every place that reads `chat.focus == ChatFocus::Prompt` reads the `prompt_focused` argument instead. It starts with `chat.frame_surfaces.clear()` and sets `chat.frame_surfaces_exclusive = false`; the two places that currently call `chat.frame_surfaces.clear()` a second time (the second-opinion setup box and the elicitation dialog) additionally set `chat.frame_surfaces_exclusive = true`.

Add the field and its accessor:

    // on ChatState
    /// The last frame's surfaces replace everything behind them, because a
    /// modal owned the frame. The combined renderer reads this to decide
    /// whether to merge or replace.
    pub(super) frame_surfaces_exclusive: bool,

    // on ChatState and re-exported on ActiveChat
    pub fn frame_surfaces_exclusive(&self) -> bool

Add the height query and the new draw entry point on `ActiveChat`:

    /// Rows the composer wants, given the width it will be drawn at. This is
    /// the wrapped input height plus up to three queued-prompt preview rows
    /// plus the block border, never fewer than four.
    pub fn desired_prompt_height(&self, width: u16) -> u16

    /// Draws the transcript and the composer into `regions`.
    ///
    /// `prompt_focused` says whether the composer owns the keyboard; only
    /// then does it draw a cursor and a double border. `transcript_selected`
    /// says the selection engine still owns a selection on the transcript,
    /// so its row space must stay frozen.
    pub fn draw_in(
        &mut self,
        frame: &mut Frame,
        regions: ChatRegions,
        prompt_focused: bool,
        transcript_selected: bool,
    )

`desired_prompt_height` is the existing arithmetic lifted out of `render`: `input_visual_rows(&self.state.input, usize::from(width.saturating_sub(2)).max(1)) + self.state.queued_prompts.len().min(3) + 2`, floored at 4, all in `u16` with saturating arithmetic.

Finally, keep the existing whole-frame path working by reimplementing it: the private `render(frame, chat, transcript_selected)` keeps its conversations-pane block, then computes `ChatRegions` from the remaining chunks and calls `render_in`. `ActiveChat::draw` still calls it.

**Acceptance.** `cargo test -p hel-core` passes unchanged. Two new tests in `src/hel_chat/active.rs`'s test module:

- `draw_in_places_the_transcript_and_prompt_in_the_given_regions`: build a chat with the existing test helpers, draw it at 80x24 into `ChatRegions { transcript: Rect::new(0, 4, 80, 12), prompt: Rect::new(0, 16, 80, 5), footer: None, overlay: Rect::new(0, 0, 80, 24) }`, and assert that rows 0..4 of the buffer are untouched (all spaces) while the prompt's border is on rows 16 and 20.
- `draw_in_draws_a_cursor_only_when_the_prompt_has_focus`: the same draw with `prompt_focused: false` leaves `frame.cursor_position()` unset; with `true` it is inside the prompt's inner rect.

### Milestone 2 — Combined focus model, minimize state, and the keymap

**Goal.** `DashboardState` becomes the reducer for the whole combined surface: four focus stops, a minimize flag, per-project collapse, and the rationalized keymap. The chat answers `Tab`, `Ctrl-G` and `F3` with outcomes the controller can act on. Nothing is drawn differently yet.

**Work in `crates/hel-tui/src/lib.rs`.**

Rename and extend the focus enum, and make it public so `hel-cli` can reason about it:

    /// Which part of the combined surface owns the keyboard.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Focus {
        Sessions,
        Quota,
        Targets,
        Prompt,
    }

    /// Tab order. Shift-Tab walks it backwards.
    pub(crate) const FOCUS_ORDER: [Focus; 4] =
        [Focus::Sessions, Focus::Quota, Focus::Targets, Focus::Prompt];

`Focus::Active` becomes `Focus::Sessions` and `Focus::Capacity` becomes `Focus::Targets` throughout `crates/hel-tui/`. `DASHBOARD_PANE_COUNT` stays 3 (Sessions, Targets, Quota are still the three bordered support panes with `SurfaceId::DashboardPane(0..3)`); Prompt is not one of them.

New fields on `DashboardState`:

    /// Targets and Quota are collapsed to one summary row each and Sessions
    /// stays compact. While this is true, focus is always `Focus::Prompt`.
    pub(crate) support_minimized: bool,
    /// Projects the user has collapsed in the focused Sessions pane. Absent
    /// means expanded, so a newly discovered project defaults to expanded.
    pub(crate) collapsed_project_keys: BTreeSet<String>,
    /// The session whose conversation is on screen, or is being opened. It
    /// decides which project the compact Sessions list belongs to.
    pub(crate) current_session_id: Option<String>,
    /// Selection anchor for the Sessions pane, by id rather than position so
    /// it survives the pane changing which rows it shows.
    pub(crate) selected_session_id: Option<String>,

Remove `session_index` and `expanded_project_key`. Replace their uses:

    /// Live sessions the Sessions pane is currently showing, as indices into
    /// `ordered_sessions()`.
    pub(crate) fn visible_session_indices(&self) -> Vec<usize>

    /// Position of the selected session among `visible_session_indices()`,
    /// for the table's highlight. `None` when nothing is selected.
    pub(crate) fn selected_visible_index(&self) -> Option<usize>

`clamp_selections` gains: if `selected_session_id` names no currently visible session, set it to the first visible session's id, or `None` when there are none; and drop from `collapsed_project_keys` any key that is no longer a project of a live session.

New public accessors used by the controller in `hel-cli`:

    pub fn focus(&self) -> Focus
    pub fn prompt_has_focus(&self) -> bool
    pub fn focus_prompt(&mut self)
    pub fn focus_sessions(&mut self)
    pub fn cycle_focus(&mut self, reverse: bool)
    pub fn toggle_support_panes(&mut self)
    pub fn support_minimized(&self) -> bool
    pub fn modal_open(&self) -> bool           // `!matches!(self.mode, Mode::Dashboard)`
    pub fn set_current_session(&mut self, session_id: Option<&str>)
    pub fn open_web_dialog(&mut self) -> DashboardAction  // sets Mode::Web(loading), returns LoadWebAccess

`cycle_focus` clears `support_minimized` first, then advances through `FOCUS_ORDER` with the existing `cycle_control` helper. That single rule gives the brief's behaviour for free: from a minimized Prompt, `Tab` restores and lands on Sessions, `Shift-Tab` restores and lands on Targets.

`toggle_support_panes` flips `support_minimized` and, when the result is `true`, sets `focus = Focus::Prompt`. When the result is `false` it leaves focus alone, so `Ctrl-G` twice returns you to Prompt.

**The keymap.** Rewrite `handle_dashboard_key` (the `Mode::Dashboard` arm) as follows. "Ctrl-" means `dashboard_accelerator`.

Applies from any focus on the normal surface:

- `Ctrl-Q` → `DashboardAction::QuitDetach`. This is checked before the mode dispatch, as it already is, so it also works while a modal is open.
- `Ctrl-G` → `self.toggle_support_panes()`, returns `DashboardAction::None`.
- `F3` → `self.open_web_dialog()`.
- `Tab` → `self.cycle_focus(false)`. `BackTab` → `self.cycle_focus(true)`.
- `Esc` → `DashboardAction::None`. It must no longer quit. (`Esc` still cancels a modal, through the existing per-mode handlers, and the selection engine still consumes it to drop a finished selection before the reducer sees it.)
- `Ctrl-C` → `DashboardAction::QuitDetach`, unchanged, and only when no text input has focus.
- `Ctrl-V` → `DashboardAction::PasteFromClipboard`, unchanged.

Applies to whichever list has focus (Sessions, Targets, Quota):

- `Up`, `k`, `Ctrl-P` → previous row. `Down`, `j`, `Ctrl-N` → next row. `Home` → first. `End` → last.

Sessions only, all plain keys with no modifier:

- `Enter` → `self.open_or_resume()`.
- `n` → `self.begin_new()`. `s` → `DashboardAction::OpenResumeDialog`. `e` → `self.begin_session_edit()`. `a` → `self.mark_all_read()`. `x` → `DashboardAction::CancelOperation { .. }` for the selected session's in-flight lifecycle operation, or `None` when it has none.
- `Space` → toggle the selected session's project in `collapsed_project_keys`.
- `1`..`9` → toggle the numbered project in `collapsed_project_keys`, independently of every other project.

Targets only: `r` → `DashboardAction::RefreshCapacity`; `Enter` or `e` → `self.begin_target_actions()`.

Quota only: `r` → `DashboardAction::RefreshQuotas`; `Enter` or `e` → `self.begin_profile_rename()`.

Onboarding (empty configuration) only: `e` → `DashboardAction::OpenConfig`.

`open_or_resume` loses its "expand the project first" special case: with every project expanded by default, `Enter` always means open.

**Work in `src/hel_chat.rs` and `src/hel_chat/active.rs`.** Replace the outcome enum's screen-switching variants:

    pub enum ChatEventOutcome {
        None,
        Handled,
        /// Tab or Shift-Tab from the composer. The combined surface owns
        /// focus, so the chat only reports the direction.
        CycleFocus { reverse: bool },
        /// Ctrl-G: collapse or restore the support panes.
        ToggleSupportPanes,
        /// F3: open the web-access dialog.
        OpenWebDialog,
        QuitDetach { last_seen_event_ordinal: u64 },
    }

`Back { .. }` and `SwitchSession { .. }` are deleted. In `ChatState::handle_key`, the `Ctrl-G` arm returns `ChatAction::ToggleSupportPanes` instead of `ChatAction::Back`; add an `F3` arm returning `ChatAction::OpenWebDialog`; the `KeyCode::Tab` arm becomes

    KeyCode::Tab => {
        if self.accept_autocomplete() {
            ChatAction::None
        } else {
            ChatAction::CycleFocus { reverse: false }
        }
    }
    KeyCode::BackTab => ChatAction::CycleFocus { reverse: true },

which is the "autocomplete precedence" rule: an open completion popup consumes `Tab` and focus does not move. `ChatAction` gains the matching `CycleFocus`, `ToggleSupportPanes`, and `OpenWebDialog` variants and loses `Back` and `SwitchSession`; `ActiveChat::dispatch` maps each straight through to the corresponding `ChatEventOutcome`.

**Acceptance.** New tests in `crates/hel-tui/src/lib.rs`'s test module:

- `tab_walks_sessions_quota_targets_prompt_and_back`: four `Tab` presses from `Focus::Sessions` visit Quota, Targets, Prompt, Sessions.
- `shift_tab_walks_the_reverse_order`: four `BackTab` presses visit Prompt, Targets, Quota, Sessions.
- `ctrl_g_minimizes_and_always_focuses_prompt`: from `Focus::Targets`, `Ctrl-G` leaves `support_minimized == true` and `focus == Focus::Prompt`; a second `Ctrl-G` leaves `support_minimized == false` and focus still Prompt.
- `tab_from_a_minimized_prompt_restores_and_focuses_sessions`, and the `BackTab` twin landing on Targets.
- `escape_never_quits_the_combined_surface`: `Esc` from each of the four focuses returns `DashboardAction::None`.
- `plain_keys_drive_the_focused_pane`: `n`, `s`, `e`, `a`, `x` on Sessions; `r` and `e` on Targets; `r` and `e` on Quota; and the negative case that `r` on Sessions does nothing.
- `ctrl_n_and_ctrl_p_move_the_focused_list`, mirroring the arrow-key test for each pane.
- `f2_and_f3_are_reachable_from_every_pane`: `F3` opens `Mode::Web` and returns `LoadWebAccess` from each pane focus. (`F2` is asserted in Milestone 5, where the controller intercept lives.)
- `digits_toggle_projects_independently`: with three projects, `1` collapses the first, `3` collapses the third, the second stays expanded, and `1` again re-expands the first.

In `src/hel_chat/active.rs`: `tab_accepts_an_open_completion_before_cycling_focus` — with an autocomplete popup open, `Tab` returns `ChatEventOutcome::None` and the popup closes; a second `Tab` returns `CycleFocus { reverse: false }`.

### Milestone 3 — Sessions pane projection and rendering

**Goal.** The Sessions pane shows the right rows in both of its modes, and its content rules are testable without drawing.

**Work in `crates/hel-tui/src/lib.rs`.** Add the row projection:

    /// One drawn row of the Sessions pane.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) enum SessionsRow {
        /// A project name with its 1-9 toggle number, in focused mode only.
        ProjectHeading { key: String, label: String, number: Option<usize> },
        /// A live session, by index into `ordered_sessions()`. `expanded`
        /// selects the four-row form over the one-line form.
        Session { index: usize, expanded: bool },
        /// The live sessions the compact list left out.
        Others { active: usize, idle: usize },
    }

    pub(crate) fn sessions_rows(&self) -> Vec<SessionsRow>

    /// The project the compact list belongs to: the open or opening
    /// conversation's project, else the selected session's project.
    pub(crate) fn current_project_key(&self) -> Option<String>

`sessions_rows` has exactly two modes, chosen by `self.focus == Focus::Sessions`.

*Focused mode* ignores the five-session threshold. Walk `ordered_sessions()`; each time the project key changes, emit a `ProjectHeading` whose `number` is `Some(n)` for the nth project when `n <= 9` and there is more than one project, and `None` otherwise. Emit each session as `Session { expanded: !self.collapsed_project_keys.contains(&key) }`. The heading label is the project's `short` name, or its `full` name when two different projects share a short name (the existing rule in `render_sessions`).

*Compact mode* emits no headings. Let `live = self.ordered_sessions()`. If `live.len() <= 5`, emit every session as `Session { expanded: false }`. Otherwise let `current = self.current_project_key()`; emit `Session { expanded: false }` for every session whose project key equals `current`, in order, and then, if any session was left out, one `Others { active, idle }` where `active` counts left-out sessions whose `SessionDetail::current_turn_started_at` is `Some` and `idle` is the rest. If `current` is `None`, every session is left out and the pane is a single `Others` row.

**Work in `crates/hel-tui/src/render.rs`.** Rewrite `render_sessions` to draw from `sessions_rows()` rather than walking `ordered_sessions()` itself, into a caller-supplied `Rect`:

    pub(crate) fn render_sessions(
        frame: &mut Frame,
        area: Rect,
        dashboard: &DashboardState,
    ) -> SessionRowsRendered

Row content:

- A compact `Session` row is one line: a two-cell caret (`"› "` for the session that is currently open, `"  "` otherwise), then the project short name, the target label, the turn clock, and the last non-empty line of the last agent message, joined by `" · "` and truncated to the pane width. Reuse `session_target_label`, `hel::usage_format::format_turn_clock`, and `session_band_color` so the row keeps the colour that says what the session is doing.
- An `Others` row is one dim line: `"  Others: {active} active, {idle} idle"`.
- A focused expanded `Session` row is exactly four lines and always four: `session_top_line` (status and identity), the `You: …` summary from `prefixed_summary_line`, and two agent/activity lines. Today the two agent lines are skipped when `show_agent_excerpt` is false; remove that condition so the block always renders, falling back to `Line::raw("No messages yet")` plus one blank line. This is what makes every expanded session occupy the same height, which is what the brief asks for and what keeps the height arithmetic simple.
- A focused collapsed `Session` row is one line from the existing `collapsed_session_line`.
- The caret in focused mode marks the *selected* session (`selected_visible_index`), and the table's `TableState` highlight uses the same index.

The pane title becomes `" Sessions "` plus, when it fits, `" · "` and the workspace greeting in dark gray, truncated to the pane width. `active_pane_title` is renamed `sessions_pane_title` and keeps its three width tiers, with `Active` replaced by `Sessions`.

`SessionRowsRendered` gains nothing; `active_row_areas` is renamed `session_row_areas` and keeps mapping a drawn rectangle to an index into `ordered_sessions()`, and `project_heading_areas` is unchanged.

**Acceptance.** New tests in `crates/hel-tui/src/render.rs`'s test module and `crates/hel-tui/src/lib.rs`'s test module:

- `compact_sessions_list_every_project_up_to_five`: five live sessions across three projects, focus on Prompt, produces five `Session` rows and no `Others`.
- `compact_sessions_list_aggregates_beyond_five`: six live sessions, two of them in the current project, produces two `Session` rows and `Others { active, idle }` counting exactly the other four, with `active` matching how many have a running turn.
- `compact_sessions_list_is_empty_for_zero_live_sessions`.
- `others_row_follows_the_current_project`: changing `current_session_id` to a session in another project changes which sessions are listed and re-counts `Others`.
- `focused_sessions_list_ignores_the_five_session_threshold`: with six live sessions and `Focus::Sessions`, every session gets a row and no `Others` row appears.
- `focused_sessions_default_to_expanded`: with no key in `collapsed_project_keys`, every `Session` row has `expanded: true`.
- `an_expanded_session_always_draws_four_rows`: draw a session with no agent messages at all and assert the drawn pane contains the identity line, the `You:` line, `No messages yet`, and a blank fourth line.
- `collapsing_one_project_leaves_the_others_expanded`: after `Space` on a session in project A, project A's rows are one line each and project B's are still four.
- `multiple_projects_get_headings_and_toggle_numbers`.

### Milestone 4 — The combined renderer

**Goal.** One function draws the whole screen. This is the milestone that makes the feature visible.

**Work.** Add `crates/hel-tui/src/combined.rs` and declare `mod combined;` in `crates/hel-tui/src/lib.rs`, re-exporting:

    /// Draws the whole combined surface: Sessions, the conversation, Prompt,
    /// Targets, Quota, the footer, and any modal over the top.
    ///
    /// `chat` is the conversation on screen, or `None` when the workspace has
    /// no live session. `transcript_selected` says the selection engine still
    /// owns a transcript selection, so the transcript's row space must stay
    /// frozen for this frame.
    pub fn render_combined(
        frame: &mut Frame,
        dashboard: &mut DashboardState,
        chat: Option<&mut hel::hel_chat::ActiveChat>,
        transcript_selected: bool,
    )

The old `pub use crate::render::render;` is kept for the onboarding path only (an empty configuration still draws the "Hel needs a little fuel" screen with Targets, Quota and a footer under it, and no conversation); `render_combined` delegates to it when `dashboard.config_is_empty()`.

*Height allocation.* Add to `combined.rs`:

    /// How tall one band wants to be and how short it may get.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct PaneBand { minimum: u16, full: u16, cap: u16 }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct CombinedHeights {
        sessions: u16, transcript: u16, prompt: u16,
        targets: u16, quota: u16, footer: u16,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum CombinedAllocation {
        Fits(CombinedHeights),
        TooSmall { required_frame_height: u16 },
    }

    fn allocate_combined_heights(
        frame_height: u16,
        sessions: PaneBand,
        targets: PaneBand,
        quota: PaneBand,
        desired_prompt: u16,
        focus: Focus,
        minimized: bool,
    ) -> CombinedAllocation

The algorithm, in order, using saturating arithmetic throughout:

1. `footer` is always 1. `transcript` starts at its minimum of 3. `prompt` starts at its minimum of 3.
2. Minimums: `sessions.minimum` is 3 (a border plus one row). When `minimized`, `targets.minimum` and `quota.minimum` are 1 (the one-row title). Otherwise both are 3.
3. `required = sessions.minimum + 3 + 3 + targets.minimum + quota.minimum + 1`. If `frame_height < required`, return `TooSmall { required_frame_height: required }`.
4. `surplus = frame_height - required`. Spend it, in this order, each step taking `min(surplus, want - current)` and subtracting what it took:
   a. `prompt` up to `min(desired_prompt, max(3, frame_height / 3))`.
   b. the focused support pane, if any (`Focus::Prompt` skips this step), up to `min(band.full, band.cap)`.
   c. `sessions` up to `min(sessions.full, sessions.cap)`.
   d. `targets` up to `min(targets.full, targets.cap)`, skipped entirely when `minimized`.
   e. `quota` likewise, skipped when `minimized`.
   f. `transcript` takes all remaining surplus.
5. Caps: `sessions.cap = max(3, if focus == Focus::Sessions { frame_height / 2 } else { frame_height / 3 })`. `targets.cap` and `quota.cap` are `max(3, if that pane has focus { frame_height / 3 } else { frame_height / 4 })`.
6. Full heights: `sessions.full = 2 + sum of the row heights sessions_rows() implies` (1 per compact row, 1 per `Others` row, 1 per heading, and 4 or 1 per focused session, plus the existing one-row spacing between groups). `targets.full = 3 + capacity_details.len()`. `quota.full = 3 + config.profiles.len()`. When `minimized`, `targets.full` and `quota.full` are 1.

`render_combined` then:

1. Clears `dashboard.frame_surfaces`, `dashboard.session_row_areas`, `dashboard.project_heading_areas`, `dashboard.pane_areas`, and the two new chat-region rectangles described below.
2. Refuses a frame narrower than `MINIMUM_TERMINAL_WIDTH` (32) with the existing `render_terminal_too_small`, and refuses a `TooSmall` allocation the same way with the required height.
3. Splits `frame.area()` vertically into sessions, transcript, prompt, targets, quota, footer using the allocated heights.
4. Draws Sessions with `render_sessions`, and registers `SurfaceId::DashboardPane(0)` over its inner content.
5. Draws the conversation. With `Some(chat)`, calls `chat.draw_in(frame, ChatRegions { transcript, prompt, footer: focus == Focus::Prompt then Some(footer) else None, overlay: frame.area() }, dashboard.prompt_has_focus(), transcript_selected)`, then merges the chat's registry: `replace_with` when `chat.frame_surfaces_exclusive()`, `append` otherwise. With `None`, draws a placeholder: the transcript band gets a bordered empty block titled `" Conversation "`, and the prompt band a bordered block titled `" Prompt (no live session) "` holding two dim lines, `No live session in this workspace.` and `Press Tab for Sessions, then n to create or s to resume.`
6. Draws Targets: `render_capacity` when not minimized, otherwise `render_minimized_targets`. Registers `SurfaceId::DashboardPane(1)` over the drawn rectangle either way.
7. Draws Quota the same way with `render_quotas` / `render_minimized_quota` and `SurfaceId::DashboardPane(2)`.
8. Draws the footer, unless the chat already drew it (step 5 with `footer: Some`).
9. Draws any modal with the existing `render_modal`, which registers its own surfaces last so it wins every cell it covers.
10. Records `dashboard.chat_transcript_area` and `dashboard.chat_prompt_area` (two new `Option<Rect>` fields) so the controller can route mouse events by region.

*The minimized rows.* In `crates/hel-tui/src/render.rs`:

    /// One row summarising every target host and its CPU load.
    pub(crate) fn minimized_targets_line(dashboard: &DashboardState, width: u16) -> Line<'static>

Bold `Targets`, then one `{host} {reading}` segment per `capacity_details` value, joined by two spaces and truncated to `width`. The reading is, in order: `refreshing…` when `detail.refreshing`; otherwise the value with ` (stale)` appended when `capacity_staleness` returns `Some`; for a `Host` with usage, `{cpu}%` from `usage.cpu_percent`, or `no CPU` when that is `None`; for an `AwsFleet` with usage, `no CPU` (fleet probes report cores, memory and disk, never a CPU percentage); for an `AwsFleet` with no usage and `on_demand`, `on demand`; otherwise `unavailable`.

    /// One row summarising every profile's weekly usage.
    pub(crate) fn minimized_quota_line(dashboard: &DashboardState, width: u16) -> Line<'static>

Bold `Quota`, then one `{profile} {used}%` segment per configured profile. `used` is `100 - remaining` where `remaining` comes from `quota_remaining_percent(quota.weekly_window()?)`. A profile in `quota_refreshing` reads `refreshing…`; a `HarnessKind::Deepseek` profile reads `api` (it is usage-priced and has no subscription window); a quota with an error, no stored quota, or no weekly window reads `unavailable`.

*The footer.* Add to `render.rs`:

    pub(crate) fn combined_footer_text(dashboard: &DashboardState) -> &'static str

returning, by focus:

- Sessions: `Enter open · n new · s resume · e edit · a mark read · x cancel · Tab pane · Ctrl-G panes · F2 workspaces · F3 web · Ctrl-Q quit`
- Targets: `r refresh · Enter/e actions · Tab pane · Ctrl-G panes · F2 workspaces · F3 web · Ctrl-Q quit`
- Quota: `r refresh · Enter/e edit profile · Tab pane · Ctrl-G panes · F2 workspaces · F3 web · Ctrl-Q quit`
- Prompt: unused; the chat draws its own footer, with `Ctrl-G dashboard` replaced by `Ctrl-G panes` in all four of its variants.

`render_footer` becomes one row, not two: a notice from the shared `Notices` slot, drawn yellow, replaces the hints when one is present. This matches what the chat footer already does and is what lets the two footers be interchangeable.

*Renames of user-visible text.* `" Capacity "` becomes `" Targets "` in `render_capacity`'s block title. The onboarding text's `Press Ctrl+E to run setup` becomes `Press E to run setup`.

**Acceptance.** New tests in `crates/hel-tui/src/render.rs`'s test module, all drawing through `ratatui::backend::TestBackend`:

- `the_combined_surface_draws_every_band_at_140_by_32`: the drawn buffer contains `Sessions`, `Prompt`, `Targets`, `Quota` and the footer hint, in that vertical order.
- `minimized_panes_collapse_targets_and_quota_to_one_row_each`: after `toggle_support_panes`, the drawn buffer has exactly one row containing `Targets` and one containing `Quota`, and the transcript band is taller than before by the rows they gave up.
- `the_minimized_quota_row_reports_weekly_percent_used`: a profile with `remaining_percent: Some(63)` draws `37%`.
- `the_minimized_rows_stay_explicit_about_missing_readings`: covers refreshing, probe error, stale sample, a fleet with no CPU percentage, and a quota with an error.
- `the_minimized_rows_truncate_rather_than_wrap`: eight hosts at width 60 produce one row.
- `a_narrow_or_short_terminal_reports_what_it_needs`: width 20 draws `Terminal too small` naming 32 columns; height 8 draws it naming the required rows.
- `the_prompt_owns_the_cursor_only_when_it_has_focus`.
- `the_combined_registry_merges_support_and_chat_surfaces`: after a draw, `frame_surfaces().surface(SurfaceId::DashboardPane(0))`, `surface(SurfaceId::Transcript)` and `surface(SurfaceId::PromptInput)` are all present.
- `a_modal_replaces_every_surface_behind_it`: with an elicitation dialog up, `surface_at` over the Sessions pane resolves to the modal's surface, not the pane's.
- `a_workspace_with_no_live_session_shows_the_guidance_prompt`.

### Milestone 5 — Controller integration

**Goal.** The binary draws and drives the combined surface. The old chat conversations pane, its poller, and the view switch are gone.

**Work in `crates/hel-cli/src/dashboard.rs`.**

Delete `enum View`, the `view` field, and `opening_chat_focus_conversations`. Then:

- `draw` always calls `hel_tui::render_combined(frame, dashboard, active_chat.as_mut(), transcript_selected)` inside the one `terminal.draw` closure, and `draw_selection` reads `dashboard.frame_surfaces()` — which now holds the chat's surfaces too, because `render_combined` merged them.
- `frame_surfaces`, `autoscroll_request`, `apply_autoscroll`, `route_selection` and `copy_selection` all read `self.dashboard.frame_surfaces()` unconditionally. `copy_selection` keeps its per-`SurfaceId` extraction: `Transcript`, `ElicitationMessage` and `ReviewerTranscript` still come out of the chat, everything else out of the drawn frame.
- `dispatch_to_view` becomes `dispatch_event` with this routing:
  1. If the event is a mouse event and the pointer is inside `dashboard.chat_region()` (either recorded chat rectangle) and no dashboard modal is open: a left press first calls `dashboard.focus_prompt()`, then the event goes to the chat; a wheel event goes to the chat without changing focus.
  2. Otherwise, if `dashboard.modal_open()`, the event goes to `DashboardState`.
  3. Otherwise, if `dashboard.prompt_has_focus()` and a chat exists, the event goes to the chat.
  4. Otherwise it goes to `DashboardState`.
- `workspace_picker_event` matches `KeyCode::F(2)` instead of `Ctrl-W`, keeping its position as a global intercept.
- `apply_chat_outcome` handles the new variants: `CycleFocus { reverse }` → `self.dashboard.cycle_focus(reverse)`; `ToggleSupportPanes` → `self.dashboard.toggle_support_panes()`; `OpenWebDialog` → run `self.dashboard.open_web_dialog()` through `actions::apply_dashboard_action`; `QuitDetach { .. }` unchanged. `Back` and `SwitchSession` are deleted along with `leave_chat`.
- `open_chat_session` gains, at its top, the detach bookkeeping the deleted `SwitchSession` arm used to do: when a different chat is warm, take its `latest_event_ordinal()` and call `self.record_detach(ordinal)` so the outgoing session's draft and read receipt are persisted before the new one opens. It also calls `self.dashboard.set_current_session(Some(session_id))` and `self.dashboard.select_active_session(session_id)`, and it builds `SessionHeaderIdentity` with only `target` and `profile` (the `position` and `others` fields are gone).
- The `import_tick` arm drops its `context.view == View::Dashboard` guard and keeps only `context.dashboard.needs_fast_tick()`. The `clock_tick` arm always marks the frame dirty, because the support panes carry clocks whatever has focus.

*Startup selection.* Add to `DashboardContext`:

    /// Live sessions whose stored summary has not come back yet. The startup
    /// pick waits for these so it can compare real activity timestamps.
    startup_summaries_pending: BTreeSet<String>,
    /// True until the surface has chosen its first conversation, or the user
    /// has acted and taken the choice away from it.
    startup_open_pending: bool,
    /// A bound on that wait, so a stalled read cannot leave the surface
    /// without a conversation for ever.
    startup_deadline: std::time::Instant,

`hydrate_stored_session_summaries` fills `startup_summaries_pending` with every live session id, sets `startup_open_pending` to whether that set is non-empty, and sets `startup_deadline` to now plus two seconds. With no live session it instead calls `self.dashboard.focus_sessions()`.

Every `DashboardIoUpdate::StoredSessionSummary` result, success or failure, removes its id from the set and then calls `maybe_open_startup_session()`. The one-second clock tick calls it too, so the deadline can fire.

    /// Opens the conversation the surface should start on: the live session
    /// with the newest materialized activity. Ties break by newest creation
    /// time and then by the larger session id, so the choice is the same on
    /// every run. Does nothing once the user has acted.
    fn maybe_open_startup_session(&mut self)

It returns immediately unless `startup_open_pending` and (`startup_summaries_pending.is_empty()` or the deadline has passed). It ranks live sessions by `session_details[id].last_activity_at_ms`, then by `SessionRecord::compare_by_creation` reversed, then by id descending. It clears `startup_open_pending`, calls `self.dashboard.focus_prompt()`, and calls `self.open_chat_session(&id)`. When no summary carried a `last_activity_at_ms` at all, the first key is `None` for everyone and the creation-time tiebreak decides — which is exactly the required fallback.

Any terminal key, mouse or paste event received while `startup_open_pending` sets it to `false` before dispatch, so a user who starts typing or navigating immediately is never yanked into a session they did not ask for.

**Work in `src/hel_chat.rs` and `src/hel_chat/active.rs`.** Delete, in this order so the compiler guides you: `ChatFocus` and `ChatState::focus`; `other_sessions`, `conversations_window_start`, `conversations_area`, and `position` on `ChatState`; `handle_conversations_key`, `focus_conversations`, `neighbour_session`, `click_conversation_row`, `scroll_conversations`; the free functions `conversation_rows`, `conversations_window_start`, `conversations_pane`, `conversation_line`, `other_session_activity`, `spawn_other_session_poller`; the types `ConversationRow`, `ConversationsPane`, `OtherSessionActivity`, `OtherSessionIdentity`; the constants `CONVERSATIONS_PANE_MAX_ROWS` and `CURRENT_SESSION_CARET`; `ChatIoUpdate::OtherSessions`; `SessionHeaderIdentity::position` and `::others`; `ChatState::set_header_position`; `ActiveChat::focus_conversations`; and `SurfaceId::Conversations` in `src/hel_selection.rs`. `ChatState::handle_mouse` loses its `over_conversations` branch and always scrolls the transcript. `ActiveChat::needs_clock_tick` loses its `other_sessions` term. `ChatState::last_agent_line` becomes dead once the pane is gone; delete it too.

The private whole-frame `render` in `src/hel_chat/active.rs` and `ActiveChat::draw` are deleted; `render_in` and `draw_in` are the only chat renderers left.

**Acceptance.** New tests in `crates/hel-cli/src/dashboard.rs`'s test module:

- `startup_opens_the_session_with_the_newest_materialized_activity`: three live sessions with `last_activity_at_ms` of 10, 300 and 200 open the second.
- `startup_falls_back_to_the_newest_creation_when_no_summary_arrives`: every summary result is an error; the session with the newest `created_at` opens; a further tie on `created_at` breaks by the larger id.
- `startup_does_not_override_a_user_who_acted_first`: a key event delivered while summaries are pending leaves `startup_open_pending` false and opens nothing.
- `a_workspace_with_no_live_session_focuses_sessions`.
- `f2_reaches_the_workspace_picker_from_every_focus`.
- `switching_sessions_persists_the_outgoing_draft_and_read_receipt`.

The existing tests `dragging_inside_a_pane_copies_only_that_panes_rows`, `click_gestures_reach_the_view_as_presses_and_still_double_click`, `presses_off_every_surface_and_wheel_events_reach_the_view`, `a_scrollable_surface_is_highlighted_without_stashing_frame_text`, `escape_clears_a_finished_selection_before_the_view_sees_it` and `only_events_that_ask_for_work_end_an_input_batch` must be updated to draw through `render_combined` and must keep passing: they are the regression net for the merged selection surfaces.

### Milestone 6 — Documentation, test harness, and final validation

**Goal.** Nothing on screen, in the repository's prose, or in the test harnesses still describes two screens.

**Work.**

- `README.md`: in the Quickstart, replace step 3's "In the dashboard, create a session" with a description of the combined surface, and add a short section after the Quickstart titled `The terminal surface` covering the six bands and the keymap: `Tab`/`Shift-Tab` to move between Sessions, Quota, Targets and Prompt; `Ctrl-G` to collapse and restore Targets and Quota; `F2` for Workspaces; `F3` for the web viewer; `Ctrl-Q` to detach; `Enter`/`n`/`s`/`e`/`a`/`x` on Sessions; `r` and `Enter`/`e` on Targets and Quota; `PageUp`/`PageDown` and the wheel for the transcript.
- `docs/src/content/docs/containers.md` line 151: `Press **Ctrl+N** to start the new-session wizard` becomes `Press **Tab** to focus Sessions, then **n** to start the new-session wizard`.
- `.agents/docs/parallel-luna-testing.md`: rewrite the paragraph beginning `Dashboard help labels such as [Q]uit` — the labels no longer use bracketed accelerators, `Escape` no longer quits, and the new-session sequence begins with `Tab` to reach Sessions and then plain `n`. This file is a runbook for test agents; leaving the old keys there would make them file false defects.
- `crates/hel-cli/tests/termination_pty.rs`: `READY_MARKER` becomes `b"Sessions"`, and the detach test sends `b"\x11"` (`Ctrl-Q`) instead of `b"\x1b"`. Rename the test to `dashboard_detach_restores_terminal_then_exits_promptly_with_final_message` unchanged in name but with a comment recording that `Escape` no longer quits.
- `tests/e2e/browser_lab.py` and `tests/e2e/test_hook_chaos.py`: every `wait_for("Active")` and `if "Active" in screen` becomes `"Sessions"`. `stop_from_dashboard` in `browser_lab.py` must send `b"\t"` first to move focus from Prompt to Sessions, then `b"e"` instead of `b"\x05"`.
- Module documentation: the header comment of `crates/hel-tui/src/lib.rs` ("Full-screen dashboard and session picker for Hel") and of `src/hel_chat/active.rs` ("The live chat view: the conversations pane…") both describe the old split and must be rewritten.

**Acceptance.** The full validation run in the next section.

## Concrete Steps

Run everything from the repository root, `/home/jonathan/Projects/hel`.

Build and test commands. `.cargo/config.toml` defaults the build target to `x86_64-unknown-linux-musl` so the controller binary doubles as the container worker. On this machine (Linux x86_64) the default is correct; on macOS pass your host triple, for example `cargo build --target aarch64-apple-darwin`.

    cargo fmt --all
    cargo test
    cargo clippy --all-targets -- -D warnings

`cargo test` must run outside the restricted sandbox with elevated permissions. The suite opens loopback TCP and Unix sockets; a sandboxed run fails with `EPERM` or hangs and is not a valid result.

To iterate on one crate while a milestone is in flight:

    cargo test -p hel-tui
    cargo test -p hel-core hel_chat
    cargo test -p hel-cli

To see the surface by hand:

    cargo run --bin hel

Expect the workspace picker, then, after choosing a workspace, the combined surface. A run with no live session shows the Sessions pane focused and the guidance prompt.

Committing. Commit each validated milestone directly to the current branch (`master`) without pushing. Stage only the files that milestone changed; in particular leave the untracked `proptest-regressions/` directory alone. Do not create a branch and do not open a pull request.

    git add <the files this milestone changed>
    git commit -m "<milestone summary>"

## Validation and Acceptance

Acceptance is behaviour a person can see, not code that exists.

**Automated.** After Milestone 6, `cargo fmt --all` leaves the tree clean, `cargo test` passes with the new tests included, and `cargo clippy --all-targets -- -D warnings` is silent. Every test named in Milestones 1 through 5 fails before its milestone's change and passes after; that is the check that each one is testing the new behaviour rather than restating the old.

**By hand, at 140x32.** Start `cargo run --bin hel` in a 140-column by 32-row terminal in a workspace that has at least two live sessions in at least two projects.

- The surface opens with a conversation already loaded and the cursor in Prompt. The conversation is the one whose agent spoke most recently.
- Type a character. It appears in Prompt; no pane steals it.
- Press `Tab`. The Sessions pane border becomes double, it expands to show every project with a heading, and each session shows four rows.
- Press `Space`. The selected session's project collapses to one line per session; the other project stays four rows per session.
- Press `1` then `2`. Each numbered project toggles on its own; both can be collapsed at once.
- Press `Tab` twice more. Focus reaches Quota, then Targets. `r` on either shows `refreshing…`.
- Press `Tab` once more. Focus returns to Prompt and Sessions shrinks back to one line per session.
- Press `Ctrl-G`. Targets and Quota become single rows reading host names with CPU percentages and profile names with weekly percentages used. The transcript grows by the rows they gave up. The cursor stays in Prompt.
- Press `Tab`. The panes come back and focus lands on Sessions. Press `Ctrl-G`, then `Shift-Tab`: the panes come back and focus lands on Targets.
- With the panes minimized, click the `Quota` row. The panes restore and Quota has focus.
- Press `Escape` from each of the four focuses. Nothing quits. With a turn running, `Escape` in Prompt cancels the turn.
- Press `F2`. The workspace picker opens. Return, then press `F3`: the web-access dialog opens and `Escape` closes it.
- With Sessions focused, press `Enter` on another session. The transcript switches, and the draft you had typed in the first session is still there when you switch back.
- Scroll the wheel over the transcript while Sessions has focus. The transcript scrolls and focus does not move.
- Drag across transcript text, then across a Sessions row. Each copies only its own pane's rows.
- Press `Ctrl-Q`. Hel detaches within a second and prints `Active sessions will continue working; Hel will reattach to them on your next invocation.`

**By hand, at the minimum size.** Shrink the terminal to 32 columns. The surface keeps drawing, with the pane titles in their compact forms. Shrink to 31 columns: `Terminal too small` names 32 columns. Restore the width and shrink the height until `Terminal too small` appears; the number it names is the height at which the surface starts drawing again when you grow back to it.

**By hand, with no live session.** In a workspace whose sessions are all stopped, the Sessions pane has focus, the prompt band reads `No live session in this workspace.` and `Press Tab for Sessions, then n to create or s to resume.`, and `n` opens the new-session wizard.

## Idempotence and Recovery

Every step here is an ordinary source edit and can be repeated. Nothing in this plan migrates data, writes to `~/.config/hel/config.toml`, or touches Hel's SQLite database schema, so a half-finished milestone leaves no durable state to clean up: `git checkout -- <file>` or `git reset --hard <last milestone commit>` is a complete rollback.

Two ordering hazards are worth naming. First, deleting the chat's conversations pane (Milestone 5) before the Sessions pane can replace it (Milestone 3) would leave a build with no way to switch sessions; keep the milestone order. Second, `render_combined` merging the chat's `FrameSurfaces` depends on the chat clearing its own registry at the top of every `render_in`; if you skip that, stale rectangles from the previous frame accumulate and mouse clicks land on the wrong pane. The test `a_modal_replaces_every_surface_behind_it` is the guard for this.

If `cargo test` fails only in `crates/hel-cli/tests/termination_pty.rs` with a timeout waiting for the ready marker, the pane title and the marker have drifted apart; check `READY_MARKER` against the title `render_sessions` draws.

## Artifacts and Notes

The current split, for reference while editing. `crates/hel-cli/src/dashboard.rs`:

    pub(crate) enum View {
        Dashboard,
        /// Only valid while the loop holds an `ActiveChat`.
        Chat,
    }

    fn draw(&mut self) -> Result<()> {
        ...
        match (*view, active_chat.as_mut()) {
            (View::Chat, Some(chat)) => {
                terminal.terminal.draw(|frame| {
                    chat.draw(frame, transcript_selected);
                    *selection_text = draw_selection(frame, selection, chat.frame_surfaces());
                })?;
            }
            _ => {
                terminal.terminal.draw(|frame| {
                    render(frame, dashboard);
                    *selection_text = draw_selection(frame, selection, dashboard.frame_surfaces());
                })?;
            }
        }

After Milestone 5 this collapses to one arm:

    fn draw(&mut self) -> Result<()> {
        ...
        terminal.terminal.draw(|frame| {
            render_combined(frame, dashboard, active_chat.as_mut(), transcript_selected);
            *selection_text = draw_selection(frame, selection, dashboard.frame_surfaces());
        })?;

The chat's current four-band layout, which Milestone 1 generalises (`src/hel_chat/active.rs`):

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(conversations_height),
            Constraint::Min(5),
            Constraint::Length(prompt_height),
            Constraint::Length(1),
        ])
        .split(inner);

The dashboard's current three-band layout, which Milestone 4 replaces (`crates/hel-tui/src/render.rs`, `render_adaptive_dashboard`): a one-row greeting, then three panes sized by `allocate_pane_heights`, then a two-row footer.

## Interfaces and Dependencies

No configuration, database schema, daemon protocol, or external API changes. No new crate: the work fits the existing `hel` / `hel-tui` / `hel-cli` boundaries, and `hel-tui` already depends on `hel`, which is what lets the combined renderer take an `ActiveChat`.

In `src/hel_selection.rs`, add to `impl FrameSurfaces`:

    pub fn append(&mut self, other: &FrameSurfaces);
    pub fn replace_with(&mut self, other: &FrameSurfaces);

and delete `SurfaceId::Conversations`.

In `src/hel_chat.rs`, define:

    pub struct ChatRegions {
        pub transcript: ratatui::layout::Rect,
        pub prompt: ratatui::layout::Rect,
        pub footer: Option<ratatui::layout::Rect>,
        pub overlay: ratatui::layout::Rect,
    }

    pub enum ChatEventOutcome {
        None,
        Handled,
        CycleFocus { reverse: bool },
        ToggleSupportPanes,
        OpenWebDialog,
        QuitDetach { last_seen_event_ordinal: u64 },
    }

and reduce `SessionHeaderIdentity` to:

    pub struct SessionHeaderIdentity {
        pub target: String,
        pub profile: String,
    }

In `src/hel_chat/active.rs`, define on `ActiveChat`:

    pub fn desired_prompt_height(&self, width: u16) -> u16;
    pub fn draw_in(
        &mut self,
        frame: &mut ratatui::Frame,
        regions: crate::hel_chat::ChatRegions,
        prompt_focused: bool,
        transcript_selected: bool,
    );
    pub fn frame_surfaces_exclusive(&self) -> bool;

and delete `ActiveChat::draw` and `ActiveChat::focus_conversations`.

In `crates/hel-tui/src/lib.rs`, define:

    pub enum Focus { Sessions, Quota, Targets, Prompt }

    impl DashboardState {
        pub fn focus(&self) -> Focus;
        pub fn prompt_has_focus(&self) -> bool;
        pub fn focus_prompt(&mut self);
        pub fn focus_sessions(&mut self);
        pub fn cycle_focus(&mut self, reverse: bool);
        pub fn toggle_support_panes(&mut self);
        pub fn support_minimized(&self) -> bool;
        pub fn modal_open(&self) -> bool;
        pub fn set_current_session(&mut self, session_id: Option<&str>);
        pub fn open_web_dialog(&mut self) -> DashboardAction;
        pub fn chat_region(&self) -> Option<(ratatui::layout::Rect, ratatui::layout::Rect)>;
    }

In `crates/hel-tui/src/combined.rs`, define:

    pub fn render_combined(
        frame: &mut ratatui::Frame,
        dashboard: &mut DashboardState,
        chat: Option<&mut hel::hel_chat::ActiveChat>,
        transcript_selected: bool,
    );

re-exported from `crates/hel-tui/src/lib.rs` as `pub use crate::combined::render_combined;`.

In `crates/hel-cli/src/dashboard.rs`, delete `enum View` and the `DashboardContext::view` and `opening_chat_focus_conversations` fields; add `startup_summaries_pending: BTreeSet<String>`, `startup_open_pending: bool`, `startup_deadline: std::time::Instant`, and `fn maybe_open_startup_session(&mut self)`.
