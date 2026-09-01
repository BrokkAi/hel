# Cross-harness cumulative turn review in the second-opinion split

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds. Maintain this document in accordance with `.agents/PLANS.md` at the repository root.

## Purpose / Big Picture

After this change, a Hel user can have every completed coding turn reviewed by a second AI agent running a *different* harness (for example: Codex writes the code, Claude reviews it), watch that review happen live in the existing split pane, and either forward the findings to the primary agent as a corrective prompt, dismiss them, or cancel the review at any time. Review is synchronous and visible: the user's prompt input is held while a review runs, so review results never land "out of the blue" mid-conversation. Review is cumulative: if the user cancels the review of turn X, the next review covers turns X and Y together, because the reviewed-through baseline only advances when a review completes.

The review engine is ported ("cp and adapt") from the sibling repository at `../mjolnir` (absolute path on this machine: `/home/jonathan/Projects/mjolnir`), whose `mj-agents/src/discrete_review.rs` implements a supervised multi-lane review: six read-only specialist reviewer agents ("lanes": control flow, duplication, error handling, dead code, tests, contracts), a supervisor that launches them and synthesizes a verdict, and a cheaper "quick tier" (one general reviewer plus a validator). The lanes and supervisor appear as live rows in Hel's review pane.

To see it working after full implementation: open a Hel session, enable auto-review for the workspace, ask the primary agent to make a code change; when the turn finishes, the split pane opens, lanes light up and report, a verdict appears, and the composer offers Forward / Dismiss. `cargo test` covers the trigger gating, delta capture, prompt rendering, and verdict parsing.

## Progress

- [x] (2026-08-31 16:30Z) Research complete: mj porting inventory and hel integration-point map produced; all design decisions below reflect verified code, with file:line citations checked against both repos.
- [x] (2026-08-31 16:45Z) ExecPlan authored.
- [x] (2026-09-01 03:55Z) Milestone 1: cumulative delta capture in the worker (git tree snapshots, `CaptureDelta`/`AdvanceBaseline`/`AnalyzeDelta` requests, Bifrost pinned into the container image).
- [x] (2026-09-01 03:55Z) Milestone 3 (brought forward): `src/hel_review/{lanes,verdict,delta,bifrost}.rs` carry the full mj port with its tests. Done before Milestone 2 because Milestone 2 needs the same prompts; Milestone 2 is now wiring rather than authoring.
- [ ] Milestone 2: quick-tier review end-to-end in the split pane (trigger, lock, quick reviewer + validator, Forward/Dismiss/Cancel, baseline advance, workspace toggle, manual trigger).
- [ ] Milestone 4: extended tier (multi-role sidecar, supervisor with `call_review_subagents` MCP dispatch, intent analyst, lane strip in the pane).
- [ ] Milestone 5: recovery semantics, docs note in `.agents/docs/`, retrospective.

## Surprises & Discoveries

- Observation: `GitCommand` (`src/hel_archive/git.rs:10`) had no per-command environment, and a review capture cannot be written without one: staging a tree without touching the real index requires `GIT_INDEX_FILE`, which Git reads only from the environment. The field was added (`env: Vec<(OsString, OsString)>`) and the eight existing construction sites updated; `SystemGit::run` applies it after the non-interactive defaults so a caller cannot accidentally re-enable a credential prompt.
  Evidence: `capture_worktree_tree` in `src/hel_archive/git.rs`; test `review_capture_sees_tracked_modified_and_untracked_changes_without_touching_the_index` asserts `git status --porcelain` is unchanged by a capture.
- Observation: `git update-ref` accepts a tree object under `refs/hel/*`. The "trying to write non-commit object to branch" refusal applies to `refs/heads/*` only, so the capture pin works as the plan assumed.
  Evidence: the same test asserts `git rev-parse refs/hel/review-capture` equals the captured tree id.
- Observation: Bifrost 0.10.7 (the latest crates.io release of `brokk-bifrost`) pins Rust 1.97.1, while Hel pins 1.96.0. The container image therefore builds it in a separate `FROM rust:1.97.1-trixie AS bifrost` stage and copies only `/usr/local/bin/bifrost` into the final image, which also keeps Bifrost's build inputs out of the shipped layers.
  Evidence: `/home/jonathan/Projects/bifrost/rust-toolchain.toml` names 1.97.1; `containers/Containerfile.agent-dev` lines 1-25.
- Observation: mj's `RawDiffSummary::diffstat` ends every line with "(raw Git patch; Bifrost analysis disabled)", which was true in mj because that summary only appeared when analysis was off. In Hel it is the worker's ordinary diffstat for `RepoDelta` metadata and Bifrost always runs, so the suffix was dropped -- the one deliberate wording change in the ported summary.
  Evidence: `src/hel_review/delta.rs`, test `a_raw_diff_summary_counts_files_and_changed_lines`.
- Observation: Hel's second-opinion reviewer is a single-slot sidecar. `ReviewerSidecar` (`src/hel_worker_runtime/reviewer.rs:92`) holds `running: Option<RunningReviewer>`; the relay session id is the fixed string `format!("{session_id}-reviewer")` (`reviewer.rs:77`), the staging directory constants are singular (`REVIEWER_DIR = "reviewer"`, `src/hel_worker_runtime.rs:15`), and the DB row is one-per-session (`second_opinion_reviews.session_id` PRIMARY KEY, `src/hel_database/schema.rs:455`). Nothing structurally forbids several concurrent ACP children in one worker, but every identifier must gain a role dimension for multi-lane review.
- Observation: the relay journal has no bounded range read. `DurableRelay::events_after(after_ordinal, after_digest)` (`src/hel_worker.rs:484`) is cursor-forward only; an X..Y window must be truncated controller-side.
- Observation: no tree-hash or per-turn-diff helper exists anywhere in hel. Checkpoints capture whole-session archives (`src/hel_checkpoint.rs`); `collect_git_snapshot` (`src/hel_archive/git.rs:311`) captures staged/unstaged/untracked state but never computes a content id of the working tree.
- Observation: ACP-delivered MCP servers are not uniform across harnesses. `new_session_request` attaches `mcp_servers` (`src/hel_acp.rs:225-230`), but Claude is excluded from ACP delivery (`src/hel_acp.rs:187`) and Kimi receives MCP by patching the staged profile's `mcp.json` instead (`ProjectMemoryMcpDelivery::HarnessProfile`, `src/hel_controller/worker_binary.rs:493-545`). Any MCP server for the review supervisor must reuse this per-harness delivery mechanism.
- Observation: hel's MCP servers are hand-rolled JSON-lines stdio loops, not an SDK. `run_mcp_stdio` (`src/hel_project_memory.rs:868`) implements `initialize`/`tools/list`/`tools/call` directly; there is no `rmcp` dependency in the workspace. The mj review tool server uses `rmcp` and must be re-expressed in hel's pattern.
- Observation: mj's `FanoutConfig::supervisor` doc comment ("the primary agent's model", `mjolnir/mj-agents/src/discrete_review.rs:263-265`) is stale; all callers pass the resolved *review* seat. Do not copy that comment.
- Observation: hel's container image is node-based, so requiring Bifrost is cheap. `containers/Containerfile.agent-dev` builds `FROM node:24-trixie` layers (line 1, copied at line 16) and already runs a global `npm install` step (line 51); adding the pinned `@brokkai/bifrost` there removes any network fetch at review time.
- Observation: mj runs the intent analyst concurrently with the Bifrost analysis tasks, not serially. `run_async` starts `start_analyze_diff_task` in the background (`discrete_review.rs:917-926`) before launching and awaiting the intent analyst (`:929-1010`); the supervisor waits on the intent brief only because the brief is embedded in `supervisor_prompt` — a data dependency. Preserve that shape.
- Observation: Bifrost is a Rust workspace, not an npm tool. It is checked out at `/home/jonathan/Projects/bifrost` (~1.1M lines of first-party Rust across 18 crates plus a root facade binary); the npm package `@brokkai/bifrost` is only its distribution wrapper (`npm/` dir), which is why mj — lacking the checkout — consumes it via npx. Hel builds the binary from source instead.
- Observation: the review toolsets never touch the policy engine at runtime. The slopcop detectors live in `bifrost-analysis/src/code_quality/` (cognitive.rs, structural_clone_smells.rs, clone_detection.rs) and `analyze_diff` in `bifrost-analysis/src/diff_analysis.rs` — not in `bifrost-policy` (118,862 lines). `bifrost-mcp` (19,040 lines) references policy in only 12 places, essentially all in `mcp_extended.rs`, the `extended` toolset; the registry (`crates/bifrost-mcp/src/mcp_registry.rs:66-88`) expands `core` to `symbol`,`workspace`,`diff`, so `--mcp core|slopcop` never exposes a policy tool. Policy (plus `bifrost-rql`, 95,353 lines, which also reaches the build via `bifrost-runtime`) is compile-time ballast for the review use, not runtime surface.

## Decision Log

- Decision: review is synchronous — while a review runs, the review view owns the screen and prompt submission to the primary is refused; queued-prompt races are avoided by only triggering when the queue is empty.
  Rationale: the user explicitly rejected asynchronous review ("review lands out of the blue is just confusing") and asked for the same lock plan-review has. Plan-review's lock is two mechanisms (TUI input capture via `second_opinion_active()` at `src/hel_chat.rs:1378`, plus the primary parked on an unanswered elicitation); only the first transfers to turn completion, so the queue gate replaces the second.
  Date/Author: 2026-08-31 / jbellis + Fable.
- Decision: review is cumulative-since-last-*completed*-review. The baseline (per-repository git tree ids plus a transcript ordinal) advances when a review reaches a verdict and the user acts on it (Forward or Dismiss) or it auto-releases clean; cancel does not advance it.
  Rationale: user requirement — cancelling the review of turn X must fold X into the next review.
  Date/Author: 2026-08-31 / jbellis.
- Decision: reuse hel's second-opinion plumbing (profile staging, reviewer sidecar, relay transport, `ReviewerPane`, waterfall reviewer selection, SQLite `ReviewerDefaults`) rather than porting mj's host-side `ProgrammaticPool` spawn kernel. Review agents run inside the session's worker container like the existing reviewer.
  Rationale: the plumbing is already cross-harness and already visible in the TUI; porting mj's pool would drag mj's roster/seat/quota concepts into hel, which is exactly the configuration complexity the user wants to avoid.
  Date/Author: 2026-08-31 / jbellis + Fable.
- Decision: port mj's prompts, lane roster, tier structure, supervisor loop, and verdict contract as directly as possible ("cp and make minor changes"), with Bifrost as a required, first-class component — no enable flag, no degraded mode. Hel consumes Bifrost as a pinned crates.io release: `cargo install brokk-bifrost@<version> --locked` in a Containerfile builder stage (the facade crate ships the `bifrost` binary) — not via npm, not a git commit. Lanes get the `core|slopcop` MCP toolset; the supervisor, validator, and quick reviewer get `core`; the `analyze_diff` pre-pass always runs; any Bifrost failure (missing from image, spawn failure, analysis error or timeout) resolves the review as Failed with an actionable message and never advances the baseline. mj's `bifrost_analysis` flag and raw-diff prompt branch are not ported; `RawDiffSummary` survives only as the worker's local diffstat calculator for `RepoDelta` protocol metadata. The policy engine stays in the build but is unreachable at runtime (`--mcp core|slopcop` never selects the `extended` toolset); a follow-up upstream patch feature-gating `mcp_extended` and the `brokk-bifrost-policy` dependency (12 references) is deferred until image build time or binary size is measured to hurt — cutting it saves ~119K of ~1.1M lines of compile input and changes no runtime behavior, and `bifrost-rql` would remain either way via `bifrost-runtime`'s dependency on it. Version pinning mirrors mj's discipline (mj pins an exact version — `DEFAULT_PINNED_VERSION`, `mjolnir/mj-core/src/bifrost.rs` — because a moving `latest` once shipped a performance regression to every review); the pin is bumped only deliberately, and there is no version configuration.
  Rationale: the lane detectors are the lanes' identity — the roster was distilled from slop-cop's code-review pack around these instruments — and the maintainer rejects fallback paths ("fix the source of a problem"): one path with instruments, failing loudly, beats two paths or one weak path. Requiring Bifrost matches mj's default behavior (`bifrost_analysis: true`), minimizing divergence for a future re-merge. mj consumes Bifrost via npm because it lacks the checkout; hel's image build installs the crates.io release, which the maintainer prefers.
  Date/Author: 2026-08-31 / jbellis + Fable.
- Decision: the intent analyst runs concurrently with the `analyze_diff` tasks (mj's own shape), and its failure fails the review loudly instead of proceeding with an "intent unavailable" context. Do not serialize any concurrent step to make a failure path cheaper; in the rare intent failure, the concurrent analysis work is discarded.
  Rationale: maintainer directive — prefer occasionally-wasted concurrent work over added latency on every review; and fail-fast removes the one remaining degraded-mode branch in the ported design. mj tolerated intent failure because its review was invisible and an aborted review meant a silently unreviewed turn; hel's review is visible and cumulative, so a Failed verdict costs one keypress to retry and loses no coverage.
  Date/Author: 2026-08-31 / jbellis.
- Decision: keep mj's supervisor-directed lane dispatch via an MCP tool (`call_review_subagents`), implemented as a new `hel worker review-mcp` stdio server (hand-rolled, following `run_mcp_stdio`) that forwards dispatch requests to the worker runtime over a Unix socket in the worker root.
  Rationale: the supervisor choosing which specialists to launch mid-turn is the core economy of mj's extended tier; a text-protocol substitute would force the supervisor to end its turn to launch lanes. Hel already runs worker-binary stdio MCP servers (`hel worker memory-mcp`) and already attaches them per harness, so the pattern is precedented. mj's loopback-TCP `BridgeServer` is not ported; a Unix socket inside the container is simpler.
  Date/Author: 2026-08-31 / Fable.
- Decision: persistent configuration is two workspace-scoped values (auto-review on/off, tier quick/extended) in a new SQLite table, plus the existing per-workspace reviewer selection. Nothing is added to `config.toml`, `HarnessProfile`, or the session wizards. mj's `correction_threshold` and `max_correction_rounds` are not ported.
  Rationale: user constraint ("don't make profile/session configuration ridiculously complex"). Visibility replaces policy: mj needs threshold/rounds because its loop is autonomous; with the human watching the pane and choosing Forward/Dismiss, those knobs are user actions. A forwarded correction is an ordinary turn and gets re-reviewed naturally, so the re-arm loop needs no bound.
  Date/Author: 2026-08-31 / jbellis + Fable.
- Decision: per-session review state (baselines, in-flight review) lives in a new session-keyed SQLite table, not on `SessionRecord`.
  Rationale: `SessionRecord` is `#[serde(deny_unknown_fields)]` (`src/hel_state.rs:791`) with nine construction sites and a state-file compatibility surface; the existing `second_opinion_reviews` table (`src/hel_database/schema.rs:455`) is precedent for session-keyed review state in the database.
  Date/Author: 2026-08-31 / Fable.
- Decision: an in-flight review does not survive a daemon restart. On recovery it is marked cancelled, cleared, and the baseline is not advanced, so the next review covers the same changes.
  Rationale: the cumulative rule makes cancellation lossless; persisting a half-run multi-agent fanout across restarts is complexity with no user payoff.
  Date/Author: 2026-08-31 / Fable.

## Outcomes & Retrospective

(To be written at milestone completions.)

## Context and Orientation

Hel is a Rust workspace (crates in `crates/`, main library code in `src/`) that runs coding-agent harnesses (Codex, Claude Code, Kimi Code, Grok Build, Deepseek — the closed enum `HarnessKind`, `src/hel_config.rs:74`) inside per-session worker containers. Terms used throughout this plan:

- The *controller* is the host-side process (TUI or daemon) that owns configuration, the SQLite database, and session lifecycle. The *worker* is the `hel` binary running inside the session's container (`src/hel_worker_runtime/unix.rs`), reached over a *relay*: an append-only, digest-chained event journal (`src/hel_worker/journal.rs`) plus a request protocol (`src/hel_worker/protocol.rs`). Every journal event carries an `ordinal` and `digest`.
- A *harness profile* is a named (harness kind, config home) pair in `config.toml` (`HarnessProfile`, `src/hel_config.rs:314`). A *workspace* groups sessions; per-workspace reviewer preferences already persist in SQLite (`ReviewerDefaults`, `src/hel_second_opinion.rs:516`, table at `src/hel_database/schema.rs:439`).
- *ACP* (Agent Client Protocol) is the stdio JSON-RPC protocol hel speaks to each harness through per-harness bridge binaries (`bridge_launch`, `src/hel_controller/worker_binary.rs:868`). `src/hel_acp.rs` drives it; `RuntimeEvent::PromptFinished` (`src/hel_acp.rs:2032`) marks the end of a prompt turn, journaled as `RelayObservation::CommandCompleted { outcome: RelayCommandOutcome::Prompt { .. } }` (`src/hel_worker/snapshot.rs:432,491`).
- The *second-opinion reviewer* is hel's existing cross-harness review feature: the controller stages a copy of another profile's home into `<worker_root>/reviewer/profile` (`stage_reviewer_profile`, `src/hel_controller/reviewer.rs:27`), the worker's *reviewer sidecar* (`src/hel_worker_runtime/reviewer.rs`) spawns `hel worker acp-supervisor --spec …` which execs the ACP bridge for that harness, and the controller drives it with `ReviewerAction::{Start,Submit,Attach,Acknowledge,Status,RespondElicitation,Pause}` (`src/hel_session_manager.rs:333`) over the primary session's relay connection. Its UI is the *split pane*: `SecondOpinion`/`ActiveReview`/`ReviewerPane` in `src/hel_chat/second_opinion.rs`, laid out 50/50 by `render_in` (`src/hel_chat/active.rs:1799`), with the reviewer transcript rendered through the primary transcript renderer (`render_entry_rows`). Today its only trigger is the plan-approval elicitation; this plan adds a second, independent trigger at turn completion. Reviewer selection UI is the *waterfall* (`ReviewerSetup`: profile → model → effort, `src/hel_second_opinion.rs:115`).
- *Queued prompts* typed during a running turn are stored durably in the relay snapshot (`RelaySnapshot.queued_prompts`, `src/hel_worker/snapshot.rs:584`) and auto-promoted when the session goes idle (`DurableRelay::promote_next_queued_command`, `src/hel_worker.rs:1650`); the TUI mirror is `ChatState.queued_prompts` (`src/hel_chat.rs:301`).

The mj source being ported lives at `/home/jonathan/Projects/mjolnir/mj-agents/src/discrete_review.rs` (5,335 lines; production code ends at line 2946, tests follow). Its structure, verified for this plan:

- *Lane data*: `ReviewLane { id, label, focus, bifrost_tools, guidance }` (`:155`), `REVIEW_LANES: [ReviewLane; 6]` (`:166-242`, ids `control_flow`, `duplication`, `error_handling`, `dead_code`, `tests`, `contracts`), `QUICK_LANE` (`:244-257`, id from `mj_core::workflow::QUICK_REVIEWER_LANE_ID`, label "General").
- *Shared prompt fragments* (`:96-121`): `INTENT_PREAMBLE`, `REVIEWER_PREAMBLE`, `SUPERVISOR_PREAMBLE`, `VALIDATOR_PREAMBLE`, `DIRECT_INTENT_CONTEXT`, `QUICK_INTENT_CONTEXT`, `REVIEW_ORACLE` (derive expectations from requirements and sibling code, never from the change's own tests), `QUALIFICATION_GATES` (what a finding must clear), `PRIORITY_FINDING_CONTRACT`, `CLEAN_SENTINEL = "No material findings."`, `LANE_CLEAN_SENTINEL = "No findings."`.
- *Prompt builders*: `lane_context` (`:2760-2779`; emits `<original_task>`, `<review_oracle>`, `<workspace_diff>` bounded to 96 KiB, optional `<corrective_pass_context>`, `<trajectory>` bounded to 16 KiB), `lane_prompt` (`:2855-2913`), `supervisor_prompt` (`:2075-2140`; adds `<primary_user_messages>`, `<intent_brief>`, `<initial_result>`, change packet), `quick_review_prompt` (`:1679-1724`), `quick_validation_prompt` (`:1726-1755`; wraps reviewer output in `<reviewer_findings trust="untrusted; verify each against source">`), `intent_prompt` (`:2829-2837`), `user_messages_packet`, `should_extract_intent` (`:2819-2827`), `review_pass_context` (`:2027-2073`), size-bounding helpers (`:2914-2946`).
- *Quick tier* (`run_quick`, `:1358-1545`): one general reviewer; if its report is clean (`lane_report_is_clean`, `:1664`), the verdict is Clean with no validator; otherwise a single-turn validator (run on the supervisor's model) verifies the findings against source. The validator-skip on clean is the tier's whole economy.
- *Extended tier* (`run_async`, `:870-1341`): optional intent analyst (skipped when one governing user message equals the task), then a supervisor session with an MCP tool `call_review_subagents` whose schema-enforced input is `{ reviewers: [{ agent_type: <lane id enum>, hypothesis: String }] }` (`:322-335`), validated for empties/duplicates (`ReviewDispatch::validate`, `:354-376`). Lane reports return asynchronously and are injected into the supervisor as follow-up turns (`drive_supervisor`, `:1825-1972`, using `format_report_injection`); the supervisor may not conclude while launched reviewers are outstanding. Lanes are read-only and step-budgeted by prompt text only (`WORKER_TOOL_STEP_BUDGET = 12`, quick 16).
- *Verdict*: `synthesis_verdict(text)` (`:2721-2747`) — empty → Failed; last non-empty line equals `CLEAN_SENTINEL` (case-insensitive, `*` trimmed) with no `[P0]`..`[P3]` marker → Clean; anything else → Findings (malformed output degrades toward Findings, never Clean). Contract types `ReviewVerdict`/`ReviewOutcome`/`ReviewPassEvidence` are plain data in `mjolnir/mj-core/src/orchestrator_contract.rs:285-500`.
- *Bifrost* (ported as a required component): a first-party semantic-diff and code-smell tool — a Rust workspace checked out at `/home/jonathan/Projects/bifrost`, whose npm package `@brokkai/bifrost` is only a distribution wrapper; hel bakes the `bifrost` binary of a pinned crates.io release (`brokk-bifrost`) into the container image. It serves two roles in mj, both kept: an `analyze_diff` pre-pass producing a changed-functions/symbols packet for the supervisor's change packet (one-shot CLI, 600-second budget, `ANALYZE_DIFF_TIMEOUT` at `:70`), and per-session MCP analyzer/navigation tools (`bifrost --root <repo> --mcp <toolset>`): lanes use toolset `core|slopcop` (each lane's `bifrost_tools` — complexity metrics, clone detection, exception-handling/dead-code/test-assertion smells, comment density — `:127`, `:168-241`), while the supervisor, validator, and quick reviewer use `core` (`:128`, `:132`). The supervisor and validator prompts mandate calling at least one analyzer tool before a verdict (`:2119`, `:1748`) — keep those sentences. mj's `bifrost_analysis` enable flag and its raw-diff branch (`:1013-1043`) are NOT ported: hel has one path, Bifrost, and Bifrost failure fails the review loudly. `RawDiffSummary::from_patch` (`:2159-2223`), mj's pure unified-diff parser, is ported only as the worker-side diffstat calculator for `RepoDelta` metadata, never as a prompt path.

## Plan of Work

The work proceeds in five milestones. Each is independently verifiable; the feature is usable from Milestone 2 onward with a single reviewer, and gains the lane fanout in Milestone 4.

### Milestone 1 — cumulative delta capture in the worker

Goal: the controller can ask the worker "what changed since these baselines?" and "record the current state as the new baseline", including untracked files, without touching the repository's real index.

Add to `src/hel_archive/git.rs` a helper `capture_worktree_tree(git: &dyn GitCommandRunner, repository: &Path) -> Result<String>` that builds a temporary index (set `GIT_INDEX_FILE` to a temp file in the repository's git dir), runs `git add -A` against that temp index only, then `git write-tree`, returning the tree id. This never modifies the real index or working tree; the existing `SystemGit` runner (`src/hel_archive/git.rs:37`) with `NON_INTERACTIVE_GIT_ENV` executes the commands. Also add `diff_between_trees(git, repository, base: Option<&str>, current: &str) -> Result<String>` running `git diff --binary --no-ext-diff <base> <current>` (when `base` is None, diff against the empty tree via `git hash-object -t tree /dev/null`'s well-known id, computed once with `git mktree </dev/null` to stay hash-agnostic). After each capture, point a ref `refs/hel/review-capture` at the tree (`git update-ref`) so objects survive gc; on baseline advance, update `refs/hel/review-baseline`.

Extend the reviewer protocol (`ReviewerRequest`/`ReviewerResponse` in `src/hel_worker_runtime/reviewer.rs:144` area and `src/hel_worker/protocol.rs:128` area) with two requests handled by the worker runtime, using the same repository-discovery the checkpoint path uses:

    CaptureDelta { baselines: BTreeMap<PathBuf, String> }
      -> Delta { repositories: Vec<RepoDelta> }
    AdvanceBaseline { trees: BTreeMap<PathBuf, String> } -> Ok

    struct RepoDelta { root: PathBuf, baseline_tree: Option<String>,
                       current_tree: String, patch: String,
                       diffstat: String, changed_lines: usize }

Patch text is bounded worker-side to the ported 96 KiB lane-diff limit with a truncation marker; diffstat and counts come from the ported `RawDiffSummary` applied to the untruncated patch. An empty `patch` across all repositories means "nothing to review".

Baseline initialization: when auto-review is enabled for a workspace (or a session launches/resumes with it already enabled), the controller immediately issues `CaptureDelta` with empty baselines and stores the returned `current_tree`s via `AdvanceBaseline` semantics — review coverage starts at the moment of enablement, which keeps pre-existing dirt out of the first review.

Bifrost in the image and in the worker: pin a crates.io release (`ARG BIFROST_VERSION=<version>` near the existing `ARG NODE_IMAGE` in `containers/Containerfile.agent-dev`, line 1) and install it in a builder stage with `cargo install brokk-bifrost@${BIFROST_VERSION} --locked` — the `brokk-bifrost` facade crate ships the `bifrost` binary — then copy it onto the image `PATH`. crates.io releases only; never a git branch or commit, and bumped only deliberately. Add a third reviewer-protocol request, `AnalyzeDelta { … } -> ChangedFunctions { packet: String }`, which runs `bifrost analyze_diff` in the repository root as a supervised, cancellable background task through the shared subprocess helpers (it can take minutes on large changesets; port mj's timeout and retry from `discrete_review.rs:2461-2616` and the packet formatting from `format_changed_functions`, `:2618-2657`). A missing or failing Bifrost binary is a request error the driver surfaces as verdict Failed with a message naming the image rebuild as the fix — never a silent downgrade.

Validation: a unit/integration test in the git module creates a temp repository, writes tracked, modified, and untracked files totaling more than 64 KiB of patch data (the repository rule: pipe-crossing code must be exercised past the 64 KiB pipe buffer), captures a baseline, mutates the tree, and asserts the delta patch contains all three change classes and that the real index and worktree are untouched (`git status --porcelain` unchanged by capture).

### Milestone 2 — quick-tier review end-to-end

Goal: with auto-review on, finishing a coding turn opens the split pane, runs mj's quick tier (one general reviewer, then a validator only if findings), and ends in Forward / Dismiss / auto-release; the user can cancel at any time; prompts are locked throughout; `/review`-style manual trigger works.

New module `src/hel_review.rs` with submodules `src/hel_review/{lanes.rs, verdict.rs, delta.rs, driver.rs}`. `lanes.rs` and `verdict.rs` are ported in Milestone 3; for this milestone bring over the minimum: `QUICK_LANE`, the shared fragments, `quick_review_prompt`, `quick_validation_prompt`, `lane_context`, `synthesis_verdict`, `RawDiffSummary`, sentinels, bounding helpers.

Trigger, controller-side, in `ActiveSession` next to the existing plan-review hook (`advance_review`, `src/hel_chat/active.rs:693`): when the session's phase transitions to `WorkerPhase::Idle` after a prompt turn (`WorkerEvent::TurnCompleted`, `src/hel_chat.rs:1746`), and all of the following hold — workspace auto-review enabled, `ChatState.queued_prompts` empty, no second-opinion or turn review active, session not archived — issue `CaptureDelta` against the stored baselines. If every repository's patch is empty, do nothing. Otherwise open the review view and start the quick reviewer. The manual trigger is a new `ChatAction::StartTurnReview` reachable from the session menu and a keybinding, allowed only when the phase is Idle; it runs even when auto-review is off, and reuses the same path.

Reviewer selection: reuse the existing waterfall (`ReviewerSetup::new`, invocation pattern at `src/hel_chat/active.rs:1137-1176`) and `ReviewerDefaults` persistence unchanged. When a remembered reviewer exists for the workspace, skip straight to launch, exactly as second-opinion's auto-resume path does.

The lock: add `ChatState.turn_review: Option<TurnReview>` in a new `src/hel_chat/turn_review.rs` modeled closely on `src/hel_chat/second_opinion.rs` — same "view owns the screen" key routing (a `turn_review_active()` check beside `second_opinion_active()` at `src/hel_chat.rs:1378`), same 50/50 split rendering through `ReviewerPane` (which is reusable as-is: it folds the reviewer's relay events through the primary projection and renders with `render_entry_rows`). Prompt submission sites that currently refuse while not idle (`src/hel_chat.rs:1257,1312,1427`) additionally refuse while a turn review is unresolved, as does the web control surface's submit path in `src/hel_server.rs`. Turn review and plan second-opinion are mutually exclusive by construction (one triggers only at Idle, the other only mid-turn at an elicitation); add a debug assertion. Per the repository's UI rule, all of this stays non-blocking: capture, launch, and polling run as supervised background operations feeding UI state, and cancellation is available the entire time.

Flow, driven by a small state machine in `src/hel_review/driver.rs` (states: CapturingDelta → LaunchingReviewer → ReviewerRunning → ValidatorRunning → Verdict{Clean|Findings|Failed} → Resolved{Forwarded|Dismissed|Cancelled}):

MCP attachment for reviewer sessions (needed here, reused in Milestone 4): the reviewer launch currently passes `project_memory: None` (`src/hel_worker_runtime/reviewer.rs:304`), so give `LaunchSpec`/`AcpSupervisorSpec` a general MCP server list and deliver it per harness kind exactly as project memory is delivered (`session/new` `mcp_servers` where ACP delivery works, staged-profile `mcp.json` patch for Kimi, and the Claude-appropriate staged-profile mechanism, following `ProjectMemoryMcpDelivery` at `src/hel_controller/worker_binary.rs:427-545`). The quick reviewer and validator sessions get one server: `bifrost --root <repo> --mcp core`.

1. Quick reviewer: `ReviewerAction::Start` with the staged reviewer profile (existing single sidecar slot; the only worker changes are Milestone 1's and the MCP list above), then `Submit` the rendered `quick_review_prompt` — mj's text as-is, including its analyzer-tool mandate, since Bifrost navigation is attached. Start the `AnalyzeDelta` request concurrently with the reviewer launch; its changed-functions packet is not needed until validation, so nothing waits on it unless findings appear. The prompt's `<original_task>` is the session's first user message; `<primary_user_messages>` are the user messages with transcript positions after the stored reviewed-through ordinal, read from the materialized session (`TranscriptItem.position`); the diff is Milestone 1's patch.
2. If the reviewer's final answer is clean (`lane_report_is_clean` port), the verdict is Clean: show a one-line notice in the primary transcript, auto-close the pane, advance the baseline (`AdvanceBaseline` with the captured trees plus the current last-user-item ordinal, persisted in the new table), release the lock. No validator runs — port this skip; it is the tier's economy.
3. Otherwise await the `AnalyzeDelta` result (a failure here fails the review loudly), launch the validator as a fresh sidecar start on the same slot (a non-reusable `ReviewerLaunchConfig` generation forces a fresh session, pausing the reviewer — the sidecar already does this, `reviewer.rs:225-229`), and `Submit` the rendered `quick_validation_prompt` wrapping the reviewer's findings as untrusted evidence, with the change packet built from the analysis. Parse its answer with `synthesis_verdict`.
4. Findings: the action bar (modeled on `SplitAction`, `src/hel_chat/second_opinion.rs:54`) offers `Forward findings` and `Dismiss`. Forward submits the synthesis to the primary as an ordinary prompt, prefixed with a short ported corrective preamble stating these are validated review findings on the just-completed change; the corrective turn is then eligible for review like any turn. Both Forward and Dismiss advance the baseline and release the lock. `Esc` is Cancel at every state before Resolved: pause/reap the sidecar sessions, do not advance the baseline, release the lock. Failed verdicts (empty/errored reviewer output) present as findings-with-failure-notice plus Dismiss; they do not advance the baseline.

Persistence: one migration adding two tables —

    turn_review_settings(workspace_id TEXT PRIMARY KEY,
                         auto_review INTEGER NOT NULL,
                         tier TEXT NOT NULL) STRICT
    turn_review_state(session_id TEXT PRIMARY KEY,
                      baselines TEXT NOT NULL,      -- JSON {repo path -> tree id}
                      reviewed_through_ordinal INTEGER NOT NULL,
                      active TEXT) STRICT           -- JSON in-flight snapshot or NULL

with accessors in `src/hel_database.rs` following the `reviewer_defaults`/`active_review` patterns (`:2371`, `:2451`). The settings UI is a small two-row dialog (auto-review on/off, tier quick/extended) added to the session menu next to the existing rename dialog pattern (`crates/hel-tui/src/dialogs.rs:1280`); nothing is added to `config.toml`, `HarnessProfile`, or the wizards.

Validation: state-machine unit tests in `src/hel_chat/turn_review.rs` (`#[cfg(test)]`, descriptive names per house style), covering at minimum: `turn_completion_with_queued_prompts_does_not_trigger_review`, `cancelled_review_leaves_baseline_so_next_review_covers_both_turns`, `clean_verdict_advances_baseline_and_releases_lock`, `submission_refused_while_review_unresolved`. Manual: tmux-driven TUI run per the dev loop — make a change, watch the pane, cancel, make another change, confirm the next review's diff includes both.

### Milestone 3 — the ported prompt/lane/verdict module

Goal: `src/hel_review/lanes.rs` and `verdict.rs` contain the full mj port with its tests, so Milestone 4 is wiring rather than authoring.

Copy from `/home/jonathan/Projects/mjolnir/mj-agents/src/discrete_review.rs`, adapting only imports: the shared fragment consts (`:96-121`), `ReviewLane` + `REVIEW_LANES` + `QUICK_LANE` (`:155-257`, keeping each lane's `bifrost_tools`), the prompt builders (`:1679-1755`, `:2027-2140`, `:2760-2913`, with `bifrost_attached=true` and the analyzer mandates intact), the analyze-diff result types and formatting (`:2142-2358`, `:2618-2657`), `RawDiffSummary` (`:2159-2223`, worker-side diffstat only), verdict parsing (`:2721-2758`), the contract types from `mjolnir/mj-core/src/orchestrator_contract.rs:285-500` (`ReviewVerdict`, `ReviewPassEvidence`, `ReviewLaneEvidence`, lane-report injection formatting from `format_report_injection`, `:166-180`), the dispatch argument types and validation (`:292-376`), and `should_extract_intent`/`intent_prompt`. Port the corresponding unit tests from the test half of the file for everything taken (verdict parsing, `RawDiffSummary`, prompt rendering, dispatch validation). Keep mj's names and text wherever possible — the repositories may re-merge, so gratuitous divergence is a cost (see `.agents/docs/` note in Milestone 5). Milestone 2's minimal copies are replaced by this module.

Validation: `cargo test` (outside the sandbox, per repository rule) passes with the ported tests; a diff of the prompt consts against mj shows no drift beyond import paths.

### Milestone 4 — extended tier: lanes, supervisor, intent analyst, lane strip

Goal: with tier = extended, the supervisor launches specialist lanes it chooses, their progress is visible as a strip in the pane, and the verdict is the supervisor's synthesis.

Multi-role sidecar: generalize `ReviewerSidecar.running` from `Option<RunningReviewer>` to a map keyed by a new `role: String` ("reviewer" for the existing second-opinion path, "supervisor", "intent", and lane ids for review). Thread the role through `ReviewerRequest`/`ReviewerAction` (defaulting to "reviewer" for wire compatibility), the relay session id (`format!("{session_id}-review-{role}")`, keeping the bare `-reviewer` suffix for the default role), and the staging layout: the controller stages the profile once as today; the worker copies it per role under `reviewer/roles/<role>/profile` so concurrent harness sessions never share a config home. Cap concurrent lane children with a worker-side semaphore, `MAX_PARALLEL_LANES: usize = 3` (lower than mj's 6 — these run inside one resource-limited container; make it a const with a comment, not configuration).

Supervisor MCP dispatch: a new subcommand `hel worker review-mcp --socket <path>` in `crates/hel-cli/src/main.rs` (beside the memory-mcp arm around `:407-427`), implemented in `src/hel_review/mcp.rs` on the hand-rolled JSON-lines pattern of `run_mcp_stdio` (`src/hel_project_memory.rs:868`). It serves one tool, `call_review_subagents`, with mj's schema, and forwards each call as one JSON request over the Unix socket to the worker runtime, which validates (ported `ReviewDispatch` rules: non-empty, known lane ids, no duplicates, no post-synthesis launches), starts the requested lane roles with rendered lane prompts, and returns the started ids immediately (the tool never blocks on lane completion — copy mj's tool description forbidding polling). Delivery reuses Milestone 2's per-harness MCP attachment. Server lists per role: the supervisor gets `bifrost --mcp core` plus the review-mcp server; each lane gets `bifrost --mcp core|slopcop`; the intent analyst gets none.

Extended flow in `driver.rs`: start the `AnalyzeDelta` task and the intent analyst (role "intent", skipped per `should_extract_intent`) concurrently — mj's own shape (`run_async` starts analysis at `:917-926` before awaiting intent at `:929-1010`); the supervisor launches only after both complete, because its prompt embeds the intent brief and the change packet. Failure of either fails the review loudly, discarding the other's in-flight work — never proceed with an "unavailable" placeholder, and never serialize the two to make that rare failure cheaper. Then the supervisor (role "supervisor", MCP attached as above, `supervisor_prompt`). Lane completion is observed controller-side by polling each lane role's relay journal (the per-role generalization of `poll_reviewer_events`, `src/hel_chat/active.rs:1507`); completed lane reports are queued and injected into the supervisor as follow-up `Submit` turns using the ported injection format, with mj's two instruction variants depending on whether lanes remain outstanding, and the verdict is accepted only when no launched lane is outstanding and the queue is drained (port the `drive_supervisor` loop conditions, `:1873-1946`). Every role's cancellation path reaps its child before the review resolves — keep mj's invariant verbatim; the sidecar's pause plus process-group termination provides it.

Lane strip in the pane: extend the turn-review render path (shared with `render_reviewer`, `src/hel_chat/second_opinion.rs:676`) with a header strip inside the reviewer block — one row per active role showing label and state (pending / running / clean / findings / failed), rendered from driver state. The body shows one selected role's transcript through its own `ReviewerPane`; Tab cycles the selection among supervisor, intent, and lanes. Adjust the `(top, total)` the renderer reports so `SurfaceFrame::scrollable` and mouse hit-testing on `chat.reviewer_area` stay correct (`src/hel_chat/active.rs:1843`, `src/hel_chat.rs:1703`). Per the shared-rendering rule, the strip is viewport furniture only; transcript content continues to render through `render_entry_rows`.

Validation: driver tests with a fake sidecar (hand-written fake per house style): supervisor-requested lanes launch and complete out of order, reports inject in arrival order, verdict blocked while a lane is outstanding, duplicate lane dispatch rejected, cancel mid-fanout reaps all roles and leaves the baseline. Manual: an extended review over a multi-file change shows the strip filling in as lanes report.

### Milestone 5 — recovery, docs, retrospective

On session recovery/resume, if `turn_review_state.active` is non-null, clear it, emit a "review cancelled by restart" transcript note, and do not advance the baseline (the cumulative rule makes this lossless). Cross-harness resume (`resume.rs:529`) keeps baselines: tree ids are content ids, valid across harness swaps; `reviewed_through_ordinal` maps onto the restored canonical history. Write a short parity note in `.agents/docs/turn-review-mj-parity.md` recording exactly which mj line ranges were ported and which were deliberately not (seats/quota, workflow-graph events, correction threshold/rounds, `held_completion`, the `bifrost_analysis` flag and raw-diff prompt branch, and npm-based Bifrost consumption — hel bakes a pinned release into the image) so a future re-merge can reconcile. Fill in `Outcomes & Retrospective`.

## Concrete Steps

Work from the repository root (`/home/jonathan/Projects/hel3`). Read the cited mj code with the repository checked out at `/home/jonathan/Projects/mjolnir`; if it has moved, any checkout of the mjolnir repository at the cited file works — the line numbers date from 2026-08-31.

Build and test exactly as the repository prescribes: the default target is `x86_64-unknown-linux-musl` (`.cargo/config.toml`); run every test invocation outside the restricted sandbox with elevated permissions, because the suite uses loopback TCP and Unix sockets:

    cargo build
    cargo test
    cargo clippy --all-targets -- -D warnings

Expected: all three succeed at every milestone boundary. Commit each validated milestone on the current branch (repository rule: commit completed, validated work without waiting to be asked; stage only files you changed; never `git add -A`).

For manual TUI verification, run the built binary against a scratch project in tmux (the maintainer's standard dev loop), enable auto-review in the new menu dialog, and drive a small edit turn with the primary agent.

## Validation and Acceptance

Acceptance is behavioral:

1. With auto-review enabled and an idle session, a turn that edits files ends with the split pane opening automatically; the reviewer's activity streams in the right pane; the composer is replaced by review actions and typed input does not reach the primary.
2. A review that finds nothing closes itself, leaves one transcript line, and the next turn proceeds normally with no user action.
3. A review with findings offers Forward and Dismiss; Forward starts a primary turn whose prompt contains the synthesis; that corrective turn is itself reviewed when it completes.
4. Cancelling a review (Esc) returns the composer immediately; the next completed turn's review diff contains the cancelled turn's changes as well (verify by making change A, cancelling, making change B, and reading the next review's `<workspace_diff>` in the reviewer pane).
5. A turn finishing while prompts are queued starts no review; the review after the queue drains covers the whole batch.
6. Turns that change no files (pure Q&A) start no review.
7. In extended tier, the lane strip shows the supervisor plus each launched lane progressing to a terminal state, and Tab switches the transcript between them.
8. `cargo test` passes, including the ported mj tests and the new gating/state tests; `cargo clippy --all-targets -- -D warnings` is clean.

## Idempotence and Recovery

All steps are additive and re-runnable. The SQLite migration is guarded by the existing schema-version mechanism. Delta capture uses a temporary index file and never mutates the real index, worktree, or refs other than `refs/hel/review-capture` / `refs/hel/review-baseline` (both safe to delete; the next capture recreates them — deleting `review-baseline`'s backing row merely widens the next review). If a milestone is abandoned mid-way, the feature is inert unless the workspace toggle is on; toggling it off restores pre-feature behavior exactly. Worker/controller version skew is tolerated because the new `ReviewerRequest` variants are additive; an old worker answering a new request with an unknown-request error surfaces as a failed review, not a wedged session — the driver must treat any request error as verdict Failed with the lock released.

## Artifacts and Notes

The reviewed-turn contract, restated in one place: the *review target* is the pair (git tree delta from stored baselines to the capture taken at trigger time, primary user messages after `reviewed_through_ordinal`). The baseline advances only in `AdvanceBaseline`, which is issued exactly when a review resolves as Forwarded, Dismissed, or Clean — never on Cancel, Failed, or restart. The prompt lock spans CapturingDelta through Resolved. These three sentences are the feature's invariants; every milestone's tests exist to defend them.

Expected shape of a passing gating test run (illustrative):

    running 6 tests
    test turn_review::tests::turn_completion_with_queued_prompts_does_not_trigger_review ... ok
    test turn_review::tests::cancelled_review_leaves_baseline_so_next_review_covers_both_turns ... ok
    test turn_review::tests::clean_verdict_advances_baseline_and_releases_lock ... ok
    ...

## Interfaces and Dependencies

No new workspace crates and no new Rust dependencies in hel: MCP serving reuses the hand-rolled JSON-lines pattern, git runs through the existing `GitCommandRunner`, and child processes go through the shared subprocess helpers (repository rule; clippy `disallowed_methods` enforces it). The container image gains one binary: the pinned Bifrost release (Milestone 1). New code lives in the main library (`src/hel_review/`, `src/hel_chat/turn_review.rs`) — do not create a new workspace crate.

In `src/hel_review/driver.rs`, the driver exposes to the chat layer:

    pub enum TurnReviewPhase {
        CapturingDelta, LaunchingReviewer, Running { roles: Vec<RoleStatus> },
        Verdict(ReviewVerdict), Resolved(Resolution),
    }
    pub struct RoleStatus { pub role: String, pub label: String, pub state: RoleState }
    pub enum RoleState { Pending, Running, Clean, Findings, Failed }
    pub enum Resolution { Forwarded, Dismissed, Cancelled }

In `src/hel_review/verdict.rs` (ported): `pub fn synthesis_verdict(text: &str) -> ReviewVerdict` and `pub const CLEAN_SENTINEL: &str = "No material findings."` with mj's exact semantics. In `src/hel_archive/git.rs`: `capture_worktree_tree` and `diff_between_trees` as specified in Milestone 1. Worker protocol: `CaptureDelta`/`AdvanceBaseline` and the `role`-keyed reviewer requests as specified in Milestones 1 and 4.

Revision note (2026-08-31): initial version, authored before any implementation work from paired research passes over hel (`src/hel_second_opinion.rs`, `src/hel_worker_runtime/reviewer.rs`, `src/hel_chat/second_opinion.rs`, `src/hel_worker/*`, `src/hel_database/*`), mjolnir (`mj-agents/src/discrete_review.rs`, `mj-core/src/orchestrator_contract.rs`, `mj-core/src/config.rs`), and bifrost (`crates/bifrost-mcp`, `crates/bifrost-analysis`). All design decisions were fixed in conversation with the maintainer on 2026-08-31 and are recorded in the Decision Log.
