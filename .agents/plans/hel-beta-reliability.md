# Build a repeatable beta-reliability test program for Hel

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

This document must be maintained in accordance with `.agents/PLANS.md` from the repository root. A future contributor must be able to resume the work using only that file, this ExecPlan, and the current working tree.

## Purpose / Big Picture

Hel is useful enough to invite other Linux and WSL users, but routine dogfooding still exposes failures at the boundaries between its persistent daemon, target-side workers, durable relay journal, terminal dashboard, and web viewer. The purpose of this work is to turn those discoveries into a repeatable reliability program. After completion, every pull request will exercise deterministic multi-client behavior, scheduled runs will kill and restart real isolated processes at important durability boundaries, and a local Luna-driven tmux campaign will cover exploratory behavior that scripted tests do not anticipate.

The visible proof is not a coverage percentage. It is a test run in which two terminal dashboards and the web viewer observe one workspace, lifecycle actions remain bounded, acknowledged transcript events survive restarts exactly once, all clients converge, and no child process is leaked. Failures must produce a seed and artifacts that replay the same scenario.

## Progress

- [x] (2026-08-30 15:18Z) Inspected the existing unit, PTY, coverage, property, and process-chaos tests and measured the current coverage shape.
- [x] (2026-08-30 15:18Z) Recorded the product-scope decisions: runtime paths are the priority, mutation testing is deferred, and Luna testing remains a local runbook.
- [x] (2026-08-30 15:29Z) Milestone 1: repaired full-package coverage reporting, established a crate-qualified baseline, and fixed the asynchronous log-writer lifetime exposed by the PTY teardown assertion.
- [x] (2026-08-30 16:04Z) Milestone 2: added the isolated replayable runtime lab, made daemon state live across web/TUI clients, centralized recovery ownership, and passed the three-client lifecycle scenario twice at seed 1 plus seed 2.
- [x] (2026-08-30 16:50Z) Milestone 3: added generated multi-segment journal faults, compiled-out named crash hooks, a table-driven six-boundary process matrix, and fixed the relay projection/revision races it reproduced.
- [x] (2026-08-30 17:20Z) Milestone 4: added pinned real-browser/TUI coverage, PTY resize and stale-browser recovery, and a reproducible local Luna tmux campaign.
- [x] (2026-08-30 17:31Z) Milestone 5: established pull-request and scheduled reliability gates, validated a five-iteration local soak, and added an evidence-based beta acceptance ledger.

## Surprises & Discoveries

- Observation: the existing coverage workflow runs tests for the workspace but its later `cargo llvm-cov report` invocations report only the root `hel-core` package.
  Evidence: a root-package report contained 80 source files and no path under `crates/hel-cli` or `crates/hel-tui`. Reporting with `-p hel-core -p hel-tui -p hel-cli` included all three packages and measured 78.13% aggregate line coverage.

- Observation: the under-tested code is concentrated in the runtime boundary, not TUI rendering.
  Evidence: the full-package report measured `crates/hel-tui/src/render.rs` at 97.33% lines, while `crates/hel-cli/src/daemon.rs` was 39.72%, `dashboard.rs` 54.15%, `dashboard/actions.rs` 2.11%, `dashboard/io.rs` 13.25%, and `server.rs` 43.38%. These `hel-cli` paths implement the daemon, dashboard, and web control loops; they are not the low-priority one-shot command surface.

- Observation: coverage instrumentation exposed an actual terminal shutdown defect before producing a report.
  Evidence: `dashboard_detach_restores_terminal_then_exits_promptly_with_final_message` failed because `[tracing-subscriber] Unable to write an event ... log worker stopped` appeared after the documented final detach message.

- Observation: the PTY symptom is timing-sensitive, but its underlying writer-lifetime violation is deterministic.
  Evidence: a focused instrumented rerun passed before the fix. A new logging regression instead holds the subscriber's writer clone, drops the controller guard, and proves a subsequent record and flush still succeed. The prior implementation returned `BrokenPipe("log worker stopped")` in that exact sequence.

- Observation: adding `hel-cli` and `hel-tui` to the report lowers the honest aggregate baseline rather than indicating a new code regression.
  Evidence: the validated report now contains crate-qualified paths for all three packages and measures 78.12% aggregate line coverage. The previous 83.75% baseline described only `hel-core` even though the workflow had run all workspace tests.

- Observation: the repository-wide rustfmt check has pre-existing differences outside this milestone.
  Evidence: `cargo fmt --all -- --check` reports committed formatting changes in `src/hel_acp/tests.rs`, `src/hel_acp.rs`, `src/hel_worker/journal.rs`, `src/hel_worker/journal_chaos.rs`, `src/hel_worker/snapshot.rs`, and `src/hel_worker.rs`. The changed `hel-cli` package was formatted independently and those unrelated files were left untouched.

- Observation: Hel already has substantial deterministic coverage and one property-based journal-chaos suite.
  Evidence: the repository contains roughly 1,600 Rust tests, `src/hel_worker/journal_chaos.rs` runs 160 generated cases, and `tests/e2e/session_restart_chaos.sh` kills five real worker-side process generations in an explicitly isolated container.

- Observation: the phone server captured the workspace list at daemon startup, so a workspace created by the first setup TUI never appeared in the already-running web viewer.
  Evidence: the first lab replay authenticated successfully but `/api/snapshot` returned no workspaces. RuntimeState now publishes workspace records over a watch feed, and a focused unit test proves an existing phone subscriber receives later creation.

- Observation: publishing live worker views was insufficient to make a daemon-created session visible in existing dashboards.
  Evidence: daemon diagnostics showed a connected view and a one-session controller while both PTYs still omitted the title. Runtime snapshots did not carry `SessionRecord`s, so each dashboard discarded the view for an unknown local record. The runtime feed now installs workspace-filtered records before applying worker updates.

- Observation: recovery coordinators in every TUI and in the phone server competed with the daemon's recovery coordinator for the same checkpoint barrier.
  Evidence: the first converged transcript acquired multiple `checkpoint-*` barriers which were cancelled as client-owned proxy connections disappeared; a subsequent web close remained running with checkpoint errors. Recovery and interrupted-close ownership now live only in the daemon, which already serializes them with lifecycle operations.

- Observation: the web viewer could advertise a newly provisioned session before its remote session handle had appeared.
  Evidence: a prompt posted immediately after the snapshot first reached `running` failed with `session ... is not managed`. Phone relay actions now wait for the managed session with the same bounded five-second window used by controller lifecycle code.

- Observation: daemon and web revision counters restarted from small integers, so a client surviving daemon death could observe a lower revision or wait thirty seconds for an old cursor the new process could not reach.
  Evidence: killing at `relay_projection_before_revision_publication` moved the web feed from revision 5 to 3. Both feeds now allocate opaque cursors from one process-generation counter seeded with the daemon start timestamp; the same crash scenario remained monotonic after restart.

- Observation: two valid controller projectors can briefly overlap while a newly provisioned worker becomes visible to the daemon actor.
  Evidence: the checkpoint hook matrix reproduced `session actor received relay event 1 after its projection had already applied it`. A page that loses this race now rolls back, reloads the newer durable projection, and reattaches with its exact ordinal and digest; digest mismatch, gaps, and immutable projection changes remain integrity errors.

- Observation: fresh fake Codex homes and relative binary paths made existing chaos fixtures depend on ambient machine behavior.
  Evidence: one matrix run spawned real plugin-marketplace clone children, and `session_restart_chaos.sh ./target/.../hel` later failed after changing its child working directory. The deterministic profile now has a private PATH containing only its Python bridge, and the topology runner canonicalizes the Hel binary before launching children.

- Observation: lifecycle state initiated by a TUI could remain stale in the already-running web viewer even after checkpoint and teardown completed durably.
  Evidence: the browser/TUI scenario stopped a local-bare session in the TUI and SQLite reached `stopped`, while the phone snapshot remained `running` until timeout. Daemon lifecycle completion now reloads its durable controller before publication, and the phone control loop subscribes to daemon runtime revisions and coalesces controller reloads off its event loop.

- Observation: Chromium's offline emulation did not reliably break an already-open EventSource connection, so returning online could leave the page stale indefinitely.
  Evidence: the first repaired lifecycle replay published the stopped web revision but the browser observed no SSE update for forty seconds. The page now retains its EventSource and explicitly recreates it plus refreshes the snapshot on the browser `online` event.

- Observation: the real web New form correctly requires a project directory for local-bare targets, which the initial browser driver omitted.
  Evidence: native form validation prevented submission and the locator waited forever for a session that had never been requested. The Playwright scenario now fills the visible required directory and logs only non-secret stage names.

- Observation: the existing CI formatting job could not serve as a required gate because four committed Rust sources did not match the repository's current rustfmt.
  Evidence: `cargo fmt --all -- --check` consistently reported `src/hel_acp.rs`, `src/hel_acp/tests.rs`, `src/hel_worker.rs`, and `src/hel_worker/snapshot.rs`. Their mechanical formatting was isolated in commit `5c4b9cc`; the repository-wide formatting check then passed.

- Observation: a duration-bounded soak can finish slightly after its requested wall time because an iteration that starts before the deadline is allowed to complete and validate cleanup.
  Evidence: a one-minute validation ran seeds 500 through 504 and passed five complete three-client iterations in 65 seconds. No new iteration started after the deadline, and every completed iteration reported three clients and zero leaks.

## Decision Log

- Decision: defer all `cargo-mutants` installation, configuration, and execution from this ExecPlan.
  Rationale: mutation runs are too slow for the current feedback loop. Deterministic runtime scenarios, exact crash injection, and multi-client convergence directly target the failures found during dogfooding. Mutation testing may be reconsidered only after these faster layers mature.
  Date/Author: 2026-08-30, user and Codex.

- Decision: prioritize runtime orchestration inside the `hel-cli` package, not broad command-line coverage.
  Rationale: `daemon.rs`, `dashboard/`, `pollers.rs`, and `server.rs` own core user-visible runtime behavior despite living in the package named `hel-cli`. One-shot parsing, import wrappers, and rarely used command branches exist largely through historical inertia and are not coverage targets for this program.
  Date/Author: 2026-08-30, user and Codex.

- Decision: use a tiered cadence.
  Rationale: deterministic tests should keep pull-request feedback below approximately fifteen minutes. Disposable-process chaos, browser automation, and longer seeded soaks belong in nightly or manually dispatched jobs.
  Date/Author: 2026-08-30, user and Codex.

- Decision: use only a disposable local stack for automated integration testing.
  Rationale: local-bare targets, fake ACP bridges, loopback sockets, PTYs, TLS fixtures, and Podman can exercise the important ownership boundaries without personal credentials, paid APIs, or fragile external services. Real harnesses, AWS, SSH hosts, and Tailscale remain optional pre-release checks.
  Date/Author: 2026-08-30, user and Codex.

- Decision: keep Luna exploratory testing live and local.
  Rationale: Luna is valuable for finding unexpected interaction sequences, but making an LLM a CI dependency would add nondeterminism without improving reproducibility. A checked-in runbook will require exact keys, captures, logs, and reproduction seeds for every finding.
  Date/Author: 2026-08-30, user and Codex.

- Decision: do not adopt Loom or Shuttle across the runtime in this plan.
  Rationale: Hel's dominant failures cross process, socket, filesystem, SQLite, and terminal boundaries. Explicit barriers, fake clocks, named test hooks, and replayable scenario seeds cover those boundaries with less production abstraction. Small synchronization components can be reconsidered later.
  Date/Author: 2026-08-30, Codex.

- Decision: never use automatic retry to turn a failed reliability case green.
  Rationale: intermittent failure is the behavior under test. Every generated run must record enough state to replay its first failing seed; a subsequent successful attempt does not erase the failure.
  Date/Author: 2026-08-30, Codex.

- Decision: the persistent daemon is the sole owner of automatic recovery and interrupted lifecycle recovery.
  Rationale: remote dashboards and the phone server consume daemon-owned views and submit commands through it; independent recovery coordinators cannot share the daemon actor's leased relay connection safely and provide no additional durability authority.
  Date/Author: 2026-08-30, Codex.

- Decision: keep ordinary worker-topology chaos and named durability hooks as two specialized runners under one process-chaos layer.
  Rationale: `session_restart_chaos.sh` validates a single live worker topology across proxy, helper, bridge, supervisor, and worker deaths. `run-test-hook-chaos.sh` is table-driven across fresh full-stack roots because every durability boundary needs independent daemon, database, PTY, and web state. Combining their setup would weaken isolation and make failures harder to replay.
  Date/Author: 2026-08-30, Codex.

- Decision: automation completion does not itself declare a beta candidate ready.
  Rationale: seven scheduled runs, a current-commit 30-to-60-minute soak, and a completed Luna campaign are observations that require elapsed time and retained artifacts. `.agents/docs/beta-acceptance.md` therefore keeps the product explicitly unapproved until those fields and the stop-ship audit are complete.
  Date/Author: 2026-08-30, Codex.

## Outcomes & Retrospective

Milestones 1 through 3 are complete. Coverage now measures `hel-core`, `hel-cli`, and `hel-tui` without collapsing same-named source paths, and the baseline records the real 78.12% aggregate with reviewed runtime exceptions. The full instrumented suite passed: 65 `hel-cli` unit tests, its logging and two PTY integration tests, 1,355 `hel-core` tests with four ignored, and 197 `hel-tui` tests with one ignored. The log worker now uses a bounded flush barrier while remaining valid for the lifetime of the global tracing subscriber, so detached runtime diagnostics cannot corrupt the restored terminal.

The system lab now creates short isolated runtime roots, a disposable Git workspace, a stateful fake ACP bridge with checkpointable Codex artifacts, a local-bare worker, one persistent daemon, two 140x32 PTYs, and an authenticated loopback web client. It asserts exact-once prompt/reply projection, screen convergence, monotonic client convergence revisions, stopped lifecycle state, sub-two-second UI quit, daemon shutdown, SQLite integrity and foreign keys, and zero processes retaining the isolated environment. Failures retain runtime state and print one replay command. Two consecutive seed-1 runs produced the same logical seven-action trace; seed 2 also passed. The lab itself exposed and drove fixes for stale web workspaces, missing cross-client session records, duplicated recovery ownership, and the new-session/managed-actor race. The remaining beta gaps are richer PTY/browser and Luna campaigns plus tiered automation.

The crash layer now adds a 128-case generated multi-segment journal property (more than one thousand append/fault/recovery transitions in the validated run), with `PROPTEST_CASES` available for larger nightly campaigns. Six feature-gated durability hooks cover daemon metadata, journal publication, config migration, lifecycle result publication, relay revision publication, and checkpoint database publication. A consecutive seed-301 matrix passed all six with clean SQLite and no leaked process; the pre-existing topology chaos also passed five durable worker/bridge generations. Deliberately inverting the multi-segment invariant failed immediately and printed seed `424242` plus a minimized input, after which the real assertion was restored and passed.

The surface layer pins Playwright 1.62.1 and its Chromium revision under `tests/e2e/web`. Seed 413 passed the real HTTPS viewer with QR-token exchange, code login, mobile rendering, web session creation, conversation open, a browser held offline while the TUI resized and stopped the same session, explicit SSE recovery, browser resume and stop, cookie expiry, logout, bounded terminal quit, clean SQLite, a valid trace archive, and zero leaked processes. The run exposed and drove fixes for stale non-phone lifecycle state and online EventSource recovery. `.agents/docs/luna-reliability-runbook.md` and `prepare-luna-lab.py` provide an isolated fake-harness tmux campaign with eight mission cards and a mandatory evidence schema. Remaining work is tiered CI/nightly automation and recording the operational beta gates.

The reliability program is now implemented end to end. Pull requests run cross-platform formatting, Clippy, tests, dependency policy, corrected three-package coverage, and a separate no-Podman daemon/two-TUI/web scenario capped at fifteen minutes. The scheduled/manual Reliability workflow separates the six-hook plus topology crash layer, real Chromium/TUI convergence, one-thread and sixteen-thread full suites, and a configurable 30-to-60-minute seeded soak; every job uploads its diagnostics even after failure. The local one-minute demonstration passed five consecutive seeds, the lifecycle crash-hook smoke passed seed 414, both the one-thread and sixteen-thread schedules passed with fixed Proptest seed 700001, `cargo test` passed 66 CLI tests plus logging and two PTY tests, 1,356 core tests with four ignored, and 197 TUI tests with one ignored, and all-target Clippy and repository formatting passed. `.agents/docs/beta-acceptance.md` turns the remaining seven-night, candidate-soak, and Luna observations into explicit evidence fields and stop-ship checks. Those operational fields are intentionally blank, so this implementation makes beta readiness evaluable but does not prematurely claim the candidate is approved.

## Context and Orientation

Hel has three default workspace packages. `hel-core`, rooted at `src/lib.rs`, owns durable state, relay journals, controllers, workers, checkpointing, and session management. `hel-tui`, rooted at `crates/hel-tui/src/lib.rs`, is a pure terminal view and input reducer; it intentionally returns actions instead of performing I/O. `hel-cli`, rooted at `crates/hel-cli/src/main.rs`, contains both historical one-shot commands and the important long-running controller surfaces. Its `crates/hel-cli/src/daemon.rs` owns the persistent per-user daemon and shared session runtime, `crates/hel-cli/src/dashboard.rs` drives the terminal event loop, `crates/hel-cli/src/dashboard/actions.rs` and `dashboard/io.rs` move blocking work off that loop, `pollers.rs` feeds capacity, quota, credentials, and remote state, and `server.rs` adapts the daemon state to the web viewer implemented by `src/hel_server.rs`.

A relay is the target-side durable event log for one coding-agent session. An acknowledged relay event is one for which the controller has observed a committed ordinal and digest. A projection is the controller's materialized transcript and operational state derived from those relay events. A lifecycle operation is a create, resume, close, force-stop, or destroy action whose ownership must be unique even when several UI clients request it concurrently.

Existing tests are split according to repository policy. Module-level unit tests live next to the code. `crates/hel-cli/tests/termination_pty.rs` owns real pseudo-terminal shutdown checks. Shell and expect-style system harnesses live under `tests/e2e/`. `tests/e2e/session_restart_chaos.sh` provides a safe pattern: it refuses to signal processes unless `HEL_CHAOS_ISOLATED=1` is present inside a disposable environment. `src/hel_worker/journal_chaos.rs` contains the generated durability model. `.github/workflows/ci.yml` is the required cross-platform build, test, and fast system-smoke workflow; `.github/workflows/coverage.yml` gates the corrected three-package baseline; and `.github/workflows/reliability.yml` owns scheduled crash, browser, scheduling-pressure, and soak work.

The user's `AGENTS.md` change that introduced ExecPlans is already present as an unrelated working-tree modification. Preserve it and do not stage it with implementation commits unless the user separately asks for that file to be committed.

## Plan of Work

Milestone 1 makes the measurement layer trustworthy. First reproduce the instrumented PTY failure with `cargo llvm-cov` and fix the logging lifetime rather than weakening the assertion: the detach message must remain the final terminal output, and the tracing subscriber must not attempt to use a stopped asynchronous writer. Add or strengthen the focused PTY regression. Then change `.github/workflows/coverage.yml` so the JSON and LCOV report commands pass `-p hel-core -p hel-tui -p hel-cli`. Update `scripts/check-coverage.mjs` so paths retain `crates/hel-cli/` and `crates/hel-tui/` instead of collapsing every last `/src/` suffix into the root package. Replace `.github/coverage-baseline.json` with a full-package baseline, then raise its runtime minima as later milestones add behavior coverage. The policy remains no aggregate regression beyond the existing 0.25 percentage-point tolerance and a reviewed per-module exception for system boundaries that cannot run headlessly. Do not spend this milestone raising coverage for one-shot command parsing or ignored import wrappers.

Milestone 2 creates a deterministic system laboratory. Add reusable fixtures under `tests/e2e/` rather than creating a crate. The laboratory must create isolated config, data, cache, workspace, profile, and worker roots; allocate loopback ports; initialize a small Git repository; start a deterministic fake ACP bridge; and supervise every child in its own process group. It must expose bounded `start`, `wait_for`, `signal`, and `cleanup` operations and save a JSON trace containing the build commit, scenario name, seed, action sequence, process identifiers, observed daemon revisions, and artifact paths. The first scenarios launch a session on a local-bare target, attach two dashboard clients, connect the web viewer, submit and queue work, detach and reattach, and issue overlapping lifecycle requests. Assertions must prove exactly-once transcript content, monotonic revisions, converged clients, bounded quit and stop, SQLite `PRAGMA integrity_check`, valid relay frontiers, and no live child processes after cleanup. Drive terminal clients through a real pseudo-terminal or tmux screen, never by invoking TUI reducer methods directly.

Milestone 3 turns faults into replayable inputs. Extend the existing Proptest suite with multi-segment journals and generated sequences of append, acknowledge, seal, reopen, prune, corrupt, truncate, delete, duplicate, and recover operations. Compare each result with a small in-memory reference model that records which acknowledged ordinals and digests must survive. Add a non-default Cargo feature named `test-hooks` to `hel-core` and `hel-cli`. When that feature is disabled, no hook code or environment interpretation may exist in production paths. When enabled, a hook activates only if `HEL_CHAOS_ISOLATED=1`, `HEL_TEST_HOOK` names the exact hook, and `HEL_TEST_HOOK_DIR` is an isolated directory. Reaching a hook atomically creates `<hook>.reached` and waits for `<hook>.continue`; the chaos driver normally sends `SIGKILL` instead. Initial hook names cover journal append before snapshot publication, checkpoint archive persistence before database publication, config replacement before reference migration, lifecycle reservation before result publication, daemon metadata creation before listening, and relay projection before revision publication. Convert `session_restart_chaos.sh` into a table-driven runner that exercises ordinary process death plus these exact crash boundaries and validates the same invariants after restart.

Milestone 4 covers the surfaces a state model cannot see. Extend the PTY harness with terminal resizing, rapid key sequences, selection, clipboard completion and failure, unexpected library stderr, workspace switching, detach, and signal termination. Assertions operate on the terminal's parsed final screen and escape sequence ordering. Add a pinned Playwright Chromium test package beneath `tests/e2e/web/` for the real served HTML: login code and QR-token exchange, cookie logout and expiry, snapshot rendering, server-sent-event reconnect, open/close/resume actions, mobile viewport, and a browser left open while a TUI changes the same session. Browser tests use the fake ACP/local stack and save traces and screenshots on failure.

Create `.agents/docs/luna-reliability-runbook.md` during this milestone. It must be a local human/agent runbook, not product documentation and not a CI job. It starts a fresh tmux server and isolated Hel roots, records terminal dimensions and commit, and gives Luna mission cards covering setup, two TUIs plus web, lifecycle operations, forced daemon/worker/bridge death, rapid cancellation, resize and scrolling, clipboard behavior, stale browser state, and recovery. Each finding must include exact keys or web actions, `tmux capture-pane -p -e` output, relevant bounded logs, a process tree, the scenario trace, and a proposed deterministic regression. Fixes are out of scope until the reproduction is checked in.

Milestone 5 wires the cadence and demonstrates the beta bar. Required pull-request jobs run current formatting, Clippy, unit tests, corrected coverage, and a deterministic no-Podman two-client scenario in under fifteen minutes. Add a scheduled and manually dispatchable reliability workflow that runs the disposable process-chaos matrix, browser suite, serialized and highly parallel test modes, and a 30-to-60-minute seeded soak. It uploads traces, PTY captures, screenshots, and logs even on failure. Before calling the Linux/WSL build beta-ready, require seven consecutive green nightly runs, one current-commit soak with no invariant violations or leaks, and a completed Luna runbook campaign. There must be no known defect involving acknowledged data loss, duplicate transcript events, stop or recovery failure, authentication leakage, terminal corruption, UI hang, or cross-client divergence. macOS keeps its existing compile and unit-test coverage; Apple-container qualification and real credentials or remote infrastructure remain separate manual work.

## Concrete Steps

Work from `/home/jonathan/Projects/hel2`. At every stopping point, update this file's `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` sections, then commit the coherent validated milestone without staging unrelated files.

For Milestone 1, reproduce and then rerun the instrumented test:

    cargo llvm-cov --workspace --exclude hel-voice-worker --no-report --locked
    cargo llvm-cov report -p hel-core -p hel-tui -p hel-cli --json --output-path coverage.json
    cargo llvm-cov report -p hel-core -p hel-tui -p hel-cli --lcov --output-path coverage.lcov
    node scripts/check-coverage.mjs coverage.json .github/coverage-baseline.json coverage-summary.md

Before the fix, the first command has been observed to fail in `dashboard_detach_restores_terminal_then_exits_promptly_with_final_message` with a tracing-subscriber writer diagnostic after the detach message. After the fix it must exit zero, and the JSON must contain files beginning with both `crates/hel-cli/src/` and `crates/hel-tui/src/`.

Run deterministic system scenarios through a single documented entry point created under `tests/e2e/`. The final command name should be:

    tests/e2e/run-reliability.sh --scenario multi-client-happy-path --seed 1 ./target/x86_64-unknown-linux-musl/debug/hel

It must print the artifact directory and a final line equivalent to:

    reliability: passed scenario=multi-client-happy-path seed=1 clients=3 leaks=0

The failure form must print one exact replay command using the same seed and preserve its artifact directory.

The process-chaos entry point remains guarded and must be runnable only inside an isolated environment:

    HEL_CHAOS_ISOLATED=1 tests/e2e/session_restart_chaos.sh ./target/x86_64-unknown-linux-musl/debug/hel

Generated Rust failures are replayed with the `PROPTEST_CASES`, `PROPTEST_RNG_SEED`, or regression-file mechanism emitted by Proptest. Do not loop until green.

Before every implementation commit run focused tests for the changed module. Before each milestone commit run, outside the restricted sandbox as required by `AGENTS.md`:

    cargo test
    cargo clippy --all-targets -- -D warnings
    git diff --check

Run `cargo fmt --all -- --check` as well. If it reports an unrelated pre-existing formatting difference, format only the files changed by the milestone and record the exact unrelated path in `Surprises & Discoveries` rather than modifying it.

## Validation and Acceptance

Milestone 1 is accepted when an instrumented workspace test run exits successfully, the PTY detach message is the final terminal output, and the report names all three packages without path collisions.

Milestone 2 is accepted when the same deterministic command starts a real daemon, two terminal clients, and one web client against isolated state; performs a session lifecycle; proves all three clients converge; exits within its deadlines; and leaves no process or filesystem owner running. Running it twice with the same seed must produce the same logical action trace.

Milestone 3 is accepted when generated journal histories survive thousands of operation/fault sequences without violating the reference model, every named crash hook can be reached and killed in isolation, and restart validation proves acknowledged events exactly once. A deliberately inverted invariant in a local test must make the suite fail and print a replayable seed before that deliberate change is reverted.

Milestone 4 is accepted when PTY tests detect stray post-frame output, Playwright proves browser/TUI convergence after an SSE reconnect, and a person or Luna can follow the runbook from a clean tmux server without undocumented setup. The runbook itself must produce a complete artifact set even when no defect is found.

Milestone 5 is accepted when pull-request jobs remain within the chosen budget, the scheduled workflow retains all failure evidence, and the beta criteria can be evaluated from recorded workflow and runbook results rather than memory.

## Idempotence and Recovery

Every test root must be newly created and must never point at the user's real config, data, cache, workspace, container, or worker directories. Cleanup first terminates the owning process group and only then removes its files. A failed test preserves artifacts by default and prints their path; a successful test may delete its temporary root after checking for leaks. Scenario and chaos commands are safe to repeat because names include a seed and unique temporary directory. Named test hooks are compiled out unless `test-hooks` is selected and reject activation unless the isolation guard is present.

Coverage outputs `coverage.json`, `coverage.lcov`, and `coverage-summary.md` are generated evidence and must not be committed. If a coverage run is interrupted, rerun it; do not hand-edit profile data. If a nightly job is interrupted, replay the printed seed locally rather than restarting the whole campaign first.

## Artifacts and Notes

The initial correct full-package report, generated after skipping the known PTY failure, measured these runtime files:

    crates/hel-cli/src/daemon.rs             39.72% lines
    crates/hel-cli/src/dashboard.rs          54.15% lines
    crates/hel-cli/src/dashboard/actions.rs   2.11% lines
    crates/hel-cli/src/dashboard/io.rs       13.25% lines
    crates/hel-cli/src/server.rs             43.38% lines
    crates/hel-tui/src/render.rs             97.33% lines

These numbers guide scenario selection; they are not an instruction to add tests that merely execute lines. The implementation must prefer assertions about the advertised multi-client and durability behavior.

A reliability artifact directory should have a stable shape so CI, the Luna runbook, and humans can inspect it:

    trace.json
    controller.log
    daemon.log
    process-tree.txt
    tui-1.capture
    tui-2.capture
    browser-trace.zip
    browser-failure.png
    integrity.txt

Absent artifacts are allowed when a scenario did not start that surface. `trace.json` and `integrity.txt` are mandatory for every completed scenario.

## Interfaces and Dependencies

Do not add a new workspace crate. Reuse existing subprocess helpers for Rust child processes and existing `tempfile` support for isolated paths.

Add non-default `test-hooks` Cargo features to the packages that contain hook sites. The core interface should be a small test-only function with behavior equivalent to:

    pub(crate) fn reach_test_hook(name: &'static str) -> anyhow::Result<()>;

With the feature disabled it must compile to an immediate `Ok(())` or be removed by conditional compilation without reading the environment. With the feature enabled it validates the isolation variables, atomically publishes the reached marker, and waits with a bounded poll for continuation or external termination. Production callers name constant hooks; arbitrary environment strings never choose code paths.

The scenario trace is versioned JSON. Define a serializable record containing `format_version`, `commit`, `scenario`, `seed`, ordered `actions`, ordered `process_events`, observed `revisions` per client, `started_at`, `finished_at`, and `outcome`. Secrets, credential contents, private repository content, and QR login tokens must never be written to it.

The fake ACP bridge implements only the protocol requests needed by the scenarios but must be stateful: it returns a stable native session id, records prompts, emits deterministic streaming and tool events, supports cancellation, and can pause at commands supplied by the driver. Messages larger than 64 KiB must be included in at least one scenario so pipe-boundary behavior remains covered.

Use the repository's existing Proptest dependency. Do not add `proptest-state-machine` unless the handwritten reference model proves materially harder to shrink or maintain; generated `Vec<Action>` sequences are sufficient for the first implementation.

Pin Playwright and its Chromium version in the test package lockfile. Browser installation belongs only to the reliability workflow and local runbook, not the normal Rust build.

Change note, 2026-08-30: Initial ExecPlan created from the agreed beta-reliability strategy. It explicitly defers mutation testing, narrows `hel-cli` work to runtime orchestration, and makes the local Luna runbook a required artifact rather than a CI dependency.

Change note, 2026-08-30: Implementation completed through Milestone 5. The plan now records the browser-discovered cross-client fixes, pinned workflows, validated soak behavior, and the distinction between a complete reliability system and a beta candidate that still needs seven nights of external evidence.
