# Harden daemon-owned turn review

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

The canonical plan rules are in `.agents/PLANS.md`, relative to the repository root. This document must be maintained in accordance with that file.

## Purpose / Big Picture

Mjolnir's daemon owns sessions and runs the optional review of each completed turn. After this work, a review must become visible to every attached terminal and phone immediately; it must block the next prompt before asynchronous preparation starts; its durable `active` marker must accurately describe whether work is open; and every review state write must run outside the Tokio task that owns the control loop. The daemon metadata file must also mean the daemon is actually ready, and every failure after startup begins must stop background owners and remove that metadata.

The behavior is observable through focused tests that start the real host actor and daemon test hooks. A review publication advances the daemon revision without waiting for an unrelated session update; a prompt is refused during both preparation and an open verdict; failed or cancelled reviews release the prompt hold and clear durable state; the primary harness profile cannot review itself; an interrupted daemon startup never leaves a ready-looking `daemon.json`; and all existing tests continue to pass.

## Progress

- [x] (2026-09-01 18:21Z) Published the rename-aware Hel convergence to both repositories at `e861523d`, so these corrections can land as ordinary shared-history commits.
- [x] (2026-09-01 18:22Z) Reconfirmed the seven review/daemon failure modes in `src/hel_review/host.rs` and `mj-cli/src/daemon.rs`, and split implementation ownership into non-overlapping host and daemon slices.
- [x] (2026-09-01 18:32Z) Inspected the actual GitHub results for shared tip `e861523d`: Linux CI, reliability, licenses, voice, and desktop passed; Coverage, Windows Clippy, and one macOS process-liveness test failed. Prioritized those master gates over further hardening.
- [x] (2026-09-01 18:34Z) Replayed the failed coverage artifact and corrected the stale `main.rs` reviewed-exception floor after proving the greeting deletion removed 82 covered lines and introduced no uncovered path.
- [x] (2026-09-01 18:41Z) Made review admission, durable active state, ordered nonblocking persistence, profile isolation, polling, and idempotent shutdown correct in `src/hel_review/host.rs`, with production-path behavior tests.
- [x] (2026-09-01 18:42Z) Connected review publication to monotonic runtime revisions, boxed the Windows-large reply variant, separated daemon-owned macOS process-group liveness from non-reaping attachment probes, and made daemon readiness and cleanup one supervised lifecycle.
- [x] (2026-09-01 18:47Z) Passed focused host and daemon tests, the full workspace suite, warnings-denied workspace Clippy, formatting, the metadata-failure chaos hook, and the three-client reliability scenario with zero leaks.
- [x] (2026-09-01 18:56Z) Published coverage correction `7814efea`, hardening commit `eb14d90b`, and the independently validated reliability-race fix `80934557` as ordinary fast-forwards to both `origin/master` and `hel/master`.

## Surprises & Discoveries

- Observation: the first merged implementation passed the full Rust suite even though a review view only mutated `HostShared.views`; no code advanced `RuntimeState.revision_tx` for that mutation.
  Evidence: `TurnReviewHost::publish` in `src/hel_review/host.rs` writes the private map, while attached surfaces wait on the watch channel exposed by `RuntimeState::revisions` in `mj-cli/src/daemon.rs`.

- Observation: the durable recovery path could not observe a real in-flight review because production code cleared `TurnReviewState.active` but never set it.
  Evidence: all assignments in the imported host set `active = None`; `hel_database::clear_interrupted_turn_reviews` only reports rows whose `active` field is present.

- Observation: the prompt hold begins after blocking preparation returns, leaving an admission window in which the next primary prompt can start and change the turn being reviewed.
  Evidence: `HostState::begin` records only `preparing`; `hold_prompts` is called later from `HostState::prepared`.

- Observation: `run_daemon_process` writes `daemon.json` before workspace loading, runtime state construction, phone initialization, and the select loop. Several later `?` and `bail!` exits bypass the cleanup epilogue.
  Evidence: `write_metadata` precedes `list_workspaces` and `spawn_remote_session_manager`, while listener accept and session publication use `?` inside the loop and a closed manager channel calls `bail!`.

- Observation: the first converged GitHub run exposed two platform-specific gates that Linux validation could not reproduce. `DaemonReply::RuntimeSnapshot` crosses Clippy's large-variant threshold under the Windows ABI, and the child-exit check's `sysinfo` zombie status is not reliable on macOS.
  Evidence: CI run `33543007506` reports a 368-byte Windows variant against a 160-byte next-largest variant, and macOS times out in `a_process_that_exited_but_was_not_reaped_counts_as_gone` after five seconds.

- Observation: the coverage failure was a stale denominator, not new untested behavior. Removing the startup greeting changed `mj-cli/src/main.rs` from 404 covered lines out of 759 to 322 out of 668: 91 measurable lines and 82 covered lines disappeared while aggregate workspace coverage rose.
  Evidence: Coverage run `33543007505` measured `main.rs` at 48.20% and aggregate coverage at 78.02%; replaying its exact artifact passes after changing only the reviewed floor from 51.75% to 48.00% with the deletion rationale.

- Observation: the first hardening run exposed an existing target-refresh race during Close. The refresher removed every active lifecycle session from the session manager; if its 500 ms tick won before Close leased the relay, checkpointing waited five seconds and failed with `session is not managed`.
  Evidence: CI run `33545102022` left the reliability session running with that exact checkpoint error. Its Close began at 18:43:53 and the durable error was written at 18:43:58. Keeping Close targets in the manager made the exact seed-1 scenario pass locally.

## Decision Log

- Decision: Keep a single actor as the ordering authority for each session's review and send persistence work through an ordered background lane rather than spawning independent writes.
  Rationale: independent blocking tasks can complete out of order and resurrect an `active` marker after a close. One lane preserves actor order without blocking the Tokio control loop.
  Date/Author: 2026-09-01, Codex.

- Decision: Acquire the prompt hold at admission before staging or loading state, and release it from every terminal refusal, failure, cancellation, close, and actor shutdown path.
  Rationale: the hold protects the identity of the completed turn. Installing it after preparation does not prevent a new prompt from racing the review's baseline.
  Date/Author: 2026-09-01, Codex.

- Decision: Publish daemon metadata only after all fallible initialization required to serve clients has completed, then use one structured epilogue for both normal and error exits.
  Rationale: clients interpret metadata as readiness. A file written earlier is a false-positive readiness signal, and duplicated cleanup branches are likely to miss new errors.
  Date/Author: 2026-09-01, Codex.

- Decision: Notify `RuntimeState` only when the externally visible review projection actually changes.
  Rationale: every real change must wake clients, but revisions are state cursors rather than an event counter and should not churn for identical publications.
  Date/Author: 2026-09-01, Codex.

- Decision: Treat remote `master` CI as the primary green-state evidence and repair all three converged-tip failures before publishing the deferred hardening work.
  Rationale: local Linux success cannot establish Windows layout lints, Darwin process semantics, or the repository's coverage policy. The user explicitly requires a genuinely green remote master.
  Date/Author: 2026-09-01, Codex.

- Decision: Exclude create, resume, force-stop, and destructive lifecycle targets from normal polling, but retain a Close target until its managed checkpoint lease finishes.
  Rationale: startup and teardown cannot tolerate opportunistic recovery, while graceful Close explicitly depends on the manager connection. Treating both cases alike creates a timing-dependent stop failure.
  Date/Author: 2026-09-01, Codex.

## Outcomes & Retrospective

The host now installs its prompt hold at admission, persists active/clear transitions through one ordered blocking lane, publishes only real view changes, and drains idempotently on shutdown. The daemon connects those publications to a monotonic revision feed, delays metadata until the client-usable runtime exists, and runs every exit through a bounded epilogue that joins its owners before the database writer. Daemon process exit probing can reap only the daemon-owned child; attachment observation cannot reap arbitrary children; the Darwin non-parent path probes the daemon-owned process group.

The full local workspace suite and warnings-denied Clippy passed after the final hardening changes. The deterministic `daemon_metadata_before_listening` failure hook and the three-client reliability scenario both passed with zero leaks. Corrections were published incrementally to both repositories at `7814efea`, `eb14d90b`, and `80934557`; the two remote-tracking master refs matched after every push. Remote coverage for `eb14d90b` passed, including the corrected CLI floor. The reliability failure found on that run was fixed forward in `80934557` rather than rewriting published history.

## Context and Orientation

The repository is a Rust workspace. Package `brokk-mj-core` lives at the root and exposes the internal Rust library name `hel`; `brokk-mjolnir` in `mj-cli/` provides the `mj` executable. The internal `hel_*` filenames are deliberate shared implementation names and are not user-facing branding.

`src/hel_review/host.rs` contains `TurnReviewHost`, a Tokio actor that receives session observations, launches reviewer roles, owns prompt holds, and projects `RuntimeReviewView` values. A prompt hold is the in-memory entry checked by normal prompt admission; while present, no new primary turn may start. `TurnReviewState` in `src/hel_database.rs` is the durable baseline plus an optional `active` marker used to recover from interrupted reviews.

`mj-cli/src/daemon.rs` contains `RuntimeState` and `run_daemon_process`. `RuntimeState.revision_tx` is a Tokio watch channel: terminal and phone clients wait for its value to change before fetching a new snapshot. `run_daemon_process` owns the database writer, session manager, recovery coordinator, target refresher, loopback listener, optional phone server, and `daemon.json`. The metadata file contains the endpoint and bearer token clients need to connect, so its existence is the daemon's readiness contract.

`mj-cli/tests/store_divergence.rs` launches the real executable with test hooks and checks daemon startup failure. Colocated `#[cfg(test)]` modules in `src/hel_review/host.rs` and `mj-cli/src/daemon.rs` exercise actor transitions and runtime revisions without a real external harness.

## Plan of Work

First change `TurnReviewHost` so a begin request atomically reserves the session and installs its prompt hold before any asynchronous preparation. Preparation must recheck the live primary actor before opening the review. Every path that decides not to open must release the hold. When a review opens, enqueue an ordered persistence update with `TurnReviewState.active` set to an identifier for that run; when it closes, fails, is cancelled, or the host shuts down, enqueue the matching clear and release the hold. The persistence worker must serialize calls to the existing blocking `ReviewEnvironment::save_state` method away from the actor's Tokio task and expose a bounded shutdown/drain method. The production environment check must reject a reviewer profile equal to the session's primary `last_profile`. Remove the redundant role poll after a `PromptRole` step already polls.

Give the host a small change-notification callback or channel. `publish` compares the old and new `RuntimeReviewView`; insertions, changes, and removals invoke the notification after releasing the view mutex. In `RuntimeState::new_with_controller_loader`, connect this notifier to the same atomic revision and watch sender used by `publish_revision`, without creating a reference cycle back to the whole runtime state. A test subscribes before opening a review, triggers the host, and observes the revision change.

Then restructure `run_daemon_process` into fallible initialization, a serving phase, and one cleanup phase. Delay `write_metadata` until the listener, workspace load, runtime state, termination token, target refresher, interrupted-close recovery, and optional phone bridge are ready. Once metadata is visible, capture every select-loop exit as a result rather than returning with `?` or `bail!`; always start the shutdown watchdog, cancel background work, shut down the review host and session manager, await supervised tasks, remove metadata, and drain the database writer. Preserve the original operational error as the return value while logging cleanup failures with context. Strengthen the test-hook integration test so it proves metadata is absent after injected startup failure and uses an actual successful client-ready condition when testing divergence.

Finally format and run focused host, daemon, and store-divergence tests. Run the complete default test suite outside the restricted sandbox, then Clippy with warnings denied. Run the daemon test-hook chaos scenario because the change affects startup and cleanup ordering. Update this living plan with exact evidence, commit on `master`, refresh both remotes, and fast-forward both only after confirming neither moved incompatibly.

## Concrete Steps

Run all commands from `/home/ryan/code/mjolnir`. Inspect focused behavior while implementing:

    cargo test -p brokk-mj-core hel_review::host::tests::
    cargo test -p brokk-mjolnir daemon::tests::
    cargo test -p brokk-mjolnir --test store_divergence

Then run repository gates:

    cargo fmt --all -- --check
    cargo test
    cargo clippy --all-targets -- -D warnings
    cargo build --locked -p brokk-mjolnir --features test-hooks
    MJ_CHAOS_ISOLATED=1 tests/e2e/run-test-hook-chaos.sh ./target/x86_64-unknown-linux-musl/debug/mj --hook daemon_metadata_before_listening --seed 700102

Every `cargo test` command must run outside the restricted sandbox because tests use loopback TCP and Unix sockets. The final Git commands are normal commits and fast-forward pushes; never force either remote.

## Validation and Acceptance

Focused host tests must demonstrate that the prompt lock is visible while preparation is deliberately paused, that each refused/failed/cancelled path removes it, that opening persists a nonempty `active` value, and that every close clears it in the same ordered lane. A deliberately blocked save followed by a close must preserve write order while the Tokio actor continues answering messages. A primary/reviewer profile collision must return the explicit separation error.

A runtime test must subscribe to revisions, cause only a review view change, and observe a larger revision before its timeout. It must also show that republishing an identical view does not allocate another revision if the host intentionally suppresses duplicates.

Daemon tests must inject failure after initialization steps that historically followed metadata publication and observe both a nonzero result and no metadata file or surviving database writer. The store-divergence test must wait for a usable endpoint rather than file existence alone. Normal daemon start and shutdown must still remove metadata.

Acceptance additionally requires `cargo test` and warnings-denied Clippy to pass, the chaos script to complete its scenarios, `git status --short` to be empty after commit, and both `origin/master` and `hel/master` to resolve to the final commit after normal fast-forward pushes.

## Idempotence and Recovery

Tests and formatting are safe to repeat. Host shutdown must be idempotent so cleanup can call it after partial initialization. Metadata removal treats `NotFound` as success. If the daemon fails before metadata publication, cleanup still stops every owner that was created; if it fails after publication, cleanup removes only the exact path returned by `metadata_path`. No test may delete a process's files instead of terminating the owning process first.

Before publication, edits can be corrected in place without rewriting the already-published convergence commits. If either remote advances, fetch it and perform one ordinary merge on `master`; do not reset, rebase, or force-push.

## Artifacts and Notes

Starting shared tip:

    e861523d78e34ddf12e9f1e80b6af68eb5716254

Published correction commits:

    7814efea Adjust CLI coverage floor after greeting removal
    eb14d90b Harden review host and daemon shutdown
    80934557 Keep close sessions attached to relay manager

The convergence already included the complete phone/TUI state-projection fixes. This plan covers only the deferred review-host and daemon lifecycle findings, not further feature work.

## Interfaces and Dependencies

Keep `ReviewEnvironment::save_state` blocking at the trait boundary so existing production and fake environments remain simple; isolate it behind one ordered worker owned by `TurnReviewHost`. Add an explicit asynchronous or otherwise bounded `TurnReviewHost::shutdown` operation that drains that worker after the actor has cleared active reviews and prompt holds.

The publication hook must be a small cloneable `Send + Sync + 'static` callback or sender owned by `HostShared`, not a reference to `RuntimeState`. `RuntimeState` may share an `Arc<AtomicU64>` with the callback and keep its existing `watch::Sender<u64>`. Existing callers that do not need notification, especially host unit tests, should receive a no-op default through the public convenience constructor.

Do not add a new crate or database schema. Reuse `TurnReviewState`, the existing controller database functions, Tokio channels, the repository's session-manager shutdown handle, and its subprocess/test-hook infrastructure.

Revision note (2026-09-01 18:22Z): Created the plan after publishing history convergence. It records the deferred correctness findings, non-overlapping implementation boundaries, ordered-persistence design, readiness contract, and required validation.

Revision note (2026-09-01 18:35Z): Added the actual remote-master CI results, the stale coverage-floor evidence, and the Windows/macOS corrections now required for acceptance. Remote CI, rather than local Linux validation alone, is the final green-state authority.

Revision note (2026-09-01 18:56Z): Closed the implementation milestones with incremental publication, full local validation, the remote reliability-race evidence, and its separate fix-forward commit.
