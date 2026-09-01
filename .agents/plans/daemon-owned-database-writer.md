# Make the daemon the sole database writer

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds. This document is maintained in accordance with `.agents/PLANS.md`.

## Purpose / Big Picture

Hel currently allows the long-running daemon, the terminal dashboard, and the phone server to open independent writable SQLite connections. SQLite serializes writes even in write-ahead-log mode, so an otherwise healthy daemon write can overlap a dashboard read-receipt write and produce `database is locked: Error code 5`. After this work, the daemon process is the only production process allowed to mutate `hel.sqlite3`. It owns a bounded first-in, first-out write service, while dashboard and phone clients continue to query through read-only SQLite connections and send every mutation through the existing daemon protocol. A user can keep a dashboard, phone client, and session synchronization active concurrently without intermittent read-status failures.

## Progress

- [x] (2026-08-31 21:00Z) Reproduced and traced the lock to independent daemon and dashboard SQLite writers, and selected daemon ownership with direct read-only client queries.
- [x] (2026-08-31 21:00Z) Created this self-contained ExecPlan and confirmed the worktree is clean on the current `hel2` branch.
- [x] (2026-08-31 22:02Z) Implemented a bounded daemon-owned writer service with a retained connection, FIFO execution, panic/error reporting, queue draining, and explicit shutdown.
- [x] (2026-08-31 22:02Z) Separated production read-only connection opening from schema initialization and mutation opening; strict readers use SQLite read-only flags plus `query_only`.
- [x] (2026-08-31 22:02Z) Extended daemon protocol version 4 with typed operations for client receipts, drafts, metadata, archive/visibility settings, imports, checkpoint/recovery, review state, and prompt history.
- [x] (2026-08-31 22:02Z) Routed dashboard, phone, chat, import, checkpoint, and recovery mutations through daemon operations without blocking UI event loops.
- [x] (2026-08-31 22:02Z) Routed session projection and metadata writes through the daemon-owned writer lane and moved projection preparation ahead of the SQLite transaction.
- [x] (2026-08-31 22:02Z) Added behavior tests for ordered writes, atomic receipt persistence, backpressure, queue draining, writer panic reporting, protocol rejection, and strict read-only connections.
- [x] (2026-08-31 22:02Z) Passed formatting, the full workspace test suite, strict clippy, all six crash hooks, worker-topology restart chaos, and the three-client reliability scenario.

## Surprises & Discoveries

- Observation: The error does not imply a second daemon. The dashboard process itself writes read receipts through `crates/hel-cli/src/dashboard/io.rs`, while session synchronization in the daemon writes materialized projections through `src/hel_session_manager.rs`.
  Evidence: `spawn_read_receipt_persist` calls both frontier mutation functions directly, and `apply_projection_page_to` begins an independent immediate transaction.

- Observation: The database is large enough that transaction duration matters, not merely the number of writers.
  Evidence: The observed production database was approximately 1.49 GB and materialized transcript content comprised most of it; projection code currently computes and applies event changes while an immediate transaction is held.

- Observation: Existing path-taking mutation helpers are extensively used by isolated database tests and migration fixtures.
  Evidence: Production entry points now serialize those helpers through the daemon writer lane, while the receipt and projection hot paths have connection-taking implementations that use the retained connection directly. This preserves fixture coverage without permitting concurrent production writers.

- Observation: Enforcing a strict read-only opener in production initially broke tests that deliberately construct and migrate old schemas.
  Evidence: Test-only path helpers continue to use the initializer/writer opener, while a dedicated strict-reader test verifies that production read connections reject mutation.

- Observation: Top-level checkpoint commands now start the daemon, so the logging integration test legitimately observes both a client log and daemon log.
  Evidence: The test now locates the client command log and verifies private permissions for both files.

- Observation: Routine pull-request CI does not run the full chaos matrix.
  Evidence: Routine CI runs the three-client reliability smoke. `.github/workflows/reliability.yml` owns the full crash-hook and topology matrix through scheduled and manual runs.

- Observation: One final crash-hook matrix attempt timed out waiting for the killed journal worker to leave `provisioning`, although database integrity remained `ok`.
  Evidence: The identical hook and seed passed immediately when rerun in a fresh isolated root, and the subsequent complete six-hook matrix passed with zero leaks. This is an existing nondeterministic worker-settlement timing path, not a SQLite writer failure; the failed artifact remains under `target/reliability-artifacts/`.

## Decision Log

- Decision: Keep direct SQLite reads in dashboard and phone processes, but open those connections in read-only/query-only mode and never initialize or migrate schema from a client.
  Rationale: Reads in write-ahead-log mode can coexist with the daemon writer and avoid turning every dashboard query into protocol surface area. Read-only opening makes the ownership rule mechanically enforceable.
  Date/Author: 2026-08-31 / Codex

- Decision: Use a bounded FIFO service of capacity 256, owned and started only after `ControllerStoreGuard` is acquired.
  Rationale: A single persistent connection removes cross-task writer contention. A bounded queue applies backpressure rather than permitting unbounded memory growth, and acquiring the controller lock first preserves the existing one-daemon invariant.
  Date/Author: 2026-08-31 / Codex

- Decision: Bump the daemon protocol from version 3 to version 4 and add typed mutation requests rather than a generic SQL or key/value operation.
  Rationale: Typed operations preserve validation, authorization boundaries, and compatibility behavior. Existing client startup already replaces an incompatible daemon, so a version bump has a defined upgrade path.
  Date/Author: 2026-08-31 / Codex

- Decision: Persist a read receipt and its viewed-through ordinal in one writer job and one transaction.
  Rationale: The current two-call sequence can partially succeed. One operation preserves monotonicity and cannot leave the two frontiers inconsistent when validation or storage fails.
  Date/Author: 2026-08-31 / Codex

- Decision: Record prompt history in the daemon only after prompt submission receives its accepted event ordinal.
  Rationale: This gives prompt history a stable idempotency key, keeps clients read-only, and reports persistence failure without retrying an already accepted prompt.
  Date/Author: 2026-08-31 / Codex

- Decision: Give `ActiveChat` a persistence request channel rather than a daemon dependency.
  Rationale: Dashboard production writes are handled asynchronously by its supervised daemon task, while the chat library remains usable by isolated tests and daemon-internal callers.
  Date/Author: 2026-08-31 / Codex

- Decision: Serialize legacy path-taking mutations inside the same writer lane and convert the high-contention receipt and projection operations to direct retained-connection implementations.
  Rationale: This establishes the required single active production writer immediately while retaining well-covered migration/test helpers. Because the lane does not start a second job until the current helper commits and closes its transient connection, these helpers cannot contend with one another or with the retained-connection operations.
  Date/Author: 2026-08-31 / Codex

## Outcomes & Retrospective

The daemon is now the only production process that can mutate `hel.sqlite3`. Dashboard, phone, chat, import, checkpoint, and recovery paths use typed protocol version 4 requests; their direct connections are opened read-only and `query_only`. The daemon installs a bounded 256-job FIFO only after it owns `ControllerStoreGuard`, drains accepted work during shutdown, and refuses post-shutdown fallback writes. Read receipts update both frontiers atomically, and projection preparation no longer extends the SQLite transaction.

Validation completed successfully: `cargo fmt --all -- --check`, `cargo test`, and `cargo clippy --all-targets -- -D warnings` all passed. The final workspace run reported 81 CLI unit tests, 1 logging integration test, 2 PTY tests, 1,454 core tests with 4 ignored environment-dependent tests, and 218 TUI tests with 1 ignored timing measurement; the four signed-in import E2E tests remained ignored as documented. The final six-hook crash matrix passed with zero leaked processes, worker-topology restart chaos produced five durable generations, and the three-client reliability scenario passed with zero leaks.

No schema migration or new dependency was required. A remaining internal cleanup opportunity is to convert every legacy path-taking mutation helper to accept the retained connection directly. It is not a correctness gap for writer ownership: all production mutation entry points already execute synchronously inside the single daemon lane, so at most one connection is actively writing.

## Context and Orientation

Hel is a Rust workspace. The `hel` library contains the normalized SQLite persistence layer in `src/hel_database.rs` and schema connection logic in `src/hel_database/schema.rs`. The controller lock in `src/hel_controller.rs` prevents two daemon processes from owning the same data directory. The executable in `crates/hel-cli` implements both the daemon and user-facing processes. `crates/hel-cli/src/daemon.rs` defines a framed request/reply protocol, the daemon client, runtime state, and request dispatch. The terminal dashboard is under `crates/hel-cli/src/dashboard/`, the phone/web server is `crates/hel-cli/src/server.rs`, and import orchestration is `crates/hel-cli/src/import.rs`.

In this plan, a writer job is a typed Rust closure submitted to one background thread that owns the sole writable `rusqlite::Connection`. FIFO means jobs execute in submission order. Backpressure means submission waits in a background task when the fixed-capacity queue is full instead of growing memory without bound. A projection is the materialized session state derived from an append-only stream of session events; projection updates are among the largest database writes.

Today, public helpers such as `hel_database::advance_client_read_frontier` resolve the global database path and open their own writable connection. Dashboard and phone background tasks call those helpers directly. The daemon also runs multiple session actors whose projection updates each open writable connections. WAL permits readers during a write, but SQLite still has only one writer at a time, and the five-second busy timeout eventually reports error code 5 when transactions overlap for too long.

## Plan of Work

First, add a `DatabaseWriter` in `src/hel_database.rs`. Its worker thread opens and retains one connection configured by the schema module. Submission uses a bounded synchronous channel of capacity 256. Each job includes a short operation label, a closure that accepts the connection, and a typed one-shot reply. Queue closure or worker panic becomes an ordinary contextual error; shutdown stops accepting work, drains queued jobs for up to the daemon's existing bounded cleanup interval, and joins the thread. Production construction is exposed through `ControllerStoreGuard`, so startup cannot create a writer before daemon exclusivity is established.

Refactor schema opening into explicit roles. Initialization/migration and writer opening may create directories and set persistent pragmas. Reader opening uses SQLite read-only flags, enables `query_only`, applies a busy timeout suitable for WAL readers, and verifies the existing schema without modifying it. Public query helpers use the reader path. Mutation SQL gains connection-taking internal helpers so writer jobs operate on the persistent connection rather than recursively opening another connection. Path-taking helpers remain available only where tests and migration utilities need isolated temporary databases.

Next, install the writer in `run_daemon_process` immediately after acquiring `ControllerStoreGuard` and before startup migrations or recovery. Add it to daemon runtime ownership and close it during bounded shutdown. Session-manager projection, workspace/session metadata, controller lifecycle state, worker metadata, prompt history, and recovery/checkpoint mutations must submit typed database jobs. Expensive event interpretation and transcript transformation must be computed before submission; the writer closure should only execute SQL and commit.

Then bump `PROTOCOL_VERSION` to 4 and add typed `DaemonAction` variants and `DaemonClient` methods for all mutations currently issued by dashboard, phone, chat, import, checkpoint, and recovery flows. The first required operation atomically advances the client read frontier and the session viewed-through ordinal; draft persistence is a separate typed operation so an invalid receipt never discards a valid draft. Other operations cover archive and native-session visibility, rename and container settings, active-review/default selections, prompt history, imported-session finalization, checkpoint, and recovery. Requests that include sizeable imported session data remain within the existing frame limit and run as supervised daemon background work.

Finally, replace every client-process call to a mutating `hel_database` or mutating `Controller` method with the corresponding daemon client call. Dashboard and web handlers must update in-flight state immediately and await daemon replies only from already-supervised background tasks. Canonical worker title and target-missing persistence moves into daemon publication code so clients do not duplicate it. Search the client sources for mutation symbols to demonstrate that remaining `hel_database` calls are queries. Add tests around the behavior rather than merely enumerating variants.

## Concrete Steps

All commands run from `/home/jonathan/Projects/hel2`.

Inspect current mutation call sites and connection creation:

    rg -n "hel_database::|Controller::" crates/hel-cli/src src/hel_chat
    rg -n "schema::open|open\(&database_path|BEGIN IMMEDIATE" src/hel_database.rs src/hel_database

After each coherent implementation milestone, format and run its focused tests:

    cargo fmt --all
    cargo test hel_database::tests
    cargo test -p hel-cli daemon

The repository instructions require all `cargo test` commands outside the restricted sandbox because tests use loopback sockets and Unix sockets. At completion run:

    cargo fmt --all -- --check
    cargo test
    cargo clippy --all-targets -- -D warnings

Expected successful endings are `test result: ok` for every test binary and no diagnostics from clippy. Inspect the final diff and stage only files named by `git status --short`, then commit on the current branch with a message describing daemon database ownership.

The completed reliability commands were:

    cargo build --locked -p hel-cli --features test-hooks
    HEL_CHAOS_ISOLATED=1 tests/e2e/run-test-hook-chaos.sh target/x86_64-unknown-linux-musl/debug/hel --seed 700101
    HEL_CHAOS_ISOLATED=1 HEL_CHAOS_ARTIFACT_DIR=target/reliability-artifacts/worker-topology-daemon-writer-final tests/e2e/session_restart_chaos.sh target/x86_64-unknown-linux-musl/debug/hel
    tests/e2e/run-reliability.sh --scenario multi-client-happy-path --seed 700101 target/x86_64-unknown-linux-musl/debug/hel

They ended with six hook passes and zero leaks, five durable worker/bridge generations, and a three-client pass with zero leaks, respectively.

## Validation and Acceptance

Low-level tests must prove that concurrent submitters execute on one writer connection in FIFO acceptance order, that more than 256 pending submissions block without losing or reordering jobs, that a failed job reports its error and later jobs still execute, and that shutdown drains accepted jobs and rejects new ones. A connection-observation test must demonstrate that client query helpers open read-only/query-only connections and that production mutation helpers cannot silently create an independent writable connection.

Protocol tests must prove version 4 round trips each new typed request and rejects version 3. The receipt test creates a workspace/session, advances both frontiers in one request, and verifies both values. It must also submit a stale receipt and verify monotonic values do not regress. An invalid receipt must fail without changing either frontier, while separately submitted draft content remains present.

Integration behavior is accepted when dashboard read-status persistence and phone read-status persistence both use a connected `DaemonClient`, no client source contains a call to a public mutating database helper, and simultaneous session synchronization plus repeated receipt updates completes without `SQLITE_BUSY`. Full workspace tests and clippy must pass.

## Idempotence and Recovery

This change does not alter schema version or stored data shape, so restarting an old build remains possible after stopping the new daemon. Protocol version negotiation already handles incompatible daemon replacement. Writer operations are monotonic or transactional where appropriate, allowing a client to retry after a lost reply. Import finalization must preserve its existing stable identifiers so a repeated request updates rather than duplicates records.

If daemon startup fails after acquiring the controller lock but before serving requests, dropping the writer handle closes its connection and dropping the guard releases the lock. If shutdown times out, the daemon reports the writer failure and interrupts the connection before joining; it must never silently detach a live database writer. Tests use temporary data paths and may be rerun safely.

## Artifacts and Notes

The original failure was observed as:

    Could not save read status for a3664f85: database is locked: Error code 5: The database file is locked

The important call chain was:

    dashboard/io.rs spawn_read_receipt_persist
      -> hel_database::advance_client_read_frontier
      -> hel_database::advance_viewed_through_event_ordinal

concurrent with daemon session synchronization:

    hel_session_manager.rs apply_event_page
      -> hel_database::apply_projection_page_to
      -> BEGIN IMMEDIATE

The final implementation should make the first chain a daemon protocol request and make both chains jobs on the same connection.

## Interfaces and Dependencies

No new third-party crate is required. Use `std::sync::mpsc::sync_channel` for the bounded FIFO and existing error/context conventions. The database service in `src/hel_database.rs` should expose a cloneable submission handle with typed internal execution and an owning shutdown handle. Only database module code receives `&mut rusqlite::Connection`; protocol and UI layers receive domain types.

`src/hel_database/schema.rs` must provide distinct writer/initializer and read-only opening functions. `src/hel_controller.rs` must expose writer startup through a method requiring `&ControllerStoreGuard`. `crates/hel-cli/src/daemon.rs` must own the service lifetime, include writer-backed mutation actions in `DaemonAction`, and expose matching async methods on `DaemonClient`. The UI methods must remain nonblocking by invoking those async methods only in Tokio tasks or existing supervised worker threads.

Revision note (2026-08-31 21:00Z): Created the ExecPlan from the completed lock-contention diagnosis and the selected daemon-owned-writer design so implementation and validation can proceed from a self-contained artifact.

Revision note (2026-08-31 22:02Z): Recorded the completed implementation, protocol boundary, validation results, chaos evidence, and the observed nondeterministic journal-worker settlement timeout and clean retry.
