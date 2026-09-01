# Preserve synchronized GitHub authentication in harness shells

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

This plan is maintained in accordance with `.agents/PLANS.md` from the repository root.

## Purpose / Big Picture

After this change, a newly provisioned Hel session with a synchronized controller GitHub token can run `gh auth status`, `git ls-remote`, and `git push` from a Codex terminal without being told that the container has no credentials. Hel will continue to keep the token in the worker-private token file and expose it only through its `gh` wrapper; the fix must not place the token itself in the long-lived harness environment.

The behavior is visible in a regression test that launches a real shell with the same login-shell boundary used by Codex commands and proves that `gh` resolves to Hel's wrapper. A separate helper-level assertion must prove that Git's HTTPS credential helper invokes the absolute wrapper path so Git authentication does not depend on shell PATH lookup.

## Progress

- [x] (2026-08-31 02:54Z) Diagnosed the live `morannon-podman` session and captured the failing Codex terminal evidence.
- [x] (2026-08-31 02:54Z) Confirmed that controller token discovery, worker token installation, and the wrapper itself are healthy; the failing Codex terminal resolved `/usr/bin/gh`.
- [x] (2026-08-31 03:00Z) Added and ran focused fail-before behavior coverage; the login shell selected the fake underlying `gh`, which observed both token variables unset instead of receiving the synchronized test token.
- [x] (2026-08-31 03:06Z) Implemented worker-private Bash environment chaining plus process-scoped absolute GitHub credential helpers without exporting `GH_TOKEN` or `GITHUB_TOKEN` to the harness.
- [x] (2026-08-31 03:06Z) Ran focused login-shell coverage, the 72-test worker relay module, formatting, the full test suite, and warning-denied Clippy outside the restricted sandbox; all required gates passed.
- [x] (2026-08-31 03:07Z) Updated this plan with final evidence and committed only the plan, worker-runtime implementation, and behavior test to the current branch without pushing.

## Surprises & Discoveries

- Observation: The live worker already contained a valid synchronized token and functioning wrapper.
  Evidence: `/var/lib/hel/workers/8c3a84a6492ea8e7c0c9452ef3b4f253/github-token` and the adjacent `bin/gh` were created at session provisioning, and invoking that wrapper made `gh auth token --hostname github.com` succeed without exposing the token.

- Observation: The failing Codex terminal did not use the wrapper even though its ancestor processes inherited a PATH containing the wrapper directory.
  Evidence: the materialized terminal record for `command -v gh && gh auth status` returned `/usr/bin/gh` followed by `You are not logged into any GitHub hosts`; the preceding HTTPS `git ls-remote` and `git push` both failed with `could not read Username`.

- Observation: Hel already repairs wrapper lookup for its own user-shell RPC, but that repair does not cover shells spawned internally by an ACP harness such as Codex.
  Evidence: `src/hel_user_shell.rs` calls `github_cli_login_shell_command`, while the supervised ACP bridge in `src/hel_worker_runtime/unix.rs` receives only environment variables and later owns its own terminal subprocesses.

- Observation: Bash reads `BASH_ENV` after a non-interactive login profile has reset PATH.
  Evidence: the fail-before regression preserved an existing `BASH_ENV` marker but selected the underlying fake `gh`; this provides a deterministic seam where Hel can chain the existing hook and restore only its wrapper directory.

- Observation: Re-running the existing GitHub CLI setup duplicated the wrapper directory in PATH.
  Evidence: the new idempotence assertion observed two identical worker `bin` entries after a second call. Setup now interprets PATH with `std::env::split_paths` and leaves an existing wrapper entry in place.

## Decision Log

- Decision: Fix both interactive `gh` lookup and Git's credential-helper lookup while keeping token contents out of the harness environment.
  Rationale: PATH repair makes direct `gh` commands truthful, while an absolute Git credential helper makes HTTPS Git robust even if a harness starts a shell that rewrites PATH in an unanticipated way. Both routes still read the worker-private token only for the lifetime of the wrapper process.
  Date/Author: 2026-08-31 / Codex.

- Decision: Keep the implementation in the existing worker-runtime GitHub CLI setup rather than special-case Codex.
  Rationale: the token wrapper and credential behavior are target-runtime concerns shared by every ACP harness. A Codex-only prompt or configuration workaround would leave the primary design broken for other harnesses that start login shells.
  Date/Author: 2026-08-31 / Codex.

## Outcomes & Retrospective

The implementation is complete and validated. New worker startups create a private Bash environment fragment that chains a profile's existing `BASH_ENV` and then restores Hel's wrapper directory after login-profile PATH changes. The harness process tree also receives Git configuration entries that clear lower-priority GitHub/Gist helpers and invoke the session's wrapper by absolute path. Token contents remain absent from the ACP bridge and ordinary shell environment; the wrapper reads the private token file only for each real `gh` invocation.

The regression reproduces both user-visible paths with a real `bash -lc`: `command -v gh` selects the worker wrapper and a direct `gh` call receives the synchronized synthetic token, while `git credential fill` reaches the same wrapper even when Git begins with inherited configuration. Re-running setup produces an identical environment, which also corrected duplicate PATH entries from the prior implementation.

## Context and Orientation

The controller discovers a canonical GitHub token in `src/hel_controller/backend.rs::controller_github_token`. For managed targets, provisioning passes that secret into the new container and converts GitHub repository URLs to HTTPS. Periodic reconciliation in `src/hel_worker_client.rs::reconcile_github_token` installs the current token into a worker-private file without exposing its value in logs.

The target-side worker starts in `src/hel_worker_runtime/unix.rs::run_daemon`. Its `configure_github_cli` function creates a private `bin/gh` shell wrapper beside the token file and prepends the wrapper directory to the ACP bridge environment's PATH. The wrapper removes itself from PATH to avoid recursion, reads the token file into `GH_TOKEN` only for the child `gh` process, and then invokes the image's real GitHub CLI. The worker deliberately removes inherited `GH_TOKEN` and `GITHUB_TOKEN` from long-lived bridge and user-shell subprocesses.

The development container bootstrap in `src/hel_targets.rs::container_git_bootstrap_script` currently configures Git's global HTTPS credential helper as `!gh auth git-credential`. That command relies on PATH lookup. Codex terminal commands are descendants of the ACP bridge, but the harness can start a login shell that reconstructs PATH and selects `/usr/bin/gh`, bypassing Hel's wrapper.

## Plan of Work

First add a behavior test next to the worker runtime tests. It must construct a temporary worker root, install a fake real `gh`, call the production GitHub CLI setup, and launch the relevant shell boundary with the returned environment. The fake real CLI should report whether the wrapper supplied `GH_TOKEN`, using a token longer than a toy fixture where practical but never printing a real credential. The test must fail on the current implementation by resolving the real/fake underlying CLI rather than the worker wrapper after the shell resets PATH.

Then update `src/hel_worker_runtime/unix.rs::configure_github_cli` and the narrow shared helpers it uses. Create a worker-private shell environment fragment that idempotently restores the wrapper directory from `HEL_GITHUB_CLI_BIN` after non-interactive shell startup, and arrange for Bash descendants to read it. Preserve an explicitly configured shell environment hook by composing rather than silently discarding it, or choose an equally scoped mechanism if the fail-before test proves Bash's environment hook unsuitable. The fragment must never contain the token.

Also make the HTTPS Git credential path independent of ordinary PATH lookup. Prefer configuring the credential helper during worker startup to invoke the absolute wrapper created for that worker. Reuse existing subprocess helpers rather than hand-rolling process pipes, and avoid changing unrelated user Git configuration. If Git supports a repository-local or environment-scoped configuration that reaches the session repository and additional roots, use that; otherwise document and test the narrowly scoped global configuration inside the one-session managed container.

Finally run the focused worker-runtime tests and all repository-required validation. Update this plan with the exact tests and outcomes, including any platform qualification. Commit the plan, code, and behavior tests to the current branch, staging only those files.

## Concrete Steps

All commands run from `/home/jonathan/Projects/hel2`.

Inspect the relevant surfaces:

    rg -n 'configure_github_cli|github_cli_login_shell_command|credential.https://github.com.helper|BASH_ENV' src crates

Run focused tests outside the restricted sandbox:

    cargo test hel_worker_runtime::relay_tests -- --nocapture

After implementation, run:

    cargo fmt --all -- --check
    cargo test
    cargo clippy --all-targets -- -D warnings

Expected focused evidence is a test in which a login-shell descendant resolves the worker-private `bin/gh`, the wrapper injects a synthetic token only into the real `gh` child, and Git's configured helper names the absolute worker wrapper rather than bare `gh`.

Final observed results:

    cargo test hel_worker_runtime::relay_tests -- --nocapture
    71 passed; 0 failed; 1 ignored

    cargo test
    CLI: 70 passed
    logging: 1 passed
    PTY: 2 passed
    core: 1435 passed; 4 ignored
    import e2e: 4 ignored because they require live harness logins and Podman
    TUI: 199 passed; 1 ignored
    doctests: passed

    cargo clippy --all-targets -- -D warnings
    Finished successfully with no warnings

## Validation and Acceptance

The change is accepted when the new behavior test fails on the pre-change implementation and passes after the fix; direct `gh auth status` semantics and HTTPS Git authentication both route through the worker wrapper even across a login-shell boundary; no long-lived ACP or shell process receives `GH_TOKEN` or `GITHUB_TOKEN`; existing credential install, removal, symlink-safety, and user-shell tests remain green; `cargo test` passes; and `cargo clippy --all-targets -- -D warnings` reports no warnings.

A newly provisioned or re-provisioned managed session is the user-visible rollout boundary because the worker runtime setup is performed when the worker daemon starts. Existing running sessions are not modified in place by this source change.

## Idempotence and Recovery

Worker setup must be safe to run again for the same root. Rewriting the wrapper and shell environment fragment must use existing atomic-write and symlink-refusal patterns. Repeated setup must not duplicate PATH entries or Git configuration. If a worker has no synchronized token, the wrapper must continue to clear both GitHub token variables before invoking the real CLI, so removing a controller token immediately removes effective access.

Tests use temporary directories and synthetic tokens. They must not read or alter the developer's actual GitHub configuration. No live session, container, token file, or remote repository is changed by implementation validation.

## Artifacts and Notes

Live failing transcript from session `8c3a84a6492ea8e7c0c9452ef3b4f253`:

    $ git push origin master
    fatal: could not read Username for 'https://github.com': No such device or address

    $ command -v gh && gh auth status
    /usr/bin/gh
    You are not logged into any GitHub hosts.

Live target inspection established that the expected wrapper and token file existed and that invoking the wrapper directly authenticated successfully. Token contents were never printed.

Fail-before and pass-after behavior:

    before: login shell selected the fake underlying gh and printed unset|unset
    after:  login shell selected <worker>/bin/gh and printed synchronized-test-token|unset
    after:  git credential fill returned x-access-token with the synchronized synthetic password

## Interfaces and Dependencies

Keep `src/hel_controller/backend.rs::controller_github_token`, the relay protocol, and `src/hel_worker_client.rs::reconcile_github_token` unchanged unless testing uncovers a separate transport defect. The primary edit belongs in `src/hel_worker_runtime/unix.rs::configure_github_cli` and may add small private constants or helpers beside it. `src/hel_worker_runtime.rs::github_cli_login_shell_command` should reuse any shared PATH-restoration text rather than drift if its existing behavior remains necessary.

Use the existing atomic filesystem helpers and shared subprocess helpers. Add no crate or third-party dependency. Preserve cross-platform compilation by keeping Unix shell behavior under the existing Unix runtime module and by leaving non-Unix builds unaffected.

Revision note (2026-08-31): Initial plan records the live token-sync diagnosis and the implementation/validation path for preserving wrapper selection in harness-owned shells.

Revision note (2026-08-31): Completed implementation details, idempotence discovery, and final focused/full validation evidence were added after all gates passed.
