# Hel Linux/WSL beta acceptance

This file is the evidence ledger for deciding whether a particular Hel commit is ready to show to other Linux and WSL users. Automation existing is not evidence that it passed: every blank field below must link to or name retained output for the exact candidate commit.

## Candidate

    commit:
    evaluated_at_utc:
    evaluator:
    Linux distribution / WSL version:

The candidate is not beta-ready until all gates below are complete. macOS compile and unit tests remain required by the ordinary CI matrix, but Apple-container qualification, personal credentials, SSH/AWS targets, and Tailscale infrastructure are separate manual checks and do not substitute for these isolated runtime gates.

## Required evidence

- Pull request CI is green for formatting, `cargo clippy --all-targets -- -D warnings`, Linux/macOS tests, Windows compilation, dependency policy, full corrected coverage, and the deterministic three-client smoke job. Workflow run:
- Seven consecutive scheduled Reliability workflows are green on this commit or descendants that do not change runtime code. Record run URLs and commits in chronological order:
  1.
  2.
  3.
  4.
  5.
  6.
  7.
- A 30-to-60-minute seeded soak passed on the exact candidate commit with no invariant violation or leaked process. Record workflow run, first seed, duration, iterations, and artifact name:
- A Luna campaign followed `.agents/docs/luna-reliability-runbook.md` from a fresh isolated lab. Record seed, artifact directory or archive, completed mission cards, and findings disposition:
- The latest crash-matrix artifacts contain successful SQLite integrity and foreign-key checks for every named hook, plus a green worker-topology run. Artifact:
- The latest Chromium artifact contains a readable `browser-trace.zip`, both authentication paths passed without secrets in artifacts, the browser reconverged after TUI lifecycle work, and process leak count was zero. Artifact:

## Stop-ship audit

Search open issues and the Luna notes for each category. Any known reproducible defect in one of these categories blocks beta regardless of other green checks:

- loss of an acknowledged relay event or checkpoint;
- duplicate transcript projection;
- Stop, checkpoint, recovery, or cancellation that cannot settle within its documented bound;
- viewer code, QR token, cookie, harness credential, or profile environment leaked into logs, traces, screenshots, or public API payloads;
- terminal corruption, post-frame stderr, unbounded UI cleanup, or an event/render-loop hang;
- TUI/web revision regression or cross-client state divergence; or
- a campaign-owned child process or worktree remaining after successful cleanup.

For every investigated issue, record either its fixing regression test and commit or why it is outside these stop-ship categories:

    issue / finding:
    disposition:
    regression:

## Decision

    [ ] All required evidence is attached.
    [ ] The stop-ship audit has no unresolved item.
    [ ] The candidate is approved for Linux/WSL beta use.

Decision notes:
