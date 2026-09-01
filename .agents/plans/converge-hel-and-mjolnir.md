# Converge Hel and Mjolnir history

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

The canonical rules for ExecPlans live in `.agents/PLANS.md`, relative to the repository root. This document must be maintained in accordance with that file.

## Purpose / Big Picture

Mjolnir 2.0 took Hel's session-control-plane source and then changed the public package, executable, configuration, environment-variable, documentation, and deployment identities from Hel to Mjolnir. Hel continued to receive work after that cutover. This integration must bring those newer behavioral changes into Mjolnir without reverting its identity, and then publish one merge history back to Hel so both repositories share a tip and future updates no longer require a rename-aware port. Both GitHub repositories must also stop offering squash or rebase merges so future pull requests preserve this ancestry.

Success is observable in four ways. The resulting commit is a descendant of both the pre-integration Mjolnir and Hel tips; the working tree retains the `mj`, `brokk-mj-*`, `.mjolnir`, and `MJ_*` public identities while containing Hel's post-cutover behavior; the complete Rust test and lint gates pass; and GitHub reports merge commits enabled with squash and rebase merges disabled. After publication, `hel/master` resolves to the converged commit rather than its old divergent tip.

## Progress

- [x] (2026-09-01 17:47Z) Confirmed a clean `master` at `ab52f179`, an existing `hel` remote, and the deliberate split between internal `hel_*` Rust module names and Mjolnir's public identities.
- [x] (2026-09-01 17:47Z) Read `.agents/PLANS.md` and began this ExecPlan before changing repository history.
- [x] (2026-09-01 17:49Z) Fetched and pruned both remotes. The tips remained `origin/master=ab52f179` and `hel/master=d2a62062`; the semantic Hel range is 30 commits and 53 paths after `7742631f`.
- [x] (2026-09-01 17:50Z) Changed `BrokkAi/mjolnir` repository settings to allow merge commits and disallow both squash and rebase merges.
- [x] (2026-09-01 17:52Z) Merged retained `mj2` tip `ab1d5f55` as ancestry-only bridge `f88dc9fe`. Its tree equals its first parent exactly, and the Hel merge base is now `7742631f`.
- [x] (2026-09-01 18:20Z) Committed the resolved Hel merge as `8f16c990`, then merged Hel's one newer minimized-grid commit as `e861523d` with no conflicts.
- [x] (2026-09-01 18:18Z) Corrected the complete TUI/web state-projection findings with focused behavior tests. Deferred the broader daemon/review-host refactor at the user's direction so convergence can ship and follow-up fixes can land on the shared history instead of racing it.
- [x] (2026-09-01 18:20Z) Passed formatting, locked metadata, viewer JavaScript syntax, release-version check, full `cargo test`, warnings-denied Clippy, optimized release build, focused UI/web tests, and the eight minimized-grid tests after the final Hel delta.
- [x] (2026-09-01 18:21Z) Fast-forwarded both `hel/master` and `origin/master` to `e861523d`; local and both remote-tracking refs contain the same commit.
- [x] (2026-09-01 18:31Z) Changed `BrokkAi/hel` to the same merge-only policy so a PR through either repository cannot discard the shared ancestry again.

## Surprises & Discoveries

- Observation: Mjolnir did not begin as a simple file-by-file rename. Commit `c1a123aa` joined the old Mjolnir and Hel histories using Hel's tree, and follow-up commit `bd326e2b` applied the Mjolnir 2.0 public/deploy identity changes. The shared ancestor reported before refreshing remotes was `e2fc032`.
  Evidence: `git cat-file -p c1a123aa` shows parents `7742631f` and `dc93098c`; its commit message says that Hel forked at `e2fc032` and that follow-up commits restore Mjolnir identities.

- Observation: internal Rust library and module names intentionally remain Hel-flavored even in Mjolnir.
  Evidence: root `Cargo.toml` publishes package `brokk-mj-core` but declares `[lib] name = "hel"`, with a comment explaining that source imports should not be renamed.

- Observation: the ordinary Git merge base between `master` and `hel/master` is unusably old because GitHub squash-merged PR #948 and discarded the imported ancestry. The retained local `mj2` branch still has that ancestry, and its final tree is byte-for-byte identical to the squash result before Mjolnir's final gate fix.
  Evidence: `git merge-base master hel/master` reports `e2fc032f`, implying 813 Hel-side commits, while `git merge-base mj2 hel/master` reports the intended Hel import `7742631f`. `git diff --quiet bd326e2b mj2` succeeds; only `licenses/about.toml` and `scripts/check-coverage.mjs` differ between `mj2` and current `master`.

- Observation: the refreshed Hel range after the true import contains 24 non-merge commits, 30 commits including merges, changing 53 paths with 6,665 insertions and 2,604 deletions.
  Evidence: `git rev-list --left-right --count 7742631f...hel/master` reports `0 30`, and `git diff --find-renames --stat 7742631f hel/master` reports the path and line counts.

- Observation: after restoring the true base, Git's rename-aware merge mapped all modified `crates/hel-cli/` and `crates/hel-tui/` files onto `mj-cli/` and `mj-tui/` automatically. It reported only nine unresolved paths: seven textual conflicts, one moved new test, and the intentional greeting deletion.
  Evidence: the merge output identified conflicts in `Cargo.lock`, `mj-cli/src/daemon.rs`, `mj-tui/src/lib.rs`, `mj-tui/src/render.rs`, `src/hel_controller.rs`, `src/hel_database/schema.rs`, and `src/web/viewer.js`; mapped `store_divergence.rs` into `mj-cli/tests/`; and reported the modify/delete decision for `src/hel_greeting.rs`.

- Observation: the merged code compiled and its full test suite passed, but independent behavior review found missing production transitions that the upstream tests did not exercise. Review-host state changes update a private map without advancing the daemon revision, the durable state is never marked active, prompt admission remains open during asynchronous preparation, and UI projections can discard or mis-bind already-open reviews.
  Evidence: the first `cargo test` pass reported 1,590 core, 236 TUI, 88 CLI, one store-divergence integration, one logging integration, and two PTY tests passing. Code inspection then traced host publication to `src/hel_review/host.rs::publish` without a daemon notification, found no assignment of `TurnReviewState.active = Some(...)`, found `hold_prompts` only after preparation, and found the web review signature omitted the session id.

- Observation: the new phone command projection is computed before session capabilities and omits harness-advertised commands, while the dashboard consumes review/config feeds without applying their current values to a chat opened afterward.
  Evidence: `mj-cli/src/server.rs::viewer_snapshot` called `phone_commands` before `session_capabilities`; `RelayOperationalState.available_commands` was unused by that projection; `mj-cli/src/dashboard.rs::drain_runtime_reviews` discarded unmatched views; and `ChatState` began with `ReviewConfig::default()` even though its session context already carried current config.

- Observation: Hel moved once during integration, from `d2a62062` to `58659f1f`, adding only the minimized-grid hidden-session marker in `crates/hel-tui/src/render.rs`.
  Evidence: the refreshed range `d2a62062..58659f1f` contains one commit and one changed file. Because the ancestry bridge is already present, this is an ordinary incremental merge rather than a repeat rename port.

## Decision Log

- Decision: Create real merge commits that make the final history descend from the retained `mj2` line and both repositories, rather than copying patches or squashing Hel's changes.
  Rationale: the user's goal is to push the result back to Hel and stop repeating this integration. Shared ancestry permits subsequent synchronization to fast-forward and preserves provenance for both lines.
  Date/Author: 2026-09-01, Codex.

- Decision: Treat `src/hel_*` names and the `hel` Rust library name as shared implementation identity, but treat `mj`, `brokk-mj-*`, `.mjolnir`, `MJ_*`, Mjolnir documentation, release automation, and deployment artifacts as downstream public identity that must survive conflict resolution.
  Rationale: root `Cargo.toml` explicitly documents this boundary, while the Mjolnir 2.0 cutover commits intentionally changed the user-facing and release-facing names.
  Date/Author: 2026-09-01, Codex.

- Decision: First merge local `mj2` commit `ab1d5f55`, then merge `hel/master` normally.
  Rationale: `mj2` contains the original merge commit that joined Hel at `7742631f`, but its tip tree matches the squash that landed on `master`. This history bridge restores the correct base without reverting product code or fabricating a custom merge tree. The bridge's only expected textual conflict is the coverage crate-path regex, where the current `mj-(cli|tui)` spelling must win.
  Date/Author: 2026-09-01, Codex.

- Decision: Resolve each content conflict semantically instead of mechanically selecting one whole side.
  Rationale: Hel contains the newer behavior, while Mjolnir contains newer identity and integration work. Either blanket side would silently lose valid changes.
  Date/Author: 2026-09-01, Codex.

- Decision: Enforce merge commits for future pull requests in both repositories by disabling both squash and rebase merge methods in GitHub.
  Rationale: merely enabling merge commits leaves the ancestry-destroying buttons available. Making it the only merge method prevents this failure mode from recurring through the normal PR UI.
  Date/Author: 2026-09-01, Codex.

- Decision: Accept Hel's removal of the contextual greeting module and show the workspace name in the compact dashboard, while retaining Mjolnir wording in onboarding and tests.
  Rationale: greeting removal is an intentional upstream behavior change tied to the new minimized grid, not a branding regression. Keeping the deleted module would preserve dead startup work and contradict the new title behavior.
  Date/Author: 2026-09-01, Codex.

- Decision: Translate new product-owned protocol and UI identifiers to Mjolnir while retaining internal `hel` Rust identifiers. In particular, the new diff metadata key is `dev.mj.diffPatch`, its truncation marker says `[mj dropped ...]`, and the phone command type is `ViewerMjCommand`.
  Rationale: these identifiers did not exist at the import seam and therefore have no Hel compatibility burden; the Mjolnir 2.0 hard split already established `dev.mj.*` and `[mj ...]` for product-owned metadata and user-visible truncation markers.
  Date/Author: 2026-09-01, Codex.

- Decision: Treat the independently reviewed state-propagation and prompt-admission defects as merge blockers even though upstream and merged tests pass.
  Rationale: they create stale or incorrectly targeted user controls and can allow a new prompt to race a review of the prior turn. Shipping them would violate the control-plane ownership model and make the converged history harder to repair later. Focused behavior tests will make the missing transitions observable before the merge commit is created.
  Date/Author: 2026-09-01, Codex.

- Decision: Freeze integration scope at refreshed Hel tip `58659f1f`, ship the fully implemented UI corrections, and defer the incomplete daemon/review-host refactor until after both repositories share a tip.
  Rationale: Hel's commit rate makes prolonged pre-publication hardening self-defeating. The original merged implementation already passed the full required suite; publishing a shared merge base makes subsequent fixes normal commits instead of another divergent rename integration.
  Date/Author: 2026-09-01, Codex.

## Outcomes & Retrospective

Mjolnir and Hel converged at `e861523d78e34ddf12e9f1e80b6af68eb5716254`. The final commit has parents `8f16c990` (the rename-aware integration) and `58659f1f` (the Hel commit that arrived during validation), so both original histories are retained and subsequent movement can be merged incrementally. `origin/master` and `hel/master` were both fast-forwarded to that object without force.

Both repositories now offer merge commits only: merge commits are enabled and squash and rebase merges are disabled. Public package, executable, protocol, state-directory, environment-variable, and web branding remain Mjolnir while intentional internal Rust `hel` names remain shared. The complete Rust suite, warnings-denied Clippy, optimized build, JavaScript checks, focused web tests, and focused final TUI tests passed. Broader review-host and daemon lifecycle findings were deliberately deferred until after convergence and continue under `.agents/plans/harden-turn-review-daemon.md` on the now-shared history.

## Context and Orientation

The repository root is a Rust workspace. `Cargo.toml` defines the core package and workspace dependency aliases; `mj-cli/`, `mj-tui/`, and `mj-desktop/` hold Mjolnir-facing binaries and UI crates; `src/hel_*.rs` and their submodules hold the shared implementation inherited from Hel. `README.md`, `RELEASING.md`, `install.sh`, `npm/`, `.github/workflows/`, `docs/`, and license configuration contain product and distribution identity and therefore require special scrutiny during the merge.

The `origin` remote names the Mjolnir repository and the `hel` remote names the Hel repository. A *tip* is the commit currently named by a branch. A *merge commit* is a commit with both tips as parents or ancestors; this is the history shape needed so neither repository sees the other's work as unrelated or missing after convergence.

The recorded starting tips after refresh are Mjolnir `ab52f179` and Hel `d2a62062`. Local branch `mj2` at `ab1d5f55` retains the unsquashed integration history and has the same tree as Mjolnir squash commit `bd326e2b`. The workspace was clean before this plan file was created.

## Plan of Work

First fetch both remotes and record `origin/master`, `hel/master`, their merge base, and their left/right commit counts. Inspect only the Hel commits after the semantic import commit `7742631f`, with special attention to file renames. Change the Mjolnir GitHub repository settings so only merge commits are offered for pull requests.

Next merge retained branch `mj2` into local `master` to restore the ancestry lost by the squash. Preserve the current versions of the two later gate-fix files and confirm that the bridge adds no unintended product-tree change. Once `git merge-base HEAD hel/master` resolves to `7742631f`, merge refreshed `hel/master` with commitment deferred. For ordinary shared source, combine both behaviors and retain tests from both sides. For paths renamed from `crates/hel-cli` and `crates/hel-tui` to `mj-cli` and `mj-tui`, apply Hel's newer edits to the Mjolnir path rather than resurrecting the old path. Preserve Mjolnir's package names, executable names, state directories, environment variables, URLs, release workflows, and documentation identity. Do not mass-replace the internal `hel` module or library names because they are intentional.

After conflict resolution, inspect the staged diff and compare representative shared files to Hel's tip to ensure every upstream behavior is present. Search public surfaces for newly introduced obsolete `hel`, `HEL_`, `.hel`, `hel-cli`, and `hel-tui` strings, distinguishing intentional historical/internal references from regressions. Format Rust and other generated artifacts only through their existing project commands.

Finally run the repository's complete required Rust validation. If failures expose integration defects, fix their source, add or retain behavior tests where needed, update this plan, and rerun affected gates. Commit only the files from this integration. Push the converged commit to `hel/master` as explicitly requested; update `origin/master` too only if publication scope and repository instructions authorize it, and record exact final remote object IDs.

## Concrete Steps

All commands run from `/home/ryan/code/mjolnir`.

Refresh and inspect history:

    git fetch --prune origin
    git fetch --prune hel
    git rev-parse master origin/master hel/master
    git rev-list --left-right --count master...hel/master
    git log --left-right --cherry-mark --oneline master...hel/master

Repair the lost ancestry, verify the new base, and then perform the Hel join:

    git merge --no-ff mj2
    git merge-base HEAD hel/master
    git merge --no-ff --no-commit hel/master
    git status --short

Validate the resolved tree:

    cargo fmt --all -- --check
    cargo test
    cargo clippy --all-targets -- -D warnings

Commit and verify ancestry:

    git commit
    git merge-base --is-ancestor <recorded-mjolnir-tip> HEAD
    git merge-base --is-ancestor <recorded-hel-tip> HEAD
    git status --short --branch

Publish only after all validation passes, then verify the destination ref points at the expected commit.

## Validation and Acceptance

`cargo test` must pass for the default workspace members using the repository's configured musl target. `cargo clippy --all-targets -- -D warnings` must complete without warnings. `cargo fmt --all -- --check` must report no diff. Any additional JavaScript, documentation, release, or end-to-end checks implicated by the actual changed files must also pass. A fresh GitHub repository query must report `allow_merge_commit=true`, `allow_squash_merge=false`, and `allow_rebase_merge=false`.

The merge is accepted only when both recorded starting tips are ancestors of `HEAD`, `git status --short` is empty after committing, no `crates/hel-cli` or `crates/hel-tui` tree has been accidentally recreated, and an identity audit shows Mjolnir public surfaces still use `mj`, `brokk-mj-*`, `.mjolnir`, and `MJ_*`. After the authorized push, `git ls-remote hel refs/heads/master` must report the converged commit.

## Idempotence and Recovery

Fetching and inspection are safe to repeat. Before the merge commit exists, a failed merge can be inspected and repaired in place; `git merge --abort` is the recovery path if resolution proves unsound, but it should be used only after recording any discoveries in this plan. Tests and audits are repeatable. Publication happens only after ancestry and validation checks, and uses a normal fast-forward push without force.

## Artifacts and Notes

Initial evidence:

    master      ab52f179 Fix dependency and coverage gates
    hel/master  d2a62062 Merge remote-tracking branch 'origin/master' into hel2
    raw merge-base       e2fc032f
    semantic merge-base  7742631f
    mj2 bridge tip       ab1d5f55
    ancestry bridge      f88dc9fe

These values were confirmed after refreshing both remotes. The raw merge base is recorded only to explain why a direct merge before the ancestry bridge would be wrong.

## Interfaces and Dependencies

No new runtime interface is planned. The integration must preserve the workspace package aliases in `Cargo.toml`: package `brokk-mj-core` exposes Rust library `hel`, and the Mjolnir CLI/TUI packages depend on it through the existing `hel` and `hel-tui` aliases. New Hel code may extend these interfaces; conflict resolution must carry those extensions through the existing Mjolnir package boundary rather than introducing duplicate Hel packages.

Revision note (2026-09-01 17:47Z): Created the plan after initial history and identity reconnaissance. The remote tips and exact conflict set remain to be refreshed and recorded.

Revision note (2026-09-01 17:51Z): Recorded refreshed tips and exact Hel delta; added the retained-`mj2` ancestry bridge after discovering PR #948's squash had discarded the correct merge base; and added the completed GitHub merge-policy change requested by the user.

Revision note (2026-09-01 17:56Z): Recorded the ancestry-only bridge commit, the actual conflict set, and the naming/removal decisions made while resolving the Hel merge. Validation and publication remain pending.

Revision note (2026-09-01 18:06Z): Recorded the green first validation pass and the production state-propagation defects found by independent review. Added a correction milestone before final validation and commit.

Revision note (2026-09-01 18:18Z): Froze scope at Hel tip `58659f1f` after it advanced once during the integration. Recorded the completed UI corrections and the explicit decision to publish convergence before undertaking the broader daemon refactor.

Revision note (2026-09-01 18:22Z): Closed the plan with the two merge commits, validation evidence, merge-only GitHub policy, and matching published remote tip. Linked the separate follow-up hardening plan for the intentionally deferred daemon findings.

Revision note (2026-09-01 18:31Z): Extended the merge-only policy to Hel as well; preserving ancestry requires removing squash and rebase entry points on either side of the shared history.
