# Release Hel 0.4.2

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds. Maintain it in accordance with `.agents/PLANS.md`.

## Purpose / Big Picture

Publish the current `master` state, including the corrected Docker guide source package, as Hel 0.4.2. Success means the versioned release commit passes all repository gates, branch CI and Coverage pass on that exact commit, the immutable annotated `v0.4.2` tag is pushed, the release workflow succeeds, all four crates are published, and the documented installer works against the tag.

## Progress

- [x] (2026-09-01 12:45Z) Read `.agents/PLANS.md` and `RELEASING.md`; confirmed clean `master`, current version 0.4.1, and latest tag `v0.4.1`.
- [x] (2026-09-01 12:48Z) Bumped and synchronized version 0.4.2, lockfile, and generated license report.
- [x] (2026-09-01 12:53Z) Ran version check, locked metadata, formatting, full tests, Clippy with warnings denied, and dirty-candidate package verification for both publishable roots.
- [ ] Commit and push the release candidate, then wait for exact-commit CI and Coverage success (first candidate `41703fd`: Coverage passed, CI failed because local license generation consolidated duplicate MIT blocks differently from CI; correction in progress).
- [ ] Recheck the clean release commit, create and push annotated tag `v0.4.2`.
- [ ] Watch release and publication workflows and verify release assets, crates, and installer.

## Surprises & Discoveries

- Observation: Cargo refuses the runbook's package commands before the release files are committed because the tree is necessarily dirty.
  Evidence: `cargo package --locked -p hel-core` reported the three modified release files and required `--allow-dirty`; both candidate packages subsequently verify-built with that flag. The exact commands remain required on the clean release commit before tagging.

- Observation: Regenerating the license report consolidated duplicate MIT license blocks for `mimalloc` and `libmimalloc-sys`.
  Evidence: The local generator made both crates share one `MIT License` section, but exact-commit CI failed `Check shipped notice reports`. The CI-reproducible prior report keeps separate blocks; the correction preserves that structure and changes only the four Hel version links.

## Decision Log

- Decision: Release 0.4.2 as the next patch version.
  Rationale: The workspace and newest existing tag are 0.4.1, and the change being released is a packaging correction appropriate for a patch release.
  Date/Author: 2026-09-01 / Codex

## Outcomes & Retrospective

In progress.

## Context and Orientation

The root `Cargo.toml` owns the shared version for four published crates and repeats versions for two published path dependencies. `scripts/release-version.mjs` synchronizes and checks those values. `Cargo.lock` and `licenses/THIRD_PARTY_LICENSES.html` embed the version. `RELEASING.md` is authoritative: no tag may be made until the release commit is clean, locally validated, pushed, and both branch CI and Coverage succeed on that exact commit.

## Plan of Work

Change `[workspace.package] version` in `Cargo.toml` to 0.4.2, run the synchronization script, refresh Cargo metadata, and regenerate the license report. Run every command in the release validation list, with Rust tests outside the restricted sandbox. Commit only release-owned files, push `master`, and use GitHub Actions status for the exact commit rather than accepting stale checks. After success, rerun the version and metadata checks on the clean commit, create an annotated tag, push it, watch the release and publication workflows, and verify their external outputs.

## Concrete Steps

Run from `/home/ryan/code/hel`:

    node scripts/release-version.mjs sync
    cargo metadata --format-version 1 >/dev/null
    cargo about generate --workspace --offline --config licenses/about.toml --locked --fail licenses/about.hbs -o licenses/THIRD_PARTY_LICENSES.html
    node scripts/release-version.mjs check v0.4.2
    cargo metadata --locked --format-version 1 >/dev/null
    cargo fmt --check
    cargo test
    cargo clippy --all-targets -- -D warnings
    cargo package --locked -p hel-core
    CARGO_BUILD_TARGET=x86_64-unknown-linux-gnu cargo package --locked -p hel-voice-worker

After committing and pushing, wait for CI and Coverage on the commit SHA. Then repeat the clean-commit checks, tag, push, and monitor GitHub workflows as prescribed by `RELEASING.md`.

## Validation and Acceptance

All local commands must exit zero. GitHub CI and Coverage must conclude successfully for the release commit. `Release Hel` and the dispatched crates.io publication must succeed for `v0.4.2`. The GitHub Release must contain checksums and archives for x86-64 Linux musl, ARM64 Linux musl, and ARM64 macOS. crates.io must show version 0.4.2 for `hel-core`, `hel-tui`, `hel-cli`, and `hel-voice-worker`; the documented installer must install from the new tag.

## Idempotence and Recovery

Synchronization, generation, and validation commands are repeatable. Do not create the tag until all pre-tag gates pass. Once a public release exists, never move the tag; correct any defect with a subsequent patch release. Workflow publication skips already-published crate versions, so a partial publication can be resumed per `RELEASING.md`.

## Artifacts and Notes

Initial state:

    master matches origin/master
    workspace version: 0.4.1
    latest tag: v0.4.1

## Interfaces and Dependencies

No product interface changes are required. This release uses the existing Node version script, Cargo metadata and package tooling, `cargo-about` license generator, Git, GitHub Actions workflows, GitHub Release, and crates.io Trusted Publishing configured in `RELEASING.md`.

Revision note (2026-09-01): Created the release execution record after inspecting the authoritative runbook and repository state.

Revision note (2026-09-01): Recorded synchronized artifacts, passing local candidate gates, and Cargo's dirty-tree packaging constraint.

Revision note (2026-09-01): Recorded the first candidate's CI license-report failure and the correction that preserves the previously CI-generated report structure.
