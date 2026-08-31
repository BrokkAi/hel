# Releasing Hel

Releases are maintainer-driven. This is the tagging runbook; see
[CONTRIBUTING.md](CONTRIBUTING.md) for development setup, runtime invariants,
tests, and dependency-license maintenance.

## Versions

All four published crates carry the same version: `hel-core` (the root
package; it publishes under that name because crates.io's `hel` belongs to an
unrelated crate, while the library keeps the `hel` crate name), `hel-tui`,
`hel-cli` (installs the `hel` binary), and `hel-voice-worker`. Each one
inherits `[workspace.package] version` in the root `Cargo.toml`, so a bump
edits that single line.

Cargo also requires each published path dependency to carry a registry
version and does not let that version inherit, so `[workspace.dependencies]`
in the same file repeats the number. After bumping `[workspace.package]`, run:

```sh
node scripts/release-version.mjs sync
cargo metadata --no-deps --format-version 1 > /dev/null   # refreshes Cargo.lock
```

CI runs `node scripts/release-version.mjs check` and fails when those repeats
fall behind the workspace version. `install.sh`'s `SCRIPT_VERSION` is an
independent installer logging revision and is not automatically synchronized
to product releases.

`licenses/THIRD_PARTY_LICENSES.html` embeds the workspace crate versions, so a
version bump must regenerate it. CI diffs the checked-in report against a fresh
`cargo about generate` and fails on any difference.

## What a tag triggers

A `vX.Y.Z` tag triggers the GitHub release workflow. The docs workflow is
manual-only for now; Hel does not publish a docs site yet.

The release workflow opens with a coverage gate and builds nothing until it
passes. CI's branch and pull request triggers do not match tags, so this is the
only check that re-runs against the tagged tree. Collecting coverage runs the
whole workspace test suite, which means a failing test and a coverage
regression both stop the release; tagging a commit whose coverage run was red
on master fails here rather than shipping.

The gate covers Linux tests and the coverage baseline only. Formatting, Clippy,
the macOS and Windows test runs, the Android target check, and the
dependency-license checks stay pull request checks, so a tag still relies on the
tagged commit having passed CI on master.

The builds cover Linux x86-64 and ARM64, Android ARM64, Windows x86-64, and a
universal macOS archive. Desktop archives contain `mj` and the voice worker;
Android omits the voice worker. Every archive includes the applicable licenses
and notices and is published with a SHA-256 sidecar.

The crates.io publish does not run off the tag push. It waits for the GitHub
Release to be published, so the coverage gate and a build failure on any
target each stop the release before anything reaches crates.io.

## Discord announcement

To announce a published GitHub Release in Discord, set the
`DISCORD_RELEASE_WEBHOOK_URL` repository Actions secret to the target channel's
webhook URL. The release workflow reuses GitHub's generated release notes,
prevents mentions from being parsed, suppresses automatic link embeds, and
leaves a failed Discord delivery as a warning so it cannot invalidate an
already-published release.

## crates.io publishing

`publish.yml` publishes `hel-core`, `hel-tui`, `hel-cli`, and
`hel-voice-worker`, in that dependency order. It refuses to publish when the
tag differs from any crate version, and packages all four crates ahead of the
`crates-io` environment gate so a packaging failure surfaces without spending
an approval (`hel-tui` and `hel-cli` package with `--no-verify` because their
dependencies are not on the registry until the same run publishes them).

Publishing runs automatically once the release workflow succeeds. The automated
release job explicitly dispatches `publish.yml` after creating the GitHub
Release. This uses a trigger supported by crates.io trusted publishing; GitHub
does not emit a second workflow from release events created with its workflow
token, and crates.io rejects the `workflow_run` trigger. A release published by
another actor also starts `publish.yml` through its release event.

Each crate is skipped when that version is already on the registry. That is the
recovery path if one crate publishes and the other fails: re-running resumes at
the crate that did not land. crates.io reserves a version number permanently
once published and yanking does not release it, so a shipped version can never
be republished.

To package a tag without publishing, run the workflow manually with `publish`
off and inspect its `.crate` artifact.

## Before tagging

Confirm that:

1. Both crate manifests and their `Cargo.lock` workspace entries match the intended tag.
2. Formatting, Clippy, release builds, tests, and relevant cross-platform or
   packaging checks pass.
3. Dependency-license policy and generated notice reports are current.
4. User-facing installation, configuration, and release documentation reflects
   the shipped behavior.
5. The release commit is merged and the tagged commit is the exact commit meant
   to be published.
