# Releasing Hel

Releases are maintainer-driven. This is the authoritative tagging runbook; see
[CONTRIBUTING.md](CONTRIBUTING.md) for development setup, runtime invariants,
tests, and dependency-license maintenance.

## Version contract

All four published crates inherit the release version from
`[workspace.package]` in the root `Cargo.toml`: `brokk-mj-core`, `brokk-mj-tui`,
`brokk-mjolnir`, and `brokk-mj-voice-worker`.

Cargo requires published path dependencies to carry a registry version and
does not allow that version to inherit. The `hel` and `brokk-mj-tui` entries under
`[workspace.dependencies]` therefore repeat the release version. After changing
the workspace package version, synchronize those entries with:

```bash
node scripts/release-version.mjs sync
```

`Cargo.lock` and `licenses/THIRD_PARTY_LICENSES.html` also embed the workspace
version and must be refreshed before tagging. `install.sh`'s `SCRIPT_VERSION`
is an independent installer logging revision; do not synchronize it to the
product version.

## One-time crates.io setup

Each published crate must authorize this repository before the workflow can
exchange GitHub's OIDC identity for a short-lived crates.io token. In the
crates.io settings for `brokk-mj-core`, `brokk-mj-tui`, `brokk-mjolnir`, and
`brokk-mj-voice-worker`, create a GitHub Trusted Publisher with these exact values:

- GitHub owner: `BrokkAi`
- Repository: `hel`
- Workflow: `publish.yml`
- Environment: `crates-io`

The GitHub repository must also have an environment named `crates-io`; use its
deployment protection rules for any desired maintainer approval. No long-lived
crates.io token or GitHub secret is needed. Trusted Publisher configuration is
one-time per crate, but confirm it before tagging the first release that uses
OIDC: a missing configuration is discovered only after the GitHub Release is
already public.

## Release procedure

1. Set `[workspace.package] version` in the root `Cargo.toml` to the next
   version. Do this before creating a tag.
2. Synchronize the repeated versions, refresh the lockfile, and regenerate the
   license report:

   ```bash
   node scripts/release-version.mjs sync
   cargo metadata --format-version 1 >/dev/null
   cargo about generate --workspace --offline --config licenses/about.toml \
     --locked --fail licenses/about.hbs -o licenses/THIRD_PARTY_LICENSES.html
   ```

3. Validate the candidate version and repository:

   ```bash
   node scripts/release-version.mjs check vX.Y.Z
   cargo metadata --locked --format-version 1 >/dev/null
   cargo fmt --check
   cargo test
   cargo clippy --all-targets -- -D warnings
   cargo package --locked -p brokk-mj-core
   CARGO_BUILD_TARGET=x86_64-unknown-linux-gnu \
     cargo package --locked -p brokk-mj-voice-worker
   ```

   Run `cargo test` outside the restricted sandbox as required by `AGENTS.md`.
   Run any additional cross-platform, packaging, or manual checks relevant to
   the changes in the release.

4. Commit the version bump and generated files. Push that commit and wait for
   both the branch CI and Coverage workflows to pass on that exact commit. Do
   not tag a candidate with a skipped, cancelled, stale, or failing check.
   Re-run the version checks on the clean commit that will be tagged.
5. Create and push an annotated tag on that exact commit:

   ```bash
   git tag -a vX.Y.Z -m "Release X.Y.Z"
   git push origin vX.Y.Z
   ```

6. Watch the `Release Hel` workflow. It checks the tag against the workspace
   version and lockfile and verify-builds the publishable source packages
   before any release build. It then packages x86-64 Linux musl, ARM64 Linux
   musl, and ARM64 macOS archives, publishes the GitHub Release and checksums,
   and dispatches the crates.io workflow.
7. Approve the `crates-io` environment when requested. `publish.yml` verifies
   and packages the tagged source, authenticates through each crate's Trusted
   Publisher configuration, then publishes `brokk-mj-core`, `brokk-mj-tui`, `brokk-mjolnir`,
   and `brokk-mj-voice-worker` in dependency order. An already-published crate
   version is skipped, so rerunning the workflow safely resumes a partial
   publication.
8. Verify the GitHub Release assets and confirm all four crates show `X.Y.Z` on
   crates.io. Test the documented installer against the new tag.

To package an existing tag without publishing crates, run `publish.yml`
manually with `publish` disabled and inspect its `.crate` artifact.

If crates.io authentication failed because Trusted Publishing was not
configured, add the four configurations above and retry the existing immutable
tag with:

```bash
gh workflow run publish.yml \
  --ref vX.Y.Z \
  --raw-field release_tag=vX.Y.Z \
  --raw-field publish=true
```

## Recovery

Do not move or overwrite a tag after its GitHub Release is public. Release
assets and cloned tags may already be cached even when download counts are low.
If a bad tag was published, document the bad release, make the correction on
the branch, and release the next patch version. A crates.io version is reserved
permanently once published; yanking it does not make the version reusable.
