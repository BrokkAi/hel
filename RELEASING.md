# Releasing Hel

Releases are maintainer-driven. This is the authoritative tagging runbook; see
[CONTRIBUTING.md](CONTRIBUTING.md) for development setup, runtime invariants,
tests, and dependency-license maintenance.

## Version contract

A release uses one version everywhere. Before creating `vX.Y.Z`, set `X.Y.Z`
in:

- The `hel-core`, `hel-tui`, `hel-cli`, and `hel-voice-worker` package versions.
- The `hel-core` and `hel-tui` dependency constraints in
  `crates/hel-tui/Cargo.toml` and `crates/hel-cli/Cargo.toml`.
- The four workspace package entries in `Cargo.lock`.

The relevant manifests are `Cargo.toml`, `crates/hel-tui/Cargo.toml`,
`crates/hel-cli/Cargo.toml`, and `voice-worker/Cargo.toml`.

`licenses/THIRD_PARTY_LICENSES.html` embeds the workspace versions and must be
regenerated after the bump. `install.sh`'s `SCRIPT_VERSION` is an independent
installer logging revision; do not synchronize it to the product version.

## Release procedure

1. Choose the next version and update every location in the version contract.
   Do this before creating a tag.
2. Refresh and inspect the lockfile and generated license report:

   ```bash
   cargo metadata --no-deps --format-version 1 >/dev/null
   cargo about generate --workspace --offline --config licenses/about.toml \
     --locked --fail licenses/about.hbs -o licenses/THIRD_PARTY_LICENSES.html
   ```

3. Validate the candidate version and the repository:

   ```bash
   scripts/check-release-version.sh vX.Y.Z
   cargo fmt --check
   cargo test
   cargo clippy --all-targets -- -D warnings
   ```

   Run `cargo test` outside the restricted sandbox as required by `AGENTS.md`.
   Run any additional cross-platform, packaging, or manual checks relevant to
   the changes in the release.

4. Commit the version bump and generated files. Push that commit and wait for
   the branch CI checks to pass. Re-run the version check on the clean commit
   that will be tagged.
5. Create and push an annotated tag on that exact commit:

   ```bash
   git tag -a vX.Y.Z -m "Release X.Y.Z"
   git push origin vX.Y.Z
   ```

6. Watch the `Release Hel` workflow. It checks the tag against all workspace
   package versions and internal dependency constraints before any build. It
   then builds and packages x86-64 Linux musl, ARM64 Linux musl, and ARM64 macOS
   archives, publishes the GitHub Release and checksums, and dispatches the
   crates.io workflow.
7. Approve the `crates-io` environment when requested. `publish.yml` verifies
   and packages the tagged source before publishing `hel-core`, `hel-tui`,
   `hel-cli`, and `hel-voice-worker` in dependency order. An already-published
   crate version is skipped, so rerunning the workflow safely resumes a partial
   publication.
8. Verify the GitHub Release assets and confirm all four crates show `X.Y.Z` on
   crates.io. Test the documented installer against the new tag.

To package an existing tag without publishing crates, run `publish.yml`
manually with `publish` disabled and inspect its `.crate` artifact.

## Recovery

Do not move or overwrite a tag after its GitHub Release is public. Release
assets and cloned tags may already be cached even when download counts are low.
If a bad tag was published, document the bad release, make the correction on
the branch, and release the next patch version. A crates.io version is reserved
permanently once published; yanking it does not make the version reusable.
