# Add a first-class local Docker target

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds. Maintain this document in accordance with `.agents/PLANS.md`.

## Purpose / Big Picture

After this change, a Hel user can configure a `local-docker` target, launch an unrestricted coding-agent session in Docker, attach host directories without letting container writes modify the originals, checkpoint and close the session, and resume it onto a fresh Docker container. The target must participate in the same dashboard, capacity, recovery, credential synchronization, Git cache, setup, and doctor workflows as `local-podman`.

The observable entry point is a target such as:

    [targets.docker]
    kind = "local-docker"
    image = "ghcr.io/brokkai/hel/agent-dev:latest"

Running `hel doctor --json --smoke` must verify Docker and the image, and creating a session on this target must produce Docker commands rather than Podman commands. A writable attached directory must be presented through a Docker local volume backed by Linux OverlayFS, using the host directory as the read-only lower layer and session-owned upper/work directories for writes.

## Progress

- [x] (2026-09-01 08:30Z) Inspected the existing Podman configuration, runtime plans, persistence, recovery, worker installation, setup, doctor, UI, and attachment paths.
- [x] (2026-09-01 08:30Z) Confirmed from Docker's official CLI documentation that the built-in Linux `local` volume driver forwards `type`, `device`, and `o` options to the host mount operation.
- [x] (2026-09-01 09:10Z) Added public and low-level `local-docker` configuration/state variants and routed controller target selection through them.
- [x] (2026-09-01 09:10Z) Added Docker provisioning, OverlayFS volume setup, worker, recovery, resource-probe, and ordered cleanup command generation.
- [x] (2026-09-01 10:05Z) Added the schema-20 migration and database round trip, plus setup, doctor, server, CLI, TUI, import, README, and rendered documentation integration.
- [x] (2026-09-01 10:20Z) Added focused behavior tests for Docker preflight, pull policy, OverlayFS provisioning, ownership-safe cleanup, recovery, persistence, setup, and doctor.
- [x] (2026-09-01 10:45Z) Passed formatting, `git diff --check`, the full host-target test suite, clippy with warnings denied, and the documentation build; also completed a live Docker OverlayFS isolation smoke test.
- [x] (2026-09-01 10:55Z) Committed the implementation as `5fc7268`, pushed `local-docker-target`, and opened https://github.com/BrokkAi/hel/pull/18 against `master`.
- [x] (2026-09-01 11:30Z) Corrected setup's smoke-test call path to use the shared Docker-aware runner, added a setup-level OverlayFS regression test, and passed the focused setup suite, full host-target suite, formatting, diff checks, and clippy.
- [x] (2026-09-01 12:20Z) Merged current `master` at `678566d`, added behavior coverage for Docker greeting classification, checkpoint upload, clone-cache discovery, and reviewer staging, and passed the repository's LLVM coverage gate locally.

## Surprises & Discoveries

- Observation: Hel already funnels Podman and Apple Container launch arguments through `container_run_args` in `src/hel_targets.rs`, while many worker operations still select a literal engine at exhaustive `TargetLocator` matches.
  Evidence: `src/hel_targets.rs` accepts an `engine` argument for run/exec construction, whereas `src/hel_controller/worker_binary.rs` explicitly maps `LocalPodman` to `podman` and `AppleContainer` to `container`.

- Observation: Docker can create the required OverlayFS mount through its built-in local volume driver; Hel does not need to invoke privileged `mount` itself.
  Evidence: Docker documents that local-driver `volume-opt` values are forwarded to the Linux mount syscall as `mount -t <type> <device> <destination> -o <options>`.

- Observation: This checkout defaults Cargo to the `x86_64-unknown-linux-musl` target, but the environment does not contain `x86_64-linux-musl-gcc`.
  Evidence: `cargo check --all-targets` failed at the linker lookup; `cargo check --all-targets --target x86_64-unknown-linux-gnu` completed successfully. Validation in this environment must name the host GNU target.

- Observation: Docker volume label filters are specifically conjunctive, unlike the semantics some other Docker list filters use.
  Evidence: Docker's `docker volume ls` CLI reference states that multiple label filters perform an “and” search. Cleanup can therefore select only volumes carrying both `dev.hel.managed=true` and the exact session label.

- Observation: A live Docker 29.7.2 Linux daemon was available, although its cache initially contained no suitable image.
  Evidence: After pulling `alpine:3.22`, a disposable volume using the same OverlayFS local-driver options allowed changes through the container mount while leaving the lower host directory unchanged. The test removed its container, volume, temporary directory, and pulled image afterward.

- Observation: The first implementation made doctor call `run_setup_smoke_test`, but setup retained a duplicate generic executor around `setup_smoke_plan`.
  Evidence: `src/hel_setup.rs::run_smoke_test` constructed and executed the plain run/exec/remove plan directly, so its `LocalDocker` target never reached the shared runner's Docker-specific OverlayFS branch. Routing setup through the shared runner removes the divergent call path.

- Observation: Passing the ordinary Rust suite did not imply passing the repository's per-module coverage baseline.
  Evidence: CI reported aggregate coverage above its regression floor but four touched modules just below their individual requirements. Behavior tests for their Docker paths raised `crates/hel-cli/src/main.rs` to 53.50%, checkpoint to 70.11%, Git cache to 73.44%, and reviewer to 60.63%; `scripts/check-coverage.mjs` then passed without baseline changes.

## Decision Log

- Decision: Implement only `local-docker` in this change; do not add `ssh-docker`, Compose, or Docker Engine API integration.
  Rationale: The requested target is local Docker. Hel's existing command-plan and supervised subprocess architecture can drive the Docker CLI directly.
  Date/Author: 2026-09-01 / Codex

- Decision: Preserve writable attachment isolation with deterministic, session-owned Docker local volumes backed by OverlayFS.
  Rationale: Ordinary writable bind mounts would modify the user's source directory and would not preserve the Hel behavior already advertised for Podman attachments. Docker's local volume driver provides the required mount without a new dependency.
  Date/Author: 2026-09-01 / Codex

- Decision: Keep the public and durable kind additive as `local-docker`, and extend the existing shared command helpers instead of introducing a new engine abstraction.
  Rationale: Existing `local-podman` configuration and database records must retain their meaning. An additive kind makes behavior explicit and lets the Rust compiler identify every target match that needs parity handling, while the existing helpers already provide the useful sharing boundary.
  Date/Author: 2026-09-01 / Codex

- Decision: Map Hel's `newer` refresh policy to Docker's `--pull=always`; keep `missing`, `never`, and digest-pinned automatic selection cache-aware.
  Rationale: Docker has no separate `newer` run mode. `--pull=always` performs the registry manifest/digest check and reuses unchanged layers, which implements Hel's intended refresh behavior without an unconditional image replacement.
  Date/Author: 2026-09-01 / Codex

- Decision: Make the Docker smoke check exercise a real OverlayFS-backed local volume, not merely a disposable plain container.
  Rationale: Docker Desktop, remote contexts, and non-Linux daemons can make host-path assumptions invalid. Testing the actual feature path gives setup and doctor an authoritative compatibility result.
  Date/Author: 2026-09-01 / Codex

- Decision: Keep smoke execution in `run_setup_smoke_test` as the single behavior path for setup and doctor.
  Rationale: Docker needs a materially different smoke plan from other runtimes. Having setup execute `setup_smoke_plan` itself bypassed that dispatch and allowed the two product surfaces to disagree about target readiness.
  Date/Author: 2026-09-01 / Codex

- Decision: Restore coverage with behavior tests, not by lowering module baselines or adding implementation-list assertions.
  Rationale: The uncovered Docker branches have observable command and classification contracts. Exercising those contracts both satisfies the gate and protects the feature from regressions.
  Date/Author: 2026-09-01 / Codex

## Outcomes & Retrospective

The implementation is complete and published for review. It delivers a first-class local Docker target across configuration, durable state, schema migration, provisioning, checkpoint/resume, recovery, metrics, cleanup, setup, doctor, CLI/TUI, and documentation. Writable attachments use owned and labeled Docker local volumes backed by OverlayFS; read-only attachments remain direct binds. Creation rollback and normal cleanup verify exact resource ownership and remove the container before volumes and backing directories.

Validation passed with 1,873 Rust tests passing and 9 ignored across the full host-target suite, `cargo clippy --all-targets --target x86_64-unknown-linux-gnu -- -D warnings`, formatting, diff checks, a documentation build covering 239 internal links, and a live disposable Docker OverlayFS isolation test. The count includes setup-level coverage of the managed OverlayFS smoke and focused behavior coverage of Docker greeting classification, checkpoint upload, clone-cache discovery, and reviewer staging. The unchanged repository coverage checker passes at 77.13% aggregate coverage with every module above its required floor. The implementation commit is `5fc7268`; the pull request is https://github.com/BrokkAi/hel/pull/18.

## Context and Orientation

`src/hel_config.rs` defines user-facing configuration. Its tagged `TargetTemplate` enum maps TOML `kind` strings to target templates. `src/hel_state.rs` defines durable session locators, which identify the exact resource owned by a running session. `src/hel_database.rs` serializes those locators to SQLite, and `src/hel_database/schema.rs` contains the numbered migration ladder and the `session_targets.kind` constraint.

`src/hel_targets.rs` is the low-level runtime planner. A `CommandSpec` is one executable plus an argument vector and purpose; a `CommandPlan` is an ordered set of those commands. The controller builds a low-level target in `src/hel_controller/backend.rs`, provisions it in `src/hel_controller/provisioning.rs`, installs and controls the worker in `src/hel_controller/worker_binary.rs`, and closes it through `src/hel_controller/lifecycle.rs`. Recovery and adoption inspect externally surviving resources in `src/hel_controller/recovery_scan.rs`.

An additional mount is a host directory attached to a session. Podman currently uses its `:O` volume option to present the directory as an OverlayFS lower layer: reads see the host directory, while writes go to a disposable upper layer. Docker will receive the same behavior through a named local volume. For each writable attachment Hel will create deterministic session-scoped `upper` and `work` directories on the Docker host, create a labeled Docker local volume with `type=overlay`, `device=overlay`, and `o=lowerdir=...,upperdir=...,workdir=...`, and attach that volume at the requested destination. Read-only attachments and the Git clone cache remain ordinary read-only bind mounts.

`src/hel_setup.rs` discovers usable local runtimes and writes initial configuration. `src/hel_doctor.rs` validates configured runtimes and images. The TUI modules under `crates/hel-tui/src` render target labels and the new-session attachment interface. `README.md` and the rendered documentation under `docs/src/content/docs` describe supported targets.

## Milestones

The first milestone establishes the target as a real public and durable type. It is complete when `kind = "local-docker"` parses through `src/hel_config.rs`, a running session can store `TargetLocator::LocalDocker` through `src/hel_database.rs`, and exhaustive controller matches select Docker without changing existing Podman behavior. `cargo check --all-targets --target x86_64-unknown-linux-gnu` is the incremental proof for this milestone.

The second milestone implements observable runtime behavior. It is complete when the first provisioning command creates labeled OverlayFS local volumes for writable attachments, starts a labeled Docker container, and all subsequent clone, worker, checkpoint, resource, recovery, and close operations use Docker. Focused behavior tests in `src/hel_targets/tests.rs` and the relevant controller modules must prove the exact command contracts and cleanup order.

The third milestone integrates product surfaces. It is complete when setup can select Docker, doctor diagnoses its daemon and images, the database migration retains old locators and accepts Docker locators, recovery scanning understands Docker JSON-lines, and the CLI/TUI labels and documentation expose the target. Focused setup, doctor, database, recovery, config, and UI tests prove these changes.

The final milestone validates and publishes the feature. Formatting, the full host-target test suite, and clippy with warnings denied must pass; then the implementation is committed, pushed, and proposed in a pull request whose URL is recorded here.

## Plan of Work

First, add `LocalDocker` variants to the configuration template, durable locator, low-level runtime template, and low-level runtime locator. Route `LocalPodman` and `LocalDocker` through the same controller paths, extending the existing shared command helpers where their behavior aligns and selecting the literal engine where the CLI contracts differ.

Second, extend Docker provisioning. Before `docker run`, create a deterministic labeled local volume for every writable attachment and deterministic upper/work directories under Hel's host cache. Pass each `--opt` value as a distinct argument to `docker volume create` inside one supervised shell command. Attach the volume with `--volume <name>:<destination>` and attach read-only directories with `--volume <source>:<destination>:ro`. Ensure a failed launch removes any created volumes and directories, and ensure normal close removes the owning container before its volumes and files. Reuse deterministic names after a stopped-container recovery only when labels identify the same Hel session.

Third, translate lifecycle operations. Docker uses `docker inspect` to establish existence and state, `docker start` for stopped containers, `docker exec` and `docker cp` for worker control, `docker container inspect --size` for disk reporting, and `docker rm --force` for removal. Cleanup must treat an exactly absent Docker container or volume as success without treating unrelated daemon failures as success. Recovery scans must parse Docker's JSON-lines container listing as well as Podman's JSON array.

Fourth, add durable integration. Increase the database schema version, migrate the `session_targets` constraint to accept `local-docker`, and map it to the new state locator. Update config validation, resume compatibility, checkpoint metadata, archive/import defaults where exhaustive matching requires it, server serialization, capacity grouping, TUI labels, and wizard attachment text.

Fifth, add setup and diagnosis. Setup should probe Docker with a daemon-backed `docker info` check and offer it as a local runtime. Doctor should report Docker availability, inspect configured images, run the existing disposable run/exec/remove smoke test, and exercise a disposable OverlayFS volume so a host that cannot supply writable attachments reports a concrete remediation. Docker pull policy handling must preserve Hel's configured meanings: digest-pinned and `missing` images remain cache-aware; refresh policies invoke Docker's digest-aware pull and reuse unchanged layers.

Finally, parameterize suitable existing Podman behavior tests over both engines and add focused Docker assertions for provisioning, mount creation, failed-provision cleanup, close, recovery, scanning, worker upload, persistence migration, setup, and doctor. Update human documentation, run all repository-required validation, commit only files changed for this feature, push the feature branch, and open a pull request against the repository's default branch.

## Concrete Steps

Work from `/home/ryan/code/hel/.mjolnir/worktrees/silly-cloud`.

Create or use the feature branch authorized by the pull-request request:

    git switch -c local-docker-target

After each coherent implementation checkpoint, format and run focused tests:

    cargo fmt --all -- --check
    cargo test --target x86_64-unknown-linux-gnu hel_targets
    cargo test --target x86_64-unknown-linux-gnu hel_database
    cargo test --target x86_64-unknown-linux-gnu hel_doctor
    cargo test --target x86_64-unknown-linux-gnu hel_setup

All `cargo test` commands must run outside the restricted sandbox because repository tests use loopback TCP and Unix sockets. Before submission run:

    cargo fmt --all -- --check
    cargo test --target x86_64-unknown-linux-gnu
    cargo clippy --all-targets --target x86_64-unknown-linux-gnu -- -D warnings

The expected result is exit status zero for all three commands. Then commit staged files explicitly, push the feature branch, and create a pull request:

    git push -u origin local-docker-target
    gh pr create --base master --head local-docker-target --fill

Record the resulting URL in `Outcomes & Retrospective`.

## Validation and Acceptance

Configuration parsing must accept `kind = "local-docker"` with the same image, platform, CPU, memory, environment, and pull-policy fields as `local-podman`, and serialization must round-trip it without changing the kind.

A Docker provisioning-plan behavior test with one read-only and one writable attachment must show: a labeled OverlayFS local volume for the writable source; a read-only bind for the read-only source; a detached labeled `docker run`; Docker `exec` for Git initialization and clone; and deterministic ownership labels on every managed Docker resource. The equivalent Podman test must remain unchanged and continue to use `:O`.

Lifecycle tests must demonstrate that stopping and recovering a Docker container preserves its overlay volume, while closing removes the container first and then removes the exact labeled volume and session upper/work directories. Repeating cleanup after confirmed absence must succeed. A foreign resource with the same deterministic name must not be removed or adopted.

Database tests must migrate the previous schema to the new schema without changing existing target rows, then save and reload a `LocalDocker` locator. Recovery-scan tests must accept Docker JSON-lines output and recover only containers carrying both Hel ownership and session labels.

Setup and doctor tests must show Docker as ready only when the CLI can reach the daemon. The smoke path must run a disposable container and a disposable OverlayFS volume. Existing Podman and Apple Container diagnosis must remain unchanged.

The full test and clippy commands listed above must pass. If a live Docker daemon is available to the executing user, perform a final disposable smoke launch using a temporary source directory and verify that writing through the mounted destination changes the overlay view but not the lower source. If the daemon is unavailable, the command-plan behavior tests are authoritative and the unavailable daemon must be recorded in `Surprises & Discoveries` rather than bypassed.

## Idempotence and Recovery

All Docker resources use deterministic names derived from Hel's validated session identifier and carry `dev.hel.managed=true` plus `dev.hel.session=<session>` labels. Provisioning may be retried after interruption: existing resources are reused only after exact ownership verification. Cleanup stops/removes the container before removing volume mounts and their backing upper/work directories. It never deletes a volume or directory merely because its name resembles a Hel resource.

The database migration is additive in meaning and rebuilds only the constrained `session_targets` table inside a transaction. Existing rows copy unchanged. Re-running an already completed migration is prevented by the schema version ledger.

If implementation is interrupted, consult `Progress`, inspect `git status --short`, and run the focused test corresponding to the last changed subsystem. Do not remove user resources or reset unrelated working-tree changes.

## Artifacts and Notes

The core Docker volume form is:

    docker volume create --driver local \
      --label dev.hel.managed=true \
      --label dev.hel.session=<session> \
      --opt type=overlay \
      --opt device=overlay \
      --opt o=lowerdir=<source>,upperdir=<upper>,workdir=<work> \
      <deterministic-volume-name>

Docker's local volume driver passes these fields to the Linux mount operation. `upperdir` and `workdir` must be on the same supported filesystem. The controller's existing filesystem probe and downgrade/error reporting should be generalized so unsupported lower or backing filesystems are reported before launch.

## Interfaces and Dependencies

No new Rust crate or external service is required. Continue to use `CommandSpec`, `CommandPlan`, and the shared subprocess execution helpers.

In `src/hel_targets.rs`, extend the existing shared run and lifecycle helpers with the executable name and capability decisions required by Docker pull, mount, existence, and removal planning. Public runtime templates and locators must include `LocalDocker` variants so exhaustive matching remains type-safe.

In `src/hel_config.rs`, add `TargetTemplate::LocalDocker { container: ContainerTemplate }`, serialized as `kind = "local-docker"`. In `src/hel_state.rs`, add `TargetLocator::LocalDocker { container_id: String }`, serialized as `kind = "local-docker"`.

In `src/hel_targets.rs`, expose deterministic helpers for Docker overlay volume identity and backing paths if controller cleanup needs them. Helpers must validate the session identifier and use `Path`/`PathBuf` for host paths until command rendering.

Plan revision note (2026-09-01): Initial plan created after repository inspection and confirmation of Docker local-driver mount behavior. It resolves the scope to local Docker with OverlayFS-backed writable attachments and no SSH Docker. Updated after the first compiling implementation to record the host-toolchain constraint, completed runtime work, concrete mount syntax, and independently verifiable milestones. Updated again after validation to reflect the implemented helper structure, Docker pull-policy mapping, real smoke-test contract, and final evidence. Updated after specialist review to record and close the divergent setup smoke-test call path. Updated after CI coverage review to record the current-master merge, added behavior tests, and passing per-module coverage evidence.
