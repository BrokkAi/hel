# Hel

> Welcome to Hel.

Hel (`hel`) is a terminal control plane for long-running ACP coding-agent
sessions. It keeps the useful ACP client and TUI shell from Mjolnir, while
removing its review, council, delegation, and subagent business logic.

The dashboard gives one view of sessions and quotas across any number of
Codex, Claude Code, and Kimi Code profiles. A session can run in a local Podman
container, an Apple `container`, a disposable EC2 instance, a named SSH
machine, or Podman reached through SSH. Hel always gives the selected harness
its unrestricted mode (`agent-full-access`, `bypassPermissions`, or `auto`).

## Status

This branch is an early Hel fork. Its state/config namespace is deliberately
separate from MJ and there is no automatic MJ migration.

## Install

Install the latest released Hel binary and its Linux worker companions with:

```console
curl -fsSL https://raw.githubusercontent.com/BrokkAi/hel/master/install.sh | sh
```

This downloads a verified release into `~/.local/bin`. Rust and a source
checkout are not required. The installer prints `hel doctor` as the first next
step and also supports `--prefix` and `--version`; run it with `--help` to see
those options.

## From source

```console
cargo build --release
./target/release/hel
./target/release/hel setup
```

For a local Podman target, build the agent-development image with:

```console
podman build --pull=always \
  --file containers/Containerfile.agent-dev \
  --tag localhost/hel/agent-dev:latest \
  containers
```

The image includes Rust, Node 24, Git, the Codex ACP bridge, and the Claude ACP
bridge. Hel copies its worker and the selected harness profile into each new
container. Kimi Code uses Hel's official on-demand installer fallback.

Configure it as a target with:

```toml
[targets.podman]
kind = "local-podman"
image = "localhost/hel/agent-dev:latest"
```

See [docs/PODMAN.md](docs/PODMAN.md) for the rootless installation,
verification, and remediation contract Hel enforces before local-Podman
provisioning.

To delegate host setup, run `hel setup instructions --platform linux` (or
`macos`) and `hel doctor --json`; give the instructions page plus the output of
`hel doctor --json` to your coding agent.

`hel` opens the session/quota dashboard. On a first run with no configuration,
it drops into the same plain-stdio setup dialog as `hel setup`. The dialog finds
local Codex, Claude Code, and Kimi Code homes, reports whether their credentials
appear present, detects the current GitHub origin, recommends a usable local
container runtime, writes the configuration after confirmation, and smoke-tests
the selected image. `Ctrl+Q` or Esc detaches; it does not stop the target-side
worker. `hel server` explicitly starts the authenticated phone controller. It
binds only to loopback unless direct TLS is configured:

```console
hel server
hel server --bind 0.0.0.0:3765 --tls-cert ./hel.crt --tls-key ./hel.key
```

## Configuration

Hel reads `~/.config/hel/config.toml` on Linux (the platform-equivalent config
directory elsewhere) and writes controller state beneath the platform data
directory's `hel/` folder.

```toml
version = 1

[profiles.codex-1]
kind = "codex"
home = "/home/me/.codex-1"

[profiles.codex-2]
kind = "codex"
home = "/home/me/.codex-2"
# Optional conservative byte budget used only for cross-harness compaction.
context_window_bytes = 262144

[profiles.claude-1]
kind = "claude"
home = "/home/me/.claude-1"

[profiles.kimi-1]
kind = "kimi"
home = "/home/me/.kimi"

[bundles.hel]
primary_repo = "hel"

[[bundles.hel.repositories]]
id = "hel"
github = "your-org/hel"
destination = "hel"

[[bundles.hel.repositories]]
id = "shared"
github = "your-org/shared"
destination = "shared"

# Declare exactly one of `github` or `local` for each repository.
[[bundles.hel.repositories]]
id = "private-local"
local = "/home/me/src/private-local"
destination = "private-local"

[targets.podman]
kind = "local-podman"
image = "ghcr.io/your-org/agent-dev:latest"
# Optional template defaults used by non-interactive callers. The dashboard's
# target step can override these for one launch without changing the image.
cpus = "8"
memory = "32g"

[targets.mac-container]
kind = "apple-container"
image = "ghcr.io/your-org/agent-dev:latest"

[targets.builder]
kind = "ssh-bare"
host = "builder"
workspace_prefix = ".local/share/hel/workspaces"

[targets.builder-podman]
kind = "ssh-podman"
host = "builder"
image = "ghcr.io/your-org/agent-dev:latest"

[targets.ec2]
kind = "aws-ec2"
region = "us-east-1"
launch_template = "hel-agent"
ssh_user = "ubuntu"
address_source = "public-dns"
```

For the rotating RunsOn Ubuntu 24 image, use the included updater instead of
pinning an upstream AMI ID. It copies the newest RunsOn image into your AWS
account, makes that copy the default version of `hel-runson`, refreshes the
controller's SSH ingress, and can add the matching target on its first run:

```bash
scripts/update-runson-launch-template.sh --write-hel-config
```

Later runs reuse an existing owned copy when the upstream image is unchanged.
EC2 launches from the account-owned AMI copy; S3 is useful for image
export/import and runtime artifacts, but is not a boot source for an EC2 launch
template.

Repository values accept `owner/repo`, an HTTPS URL, or an SSH GitHub URL.
Each target starts from a full clone. Container and EC2 resources are unnamed
templates: closing a session first writes and verifies a `.hel.zip` checkpoint,
then deletes that exact resource. Named SSH machines persist, but Hel removes
the exact per-session workspace after the same verified checkpoint.

A repository configured with `local` is bootstrapped from the controller
checkout, including its full commit history, index, worktree changes, and
nonignored untracked files. Hel then configures a real `origin` in the worker
using a background, per-session Git protocol bridge over the existing target
transport. `git fetch`, `git pull`, and normal fast-forward `git push origin`
therefore operate directly on the configured controller checkout; no inbound
port or writable host mount is exposed. Pushes to the checked-out controller
branch use Git's `updateInstead` safety checks and are rejected while that
checkout is dirty. Force pushes, ref deletion, and receive hooks are disabled.
The controller checkout must remain available while the session is live.

Local repositories that use Git LFS are not currently supported by this
bridge. Use a normal network remote for those repositories.

Profiles point at controller-side homes. Hel copies only a harness-specific
allowlist into a fresh target-side home and sets `CODEX_HOME`,
`CLAUDE_CONFIG_DIR`, or `KIMI_CODE_HOME`. Images may bake compatible harnesses
and ACP bridges in; otherwise Hel uses its pinned fallback bootstrap where one
is available. Hel probes the target architecture and uses the controller
binary only for a matching Linux host. macOS and cross-architecture installs
ship a `hel-worker-<arch>-unknown-linux-musl` companion beside `hel`;
development builds can set `HEL_WORKER_DIR` or `HEL_WORKER_BINARY`. A release
service may instead set an `HEL_WORKER_URL` containing `{target}` plus its
required `HEL_WORKER_SHA256`; Hel verifies the download before caching or
executing it.

Ephemeral workers inherit a conservative whitelist from the controller's
effective global Git configuration: `user.name`, `user.email`,
`init.defaultBranch`, `pull.ff`, `pull.rebase`, `push.default`,
`push.autoSetupRemote`, `fetch.prune`, `fetch.pruneTags`, `rebase.autoStash`,
`rerere.enabled`, `rerere.autoUpdate`, `merge.conflictStyle`, and
`diff.algorithm`. Hel applies these settings after cloning, so they do not
provide repository credentials, and repository-local configuration still
wins. Named `ssh-bare` machines keep their own Git configuration. Hel does not
copy Git aliases, credentials, URL rewrites, includes, LFS filters, signing
settings, hooks, editor or pager commands, custom drivers, proxies,
`safe.directory`, line-ending settings, or the controller's global excludes
file.

The harness homes are whitelists rather than complete dot-directory copies.
For Claude, Hel copies `.credentials.json`, `settings.json`, `CLAUDE.md`,
`skills/`, and `plugins/`; it does not copy transcripts, project history, or
caches. SSH and GPG keys, GitHub CLI and package-registry credentials, cloud
configuration, shell dotfiles, editor configuration, and toolchain state are
not transferred automatically.

The dashboard's Deployment Capacity pane groups configured local and SSH
targets by host and shows current CPU and RAM utilization. Multiple target
definitions may share a host; their IDs appear together in that host's row.
SSH aliases remain separate because Hel groups by the exact configured host
string. AWS EC2 targets remain one row per template and show the total logical
cores, RAM, and workspace-filesystem capacity of currently allocated
instances, or `on demand` when none are running. Capacity refreshes every 30
seconds; a failed refresh leaves the previous sample visible as stale.

The new-session bundle picker can save a reusable one-repository bundle from an
existing local Git checkout or a GitHub `owner/repository`/URL. Multi-repository
bundles and explicit Git refs remain TOML-first.

New and resumed sessions expose resource sizing in the target picker. The
baseline is 8 CPUs and 32 GiB; `+` doubles, `-` halves the current allocation,
`c` doubles only CPU, `m` doubles only memory, and `r` returns to the baseline.
For EC2, the independent controls select a discovered instance type only when
the other dimension remains unchanged. Container targets cap each dimension
independently at the host's total logical cores and physical RAM. Current load
is intentionally not subtracted, so overcommit remains available. Container
sizing uses runtime flags with the same image. EC2 sizing discovers the launch
template's instance family and passes an instance-type override while retaining
the same launch template and AMI.

New and resumed sessions end with a review screen where attached directories
can optionally be added, edited, or removed before launch. Container
targets expose them with the runtime's isolated mount mode. EC2 targets stream
each directory as tar.gz through one SSH process directly into the installed
Hel worker, without writing a controller or target archive. Symbolic links are
rejected so an attachment cannot escape its source or destination tree.

The first-run dialog intentionally creates one local target and, when started
inside a GitHub checkout, one-repository bundle. Advanced configurations remain
TOML-first: edit `config.toml` directly to add profiles, multi-repository virtual
monorepos, SSH targets, or AWS targets.

## Session archives

Checkpoint archives are versioned ZIPs containing a manifest, a canonical
event stream, allowlisted native harness artifacts, and Git state for every
repository (committed bundle, staged and unstaged patches, and untracked
files). Every payload is SHA-256 verified after the archive is atomically
installed. Normal Pause refuses teardown if that verification fails; explicit
force-destroy is the data-loss escape hatch.

Hel is licensed under `GPL-3.0-only`.
