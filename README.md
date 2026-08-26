# Hel

Hel is a terminal control plane for coding agents. It runs many long-lived
agent sessions — Codex, Claude Code, Kimi Code, Grok Build, and DeepSeek Harness — in disposable
isolated environments, keeps them working while you are away, and gives you one
dashboard for their sessions, quotas, and credentials. Agents connect through
the [Agent Client Protocol](https://agentclientprotocol.com) (ACP).

## Why Hel

Running one coding agent in one terminal works. Running six of them across two
Codex accounts and a Claude account, on three machines, overnight, does not.
Hel exists for the second case.

- **Sessions survive everything.** Prompts queue durably on the target and keep
  executing in order while your terminal is closed or your laptop is off. Every
  session records a hash-chained event journal. Recovery archives are verified
  end to end before Hel tears anything down, and crashed or wedged workers are
  detected and restarted automatically.
- **Full-access mode without fear.** Isolated targets run the agent in its
  unrestricted mode — no permission prompts — because the blast radius is a
  disposable container or instance, not your machine.
- **Your credentials stay canonical.** Each profile keeps one credential set on
  your machine. Hel copies a minimal allowlist into each target, reconciles
  rotating OAuth tokens across every live session within about a minute, and
  structurally excludes credentials from event streams and recovery archives.
- **One view of capacity.** Sessions, per-profile quota and usage, and host
  capacity in one dashboard — and on your phone through `hel server`.
- **Agents can operate it.** `hel doctor --json` and `hel setup instructions`
  are designed so your coding agent can converge a host to session-ready by
  looping on machine-readable checks.

## Goals

1. Run many concurrent, long-lived agent sessions and make them durable:
   detached execution, verified recovery archives, resume onto a fresh target.
2. Make unrestricted agent modes safe by pairing them with disposable,
   isolated environments.
3. Keep provisioning minimal and deterministic: per-harness allowlists,
   SHA-256-verified workers and archives, no snowflake state in targets.
4. Give one control plane across harnesses and profiles: sessions, quotas,
   credentials, and remote control in one place.
5. Fail loudly. A failed checkpoint leaves the session usable and says so;
   retired formats are rejected, never half-converted.
6. Stay operable by both humans (TUI, web) and coding agents (JSON output,
   scriptable CLI).

## Non-goals

- **Hel is not an agent.** It does not write code, plan, or pick models. It
  manages harnesses that do.
- **No privileged host setup.** Hel will not install Podman, edit
  `subuid`/`subgid`, create AWS launch templates or security groups, or make
  SSH hosts reachable. You (or your agent, with your credentials) do that;
  `hel doctor` verifies it and prescribes the exact remediation.
- **No wholesale environment transfer.** SSH and GPG keys, shell dotfiles,
  editor configuration, package-registry credentials, cloud configuration, and
  toolchain state are never copied into targets.
- **Not a team server.** One controller process owns a session store, enforced
  by an OS-backed lock. The web server is a personal remote control with one
  viewer credential, not a multi-user service.
- **Not an orchestration platform.** Containers are unnamed disposable
  templates, rebuilt from checkpoints rather than upgraded in place. There is
  no scheduler and no load-based admission; overcommit is your call.
- **No compatibility shims.** Old relay protocols and archive schemas are
  rejected with a clear error instead of being partially converted.

## Harnesses and targets

| Harness | Credentials & quota | Checkpoint/restore of native state |
|---|---|---|
| Codex | yes | yes |
| Claude Code | yes | yes |
| Kimi Code | yes | yes |
| Grok Build | yes | yes |
| DeepSeek Harness | credentials yes; usage-priced, no subscription quota | yes |

The set is extensible by design: these five are reference integrations, not a
closed list. A new ACP-speaking harness needs a launch recipe or bridge, its
credential file shapes and login command, its home environment variable, a
checkpoint allowlist for native session state, and optionally a quota reader.
Issues and pull requests for new harnesses are welcome.

| Target | Kind | Where it runs | Agent mode |
|---|---|---|---|
| Local Git worktree | `local-bare` | your machine | your configured approvals |
| Podman container | `local-podman` | Linux, WSL2 | unrestricted |
| Apple container | `apple-container` | macOS 26+, Apple silicon | unrestricted |
| SSH machine | `ssh-bare` | a Linux host you name | unrestricted |
| Podman over SSH | `ssh-podman` | a Linux host you name | unrestricted |
| EC2 instance | `aws-ec2` | your AWS account | unrestricted |

The controller (the `hel` binary you run) supports Linux and macOS. Windows is
not supported; use WSL2.

## Install

```console
curl -fsSL https://raw.githubusercontent.com/BrokkAi/hel/master/install.sh | sh
```

This downloads a verified release into `~/.local/bin` — no Rust toolchain
needed. Run `hel doctor` next. The installer also supports `--prefix` and
`--version`; see `--help`.

Building from source works too:

```console
cargo build --release
./target/release/hel
```

For container targets, pull the published multi-arch agent image (public, no
authentication):

```console
podman pull ghcr.io/brokkai/hel/agent-dev:latest
```

It includes Rust, cargo-nextest, Node, OpenJDK 25, Git, GitHub CLI, the Codex
and Claude ACP bridges, and pinned DeepSeek Harness plus `dsh-acp-server`
packages.
See [docs/src/content/docs/custom-images.md](docs/src/content/docs/custom-images.md)
to build your own.

## Quickstart

1. Run `hel`. The first run opens a plain-terminal setup dialog: it finds your
   local harness homes, checks that credentials look present, detects the
   current GitHub repository, recommends a container runtime, and writes
   `config.toml` after you confirm.
2. Run `hel doctor` (or `hel doctor --json`) and fix what it reports, until it
   is clean. Log in to any profile that needs it with
   `hel login --profile <id>`.
3. In the dashboard, create a session: pick a profile, a repository bundle,
   and a target, then send your first prompt.
4. Detach whenever you like (`Ctrl+Q`). The session keeps running and your
   queued prompts keep executing. Reattach from the dashboard, or run
   `hel server` and drive it from your phone.

In an attached TUI or the phone viewer, start a message with `!` to run the
rest as `bash -lc` inside that session's target. Shell commands run in the
session workspace without blocking an active agent turn. Their bounded live
output is saved in the transcript and included once as hidden context on the
next prompt submitted after the command finishes. Press Escape in the TUI, or
use the shell's Cancel button in the viewer, to stop it.

Configuration lives at `~/.config/hel/config.toml` (the platform-equivalent
directory elsewhere). The first-run dialog writes a working single-target
setup; everything beyond that is edited in TOML. A minimal example:

```toml
version = 1

[profiles.codex-1]
kind = "codex"
home = "/home/me/.codex"

[profiles.claude-1]
kind = "claude"
home = "/home/me/.claude"

[bundles.myapp]
primary_repo = "myapp"

[[bundles.myapp.repositories]]
id = "myapp"
github = "your-org/myapp"        # or: local = "/home/me/src/myapp"
destination = "myapp"

[targets.podman]
kind = "local-podman"
image = "ghcr.io/brokkai/hel/agent-dev:latest"
# Optional: auto (default), always, newer, missing, or never. Auto refreshes
# remote latest tags, keeps versioned tags cached, and pins digest references.
# pull_policy = "auto"
```

Profiles point at harness home directories on your machine — run as many
profiles per harness as you have accounts. Bundles describe the repositories a
session checks out (multi-repository bundles give agents a virtual monorepo).
Target prerequisites and full option lists are covered in
[docs/PODMAN.md](docs/PODMAN.md), [docs/SSH.md](docs/SSH.md), and
[docs/AWS.md](docs/AWS.md).

## Security and isolation model

- Execution policy is selected by target, then translated into each harness's
  own controls. Isolated and remote targets run unconstrained. On a local
  worktree (`local-bare`), Hel preserves the profile and harness's configured
  approval behavior. Codex, Claude Code, and Grok Build expose guardian modes;
  Kimi Code and DeepSeek Harness do not, so Hel shows a prominent warning not
  to use them on a raw, unsandboxed target.
- Harness homes are copied by allowlist, not wholesale. For Claude Code, for
  example: credentials, settings, `CLAUDE.md`, `skills/`, and `plugins/` — no
  transcripts, history, or caches. Hel sets `CODEX_HOME`, `CLAUDE_CONFIG_DIR`,
  `KIMI_CODE_HOME`, or `GROK_HOME` in the target. Skill edits on your machine
  propagate to live sessions within about a minute.
- Credentials travel only between the controller and a session's worker. They
  are never written to the event journal or recovery archives. When the
  controller's `gh` is authenticated, Hel continuously pushes its active
  GitHub token to every live non-local session, including raw SSH targets.
  The token is not stored in archives.
- A repository configured with `local` is served to workers through a
  per-session Git protocol bridge over the session's own transport: `git
  fetch` and fast-forward `git push origin` operate on your checkout with no
  inbound port and no writable mount. Force pushes, ref deletion, and receive
  hooks are disabled; pushes to a dirty checked-out branch are rejected. Git
  LFS is not supported through the bridge.
- Attached directories reject symbolic links, so an attachment cannot escape
  its source or destination tree.
- `hel server` binds only to loopback unless you configure TLS directly. The
  phone viewer authenticates with a code shown at server start, exchanged for
  a signed session cookie.

## Durability

Hel saves a recovery copy automatically after completed turns when the session
is idle (at most every ten minutes), and `hel checkpoint --session <id>`
forces one. Recovery archives are verified end to end; a normal Stop writes
and verifies the archive before any teardown, and refuses teardown if
verification fails. Explicit force-destroy is the data-loss escape hatch.

A stopped session resumes by provisioning a fresh target from its archive,
with its pending prompt queue intact (resume asks whether to keep or discard
it). A session recorded under one harness can be resumed under another; Hel
condenses the transcript into a size-bounded handoff for the new harness.

If Hel or its host crashes, workers and their queued prompts keep running.
`hel recover scan` finds managed containers and instances that are no longer
tracked; `hel recover adopt` reconnects one as a tracked session.

## License

Hel is licensed under `GPL-3.0-only`.
