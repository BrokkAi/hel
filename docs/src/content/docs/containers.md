---
title: Container targets
description: Set up a disposable container target for hel and start your first isolated session.
---

## What container targets give you

Each session on a container target runs in its own disposable, labeled
container: local Podman on Linux or WSL2, or Apple's `container` runtime on
macOS 26 or newer on Apple silicon. On these isolated targets, hel runs the
selected harness in its unrestricted mode (`agent-full-access`,
`bypassPermissions`, `auto`, Grok Build's `--always-approve` launch flag, or
DeepSeek Harness's `danger-full-access` permission mode),
instead of the localhost approval flow. Every one of those approves every
call. Note that Kimi Code's mode is named `auto` but is not a review policy
that approves only low-risk calls.

Closing a session first writes and verifies a recovery archive, then removes
that exact container. Nothing about the container persists past the session
except what the recovery archive captured and whatever you pushed to a
remote.

## Prerequisites

Pick one runtime:

- **Rootless Podman 4.0 or newer** on Linux or WSL2. See
  [Podman for Hel](/podman/) for installation and verification steps.
- **Apple's `container` CLI** on macOS 26 or newer on Apple silicon.

If you installed hel with `install.sh`, it already placed the matching
`hel-worker-<arch>-unknown-linux-musl` companion binary next to `hel`. hel
uses that companion to run its session relay inside Linux containers when the
controller binary itself isn't a Linux binary for the container's
architecture.

## Get the agent-dev image

hel ships a reference container image with everything a session needs
pre-installed: Rust, cargo-nextest, Node 24, OpenJDK 25, Git, GitHub CLI, the
Codex and Claude ACP bridges, and pinned DeepSeek Harness plus
`dsh-acp-server`. It's published at
`ghcr.io/brokkai/hel/agent-dev:latest`, public and
multi-arch for both `linux/amd64` and `linux/arm64`, so the same image name
works whether hel is running it through Podman, Apple's `container` runtime,
or an arm64 SSH host.

You don't need to do anything to get it: `hel setup`'s image prompt already
defaults to this published image, and `podman run` (and Apple's `container
run`) pull an image automatically the first time it's needed. Accepting the
default when you run `hel setup`, below, is enough.

Building it yourself remains a supported alternative, for example to
customize the image or to work offline:

```console
podman build --pull=always \
  --file containers/Containerfile.agent-dev \
  --tag localhost/hel/agent-dev:latest \
  containers
```

## Run `hel setup`

```console
hel setup
```

Setup reports the Codex, Claude Code, Kimi Code, Grok Build, and DeepSeek
Harness homes it found, the
GitHub origin of the current directory, and which local container runtimes
are usable. If a usable runtime exists, it prompts you for:

1. Which runtime to use, defaulting to its recommendation.
2. The container image, defaulting to `ghcr.io/brokkai/hel/agent-dev:latest`
   — press Enter to accept it, or enter `localhost/hel/agent-dev:latest` here
   if you built the image yourself above.

A plain image such as `ubuntu:24.04` still works if you enter it here: hel
auto-installs Git, GitHub CLI, and Node the first time a session needs them.
But that installation runs inside every new container, which slows down the
start of each session. The default agent-dev image avoids that cost.

It then shows a summary of what it's about to write and asks you to confirm
before writing `config.toml`. After you confirm, it runs a smoke test: it
creates a disposable container from the configured image, runs a trivial
command in it, and removes it, to prove the runtime actually works before you
start a real session.

## Verify with `hel doctor`

```console
hel doctor --json
```

This prints a machine-readable array of prerequisite checks. Resolve every
check reported as `fixable` — each one includes what's wrong and how to fix
it — then run `hel doctor --json` again. Repeat until none remain. The set of
checks hel runs is still growing, so treat the `fixable` status as
authoritative rather than checking for specific check names.

Once every check passes, run the same command with `--smoke` for an
end-to-end test: it creates and removes a disposable container the same way
`hel setup` does, confirming the full path works, not just static
prerequisites.

```console
hel doctor --json --smoke
```

## First session

```console
hel
```

This opens the dashboard. Press **Ctrl+N** to start the new-session wizard.
It walks you through picking a profile, a target, and a bundle.

Before launch, you can size the container's CPU and memory allocation. The
baseline is 8 CPUs and 32 GiB:

| Key | Effect |
| --- | --- |
| `+` | Doubles the current allocation |
| `-` | Halves the current allocation |
| `c` | Doubles CPU only |
| `m` | Doubles memory only |
| `r` | Resets to the 8-CPU/32-GiB baseline |

The wizard ends on a review screen where you can add, edit, or remove
attached directories before launch. On container targets, each attached
directory is mounted using the runtime's isolated mount mode, so a container
can't write back into your host filesystem through it.

Each attached directory also has a read-only checkbox. Podman's isolated mode
is a copy-on-write overlay, which some filesystems cannot host: when hel finds
a source on NFS, SMB, FUSE, a FAT-family filesystem, or another overlay, it
attaches that directory read-only instead and says so while the session
launches.

## Two useful facts

If the `gh` CLI on the machine running hel is authenticated, hel passes its
active GitHub token into every freshly provisioned container as `GH_TOKEN`.
That's what lets `gh` and HTTPS Git pushes work inside the container without
copying any SSH keys. The token never goes into a recovery archive.

If hel or the host crashes, containers it was managing can be orphaned —
still running, but no longer tracked in hel's state. Use `hel recover` to
find and reclaim them:

```console
hel recover scan --json
hel recover adopt --session <session-id> --target <target-id>
hel recover destroy --session <session-id> --target <target-id> --confirm <session-id>
```

`scan` lists managed containers that exist but aren't in hel's state. `adopt`
reconnects one back into hel as a tracked session; add `--profile` and
`--bundle` when the orphan predates hel's ownership markers and can't be
matched to a profile and bundle automatically. `destroy` removes one without
adopting it first; `--confirm` must repeat the session ID exactly, as a
safeguard against destroying the wrong container.
