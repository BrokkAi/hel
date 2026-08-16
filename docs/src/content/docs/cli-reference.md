---
title: CLI and keyboard reference
description: Common commands, options, slash commands, and terminal controls.
---

## Common CLI options

| Option | Purpose |
| --- | --- |
| `--cwd PATH` | Primary workspace; defaults to the current directory |
| `--additional-directory PATH` | Add an absolute workspace root; repeatable; alias `--add-dir` |
| `-p, --print [PROMPT]` | Run one headless prompt; omit the value or pass `-` for stdin |
| `--output-format text\|json\|stream-json` | Select headless output |
| `--permission-mode manual\|auto\|yolo` | Set headless permission behavior |
| `--model MODEL[+EFFORT]` | Override the primary agent's model for one headless invocation |
| `--review-model MODEL[+EFFORT]` | Override the review supervisor model for one headless invocation |
| `--subagent-model MODEL[+EFFORT]\|disabled` | Override or disable the default subagent model for one headless invocation |
| `-w, --worktree [NAME]` | Create or reuse a linked worktree |
| `--fullscreen-tui` | Use the alternate-screen UI instead of inline mode |
| `--debug-file PATH` | Write Mjolnir diagnostics without corrupting the TUI |
| `--agent-stderr PATH` | Capture ACP adapter stderr |
| `--no-update-check` | Skip the startup release check |

`--model`, `--review-model`, and `--subagent-model` require `--print` and
explicit model IDs. The optional `+EFFORT` suffix (`off`, `none`, `minimal`,
`low`, `medium`, `high`, `xhigh`) sets that seat's ACP reasoning effort.

## Subcommands

```bash
mj resume [SESSION_ID]
mj resume --list --format json --cwd /work/project
mj models refresh
mj memory list
mj memory add [--global] "one short fact"
mj memory forget m7
mj memory clear --yes
mj server [--hostname HOST | --tailscale]
```

See [Sessions, worktrees, and resume](/sessions-worktrees/) and [Remote
control](/remote/) for behavioral boundaries.

## Useful slash commands

| Command | Purpose |
| --- | --- |
| `/mjconfig` | Configure agents, ACP servers, and appearance |
| `/diff` | Show workspace changes against `HEAD` |
| `/agents` | Show the active model selections and per-seat usage |
| `/review` | Choose a recent, uncommitted, or HEAD findings-only review |
| `/review recent` | Review the latest change-producing turn |
| `/review uncommitted` | Review all current worktree changes |
| `/review head` | Review `HEAD` |
| `/memory` | List stored memories and the use/generate toggles |
| `/memory add [--global] <text>` | Save a memory for this project (or globally) |
| `/compact` | Compact the primary agent's session where supported |
| `/plan` | Toggle the selected agent's native planning mode where advertised |
| `/goal OBJECTIVE` | Start the selected agent's native persistent-goal workflow where advertised |
| `/subagents` | Open the session-wide actor roster and its retained transcripts |
| `/ragnarok TASK` | Summon the model-vs-model arena for one implementation task |

Agent commands are capability-driven. Codex advertises `/plan` and `/goal`,
Claude advertises both with a compatible recent adapter, and Kimi Code
advertises `/plan` but not `/goal` in compatible releases. Older or custom
adapter executables may expose fewer commands. The interactive autocomplete is
the source of truth for the active session; Hel does not emulate a missing
command or expose the adapters' general permission modes. While an advertised
`/goal` request is active, the prompt composer title shows `Pursuing goal`.

## Keyboard basics

- Enter sends a prompt or accepts the selected action.
- Up/Down navigate autocomplete and permission choices.
- PageUp/PageDown scroll the transcript.
- Ctrl+Tab switches between the four Codex and Claude coder/reviewer teams.
- Shift+Tab changes the current agent's model and effort.
- Typing `@` opens workspace file autocomplete; the chosen file is attached to
  the prompt as an ACP resource link.
- Alt+Up (or Shift+Left) pulls the newest queued entry back into the composer
  for editing. `/model` and `/effort` submitted while the agent is busy join the
  same queue and apply in turn, so they peel back the same way.
- F10 toggles help.
- Esc dismisses autocomplete, clears input, or cancels a permission prompt.
- Ctrl-C cancels the active turn together with every running subagent; on an
  idle, empty prompt it quits.
- Ctrl-D quits when input is empty.
- Ctrl-R starts or stops microphone dictation when the voice worker is available.

Long permission commands, descriptions, and options must remain reachable; a
truncated prompt is a UI bug, not an instruction to guess.
