# Parallel Luna reliability campaign

This guide runs four Luna coding agents as concurrent exploratory testers without letting them edit Hel while they test it. It complements `.agents/docs/luna-reliability-runbook.md`: that runbook defines the detailed missions and evidence standard, while this guide defines parallel ownership, browser operation, coordination, and cleanup. The campaign is local and must use only disposable fake-ACP, local-bare Hel state.

## Operating model

Use four workers and three labs. `tui-ux` owns an isolated lab for terminal interaction. `fault-recovery` owns another isolated lab for process failure and recovery. `shared-tui` owns a third lab and its two Hel dashboards. `shared-web` connects to the third lab through one browser. The shared web worker never starts, signals, stops, or deletes a Hel process; `shared-tui` is the sole infrastructure and cleanup owner for that lab.

Each Luna may write only beneath its assigned artifact directory and the disposable runtime named by that lab's `luna-env.sh`. It must not edit source, configuration tracked by Git, tests, documentation, or generated files outside those roots. A finding is recorded before anyone attempts a fix.

The browser is semantic-first. Playwright MCP exposes the page's accessibility snapshot, roles, labels, text, DOM state, console messages, and network activity to Luna. Those are the normal navigation and evidence channels. A screenshot is appropriate only for an inherently visual question such as clipping, overlap, responsive layout, focus indication, or color, or when semantic evidence cannot explain a failure. Prefer one targeted element or viewport image and record why it was needed. Do not take screenshots after routine actions.

## Prepare the campaign

Work from the repository root. Build once and install the repository-pinned browser once:

    cargo build -p hel-cli
    npm ci --prefix tests/e2e/web
    npx --prefix tests/e2e/web playwright install chromium

Choose three different integer seeds. Prepare the two isolated labs and the shared lab, recording the exact `artifacts=`, `runtime=`, and `source` lines printed by every command:

    python3 tests/e2e/prepare-luna-lab.py --seed 1101 --hel ./target/x86_64-unknown-linux-musl/debug/hel
    python3 tests/e2e/prepare-luna-lab.py --seed 1102 --hel ./target/x86_64-unknown-linux-musl/debug/hel
    python3 tests/e2e/prepare-luna-lab.py --seed 1103 --hel ./target/x86_64-unknown-linux-musl/debug/hel

Do not recover paths with a broad glob after several campaigns exist. Copy each printed path into a role-specific variable in the coordinating shell:

    export HEL_PARALLEL_TUI_ENV=/exact/tui-ux/luna-env.sh
    export HEL_PARALLEL_FAULT_ENV=/exact/fault-recovery/luna-env.sh
    export HEL_PARALLEL_SHARED_ENV=/exact/shared/luna-env.sh
    export HEL_PARALLEL_RUN=/exact/new/parallel-campaign-directory

Create `HEL_PARALLEL_RUN` beneath `target/reliability-artifacts/`, with one subdirectory per worker. Record the commit, seeds, Luna model, tmux version, terminal dimensions, and pinned Playwright version in `manifest.md`. Never copy a login code, QR URL, cookie, token, or environment containing credentials into that manifest.

Start a fresh tmux server with a control window and one window per Luna. The server name must be unique to this campaign and must not reuse a server that contains Hel dashboard clients:

    tmux -L hel-luna-parallel-1100 -f /dev/null new-session -d -s campaign -n control -x 160 -y 48
    tmux -L hel-luna-parallel-1100 new-window -d -t campaign -n tui-ux
    tmux -L hel-luna-parallel-1100 new-window -d -t campaign -n fault-recovery
    tmux -L hel-luna-parallel-1100 new-window -d -t campaign -n shared-tui
    tmux -L hel-luna-parallel-1100 new-window -d -t campaign -n shared-web

Launch each worker with `codex exec -m gpt-5.6-luna --ephemeral -s workspace-write --approve-for-me -C /home/jonathan/Projects/hel2`. Pass its exact role, environment-file path, artifact directory, mission assignment, ownership limits, and the instruction to follow this guide plus `.agents/docs/luna-reliability-runbook.md`. The browser invocation additionally configures the repository-pinned Playwright MCP server per process; do not add it to global Codex configuration:

    -c 'mcp_servers.playwright.command="/home/jonathan/Projects/hel2/tests/e2e/web/node_modules/.bin/playwright"'
    -c 'mcp_servers.playwright.args=["mcp","--headless","--isolated","--ignore-https-errors","--snapshot-mode","full","--output-dir","/exact/shared-web/browser"]'

Do not add the optional `vision` capability. Screenshots remain available for explicit visual investigation, but structured snapshots are the primary operating mode.

## Worker missions

`tui-ux` runs mission M4 from the reliability runbook and also explores every pane, dialog, hotkey, narrow size, long transcript, scroll boundary, detach/reattach path, and clipboard failure. It owns its daemon and any nested tmux server it creates. It must distinguish terminal corruption from text merely present in scrollback.

`fault-recovery` runs missions M3 and M6 through M8. Before sending any signal it verifies the exact process environment contains its lab's `HEL_CONFIG_DIR`, `HEL_DATA_DIR`, and `HEL_CHAOS_ISOLATED=1`. It never selects a victim using only a process name or grep match.

`shared-tui` runs missions M1 and M2, starts two dashboards in its lab, and creates `$HEL_PARALLEL_RUN/shared-ready` only after both dashboards show the same workspace and the Web dialog can be opened. It owns lifecycle and cleanup for the shared lab. Before each coordinated race it writes a future Unix timestamp to `$HEL_PARALLEL_RUN/phase-N-go`; both shared workers act when that timestamp arrives and record their observed start time. This makes timing comparable without either agent writing the other's files.

`shared-web` waits for `shared-ready`, authenticates through the disposable Web dialog, and runs mission M5 plus the web half of M1 and M2. It drives the real page through accessibility snapshots and role- or label-based actions. It exercises desktop and mobile viewport sizes, browser history, logout, cookie expiry, offline/online SSE recovery, stale conversations, lifecycle actions, and convergence with both dashboards. It may request shared actions through `$HEL_PARALLEL_RUN/web-requests.md`, but it never manipulates shared Hel processes directly.

Workers must not fix defects during the campaign. They may minimize a reproduction inside their disposable lab. Once the exact sequence and evidence are preserved, they finish their assigned missions even if another finding has already been recorded.

## Evidence and coordination

Every worker owns `$HEL_PARALLEL_RUN/workers/<role>/` and no worker writes another role's files. Each directory contains `actions.md`, `result.md`, bounded logs, process evidence, and any role-specific captures. Append an action before executing it, including wall-clock time and intentional delay. `result.md` ends with `PASS`, `FAIL`, or `BLOCKED` for every assigned mission.

Each finding contains the commit, worker role, lab seed, mission, dimensions or viewport, exact keys/clicks/signals and relevant pauses, expected and observed behavior, the first relevant bounded log lines, artifact paths, and the proposed deterministic regression layer. Browser findings normally include accessibility snapshots, console messages, request status, and current URL. A visual finding may additionally include one targeted screenshot. Playwright traces and screenshots must not expose viewer credentials.

Use `$HEL_PARALLEL_RUN/web-requests.md` only as an append-only request log. Every request has an identifier. `shared-tui` records its response in its own `actions.md`; it does not edit the request line. Use unique role-owned readiness files and numbered phase files instead of having several agents rewrite one shared status document.

The coordinating operator watches progress without interacting with a worker's nested Hel tmux server:

    tmux -L hel-luna-parallel-1100 list-windows -t campaign
    tmux -L hel-luna-parallel-1100 capture-pane -p -S -200 -t campaign:tui-ux
    tmux -L hel-luna-parallel-1100 capture-pane -p -S -200 -t campaign:shared-web

Do not treat a Luna final answer as sufficient evidence. Inspect its role directory and reproduce every reported failure before changing code.

## Triage, repair, and rerun

Classify each finding as a product defect, harness/runbook defect, expected refusal, duplicate, or insufficient evidence. Reproduce a product defect with the smallest exact action sequence. Add a deterministic module, PTY, browser, property, or named-hook regression before or with the repair. Fix the source rather than weakening an assertion or hiding an error. Rerun the focused regression, the failed Luna mission with the original seed, and the neighboring shared-client scenario.

If the campaign exposes an ambiguous command, unsafe ownership rule, missing prerequisite, evidence gap, or coordination failure, update this guide in the same repair checkpoint. Add a short `Campaign lessons` entry naming the mistake and the corrected procedure so later campaigns do not rediscover it.

## Finish and acceptance

Wait for all four `result.md` files before cleanup. Each isolated worker stops its daemon, verifies SQLite integrity and foreign keys, checks for processes retaining its exact `HEL_CONFIG_DIR`, terminates its nested tmux server, and only then removes its exact runtime root. `shared-web` closes its browser and writes `browser-closed`; `shared-tui` waits for that marker before performing the shared lab's integrity, leak, and cleanup checks.

The coordinator writes `summary.md` with every mission result, finding classification, repair commit when applicable, original-seed rerun result, SQLite result, leak audit, and cleanup outcome. A campaign passes only when all confirmed defects are repaired and rerun, every lab reports `integrity_check` as `ok` with no foreign-key output, no owned processes remain, and no unresolved finding involves data loss, duplicate transcript events, lifecycle failure, authentication leakage, terminal corruption, UI hang, or cross-client divergence.

Finally kill only the campaign's outer tmux server:

    tmux -L hel-luna-parallel-1100 kill-server

Retain the campaign artifact directory. Never loop a failed case until it turns green and never erase the first failing evidence after a repair.

## Campaign lessons

Add dated entries here after executing this guide. State the operational mistake, its observable consequence, and the corrected procedure.
