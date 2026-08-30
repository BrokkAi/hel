# Luna reliability campaign

This is a local exploratory test campaign for Hel's Linux/WSL runtime. It is not a CI job and it must never use personal Hel state, paid harnesses, remote targets, or real credentials. A finding is useful only when its exact actions and evidence are saved so the behavior can become a deterministic regression.

This runbook defines the missions and evidence for a single campaign operator. To distribute those missions across four concurrent Luna workers with explicit lab ownership and a semantic browser lane, follow `.agents/docs/parallel-luna-testing.md` as the outer orchestration guide and use this document for the mission details.

## Prepare an isolated campaign

Build the current commit and install the pinned browser once:

    cargo build -p hel-cli
    npm ci --prefix tests/e2e/web
    npx --prefix tests/e2e/web playwright install chromium

Choose a seed, prepare the disposable fake-ACP/local-bare lab, and source the printed environment file in the shell that will start tmux:

    python3 tests/e2e/prepare-luna-lab.py --seed 1 --hel ./target/x86_64-unknown-linux-musl/debug/hel
    source target/reliability-artifacts/luna-manual-seed-1-*/luna-env.sh

Record the build and terminal geometry before doing anything else:

    git rev-parse HEAD | tee "$HEL_LUNA_ARTIFACTS/commit.txt"
    printf 'outer=%sx%s\n' "$(tput cols)" "$(tput lines)" | tee "$HEL_LUNA_ARTIFACTS/dimensions.txt"
    tmux -L hel-luna-1 -f /dev/null new-session -d -s hel -x 140 -y 40
    tmux -L hel-luna-1 split-window -h -t hel:0
    tmux -L hel-luna-1 split-window -v -t hel:0.1
    tmux -L hel-luna-1 send-keys -t hel:0.0 "source '$HEL_LUNA_ARTIFACTS/luna-env.sh'; '$HEL_LUNA_BINARY'" Enter
    tmux -L hel-luna-1 send-keys -t hel:0.1 "source '$HEL_LUNA_ARTIFACTS/luna-env.sh'; '$HEL_LUNA_BINARY'" Enter
    tmux -L hel-luna-1 send-keys -t hel:0.2 "source '$HEL_LUNA_ARTIFACTS/luna-env.sh'; watch -n 1 '$HEL_LUNA_BINARY daemon status'" Enter
    tmux -L hel-luna-1 attach -t hel

Use the first TUI to accept the proposed workspace name. Attach the second TUI to that workspace. Open the Web dialog with `Ctrl+B`, scan its QR code or enter the six-digit code in a mobile-sized browser, and confirm all three surfaces show the same empty workspace. Record the browser name/version and viewport in `dimensions.txt`.

Before each mission, append its identifier and wall-clock time to `notes.md`. Luna should vary pauses and interleavings from the campaign seed, but must write down every key and click exactly; “clicked around” is not a reproduction.

## Mission cards

### M1 — three-client lifecycle

Create a session from the web viewer. Confirm both TUIs show provisioning immediately. Open the conversation in the browser, send one prompt from TUI 1, then queue a second before the fake reply completes. Read from TUI 2 and the browser. Stop from TUI 2, resume from the browser, and stop from TUI 1. Check that each prompt and reply appears exactly once and every surface converges without a manual refresh.

### M2 — rapid and overlapping control

With both TUIs and the browser attached, alternate `Ctrl+E`, Escape, arrow keys, Enter, Tab, and pane-specific refresh keys as quickly as tmux can deliver them. Race Stop against Resume or a second Stop from another surface. A refusal is acceptable when it names the active operation; an unbounded spinner, UI freeze, duplicate lifecycle, or stale terminal state is not.

### M3 — cancellation boundaries

Start a new session and cancel during provisioning from a different client. Repeat during resume and during a long fake prompt. Exercise both Cancel buttons and Escape. Confirm the UI remains responsive, the operation settles, and no worker or worktree survives solely because cancellation won a race.

### M4 — resize, scroll, selection, and clipboard

Resize the tmux client repeatedly through 40×10, 72×18, 140×40, and 200×60 while holding navigation keys. Scroll every pane to both limits. Select transcript text with keyboard and mouse, copy it, paste into a prompt, and repeat with `DISPLAY` and `WAYLAND_DISPLAY` unset in one fresh TUI to force clipboard failure. The failure must appear as a bounded Hel notice; library stderr must never overwrite the alternate screen. Detach and reattach after each resize series.

### M5 — stale browser and authentication

Leave a conversation open, disconnect the browser network, perform prompt and lifecycle work from both TUIs, then reconnect. The page must refresh through SSE without reloading. Expire or remove its session cookie and confirm APIs return to the login screen. Log in again by QR, sign out, and verify Back cannot expose a cached authenticated snapshot or token-bearing URL.

### M6 — daemon death

Record the daemon PID from `hel daemon status`, send it `SIGTERM`, and keep typing in both TUIs while it restarts. Repeat with `SIGKILL` only after confirming the PID's `/proc/<pid>/environ` contains the campaign's exact `HEL_CONFIG_DIR` and `HEL_DATA_DIR`. The browser and both TUIs must reconnect, revisions must not move backward, and one client stopping a session must update the other two.

### M7 — worker and bridge death

Use the process tree to identify the campaign-owned worker and fake ACP bridge for one session. Verify their environment points at this lab, then kill one generation at a time. Submit a prompt before and after each death. The session may show a named recoverable error, but acknowledged transcript events must remain exactly once and Stop must still settle. Never signal a process identified only by a name or grep match.

### M8 — recovery and shutdown

Interrupt checkpoint/close by killing the daemon, restart with either TUI, and retry Stop. Detach one TUI while work is active, terminate the other with `SIGTERM`, then reattach. Terminal modes, mouse capture, cursor visibility, and bracketed paste must be restored before any final shell message. Quit must remain bounded while supervised background cleanup reports any failure.

## Evidence for every mission

After each mission, capture both panes with escapes intact and refresh the bounded process/log evidence:

    tmux -L hel-luna-1 capture-pane -p -e -S -2000 -t hel:0.0 > "$HEL_LUNA_ARTIFACTS/tui-1.capture"
    tmux -L hel-luna-1 capture-pane -p -e -S -2000 -t hel:0.1 > "$HEL_LUNA_ARTIFACTS/tui-2.capture"
    ps -eo pid=,ppid=,pgid=,sid=,stat=,etimes=,args= > "$HEL_LUNA_ARTIFACTS/process-tree.txt"
    tail -n 2000 "$HEL_DATA_DIR/daemon.log" > "$HEL_LUNA_ARTIFACTS/daemon.log"
    find "$HEL_DATA_DIR/logs" -type f -name '*.log' -print -exec tail -n 500 {} \; > "$HEL_LUNA_ARTIFACTS/controller.log"

Save any browser screenshot and Playwright trace as `browser-failure.png` and `browser-trace.zip`. Copy or update the lab's `trace.json`; do not put viewer codes, QR URLs, cookie values, profile environments, or other authentication material in notes, traces, screenshots, or terminal captures.

Each finding in `notes.md` must contain:

1. commit, seed, terminal size, browser viewport, and mission identifier;
2. exact keys, shell signals, and web actions with pauses that mattered;
3. expected behavior and observed behavior;
4. the first relevant bounded log lines and process IDs;
5. artifact filenames; and
6. a proposed deterministic unit, PTY, browser, property, or named-hook regression.

Do not fix a finding during the campaign. First preserve a minimal reproduction and choose the right deterministic layer.

## Finish and clean up

Quit both TUIs normally when possible, then stop the daemon. Check integrity before removing anything:

    "$HEL_LUNA_BINARY" daemon stop
    sqlite3 "$HEL_DATA_DIR/hel.sqlite3" 'PRAGMA integrity_check; PRAGMA foreign_key_check;' | tee "$HEL_LUNA_ARTIFACTS/integrity.txt"
    ps eww -eo pid=,ppid=,pgid=,stat=,args= | rg -F "HEL_CONFIG_DIR=$HEL_CONFIG_DIR" | tee "$HEL_LUNA_ARTIFACTS/leaks.txt"
    tmux -L hel-luna-1 kill-server

`integrity_check` must print `ok`, `foreign_key_check` must print nothing, and `leaks.txt` must contain no campaign-owned process after the observer commands exit. Only then remove the exact directory printed as `HEL_LUNA_RUNTIME_ROOT`; retain the artifact directory and record whether every mission passed, failed, or was blocked. A no-defect campaign still keeps `notes.md`, captures, bounded logs, process tree, trace, browser evidence when used, and integrity result.
