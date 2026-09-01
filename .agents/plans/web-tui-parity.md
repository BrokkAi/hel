# Rebuild the Hel web viewer as a phone-native version of the terminal surface

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

The canonical rules for ExecPlans live in `.agents/PLANS.md`, relative to the repository root. This document must be maintained in accordance with that file.

## Purpose / Big Picture

Hel runs coding agents in isolated targets and gives you two ways to watch and drive them. The first is the terminal surface, started with `hel`: one combined screen with a Sessions pane, a transcript, a Prompt composer, a Targets pane and a Quota pane. The second is a web viewer the Hel daemon serves over HTTPS, usually reached from a phone across a Tailscale network, unlocked with a six-digit code.

Today those two surfaces are not comparable. The web viewer is a single scrolling page. It has a create-session form, one flat list of every session Hel knows about, a small "Configured" card that prints one line per profile plus a `"1 targets · 1 bundles"` count, and, once you press Open, a conversation appended underneath. It has no idea that workspaces exist: `ViewerSnapshot` carries a `workspaces` list and the JavaScript never reads it. It shows stopped and lost sessions beside running ones. It offers no rename, no active-turn cancel, no slash commands, no prompt history, no drafts, and no capacity or quota detail beyond a single summary string. Its transcript is built by joining escaped strings into `innerHTML`, and the page has no content-security policy. Its only route state is `#conversation/{id}`, so Back from a conversation leaves the app rather than returning to the list.

After this change a phone can do the normal operator's job. Concretely, someone holding a phone can:

* Choose a workspace from a row of tabs, and land back on the same workspace after a reload, a Back press, or a re-login, because the workspace is in the URL.
* Create a session on a machine that has more than one workspace at all. This is not a refinement: `apply_phone_action` in `crates/hel-cli/src/server.rs` fails a phone `new` with `phone session creation requires a workspace_id` whenever the daemon holds two or more workspaces, and the current viewer never sends one. Phone session creation is therefore broken outright on any multi-workspace install.
* Walk a guided New flow (profile, then target, then project or bundle or bare directory, then a dirty-worktree confirmation when one is needed, then a review step) and a guided Resume flow (session, profile, compatible target, keep or discard queued prompts, review), instead of filling one flat form and answering a `window.prompt()` dialog that asks them to type the word `start` or `discard`.
* Rename a session, stop it, cancel a running provision or resume, and cancel the agent's current turn — with each button shown only because the daemon said that action is legal for that session, not because the JavaScript guessed from a status string.
* Read a conversation that looks like the terminal transcript: `❯ You`, `● Agent`, `○ Thinking`, tool rows carrying their state glyph, plan rows carrying `○ ● ✓`, rendered Markdown, timestamps, collapsible thinking and tool detail, and image messages.
* Type prompts, shell commands, and the same slash commands the terminal accepts — `/help`, `/detach`, `/model`, `/effort`, `/fast`, `/plan`, `/implement` — with autocomplete, with the agent's own advertised commands forwarded as prompts, and with prompt history and reverse lookup.
* Leave a half-written prompt on the phone, close the browser, come back, and find the draft still there, because the daemon stored it against that viewer and that session.
* Open a Targets page that reports CPU, memory, cores and disk per host or fleet with honest loading, refreshing, stale and error states, and a Quota page grouped by profile with real windows, progress, reset times and a manual refresh — both as full pages reached from a menu, never as cards squeezed onto the session list.

You can see the whole thing working by running the repository's own browser reliability lab, which starts a real daemon with a fake agent harness and drives the real viewer with Playwright:

    cd /home/jonathan/Projects/hel
    cargo build --bin hel
    (cd tests/e2e/web && npm ci)
    tests/e2e/run-browser-reliability.sh --seed 4242 target/x86_64-unknown-linux-musl/debug/hel

The scope is the normal operator workflow. Native session import, force cleanup, machine configuration editing, advanced mounts and resource overrides, and raw terminal or tool streams stay in the terminal surface. Where a phone meets one of those, it says so plainly and points the person at the terminal instead of failing.

## Progress

- [x] (2026-09-01 01:46Z) Milestone 1 — The browser application now lives in `src/web/` as `viewer.html`, `viewer.css`, `viewer.js`, `service-worker.js` and `manifest.webmanifest`, embedded with `include_str!`. The vendored JetBrains Mono and the four PNG icons are served for the first time. One `axum` layer applies the content-security policy, `X-Content-Type-Options`, `Referrer-Policy` and `no-store` for `/api/` and `/auth/`, so no route can forget them. The service worker is versioned, deletes superseded caches, and declines `/api/` and `/auth/` outright. The two Node-driven JavaScript checks read the real file and run from a temporary directory. Fixing the browser reliability lab, which was red on `master` for three unrelated reasons, was part of the milestone; see `Surprises & Discoveries`.
- [x] (2026-09-01 02:10Z) Surveyed the sibling `../mjolnir` repository against every remaining milestone, with fourteen agents reading `mj-remote/src/remote_viewer.html` and `mj-remote/src/remote.rs` subsystem by subsystem and evaluating off-the-shelf libraries for Markdown, keyed DOM updates, routing and syntax highlighting. The plan was revised throughout: a new orientation subsection, a new `Porting rules` section, and a port-versus-build paragraph in every milestone. Three findings were verified by hand against the source before being acted on; see `Surprises & Discoveries`.
- [x] (2026-09-01 03:05Z) Milestone 2, first half — the design system and a renderer agent output cannot escape. `src/web/viewer.css` is now a token layer in the terminal's semantic colours with `--dim` held above 4.5:1, 44px touch targets, 16px form text, per-edge safe-area padding and a global `min-width: 0`. `src/web/markdown.js` is committed, served at `/markdown.js` and imported by the viewer as an ES module; its `renderDiffSummary` was rewritten for the real `path  +12 −3` format. Every string-built markup site in `viewer.js` is gone — the session list, the configured card, the selects, the queue, the shells and the transcript all build DOM — and `escapeHtml`/`escapeAttr` are deleted. Transcript entries carry the terminal's role glyphs in an `aria-hidden` column beside the label they repeat, and prose roles render as Markdown while tool output stays preformatted. Two new tests: 30 renderer checks covering structure and every injection case, and a sink test over `src/web/*.js` with no allowances.
- [x] (2026-09-01 03:40Z) Milestone 2, second half — `src/web/tool-output.js` ports mjolnir's tool-output layer: fourteen language families tinted by one cached regex each behind a 120,000-character bound, `<details>` folds that build their content on first open so a long dump costs nothing closed, JSON reparsed and tinted with keys apart from values, shell commands tokenised into program, subcommand, flag, path and operator with the pipeline count reset on `&&`, and conservative language sniffing that only wins at double the runner-up score. Fenced code in `markdown.js` routes through it, and non-prose transcript roles render through it rather than as Markdown. mjolnir's diff computation was deliberately not ported: hel publishes a counted summary rather than the old and new text, so it would have nothing to run on. Fifteen new checks cover tinting, sniffing, folding, and that tool output is never read as Markdown.
- [x] (2026-09-01 04:40Z) Milestone 3 — the projection now carries what the browser needs and still publishes nothing it must not. `ViewerSession` gained a sanitized `project_label` and a hashed `project_key`, a `lifecycle` category with `is_dashboard_visible`, `latest_event_ordinal`, an optional `operation`, `chat_phase`, `config_options`, `plan_mode_active`, `compatible_resume_targets`, and an explicit `ViewerSessionCapabilities` filled from the live relay state rather than guessed from the durable record. `ViewerQuota` gained structured `windows` with `percent_used` and an exhaustion projection, keeping `summary` for a cached viewer. `ViewerTargetCapacity` and `ViewerOperation` exist and are published. `ControllerAction` gained `Rename`, `CancelTurn`, `SetConfig`, `SetPlanMode`, `RefreshQuota` and `RefreshCapacity`; `New` gained a workspace, an optional title derived the way the terminal derives it, and a `dirty_ack` that names repositories and is re-checked against what is dirty at commit. Plan mode goes through the terminal's own decision, published as `hel_acp::AcpSessionFacts`, rather than a second copy. `RuntimeState::active_lifecycles` was added after confirming the existing lifecycle watch channel is built by the dashboard's poller and never reaches the phone server.
- [ ] (remaining from Milestone 3) Enriched `BrowserTranscriptEntry`: semantic kind and status, explicit timestamp, request identity, and structured tool metadata including the diffstat as data. Deferred to sit beside the transcript work in Milestone 7, where it is consumed.
- [x] (2026-09-01 05:30Z) Milestone 4 — the viewer is a router over pages rather than two hidden sections. Six routes with a legacy alias, canonicalised so a first visit writes `#workspace/{id}` and every later navigation has something to go back to. A scrollable workspace tab strip marks its selection with `aria-current` and a border rather than colour alone. The dashboard shows only live sessions, grouped by project with a count, and each row states its lifecycle with a glyph and a word, its attention and queue depth, its running operation and stage, and its target and profile. Every row control is rendered from `ViewerSessionCapabilities` and nothing infers legality from a status string. A pending-action set derives the disabled state, so a double tap cannot send twice. Targets and Quota are routes reached from a menu that closes on Escape and on an outside tap. The browser suite now drives the whole shape: two URL assertions, the menu, the Quota page, the browser's own Back button, a stopped session leaving the dashboard, and a resume driven from the Resume page.
- [ ] (not started) Milestone 5 — Guided New and Resume flows with a sanitized preflight endpoint.
- [ ] (not started) Milestone 6 — Targets and Quota as global pages backed by supervised background probes.
- [ ] (not started) Milestone 7 — The conversation screen: transcript, composer, queue, shells, elicitations, images, slash commands, autocomplete, history.
- [ ] (not started) Milestone 8 — Per-viewer client state: identity in the cookie, drafts, read frontiers, bounded history search, pruning.
- [ ] (not started) Milestone 9 — Phone behaviour, accessibility, resilience, and the Playwright matrix at 320×568, 390×844 and a wider viewport.

Use timestamps in the form `(2026-08-31 14:20Z)` as steps complete, so a later reader can measure the rate of progress.

## Surprises & Discoveries

- Observation: the phone session cookie carries no viewer identity. `signed_cookie_value` in `src/hel_server.rs` signs only the expiry timestamp, producing `"{expiry}.{signature}"`. Two phones that unlock the viewer in the same second receive byte-identical cookies, and one phone's cookie changes on every login. The read-frontier client id is `format!("phone:{cookie}")`, so today's "per-client" read frontier is really "per second at which somebody logged in". Any per-viewer draft keyed the same way would leak between phones and vanish on re-login. Milestone 8 therefore has to put a random viewer id inside the signed cookie before drafts can mean anything.
  Evidence: `src/hel_server.rs`, `fn signed_cookie_value(key: &[u8], expiry: u64) -> String` builds `canonical = expiry.to_string()` and returns `format!("{canonical}.{signature}")`; `mark_conversation_read` builds `client_id: format!("phone:{cookie}")`.

- Observation: JetBrains Mono and a full set of PWA icons are already vendored in the repository at `src/fonts/jetbrains-mono.woff2` and `src/icons/` (`icon.svg`, `icon-192.png`, `icon-512.png`, `maskable-512.png`, `apple-touch-icon.png`), and nothing references them. The served manifest points at `/icon.svg`, which comes from an inline `ICON` string constant in `src/hel_server.rs`, not from `src/icons/icon.svg`.
  Evidence: `grep -rn "jetbrains\|icon-192\|apple-touch\|maskable\|rajdhani\|staatliches" --include=*.rs --include=*.toml .` matches only the manifest's `/icon.svg` string.

- Observation: the phone server never probes capacity. `spawn_dashboard_capacity_poller` lives in `crates/hel-cli/src/pollers.rs` and is called only from `crates/hel-cli/src/dashboard.rs`. The phone control loop already spawns the sibling quota refresher (`spawn_quota_refresher`), so the Targets page is a matter of spawning the existing poller in the existing loop rather than writing a new probe.
  Evidence: `grep -rn "spawn_dashboard_capacity_poller" crates/ src/` matches only `pollers.rs:978` and `dashboard.rs:906`.

- Observation: the daemon already tracks exactly the operation progress the phone needs. `RuntimeLifecycleView` in `crates/hel-cli/src/daemon.rs` carries `kind`, `started_at_epoch_seconds`, `active_stages: Vec<(ProvisionStage, u64)>`, `resume_destination` and `notice`, and `ProvisionStage::label()` already renders `Provision`, `Boot`, `Clone`, `Sync`, `Restore`, `Start`, `Compact`. Nothing has to be invented for "current operation".
  Evidence: `crates/hel-cli/src/daemon.rs:149`, `src/hel_targets.rs:57`.

- Observation: `search_prompts` in `src/hel_database.rs` pages through the entire `prompt_history` table without any overall bound; it stops only when a page comes back short. It is acceptable for a terminal that runs it once per keystroke against a local database, but it must not be reachable straight from an HTTP route.
  Evidence: `src/hel_database.rs`, `fn search_prompts_from`, whose `loop` breaks only on `page_len < PAGE_SIZE`.

- Observation: the browser reliability lab was red on `master` before any of this work, for three independent reasons, and had to be repaired before it could serve as acceptance for anything. First, Chromium refuses to register a service worker over the lab's self-signed certificate; Playwright's `ignoreHTTPSErrors` covers page and API requests but not the service-worker script fetch, so the registration rejection surfaced as a page error and failed the suite's very first assertion. Second, `render_terminal` in `tests/e2e/reliability_lab.py` reconstructs the captured screen onto a fixed 32-row, 140-column grid, while `browser_lab.py` resizes the terminal to 40×150 — so the footer row was outside the reconstruction entirely and text past column 140 smeared, which made screen assertions match things that were not on screen. Third, `stop_from_dashboard` pressed one Tab and then `e`, on the assumption that the surface starts with the keyboard in Prompt and that one Tab reaches Sessions; the surface actually starts on Sessions, so the Tab moved focus *away* and `e` was typed into the composer as a letter.
  Evidence: on unmodified `master`, seed 4243 failed with `Failed to register a ServiceWorker … An SSL certificate error occurred when fetching the script.`; seed 4245, with only the certificate fixed, failed with `tui-1 did not display 'Edit session'` while the reconstructed screen showed the Quota pane's `Rename profile ID` dialog. Instrumenting the pane walk printed `after tab 1 footer ['Ctrl-G panes · Tab pane · PgUp/PgDn transcript …']` — the Prompt footer — proving focus began on Sessions.

- Observation: a stop issued from the terminal surface against a session the browser created moments earlier can fail with `connect to the session worker for checkpoint: session … is not managed`, and then succeed on retry. The daemon's session manager adopts sessions asynchronously, so the first stop can arrive before adoption. The surface already offers `Retry stop` for this, and the lab now takes it.
  Evidence: seed 4252 failed with that message in a `Stop could not complete` dialog; seed 4253 and seed 4254, with no product change, passed with `browser reliability: passed clients=2 sse_reconnect=1 leaks=0`.

- Observation: reformatting the extracted JavaScript is safe to verify mechanically. Running `prettier` over the original minified source and over the extracted file produces byte-identical output, and the same round-trip on the CSS is identical after whitespace normalisation, so the move provably changed no code.
  Evidence: `diff -u <(prettier orig.js) src/web/viewer.js` reported no difference; a minify-and-compare of the original `<style>` block against `src/web/viewer.css` reported `identical`.

- Observation: hel's diffstat is not the format this plan first assumed, and the renderer written against the assumption is broken. `format_diffstat` in `src/hel_chat/transcript.rs` emits the path, two spaces, `+{insertions}`, a space, and `−{deletions}` with a Unicode MINUS SIGN at U+2212 — no pipe character anywhere. `renderDiffSummary` in `src/web/markdown.js` splits on `|`, so the split never fires, the whole line lands in the path element, and the change-count element is never created. The lesson generalises: the projection should carry the diffstat as data, so the browser is not re-parsing a string the Rust side formatted for a terminal.
  Evidence: `sed -n '1857p' src/hel_chat/transcript.rs | cat -A` shows `format!("{}  +{insertions} M-bM-^HM-^R{deletions}", diff.path.display())`, where `M-bM-^HM-^R` is U+2212.

- Observation: the synchronous `active_lifecycles` accessor this plan specified for `RuntimeState` may not be needed. A `watch::Receiver<Vec<daemon::RuntimeLifecycleView>>` already exists on `RemoteDashboardWorkerPoller` in `crates/hel-cli/src/pollers.rs` and is consumed as `runtime_lifecycles` in `crates/hel-cli/src/dashboard.rs`. Subscribing the phone control loop to that channel adds no second path to the same mutex.
  Evidence: `pollers.rs` declares `pub(crate) lifecycles: tokio::sync::watch::Receiver<Vec<daemon::RuntimeLifecycleView>>`; `dashboard.rs` holds `runtime_lifecycles: Feed<watch::Receiver<Vec<crate::daemon::RuntimeLifecycleView>>>`.

- Observation: mjolnir has the identical cookie defect and never noticed, which confirms the identity work in Milestone 8 is genuinely new rather than a port. Its `session_cookie_value` signs only the expiry and returns `{exp}.{sig}`, the same shape as hel's. Nothing there was ever keyed to a viewer, so the flaw had no symptom.
  Evidence: `mj-remote/src/remote.rs`, `fn session_cookie_value` does `mac.update(exp.to_string().as_bytes())` and returns `format!("{exp}.{sig}")`.

- Observation: the whole 10,254-line mjolnir viewer contains exactly one occurrence of `innerHTML`, and it is inside the comment saying the renderer never uses it. Its no-markup-as-string discipline is real and complete — zero `style="` attributes, two CSSOM `setProperty` calls. But mjolnir ships no Content-Security-Policy at all, so none of that discipline has ever been enforced by a browser, and every borrowed piece must be treated as policy-plausible rather than policy-proven.
  Evidence: `grep -c innerHTML mj-remote/src/remote_viewer.html` returns 1, matching line 6366, the section comment; `grep -c content-security mj-remote/src/remote.rs` returns 0.

- Observation: every browser asset is embedded in the binary with `include_str!`, so editing `src/web/*.css` or `*.js` and re-running the browser lab without `cargo build --bin hel` tests the previous version. Three lab runs during Milestone 4 were spent on this before it was noticed. Anything that changes a web asset must rebuild before it means anything.
  Evidence: the same `.hidden` fix failed the lab at seed 4294 and passed at seed 4295, with no change between them but a rebuild.

- Observation: `.hidden { display: none }` did not hide the menu, because `.menu { display: flex }` is declared later in the same stylesheet and both are single-class selectors, so source order decided. The menu stayed on screen with `hidden` applied and swallowed taps meant for the page behind it, which Playwright reported as `<button role="menuitem" data-route="quota"> … intercepts pointer events`. A utility class that is applied to elements which set their own display has to win, so `.hidden` is now `display: none !important`.
  Evidence: the browser suite blocked for the full test timeout on `getByRole('button', { name: 'Resume a session' })` with that interception message, and passed once the utility was made to win.

Add further entries here as work proceeds, each with short evidence.

## Decision Log

- Decision: extract the viewer's HTML, CSS and JavaScript into real files under `src/web/` and embed them with `include_str!`, rather than introducing a frontend build system or keeping them as Rust string literals.
  Rationale: Hel ships as one binary and must keep doing so, and the existing tests already run the embedded JavaScript through Node by slicing the Rust string with `str::find`, which breaks whenever the source moves. Real files make the JavaScript reviewable, let Node import it directly, and let a content-security policy forbid inline script — which is impossible while everything lives in one `<script>` tag.
  Date/Author: 2026-08-31, plan author.

- Decision: keep the client vanilla JavaScript with no bundler, no framework and no npm dependency in the served asset path.
  Rationale: the served page must work from a single Rust binary with no build step, and the repository already has no frontend toolchain. Playwright in `tests/e2e/web` stays a test-only dependency.
  Date/Author: 2026-08-31, plan author.

- Decision: the browser renders actions from an explicit `ViewerSessionCapabilities` value supplied by the daemon, and never infers legality from the `state` string.
  Rationale: the current viewer decides between Cancel and Resume/Stop by testing `x.state==='provisioning'`, which is a copy of controller policy in JavaScript that drifts silently. Legality lives in the controller; the phone should be told, not asked to guess.
  Date/Author: 2026-08-31, plan author.

- Decision: short relay operations (prompt, shell, cancel shell, remove queued prompt, elicitation answer, config change, rename) answer `202 Accepted` as soon as the daemon acknowledges the enqueue; long lifecycle operations (new, resume, stop, capacity refresh, quota refresh) answer `202 Accepted` with an operation identifier and publish progress through the existing snapshot and SSE stream.
  Rationale: the existing comment on `async fn action` is right that a mobile network ends a request long before a provision finishes. Returning an operation id rather than nothing lets the phone show honest progress and offer cancellation for the specific operation it started.
  Date/Author: 2026-08-31, plan author.

- Decision: put a random viewer id inside the signed cookie, keep accepting the old two-part cookie during a rollout, and derive both the read frontier client id and the draft key from that id.
  Rationale: see the first entry in `Surprises & Discoveries`. Without it, "per-viewer drafts" cannot be built, and the isolation test the delivery section requires would be untestable.
  Date/Author: 2026-08-31, plan author.

- Decision: derive the initial session title on the server from the phone's own request, using the terminal's rule (`"{bundle or project directory} via {profile}"`), and never send an unrequested path back to the phone.
  Rationale: the terminal derives its title in `crates/hel-cli/src/dashboard/io.rs` as `format!("{} via {profile_id}", project_directory_or_bundle)`. For a bare-directory session the phone supplied the directory itself, so echoing it is not a disclosure; for every other session the title contains only a bundle id, which the phone already holds.
  Date/Author: 2026-08-31, plan author.

- Decision: repair the browser reliability lab as part of Milestone 1 rather than working around it or declaring it out of scope.
  Rationale: the plan uses that suite as the acceptance for six later milestones. A suite that cannot pass proves nothing, and each of the three failures was a defect in the harness rather than in the product: a certificate the browser was never told to trust, a screen reconstruction at the wrong size, and an assumption about which pane starts with the keyboard. All three would have produced misleading failures for every future change.
  Date/Author: 2026-09-01, plan author.

- Decision: verify the asset move mechanically instead of trusting a careful read.
  Rationale: moving 28 KB of minified JavaScript by hand is exactly the kind of change where a silent character-level mistake survives review. Formatting the original and the copy with the same tool and diffing them turns "I moved it faithfully" into something the tree can demonstrate.
  Date/Author: 2026-09-01, plan author.

- Decision: the security-headers layer, not each handler, owns `Cache-Control: no-store` for `/api/` and `/auth/`.
  Rationale: a rejected request never reaches its handler, so a handler-set header is missing from exactly the responses least worth storing. The first version of the layer left `no-store` in the handlers and a test caught an unauthenticated `/api/snapshot` answering with no cache directive at all.
  Date/Author: 2026-09-01, plan author.

- Decision: render Markdown with hel's own DOM-building renderer plus mjolnir's tool-output layer, and vendor no Markdown library.
  Rationale: every mainstream library except commonmark.js turns text into an HTML string, and a string has one way onto a page. Choosing one would mean routing every agent message through `innerHTML` and adding DOMPurify, which replaces a structural guarantee — this code cannot inject markup because it never builds markup — with a filtering one. It would also make security a vendored file: hel embeds assets with `include_str!` into one binary, so a bundled sanitiser is exactly as old as the binary in someone's hand, and a sanitiser's whole value is being updated when a bypass is found. Against that it buys little, because the CommonMark subset is the easy half and is already written, while the hard half — folding a five-thousand-line tool dump, tinting JSON, tokenising a shell command, sniffing the language of unfenced output — is something no Markdown library does and mjolnir already has. If this is ever overruled, commonmark.js is the one to take, because it exposes an AST that can be walked into DOM nodes and so does not force `innerHTML`.
  Date/Author: 2026-09-01, plan author.

- Decision: borrow from mjolnir at the level of presentation and phone interaction only, never data access, and never a legality check.
  Rationale: mjolnir's viewer reads its own server's records and infers what a person may do from them — `archived ? 'load' : session.web_owned ? 'archive' : 'terminal'` is a representative line. Hel's projection is different in shape and its Decision Log already forbids inferring legality in JavaScript. The DOM code lifts cleanly because it touches no globals; the conditions inside it must all be rewritten against `ViewerSessionCapabilities`. Porting the shape and keeping the inference by accident is the most likely way this goes wrong.
  Date/Author: 2026-09-01, plan author.

- Decision: take mjolnir's CSS geometry and none of its sizes or colours.
  Rationale: a faithful port would fail this plan's own Milestone 9 acceptance. Its largest interactive control is 42 pixels against hel's 44-by-44 bar, it has 94 font-size declarations at or below 12 pixels against five at 16, and its `--dim` is 4.24:1 on its own ground, below AA, while being the colour of nearly all that small text. Its accent is also semantically inverted: one signal red meaning live-or-selected, hard-coded as 68 untokenised literals, where hel's red means failure.
  Date/Author: 2026-09-01, plan author.

- Decision: the port needs no licence work. Both repositories are `GPL-3.0-only` under the same owner, verified in both `Cargo.toml` files, so this is attribution rather than a third-party dependency and nothing is added to `licenses/`. Separately, `licenses/SOURCE.md` still opens "Each official Mjolnir artifact…" and describes the mjolnir repository; that leftover should be corrected before it appears in a release.
  Rationale: recorded because the survey that produced these findings was briefed with an incorrect "Apache/MIT-ish" premise, and a later reader should not have to re-derive the real position.
  Date/Author: 2026-09-01, plan author.

Record every later decision here in the same shape, including decisions to abandon or reshape a milestone.

## Outcomes & Retrospective

Not started. Write an entry at the end of each milestone comparing what shipped against the purpose above, and a final entry when the plan completes.

## Context and Orientation

This section assumes you know nothing about this repository. Read it before touching any file.

### What the pieces are called

Hel is a Rust workspace. The library crate lives at the repository root in `src/` and is named `hel`; two more crates live under `crates/`. `crates/hel-cli` is the binary you run as `hel`: it holds the daemon, the terminal event loop and the phone server's control loop. `crates/hel-tui` holds the terminal surface's state machine and rendering.

A **target** is a place a session can run: a local bare directory, a local Podman container, an Apple container, an SSH host with or without Podman, or an AWS EC2 fleet. Target templates are configuration, defined in `HelConfig::targets` (`src/hel_config.rs`).

A **profile** is a configured agent harness with its own home directory and environment — for example a Codex profile or a Claude profile. Profiles are `HelConfig::profiles`.

A **bundle** is a named set of repositories to check out into a session. Bundles are `HelConfig::bundles`. A **bare** target instead opens one absolute project directory that you name.

A **session** is one agent conversation running against one target. Durable session records are `SessionRecord` in `src/hel_state.rs`, keyed by id inside `HelState::sessions`. `SessionState` (also `src/hel_state.rs`) is the lifecycle: `Provisioning`, `Running`, `Disconnected`, `Checkpointing`, `Closing`, `Destroying`, `Stopped`, `Lost`, `Error`, `DestroyedWithDataLoss`. `SessionState::is_active()` returns true for everything except `Stopped`, `Lost` and `DestroyedWithDataLoss`.

A **workspace** is a named grouping of sessions, stored in SQLite. `WorkspaceRecord` is in `src/hel_workspace.rs` and carries `id`, `name`, `created_at`, `last_opened_at`, `session_count`.

The **relay** is the process that speaks the Agent Client Protocol (ACP) to the agent and writes an append-only journal of observations. `RelayCommand` and `RelayOperationalState` are in `src/hel_worker/snapshot.rs`. A **materialized session** is the controller's durable projection of that journal; its `applied_event_ordinal` is the monotonic cursor everything else counts against.

The **daemon** is a long-lived process holding the controller, the session manager and the runtime state. It lives in `crates/hel-cli/src/daemon.rs`, and its shared handle type is `RuntimeState`.

The **phone server** is the HTTPS server this plan rebuilds. Its HTTP layer is `src/hel_server.rs` in the `hel` library; its control loop, which owns the data the HTTP layer publishes, is `run_server` in `crates/hel-cli/src/server.rs`. The two talk over channels only: the control loop pushes a `ViewerSnapshot` and a map of `BrowserTranscript` through `tokio::sync::watch`, and the HTTP layer pushes `ControllerRequest` and `ReadReceiptRequest` back through `tokio::sync::mpsc`. `src/hel_server.rs` deliberately contains no controller business logic, and this plan keeps it that way.

### How the phone server works today, file by file

`src/hel_server.rs` is about 2,845 lines. Roughly the first half is the HTTP layer and the public projection types; from `const VIEWER_HTML` at line 1406 to line 1494 is the entire browser application as one Rust raw string; the rest is tests.

The routes are built in `fn router`. Unauthenticated: `GET /` and `GET /login` both serve the viewer page, `GET /manifest.webmanifest`, `GET /service-worker.js`, `GET /icon.svg`, `POST /auth/session` and `DELETE /auth/session` (unlock and sign out), and `GET /auth/login?token=…` (the QR-code path). Authenticated, behind the `require_session` middleware: `GET /api/snapshot`, `GET /api/conversations/{session_id}` with an optional `after_seq` query, `POST /api/conversations/{session_id}/read`, `GET /api/events` (a Server-Sent Events stream that emits nothing but a revision number), and `POST /api/actions`.

Authentication is a signed cookie named `hel_viewer_session`, `HttpOnly; SameSite=Strict`, `Secure` unless the listener is loopback. `CodeGuard` rate-limits wrong codes with a doubling lockout capped at one hour.

The public projection is `ViewerSnapshot`, built by `ViewerSnapshot::from_config_state` from `HelConfig` and `HelState`. Its doc comment states the redaction contract, which this plan must preserve: it never copies profile homes or environment, SSH hosts or keys, container environment, AWS details, concrete resource locators, native session ids, or raw error strings. `ViewerSession` carries an id, workspace id, title, harness kind, profile/bundle/target ids, a state string, timestamps, a `has_error` flag, a four-line `preview`, queued prompts, active user shells, pending elicitations, a `conversation_available` flag, a `prompt_images_supported` flag, and `incompatible_resume_targets` — a list of target ids this session cannot resume onto, carrying only ids because the controller's reasons name paths and hosts.

`ControllerAction` is a serde-tagged enum with variants `New`, `Resume`, `Open`, `Prompt`, `RunShell`, `CancelShell`, `Close`, `Cancel`, `RemoveQueuedPrompt` and `RespondElicitation`. `fn validate_action` checks every field against the current snapshot before anything reaches the controller. `ActionOutcome` is the controller's answer: `Accepted`, `Busy`, `SessionBusy`, `NotCancellable`, `Failed`, each mapping to a fixed status and a fixed message, so controller error text never reaches a phone.

`crates/hel-cli/src/server.rs` holds the control loop. `fn viewer_snapshot` layers live per-session views on top of the durable projection: transcripts, queued prompts, active shells, pending elicitations, image support, and the four-line preview. `async fn apply_phone_action` executes one action, running the blocking parts on `spawn_blocking` and cancellable process work through `CancellableProcessExecutor`. At most `MAX_CONCURRENT_PHONE_ACTIONS` (4) actions run at once, and at most one per session. Cancellation is handled inside the loop rather than in `apply_phone_action`, because a `Cancel` has to reach the in-flight action's `PhoneActionControl` rather than start work of its own.

`BrowserTranscript` and `BrowserTranscriptEntry` are in `src/hel_chat/transcript.rs`. A `BrowserTranscriptEntry` has `id` (its first sequence number, a stable identity), `updated_seq`, `role` (one of `user`, `agent`, `thought`, `tool`, `plan`, `plan-proposal`, `system`), a `label`, an optional `recorded_at_ms`, and `lines: Vec<String>`. `fn browser_transcript` bounds the window to 1,000 lines and each line to 4 KiB, drops entries the provider compacted away, drops `raw_only` entries, and supports deltas: pass `after_seq` and you get only entries whose `updated_seq` is greater, or `reset: true` when the window has moved past your cursor.

### The terminal surface, which the web viewer must resemble

The combined terminal surface is described in `.agents/plans/combine-conversation-dashboard.md`. Its transcript styling is what the web must mirror, and it is defined in one place: `fn entry_visual` in `src/hel_chat/transcript.rs`. A user turn is cyan with the glyph `❯` and the label `You`. An agent turn is yellow with `●` and `Agent`. Thinking is dark grey, italic, with `○` and `Thinking`. A tool row takes its glyph, word and colour from `fn tool_presentation`: `•`/`waiting`/dark grey when pending, `●`/`running`/yellow, `✓`/`done`/green, `×`/`failed`/red, and its label reads `Tool · running`. A plan is magenta with `◇`; a proposed plan is light magenta with `◈`. A Hel system note is dark grey with `─` and the label `Hel`. Plan steps carry their own markers: `○` pending in dark grey, `●` running in yellow, `✓` completed in green.

Quota and capacity readings in the terminal colour by threshold, in `crates/hel-tui/src/render.rs`: 0–20 red, 21–50 yellow, above 50 green, where the number is headroom remaining for quota and is inverted for target CPU.

The terminal's slash commands are parsed in `src/hel_chat/autocomplete.rs` by `fn parse_local_command`, which recognises `help`, `detach`, `model`, `effort`, `fast`, `plan` and `implement`, and are executed in `fn submit_input` in `src/hel_chat.rs`. Anything starting with `!` is a shell command. `/model` and `/effort` take a value and become a `SetConfig` relay command that queues behind a busy agent. `/fast` toggles, and refuses when the active Codex model does not support it. `/plan` is only available while the agent is idle and goes through `plan_control`, which knows each harness's mode ids. Commands the agent itself advertises are listed by `/help` with an `[agent]` marker and are sent as ordinary prompts. The values `/model` and `/effort` offer come from `session_config_choices` in `src/hel_acp.rs`, reading whatever the harness advertised in `RelayOperationalState::config_options`.

Prompt history lives in SQLite. `search_prompts(session_id, bundle_id, scope, query)` in `src/hel_database.rs` searches with `HistoryScope::Session`, `HistoryScope::Project` or `HistoryScope::All`, newest first, de-duplicated by text.

### State and storage

State is SQLite at `hel_database::database_path()`, which is `data_dir().join("hel.sqlite3")`. The schema is created and migrated in `src/hel_database/schema.rs`; there is no separate migration directory, and each additive change is its own `fn ensure_…` or `fn migrate_…` called from `fn migrate_schema`. Only the daemon writes: `submit_database_write` funnels writes to the daemon's writer, and client processes open the file read-only through `open_reader_strict`.

Two tables matter here. `client_read_frontiers(client_id, workspace_id, session_id, through_event_ordinal, updated_at)` records how far one client has read one session; `persist_read_receipt` writes it and also advances `sessions.viewed_through_event_ordinal`. `detached_drafts(draft_id, workspace_id, session_id, source, owner_pid, saved_at, text, recovered_at)` already exists for terminal drafts, and `sessions.draft_input` holds the terminal's own in-progress composer text. Neither is keyed by a web viewer, which is why Milestone 8 adds a table rather than reusing one.

### The mjolnir viewer: what it supplies, and what it cannot

A sibling repository at `../mjolnir` — same owner, same `GPL-3.0-only` licence, verified in both `Cargo.toml` files — contains a production phone web viewer. It is `mj-remote/src/remote_viewer.html`, 10,254 lines of HTML, CSS and JavaScript in one file, served by `mj-remote/src/remote.rs`, 20,652 lines of axum routes and record types. Hel's own `src/fonts/` and `src/icons/` are copies of `mj-remote/src/fonts/` and `mj-remote/src/icons/`, which is why they sat in the tree unreferenced until Milestone 1 wired them up.

Read this section before porting anything, because the temptation is to treat mjolnir as a template for the whole viewer, and it is not one.

**What mjolnir does not have.** It has a flat session list and a conversation. It has no workspaces — the string `workspace_id` appears zero times in both files. It has no targets and no capacity: zero occurrences of `capacity`, `cpu_percent`, `memory_used`, `logical_cores` or `fleet`. Its quota is a `Vec<String>` of vendor text on one wrapping line in the chat header, with no percentage, window label, reset time or exhaustion projection. It has no read frontiers and no unread state: zero occurrences of `unread`, `read_frontier` or `latest_event_ordinal`. Its drafts are an in-memory `Map` that dies on reload. Its new-session request is, in full, `struct NewServerSessionRequest { cwd: String, worktree: bool }` — two fields, one modal, no profile, no target, no bundle, no dirty-worktree acknowledgement, and no way to cancel a launch. And its session cookie is `mac.update(exp.to_string())` returning `format!("{exp}.{sig}")`, structurally identical to hel's, so it never solved the viewer-identity problem either.

So Milestones 3, 5, 6 and 8 are hel's own work almost entirely, and Milestone 4 splits: the row-reconciling engine ports, while the router, the project grouping, the unread marker and the workspace tab strip are new.

**What mjolnir does supply** is presentation, and it is worth having. The largest single gift is the transcript renderer, `remote_viewer.html` lines 6366 to 7518 — 1,153 lines that reference no `window`, no `fetch`, no timer and no application-state global, so they lift as a unit. Inside it are things no Markdown library provides and hel would otherwise invent: fourteen language families tinted by one cached regex per family with a size bail-out, folding a five-thousand-line tool dump behind a `<details>` that builds its content only on first open, JSON reparsed and tinted, shell commands tokenised into program, subcommand, flag and path, and conservative language sniffing for unfenced tool output that only wins when its score doubles the runner-up. Beyond the renderer: the keyed create-once/patch-many row engine, the transcript reconciler that records and restores which folds the reader had opened, the pinned-scroll rule and its jump-to-latest control, the slash-command palette, the keyboard-inset calculation, the safe-area composition, and the reconnect and 401 handling.

**The load-bearing difference is the data model.** mjolnir's viewer reads its own server's records — `SessionRecord`, `TranscriptEntry`, `RuntimeActivityRecord`, `PendingPermissionRecord`. Hel's viewer reads `ViewerSnapshot`, `BrowserTranscript` and `ControllerAction` over `/api/snapshot`, `/api/events` and `/api/actions`. Nothing about the data access ports. What ports is the DOM building and the phone interaction, rewired onto hel's projection.

There is also a rhythm difference with consequences. mjolnir polls every two seconds and ships every session's entire transcript inside every poll, up to four mebibytes each; hel streams revisions over Server-Sent Events and keeps transcripts on a separate route. So anything in mjolnir that reads `session.transcript` from the list payload cannot come across, and hel faces a burst problem mjolnir never had — which makes revision coalescing and abort guards matter more here, not less.

### Porting rules

These are not suggestions. Each one is a defect that a faithful port would introduce, and each was found by reading mjolnir's source rather than by imagining what might go wrong.

Rewrite every legality check. Every renderer mjolnir has infers what a person may do from the data: `updateSessionCard` picks its action with `archived ? 'load' : session.web_owned ? 'archive' : 'terminal'`, `renderChat` gates read-only on `!selectedSessionIsLive()`, and the composer's enabled state is `Boolean(session)`. Hel's Decision Log forbids exactly this. The DOM code lifts; every condition inside it must be rewritten to read `ViewerSessionCapabilities`. Porting the shape and keeping the inference by accident is the easiest mistake available here.

Replace every error string before the handler runs. mjolnir publishes what hel's redaction contract forbids: `SessionStatusRecord.cwd`, path-derived `project` and `worktree`, a `GET /api/filesystem` directory browser returning absolute paths, `ServerSessionLaunchState::Failed { error }` carrying agent startup text verbatim, and error arms embedding paths. Hel's redaction test will catch a slip; mjolnir has no such test, so nothing caught it there.

Re-derive every size and colour. A faithful CSS port would fail hel's own Milestone 9 acceptance. mjolnir's largest interactive control is 42 pixels and its common ones are 24 to 36, against hel's 44-by-44 requirement. It has 94 font-size declarations at or below 12 pixels against five at 16. Its `--dim` computes to 4.24:1 on its own dark ground, below AA, and it is the colour of nearly all that small text. Worse, its accent is a single red meaning live-or-selected-or-needs-you, hard-coded as 68 untokenised `rgb(235 63 51 / …)` literals; in hel red means failure, so a straight port makes the focus ring, the primary button, the caret and every link read as an error. Take the geometry — the `min-width: 0` discipline at 29 sites, `overscroll-behavior: contain` on internal scrollers, `overflow-x` containment, the safe-area composition — and re-derive the rest.

Key the transcript by identity, not by position. mjolnir's `TranscriptEntry` has no id and no sequence number, so `renderTranscript` matches by position on `(kind, timestamp)` and compares a computed signature. Hel's `BrowserTranscriptEntry` already carries a stable `id` and an `updated_seq`. Key by id and rewrite a body only when `updated_seq` moves; porting the positional match would be a deliberate downgrade to work around a problem hel does not have.

Take the elicitation card's presentation and none of its transport. mjolnir encodes a whole answer into an opaque option-id string, `elicitation:accept:{json}`, because it reuses one permission queue for two shapes, and it assigns `link.href = elicitation.url` with no scheme check at all. Hel already has a typed `RespondElicitation` with schema validation, a 64 KiB cap and an unknown-id 404, and a richer field model. What is worth taking is one detail: the request text renders as a scrollable `<pre>` capped at 30dvh, so no approval is ever granted over hidden text.

Do not port the diff subsystem without deciding to feed it. Roughly 400 lines — `lineDiff`, `intraLineSegments`, `emphasizeReplacements`, `compactContext`, `diffDisplayRows`, `buildDiffView` — compute a diff in the browser from `old_text` and `new_text`. Hel publishes neither: `format_diffstat` in `src/hel_chat/transcript.rs` reads `diff.old_text` and `diff.new_text`, counts the changed lines, and discards the text. Milestone 3 must decide explicitly whether to project it.

Do not inherit two known defects. `draftBySessionId` is not cleared in mjolnir's sign-out teardown, so a draft survives a sign-out in the same tab. And `refreshSessions`, its main data path, has neither an in-flight flag nor a generation guard, so two overlapping polls can land out of order and the older one wins; only `loadFilesystem` has that discipline. Copy `loadFilesystem`'s guards onto hel's fetches, not `refreshSessions`'.

Treat every borrowed piece as CSP-plausible rather than CSP-proven. mjolnir ships no Content-Security-Policy at all — zero matches for `content-security` in `remote.rs`. Its no-`innerHTML` discipline is real and it holds perfectly: the string appears exactly once in 10,254 lines, in the comment saying it is never used, with zero `style="` attributes and two CSSOM `setProperty` calls. But no browser has ever enforced it. Hel's policy is enforced, so every port needs a browser to confirm it.

### Testing conventions

Unit tests are colocated in `#[cfg(test)] mod tests` blocks next to the code. `src/hel_server.rs` already builds an in-process `Router` in its tests and drives it with `tower::ServiceExt::oneshot`. It also already runs the browser JavaScript under Node: two tests slice the source out of `VIEWER_HTML` with `str::find`, prepend a hand-written fake DOM, append assertions, and run the result with `std::process::Command::new("node").args(["--input-type=module", "--eval"])`. Node 24 is present on this machine. Milestone 1 keeps this technique and removes the slicing.

The browser end-to-end suite is Playwright, at `tests/e2e/web/`, driven by `tests/e2e/browser_lab.py` through `tests/e2e/run-browser-reliability.sh`. The lab starts a real daemon with a fake ACP harness and no container, opens a terminal client and a browser against the same daemon, and asserts they converge.

Run tests outside the restricted sandbox with elevated permissions: the suite binds loopback TCP and Unix sockets, and a sandboxed run fails with `EPERM` or hangs. `.cargo/config.toml` defaults the build target to `x86_64-unknown-linux-musl`, so binaries land in `target/x86_64-unknown-linux-musl/debug/`.

## Plan of Work

Nine milestones. Milestones 1 to 3 are foundation; 4 to 6 build the dashboard and the global pages; 7 and 8 build the conversation; 9 hardens the whole thing. Each one leaves the tree building, tested and committable, and each one can be demonstrated on its own.

### Milestone 1 — Move the browser application out of the Rust string constants

Today the entire browser application is four Rust string constants in `src/hel_server.rs`: `MANIFEST`, `SERVICE_WORKER`, `ICON` and `VIEWER_HTML`. `VIEWER_HTML` is one line of HTML, one line of CSS, and eighty lines of minified JavaScript, all inside a single `<script>` tag. Nothing can forbid inline script while that is true, so the page has no content-security policy at all, and the tests that exercise the JavaScript have to slice it out of the Rust source by searching for function names.

Create a directory `src/web/` holding `viewer.html`, `viewer.css`, `viewer.js`, `service-worker.js` and `manifest.webmanifest`. Move the existing markup, styles and script into them unchanged apart from reformatting: this milestone deliberately changes no behaviour, so that any behavioural regression later in the plan cannot be blamed on the move. Reformat the JavaScript into readable multi-line source while you are there; it is minified today only because it lived in a string literal.

In `src/hel_server.rs`, replace the constants with `include_str!("web/viewer.html")` and siblings, add routes `GET /viewer.css` and `GET /viewer.js`, and serve `GET /icon.svg`, `GET /icon-192.png`, `GET /icon-512.png`, `GET /maskable-512.png` and `GET /apple-touch-icon.png` from the already-vendored files in `src/icons/` using `include_bytes!`. Serve `GET /fonts/jetbrains-mono.woff2` from `src/fonts/jetbrains-mono.woff2` the same way. Point the manifest at the real PNG icons, which is what a phone's home-screen installer wants, and keep the SVG for the browser tab.

Give every response a set of security headers, applied by one `axum` middleware layer so no route can forget them. The policy is `default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; font-src 'self'; connect-src 'self'; manifest-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'`. `img-src` needs `data:` because attached images are rendered from data URLs the browser itself just built. Add `X-Content-Type-Options: nosniff` and keep the existing `Referrer-Policy: no-referrer`. Static assets other than the HTML shell may be cached; give them `Cache-Control: no-cache` so the browser revalidates, and keep `no-store` on the shell and on every `/api/` response.

Rewrite `service-worker.js` so its cache name embeds a version constant, its `activate` handler deletes every cache whose name is not the current one, and its `fetch` handler leaves anything under `/api/` entirely alone — no `respondWith` at all, so those requests never touch the cache machinery. Navigations go to the network first and fall back to the cached shell only when the network fails, so a phone that has been offline does not keep serving an old application after an upgrade.

Move the two Node-driven JavaScript tests so they read `src/web/viewer.js` with `include_str!` instead of slicing `VIEWER_HTML`, and change them from `--eval` on a concatenated string to writing the fake DOM, the module source and the assertions into a temporary directory and running Node against it. That makes stack traces point at real line numbers.

You will know this milestone worked when `curl -sI https://<viewer>/` shows the `Content-Security-Policy` header, the browser console reports no CSP violations, `cargo test` passes with the relocated JavaScript tests, and the Playwright reliability suite still passes end to end. Nothing the user sees should have changed.

### Milestone 2 — The visual language, and text rendering that cannot inject markup

With the assets in real files, give the viewer the terminal's look and a renderer that agent output cannot escape from. This milestone deliberately stops short of the transcript itself: hel's `BrowserTranscriptEntry` is still a flat `lines: Vec<String>`, so a transcript ported now would render less than mjolnir's does, not more. The transcript chrome waits for the enriched projection and lands in Milestone 7.

Write `src/web/viewer.css` as a token layer plus components. Declare `@font-face` for JetBrains Mono against `/fonts/jetbrains-mono.woff2` with `font-display: swap` and a real monospace fallback stack, and use it throughout: the terminal surface is monospace, and matching it is most of the resemblance. Define custom properties for the palette so each semantic name appears once, and map the terminal's colours — cyan for the user role and for focus, yellow for the agent role and for running work, green for success and healthy headroom, red for failure and exhausted headroom, magenta for plans, dim grey for thinking and system notes. Keep flat bordered surfaces with restrained radii rather than the current fourteen-pixel cards, and give every focusable element a visible focus border, because the browser default is invisible against a dark ground on some phones.

The palette is written here, not ported. mjolnir's stylesheet header states its own rule — "one signal red", meaning live, selected or needs-you — and hel's red means failure. Its 68 hard-coded `rgb(235 63 51 / …)` literals were never tokenised, so there is nothing to re-key even if the meaning matched. Take mjolnir's structure and none of its values.

The Markdown core already exists. `src/web/markdown.js` was written before this survey and is untracked: 450 lines exporting `safeHref`, `renderInline`, `renderMarkdown` and `renderDiffSummary`, referenced by nothing. Its block and inline layers are better than mjolnir's — it has blockquotes, horizontal rules, nested lists, `mailto:`, table alignment as classes rather than forbidden style attributes, and inline markup inside link labels. Commit it and wire it in as the first act of this milestone. One thing in it is wrong and must be fixed: `renderDiffSummary` splits on `|`, but `format_diffstat` in `src/hel_chat/transcript.rs` emits `path` followed by two spaces, `+12`, a space, and `−3` with a Unicode MINUS SIGN, U+2212 — no pipe anywhere, so the split never fires and every diffstat lands whole in the path element.

Above that core, port mjolnir's tool-output layer from `remote_viewer.html` lines 6366 to 7518. It references no `window`, no `fetch`, no timer and no application-state global, so it lifts as a unit. Take `appendCodeTokens` with `CODE_LANG_FAMILY`, `CODE_FAMILY_SYNTAX` and `codeTintRegexCache` — fourteen language families as a seventeen-line data table, one cached regex each, and a 120,000-character bail-out. Take `foldBlock`, which builds a `<details>` whose content is constructed on first toggle, so a five-thousand-line payload costs nothing until someone opens it. Take `codeBlock`, `jsonBlock`, `appendJsonTokens` and `tryParseJson` for fence routing and JSON reparsing. Take `shellTokens`, `appendCommandTokens`, `appendCommandInline` and `isPathLike`, whose test distinguishes `src/**/*.rs` from the prose word "and/or". Take `detectLang` with `LANG_FINGERPRINTS`, which sniffs unfenced tool output and only wins at a score of five or more that also doubles the runner-up, so logs stay untinted. Take `appendMonoText` and `renderRichText` with its line-offset fence splitter that never copies the source twice and survives an unterminated fence. Take the CSS for `pre.code-block` with its `[data-lang]::before`, `details.block-fold` with its marker-suppression pair, `.meta-line`, `code.path`, the `.table-scroll` wrapper, and the `prefers-reduced-motion` block that covers the fold triangle.

The hard rule stands and is what makes the port safe: agent-controlled content never reaches `innerHTML`, `outerHTML`, `insertAdjacentHTML`, `document.write`, or an `href` or `src` attribute that has not been scheme-checked. mjolnir holds this line perfectly — `innerHTML` appears exactly once in its 10,254 lines, inside the comment saying it is never used — but no browser has ever enforced it there, because mjolnir ships no policy. Add a test that greps `src/web/*.js` for those sinks and fails if one appears outside an allowlist, so a later contributor cannot reintroduce one quietly.

Two things mjolnir cannot supply, because it has no concept of them. It labels a transcript entry with a word only: `entryLabel()` returns "User", "Agent", "Thought", and there is no glyph slot in its entry template. Hel needs `❯ ● ○ ◇ ◈ ─` plus the four tool glyphs in a fixed-width `aria-hidden` span. And it has one violet `--tool` where hel needs four distinct pending, running, done and failed states, and no plan concept at all, so the plan and proposed-plan roles and their three step markers are written here.

You will know this milestone worked when the viewer renders in the terminal's colours and glyphs; when a transcript entry whose text is `<img src=x onerror=alert(1)>` displays those characters literally and creates no element; when `[click](javascript:alert(1))` renders as text and `[click](https://example.com)` renders as a link; when a fenced Rust block is tinted and a five-thousand-line tool dump is one closed fold; and when the sink test fails on a deliberately introduced `innerHTML` and passes once it is removed.

### Milestone 3 — Server-side projections, capabilities and structured readings

This milestone changes no pixels. It gives the browser the facts it needs, and it is where the redaction contract is enforced.

Extend `ViewerSession` in `src/hel_server.rs` with: a sanitized project identity, being the leaf name `SessionRecord::project_name(&config)` already computes plus a stable grouping key derived from `project_source`, hashed rather than sent as a path; a lifecycle category that collapses `SessionState` into the small set the phone reasons about (`live`, `starting`, `stopping`, `stopped`, `failed`) alongside the existing precise state string; the session's `applied_event_ordinal` as `latest_event_ordinal`, so the browser can compute unread without fetching a transcript; an optional current operation; the chat phase and the current configuration the agent advertises; the list of target ids this session *can* resume onto, computed by inverting the existing `incompatible_resume_targets` so the browser never has to subtract sets; and an explicit `ViewerSessionCapabilities`.

`ViewerSessionCapabilities` is a struct of booleans, one per action the phone can offer: open, prompt, run shell, cancel the active turn, cancel the running lifecycle operation, stop, rename, resume, change configuration, and change plan mode. The control loop fills it from what it already knows — the durable `SessionState`, whether the session manager is managing the session, whether a lifecycle operation is running, whether the relay reports an active prompt, and what the harness advertised — and the browser renders buttons from it and nothing else.

Add a `ViewerOperation` describing an in-flight lifecycle operation: an operation id, the session it belongs to, its kind (`create`, `resume`, `stop`, `checkpoint`), when it started, the stages currently active with their start times, an optional notice, and whether it can be cancelled. Fill it from `RuntimeLifecycleView`, which the daemon already keeps. Add a small synchronous accessor to `RuntimeState` — `pub(crate) fn active_lifecycles(&self) -> Vec<RuntimeLifecycleView>` — that reads the existing mutex without going through the async `runtime_snapshot`, because the phone control loop needs it on every publish and must not await inside its snapshot builder.

Replace `ViewerQuota`'s single `summary: String` with structured windows. A `ViewerQuotaWindow` carries a label, the percent used, an optional reset time, and whether it projects exhaustion before that reset — all of which `ProfileQuota` and `QuotaWindow` in `src/hel_quota.rs` already compute. Keep `stale` and `has_error`, keep the last refresh time, and keep the rule that raw vendor error text stays on the controller: the phone gets a category, not a message.

Add `ViewerTargetCapacity`: a host or fleet label, the target ids it serves, an optional CPU percent, used and total memory in bytes, logical cores, optional total disk in bytes, a virtual-machine count for fleets, the sample time, and flags for refreshing, stale and errored. These map one-to-one onto `DeploymentCapacityUsage` and `CapacityDetail`, which already exist.

Enrich `BrowserTranscriptEntry` in `src/hel_chat/transcript.rs` with a semantic kind and status rather than only a role string and a pre-formatted label, an explicit timestamp, an optional request identity so a tool row can be matched to the turn that caused it, and structured tool metadata: the tool name, its status, and its diff summaries as data rather than as extra lines appended to `lines`. Keep `lines` populated for compatibility during the rollout; a cached older shell must keep working while a new server is deployed.

Expand `ControllerAction` with `Rename { session_id, title }`, `CancelTurn { session_id }`, `SetConfig { session_id, key, value }`, `SetPlanMode { session_id, active }`, `RefreshQuota { profile_id }` and `RefreshCapacity { target_id }`. Give `New` an explicit `workspace_id` (it exists but is optional today, which is why multi-workspace creation fails), an optional `title`, and a `dirty_ack` value that must name the repositories the phone was shown, so an acknowledgement cannot be replayed against a different set of dirty repositories. Extend `fn validate_action` for each, keeping the existing shape: validate ids, look the referenced things up in the current snapshot, and refuse anything the snapshot does not contain.

Extend `apply_phone_action` in `crates/hel-cli/src/server.rs` to execute them, reusing what exists: rename through `Controller::rename_session` and the daemon's `set_session_title`; cancel turn by submitting `RelayCommand::Cancel`; configuration by `RelayCommand::SetConfig`; plan mode by the same `plan_control` decision the terminal uses, which must be called rather than reimplemented; quota refresh by pushing the profile at the existing quota refresher; capacity refresh by nudging the capacity poller Milestone 6 adds.

The Rust tests for this milestone are behavioural, not structural. Prove that a projection of a session whose profile home is `/highly/secret/codex`, whose container environment holds a token, and whose `last_error` contains both, produces JSON containing none of those strings — extend the existing redaction test rather than writing a parallel one. Prove that a `new` action naming workspace B creates in workspace B when three workspaces exist. Prove capabilities: a provisioning session offers cancel and not stop; a running session with an active prompt offers cancel-turn; a stopped session offers resume and not prompt. Prove resume compatibility: a target listed as incompatible is absent from the compatible list and is refused by `validate_action`. Prove that a `set-config` for a key the harness never advertised is refused. Prove that capacity and quota round-trip their structured states including refreshing, stale and errored. Prove that an operation with two active stages projects both.

Six corrections come from reading mjolnir and hel's own source side by side. Carry the entry glyph and colour name in the projection rather than deferring them: `fn entry_visual` in `src/hel_chat/transcript.rs` already returns a `glyph: &'static str`, so this is extraction rather than invention, and it is the only thing that stops the two surfaces drifting. Project the diffstat as structured data rather than a formatted string — `format_diffstat` emits `path`, two spaces, `+12`, a space, and `−3` with a Unicode MINUS SIGN, and re-parsing that in JavaScript is how the current `renderDiffSummary` came to be wrong. Decide explicitly whether to project `diff.old_text` and `diff.new_text` as well: they exist at the point `format_diffstat` counts them and are then discarded, and roughly 400 lines of mjolnir's diff renderer are out of scope unless hel feeds them. Give `ViewerTargetCapacity` a non-optional sample time whenever a reading is present. Do not add `active_lifecycles` to `RuntimeState` before checking whether it is needed: a `watch::Receiver<Vec<RuntimeLifecycleView>>` already exists in `crates/hel-cli/src/pollers.rs` and is consumed in `crates/hel-cli/src/dashboard.rs`, and subscribing the phone control loop to that channel is better than adding a second path to the same mutex. And have `search_prompts_bounded` return whether it truncated.

Three of mjolnir's server-side types are near-exact prebuilds and should be read before writing hel's. `SessionConfigOptionRecord` with `config_option_records` and `select_choice_records` is `ViewerConfigOption` already built, including two edge cases hel will meet: skip any option kind that is not a select, so the viewer never shows a control it cannot drive, and flatten the Agent Client Protocol's *grouped* selects into `"{group} / {name}"` labels. `is_currently_editable_config_target` is literally hel's `a_config_key_the_harness_never_advertised_is_refused` acceptance test, already implemented. And `CommandRecord` with `available_command_records` carries the source discriminator `/help` needs, plus three merge guards: reserved-name shadowing, whitespace in names, and case-insensitive de-duplication.

One shape must not be ported. `NativeModeRecord`'s doc comment says it is intentionally status-only and that mjolnir never changes it, and `config_option_records` actively filters the Mode category out of the editable list. Hel's `/plan` and `/implement` must drive plan mode through `plan_control`. So that code is a good template for `model` and `effort` and precisely the wrong shape for `plan`; porting it wholesale would build a control that reports the mode and refuses to change it.

### Milestone 4 — The workspace dashboard

Now the browser gets its new shape. From this milestone on, the application is a router over pages rather than two `hidden` sections.

Put navigation in the URL fragment so Back, Forward, reload and shared links all work: `#workspace/{id}` for a workspace dashboard, `#workspace/{id}/new` for the new-session wizard, `#workspace/{id}/resume` for resumable sessions, `#conversation/{id}` for a conversation, and `#targets` and `#quota` for the two global pages. Keep accepting the bare `#conversation/{id}` form the current viewer writes, because a cached shell may still produce it. Write one router that parses the fragment into a route value, one renderer per route, and one `popstate`/`hashchange` handler; never let a page mutate the URL except through the router, or Back stops being predictable.

Restore the route after authentication, not before it. The existing `restoreRoute` already gets this right — it refreshes the snapshot first and only then reads the hash — and the Playwright suite already asserts that visiting `#conversation/not-authenticated` while signed out shows the login page and raises no page error. Keep both properties.

Render the workspaces from `ViewerSnapshot::workspaces` as a horizontal, scrollable tab strip with the selected tab marked by `aria-current="page"`. Persist the selection in the URL; when the fragment names no workspace, pick the one the daemon reports as most recently opened. Long workspace names must not push the layout wide: let the strip scroll horizontally inside its own container while the page body never does.

Show only live sessions. A live session is one whose lifecycle category is `live`, `starting` or `stopping`; stopped, lost and destroyed sessions belong to the Resume flow, not the dashboard. Group the rows by the sanitized project label, with the group's session count beside its name. Each row shows the session title, its state with both an icon and a word (never colour alone), an attention marker when the session has an error or a pending elicitation, an unread marker derived from comparing the session's `latest_event_ordinal` against this viewer's read frontier, the queue depth when it is non-zero, the current operation and its stage when one is running, the target and profile, and the latest user and agent activity as one truncated line each. No paths, no hosts, no container ids, no native session ids.

Put Targets, Quota and Sign out behind a menu button in the header, opened as a menu with proper roles and keyboard support and closed on Escape or an outside tap. They are routes, not cards; nothing about capacity or quota appears on the dashboard.

Render every row action from `ViewerSessionCapabilities`. Rename opens a small inline editor bounded by the server's 120-character limit. Stop asks for confirmation and explains what stopping does, in the same words the current viewer uses. Cancel appears when a lifecycle operation is cancellable, and cancels that operation by id. All of them post to `/api/actions` and rely on the snapshot for the result, rather than optimistically rewriting the list.

You will know this milestone worked when two workspaces appear as two tabs; when switching tabs changes the URL and the session list; when reloading returns to the same tab; when Back from a conversation returns to the dashboard rather than leaving the app; when a stopped session is absent from the list; and when a session that has no rename capability shows no rename button.

Three corrections and one gift come from mjolnir. The gift is the row engine, and it is the part most easily got subtly wrong: `renderSessions` at `remote_viewer.html:6250-6304` reconciles keyed rows and re-appends only when the joined id order actually differs, with the comment recording why — re-inserting nodes restarts their CSS animations. `createSessionCard` and `updateSessionCard` split creation from patching, and the updater is nothing but `textContent`, `classList.toggle`, `hidden` and `disabled` writes, with no queries and no node creation. Rows clone a `<template>`, which keeps the markup reviewable in HTML and is CSP-clean. `runSessionCardAction` keeps a pending-action `Set` checked at entry, added before the await and deleted in a `finally`, so the disabled state is derived rather than written imperatively. `setTimestamp` and `refreshTimestamps` put a `data-ts` on the row and sweep the document once, so "2 minutes ago" ages without re-rendering anything, with a "just now" band under 45 seconds to stop flicker. And `showAuth`'s teardown clears every keyed map, pending set and timer on sign-out, so a re-login cannot resurrect a previous viewer's rows. Take all of it, and rewrite every action condition to read `ViewerSessionCapabilities` rather than infer from the record, which is what mjolnir's updater does.

The corrections: mjolnir's router understands two routes from one regex and resets per-session state only on a session-to-session change, so going back to the list leaves stale state — the opposite of what hel needs from six routes, two of them nested. Its list is flat, and `renderSessions` and `renderHistorySessions` are two hand-copied loops over two fixed containers with nothing that generalises to N project groups. And it has no `aria-current` anywhere, no `role="list"` semantics, and no `.focus()` call on the navigation path at all. The router, the grouping, the unread marker, the workspace tab strip, and every accessibility affordance are written here.

### Milestone 5 — Guided New and Resume

Both flows are full-screen pages, not modal dialogs on top of the list, because a phone keyboard covering a modal is how the current form becomes unusable.

New walks profile, then target, then project — a bundle, or an absolute directory when the chosen target is bare — then a dirty-worktree confirmation when one is needed, then a review step that names every choice before it commits. Derive the title as the terminal does and show it on the review step; renaming later is a separate action, already built in Milestone 3.

Add `POST /api/preflight/new`. It takes the same fields as a `new` action and answers with a sanitized validation result: whether the combination is legal, and, when the bundle has uncommitted local changes, the list of dirty repositories as leaf names and short summaries with no absolute paths. The controller's own check, `dirty_local_repositories` reached through `Controller::register_session_with_resources`, produces full paths; the preflight endpoint reduces them. The phone then posts `new` carrying `dirty_ack` naming exactly those repositories, and the server refuses the action if the set has changed since the preflight — a stale acknowledgement must not authorise a launch over changes the person never saw.

Resume walks a resumable Hel-owned session, then a profile, then a target restricted to the compatible list the projection now carries, then keep-or-discard for queued prompts as two labelled buttons rather than a typed word, then review. Resumable means Hel owns the session and it is not live: `Stopped`, `Lost` or `Error`. Sessions that need a native import, a missing-repository repair, or an advanced override are listed but not startable, and each says in one plain sentence why, and that the terminal can finish it.

Both flows post their commit and receive `202 Accepted` with an operation id. The page then follows that operation in the snapshot, showing its stage, offering cancellation while the operation reports it is cancellable, and moving to the new conversation when the session becomes live. A failure surfaces as a stable safe code and message; the controller's own text stays on the controller.

You will know this milestone worked when a bare target's flow asks for a directory and a container target's flow does not; when a bundle with a dirty repository stops the flow with a named confirmation and proceeds after it; when the same acknowledgement replayed after a different repository goes dirty is refused; when a resume onto an incompatible target is not offered and is refused if forged; and when cancelling mid-provision leaves no half-created session.

mjolnir helps least here in code and most in judgement. Its entire new-session request is `struct NewServerSessionRequest { cwd: String, worktree: bool }` — two fields, presented as a modal with a filesystem browser inside, which this plan explicitly rejects. It has no profiles, no targets, no bundles, no dirty-worktree acknowledgement, and `ServerSessionManager` has no cancel method at all, so a launch can be polled but never stopped. Both wizards are written from nothing, and this is the largest genuinely-new client work in the plan; the survey's enthusiasm elsewhere must not make it look cheaper than it is.

What is worth taking is the sequence and the discipline. `create_server_owned_session` checks its preconditions, does the expensive re-resolution, then checks them *again* before committing — which is exactly the shape the dirty-worktree acknowledgement needs, and the reason `dirty_ack` must name the repositories rather than being a bare boolean. `unarchive_session` layers five refusals, each with its own status and one plain sentence telling the person what to do, ending in `202` plus a launch id. `archive_server_owned_session` refuses and points the person at the terminal, which is this plan's scope rule made concrete. `directory_under_roots` applies its containment check *after* canonicalisation, which is what stops `/allowed/../etc`. And the launch watcher carries a sequence number that retires a superseded watcher quietly, releases the interface lock before waiting rather than holding a modal hostage through a two-minute provision, and fails visibly with a dismissible card rather than a silent timeout.

### Milestone 6 — Targets and Quota as global pages

Spawn `spawn_dashboard_capacity_poller` in the phone control loop in `crates/hel-cli/src/server.rs`, exactly as `spawn_quota_refresher` already is. Feed it `controller.deployment_capacity_targets()` on every controller reload, receive `CapacityPollUpdate` in the existing `tokio::select!`, keep the last good reading beside any probe error the way `CapacityDetail` does, and publish the result as the structured `ViewerTargetCapacity` from Milestone 3. Nothing about this may block the loop: the poller already runs its probes on their own tasks with a timeout, which is why it is the right thing to reuse.

The Targets page lists one entry per host or fleet with its target ids and its readings: CPU percent and memory percent for a host, virtual-machine count, cores, memory and disk for a fleet. It has four honest states. Loading is the first sample after start. Refreshing is a sample in flight over a previous reading, which stays visible. Stale is a reading older than the sample interval allows, labelled with its age. Errored keeps the last reading and says the probe failed, without repeating the probe's own message. A manual refresh posts `refresh-capacity`.

The Quota page groups by profile and account, and for each shows every window the harness reports with its label, a progress bar, the percent used, the reset time, the last refresh time, and stale or error state. Colour follows the terminal's thresholds and is always paired with a number, so colour is never the only signal. A manual refresh posts `refresh-quota`.

You will know this milestone worked when both pages are reachable from the menu and by URL; when a target whose probe is failing shows its last reading and an error state rather than a blank; when a quota that has not refreshed within its interval reads stale; and when a manual refresh visibly moves the last-refresh time.

mjolnir has no capacity concept whatsoever — zero occurrences of `capacity`, `cpu_percent`, `memory_used`, `logical_cores` or `fleet` — and its quota is a `Vec<String>` of vendor text on one wrapping line. Both pages are written here. The one real gift is the freshness design, which is the part most implementations get wrong: `WorkspaceHeadDiffRecord` makes its sample time non-optional and says why in a doc comment — a pulled view delivered by push, so without its age the viewer cannot tell a current answer from a stale one — keeps `unavailable` as a state distinct from a zero reading, and never persists a live reading. `ViewerTargetCapacity` should follow it: whenever a reading is present its sample time is present too, because an optional sample time makes the stale state unrenderable in exactly the case that matters. Take three more shapes: `runtime_stall_seconds`, where the server publishes the *threshold* alongside the data and `0` disables the warning; `.status-field[data-role=…]`, where a server-named role drives the colour from CSS, which is the drift-proof form of hel's threshold colouring; and a status dictionary in which an unrecognised status from a newer server degrades to a bullet plus the raw word and a neutral sentence, rather than vanishing. Also take `reviewIssuesPaintKey`: skip the rebuild entirely when a poll changed nothing, so the reader's scroll position and text selection survive.

### Milestone 7 — The conversation screen

A conversation is a full-height screen: a back button that returns to the session's workspace dashboard, a compact header naming the session and its state, a transcript that scrolls inside itself, and a composer stuck to the bottom that respects the safe-area inset and the on-screen keyboard.

Render the transcript from the enriched entries. Roles get their glyphs, labels and colours. Timestamps render from `recorded_at_ms` in the phone's own locale. Bodies render through the Markdown renderer. Tool entries show their name and state, with their diff summaries as structured rows and their detail collapsed behind a disclosure that is closed by default. Thinking is collapsed the same way. Plans render their steps with the three markers. Elicitations render as forms. Image messages render as images. Raw terminal and tool streams are excluded, exactly as `browser_transcript` already excludes `raw_only` entries — the phone mirrors the terminal's Rich feed, not its Raw feed.

Update the transcript with keyed DOM updates. Each entry keeps its element, identified by the entry's stable `id`, and an update rewrites only what changed. This already half-exists in `renderEntries`, which reuses nodes by `data-entry-id`; the problem is that it looks them up with `document.querySelector` against the whole document and replaces every child unconditionally. Scope the lookup to the feed, keep a map from id to element, and reconcile.

Guard every fetch. Give the conversation view a generation counter that increments whenever the selected session changes, attach an `AbortController` to each request, and drop any response whose generation is stale. Coalesce snapshot revisions so a burst of revisions produces one refresh rather than one fetch each. Without this, switching sessions quickly lets an in-flight response for the previous session paint into the new one and steal focus — which is exactly the class of bug the existing Playwright suite watches for by failing on `Unexpected end of JSON input` and on reads of `sessions` from an absent snapshot.

Scroll only when the reader is already at the tail. Track whether the transcript is scrolled to within a small threshold of the bottom; append and auto-scroll when it is, and when it is not, show a "new messages" control that jumps to the tail. The current code calls `window.scrollTo(0, document.body.scrollHeight)` on every render, which yanks the page away from anyone reading history.

The composer keeps everything the current one does — plain-text `contenteditable`, paste and drop of images, refusal of rich content at `beforeinput`, Enter to send and Shift-Enter for a newline, IME-safe key handling — and gains the terminal's command surface. A leading `!` is a shell command. A leading `/` opens autocomplete over the Hel commands and the agent's advertised commands. `/help` lists both, marked by source. `/detach` signs the phone out of the conversation view and returns to the dashboard, which is the phone's reading of "leave without stopping the worker". `/model` and `/effort` complete over the values the harness advertised and post `set-config`. `/fast` toggles and refuses when the harness does not support it. `/plan` and `/implement` post `set-plan-mode` and are offered only while the agent is idle. Agent-advertised commands are forwarded as ordinary prompts.

The rules behind those commands must not be reimplemented in JavaScript. Whether fast mode is available, which mode ids plan mode uses, and which values `model` and `effort` accept are decided in Rust — `supports_fast_mode`, `plan_control` and `session_config_choices` — and published in the session projection. The browser reads the published answer and renders it. If a browser check and a Rust check ever disagree, the Rust one wins and the browser's check must be deleted, not patched.

Queued prompts show in order with remove buttons, and the composer offers "edit latest", which removes the newest queued prompt and puts its text back in the box — the same behaviour as `edit_latest_queued_prompt` in `src/hel_chat.rs`, including restoring the prompt if the removal fails. Active shells list with cancel buttons. The active turn shows a cancel control when the capability says it can be cancelled.

Prompt history walks with explicit controls (a phone has no comfortable Up-arrow) and offers a reverse lookup that searches as you type, backed by the bounded history endpoint from Milestone 8.

You will know this milestone worked when a long transcript can be scrolled up while the agent is still writing without being yanked to the bottom; when switching between two busy sessions never shows one session's text under the other's header; when `/model` completes over real advertised values and the change lands; when `/plan` is refused while the agent is running and accepted when it is idle; and when a queued prompt can be removed and re-edited.

This is the milestone mjolnir shortens most, and hel has already ported half the composer verbatim — `composerText`, the filler-`<br>` machinery, the image pipeline, and the `beforeinput`, paste and drop handlers all came across in Milestone 1's extraction.

Take these, each for a reason worth stating. `renderTranscript` with `entryRenderSignature` and `setEntryBody`, above all because `setEntryBody` records and restores which `<details class="block-fold">` the reader had opened, so folds do not snap shut on every update. `isNearTranscriptBottom` with the pinned-scroll rule and the `.jump-to-latest` control and its `.has-new` state. The `renderedSessionId` guard, which clears the container and the keyed array outright on a session mismatch. `loadFilesystem`'s generation counter, checked in the success path, the `catch` *and* the `finally`, and bumped on teardown as well as on a new request — this is the discipline to copy onto hel's snapshot and conversation fetches, not `refreshSessions`', which has no guard at all. The slash palette — `slashCommandQuery`, `commandSearchText`, `updateCommandPalette` matching prefix-first then substring and preserving the selected command by name across re-renders, `renderCommandPalette`, `moveCommandSelection` and `acceptCommandSelection` — with its keydown branch ordered after the input-method guard and before Enter-to-send. `queueSubmitInFlight`, whose comment names the bug hel has today: Enter calls submit directly and bypasses the disabled button. Clearing the composer only after the response is `ok`, with the draft and attachments cleared in the same breath. `setComposerEnabled` and `setComposerPlaceholder`, with a placeholder that says whether Send will send or queue. `renderQueue` from a template with per-row in-flight delete state. `createCombobox`, a searchable bottom-anchored picker that refuses to repaint while open and re-anchors above the keyboard when searching. The optimistic-with-timeout config state in `sentConfigChanges`, with three independent release conditions so a control can never be stuck disabled forever. And `.permission-title`, which renders the request text as a scrollable `<pre>` capped at 30dvh so no approval is granted over hidden text, with the pending block living inside the composer at the bottom of a flex column rather than mid-page.

Then write what mjolnir does not have. It has no prompt history and no reverse lookup at all — port those from hel's own terminal instead, where `src/hel_chat/history.rs` already has the generation counter, the original-input restore, and the session, project and all scopes. It has no edit-latest: its queue rows offer Cancel only, so `edit_latest_queued_prompt` comes from `src/hel_chat.rs`, including the restore-on-failure path a naive port drops. It has none of the seven commands — it changes model and effort through a combobox strip — so `/help`, `/detach`, `/model`, `/effort`, `/fast`, `/plan` and `/implement` are written here, along with value completion after a command and the rule that a fully-typed advertised value closes the popup so Enter submits. It has one dispatch path where hel has three, so the `!shell` prefix and its run and cancel pair are new. It has no plan concept, so the three step markers are new. Its elicitation model is a four-mode switch against hel's richer one, so porting it would be a regression. And because it polls on a fixed two-second timer, it never needed revision coalescing and uses no `AbortController` anywhere — both of which hel needs more than mjolnir did, not less.

### Milestone 8 — Per-viewer client state

Give the cookie an identity. Change `signed_cookie_value` to sign `"{viewer_id}|{expiry}"` and emit `"{viewer_id}.{expiry}.{signature}"`, where `viewer_id` is 128 bits of `getrandom` output, base64url without padding. Keep validating the old two-part form so a phone holding an existing cookie is not signed out by the deployment; treat such a cookie as an anonymous viewer with no stored state, and issue a three-part cookie on its next login. Derive the client id as `phone:{viewer_id}`.

Add a `client_session_state` table keyed by `(client_id, workspace_id, session_id)` holding the draft text, its update time, and an expiry, created by a new `fn ensure_client_session_state_schema` called from `migrate_schema` in `src/hel_database/schema.rs`, following the additive pattern the neighbouring `ensure_…` functions use. Keep read frontiers where they are; they already have a table and a write path. Prune rows whose client id starts with `phone:` once they pass the authentication retention period, and never touch rows belonging to terminal clients.

Add three routes. `GET /api/sessions/{id}/client-state` returns this viewer's draft and read frontier for that session. `PUT /api/sessions/{id}/draft` stores a draft, rejecting bodies over 64 KiB with a stable code. `POST /api/workspaces/{id}/read` marks a whole workspace read in one request, so opening a workspace does not need one request per session. Add `GET /api/sessions/{id}/history?q=…&scope=…` for prompt history, and give `src/hel_database.rs` a bounded search — a new function, or a limit parameter on `search_prompts` — because the existing one pages through the entire table and must not be reachable from HTTP.

Autosave the composer after a short debounce, around 400 milliseconds after typing stops. Keep the composer's contents when an enqueue fails, and clear it only after the daemon has accepted the prompt. Unsent image attachments stay in browser memory only and are never uploaded as drafts; say so in the interface, next to the attachments, so nobody loses a photo by trusting the draft.

You will know this milestone worked when a draft typed on one phone survives a reload and does not appear on a second phone with its own cookie; when a failed send leaves the text in the box; when a successful send clears it; when a 65 KiB draft is refused with a stable code; and when a phone row expires from the table without disturbing a terminal client's frontier.

mjolnir solved none of this. Its `session_cookie_value` is `mac.update(exp.to_string())` returning `format!("{exp}.{sig}")` — the identical flaw hel has, never noticed there because nothing was ever keyed to a viewer. `draft` appears zero times in `remote.rs`; drafts are an in-memory map that dies on reload and, a defect not to inherit, is not cleared on sign-out. There are no read frontiers, no bounded history search, and its decision route is a plain `INSERT` with browser-side dedup only, which does not survive a reload.

What it does supply are small mechanisms, each saving an hour rather than a day. A per-route `DefaultBodyLimit::max(…)` layered on one route under a smaller global limit is exactly how to bound `PUT /api/sessions/{id}/draft` to 64 KiB without loosening every other route. `session_cookie_header` emits an `Expires=` fallback alongside `Max-Age`, which matters for an installed iOS progressive web app whose storage is evicted. Cookie names are scoped per deployment, with the reason stated: browsers scope cookies by host and not by port, so two servers on one machine replay each other's cookies — hel has one hard-coded `COOKIE_NAME` and its own lab starts a second daemon. An `ensure_cookie_key` and `rotate_cookie_key` split with a `--logout-all` flag is the only revocation a stateless cookie has. `spawn_queue_pruner` with `prune_stale_records` is one background loop with per-class counts and four retention policies, each with a written reason — the shape hel needs for documenting the phone-versus-terminal asymmetry. And `search_filesystem_under_roots_with_limits` takes its limits as parameters so tests can drive truncation cheaply, and returns a `truncated` flag so the interface can say the answer is partial; `search_prompts_bounded` should do the same, because without it a phone cannot tell twenty matches from the first twenty of many.

### Milestone 9 — Phone behaviour, accessibility and resilience

Make the whole application work between 320 and 430 CSS pixels wide with no horizontal overflow anywhere, including with a workspace named at forty characters and a session titled at the full 120. Wide content — tables inside Markdown, code blocks, diff summaries — scrolls inside its own container; the page body never does. Every interactive target is at least 44 by 44 pixels. Every form control uses at least 16-pixel text, because smaller text makes iOS Safari zoom the page on focus and never zoom back. Honour all four safe-area insets. Keep the composer visible when the keyboard opens, using the visual viewport rather than guessing at heights.

Label everything. Buttons get accessible names, the tab strip gets `role="tablist"` semantics with `aria-current`, forms get real labels, and status text always pairs its colour with a word or an icon. Announce background results — a session finished provisioning, an action failed, the connection dropped — through a polite live region, and move focus deliberately when a route changes so a screen reader lands at the top of the new page rather than wherever it was.

Handle the four states a phone actually hits. Offline: show it, stop the SSE stream, and resume when `online` fires. Reconnecting: show it while the stream is re-establishing, and reconcile by full snapshot rather than assuming the deltas lined up. Expired authentication: return to the login page without losing the route, and come back to it after unlocking. Background action failure: report it against the thing that failed, never silently.

Keep a usable wider-screen presentation — larger type, more comfortable spacing, a wider column — but do not build a desktop-only layout that shows the dashboard and a conversation at once. The terminal surface is where that combined view lives.

Extend the Playwright suite at `tests/e2e/web/` into a matrix over 320×568, 390×844 and one wider viewport, covering: multiple workspace tabs and project grouping; New, Resume, rename, stop and cancel; Targets and Quota including refresh, stale and error; prompt, queue, shell, image, elicitation, history, configuration and plan workflows; a reloaded draft and isolation between two browser contexts with separate cookies; offline recovery, out-of-order responses, authentication expiry, sign out and browser navigation; and the layout assertions — no horizontal overflow, touch targets at or above 44 pixels, and a header and composer that stay put while the transcript scrolls.

The phone mechanics arrive essentially finished and are the fiddliest part of this milestone; the accessibility half is untouched, and in several places mjolnir is an anti-example whose CSS would fail hel's own acceptance.

Take the mechanics. `syncKeyboardInset` computes `Math.max(0, innerHeight - visualViewport.height - visualViewport.offsetTop)` into a `--keyboard-inset` custom property; the `offsetTop` term is the one naive versions miss, and all three listeners matter — window resize, and the visual viewport's own resize and scroll. Safe-area insets are applied per edge to the element that actually touches that edge, always as `calc(<base> + env(…))` so the design padding survives a device with no notch. Form controls are 16 pixels with the comment recording why: below that, iOS Safari zooms on focus and never zooms back. `overscroll-behavior: contain` on every internal scroller stops a flick at a list's end from rubber-banding the shell and suppresses pull-to-refresh, and `overscroll-behavior-x: contain` on wide content stops a horizontal swipe triggering browser-Back. The `min-width: 0` discipline appears at 29 sites, because a flex or grid child defaults to `min-width: auto` and refuses to shrink below its content — the single most common cause of a phone page scrolling sideways. `apiFetch` handles 401 centrally and every call site guards on `if (error.message !== "HTTP 401")`, so an expiry produces one login screen and zero spurious error banners. The reconnect trio listens for `online` *and* `visibilitychange`, and the visibility listener matters most, because a backgrounded progressive web app gets no `online` event and the person's first signal is unlocking the screen. `withViewTransition` catches on `transition.ready`, because a skipped transition otherwise surfaces as an unhandled rejection and would fail hel's own zero-page-error assertion. Escape closes only the topmost overlay, and outside-tap dismissal uses a capture-phase `pointerdown` with a containment test. The service worker's offline fallback chain ends at `caches.match("/")`, so a deep link opened offline renders the shell rather than the browser's error page, and its registration `.catch` records that a failed registration means the application is not installable and nothing more.

Everything else is written here, and three of mjolnir's numbers are the reason. Its largest interactive control is 42 pixels and its common ones are 24 to 36, against hel's 44-by-44 bar. It has exactly two media queries — 800 pixels and reduced motion — and nothing tuned below 800, so the 320-to-430 band is untested there. It has ten widget-local `role="status"` elements and no single announcer, no `aria-current` at all, and selection shown by a class and a gradient, which is colour alone.

Three defects in hel's own current viewer belong in this milestone's list, because none is called out elsewhere. `src/web/viewer.js` registers the service worker with no `.catch`, so a rejected registration is an unhandled promise rejection — precisely the failure that Milestone 1 worked around in the Playwright configuration rather than fixing in the product. There is no `visibilitychange` listener, only `online`, so a backgrounded phone waits for a keep-alive failure. And a 401 from any route other than the snapshot refresh never reaches the login swap, because `request()` throws `unauthorized` and only `refresh()`'s catch handles it.

Add one more acceptance assertion than the plan first listed: that `element.style.setProperty` raises no Content-Security-Policy violation under hel's shipped `style-src 'self'`. The keyboard-inset technique depends on it, and mjolnir cannot vouch for it, because it ships no policy and its two CSSOM `setProperty` calls have never run under one.

## Concrete Steps

All commands run from the repository root, `/home/jonathan/Projects/hel`, unless stated otherwise.

Run the Rust suite and the linter outside the restricted sandbox with elevated permissions. The suite binds loopback TCP and Unix sockets; a sandboxed run fails with `EPERM` or hangs, and either way is not a result:

    cargo test
    cargo clippy --all-targets -- -D warnings

`.cargo/config.toml` sets the default build target to `x86_64-unknown-linux-musl` so the controller binary doubles as the container worker. On a non-x86_64 Linux host, pass your own triple, for example `cargo build --target aarch64-apple-darwin`.

Node 24 must be on `PATH`; the JavaScript tests shell out to it. Check with `node --version`.

Build the binary the browser lab drives, install Playwright once, and run the lab:

    cargo build --bin hel
    (cd tests/e2e/web && npm ci)
    tests/e2e/run-browser-reliability.sh --seed 4242 target/x86_64-unknown-linux-musl/debug/hel

The lab prints staged progress; a healthy run ends with the Playwright line reporter reporting its tests passed and the script exiting zero. It is stateful in the sense that a lab directory left behind by a previous run confuses the next one, so let each run create its own.

To drive the viewer by hand against the same fake harness, start the lab's daemon and read the unlock code and QR URL from the daemon status the lab prints, then open the HTTPS URL in a browser with a phone-sized viewport. The daemon also prints `Hel viewer code: NNNNNN` on stdout when the server starts.

To check the security headers on a running viewer:

    curl -skI https://<viewer-host>:<port>/ | grep -i 'content-security-policy\|x-content-type-options\|referrer-policy\|cache-control'

Expect a `Content-Security-Policy` beginning `default-src 'none';`, `X-Content-Type-Options: nosniff`, `Referrer-Policy: no-referrer`, and `Cache-Control: no-store` on the shell.

Commit at each milestone. Stage only the files that milestone changed — never `git add -A` — and commit to the current branch without creating one. Do not push.

## Validation and Acceptance

Acceptance is behaviour someone can watch, not code that exists.

For Milestone 1, run `cargo test` and expect it to pass, including the two relocated Node-driven JavaScript tests, which must now read `src/web/viewer.js`. Run the browser lab and expect it to pass unchanged. Fetch the viewer with `curl -skI` and expect the four headers above. Open the viewer in a browser, open the developer console, and expect zero Content Security Policy violation messages. The visible application must be identical to the one before the change; if anything looks different, the move was not clean.

For Milestone 2, add Node-driven tests over `src/web/markdown.js` and expect these specific results: a heading `# Title` produces an `H1` whose `textContent` is `Title`; a nested list produces a `UL` containing a `LI` containing a `UL`; a fenced block produces a `PRE` containing a `CODE` whose text is the block's contents unparsed; a table produces one `TABLE` with a header row; the input `<img src=x onerror=alert(1)>` produces a text node with that exact text and zero `IMG` elements; the input `[click](javascript:alert(1))` produces no `A` element; and the input `[click](https://example.com)` produces an `A` whose `href` is `https://example.com`. Add the sink test and prove it fails when a stray `innerHTML =` is introduced, then remove the stray line.

For Milestone 3, extend the existing redaction test in `src/hel_server.rs` — the one built on `sample_config_state`, whose fixture deliberately contains `/highly/secret/codex`, `secret-token`, `secret.registry/image`, `secret-target` and `native-secret-id` — so the enriched snapshot's serialized JSON still contains none of those strings. Then write behaviour tests named in the repository's descriptive style, for example `a_new_action_creates_in_the_named_workspace_when_three_exist`, `a_provisioning_session_offers_cancel_and_not_stop`, `a_running_turn_offers_cancel_turn`, `a_stopped_session_offers_resume_and_not_prompt`, `an_incompatible_resume_target_is_absent_and_refused`, `a_config_key_the_harness_never_advertised_is_refused`, `capacity_reports_refreshing_stale_and_error_states`, and `an_operation_projects_every_active_stage`. Each must fail before the change and pass after.

For Milestone 4, run the browser lab and, in the Playwright spec, assert: with two workspaces configured, two tabs render; clicking the second changes the URL fragment to `#workspace/{id}` and changes the visible session list; reloading lands on the same workspace; a session in `Stopped` does not appear; pressing the browser Back button from a conversation returns to the dashboard rather than leaving the application; and a session whose capabilities deny rename shows no rename control.

For Milestone 5, assert in Playwright: choosing a bare target reveals a directory step and choosing a container target does not; a bundle with an uncommitted change stops the flow with a named confirmation listing the repository by leaf name and no path, and proceeds once confirmed; posting the same `dirty_ack` after a second repository goes dirty is refused with a stable code; a resume onto a target listed as incompatible is not offered, and posting it directly returns 400; and cancelling during provisioning leaves the session list without a half-created row once the operation settles.

For Milestone 6, assert: both pages open from the menu and by URL; a target whose probe fails keeps its last reading and shows an error state; a quota older than its refresh interval reads stale; and pressing refresh moves the last-refresh time forward.

For Milestone 7, assert: with a transcript longer than the viewport, scrolling up while the agent writes leaves the scroll position alone and shows the new-messages control, and pressing it returns to the tail; switching rapidly between two sessions never renders one session's entries under the other's header, and produces no console error; typing `/mod` opens autocomplete containing `/model`, accepting it and a value posts `set-config` and the session's reported model changes; `/plan` while the agent is running is refused with the terminal's wording and accepted while idle; `!echo hello` runs as a shell command and can be cancelled; and removing the newest queued prompt puts its text back in the composer.

For Milestone 8, assert with two browser contexts holding different cookies: a draft typed in context A survives a reload of context A, and context B's composer stays empty; a send that the server refuses leaves the text in place; a send the server accepts clears it; a 65 KiB draft returns a stable refusal code; and the client-state endpoint returns this viewer's frontier and not another's. In Rust, prove pruning removes an expired `phone:` row and leaves a terminal client's frontier untouched.

For Milestone 9, run the Playwright matrix at 320×568, 390×844 and the wider viewport, asserting on each: `document.documentElement.scrollWidth` is not greater than `clientWidth` on every route; every visible button's bounding box is at least 44 by 44; the header and composer keep their positions while the transcript scrolls; and the offline, reconnecting, expired-authentication and sign-out paths all recover. Then run the full gate one more time: `cargo test`, `cargo clippy --all-targets -- -D warnings`, and the browser reliability suite.

## Idempotence and Recovery

Every step here is additive and repeatable. Re-running `cargo test`, `cargo clippy` or the browser lab has no lasting effect on the repository.

The database change in Milestone 8 is one `CREATE TABLE IF NOT EXISTS` plus a pruning query, added as its own `ensure_…` function called from `migrate_schema`, matching the pattern of `ensure_workspace_schema` and its neighbours. Running it twice is a no-op. It creates a new table and alters no existing one, so an older Hel binary opening a database that has been through it keeps working; only `SCHEMA_VERSION` gates that, and this change does not require bumping it because nothing existing changes shape. If it later turns out a bump is needed, bump it and say so in the Decision Log, remembering that `open_reader_strict` refuses a database whose version is not exactly `SCHEMA_VERSION`, so an old client will then require the daemon to migrate first.

The cookie change in Milestone 8 is designed to be safely repeatable and safely rolled back. New cookies are three-part; old two-part cookies keep validating; a rollback to the previous binary leaves three-part cookies failing validation, which signs those phones out and asks for the code again — inconvenient, not damaging. Deleting the key file at `hel_config::data_dir().join("phone-cookie-key")` signs every phone out deliberately, which is the existing documented way to do that.

If a milestone has to be abandoned partway, the tree still builds and tests still pass at every commit, so the recovery is to revert that milestone's commits. Record why in the Decision Log and split the `Progress` entry into what landed and what did not.

The browser lab creates its own temporary lab directory per run. If a run is interrupted and leaves one behind, delete it before the next run rather than reusing it; a second run against a previous run's lab produces confusing failures that look like product bugs and are not.

## Artifacts and Notes

The redaction fixture already in `src/hel_server.rs` is the anchor for every projection test. It is worth knowing what is in it, because a new field that copies the wrong thing will be caught by it:

    profile "codex-1": home "/highly/secret/codex", environment GH_TOKEN=secret-token
    target  "podman":  image "secret.registry/image", environment TOKEN=secret-target
    session "session-1": native_session_id Some("native-secret-id")
                         last_error Some("secret-token at /highly/secret/codex")

The transcript styling the browser must mirror, read from `fn entry_visual` and `fn tool_presentation` in `src/hel_chat/transcript.rs`:

    role           glyph   label            colour
    User           ❯       You              cyan
    Agent          ●       Agent            yellow
    Thought        ○       Thinking         dark grey, italic
    Tool pending   •       Tool · waiting   dark grey
    Tool running   ●       Tool · running   yellow
    Tool done      ✓       Tool · done      green
    Tool failed    ×       Tool · failed    red
    Plan           ◇       Plan             magenta
    PlanProposal   ◈       Proposed plan    light magenta
    System         ─       Hel              dark grey

    plan step markers: ○ pending (dark grey)  ● running (yellow)  ✓ completed (green)

The threshold colouring the terminal uses for percentages, from `crates/hel-tui/src/render.rs`:

    0..=20  red     21..=50  yellow     51..    green

read as headroom remaining for quota, and inverted for target CPU, so a busy machine reads red.

The shape of the fake DOM the existing Node-driven tests build is worth copying rather than reinventing; it lives in the `#[cfg(test)]` block of `src/hel_server.rs` and supplies `document.createElement`, an `Option` class, and elements exposing `append`, `appendChild`, `replaceChildren`, `addEventListener`, `setCustomValidity` and `reportValidity`. Milestone 1 moves it into its own file so both the elicitation tests and the new Markdown tests can share it.

## Interfaces and Dependencies

Be prescriptive. These are the names and shapes that must exist when the plan is done.

No new runtime dependency is added. The server keeps `axum`, `tokio`, `serde`, `hmac`, `sha2`, `base64` and `getrandom`. The browser keeps zero dependencies. Playwright stays a test-only dependency inside `tests/e2e/web/package.json`.

New files:

    src/web/viewer.html          the application shell, no inline script or style
    src/web/viewer.css           tokens and components
    src/web/viewer.js            the application: router, pages, transport
    src/web/markdown.js          the DOM-building Markdown renderer
    src/web/service-worker.js    versioned cache, /api/ never cached
    src/web/manifest.webmanifest points at the real PNG icons
    src/web/test-dom.js          the shared fake DOM for Node-driven tests

In `src/hel_server.rs`, extend the projection types. The exact field sets may be adjusted during implementation as long as the redaction contract holds and the Decision Log records the change; these are the required names:

    pub struct ViewerSessionCapabilities {
        pub open: bool,
        pub prompt: bool,
        pub run_shell: bool,
        pub cancel_turn: bool,
        pub cancel_operation: bool,
        pub stop: bool,
        pub rename: bool,
        pub resume: bool,
        pub set_config: bool,
        pub set_plan_mode: bool,
    }

    pub enum ViewerLifecycleCategory { Live, Starting, Stopping, Stopped, Failed }

    pub struct ViewerOperation {
        pub id: String,
        pub session_id: String,
        pub kind: ViewerOperationKind,       // Create | Resume | Stop | Checkpoint
        pub started_at_epoch_seconds: u64,
        pub stages: Vec<ViewerOperationStage>, // label plus start time
        pub notice: Option<String>,          // controller-authored, already user-facing
        pub cancellable: bool,
    }

    pub struct ViewerQuotaWindow {
        pub label: String,
        pub percent_used: u8,
        pub resets_at: Option<String>,
        pub projects_exhaustion_before_reset: bool,
    }

    pub struct ViewerTargetCapacity {
        pub id: String,
        pub label: String,                   // host or fleet name, never a full locator
        pub target_ids: Vec<String>,
        pub cpu_percent: Option<u8>,
        pub memory_used_bytes: Option<u64>,
        pub memory_total_bytes: Option<u64>,
        pub logical_cores: Option<u64>,
        pub disk_total_bytes: Option<u64>,
        pub virtual_machines: Option<u64>,
        pub sampled_at_epoch_seconds: Option<u64>,
        pub refreshing: bool,
        pub stale: bool,
        pub has_error: bool,
    }

    pub enum ViewerChatPhase { Idle, Running, Closing, Closed }

    pub struct ViewerConfigOption {
        pub key: String,                     // "model", "effort", ...
        pub label: String,
        pub current: Option<String>,
        pub choices: Vec<ViewerConfigChoice>, // value, name, optional description
    }

`ViewerChatPhase` mirrors `RelayExecutionState` from `src/hel_worker/snapshot.rs`, and `ViewerConfigOption` mirrors what `session_config_choices` in `src/hel_acp.rs` reads out of the harness's advertised `config_options`. Both exist so the browser reads a published answer instead of reimplementing harness rules.

Then add to `ViewerSession`: `project_label: String`, `project_key: String`, `lifecycle: ViewerLifecycleCategory`, `latest_event_ordinal: u64`, `operation: Option<ViewerOperation>`, `chat_phase: ViewerChatPhase`, `config: BTreeMap<String, String>`, `config_options: Vec<ViewerConfigOption>`, `compatible_resume_targets: Vec<String>`, `capabilities: ViewerSessionCapabilities`. Keep `incompatible_resume_targets` during the rollout so an older cached shell keeps working.

Add to `ControllerAction`:

    Rename        { session_id: String, title: String },
    CancelTurn    { session_id: String },
    SetConfig     { session_id: String, key: String, value: String },
    SetPlanMode   { session_id: String, active: bool },
    RefreshQuota  { profile_id: String },
    RefreshCapacity { target_id: String },

and change `New` to require `workspace_id: String`, accept `title: Option<String>`, and carry `dirty_ack: Vec<String>`.

New routes on the authenticated router in `fn router`:

    POST /api/preflight/new
    GET  /api/sessions/{id}/client-state
    PUT  /api/sessions/{id}/draft            bounded to 64 KiB
    POST /api/workspaces/{id}/read
    GET  /api/sessions/{id}/history

`/api/actions` stays the single action entry point. A short relay operation answers `202 Accepted` with an empty body once the daemon acknowledges the enqueue. A long lifecycle or refresh operation answers `202 Accepted` with `{"operation_id": "..."}` and reports progress through the snapshot and the existing `/api/events` revision stream. Every rejection answers with a stable safe code and a fixed message through the existing `ApiError` path, which already guarantees no path, host, environment value or controller error text reaches a phone.

For operation progress, subscribe the phone control loop to the lifecycle feed that already exists rather than adding a second access path to the same mutex: `crates/hel-cli/src/pollers.rs` publishes a `watch::Receiver<Vec<daemon::RuntimeLifecycleView>>`, consumed today as `runtime_lifecycles` in `crates/hel-cli/src/dashboard.rs`. Add `pub(crate) fn active_lifecycles(&self) -> Vec<RuntimeLifecycleView>` to `crates/hel-cli/src/daemon.rs` only if that feed turns out not to reach the phone loop.

In `crates/hel-cli/src/server.rs`, spawn `spawn_dashboard_capacity_poller()` beside the existing `spawn_quota_refresher()`, feed it `controller.deployment_capacity_targets()` on every controller reload, and handle `CapacityPollUpdate` in the existing `tokio::select!`.

In `src/hel_database.rs` and `src/hel_database/schema.rs`, add:

    pub fn client_session_state(client_id: &str, workspace_id: &str, session_id: &str)
        -> Result<Option<ClientSessionState>>;
    pub fn persist_client_draft(client_id: &str, workspace_id: &str, session_id: &str, text: &str)
        -> Result<()>;
    pub fn prune_phone_client_state(older_than: Duration) -> Result<usize>;
    pub fn search_prompts_bounded(session_id: &str, bundle_id: &str, scope: HistoryScope,
                                  query: &str, limit: usize) -> Result<BoundedPromptHistory>;
    // BoundedPromptHistory { entries: Vec<PromptHistoryEntry>, truncated: bool }
    // The flag is not decoration: without it a phone cannot tell twenty matches
    // from the first twenty of many, and will present a partial answer as whole.

and `fn ensure_client_session_state_schema(connection: &Connection) -> Result<()>`, called from `migrate_schema`, creating the table with `IF NOT EXISTS` and `STRICT`, matching the style of the neighbouring `client_read_frontiers` definition.

Writes go through `submit_database_write` so the daemon stays the only writer, as `persist_read_receipt` already does.

## Follow-up issues to open before implementation

Two GitHub issues on `BrokkAi/hel` are required before implementation starts, and neither is implemented by this plan. Confirm with the repository owner before opening them, because opening an issue is an outward-facing action.

The first is "Web viewer: add a mobile second-opinion workflow". Hel already has a second-opinion feature in `src/hel_second_opinion.rs`; the issue is about reaching it from a phone. It must reference this parity work, and define the phone user experience, how a request in flight is cancelled and how failures are reported, the accessibility requirements, and the acceptance tests.

The second is "Web viewer: add browser dictation". Hel already has speech support in `src/speech.rs` and `voice-worker/`; the issue is about dictating a prompt in the browser. It must reference this parity work, and define the phone user experience, cancellation and error handling, accessibility, and — most importantly — the privacy and microphone-permission behaviour, plus acceptance tests.

Reviewer and dictation controls are explicitly out of scope for every milestone above.

## Revision notes

- 2026-08-31 — First version. Written after reading `src/hel_server.rs`, `crates/hel-cli/src/server.rs`, `crates/hel-cli/src/daemon.rs`, `crates/hel-cli/src/pollers.rs`, `src/hel_chat/transcript.rs`, `src/hel_chat.rs`, `src/hel_chat/autocomplete.rs`, `src/hel_chat/history.rs`, `src/hel_database.rs`, `src/hel_database/schema.rs`, `src/hel_quota.rs`, `src/hel_targets.rs`, `src/hel_state.rs`, `src/hel_controller.rs`, `crates/hel-tui/src/render.rs`, `crates/hel-tui/src/ingest.rs` and `tests/e2e/`. The plan is written so a reader with only the working tree and this file can execute it. Four findings shaped it and are recorded in `Surprises & Discoveries`: the session cookie carries no viewer identity, so per-viewer drafts need one added; JetBrains Mono and the PWA icons are already vendored and unused; the capacity poller already exists and only needs spawning in the phone loop; and the daemon already tracks operation stages, so operation progress needs projecting rather than inventing. A fifth finding shaped the scope of Milestone 4: phone session creation currently fails outright on any install with more than one workspace, because the viewer never sends a workspace id and `apply_phone_action` refuses to guess between several.

- 2026-09-01 — Second version, after surveying the sibling `../mjolnir` repository. The first version was written as though hel's viewer had no prior art. It has a great deal, but not where the first version would have guessed, and the correction runs in both directions. mjolnir's production phone viewer supplies a liftable transcript renderer, a keyed row engine, a slash palette, and the phone mechanics — keyboard inset, safe areas, overflow containment, reconnect and 401 handling — which shortens Milestones 2, 4, 7 and the mechanical half of 9. It supplies nothing at all for Milestones 3, 5, 6 and 8, because it has no workspaces, no targets, no capacity, no structured quota, no read frontiers, no server-side drafts, a two-field new-session request, and the same cookie defect hel has. The revision therefore adds an orientation subsection naming exactly what transfers, a `Porting rules` section recording seven defects a faithful port would introduce, and a port-versus-build paragraph in every milestone. Milestone 2 was rescoped: the transcript chrome moves to Milestone 7, because hel's transcript entries are still flat lines and a renderer ported before the projection is enriched would show less than mjolnir's does. Three findings were verified by hand before being written down — the diffstat format, the existing lifecycle watch channel, and mjolnir's identical cookie flaw — and are in `Surprises & Discoveries` with their evidence.
