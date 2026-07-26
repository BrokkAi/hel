# Loki — Final Report

**Program verdict (2026-07-26): closed.** Across ~27 arms and roughly 1,000
graded DeepSWE rollouts spanning three model families, no Loki configuration —
dumb or smart, sync or async, minimal or adversarial-supply, with or without an
Eitri delegate — produced a cost-effective improvement over a vanilla solo
solver. The machinery works; the thesis didn't survive measurement.

This document is the closing summary. The running index of the earlier
experiments (through 2026-07-23) is `council-results.md` in this directory;
per-arm artifacts are indexed at the bottom.

---

## 1. What we were testing

The council: **Thor** (main solver, an Anvil ACP session) with **Loki**
(reviewer, observing Thor's and Eitri's activity streams and injecting advice)
and optionally **Eitri** (implementation delegate on a nested ACP session).
The thesis, in two strengths:

1. *Weak form:* a cheap Loki can catch a strong Thor's mistakes → more solves
   for little money.
2. *Strong form:* a strong Loki can lift a weaker, cheaper Thor to
   frontier-solver quality → same solves for less money.

Benchmark: DeepSWE v1.1 via brokkbench (`bpr_agent.py --engine deepswe`),
180-minute solver cap (90 min before 2026-07-25). Capability metric is **f2p**
(fraction of hidden new-feature tests passing; equals published `avg_score`).
Our `partial` is p2p-inflated — never used for claims. Task sets: `deepswe20`
(easy-20), `deepswe-solhard-fablesolves` (11 tasks sol@high scores 0.000 on,
fable-high solves), and the 22-task opus-hard set.

## 2. Same-tier results (DeepSeek v4 family, deepswe20 ×2)

Thor = pro throughout. Baseline: **vanilla pro 13/37 solves, ~$4.60/40**.

| Arm | Loki | Eitri | Solves | Cost/40 | Note |
|---|---|---|---|---|---|
| pff (async r1) | flash | flash | 15 | ~$8.6–9.0 | round-1 "flash beats pro Loki" (15 vs 8) was noise |
| pff2 / ppf2 rerun | flash / pro | flash | 11 / 12 | — | p = 1.0 between Loki tiers |
| pff3 (compaction) | flash | flash | 14 | — | best async arm |
| pff4 (advice ledger/drain/gate) | flash | flash | 13 | — | correct-on-principle, moved nothing |
| pff5–9 (sync/grouped review, N=10/5/20/5-deminimized/∞) | flash | flash | 11/7/6/8/6 | — | **negative**: pooled sync 38/200 vs async 38/120, p≈0.01 |
| pff10 (boundary rendezvous) | flash | flash | 11 | — | null |
| pff11 (verification protocol) | flash | flash | 9 | — | offline-validated text, live null |
| thoreitri2/3 (keep-running era) | — | flash | 7 / 8 | $5.39 / $4.39 | delegation without review |
| thoreitri-pro | — | pro | 10 | $3.98 | cheapest arm ever; real context offload (Thor $4.60→$3.11) |
| proproflash | flash | pro | 9 | ~$8.35 | flash Loki on best delegation arm: −1 solve, +$4.4 |

Final async ladder across five prompt variants: 11, 14, 13, 11, 9 — one noise
band containing the vanilla baseline. Conclusion stated with numbers in hand:
**n=40 arms cannot resolve prompt-level interventions**; every further prompt
idea would land in that band and teach nothing.

## 3. Cross-tier results (weak Thor, strong Loki)

| Arm | Config (T / L / E) | Task set | f2p mean | Solves | Solo baseline |
|---|---|---|---|---|---|
| gpt56-council | sol@h / opus-4-8@h / terra@m | solhard-11 ×2 | 0.169 | 1/22 | sol@h published 0.000 |
| gpt56-opus5 | sol@h / **opus-5**@h / terra@m | solhard-11 ×2 | 0.285 | 0/22 | sol@h published 0.000 |
| opus-terra | opus-4-8@h / terra@h / terra@h | opus-hard-22 ×2 | 0.069 | 0/44 | opus@h published 0.000 |
| tl | terra@h / sol@m / — | deepswe20 ×2 | 0.489 | 5/40 | terra@h published 0.875 |
| tsl | terra@h / sol@m / luna@xh | deepswe20 ×2 | **0.868** | **29/40** | sol@h published 0.912 @ $3.07 |

The strong-Loki lift on hard tasks is real but small: upgrading the Loki from
opus-4-8 to Opus 5 on the identical arm moved f2p 0.169 → 0.285 (bimodal pairs,
n=22 — suggestive, not confirmed), against a solo-sol floor of 0.000. It never
approached fable-solo (0.500) and produced 1 formal solve across 88 attempts.
The one durable existence proof stands:
`happy-dom-deterministic-intersectionobserver`, where opus and terra each score
0.000 solo but the council reproducibly hits 13/14 f2p (2 of 22 tasks; the
other 20 showed nothing).

**tsl is the best arm we ever ran and still doesn't pay.** It reached 0.868
f2p / 29-40 solves at ~$2.89/attempt (rates least-squares-fit from published
per-task cost columns) — close to published vanilla sol@high (0.912 @ $3.07)
but not matching it, at cost parity. The tl/tsl split (0.489 → 0.868 from
adding luna@xhigh as Eitri; attempts 4 min → 30 min; 590M cached tokens
through the delegate) shows the *Eitri seat* doing the lifting, not Loki.
And published **luna@xhigh solo is 0.900 @ $1.23/task** — better than our
whole council at 40% of the cost. Unresolved confound: our scaffold extracts
much less from this family than the published agent does (terra+Loki 0.489 vs
terra-solo-published 0.875); our-harness solo controls for terra/luna were not
run.

## 4. Why it doesn't work (mechanism evidence)

Trace audits across the DeepSeek arms, sample sizes in the session notes:

- **Advice supply was never the bottleneck.** 73% of failures had a correct
  diagnosis somewhere in Loki's advice. Loki's selectivity/silence bias is
  load-bearing: de-minimizing his prompt quadrupled advice volume (6.6
  notes/run) and made partials *worse*.
- **Delivery/framing was never the bottleneck.** 80/80 sampled Thor turns
  engaged with injected advice on the first turn. Framing experiments: null.
- **The loss mode is verification, not review.** ~45% of true losses were
  MISVERIFIED — Thor "confirms" with narrow greps, truncated reads, or by
  trusting quoted output; ~80% of failures were believed-done (golden-file
  regen, unexercised new tests, post-failure scope narrowing). Another ~40% of
  ignored-advice cases were unfixable by any uptake (hidden-test structure).
- **Offline wins don't survive live.** The verification-protocol text cut
  declare-resolved-without-evidence from 91% → 55% on replayed loss points,
  then rolled out to 9/40 — indistinguishable from every other arm.
- Review-as-cadence (sync/grouped) is strictly worse than review-as-exception
  (async): boundary-concentrated advice loses mid-flight timeliness.

Cost side: Loki is never free. Flash-Loki ≈ +$2.3–4.4/40 (advice interleaving
also fattens Thor's context); sol-Loki ≈ $5.9–62 depending on run length.
Caching is not the problem — Loki seats ran 88–99.8% hit ratios (Opus 5 on
Bedrock: 99.8%). The seat's tokens are cheap; its marginal solves are ~zero.

## 5. What the program produced that we keep

The negative result paid for a pile of validated infrastructure:

- **Delegation stack** (mj): Eitri activity transcripts in results,
  keep-running slices (no turn interruption), retained-after-FINAL sessions
  with `code_agent_continue(run_id, guidance)`, supersede-on-new-delegation.
- **Two-sided Eitri contract** + the public-surface fidelity clause — killed
  the ImportError-at-collection failure class dead (returns-validated:
  0/159 ×2 → 153–156/159).
- **180-minute cap as default** (matches published methodology) — converted
  deadline-killed near-wins into solves (kcp-go: both runs).
- **Responses-API server-side chaining** on Bedrock Mantle (GPT reasoning
  cache ~30% → ~80%); per-seat reasoning effort (`+effort` selectors);
  Loki compaction.
- **MCP/ACP hardening**: anvil 60s/300s timeout split + cancellation
  notifications; rmcp idle-eviction fix (`keep_alive=None`) + client
  reinit-on-404; bounded held-completion release; post-SIGKILL reap demotion.
- **Bedrock discovery hardening** (anvil): retry with jittered backoff +
  last-good catalog caches. Fleet-startup mortality 11/22 → 0/81.
- **Opus 5 wire-up** on Bedrock (`us.anthropic.claude-opus-5`, Adaptive
  thinking, xhigh presets) and the glibc note: benchmark pins must be built in
  `rust:1-bookworm` (task images are glibc 2.36).

## 6. If anyone reopens this

Do not reopen with prompt ideas. The recorded preconditions:

1. **Power first**: 117-task set or 3–4× runs per arm; n=40 is proven blind at
   these effect sizes.
2. **Structural, not textual**: the one untested mechanism with offline
   support is forced follow-up — when Thor resolves advice without an
   intervening tool call, force one verification turn. State machine, not
   prompt.
3. **The economically live direction is the Eitri seat, not Loki**: tl→tsl
   says a deep-effort implementation delegate transforms outcomes; the missing
   experiments are our-harness solo baselines (luna@xhigh, terra@high) to
   separate scaffold loss from orchestration gain, and Thor=luna@xhigh solo vs
   council at matched cost.
4. Known benchmark artifacts to exclude from any new analysis:
   `ts-pattern-match-each`, `httpx-deterministic-cookie-store` spec quirks;
   Go-package test-name collisions (a model-authored `*_test.go` can break the
   hidden grader's build → f2p 0 with partial 0.000).

## 7. Artifact index

- Results: `/mnt/optane/{councilpff*,councilppf*,councilpnf,vanillaflash,vanillapro*,ds-*,gpt56-council*,opus-terra-council,tl-council,tsl-council,fable-council,easy2*}/`
  (each: `results/`, `archive/` with `mjolnir-events.jsonl` per attempt, `run.log`).
- Pinned binaries + launchers: `/mnt/optane/council-ab/` (current:
  `mj-652ff50-surface`, `anvil-d0d575b-discovery`, `launch-council.sh`,
  `launch-deepseek-v2.sh`).
- Published baselines:
  `~/Projects/deep-swe/published-results/deepswe-v1.1/per-task-by-model-effort.csv`.
- Cost/usage tooling: `arm_report.py` pattern — per-seat `council_usage` from
  `mjolnir-events.jsonl`; hit = total − input − output − thought.
- Earlier narrative + per-arm detail: `.agents/plans/council-results.md`.
