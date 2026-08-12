# Detection-time campaigns

Seeds-to-first-detection per (mutant, strategy): Kaplan–Meier quantiles (a campaign
censored at the 2000-seed ceiling stays in the estimate) and the geometric
per-seed detection probability with its exact Clopper–Pearson interval. No means —
detection times are heavy-tailed and censored.

| mutant | strategy | campaigns | detected | median | [q25, q75] | p̂ per seed [95% CI] |
|---|---|---|---|---|---|---|
| `D1-skip-lock` | pct-d2 | 300 | 300 | 1 | [1, 1] | 1.000 [0.988, 1.000] |
| `D1-skip-lock` | pct-d3 | 300 | 300 | 1 | [1, 1] | 1.000 [0.988, 1.000] |
| `D1-skip-lock` | random | 300 | 300 | 1 | [1, 1] | 1.000 [0.988, 1.000] |
| `D2-early-lease-release` | pct-d2 | 300 | 300 | 1 | [1, 1] | 0.965 [0.938, 0.982] |
| `D2-early-lease-release` | pct-d3 | 300 | 300 | 1 | [1, 1] | 0.920 [0.885, 0.947] |
| `D2-early-lease-release` | random | 300 | 300 | 1 | [1, 2] | 0.661 [0.615, 0.704] |
| `D3-nonatomic-acquire` | pct-d2 | 300 | 300 | 1 | [1, 1] | 1.000 [0.988, 1.000] |
| `D3-nonatomic-acquire` | pct-d3 | 300 | 300 | 1 | [1, 1] | 1.000 [0.988, 1.000] |
| `D3-nonatomic-acquire` | random | 300 | 300 | 1 | [1, 1] | 1.000 [0.988, 1.000] |
| `D4-unmerged-remote-hlc` | pct-d2 | 300 | 300 | 1 | [1, 2] | 0.703 [0.657, 0.746] |
| `D4-unmerged-remote-hlc` | pct-d3 | 300 | 300 | 1 | [1, 2] | 0.704 [0.658, 0.747] |
| `D4-unmerged-remote-hlc` | random | 300 | 300 | 1 | [1, 2] | 0.683 [0.638, 0.727] |
| `D5-wall-clock-ordering` | pct-d2 | 300 | 300 | 1 | [1, 1] | 1.000 [0.988, 1.000] |
| `D5-wall-clock-ordering` | pct-d3 | 300 | 300 | 1 | [1, 1] | 1.000 [0.988, 1.000] |
| `D5-wall-clock-ordering` | random | 300 | 300 | 1 | [1, 1] | 1.000 [0.988, 1.000] |
| `I1-retry-without-key` | pct-d2 | 300 | 300 | 11 | [5, 22] | 0.063 [0.056, 0.070] |
| `I1-retry-without-key` | pct-d3 | 300 | 300 | 12 | [6, 20] | 0.064 [0.057, 0.071] |
| `I1-retry-without-key` | random | 300 | 300 | 10 | [4, 21] | 0.066 [0.059, 0.074] |
| `I2-naive-retry-loop` | pct-d2 | 300 | 300 | 13 | [6, 23] | 0.061 [0.055, 0.068] |
| `I2-naive-retry-loop` | pct-d3 | 300 | 300 | 11 | [4, 22] | 0.065 [0.058, 0.073] |
| `I2-naive-retry-loop` | random | 300 | 300 | 10 | [5, 22] | 0.065 [0.058, 0.073] |
| `I3-ack-before-processing` | pct-d2 | 300 | 300 | 2 | [1, 4] | 0.348 [0.317, 0.381] |
| `I3-ack-before-processing` | pct-d3 | 300 | 300 | 2 | [1, 4] | 0.367 [0.334, 0.401] |
| `I3-ack-before-processing` | random | 300 | 300 | 2 | [1, 4] | 0.377 [0.343, 0.412] |
| `M1-dual-write-shipment` | pct-d2 | 300 | 300 | 2 | [1, 3] | 0.464 [0.425, 0.503] |
| `M1-dual-write-shipment` | pct-d3 | 300 | 300 | 2 | [1, 3] | 0.441 [0.403, 0.479] |
| `M1-dual-write-shipment` | random | 300 | 300 | 1 | [1, 2] | 0.522 [0.480, 0.563] |
| `M2-consumer-without-inbox` | pct-d2 | 300 | 300 | 12 | [5, 24] | 0.061 [0.054, 0.068] |
| `M2-consumer-without-inbox` | pct-d3 | 300 | 300 | 10 | [4, 19] | 0.067 [0.060, 0.075] |
| `M2-consumer-without-inbox` | random | 300 | 300 | 12 | [6, 21] | 0.062 [0.056, 0.069] |
| `N1-drop-tenant-predicate` | pct-d2 | 300 | 300 | 1 | [1, 2] | 0.530 [0.488, 0.572] |
| `N1-drop-tenant-predicate` | pct-d3 | 300 | 300 | 2 | [1, 3] | 0.467 [0.428, 0.507] |
| `N1-drop-tenant-predicate` | random | 300 | 300 | 1 | [1, 2] | 0.522 [0.480, 0.563] |
| `N2-stale-cache` | pct-d2 | 300 | 300 | 1 | [1, 1] | 1.000 [0.988, 1.000] |
| `N2-stale-cache` | pct-d3 | 300 | 300 | 1 | [1, 1] | 1.000 [0.988, 1.000] |
| `N2-stale-cache` | random | 300 | 300 | 1 | [1, 1] | 1.000 [0.988, 1.000] |
| `N3-unbound-cursor-walk` | pct-d2 | 300 | 300 | 1 | [1, 1] | 1.000 [0.988, 1.000] |
| `N3-unbound-cursor-walk` | pct-d3 | 300 | 300 | 1 | [1, 1] | 1.000 [0.988, 1.000] |
| `N3-unbound-cursor-walk` | random | 300 | 300 | 1 | [1, 1] | 1.000 [0.988, 1.000] |
| `T1-blind-write-payment` | pct-d2 | 300 | 300 | 12 | [5, 21] | 0.062 [0.055, 0.069] |
| `T1-blind-write-payment` | pct-d3 | 300 | 300 | 10 | [5, 20] | 0.064 [0.057, 0.071] |
| `T1-blind-write-payment` | random | 300 | 300 | 13 | [5, 24] | 0.059 [0.052, 0.066] |
| `T2-charge-before-guard` | pct-d2 | 300 | 300 | 11 | [5, 22] | 0.063 [0.056, 0.070] |
| `T2-charge-before-guard` | pct-d3 | 300 | 300 | 10 | [5, 22] | 0.064 [0.057, 0.071] |
| `T2-charge-before-guard` | random | 300 | 300 | 11 | [5, 21] | 0.067 [0.060, 0.074] |
| `T3-double-torn` | pct-d2 | 300 | 300 | 118 | [53, 239] | 0.006 [0.005, 0.006] |
| `T3-double-torn` | pct-d3 | 300 | 300 | 77 | [34, 138] | 0.010 [0.009, 0.011] |
| `T3-double-torn` | random | 300 | 300 | 6 | [3, 11] | 0.125 [0.112, 0.139] |
| `T3-payment-outside-tx` | pct-d2 | 300 | 300 | 11 | [5, 22] | 0.063 [0.056, 0.070] |
| `T3-payment-outside-tx` | pct-d3 | 300 | 300 | 11 | [4, 22] | 0.059 [0.052, 0.065] |
| `T3-payment-outside-tx` | random | 300 | 300 | 13 | [5, 26] | 0.057 [0.050, 0.063] |
| `T3-torn-activation` | pct-d2 | 300 | 300 | 3 | [2, 5] | 0.244 [0.220, 0.269] |
| `T3-torn-activation` | pct-d3 | 300 | 300 | 3 | [2, 5] | 0.258 [0.233, 0.285] |
| `T3-torn-activation` | random | 300 | 300 | 3 | [1, 5] | 0.262 [0.237, 0.289] |
| `T4-weakened-oncall` | pct-d2 | 300 | 300 | 23 | [9, 42] | 0.034 [0.031, 0.038] |
| `T4-weakened-oncall` | pct-d3 | 300 | 300 | 23 | [11, 48] | 0.030 [0.027, 0.034] |
| `T4-weakened-oncall` | random | 300 | 300 | 24 | [11, 45] | 0.031 [0.028, 0.035] |
| `T5-unchecked-reservation` | pct-d2 | 300 | 300 | 11 | [5, 23] | 0.059 [0.053, 0.066] |
| `T5-unchecked-reservation` | pct-d3 | 300 | 300 | 11 | [5, 22] | 0.060 [0.054, 0.067] |
| `T5-unchecked-reservation` | random | 300 | 300 | 10 | [5, 22] | 0.065 [0.058, 0.072] |

## False positives (negative controls)

The harness's violation rate on known-correct code — the gate every external claim
stands on. `0` observed violations still carries an exact upper bound, never a bare
zero.

| control | strategy | runs | violations | rate upper bound (95%) |
|---|---|---|---|---|
| `ctrl-row-after-guard` | random | 400 | 0 | 0.0092 |
| `ctrl-row-after-guard` | pct-d2 | 400 | 0 | 0.0092 |
| `ctrl-row-after-guard` | pct-d3 | 400 | 0 | 0.0092 |
| `ctrl-row-before-guard-in-tx` | random | 400 | 0 | 0.0092 |
| `ctrl-row-before-guard-in-tx` | pct-d2 | 400 | 0 | 0.0092 |
| `ctrl-row-before-guard-in-tx` | pct-d3 | 400 | 0 | 0.0092 |
| `ctrl-atomic-provision` | random | 400 | 0 | 0.0092 |
| `ctrl-atomic-provision` | pct-d2 | 400 | 0 | 0.0092 |
| `ctrl-atomic-provision` | pct-d3 | 400 | 0 | 0.0092 |
| `ctrl-unique-reservation` | random | 400 | 0 | 0.0092 |
| `ctrl-unique-reservation` | pct-d2 | 400 | 0 | 0.0092 |
| `ctrl-unique-reservation` | pct-d3 | 400 | 0 | 0.0092 |
| `ctrl-retry-with-key` | random | 400 | 0 | 0.0092 |
| `ctrl-retry-with-key` | pct-d2 | 400 | 0 | 0.0092 |
| `ctrl-retry-with-key` | pct-d3 | 400 | 0 | 0.0092 |
| `ctrl-atomic-pair` | random | 400 | 0 | 0.0092 |
| `ctrl-atomic-pair` | pct-d2 | 400 | 0 | 0.0092 |
| `ctrl-atomic-pair` | pct-d3 | 400 | 0 | 0.0092 |
| `ctrl-idempotent-retry` | random | 400 | 0 | 0.0092 |
| `ctrl-idempotent-retry` | pct-d2 | 400 | 0 | 0.0092 |
| `ctrl-idempotent-retry` | pct-d3 | 400 | 0 | 0.0092 |
| `ctrl-serializable-oncall` | random | 400 | 0 | 0.0092 |
| `ctrl-serializable-oncall` | pct-d2 | 400 | 0 | 0.0092 |
| `ctrl-serializable-oncall` | pct-d3 | 400 | 0 | 0.0092 |
| `ctrl-merged-relay` | random | 400 | 0 | 0.0092 |
| `ctrl-merged-relay` | pct-d2 | 400 | 0 | 0.0092 |
| `ctrl-merged-relay` | pct-d3 | 400 | 0 | 0.0092 |
| `ctrl-floored-append` | random | 400 | 0 | 0.0092 |
| `ctrl-floored-append` | pct-d2 | 400 | 0 | 0.0092 |
| `ctrl-floored-append` | pct-d3 | 400 | 0 | 0.0092 |
| `ctrl-bound-cursor-walk` | random | 400 | 0 | 0.0092 |
| `ctrl-bound-cursor-walk` | pct-d2 | 400 | 0 | 0.0092 |
| `ctrl-bound-cursor-walk` | pct-d3 | 400 | 0 | 0.0092 |
| `ctrl-release-after-write` | random | 400 | 0 | 0.0092 |
| `ctrl-release-after-write` | pct-d2 | 400 | 0 | 0.0092 |
| `ctrl-release-after-write` | pct-d3 | 400 | 0 | 0.0092 |
| `ctrl-outbox-in-tx` | random | 400 | 0 | 0.0092 |
| `ctrl-outbox-in-tx` | pct-d2 | 400 | 0 | 0.0092 |
| `ctrl-outbox-in-tx` | pct-d3 | 400 | 0 | 0.0092 |
| `ctrl-process-then-ack` | random | 400 | 0 | 0.0092 |
| `ctrl-process-then-ack` | pct-d2 | 400 | 0 | 0.0092 |
| `ctrl-process-then-ack` | pct-d3 | 400 | 0 | 0.0092 |
| `ctrl-lock-protocol` | random | 400 | 0 | 0.0092 |
| `ctrl-lock-protocol` | pct-d2 | 400 | 0 | 0.0092 |
| `ctrl-lock-protocol` | pct-d3 | 400 | 0 | 0.0092 |
| `ctrl-tenant-filtered-browse` | random | 400 | 0 | 0.0092 |
| `ctrl-tenant-filtered-browse` | pct-d2 | 400 | 0 | 0.0092 |
| `ctrl-tenant-filtered-browse` | pct-d3 | 400 | 0 | 0.0092 |
| `ctrl-cache-invalidate-in-tx` | random | 400 | 0 | 0.0092 |
| `ctrl-cache-invalidate-in-tx` | pct-d2 | 400 | 0 | 0.0092 |
| `ctrl-cache-invalidate-in-tx` | pct-d3 | 400 | 0 | 0.0092 |
| `ctrl-inbox-consumer` | random | 400 | 0 | 0.0092 |
| `ctrl-inbox-consumer` | pct-d2 | 400 | 0 | 0.0092 |
| `ctrl-inbox-consumer` | pct-d3 | 400 | 0 | 0.0092 |

## p̂ versus the PCT bound (W3)

PCT with depth parameter ≥ d guarantees, **per trigger-carrying execution**, a schedule-
detection probability ≥ `1/(n·k^(d−1))`. The measured per-seed p̂ is a *product*:
p(workload carries the trigger) × p(schedule realizes it) — so the bound is compared
against the conditional `p̂_sched = p̂ / p_trigger`, with `p_trigger` taken from the
recorded regime structure (the collision pool; the two-rule workload mix). Mutants whose
trigger is a *fault* lottery (crash stream) or an uninstrumented workload-order lottery
are excluded — the theorem does not speak about them.

`n` and `k` are **measured per run** (distinct contending tasks; realized
ordering-choice ticks), folded per cell as maxima — the largest observed contention
gives the lowest, most conservative floor. The formal bound uses the PCT draw range
`steps=50` for `k` (the guarantee is over the range the change points are
*drawn* from, not the schedule that happened); the **k-tuned floor** column restates
the same guarantee had `steps` been set to the measured schedule length — the honest
decomposition of any looseness into draw-range slack versus residual conservatism.
A cell whose records predate the instrumentation falls back to the structural
estimates (workload concurrency; the draw range) and says so.

**Multiplicity.** The scan checks 31 cells and reports one violation count, so
a per-cell 95% interval would not be a 95% claim about the family — under the null that
the bound holds everywhere, the chance of at least one spurious flag grows past a coin
flip by ~15 cells, and a false alarm here sends a reviewer off to re-derive a correct
depth label. Each interval below is therefore computed at **99.8347% per cell**
(Šidák over 31), holding **95% family-wise** across the scan.

**Flip margin.** Uncertainty is propagated through `p̂` and through nothing else:
`p_trigger` is a structural constant, several of its values exact combinatorics, but all
of them derived from reviewed reasoning rather than measured. Respect holds iff
`p_trigger ≤ p̂_upper / bound`, so each cell carries the exact factor `F` by which
`p_trigger` would have to be understated for that cell's verdict to flip — no arbitrary
perturbation band to calibrate. A cell at `F = 40×` is immune to any plausible derivation
error; one at `F = 1.2×` is a single reviewed assumption away from a false alarm. Where
the flip would need `p_trigger > 1` it is **unreachable**, reported as such rather than as
a meaningless factor.

| mutant | d | strategy | n | k | p̂ per seed | p_trigger | p̂_sched | bound | respected | flip margin | k-tuned floor |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `D1-skip-lock` | 1 | pct-d2 | 3 | 5 | 1.000 | 1.000 | 1.00 | 0.3333 | yes | unreachable | 0.3333 (3× loose) |
| `D1-skip-lock` | 1 | pct-d3 | 3 | 5 | 1.000 | 1.000 | 1.00 | 0.3333 | yes | unreachable | 0.3333 (3× loose) |
| `D2-early-lease-release` | 1 | pct-d2 | 4 | 9 | 0.965 | 1.000 | 0.96 | 0.2500 | yes | unreachable | 0.2500 (4× loose) |
| `D2-early-lease-release` | 1 | pct-d3 | 4 | 9 | 0.920 | 1.000 | 0.92 | 0.2500 | yes | unreachable | 0.2500 (4× loose) |
| `D3-nonatomic-acquire` | 1 | pct-d2 | 3 | 7 | 1.000 | 1.000 | 1.00 | 0.3333 | yes | unreachable | 0.3333 (3× loose) |
| `D3-nonatomic-acquire` | 1 | pct-d3 | 3 | 7 | 1.000 | 1.000 | 1.00 | 0.3333 | yes | unreachable | 0.3333 (3× loose) |
| `D5-wall-clock-ordering` | 1 | pct-d2 | 4 | 4 | 1.000 | 1.000 | 1.00 | 0.2500 | yes | unreachable | 0.2500 (4× loose) |
| `D5-wall-clock-ordering` | 1 | pct-d3 | 4 | 4 | 1.000 | 1.000 | 1.00 | 0.2500 | yes | unreachable | 0.2500 (4× loose) |
| `I1-retry-without-key` | 1 | pct-d2 | 2 | 2 | 0.063 | 0.062 | 1.00 | 0.5000 | yes | 2.4× | 0.5000 (2× loose) |
| `I1-retry-without-key` | 1 | pct-d3 | 2 | 2 | 0.064 | 0.062 | 1.00 | 0.5000 | yes | 2.4× | 0.5000 (2× loose) |
| `I2-naive-retry-loop` | 1 | pct-d2 | 2 | 2 | 0.061 | 0.062 | 0.98 | 0.5000 | yes | 2.3× | 0.5000 (2× loose) |
| `I2-naive-retry-loop` | 1 | pct-d3 | 2 | 2 | 0.065 | 0.062 | 1.00 | 0.5000 | yes | 2.5× | 0.5000 (2× loose) |
| `M2-consumer-without-inbox` | 1 | pct-d2 | 2 | 2 | 0.061 | 0.062 | 0.97 | 0.5000 | yes | 2.3× | 0.5000 (2× loose) |
| `M2-consumer-without-inbox` | 1 | pct-d3 | 2 | 2 | 0.067 | 0.062 | 1.00 | 0.5000 | yes | 2.6× | 0.5000 (2× loose) |
| `N2-stale-cache` | 1 | pct-d2 | 2 | 2 | 1.000 | 1.000 | 1.00 | 0.5000 | yes | unreachable | 0.5000 (2× loose) |
| `N2-stale-cache` | 1 | pct-d3 | 2 | 2 | 1.000 | 1.000 | 1.00 | 0.5000 | yes | unreachable | 0.5000 (2× loose) |
| `N3-unbound-cursor-walk` | 1 | pct-d2 | 2 | 2 | 1.000 | 1.000 | 1.00 | 0.5000 | yes | unreachable | 0.5000 (2× loose) |
| `N3-unbound-cursor-walk` | 1 | pct-d3 | 2 | 2 | 1.000 | 1.000 | 1.00 | 0.5000 | yes | unreachable | 0.5000 (2× loose) |
| `T1-blind-write-payment` | 1 | pct-d2 | 2 | 5 | 0.062 | 0.062 | 0.99 | 0.5000 | yes | 2.3× | 0.5000 (2× loose) |
| `T1-blind-write-payment` | 1 | pct-d3 | 2 | 5 | 0.064 | 0.062 | 1.00 | 0.5000 | yes | 2.4× | 0.5000 (2× loose) |
| `T2-charge-before-guard` | 1 | pct-d2 | 2 | 4 | 0.063 | 0.062 | 1.00 | 0.5000 | yes | 2.4× | 0.5000 (2× loose) |
| `T2-charge-before-guard` | 1 | pct-d3 | 2 | 4 | 0.064 | 0.062 | 1.00 | 0.5000 | yes | 2.4× | 0.5000 (2× loose) |
| `T3-double-torn` | 3 | pct-d3 | 2 | 7 | 0.010 | 0.500 | 0.02 | 0.0002 | yes | unreachable | 0.0102 (2× loose) |
| `T3-payment-outside-tx` | 1 | pct-d2 | 2 | 5 | 0.063 | 0.062 | 1.00 | 0.5000 | yes | 2.4× | 0.5000 (2× loose) |
| `T3-payment-outside-tx` | 1 | pct-d3 | 2 | 5 | 0.059 | 0.062 | 0.94 | 0.5000 | yes | 2.2× | 0.5000 (2× loose) |
| `T3-torn-activation` | 2 | pct-d2 | 2 | 6 | 0.244 | 0.500 | 0.49 | 0.0100 | yes | unreachable | 0.0833 (6× loose) |
| `T3-torn-activation` | 2 | pct-d3 | 2 | 6 | 0.258 | 0.500 | 0.52 | 0.0100 | yes | unreachable | 0.0833 (6× loose) |
| `T4-weakened-oncall` | 1 | pct-d2 | 2 | 5 | 0.034 | 0.031 | 1.00 | 0.5000 | yes | 2.6× | 0.5000 (2× loose) |
| `T4-weakened-oncall` | 1 | pct-d3 | 2 | 5 | 0.030 | 0.031 | 0.96 | 0.5000 | yes | 2.3× | 0.5000 (2× loose) |
| `T5-unchecked-reservation` | 1 | pct-d2 | 2 | 4 | 0.059 | 0.062 | 0.95 | 0.5000 | yes | 2.3× | 0.5000 (2× loose) |
| `T5-unchecked-reservation` | 1 | pct-d3 | 2 | 4 | 0.060 | 0.062 | 0.96 | 0.5000 | yes | 2.3× | 0.5000 (2× loose) |

Excluded from the bound comparison (trigger is not a schedule lottery): `D4-unmerged-remote-hlc`, `I3-ack-before-processing`, `M1-dual-write-shipment`, `N1-drop-tenant-predicate`.

**Bound violations: 0** (family-wise 95% over 31 cells).

Reading: for every depth-1 cell the conditional schedule probability sits at ≈ 1 — once
the workload carries the trigger, essentially any schedule realizes it, consistent with
d=1 meaning zero ordering constraints (for d=1 the bound is `1/n` and `k` drops out).
The depth-2 cells are where the bound does real work, and the measured `k` decomposes
their looseness: the formal floor divides by the draw range
(`steps=50`), but the realized schedules are far shorter — the k-tuned
floor shows how much of the gap is draw-range slack (recoverable by setting `steps`
to the measured schedule length) versus PCT's residual conservatism. A violation
anywhere would have meant a wrong depth label or wrong n/k accounting — the first
(unconditioned) pass of this analysis produced exactly such false violations and was
corrected to the conditional form above.

`n` and `k` carry no interval on purpose. They are per-run measurements folded per cell
as **maxima** — a biased extreme-order statistic, but biased toward the lowest, most
conservative floor, which is the direction that cannot manufacture a violation. The
absence of an interval there is a decision, not an oversight.
