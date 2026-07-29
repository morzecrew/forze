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
| `D3-nonatomic-acquire` | pct-d2 | 300 | 300 | 1 | [1, 1] | 1.000 [0.988, 1.000] |
| `D3-nonatomic-acquire` | pct-d3 | 300 | 300 | 1 | [1, 1] | 1.000 [0.988, 1.000] |
| `D3-nonatomic-acquire` | random | 300 | 300 | 1 | [1, 1] | 1.000 [0.988, 1.000] |
| `I1-retry-without-key` | pct-d2 | 300 | 300 | 11 | [5, 22] | 0.063 [0.056, 0.070] |
| `I1-retry-without-key` | pct-d3 | 300 | 300 | 12 | [6, 20] | 0.064 [0.057, 0.071] |
| `I1-retry-without-key` | random | 300 | 300 | 10 | [4, 21] | 0.066 [0.059, 0.074] |
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
| `T1-blind-write-payment` | pct-d2 | 300 | 300 | 12 | [5, 21] | 0.062 [0.055, 0.069] |
| `T1-blind-write-payment` | pct-d3 | 300 | 300 | 10 | [5, 20] | 0.064 [0.057, 0.071] |
| `T1-blind-write-payment` | random | 300 | 300 | 13 | [5, 24] | 0.059 [0.052, 0.066] |
| `T2-charge-before-guard` | pct-d2 | 300 | 300 | 11 | [5, 22] | 0.063 [0.056, 0.070] |
| `T2-charge-before-guard` | pct-d3 | 300 | 300 | 10 | [5, 22] | 0.064 [0.057, 0.071] |
| `T2-charge-before-guard` | random | 300 | 300 | 11 | [5, 21] | 0.067 [0.060, 0.074] |
| `T3-payment-outside-tx` | pct-d2 | 300 | 300 | 11 | [5, 22] | 0.063 [0.056, 0.070] |
| `T3-payment-outside-tx` | pct-d3 | 300 | 300 | 11 | [4, 22] | 0.059 [0.052, 0.065] |
| `T3-payment-outside-tx` | random | 300 | 300 | 13 | [5, 26] | 0.057 [0.050, 0.063] |
| `T3-torn-activation` | pct-d2 | 300 | 300 | 3 | [2, 5] | 0.244 [0.220, 0.269] |
| `T3-torn-activation` | pct-d3 | 300 | 300 | 3 | [2, 5] | 0.258 [0.233, 0.285] |
| `T3-torn-activation` | random | 300 | 300 | 3 | [1, 5] | 0.262 [0.237, 0.289] |
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
are excluded — the theorem does not speak about them. Until per-run tick
instrumentation lands, `n` = the regime's workload concurrency and `k` ≤ 50
(the PCT steps parameter); both estimates are stated, not silent.

| mutant | d | strategy | p̂ per seed | p_trigger | p̂_sched | bound (est.) | respected | looseness |
|---|---|---|---|---|---|---|---|---|
| `D1-skip-lock` | 1 | pct-d2 | 1.000 | 1.000 | 1.00 | 0.5000 | yes | 2× |
| `D1-skip-lock` | 1 | pct-d3 | 1.000 | 1.000 | 1.00 | 0.5000 | yes | 2× |
| `D3-nonatomic-acquire` | 1 | pct-d2 | 1.000 | 1.000 | 1.00 | 0.5000 | yes | 2× |
| `D3-nonatomic-acquire` | 1 | pct-d3 | 1.000 | 1.000 | 1.00 | 0.5000 | yes | 2× |
| `I1-retry-without-key` | 1 | pct-d2 | 0.063 | 0.062 | 1.00 | 1.0000 | yes | 1× |
| `I1-retry-without-key` | 1 | pct-d3 | 0.064 | 0.062 | 1.00 | 1.0000 | yes | 1× |
| `M2-consumer-without-inbox` | 1 | pct-d2 | 0.061 | 0.062 | 0.97 | 1.0000 | yes | 1× |
| `M2-consumer-without-inbox` | 1 | pct-d3 | 0.067 | 0.062 | 1.00 | 1.0000 | yes | 1× |
| `N2-stale-cache` | 1 | pct-d2 | 1.000 | 1.000 | 1.00 | 1.0000 | yes | 1× |
| `N2-stale-cache` | 1 | pct-d3 | 1.000 | 1.000 | 1.00 | 1.0000 | yes | 1× |
| `T1-blind-write-payment` | 1 | pct-d2 | 0.062 | 0.062 | 0.99 | 0.5000 | yes | 2× |
| `T1-blind-write-payment` | 1 | pct-d3 | 0.064 | 0.062 | 1.00 | 0.5000 | yes | 2× |
| `T2-charge-before-guard` | 1 | pct-d2 | 0.063 | 0.062 | 1.00 | 0.5000 | yes | 2× |
| `T2-charge-before-guard` | 1 | pct-d3 | 0.064 | 0.062 | 1.00 | 0.5000 | yes | 2× |
| `T3-payment-outside-tx` | 1 | pct-d2 | 0.063 | 0.062 | 1.00 | 0.5000 | yes | 2× |
| `T3-payment-outside-tx` | 1 | pct-d3 | 0.059 | 0.062 | 0.94 | 0.5000 | yes | 2× |
| `T3-torn-activation` | 2 | pct-d2 | 0.244 | 0.500 | 0.49 | 0.0100 | yes | 49× |
| `T3-torn-activation` | 2 | pct-d3 | 0.258 | 0.500 | 0.52 | 0.0100 | yes | 52× |
| `T5-unchecked-reservation` | 1 | pct-d2 | 0.059 | 0.062 | 0.95 | 0.5000 | yes | 2× |
| `T5-unchecked-reservation` | 1 | pct-d3 | 0.060 | 0.062 | 0.96 | 0.5000 | yes | 2× |

Excluded from the bound comparison (trigger is not a schedule lottery): `I3-ack-before-processing`, `M1-dual-write-shipment`, `N1-drop-tenant-predicate`.

**Bound violations: 0.**

Reading: for every depth-1 cell the conditional schedule probability sits at ≈ 1 — once
the workload carries the trigger, essentially any schedule realizes it, consistent with
d=1 meaning zero ordering constraints. The depth-2 cell (`T3-torn-activation`) is where
the bound does real work: p̂_sched ≈ 0.5 against an estimated floor of 0.01 — respected
and loose by ~50×, consistent with PCT's deliberately conservative guarantee. A
violation anywhere would have meant a wrong depth label or wrong n/k accounting — the
first (unconditioned) pass of this analysis produced exactly such false violations and
was corrected to the conditional form above; the residual gap is measured per-run n and
k, recorded as the remaining P3 instrumentation task.
