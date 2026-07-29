# Detection-time campaigns

Seeds-to-first-detection per (mutant, strategy): Kaplan–Meier quantiles (a campaign
censored at the 2000-seed ceiling stays in the estimate) and the geometric
per-seed detection probability with its exact Clopper–Pearson interval. No means —
detection times are heavy-tailed and censored.

| mutant | strategy | campaigns | detected | median | [q25, q75] | p̂ per seed [95% CI] |
|---|---|---|---|---|---|---|
| `D1-skip-lock` | pct-d2 | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |
| `D1-skip-lock` | pct-d3 | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |
| `D1-skip-lock` | random | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |
| `D2-early-lease-release` | pct-d2 | 100 | 100 | 1 | [1, 1] | 0.952 [0.892, 0.984] |
| `D2-early-lease-release` | pct-d3 | 100 | 100 | 1 | [1, 1] | 0.917 [0.849, 0.962] |
| `D2-early-lease-release` | random | 100 | 100 | 1 | [1, 2] | 0.741 [0.658, 0.812] |
| `D3-nonatomic-acquire` | pct-d2 | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |
| `D3-nonatomic-acquire` | pct-d3 | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |
| `D3-nonatomic-acquire` | random | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |
| `D4-unmerged-remote-hlc` | pct-d2 | 100 | 100 | 1 | [1, 2] | 0.752 [0.670, 0.823] |
| `D4-unmerged-remote-hlc` | pct-d3 | 100 | 100 | 1 | [1, 2] | 0.680 [0.598, 0.755] |
| `D4-unmerged-remote-hlc` | random | 100 | 100 | 1 | [1, 2] | 0.629 [0.549, 0.704] |
| `D5-wall-clock-ordering` | pct-d2 | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |
| `D5-wall-clock-ordering` | pct-d3 | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |
| `D5-wall-clock-ordering` | random | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |
| `I1-retry-without-key` | pct-d2 | 100 | 100 | 11 | [6, 21] | 0.063 [0.052, 0.076] |
| `I1-retry-without-key` | pct-d3 | 100 | 100 | 12 | [6, 21] | 0.069 [0.056, 0.083] |
| `I1-retry-without-key` | random | 100 | 100 | 11 | [6, 24] | 0.062 [0.050, 0.074] |
| `I2-naive-retry-loop` | pct-d2 | 100 | 100 | 14 | [7, 23] | 0.061 [0.050, 0.074] |
| `I2-naive-retry-loop` | pct-d3 | 100 | 100 | 11 | [5, 23] | 0.057 [0.046, 0.069] |
| `I2-naive-retry-loop` | random | 100 | 100 | 11 | [5, 22] | 0.063 [0.052, 0.076] |
| `I3-ack-before-processing` | pct-d2 | 100 | 100 | 2 | [1, 4] | 0.345 [0.290, 0.403] |
| `I3-ack-before-processing` | pct-d3 | 100 | 100 | 2 | [1, 3] | 0.385 [0.325, 0.447] |
| `I3-ack-before-processing` | random | 100 | 100 | 2 | [1, 4] | 0.358 [0.302, 0.418] |
| `M1-dual-write-shipment` | pct-d2 | 100 | 100 | 1 | [1, 3] | 0.474 [0.405, 0.544] |
| `M1-dual-write-shipment` | pct-d3 | 100 | 100 | 2 | [1, 3] | 0.408 [0.346, 0.473] |
| `M1-dual-write-shipment` | random | 100 | 100 | 1 | [1, 2] | 0.529 [0.455, 0.602] |
| `M2-consumer-without-inbox` | pct-d2 | 100 | 100 | 12 | [5, 22] | 0.064 [0.053, 0.078] |
| `M2-consumer-without-inbox` | pct-d3 | 100 | 100 | 11 | [5, 22] | 0.061 [0.050, 0.073] |
| `M2-consumer-without-inbox` | random | 100 | 100 | 12 | [6, 22] | 0.060 [0.049, 0.073] |
| `N1-drop-tenant-predicate` | pct-d2 | 100 | 100 | 1 | [1, 2] | 0.529 [0.455, 0.602] |
| `N1-drop-tenant-predicate` | pct-d3 | 100 | 100 | 2 | [1, 3] | 0.422 [0.358, 0.488] |
| `N1-drop-tenant-predicate` | random | 100 | 100 | 1 | [1, 2] | 0.538 [0.463, 0.611] |
| `N2-stale-cache` | pct-d2 | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |
| `N2-stale-cache` | pct-d3 | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |
| `N2-stale-cache` | random | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |
| `N3-unbound-cursor-walk` | pct-d2 | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |
| `N3-unbound-cursor-walk` | pct-d3 | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |
| `N3-unbound-cursor-walk` | random | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |
| `T1-blind-write-payment` | pct-d2 | 100 | 100 | 12 | [5, 18] | 0.067 [0.055, 0.081] |
| `T1-blind-write-payment` | pct-d3 | 100 | 100 | 11 | [5, 18] | 0.071 [0.058, 0.085] |
| `T1-blind-write-payment` | random | 100 | 100 | 17 | [6, 29] | 0.052 [0.042, 0.062] |
| `T2-charge-before-guard` | pct-d2 | 100 | 100 | 10 | [5, 20] | 0.068 [0.056, 0.082] |
| `T2-charge-before-guard` | pct-d3 | 100 | 100 | 13 | [5, 25] | 0.059 [0.049, 0.072] |
| `T2-charge-before-guard` | random | 100 | 100 | 12 | [6, 23] | 0.061 [0.050, 0.074] |
| `T3-double-torn` | pct-d2 | 100 | 100 | 110 | [55, 240] | 0.006 [0.005, 0.007] |
| `T3-double-torn` | pct-d3 | 100 | 100 | 66 | [28, 142] | 0.010 [0.008, 0.012] |
| `T3-double-torn` | random | 100 | 100 | 7 | [3, 11] | 0.118 [0.097, 0.141] |
| `T3-payment-outside-tx` | pct-d2 | 100 | 100 | 11 | [5, 22] | 0.064 [0.053, 0.078] |
| `T3-payment-outside-tx` | pct-d3 | 100 | 100 | 12 | [4, 23] | 0.061 [0.050, 0.073] |
| `T3-payment-outside-tx` | random | 100 | 100 | 12 | [5, 22] | 0.059 [0.048, 0.072] |
| `T3-torn-activation` | pct-d2 | 100 | 100 | 3 | [2, 5] | 0.258 [0.215, 0.304] |
| `T3-torn-activation` | pct-d3 | 100 | 100 | 3 | [2, 5] | 0.251 [0.209, 0.297] |
| `T3-torn-activation` | random | 100 | 100 | 3 | [2, 5] | 0.272 [0.227, 0.320] |
| `T4-weakened-oncall` | pct-d2 | 100 | 100 | 24 | [11, 38] | 0.033 [0.027, 0.040] |
| `T4-weakened-oncall` | pct-d3 | 100 | 100 | 20 | [10, 45] | 0.032 [0.026, 0.038] |
| `T4-weakened-oncall` | random | 100 | 100 | 25 | [12, 46] | 0.029 [0.023, 0.035] |
| `T5-unchecked-reservation` | pct-d2 | 100 | 100 | 12 | [5, 26] | 0.054 [0.044, 0.065] |
| `T5-unchecked-reservation` | pct-d3 | 100 | 100 | 10 | [5, 20] | 0.058 [0.048, 0.070] |
| `T5-unchecked-reservation` | random | 100 | 100 | 13 | [5, 23] | 0.064 [0.053, 0.078] |

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
