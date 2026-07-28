# Detection-time campaigns

Seeds-to-first-detection per (mutant, strategy): Kaplan–Meier quantiles (a campaign
censored at the 2000-seed ceiling stays in the estimate) and the geometric
per-seed detection probability with its exact Clopper–Pearson interval. No means —
detection times are heavy-tailed and censored.

| mutant | strategy | campaigns | detected | median | [q25, q75] | p̂ per seed [95% CI] |
|---|---|---|---|---|---|---|
| `I1-retry-without-key` | pct-d2 | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |
| `I1-retry-without-key` | pct-d3 | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |
| `I1-retry-without-key` | random | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |
| `M2-consumer-without-inbox` | pct-d2 | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |
| `M2-consumer-without-inbox` | pct-d3 | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |
| `M2-consumer-without-inbox` | random | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |
| `T1-blind-write-payment` | pct-d2 | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |
| `T1-blind-write-payment` | pct-d3 | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |
| `T1-blind-write-payment` | random | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |
| `T2-charge-before-guard` | pct-d2 | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |
| `T2-charge-before-guard` | pct-d3 | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |
| `T2-charge-before-guard` | random | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |
| `T3-payment-outside-tx` | pct-d2 | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |
| `T3-payment-outside-tx` | pct-d3 | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |
| `T3-payment-outside-tx` | random | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |
| `T5-unchecked-reservation` | pct-d2 | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |
| `T5-unchecked-reservation` | pct-d3 | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |
| `T5-unchecked-reservation` | random | 100 | 100 | 1 | [1, 1] | 1.000 [0.964, 1.000] |

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
| `ctrl-unique-reservation` | random | 400 | 0 | 0.0092 |
| `ctrl-unique-reservation` | pct-d2 | 400 | 0 | 0.0092 |
| `ctrl-unique-reservation` | pct-d3 | 400 | 0 | 0.0092 |
| `ctrl-retry-with-key` | random | 400 | 0 | 0.0092 |
| `ctrl-retry-with-key` | pct-d2 | 400 | 0 | 0.0092 |
| `ctrl-retry-with-key` | pct-d3 | 400 | 0 | 0.0092 |
| `ctrl-inbox-consumer` | random | 400 | 0 | 0.0092 |
| `ctrl-inbox-consumer` | pct-d2 | 400 | 0 | 0.0092 |
| `ctrl-inbox-consumer` | pct-d3 | 400 | 0 | 0.0092 |
