# RFC 0034 — DST uncertainty quantification: honest denominators

- **Status:** ✅ Executed — P0–P4 all landed, battery 1–12 green, every gate observed red once against its injected regression. OQ1 (retargeting the plateau stop) and OQ3/OQ4 resolved as their leans; OQ2's threshold calibrated at `distinct/S < 0.05` with a 50-seed floor, which separates the corpus's redundant end (1–3 shapes per 200 seeds) from its diverse one (19–55) with room either side. Two measurements were re-derived at pickup and both strengthened the case: the behaviour alphabet's `f1 = f2 = 0` holds for **all 18 corpus controls**, not just the six mutants sampled for the RFC, and the shipped W3 scan's tightest flip margins land at 2.2–2.6×.
- **Scope:** Five changes to what DST's clean-run surfaces claim, unified by one thesis: **every bound the plane prints divides by a number larger than what was actually tested.** P0 removes an exact fixed-`n` bound printed over a data-dependent stopping time (a live defect on a documented public path). P1 replaces the boolean `plateaued` with a measured discovery deficit (Good–Turing / Chao1 over the execution-shape alphabet). P2 makes the verdict's oracle-set clause carry **per-invariant** at-risk denominators instead of one shared `S`. P3 adds family-wise error control to the bound-respect scan, a seed-redundancy gap to the confidence report, and an exact flip margin for the one constant the PCT comparison divides by. No new dependencies; the estimator additions stay in `forze_dst.stats` under the existing stdlib-only rule.
- **Related:** the seed-statistics kernel supplies what every item below extends, and its rejection of adaptive stopping is the precedent P0 enforces rather than overturns. The unsimulatable-invariant detector's witness accounting is what P2 turns from a static set into a measured one. The DST fidelity work established the clean-verdict sentence; RFC 0007 supplies the corpus every number here was measured on. The fidelity-transfer leg is explicitly **out of scope** (§6).
- **Origin:** A read of the plane looking for unquantified uncertainty. The statistics kernel is sound and the discipline around it is unusually good — no means over censored data, honest `None` on unreachable quantiles, an explicit refusal to aggregate bounds across tests. The defects are not in the statistics; they are at the seams where a correctly-computed number is applied to a design it does not describe. Every claim below was measured against the RFC 0007 corpus, not reasoned about.

---

## 0. The thesis, and why these five are one RFC

A clean-run bound is a statement about a denominator. `detection_upper_bound(S)` says: *had the per-seed detection probability exceeded this, `S` independent chances would probably have caught it.* The number is only as good as the claim that there were `S` independent chances at the thing being bounded.

Six places break that claim, each in a different way:

| # | The denominator is `S`, but | Direction |
|---|---|---|
| P0 | `S` is a **stopping time**, chosen by looking at the data | claim too strong |
| P1 | saturation is asserted from an alphabet that saturates ~5× early; there is no denominator on the state space at all | claim unquantified |
| P2 | each invariant was at risk in `n_i ≤ S` runs; the verdict prints one `S` for all of them | claim too strong, per-invariant |
| P3a | `m` cells are scanned at 95% each and the family is reported as one verdict | false positives too likely |
| P3b | nothing simultaneous is offered where it is cheap | claim weaker than the evidence supports |
| P3c | `S` counts seeds, not distinct trials | claim too strong, magnitude unmeasured |

And one place where the denominator is not `S` at all: **P3d** — the W3 comparison divides by an assumed `p_trigger` whose own uncertainty is set to zero.

They belong in one RFC because fixing any one alone leaves the sentence "0 violations in S seeds → per-seed detection probability < X" still wrong for the remaining reasons, and because P1's estimator is what makes P3c measurable. They are phased independently because P0 is a defect and the rest are features.

---

## 1. P0 — a fixed-`n` exact bound is printed over a stopping time

### The defect

`CoverageStats.format()` prints `format_clean_verdict(self.seeds_run)`. Reached through `audit()` this is correct: `audit()` sets `coverage_plateau=0`, every configured seed runs, `n` is fixed before the data exists. Reached through `coverage()` it is not — the sweep breaks when `coverage_plateau` consecutive seeds add no new behavior, so `seeds_run` is a random variable whose value depends on the runs it is summarizing. The printed sentence still ends `(independent seeds)` and still says `exact`.

Clopper–Pearson's exactness is a fixed-design guarantee. Under a data-dependent stopping rule it is not conservative in a known direction — it is simply a different, unstated design.

### Measured

`T3-double-torn` under its recorded campaign regime, `coverage_plateau=8` (the shipped default):

```
plateau fires at seed 13
0 violations in 13 seeds → per-seed detection probability < 20.58% (95%, exact)
    for this scenario × strategy × oracle set (independent seeds)
```

The same evidence run to a 500-seed pool licenses `< 0.60%`. The gap is not the problem; the problem is that the 20.58% is labelled exact for a design that was never run.

This is not a hypothetical path. The exploration docs demonstrate exactly this call —
`simulation.coverage(SimulationConfig(seeds=range(500), coverage_plateau=8))` followed by
`print(stats.format())`.

### The decision

**The per-seed bound prints only under a fixed-`n` design.** When the sweep stopped early, `format()` states what happened and withholds the bound:

```
seeds run: 13  (saturated — plateau stop; no per-seed bound: n was chosen from the data)
```

This is the smallest correct change and it costs nothing real: `audit()` — the CI-gate path, and the one the CLI already routes through — is unaffected, because it has no plateau to hit.

### Why not an anytime-valid bound

The principled alternative is a confidence sequence: a bound valid simultaneously at every `n`, so any stopping rule is safe. It is the right general tool and it was priced rather than dismissed. Under simple `α/(n(n+1))` spending:

| n | fixed-`n` | anytime-valid | widening |
|---|---|---|---|
| 13 | 0.20582 | 0.46781 | 2.27× |
| 100 | 0.02951 | 0.11499 | 3.90× |
| 500 | 0.00597 | 0.03038 | 5.09× |

Peeking costs 2–6× width, and the alternative — run the pool you already configured — costs nothing. A tighter mixture-martingale sequence would beat the union bound, but not by enough to change the conclusion.

**If the early stop must count**, the correct design is sample splitting, recorded here so it is not re-derived: a pilot sweep chooses `N` from coverage, then `N` **disjoint** seeds supply the bound. The stopping rule is then measurable with respect to a filtration independent of the detection indicators, and the exact bound is recovered. This is deferred, not rejected — P1 makes it cheap by giving the pilot a real stopping criterion.

### Consistency with the seed-statistics protocol

The seed-statistics design rejected adaptive stopping for the campaign protocol: *"fixed-N with censoring keeps the protocol pre-registerable and the analysis simple; sequential designs invite garden-of-forking-paths critiques."* P0 does not overturn that decision — it applies it to the one surface that shipped a sequential design and printed a fixed-`n` number over it.

---

## 2. P1 — `plateaued: bool` becomes a measured discovery deficit

### The gap

Saturation is currently a boolean produced by a heuristic with no uncertainty attached. Under a per-seed discovery probability `u`, the chance that `K` consecutive seeds add nothing by luck alone is `(1−u)^K`:

| K | u=0.05 | u=0.10 | u=0.20 | u=0.30 |
|---|---|---|---|---|
| 4 | 0.815 | 0.656 | 0.410 | 0.240 |
| **8** (default) | 0.663 | **0.430** | 0.168 | 0.058 |
| 16 | 0.440 | 0.185 | 0.028 | 0.003 |

At the shipped default, a sweep still discovering new behavior on 10% of its seeds declares saturation **43% of the time**.

### The alphabet finding — and why the obvious version of this fails

`behavioral_coverage` is an unordered set of `(op, outcome)` / port-edge / fault tuples. Species-richness estimators need the frequency-of-frequencies (`f1` = features seen exactly once, `f2` = exactly twice). Measured across six corpus mutants in their campaign regimes at S=200, **`f1 = f2 = 0` in every case.** Good–Turing reports zero unseen mass; Chao1 returns the observed count. On this alphabet the estimator is degenerate and would ship as a permanently-green number — the same failure mode this program already hit once with the isolation-battery proxy.

The estimator becomes informative on the **ordered execution-shape alphabet** — `behavioral_fingerprint`, which already exists and is currently computed nowhere in the sweep path (it is used only for regression drift detection). Measured, over fingerprints:

| mutant | | S=50 | S=100 | S=200 | S=400 |
|---|---|---|---|---|---|
| T3-double-torn | observed | 24 | 42 | 55 | 65 |
| | **Chao1** | 78 | **68** | 67 | 66 |
| T3-torn-activation | observed | 15 | 19 | 19 | 19 |
| | **Chao1** | 21 | **19** | 19 | 19 |
| T5-unchecked-reservation | observed | 1 | 1 | 1 | 1 |
| | **Chao1** | **1** | 1 | 1 | 1 |

At S=100 the estimator names a richness (≈68) the sweep does not reach until S=400 — and it stays at 1 on the genuinely degenerate mutant, so it does not cry wolf. Chao1 is a **lower** bound on richness, which is the correct direction for a warning: it under-promises.

### What the two alphabets say when combined

The decisive measurement. Same mutants, plateau `K=8` on the shipped behavior alphabet:

| mutant | plateau fires | behaviors then | shapes then | shapes by seed 400 | explored |
|---|---|---|---|---|---|
| T3-double-torn | seed 13 | 17 (final 17) | 9 | 65 | **14%** |
| T3-torn-activation | seed 13 | 17 (final 17) | 7 | 19 | **37%** |

The behavior alphabet is genuinely saturated at seed 13 — the sweep is not lying about what it measured. It is measuring the wrong thing. And the signal to keep going was already in the data: Chao1 over the shapes seen by seed 13 estimated **34** against the 9 observed.

### The change

- `CoverageStats` retains a `Counter` over execution-shape fingerprints instead of discarding them, so the frequency-of-frequencies survives. `behaviors` stays a `frozenset` — it is the right structure for the reachability and vacuity questions it already serves.
- `forze_dst.stats` gains `coverage_deficit(counts)` → Good–Turing unseen mass `f1/N` and Chao1 richness with its interval. Stdlib, ~60 lines, consistent with the seed-statistics rule that "owning exactly the statistics we cite is part of the point."
- `format()` reports the deficit beside the stop reason:
  ```
  seeds run: 13  (saturated on behaviours)
  execution shapes: 9 observed; ≥34 estimated reachable (Chao1) — 8.4% of seeds still discovering
  ```
- The plateau rule itself is **unchanged in P1.** Retargeting the stop criterion onto the shape alphabet is a behavior change to a shipped default and is deliberately separated (§11 OQ1).

The degeneracy of the behavior alphabet is documented in the estimator's docstring — inline, naming the measurement — so the next reader does not re-derive it and reach the opposite conclusion.

---

## 3. P2 — the verdict's denominator becomes per-invariant

### The gap

The countable clause reads *"for this scenario × strategy × the N witnessed invariants."* `witnessed` is a **static** status: a mined witness exists and this config could trigger it. It says nothing about how many of the `S` runs actually put each invariant at risk, yet the single `S`-derived bound is scoped by name to all of them.

`HorizonProbe` accumulates `_present` as a global union of event kinds across the sweep. Vacuity is therefore binary — at risk in ≥1 run, or in none. There is no per-invariant, per-run count anywhere in the plane.

The arithmetic, at a 1000-seed sweep:

| invariant at risk in | verdict prints | its true bound |
|---|---|---|
| 1000 runs | < 0.299% | 0.299% |
| 300 runs | < 0.299% | 0.994% |
| 50 runs | < 0.299% | **5.816%** |
| 4 runs | < 0.299% | **52.7%** |

An invariant exposed on 4 of 1000 runs is covered by a sentence claiming 0.3%. It is the same shape of error as the recorded mock-server gate lesson — *an empty list is a successful answer* — and it is the arithmetic behind the audit's standing "verdict OVER-CLAIMS" finding.

### The change

`HorizonProbe` counts, per declared invariant, the runs whose recorded kinds intersect that invariant's read footprint. The footprint machinery is already there; only the count is new, and it is one `Counter` increment per invariant per run.

The verdict then carries the **weakest** member alongside the aggregate, because the aggregate is what a reader will quote:

```
0 violations in 1000 seeds → per-seed detection probability < 0.30% (95%, exact)
    for this scenario × strategy × the 7 witnessed invariants (independent seeds)
    ⚠ weakest coverage: no_lost_update at risk in 50/1000 runs → < 5.82% for that invariant alone
```

Invariants with an opaque footprint (those iterating `history.events` wholesale) are already never flagged by the vacuity analysis, and are reported here as **unmeasured exposure** rather than folded into the aggregate — the existing conservative posture, extended rather than reinterpreted.

---

## 4. P3 — multiplicity, in both directions

### P3a — the bound-respect scan has no family-wise control

`analyze_campaign.py` checks `p_sched_upper >= bound` per cell and reports `**Bound violations: N.**` across roughly 15 cells, each at 95%. Under the null that the bound holds everywhere, `P(≥1 spurious flag)`:

| m cells | 5 | 15 | 30 | 60 |
|---|---|---|---|---|
| P(≥1 spurious) | 0.226 | **0.537** | 0.785 | 0.954 |

This matters more than usual because of what the analysis says a violation *means*: *"A violation anywhere would have meant a wrong depth label or wrong n/k accounting."* A coin-flip-probability false alarm would send a reviewer to re-derive a correct depth label. The script already carries the scar of one such correction (the unconditioned first pass).

**Change:** Šidák the per-cell confidence to hold family-wise error at 5% across the cells actually scanned, and state both the per-cell and family-wise levels in the generated table. The count of cells is known at render time; nothing needs pre-registering that is not already fixed.

### P3b — nothing simultaneous is ever offered

The pytest plugin's refusal to aggregate is principled and should stay the default. But it leaves the reader with no joint statement at all, and one is cheap — a Šidák-corrected family verdict over `K` sweeps costs:

| K sweeps × 1000 seeds | per-sweep | simultaneous | widening |
|---|---|---|---|
| 10 | 0.00299 | 0.00526 | 1.76× |
| 50 | 0.00299 | 0.00686 | **2.29×** |
| 200 | 0.00299 | 0.00823 | 2.75× |

**Change:** the terminal summary keeps its per-sweep lines unchanged and adds one optional family line, printed only when every sweep in the session shares a confidence level. Opt-in, because a family claim over unrelated scenarios is rarely the question anyone is asking.

### P3c — seed redundancy as a confidence gap, not a corrected bound

Every bound treats `S` seeds as `S` independent chances. Distinct execution shapes per 200 seeds, measured across the corpus in campaign regimes:

| mutant | T1 | T4 | T2 | T3-outside-tx | T3-torn | T3-double-torn |
|---|---|---|---|---|---|---|
| distinct shapes / 200 | **1** | 2 | 3 | 3 | 19 | 55 |

A control (`ctrl-row-after-guard`) produced **2 distinct shapes across 300 seeds**. Were the effective count 120 of 1000, the honest bound would be 2.47% against a claimed 0.30% — 8× understated.

**This does not become a corrected denominator.** The fingerprint deliberately erases entity ids, and entity collision is exactly what the collision-pool regimes vary, so distinct-shape count is a coarse *lower* proxy for effective sample size. Substituting it into the bound would trade a known overstatement for an unknown understatement.

**Change:** a `ConfidenceReport` warning in the register the report already uses — the same voice as `never_raced` and `faults_never_fired`, both of which name a gap without repricing the bound:

```
⚠ 300 seeds explored 2 distinct execution shapes — the bound counts seeds, not distinct trials
```

The threshold for emitting it is a ratio, and the ratio's calibration is OQ2.

### P3d — the bound comparison is linear in an assumed constant

The W3 comparison divides out the trigger: `p̂_sched = p̂ / p_trigger`, and respect is `p̂_sched_upper >= bound`. Uncertainty is propagated through `p̂` (its exact interval) and **through nothing else**.

Two inputs are worth separating, because only one is a gap:

- **`n` and `k` are already handled.** They are per-run measurements folded per cell as **maxima**, and the generated methodology states the reason — *"the largest observed contention gives the lowest, most conservative floor."* A maximum is a biased extreme-order statistic with no interval, but the bias direction is the conservative one and it is published. **No change.** Recorded here so a later reader does not mistake the absence of an interval for an oversight and add one.
- **`p_trigger` is a structural constant carrying no uncertainty at all.** Several values are exact combinatorics (`1/pool`; `0.5` for one provision + one serve in two draws) and for those the constant is simply correct. But the comparison is *linear* in it, the values are derived from reviewed reasoning about workload structure rather than measured, and no cell states what would happen if one were wrong. A mis-derived `p_trigger` produces exactly the failure P3a exists to prevent — a spurious bound violation that sends a reviewer to re-derive a correct depth label.

**Change: report the flip margin, not a perturbation range.** Respect holds iff `p_trigger <= p̂_upper / bound`, so each cell has an exact multiplicative margin

```
F = (p̂_upper / bound) / p_trigger
```

— the factor by which `p_trigger` would have to be understated for that cell's verdict to flip. A cell at `F = 40×` is immune to any plausible derivation error; a cell at `F = 1.2×` is one reviewed assumption away from a false alarm and should be read as such. Where `F · p_trigger > 1` the flip is **unreachable**, since a probability cannot exceed 1, and the cell is reported as unconditionally respected rather than with a meaningless factor.

This is exact, needs no arbitrary perturbation band, and adds one column to a table the P3a change is already editing.

---

## 5. Where the numbers came from

Every measurement above is reproducible from the RFC 0007 corpus at its recorded campaign knobs (`act_count`/`concurrency` from `campaign_explore`, falling back to `killing.explore`), seeds `range(0, N)`, `schedule_seed = seed`, no scheduler override. The plateau measurements use the shipped `coverage_plateau=8`. P1 lands the estimator that makes them re-derivable from a sweep instead of from a bespoke probe, which is itself part of the point: none of this was observable from inside the plane's own reports.

---

## 6. What stays out

- **The fidelity-transfer leg.** Each transferable mutant runs once per backend with a deterministic verdict predicate; there is no repetition and no rate. The corpus is not a random sample of defects, so a binomial bound on "0 divergences in K mutants" would be a category error — a frequentist interval over an epistemic question. The existing write-up's *"untested, not confirmed"* is the better artifact, and the only change worth making there is prose that names the split explicitly. **No statistics are added to the transfer plane.**
- **Hierarchical / Bayesian pooling across mutants.** The seed-statistics design rules it out — *"sophistication is not evidence"* — and at 20 mutants it would add modelling assumptions without answering a question KM plus exact intervals cannot. That non-goal stands.
- **scipy / lifelines.** Unchanged from the seed-statistics design.
- **Retargeting the plateau stop onto the shape alphabet.** A behavior change to a shipped default; see OQ1.
- **Anything about `coverage_guided`.** `GuidedStats.format()` prints no bound today, which is correct — its lineage is adaptive by construction. It stays that way, and P0 must not be read as licensing one.

---

## 7. Battery

The gates, with the failure each is built to catch. Per the standing rule, **every gate ships with its regression injected once and observed failing** — a gate that has never gone red is an untested gate.

1. **A plateau-stopped sweep prints no per-seed bound**; the same config with `coverage_plateau=0` prints one. Injected regression: restore the unconditional print and watch the assertion fail. *(unit)*
2. **`audit()` is unaffected** — its verdict string is byte-identical before and after P0. *(unit)*
3. **Chao1 recovers a known richness**: a synthetic run-source with a fixed shape population, sampled below saturation, is estimated within its interval; sampled to exhaustion, the estimate equals the truth. *(unit)*
4. **The estimator does not cry wolf on a degenerate source** — a single-shape source yields richness 1 and zero deficit, no warning. *(unit)*
5. **The behavior-alphabet degeneracy is pinned**: `f1 == f2 == 0` over a corpus mutant's behavior coverage, asserted as the documented reason the estimator runs on shapes. A future alphabet change that makes this false should fail loudly and be re-reasoned, not silently improve. *(unit, corpus)*
6. **Per-invariant denominators are real**: a scenario with one invariant exercised on every run and one exercised on a known small subset produces two different bounds, and the weakest-member line names the right invariant. Injected regression: revert to the shared `S` and watch it fail. *(unit)*
7. **An opaque-footprint invariant reports unmeasured exposure**, and is not silently folded into the aggregate at `n = S`. *(unit)*
8. **Šidák family-wise level is applied to the cell count actually scanned**, verified against a hand-computed level for a fixed `m`. *(unit)*
9. **The redundancy warning fires** on a low-diversity control and stays silent on a high-diversity mutant. *(unit, corpus)*
10. **The bound is never repriced by the redundancy signal** — the printed bound for a fixed sweep is identical with the warning present and absent. This is the guard against P3c quietly becoming P3c-with-a-corrected-denominator. *(unit)*
11. **The flip margin is exact**: for a synthetic cell, scaling `p_trigger` by just under `F` keeps the verdict respected and just over `F` flips it. Injected regression: perturb the margin formula and watch the boundary move. *(unit)*
12. **An unreachable flip is reported as such**, never as a numeric factor implying a probability above 1. *(unit)*

---

## 8. Phases

- **P0** — the stopping-time fix + battery 1–2. **Not gated on this RFC's review.** It removes an unsound claim from a documented public path and is a `format()` conditional; holding it for the rest would be the wrong trade. The RFC records the reasoning rather than authorizing the change.
- **P1** — `coverage_deficit` in `stats.py`, the fingerprint `Counter` on `CoverageStats`, deficit reporting + battery 3–5. The largest item and the one that makes P3c measurable.
- **P2** — per-invariant at-risk counts in `HorizonProbe`, weakest-member verdict clause + battery 6–7. Directly retires a standing audit finding.
- **P3** — Šidák on the scan, opt-in family verdict, redundancy warning, flip-margin column + battery 8–12.
- **P4** — docs: the detection-statistics page gains the denominator section; the exploration page's `coverage()` example is corrected to show the withheld bound. The "what these numbers license" list gains the per-invariant and redundancy caveats. **The fidelity page gains the epistemic/frequentist split named in §6** — that the transfer leg's zero is not a sampling result and no interval will ever be attached to it. That clause is the entire deliverable of the transfer half of this RFC, and it lives here rather than in §6 so it cannot be dropped as "just prose".

P1 and P2 are independent; P3 depends on P1 only for the redundancy signal's input.

---

## 9. Decision log

| # | Decision | State |
|---|---|---|
| 1 | The per-seed bound prints **only under a fixed-`n` design**; a plateau-stopped sweep states its stop reason and withholds the bound | **locked** |
| 2 | Anytime-valid confidence sequences **priced and declined** — 2.3–5.1× width to buy a peek whose alternative (run the configured pool) is free | locked |
| 3 | Sample splitting (pilot chooses `N`, disjoint seeds bound it) is the recorded design **if** early stopping must ever count; deferred, not rejected | recorded |
| 4 | Coverage extrapolation runs on the **execution-shape alphabet**, not the behavior alphabet — the latter is measurably degenerate (`f1=f2=0` across the corpus) and would ship a permanently-green number | **locked** |
| 5 | Chao1's lower-bound property is a feature here: a richness warning that under-promises is the safe direction | locked |
| 6 | The plateau **stop rule is unchanged** in P1; only the reporting gains a deficit. Retargeting the stop is a default change (OQ1) | locked |
| 7 | The verdict carries a **weakest-member** per-invariant bound beside the aggregate, because the aggregate is what gets quoted | **locked** |
| 8 | Opaque-footprint invariants report **unmeasured exposure**, never `n = S` — extending the existing conservative posture | locked |
| 9 | Seed redundancy is a **warning, never a corrected denominator** — the fingerprint erases entity identity, which is the dimension the collision pools vary; substituting it trades a known overstatement for an unknown understatement | **locked** |
| 10 | The pytest plugin's per-sweep non-aggregation **stays the default**; the family verdict is opt-in | locked |
| 10a | **`n`/`k` maxima in the W3 comparison are left alone** — biased, but in the conservative direction, and the reason is already published. Recorded so the absence of an interval is not mistaken for an oversight | **locked** |
| 10b | `p_trigger`'s sensitivity is reported as an **exact flip margin** `F = (p̂_upper/bound)/p_trigger`, not a perturbation band — no arbitrary range to calibrate, and `F·p_trigger > 1` reports unreachable rather than a meaningless factor | **locked** |
| 11 | No statistics are added to the transfer plane — the question there is epistemic and the current prose is the better artifact. **The one prose clause this implies is assigned to P4**, not left in §6, so it cannot be dropped as optional | **locked** |
| 12 | The seed-statistics Bayesian non-goal and its scipy rejection both stand unchanged | recorded |
| 13 | All measurements re-derived from the RFC 0007 corpus at recorded knobs; re-verify at pickup if the corpus regimes move | recorded |

---

## 10. Alternatives considered

- **Fix P0 by widening the bound instead of withholding it.** Rejected: any widening factor honest enough to cover an arbitrary stopping rule is large enough (§1) that the number stops carrying information, and a wide bound reads as a real result in a way a withheld one does not.
- **Make `coverage()` an alias for `audit()`.** Rejected: the plateau stop is genuinely useful for exploration; the defect is the claim printed afterwards, not the early stop.
- **Ship Good–Turing on the behavior alphabet anyway, as "a start."** Rejected on the measurement — it is not a weak signal, it is a constant zero.
- **Correct the bound by effective sample size.** Rejected per decision 9. The proxy is coarse in an unquantified direction.
- **Bonferroni rather than Šidák.** Effectively identical at these `m`; Šidák is exact under independence and no more code.
- **Per-invariant bounds replacing the aggregate entirely.** Rejected: the aggregate answers a real question ("did this sweep catch anything") and removing it would push readers to quote the weakest number as the sweep's result, which overstates in the other direction.

---

## 11. Open questions

1. **Should the plateau stop retarget onto the shape alphabet?** The measurement says the behavior alphabet saturates ~5× early, which is an argument for it. Against: it changes a shipped default's compute profile substantially (seed 13 → seed 400 on `T3-double-torn`), and the shape alphabet's tail may not terminate on richer apps. *Lean: keep the behavior stop, add a deficit-based stop as an opt-in `coverage_deficit_target`, and measure both on the corpus before changing any default.*
2. **The redundancy warning's threshold.** A ratio of distinct shapes to seeds, but calibrated where? A control legitimately has low shape diversity. *Lean: warn on `distinct/S` below a threshold **and** `S` above a floor, so short sweeps stay quiet; calibrate against the corpus's own spread (1/200 to 55/200) rather than picking a round number.*
3. **Does the per-invariant count belong in the accounting or the horizon probe?** The footprint machinery is in `HorizonProbe`; the reported statuses are in `InvariantAccounting`. *Lean: count in the probe, report through the accounting — measurement and vocabulary stay where they already live.*
4. **Should P2's weakest-member line be a `warning` (gating `audit()`) or informational?** Making it gate would fail builds on sweeps that are honestly narrow. *Lean: informational in P2, with a threshold-based gate considered only after the corpus shows what typical exposure ratios look like.*
