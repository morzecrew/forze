"""Detection statistics — clean-run exclusion bounds and the survival-analysis kernel.

Two layers, both dependency-free (stdlib ``math`` only — the testing extra stays lean, and
owning exactly the statistics we cite is part of the point):

**Clean-run verdicts.** A clean sweep of ``S`` independent seeds is not "no bugs"; it is an
*exclusion bound*: the largest per-seed detection probability still consistent with an all-clean
outcome at confidence ``γ`` is ``1 - (1 - γ) ** (1 / S)`` — the exact zero-event Clopper–Pearson
upper limit, whose mnemonic is the *rule of three* (``≈ 3 / S`` at 95%).
:func:`detection_upper_bound` computes it; :func:`format_clean_verdict` renders the one locked
sentence every clean-run surface prints, scope clause included ("this scenario × strategy ×
oracle set, independent seeds") — the number never travels without the claim it is scoped to.
Clopper–Pearson's exactness is a **fixed-design** guarantee, so a sweep that chose its own ``n``
by looking at the runs it is summarizing gets :func:`format_withheld_verdict` instead: the stop
reason, stated, and no number.

**The survival kernel** (for detection-time campaigns over the misuse corpus). Time-to-detection
is survival analysis with right censoring — a campaign that hit its seed ceiling without a
detection is *censored*, not averaged away, and means over heavy-tailed censored data are simply
wrong. The kernel: :meth:`SurvivalCurve.fit` (Kaplan–Meier over discrete seed counts, Greenwood
variance bands, medians/quantiles that honestly return ``None`` when the curve never reaches
them), :func:`log_rank` (the two-sample strategy comparison; χ²(1 df) tail via ``erfc``),
:func:`binomial_ci` (exact Clopper–Pearson, computed from the regularized incomplete beta —
:func:`detection_upper_bound` is its one-sided zero-event special case), and
:func:`geometric_p_hat` (the per-seed detection probability under the geometric model — the
bridge to per-run theoretical bounds; only meaningful for iid-seed strategies, never adaptive
ones), and :func:`fisher_exact` (the two-sided 2×2 exact test, for pre-registered contingency
questions like "does anomaly-level divergence predict bug-level divergence?").

**Discovery deficit.** Saturation is otherwise a boolean out of a heuristic with no uncertainty
attached — under a per-seed discovery probability of 10%, eight consecutive quiet seeds happen by
luck alone 43% of the time. :func:`coverage_deficit` puts a number on it: Good–Turing unseen mass
and the Chao1 lower bound on richness, over a frequency table of whatever features a sweep
collected.

**Multiplicity, in both directions.** :func:`sidak_level` is the per-comparison level that holds a
family-wise one: scanning many cells at 95% each and reporting the family as one verdict makes a
spurious flag likelier than not by ~15 cells. It corrects in the conservative direction for a scan,
and in the *permissive* one for :func:`format_family_verdict`, the opt-in simultaneous claim over
several clean sweeps that per-sweep non-aggregation otherwise leaves unsaid. :func:`flip_margin`
covers the remaining input no interval reaches — an assumed structural constant a comparison
divides by — by reporting the exact factor by which it would have to be wrong to flip the verdict.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from typing import Any, final

import attrs

# ----------------------- #


def detection_upper_bound(runs: int, *, confidence: float = 0.95) -> float:
    """The exact upper bound on per-seed detection probability after *runs* clean seeds.

    Zero-event Clopper–Pearson: ``1 - (1 - confidence) ** (1 / runs)`` — the largest per-seed
    detection probability under which an all-clean sweep of *runs* independent seeds is still
    plausible at the given *confidence*. ``3 / runs`` (the rule of three) is its 95% mnemonic;
    this returns the exact value.
    """

    if runs < 1:
        raise ValueError(f"runs must be >= 1, got {runs}")

    if not 0.0 < confidence < 1.0:
        raise ValueError(f"confidence must be in (0, 1), got {confidence}")

    return 1.0 - (1.0 - confidence) ** (1.0 / runs)


# ....................... #


def _render_probability(bound: float) -> str:
    """Percent with two decimals; scientific below display resolution (so it never shows 0.00%)."""

    return f"{bound:.2%}" if bound >= 0.0005 else f"{bound:.1e}"


# ....................... #


def _render_confidence(confidence: float) -> str:
    """Percent with trailing zeros trimmed, so 0.95 → ``95%`` but 0.999 → ``99.9%``, never ``100%``."""

    return f"{confidence:.4%}".removesuffix("%").rstrip("0").rstrip(".") + "%"


# ....................... #


def format_clean_verdict(
    runs: int,
    *,
    confidence: float = 0.95,
    witnessed: int | None = None,
    declared: Sequence[str] = (),
    unexercisable: Sequence[str] = (),
    unaccounted: Sequence[str] = (),
    weakest: tuple[str, int] | None = None,
    unmeasured_exposure: Sequence[str] = (),
) -> str:
    """The locked verdict a clean run prints instead of a bare "passed".

    One shared sentence — bound plus scope clause — so every surface (sweep, confidence report,
    coverage report, CLI, pytest summary) states exactly the same claim and never a stronger one.
    With invariant accounting (*witnessed* is set), the oracle-set clause becomes **countable**:
    it names how many invariants the sweep actually speaks about (those with a live
    falsifiability witness **this sweep's config could trigger**), which are declared
    out-of-horizon, which are witnessed only under perturbations the config did not enable
    (*unexercisable* — the bound does not cover them), and — should the gate ever be bypassed —
    which are unaccounted, so the claim can never silently cover an invariant the harness was
    never shown able to catch.

    The countable clause still divides by one ``runs`` for every invariant it names, and each was
    only at risk in some ``n_i <= runs`` of them. *weakest* is the narrowest such
    ``(invariant, n_i)``: it gets its own continuation line carrying **its** bound, because the
    aggregate is the number a reader will quote. *unmeasured_exposure* names invariants whose read
    footprint is opaque, so their exposure could not be counted — stated rather than folded into
    the aggregate at ``n = runs``.

    The aggregate stays, because it answers a real question ("did this sweep catch anything") and
    removing it would push readers to quote the weakest number as the sweep's result, overstating
    in the other direction.
    """

    bound = detection_upper_bound(runs, confidence=confidence)
    plural = "seeds" if runs != 1 else "seed"

    if witnessed is None:
        scope = "for this scenario × strategy × oracle set"
    else:
        scope = (
            f"for this scenario × strategy × the {witnessed} witnessed "
            f"{'invariant' if witnessed == 1 else 'invariants'}"
        )
        if declared:
            scope += f" ({len(declared)} declared out-of-horizon: {', '.join(declared)})"
        if unexercisable:
            scope += (
                f" (⚠ {len(unexercisable)} witnessed but UNEXERCISABLE under this config: "
                f"{', '.join(unexercisable)})"
            )
        if unaccounted:
            scope += f" (⚠ {len(unaccounted)} UNACCOUNTED: {', '.join(unaccounted)})"

    lines = [
        (
            f"0 violations in {runs} {plural} → per-seed detection probability "
            f"< {_render_probability(bound)} ({_render_confidence(confidence)}, exact) "
            f"{scope} (independent seeds)"
        )
    ]

    if weakest is not None:
        name, exposed = weakest
        lines.append(
            f"    ⚠ weakest coverage: {name} at risk in {exposed}/{runs} runs → "
            + (
                f"< {_render_probability(detection_upper_bound(exposed, confidence=confidence))} "
                "for that invariant alone"
                if exposed > 0
                else "the bound above does not cover it at all"
            )
        )

    if unmeasured_exposure:
        lines.append(
            f"    ⚠ unmeasured exposure: {', '.join(unmeasured_exposure)} — opaque read "
            "footprint, so how often each was at risk could not be counted"
        )

    return "\n".join(lines)


# ....................... #


def format_withheld_verdict(runs: int, *, stop_reason: str) -> str:
    """The locked verdict for a sweep whose ``n`` was **chosen from the data** — no bound.

    Clopper–Pearson exactness is a fixed-design guarantee: it assumes the seed count was fixed
    before the runs existed. Under a data-dependent stopping rule (the coverage sweep's plateau
    break) ``runs`` is a random variable whose value depends on the very runs being summarized,
    and the exact bound is not conservative in a known direction — it is simply a different,
    unstated design. So the sweep states what it did and prints no number: a wide bound reads as
    a real result in a way a withheld one does not.

    *stop_reason* names the rule that ended the sweep (e.g. ``"plateau stop"``).
    """

    if runs < 1:
        raise ValueError(f"runs must be >= 1, got {runs}")

    plural = "seeds" if runs != 1 else "seed"

    return (
        f"0 violations in {runs} {plural} → no per-seed bound: n was chosen from the data "
        f"({stop_reason}), and an exact bound is a fixed-design guarantee. Re-run the configured "
        f"pool with the early stop disabled (audit()) for a bound."
    )


# ----------------------- #
# The regularized incomplete beta — the one piece of real numerics, powering exact
# Clopper–Pearson. Continued fraction per Lentz's method (the standard dependency-free route);
# the inverse by bisection, which trades a few microseconds for zero magic constants.


def _betacf(a: float, b: float, x: float) -> float:
    """Lentz's continued fraction for the regularized incomplete beta."""

    max_iterations = 300
    eps = 3e-15
    floor = 1e-300  # keeps a vanishing denominator from dividing by zero

    qab, qap, qam = a + b, a + 1.0, a - 1.0
    c = 1.0
    d = 1.0 - qab * x / qap
    d = floor if abs(d) < floor else d
    d = 1.0 / d
    h = d

    for m in range(1, max_iterations + 1):
        m2 = 2 * m
        numerator = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + numerator * d
        d = floor if abs(d) < floor else d
        c = 1.0 + numerator / c
        c = floor if abs(c) < floor else c
        d = 1.0 / d
        h *= d * c

        numerator = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + numerator * d
        d = floor if abs(d) < floor else d
        c = 1.0 + numerator / c
        c = floor if abs(c) < floor else c
        d = 1.0 / d
        delta = d * c
        h *= delta

        if abs(delta - 1.0) < eps:
            return h

    raise ArithmeticError(  # pragma: no cover - defensive; Lentz converges on the CI domain
        f"incomplete beta did not converge for a={a}, b={b}, x={x}"
    )


def _betainc(a: float, b: float, x: float) -> float:
    """The regularized incomplete beta ``I_x(a, b)``."""

    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0

    log_front = (
        math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b) + a * math.log(x) + b * math.log1p(-x)
    )
    front = math.exp(log_front)

    # The continued fraction converges fast on one side of the mean; use the symmetry
    # I_x(a, b) = 1 - I_{1-x}(b, a) for the other.
    if x < (a + 1.0) / (a + b + 2.0):
        return front * _betacf(a, b, x) / a
    return 1.0 - front * _betacf(b, a, 1.0 - x) / b


def _beta_ppf(q: float, a: float, b: float) -> float:
    """The beta quantile (inverse of ``I_x(a, b)``) by bisection."""

    lo, hi = 0.0, 1.0
    for _ in range(200):
        mid = (lo + hi) / 2.0
        if _betainc(a, b, mid) < q:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2.0


def _norm_ppf(q: float) -> float:
    """The standard normal quantile by bisection on ``Φ(x) = erfc(-x/√2) / 2``."""

    lo, hi = -40.0, 40.0
    for _ in range(200):
        mid = (lo + hi) / 2.0
        if 0.5 * math.erfc(-mid / math.sqrt(2.0)) < q:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2.0


# ----------------------- #


@final
@attrs.frozen(kw_only=True)
class BinomialCi:
    """An exact (Clopper–Pearson) two-sided confidence interval for a binomial proportion."""

    lower: float
    upper: float
    confidence: float


def binomial_ci(successes: int, trials: int, *, confidence: float = 0.95) -> BinomialCi:
    """Exact Clopper–Pearson interval for ``successes`` out of ``trials``.

    Beta-quantile form: lower = ``BetaInv(α/2; k, n-k+1)``, upper = ``BetaInv(1-α/2; k+1, n-k)``,
    with the closed-form edges at ``k = 0`` / ``k = n``. Conservative by construction (coverage
    at least the nominal level). :func:`detection_upper_bound` is the one-sided zero-event
    special case:
    ``binomial_ci(0, n, confidence=0.90).upper == detection_upper_bound(n, confidence=0.95)``.
    """

    if trials < 1:
        raise ValueError(f"trials must be >= 1, got {trials}")
    if not 0 <= successes <= trials:
        raise ValueError(f"successes must be in [0, {trials}], got {successes}")
    if not 0.0 < confidence < 1.0:
        raise ValueError(f"confidence must be in (0, 1), got {confidence}")

    alpha = 1.0 - confidence
    k, n = successes, trials

    lower = 0.0 if k == 0 else _beta_ppf(alpha / 2.0, k, n - k + 1.0)
    upper = 1.0 if k == n else _beta_ppf(1.0 - alpha / 2.0, k + 1.0, float(n - k))

    return BinomialCi(lower=lower, upper=upper, confidence=confidence)


# ----------------------- #
# Kaplan–Meier over discrete time (seeds-to-first-detection), right-censored at the campaign
# ceiling. A censored campaign is information ("survived C seeds"), never a discard.


@final
@attrs.frozen(kw_only=True)
class SurvivalStep:
    """One event time on the curve: who was at risk, how many detected, and S(t) with its band."""

    time: int
    at_risk: int
    events: int
    survival: float
    lower: float
    upper: float


@final
@attrs.frozen(kw_only=True)
class SurvivalCurve:
    """The Kaplan–Meier estimate of P(still undetected after t seeds), with Greenwood bands."""

    steps: tuple[SurvivalStep, ...]
    n_runs: int
    n_events: int
    n_censored: int
    confidence: float

    # ....................... #

    @classmethod
    def fit(
        cls,
        events: Sequence[int],
        censored: Sequence[int] = (),
        *,
        confidence: float = 0.95,
    ) -> SurvivalCurve:
        """Fit the estimator: *events* are seeds-to-first-detection, *censored* are clean run
        lengths (the ceiling). Ties follow the standard convention — events precede censorings
        at the same time, so same-time censored runs still count as at risk."""

        if not events and not censored:
            raise ValueError("at least one observation (event or censored) is required")
        if any(t < 1 for t in (*events, *censored)):
            raise ValueError("observation times must be >= 1 (a first seed is time 1)")
        if not 0.0 < confidence < 1.0:
            raise ValueError(f"confidence must be in (0, 1), got {confidence}")

        z = _norm_ppf(1.0 - (1.0 - confidence) / 2.0)
        total = len(events) + len(censored)

        event_counts: dict[int, int] = {}
        for time in events:
            event_counts[time] = event_counts.get(time, 0) + 1
        removed: dict[int, int] = dict(event_counts)
        for time in censored:
            removed[time] = removed.get(time, 0) + 1

        survival = 1.0
        greenwood = 0.0
        at_risk = total
        steps: list[SurvivalStep] = []

        for time in sorted(removed):
            detected = event_counts.get(time, 0)
            if detected:
                survival *= 1.0 - detected / at_risk
                if at_risk > detected:
                    greenwood += detected / (at_risk * (at_risk - detected))
                se = survival * math.sqrt(greenwood)
                steps.append(
                    SurvivalStep(
                        time=time,
                        at_risk=at_risk,
                        events=detected,
                        survival=survival,
                        lower=max(0.0, survival - z * se),
                        upper=min(1.0, survival + z * se),
                    )
                )
            at_risk -= removed[time]

        return cls(
            steps=tuple(steps),
            n_runs=total,
            n_events=len(events),
            n_censored=len(censored),
            confidence=confidence,
        )

    # ....................... #

    def survival_at(self, time: int) -> float:
        """S(t): the estimated probability a campaign is still clean after *time* seeds."""

        survival = 1.0
        for step in self.steps:
            if step.time > time:
                break
            survival = step.survival
        return survival

    # ....................... #

    def quantile(self, q: float) -> int | None:
        """The smallest time by which a fraction *q* of campaigns has detected — ``None`` when
        the curve never falls that far (too much censoring to say; an honest non-answer)."""

        if not 0.0 < q < 1.0:
            raise ValueError(f"q must be in (0, 1), got {q}")

        for step in self.steps:
            if step.survival <= 1.0 - q:
                return step.time
        return None

    # ....................... #

    @property
    def median(self) -> int | None:
        """Median seeds-to-detection, or ``None`` when the curve never reaches 0.5."""

        return self.quantile(0.5)

    # ....................... #

    def format(self) -> str:
        """Render a compact step table — quantiles and bands, never a mean."""

        lines = [
            "Kaplan–Meier detection curve",
            f"  runs:     {self.n_runs} ({self.n_events} detected, {self.n_censored} censored)",
            f"  median:   {self.median if self.median is not None else '— (censored above 0.5)'}",
            f"  seed      at-risk  detected  S(t)   [{self.confidence:.0%} band]",
        ]
        lines.extend(
            f"  {step.time:<9} {step.at_risk:<8} {step.events:<9} "
            f"{step.survival:.3f}  [{step.lower:.3f}, {step.upper:.3f}]"
            for step in self.steps
        )
        return "\n".join(lines)


# ----------------------- #


@final
@attrs.frozen(kw_only=True)
class LogRankResult:
    """The two-sample log-rank test: χ²(1 df) statistic and its p-value."""

    statistic: float
    p_value: float


def log_rank(
    events_a: Sequence[int],
    censored_a: Sequence[int],
    events_b: Sequence[int],
    censored_b: Sequence[int],
) -> LogRankResult:
    """Compare two detection-time distributions (e.g. two strategies on one mutant).

    The standard Mantel–Cox form: at every distinct event time, the observed group-A detections
    are compared with their expectation under "no difference", variance from the hypergeometric;
    the summed discrepancy is χ²(1 df), with the tail computed exactly as ``erfc(√(x/2))``.
    """

    if not events_a and not events_b:
        raise ValueError("at least one detection in either group is required")

    times_a: dict[int, int] = {}
    for time in events_a:
        times_a[time] = times_a.get(time, 0) + 1
    times_b: dict[int, int] = {}
    for time in events_b:
        times_b[time] = times_b.get(time, 0) + 1

    observed_minus_expected = 0.0
    variance = 0.0

    for time in sorted(set(times_a) | set(times_b)):
        at_risk_a = sum(1 for t in events_a if t >= time) + sum(1 for t in censored_a if t >= time)
        at_risk_b = sum(1 for t in events_b if t >= time) + sum(1 for t in censored_b if t >= time)
        at_risk = at_risk_a + at_risk_b
        detected = times_a.get(time, 0) + times_b.get(time, 0)

        if at_risk == 0:  # pragma: no cover - defensive; an event time implies a subject at risk
            continue

        expected_a = detected * at_risk_a / at_risk
        observed_minus_expected += times_a.get(time, 0) - expected_a
        if at_risk > 1:
            variance += (
                detected
                * (at_risk_a / at_risk)
                * (at_risk_b / at_risk)
                * (at_risk - detected)
                / (at_risk - 1)
            )

    if variance == 0.0:
        return LogRankResult(statistic=0.0, p_value=1.0)

    statistic = observed_minus_expected**2 / variance
    return LogRankResult(statistic=statistic, p_value=math.erfc(math.sqrt(statistic / 2.0)))


# ----------------------- #


@final
@attrs.frozen(kw_only=True)
class GeometricEstimate:
    """The per-seed detection probability under the geometric model, with its exact CI."""

    p_hat: float
    ci: BinomialCi
    detections: int
    seeds: int


def geometric_p_hat(
    events: Sequence[int],
    censored: Sequence[int] = (),
    *,
    confidence: float = 0.95,
) -> GeometricEstimate:
    """MLE of the per-seed detection probability: detections over total seeds run.

    The geometric model is the bridge to per-run theoretical bounds — it assumes independent,
    identically-distributed seeds, so it is only meaningful for iid-seed strategies (random,
    PCT), **never adaptive ones** (coverage-guided), whose seed stream is not iid; report those
    via the assumption-free :class:`SurvivalCurve` only. The CI is Clopper–Pearson on
    (detections, total seeds) — exact under the binomial view of the inverse sampling, and a
    curve that visibly violates the geometric shape (plateaus, bimodality) should be flagged
    rather than force-fit.
    """

    if not events and not censored:
        raise ValueError("at least one observation (event or censored) is required")
    if any(t < 1 for t in (*events, *censored)):
        raise ValueError("observation times must be >= 1 (a first seed is time 1)")

    detections = len(events)
    seeds = sum(events) + sum(censored)

    return GeometricEstimate(
        p_hat=detections / seeds,
        ci=binomial_ci(detections, seeds, confidence=confidence),
        detections=detections,
        seeds=seeds,
    )


# ----------------------- #


@final
@attrs.frozen(kw_only=True)
class CoverageDeficit:
    """How much of a feature alphabet a sample has *not* seen — the measured form of saturation."""

    observed: int
    """Distinct features the sample actually contains."""

    total: int
    """Total observations (the sum of the frequency table — e.g. seeds swept)."""

    singletons: int
    """``f1`` — features seen exactly once. The whole signal: an alphabet still producing
    singletons is still producing surprises."""

    doubletons: int
    """``f2`` — features seen exactly twice."""

    unseen_mass: float
    """Good–Turing: ``f1 / N``, the estimated probability that the *next* observation is a feature
    never seen before. Zero means the sample stopped finding new things, not that none remain."""

    richness: float
    """Chao1's estimate of the true richness — a **lower** bound, which is the safe direction for
    a warning: it under-promises how much is left."""

    lower: float
    upper: float
    """The log-transformed Chao1 interval (Chao 1987). Collapses onto :attr:`observed` when the
    estimator has no signal to extrapolate from (``f1 <= 1`` with no doubletons)."""

    confidence: float

    # ....................... #

    @property
    def unseen(self) -> float:
        """Estimated features not yet seen: ``richness - observed`` (``0.0`` when saturated)."""

        return max(0.0, self.richness - self.observed)


def coverage_deficit(counts: Mapping[Any, int], *, confidence: float = 0.95) -> CoverageDeficit:
    """The unseen-feature deficit of a frequency table — Good–Turing mass plus Chao1 richness.

    *counts* maps each observed feature to how many observations contained it (e.g. execution-shape
    fingerprint → number of seeds that produced it). Species-richness estimation runs entirely on
    the *frequency of frequencies*: ``f1`` (seen once) and ``f2`` (seen twice). Chao1 is
    ``S_obs + f1²/(2·f2)``, falling back to the bias-corrected ``S_obs + f1·(f1−1)/2`` when no
    feature was seen exactly twice, with the standard log-transformed interval.

    **Which alphabet you feed it decides whether it says anything.** Measured across the misuse
    corpus at 200 seeds per mutant, the sweep's *behavioural* coverage alphabet (the unordered set
    of operation outcomes / port edges / fault kinds) has ``f1 == f2 == 0`` in every case — every
    behaviour that appears at all appears in many seeds, so Good–Turing reports zero unseen mass
    and Chao1 returns the observed count, permanently. On that alphabet this estimator is
    degenerate and would ship as a green number that can never go red. It is informative on the
    *ordered execution-shape* alphabet (``behavioral_fingerprint``), where the same corpus at 100
    seeds names a richness the sweep does not reach until 400 — and still returns exactly 1 on a
    genuinely single-shape workload, so it does not cry wolf.
    """

    if not counts:
        raise ValueError("at least one observed feature is required")
    if any(count < 1 for count in counts.values()):
        raise ValueError("every feature's count must be >= 1 (a zero count is not an observation)")
    if not 0.0 < confidence < 1.0:
        raise ValueError(f"confidence must be in (0, 1), got {confidence}")

    observed = len(counts)
    total = sum(counts.values())
    f1 = sum(1 for count in counts.values() if count == 1)
    f2 = sum(1 for count in counts.values() if count == 2)

    if f2 > 0:
        extra = f1 * f1 / (2.0 * f2)
        ratio = f1 / f2
        variance = f2 * (ratio**4 / 4.0 + ratio**3 + ratio**2 / 2.0)
    else:
        # Bias-corrected form: with no doubleton the classic estimator divides by zero, and a
        # lone singleton carries no extrapolation at all (f1·(f1−1) = 0).
        extra = f1 * (f1 - 1) / 2.0
        variance = (
            f1 * (f1 - 1) / 2.0 + f1 * (2 * f1 - 1) ** 2 / 4.0 - f1**4 / (4.0 * (observed + extra))
        )

    richness = observed + extra
    z = _norm_ppf(1.0 - (1.0 - confidence) / 2.0)

    if extra > 0.0 and variance > 0.0:
        # Chao's log transform keeps the interval on the right side of S_obs — a richness estimate
        # can never be below what was actually counted.
        spread = math.exp(z * math.sqrt(math.log1p(variance / (extra * extra))))
        lower, upper = observed + extra / spread, observed + extra * spread
    else:
        lower = upper = float(observed)

    return CoverageDeficit(
        observed=observed,
        total=total,
        singletons=f1,
        doubletons=f2,
        unseen_mass=f1 / total,
        richness=richness,
        lower=lower,
        upper=upper,
        confidence=confidence,
    )


# ....................... #


def sidak_level(confidence: float, comparisons: int) -> float:
    """The per-comparison confidence that holds *confidence* family-wise across *comparisons*.

    Šidák: with ``m`` independent statements each true at level ``γ'``, all ``m`` hold together
    with probability ``γ'^m``, so ``γ' = γ^(1/m)``. Scanning ``m`` cells at 95% each and reporting
    the family as one verdict is not a 95% claim — under the null that every cell is fine,
    ``P(≥1 spurious flag)`` is 22% at ``m = 5``, 54% at 15 and 95% at 60.

    Bonferroni's ``γ' = 1 − (1 − γ)/m`` is the union-bound approximation and is effectively
    identical at these ``m``; Šidák is exact under independence and no more code.
    """

    if comparisons < 1:
        raise ValueError(f"comparisons must be >= 1, got {comparisons}")
    if not 0.0 < confidence < 1.0:
        raise ValueError(f"confidence must be in (0, 1), got {confidence}")

    return confidence ** (1.0 / comparisons)


# ....................... #


def format_family_verdict(runs: Sequence[int], *, confidence: float = 0.95) -> str:
    """One **simultaneous** claim over several independent clean sweeps.

    Per-sweep bounds are deliberately never aggregated — a combined number would claim more than
    any single sweep established. That leaves a reader with no joint statement at all, though, and
    one is cheap: Šidák-correcting each sweep's level and quoting the widest of the corrected
    bounds costs 1.8–2.8× width and is true of every sweep at once.

    Opt-in by design: a family claim over unrelated scenarios is rarely the question anyone is
    asking, so it is offered rather than printed.
    """

    if not runs:
        raise ValueError("at least one sweep is required")

    level = sidak_level(confidence, len(runs))
    widest = max(detection_upper_bound(n, confidence=level) for n in runs)
    plural = "sweeps" if len(runs) != 1 else "sweep"

    return (
        f"0 violations across {len(runs)} {plural} → every sweep's per-seed detection probability "
        f"< {_render_probability(widest)} simultaneously "
        f"({_render_confidence(confidence)} family-wise, Šidák over {len(runs)})"
    )


# ....................... #


@final
@attrs.frozen(kw_only=True)
class FlipMargin:
    """How far an assumed constant would have to be wrong to flip a bound comparison's verdict."""

    factor: float
    """The multiplicative factor on the assumed constant that lands exactly on the verdict
    boundary. Above 1 on a respected cell: how far the constant would have to be *understated*
    for the cell to start reading as a violation. Below 1 on a violating cell: how far it would
    have to be *overstated* for the violation to disappear."""

    reachable: bool
    """Whether that factor is attainable at all. ``False`` when the constant is a probability and
    :attr:`factor` would push it above 1 — the verdict cannot flip for any admissible value, and
    reporting a numeric factor there would imply a probability greater than one."""


def flip_margin(*, observed_upper: float, bound: float, trigger: float) -> FlipMargin:
    """The exact sensitivity of ``p̂/trigger >= bound`` to the assumed *trigger*.

    A measured per-seed rate is a product — p(the workload carries the trigger) × p(the schedule
    realizes it) — so a bound on the schedule half is compared against ``p̂ / trigger``. Uncertainty
    is propagated through ``p̂`` (its exact interval) and through nothing else: *trigger* is a
    structural constant derived from reviewed reasoning about workload shape, and a mis-derived one
    produces exactly the false "violation" that sends a reviewer off to re-derive a correct label.

    Respect holds iff ``trigger <= observed_upper / bound``, so the margin is exact:
    ``F = (observed_upper / bound) / trigger``. A cell at ``F = 40×`` is immune to any plausible
    derivation error; a cell at ``F = 1.2×`` is one reviewed assumption away from a false alarm and
    should be read as such. No arbitrary perturbation band to calibrate.
    """

    if not 0.0 <= observed_upper <= 1.0:
        raise ValueError(f"observed_upper must be in [0, 1], got {observed_upper}")
    if not 0.0 < bound <= 1.0:
        raise ValueError(f"bound must be in (0, 1], got {bound}")
    if not 0.0 < trigger <= 1.0:
        raise ValueError(f"trigger must be in (0, 1], got {trigger}")

    boundary = observed_upper / bound

    return FlipMargin(factor=boundary / trigger, reachable=boundary <= 1.0)


# ....................... #


def format_coverage_deficit(deficit: CoverageDeficit) -> str:
    """The locked one-line rendering of a :class:`CoverageDeficit`, beside a sweep's stop reason.

    Always states both halves, because they answer different questions and can disagree honestly:
    Chao1's ``≥`` is how many distinct features are estimated to exist, and Good–Turing's percentage
    is how often the sample was still turning one up.
    """

    return (
        f"{deficit.observed} observed; ≥{math.ceil(deficit.richness)} estimated reachable "
        f"(Chao1, {_render_confidence(deficit.confidence)} CI "
        f"{math.floor(deficit.lower)}–{math.ceil(deficit.upper)}) — "
        f"{deficit.unseen_mass:.1%} of seeds still discovering"
    )


# ----------------------- #


def fisher_exact(table: tuple[tuple[int, int], tuple[int, int]]) -> float:
    """Two-sided Fisher exact test on a 2×2 contingency table; returns the p-value.

    The conditional exact test: with all margins fixed, the top-left count is hypergeometric,
    and the two-sided p-value sums the probabilities of every table at most as probable as the
    observed one (the standard "sum of small p" convention). Enumerated directly with
    ``math.comb`` — exact at the small counts this kernel exists for, no χ² approximation. A
    table with an empty margin carries no evidence either way and returns ``1.0``.
    """

    (a, b), (c, d) = table
    if min(a, b, c, d) < 0:
        raise ValueError(f"cell counts must be >= 0, got {table}")

    row1, col1, total = a + b, a + c, a + b + c + d
    if row1 in (0, total) or col1 in (0, total):
        return 1.0

    def probability(k: int) -> float:
        return math.comb(col1, k) * math.comb(total - col1, row1 - k) / math.comb(total, row1)

    observed = probability(a)
    lowest = max(0, row1 + col1 - total)
    highest = min(row1, col1)

    # Sum every table at most as probable as the observed one; the epsilon absorbs float
    # round-off so the observed table always counts itself.
    return min(
        1.0,
        sum(
            p
            for k in range(lowest, highest + 1)
            if (p := probability(k)) <= observed * (1.0 + 1e-9)
        ),
    )
