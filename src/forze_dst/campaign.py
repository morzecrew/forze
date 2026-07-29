"""Detection-time campaigns — measure how fast each strategy finds each known bug.

For every ``(mutant, strategy)`` pair, run *N* independent campaigns: derive an iid trial-seed
stream from one master seed, run trials until the first detection or the censoring *ceiling*,
and record seeds-to-first-detection (a campaign that hits the ceiling is right-censored, never
discarded). The records feed the :mod:`forze_dst.stats` kernel — Kaplan–Meier curves, quantiles,
and the geometric per-seed detection probability with its exact interval; the false-positive
side runs the corpus's *negative controls* through the same strategies and reports the violation
rate with an exact interval (a harness that flags a known-correct control has a bug, and no
external claim survives a measurable false-positive rate).

Every mutant runs under its **own** recorded exploration knobs (the ``act_count``/``concurrency``
its killing entry carries) so a campaign measures the strategy, not a workload change; a corpus
whose catalog fingerprint drifted from the killing entry fails loud, exactly like the smoke tier.
Campaign wall time is measured and reported as a cost axis, but never enters the statistics.
"""

from __future__ import annotations

import importlib
import json
import time
from collections.abc import Sequence
from pathlib import Path
from typing import cast, final

import attrs

from forze.base.primitives import derive_seed
from forze_dst.config import SimulationConfig
from forze_dst.misuse import MisuseCase, MisuseControl, MisuseMutant
from forze_dst.runtime import ScheduleProfiler, profile_schedules
from forze_dst.scheduler import PCTScheduler, RandomScheduler, SchedulerSpec
from forze_dst.stats import BinomialCi, SurvivalCurve, binomial_ci, geometric_p_hat

# ----------------------- #


@final
@attrs.frozen(kw_only=True)
class CampaignStrategy:
    """One named exploration strategy a campaign sweeps under."""

    name: str
    scheduler: SchedulerSpec


DEFAULT_STRATEGIES: tuple[CampaignStrategy, ...] = (
    CampaignStrategy(name="random", scheduler=RandomScheduler()),
    CampaignStrategy(name="pct-d2", scheduler=PCTScheduler(depth=2)),
    CampaignStrategy(name="pct-d3", scheduler=PCTScheduler(depth=3)),
)
"""The iid-seed strategies (the geometric model applies). The adaptive coverage-guided strategy
is deliberately absent — its seed stream is not iid, so it needs its own campaign shape."""


# ....................... #


@final
@attrs.frozen(kw_only=True)
class CampaignRecord:
    """One campaign's outcome: seeds-to-first-detection, or censored at the ceiling."""

    mutant_id: str
    strategy: str
    campaign: int
    detection_trial: int | None
    """1-indexed trial of the first detection; ``None`` = censored (ran the ceiling clean)."""

    trials_run: int
    wall_seconds: float

    max_tasks: int | None = None
    """Largest number of distinct contending tasks any of this campaign's runs observed — the
    measured ``n`` of the PCT bound (``None`` on records written before instrumentation)."""

    max_choice_steps: int | None = None
    """Largest number of real ordering-choice ticks any run observed — the measured schedule
    length the PCT ``steps`` draw range is compared against."""

    # ....................... #

    def to_json(self) -> str:
        return json.dumps(
            {
                "kind": "campaign",
                "mutant_id": self.mutant_id,
                "strategy": self.strategy,
                "campaign": self.campaign,
                "detection_trial": self.detection_trial,
                "trials_run": self.trials_run,
                "wall_seconds": round(self.wall_seconds, 6),
                "max_tasks": self.max_tasks,
                "max_choice_steps": self.max_choice_steps,
            }
        )


# ....................... #


@final
@attrs.frozen(kw_only=True)
class FalsePositiveRecord:
    """One (control, strategy) band: how often the harness flagged known-correct code."""

    control_id: str
    strategy: str
    runs: int
    violations: int
    ci: BinomialCi

    # ....................... #

    def to_json(self) -> str:
        return json.dumps(
            {
                "kind": "false_positive",
                "control_id": self.control_id,
                "strategy": self.strategy,
                "runs": self.runs,
                "violations": self.violations,
                "ci_upper": self.ci.upper,
                "confidence": self.ci.confidence,
            }
        )


# ....................... #


def _resolve_case(base: str) -> MisuseCase:
    module_name, _, attr = base.partition(":")
    case = getattr(importlib.import_module(module_name), attr)()

    if not isinstance(case, MisuseCase):
        raise TypeError(f"{base!r} did not produce a MisuseCase")

    return case


def _knobs(explore: dict[str, object] | None, owner: str) -> tuple[int, int]:
    if explore is None:
        raise ValueError(f"{owner}: killing entry carries no explore knobs")

    return int(cast("int", explore["act_count"])), int(cast("int", explore["concurrency"]))


# ....................... #


def run_mutant_campaigns(
    mutant: MisuseMutant,
    *,
    strategies: Sequence[CampaignStrategy] = DEFAULT_STRATEGIES,
    campaigns: int,
    ceiling: int,
    master_seed: int = 0,
    explore: dict[str, object] | None = None,
) -> tuple[CampaignRecord, ...]:
    """Run *campaigns* independent detection-time campaigns per strategy for one mutant.

    Trial seeds derive from ``(master_seed, mutant, strategy, campaign, trial)``, so the whole
    dataset reproduces from one integer and campaigns are independent by construction. *explore*
    overrides the mutant's recorded workload knobs — the campaign regime may deliberately be
    **harder** than the kill-fast smoke regime (a saturated workload where every seed detects
    has no discriminating power between strategies); an override must be recorded in the run's
    meta, never silent.
    """

    if campaigns < 1 or ceiling < 1:
        raise ValueError("campaigns and ceiling must both be >= 1")

    # Prefer the mutant's campaign regime (the de-saturated collision-pool workload) when it
    # declares one; the kill-fast smoke regime is the fallback, and an explicit *explore*
    # override beats both. The campaign factory shares the smoke factory's operation catalog,
    # so the fingerprint gate holds across regimes.
    case = _resolve_case(mutant.campaign_base or mutant.base)
    if case.simulation.fingerprint() != mutant.killing.registry_fingerprint:
        raise RuntimeError(
            f"{mutant.mutant_id}: registry fingerprint drifted — re-mine before measuring"
        )

    act_count, concurrency = _knobs(
        explore or mutant.campaign_explore or mutant.killing.explore, mutant.mutant_id
    )
    records: list[CampaignRecord] = []

    for strategy in strategies:
        for index in range(campaigns):
            campaign_seed = derive_seed(master_seed, f"{mutant.mutant_id}/{strategy.name}/{index}")
            started = time.perf_counter()
            detection: int | None = None
            trials = 0
            profiler = ScheduleProfiler()

            # Profile every trial's schedule so the record carries the measured n / k the
            # PCT-bound analysis needs, instead of structural estimates.
            with profile_schedules(profiler):
                for trial in range(ceiling):
                    trials += 1
                    report = case.simulation.run(
                        SimulationConfig(
                            seeds=[derive_seed(campaign_seed, f"trial-{trial}")],
                            act_count=act_count,
                            concurrency=concurrency,
                            scheduler=strategy.scheduler,
                            crash=case.crash,
                        ),
                        scenario=case.scenario,
                    )
                    if report is not None:
                        detection = trial + 1
                        break

            records.append(
                CampaignRecord(
                    mutant_id=mutant.mutant_id,
                    strategy=strategy.name,
                    campaign=index,
                    detection_trial=detection,
                    trials_run=trials,
                    wall_seconds=time.perf_counter() - started,
                    max_tasks=profiler.max_tasks,
                    max_choice_steps=profiler.max_choice_steps,
                )
            )

    return tuple(records)


# ....................... #


def run_control_band(
    control: MisuseControl,
    *,
    explore: dict[str, object],
    strategies: Sequence[CampaignStrategy] = DEFAULT_STRATEGIES,
    runs: int,
    master_seed: int = 0,
) -> tuple[FalsePositiveRecord, ...]:
    """Run a known-correct control *runs* seeds per strategy; count harness false positives."""

    if runs < 1:
        raise ValueError("runs must be >= 1")

    case = _resolve_case(control.base)
    act_count, concurrency = _knobs(explore, control.control_id)
    records: list[FalsePositiveRecord] = []

    for strategy in strategies:
        violations = 0
        for index in range(runs):
            report = case.simulation.run(
                SimulationConfig(
                    seeds=[
                        derive_seed(master_seed, f"{control.control_id}/{strategy.name}/{index}")
                    ],
                    act_count=act_count,
                    concurrency=concurrency,
                    scheduler=strategy.scheduler,
                    crash=case.crash,
                ),
                scenario=case.scenario,
            )
            if report is not None:
                violations += 1

        records.append(
            FalsePositiveRecord(
                control_id=control.control_id,
                strategy=strategy.name,
                runs=runs,
                violations=violations,
                ci=binomial_ci(violations, runs),
            )
        )

    return tuple(records)


# ....................... #


def write_records(
    path: str | Path,
    *,
    campaigns: Sequence[CampaignRecord],
    false_positives: Sequence[FalsePositiveRecord] = (),
    meta: dict[str, object] | None = None,
) -> Path:
    """Write one JSONL file: a meta header line, then one line per record."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    lines = [json.dumps({"kind": "meta", **(meta or {})})]
    lines.extend(record.to_json() for record in campaigns)
    lines.extend(record.to_json() for record in false_positives)
    target.write_text("\n".join(lines) + "\n")
    return target


# ....................... #


def summarize(
    campaigns: Sequence[CampaignRecord],
    false_positives: Sequence[FalsePositiveRecord] = (),
    *,
    ceiling: int,
) -> str:
    """Render the campaign dataset as markdown — curves' quantiles and exact intervals, no means."""

    lines = [
        "# Detection-time campaigns",
        "",
        "Seeds-to-first-detection per (mutant, strategy): Kaplan–Meier quantiles (a campaign",
        f"censored at the {ceiling}-seed ceiling stays in the estimate) and the geometric",
        "per-seed detection probability with its exact Clopper–Pearson interval. No means —",
        "detection times are heavy-tailed and censored.",
        "",
        "| mutant | strategy | campaigns | detected | median | [q25, q75] | p̂ per seed [95% CI] |",
        "|---|---|---|---|---|---|---|",
    ]

    def _quantile(curve: SurvivalCurve, q: float) -> str:
        value = curve.quantile(q)
        return "—" if value is None else str(value)

    keys = sorted({(record.mutant_id, record.strategy) for record in campaigns})
    for mutant_id, strategy in keys:
        group = [r for r in campaigns if r.mutant_id == mutant_id and r.strategy == strategy]
        events = [r.detection_trial for r in group if r.detection_trial is not None]
        censored = [r.trials_run for r in group if r.detection_trial is None]
        curve = SurvivalCurve.fit(events, censored)
        estimate = geometric_p_hat(events, censored)

        lines.append(
            f"| `{mutant_id}` | {strategy} | {len(group)} | {len(events)} "
            f"| {_quantile(curve, 0.5)} | [{_quantile(curve, 0.25)}, {_quantile(curve, 0.75)}] "
            f"| {estimate.p_hat:.3f} [{estimate.ci.lower:.3f}, {estimate.ci.upper:.3f}] |"
        )

    if false_positives:
        lines += [
            "",
            "## False positives (negative controls)",
            "",
            "The harness's violation rate on known-correct code — the gate every external claim",
            "stands on. `0` observed violations still carries an exact upper bound, never a bare",
            "zero.",
            "",
            "| control | strategy | runs | violations | rate upper bound (95%) |",
            "|---|---|---|---|---|",
        ]
        lines.extend(
            f"| `{record.control_id}` | {record.strategy} | {record.runs} "
            f"| {record.violations} | {record.ci.upper:.4f} |"
            for record in false_positives
        )

    lines.append("")
    return "\n".join(lines)
