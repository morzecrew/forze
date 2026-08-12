"""The alphabet choice behind the discovery deficit, pinned against the misuse corpus.

:func:`~forze_dst.stats.coverage_deficit` extrapolates from a frequency-of-frequencies — features
seen exactly once (``f1``) and exactly twice (``f2``). Which alphabet it is fed therefore decides
whether it says anything at all, and the choice was made from a measurement rather than from taste:

* On the sweep's **behavioural** coverage alphabet (the unordered set of operation outcomes, port
  edges and fault kinds) every behaviour that appears at all appears in many seeds. ``f1 == f2 == 0``,
  Good–Turing reports zero unseen mass, and Chao1 returns the observed count — for every corpus
  control, at every seed count. On that alphabet the estimator is not a weak signal; it is a
  constant zero that could never go red.
* On the **execution-shape** alphabet (``behavioral_fingerprint``) the same controls span one shape
  to dozens, with live singletons and doubletons, so the estimator both warns and stays quiet where
  it should.

These tests hold that measurement in place. A future change to either alphabet that makes the
degeneracy false should **fail loudly and be re-reasoned** — silently "improving" it would put the
estimator back on an alphabet nobody re-measured.
"""

from __future__ import annotations

import importlib
from collections import Counter

import pytest

from forze_dst import SimulationConfig
from forze_dst.misuse import MisuseCase, MisuseControl
from forze_dst.oracle.confidence import REDUNDANCY_MIN_SEEDS, ConfidenceReport
from forze_dst.oracle.coverage import Behavior
from forze_dst.stats import coverage_deficit
from tests.support.misuse import CONTROLS, SMOKE_CONTROL_EXPLORE

# ----------------------- #

_SEEDS = 30
"""Seeds per control. The degeneracy is not a small-sample artifact — it holds at 200 seeds per
mutant across the corpus (the RFC measurement); 30 is what a per-build tier can afford."""


def _resolve(base: str) -> MisuseCase:
    module_name, _, attr = base.partition(":")
    case = getattr(importlib.import_module(module_name), attr)()
    assert isinstance(case, MisuseCase)
    return case


def _alphabets(control: MisuseControl) -> tuple[Counter[Behavior], Counter[str]]:
    """Both alphabets for *control*, counted by how many seeds exhibited each feature.

    A control (not a mutant) on purpose: it stays clean by construction, so every seed in the pool
    actually runs instead of the sweep stopping at a violation.
    """

    case = _resolve(control.base)
    behaviours: Counter[Behavior] = Counter()
    shapes: Counter[str] = Counter()

    for seed in range(_SEEDS):
        stats = case.simulation.coverage(
            SimulationConfig(
                seeds=[seed],
                act_count=int(SMOKE_CONTROL_EXPLORE["act_count"]),  # type: ignore[arg-type]
                concurrency=int(SMOKE_CONTROL_EXPLORE["concurrency"]),  # type: ignore[arg-type]
                coverage_plateau=0,
                crash=case.crash,
            ),
            scenario=case.scenario,
        )
        behaviours.update(stats.behaviors)
        shapes.update(stats.shape_counts)

    return behaviours, shapes


def _redundancy_report(control_id: str) -> ConfidenceReport:
    """A confidence report over *control_id*, swept past the redundancy floor.

    Runs the whole pool in one sweep (so ``seeds_run`` and the shape table come from the same
    place) at a seed count above :data:`~forze_dst.oracle.confidence.REDUNDANCY_MIN_SEEDS` — the
    check is about the ratio threshold, not the floor.
    """

    control = next(c for c in CONTROLS if c.control_id == control_id)
    case = _resolve(control.base)

    stats = case.simulation.audit(
        SimulationConfig(
            seeds=range(REDUNDANCY_MIN_SEEDS + 10),
            act_count=int(SMOKE_CONTROL_EXPLORE["act_count"]),  # type: ignore[arg-type]
            concurrency=int(SMOKE_CONTROL_EXPLORE["concurrency"]),  # type: ignore[arg-type]
            crash=case.crash,
        ),
        scenario=case.scenario,
    )

    assert stats.violation is None, f"{control_id} is a control and must sweep clean"
    assert stats.confidence is not None

    return stats.confidence


# ....................... #


@pytest.fixture(scope="module")
def measured() -> dict[str, tuple[Counter[Behavior], Counter[str]]]:
    """Both alphabets for every corpus control, measured once for the whole module."""

    return {control.control_id: _alphabets(control) for control in CONTROLS}


# ....................... #


class TestBehaviourAlphabetIsDegenerate:
    def test_no_control_produces_a_singleton_or_doubleton_behaviour(
        self, measured: dict[str, tuple[Counter[Behavior], Counter[str]]]
    ) -> None:
        # The documented reason the estimator does not run on this alphabet. If this ever fails,
        # re-derive the choice — do not just point the estimator at behaviours because it "works
        # now".
        offenders = {
            control_id: (
                sum(1 for n in behaviours.values() if n == 1),
                sum(1 for n in behaviours.values() if n == 2),
            )
            for control_id, (behaviours, _) in measured.items()
            if any(n <= 2 for n in behaviours.values())
        }

        assert not offenders, (
            "the behaviour alphabet is no longer degenerate for "
            f"{sorted(offenders)} — re-measure before trusting a deficit over it"
        )

    def test_the_estimator_is_therefore_a_constant_zero_over_behaviours(
        self, measured: dict[str, tuple[Counter[Behavior], Counter[str]]]
    ) -> None:
        for control_id, (behaviours, _) in measured.items():
            deficit = coverage_deficit(behaviours)

            assert deficit.unseen_mass == 0.0, control_id
            assert deficit.richness == float(deficit.observed), control_id
            assert (deficit.lower, deficit.upper) == (
                float(deficit.observed),
                float(deficit.observed),
            ), control_id


# ....................... #


class TestShapeAlphabetIsInformative:
    def test_some_controls_carry_a_live_deficit(
        self, measured: dict[str, tuple[Counter[Behavior], Counter[str]]]
    ) -> None:
        # The positive half: on shapes the estimator names richness the sweep has not reached.
        naming_a_deficit = {
            control_id
            for control_id, (_, shapes) in measured.items()
            if coverage_deficit(shapes).unseen > 0.0
        }

        assert naming_a_deficit, "the shape alphabet produced no deficit anywhere — re-measure"

    def test_and_stays_silent_on_the_genuinely_single_shape_ones(
        self, measured: dict[str, tuple[Counter[Behavior], Counter[str]]]
    ) -> None:
        # The negative half: a workload that really does drive one path is not accused of hiding
        # anything. Both halves matter — an estimator that always warns is as useless as one that
        # never does.
        single_shape = {
            control_id
            for control_id, (_, shapes) in measured.items()
            if len(shapes) == 1
        }

        assert single_shape, "no single-shape control left to prove the estimator stays quiet"
        for control_id in single_shape:
            deficit = coverage_deficit(measured[control_id][1])

            assert deficit.richness == 1.0, control_id
            assert deficit.unseen == 0.0, control_id

    def test_the_redundancy_threshold_discriminates_on_real_corpus_sweeps(self) -> None:
        # The calibration claim, on corpus data rather than synthetic ratios: the threshold has to
        # fire on a genuinely single-shape control and stay silent on a diverse one, at a seed
        # count above the floor. A threshold that did neither would be a constant dressed up as a
        # measurement.
        loud = _redundancy_report("ctrl-unique-reservation")
        quiet = _redundancy_report("ctrl-atomic-provision")

        assert loud.redundant_seeds, "a single-shape control did not trip the redundancy warning"
        assert not quiet.redundant_seeds, "a diverse control tripped it — the threshold is noise"
        assert len(loud.shape_counts) < len(quiet.shape_counts)

    def test_the_two_alphabets_disagree_about_saturation(
        self, measured: dict[str, tuple[Counter[Behavior], Counter[str]]]
    ) -> None:
        # The decisive comparison: the same runs, saturated on one alphabet and demonstrably not
        # on the other. This is why the plateau flag alone overstates what a sweep explored.
        disagreements = [
            control_id
            for control_id, (behaviours, shapes) in measured.items()
            if coverage_deficit(behaviours).unseen == 0.0 and coverage_deficit(shapes).unseen > 0.0
        ]

        assert disagreements, (
            "no control shows the behaviour alphabet saturating while shapes keep appearing — "
            "the premise of reporting a shape deficit beside the plateau flag no longer holds"
        )
