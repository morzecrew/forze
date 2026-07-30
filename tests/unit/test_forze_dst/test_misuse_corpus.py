"""The misuse-corpus smoke tier — the per-build merge guard over the harness's catching power.

Every mutant's killing seed must still kill (and the *expected* invariant must be the one that
fired — sharp attribution, not merely "something failed"); every control's clean band must stay
clean (the false-positive guard); and the registries must stay schema-complete. A fingerprint
drift fails loud: a killing seed found against a different operation catalog can no longer be
trusted to reproduce, so it must be re-mined, never silently quarantined.
"""

from __future__ import annotations

import importlib

import attrs
import pytest

from forze_dst import SimulationConfig
from forze_dst.misuse import MisuseCase, MisuseControl, MisuseMutant, TransferTier
from tests.support.misuse import CONTROLS, CORPUS, SMOKE_CONTROL_EXPLORE

# ----------------------- #


def _resolve(base: str) -> MisuseCase:
    module_name, _, attr = base.partition(":")
    factory = getattr(importlib.import_module(module_name), attr)
    case = factory()
    assert isinstance(case, MisuseCase)
    return case


def _config(explore: dict, seeds, case: MisuseCase) -> SimulationConfig:  # type: ignore[no-untyped-def, type-arg]
    return SimulationConfig(
        seeds=seeds,
        act_count=int(explore["act_count"]),
        concurrency=int(explore["concurrency"]),
        crash=case.crash,  # crash-fault instances run the crash → restart → recovery scenario
    )


# ....................... #


class TestRegistryCompleteness:
    def test_corpus_is_populated_and_ids_are_unique(self) -> None:
        assert len(CORPUS) >= 6
        ids = [mutant.mutant_id for mutant in CORPUS] + [c.control_id for c in CONTROLS]
        assert len(ids) == len(set(ids))

    def test_three_families_are_covered(self) -> None:
        assert len({mutant.family for mutant in CORPUS}) >= 3

    def test_control_ratio_is_at_least_one_to_three(self) -> None:
        assert len(CONTROLS) * 3 >= len(CORPUS)

    def test_adversarial_controls_exist(self) -> None:
        # The load-bearing half of the negative controls: misuse-shaped but correct.
        assert sum(1 for control in CONTROLS if control.adversarial) >= 2

    def test_every_mutant_is_replayable(self) -> None:
        for mutant in CORPUS:
            assert mutant.killing.target == mutant.base
            assert mutant.killing.registry_fingerprint, mutant.mutant_id
            assert mutant.killing.explore is not None, mutant.mutant_id
            assert mutant.depth_evidence, mutant.mutant_id

    def test_every_base_resolves_to_a_case(self) -> None:
        for entry in (*CORPUS, *CONTROLS):
            _resolve(entry.base)

    def test_campaign_regimes_are_complete_and_resolvable(self) -> None:
        # A campaign regime comes whole or not at all: pooled factory + its recorded knobs,
        # sharing the smoke factory's operation catalog (the fingerprint gate spans regimes).
        for mutant in CORPUS:
            assert (mutant.campaign_base is None) == (mutant.campaign_explore is None)
            if mutant.campaign_base is not None:
                case = _resolve(mutant.campaign_base)
                assert case.simulation.fingerprint() == mutant.killing.registry_fingerprint

    def test_not_transferable_requires_a_stated_reason(self) -> None:
        # The declared fraction travels with its reasons; a bare NOT_TRANSFERABLE would render
        # as an unexplained exclusion — the schema refuses it.
        undocumented = next(
            m for m in CORPUS if m.transfer_tier is TransferTier.NOT_TRANSFERABLE
        )

        with pytest.raises(ValueError, match="requires notes"):
            attrs.evolve(undocumented, notes="")

    def test_depth_labels_are_mechanical(self) -> None:
        for mutant in CORPUS:
            assert mutant.depth_evidence.startswith(("mechanical", "fault")), mutant.mutant_id


# ....................... #


@pytest.mark.parametrize("mutant", CORPUS, ids=lambda m: m.mutant_id)
class TestMutantsStillKill:
    def test_killing_seed_replays_and_the_expected_invariant_fires(
        self, mutant: MisuseMutant
    ) -> None:
        case = _resolve(mutant.base)

        # Fail loud on drift: a changed catalog means the stored seed proves nothing (re-mine).
        assert case.simulation.fingerprint() == mutant.killing.registry_fingerprint, (
            f"{mutant.mutant_id}: registry fingerprint drifted — re-mine the killing seed"
        )

        assert mutant.killing.explore is not None
        report = case.simulation.run(
            _config(mutant.killing.explore, [mutant.killing.seed], case), scenario=case.scenario
        )

        assert report is not None, f"{mutant.mutant_id}: the killing seed no longer kills"
        fired = {violation.invariant for violation in report.violations}
        assert fired & set(mutant.expected_invariants), (
            f"{mutant.mutant_id}: expected one of {mutant.expected_invariants}, got {sorted(fired)}"
        )


# ....................... #


@pytest.mark.parametrize("control", CONTROLS, ids=lambda c: c.control_id)
class TestControlsStayClean:
    def test_clean_band_reports_no_violation(self, control: MisuseControl) -> None:
        case = _resolve(control.base)

        report = case.simulation.run(
            _config(SMOKE_CONTROL_EXPLORE, range(*control.clean_band), case), scenario=case.scenario
        )

        assert report is None, (
            f"{control.control_id}: control violated at seed {report.seed} — "
            "a harness false positive (or a corpus authoring bug), fix before any external claim"
        )
