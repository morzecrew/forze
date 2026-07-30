"""The campaign engine — reproducible detection-time measurement over the misuse corpus.

Small-N runs over real corpus instances (the fast d=1 mutant and one control), so the tests pin
the engine's semantics — censoring, determinism from the master seed, fingerprint fail-loud,
JSONL shape, and the no-means summary — without paying pilot-sized wall time.
"""

from __future__ import annotations

import json

import attrs
import pytest

from forze_dst.campaign import (
    CampaignStrategy,
    run_control_band,
    run_mutant_campaigns,
    summarize,
    write_records,
)
from forze_dst.scheduler import RandomScheduler
from tests.support.misuse import CONTROLS, CORPUS, SMOKE_CONTROL_EXPLORE

# ----------------------- #

_RANDOM_ONLY = (CampaignStrategy(name="random", scheduler=RandomScheduler()),)
_I1 = next(m for m in CORPUS if m.mutant_id == "I1-retry-without-key")
_CONTROL = next(c for c in CONTROLS if c.control_id == "ctrl-retry-with-key")


class TestMutantCampaigns:
    def test_records_detections_and_censors_at_the_ceiling(self) -> None:
        records = run_mutant_campaigns(
            _I1, strategies=_RANDOM_ONLY, campaigns=4, ceiling=6, master_seed=0
        )

        assert len(records) == 4
        for record in records:
            assert record.mutant_id == _I1.mutant_id
            if record.detection_trial is None:
                assert record.trials_run == 6  # censored = ran the full ceiling clean
            else:
                assert 1 <= record.detection_trial == record.trials_run <= 6
            # The measured schedule profile rides on every record — the bound analysis
            # consumes these instead of structural estimates.
            assert record.max_tasks is not None and record.max_tasks >= 1
            assert record.max_choice_steps is not None and record.max_choice_steps >= 1
        # The d=1 mutant detects fast — the small band must produce at least one detection.
        assert any(record.detection_trial is not None for record in records)

    def test_the_master_seed_reproduces_the_dataset(self) -> None:
        first = run_mutant_campaigns(
            _I1, strategies=_RANDOM_ONLY, campaigns=3, ceiling=4, master_seed=7
        )
        second = run_mutant_campaigns(
            _I1, strategies=_RANDOM_ONLY, campaigns=3, ceiling=4, master_seed=7
        )

        strip = [attrs.evolve(record, wall_seconds=0.0) for record in first]
        assert strip == [attrs.evolve(record, wall_seconds=0.0) for record in second]

    def test_fingerprint_drift_fails_loud(self) -> None:
        drifted = attrs.evolve(
            _I1, killing=attrs.evolve(_I1.killing, registry_fingerprint="sha256:not-this")
        )

        with pytest.raises(RuntimeError, match="fingerprint drifted"):
            run_mutant_campaigns(drifted, strategies=_RANDOM_ONLY, campaigns=1, ceiling=1)


class TestControlBand:
    def test_clean_control_reports_zero_with_an_exact_upper_bound(self) -> None:
        (record,) = run_control_band(
            _CONTROL,
            explore=dict(SMOKE_CONTROL_EXPLORE),
            strategies=_RANDOM_ONLY,
            runs=12,
            master_seed=0,
        )

        assert record.violations == 0
        assert record.ci.lower == 0.0
        assert 0.0 < record.ci.upper < 1.0  # never a bare zero — the band size is the claim


class TestArtifactsAndSummary:
    def test_jsonl_round_trip_with_meta_header(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        records = run_mutant_campaigns(
            _I1, strategies=_RANDOM_ONLY, campaigns=2, ceiling=3, master_seed=0
        )

        path = write_records(
            tmp_path / "campaigns.jsonl", campaigns=records, meta={"master_seed": 0}
        )

        lines = [json.loads(line) for line in path.read_text().splitlines()]
        assert lines[0]["kind"] == "meta"
        assert lines[0]["master_seed"] == 0
        assert [line["kind"] for line in lines[1:]] == ["campaign", "campaign"]
        assert lines[1]["mutant_id"] == _I1.mutant_id
        assert lines[1]["max_tasks"] >= 1
        assert lines[1]["max_choice_steps"] >= 1

    def test_summary_has_quantiles_and_intervals_never_means(self) -> None:
        records = run_mutant_campaigns(
            _I1, strategies=_RANDOM_ONLY, campaigns=4, ceiling=6, master_seed=0
        )
        fp = run_control_band(
            _CONTROL,
            explore=dict(SMOKE_CONTROL_EXPLORE),
            strategies=_RANDOM_ONLY,
            runs=8,
            master_seed=0,
        )

        out = summarize(records, fp, ceiling=6)

        assert "| `I1-retry-without-key` | random | 4 |" in out
        assert "p̂ per seed [95% CI]" in out
        assert "rate upper bound (95%)" in out
        assert "| mean" not in out  # no mean column exists — quantiles and intervals only


class TestValidationAndEdges:
    def test_campaign_counts_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match=">= 1"):
            run_mutant_campaigns(_I1, strategies=_RANDOM_ONLY, campaigns=0, ceiling=5)

    def test_control_runs_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match=">= 1"):
            run_control_band(
                _CONTROL, explore=dict(SMOKE_CONTROL_EXPLORE), strategies=_RANDOM_ONLY, runs=0
            )

    def test_base_must_produce_a_misuse_case(self) -> None:
        broken = attrs.evolve(
            _I1, base="builtins:dict", campaign_base=None, campaign_explore=None,
            killing=attrs.evolve(_I1.killing, target="builtins:dict"),
        )

        with pytest.raises(TypeError, match="did not produce a MisuseCase"):
            run_mutant_campaigns(broken, strategies=_RANDOM_ONLY, campaigns=1, ceiling=1)

    def test_missing_explore_knobs_fail_loud(self) -> None:
        knobless = attrs.evolve(
            _I1, campaign_base=None, campaign_explore=None,
            killing=attrs.evolve(_I1.killing, explore=None),
        )

        with pytest.raises(ValueError, match="no explore knobs"):
            run_mutant_campaigns(knobless, strategies=_RANDOM_ONLY, campaigns=1, ceiling=1)

    def test_a_violating_control_is_counted_not_hidden(self) -> None:
        # Point a "control" at the mutant factory: the band must report the violations —
        # this is the false-positive metric doing its job, never a silent zero.
        framed = attrs.evolve(_CONTROL, base=_I1.base)

        (record,) = run_control_band(
            framed, explore=dict(SMOKE_CONTROL_EXPLORE), strategies=_RANDOM_ONLY, runs=6
        )

        assert record.violations > 0
        assert record.ci.upper > record.ci.lower > 0.0

    def test_summary_renders_the_false_positive_section(self) -> None:
        campaigns = run_mutant_campaigns(
            _I1, strategies=_RANDOM_ONLY, campaigns=2, ceiling=3, master_seed=0
        )
        fps = run_control_band(
            _CONTROL, explore=dict(SMOKE_CONTROL_EXPLORE), strategies=_RANDOM_ONLY, runs=3
        )

        rendered = summarize(campaigns, fps, ceiling=3)

        assert "## False positives (negative controls)" in rendered
        assert "`ctrl-retry-with-key`" in rendered

    def test_false_positive_records_round_trip_the_jsonl(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        fps = run_control_band(
            _CONTROL, explore=dict(SMOKE_CONTROL_EXPLORE), strategies=_RANDOM_ONLY, runs=2
        )

        path = write_records(tmp_path / "records.jsonl", campaigns=(), false_positives=fps)

        lines = [json.loads(line) for line in path.read_text().splitlines()]
        assert [line["kind"] for line in lines[1:]] == ["false_positive"]
        assert lines[1]["violations"] == 0
