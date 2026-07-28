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
