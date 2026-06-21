"""Coverage for config (de)serialization edge cases — legacy formats and unknown kinds."""

from __future__ import annotations

import pytest

from forze_dst import SimulationConfig
from forze_dst.artifacts.serialize import (
    _dist_from_dict,
    _dist_to_dict,
    _scheduler_from_dict,
    config_from_dict,
    config_to_dict,
)
from forze_dst.latency import Constant, LatencyProfile, LatencyRule, LogNormal, Pareto
from forze_dst.scheduler import FIFOScheduler, PCTScheduler, RandomScheduler

# ----------------------- #


class TestSchedulerFromDict:
    def test_bare_string_forms(self) -> None:
        assert isinstance(_scheduler_from_dict("fifo"), FIFOScheduler)
        assert isinstance(_scheduler_from_dict("random"), RandomScheduler)


# ....................... #


class TestDistribution:
    def test_to_dict_rejects_unknown(self) -> None:
        with pytest.raises(TypeError):
            _dist_to_dict(object())  # type: ignore[arg-type]

    def test_from_dict_rejects_unknown_kind(self) -> None:
        with pytest.raises(ValueError):
            _dist_from_dict({"kind": "bogus"})

    def test_heavy_tailed_round_trip(self) -> None:
        for dist in (LogNormal(median=0.1, sigma=0.5), Pareto(scale=0.05, alpha=1.2)):
            assert _dist_from_dict(_dist_to_dict(dist)) == dist


# ....................... #


class TestLegacyScheduler:
    def test_bare_pct_string_with_sibling_keys(self) -> None:
        data = config_to_dict(SimulationConfig(seeds=[1]))
        # Pre-tagged-union bundles stored ``scheduler`` as a bare ``"pct"`` string with
        # ``pct_depth`` / ``pct_steps`` siblings on the config dict.
        data["scheduler"] = "pct"
        data["pct_depth"] = 4
        data["pct_steps"] = 99

        config = config_from_dict(data)

        assert isinstance(config.scheduler, PCTScheduler)
        assert config.scheduler.depth == 4
        assert config.scheduler.steps == 99


# ....................... #


class TestRoundTrip:
    def test_full_config_survives_round_trip(self) -> None:
        config = SimulationConfig(
            seeds=[1, 2, 3],
            scheduler=PCTScheduler(depth=2, steps=7),
            latency=LatencyProfile(rules=(LatencyRule(dist=Constant(0.1)),)),
        )

        restored = config_from_dict(config_to_dict(config))

        assert restored.seeds == config.seeds
        assert isinstance(restored.scheduler, PCTScheduler)
        assert restored.scheduler.depth == 2
        assert restored.latency is not None

    def test_capture_values_survives_round_trip(self) -> None:
        # A bundle from a value-level-invariant run must replay with capture on, or the
        # trace carries no values and the violation fails to reproduce.
        config = SimulationConfig(seeds=[1], capture_values=True)

        assert config_to_dict(config)["capture_values"] is True
        assert config_from_dict(config_to_dict(config)).capture_values is True

    def test_capture_values_defaults_false_on_legacy_bundle(self) -> None:
        data = config_to_dict(SimulationConfig(seeds=[1]))
        del data["capture_values"]  # an older bundle predating the field

        assert config_from_dict(data).capture_values is False
