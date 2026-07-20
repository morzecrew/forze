"""Coverage for config (de)serialization edge cases — legacy formats and unknown kinds."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any

import attrs
import pytest

from forze_dst import SimulationConfig
from forze_dst.artifacts.serialize import (
    _dist_from_dict,
    _dist_to_dict,
    _fault_rule_from_dict,
    _fault_rule_to_dict,
    _partition_from_dict,
    _partition_to_dict,
    _scheduler_from_dict,
    config_from_dict,
    config_to_dict,
)
from forze_dst.config import ClusterConfig, Partition, PartitionSchedule, Strategy
from forze_dst.faults import CrashPolicy, FaultPolicy, FaultRule
from forze_dst.latency import (
    Constant,
    Exponential,
    LatencyProfile,
    LatencyRule,
    LogNormal,
    Pareto,
    Uniform,
)
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


# ....................... #
# Structural round-trip guard: every attrs field of every bundle-serialized type must
# survive (de)serialization. Field coverage is checked by introspection over
# ``attrs.fields``, so ADDING a field to any of these types fails the coverage test until
# a non-default value is registered here — and the equality round-trip then fails until
# the serializer carries the new field. A silently dropped field is a corrupted repro: a
# counterexample bundle that replays a *different* configuration than the one that failed.


def _non_default_values(cls: type) -> dict[str, Any]:
    """A value for EVERY attrs field of *cls*, each different from the field's default."""

    factories: dict[type, Callable[[], dict[str, Any]]] = {
        Constant: lambda: {"seconds": 0.7},
        Uniform: lambda: {"low": 0.15, "high": 0.9},
        Exponential: lambda: {"mean": 0.3},
        LogNormal: lambda: {"median": 0.2, "sigma": 0.8},
        Pareto: lambda: {"scale": 0.05, "alpha": 2.5},
        PCTScheduler: lambda: {"depth": 4, "steps": 61},
        FaultRule: lambda: {
            "surface": "queue_command",
            "route": "jobs",
            "op": "enqueue",
            "error": 0.11,
            "timeout": 0.12,
            "crash": 0.13,
            "drop": 0.14,
            "duplicate": 0.15,
            "delay": 0.16,
            "max_delay": timedelta(seconds=9),
            "stream_faults": True,
        },
        FaultPolicy: lambda: {"rules": (_instance(FaultRule),)},
        LatencyRule: lambda: {
            "surface": "document_command",
            "route": "orders",
            "op": "update",
            "dist": _instance(Uniform),
        },
        LatencyProfile: lambda: {"rules": (_instance(LatencyRule),)},
        Partition: lambda: {
            "start": 3.0,
            "end": 11.0,
            "isolated": frozenset({1, 2}),
            "loss": 0.5,
        },
        PartitionSchedule: lambda: {
            "windows": (_instance(Partition),),
            "surfaces": frozenset({"queue_command"}),
        },
        ClusterConfig: lambda: {
            "nodes": 5,
            "partitions": _instance(PartitionSchedule),
        },
        CrashPolicy: lambda: {
            "surface": "outbox",
            "route": "events",
            "op": "persist_rows",
            "probability": 0.25,
        },
        SimulationConfig: lambda: {
            "seeds": [7, 8],
            "strategy": Strategy.OP_CASE,
            "scheduler": _instance(PCTScheduler),
            "concurrency": 3,
            "epoch": datetime(2031, 5, 6, 7, 8, 9),
            "count": 13,
            "act_count": 27,
            "max_examples": 41,
            "max_runs": 55,
            "dpor_seed": 17,
            "dpor_prune": False,
            "coverage_plateau": 5,
            "guided_budget": 99,
            "capture_values": True,
            "reachability_targets": frozenset({"payment.refunded"}),
            "faults": _instance(FaultPolicy),
            "latency": _instance(LatencyProfile),
            "runtime": True,
            "cluster": _instance(ClusterConfig),
            "crash": _instance(CrashPolicy),
        },
    }
    return factories[cls]()


def _resolved_default(field: Any) -> Any:
    """The field's default value (``attrs.NOTHING`` for required / takes-self factories)."""

    default = field.default
    if isinstance(default, attrs.Factory):  # type: ignore[arg-type]
        return attrs.NOTHING if default.takes_self else default.factory()
    return default


def _instance(cls: type) -> Any:
    """Build *cls* with every attrs field explicitly set to a non-default value."""

    values = _non_default_values(cls)
    field_names = {field.name for field in attrs.fields(cls)}
    missing = field_names - values.keys()
    unknown = values.keys() - field_names

    assert not missing, (
        f"{cls.__name__} grew field(s) {sorted(missing)} this round-trip guard does not "
        "cover: teach forze_dst/artifacts/serialize.py the new field(s) and register "
        "non-default values in _non_default_values."
    )
    assert not unknown, f"{cls.__name__} lost field(s) {sorted(unknown)}"

    for field in attrs.fields(cls):
        default = _resolved_default(field)
        if default is not attrs.NOTHING:
            assert values[field.name] != default, (
                f"{cls.__name__}.{field.name}: the registered value equals the default, "
                "so a serializer that drops the field would still round-trip — pick a "
                "non-default value."
            )

    return cls(**values)


_SERIALIZED_TYPES: tuple[type, ...] = (
    Constant,
    Uniform,
    Exponential,
    LogNormal,
    Pareto,
    PCTScheduler,
    FaultRule,
    FaultPolicy,
    LatencyRule,
    LatencyProfile,
    Partition,
    PartitionSchedule,
    ClusterConfig,
    CrashPolicy,
    SimulationConfig,
)


def _config_round_trip(config: SimulationConfig) -> SimulationConfig:
    return config_from_dict(config_to_dict(config))


class TestIntrospectiveRoundTrip:
    @pytest.mark.parametrize("cls", _SERIALIZED_TYPES, ids=lambda cls: cls.__name__)
    def test_every_field_has_a_registered_non_default_value(self, cls: type) -> None:
        _instance(cls)

    def test_fault_rule_round_trips_every_field(self) -> None:
        rule = _instance(FaultRule)
        assert _fault_rule_from_dict(_fault_rule_to_dict(rule)) == rule

    def test_partition_round_trips_every_field(self) -> None:
        window = _instance(Partition)
        assert _partition_from_dict(_partition_to_dict(window)) == window

    @pytest.mark.parametrize(
        "cls",
        (Constant, Uniform, Exponential, LogNormal, Pareto),
        ids=lambda cls: cls.__name__,
    )
    def test_distribution_round_trips_every_field(self, cls: type) -> None:
        dist = _instance(cls)
        assert _dist_from_dict(_dist_to_dict(dist)) == dist

    def test_simulation_config_round_trips_every_field_everywhere(self) -> None:
        # The whole tree — config plus every nested policy — must survive the bundle.
        config = _instance(SimulationConfig)
        assert _config_round_trip(config) == config
