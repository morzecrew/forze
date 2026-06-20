"""Failure artifact bundle (E5.2) — a found bug as a portable, replayable JSON file.

A bundle must carry *everything* needed to reproduce: the seed and the full configuration that
produced it (faults, latency, partitions, crash, scheduler), serialized faithfully so a replay on
another machine re-runs the exact run rather than an approximation. These tests pin the config
round-trip across every sub-policy, the JSON/save round-trip, and that `replay_bundle` reproduces
the original violation at the original seed.
"""

from __future__ import annotations

import asyncio

import attrs
from pydantic import BaseModel

from forze.application.contracts.execution import Handler
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze_dst import Simulation, SimulationConfig, Strategy
from forze_dst.markers import record_event
from forze_dst.artifacts import FailureBundle, bundle_from_report, config_from_dict, config_to_dict, replay_bundle
from forze_dst.cluster import ClusterConfig, Partition, PartitionSchedule
from forze_dst.faults import FaultPolicy, FaultRule
from forze_dst.invariants import expect
from forze_dst.latency import Constant, Exponential, LatencyProfile, LatencyRule, LogNormal, Pareto, Uniform
from forze_dst.config import CrashPolicy
from forze_dst.scheduler import Pct
from forze_mock import MockDepsModule

# ----------------------- #


def _rich_config() -> SimulationConfig:
    """A config that exercises every serialized sub-policy and edge-case type."""

    return SimulationConfig(
        strategy=Strategy.SCENARIO,
        seeds=[0, 1, 2, 3],  # a list (a range would round-trip to a list and break ==)
        scheduler=Pct(depth=4),
        concurrency=3,
        act_count=7,
        coverage_plateau=0,
        guided_budget=128,
        reachability_targets=frozenset({"lock-contended", "write-retried"}),
        runtime=True,
        faults=FaultPolicy(
            rules=(
                FaultRule(surface="document_command", error=0.2, timeout=0.1),
                FaultRule(op="create", drop=0.05, duplicate=0.05),
            )
        ),
        latency=LatencyProfile(
            rules=(
                LatencyRule(dist=Constant(0.05), surface="document_command"),
                LatencyRule(dist=Uniform(0.01, 0.2), route="orders"),
                LatencyRule(dist=Exponential(0.03)),
            )
        ),
        cluster=ClusterConfig(
            nodes=3,
            partitions=PartitionSchedule(
                windows=(
                    Partition(start=0.5, end=1.5, isolated=frozenset({1}), loss=0.3),
                    Partition(start=2.0, end=3.0, isolated=frozenset({0, 2})),
                ),
                surfaces=frozenset({"document_command", "queue_command"}),
            ),
        ),
        crash=CrashPolicy(surface="document_command", op="update", probability=0.5),
    )


class TestConfigRoundTrip:
    def test_full_config_survives_round_trip(self) -> None:
        config = _rich_config()
        assert config_from_dict(config_to_dict(config)) == config

    def test_minimal_config_round_trips(self) -> None:
        config = SimulationConfig(seeds=[7])
        assert config_from_dict(config_to_dict(config)) == config

    def test_each_distribution_kind_round_trips(self) -> None:
        dists = (
            Constant(0.1),
            Uniform(0.0, 1.0),
            Exponential(0.5),
            LogNormal(median=0.05, sigma=1.2),
            Pareto(scale=0.01, alpha=1.3),
        )
        for dist in dists:
            config = SimulationConfig(
                seeds=[0], latency=LatencyProfile(rules=(LatencyRule(dist=dist),))
            )
            assert config_from_dict(config_to_dict(config)) == config

    def test_serialized_form_is_json_safe(self) -> None:
        import json

        json.dumps(config_to_dict(_rich_config()))  # no datetime / timedelta / set leaks


# ....................... #
# A racy ledger that loses an update under concurrency — violates under the scenario strategy.


class DepositDTO(BaseModel):
    amount: int


@attrs.define(slots=True, kw_only=True)
class _Deposit(Handler[DepositDTO, None]):
    ledger: dict[str, int]

    async def __call__(self, args: DepositDTO) -> None:
        self.ledger["expected"] += args.amount
        current = self.ledger["balance"]
        await asyncio.sleep(0)
        self.ledger["balance"] = current + args.amount


def _racy_sim() -> Simulation:
    ledger = {"balance": 0, "expected": 0}
    registry = OperationRegistry(
        handlers={"deposit": lambda _c: _Deposit(ledger=ledger)},
        descriptors={
            "deposit": OperationDescriptor(
                input_type=DepositDTO, output_type=None, description="x"
            )
        },
    ).freeze()

    async def reset(_ctx: object) -> None:
        ledger["balance"] = ledger["expected"] = 0

    async def observe(_ctx: object) -> None:
        record_event("balance", final=ledger["balance"], expected=ledger["expected"])

    return Simulation(
        operations=registry,
        deps=lambda: MockDepsModule(),
        setup=reset,
        observe=observe,
        invariants=[
            expect("balance", lambda e: e.fields["final"] == e.fields["expected"],
                   message="lost deposit")
        ],
    )


_FIND_CONFIG = SimulationConfig(
    strategy=Strategy.SCENARIO, seeds=range(30), concurrency=4, act_count=6
)


class TestBundleRoundTrip:
    def _bundle(self) -> FailureBundle:
        report = _racy_sim().run(_FIND_CONFIG)
        assert report is not None
        return bundle_from_report(report, _FIND_CONFIG, target="app:sim")

    def test_json_round_trip_preserves_the_bundle(self) -> None:
        bundle = self._bundle()
        assert FailureBundle.from_json(bundle.to_json()) == bundle

    def test_save_and_load(self, tmp_path: object) -> None:
        path = tmp_path / "bug.bundle.json"  # type: ignore[operator]
        bundle = self._bundle()
        bundle.save(path)
        assert FailureBundle.load(path) == bundle

    def test_carries_context(self) -> None:
        bundle = self._bundle()
        assert bundle.target == "app:sim"
        assert "expect" in bundle.invariants
        assert bundle.registry_fingerprint is not None
        assert bundle.workload  # minimized (op, repr(arg)) pairs, for the eye


class TestReplay:
    def test_replay_reproduces_the_violation(self) -> None:
        report = _racy_sim().run(_FIND_CONFIG)
        assert report is not None
        bundle = bundle_from_report(report, _FIND_CONFIG, target="app:sim")

        # A fresh sim per replay (state resets via setup); load is injected so no real import.
        reproduced = replay_bundle(
            FailureBundle.from_json(bundle.to_json()),
            load=lambda _target: _racy_sim(),
        )

        assert reproduced is not None
        assert reproduced.seed == report.seed
        assert {v.invariant for v in reproduced.violations} == {"expect"}

    def test_replay_is_deterministic(self) -> None:
        report = _racy_sim().run(_FIND_CONFIG)
        assert report is not None
        bundle = bundle_from_report(report, _FIND_CONFIG, target="app:sim")

        a = replay_bundle(bundle, load=lambda _t: _racy_sim())
        b = replay_bundle(bundle, load=lambda _t: _racy_sim())
        assert a is not None and b is not None
        assert a.seed == b.seed

    def test_replay_rejects_missing_target(self) -> None:
        import pytest

        bundle = FailureBundle(
            seed=0, schedule_seed=None, target=None, config=config_to_dict(_FIND_CONFIG)
        )
        with pytest.raises(ValueError):
            replay_bundle(bundle, load=lambda _t: _racy_sim())

    def test_replay_rejects_non_simulation(self) -> None:
        import pytest

        bundle = FailureBundle(
            seed=0, schedule_seed=None, target="x:y", config=config_to_dict(_FIND_CONFIG)
        )
        with pytest.raises(TypeError):
            replay_bundle(bundle, load=lambda _t: object())
