"""JSON-shaped (de)serialization of a :class:`SimulationConfig` and its seeded environment.

A found bug reproduces only if the *whole* configuration that produced it travels with the seed —
fault policy, latency profile, partition schedule, crash policy, scheduler, counts. This module
turns a :class:`SimulationConfig` into plain JSON-able dicts and back, faithfully, so a
:class:`~forze_dst.artifacts.bundle.FailureBundle` is self-contained and a replay re-runs the exact
configuration rather than re-deriving an approximation from CLI flags. Edge cases handled: the
distribution union (tagged by ``kind``), ``timedelta`` (seconds), ``frozenset`` (sorted list), the
``datetime`` epoch (ISO-8601), and the ``StrEnum`` knobs.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from forze_dst.config import (
    ClusterConfig,
    Partition,
    PartitionSchedule,
    SimulationConfig,
    Strategy,
)
from forze_dst.faults import CrashPolicy, FaultPolicy, FaultRule
from forze_dst.latency import (
    Constant,
    Distribution,
    Exponential,
    LatencyProfile,
    LatencyRule,
    LogNormal,
    Pareto,
    Uniform,
)
from forze_dst.scheduler import Fifo, Pct, Random, SchedulerSpec

# ----------------------- #


def _scheduler_to_dict(spec: SchedulerSpec) -> dict[str, Any]:
    if isinstance(spec, Pct):
        return {"kind": "pct", "depth": spec.depth, "steps": spec.steps}

    return {"kind": "random" if isinstance(spec, Random) else "fifo"}


# ....................... #


def _scheduler_from_dict(data: dict[str, Any] | str) -> SchedulerSpec:
    # Tolerate the pre-tagged-union format: a bare ``"pct"`` string carried ``pct_depth`` /
    # ``pct_steps`` as sibling keys (handled by the caller, which passes the whole config dict).
    if isinstance(data, str):
        return Fifo() if data == "fifo" else Random()

    kind = data["kind"]
    if kind == "pct":
        return Pct(depth=data.get("depth", 3), steps=data.get("steps", 50))

    return Fifo() if kind == "fifo" else Random()


# ....................... #


def _dist_to_dict(dist: Distribution) -> dict[str, Any]:
    if isinstance(dist, Constant):
        return {"kind": "constant", "seconds": dist.seconds}

    if isinstance(dist, Uniform):
        return {"kind": "uniform", "low": dist.low, "high": dist.high}

    if isinstance(dist, Exponential):
        return {"kind": "exponential", "mean": dist.mean}

    if isinstance(dist, LogNormal):
        return {"kind": "lognormal", "median": dist.median, "sigma": dist.sigma}

    if isinstance(dist, Pareto):  # pyright: ignore[reportUnnecessaryIsInstance]
        return {"kind": "pareto", "scale": dist.scale, "alpha": dist.alpha}

    raise TypeError(f"unknown latency distribution: {type(dist).__name__}")


# ....................... #


def _dist_from_dict(data: dict[str, Any]) -> Distribution:
    kind = data["kind"]

    if kind == "constant":
        return Constant(data["seconds"])

    if kind == "uniform":
        return Uniform(data["low"], data["high"])

    if kind == "exponential":
        return Exponential(data["mean"])

    if kind == "lognormal":
        return LogNormal(median=data["median"], sigma=data.get("sigma", 1.0))

    if kind == "pareto":
        return Pareto(scale=data["scale"], alpha=data.get("alpha", 1.5))

    raise ValueError(f"unknown latency distribution kind: {kind!r}")


# ....................... #


def _fault_rule_to_dict(rule: FaultRule) -> dict[str, Any]:
    return {
        "surface": rule.surface,
        "route": rule.route,
        "op": rule.op,
        "error": rule.error,
        "timeout": rule.timeout,
        "crash": rule.crash,
        "drop": rule.drop,
        "duplicate": rule.duplicate,
        "delay": rule.delay,
        "max_delay": rule.max_delay.total_seconds(),
    }


# ....................... #


def _fault_rule_from_dict(data: dict[str, Any]) -> FaultRule:
    return FaultRule(
        surface=data.get("surface"),
        route=data.get("route"),
        op=data.get("op"),
        error=data.get("error", 0.0),
        timeout=data.get("timeout", 0.0),
        crash=data.get("crash", 0.0),
        drop=data.get("drop", 0.0),
        duplicate=data.get("duplicate", 0.0),
        delay=data.get("delay", 0.0),
        max_delay=timedelta(seconds=data.get("max_delay", 5.0)),
    )


# ....................... #


def _partition_to_dict(window: Partition) -> dict[str, Any]:
    return {
        "start": window.start,
        "end": window.end,
        "isolated": sorted(window.isolated),
        "loss": window.loss,
    }


# ....................... #


def _partition_from_dict(data: dict[str, Any]) -> Partition:
    return Partition(
        start=data["start"],
        end=data["end"],
        isolated=frozenset(data["isolated"]),
        loss=data.get("loss", 1.0),
    )


# ....................... #


def config_to_dict(config: SimulationConfig) -> dict[str, Any]:
    """Render *config* — every seeded knob and sub-policy — as a JSON-able dict."""

    faults = (
        {"rules": [_fault_rule_to_dict(rule) for rule in config.faults.rules]}
        if config.faults is not None
        else None
    )
    latency = (
        {
            "rules": [
                {
                    "dist": _dist_to_dict(rule.dist),
                    "surface": rule.surface,
                    "route": rule.route,
                    "op": rule.op,
                }
                for rule in config.latency.rules
            ]
        }
        if config.latency is not None
        else None
    )
    cluster = (
        {
            "nodes": config.cluster.nodes,
            "partitions": (
                {
                    "windows": [
                        _partition_to_dict(window)
                        for window in config.cluster.partitions.windows
                    ],
                    "surfaces": sorted(config.cluster.partitions.surfaces),
                }
                if config.cluster.partitions is not None
                else None
            ),
        }
        if config.cluster is not None
        else None
    )
    crash = (
        {
            "surface": config.crash.surface,
            "route": config.crash.route,
            "op": config.crash.op,
            "probability": config.crash.probability,
        }
        if config.crash is not None
        else None
    )

    return {
        "strategy": config.strategy.value,
        "seeds": list(config.seeds),
        "scheduler": _scheduler_to_dict(config.scheduler),
        "concurrency": config.concurrency,
        "epoch": config.epoch.isoformat(),
        "count": config.count,
        "act_count": config.act_count,
        "max_examples": config.max_examples,
        "max_runs": config.max_runs,
        "dpor_seed": config.dpor_seed,
        "coverage_plateau": config.coverage_plateau,
        "guided_budget": config.guided_budget,
        "reachability_targets": sorted(config.reachability_targets),
        "runtime": config.runtime,
        "faults": faults,
        "latency": latency,
        "cluster": cluster,
        "crash": crash,
    }


# ....................... #


def config_from_dict(data: dict[str, Any]) -> SimulationConfig:
    """Rebuild a :class:`SimulationConfig` from :func:`config_to_dict`'s output (tolerant of
    missing optional keys, so a hand-trimmed or older bundle still loads)."""

    faults = (
        FaultPolicy(
            rules=tuple(_fault_rule_from_dict(rule) for rule in data["faults"]["rules"])
        )
        if data.get("faults") is not None
        else None
    )
    latency = (
        LatencyProfile(
            rules=tuple(
                LatencyRule(
                    dist=_dist_from_dict(rule["dist"]),
                    surface=rule.get("surface"),
                    route=rule.get("route"),
                    op=rule.get("op"),
                )
                for rule in data["latency"]["rules"]
            )
        )
        if data.get("latency") is not None
        else None
    )
    cluster = None
    if data.get("cluster") is not None:
        partitions_data = data["cluster"].get("partitions")
        partitions = (
            PartitionSchedule(
                windows=tuple(
                    _partition_from_dict(window)
                    for window in partitions_data["windows"]
                ),
                surfaces=frozenset(partitions_data["surfaces"]),
            )
            if partitions_data is not None
            else None
        )
        cluster = ClusterConfig(nodes=data["cluster"]["nodes"], partitions=partitions)
    crash = (
        CrashPolicy(
            surface=data["crash"].get("surface"),
            route=data["crash"].get("route"),
            op=data["crash"].get("op"),
            probability=data["crash"].get("probability", 1.0),
        )
        if data.get("crash") is not None
        else None
    )

    # Legacy bundles stored ``scheduler`` as a bare ``"pct"`` string with sibling pct_depth/steps.
    raw_scheduler = data["scheduler"]
    if isinstance(raw_scheduler, str) and raw_scheduler == "pct":
        scheduler: SchedulerSpec = Pct(
            depth=data.get("pct_depth", 3), steps=data.get("pct_steps", 50)
        )
    else:
        scheduler = _scheduler_from_dict(raw_scheduler)

    return SimulationConfig(
        strategy=Strategy(data["strategy"]),
        seeds=list(data["seeds"]),
        scheduler=scheduler,
        concurrency=data["concurrency"],
        epoch=datetime.fromisoformat(data["epoch"]),
        count=data["count"],
        act_count=data["act_count"],
        max_examples=data["max_examples"],
        max_runs=data["max_runs"],
        dpor_seed=data["dpor_seed"],
        coverage_plateau=data["coverage_plateau"],
        guided_budget=data.get("guided_budget", 256),
        reachability_targets=frozenset(data.get("reachability_targets", ())),
        runtime=data.get("runtime", False),
        faults=faults,
        latency=latency,
        cluster=cluster,
        crash=crash,
    )
