"""Deterministic Simulation Testing (DST) for Forze — the public facade.

Point a :class:`Simulation` at a real app and one master seed reproduces the whole run
(schedule, faults, latency, inputs, crashes, partitions), single-process or N-node, over real
registries and runtimes, with no app changes. A found violation minimizes to a replayable
:class:`ViolationReport`.

The facade is a thin core plus namespaces — reach into a namespace for depth::

    from forze_dst import Simulation, OperationCase, invariants as inv

    sim = Simulation(operations, deps, invariants=[inv.no_duplicate_effect("paid", by="order")])

Namespaces: ``invariants`` (the assertion toolkit), ``markers`` (``record_event`` / ``reached``
— the no-op-in-production annotations you add to app code), ``faults`` / ``latency`` (injection),
``scheduler`` (interleaving — the ``Fifo`` / ``Random`` / ``Pct`` config variants live here and at
top level), ``cluster`` (distributed), ``artifacts`` (bundles / sweeps /
corpus), ``runtime`` (the low-level virtual-time loop), ``oracle`` (recording / report /
coverage internals), and ``workload`` / ``explore_guided`` / ``derive`` (input generation).
"""

from __future__ import annotations

# Core — the happy path: build a Simulation, feed inputs, run, read the report.
from .cluster import Cluster
from .config import SimulationConfig, Strategy
from .engines.cases import OperationCase
from .harness import Simulation
from .oracle import ViolationReport
from .scenario import ModelState, Rule, Scenario
from .scheduler import Fifo, Pct, Random

# Namespaces — depth on demand (``forze_dst.<namespace>``).
from . import (  # noqa: F401  (re-exported namespaces)
    artifacts,
    cluster,
    derive,
    explore_guided,
    faults,
    invariants,
    latency,
    markers,
    oracle,
    runtime,
    scheduler,
    workload,
)

# ----------------------- #

__all__ = [
    # Core
    "Simulation",
    "SimulationConfig",
    "Strategy",
    "Fifo",
    "Random",
    "Pct",
    "OperationCase",
    "Scenario",
    "Rule",
    "ModelState",
    "Cluster",
    "ViolationReport",
    # Namespaces
    "invariants",
    "markers",
    "faults",
    "latency",
    "scheduler",
    "cluster",
    "artifacts",
    "runtime",
    "oracle",
    "workload",
    "explore_guided",
    "derive",
]
