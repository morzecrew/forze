"""Deterministic simulation runtime: a native virtual-time event loop + clock seams.

The substrate for deterministic simulation testing (DST) — run an async scenario in
virtual time, seed-replayable, with no real I/O. Framework-owned (no external loop
dependency); the simulation clock drives the ambient ``TimeSource`` so application
time reads track it.
"""

from __future__ import annotations

from .derive import DEFAULT_CREATE_VERBS, derive_scenario
from .faults import FaultyQueueCommand, TransportFault, TransportFaultPolicy
from .harness import OperationCase, Simulation
from .invariants import (
    Invariant,
    Violation,
    check,
    expect,
    monotonic_per,
    mutual_exclusion,
    no_duplicate_effect,
)
from .linearizability import (
    RegisterSpec,
    SequentialSpec,
    is_linearizable,
    linearizable,
    record_operation,
)
from .loop import (
    RealIOForbidden,
    SimulationDeadlock,
    SimulationEventLoop,
)
from .oracle import ViolationReport, explore, minimize, run_recorded
from .report import CausalGraph, OperationSpan, TraceStep, format_report
from .recorder import (
    Event,
    History,
    Recorder,
    bind_recorder,
    current_recorder,
    record_event,
)
from .runtime import run_simulation
from .scenario import ModelState, Rule, Scenario
from .scheduler import (
    PCTScheduler,
    RandomScheduler,
    Scheduler,
    pct_scheduler_factory,
)
from .time_source import DEFAULT_EPOCH, SimulationTimeSource
from .workload import (
    OpSpec,
    generate_workload,
    run_workload,
    simulate_workload,
)

# ----------------------- #

__all__ = [
    "SimulationEventLoop",
    "SimulationTimeSource",
    "run_simulation",
    "RealIOForbidden",
    "SimulationDeadlock",
    "DEFAULT_EPOCH",
    "OpSpec",
    "generate_workload",
    "run_workload",
    "simulate_workload",
    "TransportFaultPolicy",
    "FaultyQueueCommand",
    "TransportFault",
    "Event",
    "History",
    "Recorder",
    "record_event",
    "bind_recorder",
    "current_recorder",
    "Invariant",
    "Violation",
    "check",
    "no_duplicate_effect",
    "monotonic_per",
    "mutual_exclusion",
    "expect",
    "run_recorded",
    "minimize",
    "explore",
    "ViolationReport",
    "CausalGraph",
    "OperationSpan",
    "TraceStep",
    "format_report",
    "SequentialSpec",
    "RegisterSpec",
    "record_operation",
    "is_linearizable",
    "linearizable",
    "Simulation",
    "OperationCase",
    "Scenario",
    "Rule",
    "ModelState",
    "derive_scenario",
    "DEFAULT_CREATE_VERBS",
    "Scheduler",
    "RandomScheduler",
    "PCTScheduler",
    "pct_scheduler_factory",
]
