"""Deterministic simulation runtime: a native virtual-time event loop + clock seams.

The substrate for deterministic simulation testing (DST) — run an async scenario in
virtual time, seed-replayable, with no real I/O. Framework-owned (no external loop
dependency); the simulation clock drives the ambient ``TimeSource`` so application
time reads track it.
"""

from __future__ import annotations

from .faults import FaultyQueueCommand, TransportFault, TransportFaultPolicy
from .invariants import (
    Invariant,
    Violation,
    check,
    expect,
    monotonic_per,
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
from .recorder import (
    Event,
    History,
    Recorder,
    bind_recorder,
    current_recorder,
    record_event,
)
from .runtime import run_simulation
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
    "expect",
    "run_recorded",
    "minimize",
    "explore",
    "ViolationReport",
    "SequentialSpec",
    "RegisterSpec",
    "record_operation",
    "is_linearizable",
    "linearizable",
]
