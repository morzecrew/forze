"""Deterministic simulation runtime: a native virtual-time event loop + clock seams.

The substrate for deterministic simulation testing (DST) — run an async scenario in
virtual time, seed-replayable, with no real I/O. Framework-owned (no external loop
dependency); the simulation clock drives the ambient ``TimeSource`` so application
time reads track it.
"""

from __future__ import annotations

from .derive import DEFAULT_CREATE_VERBS, derive_scenario
from .faults import (
    CrashInterceptor,
    CrashPolicy,
    FaultPolicy,
    FaultRule,
    PortFaultInterceptor,
    SimulatedCrash,
)
from .latency import (
    Constant,
    Exponential,
    LatencyProfile,
    LatencyRule,
    Uniform,
)
from .harness import OperationCase, Simulation
from .invariants import (
    Invariant,
    Violation,
    check,
    completes_within,
    expect,
    monotonic_per,
    mutual_exclusion,
    no_duplicate_effect,
    no_unexpected_error,
    operation_succeeds,
    single_key_per_operation,
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
from .corpus import (
    RegressionEntry,
    append_regression,
    entry_from_report,
    load_regressions,
)
from .oracle import ViolationReport, explore, minimize, run_recorded
from .reactive import ReactiveMap
from .report import CausalGraph, OperationSpan, TraceStep, format_report
from .recorder import (
    Event,
    History,
    Recorder,
    bind_recorder,
    current_recorder,
    record_event,
)
from forze.application.execution.interception import LatencyModel

from .runtime import run_simulation
from .config import (
    ClusterConfig,
    Partition,
    PartitionSchedule,
    SchedulerKind,
    SimulationConfig,
    Strategy,
)
from .cluster import Cluster
from .coverage import CoverageStats, behavioral_coverage
from .scenario import ModelState, Rule, Scenario
from .scheduler import (
    PCTScheduler,
    RandomScheduler,
    Scheduler,
    SystematicScheduler,
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
    "PortFaultInterceptor",
    "CrashInterceptor",
    "CrashPolicy",
    "SimulatedCrash",
    "FaultPolicy",
    "FaultRule",
    "LatencyProfile",
    "LatencyRule",
    "Constant",
    "Uniform",
    "Exponential",
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
    "no_unexpected_error",
    "operation_succeeds",
    "completes_within",
    "single_key_per_operation",
    "expect",
    "run_recorded",
    "minimize",
    "explore",
    "ViolationReport",
    "RegressionEntry",
    "append_regression",
    "entry_from_report",
    "load_regressions",
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
    "LatencyModel",
    "Scenario",
    "Rule",
    "ModelState",
    "SimulationConfig",
    "Strategy",
    "SchedulerKind",
    "Cluster",
    "ClusterConfig",
    "Partition",
    "PartitionSchedule",
    "CoverageStats",
    "behavioral_coverage",
    "derive_scenario",
    "DEFAULT_CREATE_VERBS",
    "ReactiveMap",
    "Scheduler",
    "RandomScheduler",
    "PCTScheduler",
    "SystematicScheduler",
    "pct_scheduler_factory",
]
