"""The oracle — assert over a recorded history, then shrink a violation to a counterexample.

Submodules: ``recorder`` (the :class:`History` substrate), ``invariants`` + ``linearizability`` +
``reachability`` (the assertions — what must always / sometimes hold), ``report`` (the causal-graph
renderer + timeline), ``coverage`` (the behavioural signal + fingerprint), and ``replay`` (explore →
minimize → :class:`ViolationReport`). This ``__init__`` re-exports the oracle's public surface so
``from forze_dst.oracle import …`` works; the top-level ``forze_dst`` facade re-exports the common
subset from here.
"""

from __future__ import annotations

from forze_dst.oracle.confidence import ConfidenceReport, assess_confidence
from forze_dst.oracle.coverage import (
    CoverageStats,
    behavioral_coverage,
    behavioral_fingerprint,
)
from forze_dst.oracle.invariants import (
    Invariant,
    Violation,
    check,
    completes_within,
    expect,
    expect_value,
    monotonic_per,
    mutual_exclusion,
    no_duplicate_effect,
    no_resource_leak,
    no_unclosed_transaction,
    no_unexpected_error,
    operation_succeeds,
    read_your_writes,
    single_key_per_operation,
)
from forze_dst.oracle.linearizability import (
    RegisterSpec,
    SequentialSpec,
    is_linearizable,
    linearizable,
    monotonic_reads,
    record_operation,
    sequential,
)
from forze_dst.oracle.reachability import (
    ReachabilityReport,
    assess_reachability,
    reached,
    reached_labels,
    sometimes,
)
from forze_dst.oracle.recorder import (
    Event,
    History,
    Recorder,
    bind_recorder,
    current_recorder,
    record_event,
)
from forze_dst.oracle.replay import (
    ViolationReport,
    explore,
    minimize,
    run_recorded,
)
from forze_dst.oracle.report import (
    CausalGraph,
    OperationSpan,
    TimelineEntry,
    TraceStep,
    build_timeline,
    format_report,
    render_timeline,
)

__all__ = [
    # recorder substrate
    "Event",
    "History",
    "Recorder",
    "record_event",
    "bind_recorder",
    "current_recorder",
    # invariants
    "Invariant",
    "Violation",
    "check",
    "no_duplicate_effect",
    "no_resource_leak",
    "no_unclosed_transaction",
    "monotonic_per",
    "mutual_exclusion",
    "no_unexpected_error",
    "operation_succeeds",
    "completes_within",
    "single_key_per_operation",
    "read_your_writes",
    "expect_value",
    "expect",
    # linearizability + weaker consistency models
    "linearizable",
    "is_linearizable",
    "sequential",
    "monotonic_reads",
    "RegisterSpec",
    "SequentialSpec",
    "record_operation",
    # reachability
    "sometimes",
    "reached",
    "reached_labels",
    "assess_reachability",
    "ReachabilityReport",
    # coverage
    "behavioral_coverage",
    "behavioral_fingerprint",
    "CoverageStats",
    # confidence
    "ConfidenceReport",
    "assess_confidence",
    # report + timeline
    "format_report",
    "build_timeline",
    "render_timeline",
    "CausalGraph",
    "OperationSpan",
    "TimelineEntry",
    "TraceStep",
    # replay
    "ViolationReport",
    "explore",
    "minimize",
    "run_recorded",
]
