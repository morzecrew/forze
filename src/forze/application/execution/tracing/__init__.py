"""Opt-in runtime tracing for configurable ports and transaction boundaries."""

from .buffer import RuntimeTrace
from .emit import init_runtime_tracing, record
from .events import TracingEvent, TracingViolation
from .harness import TracedOperationResult, run_traced_operation
from .match import TraceExpectation, assert_trace_contains, assert_trace_equals
from .report import format_runtime_trace_report, format_violation, format_violations
from .runtime_tracer import (
    NOOP_RUNTIME_TRACER,
    NoopRuntimeTracer,
    RecordingRuntimeTracer,
    RuntimeTracer,
    runtime_tracer_from_flag,
)
from .session import active_deps, active_runtime_tracer, bind_active_deps
from .tx_tracer import (
    NOOP_TX_TRACER,
    NoopTxTracer,
    RuntimeBackedTxTracer,
    TxTracer,
    tx_tracer_from_runtime,
)
from .validate import (
    RuntimeTraceValidationError,
    RuntimeTraceValidator,
    assert_runtime_trace_valid,
    validate_runtime_trace,
)

__all__ = [
    "NOOP_RUNTIME_TRACER",
    "NOOP_TX_TRACER",
    "NoopRuntimeTracer",
    "NoopTxTracer",
    "RecordingRuntimeTracer",
    "RuntimeBackedTxTracer",
    "RuntimeTrace",
    "RuntimeTraceValidationError",
    "RuntimeTraceValidator",
    "RuntimeTracer",
    "TraceExpectation",
    "TracedOperationResult",
    "TracingEvent",
    "TracingViolation",
    "TxTracer",
    "active_deps",
    "active_runtime_tracer",
    "assert_runtime_trace_valid",
    "assert_trace_contains",
    "assert_trace_equals",
    "bind_active_deps",
    "format_runtime_trace_report",
    "format_violation",
    "format_violations",
    "init_runtime_tracing",
    "record",
    "run_traced_operation",
    "runtime_tracer_from_flag",
    "tx_tracer_from_runtime",
    "validate_runtime_trace",
]
