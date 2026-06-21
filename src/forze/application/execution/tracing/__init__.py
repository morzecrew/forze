"""Opt-in runtime tracing for configurable ports and transaction boundaries."""

from .assertions import (
    RuntimeTraceValidationError,
    RuntimeTraceValidator,
    TraceExpectation,
    assert_runtime_trace_valid,
    assert_trace_contains,
    assert_trace_equals,
    format_runtime_trace_report,
    format_violation,
    format_violations,
    validate_runtime_trace,
)
from .emit import (
    active_deps,
    active_runtime_tracer,
    bind_active_deps,
    init_runtime_tracing,
    record,
)
from .harness import TracedOperationResult, run_traced_operation
from .trace import RuntimeTrace, TracingEvent, TracingViolation
from .tracers import (
    NOOP_RUNTIME_TRACER,
    NOOP_TX_TRACER,
    NoopRuntimeTracer,
    NoopTxTracer,
    RecordingRuntimeTracer,
    RuntimeBackedTxTracer,
    RuntimeTracer,
    TxTracer,
    runtime_tracer_from_flag,
    tx_tracer_from_runtime,
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
