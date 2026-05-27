"""Opt-in runtime tracing for configurable ports and transaction boundaries."""

from .buffer import RuntimeTrace
from .emit import init_runtime_tracing, record
from .events import TracingEvent, TracingViolation
from .harness import TracedOperationResult, run_traced_operation
from .match import TraceExpectation, assert_trace_contains, assert_trace_equals
from .report import format_runtime_trace_report, format_violation, format_violations
from .session import active_deps, active_runtime_tracer, bind_active_deps
from .validate import (
    RuntimeTraceValidationError,
    RuntimeTraceValidator,
    assert_runtime_trace_valid,
    validate_runtime_trace,
)

__all__ = [
    "RuntimeTrace",
    "RuntimeTraceValidationError",
    "RuntimeTraceValidator",
    "TraceExpectation",
    "TracedOperationResult",
    "TracingEvent",
    "TracingViolation",
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
    "validate_runtime_trace",
]
