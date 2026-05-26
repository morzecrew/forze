"""Opt-in runtime tracing for configurable ports and transaction boundaries."""

from .buffer import RuntimeTrace
from .emit import init_runtime_tracing, record
from .events import TracingEvent, TracingViolation
from .session import active_deps, bind_active_deps
from .validate import RuntimeTraceValidator, validate_runtime_trace

__all__ = [
    "RuntimeTrace",
    "RuntimeTraceValidator",
    "TracingEvent",
    "TracingViolation",
    "active_deps",
    "bind_active_deps",
    "init_runtime_tracing",
    "record",
    "validate_runtime_trace",
]
