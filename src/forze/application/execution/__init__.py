"""Execution kernel, dependency injection, and lifecycle."""

from .context import ExecutionContext, InvocationMetadata
from .deps import Deps, DepsModule, DepsPlan
from .lifecycle import LifecyclePlan
from .planning import OperationPlan
from .registry import FrozenOperationRegistry, OperationRegistry
from .runtime import ExecutionRuntime
from .tracing import (
    RuntimeTrace,
    RuntimeTraceValidator,
    TracingEvent,
    TracingViolation,
    active_deps,
    validate_runtime_trace,
)

# ----------------------- #

__all__ = [
    "InvocationMetadata",
    "Deps",
    "DepsModule",
    "DepsPlan",
    "ExecutionContext",
    "ExecutionRuntime",
    "FrozenOperationRegistry",
    "LifecyclePlan",
    "OperationPlan",
    "OperationRegistry",
    "RuntimeTrace",
    "RuntimeTraceValidator",
    "TracingEvent",
    "TracingViolation",
    "active_deps",
    "validate_runtime_trace",
]
