"""Execution kernel, dependency injection, and lifecycle."""

from .context import ExecutionContext, InvocationMetadata
from .deps import (
    Deps,
    DepsModule,
    DepsRegistry,
    FrozenDeps,
    FrozenDepsRegistry,
    ResolutionContext,
    ResolutionTracer,
    RuntimeTracer,
    resolution_tracer_from_flag,
    runtime_tracer_from_flag,
)
from .lifecycle import (
    LifecycleModule,
    LifecyclePlan,
)
from .planning import OperationPlan
from .registry import FrozenOperationRegistry, OperationRegistry
from .runtime import ExecutionRuntime
from .tracing import (
    RuntimeTrace,
    RuntimeTraceValidationError,
    RuntimeTraceValidator,
    TracedOperationResult,
    TraceExpectation,
    TracingEvent,
    TracingViolation,
    active_deps,
    assert_runtime_trace_valid,
    assert_trace_contains,
    assert_trace_equals,
    run_traced_operation,
    validate_runtime_trace,
)

# ----------------------- #

__all__ = [
    "InvocationMetadata",
    "Deps",
    "DepsModule",
    "DepsRegistry",
    "FrozenDeps",
    "FrozenDepsRegistry",
    "ResolutionContext",
    "ResolutionTracer",
    "RuntimeTracer",
    "resolution_tracer_from_flag",
    "runtime_tracer_from_flag",
    "ExecutionContext",
    "ExecutionRuntime",
    "FrozenOperationRegistry",
    "LifecycleModule",
    "LifecyclePlan",
    "OperationPlan",
    "OperationRegistry",
    "RuntimeTrace",
    "RuntimeTraceValidationError",
    "RuntimeTraceValidator",
    "TraceExpectation",
    "TracedOperationResult",
    "TracingEvent",
    "TracingViolation",
    "active_deps",
    "assert_runtime_trace_valid",
    "assert_trace_contains",
    "assert_trace_equals",
    "run_traced_operation",
    "validate_runtime_trace",
]
