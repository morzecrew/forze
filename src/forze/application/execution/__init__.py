"""Execution kernel, dependency injection, and lifecycle."""

from .context import ExecutionContext, InvocationMetadata
from .domain import (
    DomainEventHandler,
    DomainEventHandlerFactory,
    DomainEventRegistry,
    DomainEventsDepsModule,
    InProcessDomainEventDispatcher,
    outbox_event_handler,
)
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
from .operations import OperationKind, OperationPlan
from .operations.registry import FrozenOperationRegistry, OperationRegistry
from .resilience import (
    CircuitBreakerStore,
    InMemoryCircuitBreakerStore,
    InProcessResilienceExecutor,
    ResilienceDepsModule,
    builtin_default_policies,
    default_resilience_executor,
    occ_retry,
    resolve_resilience_executor,
)
from .observability import (
    DURATION_HISTOGRAM,
    OPERATIONS_COUNTER,
    instrument_operations,
)
from .runtime import ExecutionRuntime
from .saga import (
    InProcessSagaExecutor,
    SagaDepsModule,
    default_saga_executor,
    resolve_saga_executor,
    run_saga,
)
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
    "DomainEventHandler",
    "DomainEventHandlerFactory",
    "DomainEventRegistry",
    "DomainEventsDepsModule",
    "ExecutionContext",
    "ExecutionRuntime",
    "CircuitBreakerStore",
    "FrozenOperationRegistry",
    "InMemoryCircuitBreakerStore",
    "InProcessDomainEventDispatcher",
    "InProcessResilienceExecutor",
    "LifecycleModule",
    "outbox_event_handler",
    "LifecyclePlan",
    "OperationKind",
    "OperationPlan",
    "OperationRegistry",
    "instrument_operations",
    "OPERATIONS_COUNTER",
    "DURATION_HISTOGRAM",
    "ResilienceDepsModule",
    "builtin_default_policies",
    "default_resilience_executor",
    "occ_retry",
    "resolve_resilience_executor",
    "InProcessSagaExecutor",
    "SagaDepsModule",
    "default_saga_executor",
    "resolve_saga_executor",
    "run_saga",
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
