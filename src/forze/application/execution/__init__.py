"""Execution kernel for usecases, dependency injection, and lifecycle."""

from .context import CallContext, ExecutionContext
from .delegate import UsecaseDelegate, delegated_usecase_effect
from .deps import Deps, DepsModule, DepsPlan
from .dispatch import (
    expand_wildcard_dispatch_sources,
    find_dispatch_cycle,
    format_dispatch_cycle,
)
from .facade import FacadeOpRef, UsecasesFacade, facade_call, facade_op
from .lifecycle import LifecycleHook, LifecyclePlan, LifecycleStep
from .middleware import (
    ConditionalEffect,
    ConditionalGuard,
    Effect,
    Failed,
    Finally,
    FinallyMiddleware,
    Guard,
    Middleware,
    NextCall,
    OnFailure,
    OnFailureMiddleware,
    Successful,
    UsecaseOutcome,
    WhenEffect,
    WhenGuard,
)
from .capabilities import (
    CapabilityExecutionEvent,
    CapabilitySkip,
    CapabilityStore,
    GuardSkip,
    SchedulableCapabilitySpec,
    schedule_capability_specs,
)
from .capability_keys import (
    AUTHN_PRINCIPAL,
    AUTHZ_PERMITS_PREFIX,
    TENANCY_TENANT,
    CapabilityKey,
    authz_permits_capability,
)
from .plan import (
    CAPABILITY_SCHEDULER_BUCKETS,
    DispatchDeclaringEffectFactory,
    EffectStep,
    ExecutionPlanReport,
    GuardStep,
    MiddlewareSpec,
    ScheduleMode,
    StepExplainKind,
    StepExplainRow,
    UsecasePlan,
    frozenset_capability_keys,
    middleware_specs_for_usecase_tuple,
)
from .registry import UsecaseRegistry
from .runtime import ExecutionRuntime
from .usecase import Usecase, UsecaseFactory

# ----------------------- #

__all__ = [
    "AUTHN_PRINCIPAL",
    "AUTHZ_PERMITS_PREFIX",
    "CapabilityKey",
    "TENANCY_TENANT",
    "authz_permits_capability",
    "CAPABILITY_SCHEDULER_BUCKETS",
    "CallContext",
    "CapabilityExecutionEvent",
    "CapabilitySkip",
    "CapabilityStore",
    "ConditionalEffect",
    "ConditionalGuard",
    "Deps",
    "DepsModule",
    "DepsPlan",
    "DispatchDeclaringEffectFactory",
    "Effect",
    "EffectStep",
    "ExecutionContext",
    "ExecutionPlanReport",
    "ExecutionRuntime",
    "FacadeOpRef",
    "Failed",
    "Finally",
    "FinallyMiddleware",
    "frozenset_capability_keys",
    "Guard",
    "GuardSkip",
    "GuardStep",
    "LifecycleHook",
    "LifecyclePlan",
    "LifecycleStep",
    "Middleware",
    "MiddlewareSpec",
    "middleware_specs_for_usecase_tuple",
    "NextCall",
    "OnFailure",
    "OnFailureMiddleware",
    "ScheduleMode",
    "SchedulableCapabilitySpec",
    "StepExplainKind",
    "StepExplainRow",
    "Successful",
    "Usecase",
    "UsecaseFactory",
    "UsecaseOutcome",
    "UsecasePlan",
    "UsecaseRegistry",
    "UsecaseDelegate",
    "UsecasesFacade",
    "WhenEffect",
    "WhenGuard",
    "delegated_usecase_effect",
    "expand_wildcard_dispatch_sources",
    "facade_call",
    "facade_op",
    "find_dispatch_cycle",
    "format_dispatch_cycle",
    "schedule_capability_specs",
]
