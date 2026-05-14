"""Execution kernel for usecases, dependency injection, and lifecycle."""

from .bucket import BucketKey, Phase, Slot
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
    CapabilityAfterCommitRunner,
    CapabilityChainBuilder,
    CapabilityExecutionEvent,
    CapabilitySkip,
    CapabilityStore,
    GuardSkip,
    LegacyChainBuilder,
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
    DispatchDeclaringEffectFactory,
    EffectStep,
    ExecutionPlanReport,
    GuardStep,
    MiddlewareSpec,
    ScheduleMode,
    STEP_EXPLAIN_TX_BUCKET,
    StepExplainKind,
    StepExplainRow,
    UsecasePlan,
    frozenset_capability_keys,
)
from .registry import UsecaseRegistry
from .runtime import ExecutionRuntime
from .usecase import Usecase, UsecaseFactory

# ----------------------- #

__all__ = [
    "AUTHN_PRINCIPAL",
    "AUTHZ_PERMITS_PREFIX",
    "BucketKey",
    "CapabilityKey",
    "TENANCY_TENANT",
    "authz_permits_capability",
    "CallContext",
    "CapabilityAfterCommitRunner",
    "CapabilityChainBuilder",
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
    "LegacyChainBuilder",
    "LifecycleHook",
    "LifecyclePlan",
    "LifecycleStep",
    "Middleware",
    "MiddlewareSpec",
    "NextCall",
    "OnFailure",
    "OnFailureMiddleware",
    "Phase",
    "ScheduleMode",
    "SchedulableCapabilitySpec",
    "Slot",
    "STEP_EXPLAIN_TX_BUCKET",
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
