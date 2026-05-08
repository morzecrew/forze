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
from .plan import DispatchDeclaringEffectFactory, UsecasePlan
from .registry import UsecaseRegistry
from .runtime import ExecutionRuntime
from .usecase import Usecase, UsecaseFactory

# ----------------------- #

__all__ = [
    "ExecutionContext",
    "CallContext",
    "DispatchDeclaringEffectFactory",
    "UsecasePlan",
    "UsecaseRegistry",
    "UsecaseDelegate",
    "delegated_usecase_effect",
    "find_dispatch_cycle",
    "format_dispatch_cycle",
    "expand_wildcard_dispatch_sources",
    "Usecase",
    "UsecaseFactory",
    "ConditionalEffect",
    "ConditionalGuard",
    "Effect",
    "Failed",
    "Finally",
    "FinallyMiddleware",
    "Guard",
    "Middleware",
    "NextCall",
    "OnFailure",
    "OnFailureMiddleware",
    "Successful",
    "UsecaseOutcome",
    "WhenEffect",
    "WhenGuard",
    "ExecutionRuntime",
    "LifecyclePlan",
    "DepsPlan",
    "Deps",
    "DepsModule",
    "LifecycleHook",
    "LifecycleStep",
    "UsecasesFacade",
    "facade_op",
    "FacadeOpRef",
    "facade_call",
]
