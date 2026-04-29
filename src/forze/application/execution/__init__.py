"""Execution kernel for usecases, dependency injection, and lifecycle.

Provides :class:`ExecutionContext` (dependency resolution, transactions),
:class:`Usecase` (base for application workflows), :class:`ExecutionRuntime`
(scoped execution with deps and lifecycle), and :class:`UsecaseRegistry` for
composing usecases with middlewares. Middlewares include guards, effects,
on-failure and finally hooks, and transaction wrapping.
"""

from ..contracts.auth import AuthIdentity
from .context import CallContext, ExecutionContext
from .deps import Deps, DepsModule, DepsPlan
from .facade import FacadeOpRef, UsecasesFacade, facade_call, facade_op
from .lifecycle import LifecycleHook, LifecyclePlan, LifecycleStep
from .middleware import (
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
)
from .plan import UsecasePlan
from .registry import UsecaseRegistry
from .runtime import ExecutionRuntime
from .usecase import Usecase, UsecaseFactory

# ----------------------- #

__all__ = [
    "ExecutionContext",
    "CallContext",
    "AuthIdentity",
    "UsecasePlan",
    "UsecaseRegistry",
    "Usecase",
    "UsecaseFactory",
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
