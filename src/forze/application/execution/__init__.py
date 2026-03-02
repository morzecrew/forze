"""Execution kernel for usecases, dependency injection, and lifecycle.

Provides :class:`ExecutionContext` (dependency resolution, transactions),
:class:`Usecase` (base for application workflows), :class:`ExecutionRuntime`
(scoped execution with deps and lifecycle), and :class:`UsecaseRegistry` for
composing usecases with middlewares. Middlewares include guards, effects, and
transaction wrapping.
"""

from .context import ExecutionContext
from .deps import Deps, DepsModule, DepsPlan
from .lifecycle import LifecycleHook, LifecyclePlan, LifecycleStep
from .middleware import Effect, Guard, Middleware, NextCall
from .plan import UsecasePlan
from .registry import UsecaseRegistry
from .runtime import ExecutionRuntime
from .usecase import Usecase

# ----------------------- #

__all__ = [
    "ExecutionContext",
    "UsecasePlan",
    "UsecaseRegistry",
    "Usecase",
    "Effect",
    "Guard",
    "Middleware",
    "NextCall",
    "ExecutionRuntime",
    "LifecyclePlan",
    "DepsPlan",
    "Deps",
    "DepsModule",
    "LifecycleHook",
    "LifecycleStep",
]
