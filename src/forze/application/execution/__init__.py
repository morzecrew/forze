"""Execution kernel, dependency injection, and lifecycle."""

from .context import ExecutionContext, InvocationMetadata
from .deps import Deps, DepsModule, DepsPlan
from .lifecycle import LifecyclePlan, LifecycleStep
from .planning import OperationPlan
from .runtime import ExecutionRuntime

# ----------------------- #

__all__ = [
    "InvocationMetadata",
    "Deps",
    "DepsModule",
    "DepsPlan",
    "ExecutionContext",
    "ExecutionRuntime",
    "LifecyclePlan",
    "LifecycleStep",
    "OperationPlan",
]
