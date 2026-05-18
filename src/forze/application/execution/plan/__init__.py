"""Public plan helpers for registry-driven execution."""

from ..engine.model import OperationStages
from .builders import (
    finally_middleware_factory,
    guard_middleware_factory,
    on_failure_middleware_factory,
    success_hook_middleware_factory,
)
from .dag import DagNode, PlanDag
from .report import (  # type: ignore[attr-defined]
    ExecutionPlanReport,
    StepExplainKind,
    StepExplainRow,
)
from .spec import MiddlewareSpec, TransactionSpec, frozenset_capability_keys
from .types import (
    WILDCARD,
    FinallyFactory,
    GuardFactory,
    MiddlewareFactory,
    OnFailureFactory,
    SuccessHookFactory,
)

# ----------------------- #

# Backward-compatible name for tests and older docs.
OperationPlan = OperationStages

__all__ = [
    "DagNode",
    "ExecutionPlanReport",
    "FinallyFactory",
    "GuardFactory",
    "MiddlewareFactory",
    "MiddlewareSpec",
    "OnFailureFactory",
    "OperationPlan",
    "PlanDag",
    "StepExplainRow",
    "SuccessHookFactory",
    "WILDCARD",
    "finally_middleware_factory",
    "guard_middleware_factory",
    "on_failure_middleware_factory",
    "success_hook_middleware_factory",
    "frozenset_capability_keys",
    "TransactionSpec",
    "StepExplainKind",
]
