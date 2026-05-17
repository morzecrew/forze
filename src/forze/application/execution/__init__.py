"""Execution kernel for usecases, dependency injection, and lifecycle."""

from .capabilities import (  # type: ignore[import-not-found, attr-defined]
    CapabilityAfterCommitRunner,  # type: ignore[attr-defined]
    CapabilityExecutionEvent,  # type: ignore[attr-defined]
    CapabilityStore,  # type: ignore[attr-defined]
    ExecutionChainCompiler,  # type: ignore[attr-defined]
)
from .capability_keys import (
    AUTHN_PRINCIPAL,
    AUTHZ_PERMITS_PREFIX,
    TENANCY_TENANT,
    authz_permits_capability,
)
from .context import CallContext, ExecutionContext
from .deps import Deps, DepsModule, DepsPlan
from .dispatch import find_dispatch_cycle
from .engine import Stage
from .facade import (
    FacadeOperationDescriptor,
    OperationNamespace,
    OperationRef,
    UsecasesFacade,
    namespaced_facade,
    operation_namespace_for,
)
from .lifecycle import LifecycleHook, LifecyclePlan, LifecycleStep
from .middlewares import (
    ConditionalGuard,
    ConditionalOnSuccess,
    Failure,
    Finally,
    FinallyFactory,
    FinallyMiddleware,
    Guard,
    GuardFactory,
    GuardMiddleware,
    Middleware,
    MiddlewareFactory,
    NextCall,
    OnFailure,
    OnFailureFactory,
    OnFailureMiddleware,
    OnSuccess,
    OnSuccessFactory,
    OnSuccessMiddleware,
    Skip,
    Success,
    TxMiddleware,
)
from .plan import (
    DagNode,
    ExecutionPlanReport,
    PlanDag,
    StepExplainRow,
)
from .registry import UsecaseRegistry
from .runtime import ExecutionRuntime
from .usecase import Usecase, UsecaseFactory

# ----------------------- #

__all__ = [
    "AUTHN_PRINCIPAL",
    "AUTHZ_PERMITS_PREFIX",
    "CapabilityAfterCommitRunner",
    "CapabilityExecutionEvent",
    "CapabilityStore",
    "CallContext",
    "ConditionalGuard",
    "DagNode",
    "Deps",
    "DepsModule",
    "DepsPlan",
    "ExecutionChainCompiler",
    "ExecutionContext",
    "ExecutionPlanReport",
    "ExecutionRuntime",
    "FacadeOperationDescriptor",
    "Finally",
    "FinallyMiddleware",
    "find_dispatch_cycle",
    "Guard",
    "LifecycleHook",
    "LifecyclePlan",
    "LifecycleStep",
    "Middleware",
    "NextCall",
    "OnFailure",
    "OnFailureMiddleware",
    "OperationNamespace",
    "OperationRef",
    "PlanDag",
    "Skip",
    "Stage",
    "StepExplainRow",
    "TENANCY_TENANT",
    "Usecase",
    "UsecaseFactory",
    "UsecaseRegistry",
    "UsecasesFacade",
    "authz_permits_capability",
    "namespaced_facade",
    "operation_namespace_for",
    "ConditionalGuard",
    "ConditionalOnSuccess",
    "Guard",
    "OnFailure",
    "OnSuccess",
    "Finally",
    "Middleware",
    "NextCall",
    "GuardMiddleware",
    "OnSuccessMiddleware",
    "OnFailureMiddleware",
    "FinallyMiddleware",
    "Failure",
    "Success",
    "Skip",
    "TxMiddleware",
    "GuardFactory",
    "OnSuccessFactory",
    "OnFailureFactory",
    "FinallyFactory",
    "MiddlewareFactory",
]
