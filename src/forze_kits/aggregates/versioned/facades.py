"""Typed facade for a versioned aggregate: the document surface plus its lineage."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, TypeVar

import attrs
from pydantic import BaseModel

from forze.application.execution.operations.facade import (
    OperationFacadeFactory,
    facade_op,
    namespaced_facade,
)
from forze.domain.models import BaseDTO
from forze_kits.aggregates.document.facades import DocumentFacade

from .handlers import CorrectDocument, FactAsOf, FactHistory
from .operations import VersionedKernelOp

if TYPE_CHECKING:
    from forze.application.contracts.document import DocumentSpec
    from forze.application.execution import ExecutionRuntime
    from forze.application.execution.operations import FrozenOperationRegistry
    from forze.base.primitives import StrKeyNamespace

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
C = TypeVar("C", bound=BaseDTO, default=BaseDTO)
U = TypeVar("U", bound=BaseDTO, default=BaseDTO)

# ....................... #


@namespaced_facade
@attrs.define(slots=True, kw_only=True, frozen=True)
class VersionedFacade(DocumentFacade[R, C, U], Generic[R, C, U]):
    """The document facade plus the three operations correction lineage adds.

    Extends rather than replaces: a versioned aggregate is an ordinary document aggregate whose
    reads happen to be restricted to current versions, so every generated operation still reads
    the way it always did.
    """

    correct = facade_op(
        VersionedKernelOp.CORRECT,
        uc=CorrectDocument[R, Any, C, Any],
    )
    """Supersede the current version of a fact with a corrected one."""

    history = facade_op(
        VersionedKernelOp.HISTORY,
        uc=FactHistory[R],
    )
    """Every version of a fact, oldest first."""

    as_of = facade_op(
        VersionedKernelOp.AS_OF,
        uc=FactAsOf[Any],
    )
    """The version of a fact that was current at an instant."""


# ....................... #


def versioned_facade(
    runtime: ExecutionRuntime,
    registry: FrozenOperationRegistry,
    spec: DocumentSpec[R, Any, C, U],
    *,
    namespace: StrKeyNamespace | None = None,
) -> OperationFacadeFactory[VersionedFacade[R, C, U]]:
    """Build a per-call :class:`VersionedFacade` factory bound to *runtime*'s context.

    The versioned counterpart of :func:`~forze_kits.aggregates.document.facades.document_facade`,
    with the same per-call contract: a fresh facade each call, reading the runtime's current
    scope, safe to build once at startup.
    """

    return OperationFacadeFactory(
        type=VersionedFacade,
        registry=registry,
        ctx_factory=runtime.get_context,
        ns=namespace if namespace is not None else spec.default_namespace,
    )


# ....................... #

__all__ = ["VersionedFacade", "versioned_facade"]
