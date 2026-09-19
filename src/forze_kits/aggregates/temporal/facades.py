"""Typed facade for a temporal aggregate: the document surface plus its two dated reads."""

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

from .handlers import EffectiveOn, Timeline
from .operations import TemporalKernelOp

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
class TemporalFacade(DocumentFacade[R, C, U], Generic[R, C, U]):
    """The document facade plus the two reads effective dating adds.

    Extends rather than replaces: an effective-dated aggregate is an ordinary document
    aggregate, and every generated operation still reads the way it always did. What the
    declaration adds is the two questions the dates make askable.
    """

    effective_on = facade_op(
        TemporalKernelOp.EFFECTIVE_ON,
        uc=EffectiveOn[Any],
    )
    """The row in force for one key on a given day."""

    timeline = facade_op(
        TemporalKernelOp.TIMELINE,
        uc=Timeline[Any],
    )
    """Every row for one key whose period meets a window, earliest first."""


# ....................... #


def temporal_facade(
    runtime: ExecutionRuntime,
    registry: FrozenOperationRegistry,
    spec: DocumentSpec[R, Any, C, U],
    *,
    namespace: StrKeyNamespace | None = None,
) -> OperationFacadeFactory[TemporalFacade[R, C, U]]:
    """Build a per-call :class:`TemporalFacade` factory bound to *runtime*'s context.

    The temporal counterpart of :func:`~forze_kits.aggregates.document.facades.document_facade`,
    with the same per-call contract: a fresh facade each call, reading the runtime's current
    scope, safe to build once at startup.
    """

    return OperationFacadeFactory(
        type=TemporalFacade,
        registry=registry,
        ctx_factory=runtime.get_context,
        ns=namespace if namespace is not None else spec.default_namespace,
    )


# ....................... #

__all__ = ["TemporalFacade", "temporal_facade"]
