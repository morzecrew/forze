from typing import Any, Generic, TypeVar

import attrs
from pydantic import BaseModel

from forze.application.execution.facade import (
    OperationFacade,
    facade_op,
    namespaced_facade,
)
from forze.application.handlers.document import (
    AggregatedListDocuments,
    CreateDocument,
    GetDocument,
    KillDocument,
    RawCursorListDocuments,
    RawListDocuments,
    TypedCursorListDocuments,
    TypedListDocuments,
    UpdateDocument,
)
from forze.domain.models import BaseDTO

from .operations import DocumentKernelOp

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
C = TypeVar("C", bound=BaseDTO, default=BaseDTO)
U = TypeVar("U", bound=BaseDTO, default=BaseDTO)

# ....................... #


@namespaced_facade
@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentFacade(OperationFacade, Generic[R, C, U]):
    """Typed facade for document operations."""

    get = facade_op(
        DocumentKernelOp.GET,
        uc=GetDocument[R],
    )
    """Get document operation."""

    list = facade_op(
        DocumentKernelOp.LIST,
        uc=TypedListDocuments[R],
    )
    """List documents operation."""

    raw_list = facade_op(
        DocumentKernelOp.RAW_LIST,
        uc=RawListDocuments,
    )
    """Raw list documents operation."""

    list_cursor = facade_op(
        DocumentKernelOp.LIST_CURSOR,
        uc=TypedCursorListDocuments[R],
    )
    """List documents with cursor (keyset) pagination operation."""

    raw_list_cursor = facade_op(
        DocumentKernelOp.RAW_LIST_CURSOR,
        uc=RawCursorListDocuments,
    )
    """Raw list with cursor (keyset) pagination operation."""

    agg_list = facade_op(
        DocumentKernelOp.AGG_LIST,
        uc=AggregatedListDocuments,
    )
    """List documents with aggregates operation."""

    create = facade_op(
        DocumentKernelOp.CREATE,
        uc=CreateDocument[C, Any, R],
    )
    """Create document operation."""

    update = facade_op(
        DocumentKernelOp.UPDATE,
        uc=UpdateDocument[U, Any, R],
    )
    """Update document operation."""

    kill = facade_op(
        DocumentKernelOp.KILL,
        uc=KillDocument,
    )
    """Kill document operation."""
