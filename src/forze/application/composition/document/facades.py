from typing import Any, Generic, TypeVar

import attrs
from pydantic import BaseModel

from forze.application.execution import (
    FacadeOperationDescriptor,
    UsecasesFacade,
    namespaced_facade,
)
from forze.application.usecases.document import (
    AggregatedListDocuments,
    CreateDocument,
    DeleteDocument,
    GetDocument,
    GetDocumentByNumberId,
    KillDocument,
    RawCursorListDocuments,
    RawListDocuments,
    RestoreDocument,
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


@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentDTOs(Generic[R, C, U]):
    """DTO type mapping for a document aggregate."""

    read: type[R]
    """Get command type."""

    create: type[C] | None = attrs.field(default=None)
    """Create command type; optional when create is not supported."""

    update: type[U] | None = attrs.field(default=None)
    """Update command type; optional when update is not supported."""


# ....................... #


@namespaced_facade
@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentUsecasesFacade(UsecasesFacade, Generic[R, C, U]):
    """Typed facade for document usecases."""

    get = FacadeOperationDescriptor(
        DocumentKernelOp.GET,
        uc=GetDocument[R],
    )
    """Get document usecase."""

    get_by_number_id = FacadeOperationDescriptor(
        DocumentKernelOp.GET_BY_NUMBER_ID,
        uc=GetDocumentByNumberId[R],
    )
    """Get document by number ID usecase."""

    list = FacadeOperationDescriptor(
        DocumentKernelOp.LIST,
        uc=TypedListDocuments[R],
    )
    """List documents usecase."""

    raw_list = FacadeOperationDescriptor(
        DocumentKernelOp.RAW_LIST,
        uc=RawListDocuments,
    )
    """Raw list documents usecase."""

    list_cursor = FacadeOperationDescriptor(
        DocumentKernelOp.LIST_CURSOR,
        uc=TypedCursorListDocuments[R],
    )
    """List documents with cursor (keyset) pagination."""

    raw_list_cursor = FacadeOperationDescriptor(
        DocumentKernelOp.RAW_LIST_CURSOR,
        uc=RawCursorListDocuments,
    )
    """Raw list with cursor (keyset) pagination."""

    agg_list = FacadeOperationDescriptor(
        DocumentKernelOp.AGG_LIST,
        uc=AggregatedListDocuments,
    )
    """List documents with aggregates."""

    create = FacadeOperationDescriptor(
        DocumentKernelOp.CREATE,
        uc=CreateDocument[C, Any, R],
    )
    """Create document usecase."""

    update = FacadeOperationDescriptor(
        DocumentKernelOp.UPDATE,
        uc=UpdateDocument[U, Any, R],
    )
    """Update document usecase."""

    kill = FacadeOperationDescriptor(
        DocumentKernelOp.KILL,
        uc=KillDocument,
    )
    """Kill document usecase."""

    delete = FacadeOperationDescriptor(
        DocumentKernelOp.DELETE,
        uc=DeleteDocument[R],
    )
    """Delete document usecase."""

    restore = FacadeOperationDescriptor(
        DocumentKernelOp.RESTORE,
        uc=RestoreDocument[R],
    )
    """Restore document usecase."""
