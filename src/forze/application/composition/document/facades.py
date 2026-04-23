from typing import Any, Generic, TypeVar

import attrs

from forze.application.execution import UsecasesFacade, facade_op
from forze.application.usecases.document import (
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
from forze.domain.models import BaseDTO, ReadDocument

from .operations import DocumentOperation

# ----------------------- #

R = TypeVar("R", bound=ReadDocument)
C = TypeVar("C", bound=BaseDTO, default=BaseDTO)
U = TypeVar("U", bound=BaseDTO, default=BaseDTO)

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentDTOs(Generic[R, C, U]):
    """DTO type mapping for a document aggregate."""

    read: type[R]
    """Get command type (e.g. :class:`ReadDocument`)."""

    create: type[C] | None = attrs.field(default=None)
    """Create command type; optional when create is not supported."""

    update: type[U] | None = attrs.field(default=None)
    """Update command type; optional when update is not supported."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentUsecasesFacade(UsecasesFacade, Generic[R, C, U]):
    """Typed facade for document usecases."""

    get = facade_op(
        DocumentOperation.GET,
        uc=GetDocument[R],
    )
    """Get document usecase."""

    get_by_number_id = facade_op(
        DocumentOperation.GET_BY_NUMBER_ID,
        uc=GetDocumentByNumberId[R],
    )
    """Get document by number ID usecase."""

    list = facade_op(
        DocumentOperation.LIST,
        uc=TypedListDocuments[R],
    )
    """List documents usecase."""

    raw_list = facade_op(
        DocumentOperation.RAW_LIST,
        uc=RawListDocuments,
    )
    """Raw list documents usecase."""

    list_cursor = facade_op(
        DocumentOperation.LIST_CURSOR,
        uc=TypedCursorListDocuments[R],
    )
    """List documents with cursor (keyset) pagination."""

    raw_list_cursor = facade_op(
        DocumentOperation.RAW_LIST_CURSOR,
        uc=RawCursorListDocuments,
    )
    """Raw list with cursor (keyset) pagination."""

    create = facade_op(
        DocumentOperation.CREATE,
        uc=CreateDocument[C, Any, R],
    )
    """Create document usecase."""

    update = facade_op(
        DocumentOperation.UPDATE,
        uc=UpdateDocument[U, Any, R],
    )
    """Update document usecase."""

    kill = facade_op(
        DocumentOperation.KILL,
        uc=KillDocument,
    )
    """Kill document usecase."""

    delete = facade_op(
        DocumentOperation.DELETE,
        uc=DeleteDocument[R],
    )
    """Delete document usecase."""

    restore = facade_op(
        DocumentOperation.RESTORE,
        uc=RestoreDocument[R],
    )
    """Restore document usecase."""
