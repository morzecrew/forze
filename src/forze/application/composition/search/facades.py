import attrs
from pydantic import BaseModel

from forze.application.execution import (
    FacadeOperationDescriptor,
    UsecasesFacade,
    namespaced_facade,
)
from forze.application.usecases.search import (
    RawCursorSearch,
    RawSearch,
    TypedCursorSearch,
    TypedSearch,
)

from .operations import SearchKernelOp

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchDTOs[M: BaseModel]:
    """DTO type mapping for a search aggregate."""

    read: type[M]
    """Read DTO type."""


# ....................... #


@namespaced_facade
@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchUsecasesFacade[M: BaseModel](UsecasesFacade):
    """Typed facade for search usecases."""

    raw_search = FacadeOperationDescriptor(
        SearchKernelOp.RAW,
        uc=RawSearch,
    )
    """Raw search usecase."""

    search = FacadeOperationDescriptor(
        SearchKernelOp.TYPED,
        uc=TypedSearch[M],
    )
    """Typed search usecase."""

    search_cursor = FacadeOperationDescriptor(
        SearchKernelOp.TYPED_CURSOR,
        uc=TypedCursorSearch[M],
    )
    """Typed search with cursor (keyset) pagination."""

    raw_search_cursor = FacadeOperationDescriptor(
        SearchKernelOp.RAW_CURSOR,
        uc=RawCursorSearch,
    )
    """Raw search with cursor (keyset) pagination."""
