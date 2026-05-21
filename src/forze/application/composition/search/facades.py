import attrs
from pydantic import BaseModel

from forze.application.execution.facade import (
    OperationFacade,
    facade_op,
    namespaced_facade,
)
from forze.application.handlers.search import (
    RawCursorSearch,
    RawSearch,
    TypedCursorSearch,
    TypedSearch,
)

from .operations import SearchKernelOp

# ----------------------- #


@namespaced_facade
@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchFacade[M: BaseModel](OperationFacade):
    """Typed facade for search operations."""

    raw_search = facade_op(
        SearchKernelOp.RAW,
        uc=RawSearch,
    )
    """Raw search operation."""

    search = facade_op(
        SearchKernelOp.TYPED,
        uc=TypedSearch[M],
    )
    """Typed search operation."""

    search_cursor = facade_op(
        SearchKernelOp.TYPED_CURSOR,
        uc=TypedCursorSearch[M],
    )
    """Typed search with cursor (keyset) pagination operation."""

    raw_search_cursor = facade_op(
        SearchKernelOp.RAW_CURSOR,
        uc=RawCursorSearch,
    )
    """Raw search with cursor (keyset) pagination operation."""
