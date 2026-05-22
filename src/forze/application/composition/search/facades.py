import attrs
from pydantic import BaseModel

from forze.application.execution.facade import (
    OperationFacade,
    facade_op,
    namespaced_facade,
)
from forze.application.handlers.search import (
    CursorSearch,
    ProjectedCursorSearch,
    ProjectedSearch,
    Search,
)

from .operations import SearchKernelOp

# ----------------------- #


@namespaced_facade
@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchFacade[M: BaseModel](OperationFacade):
    """Typed facade for search operations."""

    projected_search = facade_op(
        SearchKernelOp.RAW,
        uc=ProjectedSearch,
    )
    """Projected search operation."""

    search = facade_op(
        SearchKernelOp.TYPED,
        uc=Search[M],
    )
    """Search operation."""

    cursor_search = facade_op(
        SearchKernelOp.TYPED_CURSOR,
        uc=CursorSearch[M],
    )
    """Cursor search operation."""

    projected_cursor_search = facade_op(
        SearchKernelOp.RAW_CURSOR,
        uc=ProjectedCursorSearch,
    )
    """Projected cursor search operation."""
