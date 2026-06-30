import attrs
from pydantic import BaseModel

from forze.application.contracts.search import SearchOptions
from forze.application.execution.operations.facade import (
    OperationFacade,
    facade_op,
    namespaced_facade,
)

from .handlers import (
    CursorSearch,
    ProjectedCursorSearch,
    ProjectedSearch,
    Search,
)
from .operations import SearchKernelOp

# ----------------------- #


@namespaced_facade
@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchFacade[M: BaseModel, Opt: SearchOptions](OperationFacade):
    """Typed facade for search operations."""

    projected_search = facade_op(
        SearchKernelOp.RAW,
        uc=ProjectedSearch[Opt],
    )
    """Projected search operation."""

    search = facade_op(
        SearchKernelOp.TYPED,
        uc=Search[M, Opt],
    )
    """Search operation."""

    cursor_search = facade_op(
        SearchKernelOp.TYPED_CURSOR,
        uc=CursorSearch[M, Opt],
    )
    """Cursor search operation."""

    projected_cursor_search = facade_op(
        SearchKernelOp.RAW_CURSOR,
        uc=ProjectedCursorSearch[Opt],
    )
    """Projected cursor search operation."""
