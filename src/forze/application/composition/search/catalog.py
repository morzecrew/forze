"""Search operation catalog for transport attach (protocol-agnostic)."""

from dataclasses import dataclass
from typing import Any, Final

from forze.application.composition.search.operations import SearchKernelOp
from forze.application.handlers.search import (
    CursorSearchRequestDTO,
    ProjectedCursorSearchRequestDTO,
    ProjectedSearchRequestDTO,
    SearchRequestDTO,
)
from forze.base.primitives import StrKey

# ----------------------- #
#! Super useless isn't it?


@dataclass(frozen=True, slots=True)
class SearchOperationEntry:
    """One attachable search operation."""

    enable_name: str
    facade_attr: str
    kernel_op: StrKey
    body_type: type[Any]


class SearchPreset:
    """Named sets of search operations for ``enable=``."""

    TYPED: Final = ("search",)
    ALL: Final = ("search", "raw_search", "search_cursor", "raw_search_cursor")


SEARCH_OPERATIONS: dict[str, SearchOperationEntry] = {
    "search": SearchOperationEntry(
        "search",
        "search",
        SearchKernelOp.TYPED,
        SearchRequestDTO,
    ),
    "raw_search": SearchOperationEntry(
        "raw_search",
        "projected_search",
        SearchKernelOp.RAW,
        ProjectedSearchRequestDTO,
    ),
    "search_cursor": SearchOperationEntry(
        "search_cursor",
        "cursor_search",
        SearchKernelOp.TYPED_CURSOR,
        CursorSearchRequestDTO,
    ),
    "raw_search_cursor": SearchOperationEntry(
        "raw_search_cursor",
        "projected_cursor_search",
        SearchKernelOp.RAW_CURSOR,
        ProjectedCursorSearchRequestDTO,
    ),
}
