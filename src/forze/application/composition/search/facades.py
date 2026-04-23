import attrs
from pydantic import BaseModel

from forze.application.execution import UsecasesFacade, facade_op
from forze.application.usecases.search import (
    RawCursorSearch,
    RawSearch,
    TypedCursorSearch,
    TypedSearch,
)

from .operations import SearchOperation

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchDTOs[M: BaseModel]:
    """DTO type mapping for a search aggregate."""

    read: type[M]
    """Read DTO type."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchUsecasesFacade[M: BaseModel](UsecasesFacade):
    """Typed facade for search usecases."""

    raw_search = facade_op(SearchOperation.RAW_SEARCH, uc=RawSearch)
    """Raw search usecase."""

    search = facade_op(SearchOperation.TYPED_SEARCH, uc=TypedSearch[M])
    """Typed search usecase."""

    search_cursor = facade_op(
        SearchOperation.TYPED_SEARCH_CURSOR,
        uc=TypedCursorSearch[M],
    )
    """Typed search with cursor (keyset) pagination."""

    raw_search_cursor = facade_op(
        SearchOperation.RAW_SEARCH_CURSOR,
        uc=RawCursorSearch,
    )
    """Raw search with cursor (keyset) pagination."""
