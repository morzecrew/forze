"""Meilisearch search port delegation; cursor pagination is unsupported."""

from typing import Any, Sequence

from pydantic import BaseModel

from forze.application.contracts.querying import (
    CursorPaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.contracts.search import SearchOptions
from forze.application.integrations.search import SimpleSearchPortMixin
from forze.base.exceptions import exc

# ----------------------- #


class MeilisearchSearchPortMixin[M: BaseModel](SimpleSearchPortMixin[M]):
    """Meilisearch :class:`~forze.application.contracts.search.SearchQueryPort` delegation.

    Offset/projection/select variants are inherited from
    :class:`~forze.application.integrations.search.SimpleSearchPortMixin`.
    Meilisearch does not support keyset cursor pagination: the simple adapter
    inherits the raising ``_cursor_search_impl`` below, while the federated
    adapter overrides it with a real implementation.
    """

    def _raise_cursor_not_supported(self) -> None:
        raise exc.internal(
            "search_cursor is not implemented for Meilisearch search; use search or "
            "search_page with limit/offset, or result snapshots for deep paging.",
        )

    # ....................... #

    async def _cursor_search_impl(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        return_type: type[BaseModel] | None = None,
        return_fields: Sequence[str] | None = None,
    ) -> Any:
        del query, filters, cursor, sorts, options, return_type, return_fields
        self._raise_cursor_not_supported()
