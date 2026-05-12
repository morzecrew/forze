"""Search query and command port definitions.

**Cursor search (``search_cursor`` / ``project_search_cursor`` / ``select_search_cursor``):**
SQL and vector adapters must keyset within the same ranked ``ORDER BY`` (score columns +
tie-breakers, typically ``id``). That implies declaring cursor columns in :class:`.SearchSpec` or
Postgres search config, reusing the index heap primary key where applicable. Postgres hub and
simple adapters inject keyset columns into the query when using projection cursor methods, then
return only the requested fields. Federated (RRF) search does not implement cursors yet; use
offset methods such as :meth:`~SearchQueryPort.search` or :meth:`~SearchQueryPort.search_page`
with limit/offset there.
"""

from datetime import timedelta
from typing import Awaitable, Protocol, Sequence, TypeVar

from pydantic import BaseModel

from forze.base.primitives import JsonDict

from ..base import CountlessPage, CursorPage, Page
from ..query import (
    CursorPaginationExpression,
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from .types import SearchOptions, SearchResultSnapshotOptions
from .value_objects import SearchResultSnapshotMeta

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

# ....................... #


class SearchQueryPort[R: BaseModel](Protocol):
    """Full-text search with result shape encoded in method names.

    ``search*`` returns the spec read model ``R``; ``project_search*`` returns ``JsonDict`` rows;
    ``select_search*`` validates rows as ``return_type``. Methods without ``_page`` or ``_cursor``
    return :class:`~.CountlessPage` (no total count query); ``*_page`` returns :class:`~.Page`;
    ``*_cursor`` returns :class:`~.CursorPage`.
    """

    def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> Awaitable[CountlessPage[R]]:
        """Search and return typed read models (no total count query)."""
        ...  # pragma: no cover

    def search_page(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> Awaitable[Page[R]]:
        """Search and return typed read models with total matching count."""
        ...  # pragma: no cover

    def project_search(
        self,
        fields: Sequence[str],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> Awaitable[CountlessPage[JsonDict]]:
        """Search with field projection (no total count query)."""
        ...  # pragma: no cover

    def project_search_page(
        self,
        fields: Sequence[str],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> Awaitable[Page[JsonDict]]:
        """Search with field projection and total matching count."""
        ...  # pragma: no cover

    def select_search(
        self,
        return_type: type[T],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> Awaitable[CountlessPage[T]]:
        """Search validating each hit as ``return_type`` (no total count query)."""
        ...  # pragma: no cover

    def select_search_page(
        self,
        return_type: type[T],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> Awaitable[Page[T]]:
        """Search as ``return_type`` with total matching count."""
        ...  # pragma: no cover

    def search_cursor(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
    ) -> Awaitable[CursorPage[R]]:
        """Keyset / cursor page of typed read models."""
        ...  # pragma: no cover

    def project_search_cursor(
        self,
        fields: Sequence[str],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
    ) -> Awaitable[CursorPage[JsonDict]]:
        """Keyset / cursor page with field projection."""
        ...  # pragma: no cover

    def select_search_cursor(
        self,
        return_type: type[T],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: SearchOptions | None = None,
    ) -> Awaitable[CursorPage[T]]:
        """Keyset / cursor page validating each hit as ``return_type``."""
        ...  # pragma: no cover


# ....................... #


#! Not implemented yet
class SearchCommandPort[M: BaseModel](Protocol): ...  # pragma: no cover


# ....................... #


class SearchResultSnapshotPort(Protocol):
    """Store an ordered list of document identifiers for fast paged reads without re-search.

    Implementations (e.g. Redis with chunked string values) may partition ``ordered_ids`` into
    multiple keys. Callers that need streaming writes can use :meth:`begin_run` and
    :meth:`append_chunk` instead of a single :meth:`put_run`.
    """

    def put_run(
        self,
        *,
        run_id: str,
        fingerprint: str,
        ordered_ids: Sequence[str],
        ttl: timedelta | None = None,
        chunk_size: int | None = None,
    ) -> Awaitable[None]:
        """Write ``ordered_ids`` in order, split into fixed-size chunks, and set metadata.

        :param run_id: Opaque id for this run (e.g. UUID). Overwrites an existing run with the
            same id.
        :param fingerprint: For :meth:`get_id_range` validation.
        :param ordered_ids: Final relevance order. Empty is allowed.
        :param ttl: Expiry for the meta and every chunk key; ``None`` uses the adapter default
            (from :class:`.SearchResultSnapshotSpec` when built via DI).
        :param chunk_size: Chunk size; ``None`` uses the adapter default. Must be at least ``1``.
        """
        ...  # pragma: no cover

    def begin_run(
        self,
        *,
        run_id: str,
        fingerprint: str,
        chunk_size: int | None = None,
        ttl: timedelta | None = None,
    ) -> Awaitable[None]:
        """Start a multi-chunk run (incomplete until :meth:`append_chunk` with ``is_last=True``).

        ``ttl`` and ``chunk_size`` default to the adapter when ``None`` (typically from
        :class:`.SearchResultSnapshotSpec`).
        """
        ...  # pragma: no cover

    def append_chunk(
        self,
        *,
        run_id: str,
        chunk_index: int,
        ids: Sequence[str],
        is_last: bool,
    ) -> Awaitable[None]:
        """Append a chunk. ``chunk_index`` must be ``0, 1, …`` without gaps. When ``is_last`` is
        ``True``, total length and final metadata are finalized and TTL is refreshed.
        """
        ...  # pragma: no cover

    def get_id_range(
        self,
        run_id: str,
        offset: int,
        limit: int,
        *,
        expected_fingerprint: str | None = None,
    ) -> Awaitable[list[str] | None]:
        """Return up to ``limit`` IDs starting at ``offset`` in sort order, or ``None`` if the run
        is missing, incomplete, or ``expected_fingerprint`` does not match.
        If ``offset`` is past the end, returns an empty list when the run exists and matches.
        """
        ...  # pragma: no cover

    def get_meta(
        self,
        run_id: str,
    ) -> Awaitable[SearchResultSnapshotMeta | None]:
        """Return metadata for the run, or ``None`` if there is no meta key."""
        ...  # pragma: no cover

    def delete_run(
        self,
        run_id: str,
    ) -> Awaitable[None]:
        """Delete meta and all chunk keys for the run, if they exist."""
        ...  # pragma: no cover
