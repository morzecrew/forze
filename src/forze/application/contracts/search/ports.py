"""Search query and command port definitions."""

from datetime import timedelta
from typing import (
    AsyncGenerator,
    Awaitable,
    Protocol,
    Sequence,
    TypeVar,
    runtime_checkable,
)

from pydantic import BaseModel

from forze.base.primitives import JsonDict

from .capabilities import DEFAULT_SEARCH_CAPABILITIES, SearchCapabilities
from .pages import SearchCountlessPage, SearchCursorPage, SearchPage
from ..querying import (
    CursorPaginationExpression,
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from .types import SearchOptions, SearchResultSnapshotOptions
from .value_objects import SearchResultSnapshotMeta

# ----------------------- #

T = TypeVar("T", bound=BaseModel)
M = TypeVar("M", bound=BaseModel)

# ....................... #


class SearchQueryPort[R: BaseModel, O: SearchOptions = SearchOptions](Protocol):
    """Full-text search with result shape encoded in method names.

    ``search*`` returns the spec read model ``R``; ``project_search*`` returns ``JsonDict`` rows;
    ``select_search*`` validates rows as ``return_type``. Methods without ``_page`` or ``_cursor``
    return :class:`~.SearchCountlessPage` (no total count query); ``*_page`` returns
    :class:`~.SearchPage`; ``*_cursor`` returns :class:`~.SearchCursorPage`.

    ``O`` is the per-request ``options`` type: the backend- and topology-agnostic
    :class:`~.SearchOptions` for single-index search (the default), widened to
    :class:`~.MultiSourceSearchOptions` for hub / federated ports so callers may pass the
    member-selection keys. ``O`` is an input-only (contravariant) param, so an adapter whose
    methods accept the base :class:`~.SearchOptions` still satisfies the multi-source port.
    """

    @property
    def search_capabilities(self) -> SearchCapabilities:
        """What retrieval features this adapter can serve (vector, fusion, filtered-ANN,
        engine-side embedding). Lets a caller introspect — and the adapter fail closed on —
        a feature the backend does not support, instead of a silent empty page. Defaults to
        the plain keyword single-index surface (:data:`.DEFAULT_SEARCH_CAPABILITIES`)."""
        return DEFAULT_SEARCH_CAPABILITIES

    def search(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: O | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> Awaitable[SearchCountlessPage[R]]:
        """Search and return typed read models (no total count query)."""
        ...  # pragma: no cover

    def search_page(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: O | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> Awaitable[SearchPage[R]]:
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
        options: O | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> Awaitable[SearchCountlessPage[JsonDict]]:
        """Search with field projection (no total count query).

        ``fields`` accepts dotted paths: ``contract.reg_number`` returns the nested
        ``{"contract": {"reg_number": ...}}`` shape, the same as document projection. The
        rule applies to every ``project_search_*`` method on this port.
        """
        ...  # pragma: no cover

    def project_search_page(
        self,
        fields: Sequence[str],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: O | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> Awaitable[SearchPage[JsonDict]]:
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
        options: O | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> Awaitable[SearchCountlessPage[T]]:
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
        options: O | None = None,
        snapshot: SearchResultSnapshotOptions | None = None,
    ) -> Awaitable[SearchPage[T]]:
        """Search as ``return_type`` with total matching count."""
        ...  # pragma: no cover

    def search_cursor(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
        *,
        options: O | None = None,
    ) -> Awaitable[SearchCursorPage[R]]:
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
        options: O | None = None,
    ) -> Awaitable[SearchCursorPage[JsonDict]]:
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
        options: O | None = None,
    ) -> Awaitable[SearchCursorPage[T]]:
        """Keyset / cursor page validating each hit as ``return_type``."""
        ...  # pragma: no cover

    def search_stream(
        self,
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        sorts: QuerySortExpression | None = None,
        *,
        options: O | None = None,
        chunk_size: int = 500,
    ) -> AsyncGenerator[Sequence[R]]:
        """Yield keyset chunks of the ranked result set for a bounded-memory export.

        No total count, no snapshot: iterates the whole matching set a chunk at a time
        (peak memory is one chunk) via the keyset cursor, in relevance order. A concurrent
        write may shift a hit between chunks — use a result snapshot for a frozen order.
        Requires :attr:`~.SearchCapabilities.supports_stream`; an offset-only (Meilisearch)
        or top-k (vector) backend refuses with ``query_feature_unsupported``.
        """
        ...  # pragma: no cover

    def project_search_stream(
        self,
        fields: Sequence[str],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        sorts: QuerySortExpression | None = None,
        *,
        options: O | None = None,
        chunk_size: int = 500,
    ) -> AsyncGenerator[Sequence[JsonDict]]:
        """Yield keyset export chunks with dotted-path field projection (no total count)."""
        ...  # pragma: no cover

    def select_search_stream(
        self,
        return_type: type[T],
        query: str | Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        sorts: QuerySortExpression | None = None,
        *,
        options: O | None = None,
        chunk_size: int = 500,
    ) -> AsyncGenerator[Sequence[T]]:
        """Yield keyset export chunks validated as ``return_type`` (no total count)."""
        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class SearchManagementPort(Protocol):
    """Control-plane provisioning for an external search index.

    Kept **separate** from the data-plane :class:`SearchCommandPort`
    (upsert / delete): index creation mutates shared topology and a full wipe is
    destructive admin — both run outside the request path (typically once at
    startup or in tooling), so a request-path writer never sees them. Mirrors the
    framework's management/data split (e.g. ``StreamGroupAdminPort`` vs
    ``StreamGroupQueryPort``).
    """

    def ensure_index(self) -> Awaitable[None]:
        """Create or update the backing index settings for the configured search surface."""
        ...  # pragma: no cover

    def delete_all(self) -> Awaitable[None]:
        """Remove all documents from the search index."""
        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class SearchCommandPort[M: BaseModel](Protocol):
    """Maintain documents in an external search index (e.g. Meilisearch).

    Data-plane only — index provisioning (``ensure_index``) and the full wipe
    (``delete_all``) live on :class:`SearchManagementPort`.
    """

    def upsert(self, documents: Sequence[M]) -> Awaitable[None]:
        """Add or update documents in the search index."""
        ...  # pragma: no cover

    def upsert_many(self, documents: Sequence[M]) -> Awaitable[None]:
        """Batch add or update documents in the search index."""
        ...  # pragma: no cover

    def delete(self, ids: Sequence[str]) -> Awaitable[None]:
        """Remove documents from the search index by primary key."""
        ...  # pragma: no cover


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
