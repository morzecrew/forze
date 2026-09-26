"""Document query port methods."""

from collections.abc import AsyncGenerator, Sequence
from typing import Any, Generic, cast
from uuid import UUID

from forze.application.contracts.base import CountlessPage, CursorPage, Page
from forze.application.contracts.document import DocumentSpec, OwnedBy, RowLockMode
from forze.application.contracts.document.gateways import DocumentReadGatewayPort
from forze.application.contracts.querying import (
    AggregatesExpression,
    CursorPaginationExpression,
    PaginationExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

from ._pagination import (
    CursorQuery,
    DocumentPaginationMixin,
    OffsetQuery,
    StreamQuery,
)
from ._types import R, T
from .cache import DocumentCache


class DocumentQueryMixin(DocumentPaginationMixin[R], Generic[R]):
    """Query operations mixin for :class:`~.adapter.DocumentAdapter`."""

    read_gw: DocumentReadGatewayPort[R]
    document_cache: DocumentCache[R]
    spec: DocumentSpec[R, Any, Any, Any]

    # ....................... #

    async def get(
        self,
        pk: UUID,
        *,
        owned_by: OwnedBy | None = None,
        for_update: RowLockMode = False,
        skip_cache: bool = False,
    ) -> R:
        """Fetch a single document by primary key, using the cache when available.

        A locking read (``for_update``) always goes to the database: a cached copy cannot hold a
        row lock. On a direct read ``owned_by`` goes into the database predicate, so a foreign
        row is neither returned nor locked. A read through the cache (keyed by pk alone) fetches
        by pk and checks the row before returning it.
        """

        if not self.document_cache.id_rev_capable():
            raise exc.internal(
                f"Cannot get document of type '{type(self.read_gw.model_type).__name__}' as it does not have defined id field"
            )

        if owned_by is not None:
            owned_by.check(self.spec)

        async def fetch(lock: RowLockMode) -> R:
            if owned_by is None:
                return await self.read_gw.get(pk, for_update=lock)

            row = await self.read_gw.find(owned_by.filter(pk), for_update=lock)

            if row is None:
                raise exc.not_found(f"Record not found: {pk}")

            return row

        if for_update or not self.document_cache.read_through_eligible(
            skip_cache=skip_cache,
            return_fields=None,
        ):
            return await fetch(for_update)

        row = await self.document_cache.get_read_through(
            pk,
            fetch_on_cache_fault=lambda: fetch(False),
            fetch_on_miss_without_lock=lambda: self.read_gw.get(pk),
        )

        if owned_by is not None and not owned_by.owns(row):
            raise exc.not_found(f"Record not found: {pk}")

        return row

    # ....................... #

    async def get_many(
        self,
        pks: Sequence[UUID],
        *,
        owned_by: OwnedBy | None = None,
        skip_cache: bool = False,
    ) -> Sequence[R]:
        """Fetch multiple documents by primary key with cache-aware batching.

        With ``owned_by``, a missing or foreign id fails the whole call with one summary that
        names no id, and the owner is checked on the rows read (no lock is taken, so checking
        after the read reveals nothing a predicate would hide).
        """

        if owned_by is None:
            return await self._get_many(pks, skip_cache=skip_cache)

        owned_by.check(self.spec)

        return await owned_by.read_batch(self._get_many(pks, skip_cache=skip_cache))

    async def _get_many(self, pks: Sequence[UUID], *, skip_cache: bool) -> Sequence[R]:

        if not pks:
            return []

        if not self.document_cache.id_rev_capable():
            raise exc.internal(
                f"Cannot get many documents of type '{type(self.read_gw.model_type).__name__}' as it does not have defined id field"
            )

        if not self.document_cache.read_through_eligible(
            skip_cache=skip_cache,
            return_fields=None,
        ):
            return await self.read_gw.get_many(pks)

        return await self.document_cache.get_many_read_through(
            pks,
            fetch_many_on_cache_fault=lambda: self.read_gw.get_many(pks),
            fetch_misses_many=lambda misses: self.read_gw.get_many([UUID(x) for x in misses]),
        )

    # ....................... #

    async def find(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        *,
        for_update: RowLockMode = False,
    ) -> R | None:
        """Find a single document matching the given filters."""

        return await self.read_gw.find(filters, for_update=for_update)

    # ....................... #

    async def project(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        fields: Sequence[str],
        *,
        for_update: RowLockMode = False,
    ) -> JsonDict | None:
        """Find one document matching filters and project ``fields``."""

        return await self.read_gw.find(
            filters,
            for_update=for_update,
            return_fields=tuple(fields),
        )

    # ....................... #

    async def select(
        self,
        filters: QueryFilterExpression,  # type: ignore[valid-type]
        return_type: type[T],
        *,
        for_update: RowLockMode = False,
    ) -> T | None:
        """Find one document matching filters as ``return_type``."""

        return await self.read_gw.find(
            filters,
            for_update=for_update,
            return_model=return_type,
        )

    # ....................... #
    async def find_many(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> CountlessPage[R]:
        return await self._offset_page(
            OffsetQuery(
                return_count=False,
                aggregates=None,
                return_model=None,
                return_fields=None,
            ),
            filters=filters,
            pagination=pagination,
            sorts=sorts,
        )

    # ....................... #

    async def project_many(
        self,
        fields: Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> CountlessPage[JsonDict]:
        return await self._offset_page(
            OffsetQuery(
                return_count=False,
                aggregates=None,
                return_model=None,
                return_fields=tuple(fields),
            ),
            filters=filters,
            pagination=pagination,
            sorts=sorts,
        )

    # ....................... #

    async def select_many(
        self,
        return_type: type[T],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> CountlessPage[T]:
        return await self._offset_page(
            OffsetQuery(
                return_count=False,
                aggregates=None,
                return_model=return_type,
                return_fields=None,
            ),
            filters=filters,
            pagination=pagination,
            sorts=sorts,
        )

    # ....................... #

    async def find_page(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Page[R]:
        return await self._offset_page(
            OffsetQuery(
                return_count=True,
                aggregates=None,
                return_model=None,
                return_fields=None,
            ),
            filters=filters,
            pagination=pagination,
            sorts=sorts,
        )

    # ....................... #

    async def project_page(
        self,
        fields: Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Page[JsonDict]:
        return await self._offset_page(
            OffsetQuery(
                return_count=True,
                aggregates=None,
                return_model=None,
                return_fields=tuple(fields),
            ),
            filters=filters,
            pagination=pagination,
            sorts=sorts,
        )

    # ....................... #

    async def select_page(
        self,
        return_type: type[T],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Page[T]:
        return await self._offset_page(
            OffsetQuery(
                return_count=True,
                aggregates=None,
                return_model=return_type,
                return_fields=None,
            ),
            filters=filters,
            pagination=pagination,
            sorts=sorts,
        )

    # ....................... #

    async def aggregate_many(
        self,
        aggregates: AggregatesExpression,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> CountlessPage[JsonDict]:
        return await self._offset_page(
            OffsetQuery(
                return_count=False,
                aggregates=aggregates,
                return_model=None,
                return_fields=None,
            ),
            filters=filters,
            pagination=pagination,
            sorts=sorts,
        )

    # ....................... #

    async def aggregate_page(
        self,
        aggregates: AggregatesExpression,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Page[JsonDict]:
        return await self._offset_page(
            OffsetQuery(
                return_count=True,
                aggregates=aggregates,
                return_model=None,
                return_fields=None,
            ),
            filters=filters,
            pagination=pagination,
            sorts=sorts,
        )

    # ....................... #

    async def select_many_aggregated(
        self,
        return_type: type[T],
        aggregates: AggregatesExpression,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> CountlessPage[T]:
        return await self._offset_page(
            OffsetQuery(
                return_count=False,
                aggregates=aggregates,
                return_model=return_type,
                return_fields=None,
            ),
            filters=filters,
            pagination=pagination,
            sorts=sorts,
        )

    # ....................... #

    async def select_page_aggregated(
        self,
        return_type: type[T],
        aggregates: AggregatesExpression,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        pagination: PaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> Page[T]:
        return await self._offset_page(
            OffsetQuery(
                return_count=True,
                aggregates=aggregates,
                return_model=return_type,
                return_fields=None,
            ),
            filters=filters,
            pagination=pagination,
            sorts=sorts,
        )

    # ....................... #

    async def find_cursor(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> CursorPage[R]:
        return cast(
            CursorPage[R],
            await self._cursor_page(
                CursorQuery(return_model=None, return_fields=None),
                filters=filters,
                cursor=cursor,
                sorts=sorts,
            ),
        )

    # ....................... #

    async def project_cursor(
        self,
        fields: Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> CursorPage[JsonDict]:
        return cast(
            CursorPage[JsonDict],
            await self._cursor_page(
                CursorQuery(return_model=None, return_fields=tuple(fields)),
                filters=filters,
                cursor=cursor,
                sorts=sorts,
            ),
        )

    # ....................... #

    async def select_cursor(
        self,
        return_type: type[T],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        cursor: CursorPaginationExpression | None = None,
        sorts: QuerySortExpression | None = None,
    ) -> CursorPage[T]:
        return cast(
            CursorPage[T],
            await self._cursor_page(
                CursorQuery(return_model=return_type, return_fields=None),
                filters=filters,
                cursor=cursor,
                sorts=sorts,
            ),
        )

    # ....................... #

    async def find_stream(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        *,
        sorts: QuerySortExpression | None = None,
        chunk_size: int = 500,
    ) -> AsyncGenerator[Sequence[R]]:
        async for raw_chunk in self._stream(  # type: ignore[var-annotated]
            StreamQuery(return_model=None, return_fields=None),
            filters=filters,
            sorts=sorts,
            chunk_size=chunk_size,
        ):
            yield cast(Sequence[R], raw_chunk)

    # ....................... #

    async def project_stream(
        self,
        fields: Sequence[str],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        *,
        sorts: QuerySortExpression | None = None,
        chunk_size: int = 500,
    ) -> AsyncGenerator[Sequence[JsonDict]]:
        async for raw_chunk in self._stream(  # type: ignore[var-annotated]
            StreamQuery(return_model=None, return_fields=tuple(fields)),
            filters=filters,
            sorts=sorts,
            chunk_size=chunk_size,
        ):
            yield cast(Sequence[JsonDict], raw_chunk)

    # ....................... #

    async def select_stream(
        self,
        return_type: type[T],
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        *,
        sorts: QuerySortExpression | None = None,
        chunk_size: int = 500,
    ) -> AsyncGenerator[Sequence[T]]:
        async for raw_chunk in self._stream(  # type: ignore[var-annotated]
            StreamQuery(return_model=return_type, return_fields=None),
            filters=filters,
            sorts=sorts,
            chunk_size=chunk_size,
        ):
            yield cast(Sequence[T], raw_chunk)

    # ....................... #

    async def count(self, filters: QueryFilterExpression | None = None) -> int:  # type: ignore[valid-type]
        """Count documents matching the given filters.

        :param filters: Optional filter expression.
        """

        return await self.read_gw.count(filters)
