"""In-memory analytics adapter."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Mapping, Sequence
from typing import (
    Any,
    Literal,
    cast,
    final,
    overload,
)

import attrs
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsAppendResult,
    AnalyticsIngestPort,
    AnalyticsQueryPort,
    AnalyticsRunOptions,
    AnalyticsSpec,
)
from forze.application.contracts.base import (
    CountlessPage,
    CursorPage,
    Page,
    page_from_limit_offset,
)
from forze.application.contracts.querying import (
    CursorPaginationExpression,
    PaginationExpression,
)
from forze.application.integrations.analytics import (
    decrypt_and_shape_rows,
    encode_ingest_payloads,
)
from forze.application.integrations.analytics.adapter_common import (
    computed_rows_required,
    dry_run_enabled,
    validate_fetch_batch_size,
    validated_params,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze_mock.adapters.analytics_resolvers import MockAnalyticsRegistry, MockAnalyticsView
from forze_mock.query._types import T
from forze_mock.query.cursors import (
    _mock_cursor_start_and_limit,  # pyright: ignore[reportPrivateUsage]
    _mock_cursor_tokens,  # pyright: ignore[reportPrivateUsage]
)
from forze_mock.state import MockState
from forze_mock.tenancy import MockTenancyMixin


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockAnalyticsAdapter[R: BaseModel, Ing: BaseModel](
    MockTenancyMixin,
    AnalyticsQueryPort[R],
    AnalyticsIngestPort[Ing],
):
    """In-memory analytics adapter over seeded query hits, resolvers and an ingest log."""

    state: MockState
    spec: AnalyticsSpec[R, Ing]

    resolvers: MockAnalyticsRegistry | None = None
    """Resolvers computing a query's rows from current documents; ``None`` answers every query
    from the seeded hits."""

    view: MockAnalyticsView | None = None
    """The document view handed to a resolver; ``None`` reads each spec's rows from the
    namespace named after it, unpartitioned."""

    # ....................... #

    def _route(self) -> str:
        return self._partitioned_namespace(str(self.spec.name))

    # ....................... #

    def _validated_params(self, query_key: str, params: BaseModel) -> BaseModel:
        # The real adapters' helper, so an undeclared key is the same configuration error.
        return validated_params(self.spec, query_key, params)

    # ....................... #

    def _query_rows(self, query_key: str, params: BaseModel) -> list[JsonDict]:
        resolver = (
            None
            if self.resolvers is None
            else self.resolvers.resolver_for(str(self.spec.name), query_key)
        )

        if resolver is None:
            if computed_rows_required():
                raise exc.configuration(
                    f"Analytics query {str(self.spec.name)!r}/{query_key!r} has no mock "
                    "resolver, so the mock would answer it from seeded rows (or none) that no "
                    "write moves. Register one with MockAnalyticsRegistry.on() for a caller "
                    "that needs the answer computed from current documents, such as "
                    "query_invariant.",
                    code="mock.analytics.resolver_required",
                )

            with self.state.lock:
                route_store = self.state.analytics_query_hits.setdefault(self._route(), {})
                return [dict(row) for row in route_store.get(query_key, [])]

        # A tenant-aware route refuses with no tenant bound, and narrows the view to the bound
        # tenant's rows whatever the document route's partitioning.
        view = attrs.evolve(
            self.view or MockAnalyticsView(state=self.state),
            tenant_id=self.require_tenant_if_aware(),
        )
        rows: list[JsonDict] = []

        for row in resolver(view, params):
            if not isinstance(row, Mapping):  # pyright: ignore[reportUnnecessaryIsInstance]
                raise exc.internal(
                    f"Mock analytics resolver for {str(self.spec.name)!r}/{query_key!r} must "
                    f"return row mappings, got {type(row).__name__}.",
                )

            rows.append(dict(row))

        return rows

    # ....................... #

    def _dry_run(self, options: AnalyticsRunOptions | None) -> bool:
        return dry_run_enabled(options)

    # ....................... #

    def _apply_max_rows(
        self,
        rows: list[JsonDict],
        options: AnalyticsRunOptions | None,
    ) -> list[JsonDict]:
        opts = options or {}
        max_rows = opts.get("max_rows")
        if max_rows is not None and max_rows >= 0:
            return rows[: int(max_rows)]
        return rows

    # ....................... #

    @overload
    async def _offset_page(
        self,
        query_key: str,
        params: BaseModel,
        pagination: PaginationExpression | None,
        *,
        options: AnalyticsRunOptions | None,
        return_count: Literal[True],
        return_type: type[T] | None,
        return_fields: Sequence[str] | None,
    ) -> Page[Any]: ...

    @overload
    async def _offset_page(
        self,
        query_key: str,
        params: BaseModel,
        pagination: PaginationExpression | None,
        *,
        options: AnalyticsRunOptions | None,
        return_count: Literal[False],
        return_type: type[T] | None,
        return_fields: Sequence[str] | None,
    ) -> CountlessPage[Any]: ...

    async def _offset_page(
        self,
        query_key: str,
        params: BaseModel,
        pagination: PaginationExpression | None,
        *,
        options: AnalyticsRunOptions | None,
        return_count: bool,
        return_type: type[T] | None,
        return_fields: Sequence[str] | None,
    ) -> CountlessPage[Any] | Page[Any]:
        params = self._validated_params(query_key, params)
        if self._dry_run(options):
            empty: list[Any] = []
            if return_count:
                return page_from_limit_offset(empty, pagination, total=0)
            return page_from_limit_offset(empty, pagination, total=None)

        rows = self._apply_max_rows(self._query_rows(query_key, params), options)
        data = await decrypt_and_shape_rows(
            rows,
            read_codec=self.spec.resolved_read_codec,
            read_type=self.spec.read,
            return_type=return_type,
            return_fields=return_fields,
        )

        if return_count:
            return page_from_limit_offset(data, pagination, total=len(data))
        return page_from_limit_offset(data, pagination, total=None)

    # ....................... #

    async def _cursor_page(
        self,
        query_key: str,
        params: BaseModel,
        cursor: CursorPaginationExpression | None,
        *,
        options: AnalyticsRunOptions | None,
        return_type: type[T] | None,
        return_fields: Sequence[str] | None,
    ) -> CursorPage[Any]:
        params = self._validated_params(query_key, params)
        if self._dry_run(options):
            return CursorPage(
                hits=[],
                next_cursor=None,
                prev_cursor=None,
                has_more=False,
            )

        rows = self._apply_max_rows(self._query_rows(query_key, params), options)
        start, lim = _mock_cursor_start_and_limit(cursor)
        window = rows[start : start + lim + 1]
        has_more = len(window) > lim
        page_rows = window[:lim]

        hits = await decrypt_and_shape_rows(
            page_rows,
            read_codec=self.spec.resolved_read_codec,
            read_type=self.spec.read,
            return_type=return_type,
            return_fields=return_fields,
        )

        next_c, prev_c = _mock_cursor_tokens(start, len(page_rows), has_more=has_more)
        return CursorPage(
            hits=hits,
            next_cursor=next_c,
            prev_cursor=prev_c,
            has_more=has_more,
        )

    # ....................... #

    async def _chunked(
        self,
        query_key: str,
        params: BaseModel,
        pagination: PaginationExpression | None,
        *,
        options: AnalyticsRunOptions | None,
        return_type: type[T] | None,
        return_fields: Sequence[str] | None,
        fetch_batch_size: int,
    ) -> AsyncGenerator[Sequence[Any]]:
        validate_fetch_batch_size(fetch_batch_size)
        _ = self._validated_params(query_key, params)
        if self._dry_run(options):
            return

        page = await self._offset_page(
            query_key,
            params,
            pagination,
            options=options,
            return_count=False,
            return_type=return_type,
            return_fields=return_fields,
        )
        hits = page.hits
        for offset in range(0, len(hits), fetch_batch_size):
            yield hits[offset : offset + fetch_batch_size]

    # ....................... #

    async def run(
        self,
        query_key: str,
        params: BaseModel,
        pagination: PaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
    ) -> CountlessPage[R]:
        return await self._offset_page(
            query_key,
            params,
            pagination,
            options=options,
            return_count=False,
            return_type=None,
            return_fields=None,
        )

    async def run_page(
        self,
        query_key: str,
        params: BaseModel,
        pagination: PaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
    ) -> Page[R]:
        return await self._offset_page(
            query_key,
            params,
            pagination,
            options=options,
            return_count=True,
            return_type=None,
            return_fields=None,
        )

    async def run_chunked(
        self,
        query_key: str,
        params: BaseModel,
        pagination: PaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
        fetch_batch_size: int = 2000,
    ) -> AsyncGenerator[Sequence[R]]:
        async for chunk in self._chunked(
            query_key,
            params,
            pagination,
            options=options,
            return_type=None,
            return_fields=None,
            fetch_batch_size=fetch_batch_size,
        ):
            yield cast(Sequence[R], chunk)

    async def project_run(
        self,
        fields: Sequence[str],
        query_key: str,
        params: BaseModel,
        pagination: PaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
    ) -> CountlessPage[JsonDict]:
        return await self._offset_page(
            query_key,
            params,
            pagination,
            options=options,
            return_count=False,
            return_type=None,
            return_fields=fields,
        )

    async def project_run_page(
        self,
        fields: Sequence[str],
        query_key: str,
        params: BaseModel,
        pagination: PaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
    ) -> Page[JsonDict]:
        return await self._offset_page(
            query_key,
            params,
            pagination,
            options=options,
            return_count=True,
            return_type=None,
            return_fields=fields,
        )

    async def project_run_chunked(
        self,
        fields: Sequence[str],
        query_key: str,
        params: BaseModel,
        pagination: PaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
        fetch_batch_size: int = 2000,
    ) -> AsyncGenerator[Sequence[JsonDict]]:
        async for chunk in self._chunked(
            query_key,
            params,
            pagination,
            options=options,
            return_type=None,
            return_fields=fields,
            fetch_batch_size=fetch_batch_size,
        ):
            yield cast(Sequence[JsonDict], chunk)

    async def select_run(
        self,
        return_type: type[T],
        query_key: str,
        params: BaseModel,
        pagination: PaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
    ) -> CountlessPage[T]:
        return await self._offset_page(
            query_key,
            params,
            pagination,
            options=options,
            return_count=False,
            return_type=return_type,
            return_fields=None,
        )

    async def select_run_page(
        self,
        return_type: type[T],
        query_key: str,
        params: BaseModel,
        pagination: PaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
    ) -> Page[T]:
        return await self._offset_page(
            query_key,
            params,
            pagination,
            options=options,
            return_count=True,
            return_type=return_type,
            return_fields=None,
        )

    async def select_run_chunked(
        self,
        return_type: type[T],
        query_key: str,
        params: BaseModel,
        pagination: PaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
        fetch_batch_size: int = 2000,
    ) -> AsyncGenerator[Sequence[T]]:
        async for chunk in self._chunked(
            query_key,
            params,
            pagination,
            options=options,
            return_type=return_type,
            return_fields=None,
            fetch_batch_size=fetch_batch_size,
        ):
            yield cast(Sequence[T], chunk)

    async def run_cursor(
        self,
        query_key: str,
        params: BaseModel,
        cursor: CursorPaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
    ) -> CursorPage[R]:
        return await self._cursor_page(
            query_key,
            params,
            cursor,
            options=options,
            return_type=None,
            return_fields=None,
        )

    async def project_run_cursor(
        self,
        fields: Sequence[str],
        query_key: str,
        params: BaseModel,
        cursor: CursorPaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
    ) -> CursorPage[JsonDict]:
        return await self._cursor_page(
            query_key,
            params,
            cursor,
            options=options,
            return_type=None,
            return_fields=fields,
        )

    async def select_run_cursor(
        self,
        return_type: type[T],
        query_key: str,
        params: BaseModel,
        cursor: CursorPaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
    ) -> CursorPage[T]:
        return await self._cursor_page(
            query_key,
            params,
            cursor,
            options=options,
            return_type=return_type,
            return_fields=None,
        )

    # ....................... #

    async def append(self, rows: Sequence[Ing]) -> AnalyticsAppendResult | None:
        if self.spec.ingest is None:
            raise exc.internal(f"Analytics ingest is not configured for route {self._route()!r}.")
        if not rows:
            return AnalyticsAppendResult(accepted=0)

        ingest_codec = self.spec.resolved_ingest_codec
        if ingest_codec is None:
            raise exc.internal(
                f"Analytics ingest codec is not configured for route {self._route()!r}."
            )

        payloads = await encode_ingest_payloads(ingest_codec, list(rows))

        with self.state.lock:
            log = self.state.analytics_ingest_log.setdefault(self._route(), [])
            log.extend(payloads)

        return AnalyticsAppendResult(accepted=len(payloads))
