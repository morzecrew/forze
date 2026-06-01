"""In-memory analytics adapter."""

from __future__ import annotations

from typing import (
    Any,
    AsyncGenerator,
    Sequence,
    cast,
    final,
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
from forze.application.contracts.analytics.specs import AnalyticsQueryDefinition
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
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.serialization import (
    pydantic_dump,
    pydantic_validate,
    pydantic_validate_many,
)
from forze_mock.query._types import T
from forze_mock.query.cursors import (
    _mock_cursor_start_and_limit,  # type: ignore[reportPrivateUsage]
    _mock_cursor_tokens,  # type: ignore[reportPrivateUsage]
)
from forze_mock.state import MockState
from forze_mock.tenancy import MockTenancyMixin, partition_namespace


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockAnalyticsAdapter[R: BaseModel, Ing: BaseModel](
    MockTenancyMixin,
    AnalyticsQueryPort[R],
    AnalyticsIngestPort[Ing],
):
    """In-memory analytics adapter over seeded query hits and an ingest log."""

    state: MockState
    spec: AnalyticsSpec[R, Ing]

    # ....................... #

    def _route(self) -> str:
        return partition_namespace(
            self.require_tenant_if_aware(),
            str(self.spec.name),
        )

    # ....................... #

    def _definition(self, query_key: str) -> AnalyticsQueryDefinition:
        try:
            return self.spec.queries[query_key]
        except KeyError as e:
            raise exc.internal(f"Unknown analytics query key: {query_key!r}") from e

    # ....................... #

    def _validated_params(self, query_key: str, params: BaseModel) -> BaseModel:
        defn = self._definition(query_key)
        if isinstance(params, defn.params):
            return params
        if isinstance(
            params, BaseModel
        ):  # pyright: ignore[reportUnnecessaryIsInstance]
            return pydantic_validate(defn.params, params.model_dump())
        raise exc.internal("Analytics params must be a Pydantic model instance.")

    # ....................... #

    def _query_rows(self, query_key: str) -> list[JsonDict]:
        with self.state.lock:
            route_store = self.state.analytics_query_hits.setdefault(self._route(), {})
            return [dict(row) for row in route_store.get(query_key, [])]

    # ....................... #

    def _dry_run(self, options: AnalyticsRunOptions | None) -> bool:
        return bool((options or {}).get("dry_run"))

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

    def _to_typed(self, rows: list[JsonDict]) -> list[R]:
        return pydantic_validate_many(self.spec.read, rows)

    # ....................... #

    def _to_projected(
        self,
        rows: list[JsonDict],
        fields: Sequence[str],
    ) -> list[JsonDict]:
        return [{k: row.get(k) for k in fields} for row in rows]

    # ....................... #

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
        _ = self._validated_params(query_key, params)
        if self._dry_run(options):
            empty: list[Any] = []
            if return_count:
                return page_from_limit_offset(empty, pagination, total=0)
            return page_from_limit_offset(empty, pagination, total=None)

        rows = self._apply_max_rows(self._query_rows(query_key), options)
        if return_fields is not None:
            data: list[Any] = self._to_projected(rows, return_fields)
        elif return_type is not None:
            data = pydantic_validate_many(return_type, rows)
        else:
            data = self._to_typed(rows)

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
        _ = self._validated_params(query_key, params)
        if self._dry_run(options):
            return CursorPage(
                hits=[],
                next_cursor=None,
                prev_cursor=None,
                has_more=False,
            )

        rows = self._apply_max_rows(self._query_rows(query_key), options)
        start, lim = _mock_cursor_start_and_limit(cursor)
        window = rows[start : start + lim + 1]
        has_more = len(window) > lim
        page_rows = window[:lim]

        if return_fields is not None:
            hits: list[Any] = self._to_projected(page_rows, return_fields)
        elif return_type is not None:
            hits = pydantic_validate_many(return_type, page_rows)
        else:
            hits = self._to_typed(page_rows)

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
        if fetch_batch_size < 1:
            raise exc.internal("fetch_batch_size must be >= 1")
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
        return await self._offset_page(  # type: ignore[return-value]
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
        return await self._offset_page(  # type: ignore[return-value]
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
        return await self._offset_page(  # type: ignore[return-value]
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
            raise exc.internal(
                f"Analytics ingest is not configured for route {self._route()!r}."
            )
        if not rows:
            return AnalyticsAppendResult(accepted=0)

        ingest_type = self.spec.ingest
        accepted = 0
        payloads: list[JsonDict] = []

        for row in rows:
            if isinstance(row, ingest_type):
                payloads.append(pydantic_dump(row))
            elif isinstance(
                row, BaseModel
            ):  # pyright: ignore[reportUnnecessaryIsInstance]
                payloads.append(
                    pydantic_dump(pydantic_validate(ingest_type, row.model_dump()))
                )
            else:
                raise exc.internal(
                    "Analytics ingest rows must be Pydantic model instances."
                )
            accepted += 1

        with self.state.lock:
            log = self.state.analytics_ingest_log.setdefault(self._route(), [])
            log.extend(payloads)

        return AnalyticsAppendResult(accepted=accepted)
