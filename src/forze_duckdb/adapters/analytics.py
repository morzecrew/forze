"""DuckDB implementation of the analytics query port (query-only)."""

from datetime import timedelta
from typing import Any, AsyncGenerator, Sequence, TypeVar, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryPort,
    AnalyticsRunOptions,
    AnalyticsSpec,
)
from forze.application.contracts.base import (
    CountlessPage,
    CursorPage,
    Page,
)
from forze.application.contracts.querying import (
    CursorPaginationExpression,
    PaginationExpression,
)
from forze.application.integrations.analytics import AnalyticsQueryPortMixin
from forze.application.integrations.analytics.adapter_common import (
    dry_run_enabled,
    dry_run_offset_page,
    encode_offset_cursor_next_prev,
    execute_analytics_offset_page,
    parse_count_row,
    parse_offset_cursor_after,
    shape_rows,
    validated_params,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.serialization import default_model_codec

from forze_duckdb.execution.deps.configs import (
    DuckDbAnalyticsConfig,
    DuckDbQueryConfig,
)
from forze_duckdb.kernel.client import DuckDbClientPort, build_count_sql

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
T = TypeVar("T", bound=BaseModel)

_DUCKDB_BACKWARD_CURSOR = "Backward analytics cursors are not supported on DuckDB."

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DuckDbAnalyticsAdapter[R: BaseModel](
    AnalyticsQueryPortMixin[R],
    AnalyticsQueryPort[R],
):
    """Analytics query port backed by DuckDB via :class:`~forze_duckdb.kernel.client.DuckDbClient`."""

    client: DuckDbClientPort
    spec: AnalyticsSpec[R, Any]
    config: DuckDbAnalyticsConfig

    # ....................... #

    def _query_config(self, query_key: str) -> DuckDbQueryConfig:
        try:
            return self.config.queries[query_key]

        except KeyError as e:
            raise exc.precondition(f"Unknown analytics query key: {query_key!r}") from e

    # ....................... #

    def _validated_params(self, query_key: str, params: BaseModel) -> BaseModel:
        return validated_params(self.spec, query_key, params)

    # ....................... #

    def _sql(self, query_key: str) -> str:
        return self._query_config(query_key).sql

    # ....................... #

    def _skip_total(self, query_key: str) -> bool:
        return self._query_config(query_key).skip_total

    # ....................... #

    @staticmethod
    def _run_timeout(options: AnalyticsRunOptions | None) -> timedelta | None:
        if options is None:
            return None

        return options.get("timeout")

    # ....................... #

    async def _fetch_rows(
        self,
        query_key: str,
        params: BaseModel,
        *,
        options: AnalyticsRunOptions | None,
        limit: int | None,
        offset: int | None,
    ) -> list[JsonDict]:
        max_rows = (options or {}).get("max_rows")
        result = await self.client.run_query(
            self._sql(query_key),
            params,
            limit=limit,
            offset=offset,
            max_rows=int(max_rows) if max_rows is not None else None,
            timeout=self._run_timeout(options),
        )

        return result.rows

    # ....................... #

    async def _total_count(
        self,
        query_key: str,
        params: BaseModel,
        *,
        options: AnalyticsRunOptions | None,
    ) -> int:
        result = await self.client.run_query(
            build_count_sql(self._sql(query_key)),
            params,
            limit=1,
            timeout=self._run_timeout(options),
        )

        return parse_count_row(result.rows)

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
        params = self._validated_params(query_key, params)

        if dry_run_enabled(options):
            return dry_run_offset_page(pagination, return_count=return_count)

        async def _fetch(limit: int | None, offset: int | None) -> list[JsonDict]:
            return await self._fetch_rows(
                query_key,
                params,
                options=options,
                limit=limit,
                offset=offset,
            )

        return await execute_analytics_offset_page(
            pagination=pagination,
            return_count=return_count,
            return_type=return_type,
            return_fields=return_fields,
            read_codec=self.spec.resolved_read_codec,
            read_type=self.spec.read,
            skip_total=self._skip_total(query_key),
            fetch_rows=_fetch,
            total_count=lambda: self._total_count(query_key, params, options=options),
        )

    # ....................... #

    async def run_chunked(
        self,
        query_key: str,
        params: BaseModel,
        pagination: PaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
        fetch_batch_size: int = 2000,
    ) -> AsyncGenerator[Sequence[R]]:
        if fetch_batch_size <= 0:
            raise exc.precondition("fetch_batch_size must be a positive integer.")

        params = self._validated_params(query_key, params)

        if dry_run_enabled(options):
            return

        max_rows = (options or {}).get("max_rows")
        rows = await self.client.run_query_all_pages(
            self._sql(query_key),
            params,
            max_rows=int(max_rows) if max_rows is not None else None,
            timeout=self._run_timeout(options),
            fetch_batch_size=fetch_batch_size,
        )

        typed = self.spec.resolved_read_codec.decode_mapping_many(rows)

        for offset in range(0, len(typed), fetch_batch_size):
            yield typed[offset : offset + fetch_batch_size]

    # ....................... #

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
        if fetch_batch_size <= 0:
            raise exc.precondition("fetch_batch_size must be a positive integer.")

        params = self._validated_params(query_key, params)

        if dry_run_enabled(options):
            return

        max_rows = (options or {}).get("max_rows")
        rows = await self.client.run_query_all_pages(
            self._sql(query_key),
            params,
            max_rows=int(max_rows) if max_rows is not None else None,
            timeout=self._run_timeout(options),
            fetch_batch_size=fetch_batch_size,
        )
        typed = default_model_codec(return_type).decode_mapping_many(rows)

        for offset in range(0, len(typed), fetch_batch_size):
            yield typed[offset : offset + fetch_batch_size]

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

        if dry_run_enabled(options):
            return CursorPage(
                hits=[], next_cursor=None, prev_cursor=None, has_more=False
            )

        start, lim = parse_offset_cursor_after(
            cursor,
            backward_not_supported=_DUCKDB_BACKWARD_CURSOR,
        )
        rows = await self._fetch_rows(
            query_key,
            params,
            options=options,
            limit=lim,
            offset=start,
        )
        hits = shape_rows(
            rows,
            read_codec=self.spec.resolved_read_codec,
            read_type=self.spec.read,
            return_type=return_type,
            return_fields=return_fields,
        )
        next_c, prev_c = encode_offset_cursor_next_prev(
            start=start,
            page_len=len(hits),
            limit=lim,
        )

        return CursorPage(
            hits=hits,
            next_cursor=next_c,
            prev_cursor=prev_c,
            has_more=next_c is not None,
        )
