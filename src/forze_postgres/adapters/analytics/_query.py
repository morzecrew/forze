"""Query execution helpers for Postgres analytics."""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Sequence, TypeVar, cast

from psycopg import sql
from psycopg.abc import QueryNoTemplate
from pydantic import BaseModel

from forze.application.contracts.analytics import AnalyticsRunOptions, AnalyticsSpec
from forze.application.contracts.analytics._adapter_common import (
    dry_run_enabled,
    dry_run_offset_page,
    pagination_window,
    parse_count_row,
    shape_rows,
    timeout_seconds,
    validated_params,
)
from forze.application.contracts.base import CountlessPage, Page, page_from_limit_offset
from forze.application.contracts.querying import PaginationExpression
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze_postgres.execution.deps.configs import (
    PostgresAnalyticsConfig,
    PostgresQueryConfig,
)
from forze_postgres.kernel.client import PostgresClientPort
from forze_postgres.kernel.sql import (
    apply_limit_offset,
    build_count_sql,
    parameters_from_model,
)

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
Ing = TypeVar("Ing", bound=BaseModel)
T = TypeVar("T", bound=BaseModel)


class PostgresAnalyticsQueryMixin[R: BaseModel, Ing: BaseModel]:
    """Config accessors, fetch, and offset pagination for analytics queries."""

    client: PostgresClientPort
    spec: AnalyticsSpec[R, Ing]
    config: PostgresAnalyticsConfig

    # ....................... #

    def _query_config(self, query_key: str) -> PostgresQueryConfig:
        try:
            return self.config["queries"][query_key]

        except KeyError as e:
            raise exc.precondition(f"Unknown analytics query key: {query_key!r}") from e

    # ....................... #

    def _validated_params(self, query_key: str, params: BaseModel) -> BaseModel:
        return validated_params(self.spec, query_key, params)

    # ....................... #

    def _sql(self, query_key: str) -> str:
        return self._query_config(query_key)["sql"]

    # ....................... #

    def _schema(self) -> str:
        return self.config.get("schema", "public")

    # ....................... #

    def _timeout_sec(self, options: AnalyticsRunOptions | None) -> int | None:
        return timeout_seconds(options)

    # ....................... #

    def _skip_total(self, query_key: str) -> bool:
        return bool(self._query_config(query_key).get("skip_total"))

    # ....................... #

    def _max_append_rows(self) -> int:
        return int(self.config.get("max_append_rows", 10_000))

    # ....................... #

    def _cursor_column(self, query_key: str) -> str | None:
        col = self._query_config(query_key).get("cursor_column")

        return str(col) if col else None

    # ....................... #

    def _param_dict(self, params: BaseModel | JsonDict) -> dict[str, object]:
        if isinstance(params, BaseModel):
            return parameters_from_model(params)

        return dict(params)

    # ....................... #

    async def _run_with_timeout(
        self,
        options: AnalyticsRunOptions | None,
        fn: Callable[[], Awaitable[Any]],
    ) -> Any:
        timeout_sec = self._timeout_sec(options)

        if timeout_sec is None:
            return await fn()

        async with self.client.transaction():
            await self.client.execute(
                sql.SQL("SET LOCAL statement_timeout = {}").format(
                    sql.Literal(timeout_sec * 1000),
                ),
            )
            return await fn()

    # ....................... #

    def _effective_limit(
        self,
        limit: int | None,
        options: AnalyticsRunOptions | None,
    ) -> int | None:
        max_rows = (options or {}).get("max_rows")

        if max_rows is None:
            return limit

        cap = int(max_rows)

        if limit is None:
            return cap

        return min(limit, cap)

    # ....................... #

    async def _fetch_rows(
        self,
        query_key: str,
        params: BaseModel | JsonDict,
        *,
        options: AnalyticsRunOptions | None,
        limit: int | None,
        offset: int | None,
    ) -> list[JsonDict]:
        eff_limit = self._effective_limit(limit, options)
        query_sql = apply_limit_offset(
            self._sql(query_key),
            limit=eff_limit,
            offset=offset,
        )
        bound = self._param_dict(params)

        async def _run() -> list[JsonDict]:
            return await self.client.fetch_all(cast(QueryNoTemplate, query_sql), bound)

        return await self._run_with_timeout(options, _run)

    # ....................... #

    async def _total_count(
        self,
        query_key: str,
        params: BaseModel,
        *,
        options: AnalyticsRunOptions | None,
    ) -> int:
        count_sql = build_count_sql(self._sql(query_key))
        bound = self._param_dict(params)

        async def _run() -> int:
            rows = await self.client.fetch_all(cast(QueryNoTemplate, count_sql), bound)
            return parse_count_row(rows)

        return await self._run_with_timeout(options, _run)

    # ....................... #

    async def _offset_page(
        self,
        query_key: str,
        params: BaseModel,
        pagination: PaginationExpression | None,
        *,
        options: AnalyticsRunOptions | None,
        return_count: bool,
        return_type: type[BaseModel] | None,
        return_fields: Sequence[str] | None,
    ) -> CountlessPage[Any] | Page[Any]:
        params = self._validated_params(query_key, params)

        if dry_run_enabled(options):
            return dry_run_offset_page(pagination, return_count=return_count)

        limit, offset = pagination_window(pagination)
        rows = await self._fetch_rows(
            query_key,
            params,
            options=options,
            limit=limit,
            offset=offset,
        )
        data = shape_rows(
            rows,
            read_type=self.spec.read,
            return_type=return_type,
            return_fields=return_fields,
        )

        if return_count:
            if self._skip_total(query_key):
                return page_from_limit_offset(data, pagination, total=None)

            total = await self._total_count(query_key, params, options=options)
            return page_from_limit_offset(data, pagination, total=total)

        return page_from_limit_offset(data, pagination, total=None)
