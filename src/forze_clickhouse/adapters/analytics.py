"""ClickHouse implementation of analytics query and ingest ports."""

from typing import Any, AsyncGenerator, Sequence, TypeVar, cast, final
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsAppendResult,
    AnalyticsIngestPort,
    AnalyticsQueryPort,
    AnalyticsRunOptions,
    AnalyticsSpec,
)
from forze.application.contracts.analytics._adapter_common import (
    dry_run_enabled,
    dry_run_offset_page,
    encode_keyset_cursor_next,
    encode_offset_cursor_next_prev,
    merge_forze_after_params,
    pagination_window,
    parse_count_row,
    parse_keyset_cursor_after,
    parse_offset_cursor_after,
    shape_rows,
    timeout_seconds,
    validated_params,
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
from forze.application.contracts.tenancy import TenantProviderPort
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.serialization import (
    pydantic_dump,
    pydantic_validate,
    pydantic_validate_many,
)
from forze_clickhouse.execution.deps.configs import (
    ClickHouseAnalyticsConfig,
    ClickHouseQueryConfig,
)
from forze_clickhouse.kernel.client import (
    ClickHouseClientPort,
    build_count_sql,
)
from forze_clickhouse.kernel.client.query import parameters_from_model
from forze_clickhouse.kernel.client.value_objects import ClickHouseQueryResult
from forze_clickhouse.kernel.relation import resolve_clickhouse_ingest_target

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
Ing = TypeVar("Ing", bound=BaseModel)
T = TypeVar("T", bound=BaseModel)

_CH_BACKWARD_CURSOR = "Backward analytics cursors are not supported on ClickHouse."

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ClickHouseAnalyticsAdapter[R: BaseModel, Ing: BaseModel](
    AnalyticsQueryPort[R],
    AnalyticsIngestPort[Ing],
):
    """Analytics ports backed by ClickHouse via :class:`~forze_clickhouse.kernel.client.ClickHouseClient`."""

    client: ClickHouseClientPort
    spec: AnalyticsSpec[R, Ing]
    config: ClickHouseAnalyticsConfig
    tenant_provider: TenantProviderPort | None = None
    """Tenant context for dynamic ingest :class:`~forze_clickhouse.kernel.relation.RelationSpec` resolvers."""

    _ingest_target_resolved: tuple[str, str] | None = attrs.field(
        default=None,
        init=False,
        eq=False,
        repr=False,
    )

    # ....................... #

    def _tenant_id_for_resolve(self) -> UUID | None:
        if self.tenant_provider is None:
            return None

        tenant = self.tenant_provider()

        return tenant.tenant_id if tenant is not None else None

    # ....................... #

    async def _resolved_ingest_target(self) -> tuple[str, str]:
        spec = self.config.resolved_ingest_relation()

        if spec is None:
            raise exc.internal(
                f"ClickHouse ingest relation is required for route {self.spec.name!r}."
            )

        if self._ingest_target_resolved is not None:
            return self._ingest_target_resolved

        resolved = await resolve_clickhouse_ingest_target(
            spec,
            self._tenant_id_for_resolve(),
        )
        object.__setattr__(self, "_ingest_target_resolved", resolved)

        return resolved

    # ....................... #

    def _query_config(self, query_key: str) -> ClickHouseQueryConfig:
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

    def _database(self) -> str:
        return self.config.database

    # ....................... #

    def _timeout_sec(self, options: AnalyticsRunOptions | None) -> int | None:
        return timeout_seconds(options)

    # ....................... #

    def _skip_total(self, query_key: str) -> bool:
        return self._query_config(query_key).skip_total

    # ....................... #

    def _max_append_rows(self) -> int:
        return self.config.max_append_rows

    # ....................... #

    def _cursor_column(self, query_key: str) -> str | None:
        col = self._query_config(query_key).cursor_column

        return str(col) if col else None

    # ....................... #

    async def _fetch_rows(
        self,
        query_key: str,
        params: BaseModel | JsonDict,
        *,
        options: AnalyticsRunOptions | None,
        limit: int | None,
        offset: int | None,
    ) -> ClickHouseQueryResult:
        max_rows = (options or {}).get("max_rows")

        return await self.client.run_query(
            self._sql(query_key),
            params,
            database=self._database(),
            limit=limit,
            offset=offset,
            timeout=self._timeout_sec(options),
            max_rows=int(max_rows) if max_rows is not None else None,
        )

    # ....................... #

    async def _total_count(
        self,
        query_key: str,
        params: BaseModel,
        *,
        options: AnalyticsRunOptions | None,
    ) -> int:
        count_sql = build_count_sql(self._sql(query_key))
        result = await self.client.run_query(
            count_sql,
            params,
            database=self._database(),
            limit=1,
            timeout=self._timeout_sec(options),
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

        limit, offset = pagination_window(pagination)
        result = await self._fetch_rows(
            query_key,
            params,
            options=options,
            limit=limit,
            offset=offset,
        )
        data = shape_rows(
            result.rows,
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

    # ....................... #

    async def run_page(
        self,
        query_key: str,
        params: BaseModel,
        pagination: PaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
    ) -> Page[R]:
        return cast(
            Page[R],
            await self._offset_page(
                query_key,
                params,
                pagination,
                options=options,
                return_count=True,
                return_type=None,
                return_fields=None,
            ),
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
        params = self._validated_params(query_key, params)

        if dry_run_enabled(options):
            return

        max_rows = (options or {}).get("max_rows")
        rows = await self.client.run_query_all_pages(
            self._sql(query_key),
            params,
            database=self._database(),
            max_rows=int(max_rows) if max_rows is not None else None,
            timeout=self._timeout_sec(options),
            fetch_batch_size=fetch_batch_size,
        )

        typed = pydantic_validate_many(self.spec.read, rows)

        for offset in range(0, len(typed), fetch_batch_size):
            yield typed[offset : offset + fetch_batch_size]

    # ....................... #

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

    # ....................... #

    async def project_run_page(
        self,
        fields: Sequence[str],
        query_key: str,
        params: BaseModel,
        pagination: PaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
    ) -> Page[JsonDict]:
        return cast(
            Page[JsonDict],
            await self._offset_page(
                query_key,
                params,
                pagination,
                options=options,
                return_count=True,
                return_type=None,
                return_fields=fields,
            ),
        )

    # ....................... #

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
        async for chunk in self.run_chunked(
            query_key,
            params,
            pagination,
            options=options,
            fetch_batch_size=fetch_batch_size,
        ):
            yield [{k: row.model_dump().get(k) for k in fields} for row in chunk]

    # ....................... #

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

    # ....................... #

    async def select_run_page(
        self,
        return_type: type[T],
        query_key: str,
        params: BaseModel,
        pagination: PaginationExpression | None = None,
        *,
        options: AnalyticsRunOptions | None = None,
    ) -> Page[T]:
        return cast(
            Page[T],
            await self._offset_page(
                query_key,
                params,
                pagination,
                options=options,
                return_count=True,
                return_type=return_type,
                return_fields=None,
            ),
        )

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
        params = self._validated_params(query_key, params)

        if dry_run_enabled(options):
            return

        max_rows = (options or {}).get("max_rows")
        rows = await self.client.run_query_all_pages(
            self._sql(query_key),
            params,
            database=self._database(),
            max_rows=int(max_rows) if max_rows is not None else None,
            timeout=self._timeout_sec(options),
            fetch_batch_size=fetch_batch_size,
        )
        typed = pydantic_validate_many(return_type, rows)

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

        cursor_col = self._cursor_column(query_key)

        if cursor_col:
            after_value, lim = parse_keyset_cursor_after(
                cursor,
                backward_not_supported=_CH_BACKWARD_CURSOR,
            )
            bound: BaseModel | JsonDict = (
                params
                if after_value is None
                else merge_forze_after_params(
                    parameters_from_model(params),
                    after_value,
                )
            )
            result = await self._fetch_rows(
                query_key,
                bound,
                options=options,
                limit=lim,
                offset=None,
            )
            hits = shape_rows(
                result.rows,
                read_type=self.spec.read,
                return_type=return_type,
                return_fields=return_fields,
            )
            next_c = encode_keyset_cursor_next(
                column=cursor_col,
                hits=hits,
                limit=lim,
            )
            prev_c = None
        else:
            start, lim = parse_offset_cursor_after(
                cursor,
                backward_not_supported=_CH_BACKWARD_CURSOR,
            )
            result = await self._fetch_rows(
                query_key,
                params,
                options=options,
                limit=lim,
                offset=start,
            )
            hits = shape_rows(
                result.rows,
                read_type=self.spec.read,
                return_type=return_type,
                return_fields=return_fields,
            )
            next_c, prev_c = encode_offset_cursor_next_prev(
                start=start,
                page_len=len(hits),
                limit=lim,
            )

        has_more = next_c is not None

        return CursorPage(
            hits=hits,
            next_cursor=next_c,
            prev_cursor=prev_c,
            has_more=has_more,
        )

    # ....................... #

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

    # ....................... #

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

    # ....................... #

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
                f"Analytics ingest is not configured for route {self.spec.name!r}."
            )

        if self.config.resolved_ingest_relation() is None:
            raise exc.internal(
                f"ClickHouse ingest relation is required for route {self.spec.name!r}."
            )

        if not rows:
            return AnalyticsAppendResult(accepted=0)

        max_append = self._max_append_rows()

        if len(rows) > max_append:
            raise exc.internal(
                f"Analytics append batch exceeds max_append_rows ({max_append})."
            )

        ingest_type = self.spec.ingest
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

        database, table = await self._resolved_ingest_target()

        insert_result = await self.client.insert_rows(
            database,
            table,
            payloads,
            timeout=self._timeout_sec(None),
        )

        return AnalyticsAppendResult(
            accepted=insert_result.accepted,
            rejected=insert_result.rejected,
            errors=insert_result.errors,
        )
