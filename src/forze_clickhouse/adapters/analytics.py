"""ClickHouse implementation of analytics query and ingest ports."""

from collections.abc import AsyncGenerator, Sequence
from datetime import timedelta
from typing import Any, TypeVar, final
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
from forze.application.contracts.base import (
    CountlessPage,
    CursorPage,
    Page,
)
from forze.application.contracts.querying import (
    CursorPaginationExpression,
    PaginationExpression,
)
from forze.application.contracts.resolution import (
    is_static_relation,
    resolve_scoped_namespace,
)
from forze.application.contracts.tenancy import TenantProviderPort, soft_tenant_id
from forze.application.integrations.analytics import (
    AnalyticsQueryPortMixin,
    decrypt_and_shape_rows,
    encode_ingest_payloads,
    stream_shaped_chunks,
)
from forze.application.integrations.analytics.adapter_common import (
    bind_tenant_param,
    dry_run_enabled,
    dry_run_offset_page,
    encode_keyset_cursor_next,
    encode_offset_cursor_next_prev,
    execute_analytics_offset_page,
    merge_forze_after_params,
    parse_count_row,
    parse_keyset_cursor_after,
    parse_offset_cursor_after,
    validate_fetch_batch_size,
    validated_params,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, OnceCell
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
    AnalyticsQueryPortMixin[R],
    AnalyticsQueryPort[R],
    AnalyticsIngestPort[Ing],
):
    """Analytics ports backed by ClickHouse via :class:`~forze_clickhouse.kernel.client.ClickHouseClient`."""

    client: ClickHouseClientPort
    spec: AnalyticsSpec[R, Ing]
    config: ClickHouseAnalyticsConfig
    tenant_provider: TenantProviderPort | None = None
    """Tenant context for dynamic ingest :class:`~forze_clickhouse.kernel.relation.RelationSpec` resolvers."""

    _ingest_target_cell: OnceCell[tuple[str, str]] = attrs.field(
        factory=OnceCell,
        init=False,
        eq=False,
        repr=False,
    )

    _query_database_cell: OnceCell[str] = attrs.field(
        factory=OnceCell,
        init=False,
        eq=False,
        repr=False,
    )

    # ....................... #

    def _tenant_id_for_resolve(self) -> UUID | None:
        return soft_tenant_id(self.tenant_provider)

    # ....................... #

    def _bind_tenant(self, params: BaseModel | JsonDict) -> BaseModel | JsonDict:
        """Bind the current tenant as ``{tenant:UUID}`` on a tenant-aware route (fail-closed)."""

        return bind_tenant_param(
            params,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=self.tenant_provider,
        )

    # ....................... #

    async def _resolved_ingest_target(self) -> tuple[str, str]:
        spec = self.config.resolved_ingest_relation()

        if spec is None:
            raise exc.internal(
                f"ClickHouse ingest relation is required for route {self.spec.name!r}."
            )

        async def _factory() -> tuple[str, str]:
            return await resolve_clickhouse_ingest_target(
                spec,
                self._tenant_id_for_resolve(),
            )

        # Only memoize tenant-independent (static) relations; a dynamic resolver
        # depends on the bound tenant and the adapter may be shared across tenants.
        return await self._ingest_target_cell.resolve(
            _factory,
            cache=is_static_relation(spec),
        )

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

    async def _database(self) -> str:
        spec = self.config.query_database

        if spec is None:
            return self.config.database

        return await resolve_scoped_namespace(
            spec,
            tenant_id=self._tenant_id_for_resolve(),
            cell=self._query_database_cell,
        )

    # ....................... #

    @staticmethod
    def _run_timeout(options: AnalyticsRunOptions | None) -> timedelta | None:
        if options is None:
            return None

        return options.get("timeout")

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
            self._bind_tenant(params),
            database=await self._database(),
            limit=limit,
            offset=offset,
            timeout=self._run_timeout(options),
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
            self._bind_tenant(params),
            database=await self._database(),
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
            result = await self._fetch_rows(
                query_key,
                params,
                options=options,
                limit=limit,
                offset=offset,
            )

            return result.rows

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
        validate_fetch_batch_size(fetch_batch_size)
        params = self._validated_params(query_key, params)

        if dry_run_enabled(options):
            return

        max_rows = (options or {}).get("max_rows")
        raw = self.client.run_query_streamed(
            self._sql(query_key),
            self._bind_tenant(params),
            database=await self._database(),
            max_rows=int(max_rows) if max_rows is not None else None,
            timeout=self._run_timeout(options),
            fetch_batch_size=fetch_batch_size,
        )

        async for chunk in stream_shaped_chunks(
            raw,
            fetch_batch_size=fetch_batch_size,
            read_codec=self.spec.resolved_read_codec,
            read_type=self.spec.read,
            return_type=None,
            return_fields=None,
        ):
            yield chunk

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
        validate_fetch_batch_size(fetch_batch_size)
        params = self._validated_params(query_key, params)

        if dry_run_enabled(options):
            return

        max_rows = (options or {}).get("max_rows")
        raw = self.client.run_query_streamed(
            self._sql(query_key),
            self._bind_tenant(params),
            database=await self._database(),
            max_rows=int(max_rows) if max_rows is not None else None,
            timeout=self._run_timeout(options),
            fetch_batch_size=fetch_batch_size,
        )

        async for chunk in stream_shaped_chunks(
            raw,
            fetch_batch_size=fetch_batch_size,
            read_codec=self.spec.resolved_read_codec,
            read_type=self.spec.read,
            return_type=return_type,
            return_fields=None,
        ):
            yield chunk

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
            return CursorPage(hits=[], next_cursor=None, prev_cursor=None, has_more=False)

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
            hits = await decrypt_and_shape_rows(
                result.rows,
                read_codec=self.spec.resolved_read_codec,
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
            hits = await decrypt_and_shape_rows(
                result.rows,
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

        has_more = next_c is not None

        return CursorPage(
            hits=hits,
            next_cursor=next_c,
            prev_cursor=prev_c,
            has_more=has_more,
        )

    # ....................... #

    async def append(self, rows: Sequence[Ing]) -> AnalyticsAppendResult | None:
        if self.spec.ingest is None:
            raise exc.internal(f"Analytics ingest is not configured for route {self.spec.name!r}.")

        if self.config.resolved_ingest_relation() is None:
            raise exc.internal(
                f"ClickHouse ingest relation is required for route {self.spec.name!r}."
            )

        if not rows:
            return AnalyticsAppendResult(accepted=0)

        max_append = self._max_append_rows()

        if len(rows) > max_append:
            raise exc.internal(f"Analytics append batch exceeds max_append_rows ({max_append}).")

        ingest_codec = self.spec.resolved_ingest_codec
        if ingest_codec is None:
            raise exc.internal(
                f"Analytics ingest codec is not configured for route {self.spec.name!r}."
            )

        payloads = await encode_ingest_payloads(ingest_codec, list(rows))

        database, table = await self._resolved_ingest_target()

        insert_result = await self.client.insert_rows(
            database,
            table,
            payloads,
            timeout=self._run_timeout(None),
        )

        return AnalyticsAppendResult(
            accepted=insert_result.accepted,
            rejected=insert_result.rejected,
            errors=insert_result.errors,
        )
