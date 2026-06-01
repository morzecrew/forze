"""BigQuery implementation of analytics query and ingest ports."""

from datetime import timedelta
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
from forze.application.integrations.analytics.adapter_common import (
    dry_run_enabled,
    dry_run_offset_page,
    pagination_window,
    parse_count_row,
    shape_rows,
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
from forze.base.codecs import B64UrlJsonCodec
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.serialization import PydanticRecordMappingCodec
from forze_bigquery.execution.deps.configs import (
    BigQueryAnalyticsConfig,
    BigQueryQueryConfig,
)
from forze_bigquery.kernel.client import (
    BigQueryClientPort,
    build_count_sql,
)
from forze_bigquery.kernel.client.value_objects import BigQueryQueryResult
from forze_bigquery.kernel.relation import resolve_bigquery_ingest_target

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
Ing = TypeVar("Ing", bound=BaseModel)
T = TypeVar("T", bound=BaseModel)

_CURSOR_CODEC = B64UrlJsonCodec()

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class BigQueryAnalyticsAdapter[R: BaseModel, Ing: BaseModel](
    AnalyticsQueryPort[R],
    AnalyticsIngestPort[Ing],
):
    """Analytics ports backed by BigQuery via :class:`~forze_bigquery.kernel.client.BigQueryClient`."""

    client: BigQueryClientPort
    spec: AnalyticsSpec[R, Ing]
    config: BigQueryAnalyticsConfig
    tenant_provider: TenantProviderPort | None = None
    """Tenant context for dynamic ingest :class:`~forze_bigquery.kernel.relation.RelationSpec` resolvers."""

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
                f"BigQuery ingest relation is required for route {self.spec.name!r}."
            )

        if self._ingest_target_resolved is not None:
            return self._ingest_target_resolved

        resolved = await resolve_bigquery_ingest_target(
            spec,
            self._tenant_id_for_resolve(),
        )
        object.__setattr__(self, "_ingest_target_resolved", resolved)

        return resolved

    # ....................... #

    def _query_config(self, query_key: str) -> BigQueryQueryConfig:
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

    def _max_bytes(
        self,
        query_key: str,
        options: AnalyticsRunOptions | None,
    ) -> int | None:
        qc = self._query_config(query_key)

        return qc.maximum_bytes_billed

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

    def _cursor_page_token(
        self,
        cursor: CursorPaginationExpression | None,
    ) -> tuple[str | None, int]:
        c = dict(cursor or {})
        lim_raw = c.get("limit")
        lim = int(cast(Any, lim_raw)) if lim_raw is not None else 10
        if lim < 1:
            raise exc.internal("Cursor pagination 'limit' must be positive")

        if c.get("after") and c.get("before"):
            raise exc.internal(
                "Cursor pagination: pass at most one of 'after' or 'before'"
            )

        if c.get("after"):
            try:
                payload = _CURSOR_CODEC.loads(str(c["after"]))

                if not isinstance(payload, dict):
                    raise exc.internal("Invalid analytics cursor token")

                return str(payload["pt"]), lim  # type: ignore[return-value]

            except (ValueError, KeyError, TypeError) as e:
                raise exc.internal("Invalid analytics cursor token") from e

        if c.get("before"):
            raise exc.internal(
                "Backward analytics cursors are not supported on BigQuery."
            )

        return None, lim

    # ....................... #

    def _cursor_tokens(self, page_token: str | None) -> tuple[str | None, str | None]:
        if page_token:
            return _CURSOR_CODEC.dumps({"pt": page_token}), None

        return None, None

    # ....................... #

    async def _fetch_rows(
        self,
        query_key: str,
        params: BaseModel,
        *,
        options: AnalyticsRunOptions | None,
        max_results: int | None,
        start_index: int | None,
        page_token: str | None,
    ) -> BigQueryQueryResult:
        max_rows = (options or {}).get("max_rows")
        effective_max = max_results

        if max_rows is not None:
            if effective_max is None:
                effective_max = int(max_rows)

            else:
                effective_max = min(effective_max, int(max_rows))

        return await self.client.run_query(
            self._sql(query_key),
            params,
            dry_run=dry_run_enabled(options),
            maximum_bytes_billed=self._max_bytes(query_key, options),
            max_results=effective_max,
            start_index=start_index,
            page_token=page_token,
            timeout=self._run_timeout(options),
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
            dry_run=False,
            maximum_bytes_billed=self._max_bytes(query_key, options),
            max_results=1,
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

        max_results, start_index = pagination_window(pagination)
        result = await self._fetch_rows(
            query_key,
            params,
            options=options,
            max_results=max_results,
            start_index=start_index,
            page_token=None,
        )
        data = shape_rows(
            result.rows,
            read_codec=self.spec.resolved_read_codec,
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
            maximum_bytes_billed=self._max_bytes(query_key, options),
            max_rows=max_rows,
            timeout=self._run_timeout(options),
            fetch_batch_size=fetch_batch_size,
        )

        typed = self.spec.resolved_read_codec.decode_mapping_many(rows)

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
            maximum_bytes_billed=self._max_bytes(query_key, options),
            max_rows=max_rows,
            timeout=self._run_timeout(options),
            fetch_batch_size=fetch_batch_size,
        )
        typed = PydanticRecordMappingCodec(return_type).decode_mapping_many(rows)
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

        page_token, lim = self._cursor_page_token(cursor)
        result = await self._fetch_rows(
            query_key,
            params,
            options=options,
            max_results=lim,
            start_index=None,
            page_token=page_token,
        )
        hits = shape_rows(
            result.rows,
            read_codec=self.spec.resolved_read_codec,
            read_type=self.spec.read,
            return_type=return_type,
            return_fields=return_fields,
        )
        next_c, prev_c = self._cursor_tokens(result.page_token)
        has_more = result.page_token is not None

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
                f"BigQuery ingest relation is required for route {self.spec.name!r}."
            )

        if not rows:
            return AnalyticsAppendResult(accepted=0)

        max_append = self._max_append_rows()

        if len(rows) > max_append:
            raise exc.internal(
                f"Analytics append batch exceeds max_append_rows ({max_append})."
            )

        ingest_codec = self.spec.ingest_codec
        if ingest_codec is None:
            raise exc.internal(
                f"Analytics ingest codec is not configured for route {self.spec.name!r}."
            )

        payloads: list[JsonDict] = []

        for row in rows:
            if isinstance(row, ingest_codec.model_type):
                payloads.append(ingest_codec.encode_mapping(row))

            elif isinstance(
                row, BaseModel
            ):  # pyright: ignore[reportUnnecessaryIsInstance]
                payloads.append(
                    ingest_codec.encode_mapping(
                        ingest_codec.decode_mapping(row.model_dump()),
                    )
                )

            else:
                raise exc.internal(
                    "Analytics ingest rows must be Pydantic model instances."
                )

        dataset, table = await self._resolved_ingest_target()

        insert_result = await self.client.insert_rows(
            dataset,
            table,
            payloads,
            insert_id_field=self.config.insert_id_field,
        )

        return AnalyticsAppendResult(
            accepted=insert_result.accepted,
            rejected=insert_result.rejected,
            errors=insert_result.errors,
        )
