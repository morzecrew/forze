"""PostgreSQL implementation of analytics query and ingest ports."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, AsyncGenerator, Sequence, TypeVar, cast, final

import attrs
from psycopg import sql
from psycopg.abc import QueryNoTemplate
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
    pagination_window,
    parse_count_row,
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
from forze.base.codecs import B64UrlJsonCodec
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.serialization import (
    pydantic_dump,
    pydantic_validate,
    pydantic_validate_many,
)
from forze_postgres.execution.deps.configs import (
    PostgresAnalyticsConfig,
    PostgresQueryConfig,
)
from forze_postgres.kernel.client import PostgresClientPort
from forze_postgres.kernel.client.analytics_query import (
    apply_limit_offset,
    build_count_sql,
    parameters_from_model,
)

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
Ing = TypeVar("Ing", bound=BaseModel)
T = TypeVar("T", bound=BaseModel)

_CURSOR_CODEC = B64UrlJsonCodec()

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresAnalyticsAdapter[R: BaseModel, Ing: BaseModel](
    AnalyticsQueryPort[R],
    AnalyticsIngestPort[Ing],
):
    """Analytics ports backed by PostgreSQL via :class:`~forze_postgres.kernel.client.PostgresClient`."""

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

    def _cursor_start_and_limit(
        self,
        cursor: CursorPaginationExpression | None,
    ) -> tuple[int, int]:
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

                if "kc" in payload:
                    raise exc.internal(
                        "Offset cursor token passed to offset-based query."
                    )

                return int(payload["o"]), lim  # type: ignore[arg-type]

            except (ValueError, KeyError, TypeError) as e:
                raise exc.internal("Invalid analytics cursor token") from e

        if c.get("before"):
            raise exc.internal(
                "Backward analytics cursors are not supported on PostgreSQL."
            )

        return 0, lim

    # ....................... #

    def _keyset_after_value(
        self,
        cursor: CursorPaginationExpression | None,
    ) -> tuple[Any | None, int]:
        c = dict(cursor or {})
        lim_raw = c.get("limit")
        lim = int(cast(Any, lim_raw)) if lim_raw is not None else 10

        if lim < 1:
            raise exc.internal("Cursor pagination 'limit' must be positive")

        if c.get("before"):
            raise exc.internal(
                "Backward analytics cursors are not supported on PostgreSQL."
            )

        if not c.get("after"):
            return None, lim

        try:
            payload = _CURSOR_CODEC.loads(str(c["after"]))

            if not isinstance(payload, dict) or "kv" not in payload:
                raise exc.internal("Invalid analytics keyset cursor token")

            return payload["kv"], lim  # type: ignore[return-value]

        except (ValueError, KeyError, TypeError) as e:
            raise exc.internal("Invalid analytics keyset cursor token") from e

    # ....................... #

    def _cursor_tokens(
        self,
        *,
        start: int,
        page_len: int,
        limit: int,
    ) -> tuple[str | None, str | None]:
        has_more = page_len >= limit
        next_c = _CURSOR_CODEC.dumps({"o": start + page_len}) if has_more else None
        prev_c = _CURSOR_CODEC.dumps({"o": start}) if start > 0 else None

        return next_c, prev_c

    # ....................... #

    def _keyset_cursor_tokens(
        self,
        *,
        column: str,
        hits: list[Any],
        limit: int,
    ) -> tuple[str | None, str | None]:
        has_more = len(hits) >= limit
        next_c = None

        if has_more and hits:
            last = hits[-1]
            if isinstance(last, BaseModel):
                value = last.model_dump().get(column)

            elif isinstance(last, dict):
                value = last.get(column)  # type: ignore[assignment]

            else:
                value = getattr(last, column, None)

            if value is not None:
                next_c = _CURSOR_CODEC.dumps({"kc": column, "kv": value})

        return next_c, None

    # ....................... #

    def _params_with_keyset(
        self,
        params: BaseModel,
        after_value: Any | None,
    ) -> dict[str, object]:
        if after_value is None:
            return self._param_dict(params)

        return {**self._param_dict(params), "forze_after": after_value}

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
        return_type: type[T] | None,
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
        cap = int(max_rows) if max_rows is not None else None
        batch = max(1, fetch_batch_size)
        offset = 0
        collected = 0
        buffer: list[R] = []

        while True:
            if cap is not None and collected >= cap:
                break

            fetch_limit = batch

            if cap is not None:
                fetch_limit = min(batch, cap - collected)

            rows = await self._fetch_rows(
                query_key,
                params,
                options=options,
                limit=fetch_limit,
                offset=offset,
            )

            if not rows:
                break

            typed = pydantic_validate_many(self.spec.read, rows)
            collected += len(typed)
            offset += len(typed)
            buffer.extend(typed)

            while len(buffer) >= batch:
                yield buffer[:batch]
                buffer = buffer[batch:]

            if len(typed) < fetch_limit:
                break

        if buffer:
            yield buffer

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
        cap = int(max_rows) if max_rows is not None else None
        batch = max(1, fetch_batch_size)
        offset = 0
        collected = 0
        buffer: list[T] = []

        while True:
            if cap is not None and collected >= cap:
                break

            fetch_limit = batch

            if cap is not None:
                fetch_limit = min(batch, cap - collected)

            rows = await self._fetch_rows(
                query_key,
                params,
                options=options,
                limit=fetch_limit,
                offset=offset,
            )

            if not rows:
                break

            typed = pydantic_validate_many(return_type, rows)
            collected += len(typed)
            offset += len(typed)
            buffer.extend(typed)

            while len(buffer) >= batch:
                yield buffer[:batch]
                buffer = buffer[batch:]

            if len(typed) < fetch_limit:
                break

        if buffer:
            yield buffer

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
            after_value, lim = self._keyset_after_value(cursor)
            bound = self._params_with_keyset(params, after_value)
            rows = await self._fetch_rows(
                query_key,
                bound,
                options=options,
                limit=lim,
                offset=None,
            )
            hits = shape_rows(
                rows,
                read_type=self.spec.read,
                return_type=return_type,
                return_fields=return_fields,
            )
            next_c, prev_c = self._keyset_cursor_tokens(
                column=cursor_col,
                hits=hits,
                limit=lim,
            )
        else:
            start, lim = self._cursor_start_and_limit(cursor)
            rows = await self._fetch_rows(
                query_key,
                params,
                options=options,
                limit=lim,
                offset=start,
            )
            hits = shape_rows(
                rows,
                read_type=self.spec.read,
                return_type=return_type,
                return_fields=return_fields,
            )
            next_c, prev_c = self._cursor_tokens(
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

        table = self.config.get("ingest_table")

        if not table:
            raise exc.internal(
                f"Postgres ingest_table is required for route {self.spec.name!r}."
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

        keys = list(payloads[0].keys())
        col_idents = [sql.Identifier(k) for k in keys]
        row_template = (
            sql.SQL("(") + sql.SQL(", ").join(sql.Placeholder() for _ in keys) + sql.SQL(")")
        )
        value_parts = [row_template] * len(payloads)
        flat_params: list[Any] = []

        for payload in payloads:
            flat_params.extend(payload[k] for k in keys)

        stmt = sql.SQL("INSERT INTO {schema}.{table} ({cols}) VALUES {vals}").format(
            schema=sql.Identifier(self._schema()),
            table=sql.Identifier(table),
            cols=sql.SQL(", ").join(col_idents),
            vals=sql.SQL(", ").join(value_parts),
        )

        async def _run() -> None:
            await self.client.execute(stmt, flat_params)

        await self._run_with_timeout(None, _run)

        return AnalyticsAppendResult(accepted=len(rows))
