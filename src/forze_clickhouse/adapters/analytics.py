"""ClickHouse implementation of analytics query and ingest ports."""

from collections.abc import AsyncIterator, Sequence
from typing import Any, TypeVar, cast, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsAppendResult,
    AnalyticsIngestPort,
    AnalyticsQueryDefinition,
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
from forze.base.codecs import B64UrlJsonCodec
from forze.base.errors import CoreError
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
from forze_clickhouse.kernel.platform import (
    ClickHouseClientPort,
    build_count_sql,
)
from forze_clickhouse.kernel.platform.value_objects import ClickHouseQueryResult

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
Ing = TypeVar("Ing", bound=BaseModel)
T = TypeVar("T", bound=BaseModel)

_CURSOR_CODEC = B64UrlJsonCodec()

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ClickHouseAnalyticsAdapter[R: BaseModel, Ing: BaseModel](
    AnalyticsQueryPort[R],
    AnalyticsIngestPort[Ing],
):
    """Analytics ports backed by ClickHouse via :class:`~forze_clickhouse.kernel.platform.ClickHouseClient`."""

    client: ClickHouseClientPort
    spec: AnalyticsSpec[R, Ing]
    config: ClickHouseAnalyticsConfig

    # ....................... #

    def _query_config(self, query_key: str) -> ClickHouseQueryConfig:
        try:
            return self.config["queries"][query_key]

        except KeyError as exc:
            raise CoreError(f"Unknown analytics query key: {query_key!r}") from exc

    # ....................... #

    def _definition(self, query_key: str) -> AnalyticsQueryDefinition:
        try:
            return self.spec.queries[query_key]

        except KeyError as exc:
            raise CoreError(f"Unknown analytics query key: {query_key!r}") from exc

    # ....................... #

    def _validated_params(self, query_key: str, params: BaseModel) -> BaseModel:
        defn = self._definition(query_key)

        if isinstance(params, defn.params):
            return params

        if isinstance(
            params, BaseModel
        ):  # pyright: ignore[reportUnnecessaryIsInstance]
            return pydantic_validate(defn.params, params.model_dump())

        raise CoreError("Analytics params must be a Pydantic model instance.")

    # ....................... #

    def _sql(self, query_key: str) -> str:
        return self._query_config(query_key)["sql"]

    # ....................... #

    def _database(self) -> str:
        return self.config["database"]

    # ....................... #

    def _timeout_sec(self, options: AnalyticsRunOptions | None) -> int | None:
        if options is None:
            return None

        timeout = options.get("timeout")

        if timeout is None:
            return None

        return max(1, int(timeout.total_seconds()))

    # ....................... #

    def _dry_run(self, options: AnalyticsRunOptions | None) -> bool:
        return bool((options or {}).get("dry_run"))

    # ....................... #

    def _pagination_window(
        self,
        pagination: PaginationExpression | None,
    ) -> tuple[int | None, int | None]:
        p = dict(pagination or {})
        limit = p.get("limit")
        offset = p.get("offset")
        max_results = int(cast(Any, limit)) if limit is not None else None
        start_index = int(cast(Any, offset)) if offset is not None else None

        return max_results, start_index

    # ....................... #

    def _cursor_start_and_limit(
        self,
        cursor: CursorPaginationExpression | None,
    ) -> tuple[int, int]:
        c = dict(cursor or {})
        lim_raw = c.get("limit")
        lim = int(cast(Any, lim_raw)) if lim_raw is not None else 10

        if lim < 1:
            raise CoreError("Cursor pagination 'limit' must be positive")

        if c.get("after") and c.get("before"):
            raise CoreError(
                "Cursor pagination: pass at most one of 'after' or 'before'"
            )

        if c.get("after"):
            try:
                payload = _CURSOR_CODEC.loads(str(c["after"]))

                if not isinstance(payload, dict):
                    raise CoreError("Invalid analytics cursor token")

                return int(payload["o"]), lim  # type: ignore[arg-type]

            except (ValueError, KeyError, TypeError) as e:
                raise CoreError("Invalid analytics cursor token") from e

        if c.get("before"):
            raise CoreError(
                "Backward analytics cursors are not supported on ClickHouse."
            )

        return 0, lim

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

    async def _fetch_rows(
        self,
        query_key: str,
        params: BaseModel,
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

        if not result.rows:
            return 0

        raw = result.rows[0].get("forze_cnt", 0)

        return int(raw)

    # ....................... #

    def _shape_rows(
        self,
        rows: list[JsonDict],
        *,
        return_type: type[T] | None,
        return_fields: Sequence[str] | None,
    ) -> list[Any]:
        if return_fields is not None:
            return [{k: row.get(k) for k in return_fields} for row in rows]

        if return_type is not None:
            return pydantic_validate_many(return_type, rows)

        return pydantic_validate_many(self.spec.read, rows)

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

        if self._dry_run(options):
            empty: list[Any] = []

            if return_count:
                return page_from_limit_offset(empty, pagination, total=0)

            return page_from_limit_offset(empty, pagination, total=None)

        limit, offset = self._pagination_window(pagination)
        result = await self._fetch_rows(
            query_key,
            params,
            options=options,
            limit=limit,
            offset=offset,
        )
        data = self._shape_rows(
            result.rows,
            return_type=return_type,
            return_fields=return_fields,
        )

        if return_count:
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
    ) -> AsyncIterator[Sequence[R]]:
        params = self._validated_params(query_key, params)

        if self._dry_run(options):
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
    ) -> AsyncIterator[Sequence[JsonDict]]:
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
    ) -> AsyncIterator[Sequence[T]]:
        params = self._validated_params(query_key, params)

        if self._dry_run(options):
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

        if self._dry_run(options):
            return CursorPage(
                hits=[], next_cursor=None, prev_cursor=None, has_more=False
            )

        start, lim = self._cursor_start_and_limit(cursor)
        result = await self._fetch_rows(
            query_key,
            params,
            options=options,
            limit=lim,
            offset=start,
        )
        hits = self._shape_rows(
            result.rows,
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
            raise CoreError(
                f"Analytics ingest is not configured for route {self.spec.name!r}."
            )

        table = self.config.get("ingest_table")

        if not table:
            raise CoreError(
                f"ClickHouse ingest_table is required for route {self.spec.name!r}."
            )

        if not rows:
            return AnalyticsAppendResult(accepted=0)

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
                raise CoreError(
                    "Analytics ingest rows must be Pydantic model instances."
                )

        accepted = await self.client.insert_rows(
            self._database(),
            table,
            payloads,
            timeout=self._timeout_sec(None),
        )

        return AnalyticsAppendResult(accepted=accepted)
