from forze_clickhouse._compat import require_clickhouse

require_clickhouse()

# ....................... #

from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import Any, AsyncIterator, final

import attrs
import clickhouse_connect  # pyright: ignore[reportMissingTypeStubs]
from pydantic import BaseModel

from forze.base.errors import CoreError
from forze.base.primitives import JsonDict

from .errors import clickhouse_handled
from .port import ClickHouseClientPort
from .query import apply_limit_offset, parameters_from_model
from .value_objects import DEFAULT_TIMEOUT, ClickHouseConfig, ClickHouseQueryResult

# ----------------------- #


@final
@attrs.define(slots=True)
class ClickHouseClient(ClickHouseClientPort):
    """Async ClickHouse client backed by :mod:`clickhouse_connect`."""

    __client: Any = attrs.field(default=None, init=False)
    __config: ClickHouseConfig | None = attrs.field(default=None, init=False)

    __ctx_depth: ContextVar[int] = attrs.field(
        factory=lambda: ContextVar("clickhouse_depth", default=0),
        init=False,
    )

    # ....................... #

    async def initialize(self, config: ClickHouseConfig) -> None:
        """Open an async client from *config*."""

        if self.__client is not None:
            return

        self.__config = config
        self.__client = await clickhouse_connect.get_async_client(  # type: ignore[reportUnknownReturnType]
            host=config.host,
            port=config.port,
            username=config.username,
            password=config.password,
            database=config.database,
            secure=config.secure,
            connect_timeout=int(config.timeout.total_seconds()),
            send_receive_timeout=int(config.timeout.total_seconds()),
        )

    # ....................... #

    async def close(self) -> None:
        client = self.__client

        if client is not None:
            await client.close()
            self.__client = None

        self.__config = None

    # ....................... #

    def __require_client(self) -> Any:
        if self.__client is None:
            raise CoreError("ClickHouse client is not initialized")

        return self.__client

    # ....................... #

    def __timeout_sec(self, override: int | None) -> int:
        if override is not None:
            return override

        if self.__config is not None:
            return max(1, int(self.__config.timeout.total_seconds()))

        return max(1, int(DEFAULT_TIMEOUT.total_seconds()))

    # ....................... #

    def __database(self, override: str | None) -> str:
        if override is not None:
            return override

        if self.__config is not None:
            return self.__config.database

        return "default"

    # ....................... #

    @asynccontextmanager
    async def client(self) -> AsyncIterator[Any]:
        depth = self.__ctx_depth.get()

        if depth > 0:
            self.__ctx_depth.set(depth + 1)

            try:
                yield self.__require_client()

            finally:
                self.__ctx_depth.set(depth)

            return

        token = self.__ctx_depth.set(1)

        try:
            yield self.__require_client()

        finally:
            self.__ctx_depth.reset(token)

    # ....................... #

    def __rows_from_result(self, result: Any) -> list[JsonDict]:
        named = result.named_results()

        return [dict(row) for row in named]

    # ....................... #

    @clickhouse_handled("clickhouse.run_query")  # type: ignore[untyped-decorator]
    async def run_query(
        self,
        sql: str,
        params: BaseModel | None = None,
        *,
        database: str | None = None,
        max_rows: int | None = None,
        limit: int | None = None,
        offset: int | None = None,
        timeout: int | None = None,
    ) -> ClickHouseQueryResult:
        effective_limit = limit

        if max_rows is not None:
            effective_limit = (
                min(effective_limit, max_rows)
                if effective_limit is not None
                else max_rows
            )

        query_sql = apply_limit_offset(sql, limit=effective_limit, offset=offset)
        parameters = parameters_from_model(params) if params is not None else None
        timeout_sec = self.__timeout_sec(timeout)
        ch = self.__require_client()
        target_db = self.__database(database)
        prev_db = ch.database

        try:
            ch.database = target_db
            result = await ch.query(
                query_sql,
                parameters=parameters,
                settings={"max_execution_time": timeout_sec},
            )

        finally:
            ch.database = prev_db

        rows = self.__rows_from_result(result)

        return ClickHouseQueryResult(rows=rows, row_count=len(rows))

    # ....................... #

    @clickhouse_handled("clickhouse.run_query_all_pages")  # type: ignore[untyped-decorator]
    async def run_query_all_pages(
        self,
        sql: str,
        params: BaseModel | None = None,
        *,
        database: str | None = None,
        max_rows: int | None = None,
        timeout: int | None = None,
        fetch_batch_size: int = 2000,
    ) -> list[JsonDict]:
        if fetch_batch_size < 1:
            raise CoreError("fetch_batch_size must be >= 1")

        all_rows: list[JsonDict] = []
        offset = 0

        while True:
            batch_limit = fetch_batch_size

            if max_rows is not None:
                remaining = max_rows - len(all_rows)

                if remaining <= 0:
                    break

                batch_limit = min(batch_limit, remaining)

            result = await self.run_query(
                sql,
                params,
                database=database,
                limit=batch_limit,
                offset=offset,
                timeout=timeout,
            )
            all_rows.extend(result.rows)

            if result.row_count < batch_limit:
                break

            offset += batch_limit

        return all_rows

    # ....................... #

    @clickhouse_handled("clickhouse.insert_rows")  # type: ignore[untyped-decorator]
    async def insert_rows(
        self,
        database: str,
        table: str,
        rows: list[JsonDict],
        *,
        timeout: int | None = None,
    ) -> int:
        if not rows:
            return 0

        columns = list(rows[0].keys())
        data = [[row.get(col) for col in columns] for row in rows]
        timeout_sec = self.__timeout_sec(timeout)

        await self.__require_client().insert(
            table,
            data,
            column_names=columns,
            database=database,
            settings={"max_execution_time": timeout_sec},
        )

        return len(rows)
