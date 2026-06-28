from forze_clickhouse._compat import require_clickhouse

require_clickhouse()

# ....................... #

import asyncio
from datetime import timedelta
from operator import itemgetter
from typing import Any, Awaitable, Callable, Sequence, TypeVar, cast, final

import attrs
from clickhouse_connect.driver import (  # type: ignore[import-untyped]
    create_async_client,  # pyright: ignore[reportUnknownVariableType]
)
from clickhouse_connect.driver.asyncclient import (  # type: ignore[import-untyped]
    AsyncClient,
)
from clickhouse_connect.driver.query import (  # type: ignore[import-untyped]
    QueryResult,
)
from pydantic import BaseModel

from forze.application.execution.resilience.read_retry import retry_read
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

from .errors import exc_interceptor
from .port import ClickHouseClientPort
from .query import apply_limit_offset, parameters_from_model
from .value_objects import (
    DEFAULT_TIMEOUT,
    ClickHouseConfig,
    ClickHouseInsertResult,
    ClickHouseQueryResult,
)

# ----------------------- #

T = TypeVar("T")

# ....................... #


@final
@attrs.define(slots=True)
class ClickHouseClient(ClickHouseClientPort):
    """Async ClickHouse client backed by :mod:`clickhouse_connect`."""

    __client: AsyncClient | None = attrs.field(default=None, init=False)
    __config: ClickHouseConfig | None = attrs.field(default=None, init=False)
    __init_lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False)

    # ....................... #

    async def initialize(self, config: ClickHouseConfig) -> None:
        """Open an async client from *config*."""

        async with self.__init_lock:
            if self.__client is not None:
                return

            self.__config = config
            timeout_sec = int(config.timeout.total_seconds())
            self.__client = await create_async_client(  # type: ignore[reportUnknownReturnType]
                host=config.host,
                port=config.port,
                username=config.username,
                password=config.password.get_secret_value(),
                database=config.database,
                secure=config.secure,
                connect_timeout=timeout_sec,
                send_receive_timeout=timeout_sec,
                connector_limit=config.connector_limit,
                connector_limit_per_host=config.connector_limit_per_host,
                keepalive_timeout=config.keepalive_timeout.total_seconds(),
            )

    # ....................... #

    async def close(self) -> None:
        async with self.__init_lock:
            client = self.__client

            if client is not None:
                await client.close()
                self.__client = None

            self.__config = None

    # ....................... #

    def __require_client(self) -> AsyncClient:
        if self.__client is None:
            raise exc.internal("ClickHouse client is not initialized")

        return self.__client

    # ....................... #

    def __require_config(self) -> ClickHouseConfig:
        if self.__config is None:
            raise exc.internal("ClickHouse client is not initialized")

        return self.__config

    # ....................... #

    def __timeout_sec(self, override: timedelta | None) -> int:
        if override is not None:
            return max(1, int(override.total_seconds()))

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

    def __query_settings(
        self,
        *,
        database: str,
        timeout_sec: int,
    ) -> dict[str, Any]:
        return {
            "database": database,
            "max_execution_time": timeout_sec,
        }

    # ....................... #

    async def __maybe_read_retry(
        self,
        fn: Callable[[], Awaitable[T]],
    ) -> T:
        cfg = self.__require_config()

        return await retry_read(
            fn,
            attempts=cfg.read_retry_attempts,
            base_delay=cfg.read_retry_base_delay.total_seconds(),
        )

    # ....................... #

    def __rows_from_result(self, result: Any) -> list[JsonDict]:
        named = result.named_results()

        return [dict(row) for row in named]

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        """Check ClickHouse connectivity with ``SELECT 1``."""

        try:
            ch = self.__require_client()
            await ch.command("SELECT 1", use_database=False)  # type: ignore[untyped-call]
            return "ok", True

        except Exception as e:
            return str(e), False

    # ....................... #

    @exc_interceptor.coroutine("clickhouse.run_query")  # type: ignore[untyped-decorator]
    async def run_query(
        self,
        sql: str,
        params: BaseModel | JsonDict | Sequence[Any] | None = None,
        *,
        database: str | None = None,
        max_rows: int | None = None,
        limit: int | None = None,
        offset: int | None = None,
        timeout: timedelta | None = None,
    ) -> ClickHouseQueryResult:
        async def _run() -> ClickHouseQueryResult:
            effective_limit = limit

            if max_rows is not None:
                effective_limit = (
                    min(effective_limit, max_rows)
                    if effective_limit is not None
                    else max_rows
                )

            query_sql = apply_limit_offset(sql, limit=effective_limit, offset=offset)

            if isinstance(params, BaseModel):
                bound_params = parameters_from_model(params)

            else:
                bound_params = params  # type: ignore[assignment]

            timeout_sec = self.__timeout_sec(timeout)
            target_db = self.__database(database)
            ch = self.__require_client()
            result = await ch.query(  # type: ignore[untyped-call]
                query_sql,
                parameters=bound_params,
                settings=self.__query_settings(
                    database=target_db,
                    timeout_sec=timeout_sec,
                ),
            )
            rows = self.__rows_from_result(result)

            return ClickHouseQueryResult(rows=rows, row_count=len(rows))

        return await self.__maybe_read_retry(_run)

    # ....................... #

    @exc_interceptor.coroutine("clickhouse.run_query_all_pages")  # type: ignore[untyped-decorator]
    async def run_query_all_pages(
        self,
        sql: str,
        params: BaseModel | JsonDict | Sequence[Any] | None = None,
        *,
        database: str | None = None,
        max_rows: int | None = None,
        timeout: timedelta | None = None,
        fetch_batch_size: int = 2000,
    ) -> list[JsonDict]:
        """Fetch every row of *sql* via a single streaming query execution.

        The query is executed exactly once and the result is consumed in
        row blocks (:meth:`AsyncClient.query_row_block_stream`), so the
        rows come from one consistent result set — no per-page rescans and
        no duplicated/missing rows between pages, even when the registered
        SQL has no deterministic ``ORDER BY``.

        :param max_rows: Hard cap on returned rows; pushed into the SQL as
            ``LIMIT`` and additionally enforced while consuming blocks.
        :param fetch_batch_size: Streaming block-size hint forwarded as the
            ClickHouse ``max_block_size`` setting (server treats it as a
            recommendation; actual block sizes may differ).
        """

        if fetch_batch_size < 1:
            raise exc.internal("fetch_batch_size must be >= 1")

        query_sql = apply_limit_offset(sql, limit=max_rows, offset=None)

        if isinstance(params, BaseModel):
            bound_params = parameters_from_model(params)

        else:
            bound_params = params  # type: ignore[assignment]

        timeout_sec = self.__timeout_sec(timeout)
        target_db = self.__database(database)
        settings = self.__query_settings(database=target_db, timeout_sec=timeout_sec)
        settings["max_block_size"] = fetch_batch_size

        # Single retry layer: the streaming execution below does not go
        # through ``run_query`` (which retries itself), so a transient
        # failure restarts at most ``read_retry_attempts`` times total.
        async def _run() -> list[JsonDict]:
            ch = self.__require_client()
            all_rows: list[JsonDict] = []

            stream = await ch.query_row_block_stream(  # type: ignore[untyped-call]
                query_sql,
                parameters=bound_params,
                settings=settings,
            )

            async with stream:
                # ``StreamContext.source`` is typed as a bare ``Closable``; for a
                # query row-block stream it is the underlying ``QueryResult``,
                # whose first block is parsed eagerly so ``column_names`` is set.
                source = cast(QueryResult, stream.source)
                column_names: tuple[str, ...] = source.column_names

                # Badly typed...
                async for block in stream:  # pyright: ignore[reportUnknownVariableType]
                    for (
                        row
                    ) in (  # pyright: ignore[reportOptionalIterable, reportUnknownVariableType]
                        block
                    ):  # pyright: ignore[reportOptionalIterable, reportUnknownVariableType]
                        all_rows.append(
                            dict(
                                zip(
                                    column_names,
                                    row,  # pyright: ignore[reportUnknownArgumentType]
                                )
                            )
                        )

                    # ``LIMIT`` already caps the result server-side; this is a
                    # defensive client-side stop that also ends consumption early.
                    if max_rows is not None and len(all_rows) >= max_rows:
                        del all_rows[max_rows:]
                        break

            return all_rows

        return await self.__maybe_read_retry(_run)

    # ....................... #

    @exc_interceptor.coroutine("clickhouse.insert_rows")  # type: ignore[untyped-decorator]
    async def insert_rows(
        self,
        database: str,
        table: str,
        rows: list[JsonDict],
        *,
        timeout: timedelta | None = None,
    ) -> ClickHouseInsertResult:
        if not rows:
            return ClickHouseInsertResult(accepted=0)

        cfg = self.__require_config()
        batch_size = max(1, cfg.insert_batch_size)
        timeout_sec = self.__timeout_sec(timeout)
        ch = self.__require_client()
        accepted_total = 0

        for start in range(0, len(rows), batch_size):
            batch = rows[start : start + batch_size]
            columns = list(batch[0].keys())

            # Fast path: rows from ``encode_ingest_payloads`` share one key set, so a
            # single C-level ``itemgetter`` extracts each row's columns rather than a
            # per-cell ``.get`` (which also blocks the event loop on large ingests).
            # ``itemgetter`` returns a bare value for one column, a tuple for several.
            # A direct ``ClickHouseClientPort.insert_rows`` caller may pass a sparse
            # row missing one of the first row's keys, though — fall back to the
            # None-filling ``.get`` for the whole batch then, preserving the legacy
            # NULL-insertion contract.
            pick: Callable[[JsonDict], Any] = itemgetter(*columns)
            single_column = len(columns) == 1

            try:
                data = (
                    [[pick(row)] for row in batch]
                    if single_column
                    else [list(pick(row)) for row in batch]
                )

            except KeyError:
                data = [[row.get(col) for col in columns] for row in batch]

            await ch.insert(
                table,
                data,
                column_names=columns,
                database=database,
                settings={"max_execution_time": timeout_sec},
            )
            accepted_total += len(batch)

        return ClickHouseInsertResult(accepted=accepted_total)

    # ....................... #

    @exc_interceptor.coroutine("clickhouse.run_command")  # type: ignore[untyped-decorator]
    async def run_command(
        self,
        command: str,
        params: BaseModel | JsonDict | Sequence[Any] | None = None,
        *,
        database: str | None = None,
        timeout: timedelta | None = None,
    ) -> None:
        if isinstance(params, BaseModel):
            bound_params = parameters_from_model(params)

        else:
            bound_params = params  # type: ignore[assignment]

        timeout_sec = self.__timeout_sec(timeout)
        target_db = self.__database(database)
        ch = self.__require_client()

        await ch.command(  # type: ignore[untyped-call]
            command,
            parameters=bound_params,
            settings=self.__query_settings(
                database=target_db,
                timeout_sec=timeout_sec,
            ),
        )
