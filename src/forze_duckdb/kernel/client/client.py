"""Async DuckDB client: in-process engine bridged to asyncio via a bounded executor."""

from forze_duckdb._compat import require_duckdb

require_duckdb()

# ....................... #

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import Any, AsyncGenerator, Callable, Mapping, Sequence, TypeVar, final

import attrs
import duckdb

from pydantic import BaseModel

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

from .._logger import logger
from .port import DuckDbClientPort
from .sql import apply_limit_offset
from .value_objects import DuckDbConfig, DuckDbQueryResult

# ----------------------- #

T = TypeVar("T")

_DEFAULT_EXTENSIONS: tuple[str, ...] = ("httpfs",)

# ....................... #


def _params_dict(params: BaseModel | JsonDict | None) -> dict[str, Any] | None:
    """Coerce params to a ``$name`` binding dict, or ``None`` when there is nothing to bind."""

    if params is None:
        return None

    if isinstance(params, BaseModel):
        data: dict[str, Any] = params.model_dump()

    else:
        data = dict(params)

    return data or None


# ....................... #


def _effective_limit(limit: int | None, max_rows: int | None) -> int | None:
    """Combine an explicit window ``limit`` with an options ``max_rows`` cap."""

    candidates = [c for c in (limit, max_rows) if c is not None]

    return min(candidates) if candidates else None


# ....................... #


@final
@attrs.define(slots=True)
class DuckDbClient(DuckDbClientPort):
    """In-process DuckDB client exposing an async, concurrency-safe query surface.

    DuckDB is synchronous and embedded; queries run on a dedicated bounded thread
    pool so the event loop stays responsive and analytics load cannot starve other
    ``asyncio.to_thread`` users. DuckDB releases the GIL during query execution, so
    the offload is genuinely concurrent. Each query uses its own ``cursor()`` (an
    independent connection over the shared database) so concurrent reads do not
    serialize and a timeout can interrupt exactly one query via that cursor.
    """

    __conn: Any = attrs.field(default=None, init=False)
    __config: DuckDbConfig | None = attrs.field(default=None, init=False)
    __executor: ThreadPoolExecutor | None = attrs.field(default=None, init=False)
    __init_lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False)

    # ....................... #
    # Lifecycle

    async def initialize(
        self,
        database: str = ":memory:",
        *,
        config: DuckDbConfig | None = None,
        extensions: Sequence[str] = _DEFAULT_EXTENSIONS,
        secrets: Sequence[str] = (),
        sources: Mapping[str, str] | None = None,
        bootstrap_sql: Sequence[str] = (),
    ) -> None:
        """Open the connection, load extensions, register secrets and views.

        Idempotent; later calls are no-ops. Concurrent calls serialize on an
        internal lock so only one coroutine performs the setup.

        :param database: DuckDB database path, or ``:memory:`` for an ephemeral db.
        :param config: Engine/executor configuration.
        :param extensions: Extensions to ``INSTALL`` + ``LOAD`` (e.g. ``httpfs``, ``iceberg``).
        :param secrets: Raw ``CREATE SECRET ...`` statements (object-storage credentials).
        :param sources: Optional ``name -> scan expression`` map registered as
            ``CREATE OR REPLACE VIEW name AS SELECT * FROM <expr>`` (the "view" source style).
        :param bootstrap_sql: Additional raw statements run after the above.
        """

        async with self.__init_lock:
            if self.__conn is not None:
                return

            cfg = config or DuckDbConfig()
            self.__config = cfg

            def _open() -> Any:
                conn = duckdb.connect(database, read_only=cfg.read_only)

                if cfg.threads is not None:
                    conn.execute(f"PRAGMA threads={int(cfg.threads)}")

                if cfg.memory_limit is not None:
                    conn.execute(f"SET memory_limit='{cfg.memory_limit}'")

                for ext in extensions:
                    conn.execute(f"INSTALL {ext}")
                    conn.execute(f"LOAD {ext}")

                for secret_sql in secrets:
                    conn.execute(secret_sql)

                if sources:
                    for name, scan_expr in sources.items():
                        conn.execute(
                            f"CREATE OR REPLACE VIEW {name} "  # nosec B608
                            f"AS SELECT * FROM {scan_expr}"
                        )

                for stmt in bootstrap_sql:
                    conn.execute(stmt)

                return conn

            self.__conn = await asyncio.to_thread(_open)
            self.__executor = ThreadPoolExecutor(
                max_workers=cfg.max_concurrent_queries,
                thread_name_prefix="forze-duckdb",
            )
            logger.debug("DuckDB connection opened")

    # ....................... #

    async def close(self) -> None:
        """Shut down the executor and close the connection. No-op if not initialized.

        Serializes on the same lock as :meth:`initialize` so a concurrent
        initialize cannot interleave with teardown.
        """

        async with self.__init_lock:
            executor = self.__executor
            conn = self.__conn

            self.__executor = None
            self.__conn = None
            self.__config = None

            if executor is not None:
                await asyncio.to_thread(executor.shutdown, wait=True)

            if conn is not None:
                await asyncio.to_thread(conn.close)

            if executor is not None or conn is not None:
                logger.debug("DuckDB connection closed")

    # ....................... #
    # Helpers

    def __require_conn(self) -> Any:
        if self.__conn is None:
            raise exc.internal("DuckDB client is not initialized")

        return self.__conn

    # ....................... #

    def __require_executor(self) -> ThreadPoolExecutor:
        if self.__executor is None:
            raise exc.internal("DuckDB client is not initialized")

        return self.__executor

    # ....................... #

    async def __submit(
        self,
        fn: Callable[[], T],
        holder: dict[str, Any],
        timeout: timedelta | None,
    ) -> T:
        """Run *fn* on the dedicated executor, honoring *timeout* via cursor interrupt."""

        loop = asyncio.get_running_loop()
        fut = loop.run_in_executor(self.__require_executor(), fn)

        if timeout is None:
            return await fut

        timeout_sec = max(0.0, timeout.total_seconds())
        done, _pending = await asyncio.wait({fut}, timeout=timeout_sec)

        if fut not in done:
            cursor = holder.get("cursor")

            if cursor is not None:
                try:
                    cursor.interrupt()

                except Exception:  # noqa: BLE001 # nosec B110 - best-effort cancellation
                    logger.debug("DuckDB cursor interrupt failed", exc_info=True)

            await asyncio.gather(fut, return_exceptions=True)
            # Infrastructure (not internal): timeouts are transient backend
            # conditions, mirroring how Postgres/ClickHouse classify query
            # timeouts for retry purposes.
            raise exc.infrastructure(
                f"DuckDB query exceeded timeout of {timeout_sec}s"
            )

        return fut.result()

    # ....................... #
    # Query API

    async def run_query(
        self,
        sql: str,
        params: BaseModel | JsonDict | None = None,
        *,
        limit: int | None = None,
        offset: int | None = None,
        max_rows: int | None = None,
        timeout: timedelta | None = None,
    ) -> DuckDbQueryResult:
        conn = self.__require_conn()
        final_sql = apply_limit_offset(sql, _effective_limit(limit, max_rows), offset)
        bind = _params_dict(params)
        holder: dict[str, Any] = {}

        def _exec() -> DuckDbQueryResult:
            cursor = conn.cursor()
            holder["cursor"] = cursor

            try:
                if bind is not None:
                    cursor.execute(final_sql, bind)

                else:
                    cursor.execute(final_sql)

                return DuckDbQueryResult(arrow=cursor.fetch_arrow_table())

            finally:
                holder.pop("cursor", None)
                cursor.close()

        return await self.__submit(_exec, holder, timeout)

    # ....................... #

    async def run_query_all_pages(
        self,
        sql: str,
        params: BaseModel | JsonDict | None = None,
        *,
        max_rows: int | None = None,
        timeout: timedelta | None = None,
        fetch_batch_size: int = 2000,
    ) -> list[JsonDict]:
        if fetch_batch_size < 1:
            raise exc.internal("fetch_batch_size must be >= 1")

        # DuckDB materializes the full result in-process; batching happens in the
        # adapter. ``fetch_batch_size`` is accepted for port symmetry.
        result = await self.run_query(
            sql,
            params,
            limit=max_rows,
            max_rows=max_rows,
            timeout=timeout,
        )

        return result.rows

    # ....................... #

    async def run_query_streamed(
        self,
        sql: str,
        params: BaseModel | JsonDict | None = None,
        *,
        max_rows: int | None = None,
        timeout: timedelta | None = None,
        fetch_batch_size: int = 2000,
    ) -> AsyncGenerator[Sequence[JsonDict]]:
        """Yield result rows in ``fetch_batch_size`` windows without a full pylist.

        DuckDB computes the result as a native Arrow table (held columnar and
        compact), but only one record batch is converted to plain dict rows at a
        time, so the Python-heap footprint stays at a single window instead of the
        whole result materialized as a ``list[dict]``.
        """

        if fetch_batch_size < 1:
            raise exc.internal("fetch_batch_size must be >= 1")

        result = await self.run_query(
            sql,
            params,
            limit=max_rows,
            max_rows=max_rows,
            timeout=timeout,
        )

        for batch in result.arrow.to_batches(max_chunksize=fetch_batch_size):
            yield batch.to_pylist()

    # ....................... #

    async def run_command(
        self,
        command: str,
        params: BaseModel | JsonDict | None = None,
        *,
        timeout: timedelta | None = None,
    ) -> None:
        conn = self.__require_conn()
        bind = _params_dict(params)
        holder: dict[str, Any] = {}

        def _exec() -> None:
            cursor = conn.cursor()
            holder["cursor"] = cursor

            try:
                if bind is not None:
                    cursor.execute(command, bind)

                else:
                    cursor.execute(command)

            finally:
                holder.pop("cursor", None)
                cursor.close()

        await self.__submit(_exec, holder, timeout)

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        """Run a trivial query to check the engine is responsive."""

        try:
            await self.run_query("SELECT 1")
            return "ok", True

        except Exception as e:  # noqa: BLE001 - health must not raise
            logger.debug("DuckDB health check failed", exc_info=True)
            return str(e), False
