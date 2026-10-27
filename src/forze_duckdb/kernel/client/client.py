"""Async DuckDB client: in-process engine bridged to asyncio via a bounded executor."""

from forze_duckdb._compat import require_duckdb

require_duckdb()

# ....................... #

import asyncio
import threading
from collections.abc import AsyncGenerator, Callable, Mapping, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import timedelta
from decimal import Decimal
from typing import Any, TypeVar, final

import attrs
import duckdb
from pydantic import BaseModel

from forze.application.integrations.dynamic_read import (
    MULTI_STATEMENT_CODE,
    STATEMENT_INVALID_CODE,
    WRITE_REFUSED_CODE,
)
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import JsonDict

from .._logger import logger
from .port import DuckDbDynamicReadClientPort
from .sql import apply_limit_offset
from .value_objects import DuckDbConfig, DuckDbQueryResult

# ----------------------- #

T = TypeVar("T")

_DEFAULT_EXTENSIONS: tuple[str, ...] = ("httpfs",)

QUERY_TIMEOUT_CODE = "duckdb_query_timeout"
"""Code of the infrastructure error a statement raises when its timeout fires."""

INSTANCE_RETIRED_CODE = "duckdb_instance_retired"
"""Code of the infrastructure error a retired client raises for every statement."""

_VENDED_SECRET_PREFIX = "__internal_ic_"  # nosec B105 - a secret-name prefix, not a secret
"""Name prefix of the temporary S3 secrets the ``iceberg`` extension creates per ``LoadTable``."""

_REINTERRUPT_S = 0.1
"""How often a timed-out statement is interrupted again until its worker returns."""

# ....................... #


_DECIMAL_MAX_WIDTH = 38
"""DuckDB ``DECIMAL`` holds at most 38 digits. The driver does not fail past that —
it silently rebinds the parameter as ``DOUBLE``, rounding the value (and a
non-finite ``Decimal`` binds as a float ``nan``/``inf``). Refuse instead: a money
value that cannot bind exactly must not bind at all."""


def _require_exact_decimal(value: Decimal) -> None:
    """Refuse a ``Decimal`` DuckDB cannot bind as an exact ``DECIMAL(38)``."""

    if not value.is_finite():
        raise exc.precondition(f"Non-finite numeric not allowed: {value!r}")

    if value.is_zero():
        return

    _sign, digits, exponent = value.as_tuple()
    exponent = int(exponent)

    # Trailing zero digits are spelling, not precision — strip before counting.
    while len(digits) > 1 and digits[-1] == 0:
        digits = digits[:-1]
        exponent += 1

    scale = max(0, -exponent)
    integer_digits = max(0, len(digits) + exponent)

    if integer_digits + scale > _DECIMAL_MAX_WIDTH:
        raise exc.precondition(
            f"Decimal exceeds DuckDB DECIMAL({_DECIMAL_MAX_WIDTH}) exactness "
            f"({integer_digits} integer digits, scale {scale}); the driver would "
            f"silently bind it as a rounded DOUBLE: {value!r}"
        )


def _validate_param_value(value: Any) -> None:
    if isinstance(value, Decimal):
        _require_exact_decimal(value)

    elif isinstance(value, (list, tuple)):
        for item in value:  # pyright: ignore[reportUnknownVariableType]
            _validate_param_value(item)

    elif isinstance(value, dict):
        for item in value.values():  # pyright: ignore[reportUnknownVariableType]
            _validate_param_value(item)


def _params_dict(params: BaseModel | JsonDict | None) -> dict[str, Any] | None:
    """Coerce params to a ``$name`` binding dict, or ``None`` when there is nothing to bind."""

    if params is None:
        return None

    if isinstance(params, BaseModel):
        data: dict[str, Any] = params.model_dump()

    else:
        data = dict(params)

    for value in data.values():
        _validate_param_value(value)

    return data or None


# ....................... #


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


# ....................... #


def _single_select(cursor: Any, statement: str) -> Any:
    """Classify *statement* with the engine's own parser and return the one ``SELECT`` in it.

    Raises when the text holds anything but exactly one statement, or a statement that is not a
    ``SELECT``. The caller executes the returned object rather than the string, so the text that
    runs is the text that was classified. A belt, not the read-only boundary: a ``SELECT`` can
    still call a side-effecting table function.
    """

    statements = cursor.extract_statements(statement)

    if len(statements) != 1:
        raise exc.validation(
            "DuckDB dynamic read executes exactly one statement per call.",
            code=MULTI_STATEMENT_CODE,
            details={"statement_count": len(statements)},
        )

    (single,) = statements

    if single.type != duckdb.StatementType.SELECT:
        raise exc.precondition(
            "DuckDB dynamic read executes SELECT statements only.",
            code=WRITE_REFUSED_CODE,
            details={"statement_type": single.type.name},
        )

    return single


# ....................... #


def _drop_vended_secrets(conn: Any, lock: threading.Lock) -> None:
    """Drop every temporary S3 secret the ``iceberg`` extension vended.

    The extension creates one per ``LoadTable`` and keeps it once the statement's transaction
    commits. Kept, it lets a revoked principal read by path until the credential expires
    (RFC 0078 EXECUTION-LOG D-025), and once expired it outranks the fresh secret of the next
    statement on the same table, which then fails (D-047). *lock* serializes drops: two
    transactions dropping one secret conflict.

    Safe beside a running statement only because DuckDB keeps a statement's vended secret
    uncommitted until the statement ends, so ``duckdb_secrets()`` here cannot list it (D-048).
    The P0 probe ``test_a_running_statements_secret_is_invisible_to_another_cursor`` is the
    tripwire: if an engine bump makes that secret visible, it fails first.
    """

    with lock:
        cursor = conn.cursor()

        try:
            rows = cursor.execute(
                "SELECT name FROM duckdb_secrets() WHERE starts_with(name, $prefix)",
                {"prefix": _VENDED_SECRET_PREFIX},
            ).fetchall()

            for (name,) in rows:
                cursor.execute(f"DROP TEMPORARY SECRET {_quote_identifier(name)}")

        finally:
            cursor.close()


# ....................... #


def _timed_out(detail: str) -> CoreException:
    # Infrastructure (not internal): timeouts are transient backend conditions, mirroring how
    # Postgres/ClickHouse classify query timeouts for retry purposes.
    return exc.infrastructure(f"DuckDB query exceeded timeout {detail}", code=QUERY_TIMEOUT_CODE)


def _retired() -> CoreException:
    return exc.infrastructure(
        "DuckDB instance retired after a failed vended-secret drop; it serves nothing.",
        code=INSTANCE_RETIRED_CODE,
    )


def _open_cursor(conn: Any, holder: dict[str, Any]) -> Any:
    """Open a statement's cursor and publish it for interruption, unless its caller gave up.

    Publish first, then check: the timed-out caller raises the flag first, then reads the cursor,
    so one of the two always sees the other.
    """

    cursor = conn.cursor()
    holder["cursor"] = cursor

    if holder.get("expired"):
        holder.pop("cursor", None)
        cursor.close()
        raise _timed_out("before it started")

    return cursor


def _keep_interrupting(loop: asyncio.AbstractEventLoop, fut: Any, cursor: Any) -> None:
    """Interrupt *cursor* until *fut* settles.

    DuckDB drops an interrupt that arrives before its statement starts executing, so a single
    call can miss a worker that has published its cursor but not yet begun.
    """

    if cursor is None or fut.done():
        return

    try:
        cursor.interrupt()

    except Exception:  # nosec B110 - best-effort cancellation
        logger.debug("DuckDB cursor interrupt failed", exc_info=True)

    loop.call_later(_REINTERRUPT_S, _keep_interrupting, loop, fut, cursor)


def _release_from_worker(loop: asyncio.AbstractEventLoop, admission: asyncio.Semaphore) -> None:
    """Free an admission slot once the work that held it has finished, from any thread."""

    try:
        loop.call_soon_threadsafe(admission.release)

    except RuntimeError:  # the loop is closed: no caller is left waiting for the slot
        logger.debug("DuckDB admission slot released after its loop closed")


def _consume_late_result(fut: asyncio.Future[Any]) -> None:
    """Retrieve a timed-out worker's outcome, so asyncio does not report it as unhandled."""

    if not fut.cancelled():
        fut.exception()


def _abandon(
    loop: asyncio.AbstractEventLoop,
    work: Future[Any],
    fut: asyncio.Future[Any],
    holder: dict[str, Any],
) -> None:
    """Stop the statement of a caller that stopped waiting for it, timed out or cancelled.

    Returns at once rather than await the worker: an interrupt does not cut a blocking HTTP call
    short, so the worker may run on for minutes (RFC 0078 EXECUTION-LOG D-028). It keeps its
    thread and its admission slot until DuckDB unwinds; ``close`` waits for it.
    """

    holder["expired"] = True
    work.cancel()  # a statement still queued never starts
    _keep_interrupting(loop, fut, holder.get("cursor"))
    fut.add_done_callback(_consume_late_result)


# ....................... #


def _effective_limit(limit: int | None, max_rows: int | None) -> int | None:
    """Combine an explicit window ``limit`` with an options ``max_rows`` cap."""

    candidates = [c for c in (limit, max_rows) if c is not None]

    return min(candidates) if candidates else None


# ....................... #


@final
@attrs.define(slots=True)
class DuckDbClient(DuckDbDynamicReadClientPort):
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
    __admission: asyncio.Semaphore | None = attrs.field(default=None, init=False)
    __init_lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False)
    __drop_lock: threading.Lock | None = attrs.field(default=None, init=False)
    __retired: bool = attrs.field(default=False, init=False)

    # ....................... #
    # Lifecycle

    async def initialize(
        self,
        database: str = ":memory:",
        *,
        config: DuckDbConfig | None = None,
        extensions: Sequence[str] = _DEFAULT_EXTENSIONS,
        install_extensions: bool = True,
        connect_config: Mapping[str, Any] | None = None,
        admission: asyncio.Semaphore | None = None,
        drop_vended_secrets: bool = False,
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
        :param install_extensions: ``False`` loads *extensions* without installing them, for an
            image that ships them pre-installed and an engine that must not download at runtime.
        :param connect_config: Options passed to ``duckdb.connect``, for the settings that
            only take effect at connect time (``allow_persistent_secrets``, autoloading).
        :param admission: A semaphore shared with other clients. Each statement holds a slot
            from submission until its worker thread returns, so it bounds statements across
            every client that shares it.
        :param drop_vended_secrets: Drop the S3 secrets an attached Iceberg catalog vends after
            every read, for an instance attached with vended credentials (RFC 0078). A drop that
            fails retires the client: see :attr:`retired`.
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
                conn = duckdb.connect(
                    database,
                    read_only=cfg.read_only,
                    config=dict(connect_config or {}),
                )

                if cfg.threads is not None:
                    conn.execute(f"PRAGMA threads={int(cfg.threads)}")

                if cfg.memory_limit is not None:
                    conn.execute(f"SET memory_limit='{cfg.memory_limit}'")

                for ext in extensions:
                    if install_extensions:
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
            self.__admission = admission
            self.__drop_lock = threading.Lock() if drop_vended_secrets else None
            self.__retired = False  # a fresh connection holds no leftover; close drained workers
            self.__executor = ThreadPoolExecutor(
                max_workers=cfg.max_concurrent_queries,
                thread_name_prefix="forze-duckdb",
            )
            logger.trace("DuckDB connection opened")

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
            self.__admission = None

            if executor is not None:
                await asyncio.to_thread(executor.shutdown, wait=True)

            if conn is not None:
                await asyncio.to_thread(conn.close)

            if executor is not None or conn is not None:
                logger.trace("DuckDB connection closed")

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

    @property
    def retired(self) -> bool:
        """Whether a failed vended-secret drop retired this client; it then refuses statements.

        A secret left behind fails every later read of its table once it expires
        (RFC 0078 EXECUTION-LOG D-047, D-049), so a retired instance must be rebuilt.
        """

        return self.__retired

    def __end_read(self, conn: Any) -> None:
        """Drop the read's vended secrets, if this client drops them. Never raises.

        The read's own outcome, its rows or its error, is what the caller gets either way. A
        failed drop is logged and retires the client (D-049).
        """

        lock = self.__drop_lock

        if lock is None:
            return

        try:
            _drop_vended_secrets(conn, lock)

        except Exception:
            self.__retired = True
            logger.warning(
                "DuckDB could not drop vended secrets after a read; the instance is retired",
                exc_info=True,
            )

    # ....................... #

    async def __submit(
        self,
        fn: Callable[[], T],
        holder: dict[str, Any],
        timeout: timedelta | None,
    ) -> T:
        """Run *fn* on the dedicated executor, honoring *timeout* via cursor interrupt.

        One deadline covers the wait for an admission slot and the statement itself. The slot is
        released when the worker returns, not when the caller stops waiting, so a cancelled or
        timed-out caller never frees capacity its statement still uses.
        """

        if self.__retired:
            raise _retired()

        loop = asyncio.get_running_loop()
        executor = self.__require_executor()
        admission = self.__admission
        timeout_sec = None if timeout is None else max(0.0, timeout.total_seconds())
        deadline = None if timeout_sec is None else loop.time() + timeout_sec

        if admission is not None:
            try:
                async with asyncio.timeout_at(deadline):
                    await admission.acquire()

            except TimeoutError:
                raise _timed_out(f"of {timeout_sec}s waiting for a slot") from None

        try:
            if self.__retired:  # again: a drop may have failed while this waited for a slot
                raise _retired()

            work: Future[T] = executor.submit(fn)

        except BaseException:
            if admission is not None:
                admission.release()

            raise

        if admission is not None:
            work.add_done_callback(lambda _work: _release_from_worker(loop, admission))

        fut = asyncio.wrap_future(work, loop=loop)
        remaining = None if deadline is None else max(0.0, deadline - loop.time())

        try:
            # ``wait`` never cancels *fut*, so a cancelled caller reaches the handler below.
            done, _pending = await asyncio.wait({fut}, timeout=remaining)

        except asyncio.CancelledError:
            _abandon(loop, work, fut, holder)
            raise

        if fut not in done:
            _abandon(loop, work, fut, holder)
            raise _timed_out(f"of {timeout_sec}s")

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
            cursor = _open_cursor(conn, holder)

            try:
                if bind is not None:
                    cursor.execute(final_sql, bind)

                else:
                    cursor.execute(final_sql)

                return DuckDbQueryResult(arrow=cursor.fetch_arrow_table())

            finally:
                holder.pop("cursor", None)
                cursor.close()
                self.__end_read(conn)

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
            cursor = _open_cursor(conn, holder)

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

    async def run_dynamic(
        self,
        statement: str,
        params: JsonDict | None = None,
        *,
        row_probe: int,
        timeout: timedelta,
        use: str | None = None,
    ) -> list[JsonDict]:
        """Run one runtime-authored ``SELECT`` and return at most *row_probe* rows.

        On a fresh cursor: ``USE`` *use* when given, classify *statement* with the engine's
        parser (exactly one ``SELECT``, else a ``dynamic_read_multi_statement`` /
        ``dynamic_read_write_refused`` error without a route), execute the classified statement
        unmodified, and fetch *row_probe* rows. The vended secrets the statement left behind
        are dropped afterwards, whatever the outcome.

        :param statement: The statement text, never rewritten.
        :param params: ``$name`` bindings.
        :param row_probe: Rows to fetch; the caller's cap plus one.
        :param timeout: Statement timeout, enforced by cursor interrupt.
        :param use: Dotted ``catalog.schema`` path issued as ``USE`` first; each part is quoted.
        :returns: The fetched rows as dictionaries.
        """

        conn = self.__require_conn()
        bind = _params_dict(params)
        holder: dict[str, Any] = {}

        def _run(cursor: Any) -> list[JsonDict]:
            if use is not None:
                cursor.execute(
                    "USE " + ".".join(_quote_identifier(part) for part in use.split("."))
                )

            single = _single_select(cursor, statement)

            if bind is not None:
                cursor.execute(single, bind)

            else:
                cursor.execute(single)

            columns = [column[0] for column in cursor.description]
            duplicates = sorted({name for name in columns if columns.count(name) > 1})

            if duplicates:
                # A row is a mapping: a second column of the same name would replace the first.
                raise exc.validation(
                    "DuckDB dynamic read refuses a result with duplicate column names.",
                    code=STATEMENT_INVALID_CODE,
                    details={"detail": f"duplicate column names: {', '.join(duplicates)}"},
                )

            return [dict(zip(columns, row, strict=True)) for row in cursor.fetchmany(row_probe)]

        def _exec() -> list[JsonDict]:
            cursor = _open_cursor(conn, holder)

            try:
                return _run(cursor)

            finally:
                holder.pop("cursor", None)
                # An unexhausted stream rolls back here, discarding its own vended secret.
                cursor.close()
                self.__end_read(conn)

        return await self.__submit(_exec, holder, timeout)

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        """Run a trivial query to check the engine is responsive."""

        try:
            await self.run_query("SELECT 1")
            return "ok", True

        except Exception as e:
            logger.debug("DuckDB health check failed", exc_info=True)
            return str(e), False
