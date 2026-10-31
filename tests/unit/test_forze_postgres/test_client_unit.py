"""Unit tests for :mod:`forze_postgres.kernel.client.client` helpers (no DB I/O)."""

import asyncio
import gc
import socket
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from forze.base.exceptions import CoreException

pytest.importorskip("psycopg")

from psycopg import IsolationLevel, OperationalError
from psycopg_pool import PoolTimeout

from forze_postgres.kernel.client.client import (
    PostgresClient,
    PostgresConfig,
    PostgresTransactionOptions,
    _checkout_deadline,
    _pool_check,
    _pool_reset_transaction_attributes,
)
from forze_postgres.kernel.client.helpers import isolation_level_enum

# ----------------------- #


class TestPostgresConfig:
    def test_rejects_min_greater_than_max(self) -> None:
        with pytest.raises(CoreException, match="Minimum size must be less"):
            PostgresConfig(min_size=5, max_size=3)

    def test_rejects_negative_min_size(self) -> None:
        with pytest.raises(CoreException, match="Minimum size must be greater"):
            PostgresConfig(min_size=-1)

    def test_rejects_negative_num_workers(self) -> None:
        with pytest.raises(CoreException, match="workers must be greater"):
            PostgresConfig(num_workers=-1)

    def test_rejects_negative_pool_headroom(self) -> None:
        with pytest.raises(CoreException, match="pool_headroom"):
            PostgresConfig(pool_headroom=-1)

    def test_rejects_max_concurrent_queries_below_one(self) -> None:
        with pytest.raises(CoreException, match="max_concurrent_queries"):
            PostgresConfig(max_concurrent_queries=0)

    def test_warns_on_large_min_and_max_pool_size(self) -> None:
        mock_logger = MagicMock()

        with patch(
            "forze_postgres.kernel.client.value_objects.logger",
            mock_logger,
        ):
            PostgresConfig(min_size=11, max_size=101)

        assert mock_logger.warning.call_count == 2
        joined = " ".join(str(c) for c in mock_logger.warning.call_args_list)
        assert "Minimum size is greater than 10" in joined
        assert "Maximum size is greater than 100" in joined


class TestIsolationLevelEnum:
    def test_rejects_unknown_level(self) -> None:
        with pytest.raises(CoreException, match="Unsupported transaction isolation"):
            isolation_level_enum("phantom")

    def test_maps_all_levels(self) -> None:
        assert isolation_level_enum("read_committed") is IsolationLevel.READ_COMMITTED
        assert isolation_level_enum("repeatable_read") is IsolationLevel.REPEATABLE_READ
        assert isolation_level_enum("serializable") is IsolationLevel.SERIALIZABLE


class TestTransactionOptionsAreDefault:
    """Default options (read-write + read committed) must touch nothing at all."""

    def test_defaults(self) -> None:
        assert PostgresClient._options_are_default(PostgresTransactionOptions())

    def test_read_only_is_not_default(self) -> None:
        assert not PostgresClient._options_are_default(
            PostgresTransactionOptions(read_only=True),
        )

    def test_isolation_is_not_default(self) -> None:
        assert not PostgresClient._options_are_default(
            PostgresTransactionOptions(isolation="repeatable_read"),
        )


def _mock_conn() -> MagicMock:
    """Connection mock with awaitable attribute setters and a strict cursor."""

    conn = MagicMock()
    conn.set_isolation_level = AsyncMock()
    conn.set_read_only = AsyncMock()
    conn.set_autocommit = AsyncMock()
    return conn


class TestApplyTransactionOptions:
    """Options are applied as connection attributes BEFORE ``BEGIN`` (0 round-trips).

    psycopg folds ``isolation_level`` / ``read_only`` into the ``BEGIN``
    statement itself; a separate ``SET TRANSACTION`` statement must never be
    executed (that was the +1 round-trip per root transaction). The attribute
    setters are pure client-side on an idle connection.
    """

    @pytest.mark.asyncio
    async def test_sets_attributes_and_executes_no_sql(self) -> None:
        conn = _mock_conn()

        options = PostgresTransactionOptions(isolation="serializable", read_only=True)
        await PostgresClient._apply_transaction_options(conn, options)

        conn.set_isolation_level.assert_awaited_once_with(IsolationLevel.SERIALIZABLE)
        conn.set_read_only.assert_awaited_once_with(True)

        # No SET TRANSACTION (or any other statement) may be executed: the
        # whole point of the attribute approach is zero extra round-trips.
        conn.cursor.assert_not_called()
        conn.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_read_write_leaves_read_only_attribute_alone(self) -> None:
        conn = _mock_conn()

        options = PostgresTransactionOptions(isolation="repeatable_read")
        await PostgresClient._apply_transaction_options(conn, options)

        conn.set_isolation_level.assert_awaited_once_with(
            IsolationLevel.REPEATABLE_READ,
        )
        conn.set_read_only.assert_not_called()


class TestRestoreTransactionAttributes:
    """The ``finally`` belt: attributes are cleared back to psycopg defaults."""

    @pytest.mark.asyncio
    async def test_clears_both_attributes(self) -> None:
        conn = _mock_conn()

        await PostgresClient._restore_transaction_attributes(conn)

        conn.set_isolation_level.assert_awaited_once_with(None)
        conn.set_read_only.assert_awaited_once_with(None)
        conn.cursor.assert_not_called()

    @pytest.mark.asyncio
    async def test_swallows_restore_failure(self) -> None:
        """A broken connection must not mask the original transaction error."""

        conn = _mock_conn()
        conn.set_isolation_level.side_effect = RuntimeError("connection is broken")

        await PostgresClient._restore_transaction_attributes(conn)  # does not raise


class TestPoolResetCallback:
    """The pool ``reset`` belt: attributes are cleared on every check-in."""

    @pytest.mark.asyncio
    async def test_clears_leaked_attributes(self) -> None:
        conn = _mock_conn()
        conn.isolation_level = IsolationLevel.SERIALIZABLE
        conn.read_only = True
        conn.autocommit = False

        await _pool_reset_transaction_attributes(conn)

        conn.set_isolation_level.assert_awaited_once_with(None)
        conn.set_read_only.assert_awaited_once_with(None)

    @pytest.mark.asyncio
    async def test_noop_when_attributes_are_default(self) -> None:
        """Common check-in path (nothing leaked) must not even call the setters."""

        conn = _mock_conn()
        conn.isolation_level = None
        conn.read_only = None
        conn.autocommit = False

        await _pool_reset_transaction_attributes(conn)

        conn.set_isolation_level.assert_not_called()
        conn.set_read_only.assert_not_called()
        conn.set_autocommit.assert_not_called()

    @pytest.mark.asyncio
    async def test_clears_poisoned_autocommit(self) -> None:
        """Second belt for P12a: a leaked autocommit flag is cleared on check-in."""

        conn = _mock_conn()
        conn.isolation_level = None
        conn.read_only = None
        conn.autocommit = True

        await _pool_reset_transaction_attributes(conn)

        conn.set_autocommit.assert_awaited_once_with(False)
        # The other attributes stay untouched on this clean path.
        conn.set_isolation_level.assert_not_called()
        conn.set_read_only.assert_not_called()


_BOUND = timedelta(seconds=5)
"""A check deadline the tests that are not about it never reach."""


def _ping_conn(execute: Any) -> MagicMock:
    """Connection mock for ``check_connection``: idle, not in autocommit, pinged via ``execute``."""

    conn = _mock_conn()
    conn.autocommit = False
    conn.execute = AsyncMock(side_effect=execute)
    return conn


def _lost(_query: str) -> None:
    raise OperationalError("the connection is lost")


def _checker(pool: MagicMock, timeout: timedelta = _BOUND) -> Any:
    return _pool_check(lambda: pool, timeout)


class TestPoolCheckCallback:
    """The checkout ``check`` (#776): on by default, and one failure sweeps the idle pool."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("config", "checked"),
        [(PostgresConfig(), True), (PostgresConfig(check_on_checkout=False), False)],
    )
    async def test_pool_is_built_with_the_check_unless_disabled(
        self, config: PostgresConfig, checked: bool
    ) -> None:
        with patch("forze_postgres.kernel.client.client.AsyncConnectionPool") as pool_cls:
            pool_cls.return_value.open = AsyncMock()
            await PostgresClient().initialize("postgresql://unused", config=config)

        assert (pool_cls.call_args.kwargs["check"] is not None) is checked

    @pytest.mark.asyncio
    async def test_a_live_connection_passes_without_a_sweep(self) -> None:
        pool = MagicMock(check=AsyncMock())
        conn = _ping_conn(None)

        await _checker(pool)(conn)

        conn.execute.assert_awaited_once_with("")
        pool.check.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_dead_connection_sweeps_the_pool_then_fails(self) -> None:
        pool = MagicMock(check=AsyncMock())

        with pytest.raises(OperationalError):
            await _checker(pool)(_ping_conn(_lost))

        pool.check.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_concurrent_failures_share_one_sweep(self) -> None:
        """The sweep holds until all three checks have failed, so each meets it running."""

        failures = 0
        all_failed = asyncio.Event()

        def lost(query: str) -> None:
            nonlocal failures
            failures += 1

            if failures == 3:
                all_failed.set()

            _lost(query)

        pool = MagicMock(check=AsyncMock(side_effect=all_failed.wait))
        check = _checker(pool)

        results = await asyncio.gather(
            *(check(_ping_conn(lost)) for _ in range(3)),
            return_exceptions=True,
        )

        assert [type(result) for result in results] == [OperationalError] * 3
        pool.check.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_failure_after_a_finished_sweep_starts_another(self) -> None:
        pool = MagicMock(check=AsyncMock())
        check = _checker(pool)

        for _ in range(2):
            with pytest.raises(OperationalError):
                await check(_ping_conn(_lost))

        assert pool.check.await_count == 2

    @pytest.mark.asyncio
    async def test_a_cancelled_checkout_leaves_the_sweep_to_finish(self) -> None:
        """The sweep holds the idle connections outside the pool until it puts them back, so
        cancelling the checkout that waits on it must not cancel it."""

        started, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def sweep() -> None:
            started.set()
            await release.wait()
            finished.set()  # stands for: the idle connections are back in the pool

        pool = MagicMock(check=AsyncMock(side_effect=sweep))
        checkout = asyncio.ensure_future(_checker(pool)(_ping_conn(_lost)))
        await asyncio.wait_for(started.wait(), 1)

        checkout.cancel()

        with pytest.raises(asyncio.CancelledError):
            await checkout

        release.set()
        await asyncio.wait_for(finished.wait(), 1)

    @pytest.mark.asyncio
    async def test_a_stalled_sweep_holds_a_checkout_only_until_its_deadline(self) -> None:
        release, finished = asyncio.Event(), asyncio.Event()

        async def stalled() -> None:
            await release.wait()
            finished.set()

        pool = MagicMock(check=AsyncMock(side_effect=stalled))
        check = _checker(pool, timedelta(milliseconds=50))

        # Twice: the second failure finds the stalled sweep running and waits on it too.
        for _ in range(2):
            with pytest.raises(OperationalError):
                await asyncio.wait_for(check(_ping_conn(_lost)), 1)

        pool.check.assert_awaited_once()

        release.set()
        await asyncio.wait_for(finished.wait(), 1)

    @pytest.mark.asyncio
    async def test_a_ping_that_is_never_answered_fails_at_the_deadline(self) -> None:
        """The bound shuts the socket down rather than cancelling the ping, which psycopg
        would meet by waiting for the query again. The stub's read, like psycopg's, returns
        only when the socket ends."""

        loop = asyncio.get_running_loop()
        ours, theirs = socket.socketpair()
        ours.setblocking(False)

        async def unanswered(_query: str) -> None:
            if not await loop.sock_recv(ours, 1):
                raise OperationalError("server closed the connection unexpectedly")

        swept = asyncio.Event()
        pool = MagicMock(check=AsyncMock(side_effect=swept.set))
        conn = _ping_conn(unanswered)
        conn.pgconn.socket = ours.fileno()
        started = loop.time()

        try:
            with pytest.raises(OperationalError):
                await asyncio.wait_for(_checker(pool, timedelta(milliseconds=50))(conn), 2)

            elapsed = loop.time() - started
            await asyncio.wait_for(swept.wait(), 1)

        finally:
            ours.close()
            theirs.close()

        assert elapsed < 1

    @pytest.mark.asyncio
    async def test_a_ping_that_returns_as_the_deadline_shuts_it_still_fails(self) -> None:
        """Its reply may have been read just before the socket was shut down, but the
        connection is broken either way, so it must not be handed out."""

        loop = asyncio.get_running_loop()
        ours, theirs = socket.socketpair()
        ours.setblocking(False)

        async def returns_anyway(_query: str) -> None:
            await loop.sock_recv(ours, 1)  # b"" once the deadline shuts the socket down

        conn = _ping_conn(returns_anyway)
        conn.pgconn.socket = ours.fileno()

        try:
            with pytest.raises(TimeoutError, match="connection check timed out"):
                await _checker(MagicMock(check=AsyncMock()), timedelta(milliseconds=50))(conn)

        finally:
            ours.close()
            theirs.close()

    @pytest.mark.asyncio
    async def test_a_failed_sweep_is_logged_not_left_unretrieved(self) -> None:
        """The checkout stopped waiting before the sweep failed, so nothing else reads its
        error: unread, asyncio would report it when the task is collected."""

        release, failing = asyncio.Event(), asyncio.Event()

        async def broken() -> None:
            await release.wait()
            failing.set()
            raise RuntimeError("sweep broke")

        pool = MagicMock(check=AsyncMock(side_effect=broken))
        check = _checker(pool, timedelta(milliseconds=20))
        loop = asyncio.get_running_loop()
        reported: list[dict[str, Any]] = []
        loop.set_exception_handler(lambda _loop, context: reported.append(context))

        try:
            with patch("forze_postgres.kernel.client.client.logger") as log:
                with pytest.raises(OperationalError):
                    await check(_ping_conn(_lost))

                release.set()
                await asyncio.wait_for(failing.wait(), 1)

                for _ in range(3):  # the task finishes, then its done callbacks run
                    await asyncio.sleep(0)

            del check
            gc.collect()

        finally:
            loop.set_exception_handler(None)

        log.warning.assert_called_once()
        assert reported == []

    @pytest.mark.asyncio
    async def test_queueing_and_the_check_share_one_acquire_timeout(self) -> None:
        """The check ends at the checkout's deadline rather than a whole acquire timeout after
        it starts, so the time spent queued for the connection counts against it too."""

        acquire = timedelta(milliseconds=300)
        release, finished = asyncio.Event(), asyncio.Event()

        async def stalled() -> None:
            await release.wait()
            finished.set()

        check = _checker(MagicMock(check=AsyncMock(side_effect=stalled)), acquire)
        client = PostgresClient()
        pool = _QueueingPool(check, queued=0.2)  # two thirds of the timeout spent queued
        client._PostgresClient__pool = pool  # type: ignore[attr-defined]
        client._PostgresClient__acquire_timeout = acquire  # type: ignore[attr-defined]
        loop = asyncio.get_running_loop()
        started = loop.time()

        with pytest.raises(CoreException):
            async with client._acquire():
                pass

        elapsed = loop.time() - started
        release.set()
        await asyncio.wait_for(finished.wait(), 1)

        assert elapsed < acquire.total_seconds() + 0.08, elapsed
        assert _checkout_deadline.get() is None

    @pytest.mark.asyncio
    async def test_the_checkout_deadline_is_set_only_while_waiting(self) -> None:
        client, _conn, _pool = _client_with_stub_pool()

        async with client._acquire():
            inside = _checkout_deadline.get()

        assert inside is None
        assert _checkout_deadline.get() is None

    @pytest.mark.asyncio
    async def test_a_connection_handed_over_at_the_deadline_still_gets_its_ping(self) -> None:
        """Under saturation each returned connection goes to the longest waiter, the one nearest
        its deadline; cutting the ping there would discard a healthy connection."""

        loop = asyncio.get_running_loop()
        ours, theirs = socket.socketpair()
        ours.setblocking(False)

        async def answered_late(_query: str) -> None:
            loop.call_later(0.05, theirs.send, b"I")

            if not await loop.sock_recv(ours, 1):
                raise OperationalError("server closed the connection unexpectedly")

        pool = MagicMock(check=AsyncMock())
        conn = _ping_conn(answered_late)
        conn.pgconn.socket = ours.fileno()
        token = _checkout_deadline.set(loop.time())  # the checkout's deadline is now

        try:
            await _checker(pool, timedelta(milliseconds=300))(conn)

        finally:
            _checkout_deadline.reset(token)
            ours.close()
            theirs.close()

        pool.check.assert_not_awaited()


class _QueueingPool:
    """psycopg_pool's ``getconn`` reduced to what the shared deadline depends on: queue for a
    connection, run the ``check`` callback in the caller's task, give up when it fails."""

    def __init__(self, check: Any, *, queued: float) -> None:
        self.check = check
        self.queued = queued

    def get_stats(self) -> dict[str, int]:
        return {"connections_errors": 0}

    def connection(self, timeout: float | None = None) -> Any:
        @asynccontextmanager
        async def _cm() -> Any:
            await asyncio.sleep(self.queued)

            try:
                await self.check(_ping_conn(_lost))

            except Exception:
                raise PoolTimeout("past the deadline") from None

            yield MagicMock()

        return _cm()


class _StubCursor:
    """Records executed statements; usable as an async context manager."""

    def __init__(self) -> None:
        self.executed: list[tuple] = []
        self.rowcount = 3
        self.description = None

    async def execute(self, query, params=None) -> None:
        self.executed.append((query, params))

    async def executemany(self, query, params) -> None:
        self.executed.append((query, tuple(params)))

    async def fetchone(self):
        return None

    async def fetchall(self):
        return []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False


class _StubTxn:
    """``conn.transaction()`` stub recording begin/commit/rollback on the conn."""

    def __init__(self, conn: "_StubConn", savepoint_name: str | None) -> None:
        self.conn = conn
        self.savepoint_name = savepoint_name

    async def __aenter__(self) -> "_StubTxn":
        self.conn.tx_events.append(("begin", self.savepoint_name))
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        self.conn.tx_events.append(
            ("rollback" if exc_type is not None else "commit", self.savepoint_name)
        )
        return False


class _StubConn:
    """Connection stub recording autocommit transitions and commit calls."""

    def __init__(self) -> None:
        self.cursor_obj = _StubCursor()
        self.autocommit_calls: list[bool] = []
        self.commit_calls = 0
        self.autocommit = False
        self.isolation_level = None
        self.read_only = None
        self.fail_autocommit_restore = False
        # ("begin"|"commit"|"rollback", savepoint_name|None) in order.
        self.tx_events: list[tuple[str, str | None]] = []

    def cursor(self, *args, **kwargs) -> _StubCursor:
        return self.cursor_obj

    def transaction(self, *, savepoint_name: str | None = None) -> _StubTxn:
        return _StubTxn(self, savepoint_name)

    async def set_autocommit(self, value: bool) -> None:
        if self.fail_autocommit_restore and value is False:
            raise RuntimeError("connection is broken")

        self.autocommit_calls.append(value)
        self.autocommit = value

    async def set_isolation_level(self, value) -> None:
        self.isolation_level = value

    async def set_read_only(self, value) -> None:
        self.read_only = value

    async def commit(self) -> None:
        self.commit_calls += 1


class _StubPool:
    """Pool stub handing out a single stub connection."""

    def __init__(self, conn: _StubConn) -> None:
        self.conn = conn
        self.checkouts = 0

    def get_stats(self) -> dict[str, int]:
        # The real pool always reports these; the client samples `connections_errors`
        # around a checkout to tell a full pool from an unreachable server.
        return {"connections_errors": 0}

    def connection(self, timeout=None):
        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def _cm():
            self.checkouts += 1
            yield self.conn

        return _cm()


def _client_with_stub_pool() -> tuple[PostgresClient, _StubConn, _StubPool]:
    client = PostgresClient()
    conn = _StubConn()
    pool = _StubPool(conn)
    client._PostgresClient__pool = pool  # type: ignore[attr-defined]
    return client, conn, pool


class TestStatementAutocommitPath:
    """Out-of-tx statements ride an autocommit checkout: exactly one server
    statement (psycopg skips the implicit BEGIN), NO explicit commit, and the
    autocommit flag set-then-restored around the statement."""

    @pytest.mark.asyncio
    async def test_execute_sets_and_restores_autocommit_without_commit(self) -> None:
        client, conn, pool = _client_with_stub_pool()

        await client.execute("INSERT INTO t (v) VALUES (1)")

        assert conn.autocommit_calls == [True, False]
        assert conn.commit_calls == 0
        assert conn.cursor_obj.executed == [("INSERT INTO t (v) VALUES (1)", None)]
        assert pool.checkouts == 1

    @pytest.mark.asyncio
    async def test_execute_returns_rowcount(self) -> None:
        client, conn, _ = _client_with_stub_pool()

        rowcount = await client.execute("UPDATE t SET v = 1", return_rowcount=True)

        assert rowcount == 3
        assert conn.commit_calls == 0

    @pytest.mark.asyncio
    async def test_fetch_one_sets_and_restores_autocommit_without_commit(self) -> None:
        client, conn, _ = _client_with_stub_pool()

        res = await client.fetch_one("SELECT 1", commit=True)

        assert res is None
        assert conn.autocommit_calls == [True, False]
        assert conn.commit_calls == 0

    @pytest.mark.asyncio
    async def test_fetch_all_and_fetch_value_use_autocommit(self) -> None:
        client, conn, _ = _client_with_stub_pool()

        assert await client.fetch_all("SELECT 1", commit=True) == []
        assert await client.fetch_value("SELECT 1", default=7) == 7

        assert conn.autocommit_calls == [True, False, True, False]
        assert conn.commit_calls == 0

    @pytest.mark.asyncio
    async def test_execute_many_uses_autocommit_without_commit(self) -> None:
        client, conn, _ = _client_with_stub_pool()

        await client.execute_many("INSERT INTO t (v) VALUES (%s)", [(1,), (2,)])

        assert conn.autocommit_calls == [True, False]
        assert conn.commit_calls == 0

    @pytest.mark.asyncio
    async def test_autocommit_restored_when_statement_raises(self) -> None:
        client, conn, _ = _client_with_stub_pool()

        async def boom(query, params=None):
            raise RuntimeError("statement failed")

        conn.cursor_obj.execute = boom  # type: ignore[method-assign]

        with pytest.raises(Exception):
            await client.execute("SELECT broken")

        assert conn.autocommit_calls == [True, False]
        assert conn.autocommit is False

    @pytest.mark.asyncio
    async def test_restore_failure_is_swallowed(self) -> None:
        """A broken connection on restore must not mask the statement result;
        the pool reset callback is the second belt."""

        client, conn, _ = _client_with_stub_pool()
        conn.fail_autocommit_restore = True

        res = await client.fetch_value("SELECT 1", default="ok")

        assert res == "ok"
        assert conn.autocommit_calls == [True]  # restore raised, was swallowed
        assert conn.autocommit is True  # cleared later by the pool reset belt

    @pytest.mark.asyncio
    async def test_context_bound_connection_path_is_untouched(self) -> None:
        """In-tx / bound paths never toggle autocommit and never commit: the
        context owner controls the transaction."""

        client, pooled_conn, pool = _client_with_stub_pool()
        bound_conn = _StubConn()

        token = client._PostgresClient__ctx_conn.set(bound_conn)  # type: ignore[attr-defined]

        try:
            await client.execute("INSERT INTO t (v) VALUES (1)")
            await client.fetch_one("SELECT 1")
            await client.fetch_all("SELECT 1")
            await client.fetch_value("SELECT 1")
            await client.execute_many("INSERT INTO t (v) VALUES (%s)", [(1,)])

        finally:
            client._PostgresClient__ctx_conn.reset(token)  # type: ignore[attr-defined]

        assert bound_conn.autocommit_calls == []
        assert bound_conn.commit_calls == 0
        assert len(bound_conn.cursor_obj.executed) == 5

        # The pool was never touched.
        assert pool.checkouts == 0
        assert pooled_conn.autocommit_calls == []


class TestPostgresClientRowHelpers:
    def test_rows_to_dicts_empty_description(self) -> None:
        rows = [(1, "a"), (2, "b")]
        assert PostgresClient._rows_to_dicts(None, rows) == [{}, {}]

    def test_row_to_dict_empty_description(self) -> None:
        assert PostgresClient._row_to_dict(None, (1, "x")) == {}


def _lazy_client_with_stub_pool() -> tuple[PostgresClient, _StubConn, _StubPool]:
    client, conn, pool = _client_with_stub_pool()
    client._PostgresClient__lazy_tx = True  # type: ignore[attr-defined]
    return client, conn, pool


class TestLazyTransaction:
    """``lazy_transaction``: a root scope holds no connection until the first query.

    The scope still counts as a transaction (statements ride its connection, not
    an autocommit checkout), the connection materializes once and is reused, and
    the scope commits on clean exit / rolls back on error — all without touching
    the pool when no statement runs.
    """

    @pytest.mark.asyncio
    async def test_open_scope_without_query_checks_out_nothing(self) -> None:
        client, conn, pool = _lazy_client_with_stub_pool()

        async with client.transaction():
            # Logically in a transaction, but nothing acquired or begun yet.
            assert client.is_in_transaction() is True
            assert pool.checkouts == 0
            assert conn.tx_events == []

        # An empty lazy scope holds and commits nothing.
        assert pool.checkouts == 0
        assert conn.tx_events == []
        assert not client.is_in_transaction()

    @pytest.mark.asyncio
    async def test_first_query_materializes_and_commits(self) -> None:
        client, conn, pool = _lazy_client_with_stub_pool()

        async with client.transaction():
            assert pool.checkouts == 0
            await client.execute("INSERT INTO t (v) VALUES (1)")
            # First statement checked out exactly one connection and opened BEGIN.
            assert pool.checkouts == 1
            assert conn.tx_events == [("begin", None)]
            # Rode the transaction, NOT an autocommit checkout.
            assert conn.autocommit_calls == []
            assert conn.cursor_obj.executed == [("INSERT INTO t (v) VALUES (1)", None)]

        # Clean exit commits the materialized transaction.
        assert conn.tx_events == [("begin", None), ("commit", None)]

    @pytest.mark.asyncio
    async def test_second_query_reuses_connection(self) -> None:
        client, conn, pool = _lazy_client_with_stub_pool()

        async with client.transaction():
            await client.execute("INSERT INTO t (v) VALUES (1)")
            await client.fetch_all("SELECT 1")

        assert pool.checkouts == 1
        assert conn.tx_events == [("begin", None), ("commit", None)]
        assert conn.autocommit_calls == []

    @pytest.mark.asyncio
    async def test_error_after_materialization_rolls_back(self) -> None:
        client, conn, pool = _lazy_client_with_stub_pool()

        with pytest.raises(RuntimeError, match="boom"):
            async with client.transaction():
                await client.execute("INSERT INTO t (v) VALUES (1)")
                raise RuntimeError("boom")

        # The exception must reach conn.transaction().__aexit__ as a rollback,
        # never a commit (the bare-aclose-commits-on-error trap).
        assert conn.tx_events == [("begin", None), ("rollback", None)]

    @pytest.mark.asyncio
    async def test_error_before_materialization_holds_nothing(self) -> None:
        client, conn, pool = _lazy_client_with_stub_pool()

        with pytest.raises(RuntimeError, match="boom"):
            async with client.transaction():
                raise RuntimeError("boom")

        assert pool.checkouts == 0
        assert conn.tx_events == []

    @pytest.mark.asyncio
    async def test_nested_scope_before_first_query_materializes_root(self) -> None:
        client, conn, pool = _lazy_client_with_stub_pool()

        async with client.transaction():
            assert pool.checkouts == 0
            async with client.transaction():
                # Entering the nested scope materialized the root and opened a
                # savepoint on its connection.
                assert pool.checkouts == 1
                await client.execute("INSERT INTO t (v) VALUES (1)")

        kinds = [e[0] for e in conn.tx_events]
        # begin (root) → begin (savepoint) → commit (savepoint) → commit (root)
        assert kinds == ["begin", "begin", "commit", "commit"]
        savepoints = [e[1] for e in conn.tx_events]
        assert savepoints[0] is None and savepoints[1] is not None

    @pytest.mark.asyncio
    async def test_eager_mode_unchanged_checks_out_eagerly(self) -> None:
        client, conn, pool = _client_with_stub_pool()  # lazy OFF

        async with client.transaction():
            # Eager: connection checked out and BEGIN issued on scope entry.
            assert pool.checkouts == 1
            assert conn.tx_events == [("begin", None)]
            assert client.is_in_transaction() is True

        assert conn.tx_events == [("begin", None), ("commit", None)]

    @pytest.mark.asyncio
    async def test_concurrent_first_statements_materialize_once(self) -> None:
        """Concurrent first statements in one lazy scope open a single transaction
        (the pending materialization lock serializes the checkout + BEGIN)."""

        client, conn, pool = _lazy_client_with_stub_pool()

        async with client.transaction():
            await asyncio.gather(
                client.execute("INSERT INTO t (v) VALUES (1)"),
                client.execute("INSERT INTO t (v) VALUES (2)"),
            )

        # One connection checked out, one BEGIN/COMMIT — not one per statement.
        assert pool.checkouts == 1
        assert conn.tx_events == [("begin", None), ("commit", None)]

    @pytest.mark.asyncio
    async def test_first_query_in_child_context_does_not_leak_token(self) -> None:
        """Regression: the first query may materialize the scope in a *different*
        context than the one that opened it — the resilience executor runs the
        operation in a child context. The materialized connection must NOT be
        bound to a context var (its token could not be reset across contexts:
        ``ValueError: Token was created in a different Context``); it rides the
        pending object, so scope exit commits cleanly."""

        client, conn, pool = _lazy_client_with_stub_pool()

        async with client.transaction():
            # create_task copies the context: the materializing query runs in a
            # child context, the scope exit unwinds in this (parent) context.
            await asyncio.create_task(
                client.execute("INSERT INTO t (v) VALUES (1)")
            )
            # The parent context sees the materialized connection (fall-through).
            assert await client.fetch_all("SELECT 1") == []

        # Materialized once, committed on a clean exit — no token reset error.
        assert pool.checkouts == 1
        assert conn.tx_events == [("begin", None), ("commit", None)]


class TestAcquireTranslatesOnlyItsOwnWait:
    """The guard that keeps `_acquire` from claiming a timeout it did not cause."""

    @pytest.mark.asyncio
    async def test_a_timeout_raised_inside_the_body_passes_through(self) -> None:
        """A nested checkout's timeout belongs to that wait, not to this one.

        Without the guard, `_acquire` would catch anything the body raised — including a
        `PoolTimeout` from a *different* acquisition — and reclassify it against counters
        sampled around the wrong window, reporting an unreachable database from a pool that
        had just handed out a connection.
        """

        client, _conn, _pool = _client_with_stub_pool()
        inner = PoolTimeout("a wait that belongs to someone else")

        with pytest.raises(PoolTimeout) as caught:
            async with client._acquire():
                raise inner

        assert caught.value is inner
