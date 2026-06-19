"""Unit tests for :mod:`forze_postgres.kernel.client.client` helpers (no DB I/O)."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from forze.base.exceptions import CoreException

pytest.importorskip("psycopg")

from psycopg import IsolationLevel

from forze_postgres.kernel.client.client import (
    PostgresClient,
    PostgresConfig,
    PostgresTransactionOptions,
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
