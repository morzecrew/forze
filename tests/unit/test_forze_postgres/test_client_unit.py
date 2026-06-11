"""Unit tests for :mod:`forze_postgres.kernel.client.client` helpers (no DB I/O)."""

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

        await _pool_reset_transaction_attributes(conn)

        conn.set_isolation_level.assert_awaited_once_with(None)
        conn.set_read_only.assert_awaited_once_with(None)

    @pytest.mark.asyncio
    async def test_noop_when_attributes_are_default(self) -> None:
        """Common check-in path (nothing leaked) must not even call the setters."""

        conn = _mock_conn()
        conn.isolation_level = None
        conn.read_only = None

        await _pool_reset_transaction_attributes(conn)

        conn.set_isolation_level.assert_not_called()
        conn.set_read_only.assert_not_called()


class TestPostgresClientRowHelpers:
    def test_rows_to_dicts_empty_description(self) -> None:
        rows = [(1, "a"), (2, "b")]
        assert PostgresClient._rows_to_dicts(None, rows) == [{}, {}]

    def test_row_to_dict_empty_description(self) -> None:
        assert PostgresClient._row_to_dict(None, (1, "x")) == {}
