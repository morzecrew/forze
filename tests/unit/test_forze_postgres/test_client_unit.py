"""Unit tests for :mod:`forze_postgres.kernel.client.client` helpers (no DB I/O)."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from forze.base.exceptions import CoreException

pytest.importorskip("psycopg")

from forze_postgres.kernel.client.client import (
    PostgresClient,
    PostgresConfig,
    PostgresTransactionOptions,
)
from forze_postgres.kernel.client.helpers import (
    isolation_level_sql_fragment,
    set_transaction_sql,
)

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


class TestIsolationLevelSql:
    def test_rejects_unknown_level(self) -> None:
        with pytest.raises(CoreException, match="Unsupported transaction isolation"):
            isolation_level_sql_fragment("phantom")


class TestSetTransactionSql:
    """``SET TRANSACTION`` statement generation for root transaction options."""

    def test_default_options(self) -> None:
        stmt = set_transaction_sql(PostgresTransactionOptions())
        assert stmt.as_string() == "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"

    def test_read_only(self) -> None:
        stmt = set_transaction_sql(PostgresTransactionOptions(read_only=True))
        assert (
            stmt.as_string()
            == "SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY"
        )

    def test_serializable_read_only(self) -> None:
        stmt = set_transaction_sql(
            PostgresTransactionOptions(isolation="serializable", read_only=True),
        )
        assert (
            stmt.as_string() == "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY"
        )

    def test_repeatable_read(self) -> None:
        stmt = set_transaction_sql(
            PostgresTransactionOptions(isolation="repeatable_read"),
        )
        assert stmt.as_string() == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"


class TestApplyTransactionOptions:
    """Options must be scoped via ``SET TRANSACTION``, never connection attributes."""

    @pytest.mark.asyncio
    async def test_emits_set_transaction_without_mutating_connection(self) -> None:
        cursor = AsyncMock()
        conn = MagicMock()
        conn.cursor.return_value.__aenter__ = AsyncMock(return_value=cursor)
        conn.cursor.return_value.__aexit__ = AsyncMock(return_value=False)

        options = PostgresTransactionOptions(isolation="serializable", read_only=True)
        await PostgresClient._apply_transaction_options(conn, options)

        cursor.execute.assert_awaited_once()
        (stmt,) = cursor.execute.await_args.args
        assert (
            stmt.as_string() == "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY"
        )

        # Connection attributes must stay untouched — they persist across pool
        # check-ins and previously leaked READ ONLY / isolation to later work.
        conn.set_read_only.assert_not_called()
        conn.set_isolation_level.assert_not_called()


class TestPostgresClientRowHelpers:
    def test_rows_to_dicts_empty_description(self) -> None:
        rows = [(1, "a"), (2, "b")]
        assert PostgresClient._rows_to_dicts(None, rows) == [{}, {}]

    def test_row_to_dict_empty_description(self) -> None:
        assert PostgresClient._row_to_dict(None, (1, "x")) == {}
