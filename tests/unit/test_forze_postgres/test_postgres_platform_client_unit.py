"""Unit tests for :mod:`forze_postgres.kernel.platform.client` helpers (no DB I/O)."""

from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("psycopg")

from forze.base.errors import CoreError
from forze_postgres.kernel.platform.client import (
    PostgresClient,
    PostgresConfig,
)
from forze_postgres.kernel.platform.helpers import isolation_level_sql_fragment

# ----------------------- #


class TestPostgresConfig:
    def test_rejects_min_greater_than_max(self) -> None:
        with pytest.raises(CoreError, match="Minimum size must be less"):
            PostgresConfig(min_size=5, max_size=3)

    def test_rejects_negative_min_size(self) -> None:
        with pytest.raises(CoreError, match="Minimum size must be greater"):
            PostgresConfig(min_size=-1)

    def test_rejects_negative_num_workers(self) -> None:
        with pytest.raises(CoreError, match="workers must be greater"):
            PostgresConfig(num_workers=-1)

    def test_rejects_negative_pool_headroom(self) -> None:
        with pytest.raises(CoreError, match="pool_headroom"):
            PostgresConfig(pool_headroom=-1)

    def test_rejects_max_concurrent_queries_below_one(self) -> None:
        with pytest.raises(CoreError, match="max_concurrent_queries"):
            PostgresConfig(max_concurrent_queries=0)

    def test_warns_on_large_min_and_max_pool_size(self) -> None:
        mock_logger = MagicMock()

        with patch(
            "forze_postgres.kernel.platform.value_objects.logger",
            mock_logger,
        ):
            PostgresConfig(min_size=11, max_size=101)

        assert mock_logger.warning.call_count == 2
        joined = " ".join(str(c) for c in mock_logger.warning.call_args_list)
        assert "Minimum size is greater than 10" in joined
        assert "Maximum size is greater than 100" in joined


class TestIsolationLevelSql:
    def test_rejects_unknown_level(self) -> None:
        with pytest.raises(CoreError, match="Unsupported transaction isolation"):
            isolation_level_sql_fragment("phantom")


class TestPostgresClientRowHelpers:
    def test_rows_to_dicts_empty_description(self) -> None:
        rows = [(1, "a"), (2, "b")]
        assert PostgresClient._rows_to_dicts(None, rows) == [{}, {}]

    def test_row_to_dict_empty_description(self) -> None:
        assert PostgresClient._row_to_dict(None, (1, "x")) == {}
