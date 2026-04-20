"""Unit tests for :mod:`forze_postgres.kernel.platform.client` helpers (no DB I/O)."""

import pytest

pytest.importorskip("psycopg")

from forze.base.errors import CoreError
from forze_postgres.kernel.platform.client import PostgresClient, PostgresConfig


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


class TestPostgresClientRowHelpers:
    def test_rows_to_dicts_empty_description(self) -> None:
        rows = [(1, "a"), (2, "b")]
        assert PostgresClient._rows_to_dicts(None, rows) == [{}, {}]

    def test_row_to_dict_empty_description(self) -> None:
        assert PostgresClient._row_to_dict(None, (1, "x")) == {}
