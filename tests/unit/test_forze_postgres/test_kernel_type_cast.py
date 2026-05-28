"""Unit tests for :mod:`forze_postgres.kernel.sql.type_cast`."""

import pytest
from psycopg import sql

from forze_postgres.kernel.catalog.introspect import PostgresType
from forze_postgres.kernel.sql.type_cast import (
    assignment_from_values_column,
    cast_sql_for_column_type,
)


def _as_string(composable: sql.Composable) -> str:
    return composable.as_string()


class TestCastSqlForColumnType:
    """Tests for :func:`cast_sql_for_column_type`."""

    @pytest.mark.parametrize(
        ("pg", "expected"),
        [
            (PostgresType(base="int4", is_array=False, not_null=True), "int4"),
            (PostgresType(base="numeric", is_array=False, not_null=False), "numeric"),
            (
                PostgresType(base="numeric(10,2)", is_array=False, not_null=False),
                "numeric(10,2)",
            ),
            (
                PostgresType(base="timestamptz", is_array=False, not_null=False),
                "timestamptz",
            ),
            (PostgresType(base="uuid", is_array=False, not_null=True), "uuid"),
            (PostgresType(base="bool", is_array=False, not_null=True), "boolean"),
            (PostgresType(base="jsonb", is_array=False, not_null=False), "jsonb"),
            (
                PostgresType(base="my_status", is_array=False, not_null=False),
                "my_status",
            ),
        ],
    )
    def test_scalar_cast_targets(self, pg: PostgresType, expected: str) -> None:
        cast = cast_sql_for_column_type(pg)
        assert cast is not None
        assert _as_string(cast) == expected

    @pytest.mark.parametrize(
        "base",
        ["text", "varchar", "char", "citext"],
    )
    def test_text_like_returns_none(self, base: str) -> None:
        pg = PostgresType(base=base, is_array=False, not_null=False)
        assert cast_sql_for_column_type(pg) is None

    def test_array_int4(self) -> None:
        pg = PostgresType(base="int4", is_array=True, not_null=False)
        cast = cast_sql_for_column_type(pg)
        assert cast is not None
        assert _as_string(cast) == "int4[]"


class TestAssignmentFromValuesColumn:
    """Tests for :func:`assignment_from_values_column`."""

    def test_without_cast(self) -> None:
        expr = assignment_from_values_column("name", None)
        assert _as_string(expr) == '"name" = "v"."name"'

    def test_with_numeric_cast(self) -> None:
        pg = PostgresType(base="numeric", is_array=False, not_null=False)
        expr = assignment_from_values_column("amount", pg)
        assert _as_string(expr) == '"amount" = "v"."amount"::numeric'

    def test_with_timestamptz_cast(self) -> None:
        pg = PostgresType(base="timestamptz", is_array=False, not_null=False)
        expr = assignment_from_values_column("seen_at", pg)
        assert _as_string(expr) == '"seen_at" = "v"."seen_at"::timestamptz'
