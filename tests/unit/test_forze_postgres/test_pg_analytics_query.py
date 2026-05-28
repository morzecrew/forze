"""Tests for Postgres analytics query SQL helpers."""

from __future__ import annotations

from pydantic import BaseModel

from forze_postgres.kernel.sql import (
    apply_limit_offset,
    build_count_sql,
    parameters_from_model,
)


class _Params(BaseModel):
    day: str
    n: int = 1


def test_parameters_from_model() -> None:
    params = _Params(day="2026-01-01", n=2)
    assert parameters_from_model(params) == {"day": "2026-01-01", "n": 2}


def test_apply_limit_offset() -> None:
    sql = apply_limit_offset("SELECT 1", limit=10, offset=5)
    assert sql.endswith("LIMIT 10 OFFSET 5")


def test_build_count_sql_wraps_inner() -> None:
    sql = build_count_sql("SELECT value FROM t WHERE day = %(day)s")
    assert "COUNT(*)" in sql
    assert "forze_analytics_subq" in sql
