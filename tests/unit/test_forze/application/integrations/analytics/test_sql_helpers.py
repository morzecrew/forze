"""Tests for the shared analytics SQL builders."""

import pytest
from pydantic import BaseModel

from forze.application.integrations.analytics.sql import (
    apply_limit_offset,
    build_count_sql,
    parameters_from_model,
)
from forze.base.exceptions import CoreException

# ----------------------- #


class _Params(BaseModel):
    day: str
    n: int = 1


# ....................... #


def test_parameters_from_model() -> None:
    params = _Params(day="2026-01-01", n=2)
    assert parameters_from_model(params) == {"day": "2026-01-01", "n": 2}


# ....................... #


def test_build_count_sql_wraps_inner() -> None:
    sql = build_count_sql("SELECT value FROM t WHERE day = %(day)s;")
    assert sql == (
        "SELECT COUNT(*) AS forze_cnt "
        "FROM (SELECT value FROM t WHERE day = %(day)s) AS forze_analytics_subq"
    )


def test_build_count_sql_custom_count_expr() -> None:
    sql = build_count_sql("SELECT 1", count_expr="count()")
    assert sql == "SELECT count() AS forze_cnt FROM (SELECT 1) AS forze_analytics_subq"


# ....................... #


def test_apply_limit_offset_noop_without_window() -> None:
    assert apply_limit_offset("SELECT 1; ") == "SELECT 1"


def test_apply_limit_offset_wraps_in_subquery() -> None:
    sql = apply_limit_offset("SELECT 1", limit=10, offset=5)
    assert sql == "SELECT * FROM (SELECT 1) AS forze_page_subq LIMIT 10 OFFSET 5"


def test_apply_limit_offset_limit_only() -> None:
    sql = apply_limit_offset("SELECT 1", limit=10)
    assert sql == "SELECT * FROM (SELECT 1) AS forze_page_subq LIMIT 10"


def test_apply_limit_offset_offset_only() -> None:
    sql = apply_limit_offset("SELECT 1", offset=5)
    assert sql == "SELECT * FROM (SELECT 1) AS forze_page_subq OFFSET 5"


def test_apply_limit_offset_tolerates_inner_limit() -> None:
    # wrapping (not appending) keeps queries with their own LIMIT valid
    sql = apply_limit_offset("SELECT 1 LIMIT 3;", limit=10)
    assert sql == "SELECT * FROM (SELECT 1 LIMIT 3) AS forze_page_subq LIMIT 10"


def test_apply_limit_offset_preserves_inner_order_by() -> None:
    sql = apply_limit_offset("SELECT v FROM t ORDER BY v DESC", limit=2)
    assert sql == (
        "SELECT * FROM (SELECT v FROM t ORDER BY v DESC) AS forze_page_subq LIMIT 2"
    )


@pytest.mark.parametrize("window", [{"limit": -1}, {"offset": -1}])
def test_apply_limit_offset_rejects_negative(window: dict[str, int]) -> None:
    with pytest.raises(CoreException):
        apply_limit_offset("SELECT 1", **window)
