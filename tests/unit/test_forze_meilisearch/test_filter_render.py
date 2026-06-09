"""Unit tests for Meilisearch filter rendering."""

from datetime import UTC, date, datetime
from uuid import UUID

import pytest

from forze.application.contracts.querying import QueryFilterExpressionParser
from forze.base.exceptions import CoreException
from forze_meilisearch.adapters.search._filter_render import (
    MeilisearchFilterRenderer,
    _format_array,
    format_literal,
    safe_attribute,
)

# ----------------------- #


def test_eq_filter() -> None:
    r = MeilisearchFilterRenderer()
    expr = QueryFilterExpressionParser.parse(
        {"$values": {"status": {"$eq": "open"}}},
    )
    assert r._render_expr(expr) == 'status = "open"'


def test_and_filter() -> None:
    r = MeilisearchFilterRenderer()
    expr = QueryFilterExpressionParser.parse(
        {
            "$and": [
                {"$values": {"status": {"$eq": "open"}}},
                {"$values": {"count": {"$gt": 1}}},
            ]
        }
    )
    out = r._render_expr(expr)
    assert "AND" in out
    assert 'status = "open"' in out


def test_like_unsupported() -> None:
    r = MeilisearchFilterRenderer()
    expr = QueryFilterExpressionParser.parse(
        {"$values": {"title": {"$like": "%foo%"}}},
    )

    with pytest.raises(CoreException):
        r._render_expr(expr)


def test_safe_attribute_accepts_identifiers_and_nested_paths() -> None:
    assert safe_attribute("status") == "status"
    assert safe_attribute("meta.kind") == "meta.kind"


def test_safe_attribute_rejects_injection_payloads() -> None:
    for bad in ("id = 1 OR _geoRadius(0,0,9e9)", 'x" OR "1', "a b", "x;y", "(a)"):
        with pytest.raises(CoreException):
            safe_attribute(bad)


def test_filter_rejects_injected_field_name() -> None:
    # A user-controlled filter key cannot inject filter-expression fragments.
    r = MeilisearchFilterRenderer()
    expr = QueryFilterExpressionParser.parse(
        {"$values": {"id = 1 OR _geoRadius(0,0,9e9)": {"$eq": "x"}}},
    )

    with pytest.raises(CoreException):
        r._render_expr(expr)


def test_format_literals() -> None:
    assert format_literal(None) == "NULL"
    assert format_literal(True) == "true"
    assert format_literal(False) == "false"
    assert format_literal(3) == "3"
    uid = UUID("00000000-0000-0000-0000-000000000001")
    assert format_literal(uid) == '"00000000-0000-0000-0000-000000000001"'
    assert format_literal('say "hi"') == r'"say \"hi\""'
    assert format_literal(datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)) == '"2024-01-02T03:04:05+00:00"'
    assert format_literal(date(2024, 1, 2)) == '"2024-01-02"'


def test_format_array_requires_sequence() -> None:
    with pytest.raises(CoreException):
        _format_array("nope")


def test_or_not_and_comparison_operators() -> None:
    r = MeilisearchFilterRenderer(field_map={"status": "cat"})
    expr = QueryFilterExpressionParser.parse(
        {
            "$or": [
                {"$values": {"status": {"$eq": "food"}}},
                {"$not": {"$values": {"count": {"$lt": 0}}}},
            ],
        },
    )
    out = r._render_expr(expr)
    assert " OR " in out
    assert "NOT" in out
    assert "cat = " in out


def test_in_nin_null_operators() -> None:
    r = MeilisearchFilterRenderer()
    expr = QueryFilterExpressionParser.parse(
        {
            "$and": [
                {"$values": {"tag": {"$in": ["a", "b"]}}},
                {"$values": {"tag": {"$nin": ["c"]}}},
                {"$values": {"deleted": {"$null": True}}},
                {"$values": {"active": {"$null": False}}},
            ],
        },
    )
    out = r._render_expr(expr)
    assert " IN [" in out
    assert " NOT IN [" in out
    assert "IS NULL" in out
    assert "IS NOT NULL" in out


def test_render_filters_returns_none_for_empty_tree() -> None:
    r = MeilisearchFilterRenderer()
    assert r.render_filters(None) is None


def test_physical_field_map() -> None:
    r = MeilisearchFilterRenderer(field_map={"logical": "indexed"})
    expr = QueryFilterExpressionParser.parse(
        {"$values": {"logical": {"$eq": "x"}}},
    )
    assert r._render_expr(expr) == 'indexed = "x"'
