"""Unit tests for Meilisearch filter rendering."""

import pytest

from forze.application.contracts.querying import QueryFilterExpressionParser
from forze.base.exceptions import CoreException
from forze_meilisearch.adapters.search._filter_render import MeilisearchFilterRenderer

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
