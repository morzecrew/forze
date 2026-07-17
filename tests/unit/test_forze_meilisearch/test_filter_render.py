"""Unit tests for Meilisearch filter rendering."""

from datetime import UTC, date, datetime
from uuid import UUID

import pytest

from forze.application.contracts.querying import (
    UNSUPPORTED_QUERY_FEATURE_CODE,
    QueryFilterExpressionParser,
)
from forze.base.exceptions import CoreException, ExceptionKind
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


class TestCapabilityRejectionViaRenderFilters:
    """The public entry rejects unsupported features up front with a clean error.

    ``render_filters`` validates against ``MEILISEARCH_QUERY_CAPABILITIES`` before
    rendering, so the caller gets a ``precondition`` (``query_feature_unsupported``)
    naming the backend — not the render-time ``internal`` of the inner backstop.
    """

    @pytest.mark.parametrize(
        "filters",
        [
            {"$values": {"title": {"$regex": "a.*"}}},  # text op
            {"$values": {"tags": {"$superset": ["a"]}}},  # set op
            {"$values": {"tags": {"$any": "x"}}},  # element quantifier
            {"$fields": {"a": {"$eq": "b"}}},  # field-to-field compare
        ],
    )
    def test_unsupported_features_rejected_clean(self, filters: dict) -> None:
        r = MeilisearchFilterRenderer()

        with pytest.raises(CoreException) as ei:
            r.render_filters(filters)

        assert ei.value.kind is ExceptionKind.PRECONDITION
        assert ei.value.code == UNSUPPORTED_QUERY_FEATURE_CODE
        assert "meilisearch" in str(ei.value)

    def test_supported_filter_still_renders(self) -> None:
        r = MeilisearchFilterRenderer()

        out = r.render_filters(
            {"$and": [{"$values": {"age": {"$gte": 18}}}, {"$values": {"city": "NYC"}}]}
        )

        assert out is not None and "AND" in out


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
    assert format_literal(date(2024, 1, 2)) == '"2024-01-02"'


def test_format_literal_datetime_matches_indexed_representation() -> None:
    """The literal must equal what a json-mode dump indexed: UTC renders as ``Z``,
    other offsets normalize to the same UTC instant, naive stays offset-free."""

    from datetime import timedelta, timezone

    assert format_literal(datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)) == '"2024-01-02T03:04:05Z"'
    assert format_literal(datetime(2024, 1, 2, 3, 4, 5)) == '"2024-01-02T03:04:05"'
    assert (
        format_literal(datetime(2024, 1, 2, 6, 4, 5, tzinfo=timezone(timedelta(hours=3))))
        == '"2024-01-02T03:04:05Z"'
    )


def test_format_literal_enum_renders_value() -> None:
    from enum import Enum

    class _Color(str, Enum):
        red = "red"

    assert format_literal(_Color.red) == '"red"'


def test_format_literal_decimal_renders_bare_number() -> None:
    from decimal import Decimal

    assert format_literal(Decimal("10.50")) == "10.5"
    assert format_literal(Decimal("0")) == "0.0"
    # repr() would produce exponent notation for huge magnitudes; the literal expands it.
    assert format_literal(Decimal("1E+25")) == "10000000000000000000000000"

    with pytest.raises(CoreException, match="Non-finite"):
        format_literal(Decimal("NaN"))

    with pytest.raises(CoreException, match="Non-finite"):
        format_literal(Decimal("Infinity"))


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
