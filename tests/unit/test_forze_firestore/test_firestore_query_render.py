"""Unit tests for :class:`~forze_firestore.kernel.query.render.FirestoreQueryRenderer`."""

from __future__ import annotations

import attrs
import pytest

from forze.base.exceptions import CoreException
from google.cloud.firestore_v1.base_query import And, FieldFilter, Or

from forze.application.contracts.querying import (
    QueryAnd,
    QueryCompare,
    QueryElem,
    QueryExpr,
    QueryField,
    QueryFilterExpressionParser,
    QueryOr,
)
from forze_firestore.kernel.query.render import FirestoreQueryRenderer


@attrs.define(slots=True, frozen=True)
class _UnknownExpr(QueryExpr):
    pass


class TestFirestoreQueryRenderer:
    def test_query_and_empty(self) -> None:
        r = FirestoreQueryRenderer()
        assert r.render(QueryAnd(())) is None

    def test_query_and_multiple(self) -> None:
        r = FirestoreQueryRenderer()
        expr = QueryAnd((QueryField("a", "$eq", 1), QueryField("b", "$eq", 2)))
        out = r.render(expr)
        assert isinstance(out, And)

    def test_query_or_multiple(self) -> None:
        r = FirestoreQueryRenderer()
        expr = QueryOr((QueryField("a", "$eq", 1), QueryField("b", "$eq", 2)))
        out = r.render(expr)
        assert isinstance(out, Or)

    def test_field_eq(self) -> None:
        r = FirestoreQueryRenderer()
        out = r.render(QueryField("status", "$eq", "active"))
        assert isinstance(out, FieldFilter)
        assert out.field_path == "status"

    def test_compare_raises(self) -> None:
        r = FirestoreQueryRenderer()
        with pytest.raises(CoreException, match="field-to-field"):
            r.render(QueryCompare("a", "$eq", "b"))

    def test_not_raises(self) -> None:
        expr = QueryFilterExpressionParser.parse(
            {"$not": {"$values": {"status": "archived"}}},
        )
        r = FirestoreQueryRenderer()
        with pytest.raises(CoreException, match="\\$not"):
            r.render(expr)

    def test_element_quantifier_raises(self) -> None:
        expr = QueryFilterExpressionParser.parse(
            {"$values": {"tags": {"$any": "urgent"}}},
        )
        r = FirestoreQueryRenderer()
        with pytest.raises(CoreException, match="element quantifier"):
            r.render(expr)

    def test_elem_node_raises(self) -> None:
        r = FirestoreQueryRenderer()
        with pytest.raises(CoreException, match="element quantifier"):
            r.render(QueryElem("tags", "$any", QueryField("x", "$eq", 1)))

    def test_aggregates_raises(self) -> None:
        r = FirestoreQueryRenderer()
        with pytest.raises(CoreException, match="aggregates"):
            r.render_aggregates({"$computed": {"orders": {"$count": None}}})

    def test_unknown_expression_raises(self) -> None:
        r = FirestoreQueryRenderer()
        with pytest.raises(CoreException, match="Unknown expression"):
            r.render(_UnknownExpr())

    def test_ilike_raises(self) -> None:
        r = FirestoreQueryRenderer()
        with pytest.raises(CoreException, match=r"\$ilike"):
            r.render(QueryField("title", "$ilike", "%x%"))

    def test_query_or_empty_raises(self) -> None:
        r = FirestoreQueryRenderer()
        with pytest.raises(CoreException, match="Empty \\$or"):
            r.render(QueryOr(()))

    def test_neq_in_nin_null_empty(self) -> None:
        r = FirestoreQueryRenderer()
        assert isinstance(r.render(QueryField("a", "$neq", 1)), FieldFilter)
        assert isinstance(r.render(QueryField("a", "$in", [1, 2])), FieldFilter)
        assert isinstance(r.render(QueryField("a", "$nin", ["x"])), FieldFilter)
        assert isinstance(r.render(QueryField("a", "$null", True)), FieldFilter)
        assert isinstance(r.render(QueryField("a", "$empty", False)), FieldFilter)

    def test_comparison_ops(self) -> None:
        r = FirestoreQueryRenderer()
        assert isinstance(r.render(QueryField("n", "$gt", 1)), FieldFilter)
        assert isinstance(r.render(QueryField("n", "$gte", 1)), FieldFilter)

    def test_in_scalar_raises(self) -> None:
        r = FirestoreQueryRenderer()
        with pytest.raises(CoreException, match="expects list"):
            r.render(QueryField("a", "$in", 1))

    def test_set_ops_raise(self) -> None:
        r = FirestoreQueryRenderer()
        with pytest.raises(CoreException, match=r"\$overlaps"):
            r.render(QueryField("tags", "$overlaps", ["a"]))
