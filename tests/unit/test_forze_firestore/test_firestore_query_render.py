"""Unit tests for :class:`~forze_firestore.kernel.query.render.FirestoreQueryRenderer`."""

from __future__ import annotations

import attrs
import pytest
from google.cloud.firestore_v1.base_query import And, FieldFilter, Or

from forze.application.contracts.querying import (
    UNSUPPORTED_QUERY_FEATURE_CODE,
    QueryAnd,
    QueryCompare,
    QueryElem,
    QueryExpr,
    QueryField,
    QueryFilterExpressionParser,
    QueryOr,
)
from forze.base.exceptions import CoreException, ExceptionKind
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

    def test_filter_values_coerced_like_writes(self) -> None:
        """UUID and Decimal filter values match the representation writes persist."""

        from decimal import Decimal
        from uuid import UUID

        r = FirestoreQueryRenderer()
        uid = UUID("00000000-0000-0000-0000-000000000001")

        out = r.render(QueryField("owner", "$eq", uid))
        assert isinstance(out, FieldFilter)
        assert out.value == str(uid)

        out = r.render(QueryField("price", "$gt", Decimal("10.5")))
        assert isinstance(out, FieldFilter)
        assert out.value == 10.5

        out = r.render(QueryField("price", "$in", [Decimal("1.5"), uid]))
        assert isinstance(out, FieldFilter)
        assert out.value == [1.5, str(uid)]

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
        # supports_aggregates=False -> a clean fail-closed precondition naming the backend,
        # not the opaque render-time ``internal`` the MVP raised before.
        r = FirestoreQueryRenderer()
        with pytest.raises(CoreException, match="aggregates") as ei:
            r.render_aggregates({"$computed": {"orders": {"$count": None}}})

        assert ei.value.kind is ExceptionKind.PRECONDITION
        assert ei.value.code == UNSUPPORTED_QUERY_FEATURE_CODE
        assert "firestore" in str(ei.value)

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

    def test_in_and_empty_render(self) -> None:
        r = FirestoreQueryRenderer()
        assert isinstance(r.render(QueryField("a", "$in", [1, 2])), FieldFilter)
        assert isinstance(r.render(QueryField("a", "$empty", False)), FieldFilter)

    @pytest.mark.parametrize(
        ("op", "value"),
        [("$neq", 1), ("$nin", ["x"]), ("$null", True)],
    )
    def test_null_absent_diverging_ops_are_unsupported(
        self, op: str, value: object
    ) -> None:
        # $neq / $nin / $null are not advertised (Firestore !=/not-in exclude
        # absent/null fields, diverging from the agnostic semantics). render()
        # fails closed via the capability validator with the clean unsupported code.
        r = FirestoreQueryRenderer()
        with pytest.raises(CoreException) as ei:
            r.render(QueryField("a", op, value))

        assert ei.value.code == UNSUPPORTED_QUERY_FEATURE_CODE

    @pytest.mark.parametrize("op", ["$neq", "$nin", "$null"])
    def test_diverging_ops_render_backstop_raises(self, op: str) -> None:
        # Direct _render_field call (bypassing the validator) still refuses:
        # defense-in-depth backstop.
        r = FirestoreQueryRenderer()
        value: object = ["x"] if op == "$nin" else (True if op == "$null" else 1)
        with pytest.raises(CoreException, match="does not support"):
            r._render_field("a", op, value)  # type: ignore[arg-type]

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
