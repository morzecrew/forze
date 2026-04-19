"""Unit tests for :class:`PsycopgQueryRenderer` and expression rendering."""

from __future__ import annotations

from typing import Any, cast
from uuid import uuid4

import attrs
import pytest

from forze.application.contracts.query import (
    QueryAnd,
    QueryExpr,
    QueryField,
    QueryOr,
)
from forze.base.errors import CoreError
from forze_postgres.kernel.introspect import PostgresColumnTypes, PostgresType
from forze_postgres.kernel.query.render import PsycopgQueryRenderer

# ----------------------- #


@attrs.define(slots=True, frozen=True)
class _UnknownExpr(QueryExpr):
    """Leaf node not handled by the renderer (for error-path tests)."""


def _t(base: str, *, is_array: bool = False) -> PostgresType:
    return PostgresType(base=base, is_array=is_array, not_null=True)


class TestPsycopgQueryRenderer:
    """Tests for :class:`PsycopgQueryRenderer`."""

    def test_unknown_column_when_types_provided(self) -> None:
        """Missing column in type map raises."""
        types: PostgresColumnTypes = {"id": _t("int4")}
        r = PsycopgQueryRenderer(types=types)

        with pytest.raises(CoreError, match="Unknown column"):
            r.render(QueryField("missing", "$eq", 1))

    def test_query_and_empty_is_true(self) -> None:
        """Empty AND is SQL TRUE with no parameters."""
        r = PsycopgQueryRenderer()
        _sql, params = r.render(QueryAnd(()))
        assert params == []

    def test_query_or_empty_is_false(self) -> None:
        """Empty OR is SQL FALSE with no parameters."""
        r = PsycopgQueryRenderer()
        _sql, params = r.render(QueryOr(()))
        assert params == []

    def test_query_and_single_child_no_extra_grouping(self) -> None:
        """Single AND child is rendered without redundant grouping."""
        r = PsycopgQueryRenderer()
        _sql, params = r.render(QueryAnd((QueryField("a", "$eq", 1),)))
        assert params == [1]

    def test_query_or_single_child(self) -> None:
        """Single OR child is rendered directly."""
        r = PsycopgQueryRenderer()
        _sql, params = r.render(QueryOr((QueryField("b", "$eq", "x"),)))
        assert params == ["x"]

    def test_query_and_multiple_joins_with_and(self) -> None:
        """Multiple AND children produce a grouped conjunction."""
        r = PsycopgQueryRenderer()
        expr = QueryAnd(
            (
                QueryField("a", "$eq", 1),
                QueryField("b", "$eq", 2),
            )
        )
        _sql, params = r.render(expr)
        assert params == [1, 2]

    def test_query_or_multiple_joins_with_or(self) -> None:
        """Multiple OR children produce a grouped disjunction."""
        r = PsycopgQueryRenderer()
        expr = QueryOr(
            (
                QueryField("a", "$eq", 1),
                QueryField("b", "$eq", 2),
            )
        )
        _sql, params = r.render(expr)
        assert params == [1, 2]

    def test_unknown_expression_node_raises(self) -> None:
        """Unsupported :class:`QueryExpr` subtype raises."""
        r = PsycopgQueryRenderer()
        with pytest.raises(CoreError, match="Unknown expression"):
            r.render(_UnknownExpr())

    def test_unknown_operator_raises(self) -> None:
        """Unsupported operator raises."""
        r = PsycopgQueryRenderer()
        bad = QueryField("x", cast(Any, "$not_a_real_op"), 1)
        with pytest.raises(CoreError, match="Unknown operator"):
            r.render(bad)

    def test_null_operators(self) -> None:
        """$null true/false render IS NULL / IS NOT NULL."""
        r = PsycopgQueryRenderer()
        _, p1 = r.render(QueryField("n", "$null", True))
        _, p2 = r.render(QueryField("n", "$null", False))
        assert p1 == []
        assert p2 == []

    def test_empty_operators_on_array_column(self) -> None:
        """$empty uses cardinality for array-typed columns."""
        types: PostgresColumnTypes = {"tags": _t("text", is_array=True)}
        r = PsycopgQueryRenderer(types=types)
        _, p1 = r.render(QueryField("tags", "$empty", True))
        _, p2 = r.render(QueryField("tags", "$empty", False))
        assert p1 == []
        assert p2 == []

    @pytest.mark.parametrize(
        ("op", "value"),
        [
            ("$gt", 1),
            ("$gte", 2),
            ("$lt", 3),
            ("$lte", 4),
        ],
    )
    def test_ordering_ops(self, op: str, value: int) -> None:
        """Ordering operators bind one scalar parameter."""
        r = PsycopgQueryRenderer()
        _sql, params = r.render(QueryField("n", op, value))
        assert params == [value]

    def test_eq_and_neq(self) -> None:
        """Equality operators bind one parameter."""
        _, p1 = PsycopgQueryRenderer().render(QueryField("n", "$eq", 10))
        _, p2 = PsycopgQueryRenderer().render(QueryField("n", "$neq", 20))
        assert p1 == [10]
        assert p2 == [20]

    def test_sql_injection_like_strings_are_bound_not_interpolated(self) -> None:
        """User-supplied text including quotes and SQL keywords is a single parameter."""
        payload = "'; DROP TABLE users; --"
        _, params = PsycopgQueryRenderer().render(QueryField("title", "$eq", payload))
        assert params == [payload]

    def test_in_and_nin(self) -> None:
        """Membership operators bind an array parameter."""
        vals = [1, 2, 3]
        _, p_in = PsycopgQueryRenderer().render(QueryField("n", "$in", vals))
        _, p_nin = PsycopgQueryRenderer().render(QueryField("n", "$nin", vals))
        assert p_in == [vals]
        assert p_nin == [vals]

    def test_set_rel_ops_with_array_column(self) -> None:
        """Set-relation ops bind array parameters when column is an array."""
        types: PostgresColumnTypes = {"tags": _t("text", is_array=True)}
        arg = ["a", "b"]

        for op in ("$superset", "$subset", "$overlaps"):
            _sql, params = PsycopgQueryRenderer(types=types).render(
                QueryField("tags", op, arg)
            )
            assert params == [arg], op

        _sql, p_dis = PsycopgQueryRenderer(types=types).render(
            QueryField("tags", "$disjoint", arg)
        )
        assert p_dis == [arg]

    def test_array_column_eq_normalizes_to_superset(self) -> None:
        """Array columns map ``$eq`` to superset semantics."""
        types: PostgresColumnTypes = {"tags": _t("text", is_array=True)}
        r = PsycopgQueryRenderer(types=types)
        _sql, params = r.render(QueryField("tags", "$eq", ["x", "y"]))
        assert params == [["x", "y"]]

    def test_array_column_eq_scalar_wraps_to_superset(self) -> None:
        """Array ``$eq`` with a scalar operand wraps as a one-element array."""
        types: PostgresColumnTypes = {"tags": _t("text", is_array=True)}
        r = PsycopgQueryRenderer(types=types)
        _sql, params = r.render(QueryField("tags", "$eq", "only"))
        assert params == [["only"]]

    def test_array_column_in_normalizes_to_overlaps(self) -> None:
        """Array columns map ``$in`` to overlaps."""
        types: PostgresColumnTypes = {"tags": _t("text", is_array=True)}
        r = PsycopgQueryRenderer(types=types)
        _sql, params = r.render(QueryField("tags", "$in", ["a", "b"]))
        assert params == [["a", "b"]]

    def test_array_column_nin_normalizes_to_disjoint(self) -> None:
        """Array columns map ``$nin`` to disjoint."""
        types: PostgresColumnTypes = {"tags": _t("text", is_array=True)}
        r = PsycopgQueryRenderer(types=types)
        _sql, params = r.render(QueryField("tags", "$nin", ["z"]))
        assert params == [["z"]]

    def test_typed_scalar_coercion(self) -> None:
        """With a type map, scalar operands are coerced."""
        uid = uuid4()
        types: PostgresColumnTypes = {"id": _t("uuid")}
        r = PsycopgQueryRenderer(types=types)
        _sql, params = r.render(QueryField("id", "$eq", str(uid)))
        assert params == [uid]
