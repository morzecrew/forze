"""Unit tests for :class:`PsycopgQueryRenderer` and expression rendering."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, cast
from uuid import UUID, uuid4

import attrs
import pytest
from psycopg import sql as psql
from pydantic import BaseModel

from forze.application.contracts.querying import (
    ELEM_SCALAR_FIELD,
    QueryAnd,
    QueryCompare,
    QueryElem,
    QueryExpr,
    QueryField,
    QueryFilterExpressionParser,
    QueryNot,
    QueryOr,
    QueryValue,
)
from forze.base.exceptions import CoreException
from forze_postgres.kernel.catalog.introspect import PostgresColumnTypes, PostgresType
from forze_postgres.kernel.sql.query.render import (
    PsycopgQueryRenderer,
    PsycopgValueCoercer,
)

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

        with pytest.raises(CoreException, match="Unknown column"):
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
        with pytest.raises(CoreException, match="Unknown expression"):
            r.render(_UnknownExpr())

    def test_compare_renders_without_parameters(self) -> None:
        """Field-to-field compare uses column expressions only."""
        r = PsycopgQueryRenderer()
        _sql, params = r.render(QueryCompare("starts_at", "$lte", "ends_at"))
        assert params == []
        assert b"starts_at" in _sql.as_bytes()
        assert b"ends_at" in _sql.as_bytes()

    def test_compare_typed_columns(self) -> None:
        types: PostgresColumnTypes = {
            "a": _t("int4"),
            "b": _t("int4"),
        }
        r = PsycopgQueryRenderer(types=types)
        _sql, params = r.render(QueryCompare("a", "$eq", "b"))
        assert params == []

    def test_compare_nested_json_paths(self) -> None:
        class _Inner(BaseModel):
            score: int
            min_score: int = 0

        class _Outer(BaseModel):
            meta: _Inner

        types: PostgresColumnTypes = {"meta": _t("jsonb")}
        r = PsycopgQueryRenderer(types=types, model_type=_Outer)
        _sql, params = r.render(QueryCompare("meta.score", "$gte", "meta.min_score"))
        assert params == []
        assert b"meta" in _sql.as_bytes()

    def test_compare_array_column_raises(self) -> None:
        types: PostgresColumnTypes = {
            "a": _t("text", is_array=True),
            "b": _t("text", is_array=True),
        }
        r = PsycopgQueryRenderer(types=types)
        with pytest.raises(CoreException, match="array columns"):
            r.render(QueryCompare("a", "$eq", "b"))

    def test_compare_incompatible_types_raises(self) -> None:
        types: PostgresColumnTypes = {
            "a": _t("uuid"),
            "b": _t("text"),
        }
        r = PsycopgQueryRenderer(types=types)
        with pytest.raises(CoreException, match="Incompatible types"):
            r.render(QueryCompare("a", "$eq", "b"))

    def test_unknown_operator_raises(self) -> None:
        """Unsupported operator raises."""
        # The capability validator rejects unknown ops at render(); this exercises
        # the renderer's own defense-in-depth backstop directly.
        r = PsycopgQueryRenderer()
        bad = QueryField("x", cast(Any, "$not_a_real_op"), 1)
        with pytest.raises(CoreException, match="Unknown operator"):
            r._render_expr(bad)

    def test_null_operators(self) -> None:
        """$null true/false render IS NULL / IS NOT NULL."""
        r = PsycopgQueryRenderer()
        _, p1 = r.render(QueryField("n", "$null", True))
        _, p2 = r.render(QueryField("n", "$null", False))
        assert p1 == []
        assert p2 == []

    def test_empty_operators_on_array_column(self) -> None:
        """$empty uses cardinality for native array-typed columns."""
        types: PostgresColumnTypes = {"tags": _t("text", is_array=True)}
        r = PsycopgQueryRenderer(types=types)
        sql_true, p1 = r.render(QueryField("tags", "$empty", True))
        sql_false, p2 = r.render(QueryField("tags", "$empty", False))
        assert p1 == []
        assert p2 == []
        assert b"cardinality" in sql_true.as_bytes()
        assert b"cardinality" in sql_false.as_bytes()

    def test_empty_operators_on_jsonb_column(self) -> None:
        """$empty on jsonb uses jsonb_typeof / jsonb_array_length, not cardinality."""
        types: PostgresColumnTypes = {"characteristics": _t("jsonb")}
        r = PsycopgQueryRenderer(types=types)
        sql_true, p1 = r.render(QueryField("characteristics", "$empty", True))
        sql_false, p2 = r.render(QueryField("characteristics", "$empty", False))
        assert p1 == []
        assert p2 == []
        b_true = sql_true.as_bytes()
        b_false = sql_false.as_bytes()
        assert b"jsonb_typeof" in b_true and b"jsonb_array_length" in b_true
        assert b"cardinality" not in b_true
        assert b"jsonb_typeof" in b_false and b"jsonb_array_length" in b_false
        assert b"cardinality" not in b_false

    def test_empty_operators_on_json_column(self) -> None:
        """$empty on json column casts to jsonb for length checks."""
        types: PostgresColumnTypes = {"payload": _t("json")}
        r = PsycopgQueryRenderer(types=types)
        sql_false, _ = r.render(QueryField("payload", "$empty", False))
        assert b"::jsonb" in sql_false.as_bytes()
        assert b"jsonb_array_length" in sql_false.as_bytes()

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

    def test_array_column_eq_uses_exact_array_equality(self) -> None:
        """Native array columns compare with ``=`` against a bound array value."""
        types: PostgresColumnTypes = {"tags": _t("text", is_array=True)}
        r = PsycopgQueryRenderer(types=types)
        sql_out, params = r.render(QueryField("tags", "$eq", ["x", "y"]))
        assert params == [["x", "y"]]
        b = sql_out.as_bytes()
        assert b"=" in b and b"@>" not in b

    def test_array_column_eq_scalar_raises(self) -> None:
        """``$eq`` on an array column requires a list/tuple RHS."""
        types: PostgresColumnTypes = {"tags": _t("text", is_array=True)}
        r = PsycopgQueryRenderer(types=types)
        with pytest.raises(CoreException, match="list/tuple"):
            r.render(QueryField("tags", "$eq", "only"))

    def test_array_column_in_uses_element_membership(self) -> None:
        """``$in`` on array columns matches any unnested element against the list."""
        types: PostgresColumnTypes = {"tags": _t("text", is_array=True)}
        r = PsycopgQueryRenderer(types=types)
        sql_out, params = r.render(QueryField("tags", "$in", ["a", "b"]))
        assert params == [["a", "b"]]
        assert b"unnest" in sql_out.as_bytes() and b"&&" not in sql_out.as_bytes()

    def test_array_column_nin_negates_element_membership(self) -> None:
        """``$nin`` on array columns negates the element-wise membership predicate."""
        types: PostgresColumnTypes = {"tags": _t("text", is_array=True)}
        r = PsycopgQueryRenderer(types=types)
        sql_out, params = r.render(QueryField("tags", "$nin", ["z"]))
        assert params == [["z"]]
        b = sql_out.as_bytes()
        assert b"unnest" in b and b"NOT" in b and b"&&" not in b

    def test_query_not_renders_sql_negation(self) -> None:
        expr = QueryFilterExpressionParser.parse(
            {"$not": {"$values": {"status": "archived"}}},
        )
        r = PsycopgQueryRenderer()
        sql_out, params = r.render(expr)
        assert b"NOT" in sql_out.as_bytes()
        assert params == ["archived"]

    def test_element_any_scalar_native_array(self) -> None:
        types: PostgresColumnTypes = {"tags": _t("text", is_array=True)}
        expr = QueryFilterExpressionParser.parse(
            {"$values": {"tags": {"$any": "urgent"}}},
        )
        r = PsycopgQueryRenderer(types=types)
        sql_out, params = r.render(expr)
        b = sql_out.as_bytes()
        assert b"unnest" in b and b"EXISTS" in b
        assert params == ["urgent"]

    def test_element_all_vacuous_on_empty_array_column(self) -> None:
        types: PostgresColumnTypes = {"tags": _t("text", is_array=True)}
        expr = QueryElem(
            "tags",
            "$all",
            QueryField(ELEM_SCALAR_FIELD, "$eq", "x"),
        )
        r = PsycopgQueryRenderer(types=types)
        sql_out, _params = r.render(expr)
        assert b"TRUE" in sql_out.as_bytes()

    def test_element_any_object_jsonb(self) -> None:
        class _Item(BaseModel):
            status: str

        class _Row(BaseModel):
            items: list[_Item]

        types: PostgresColumnTypes = {"items": _t("jsonb")}
        expr = QueryFilterExpressionParser.parse(
            {
                "$values": {
                    "items": {
                        "$any": {"$values": {"status": "open"}},
                    },
                },
            },
        )
        r = PsycopgQueryRenderer(types=types, model_type=_Row)
        sql_out, params = r.render(expr)
        b = sql_out.as_bytes()
        assert b"jsonb_array_elements" in b
        assert params == ["open"]

    def test_element_none_scalar_native_array(self) -> None:
        types: PostgresColumnTypes = {"tags": _t("text", is_array=True)}
        expr = QueryFilterExpressionParser.parse(
            {"$values": {"tags": {"$none": "urgent"}}},
        )
        r = PsycopgQueryRenderer(types=types)
        sql_out, params = r.render(expr)
        b = sql_out.as_bytes()
        assert b"unnest" in b
        assert b"NOT (EXISTS" in b
        assert params == ["urgent"]

    def test_element_all_scalar_forall_sql(self) -> None:
        types: PostgresColumnTypes = {"tags": _t("text", is_array=True)}
        expr = QueryFilterExpressionParser.parse(
            {"$values": {"tags": {"$all": {"$eq": "ops"}}}},
        )
        r = PsycopgQueryRenderer(types=types)
        sql_out, _params = r.render(expr)
        b = sql_out.as_bytes()
        assert b"NOT EXISTS" in b
        assert b"unnest" in b

    def test_element_any_scalar_gte(self) -> None:
        types: PostgresColumnTypes = {"scores": _t("int4", is_array=True)}
        expr = QueryFilterExpressionParser.parse(
            {"$values": {"scores": {"$any": {"$gte": 10}}}},
        )
        r = PsycopgQueryRenderer(types=types)
        sql_out, params = r.render(expr)
        assert b">=" in sql_out.as_bytes()
        assert params == [10]

    def test_not_nested_or(self) -> None:
        expr = QueryFilterExpressionParser.parse(
            {
                "$not": {
                    "$or": [
                        {"$values": {"status": "archived"}},
                        {"$values": {"status": "pending"}},
                    ],
                },
            },
        )
        r = PsycopgQueryRenderer()
        sql_out, params = r.render(expr)
        b = sql_out.as_bytes()
        assert b"NOT" in b
        assert b" OR " in b
        assert set(params) == {"archived", "pending"}

    def test_query_not_with_query_and_child(self) -> None:
        expr = QueryNot(
            QueryAnd(
                (
                    QueryField("a", "$eq", 1),
                    QueryField("b", "$eq", 2),
                ),
            ),
        )
        r = PsycopgQueryRenderer()
        sql_out, params = r.render(expr)
        assert b"NOT" in sql_out.as_bytes()
        assert params == [1, 2]

    def test_typed_scalar_coercion(self) -> None:
        """With a type map, scalar operands are coerced."""
        uid = uuid4()
        types: PostgresColumnTypes = {"id": _t("uuid")}
        r = PsycopgQueryRenderer(types=types)
        _sql, params = r.render(QueryField("id", "$eq", str(uid)))
        assert params == [uid]

    def test_nested_json_path_coerces_int(self) -> None:
        class _Inner(BaseModel):
            score: int

        class _Outer(BaseModel):
            meta: _Inner

        types: PostgresColumnTypes = {"meta": _t("jsonb")}
        r = PsycopgQueryRenderer(types=types, model_type=_Outer)
        _sql, params = r.render(QueryField("meta.score", "$eq", "7"))
        assert params == [7]

    def test_nested_json_with_table_alias(self) -> None:
        class _Inner(BaseModel):
            score: int

        class _Outer(BaseModel):
            meta: _Inner

        types: PostgresColumnTypes = {"meta": _t("jsonb")}
        r = PsycopgQueryRenderer(
            types=types,
            model_type=_Outer,
            table_alias="v",
        )
        _sql, params = r.render(QueryField("meta.score", "$gte", 1))
        assert params == [1]
        assert b"v" in _sql.as_bytes()  # qualified root column

    def test_nested_json_decimal_leaf_casts_to_numeric(self) -> None:
        class _Inner(BaseModel):
            price: Decimal

        class _Outer(BaseModel):
            meta: _Inner

        types: PostgresColumnTypes = {"meta": _t("jsonb")}
        r = PsycopgQueryRenderer(types=types, model_type=_Outer)
        sql_out, params = r.render(QueryField("meta.price", "$gt", Decimal("10.50")))
        assert b"numeric" in sql_out.as_bytes()
        assert params == [Decimal("10.50")]

    def test_nested_json_mixed_numeric_union_leaf_casts_to_numeric(self) -> None:
        class _Inner(BaseModel):
            amount: Decimal | int

        class _Outer(BaseModel):
            meta: _Inner

        types: PostgresColumnTypes = {"meta": _t("jsonb")}
        r = PsycopgQueryRenderer(types=types, model_type=_Outer)
        sql_out, params = r.render(QueryField("meta.amount", "$gt", Decimal("9.5")))
        assert b"numeric" in sql_out.as_bytes()
        assert params == [Decimal("9.5")]

    def test_scalar_elem_decimal_compares_numerically_not_in_jsonb_space(self) -> None:
        """A Decimal element operand extracts text and casts ::numeric — stored decimals
        are JSON strings, so a ``to_jsonb`` comparison would never match them."""

        r = PsycopgQueryRenderer(types={"prices": _t("jsonb")})
        sql_out, params = r.render(
            QueryElem("prices", "$any", QueryField(ELEM_SCALAR_FIELD, "$gte", Decimal("10.5"))),
        )
        s = sql_out.as_string(None)
        assert "#>>" in s and "::numeric" in s
        assert "to_jsonb" not in s
        assert params == [Decimal("10.5")]

        r2 = PsycopgQueryRenderer(types={"prices": _t("jsonb")})
        sql_out, params = r2.render(
            QueryElem(
                "prices",
                "$any",
                QueryField(ELEM_SCALAR_FIELD, "$in", [Decimal("1.5"), 2]),
            ),
        )
        s = sql_out.as_string(None)
        assert "#>>" in s and "::numeric" in s
        assert params == [Decimal("1.5"), Decimal("2")]

    def test_scalar_elem_decimal_typed_int_operand_compares_numerically(self) -> None:
        """An int operand against a Decimal-annotated jsonb array must also take the
        numeric path — stored decimals are JSON strings, so jsonb type ordering would
        rank them above every number regardless of value."""

        class _Doc(BaseModel):
            prices: list[Decimal | int]

        r = PsycopgQueryRenderer(types={"prices": _t("jsonb")}, model_type=_Doc)
        sql_out, params = r.render(
            QueryElem("prices", "$any", QueryField(ELEM_SCALAR_FIELD, "$gte", 5)),
        )
        s = sql_out.as_string(None)
        assert "#>>" in s and "::numeric" in s
        assert "to_jsonb" not in s
        assert params == [Decimal("5")]

    def test_scalar_elem_int_typed_int_operand_stays_in_jsonb_space(self) -> None:
        class _Doc(BaseModel):
            counts: list[int]

        r = PsycopgQueryRenderer(types={"counts": _t("jsonb")}, model_type=_Doc)
        sql_out, _params = r.render(
            QueryElem("counts", "$any", QueryField(ELEM_SCALAR_FIELD, "$gte", 5)),
        )
        assert "::numeric" not in sql_out.as_string(None)

    def test_nested_scalar_subarray_decimal_typed_int_operand_compares_numerically(
        self,
    ) -> None:
        """The same type-aware routing applies one level down: a scalar sub-array
        annotated with Decimal elements compares numerically for an int operand."""

        class _Item(BaseModel):
            prices: list[Decimal | int]

        class _Doc(BaseModel):
            items: list[_Item]

        r = PsycopgQueryRenderer(types={"items": _t("jsonb")}, model_type=_Doc)
        sql_out, params = r.render(
            QueryElem(
                "items",
                "$any",
                QueryElem("prices", "$any", QueryField(ELEM_SCALAR_FIELD, "$gte", 5)),
            ),
        )
        s = sql_out.as_string(None)
        assert "#>>" in s and "::numeric" in s
        assert params == [Decimal("5")]

    def test_scalar_elem_decimal_typed_int_membership_compares_numerically(self) -> None:
        """An all-int ``$in`` list on a Decimal-annotated array coerces every member
        to Decimal on the numeric path; one non-numeric member falls back to jsonb
        space, since ``::numeric`` could not represent it."""

        class _Doc(BaseModel):
            prices: list[Decimal | int]

        r = PsycopgQueryRenderer(types={"prices": _t("jsonb")}, model_type=_Doc)
        sql_out, params = r.render(
            QueryElem("prices", "$any", QueryField(ELEM_SCALAR_FIELD, "$in", [5, 10])),
        )
        assert "::numeric" in sql_out.as_string(None)
        assert params == [Decimal("5"), Decimal("10")]

        r2 = PsycopgQueryRenderer(types={"prices": _t("jsonb")}, model_type=_Doc)
        sql_out, _params = r2.render(
            QueryElem("prices", "$any", QueryField(ELEM_SCALAR_FIELD, "$in", [5, "n/a"])),
        )
        assert "::numeric" not in sql_out.as_string(None)

    def test_nested_scalar_subarray_or_inner_routes_each_arm(self) -> None:
        class _Item(BaseModel):
            prices: list[Decimal | int]

        class _Doc(BaseModel):
            items: list[_Item]

        r = PsycopgQueryRenderer(types={"items": _t("jsonb")}, model_type=_Doc)
        sql_out, params = r.render(
            QueryElem(
                "items",
                "$any",
                QueryElem(
                    "prices",
                    "$any",
                    QueryOr(
                        (
                            QueryField(ELEM_SCALAR_FIELD, "$gte", 5),
                            QueryField(ELEM_SCALAR_FIELD, "$eq", Decimal("1.5")),
                        ),
                    ),
                ),
            ),
        )
        s = sql_out.as_string(None)
        assert " OR " in s and "::numeric" in s
        assert params == [Decimal("5"), Decimal("1.5")]

    def test_scalar_elem_decimal_operand_with_text_operator_raises(self) -> None:
        """A Decimal operand routes to the numeric element path, which has no text
        operators — surfaced as an internal error rather than a silent text match."""

        r = PsycopgQueryRenderer(types={"prices": _t("jsonb")})
        with pytest.raises(CoreException, match="Unsupported nested scalar element operator"):
            r.render(
                QueryElem(
                    "prices",
                    "$any",
                    QueryField(ELEM_SCALAR_FIELD, "$like", Decimal("1.5")),
                ),
            )

    def test_jsonb_scalar_numeric_empty_membership_renders_constant(self) -> None:
        """Defensive branch: routing never sends an empty list to the numeric path
        (an empty operand list carries no Decimal and is not a numeric operand), but
        the renderer still degrades to the membership identities if called."""

        r = PsycopgQueryRenderer()
        elem = psql.Identifier("e")
        assert r._render_jsonb_scalar_numeric(elem, "$in", []).as_string(None) == "FALSE"
        assert r._render_jsonb_scalar_numeric(elem, "$nin", []).as_string(None) == "TRUE"

    def test_object_elem_decimal_and_union_leaves_cast_numeric(self) -> None:
        class _Item(BaseModel):
            price: Decimal
            amount: Decimal | int

        class _Doc(BaseModel):
            items: list[_Item]

        r = PsycopgQueryRenderer(types={"items": _t("jsonb")}, model_type=_Doc)
        sql_out, params = r.render(
            QueryElem("items", "$any", QueryField("price", "$gte", Decimal("2"))),
        )
        assert "::numeric" in sql_out.as_string(None)
        assert params == [Decimal("2")]

        r2 = PsycopgQueryRenderer(types={"items": _t("jsonb")}, model_type=_Doc)
        sql_out, params = r2.render(
            QueryElem("items", "$any", QueryField("amount", "$gte", 3)),
        )
        assert "::numeric" in sql_out.as_string(None)
        assert params == [Decimal("3")]

    def test_nested_field_hints_for_dict_leaf(self) -> None:
        class _Blob(BaseModel):
            data: dict[str, Any]

        types: PostgresColumnTypes = {"data": _t("jsonb")}
        r = PsycopgQueryRenderer(
            types=types,
            model_type=_Blob,
            nested_field_hints={"data.x": int},
        )
        _sql, params = r.render(QueryField("data.x", "$eq", "3"))
        assert params == [3]

    def test_nested_unsupported_operator(self) -> None:
        class _Inner(BaseModel):
            score: int

        class _Outer(BaseModel):
            meta: _Inner

        types: PostgresColumnTypes = {"meta": _t("jsonb")}
        r = PsycopgQueryRenderer(types=types, model_type=_Outer)
        with pytest.raises(CoreException, match="not supported on the nested JSON path"):
            r.render(QueryField("meta.score", "$empty", True))

    def test_nested_requires_json_column(self) -> None:
        class _Inner(BaseModel):
            score: int

        class _Outer(BaseModel):
            meta: _Inner

        types: PostgresColumnTypes = {"meta": _t("int8")}
        r = PsycopgQueryRenderer(types=types, model_type=_Outer)
        with pytest.raises(CoreException, match="json or jsonb"):
            r.render(QueryField("meta.score", "$eq", 1))

    def test_ilike_renders_sql_with_bound_param(self) -> None:
        types: PostgresColumnTypes = {"title": _t("text")}
        r = PsycopgQueryRenderer(types=types)
        _sql, params = r.render(QueryField("title", "$ilike", "%road%"))
        assert params == ["%road%"]
        assert b"ILIKE" in _sql.as_bytes()

    def test_ilike_rejects_non_text_column(self) -> None:
        types: PostgresColumnTypes = {"n": _t("int8")}
        r = PsycopgQueryRenderer(types=types)
        with pytest.raises(CoreException, match="text-like"):
            r.render(QueryField("n", "$ilike", "%x%"))

    def test_regex_renders_tilde_operator(self) -> None:
        types: PostgresColumnTypes = {"title": _t("text")}
        r = PsycopgQueryRenderer(types=types)
        _sql, params = r.render(QueryField("title", "$regex", "^foo"))
        assert params == ["^foo"]
        assert b"~" in _sql.as_bytes()


class _OrderRow(BaseModel):
    category: str
    price: float


class _TsRow(BaseModel):
    item_id: str
    ts: datetime
    price: float


class TestPostgresAggregateRendering:
    def test_renders_grouped_aggregate_select_and_sort(self) -> None:
        renderer = PsycopgQueryRenderer(
            types={"category": _t("text"), "price": _t("numeric")},
            model_type=_OrderRow,
        )

        parsed, select_clause, group_clause, params = renderer.render_aggregates(
            {
                "$groups": {"category": "category"},
                "$computed": {
                    "orders": {"$count": None},
                    "revenue": {"$sum": "price"},
                    "median_price": {"$median": "price"},
                },
            },
        )
        sort_clause = renderer.render_aggregate_order_by(parsed, {"revenue": "desc"})

        select_sql = select_clause.as_bytes()
        assert b"COUNT(*) AS" in select_sql
        assert b"SUM" in select_sql
        assert b"percentile_cont(0.5)" in select_sql
        assert group_clause is not None
        assert b"category" in group_clause.as_bytes()
        assert sort_clause is not None
        assert b"revenue" in sort_clause.as_bytes()
        assert params == []

    def test_renders_conditional_aggregate_filters(self) -> None:
        renderer = PsycopgQueryRenderer(
            types={"category": _t("text"), "price": _t("numeric")},
            model_type=_OrderRow,
        )

        _parsed, select_clause, _group_clause, params = renderer.render_aggregates(
            {
                "$computed": {
                    "mid_count": {
                        "$count": {
                            "filter": {
                                "$values": {"price": {"$gte": 10, "$lte": 20}},
                            },
                        },
                    },
                    "book_revenue": {
                        "$sum": {
                            "field": "price",
                            "filter": {"$values": {"category": "books"}},
                        },
                    },
                },
            },
        )

        select_sql = select_clause.as_bytes()
        assert b"FILTER (WHERE" in select_sql
        assert b"COUNT(*) FILTER" in select_sql
        assert b"SUM" in select_sql
        assert params == [10, 20, "books"]

    def test_renders_trunc_group_with_field_group(self) -> None:
        renderer = PsycopgQueryRenderer(
            types={
                "item_id": _t("text"),
                "ts": _t("timestamptz"),
                "price": _t("numeric"),
            },
            model_type=_TsRow,
        )
        parsed, select_clause, group_clause, params = renderer.render_aggregates(
            {
                "$groups": {
                    "item": "item_id",
                    "day_start": {
                        "$trunc": {"field": "ts", "unit": "day", "timezone": "UTC"},
                    },
                },
                "$computed": {"avg_p": {"$avg": "price"}},
            },
        )
        sel = select_clause.as_bytes()
        gro = group_clause.as_bytes() if group_clause else b""
        assert b"date_trunc" in sel
        assert b"item_id" in gro
        assert len(parsed.groups) == 2
        assert params == []

    def test_rejects_unknown_aggregate_sort_alias(self) -> None:
        renderer = PsycopgQueryRenderer(
            types={"category": _t("text"), "price": _t("numeric")},
            model_type=_OrderRow,
        )
        parsed, _select_clause, _group_clause, _params = renderer.render_aggregates(
            {"$computed": {"orders": {"$count": None}}},
        )

        with pytest.raises(CoreException, match="Invalid aggregate sort fields"):
            renderer.render_aggregate_order_by(parsed, {"missing": "asc"})

    def test_trunc_with_fixed_offset_timezone(self) -> None:
        """A numeric-offset timezone renders the AT TIME ZONE 'UTC' + offset form."""
        renderer = PsycopgQueryRenderer(
            types={
                "item_id": _t("text"),
                "ts": _t("timestamptz"),
                "price": _t("numeric"),
            },
            model_type=_TsRow,
        )
        parsed, select_clause, group_clause, params = renderer.render_aggregates(
            {
                "$groups": {
                    "bucket": {
                        "$trunc": {
                            "field": "ts",
                            "unit": "hour",
                            "timezone": "+03:00",
                        },
                    },
                },
                "$computed": {"avg_p": {"$avg": "price"}},
            },
        )
        sel = select_clause.as_string(None)
        assert "date_trunc" in sel
        assert "AT TIME ZONE 'UTC'" in sel
        assert group_clause is not None
        assert len(parsed.groups) == 1
        assert params == []

    @pytest.mark.parametrize(
        ("function", "marker"),
        [
            ("$min", "MIN"),
            ("$max", "MAX"),
        ],
    )
    def test_min_max_aggregate_functions(self, function: str, marker: str) -> None:
        """$min / $max aggregate functions render their SQL wrappers."""
        renderer = PsycopgQueryRenderer(
            types={"category": _t("text"), "price": _t("numeric")},
            model_type=_OrderRow,
        )
        _parsed, select_clause, _group_clause, _params = renderer.render_aggregates(
            {"$computed": {"result": {function: "price"}}},
        )
        assert marker in select_clause.as_string(None)

    def test_aggregate_order_by_ascending(self) -> None:
        """An ``asc`` sort renders the ASC direction keyword."""
        renderer = PsycopgQueryRenderer(
            types={"category": _t("text"), "price": _t("numeric")},
            model_type=_OrderRow,
        )
        parsed, _select, _group, _params = renderer.render_aggregates(
            {"$computed": {"orders": {"$count": None}}},
        )
        sort_clause = renderer.render_aggregate_order_by(parsed, {"orders": "asc"})
        assert sort_clause is not None
        assert "ASC" in sort_clause.as_string(None)

    def test_aggregate_order_by_none_when_no_sorts(self) -> None:
        renderer = PsycopgQueryRenderer(
            types={"category": _t("text")},
            model_type=_OrderRow,
        )
        parsed, _select, _group, _params = renderer.render_aggregates(
            {"$computed": {"orders": {"$count": None}}},
        )
        assert renderer.render_aggregate_order_by(parsed, None) is None

    def test_median_aggregate_function(self) -> None:
        renderer = PsycopgQueryRenderer(
            types={"price": _t("numeric")},
            model_type=_OrderRow,
        )
        _parsed, select_clause, _group, _params = renderer.render_aggregates(
            {"$computed": {"med": {"$median": "price"}}},
        )
        assert "percentile_cont(0.5)" in select_clause.as_string(None)

    def test_extended_aggregate_functions_render(self) -> None:
        renderer = PsycopgQueryRenderer(
            types={"category": _t("text"), "price": _t("numeric")},
            model_type=_OrderRow,
        )
        _parsed, select_clause, _group, _params = renderer.render_aggregates(
            {
                "$groups": {"category": "category"},
                "$computed": {
                    "distinct": {"$count_distinct": "price"},
                    "sp": {"$stddev_pop": "price"},
                    "ss": {"$stddev_samp": "price"},
                    "vp": {"$var_pop": "price"},
                    "vs": {"$var_samp": "price"},
                    "p90": {"$percentile": {"field": "price", "p": 0.9}},
                },
            },
        )
        sql = select_clause.as_string(None)
        assert "COUNT(DISTINCT" in sql
        assert "stddev_pop(" in sql
        assert "stddev_samp(" in sql
        assert "var_pop(" in sql
        assert "var_samp(" in sql
        assert "percentile_cont(0.9)" in sql

    def test_aggregate_filter_parses_raw_filter_when_unparsed(self) -> None:
        """A computed field with a raw (not pre-parsed) filter is parsed lazily."""
        renderer = PsycopgQueryRenderer(
            types={"category": _t("text"), "price": _t("numeric")},
            model_type=_OrderRow,
        )
        # Construct a computed field whose ``parsed_filter`` is None to force the
        # lazy parse branch in ``_render_aggregate_filter``.
        from forze.application.contracts.querying import AggregateComputedField

        computed = AggregateComputedField(
            alias="hits",
            function="$count",
            field=None,
            filter={"$values": {"category": "books"}},
            parsed_filter=None,
        )
        expr = renderer._render_aggregate_function(computed)
        assert "FILTER (WHERE" in expr.as_string(None)
        assert renderer.binder.values() == ["books"]


class TestPostgresAggregateRenderingErrors:
    """Error branches in aggregate/source-expression rendering."""

    def test_source_expr_requires_types(self) -> None:
        from forze.application.contracts.querying import AggregateComputedField

        renderer = PsycopgQueryRenderer(model_type=_OrderRow)
        computed = AggregateComputedField(
            alias="s",
            function="$sum",
            field="price",
            filter=None,
            parsed_filter=None,
        )
        with pytest.raises(CoreException, match="column type metadata"):
            renderer._render_aggregate_function(computed)

    def test_source_expr_requires_model_type(self) -> None:
        from forze.application.contracts.querying import AggregateComputedField

        renderer = PsycopgQueryRenderer(types={"price": _t("numeric")})
        computed = AggregateComputedField(
            alias="s",
            function="$sum",
            field="price",
            filter=None,
            parsed_filter=None,
        )
        with pytest.raises(CoreException, match="model_type"):
            renderer._render_aggregate_function(computed)

    def test_computed_field_without_field_path_raises(self) -> None:
        from forze.application.contracts.querying import AggregateComputedField

        renderer = PsycopgQueryRenderer(
            types={"price": _t("numeric")},
            model_type=_OrderRow,
        )
        computed = AggregateComputedField(
            alias="s",
            function="$sum",
            field=None,
            filter=None,
            parsed_filter=None,
        )
        with pytest.raises(CoreException, match="no field path"):
            renderer._render_aggregate_function(computed)


class TestPostgresHierarchyRendering:
    """``$descendant_of`` / ``$ancestor_of`` render via ltree ops or a text fallback."""

    def test_ltree_descendant_uses_containment_operator(self) -> None:
        r = PsycopgQueryRenderer(types={"path": _t("ltree")})
        _sql, params = r.render(QueryField("path", "$descendant_of", "top.science"))
        s = _sql.as_string(None)
        assert "<@" in s and "::ltree" in s
        assert params == ["top.science"]

    def test_ltree_ancestor_uses_containment_operator(self) -> None:
        r = PsycopgQueryRenderer(types={"path": _t("ltree")})
        _sql, params = r.render(QueryField("path", "$ancestor_of", "top.science"))
        s = _sql.as_string(None)
        assert "@>" in s and "::ltree" in s
        assert params == ["top.science"]

    def test_text_column_uses_starts_with_fallback(self) -> None:
        r = PsycopgQueryRenderer(types={"path": _t("text")})
        _sql, params = r.render(QueryField("path", "$descendant_of", "top.science"))
        s = _sql.as_string(None)
        assert "starts_with" in s and "::text" in s
        assert params == ["top.science"]

    def test_ancestor_text_fallback_reverses_prefix(self) -> None:
        r = PsycopgQueryRenderer(types={"path": _t("text")})
        _sql, _params = r.render(QueryField("path", "$ancestor_of", "a.b"))
        assert "starts_with" in _sql.as_string(None)

    def test_no_type_metadata_uses_text_fallback(self) -> None:
        r = PsycopgQueryRenderer()
        _sql, _params = r.render(QueryField("path", "$descendant_of", "a"))
        assert "starts_with" in _sql.as_string(None)

    def test_array_column_rejected(self) -> None:
        r = PsycopgQueryRenderer(types={"path": _t("text", is_array=True)})
        with pytest.raises(CoreException, match="array column"):
            r.render(QueryField("path", "$descendant_of", "a"))

    def test_non_text_non_ltree_column_rejected(self) -> None:
        r = PsycopgQueryRenderer(types={"path": _t("int4")})
        with pytest.raises(CoreException, match="ltree or text-like"):
            r.render(QueryField("path", "$ancestor_of", "a"))


class _CompareInner(BaseModel):
    score: int
    min_score: int = 0


class _CompareOuter(BaseModel):
    meta: _CompareInner


class TestCompareErrorBranches:
    """Error branches in field-to-field compare resolution."""

    def test_nested_compare_requires_types(self) -> None:
        r = PsycopgQueryRenderer(model_type=_CompareOuter)
        with pytest.raises(CoreException, match="column type metadata"):
            r.render(QueryCompare("meta.score", "$gte", "meta.min_score"))

    def test_nested_compare_requires_model_type(self) -> None:
        r = PsycopgQueryRenderer(types={"meta": _t("jsonb")})
        with pytest.raises(CoreException, match="model_type"):
            r.render(QueryCompare("meta.score", "$gte", "meta.min_score"))

    def test_compare_unknown_column_raises(self) -> None:
        r = PsycopgQueryRenderer(types={"a": _t("int4")})
        with pytest.raises(CoreException, match="Unknown column"):
            r.render(QueryCompare("a", "$eq", "missing"))

    def test_compare_compatible_numeric_group(self) -> None:
        """Different bases within a compatibility group compare without error."""
        r = PsycopgQueryRenderer(types={"a": _t("int2"), "b": _t("int8")})
        _sql, params = r.render(QueryCompare("a", "$eq", "b"))
        assert params == []

    def test_compare_unknown_operator_raises(self) -> None:
        r = PsycopgQueryRenderer(types={"a": _t("int4"), "b": _t("int4")})
        bad = QueryCompare("a", cast(Any, "$bogus"), "b")
        with pytest.raises(CoreException, match="Unknown compare operator"):
            r.render(bad)


class TestNestedFilterErrorBranches:
    """Error branches in single-field nested-path filter rendering."""

    def test_nested_filter_requires_types(self) -> None:
        r = PsycopgQueryRenderer(model_type=_CompareOuter)
        with pytest.raises(CoreException, match="column type metadata"):
            r.render(QueryField("meta.score", "$eq", 1))

    def test_nested_filter_requires_model_type(self) -> None:
        r = PsycopgQueryRenderer(types={"meta": _t("jsonb")})
        with pytest.raises(CoreException, match="model_type"):
            r.render(QueryField("meta.score", "$eq", 1))


class TestTextOperatorGuards:
    """Text operator coverage and column guards."""

    def test_like_renders_sql(self) -> None:
        r = PsycopgQueryRenderer(types={"title": _t("text")})
        _sql, params = r.render(QueryField("title", "$like", "ro%"))
        assert params == ["ro%"]
        assert "LIKE" in _sql.as_string(None)

    def test_text_op_without_types_is_allowed(self) -> None:
        """No type metadata means the text-column guard is a no-op."""
        r = PsycopgQueryRenderer()
        _sql, params = r.render(QueryField("title", "$like", "ro%"))
        assert params == ["ro%"]
        assert "LIKE" in _sql.as_string(None)

    def test_text_op_on_array_column_raises(self) -> None:
        r = PsycopgQueryRenderer(types={"tags": _t("text", is_array=True)})
        with pytest.raises(CoreException, match="array column"):
            r.render(QueryField("tags", "$like", "x%"))

    def test_text_op_on_non_text_column_raises(self) -> None:
        r = PsycopgQueryRenderer(types={"n": _t("int8")})
        with pytest.raises(CoreException, match="text-like"):
            r.render(QueryField("n", "$like", "x%"))


class TestElementErrorBranches:
    """Element quantifier resolution and inner-predicate error branches."""

    def test_elem_on_non_array_non_json_column_raises(self) -> None:
        r = PsycopgQueryRenderer(types={"n": _t("int8")})
        with pytest.raises(CoreException, match="array or jsonb"):
            r.render(QueryElem("n", "$any", QueryField(ELEM_SCALAR_FIELD, "$eq", 1)))

    def test_nested_elem_path_requires_types_and_model(self) -> None:
        r = PsycopgQueryRenderer()
        with pytest.raises(CoreException, match="column types and model_type"):
            r.render(
                QueryElem("a.b", "$any", QueryField(ELEM_SCALAR_FIELD, "$eq", 1)),
            )

    def test_elem_unknown_column_raises(self) -> None:
        r = PsycopgQueryRenderer(types={"tags": _t("text", is_array=True)})
        with pytest.raises(CoreException, match="Unknown column"):
            r.render(
                QueryElem("missing", "$any", QueryField(ELEM_SCALAR_FIELD, "$eq", 1)),
            )

    def test_elem_without_types_renders(self) -> None:
        """No type metadata falls through to the json-array element shape."""
        r = PsycopgQueryRenderer()
        _sql, params = r.render(
            QueryElem("tags", "$any", QueryField(ELEM_SCALAR_FIELD, "$eq", "x")),
        )
        assert params == ["x"]
        assert "jsonb_array_elements" in _sql.as_string(None)

    def test_scalar_inner_and_multiple_fields(self) -> None:
        """A QueryAnd scalar inner with multiple fields joins predicates with AND."""
        r = PsycopgQueryRenderer(types={"scores": _t("int4", is_array=True)})
        inner = QueryAnd(
            (
                QueryField(ELEM_SCALAR_FIELD, "$gte", 1),
                QueryField(ELEM_SCALAR_FIELD, "$lte", 10),
            ),
        )
        _sql, params = r.render(QueryElem("scores", "$any", inner))
        s = _sql.as_string(None)
        assert params == [1, 10]
        assert " AND " in s

    def test_scalar_inner_or_branch(self) -> None:
        """A QueryOr scalar inner renders an OR of element predicates."""
        r = PsycopgQueryRenderer(types={"scores": _t("int4", is_array=True)})
        inner = QueryOr(
            (
                QueryField(ELEM_SCALAR_FIELD, "$eq", 1),
                QueryField(ELEM_SCALAR_FIELD, "$eq", 2),
            ),
        )
        _sql, params = r.render(QueryElem("scores", "$any", inner))
        s = _sql.as_string(None)
        assert params == [1, 2]
        assert " OR " in s

    def test_scalar_inner_or_single_child(self) -> None:
        """A single-child OR scalar inner renders directly (no extra grouping)."""
        r = PsycopgQueryRenderer(types={"scores": _t("int4", is_array=True)})
        inner = QueryOr((QueryField(ELEM_SCALAR_FIELD, "$eq", 5),))
        _sql, params = r.render(QueryElem("scores", "$any", inner))
        assert params == [5]

    def test_non_scalar_inner_node_routes_to_object_path_and_raises(self) -> None:
        """A QueryNot inner is not scalar, so it routes to the object path and is rejected."""
        r = PsycopgQueryRenderer(types={"scores": _t("int4", is_array=True)})
        bad_inner = QueryNot(QueryField(ELEM_SCALAR_FIELD, "$eq", 1))
        with pytest.raises(CoreException, match="Invalid object element inner"):
            r.render(QueryElem("scores", "$any", bad_inner))

    def test_object_inner_single_field(self) -> None:
        class _Item(BaseModel):
            status: str

        class _Row(BaseModel):
            items: list[_Item]

        r = PsycopgQueryRenderer(types={"items": _t("jsonb")}, model_type=_Row)
        inner = QueryField("status", "$eq", "open")
        _sql, params = r.render(QueryElem("items", "$any", inner))
        assert params == ["open"]
        assert '"_fz_elem" ->>' in _sql.as_string(None)

    def test_object_inner_or_branch(self) -> None:
        class _Item(BaseModel):
            status: str

        class _Row(BaseModel):
            items: list[_Item]

        r = PsycopgQueryRenderer(types={"items": _t("jsonb")}, model_type=_Row)
        inner = QueryOr(
            (
                QueryField("status", "$eq", "open"),
                QueryField("status", "$eq", "done"),
            ),
        )
        _sql, params = r.render(QueryElem("items", "$any", inner))
        s = _sql.as_string(None)
        assert params == ["open", "done"]
        assert " OR " in s

    def test_object_inner_or_single_child(self) -> None:
        class _Item(BaseModel):
            status: str

        class _Row(BaseModel):
            items: list[_Item]

        r = PsycopgQueryRenderer(types={"items": _t("jsonb")}, model_type=_Row)
        inner = QueryOr((QueryField("status", "$eq", "open"),))
        _sql, params = r.render(QueryElem("items", "$any", inner))
        assert params == ["open"]

    def test_object_inner_without_model_type_no_cast(self) -> None:
        """Object inner on a jsonb column without a model skips leaf-type inference."""
        r = PsycopgQueryRenderer(types={"items": _t("jsonb")})
        inner = QueryField("status", "$eq", "open")
        _sql, params = r.render(QueryElem("items", "$any", inner))
        s = _sql.as_string(None)
        assert params == ["open"]
        assert '("_fz_elem" ->> \'status\')' in s

    def test_object_inner_unwalkable_field_no_cast(self) -> None:
        """A field not present on the element model resolves to text (no cast)."""

        class _Item(BaseModel):
            status: str

        class _Row(BaseModel):
            items: list[_Item]

        r = PsycopgQueryRenderer(types={"items": _t("jsonb")}, model_type=_Row)
        inner = QueryField("unknown_field", "$eq", "v")
        _sql, params = r.render(QueryElem("items", "$any", inner))
        assert params == ["v"]
        assert '("_fz_elem" ->> \'unknown_field\')' in _sql.as_string(None)

    def test_object_inner_invalid_node_raises(self) -> None:
        class _Item(BaseModel):
            status: str

        class _Row(BaseModel):
            items: list[_Item]

        r = PsycopgQueryRenderer(types={"items": _t("jsonb")}, model_type=_Row)
        with pytest.raises(CoreException, match="Invalid object element inner"):
            r.render(QueryElem("items", "$any", _UnknownExpr()))

    def test_object_inner_nested_segment_path(self) -> None:
        """A dotted object-inner field path uses the ``#>>`` JSON extraction form."""

        class _Inner(BaseModel):
            code: str

        class _Item(BaseModel):
            attr: _Inner

        class _Row(BaseModel):
            items: list[_Item]

        r = PsycopgQueryRenderer(types={"items": _t("jsonb")}, model_type=_Row)
        inner = QueryField("attr.code", "$eq", "x")
        _sql, params = r.render(QueryElem("items", "$any", inner))
        s = _sql.as_string(None)
        assert params == ["x"]
        assert '"_fz_elem" #>>' in s

    def test_object_inner_leaf_type_resolution_failure_is_swallowed(self) -> None:
        """A leaf whose type resolution raises falls back to text (no cast)."""

        class _Item(BaseModel):
            # ``walk_pydantic_path`` resolves ``codes`` to ``list[str]`` (non-None),
            # but ``resolve_leaf_python_type`` rejects array-typed JSON leaves; the
            # renderer swallows that and treats the field as untyped text.
            codes: list[str]

        class _Row(BaseModel):
            items: list[_Item]

        r = PsycopgQueryRenderer(types={"items": _t("jsonb")}, model_type=_Row)
        inner = QueryField("codes", "$eq", "v")
        _sql, params = r.render(QueryElem("items", "$any", inner))
        assert params == ["v"]
        # No per-field cast on the extracted value (leaf type stayed unresolved):
        # the extraction uses ``->>`` (text) with no trailing ``)::<type>`` cast.
        assert '("_fz_elem" ->> \'codes\')' in _sql.as_string(None)

    def test_object_inner_typed_field_applies_cast(self) -> None:
        """A resolvable non-text leaf type applies a SQL cast to the extracted value."""

        class _Item(BaseModel):
            qty: int

        class _Row(BaseModel):
            items: list[_Item]

        r = PsycopgQueryRenderer(types={"items": _t("jsonb")}, model_type=_Row)
        inner = QueryField("qty", "$gte", "3")
        _sql, params = r.render(QueryElem("items", "$any", inner))
        s = _sql.as_string(None)
        assert params == [3]
        assert "::int8" in s

    def test_object_inner_multiple_fields_joined_with_and(self) -> None:
        """Multiple object-inner fields are joined with AND."""

        class _Item(BaseModel):
            status: str
            qty: int

        class _Row(BaseModel):
            items: list[_Item]

        r = PsycopgQueryRenderer(types={"items": _t("jsonb")}, model_type=_Row)
        inner = QueryAnd(
            (
                QueryField("status", "$eq", "open"),
                QueryField("qty", "$gte", 1),
            ),
        )
        _sql, params = r.render(QueryElem("items", "$any", inner))
        s = _sql.as_string(None)
        assert params == ["open", 1]
        assert " AND " in s

    def test_nested_elem_path_array_leaf_rejected(self) -> None:
        """A dotted element path resolving to an array leaf is rejected by the builder."""

        class _Outer(BaseModel):
            tags: list[str]

        class _Row(BaseModel):
            meta: _Outer

        r = PsycopgQueryRenderer(types={"meta": _t("jsonb")}, model_type=_Row)
        with pytest.raises(CoreException, match="array-typed leaves"):
            r.render(
                QueryElem(
                    "meta.tags",
                    "$any",
                    QueryField(ELEM_SCALAR_FIELD, "$eq", "x"),
                ),
            )


class TestNestedQuantifierRendering:
    """A quantifier nested inside another element's object predicate (jsonb arrays)."""

    class _Item(BaseModel):
        tags: list[str]
        scores: list[int]

    class _Row(BaseModel):
        items: list[TestNestedQuantifierRendering._Item]

    def _renderer(self) -> PsycopgQueryRenderer:
        return PsycopgQueryRenderer(
            types={"items": _t("jsonb")},
            model_type=self._Row,
        )

    def _nested(self, sub: str, inner: QueryExpr, quant: str = "$any") -> QueryElem:
        return QueryElem("items", "$any", QueryElem(sub, quant, inner))

    def test_nested_scalar_subarray_any_eq_uses_jsonb_space(self) -> None:
        r = self._renderer()
        node = self._nested("tags", QueryField(ELEM_SCALAR_FIELD, "$eq", "x"))
        _sql, params = r.render(node)
        s = _sql.as_string(None)
        assert "jsonb_array_elements" in s
        assert "to_jsonb" in s and "::text" in s  # _jsonb_bound text cast
        assert params == ["x"]

    def test_nested_scalar_subarray_ordering_int_bound(self) -> None:
        r = self._renderer()
        node = self._nested("scores", QueryField(ELEM_SCALAR_FIELD, "$gte", 5))
        _sql, params = r.render(node)
        s = _sql.as_string(None)
        assert "::bigint" in s  # _jsonb_bound int cast
        assert params == [5]

    def test_nested_scalar_subarray_membership_and_text(self) -> None:
        r = self._renderer()
        node = self._nested(
            "tags",
            QueryAnd(
                (
                    QueryField(ELEM_SCALAR_FIELD, "$in", ["a", "b"]),
                    QueryField(ELEM_SCALAR_FIELD, "$like", "a%"),
                ),
            ),
        )
        _sql, _params = r.render(node)
        s = _sql.as_string(None)
        assert " IN (" in s
        assert "#>>" in s  # text op extracts element text

    def test_nested_scalar_subarray_all_and_none_quantifiers(self) -> None:
        r = self._renderer()
        for quant in ("$all", "$none"):
            node = self._nested("tags", QueryField(ELEM_SCALAR_FIELD, "$eq", "x"), quant)
            _sql, _params = r.render(node)
            s = _sql.as_string(None)
            assert "jsonb_array_elements" in s

    def test_object_inner_rejects_unsupported_node(self) -> None:
        # A node that is neither a field nor a nested quantifier must fail fast, not be
        # silently dropped from the object-element predicate.
        r = self._renderer()
        bad_inner = QueryAnd(
            (QueryField("tags", "$eq", "x"), QueryCompare("a", "$eq", "b")),
        )
        with pytest.raises(CoreException, match="Unsupported node in object element"):
            r.render(QueryElem("items", "$any", bad_inner))

    def test_scalar_array_of_arrays_nested_quantifier(self) -> None:
        # ``matrix $any {$any: "x"}`` — the element is itself a scalar sub-array.
        class _Row(BaseModel):
            matrix: list[list[str]]

        r = PsycopgQueryRenderer(types={"matrix": _t("jsonb")}, model_type=_Row)
        node = QueryElem(
            "matrix",
            "$any",
            QueryElem(ELEM_SCALAR_FIELD, "$any", QueryField(ELEM_SCALAR_FIELD, "$eq", "x")),
        )
        _sql, params = r.render(node)
        assert "jsonb_array_elements" in _sql.as_string(None)
        assert params == ["x"]


class TestPythonAnnToPostgresType:
    """Coverage for the python-annotation to PostgresType mapping."""

    @pytest.mark.parametrize(
        ("py_type", "expected_base"),
        [
            (UUID, "uuid"),
            (bool, "bool"),
            (int, "int8"),
            (float, "float8"),
            (datetime, "timestamptz"),
            (date, "date"),
            (str, "text"),
        ],
    )
    def test_known_annotations_map_to_bases(
        self,
        py_type: type,
        expected_base: str,
    ) -> None:
        result = PsycopgQueryRenderer._python_ann_to_postgres_type(py_type)
        assert result is not None
        assert result.base == expected_base
        assert result.is_array is False

    def test_none_annotation_returns_none(self) -> None:
        assert PsycopgQueryRenderer._python_ann_to_postgres_type(None) is None

    def test_unknown_annotation_returns_none(self) -> None:
        assert PsycopgQueryRenderer._python_ann_to_postgres_type(complex) is None


class TestPsycopgValueCoercer:
    """Direct coverage for the value coercer used by the renderer."""

    def test_scalar_none_returns_none(self) -> None:
        c = PsycopgValueCoercer()
        assert c.scalar(None, t=_t("int4")) is None

    def test_scalar_no_type_passes_through(self) -> None:
        c = PsycopgValueCoercer()
        assert c.scalar("raw", t=None) == "raw"

    def test_scalar_array_type_raises(self) -> None:
        c = PsycopgValueCoercer()
        with pytest.raises(CoreException, match="Array type not supported"):
            c.scalar("x", t=_t("text", is_array=True))

    @pytest.mark.parametrize(
        ("base", "value", "expected"),
        [
            ("text", 5, "5"),
            ("bool", "true", True),
            ("int4", "7", 7),
            ("float8", "1.5", 1.5),
        ],
    )
    def test_scalar_base_coercions(
        self,
        base: str,
        value: Any,
        expected: Any,
    ) -> None:
        c = PsycopgValueCoercer()
        assert c.scalar(value, t=_t(base)) == expected

    def test_scalar_unknown_base_passthrough(self) -> None:
        c = PsycopgValueCoercer()
        assert c.scalar("v", t=_t("bytea")) == "v"

    def test_scalar_uuid_coercion(self) -> None:
        c = PsycopgValueCoercer()
        u = uuid4()
        assert c.scalar(str(u), t=_t("uuid")) == u

    def test_scalar_date_coercion(self) -> None:
        c = PsycopgValueCoercer()
        assert c.scalar("2024-01-02", t=_t("date")) == date(2024, 1, 2)

    def test_scalar_timestamptz_coercion(self) -> None:
        c = PsycopgValueCoercer()
        out = c.scalar("2024-01-02T03:04:05+00:00", t=_t("timestamptz"))
        assert isinstance(out, datetime)
        assert out.tzinfo is not None

    def test_scalar_timestamp_coercion(self) -> None:
        c = PsycopgValueCoercer()
        out = c.scalar("2024-01-02T03:04:05", t=_t("timestamp"))
        assert isinstance(out, datetime)

    def test_array_none_returns_empty(self) -> None:
        c = PsycopgValueCoercer()
        assert c.array(None, t=_t("int4", is_array=True)) == []

    def test_array_scalar_value_raises(self) -> None:
        c = PsycopgValueCoercer()
        with pytest.raises(CoreException, match="Scalar value not supported"):
            c.array(5, t=None)

    def test_array_no_type_coerces_elements(self) -> None:
        c = PsycopgValueCoercer()
        assert c.array([1, "a"], t=None) == [1, "a"]

    def test_array_raise_on_scalar_type(self) -> None:
        c = PsycopgValueCoercer()
        with pytest.raises(CoreException, match="require an array column"):
            c.array([1, 2], t=_t("int4"), raise_on_scalar_t=True)

    def test_array_typed_elements_coerced(self) -> None:
        c = PsycopgValueCoercer()
        assert c.array(["1", "2"], t=_t("int4", is_array=True)) == [1, 2]

    def test_scalar_is_a_query_value_scalar(self) -> None:
        # Sanity: the alias used by ``array`` recognizes plain scalars.
        assert isinstance(5, QueryValue.Scalar)
