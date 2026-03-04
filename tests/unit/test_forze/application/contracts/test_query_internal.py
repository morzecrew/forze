"""Unit tests for forze.application.contracts.query.internal."""

from datetime import date, datetime, timezone
from uuid import UUID

import pytest

from forze.application.contracts.query.internal import (
    QueryAnd,
    QueryExpr,
    QueryField,
    QueryFilterExpressionParser,
    QueryOr,
    QueryValueCaster,
)

# ----------------------- #


class TestQueryValueCaster:
    """Tests for QueryValueCaster."""

    # as_bool
    def test_as_bool_true(self) -> None:
        assert QueryValueCaster.as_bool(True) is True
        assert QueryValueCaster.as_bool(1) is True
        assert QueryValueCaster.as_bool("true") is True
        assert QueryValueCaster.as_bool("yes") is True
        assert QueryValueCaster.as_bool("on") is True

    def test_as_bool_false(self) -> None:
        assert QueryValueCaster.as_bool(False) is False
        assert QueryValueCaster.as_bool(0) is False
        assert QueryValueCaster.as_bool("false") is False
        assert QueryValueCaster.as_bool("no") is False
        assert QueryValueCaster.as_bool("off") is False

    def test_as_bool_invalid_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid boolean"):
            QueryValueCaster.as_bool("invalid")
        with pytest.raises(ValueError, match="Invalid boolean"):
            QueryValueCaster.as_bool(42)

    # as_int
    def test_as_int(self) -> None:
        assert QueryValueCaster.as_int(42) == 42
        assert QueryValueCaster.as_int("42") == 42
        assert QueryValueCaster.as_int(42.0) == 42

    def test_as_int_bool_raises(self) -> None:
        with pytest.raises(ValueError, match="got bool"):
            QueryValueCaster.as_int(True)

    def test_as_int_invalid_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid int"):
            QueryValueCaster.as_int("abc")

    # as_float
    def test_as_float(self) -> None:
        assert QueryValueCaster.as_float(3.14) == 3.14
        assert QueryValueCaster.as_float(42) == 42.0
        assert QueryValueCaster.as_float("3.14") == 3.14
        assert QueryValueCaster.as_float("3,14") == 3.14

    def test_as_float_bool_raises(self) -> None:
        with pytest.raises(ValueError, match="got bool"):
            QueryValueCaster.as_float(True)

    def test_as_float_invalid_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid float"):
            QueryValueCaster.as_float("abc")

    # as_uuid
    def test_as_uuid(self) -> None:
        u = UUID("550e8400-e29b-41d4-a716-446655440000")
        assert QueryValueCaster.as_uuid(u) == u
        assert QueryValueCaster.as_uuid(str(u)) == u

    def test_as_uuid_invalid_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid UUID"):
            QueryValueCaster.as_uuid("not-a-uuid")
        with pytest.raises(ValueError, match="Invalid UUID"):
            QueryValueCaster.as_uuid(42)

    # as_datetime
    def test_as_datetime_from_datetime(self) -> None:
        dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        assert QueryValueCaster.as_datetime(dt, force_tz=True) == dt

    def test_as_datetime_from_iso_string(self) -> None:
        result = QueryValueCaster.as_datetime("2024-01-15T12:00:00Z", force_tz=True)
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15

    def test_as_datetime_from_timestamp(self) -> None:
        result = QueryValueCaster.as_datetime(1705312800, force_tz=True)
        assert result.tzinfo is not None

    def test_as_datetime_force_tz_adds_utc(self) -> None:
        dt = datetime(2024, 1, 15, 12, 0, 0)
        result = QueryValueCaster.as_datetime(dt, force_tz=True)
        assert result.tzinfo is timezone.utc

    def test_as_datetime_invalid_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid datetime"):
            QueryValueCaster.as_datetime("not-a-date", force_tz=True)

    # as_date
    def test_as_date_from_date(self) -> None:
        d = date(2024, 1, 15)
        assert QueryValueCaster.as_date(d) == d

    def test_as_date_from_datetime(self) -> None:
        dt = datetime(2024, 1, 15, 12, 0, 0)
        assert QueryValueCaster.as_date(dt) == date(2024, 1, 15)

    def test_as_date_from_iso_string(self) -> None:
        assert QueryValueCaster.as_date("2024-01-15") == date(2024, 1, 15)

    def test_as_date_invalid_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid date"):
            QueryValueCaster.as_date("not-a-date")

    # pass_through
    def test_pass_through_scalar(self) -> None:
        assert QueryValueCaster.pass_through(None) is None
        assert QueryValueCaster.pass_through(42) == 42
        assert QueryValueCaster.pass_through("foo") == "foo"
        assert QueryValueCaster.pass_through(True) is True

    def test_pass_through_non_scalar_coerces_to_str(self) -> None:
        assert QueryValueCaster.pass_through([1, 2]) == "[1, 2]"


class TestQueryFilterExpressionParser:
    """Tests for QueryFilterExpressionParser."""

    def test_parse_simple_predicate_eq_shortcut(self) -> None:
        expr = {"$fields": {"name": "foo"}}
        result = QueryFilterExpressionParser.parse(expr)
        assert isinstance(result, QueryAnd)
        assert len(result.items) == 1
        assert isinstance(result.items[0], QueryField)
        assert result.items[0].name == "name"
        assert result.items[0].op == "$eq"
        assert result.items[0].value == "foo"

    def test_parse_predicate_null_shortcut(self) -> None:
        expr = {"$fields": {"deleted_at": None}}
        result = QueryFilterExpressionParser.parse(expr)
        assert isinstance(result, QueryAnd)
        assert len(result.items) == 1
        f = result.items[0]
        assert isinstance(f, QueryField)
        assert f.name == "deleted_at"
        assert f.op == "$null"
        assert f.value is True

    def test_parse_predicate_in_shortcut(self) -> None:
        expr = {"$fields": {"status": ["a", "b"]}}
        result = QueryFilterExpressionParser.parse(expr)
        assert isinstance(result, QueryAnd)
        f = result.items[0]
        assert f.op == "$in"
        assert f.value == ["a", "b"]

    def test_parse_predicate_with_operators(self) -> None:
        expr = {"$fields": {"age": {"$gte": 18, "$lte": 65}}}
        result = QueryFilterExpressionParser.parse(expr)
        assert isinstance(result, QueryAnd)
        assert len(result.items) == 2

    def test_parse_conjunction(self) -> None:
        expr = {"$and": [{"$fields": {"a": 1}}, {"$fields": {"b": 2}}]}
        result = QueryFilterExpressionParser.parse(expr)
        assert isinstance(result, QueryAnd)
        assert len(result.items) == 2

    def test_parse_disjunction(self) -> None:
        expr = {"$or": [{"$fields": {"a": 1}}, {"$fields": {"b": 2}}]}
        result = QueryFilterExpressionParser.parse(expr)
        assert isinstance(result, QueryOr)
        assert len(result.items) == 2

    def test_parse_nested(self) -> None:
        expr = {
            "$and": [
                {"$fields": {"a": 1}},
                {"$or": [{"$fields": {"b": 2}}, {"$fields": {"c": 3}}]},
            ]
        }
        result = QueryFilterExpressionParser.parse(expr)
        assert isinstance(result, QueryAnd)
        assert len(result.items) == 2
        assert isinstance(result.items[1], QueryOr)
        assert len(result.items[1].items) == 2

    def test_parse_invalid_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid filter expression"):
            QueryFilterExpressionParser.parse({})
        with pytest.raises(ValueError, match="Invalid filter expression"):
            QueryFilterExpressionParser.parse({"$unknown": []})

    def test_parse_empty_field_map_raises(self) -> None:
        expr = {"$fields": {"x": {}}}
        with pytest.raises(ValueError, match="Empty field map"):
            QueryFilterExpressionParser.parse(expr)

    def test_parse_eq_invalid_value_raises(self) -> None:
        expr = {"$fields": {"x": {"$eq": [1, 2]}}}
        with pytest.raises(ValueError, match="Invalid value for"):
            QueryFilterExpressionParser.parse(expr)

    def test_parse_ord_invalid_value_raises(self) -> None:
        expr = {"$fields": {"x": {"$gte": "not-numeric"}}}
        with pytest.raises(ValueError, match="Invalid value for"):
            QueryFilterExpressionParser.parse(expr)

    def test_parse_in_invalid_value_raises(self) -> None:
        expr = {"$fields": {"x": {"$in": "not-a-list"}}}
        with pytest.raises(ValueError, match="Invalid value for"):
            QueryFilterExpressionParser.parse(expr)

    def test_parse_null_invalid_value_raises(self) -> None:
        expr = {"$fields": {"x": {"$null": "not-bool"}}}
        with pytest.raises(ValueError, match="Invalid value for"):
            QueryFilterExpressionParser.parse(expr)

    def test_parse_invalid_operator_raises(self) -> None:
        expr = {"$fields": {"x": {"$unknown": 1}}}
        with pytest.raises(ValueError, match="Invalid operator"):
            QueryFilterExpressionParser.parse(expr)


class TestQueryNodes:
    """Tests for QueryAnd, QueryOr, QueryField."""

    def test_query_field(self) -> None:
        f = QueryField("name", "$eq", "foo")
        assert f.name == "name"
        assert f.op == "$eq"
        assert f.value == "foo"

    def test_query_and(self) -> None:
        f1 = QueryField("a", "$eq", 1)
        f2 = QueryField("b", "$eq", 2)
        and_node = QueryAnd((f1, f2))
        assert len(and_node.items) == 2
        assert and_node.items[0] == f1
        assert and_node.items[1] == f2

    def test_query_or(self) -> None:
        f1 = QueryField("a", "$eq", 1)
        f2 = QueryField("b", "$eq", 2)
        or_node = QueryOr((f1, f2))
        assert len(or_node.items) == 2

    def test_query_expr_inheritance(self) -> None:
        assert issubclass(QueryField, QueryExpr)
        assert issubclass(QueryAnd, QueryExpr)
        assert issubclass(QueryOr, QueryExpr)
