"""Unit tests for forze.application.contracts.query.internal."""

from datetime import date, datetime, timezone
from uuid import UUID

import pytest

from forze.application.contracts.query import AggregatesExpressionParser
from forze.application.contracts.query.internal import (
    QueryAnd,
    QueryExpr,
    QueryField,
    QueryFilterExpressionParser,
    QueryOr,
    QueryValueCaster,
)
from forze.base.errors import CoreError, ValidationError

# ----------------------- #


class TestAggregatesExpressionParser:
    def test_parses_group_fields_and_computed_fields(self) -> None:
        parsed = AggregatesExpressionParser.parse(
            {
                "$fields": {"category": "category"},
                "$computed": {
                    "rows": {"$count": None},
                    "revenue": {"$sum": "price"},
                },
            },
        )

        assert parsed.aliases == {"category", "rows", "revenue"}
        assert parsed.fields[0].field == "category"
        assert parsed.computed_fields[0].function == "$count"

    def test_parses_group_fields_as_name_sequence(self) -> None:
        parsed = AggregatesExpressionParser.parse(
            {
                "$fields": ["detail_id", "warehouse_id"],
                "$computed": {"n": {"$count": None}},
            },
        )
        assert [f.alias for f in parsed.fields] == ["detail_id", "warehouse_id"]
        assert [f.field for f in parsed.fields] == ["detail_id", "warehouse_id"]

    def test_rejects_invalid_count_argument(self) -> None:
        with pytest.raises(CoreError, match="expects no field"):
            AggregatesExpressionParser.parse(
                {"$computed": {"rows": {"$count": "id"}}},
            )

    def test_parses_conditional_computed_fields(self) -> None:
        parsed = AggregatesExpressionParser.parse(
            {
                "$computed": {
                    "mid_rows": {
                        "$count": {
                            "filter": {
                                "$and": [
                                    {"$fields": {"price": {"$gte": 10}}},
                                    {"$fields": {"price": {"$lte": 20}}},
                                ],
                            },
                        },
                    },
                    "book_revenue": {
                        "$sum": {
                            "field": "price",
                            "filter": {"$fields": {"category": "books"}},
                        },
                    },
                },
            },
        )

        assert parsed.computed_fields[0].filter is not None
        assert parsed.computed_fields[1].field == "price"
        assert parsed.computed_fields[1].filter == {
            "$fields": {"category": "books"},
        }

    def test_rejects_conditional_value_aggregate_without_field(self) -> None:
        with pytest.raises(CoreError, match="requires a field"):
            AggregatesExpressionParser.parse(
                {
                    "$computed": {
                        "revenue": {
                            "$sum": {"filter": {"$fields": {"category": "books"}}},
                        },
                    },
                },
            )

    def test_rejects_duplicate_aliases(self) -> None:
        with pytest.raises(CoreError, match="Duplicate aggregate aliases"):
            AggregatesExpressionParser.parse(
                {
                    "$fields": {"total": "category"},
                    "$computed": {"total": {"$sum": "price"}},
                },
            )

    def test_rejects_invalid_group_keys_type(self) -> None:
        with pytest.raises(CoreError, match=r"Invalid aggregate \$fields"):
            AggregatesExpressionParser.parse(
                {
                    "$fields": "category",
                    "$computed": {"n": {"$count": None}},
                },
            )


# ----------------------- #


class TestQueryValueCaster:
    # as_bool
    def test_as_bool_true_variants(self) -> None:
        for v in (
            True,
            1,
            "true",
            "True",
            "TRUE",
            "t",
            "T",
            "1",
            "yes",
            "YES",
            "y",
            "on",
            "ON",
        ):
            assert QueryValueCaster.as_bool(v) is True

    def test_as_bool_false_variants(self) -> None:
        for v in (
            False,
            0,
            "false",
            "False",
            "f",
            "F",
            "0",
            "no",
            "NO",
            "n",
            "off",
            "OFF",
        ):
            assert QueryValueCaster.as_bool(v) is False

    def test_as_bool_whitespace_stripped(self) -> None:
        assert QueryValueCaster.as_bool("  true  ") is True
        assert QueryValueCaster.as_bool("  false  ") is False

    def test_as_bool_invalid_string_raises(self) -> None:
        with pytest.raises(ValidationError, match="Invalid boolean"):
            QueryValueCaster.as_bool("maybe")

    def test_as_bool_invalid_int_raises(self) -> None:
        with pytest.raises(ValidationError, match="Invalid boolean"):
            QueryValueCaster.as_bool(42)

    def test_as_bool_invalid_type_raises(self) -> None:
        with pytest.raises(ValidationError, match="Invalid boolean"):
            QueryValueCaster.as_bool(3.14)

    # as_uuid
    def test_as_uuid_from_uuid(self) -> None:
        u = UUID("550e8400-e29b-41d4-a716-446655440000")
        assert QueryValueCaster.as_uuid(u) == u

    def test_as_uuid_from_string(self) -> None:
        s = "550e8400-e29b-41d4-a716-446655440000"
        assert QueryValueCaster.as_uuid(s) == UUID(s)

    def test_as_uuid_invalid_string_raises(self) -> None:
        with pytest.raises(ValidationError, match="Invalid UUID"):
            QueryValueCaster.as_uuid("not-a-uuid")

    def test_as_uuid_invalid_type_raises(self) -> None:
        with pytest.raises(ValidationError, match="Invalid UUID"):
            QueryValueCaster.as_uuid(42)

    # as_int
    def test_as_int_from_int(self) -> None:
        assert QueryValueCaster.as_int(42) == 42

    def test_as_int_from_string(self) -> None:
        assert QueryValueCaster.as_int("42") == 42
        assert QueryValueCaster.as_int(" -7 ") == -7

    def test_as_int_from_integer_float(self) -> None:
        assert QueryValueCaster.as_int(42.0) == 42

    def test_as_int_bool_raises(self) -> None:
        with pytest.raises(ValidationError, match="got bool"):
            QueryValueCaster.as_int(True)

    def test_as_int_invalid_string_raises(self) -> None:
        with pytest.raises(ValidationError, match="Invalid int"):
            QueryValueCaster.as_int("abc")

    def test_as_int_invalid_type_raises(self) -> None:
        with pytest.raises(ValidationError, match="Invalid int"):
            QueryValueCaster.as_int([1])

    def test_as_int_non_integer_float_raises(self) -> None:
        with pytest.raises(ValidationError, match="Invalid int"):
            QueryValueCaster.as_int(3.14)

    # as_float
    def test_as_float_from_float(self) -> None:
        assert QueryValueCaster.as_float(3.14) == 3.14

    def test_as_float_from_int(self) -> None:
        assert QueryValueCaster.as_float(42) == 42.0

    def test_as_float_from_string(self) -> None:
        assert QueryValueCaster.as_float("3.14") == 3.14

    def test_as_float_comma_decimal(self) -> None:
        assert QueryValueCaster.as_float("3,14") == 3.14

    def test_as_float_bool_raises(self) -> None:
        with pytest.raises(ValidationError, match="got bool"):
            QueryValueCaster.as_float(True)

    def test_as_float_invalid_string_raises(self) -> None:
        with pytest.raises(ValidationError, match="Invalid float"):
            QueryValueCaster.as_float("abc")

    def test_as_float_invalid_type_raises(self) -> None:
        with pytest.raises(ValidationError, match="Invalid float"):
            QueryValueCaster.as_float([1.0])

    # _to_seconds
    def test_to_seconds_seconds(self) -> None:
        assert QueryValueCaster._to_seconds(1000) == 1000.0

    def test_to_seconds_milliseconds(self) -> None:
        result = QueryValueCaster._to_seconds(1_700_000_000_000)
        assert abs(result - 1_700_000_000.0) < 0.01

    def test_to_seconds_microseconds(self) -> None:
        result = QueryValueCaster._to_seconds(1_700_000_000_000_000)
        assert abs(result - 1_700_000_000.0) < 0.01

    def test_to_seconds_nanoseconds(self) -> None:
        result = QueryValueCaster._to_seconds(1_700_000_000_000_000_000)
        assert abs(result - 1_700_000_000.0) < 0.01

    # _like_num
    def test_like_num_positive(self) -> None:
        assert QueryValueCaster._like_num(42) is True
        assert QueryValueCaster._like_num("3.14") is True
        assert QueryValueCaster._like_num(0) is True

    def test_like_num_negative(self) -> None:
        assert QueryValueCaster._like_num("abc") is False
        assert QueryValueCaster._like_num("") is False

    # as_datetime
    def test_as_datetime_from_datetime(self) -> None:
        dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        assert QueryValueCaster.as_datetime(dt, force_tz=True) == dt

    def test_as_datetime_from_iso_string(self) -> None:
        result = QueryValueCaster.as_datetime("2024-01-15T12:00:00Z", force_tz=True)
        assert result.year == 2024
        assert result.tzinfo is not None

    def test_as_datetime_from_timestamp_int(self) -> None:
        result = QueryValueCaster.as_datetime(1705312800, force_tz=True)
        assert result.tzinfo is not None

    def test_as_datetime_from_timestamp_string(self) -> None:
        result = QueryValueCaster.as_datetime("1705312800", force_tz=True)
        assert result.tzinfo is not None

    def test_as_datetime_from_float_timestamp_string(self) -> None:
        result = QueryValueCaster.as_datetime("1705312800.123", force_tz=True)
        assert result.tzinfo is not None

    def test_as_datetime_force_tz_adds_utc_to_naive(self) -> None:
        dt = datetime(2024, 1, 15, 12, 0, 0)
        result = QueryValueCaster.as_datetime(dt, force_tz=True)
        assert result.tzinfo is timezone.utc

    def test_as_datetime_no_force_tz_strips_tz(self) -> None:
        dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        result = QueryValueCaster.as_datetime(dt, force_tz=False)
        assert result.tzinfo is None

    def test_as_datetime_invalid_string_raises(self) -> None:
        with pytest.raises(ValidationError, match="Invalid datetime"):
            QueryValueCaster.as_datetime("not-a-date", force_tz=True)

    def test_as_datetime_invalid_type_raises(self) -> None:
        with pytest.raises(ValidationError, match="Invalid datetime"):
            QueryValueCaster.as_datetime([2024], force_tz=True)

    def test_as_datetime_from_milliseconds(self) -> None:
        result = QueryValueCaster.as_datetime(1_705_312_800_000, force_tz=True)
        assert result.year >= 2024

    # as_date
    def test_as_date_from_date(self) -> None:
        d = date(2024, 1, 15)
        assert QueryValueCaster.as_date(d) == d

    def test_as_date_from_datetime(self) -> None:
        dt = datetime(2024, 1, 15, 12, 0, 0)
        assert QueryValueCaster.as_date(dt) == date(2024, 1, 15)

    def test_as_date_from_iso_string(self) -> None:
        assert QueryValueCaster.as_date("2024-01-15") == date(2024, 1, 15)

    def test_as_date_from_timestamp(self) -> None:
        result = QueryValueCaster.as_date(1705312800)
        assert isinstance(result, date)

    def test_as_date_invalid_string_raises(self) -> None:
        with pytest.raises(ValidationError, match="Invalid date"):
            QueryValueCaster.as_date("not-a-date")

    def test_as_date_invalid_type_raises(self) -> None:
        with pytest.raises(ValidationError, match="Invalid date"):
            QueryValueCaster.as_date([2024])

    # pass_through
    def test_pass_through_none(self) -> None:
        assert QueryValueCaster.pass_through(None) is None

    def test_pass_through_scalar(self) -> None:
        assert QueryValueCaster.pass_through(42) == 42
        assert QueryValueCaster.pass_through("foo") == "foo"
        assert QueryValueCaster.pass_through(True) is True

    def test_pass_through_non_scalar_to_str(self) -> None:
        assert QueryValueCaster.pass_through([1, 2]) == "[1, 2]"
        assert QueryValueCaster.pass_through({"a": 1}) == "{'a': 1}"


# ----------------------- #


class TestQueryFilterExpressionParser:
    # Predicate shortcuts
    def test_parse_eq_shortcut(self) -> None:
        expr = {"$fields": {"name": "foo"}}
        result = QueryFilterExpressionParser.parse(expr)
        assert isinstance(result, QueryAnd)
        f = result.items[0]
        assert isinstance(f, QueryField)
        assert f.op == "$eq" and f.value == "foo"

    def test_parse_null_shortcut(self) -> None:
        expr = {"$fields": {"deleted_at": None}}
        result = QueryFilterExpressionParser.parse(expr)
        f = result.items[0]
        assert isinstance(f, QueryField)
        assert f.op == "$null" and f.value is True

    def test_parse_in_shortcut(self) -> None:
        expr = {"$fields": {"status": ["a", "b"]}}
        result = QueryFilterExpressionParser.parse(expr)
        f = result.items[0]
        assert isinstance(f, QueryField)
        assert f.op == "$in" and f.value == ["a", "b"]

    # Operator-based predicates
    def test_parse_eq_operator(self) -> None:
        expr = {"$fields": {"x": {"$eq": 42}}}
        result = QueryFilterExpressionParser.parse(expr)
        f = result.items[0]
        assert isinstance(f, QueryField)
        assert f.op == "$eq"

    def test_parse_neq_operator(self) -> None:
        expr = {"$fields": {"x": {"$neq": 42}}}
        result = QueryFilterExpressionParser.parse(expr)
        f = result.items[0]
        assert isinstance(f, QueryField)
        assert f.op == "$neq"

    def test_parse_ord_operators(self) -> None:
        for op in ("$gt", "$gte", "$lt", "$lte"):
            expr = {"$fields": {"age": {op: 18}}}
            result = QueryFilterExpressionParser.parse(expr)
            f = result.items[0]
            assert isinstance(f, QueryField)
            assert f.op == op

    def test_parse_in_operator(self) -> None:
        expr = {"$fields": {"x": {"$in": [1, 2, 3]}}}
        result = QueryFilterExpressionParser.parse(expr)
        f = result.items[0]
        assert isinstance(f, QueryField)
        assert f.op == "$in"

    def test_parse_nin_operator(self) -> None:
        expr = {"$fields": {"x": {"$nin": [1, 2]}}}
        result = QueryFilterExpressionParser.parse(expr)
        f = result.items[0]
        assert isinstance(f, QueryField)
        assert f.op == "$nin"

    def test_parse_null_operator(self) -> None:
        expr = {"$fields": {"x": {"$null": True}}}
        result = QueryFilterExpressionParser.parse(expr)
        f = result.items[0]
        assert isinstance(f, QueryField)
        assert f.op == "$null"

    def test_parse_empty_operator(self) -> None:
        expr = {"$fields": {"x": {"$empty": False}}}
        result = QueryFilterExpressionParser.parse(expr)
        f = result.items[0]
        assert isinstance(f, QueryField)
        assert f.op == "$empty"

    def test_parse_set_rel_operators(self) -> None:
        for op in ("$superset", "$subset", "$disjoint", "$overlaps"):
            expr = {"$fields": {"tags": {op: ["a", "b"]}}}
            result = QueryFilterExpressionParser.parse(expr)
            f = result.items[0]
            assert isinstance(f, QueryField)
            assert f.op == op

    # Conjunction / disjunction
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
        assert isinstance(result.items[1], QueryOr)

    # Multiple operators per field
    def test_parse_multiple_ops_same_field(self) -> None:
        expr = {"$fields": {"age": {"$gte": 18, "$lte": 65}}}
        result = QueryFilterExpressionParser.parse(expr)
        assert isinstance(result, QueryAnd)
        assert len(result.items) == 2

    # Validation errors
    def test_parse_invalid_expression_raises(self) -> None:
        with pytest.raises(ValidationError, match="Invalid filter expression"):
            QueryFilterExpressionParser.parse({})

    def test_parse_unknown_key_raises(self) -> None:
        with pytest.raises(ValidationError, match="Invalid filter expression"):
            QueryFilterExpressionParser.parse({"$unknown": []})

    def test_parse_empty_field_map_raises(self) -> None:
        with pytest.raises(ValidationError, match="Empty field map"):
            QueryFilterExpressionParser.parse({"$fields": {"x": {}}})

    def test_parse_eq_invalid_value_raises(self) -> None:
        with pytest.raises(ValidationError, match="Invalid value for"):
            QueryFilterExpressionParser.parse({"$fields": {"x": {"$eq": [1, 2]}}})

    def test_parse_ord_invalid_value_raises(self) -> None:
        with pytest.raises(ValidationError, match="Invalid value for"):
            QueryFilterExpressionParser.parse(
                {"$fields": {"x": {"$gte": "not-numeric"}}}
            )

    def test_parse_in_invalid_value_raises(self) -> None:
        with pytest.raises(ValidationError, match="Invalid value for"):
            QueryFilterExpressionParser.parse({"$fields": {"x": {"$in": "not-a-list"}}})

    def test_parse_null_invalid_value_raises(self) -> None:
        with pytest.raises(ValidationError, match="Invalid value for"):
            QueryFilterExpressionParser.parse({"$fields": {"x": {"$null": "not-bool"}}})

    def test_parse_invalid_operator_raises(self) -> None:
        with pytest.raises(ValidationError, match="Invalid operator"):
            QueryFilterExpressionParser.parse({"$fields": {"x": {"$unknown": 1}}})

    def test_parse_set_rel_invalid_value_raises(self) -> None:
        with pytest.raises(ValidationError, match="Invalid value for"):
            QueryFilterExpressionParser.parse(
                {"$fields": {"x": {"$superset": "not-list"}}}
            )

    # Validate null=True with other ops
    def test_null_true_with_other_ops_raises(self) -> None:
        with pytest.raises(ValidationError, match="cannot be null"):
            QueryFilterExpressionParser.parse(
                {"$fields": {"x": {"$null": True, "$eq": 1}}}
            )

    def test_null_false_with_other_ops_ok(self) -> None:
        result = QueryFilterExpressionParser.parse(
            {"$fields": {"x": {"$null": False, "$eq": 1}}}
        )
        assert isinstance(result, QueryAnd)

    # Validate empty=True with other ops
    def test_empty_true_with_other_ops_raises(self) -> None:
        with pytest.raises(ValidationError, match="cannot be empty"):
            QueryFilterExpressionParser.parse(
                {"$fields": {"x": {"$empty": True, "$eq": 1}}}
            )

    def test_empty_false_with_other_ops_ok(self) -> None:
        result = QueryFilterExpressionParser.parse(
            {"$fields": {"x": {"$empty": False, "$eq": 1}}}
        )
        assert isinstance(result, QueryAnd)

    # Multiple fields in predicate
    def test_multiple_fields_in_predicate(self) -> None:
        expr = {"$fields": {"a": 1, "b": "hello"}}
        result = QueryFilterExpressionParser.parse(expr)
        assert isinstance(result, QueryAnd)
        assert len(result.items) == 2


# ----------------------- #


class TestQueryNodes:
    def test_query_field(self) -> None:
        f = QueryField("name", "$eq", "foo")
        assert f.name == "name"
        assert f.op == "$eq"
        assert f.value == "foo"

    def test_query_and(self) -> None:
        f1 = QueryField("a", "$eq", 1)
        f2 = QueryField("b", "$eq", 2)
        node = QueryAnd((f1, f2))
        assert len(node.items) == 2

    def test_query_or(self) -> None:
        f1 = QueryField("a", "$eq", 1)
        f2 = QueryField("b", "$eq", 2)
        node = QueryOr((f1, f2))
        assert len(node.items) == 2

    def test_inheritance(self) -> None:
        assert issubclass(QueryField, QueryExpr)
        assert issubclass(QueryAnd, QueryExpr)
        assert issubclass(QueryOr, QueryExpr)
