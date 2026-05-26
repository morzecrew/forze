"""Unit tests for forze.application.contracts.querying.internal."""

from datetime import date, datetime, timezone
from uuid import UUID

import pytest

from forze.application.contracts.querying import AggregatesExpressionParser
from forze.application.contracts.querying.internal import (
    ELEM_SCALAR_FIELD,
    GroupRef,
    GroupTrunc,
    QueryAnd,
    QueryCompare,
    QueryElem,
    QueryExpr,
    QueryField,
    QueryFilterExpressionParser,
    QueryFilterLimits,
    QueryNot,
    QueryOr,
    QueryValueCaster,
)
from forze.base.errors import CoreError, ValidationError

# ----------------------- #


class TestAggregatesExpressionParser:
    def test_parses_group_fields_and_computed_fields(self) -> None:
        parsed = AggregatesExpressionParser.parse(
            {
                "$groups": {"category": "category"},
                "$computed": {
                    "rows": {"$count": None},
                    "revenue": {"$sum": "price"},
                },
            },
        )

        assert parsed.aliases == {"category", "rows", "revenue"}
        assert isinstance(parsed.groups[0].expr, GroupRef)
        assert parsed.groups[0].expr.field == "category"
        assert parsed.computed_fields[0].function == "$count"

    def test_parses_group_fields_as_name_sequence(self) -> None:
        parsed = AggregatesExpressionParser.parse(
            {
                "$groups": ["detail_id", "warehouse_id"],
                "$computed": {"n": {"$count": None}},
            },
        )
        assert [g.alias for g in parsed.groups] == ["detail_id", "warehouse_id"]
        assert [g.expr.field for g in parsed.groups] == ["detail_id", "warehouse_id"]

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
                                    {"$values": {"price": {"$gte": 10}}},
                                    {"$values": {"price": {"$lte": 20}}},
                                ],
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

        assert parsed.computed_fields[0].filter is not None
        assert parsed.computed_fields[1].field == "price"
        assert parsed.computed_fields[1].filter == {
            "$values": {"category": "books"},
        }

    def test_rejects_conditional_value_aggregate_without_field(self) -> None:
        with pytest.raises(CoreError, match="requires a field"):
            AggregatesExpressionParser.parse(
                {
                    "$computed": {
                        "revenue": {
                            "$sum": {"filter": {"$values": {"category": "books"}}},
                        },
                    },
                },
            )

    def test_rejects_duplicate_aliases(self) -> None:
        with pytest.raises(CoreError, match="Duplicate aggregate aliases"):
            AggregatesExpressionParser.parse(
                {
                    "$groups": {"total": "category"},
                    "$computed": {"total": {"$sum": "price"}},
                },
            )

    def test_rejects_invalid_group_keys_type(self) -> None:
        with pytest.raises(CoreError, match=r"Invalid aggregate \$groups"):
            AggregatesExpressionParser.parse(
                {
                    "$groups": "category",
                    "$computed": {"n": {"$count": None}},
                },
            )

    def test_parses_trunc_group(self) -> None:
        parsed = AggregatesExpressionParser.parse(
            {
                "$groups": {
                    "item": "item_id",
                    "day_start": {
                        "$trunc": {
                            "field": "ts",
                            "unit": "day",
                            "timezone": "+3",
                        },
                    },
                },
                "$computed": {"avg_p": {"$avg": "price"}},
            },
        )
        assert len(parsed.groups) == 2
        trunc = parsed.groups[1]
        assert trunc.alias == "day_start"
        assert isinstance(trunc.expr, GroupTrunc)
        assert trunc.expr.field == "ts"
        assert trunc.expr.unit == "day"
        assert trunc.expr.timezone.mode == "fixed"
        assert "day_start" in parsed.aliases

    def test_parses_multiple_trunc_groups(self) -> None:
        parsed = AggregatesExpressionParser.parse(
            {
                "$groups": {
                    "by_day": {"$trunc": {"field": "ts", "unit": "day"}},
                    "by_hour": {"$trunc": {"field": "ts", "unit": "hour"}},
                },
                "$computed": {"n": {"$count": None}},
            },
        )
        assert len(parsed.groups) == 2
        assert all(isinstance(g.expr, GroupTrunc) for g in parsed.groups)

    def test_rejects_duplicate_trunc_alias(self) -> None:
        with pytest.raises(CoreError, match="Duplicate aggregate aliases"):
            AggregatesExpressionParser.parse(
                {
                    "$groups": {
                        "bucket": "item_id",
                        "week": {"$trunc": {"field": "ts", "unit": "hour"}},
                    },
                    "$computed": {"bucket": {"$count": None}},
                },
            )

    def test_rejects_unknown_group_operator(self) -> None:
        with pytest.raises(CoreError, match="Invalid \\$groups operator"):
            AggregatesExpressionParser.parse(
                {
                    "$groups": {"x": {"$unknown": {"field": "ts"}}},
                    "$computed": {"n": {"$count": None}},
                },
            )

    def test_rejects_bare_trunc_spec_without_operator(self) -> None:
        with pytest.raises(CoreError, match="exactly one operator"):
            AggregatesExpressionParser.parse(
                {
                    "$groups": {"day": {"field": "ts", "unit": "day"}},
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
        expr = {"$values": {"name": "foo"}}
        result = QueryFilterExpressionParser.parse(expr)
        assert isinstance(result, QueryAnd)
        f = result.items[0]
        assert isinstance(f, QueryField)
        assert f.op == "$eq" and f.value == "foo"

    def test_parse_null_shortcut(self) -> None:
        expr = {"$values": {"deleted_at": None}}
        result = QueryFilterExpressionParser.parse(expr)
        f = result.items[0]
        assert isinstance(f, QueryField)
        assert f.op == "$null" and f.value is True

    def test_parse_in_shortcut(self) -> None:
        expr = {"$values": {"status": ["a", "b"]}}
        result = QueryFilterExpressionParser.parse(expr)
        f = result.items[0]
        assert isinstance(f, QueryField)
        assert f.op == "$in" and f.value == ["a", "b"]

    # Operator-based predicates
    def test_parse_eq_operator(self) -> None:
        expr = {"$values": {"x": {"$eq": 42}}}
        result = QueryFilterExpressionParser.parse(expr)
        f = result.items[0]
        assert isinstance(f, QueryField)
        assert f.op == "$eq"

    def test_parse_neq_operator(self) -> None:
        expr = {"$values": {"x": {"$neq": 42}}}
        result = QueryFilterExpressionParser.parse(expr)
        f = result.items[0]
        assert isinstance(f, QueryField)
        assert f.op == "$neq"

    def test_parse_ord_operators(self) -> None:
        for op in ("$gt", "$gte", "$lt", "$lte"):
            expr = {"$values": {"age": {op: 18}}}
            result = QueryFilterExpressionParser.parse(expr)
            f = result.items[0]
            assert isinstance(f, QueryField)
            assert f.op == op

    def test_parse_in_operator(self) -> None:
        expr = {"$values": {"x": {"$in": [1, 2, 3]}}}
        result = QueryFilterExpressionParser.parse(expr)
        f = result.items[0]
        assert isinstance(f, QueryField)
        assert f.op == "$in"

    def test_parse_nin_operator(self) -> None:
        expr = {"$values": {"x": {"$nin": [1, 2]}}}
        result = QueryFilterExpressionParser.parse(expr)
        f = result.items[0]
        assert isinstance(f, QueryField)
        assert f.op == "$nin"

    def test_parse_null_operator(self) -> None:
        expr = {"$values": {"x": {"$null": True}}}
        result = QueryFilterExpressionParser.parse(expr)
        f = result.items[0]
        assert isinstance(f, QueryField)
        assert f.op == "$null"

    def test_parse_empty_operator(self) -> None:
        expr = {"$values": {"x": {"$empty": False}}}
        result = QueryFilterExpressionParser.parse(expr)
        f = result.items[0]
        assert isinstance(f, QueryField)
        assert f.op == "$empty"

    def test_parse_set_rel_operators(self) -> None:
        for op in ("$superset", "$subset", "$disjoint", "$overlaps"):
            expr = {"$values": {"tags": {op: ["a", "b"]}}}
            result = QueryFilterExpressionParser.parse(expr)
            f = result.items[0]
            assert isinstance(f, QueryField)
            assert f.op == op

    def test_parse_ilike_single_pattern(self) -> None:
        result = QueryFilterExpressionParser.parse(
            {"$values": {"title": {"$ilike": "%road%"}}},
        )
        f = result.items[0]
        assert isinstance(f, QueryField)
        assert f.op == "$ilike"
        assert f.value == "%road%"

    def test_parse_ilike_sequence_desugars_to_or(self) -> None:
        result = QueryFilterExpressionParser.parse(
            {"$values": {"title": {"$ilike": ["%road%", "%map%"]}}},
        )
        assert isinstance(result, QueryAnd)
        or_node = result.items[0]
        assert isinstance(or_node, QueryOr)
        assert len(or_node.items) == 2
        assert all(isinstance(i, QueryField) and i.op == "$ilike" for i in or_node.items)

    def test_parse_element_any_ilike(self) -> None:
        result = QueryFilterExpressionParser.parse(
            {
                "$values": {
                    "items": {
                        "$any": {"$values": {"name": {"$ilike": "%foo%"}}},
                    },
                },
            },
        )
        elem = result.items[0]
        assert isinstance(elem, QueryElem)
        inner = elem.inner
        assert isinstance(inner, QueryAnd)
        field = inner.items[0]
        assert isinstance(field, QueryField)
        assert field.op == "$ilike"

    def test_parse_regex_rejects_unsafe_pattern(self) -> None:
        with pytest.raises(ValidationError, match="nested quantifiers"):
            QueryFilterExpressionParser.parse(
                {"$values": {"title": {"$regex": "(a+)+"}}},
            )

    # Conjunction / disjunction
    def test_parse_conjunction(self) -> None:
        expr = {"$and": [{"$values": {"a": 1}}, {"$values": {"b": 2}}]}
        result = QueryFilterExpressionParser.parse(expr)
        assert isinstance(result, QueryAnd)
        assert len(result.items) == 2

    def test_parse_disjunction(self) -> None:
        expr = {"$or": [{"$values": {"a": 1}}, {"$values": {"b": 2}}]}
        result = QueryFilterExpressionParser.parse(expr)
        assert isinstance(result, QueryOr)
        assert len(result.items) == 2

    def test_parse_nested(self) -> None:
        expr = {
            "$and": [
                {"$values": {"a": 1}},
                {"$or": [{"$values": {"b": 2}}, {"$values": {"c": 3}}]},
            ]
        }
        result = QueryFilterExpressionParser.parse(expr)
        assert isinstance(result, QueryAnd)
        assert isinstance(result.items[1], QueryOr)

    # Multiple operators per field
    def test_parse_multiple_ops_same_field(self) -> None:
        expr = {"$values": {"age": {"$gte": 18, "$lte": 65}}}
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
        with pytest.raises(ValidationError, match="Empty \\$values"):
            QueryFilterExpressionParser.parse({"$values": {"x": {}}})

    def test_parse_eq_invalid_value_raises(self) -> None:
        with pytest.raises(ValidationError, match="Invalid value for"):
            QueryFilterExpressionParser.parse({"$values": {"x": {"$eq": [1, 2]}}})

    def test_parse_ord_invalid_value_raises(self) -> None:
        with pytest.raises(ValidationError, match="Invalid value for"):
            QueryFilterExpressionParser.parse(
                {"$values": {"x": {"$gte": "not-numeric"}}}
            )

    def test_parse_in_invalid_value_raises(self) -> None:
        with pytest.raises(ValidationError, match="Invalid value for"):
            QueryFilterExpressionParser.parse({"$values": {"x": {"$in": "not-a-list"}}})

    def test_parse_null_invalid_value_raises(self) -> None:
        with pytest.raises(ValidationError, match="Invalid value for"):
            QueryFilterExpressionParser.parse({"$values": {"x": {"$null": "not-bool"}}})

    def test_parse_invalid_operator_raises(self) -> None:
        with pytest.raises(ValidationError, match="Invalid operator"):
            QueryFilterExpressionParser.parse({"$values": {"x": {"$unknown": 1}}})

    def test_parse_set_rel_invalid_value_raises(self) -> None:
        with pytest.raises(ValidationError, match="Invalid value for"):
            QueryFilterExpressionParser.parse(
                {"$values": {"x": {"$superset": "not-list"}}}
            )

    # Validate null=True with other ops
    def test_null_true_with_other_ops_raises(self) -> None:
        with pytest.raises(ValidationError, match="cannot be null"):
            QueryFilterExpressionParser.parse(
                {"$values": {"x": {"$null": True, "$eq": 1}}}
            )

    def test_null_false_with_other_ops_ok(self) -> None:
        result = QueryFilterExpressionParser.parse(
            {"$values": {"x": {"$null": False, "$eq": 1}}}
        )
        assert isinstance(result, QueryAnd)

    # Validate empty=True with other ops
    def test_empty_true_with_other_ops_raises(self) -> None:
        with pytest.raises(ValidationError, match="cannot be empty"):
            QueryFilterExpressionParser.parse(
                {"$values": {"x": {"$empty": True, "$eq": 1}}}
            )

    def test_empty_false_with_other_ops_ok(self) -> None:
        result = QueryFilterExpressionParser.parse(
            {"$values": {"x": {"$empty": False, "$eq": 1}}}
        )
        assert isinstance(result, QueryAnd)

    # Multiple fields in predicate
    def test_multiple_fields_in_predicate(self) -> None:
        expr = {"$values": {"a": 1, "b": "hello"}}
        result = QueryFilterExpressionParser.parse(expr)
        assert isinstance(result, QueryAnd)
        assert len(result.items) == 2


class TestQueryCompareExpressionParser:
    def test_parse_compare_eq_shortcut(self) -> None:
        result = QueryFilterExpressionParser.parse(
            {"$fields": {"starts_at": "ends_at"}},
        )
        assert isinstance(result, QueryAnd)
        node = result.items[0]
        assert isinstance(node, QueryCompare)
        assert node.left == "starts_at"
        assert node.op == "$eq"
        assert node.right == "ends_at"

    def test_parse_compare_ord_operator(self) -> None:
        result = QueryFilterExpressionParser.parse(
            {"$fields": {"a": {"$lte": "b"}}},
        )
        node = result.items[0]
        assert isinstance(node, QueryCompare)
        assert node.op == "$lte"

    def test_parse_compare_multiple_ops_same_left(self) -> None:
        result = QueryFilterExpressionParser.parse(
            {"$fields": {"a": {"$gte": "b", "$lte": "c"}}},
        )
        assert isinstance(result, QueryAnd)
        assert len(result.items) == 2
        assert all(isinstance(n, QueryCompare) for n in result.items)

    def test_parse_compare_multiple_left_fields(self) -> None:
        result = QueryFilterExpressionParser.parse(
            {"$fields": {"a": {"$eq": "b"}, "x": {"$neq": "y"}}},
        )
        assert isinstance(result, QueryAnd)
        assert len(result.items) == 2

    def test_parse_compare_in_conjunction(self) -> None:
        result = QueryFilterExpressionParser.parse(
            {
                "$and": [
                    {"$values": {"status": "active"}},
                    {"$fields": {"starts_at": {"$lte": "ends_at"}}},
                ],
            },
        )
        assert isinstance(result, QueryAnd)
        assert len(result.items) == 2

    def test_parse_combined_values_and_fields(self) -> None:
        result = QueryFilterExpressionParser.parse(
            {
                "$values": {"status": "active"},
                "$fields": {"starts_at": {"$lte": "ends_at"}},
            },
        )
        assert isinstance(result, QueryAnd)
        assert len(result.items) == 2
        assert isinstance(result.items[0], QueryField)
        assert isinstance(result.items[1], QueryCompare)

    def test_parse_constraint_with_and_raises(self) -> None:
        with pytest.raises(ValidationError, match="cannot mix"):
            QueryFilterExpressionParser.parse(
                {"$and": [], "$values": {"a": 1}},
            )

    def test_parse_empty_fields_map_raises(self) -> None:
        with pytest.raises(ValidationError, match="Empty \\$fields"):
            QueryFilterExpressionParser.parse({"$fields": {}})

    def test_parse_compare_invalid_operator_raises(self) -> None:
        with pytest.raises(ValidationError, match="Invalid field compare operator"):
            QueryFilterExpressionParser.parse(
                {"$fields": {"a": {"$in": "b"}}},
            )

    def test_parse_compare_scalar_rhs_raises(self) -> None:
        with pytest.raises(ValidationError, match="field path string"):
            QueryFilterExpressionParser.parse(
                {"$fields": {"a": {"$eq": 1}}},
            )

    def test_parse_compare_empty_rhs_raises(self) -> None:
        with pytest.raises(ValidationError, match="field path string"):
            QueryFilterExpressionParser.parse(
                {"$fields": {"a": {"$eq": ""}}},
            )

    def test_parse_exceeds_max_depth(self) -> None:
        expr: dict[str, object] = {"$values": {"x": 1}}
        for _ in range(5):
            expr = {"$and": [expr]}
        parser = QueryFilterExpressionParser(
            limits=QueryFilterLimits(max_depth=3, max_clauses=100, max_in_size=100),
        )
        with pytest.raises(ValidationError, match="maximum depth"):
            parser.parse_filter(expr)  # type: ignore[arg-type]

    def test_parse_exceeds_max_clauses(self) -> None:
        parser = QueryFilterExpressionParser(
            limits=QueryFilterLimits(max_depth=32, max_clauses=2, max_in_size=100),
        )
        expr = {
            "$and": [
                {"$values": {"a": 1}},
                {"$values": {"b": 2}},
                {"$values": {"c": 3}},
            ],
        }
        with pytest.raises(ValidationError, match="maximum clause count"):
            parser.parse_filter(expr)  # type: ignore[arg-type]

    def test_parse_exceeds_max_in_size(self) -> None:
        parser = QueryFilterExpressionParser(
            limits=QueryFilterLimits(max_depth=32, max_clauses=256, max_in_size=2),
        )
        with pytest.raises(ValidationError, match="maximum size"):
            parser.parse_filter({"$values": {"x": [1, 2, 3]}})  # type: ignore[arg-type]

    def test_parse_exceeds_max_in_size_operator_form(self) -> None:
        parser = QueryFilterExpressionParser(
            limits=QueryFilterLimits(max_depth=32, max_clauses=256, max_in_size=1),
        )
        with pytest.raises(ValidationError, match="maximum size"):
            parser.parse_filter({"$values": {"x": {"$in": [1, 2]}}})  # type: ignore[arg-type]

    def test_parse_not(self) -> None:
        result = QueryFilterExpressionParser.parse(
            {"$not": {"$values": {"status": "archived"}}},
        )
        assert isinstance(result, QueryNot)
        assert isinstance(result.item, QueryAnd)

    def test_parse_not_nested(self) -> None:
        result = QueryFilterExpressionParser.parse(
            {
                "$not": {
                    "$or": [
                        {"$values": {"a": 1}},
                        {"$values": {"b": 2}},
                    ],
                },
            },
        )
        assert isinstance(result, QueryNot)
        assert isinstance(result.item, QueryOr)

    def test_parse_not_mix_constraint_raises(self) -> None:
        with pytest.raises(ValidationError, match="cannot mix"):
            QueryFilterExpressionParser.parse(
                {"$not": {"$values": {"a": 1}}, "$values": {"b": 2}},
            )

    def test_parse_element_any_scalar_shortcut(self) -> None:
        result = QueryFilterExpressionParser.parse(
            {"$values": {"tags": {"$any": "urgent"}}},
        )
        assert isinstance(result, QueryAnd)
        elem = result.items[0]
        assert isinstance(elem, QueryElem)
        assert elem.path == "tags"
        assert elem.quantifier == "$any"
        assert isinstance(elem.inner, QueryField)
        assert elem.inner.name == ELEM_SCALAR_FIELD
        assert elem.inner.op == "$eq"

    def test_parse_element_any_scalar_op_map(self) -> None:
        result = QueryFilterExpressionParser.parse(
            {"$values": {"scores": {"$any": {"$gte": 10}}}},
        )
        elem = result.items[0]
        assert isinstance(elem, QueryElem)
        assert isinstance(elem.inner, QueryField)
        assert elem.inner.op == "$gte"

    def test_parse_element_any_object_values(self) -> None:
        result = QueryFilterExpressionParser.parse(
            {
                "$values": {
                    "items": {
                        "$any": {
                            "$values": {
                                "status": "open",
                                "qty": {"$gte": 1},
                            },
                        },
                    },
                },
            },
        )
        elem = result.items[0]
        assert isinstance(elem, QueryElem)
        assert isinstance(elem.inner, QueryAnd)
        assert len(elem.inner.items) == 2

    def test_parse_element_all_and_none(self) -> None:
        all_result = QueryFilterExpressionParser.parse(
            {"$values": {"tags": {"$all": {"$eq": "x"}}}},
        )
        none_result = QueryFilterExpressionParser.parse(
            {"$values": {"tags": {"$none": "spam"}}},
        )
        assert isinstance(all_result.items[0], QueryElem)
        assert all_result.items[0].quantifier == "$all"
        assert isinstance(none_result.items[0], QueryElem)
        assert none_result.items[0].quantifier == "$none"

    def test_parse_element_invalid_nested_quantifier_raises(self) -> None:
        with pytest.raises(ValidationError, match="Nested element quantifiers"):
            QueryFilterExpressionParser.parse(
                {"$values": {"tags": {"$any": {"$all": "x"}}}},
            )

    def test_parse_element_invalid_operator_raises(self) -> None:
        with pytest.raises(ValidationError, match="Element constraint must be"):
            QueryFilterExpressionParser.parse(
                {"$values": {"tags": {"$any": {"$in": ["a"]}}}},
            )


# ----------------------- #


class TestMockElementQuantifiers:
    def test_match_elem_any_scalar(self) -> None:
        from forze_mock.adapters import _match_expr

        expr = QueryFilterExpressionParser.parse(
            {"$values": {"tags": {"$any": "urgent"}}},
        )
        assert _match_expr({"tags": ["ops", "urgent"]}, expr) is True
        assert _match_expr({"tags": ["ops"]}, expr) is False
        assert _match_expr({"tags": []}, expr) is False

    def test_match_elem_any_scalar_gte(self) -> None:
        from forze_mock.adapters import _match_expr

        expr = QueryFilterExpressionParser.parse(
            {"$values": {"scores": {"$any": {"$gte": 10}}}},
        )
        assert _match_expr({"scores": [5, 15]}, expr) is True
        assert _match_expr({"scores": [1, 2]}, expr) is False

    def test_match_elem_all_vacuous_empty(self) -> None:
        from forze_mock.adapters import _match_expr

        expr = QueryFilterExpressionParser.parse(
            {"$values": {"tags": {"$all": {"$eq": "x"}}}},
        )
        assert _match_expr({"tags": []}, expr) is True
        assert _match_expr({}, expr) is True

    def test_match_elem_all_requires_every_element(self) -> None:
        from forze_mock.adapters import _match_expr

        expr = QueryFilterExpressionParser.parse(
            {"$values": {"tags": {"$all": {"$eq": "ops"}}}},
        )
        assert _match_expr({"tags": ["ops"]}, expr) is True
        assert _match_expr({"tags": ["ops", "urgent"]}, expr) is False

    def test_match_elem_none_scalar(self) -> None:
        from forze_mock.adapters import _match_expr

        expr = QueryFilterExpressionParser.parse(
            {"$values": {"tags": {"$none": "urgent"}}},
        )
        assert _match_expr({"tags": ["api"]}, expr) is True
        assert _match_expr({"tags": ["urgent"]}, expr) is False

    def test_match_elem_any_object_array(self) -> None:
        from forze_mock.adapters import _match_expr

        expr = QueryFilterExpressionParser.parse(
            {
                "$values": {
                    "items": {
                        "$any": {
                            "$values": {
                                "status": "open",
                                "qty": {"$gte": 2},
                            },
                        },
                    },
                },
            },
        )
        doc = {
            "items": [
                {"status": "closed", "qty": 1},
                {"status": "open", "qty": 3},
            ],
        }
        assert _match_expr(doc, expr) is True
        assert _match_expr({"items": [{"status": "closed", "qty": 5}]}, expr) is False

    def test_match_not(self) -> None:
        from forze_mock.adapters import _match_expr

        expr = QueryFilterExpressionParser.parse(
            {"$not": {"$values": {"status": "archived"}}},
        )
        assert _match_expr({"status": "active"}, expr) is True
        assert _match_expr({"status": "archived"}, expr) is False

    def test_match_not_with_or(self) -> None:
        from forze_mock.adapters import _match_expr

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
        assert _match_expr({"status": "active"}, expr) is True
        assert _match_expr({"status": "archived"}, expr) is False
        assert _match_expr({"status": "pending"}, expr) is False

    def test_match_and_with_elem_and_not(self) -> None:
        from forze_mock.adapters import _match_expr

        expr = QueryFilterExpressionParser.parse(
            {
                "$and": [
                    {"$values": {"title": "yes"}},
                    {"$values": {"tags": {"$any": "urgent"}}},
                    {
                        "$not": {
                            "$values": {"blocked": True},
                        },
                    },
                ],
            },
        )
        assert _match_expr(
            {"title": "yes", "tags": ["urgent"], "blocked": False},
            expr,
        )
        assert (
            _match_expr(
                {"title": "yes", "tags": ["urgent"], "blocked": True},
                expr,
            )
            is False
        )


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

    def test_query_compare(self) -> None:
        node = QueryCompare("a", "$lte", "b")
        assert node.left == "a"
        assert node.op == "$lte"
        assert node.right == "b"

    def test_inheritance(self) -> None:
        assert issubclass(QueryField, QueryExpr)
        assert issubclass(QueryCompare, QueryExpr)
        assert issubclass(QueryAnd, QueryExpr)
        assert issubclass(QueryOr, QueryExpr)
        assert issubclass(QueryNot, QueryExpr)
        assert issubclass(QueryElem, QueryExpr)
