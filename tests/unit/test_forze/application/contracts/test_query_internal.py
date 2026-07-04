"""Unit tests for forze.application.contracts.querying.internal."""

from datetime import date, datetime, timezone
from uuid import UUID

import pytest

from forze.application.contracts.querying import AggregatesExpressionParser
from forze.application.contracts.querying.internal import (
    ELEM_SCALAR_FIELD,
    GroupField,
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
    elem_inner_is_scalar,
)
from forze.application.contracts.querying.internal.aggregate import (
    _having_field_roots,
)
from forze.base.exceptions import CoreException

# ----------------------- #


class TestHavingFieldRoots:
    """Direct tests for the ``$having`` alias-root extractor."""

    def test_extracts_field_compare_and_elem_roots_dotted(self) -> None:
        # Roots are top-level names: dotted paths collapse to their first segment.
        expr = QueryAnd(
            (
                QueryField("count.total", "$gt", 1),
                QueryCompare("a.x", "$lt", "b.y"),
                QueryElem("tags.inner", "$any", QueryField("$", "$eq", "vip")),
                QueryNot(QueryField("flag", "$eq", True)),
                QueryOr((QueryField("region", "$eq", "eu"),)),
            ),
        )
        assert _having_field_roots(expr) == frozenset(
            {"count", "a", "b", "tags", "flag", "region"},
        )

    def test_unmatched_node_contributes_no_roots(self) -> None:
        # A bare ``QueryExpr`` base instance hits the ``case _`` fall-through and
        # adds nothing (defensive branch for unknown node kinds).
        assert _having_field_roots(QueryExpr()) == frozenset()


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
        assert isinstance(parsed.groups[0].expr, GroupField)
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
        with pytest.raises(CoreException, match="expects no field"):
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
        with pytest.raises(CoreException, match="requires a field"):
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
        with pytest.raises(CoreException, match="Duplicate aggregate aliases"):
            AggregatesExpressionParser.parse(
                {
                    "$groups": {"total": "category"},
                    "$computed": {"total": {"$sum": "price"}},
                },
            )

    def test_rejects_invalid_group_keys_type(self) -> None:
        with pytest.raises(CoreException, match=r"Invalid aggregate \$groups"):
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
        with pytest.raises(CoreException, match="Duplicate aggregate aliases"):
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
        with pytest.raises(CoreException, match="Invalid \\$groups operator"):
            AggregatesExpressionParser.parse(
                {
                    "$groups": {"x": {"$unknown": {"field": "ts"}}},
                    "$computed": {"n": {"$count": None}},
                },
            )

    def test_rejects_bare_trunc_spec_without_operator(self) -> None:
        with pytest.raises(CoreException, match="exactly one operator"):
            AggregatesExpressionParser.parse(
                {
                    "$groups": {"day": {"field": "ts", "unit": "day"}},
                    "$computed": {"n": {"$count": None}},
                },
            )

    # Branch coverage for validation / error paths ............. #

    def test_rejects_non_mapping_computed(self) -> None:
        with pytest.raises(CoreException, match=r"Invalid aggregate \$computed"):
            AggregatesExpressionParser.parse({"$computed": 5})  # type: ignore[arg-type]

    def test_rejects_missing_computed(self) -> None:
        with pytest.raises(CoreException, match="requires \\$computed"):
            AggregatesExpressionParser.parse({"$computed": {}})

    def test_rejects_invalid_group_map_value_type(self) -> None:
        with pytest.raises(CoreException, match=r"Invalid \$groups map value"):
            AggregatesExpressionParser.parse(
                {
                    "$groups": {"x": 5},
                    "$computed": {"n": {"$count": None}},
                },
            )

    def test_rejects_non_mapping_trunc_spec(self) -> None:
        with pytest.raises(CoreException, match=r"Invalid \$trunc spec"):
            AggregatesExpressionParser.parse(
                {
                    "$groups": {"x": {"$trunc": "ts"}},
                    "$computed": {"n": {"$count": None}},
                },
            )

    def test_rejects_unknown_trunc_keys(self) -> None:
        with pytest.raises(CoreException, match=r"Invalid \$trunc keys"):
            AggregatesExpressionParser.parse(
                {
                    "$groups": {"x": {"$trunc": {"field": "ts", "bad": 1}}},
                    "$computed": {"n": {"$count": None}},
                },
            )

    def test_rejects_trunc_empty_field(self) -> None:
        with pytest.raises(CoreException, match=r"\$trunc.field must be"):
            AggregatesExpressionParser.parse(
                {
                    "$groups": {"x": {"$trunc": {"field": "  ", "unit": "day"}}},
                    "$computed": {"n": {"$count": None}},
                },
            )

    def test_rejects_trunc_invalid_unit(self) -> None:
        with pytest.raises(CoreException, match=r"\$trunc.unit must be"):
            AggregatesExpressionParser.parse(
                {
                    "$groups": {"x": {"$trunc": {"field": "ts", "unit": "year"}}},
                    "$computed": {"n": {"$count": None}},
                },
            )

    def test_rejects_trunc_non_string_timezone(self) -> None:
        with pytest.raises(CoreException, match=r"\$trunc.timezone must be a string"):
            AggregatesExpressionParser.parse(
                {
                    "$groups": {
                        "x": {"$trunc": {"field": "ts", "unit": "day", "timezone": 5}},
                    },
                    "$computed": {"n": {"$count": None}},
                },
            )

    def test_rejects_invalid_alias(self) -> None:
        with pytest.raises(CoreException, match="Invalid aggregate alias"):
            AggregatesExpressionParser.parse(
                {
                    "$groups": {"1bad": "category"},
                    "$computed": {"n": {"$count": None}},
                },
            )

    def test_rejects_empty_field_path(self) -> None:
        with pytest.raises(CoreException, match="Invalid aggregate field path"):
            AggregatesExpressionParser.parse(
                {
                    "$groups": {"a": "  "},
                    "$computed": {"n": {"$count": None}},
                },
            )

    def test_rejects_non_mapping_computed_spec(self) -> None:
        with pytest.raises(CoreException, match="Invalid aggregate computed field spec"):
            AggregatesExpressionParser.parse({"$computed": {"a": 5}})

    def test_rejects_computed_multiple_functions(self) -> None:
        with pytest.raises(CoreException, match="exactly one function"):
            AggregatesExpressionParser.parse(
                {"$computed": {"a": {"$sum": "x", "$avg": "y"}}},
            )

    def test_rejects_unknown_function(self) -> None:
        with pytest.raises(CoreException, match="Invalid aggregate function"):
            AggregatesExpressionParser.parse(
                {"$computed": {"a": {"$unknown": "x"}}},
            )

    def test_rejects_invalid_function_keys(self) -> None:
        with pytest.raises(CoreException, match="Invalid aggregate function keys"):
            AggregatesExpressionParser.parse(
                {"$computed": {"a": {"$sum": {"field": "x", "bad": 1}}}},
            )

    def test_rejects_count_with_field_in_mapping_form(self) -> None:
        with pytest.raises(CoreException, match="expects no field"):
            AggregatesExpressionParser.parse(
                {"$computed": {"a": {"$count": {"field": "x"}}}},
            )

    def test_mapping_form_without_filter_ok(self) -> None:
        # Covers the ``filter is None`` branch of the mapping argument form.
        parsed = AggregatesExpressionParser.parse(
            {"$computed": {"revenue": {"$sum": {"field": "price"}}}},
        )
        assert parsed.computed_fields[0].field == "price"
        assert parsed.computed_fields[0].parsed_filter is None

    def test_percentile_rejects_non_numeric_quantile(self) -> None:
        with pytest.raises(CoreException, match="must be a number"):
            AggregatesExpressionParser.parse(
                {"$computed": {"p": {"$percentile": {"field": "x", "p": "bad"}}}},
            )

    def test_percentile_rejects_out_of_range_quantile(self) -> None:
        with pytest.raises(CoreException, match="must be a number"):
            AggregatesExpressionParser.parse(
                {"$computed": {"p": {"$percentile": {"field": "x", "p": 1.5}}}},
            )

    def test_percentile_rejects_bool_quantile(self) -> None:
        # bool is an int subtype; reject it explicitly rather than treating True as 1.
        with pytest.raises(CoreException, match="must be a number"):
            AggregatesExpressionParser.parse(
                {"$computed": {"p": {"$percentile": {"field": "x", "p": True}}}},
            )

    def test_having_walks_not_and_field_compare(self) -> None:
        # The $having alias-root validation walks $not (negation) and $fields (compare)
        # nodes, not just plain value predicates.
        parsed = AggregatesExpressionParser.parse(
            {
                "$groups": {"region": "region"},
                "$computed": {"cnt": {"$count": None}, "total": {"$sum": "amount"}},
                "$having": {
                    "$and": [
                        {"$not": {"$values": {"cnt": {"$lt": 2}}}},
                        {"$fields": {"total": {"$gt": "cnt"}}},
                    ],
                },
            },
        )
        assert parsed.having is not None

    def test_having_rejects_unknown_alias_via_field_compare(self) -> None:
        with pytest.raises(CoreException):
            AggregatesExpressionParser.parse(
                {
                    "$groups": {"region": "region"},
                    "$computed": {"cnt": {"$count": None}},
                    "$having": {"$fields": {"cnt": {"$gt": "ghost"}}},
                },
            )

    def test_having_none_when_absent(self) -> None:
        # No ``$having`` key -> ``having`` is ``None`` (early-return branch).
        parsed = AggregatesExpressionParser.parse(
            {"$computed": {"n": {"$count": None}}},
        )
        assert parsed.having is None

    def test_having_empty_filter_is_none(self) -> None:
        # A falsy ``$having`` (empty mapping) short-circuits to ``None``.
        parsed = AggregatesExpressionParser.parse(
            {"$computed": {"n": {"$count": None}}, "$having": {}},
        )
        assert parsed.having is None

    def test_having_walks_element_quantifier_alias_root(self) -> None:
        # The root-extraction walks ``QueryElem`` nodes: a quantifier whose array
        # path is a valid output alias passes validation.
        parsed = AggregatesExpressionParser.parse(
            {
                "$groups": {"tags": "tags"},
                "$computed": {"n": {"$count": None}},
                "$having": {"$values": {"tags": {"$any": "vip"}}},
            },
        )
        assert parsed.having is not None

    def test_having_rejects_unknown_alias_via_element_quantifier(self) -> None:
        # The ``QueryElem`` root ("ghost") is not an output alias -> rejected.
        with pytest.raises(CoreException, match="may only reference"):
            AggregatesExpressionParser.parse(
                {
                    "$groups": {"tags": "tags"},
                    "$computed": {"n": {"$count": None}},
                    "$having": {"$values": {"ghost": {"$any": "vip"}}},
                },
            )

    # Per-function valid parses ................................. #

    @pytest.mark.parametrize(
        "function",
        ["$sum", "$avg", "$min", "$max", "$median", "$count_distinct"],
    )
    def test_parses_value_function_scalar_shorthand(self, function: str) -> None:
        parsed = AggregatesExpressionParser.parse(
            {"$computed": {"metric": {function: "price"}}},
        )
        field = parsed.computed_fields[0]
        assert field.alias == "metric"
        assert field.function == function
        assert field.field == "price"
        assert field.p is None
        assert field.parsed_filter is None

    def test_parses_count_with_null_field(self) -> None:
        parsed = AggregatesExpressionParser.parse(
            {"$computed": {"rows": {"$count": None}}},
        )
        field = parsed.computed_fields[0]
        assert field.function == "$count"
        assert field.field is None

    def test_parses_percentile_with_valid_quantile(self) -> None:
        parsed = AggregatesExpressionParser.parse(
            {"$computed": {"p95": {"$percentile": {"field": "latency", "p": 0.95}}}},
        )
        field = parsed.computed_fields[0]
        assert field.function == "$percentile"
        assert field.field == "latency"
        assert field.p == 0.95

    @pytest.mark.parametrize("p", [0, 1, 0.0, 1.0])
    def test_parses_percentile_boundary_quantiles(self, p: float) -> None:
        parsed = AggregatesExpressionParser.parse(
            {"$computed": {"q": {"$percentile": {"field": "x", "p": p}}}},
        )
        # ``_quantile`` always returns a ``float`` (covers ``float(p)`` return).
        assert parsed.computed_fields[0].p == float(p)
        assert isinstance(parsed.computed_fields[0].p, float)

    def test_percentile_rejects_scalar_shorthand(self) -> None:
        # ``$percentile`` has no scalar form: a bare field string is rejected.
        with pytest.raises(CoreException, match="requires the .* form"):
            AggregatesExpressionParser.parse(
                {"$computed": {"p": {"$percentile": "latency"}}},
            )

    def test_percentile_rejects_missing_quantile(self) -> None:
        # Mapping form present but no ``p`` key -> ``_quantile(None)`` raise.
        with pytest.raises(CoreException, match="requires a 'p' quantile"):
            AggregatesExpressionParser.parse(
                {"$computed": {"p": {"$percentile": {"field": "latency"}}}},
            )

    def test_non_percentile_rejects_p_key(self) -> None:
        # ``p`` is only an allowed key for ``$percentile``; on ``$sum`` it is extra.
        with pytest.raises(CoreException, match="Invalid aggregate function keys"):
            AggregatesExpressionParser.parse(
                {"$computed": {"s": {"$sum": {"field": "x", "p": 0.5}}}},
            )

    @pytest.mark.parametrize("unit", ["hour", "day", "week", "month"])
    def test_parses_trunc_each_unit(self, unit: str) -> None:
        parsed = AggregatesExpressionParser.parse(
            {
                "$groups": {"bucket": {"$trunc": {"field": "ts", "unit": unit}}},
                "$computed": {"n": {"$count": None}},
            },
        )
        trunc = parsed.groups[0].expr
        assert isinstance(trunc, GroupTrunc)
        assert trunc.unit == unit
        # Default timezone resolves to UTC (IANA mode) when omitted.
        assert trunc.timezone.mode == "iana"
        assert trunc.timezone.iana == "UTC"

    def test_per_aggregate_filter_parses_into_parsed_filter(self) -> None:
        parsed = AggregatesExpressionParser.parse(
            {
                "$computed": {
                    "books": {
                        "$sum": {
                            "field": "price",
                            "filter": {"$values": {"category": "books"}},
                        },
                    },
                },
            },
        )
        field = parsed.computed_fields[0]
        assert field.field == "price"
        assert field.filter == {"$values": {"category": "books"}}
        assert isinstance(field.parsed_filter, QueryAnd)
        inner = field.parsed_filter.items[0]
        assert isinstance(inner, QueryField)
        assert inner.name == "category" and inner.value == "books"


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
        with pytest.raises(CoreException, match="Invalid boolean"):
            QueryValueCaster.as_bool("maybe")

    def test_as_bool_invalid_int_raises(self) -> None:
        with pytest.raises(CoreException, match="Invalid boolean"):
            QueryValueCaster.as_bool(42)

    def test_as_bool_invalid_type_raises(self) -> None:
        with pytest.raises(CoreException, match="Invalid boolean"):
            QueryValueCaster.as_bool(3.14)

    # as_uuid
    def test_as_uuid_from_uuid(self) -> None:
        u = UUID("550e8400-e29b-41d4-a716-446655440000")
        assert QueryValueCaster.as_uuid(u) == u

    def test_as_uuid_from_string(self) -> None:
        s = "550e8400-e29b-41d4-a716-446655440000"
        assert QueryValueCaster.as_uuid(s) == UUID(s)

    def test_as_uuid_invalid_string_raises(self) -> None:
        with pytest.raises(CoreException, match="Invalid UUID"):
            QueryValueCaster.as_uuid("not-a-uuid")

    def test_as_uuid_invalid_type_raises(self) -> None:
        with pytest.raises(CoreException, match="Invalid UUID"):
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
        with pytest.raises(CoreException, match="got bool"):
            QueryValueCaster.as_int(True)

    def test_as_int_invalid_string_raises(self) -> None:
        with pytest.raises(CoreException, match="Invalid int"):
            QueryValueCaster.as_int("abc")

    def test_as_int_invalid_type_raises(self) -> None:
        with pytest.raises(CoreException, match="Invalid int"):
            QueryValueCaster.as_int([1])

    def test_as_int_non_integer_float_raises(self) -> None:
        with pytest.raises(CoreException, match="Invalid int"):
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
        with pytest.raises(CoreException, match="got bool"):
            QueryValueCaster.as_float(True)

    def test_as_float_invalid_string_raises(self) -> None:
        with pytest.raises(CoreException, match="Invalid float"):
            QueryValueCaster.as_float("abc")

    def test_as_float_invalid_type_raises(self) -> None:
        with pytest.raises(CoreException, match="Invalid float"):
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
        with pytest.raises(CoreException, match="Invalid datetime"):
            QueryValueCaster.as_datetime("not-a-date", force_tz=True)

    def test_as_datetime_invalid_type_raises(self) -> None:
        with pytest.raises(CoreException, match="Invalid datetime"):
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
        with pytest.raises(CoreException, match="Invalid date"):
            QueryValueCaster.as_date("not-a-date")

    def test_as_date_invalid_type_raises(self) -> None:
        with pytest.raises(CoreException, match="Invalid date"):
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

    def test_parse_hierarchy_scalar(self) -> None:
        for op in ("$descendant_of", "$ancestor_of"):
            result = QueryFilterExpressionParser.parse(
                {"$values": {"path": {op: "top.science"}}},
            )
            f = result.items[0]
            assert isinstance(f, QueryField)
            assert f.op == op
            assert f.value == "top.science"

    def test_parse_hierarchy_list_desugars_to_or(self) -> None:
        # A list operand is "any" semantics → OR of single-path predicates.
        result = QueryFilterExpressionParser.parse(
            {"$values": {"path": {"$descendant_of": ["top.science", "top.arts"]}}},
        )
        assert isinstance(result, QueryAnd)
        or_node = result.items[0]
        assert isinstance(or_node, QueryOr)
        assert len(or_node.items) == 2
        assert [i.value for i in or_node.items if isinstance(i, QueryField)] == [
            "top.science",
            "top.arts",
        ]
        assert all(
            isinstance(i, QueryField) and i.op == "$descendant_of"
            for i in or_node.items
        )

    def test_parse_hierarchy_single_item_list_stays_flat(self) -> None:
        result = QueryFilterExpressionParser.parse(
            {"$values": {"path": {"$ancestor_of": ["top.science"]}}},
        )
        f = result.items[0]
        assert isinstance(f, QueryField)
        assert f.op == "$ancestor_of" and f.value == "top.science"

    def test_parse_hierarchy_rejects_empty_list(self) -> None:
        with pytest.raises(CoreException, match="cannot be empty"):
            QueryFilterExpressionParser.parse(
                {"$values": {"path": {"$descendant_of": []}}},
            )

    def test_parse_hierarchy_rejects_non_string_operand(self) -> None:
        with pytest.raises(CoreException, match="path string"):
            QueryFilterExpressionParser.parse(
                {"$values": {"path": {"$descendant_of": 42}}},
            )

        with pytest.raises(CoreException, match="path string"):
            QueryFilterExpressionParser.parse(
                {"$values": {"path": {"$ancestor_of": ["top", 7]}}},
            )

    def test_parse_hierarchy_rejects_blank_path(self) -> None:
        with pytest.raises(CoreException, match="non-empty"):
            QueryFilterExpressionParser.parse(
                {"$values": {"path": {"$descendant_of": "   "}}},
            )

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
        assert all(
            isinstance(i, QueryField) and i.op == "$ilike" for i in or_node.items
        )

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
        with pytest.raises(CoreException, match="nested quantifiers"):
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
        with pytest.raises(CoreException, match="Invalid filter expression"):
            QueryFilterExpressionParser.parse({})

    def test_parse_unknown_key_raises(self) -> None:
        with pytest.raises(CoreException, match="Invalid filter expression"):
            QueryFilterExpressionParser.parse({"$unknown": []})

    def test_parse_empty_field_map_raises(self) -> None:
        with pytest.raises(CoreException, match="Empty \\$values"):
            QueryFilterExpressionParser.parse({"$values": {"x": {}}})

    def test_parse_eq_invalid_value_raises(self) -> None:
        with pytest.raises(CoreException, match="Invalid value for"):
            QueryFilterExpressionParser.parse({"$values": {"x": {"$eq": [1, 2]}}})

    def test_parse_ord_invalid_value_raises(self) -> None:
        with pytest.raises(CoreException, match="Invalid value for"):
            QueryFilterExpressionParser.parse(
                {"$values": {"x": {"$gte": "not-numeric"}}}
            )

    def test_parse_in_invalid_value_raises(self) -> None:
        with pytest.raises(CoreException, match="Invalid value for"):
            QueryFilterExpressionParser.parse({"$values": {"x": {"$in": "not-a-list"}}})

    def test_parse_null_invalid_value_raises(self) -> None:
        with pytest.raises(CoreException, match="Invalid value for"):
            QueryFilterExpressionParser.parse({"$values": {"x": {"$null": "not-bool"}}})

    def test_parse_invalid_operator_raises(self) -> None:
        with pytest.raises(CoreException, match="Invalid operator"):
            QueryFilterExpressionParser.parse({"$values": {"x": {"$unknown": 1}}})

    def test_parse_set_rel_invalid_value_raises(self) -> None:
        with pytest.raises(CoreException, match="Invalid value for"):
            QueryFilterExpressionParser.parse(
                {"$values": {"x": {"$superset": "not-list"}}}
            )

    # Validate null=True with other ops
    def test_null_true_with_other_ops_raises(self) -> None:
        with pytest.raises(CoreException, match="cannot be null"):
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
        with pytest.raises(CoreException, match="cannot be empty"):
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
        with pytest.raises(CoreException, match="cannot mix"):
            QueryFilterExpressionParser.parse(
                {"$and": [], "$values": {"a": 1}},
            )

    def test_parse_empty_fields_map_raises(self) -> None:
        with pytest.raises(CoreException, match="Empty \\$fields"):
            QueryFilterExpressionParser.parse({"$fields": {}})

    def test_parse_compare_invalid_operator_raises(self) -> None:
        with pytest.raises(CoreException, match="Invalid field compare operator"):
            QueryFilterExpressionParser.parse(
                {"$fields": {"a": {"$in": "b"}}},
            )

    def test_parse_compare_scalar_rhs_raises(self) -> None:
        with pytest.raises(CoreException, match="field path string"):
            QueryFilterExpressionParser.parse(
                {"$fields": {"a": {"$eq": 1}}},
            )

    def test_parse_compare_empty_rhs_raises(self) -> None:
        with pytest.raises(CoreException, match="field path string"):
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
        with pytest.raises(CoreException, match="maximum depth"):
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
        with pytest.raises(CoreException, match="maximum clause count"):
            parser.parse_filter(expr)  # type: ignore[arg-type]

    def test_parse_exceeds_max_in_size(self) -> None:
        parser = QueryFilterExpressionParser(
            limits=QueryFilterLimits(max_depth=32, max_clauses=256, max_in_size=2),
        )
        with pytest.raises(CoreException, match="maximum size"):
            parser.parse_filter({"$values": {"x": [1, 2, 3]}})  # type: ignore[arg-type]

    def test_parse_exceeds_max_in_size_operator_form(self) -> None:
        parser = QueryFilterExpressionParser(
            limits=QueryFilterLimits(max_depth=32, max_clauses=256, max_in_size=1),
        )
        with pytest.raises(CoreException, match="maximum size"):
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
        with pytest.raises(CoreException, match="cannot mix"):
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

    def test_parse_scalar_array_of_arrays_nesting(self) -> None:
        # A quantifier directly on the (array) element — scalar array-of-arrays.
        # Modeled as a nested QueryElem on the element itself ("$" sentinel path).
        result = QueryFilterExpressionParser.parse(
            {"$values": {"matrix": {"$any": {"$all": "x"}}}},
        )
        outer = result.items[0]
        assert isinstance(outer, QueryElem)
        assert outer.path == "matrix" and outer.quantifier == "$any"
        inner = outer.inner
        assert isinstance(inner, QueryElem)
        assert inner.path == "$" and inner.quantifier == "$all"
        assert isinstance(inner.inner, QueryField)
        assert inner.inner.name == "$" and inner.inner.value == "x"

    def test_parse_element_quantifier_mixed_with_operator_raises(self) -> None:
        # A quantifier key cannot be combined with other operators in the same map.
        with pytest.raises(CoreException, match="cannot be combined"):
            QueryFilterExpressionParser.parse(
                {"$values": {"tags": {"$any": {"$any": "x", "$gt": 1}}}},
            )

    def test_parse_element_invalid_operator_raises(self) -> None:
        # $superset is a set-relation op, not an element op (unlike $in/$gt/...).
        with pytest.raises(CoreException, match="Element constraint must be"):
            QueryFilterExpressionParser.parse(
                {"$values": {"tags": {"$any": {"$superset": ["a"]}}}},
            )


# ----------------------- #
# Extra branch coverage for the filter parser (error/edge paths).


class TestQueryFilterParserBranches:
    def test_empty_and_yields_empty_conjunction(self) -> None:
        # Hits the early-return in ``_add_clauses`` when ``count <= 0``.
        result = QueryFilterExpressionParser.parse({"$and": []})
        assert isinstance(result, QueryAnd)
        assert result.items == ()

    def test_not_requires_object(self) -> None:
        with pytest.raises(CoreException, match=r"\$not requires a filter expression"):
            QueryFilterExpressionParser.parse({"$not": "nope"})

    def test_empty_values_map_raises(self) -> None:
        with pytest.raises(CoreException, match=r"Empty \$values map"):
            QueryFilterExpressionParser.parse({"$values": {}})

    def test_empty_fields_compare_map_raises(self) -> None:
        with pytest.raises(CoreException, match=r"Empty \$fields compare map"):
            QueryFilterExpressionParser.parse({"$fields": {"a": {}}})

    def test_invalid_fields_map_value_raises(self) -> None:
        with pytest.raises(CoreException, match=r"Invalid \$fields map value"):
            QueryFilterExpressionParser.parse({"$fields": {"a": 1}})

    def test_in_shortcut_non_collection_skips_size_check(self) -> None:
        # frozenset is not list/tuple/set -> ``_check_in_size`` early-returns.
        result = QueryFilterExpressionParser.parse(
            {"$values": {"x": frozenset({1, 2})}},
        )
        f = result.items[0]
        assert isinstance(f, QueryField)
        assert f.op == "$in"

    def test_validate_op_static_entrypoint(self) -> None:
        node = QueryFilterExpressionParser._validate_op("f", "$eq", 1)
        assert isinstance(node, QueryField)
        assert node.op == "$eq" and node.value == 1

    # Element-constraint edge cases ............................. #

    def test_element_constraint_non_scalar_non_dict_raises(self) -> None:
        with pytest.raises(CoreException, match="Invalid element constraint"):
            QueryFilterExpressionParser.parse(
                {"$values": {"tags": {"$any": ["a"]}}},
            )

    def test_element_empty_values_map_raises(self) -> None:
        with pytest.raises(CoreException, match=r"Empty \$values map in element"):
            QueryFilterExpressionParser.parse(
                {"$values": {"items": {"$any": {"$values": {}}}}},
            )

    def test_element_empty_constraint_map_raises(self) -> None:
        with pytest.raises(CoreException, match="Empty element constraint map"):
            QueryFilterExpressionParser.parse(
                {"$values": {"tags": {"$any": {}}}},
            )

    def test_element_scalar_constraint_multiple_ops_conjoins_to_range(self) -> None:
        # Multiple operators on a scalar element are a range — they conjoin into a
        # QueryAnd of element-scalar predicates.
        ast = QueryFilterExpressionParser.parse(
            {"$values": {"scores": {"$any": {"$gt": 1, "$lt": 5}}}},
        )
        elem = ast.items[0]  # type: ignore[attr-defined]
        assert isinstance(elem, QueryElem)
        assert elem.path == "scores" and elem.quantifier == "$any"
        assert isinstance(elem.inner, QueryAnd)
        ops = {f.op for f in elem.inner.items}  # type: ignore[attr-defined]
        assert ops == {"$gt", "$lt"}
        assert all(f.name == ELEM_SCALAR_FIELD for f in elem.inner.items)  # type: ignore[attr-defined]

    def test_element_scalar_eq_invalid_value_raises(self) -> None:
        with pytest.raises(CoreException, match="Invalid value for"):
            QueryFilterExpressionParser.parse(
                {"$values": {"scores": {"$any": {"$eq": [1, 2]}}}},
            )

    def test_element_scalar_ord_invalid_value_raises(self) -> None:
        with pytest.raises(CoreException, match="Invalid value for"):
            QueryFilterExpressionParser.parse(
                {"$values": {"scores": {"$any": {"$gt": "abc"}}}},
            )

    def test_element_scalar_text_op_sequence_desugars_to_or(self) -> None:
        result = QueryFilterExpressionParser.parse(
            {"$values": {"tags": {"$any": {"$ilike": ["%a%", "%b%"]}}}},
        )
        elem = result.items[0]
        assert isinstance(elem, QueryElem)
        assert isinstance(elem.inner, QueryOr)
        assert len(elem.inner.items) == 2

    # Object-array element $values edge cases .................. #

    def test_element_values_nested_quantifier_parses(self) -> None:
        # A quantifier inside an object element's $values is a nested quantifier
        # (over a sub-array) — it parses to a nested QueryElem (capability-gated
        # per backend at render time).
        ast = QueryFilterExpressionParser.parse(
            {
                "$values": {
                    "items": {"$any": {"$values": {"f": {"$any": "x"}}}},
                },
            },
        )
        outer = ast.items[0]  # type: ignore[attr-defined]
        assert isinstance(outer, QueryElem) and outer.path == "items"
        nested = outer.inner.items[0]  # type: ignore[attr-defined]
        assert isinstance(nested, QueryElem)
        assert nested.path == "f" and nested.quantifier == "$any"

    def test_element_values_null_shortcut_raises(self) -> None:
        with pytest.raises(CoreException, match="cannot use null shortcut"):
            QueryFilterExpressionParser.parse(
                {"$values": {"items": {"$any": {"$values": {"f": None}}}}},
            )

    def test_element_values_array_shortcut_raises(self) -> None:
        with pytest.raises(CoreException, match="cannot use array shortcut"):
            QueryFilterExpressionParser.parse(
                {"$values": {"items": {"$any": {"$values": {"f": ["a"]}}}}},
            )

    def test_element_values_scalar_shortcut_ok(self) -> None:
        result = QueryFilterExpressionParser.parse(
            {"$values": {"items": {"$any": {"$values": {"f": "x"}}}}},
        )
        elem = result.items[0]
        assert isinstance(elem, QueryElem)
        assert isinstance(elem.inner, QueryAnd)
        field = elem.inner.items[0]
        assert isinstance(field, QueryField)
        assert field.op == "$eq" and field.value == "x"

    def test_element_values_empty_field_map_raises(self) -> None:
        with pytest.raises(CoreException, match="Empty element \\$values field map"):
            QueryFilterExpressionParser.parse(
                {"$values": {"items": {"$any": {"$values": {"f": {}}}}}},
            )

    def test_element_values_nested_quantifier_in_conjunction_raises(self) -> None:
        # dict with a quantifier key plus extra key -> conjunction, then rejected.
        with pytest.raises(CoreException, match="cannot be combined"):
            QueryFilterExpressionParser.parse(
                {
                    "$values": {
                        "items": {
                            "$any": {
                                "$values": {"f": {"$any": "x", "extra": 1}},
                            },
                        },
                    },
                },
            )

    def test_element_values_field_multiple_element_ops_conjoins(self) -> None:
        # A range on an object element's field conjoins into two QueryFields on it.
        ast = QueryFilterExpressionParser.parse(
            {
                "$values": {
                    "items": {
                        "$any": {"$values": {"f": {"$gt": 1, "$lt": 5}}},
                    },
                },
            },
        )
        elem = ast.items[0]  # type: ignore[attr-defined]
        assert isinstance(elem, QueryElem)
        assert isinstance(elem.inner, QueryAnd)
        pairs = {(f.name, f.op) for f in elem.inner.items}  # type: ignore[attr-defined]
        assert pairs == {("f", "$gt"), ("f", "$lt")}

    def test_element_values_field_mixed_ops_invalid_op_raises(self) -> None:
        # A non-element op ($superset) mixed into an element field's op-map is
        # rejected per-op as an invalid element operator.
        with pytest.raises(CoreException, match="Invalid element operator"):
            QueryFilterExpressionParser.parse(
                {
                    "$values": {
                        "items": {
                            "$any": {"$values": {"f": {"$gt": 1, "$superset": [1]}}},
                        },
                    },
                },
            )


# ----------------------- #


class TestMockElementQuantifiers:
    def test_match_elem_any_scalar(self) -> None:
        from forze_mock.query import _match_expr

        expr = QueryFilterExpressionParser.parse(
            {"$values": {"tags": {"$any": "urgent"}}},
        )
        assert _match_expr({"tags": ["ops", "urgent"]}, expr) is True
        assert _match_expr({"tags": ["ops"]}, expr) is False
        assert _match_expr({"tags": []}, expr) is False

    def test_match_elem_any_scalar_gte(self) -> None:
        from forze_mock.query import _match_expr

        expr = QueryFilterExpressionParser.parse(
            {"$values": {"scores": {"$any": {"$gte": 10}}}},
        )
        assert _match_expr({"scores": [5, 15]}, expr) is True
        assert _match_expr({"scores": [1, 2]}, expr) is False

    def test_match_elem_all_vacuous_empty(self) -> None:
        from forze_mock.query import _match_expr

        expr = QueryFilterExpressionParser.parse(
            {"$values": {"tags": {"$all": {"$eq": "x"}}}},
        )
        assert _match_expr({"tags": []}, expr) is True
        assert _match_expr({}, expr) is True

    def test_match_elem_all_requires_every_element(self) -> None:
        from forze_mock.query import _match_expr

        expr = QueryFilterExpressionParser.parse(
            {"$values": {"tags": {"$all": {"$eq": "ops"}}}},
        )
        assert _match_expr({"tags": ["ops"]}, expr) is True
        assert _match_expr({"tags": ["ops", "urgent"]}, expr) is False

    def test_match_elem_none_scalar(self) -> None:
        from forze_mock.query import _match_expr

        expr = QueryFilterExpressionParser.parse(
            {"$values": {"tags": {"$none": "urgent"}}},
        )
        assert _match_expr({"tags": ["api"]}, expr) is True
        assert _match_expr({"tags": ["urgent"]}, expr) is False

    def test_match_elem_any_object_array(self) -> None:
        from forze_mock.query import _match_expr

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
        from forze_mock.query import _match_expr

        expr = QueryFilterExpressionParser.parse(
            {"$not": {"$values": {"status": "archived"}}},
        )
        assert _match_expr({"status": "active"}, expr) is True
        assert _match_expr({"status": "archived"}, expr) is False

    def test_match_not_with_or(self) -> None:
        from forze_mock.query import _match_expr

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
        from forze_mock.query import _match_expr

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


# ----------------------- #
# elem_inner_is_scalar (shared by backend renderers)


def test_elem_inner_is_scalar_single_scalar_field() -> None:
    assert elem_inner_is_scalar(QueryField(ELEM_SCALAR_FIELD, "$eq", "x")) is True
    assert elem_inner_is_scalar(QueryField("name", "$eq", "x")) is False


def test_elem_inner_is_scalar_and_all_scalar() -> None:
    expr = QueryAnd(
        (
            QueryField(ELEM_SCALAR_FIELD, "$gte", 1),
            QueryField(ELEM_SCALAR_FIELD, "$lt", 10),
        )
    )
    assert elem_inner_is_scalar(expr) is True

    mixed = QueryAnd(
        (QueryField(ELEM_SCALAR_FIELD, "$gte", 1), QueryField("qty", "$lt", 10))
    )
    assert elem_inner_is_scalar(mixed) is False


def test_elem_inner_is_scalar_or_recurses() -> None:
    expr = QueryOr(
        (
            QueryField(ELEM_SCALAR_FIELD, "$eq", "a"),
            QueryAnd((QueryField(ELEM_SCALAR_FIELD, "$eq", "b"),)),
        )
    )
    assert elem_inner_is_scalar(expr) is True


def test_elem_inner_is_scalar_object_predicate_is_false() -> None:
    assert elem_inner_is_scalar(QueryField("sku", "$eq", "x")) is False
    assert elem_inner_is_scalar(QueryNot(QueryField(ELEM_SCALAR_FIELD, "$eq", "x"))) is False


# ....................... #


def test_combinator_operand_must_be_a_list() -> None:
    # A non-list ``$or`` / ``$and`` operand (e.g. a bare string) is a clean client-caused
    # precondition, not an AttributeError deep in the recursive parse.
    for op in ("$or", "$and"):
        with pytest.raises(CoreException, match="list of filter expression"):
            QueryFilterExpressionParser.parse({op: "not-a-list"})


def test_combinator_operand_entries_must_be_objects() -> None:
    # A list entry that is not a filter-expression object is likewise rejected up front.
    for op in ("$or", "$and"):
        with pytest.raises(CoreException, match="must be filter expression"):
            QueryFilterExpressionParser.parse({op: ["not-a-dict"]})
