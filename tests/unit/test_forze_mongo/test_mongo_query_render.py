"""Unit tests for :class:`~forze_mongo.kernel.query.render.MongoQueryRenderer`."""

from __future__ import annotations

from uuid import uuid4

import attrs
import pytest

from forze.base.exceptions import CoreException
from pydantic import BaseModel

from forze.application.contracts.querying import (
    ELEM_SCALAR_FIELD,
    AggregateComputedField,
    QueryAnd,
    QueryCompare,
    QueryElem,
    QueryExpr,
    QueryField,
    QueryFilterExpressionParser,
    QueryNot,
    QueryOr,
)
from forze_mongo.kernel.query.render import MongoQueryRenderer


@attrs.define(slots=True, frozen=True)
class _UnknownExpr(QueryExpr):
    """Expression node not handled by the renderer."""


class TestMongoQueryRenderer:
    def test_query_and_empty(self) -> None:
        r = MongoQueryRenderer()
        assert r.render(QueryAnd(())) == {}

    def test_query_and_drops_empty_children(self) -> None:
        r = MongoQueryRenderer()
        expr = QueryAnd((QueryAnd(()), QueryField("a", "$eq", 1)))
        assert r.render(expr) == {"a": 1}

    def test_query_and_multiple(self) -> None:
        r = MongoQueryRenderer()
        expr = QueryAnd((QueryField("a", "$eq", 1), QueryField("b", "$eq", 2)))
        assert r.render(expr) == {"$and": [{"a": 1}, {"b": 2}]}

    def test_query_or_empty(self) -> None:
        r = MongoQueryRenderer()
        assert r.render(QueryOr(())) == {"$expr": False}

    def test_query_or_drops_empty_children(self) -> None:
        r = MongoQueryRenderer()
        expr = QueryOr((QueryAnd(()), QueryField("x", "$eq", "y")))
        assert r.render(expr) == {"x": "y"}

    def test_query_or_multiple(self) -> None:
        r = MongoQueryRenderer()
        expr = QueryOr((QueryField("a", "$eq", 1), QueryField("b", "$eq", 2)))
        assert r.render(expr) == {"$or": [{"a": 1}, {"b": 2}]}

    def test_unknown_expression_raises(self) -> None:
        r = MongoQueryRenderer()
        with pytest.raises(CoreException, match="Unknown expression"):
            r.render(_UnknownExpr())

    def test_compare_renders_expr(self) -> None:
        r = MongoQueryRenderer()
        assert r.render(QueryCompare("starts_at", "$lte", "ends_at")) == {
            "$expr": {"$lte": ["$starts_at", "$ends_at"]},
        }

    def test_compare_eq_shortcut_via_parser_shape(self) -> None:
        from forze.application.contracts.querying import QueryFilterExpressionParser

        expr = QueryFilterExpressionParser.parse(
            {"$fields": {"a": "b"}},
        )
        r = MongoQueryRenderer()
        assert r.render(expr) == {"$expr": {"$eq": ["$a", "$b"]}}

    def test_compare_dot_paths(self) -> None:
        r = MongoQueryRenderer()
        assert r.render(QueryCompare("meta.score", "$gte", "meta.min")) == {
            "$expr": {"$gte": ["$meta.score", "$meta.min"]},
        }

    def test_query_not_renders_nor(self) -> None:
        expr = QueryFilterExpressionParser.parse(
            {"$not": {"$values": {"status": "archived"}}},
        )
        r = MongoQueryRenderer()
        out = r.render(expr)
        assert "$nor" in out

    def test_element_any_scalar_eq(self) -> None:
        expr = QueryFilterExpressionParser.parse(
            {"$values": {"tags": {"$any": "urgent"}}},
        )
        r = MongoQueryRenderer()
        out = r.render(expr)
        assert out["$or"][1] == {
            "$and": [
                {
                    "$and": [
                        {"tags": {"$exists": True}},
                        {"tags": {"$type": "array"}},
                        {"tags": {"$not": {"$size": 0}}},
                    ],
                },
                {"tags": "urgent"},
            ],
        }
        assert out["$or"][0]["$and"][1] == {"$expr": False}

    def test_element_any_object_elem_match(self) -> None:
        expr = QueryFilterExpressionParser.parse(
            {
                "$values": {
                    "items": {
                        "$any": {"$values": {"status": "open", "qty": {"$gte": 1}}},
                    },
                },
            },
        )
        r = MongoQueryRenderer()
        out = r.render(expr)
        match = out["$or"][1]["$and"][1]
        assert match == {
            "items": {"$elemMatch": {"status": "open", "qty": {"$gte": 1}}}
        }

    def test_element_all_scalar_eq_uses_min_max_expr(self) -> None:
        expr = QueryFilterExpressionParser.parse(
            {"$values": {"tags": {"$all": {"$eq": "x"}}}},
        )
        r = MongoQueryRenderer()
        match = r.render(expr)["$or"][1]["$and"][1]
        assert "$expr" in match
        assert "$min" in str(match)

    def test_element_none_scalar_eq_uses_nor(self) -> None:
        expr = QueryFilterExpressionParser.parse(
            {"$values": {"tags": {"$none": "spam"}}},
        )
        r = MongoQueryRenderer()
        match = r.render(expr)["$or"][1]["$and"][1]
        assert match == {"$nor": [{"tags": "spam"}]}

    def test_element_any_scalar_gte_uses_expr(self) -> None:
        expr = QueryFilterExpressionParser.parse(
            {"$values": {"scores": {"$any": {"$gte": 10}}}},
        )
        r = MongoQueryRenderer()
        match = r.render(expr)["$or"][1]["$and"][1]
        assert match == {"$expr": {"$gte": [{"$max": "$scores"}, 10]}}

    def test_element_all_object_uses_not_elem_match(self) -> None:
        inner = QueryAnd(
            (
                QueryField("status", "$eq", "open"),
                QueryField("qty", "$gte", 1),
            ),
        )
        r = MongoQueryRenderer()
        match = r.render(
            QueryElem("items", "$all", inner),
        )[
            "$or"
        ][1][
            "$and"
        ][1]
        assert "items" in match
        assert "$not" in match["items"]
        assert "$elemMatch" in match["items"]["$not"]

    def test_not_with_elem_inside_and(self) -> None:
        expr = QueryFilterExpressionParser.parse(
            {
                "$not": {
                    "$and": [
                        {"$values": {"a": 1}},
                        {"$values": {"tags": {"$any": "x"}}},
                    ],
                },
            },
        )
        r = MongoQueryRenderer()
        out = r.render(expr)
        assert "$nor" in out
        assert len(out["$nor"]) == 1

    def test_compare_with_fields_in_and(self) -> None:
        r = MongoQueryRenderer()
        expr = QueryAnd(
            (
                QueryField("status", "$eq", "active"),
                QueryCompare("a", "$lt", "b"),
            ),
        )
        assert r.render(expr) == {
            "$and": [
                {"status": "active"},
                {"$expr": {"$lt": ["$a", "$b"]}},
            ],
        }

    def test_unknown_operator_raises(self) -> None:
        r = MongoQueryRenderer()
        with pytest.raises(CoreException, match="Unknown operator"):
            r.render(QueryField("f", "$bogus", 1))  # type: ignore[arg-type]

    def test_eq_neq_ord(self) -> None:
        r = MongoQueryRenderer()
        assert r.render(QueryField("n", "$gt", 3)) == {"n": {"$gt": 3}}
        assert r.render(QueryField("n", "$eq", 3)) == {"n": 3}
        assert r.render(QueryField("n", "$neq", 3)) == {"n": {"$ne": 3}}

    def test_dot_notation_nested_field_passthrough(self) -> None:
        """MongoDB interprets dotted keys as nested paths in query documents."""
        r = MongoQueryRenderer()
        assert r.render(QueryField("meta.score", "$eq", 1)) == {"meta.score": 1}

    def test_membership(self) -> None:
        r = MongoQueryRenderer()
        assert r.render(QueryField("t", "$in", [1, 2])) == {"t": {"$in": [1, 2]}}
        assert r.render(QueryField("t", "$nin", [1, 2])) == {"t": {"$nin": [1, 2]}}

    def test_membership_scalar_raises(self) -> None:
        r = MongoQueryRenderer()
        with pytest.raises(CoreException, match="expects list"):
            r.render(QueryField("t", "$in", 1))

    def test_set_relations(self) -> None:
        r = MongoQueryRenderer()
        vs = [1, 2]
        assert r.render(QueryField("s", "$superset", vs)) == {"s": {"$all": vs}}
        assert r.render(QueryField("s", "$overlaps", vs)) == {"s": {"$in": vs}}
        assert r.render(QueryField("s", "$disjoint", vs)) == {"s": {"$nin": vs}}
        assert r.render(QueryField("s", "$subset", vs)) == {
            "$expr": {"$setIsSubset": ["$s", vs]},
        }

    def test_set_rel_scalar_raises(self) -> None:
        r = MongoQueryRenderer()
        with pytest.raises(CoreException, match="expects list"):
            r.render(QueryField("s", "$subset", 1))

    def test_null_default_matches_missing(self) -> None:
        r = MongoQueryRenderer(
            null_matches_missing=True, require_exists_for_not_null=True
        )
        assert r.render(QueryField("z", "$null", True)) == {"z": None}
        assert r.render(QueryField("z", "$null", False)) == {
            "$and": [{"z": {"$ne": None}}, {"z": {"$exists": True}}],
        }

    def test_null_explicit_missing_only(self) -> None:
        r = MongoQueryRenderer(
            null_matches_missing=False, require_exists_for_not_null=False
        )
        assert r.render(QueryField("z", "$null", True)) == {
            "$and": [{"z": None}, {"z": {"$exists": True}}],
        }
        assert r.render(QueryField("z", "$null", False)) == {"z": {"$ne": None}}

    def test_empty_unary(self) -> None:
        r = MongoQueryRenderer(require_exists_for_not_null=True)
        assert r.render(QueryField("e", "$empty", True)) == {"e": []}
        assert r.render(QueryField("e", "$empty", False)) == {
            "$and": [{"e": {"$ne": []}}, {"e": {"$exists": True}}],
        }

    def test_ilike_renders_regex_with_i_option(self) -> None:
        r = MongoQueryRenderer()
        out = r.render(QueryField("title", "$ilike", "%road%"))
        assert out == {"title": {"$regex": "^.*road.*$", "$options": "i"}}

    def test_like_field_renders_regex_without_options(self) -> None:
        r = MongoQueryRenderer()
        out = r.render(QueryField("title", "$like", "%road%"))
        assert out == {"title": {"$regex": "^.*road.*$"}}

    def test_ilike_sequence_parsed_as_or(self) -> None:
        expr = QueryFilterExpressionParser.parse(
            {"$values": {"title": {"$ilike": ["%a%", "%b%"]}}},
        )
        r = MongoQueryRenderer()
        out = r.render(expr)
        assert "$or" in out
        assert len(out["$or"]) == 2

    def test_element_any_object_ilike(self) -> None:
        expr = QueryFilterExpressionParser.parse(
            {
                "$values": {
                    "items": {
                        "$any": {"$values": {"name": {"$ilike": "%x%"}}},
                    },
                },
            },
        )
        r = MongoQueryRenderer()
        out = r.render(expr)

        def _has_elem_match(node: object) -> bool:
            if isinstance(node, dict):
                if "$elemMatch" in node:
                    return True
                return any(_has_elem_match(v) for v in node.values())
            if isinstance(node, list):
                return any(_has_elem_match(v) for v in node)
            return False

        assert _has_elem_match(out)

    def test_passes_uuid_through(self) -> None:
        u = uuid4()
        r = MongoQueryRenderer()
        assert r.render(QueryField("id", "$eq", u)) == {"id": u}


class _OrderRow(BaseModel):
    category: str
    price: float


class TestMongoAggregateRendering:
    def test_renders_grouped_aggregate_pipeline(self) -> None:
        renderer = MongoQueryRenderer()

        _parsed, pipeline = renderer.render_aggregates(
            {
                "$groups": {"category": "category"},
                "$computed": {
                    "orders": {"$count": None},
                    "revenue": {"$sum": "price"},
                    "median_price": {"$median": "price"},
                },
            },
            match={"category": "books"},
            sorts={"revenue": "desc"},
            limit=10,
            skip=5,
        )

        assert pipeline == [
            {"$match": {"category": "books"}},
            {
                "$group": {
                    "_id": {"category": "$category"},
                    "orders": {"$sum": 1},
                    "revenue": {"$sum": "$price"},
                    "median_price": {
                        "$median": {"input": "$price", "method": "approximate"},
                    },
                },
            },
            {
                "$project": {
                    "_id": 0,
                    "category": "$_id.category",
                    "orders": 1,
                    "revenue": 1,
                    "median_price": 1,
                },
            },
            {"$sort": {"revenue": -1}},
            {"$skip": 5},
            {"$limit": 10},
        ]

    def test_renders_conditional_aggregate_pipeline(self) -> None:
        renderer = MongoQueryRenderer()

        _parsed, pipeline = renderer.render_aggregates(
            {
                "$computed": {
                    "mid_rows": {
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

        assert pipeline[0] == {
            "$group": {
                "_id": None,
                "mid_rows": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$gte": ["$price", 10]},
                                    {"$lte": ["$price", 20]},
                                ],
                            },
                            1,
                            0,
                        ],
                    },
                },
                "book_revenue": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$category", "books"]},
                            "$price",
                            0,
                        ],
                    },
                },
            },
        }

    def test_rejects_unknown_aggregate_sort_alias(self) -> None:
        renderer = MongoQueryRenderer()

        with pytest.raises(CoreException, match="Invalid aggregate sort fields"):
            renderer.render_aggregates(
                {"$computed": {"orders": {"$count": None}}},
                sorts={"missing": "asc"},
            )

    def test_renders_avg_min_max_median_aggregates(self) -> None:
        renderer = MongoQueryRenderer()
        _parsed, pipeline = renderer.render_aggregates(
            {
                "$groups": {"cat": "category"},
                "$computed": {
                    "avg_p": {"$avg": "price"},
                    "lo": {"$min": "price"},
                    "hi": {"$max": "price"},
                },
            },
        )
        group = pipeline[0]["$group"]
        assert group["avg_p"] == {"$avg": "$price"}
        assert group["lo"] == {"$min": "$price"}
        assert group["hi"] == {"$max": "$price"}
        _p2, pl2 = renderer.render_aggregates(
            {"$computed": {"md": {"$median": "p"}}},
        )
        assert pl2[0]["$group"]["md"] == {
            "$median": {"input": "$p", "method": "approximate"},
        }

    def test_renders_trunc_in_group_id(self) -> None:
        renderer = MongoQueryRenderer()
        _parsed, pipeline = renderer.render_aggregates(
            {
                "$groups": {
                    "cat": "category",
                    "week_start": {
                        "$trunc": {
                            "field": "created_at",
                            "unit": "week",
                            "timezone": "+03:00",
                        },
                    },
                },
                "$computed": {"n": {"$count": None}},
            },
        )
        group = pipeline[0]["$group"]
        assert group["_id"]["week_start"] == {
            "$dateTrunc": {
                "date": "$created_at",
                "unit": "week",
                "timezone": "+03:00",
                "startOfWeek": "monday",
            },
        }
        assert group["_id"]["cat"] == "$category"


class TestMongoQueryRendererExprPredicate:
    """Tests for :meth:`~MongoQueryRenderer.render_expr_predicate` (aggregation filters)."""

    def test_and_or_short_circuit(self) -> None:
        r = MongoQueryRenderer()
        e1 = r.render_expr_predicate(
            QueryAnd(
                (QueryField("a", "$eq", 1), QueryField("b", "$eq", 2)),
            ),
        )
        assert e1 == {"$and": [{"$eq": ["$a", 1]}, {"$eq": ["$b", 2]}]}
        one = r.render_expr_predicate(
            QueryAnd((QueryField("a", "$eq", 1),)),
        )
        assert one == {"$eq": ["$a", 1]}

    def test_not_expr_predicate(self) -> None:
        r = MongoQueryRenderer()
        out = r.render_expr_predicate(
            QueryNot(QueryField("a", "$eq", 1)),
        )
        assert out == {"$nor": [{"$eq": ["$a", 1]}]}

    def test_elem_expr_predicate_scalar_any(self) -> None:
        r = MongoQueryRenderer()
        out = r.render_expr_predicate(
            QueryElem(
                "tags",
                "$any",
                QueryField(ELEM_SCALAR_FIELD, "$eq", "z"),
            ),
        )
        assert "$or" in out

    def test_or_multi_and_empty(self) -> None:
        r = MongoQueryRenderer()
        empty = r.render_expr_predicate(QueryOr(()))
        assert empty == {"$const": False}
        two = r.render_expr_predicate(
            QueryOr(
                (QueryField("a", "$eq", 1), QueryField("b", "$eq", 2)),
            ),
        )
        assert two == {"$or": [{"$eq": ["$a", 1]}, {"$eq": ["$b", 2]}]}

    def test_field_predicate_comparators_and_null_empty(self) -> None:
        r = MongoQueryRenderer()
        assert r.render_expr_predicate(QueryField("n", "$gt", 1)) == {
            "$gt": ["$n", 1],
        }
        assert r.render_expr_predicate(QueryField("n", "$null", True)) == {
            "$eq": ["$n", None],
        }
        assert r.render_expr_predicate(QueryField("e", "$empty", False)) == {
            "$not": [{"$eq": ["$e", []]}],
        }

    def test_field_predicate_in_and_set_ops(self) -> None:
        r = MongoQueryRenderer()
        assert r.render_expr_predicate(QueryField("t", "$in", [1, 2])) == {
            "$in": ["$t", [1, 2]],
        }
        assert r.render_expr_predicate(QueryField("s", "$overlaps", [1, 2])) == {
            "$gt": [
                {
                    "$size": {
                        "$setIntersection": [
                            "$s",
                            [1, 2],
                        ],
                    },
                },
                0,
            ],
        }
        assert r.render_expr_predicate(QueryField("s", "$disjoint", [1])) == {
            "$eq": [{"$size": {"$setIntersection": ["$s", [1]]}}, 0],
        }
        with pytest.raises(CoreException, match="expects list"):
            r.render_expr_predicate(QueryField("t", "$in", 1))


class TestMongoComparePredicate:
    """Field-to-field compare rendering (``$expr``) and error branches."""

    @pytest.mark.parametrize(
        ("op", "mongo"),
        [
            ("$eq", "$eq"),
            ("$neq", "$ne"),
            ("$gt", "$gt"),
            ("$gte", "$gte"),
            ("$lt", "$lt"),
            ("$lte", "$lte"),
        ],
    )
    def test_compare_operators(self, op: str, mongo: str) -> None:
        r = MongoQueryRenderer()
        out = r.render(QueryCompare("a", op, "b"))  # type: ignore[arg-type]
        assert out == {"$expr": {mongo: ["$a", "$b"]}}

    def test_compare_neq_predicate(self) -> None:
        r = MongoQueryRenderer()
        out = r.render_expr_predicate(QueryCompare("a", "$neq", "b"))
        assert out == {"$expr": {"$ne": ["$a", "$b"]}}

    def test_compare_unknown_operator_raises(self) -> None:
        r = MongoQueryRenderer()
        with pytest.raises(CoreException, match="Unknown compare operator"):
            r.render(QueryCompare("a", "$bogus", "b"))  # type: ignore[arg-type]


class TestMongoExprPredicateBranches:
    """Remaining :meth:`render_expr_predicate` branches."""

    def test_and_empty_const_true(self) -> None:
        r = MongoQueryRenderer()
        assert r.render_expr_predicate(QueryAnd(())) == {"$const": True}

    def test_unknown_expression_raises(self) -> None:
        r = MongoQueryRenderer()
        with pytest.raises(CoreException, match="Unknown expression"):
            r.render_expr_predicate(_UnknownExpr())

    def test_field_neq_predicate(self) -> None:
        r = MongoQueryRenderer()
        assert r.render_expr_predicate(QueryField("n", "$neq", 3)) == {
            "$ne": ["$n", 3],
        }

    def test_nin_predicate(self) -> None:
        r = MongoQueryRenderer()
        assert r.render_expr_predicate(QueryField("t", "$nin", [1, 2])) == {
            "$not": [{"$in": ["$t", [1, 2]]}],
        }

    def test_superset_predicate(self) -> None:
        r = MongoQueryRenderer()
        assert r.render_expr_predicate(QueryField("s", "$superset", [1, 2])) == {
            "$setIsSubset": [[1, 2], "$s"],
        }

    def test_subset_predicate(self) -> None:
        r = MongoQueryRenderer()
        assert r.render_expr_predicate(QueryField("s", "$subset", [1, 2])) == {
            "$setIsSubset": ["$s", [1, 2]],
        }

    @pytest.mark.parametrize(
        "op",
        ["$nin", "$superset", "$subset", "$overlaps", "$disjoint"],
    )
    def test_list_predicate_scalar_raises(self, op: str) -> None:
        r = MongoQueryRenderer()
        with pytest.raises(CoreException, match="expects list"):
            r.render_expr_predicate(QueryField("s", op, 1))  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        ("op", "expected"),
        [
            (
                "$like",
                {"$regexMatch": {"input": "$t", "regex": "^a.*$"}},
            ),
            (
                "$ilike",
                {"$regexMatch": {"input": "$t", "regex": "^a.*$", "options": "i"}},
            ),
            (
                "$regex",
                {"$regexMatch": {"input": "$t", "regex": "a.*"}},
            ),
        ],
    )
    def test_text_predicate(self, op: str, expected: dict) -> None:
        r = MongoQueryRenderer()
        pattern = "a.*" if op == "$regex" else "a%"
        out = r.render_expr_predicate(QueryField("t", op, pattern))  # type: ignore[arg-type]
        assert out == expected


class TestMongoScalarElementMatch:
    """Scalar element-quantifier inner-predicate shapes (via ``render``)."""

    @staticmethod
    def _elem(quantifier: str, inner: QueryExpr) -> dict:
        r = MongoQueryRenderer()
        return r.render(QueryElem("tags", quantifier, inner))["$or"][1]["$and"][1]

    def test_scalar_and_single_field(self) -> None:
        inner = QueryAnd((QueryField(ELEM_SCALAR_FIELD, "$eq", "x"),))
        assert self._elem("$any", inner) == {"tags": "x"}

    def test_scalar_and_multiple_fields_raises(self) -> None:
        inner = QueryAnd(
            (
                QueryField(ELEM_SCALAR_FIELD, "$eq", "x"),
                QueryField(ELEM_SCALAR_FIELD, "$gt", 1),
            ),
        )
        r = MongoQueryRenderer()
        with pytest.raises(CoreException, match="one comparison only"):
            r.render(QueryElem("tags", "$any", inner))

    def test_scalar_or_single_collapses(self) -> None:
        inner = QueryOr((QueryField(ELEM_SCALAR_FIELD, "$eq", "x"),))
        assert self._elem("$any", inner) == {"tags": "x"}

    def test_scalar_or_any_uses_or(self) -> None:
        inner = QueryOr(
            (
                QueryField(ELEM_SCALAR_FIELD, "$eq", "x"),
                QueryField(ELEM_SCALAR_FIELD, "$eq", "y"),
            ),
        )
        out = self._elem("$any", inner)
        assert out == {"$or": [{"tags": "x"}, {"tags": "y"}]}

    def test_scalar_or_none_uses_and(self) -> None:
        inner = QueryOr(
            (
                QueryField(ELEM_SCALAR_FIELD, "$eq", "x"),
                QueryField(ELEM_SCALAR_FIELD, "$eq", "y"),
            ),
        )
        out = self._elem("$none", inner)
        assert out == {"$and": [{"$nor": [{"tags": "x"}]}, {"$nor": [{"tags": "y"}]}]}

    def test_scalar_or_all_uses_and(self) -> None:
        inner = QueryOr(
            (
                QueryField(ELEM_SCALAR_FIELD, "$eq", "x"),
                QueryField(ELEM_SCALAR_FIELD, "$eq", "y"),
            ),
        )
        out = self._elem("$all", inner)
        assert "$and" in out
        assert len(out["$and"]) == 2

    def test_scalar_invalid_inner_raises(self) -> None:
        # A scalar inner that is neither field/and/or (an empty QueryAnd is scalar
        # per ``elem_inner_is_scalar`` but hits the field-count guard); use a
        # QueryElem nested inner which is "scalar" only if it recurses — instead
        # force the default branch with a Compare wrapped where scalar detection
        # passes vacuously.
        r = MongoQueryRenderer()
        # empty QueryAnd -> elem_inner_is_scalar True, fields == [] -> count guard
        with pytest.raises(CoreException, match="one comparison only"):
            r.render(QueryElem("tags", "$any", QueryAnd(())))

    @pytest.mark.parametrize(
        ("op", "quantifier", "expected"),
        [
            ("$eq", "$any", {"tags": "v"}),
            ("$eq", "$none", {"$nor": [{"tags": "v"}]}),
        ],
    )
    def test_scalar_eq_quantifiers(
        self,
        op: str,
        quantifier: str,
        expected: dict,
    ) -> None:
        inner = QueryField(ELEM_SCALAR_FIELD, op, "v")  # type: ignore[arg-type]
        assert self._elem(quantifier, inner) == expected

    def test_scalar_eq_all_min_max(self) -> None:
        inner = QueryField(ELEM_SCALAR_FIELD, "$eq", "v")
        out = self._elem("$all", inner)
        assert out == {
            "$expr": {
                "$and": [
                    {"$eq": [{"$min": "$tags"}, "v"]},
                    {"$eq": [{"$max": "$tags"}, "v"]},
                ],
            },
        }

    @pytest.mark.parametrize(
        ("op", "quantifier", "expected"),
        [
            # $any -> agg "$max"; $all/$none -> agg "$min" (default)
            ("$gt", "$any", {"$expr": {"$gt": [{"$max": "$tags"}, 5]}}),
            ("$gte", "$all", {"$expr": {"$gte": [{"$min": "$tags"}, 5]}}),
            ("$gt", "$none", {"$expr": {"$gt": [{"$min": "$tags"}, 5]}}),
        ],
    )
    def test_scalar_ordinal_aggregations(
        self,
        op: str,
        quantifier: str,
        expected: dict,
    ) -> None:
        inner = QueryField(ELEM_SCALAR_FIELD, op, 5)  # type: ignore[arg-type]
        out = self._elem(quantifier, inner)
        assert out == expected

    def test_scalar_lt_any_uses_min(self) -> None:
        inner = QueryField(ELEM_SCALAR_FIELD, "$lt", 5)
        out = self._elem("$any", inner)
        assert out == {"$expr": {"$lt": [{"$min": "$tags"}, 5]}}

    def test_scalar_lte_all_uses_max(self) -> None:
        inner = QueryField(ELEM_SCALAR_FIELD, "$lte", 5)
        out = self._elem("$all", inner)
        assert out == {"$expr": {"$lte": [{"$max": "$tags"}, 5]}}

    @pytest.mark.parametrize(
        ("op", "quantifier", "expect_substr"),
        [
            ("$like", "$any", "$gt"),
            ("$ilike", "$none", "$eq"),
            ("$regex", "$all", "$size"),
        ],
    )
    def test_scalar_text_quantifiers(
        self,
        op: str,
        quantifier: str,
        expect_substr: str,
    ) -> None:
        pattern = "a.*" if op == "$regex" else "%a%"
        inner = QueryField(ELEM_SCALAR_FIELD, op, pattern)  # type: ignore[arg-type]
        out = self._elem(quantifier, inner)
        assert "$expr" in out
        assert expect_substr in str(out)
        assert "$filter" in str(out)

    def test_scalar_ilike_carries_options(self) -> None:
        inner = QueryField(ELEM_SCALAR_FIELD, "$ilike", "%a%")
        out = self._elem("$any", inner)
        assert "options" in str(out)

    def test_scalar_neq_any_uses_filter(self) -> None:
        inner = QueryField(ELEM_SCALAR_FIELD, "$neq", "v")
        out = self._elem("$any", inner)
        assert out == {
            "$expr": {
                "$gt": [
                    {
                        "$size": {
                            "$filter": {
                                "input": "$tags",
                                "cond": {"$ne": ["$$this", "v"]},
                            },
                        },
                    },
                    0,
                ],
            },
        }

    def test_scalar_neq_all_unsupported_raises(self) -> None:
        inner = QueryField(ELEM_SCALAR_FIELD, "$neq", "v")
        r = MongoQueryRenderer()
        with pytest.raises(CoreException, match="Unsupported scalar element operator"):
            r.render(QueryElem("tags", "$all", inner))


class TestMongoObjectElementMatch:
    """Object element-quantifier inner-predicate shapes."""

    @staticmethod
    def _match(inner: QueryExpr) -> dict:
        r = MongoQueryRenderer()
        return r.render(QueryElem("items", "$any", inner))["$or"][1]["$and"][1]

    def test_single_field(self) -> None:
        inner = QueryField("status", "$eq", "open")
        assert self._match(inner) == {"items": {"$elemMatch": {"status": "open"}}}

    def test_and_fields(self) -> None:
        inner = QueryAnd(
            (
                QueryField("status", "$eq", "open"),
                QueryField("qty", "$gte", 1),
            ),
        )
        assert self._match(inner) == {
            "items": {"$elemMatch": {"status": "open", "qty": {"$gte": 1}}},
        }

    def test_field_text_op(self) -> None:
        inner = QueryField("name", "$ilike", "%x%")
        out = self._match(inner)
        spec = out["items"]["$elemMatch"]["name"]
        assert spec == {"$regex": "^.*x.*$", "$options": "i"}

    def test_field_text_op_no_options(self) -> None:
        inner = QueryField("name", "$like", "%x%")
        out = self._match(inner)
        spec = out["items"]["$elemMatch"]["name"]
        assert spec == {"$regex": "^.*x.*$"}

    def test_or_branches(self) -> None:
        inner = QueryOr(
            (
                QueryField("status", "$eq", "open"),
                QueryField("status", "$eq", "closed"),
            ),
        )
        out = self._match(inner)
        em = out["items"]["$elemMatch"]
        assert em == {"$or": [{"status": "open"}, {"status": "closed"}]}

    def test_invalid_object_inner_raises(self) -> None:
        r = MongoQueryRenderer()
        with pytest.raises(CoreException, match="Invalid object element inner"):
            r.render(QueryElem("items", "$any", QueryCompare("a", "$eq", "b")))

    def test_none_object_uses_nor_elem_match(self) -> None:
        r = MongoQueryRenderer()
        inner = QueryAnd((QueryField("status", "$eq", "open"),))
        out = r.render(QueryElem("items", "$none", inner))["$or"][1]["$and"][1]
        assert out == {"$nor": [{"items": {"$elemMatch": {"status": "open"}}}]}


class TestMongoRenderExprEmptyParts:
    """``_render_expr`` branches where all child parts render empty."""

    def test_and_all_children_empty(self) -> None:
        r = MongoQueryRenderer()
        expr = QueryAnd((QueryAnd(()), QueryAnd(())))
        assert r.render(expr) == {}

    def test_or_all_children_empty(self) -> None:
        r = MongoQueryRenderer()
        expr = QueryOr((QueryAnd(()), QueryAnd(())))
        assert r.render(expr) == {"$expr": False}


class TestMongoRendererInternalEdges:
    """Hard-to-reach internal branches exercised via direct calls."""

    def test_unknown_text_operator_raises(self) -> None:
        r = MongoQueryRenderer()
        with pytest.raises(CoreException, match="Unknown text operator"):
            r._text_regex_and_options("$nope", "p")  # type: ignore[arg-type]  # noqa: SLF001

    def test_invalid_scalar_inner_default_raises(self) -> None:
        r = MongoQueryRenderer()
        with pytest.raises(CoreException, match="Invalid scalar element inner"):
            r._render_elem_scalar_match(  # noqa: SLF001
                "tags",
                "$any",
                QueryCompare("a", "$eq", "b"),
            )


class TestMongoConfigFlags:
    """Config-flag-dependent null/empty rendering branches."""

    def test_null_explicit_with_exists_guard(self) -> None:
        r = MongoQueryRenderer(null_matches_missing=False)
        assert r.render(QueryField("z", "$null", True)) == {
            "$and": [{"z": None}, {"z": {"$exists": True}}],
        }

    def test_not_null_without_exists_guard(self) -> None:
        r = MongoQueryRenderer(require_exists_for_not_null=False)
        assert r.render(QueryField("z", "$null", False)) == {"z": {"$ne": None}}

    def test_not_empty_without_exists_guard(self) -> None:
        r = MongoQueryRenderer(require_exists_for_not_null=False)
        assert r.render(QueryField("e", "$empty", False)) == {"e": {"$ne": []}}


class TestMongoAggregateInternals:
    """Directly-constructed computed fields for hard-to-reach branches."""

    def test_iana_timezone_in_trunc(self) -> None:
        renderer = MongoQueryRenderer()
        _parsed, pipeline = renderer.render_aggregates(
            {
                "$groups": {
                    "wk": {
                        "$trunc": {
                            "field": "created_at",
                            "unit": "week",
                            "timezone": "Europe/Paris",
                        },
                    },
                },
                "$computed": {"n": {"$count": None}},
            },
        )
        tz = pipeline[0]["$group"]["_id"]["wk"]["$dateTrunc"]["timezone"]
        assert tz == "Europe/Paris"

    def test_computed_field_without_path_raises(self) -> None:
        renderer = MongoQueryRenderer()
        computed = AggregateComputedField(
            alias="bad",
            function="$sum",
            field=None,
        )
        with pytest.raises(CoreException, match="no field path"):
            renderer._render_aggregate_function(computed)  # noqa: SLF001

    def test_conditional_value_parses_filter_when_not_prepared(self) -> None:
        renderer = MongoQueryRenderer()
        computed = AggregateComputedField(
            alias="c",
            function="$count",
            field=None,
            filter={"$values": {"category": "books"}},
            parsed_filter=None,
        )
        out = renderer._conditional_value(computed, 1, 0)  # noqa: SLF001
        assert out == {
            "$cond": [{"$eq": ["$category", "books"]}, 1, 0],
        }
