"""Unit tests for :class:`~forze_mongo.kernel.query.render.MongoQueryRenderer`."""

from __future__ import annotations

from uuid import uuid4

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.query import (
    QueryAnd,
    QueryExpr,
    QueryField,
    QueryOr,
)
from forze.base.errors import CoreError
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
        with pytest.raises(CoreError, match="Unknown expression"):
            r.render(_UnknownExpr())

    def test_unknown_operator_raises(self) -> None:
        r = MongoQueryRenderer()
        with pytest.raises(CoreError, match="Unknown operator"):
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
        with pytest.raises(CoreError, match="expects list"):
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
        with pytest.raises(CoreError, match="expects list"):
            r.render(QueryField("s", "$subset", 1))

    def test_null_default_matches_missing(self) -> None:
        r = MongoQueryRenderer(null_matches_missing=True, require_exists_for_not_null=True)
        assert r.render(QueryField("z", "$null", True)) == {"z": None}
        assert r.render(QueryField("z", "$null", False)) == {
            "$and": [{"z": {"$ne": None}}, {"z": {"$exists": True}}],
        }

    def test_null_explicit_missing_only(self) -> None:
        r = MongoQueryRenderer(null_matches_missing=False, require_exists_for_not_null=False)
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
                "$fields": {"category": "category"},
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
                                "$fields": {"price": {"$gte": 10, "$lte": 20}},
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

        with pytest.raises(CoreError, match="Invalid aggregate sort fields"):
            renderer.render_aggregates(
                {"$computed": {"orders": {"$count": None}}},
                sorts={"missing": "asc"},
            )

    def test_renders_avg_min_max_median_aggregates(self) -> None:
        renderer = MongoQueryRenderer()
        _parsed, pipeline = renderer.render_aggregates(
            {
                "$fields": {"cat": "category"},
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
        with pytest.raises(CoreError, match="expects list"):
            r.render_expr_predicate(QueryField("t", "$in", 1))
