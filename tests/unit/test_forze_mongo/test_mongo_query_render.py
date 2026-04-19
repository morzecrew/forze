"""Unit tests for :class:`~forze_mongo.kernel.query.render.MongoQueryRenderer`."""

from __future__ import annotations

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
