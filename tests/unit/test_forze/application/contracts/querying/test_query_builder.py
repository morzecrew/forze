"""The fluent query builder (`Q`): lowers to the same dict/AST as the wire form.

The builder is pure sugar — every case asserts the lowered dict equals the hand-written
form a caller would otherwise type, and that it parses to the same `QueryExpr`. The builder
does not re-validate; malformed *element-context* compositions (`$or`/`$not` inside a
quantifier, mixing scalar-element and object-field predicates) raise at build time, and
malformed *queries* surface from the parser exactly as a bad dict would.
"""

from __future__ import annotations

from typing import Any

import pytest

from forze.application.contracts.querying import (
    Q,
    QueryFilterExpressionParser,
)
from forze.application.contracts.querying.builder import FieldRef, QueryCondition
from forze.base.exceptions import CoreException

pytestmark = pytest.mark.unit


def _parse(d: dict[str, Any]):
    return QueryFilterExpressionParser.parse(d)


# Each pair: a built condition and the equivalent hand-written dict.
EQUIVALENT: tuple[tuple[QueryCondition, dict[str, Any]], ...] = (
    # value operators
    (Q.field("age").gt(18), {"$values": {"age": {"$gt": 18}}}),
    (Q.field("age").gte(18), {"$values": {"age": {"$gte": 18}}}),
    (Q.field("age").lt(18), {"$values": {"age": {"$lt": 18}}}),
    (Q.field("age").lte(18), {"$values": {"age": {"$lte": 18}}}),
    (Q.field("name").eq("x"), {"$values": {"name": {"$eq": "x"}}}),
    (Q.field("name").neq("x"), {"$values": {"name": {"$neq": "x"}}}),
    (Q.field("s").in_(["a", "b"]), {"$values": {"s": {"$in": ["a", "b"]}}}),
    (Q.field("s").nin(["a", "b"]), {"$values": {"s": {"$nin": ["a", "b"]}}}),
    (Q.field("n").like("a%"), {"$values": {"n": {"$like": "a%"}}}),
    (Q.field("n").ilike("a%"), {"$values": {"n": {"$ilike": "a%"}}}),
    (Q.field("n").regex("^a"), {"$values": {"n": {"$regex": "^a"}}}),
    (Q.field("d").is_null(), {"$values": {"d": {"$null": True}}}),
    (Q.field("d").is_null(False), {"$values": {"d": {"$null": False}}}),
    (Q.field("t").is_empty(), {"$values": {"t": {"$empty": True}}}),
    (Q.field("t").superset(["a"]), {"$values": {"t": {"$superset": ["a"]}}}),
    (Q.field("t").subset(["a"]), {"$values": {"t": {"$subset": ["a"]}}}),
    (Q.field("t").disjoint(["a"]), {"$values": {"t": {"$disjoint": ["a"]}}}),
    (Q.field("t").overlaps(["a"]), {"$values": {"t": {"$overlaps": ["a"]}}}),
    # hierarchy
    (
        Q.field("path").descendant_of("top.science"),
        {"$values": {"path": {"$descendant_of": "top.science"}}},
    ),
    (
        Q.field("path").ancestor_of(["a", "b"]),
        {"$values": {"path": {"$ancestor_of": ["a", "b"]}}},
    ),
    # field-to-field comparison
    (Q.field("a").gt(Q.field("b")), {"$fields": {"a": {"$gt": "b"}}}),
    (Q.field("a").eq(Q.field("b")), {"$fields": {"a": {"$eq": "b"}}}),
    # combinators
    (
        Q.field("age").gt(18) & Q.field("name").like("a%"),
        {
            "$and": [
                {"$values": {"age": {"$gt": 18}}},
                {"$values": {"name": {"$like": "a%"}}},
            ]
        },
    ),
    (
        Q.field("a").eq(1) | Q.field("b").eq(2),
        {"$or": [{"$values": {"a": {"$eq": 1}}}, {"$values": {"b": {"$eq": 2}}}]},
    ),
    (
        ~Q.field("banned").eq(True),
        {"$not": {"$values": {"banned": {"$eq": True}}}},
    ),
    # quantifiers
    (
        Q.field("tags").any(Q.elem().like("h%")),
        {"$values": {"tags": {"$any": {"$like": "h%"}}}},
    ),
    (
        Q.field("tags").any("hot"),  # scalar shorthand == Q.elem().eq("hot")
        {"$values": {"tags": {"$any": {"$eq": "hot"}}}},
    ),
    (
        Q.field("scores").all(Q.elem().gte(1) & Q.elem().lt(9)),
        {"$values": {"scores": {"$all": {"$gte": 1, "$lt": 9}}}},
    ),
    (
        Q.field("items").any(Q.field("qty").gte(1) & Q.field("sku").like("a%")),
        {
            "$values": {
                "items": {"$any": {"$values": {"qty": {"$gte": 1}, "sku": {"$like": "a%"}}}}
            }
        },
    ),
    (
        Q.field("items").any(Q.field("tags").any("hot")),
        {"$values": {"items": {"$any": {"$values": {"tags": {"$any": {"$eq": "hot"}}}}}}},
    ),
    (
        Q.field("matrix").any(Q.elem().any("x")),
        {"$values": {"matrix": {"$any": {"$any": {"$eq": "x"}}}}},
    ),
)


class TestLoweringEquivalence:
    @pytest.mark.parametrize(("built", "expected"), EQUIVALENT)
    def test_builds_expected_dict(
        self, built: QueryCondition, expected: dict[str, Any]
    ) -> None:
        assert built.build() == expected

    @pytest.mark.parametrize(("built", "expected"), EQUIVALENT)
    def test_parses_to_same_ast(
        self, built: QueryCondition, expected: dict[str, Any]
    ) -> None:
        # The built dict and the hand-written dict parse to identical ASTs.
        assert built.to_ast() == _parse(expected)


class TestCombinatorShape:
    def test_chained_and_flattens(self) -> None:
        built = Q.field("a").eq(1) & Q.field("b").eq(2) & Q.field("c").eq(3)
        d = built.build()
        assert list(d) == ["$and"]
        assert len(d["$and"]) == 3  # flat, not nested

    def test_chained_or_flattens(self) -> None:
        built = Q.field("a").eq(1) | Q.field("b").eq(2) | Q.field("c").eq(3)
        assert len(built.build()["$or"]) == 3

    def test_q_and_explicit_matches_operator(self) -> None:
        a, b = Q.field("a").eq(1), Q.field("b").eq(2)
        assert Q.and_(a, b).build() == (a & b).build()
        assert Q.or_(a, b).build() == (a | b).build()
        assert Q.not_(a).build() == (~a).build()

    def test_single_condition_and_is_identity(self) -> None:
        a = Q.field("a").eq(1)
        assert Q.and_(a).build() == a.build()

    def test_mixed_and_or_with_not(self) -> None:
        built = (Q.field("a").eq(1) | Q.field("b").eq(2)) & ~Q.field("c").eq(3)
        assert built.build() == {
            "$and": [
                {"$or": [{"$values": {"a": {"$eq": 1}}}, {"$values": {"b": {"$eq": 2}}}]},
                {"$not": {"$values": {"c": {"$eq": 3}}}},
            ]
        }


class TestRepr:
    def test_repr_is_informative(self) -> None:
        # attrs-generated repr exposes the field name and operator.
        r = repr(Q.field("age").gt(1))
        assert "age" in r and "$gt" in r


class TestBuildTimeErrors:
    def test_empty_field_name_rejected(self) -> None:
        with pytest.raises(CoreException, match="non-empty"):
            Q.field("   ")

    def test_field_operand_only_valid_for_comparison(self) -> None:
        # A field operand means field-compare and is only meaningful on the six comparison
        # methods; passed to a text op it raises a clean error (not a deep TypeError).
        with pytest.raises(CoreException, match="field operand"):
            Q.field("a").like(Q.field("b"))

    def test_field_operand_to_membership_rejected(self) -> None:
        with pytest.raises(CoreException, match="field reference"):
            Q.field("a").in_(Q.field("b"))

    def test_or_inside_quantifier_rejected(self) -> None:
        # $or has no element-constraint form — surface it when lowering, not silently.
        cond = Q.field("items").any(Q.field("a").eq(1) | Q.field("b").eq(2))
        with pytest.raises(CoreException, match="not expressible"):
            cond.build()

    def test_not_inside_quantifier_rejected(self) -> None:
        cond = Q.field("items").any(~Q.field("a").eq(1))
        with pytest.raises(CoreException, match="not expressible"):
            cond.build()

    def test_mixing_scalar_and_object_element_predicates_rejected(self) -> None:
        cond = Q.field("items").any(Q.elem().gt(1) & Q.field("qty").gte(1))
        with pytest.raises(CoreException, match="scalar-element|combined"):
            cond.build()

    def test_q_and_requires_a_condition(self) -> None:
        with pytest.raises(CoreException, match="at least one"):
            Q.and_()

    def test_q_or_requires_a_condition(self) -> None:
        with pytest.raises(CoreException, match="at least one"):
            Q.or_()


class TestBuilderCoverage:
    """Less-travelled lowering paths and grammar-limit guards."""

    def test_none_quantifier(self) -> None:
        assert Q.field("tags").none("x").build() == {
            "$values": {"tags": {"$none": {"$eq": "x"}}}
        }

    def test_q_or_single_condition_is_identity(self) -> None:
        a = Q.field("a").eq(1)
        assert Q.or_(a).build() == a.build()

    def test_single_object_field_quantifier_inner(self) -> None:
        # A lone object-field predicate as the quantifier inner lowers via $values.
        assert Q.field("items").any(Q.field("qty").gte(1)).build() == {
            "$values": {"items": {"$any": {"$values": {"qty": {"$gte": 1}}}}}
        }

    def test_object_merge_with_nested_quantifier_entry(self) -> None:
        # An object element predicate that conjoins a nested quantifier with a field.
        built = Q.field("items").any(
            Q.field("subs").any("x") & Q.field("a").eq(1)
        )
        assert built.build() == {
            "$values": {
                "items": {
                    "$any": {
                        "$values": {
                            "subs": {"$any": {"$eq": "x"}},
                            "a": {"$eq": 1},
                        }
                    }
                }
            }
        }

    def test_scalar_array_of_arrays_quantifier_cannot_combine(self) -> None:
        built = Q.field("matrix").any(Q.elem().any("x") & Q.field("a").eq(1))
        with pytest.raises(CoreException, match="array-of-arrays"):
            built.build()

    def test_or_node_cannot_be_element_values_entry(self) -> None:
        built = Q.field("items").any(
            (Q.field("a").eq(1) | Q.field("b").eq(2)) & Q.field("c").eq(1)
        )
        with pytest.raises(CoreException, match="combined inside an element"):
            built.build()

    def test_base_condition_filter_is_abstract(self) -> None:
        with pytest.raises(NotImplementedError):
            QueryCondition().build()


class TestTypeSurface:
    def test_field_returns_field_ref(self) -> None:
        assert isinstance(Q.field("x"), FieldRef)

    def test_leaf_and_combinator_are_query_conditions(self) -> None:
        assert isinstance(Q.field("x").eq(1), QueryCondition)
        assert isinstance(Q.field("x").eq(1) & Q.field("y").eq(2), QueryCondition)
