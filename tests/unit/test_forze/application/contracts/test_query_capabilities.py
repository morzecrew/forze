"""Backend query-capability validation: unsupported features fail clean, not internal.

The validator walks a parsed filter AST and rejects any feature the target backend's
:class:`QueryCapabilities` does not advertise, with a ``precondition`` (code
``query_feature_unsupported``) — never a render-time ``internal`` (500).
"""

from __future__ import annotations

import pytest

from forze.application.contracts.querying import (
    FULL_QUERY_CAPABILITIES,
    UNSUPPORTED_QUERY_FEATURE_CODE,
    QueryCapabilities,
    QueryFilterExpressionParser,
    validate_query_capabilities,
)
from forze.base.exceptions import CoreException, ExceptionKind

pytestmark = pytest.mark.unit


def _ast(expr: dict):
    return QueryFilterExpressionParser.parse(expr)


def _check(expr: dict, caps: QueryCapabilities) -> None:
    validate_query_capabilities(_ast(expr), caps, backend="test")


# ....................... #


class TestFullCapabilities:
    @pytest.mark.parametrize(
        "expr",
        [
            {"$values": {"name": {"$eq": "x"}}},
            {"$values": {"name": {"$regex": "a.*"}}},
            {"$values": {"tags": {"$superset": ["a", "b"]}}},
            {"$values": {"tags": {"$any": "urgent"}}},
            {"$values": {"tags": {"$any": {"$in": ["a", "b"]}}}},
            {"$values": {"scores": {"$all": {"$gte": 10}}}},
            {"$values": {"scores": {"$any": {"$gt": 1, "$lt": 3}}}},
            {"$fields": {"a": {"$lte": "b"}}},
            {"$not": {"$values": {"a": 1}}},
            {"$or": [{"$values": {"a": 1}}, {"$values": {"b": {"$in": [1, 2]}}}]},
        ],
    )
    def test_everything_passes(self, expr: dict) -> None:
        # The mock's superset compiles all of it — the canonical reference.
        _check(expr, FULL_QUERY_CAPABILITIES)


class TestRejections:
    def test_unsupported_top_level_operator(self) -> None:
        caps = QueryCapabilities(value_ops=frozenset({"$eq", "$neq"}))

        with pytest.raises(CoreException, match=r"\$regex") as ei:
            _check({"$values": {"name": {"$regex": "a.*"}}}, caps)

        assert ei.value.kind is ExceptionKind.PRECONDITION
        assert ei.value.code == UNSUPPORTED_QUERY_FEATURE_CODE
        assert "test" in str(ei.value)

    def test_quantifiers_unsupported(self) -> None:
        caps = QueryCapabilities(supports_quantifiers=False)

        with pytest.raises(CoreException, match="element quantifier"):
            _check({"$values": {"tags": {"$any": "x"}}}, caps)

    def test_nested_quantifiers_unsupported(self) -> None:
        # Quantifiers allowed, but not nested ones (a backend that compiles a single
        # quantifier but cannot nest one inside another's element predicate).
        caps = QueryCapabilities(supports_nested_quantifiers=False)

        # A single (non-nested) quantifier is fine.
        _check({"$values": {"items": {"$any": {"$values": {"qty": {"$gte": 1}}}}}}, caps)

        # A quantifier inside another's predicate is rejected.
        with pytest.raises(CoreException, match="nested element quantifiers"):
            _check(
                {"$values": {"items": {"$any": {"$values": {"tags": {"$any": "x"}}}}}},
                caps,
            )

    def test_negation_unsupported(self) -> None:
        caps = QueryCapabilities(supports_negation=False)

        with pytest.raises(CoreException, match="negation"):
            _check({"$not": {"$values": {"a": 1}}}, caps)

    def test_field_compare_unsupported(self) -> None:
        caps = QueryCapabilities(supports_field_compare=False)

        with pytest.raises(CoreException, match="field-to-field"):
            _check({"$fields": {"a": {"$eq": "b"}}}, caps)

    def test_element_op_distinct_from_top_level(self) -> None:
        # An op allowed at top level but not inside an element quantifier is rejected
        # only in the element context (the two axes are independent).
        caps = QueryCapabilities(
            value_ops=frozenset({"$eq", "$gte", "$regex"}),
            element_ops=frozenset({"$eq"}),  # no $regex inside elements
        )

        # Top-level $regex is fine.
        _check({"$values": {"name": {"$regex": "a.*"}}}, caps)

        # The same op inside a quantifier is rejected.
        with pytest.raises(CoreException, match=r"\$regex.*element"):
            _check(
                {"$values": {"items": {"$any": {"$values": {"name": {"$regex": "a.*"}}}}}},
                caps,
            )

    def test_rejection_is_found_nested_under_combinators(self) -> None:
        caps = QueryCapabilities(supports_quantifiers=False)

        expr = {
            "$or": [
                {"$values": {"a": 1}},
                {"$and": [{"$values": {"tags": {"$any": "x"}}}]},
            ]
        }

        with pytest.raises(CoreException, match="element quantifier"):
            _check(expr, caps)


class TestNoOpWhenSupported:
    def test_restricted_caps_still_pass_supported_features(self) -> None:
        # A search-engine-shaped backend: equality/range/membership, negation, but no
        # quantifiers, set ops, text ops, or field compare.
        caps = QueryCapabilities(
            value_ops=frozenset({"$eq", "$neq", "$gt", "$gte", "$lt", "$lte", "$in", "$nin", "$null"}),
            element_ops=frozenset(),
            supports_quantifiers=False,
            supports_field_compare=False,
        )

        _check(
            {"$and": [{"$values": {"age": {"$gte": 18}}}, {"$not": {"$values": {"banned": True}}}]},
            caps,
        )
