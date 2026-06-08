"""Tests for queryable-field allow-sets and boundary enforcement primitives."""

from __future__ import annotations

import pytest

from forze.application.contracts.querying import (
    QueryFieldGuard,
    QueryFieldPolicy,
    collect_filter_field_roots,
    validate_filterable_fields,
    validate_sortable_fields,
)
from forze.base.exceptions import CoreException

pytestmark = pytest.mark.unit


class TestCollectFilterFieldRoots:
    def test_values_predicate(self) -> None:
        roots = collect_filter_field_roots({"$values": {"status": "active"}})
        assert roots == frozenset({"status"})

    def test_logical_combinators(self) -> None:
        expr = {
            "$and": [
                {"$values": {"status": "active"}},
                {"$or": [
                    {"$values": {"price": {"$gte": 10}}},
                    {"$not": {"$values": {"title": "x"}}},
                ]},
            ]
        }
        assert collect_filter_field_roots(expr) == frozenset({"status", "price", "title"})

    def test_dot_path_reduced_to_root(self) -> None:
        roots = collect_filter_field_roots({"$values": {"meta.score": {"$gt": 1}}})
        assert roots == frozenset({"meta"})

    def test_field_comparison(self) -> None:
        roots = collect_filter_field_roots(
            {"$fields": {"starts_at": {"$lte": "ends_at"}}}
        )
        assert roots == frozenset({"starts_at", "ends_at"})

    def test_element_quantifier_collects_path_not_inner(self) -> None:
        # Object-array element: only the array field `items` is a top-level reference;
        # `status` is element-relative and must not leak into the root set.
        expr = {"$values": {"items": {"$any": {"$values": {"status": "open"}}}}}
        assert collect_filter_field_roots(expr) == frozenset({"items"})

    def test_scalar_element_quantifier(self) -> None:
        assert collect_filter_field_roots(
            {"$values": {"tags": {"$any": "urgent"}}}
        ) == frozenset({"tags"})


class TestValidateFilterable:
    def test_allowed_passes(self) -> None:
        validate_filterable_fields(
            {"$values": {"title": "x"}}, allowed=frozenset({"title"}), spec_name="notes"
        )

    def test_none_is_noop(self) -> None:
        validate_filterable_fields(None, allowed=frozenset({"title"}), spec_name="notes")

    def test_forbidden_raises(self) -> None:
        with pytest.raises(CoreException) as ei:
            validate_filterable_fields(
                {"$values": {"secret": "x"}},
                allowed=frozenset({"title"}),
                spec_name="notes",
            )
        assert ei.value.code == "field_not_filterable"


class TestValidateSortable:
    def test_allowed_passes(self) -> None:
        validate_sortable_fields(
            {"title": "asc"}, allowed=frozenset({"title"}), spec_name="notes"
        )

    def test_none_and_empty_are_noop(self) -> None:
        validate_sortable_fields(None, allowed=frozenset({"title"}), spec_name="notes")
        validate_sortable_fields({}, allowed=frozenset({"title"}), spec_name="notes")

    def test_dot_path_root_checked(self) -> None:
        validate_sortable_fields(
            {"meta.score": "desc"}, allowed=frozenset({"meta"}), spec_name="notes"
        )

    def test_forbidden_raises(self) -> None:
        with pytest.raises(CoreException) as ei:
            validate_sortable_fields(
                {"created_at": "desc"},
                allowed=frozenset({"title"}),
                spec_name="notes",
            )
        assert ei.value.code == "field_not_sortable"


class TestQueryFieldGuard:
    def test_skips_unrestricted_axes(self) -> None:
        # filterable restricted, sortable None → only filters checked.
        guard = QueryFieldGuard(
            policy=QueryFieldPolicy(filterable={"title"}),
            spec_name="notes",
        )
        # Sort by anything is fine (sortable is None = unrestricted).
        guard.check(filters={"$values": {"title": "x"}}, sorts={"created_at": "desc"})

    def test_enforces_both_axes(self) -> None:
        guard = QueryFieldGuard(
            policy=QueryFieldPolicy(filterable={"title"}, sortable={"title"}),
            spec_name="notes",
        )
        with pytest.raises(CoreException) as ei:
            guard.check(filters={"$values": {"secret": "x"}})
        assert ei.value.code == "field_not_filterable"

        with pytest.raises(CoreException) as ei2:
            guard.check(sorts={"created_at": "asc"})
        assert ei2.value.code == "field_not_sortable"
