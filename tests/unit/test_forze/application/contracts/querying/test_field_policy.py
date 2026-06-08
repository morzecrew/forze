"""Tests for queryable-field allow-sets and boundary enforcement primitives."""

from __future__ import annotations

import pytest

from forze.application.contracts.querying import (
    QueryFieldGuard,
    QueryFieldPolicy,
    collect_aggregate_field_roots,
    collect_aggregate_filter_expressions,
    collect_filter_field_roots,
    validate_aggregatable_fields,
    validate_filterable_fields,
    validate_sortable_fields,
)
from forze.base.exceptions import CoreException

pytestmark = pytest.mark.unit

# A grouped aggregate over `category`, summing `price`, with a per-metric filter on `price`.
_AGG = {
    "$groups": {"category": "category"},
    "$computed": {
        "orders": {"$count": None},
        "revenue": {"$sum": "price"},
        "premium_revenue": {
            "$sum": {"field": "price", "filter": {"$values": {"price": {"$gte": 20}}}}
        },
    },
}


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


class TestCollectAggregateFields:
    def test_group_and_measure_roots(self) -> None:
        # `category` (group) + `price` (computed measure); per-metric filter excluded here.
        assert collect_aggregate_field_roots(_AGG) == frozenset({"category", "price"})

    def test_trunc_group_source_field(self) -> None:
        agg = {
            "$groups": {"day": {"$trunc": {"field": "created_at", "unit": "day"}}},
            "$computed": {"n": {"$count": None}},
        }
        assert collect_aggregate_field_roots(agg) == frozenset({"created_at"})

    def test_per_metric_filters_extracted(self) -> None:
        filters = collect_aggregate_filter_expressions(_AGG)
        assert filters == ({"$values": {"price": {"$gte": 20}}},)


class TestValidateAggregatable:
    def test_allowed_passes(self) -> None:
        validate_aggregatable_fields(
            _AGG, allowed=frozenset({"category", "price"}), spec_name="orders"
        )

    def test_none_is_noop(self) -> None:
        validate_aggregatable_fields(None, allowed=frozenset(), spec_name="orders")

    def test_forbidden_raises(self) -> None:
        with pytest.raises(CoreException) as ei:
            validate_aggregatable_fields(
                _AGG, allowed=frozenset({"category"}), spec_name="orders"
            )
        assert ei.value.code == "field_not_aggregatable"


class TestGuardAggregates:
    def test_aggregatable_axis_enforced(self) -> None:
        guard = QueryFieldGuard(
            policy=QueryFieldPolicy(aggregatable={"category"}),
            spec_name="orders",
        )
        with pytest.raises(CoreException) as ei:
            guard.check(aggregates=_AGG)  # `price` measure not allowed
        assert ei.value.code == "field_not_aggregatable"

    def test_per_metric_filter_governed_by_filterable(self) -> None:
        # aggregatable allows the group/measure fields, but the per-metric filter on `price`
        # is checked against the filterable axis (which excludes `price`).
        guard = QueryFieldGuard(
            policy=QueryFieldPolicy(
                aggregatable={"category", "price"}, filterable={"category"}
            ),
            spec_name="orders",
        )
        with pytest.raises(CoreException) as ei:
            guard.check(aggregates=_AGG)
        assert ei.value.code == "field_not_filterable"

    def test_unrestricted_aggregatable_skips(self) -> None:
        # No aggregatable / filterable axes → aggregate passes untouched.
        guard = QueryFieldGuard(
            policy=QueryFieldPolicy(sortable={"category"}), spec_name="orders"
        )
        guard.check(aggregates=_AGG)
