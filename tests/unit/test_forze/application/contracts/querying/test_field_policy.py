"""Tests for queryable-field allow-sets and boundary enforcement primitives."""

from __future__ import annotations

from enum import StrEnum

import pytest

from forze.application.contracts.querying import (
    QueryFieldGuard,
    QueryFieldPolicy,
    QueryFilterLimits,
    collect_aggregate_field_roots,
    collect_aggregate_filter_expressions,
    collect_filter_field_roots,
    validate_aggregatable_fields,
    validate_filterable_fields,
    validate_sortable_fields,
)
from forze.base.exceptions import CoreException, ExceptionKind

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


class TestGuardFilterLimits:
    _FILTERS = {"$values": {"title": {"$in": [f"t{i}" for i in range(1_500)]}}}

    def test_parses_under_the_spec_limits(self) -> None:
        QueryFieldGuard(
            policy=QueryFieldPolicy(filterable={"title"}),
            spec_name="notes",
            filter_limits=QueryFilterLimits(max_in_size=2_000),
        ).check(filters=self._FILTERS)

    def test_parses_aggregate_filters_under_the_spec_limits(self) -> None:
        aggregates = {"$computed": {"n": {"$count": {"filter": self._FILTERS}}}}

        QueryFieldGuard(
            policy=QueryFieldPolicy(filterable={"title"}, aggregatable={"title"}),
            spec_name="notes",
            filter_limits=QueryFilterLimits(max_in_size=2_000),
        ).check(aggregates=aggregates)

    def test_keeps_the_default_limits_without_them(self) -> None:
        guard = QueryFieldGuard(policy=QueryFieldPolicy(filterable={"title"}), spec_name="notes")

        with pytest.raises(CoreException, match="1000"):
            guard.check(filters=self._FILTERS)


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


class TestGuardAggregateAliases:
    """`$having` and aggregate sorts name output aliases; each is checked as its source."""

    _GUARD = QueryFieldGuard(
        policy=QueryFieldPolicy(
            filterable={"category"},
            sortable={"category"},
            aggregatable={"category", "price", "created_at"},
        ),
        spec_name="orders",
    )

    @pytest.mark.parametrize(
        ("having", "source"),
        [
            ({"$values": {"p": {"$gt": 10}}}, "price"),  # group alias over `price`
            ({"$values": {"total": {"$gt": 10}}}, "price"),  # `$sum` of `price`
            ({"$values": {"total.x": {"$gt": 10}}}, "price"),  # dotted, by its root
            ({"$values": {"p90": {"$gt": 10}}}, "price"),  # `$percentile` with `p`
            ({"$values": {"day": {"$gte": "2026-01-01T00:00:00Z"}}}, "created_at"),  # `$trunc`
            (
                {"$or": [{"$values": {"c": "x"}}, {"$values": {"total": {"$gt": 10}}}]},
                "price",
            ),
            ({"$not": {"$values": {"total": {"$gt": 10}}}}, "price"),
            ({"$fields": {"n": {"$gt": "total"}}}, "price"),  # right-hand side of a compare
        ],
    )
    def test_having_needs_a_filterable_source(
        self, having: dict[str, object], source: str
    ) -> None:
        aggregates = {
            "$groups": {
                "c": "category",
                "p": "price",
                "day": {"$trunc": {"field": "created_at", "unit": "day"}},
            },
            "$computed": {
                "n": {"$count": None},
                "total": {"$sum": "price"},
                "p90": {"$percentile": {"field": "price", "p": 0.9}},
            },
            "$having": having,
        }

        with pytest.raises(CoreException) as ei:
            self._GUARD.check(aggregates=aggregates)

        assert ei.value.code == "field_not_filterable"
        assert repr(source) in str(ei.value)

    def test_having_on_a_filterable_source_or_a_count_passes(self) -> None:
        self._GUARD.check(
            aggregates={
                "$groups": {"c": "category"},
                "$computed": {"n": {"$count": None}, "top": {"$max": "category"}},
                "$having": {"$values": {"c": "x", "n": {"$gt": 1}, "top": "y"}},
            }
        )

    def test_list_form_groups_check_their_own_field(self) -> None:
        self._GUARD.check(
            aggregates={
                "$groups": ["category"],
                "$computed": {"n": {"$count": None}},
                "$having": {"$values": {"category": "x"}},
            },
            sorts={"category": "asc"},
        )

    @pytest.mark.parametrize(
        ("aggregates", "sorts"),
        [
            # A group alias named like a sortable field, over `price`.
            (
                {"$groups": {"category": "price"}, "$computed": {"n": {"$count": None}}},
                {"category": "asc"},
            ),
            # A computed alias named like one.
            (
                {"$groups": {"c": "category"}, "$computed": {"category": {"$sum": "price"}}},
                {"category": "desc"},
            ),
            # List-form groups: the alias is the field.
            ({"$groups": ["price"], "$computed": {"n": {"$count": None}}}, {"price": "asc"}),
            # A dotted key on an alias, by its root.
            ({"$groups": {"p": "price"}, "$computed": {"n": {"$count": None}}}, {"p.x": "asc"}),
            (
                {"$computed": {"p90": {"$percentile": {"field": "price", "p": 0.9}}}},
                {"p90": "desc"},
            ),
        ],
    )
    def test_sort_needs_a_sortable_source(
        self, aggregates: dict[str, object], sorts: dict[str, str]
    ) -> None:
        with pytest.raises(CoreException) as ei:
            self._GUARD.check(aggregates=aggregates, sorts=sorts)

        assert ei.value.code == "field_not_sortable"
        assert "'price'" in str(ei.value)

    def test_sort_on_a_trunc_group_needs_its_source_sortable(self) -> None:
        with pytest.raises(CoreException) as ei:
            self._GUARD.check(
                aggregates={
                    "$groups": {"day": {"$trunc": {"field": "created_at", "unit": "day"}}},
                    "$computed": {"n": {"$count": None}},
                },
                sorts={"day": "asc"},
            )

        assert ei.value.code == "field_not_sortable"
        assert "'created_at'" in str(ei.value)

    @pytest.mark.parametrize("sorts", ["category", ["category"]])
    def test_a_sort_that_is_not_a_mapping_is_refused(self, sorts: object) -> None:
        with pytest.raises(CoreException, match="must be an object") as ei:
            self._GUARD.check(sorts=sorts)  # type: ignore[arg-type]

        assert ei.value.kind is ExceptionKind.PRECONDITION

    def test_having_through_a_shadowing_alias_is_refused(self) -> None:
        with pytest.raises(CoreException) as ei:
            self._GUARD.check(
                aggregates={
                    "$groups": {"category": "price"},
                    "$computed": {"n": {"$count": None}},
                    "$having": {"$values": {"category": {"$gt": 5}}},
                }
            )

        assert ei.value.code == "field_not_filterable"

    def test_sort_on_a_sortable_source_or_a_count_passes(self) -> None:
        self._GUARD.check(
            aggregates={
                "$groups": {"c": "category"},
                "$computed": {"n": {"$count": None}},
            },
            sorts={"c": "asc", "c.x": "asc", "n": "desc"},
        )


class TestValidateRuntimeFilterFields:
    def test_known_fields_pass(self) -> None:
        from pydantic import BaseModel, computed_field

        from forze.application.contracts.querying import validate_runtime_filter_fields

        class _Doc(BaseModel):
            id: str
            name: str

            @computed_field  # type: ignore[prop-decorator]
            @property
            def display(self) -> str:
                return self.name

        # real field → ok
        validate_runtime_filter_fields(
            {"$values": {"name": "x"}}, model=_Doc
        )
        # no filter → no-op
        validate_runtime_filter_fields(None, model=_Doc)

        # computed field (not stored) → rejected
        with pytest.raises(CoreException, match="not on the read model"):
            validate_runtime_filter_fields(
                {"$values": {"display": "x"}}, model=_Doc
            )

        # unknown field → rejected as a caller precondition (HTTP 400), not a server error
        with pytest.raises(CoreException, match="not on the read model") as ei:
            validate_runtime_filter_fields(
                {"$values": {"nmae": "x"}}, model=_Doc
            )
        assert ei.value.kind is ExceptionKind.PRECONDITION
        assert ei.value.code == "field_not_on_read_model"

    def test_lenient_field_is_rejected(self) -> None:
        from pydantic import BaseModel

        from forze.application.contracts.querying import validate_runtime_filter_fields

        class _Doc(BaseModel):
            id: str
            name: str
            nickname: str = "anon"

        # ``nickname`` is a real model field but declared lenient (not stored), so a
        # filter on it must be rejected before reaching the backend.
        with pytest.raises(CoreException, match="not on the read model") as ei:
            validate_runtime_filter_fields(
                {"$values": {"nickname": "x"}},
                model=_Doc,
                lenient=frozenset({"nickname"}),
            )
        assert ei.value.code == "field_not_on_read_model"

        # without the lenient marker the same filter passes (field is on the model)
        validate_runtime_filter_fields({"$values": {"nickname": "x"}}, model=_Doc)

    def test_randomized_encrypted_field_is_rejected(self) -> None:
        """A predicate on a randomized-encrypted field can never match its non-deterministic
        ciphertext. Refusing it here — from the *declaration*, before any cipher is involved —
        is what makes a backend that stores plaintext (the mock) answer exactly as one that
        stores ciphertext, instead of green-lighting a query that fails in production."""

        from pydantic import BaseModel

        from forze.application.contracts.querying import validate_runtime_filter_fields

        class _Doc(BaseModel):
            id: str
            email: str
            secret: str

        with pytest.raises(CoreException, match="randomized-encrypted") as ei:
            validate_runtime_filter_fields(
                {"$values": {"secret": "x"}},
                model=_Doc,
                encrypted=frozenset({"secret"}),
            )
        # The same code an EncryptingModelCodec raises from its rewrite path: one rule, one code,
        # whichever layer catches it first.
        assert ei.value.code == "core.crypto.encrypted_field_not_filterable"
        assert ei.value.kind is ExceptionKind.PRECONDITION

        # A plaintext field on the same spec stays filterable.
        validate_runtime_filter_fields(
            {"$values": {"email": "x"}}, model=_Doc, encrypted=frozenset({"secret"})
        )

        # Without the declaration the field is just a field — the guard reads policy, not data.
        validate_runtime_filter_fields({"$values": {"secret": "x"}}, model=_Doc)

    def test_searchable_deterministic_field_stays_filterable(self) -> None:
        """Only ``encrypted`` (randomized) is refused. Deterministic ``searchable`` fields keep
        equality — the codec rewrites the literal to match the value at rest — so they must not
        be swept up by the guard."""

        from pydantic import BaseModel

        from forze.application.contracts.querying import validate_runtime_filter_fields

        class _Doc(BaseModel):
            id: str
            email: str

        # ``email`` is searchable, not randomized: it is never passed in ``encrypted``.
        validate_runtime_filter_fields(
            {"$values": {"email": "x"}}, model=_Doc, encrypted=frozenset()
        )


class _Status(StrEnum):
    ARCHIVED = "archived"


class TestAStringValueUnderFieldsNamesTheMigration:
    """A string under `$fields` is a field path, so a pre-0.7 string value parses as one.

    When it names no field the read model has, field validation is where it fails — and the only
    place the refusal can say it was a value that belongs under `$values`. A name on the left, or
    anywhere outside a `$fields` right-hand side, is an ordinary unknown field and says nothing
    about the migration.
    """

    @staticmethod
    def _refusal(filters: dict[str, object]) -> CoreException:
        from pydantic import BaseModel

        from forze.application.contracts.querying import validate_runtime_filter_fields

        class _Doc(BaseModel):
            id: str
            status: str
            created_at: str

        with pytest.raises(CoreException) as caught:
            validate_runtime_filter_fields(filters, model=_Doc)

        assert (caught.value.kind, caught.value.code) == (
            ExceptionKind.PRECONDITION,
            "field_not_on_read_model",
        )
        return caught.value

    @pytest.mark.parametrize(
        "filters",
        [
            {"$fields": {"status": "archived"}},
            {"$fields": {"status": {"$neq": "archived"}}},
            {"$values": {"id": "x"}, "$fields": {"status": _Status.ARCHIVED}},
            {"$fields": {"status": {"$eq": _Status.ARCHIVED}}},
        ],
        ids=["shortcut", "operator", "string-enum-shortcut", "string-enum-operator"],
    )
    def test_an_unknown_right_hand_side_names_values(self, filters: dict[str, object]) -> None:
        error = self._refusal(filters)

        assert "['archived']" in error.summary
        assert "`$values`" in error.summary

    @pytest.mark.parametrize(
        "filters",
        [{"$values": {"archived": "x"}}, {"$fields": {"archived": "created_at"}}],
        ids=["values-key", "fields-left-side"],
    )
    def test_any_other_unknown_field_says_nothing_about_it(
        self, filters: dict[str, object]
    ) -> None:
        assert "`$values`" not in self._refusal(filters).summary


    async def test_a_document_port_says_so(self) -> None:
        """Wired, not only callable: an adapter's filter validation carries the pointer."""

        from forze_mock import MockDepsModule
        from tests.support.execution_context import context_from_modules
        from tests.support.owned_reads_conformance import owned_spec

        query = context_from_modules(MockDepsModule()).doc.query(owned_spec("notes"))

        with pytest.raises(CoreException) as caught:
            await query.find({"$fields": {"title": "archived"}})

        assert caught.value.code == "field_not_on_read_model"
        assert "`$values`" in caught.value.summary
