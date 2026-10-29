"""Query discovery: project a read model's filter surface for clients.

The inverse of validation — instead of rejecting a bad operator/field pairing, enumerate
the allowed ones (per-field operators, sortable/aggregatable fields) so a human or LLM
can discover the query contract up front. Backend-agnostic (type-derived upper bound).
"""

from __future__ import annotations

import re
from datetime import datetime
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.querying import (
    ALL_VALUE_OPS,
    QUANTIFIER_OPS,
    QueryFieldPolicy,
    QueryFilterLimits,
    build_query_discovery,
    classify_field_type,
    describe_query_discovery,
    field_value_operators,
    is_quantifiable_field,
    narrow_query_discovery,
)

pytestmark = pytest.mark.unit


class _Item(BaseModel):
    sku: str


class _Doc(BaseModel):
    name: str
    age: int
    score: float
    active: bool
    created: datetime
    ref: UUID
    tags: list[str]
    meta: dict[str, str]
    item: _Item


# ....................... #


class TestClassification:
    @pytest.mark.parametrize(
        ("field", "expected"),
        [
            ("name", "string"),
            ("age", "number"),
            ("score", "number"),
            ("active", "bool"),
            ("created", "temporal"),
            ("ref", "scalar"),
            ("tags", "collection"),
            ("meta", "mapping"),
            ("item", "object"),
        ],
    )
    def test_classify(self, field: str, expected: str) -> None:
        ann = _Doc.model_fields[field].annotation
        assert classify_field_type(ann) == expected

    def test_unresolvable_is_unknown_and_unconstrained(self) -> None:
        from typing import Any

        assert classify_field_type(Any) == "unknown"
        # An unknown type places no constraint — the full value-op surface.
        assert field_value_operators(Any) == ALL_VALUE_OPS

    def test_operators_match_type(self) -> None:
        ops = field_value_operators(_Doc.model_fields["age"].annotation)
        assert {"$gt", "$gte", "$lt", "$lte", "$in", "$nin", "$eq", "$neq", "$null"} == ops
        # numbers don't take text ops or set ops
        assert "$like" not in ops
        assert "$superset" not in ops

    def test_string_takes_text_not_ordering(self) -> None:
        ops = field_value_operators(_Doc.model_fields["name"].annotation)
        assert {"$like", "$ilike", "$regex"} <= ops
        assert not ({"$gt", "$lt"} & ops)

    def test_bool_takes_neither_ordering_nor_text(self) -> None:
        ops = field_value_operators(_Doc.model_fields["active"].annotation)
        assert ops == {"$eq", "$neq", "$null", "$in", "$nin"}

    def test_collection_quantifiable(self) -> None:
        assert is_quantifiable_field(_Doc.model_fields["tags"].annotation) is True
        assert is_quantifiable_field(_Doc.model_fields["name"].annotation) is False


# ....................... #


class TestBuildDiscovery:
    def _discovery(self, **kw):
        names = set(_Doc.model_fields)
        return build_query_discovery(
            _Doc,
            filterable=kw.get("filterable", names),
            sortable=kw.get("sortable", names),
            aggregatable=kw.get("aggregatable", names),
        )

    def test_filterable_sorted_with_per_field_ops(self) -> None:
        qd = self._discovery()
        fields = {f.field: f for f in qd.filterable}

        assert [f.field for f in qd.filterable] == sorted(fields)  # sorted by name
        assert fields["name"].type == "string"
        assert "$like" in fields["name"].operators
        assert fields["age"].type == "number"
        assert fields["tags"].quantifiable is True
        assert fields["name"].quantifiable is False

    def test_operators_are_sorted_tuples(self) -> None:
        qd = self._discovery()
        for f in qd.filterable:
            assert isinstance(f.operators, tuple)
            assert list(f.operators) == sorted(f.operators)

    def test_allow_sets_respected(self) -> None:
        qd = self._discovery(
            filterable={"name", "age"},
            sortable={"age"},
            aggregatable=set(),
        )

        assert {f.field for f in qd.filterable} == {"name", "age"}
        assert qd.sortable == ("age",)
        assert qd.aggregatable == ()

    def test_field_absent_from_model_is_unconstrained(self) -> None:
        # A name in the allow-set but not on the model resolves to unknown → full ops.
        qd = build_query_discovery(
            _Doc, filterable={"ghost"}, sortable=set(), aggregatable=set()
        )
        ghost = qd.filterable[0]

        assert ghost.field == "ghost"
        assert ghost.type == "unknown"
        assert set(ghost.operators) == ALL_VALUE_OPS

    def test_quantifier_constant(self) -> None:
        assert QUANTIFIER_OPS == ("$any", "$all", "$none")

    def test_discovery_is_hashable(self) -> None:
        # Carried on a frozen OperationDescriptor → must hash.
        assert hash(self._discovery()) is not None


# ....................... #


class TestDescribeDiscovery:
    """The prose states each operator list once, grouped by the fields that share it."""

    def _describe(self, model: type[BaseModel], **kw) -> str:
        names = set(model.model_fields)
        return describe_query_discovery(
            build_query_discovery(
                model,
                filterable=kw.get("filterable", names),
                sortable=kw.get("sortable", set()),
                aggregatable=kw.get("aggregatable", set()),
            )
        )

    def test_groups_fields_by_operator_set(self) -> None:
        # ``meta`` and ``item`` take only the universal operators, so ``$in`` / ``$nin``
        # move into each group that has them; number, temporal and scalar share one set,
        # stated once, while each field stays listed under its own type.
        assert self._describe(_Doc) == (
            "Filterable fields — $eq, $neq, $null on every field; "
            "bool (also $in, $nin): active; "
            "number (also $gt, $gte, $in, $lt, $lte, $nin): age, score; "
            "temporal (as number): created; scalar (as number): ref; "
            "object: item; mapping: meta; "
            "string (also $ilike, $in, $like, $nin, $regex): name; "
            "collection (also $disjoint, $empty, $in, $nin, $overlaps, $subset, $superset; "
            "element quantifiers $any, $all, $none): tags."
        )

    def test_every_field_keeps_its_operators(self) -> None:
        # Grouping changes the wording, never what a field is said to accept: read the
        # sentence back and compare it with the per-field discovery, an unknown field too.
        qd = build_query_discovery(
            _Doc, filterable={*_Doc.model_fields, "ghost"}, sortable=set(), aggregatable=set()
        )
        body = describe_query_discovery(qd).removeprefix("Filterable fields — ").rstrip(".")
        first, *clauses = re.split(r"; (?![^()]*\))", body)
        common = set(first.removesuffix(" on every field").split(", "))
        read_back: dict[str, tuple[set[str], bool]] = {}
        by_type: dict[str, tuple[set[str], bool]] = {}

        for clause in clauses:
            head, names = clause.rsplit(": ", 1)
            kind, _, note = head.partition(" (")
            notes = note.rstrip(")").split("; ")
            also = next((n.removeprefix("also ") for n in notes if n.startswith("also ")), "")
            ops = common | (set(also.split(", ")) if also else set())
            quantifiable = f"element quantifiers {', '.join(QUANTIFIER_OPS)}" in notes
            # ``(as number)``: the same operators as the type it names.
            same = next((n.removeprefix("as ") for n in notes if n.startswith("as ")), None)
            by_type[kind] = by_type[same] if same else (ops, quantifiable)

            for name in names.split(", "):
                read_back[name] = by_type[kind]

        assert read_back == {f.field: (set(f.operators), f.quantifiable) for f in qd.filterable}

    def test_sort_and_aggregate_follow(self) -> None:
        assert self._describe(_Item, sortable={"sku"}, aggregatable={"sku"}) == (
            "Filterable fields — $eq, $ilike, $in, $like, $neq, $nin, $null, $regex on every "
            "field; string: sku. Sortable by: sku. Aggregatable by: sku."
        )

    def test_empty_surface_is_empty(self) -> None:
        assert self._describe(_Doc, filterable=set()) == ""


# ....................... #


class TestNarrowDiscovery:
    def test_keeps_only_the_allowed_fields_and_never_adds_one(self) -> None:
        qd = build_query_discovery(
            _Doc, filterable={"name", "age"}, sortable={"name"}, aggregatable={"age"}
        )
        # `score` is on the model but not in the discovery: an intersection cannot add it.
        narrowed = narrow_query_discovery(
            qd, QueryFieldPolicy(filterable={"age", "score"}, sortable=set())
        )

        assert narrowed.filterable == tuple(f for f in qd.filterable if f.field == "age")
        assert narrowed.sortable == ()
        assert narrowed.aggregatable == ("age",)

    def test_keeps_the_filter_limits(self) -> None:
        limits = QueryFilterLimits(max_pattern_length=512)
        qd = build_query_discovery(
            _Doc, filterable={"name"}, sortable=set(), aggregatable=set(), filter_limits=limits
        )

        narrowed = narrow_query_discovery(qd, QueryFieldPolicy(filterable=set()))

        assert narrowed.filter_limits == limits
