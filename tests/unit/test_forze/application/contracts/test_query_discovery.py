"""Query discovery: project a read model's filter surface for clients.

The inverse of validation — instead of rejecting a bad operator/field pairing, enumerate
the allowed ones (per-field operators, sortable/aggregatable fields) so a human or LLM
can discover the query contract up front. Backend-agnostic (type-derived upper bound).
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.querying import (
    ALL_VALUE_OPS,
    QUANTIFIER_OPS,
    build_query_discovery,
    classify_field_type,
    field_value_operators,
    is_quantifiable_field,
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
    def _discovery(self, **kw):  # noqa: ANN003
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
