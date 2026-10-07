"""The shared checks every backend runs before a search aggregate."""

from __future__ import annotations

from decimal import Decimal
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.crypto import FieldEncryption
from forze.application.contracts.querying import UNSUPPORTED_QUERY_FEATURE_CODE
from forze.application.contracts.search import (
    DEFAULT_SEARCH_CAPABILITIES,
    FULL_SEARCH_CAPABILITIES,
    SearchSpec,
    validate_aggregates_supported,
    validate_search_aggregates,
)
from forze.base.exceptions import CoreException

# ----------------------- #


class _Row(BaseModel):
    id: UUID
    title: str
    category: str
    amount: Decimal
    secret: str
    token: str
    note: str = ""


_SPEC = SearchSpec(
    name="rows",
    model_type=_Row,
    fields=["title"],
    lenient_read_fields=frozenset({"note"}),
    encryption=FieldEncryption(encrypted=frozenset({"secret"}), searchable=frozenset({"token"})),
)

_BY_CATEGORY = {
    "$groups": {"category": "category"},
    "$computed": {"total": {"$sum": "amount"}},
}


def test_stored_plaintext_fields_are_aggregatable() -> None:
    assert _SPEC.aggregatable_fields == {"id", "title", "category", "amount"}

    validate_search_aggregates(_SPEC, _BY_CATEGORY, None)


@pytest.mark.parametrize(
    "aggregates",
    [
        {"$groups": {"s": "secret"}, "$computed": {"n": {"$count": None}}},
        {"$computed": {"s": {"$max": "secret"}}},
        {"$groups": {"n": "note"}, "$computed": {"c": {"$count": None}}},
    ],
)
def test_a_sealed_or_unstored_field_is_refused(aggregates: dict[str, object]) -> None:
    with pytest.raises(CoreException) as refused:
        validate_search_aggregates(_SPEC, aggregates, None)

    assert refused.value.code == "field_not_aggregatable"


@pytest.mark.parametrize("field", ["secret", "token"])
def test_a_metric_filter_on_an_encrypted_field_is_refused(field: str) -> None:
    """Randomized or deterministic, the stored value is ciphertext a measure cannot read."""

    aggregates = {
        "$groups": {"category": "category"},
        "$computed": {"n": {"$count": {"filter": {"$values": {field: "x"}}}}},
    }

    with pytest.raises(CoreException) as refused:
        validate_search_aggregates(_SPEC, aggregates, None)

    assert refused.value.code == "field_not_aggregatable"
    assert field in str(refused.value)


@pytest.mark.parametrize(
    "options",
    [
        {"facets": ["category"]},
        {"highlight": {"fields": ["title"]}},
        {"highlight": {}},
        {"highlight": True},
        {"max_candidates": 2},
    ],
)
def test_hit_options_and_a_candidate_cap_are_refused_not_ignored(
    options: dict[str, object],
) -> None:
    with pytest.raises(CoreException) as refused:
        validate_search_aggregates(_SPEC, _BY_CATEGORY, options)  # type: ignore[arg-type]

    assert refused.value.code == UNSUPPORTED_QUERY_FEATURE_CODE


def test_the_gate_follows_the_declaration() -> None:
    validate_aggregates_supported(FULL_SEARCH_CAPABILITIES, backend="full")

    with pytest.raises(CoreException) as refused:
        validate_aggregates_supported(DEFAULT_SEARCH_CAPABILITIES, backend="plain")

    assert refused.value.code == UNSUPPORTED_QUERY_FEATURE_CODE


@pytest.mark.parametrize("options", [{"facets": []}, {"highlight": False}, {"highlight": None}])
def test_options_that_request_nothing_pass(options: dict[str, object]) -> None:
    validate_search_aggregates(_SPEC, _BY_CATEGORY, options)  # type: ignore[arg-type]
