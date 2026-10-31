"""The shared checks every backend runs before a search aggregate."""

from __future__ import annotations

from decimal import Decimal
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.crypto import FieldEncryption
from forze.application.contracts.querying import (
    UNSUPPORTED_QUERY_FEATURE_CODE,
    QueryExpr,
    QueryFilterExpressionParser,
)
from forze.application.contracts.search import (
    DEFAULT_SEARCH_CAPABILITIES,
    FULL_SEARCH_CAPABILITIES,
    HubSearchSpec,
    SearchSpec,
    validate_aggregates_supported,
    validate_search_aggregates,
)
from forze.application.contracts.search.aggregates import (
    _paths_into,  # pyright: ignore[reportPrivateUsage]
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
        {"merge_candidates": 2},
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


# ....................... #


class _HubRow(BaseModel):
    id: UUID
    region: str
    secret: str


_HUB = HubSearchSpec(
    name="hub",
    model_type=_HubRow,
    members=(SearchSpec(name="leg", model_type=_Row, fields=["title"]),),
    encryption=FieldEncryption(encrypted=frozenset({"secret"})),
    extra_aggregatable_fields=frozenset({"quantity", "price_upd_quantity"}),
)


def test_a_hub_aggregates_its_stored_fields_and_its_extra_columns() -> None:
    assert _HUB.aggregatable_fields == {"id", "region", "quantity", "price_upd_quantity"}

    validate_search_aggregates(
        _HUB,
        {"$groups": {"region": "region"}, "$computed": {"q": {"$sum": "price_upd_quantity"}}},
        None,
    )


@pytest.mark.parametrize("field", ["region", "secret"])
def test_an_extra_aggregatable_field_on_the_hub_model_is_refused(field: str) -> None:
    """A stored field is aggregatable already; a sealed one must not become so."""

    with pytest.raises(CoreException) as refused:
        HubSearchSpec(
            name="hub",
            model_type=_HubRow,
            members=(SearchSpec(name="leg", model_type=_Row, fields=["title"]),),
            extra_aggregatable_fields=frozenset({field}),
        )

    assert field in str(refused.value)


def test_a_hub_metric_filter_may_name_an_extra_column() -> None:
    """An extra column is aggregatable, so a metric filter may name it as a measure does."""

    validate_search_aggregates(
        _HUB,
        {
            "$computed": {
                "total": {"$sum": "quantity"},
                "missing": {"$count": {"filter": {"$values": {"quantity": {"$null": True}}}}},
                "large": {
                    "$sum": {
                        "field": "price_upd_quantity",
                        "filter": {"$values": {"region": "north", "quantity": {"$gt": 2}}},
                    }
                },
            }
        },
        None,
    )


@pytest.mark.parametrize(
    ("field", "code"),
    [("discount", "field_not_on_read_model"), ("secret", "field_not_aggregatable")],
)
def test_a_hub_metric_filter_on_an_undeclared_or_sealed_field_is_refused(
    field: str, code: str
) -> None:
    aggregates = {"$computed": {"n": {"$count": {"filter": {"$values": {field: "x"}}}}}}

    with pytest.raises(CoreException) as refused:
        validate_search_aggregates(_HUB, aggregates, None)

    assert refused.value.code == code
    assert field in str(refused.value)


@pytest.mark.parametrize(
    "predicate",
    [
        {"$values": {"quantity.unit": "kg"}},
        {"$or": [{"$values": {"region": "north"}}, {"$values": {"quantity.unit": "kg"}}]},
        {"$not": {"$fields": {"quantity.value": {"$gt": "price_upd_quantity"}}}},
        {"$values": {"quantity": {"$any": {"$gt": 1}}}},
    ],
)
def test_a_hub_metric_filter_reaching_into_an_extra_column_is_refused(
    predicate: dict[str, object],
) -> None:
    """No model describes what an extra column holds, so it is filtered by its own value."""

    aggregates = {"$computed": {"n": {"$count": {"filter": predicate}}}}

    with pytest.raises(CoreException) as refused:
        validate_search_aggregates(_HUB, aggregates, None)

    assert refused.value.code == UNSUPPORTED_QUERY_FEATURE_CODE
    assert "quantity" in str(refused.value)


@pytest.mark.parametrize(
    "aggregates",
    [
        {"$groups": {"unit": "quantity.unit"}, "$computed": {"n": {"$count": None}}},
        {
            "$groups": {"day": {"$trunc": {"field": "quantity.at", "unit": "day"}}},
            "$computed": {"n": {"$count": None}},
        },
        {"$computed": {"q": {"$sum": "quantity.value"}}},
        {"$computed": {"q": {"$max": {"field": "quantity.value"}}}},
    ],
)
def test_a_hub_group_or_measure_reaching_into_an_extra_column_is_refused(
    aggregates: dict[str, object],
) -> None:
    """As a metric filter is: an extra column is grouped by or measured by its own value."""

    with pytest.raises(CoreException) as refused:
        validate_search_aggregates(_HUB, aggregates, None)

    assert refused.value.code == UNSUPPORTED_QUERY_FEATURE_CODE
    assert "quantity" in str(refused.value)


def test_a_metric_filter_on_a_stored_field_passes() -> None:
    """A plain search spec has no extra columns to reach into."""

    validate_search_aggregates(
        _SPEC,
        {"$computed": {"n": {"$count": {"filter": {"$values": {"category": "books"}}}}}},
        None,
        parser=QueryFilterExpressionParser(),
    )


def test_a_node_with_no_field_names_no_path() -> None:
    class _Leaf(QueryExpr):
        pass

    assert _paths_into(_Leaf(), frozenset({"quantity"})) == frozenset()
