"""A query expression refuses a key it does not define, wherever it is validated.

Validated as a pydantic type (a request DTO, a generated tool schema) the expression types
dropped an unknown key: ``{"$values": {...}, "$bogus": 1}`` lost ``$bogus`` and widened the
page, and ``{"$bogus": {...}}`` became ``{}``. Each now refuses it, naming the key.
"""

from __future__ import annotations

import random
import re
from typing import Any

import pytest
from pydantic import TypeAdapter, ValidationError

from forze.application.contracts.querying import (
    AggregatesExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.contracts.querying import (
    AggregatesExpressionParser,
    QueryFilterExpressionParser,
)
from forze.application.contracts.search import (
    MultiSourceSearchOptions,
    SearchOptions,
    SearchResultSnapshotOptions,
)
from forze.base.exceptions import CoreException
from forze_kits.aggregates.document.dto import AggregatedListRequestDTO, ListRequestDTO
from tests.support.query_dsl_corpus import REFUSALS

# ----------------------- #

_FILTER = TypeAdapter[Any](QueryFilterExpression)


@pytest.mark.parametrize(("name", "filters", "key"), REFUSALS, ids=[r[0] for r in REFUSALS])
def test_a_filter_type_refuses_a_key_it_does_not_define(
    name: str, filters: dict[str, Any], key: str
) -> None:
    with pytest.raises(ValidationError, match=f"\\{key}"):
        _FILTER.validate_python(filters)

    with pytest.raises(ValidationError, match=f"\\{key}"):
        ListRequestDTO.model_validate({"filters": filters})


def test_a_valid_filter_is_kept_whole() -> None:
    filters = {
        "$or": [
            {"$values": {"name": {"$in": ["a", "b"]}, "tags": {"$any": {"$eq": "x"}}}},
            {"$not": {"$fields": {"age": {"$gt": "score"}}}},
        ]
    }

    assert _FILTER.validate_python(filters) == filters
    assert ListRequestDTO.model_validate({"filters": filters}).filters == filters


@pytest.mark.parametrize(
    ("adapter", "value", "key"),
    [
        (TypeAdapter[Any](QuerySortExpression), {"name": {"dir": "asc", "nul": "last"}}, "nul"),
        (
            TypeAdapter[Any](AggregatesExpression),
            {"$computed": {"n": {"$count": None}}, "$havng": {"$values": {"n": 1}}},
            "$havng",
        ),
        (
            TypeAdapter[Any](AggregatesExpression),
            {"$computed": {"n": {"$sum": {"field": "qty", "filters": {}}}}},
            "filters",
        ),
        (
            TypeAdapter[Any](AggregatesExpression),
            {
                "$groups": {"d": {"$trunc": {"field": "at", "unit": "day", "tz": "UTC"}}},
                "$computed": {"n": {"$count": None}},
            },
            "tz",
        ),
        (TypeAdapter[Any](SearchOptions), {"fuzy": True}, "fuzy"),
        (TypeAdapter[Any](SearchOptions), {"highlight": {"pre": "<b>"}}, "pre"),
        (TypeAdapter[Any](MultiSourceSearchOptions), {"membrs": ["a"]}, "membrs"),
        (TypeAdapter[Any](SearchResultSnapshotOptions), {"ttl": 5}, "ttl"),
    ],
    ids=["sort", "aggregate", "metric", "trunc", "search", "highlight", "multi", "snapshot"],
)
def test_a_sibling_expression_type_refuses_a_key_it_does_not_define(
    adapter: TypeAdapter[Any], value: dict[str, Any], key: str
) -> None:
    with pytest.raises(ValidationError, match=f"\\{key}" if key.startswith("$") else key):
        adapter.validate_python(value)


def test_the_schema_says_no_other_keys() -> None:
    """A generated tool schema tells its client the same thing validation enforces."""

    schema = _FILTER.json_schema()

    assert schema["$defs"]["QueryValueOpConjunction"]["additionalProperties"] is False


@pytest.mark.parametrize(
    ("filters", "message"),
    [
        ([{"$values": {"a": 1}}], "A filter expression must be an object"),
        ({"$values": [1, 2]}, "$values must be an object"),
        ({"$values": "abc"}, "$values must be an object"),
        ({"$fields": ["a"]}, "$fields must be an object"),
        ({"$values": {"tags": {"$any": {"$values": [1]}}}}, "$values must be an object"),
        ({"$values": {"name": {"$like": 5}}}, "$like operand must be a pattern string"),
        (
            {"$values": {"items": {"$any": {"$values": {"tags": {"$any": "x", "$eq": 1}}}}}},
            "an element quantifier cannot be combined",
        ),
    ],
)
def test_a_malformed_filter_is_refused_naming_its_key(filters: Any, message: str) -> None:
    """Refused as a precondition (a 400 at the port, a 422 at a request), never a crash."""

    with pytest.raises(CoreException, match=re.escape(message)):
        QueryFilterExpressionParser.parse(filters)

    with pytest.raises(ValidationError, match=re.escape(message)):
        ListRequestDTO.model_validate({"filters": filters})


_KEYS = [
    "$values", "$fields", "$and", "$or", "$not", "$any", "$all", "$none", "$eq", "$gt",
    "$in", "$like", "$regex", "$null", "$superset", "$descendant_of", "a", "b.c",
]  # fmt: skip
_LEAVES = [1, "x", None, True, 1.5, [], [1], ["a", "b"], {}, "a%", [None], [[1]], [{}]]


def _random_value(rng: random.Random, depth: int = 0) -> Any:
    roll = rng.random()

    if depth > 4 or roll < 0.3:
        return rng.choice(_LEAVES)

    if roll < 0.45:
        return [_random_value(rng, depth + 1) for _ in range(rng.randint(0, 2))]

    return {rng.choice(_KEYS): _random_value(rng, depth + 1) for _ in range(rng.randint(1, 2))}


def test_any_filter_is_parsed_or_refused_never_crashes() -> None:
    """Over arbitrary nested input the parser raises only its own refusal, and a request only
    a validation error: a crash there is a 500 for what the caller got wrong."""

    rng = random.Random(7)

    for _ in range(600):
        filters = {rng.choice(_KEYS[:5]): _random_value(rng)}
        aggregates = {"$computed": {"n": {"$count": {"filter": filters}}}, "$having": filters}

        for parse, value in (
            (QueryFilterExpressionParser.parse, filters),
            (AggregatesExpressionParser.parse, aggregates),
        ):
            try:
                parse(value)
            except CoreException:
                pass

        for model, body in (
            (ListRequestDTO, {"filters": filters}),
            (AggregatedListRequestDTO, {"aggregates": aggregates}),
        ):
            try:
                model.model_validate(body)
            except ValidationError:
                pass


def test_a_parser_crash_leaves_the_type_refusal_standing() -> None:
    """Should the parser ever fail on input the type already refused, the request still gets
    the validation error, never the crash."""

    from typing import Annotated

    from forze_kits.dto.querying import _named_by  # pyright: ignore[reportPrivateUsage]

    def crashes(_value: Any) -> None:
        raise TypeError("boom")

    adapter = TypeAdapter[Any](Annotated[QueryFilterExpression, _named_by(crashes)])

    with pytest.raises(ValidationError, match="extra_forbidden|Extra inputs"):
        adapter.validate_python({"$values": {"a": 1}, "$bogus": 1})


def test_stored_file_and_search_requests_refuse_by_name() -> None:
    from forze_kits.aggregates.search.dto import SearchRequestDTO
    from forze_kits.aggregates.stored_file.dto import ListStoredFilesRequestDTO

    with pytest.raises(ValidationError, match=re.escape("Unknown filter key $bogus")):
        ListStoredFilesRequestDTO.model_validate({"filters": {"$bogus": {"a": 1}}})

    # A single-index search defines no member keys; a hub or federated one does.
    with pytest.raises(ValidationError, match="members"):
        SearchRequestDTO.model_validate({"query": "q", "options": {"members": ["a"]}})

    SearchRequestDTO[MultiSourceSearchOptions].model_validate(
        {"query": "q", "options": {"members": ["a"]}}
    )
