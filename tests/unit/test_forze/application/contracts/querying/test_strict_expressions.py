"""A query expression refuses a key it does not define, wherever it is validated.

Validated as a pydantic type (a request DTO, a generated tool schema) the expression types
dropped an unknown key: ``{"$values": {...}, "$bogus": 1}`` lost ``$bogus`` and widened the
page, and ``{"$bogus": {...}}`` became ``{}``. Each now refuses it, naming the key.
"""

from __future__ import annotations

import json
import random
import re
from typing import Any

import pytest
from pydantic import TypeAdapter, ValidationError

from forze.application.contracts.analytics import AnalyticsRunOptions
from forze.application.contracts.dynamic_read import DynamicReadOptions
from forze.application.contracts.inference import InferenceRunOptions
from forze.application.contracts.querying import (
    AggregatesExpression,
    AggregatesExpressionParser,
    QueryFilterExpression,
    QueryFilterExpressionParser,
    QueryFilterLimits,
    QuerySortExpression,
    QuerySortKeySpec,
    refuse_unknown_keys,
)
from forze.application.contracts.search import (
    MultiSourceSearchOptions,
    SearchOptions,
    SearchResultSnapshotOptions,
)
from forze.base.exceptions import CoreException
from forze_kits.aggregates.document.dto import AggregatedListRequestDTO, ListRequestDTO
from tests.support.query_dsl_corpus import CASES, REFUSALS, parse_corpus_filter

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


@pytest.mark.parametrize("case", CASES, ids=[c.name for c in CASES])
def test_a_filter_the_parser_accepts_passes_the_request_type(case: Any) -> None:
    """The request type is no narrower than the parser behind the port: a filter a backend
    would serve is not refused at the door. Python-only operands (a set) go as JSON does."""

    filters = json.loads(json.dumps(case.filters, default=list))
    parse_corpus_filter(filters)

    assert _FILTER.validate_python(filters) == filters


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
        (TypeAdapter[Any](AnalyticsRunOptions), {"max_row": 5}, "max_row"),
        (TypeAdapter[Any](DynamicReadOptions), {"row_capp": 5}, "row_capp"),
        (TypeAdapter[Any](InferenceRunOptions), {"timeot": 5}, "timeot"),
    ],
    ids=[
        "sort",
        "aggregate",
        "metric",
        "trunc",
        "search",
        "highlight",
        "multi",
        "snapshot",
        "analytics",
        "dynamic_read",
        "inference",
    ],
)
def test_a_sibling_expression_type_refuses_a_key_it_does_not_define(
    adapter: TypeAdapter[Any], value: dict[str, Any], key: str
) -> None:
    with pytest.raises(ValidationError, match=f"\\{key}" if key.startswith("$") else key):
        adapter.validate_python(value)


def _quantifiers_nested(depth: int) -> dict[str, Any]:
    inner: Any = "x"

    for _ in range(depth):
        inner = {"$any": inner}

    return {"$values": {"matrix": inner}}


def test_quantifier_nesting_is_bounded_by_the_depth_limit() -> None:
    """Nested quantifiers recurse like combinators, so ``max_depth`` bounds them: past it the
    filter is refused, never a ``RecursionError``, however many clauses a spec allows."""

    wide = QueryFilterExpressionParser(limits=QueryFilterLimits(max_clauses=1_000_000))
    wide.parse_filter(_quantifiers_nested(32))

    for depth in (33, 5_000):
        with pytest.raises(CoreException, match="maximum depth of 32"):
            wide.parse_filter(_quantifiers_nested(depth))

    # Only nesting counts: quantifiers side by side are each one level deep.
    side_by_side = {"$values": {f"f{i}": {"$any": "x"} for i in range(40)}}
    QueryFilterExpressionParser.parse(side_by_side)

    # Nor do the combinators before it: a shallow quantifier after 32 of them still parses.
    after_combinators = {
        "$and": [{"$not": {"$values": {"a": 1}}}] * 31 + [{"$values": {"tags": {"$any": "x"}}}]
    }
    QueryFilterExpressionParser.parse(after_combinators)


def _negations_nested(depth: int) -> dict[str, Any]:
    expr: dict[str, Any] = {"$values": {"a": 1}}

    for _ in range(depth):
        expr = {"$not": expr}

    return expr


def test_combinator_depth_counts_nesting_not_siblings() -> None:
    """``max_depth`` bounds how deep ``$and`` / ``$or`` / ``$not`` nest; how many a filter has
    side by side is ``max_clauses``' to bound."""

    QueryFilterExpressionParser.parse(_negations_nested(32))

    with pytest.raises(CoreException, match="maximum depth of 32"):
        QueryFilterExpressionParser.parse(_negations_nested(33))

    # Two levels deep, forty wide, whichever combinator the siblings are.
    leaf = {"$values": {"a": 1}}

    for sibling in ({"$not": leaf}, {"$and": [leaf]}, {"$or": [leaf]}):
        QueryFilterExpressionParser.parse({"$and": [sibling] * 40})

        # A finished sibling gives back exactly its own level: the next one is still 33 deep.
        with pytest.raises(CoreException, match="maximum depth of 32"):
            QueryFilterExpressionParser.parse({"$and": [sibling, _negations_nested(32)]})
    QueryFilterExpressionParser.parse({"$or": [_negations_nested(31), _negations_nested(31)]})


def test_a_port_refuses_only_the_keys_a_type_does_not_define() -> None:
    """Required keys count as defined, as optional ones do."""

    refuse_unknown_keys({"dir": "asc", "nulls": "last"}, QuerySortKeySpec)
    refuse_unknown_keys(None, QuerySortKeySpec)

    with pytest.raises(CoreException, match="Unknown key in QuerySortKeySpec: nul$"):
        refuse_unknown_keys({"dir": "asc", "nul": "last"}, QuerySortKeySpec)


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
        (
            {"$values": {"matrix": {"$any": {"$any": "x", "$bogus": 1}}}},
            "cannot be combined with other operators ($any, $bogus)",
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


def test_an_ordering_bound_message_names_what_it_accepts() -> None:
    """A UUID bound orders; a bare ``time`` does not, and the refusal says so rather than
    claiming to accept a time."""

    from datetime import time
    from uuid import uuid4

    from forze.application.contracts.querying.internal.parse import (
        QueryFilterExpressionParser,
    )
    from forze.base.exceptions import CoreException

    QueryFilterExpressionParser.parse({"$values": {"x": {"$gt": uuid4()}}})

    with pytest.raises(CoreException) as caught:
        QueryFilterExpressionParser.parse({"$values": {"x": {"$gt": time(1, 2)}}})

    assert "a number, string, date, datetime or UUID, got time" in str(caught.value)


@pytest.mark.parametrize("expr", [[{"a": 1}], ["x"], 5, "abc", None])
def test_an_aggregates_expression_that_is_not_an_object_is_refused(expr: Any) -> None:
    from forze.application.contracts.querying.internal.aggregate import (
        AggregatesExpressionParser,
    )
    from forze.base.exceptions import CoreException

    with pytest.raises(CoreException, match="An aggregates expression must be an object"):
        AggregatesExpressionParser.parse(expr)
