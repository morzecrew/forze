"""What a refused query expression tells its caller, and what it never repeats.

A refusal names the key or operator at fault and the shape it expected (a key the caller sent
may be named, as it is the thing at fault). It never quotes a value the caller sent: the message reaches the HTTP response (a request DTO forwards the
parser's message into its 422), and an operand can be anything, a secret pasted into the wrong
field included.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from typing import Any

import pytest
from fastapi.testclient import TestClient

from forze.application.contracts.querying import (
    AggregatesExpressionParser,
    QueryFilterExpressionParser,
)
from forze.application.contracts.querying.sort_resolution import parse_sort_value
from forze.base.exceptions import CoreException
from tests.unit.test_forze_fastapi.test_document_routes import (
    _build_app,  # pyright: ignore[reportPrivateUsage]
)

# ----------------------- #

SECRET = "sk_live_SENTINEL_9f3a"


def _filters() -> list[Any]:
    """A filter for every refusal path whose input carries :data:`SECRET` as an operand."""

    s = SECRET
    return [
        {"$values": {"name": {"$eq": {s: 1}}}},
        {"$values": {"age": {"$gt": {s: 1}}}},
        {"$values": {"name": {"$in": {s: 1}}}},
        {"$values": {"name": {"$null": s}}},
        {"$values": {"tags": {"$superset": s}}},
        {"$values": {"path": {"$descendant_of": {s: 1}}}},
        {"$values": {"path": {"$descendant_of": [s, 1]}}},
        {"$values": {"name": {"$like": [s, 1]}}},
        {"$values": {"name": {"$regex": f"(a+)+{s}"}}},
        {"$values": {"name": {"$regex": f"a{{1,99999}}{s}"}}},
        {"$values": {"name": {"$regex": "|".join([s] * 40)}}},
        {"$values": {"name": object()}},
        {"$values": {"tags": {"$any": {"$eq": {s: 1}}}}},
        {"$values": {"tags": {"$any": {"$gt": {s: 1}}}}},
        {"$values": {"tags": {"$any": {"$in": s}}}},
        {"$values": {"tags": {"$any": object()}}},
        {"$values": {"items": {"$any": {"$values": {"sku": object()}}}}},
        {"$fields": {"age": {"$gt": {s: 1}}}},
        {"$fields": {"age": 1.5}},
        {"$fields": {"age": {"$eq": {s: 1}}}},
    ]


def _aggregates() -> list[Any]:
    s = SECRET
    return [
        {"$computed": s},
        {"$computed": {"n": {"$count": None}}, "$groups": s},
        {"$computed": {"n": {"$count": None}}, "$groups": {"g": 7}},
        {"$computed": {"n": {"$count": None}}, "$groups": {"g": {"$trunc": s}}},
        {
            "$computed": {"n": {"$count": None}},
            "$groups": {"g": {"$trunc": {"field": "at", "unit": "day", "timezone": 7}}},
        },
        {
            "$computed": {"n": {"$count": None}},
            "$groups": {"g": {"$trunc": {"field": "at", "unit": "day", "timezone": s}}},
        },
        {
            "$computed": {"n": {"$count": None}},
            "$groups": {"g": {"$trunc": {"field": "at", "unit": "day", "timezone": "+99"}}},
        },
        {"$computed": {"n": {"$sum": {"field": "age", "p": s, "filter": {"$values": {"a": {"$eq": [s]}}}}}}},
        {"$computed": {"n": s}},
        {"$computed": {"n": {"$sum": {"field": {s: 1}}}}},
        {"$computed": {"n": {"$percentile": {"field": "age", "p": s}}}},
    ]


def _sorts() -> list[Any]:
    return [SECRET, {"dir": SECRET}, {"dir": "asc", "nulls": SECRET}]


def test_a_port_refusal_never_quotes_the_operand() -> None:
    cases = [
        *((QueryFilterExpressionParser.parse, f) for f in _filters()),
        *((AggregatesExpressionParser.parse, a) for a in _aggregates()),
        *((parse_sort_value, v) for v in _sorts()),
    ]

    for parse, value in cases:
        with pytest.raises(CoreException) as ei:
            parse(value)

        assert SECRET not in str(ei.value), (value, str(ei.value))


def test_a_cast_refusal_never_quotes_the_operand() -> None:
    """A bound cast to its field's type (a ``$gt: "28"`` on an int) refuses without it."""

    from forze.application.contracts.querying.internal.cast import QueryValueCaster as C

    for cast in (
        C.as_bool,
        C.as_uuid,
        C.as_int,
        C.as_float,
        C.as_decimal,
        C.parse_datetime,
        C.as_date,
        lambda v: C.as_datetime(v, force_tz=True),
    ):
        with pytest.raises(CoreException) as ei:
            cast(SECRET)

        assert SECRET not in str(ei.value), str(ei.value)


def test_a_request_refusal_never_quotes_the_operand() -> None:
    """The 422 body FastAPI returns (its handler already drops pydantic's ``input``)."""

    client = TestClient(_build_app("rest"))
    bodies = [
        *(("/notes/list", {"filters": f}) for f in _filters() if _json(f)),
        *(("/notes/agg_list", {"aggregates": a}) for a in _aggregates()),
        *(("/notes/list", {"sorts": {"title": v}}) for v in _sorts()),
    ]

    for path, body in bodies:
        refused = client.post(path, json=body)

        assert refused.status_code in (400, 422), (body, refused.text)
        assert SECRET not in refused.text, (body, refused.text)


def _json(value: Any) -> bool:
    return "object at 0x" not in repr(value)


# ....................... #


@pytest.mark.parametrize(
    "filters",
    [
        {"$values": {"items": {"$any": {"$bogus": 1}}}},
        {"$values": {"tags": {"$any": {"$eq": 1, "$bogus": 2}}}},
    ],
)
def test_an_unknown_element_operator_is_named(filters: Any) -> None:
    from pydantic import ValidationError

    from forze_kits.aggregates.document.dto import ListRequestDTO

    message = "Unknown element operator $bogus"

    with pytest.raises(CoreException, match=re.escape(message)):
        QueryFilterExpressionParser.parse(filters)

    with pytest.raises(ValidationError, match=re.escape(message)):
        ListRequestDTO.model_validate({"filters": filters})


# ....................... #


def test_a_set_of_patterns_parses_alike_in_every_process() -> None:
    """A set iterates in hash order, seeded per process; its patterns, and the filter
    fingerprint a cursor carries, are ordered canonically instead."""

    script = (
        "from forze.application.contracts.querying import QueryFilterExpressionParser as P\n"
        "patterns = {'alpha%', 'beta%', 'gamma%', 'delta%', 'eps%', 'zeta%'}\n"
        "print(repr(P.parse({'$values': {'name': {'$like': patterns}, "
        "'tags': {'$any': {'$ilike': frozenset(patterns)}}}})))\n"
    )
    parsed = {
        seed: subprocess.run(
            [sys.executable, "-c", script],
            env={**os.environ, "PYTHONHASHSEED": seed},
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        for seed in ("0", "1", "7", "12345")
    }

    assert len(set(parsed.values())) == 1, f"parsed filter varies by seed: {parsed}"
