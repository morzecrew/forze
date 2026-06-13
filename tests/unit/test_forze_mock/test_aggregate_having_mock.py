"""Validate the in-memory ``$having`` oracle against hand-computed expected rows."""

from __future__ import annotations

from typing import Any

import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.base.exceptions import CoreException
from forze_mock.adapters import MockDocumentAdapter, MockState
from tests.support.aggregate_having import (
    SEED,
    AggCreate,
    AggDoc,
    AggRead,
    rowset,
    seed_aggregate_corpus,
)

pytestmark = pytest.mark.unit


def _mock() -> MockDocumentAdapter[Any, Any, Any, Any]:
    spec = DocumentSpec(
        name="agg",
        read=AggRead,
        write=DocumentWriteTypes(domain=AggDoc, create_cmd=AggCreate),
    )
    return MockDocumentAdapter(
        spec=spec,
        state=MockState(),
        namespace="agg",
        read_model=AggRead,
        domain_model=AggDoc,
    )


async def _agg(doc: Any, aggregates: dict[str, Any]) -> set[Any]:
    page = await doc.aggregate_page(aggregates=aggregates, pagination={"limit": 100})
    return rowset(page.hits)


@pytest.mark.asyncio
async def test_having_count_threshold() -> None:
    doc = _mock()
    await seed_aggregate_corpus(doc)

    got = await _agg(
        doc,
        {
            "$groups": {"region": "region"},
            "$computed": {"cnt": {"$count": None}, "total": {"$sum": "amount"}},
            "$having": {"$values": {"cnt": {"$gte": 2}}},
        },
    )
    # east(3,35), west(2,150), north(2,10) keep; south(1,40) drops.
    assert got == {
        (("cnt", 3), ("region", "east"), ("total", 35)),
        (("cnt", 2), ("region", "west"), ("total", 150)),
        (("cnt", 2), ("region", "north"), ("total", 10)),
    }


@pytest.mark.asyncio
async def test_having_sum_threshold() -> None:
    doc = _mock()
    await seed_aggregate_corpus(doc)

    got = await _agg(
        doc,
        {
            "$groups": {"region": "region"},
            "$computed": {"total": {"$sum": "amount"}},
            "$having": {"$values": {"total": {"$gt": 50}}},
        },
    )
    assert got == {(("region", "west"), ("total", 150))}


@pytest.mark.asyncio
async def test_having_group_key_and_metric() -> None:
    doc = _mock()
    await seed_aggregate_corpus(doc)

    got = await _agg(
        doc,
        {
            "$groups": {"region": "region"},
            "$computed": {"cnt": {"$count": None}, "total": {"$sum": "amount"}},
            "$having": {
                "$and": [
                    {"$values": {"region": {"$in": ["east", "west"]}}},
                    {"$values": {"cnt": {"$gte": 2}}},
                ],
            },
        },
    )
    assert got == {
        (("cnt", 3), ("region", "east"), ("total", 35)),
        (("cnt", 2), ("region", "west"), ("total", 150)),
    }


def test_having_rejects_unknown_alias() -> None:
    from forze.application.contracts.querying.internal.aggregate import (
        AggregatesExpressionParser,
    )

    with pytest.raises(CoreException, match="aggregate output aliases"):
        AggregatesExpressionParser.parse(
            {
                "$computed": {"cnt": {"$count": None}},
                "$having": {"$values": {"amount": {"$gt": 1}}},  # raw field, not an alias
            }
        )
