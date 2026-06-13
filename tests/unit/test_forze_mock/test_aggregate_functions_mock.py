"""The in-memory oracle for the extended aggregate functions (exact values + edges)."""

from __future__ import annotations

import statistics
from typing import Any

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.base.exceptions import CoreException
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_mock.adapters import MockDocumentAdapter, MockState

pytestmark = pytest.mark.unit


class _Fields(BaseModel):
    grp: str
    tier: str | None = None
    amount: int = 0


class _Create(CreateDocumentCmd, _Fields):
    pass


class _Doc(Document, _Fields):
    pass


class _Read(ReadDocument, _Fields):
    pass


def _mock() -> MockDocumentAdapter[Any, Any, Any, Any]:
    spec = DocumentSpec(
        name="agg",
        read=_Read,
        write=DocumentWriteTypes(domain=_Doc, create_cmd=_Create),
    )
    return MockDocumentAdapter(
        spec=spec,
        state=MockState(),
        namespace="agg",
        read_model=_Read,
        domain_model=_Doc,
    )


async def _row(doc: Any, aggregates: dict[str, Any]) -> dict[str, Any]:
    page = await doc.aggregate_page(aggregates=aggregates, pagination={"limit": 10})
    assert len(page.hits) == 1
    return page.hits[0]


@pytest.mark.asyncio
async def test_count_distinct_excludes_null() -> None:
    doc = _mock()
    for tier in ("gold", "gold", "silver", None, None):
        await doc.create(_Create(grp="g", tier=tier, amount=1))

    row = await _row(
        doc,
        {"$groups": {"g": "grp"}, "$computed": {"d": {"$count_distinct": "tier"}}},
    )
    # {gold, silver} = 2; nulls don't count (SQL DISTINCT semantics)
    assert row["d"] == 2


@pytest.mark.asyncio
async def test_stddev_variance_pop_and_samp() -> None:
    doc = _mock()
    for a in (10, 20, 30, 40):
        await doc.create(_Create(grp="g", amount=a))

    row = await _row(
        doc,
        {
            "$groups": {"g": "grp"},
            "$computed": {
                "sp": {"$stddev_pop": "amount"},
                "ss": {"$stddev_samp": "amount"},
                "vp": {"$var_pop": "amount"},
                "vs": {"$var_samp": "amount"},
            },
        },
    )
    data = [10, 20, 30, 40]
    assert row["sp"] == pytest.approx(statistics.pstdev(data))
    assert row["ss"] == pytest.approx(statistics.stdev(data))
    assert row["vp"] == pytest.approx(statistics.pvariance(data))
    assert row["vs"] == pytest.approx(statistics.variance(data))


@pytest.mark.asyncio
async def test_sample_stats_single_value_is_none() -> None:
    doc = _mock()
    await doc.create(_Create(grp="g", amount=5))

    row = await _row(
        doc,
        {
            "$groups": {"g": "grp"},
            "$computed": {
                "sp": {"$stddev_pop": "amount"},  # population of one = 0
                "ss": {"$stddev_samp": "amount"},  # sample of one = undefined → None
                "vs": {"$var_samp": "amount"},
            },
        },
    )
    assert row["sp"] == 0
    assert row["ss"] is None
    assert row["vs"] is None


@pytest.mark.asyncio
async def test_percentile_interpolates() -> None:
    doc = _mock()
    for a in (10, 20, 30, 40):
        await doc.create(_Create(grp="g", amount=a))

    row = await _row(
        doc,
        {
            "$groups": {"g": "grp"},
            "$computed": {
                "p0": {"$percentile": {"field": "amount", "p": 0.0}},
                "p50": {"$percentile": {"field": "amount", "p": 0.5}},
                "p100": {"$percentile": {"field": "amount", "p": 1.0}},
            },
        },
    )
    assert row["p0"] == 10
    assert row["p50"] == pytest.approx(25.0)  # interpolated (20+30)/2
    assert row["p100"] == 40


def test_percentile_requires_p() -> None:
    from forze.application.contracts.querying.internal.aggregate import (
        AggregatesExpressionParser,
    )

    # The application form without a quantile is rejected...
    with pytest.raises(CoreException, match="quantile"):
        AggregatesExpressionParser.parse(
            {"$computed": {"p": {"$percentile": {"field": "amount"}}}}
        )

    # ...and there is no scalar shorthand for percentile.
    with pytest.raises(CoreException, match="no scalar shorthand"):
        AggregatesExpressionParser.parse(
            {"$computed": {"p": {"$percentile": "amount"}}}
        )
