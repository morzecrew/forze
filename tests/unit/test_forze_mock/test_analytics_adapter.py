"""Tests for MockAnalyticsAdapter and ctx.analytics resolution."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
)
from forze.application.execution import ExecutionContext
from forze_mock import MockDepsModule, MockState
from forze_mock.execution import MockStateDepKey


class _Row(BaseModel):
    value: int


class _Params(BaseModel):
    day: str = "2026-01-01"


class _Ingest(BaseModel):
    event: str


def _spec() -> AnalyticsSpec[_Row, _Ingest]:
    return AnalyticsSpec(
        name="events",
        read=_Row,
        queries={"counts": AnalyticsQueryDefinition(params=_Params)},
        ingest=_Ingest,
    )


@pytest.fixture
def ctx() -> ExecutionContext:
    state = MockState()
    state.analytics_query_hits["events"] = {
        "counts": [{"value": 10}, {"value": 20}],
    }
    module = MockDepsModule(state=state)
    return ExecutionContext(deps=module())


@pytest.mark.asyncio
async def test_query_via_ctx_analytics(ctx: ExecutionContext) -> None:
    spec = _spec()
    port = ctx.analytics.query(spec)
    page = await port.run_page("counts", _Params())
    assert page.count == 2
    assert page.hits[0].value == 10


@pytest.mark.asyncio
async def test_ingest_via_ctx_analytics(ctx: ExecutionContext) -> None:
    spec = _spec()
    ingest = ctx.analytics.ingest(spec)
    result = await ingest.append([_Ingest(event="signup")])
    assert result is not None
    assert result.accepted == 1
    state = ctx.deps.provide(MockStateDepKey)
    assert state.analytics_ingest_log["events"][0]["event"] == "signup"


@pytest.mark.asyncio
async def test_unknown_query_key_raises(ctx: ExecutionContext) -> None:
    spec = _spec()
    port = ctx.analytics.query(spec)
    with pytest.raises(exc.internal, match="Unknown analytics query"):
        await port.run("missing", _Params())
