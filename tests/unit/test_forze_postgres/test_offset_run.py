"""Unit tests for :mod:`forze_postgres.adapters.search._offset_run`."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

pytest.importorskip("psycopg")

from psycopg import sql

from forze.application.contracts.search import SearchSpec
from forze_postgres.adapters.search import PostgresFTSSearchAdapter
from forze_postgres.adapters.search._offset_run import (
    RankedOffsetPlan,
    execute_simple_ranked_offset_search,
)
from forze_postgres.kernel.gateways import PostgresQualifiedName
from pydantic import BaseModel


class _M(BaseModel):
    id: UUID
    label: str


def _gateway() -> PostgresFTSSearchAdapter[_M]:
    client = MagicMock()
    client.fetch_value = AsyncMock(return_value=2)
    client.fetch_all = AsyncMock(
        return_value=[
            {"id": "00000000-0000-0000-0000-000000000001", "label": "a"},
            {"id": "00000000-0000-0000-0000-000000000002", "label": "b"},
        ],
    )
    intro = MagicMock()
    intro.get_column_types = AsyncMock(return_value={})
    spec = SearchSpec(name="t", model_type=_M, fields=["id", "label"])
    return PostgresFTSSearchAdapter(
        spec=spec,
        codec=spec.resolved_read_codec,
        index_relation=("public", "idx"),
        relation=("public", "v"),
        index_heap_relation=("public", "h"),
        fts_groups={"A": ("label",)},
        client=client,
        model_type=_M,
        introspector=intro,
        tenant_aware=False,
        tenant_provider=None,
        filter_table_alias="v",
    )


@pytest.mark.asyncio
async def test_execute_simple_ranked_offset_search_applies_limit_offset() -> None:
    gw = _gateway()
    plan = RankedOffsetPlan(
        with_clause=sql.SQL("WITH x AS (SELECT 1)"),
        from_outer=sql.SQL("FROM public.v v"),
        order_sql=sql.SQL("1"),
        params=[],
        select_table_alias="v",
    )
    page = await execute_simple_ranked_offset_search(
        gw,
        plan=plan,
        query="q",
        filters=None,
        sorts=None,
        spec=gw.spec,
        variant="test",
        fingerprint_extras=None,
        pagination={"limit": 10, "offset": 0},
        snapshot=None,
        return_count=True,
        return_type=None,
        return_fields=None,
        model_type=_M,
        result_snapshot=None,
    )
    assert page.count == 2
    assert len(page.hits) == 2
    gw.client.fetch_all.assert_awaited_once()
    call_args = gw.client.fetch_all.await_args
    assert call_args is not None
    params = call_args.args[1]
    assert 10 in params
