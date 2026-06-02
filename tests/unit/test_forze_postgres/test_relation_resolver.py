"""Unit tests for Postgres document relation resolvers."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze_postgres.kernel.relation import resolve_postgres_qname


@pytest.mark.asyncio
async def test_resolve_static_relation() -> None:
    qname = await resolve_postgres_qname(("public", "items"), None)
    assert qname.schema == "public"
    assert qname.name == "items"


@pytest.mark.asyncio
async def test_resolve_callable_relation() -> None:
    tid = uuid4()

    def resolver(tenant_id: object) -> tuple[str, str]:
        assert tenant_id == tid
        return (f"tenant_{tid.hex[:8]}", "items")

    qname = await resolve_postgres_qname(resolver, tid)
    assert qname.schema == f"tenant_{tid.hex[:8]}"
    assert qname.name == "items"


@pytest.mark.asyncio
async def test_search_adapter_resolves_dynamic_index_and_heap() -> None:
    from unittest.mock import MagicMock
    from uuid import uuid4

    from pydantic import BaseModel

    from forze.application.contracts.search import SearchSpec
    from forze_postgres.adapters.search import PostgresFTSSearchAdapter

    class _Row(BaseModel):
        id: str
        a: str

    tid = uuid4()
    spec = SearchSpec(name="s", model_type=_Row, fields=["a"])

    adapter = PostgresFTSSearchAdapter(
        spec=spec,
        codec=spec.resolved_read_codec,
        relation=("public", "v"),
        index_relation=lambda t: (f"t_{t}", "idx") if t else ("public", "idx"),
        index_heap_relation=lambda t: (f"t_{t}", "heap") if t else ("public", "heap"),
        fts_groups={"A": ("a",)},
        client=MagicMock(),
        model_type=_Row,
        introspector=MagicMock(),
        tenant_provider=lambda: type("T", (), {"tenant_id": tid})(),
        tenant_aware=False,
    )

    index_qn = await adapter._index_qname()
    heap_qn = await adapter._index_heap_qname()

    assert index_qn.schema == f"t_{tid}"
    assert index_qn.name == "idx"
    assert heap_qn.schema == f"t_{tid}"
    assert heap_qn.name == "heap"


@pytest.mark.asyncio
async def test_analytics_adapter_resolves_dynamic_ingest_relation() -> None:
    from unittest.mock import MagicMock
    from uuid import uuid4

    from pydantic import BaseModel

    from forze.application.contracts.analytics import (
        AnalyticsQueryDefinition,
        AnalyticsSpec,
    )
    from forze_postgres.adapters.analytics import PostgresAnalyticsAdapter
    from forze_postgres.execution.deps.configs import (
        PostgresAnalyticsConfig,
        PostgresQueryConfig,
    )

    class _Row(BaseModel):
        value: int

    class _Params(BaseModel):
        day: str = "2026-01-01"

    tid = uuid4()
    spec = AnalyticsSpec(
        name="events",
        read=_Row,
        queries={"counts": AnalyticsQueryDefinition(params=_Params)},
    )
    config = PostgresAnalyticsConfig(
        queries={
            "counts": PostgresQueryConfig(sql="SELECT 1"),
        },
        ingest_relation=lambda t: (f"t_{t}", "raw") if t else ("public", "raw"),
    )
    adapter = PostgresAnalyticsAdapter(
        client=MagicMock(),
        spec=spec,
        config=config,
        tenant_provider=lambda: type("T", (), {"tenant_id": tid})(),
    )

    qn = await adapter._ingest_qname()

    assert qn.schema == f"t_{tid}"
    assert qn.name == "raw"
