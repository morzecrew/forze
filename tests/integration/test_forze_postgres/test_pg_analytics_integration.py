"""Integration tests for Postgres analytics adapter."""

from __future__ import annotations

from datetime import timedelta

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
    IngestSpec,
)
from forze.base.exceptions import CoreException
from forze_postgres.adapters.analytics import PostgresAnalyticsAdapter
from forze_postgres.execution import PostgresDepsModule
from forze_postgres.execution.deps.configs import PostgresAnalyticsConfig, PostgresQueryConfig
from forze_postgres.kernel.client import PostgresClient
from tests.support.execution_context import (
    context_from_deps,
)

pytestmark = pytest.mark.integration


class _Row(BaseModel):
    value: int
    event: str


class _Params(BaseModel):
    pass


class _Ingest(BaseModel):
    event: str
    value: int = 1


def _spec() -> AnalyticsSpec[_Row, _Ingest]:
    return AnalyticsSpec(
        name="events",
        read=_Row,
        queries={
            "all": AnalyticsQueryDefinition(params=_Params),
        },
        ingest=_Ingest,
    )


def _config(table_id: str) -> PostgresAnalyticsConfig:
    return PostgresAnalyticsConfig(
        queries={
            "all": PostgresQueryConfig(
                sql=f"SELECT event, value FROM public.{table_id}",
            ),
        },
        ingest=IngestSpec(("public", table_id)),
    )


@pytest.mark.asyncio
async def test_append_and_query(pg_client: PostgresClient, pg_analytics_table: str) -> None:
    table_id = pg_analytics_table
    spec = _spec()
    adapter = PostgresAnalyticsAdapter(
        client=pg_client,
        spec=spec,
        config=_config(table_id),
    )

    await adapter.append([_Ingest(event="signup", value=42)])

    page = await adapter.run_page("all", _Params())
    assert page.count >= 1
    assert any(hit.event == "signup" and hit.value == 42 for hit in page.hits)


@pytest.mark.asyncio
async def test_deps_module_wiring(pg_client: PostgresClient, pg_analytics_table: str) -> None:
    table_id = pg_analytics_table
    spec = _spec()
    module = PostgresDepsModule(
        client=pg_client,
        analytics={
            "events": _config(table_id),
        },
    )
    ctx = context_from_deps(module())
    port = ctx.analytics.query(spec)
    page = await port.run("all", _Params())
    assert len(page.hits) >= 0


@pytest.mark.asyncio
async def test_run_chunked_reads_batches(
    pg_client: PostgresClient,
    pg_analytics_table: str,
) -> None:
    table_id = pg_analytics_table
    spec = _spec()
    adapter = PostgresAnalyticsAdapter(
        client=pg_client,
        spec=spec,
        config=_config(table_id),
    )

    await adapter.append([_Ingest(event=f"evt_{i}", value=i) for i in range(3)])

    batches = [
        batch
        async for batch in adapter.run_chunked(
            "all",
            _Params(),
            fetch_batch_size=2,
        )
    ]
    total = sum(len(batch) for batch in batches)
    assert total >= 3


@pytest.mark.asyncio
async def test_run_cursor_offset_pagination(
    pg_client: PostgresClient,
    pg_analytics_table: str,
) -> None:
    table_id = pg_analytics_table
    spec = _spec()
    adapter = PostgresAnalyticsAdapter(
        client=pg_client,
        spec=spec,
        config=_config(table_id),
    )

    await adapter.append([_Ingest(event=f"page_{i}", value=i) for i in range(5)])

    seen: list[str] = []
    cursor: dict[str, object] | None = {"limit": 2}
    while cursor is not None:
        page = await adapter.run_cursor("all", _Params(), cursor=cursor)
        seen.extend(hit.event for hit in page.hits)
        cursor = {"limit": 2, "after": page.next_cursor} if page.next_cursor is not None else None

    assert len(seen) == 5
    assert len(set(seen)) == 5


@pytest.mark.asyncio
async def test_run_cursor_rejects_before_cursor(
    pg_client: PostgresClient,
    pg_analytics_table: str,
) -> None:
    table_id = pg_analytics_table
    adapter = PostgresAnalyticsAdapter(
        client=pg_client,
        spec=_spec(),
        config=_config(table_id),
    )

    with pytest.raises(CoreException, match="Backward analytics cursors"):
        await adapter.run_cursor(
            "all",
            _Params(),
            cursor={"limit": 2, "before": "opaque"},
        )


@pytest.mark.asyncio
async def test_timeout_option_does_not_leak_into_a_caller_transaction(
    pg_client: PostgresClient,
    pg_analytics_table: str,
) -> None:
    """Inside a caller transaction the analytics run becomes a savepoint; its
    ``SET LOCAL statement_timeout`` must not survive the savepoint release."""

    adapter = PostgresAnalyticsAdapter(
        client=pg_client,
        spec=_spec(),
        config=_config(pg_analytics_table),
    )

    async with pg_client.transaction():
        before = await pg_client.fetch_value("SHOW statement_timeout", None)
        await adapter.run_page(
            "all",
            _Params(),
            options={"timeout": timedelta(seconds=55)},
        )
        after = await pg_client.fetch_value("SHOW statement_timeout", None)

    assert after == before


@pytest.mark.asyncio
async def test_query_schema_does_not_leak_search_path_into_a_caller_transaction(
    pg_client: PostgresClient,
    pg_analytics_table: str,
) -> None:
    """Same seam for the per-tenant query schema: the run's ``SET LOCAL search_path``
    must not merge into the caller's transaction when the savepoint is released."""

    table_id = pg_analytics_table
    config = PostgresAnalyticsConfig(
        queries={
            "all": PostgresQueryConfig(
                sql=f"SELECT event, value FROM public.{table_id}",
            ),
        },
        ingest=IngestSpec(("public", table_id)),
        query_schema="public",
    )
    adapter = PostgresAnalyticsAdapter(
        client=pg_client,
        spec=_spec(),
        config=config,
    )

    async with pg_client.transaction():
        before = await pg_client.fetch_value("SHOW search_path", None)
        await adapter.run_page("all", _Params())
        after = await pg_client.fetch_value("SHOW search_path", None)

    assert after == before


@pytest.mark.asyncio
async def test_timeout_option_still_applies_at_the_root(
    pg_client: PostgresClient,
    pg_analytics_table: str,
) -> None:
    """Outside a caller transaction the run's own transaction ends with the query, so no
    restore round-trips are needed and the timed run behaves as before."""

    adapter = PostgresAnalyticsAdapter(
        client=pg_client,
        spec=_spec(),
        config=_config(pg_analytics_table),
    )

    await adapter.append([_Ingest(event="timed", value=7)])

    page = await adapter.run_page(
        "all",
        _Params(),
        options={"timeout": timedelta(seconds=30)},
    )
    assert any(hit.event == "timed" for hit in page.hits)
