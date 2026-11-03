"""Registered routes are bounded by their own timeout on a real server (RFC 0028 §6.1).

An analytics query and a procedure each carry a timeout that applies when the call sets none.
A per-call timeout lowers it and never raises it, and the route's bound never loosens a lower
``statement_timeout`` already in force. The server, not the client, has to stop the statement:
afterwards the connection is reused and nothing is left running.
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any

import pytest
import pytest_asyncio
from pydantic import BaseModel

from forze.application.contracts.analytics import AnalyticsQueryDefinition, AnalyticsSpec
from forze.application.contracts.procedure import ProcedureSpec
from forze.base.exceptions import CoreException, ExceptionKind
from forze_postgres.adapters.analytics import PostgresAnalyticsAdapter
from forze_postgres.adapters.procedure import PostgresProcedureAdapter
from forze_postgres.execution.deps.configs import (
    PostgresAnalyticsConfig,
    PostgresProcedureConfig,
    PostgresQueryConfig,
)
from forze_postgres.kernel.client import PostgresClient, PostgresConfig

pytestmark = pytest.mark.integration

# ----------------------- #

_SHORT = timedelta(milliseconds=300)
_SLEEP_SQL = "SELECT 'x' AS event, 1 AS value FROM pg_sleep(5)"


class _Row(BaseModel):
    value: int
    event: str


class _Params(BaseModel):
    pass


def _analytics(
    client: PostgresClient, *, sql: str = _SLEEP_SQL, **query: Any
) -> PostgresAnalyticsAdapter[_Row, Any]:
    spec: AnalyticsSpec[_Row, Any] = AnalyticsSpec(
        name="slow",
        read=_Row,
        queries={"sleep": AnalyticsQueryDefinition(params=_Params)},
    )
    config = PostgresAnalyticsConfig(
        queries={"sleep": PostgresQueryConfig(sql=sql, skip_total=True, **query)},
    )

    return PostgresAnalyticsAdapter(client=client, spec=spec, config=config)


def _procedure(client: PostgresClient, **config: Any) -> PostgresProcedureAdapter[_Params, Any]:
    return PostgresProcedureAdapter(
        client=client,
        spec=ProcedureSpec(name="sleep", params=_Params),
        config=PostgresProcedureConfig(sql="SELECT pg_sleep(5)", **config),
    )


async def _assert_canceled_and_reusable(client: PostgresClient, call: Any) -> None:
    started = time.monotonic()

    with pytest.raises(CoreException, match="canceled") as caught:
        await call()

    assert caught.value.kind is ExceptionKind.INFRASTRUCTURE
    assert time.monotonic() - started < 3  # stopped at the bound, not at the 5 s sleep

    # Nothing is still running on the server, and the pool hands out a working connection.
    running = await client.fetch_value(
        "SELECT count(*) FROM pg_stat_activity "
        "WHERE state = 'active' AND query LIKE '%FROM pg_sleep(5)%' "
        "AND pid <> pg_backend_pid()",
        None,
    )
    assert running == 0
    assert await client.fetch_value("SELECT 1", None) == 1


# ....................... #


@pytest.mark.asyncio
async def test_an_analytics_read_with_no_option_is_bounded_by_its_route(
    pg_client: PostgresClient,
) -> None:
    adapter = _analytics(pg_client, timeout=_SHORT)

    await _assert_canceled_and_reusable(pg_client, lambda: adapter.run("sleep", _Params()))


@pytest.mark.asyncio
async def test_a_per_call_timeout_cannot_lift_the_route_timeout(
    pg_client: PostgresClient,
) -> None:
    adapter = _analytics(pg_client, timeout=_SHORT)

    await _assert_canceled_and_reusable(
        pg_client,
        lambda: adapter.run("sleep", _Params(), options={"timeout": timedelta(hours=1)}),
    )


@pytest.mark.asyncio
async def test_the_route_timeout_never_loosens_a_tighter_caller_bound(
    pg_client: PostgresClient,
) -> None:
    adapter = _analytics(pg_client)  # the 60 s default

    async with pg_client.transaction():
        await pg_client.execute("SET LOCAL statement_timeout = 300")

        await _assert_canceled_and_reusable(pg_client, lambda: adapter.run("sleep", _Params()))

        # The run's savepoint left the caller's own bound as it was.
        assert await pg_client.fetch_value("SHOW statement_timeout", None) == "300ms"


@pytest_asyncio.fixture
async def capped_client(postgres_container: Any) -> AsyncIterator[PostgresClient]:
    """A client whose pool sets a session ``statement_timeout`` of 300 ms."""

    url = postgres_container.get_connection_url().replace("postgresql+psycopg://", "postgresql://")
    client = PostgresClient()
    await client.initialize(
        dsn=url,
        config=PostgresConfig(min_size=1, max_size=2, statement_timeout=_SHORT),
    )

    try:
        yield client

    finally:
        await client.close()


@pytest.mark.asyncio
async def test_the_route_timeout_never_loosens_the_pool_statement_timeout(
    capped_client: PostgresClient,
) -> None:
    adapter = _analytics(capped_client)  # the 60 s default, above the pool's 300 ms

    await _assert_canceled_and_reusable(capped_client, lambda: adapter.run("sleep", _Params()))


@pytest.mark.asyncio
async def test_a_route_timeout_below_the_pool_statement_timeout_applies(
    capped_client: PostgresClient,
) -> None:
    # A 200 ms sleep fits inside the pool's 300 ms, so only the route's 100 ms can stop it.
    adapter = _analytics(
        capped_client,
        sql="SELECT 'x' AS event, 1 AS value FROM pg_sleep(0.2)",
        timeout=timedelta(milliseconds=100),
    )

    with pytest.raises(CoreException, match="canceled"):
        await adapter.run("sleep", _Params())


# ....................... #


@pytest.mark.asyncio
async def test_a_procedure_is_bounded_by_its_statement_timeout(
    pg_client: PostgresClient,
) -> None:
    adapter = _procedure(pg_client, statement_timeout=_SHORT)

    await _assert_canceled_and_reusable(pg_client, lambda: adapter.run(_Params()))


@pytest.mark.asyncio
async def test_an_autocommit_procedure_is_canceled_on_the_server(
    pg_client: PostgresClient,
) -> None:
    adapter = _procedure(pg_client, in_transaction=False, statement_timeout=_SHORT)

    await _assert_canceled_and_reusable(pg_client, lambda: adapter.run(_Params()))


# ....................... #


@pytest_asyncio.fixture
async def one_connection_client(postgres_container: Any) -> AsyncIterator[PostgresClient]:
    """A client whose pool holds one connection, so a later statement sees the same one."""

    url = postgres_container.get_connection_url().replace("postgresql+psycopg://", "postgresql://")
    client = PostgresClient()
    await client.initialize(dsn=url, config=PostgresConfig(min_size=1, max_size=1))

    try:
        yield client

    finally:
        await client.close()


@pytest.mark.asyncio
async def test_a_root_read_leaves_nothing_on_its_pooled_connection(
    one_connection_client: PostgresClient,
) -> None:
    client = one_connection_client
    sql = (
        "SELECT current_setting('search_path') || ' / ' "
        "|| current_setting('statement_timeout') AS event, 1 AS value"
    )
    adapter = PostgresAnalyticsAdapter(
        client=client,
        spec=_analytics(client).spec,
        config=PostgresAnalyticsConfig(
            queries={"sleep": PostgresQueryConfig(sql=sql, timeout=timedelta(seconds=7))},
            query_schema="pg_catalog",
        ),
    )

    page = await adapter.run("sleep", _Params())

    # Both settings applied to the read, the tenant schema before public...
    assert page.hits[0].event == "pg_catalog, public / 7s"
    # ...and ended with its implicit transaction, on the pool's one connection.
    assert await client.fetch_value("SHOW statement_timeout", None) == "0"
    assert await client.fetch_value("SHOW search_path", None) == '"$user", public'


@pytest.mark.asyncio
async def test_a_procedure_in_a_caller_transaction_restores_its_settings(
    pg_client: PostgresClient,
) -> None:
    adapter = PostgresProcedureAdapter(
        client=pg_client,
        spec=ProcedureSpec(name="noop", params=_Params),
        config=PostgresProcedureConfig(
            sql="SELECT 1",
            statement_timeout=timedelta(seconds=7),
            query_schema="pg_catalog",
        ),
    )

    async with pg_client.transaction():
        await pg_client.execute("SET LOCAL statement_timeout = 30000")
        await adapter.run(_Params())

        assert await pg_client.fetch_value("SHOW statement_timeout", None) == "30s"
        assert await pg_client.fetch_value("SHOW search_path", None) == '"$user", public'
