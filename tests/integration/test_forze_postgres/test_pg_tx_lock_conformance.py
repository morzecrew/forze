"""Postgres transaction-scoped lock against the shared conformance battery.

Two pools, so the two sessions are different connections and the lock is the database's
``pg_advisory_xact_lock``; a deadlock between them is the one Postgres itself detects.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from testcontainers.postgres import PostgresContainer

from forze_postgres.execution.deps import PostgresDepsModule
from forze_postgres.kernel.client import PostgresConfig
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps
from tests.support.tx_lock_conformance import TX_LOCK_BATTERY, Check, TxLockHarness

# ----------------------- #

_ROUTE = "postgres"


@pytest.fixture
async def second_client(postgres_container: PostgresContainer) -> AsyncIterator[PostgresClient]:
    url = postgres_container.get_connection_url().replace("postgresql+psycopg://", "postgresql://")
    client = PostgresClient()
    await client.initialize(dsn=url, config=PostgresConfig(min_size=1, max_size=3))

    yield client

    await client.close()


@pytest.fixture
def harness(pg_client: PostgresClient, second_client: PostgresClient) -> TxLockHarness:
    return TxLockHarness(
        first=context_from_deps(PostgresDepsModule(client=pg_client, tx={_ROUTE})()),
        second=context_from_deps(PostgresDepsModule(client=second_client, tx={_ROUTE})()),
        route=_ROUTE,
        backend="postgres",
    )


@pytest.mark.conformance(plane="tx_lock", engine="postgres")
@pytest.mark.parametrize("check", TX_LOCK_BATTERY, ids=lambda check: check.__name__)
async def test_tx_lock_battery(check: Check, harness: TxLockHarness) -> None:
    await check(harness)
