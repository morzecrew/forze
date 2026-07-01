"""Integration: a bound invocation deadline is pushed to Postgres as a per-tx statement_timeout.

The server then cancels a query the deadline would kill anyway, so the connection recovers
cleanly instead of being stuck behind an asyncio-cancelled-but-server-running statement.
"""

from __future__ import annotations

import pytest

from forze.application.execution import bind_deadline
from forze.base.exceptions import CoreException
from forze_postgres.adapters.txmanager import PostgresTxManagerAdapter
from forze_postgres.kernel.client.client import PostgresClient

# ----------------------- #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_deadline_sets_a_per_tx_statement_timeout(
    pg_client: PostgresClient,
) -> None:
    tx = PostgresTxManagerAdapter(client=pg_client)

    with bind_deadline(5.0):  # generous — won't fire; just confirm the bound is applied
        async with tx.transaction():
            in_tx = await pg_client.fetch_value("SHOW statement_timeout")

    assert in_tx != "0"  # a per-tx statement_timeout (~5.1s) was set from the deadline


@pytest.mark.integration
@pytest.mark.asyncio
async def test_deadline_cancels_a_slow_query_and_the_connection_recovers(
    pg_client: PostgresClient,
) -> None:
    tx = PostgresTxManagerAdapter(client=pg_client)

    with bind_deadline(0.2):  # 200ms budget → statement_timeout ~300ms
        with pytest.raises(CoreException):  # pg_sleep(3) far exceeds it → server cancels
            async with tx.transaction():
                await pg_client.fetch_value("SELECT pg_sleep(3)")

    # The connection was not left stuck behind the cancelled query — it is reusable.
    assert await pg_client.fetch_value("SELECT 1") == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_no_deadline_does_not_change_statement_timeout(
    pg_client: PostgresClient,
) -> None:
    baseline = await pg_client.fetch_value("SHOW statement_timeout")
    tx = PostgresTxManagerAdapter(client=pg_client)

    async with tx.transaction():  # no deadline bound
        in_tx = await pg_client.fetch_value("SHOW statement_timeout")

    assert in_tx == baseline  # the backstop is not applied without a deadline
