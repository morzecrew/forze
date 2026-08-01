"""Postgres counter against a live server — the shared conformance battery.

# covers: CounterPort.incr
# covers: CounterPort.incr_batch
# covers: CounterPort.decr
# covers: CounterPort.reset
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
import pytest_asyncio

pytest.importorskip("psycopg")

from forze.application.contracts.tenancy import TenantIdentity
from forze_postgres.adapters.counter import PostgresCounterAdapter
from forze_postgres.execution.deps.configs import PostgresCounterConfig
from tests.support.counter_conformance import COUNTER_BATTERY, Check, CounterHarness

# ----------------------- #


@pytest_asyncio.fixture
async def harness(pg_client) -> CounterHarness:
    table = f"counter_conf_{uuid4().hex[:8]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {table} (
            tenant_id text   NOT NULL,
            suffix    text   NOT NULL,
            value     bigint NOT NULL,
            PRIMARY KEY (tenant_id, suffix)
        )
        """
    )
    run = uuid4().hex[:8]
    config = PostgresCounterConfig(relation=("public", table))

    def _for_tenant(tenant: UUID) -> PostgresCounterAdapter:
        # One relation, two tenants — the shared-store shape the mock cannot represent.
        return PostgresCounterAdapter(
            client=pg_client,
            config=config,
            route="conformance",
            tenant_aware=True,
            tenant_provider=lambda: TenantIdentity(tenant_id=tenant),
        )

    return CounterHarness(
        counter=PostgresCounterAdapter(client=pg_client, config=config, route="conformance"),
        suffix=lambda name: f"{name}-{run}",
        for_tenant=_for_tenant,
    )


@pytest.mark.conformance(plane="counter", engine="postgres")
@pytest.mark.parametrize("check", COUNTER_BATTERY, ids=lambda check: check.__name__)
async def test_counter_battery(check: Check, harness: CounterHarness) -> None:
    await check(harness)
