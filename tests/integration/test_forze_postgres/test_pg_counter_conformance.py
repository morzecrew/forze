"""Postgres counter against a live server — the shared conformance battery.

# covers: CounterPort.incr
# covers: CounterPort.incr_batch
# covers: CounterPort.decr
# covers: CounterPort.reset
"""

from __future__ import annotations

from uuid import uuid4

import pytest
import pytest_asyncio

pytest.importorskip("psycopg")

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

    return CounterHarness(
        counter=PostgresCounterAdapter(
            client=pg_client,
            config=PostgresCounterConfig(relation=("public", table)),
            route="conformance",
        ),
        suffix=lambda name: f"{name}-{run}",
    )


@pytest.mark.parametrize("check", COUNTER_BATTERY, ids=lambda check: check.__name__)
async def test_counter_battery(check: Check, harness: CounterHarness) -> None:
    await check(harness)
