"""The Postgres high-water-mark store against the shared battery.

The engine leg the oracle is compared to. Postgres reaches the two promises through
``GREATEST`` in an upsert and through writing on the caller's transaction connection —
mechanisms the mock models rather than shares, which is what the comparison is for.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from psycopg import sql

from forze.application.contracts.hlc import HlcCheckpointPort
from forze_postgres.adapters.hlc_checkpoint import PostgresHlcCheckpointStore
from forze_postgres.adapters.txmanager import PostgresTxManagerAdapter
from forze_postgres.execution.deps.configs import PostgresHlcCheckpointConfig
from forze_postgres.kernel.client import PostgresClient
from tests.support.hlc_checkpoint_conformance import (
    HLC_CHECKPOINT_BATTERY,
    Check,
    HlcCheckpointHarness,
)

# ----------------------- #

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture
async def hlc_table(pg_client: PostgresClient) -> str:
    table = f"hlc_checkpoint_{uuid4().hex[:8]}"
    await pg_client.execute(
        sql.SQL(
            """
            CREATE TABLE {table} (
                node_key text   NOT NULL,
                hlc      bigint NOT NULL,
                PRIMARY KEY (node_key)
            )
            """
        ).format(table=sql.Identifier("public", table))
    )

    return table


@pytest.fixture
def harness(pg_client: PostgresClient, hlc_table: str) -> HlcCheckpointHarness:
    def store_for(node_key: str) -> HlcCheckpointPort:
        return PostgresHlcCheckpointStore(
            client=pg_client,
            config=PostgresHlcCheckpointConfig(
                relation=("public", hlc_table), node_key=node_key
            ),
        )

    return HlcCheckpointHarness(
        store_for=store_for,
        transaction=lambda: PostgresTxManagerAdapter(client=pg_client).transaction(),
        backend="postgres",
    )


@pytest.mark.conformance(plane="hlc_checkpoint", engine="postgres")
@pytest.mark.parametrize("check", HLC_CHECKPOINT_BATTERY, ids=lambda check: check.__name__)
async def test_hlc_checkpoint_battery(check: Check, harness: HlcCheckpointHarness) -> None:
    await check(harness)
