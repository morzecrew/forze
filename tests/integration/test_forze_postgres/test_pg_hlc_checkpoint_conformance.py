"""The Postgres high-water-mark store against the shared battery.

The engine leg the oracle is compared to. Postgres reaches the two promises through
``GREATEST`` in an upsert and through writing on the caller's transaction connection —
mechanisms the mock models rather than shares, which is what the comparison is for.
"""

from __future__ import annotations

from typing import Any, cast
from uuid import uuid4

import attrs
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
    WriteGate,
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


@attrs.define(slots=True)
class _GatedWriteClient:
    """Delegates to a real client, holding the first ``execute`` at the gate.

    Reads pass straight through, so the store stops *at its write* — after whatever it read
    — and another writer can land in that window. Only the first write is held: the store
    issues one, and holding every write would deadlock a check that advances twice.
    """

    inner: PostgresClient
    gate: WriteGate
    held: bool = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)

    async def execute(self, *args: Any, **kwargs: Any) -> Any:
        if not self.held:
            self.held = True
            await self.gate.hold()

        return await self.inner.execute(*args, **kwargs)


@pytest.fixture
def harness(pg_client: PostgresClient, hlc_table: str) -> HlcCheckpointHarness:
    def store_for(node_key: str) -> HlcCheckpointPort:
        return PostgresHlcCheckpointStore(
            client=pg_client,
            config=PostgresHlcCheckpointConfig(
                relation=("public", hlc_table), node_key=node_key
            ),
        )

    def gated_writer() -> tuple[HlcCheckpointPort, WriteGate]:
        gate = WriteGate()
        # Run in its own task, and the pooled client binds it a separate connection — so
        # holding this statement does not hold the other writer's.
        gated = PostgresHlcCheckpointStore(
            client=cast("PostgresClient", _GatedWriteClient(inner=pg_client, gate=gate)),
            config=PostgresHlcCheckpointConfig(relation=("public", hlc_table), node_key="solo"),
        )

        return gated, gate

    return HlcCheckpointHarness(
        store_for=store_for,
        transaction=lambda: PostgresTxManagerAdapter(client=pg_client).transaction(),
        backend="postgres",
        gated_writer=gated_writer,
    )


@pytest.mark.conformance(plane="hlc_checkpoint", engine="postgres")
@pytest.mark.parametrize("check", HLC_CHECKPOINT_BATTERY, ids=lambda check: check.__name__)
async def test_hlc_checkpoint_battery(check: Check, harness: HlcCheckpointHarness) -> None:
    await check(harness)
