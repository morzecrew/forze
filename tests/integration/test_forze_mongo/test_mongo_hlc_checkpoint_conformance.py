"""The Mongo high-water-mark store against the shared battery.

Mongo reaches the port's two promises by different mechanisms than Postgres — ``$max``
where Postgres writes ``GREATEST``, an ambient session where Postgres has the connection
itself — so this is exactly the shape the shared battery exists for: one rule, written
twice, and only running both shows whether they agree.

The whole leg runs against the replica-set client: Mongo has no transactions without one,
and the atomicity half of this port is not testable on a standalone server.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.hlc import HlcCheckpointPort
from forze_mongo.adapters import MongoTxManagerAdapter
from forze_mongo.adapters.hlc_checkpoint import MongoHlcCheckpointStore
from forze_mongo.execution.deps.configs import MongoHlcCheckpointConfig
from forze_mongo.kernel.client import MongoClient
from tests.support.hlc_checkpoint_conformance import (
    HLC_CHECKPOINT_BATTERY,
    Check,
    HlcCheckpointHarness,
)

# ----------------------- #

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture
async def hlc_collection(mongo_client_replica: MongoClient) -> tuple[str, str]:
    """A checkpoint collection, which needs no index: the write is keyed on ``_id``."""

    db_name = (await mongo_client_replica.db()).name

    return db_name, f"hlc_checkpoint_{uuid4().hex[:8]}"


@pytest.fixture
def harness(
    mongo_client_replica: MongoClient, hlc_collection: tuple[str, str]
) -> HlcCheckpointHarness:
    def store_for(node_key: str) -> HlcCheckpointPort:
        return MongoHlcCheckpointStore(
            client=mongo_client_replica,
            config=MongoHlcCheckpointConfig(collection=hlc_collection, node_key=node_key),
        )

    return HlcCheckpointHarness(
        store_for=store_for,
        transaction=lambda: MongoTxManagerAdapter(client=mongo_client_replica).transaction(),
        backend="mongo",
    )


@pytest.mark.conformance(plane="hlc_checkpoint", engine="mongo")
@pytest.mark.parametrize("check", HLC_CHECKPOINT_BATTERY, ids=lambda check: check.__name__)
async def test_hlc_checkpoint_battery(check: Check, harness: HlcCheckpointHarness) -> None:
    await check(harness)
