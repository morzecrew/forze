"""The Mongo high-water-mark store against the shared battery.

Mongo reaches the port's two promises by different mechanisms than Postgres — ``$max``
where Postgres writes ``GREATEST``, an ambient session where Postgres has the connection
itself — so this is exactly the shape the shared battery exists for: one rule, written
twice, and only running both shows whether they agree.

The whole leg runs against the replica-set client: Mongo has no transactions without one,
and the atomicity half of this port is not testable on a standalone server.
"""

from __future__ import annotations

from typing import Any, cast
from uuid import uuid4

import attrs
import pytest

from forze.application.contracts.hlc import HlcCheckpointPort
from forze.base.exceptions import CoreException, ExceptionKind
from forze_mongo.adapters import MongoTxManagerAdapter
from forze_mongo.adapters.hlc_checkpoint import MongoHlcCheckpointStore
from forze_mongo.execution.deps.configs import MongoHlcCheckpointConfig
from forze_mongo.kernel.client import MongoClient
from tests.support.hlc_checkpoint_conformance import (
    HLC_CHECKPOINT_BATTERY,
    Check,
    HlcCheckpointHarness,
    WriteGate,
)

# ----------------------- #

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture
async def hlc_collection(mongo_client_replica: MongoClient) -> tuple[str, str]:
    """A checkpoint collection, which needs no index: the write is keyed on ``_id``."""

    db_name = (await mongo_client_replica.db()).name

    return db_name, f"hlc_checkpoint_{uuid4().hex[:8]}"


@attrs.define(slots=True)
class _GatedWriteClient:
    """Delegates to a real client, holding the first ``update_one_upsert`` at the gate.

    Everything else passes straight through — the point is to stop the store *at its write*,
    after any reading it does, so another writer can land in that window. Only the first
    write is held; the store issues one, and a wrapper that held every write would deadlock
    a battery check that advances twice.
    """

    inner: MongoClient
    gate: WriteGate
    held: bool = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)

    async def update_one_upsert(self, coll: Any, flt: Any, update: Any) -> Any:
        if not self.held:
            self.held = True
            await self.gate.hold()

        return await self.inner.update_one_upsert(coll, flt, update)


@pytest.fixture
def harness(
    mongo_client_replica: MongoClient, hlc_collection: tuple[str, str]
) -> HlcCheckpointHarness:
    def store_for(node_key: str) -> HlcCheckpointPort:
        return MongoHlcCheckpointStore(
            client=mongo_client_replica,
            config=MongoHlcCheckpointConfig(collection=hlc_collection, node_key=node_key),
        )

    def gated_writer() -> tuple[HlcCheckpointPort, WriteGate]:
        gate = WriteGate()
        # Its own task gets its own session from the pooled client, so holding this write
        # does not hold the other writer's.
        gated = MongoHlcCheckpointStore(
            client=cast("MongoClient", _GatedWriteClient(inner=mongo_client_replica, gate=gate)),
            config=MongoHlcCheckpointConfig(collection=hlc_collection, node_key="solo"),
        )

        return gated, gate

    return HlcCheckpointHarness(
        store_for=store_for,
        transaction=lambda: MongoTxManagerAdapter(client=mongo_client_replica).transaction(),
        backend="mongo",
        gated_writer=gated_writer,
    )


async def test_a_corrupt_mark_is_refused_rather_than_read_as_no_mark(
    mongo_client_replica: MongoClient, hlc_collection: tuple[str, str]
) -> None:
    """What Mongo's schemalessness costs, and why the answer is to fail loudly.

    Postgres declares ``hlc BIGINT`` and the question cannot arise; here any document can
    land in the collection. Reading a non-integer mark as "no mark" is the dangerous
    reading — the clock would resume at ``(0, 0)`` and re-issue beneath stamps this node
    already relayed, which is the one failure the checkpoint exists to prevent, reached in
    silence. Refusing stops the node instead, which is recoverable.
    """

    db_name, coll_name = hlc_collection
    coll = await mongo_client_replica.collection(coll_name, db_name=db_name)
    await coll.insert_one({"_id": "default", "hlc": "not-a-mark"})

    store = MongoHlcCheckpointStore(
        client=mongo_client_replica,
        config=MongoHlcCheckpointConfig(collection=hlc_collection),
    )

    with pytest.raises(CoreException) as raised:
        await store.load()

    assert raised.value.kind is ExceptionKind.CONFIGURATION


async def test_a_nulled_mark_is_refused_rather_than_read_as_no_mark(
    mongo_client_replica: MongoClient, hlc_collection: tuple[str, str]
) -> None:
    """The corruption that looks exactly like an empty collection.

    A document whose ``hlc`` was overwritten with ``null`` sorts where a document that
    never had one sorts, so "the field is absent" cannot be the test for emptiness — and
    reading it as no mark is the failure this store refuses everywhere else: the clock
    resumes at ``(0, 0)`` and re-issues beneath stamps already relayed. Emptiness is
    "no documents at all"; anything present must carry a real mark.
    """

    db_name, coll_name = hlc_collection
    coll = await mongo_client_replica.collection(coll_name, db_name=db_name)
    await coll.insert_one({"_id": "default", "hlc": None})

    store = MongoHlcCheckpointStore(
        client=mongo_client_replica,
        config=MongoHlcCheckpointConfig(collection=hlc_collection),
    )

    with pytest.raises(CoreException) as raised:
        await store.load()

    assert raised.value.kind is ExceptionKind.CONFIGURATION


# ....................... #


@pytest.mark.conformance(plane="hlc_checkpoint", engine="mongo")
@pytest.mark.parametrize("check", HLC_CHECKPOINT_BATTERY, ids=lambda check: check.__name__)
async def test_hlc_checkpoint_battery(check: Check, harness: HlcCheckpointHarness) -> None:
    await check(harness)
