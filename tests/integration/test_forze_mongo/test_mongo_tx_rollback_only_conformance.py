"""Mongo ``rollback_only()`` against the shared conformance battery.

Mongo has no savepoints: a nested scope shares the root's transaction, so a nested
``rollback_only()`` is refused rather than ignored. Run on a lazy and an eager client, since
each opens its root on its own path. Needs a replica set, as every Mongo transaction does.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from uuid import uuid4

import pytest

from forze_mongo.execution.deps import MongoDepsModule
from forze_mongo.kernel.client import MongoClient, MongoConfig
from tests.support.execution_context import context_from_deps
from tests.support.tx_rollback_only_conformance import (
    TX_ROLLBACK_ONLY_BATTERY,
    Check,
    RollbackOnlyHarness,
)

# ----------------------- #

_ROUTE = "mongo"


@pytest.fixture(params=["lazy", "eager"])
async def client(
    request: pytest.FixtureRequest, mongo_replica_uri: str
) -> AsyncIterator[MongoClient]:
    client = MongoClient()
    await client.initialize(
        mongo_replica_uri,
        db_name=f"forze_test_{uuid4().hex[:8]}",
        config=MongoConfig(lazy_transaction=request.param == "lazy"),
    )

    yield client

    await client.close()


@pytest.fixture
async def harness(client: MongoClient, caplog: pytest.LogCaptureFixture) -> RollbackOnlyHarness:
    coll = await client.collection(f"rollback_only_{uuid4().hex[:8]}")

    async def write(key: str) -> None:
        await client.insert_one(coll, {"k": key})

    async def stored() -> set[str]:
        return {doc["k"] for doc in await client.find_many(coll, {})}

    return RollbackOnlyHarness(
        ctx=context_from_deps(MongoDepsModule(client=client, tx={_ROUTE})()),
        route=_ROUTE,
        write=write,
        stored=stored,
        savepoints=False,
        caplog=caplog,
        backend="mongo",
    )


@pytest.mark.conformance(plane="tx_rollback_only", engine="mongo")
@pytest.mark.parametrize("check", TX_ROLLBACK_ONLY_BATTERY, ids=lambda check: check.__name__)
async def test_tx_rollback_only_battery(check: Check, harness: RollbackOnlyHarness) -> None:
    await check(harness)
