"""The Mongo co-located idempotency store against the shared battery.

Its own suite covers what makes this store distinctive — ``commit`` riding the caller's
session, and claims taken detached so a concurrent duplicate sees them. This file covers
what it has in common with the other three stores, which is what makes "Mongo implements
the port" a statement rather than a structural-typing claim.
"""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID, uuid4

import pytest
import pytest_asyncio

from forze.application.contracts.idempotency import IdempotencySpec
from forze_mongo.adapters.idempotency import MongoIdempotencyStore
from forze_mongo.execution.deps.configs import MongoIdempotencyConfig
from forze_mongo.kernel.client import MongoClient
from tests.support.idempotency_conformance import (
    IDEMPOTENCY_BATTERY,
    Check,
    IdempotencyHarness,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest_asyncio.fixture
async def harness(mongo_client_replica: MongoClient) -> IdempotencyHarness:
    db_name = (await mongo_client_replica.db()).name

    config = MongoIdempotencyConfig(collection=(db_name, f"idem_conf_{uuid4().hex[:8]}"))

    def _store(ttl: timedelta, owner: UUID | None) -> MongoIdempotencyStore:
        return MongoIdempotencyStore(
            client=mongo_client_replica,
            spec=IdempotencySpec(name="idem", ttl=ttl),
            config=config,
            owner_provider=lambda: owner,
        )

    return IdempotencyHarness(
        backend="mongo",
        key=lambda: f"battery-{uuid4().hex[:12]}",
        store_for=_store,
    )


@pytest.mark.conformance(plane="idempotency", engine="mongo")
@pytest.mark.parametrize("check", IDEMPOTENCY_BATTERY, ids=lambda check: check.__name__)
async def test_idempotency_battery(check: Check, harness: IdempotencyHarness) -> None:
    await check(harness)
