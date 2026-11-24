"""The Postgres co-located idempotency store against the shared battery.

Its own suite covers what makes this store distinctive — ``commit`` running on the caller's
business transaction, so record and effect commit atomically. This file covers what it has
in common with the other two, which is where its gaps were: the null-key skip and the
unowned-``fail`` promise had no Postgres coverage at all.
"""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID, uuid4

import pytest
import pytest_asyncio

from forze.application.contracts.idempotency import IdempotencySpec
from forze_postgres.adapters.idempotency import PostgresIdempotencyStore
from forze_postgres.adapters.store_schema import idempotency_shape
from forze_postgres.execution.deps.configs import PostgresIdempotencyConfig
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client import PostgresClient
from tests.support.idempotency_conformance import (
    IDEMPOTENCY_BATTERY,
    Check,
    IdempotencyHarness,
)
from tests.support.owned_schema import UNUSED_RELATION, create_owned_table

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest_asyncio.fixture
async def harness(pg_client: PostgresClient) -> IdempotencyHarness:
    table = f"idem_conf_{uuid4().hex[:8]}"
    shape = idempotency_shape(PostgresIdempotencyConfig(relation=UNUSED_RELATION))

    await create_owned_table(pg_client, shape, "public", table)

    def _store(ttl: timedelta, owner: UUID | None) -> PostgresIdempotencyStore:
        return PostgresIdempotencyStore(
            client=pg_client,
            spec=IdempotencySpec(name="idem", ttl=ttl),
            config=PostgresIdempotencyConfig(relation=("public", table)),
            owner_provider=lambda: owner,
            # The migrated table, so the battery runs the owner-aware statements; the
            # un-migrated and no-provider shapes are separate legs in this store's own
            # suite, because each is a shipped configuration that fails differently.
            introspector=PostgresIntrospector(client=pg_client),
        )

    return IdempotencyHarness(
        backend="pg",
        key=lambda: f"battery-{uuid4().hex[:12]}",
        store_for=_store,
    )


@pytest.mark.conformance(plane="idempotency", engine="postgres")
@pytest.mark.parametrize("check", IDEMPOTENCY_BATTERY, ids=lambda check: check.__name__)
async def test_idempotency_battery(check: Check, harness: IdempotencyHarness) -> None:
    await check(harness)
