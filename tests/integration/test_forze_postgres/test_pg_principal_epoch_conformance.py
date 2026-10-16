"""The Postgres principal epochs against the shared battery, on real transactions and locks."""

from __future__ import annotations

from uuid import uuid4

import pytest
from psycopg import sql

from forze_postgres.adapters.principal_epoch import PostgresPrincipalEpochStore
from forze_postgres.adapters.txmanager import PostgresTxManagerAdapter
from forze_postgres.execution.deps.configs import PostgresPrincipalEpochConfig
from forze_postgres.kernel.client import PostgresClient
from tests.support.revocation_states_conformance import (
    EPOCH_BATTERY,
    EpochCheck,
    EpochHarness,
)

# ----------------------- #

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture
async def harness(pg_client: PostgresClient) -> EpochHarness:
    table = f"principal_epochs_{uuid4().hex[:8]}"
    await pg_client.execute(
        sql.SQL(
            "CREATE TABLE {table} (principal_id uuid PRIMARY KEY, epoch bigint NOT NULL)"
        ).format(table=sql.Identifier("public", table))
    )

    return EpochHarness(
        backend="postgres",
        epochs=PostgresPrincipalEpochStore(
            client=pg_client,
            config=PostgresPrincipalEpochConfig(relation=("public", table)),
        ),
        # Each task binds its own pooled connection, so two transactions really contend.
        transaction=lambda: PostgresTxManagerAdapter(client=pg_client).transaction(),
        locks=True,
    )


@pytest.mark.conformance(plane="principal_epoch", engine="postgres")
@pytest.mark.parametrize("check", EPOCH_BATTERY, ids=lambda check: check.__name__)
async def test_principal_epoch_battery(check: EpochCheck, harness: EpochHarness) -> None:
    await check(harness)
