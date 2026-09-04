"""The Postgres co-located idempotency store against the shared battery.

Its own suite covers what makes this store distinctive — ``commit`` running on the caller's
business transaction, so record and effect commit atomically. This file covers what it has
in common with the other two, which is where its gaps were: the null-key skip and the
unowned-``fail`` promise had no Postgres coverage at all.
"""

from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import pytest
import pytest_asyncio
from psycopg import sql

from forze.application.contracts.idempotency import IdempotencySpec
from forze_postgres.adapters.idempotency import PostgresIdempotencyStore
from forze_postgres.execution.deps.configs import PostgresIdempotencyConfig
from forze_postgres.kernel.client import PostgresClient
from tests.support.idempotency_conformance import (
    IDEMPOTENCY_BATTERY,
    Check,
    IdempotencyHarness,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest_asyncio.fixture
async def harness(pg_client: PostgresClient) -> IdempotencyHarness:
    table = f"idem_conf_{uuid4().hex[:8]}"

    await pg_client.execute(
        sql.SQL(
            """
            CREATE TABLE {table} (
                op           TEXT        NOT NULL,
                idem_key     TEXT        NOT NULL,
                payload_hash TEXT        NOT NULL,
                status       TEXT        NOT NULL,
                result       BYTEA,
                expires_at   TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (op, idem_key)
            )
            """
        ).format(table=sql.Identifier("public", table))
    )

    def _store(ttl: timedelta) -> PostgresIdempotencyStore:
        return PostgresIdempotencyStore(
            client=pg_client,
            spec=IdempotencySpec(name="idem", ttl=ttl),
            config=PostgresIdempotencyConfig(relation=("public", table)),
        )

    return IdempotencyHarness(
        store=_store(timedelta(hours=1)),
        backend="pg",
        key=lambda: f"battery-{uuid4().hex[:12]}",
        store_with_ttl=_store,
    )


@pytest.mark.conformance(plane="idempotency", engine="postgres")
@pytest.mark.parametrize("check", IDEMPOTENCY_BATTERY, ids=lambda check: check.__name__)
async def test_idempotency_battery(check: Check, harness: IdempotencyHarness) -> None:
    await check(harness)
