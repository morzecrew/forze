"""Integration tests for the Postgres durable-schedule store.

# covers: DurableScheduleStorePort.put
# covers: DurableScheduleStorePort.claim_due
# covers: DurableScheduleStorePort.advance
# covers: DurableScheduleStorePort.load
"""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from psycopg import sql

from forze.application.contracts.durable.function import DurableScheduleRecord
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.primitives import utcnow
from forze_postgres.adapters.durable import PostgresDurableScheduleStore
from forze_postgres.execution.deps.configs import PostgresDurableScheduleConfig
from forze_postgres.kernel.client import PostgresClient

# ----------------------- #


@pytest.fixture
async def schedule_table(pg_client: PostgresClient) -> str:
    table = f"durable_schedule_{uuid4().hex[:8]}"
    await pg_client.execute(
        sql.SQL(
            """
            CREATE TABLE {table} (
                schedule_id  TEXT        NOT NULL,
                name         TEXT        NOT NULL,
                cron         TEXT        NOT NULL,
                tz           TEXT,
                input        JSONB,
                next_fire_at TIMESTAMPTZ NOT NULL,
                enabled      BOOLEAN     NOT NULL DEFAULT true,
                tenant_id    UUID,
                created_at   TIMESTAMPTZ NOT NULL,
                updated_at   TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (schedule_id)
            )
            """
        ).format(table=sql.Identifier("public", table))
    )
    return table


def _store(pg_client: PostgresClient, table: str) -> PostgresDurableScheduleStore:
    return PostgresDurableScheduleStore(
        client=pg_client,
        config=PostgresDurableScheduleConfig(relation=("public", table)),
    )


def _tenant_store(
    pg_client: PostgresClient, table: str, tenant: UUID
) -> PostgresDurableScheduleStore:
    return PostgresDurableScheduleStore(
        client=pg_client,
        config=PostgresDurableScheduleConfig(relation=("public", table)),
        tenant_provider=lambda: TenantIdentity(tenant_id=tenant),
    )


def _record(schedule_id: str, *, next_fire_at, enabled: bool = True) -> DurableScheduleRecord:
    return DurableScheduleRecord(
        schedule_id=schedule_id,
        name="fn",
        cron="* * * * *",
        next_fire_at=next_fire_at,
        input_json={"k": 1},
        enabled=enabled,
    )


# ....................... #


class TestPostgresDurableScheduleStore:
    async def test_put_and_load_round_trips(
        self, pg_client: PostgresClient, schedule_table: str
    ) -> None:
        store = _store(pg_client, schedule_table)
        fire = utcnow() + timedelta(minutes=1)

        await store.put(_record("s", next_fire_at=fire))

        loaded = await store.load("s")
        assert loaded is not None
        assert loaded.name == "fn"
        assert loaded.cron == "* * * * *"
        assert loaded.input_json == {"k": 1}
        assert loaded.next_fire_at == fire

    async def test_put_upserts(
        self, pg_client: PostgresClient, schedule_table: str
    ) -> None:
        store = _store(pg_client, schedule_table)
        await store.put(_record("s", next_fire_at=utcnow() + timedelta(minutes=1)))

        new_fire = utcnow() + timedelta(hours=1)
        await store.put(_record("s", next_fire_at=new_fire, enabled=False))

        loaded = await store.load("s")
        assert loaded is not None
        assert loaded.enabled is False
        assert loaded.next_fire_at == new_fire

    async def test_claim_due_returns_due_and_skips_future_and_disabled(
        self, pg_client: PostgresClient, schedule_table: str
    ) -> None:
        store = _store(pg_client, schedule_table)
        now = utcnow()

        await store.put(_record("due", next_fire_at=now - timedelta(seconds=1)))
        await store.put(_record("future", next_fire_at=now + timedelta(hours=1)))
        await store.put(
            _record("off", next_fire_at=now - timedelta(seconds=1), enabled=False)
        )

        claimed = {s.schedule_id for s in await store.claim_due(now=now, limit=10)}

        assert "due" in claimed
        assert "future" not in claimed  # not yet due
        assert "off" not in claimed  # disabled

    async def test_advance_is_compare_and_set(
        self, pg_client: PostgresClient, schedule_table: str
    ) -> None:
        store = _store(pg_client, schedule_table)
        fire = utcnow()
        await store.put(_record("s", next_fire_at=fire))

        nxt = fire + timedelta(minutes=1)

        # A stale `from` (some other value) does not advance.
        assert await store.advance(
            "s", from_fire_at=fire + timedelta(days=99), to_fire_at=nxt
        ) is False
        assert (await store.load("s")).next_fire_at == fire

        # The matching `from` advances exactly once; a replay of it is then a no-op.
        assert await store.advance("s", from_fire_at=fire, to_fire_at=nxt) is True
        assert (await store.load("s")).next_fire_at == nxt
        assert await store.advance("s", from_fire_at=fire, to_fire_at=nxt) is False

    async def test_schedule_id_is_scoped_per_tenant(
        self, pg_client: PostgresClient, schedule_table: str
    ) -> None:
        # Two tenants share one tagged table and register the same schedule id — one must
        # not overwrite the other, and each acts only on its own schedule.
        tenant_a, tenant_b = uuid4(), uuid4()
        store_a = _tenant_store(pg_client, schedule_table, tenant_a)
        store_b = _tenant_store(pg_client, schedule_table, tenant_b)
        now = utcnow()
        fire_a, fire_b = now - timedelta(seconds=1), now - timedelta(seconds=2)

        await store_a.put(
            DurableScheduleRecord(
                schedule_id="s", name="fn_a", cron="* * * * *", next_fire_at=fire_a
            )
        )
        await store_b.put(
            DurableScheduleRecord(
                schedule_id="s", name="fn_b", cron="0 3 * * *", next_fire_at=fire_b
            )
        )

        # Neither put overwrote the other; each loads its own schedule.
        loaded_a, loaded_b = await store_a.load("s"), await store_b.load("s")
        assert loaded_a is not None and loaded_a.name == "fn_a"
        assert loaded_b is not None and loaded_b.name == "fn_b"
        assert loaded_a.schedule_id == "s" and loaded_b.schedule_id == "s"

        # A bound scan claims only its tenant's schedule.
        claimed_a = await store_a.claim_due(now=now, limit=10)
        assert [c.name for c in claimed_a] == ["fn_a"]

        # Advancing A's schedule leaves B's untouched.
        assert await store_a.advance(
            "s", from_fire_at=fire_a, to_fire_at=now + timedelta(minutes=1)
        )
        assert (await store_b.load("s")).next_fire_at == fire_b  # type: ignore[union-attr]
