"""Integration tests for the Postgres durable-run store.

# covers: DurableRunStorePort.enqueue
# covers: DurableRunStorePort.begin
# covers: DurableRunStorePort.claim_abandoned
# covers: DurableRunStorePort.complete
# covers: DurableRunStorePort.fail
# covers: DurableRunStorePort.load
"""

from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import pytest
from psycopg import sql

from forze.application.contracts.durable.function import DurableRunStatus
from forze_postgres.adapters.durable import PostgresDurableRunStore
from forze_postgres.execution.deps.configs import PostgresDurableRunConfig
from forze_postgres.kernel.client import PostgresClient

# ----------------------- #


@pytest.fixture
async def durable_run_table(pg_client: PostgresClient) -> str:
    table = f"durable_run_{uuid4().hex[:8]}"
    await pg_client.execute(
        sql.SQL(
            """
            CREATE TABLE {table} (
                run_id          TEXT        NOT NULL,
                name            TEXT        NOT NULL,
                status          TEXT        NOT NULL,
                idempotency_key TEXT,
                input           JSONB,
                output          JSONB,
                error           TEXT,
                tenant_id       UUID,
                attempts        INTEGER     NOT NULL DEFAULT 0,
                leased_until    TIMESTAMPTZ,
                created_at      TIMESTAMPTZ NOT NULL,
                updated_at      TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (run_id),
                UNIQUE (idempotency_key)
            )
            """
        ).format(table=sql.Identifier("public", table))
    )
    return table


def _store(pg_client: PostgresClient, table: str) -> PostgresDurableRunStore:
    return PostgresDurableRunStore(
        client=pg_client,
        config=PostgresDurableRunConfig(relation=("public", table)),
    )


async def _expire_lease(pg_client: PostgresClient, table: str, run_id: str) -> None:
    await pg_client.execute(
        sql.SQL(
            "UPDATE {table} SET leased_until = now() - interval '1 hour' "
            "WHERE run_id = {run_id}"
        ).format(table=sql.Identifier("public", table), run_id=sql.Placeholder()),
        [run_id],
    )


# ....................... #


class TestPostgresDurableRunStore:
    async def test_enqueue_pending_and_load_round_trips_input(
        self, pg_client: PostgresClient, durable_run_table: str
    ) -> None:
        store = _store(pg_client, durable_run_table)

        record = await store.enqueue("fn", input_json={"n": 3})

        assert record.status is DurableRunStatus.PENDING
        loaded = await store.load(record.run_id)
        assert loaded is not None
        assert loaded.name == "fn"
        assert loaded.input_json == {"n": 3}
        assert loaded.attempts == 0

    async def test_idempotency_key_converges_on_one_run(
        self, pg_client: PostgresClient, durable_run_table: str
    ) -> None:
        store = _store(pg_client, durable_run_table)

        first = await store.enqueue("fn", input_json={"n": 1}, idempotency_key="k")
        second = await store.enqueue("fn", input_json={"n": 2}, idempotency_key="k")

        assert first.run_id == second.run_id
        assert second.input_json == {"n": 1}  # the original run, not the re-submit

    async def test_begin_claims_pending_then_refuses_second_claim(
        self, pg_client: PostgresClient, durable_run_table: str
    ) -> None:
        store = _store(pg_client, durable_run_table)
        record = await store.enqueue("fn", input_json=None)

        claimed = await store.begin(record.run_id, lease_for=timedelta(minutes=5))
        assert claimed is not None
        assert claimed.status is DurableRunStatus.RUNNING
        assert claimed.attempts == 1

        # Already RUNNING under a live lease -> not claimable again.
        assert await store.begin(record.run_id, lease_for=timedelta(minutes=5)) is None

    async def test_complete_stores_output_and_blocks_reclaim(
        self, pg_client: PostgresClient, durable_run_table: str
    ) -> None:
        store = _store(pg_client, durable_run_table)
        record = await store.enqueue("fn", input_json=None)
        await store.begin(record.run_id, lease_for=timedelta(minutes=5))

        await store.complete(record.run_id, output_json={"ok": True})

        loaded = await store.load(record.run_id)
        assert loaded is not None
        assert loaded.status is DurableRunStatus.COMPLETED
        assert loaded.output_json == {"ok": True}

        # A completed run is never resurrected by the recovery scan.
        claimed = await store.claim_abandoned(limit=10, lease_for=timedelta(minutes=5))
        assert record.run_id not in {c.run_id for c in claimed}

    async def test_fail_marks_failed(
        self, pg_client: PostgresClient, durable_run_table: str
    ) -> None:
        store = _store(pg_client, durable_run_table)
        record = await store.enqueue("fn", input_json=None)
        await store.begin(record.run_id, lease_for=timedelta(minutes=5))

        await store.fail(record.run_id, error="nope")

        loaded = await store.load(record.run_id)
        assert loaded is not None
        assert loaded.status is DurableRunStatus.FAILED
        assert loaded.error == "nope"

    async def test_claim_abandoned_reclaims_pending_and_expired_running(
        self, pg_client: PostgresClient, durable_run_table: str
    ) -> None:
        store = _store(pg_client, durable_run_table)

        pending = await store.enqueue("fn", input_json=None)
        expired = await store.enqueue("fn", input_json=None)
        await store.begin(expired.run_id, lease_for=timedelta(minutes=5))
        await _expire_lease(pg_client, durable_run_table, expired.run_id)

        live = await store.enqueue("fn", input_json=None)
        await store.begin(live.run_id, lease_for=timedelta(minutes=5))  # live lease

        claimed = await store.claim_abandoned(limit=10, lease_for=timedelta(minutes=5))
        ids = {c.run_id for c in claimed}

        assert pending.run_id in ids  # PENDING is claimable
        assert expired.run_id in ids  # RUNNING with an expired lease is reclaimable
        assert live.run_id not in ids  # RUNNING under a live lease is left alone

        # The reclaimed-once expired run shows two attempts (begin + reclaim).
        reclaimed_expired = next(c for c in claimed if c.run_id == expired.run_id)
        assert reclaimed_expired.attempts == 2
