"""Integration tests for the Postgres durable-run store.

# covers: DurableRunStorePort.enqueue
# covers: DurableRunStorePort.begin
# covers: DurableRunStorePort.claim_abandoned
# covers: DurableRunStorePort.complete
# covers: DurableRunStorePort.fail
# covers: DurableRunStorePort.load
# covers: DurableRunStorePort.renew
"""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from psycopg import sql

from forze.application.contracts.crypto import (
    AesGcmAead,
    KeyRef,
    StaticKeyDirectory,
    is_encrypted_payload,
)
from forze.application.contracts.durable.function import DurableRunStatus
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.crypto import Keyring
from forze.base.primitives import utcnow
from forze_mock import MockKeyManagement
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
                available_at    TIMESTAMPTZ,
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


def _tenant_store(
    pg_client: PostgresClient,
    table: str,
    tenant: UUID,
    *,
    cipher: Keyring | None = None,
) -> PostgresDurableRunStore:
    """A store bound to *tenant* over a shared tagged table (optionally sealing payloads)."""
    return PostgresDurableRunStore(
        client=pg_client,
        config=PostgresDurableRunConfig(relation=("public", table)),
        cipher=cipher,
        tenant_provider=lambda: TenantIdentity(tenant_id=tenant),
    )


def _keyring() -> Keyring:
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
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

    async def test_stale_worker_is_fenced_out_after_reclaim(
        self, pg_client: PostgresClient, durable_run_table: str
    ) -> None:
        store = _store(pg_client, durable_run_table)
        record = await store.enqueue("fn", input_json=None)

        worker_a = await store.begin(record.run_id, lease_for=timedelta(minutes=5))
        assert worker_a is not None and worker_a.attempts == 1

        await _expire_lease(pg_client, durable_run_table, record.run_id)
        reclaimed = await store.claim_abandoned(limit=10, lease_for=timedelta(minutes=5))
        worker_b = next(c for c in reclaimed if c.run_id == record.run_id)
        assert worker_b.attempts == 2

        # Worker A's stale fence must not finish the run out from under worker B.
        await store.complete(
            record.run_id, output_json={"by": "A"}, fence=worker_a.attempts
        )
        loaded = await store.load(record.run_id)
        assert loaded is not None
        assert loaded.status is DurableRunStatus.RUNNING
        assert loaded.output_json is None

        await store.complete(
            record.run_id, output_json={"by": "B"}, fence=worker_b.attempts
        )
        loaded = await store.load(record.run_id)
        assert loaded is not None
        assert loaded.status is DurableRunStatus.COMPLETED
        assert loaded.output_json == {"by": "B"}

    async def test_renew_extends_lease_and_blocks_reclaim_while_held(
        self, pg_client: PostgresClient, durable_run_table: str
    ) -> None:
        store = _store(pg_client, durable_run_table)
        record = await store.enqueue("fn", input_json=None)

        claimed = await store.begin(record.run_id, lease_for=timedelta(minutes=5))
        assert claimed is not None and claimed.attempts == 1

        # Simulate a body that outran its lease, then heartbeats to renew it.
        await _expire_lease(pg_client, durable_run_table, record.run_id)
        held = await store.renew(
            record.run_id, lease_for=timedelta(minutes=5), fence=claimed.attempts
        )
        assert held is True

        # With the lease pushed forward, the recovery scan leaves the running run alone.
        claimed_ids = {
            c.run_id
            for c in await store.claim_abandoned(limit=10, lease_for=timedelta(minutes=5))
        }
        assert record.run_id not in claimed_ids

        loaded = await store.load(record.run_id)
        assert loaded is not None
        assert loaded.status is DurableRunStatus.RUNNING
        assert loaded.attempts == 1  # never reclaimed

    async def test_renew_with_stale_fence_reports_lost_lease(
        self, pg_client: PostgresClient, durable_run_table: str
    ) -> None:
        store = _store(pg_client, durable_run_table)
        record = await store.enqueue("fn", input_json=None)

        worker_a = await store.begin(record.run_id, lease_for=timedelta(minutes=5))
        assert worker_a is not None and worker_a.attempts == 1

        # Worker B reclaims the expired lease (attempts -> 2).
        await _expire_lease(pg_client, durable_run_table, record.run_id)
        reclaimed = await store.claim_abandoned(limit=10, lease_for=timedelta(minutes=5))
        worker_b = next(c for c in reclaimed if c.run_id == record.run_id)
        assert worker_b.attempts == 2

        # Worker A's heartbeat can no longer renew (stale fence) and learns to stop.
        assert (
            await store.renew(
                record.run_id, lease_for=timedelta(minutes=5), fence=worker_a.attempts
            )
            is False
        )
        # Worker B, the current holder, still renews.
        assert (
            await store.renew(
                record.run_id, lease_for=timedelta(minutes=5), fence=worker_b.attempts
            )
            is True
        )

    async def test_delayed_run_is_not_claimed_until_due(
        self, pg_client: PostgresClient, durable_run_table: str
    ) -> None:
        store = _store(pg_client, durable_run_table)

        future = await store.enqueue(
            "fn", input_json=None, available_at=utcnow() + timedelta(hours=1)
        )
        due = await store.enqueue(
            "fn", input_json=None, available_at=utcnow() - timedelta(minutes=1)
        )
        immediate = await store.enqueue("fn", input_json=None)

        claimed = {
            c.run_id
            for c in await store.claim_abandoned(limit=10, lease_for=timedelta(minutes=5))
        }

        assert future.run_id not in claimed  # not yet due
        assert due.run_id in claimed
        assert immediate.run_id in claimed

    async def test_idempotency_key_is_scoped_per_tenant(
        self, pg_client: PostgresClient, durable_run_table: str
    ) -> None:
        # Two tenants share one tagged table (global ``UNIQUE (idempotency_key)``) and reuse
        # the same key — they must stay distinct runs, not converge onto one.
        tenant_a, tenant_b = uuid4(), uuid4()
        store_a = _tenant_store(pg_client, durable_run_table, tenant_a)
        store_b = _tenant_store(pg_client, durable_run_table, tenant_b)

        a1 = await store_a.enqueue("fn", input_json={"n": 1}, idempotency_key="k")
        a2 = await store_a.enqueue("fn", input_json={"n": 2}, idempotency_key="k")
        b1 = await store_b.enqueue("fn", input_json={"n": 3}, idempotency_key="k")

        assert a1.run_id == a2.run_id  # same tenant + key converges on one run
        assert b1.run_id != a1.run_id  # a different tenant's same key is its own run
        assert a1.idempotency_key == "k"  # the caller's key, not the tenant-scoped form
        assert b1.idempotency_key == "k"
        assert b1.tenant_id == tenant_b

    async def test_encrypted_tenant_output_round_trips_on_load(
        self, pg_client: PostgresClient, durable_run_table: str
    ) -> None:
        # A tenant-scoped run whose payloads are sealed: the output must decrypt on load
        # (its AAD tenant must match the row's tenant, not ``None``).
        tenant = uuid4()
        store = _tenant_store(pg_client, durable_run_table, tenant, cipher=_keyring())

        record = await store.enqueue("fn", input_json={"secret": "in"})
        await store.begin(record.run_id, lease_for=timedelta(minutes=5))
        await store.complete(record.run_id, output_json={"secret": "out"})

        # Sealed at rest: the raw columns hold an encryption envelope, not the plaintext.
        raw = await pg_client.fetch_one(
            sql.SQL(
                "SELECT input, output FROM {table} WHERE run_id = {rid}"
            ).format(
                table=sql.Identifier("public", durable_run_table),
                rid=sql.Placeholder(),
            ),
            [record.run_id],
            row_factory="tuple",
        )
        assert raw is not None
        raw_input, raw_output = raw
        assert is_encrypted_payload(raw_input)
        assert is_encrypted_payload(raw_output)
        assert raw_input != {"secret": "in"} and raw_output != {"secret": "out"}

        # And they decrypt back on load (AAD tenant matches the row's tenant, not ``None``).
        loaded = await store.load(record.run_id)
        assert loaded is not None
        assert loaded.tenant_id == tenant
        assert loaded.input_json == {"secret": "in"}
        assert loaded.output_json == {"secret": "out"}
