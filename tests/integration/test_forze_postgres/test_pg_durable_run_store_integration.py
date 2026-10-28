"""Integration tests for the Postgres durable-run store.

# covers: DurableRunStorePort.enqueue
# covers: DurableRunStorePort.begin
# covers: DurableRunStorePort.claim_abandoned
# covers: DurableRunStorePort.complete
# covers: DurableRunStorePort.fail
# covers: DurableRunStorePort.load
# covers: DurableRunStorePort.renew
# covers: DurableRunAdminPort.requeue_quarantined
# covers: DurableRunAdminPort.purge_finished
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from psycopg import sql

from forze.application.contracts.crypto import (
    AesGcmAead,
    BytesCipherPort,
    KeyRef,
    StaticKeyDirectory,
    is_encrypted_payload,
)
from forze.application.contracts.durable.function import DurableRunStatus
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.crypto import PAYLOAD_CIPHER_MISSING_CODE, Keyring
from forze.base.primitives import utcnow
from forze_mock import MockKeyManagement
from forze_postgres.adapters.durable import PostgresDurableRunStore
from forze_postgres.execution.deps.configs import (
    PostgresDurableRunConfig,
    PostgresDurableStepConfig,
)
from forze_postgres.kernel.client import PostgresClient
from tests.support.durable_conformance import (
    QUARANTINE_OUTCOME,
    FirstCallRuns,
    run_quarantine_fence_scenario,
    run_quarantine_scenario,
    run_unopenable_claim_scenario,
    run_unreadable_listing_scenario,
    tenant_provider_for,
)
from tests.support.durable_schema import (
    DURABLE_RUN_COLUMNS,
    DURABLE_STEP_COLUMNS,
    create_durable_table,
)

# ----------------------- #


@pytest.fixture
async def durable_run_table(pg_client: PostgresClient) -> str:
    table = f"durable_run_{uuid4().hex[:8]}"
    await create_durable_table(pg_client, DURABLE_RUN_COLUMNS, sql.Identifier("public", table))
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

    async def test_a_run_whose_key_is_gone_costs_only_that_run(
        self, pg_client: PostgresClient, durable_run_table: str
    ) -> None:
        def store_for(tenant: UUID | None, cipher: BytesCipherPort | None) -> PostgresDurableRunStore:
            return PostgresDurableRunStore(
                client=pg_client,
                config=PostgresDurableRunConfig(relation=("public", durable_run_table)),
                cipher=cipher,
                tenant_provider=tenant_provider_for(tenant),
            )

        async def lease_left(run_id: str) -> timedelta:
            row = await pg_client.fetch_one(
                sql.SQL("SELECT leased_until - now() AS left FROM {table} WHERE run_id = %s").format(
                    table=sql.Identifier("public", durable_run_table)
                ),
                [run_id],
            )
            assert row is not None
            return row["left"]

        assert await run_unopenable_claim_scenario(store_for, lease_left) == {
            "claimed": [("kept", {"n": 1}, False)],
            "kept_fence": 1,
            "kept_leased_for_its_lease": True,
            "lost_held_past_its_lease": True,
            "busy_leased_for_its_lease": True,
            "stopped_held_past_its_lease": True,
            "no_cipher": PAYLOAD_CIPHER_MISSING_CODE,
        }

    async def test_a_run_whose_key_is_gone_is_listed_without_its_payloads(
        self, pg_client: PostgresClient, durable_run_table: str
    ) -> None:
        def store_for(tenant: UUID | None, cipher: BytesCipherPort | None) -> PostgresDurableRunStore:
            return PostgresDurableRunStore(
                client=pg_client,
                config=PostgresDurableRunConfig(relation=("public", durable_run_table)),
                cipher=cipher,
                tenant_provider=tenant_provider_for(tenant),
            )

        assert await run_unreadable_listing_scenario(store_for) == {
            "listed": [("kept", {"n": 1}, False), ("lost", None, True)],
            "passing_fault": "test.kms_throttled",
            "no_cipher": PAYLOAD_CIPHER_MISSING_CODE,
        }

    async def test_a_run_recovery_keeps_failing_to_open_is_quarantined_until_requeued(
        self, pg_client: PostgresClient, durable_run_table: str
    ) -> None:
        def store_for(
            tenant: UUID | None, cipher: BytesCipherPort | None
        ) -> PostgresDurableRunStore:
            return PostgresDurableRunStore(
                client=pg_client,
                config=PostgresDurableRunConfig(relation=("public", durable_run_table)),
                cipher=cipher,
                tenant_provider=tenant_provider_for(tenant),
            )

        async def expire(run_id: str) -> None:
            await _expire_lease(pg_client, durable_run_table, run_id)

        async def lease_left(run_id: str) -> timedelta | None:
            row = await pg_client.fetch_one(
                sql.SQL(
                    "SELECT leased_until - now() AS left FROM {table} WHERE run_id = %s"
                ).format(table=sql.Identifier("public", durable_run_table)),
                [run_id],
            )
            assert row is not None
            return row["left"]

        assert await run_quarantine_scenario(store_for, expire, lease_left) == QUARANTINE_OUTCOME

    async def test_a_scan_quarantines_only_a_run_it_still_holds(
        self, pg_client: PostgresClient, durable_run_table: str
    ) -> None:
        def store_for(
            tenant: UUID | None, cipher: BytesCipherPort | None, client: Any = pg_client
        ) -> PostgresDurableRunStore:
            return PostgresDurableRunStore(
                client=client,
                config=PostgresDurableRunConfig(relation=("public", durable_run_table)),
                cipher=cipher,
                tenant_provider=tenant_provider_for(tenant),
            )

        async def expire(run_id: str) -> None:
            await _expire_lease(pg_client, durable_run_table, run_id)

        def racing(
            cipher: BytesCipherPort, hook: Callable[[], Awaitable[None]]
        ) -> PostgresDurableRunStore:
            return store_for(None, cipher, FirstCallRuns(pg_client, "execute", hook))

        assert await run_quarantine_fence_scenario(store_for, racing, expire) == {
            "reclaimed": [3],
            "after": ("running", 3),
        }

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
        renewal = await store.renew(
            record.run_id, lease_for=timedelta(minutes=5), fence=claimed.attempts
        )
        assert renewal.held is True
        assert renewal.cancel_requested is False  # nobody asked it to stop

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
        ).held is False
        # Worker B, the current holder, still renews.
        assert (
            await store.renew(
                record.run_id, lease_for=timedelta(minutes=5), fence=worker_b.attempts
            )
        ).held is True

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

    async def test_an_untagged_run_completed_under_a_binding_still_decrypts(
        self, pg_client: PostgresClient, durable_run_table: str
    ) -> None:
        """The case the stored-tenant read exists for.

        A run tagged with no tenant stays completable from any binding — otherwise its
        terminal write matches nothing and it is reclaimed and re-run forever. Sealing that
        output under the *binding* would then write ciphertext whose AAD names a tenant the
        row does not, and ``load`` — which unseals with the column — could never open it. The
        run would complete and its result would be unreadable.
        """

        keyring = _keyring()
        unbound = PostgresDurableRunStore(
            client=pg_client,
            config=PostgresDurableRunConfig(relation=("public", durable_run_table)),
            cipher=keyring,
        )
        bound = _tenant_store(pg_client, durable_run_table, uuid4(), cipher=keyring)

        run = await unbound.enqueue("orphan", input_json=None)
        held = await bound.begin(run.run_id, lease_for=timedelta(minutes=5))

        assert held is not None

        await bound.complete(run.run_id, output_json={"secret": "out"}, fence=held.attempts)

        loaded = await unbound.load(run.run_id)

        assert loaded is not None
        assert loaded.tenant_id is None
        assert loaded.status.value == "completed"
        assert loaded.output_json == {"secret": "out"}

    async def test_a_purge_skips_a_run_another_transaction_holds_and_keeps_its_steps(
        self, pg_client: PostgresClient, durable_run_table: str
    ) -> None:
        """A failed run being requeued holds its row lock; the purge must leave it whole.

        Deleting it would lose a run its requeue is about to make due, and deleting its
        journal while keeping it would make the requeued run re-run steps it already did.
        """

        step_table = f"durable_step_{uuid4().hex[:8]}"
        await create_durable_table(
            pg_client, DURABLE_STEP_COLUMNS, sql.Identifier("public", step_table)
        )
        store = PostgresDurableRunStore(
            client=pg_client,
            config=PostgresDurableRunConfig(relation=("public", durable_run_table)),
            step_config=PostgresDurableStepConfig(relation=("public", step_table)),
        )
        run = await store.enqueue("fn", input_json=None)
        held = await store.begin(run.run_id, lease_for=timedelta(minutes=5))
        assert held is not None
        await store.fail(run.run_id, error="boom", fence=held.attempts)
        await pg_client.execute(
            sql.SQL(
                "INSERT INTO {steps} (run_id, step_id, result, created_at) "
                "VALUES (%(run_id)s, 's1', '{{}}', now())"
            ).format(steps=sql.Identifier("public", step_table)),
            {"run_id": run.run_id},
        )
        past = timedelta(microseconds=1)

        async def steps_left() -> int:
            row = await pg_client.fetch_one(
                sql.SQL("SELECT count(*) FROM {steps}").format(
                    steps=sql.Identifier("public", step_table)
                ),
                row_factory="tuple",
            )
            assert row is not None
            return int(row[0])

        async with pg_client.transaction():
            await pg_client.execute(
                sql.SQL("SELECT 1 FROM {table} WHERE run_id = %(run_id)s FOR UPDATE").format(
                    table=sql.Identifier("public", durable_run_table)
                ),
                {"run_id": run.run_id},
            )

            async with pg_client.detached():
                skipped = await store.purge_finished(past)
                kept = await store.load(run.run_id)
                kept_steps = await steps_left()

        assert skipped == 0
        assert kept is not None and kept.status is DurableRunStatus.FAILED
        assert kept_steps == 1

        assert await store.purge_finished(past) == 1
        assert await store.load(run.run_id) is None
        assert await steps_left() == 0
