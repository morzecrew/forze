"""Durable tenancy on Postgres, in both tiers — and they prove different things.

**Namespace tier** (a per-tenant schema): isolation comes from *table resolution*. Each
store resolves a different relation, so one tenant's statement cannot reach another's row
whatever its `WHERE` clause says. What these tests prove is that each verb resolves its
tenant's schema at all — not that any tenant predicate works.

**Tagged tier** (one shared relation with a `tenant_id` column): every tenant's rows live in
the same table, so isolation rests *entirely* on the predicate each statement carries. This
is the only tier where a missing `AND tenant_id = …` is observable, and therefore the only
place a test can hold `request_cancel` / `refuse_cancel` to their documented scoping. A
cross-tenant test written against the namespace tier passes with the predicate deleted.

# covers: DurableRunStorePort.enqueue
# covers: DurableRunStorePort.claim_abandoned
# covers: DurableRunStorePort.load
# covers: DurableRunAdminPort.request_cancel
# covers: DurableRunStorePort.refuse_cancel
# covers: DurableScheduleStorePort.put
"""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from psycopg import sql

from forze.application.contracts.durable.function import DurableScheduleRecord
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException
from forze.base.primitives import utcnow
from forze_postgres.adapters.durable import (
    PostgresDurableRunStore,
    PostgresDurableScheduleStore,
)
from forze_postgres.execution.deps.configs import (
    PostgresDurableRunConfig,
    PostgresDurableScheduleConfig,
)
from forze_postgres.kernel.client import PostgresClient

# ----------------------- #


@pytest.fixture
async def namespaced_tenants(pg_client: PostgresClient) -> tuple[str, UUID, UUID]:
    """Create a per-tenant ``durable_run`` table in each of two tenant schemas."""

    table = f"durable_run_{uuid4().hex[:8]}"
    tenant_a, tenant_b = uuid4(), uuid4()

    for tenant in (tenant_a, tenant_b):
        schema = f"tnt_{tenant.hex[:8]}"
        await pg_client.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema}").format(
                schema=sql.Identifier(schema)
            )
        )
        await pg_client.execute(
            sql.SQL(
                """
                CREATE TABLE {table} (
                    run_id text NOT NULL, name text NOT NULL, status text NOT NULL,
                    idempotency_key text, input jsonb, output jsonb, error text,
                    tenant_id uuid, attempts integer NOT NULL DEFAULT 0,
                    leased_until timestamptz, available_at timestamptz,
                    created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
                    cancel_requested_at timestamptz, cancel_refused_at timestamptz,
                    PRIMARY KEY (run_id), UNIQUE (idempotency_key)
                )
                """
            ).format(table=sql.Identifier(schema, table))
        )

    return table, tenant_a, tenant_b


def _store(
    pg_client: PostgresClient, table: str, tenant: UUID
) -> PostgresDurableRunStore:
    def relation(tenant_id: UUID | None) -> tuple[str, str]:
        assert tenant_id is not None
        return (f"tnt_{tenant_id.hex[:8]}", table)

    return PostgresDurableRunStore(
        client=pg_client,
        config=PostgresDurableRunConfig(relation=relation),
        tenant_provider=lambda: TenantIdentity(tenant_id=tenant),
    )


def _unbound_store(pg_client: PostgresClient, table: str) -> PostgresDurableRunStore:
    """A store with *no* binding over the same per-tenant resolver.

    The shape a control plane has: it knows which tenant it is enqueueing for and is bound
    to none of them. The relation it writes into can only come from the tenant it names.
    """

    def relation(tenant_id: UUID | None) -> tuple[str, str]:
        assert tenant_id is not None
        return (f"tnt_{tenant_id.hex[:8]}", table)

    return PostgresDurableRunStore(
        client=pg_client,
        config=PostgresDurableRunConfig(relation=relation),
    )


def _schedule_store(
    pg_client: PostgresClient, table: str, tenant: UUID | None
) -> PostgresDurableScheduleStore:
    def relation(tenant_id: UUID | None) -> tuple[str, str]:
        assert tenant_id is not None
        return (f"tnt_{tenant_id.hex[:8]}", table)

    return PostgresDurableScheduleStore(
        client=pg_client,
        config=PostgresDurableScheduleConfig(relation=relation),
        tenant_provider=(
            None if tenant is None else (lambda: TenantIdentity(tenant_id=tenant))
        ),
    )


@pytest.fixture
async def namespaced_schedule_tenants(pg_client: PostgresClient) -> tuple[str, UUID, UUID]:
    """A per-tenant ``durable_schedule`` table in each of two tenant schemas."""

    table = f"durable_schedule_{uuid4().hex[:8]}"
    tenant_a, tenant_b = uuid4(), uuid4()

    for tenant in (tenant_a, tenant_b):
        schema = f"tnt_{tenant.hex[:8]}"
        await pg_client.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema}").format(schema=sql.Identifier(schema))
        )
        await pg_client.execute(
            sql.SQL(
                """
                CREATE TABLE {table} (
                    schedule_id text NOT NULL, name text NOT NULL, cron text NOT NULL,
                    tz text, input jsonb, next_fire_at timestamptz NOT NULL,
                    enabled boolean NOT NULL DEFAULT true, tenant_id uuid,
                    created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
                    PRIMARY KEY (schedule_id)
                )
                """
            ).format(table=sql.Identifier(schema, table))
        )

    return table, tenant_a, tenant_b


# ....................... #


class TestNamespacePlacement:
    """Where a row lands when the caller names a tenant it is not bound to.

    The tier that can prove it: with a per-tenant resolver the relation is *the* isolation
    mechanism, so a row written into the wrong one is unreachable rather than merely
    unfiltered. On a shared relation both readings put the row in the same table and the
    defect is invisible — which is why the shared battery cannot cover this and this file
    must.
    """

    async def test_a_run_enqueued_for_a_tenant_lands_in_that_tenants_schema(
        self, pg_client: PostgresClient, namespaced_tenants: tuple[str, UUID, UUID]
    ) -> None:
        table, tenant_a, _ = namespaced_tenants

        run = await _unbound_store(pg_client, table).enqueue(
            "for-a", input_json={"t": "a"}, tenant_id=tenant_a
        )

        # A's own store resolves A's schema. Finding the run there is the whole assertion:
        # resolving the relation from the (absent) binding would have raised, and resolving
        # it from anywhere else would leave A's scanner reading an empty table forever.
        reached = await _store(pg_client, table, tenant_a).load(run.run_id)

        assert reached is not None
        assert reached.name == "for-a"
        assert reached.tenant_id == tenant_a

    async def test_a_schedule_put_for_a_tenant_lands_where_that_tenant_looks(
        self, pg_client: PostgresClient, namespaced_schedule_tenants: tuple[str, UUID, UUID]
    ) -> None:
        table, tenant_a, tenant_b = namespaced_schedule_tenants

        await _schedule_store(pg_client, table, None).put(
            DurableScheduleRecord(
                schedule_id="nightly",
                name="fn",
                cron="0 3 * * *",
                next_fire_at=utcnow() - timedelta(minutes=1),
                tenant_id=tenant_a,
            )
        )

        loaded = await _schedule_store(pg_client, table, tenant_a).load("nightly")

        assert loaded is not None
        assert loaded.name == "fn"
        # And it is due for the tenant it was registered for, which is the point of putting
        # it there — a schedule in the wrong relation never fires and nothing reports it.
        due = await _schedule_store(pg_client, table, tenant_a).claim_due(now=utcnow(), limit=10)
        assert [record.schedule_id for record in due] == ["nightly"]
        assert await _schedule_store(pg_client, table, tenant_b).load("nightly") is None

    async def test_a_contradicted_tenant_is_refused_rather_than_half_applied(
        self, pg_client: PostgresClient, namespaced_tenants: tuple[str, UUID, UUID]
    ) -> None:
        table, tenant_a, tenant_b = namespaced_tenants

        with pytest.raises(CoreException) as raised:
            await _store(pg_client, table, tenant_a).enqueue(
                "cross", input_json=None, tenant_id=tenant_b
            )

        assert raised.value.code == "tenant_mismatch"

    async def test_a_tenant_aware_store_refuses_the_unbound_call_first(
        self, pg_client: PostgresClient, namespaced_tenants: tuple[str, UUID, UUID]
    ) -> None:
        """Fail-closed outranks an explicitly named tenant.

        A ``tenant_aware`` store reads its binding before it considers what the caller
        passed, so naming a tenant does not stand in for being bound to one — the refusal is
        the missing binding, not a mismatch.
        """

        table, tenant_a, _ = namespaced_tenants

        def relation(tenant_id: UUID | None) -> tuple[str, str]:
            assert tenant_id is not None
            return (f"tnt_{tenant_id.hex[:8]}", table)

        # ``tenant_aware`` on the adapter, which is where the fail-closed read lives; the
        # config field of the same name is what the factory copies into it.
        store = PostgresDurableRunStore(
            client=pg_client,
            config=PostgresDurableRunConfig(relation=relation),
            tenant_aware=True,
            tenant_provider=lambda: None,
        )

        with pytest.raises(CoreException) as raised:
            await store.enqueue("named", input_json=None, tenant_id=tenant_a)

        assert raised.value.code == "tenant_required"


# ....................... #


class TestNamespaceTierDurableRecovery:
    async def test_recovery_is_isolated_per_tenant_schema(
        self, pg_client: PostgresClient, namespaced_tenants: tuple[str, UUID, UUID]
    ) -> None:
        table, tenant_a, tenant_b = namespaced_tenants
        store_a = _store(pg_client, table, tenant_a)
        store_b = _store(pg_client, table, tenant_b)

        # Each store resolves its own tenant's schema; enqueue lands in separate tables.
        run_a = await store_a.enqueue("fn", input_json={"t": "a"})
        run_b = await store_b.enqueue("fn", input_json={"t": "b"})
        assert run_a.tenant_id == tenant_a
        assert run_b.tenant_id == tenant_b

        # Tenant A's scanner claims only tenant A's runs (from A's schema).
        claimed_a = {
            c.run_id
            for c in await store_a.claim_abandoned(
                limit=10, lease_for=timedelta(minutes=5)
            )
        }
        assert run_a.run_id in claimed_a
        assert run_b.run_id not in claimed_a

        # Tenant B's scanner claims only tenant B's runs.
        claimed_b = {
            c.run_id
            for c in await store_b.claim_abandoned(
                limit=10, lease_for=timedelta(minutes=5)
            )
        }
        assert run_b.run_id in claimed_b
        assert run_a.run_id not in claimed_b

        # Cross-tenant load misses: A's store cannot see B's run (different schema).
        assert await store_a.load(run_b.run_id) is None
        assert await store_b.load(run_a.run_id) is None

    async def test_run_control_verbs_resolve_their_own_tenants_schema(
        self, pg_client: PostgresClient, namespaced_tenants: tuple[str, UUID, UUID]
    ) -> None:
        # What this tier can prove about the cancel verbs: they resolve *a per-tenant table*
        # rather than a static one. It deliberately does NOT prove the tenant predicate —
        # A's statement runs against A's schema, so B's row is unreachable with or without
        # it. The predicate is held to account on the tagged tier below.
        table, tenant_a, tenant_b = namespaced_tenants
        store_a = _store(pg_client, table, tenant_a)
        store_b = _store(pg_client, table, tenant_b)

        run_b = await store_b.enqueue("fn", input_json={"t": "b"})

        assert await store_a.request_cancel(run_b.run_id) is False
        assert await store_b.request_cancel(run_b.run_id) is True

        stopped = await store_b.load(run_b.run_id)
        assert stopped is not None
        assert stopped.status.value == "cancelled"


@pytest.fixture
async def tagged_table(pg_client: PostgresClient) -> str:
    """One shared ``durable_run`` relation — the tier where the tenant predicate is load-bearing."""

    table = f"durable_run_tagged_{uuid4().hex[:8]}"
    await pg_client.execute(
        sql.SQL(
            """
            CREATE TABLE {table} (
                run_id text NOT NULL, name text NOT NULL, status text NOT NULL,
                idempotency_key text, input jsonb, output jsonb, error text,
                tenant_id uuid, attempts integer NOT NULL DEFAULT 0,
                leased_until timestamptz, available_at timestamptz,
                created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
                cancel_requested_at timestamptz, cancel_refused_at timestamptz,
                PRIMARY KEY (run_id), UNIQUE (idempotency_key)
            )
            """
        ).format(table=sql.Identifier("public", table))
    )
    return table


def _tagged_store(
    pg_client: PostgresClient, table: str, tenant: UUID
) -> PostgresDurableRunStore:
    """A store over the *shared* relation, differing from its sibling only by bound tenant."""

    return PostgresDurableRunStore(
        client=pg_client,
        config=PostgresDurableRunConfig(relation=("public", table)),
        tenant_provider=lambda: TenantIdentity(tenant_id=tenant),
    )


# ....................... #


class TestTaggedTierRunControlIsolation:
    async def test_cancel_cannot_cross_a_tenant_boundary(
        self, pg_client: PostgresClient, tagged_table: str
    ) -> None:
        # ``request_cancel`` is the admin port's only mutating verb, so it must scope exactly
        # like the ``list_runs`` beside it: an operator bound to one tenant must not stop a
        # run it could not have listed. On a shared table only the predicate enforces that.
        tenant_a, tenant_b = uuid4(), uuid4()
        store_a = _tagged_store(pg_client, tagged_table, tenant_a)
        store_b = _tagged_store(pg_client, tagged_table, tenant_b)

        run_b = await store_b.enqueue("fn", input_json={"t": "b"})
        assert run_b.tenant_id == tenant_b

        # A asks to cancel B's run — same table, same row visible to the statement.
        assert await store_a.request_cancel(run_b.run_id) is False

        untouched = await store_b.load(run_b.run_id)
        assert untouched is not None
        assert untouched.status.value == "pending"
        assert untouched.cancel_requested_at is None

        # B, the owner, can stop it.
        assert await store_b.request_cancel(run_b.run_id) is True

        stopped = await store_b.load(run_b.run_id)
        assert stopped is not None
        assert stopped.status.value == "cancelled"

    async def test_a_refusal_cannot_cross_a_tenant_boundary(
        self, pg_client: PostgresClient, tagged_table: str
    ) -> None:
        # ``refuse_cancel`` is the one write on this port with no ``status = 'running'``
        # guard, so it can stamp a run in any state — the widest write here — while
        # ``attempts`` is a small integer that collides freely across tenants. On a shared
        # table the fence alone is thin protection; the tenant predicate carries it.
        tenant_a, tenant_b = uuid4(), uuid4()
        store_a = _tagged_store(pg_client, tagged_table, tenant_a)
        store_b = _tagged_store(pg_client, tagged_table, tenant_b)

        run_b = await store_b.enqueue("fn", input_json={"t": "b"})
        claimed = await store_b.begin(run_b.run_id, lease_for=timedelta(minutes=5))
        assert claimed is not None

        # A refuses against B's run with a fence that legitimately matches.
        await store_a.refuse_cancel(run_b.run_id, fence=claimed.attempts)

        untouched = await store_b.load(run_b.run_id)
        assert untouched is not None
        assert untouched.cancel_refused_at is None

        # B, the owner, records it.
        await store_b.refuse_cancel(run_b.run_id, fence=claimed.attempts)

        stamped = await store_b.load(run_b.run_id)
        assert stamped is not None
        assert stamped.cancel_refused_at is not None
