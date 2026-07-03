"""Namespace-tier durable recovery on Postgres: per-tenant schemas are fully isolated.

# covers: DurableRunStorePort.enqueue
# covers: DurableRunStorePort.claim_abandoned
# covers: DurableRunStorePort.load
"""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from psycopg import sql

from forze.application.contracts.tenancy import TenantIdentity
from forze_postgres.adapters.durable import PostgresDurableRunStore
from forze_postgres.execution.deps.configs import PostgresDurableRunConfig
from forze_postgres.kernel.client import PostgresClient

# ----------------------- #


@pytest.fixture
async def namespaced_tenants(pg_client: PostgresClient) -> tuple[str, UUID, UUID]:
    """Create a per-tenant ``durable_run`` table in each of two tenant schemas."""

    table = f"durable_run_{uuid4().hex[:8]}"
    tenant_a, tenant_b = uuid4(), uuid4()

    for tenant in (tenant_a, tenant_b):
        schema = f"tnt_{tenant.hex[:8]}"
        await pg_client.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        await pg_client.execute(
            sql.SQL(
                """
                CREATE TABLE {table} (
                    run_id text NOT NULL, name text NOT NULL, status text NOT NULL,
                    idempotency_key text, input jsonb, output jsonb, error text,
                    tenant_id uuid, attempts integer NOT NULL DEFAULT 0,
                    leased_until timestamptz, available_at timestamptz,
                    created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
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
