"""A tenant-aware Postgres inbox keeps tenants apart on one shared table.

The table's identity is ``(inbox_route, message_id)`` with no tenant column, so the tenant
has to ride in the stored key — as the Mongo inbox and the mock already carry it.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from psycopg import sql

from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze_postgres.adapters.inbox import PostgresInboxStore
from forze_postgres.execution.deps.configs import PostgresInboxConfig
from forze_postgres.kernel.client import PostgresClient

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# ----------------------- #


@pytest_asyncio.fixture
async def inbox_table(pg_client: PostgresClient):
    name = f"inbox_tenancy_{uuid4().hex[:8]}"
    await pg_client.execute(
        sql.SQL(
            """
            CREATE TABLE {t} (
                inbox_route  TEXT NOT NULL,
                message_id   TEXT NOT NULL,
                processed_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (inbox_route, message_id)
            )
            """
        ).format(t=sql.Identifier("public", name))
    )

    yield name

    await pg_client.execute(
        sql.SQL("DROP TABLE IF EXISTS {t}").format(t=sql.Identifier("public", name))
    )


def _store(
    client: PostgresClient,
    table: str,
    tenant_id: UUID | None,
) -> PostgresInboxStore:
    aware = tenant_id is not None

    return PostgresInboxStore(
        client=client,
        spec=InboxSpec(name="events"),
        config=PostgresInboxConfig(relation=("public", table), tenant_aware=aware),
        tenant_aware=aware,
        tenant_provider=lambda: (
            TenantIdentity(tenant_id=tenant_id) if tenant_id is not None else None
        ),
    )


# ....................... #


async def test_two_tenants_with_one_message_id_never_meet(
    pg_client: PostgresClient,
    inbox_table: str,
) -> None:
    first = _store(pg_client, inbox_table, uuid4())
    other = _store(pg_client, inbox_table, uuid4())

    assert await first.mark_if_unseen("events", "m1") is True
    assert await other.mark_if_unseen("events", "m1") is True
    assert await first.mark_if_unseen("events", "m1") is False
    assert await other.mark_if_unseen("events", "m1") is False


async def test_a_message_id_shaped_like_a_tagged_one_never_meets_that_tenant(
    pg_client: PostgresClient,
    inbox_table: str,
) -> None:
    tenant_id = uuid4()
    # A row written without a tenant (before the upgrade, or by a store that is not
    # tenant-aware), whose producer chose an id that names the tenant.
    untagged = _store(pg_client, inbox_table, None)
    tenant = _store(pg_client, inbox_table, tenant_id)

    assert await untagged.mark_if_unseen("events", f"t:{tenant_id}:m1") is True
    assert await tenant.mark_if_unseen("events", "m1") is True


async def test_store_that_is_not_tenant_aware_keeps_its_keys(
    pg_client: PostgresClient,
    inbox_table: str,
) -> None:
    store = _store(pg_client, inbox_table, None)

    assert await store.mark_if_unseen("events", "m1") is True

    rows = await pg_client.fetch_all(
        sql.SQL("SELECT inbox_route, message_id FROM {t}").format(
            t=sql.Identifier("public", inbox_table)
        ),
    )

    assert [(row["inbox_route"], row["message_id"]) for row in rows] == [("events", "m1")]
