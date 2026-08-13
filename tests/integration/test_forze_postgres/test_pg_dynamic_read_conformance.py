"""The Postgres dynamic-read adapter against the shared governance battery.

The engine-enforced half of this plane — writes refused by the read-only transaction,
multi-command strings refused by the wire protocol, the statement timeout — lives in
``test_pg_dynamic_read_integration``, because the mock cannot answer any of it. What runs here
is the half both engines share, against a real server: the same caps, the same clamping, the
same fail-closed tenancy, over per-tenant schemas that a real ``SET LOCAL search_path`` routes.
"""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from psycopg import sql

from forze.application.contracts.dynamic_read import DynamicReadPort, DynamicReadSpec
from forze.application.contracts.tenancy import TenantProviderPort
from forze_postgres.adapters.dynamic_read import PostgresDynamicReadAdapter
from forze_postgres.execution.deps.configs import PostgresDynamicReadConfig
from forze_postgres.kernel.client import PostgresClient
from tests.support.dynamic_read_conformance import (
    DYNAMIC_READ_BATTERY,
    Check,
    DynamicReadHarness,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

ROWS_STATEMENT = "SELECT n FROM items ORDER BY n"
TENANT_STATEMENT = "SELECT %(tenant)s::uuid AS t"


@pytest_asyncio.fixture
async def harness(pg_client: PostgresClient) -> DynamicReadHarness:
    # One schema family per test run: the battery seeds the same tenants in several checks and
    # a shared prefix across runs would let one test's leftovers answer another's statement.
    prefix = f"dr_{uuid4().hex[:8]}"

    def schema_for(tenant: UUID | None) -> str:
        # The whole hex, not a prefix: two tenant ids sharing their first bytes would share
        # a schema, and the disjointness check would pass while proving nothing.
        return f"{prefix}_shared" if tenant is None else f"{prefix}_{tenant.hex}"

    async def seed(tenant: UUID | None, count: int) -> None:
        schema = schema_for(tenant)

        await pg_client.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))
        )
        # Replace, never append: several checks seed the same tenant twice with different
        # counts, and a leftover row would make the second one pass for the wrong reason.
        await pg_client.execute(
            sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(schema, "items"))
        )
        await pg_client.execute(
            sql.SQL("CREATE TABLE {} (n INTEGER NOT NULL)").format(
                sql.Identifier(schema, "items")
            )
        )

        if count:
            await pg_client.execute(
                sql.SQL("INSERT INTO {} SELECT generate_series(0, {})").format(
                    sql.Identifier(schema, "items"),
                    sql.Literal(count - 1),
                )
            )

    def build(
        spec: DynamicReadSpec,
        tenant_provider: TenantProviderPort | None,
        tenant_aware: bool,
    ) -> DynamicReadPort:
        config = PostgresDynamicReadConfig(
            provenance="trusted",
            query_schema=schema_for,
            statement_timeout=timedelta(seconds=5),
            tenant_aware=tenant_aware,
        )
        return PostgresDynamicReadAdapter(
            client=pg_client,
            spec=spec,
            config=config,
            statement_timeout=config.statement_timeout,
            tenant_aware=tenant_aware,
            tenant_provider=tenant_provider,
        )

    return DynamicReadHarness(
        backend="pg",
        build=build,
        seed=seed,
        rows_statement=ROWS_STATEMENT,
        tenant_statement=TENANT_STATEMENT,
    )


@pytest.mark.conformance(plane="dynamic_read", engine="postgres")
@pytest.mark.parametrize("check", DYNAMIC_READ_BATTERY, ids=lambda check: check.__name__)
async def test_dynamic_read_battery(check: Check, harness: DynamicReadHarness) -> None:
    await check(harness)
