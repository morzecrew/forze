"""The DuckDB dynamic-read adapter against the shared governance battery (mock ≡ DuckDB).

The battery pins the shell — caps, clamping, fail-closed tenancy, the tenant-parameter merge —
so it runs here on a real in-process engine, one ``:memory:`` instance per tenant, each with a
``lake.gold.items`` table: the shape a routed lake client gives every tenant, without the
catalog. The engine refusals and the locked session run against Lakekeeper in
``tests/integration/test_forze_duckdb/test_lake_routed_reads.py``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any, cast
from uuid import UUID

import pytest

from forze.application.contracts.dynamic_read import DynamicReadPort, DynamicReadSpec
from forze.application.contracts.tenancy import TenantIdentity, TenantProviderPort
from forze_duckdb import DuckDbClient, DuckDbClientPort, DuckDbDynamicReadConfig
from forze_duckdb.adapters import DuckDbDynamicReadAdapter
from tests.support.dynamic_read_conformance import (
    DYNAMIC_READ_BATTERY,
    Check,
    DynamicReadHarness,
)

ROWS_STATEMENT = "SELECT n FROM items ORDER BY n"
TENANT_STATEMENT = "SELECT $tenant AS t"


class _PerTenant:
    """Sends each statement to the bound tenant's own instance, as the routed client does."""

    def __init__(self, provider: TenantProviderPort | None, instances: dict[Any, Any]) -> None:
        self.provider = provider
        self.instances = instances

    async def run_dynamic(self, *args: Any, **kwargs: Any) -> Any:
        bound = self.provider() if self.provider is not None else None
        key = bound.tenant_id if isinstance(bound, TenantIdentity) else bound
        return await self.instances[key].run_dynamic(*args, **kwargs)


@pytest.fixture
async def harness() -> AsyncIterator[DynamicReadHarness]:
    instances: dict[UUID | None, DuckDbClient] = {}

    async def seed(tenant: UUID | None, count: int) -> None:
        if tenant not in instances:
            client = DuckDbClient()
            await client.initialize(
                ":memory:",
                extensions=(),
                bootstrap_sql=("ATTACH ':memory:' AS lake", "CREATE SCHEMA lake.gold"),
            )
            instances[tenant] = client

        await instances[tenant].run_command(
            f"CREATE OR REPLACE TABLE lake.gold.items AS SELECT range AS n FROM range({count})"
        )

    def build(
        spec: DynamicReadSpec,
        tenant_provider: TenantProviderPort | None,
        tenant_aware: bool,
    ) -> DynamicReadPort:
        config = DuckDbDynamicReadConfig(
            provenance="trusted",
            namespace="gold",
            statement_timeout=timedelta(seconds=5),
            tenant_aware=tenant_aware,
        )
        return DuckDbDynamicReadAdapter(
            client=cast(DuckDbClientPort, _PerTenant(tenant_provider, instances)),
            spec=spec,
            config=config,
            statement_timeout=config.statement_timeout,
            tenant_aware=tenant_aware,
            tenant_provider=tenant_provider,
        )

    yield DynamicReadHarness(
        backend="duckdb",
        build=build,
        seed=seed,
        rows_statement=ROWS_STATEMENT,
        tenant_statement=TENANT_STATEMENT,
    )

    for client in instances.values():
        await client.close()


@pytest.mark.conformance(plane="dynamic_read", engine="duckdb")
@pytest.mark.parametrize("check", DYNAMIC_READ_BATTERY, ids=lambda check: check.__name__)
async def test_dynamic_read_battery(check: Check, harness: DynamicReadHarness) -> None:
    await check(harness)
