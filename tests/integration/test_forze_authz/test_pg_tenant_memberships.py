"""A principal's tenants on Postgres: read in one batch, listed as the row-by-row reads did."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from forze.application.contracts.document import DocumentCommandDepKey, DocumentQueryDepKey
from forze.application.execution import Deps
from forze_postgres.execution.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import PostgresClientDepKey, PostgresIntrospectorDepKey
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps
from tests.support.tenant_memberships import (
    TENANCY_SPECS,
    list_both_ways,
    refuses_a_missing_tenant,
    wide_memberships,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _ctx(pg_client: PostgresClient) -> Any:
    suffix = uuid4().hex[:8]
    base = (
        "id uuid PRIMARY KEY, rev integer NOT NULL, created_at timestamptz NOT NULL, "
        "last_update_at timestamptz NOT NULL"
    )
    await pg_client.execute(
        f"""
        CREATE TABLE tenants_{suffix} ({base}, tenant_key text, is_active boolean NOT NULL);
        CREATE TABLE tenant_bindings_{suffix} (
            {base}, principal_id uuid NOT NULL, tenant_id uuid NOT NULL
        );
        """
    )
    tables = (f"tenants_{suffix}", f"tenant_bindings_{suffix}")
    routes = {
        spec.name: ConfigurablePostgresDocument(
            config=PostgresDocumentConfig(
                read=("public", table),
                write=("public", table),
                bookkeeping_strategy="application",
            )
        )
        for spec, table in zip(TENANCY_SPECS, tables, strict=True)
    }

    return context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            }
        ).merge(Deps.routed({DocumentQueryDepKey: routes, DocumentCommandDepKey: routes}))
    )


async def test_a_principal_in_more_tenants_than_one_batch_lists_them_all(
    pg_client: PostgresClient,
) -> None:
    ctx = await _ctx(pg_client)
    principal_id, active = await wide_memberships(ctx)

    listed = await list_both_ways(ctx, principal_id)

    assert sorted(t.tenant_key for t in listed) == sorted([*active, "tenant-34"])


async def test_a_membership_naming_no_tenant_fails_the_listing(pg_client: PostgresClient) -> None:
    assert await refuses_a_missing_tenant(await _ctx(pg_client)) == "core.not_found"
