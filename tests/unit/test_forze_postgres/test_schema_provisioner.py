"""Unit tests for the Postgres schema tenant provisioner."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest
from psycopg import sql

from forze.application.contracts.tenancy import TenantIdentity
from forze_postgres import PostgresSchemaTenantProvisioner

# ----------------------- #


class _FakeClient:
    def __init__(self) -> None:
        self.executed: list[str] = []

    async def execute(self, query: Any, params: Any = None, **kwargs: Any) -> None:
        _ = params, kwargs
        self.executed.append(query.as_string(None) if hasattr(query, "as_string") else str(query))


@pytest.mark.asyncio
async def test_provision_creates_per_tenant_schema() -> None:
    tid = uuid4()
    client = _FakeClient()
    provisioner = PostgresSchemaTenantProvisioner(
        client=client,  # type: ignore[arg-type]
        schema=lambda t: f"tenant_{str(t).replace('-', '')}",
    )

    await provisioner.provision(TenantIdentity(tenant_id=tid))

    expected = f"tenant_{str(tid).replace('-', '')}"
    assert any("CREATE SCHEMA IF NOT EXISTS" in q and expected in q for q in client.executed)


@pytest.mark.asyncio
async def test_deprovision_is_noop_by_default() -> None:
    client = _FakeClient()
    provisioner = PostgresSchemaTenantProvisioner(
        client=client,  # type: ignore[arg-type]
        schema="static",
    )

    await provisioner.deprovision(TenantIdentity(tenant_id=uuid4()))

    assert client.executed == []  # schemas are not dropped unless opted in


@pytest.mark.asyncio
async def test_deprovision_drops_schema_when_opted_in() -> None:
    client = _FakeClient()
    provisioner = PostgresSchemaTenantProvisioner(
        client=client,  # type: ignore[arg-type]
        schema="acme",
        drop_on_deprovision=True,
    )

    await provisioner.deprovision(TenantIdentity(tenant_id=uuid4()))

    assert any("DROP SCHEMA IF EXISTS" in q and "CASCADE" in q for q in client.executed)


def test_create_schema_identifier_is_quoted() -> None:
    # sanity: the schema name is rendered as a quoted identifier (no injection).
    rendered = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
        sql.Identifier("weird name")
    ).as_string(None)
    assert '"weird name"' in rendered
