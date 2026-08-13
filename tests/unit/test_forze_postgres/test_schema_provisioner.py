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
    def __init__(self, *, existing_role: bool = False) -> None:
        self.executed: list[str] = []
        self.existing_role = existing_role

    async def execute(self, query: Any, params: Any = None, **kwargs: Any) -> None:
        _ = params, kwargs
        self.executed.append(query.as_string(None) if hasattr(query, "as_string") else str(query))

    async def fetch_value(self, query: Any, params: Any = None, **kwargs: Any) -> Any:
        _ = query, params, kwargs
        return 1 if self.existing_role else None


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


@pytest.mark.asyncio
async def test_a_role_is_created_and_confined_to_the_tenant_schema() -> None:
    """The three grants that make up the confinement, in one onboarding."""

    tid = uuid4()
    client = _FakeClient()
    provisioner = PostgresSchemaTenantProvisioner(
        client=client,  # type: ignore[arg-type]
        schema=lambda t: f"tenant_{t.hex}",
        role=lambda t: f"reader_{t.hex}",
    )

    await provisioner.provision(TenantIdentity(tenant_id=tid))

    statements = "\n".join(client.executed)
    schema, role = f"tenant_{tid.hex}", f"reader_{tid.hex}"

    assert f'CREATE ROLE "{role}" NOLOGIN' in statements
    assert f'GRANT USAGE ON SCHEMA "{schema}" TO "{role}"' in statements
    assert f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{role}"' in statements
    # The half that keeps the grant true for relations a pipeline creates later.
    assert (
        f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{schema}" GRANT SELECT ON TABLES TO "{role}"'
        in statements
    )
    # Read-only means read-only: no write privilege is handed out anywhere.
    assert "INSERT" not in statements
    assert "ALL PRIVILEGES" not in statements


@pytest.mark.asyncio
async def test_an_existing_role_is_not_recreated() -> None:
    """Roles are cluster-wide, so a second tenant onboarding must not fail on the name."""

    client = _FakeClient(existing_role=True)
    provisioner = PostgresSchemaTenantProvisioner(
        client=client,  # type: ignore[arg-type]
        schema="acme",
        role="shared_reader",
    )

    await provisioner.provision(TenantIdentity(tenant_id=uuid4()))

    statements = "\n".join(client.executed)

    assert "CREATE ROLE" not in statements
    # The grants still run: an existing role may not yet reach this tenant's schema.
    assert 'GRANT USAGE ON SCHEMA "acme" TO "shared_reader"' in statements


@pytest.mark.asyncio
async def test_no_role_is_touched_when_none_is_configured() -> None:
    """The role is opt-in; a schema-only provisioner behaves exactly as before."""

    client = _FakeClient()
    provisioner = PostgresSchemaTenantProvisioner(
        client=client,  # type: ignore[arg-type]
        schema="acme",
    )

    await provisioner.provision(TenantIdentity(tenant_id=uuid4()))

    assert all("ROLE" not in q and "GRANT" not in q for q in client.executed)


@pytest.mark.asyncio
async def test_deprovision_drops_the_role_before_the_schema() -> None:
    """Order matters: a role Postgres still sees privileges for cannot be dropped.

    And after ``DROP SCHEMA … CASCADE`` the default-privileges entry outlives the tables it
    referred to, so the revoke has to happen while the schema is still there.
    """

    client = _FakeClient()
    provisioner = PostgresSchemaTenantProvisioner(
        client=client,  # type: ignore[arg-type]
        schema="acme",
        role="acme_reader",
        drop_on_deprovision=True,
    )

    await provisioner.deprovision(TenantIdentity(tenant_id=uuid4()))

    kinds = [q for q in client.executed]
    drop_role = next(i for i, q in enumerate(kinds) if "DROP ROLE" in q)
    drop_schema = next(i for i, q in enumerate(kinds) if "DROP SCHEMA" in q)
    revoke = next(i for i, q in enumerate(kinds) if "ALTER DEFAULT PRIVILEGES" in q)

    assert revoke < drop_role < drop_schema


@pytest.mark.asyncio
async def test_deprovision_leaves_the_role_alone_without_the_opt_in() -> None:
    """``drop_on_deprovision`` gates the role exactly as it gates the schema."""

    client = _FakeClient()
    provisioner = PostgresSchemaTenantProvisioner(
        client=client,  # type: ignore[arg-type]
        schema="acme",
        role="acme_reader",
    )

    await provisioner.deprovision(TenantIdentity(tenant_id=uuid4()))

    assert client.executed == []


def test_create_schema_identifier_is_quoted() -> None:
    # sanity: the schema name is rendered as a quoted identifier (no injection).
    rendered = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
        sql.Identifier("weird name")
    ).as_string(None)
    assert '"weird name"' in rendered
