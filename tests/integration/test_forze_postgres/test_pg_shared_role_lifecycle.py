"""What a role shared across tenant schemas does to onboarding and teardown, measured.

The provisioner's own contract says the role is "confined to" the tenant's schema. Resolve a
*static* name and it isn't: every onboarding grants the same role another schema, and the
first offboarding tries to drop a role two tenants still depend on.

Both halves are asserted against a real server rather than reasoned about, because both are
claims about PostgreSQL's dependency bookkeeping — which relations count as a dependency, and
whether ``DROP ROLE`` refuses or cascades — and that is exactly the kind of claim that reads
as obvious and turns out to be wrong.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from uuid import uuid4

import pytest
import pytest_asyncio
from psycopg import sql

from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException, ExceptionKind
from forze_postgres.adapters.tenant_provisioner import PostgresSchemaTenantProvisioner
from forze_postgres.kernel.client import PostgresClient

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest_asyncio.fixture
async def tag() -> AsyncIterator[str]:
    """A per-test suffix, plus teardown that cannot itself depend on the code under test."""

    value = uuid4().hex[:8]
    yield value


async def _cleanup(client: PostgresClient, *, schemas: list[str], role: str) -> None:
    """Tear down whatever survived, in the order PostgreSQL insists on.

    Neither `ALTER DEFAULT PRIVILEGES` nor `REVOKE` has an `IF EXISTS`, and this runs after
    tests that leave the world in several different states — including ones where the
    provisioner already dropped the role. So both objects are probed first; a cleanup that
    raised would mask the failure it was cleaning up after.
    """

    if not await _role_exists(client, role):
        for schema in schemas:
            await client.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema))
            )

        return

    for schema in schemas:
        # Default-privileges entries survive the tables but not the schema, so they are
        # revoked while the schema is still there — the exact bookkeeping this file is about.
        if await _schema_exists(client, schema):
            await client.execute(
                sql.SQL(
                    "ALTER DEFAULT PRIVILEGES IN SCHEMA {} REVOKE SELECT ON TABLES FROM {}"
                ).format(sql.Identifier(schema), sql.Identifier(role))
            )
            await client.execute(
                sql.SQL("REVOKE ALL ON SCHEMA {} FROM {}").format(
                    sql.Identifier(schema), sql.Identifier(role)
                )
            )
            await client.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema))
            )

    await client.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role)))


# ....................... #


async def test_a_per_tenant_schema_with_a_shared_role_is_refused(
    pg_client: PostgresClient,
    tag: str,
) -> None:
    """The combination is refused where it is created, not only where it is wired.

    A route's wiring guard catches this for the dynamic-read plane. The provisioner is a
    standalone component with its own lifecycle, usable with no route at all, so the same
    incoherence has to be refused here too: many schemas, one role, and a class whose own
    docstring promises the role is confined to *the* tenant's schema.
    """

    with pytest.raises(CoreException) as ei:
        PostgresSchemaTenantProvisioner(
            client=pg_client,
            schema=lambda tenant_id: f"t_{tenant_id.hex[:8]}_{tag}",
            role=f"shared_reader_{tag}",
        )

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert ei.value.code == "tenant_role_shared_across_schemas"


async def test_a_per_tenant_role_alongside_per_tenant_schemas_onboards_and_tears_down(
    pg_client: PostgresClient,
    tag: str,
) -> None:
    """The shape that replaces it, end to end on a real server.

    Two tenants, each with its own schema *and* its own role. The second teardown is the one
    that matters: under a shared role it is the first teardown that fails, so a test that
    offboarded only one tenant would pass either way.
    """

    provisioner = PostgresSchemaTenantProvisioner(
        client=pg_client,
        schema=lambda tenant_id: f"t_{tenant_id.hex[:8]}_{tag}",
        role=lambda tenant_id: f"r_{tenant_id.hex[:8]}_{tag}",
        drop_on_deprovision=True,
    )

    first, second = TenantIdentity(tenant_id=uuid4()), TenantIdentity(tenant_id=uuid4())
    schemas = [f"t_{t.tenant_id.hex[:8]}_{tag}" for t in (first, second)]
    roles = [f"r_{t.tenant_id.hex[:8]}_{tag}" for t in (first, second)]

    try:
        await provisioner.provision(first)
        await provisioner.provision(second)

        await provisioner.deprovision(first)

        # The first tenant is gone and the second is untouched — the property a shared role
        # cannot provide, since dropping it would take the survivor's grants with it.
        assert await _schema_exists(pg_client, schemas[0]) is False
        assert await _schema_exists(pg_client, schemas[1]) is True
        assert await _role_exists(pg_client, roles[0]) is False
        assert await _role_exists(pg_client, roles[1]) is True

        await provisioner.deprovision(second)

        assert await _schema_exists(pg_client, schemas[1]) is False
        assert await _role_exists(pg_client, roles[1]) is False

    finally:
        for schema, role in zip(schemas, roles, strict=True):
            await _cleanup(pg_client, schemas=[schema], role=role)


async def test_postgres_really_does_refuse_to_drop_a_depended_on_role(
    pg_client: PostgresClient,
    tag: str,
) -> None:
    """The server behaviour the refusal above is justified by.

    Without this, "a shared role breaks teardown" is a plausible story about PostgreSQL rather
    than a fact, and the guard could be defending against nothing. It is asserted directly —
    grants placed by hand, no provisioner — so it keeps reporting on the server even if the
    provisioner stops producing this shape.
    """

    role = f"depended_{tag}"
    schemas = [f"dep_a_{tag}", f"dep_b_{tag}"]

    await pg_client.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))

    try:
        for schema in schemas:
            await pg_client.execute(
                sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema))
            )
            await pg_client.execute(
                sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                    sql.Identifier(schema), sql.Identifier(role)
                )
            )

        # Drop only the first schema, as a single-tenant offboarding would.
        await pg_client.execute(
            sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schemas[0]))
        )

        # The second schema's grant still names the role, so the drop is refused — and it is
        # `DROP ROLE IF EXISTS`, whose `IF EXISTS` covers absence and not dependency.
        with pytest.raises(Exception) as ei:
            await pg_client.execute(
                sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role))
            )

        # The server's message rides in ``details``; the summary is the sanitized one that
        # egresses, so asserting on ``str()`` would pass for any Postgres error at all.
        detail = " ".join(str(v) for v in (ei.value.details or {}).values()).lower()
        assert "depend" in detail, detail

    finally:
        await _cleanup(pg_client, schemas=schemas, role=role)


# ....................... #


async def _schema_exists(client: PostgresClient, schema: str) -> bool:
    return (
        await client.fetch_value(
            "SELECT 1 FROM pg_namespace WHERE nspname = %(s)s", {"s": schema}
        )
    ) is not None


async def _role_exists(client: PostgresClient, role: str) -> bool:
    return (
        await client.fetch_value("SELECT 1 FROM pg_roles WHERE rolname = %(r)s", {"r": role})
    ) is not None
