"""Postgres schema tenant provisioner — ensure a tenant's schema exists on onboarding.

The ``namespace``-tier provisioner for Postgres: on ``provision`` it resolves the tenant's
schema name (a per-tenant ``NamedResourceSpec`` resolver) and ``CREATE SCHEMA IF NOT
EXISTS``. The same pattern (resolve per-tenant namespace → create-if-missing) applies to the
other ``namespace``-tier backends — a BigQuery dataset or a ClickHouse database — for which a
``FunctionTenantProvisioner`` wrapping the backend client is usually enough.
"""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import cast

import attrs
from psycopg import sql
from psycopg.abc import QueryNoTemplate

from forze.application.contracts.resolution import NamedResourceSpec, resolve_value
from forze.application.contracts.tenancy import TenantIdentity, TenantProvisionerPort

from ..kernel.client import PostgresClientPort

# ----------------------- #


@attrs.define(slots=True, frozen=True, kw_only=True)
class PostgresSchemaTenantProvisioner(TenantProvisionerPort):
    """Ensure a tenant's Postgres schema exists when the tenant is onboarded.

    Pair this with the per-tenant schema used by the document/analytics routes. Teardown is a
    deliberate no-op unless ``drop_on_deprovision`` is set — ``DROP SCHEMA ... CASCADE``
    destroys the tenant's data, so it is opt-in.
    """

    client: PostgresClientPort
    schema: NamedResourceSpec
    drop_on_deprovision: bool = False

    async def provision(self, tenant: TenantIdentity) -> None:
        name = await resolve_value(self.schema, tenant.tenant_id)
        await self.client.execute(
            cast(
                QueryNoTemplate,
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(name)),
            )
        )

    async def deprovision(self, tenant: TenantIdentity) -> None:
        if not self.drop_on_deprovision:
            return

        name = await resolve_value(self.schema, tenant.tenant_id)
        await self.client.execute(
            cast(
                QueryNoTemplate,
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(name)),
            )
        )
