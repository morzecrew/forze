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
from psycopg import errors, sql
from psycopg.abc import QueryNoTemplate

from forze.application.contracts.resolution import (
    NamedResourceSpec,
    coerce_optional_named_resource_spec,
    resolve_value,
)
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

    role: NamedResourceSpec | None = attrs.field(
        default=None,
        converter=coerce_optional_named_resource_spec,
    )
    """Optional read-only role to create alongside the schema, confined to it.

    The enabler for the dynamic-read plane's untrusted tier: a ``NOLOGIN`` role holding
    ``USAGE`` on this tenant's schema and ``SELECT`` on its relations, which
    ``PostgresDynamicReadConfig.role`` then enters with ``SET LOCAL ROLE``. Provisioning it
    here rather than by hand is what keeps it in step with the schema — a role granted once at
    onboarding and never again would stop covering the tenant the day a pipeline creates a new
    table, which is why ``ALTER DEFAULT PRIVILEGES`` is issued too.

    Two deployment facts the grants cannot cover on their own:

    - **The connection user must be a member of the role**, or ``SET LOCAL ROLE`` is refused at
      read time. A non-superuser that creates the role gets that membership implicitly
      (Postgres 16+); a superuser needs none.
    - ``ALTER DEFAULT PRIVILEGES`` applies to relations created by **the role that ran this
      provisioning**. A pipeline writing as a different user must issue its own default
      privileges, or grant ``SELECT`` as it creates.
    """

    async def provision(self, tenant: TenantIdentity) -> None:
        name = await resolve_value(self.schema, tenant.tenant_id)
        await self.client.execute(
            cast(
                QueryNoTemplate,
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(name)),
            )
        )

        if self.role is None:
            return

        role = await resolve_value(self.role, tenant.tenant_id)
        await self._ensure_role(role)
        await self._grant_read_only(schema=name, role=role)

    async def deprovision(self, tenant: TenantIdentity) -> None:
        if not self.drop_on_deprovision:
            return

        name = await resolve_value(self.schema, tenant.tenant_id)

        if self.role is not None:
            # Before the schema, not after: dropping a role Postgres still sees privileges for
            # fails with "cannot be dropped because some objects depend on it", and after a
            # CASCADE the default-privileges entry survives the tables it referred to.
            role = await resolve_value(self.role, tenant.tenant_id)
            await self._revoke_read_only(schema=name, role=role)
            await self.client.execute(
                cast(
                    QueryNoTemplate,
                    sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role)),
                )
            )

        await self.client.execute(
            cast(
                QueryNoTemplate,
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(name)),
            )
        )

    # ....................... #

    async def _ensure_role(self, role: str) -> None:
        """Create the role if it is missing, tolerating a concurrent creator.

        ``CREATE ROLE`` has no ``IF NOT EXISTS``, and roles are cluster-wide — so two
        onboardings racing on the same name is a real ordering, not a theoretical one. The
        existence probe keeps the common path quiet; catching the duplicate keeps the race from
        failing an onboarding that has nothing wrong with it.
        """

        existing = await self.client.fetch_value(
            "SELECT 1 FROM pg_roles WHERE rolname = %(role)s",
            {"role": role},
        )

        if existing is not None:
            return

        try:
            await self.client.execute(
                cast(
                    QueryNoTemplate,
                    sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)),
                )
            )

        except Exception as error:
            if not _is_duplicate_object(error):
                raise

    # ....................... #

    async def _grant_read_only(self, *, schema: str, role: str) -> None:
        schema_ident = sql.Identifier(schema)
        role_ident = sql.Identifier(role)

        await self.client.execute(
            cast(
                QueryNoTemplate,
                sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(schema_ident, role_ident),
            )
        )
        await self.client.execute(
            cast(
                QueryNoTemplate,
                sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(
                    schema_ident,
                    role_ident,
                ),
            )
        )
        # The half that keeps the confinement true over time: relations a pipeline creates
        # after onboarding are covered without anyone remembering to re-provision.
        await self.client.execute(
            cast(
                QueryNoTemplate,
                sql.SQL(
                    "ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT SELECT ON TABLES TO {}"
                ).format(schema_ident, role_ident),
            )
        )

    # ....................... #

    async def _revoke_read_only(self, *, schema: str, role: str) -> None:
        schema_ident = sql.Identifier(schema)
        role_ident = sql.Identifier(role)

        await self.client.execute(
            cast(
                QueryNoTemplate,
                sql.SQL(
                    "ALTER DEFAULT PRIVILEGES IN SCHEMA {} REVOKE SELECT ON TABLES FROM {}"
                ).format(schema_ident, role_ident),
            )
        )
        await self.client.execute(
            cast(
                QueryNoTemplate,
                sql.SQL("REVOKE ALL ON ALL TABLES IN SCHEMA {} FROM {}").format(
                    schema_ident,
                    role_ident,
                ),
            )
        )
        await self.client.execute(
            cast(
                QueryNoTemplate,
                sql.SQL("REVOKE ALL ON SCHEMA {} FROM {}").format(schema_ident, role_ident),
            )
        )


# ....................... #


def _is_duplicate_object(error: BaseException) -> bool:
    """Whether *error* (or anything in its chain) is Postgres reporting an existing role.

    The client's exception interceptor wraps the psycopg error, so the raw
    :class:`~psycopg.errors.DuplicateObject` sits somewhere in the cause chain rather than at
    the top.
    """

    seen: set[int] = set()
    current: BaseException | None = error

    while current is not None and id(current) not in seen:
        if isinstance(current, errors.DuplicateObject):
            return True

        seen.add(id(current))
        current = current.__cause__ or current.__context__

    return False
