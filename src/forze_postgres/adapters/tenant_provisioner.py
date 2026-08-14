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
from forze.base.exceptions import exc

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

    - **The connection user must be able to ``SET ROLE`` into it**, and creating the role is
      not enough to get that: since Postgres 16 the creator is granted ``ADMIN OPTION`` but
      ``SET FALSE``. A non-superuser needs an explicit ``GRANT <role> TO <user> WITH SET TRUE``
      (or ``createrole_self_grant = 'set, inherit'`` set before the role is created); a
      superuser needs neither. Without it every read on the route raises
      ``dynamic_read_role_unavailable``.
    - ``ALTER DEFAULT PRIVILEGES`` applies to relations created by **the role that ran this
      provisioning**. A pipeline writing as a different user must issue its own default
      privileges, or grant ``SELECT`` as it creates.

    It must resolve per tenant whenever :attr:`schema` does — see
    :meth:`__attrs_post_init__`.
    """

    def __attrs_post_init__(self) -> None:
        """Refuse a per-tenant schema paired with one shared role.

        "Confined to it" above is the whole claim, and a static name cannot make it: every
        onboarding grants the same role another schema, so it ends up holding ``USAGE`` and
        ``SELECT`` across all of them. Two things then break, and the quieter one is worse.

        Teardown fails loudly: ``deprovision`` drops the role per tenant, and PostgreSQL
        refuses to drop one another schema's grants still name — ``IF EXISTS`` covers absence,
        not dependency — so the ``DROP SCHEMA`` behind it never runs and offboarding leaves
        the tenant's data in place.

        Confinement fails silently: a statement entering the role reaches every other tenant's
        schema by naming it, because ``search_path`` is routing rather than a boundary. The
        role that was meant to stop a cross-schema reference is what permits it.

        A deliberate cross-tenant reader is a different object with a different lifecycle;
        this class provisions per-tenant containers, so it refuses rather than half-serving
        both.
        """

        if callable(self.schema) and self.role is not None and not callable(self.role):
            raise exc.configuration(
                "PostgresSchemaTenantProvisioner resolves a per-tenant schema but a single "
                "shared role. Every onboarding would grant that one role another tenant's "
                "schema, so it confines to none of them — and the first offboarding would "
                "fail to drop a role the remaining tenants still depend on, leaving their "
                "schema behind it undropped. Resolve the role per tenant "
                "(tenant_id -> str) as well.",
                code="tenant_role_shared_across_schemas",
                details={"role": repr(self.role)},
            )

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

            # Both existence probes, because `REVOKE` has no `IF EXISTS` and offboarding is the
            # operation most likely to be re-run: a half-finished teardown, a retried cleanup
            # job, an operator repeating a command. Every other statement in this class is
            # `IF EXISTS`/`IF NOT EXISTS`, and a revoke that raised on a schema already gone
            # would leave that half-finished teardown with no way to complete.
            if await self._schema_exists(name) and await self._role_exists(role):
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

    async def _role_exists(self, role: str) -> bool:
        return (
            await self.client.fetch_value(
                "SELECT 1 FROM pg_roles WHERE rolname = %(role)s",
                {"role": role},
            )
        ) is not None

    # ....................... #

    async def _schema_exists(self, schema: str) -> bool:
        return (
            await self.client.fetch_value(
                "SELECT 1 FROM pg_namespace WHERE nspname = %(schema)s",
                {"schema": schema},
            )
        ) is not None

    # ....................... #

    async def _ensure_role(self, role: str) -> None:
        """Create the role if it is missing, tolerating a concurrent creator.

        ``CREATE ROLE`` has no ``IF NOT EXISTS``, and roles are cluster-wide — so two
        onboardings racing on the same name is a real ordering, not a theoretical one. The
        existence probe keeps the common path quiet; catching the duplicate keeps the race from
        failing an onboarding that has nothing wrong with it.

        Reuse is checked rather than assumed, because the same skip that makes onboarding
        idempotent also adopts a name that already belongs to something else. What is checked
        is only what stays true: ``NOLOGIN`` and not a superuser are attributes of the role,
        while memberships and privileges can be granted the day after onboarding — a check on
        those would read as a boundary and be a snapshot.
        """

        if await self._role_exists(role):
            await self._refuse_privileged_role(role)
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

    async def _refuse_privileged_role(self, role: str) -> None:
        """Refuse to adopt an existing role that can log in or is a superuser.

        Both are disqualifying for the same reason: statements run *as* this role via ``SET
        LOCAL ROLE``, so adopting an application login user or a superuser hands every
        dynamic statement that identity's reach, and then grants it the tenant's schema on
        top. The refusal comes before any grant, so a mistyped name changes nothing.
        """

        privileged = await self.client.fetch_value(
            "SELECT 1 FROM pg_roles WHERE rolname = %(role)s AND (rolcanlogin OR rolsuper)",
            {"role": role},
        )

        if privileged is None:
            return

        raise exc.configuration(
            f"Role {role!r} already exists and can log in or is a superuser, so it cannot be "
            "adopted as a tenant read role. Statements run as this role via SET LOCAL ROLE, "
            "which would give them that identity's reach and then add the tenant's schema to "
            "it. Point `role` at a name this provisioner owns, or create it NOLOGIN and "
            "without superuser first.",
            code="tenant_role_not_confinable",
            details={"role": role},
        )

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
