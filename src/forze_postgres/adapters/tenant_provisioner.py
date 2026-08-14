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

import hashlib
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
from forze.base.exceptions import CoreException, exc

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

    **It must resolve to a distinct name per tenant whenever :attr:`schema` does.** A static
    name is refused at construction (:meth:`__attrs_post_init__`); a resolver is taken on trust
    there, because no tenant ids exist yet and it may be async — so a constant one has the shape
    of per-tenant scoping without the substance. That case is caught at the second onboarding
    instead, where the server can be asked
    (:meth:`_refuse_role_bound_elsewhere`).
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

        try:
            await self.client.execute(
                cast(
                    QueryNoTemplate,
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(name)),
                )
            )

        except Exception as error:
            # `IF NOT EXISTS` is check-then-act inside the server, so PostgreSQL documents it
            # as racy: two onboardings for one tenant can both pass the check and the loser
            # gets a unique violation on the catalog index. Onboarding is re-run routinely, so
            # that has to be a no-op rather than a failure.
            if not _is_already_exists(error):
                raise

        if self.role is None:
            return

        role = await resolve_value(self.role, tenant.tenant_id)
        await self._ensure_role(role)

        # The binding is check-then-act too, and unlike the creates above there is no server
        # constraint behind it: two onboardings resolving one role can both read "unbound" and
        # both grant, leaving that role holding read access to two tenants. The lock is keyed
        # on the role because the role is what must not be shared, and transaction-scoped so it
        # cannot outlive the connection's return to the pool.
        async with self.client.transaction():
            await self._lock_role(role)
            await self._refuse_role_bound_elsewhere(schema=name, role=role)
            await self._grant_read_only(schema=name, role=role)

    async def deprovision(self, tenant: TenantIdentity) -> None:
        if not self.drop_on_deprovision:
            return

        name = await resolve_value(self.schema, tenant.tenant_id)

        role = await resolve_value(self.role, tenant.tenant_id) if self.role is not None else None

        # One transaction, so the lock `provision` binds under is held continuously from
        # before the revoke to after the role drop. Splitting it released the lock in between,
        # and a provisioning that took it in that gap could recreate the schema and rebind the
        # role — which this teardown would then drop, leaving a tenant whose schema exists and
        # whose reads have no role to enter. Both calls would have returned successfully.
        #
        # What the lock still does not do is make contradictory operations succeed: a provision
        # and a deprovision of the same tenant at the same moment resolve to one of them
        # failing, which is the honest outcome. It stops them leaving a half-torn-down tenant.
        stranded: CoreException | None = None

        async with self.client.transaction():
            if role is not None:
                await self._lock_role(role)

                # The revoke goes before the schema drop, because after a CASCADE the
                # default-privileges entry has no schema left to name in `ALTER DEFAULT
                # PRIVILEGES`.
                #
                # Both existence probes, because `REVOKE` has no `IF EXISTS` and offboarding is
                # the operation most likely to be re-run: a half-finished teardown, a retried
                # cleanup job, an operator repeating a command. Every other statement in this
                # class is `IF EXISTS`/`IF NOT EXISTS`, and a revoke that raised on a schema
                # already gone would leave that half-finished teardown with no way to complete.
                if await self._schema_exists(name) and await self._role_exists(role):
                    await self._revoke_read_only(schema=name, role=role)

            # The schema goes before the role, and the ordering is the point rather than a
            # preference. Roles are cluster-wide but grants are per-database, so this connection
            # cannot revoke a privilege the role holds in another database of the cluster — and
            # `DROP ROLE` refuses while any remains, with `IF EXISTS` covering absence rather
            # than dependency. Dropping the role first therefore let a grant nobody here can see
            # abort the whole teardown *before* the tenant's data was removed.
            await self.client.execute(
                cast(
                    QueryNoTemplate,
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(name)),
                )
            )

            if role is not None:
                stranded = await self._drop_role_in_savepoint(role)

        # Raised after the commit, so the schema drop is durable before the operator hears
        # about the role. A failed statement poisons its transaction, which is why the attempt
        # sits in a savepoint rather than being caught in place.
        if stranded is not None:
            raise stranded

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
        idempotent also adopts a name that already belongs to something else. The check is
        **adoption-time mistake-proofing, not a standing boundary** — every disqualifying
        attribute can be granted after the fact, which is a reason to say so rather than a
        reason to skip it. See :meth:`_refuse_privileged_role` for what disqualifies a role
        and why direct schema grants are deliberately left to the binding check instead.
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
            if not _is_already_exists(error):
                raise

            # Losing the race means adopting a role this provisioner did not create, which is
            # the probe path's situation arriving by a different route — so it gets the probe
            # path's check. Without it the same wiring would be refused or accepted depending
            # on which onboarding ran first.
            await self._refuse_privileged_role(role)

    # ....................... #

    async def _drop_role_in_savepoint(self, role: str) -> CoreException | None:
        """Attempt the role drop without letting its failure undo the teardown.

        A statement that errors aborts its transaction in PostgreSQL, so catching the
        dependency failure in place would leave nothing else in this teardown able to run — and
        the schema drop above it would roll back. A savepoint scopes the damage to the attempt.

        Returns the refusal instead of raising it, so the caller can commit first and report
        after: the tenant's data being gone is the part of offboarding that has to survive.
        """

        try:
            async with self.client.transaction():
                await self._drop_role(role)

        except CoreException as error:
            if error.code != "tenant_role_still_depended_on":
                raise

            return error

        return None

    # ....................... #

    async def _drop_role(self, role: str) -> None:
        """Drop the role, naming the dependency when the cluster still holds one.

        Reached only after the tenant's schema is gone, so a refusal here leaves a role behind
        rather than data. It is still raised: a silently orphaned role accumulates, and the one
        thing an operator needs is which role and why, since the blocking grant is usually in a
        database this connection cannot see.
        """

        try:
            await self.client.execute(
                cast(
                    QueryNoTemplate,
                    sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role)),
                )
            )

        except Exception as error:
            if not _is_dependent_objects(error):
                raise

            raise exc.configuration(
                f"Tenant schema was dropped, but role {role!r} could not be: the cluster still "
                "holds privileges that depend on it. Grants are per-database while roles are "
                "cluster-wide, so the blocker is often in another database this connection "
                "cannot revoke from. Revoke there and drop the role, or give each tenant its "
                "own role so teardown owns everything it created.",
                code="tenant_role_still_depended_on",
                details={"role": role},
            ) from error

    # ....................... #

    async def _lock_role(self, role: str) -> None:
        """Take the transaction-scoped advisory lock guarding one role's binding.

        The key is a stable digest of the role name rather than :func:`hash`, which is salted
        per process and would put two workers on different keys, and rather than the server's
        ``hashtext``, which is undocumented. Only onboardings resolving the *same* role
        contend; everything else proceeds in parallel.
        """

        await self.client.execute(
            "SELECT pg_advisory_xact_lock(%(key)s)",
            {"key": _advisory_key(role)},
        )

    # ....................... #

    async def _refuse_role_bound_elsewhere(self, *, schema: str, role: str) -> None:
        """Refuse a role this provisioner already bound to a different tenant's schema.

        ``__attrs_post_init__`` refuses a *static* role beside a per-tenant schema, but a
        resolver can only be checked for its shape: the tenant ids do not exist at construction
        and the resolver may be async, so ``lambda _: "shared"`` passes and then produces the
        same accumulation one onboarding at a time.

        The collision is a fact about the server rather than about the resolver's source, so it
        is read from the server, at the moment it would begin. The signal is this class's own
        ``ALTER DEFAULT PRIVILEGES`` entry — narrower than "holds USAGE somewhere", which an
        operator granting a shared reference schema would trip on. A different *schema* is the
        collision; the same one is a re-run, which onboarding does routinely.
        """

        bound = await self.client.fetch_value(
            """
            SELECT n.nspname
            FROM pg_default_acl d
            JOIN pg_namespace n ON n.oid = d.defaclnamespace
            WHERE d.defaclobjtype = 'r'
              AND n.nspname <> %(schema)s
              AND EXISTS (
                  SELECT 1 FROM aclexplode(d.defaclacl) a
                  JOIN pg_roles r ON r.oid = a.grantee
                  WHERE r.rolname = %(role)s
              )
            LIMIT 1
            """,
            {"schema": schema, "role": role},
        )

        if bound is None:
            return

        raise exc.configuration(
            f"Role {role!r} is already provisioned for schema {str(bound)!r}, so binding it to "
            f"{schema!r} as well would leave one role holding read access to both. A role "
            "resolver must return a distinct name per tenant — a constant one has the shape of "
            "per-tenant scoping without the substance, and the container is what confines this "
            "plane's statements.",
            code="tenant_role_already_bound",
            details={"role": role, "schema": schema, "already_bound_to": str(bound)},
        )

    # ....................... #

    async def _refuse_privileged_role(self, role: str) -> None:
        """Refuse to adopt an existing role that can log in or is a superuser.

        All four are disqualifying for one reason: statements run *as* this role via ``SET
        LOCAL ROLE``, so adopting a role that already reaches somewhere hands every dynamic
        statement that reach, and then grants it the tenant's schema on top of it. ``LOGIN``
        means it is somebody's application identity; ``SUPERUSER`` ignores every grant;
        ``BYPASSRLS`` is the quietest, reading every row of a table the role was legitimately
        granted while each grant still looks correct; and a membership carries whatever the
        other role holds, which makes the tenant grant the *smallest* thing it can reach.

        The refusal comes before any grant, so a mistyped name changes nothing.

        This is **adoption-time mistake-proofing, not a standing boundary** — every one of
        these can be granted the day after onboarding, ``LOGIN`` included. That is a reason to
        say so plainly rather than a reason to skip the check: it catches the operator who
        points ``role`` at something that was never meant for this, which is the mistake that
        actually happens. A statement that must be contained against a hostile author still
        needs the dedicated tier.

        Deliberately *not* checked: direct grants the role holds on other schemas. Refusing
        those would also refuse the legitimate pattern of a tenant role granted read access to
        a shared reference schema. The cross-tenant case that matters — this role already
        bound to another tenant's schema — is :meth:`_refuse_role_bound_elsewhere`, which uses
        a signal only this class produces and so has no such false positive.
        """

        privileged = await self.client.fetch_value(
            """
            SELECT 1 FROM pg_roles r
            WHERE r.rolname = %(role)s
              AND (
                  r.rolcanlogin
                  OR r.rolsuper
                  OR r.rolbypassrls
                  OR EXISTS (SELECT 1 FROM pg_auth_members m WHERE m.member = r.oid)
              )
            """,
            {"role": role},
        )

        if privileged is None:
            return

        raise exc.configuration(
            f"Role {role!r} already exists and already reaches somewhere — it can log in, is "
            "a superuser, bypasses row-level security, or is a member of another role — so it "
            "cannot be adopted as a tenant read role. Statements run as this role via SET "
            "LOCAL ROLE, which would give them that reach and then add the tenant's schema to "
            "it. Point `role` at a name this provisioner owns, or create it NOLOGIN, "
            "NOSUPERUSER, NOBYPASSRLS and unaffiliated first.",
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


def _advisory_key(role: str) -> int:
    """The advisory-lock key for *role*, stable across processes.

    A digest rather than :func:`hash`, which is salted per interpreter: two workers onboarding
    at the same time would take *different* keys and serialize against nobody, which is the one
    deployment this lock exists for. Salting is invisible inside a single process, so the
    property is pinned by comparing subprocesses.

    Not the server's ``hashtext`` either — it is undocumented and free to change between
    versions, and a key that shifts on upgrade means a rolling deployment where old and new
    nodes do not contend.
    """

    return int.from_bytes(
        hashlib.blake2b(role.encode("utf-8"), digest_size=8).digest(),
        "big",
        signed=True,
    )


# ....................... #


def _caused_by(
    error: BaseException,
    kind: type[BaseException] | tuple[type[BaseException], ...],
) -> bool:
    """Whether *error* or anything in its cause chain is an instance of *kind*.

    The client's exception interceptor wraps the psycopg error, so the raw
    :mod:`psycopg.errors` class sits somewhere in the chain rather than at the top. Walked with
    a seen-set because ``__cause__``/``__context__`` can form a cycle when an error is re-raised
    inside the handling of another.
    """

    seen: set[int] = set()
    current: BaseException | None = error

    while current is not None and id(current) not in seen:
        if isinstance(current, kind):
            return True

        seen.add(id(current))
        current = current.__cause__ or current.__context__

    return False


# ....................... #


def _is_dependent_objects(error: BaseException) -> bool:
    """Whether *error* is Postgres refusing a drop because something still depends on it.

    SQLSTATE ``2BP01``. Matched on the class rather than the message, which names the blocking
    objects and so varies with what they are.
    """

    return _caused_by(error, errors.DependentObjectsStillExist)


# ....................... #


def _is_already_exists(error: BaseException) -> bool:
    """Whether *error* is Postgres reporting that the object is already there.

    Three classes, because which one arrives depends on *when* the collision is noticed. A
    conflict visible while the statement is planned raises ``DuplicateObject`` (42710) or
    ``DuplicateSchema`` (42P06); one that only surfaces as the row is written raises a
    ``UniqueViolation`` (23505) on the catalog index, which is what a live cluster produces
    when two sessions genuinely race.

    Matching only the first two is the shape of a tolerance that passes its test and does
    nothing: a fabricated ``DuplicateObject`` satisfies it, and no real race ever does.
    """

    return _caused_by(
        error, (errors.DuplicateObject, errors.DuplicateSchema, errors.UniqueViolation)
    )
