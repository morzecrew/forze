"""Mongo database tenant provisioner — make a tenant's database real at onboarding.

The ``namespace``-tier provisioner for MongoDB, and the counterpart of
:class:`~forze_postgres.adapters.tenant_provisioner.PostgresSchemaTenantProvisioner`. What
differs is not the seam but the server: MongoDB creates a database on its first write, so
there is no ``CREATE SCHEMA IF NOT EXISTS`` to run and nothing that *has* to happen at
onboarding for reads and writes to work.

That autocreation is exactly why this class exists. Three things it buys that the first
request cannot:

- **Onboarding fails where onboarding can be retried.** A connection user that cannot write
  the tenant's database is otherwise discovered by a customer's first request, hours later
  and one request at a time. The marker write below is a real write to the tenant's own
  database, so the permission is proved when the tenant is created.
- **Teardown becomes safe to offer at all.** ``dropDatabase`` destroys everything under a
  name, and a name is all a resolver returns. The marker records which tenant a database was
  provisioned for, so :meth:`deprovision` can refuse to drop one holding somebody else's
  data instead of trusting the resolver that produced the name.
- **A constant resolver is caught.** ``lambda _: "app"`` has the shape of per-tenant naming
  without the substance; it passes every construction-time check and then collects every
  tenant into one database. The second onboarding leaves a second marker there, which is
  what the teardown refuses on.

Deliberately not ported from Postgres: the read-only ``role``. It exists there to be entered
with ``SET LOCAL ROLE`` by the dynamic-read plane, and MongoDB has no in-connection identity
switch to enter one with — the equivalent boundary is a per-tenant *login* user reached by a
routed client, which is the dedicated tier and a different object with a different lifecycle
(it needs somewhere to keep the credential). Indexes are likewise left alone: the document
plane validates them at startup rather than creating them, and provisioning them here would
be a second, quieter policy for the same thing.
"""

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from typing import Final, final

import attrs

from forze.application.contracts.resolution import (
    NamedResourceSpec,
    coerce_named_resource_spec,
)
from forze.application.contracts.tenancy import TenantIdentity, TenantProvisionerPort
from forze.application.contracts.tenancy.routed_client_base import RoutedTenantClientBase
from forze.base.exceptions import exc
from forze.base.primitives import utcnow

from ..kernel.client import MongoClientPort
from ..kernel.relation import resolve_mongo_named_resource

# ----------------------- #

_MARKER_COLLECTION: Final = "_forze_tenants"
"""Default name of the per-database collection holding one marker document per tenant."""

_SYSTEM_DATABASES: Final = frozenset({"admin", "config", "local"})
"""Databases MongoDB owns. Dropping any of them takes the deployment with it."""


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class MongoDatabaseTenantProvisioner(TenantProvisionerPort):
    """Register a tenant in its MongoDB database, and optionally drop that database again.

    Pair this with the per-tenant database used by the document routes. Teardown is a
    deliberate no-op unless :attr:`drop_on_deprovision` is set — ``dropDatabase`` destroys the
    tenant's data, so it is opt-in.
    """

    client: MongoClientPort
    """Admin connection. Must **not** be a routed client; see :meth:`__attrs_post_init__`."""

    database: NamedResourceSpec = attrs.field(converter=coerce_named_resource_spec)
    """The tenant's database — a static name or a ``(tenant_id) -> str`` resolver."""

    drop_on_deprovision: bool = False
    """Whether offboarding runs ``dropDatabase``. Off by default: it destroys the tenant."""

    marker_collection: str = _MARKER_COLLECTION
    """Collection, inside the tenant's own database, holding one document per tenant onboarded
    into it.

    It lives in the tenant's database rather than in a central registry on purpose: a registry
    elsewhere can disagree with the deployment (restored from a backup, edited, pointed at the
    wrong cluster), and the question teardown has to answer — *whose data is under this
    name?* — is only answerable where the data is."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        """Refuse the two wirings whose failure is silent.

        **A routed client.** :class:`~forze_mongo.kernel.client.RoutedMongoClient` resolves
        its connection from the *ambient* tenant, and a provisioner's tenant is by contract
        not the ambient one — an admin onboards tenant X without acting as X. So every call
        would provision in whichever tenant's deployment the admin happened to be bound to,
        or fail outright when bound to none. Neither is recoverable from here: the port takes
        a name, and a routed client takes no tenant.

        **A static database name paired with teardown.** A static name means every tenant
        shares one database, so offboarding the first of them drops the data of all the
        others. There is no version of that which is what someone meant. The name alone is
        allowed — a shared database with per-tenant collections is a real deployment, and
        this then registers tenants into it — but it may not be paired with a drop.
        """

        if isinstance(self.client, RoutedTenantClientBase):
            raise exc.configuration(
                "MongoDatabaseTenantProvisioner was given a routed Mongo client, which "
                "resolves its connection from the ambient tenant. A provisioner is handed "
                "the tenant being onboarded explicitly, and that is generally not the "
                "ambient one, so every onboarding would land in the wrong deployment or in "
                "none. Pass the unrouted admin client for the deployment the tenants live "
                "on.",
                code="tenant_provisioner_routed_client",
            )

        if not callable(self.database) and self.drop_on_deprovision:
            raise exc.configuration(
                f"MongoDatabaseTenantProvisioner resolves the static database "
                f"{self.database!r} for every tenant but is set to drop it on deprovision, "
                "so offboarding one tenant would destroy the data of all of them. Resolve "
                "the database per tenant (tenant_id -> str), or leave drop_on_deprovision "
                "off and tear the shared database down by hand.",
                code="tenant_database_shared_across_tenants",
                details={"database": repr(self.database)},
            )

    # ....................... #

    async def provision(self, tenant: TenantIdentity) -> None:
        """Record *tenant* in its database, creating the database if it is not there yet."""

        name = await self._database_for(tenant)
        coll = await self.client.collection(self.marker_collection, db_name=name)

        # No duplicate-key tolerance, where the Postgres provisioner needs one: an upsert is
        # check-then-act, so concurrent onboardings of a tenant all find nothing and all
        # insert — but the predicate here *is* the unique index, and mongod retries that
        # collision itself instead of surfacing it. Probed rather than assumed, and the
        # concurrency test is the standing check that a server still converges them.
        await self.client.update_one_upsert(
            coll,
            {"_id": _marker_id(tenant)},
            # Insert-only: a re-provision must not restamp `provisioned_at`, which is the one
            # field an operator reads to answer when a tenant was onboarded.
            {"$setOnInsert": {"tenant_id": str(tenant.tenant_id), "provisioned_at": utcnow()}},
        )

    # ....................... #

    async def deprovision(self, tenant: TenantIdentity) -> None:
        """Drop *tenant*'s database, if that was asked for and the database is only theirs.

        Do not call this inside a transaction expecting it to roll back with one. MongoDB does
        not admit ``dropDatabase`` into a transaction, so the drop commits on its own while the
        ownership read above joins the ambient session — an offboarding that aborted afterwards
        would have kept the tenant record and destroyed the data under it. The shipped
        :class:`~forze_identity.tenancy.adapters.management.TenantManagementAdapter` calls it
        outside one.
        """

        if not self.drop_on_deprovision:
            return

        name = await self._database_for(tenant)
        await self._refuse_database_shared_with_others(tenant, database=name)

        database = await self.client.db(name)
        await database.command("dropDatabase")

    # ....................... #

    async def _database_for(self, tenant: TenantIdentity) -> str:
        """Resolve the tenant's database name and refuse the ones MongoDB owns.

        A resolver returns a string, and ``"admin"`` is a string. The check sits on the read
        path of both operations rather than only before the drop, because provisioning into a
        system database writes the marker that later authorizes dropping it.
        """

        name = await resolve_mongo_named_resource(self.database, tenant.tenant_id)

        if not name:
            raise exc.configuration(
                "MongoDatabaseTenantProvisioner resolved an empty database name for tenant "
                f"{tenant.tenant_id}.",
                code="tenant_database_unresolved",
            )

        if name in _SYSTEM_DATABASES:
            raise exc.configuration(
                f"MongoDatabaseTenantProvisioner resolved the system database {name!r} for "
                f"tenant {tenant.tenant_id}. MongoDB keeps its users, its replica-set "
                "configuration and its oplog there, so provisioning into it is wrong and "
                "dropping it takes the deployment down. Resolve a database this tenant owns.",
                code="tenant_database_is_system",
                details={"database": name},
            )

        return name

    # ....................... #

    async def _refuse_database_shared_with_others(
        self,
        tenant: TenantIdentity,
        *,
        database: str,
    ) -> None:
        """Refuse to drop a database that is not this tenant's alone.

        Two refusals with one read, because both are answers to *whose data is under this
        name?* and both are wrong to guess at:

        - **Another tenant's marker is there.** The resolver is constant, or two tenants were
          onboarded onto one name; either way the drop would take a live tenant's data with
          it. This is where a resolver that only looks per-tenant is finally caught, and it is
          caught by the server rather than by inspecting the resolver — which cannot be
          inspected, since the tenant ids do not exist at construction and it may be async.
        - **No marker at all, but collections exist.** This provisioner never registered the
          database, so nothing here knows what is in it. Dropping on the strength of a name a
          resolver produced is how an unrelated database gets destroyed by a typo.

        A database with neither markers nor collections is simply absent — MongoDB has no such
        thing as an empty database — so an offboarding that already ran, or one for a tenant
        that never wrote, returns quietly. Teardown is re-run at least as often as onboarding.

        **What this check is not.** It reads and then drops, with no lock between: an
        onboarding that lands its marker in that window is dropped along with the database.
        Postgres closes the equivalent gap with a transaction-scoped advisory lock, and
        MongoDB offers nothing that spans a ``dropDatabase`` the same way. The exposure is
        narrow — it needs the constant resolver this refusal exists to catch *and* a
        concurrent onboarding of a second tenant — and the honest fix for it is the
        per-tenant name, under which no other tenant's marker can appear in the first place.
        """

        coll = await self.client.collection(self.marker_collection, db_name=database)
        mine = _marker_id(tenant)

        # Two is enough to answer the question: this tenant's marker, plus evidence of one
        # other. Naming that other one is what makes the refusal actionable.
        markers = await self.client.find_many(coll, {}, projection={"_id": 1}, limit=2)
        others = [str(doc["_id"]) for doc in markers if str(doc["_id"]) != mine]

        if others:
            raise exc.configuration(
                f"Database {database!r} was also provisioned for tenant {others[0]!r}, so "
                f"dropping it while offboarding {tenant.tenant_id} would destroy a live "
                "tenant's data. A database resolver must return a distinct name per tenant — "
                "a constant one has the shape of per-tenant scoping without the substance. "
                "Drop the tenant's own collections instead, or give each tenant its own "
                "database so teardown owns everything it created.",
                code="tenant_database_shared_across_tenants",
                details={"database": database, "also_provisioned_for": others[0]},
            )

        # This tenant's marker and nothing else: the database is theirs, and the drop is what
        # offboarding asked for.
        if markers:
            return

        db_handle = await self.client.db(database)

        if await db_handle.list_collection_names():
            raise exc.configuration(
                f"Database {database!r} holds collections but no record of being provisioned "
                f"for tenant {tenant.tenant_id}, so what it contains is unknown here and "
                "dropping it is not this provisioner's call. If it really is this tenant's, "
                "run provision() first — it is idempotent and only writes the marker — then "
                "offboard again.",
                code="tenant_database_not_provisioned",
                details={"database": database},
            )


# ....................... #


def _marker_id(tenant: TenantIdentity) -> str:
    """The marker document's ``_id`` for *tenant*.

    The tenant id and nothing else, so the identity is carried by the one field MongoDB
    already indexes uniquely. That is what makes concurrent onboardings of one tenant
    converge on a single document instead of accumulating near-duplicates, and it is why the
    marker collection needs no index of its own on a database created per tenant.
    """

    return str(tenant.tenant_id)
