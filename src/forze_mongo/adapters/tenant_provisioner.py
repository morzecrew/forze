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

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Final, final

import attrs
from pymongo.asynchronous.collection import AsyncCollection

from forze.application.contracts.resolution import (
    NamedResourceSpec,
    coerce_named_resource_spec,
)
from forze.application.contracts.tenancy import TenantIdentity, TenantProvisionerPort
from forze.application.contracts.tenancy.routed_client_base import RoutedTenantClientBase
from forze.base.exceptions import exc
from forze.base.logging import get_logger
from forze.base.primitives import JsonDict, utcnow

from ..kernel.client import MongoClientPort
from ..kernel.relation import resolve_mongo_named_resource

# ----------------------- #

_MARKER_COLLECTION: Final = "_forze_tenants"
"""Default name of the per-database collection holding one marker document per tenant."""

_LOCK_COLLECTION: Final = "_forze_tenant_offboarding"
"""Default name of the collection holding one document per offboarding in flight."""

_SYSTEM_DATABASES: Final = frozenset({"admin", "config", "local"})
"""Databases MongoDB owns. Dropping any of them takes the deployment with it."""

log = get_logger(__name__)


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

    lock_collection: str = _LOCK_COLLECTION
    """Collection holding one document per offboarding in flight, keyed by database name.

    Read by :meth:`provision` and written by :meth:`deprovision`; see
    :meth:`_offboarding_lock` for what the pair of them buys."""

    lock_database: str | None = None
    """Database holding :attr:`lock_collection`. ``None`` means the client's own.

    It has to be somewhere ``dropDatabase`` does not reach, which is the whole reason it is
    not simply another collection beside the markers. The client's default database is that
    for every deployment resolving a database per tenant, and the one deployment where it is
    not — an admin client pointed at a database that is also somebody's tenant database — is
    refused rather than served, since a lock the drop destroys is not a lock."""

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
            # A pipeline rather than `$set` beside `$setOnInsert`, because the two fields want
            # opposite things and `$setOnInsert` can only express one of them. The identity is
            # rewritten on every pass, so the re-provision the teardown's refusal asks for
            # repairs a damaged marker; the onboarding stamp is filled where it is missing and
            # never moved where it is not, since it is the one field an operator reads to
            # answer when a tenant joined. On an insert the pipeline runs against no document,
            # so `$ifNull` yields the stamp below.
            [
                {
                    "$set": {
                        "tenant_id": str(tenant.tenant_id),
                        "provisioned_at": {"$ifNull": ["$provisioned_at", utcnow()]},
                    }
                }
            ],
        )

        # Both reads come after the write, and the lock before the marker. That ordering is
        # the whole protocol — see :meth:`_offboarding_lock` — and neither half of it is
        # cosmetic. Reading before writing leaves the marker landing in a gap the teardown has
        # already read past; reading the marker first lets an onboarding interrupted between
        # the two see its marker intact and then see a lock already released, which is the
        # very conclusion the pair exists to refuse.
        await self._refuse_an_offboarding_in_flight(name)
        await self._refuse_an_onboarding_that_lost_its_database(coll, tenant, database=name)

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

        async with self._offboarding_lock(tenant, database=name):
            await self._refuse_a_database_not_solely_this_tenants(tenant, database=name)

            database = await self.client.db(name)
            await database.command("dropDatabase")

    # ....................... #

    @asynccontextmanager
    async def _offboarding_lock(
        self,
        tenant: TenantIdentity,
        *,
        database: str,
    ) -> AsyncIterator[None]:
        """Hold a teardown of *database* open, so no onboarding can finish underneath it.

        The ownership read that authorises the drop goes stale the instant it returns: an
        onboarding landing between it and ``dropDatabase`` has its marker — and its data —
        destroyed by a teardown that never saw it. Postgres closes the same gap with a
        transaction-scoped advisory lock, and MongoDB has nothing that spans a
        ``dropDatabase``, so the exclusion is a document, and it lives *outside* the database
        being dropped because anything inside it goes with the drop.

        **Not mutual exclusion, and deliberately less.** Only the teardown writes the lock;
        :meth:`provision` writes its marker and then reads two things — this lock, and back
        the marker it just wrote. Suppose a drop destroyed the marker of an onboarding that
        returned successfully anyway. Finding that marker on the way out puts the marker read
        before the drop, and the lock read before that, so the lock read happened before the
        release. A lock read that finds nothing before the release must have run before the
        write — so the marker was in place before the lock, and the ownership read, which
        follows the lock, saw it and refused. No drop happened, contradicting the premise.

        Neither read is redundant, and the second is the one an argument gets wrong first.
        "No lock" has two readings: not started, or already finished. An onboarding that wrote
        its marker while a teardown sat between its ownership read and its drop passes the
        lock read on the second reading, having had its marker destroyed in between — which
        only reading the marker back can catch.

        Their *order* is load-bearing too, and it is the step the argument above turns on.
        Reading the lock first is what puts its answer before the marker read, which is what
        the surviving marker then bounds before the release. Marker first inverts that: an
        onboarding interrupted between the two finds its marker intact, then finds a lock
        already released, and concludes exactly what the pair exists to refuse.

        What that buys over locking both sides is that onboarding, the frequent operation,
        never contends: concurrent onboardings of one tenant still converge on the server, and
        onboardings of different tenants never meet at all. Only teardowns serialize, and a
        second teardown of the same database is refused rather than queued — two overlapping
        ``dropDatabase`` calls have no ordering worth waiting for.

        **A holder that dies leaves the lock behind, and that is the trade.** Nothing here
        expires it, because the only bound available would be a timeout, and a ``dropDatabase``
        that outran its timeout would have the lock released under a live drop — reopening
        precisely this race, and quietly. So a crashed teardown wedges that one database name
        until an operator removes the document, and every refusal names it. Wedged onboarding
        is recoverable; a destroyed tenant is not.

        One assumption the argument rests on: these reads see prior writes, which holds for
        the primary and not for a client pointed at secondaries. An admin connection reading
        stale catalogue state would be misreading more than this lock.
        """

        coll, lock = await self._lock_target(database)

        # Insert-only, so the answer is "did it already exist?" rather than "did I win a
        # write?": a matched document is a teardown already in flight. Concurrent claims race
        # on `_id`, which the server resolves — the loser matches and reports 1.
        held = await self.client.update_one(
            coll,
            {"_id": database},
            {"$setOnInsert": {"tenant_id": str(tenant.tenant_id), "at": utcnow()}},
            upsert=True,
        )

        if held:
            # Concurrency rather than configuration, so a caller's retry strategy can treat it
            # as what it usually is — a teardown that will be finished shortly. The one shape
            # that will not clear on its own is the orphaned lock, which is why the message
            # says so rather than leaving a bounded retry to discover it.
            raise exc.concurrency(
                f"An offboarding of database {database!r} is already in flight, so this one "
                "stops rather than running a second dropDatabase beside it. If no teardown is "
                "actually running, the previous one died holding the lock: remove "
                f"{{_id: {database!r}}} from {lock} and offboard "
                "again. Nothing expires it on its own, because a lock that expired under a "
                "live drop would reopen the race it exists to close.",
                code="tenant_offboarding_in_flight",
                details={"database": database, "lock": lock},
            )

        failed = False

        try:
            yield

        except BaseException:
            failed = True
            raise

        finally:
            try:
                await self.client.delete_one(coll, {"_id": database})

            except Exception as error:
                # Two different situations, and only one of them may speak. A teardown that
                # already failed owns the outcome — a release error raised here would replace
                # the refusal or the storage error the caller needs, and the stuck lock is the
                # lesser of the two facts. A teardown that succeeded is outranking nothing:
                # the drop is durable either way, and the caller has to hear that the name is
                # now wedged, because nothing else will tell them.
                if not failed:
                    raise exc.infrastructure(
                        f"Database {database!r} was dropped, but the offboarding lock could "
                        f"not be released: {error}. Until {{_id: {database!r}}} is removed "
                        f"from {lock}, provisioning and offboarding that database both "
                        "refuse.",
                        code="tenant_offboarding_lock_stuck",
                        details={"database": database, "lock": lock},
                    ) from error

                log.warning(
                    "could not release a tenant offboarding lock",
                    database=database,
                    error=str(error),
                )

    # ....................... #

    async def _refuse_an_offboarding_in_flight(self, database: str) -> None:
        """Refuse an onboarding whose marker a teardown in flight is about to destroy.

        Read after the marker is written rather than before it — the ordering :meth:`_offboarding_lock`
        proves correct. The refusal is a failed onboarding, which is retried; the marker it
        already wrote is left where it is, because a marker for this tenant in this tenant's
        database is right whether the teardown proceeds or refuses.
        """

        coll, lock = await self._lock_target(database)

        if await self.client.find_one(coll, {"_id": database}, projection={"_id": 1}) is None:
            return

        raise exc.concurrency(
            f"Database {database!r} is being offboarded, so this onboarding stops rather than "
            "writing into a database that is about to be dropped. Retry once the offboarding "
            f"has finished; if none is running, it died holding {{_id: {database!r}}} in "
            f"{lock}, which has to be removed by hand.",
            code="tenant_offboarding_in_flight",
            details={"database": database, "lock": lock},
        )

    # ....................... #

    async def _refuse_an_onboarding_that_lost_its_database(
        self,
        coll: AsyncCollection[JsonDict],
        tenant: TenantIdentity,
        *,
        database: str,
    ) -> None:
        """Refuse an onboarding whose marker a finished teardown already took.

        The lock check answers "is a teardown running?", which is not the same question as
        "did one run?" — an onboarding slow enough to write its marker inside a teardown's
        window and look at the lock after the release passes that check having written into a
        database that no longer exists. Reading the marker back is what closes it; see
        :meth:`_offboarding_lock` for why both are needed.
        """

        if await self.client.find_one(coll, {"_id": _marker_id(tenant)}) is not None:
            return

        raise exc.concurrency(
            f"Database {database!r} was dropped while tenant {tenant.tenant_id} was being "
            "onboarded into it, so this onboarding wrote nothing that still exists. Retry it "
            "— an offboarding that has finished no longer blocks anything, and the retry "
            "recreates the database.",
            code="tenant_onboarding_lost_its_database",
            details={"database": database},
        )

    # ....................... #

    async def _lock_target(self, database: str) -> tuple[AsyncCollection[JsonDict], str]:
        """The lock collection and the ``db.collection`` an operator would go and look at.

        Refuses a configuration that puts the lock inside *database*: a lock kept in the
        database a teardown drops is destroyed by that teardown, so the window stands open
        exactly while it is being used. The default — the admin client's own database — is
        outside every tenant's for any deployment resolving a database per tenant, and the one
        deployment where it is not is a mistake worth naming rather than serving.
        """

        name = self.lock_database or await self._client_database()

        if name == database:
            raise exc.configuration(
                f"The offboarding lock would live in {name!r}, which is the database being "
                "provisioned or dropped, so a teardown would destroy the lock holding it "
                "open. Point lock_database at a database no tenant resolves to.",
                code="tenant_lock_inside_target_database",
                details={"database": database},
            )

        coll = await self.client.collection(self.lock_collection, db_name=name)

        return coll, f"{name}.{self.lock_collection}"

    # ....................... #

    async def _client_database(self) -> str:
        """The admin client's own default database, refused clearly when it has none."""

        try:
            handle = await self.client.db()

        except Exception as error:
            raise exc.configuration(
                "The offboarding lock defaults to the client's own database and this client "
                "has none configured. Set lock_database to a database no tenant resolves to.",
                code="tenant_lock_database_unresolved",
            ) from error

        return handle.name

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

    async def _refuse_a_database_not_solely_this_tenants(
        self,
        tenant: TenantIdentity,
        *,
        database: str,
    ) -> None:
        """Refuse to drop a database that is not this tenant's alone.

        Three refusals off one read, because all three are answers to *whose data is under
        this name?* and all three are wrong to guess at:

        - **Another tenant's marker is there.** The resolver is constant, or two tenants were
          onboarded onto one name; either way the drop would take a live tenant's data with
          it. This is where a resolver that only looks per-tenant is finally caught, and it is
          caught by the server rather than by inspecting the resolver — which cannot be
          inspected, since the tenant ids do not exist at construction and it may be async.
        - **A document under this tenant's id that this provisioner did not write.** ``_id``
          is a name anyone can write, so presence is not recognition: a document missing
          either field :meth:`provision` writes is another program's record or a damaged one,
          and both readings say the contents are unknown here. Reading it as ownership would
          be a branch lenient about *missing* state quietly swallowing *corrupt* state, with a
          ``dropDatabase`` behind it. Both fields, not the first one — half a marker is not a
          marker, and a document nearly right is likelier to be a collision than one sharing
          only its name.
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
        # thing that is not it. Naming that other thing is what makes the refusal actionable.
        markers = await self.client.find_many(
            coll,
            {},
            projection={"_id": 1, "tenant_id": 1, "provisioned_at": 1},
            limit=2,
        )
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

        # Reached only with `others` empty, so every marker here is under this tenant's id and
        # the question left is whether this provisioner is the one that put it there.
        if any(
            doc.get("tenant_id") != mine or doc.get("provisioned_at") is None for doc in markers
        ):
            raise exc.configuration(
                f"Database {database!r} holds a document under tenant {tenant.tenant_id}'s id "
                f"in {self.marker_collection!r} that this provisioner did not write — it is "
                "missing the matching tenant_id, the onboarding stamp, or both — so it is "
                "another program's record or a damaged one, and either way what the database "
                "contains is unknown here. Look at the document; if the database really is "
                "this tenant's, provision() completes the marker and the offboarding then "
                "goes through.",
                code="tenant_marker_unrecognized",
                details={"database": database, "collection": self.marker_collection},
            )

        # This tenant's own marker and nothing else: the database is theirs, and the drop is
        # what offboarding asked for.
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
