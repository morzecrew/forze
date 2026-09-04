"""Mongo co-located idempotency store — atomic in-transaction result commit."""

from __future__ import annotations

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from datetime import datetime
from typing import Any, Final, final
from uuid import UUID

import attrs
from pymongo.asynchronous.collection import AsyncCollection

from forze.application.contracts.idempotency import (
    ClaimOwnerMixin,
    IdempotencyPort,
    IdempotencyRecord,
    IdempotencySpec,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, utcnow, uuid4
from forze_mongo.execution.deps.configs.idempotency import MongoIdempotencyConfig
from forze_mongo.kernel.client import MongoClientPort
from forze_mongo.kernel.relation import resolve_mongo_collection

# ----------------------- #

_PENDING: Final[str] = "pending"
_DONE: Final[str] = "done"


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoIdempotencyStore(TenancyMixin, ClaimOwnerMixin, IdempotencyPort):
    """Mongo-backed co-located idempotency store (``commits_in_transaction``).

    :meth:`commit` runs on the caller's session — the auto-injected ``on_success`` hook
    invokes it inside the business transaction — so the result record and the business
    writes commit atomically, closing the crash window an out-of-transaction store leaves
    open. That holds only while the business writes go through **this** Mongo client and a
    transaction is open (a replica set); on a standalone deployment the record still lands,
    just not atomically, which is the at-least-once behaviour every other store has.

    :meth:`begin` and :meth:`fail` run **detached** — never on the caller's session — so a
    pending claim is visible to a concurrent duplicate the moment it is taken, and a
    release survives whatever transaction is unwinding around it. Both would otherwise be
    invisible until commit, or be rolled back with the operation they are reporting on.

    Documents look like ``{_id, op, idem_key, payload_hash, tenant_id, status, result,
    expires_at, claim_token, owner}``; the ``_id`` is the atomicity anchor, so concurrent claims
    serialize on it without a unique index the application never migrated. Expired
    documents (past ``IdempotencySpec.ttl``) are re-claimed in place; a TTL index on
    ``expires_at`` is the optional cleanup for keys that are never reused.

    ``claim_token`` and ``owner`` are separate fields answering different questions.
    The token is how :meth:`begin` tells its own fresh insert from a live document it
    merely read back, so it must be unique *per call*; the owner is the invocation, which
    is unique per request and deliberately the same across one invocation's calls. Folding
    the two into one field would break the first test twice — with no provider both sides
    are ``None`` and a live claim reads as a fresh insert, and even with one an invocation
    that calls :meth:`begin` twice for a key would match its own earlier claim.
    """

    client: MongoClientPort
    spec: IdempotencySpec
    config: MongoIdempotencyConfig

    # ....................... #

    @property
    def commits_in_transaction(self) -> bool:
        """Always ``True``: :meth:`commit` writes on the caller's session."""

        return True

    # ....................... #

    async def _collection(self) -> AsyncCollection[JsonDict]:
        # Namespace-tier resolution: the bound tenant scopes a per-tenant collection even
        # without tagged-tier ``tenant_aware`` (see the counter and inbox adapters).
        tenant_id = self._tenant_id_for_resolve()
        db_name, coll_name = await resolve_mongo_collection(
            self.config.collection,
            tenant_id,
        )
        return await self.client.collection(coll_name, db_name=db_name)

    # ....................... #

    def _doc_id(self, op: str, key: str, tenant_id: UUID | None) -> str:
        # The ``_id`` is the atomicity anchor. The tenant prefix keeps tagged-tier tenants
        # apart; the length prefix on the operation makes the composition unambiguous for
        # any op/key contents (``a|b`` + ``c`` cannot collide with ``a`` + ``b|c``). The
        # tenant is resolved once by the caller so the ``_id`` tag and the stored
        # ``tenant_id`` field can never disagree.
        body = f"{len(op)}:{op}|{key}"

        return f"tenant:{tenant_id}|{body}" if tenant_id is not None else body

    # ....................... #

    def _claim_fields(
        self,
        op: str,
        key: str,
        payload_hash: str,
        tenant_id: UUID | None,
        claim_token: str,
        now: datetime,
    ) -> dict[str, Any]:
        """The pending-claim document body, shared by a fresh claim and an expired reclaim."""

        owner = self.claim_owner()

        return {
            # The invocation this claim belongs to, so a duplicate that reclaims the key
            # cannot have its claim completed or released by the operation it displaced.
            # ``None`` when nothing was wired, which is what leaves the fence degraded
            # rather than refusing every commit.
            "owner": str(owner) if owner is not None else None,
            "op": op,
            "idem_key": key,
            "payload_hash": payload_hash,
            "tenant_id": str(tenant_id) if tenant_id is not None else None,
            "status": _PENDING,
            # A reclaim overwrites a previous record, and the stale result goes with it: no
            # read path would serve those bytes again (``begin`` replays only a ``done``
            # document), so keeping them would retain an operation's payload past the expiry
            # that retired it, for nothing.
            "result": None,
            "expires_at": now + self.spec.ttl,
            "claim_token": claim_token,
        }

    # ....................... #

    async def begin(
        self,
        op: str,
        key: str | None,
        payload_hash: str,
    ) -> IdempotencyRecord | None:
        if not key:
            return None

        coll = await self._collection()
        tenant_id = self.require_tenant_if_aware()
        doc_id = self._doc_id(op, key, tenant_id)
        now = utcnow()
        claim_token = uuid4().hex

        async with self.client.detached():
            # Claim-or-read in one atomic step: ``$setOnInsert`` writes the claim only when
            # the document is absent, and the returned document is either the one just
            # inserted or the live one that blocked it. The claim token is what tells those
            # apart — a status read could not, since a concurrent claim also reads
            # ``pending``. An exact ``_id`` filter never raises a duplicate key here.
            doc = await self.client.find_one_and_update(
                coll,
                {"_id": doc_id},
                {
                    "$setOnInsert": self._claim_fields(
                        op, key, payload_hash, tenant_id, claim_token, now
                    )
                },
                upsert=True,
            )

            if doc is None:  # pragma: no cover - the driver always returns the document
                raise exc.conflict("Idempotency is in progress")

            if doc.get("claim_token") == claim_token:
                return None  # fresh claim

            if self._is_expired(doc, now):
                await self._reclaim(coll, doc_id, op, key, payload_hash, tenant_id, now)
                return None

        return self._replay(doc, payload_hash)

    # ....................... #

    def _is_expired(self, doc: JsonDict, now: datetime) -> bool:
        """Whether *doc* has passed its dedup window, against the caller's own ``now``.

        The same ``now`` decides this and matches :meth:`_reclaim`'s ``$lte`` filter, so a
        document judged expired here cannot then fail to match the reclaim — the two must
        keep using one comparison and one clock, or a caller gets a conflict for a document
        it was just told it could take.

        A document with **no** ``expires_at`` counts as expired: only something other than
        this store could have written one, and treating it as live would block its key
        forever with no way to clear it, where treating it as expired costs one re-execution.
        """

        expires_at = doc.get("expires_at")

        return expires_at is None or expires_at <= now

    # ....................... #

    async def _reclaim(
        self,
        coll: AsyncCollection[JsonDict],
        doc_id: str,
        op: str,
        key: str,
        payload_hash: str,
        tenant_id: UUID | None,
        now: datetime,
    ) -> None:
        """Take over an expired document, or refuse if another writer took it first.

        Returns nothing on success, like a fresh claim: a reclaimed document never carries
        a result to replay, because taking it over is what cleared one.
        """

        claim_token = uuid4().hex
        # The ``expires_at`` guard is what makes this a race rather than a steal: exactly
        # one of two writers seeing the same expired document matches the filter, and the
        # loser is told the operation is in progress instead of running a duplicate.
        #
        # The ``None`` branch is not decoration: a Mongo range query does not match a
        # document whose field is *missing*, so without it a document written by something
        # other than this store — which :meth:`_is_expired` judges expired precisely so it
        # can be taken over — would fail this filter and refuse its key forever. ``None``
        # matches both a null and an absent field, which is what keeps the two in step.
        doc = await self.client.find_one_and_update(
            coll,
            {
                "_id": doc_id,
                "$or": [{"expires_at": {"$lte": now}}, {"expires_at": None}],
            },
            {"$set": self._claim_fields(op, key, payload_hash, tenant_id, claim_token, now)},
        )

        if doc is None:
            raise exc.conflict("Idempotency is in progress")

        return None

    # ....................... #

    def _owner_filter(self) -> dict[str, Any]:
        """The ownership predicate for :meth:`commit` / :meth:`fail`, empty when degraded.

        ``{"owner": None}`` matches a null **and a missing** field, which is what lets a
        claim written before this store carried an owner still be completed; omitting the
        predicate entirely is what a store with no provider does, since a caller who cannot
        name itself has nothing to prove ownership with.
        """

        owner = self.claim_owner()

        if owner is None:
            return {}

        return {"$or": [{"owner": str(owner)}, {"owner": None}]}

    # ....................... #

    def _replay(self, doc: JsonDict, payload_hash: str) -> IdempotencyRecord:
        """Decide what a live document owned by someone else means for this caller."""

        if doc.get("payload_hash") != payload_hash:
            # conflict, not precondition: the key is already bound to other arguments,
            # which the boundary renders as 409 on every store.
            raise exc.conflict("Payload hash mismatch")

        result = doc.get("result")

        if doc.get("status") != _DONE or result is None:
            raise exc.conflict("Idempotency is in progress")

        return IdempotencyRecord(result=bytes(result))

    # ....................... #

    async def commit(
        self,
        op: str,
        key: str | None,
        payload_hash: str,
        record: IdempotencyRecord,
    ) -> None:
        if not key:
            return

        coll = await self._collection()
        tenant_id = self.require_tenant_if_aware()
        now = utcnow()

        # Rides the caller's session (the ``on_success`` hook) -> the record commits
        # atomically with the business writes; a rollback reverts it.
        matched = await self.client.update_one(
            coll,
            {
                "_id": self._doc_id(op, key, tenant_id),
                "payload_hash": payload_hash,
                "status": _PENDING,
                **self._owner_filter(),
            },
            {
                "$set": {
                    "status": _DONE,
                    "result": record.result,
                    "expires_at": now + self.spec.ttl,
                }
            },
        )

        if matched == 0:
            # No matching pending claim of our own: fail closed so the business transaction
            # rolls back rather than committing an effect with no idempotency record — or,
            # where the claim was reclaimed by a duplicate, rather than replacing that
            # operation's live claim with this one's result.
            raise exc.conflict(
                "Idempotency commit failed (claim missing, non-pending, or reclaimed)"
            )

    # ....................... #

    async def fail(
        self,
        op: str,
        key: str | None,
        payload_hash: str,
    ) -> None:
        if not key:
            return

        coll = await self._collection()
        tenant_id = self.require_tenant_if_aware()

        async with self.client.detached():
            # Only release our own pending claim: a completed record, a claim taken for a
            # different payload hash, or one a duplicate reclaimed is left untouched.
            await self.client.delete_one(
                coll,
                {
                    "_id": self._doc_id(op, key, tenant_id),
                    "payload_hash": payload_hash,
                    "status": _PENDING,
                    **self._owner_filter(),
                },
            )
