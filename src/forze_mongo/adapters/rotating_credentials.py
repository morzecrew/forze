"""Mongo store for credentials a counterparty rotates as a side effect of use.

The plane's two hard requirements land differently here than they do on Postgres, and the
difference is not a stylistic one — it follows from what Mongo can and cannot hold:

- **Serialize the exchange.** Postgres takes ``SELECT … FOR UPDATE`` and lets a second
  worker *block* on the row until the winner commits. Mongo has no such wait: a second
  transaction writing the same document is aborted with a write conflict rather than
  queued, and a transaction is capped by the server's ``transactionLifetimeLimitSeconds``
  (60 s by default) — a bound a third party's token endpoint has no obligation to respect.
  So the exclusion is an explicit **lease** on the credential's own document, taken with
  one atomic ``findAndModify`` and held across the exchange. A racer that finds the lease
  held waits for it, re-reads, and converges on the winner's document. An in-process stripe
  of locks sits in front so same-process racers never even reach the server.
- **Persist before use.** The replacement is written with one fenced update — matched on
  the version the lease was taken at *and* on the lease's own owner — and that write is
  what makes it durable. Nothing observes a credential that is not already written, and a
  write that fails after a successful exchange is reported as a lost credential rather than
  a retryable storage error: the presented token is already burned by then, and no retry can
  bring it back.

Leases expire, and row locks do not. That is the one structural difference this store has
to answer for, because a worker that dies mid-exchange must not brick the grant forever and
must not hand the next worker a token the counterparty may already have consumed. The
document therefore records ``presented`` the moment before the token leaves the process:

- an expired lease with ``presented`` unset means the holder died *before* the exchange, so
  the stored token was never shown and the taker may exchange it normally;
- an expired lease with ``presented`` set means the outcome was lost. The taker refuses to
  exchange and marks the grant unusable, exactly as a timeout or a failed persist does —
  reuse of a spent refresh token revokes the whole grant family, and "probably fine" is not
  a basis for finding out.

Every operation runs **detached** — never on the caller's session. A credential's rotation
is not part of the caller's transaction: the token is burned at the counterparty whatever
the caller's transaction does next, so a write that a rollback could take back is a write
that loses the replacement.

The collection is provided by the application; documents look like::

    {
        _id:          "<tenant>|<ref>",   # tenant is "" when unbound
        tenant_id:    "<tenant>",
        ref:          "<ref>",
        payload:      {...},              # sealed by default, see below
        expires_at:   ISODate | null,
        version:      NumberLong,
        burnt_reason: "..." | null,
        lease_until:  ISODate | null,
        lease_owner:  "..." | null,
        presented:    true | null,
        created_at:   ISODate,
        updated_at:   ISODate,
    }

``_id`` carries the tenant because a collection keyed on the ref alone would hand one
tenant another's grant; it is also the atomicity anchor, so concurrent leases and upserts
serialize on it without a unique index the application never migrated. ``tenant_id`` and
``ref`` are repeated as plain fields for the control-plane scan.

**The application owns one index**, for :class:`MongoRotatingCredentialsAdmin`::

    db.<collection>.createIndex({tenant_id: 1, updated_at: 1})

The scan orders by idleness within a tenant, which ``_id`` cannot answer. Without it the
scan is a collection scan — tolerable for tens of grants, not for tens of thousands.

``updated_at`` is the idleness clock, and BSON dates carry milliseconds where a Postgres
``timestamptz`` carries microseconds. The window a sweep runs against is measured in days,
so the granularity is immaterial to the contract; it is stated because two exchanges inside
one millisecond record the same stamp.

**The payload is sealed at rest by default.** Every document here is a replayable long-lived
credential, so a plaintext collection turns a leaked backup or a read-only secondary into
working third-party access for every tenant. With a :attr:`cipher` wired, ``payload`` is
stored as a self-describing envelope whose AAD binds it to ``(domain, tenant, ref)`` — so a
document lifted into another ref or another tenant fails authentication instead of
decrypting into the wrong grant. ``expires_at`` stays readable.

Enabling encryption needs no migration: a plaintext payload is passed through on read and
sealed on its next write. Turning it *off* on a collection that already holds envelopes is
not symmetric — those documents still need the key, and a store wired without a cipher
refuses them rather than returning garbage.
"""

from __future__ import annotations

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

import asyncio
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Final, cast, final
from uuid import UUID

import attrs
from pymongo.asynchronous.collection import AsyncCollection

from forze.application.contracts.crypto import BytesCipherPort
from forze.application.contracts.secrets import (
    BURNT_CREDENTIAL_CODE,
    CREDENTIAL_EXCHANGE_TIMEOUT_CODE,
    CREDENTIAL_PERSIST_LOST_CODE,
    INVALID_GRANT_CODE,
    CredentialExchangerPort,
    DueCredential,
    ExchangedCredential,
    RotatingCredential,
    RotatingCredentialsAdminPort,
    RotatingCredentialStorePort,
    SecretRef,
    SecretVersion,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.application.integrations.crypto.payload import (
    ROTATING_CREDENTIAL_PAYLOAD_DOMAIN,
    decrypt_payload,
    encrypt_payload,
)
from forze.base.exceptions import CoreException, exc
from forze.base.logging import get_logger
from forze.base.primitives import JsonDict, StripedAsyncLocks, utcnow, uuid7
from forze_mongo.execution.deps.configs.rotating_credentials import (
    MongoRotatingCredentialsConfig,
)
from forze_mongo.kernel.client import MongoClientPort
from forze_mongo.kernel.relation import resolve_mongo_collection

# ----------------------- #

_LEASE_BOUND_FACTOR: Final[int] = 2
"""Multiple of :attr:`~MongoRotatingCredentialStore.exchange_timeout` the lease is held for.

The lease must *outlive* the exchange it guards, never merely match it: everything else the
holder does inside the lease — opening the payload (a cold data key may cost one KMS round
trip), sealing the replacement, the fenced write — happens after the exchange's own clock
has run, and a lease that expires under its holder is a lease another worker can steal from
a live rotation.

It is also the waiting racer's patience: converging on the winner is a better outcome than
erroring, so a loser waits out one full exchange and its write before giving up.
"""

_LEASE_POLL: Final[float] = 0.01
"""Seconds between attempts to take a lease another worker holds.

Mongo offers no blocking wait on a document, so the racer polls. Small enough that
converging on the winner costs a fraction of the exchange it waited for, large enough that a
handful of racers do not turn a 50 ms exchange into a busy loop.
"""

log = get_logger(__name__)


def _discard_outcome(rotation: asyncio.Future[RotatingCredential]) -> None:
    """Collect an abandoned rotation's result so it cannot surface as a stray warning.

    A caller that cancelled its refresh is no longer waiting, but the rotation itself was
    shielded and runs on to record an outcome. Whatever it ends with — a fresh credential, or
    the mark that the old one is spent — has already been logged and written by the time it
    gets here; retrieving it just keeps the event loop from complaining about a result nobody
    read.
    """

    if not rotation.cancelled():
        rotation.exception()


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class _MongoRotatingBase(TenancyMixin):
    """Collection and key resolution shared by the store and its control-plane scan."""

    client: MongoClientPort
    config: MongoRotatingCredentialsConfig

    # ....................... #

    async def _collection(self) -> AsyncCollection[JsonDict]:
        # Namespace-tier resolution: the bound tenant scopes a per-tenant collection even
        # without tagged-tier ``tenant_aware`` (see the counter and inbox adapters).
        db_name, coll_name = await resolve_mongo_collection(
            self.config.collection,
            self._tenant_id_for_resolve(),
        )

        return await self.client.collection(coll_name, db_name=db_name)

    # ....................... #

    def _tenant_scope(self) -> tuple[UUID | None, str]:
        """The ambient tenant, as the AAD needs it and as the document key needs it.

        Resolved once per call and threaded, so the key a document is written under and the
        AAD it is sealed under can never disagree. The key is part of ``_id``, which cannot
        hold ``None``, so an unbound tenant is the empty string — the Postgres store's
        convention, kept identical so the two read the same to an operator. The AAD keeps
        the honest ``None``.
        """

        tenant: UUID | None = self._tenant_id_for_resolve()

        return tenant, "" if tenant is None else str(tenant)

    # ....................... #

    @staticmethod
    def _doc_id(tenant: str, ref: SecretRef) -> str:
        # Injective without a length prefix: the tenant half is either empty or a UUID, and
        # neither can contain the separator, so the first ``|`` always splits the two.
        return f"{tenant}|{ref.path}"


# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoRotatingCredentialStore(_MongoRotatingBase, RotatingCredentialStorePort):
    """:class:`RotatingCredentialStorePort` over one Mongo collection (see the module docstring)."""

    exchanger: CredentialExchangerPort
    """The counterparty call. Invoked only while the lease is held."""

    exchange_timeout: timedelta = timedelta(seconds=30)
    """Bound on the counterparty call, and the source of the lease's own duration.

    An unbounded exchange would hold the credential's lease for as long as the provider is
    willing to stall, and every other worker for the same grant behind it."""

    cipher: BytesCipherPort | None = None
    """Keyring sealing ``payload`` at rest. ``None`` stores credentials in the clear — only
    ever the result of an explicit acknowledgment at the wiring layer.

    Sealing and opening happen under the lease, because the refresh token the exchange needs
    is what is sealed. The keyring caches unwrapped data keys, so this is a local computation
    after the first grant, and the lease's duration leaves room for the cold case (see
    :data:`_LEASE_BOUND_FACTOR`)."""

    _locks: StripedAsyncLocks = attrs.field(factory=StripedAsyncLocks, init=False, repr=False)
    """First line of serialization: collapses same-process racers before any of them writes
    to the server to take the lease."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.exchange_timeout.total_seconds() <= 0:
            raise exc.configuration(
                "Exchange timeout must be positive; an unbounded exchange holds the "
                "credential's lease for as long as the counterparty stalls.",
            )

    # ....................... #

    @property
    def _lease_bound(self) -> timedelta:
        return self.exchange_timeout * _LEASE_BOUND_FACTOR

    # ....................... #

    @staticmethod
    def _payload(doc: JsonDict) -> dict[str, object]:
        payload = doc.get("payload")

        return cast("dict[str, object]", payload) if isinstance(payload, dict) else {}

    # ....................... #

    @staticmethod
    def _metadata(payload: dict[str, object]) -> dict[str, str]:
        metadata = payload.get("metadata")

        if not isinstance(metadata, dict):
            return {}

        return {
            str(key): str(value) for key, value in cast("dict[object, object]", metadata).items()
        }

    # ....................... #

    @classmethod
    def _view(cls, doc: JsonDict, payload: dict[str, object]) -> RotatingCredential:
        """Project a document plus its already-opened payload to the caller-facing view.

        Takes the opened payload rather than re-reading ``doc["payload"]`` so a sealed
        document is never decrypted twice on one call.
        """

        expires_at = doc.get("expires_at")

        return RotatingCredential(
            access_token=str(payload.get("access_token", "")),
            version=SecretVersion(str(doc["version"])),
            expires_at=expires_at if isinstance(expires_at, datetime) else None,
            metadata=cls._metadata(payload),
        )

    # ....................... #

    @staticmethod
    def _view_of(credential: ExchangedCredential, version: int) -> RotatingCredential:
        """The view of a credential this call just wrote — no read-back, no second decrypt."""

        return RotatingCredential(
            access_token=credential.access_token,
            version=SecretVersion(str(version)),
            expires_at=credential.expires_at,
            metadata=dict(credential.metadata),
        )

    # ....................... #

    async def _open(
        self,
        doc: JsonDict,
        ref: SecretRef,
        tenant_id: UUID | None,
    ) -> dict[str, object]:
        """Decrypt a stored payload, passing legacy plaintext through unchanged.

        The pass-through is what makes enabling encryption migration-free: documents written
        before the keyring was wired keep reading, and each seals on its next write.
        """

        opened = await decrypt_payload(
            self.cipher,
            cast("JsonDict", self._payload(doc)),
            domain=ROTATING_CREDENTIAL_PAYLOAD_DOMAIN,
            tenant_id=tenant_id,
            record_id=ref.path,
        )

        return cast("dict[str, object]", opened)

    # ....................... #

    async def _seal(
        self,
        payload: JsonDict,
        ref: SecretRef,
        tenant_id: UUID | None,
    ) -> JsonDict:
        """Seal a payload for storage, or return it as-is when no cipher is wired."""

        if self.cipher is None:
            return payload

        return await encrypt_payload(
            self.cipher,
            payload,
            domain=ROTATING_CREDENTIAL_PAYLOAD_DOMAIN,
            tenant_id=tenant_id,
            record_id=ref.path,
        )

    # ....................... #

    @staticmethod
    def _guard_live(doc: JsonDict | None, ref: SecretRef) -> JsonDict:
        if doc is None:
            raise exc.not_found(f"No rotating credential stored at {ref.path!r}")

        reason = doc.get("burnt_reason")

        if reason is not None:
            raise exc.precondition(
                f"Grant at {ref.path!r} is burnt and needs re-authorization: {reason}",
                code=BURNT_CREDENTIAL_CODE,
                details={"ref": ref.path},
            )

        return doc

    # ....................... #

    async def _take_lease(
        self,
        coll: AsyncCollection[JsonDict],
        doc_id: str,
        owner: str,
        now: datetime,
    ) -> JsonDict | None:
        """Claim the credential's lease atomically, or return ``None`` if someone holds it.

        The filter's ``$or`` is what makes this exclusive in one round trip: a document whose
        lease is absent, cleared, or expired is claimable, and any other is not. ``None`` also
        covers "no such document", which the caller separates by reading.

        ``presented`` is deliberately left alone: the claim returns the document *after* the
        update, so this is the one moment the previous holder's flag is still readable, and
        it is the flag that says whether their token ever left the process.
        """

        return await self.client.find_one_and_update(
            coll,
            {
                "_id": doc_id,
                # ``lease_until: None`` matches an explicit null and a missing field alike,
                # which is what a document written by ``put`` or by an older build looks
                # like — both are unleased.
                "$or": [{"lease_until": None}, {"lease_until": {"$lte": now}}],
            },
            {"$set": {"lease_until": now + self._lease_bound, "lease_owner": owner}},
        )

    # ....................... #

    async def _release_lease(
        self,
        coll: AsyncCollection[JsonDict],
        doc_id: str,
        owner: str,
    ) -> None:
        """Drop a lease this call still owns, so a waiting racer proceeds immediately.

        Fenced on the owner: a lease that already expired and was taken by someone else must
        not be cleared out from under its new holder.

        ``presented`` is cleared with it, and that is not tidiness — a plain release only
        ever happens where the token demonstrably never reached the counterparty (the
        exchanger says so, or the failure landed before the call), so leaving the flag set
        would make the next worker treat a perfectly good grant as spent.

        Best effort, and that is load-bearing rather than lax: this runs on the way out of a
        rotation that is already carrying its own answer — a lost credential, a burn notice,
        a fresh grant — and an exception raised here would replace it with a storage error
        that reads retryable. The lease expires on its own; the outcome does not survive
        being overwritten.
        """

        try:
            await self.client.update_one(
                coll,
                {"_id": doc_id, "lease_owner": owner},
                {"$set": {"lease_until": None, "lease_owner": None, "presented": None}},
            )

        except Exception as e:
            log.warning("could not release a rotating credential's lease", error=str(e))

    # ....................... #

    async def _mark_presented(
        self,
        coll: AsyncCollection[JsonDict],
        doc_id: str,
        ref: SecretRef,
        owner: str,
    ) -> None:
        """Record that the stored token is about to leave this process.

        The one fact a lease cannot carry on its own. A lease expires; when another worker
        takes it over, this flag is the difference between "the holder died before showing
        the token, so it is still good" and "the outcome is lost, so the grant is spent".
        Written *before* the exchange, because after it there may be no chance to write
        anything — and it doubles as the check that this call still holds the lease it took.
        A write matching nothing means the lease was stolen (it expired under a stalled
        holder) or superseded by a re-authorization, and the taker owns the outcome now: this
        call must not also present the token, because two exchanges of one refresh token is
        the reuse the whole plane is arranged to avoid. It is the only window in which a
        theft is invisible to the ``presented`` flag, since the flag is what this write sets.
        """

        if not await self.client.update_one(
            coll,
            {"_id": doc_id, "lease_owner": owner},
            {"$set": {"presented": True}},
        ):
            raise exc.infrastructure(
                f"Lost the lease on the rotating credential at {ref.path!r} before its "
                "token was presented; another worker owns this rotation.",
                details={"ref": ref.path},
            )

    # ....................... #

    async def _persist(
        self,
        coll: AsyncCollection[JsonDict],
        doc_id: str,
        ref: SecretRef,
        credential: ExchangedCredential,
        *,
        version: int,
        owner: str,
        tenant_id: UUID | None,
    ) -> int:
        """Write the replacement, clearing the burn notice and the lease by construction.

        Fenced on the lease's owner: a stolen or superseded lease (a re-authorization landed,
        or this holder stalled past its lease) must not have its replacement land on top of
        whatever took its place. The version rides along as defence in depth rather than as a
        second guard — nothing can move it without also clearing this lease — and the burn
        notice is the one condition the owner does *not* cover, since ``burn`` is
        unconditional and touches neither field. Returns the matched count so the caller can
        treat a fenced-out write exactly like a failed one: the token is burned either way.
        """

        payload: JsonDict = await self._seal(
            {
                "access_token": credential.access_token,
                "refresh_token": credential.refresh_token,
                "metadata": {str(key): str(value) for key, value in credential.metadata.items()},
            },
            ref,
            tenant_id,
        )

        return await self.client.update_one(
            coll,
            # ``burnt_reason: None`` matches an explicit null and a missing field alike —
            # both mean "not burnt". It is in the fence because ``burn`` is unconditional and
            # touches neither the version nor the lease: without it, a rotation that raced an
            # operator's "the provider revoked this" would write the notice away and leave a
            # dead grant reading as live. Postgres serialises the two on its row lock; here
            # the fence is what orders them.
            {"_id": doc_id, "version": version, "lease_owner": owner, "burnt_reason": None},
            {
                "$set": {
                    "payload": payload,
                    "expires_at": credential.expires_at,
                    "version": version + 1,
                    "burnt_reason": None,
                    "updated_at": utcnow(),
                    "lease_until": None,
                    "lease_owner": None,
                    "presented": None,
                },
            },
        )

    # ....................... #

    async def _write_poison(
        self,
        coll: AsyncCollection[JsonDict],
        doc_id: str,
        ref: SecretRef,
        *,
        reason: str,
        version: int,
        owner: str,
    ) -> None:
        """Mark a grant unusable after its token was presented but the outcome was lost.

        Leaving the document untouched is what makes that state dangerous: it still *looks*
        live at the version a waiting worker holds, so the next refresh would present a token
        the counterparty may already have consumed and trip reuse detection.

        Fenced like the persist, on version and owner, so a re-authorization that landed in
        the meantime is never clobbered. Best effort by nature: if this write fails too, the
        caller still learns its own outcome, and the log carries the rest.
        """

        try:
            await self.client.update_one(
                coll,
                # Never overwrites an existing notice: an operator's reason ("revoked in the
                # provider console") is the more useful of the two, and this store's own
                # reasons are all variations of "spent". Postgres fences the same way.
                {"_id": doc_id, "version": version, "lease_owner": owner, "burnt_reason": None},
                {
                    "$set": {
                        "burnt_reason": reason,
                        "updated_at": utcnow(),
                        "lease_until": None,
                        "lease_owner": None,
                        "presented": None,
                    },
                },
            )

        except Exception as e:
            log.critical(
                "could not mark a spent rotating credential unusable",
                ref=ref.path,
                error=str(e),
            )

    # ....................... #

    async def _mark_burnt(
        self,
        coll: AsyncCollection[JsonDict],
        doc_id: str,
        tenant: str,
        ref: SecretRef,
        reason: str,
    ) -> None:
        """Record the burn notice, inserting a placeholder when no grant was ever stored.

        A notice for an unknown ref still has to stick: the caller learned the grant is dead,
        and a later read must report *needs re-authorization* rather than a bare "not found".
        """

        now = utcnow()

        await self.client.update_one_upsert(
            coll,
            {"_id": doc_id},
            {
                "$set": {"burnt_reason": reason, "updated_at": now},
                "$setOnInsert": {
                    "tenant_id": tenant,
                    "ref": ref.path,
                    "payload": {},
                    "expires_at": None,
                    "version": 0,
                    "lease_until": None,
                    "lease_owner": None,
                    "presented": None,
                    "created_at": now,
                },
            },
        )

    # ....................... #

    async def _exchange(
        self,
        ref: SecretRef,
        payload: dict[str, object],
    ) -> ExchangedCredential:
        """Run the bounded counterparty call over an already-opened payload."""

        try:
            async with asyncio.timeout(self.exchange_timeout.total_seconds()):
                return await self.exchanger.exchange(
                    ref,
                    refresh_token=str(payload.get("refresh_token", "")),
                    metadata=self._metadata(payload),
                )

        except TimeoutError as e:
            raise exc.infrastructure(
                f"Credential exchange for {ref.path!r} exceeded {self.exchange_timeout}.",
                code=CREDENTIAL_EXCHANGE_TIMEOUT_CODE,
                details={"ref": ref.path},
            ) from e

    # ....................... #

    async def get(self, ref: SecretRef) -> RotatingCredential:
        tenant_id, tenant = self._tenant_scope()
        coll = await self._collection()

        async with self.client.detached():
            doc = self._guard_live(
                await self.client.find_one(coll, {"_id": self._doc_id(tenant, ref)}), ref
            )

        return self._view(doc, await self._open(doc, ref, tenant_id))

    # ....................... #

    async def refresh(self, ref: SecretRef, *, observed: SecretVersion) -> RotatingCredential:
        tenant_id, tenant = self._tenant_scope()

        async with self._locks.for_key(f"{tenant}|{ref.path}"):
            # Shielded, because the leased section must not be abandoned once the token may
            # be in the counterparty's hands. A cancellation delivered here would otherwise
            # unwind while the lease is still held and the document still looks refreshable
            # at the version a waiting worker holds — a race no after-the-fact cleanup can
            # win. Shielding lets the section run to its own bounded end (it writes either
            # the new credential or the mark that the old one is spent) while the caller
            # still sees the cancellation immediately.
            rotation = asyncio.ensure_future(
                self._rotate_under_lease(tenant_id, tenant, ref, observed)
            )

            try:
                return await asyncio.shield(rotation)

            except asyncio.CancelledError:
                # It keeps running; make sure its outcome is collected rather than surfacing
                # later as an unretrieved-exception warning.
                rotation.add_done_callback(_discard_outcome)

                raise

    # ....................... #

    async def _await_lease(
        self,
        coll: AsyncCollection[JsonDict],
        doc_id: str,
        ref: SecretRef,
        observed: SecretVersion,
        owner: str,
        tenant_id: UUID | None,
    ) -> JsonDict | RotatingCredential:
        """Hold out for the lease, or converge on the worker that already has it.

        Returns the leased document — or, when the holder finished while this call waited,
        the caller-facing view it should return instead:

        - the lease is free (or expired) and taken here — the caller proceeds;
        - it was held and its holder is still working — wait, up to one lease;
        - it was held, and meanwhile the version moved past *observed* — return the winner's
          document without waiting for whoever holds the lease now.

        That third outcome carries no correctness of its own: a caller that waited it out
        would take the lease and converge on the same document through the single-flight
        check under it. What it prevents is a *spurious* failure — a busy grant whose lease
        passes from one worker to the next can outlast this wait, and answering from a
        version that already moved beats erroring when the answer is sitting there.

        Bounded by the lease's own duration, because that is how long a holder can legally
        take. Waiting longer would mean waiting on a lease that is already stealable, which
        the next attempt does anyway.
        """

        deadline = asyncio.get_running_loop().time() + self._lease_bound.total_seconds()

        while True:
            leased = await self._take_lease(coll, doc_id, owner, utcnow())

            if leased is not None:
                return leased

            # A claim that matched nothing is either a held lease or no document at all, and
            # the read is what separates them: ``_guard_live`` answers not-found rather than
            # polling until the deadline for a grant that will never appear. It also lets a
            # waiter learn the holder burnt the grant without waiting for the lease to clear.
            current = self._guard_live(await self.client.find_one(coll, {"_id": doc_id}), ref)

            if SecretVersion(str(current["version"])) != observed:
                # The holder settled while we queued: converge on it rather than waiting out
                # a lease that is about to be released anyway.
                return self._view(current, await self._open(current, ref, tenant_id))

            if asyncio.get_running_loop().time() > deadline:
                raise exc.infrastructure(
                    f"Rotating credential at {ref.path!r} stayed leased for longer than "
                    f"{self._lease_bound}; another worker's exchange has not settled.",
                    details={"ref": ref.path},
                )

            await asyncio.sleep(_LEASE_POLL)

    # ....................... #

    async def _rotate_under_lease(
        self,
        tenant_id: UUID | None,
        tenant: str,
        ref: SecretRef,
        observed: SecretVersion,
    ) -> RotatingCredential:
        """Exchange and write under the lease, classifying every way it can end.

        The non-happy endings are genuinely different and must not be collapsed: a *stale*
        caller converges silently, a *dead grant* records its burn notice and then raises,
        and anything that goes wrong *after the token was presented* leaves a credential that
        must never be presented again.

        That last one is the invariant the whole method is arranged around. A refresh token
        is single-use, so the moment it reaches the counterparty it is spent-or-unknown, and
        releasing the lease over a document that still looks live hides exactly that. Every
        way of losing the outcome — a write that failed after a successful exchange, a
        timeout, a cancellation, and a lease inherited from a holder that had already
        presented — therefore ends the same way, with the document marked unusable.

        The lease is released in a ``finally`` rather than on each branch, and the release is
        fenced on this call's own ownership — so on the paths whose terminal write already
        cleared the lease it matches nothing and costs one no-op, and on the ones that did not
        (a recorded burn notice, a converged read, a guard that refused) it is what hands the
        document to a waiter immediately. No exit can strand a grant behind a lease until it
        expires, including exits a later edit adds.
        """

        coll = await self._collection()
        doc_id = self._doc_id(tenant, ref)
        owner = str(uuid7())
        presented = False

        async with self.client.detached():
            leased = await self._await_lease(coll, doc_id, ref, observed, owner, tenant_id)

            if isinstance(leased, RotatingCredential):
                return leased

            locked_version = int(str(leased["version"]))

            try:
                # The lease is taken before the document is judged, so a burnt grant is
                # answered here rather than by a pre-read costing every rotation a round trip
                # to say what the claim is about to say anyway. The lease it briefly holds
                # over a dead grant is released on the way out.
                self._guard_live(leased, ref)

                if leased.get("presented"):
                    # A previous holder showed the token and never recorded what came back —
                    # its lease expired mid-flight. Exchanging now would replay a token the
                    # counterparty may have consumed, and reuse revokes the whole grant
                    # family, so the grant is spent and says so.
                    log.critical(
                        "rotating credential inherited from an exchange that never settled",
                        ref=ref.path,
                    )
                    await self._write_poison(
                        coll,
                        doc_id,
                        ref,
                        reason=(
                            "a previous exchange left its lease expired with the token "
                            "already presented"
                        ),
                        version=locked_version,
                        owner=owner,
                    )

                    raise exc.precondition(
                        f"Grant at {ref.path!r} is burnt and needs re-authorization: an "
                        "earlier exchange presented its token and never recorded the result.",
                        code=BURNT_CREDENTIAL_CODE,
                        details={"ref": ref.path},
                    )

                if SecretVersion(str(locked_version)) != observed:
                    # Single-flight: the version moved before we took the lease, so another
                    # worker already exchanged. Presenting the stored token again would be
                    # reuse, and reuse detection can revoke the whole family.
                    return self._view(leased, await self._open(leased, ref, tenant_id))

                payload = await self._open(leased, ref, tenant_id)

                # Recorded before the call and only once the record landed: if this write
                # fails the token never left, so the release below is the right ending.
                await self._mark_presented(coll, doc_id, ref, owner)
                presented = True

                credential = await self._exchange(ref, payload)

                # Inside the ``try``, and deliberately: the fenced write needs the lease this
                # call still owns, so it cannot wait for the release below.
                return await self._commit(
                    coll,
                    doc_id,
                    ref,
                    credential,
                    version=locked_version,
                    owner=owner,
                    tenant_id=tenant_id,
                )

            except asyncio.CancelledError:
                # Cancellation is not an Exception, so a handler for the rest does nothing
                # here — and a shutdown landing mid-exchange is the ordinary way it happens.
                if presented:
                    log.critical(
                        "rotating credential left unusable by a cancelled exchange",
                        ref=ref.path,
                    )
                    # Shielded: the write has to survive the cancellation that prompted it.
                    await asyncio.shield(
                        self._write_poison(
                            coll,
                            doc_id,
                            ref,
                            reason="exchange was cancelled with the token already presented",
                            version=locked_version,
                            owner=owner,
                        )
                    )

                raise

            except CoreException as e:
                if e.code == INVALID_GRANT_CODE:
                    # The grant is dead, not merely unusable: record the notice and let the
                    # release below hand the (now burnt) document straight to any waiter.
                    await self._mark_burnt(coll, doc_id, tenant, ref, e.summary)

                    raise exc.precondition(
                        f"Counterparty permanently rejected the grant at {ref.path!r}; "
                        f"re-authorization required: {e.summary}",
                        code=BURNT_CREDENTIAL_CODE,
                        details={"ref": ref.path},
                    ) from e

                if e.code == CREDENTIAL_EXCHANGE_TIMEOUT_CODE:
                    # Presented, no answer: transient for the network, terminal for this
                    # credential.
                    log.critical(
                        "rotating credential left unusable by an ambiguous exchange",
                        ref=ref.path,
                        error=str(e),
                    )
                    await self._write_poison(
                        coll,
                        doc_id,
                        ref,
                        reason="exchange timed out with the token already presented",
                        version=locked_version,
                        owner=owner,
                    )

                raise

            finally:
                # Fenced on this call's ownership, so a terminal write that already cleared
                # the lease — or a re-authorization that superseded it — is untouched. It
                # also clears ``presented``, which is what keeps a transient failure (the
                # exchanger's own classification: the request never reached the counterparty)
                # from looking like a lost outcome to the next worker.
                await asyncio.shield(self._release_lease(coll, doc_id, owner))

    # ....................... #

    async def _commit(
        self,
        coll: AsyncCollection[JsonDict],
        doc_id: str,
        ref: SecretRef,
        credential: ExchangedCredential,
        *,
        version: int,
        owner: str,
        tenant_id: UUID | None,
    ) -> RotatingCredential:
        """Make the replacement durable, or report it lost — there is no third answer.

        The exchange already happened, so a write that fails or is fenced out has destroyed
        the grant: the presented token is burned at the counterparty and this frame holds the
        only copy of what replaced it. Say exactly that — a generic storage error would read
        as retryable, and no retry can help.
        """

        try:
            matched = await self._persist(
                coll,
                doc_id,
                ref,
                credential,
                version=version,
                owner=owner,
                tenant_id=tenant_id,
            )
            failure: Exception | None = None

        except Exception as e:
            matched, failure = 0, e

        if matched:
            # Built from the credential just written, not from a document read back: the
            # stored payload is sealed, and re-opening it would decrypt what this frame
            # already holds in the clear.
            return self._view_of(credential, version + 1)

        log.critical(
            "rotating credential lost after a successful exchange",
            ref=ref.path,
            error=str(failure) if failure is not None else "the fenced write matched nothing",
        )
        await self._write_poison(
            coll,
            doc_id,
            ref,
            reason="exchange succeeded but its replacement could not be stored",
            version=version,
            owner=owner,
        )

        raise exc.internal(
            f"Exchanged credential for {ref.path!r} could not be stored; the presented "
            "token is already burned, so this grant needs re-authorization.",
            code=CREDENTIAL_PERSIST_LOST_CODE,
            details={"ref": ref.path},
        ) from failure

    # ....................... #

    async def put(self, ref: SecretRef, credential: ExchangedCredential) -> RotatingCredential:
        tenant_id, tenant = self._tenant_scope()
        coll = await self._collection()
        doc_id = self._doc_id(tenant, ref)
        now = utcnow()
        payload: JsonDict = await self._seal(
            {
                "access_token": credential.access_token,
                "refresh_token": credential.refresh_token,
                "metadata": {str(key): str(value) for key, value in credential.metadata.items()},
            },
            ref,
            tenant_id,
        )

        async with self._locks.for_key(f"{tenant}|{ref.path}"), self.client.detached():
            # Unconditional, and it clears the lease: a human has just proven possession of a
            # new grant, so there is no earlier version — and no rotation in flight — worth
            # defending. A rotation this overtakes finds its own fenced write matched nothing
            # and reports its replacement lost, which is the truth: the re-authorization won.
            #
            # ``$inc`` rather than read-then-write: the version advances atomically, so two
            # concurrent re-authorizations cannot land on the same one.
            stored = await self.client.find_one_and_update(
                coll,
                {"_id": doc_id},
                {
                    "$set": {
                        "payload": payload,
                        "expires_at": credential.expires_at,
                        "burnt_reason": None,
                        "updated_at": now,
                        "lease_until": None,
                        "lease_owner": None,
                        "presented": None,
                    },
                    "$inc": {"version": 1},
                    "$setOnInsert": {
                        "tenant_id": tenant,
                        "ref": ref.path,
                        "created_at": now,
                    },
                },
                upsert=True,
            )

        if stored is None:  # pragma: no cover — an upsert returning AFTER always yields one
            raise exc.internal(f"Rotating credential upsert at {ref.path!r} returned no document.")

        return self._view_of(credential, int(str(stored["version"])))

    # ....................... #

    async def burn(self, ref: SecretRef, *, reason: str) -> None:
        _, tenant = self._tenant_scope()
        coll = await self._collection()

        async with self._locks.for_key(f"{tenant}|{ref.path}"), self.client.detached():
            await self._mark_burnt(coll, self._doc_id(tenant, ref), tenant, ref, reason)


# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoRotatingCredentialsAdmin(_MongoRotatingBase, RotatingCredentialsAdminPort):
    """:class:`RotatingCredentialsAdminPort` over the same collection as the store.

    Control plane only: this adapter never opens ``payload``, so it works identically over
    sealed and plaintext documents and cannot leak a token — the scan reads scheduling fields
    (``ref``, ``version``, ``burnt_reason``, ``updated_at``) and nothing else.

    The scan filters and orders on ``updated_at`` within a tenant, which ``_id`` cannot
    answer, so the documented index exists for this port::

        db.<collection>.createIndex({tenant_id: 1, updated_at: 1})

    ``tenant_id`` leads it because the scan is always tenant-scoped — a fleet sweep is one
    scan per tenant, so the index prefix matches every query this port ever issues.
    """

    async def due_for_refresh(
        self,
        *,
        idle_since: datetime,
        limit: int,
    ) -> Sequence[DueCredential]:
        if limit < 1:
            raise exc.precondition(
                f"due_for_refresh limit must be positive, got {limit}.",
            )

        _, tenant = self._tenant_scope()
        coll = await self._collection()

        async with self.client.detached():
            docs = await self.client.find_many(
                coll,
                {"tenant_id": tenant, "updated_at": {"$lt": idle_since}},
                projection={"ref": 1, "version": 1, "burnt_reason": 1, "updated_at": 1},
                sort=[("updated_at", 1)],
                limit=limit,
            )

        return [
            DueCredential(
                ref=SecretRef(path=str(doc["ref"])),
                version=SecretVersion(str(doc["version"])),
                last_exchanged_at=cast("datetime", doc["updated_at"]),
                burnt_reason=cast("str | None", doc.get("burnt_reason")),
            )
            for doc in docs
        ]
