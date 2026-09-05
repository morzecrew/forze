"""Mongo durable-run store (run instances + lease-based crash recovery claims)."""

from __future__ import annotations

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any, final
from uuid import UUID

import attrs
from pymongo.asynchronous.collection import AsyncCollection

from forze.application.contracts.crypto import BytesCipherPort
from forze.application.contracts.durable.function import (
    DurableLeaseRenewal,
    DurableRunAdminPort,
    DurableRunControlAware,
    DurableRunControlCapabilities,
    DurableRunPage,
    DurableRunRecord,
    DurableRunStatus,
    DurableRunStorePort,
    build_run_page,
    decode_run_cursor,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.application.integrations.crypto.payload import (
    decrypt_payload,
    encrypt_payload,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, utcnow, uuid4, uuid7
from forze_mongo.adapters.durable.function_step import DURABLE_PAYLOAD_DOMAIN
from forze_mongo.execution.deps.configs.durable import MongoDurableRunConfig
from forze_mongo.kernel.client import MongoClientPort
from forze_mongo.kernel.relation import resolve_mongo_collection

# ----------------------- #

_PENDING = DurableRunStatus.PENDING.value
_RUNNING = DurableRunStatus.RUNNING.value


def _scope_idem(idempotency_key: str | None, tenant_id: UUID | None) -> str | None:
    """Namespace an idempotency key under its tenant for the stored ``idempotency_key``.

    A shared **tagged** collection carries every tenant's runs, so prefixing the key with
    the tenant scopes convergence per tenant: two tenants reusing one key (e.g. a
    scheduler's ``{schedule_id}:{fire_epoch}``) stay distinct runs. Single-tenant keys
    (``tenant_id is None``) are stored verbatim, and a ``None`` key is never namespaced —
    no idempotency was asked for, so nothing converges.
    """

    if idempotency_key is None or tenant_id is None:
        return idempotency_key

    return f"{tenant_id}:{idempotency_key}"


def _unscope_idem(stored: str | None, tenant_id: UUID | None) -> str | None:
    """Strip the tenant prefix :func:`_scope_idem` added, so a record surfaces the key the
    caller passed (the fixed-width ``{uuid}:`` prefix makes the strip exact)."""

    if stored is None or tenant_id is None:
        return stored

    prefix = f"{tenant_id}:"

    return stored[len(prefix) :] if stored.startswith(prefix) else stored


def _as_uuid(value: Any) -> UUID | None:
    return None if value is None else (value if isinstance(value, UUID) else UUID(str(value)))


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoDurableRunStore(
    TenancyMixin,
    DurableRunStorePort,
    DurableRunAdminPort,
    DurableRunControlAware,
):
    """Mongo-backed durable-run store.

    Records run instances and hands out claims for execution and crash recovery. A crashed
    run is left ``RUNNING`` with an expired lease; :meth:`claim_abandoned` re-claims it.
    Re-submits under one ``idempotency_key`` converge on a single run; the key is stored
    **tenant-scoped**, so on a shared tagged collection two tenants reusing one key stay
    distinct runs.

    **Claiming without row locks.** Postgres claims under ``FOR UPDATE SKIP LOCKED``; Mongo
    has no such thing, so a batch claim reads candidate ids, stamps them with a fresh claim
    token in one ``update_many`` — whose per-document filter still requires the candidate to
    be claimable, so exactly one scanner wins each — and reads back what it actually took.
    A contended batch may claim fewer than *limit* runs while claimable ones remain; the
    next scan catches up. Single-run :meth:`begin` uses one ``findAndModify``, which is
    atomic on its own.

    **Tenancy.** The collection is resolved under the bound tenant, so a static
    ``collection`` is a shared **tagged** collection (``tenant_id`` field) and a per-tenant
    resolver is a **namespace** collection. Recovery either runs unbound over a tagged
    collection (claims every tenant's runs; the runner re-binds each run's tenant to execute
    it) or per-tenant over a namespace collection. Non-enforcing: an unbound scan never
    fails, and a bound scan claims only that tenant's runs.

    Documents look like ``{_id, name, status, idempotency_key, input, output, error,
    tenant_id, attempts, leased_until, available_at, created_at, updated_at,
    cancel_requested_at, cancel_refused_at, claim_token}``, where ``_id`` is the ``run_id``.

    The application owns the indexes; two are worth creating::

        // Idempotency convergence. **Required for correctness under concurrency**: without
        // it two simultaneous enqueues of one key can both insert, and the port promises
        // one run. Partial, because a run without a key must not collide with another.
        db.<collection>.createIndex(
            {idempotency_key: 1},
            {unique: true, partialFilterExpression: {idempotency_key: {$type: "string"}}},
        )

        // Recovery scan + admin listing. Recommended, not required: without it the scan
        // collection-scans as the collection grows.
        db.<collection>.createIndex({status: 1, created_at: 1})
        db.<collection>.createIndex({created_at: -1, _id: -1})

    ``attempts`` doubles as the fence token (a claim advances it), so a terminal write can
    be fenced against a reclaimed lease — the store is multi-worker-safe, not just
    single-leader.
    """

    client: MongoClientPort
    config: MongoDurableRunConfig
    cipher: BytesCipherPort | None = None

    # ....................... #

    async def _collection(self) -> AsyncCollection[JsonDict]:
        db_name, coll_name = await resolve_mongo_collection(
            self.config.collection,
            self._tenant_id_for_resolve(),
        )
        return await self.client.collection(coll_name, db_name=db_name)

    # ....................... #

    def _tenant_filter(self) -> dict[str, Any]:
        """Scope a scan or an admin write to the bound tenant, when one is bound.

        Unbound it is empty, which is what lets recovery sweep every tenant's runs on a
        tagged collection. On a namespace collection the resolved collection is already
        per-tenant, so the filter is a redundant no-op rather than a second gate.
        """

        tenant_id = self._tenant_id_for_resolve()

        return {} if tenant_id is None else {"tenant_id": str(tenant_id)}

    # ....................... #

    async def enqueue(
        self,
        name: str,
        *,
        input_json: JsonDict | None,
        idempotency_key: str | None = None,
        tenant_id: UUID | None = None,
        available_at: datetime | None = None,
    ) -> DurableRunRecord:
        # Default the tenant field to the bound tenant so a run enqueued under a namespace
        # binding still tags its tenant (the recovery filter matches on it).
        tenant_id = tenant_id if tenant_id is not None else self._tenant_id_for_resolve()
        coll = await self._collection()
        run_id = str(uuid7())
        now = utcnow()
        stored_input = await self._seal(input_json, run_id, "input", tenant_id)
        stored_idem = _scope_idem(idempotency_key, tenant_id)

        document: dict[str, Any] = {
            "_id": run_id,
            "name": name,
            "status": _PENDING,
            "idempotency_key": stored_idem,
            "input": stored_input,
            "output": None,
            "error": None,
            "tenant_id": str(tenant_id) if tenant_id is not None else None,
            "attempts": 0,
            "leased_until": None,
            "available_at": available_at,
            "created_at": now,
            "updated_at": now,
            "cancel_requested_at": None,
            "cancel_refused_at": None,
        }

        if stored_idem is None:
            # No key, nothing to converge on: a plain insert, and every call is its own run.
            await self.client.insert_one(coll, document)

            return self._record_from_document(document, input_json=input_json)

        # Claim-or-read in one atomic step on the *idempotency key*, not the run id: the
        # upsert filter is the key, so a second enqueue under it never inserts and comes
        # back holding the first run. That is what makes convergence hold without relying on
        # the unique index to raise — the index is the backstop for two writers racing this
        # same statement, which is why it is required rather than recommended.
        existing = await self.client.find_one_and_update(
            coll,
            {"idempotency_key": stored_idem},
            {"$setOnInsert": document},
            upsert=True,
        )

        if existing is None:  # pragma: no cover - the driver always returns the document
            raise exc.internal("Durable run enqueue returned no document.")

        if existing["_id"] == run_id:
            return self._record_from_document(document, input_json=input_json)

        return await self._record_from_row(existing)

    # ....................... #

    async def begin(
        self,
        run_id: str,
        *,
        lease_for: timedelta,
    ) -> DurableRunRecord | None:
        coll = await self._collection()
        now = utcnow()

        # One atomic findAndModify: the ``status = pending`` predicate is re-checked by the
        # server as it writes, so two workers racing the same run cannot both claim it.
        doc = await self.client.find_one_and_update(
            coll,
            {"_id": run_id, "status": _PENDING},
            {
                "$set": {
                    "status": _RUNNING,
                    "leased_until": now + lease_for,
                    "updated_at": now,
                },
                "$inc": {"attempts": 1},
            },
        )

        return None if doc is None else await self._record_from_row(doc)

    # ....................... #

    async def claim_abandoned(
        self,
        *,
        limit: int,
        lease_for: timedelta,
    ) -> Sequence[DurableRunRecord]:
        coll = await self._collection()
        now = utcnow()
        claimable: dict[str, Any] = {
            "$or": [
                {
                    "status": _PENDING,
                    "$or": [{"available_at": None}, {"available_at": {"$lte": now}}],
                },
                {
                    "status": _RUNNING,
                    "$or": [{"leased_until": None}, {"leased_until": {"$lte": now}}],
                },
            ],
            **self._tenant_filter(),
        }

        candidates = await self.client.find_many(
            coll,
            claimable,
            projection={"_id": 1},
            sort=[("created_at", 1)],
            limit=limit,
        )

        if not candidates:
            return []

        # ``update_many`` re-evaluates the claimable predicate per document as it writes, so
        # each run is claimed by exactly one scanner even though the candidate read was not
        # locked. A contended batch takes fewer than *limit*; the next scan catches up.
        claim_token = uuid4().hex
        claimed = await self.client.update_many(
            coll,
            {"_id": {"$in": [doc["_id"] for doc in candidates]}, **claimable},
            {
                "$set": {
                    "status": _RUNNING,
                    "leased_until": now + lease_for,
                    "updated_at": now,
                    "claim_token": claim_token,
                },
                "$inc": {"attempts": 1},
            },
        )

        if claimed == 0:
            return []

        # Read back by this batch's own token rather than by the candidate ids: the ids
        # include the runs another scanner took, and a stale token left on a row by an
        # earlier batch cannot match a token minted for this one.
        rows = await self.client.find_many(
            coll,
            {"claim_token": claim_token},
            sort=[("created_at", 1)],
        )

        return [await self._record_from_row(row) for row in rows]

    # ....................... #

    async def renew(
        self,
        run_id: str,
        *,
        lease_for: timedelta,
        fence: int,
    ) -> DurableLeaseRenewal:
        coll = await self._collection()
        now = utcnow()

        # Fenced on ``attempts = fence`` (and ``status = running``), mirroring ``_finish``:
        # the lease moves forward only while this worker is still the current claim holder.
        # If a recovery scan reclaimed the run its ``attempts`` advanced, no document
        # matches, and the caller learns it must stop.
        #
        # The cancel stamp rides back on the same document, so observing a cancel request
        # costs the heartbeat the body already makes rather than a second polling loop.
        doc = await self.client.find_one_and_update(
            coll,
            {"_id": run_id, "status": _RUNNING, "attempts": fence},
            {"$set": {"leased_until": now + lease_for, "updated_at": now}},
        )

        if doc is None:
            return DurableLeaseRenewal(held=False)

        return DurableLeaseRenewal(
            held=True,
            cancel_requested=doc.get("cancel_requested_at") is not None,
        )

    # ....................... #

    async def complete(
        self,
        run_id: str,
        *,
        output_json: JsonDict | None,
        fence: int | None = None,
    ) -> None:
        # Seal under the bound tenant so the output AAD matches what ``_record_from_row``
        # reconstructs on load: the runner binds the run's tenant before completing it
        # (mirrors the input seal in ``enqueue``).
        stored = await self._seal(output_json, run_id, "output", self._tenant_id_for_resolve())
        await self._finish(
            run_id,
            status=DurableRunStatus.COMPLETED,
            output=stored,
            error=None,
            fence=fence,
        )

    # ....................... #

    async def fail(self, run_id: str, *, error: str, fence: int | None = None) -> None:
        await self._finish(
            run_id,
            status=DurableRunStatus.FAILED,
            output=None,
            error=error,
            fence=fence,
        )

    # ....................... #

    async def mark_forward_incomplete(
        self, run_id: str, *, error: str, fence: int | None = None
    ) -> None:
        await self._finish(
            run_id,
            status=DurableRunStatus.FORWARD_INCOMPLETE,
            output=None,
            error=error,
            fence=fence,
        )

    # ....................... #

    async def mark_cancelled(
        self, run_id: str, *, error: str | None = None, fence: int | None = None
    ) -> None:
        await self._finish(
            run_id,
            status=DurableRunStatus.CANCELLED,
            output=None,
            error=error,
            fence=fence,
        )

    # ....................... #

    async def mark_timed_out(self, run_id: str, *, error: str, fence: int | None = None) -> None:
        await self._finish(
            run_id,
            status=DurableRunStatus.TIMED_OUT,
            output=None,
            error=error,
            fence=fence,
        )

    # ....................... #

    async def refuse_cancel(self, run_id: str, *, fence: int | None = None) -> None:
        coll = await self._collection()
        now = utcnow()

        # Fenced but NOT guarded on ``status = running`` (unlike ``_finish``): a refusal
        # records what happened to the *ask*, and the run it describes may already have
        # landed by the time the holder writes it down.
        #
        # Losing that guard makes this the widest write on the port — it can stamp a run in
        # any state — and ``attempts`` is a small integer that collides freely across
        # tenants, so the fence alone is thin protection here. The ask and the refusal are
        # read together by an operator, so they get the same tenant scoping.
        filter_: dict[str, Any] = {"_id": run_id, **self._tenant_filter()}

        if fence is not None:
            filter_["attempts"] = fence

        # A repeated refusal keeps the first instant: ``$setOnInsert`` cannot express that on
        # an update, so the stamp is written only where it is still absent, and a second call
        # matches nothing.
        await self.client.update_one(
            coll,
            {**filter_, "cancel_refused_at": None},
            {"$set": {"cancel_refused_at": now, "updated_at": now}},
        )

    # ....................... #

    async def request_cancel(self, run_id: str) -> bool:
        coll = await self._collection()
        now = utcnow()
        scope = self._tenant_filter()

        # A PENDING run has no holder to wait for and no fence to respect, so it lands
        # CANCELLED here and the recovery scan never picks it up. Two statements rather than
        # Postgres's one CASE: the state a document lands in differs by the state it is in,
        # and Mongo has no conditional assignment. The PENDING arm runs first so a run that
        # slips from PENDING to RUNNING between them is stamped by the second — the reverse
        # order could leave a run cancelled *and* unstamped.
        landed = await self.client.update_one(
            coll,
            {"_id": run_id, "status": _PENDING, **scope},
            {
                "$set": {
                    "status": DurableRunStatus.CANCELLED.value,
                    "leased_until": None,
                    "cancel_requested_at": now,
                    "updated_at": now,
                }
            },
        )

        if landed:
            return True

        # A RUNNING run only gets the stamp; its holder lands the terminal state under its
        # fence. The ``cancel_requested_at: None`` guard keeps a repeated ask on the first
        # instant, and a run already asked about still answers ``True`` — the ask stands.
        stamped = await self.client.update_one(
            coll,
            {"_id": run_id, "status": _RUNNING, "cancel_requested_at": None, **scope},
            {"$set": {"cancel_requested_at": now, "updated_at": now}},
        )

        if stamped:
            return True

        # Nothing was written: either the run is terminal (or absent, or another tenant's),
        # or it is RUNNING and already carries the stamp. Only the second is a live ask.
        existing = await self.client.find_one(
            coll,
            {"_id": run_id, "status": _RUNNING, **scope},
            projection={"_id": 1},
        )

        return existing is not None

    # ....................... #

    def control_capabilities(self) -> DurableRunControlCapabilities:
        return DurableRunControlCapabilities(supports_cancel=True)

    # ....................... #

    async def load(self, run_id: str) -> DurableRunRecord | None:
        coll = await self._collection()
        doc = await self.client.find_one(coll, {"_id": run_id})

        return None if doc is None else await self._record_from_row(doc)

    # ....................... #

    async def list_runs(
        self,
        *,
        status: DurableRunStatus | None = None,
        name: str | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> DurableRunPage:
        if limit < 1:
            raise exc.validation("Durable run list limit must be >= 1.")

        coll = await self._collection()
        filter_: dict[str, Any] = dict(self._tenant_filter())

        if status is not None:
            filter_["status"] = str(status)

        if name is not None:
            filter_["name"] = name

        if cursor is not None:
            # Keyset seek past the last row of the previous page, in the same
            # (created_at DESC, run_id DESC) order the sort imposes. Mongo has no row-value
            # comparison, so the pair is spelled out: strictly older, or the same instant
            # with a smaller id.
            cursor_ts, cursor_id = decode_run_cursor(cursor)
            filter_["$or"] = [
                {"created_at": {"$lt": cursor_ts}},
                {"created_at": cursor_ts, "_id": {"$lt": cursor_id}},
            ]

        # Over-fetch one document so build_run_page can tell whether an older page follows
        # without a second round trip.
        rows = await self.client.find_many(
            coll,
            filter_,
            sort=[("created_at", -1), ("_id", -1)],
            limit=limit + 1,
        )

        records = [await self._record_from_row(row) for row in rows]

        return build_run_page(records, limit)

    # ....................... #

    async def _finish(
        self,
        run_id: str,
        *,
        status: DurableRunStatus,
        output: JsonDict | None,
        error: str | None,
        fence: int | None = None,
    ) -> None:
        coll = await self._collection()
        now = utcnow()

        # Guarded on ``status = running`` so a terminal state is not overwritten and a
        # duplicate/late completion is a no-op (idempotent under recovery re-invocation).
        # When *fence* is given, also require it to match ``attempts`` so a stale worker
        # whose lease was reclaimed cannot finish the run.
        filter_: dict[str, Any] = {"_id": run_id, "status": _RUNNING}

        if fence is not None:
            filter_["attempts"] = fence

        await self.client.update_one(
            coll,
            filter_,
            {
                "$set": {
                    "status": status.value,
                    "output": output,
                    "error": error,
                    "leased_until": None,
                    "updated_at": now,
                }
            },
        )

    # ....................... #

    def _record_from_document(
        self,
        document: dict[str, Any],
        *,
        input_json: JsonDict | None,
    ) -> DurableRunRecord:
        """Build the record for a document this call just wrote, with its plaintext input.

        Kept apart from :meth:`_record_from_row` so a fresh enqueue returns the input the
        caller handed over rather than a decrypt of what was just sealed.
        """

        return DurableRunRecord(
            run_id=str(document["_id"]),
            name=str(document["name"]),
            status=DurableRunStatus(document["status"]),
            idempotency_key=_unscope_idem(
                document["idempotency_key"], _as_uuid(document["tenant_id"])
            ),
            input_json=input_json,
            tenant_id=_as_uuid(document["tenant_id"]),
            attempts=int(document["attempts"]),
            available_at=document["available_at"],
            created_at=document["created_at"],
        )

    # ....................... #

    async def _record_from_row(self, row: JsonDict) -> DurableRunRecord:
        tenant_id = _as_uuid(row.get("tenant_id"))
        run_id = str(row["_id"])
        input_json = await self._unseal(row.get("input"), run_id, "input", tenant_id)
        output_json = await self._unseal(row.get("output"), run_id, "output", tenant_id)

        return DurableRunRecord(
            run_id=run_id,
            name=str(row["name"]),
            status=DurableRunStatus(row["status"]),
            idempotency_key=_unscope_idem(row.get("idempotency_key"), tenant_id),
            input_json=input_json,
            output_json=output_json,
            error=row.get("error"),
            tenant_id=tenant_id,
            attempts=int(row.get("attempts") or 0),
            available_at=row.get("available_at"),
            created_at=row.get("created_at"),
            cancel_requested_at=row.get("cancel_requested_at"),
            cancel_refused_at=row.get("cancel_refused_at"),
        )

    # ....................... #

    async def _seal(
        self,
        payload: JsonDict | None,
        run_id: str,
        slot: str,
        tenant_id: UUID | None,
    ) -> JsonDict | None:
        if payload is None or self.cipher is None:
            return payload

        return await encrypt_payload(
            self.cipher,
            payload,
            domain=DURABLE_PAYLOAD_DOMAIN,
            tenant_id=tenant_id,
            record_id=f"{run_id}:{slot}",
        )

    # ....................... #

    async def _unseal(
        self,
        raw: JsonDict | None,
        run_id: str,
        slot: str,
        tenant_id: UUID | None,
    ) -> JsonDict | None:
        if raw is None:
            return None

        return await decrypt_payload(
            self.cipher,
            raw,
            domain=DURABLE_PAYLOAD_DOMAIN,
            tenant_id=tenant_id,
            record_id=f"{run_id}:{slot}",
        )
