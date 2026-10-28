"""Mongo durable-run store (run instances + lease-based crash recovery claims)."""

from __future__ import annotations

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from collections.abc import Collection, Sequence
from datetime import datetime, timedelta
from typing import Any, final
from uuid import UUID

import attrs
from pymongo.asynchronous.collection import AsyncCollection

from forze.application.contracts.crypto import BytesCipherPort
from forze.application.contracts.durable.function import (
    PURGEABLE_STATUSES,
    DurableLeaseRenewal,
    DurableRunAdminPort,
    DurableRunControlAware,
    DurableRunControlCapabilities,
    DurableRunPage,
    DurableRunRecord,
    DurableRunStatus,
    DurableRunStorePort,
    build_run_page,
    check_enqueue_keys,
    check_purge,
    decode_run_cursor,
    requeue_failed_unnamed_error,
    requeue_unscoped_error,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.application.integrations.crypto.payload import (
    decrypt_payload,
    encrypt_payload,
    is_payload_cipher_missing,
)
from forze.application.integrations.durable import (
    is_passing_fault,
    quarantines_unopenable,
    unopenable_hold,
)
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.primitives import JsonDict, utcnow, uuid4, uuid7
from forze_mongo.adapters._logger import logger
from forze_mongo.adapters.durable._ids import as_uuid, scope_id, unscope_id
from forze_mongo.adapters.durable.function_step import DURABLE_PAYLOAD_DOMAIN
from forze_mongo.execution.deps.configs.durable import (
    MongoDurableRunConfig,
    MongoDurableStepConfig,
)
from forze_mongo.kernel.client import MongoClientPort
from forze_mongo.kernel.relation import resolve_mongo_collection

# ----------------------- #


def _unscope_key(stored: str | None, tenant_id: UUID | None) -> str | None:
    """:func:`unscope_id` over an optional key — ``None`` means no idempotency was asked
    for, so there is nothing scoped and nothing to strip."""

    return None if stored is None else unscope_id(stored, tenant_id)


# ....................... #

_PENDING = DurableRunStatus.PENDING.value
_RUNNING = DurableRunStatus.RUNNING.value
_QUARANTINED = DurableRunStatus.QUARANTINED.value
_REQUEUEABLE = [DurableRunStatus.FAILED.value, DurableRunStatus.TIMED_OUT.value]
"""The landed states :meth:`MongoDurableRunStore.requeue_failed` returns to ``PENDING``."""


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
    it) or per-tenant over a namespace collection. An unbound scan never fails, and a bound
    scan claims only that tenant's runs.

    What a bound caller reaches splits by verb. :meth:`begin`, :meth:`renew`, :meth:`load`,
    :meth:`mark_retry` and every terminal write reach that tenant's runs and untagged ones (see
    :meth:`_worker_scope`); :meth:`claim_abandoned`, :meth:`list_runs`,
    :meth:`request_cancel`, :meth:`requeue_quarantined`, :meth:`requeue_failed`,
    :meth:`purge_finished` and :meth:`refuse_cancel` match the tenant exactly.
    :meth:`enqueue` resolves one effective tenant for the collection, the tag and the scoped
    key together, and refuses an explicit tenant that contradicts the binding.

    Documents look like ``{_id, name, status, idempotency_key, input, output, error,
    tenant_id, attempts, leased_until, available_at, created_at, updated_at,
    cancel_requested_at, cancel_refused_at, claim_token}``, where ``_id`` is the ``run_id``.

    The application owns the indexes (see ``pages/docs/reference/durable-schema.md``).
    One is required; the rest are recommended::

        // Idempotency convergence. **Required for correctness under concurrency**: without
        // it two simultaneous enqueues of one key can both insert, and the port promises
        // one run. Partial, because a run without a key must not collide with another.
        db.<collection>.createIndex(
            {idempotency_key: 1},
            {unique: true, partialFilterExpression: {idempotency_key: {$type: "string"}}},
        )

        // Coalescing: required before enqueue(coalesce_key=...) is used, so a burst of
        // submits joins one unclaimed pending run instead of inserting several.
        db.<collection>.createIndex(
            {coalesce_key: 1},
            {
                unique: true,
                partialFilterExpression: {
                    coalesce_key: {$type: "string"},
                    status: "pending",
                    attempts: 0,
                },
            },
        )

        // Recovery scan + admin listing. Recommended, not required: without it the scan
        // collection-scans as the collection grows.
        db.<collection>.createIndex({status: 1, created_at: 1})
        db.<collection>.createIndex({created_at: -1, _id: -1})
        // On a shared tagged collection a bound scan and listing also filter `tenant_id`,
        // so lead with it to skip other tenants' runs:
        db.<collection>.createIndex({tenant_id: 1, status: 1, created_at: 1})
        db.<collection>.createIndex({tenant_id: 1, created_at: -1, _id: -1})

        // Recommended where runs are purged: purge_finished filters on status and
        // updated_at (when the run landed) and deletes the oldest landings first.
        db.<collection>.createIndex({status: 1, updated_at: 1})

        // Read-back of a batch claim, which every recovery cycle performs. Recommended for
        // the same reason as the scan itself: without it the read-back collection-scans.
        // Sparse, because only a run some scan has claimed carries a token at all.
        db.<collection>.createIndex({claim_token: 1}, {sparse: true})

    ``attempts`` doubles as the fence token (a claim advances it), so a terminal write can
    be fenced against a reclaimed lease — the store is multi-worker-safe, not just
    single-leader.
    """

    client: MongoClientPort
    config: MongoDurableRunConfig
    cipher: BytesCipherPort | None = None

    step_config: MongoDurableStepConfig | None = None
    """The step journal :meth:`purge_finished` deletes a purged run's steps from.
    ``MongoDepsModule`` passes its ``durable_step``."""

    journal: bool = True
    """Whether runs in this collection journal steps. While ``True`` and no
    :attr:`step_config` is given, :meth:`purge_finished` refuses rather than leave each purged
    run's steps behind. ``MongoDepsModule`` sets it from whether ``durable_step`` is wired."""

    # ....................... #

    async def _collection(self, tenant_id: UUID | None = None) -> AsyncCollection[JsonDict]:
        """Resolve the collection, under *tenant_id* when the caller has already settled it.

        A verb that accepts an explicit tenant passes the effective one here so the document
        lands in the collection its tag says it belongs to; every other verb resolves from
        the binding, as before.
        """

        if tenant_id is None:
            tenant_id = self._tenant_id_for_resolve()

        db_name, coll_name = await resolve_mongo_collection(self.config.collection, tenant_id)

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

    def _worker_scope(self) -> dict[str, Any]:
        """Tenant predicate for the verbs reached with a run id: :meth:`begin`,
        :meth:`renew`, :meth:`load` and every terminal write.

        Bound, a caller reaches its own tenant's runs and the untagged ones; unbound — the
        recovery and single-tenant role — it reaches everything, which is what lets one
        sweep serve every tenant.

        The null arm is load-bearing rather than lax. A run tagged with no tenant belongs to
        none, and the runner leaves the ambient binding alone for such a run, so an exact
        match would make its terminal write hit nothing: the run would sit ``running`` until
        its lease expired, be reclaimed, and re-run forever. Tightening this re-opens that
        loop. ``{"tenant_id": None}`` matches a missing field as well as a null one, which
        is the intent — a document written before the field existed is untagged.
        """

        tenant_id = self._tenant_id_for_resolve()

        if tenant_id is None:
            return {}

        return {"$or": [{"tenant_id": str(tenant_id)}, {"tenant_id": None}]}

    # ....................... #

    async def enqueue(
        self,
        name: str,
        *,
        input_json: JsonDict | None,
        idempotency_key: str | None = None,
        tenant_id: UUID | None = None,
        available_at: datetime | None = None,
        coalesce_key: str | None = None,
    ) -> DurableRunRecord:
        check_enqueue_keys(idempotency_key, coalesce_key)

        # One effective tenant for the document's field, its scoped idempotency key *and*
        # the collection it lands in — resolving the collection from the binding while
        # tagging from the argument is how a document ends up where nobody looks for it.
        tenant_id = self._effective_tenant(tenant_id)
        coll = await self._collection(tenant_id)
        run_id = str(uuid7())
        now = utcnow()
        stored_input = await self._seal(input_json, run_id, "input", tenant_id)
        stored_idem = scope_id(idempotency_key, tenant_id) if idempotency_key is not None else None

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

        if coalesce_key is not None:
            document["coalesce_key"] = scope_id(coalesce_key, tenant_id)

            return await self._enqueue_coalesced(coll, document, input_json)

        if stored_idem is None:
            # No key, nothing to converge on: a plain insert, and every call is its own run.
            await self.client.insert_one(coll, document)

            return self._record_from_document(document, input_json=input_json)

        # Claim-or-read in one atomic step on the *idempotency key*, not the run id: the
        # upsert filter is the key, so a second enqueue under it never inserts and comes
        # back holding the first run. That is what makes convergence hold without relying on
        # the unique index to raise — the index is the backstop for two writers racing this
        # same statement, which is why it is required rather than recommended.
        try:
            existing = await self.client.find_one_and_update(
                coll,
                {"idempotency_key": stored_idem},
                {"$setOnInsert": document},
                upsert=True,
            )

        except CoreException as error:
            if error.kind is not ExceptionKind.CONFLICT:
                raise

            # Two writers reached the upsert together and the unique index refused the
            # loser's insert — documented Mongo behaviour for concurrent upserts, and the
            # reason that index is required rather than advisory. Converge on the winner,
            # which is what ``ON CONFLICT DO NOTHING`` plus a re-read gives on Postgres:
            # the caller asked for idempotency and must not get an error for using it.
            existing = await self.client.find_one(coll, {"idempotency_key": stored_idem})

            if existing is None:  # pragma: no cover - the conflicting document must exist
                raise exc.internal(
                    "Durable run enqueue conflicted on idempotency_key but the existing "
                    "run could not be loaded.",
                ) from error

            return await self._record_from_row(existing)

        if existing is None:  # pragma: no cover - the driver always returns the document
            raise exc.internal("Durable run enqueue returned no document.")

        if existing["_id"] == run_id:
            return self._record_from_document(document, input_json=input_json)

        return await self._record_from_row(existing)

    # ....................... #

    async def _enqueue_coalesced(
        self,
        coll: AsyncCollection[JsonDict],
        document: dict[str, Any],
        input_json: JsonDict | None,
    ) -> DurableRunRecord:
        # Join-or-insert in one findAndModify on the unclaimed pending run for the key. A
        # claim moves that run out of the filter (and out of the partial unique index), so
        # a submit after it inserts a new run.
        waiting = {
            "coalesce_key": document["coalesce_key"],
            "status": _PENDING,
            "attempts": 0,
        }

        for attempt in range(3):
            try:
                existing = await self.client.find_one_and_update(
                    coll,
                    waiting,
                    {"$setOnInsert": document},
                    upsert=True,
                )

            except CoreException as error:
                # Two submits raced to insert and the unique index refused this one: the
                # next pass joins the winner, or inserts if a claim took it meanwhile.
                if error.kind is not ExceptionKind.CONFLICT or attempt == 2:
                    raise

                continue

            if existing is None:  # pragma: no cover - the driver always returns the document
                raise exc.internal("Durable run enqueue returned no document.")

            if existing["_id"] == document["_id"]:
                return self._record_from_document(document, input_json=input_json)

            return await self._record_from_row(existing)

        raise AssertionError("unreachable")  # pragma: no cover

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
            {"_id": run_id, "status": _PENDING, **self._worker_scope()},
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
        if limit <= 0:
            # The driver refuses a zero cursor length, where Postgres answers ``LIMIT 0``
            # with no rows and the oracle slices an empty list. A scanner computing a
            # remaining budget hits zero routinely, and it must idle rather than take the
            # recovery loop down.
            return []

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
        #
        # Crashing between the claim and this read leaves those runs RUNNING under a lease
        # nobody is serving — a window Postgres does not have, since its claim and its
        # RETURNING are one statement. It closes itself: the lease expires and the next scan
        # reclaims them, which is the same recovery path a worker that died mid-body takes.
        # The cost is one lease interval of delay, not a lost run.
        rows = await self.client.find_many(
            coll,
            {"claim_token": claim_token},
            sort=[("created_at", 1)],
        )

        return await self._open_claimed(rows, lease_for)

    # ....................... #

    async def _open_claimed(
        self,
        rows: Sequence[Any],
        lease_for: timedelta,
    ) -> list[DurableRunRecord]:
        # One by one: the claim has already leased every row, so a run whose payload cannot
        # be opened must not fail the runs claimed with it. One whose key is gone is held
        # out of the scan for longer than a lease, or it would be back at the head of the
        # next one, and quarantined once that hold has doubled to its cap; one that met a
        # passing fault keeps its lease. A missing cipher is the deployment's, not the
        # run's, and fails the scan.
        records: list[DurableRunRecord] = []

        for row in rows:
            try:
                records.append(await self._record_from_row(row))

            except Exception as error:
                if is_payload_cipher_missing(error):
                    raise

                if quarantines_unopenable(error, lease_for=lease_for, attempts=row["attempts"]):
                    logger.exception(
                        "Durable run %s could not be opened on its claim %d; quarantined, "
                        "recovery will not claim it again until it is requeued",
                        row["_id"],
                        row["attempts"],
                    )
                    # Fenced like the hold: only the claim that failed to open the run moves
                    # it, and only while it is still running under that claim.
                    await self.client.update_one(
                        await self._collection(),
                        {"_id": row["_id"], "status": _RUNNING, "attempts": row["attempts"]},
                        {
                            "$set": {
                                "status": _QUARANTINED,
                                "leased_until": None,
                                "updated_at": utcnow(),
                            }
                        },
                    )
                    continue

                hold = unopenable_hold(error, lease_for=lease_for, attempts=row["attempts"])
                logger.exception(
                    "Durable run %s was claimed for recovery but could not be opened; %s",
                    row["_id"],
                    "retried once its lease lapses"
                    if hold is None
                    else f"held out of recovery for {hold}",
                )

                if hold is None:
                    continue

                now = utcnow()
                await self.client.update_one(
                    await self._collection(),
                    {"_id": row["_id"], "status": _RUNNING, "attempts": row["attempts"]},
                    {"$set": {"leased_until": now + hold, "updated_at": now}},
                )

        return records

    # ....................... #

    async def requeue_quarantined(self, *, run_ids: Sequence[str] | None = None) -> int:
        coll = await self._collection()
        scope = self._tenant_filter()

        # Scoped like ``list_runs``. A quarantined run has no lease and no holder, so there
        # is nothing to fence against. It is made due at once: ``begin`` takes a run before
        # its ``available_at``, so one can reach quarantine still delayed.
        filter_: dict[str, Any] = {"status": _QUARANTINED, **scope}

        if run_ids is not None:
            filter_["_id"] = {"$in": list(run_ids)}

        elif not scope:
            # Unbound and unnamed, every tenant's runs would come back ahead of the fresh
            # ones, those whose key is gone for good among them. Refused while any is
            # tagged; the update then takes untagged runs only, so a run tagged and
            # quarantined after this check stays where it is.
            tagged = await self.client.find_one(
                coll,
                {"status": _QUARANTINED, "tenant_id": {"$ne": None}},
                projection={"_id": 1},
            )

            if tagged is not None:
                raise requeue_unscoped_error()

            filter_["tenant_id"] = None

        return await self._requeue(coll, filter_)

    # ....................... #

    async def requeue_failed(
        self,
        *,
        run_ids: Sequence[str] | None = None,
        name: str | None = None,
    ) -> int:
        if run_ids is None and name is None:
            raise requeue_failed_unnamed_error()

        coll = await self._collection()

        # Scoped like ``list_runs``; a landed run has no lease and no holder to fence against.
        # An unanswered cancel ask is honoured: such a run stays where it landed.
        filter_: dict[str, Any] = {
            "status": {"$in": _REQUEUEABLE},
            "$or": [{"cancel_requested_at": None}, {"cancel_refused_at": {"$ne": None}}],
            **self._tenant_filter(),
        }

        if run_ids is not None:
            filter_["_id"] = {"$in": list(run_ids)}

        if name is not None:
            filter_["name"] = name

        return await self._requeue(coll, filter_)

    # ....................... #

    async def _requeue(self, coll: AsyncCollection[JsonDict], filter_: dict[str, Any]) -> int:
        # Due at once, keeping attempts (the fence), payloads and cancel stamps.
        return await self.client.update_many(
            coll,
            filter_,
            {"$set": {"status": _PENDING, "available_at": None, "updated_at": utcnow()}},
        )

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
            {"_id": run_id, "status": _RUNNING, "attempts": fence, **self._worker_scope()},
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
        # Seal under the run's *stored* tenant, read here rather than taken from the
        # binding: ``_record_from_row`` unseals with the field, and the two must agree or a
        # completed run is one nothing can open. They differ for exactly the case
        # ``_worker_scope`` keeps reachable — an untagged run finished under a binding.
        stored = await self._seal(output_json, run_id, "output", await self._stored_tenant(run_id))
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

    async def mark_retry(
        self,
        run_id: str,
        *,
        error: str,
        available_at: datetime,
        fence: int | None = None,
    ) -> None:
        await self._finish(
            run_id,
            status=DurableRunStatus.PENDING,
            output=None,
            error=error,
            fence=fence,
            available_at=available_at,
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

        # A PENDING run never claimed has no holder to wait for and no journal to decide by,
        # so it lands CANCELLED here and the recovery scan never picks it up. A document
        # with no ``attempts`` was never claimed either, as ``_record_from_row`` reads it. Two statements
        # rather than Postgres's one CASE: the state a document lands in differs by the
        # state it is in, and Mongo has no conditional assignment. The PENDING arm runs
        # first so a run that slips from PENDING to RUNNING between them is stamped by the
        # second — the reverse order could leave a run cancelled *and* unstamped.
        landed = await self.client.update_one(
            coll,
            {"_id": run_id, "status": _PENDING, "attempts": {"$in": [0, None]}, **scope},
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

        # Every other live run only gets the stamp: a RUNNING one's holder lands the terminal
        # state under its fence, and a QUARANTINED one, or a PENDING one requeued from it,
        # may be a saga past its pivot, decided once a claim can read it. The
        # ``cancel_requested_at: None`` guard keeps a repeated ask on the first instant, and
        # a run already asked about still answers ``True`` — the ask stands.
        live = {"$in": [_PENDING, _RUNNING, _QUARANTINED]}
        stamped = await self.client.update_one(
            coll,
            {"_id": run_id, "status": live, "cancel_requested_at": None, **scope},
            {"$set": {"cancel_requested_at": now, "updated_at": now}},
        )

        if stamped:
            return True

        # Nothing was written: either the run is terminal (or absent, or another tenant's),
        # or it is live and already carries the stamp. Only the second is a live ask.
        existing = await self.client.find_one(
            coll,
            {"_id": run_id, "status": live, **scope},
            projection={"_id": 1},
        )

        return existing is not None

    # ....................... #

    async def purge_finished(
        self,
        older_than: timedelta,
        *,
        statuses: Collection[DurableRunStatus] = PURGEABLE_STATUSES,
        name: str | None = None,
        limit: int = 1000,
    ) -> int:
        check_purge(older_than, statuses, limit)

        if self.journal and self.step_config is None:
            raise exc.configuration(
                "Purging durable runs would leave their step journal behind: pass the run "
                "store its step_config, or journal=False if its runs journal no steps.",
            )

        coll = await self._collection()
        steps = await self._step_collection()

        # Finish what an interrupted purge left: its marked steps, before marking more.
        if steps is not None:
            marked = await self.client.find_many(
                steps,
                {"purge_pending": True, **self._tenant_filter()},
                projection={"run_id": 1},
                limit=limit,
            )
            await self._settle_steps(coll, steps, list({doc["run_id"] for doc in marked}))

        # Scoped like ``list_runs``. ``updated_at`` is the landing: every terminal write sets
        # it, so the window starts when the run finished, not when it was submitted.
        finished: dict[str, Any] = {
            "status": {"$in": [str(status) for status in statuses]},
            "updated_at": {"$lt": utcnow() - older_than},
            **self._tenant_filter(),
        }

        if name is not None:
            finished["name"] = name

        candidates = await self.client.find_many(
            coll,
            finished,
            projection={"_id": 1},
            sort=[("updated_at", 1)],
            limit=limit,
        )
        run_ids = [doc["_id"] for doc in candidates]

        if not run_ids:
            return 0

        # Mark the journal first, with no transaction to delete both together: a crash after
        # the runs go leaves marked steps, which the next purge settles above.
        if steps is not None:
            await self.client.update_many(
                steps, {"run_id": {"$in": run_ids}}, {"$set": {"purge_pending": True}}
            )

        # The filter is re-checked per document as it deletes, so a run requeued since the
        # read is kept.
        deleted = await self.client.delete_many(coll, {"_id": {"$in": run_ids}, **finished})

        if steps is not None:
            await self._settle_steps(coll, steps, run_ids)

        return deleted

    # ....................... #

    async def _step_collection(self) -> AsyncCollection[JsonDict] | None:
        if self.step_config is None:
            return None

        db_name, coll_name = await resolve_mongo_collection(
            self.step_config.collection, self._tenant_id_for_resolve()
        )

        return await self.client.collection(coll_name, db_name=db_name)

    # ....................... #

    async def _settle_steps(
        self,
        coll: AsyncCollection[JsonDict],
        steps: AsyncCollection[JsonDict],
        run_ids: Sequence[str],
    ) -> None:
        """Delete the steps of the runs in *run_ids* that are gone; unmark the rest.

        A run kept — requeued while the purge ran — keeps its journal, so its steps replay.
        Run ids are never reused, so a step whose run is gone is read by nothing again.
        """

        if not run_ids:
            return

        kept = await self.client.find_many(coll, {"_id": {"$in": run_ids}}, projection={"_id": 1})
        kept_ids = {doc["_id"] for doc in kept}
        gone = [run_id for run_id in run_ids if run_id not in kept_ids]

        if gone:
            await self.client.delete_many(steps, {"run_id": {"$in": gone}})

        if kept_ids:
            await self.client.update_many(
                steps, {"run_id": {"$in": list(kept_ids)}}, {"$unset": {"purge_pending": ""}}
            )

    # ....................... #

    def control_capabilities(self) -> DurableRunControlCapabilities:
        return DurableRunControlCapabilities(supports_cancel=True)

    # ....................... #

    async def load(self, run_id: str) -> DurableRunRecord | None:
        coll = await self._collection()
        doc = await self.client.find_one(coll, {"_id": run_id, **self._worker_scope()})

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

        records = [await self._listed(row) for row in rows]

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
        available_at: datetime | None = None,
    ) -> None:
        coll = await self._collection()
        now = utcnow()

        # Guarded on ``status = running`` so a terminal state is not overwritten and a
        # duplicate/late completion is a no-op (idempotent under recovery re-invocation).
        # When *fence* is given, also require it to match ``attempts`` so a stale worker
        # whose lease was reclaimed cannot finish the run.
        filter_: dict[str, Any] = {"_id": run_id, "status": _RUNNING, **self._worker_scope()}

        if fence is not None:
            filter_["attempts"] = fence

        update: dict[str, Any] = {
            "status": status.value,
            "output": output,
            "error": error,
            "leased_until": None,
            "updated_at": now,
        }

        # A retry goes back to PENDING due at *available_at*; a terminal write leaves
        # ``available_at`` as it is.
        if status is DurableRunStatus.PENDING:
            update["available_at"] = available_at

        await self.client.update_one(coll, filter_, {"$set": update})

    # ....................... #

    async def _stored_tenant(self, run_id: str) -> UUID | None:
        """The ``tenant_id`` of a run this caller can reach, or ``None``.

        Scoped like the write that follows it, so a run outside the caller's reach reads as
        untagged and the terminal write it feeds then matches nothing — the same silence an
        unreachable run already answers with, rather than a seal against a tenant whose key
        this caller does not hold.
        """

        coll = await self._collection()
        doc = await self.client.find_one(
            coll,
            {"_id": run_id, **self._worker_scope()},
            projection={"tenant_id": 1},
        )

        return None if doc is None else as_uuid(doc.get("tenant_id"))

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
            idempotency_key=_unscope_key(
                document["idempotency_key"], as_uuid(document["tenant_id"])
            ),
            input_json=input_json,
            tenant_id=as_uuid(document["tenant_id"]),
            attempts=int(document["attempts"]),
            available_at=document["available_at"],
            created_at=document["created_at"],
        )

    # ....................... #

    async def _listed(self, row: Any) -> DurableRunRecord:
        # A listing is how an operator finds a run to cancel, so one whose payloads cannot
        # be decrypted is listed without them rather than failing the page.
        try:
            return await self._record_from_row(row)

        except Exception as error:
            # A passing fault would cost one KMS call per row, each to its own timeout,
            # before the page came back stripped; it fails the page instead.
            if is_payload_cipher_missing(error) or is_passing_fault(error):
                raise

            logger.warning(
                "Durable run %s is listed without its payloads, which could not be decrypted (%s)",
                row["_id"],
                type(error).__name__,
            )

            return await self._record_from_row(row, open_payloads=False)

    # ....................... #

    async def _record_from_row(
        self,
        row: JsonDict,
        *,
        open_payloads: bool = True,
    ) -> DurableRunRecord:
        tenant_id = as_uuid(row.get("tenant_id"))
        run_id = str(row["_id"])
        input_json = output_json = None

        if open_payloads:
            input_json = await self._unseal(row.get("input"), run_id, "input", tenant_id)
            output_json = await self._unseal(row.get("output"), run_id, "output", tenant_id)

        return DurableRunRecord(
            run_id=run_id,
            name=str(row["name"]),
            status=DurableRunStatus(row["status"]),
            idempotency_key=_unscope_key(row.get("idempotency_key"), tenant_id),
            input_json=input_json,
            output_json=output_json,
            error=row.get("error"),
            tenant_id=tenant_id,
            attempts=int(row.get("attempts") or 0),
            available_at=row.get("available_at"),
            created_at=row.get("created_at"),
            cancel_requested_at=row.get("cancel_requested_at"),
            cancel_refused_at=row.get("cancel_refused_at"),
            payloads_unreadable=not open_payloads,
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
