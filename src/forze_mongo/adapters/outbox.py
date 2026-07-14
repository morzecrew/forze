"""Mongo transactional outbox store."""

from __future__ import annotations

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any, final
from uuid import UUID

import attrs
from pydantic import BaseModel
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.errors import BulkWriteError

from forze.application.contracts.outbox import (
    OutboxAdminPort,
    OutboxClaim,
    OutboxDepth,
    OutboxQueryPort,
    OutboxSpec,
    OutboxStatus,
    StagedOutboxEntry,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.base.exceptions import exc
from forze.base.primitives import HlcTimestamp, JsonDict, utcnow, uuid4, uuid7
from forze_mongo.execution.deps.configs.outbox import MongoOutboxConfig
from forze_mongo.kernel.client import MongoClientPort
from forze_mongo.kernel.relation import resolve_mongo_collection

# ----------------------- #


def _as_uuid(value: Any) -> UUID:
    if isinstance(value, UUID):
        return value

    return UUID(str(value))


# ....................... #


def _claim_from_doc(doc: JsonDict) -> OutboxClaim:
    return OutboxClaim(
        id=_as_uuid(doc["id"]),
        outbox_route=str(doc["outbox_route"]),
        event_id=_as_uuid(doc["event_id"]),
        event_type=str(doc["event_type"]),
        payload=dict(doc["payload"]),
        tenant_id=(_as_uuid(doc["tenant_id"]) if doc.get("tenant_id") is not None else None),
        execution_id=(
            _as_uuid(doc["execution_id"]) if doc.get("execution_id") is not None else None
        ),
        correlation_id=(
            _as_uuid(doc["correlation_id"]) if doc.get("correlation_id") is not None else None
        ),
        causation_id=(
            _as_uuid(doc["causation_id"]) if doc.get("causation_id") is not None else None
        ),
        occurred_at=doc.get("occurred_at"),
        attempts=int(doc.get("attempts") or 0),
        ordering_key=(str(doc["ordering_key"]) if doc.get("ordering_key") is not None else None),
        hlc=(HlcTimestamp.unpack(int(doc["hlc"])) if doc.get("hlc") is not None else None),
    )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoOutboxStore[M: BaseModel](TenancyMixin, OutboxQueryPort, OutboxAdminPort):
    """Mongo-backed outbox persistence, query, and admin port."""

    client: MongoClientPort
    spec: OutboxSpec[M]
    config: MongoOutboxConfig

    # ....................... #

    async def _collection(self) -> AsyncCollection[JsonDict]:
        tenant_id = self.require_tenant_if_aware()
        db_name, coll_name = await resolve_mongo_collection(
            self.config.collection,
            tenant_id,
        )
        return await self.client.collection(coll_name, db_name=db_name)

    def _route_filter(self) -> dict[str, Any]:
        flt: dict[str, Any] = {"outbox_route": str(self.spec.name)}
        tenant_id = self.require_tenant_if_aware()

        if tenant_id is not None:
            flt["tenant_id"] = str(tenant_id)

        return flt

    # ....................... #

    async def persist_rows(self, rows: Sequence[StagedOutboxEntry]) -> int:
        if not rows:
            return 0

        if len(rows) > self.config.max_flush_rows:
            raise exc.internal(
                f"Outbox flush exceeds max_flush_rows ({self.config.max_flush_rows})."
            )

        coll = await self._collection()
        created_at = utcnow()
        hlc_ordering = self.config.hlc_ordering
        event_ids = [str(entry.event.event_id) for entry in rows]

        existing_filter = self._route_filter()
        existing_filter["event_id"] = {"$in": event_ids}
        existing_rows = await self.client.find_many(coll, existing_filter)
        existing_ids = {str(row["event_id"]) for row in existing_rows}

        documents: list[JsonDict] = []

        for entry in rows:
            event = entry.event
            eid = str(event.event_id)

            if eid in existing_ids:
                continue

            row_id = uuid7()
            documents.append(
                {
                    "id": str(row_id),
                    "outbox_route": entry.outbox_route,
                    "event_id": eid,
                    "event_type": event.event_type,
                    "tenant_id": (str(event.tenant_id) if event.tenant_id is not None else None),
                    "execution_id": (
                        str(event.execution_id) if event.execution_id is not None else None
                    ),
                    "correlation_id": (
                        str(event.correlation_id) if event.correlation_id is not None else None
                    ),
                    "causation_id": (
                        str(event.causation_id) if event.causation_id is not None else None
                    ),
                    "occurred_at": event.occurred_at,
                    "ordering_key": event.ordering_key,
                    "payload": dict(entry.payload_json),
                    "status": OutboxStatus.PENDING.value,
                    "created_at": created_at,
                    "processing_at": None,
                    "published_at": None,
                    "last_error": None,
                    "attempts": 0,
                    "available_at": None,
                    **(
                        {"hlc": (event.hlc.pack() if event.hlc is not None else None)}
                        if hlc_ordering
                        else {}
                    ),
                }
            )

        if not documents:
            return 0

        try:
            inserted = await self.client.insert_many(coll, documents, ordered=False)
            return len(inserted)

        except BulkWriteError as e:
            details = e.details or {}
            n_inserted = details.get("nInserted")

            if isinstance(n_inserted, int):
                return n_inserted

            inserted_ids = details.get("insertedIds")

            if isinstance(inserted_ids, list):
                return len(inserted_ids)  # type: ignore[arg-type]

            return 0

    # ....................... #

    async def claim_pending(
        self,
        *,
        limit: int | None = None,
    ) -> Sequence[OutboxClaim]:
        # Three round trips for the whole batch (vs. one findAndModify per
        # claim): find candidate ids, claim them with a fresh batch token,
        # read the claimed rows back by token.
        coll = await self._collection()
        max_n = limit if limit is not None else self.config.max_claim_rows
        now = utcnow()
        base_filter = self._route_filter()
        base_filter["status"] = OutboxStatus.PENDING.value
        # NULL/absent available_at means immediately claimable.
        base_filter["$or"] = [
            {"available_at": None},
            {"available_at": {"$lte": now}},
        ]

        # HLC ordering: claim in causal order with the time-ordered uuid7 ``id``
        # as a deterministic tiebreaker. Mongo sorts missing ``hlc`` first, so
        # legacy pre-migration rows drain oldest-first; off keeps created_at.
        claim_sort: list[tuple[str, int]] = (
            [("hlc", 1), ("created_at", 1), ("id", 1)]
            if self.config.hlc_ordering
            else [("created_at", 1)]
        )

        candidates = await self.client.find_many(
            coll,
            base_filter,
            projection={"_id": 1},
            sort=claim_sort,
            limit=max_n,
        )

        if not candidates:
            return []

        # update_many rechecks status:pending per document atomically, so each
        # row is claimed by exactly one relay. A contended batch may claim
        # fewer than ``max_n`` rows while pending rows remain; the next poll
        # catches up. Stale tokens left on rows by previous batches are
        # harmless: every batch reads back its own fresh token.
        claim_token = uuid4().hex
        claim_filter = self._route_filter()
        claim_filter["status"] = OutboxStatus.PENDING.value
        claim_filter["_id"] = {"$in": [doc["_id"] for doc in candidates]}

        claimed = await self.client.update_many(
            coll,
            claim_filter,
            {
                "$set": {
                    "status": OutboxStatus.PROCESSING.value,
                    "processing_at": now,
                    "claim_token": claim_token,
                }
            },
        )

        if claimed == 0:
            return []

        # A crash here leaves rows processing without a read-back — the same
        # window as crashing after a findAndModify claim; covered by
        # reclaim_stale_processing. Recommended: sparse index on claim_token.
        docs = await self.client.find_many(
            coll,
            {"claim_token": claim_token},
            sort=claim_sort,
        )

        return [_claim_from_doc(doc) for doc in docs]

    async def mark_published(self, ids: Sequence[UUID]) -> int:
        return await self._mark(ids, OutboxStatus.PUBLISHED)

    async def mark_failed(
        self,
        ids: Sequence[UUID],
        *,
        error: str | None = None,
    ) -> int:
        return await self._mark(ids, OutboxStatus.FAILED, error=error)

    async def _mark(
        self,
        ids: Sequence[UUID],
        status: OutboxStatus,
        *,
        error: str | None = None,
    ) -> int:
        if not ids:
            return 0

        coll = await self._collection()
        now = utcnow()
        update: dict[str, Any] = {
            "status": status.value,
            "last_error": error,
        }

        if status == OutboxStatus.PUBLISHED:
            update["published_at"] = now

        return await self.client.update_many(
            coll,
            {
                "id": {"$in": [str(i) for i in ids]},
                "status": OutboxStatus.PROCESSING.value,
            },
            {"$set": update},
        )

    async def mark_retry(
        self,
        ids: Sequence[UUID],
        *,
        attempts: int,
        available_at: datetime,
        error: str | None = None,
    ) -> int:
        if not ids:
            return 0

        coll = await self._collection()
        return await self.client.update_many(
            coll,
            {
                "id": {"$in": [str(i) for i in ids]},
                "status": OutboxStatus.PROCESSING.value,
            },
            {
                "$set": {
                    "status": OutboxStatus.PENDING.value,
                    "processing_at": None,
                    "attempts": attempts,
                    "available_at": available_at,
                    "last_error": error,
                }
            },
        )

    async def reclaim_stale_processing(
        self,
        *,
        older_than: datetime,
    ) -> int:
        coll = await self._collection()
        flt = self._route_filter()
        flt["status"] = OutboxStatus.PROCESSING.value
        flt["processing_at"] = {"$lt": older_than}

        return await self.client.update_many(
            coll,
            flt,
            {
                "$set": {
                    "status": OutboxStatus.PENDING.value,
                    "processing_at": None,
                }
            },
        )

    async def requeue_failed(self, ids: Sequence[UUID]) -> int:
        if not ids:
            return 0

        coll = await self._collection()
        return await self.client.update_many(
            coll,
            {
                "id": {"$in": [str(i) for i in ids]},
                "status": OutboxStatus.FAILED.value,
            },
            {
                "$set": {
                    "status": OutboxStatus.PENDING.value,
                    "processing_at": None,
                    "published_at": None,
                    "last_error": None,
                    "attempts": 0,
                    "available_at": None,
                }
            },
        )

    # ....................... #
    # Admin (observability) port

    def _undrained_filter(self) -> dict[str, Any]:
        flt = self._route_filter()
        flt["status"] = {"$in": [OutboxStatus.PENDING.value, OutboxStatus.PROCESSING.value]}
        return flt

    # ....................... #

    async def has_undrained(self) -> bool:
        coll = await self._collection()
        # find_one over count: stops at the first match instead of walking the whole
        # matching set, so a quiesce loop can poll it cheaply.
        hit = await self.client.find_one(coll, self._undrained_filter(), projection={"_id": 1})

        return hit is not None

    # ....................... #

    async def depth(self) -> OutboxDepth:
        coll = await self._collection()
        counts: dict[OutboxStatus, int] = {}

        # Undrained buckets only: `published` is never pruned, so counting it would walk
        # every event the app has ever emitted.
        for status in (OutboxStatus.PENDING, OutboxStatus.PROCESSING, OutboxStatus.FAILED):
            flt = self._route_filter()
            flt["status"] = status.value
            counts[status] = await self.client.count(coll, flt)

        return OutboxDepth(
            pending=counts[OutboxStatus.PENDING],
            processing=counts[OutboxStatus.PROCESSING],
            failed=counts[OutboxStatus.FAILED],
        )

    # ....................... #

    async def oldest_pending_age(self) -> timedelta | None:
        coll = await self._collection()
        flt = self._route_filter()
        flt["status"] = OutboxStatus.PENDING.value

        oldest = await self.client.find_one(
            coll,
            flt,
            projection={"created_at": 1},
            sort=[("created_at", 1)],
        )

        if oldest is None:
            return None

        return utcnow() - oldest["created_at"]
