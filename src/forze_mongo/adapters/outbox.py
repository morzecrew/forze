"""Mongo transactional outbox store."""

from __future__ import annotations

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from collections.abc import Sequence
from datetime import datetime
from typing import Any, final
from uuid import UUID

import attrs
from pydantic import BaseModel
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.errors import BulkWriteError

from forze.application.contracts.outbox import (
    OutboxClaim,
    OutboxQueryPort,
    OutboxSpec,
    OutboxStatus,
    StagedOutboxEntry,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, utcnow, uuid7
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
        tenant_id=(
            _as_uuid(doc["tenant_id"]) if doc.get("tenant_id") is not None else None
        ),
        execution_id=(
            _as_uuid(doc["execution_id"])
            if doc.get("execution_id") is not None
            else None
        ),
        correlation_id=(
            _as_uuid(doc["correlation_id"])
            if doc.get("correlation_id") is not None
            else None
        ),
        causation_id=(
            _as_uuid(doc["causation_id"])
            if doc.get("causation_id") is not None
            else None
        ),
        occurred_at=doc.get("occurred_at"),
    )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoOutboxStore[M: BaseModel](TenancyMixin, OutboxQueryPort):
    """Mongo-backed outbox persistence and query port."""

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
                    "tenant_id": (
                        str(event.tenant_id) if event.tenant_id is not None else None
                    ),
                    "execution_id": (
                        str(event.execution_id)
                        if event.execution_id is not None
                        else None
                    ),
                    "correlation_id": (
                        str(event.correlation_id)
                        if event.correlation_id is not None
                        else None
                    ),
                    "causation_id": (
                        str(event.causation_id)
                        if event.causation_id is not None
                        else None
                    ),
                    "occurred_at": event.occurred_at,
                    "payload": dict(entry.payload_json),
                    "status": OutboxStatus.PENDING.value,
                    "created_at": created_at,
                    "processing_at": None,
                    "published_at": None,
                    "last_error": None,
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
        coll = await self._collection()
        max_n = limit if limit is not None else self.config.max_claim_rows
        now = utcnow()
        claims: list[OutboxClaim] = []
        base_filter = self._route_filter()
        base_filter["status"] = OutboxStatus.PENDING.value

        for _ in range(max_n):
            doc = await self.client.find_one_and_update(
                coll,
                base_filter,
                {
                    "$set": {
                        "status": OutboxStatus.PROCESSING.value,
                        "processing_at": now,
                    }
                },
                sort=[("created_at", 1)],
            )

            if doc is None:
                break

            claims.append(_claim_from_doc(doc))

        return claims

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
                }
            },
        )
