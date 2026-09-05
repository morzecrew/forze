"""Mongo durable-schedule store (recurring cron triggers)."""

from __future__ import annotations

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from collections.abc import Sequence
from datetime import datetime
from typing import Any, final
from uuid import UUID

import attrs
from pymongo.asynchronous.collection import AsyncCollection

from forze.application.contracts.durable.function import (
    DurableScheduleRecord,
    DurableScheduleStorePort,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.base.primitives import JsonDict, utcnow
from forze_mongo.adapters.durable._ids import as_uuid, scope_id, unscope_id
from forze_mongo.execution.deps.configs.durable import MongoDurableScheduleConfig
from forze_mongo.kernel.client import MongoClientPort
from forze_mongo.kernel.relation import resolve_mongo_collection

# ----------------------- #


def _record_from_row(row: JsonDict) -> DurableScheduleRecord:
    tenant_id = as_uuid(row.get("tenant_id"))

    return DurableScheduleRecord(
        schedule_id=unscope_id(str(row["_id"]), tenant_id),
        name=str(row["name"]),
        cron=str(row["cron"]),
        next_fire_at=row["next_fire_at"],
        tz=row.get("tz"),
        input_json=row.get("input"),
        enabled=bool(row.get("enabled", True)),
        tenant_id=tenant_id,
    )


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoDurableScheduleStore(TenancyMixin, DurableScheduleStorePort):
    """Mongo-backed recurring-schedule store.

    :meth:`put` upserts a schedule; :meth:`claim_due` hands out due schedules; :meth:`advance`
    compare-and-sets the next fire so two schedulers firing the same instant converge to one
    advance (and one run, via the run's ``{schedule_id}:{fire_epoch}`` idempotency key).

    **No lease, and none needed.** Postgres claims due rows under ``FOR UPDATE SKIP LOCKED``
    to reduce contention, which Mongo has no equivalent of — but correctness never rested on
    it: exactly-once firing comes from the run's idempotency key plus :meth:`advance`'s
    compare-and-set, so two schedulers reading the same due schedule still fire it once.
    What Mongo loses is the contention hint, not a guarantee.

    Tenancy mirrors the run store: the collection resolves under the bound tenant (tagged
    shared or per-tenant namespace), and a bound scheduler claims only that tenant's
    schedules. On a shared tagged collection the ``_id`` is the **tenant-scoped**
    ``schedule_id``.

    Tenancy lives in that key rather than in a predicate, so :meth:`advance`, :meth:`load`
    and :meth:`delete` reach exactly the bound tenant's schedules — there is no untagged
    allowance of the kind the run store's worker verbs carry, because a fallback lookup
    would let two keys answer one :meth:`load` and the compare-and-set above depends on the
    key being exact. :meth:`put` resolves one effective tenant for the collection, the tag
    and the key together, and refuses a record naming a tenant that contradicts the binding.

    Documents look like ``{_id, name, cron, tz, input, next_fire_at, enabled, tenant_id,
    created_at, updated_at}``. One index is worth creating; none is required::

        // Backs claim_due, which scans `enabled` schedules due at now, oldest first.
        db.<collection>.createIndex({enabled: 1, next_fire_at: 1})
        // On a shared tagged collection claim_due also filters the bound tenant:
        db.<collection>.createIndex({tenant_id: 1, enabled: 1, next_fire_at: 1})
    """

    client: MongoClientPort
    config: MongoDurableScheduleConfig

    # ....................... #

    async def _collection(self, tenant_id: UUID | None = None) -> AsyncCollection[JsonDict]:
        """Resolve the collection, under *tenant_id* when the caller has already settled it.

        Only :meth:`put` passes one — it is the sole verb that accepts an explicit tenant,
        and the collection it writes into has to be the one the stored ``_id`` is scoped for.
        """

        if tenant_id is None:
            tenant_id = self._tenant_id_for_resolve()

        db_name, coll_name = await resolve_mongo_collection(self.config.collection, tenant_id)

        return await self.client.collection(coll_name, db_name=db_name)

    # ....................... #

    async def put(self, record: DurableScheduleRecord) -> None:
        # One effective tenant for the stored field, the scoped ``_id`` *and* the collection.
        # A record naming a tenant the caller is not bound to is refused; one naming a tenant
        # where nothing is bound resolves the collection under it, so the schedule lands
        # where that tenant's scheduler will look for it rather than in the unbound
        # collection under an ``_id`` nobody composes.
        tenant_id = self._effective_tenant(record.tenant_id)
        coll = await self._collection(tenant_id)
        now = utcnow()

        await self.client.update_one_upsert(
            coll,
            {"_id": scope_id(record.schedule_id, tenant_id)},
            {
                "$set": {
                    "name": record.name,
                    "cron": record.cron,
                    "tz": record.tz,
                    "input": record.input_json,
                    "next_fire_at": record.next_fire_at,
                    "enabled": record.enabled,
                    "tenant_id": str(tenant_id) if tenant_id is not None else None,
                    "updated_at": now,
                },
                # Re-putting a schedule replaces its definition but keeps when it was first
                # registered, so ``created_at`` stays the schedule's age rather than the last
                # edit's timestamp.
                "$setOnInsert": {"created_at": now},
            },
        )

    # ....................... #

    async def claim_due(
        self,
        *,
        now: datetime,
        limit: int,
    ) -> Sequence[DurableScheduleRecord]:
        if limit <= 0:
            # Like the run store's scan: the driver refuses a zero cursor length where
            # Postgres answers ``LIMIT 0`` with no rows.
            return []

        coll = await self._collection()
        filter_: dict[str, Any] = {"enabled": True, "next_fire_at": {"$lte": now}}
        tenant_id = self._tenant_id_for_resolve()

        if tenant_id is not None:
            filter_["tenant_id"] = str(tenant_id)

        rows = await self.client.find_many(
            coll,
            filter_,
            sort=[("next_fire_at", 1)],
            limit=limit,
        )

        return [_record_from_row(row) for row in rows]

    # ....................... #

    async def advance(
        self,
        schedule_id: str,
        *,
        from_fire_at: datetime,
        to_fire_at: datetime,
    ) -> bool:
        coll = await self._collection()
        stored_sid = scope_id(schedule_id, self._tenant_id_for_resolve())

        # The compare-and-set that makes firing exactly-once: only the scheduler whose read
        # matches the stored instant moves it on, so a second one firing the same instant
        # advances nothing and is told so.
        matched = await self.client.update_one(
            coll,
            {"_id": stored_sid, "next_fire_at": from_fire_at},
            {"$set": {"next_fire_at": to_fire_at, "updated_at": utcnow()}},
        )

        return bool(matched)

    # ....................... #

    async def load(self, schedule_id: str) -> DurableScheduleRecord | None:
        coll = await self._collection()
        stored_sid = scope_id(schedule_id, self._tenant_id_for_resolve())
        row = await self.client.find_one(coll, {"_id": stored_sid})

        return None if row is None else _record_from_row(row)

    # ....................... #

    async def delete(self, schedule_id: str) -> bool:
        coll = await self._collection()
        stored_sid = scope_id(schedule_id, self._tenant_id_for_resolve())
        deleted = await self.client.delete_one(coll, {"_id": stored_sid})

        return bool(deleted)
