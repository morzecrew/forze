"""Mongo-backed counter — atomic ``$inc`` upsert via ``find_one_and_update``."""

from __future__ import annotations

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from collections.abc import Sequence
from typing import Any, Final, final
from uuid import UUID

import attrs
from pymongo.asynchronous.collection import AsyncCollection

from forze.application.contracts.counter import (
    CounterAdminPort,
    CounterEntry,
    CounterPort,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.primitives import JsonDict
from forze_mongo.execution.deps.configs.counter import MongoCounterConfig
from forze_mongo.kernel.client import MongoClientPort
from forze_mongo.kernel.relation import resolve_mongo_collection

from ._logger import logger

# ----------------------- #

_UNSUFFIXED: Final[str] = ""
"""``_id`` form of the unsuffixed counter — the primary key cannot hold ``None``."""

_SUFFIX_PREFIX: Final[str] = "s:"
"""Prefix for suffixed ``_id``s, so no suffix (including ``""``) can collide with the
unsuffixed sentinel."""

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class _MongoCounterBase(TenancyMixin):
    """Shared collection/key resolution for the counter data and admin adapters."""

    client: MongoClientPort
    config: MongoCounterConfig
    route: str
    """The counter spec's route — a discriminator in the ``_id`` and a stored field, so
    two specs wired to one collection do not silently share rows."""

    # ....................... #

    async def _collection(self) -> AsyncCollection[JsonDict]:
        # Namespace-tier resolution: ``_tenant_id_for_resolve`` returns the bound tenant
        # for a per-tenant collection even without tagged-tier ``tenant_aware`` — using
        # ``require_tenant_if_aware`` here collapsed every tenant onto one collection.
        tenant_id = self._tenant_id_for_resolve()
        db_name, coll_name = await resolve_mongo_collection(
            self.config.collection,
            tenant_id,
        )
        return await self.client.collection(coll_name, db_name=db_name)

    # ....................... #

    def _doc_id(self, suffix: str | None, tenant_id: UUID | None) -> str:
        # The ``_id`` is the atomicity anchor: a concurrent upsert of the same counter
        # collides on it instead of inserting a duplicate row (a filter over plain fields
        # would need a unique index the application never migrated). The length-prefixed
        # route tag keeps two specs sharing a collection apart; the suffix prefix keeps
        # any suffix (including ``""``) apart from the unsuffixed sentinel; the tenant
        # prefix keeps tagged-tier tenants apart, and the fixed-length UUID makes the
        # composition unambiguous for any suffix.
        key = f"{_SUFFIX_PREFIX}{suffix}" if suffix is not None else _UNSUFFIXED
        body = f"tenant:{tenant_id}:{key}" if tenant_id is not None else key

        return f"{len(self.route)}:{self.route}|{body}"

    # ....................... #

    def _legacy_doc_id(self, suffix: str | None, tenant_id: UUID | None) -> str:
        # The pre-route ``_id`` (no route tag) a counter allocated before the route fold
        # was keyed under. The allocation path migrates it onto the new id so an existing
        # sequence continues instead of restarting from zero.
        key = f"{_SUFFIX_PREFIX}{suffix}" if suffix is not None else _UNSUFFIXED

        return f"tenant:{tenant_id}:{key}" if tenant_id is not None else key

    # ....................... #

    def _tenant_filter(self) -> dict[str, Any]:
        # Route folded into every read filter so a shared collection reports only this
        # spec's counters, alongside the tenant discriminator.
        tenant_id = self.require_tenant_if_aware()

        return {"tenant_id": str(tenant_id) if tenant_id is not None else None, "route": self.route}


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoCounterAdapter(_MongoCounterBase, CounterPort):
    """Mongo implementation of :class:`~forze.application.contracts.counter.CounterPort`.

    Every operation is a single ``find_one_and_update`` with ``upsert`` returning the
    post-update document, so allocation is atomic without a session: concurrent callers
    serialize on the ``_id`` and each sees a distinct value. Operations run **detached**
    — never on the caller's transaction/session — so an allocation survives the caller's
    rollback; otherwise the same value could be handed out twice (Redis parity: a counter
    value is burned the moment it is returned).

    Documents look like ``{_id, suffix, tenant_id, route, value}``; ``suffix`` /
    ``tenant_id`` / ``route`` are carried as plain fields so enumeration never has to
    parse the ``_id`` composition.
    """

    async def _migrate_legacy(
        self, coll: AsyncCollection[JsonDict], suffix: str | None, tenant_id: UUID | None
    ) -> None:
        """Carry a pre-route counter document onto its new route-prefixed ``_id``, once.

        A counter allocated before the route fold lives under the legacy ``_id`` with no
        ``route`` field, so the new upsert would create a fresh document from zero and
        reissue numbers already handed out. This copies the legacy value onto the new id
        (leaving the new document untouched if it already exists — a concurrent writer or a
        prior migration) and drops the legacy document, so enumeration and the next
        allocation both see one row. A no-op once migrated (the legacy read finds nothing).
        """

        legacy_id = self._legacy_doc_id(suffix, tenant_id)
        legacy = await self.client.find_one(coll, {"_id": legacy_id})

        if legacy is None:
            return

        try:
            await self.client.insert_one(
                coll,
                {
                    "_id": self._doc_id(suffix, tenant_id),
                    "value": int(legacy["value"]),
                    "suffix": suffix,
                    "tenant_id": (str(tenant_id) if tenant_id is not None else None),
                    "route": self.route,
                },
            )
        except CoreException as error:
            if error.kind is not ExceptionKind.CONFLICT:
                raise
            # the new document already exists — keep it, just retire the legacy one

        await self.client.delete_one(coll, {"_id": legacy_id})

    # ....................... #

    async def _apply(self, update: JsonDict, suffix: str | None) -> int:
        coll = await self._collection()
        tenant_id = self.require_tenant_if_aware()

        async with self.client.detached():
            await self._migrate_legacy(coll, suffix, tenant_id)
            doc = await self.client.find_one_and_update(
                coll,
                {"_id": self._doc_id(suffix, tenant_id)},
                {
                    **update,
                    # Idempotent bookkeeping so the admin enumeration reads fields,
                    # not ids.
                    "$set": {
                        **update.get("$set", {}),
                        "suffix": suffix,
                        "tenant_id": (str(tenant_id) if tenant_id is not None else None),
                        "route": self.route,
                    },
                },
                upsert=True,
            )

        if doc is None:  # pragma: no cover - upsert + AFTER always yields the document
            raise exc.internal("Counter upsert returned no document")

        return int(doc["value"])

    # ....................... #

    async def incr(self, by: int = 1, *, suffix: str | None = None) -> int:
        logger.debug("Incrementing counter suffix '%s' by %s", suffix, by)

        return await self._apply({"$inc": {"value": by}}, suffix)

    # ....................... #

    async def incr_batch(
        self,
        size: int = 2,
        *,
        suffix: str | None = None,
    ) -> list[int]:
        if size < 1:
            raise exc.precondition("Batch size must be at least 1")

        logger.debug(
            "Incrementing counter suffix '%s' by %s, returning batch range",
            suffix,
            size,
        )

        max_cnt = await self._apply({"$inc": {"value": size}}, suffix)

        return list(range(max_cnt - size + 1, max_cnt + 1))

    # ....................... #

    async def decr(self, by: int = 1, *, suffix: str | None = None) -> int:
        logger.debug("Decrementing counter suffix '%s' by %s", suffix, by)

        return await self._apply({"$inc": {"value": -by}}, suffix)

    # ....................... #

    async def reset(self, value: int = 1, *, suffix: str | None = None) -> int:
        logger.debug("Resetting counter suffix '%s' to %s", suffix, value)

        return await self._apply({"$set": {"value": value}}, suffix)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoCounterAdminAdapter(_MongoCounterBase, CounterAdminPort):
    """Enumerate the counters allocated in one Mongo counters collection."""

    async def list_counters(self) -> Sequence[CounterEntry]:
        coll = await self._collection()

        # The tenant filter matches on the stored field (exactly like the outbox route
        # filter), so a shared tagged-tier collection only ever reports the bound
        # tenant's counters.
        async with self.client.detached():
            docs = await self.client.find_many(coll, self._tenant_filter())

        return [
            CounterEntry(
                suffix=(str(doc["suffix"]) if doc.get("suffix") is not None else None),
                value=int(doc["value"]),
            )
            for doc in docs
        ]
