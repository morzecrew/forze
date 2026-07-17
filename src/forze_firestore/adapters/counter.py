"""Firestore-backed counter — transactional read-modify-write allocation."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Final, final
from uuid import UUID

import attrs
from google.cloud.firestore_v1.async_collection import AsyncCollectionReference
from google.cloud.firestore_v1.base_query import FieldFilter

from forze.application.contracts.counter import (
    CounterAdminPort,
    CounterEntry,
    CounterPort,
)
from forze.application.contracts.resilience import ResilienceExecutorPort
from forze.application.contracts.tenancy import TenancyMixin
from forze.application.execution.resilience import (
    default_resilience_executor,
    occ_retry,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze_firestore.execution.deps.configs import FirestoreCounterConfig
from forze_firestore.kernel.client import FirestoreClientPort
from forze_firestore.kernel.relation import resolve_firestore_collection

from ._logger import logger

# ----------------------- #

_UNSUFFIXED_DOC_ID: Final[str] = "_"
"""Document id of the unsuffixed counter — Firestore forbids empty-string ids."""

_SUFFIX_DOC_PREFIX: Final[str] = "s:"
"""Prefix for suffixed counter ids, so no suffix can collide with the unsuffixed id."""

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class _FirestoreCounterBase(TenancyMixin):
    """Shared collection/key resolution for the counter data and admin adapters."""

    client: FirestoreClientPort
    config: FirestoreCounterConfig

    # ....................... #

    async def _collection(self) -> AsyncCollectionReference:
        tenant_id = self.require_tenant_if_aware()
        db_name, coll_name = await resolve_firestore_collection(
            self.config.collection,
            tenant_id,
        )
        return await self.client.collection(coll_name, database=db_name)

    # ....................... #

    def _doc_id(self, suffix: str | None, tenant_id: UUID | None) -> str:
        # The document id is the atomicity anchor: concurrent transactions on the same
        # counter contend on one document. The suffix prefix keeps any suffix (including
        # one literally named like the unsuffixed sentinel) collision-free, and the
        # fixed-length tenant UUID keeps tagged-tier tenants apart in a shared collection.
        key = f"{_SUFFIX_DOC_PREFIX}{suffix}" if suffix is not None else _UNSUFFIXED_DOC_ID

        if tenant_id is not None:
            return f"tenant:{tenant_id}:{key}"

        return key

    # ....................... #

    def _doc_body(self, value: int, suffix: str | None, tenant_id: UUID | None) -> JsonDict:
        # ``suffix``/``tenant_id`` are carried as plain fields so enumeration reads
        # fields instead of parsing the id composition.
        return {
            "value": value,
            "suffix": suffix,
            "tenant_id": (str(tenant_id) if tenant_id is not None else None),
        }


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class FirestoreCounterAdapter(_FirestoreCounterBase, CounterPort):
    """Firestore implementation of :class:`~forze.application.contracts.counter.CounterPort`.

    Allocation is a serializable read-modify-write transaction on the counter's document:
    a concurrent writer aborts the commit and the operation retries under the ``occ``
    policy, so every caller still sees a distinct value. Operations run **detached** —
    the allocation transaction is always the counter's own, never the caller's — so an
    allocation survives the caller's rollback; otherwise the same value could be handed
    out twice (Redis parity: a counter value is burned the moment it is returned).

    **Throughput caveat**: Firestore sustains roughly one write per second per document,
    so a hot counter contends and retries. Allocate blocks with :meth:`incr_batch` to
    amortize the ceiling, or route high-rate counters to a Redis-backed adapter.
    """

    resilience: ResilienceExecutorPort = attrs.field(factory=default_resilience_executor)

    # ....................... #

    @occ_retry
    async def _bump(self, by: int, suffix: str | None) -> int:
        coll = await self._collection()
        tenant_id = self.require_tenant_if_aware()
        doc_id = self._doc_id(suffix, tenant_id)

        async with self.client.detached(), self.client.transaction():
            current = await self.client.get_document(coll, doc_id)
            new_value = (int(current["value"]) if current else 0) + by
            await self.client.set_document(
                coll,
                doc_id,
                self._doc_body(new_value, suffix, tenant_id),
            )

        return new_value

    # ....................... #

    async def incr(self, by: int = 1, *, suffix: str | None = None) -> int:
        logger.debug("Incrementing counter suffix '%s' by %s", suffix, by)

        return await self._bump(by, suffix)

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

        max_cnt = await self._bump(size, suffix)

        return list(range(max_cnt - size + 1, max_cnt + 1))

    # ....................... #

    async def decr(self, by: int = 1, *, suffix: str | None = None) -> int:
        logger.debug("Decrementing counter suffix '%s' by %s", suffix, by)

        return await self._bump(-by, suffix)

    # ....................... #

    async def reset(self, value: int = 1, *, suffix: str | None = None) -> int:
        coll = await self._collection()
        tenant_id = self.require_tenant_if_aware()

        logger.debug("Resetting counter suffix '%s' to %s", suffix, value)

        # A blind absolute set is atomic on its own — no transaction needed.
        async with self.client.detached():
            await self.client.set_document(
                coll,
                self._doc_id(suffix, tenant_id),
                self._doc_body(value, suffix, tenant_id),
            )

        return value


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class FirestoreCounterAdminAdapter(_FirestoreCounterBase, CounterAdminPort):
    """Enumerate the counters allocated in one Firestore counters collection."""

    async def list_counters(self) -> Sequence[CounterEntry]:
        coll = await self._collection()
        tenant_id = self.require_tenant_if_aware()

        # Server-side tenant filter on the stored field, so a shared tagged-tier
        # collection only ever reports the bound tenant's counters. Detached, so the
        # read neither joins a caller's transaction nor trips its read-before-write
        # ordering rule.
        async with self.client.detached():
            docs = await self.client.query_stream(
                coll,
                filters=FieldFilter(
                    "tenant_id",
                    "==",
                    (str(tenant_id) if tenant_id is not None else None),
                ),
            )

        return [
            CounterEntry(
                suffix=(str(doc["suffix"]) if doc.get("suffix") is not None else None),
                value=int(doc["value"]),
            )
            for doc in docs
        ]
