"""Firestore-backed counter — transactional read-modify-write allocation."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta
from typing import Final, final
from uuid import UUID

import attrs
from google.cloud.firestore_v1.async_collection import AsyncCollectionReference
from google.cloud.firestore_v1.base_query import And, FieldFilter

from forze.application.contracts.counter import (
    CounterAdminPort,
    CounterEntry,
    CounterPort,
)
from forze.application.contracts.resilience import (
    BackoffStrategy,
    ResilienceExecutorPort,
    ResiliencePolicy,
    RetryStrategy,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.application.execution.resilience import (
    OCC_POLICY,
    InProcessResilienceExecutor,
    builtin_default_policies,
    occ_retry,
)
from forze.base.exceptions import ExceptionKind, exc
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

_COUNTER_OCC_ATTEMPTS: Final[int] = 8
"""Retry budget for a contended allocation.

The shared ``occ`` default (3) is sized for a *document revision* conflict — an anomaly,
where two writers happened to touch the same row. A counter inverts that: every caller
targets the same document by definition, so conflict is the steady state, not the
exception. With N concurrent allocations the last writer to win can be aborted N-1 times,
so a budget of 3 starts surfacing ``core.concurrency`` to callers at N=4 — contradicting
this adapter's own contract that every caller receives a distinct value.

Sized for the contention a single process realistically generates, not unbounded N; past
that the documented answer is still ``incr_batch`` (one allocation for many values) or a
Redis-backed counter. Only Firestore needs this: Postgres allocates with
``INSERT … ON CONFLICT … RETURNING`` and Mongo with ``$inc`` + ``find_one_and_update`` —
single atomic statements that never contend at this layer.
"""


def _counter_resilience_executor() -> ResilienceExecutorPort:
    """A resilience executor whose ``occ`` retry is sized for counter contention.

    Deliberately not the process default: raising the shared budget would lengthen the
    retry tail of every optimistic-concurrency path in the framework to fix one adapter
    whose contention profile is genuinely different. Everything else in the policy set is
    inherited unchanged.

    Built per adapter rather than memoized process-wide. The default executor is cached
    *per event loop* because bulkhead waiter futures are loop-affine, and a single shared
    instance would resolve a waiter on a foreign or closed loop; an instance that never
    outlives the adapter holding it cannot cross loops in the first place. These policies
    carry no bulkhead today, but caching one process-wide would silently depend on that
    staying true.
    """

    policies = dict(builtin_default_policies())
    policies[OCC_POLICY] = ResiliencePolicy(
        name=OCC_POLICY,
        strategies=(
            RetryStrategy(
                max_attempts=_COUNTER_OCC_ATTEMPTS,
                backoff=BackoffStrategy(
                    base=timedelta(milliseconds=50),
                    max=timedelta(seconds=2),
                    jitter="decorrelated",
                ),
                retry_on=frozenset({ExceptionKind.CONCURRENCY}),
            ),
        ),
    )

    return InProcessResilienceExecutor(policies=policies)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class _FirestoreCounterBase(TenancyMixin):
    """Shared collection/key resolution for the counter data and admin adapters."""

    client: FirestoreClientPort
    config: FirestoreCounterConfig
    route: str
    """The counter spec's route — a discriminator in the document id and a stored field,
    so two specs wired to one collection do not silently share rows."""

    # ....................... #

    async def _collection(self) -> AsyncCollectionReference:
        # Namespace-tier resolution: ``_tenant_id_for_resolve`` returns the bound tenant
        # for a per-tenant collection even without tagged-tier ``tenant_aware`` — using
        # ``require_tenant_if_aware`` here collapsed every tenant onto one collection.
        tenant_id = self._tenant_id_for_resolve()
        db_name, coll_name = await resolve_firestore_collection(
            self.config.collection,
            tenant_id,
        )
        return await self.client.collection(coll_name, database=db_name)

    # ....................... #

    def _doc_id(self, suffix: str | None, tenant_id: UUID | None) -> str:
        # The document id is the atomicity anchor: concurrent transactions on the same
        # counter contend on one document. The length-prefixed route tag keeps two specs
        # sharing a collection apart; the suffix prefix keeps any suffix (including one
        # literally named like the unsuffixed sentinel) collision-free, and the
        # fixed-length tenant UUID keeps tagged-tier tenants apart in a shared collection.
        key = f"{_SUFFIX_DOC_PREFIX}{suffix}" if suffix is not None else _UNSUFFIXED_DOC_ID
        body = f"tenant:{tenant_id}:{key}" if tenant_id is not None else key

        return f"{len(self.route)}:{self.route}|{body}"

    # ....................... #

    def _legacy_doc_id(self, suffix: str | None, tenant_id: UUID | None) -> str:
        # The pre-route document id (no route tag) a counter allocated before the route
        # fold was keyed under; the allocation path seeds the new document from it so an
        # existing sequence continues instead of restarting from zero.
        key = f"{_SUFFIX_DOC_PREFIX}{suffix}" if suffix is not None else _UNSUFFIXED_DOC_ID

        return f"tenant:{tenant_id}:{key}" if tenant_id is not None else key

    # ....................... #

    def _doc_body(self, value: int, suffix: str | None, tenant_id: UUID | None) -> JsonDict:
        # ``suffix``/``tenant_id``/``route`` are carried as plain fields so enumeration
        # reads fields instead of parsing the id composition.
        return {
            "value": value,
            "suffix": suffix,
            "tenant_id": (str(tenant_id) if tenant_id is not None else None),
            "route": self.route,
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

    resilience: ResilienceExecutorPort = attrs.field(factory=_counter_resilience_executor)

    # ....................... #

    @occ_retry
    async def _bump(self, by: int, suffix: str | None) -> int:
        coll = await self._collection()
        tenant_id = self.require_tenant_if_aware()
        doc_id = self._doc_id(suffix, tenant_id)

        legacy_id = self._legacy_doc_id(suffix, tenant_id)

        async with self.client.detached(), self.client.transaction():
            # Reads before writes (Firestore's transaction rule): read the new document,
            # and — only on its first touch — seed the base from the pre-route legacy
            # document so an existing sequence continues instead of restarting at zero
            # (which would reissue numbers already handed out). The legacy document is
            # retired in the same transaction; once migrated the fallback read finds none.
            current = await self.client.get_document(coll, doc_id)
            legacy = None if current else await self.client.get_document(coll, legacy_id)
            base = int(current["value"]) if current else (int(legacy["value"]) if legacy else 0)
            new_value = base + by

            await self.client.set_document(
                coll,
                doc_id,
                self._doc_body(new_value, suffix, tenant_id),
            )

            if legacy is not None:
                await self.client.delete_document(coll, legacy_id)

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

        # Server-side tenant *and* route filter on the stored fields, so a shared
        # collection only ever reports the bound tenant's counters for this spec — never
        # another tenant's, nor another spec sharing the collection. Detached, so the
        # read neither joins a caller's transaction nor trips its read-before-write
        # ordering rule.
        async with self.client.detached():
            docs = await self.client.query_stream(
                coll,
                filters=And(
                    [
                        FieldFilter(
                            "tenant_id",
                            "==",
                            (str(tenant_id) if tenant_id is not None else None),
                        ),
                        FieldFilter("route", "==", self.route),
                    ]
                ),
            )

        return [
            CounterEntry(
                suffix=(str(doc["suffix"]) if doc.get("suffix") is not None else None),
                value=int(doc["value"]),
            )
            for doc in docs
        ]
