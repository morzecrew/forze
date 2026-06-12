import time
from collections import OrderedDict
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import AsyncGenerator, Awaitable, Callable, final
from uuid import UUID

import attrs

from forze.base.exceptions import exc
from forze.base.primitives import GuardedLruRegistry, SimpleLruRegistry

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class TenantPoolStats:
    """Snapshot of a tenant pool registry's churn counters.

    The thrash signature to alert on: a sustained ``created`` rate while
    ``size == capacity`` — pools are being rebuilt for tenants the LRU just
    evicted, and every rebuild pays full connection establishment.
    """

    size: int
    """Live pools right now (guarded registries exclude draining entries)."""

    capacity: int
    """The registry's ``max_entries`` bound."""

    created: int
    """Cumulative successful pool creations."""

    disposed: int
    """Cumulative pool disposals (capacity evictions, rotations, shutdown)."""

    evicted_explicit: int
    """Cumulative explicit evictions (rotation signals, manual ``evict_tenant``)."""


# ....................... #


@attrs.define(slots=True)
class TenantClientRegistry[C, R = str]:
    """LRU pool keyed by tenant id with optional fingerprint dedup.

    When ``guarded=True``, the underlying :class:`~forze.base.primitives.GuardedLruRegistry`
    ``create`` callback must not call :meth:`use` for the same tenant (or deduplicated slot)
    while that tenant is being created — reentrant access raises
    :exc:`~forze.base.errors.exc.internal` instead of deadlocking.
    """

    max_entries: int
    """Maximum number of entries in the registry."""

    create: Callable[[UUID], Awaitable[C]]
    """Function to create a new client."""

    dispose: Callable[[C], Awaitable[None]]
    """Function to dispose a client."""

    guarded: bool = attrs.field(default=False, on_setattr=attrs.setters.frozen)
    """Whether to use a guarded LRU registry underneath."""

    __fingerprints: "OrderedDict[UUID, R]" = attrs.field(
        factory=OrderedDict,
        init=False,
        repr=False,
    )
    """LRU-bounded (to ``max_entries``) cache of per-tenant dedup fingerprints.

    Capped so it cannot grow without bound across the lifetime of a long-lived
    process; an evicted entry is simply recomputed on the tenant's next access.
    """

    __fingerprint_times: dict[UUID, float] = attrs.field(
        factory=dict,
        init=False,
        repr=False,
    )
    """Monotonic timestamps for cached fingerprints, used for optional TTL refresh."""

    __started: bool = attrs.field(default=False, init=False)

    __created_count: int = attrs.field(default=0, init=False, repr=False)
    __disposed_count: int = attrs.field(default=0, init=False, repr=False)
    __evicted_explicit_count: int = attrs.field(default=0, init=False, repr=False)

    __registry: GuardedLruRegistry[UUID, C, R] | SimpleLruRegistry[UUID, C, R] = (
        attrs.field(init=False, repr=False)
    )

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.max_entries < 1:
            raise exc.configuration("max_entries must be at least 1")

        registry_cls = (
            GuardedLruRegistry[UUID, C, R]
            if self.guarded
            else SimpleLruRegistry[UUID, C, R]
        )

        self.__registry = registry_cls(
            max_entries=self.max_entries,
            create=self._counted_create,
            dispose=self._counted_dispose,
            dedup_key=lambda tid: self.__fingerprints[tid],
        )

    # ....................... #

    async def _counted_create(self, tenant_id: UUID) -> C:
        client = await self.create(tenant_id)
        # Incremented after `create` returns, so failed attempts don't count.
        self.__created_count += 1

        return client

    # ....................... #

    async def _counted_dispose(self, client: C) -> None:
        await self.dispose(client)
        # Incremented after `dispose` returns, so failed disposals don't count.
        self.__disposed_count += 1

    # ....................... #

    def stats(self) -> TenantPoolStats:
        """Best-effort snapshot of pool churn counters (no lock).

        A sustained ``created`` rate while ``size == capacity`` means LRU
        thrash: pools rebuilt for tenants just evicted, each rebuild paying
        full connection establishment.
        """

        return TenantPoolStats(
            size=self.__registry.size,
            capacity=self.max_entries,
            created=self.__created_count,
            disposed=self.__disposed_count,
            evicted_explicit=self.__evicted_explicit_count,
        )

    # ....................... #

    async def startup(self) -> None:
        self.__started = True

    # ....................... #

    async def close(self) -> None:
        await self.__registry.close_all()
        self.__fingerprints.clear()
        self.__fingerprint_times.clear()
        self.__started = False

    # ....................... #

    async def evict(self, tenant_id: UUID) -> None:
        self.__evicted_explicit_count += 1
        self.__fingerprints.pop(tenant_id, None)
        self.__fingerprint_times.pop(tenant_id, None)
        await self.__registry.evict(tenant_id)

    # ....................... #

    def set_fingerprint(self, tenant_id: UUID, fingerprint: R) -> None:
        """Call before first get/create so dedup_key is defined."""

        self.__fingerprints[tenant_id] = fingerprint
        self.__fingerprints.move_to_end(tenant_id)
        self.__fingerprint_times[tenant_id] = time.monotonic()

        while len(self.__fingerprints) > self.max_entries:
            evicted, _ = self.__fingerprints.popitem(last=False)
            self.__fingerprint_times.pop(evicted, None)

    # ....................... #

    def get_fingerprint(self, tenant_id: UUID) -> R | None:
        fingerprint = self.__fingerprints.get(tenant_id)

        if fingerprint is not None:
            self.__fingerprints.move_to_end(tenant_id)

        return fingerprint

    # ....................... #

    def is_fingerprint_expired(self, tenant_id: UUID, ttl: timedelta) -> bool:
        """Whether *tenant_id*'s cached fingerprint is older than *ttl*.

        Returns ``True`` when no timestamp is recorded (treat as expired).
        """

        stamped = self.__fingerprint_times.get(tenant_id)

        return stamped is None or (time.monotonic() - stamped) > ttl.total_seconds()

    # ....................... #

    def peek(self, tenant_id: UUID) -> C | None:
        """Return a cached client without LRU touch (best-effort, no lock)."""

        return self.__registry.peek(tenant_id)

    # ....................... #

    def require_started(self) -> None:
        if not self.__started:
            raise exc.internal("Tenant client registry is not started")

    # ....................... #

    async def get(self, tenant_id: UUID) -> C:
        self.require_started()

        if isinstance(self.__registry, GuardedLruRegistry):
            raise exc.internal("Get is not supported for guarded registry")

        return await self.__registry.get_or_create(tenant_id)

    # ....................... #

    @asynccontextmanager
    async def use(self, tenant_id: UUID) -> AsyncGenerator[C]:
        self.require_started()

        if isinstance(self.__registry, SimpleLruRegistry):
            raise exc.internal("Use is not supported for simple registry")

        async with self.__registry.use(tenant_id) as client:
            yield client
