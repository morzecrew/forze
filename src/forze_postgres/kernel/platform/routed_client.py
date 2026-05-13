"""Postgres client that resolves a DSN per tenant via :class:`AsyncSecretsPort`."""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any, Literal, Sequence, overload
from uuid import UUID

import attrs
from psycopg import AsyncConnection
from psycopg.abc import Params, QueryNoTemplate

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.base.errors import CoreError, InfrastructureError, SecretNotFoundError
from forze.base.primitives import JsonDict

from .client import PostgresClient
from .port import PostgresClientPort
from .types import RowFactory
from .value_objects import PostgresConfig, PostgresTransactionOptions

# ----------------------- #


@attrs.define
class _TenantPoolSlot:
    """Per-tenant pool entry with in-flight refcount for safe LRU eviction."""

    tenant_id: UUID
    client: PostgresClient
    router: RoutedPostgresClient
    refcount: int = 0
    drain_after_idle: bool = False
    condition: asyncio.Condition = attrs.field(factory=asyncio.Condition)
    draining_barrier: asyncio.Event = attrs.field(factory=asyncio.Event)

    def __attrs_post_init__(self) -> None:
        self.draining_barrier.set()

    # ....................... #

    def mark_entered_draining_registry(self) -> None:
        """Block :meth:`wait_until_not_in_draining_registry` until the pool is deregistered."""

        self.draining_barrier.clear()

    # ....................... #

    @asynccontextmanager
    async def use(self) -> AsyncIterator[PostgresClient]:
        """Increment refcount around work on :attr:`client`; close when draining and idle."""

        async with self.condition:
            self.refcount += 1

        try:
            yield self.client

        finally:
            do_finish_drain = False
            async with self.condition:
                self.refcount -= 1
                do_finish_drain = self.refcount == 0 and self.drain_after_idle
                self.condition.notify_all()

            if do_finish_drain:
                await self.client.close()
                self.drain_after_idle = False
                await self.router.deregister_draining_tenant_pool(self.tenant_id)
                async with self.condition:
                    self.condition.notify_all()

    # ....................... #

    async def wait_until_not_in_draining_registry(self) -> None:
        """Wait until this tenant's drained pool has been closed and deregistered."""

        await self.draining_barrier.wait()


# ----------------------- #


@attrs.define(slots=True)
class RoutedPostgresClient(PostgresClientPort):
    """Routes each call to a lazily created :class:`PostgresClient` for the current tenant.

    The tenant is read from ``tenant_provider`` (typically
    :meth:`forze.application.execution.ExecutionContext.get_tenant_id`).
    DSN strings are loaded via :meth:`SecretsPort.resolve_str` using
    ``secret_ref_for_tenant``.

    Call :meth:`startup` during application startup (see
    :func:`~forze_postgres.execution.lifecycle.routed_postgres_lifecycle_step`)
    before use. Call :meth:`close` on shutdown to drain all per-tenant pools.

    LRU eviction never closes a pool that still has in-flight routed operations:
    evicted tenants move to a draining set until the last in-flight use of that pool
    finishes, then the pool is closed and the tenant slot may be recreated.
    """

    secrets: SecretsPort
    """Backend used to resolve connection strings."""

    secret_ref_for_tenant: Callable[[UUID], SecretRef] | Mapping[UUID, SecretRef]
    """Build a :class:`SecretRef` for a tenant's database DSN."""

    tenant_provider: Callable[[], UUID | None]
    """Return the current tenant id (or ``None`` if unauthenticated)."""

    pool_config: PostgresConfig = attrs.field(factory=PostgresConfig)
    """Pool configuration applied to each per-tenant :class:`PostgresClient`."""

    acquire_timeout: timedelta = attrs.field(default=timedelta(seconds=5))
    """Pool checkout timeout passed to each inner client."""

    max_cached_tenants: int = 100
    """Maximum number of tenant pools to retain; LRU eviction closes overflow pools."""

    _registry_lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False)
    _slots: OrderedDict[UUID, _TenantPoolSlot] = attrs.field(
        factory=OrderedDict,
        init=False,
    )
    _draining: dict[UUID, _TenantPoolSlot] = attrs.field(factory=dict, init=False)
    _tenant_init_locks: dict[UUID, asyncio.Lock] = attrs.field(
        factory=dict,
        init=False,
    )
    _started: bool = attrs.field(default=False, init=False)
    _gather_sem: asyncio.Semaphore | None = attrs.field(default=None, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.max_cached_tenants < 1:
            raise CoreError("max_cached_tenants must be at least 1")

    # ....................... #

    async def startup(self) -> None:
        """Mark the client as ready (idempotent)."""

        cfg = self.pool_config
        lim = (
            cfg.max_concurrent_queries
            if cfg.max_concurrent_queries is not None
            else max(1, cfg.max_size - cfg.pool_headroom)
        )
        self._gather_sem = asyncio.Semaphore(lim)
        self._started = True

    # ....................... #

    async def close(self) -> None:
        """Close all per-tenant pools and reset startup state."""

        async with self._registry_lock:
            to_close = [s.client for s in self._slots.values()] + [
                s.client for s in self._draining.values()
            ]
            self._slots.clear()
            self._draining.clear()
            self._tenant_init_locks.clear()

        for c in to_close:
            await c.close()

        self._started = False
        self._gather_sem = None

    # ....................... #

    async def evict_tenant(self, tenant_id: UUID) -> None:
        """Close and remove the pool for one tenant (e.g. after credential rotation)."""

        immediate: PostgresClient | None = None
        async with self._registry_lock:
            self._tenant_init_locks.pop(tenant_id, None)
            slot = self._slots.pop(tenant_id, None)
            if slot is None:
                slot = self._draining.pop(tenant_id, None)

            if slot is not None:
                if slot.refcount == 0:
                    immediate = slot.client
                else:
                    slot.mark_entered_draining_registry()
                    slot.drain_after_idle = True
                    self._draining[tenant_id] = slot

        if immediate is not None:
            await immediate.close()

    # ....................... #

    async def deregister_draining_tenant_pool(self, tenant_id: UUID) -> None:
        """Remove *tenant_id* from the draining map and wake waiters (internal hook for slots)."""

        slot: _TenantPoolSlot | None = None
        async with self._registry_lock:
            slot = self._draining.pop(tenant_id, None)

        if slot is not None:
            async with slot.condition:
                slot.condition.notify_all()
            slot.draining_barrier.set()

    # ....................... #

    def _require_tenant_id(self) -> UUID:
        tid = self.tenant_provider()

        if tid is None:
            raise CoreError(
                "Tenant ID is required for routed Postgres access",
                code="tenant_required",
            )

        return tid

    # ....................... #

    async def _lock_for_tenant_init(self, tid: UUID) -> asyncio.Lock:
        async with self._registry_lock:
            lock = self._tenant_init_locks.get(tid)
            if lock is None:
                lock = asyncio.Lock()
                self._tenant_init_locks[tid] = lock

            return lock

    # ....................... #

    def _get_secret_ref(self, tenant_id: UUID) -> SecretRef:
        if callable(self.secret_ref_for_tenant):
            return self.secret_ref_for_tenant(tenant_id)

        return self.secret_ref_for_tenant[tenant_id]

    # ....................... #

    async def _await_not_draining(self, tid: UUID) -> None:
        """Block until *tid* is not waiting on a prior drained pool."""

        while True:
            async with self._registry_lock:
                slot = self._draining.get(tid)
                if slot is None:
                    return

            await slot.wait_until_not_in_draining_registry()

    # ....................... #

    @asynccontextmanager
    async def _client_scope(self) -> AsyncIterator[PostgresClient]:
        """Resolve the tenant slot and yield its client with refcount protection."""

        if not self._started:
            raise InfrastructureError("Routed Postgres client is not started")

        tid = self._require_tenant_id()
        await self._await_not_draining(tid)

        slot: _TenantPoolSlot | None = None
        async with self._registry_lock:
            if tid in self._slots:
                slot = self._slots[tid]
                self._slots.move_to_end(tid)

        if slot is not None:
            async with slot.use():
                yield slot.client

            return

        tlock = await self._lock_for_tenant_init(tid)
        async with tlock:
            await self._await_not_draining(tid)

            slot = None
            async with self._registry_lock:
                if tid in self._slots:
                    slot = self._slots[tid]
                    self._slots.move_to_end(tid)

            if slot is not None:
                async with slot.use():
                    yield slot.client

                return

            ref = self._get_secret_ref(tid)

            try:
                dsn = await self.secrets.resolve_str(ref)

            except SecretNotFoundError:
                raise

            except Exception as e:
                raise InfrastructureError(
                    f"Failed to resolve database secret for tenant {tid}: {e}",
                ) from e

            client = PostgresClient()
            await client.initialize(
                dsn,
                config=self.pool_config,
                acquire_timeout=self.acquire_timeout,
            )

            immediate_close: list[PostgresClient] = []
            new_slot: _TenantPoolSlot

            async with self._registry_lock:
                if tid in self._slots:
                    await client.close()
                    existing_slot = self._slots[tid]
                    self._slots.move_to_end(tid)
                else:
                    existing_slot = None

            if existing_slot is not None:
                async with existing_slot.use():
                    yield existing_slot.client

                return

            async with self._registry_lock:
                new_slot = _TenantPoolSlot(tenant_id=tid, client=client, router=self)
                self._slots[tid] = new_slot
                self._slots.move_to_end(tid)

                while len(self._slots) > self.max_cached_tenants:
                    old_tid, old_slot = self._slots.popitem(last=False)

                    if old_slot.refcount == 0:
                        immediate_close.append(old_slot.client)
                    else:
                        old_slot.mark_entered_draining_registry()
                        old_slot.drain_after_idle = True
                        self._draining[old_tid] = old_slot

            for c in immediate_close:
                await c.close()

            async with new_slot.use():
                yield new_slot.client

    # ....................... #

    def _slot_for_tenant_read(self, tid: UUID) -> _TenantPoolSlot | None:
        """Best-effort slot lookup for sync helpers (may race with eviction)."""

        s = self._slots.get(tid)

        if s is not None:
            return s

        return self._draining.get(tid)

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        async with self._client_scope() as inner:
            return await inner.health()

    # ....................... #

    def is_in_transaction(self) -> bool:
        """Return whether the inner client for the current tenant is in a transaction.

        Best-effort: reads routing dicts without locking and may race with eviction
        or first-time client creation; treat as a hint for diagnostics rather than a
        strict mutex over tenant routing.
        """

        tid = self.tenant_provider()

        if tid is None:
            return False

        slot = self._slot_for_tenant_read(tid)

        if slot is None:
            return False

        return slot.client.is_in_transaction()

    # ....................... #

    def query_concurrency_limit(self) -> int:
        tid = self.tenant_provider()

        if tid is not None:
            slot = self._slot_for_tenant_read(tid)

            if slot is not None:
                return slot.client.query_concurrency_limit()

        cfg = self.pool_config

        if cfg.max_concurrent_queries is not None:
            return cfg.max_concurrent_queries

        return max(1, cfg.max_size - cfg.pool_headroom)

    # ....................... #

    def gather_concurrency_semaphore(self) -> asyncio.Semaphore:
        if not self._started or self._gather_sem is None:
            raise InfrastructureError("Routed Postgres client is not started")

        return self._gather_sem

    # ....................... #

    def require_transaction(self) -> None:
        tid = self.tenant_provider()

        if tid is None:
            raise InfrastructureError("Transactional context is required")

        slot = self._slot_for_tenant_read(tid)

        if slot is None:
            raise InfrastructureError("Transactional context is required")

        slot.client.require_transaction()

    # ....................... #

    @asynccontextmanager
    async def bound_connection(self) -> AsyncIterator[AsyncConnection]:
        async with self._client_scope() as inner:
            async with inner.bound_connection() as conn:
                yield conn

    # ....................... #

    @asynccontextmanager
    async def transaction(
        self,
        *,
        options: PostgresTransactionOptions | None = None,
    ) -> AsyncIterator[AsyncConnection]:
        async with self._client_scope() as inner:
            async with inner.transaction(options=options) as conn:
                yield conn

    # ....................... #

    @overload
    async def execute(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        return_rowcount: Literal[False] = False,
    ) -> None: ...

    @overload
    async def execute(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        return_rowcount: Literal[True],
    ) -> int: ...

    async def execute(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        return_rowcount: bool = False,
    ) -> int | None:
        async with self._client_scope() as inner:
            if return_rowcount:
                return await inner.execute(query, params, return_rowcount=True)

            await inner.execute(query, params, return_rowcount=False)

            return None

    # ....................... #

    async def execute_many(
        self, query: QueryNoTemplate, params: Sequence[Params]
    ) -> None:
        async with self._client_scope() as inner:
            await inner.execute_many(query, params)

    # ....................... #

    @overload
    async def fetch_all(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        row_factory: Literal["dict"] = "dict",
        commit: bool = False,
    ) -> list[JsonDict]: ...

    @overload
    async def fetch_all(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        row_factory: Literal["tuple"] = "tuple",
        commit: bool = False,
    ) -> list[tuple[Any, ...]]: ...

    async def fetch_all(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        row_factory: RowFactory = "dict",
        commit: bool = False,
    ) -> list[JsonDict] | list[tuple[Any, ...]]:
        async with self._client_scope() as inner:
            return await inner.fetch_all(
                query,
                params,
                row_factory=row_factory,
                commit=commit,
            )

    # ....................... #

    @overload
    async def fetch_one(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        row_factory: Literal["dict"] = "dict",
        commit: bool = False,
    ) -> JsonDict | None: ...

    @overload
    async def fetch_one(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        row_factory: Literal["tuple"] = "tuple",
        commit: bool = False,
    ) -> tuple[Any, ...] | None: ...

    async def fetch_one(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        row_factory: RowFactory = "dict",
        commit: bool = False,
    ) -> JsonDict | tuple[Any, ...] | None:
        async with self._client_scope() as inner:
            return await inner.fetch_one(
                query,
                params,
                row_factory=row_factory,
                commit=commit,
            )

    # ....................... #

    async def fetch_value(
        self,
        query: QueryNoTemplate,
        params: Params | None = None,
        *,
        default: Any = None,
    ) -> Any:
        async with self._client_scope() as inner:
            return await inner.fetch_value(query, params, default=default)
