"""An opt-in, per-process cache of the catalog grants a principal resolves to, and the broadcast
that keeps every process's copy current."""

import threading
import time
from collections import Counter, OrderedDict
from collections.abc import Callable
from datetime import timedelta
from typing import TYPE_CHECKING, Final, Literal, cast, final
from uuid import UUID

import attrs

from forze.application.contracts.authz import AuthzScope, PermissionRef, RoleRef
from forze.application.contracts.cache import (
    CacheInvalidation,
    CacheSpec,
    SupportsInvalidationPush,
    Unsubscribe,
)
from forze.application.contracts.deps import backend_of, implements
from forze.application.contracts.tenancy import TenancyMixin
from forze.base.exceptions import exc
from forze.base.primitives import uuid4

from .._logger import logger

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #

CatalogGrants = tuple[frozenset[RoleRef], frozenset[PermissionRef]]
"""The roles and permissions the catalog bindings grant, without provider-derived ones."""

GrantsCacheKey = tuple[UUID, UUID | None, AuthzScope | None]
"""Principal, the invocation tenant read on the call, and the requested scope."""


@attrs.define(slots=True, kw_only=True, eq=False)
class GrantsCache:
    """Remembers each principal's catalog grants for :attr:`ttl`, per process.

    Pass one to ``AuthzKernelConfig(grants_cache=...)``; without it nothing is cached. What it
    holds is the part of a decision read from role, group and permission bindings. The
    principal's active flag and the permissions providers derive are read on every decision,
    so a deactivation, or a provider's change, applies at once.

    Decisions inside a transaction read the bindings and leave the cache alone, since what they
    read may still roll back.

    ``RoleAssignmentPort`` forgets the principal in this process when its write commits, and,
    with a :class:`GrantsInvalidation`, in every subscribed process. For every other change the
    TTL, counted from when the cached read began, bounds how long a removed grant keeps working:
    a binding removed by a plain document command, or by another process with no broadcast,
    keeps granting here until the entry expires. After committing another change, call
    :meth:`forget` when it touches one principal's own bindings (its role, permission or group
    membership bindings), and :meth:`clear` when it touches what many principals reach: a role's
    permissions or parent, a group's roles, permissions or active flag, or a deleted role or
    permission. Both act in this process only; ``invalidate_grants`` does either once the
    transaction commits, and broadcasts it.

    One cache serves one catalog: its key holds no catalog, so two kernels over different
    catalogs need a cache each.

    Its methods do not await and hold a lock, so decisions and forgets in any number of event
    loops or threads cannot interleave inside one.
    """

    ttl: timedelta
    """How long an entry is served after it was read."""

    max_entries: int = 10_000
    """Entries kept; the least recently used goes first."""

    clock: Callable[[], float] = time.monotonic
    """Seconds, for expiry."""

    _entries: OrderedDict[GrantsCacheKey, tuple[float, CatalogGrants]] = attrs.field(
        factory=OrderedDict[GrantsCacheKey, tuple[float, CatalogGrants]],
        init=False,
    )
    _epoch: int = attrs.field(default=0, init=False)
    _lock: threading.Lock = attrs.field(factory=threading.Lock, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not callable(self.clock):
            raise exc.configuration(f"GrantsCache.clock must be callable, not {self.clock!r}")

        if not isinstance(self.ttl, timedelta) or self.ttl <= timedelta(0):
            raise exc.configuration(
                f"GrantsCache.ttl must be a positive timedelta, not {self.ttl!r}"
            )

        if (
            not isinstance(self.max_entries, int)
            or isinstance(self.max_entries, bool)
            or self.max_entries < 1
        ):
            raise exc.configuration(
                f"GrantsCache.max_entries must be an integer of at least 1, not {self.max_entries!r}"
            )

    # ....................... #

    def begin(self) -> tuple[int, float]:
        """Mark the start of a read: pass what it returns back to :meth:`put`.

        The epoch advances on every :meth:`forget` and :meth:`clear`; the time is when the read
        began, which the entry's TTL counts from.
        """

        with self._lock:
            return self._epoch, self.clock()

    # ....................... #

    def get(self, key: GrantsCacheKey) -> CatalogGrants | None:
        """The grants remembered under *key*, unless absent or expired."""

        with self._lock:
            entry = self._entries.get(key)

            if entry is None:
                return None

            expires_at, grants = entry

            if self.clock() >= expires_at:
                del self._entries[key]
                return None

            self._entries.move_to_end(key)

            return grants

    # ....................... #

    def put(self, key: GrantsCacheKey, grants: CatalogGrants, *, began: tuple[int, float]) -> None:
        """Remember *grants* from a read that began at *began*, as :meth:`begin` returned it.

        The entry expires a TTL after the read began, so a slow read does not stretch the TTL.
        Dropped when a forget ran since: the grants may have been read before the change it
        announces.
        """

        epoch, read_at = began

        # ponytail: one epoch for every principal, so any forget voids every read in flight;
        # per-principal generations if forgets ever become frequent.
        with self._lock:
            expires_at = read_at + self.ttl.total_seconds()

            if epoch != self._epoch:
                return

            self._entries[key] = (expires_at, grants)
            self._entries.move_to_end(key)

            while len(self._entries) > self.max_entries:
                self._entries.popitem(last=False)

    # ....................... #

    def forget(self, principal_id: UUID) -> None:
        """Drop *principal_id*'s entries in every tenant and scope."""

        with self._lock:
            self._epoch += 1

            for key in [key for key in self._entries if key[0] == principal_id]:
                del self._entries[key]

    # ....................... #

    def clear(self) -> None:
        """Drop every entry."""

        with self._lock:
            self._epoch += 1
            self._entries.clear()


# ....................... #

ALL_MARKER: Final = "all"
"""The marker key a catalog-wide invalidation writes."""

PRINCIPAL_PREFIX: Final = "principal:"
"""Starts the marker key of one principal's invalidation, before the principal id."""

_LIVE_WITHIN: Final = timedelta(seconds=10)
"""How long a subscription may take to bring its stream up before startup fails."""

GrantsInvalidationEvent = Literal["publish_failure", "flush"]
"""What a broadcast counts: a committed invalidation whose marker write failed
(``publish_failure``), and a whole grants cache dropped on a received flush or catalog marker
(``flush``)."""


def parse_principal_marker(key: str) -> UUID | None:
    """The principal a marker key names, or ``None`` for any other key."""

    if not key.startswith(PRINCIPAL_PREFIX):
        return None

    try:
        return UUID(key.removeprefix(PRINCIPAL_PREFIX))

    except ValueError:
        return None


@final
@attrs.define(slots=True, kw_only=True)
class GrantsInvalidationCounters:
    """Cumulative broadcast counts, by event, for metrics export."""

    _counts: Counter[GrantsInvalidationEvent] = attrs.field(factory=Counter, init=False)

    # ....................... #

    def add(self, event: GrantsInvalidationEvent) -> None:
        """Count one *event*."""

        self._counts[event] += 1

    def get(self, event: GrantsInvalidationEvent) -> int:
        """How many *event* so far."""

        return self._counts[event]


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class GrantsInvalidation:
    """Broadcasts grants-cache invalidations to every process through a cache backend.

    Pass one to ``AuthzKernelConfig(grants_invalidation=...)`` beside its ``grants_cache``, and
    register ``grants_invalidation_lifecycle_step`` so each process subscribes. Every committed
    invalidation then writes a marker key through :attr:`cache` with ``set_versioned``, the
    write the backend's push tracks, and every subscribed process drops what the marker names:
    one principal's entries, or the whole cache for a catalog change or a gap in the stream.

    Grants stay in each process; only invalidations travel, and the TTL still bounds what a
    broadcast does not reach. Between a commit and the broadcast's arrival another process can
    still serve the old grants. A marker write that fails is logged and counted, not raised: the
    change has committed.

    Until a subscription keeps a kernel's cache current in this process, decisions read the
    bindings instead of the cache. One per kernel, as the cache is.
    """

    cache: CacheSpec
    """A cache spec with a static namespace whose port implements ``SupportsInvalidationPush``,
    used for nothing else; keys that are not markers are ignored."""

    marker_ttl: timedelta = timedelta(minutes=1)
    """How long a marker key lives. Its expiry broadcasts too: a second, idempotent forget for a
    principal, and a second flush for a catalog change."""

    counters: GrantsInvalidationCounters = attrs.field(
        factory=GrantsInvalidationCounters,
        init=False,
        eq=False,
        repr=False,
    )
    """Cumulative publish-failure and flush counts, for metrics export."""

    _listening: list[GrantsCache] = attrs.field(
        factory=list[GrantsCache],
        init=False,
        eq=False,
        repr=False,
    )

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not isinstance(self.cache, CacheSpec):
            raise exc.configuration(
                f"GrantsInvalidation.cache must be a CacheSpec, not {self.cache!r}"
            )

        if not isinstance(self.marker_ttl, timedelta) or self.marker_ttl <= timedelta(0):
            raise exc.configuration(
                "GrantsInvalidation.marker_ttl must be a positive timedelta, not "
                f"{self.marker_ttl!r}"
            )

    # ....................... #

    def listening(self, cache: GrantsCache) -> bool:
        """Whether a subscription in this process keeps *cache* current."""

        return any(held is cache for held in self._listening)

    # ....................... #

    async def subscribe(self, ctx: "ExecutionContext", cache: GrantsCache) -> Unsubscribe:
        """Drop from *cache* what any process's marker names, until the returned call is awaited
        or the stream ends.

        Returns once the stream is live, with *cache* cleared: nothing cached before it can be
        trusted.

        :raises CoreException: ``configuration``: ``authz_grants_invalidation_unavailable``
            when the port of :attr:`cache` cannot push invalidations, declines to, or does not
            bring its stream up within ten seconds; ``authz_grants_invalidation_tenant_aware``
            when that port is tenant-aware.
        """

        port = ctx.cache(self.cache)

        if not implements(port, SupportsInvalidationPush):
            raise exc.configuration(
                f"GrantsInvalidation.cache {self.cache.name!r} resolves to a cache port that "
                "cannot push invalidations, so other processes' revocations would wait out the "
                "TTL here. Route it to a backend that can, such as Redis with invalidation_push.",
                code="authz_grants_invalidation_unavailable",
            )

        backend = backend_of(port)

        if isinstance(backend, TenancyMixin) and backend.tenant_aware:
            raise exc.configuration(
                f"GrantsInvalidation.cache {self.cache.name!r} is routed tenant-aware, so every "
                "marker written with no tenant bound would fail. A marker names a principal, "
                "forgotten in every tenant: route the spec without tenant_aware.",
                code="authz_grants_invalidation_tenant_aware",
            )

        attached = False

        def _detach() -> None:
            nonlocal attached

            if attached:
                attached = False
                self._listening.remove(cache)  # GrantsCache compares by identity

        def _on_invalidation(inv: CacheInvalidation) -> None:
            if inv.key is None or inv.key == ALL_MARKER:
                # A gap in the stream, or a catalog change: nothing cached can be trusted.
                self.counters.add("flush")
                cache.clear()

                if inv.ended:
                    _detach()

            elif (principal_id := parse_principal_marker(inv.key)) is not None:
                cache.forget(principal_id)

        unsubscribe = await cast(SupportsInvalidationPush, port).subscribe_invalidations(
            _on_invalidation, live_within=_LIVE_WITHIN
        )

        if unsubscribe is None:
            raise exc.configuration(
                f"GrantsInvalidation.cache {self.cache.name!r}: its cache port declined to push "
                "invalidations, or its stream did not come up within ten seconds. Push must be "
                "on for its route (invalidation_push), with a static namespace and a client not "
                "routed per tenant; Redis needs redis-py 8 or later and a RESP3 connection.",
                code="authz_grants_invalidation_unavailable",
            )

        # Whenever the transport's own first flush arrives, nothing from before counts.
        cache.clear()
        attached = True
        self._listening.append(cache)

        async def _unsubscribe() -> None:
            _detach()
            await unsubscribe()

        return _unsubscribe

    # ....................... #

    async def publish(self, ctx: "ExecutionContext", principal_id: UUID | None) -> None:
        """Write the marker for *principal_id*, or the catalog marker for ``None``.

        A failure is logged (``authz.grants_invalidation_publish_failed``) and counted, never
        raised: the change has committed, and the TTL bounds the processes the broadcast missed.
        """

        marker = ALL_MARKER if principal_id is None else f"{PRINCIPAL_PREFIX}{principal_id}"
        # A fresh version makes every write a change the backend broadcasts.
        token = uuid4().hex

        try:
            await ctx.cache(self.cache).set_versioned(marker, token, token, ttl=self.marker_ttl)

        except Exception:
            self.counters.add("publish_failure")
            logger.exception(
                "authz.grants_invalidation_publish_failed",
                principal_id=principal_id,
                marker=marker,
            )


async def invalidate_after_commit(
    ctx: "ExecutionContext",
    cache: GrantsCache,
    invalidation: GrantsInvalidation | None,
    principal_id: UUID | None,
) -> None:
    """Forget *principal_id* in *cache*, or clear it for ``None``, once the current transaction
    commits, or now outside one; then publish through *invalidation* when there is one."""

    async def _invalidate() -> None:
        if principal_id is None:
            cache.clear()

        else:
            cache.forget(principal_id)

        if invalidation is not None:
            await invalidation.publish(ctx, principal_id)

    await ctx.tx_ctx.run_or_defer(_invalidate)
