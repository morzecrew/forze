"""Async LRU registries for resources that require explicit disposal."""

import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Generic,
    Hashable,
    OrderedDict,
    TypeVar,
    final,
)

import attrs

from forze.base.exceptions import exc

# ----------------------- #

K = TypeVar("K", bound=Hashable)
V = TypeVar("V")
R = TypeVar("R", bound=Hashable, default=Any)

_MAX_DRAIN_WAIT_ATTEMPTS = 64

_REENTRANT_CREATE_MSG = (
    "Reentrant LRU registry access for the same slot during create; "
    "do not call use() or get_or_create() from create()."
)

_creating_slot: ContextVar[Any | None] = ContextVar(
    "lru_registry_creating_slot",
    default=None,
)

# ....................... #


def _validate_max_entries(max_entries: int) -> None:
    if max_entries < 1:
        raise exc.internal("max_entries must be at least 1")


# ....................... #


@attrs.define(slots=True)
class _DedupIndex(Generic[K, R]):
    """Maps logical keys to deduplicated slot keys with refcounting."""

    dedup_key: Callable[[K], R] | None = None

    logical_to_resource: dict[K, R] = attrs.field(factory=dict)
    resource_refcount: dict[R, int] = attrs.field(factory=dict)
    resource_to_keys: dict[R, set[K]] = attrs.field(factory=dict)

    # ....................... #

    def slot_for(self, key: K) -> R:
        if self.dedup_key is None:
            return key  # type: ignore[return-value]

        existing = self.logical_to_resource.get(key)

        if existing is not None:
            return existing

        slot = self.dedup_key(key)
        self.logical_to_resource[key] = slot
        self.resource_refcount[slot] = self.resource_refcount.get(slot, 0) + 1
        self.resource_to_keys.setdefault(slot, set()).add(key)

        return slot

    # ....................... #

    def release(self, key: K) -> R | None:
        """Drop *key*; return slot to evict when refcount reaches zero."""

        if self.dedup_key is None:
            return key  # type: ignore[return-value]

        slot = self.logical_to_resource.pop(key, None)

        if slot is None:
            return None

        keys = self.resource_to_keys.get(slot)

        if keys is not None:
            keys.discard(key)

            if not keys:
                del self.resource_to_keys[slot]

        self.resource_refcount[slot] -= 1

        if self.resource_refcount[slot] <= 0:
            del self.resource_refcount[slot]
            return slot

        return None

    # ....................... #

    def release_slot(self, slot: R) -> None:
        """Drop every logical key mapped to *slot* (called when the slot is LRU-evicted).

        Without this, the forward and refcount maps would retain entries for evicted
        slots and grow unbounded with the number of distinct logical keys ever seen.
        Callers must hold the registry lock so the captured key set is consistent.
        """

        if self.dedup_key is None:
            return

        for key in self.resource_to_keys.pop(slot, set()):
            self.logical_to_resource.pop(key, None)

        self.resource_refcount.pop(slot, None)

    # ....................... #

    def clear(self) -> None:
        self.logical_to_resource.clear()
        self.resource_refcount.clear()
        self.resource_to_keys.clear()


# ....................... #


@final
@attrs.define(slots=True)
class SimpleLruRegistry(Generic[K, V, R]):
    """LRU map with async create/dispose; evicts oldest entry when over capacity.

      Eviction calls ``dispose`` immediately, including while other keys are in use.

      When ``dedup_key`` is set, LRU slots are keyed by ``dedup_key(logical_key)`` while
    ``create`` still receives the logical key. Multiple logical keys may share one slot.
    """

    max_entries: int
    """Maximum number of entries in the registry."""

    create: Callable[[K], Awaitable[V]] = attrs.field(repr=False, eq=False)
    """Function to create a new value."""

    dispose: Callable[[V], Awaitable[None]] = attrs.field(repr=False, eq=False)
    """Function to dispose a value."""

    dedup_key: Callable[[K], R] | None = attrs.field(default=None, repr=False, eq=False)
    """When set, LRU slots are keyed by ``dedup_key(logical_key)``."""

    # ....................... #

    _lock: asyncio.Lock = attrs.field(
        factory=asyncio.Lock,
        init=False,
        repr=False,
    )
    _entries: OrderedDict[R, V] = attrs.field(
        factory=OrderedDict,
        init=False,
        repr=False,
    )
    _init_locks: dict[R, asyncio.Lock] = attrs.field(
        factory=dict,
        init=False,
        repr=False,
    )
    _dedup: _DedupIndex[K, R] = attrs.field(init=False, repr=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        _validate_max_entries(self.max_entries)

        self._dedup = _DedupIndex(dedup_key=self.dedup_key)

    # ....................... #

    @staticmethod
    def _assert_slot_not_creating(slot: R) -> None:
        if _creating_slot.get() == slot:
            raise exc.internal(_REENTRANT_CREATE_MSG)

    # ....................... #

    async def _invoke_create(self, key: K, slot: R) -> V:
        token = _creating_slot.set(slot)

        try:
            return await self.create(key)

        finally:
            _creating_slot.reset(token)

    # ....................... #

    def peek(self, key: K) -> V | None:
        """Return a cached value without LRU touch (best-effort, no lock)."""

        if self.dedup_key is not None:
            slot = self._dedup.logical_to_resource.get(key)

            if slot is None:
                return None

        else:
            slot = key  # type: ignore[assignment]

        return self._entries.get(slot)  # type: ignore[arg-type]

    # ....................... #

    async def _lock_for_init(self, slot: R) -> asyncio.Lock:
        async with self._lock:
            init_lock = self._init_locks.get(slot)

            if init_lock is None:
                init_lock = asyncio.Lock()
                self._init_locks[slot] = init_lock

            return init_lock

    # ....................... #

    async def get_or_create(self, key: K) -> V:
        """Return an existing value or create, register, and LRU-evict overflow."""

        async with self._lock:
            slot = self._dedup.slot_for(key)

            if slot in self._entries:
                value = self._entries[slot]
                self._entries.move_to_end(slot)
                return value

        self._assert_slot_not_creating(slot)

        init_lock = await self._lock_for_init(slot)

        async with init_lock:
            async with self._lock:
                if slot in self._entries:
                    value = self._entries[slot]
                    self._entries.move_to_end(slot)
                    return value

            value = await self._invoke_create(key, slot)

            evicted: list[V] = []

            async with self._lock:
                if slot in self._entries:
                    await self.dispose(value)
                    existing = self._entries[slot]
                    self._entries.move_to_end(slot)
                    return existing

                self._entries[slot] = value
                self._entries.move_to_end(slot)

                while len(self._entries) > self.max_entries:
                    evicted_slot, old = self._entries.popitem(last=False)
                    self._dedup.release_slot(evicted_slot)
                    self._init_locks.pop(evicted_slot, None)
                    evicted.append(old)

            for old in evicted:
                await self.dispose(old)

            return value

    # ....................... #

    async def evict(self, key: K) -> None:
        """Remove *key* and dispose its value when the deduplicated slot is unused."""

        async with self._lock:
            slot = self._dedup.release(key)

            if slot is None:
                return

            self._init_locks.pop(slot, None)
            value = self._entries.pop(slot, None)

        if value is not None:
            await self.dispose(value)

    # ....................... #

    async def close_all(self) -> None:
        """Dispose all entries and clear the registry."""

        async with self._lock:
            values = list(self._entries.values())
            self._entries.clear()
            self._init_locks.clear()
            self._dedup.clear()

        for value in values:
            await self.dispose(value)


# ....................... #


@attrs.define
class _GuardedEntry(Generic[V, R]):
    """Registry slot with in-flight refcount for safe LRU eviction."""

    key: R
    """Slot key for the entry."""

    value: V
    """Value for the entry."""

    on_finish_drain: Callable[[R], Awaitable[None]]
    """Function to finish the drain."""

    dispose: Callable[[V], Awaitable[None]]
    """Function to dispose the value."""

    refcount: int = 0
    """Reference count for the entry."""

    drain_after_idle: bool = False
    """Whether to drain the entry after it is idle."""

    condition: asyncio.Condition = attrs.field(factory=asyncio.Condition)
    """Condition for the entry to be used."""

    draining_barrier: asyncio.Event = attrs.field(factory=asyncio.Event)
    """Barrier for the entry to be drained."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        self.draining_barrier.set()

    # ....................... #

    def mark_draining(self) -> None:
        """Block :meth:`wait_until_drained` until the entry leaves the draining map."""

        self.draining_barrier.clear()

    # ....................... #

    def reserve(self) -> None:
        """Synchronously increment :attr:`refcount`; callers hold the registry lock.

        Reserving while the registry lock is held makes lookup and refcount
        increment atomic with respect to eviction, which inspects ``refcount``
        under the same lock — so eviction can never observe ``refcount == 0``
        for an entry a ``use`` scope is about to receive.
        """

        self.refcount += 1

    # ....................... #

    @asynccontextmanager
    async def use_reserved(self) -> AsyncGenerator[V]:
        """Yield :attr:`value` for a reservation taken via :meth:`reserve`.

        Decrements the refcount on exit; when the entry is draining and idle,
        disposes the value. The slot is always deregistered (even when
        ``dispose`` raises) so the draining barrier cannot wedge future users.

        There is no suspension point before the ``try`` block, so a reservation
        taken under the registry lock is released exactly once.
        """

        try:
            yield self.value

        finally:
            do_finish_drain = False

            async with self.condition:
                self.refcount -= 1
                do_finish_drain = self.refcount == 0 and self.drain_after_idle
                self.condition.notify_all()

            if do_finish_drain:
                self.drain_after_idle = False

                try:
                    await self.dispose(self.value)

                finally:
                    # Dispose failure must still deregister the slot and lift
                    # the draining barrier, or future use() of it would hang.
                    await self.on_finish_drain(self.key)

                    async with self.condition:
                        self.condition.notify_all()

    # ....................... #

    async def wait_until_drained(self) -> None:
        """Wait until this entry has been disposed and deregistered from draining."""

        await self.draining_barrier.wait()


# ....................... #


@final
@attrs.define(slots=True)
class GuardedLruRegistry(Generic[K, V, R]):
    """LRU map that defers ``dispose`` until in-flight ``use`` scopes complete.

    When capacity is exceeded or :meth:`evict` is called on an in-use entry, the
    entry moves to an internal draining set until the last ``use`` scope exits.

    When ``dedup_key`` is set, LRU slots are keyed by ``dedup_key(logical_key)`` while
    ``create`` still receives the logical key.

    Do not call :meth:`use` or nested registry access for the same slot from
    :meth:`create` (raises :exc:`~forze.base.errors.exc.internal` instead of deadlocking).
    """

    max_entries: int
    """Maximum number of entries in the registry."""

    create: Callable[[K], Awaitable[V]] = attrs.field(repr=False, eq=False)
    """Function to create a new value."""

    dispose: Callable[[V], Awaitable[None]] = attrs.field(repr=False, eq=False)
    """Function to dispose a value."""

    dedup_key: Callable[[K], R] | None = attrs.field(default=None, repr=False, eq=False)
    """When set, LRU slots are keyed by ``dedup_key(logical_key)``."""

    # ....................... #

    _registry_lock: asyncio.Lock = attrs.field(
        factory=asyncio.Lock,
        init=False,
        repr=False,
    )
    _slots: OrderedDict[R, _GuardedEntry[V, R]] = attrs.field(
        factory=OrderedDict,
        init=False,
        repr=False,
    )
    _draining: dict[R, _GuardedEntry[V, R]] = attrs.field(
        factory=dict,
        init=False,
        repr=False,
    )
    _init_locks: dict[R, asyncio.Lock] = attrs.field(
        factory=dict,
        init=False,
        repr=False,
    )
    _dedup: _DedupIndex[K, R] = attrs.field(init=False, repr=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        _validate_max_entries(self.max_entries)

        self._dedup = _DedupIndex(dedup_key=self.dedup_key)

    # ....................... #

    @staticmethod
    def _assert_slot_not_creating(slot: R) -> None:
        if _creating_slot.get() == slot:
            raise exc.internal(_REENTRANT_CREATE_MSG)

    # ....................... #

    async def _invoke_create(self, key: K, slot: R) -> V:
        token = _creating_slot.set(slot)

        try:
            return await self.create(key)

        finally:
            _creating_slot.reset(token)

    # ....................... #

    def peek(self, key: K) -> V | None:
        """Best-effort value lookup from active or draining maps (no lock)."""

        if self.dedup_key is not None:
            slot = self._dedup.logical_to_resource.get(key)

            if slot is None:
                return None

        else:
            slot = key  # type: ignore[assignment]

        entry = self._slots.get(slot)  # type: ignore[arg-type]

        if entry is not None:
            return entry.value

        draining = self._draining.get(slot)  # type: ignore[arg-type]

        if draining is not None:
            return draining.value

        return None

    # ....................... #

    async def _finish_drain(self, slot: R) -> None:
        entry: _GuardedEntry[V, R] | None = None

        async with self._registry_lock:
            entry = self._draining.pop(slot, None)

        if entry is not None:
            async with entry.condition:
                entry.condition.notify_all()

            entry.draining_barrier.set()

    # ....................... #

    async def _lock_for_init(self, slot: R) -> asyncio.Lock:
        async with self._registry_lock:
            lock = self._init_locks.get(slot)

            if lock is None:
                lock = asyncio.Lock()
                self._init_locks[slot] = lock

            return lock

    # ....................... #

    async def _await_not_draining(self, slot: R) -> None:
        for _ in range(_MAX_DRAIN_WAIT_ATTEMPTS):
            async with self._registry_lock:
                entry = self._draining.get(slot)

                if entry is None:
                    return

            await entry.wait_until_drained()
            await asyncio.sleep(0)

        raise exc.internal("Timed out waiting for LRU registry slot to finish draining")

    # ....................... #

    def _make_entry(self, slot: R, value: V) -> _GuardedEntry[V, R]:
        return _GuardedEntry(
            key=slot,
            value=value,
            on_finish_drain=self._finish_drain,
            dispose=self.dispose,
        )

    # ....................... #

    async def _evict_overflow(self) -> list[V]:
        immediate_close: list[V] = []

        async with self._registry_lock:
            while len(self._slots) > self.max_entries:
                old_key, old_entry = self._slots.popitem(last=False)
                self._dedup.release_slot(old_key)
                self._init_locks.pop(old_key, None)

                if old_entry.refcount == 0:
                    immediate_close.append(old_entry.value)

                else:
                    old_entry.mark_draining()
                    old_entry.drain_after_idle = True
                    self._draining[old_key] = old_entry

        return immediate_close

    # ....................... #

    @asynccontextmanager
    async def use(self, key: K) -> AsyncGenerator[V]:
        """Resolve *key*, LRU-touch, create if needed, and yield with refcount guard."""

        async with self._registry_lock:
            slot = self._dedup.slot_for(key)

        self._assert_slot_not_creating(slot)

        await self._await_not_draining(slot)

        entry: _GuardedEntry[V, R] | None = None

        async with self._registry_lock:
            if slot in self._slots:
                entry = self._slots[slot]
                self._slots.move_to_end(slot)
                entry.reserve()

        if entry is not None:
            async with entry.use_reserved() as value:
                yield value

            return

        init_lock = await self._lock_for_init(slot)

        async with init_lock:
            await self._await_not_draining(slot)

            entry = None

            async with self._registry_lock:
                if slot in self._slots:
                    entry = self._slots[slot]
                    self._slots.move_to_end(slot)
                    entry.reserve()

            if entry is not None:
                async with entry.use_reserved() as value:
                    yield value

                return

            value = await self._invoke_create(key, slot)

            async with self._registry_lock:
                if slot in self._slots:
                    existing = self._slots[slot]
                    self._slots.move_to_end(slot)
                    existing.reserve()

                else:
                    existing = None

            if existing is not None:
                # Dispose the losing duplicate inside the reserved-use scope and
                # outside the registry lock: disposal may be slow I/O, and if it
                # raises, ``use_reserved`` still releases the reservation taken
                # above (otherwise the entry would be unevictable forever).
                async with existing.use_reserved() as v:
                    await self.dispose(value)
                    yield v

                return

            new_entry = self._make_entry(slot, value)

            async with self._registry_lock:
                self._slots[slot] = new_entry
                self._slots.move_to_end(slot)
                new_entry.reserve()

            # The reservation taken above is owned by this scope from here on:
            # overflow eviction (here or in a concurrent task) sees a non-zero
            # refcount and drains instead of disposing the value in use.
            async with new_entry.use_reserved() as v:
                immediate_close = await self._evict_overflow()

                for old in immediate_close:
                    await self.dispose(old)

                yield v

    # ....................... #

    async def evict(self, key: K) -> None:
        """Remove *key*; dispose immediately if idle, else drain when last use ends."""

        async with self._registry_lock:
            slot = self._dedup.release(key)

            if slot is None:
                return

            self._init_locks.pop(slot, None)
            entry = self._slots.pop(slot, None)

            if entry is None:
                entry = self._draining.pop(slot, None)

            if entry is not None:
                if entry.refcount == 0:
                    immediate = entry.value

                else:
                    entry.mark_draining()
                    entry.drain_after_idle = True
                    self._draining[slot] = entry
                    immediate = None

            else:
                immediate = None

        if immediate is not None:
            await self.dispose(immediate)

    # ....................... #

    async def close_all(self) -> None:
        """Dispose all active and draining entries and reset internal state."""

        async with self._registry_lock:
            to_close = [e.value for e in self._slots.values()] + [
                e.value for e in self._draining.values()
            ]
            self._slots.clear()
            self._draining.clear()
            self._init_locks.clear()
            self._dedup.clear()

        for value in to_close:
            await self.dispose(value)
