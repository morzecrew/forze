"""Async LRU registries for resources that require explicit disposal."""

import asyncio
from contextlib import asynccontextmanager
from typing import (
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

# ....................... #


@final
@attrs.define(slots=True)
class SimpleLruRegistry(Generic[K, V]):
    """LRU map with async create/dispose; evicts oldest entry when over capacity.

    Eviction calls ``dispose`` immediately, including while other keys are in use.
    """

    max_entries: int
    """Maximum number of entries in the registry."""

    create: Callable[[K], Awaitable[V]] = attrs.field(repr=False, eq=False)
    """Function to create a new value."""

    dispose: Callable[[V], Awaitable[None]] = attrs.field(repr=False, eq=False)
    """Function to dispose a value."""

    # ....................... #

    _lock: asyncio.Lock = attrs.field(
        factory=asyncio.Lock,
        init=False,
        repr=False,
    )
    _entries: OrderedDict[K, V] = attrs.field(
        factory=OrderedDict,
        init=False,
        repr=False,
    )
    _init_locks: dict[K, asyncio.Lock] = attrs.field(
        factory=dict,
        init=False,
        repr=False,
    )

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.max_entries < 1:
            raise exc.internal("max_entries must be at least 1")

    # ....................... #

    def peek(self, key: K) -> V | None:
        """Return a cached value without LRU touch (best-effort, no lock)."""

        return self._entries.get(key)

    # ....................... #

    async def _lock_for_init(self, key: K) -> asyncio.Lock:
        async with self._lock:
            init_lock = self._init_locks.get(key)

            if init_lock is None:
                init_lock = asyncio.Lock()
                self._init_locks[key] = init_lock

            return init_lock

    # ....................... #

    async def get_or_create(self, key: K) -> V:
        """Return an existing value or create, register, and LRU-evict overflow."""

        async with self._lock:
            if key in self._entries:
                value = self._entries[key]
                self._entries.move_to_end(key)
                return value

        init_lock = await self._lock_for_init(key)

        async with init_lock:
            async with self._lock:
                if key in self._entries:
                    value = self._entries[key]
                    self._entries.move_to_end(key)
                    return value

            value = await self.create(key)

            evicted: list[V] = []

            async with self._lock:
                if key in self._entries:
                    await self.dispose(value)
                    existing = self._entries[key]
                    self._entries.move_to_end(key)
                    return existing

                self._entries[key] = value
                self._entries.move_to_end(key)

                while len(self._entries) > self.max_entries:
                    _, old = self._entries.popitem(last=False)
                    evicted.append(old)

            for old in evicted:
                await self.dispose(old)

            return value

    # ....................... #

    async def evict(self, key: K) -> None:
        """Remove *key* and dispose its value if present."""

        async with self._lock:
            self._init_locks.pop(key, None)
            value = self._entries.pop(key, None)

        if value is not None:
            await self.dispose(value)

    # ....................... #

    async def close_all(self) -> None:
        """Dispose all entries and clear the registry."""

        async with self._lock:
            values = list(self._entries.values())
            self._entries.clear()
            self._init_locks.clear()

        for value in values:
            await self.dispose(value)


# ....................... #


@attrs.define
class _GuardedEntry(Generic[K, V]):
    """Registry slot with in-flight refcount for safe LRU eviction."""

    key: K
    """Key for the entry."""

    value: V
    """Value for the entry."""

    on_finish_drain: Callable[[K], Awaitable[None]]
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

    @asynccontextmanager
    async def use(self) -> AsyncGenerator[V]:
        """Increment refcount around work on :attr:`value`; dispose when draining and idle."""

        async with self.condition:
            self.refcount += 1

        try:
            yield self.value

        finally:
            do_finish_drain = False

            async with self.condition:
                self.refcount -= 1
                do_finish_drain = self.refcount == 0 and self.drain_after_idle
                self.condition.notify_all()

            if do_finish_drain:
                await self.dispose(self.value)
                self.drain_after_idle = False
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
class GuardedLruRegistry(Generic[K, V]):
    """LRU map that defers ``dispose`` until in-flight ``use`` scopes complete.

    When capacity is exceeded or :meth:`evict` is called on an in-use entry, the
    entry moves to an internal draining set until the last ``use`` scope exits.
    """

    max_entries: int
    """Maximum number of entries in the registry."""

    create: Callable[[K], Awaitable[V]] = attrs.field(repr=False, eq=False)
    """Function to create a new value."""

    dispose: Callable[[V], Awaitable[None]] = attrs.field(repr=False, eq=False)
    """Function to dispose a value."""

    # ....................... #

    _registry_lock: asyncio.Lock = attrs.field(
        factory=asyncio.Lock,
        init=False,
        repr=False,
    )
    _slots: OrderedDict[K, _GuardedEntry[K, V]] = attrs.field(
        factory=OrderedDict,
        init=False,
        repr=False,
    )
    _draining: dict[K, _GuardedEntry[K, V]] = attrs.field(
        factory=dict,
        init=False,
        repr=False,
    )
    _init_locks: dict[K, asyncio.Lock] = attrs.field(
        factory=dict,
        init=False,
        repr=False,
    )

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.max_entries < 1:
            raise exc.internal("max_entries must be at least 1")

    # ....................... #

    def peek(self, key: K) -> V | None:
        """Best-effort value lookup from active or draining maps (no lock)."""

        entry = self._slots.get(key)

        if entry is not None:
            return entry.value

        draining = self._draining.get(key)

        if draining is not None:
            return draining.value

        return None

    # ....................... #

    async def _finish_drain(self, key: K) -> None:
        entry: _GuardedEntry[K, V] | None = None

        async with self._registry_lock:
            entry = self._draining.pop(key, None)

        if entry is not None:
            async with entry.condition:
                entry.condition.notify_all()

            entry.draining_barrier.set()

    # ....................... #

    async def _lock_for_init(self, key: K) -> asyncio.Lock:
        async with self._registry_lock:
            lock = self._init_locks.get(key)

            if lock is None:
                lock = asyncio.Lock()
                self._init_locks[key] = lock

            return lock

    # ....................... #

    async def _await_not_draining(self, key: K) -> None:
        while True:
            async with self._registry_lock:
                entry = self._draining.get(key)

                if entry is None:
                    return

            await entry.wait_until_drained()

    # ....................... #

    def _make_entry(self, key: K, value: V) -> _GuardedEntry[K, V]:
        return _GuardedEntry(
            key=key,
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

        await self._await_not_draining(key)

        entry: _GuardedEntry[K, V] | None = None

        async with self._registry_lock:
            if key in self._slots:
                entry = self._slots[key]
                self._slots.move_to_end(key)

        if entry is not None:
            async with entry.use():
                yield entry.value

            return

        init_lock = await self._lock_for_init(key)

        async with init_lock:
            await self._await_not_draining(key)

            entry = None

            async with self._registry_lock:
                if key in self._slots:
                    entry = self._slots[key]
                    self._slots.move_to_end(key)

            if entry is not None:
                async with entry.use():
                    yield entry.value

                return

            value = await self.create(key)

            async with self._registry_lock:
                if key in self._slots:
                    await self.dispose(value)
                    existing = self._slots[key]
                    self._slots.move_to_end(key)

                else:
                    existing = None

            if existing is not None:
                async with existing.use():
                    yield existing.value

                return

            new_entry = self._make_entry(key, value)

            async with self._registry_lock:
                self._slots[key] = new_entry
                self._slots.move_to_end(key)

            immediate_close = await self._evict_overflow()

            for v in immediate_close:
                await self.dispose(v)

            async with new_entry.use():
                yield new_entry.value

    # ....................... #

    async def evict(self, key: K) -> None:
        """Remove *key*; dispose immediately if idle, else drain when last use ends."""

        immediate: V | None = None

        async with self._registry_lock:
            self._init_locks.pop(key, None)
            entry = self._slots.pop(key, None)

            if entry is None:
                entry = self._draining.pop(key, None)

            if entry is not None:
                if entry.refcount == 0:
                    immediate = entry.value

                else:
                    entry.mark_draining()
                    entry.drain_after_idle = True
                    self._draining[key] = entry

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

        for value in to_close:
            await self.dispose(value)
