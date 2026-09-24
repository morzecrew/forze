"""Bounded stripe of :class:`asyncio.Lock` objects addressed by an arbitrary key.

Serializing work *per key* — per tenant, per credential, per cache slot — wants one lock
per key, but a ``dict[str, asyncio.Lock]`` filled on demand grows without bound when the
key space is open (tenant ids, secret refs) and needs a lock of its own to fill safely.

A fixed stripe sidesteps both problems: the lock set is bounded regardless of how many
keys appear, and there is nothing to fill. The cost is that two keys hashing to the same
stripe serialize with each other — always *correct*, occasionally less parallel, which is
the right trade for a lock whose job is safety.
"""

from __future__ import annotations

import asyncio
import hashlib
from typing import Any, Final, final

import attrs

# ----------------------- #

LOCK_STRIPES: Final[int] = 64
"""Number of stripes. Bounds the lock set; collisions only cost parallelism."""


@attrs.define(slots=True)
class AsyncLockTable:
    """Marker for a table of :class:`asyncio.Lock` objects held across calls.

    Machinery rather than data, and the distinction has teeth: a reset that replaced one of
    these while a caller waited on a lock inside it would hand the next caller a *different*
    lock and silently break whatever the first was serializing. Anything that clears a store
    wholesale exempts these **by type**, so a table added later is exempt without anyone
    remembering to add its name to a list.
    """


@final
@attrs.define(slots=True)
class PerOwnerAsyncLocks(AsyncLockTable):
    """One lock per key, per event loop — exact rather than striped.

    Striping is the right trade where collisions only cost parallelism. It is the wrong one
    where the *absence* of a collision is the property being observed: a simulation asserting
    that two different owners proceed at once would fail against a shared stripe, and one
    asserting they do not would pass for the wrong reason.

    Keyed by loop because an :class:`asyncio.Lock` belongs to the loop it was awaited on, and a
    simulation gives each attempt a loop of its own while the state holding this table outlives
    them all. A loop's table is dropped once the loop reports itself closed, so what is retained
    is the keys one live loop has seen.
    """

    _tables: dict[Any, dict[int, asyncio.Lock]] = attrs.field(factory=dict, init=False)
    _holders: dict[int, Any] = attrs.field(factory=dict, init=False)
    """Key → whoever holds it, for a caller that has to know before it waits."""

    # ....................... #

    def hold(self, key: int, holder: Any) -> None:
        """Record that *holder* has taken *key*."""

        self._holders[key] = holder

    # ....................... #

    def release(self, key: int) -> None:
        """Forget who held *key*."""

        self._holders.pop(key, None)

    # ....................... #

    def holder_of(self, key: int) -> Any:
        """Whoever holds *key*, or ``None``."""

        return self._holders.get(key)

    # ....................... #

    def for_key(self, key: int) -> asyncio.Lock:
        """The lock serializing *key* on the running loop."""

        loop = asyncio.get_running_loop()

        for finished in [entry for entry in self._tables if entry.is_closed()]:
            del self._tables[finished]

        return self._tables.setdefault(loop, {}).setdefault(key, asyncio.Lock())

    # ....................... #

    def held(self) -> tuple[asyncio.Lock, ...]:
        """Every lock currently held on the running loop — for a test to assert none is."""

        loop = asyncio.get_running_loop()

        return tuple(lock for lock in self._tables.get(loop, {}).values() if lock.locked())


@final
@attrs.define(slots=True)
class StripedAsyncLocks(AsyncLockTable):
    """Keyed in-process serialization over a fixed number of locks.

    Guards a critical section against concurrency *within one process*. It is not a
    substitute for a distributed lock or a row lock — pair it with one whenever more
    than one process can run the same section, and treat this layer as the cheap
    front line that collapses same-process racers before they reach the expensive one.
    """

    _locks: tuple[asyncio.Lock, ...] = attrs.field(
        factory=lambda: tuple(asyncio.Lock() for _ in range(LOCK_STRIPES)),
        init=False,
        repr=False,
    )

    # ....................... #

    def for_key(self, key: str) -> asyncio.Lock:
        """Return the lock serializing *key*.

        The mapping is a content digest rather than :func:`hash`, so a key lands on the
        same stripe in every process and every run — which keeps a forced-collision test
        meaningful instead of hash-seed dependent.
        """

        digest = hashlib.blake2b(key.encode("utf-8"), digest_size=8).digest()

        return self._locks[int.from_bytes(digest, "big") % LOCK_STRIPES]
