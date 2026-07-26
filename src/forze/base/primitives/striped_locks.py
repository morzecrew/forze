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
from typing import Final, final

import attrs

# ----------------------- #

LOCK_STRIPES: Final[int] = 64
"""Number of stripes. Bounds the lock set; collisions only cost parallelism."""


@final
@attrs.define(slots=True)
class StripedAsyncLocks:
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
