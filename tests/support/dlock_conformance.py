"""Shared distributed-lock conformance battery.

The Redis lock and the in-memory mock implement the same command and query ports, and the
mock is what unit tests and simulation resolve. So every promise below runs against both:

1. The lock's own lifecycle — exclusion, owner, release by owner only, and a fencing token
   that rises by one per acquisition while ``reset`` keeps it.
2. ``advance_fence`` moves the counter to the larger of its value and ``past``: below or at
   the counter nothing changes and the counter is returned; above it the next acquisition
   draws ``past + 1``. On a key with no counter (a total loss) ``past=0`` leaves the next
   token at 1.
3. It never lowers the counter, so racing advances settle on the largest.
4. It needs no lock ownership and does not touch a held lock.
5. A ``past`` outside ``[0, 2**53 - 1]``, a bool, an ``int`` subclass or a non-integer is
   refused before anything is sent, and the counter is unchanged.
6. Large counters round-trip exactly: no exponent formatting on the way in or out.
7. A tenant-bound lock advances the counter its own ``acquire`` increments, and only that
   one.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from typing import Any, Protocol
from uuid import UUID, uuid4

import attrs
import pytest

from forze.application.contracts.dlock import (
    FENCE_MAX_VALUE,
    FENCE_OUT_OF_RANGE_CODE,
    DistributedLockCommandPort,
    DistributedLockQueryPort,
    FenceAdvancing,
    FencingAware,
)
from forze.base.exceptions import CoreException

# ----------------------- #


class FencedLock(
    DistributedLockCommandPort,
    DistributedLockQueryPort,
    FencingAware,
    FenceAdvancing,
    Protocol,
):
    """What both backends are: a command and query port that fences and can advance."""


@attrs.define(slots=True, kw_only=True, frozen=True)
class DlockHarness:
    """One backend's seam for the battery."""

    lock: FencedLock
    """The adapter under test, with no tenant bound."""

    for_tenant: Callable[[UUID], FencedLock]
    """Build the same backend bound to a tenant, over the same store."""


Check = Callable[[DlockHarness], Awaitable[Any]]
"""One battery check."""


def _key() -> str:
    return f"k-{uuid4().hex[:8]}"


async def _draw(lock: FencedLock, key: str) -> int:
    """Acquire and release once, returning the token drawn."""

    acquired = await lock.acquire(key, "drawer")

    assert acquired is not None and acquired.token is not None
    assert await lock.release(key, "drawer") is True

    return acquired.token


async def _draw_to(lock: FencedLock, key: str, count: int) -> None:
    for _ in range(count):
        await _draw(lock, key)


# ....................... #


async def check_reports_fence_advance(h: DlockHarness) -> None:
    """Both backends report the capability the fail-closed resolve reads."""

    caps = h.lock.capabilities()

    assert caps.fencing_tokens is True
    assert caps.fence_advance is True
    assert isinstance(h.lock, FenceAdvancing)


async def check_lock_lifecycle(h: DlockHarness) -> None:
    """Exclusion, owner, owner-only release, and a token that rises by one per acquisition."""

    key = _key()
    first = await h.lock.acquire(key, "alice")

    assert first is not None and first.token == 1
    assert await h.lock.is_locked(key) is True
    assert await h.lock.get_owner(key) == "alice"
    ttl = await h.lock.get_ttl(key)
    assert ttl is not None and ttl.total_seconds() > 0

    assert await h.lock.acquire(key, "bob") is None
    assert await h.lock.release(key, "bob") is False
    assert await h.lock.reset(key, "alice") is True
    assert await h.lock.release(key, "alice") is True
    assert await h.lock.is_locked(key) is False
    assert await h.lock.get_owner(key) is None

    # reset kept the generation, so the next token is exactly one higher.
    assert await _draw(h.lock, key) == 2


async def check_advance_above_counter_moves_next_token(h: DlockHarness) -> None:
    """Above the counter, the counter becomes ``past`` and the next token is ``past + 1``."""

    key = _key()
    await _draw_to(h.lock, key, 3)

    assert await h.lock.advance_fence(key, past=10) == 10
    assert await _draw(h.lock, key) == 11


async def check_advance_at_or_below_counter_changes_nothing(h: DlockHarness) -> None:
    """At or below the counter, nothing moves and the stored counter is returned."""

    key = _key()
    await _draw_to(h.lock, key, 5)

    assert await h.lock.advance_fence(key, past=5) == 5
    assert await h.lock.advance_fence(key, past=2) == 5
    assert await h.lock.advance_fence(key, past=0) == 5
    assert await _draw(h.lock, key) == 6


async def check_advance_on_a_lost_counter(h: DlockHarness) -> None:
    """A key with no counter — a total loss — starts from ``past``, zero included."""

    lost = _key()

    assert await h.lock.advance_fence(lost, past=0) == 0
    assert await _draw(h.lock, lost) == 1

    lost_above = _key()

    assert await h.lock.advance_fence(lost_above, past=50) == 50
    assert await _draw(h.lock, lost_above) == 51


async def check_racing_advances_settle_on_the_largest(h: DlockHarness) -> None:
    """The counter never goes down, so concurrent advances end at the largest ``past``."""

    key = _key()
    rng = random.Random(7)
    pasts = [rng.randrange(1, 1_000) for _ in range(20)]

    # Batches of four stay inside a small client pool; each batch is a real race.
    for start in range(0, len(pasts), 4):
        batch = pasts[start : start + 4]
        results = await asyncio.gather(*(h.lock.advance_fence(key, past=p) for p in batch))

        for past, result in zip(batch, results, strict=True):
            assert result >= past, f"advance to {past} returned {result}"

    assert await h.lock.advance_fence(key, past=0) == max(pasts)
    assert await _draw(h.lock, key) == max(pasts) + 1


async def check_advance_needs_no_ownership(h: DlockHarness) -> None:
    """Anyone may advance, and a held lock keeps its owner and its token."""

    key = _key()
    held = await h.lock.acquire(key, "holder")
    assert held is not None and held.token == 1

    assert await h.lock.advance_fence(key, past=100) == 100

    assert await h.lock.get_owner(key) == "holder"
    assert await h.lock.reset(key, "holder") is True
    assert await h.lock.release(key, "holder") is True
    assert await _draw(h.lock, key) == 101


class _ExponentInt(int):
    """An ``int`` that prints as ``1e20``: in range by value, garbage on the wire."""

    def __str__(self) -> str:
        return "1e20"


_OUT_OF_RANGE: tuple[object, ...] = (
    -1,
    FENCE_MAX_VALUE + 1,
    2**63,
    True,
    False,
    _ExponentInt(7),
    1.5,
    "5",
    None,
)


async def check_out_of_range_past_is_refused_before_sending(h: DlockHarness) -> None:
    """A bad ``past`` raises the shared precondition and leaves the counter where it was.

    ``FENCE_MAX_VALUE + 1`` is the value that would land if it were sent: Redis would store
    it. An unchanged counter is what shows it never left the process.
    """

    key = _key()
    await _draw_to(h.lock, key, 2)

    for bad in _OUT_OF_RANGE:
        with pytest.raises(CoreException) as caught:
            await h.lock.advance_fence(key, past=bad)  # type: ignore[arg-type]

        assert caught.value.code == FENCE_OUT_OF_RANGE_CODE, f"past={bad!r}"

    assert await h.lock.advance_fence(key, past=0) == 2
    assert await _draw(h.lock, key) == 3


async def check_large_counters_round_trip_exactly(h: DlockHarness) -> None:
    """Values past Lua's ``%.14g`` range come back digit for digit, both ways."""

    for big in (10**15 + 1, FENCE_MAX_VALUE):
        key = _key()

        assert await h.lock.advance_fence(key, past=big) == big
        # The no-change branch returns the stored value rather than the argument.
        assert await h.lock.advance_fence(key, past=big - 1) == big
        assert await _draw(h.lock, key) == big + 1


async def check_tenant_advance_moves_its_own_counter(h: DlockHarness) -> None:
    """A tenant-bound advance moves the counter that tenant's ``acquire`` draws from."""

    key = _key()
    lock_a = h.for_tenant(uuid4())
    lock_b = h.for_tenant(uuid4())

    assert await lock_a.advance_fence(key, past=50) == 50
    assert await _draw(lock_a, key) == 51

    # The other tenant, and the unbound lock, keep counters of their own.
    assert await _draw(lock_b, key) == 1
    assert await _draw(h.lock, key) == 1


DLOCK_BATTERY: tuple[Check, ...] = (
    check_reports_fence_advance,
    check_lock_lifecycle,
    check_advance_above_counter_moves_next_token,
    check_advance_at_or_below_counter_changes_nothing,
    check_advance_on_a_lost_counter,
    check_racing_advances_settle_on_the_largest,
    check_advance_needs_no_ownership,
    check_out_of_range_past_is_refused_before_sending,
    check_large_counters_round_trip_exactly,
    check_tenant_advance_moves_its_own_counter,
)
"""Every check, in the order a reader should meet them."""
