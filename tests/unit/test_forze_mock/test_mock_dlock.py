"""Tests for :class:`~forze_mock.adapters.dlock.MockDistributedLockAdapter`."""

from datetime import timedelta

import pytest

from forze.application.contracts.dlock import AcquiredLock, DistributedLockSpec
from forze_mock.adapters.dlock import MockDistributedLockAdapter
from forze_mock.state import MockState

# ----------------------- #


def _lock(
    state: MockState | None = None,
    ttl: timedelta = timedelta(seconds=30),
) -> MockDistributedLockAdapter:
    return MockDistributedLockAdapter(
        spec=DistributedLockSpec(name="locks", ttl=ttl),
        state=state or MockState(),
        namespace="main",
    )


# ----------------------- #


@pytest.mark.asyncio
async def test_dlock_acquire_release_and_ttl() -> None:
    lock = _lock()

    acquired = await lock.acquire("resource", "owner-a")
    assert isinstance(acquired, AcquiredLock)
    assert acquired.key == "resource"
    assert acquired.owner == "owner-a"
    assert acquired.token == 1

    assert await lock.is_locked("resource") is True
    assert await lock.get_owner("resource") == "owner-a"
    ttl = await lock.get_ttl("resource")
    assert ttl is not None and ttl.total_seconds() > 0

    # Contention: the loser gets None.
    assert await lock.acquire("resource", "owner-b") is None
    assert await lock.release("resource", "owner-a") is True
    assert await lock.is_locked("resource") is False


@pytest.mark.asyncio
async def test_dlock_tokens_increase_across_generations() -> None:
    lock = _lock()

    tokens: list[int | None] = []

    for owner in ("owner-a", "owner-b", "owner-c"):
        acquired = await lock.acquire("resource", owner)
        assert acquired is not None
        tokens.append(acquired.token)
        assert await lock.release("resource", owner) is True

    assert tokens == [1, 2, 3]


@pytest.mark.asyncio
async def test_dlock_token_survives_expiry_driven_reacquisition() -> None:
    # Tiny TTL: the first holder's lease expires immediately.
    lock = _lock(ttl=timedelta(microseconds=1))

    first = await lock.acquire("resource", "stale-holder")
    assert first is not None and first.token == 1

    import asyncio

    await asyncio.sleep(0.001)

    # The lock expired without a release; the new generation's token is higher,
    # so the stale holder's token is provably stale.
    second = await lock.acquire("resource", "new-holder")
    assert second is not None
    assert second.token is not None and first.token is not None
    assert second.token > first.token


@pytest.mark.asyncio
async def test_dlock_reset_does_not_bump_token() -> None:
    state = MockState()
    lock = _lock(state=state)

    acquired = await lock.acquire("resource", "owner-a")
    assert acquired is not None and acquired.token == 1

    # Heartbeat extends are the same lock generation: the counter is untouched.
    assert await lock.reset("resource", "owner-a") is True
    assert await lock.reset("resource", "owner-a") is True
    assert state.dlock_fences["main"]["resource"] == 1

    assert await lock.release("resource", "owner-a") is True

    # Release does not reset the counter either: the next generation goes up.
    again = await lock.acquire("resource", "owner-b")
    assert again is not None and again.token == 2


@pytest.mark.asyncio
async def test_dlock_counters_are_per_key() -> None:
    lock = _lock()

    a = await lock.acquire("res-a", "owner")
    b = await lock.acquire("res-b", "owner")

    assert a is not None and a.token == 1
    assert b is not None and b.token == 1
