"""Coverage tests for :class:`MockDistributedLockAdapter`."""

from __future__ import annotations

import time
from datetime import timedelta

from forze.application.contracts.dlock import DistributedLockSpec
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


def _seed_expired(state: MockState, key: str, owner: str = "stale") -> None:
    """Inject an already-expired lock entry into the store."""
    state.dlocks.setdefault("main", {})[key] = (owner, time.monotonic() - 1.0)


# ----------------------- #


async def test_is_locked_cleans_up_expired() -> None:
    state = MockState()
    lock = _lock(state)
    _seed_expired(state, "res")
    assert await lock.is_locked("res") is False
    assert "res" not in state.dlocks["main"]


async def test_is_locked_false_when_absent() -> None:
    lock = _lock()
    assert await lock.is_locked("nope") is False


async def test_get_owner_cleans_up_expired() -> None:
    state = MockState()
    lock = _lock(state)
    _seed_expired(state, "res")
    assert await lock.get_owner("res") is None
    assert "res" not in state.dlocks["main"]


async def test_get_owner_none_when_absent() -> None:
    lock = _lock()
    assert await lock.get_owner("nope") is None


async def test_get_ttl_cleans_up_expired() -> None:
    state = MockState()
    lock = _lock(state)
    _seed_expired(state, "res")
    assert await lock.get_ttl("res") is None
    assert "res" not in state.dlocks["main"]


async def test_get_ttl_none_when_absent() -> None:
    lock = _lock()
    assert await lock.get_ttl("nope") is None


async def test_acquire_returns_none_when_already_held() -> None:
    lock = _lock()
    first = await lock.acquire("res", "owner-a")
    assert first is not None
    assert await lock.acquire("res", "owner-b") is None


async def test_release_wrong_owner_returns_false() -> None:
    lock = _lock()
    await lock.acquire("res", "owner-a")
    assert await lock.release("res", "owner-b") is False
    # Still held by owner-a.
    assert await lock.is_locked("res") is True


async def test_release_expired_returns_false() -> None:
    state = MockState()
    lock = _lock(state)
    _seed_expired(state, "res", owner="owner-a")
    assert await lock.release("res", "owner-a") is False
    assert "res" not in state.dlocks["main"]


async def test_release_absent_returns_false() -> None:
    lock = _lock()
    assert await lock.release("nope", "owner-a") is False


async def test_reset_wrong_owner_returns_false() -> None:
    lock = _lock()
    await lock.acquire("res", "owner-a")
    assert await lock.reset("res", "owner-b") is False


async def test_reset_expired_returns_false() -> None:
    state = MockState()
    lock = _lock(state)
    _seed_expired(state, "res", owner="owner-a")
    assert await lock.reset("res", "owner-a") is False
    assert "res" not in state.dlocks["main"]


async def test_reset_absent_returns_false() -> None:
    lock = _lock()
    assert await lock.reset("nope", "owner-a") is False
