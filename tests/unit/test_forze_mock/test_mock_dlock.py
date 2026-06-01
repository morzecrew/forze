"""Tests for :class:`~forze_mock.adapters.dlock.MockDistributedLockAdapter`."""

from datetime import timedelta

import pytest

from forze.application.contracts.dlock import DistributedLockSpec
from forze_mock.adapters.dlock import MockDistributedLockAdapter
from forze_mock.state import MockState

# ----------------------- #


@pytest.mark.asyncio
async def test_dlock_acquire_release_and_ttl() -> None:
    state = MockState()
    spec = DistributedLockSpec(name="locks", ttl=timedelta(seconds=30))
    lock = MockDistributedLockAdapter(spec=spec, state=state, namespace="main")

    assert await lock.acquire("resource", "owner-a") is True
    assert await lock.is_locked("resource") is True
    assert await lock.get_owner("resource") == "owner-a"
    ttl = await lock.get_ttl("resource")
    assert ttl is not None and ttl.total_seconds() > 0

    assert await lock.acquire("resource", "owner-b") is False
    assert await lock.release("resource", "owner-a") is True
    assert await lock.is_locked("resource") is False
