"""Tests for :class:`~forze.application.coordinators.DistributedLockCoordinator`."""

import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from forze.application.coordinators import DistributedLockCoordinator
from forze.base.errors import CoreError


@pytest.mark.asyncio
async def test_scope_acquires_and_releases_on_success() -> None:
    cmd = MagicMock()
    cmd.acquire = AsyncMock(return_value=True)
    cmd.release = AsyncMock()
    cmd.reset = AsyncMock(return_value=True)

    coord = DistributedLockCoordinator(
        cmd=cmd,
        owner_provider=lambda: "owner-1",
    )

    async with coord.scope("resource-key") as held:
        assert held is True

    cmd.acquire.assert_awaited_once_with("resource-key", "owner-1")
    cmd.release.assert_awaited_once_with("resource-key", "owner-1")


@pytest.mark.asyncio
async def test_scope_raises_when_acquire_fails_without_wait() -> None:
    cmd = MagicMock()
    cmd.acquire = AsyncMock(return_value=False)

    coord = DistributedLockCoordinator(
        cmd=cmd,
        owner_provider=lambda: "o",
        wait_timeout=None,
    )

    with pytest.raises(CoreError, match="Failed to acquire distributed lock"):
        async with coord.scope("k"):
            pass


@pytest.mark.asyncio
async def test_scope_raises_when_acquire_times_out(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(asyncio, "sleep", AsyncMock())

    cmd = MagicMock()
    cmd.acquire = AsyncMock(return_value=False)

    coord = DistributedLockCoordinator(
        cmd=cmd,
        owner_provider=lambda: "o",
        wait_timeout=timedelta(milliseconds=50),
        retry_interval=timedelta(milliseconds=1),
        retry_jitter=timedelta(0),
    )

    with pytest.raises(CoreError, match="Failed to acquire distributed lock"):
        async with coord.scope("k"):
            pass

    assert cmd.acquire.await_count >= 1


@pytest.mark.asyncio
async def test_acquire_retry_sleep_respects_interval_plus_jitter_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Backoff must stay within ``retry_interval + retry_jitter`` (seconds), not seconds+ms."""
    sleeps: list[float] = []

    async def capture_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", capture_sleep)

    cmd = MagicMock()
    cmd.acquire = AsyncMock(side_effect=[False, False, True])

    coord = DistributedLockCoordinator(
        cmd=cmd,
        owner_provider=lambda: "o",
        wait_timeout=timedelta(seconds=10),
        retry_interval=timedelta(milliseconds=100),
        retry_jitter=timedelta(milliseconds=20),
    )

    async with coord.scope("k") as held:
        assert held is True

    assert len(sleeps) == 2
    max_sleep = max(sleeps)
    assert max_sleep <= 0.1 + 0.02 + 1e-9
    assert min(sleeps) >= 0.1
