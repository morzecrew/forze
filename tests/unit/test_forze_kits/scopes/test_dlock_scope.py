"""Tests for :class:`~forze_kits.scopes.DistributedLockScope`."""

import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
import structlog.testing

from forze.base.exceptions import CoreException, ExceptionKind
from forze_kits.scopes import DistributedLockScope


@pytest.mark.asyncio
async def test_scope_acquires_and_releases_on_success() -> None:
    cmd = MagicMock()
    cmd.acquire = AsyncMock(return_value=True)
    cmd.release = AsyncMock()
    cmd.reset = AsyncMock(return_value=True)

    coord = DistributedLockScope(
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

    coord = DistributedLockScope(
        cmd=cmd,
        owner_provider=lambda: "o",
        wait_timeout=None,
    )

    with pytest.raises(CoreException, match="Failed to acquire distributed lock"):
        async with coord.scope("k"):
            pass


@pytest.mark.asyncio
async def test_scope_raises_when_acquire_times_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(asyncio, "sleep", AsyncMock())

    cmd = MagicMock()
    cmd.acquire = AsyncMock(return_value=False)

    coord = DistributedLockScope(
        cmd=cmd,
        owner_provider=lambda: "o",
        wait_timeout=timedelta(milliseconds=50),
        retry_interval=timedelta(milliseconds=1),
        retry_jitter=timedelta(0),
    )

    with pytest.raises(CoreException, match="Failed to acquire distributed lock"):
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

    coord = DistributedLockScope(
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


@pytest.mark.asyncio
async def test_scope_extend_interval_calls_reset() -> None:
    cmd = MagicMock()
    cmd.acquire = AsyncMock(return_value=True)
    cmd.release = AsyncMock()
    cmd.reset = AsyncMock(return_value=True)

    coord = DistributedLockScope(
        cmd=cmd,
        owner_provider=lambda: "owner",
        extend_interval=timedelta(milliseconds=20),
    )

    async with coord.scope("resource"):
        await asyncio.sleep(0.05)

    assert cmd.reset.await_count >= 1


@pytest.mark.asyncio
async def test_scope_extend_failure_raises_on_exit() -> None:
    cmd = MagicMock()
    cmd.acquire = AsyncMock(return_value=True)
    cmd.release = AsyncMock()
    cmd.reset = AsyncMock(side_effect=RuntimeError("extend broke"))

    coord = DistributedLockScope(
        cmd=cmd,
        owner_provider=lambda: "owner",
        extend_interval=timedelta(milliseconds=5),
    )

    with pytest.raises(CoreException, match="Failed to extend distributed lock"):
        async with coord.scope("resource"):
            await asyncio.sleep(0.03)


@pytest.mark.asyncio
async def test_scope_raises_on_exit_when_lock_lost_mid_scope() -> None:
    """``reset()`` returning False (expired/stolen) must surface at scope exit."""
    cmd = MagicMock()
    cmd.acquire = AsyncMock(return_value=True)
    cmd.release = AsyncMock()
    cmd.reset = AsyncMock(return_value=False)

    coord = DistributedLockScope(
        cmd=cmd,
        owner_provider=lambda: "owner",
        extend_interval=timedelta(milliseconds=5),
    )

    body_completed = False

    with pytest.raises(CoreException, match="Distributed lock lost") as excinfo:
        async with coord.scope("resource"):
            await asyncio.sleep(0.03)
            body_completed = True

    # The body itself ran to completion; the loss is reported at exit.
    assert body_completed is True
    assert excinfo.value.kind is ExceptionKind.CONCURRENCY
    assert excinfo.value.details == {"key": "resource", "owner": "owner"}
    # Extending stopped after the loss was detected.
    assert cmd.reset.await_count == 1
    # The lock is still released best-effort.
    cmd.release.assert_awaited_once_with("resource", "owner")


@pytest.mark.asyncio
async def test_body_exception_not_masked_by_extend_failure() -> None:
    """When the body raises AND extend failed, the body's exception propagates."""
    cmd = MagicMock()
    cmd.acquire = AsyncMock(return_value=True)
    cmd.release = AsyncMock()
    cmd.reset = AsyncMock(side_effect=RuntimeError("extend broke"))

    coord = DistributedLockScope(
        cmd=cmd,
        owner_provider=lambda: "owner",
        extend_interval=timedelta(milliseconds=5),
    )

    with pytest.raises(ValueError, match="body failed") as excinfo:
        async with coord.scope("resource"):
            await asyncio.sleep(0.03)
            raise ValueError("body failed")

    # The lock problem is attached as a note, not raised over the body error.
    assert any(
        "distributed lock problem" in note
        for note in getattr(excinfo.value, "__notes__", [])
    )


@pytest.mark.asyncio
async def test_body_exception_not_masked_when_lock_lost() -> None:
    cmd = MagicMock()
    cmd.acquire = AsyncMock(return_value=True)
    cmd.release = AsyncMock()
    cmd.reset = AsyncMock(return_value=False)

    coord = DistributedLockScope(
        cmd=cmd,
        owner_provider=lambda: "owner",
        extend_interval=timedelta(milliseconds=5),
    )

    with pytest.raises(ValueError, match="body failed"):
        async with coord.scope("resource"):
            await asyncio.sleep(0.03)
            raise ValueError("body failed")


@pytest.mark.asyncio
async def test_release_failure_is_swallowed_and_logged() -> None:
    cmd = MagicMock()
    cmd.acquire = AsyncMock(return_value=True)
    cmd.release = AsyncMock(side_effect=RuntimeError("release broke"))
    cmd.reset = AsyncMock(return_value=True)

    coord = DistributedLockScope(
        cmd=cmd,
        owner_provider=lambda: "owner",
    )

    with structlog.testing.capture_logs() as logs:
        async with coord.scope("resource") as held:
            assert held is True

    warnings = [
        log
        for log in logs
        if log.get("log_level") == "warning"
        and "Failed to release distributed lock" in log["event"]
    ]
    assert len(warnings) == 1
    assert warnings[0]["key"] == "resource"
    assert warnings[0]["owner"] == "owner"
