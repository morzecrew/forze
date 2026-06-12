"""Unit tests for :class:`LoginLockoutGuard` and :class:`LockoutConfig` (frozen time)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from forze.application.contracts.authn import login_digest
from forze.application.contracts.counter import CounterPort
from forze.application.integrations.authn import (
    LOCKED_LOGIN_CODE,
    LOCKED_LOGIN_MSG,
    LockoutConfig,
    LoginLockoutGuard,
)
from forze.base.exceptions import CoreException
from forze.base.primitives import FrozenTimeSource, bind_time_source

pytestmark = pytest.mark.unit

# ----------------------- #

T0 = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)
WINDOW = timedelta(minutes=15)
DIGEST = login_digest("alice")


class _MemoryCounter(CounterPort):
    """Minimal in-memory ``CounterPort`` (suffix-partitioned, like the adapters)."""

    def __init__(self) -> None:
        self.values: dict[str | None, int] = {}

    async def incr(self, by: int = 1, *, suffix: str | None = None) -> int:
        self.values[suffix] = self.values.get(suffix, 0) + by
        return self.values[suffix]

    async def incr_batch(self, size: int = 2, *, suffix: str | None = None) -> list[int]:
        prev = self.values.get(suffix, 0)
        self.values[suffix] = prev + size
        return list(range(prev + 1, prev + size + 1))

    async def decr(self, by: int = 1, *, suffix: str | None = None) -> int:
        self.values[suffix] = self.values.get(suffix, 0) - by
        return self.values[suffix]

    async def reset(self, value: int = 1, *, suffix: str | None = None) -> int:
        self.values[suffix] = value
        return value


def _guard(threshold: int = 3, window: timedelta = WINDOW) -> tuple[LoginLockoutGuard, _MemoryCounter]:
    counter = _MemoryCounter()
    guard = LoginLockoutGuard(
        counter=counter,
        config=LockoutConfig(threshold=threshold, window=window),
    )
    return guard, counter


# ----------------------- #


class TestLockoutConfig:
    def test_defaults(self) -> None:
        config = LockoutConfig()

        assert config.threshold == 5
        assert config.window == timedelta(minutes=15)

    def test_rejects_non_positive_threshold(self) -> None:
        with pytest.raises(CoreException, match="threshold"):
            LockoutConfig(threshold=0)

    def test_rejects_non_positive_window(self) -> None:
        with pytest.raises(CoreException, match="window"):
            LockoutConfig(window=timedelta(0))

    def test_error_constants_shape(self) -> None:
        assert LOCKED_LOGIN_CODE == "login_locked"
        assert "attempts" in LOCKED_LOGIN_MSG


class TestLoginLockoutGuard:
    async def test_below_threshold_failures_do_not_lock(self) -> None:
        guard, _counter = _guard(threshold=3)

        with bind_time_source(FrozenTimeSource(T0)):
            for _ in range(2):
                assert await guard.is_locked(DIGEST) is False
                await guard.record_failure(DIGEST)

            # N-1 failures recorded: the Nth attempt still passes the gate.
            assert await guard.is_locked(DIGEST) is False

    async def test_threshold_failures_lock_the_next_attempt(self) -> None:
        guard, _counter = _guard(threshold=3)

        with bind_time_source(FrozenTimeSource(T0)):
            for _ in range(3):
                await guard.record_failure(DIGEST)

            assert await guard.is_locked(DIGEST) is True

    async def test_success_resets_the_current_bucket(self) -> None:
        guard, _counter = _guard(threshold=3)

        with bind_time_source(FrozenTimeSource(T0)):
            for _ in range(3):
                await guard.record_failure(DIGEST)
            assert await guard.is_locked(DIGEST) is True

            await guard.record_success(DIGEST)

            assert await guard.is_locked(DIGEST) is False

    async def test_window_rollover_unlocks(self) -> None:
        guard, _counter = _guard(threshold=3, window=WINDOW)

        with bind_time_source(FrozenTimeSource(T0)):
            for _ in range(3):
                await guard.record_failure(DIGEST)
            assert await guard.is_locked(DIGEST) is True

        with bind_time_source(FrozenTimeSource(T0 + WINDOW)):
            assert await guard.is_locked(DIGEST) is False

    async def test_attempts_within_the_window_share_the_bucket(self) -> None:
        guard, _counter = _guard(threshold=2, window=WINDOW)
        # Pick a bucket-aligned origin so T0 .. T0+window-1s share one bucket.
        window_s = int(WINDOW.total_seconds())
        aligned = datetime.fromtimestamp(
            (int(T0.timestamp()) // window_s) * window_s,
            tz=UTC,
        )

        with bind_time_source(FrozenTimeSource(aligned)):
            await guard.record_failure(DIGEST)

        with bind_time_source(FrozenTimeSource(aligned + WINDOW - timedelta(seconds=1))):
            await guard.record_failure(DIGEST)
            assert await guard.is_locked(DIGEST) is True

    async def test_digests_are_isolated(self) -> None:
        guard, _counter = _guard(threshold=1)
        other = login_digest("bob")

        with bind_time_source(FrozenTimeSource(T0)):
            await guard.record_failure(DIGEST)

            assert await guard.is_locked(DIGEST) is True
            assert await guard.is_locked(other) is False

    async def test_is_locked_reads_without_counting(self) -> None:
        guard, counter = _guard(threshold=3)

        with bind_time_source(FrozenTimeSource(T0)):
            for _ in range(5):
                assert await guard.is_locked(DIGEST) is False

            # The incr(0) read never inflated the failure count.
            assert all(value == 0 for value in counter.values.values())

    async def test_counter_suffix_carries_digest_and_bucket_never_the_login(self) -> None:
        guard, counter = _guard(threshold=3, window=WINDOW)

        with bind_time_source(FrozenTimeSource(T0)):
            await guard.record_failure(DIGEST)

            bucket = int(T0.timestamp()) // int(WINDOW.total_seconds())
            (suffix,) = counter.values.keys()

            assert suffix == f"authn_lockout:{DIGEST}:{bucket}"
            assert "alice" not in suffix
