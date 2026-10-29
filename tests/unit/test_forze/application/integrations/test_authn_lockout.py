"""Unit tests for :class:`LoginLockoutGuard` and :class:`LockoutConfig` (frozen time)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from forze.application.contracts.authn import login_digest
from forze.application.contracts.counter import (
    CounterCapabilities,
    CounterPort,
    counter_capabilities,
    counter_ttl_unsupported,
)
from forze.application.integrations.authn import (
    LOCKED_LOGIN_CODE,
    LOCKED_LOGIN_MSG,
    LOCKOUT_TTL_SKEW,
    LockoutConfig,
    LoginLockoutGuard,
)
from forze.application.integrations.authn import lockout as lockout_module
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import CacheLane, FrozenTimeSource, bind_time_source
from forze_mock.adapters.counter import MockCounterAdapter, MockCounterAdminAdapter
from forze_mock.state import MockState

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


def _guard(
    threshold: int = 3, window: timedelta = WINDOW
) -> tuple[LoginLockoutGuard, _MemoryCounter]:
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

    async def test_locked_until_is_the_window_end(self) -> None:
        guard, _counter = _guard(threshold=2, window=WINDOW)
        window_s = int(WINDOW.total_seconds())
        end = datetime.fromtimestamp((int(T0.timestamp()) // window_s + 1) * window_s, tz=UTC)

        with bind_time_source(FrozenTimeSource(T0)):
            await guard.record_failure(DIGEST)
            assert await guard.locked_until(DIGEST) is None

            await guard.record_failure(DIGEST)
            assert await guard.locked_until(DIGEST) == end

        # Any instant inside the window reports the same end; the boundary itself unlocks.
        with bind_time_source(FrozenTimeSource(end - timedelta(seconds=1))):
            assert await guard.locked_until(DIGEST) == end

        with bind_time_source(FrozenTimeSource(end)):
            assert await guard.locked_until(DIGEST) is None

    @pytest.mark.parametrize(
        "window",
        [timedelta(milliseconds=500), timedelta(seconds=1.5)],
        ids=["sub-second", "fractional"],
    )
    async def test_a_window_keeps_its_sub_second_part(self, window: timedelta) -> None:
        # Truncated to whole seconds, 500 ms divided by zero and 1.5 s counted as 1 s.
        guard, _counter = _guard(threshold=1, window=window)

        with bind_time_source(FrozenTimeSource(T0)):  # T0 is on a boundary of both windows
            await guard.record_failure(DIGEST)
            assert await guard.locked_until(DIGEST) == T0 + window

        with bind_time_source(FrozenTimeSource(T0 + window - timedelta(microseconds=1))):
            assert await guard.is_locked(DIGEST) is True

        with bind_time_source(FrozenTimeSource(T0 + window)):
            assert await guard.is_locked(DIGEST) is False

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


# ....................... #


def _expiring_guard(threshold: int = 3) -> tuple[LoginLockoutGuard, MockState]:
    """A guard over the in-memory counter, which reports expiry and honours the frozen clock."""

    state = MockState()
    guard = LoginLockoutGuard(
        counter=MockCounterAdapter(state=state, namespace="lockout"),
        config=LockoutConfig(threshold=threshold, window=WINDOW),
    )
    return guard, state


class TestLockoutKeyGrowth:
    """Every login attempt reads the counter; only a failure (or a reset) may write one, and
    whatever is written expires once its window is over."""

    async def test_checking_a_login_creates_no_key(self) -> None:
        guard, state = _expiring_guard()

        with bind_time_source(FrozenTimeSource(T0)):
            for login in ("alice", "nobody-by-this-name", "x" * 200):
                assert await guard.locked_until(login_digest(login)) is None

        assert state.counters == {}, "a lockout check must not materialize a key"

    async def test_a_bucket_lives_one_window_plus_the_skew(self) -> None:
        guard, state = _expiring_guard()

        with bind_time_source(FrozenTimeSource(T0)):
            await guard.record_failure(DIGEST)
            await guard.record_failure(DIGEST)

        assert list(state.counter_expiry.values()) == [T0 + WINDOW + LOCKOUT_TTL_SKEW]

    async def test_a_reset_bucket_expires_too(self) -> None:
        guard, state = _expiring_guard()

        with bind_time_source(FrozenTimeSource(T0)):
            await guard.record_success(DIGEST)

        assert list(state.counter_expiry.values()) == [T0 + WINDOW + LOCKOUT_TTL_SKEW]

    async def test_old_buckets_are_gone_once_their_lifetime_ends(self) -> None:
        guard, state = _expiring_guard(threshold=3)
        admin = MockCounterAdminAdapter(state=state, namespace="lockout")

        with bind_time_source(FrozenTimeSource(T0)):
            for _ in range(3):
                await guard.record_failure(DIGEST)

            assert await guard.is_locked(DIGEST) is True

        with bind_time_source(FrozenTimeSource(T0 + WINDOW + LOCKOUT_TTL_SKEW)):
            assert await guard.is_locked(DIGEST) is False
            assert await admin.list_counters() == [], "the old bucket must not linger"

    async def test_a_bucket_outlives_its_window(self) -> None:
        """Created at the first instant of its window, still locking at the last one.

        The lifetime starts at creation, so a bucket created at its window's start is the one
        closest to lapsing early; ``window + LOCKOUT_TTL_SKEW`` keeps it past the window's
        end by the skew allowed between nodes.
        """

        guard, state = _expiring_guard(threshold=1)
        window_s = WINDOW.total_seconds()
        start = datetime.fromtimestamp((T0.timestamp() // window_s) * window_s, tz=UTC)

        with bind_time_source(FrozenTimeSource(start)):
            await guard.record_failure(DIGEST)

        (expires_at,) = state.counter_expiry.values()

        assert expires_at - (start + WINDOW) == LOCKOUT_TTL_SKEW

        with bind_time_source(FrozenTimeSource(start + WINDOW - timedelta(microseconds=1))):
            assert await guard.is_locked(DIGEST) is True

    async def test_a_counter_that_cannot_expire_is_written_without_a_ttl(self) -> None:
        # ``_MemoryCounter`` reports no capabilities and its verbs take no ``ttl`` keyword, so
        # passing one — even ``None`` — would raise ``TypeError`` here.
        guard, counter = _guard(threshold=1)

        with bind_time_source(FrozenTimeSource(T0)):
            await guard.record_failure(DIGEST)
            assert await guard.is_locked(DIGEST) is True

            await guard.record_success(DIGEST)
            assert await guard.is_locked(DIGEST) is False

        assert counter_capabilities(counter).expiry is False


# ....................... #


class _RefusingCounter(_MemoryCounter):
    """Reports expiry but refuses every ttl — a Postgres table without ``expires_at``."""

    def __init__(self, error: CoreException) -> None:
        super().__init__()
        self.error = error

    def capabilities(self) -> CounterCapabilities:
        return CounterCapabilities(expiry=True)

    async def incr(self, by: int = 1, *, suffix: str | None = None, **kw: object) -> int:
        if kw.get("ttl") is not None:
            raise self.error

        return await super().incr(by, suffix=suffix)

    async def reset(self, value: int = 1, *, suffix: str | None = None, **kw: object) -> int:
        if kw.get("ttl") is not None:
            raise self.error

        return await super().reset(value, suffix=suffix)


class TestLockoutOnACounterThatRefusesTheTtl:
    """Login availability outranks key growth: the lockout degrades, it does not fail."""

    async def test_it_still_counts_and_locks_and_warns_once(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        warnings: list[str] = []
        monkeypatch.setattr(lockout_module, "_TTL_REFUSALS_REPORTED", CacheLane(max_entries=8))
        monkeypatch.setattr(
            lockout_module,
            "_logger",
            SimpleNamespace(warning=lambda event, **extras: warnings.append(extras["reason"])),
        )
        counter = _RefusingCounter(counter_ttl_unsupported("the table has no expires_at"))
        guard = LoginLockoutGuard(counter=counter, config=LockoutConfig(threshold=2))

        with bind_time_source(FrozenTimeSource(T0)):
            await guard.record_failure(DIGEST)
            await guard.record_failure(DIGEST)
            assert await guard.is_locked(DIGEST) is True

            await guard.record_success(DIGEST)
            assert await guard.is_locked(DIGEST) is False

        assert len(warnings) == 1, "warned once per process, not once per write"
        assert "expires_at" in warnings[0]

    async def test_each_refusing_table_is_reported(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Once per process would hide a second tenant's or route's table behind the first."""

        warnings: list[str] = []
        monkeypatch.setattr(lockout_module, "_TTL_REFUSALS_REPORTED", CacheLane(max_entries=8))
        monkeypatch.setattr(
            lockout_module,
            "_logger",
            SimpleNamespace(warning=lambda event, **extras: warnings.append(extras["reason"])),
        )

        for relation in ("public.lockout_a", "public.lockout_a", "public.lockout_b"):
            refusal = counter_ttl_unsupported("no expires_at", details={"relation": relation})
            guard = LoginLockoutGuard(counter=_RefusingCounter(refusal))

            with bind_time_source(FrozenTimeSource(T0)):
                await guard.record_failure(DIGEST)

        assert len(warnings) == 2, "one warning per refusing table"

    async def test_any_other_refusal_still_fails_the_write(self) -> None:
        counter = _RefusingCounter(exc.configuration("the counter route is not wired"))
        guard = LoginLockoutGuard(counter=counter)

        with bind_time_source(FrozenTimeSource(T0)), pytest.raises(CoreException):
            await guard.record_failure(DIGEST)
