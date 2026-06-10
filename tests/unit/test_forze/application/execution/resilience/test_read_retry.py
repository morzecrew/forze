"""Tests for the shared read-retry helper."""

import asyncio
from typing import Any

import pytest

from forze.application.execution.resilience import retry_read

# ----------------------- #


class _Flaky:
    """Callable failing *failures* times before returning *value*."""

    def __init__(
        self,
        failures: int,
        *,
        value: str = "ok",
        error: type[BaseException] = TimeoutError,
    ) -> None:
        self.calls = 0
        self._failures = failures
        self._value = value
        self._error = error

    async def __call__(self) -> str:
        self.calls += 1

        if self.calls <= self._failures:
            raise self._error(f"transient {self.calls}")

        return self._value


# ....................... #


async def test_success_first_try_no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _no_sleep(_delay: float) -> None:  # pragma: no cover
        raise AssertionError("should not sleep on first-try success")

    monkeypatch.setattr(asyncio, "sleep", _no_sleep)
    fn = _Flaky(0)

    assert await retry_read(fn, attempts=3, base_delay=0.1) == "ok"
    assert fn.calls == 1


async def test_success_after_n_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []

    async def _record_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(asyncio, "sleep", _record_sleep)
    fn = _Flaky(2)

    assert await retry_read(fn, attempts=3, base_delay=0.1) == "ok"
    assert fn.calls == 3
    assert sleeps == [pytest.approx(0.1), pytest.approx(0.2)]


async def test_exponential_backoff_sequence(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []

    async def _record_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(asyncio, "sleep", _record_sleep)
    fn = _Flaky(3)

    await retry_read(fn, attempts=3, base_delay=0.5)
    assert sleeps == [pytest.approx(0.5), pytest.approx(1.0), pytest.approx(2.0)]


async def test_exhaustion_raises_last_error(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _record_sleep(_delay: float) -> None:
        pass

    monkeypatch.setattr(asyncio, "sleep", _record_sleep)
    fn = _Flaky(10, error=ConnectionError)

    with pytest.raises(ConnectionError, match="transient 3"):
        await retry_read(fn, attempts=2, base_delay=0.0)

    assert fn.calls == 3


async def test_zero_attempts_no_retry() -> None:
    fn = _Flaky(1)

    with pytest.raises(TimeoutError):
        await retry_read(fn, attempts=0, base_delay=0.0)

    assert fn.calls == 1


async def test_negative_attempts_treated_as_zero() -> None:
    fn = _Flaky(1)

    with pytest.raises(TimeoutError):
        await retry_read(fn, attempts=-3, base_delay=-1.0)

    assert fn.calls == 1


async def test_non_transient_error_propagates_immediately() -> None:
    fn = _Flaky(1, error=ValueError)

    with pytest.raises(ValueError):
        await retry_read(fn, attempts=5, base_delay=0.0)

    assert fn.calls == 1


async def test_custom_retry_on(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _record_sleep(_delay: float) -> None:
        pass

    monkeypatch.setattr(asyncio, "sleep", _record_sleep)
    fn = _Flaky(1, error=KeyError)

    assert await retry_read(fn, attempts=1, base_delay=0.0, retry_on=(KeyError,)) == "ok"
    assert fn.calls == 2


async def test_on_retry_hook_receives_attempt_numbers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _record_sleep(_delay: float) -> None:
        pass

    monkeypatch.setattr(asyncio, "sleep", _record_sleep)
    hook_calls: list[Any] = []
    fn = _Flaky(2)

    await retry_read(
        fn,
        attempts=3,
        base_delay=0.0,
        on_retry=hook_calls.append,
    )

    # hook fires before each sleep, with the 1-based retry number
    assert hook_calls == [1, 2]
