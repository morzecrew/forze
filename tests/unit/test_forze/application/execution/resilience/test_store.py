"""Tests for the circuit-breaker store seam (in-memory, drives the state machine)."""

from __future__ import annotations

from datetime import timedelta

from forze.application.contracts.resilience import CircuitBreakerStrategy
from forze.application.execution.resilience import InMemoryCircuitBreakerStore

# ----------------------- #


def _strat() -> CircuitBreakerStrategy:
    return CircuitBreakerStrategy(
        failure_ratio=0.5,
        sampling_window=timedelta(seconds=10),
        min_throughput=2,
        break_duration=timedelta(seconds=5),
        half_open_max_calls=1,
    )


class _Clock:
    def __init__(self) -> None:
        self.t = 100.0

    def __call__(self) -> float:
        return self.t


_KEY = ("p", "r")


class TestInMemoryCircuitBreakerStore:
    async def test_open_half_open_close_lifecycle(self) -> None:
        clock = _Clock()
        store = InMemoryCircuitBreakerStore(clock=clock)
        strat = _strat()

        allowed, tr = await store.admit(_KEY, strat)
        assert allowed is True
        assert tr is None

        # 1st failure: total below min_throughput -> no trip
        assert await store.record(_KEY, strat, False) is None
        # 2nd failure: ratio 1.0 >= 0.5 over min_throughput -> opens
        assert await store.record(_KEY, strat, False) == "open"

        allowed, _ = await store.admit(_KEY, strat)
        assert allowed is False  # open, within break window

        clock.t += 6  # past break_duration -> half-open on next admit
        allowed, tr = await store.admit(_KEY, strat)
        assert allowed is True
        assert tr == "half_open"

        assert await store.record(_KEY, strat, True) == "closed"  # probe ok -> close

        allowed, tr = await store.admit(_KEY, strat)
        assert allowed is True
        assert tr is None

    async def test_half_open_failure_reopens(self) -> None:
        clock = _Clock()
        store = InMemoryCircuitBreakerStore(clock=clock)
        strat = _strat()

        await store.record(_KEY, strat, False)
        await store.record(_KEY, strat, False)  # open
        clock.t += 6
        _, tr = await store.admit(_KEY, strat)
        assert tr == "half_open"

        assert await store.record(_KEY, strat, False) == "open"  # probe fails -> reopen

    async def test_keys_are_independent(self) -> None:
        store = InMemoryCircuitBreakerStore(clock=_Clock())
        strat = _strat()

        await store.record(("p", "a"), strat, False)
        await store.record(("p", "a"), strat, False)  # route a opens

        allowed, _ = await store.admit(("p", "b"), strat)
        assert allowed is True  # route b unaffected
