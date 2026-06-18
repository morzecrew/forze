"""Resilience state store seams (circuit breaker, rate limit).

The executor touches the breaker at two points — ``admit`` (before a call) and
``record`` (after) — and the rate limiter at one (``try_acquire``); it otherwise
does not care where the state lives. These seams abstract that storage so the
state can be process-local (default) or shared across replicas (e.g. a Redis
adapter): a shared breaker makes the fleet trip and recover together, and a
shared rate limit makes ``permits/per`` mean the *fleet's* rate instead of
silently becoming ``permits × replicas``.
"""

from typing import Awaitable, Callable, Protocol, final, runtime_checkable

import attrs

from forze.application.contracts.resilience import (
    AdaptiveBulkheadStrategy,
    CircuitBreakerStrategy,
    RateLimitStrategy,
)
from forze.base.primitives import StrKey, WindowedP2Quantile, monotonic

from .state import BreakerState, RateLimitState, Transition

# ----------------------- #

BreakerKey = tuple[StrKey, StrKey | None]
"""Identifies a breaker instance by ``(policy_name, route)``."""

RateLimitKey = tuple[StrKey, StrKey | None]
"""Identifies a rate-limit bucket by ``(policy_name, route)``."""

LatencyDigestKey = tuple[StrKey, StrKey | None]
"""Identifies an adaptive-bulkhead latency digest by ``(policy_name, route)``."""


# ....................... #


@runtime_checkable
class CircuitBreakerStore(Protocol):
    """Stores circuit-breaker state and applies its transitions atomically.

    Implementations own their time source — in-memory uses an injected clock, a
    distributed store uses server time — so ``now`` is never passed across the seam
    (replica clocks diverge).
    """

    def admit(
        self,
        key: BreakerKey,
        strat: CircuitBreakerStrategy,
    ) -> Awaitable[tuple[bool, Transition]]:
        """Decide whether a call may proceed; return ``(allowed, transition)``."""
        ...  # pragma: no cover

    def record(
        self,
        key: BreakerKey,
        strat: CircuitBreakerStrategy,
        ok: bool,
    ) -> Awaitable[Transition]:
        """Record a call outcome; return any phase transition it caused."""
        ...  # pragma: no cover


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class InMemoryCircuitBreakerStore(CircuitBreakerStore):
    """Process-local breaker state keyed by ``(policy, route)`` (the default store)."""

    clock: Callable[[], float] = attrs.field(default=monotonic)

    _states: dict[BreakerKey, BreakerState] = attrs.field(factory=dict, init=False)

    # ....................... #

    def _state_for(
        self,
        key: BreakerKey,
        strat: CircuitBreakerStrategy,
    ) -> BreakerState:
        state = self._states.get(key)

        if state is None:
            state = BreakerState(
                failure_ratio=strat.failure_ratio,
                window=strat.sampling_window.total_seconds(),
                min_throughput=strat.min_throughput,
                break_duration=strat.break_duration.total_seconds(),
                half_open_max_calls=strat.half_open_max_calls,
                window_start=self.clock(),
            )
            self._states[key] = state

        return state

    # ....................... #

    async def admit(
        self,
        key: BreakerKey,
        strat: CircuitBreakerStrategy,
    ) -> tuple[bool, Transition]:
        return self._state_for(key, strat).try_admit(self.clock())

    # ....................... #

    async def record(
        self,
        key: BreakerKey,
        strat: CircuitBreakerStrategy,
        ok: bool,
    ) -> Transition:
        state = self._state_for(key, strat)

        return state.on_success(self.clock()) if ok else state.on_failure(self.clock())


# ....................... #


@runtime_checkable
class RateLimitStore(Protocol):
    """Stores token-bucket state and applies acquisition atomically.

    With the in-memory default each replica enforces ``permits/per``
    independently, so the fleet-effective rate is ``permits × replicas``; a
    shared store (e.g. Redis) makes the declared rate the *fleet's* rate.
    Implementations own their time source — in-memory uses an injected clock,
    a distributed store uses server time — so ``now`` is never passed across
    the seam (replica clocks diverge).
    """

    def try_acquire(
        self,
        key: RateLimitKey,
        strat: RateLimitStrategy,
    ) -> Awaitable[bool]:
        """Consume one token if available; return whether the call may proceed."""
        ...  # pragma: no cover


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class InMemoryRateLimitStore(RateLimitStore):
    """Process-local token buckets keyed by ``(policy, route)`` (the default store)."""

    clock: Callable[[], float] = attrs.field(default=monotonic)

    # ....................... #

    _states: dict[RateLimitKey, RateLimitState] = attrs.field(factory=dict, init=False)

    # ....................... #

    def _state_for(
        self,
        key: RateLimitKey,
        strat: RateLimitStrategy,
    ) -> RateLimitState:
        state = self._states.get(key)

        if state is None:
            state = RateLimitState(
                rate=strat.permits / strat.per.total_seconds(),
                capacity=float(strat.capacity),
                updated_at=self.clock(),
            )
            self._states[key] = state

        return state

    # ....................... #

    async def try_acquire(
        self,
        key: RateLimitKey,
        strat: RateLimitStrategy,
    ) -> bool:
        return self._state_for(key, strat).try_acquire(self.clock())


# ....................... #


@runtime_checkable
class LatencyDigestStore(Protocol):
    """Stores the adaptive bulkhead's latency-quantile congestion signal.

    The executor records each completed call's latency (:meth:`observe`) and
    reads back the windowed quantile that drives the AIMD breach decision; after
    a backoff it opens a fresh epoch (:meth:`reset`). With the in-memory default
    each replica reacts to its *own* p95; a shared store (e.g. a Redis-backed
    mergeable digest) makes the signal reflect the *fleet's* latency. Only
    consulted in quantile mode (``AdaptiveBulkheadStrategy.latency_quantile``
    set); the per-sample default makes no call.
    """

    def observe(
        self,
        key: LatencyDigestKey,
        latency: float,
        strat: AdaptiveBulkheadStrategy,
    ) -> Awaitable[float | None]:
        """Record one latency sample; return the current quantile, or ``None`` warming."""
        ...  # pragma: no cover

    def reset(
        self,
        key: LatencyDigestKey,
        strat: AdaptiveBulkheadStrategy,
    ) -> Awaitable[None]:
        """Open a fresh measurement epoch (the old distribution justified a backoff)."""
        ...  # pragma: no cover


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class InMemoryLatencyDigestStore(LatencyDigestStore):
    """Process-local windowed-P² latency digest keyed by ``(policy, route)``.

    The default store — behaviorally identical to the estimator the bulkhead
    owned before the digest seam existed. ``p`` is taken from the strategy's
    ``latency_quantile`` (which is set whenever this store is consulted).
    """

    _estimators: dict[LatencyDigestKey, WindowedP2Quantile] = attrs.field(
        factory=dict,
        init=False,
    )

    # ....................... #

    def _estimator_for(
        self,
        key: LatencyDigestKey,
        strat: AdaptiveBulkheadStrategy,
    ) -> WindowedP2Quantile:
        estimator = self._estimators.get(key)

        if estimator is None:
            estimator = WindowedP2Quantile(p=strat.latency_quantile or 0.95)
            self._estimators[key] = estimator

        return estimator

    # ....................... #

    async def observe(
        self,
        key: LatencyDigestKey,
        latency: float,
        strat: AdaptiveBulkheadStrategy,
    ) -> float | None:
        estimator = self._estimator_for(key, strat)
        estimator.observe(latency)

        return estimator.value()

    # ....................... #

    async def reset(
        self,
        key: LatencyDigestKey,
        strat: AdaptiveBulkheadStrategy,
    ) -> None:
        # Fresh epoch: only the new concurrency's latencies should decide the
        # next move, so a stale-high quantile cannot ratchet the limit down for
        # up to two windows after the downstream recovers.
        self._estimators[key] = WindowedP2Quantile(p=strat.latency_quantile or 0.95)
