"""In-memory resilience state stores (circuit breaker, rate limit, latency digest).

The store *seams* (``CircuitBreakerStore``, ``RateLimitStore``,
``LatencyDigestStore`` and their key/transition types) are contracts — see
``forze.application.contracts.resilience.stores``. This module ships the default
process-local implementations the executor falls back to when no shared
(e.g. Redis-backed) adapter is wired.
"""

from typing import Callable, final

import attrs

from forze.application.contracts.resilience import (
    AdaptiveBulkheadStrategy,
    BreakerKey,
    CircuitBreakerStore,
    CircuitBreakerStrategy,
    LatencyDigestKey,
    LatencyDigestStore,
    RateLimitKey,
    RateLimitStore,
    RateLimitStrategy,
    Transition,
)
from forze.base.primitives import BoundedLruMap, StrKey, WindowedP2Quantile, monotonic

from .state import BreakerState, RateLimitState

# ----------------------- #

DEFAULT_MAX_STATE_ENTRIES = 4096
"""Default per-store cap on ``(policy, route)`` entries — bounds memory when ``route`` is
high-cardinality (per-tenant, per-object). Eviction is plain LRU, so under high cardinality
it can drop a *hot* entry, not only an idle one: an evicted OPEN breaker resets to closed
(a burst then passes until it re-trips) and a saturated bucket refills. Keep the distinct
``route`` count per policy comfortably under the cap (or raise it) so live control state is
not churned; an evicted entry is recreated fresh on next access."""


@final
@attrs.define(slots=True, kw_only=True)
class InMemoryCircuitBreakerStore(CircuitBreakerStore):
    """Process-local breaker state keyed by ``(policy, route)`` (the default store)."""

    clock: Callable[[], float] = attrs.field(default=monotonic)

    max_entries: int = DEFAULT_MAX_STATE_ENTRIES
    """LRU cap on ``(policy, route)`` breaker states (see :data:`DEFAULT_MAX_STATE_ENTRIES`)."""

    _states: BoundedLruMap[BreakerKey, BreakerState] = attrs.field(
        default=attrs.Factory(
            lambda self: BoundedLruMap[BreakerKey, BreakerState](self.max_entries),
            takes_self=True,
        ),
        init=False,
    )

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

    async def reset_breaker(
        self,
        policy: StrKey,
        route: StrKey | None = None,
    ) -> None:
        """Drop breaker state so the scope starts a fresh epoch (``route=None`` = whole policy).

        Consulted by ``clear_forced_open``: releasing a manual kill-switch must not leave
        the released scope rejecting on state that predates the switch (nothing was
        recorded while it was armed). A dropped entry is recreated closed on next access.
        """

        if route is None:
            for key in [k for k in self._states if k[0] == policy]:
                self._states.pop(key, None)

        else:
            self._states.pop((policy, route), None)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class InMemoryRateLimitStore(RateLimitStore):
    """Process-local token buckets keyed by ``(policy, route)`` (the default store)."""

    clock: Callable[[], float] = attrs.field(default=monotonic)

    max_entries: int = DEFAULT_MAX_STATE_ENTRIES
    """LRU cap on ``(policy, route)`` token buckets (see :data:`DEFAULT_MAX_STATE_ENTRIES`)."""

    # ....................... #

    _states: BoundedLruMap[RateLimitKey, RateLimitState] = attrs.field(
        default=attrs.Factory(
            lambda self: BoundedLruMap[RateLimitKey, RateLimitState](self.max_entries),
            takes_self=True,
        ),
        init=False,
    )

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


@final
@attrs.define(slots=True, kw_only=True)
class InMemoryLatencyDigestStore(LatencyDigestStore):
    """Process-local windowed-P² latency digest keyed by ``(policy, route)``.

    The default store — behaviorally identical to the estimator the bulkhead
    owned before the digest seam existed. ``p`` is taken from the strategy's
    ``latency_quantile`` (which is set whenever this store is consulted).
    """

    max_entries: int = DEFAULT_MAX_STATE_ENTRIES
    """LRU cap on ``(policy, route)`` latency digests (see :data:`DEFAULT_MAX_STATE_ENTRIES`)."""

    _estimators: BoundedLruMap[LatencyDigestKey, WindowedP2Quantile] = attrs.field(
        default=attrs.Factory(
            lambda self: BoundedLruMap[LatencyDigestKey, WindowedP2Quantile](
                self.max_entries
            ),
            takes_self=True,
        ),
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
