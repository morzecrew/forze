"""Circuit-breaker state store seam.

The executor touches the breaker at two points — ``admit`` (before a call) and
``record`` (after) — and otherwise does not care where breaker state lives. This
abstracts that storage so the breaker can be process-local (default) or shared across
replicas (e.g. a Redis adapter), making the fleet trip and recover together.
"""

import time
from typing import Awaitable, Callable, Protocol, final, runtime_checkable

import attrs

from forze.application.contracts.resilience import CircuitBreakerStrategy
from forze.base.primitives import StrKey

from .state import BreakerState, Transition

# ----------------------- #

BreakerKey = tuple[StrKey, StrKey | None]
"""Identifies a breaker instance by ``(policy_name, route)``."""


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

    clock: Callable[[], float] = attrs.field(default=time.monotonic)

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

        if ok:
            return state.on_success(self.clock())

        return state.on_failure(self.clock())
