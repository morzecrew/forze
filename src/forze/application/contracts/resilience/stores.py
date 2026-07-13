"""Resilience state-store seams (circuit breaker, rate limit, latency digest).

These are the contracts a distributed resilience adapter implements: the executor
touches the breaker at two points — ``admit`` (before a call) and ``record``
(after) — the rate limiter at one (``try_acquire``), and the latency digest at
``observe`` / ``reset``; it otherwise does not care where the state lives. The
seams abstract that storage so state can be process-local (the in-memory default,
in the execution engine) or shared across replicas (e.g. a Redis adapter): a
shared breaker makes the fleet trip and recover together, and a shared rate limit
makes ``permits/per`` mean the *fleet's* rate instead of ``permits × replicas``.

The seams live in contracts so an adapter depends only on the contract, never on
the execution engine; the engine ships the default in-memory implementations.

Store-failure semantics: the executor treats any exception raised by ``admit`` /
``try_acquire`` as a store outage and **fails open by default** (admits the call),
so an unreachable distributed store can never make the resilience layer itself the
outage; a policy may opt into fail-closed via
``ResiliencePolicy.fail_open_on_store_error``. A ``record`` failure is always
swallowed — bookkeeping must never turn a successful call into a failure. Both are
surfaced as ``breaker_store_error`` / ``rate_limit_store_error`` metrics. The
in-memory defaults never raise, so single-process deployments see none of this.
"""

from collections.abc import Awaitable
from typing import Literal, Protocol, runtime_checkable

from forze.base.primitives import StrKey

from .value_objects import (
    AdaptiveBulkheadStrategy,
    CircuitBreakerStrategy,
    RateLimitStrategy,
)

# ----------------------- #

Transition = Literal["open", "closed", "half_open"] | None
"""Phase transition emitted by a breaker state update, or ``None`` when unchanged."""

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


@runtime_checkable
class BreakerStateResettable(Protocol):
    """Optional :class:`CircuitBreakerStore` capability: drop stored breaker state.

    ``clear_forced_open`` consults it so releasing a manual kill-switch starts the
    released scope on a fresh breaker epoch: nothing is recorded while the switch
    is armed (rejection happens before breaker admission), so the stored state is
    stale, and without the reset a breaker that tripped organically just before
    the switch was armed keeps rejecting until its ``break_duration`` elapses —
    after the operator has declared recovery. If the downstream is in fact still
    unhealthy, the fresh breaker re-trips on real failures. A store without this
    capability keeps its state; the clear itself still succeeds.
    """

    def reset_breaker(
        self,
        policy: StrKey,
        route: StrKey | None = None,
    ) -> Awaitable[None]:
        """Drop the ``(policy, route)`` breaker state; ``route=None`` drops every
        route under *policy*. Missing state is a no-op."""
        ...  # pragma: no cover


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
