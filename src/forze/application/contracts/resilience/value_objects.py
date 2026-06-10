"""Resilience policy value objects: strategies and the composed policy."""

from datetime import timedelta
from enum import StrEnum
from typing import Literal, final

import attrs

from forze.base.exceptions import ExceptionKind, exc, exception_egress_policy
from forze.base.primitives import StrKey

# ----------------------- #

JitterMode = Literal["none", "full", "equal", "decorrelated"]
"""Jitter applied to retry backoff delays (``decorrelated`` is the modern default)."""


# ....................... #


class HedgeSafety(StrEnum):
    """Why hedging is safe on an operation (concurrent duplicates require it)."""

    READ_ONLY = "read_only"
    """The operation has no side effects."""

    IDEMPOTENT = "idempotent"
    """Duplicate effects collapse (idempotency key, OCC, or natural idempotency)."""


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class BackoffStrategy:
    """Exponential backoff schedule with optional jitter."""

    base: timedelta
    """Delay before the first retry (and exponential base)."""

    max: timedelta
    """Upper bound for any single backoff delay."""

    multiplier: float = 2.0
    """Growth factor applied per attempt."""

    jitter: JitterMode = "decorrelated"
    """Jitter mode applied to each computed delay."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.base.total_seconds() <= 0:
            raise exc.configuration("Backoff base must be positive")

        if self.max < self.base:
            raise exc.configuration("Backoff max must be >= base")

        if self.multiplier < 1:
            raise exc.configuration("Backoff multiplier must be >= 1")


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RetryBudget:
    """Token-bucket cap limiting retries to a fraction of total calls."""

    ratio: float
    """Retry tokens earned per call (e.g. ``0.1`` caps retries to ~10% of calls)."""

    min_throughput: int = 0
    """Calls allowed to retry freely before the cap engages (warmup)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not 0 < self.ratio <= 1:
            raise exc.configuration("Retry budget ratio must be in (0, 1]")

        if self.min_throughput < 0:
            raise exc.configuration("Retry budget min_throughput must be >= 0")


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class BulkheadStrategy:
    """Concurrency limiter with a bounded waiting queue."""

    max_concurrency: int
    """Maximum number of concurrent in-flight calls."""

    max_queue: int = 0
    """Maximum number of calls allowed to wait for a slot before rejection."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.max_concurrency < 1:
            raise exc.configuration("Bulkhead max_concurrency must be >= 1")

        if self.max_queue < 0:
            raise exc.configuration("Bulkhead max_queue must be >= 0")


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class CircuitBreakerStrategy:
    """Rolling-window circuit breaker."""

    failure_ratio: float
    """Failure fraction within the window that trips the breaker open."""

    sampling_window: timedelta
    """Rolling window over which outcomes are counted."""

    min_throughput: int
    """Minimum calls in the window before the breaker may trip."""

    break_duration: timedelta
    """How long the breaker stays open before probing half-open."""

    half_open_max_calls: int = 1
    """Number of probe calls allowed while half-open."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not 0 < self.failure_ratio <= 1:
            raise exc.configuration("Circuit breaker failure_ratio must be in (0, 1]")

        if self.sampling_window.total_seconds() <= 0:
            raise exc.configuration("Circuit breaker sampling_window must be positive")

        if self.min_throughput < 1:
            raise exc.configuration("Circuit breaker min_throughput must be >= 1")

        if self.break_duration.total_seconds() <= 0:
            raise exc.configuration("Circuit breaker break_duration must be positive")

        if self.half_open_max_calls < 1:
            raise exc.configuration("Circuit breaker half_open_max_calls must be >= 1")


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RetryStrategy:
    """Bounded retry on classified-retryable failures."""

    max_attempts: int
    """Total attempts including the first (``>= 1``)."""

    backoff: BackoffStrategy
    """Backoff schedule between attempts."""

    retry_on: frozenset[ExceptionKind]
    """Exception kinds that trigger a retry (all must be retryable)."""

    budget: RetryBudget | None = None
    """Optional token-bucket cap on retries across calls."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.max_attempts < 1:
            raise exc.configuration("Retry max_attempts must be >= 1")

        if not self.retry_on:
            raise exc.configuration("Retry retry_on must not be empty")

        non_retryable = sorted(
            kind.value
            for kind in self.retry_on
            if not exception_egress_policy(kind).retryable
        )

        if non_retryable:
            raise exc.configuration(
                "Retry retry_on includes non-retryable kinds: "
                + ", ".join(non_retryable),
            )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class TimeoutStrategy:
    """Per-attempt timeout."""

    timeout: timedelta
    """Maximum duration of a single attempt before cancellation."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.timeout.total_seconds() <= 0:
            raise exc.configuration("Timeout must be positive")


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class FallbackStrategy:
    """Marker enabling a call-site ``fallback`` callable for this policy."""


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class HedgeStrategy:
    """Concurrent redundant attempts to cut tail latency (parallel-on-slowness).

    A meta-strategy applied *outside* the policy pipeline (by ``run_hedged`` via
    :class:`~forze.application.hooks.resilience.HedgeWrap`), not composed into the
    outer-to-inner strategy order. Only safe on idempotent / read-only operations.
    """

    delay: timedelta
    """Wait before firing the next concurrent copy (~p95 latency)."""

    max_attempts: int
    """Total concurrent attempts including the primary (``>= 2``)."""

    budget: RetryBudget | None = None
    """Optional token-bucket cap on extra attempts (load amplification)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.delay.total_seconds() < 0:
            raise exc.configuration("Hedge delay must be >= 0")

        if self.max_attempts < 2:
            raise exc.configuration("Hedge max_attempts must be >= 2")


# ....................... #

Strategy = (
    BulkheadStrategy
    | CircuitBreakerStrategy
    | RetryStrategy
    | TimeoutStrategy
    | FallbackStrategy
)
"""Union of all strategy value objects composable into a policy."""

_STRATEGY_ORDER: tuple[type, ...] = (
    BulkheadStrategy,
    CircuitBreakerStrategy,
    RetryStrategy,
    TimeoutStrategy,
)
"""Canonical outer-to-inner order for functional strategies."""


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ResiliencePolicy:
    """Ordered composition of strategies applied outer-to-inner around a call.

    The canonical order is Bulkhead -> CircuitBreaker -> Retry -> Timeout, which
    means the circuit breaker composes **outside** retry: the breaker admits and
    records exactly **one** outcome per logical call, after the whole retry loop
    has run. A retry storm (``max_attempts`` failing attempts) therefore counts
    as a *single* breaker failure — breaker thresholds are tuned against logical
    calls, not individual attempts.
    """

    name: StrKey
    """Policy name used to reference it and to key process-local state."""

    strategies: tuple[Strategy, ...]
    """Strategies in canonical order; the fallback marker may appear anywhere."""

    hedge: HedgeStrategy | None = None
    """Optional hedging meta-strategy applied outer to the pipeline by ``run_hedged``."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.strategies:
            raise exc.configuration("Resilience policy must declare a strategy")

        functional = [
            type(s) for s in self.strategies if not isinstance(s, FallbackStrategy)
        ]

        if len(set(functional)) != len(functional):
            raise exc.configuration("Resilience policy has duplicate strategy types")

        ranks = [_STRATEGY_ORDER.index(t) for t in functional]

        if ranks != sorted(ranks):
            raise exc.configuration(
                "Resilience policy strategies must be ordered "
                "Bulkhead -> CircuitBreaker -> Retry -> Timeout",
            )

    # ....................... #

    def _of_type[S: Strategy](self, tp: type[S]) -> S | None:
        for strategy in self.strategies:
            if isinstance(strategy, tp):
                return strategy

        return None

    # ....................... #

    @property
    def bulkhead(self) -> BulkheadStrategy | None:
        """Bulkhead strategy if declared."""

        return self._of_type(BulkheadStrategy)

    @property
    def circuit_breaker(self) -> CircuitBreakerStrategy | None:
        """Circuit breaker strategy if declared."""

        return self._of_type(CircuitBreakerStrategy)

    @property
    def retry(self) -> RetryStrategy | None:
        """Retry strategy if declared."""

        return self._of_type(RetryStrategy)

    @property
    def timeout(self) -> TimeoutStrategy | None:
        """Timeout strategy if declared."""

        return self._of_type(TimeoutStrategy)

    @property
    def has_fallback(self) -> bool:
        """Whether a call-site fallback is permitted for this policy."""

        return any(isinstance(s, FallbackStrategy) for s in self.strategies)
