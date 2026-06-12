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
class RateLimitStrategy:
    """Token-bucket rate limiter: sustained rate ``permits/per``, capacity ``burst or permits``.

    The bucket starts full and refills continuously at the sustained rate.
    Each call consumes one token; a call that finds the bucket empty is
    **rejected immediately** (no queuing) with
    ``exc.throttled(code="rate_limited", details={"policy": ..., "route": ...})``.

    The limiter composes **outermost** (before Bulkhead): it caps the admission
    rate of the whole pipeline, and its rejection is *not* retried by the same
    policy's (inner) Retry strategy. To **wait** for capacity instead of failing
    fast, compose at the next level out: ``THROTTLED`` is classified retryable,
    so a retry-with-backoff policy *around* the rate-limited call turns
    rejection into waiting::

        # Policy A: the limit itself (e.g. attached to a port via PortPolicy).
        limited = ResiliencePolicy(
            name="vendor_api",
            strategies=(RateLimitStrategy(permits=10, per=timedelta(seconds=1)),),
        )

        # Policy B: wait out the throttle at the call site.
        patient = ResiliencePolicy(
            name="patient",
            strategies=(
                RetryStrategy(
                    max_attempts=4,
                    backoff=BackoffStrategy(
                        base=timedelta(milliseconds=100),
                        max=timedelta(seconds=2),
                    ),
                    retry_on=frozenset({ExceptionKind.THROTTLED}),
                ),
            ),
        )

        result = await ctx.resilience().run(
            lambda: vendor.fetch(...),  # raises THROTTLED when the bucket is empty
            policy="patient",
        )

    The same retry also waits out backend-raised throttles (e.g. an HTTP 429
    mapped to ``THROTTLED``) — the limiter and the backend reject the same way.
    """

    permits: int
    """Permits issued per ``per`` window (sustained rate numerator, ``>= 1``)."""

    per: timedelta
    """Window over which ``permits`` tokens refill (sustained rate denominator)."""

    burst: int | None = None
    """Bucket capacity (max tokens saved up); defaults to ``permits``."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.permits < 1:
            raise exc.configuration("Rate limit permits must be >= 1")

        if self.per.total_seconds() <= 0:
            raise exc.configuration("Rate limit per must be positive")

        if self.burst is not None and self.burst < 1:
            raise exc.configuration("Rate limit burst must be >= 1")

    # ....................... #

    @property
    def capacity(self) -> int:
        """Effective bucket capacity (``burst`` or ``permits``)."""

        return self.burst if self.burst is not None else self.permits


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
class AdaptiveBulkheadStrategy:
    """AIMD concurrency limiter: a bulkhead that backs off under latency pressure.

    Starts at ``max_concurrency`` and behaves exactly like a fixed bulkhead
    until a completed call exceeds ``latency_threshold``; the limit then
    decreases multiplicatively (``limit *= backoff_ratio``, at most once per
    ``cooldown``) and recovers additively (``+= increase_step / limit`` per
    in-budget completion — one slot per ~limit successes, the TCP-style probe).
    AIMD is the principled choice for *uncoordinated* replicas: N process-local
    limits sharing one downstream converge like N TCP flows sharing a link, so
    no distributed state is needed.

    Congestion signal is **latency only** — errors stay the circuit breaker's
    job (a fast-failing downstream must not crater concurrency exactly when
    failures are cheap). A per-attempt timeout firing counts as a breach at the
    timeout value. The sample is the whole guarded call (retries and backoff
    sleeps included when composed with a Retry strategy) — set the threshold
    for the logical call, not the single attempt.

    Shrinking never evicts in-flight work: the limit only gates admission.
    Mutually exclusive with :class:`BulkheadStrategy` within one policy.
    """

    latency_threshold: timedelta
    """Completed-call latency above this counts as congestion."""

    max_concurrency: int
    """Ceiling and initial limit."""

    min_concurrency: int = 1
    """Floor the limit never decreases below."""

    max_queue: int = 0
    """Maximum number of calls allowed to wait for a slot before rejection."""

    backoff_ratio: float = 0.9
    """Multiplicative decrease applied on a latency breach."""

    increase_step: float = 1.0
    """Additive recovery: ``increase_step / limit`` per in-budget completion."""

    cooldown: timedelta = timedelta(seconds=1)
    """Minimum spacing between decreases — coalesces a burst of slow
    completions into one backoff instead of collapsing the limit to the floor."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.latency_threshold.total_seconds() <= 0:
            raise exc.configuration("Adaptive bulkhead latency_threshold must be positive")

        if self.min_concurrency < 1:
            raise exc.configuration("Adaptive bulkhead min_concurrency must be >= 1")

        if self.max_concurrency < self.min_concurrency:
            raise exc.configuration(
                "Adaptive bulkhead max_concurrency must be >= min_concurrency"
            )

        if self.max_queue < 0:
            raise exc.configuration("Adaptive bulkhead max_queue must be >= 0")

        if not 0.0 < self.backoff_ratio < 1.0:
            raise exc.configuration("Adaptive bulkhead backoff_ratio must be in (0, 1)")

        if self.increase_step <= 0:
            raise exc.configuration("Adaptive bulkhead increase_step must be positive")

        if self.cooldown.total_seconds() < 0:
            raise exc.configuration("Adaptive bulkhead cooldown must be >= 0")


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
    RateLimitStrategy
    | BulkheadStrategy
    | AdaptiveBulkheadStrategy
    | CircuitBreakerStrategy
    | RetryStrategy
    | TimeoutStrategy
    | FallbackStrategy
)
"""Union of all strategy value objects composable into a policy."""

_STRATEGY_ORDER: tuple[type, ...] = (
    RateLimitStrategy,
    BulkheadStrategy,
    AdaptiveBulkheadStrategy,
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

    The canonical order is RateLimit -> Bulkhead -> CircuitBreaker -> Retry ->
    Timeout. The rate limiter composes **outermost**: it gates admission before
    a call may even occupy a bulkhead slot, so throttled calls are rejected
    without consuming concurrency or counting against the breaker. The circuit
    breaker composes **outside** retry: the breaker admits and records exactly
    **one** outcome per logical call, after the whole retry loop has run. A
    retry storm (``max_attempts`` failing attempts) therefore counts as a
    *single* breaker failure — breaker thresholds are tuned against logical
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

        if BulkheadStrategy in functional and AdaptiveBulkheadStrategy in functional:
            raise exc.configuration(
                "Resilience policy cannot combine BulkheadStrategy with "
                "AdaptiveBulkheadStrategy — they occupy the same slot",
            )

        ranks = [_STRATEGY_ORDER.index(t) for t in functional]

        if ranks != sorted(ranks):
            raise exc.configuration(
                "Resilience policy strategies must be ordered "
                "RateLimit -> Bulkhead -> CircuitBreaker -> Retry -> Timeout",
            )

    # ....................... #

    def _of_type[S: Strategy](self, tp: type[S]) -> S | None:
        for strategy in self.strategies:
            if isinstance(strategy, tp):
                return strategy

        return None

    # ....................... #

    @property
    def rate_limit(self) -> RateLimitStrategy | None:
        """Rate limit strategy if declared."""

        return self._of_type(RateLimitStrategy)

    @property
    def bulkhead(self) -> BulkheadStrategy | None:
        """Bulkhead strategy if declared."""

        return self._of_type(BulkheadStrategy)

    @property
    def adaptive_bulkhead(self) -> AdaptiveBulkheadStrategy | None:
        """Adaptive (AIMD) bulkhead strategy if declared."""

        return self._of_type(AdaptiveBulkheadStrategy)

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
