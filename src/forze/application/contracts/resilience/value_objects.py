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

    queue_target: timedelta | None = None
    """CoDel target sojourn: under *sustained* congestion (the queue has not
    been empty for ``queue_interval``), a waiter parked longer than this is
    shed at dequeue with ``code="bulkhead_queue_shed"`` — bounding queueing by
    *time* the caller experiences, not just queue length. ``None`` (default)
    keeps the size-only bound. Requires ``max_queue >= 1``."""

    queue_interval: timedelta = timedelta(milliseconds=100)
    """CoDel interval: the congestion-detection window and the generous sojourn
    allowance while the queue has recently been empty."""

    queue_adaptive_lifo: bool = False
    """Serve the *newest* waiter first while congested — its client is the one
    most likely still waiting (Facebook, "Fail at Scale"); FIFO otherwise.
    Deliberately starves the old tail under overload; pair with
    ``queue_target`` so the starved tail is shed instead of parked forever.
    Requires ``max_queue >= 1``."""

    prioritized: bool = False
    """Criticality-aware shedding (Netflix-style prioritized load shedding).
    When set, the per-request :class:`~forze.application.execution.context.Criticality`
    drives admission and CoDel shedding: a full queue admits a higher-criticality
    arrival by shedding the lowest-criticality waiter, and lower tiers are shed
    sooner under sustained congestion. A no-op while every request shares a tier.
    Requires ``max_queue >= 1``."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.max_concurrency < 1:
            raise exc.configuration("Bulkhead max_concurrency must be >= 1")

        if self.max_queue < 0:
            raise exc.configuration("Bulkhead max_queue must be >= 0")

        if self.queue_target is not None and self.queue_target.total_seconds() <= 0:
            raise exc.configuration("Bulkhead queue_target must be positive")

        if self.queue_interval.total_seconds() <= 0:
            raise exc.configuration("Bulkhead queue_interval must be positive")

        if self.queue_target is not None and self.queue_target >= self.queue_interval:
            raise exc.configuration(
                "Bulkhead queue_target must be smaller than queue_interval"
            )

        if (
            self.queue_target is not None
            or self.queue_adaptive_lifo
            or self.prioritized
        ) and self.max_queue < 1:
            raise exc.configuration(
                "Bulkhead queue management (queue_target / queue_adaptive_lifo / "
                "prioritized) requires max_queue >= 1"
            )


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

    latency_quantile: float | None = None
    """Opt-in percentile-windowed congestion signal: instead of *any single*
    completion over the threshold counting as a breach, breach only when this
    quantile of recent completed-call latencies (windowed streaming P²
    estimate) exceeds ``latency_threshold``. Typical ``0.95``: the contract
    becomes "the p95 must stay under the threshold" — one GC pause or cold
    query can no longer crater concurrency, only a genuinely shifted
    distribution can. A backoff opens a fresh measurement epoch (the estimator
    resets), so a stale-high quantile never ratchets the limit to the floor
    after the downstream recovers. ``None`` (default) keeps the per-sample
    signal."""

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

    queue_target: timedelta | None = None
    """CoDel target sojourn: under *sustained* congestion (the queue has not
    been empty for ``queue_interval``), a waiter parked longer than this is
    shed at dequeue with ``code="bulkhead_queue_shed"`` — bounding queueing by
    *time* the caller experiences, not just queue length. ``None`` (default)
    keeps the size-only bound. Requires ``max_queue >= 1``."""

    queue_interval: timedelta = timedelta(milliseconds=100)
    """CoDel interval: the congestion-detection window and the generous sojourn
    allowance while the queue has recently been empty."""

    queue_adaptive_lifo: bool = False
    """Serve the *newest* waiter first while congested — its client is the one
    most likely still waiting (Facebook, "Fail at Scale"); FIFO otherwise.
    Deliberately starves the old tail under overload; pair with
    ``queue_target`` so the starved tail is shed instead of parked forever.
    Requires ``max_queue >= 1``."""

    prioritized: bool = False
    """Criticality-aware shedding (Netflix-style prioritized load shedding).
    When set, the per-request :class:`~forze.application.execution.context.Criticality`
    drives admission and CoDel shedding: a full queue admits a higher-criticality
    arrival by shedding the lowest-criticality waiter, and lower tiers are shed
    sooner under sustained congestion. A no-op while every request shares a tier.
    Requires ``max_queue >= 1``."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.latency_threshold.total_seconds() <= 0:
            raise exc.configuration(
                "Adaptive bulkhead latency_threshold must be positive"
            )

        if self.latency_quantile is not None and not (
            0.0 < self.latency_quantile < 1.0
        ):
            raise exc.configuration(
                "Adaptive bulkhead latency_quantile must be in (0, 1)"
            )

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

        if self.queue_target is not None and self.queue_target.total_seconds() <= 0:
            raise exc.configuration("Bulkhead queue_target must be positive")

        if self.queue_interval.total_seconds() <= 0:
            raise exc.configuration("Bulkhead queue_interval must be positive")

        if self.queue_target is not None and self.queue_target >= self.queue_interval:
            raise exc.configuration(
                "Bulkhead queue_target must be smaller than queue_interval"
            )

        if (
            self.queue_target is not None
            or self.queue_adaptive_lifo
            or self.prioritized
        ) and self.max_queue < 1:
            raise exc.configuration(
                "Bulkhead queue management (queue_target / queue_adaptive_lifo / "
                "prioritized) requires max_queue >= 1"
            )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class GradientBulkheadStrategy:
    """Delay-based concurrency limiter: a bulkhead whose limit tracks the latency
    gradient (Gradient2, Netflix concurrency-limits).

    The same admission machinery as :class:`AdaptiveBulkheadStrategy` (bounded
    queue, optional CoDel / adaptive-LIFO / prioritized shedding), but the limit
    is driven by a delay-based controller instead of AIMD: it learns the no-load
    latency baseline and contracts as latency inflates — with **no
    ``latency_threshold`` to tune**. Only *successful* completions feed the
    controller; failures are the circuit breaker's job and leave the limit
    untouched. Mutually exclusive with the other bulkhead kinds within a policy.
    """

    max_concurrency: int
    """Ceiling and initial limit."""

    min_concurrency: int = 1
    """Floor the limit never decreases below."""

    max_queue: int = 0
    """Maximum number of calls allowed to wait for a slot before rejection."""

    rtt_tolerance: float = 1.5
    """Latency-rise headroom before contracting: ``1.5`` tolerates a 50% rise
    over the learned baseline before the gradient drops below ``1.0``."""

    smoothing: float = 0.2
    """EWMA factor applied to limit *increases* (gentle ramp up; fast down)."""

    long_window: int = 600
    """Samples over which the no-load baseline RTT is averaged."""

    headroom: float = 4.0
    """Standing in-flight headroom the limit probes toward while healthy (the
    Gradient2 ``queue_size`` term — distinct from ``max_queue``)."""

    queue_target: timedelta | None = None
    """CoDel target sojourn (see :class:`AdaptiveBulkheadStrategy`)."""

    queue_interval: timedelta = timedelta(milliseconds=100)
    """CoDel interval (see :class:`AdaptiveBulkheadStrategy`)."""

    queue_adaptive_lifo: bool = False
    """Serve the newest waiter first while congested (see :class:`AdaptiveBulkheadStrategy`)."""

    prioritized: bool = False
    """Criticality-aware shedding (see :class:`AdaptiveBulkheadStrategy`)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.min_concurrency < 1:
            raise exc.configuration("Gradient bulkhead min_concurrency must be >= 1")

        if self.max_concurrency < self.min_concurrency:
            raise exc.configuration(
                "Gradient bulkhead max_concurrency must be >= min_concurrency"
            )

        if self.max_queue < 0:
            raise exc.configuration("Gradient bulkhead max_queue must be >= 0")

        if self.rtt_tolerance < 1.0:
            raise exc.configuration("Gradient bulkhead rtt_tolerance must be >= 1.0")

        if not 0.0 < self.smoothing <= 1.0:
            raise exc.configuration("Gradient bulkhead smoothing must be in (0, 1]")

        if self.long_window < 2:
            raise exc.configuration("Gradient bulkhead long_window must be >= 2")

        if self.headroom < 0.0:
            raise exc.configuration("Gradient bulkhead headroom must be >= 0")

        if self.queue_target is not None and self.queue_target.total_seconds() <= 0:
            raise exc.configuration("Bulkhead queue_target must be positive")

        if self.queue_interval.total_seconds() <= 0:
            raise exc.configuration("Bulkhead queue_interval must be positive")

        if self.queue_target is not None and self.queue_target >= self.queue_interval:
            raise exc.configuration(
                "Bulkhead queue_target must be smaller than queue_interval"
            )

        if (
            self.queue_target is not None
            or self.queue_adaptive_lifo
            or self.prioritized
        ) and self.max_queue < 1:
            raise exc.configuration(
                "Bulkhead queue management (queue_target / queue_adaptive_lifo / "
                "prioritized) requires max_queue >= 1"
            )


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
class AdaptiveThrottleStrategy:
    """Probabilistic client-side shedding for a degraded downstream (SRE book).

    Tracks ``requests`` and ``accepts`` per window and rejects locally with
    probability ``max(0, (requests − k·accepts) / (requests + 1))``. Where the
    circuit breaker is binary — full traffic or a half-open trickle — this
    sheds *proportionally*: at 50% downstream failure it sends roughly the
    traffic the downstream is absorbing, and as the downstream recovers,
    passed-through successes rebuild the accept ratio and shedding decays to
    zero on its own. Locally-shed calls count as requests but not accepts,
    which is what makes the steady state self-limiting (the client converges
    on roughly ``k ×`` the downstream's current capacity).

    "Accepted" mirrors the breaker's outcome classification inverted: a
    success, or a failure whose kind is *non-retryable* (a domain error is
    the downstream doing its job, not buckling). Mutually exclusive with
    :class:`CircuitBreakerStrategy` in one policy — composed, the throttle
    would observe the breaker's own local rejections as overload evidence.
    Position the throttle on degraded-but-alive downstreams and the breaker
    on ones that fail outright.
    """

    k: float = 2.0
    """Permissiveness multiplier. Higher tolerates more failure before
    shedding; ``2.0`` is the published production default. Must be ``>= 1``
    (below that, a perfectly healthy downstream would already be shed)."""

    window: timedelta = timedelta(minutes=2)
    """Counting window. Counters reset when it elapses, so shedding decays
    within one window of a recovery even with no traffic passing."""

    min_throughput: int = 10
    """Requests per window below which nothing is shed — trivial volume is
    never throttled."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.k < 1.0:
            raise exc.configuration("Adaptive throttle k must be >= 1")

        if self.window.total_seconds() <= 0:
            raise exc.configuration("Adaptive throttle window must be positive")

        if self.min_throughput < 1:
            raise exc.configuration("Adaptive throttle min_throughput must be >= 1")


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
    """Wait before firing the next concurrent copy (~p95 latency). With
    :attr:`adaptive_delay_quantile` set, this is the fallback used until the
    estimator has warmed up."""

    max_attempts: int
    """Total concurrent attempts including the primary (``>= 2``)."""

    budget: RetryBudget | None = None
    """Optional token-bucket cap on extra attempts (load amplification)."""

    adaptive_delay_quantile: float | None = None
    """Opt-in tail-based hedge delay (*The Tail at Scale*): track this quantile
    of observed primary-attempt latencies per ``(policy, route)`` (streaming P²
    estimation — five floats, no sample storage) and hedge after *that* instead
    of the fixed :attr:`delay`. Typical ``0.95``: the hedge fires only for the
    slowest ~5% of calls, and the trigger point follows the downstream's
    latency distribution as it moves. ``None`` (default) keeps the fixed
    delay."""

    delay_min: timedelta | None = None
    """Floor for the adaptive delay. Guards against over-eager hedging when the
    observed distribution collapses (every call fast → tiny quantile → hedges
    fire on the slightest blip). Requires :attr:`adaptive_delay_quantile`."""

    delay_max: timedelta | None = None
    """Cap for the adaptive delay. Guards against a degraded downstream
    dragging the quantile so high the hedge never rescues anything. Requires
    :attr:`adaptive_delay_quantile`."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.delay.total_seconds() < 0:
            raise exc.configuration("Hedge delay must be >= 0")

        if self.max_attempts < 2:
            raise exc.configuration("Hedge max_attempts must be >= 2")

        if self.adaptive_delay_quantile is not None and not (
            0.0 < self.adaptive_delay_quantile < 1.0
        ):
            raise exc.configuration("Hedge adaptive_delay_quantile must be in (0, 1)")

        if (
            self.delay_min is not None or self.delay_max is not None
        ) and self.adaptive_delay_quantile is None:
            raise exc.configuration(
                "Hedge delay_min/delay_max require adaptive_delay_quantile"
            )

        if self.delay_min is not None and self.delay_min.total_seconds() <= 0:
            raise exc.configuration("Hedge delay_min must be positive")

        if self.delay_max is not None and self.delay_max.total_seconds() <= 0:
            raise exc.configuration("Hedge delay_max must be positive")

        if (
            self.delay_min is not None
            and self.delay_max is not None
            and self.delay_min > self.delay_max
        ):
            raise exc.configuration("Hedge delay_min must be <= delay_max")


# ....................... #

Strategy = (
    RateLimitStrategy
    | BulkheadStrategy
    | AdaptiveBulkheadStrategy
    | GradientBulkheadStrategy
    | CircuitBreakerStrategy
    | AdaptiveThrottleStrategy
    | RetryStrategy
    | TimeoutStrategy
    | FallbackStrategy
)
"""Union of all strategy value objects composable into a policy."""

_STRATEGY_ORDER: tuple[type, ...] = (
    RateLimitStrategy,
    BulkheadStrategy,
    AdaptiveBulkheadStrategy,
    GradientBulkheadStrategy,
    CircuitBreakerStrategy,
    AdaptiveThrottleStrategy,
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

        bulkhead_kinds = sum(
            kind in functional
            for kind in (
                BulkheadStrategy,
                AdaptiveBulkheadStrategy,
                GradientBulkheadStrategy,
            )
        )

        if bulkhead_kinds > 1:
            raise exc.configuration(
                "Resilience policy cannot combine Bulkhead / AdaptiveBulkhead / "
                "GradientBulkhead strategies — they occupy the same slot",
            )

        if (
            CircuitBreakerStrategy in functional
            and AdaptiveThrottleStrategy in functional
        ):
            raise exc.configuration(
                "Resilience policy cannot combine CircuitBreakerStrategy with "
                "AdaptiveThrottleStrategy — composed, the throttle would count "
                "the breaker's own local rejections as overload evidence; pick "
                "the throttle for degraded downstreams, the breaker for dead ones",
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
    def gradient_bulkhead(self) -> GradientBulkheadStrategy | None:
        """Delay-based (Gradient2) bulkhead strategy if declared."""

        return self._of_type(GradientBulkheadStrategy)

    @property
    def circuit_breaker(self) -> CircuitBreakerStrategy | None:
        """Circuit breaker strategy if declared."""

        return self._of_type(CircuitBreakerStrategy)

    @property
    def adaptive_throttle(self) -> AdaptiveThrottleStrategy | None:
        """Adaptive client-throttle strategy if declared."""

        return self._of_type(AdaptiveThrottleStrategy)

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
