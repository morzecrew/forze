"""Process-local mutable state for circuit breaker, bulkhead, rate limit, and retry budget."""

import asyncio
import time
from collections import deque
from typing import Callable, Literal

import attrs

from forze.base.exceptions import exc
from forze.base.primitives import WindowedP2Quantile

from ..context.criticality import Criticality, current_criticality
from ..context.deadline import current_deadline

# ----------------------- #

BreakerPhase = Literal["closed", "open", "half_open"]
"""Circuit breaker lifecycle phase."""

Transition = Literal["open", "closed", "half_open"] | None
"""Phase transition emitted by a state update, or ``None`` when unchanged."""

_CRITICALITY_GRACE: dict[Criticality, float] = {
    Criticality.BEST_EFFORT: 0.25,
    Criticality.DEGRADED: 0.5,
    Criticality.NORMAL: 1.0,
    Criticality.CRITICAL: 2.0,
}
"""Per-tier CoDel sojourn-allowance multiplier (prioritized mode): a lower tier
breaches its (tighter) allowance — and so is shed — sooner under congestion,
while a critical request is granted extra grace before it can be shed."""

# ....................... #


@attrs.define(slots=True)
class BreakerState:
    """Rolling-window circuit breaker state keyed by ``(policy, route)``."""

    failure_ratio: float
    window: float
    min_throughput: int
    break_duration: float
    half_open_max_calls: int

    phase: BreakerPhase = "closed"
    window_start: float = 0.0
    successes: int = 0
    failures: int = 0
    opened_at: float = 0.0
    half_open_calls: int = 0

    # ....................... #

    def _roll(self, now: float) -> None:
        if now - self.window_start >= self.window:
            self.window_start = now
            self.successes = 0
            self.failures = 0

    # ....................... #

    def try_admit(self, now: float) -> tuple[bool, Transition]:
        """Return whether a call may proceed, plus any phase transition."""

        transition: Transition = None

        if self.phase == "open":
            if now - self.opened_at >= self.break_duration:
                self.phase = "half_open"
                self.half_open_calls = 0
                transition = "half_open"

            else:
                return False, None

        if self.phase == "half_open":
            if self.half_open_calls < self.half_open_max_calls:
                self.half_open_calls += 1
                return True, transition

            return False, transition

        return True, transition

    # ....................... #

    def on_success(self, now: float) -> Transition:
        """Record a successful outcome; return a transition if the breaker closes."""

        if self.phase == "half_open":
            self.phase = "closed"
            self.window_start = now
            self.successes = 0
            self.failures = 0
            return "closed"

        self._roll(now)
        self.successes += 1
        return None

    # ....................... #

    def on_failure(self, now: float) -> Transition:
        """Record a failed outcome; return a transition if the breaker opens."""

        if self.phase == "half_open":
            self.phase = "open"
            self.opened_at = now
            return "open"

        self._roll(now)
        self.failures += 1
        total = self.successes + self.failures

        if total >= self.min_throughput and self.failures / total >= self.failure_ratio:
            self.phase = "open"
            self.opened_at = now
            return "open"

        return None


# ....................... #


@attrs.define(slots=True)
class RateLimitState:
    """Token-bucket rate limiter state keyed by ``(policy, route)``.

    Refill is computed lazily on each acquire from the monotonic-clock delta —
    no background task. Mutation happens synchronously between awaits, so the
    state is safe under a single event loop without locks (same model as
    :class:`BulkheadState` and :class:`BudgetState`).
    """

    rate: float
    """Tokens refilled per second (``permits / per``)."""

    capacity: float
    """Maximum tokens the bucket holds (``burst or permits``); starts full."""

    tokens: float = attrs.field(
        default=attrs.Factory(lambda self: self.capacity, takes_self=True),
    )
    """Currently available tokens."""

    updated_at: float = 0.0
    """Monotonic timestamp of the last refill computation."""

    # ....................... #

    def try_acquire(self, now: float) -> bool:
        """Refill from elapsed time, then spend one token if available."""

        elapsed = now - self.updated_at

        if elapsed > 0:
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)

        self.updated_at = now

        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True

        return False


# ....................... #


@attrs.define(slots=True)
class BudgetState:
    """Token-bucket retry budget keyed by ``(policy, route)``."""

    ratio: float
    min_throughput: int
    calls: int = 0
    tokens: float = 0.0

    # ....................... #

    def _bucket(self) -> float:
        return max(1.0, self.min_throughput * self.ratio)

    # ....................... #

    def on_call(self) -> None:
        """Account for a new top-level call and earn retry budget."""

        self.calls += 1
        self.tokens = min(self._bucket(), self.tokens + self.ratio)

    # ....................... #

    def try_spend(self) -> bool:
        """Return whether a retry is permitted, spending a token if so."""

        if self.calls <= self.min_throughput:
            return True

        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True

        return False


# ....................... #


@attrs.define(slots=True)
class AdaptiveBulkheadState:
    """AIMD concurrency limiter: counter-based admission with a dynamic limit.

    A semaphore cannot change capacity, so admission is an ``in_use`` counter
    plus a FIFO of waiter futures: admit while ``in_use < floor(limit)``, park
    up to ``max_queue`` waiters, wake them on release. Shrinking the limit
    never evicts in-flight work — it only gates new admissions.

    The AIMD controller: an in-budget completion adds ``increase_step / limit``
    (one slot per ~limit successes); a breach multiplies by ``backoff_ratio``,
    at most once per ``cooldown`` so a burst of slow completions backs off once
    instead of collapsing the limit to the floor. Single-event-loop discipline:
    state mutations happen between awaits.
    """

    latency_threshold: float
    min_concurrency: int
    max_concurrency: int
    max_queue: int
    backoff_ratio: float
    increase_step: float
    cooldown: float

    clock: Callable[[], float] = time.monotonic
    """Time source for queue sojourn/congestion tracking (injectable for tests).
    Waiter *deadlines* always compare against ``time.monotonic`` — they come
    from the deadline ContextVar, which is monotonic-based by contract."""

    latency_quantile: float | None = None
    """Percentile-windowed congestion signal: when set, a breach is "this
    quantile of recent completed-call latencies exceeds the threshold", not
    "this one sample did". ``None`` keeps the per-sample signal."""

    queue_target_s: float | None = None
    """CoDel target sojourn (seconds): under sustained congestion, a waiter
    queued longer than this is shed at dequeue. ``None`` disables CoDel."""

    queue_interval_s: float = 0.1
    """CoDel interval (seconds): the sojourn allowance while the queue has
    recently been empty, and the congestion detection window."""

    queue_adaptive_lifo: bool = False
    """Serve the *newest* waiter first while congested (its client is the most
    likely to still be waiting); FIFO otherwise."""

    prioritized: bool = False
    """Criticality-aware shedding: a full queue admits a higher-criticality
    arrival by displacing the lowest-criticality waiter, and lower tiers get a
    tighter CoDel sojourn allowance (see :data:`_CRITICALITY_GRACE`). Reads the
    ambient :func:`current_criticality` at park time. Inert when ``False``."""

    limit: float = attrs.field(
        default=attrs.Factory(
            lambda self: float(self.max_concurrency), takes_self=True
        ),
        init=False,
    )
    """Current concurrency limit (floored to int for admission)."""

    in_use: int = attrs.field(default=0, init=False)
    """Admitted calls currently in flight."""

    waiting: int = attrs.field(default=0, init=False)
    """Calls parked waiting for a slot."""

    last_decrease_at: float = attrs.field(default=float("-inf"), init=False)
    """Clock of the last multiplicative decrease (cooldown anchor)."""

    last_empty_at: float = attrs.field(
        default=attrs.Factory(lambda self: self.clock(), takes_self=True),
        init=False,
    )
    """Last instant the wait queue was observed empty (congestion anchor)."""

    _waiters: deque[tuple[asyncio.Future[None], float | None, float, Criticality]] = (
        attrs.field(
            factory=deque,
            init=False,
        )
    )

    _latency_estimator: WindowedP2Quantile | None = attrs.field(
        default=attrs.Factory(
            lambda self: (
                WindowedP2Quantile(p=self.latency_quantile)
                if self.latency_quantile is not None
                else None
            ),
            takes_self=True,
        ),
        init=False,
    )
    """Windowed P² estimate of completed-call latency (quantile mode only)."""

    # ....................... #

    def can_admit(self) -> bool:
        """Whether a call may take a slot or join the wait queue.

        Prioritized mode: a full queue still admits a higher-criticality arrival,
        which will displace the lowest-criticality waiter on :meth:`acquire`.
        """

        if self.in_use < int(self.limit):
            return True

        if self.waiting < self.max_queue:
            return True

        if not self.prioritized:
            return False

        incoming = current_criticality()

        return any(crit < incoming for *_, crit in self._waiters)

    # ....................... #

    async def acquire(self) -> None:
        """Take a slot, waiting in FIFO order when the limit is saturated.

        Waiters carry their invocation deadline (captured at park time): a
        waiter whose budget expired while parked is failed at wake instead of
        being granted a slot it can only waste — the outer deadline timeout
        would reclaim the grant immediately anyway.
        """

        criticality = current_criticality() if self.prioritized else Criticality.NORMAL

        if self.in_use < int(self.limit) and not self._waiters:
            self.in_use += 1
            return

        queue_was_empty = not self._waiters

        if self.prioritized and self.waiting >= self.max_queue:
            # Full queue: shed the lowest-criticality waiter to make room. The
            # executor's can_admit gate guarantees a strictly-lower victim.
            self._displace_lowest()

        if self._tracks_congestion() and queue_was_empty:
            # The queue was empty until this park: reset the congestion anchor.
            self.last_empty_at = self.clock()

        waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        entry = (waiter, current_deadline(), self.clock(), criticality)
        self._waiters.append(entry)
        self.waiting += 1

        try:
            await waiter

        except asyncio.CancelledError:
            if not waiter.cancelled() and waiter.done():
                # Slot was granted concurrently with the cancellation: return
                # it so the capacity is not leaked.
                self.in_use -= 1
                self._wake()

            raise

        finally:
            self.waiting -= 1

            if not waiter.done():
                self._waiters.remove(entry)

    # ....................... #

    def release(self) -> None:
        """Return a slot and wake a waiter if capacity allows."""

        self.in_use -= 1
        self._wake()

    # ....................... #

    def _displace_lowest(self) -> None:
        """Shed the lowest-criticality parked waiter to admit a higher one.

        Mirrors the CoDel shed path: the victim is removed from the queue and
        failed with ``bulkhead_queue_shed``; its own :meth:`acquire` ``finally``
        decrements ``waiting``. The caller (via :meth:`can_admit`) guarantees a
        strictly-lower-criticality victim exists.
        """

        victim = min(self._waiters, key=lambda entry: entry[3])
        self._waiters.remove(victim)
        victim[0].set_exception(
            exc.infrastructure(
                "Bulkhead queue shed: displaced by a higher-criticality request",
                code="bulkhead_queue_shed",
            )
        )

    # ....................... #

    def _tracks_congestion(self) -> bool:
        """Whether any queue feature needs the congestion anchor maintained."""

        return self.queue_target_s is not None or self.queue_adaptive_lifo

    # ....................... #

    def _congested(self, now: float) -> bool:
        """Sustained congestion: the queue has not been empty for an interval."""

        return bool(self._waiters) and now - self.last_empty_at >= self.queue_interval_s

    # ....................... #

    def _wake(self) -> None:
        now = self.clock()
        congested = self._congested(now)

        while self._waiters and self.in_use < int(self.limit):
            # Adaptive LIFO: while congested, the newest waiter is the one
            # whose client most likely still cares about the answer.
            entry = (
                self._waiters.pop()
                if self.queue_adaptive_lifo and congested
                else self._waiters.popleft()
            )
            waiter, deadline, enqueued_at, criticality = entry

            if waiter.cancelled():
                continue

            if self.queue_target_s is not None:
                # CoDel (simplified, Facebook-style): generous sojourn
                # allowance while the queue has recently been empty, tight
                # allowance under sustained congestion. Prioritized mode scales
                # the allowance by tier so lower-criticality waiters shed sooner.
                allowed = self.queue_target_s if congested else self.queue_interval_s

                if self.prioritized:
                    allowed *= _CRITICALITY_GRACE.get(criticality, 1.0)

                if now - enqueued_at > allowed:
                    waiter.set_exception(
                        exc.infrastructure(
                            "Bulkhead queue shed: waiter exceeded its sojourn "
                            "allowance under congestion",
                            code="bulkhead_queue_shed",
                        )
                    )
                    continue

            if deadline is not None and time.monotonic() >= deadline:
                # Expired while parked: fail it instead of granting a slot the
                # outer deadline timeout would reclaim before any work ran.
                waiter.set_exception(
                    exc.timeout(
                        "Invocation deadline expired while queued for a bulkhead slot",
                        code="deadline_exceeded",
                    )
                )
                continue

            self.in_use += 1
            waiter.set_result(None)

        if self._tracks_congestion() and not self._waiters:
            self.last_empty_at = self.clock()

    # ....................... #

    def on_complete(self, latency: float, now: float) -> bool:
        """Adjust the limit for a completed call; return ``True`` on a decrease.

        Per-sample mode: this completion's latency over the threshold is a
        breach. Quantile mode (``latency_quantile`` set): the windowed P²
        estimate over the threshold is — one outlier can't move a quantile,
        only a shifted distribution can. An undefined estimate (warming up)
        never breaches.
        """

        estimator = self._latency_estimator

        if estimator is not None:
            estimator.observe(latency)
            value = estimator.value()

            if value is None:
                # Warming (fewer than five samples this epoch): no signal in
                # either direction — hold the limit rather than reading
                # "unknown" as "healthy" and creeping back up mid-incident.
                return False

            breached = value > self.latency_threshold

        else:
            breached = latency > self.latency_threshold

        if not breached:
            self.limit = min(
                float(self.max_concurrency),
                self.limit + self.increase_step / max(self.limit, 1.0),
            )

            return False

        if now - self.last_decrease_at < self.cooldown:
            return False

        self.last_decrease_at = now
        self.limit = max(float(self.min_concurrency), self.limit * self.backoff_ratio)

        if estimator is not None:
            # Fresh measurement epoch: the old distribution justified this
            # decrease; only the *new* concurrency's latencies should decide
            # the next move. Without the reset, a stale-high quantile keeps
            # re-breaching for up to two windows after the downstream
            # recovers, ratcheting the limit to the floor once per cooldown.
            self._latency_estimator = WindowedP2Quantile(p=estimator.p)

        return True


# ....................... #


@attrs.define(slots=True)
class HedgeDelayState:
    """Tail-based hedge delay keyed by ``(policy, route)``.

    Tracks a quantile of observed **primary-attempt** latencies (windowed P²)
    and serves it — clamped by the configured floor/cap — as the hedge delay.
    Until the estimator has warmed up (five observations), the strategy's
    fixed delay is served unchanged.
    """

    quantile: float
    fixed_delay: float
    floor: float | None = None
    cap: float | None = None

    _estimator: WindowedP2Quantile = attrs.field(
        default=attrs.Factory(
            lambda self: WindowedP2Quantile(p=self.quantile),
            takes_self=True,
        ),
        init=False,
    )

    # ....................... #

    def observe(self, latency: float) -> None:
        """Record one primary-attempt latency sample (seconds)."""

        self._estimator.observe(latency)

    # ....................... #

    def delay(self) -> float:
        """The effective hedge delay in seconds."""

        estimate = self._estimator.value()

        if estimate is None:
            return self.fixed_delay

        if self.floor is not None:
            estimate = max(self.floor, estimate)

        if self.cap is not None:
            estimate = min(self.cap, estimate)

        return estimate


# ....................... #


@attrs.define(slots=True)
class AdaptiveThrottleState:
    """Adaptive client-throttle counters keyed by ``(policy, route)``.

    Tumbling window (same shape as :class:`BreakerState`): ``requests`` and
    ``accepts`` reset when the window elapses, so shedding decays within one
    window of a downstream recovery even when no traffic is passing. Shed
    calls count as requests but not accepts — the self-limiting property of
    the SRE-book algorithm.
    """

    k: float
    window: float
    min_throughput: int

    window_start: float = 0.0
    requests: int = 0
    accepts: int = 0

    # ....................... #

    def _roll(self, now: float) -> None:
        if now - self.window_start >= self.window:
            self.window_start = now
            self.requests = 0
            self.accepts = 0

    # ....................... #

    def reject_probability(self, now: float) -> float:
        """Current local-rejection probability: ``max(0, (req − k·acc)/(req + 1))``."""

        self._roll(now)

        if self.requests < self.min_throughput:
            return 0.0

        return max(0.0, (self.requests - self.k * self.accepts) / (self.requests + 1))

    # ....................... #

    def record_request(self, now: float) -> None:
        """Count one request — sent downstream or shed locally."""

        self._roll(now)
        self.requests += 1

    # ....................... #

    def record_accept(self, now: float) -> None:
        """Count one downstream accept (success, or a non-retryable failure)."""

        self._roll(now)
        self.accepts += 1
