"""Process-local mutable state for circuit breaker, bulkhead, rate limit, and retry budget."""

import asyncio
from typing import Literal

import attrs

# ----------------------- #

BreakerPhase = Literal["closed", "open", "half_open"]
"""Circuit breaker lifecycle phase."""

Transition = Literal["open", "closed", "half_open"] | None
"""Phase transition emitted by a state update, or ``None`` when unchanged."""

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
class BulkheadState:
    """Concurrency limiter with a bounded waiting queue."""

    max_concurrency: int
    max_queue: int
    sem: asyncio.Semaphore = attrs.field(
        default=attrs.Factory(
            lambda self: asyncio.Semaphore(self.max_concurrency),
            takes_self=True,
        ),
        init=False,
    )
    waiting: int = 0

    # ....................... #

    def can_admit(self) -> bool:
        """Return whether a call may acquire a slot or join the wait queue."""

        if not self.sem.locked():
            return True

        return self.waiting < self.max_queue


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
