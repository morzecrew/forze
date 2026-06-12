"""Process-local mutable state for circuit breaker, bulkhead, rate limit, and retry budget."""

import asyncio
import time
from collections import deque
from typing import Literal

import attrs

from forze.base.exceptions import exc

from ..context.deadline import current_deadline

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

    limit: float = attrs.field(
        default=attrs.Factory(lambda self: float(self.max_concurrency), takes_self=True),
        init=False,
    )
    """Current concurrency limit (floored to int for admission)."""

    in_use: int = attrs.field(default=0, init=False)
    """Admitted calls currently in flight."""

    waiting: int = attrs.field(default=0, init=False)
    """Calls parked waiting for a slot."""

    last_decrease_at: float = attrs.field(default=float("-inf"), init=False)
    """Clock of the last multiplicative decrease (cooldown anchor)."""

    _waiters: deque[tuple[asyncio.Future[None], float | None]] = attrs.field(
        factory=deque,
        init=False,
    )

    # ....................... #

    def can_admit(self) -> bool:
        """Whether a call may take a slot or join the wait queue."""

        if self.in_use < int(self.limit):
            return True

        return self.waiting < self.max_queue

    # ....................... #

    async def acquire(self) -> None:
        """Take a slot, waiting in FIFO order when the limit is saturated.

        Waiters carry their invocation deadline (captured at park time): a
        waiter whose budget expired while parked is failed at wake instead of
        being granted a slot it can only waste — the outer deadline timeout
        would reclaim the grant immediately anyway.
        """

        if self.in_use < int(self.limit) and not self._waiters:
            self.in_use += 1
            return

        waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        entry = (waiter, current_deadline())
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

    def _wake(self) -> None:
        while self._waiters and self.in_use < int(self.limit):
            waiter, deadline = self._waiters.popleft()

            if waiter.cancelled():
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

    # ....................... #

    def on_complete(self, latency: float, now: float) -> bool:
        """Adjust the limit for a completed call; return ``True`` on a decrease."""

        if latency > self.latency_threshold:
            if now - self.last_decrease_at < self.cooldown:
                return False

            self.last_decrease_at = now
            self.limit = max(float(self.min_concurrency), self.limit * self.backoff_ratio)

            return True

        self.limit = min(
            float(self.max_concurrency),
            self.limit + self.increase_step / max(self.limit, 1.0),
        )

        return False
