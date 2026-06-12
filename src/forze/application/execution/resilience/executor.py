"""In-process resilience executor composing strategies into a call pipeline."""

import asyncio
import random
import time
from collections.abc import Awaitable, Callable

import attrs

from forze.application.contracts.resilience import (
    BulkheadStrategy,
    CircuitBreakerStrategy,
    HedgeStrategy,
    RateLimitStrategy,
    ResiliencePolicy,
    RetryStrategy,
    TimeoutStrategy,
)
from forze.base.exceptions import CoreException, exc, exception_egress_policy
from forze.base.primitives import MappingConverter, StrKey, StrKeyMapping

from ..tracing import record
from .backoff import compute_delay
from .state import BudgetState, BulkheadState, RateLimitState, Transition
from .store import CircuitBreakerStore, InMemoryCircuitBreakerStore

# ----------------------- #

_StateKey = tuple[StrKey, StrKey | None]

# ....................... #


@attrs.define(slots=True, kw_only=True)
class InProcessResilienceExecutor:
    """Process-wide singleton applying named policies.

    Bulkhead/rate-limit/budget state lives on this instance keyed by
    ``(policy_name, route)``; breaker state lives behind :attr:`breaker_store`
    (process-local by default, or a distributed store so the fleet trips
    together). The instance must be registered once via :meth:`Deps.plain` (not
    a per-scope factory), or that state would reset every request.
    """

    policies: StrKeyMapping[ResiliencePolicy] = attrs.field(
        converter=MappingConverter.to_str_key,  # type: ignore[misc]
    )
    """Named policies, keyed by policy name."""

    clock: Callable[[], float] = attrs.field(default=time.monotonic)
    """Time source for the executor."""

    rng: random.Random = attrs.field(factory=random.Random)
    """Random number generator for the executor."""

    sleep: Callable[[float], Awaitable[None]] = attrs.field(default=asyncio.sleep)
    """Sleep function for the executor."""

    breaker_store: CircuitBreakerStore = attrs.field(
        default=attrs.Factory(
            lambda self: InMemoryCircuitBreakerStore(clock=self.clock),
            takes_self=True,
        ),
    )
    """Circuit breaker store for the executor."""

    # ....................... #

    _bulkheads: dict[_StateKey, BulkheadState] = attrs.field(factory=dict, init=False)
    """Bulkhead state for the executor."""

    _rate_limits: dict[_StateKey, RateLimitState] = attrs.field(
        factory=dict,
        init=False,
    )
    """Rate limit token-bucket state for the executor."""

    _budgets: dict[_StateKey, BudgetState] = attrs.field(factory=dict, init=False)
    """Budget state for the executor."""

    _hedge_budgets: dict[_StateKey, BudgetState] = attrs.field(factory=dict, init=False)
    """Hedge budget state for the executor."""

    # ....................... #

    async def run[T](
        self,
        fn: Callable[[], Awaitable[T]],
        *,
        policy: StrKey,
        route: StrKey | None = None,
        fallback: Callable[[BaseException], Awaitable[T]] | None = None,
    ) -> T:
        """Run ``fn`` under the named ``policy``."""

        pol = self.policies.get(policy)

        if pol is None:
            raise exc.configuration(f"Unknown resilience policy {policy!r}")

        if fallback is not None and not pol.has_fallback:
            raise exc.configuration(
                f"Policy {policy!r} declares no FallbackStrategy "
                "but a fallback was provided",
            )

        try:
            return await self._apply(pol, fn, route)

        except Exception as error:  # noqa: BLE001 — terminal fallback boundary
            if fallback is not None and pol.has_fallback:
                return await fallback(error)

            raise

    # ....................... #

    async def run_hedged[T](
        self,
        fn: Callable[[], Awaitable[T]],
        *,
        policy: StrKey,
        route: StrKey | None = None,
    ) -> T:
        """Run ``fn`` with hedging: staggered concurrent attempts, first success wins."""

        pol = self.policies.get(policy)

        if pol is None:
            raise exc.configuration(f"Unknown resilience policy {policy!r}")

        hedge = pol.hedge

        if hedge is None:
            raise exc.configuration(f"Policy {policy!r} declares no HedgeStrategy")

        budget = self._hedge_budget_for(hedge, pol, route)

        if budget is not None:
            budget.on_call()

        delay = hedge.delay.total_seconds()
        tasks: set[asyncio.Future[T]] = set()
        errors: list[BaseException] = []
        attempts = 0
        budget_spent = False

        def spawn() -> None:
            nonlocal attempts
            attempts += 1
            tasks.add(asyncio.ensure_future(fn()))

            if attempts > 1:
                self._emit("hedge_attempt", pol, route)

        spawn()  # primary attempt

        try:
            while tasks:
                can_hedge = attempts < hedge.max_attempts and not budget_spent
                timeout = delay if can_hedge else None

                done, _ = await asyncio.wait(
                    tasks,
                    timeout=timeout,
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if not done:
                    # The hedge delay elapsed with no completion -> fire another copy.
                    if budget is not None and not budget.try_spend():
                        self._emit("hedge_budget_exhausted", pol, route)
                        budget_spent = True
                        continue

                    spawn()
                    continue

                for task in done:
                    tasks.discard(task)
                    error = task.exception()

                    if error is None:
                        self._emit("hedge_won", pol, route)
                        return task.result()

                    errors.append(error)

            raise errors[-1]

        finally:
            for task in tasks:
                task.cancel()

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    # ....................... #

    async def _apply[T](
        self,
        pol: ResiliencePolicy,
        fn: Callable[[], Awaitable[T]],
        route: StrKey | None,
    ) -> T:
        # Fixed composition order, innermost-out: timeout -> retry -> circuit
        # breaker -> bulkhead -> rate limit. The breaker deliberately wraps
        # *outside* retry: it admits once and records one outcome per logical
        # call, so a retry storm counts as a single breaker failure (thresholds
        # track logical calls, not attempts). The rate limit wraps outermost:
        # throttled calls are rejected before consuming a bulkhead slot or
        # registering a breaker outcome. Mirrors ResiliencePolicy's canonical
        # order.
        call: Callable[[], Awaitable[T]] = fn
        timeout = pol.timeout

        if timeout is not None:
            t, t_inner = timeout, call

            async def with_timeout() -> T:
                return await self._with_timeout(t, t_inner, pol, route)

            call = with_timeout

        retry = pol.retry

        if retry is not None:
            r, r_inner = retry, call

            async def with_retry() -> T:
                return await self._with_retry(r, r_inner, pol, route)

            call = with_retry

        breaker = pol.circuit_breaker

        if breaker is not None:
            b, b_inner = breaker, call

            async def with_breaker() -> T:
                return await self._with_breaker(b, b_inner, pol, route)

            call = with_breaker

        bulkhead = pol.bulkhead

        if bulkhead is not None:
            bh, bh_inner = bulkhead, call

            async def with_bulkhead() -> T:
                return await self._with_bulkhead(bh, bh_inner, pol, route)

            call = with_bulkhead

        rate_limit = pol.rate_limit

        if rate_limit is not None:
            rl, rl_inner = rate_limit, call

            async def with_rate_limit() -> T:
                return await self._with_rate_limit(rl, rl_inner, pol, route)

            call = with_rate_limit

        return await call()

    # ....................... #

    async def _with_timeout[T](
        self,
        strat: TimeoutStrategy,
        inner: Callable[[], Awaitable[T]],
        pol: ResiliencePolicy,
        route: StrKey | None,
    ) -> T:
        try:
            async with asyncio.timeout(strat.timeout.total_seconds()):
                return await inner()

        except TimeoutError as error:
            self._emit("timeout", pol, route)
            raise exc.infrastructure(
                f"Resilience timeout after {strat.timeout.total_seconds()}s "
                f"for policy {pol.name!r}",
            ) from error

    # ....................... #

    async def _with_retry[T](
        self,
        strat: RetryStrategy,
        inner: Callable[[], Awaitable[T]],
        pol: ResiliencePolicy,
        route: StrKey | None,
    ) -> T:
        budget = self._budget_for(strat, pol, route)

        if budget is not None:
            budget.on_call()

        attempt = 0
        prev_delay = 0.0

        while True:
            attempt += 1

            try:
                return await inner()

            except CoreException as error:
                retryable = (
                    error.kind in strat.retry_on
                    and exception_egress_policy(error.kind).retryable
                )

                if not retryable or attempt >= strat.max_attempts:
                    raise

                if budget is not None and not budget.try_spend():
                    self._emit("retry_budget_exhausted", pol, route)
                    raise

                delay = compute_delay(strat.backoff, attempt, prev_delay, self.rng)
                prev_delay = delay
                self._emit("retry_attempt", pol, route)
                await self.sleep(delay)

    # ....................... #

    async def _with_breaker[T](
        self,
        strat: CircuitBreakerStrategy,
        inner: Callable[[], Awaitable[T]],
        pol: ResiliencePolicy,
        route: StrKey | None,
    ) -> T:
        key = (pol.name, route)
        allowed, transition = await self.breaker_store.admit(key, strat)

        if transition == "half_open":
            self._emit("breaker_half_open", pol, route)

        if not allowed:
            self._emit("breaker_open", pol, route)
            raise exc.infrastructure(f"Circuit breaker open for policy {pol.name!r}")

        try:
            result = await inner()

        except CoreException as error:
            ok = not exception_egress_policy(error.kind).retryable
            self._breaker_outcome(
                await self.breaker_store.record(key, strat, ok), pol, route
            )
            raise

        except Exception:
            self._breaker_outcome(
                await self.breaker_store.record(key, strat, False), pol, route
            )
            raise

        self._breaker_outcome(
            await self.breaker_store.record(key, strat, True), pol, route
        )
        return result

    # ....................... #

    async def _with_rate_limit[T](
        self,
        strat: RateLimitStrategy,
        inner: Callable[[], Awaitable[T]],
        pol: ResiliencePolicy,
        route: StrKey | None,
    ) -> T:
        state = self._rate_limit_for(strat, pol, route)

        if not state.try_acquire(self.clock()):
            self._emit("rate_limit_reject", pol, route)
            raise exc.throttled(
                f"Rate limit exceeded for policy {pol.name!r}",
                code="rate_limited",
                details={
                    "policy": str(pol.name),
                    "route": str(route) if route is not None else None,
                },
            )

        return await inner()

    # ....................... #

    async def _with_bulkhead[T](
        self,
        strat: BulkheadStrategy,
        inner: Callable[[], Awaitable[T]],
        pol: ResiliencePolicy,
        route: StrKey | None,
    ) -> T:
        state = self._bulkhead_for(strat, pol, route)

        if not state.can_admit():
            self._emit("bulkhead_reject", pol, route)
            raise exc.infrastructure(f"Bulkhead full for policy {pol.name!r}")

        state.waiting += 1

        try:
            await state.sem.acquire()

        finally:
            state.waiting -= 1

        try:
            return await inner()

        finally:
            state.sem.release()

    # ....................... #

    def _breaker_outcome(
        self,
        transition: Transition,
        pol: ResiliencePolicy,
        route: StrKey | None,
    ) -> None:
        if transition == "open":
            self._emit("breaker_open", pol, route)

        elif transition == "closed":
            self._emit("breaker_close", pol, route)

    # ....................... #

    def _rate_limit_for(
        self,
        strat: RateLimitStrategy,
        pol: ResiliencePolicy,
        route: StrKey | None,
    ) -> RateLimitState:
        key = (pol.name, route)
        state = self._rate_limits.get(key)

        if state is None:
            state = RateLimitState(
                rate=strat.permits / strat.per.total_seconds(),
                capacity=float(strat.capacity),
                updated_at=self.clock(),
            )
            self._rate_limits[key] = state

        return state

    # ....................... #

    def _bulkhead_for(
        self,
        strat: BulkheadStrategy,
        pol: ResiliencePolicy,
        route: StrKey | None,
    ) -> BulkheadState:
        key = (pol.name, route)
        state = self._bulkheads.get(key)

        if state is None:
            state = BulkheadState(
                max_concurrency=strat.max_concurrency,
                max_queue=strat.max_queue,
            )
            self._bulkheads[key] = state

        return state

    # ....................... #

    def _budget_for(
        self,
        strat: RetryStrategy,
        pol: ResiliencePolicy,
        route: StrKey | None,
    ) -> BudgetState | None:
        if strat.budget is None:
            return None

        key = (pol.name, route)
        state = self._budgets.get(key)

        if state is None:
            state = BudgetState(
                ratio=strat.budget.ratio,
                min_throughput=strat.budget.min_throughput,
            )
            self._budgets[key] = state

        return state

    # ....................... #

    def _hedge_budget_for(
        self,
        strat: HedgeStrategy,
        pol: ResiliencePolicy,
        route: StrKey | None,
    ) -> BudgetState | None:
        if strat.budget is None:
            return None

        key = (pol.name, route)
        state = self._hedge_budgets.get(key)

        if state is None:
            state = BudgetState(
                ratio=strat.budget.ratio,
                min_throughput=strat.budget.min_throughput,
            )
            self._hedge_budgets[key] = state

        return state

    # ....................... #

    def _emit(self, op: str, pol: ResiliencePolicy, route: StrKey | None) -> None:
        record(
            domain="resilience",
            op=op,
            surface="resilience_executor",
            route=str(route) if route is not None else None,
            phase=str(pol.name),
        )
