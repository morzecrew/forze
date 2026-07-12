"""In-process resilience executor composing strategies into a call pipeline."""

import asyncio
from collections.abc import Awaitable, Callable, Iterator, Mapping

import attrs

from forze.application.contracts.resilience import (
    AdaptiveBulkheadStrategy,
    AdaptiveThrottleStrategy,
    BulkheadStrategy,
    CircuitBreakerStore,
    CircuitBreakerStrategy,
    GradientBulkheadStrategy,
    HedgeStrategy,
    LatencyDigestStore,
    RateLimitStore,
    RateLimitStrategy,
    ResiliencePolicy,
    ResilienceStateSnapshot,
    RetryStrategy,
    TimeoutStrategy,
    Transition,
)
from forze.base.exceptions import (
    CoreException,
    ExceptionKind,
    exc,
    exception_egress_policy,
)
from forze.base.primitives import (
    BoundedLruMap,
    MappingConverter,
    StrKey,
    StrKeyMapping,
    current_entropy_source,
    monotonic,
)

from ..context.active_operation import continue_operation_on_task
from ..context.deadline import remaining_time
from ..tracing import record
from .backoff import compute_delay
from .limiter import Gradient2Limiter
from .state import (
    AdaptiveBulkheadState,
    AdaptiveThrottleState,
    BudgetState,
    HedgeDelayState,
)
from .store import (
    DEFAULT_MAX_STATE_ENTRIES,
    InMemoryCircuitBreakerStore,
    InMemoryLatencyDigestStore,
    InMemoryRateLimitStore,
)

# ----------------------- #

_StateKey = tuple[StrKey, StrKey | None]


def _bulkhead_idle(state: AdaptiveBulkheadState) -> bool:
    """A bulkhead is safe to evict only when it holds no permits and has no waiters.

    Evicting a live bulkhead would recreate it empty on the next access, so its in-flight
    calls go uncounted and concurrency control is reset — over-admitting past the limit.
    """

    return state.in_use == 0 and state.waiting == 0


MetricsSink = Callable[[str, str, str | None], None]
"""Callback receiving every resilience event as ``(event, policy, route)``.

Unlike the tracing emitter, the sink is **not** gated behind tracing — attach
one (e.g. via ``instrument_resilience``) to export breaker transitions and
rejection counts as always-on metrics in production.
"""

# ....................... #

_BREAKER_FAILURE_KINDS = frozenset(
    {ExceptionKind.INFRASTRUCTURE, ExceptionKind.TIMEOUT}
)
"""Kinds that mean the downstream is unreachable / unresponsive — a breaker *failure*."""

_BREAKER_NEUTRAL_KINDS = frozenset(
    {ExceptionKind.THROTTLED, ExceptionKind.CONCURRENCY}
)
"""Kinds the breaker ignores: throttling is backpressure and concurrency is contention —
neither is a downstream *health* signal, so they are recorded as neither success nor failure."""


def _classify_breaker_outcome(kind: ExceptionKind) -> bool | None:
    """Classify a failed call for the circuit breaker by downstream **health**.

    The breaker opens on an unhealthy *downstream*, which is a different question from
    whether the error is retryable. Returns ``False`` for a failure (infrastructure /
    timeout), ``None`` to not record the call at all (throttling / concurrency — backpressure
    and contention, not health), and ``True`` for a success (any other error is caller-caused;
    the downstream answered fine). Using retryability instead would wrongly trip the breaker
    open on a downstream's ``429`` and count a timeout as a success.
    """

    if kind in _BREAKER_FAILURE_KINDS:
        return False

    if kind in _BREAKER_NEUTRAL_KINDS:
        return None

    return True


# ....................... #

_AMBIGUOUS_RETRY_KINDS = frozenset(
    {ExceptionKind.INFRASTRUCTURE, ExceptionKind.TIMEOUT}
)
"""Retry-triggering kinds whose outcome is *ambiguous* — an infrastructure error or a
per-attempt timeout can leave a write applied-but-unacknowledged, so retrying may duplicate
it. Concurrency conflicts and throttles are unambiguous (the call did not take effect), so
they are safe to retry on any method."""


def reject_blanket_ambiguous_retry(
    policy: ResiliencePolicy,
    key_name: str,
) -> None:
    """Refuse *policy* when applying it to **every** method of the port at *key_name* is unsafe.

    A retrying policy bound to a whole port (``methods=None``) retries writes too, and retrying
    an ambiguous failure can duplicate a non-idempotent write — so the author must opt in per
    method. Enforced at wiring time (the deps module) and again on every
    :meth:`InProcessResilienceExecutor.retune`, so a hot-swap cannot re-enable the hazard the
    wiring gate refused.
    """

    retry = policy.retry

    if retry is None:
        return

    if ambiguous := sorted(
        kind.value for kind in retry.retry_on & _AMBIGUOUS_RETRY_KINDS
    ):
        raise exc.configuration(
            f"Port policy for {key_name!r} applies retrying policy "
            f"{str(policy.name)!r} (retries {ambiguous}) to every method: this would "
            "retry a non-idempotent write on an ambiguous failure and risk "
            "duplicating it. Declare an explicit `methods` list of the operations "
            "that are safe to retry.",
            code="resilience.blanket_write_retry",
        )


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

    clock: Callable[[], float] = monotonic
    """Time source for the executor."""

    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep
    """Sleep function for the executor."""

    breaker_store: CircuitBreakerStore = attrs.Factory(
        lambda self: InMemoryCircuitBreakerStore(clock=self.clock),
        takes_self=True,
    )
    """Circuit breaker store for the executor."""

    rate_limit_store: RateLimitStore = attrs.Factory(
        lambda self: InMemoryRateLimitStore(clock=self.clock),
        takes_self=True,
    )
    """Rate-limit token-bucket store (process-local by default, or a distributed
    store so ``permits/per`` is the fleet's rate instead of per-replica)."""

    latency_digest_store: LatencyDigestStore = attrs.field(
        factory=InMemoryLatencyDigestStore,
    )
    """Adaptive-bulkhead latency-quantile digest (process-local by default, or a
    distributed store so the AIMD congestion signal reflects the fleet's latency
    instead of one replica's). Only consulted when a policy sets
    ``AdaptiveBulkheadStrategy.latency_quantile``."""

    max_state_entries: int = DEFAULT_MAX_STATE_ENTRIES
    """LRU cap on this instance's per-``(policy, route)`` state maps — bounds memory when
    ``route`` is high-cardinality (per-tenant, per-object). A budget/throttle/hedge entry is
    safe to drop (recreated fresh on next access); the bulkhead maps evict **only idle**
    entries (no permits held, no waiters), so eviction never resets live concurrency control.
    The breaker and rate-limit *stores* have their own caps."""

    blanket_policy_bindings: Mapping[StrKey, tuple[str, ...]] = attrs.field(
        factory=dict[StrKey, tuple[str, ...]],
    )
    """Dependency-key names bound to each policy with no ``methods`` narrowing (the whole
    port), keyed by policy name. :meth:`retune` re-runs the wiring-time ambiguous-retry gate
    against these bindings before swapping a policy in."""

    # ....................... #

    _bulkheads: BoundedLruMap[_StateKey, AdaptiveBulkheadState] = attrs.field(
        init=False,
        default=attrs.Factory(
            lambda self: BoundedLruMap[_StateKey, AdaptiveBulkheadState](
                self.max_state_entries, evictable=_bulkhead_idle
            ),
            takes_self=True,
        ),
    )
    """Fixed bulkhead state: the unified admission machinery with a constant
    limit (the AIMD controller is simply never consulted)."""

    _adaptive_bulkheads: BoundedLruMap[_StateKey, AdaptiveBulkheadState] = attrs.field(
        init=False,
        default=attrs.Factory(
            lambda self: BoundedLruMap[_StateKey, AdaptiveBulkheadState](
                self.max_state_entries, evictable=_bulkhead_idle
            ),
            takes_self=True,
        ),
    )
    """Adaptive (AIMD) bulkhead state for the executor."""

    _gradient_bulkheads: BoundedLruMap[_StateKey, AdaptiveBulkheadState] = attrs.field(
        init=False,
        default=attrs.Factory(
            lambda self: BoundedLruMap[_StateKey, AdaptiveBulkheadState](
                self.max_state_entries, evictable=_bulkhead_idle
            ),
            takes_self=True,
        ),
    )
    """Delay-based (Gradient2) bulkhead state for the executor."""

    _budgets: BoundedLruMap[_StateKey, BudgetState] = attrs.field(
        init=False,
        default=attrs.Factory(
            lambda self: BoundedLruMap[_StateKey, BudgetState](self.max_state_entries),
            takes_self=True,
        ),
    )
    """Budget state for the executor."""

    _hedge_budgets: BoundedLruMap[_StateKey, BudgetState] = attrs.field(
        init=False,
        default=attrs.Factory(
            lambda self: BoundedLruMap[_StateKey, BudgetState](self.max_state_entries),
            takes_self=True,
        ),
    )
    """Hedge budget state for the executor."""

    _throttles: BoundedLruMap[_StateKey, AdaptiveThrottleState] = attrs.field(
        init=False,
        default=attrs.Factory(
            lambda self: BoundedLruMap[_StateKey, AdaptiveThrottleState](
                self.max_state_entries
            ),
            takes_self=True,
        ),
    )
    """Adaptive client-throttle counters (requests/accepts per policy/route)."""

    _hedge_delays: BoundedLruMap[_StateKey, HedgeDelayState] = attrs.field(
        init=False,
        default=attrs.Factory(
            lambda self: BoundedLruMap[_StateKey, HedgeDelayState](
                self.max_state_entries
            ),
            takes_self=True,
        ),
    )
    """Adaptive hedge-delay state (windowed P² quantile per policy/route)."""

    _metrics_sink: MetricsSink | None = attrs.field(
        default=None,
        init=False,
        repr=False,
    )
    """Optional always-on metrics callback (see :data:`MetricsSink`)."""

    _forced_open: set[_StateKey] = attrs.field(
        factory=set,
        init=False,
        repr=False,
    )
    """``(policy, route)`` keys an operator has force-opened via the admin port — a manual
    kill-switch that rejects every call under the key until cleared (see :meth:`force_open`).
    A ``route=None`` entry is a policy-wide wildcard matching every route under the policy."""

    # ....................... #

    def set_metrics_sink(self, sink: MetricsSink | None) -> None:
        """Attach (or detach with ``None``) the always-on metrics sink.

        Called once at assembly time (``instrument_resilience``); the sink
        receives every resilience event regardless of the tracing gate.
        """

        self._metrics_sink = sink

    # ....................... #

    def bulkhead_queue_depths(self) -> Iterator[tuple[str, str | None, int]]:
        """Yield ``(policy, route, waiting)`` for every bulkhead with live state.

        Snapshot accessor for observable gauges: ``waiting`` is the number of
        calls queued behind the semaphore right now. State appears lazily on
        first use of a bulkhead-bearing policy.
        """

        for (policy, route), state in self._bulkheads.items():
            yield (
                str(policy),
                str(route) if route is not None else None,
                state.waiting,
            )

        for (policy, route), adaptive in self._adaptive_bulkheads.items():
            yield (
                str(policy),
                str(route) if route is not None else None,
                adaptive.waiting,
            )

        for (policy, route), gradient in self._gradient_bulkheads.items():
            yield (
                str(policy),
                str(route) if route is not None else None,
                gradient.waiting,
            )

    # ....................... #

    def adaptive_bulkhead_limits(self) -> Iterator[tuple[str, str | None, float]]:
        """Yield ``(policy, route, limit)`` for every dynamic bulkhead with live state.

        Snapshot accessor for observable gauges: the current AIMD or Gradient2
        concurrency limit. State appears lazily on first use of the policy.
        """

        for (policy, route), state in (
            *self._adaptive_bulkheads.items(),
            *self._gradient_bulkheads.items(),
        ):
            yield (
                str(policy),
                str(route) if route is not None else None,
                state.limit,
            )

    # ....................... #

    def hedge_delays(self) -> Iterator[tuple[str, str | None, float]]:
        """Yield ``(policy, route, delay_seconds)`` for every adaptive hedge delay.

        Snapshot accessor for observable gauges: the effective hedge delay —
        the windowed P² quantile estimate clamped by the strategy's
        floor/cap, or the fixed fallback until warmed up. State appears
        lazily on first ``run_hedged`` of an adaptive-delay policy.
        """

        for (policy, route), state in self._hedge_delays.items():
            yield (
                str(policy),
                str(route) if route is not None else None,
                state.delay(),
            )

    # ....................... #
    # Admin / control plane — implements ResilienceAdminPort over this singleton.

    async def inspect(
        self,
        *,
        policy: StrKey | None = None,
    ) -> list[ResilienceStateSnapshot]:
        """Snapshot the live, executor-owned resilience state (optionally filtered to one policy)."""

        def _match(key: _StateKey) -> bool:
            return policy is None or key[0] == policy

        # A policy uses at most one bulkhead kind, so a key appears in at most one bulkhead map.
        bulkheads: dict[_StateKey, AdaptiveBulkheadState] = {}
        for pool in (
            self._bulkheads,
            self._adaptive_bulkheads,
            self._gradient_bulkheads,
        ):
            for key, state in pool.items():
                bulkheads[key] = state

        hedges = dict(self._hedge_delays.items())

        keys: set[_StateKey] = set()
        keys.update(k for k in bulkheads if _match(k))
        keys.update(k for k in hedges if _match(k))
        keys.update(k for k in self._forced_open if _match(k))

        snapshots: list[ResilienceStateSnapshot] = []

        for key in sorted(
            keys, key=lambda k: (str(k[0]), str(k[1]) if k[1] is not None else "")
        ):
            name, route = key
            bulkhead = bulkheads.get(key)
            hedge = hedges.get(key)
            snapshots.append(
                ResilienceStateSnapshot(
                    policy=str(name),
                    route=str(route) if route is not None else None,
                    forced_open=self._is_forced_open(name, route),
                    concurrency_limit=bulkhead.limit if bulkhead is not None else None,
                    in_use=bulkhead.in_use if bulkhead is not None else None,
                    waiting=bulkhead.waiting if bulkhead is not None else None,
                    hedge_delay=hedge.delay() if hedge is not None else None,
                )
            )

        return snapshots

    # ....................... #

    async def force_open(
        self,
        policy: StrKey,
        route: StrKey | None = None,
    ) -> None:
        """Trip the ``(policy, route)`` breaker open by hand — a manual kill-switch. Idempotent.

        ``route=None`` is a policy-wide wildcard: it sheds **every** route under *policy*.
        Port policies key their state by the resolved route (typically the spec name), so an
        operator force-opening a whole policy must not have to enumerate them.
        """

        self._forced_open.add((policy, route))

    # ....................... #

    async def clear_forced_open(
        self,
        policy: StrKey,
        route: StrKey | None = None,
    ) -> None:
        """Release a :meth:`force_open` kill-switch for ``(policy, route)``. Idempotent.

        ``route=None`` mirrors the wildcard on :meth:`force_open`: it releases the policy-wide
        switch **and** every route-scoped switch under *policy*.
        """

        if route is None:
            self._forced_open -= {k for k in self._forced_open if k[0] == policy}
            return

        self._forced_open.discard((policy, route))

    # ....................... #

    async def retune(self, policy: ResiliencePolicy) -> None:
        """Hot-swap a policy's parameters by name and rebuild its adaptive state on the next call.

        Re-runs the wiring-time blanket-retry gate first: if the retuned policy would retry an
        ambiguous failure on a port it is bound to with no ``methods`` narrowing, the retune is
        rejected and the current policy stays in place.
        """

        for key_name in self.blanket_policy_bindings.get(policy.name, ()):
            reject_blanket_ambiguous_retry(policy, key_name)

        self.policies = {**self.policies, policy.name: policy}
        self._evict_policy_state(policy.name)

    # ....................... #

    def _evict_policy_state(self, name: StrKey) -> None:
        """Drop the executor's cached ``(name, *)`` adaptive state so it rebuilds with the new
        parameters. Calls already in flight keep the state object they captured at start (it stays
        alive through their coroutines), so they drain safely and are never stranded."""

        for pool in (
            self._bulkheads,
            self._adaptive_bulkheads,
            self._gradient_bulkheads,
            self._budgets,
            self._hedge_budgets,
            self._throttles,
            self._hedge_delays,
        ):
            for key in [k for k in pool if k[0] == name]:
                del pool[key]

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

        self._reject_if_forced_open(pol, route)

        budget = self._hedge_budget_for(hedge, pol, route)

        if budget is not None:
            budget.on_call()

        delay_state = self._hedge_delay_for(hedge, pol, route)

        delay = (
            delay_state.delay()
            if delay_state is not None
            else (hedge.delay.total_seconds())
        )

        tasks: set[asyncio.Future[T]] = set()
        errors: list[BaseException] = []
        attempts = 0
        budget_spent = False

        def spawn() -> asyncio.Future[T]:
            nonlocal attempts
            attempts += 1
            # Each attempt task is an engine-internal continuation of the
            # operation awaiting run_hedged (awaited or cancelled in the finally
            # below): adopt the operation onto it so a dispatch the attempt makes
            # rides the admitted drain slot instead of being re-admitted.
            task = asyncio.ensure_future(continue_operation_on_task(fn()))
            tasks.add(task)

            if attempts > 1:
                self._emit("hedge_attempt", pol, route)

            return task

        # The estimator samples the *primary* attempt only: a completed primary
        # is an unbiased latency sample, and hedge attempts are excluded so a
        # hedged call doesn't double-weight one logical request. Sampling
        # winners-of-races instead would bias the quantile down, making the
        # delay ever more eager (the classic hedging feedback spiral).

        primary_start = self.clock()
        primary = spawn()
        hedge_won = False

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

                    if task is primary and delay_state is not None and error is None:
                        delay_state.observe(self.clock() - primary_start)

                    if error is None:
                        # ``hedge_won`` counts only the calls a hedge actually
                        # rescued; ``hedge_primary_won`` counts a primary that won a
                        # race a hedge had ALSO entered. Both are emitted only when a
                        # hedge was actually fired (``attempts > 1``), so their sum is
                        # "hedged completions" and the effectiveness ratio
                        # (hedge_won / (hedge_won + hedge_primary_won)) is meaningful.
                        # A fast primary that beat the delay (no hedge fired) emits
                        # neither — it never entered the hedge population.
                        if task is not primary:
                            hedge_won = True
                            self._emit("hedge_won", pol, route)

                        elif attempts > 1:
                            self._emit("hedge_primary_won", pol, route)

                        return task.result()

                    errors.append(error)

            raise errors[-1]

        finally:
            if hedge_won and primary in tasks and delay_state is not None:
                # A hedge won and the primary is being cancelled: record its
                # elapsed time as a right-censored sample. It understates the
                # true latency but is >= the current delay, so it still pulls
                # the estimated tail up instead of silently dropping the
                # slowest calls from the distribution. Guarded by `hedge_won`:
                # a *caller* cancellation can land at any elapsed time, and
                # recording it would feed arbitrary garbage into the quantile.

                delay_state.observe(self.clock() - primary_start)

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
        self._reject_if_forced_open(pol, route)

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

        throttle = pol.adaptive_throttle

        if throttle is not None:
            th, th_inner = throttle, call

            async def with_throttle() -> T:
                return await self._with_adaptive_throttle(th, th_inner, pol, route)

            call = with_throttle

        bulkhead = pol.bulkhead

        if bulkhead is not None:
            bh, bh_inner = bulkhead, call

            async def with_bulkhead() -> T:
                return await self._with_bulkhead(bh, bh_inner, pol, route)

            call = with_bulkhead

        adaptive = pol.adaptive_bulkhead

        if adaptive is not None:
            ab, ab_inner = adaptive, call

            async def with_adaptive_bulkhead() -> T:
                return await self._with_adaptive_bulkhead(ab, ab_inner, pol, route)

            call = with_adaptive_bulkhead

        gradient = pol.gradient_bulkhead

        if gradient is not None:
            gb, gb_inner = gradient, call

            async def with_gradient_bulkhead() -> T:
                return await self._with_gradient_bulkhead(gb, gb_inner, pol, route)

            call = with_gradient_bulkhead

        rate_limit = pol.rate_limit

        if rate_limit is not None:
            rl, rl_inner = rate_limit, call

            async def with_rate_limit() -> T:
                return await self._with_rate_limit(rl, rl_inner, pol, route)

            call = with_rate_limit

        # An invocation deadline (see ``context.deadline``) bounds the whole
        # strategy chain from the outside: retries, breaker admission, bulkhead
        # queueing, and rate-limit rejection all share the remaining budget.
        # Raises non-retryable TIMEOUT — distinct from the per-attempt
        # TimeoutStrategy, which raises retryable INFRASTRUCTURE.

        remaining = remaining_time()

        if remaining is not None:
            if remaining <= 0.0:
                self._emit("deadline_exceeded", pol, route)
                raise exc.timeout(
                    f"Invocation deadline exceeded before call "
                    f"under policy {pol.name!r}",
                    code="deadline_exceeded",
                )

            d_inner = call

            async def with_deadline() -> T:
                try:
                    async with asyncio.timeout(remaining):
                        return await d_inner()

                except TimeoutError as error:
                    self._emit("deadline_exceeded", pol, route)
                    raise exc.timeout(
                        f"Invocation deadline exceeded during call "
                        f"under policy {pol.name!r}",
                        code="deadline_exceeded",
                    ) from error

            call = with_deadline

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

                delay = compute_delay(
                    strat.backoff,
                    attempt,
                    prev_delay,
                    current_entropy_source().as_random(),
                )

                # Deadline-aware retry: when the backoff sleep would outlive
                # the invocation deadline, surface the real error now instead
                # of sleeping into a guaranteed deadline timeout.

                deadline_left = remaining_time()

                if deadline_left is not None and delay >= deadline_left:
                    self._emit("retry_deadline_exhausted", pol, route)
                    raise

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

        try:
            allowed, transition = await self.breaker_store.admit(key, strat)

        except Exception as error:  # noqa: BLE001 — store-outage fail-open boundary
            # Store unreachable (e.g. a distributed breaker's Redis is down):
            # fail open by default so the breaker can't become the outage, or
            # fail closed if the policy demands it.
            self._on_store_unavailable("breaker_store_error", pol, route, error)
            allowed, transition = True, None

        if transition == "half_open":
            self._emit("breaker_half_open", pol, route)

        if not allowed:
            self._emit("breaker_open", pol, route)
            raise exc.infrastructure(f"Circuit breaker open for policy {pol.name!r}")

        try:
            result = await inner()

        except CoreException as error:
            # Classify by downstream health, not retryability: a throttle (429) or a
            # concurrency conflict is not a breaker failure, and a timeout is not a success.
            ok = _classify_breaker_outcome(error.kind)

            if ok is not None:
                await self._record_breaker_outcome(key, strat, ok, pol, route)

            raise

        except Exception:
            await self._record_breaker_outcome(key, strat, False, pol, route)

            raise

        await self._record_breaker_outcome(key, strat, True, pol, route)

        return result

    # ....................... #

    async def _with_adaptive_throttle[T](
        self,
        strat: AdaptiveThrottleStrategy,
        inner: Callable[[], Awaitable[T]],
        pol: ResiliencePolicy,
        route: StrKey | None,
    ) -> T:
        state = self._throttle_for(strat, pol, route)
        now = self.clock()
        probability = state.reject_probability(now)

        # Shed calls count as requests but not accepts — the algorithm's
        # self-limiting property (the client converges on ~k× the downstream's
        # current capacity instead of hammering it with full traffic). The shed
        # roll (and the backoff jitter above) draw from the replayable entropy
        # seam, so a simulation's seeded source makes them reproducible.
        if probability > 0.0 and current_entropy_source().random() < probability:
            state.record_request(now)
            self._emit("throttle_reject", pol, route)

            raise exc.throttled(
                f"Adaptive throttle shedding for policy {pol.name!r}",
                code="adaptive_throttle",
            )

        state.record_request(now)

        try:
            result = await inner()

        except CoreException as error:
            # Same outcome classification as the breaker, inverted: a
            # non-retryable failure is the downstream doing its job (a domain
            # rejection), not buckling — it counts as an accept.
            if not exception_egress_policy(error.kind).retryable:
                state.record_accept(self.clock())

            raise

        state.record_accept(self.clock())
        return result

    # ....................... #

    async def _with_rate_limit[T](
        self,
        strat: RateLimitStrategy,
        inner: Callable[[], Awaitable[T]],
        pol: ResiliencePolicy,
        route: StrKey | None,
    ) -> T:
        try:
            acquired = await self.rate_limit_store.try_acquire((pol.name, route), strat)

        except Exception as error:  # noqa: BLE001 — store-outage fail-open boundary
            # Store unreachable: fail open by default (don't let a down limiter
            # store shed live traffic), or fail closed if the policy demands it.
            self._on_store_unavailable("rate_limit_store_error", pol, route, error)
            acquired = True

        if not acquired:
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

        await self._admit(state, pol, route)

        try:
            return await inner()

        finally:
            state.release()
            # Reclaim any idle overshoot the LRU kept while every entry was live (oldest
            # first, so this just-used state — now newest — survives).
            self._bulkheads.prune()

    # ....................... #

    async def _admit(
        self,
        state: AdaptiveBulkheadState,
        pol: ResiliencePolicy,
        route: StrKey | None,
    ) -> None:
        """Acquire a bulkhead slot, surfacing CoDel sheds as resilience events."""

        try:
            await state.acquire()

        except CoreException as error:
            if error.code == "bulkhead_queue_shed":
                self._emit("bulkhead_shed", pol, route)

            raise

    # ....................... #

    async def _with_adaptive_bulkhead[T](
        self,
        strat: AdaptiveBulkheadStrategy,
        inner: Callable[[], Awaitable[T]],
        pol: ResiliencePolicy,
        route: StrKey | None,
    ) -> T:
        state = self._adaptive_bulkhead_for(strat, pol, route)

        if not state.can_admit():
            self._emit("bulkhead_reject", pol, route)
            raise exc.infrastructure(f"Bulkhead full for policy {pol.name!r}")

        await self._admit(state, pol, route)
        start = self.clock()

        try:
            result = await inner()

        except asyncio.CancelledError:
            # A cancellation is not a downstream-latency signal, and awaiting the
            # digest store while unwinding a cancellation risks re-interruption —
            # just release the slot and propagate.
            state.release()
            raise

        except BaseException:
            state.release()
            elapsed = self.clock() - start

            # Latency-only congestion signal: a failure adjusts the limit only
            # when it ALSO breached the threshold (a per-attempt timeout firing
            # is a breach at the timeout value); fast failures are the circuit
            # breaker's job and leave the limit untouched.
            if elapsed > state.latency_threshold:
                await self._adaptive_on_complete(state, strat, pol, route, elapsed)

            raise

        state.release()
        elapsed = self.clock() - start

        # A zero-duration completion (clock resolution / no advance) carries no
        # latency signal; the failure path is already threshold-gated above.
        if elapsed > 0.0:
            await self._adaptive_on_complete(state, strat, pol, route, elapsed)

        # Reclaim idle overshoot after the AIMD update (oldest first; this state is newest).
        self._adaptive_bulkheads.prune()
        return result

    # ....................... #

    async def _adaptive_on_complete(
        self,
        state: AdaptiveBulkheadState,
        strat: AdaptiveBulkheadStrategy,
        pol: ResiliencePolicy,
        route: StrKey | None,
        elapsed: float,
    ) -> None:
        """Feed a completed call into the AIMD controller.

        In quantile mode the congestion signal comes from the latency digest
        store (process-local or fleet-shared); a backoff opens a fresh epoch.

        Feeding the controller is bookkeeping: like the breaker's outcome recording,
        a digest-store error must never turn a successful call into a failure nor mask
        an in-flight domain error on the failure path, so a store failure is swallowed
        and surfaced as a metric only (the sample is simply dropped from the controller).
        """

        quantile_value: float | None = None

        if strat.latency_quantile is not None:
            key = (pol.name, route)

            try:
                quantile_value = await self.latency_digest_store.observe(
                    key, elapsed, strat
                )

            except Exception:  # noqa: BLE001 — store-outage fail-open boundary
                self._emit("latency_digest_store_error", pol, route)
                return

        if self._controller_on_complete(
            state, pol, route, latency=elapsed, quantile_value=quantile_value
        ):
            self._emit("bulkhead_backoff", pol, route)

            if strat.latency_quantile is not None:
                try:
                    await self.latency_digest_store.reset((pol.name, route), strat)

                except Exception:  # noqa: BLE001 — store-outage fail-open boundary
                    self._emit("latency_digest_store_error", pol, route)

    # ....................... #

    async def _with_gradient_bulkhead[T](
        self,
        strat: GradientBulkheadStrategy,
        inner: Callable[[], Awaitable[T]],
        pol: ResiliencePolicy,
        route: StrKey | None,
    ) -> T:
        state = self._gradient_bulkhead_for(strat, pol, route)

        if not state.can_admit():
            self._emit("bulkhead_reject", pol, route)
            raise exc.infrastructure(f"Bulkhead full for policy {pol.name!r}")

        await self._admit(state, pol, route)
        start = self.clock()

        try:
            result = await inner()

        except BaseException:
            # Gradient feeds *successful* completions only: a failure (fast or
            # slow) is the circuit breaker's job and leaves the limit untouched.
            state.release()
            raise

        # Take the in-flight count before release for an accurate no-load guard.
        inflight = state.in_use
        state.release()
        elapsed = self.clock() - start

        # A zero-duration completion (clock resolution / no advance) carries no
        # latency signal — don't feed the gradient controller a non-positive rtt.
        if elapsed > 0.0 and self._controller_on_complete(
            state, pol, route, latency=elapsed, inflight=inflight
        ):
            self._emit("bulkhead_backoff", pol, route)

        # Reclaim idle overshoot after the gradient update (oldest first; this state is newest).
        self._gradient_bulkheads.prune()
        return result

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

    def _on_store_unavailable(
        self,
        event: str,
        pol: ResiliencePolicy,
        route: StrKey | None,
        error: Exception,
    ) -> None:
        """Handle a breaker/rate-limit *store* failure on the admission path.

        Emits *event* as an always-on metric, then honours the policy's
        :attr:`ResiliencePolicy.fail_open_on_store_error`: returns (the caller
        admits the call — the resilience layer never becomes the outage) when
        failing open, or raises a retryable ``infrastructure`` error when failing
        closed. A store outage is never allowed to surface as an opaque 500.
        """

        self._emit(event, pol, route)

        if not pol.fail_open_on_store_error:
            raise exc.infrastructure(
                f"Resilience store unavailable for policy {pol.name!r}",
                code="resilience_store_unavailable",
            ) from error

    # ....................... #

    def _is_forced_open(
        self,
        policy: StrKey,
        route: StrKey | None,
    ) -> bool:
        """Whether a kill-switch covers ``(policy, route)`` — its own key or the policy-wide
        ``route=None`` wildcard. The single lookup shared by enforcement and :meth:`inspect`,
        so what is reported forced-open is exactly what is enforced."""

        forced = self._forced_open
        return (policy, route) in forced or (policy, None) in forced

    # ....................... #

    def _reject_if_forced_open(
        self,
        pol: ResiliencePolicy,
        route: StrKey | None,
    ) -> None:
        """Reject the call if an operator force-opened this ``(policy, route)``.

        The admin kill-switch: it short-circuits before any strategy runs (so it works even for a
        policy with no circuit breaker) and raises the same retryable ``infrastructure`` error an
        open breaker does, so callers already tolerant of a tripped breaker need no new handling.
        """

        if self._is_forced_open(pol.name, route):
            self._emit("breaker_forced_open", pol, route)
            raise exc.infrastructure(
                f"Resilience policy {pol.name!r} force-opened by operator",
                code="resilience_forced_open",
            )

    # ....................... #

    def _controller_on_complete(
        self,
        state: AdaptiveBulkheadState,
        pol: ResiliencePolicy,
        route: StrKey | None,
        *,
        latency: float,
        quantile_value: float | None = None,
        inflight: int | None = None,
    ) -> bool:
        """Fold a completed call into the concurrency controller, swallowing a bookkeeping error.

        The AIMD / Gradient2 limit update is post-completion bookkeeping, the same category as the
        breaker's outcome recording and the digest store's ``observe``/``reset``: an exception here
        (e.g. a controller that rejects a degenerate sample) must never turn a completed call into a
        failure nor mask an in-flight domain error on the failure path. It is swallowed and surfaced
        as a metric; the sample is dropped. Returns whether the controller backed off the limit.
        """

        try:
            return state.on_complete(latency, self.clock(), quantile_value, inflight)

        except Exception:  # noqa: BLE001 — controller bookkeeping must never fail a completed call
            self._emit("bulkhead_controller_error", pol, route)
            return False

    # ....................... #

    async def _record_breaker_outcome(
        self,
        key: _StateKey,
        strat: CircuitBreakerStrategy,
        ok: bool,
        pol: ResiliencePolicy,
        route: StrKey | None,
    ) -> None:
        """Record a breaker outcome, swallowing a store failure.

        Recording is bookkeeping: a store error here must never turn a
        successful call into a failure, nor mask the in-flight domain error on
        the failure paths. The failure is surfaced as a metric only.
        """

        try:
            transition = await self.breaker_store.record(key, strat, ok)

        except Exception:
            self._emit("breaker_store_error", pol, route)
            return

        self._breaker_outcome(transition, pol, route)

    # ....................... #

    def _bulkhead_for(
        self,
        strat: BulkheadStrategy,
        pol: ResiliencePolicy,
        route: StrKey | None,
    ) -> AdaptiveBulkheadState:
        key = (pol.name, route)
        state = self._bulkheads.get(key)

        if state is None:
            # Unified admission with a constant limit: the AIMD controller
            # fields are inert because the fixed path never calls on_complete.
            state = AdaptiveBulkheadState(
                latency_threshold=float("inf"),
                min_concurrency=strat.max_concurrency,
                max_concurrency=strat.max_concurrency,
                max_queue=strat.max_queue,
                backoff_ratio=0.5,
                increase_step=1.0,
                cooldown=0.0,
                clock=self.clock,
                queue_target_s=(
                    strat.queue_target.total_seconds()
                    if strat.queue_target is not None
                    else None
                ),
                queue_interval_s=strat.queue_interval.total_seconds(),
                queue_adaptive_lifo=strat.queue_adaptive_lifo,
                prioritized=strat.prioritized,
            )
            self._bulkheads[key] = state

        return state

    # ....................... #

    def _adaptive_bulkhead_for(
        self,
        strat: AdaptiveBulkheadStrategy,
        pol: ResiliencePolicy,
        route: StrKey | None,
    ) -> AdaptiveBulkheadState:
        key = (pol.name, route)
        state = self._adaptive_bulkheads.get(key)

        if state is None:
            state = AdaptiveBulkheadState(
                latency_threshold=strat.latency_threshold.total_seconds(),
                latency_quantile=strat.latency_quantile,
                min_concurrency=strat.min_concurrency,
                max_concurrency=strat.max_concurrency,
                max_queue=strat.max_queue,
                backoff_ratio=strat.backoff_ratio,
                increase_step=strat.increase_step,
                cooldown=strat.cooldown.total_seconds(),
                clock=self.clock,
                queue_target_s=(
                    strat.queue_target.total_seconds()
                    if strat.queue_target is not None
                    else None
                ),
                queue_interval_s=strat.queue_interval.total_seconds(),
                queue_adaptive_lifo=strat.queue_adaptive_lifo,
                prioritized=strat.prioritized,
            )
            self._adaptive_bulkheads[key] = state

        return state

    # ....................... #

    def _gradient_bulkhead_for(
        self,
        strat: GradientBulkheadStrategy,
        pol: ResiliencePolicy,
        route: StrKey | None,
    ) -> AdaptiveBulkheadState:
        key = (pol.name, route)
        state = self._gradient_bulkheads.get(key)

        if state is None:
            # The Gradient2 controller owns the limit; the AIMD fields are inert
            # (latency_threshold = inf so the failure path never feeds a sample).
            state = AdaptiveBulkheadState(
                latency_threshold=float("inf"),
                min_concurrency=strat.min_concurrency,
                max_concurrency=strat.max_concurrency,
                max_queue=strat.max_queue,
                backoff_ratio=0.5,
                increase_step=1.0,
                cooldown=0.0,
                clock=self.clock,
                queue_target_s=(
                    strat.queue_target.total_seconds()
                    if strat.queue_target is not None
                    else None
                ),
                queue_interval_s=strat.queue_interval.total_seconds(),
                queue_adaptive_lifo=strat.queue_adaptive_lifo,
                prioritized=strat.prioritized,
                limiter=Gradient2Limiter(
                    initial_limit=strat.max_concurrency,
                    max_limit=strat.max_concurrency,
                    min_limit=strat.min_concurrency,
                    rtt_tolerance=strat.rtt_tolerance,
                    smoothing=strat.smoothing,
                    long_window=strat.long_window,
                    queue_size=strat.headroom,
                ),
            )
            self._gradient_bulkheads[key] = state

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

    def _throttle_for(
        self,
        strat: AdaptiveThrottleStrategy,
        pol: ResiliencePolicy,
        route: StrKey | None,
    ) -> AdaptiveThrottleState:
        key = (pol.name, route)
        state = self._throttles.get(key)

        if state is None:
            state = AdaptiveThrottleState(
                k=strat.k,
                window=strat.window.total_seconds(),
                min_throughput=strat.min_throughput,
            )
            self._throttles[key] = state

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

    def _hedge_delay_for(
        self,
        strat: HedgeStrategy,
        pol: ResiliencePolicy,
        route: StrKey | None,
    ) -> HedgeDelayState | None:
        if strat.adaptive_delay_quantile is None:
            return None

        key = (pol.name, route)
        state = self._hedge_delays.get(key)

        if state is None:
            state = HedgeDelayState(
                quantile=strat.adaptive_delay_quantile,
                fixed_delay=strat.delay.total_seconds(),
                floor=(
                    strat.delay_min.total_seconds()
                    if strat.delay_min is not None
                    else None
                ),
                cap=(
                    strat.delay_max.total_seconds()
                    if strat.delay_max is not None
                    else None
                ),
            )
            self._hedge_delays[key] = state

        return state

    # ....................... #

    def _emit(self, op: str, pol: ResiliencePolicy, route: StrKey | None) -> None:
        route_name = str(route) if route is not None else None

        # The metrics sink is independent of the tracing gate: production runs
        # with tracing off still export breaker/rejection metrics.
        if self._metrics_sink is not None:
            self._metrics_sink(op, str(pol.name), route_name)

        record(
            domain="resilience",
            op=op,
            surface="resilience_executor",
            route=route_name,
            phase=str(pol.name),
        )
