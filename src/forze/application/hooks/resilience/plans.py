"""Wire engine-level resilience policies into operation-registry plans."""

from collections.abc import Awaitable, Callable
from typing import Any, final

import attrs

from forze.application.contracts.execution import (
    Middleware,
    MiddlewareFactory,
    MiddlewareStep,
)
from forze.application.contracts.resilience import HedgeSafety
from forze.application.execution.context import ExecutionContext
from forze.application.execution.resilience import resolve_resilience_executor
from forze.base.primitives import StrKey

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ResilienceWrap(MiddlewareFactory):
    """Wrap a whole operation in a named resilience policy.

    Applies the policy (timeout / circuit-breaker / bulkhead / retry) around the
    operation via the resolved executor, falling back to the shared default
    executor when no :class:`ResilienceDepsModule` is registered (so the builtin
    ``occ`` / ``transient`` policies work out of the box).

    **Retry safety:** a retry re-executes the whole operation, opening a fresh
    transaction per attempt — so transactional side effects roll back between
    attempts and are safe to retry. Only attach a *retry-bearing* policy to
    operations that tolerate re-execution: read-only operations, fully
    transactional operations, or commands guarded by
    :class:`~forze.application.hooks.idempotency.IdempotencyWrap`. Operations with
    non-transactional external side effects (a direct outbound call, a send)
    should use a timeout/breaker-only policy.
    """

    policy: StrKey
    """Name of the resilience policy to apply."""

    route: StrKey | None = None
    """Optional route keying process-local breaker/bulkhead state."""

    # ....................... #

    def __call__(self, ctx: ExecutionContext) -> Middleware[Any, Any]:
        executor = resolve_resilience_executor(ctx)

        async def _wrap(
            next: Callable[[Any], Awaitable[Any]],
            args: Any,
        ) -> Any:
            return await executor.run(
                lambda: next(args),
                policy=self.policy,
                route=self.route,
            )

        return _wrap

    # ....................... #

    def to_step(
        self,
        *,
        step_id: StrKey = "resilience",
        priority: int = 20,
    ) -> MiddlewareStep:
        """Build a :class:`MiddlewareStep`.

        The default ``priority`` places the policy just inside an
        :class:`~forze.application.hooks.idempotency.IdempotencyWrap` (priority 10)
        so a replayed result skips retries, while still wrapping the operation's
        ``before`` hooks and handler.
        """

        return MiddlewareStep(id=step_id, factory=self, priority=priority)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class HedgeWrap(MiddlewareFactory):
    """Hedge a whole operation: staggered concurrent attempts, first success wins.

    Cuts tail latency by racing redundant copies of the operation. Sits **outer** to
    :class:`ResilienceWrap` (each attempt re-runs the resilience pipeline) and inner
    to :class:`~forze.application.hooks.idempotency.IdempotencyWrap` (a replayed
    result skips hedging).

    **Safety:** concurrent duplicates are only safe on idempotent / read-only
    operations. Enforced at registry freeze — the operation must carry an
    ``IdempotencyWrap`` (auto-detected) **or** this wrap must declare an explicit
    ``safety`` (:class:`~forze.application.contracts.resilience.HedgeSafety`).
    """

    policy: StrKey
    """Name of the resilience policy whose ``HedgeStrategy`` drives the hedge."""

    route: StrKey | None = None
    """Optional route keying the hedge budget state."""

    safety: HedgeSafety | None = None
    """Explicit safety basis; omit only when the op carries an ``IdempotencyWrap``."""

    # ....................... #

    def __call__(self, ctx: ExecutionContext) -> Middleware[Any, Any]:
        executor = resolve_resilience_executor(ctx)

        async def _wrap(
            next: Callable[[Any], Awaitable[Any]],
            args: Any,
        ) -> Any:
            return await executor.run_hedged(
                lambda: next(args),
                policy=self.policy,
                route=self.route,
            )

        return _wrap

    # ....................... #

    def hedge_safety_declared(self) -> bool:
        """Marker (``DeclaresHedge``): whether an explicit safety basis was given."""

        return self.safety is not None

    # ....................... #

    def to_step(
        self,
        *,
        step_id: StrKey = "hedge",
        priority: int = 15,
    ) -> MiddlewareStep:
        """Build a :class:`MiddlewareStep`.

        The default ``priority`` (15) places hedging just inside an
        :class:`~forze.application.hooks.idempotency.IdempotencyWrap` (priority 10)
        and just outside a :class:`ResilienceWrap` (priority 20).
        """

        return MiddlewareStep(id=step_id, factory=self, priority=priority)
