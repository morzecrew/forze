"""Wire engine-level resilience policies into operation-registry plans."""

from collections.abc import Awaitable, Callable
from typing import Any, final

import attrs

from forze.application.contracts.execution import (
    Middleware,
    MiddlewareFactory,
    MiddlewareStep,
)
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
