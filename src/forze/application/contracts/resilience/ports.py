"""Port for executing a callable under a named resilience policy."""

from collections.abc import Awaitable, Callable
from typing import Protocol, runtime_checkable

from forze.base.primitives import StrKey

# ----------------------- #


@runtime_checkable
class ResilienceExecutorPort(Protocol):
    """Executes a zero-arg async callable under a named, composed policy."""

    def run[T](
        self,
        fn: Callable[[], Awaitable[T]],
        *,
        policy: StrKey,
        route: StrKey | None = None,
        fallback: Callable[[BaseException], Awaitable[T]] | None = None,
    ) -> Awaitable[T]:
        """Run ``fn`` under ``policy``.

        ``route`` keys process-local breaker/bulkhead state so distinct backends
        under one policy fail independently. ``fallback`` is invoked with the
        terminal exception only when the policy declares a
        :class:`~forze.application.contracts.resilience.FallbackStrategy`.
        """

        ...  # pragma: no cover

    # ....................... #

    def run_hedged[T](
        self,
        fn: Callable[[], Awaitable[T]],
        *,
        policy: StrKey,
        route: StrKey | None = None,
    ) -> Awaitable[T]:
        """Run ``fn`` with hedging: concurrent staggered attempts, first success wins.

        Uses the named policy's
        :class:`~forze.application.contracts.resilience.HedgeStrategy`. Each attempt
        is an independent call of ``fn``; losers are cancelled. Only safe for
        idempotent / read-only operations.
        """

        ...  # pragma: no cover
