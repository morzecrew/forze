"""Port for executing a callable under a named resilience policy."""

from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable
from typing import (
    Protocol,
    runtime_checkable,
)

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

    def run_stream[T](
        self,
        fn: Callable[[], AsyncIterator[T]],
        *,
        policy: StrKey,
        route: StrKey | None = None,
    ) -> AsyncGenerator[T]:
        """Stream ``fn`` under ``policy``'s circuit breaker.

        A stream gets the breaker only: acquisition (the first pull) is
        rejected while the breaker is open or force-opened, and the stream's
        outcome feeds the breaker — a mid-stream infrastructure/timeout
        failure is a breaker failure; clean exhaustion or a consumer-initiated
        close is a success; caller-caused errors do not trip it. Retry,
        hedging, timeout, bulkhead, and rate-limit strategies never apply to
        streams (a partially consumed stream cannot be replayed, and a
        long-lived stream is legitimate).
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
