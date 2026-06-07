"""Pass-through resilience executor for tests.

Runs the callable directly with no retries, timeouts, breaker, or backoff sleeps,
so tests exercise business logic without resilience-induced latency. Fallbacks are
still honored so fallback-dependent behavior remains testable.
"""

from collections.abc import Awaitable, Callable
from typing import final

import attrs

from forze.base.primitives import StrKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True)
class PassthroughResilienceExecutor:
    """Resilience executor double that applies no policy behavior."""

    async def run[T](
        self,
        fn: Callable[[], Awaitable[T]],
        *,
        policy: StrKey,
        route: StrKey | None = None,
        fallback: Callable[[BaseException], Awaitable[T]] | None = None,
    ) -> T:
        """Run ``fn`` directly; on failure, invoke ``fallback`` if provided."""

        try:
            return await fn()

        except Exception as error:  # noqa: BLE001 — fallback boundary
            if fallback is not None:
                return await fallback(error)

            raise
