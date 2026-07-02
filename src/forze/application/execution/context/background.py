"""Per-scope registry of detached-background-work owners, cancelled at shutdown.

Some ports spawn detached tasks that outlive the operation that started them — the
document read-through cache's probabilistic early refresh is the motivating case: a hit
elected for refresh fetches and re-warms in a background task after the request commits.
Those tasks hold references to the same clients (cache, read gateway) that lifecycle
teardown closes, so on shutdown they must be cancelled *before* the close, or they run on
against closing resources.

The owner (e.g. a ``DocumentCache``) registers itself here at construction; the runtime
closes the registry after draining in-flight operations and before lifecycle teardown. The
registry holds owners weakly, so a transient owner with no live background work is collected
normally — one whose task is still running is kept alive by that task and so is still
closed.
"""

import asyncio
import weakref
from typing import Protocol, final, runtime_checkable

import attrs

from forze.application._logger import logger

# ----------------------- #


@runtime_checkable
class SupportsAsyncClose(Protocol):
    """An owner of detached background work that can be cancelled/released at shutdown."""

    async def aclose(self) -> None:
        """Cancel in-flight background work and release held subscriptions (idempotent)."""
        ...  # pragma: no cover


# ....................... #


@final
@attrs.define(slots=True)
class BackgroundOwners:
    """Weak registry of :class:`SupportsAsyncClose` owners, closed once at shutdown."""

    _owners: "weakref.WeakSet[SupportsAsyncClose]" = attrs.field(
        factory=weakref.WeakSet,
        init=False,
        repr=False,
    )

    # ....................... #

    def register(self, owner: SupportsAsyncClose) -> None:
        """Register *owner* to be closed at runtime shutdown (idempotent per instance)."""

        self._owners.add(owner)

    # ....................... #

    async def close(self, *, grace: float) -> int:
        """Close every registered owner concurrently, bounded overall by *grace* seconds.

        Each :meth:`SupportsAsyncClose.aclose` cancels its detached tasks and releases its
        subscriptions; failures are isolated (one owner never blocks another) and the whole
        pass is bounded so a wedged ``aclose`` cannot hang shutdown. Returns the number of
        owners closed.
        """

        owners = list(self._owners)

        if not owners:
            return 0

        async def _close_all() -> None:
            results = await asyncio.gather(
                *(owner.aclose() for owner in owners),
                return_exceptions=True,
            )
            # Failures are isolated (return_exceptions), but must not vanish: log each one
            # against its owner so a broken aclose is diagnosable, not silently swallowed.
            for owner, result in zip(owners, results):
                if isinstance(result, BaseException):
                    logger.error(
                        "Background owner %s failed to close at shutdown",
                        type(owner).__name__,
                        exc_info=result,
                    )

        try:
            async with asyncio.timeout(grace):
                await _close_all()

        except TimeoutError:
            logger.warning(
                "Background-owner shutdown exceeded %.1fs; proceeding with teardown",
                grace,
            )

        return len(owners)
