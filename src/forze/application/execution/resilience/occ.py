"""Decorator applying the ``occ`` resilience policy to read-modify-write methods."""

import functools
from collections.abc import Awaitable, Callable
from typing import (
    Concatenate,
    Final,
    ParamSpec,
    Protocol,
    TypeVar,
    cast,
)

from forze.application.contracts.resilience import ResilienceExecutorPort

# ----------------------- #

OCC_POLICY: Final = "occ"
"""Name of the built-in optimistic-concurrency retry policy."""

# ....................... #


class _ResilienceBoundary(Protocol):
    """Structural requirement for :func:`occ_retry`: a ``resilience`` executor."""

    @property
    def resilience(self) -> ResilienceExecutorPort: ...


# ....................... #

_S = TypeVar("_S", bound=_ResilienceBoundary)
_P = ParamSpec("_P")
_R = TypeVar("_R")


# ....................... #


def occ_retry(
    method: Callable[Concatenate[_S, _P], Awaitable[_R]],
) -> Callable[Concatenate[_S, _P], Awaitable[_R]]:
    """Retry a read-modify-write method under the ``occ`` policy.

    The owning class must expose a ``resilience`` executor. Each retry re-invokes
    the whole method, so it re-reads current state before recomputing the write
    (the optimistic-concurrency boundary stays local to the gateway).
    """

    @functools.wraps(method)
    async def wrapper(self: _S, *args: _P.args, **kwargs: _P.kwargs) -> _R:
        return await self.resilience.run(
            lambda: method(self, *args, **kwargs),
            policy=OCC_POLICY,
        )

    return cast(Callable[Concatenate[_S, _P], Awaitable[_R]], wrapper)
