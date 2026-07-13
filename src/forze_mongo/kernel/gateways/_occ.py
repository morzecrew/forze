"""Transaction-aware optimistic-concurrency retry for Mongo gateways."""

import functools
from collections.abc import Awaitable, Callable
from typing import Concatenate, ParamSpec, Protocol, TypeVar, cast

from forze.application.contracts.resilience import ResilienceExecutorPort
from forze.application.execution.resilience import OCC_POLICY

from ..client.port import MongoClientPort

# ----------------------- #


class _MongoOccBoundary(Protocol):
    """Structural requirement: a Mongo client and a resilience executor."""

    @property
    def client(self) -> MongoClientPort: ...

    @property
    def resilience(self) -> ResilienceExecutorPort: ...


# ....................... #

_S = TypeVar("_S", bound=_MongoOccBoundary)
_P = ParamSpec("_P")
_R = TypeVar("_R")


# ....................... #


def mongo_occ_retry(
    method: Callable[Concatenate[_S, _P], Awaitable[_R]],
) -> Callable[Concatenate[_S, _P], Awaitable[_R]]:
    """Retry a read-modify-write method under the ``occ`` policy — outside transactions only.

    Outside a transaction each retry re-invokes the whole method, so it re-reads
    current state before recomputing the write (same contract as the shared
    ``occ_retry``).

    Inside a Mongo multi-document transaction a retry can never succeed, so the
    method runs exactly once and a conflict propagates as ``concurrency``:

    - a server write conflict aborts the whole transaction, and every further
      operation on that session fails with ``NoSuchTransaction``;
    - a stale-revision miss re-reads the transaction's own snapshot, which cannot
      observe the competing writer's committed state.

    The owner of the transaction scope (the caller retrying a retryable
    ``concurrency`` egress) re-runs the whole scope on a fresh transaction.
    """

    @functools.wraps(method)
    async def wrapper(self: _S, *args: _P.args, **kwargs: _P.kwargs) -> _R:
        if self.client.is_in_transaction():
            return await method(self, *args, **kwargs)

        return await self.resilience.run(
            lambda: method(self, *args, **kwargs),
            policy=OCC_POLICY,
        )

    return cast(Callable[Concatenate[_S, _P], Awaitable[_R]], wrapper)
