"""Transaction-aware optimistic-concurrency retry for Postgres gateways."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

import functools
from collections.abc import Awaitable, Callable
from typing import Concatenate, ParamSpec, Protocol, TypeVar, cast

from psycopg import Error as PsycopgError

from forze.application.contracts.resilience import ResilienceExecutorPort
from forze.application.execution.resilience import OCC_POLICY
from forze.base.exceptions import CoreException, ExceptionKind

from ..client.port import PostgresClientPort

# ----------------------- #


class _PostgresOccBoundary(Protocol):
    """Structural requirement: a Postgres client and a resilience executor."""

    @property
    def client(self) -> PostgresClientPort: ...

    @property
    def resilience(self) -> ResilienceExecutorPort: ...


# ....................... #

_S = TypeVar("_S", bound=_PostgresOccBoundary)
_P = ParamSpec("_P")
_R = TypeVar("_R")


# ....................... #


def _server_reported(error: BaseException) -> bool:
    """Whether *error*'s chain contains a psycopg (server or driver) error.

    Postgres aborts the whole transaction on any server-reported error — every
    further command fails with "current transaction is aborted, commands ignored
    until end of transaction block" — and a driver-level failure (lost
    connection) leaves the context-bound connection equally unusable. Either
    way, re-running the method inside the same transaction cannot succeed.

    Conflicts the gateway detects itself (an ``UPDATE … WHERE rev = X`` matching
    zero rows) raise a plain ``concurrency`` error with no psycopg error in the
    chain: no server error occurred and the transaction is still healthy.
    """

    seen: set[int] = set()
    current: BaseException | None = error

    while current is not None and id(current) not in seen:
        if isinstance(current, PsycopgError):
            return True

        seen.add(id(current))
        current = current.__cause__ or current.__context__

    return False


# ....................... #


class _AbortedTransactionConflict(Exception):
    """Internal marker: a conflict that aborted the caller's transaction.

    Not a :class:`CoreException`, so the resilience retry loop propagates it
    without further attempts; the decorator unwraps it and re-raises the
    original conflict.
    """

    def __init__(self, original: CoreException) -> None:
        super().__init__(str(original))
        self.original = original


# ....................... #


def postgres_occ_retry(
    method: Callable[Concatenate[_S, _P], Awaitable[_R]],
) -> Callable[Concatenate[_S, _P], Awaitable[_R]]:
    """Retry a read-modify-write method under the ``occ`` policy — transaction-aware.

    Outside a caller transaction each retry re-invokes the whole method (same
    contract as the shared ``occ_retry``): the method's own transaction rolled
    back with the failure, so every attempt starts clean and re-reads current
    state before recomputing the write.

    Inside a caller-owned transaction only conflicts detected client-side (a
    stale-revision ``UPDATE`` matching zero rows) are retried: the transaction
    is still healthy and, under READ COMMITTED, the retry's re-read observes
    the competing writer's committed row, so the retry can genuinely succeed
    in place. A conflict reported by the server (serialization failure,
    deadlock, lock timeout) has already aborted the transaction, so the method
    is not re-attempted and the original conflict surfaces as ``concurrency``
    for the owner of the transaction scope to re-run the whole scope.
    """

    @functools.wraps(method)
    async def wrapper(self: _S, *args: _P.args, **kwargs: _P.kwargs) -> _R:
        if not self.client.is_in_transaction():
            return await self.resilience.run(
                lambda: method(self, *args, **kwargs),
                policy=OCC_POLICY,
            )

        async def attempt() -> _R:
            try:
                return await method(self, *args, **kwargs)

            except CoreException as error:
                if error.kind is ExceptionKind.CONCURRENCY and _server_reported(error):
                    raise _AbortedTransactionConflict(error) from error

                raise

        try:
            return await self.resilience.run(attempt, policy=OCC_POLICY)

        except _AbortedTransactionConflict as marker:
            raise marker.original from marker.original.__cause__

    return cast(Callable[Concatenate[_S, _P], Awaitable[_R]], wrapper)
