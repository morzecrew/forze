"""The conformance verdict model, the backend seam, and conflict normalization.

The isolation battery is **adapter-agnostic**: an anomaly runs against a
:class:`ConformanceBackend` that yields *N* independent sessions (execution contexts) over one
shared store. The mock backend lives in tests; the real-adapter (testcontainers) backend is the
differential leg that turns "passed on the mock" into "matches the real engine".

A run produces a :class:`Verdict` — ``PERMITTED`` (the anomaly occurred) or ``PREVENTED`` (it did
not). Every "prevented" *mechanism* collapses to ``PREVENTED`` before comparison: Forze's rev-OCC
revision conflict, the mock/real serialization failure, and a lock-based block all normalize the
same way, because the differential compares the anomaly OUTCOME at the declared isolation level —
never the mechanism, the error code, or which transaction was the victim.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from enum import Enum
from typing import AsyncIterator, MutableMapping, Protocol, Sequence, runtime_checkable

from forze.application.execution import ExecutionContext
from forze.base.exceptions.model import CoreException, ExceptionKind

# ----------------------- #


class Verdict(Enum):
    """Whether an isolation anomaly was observed at a level (``PERMITTED``) or not (``PREVENTED``)."""

    PERMITTED = "permitted"
    PREVENTED = "prevented"


# ....................... #


@runtime_checkable
class ConformanceBackend(Protocol):
    """A backend the battery runs against: *N* independent sessions over one shared store.

    :meth:`contexts` returns a **fresh** world of *n* sessions each call, so cases never bleed
    into one another; :attr:`scope_name` is the transaction scope key the sessions open
    (``"mock"``, ``"postgres"``, …). The mock backend is provided by the test suite; the
    real-adapter differential leg adds a testcontainers-backed implementation.
    """

    @property
    def scope_name(self) -> str: ...

    def contexts(self, n: int) -> Sequence[ExecutionContext]: ...


# ....................... #


def is_serialization_conflict(error: BaseException) -> bool:
    """Whether *error* is the normalized "this transaction was aborted by a conflict" signal.

    True for any concurrency-kind :class:`CoreException`. Forze's rev-OCC revision conflict and
    the snapshot/serializable serialization failure both surface here, and the real adapters map
    a backend serialization error (e.g. Postgres ``SQLSTATE 40001``) to the same kind. The
    differential matches on this class, never on the literal code, the victim, or whether the
    engine aborted vs. blocked.
    """

    return isinstance(error, CoreException) and error.kind is ExceptionKind.CONCURRENCY


# ....................... #


@asynccontextmanager
async def record_outcome(
    outcomes: MutableMapping[str, str], name: str
) -> AsyncIterator[None]:
    """Run a session body, recording ``"committed"`` or — on a conflict — ``"aborted"``.

    A conflict (rev-OCC or serialization failure) is swallowed and recorded, so it does not
    propagate out of the participant's coroutine and wedge the forced interleaving; any other
    error re-raises (a real bug, not an isolation outcome). The commit point is the scope exit,
    so wrap the whole ``async with ...tx_ctx.scope(...)`` block.
    """

    try:
        yield
        outcomes[name] = "committed"

    except CoreException as error:
        if is_serialization_conflict(error):
            outcomes[name] = "aborted"
        else:
            raise
