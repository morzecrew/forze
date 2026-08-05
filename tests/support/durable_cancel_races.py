"""Forced-schedule race battery for durable run control, shared by the mock and Postgres legs.

The two races the cancel design has to survive are both *write* races resolved inside the
store, so this forces the schedule where it actually matters: every ordering of the competing
writes is enumerated and replayed against a fresh run, rather than sampled by hoping a sleep
lands in the right place.

Enumerating beats seeding here. A seeded scheduler visits some interleavings; permutations
visit all of them, and the count is small enough (2 and 4 writers) that "all" is cheap. A
race that only fails under one ordering in six is exactly the bug this must not miss.

Two properties are checked after every ordering:

- **exactly one terminal** — the run ends in one of the legal terminal states, never
  ``RUNNING`` and never a mixture;
- **never torn** — the surviving status and the surviving payload came from the *same*
  writer. A run reported ``CANCELLED`` while carrying a completion's output would be the
  worst outcome available: a caller who already saw the result, and a dashboard that says the
  work was stopped.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import timedelta
from itertools import permutations
from typing import Any, cast

from forze.application.contracts.durable.function import (
    DurableRunAdminPort,
    DurableRunRecord,
    DurableRunStatus,
    DurableRunStorePort,
)

# ----------------------- #

_LEASE = timedelta(minutes=5)

_LEGAL_TERMINALS = (DurableRunStatus.CANCELLED, DurableRunStatus.COMPLETED)
"""Both outcomes are legal for a cancel racing a completion — nobody can say which *should*
win when an operator asks while a body is finishing. What is not legal is a third answer."""

_OUTPUT = {"result": "done"}


# ....................... #


def _assert_untorn(record: DurableRunRecord, ordering: str) -> None:
    """One writer won outright: the status and the payload must agree on which."""

    assert record.status in _LEGAL_TERMINALS, f"{ordering}: illegal terminal {record.status}"

    if record.status is DurableRunStatus.COMPLETED:
        assert record.output_json == _OUTPUT, f"{ordering}: completed without its output"
        assert record.error is None, f"{ordering}: completed carrying an error"

    else:
        assert record.output_json is None, f"{ordering}: cancelled carrying a completion output"


# ....................... #


async def run_cancel_vs_complete_race(
    store_factory: Callable[[], DurableRunStorePort],
) -> list[str]:
    """Every ordering of {ask, land-cancel, complete} against one claimed run.

    Returns the winning terminal per ordering so a caller can assert the mock and the real
    engine resolve each *identical* ordering identically — a store that silently prefers a
    different writer is a divergence a per-ordering "one of two is fine" check would miss.
    """

    winners: list[str] = []

    for ordering in permutations(("ask", "cancel", "complete")):
        store = store_factory()
        admin = cast(DurableRunAdminPort, store)

        record = await store.enqueue("race", input_json=None)
        holder = await store.begin(record.run_id, lease_for=_LEASE)
        assert holder is not None

        actions: dict[str, Callable[[], Awaitable[Any]]] = {
            "ask": lambda: admin.request_cancel(record.run_id),
            "cancel": lambda: store.mark_cancelled(record.run_id, fence=holder.attempts),
            "complete": lambda: store.complete(
                record.run_id, output_json=_OUTPUT, fence=holder.attempts
            ),
        }

        for name in ordering:
            await actions[name]()

        landed = await store.load(record.run_id)
        assert landed is not None
        _assert_untorn(landed, "->".join(ordering))

        winners.append(f"{'->'.join(ordering)}={landed.status.value}")

    return winners


# ....................... #


async def run_stale_holder_race(
    store_factory: Callable[[], DurableRunStorePort],
    expire_lease: Callable[[DurableRunStorePort, str], Awaitable[None]],
) -> list[str]:
    """Every ordering of a stale worker's and the current owner's competing writes.

    Worker A lost its lease but is still alive and still believes it owns the run — the
    zombie-replica case. Whatever order the four writes arrive in, only the current holder's
    may take effect; A's are inert. This is the single property that makes cancellation safe
    on more than one replica, so it is checked against every arrival order rather than the
    one the happy path happens to produce.
    """

    outcomes: list[str] = []
    writes = ("ask", "stale_cancel", "stale_complete", "owner_cancel")

    for ordering in permutations(writes):
        store = store_factory()
        admin = cast(DurableRunAdminPort, store)

        record = await store.enqueue("race", input_json=None)
        worker_a = await store.begin(record.run_id, lease_for=_LEASE)
        assert worker_a is not None

        # A stalls past its lease; B reclaims and its claim advances the fence.
        await expire_lease(store, record.run_id)
        reclaimed = await store.claim_abandoned(limit=10, lease_for=_LEASE)
        worker_b = next(r for r in reclaimed if r.run_id == record.run_id)
        assert worker_b.attempts > worker_a.attempts

        actions: dict[str, Callable[[], Awaitable[Any]]] = {
            "ask": lambda: admin.request_cancel(record.run_id),
            "stale_cancel": lambda: store.mark_cancelled(
                record.run_id, fence=worker_a.attempts
            ),
            "stale_complete": lambda: store.complete(
                record.run_id, output_json=_OUTPUT, fence=worker_a.attempts
            ),
            "owner_cancel": lambda: store.mark_cancelled(
                record.run_id, fence=worker_b.attempts
            ),
        }

        for name in ordering:
            await actions[name]()

        landed = await store.load(record.run_id)
        assert landed is not None

        label = "->".join(ordering)

        # Only the owner's write can land, so the run is CANCELLED and carries no output —
        # A's completion never took, whatever position it arrived in.
        assert landed.status is DurableRunStatus.CANCELLED, f"{label}: {landed.status}"
        assert landed.output_json is None, f"{label}: the stale worker's output survived"
        assert landed.attempts == worker_b.attempts, f"{label}: the fence moved"

        outcomes.append(f"{label}={landed.status.value}")

    return outcomes
