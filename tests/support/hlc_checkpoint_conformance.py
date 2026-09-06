"""Shared ``HlcCheckpointPort`` conformance battery.

The port exists to keep a hybrid logical clock monotonic across a process restart: the mark
it persists is the floor a restarted clock resumes above. Two promises carry that, and both
are easy to implement *almost* right.

**Monotonic max.** ``advance`` raises the mark and never lowers it, so out-of-order or
concurrent writers are safe. An implementation that simply assigns — the obvious one —
passes every round-trip test and silently lowers the floor the moment a slower writer lands
second, which is exactly the case a restart then re-issues into.

**Atomic with the business transaction.** ``advance`` runs inside the caller's transaction,
so a committed stamp is never durable without a mark covering it, and a rolled-back flush
does not advance the mark. An implementation that writes on its own connection passes every
single-threaded test here and breaks the guarantee the port is *for*: the mark races ahead
of rows that never committed, or lags behind rows that did.

Neither promise is expressible in a signature, and the three backends reach them by
different mechanisms — ``GREATEST`` in an upsert on Postgres, ``$max`` on Mongo, a compare
plus an undo-journal entry in the mock. Reading the code proves nothing about whether they
agree; running the same checks against all three is what does.

Used by:

- ``tests/unit/test_forze_mock/test_mock_hlc_checkpoint_conformance.py`` (the oracle)
- ``tests/integration/test_forze_postgres/test_pg_hlc_checkpoint_conformance.py``
- ``tests/integration/test_forze_mongo/test_mongo_hlc_checkpoint_conformance.py``
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from typing import Any

import attrs
import pytest

from forze.application.contracts.hlc import HlcCheckpointPort
from forze.base.primitives import HlcTimestamp

# ----------------------- #


class _Rollback(RuntimeError):
    """Raised inside a transaction to abort it, and swallowed by the check that raises it."""


@attrs.define(slots=True, kw_only=True)
class WriteGate:
    """Holds a store's next write open so another writer can land inside it.

    ``reached`` fires when the gated store is about to write — that is, after it has done
    whatever reading it does. ``release`` lets that write proceed. A backend whose advance is
    a read-modify-write has therefore already chosen its value when ``reached`` fires; one
    that hands the comparison to the server has not.
    """

    reached: asyncio.Event = attrs.field(factory=asyncio.Event)
    release: asyncio.Event = attrs.field(factory=asyncio.Event)

    async def hold(self) -> None:
        """Called by the gated transport at the write: announce, then wait."""

        self.reached.set()
        await self.release.wait()


@attrs.define(slots=True, kw_only=True, frozen=True)
class HlcCheckpointHarness:
    """One backend's seam for the high-water-mark battery."""

    store_for: Callable[[str], HlcCheckpointPort]
    """Build a store for one *node key*, over the backing relation every other store here
    shares. A seam rather than a fixed pair because ``load`` reads across node keys, and
    that is only observable with two stores writing different ones."""

    transaction: Callable[[], AbstractAsyncContextManager[Any]]
    """Open a business transaction on the same connection the stores write through.

    The mock's is its in-memory transaction, Postgres's and Mongo's are real. Without this
    seam the battery could only test the half of the port that does not matter."""

    backend: str
    """Label used in assertion messages."""

    gated_writer: Callable[[], tuple[HlcCheckpointPort, WriteGate]] | None = None
    """Build a store whose next *write* blocks until released, on its own connection.

    The seam the concurrency check needs, and the only one it needs: holding the write is
    what separates a store that decided what to write **before** the other advance from one
    that lets the server decide **at** the write.

    ``None`` declares that this backend's advance has no client-visible read/write split to
    gate — the mock compares and assigns under one held lock with no await point, so there is
    no interleaving to force and no mutant for the check to catch. Declared rather than
    quietly skipped, so "not applicable" is visible in the run.
    """


Check = Callable[[HlcCheckpointHarness], Any]
"""One battery check."""


# ....................... #


async def check_load_is_none_before_anything_is_written(h: HlcCheckpointHarness) -> None:
    """An empty store reports no floor, rather than ``(0, 0)``.

    The distinction is load-bearing at startup: ``None`` means "resume from wall time", and
    a zero timestamp would mean "resume above the epoch" — the same thing today and not
    after the first mark is written, so a store that conflates them fails only later.
    """

    assert await h.store_for("solo").load() is None, h.backend


async def check_advance_then_load_round_trips(h: HlcCheckpointHarness) -> None:
    """Both components survive the round trip, not just the physical one.

    The logical counter is what orders two stamps inside one millisecond, so a store that
    persists only the physical part reads back as monotonic while losing exactly the
    ordering the logical component exists to provide.
    """

    store = h.store_for("solo")
    await store.advance(HlcTimestamp(1_700_000_000_000, 3))

    assert await store.load() == HlcTimestamp(1_700_000_000_000, 3), h.backend


async def check_a_lower_mark_never_lowers_the_stored_one(h: HlcCheckpointHarness) -> None:
    """The promise the port is named for, and the one a plain assignment breaks.

    Ordering is over the *packed* value, so a lower logical component at the same
    millisecond is a lower mark too — checked here because a backend comparing only the
    physical part would accept it and quietly drop the floor within that millisecond.

    Every lower write is asserted *immediately*, and the sequence deliberately does not end
    on the high mark: a store that simply assigns leaves the right value behind whenever the
    last write happens to be the largest, so a check that only inspects the end state passes
    against the exact implementation it exists to reject. This one was written the wrong way
    round first and a ``$set`` mutant survived it.
    """

    store = h.store_for("solo")
    await store.advance(HlcTimestamp(9_000, 3))
    assert await store.load() == HlcTimestamp(9_000, 3), h.backend

    await store.advance(HlcTimestamp(5_000, 0))
    assert await store.load() == HlcTimestamp(9_000, 3), h.backend

    # Same millisecond, lower logical: only a comparison over the packed value refuses it.
    await store.advance(HlcTimestamp(9_000, 1))
    assert await store.load() == HlcTimestamp(9_000, 3), h.backend

    # An equal mark is a documented no-op rather than an error.
    await store.advance(HlcTimestamp(9_000, 3))
    assert await store.load() == HlcTimestamp(9_000, 3), h.backend


async def check_load_reads_the_max_across_node_keys(h: HlcCheckpointHarness) -> None:
    """A restart resumes above the *deployment's* emissions, not just this replica's.

    Per-replica keys exist to avoid contention on one row; if ``load`` read only its own
    key, that optimisation would silently narrow the recovery floor to one node and a
    restarted replica could re-issue beneath a peer it had already merged with.
    """

    await h.store_for("a").advance(HlcTimestamp(1_000, 0))
    await h.store_for("b").advance(HlcTimestamp(2_000, 9))

    assert await h.store_for("a").load() == HlcTimestamp(2_000, 9), h.backend


async def check_an_in_transaction_advance_commits_with_it(h: HlcCheckpointHarness) -> None:
    """The mark lands when the business transaction it rode does."""

    store = h.store_for("solo")

    async with h.transaction():
        await store.advance(HlcTimestamp(1_700_000_000_000, 2))

    assert await store.load() == HlcTimestamp(1_700_000_000_000, 2), h.backend


async def check_a_rolled_back_advance_leaves_the_mark_alone(h: HlcCheckpointHarness) -> None:
    """The half that a store writing on its own connection gets wrong.

    A flush that rolls back stamped nothing durable, so the mark must not move: a floor
    ahead of the emissions it describes is not unsafe by itself, but it is a lie about what
    the node emitted, and the same defect in the other direction — advancing on a
    connection of one's own *before* the business transaction commits — is what lets a
    restart resume beneath rows that did commit.
    """

    store = h.store_for("solo")
    await store.advance(HlcTimestamp(4_000, 0))

    with pytest.raises(_Rollback):
        async with h.transaction():
            await store.advance(HlcTimestamp(9_000, 0))
            raise _Rollback

    assert await store.load() == HlcTimestamp(4_000, 0), h.backend


async def check_a_concurrent_advance_cannot_lose_the_higher_mark(
    h: HlcCheckpointHarness,
) -> None:
    """The promise no sequential test can reach: two writers, and the higher mark survives.

    Every other check here runs one writer at a time, and against those a plain
    read-modify-write is indistinguishable from a compare-and-set — it reads, finds nothing
    larger, and assigns the right answer. The difference only shows when a second writer
    lands *between* the read and the write, which is why this forces that interleaving
    rather than hoping for it: a gathered pair would pass on either implementation most of
    the time, and fail on neither reliably.

    The schedule: hold the low writer at its write, land the high mark from another
    connection, then release. A store that compares server-side keeps the high mark. A store
    that read before the gate already decided to write the low one, and clobbers it.
    """

    if h.gated_writer is None:
        pytest.skip(f"{h.backend}: advance is structurally atomic — no write to gate")

    gated, gate = h.gated_writer()
    low, high = HlcTimestamp(5_000, 0), HlcTimestamp(9_000, 0)

    held = asyncio.create_task(gated.advance(low))

    try:
        await asyncio.wait_for(gate.reached.wait(), timeout=10)

        # Lands entirely inside the held write's window, on its own connection.
        await h.store_for("solo").advance(high)

    finally:
        gate.release.set()
        await held

    assert await h.store_for("solo").load() == high, h.backend


# ....................... #

HLC_CHECKPOINT_BATTERY: tuple[Check, ...] = (
    check_load_is_none_before_anything_is_written,
    check_advance_then_load_round_trips,
    check_a_lower_mark_never_lowers_the_stored_one,
    check_load_reads_the_max_across_node_keys,
    check_an_in_transaction_advance_commits_with_it,
    check_a_rolled_back_advance_leaves_the_mark_alone,
    check_a_concurrent_advance_cannot_lose_the_higher_mark,
)
