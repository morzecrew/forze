"""The isolation anomaly battery — Adya phenomena as forced interleavings with known verdicts.

Each :class:`AnomalyCase` is a deterministic two/three-session interleaving (driven by the shipped
:class:`~forze.testing.Conductor`) that provokes one classic anomaly, plus the textbook ``contract``
verdict it should get at each :class:`IsolationLevel`. Running a case against a
:class:`~forze_dst.conformance.harness.ConformanceBackend` returns the *observed* :class:`Verdict`;
the differential asserts the observed verdict equals the contract overlaid with the registered
:data:`~forze_dst.conformance.divergence.CONTRACT_STRENGTHENINGS` — so the only way an adapter may
deviate from the textbook is a reviewed, justified strengthening.

The cases span every level boundary Forze models:

- ``dirty_read`` must be prevented at every level (no transaction observes an uncommitted write) —
  the case that drove the mock's faithful read-committed isolation;
- ``non_repeatable_read``, ``read_skew``, and ``phantom`` discriminate READ_COMMITTED from SNAPSHOT
  (``phantom`` is the predicate analogue — a re-run scan seeing a concurrent insert, not a re-read row);
- ``write_skew``, ``predicate_write_skew``, and ``read_only_anomaly`` discriminate SNAPSHOT from
  SERIALIZABLE (the headline SI↔serializable gap): an item write skew, its predicate (phantom-based)
  analogue, and the three-transaction read-only anomaly that proves SI is non-serializable even for a
  read-only transaction;
- ``lost_update`` documents the rev-OCC strengthening (prevented at every level, vs the textbook
  permitting it under READ_COMMITTED).

The forced interleaving assumes an **abort-based** engine (snapshot/serializable that aborts the
loser, like the mock, Postgres, and Mongo): a participant either proceeds or aborts, never blocks. A
purely lock-based engine could block one participant inside a step and wedge the ``Conductor`` (which
advances one participant at a time); adapting the battery to such an engine would mean running the
would-block step so a lock wait is converted into an explicit signal normalized to ``PREVENTED``.
"""

from __future__ import annotations

import asyncio
import itertools
from contextlib import suppress
from typing import Awaitable, Callable, Mapping
from uuid import UUID

import attrs

from forze.application.contracts.querying import QueryFilterExpression
from forze.application.contracts.transaction import IsolationLevel
from forze.application.execution import ExecutionContext
from forze.base.exceptions.model import CoreException, ExceptionKind
from forze.testing import Conductor, Gate
from forze.testing.interleaving import Session

from ._models import (
    CELL,
    ONCALL,
    CellCreate,
    CellUpdate,
    OnCallCreate,
    OnCallUpdate,
)
from .divergence import CONTRACT_STRENGTHENINGS
from .harness import (
    ConformanceBackend,
    Verdict,
    is_serialization_conflict,
    record_outcome,
)

# ----------------------- #

_RC = IsolationLevel.READ_COMMITTED
_SI = IsolationLevel.SNAPSHOT
_SER = IsolationLevel.SERIALIZABLE

_PERMIT = Verdict.PERMITTED
_PREVENT = Verdict.PREVENTED

# The predicate the phantom / predicate-write-skew cases scan for: cells whose value equals a marker.
# A "matching" insert is a new row the predicate selects — the phantom a re-scan would see. Each run
# claims a FRESH marker, so its predicate selects only the rows it itself created. That keeps the
# predicate cases self-isolating (like the id-based cases, which read back only the ids they wrote)
# even on a store reused across runs — otherwise an absolute predicate count would be corrupted by
# rows a prior run left behind.
_marker_seq = itertools.count(7)

# A deterministic (no-random) source of fresh primary keys for the duplicate-key race — a monotonic
# counter, so each run claims an id absent from the store even when it is reused across runs.
_contested_id_seq = itertools.count(1)

# ....................... #


def _fresh_predicate() -> tuple[int, QueryFilterExpression]:
    """A unique marker value for this run and the equality filter that selects only its rows."""

    value = next(_marker_seq)
    return value, {"$values": {"value": value}}


# ....................... #


@attrs.frozen(kw_only=True)
class AnomalyCase:
    """One isolation anomaly: its textbook contract + the forced interleaving that provokes it."""

    name: str
    summary: str
    contract: Mapping[IsolationLevel, Verdict]
    """The Adya/Berenson verdict the anomaly should get at each level (the differential oracle)."""

    run: Callable[[ConformanceBackend, IsolationLevel], Awaitable[Verdict]]
    """Run the interleaving against a backend at a level; return the observed verdict."""

    abort_engine_only: bool = False
    """This case races for a resource one participant holds, so the **vanilla** one-at-a-time
    :class:`~forze.testing.Conductor` can't drive it on a lock-based engine.

    On a lock-based engine (real Postgres) the case's contender BLOCKS mid-step — a duplicate-key
    insert waits on the unique index, a ``FOR UPDATE`` lock waits on the row — which would wedge the
    vanilla Conductor (it advances one participant at a time, waiting for each to park). These cases
    therefore run via the dedicated block-aware ``_drive_lock_race`` driver, and the generic
    parametrized differential legs (which use the vanilla Conductor) split them into dedicated
    lock-race classes: real Postgres exercises the blocking semantics, real Mongo the abort-based
    (immediate WriteConflict) semantics. See the ``lock-block-vs-abort-conductor``
    :data:`~forze_dst.conformance.MECHANISM_DIVERGENCES`."""


# ....................... #
# Non-repeatable read (P2): a transaction reads a row twice and sees a concurrent commit between.


async def _run_non_repeatable_read(
    backend: ConformanceBackend,
    level: IsolationLevel,
) -> Verdict:
    sessions = backend.contexts(2)
    reader, writer = sessions[0], sessions[1]
    scope = backend.scope_name

    async with reader.tx_ctx.scope(scope):
        cid = (await reader.document.command(CELL).create(CellCreate(value=1))).id

    reads: list[int] = []
    outcomes: dict[str, str] = {}

    async def read_twice(gate: Gate) -> None:
        async with record_outcome(outcomes, "reader"):
            async with reader.tx_ctx.scope(scope, isolation=level):
                reads.append((await reader.document.query(CELL).get(cid)).value)
                await (
                    gate.checkpoint()
                )  # let the writer update + commit between the two reads
                reads.append((await reader.document.query(CELL).get(cid)).value)

    async def update_once(gate: Gate) -> None:
        await gate.checkpoint()
        async with record_outcome(outcomes, "writer"):
            async with writer.tx_ctx.scope(scope, isolation=level):
                current = await writer.document.query(CELL).get(cid)
                await writer.document.command(CELL).update(
                    cid, current.rev, CellUpdate(value=2)
                )

    await Conductor(schedule=("writer", "reader")).run(
        {"reader": read_twice, "writer": update_once}
    )

    return _PERMIT if reads[0] != reads[1] else _PREVENT


# ....................... #
# Read skew (G-single): reads one row old and a related row new, seeing an inconsistent cross-item state.


async def _run_read_skew(backend: ConformanceBackend, level: IsolationLevel) -> Verdict:
    sessions = backend.contexts(2)
    reader, writer = sessions[0], sessions[1]
    scope = backend.scope_name

    async with reader.tx_ctx.scope(scope):
        command = reader.document.command(CELL)
        x = (await command.create(CellCreate(value=10))).id
        y = (await command.create(CellCreate(value=20))).id

    seen: dict[str, int] = {}
    outcomes: dict[str, str] = {}

    async def read_x_then_y(gate: Gate) -> None:
        async with record_outcome(outcomes, "reader"):
            async with reader.tx_ctx.scope(scope, isolation=level):
                seen["x"] = (await reader.document.query(CELL).get(x)).value
                await gate.checkpoint()  # let the writer update both rows + commit
                seen["y"] = (await reader.document.query(CELL).get(y)).value

    async def update_both(gate: Gate) -> None:
        await gate.checkpoint()
        async with record_outcome(outcomes, "writer"):
            async with writer.tx_ctx.scope(scope, isolation=level):
                query = writer.document.query(CELL)
                command = writer.document.command(CELL)
                xr = await query.get(x)
                yr = await query.get(y)
                await command.update(x, xr.rev, CellUpdate(value=11))
                await command.update(y, yr.rev, CellUpdate(value=18))

    await Conductor(schedule=("writer", "reader")).run(
        {"reader": read_x_then_y, "writer": update_both}
    )

    # Skew = the reader saw the old x (10) together with the new y (18).
    return _PERMIT if (seen["x"] == 10 and seen["y"] == 18) else _PREVENT


# ....................... #
# Write skew (G2-item): two transactions read an overlapping set, then make disjoint writes that
# together break a cross-item invariant ("at least one on call"). The SI↔serializable discriminator.


def _writeskew_session(
    ctx: ExecutionContext,
    *,
    id1: UUID,
    id2: UUID,
    mine: UUID,
    level: IsolationLevel,
    scope: str,
    outcomes: dict[str, str],
    name: str,
) -> Callable[[Gate], Awaitable[None]]:
    async def session(gate: Gate) -> None:
        async with record_outcome(outcomes, name):
            async with ctx.tx_ctx.scope(scope, isolation=level):
                query = ctx.document.query(ONCALL)
                d1 = await query.get(id1)
                d2 = await query.get(id2)
                await gate.checkpoint()  # both sessions have read before either writes

                if (
                    d1.on_call and d2.on_call
                ):  # "safe" to drop mine — both still on call
                    target = d1 if mine == id1 else d2
                    await ctx.document.command(ONCALL).update(
                        mine, target.rev, OnCallUpdate(on_call=False)
                    )

                await gate.checkpoint()  # commit happens on scope exit, after this

    return session


# ....................... #


async def _run_write_skew(
    backend: ConformanceBackend,
    level: IsolationLevel,
) -> Verdict:
    sessions = backend.contexts(2)
    a, b = sessions[0], sessions[1]
    scope = backend.scope_name

    async with a.tx_ctx.scope(scope):
        command = a.document.command(ONCALL)
        id1 = (await command.create(OnCallCreate(on_call=True))).id
        id2 = (await command.create(OnCallCreate(on_call=True))).id

    outcomes: dict[str, str] = {}
    await Conductor(schedule=("A", "A", "B", "B")).run(
        {
            "A": _writeskew_session(
                a,
                id1=id1,
                id2=id2,
                mine=id1,
                level=level,
                scope=scope,
                outcomes=outcomes,
                name="A",
            ),
            "B": _writeskew_session(
                b,
                id1=id1,
                id2=id2,
                mine=id2,
                level=level,
                scope=scope,
                outcomes=outcomes,
                name="B",
            ),
        }
    )

    async with a.tx_ctx.scope(scope):
        query = a.document.query(ONCALL)
        still_on = int((await query.get(id1)).on_call) + int(
            (await query.get(id2)).on_call
        )

    # The invariant broke (nobody on call) only if both disjoint writes committed.
    return _PERMIT if still_on == 0 else _PREVENT


# ....................... #
# Lost update (P4): two transactions read a row, both write it. Forze's rev-OCC rejects the stale
# writer at every level (a strengthening; see CONTRACT_STRENGTHENINGS).


def _lost_update_session(
    ctx: ExecutionContext,
    *,
    cid: UUID,
    new_value: int,
    level: IsolationLevel,
    scope: str,
    outcomes: dict[str, str],
    name: str,
) -> Callable[[Gate], Awaitable[None]]:
    async def session(gate: Gate) -> None:
        async with record_outcome(outcomes, name):
            async with ctx.tx_ctx.scope(scope, isolation=level):
                current = await ctx.document.query(CELL).get(cid)
                await gate.checkpoint()  # both read before either writes
                await ctx.document.command(CELL).update(
                    cid, current.rev, CellUpdate(value=new_value)
                )

    return session


async def _run_lost_update(
    backend: ConformanceBackend, level: IsolationLevel
) -> Verdict:
    sessions = backend.contexts(2)
    a, b = sessions[0], sessions[1]
    scope = backend.scope_name

    async with a.tx_ctx.scope(scope):
        cid = (await a.document.command(CELL).create(CellCreate(value=0))).id

    outcomes: dict[str, str] = {}
    await Conductor(schedule=("A", "B")).run(
        {
            "A": _lost_update_session(
                a,
                cid=cid,
                new_value=1,
                level=level,
                scope=scope,
                outcomes=outcomes,
                name="A",
            ),
            "B": _lost_update_session(
                b,
                cid=cid,
                new_value=2,
                level=level,
                scope=scope,
                outcomes=outcomes,
                name="B",
            ),
        }
    )

    # An update was lost only if both writes committed; rev-OCC (or MVCC write-write) aborts one.
    return _PERMIT if "aborted" not in outcomes.values() else _PREVENT


# ....................... #


class _Rollback(Exception):
    """Sentinel used to force a writer's transaction to roll back in the dirty-read case."""


# ....................... #


async def _run_dirty_read(
    backend: ConformanceBackend,
    level: IsolationLevel,
) -> Verdict:
    sessions = backend.contexts(2)
    writer, reader = sessions[0], sessions[1]
    scope = backend.scope_name

    async with writer.tx_ctx.scope(scope):
        cid = (await writer.document.command(CELL).create(CellCreate(value=1))).id

    seen: dict[str, int] = {}

    async def roll_back_writer(gate: Gate) -> None:
        with suppress(_Rollback):
            async with writer.tx_ctx.scope(scope, isolation=level):
                current = await writer.document.query(CELL).get(cid)
                await writer.document.command(CELL).update(
                    cid, current.rev, CellUpdate(value=99)
                )
                await gate.checkpoint()  # 99 is written but not committed
                raise _Rollback()

    async def read_during_window(gate: Gate) -> None:
        await gate.checkpoint()
        async with reader.tx_ctx.scope(scope, isolation=level):
            seen["value"] = (await reader.document.query(CELL).get(cid)).value

    await Conductor(schedule=("reader", "writer")).run(
        {"writer": roll_back_writer, "reader": read_during_window}
    )

    # A dirty read = the reader observed the writer's uncommitted, later-rolled-back value (99).
    return _PERMIT if seen.get("value") == 99 else _PREVENT


# ....................... #
# Phantom (A3 / predicate read): a transaction runs a predicate scan twice and a concurrent
# transaction inserts a matching row between the two — a non-repeatable *predicate* read. The
# predicate analogue of non_repeatable_read: READ_COMMITTED's re-scan sees the insert; SNAPSHOT reads
# the as-of-begin set both times. A serializable backend prevents it the same way (the frozen
# snapshot) and may *additionally* abort the reader for having scanned a namespace a concurrent
# transaction then wrote — a deliberately conservative phantom check, not a result-sensitive one.


async def _run_phantom(backend: ConformanceBackend, level: IsolationLevel) -> Verdict:
    sessions = backend.contexts(2)
    reader, writer = sessions[0], sessions[1]
    scope = backend.scope_name

    marker, matching = _fresh_predicate()
    counts: list[int] = []
    outcomes: dict[str, str] = {}

    async def scan_twice(gate: Gate) -> None:
        async with record_outcome(outcomes, "reader"):
            async with reader.tx_ctx.scope(scope, isolation=level):
                counts.append(await reader.document.query(CELL).count(matching))
                await gate.checkpoint()  # let the writer insert a matching row + commit
                counts.append(await reader.document.query(CELL).count(matching))

    async def insert_match(gate: Gate) -> None:
        await gate.checkpoint()
        async with record_outcome(outcomes, "writer"):
            async with writer.tx_ctx.scope(scope, isolation=level):
                await writer.document.command(CELL).create(CellCreate(value=marker))

    await Conductor(schedule=("writer", "reader")).run(
        {"reader": scan_twice, "writer": insert_match}
    )

    # Phantom = the re-scan saw the inserted row (counts grew) and the read was not rejected. A
    # serializable backend prevents it by freezing the re-scan to its snapshot (counts equal) and/or
    # by aborting the reader; the abort guard runs first so it also covers a backend that shows the
    # row then aborts at commit (which counts-grew alone would mis-score as PERMITTED).
    if "aborted" in outcomes.values():
        return _PREVENT

    return _PERMIT if counts[1] > counts[0] else _PREVENT


# ....................... #
# Predicate write skew (G2): two transactions scan the same predicate, each finds it empty and
# inserts a matching row — together breaking "at most one matching row". The predicate analogue of
# write_skew: SNAPSHOT permits it (each snapshot saw zero), SERIALIZABLE prevents it (a predicate /
# phantom conflict on the scanned set, which a key-level read-write check would miss for a new row).


def _predicate_skew_session(
    ctx: ExecutionContext,
    *,
    marker: int,
    matching: QueryFilterExpression,
    level: IsolationLevel,
    scope: str,
    outcomes: dict[str, str],
    name: str,
) -> Callable[[Gate], Awaitable[None]]:
    async def session(gate: Gate) -> None:
        async with record_outcome(outcomes, name):
            async with ctx.tx_ctx.scope(scope, isolation=level):
                count = await ctx.document.query(CELL).count(matching)
                await gate.checkpoint()  # both scan before either inserts

                if (
                    count == 0
                ):  # "safe" to insert — my scan saw none of this run's marker
                    await ctx.document.command(CELL).create(CellCreate(value=marker))

                await (
                    gate.checkpoint()
                )  # commit happens on scope exit, after both decided

    return session


# ....................... #


async def _run_predicate_write_skew(
    backend: ConformanceBackend,
    level: IsolationLevel,
) -> Verdict:
    sessions = backend.contexts(2)
    a, b = sessions[0], sessions[1]
    scope = backend.scope_name

    # A fresh marker per run, so the predicate selects only rows THIS run inserts — the absolute
    # ``== 2`` check below then holds regardless of what the (possibly reused) store already contains.
    marker, matching = _fresh_predicate()
    outcomes: dict[str, str] = {}
    await Conductor(schedule=("A", "A", "B", "B")).run(
        {
            "A": _predicate_skew_session(
                a,
                marker=marker,
                matching=matching,
                level=level,
                scope=scope,
                outcomes=outcomes,
                name="A",
            ),
            "B": _predicate_skew_session(
                b,
                marker=marker,
                matching=matching,
                level=level,
                scope=scope,
                outcomes=outcomes,
                name="B",
            ),
        }
    )

    async with a.tx_ctx.scope(scope):
        matching_count = await a.document.query(CELL).count(matching)

    # The invariant broke (two rows for this run's marker) only if both disjoint inserts committed.
    return _PERMIT if matching_count == 2 else _PREVENT


# ....................... #
# Read-only transaction anomaly (Fekete et al.): three transactions where, under SNAPSHOT, a
# read-only transaction observes a state no serial order admits — SI is non-serializable even for a
# read-only transaction. Two accounts (checking X, savings Y), both 0. T3 withdraws 10 from X with a
# 1 penalty if the combined balance would go negative; T2 deposits 20 into Y; T1 (read-only) reads
# both. Interleaved so T3 reads first (snapshot X=0,Y=0) but commits last, T2 commits in between, and
# T1 reads after T2 but before T3: T1 sees X=0 with Y=20 (implying T2 < T1 < T3), yet T3 commits
# X=-11 from its stale snapshot (implying T3 < T2). No serial order yields both. SERIALIZABLE alone
# prevents it (the pivot T3 is aborted). Each participant parks at a checkpoint *before* opening its
# scope, so its snapshot is captured at an explicit schedule step — a three-transaction anomaly needs
# the snapshot *order* pinned, unlike the two-transaction cases that can safely snapshot at startup.


async def _run_read_only_anomaly(
    backend: ConformanceBackend,
    level: IsolationLevel,
) -> Verdict:
    sessions = backend.contexts(3)
    t1ctx, t2ctx, t3ctx = sessions[0], sessions[1], sessions[2]
    scope = backend.scope_name

    async with t1ctx.tx_ctx.scope(scope):
        command = t1ctx.document.command(CELL)
        x = (await command.create(CellCreate(value=0))).id  # checking
        y = (await command.create(CellCreate(value=0))).id  # savings

    seen: dict[str, int] = {}
    outcomes: dict[str, str] = {}

    async def withdraw_with_penalty(gate: Gate) -> None:  # T3 — the pivot
        await (
            gate.checkpoint()
        )  # park; T3 opens its scope (snapshot X=0,Y=0) here, before T2 deposits
        async with record_outcome(outcomes, "t3"):
            async with t3ctx.tx_ctx.scope(scope, isolation=level):
                xr = await t3ctx.document.query(CELL).get(x)
                yr = await t3ctx.document.query(CELL).get(y)
                await gate.checkpoint()  # hold open while T2 then T1 run + commit
                penalty = 1 if (xr.value + yr.value - 10) < 0 else 0
                await t3ctx.document.command(CELL).update(
                    x, xr.rev, CellUpdate(value=xr.value - 10 - penalty)
                )

    async def deposit_savings(gate: Gate) -> None:  # T2
        await (
            gate.checkpoint()
        )  # park; T2 runs after T3's snapshot, and commits before T1 reads
        async with record_outcome(outcomes, "t2"):
            async with t2ctx.tx_ctx.scope(scope, isolation=level):
                yr = await t2ctx.document.query(CELL).get(y)
                await t2ctx.document.command(CELL).update(
                    y, yr.rev, CellUpdate(value=yr.value + 20)
                )

    async def read_only(gate: Gate) -> None:  # T1 — read-only
        await gate.checkpoint()  # park; T1 runs after T2 commits, before T3 writes
        async with record_outcome(outcomes, "t1"):
            async with t1ctx.tx_ctx.scope(scope, isolation=level):
                seen["x"] = (await t1ctx.document.query(CELL).get(x)).value
                seen["y"] = (await t1ctx.document.query(CELL).get(y)).value

    await Conductor(schedule=("t3", "t2", "t1", "t3")).run(
        {"t3": withdraw_with_penalty, "t2": deposit_savings, "t1": read_only}
    )

    # The anomaly manifested iff every transaction committed and T1 observed the inconsistent pair
    # (checking 0 with savings 20). Serializable prevents it by aborting the pivot (recorded).
    anomaly = seen.get("x") == 0 and seen.get("y") == 20
    return _PERMIT if ("aborted" not in outcomes.values() and anomaly) else _PREVENT


# ....................... #
# Fresh read then update (read-committed window): a transaction opens, a concurrent transaction
# commits an update to a row, then the first reads that row FRESH and updates it. At READ_COMMITTED
# the fresh re-read sees the concurrent commit, so the update is correctly rev-guarded and must
# COMMIT (Postgres read-committed re-reads per statement) — a spurious abort here is the over-strict
# begin-anchored conflict bug. At SNAPSHOT/SERIALIZABLE the same pattern reads the stale as-of-begin
# snapshot and must ABORT (first-committer-wins on a row a concurrent transaction changed). The
# READ_COMMITTED↔SNAPSHOT discriminator on the WRITE path (non_repeatable_read is its read-path twin).


async def _run_fresh_read_update(
    backend: ConformanceBackend,
    level: IsolationLevel,
) -> Verdict:
    sessions = backend.contexts(2)
    updater, precommitter = sessions[0], sessions[1]
    scope = backend.scope_name

    async with updater.tx_ctx.scope(scope):
        cid = (await updater.document.command(CELL).create(CellCreate(value=1))).id

    outcomes: dict[str, str] = {}

    async def open_then_fresh_update(gate: Gate) -> None:
        async with record_outcome(outcomes, "updater"):
            async with updater.tx_ctx.scope(scope, isolation=level):
                # Read once to PIN the snapshot before the concurrent commit — Postgres REPEATABLE
                # READ captures its snapshot at the first statement, not at BEGIN, so a bare park here
                # would let SI see the fresh value too. Then park so the concurrent update lands.
                await updater.document.query(CELL).get(cid)
                await gate.checkpoint()
                # Re-read: RC reads through to the fresh committed row (new rev); SI/SER still see the
                # pinned as-of-begin snapshot (stale rev).
                current = await updater.document.query(CELL).get(cid)
                await updater.document.command(CELL).update(
                    cid, current.rev, CellUpdate(value=current.value + 1)
                )

    async def update_and_commit(gate: Gate) -> None:
        await gate.checkpoint()  # run only after the updater has opened its scope
        async with record_outcome(outcomes, "precommitter"):
            async with precommitter.tx_ctx.scope(scope, isolation=level):
                current = await precommitter.document.query(CELL).get(cid)
                await precommitter.document.command(CELL).update(
                    cid, current.rev, CellUpdate(value=current.value + 10)
                )

    await Conductor(schedule=("precommitter", "updater")).run(
        {"updater": open_then_fresh_update, "precommitter": update_and_commit}
    )

    # PERMITTED = the fresh-read update committed (correct at RC, no spurious abort); PREVENTED = it
    # aborted (correct at SI/SER, where it read the stale snapshot; the over-strict RC bug if here).
    return _PERMIT if outcomes.get("updater") == "committed" else _PREVENT


# ....................... #
# Lock-race driver: the two cases below race for a resource one participant holds (a duplicate key, a
# FOR UPDATE row lock). On a lock-based engine (real Postgres) the contender BLOCKS on that resource;
# the vanilla Conductor advances one participant at a time and would wedge waiting for a park the
# blocked contender can't reach. This driver instead holds → lets the contender announce its blocking
# step → commits the holder to release it, so the SAME script runs on the abort-based mock and on real
# Postgres. See the ``lock-block-vs-abort-conductor`` MECHANISM_DIVERGENCE.


async def _drive_lock_race(holder: Session, contender: Session) -> None:
    """Drive a two-participant lock race deterministically on both abort- and lock-based engines.

    The ``holder`` opens first, acquires the contested resource, and parks. The ``contender`` — let go
    only once the holder holds the resource — announces its blocking step (:meth:`Gate.arrive_blocking`)
    and runs into it: on Postgres it suspends on the holder's lock; on the mock it buffers. Either way
    the holder is then released to commit, which unblocks the contender (lock-based) or leaves it to
    conflict at its own commit (abort-based). The contested outcome is identical on both engines.
    """

    hg, cg = Gate(), Gate()

    async def drive(session: Session, gate: Gate) -> None:
        try:
            await session(gate)
        finally:
            gate.mark_done()

    tasks = [
        asyncio.create_task(drive(holder, hg)),
        asyncio.create_task(drive(contender, cg)),
    ]

    await hg.wait_parked()  # holder opened and holds the contested resource
    await cg.wait_parked()  # contender parked at its start gate (before touching the resource)
    cg.resume()  # let the contender enter its (blocking) step — do not await a re-park it can't reach
    await cg.wait_blocking()  # contender announced it is in the blocking step (or finished/errored)
    await hg.release()  # holder commits, releasing the resource / unblocking the contender

    await asyncio.gather(*tasks)


# ....................... #
# Duplicate-key insert race (unique violation): two transactions each INSERT the SAME primary key. The
# holder inserts and holds; the contender's insert of the same id BLOCKS on the unique index (real
# Postgres) or buffers (mock). Postgres raises 23505 once the holder commits; the mock conflicts at
# commit — at EVERY level the duplicate must be REJECTED, never a silent merge.


async def _run_duplicate_key_insert(
    backend: ConformanceBackend, level: IsolationLevel
) -> Verdict:
    sessions = backend.contexts(2)
    holder_ctx, contender_ctx = sessions[0], sessions[1]
    scope = backend.scope_name

    contested = UUID(
        int=next(_contested_id_seq)
    )  # one id both racers insert; fresh per run
    outcomes: dict[str, str] = {}

    def insert_session(ctx: ExecutionContext, name: str, *, is_holder: bool) -> Session:
        async def session(gate: Gate) -> None:
            if not is_holder:
                await gate.checkpoint()  # start gate: let the holder acquire the id first
            try:
                async with ctx.tx_ctx.scope(scope, isolation=level):
                    if is_holder:
                        await ctx.document.command(CELL).create(
                            CellCreate(value=1), id=contested
                        )
                        await gate.checkpoint()  # hold the id, uncommitted, until released to commit
                    else:
                        # The next insert of the same id blocks on the holder's unique index (Postgres)
                        # or buffers (mock); announce it so the driver commits the holder to release it.
                        await gate.arrive_blocking()
                        await ctx.document.command(CELL).create(
                            CellCreate(value=1), id=contested
                        )
                outcomes[name] = "committed"
            except CoreException as error:
                # A unique violation (conflict) OR a serialization failure both mean "the duplicate
                # was rejected" — the anomaly (a silent merge) did NOT occur. Any other error is a bug.
                if error.kind is ExceptionKind.CONFLICT or is_serialization_conflict(
                    error
                ):
                    outcomes[name] = "rejected"
                else:
                    raise

        return session

    await _drive_lock_race(
        insert_session(holder_ctx, "holder", is_holder=True),
        insert_session(contender_ctx, "contender", is_holder=False),
    )

    # PERMITTED = both inserts committed (a silent merge — the dangerous under-strict bug); PREVENTED
    # = the duplicate was rejected (Postgres blocks then raises 23505; the mock conflicts at commit).
    return _PERMIT if all(v == "committed" for v in outcomes.values()) else _PREVENT


# ....................... #
# SELECT ... FOR UPDATE (pessimistic lock prevents lost update): two transactions each lock the SAME
# row with a locked read, then blind-write it (no rev guard). The FOR UPDATE lock is what prevents the
# lost update at READ_COMMITTED — without it, two blind writes both commit and one is lost. On Postgres
# the contender's locked read BLOCKS until the holder commits, then re-reads the fresh committed value
# (READ COMMITTED) or serialization-aborts (SNAPSHOT/SERIALIZABLE); the mock conflicts on the double
# claim. Verdict is by the final value (was an update lost?), not by whether a transaction aborted —
# on Postgres READ COMMITTED both writers commit and no update is lost.

_FOR_UPDATE_BASE = 0
_FOR_UPDATE_DELTAS = {"holder": 1, "contender": 2}


def _for_update_session(
    ctx: ExecutionContext,
    *,
    cid: UUID,
    delta: int,
    level: IsolationLevel,
    scope: str,
    outcomes: dict[str, str],
    name: str,
    is_holder: bool,
) -> Session:
    async def session(gate: Gate) -> None:
        if not is_holder:
            await gate.checkpoint()  # start gate: let the holder take the row lock first
        async with record_outcome(outcomes, name):
            async with ctx.tx_ctx.scope(scope, isolation=level):
                if not is_holder:
                    # The locked read blocks on the holder's row lock (Postgres) or claims the same
                    # row (mock); announce it so the driver commits the holder to release it.
                    await gate.arrive_blocking()
                row = await ctx.document.query(CELL).get(cid, for_update=True)
                if is_holder:
                    await gate.checkpoint()  # hold the lock until released to commit
                # Rev-less write via the contract's bulk fast path: ``update_matching`` takes no
                # expected revision, so the revision guard cannot catch the lost update — only the
                # FOR UPDATE lock can. That isolates the lock's effect (a plain rev-guarded
                # ``update`` would be prevented by rev-OCC at every level, masking it).
                await ctx.document.command(CELL).update_matching(
                    {"$values": {"id": {"$eq": cid}}},
                    CellUpdate(value=row.value + delta),
                    return_new=False,
                )

    return session


async def _run_for_update_lost_update(
    backend: ConformanceBackend, level: IsolationLevel
) -> Verdict:
    sessions = backend.contexts(2)
    holder_ctx, contender_ctx = sessions[0], sessions[1]
    scope = backend.scope_name

    async with holder_ctx.tx_ctx.scope(scope):
        cid = (
            await holder_ctx.document.command(CELL).create(
                CellCreate(value=_FOR_UPDATE_BASE)
            )
        ).id

    outcomes: dict[str, str] = {}
    await _drive_lock_race(
        _for_update_session(
            holder_ctx,
            cid=cid,
            delta=_FOR_UPDATE_DELTAS["holder"],
            level=level,
            scope=scope,
            outcomes=outcomes,
            name="holder",
            is_holder=True,
        ),
        _for_update_session(
            contender_ctx,
            cid=cid,
            delta=_FOR_UPDATE_DELTAS["contender"],
            level=level,
            scope=scope,
            outcomes=outcomes,
            name="contender",
            is_holder=False,
        ),
    )

    async with holder_ctx.tx_ctx.scope(scope):
        final = (await holder_ctx.document.query(CELL).get(cid)).value

    # The invariant: the row equals the base plus the deltas that actually committed. A lost update
    # breaks it only when BOTH writers commit yet one write vanished (Postgres READ COMMITTED commits
    # both and preserves both — the locked re-read sees the fresh value). If a writer aborted (the mock
    # double-claim, or a Postgres serialization failure at SNAPSHOT/SERIALIZABLE) it will retry, so no
    # update was lost. PERMITTED = an update was lost; PREVENTED = the lock did its job.
    committed = {name for name, outcome in outcomes.items() if outcome == "committed"}
    expected_if_all_committed = _FOR_UPDATE_BASE + sum(_FOR_UPDATE_DELTAS.values())
    lost = len(committed) == 2 and final != expected_if_all_committed
    return _PERMIT if lost else _PREVENT


# ....................... #


BATTERY: tuple[AnomalyCase, ...] = (
    AnomalyCase(
        name="dirty_read",
        summary="A transaction reads another transaction's uncommitted (later rolled-back) write.",
        contract={_RC: _PREVENT, _SI: _PREVENT, _SER: _PREVENT},
        run=_run_dirty_read,
    ),
    AnomalyCase(
        name="non_repeatable_read",
        summary="A transaction reads a row twice and sees a concurrent commit between the reads.",
        contract={_RC: _PERMIT, _SI: _PREVENT, _SER: _PREVENT},
        run=_run_non_repeatable_read,
    ),
    AnomalyCase(
        name="read_skew",
        summary="A transaction reads one row old and a related row new (inconsistent cross-item read).",
        contract={_RC: _PERMIT, _SI: _PREVENT, _SER: _PREVENT},
        run=_run_read_skew,
    ),
    AnomalyCase(
        name="phantom",
        summary="A predicate scan run twice sees a row a concurrent transaction inserted between.",
        contract={_RC: _PERMIT, _SI: _PREVENT, _SER: _PREVENT},
        run=_run_phantom,
    ),
    AnomalyCase(
        name="write_skew",
        summary="Disjoint writes on an overlapping read set break a cross-item invariant.",
        contract={_RC: _PERMIT, _SI: _PERMIT, _SER: _PREVENT},
        run=_run_write_skew,
    ),
    AnomalyCase(
        name="predicate_write_skew",
        summary="Two predicate scans each insert a matching row, together breaking a predicate invariant.",
        contract={_RC: _PERMIT, _SI: _PERMIT, _SER: _PREVENT},
        run=_run_predicate_write_skew,
    ),
    AnomalyCase(
        name="read_only_anomaly",
        summary="A read-only transaction observes a 3-transaction state no serial order admits (SI only).",
        contract={_RC: _PERMIT, _SI: _PERMIT, _SER: _PREVENT},
        run=_run_read_only_anomaly,
    ),
    AnomalyCase(
        name="lost_update",
        summary="Two transactions read then write the same row; one update would be lost.",
        contract={_RC: _PERMIT, _SI: _PREVENT, _SER: _PREVENT},
        run=_run_lost_update,
    ),
    AnomalyCase(
        name="fresh_read_update",
        summary=(
            "A transaction reads a row after a concurrent commit, then updates it; RC re-reads fresh "
            "and commits, SI/SER abort (stale as-of-begin snapshot)."
        ),
        contract={_RC: _PERMIT, _SI: _PREVENT, _SER: _PREVENT},
        run=_run_fresh_read_update,
    ),
    AnomalyCase(
        name="duplicate_key_insert",
        summary="Two transactions insert the same primary key; the duplicate must be rejected, not merged.",
        contract={_RC: _PREVENT, _SI: _PREVENT, _SER: _PREVENT},
        run=_run_duplicate_key_insert,
        abort_engine_only=True,
    ),
    AnomalyCase(
        name="for_update_lost_update",
        summary="Two transactions lock the same row (FOR UPDATE) then blind-write it; the lock prevents the lost update.",
        contract={_RC: _PREVENT, _SI: _PREVENT, _SER: _PREVENT},
        run=_run_for_update_lost_update,
        abort_engine_only=True,
    ),
)
"""The isolation anomaly battery, weakest-discriminator first."""


# ....................... #


def expected_verdict(
    case: AnomalyCase, level: IsolationLevel, *, engine: str | None = None
) -> Verdict:
    """The verdict a correct Forze adapter should produce: the contract, overlaid with strengthenings.

    The textbook ``contract`` verdict unless a registered
    :data:`~forze_dst.conformance.divergence.CONTRACT_STRENGTHENINGS` entry overrides it — so a
    strengthening is the only sanctioned way an observed verdict may differ from the textbook.

    :param engine: The backend scope name of the leg being asserted (its ``scope_name``:
        ``"mongo"``, ``"postgres"``, …). ``None`` applies only the backend-agnostic
        strengthenings; an engine-scoped entry overlays only its own backend's oracle.
    """

    for strengthening in CONTRACT_STRENGTHENINGS:
        if strengthening.anomaly != case.name or strengthening.level != level:
            continue

        if strengthening.engine is None or strengthening.engine == engine:
            return strengthening.observed

    return case.contract[level]
