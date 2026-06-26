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

import itertools
from typing import Awaitable, Callable, Mapping
from uuid import UUID

import attrs

from forze.application.contracts.querying import QueryFilterExpression
from forze.application.contracts.transaction import IsolationLevel
from forze.application.execution import ExecutionContext
from forze.testing import Conductor, Gate

from ._models import (
    CELL,
    ONCALL,
    CellCreate,
    CellUpdate,
    OnCallCreate,
    OnCallUpdate,
)
from .divergence import CONTRACT_STRENGTHENINGS
from .harness import ConformanceBackend, Verdict, record_outcome

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


# ....................... #
# Non-repeatable read (P2): a transaction reads a row twice and sees a concurrent commit between.


async def _run_non_repeatable_read(
    backend: ConformanceBackend, level: IsolationLevel
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


async def _run_write_skew(
    backend: ConformanceBackend, level: IsolationLevel
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


async def _run_dirty_read(
    backend: ConformanceBackend, level: IsolationLevel
) -> Verdict:
    sessions = backend.contexts(2)
    writer, reader = sessions[0], sessions[1]
    scope = backend.scope_name

    async with writer.tx_ctx.scope(scope):
        cid = (await writer.document.command(CELL).create(CellCreate(value=1))).id

    seen: dict[str, int] = {}

    async def roll_back_writer(gate: Gate) -> None:
        try:
            async with writer.tx_ctx.scope(scope, isolation=level):
                current = await writer.document.query(CELL).get(cid)
                await writer.document.command(CELL).update(
                    cid, current.rev, CellUpdate(value=99)
                )
                await gate.checkpoint()  # 99 is written but not committed
                raise _Rollback()
        except _Rollback:
            pass

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


async def _run_predicate_write_skew(
    backend: ConformanceBackend, level: IsolationLevel
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
    backend: ConformanceBackend, level: IsolationLevel
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
)
"""The isolation anomaly battery, weakest-discriminator first."""


# ....................... #


def expected_verdict(case: AnomalyCase, level: IsolationLevel) -> Verdict:
    """The verdict a correct Forze adapter should produce: the contract, overlaid with strengthenings.

    The textbook ``contract`` verdict unless a registered
    :data:`~forze_dst.conformance.divergence.CONTRACT_STRENGTHENINGS` entry overrides it — so a
    strengthening is the only sanctioned way an observed verdict may differ from the textbook.
    """

    for strengthening in CONTRACT_STRENGTHENINGS:
        if strengthening.anomaly == case.name and strengthening.level == level:
            return strengthening.observed

    return case.contract[level]
