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
- ``non_repeatable_read`` and ``read_skew`` discriminate READ_COMMITTED from SNAPSHOT;
- ``write_skew`` discriminates SNAPSHOT from SERIALIZABLE (the headline SI↔serializable gap);
- ``lost_update`` documents the rev-OCC strengthening (prevented at every level, vs the textbook
  permitting it under READ_COMMITTED).

Phantom/predicate (G2) and the 3-transaction read-only anomaly are the next cases to add (they need
predicate scans / a third session); the harness already supports them.
"""

from __future__ import annotations

from typing import Awaitable, Callable, Mapping
from uuid import UUID

import attrs

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
                await gate.checkpoint()  # let the writer update + commit between the two reads
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
        name="write_skew",
        summary="Disjoint writes on an overlapping read set break a cross-item invariant.",
        contract={_RC: _PERMIT, _SI: _PERMIT, _SER: _PREVENT},
        run=_run_write_skew,
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
