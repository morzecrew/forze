"""A contended isolation-stress workload — generated operations that actually exercise the oracle.

The serializability oracle (``serializable(complete=True)``) can *detect* anti-dependency cycles and
predicate phantoms, but a random DST workload almost never *produces* them: ``state.pick`` is uniform,
so over a large pool concurrent transactions rarely touch the same key, and the default scenarios
don't race read-modify-write or scan-then-insert shapes. This module closes that gap with a small,
reusable scenario built for contention:

* a **bounded key pool** (a couple of cells + a couple of on-call rows) so concurrent transactions
  collide often;
* act rules that race the three shapes a serialization anomaly needs — **read-modify-write** on one
  cell (lost update), **write skew** over the two on-call rows (read both, drop a different one), and
  **scan-then-insert** of a marker cell (predicate write skew / phantom).

Each act operation opens its *own* transaction at a configurable :class:`IsolationLevel`, so the same
scenario run at ``SNAPSHOT`` exhibits write skew + phantoms (which the oracle then catches) and at
``SERIALIZABLE`` is prevented by the mock's SSI. The arrange phase is serial (it just seeds the pool);
the act phase is the raced, oracle-checked one. Reuses the shipped conformance documents so an app
author can copy the shape without wiring a domain.
"""

from __future__ import annotations

import random
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.execution import Handler
from forze.application.contracts.transaction import IsolationLevel
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze_dst import ModelState, Rule, Scenario
from forze_dst.conformance._models import (  # type: ignore[reportPrivateUsage]
    CELL,
    ONCALL,
    CellCreate,
    CellUpdate,
    OnCallCreate,
    OnCallUpdate,
)

# ----------------------- #

_ROUTE = "mock"
_MARKER = 999
"""The value the scan-then-insert shape scans/inserts. A fixed constant is safe: each DST run builds
fresh deps, so the store starts empty per run (no cross-run contamination)."""

_PREDICATE = {"$values": {"value": _MARKER}}


class _CellArg(BaseModel):
    cell_id: UUID


class _SkewArg(BaseModel):
    mine: UUID
    other: UUID


# ....................... #
# Arrange handlers — seed the bounded pool (serial; not part of the raced, oracle-checked phase).


@attrs.define(slots=True, kw_only=True)
class _MakeCell(Handler[None, UUID]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> UUID:
        return (await self.ctx.document.command(CELL).create(CellCreate(value=0))).id


@attrs.define(slots=True, kw_only=True)
class _MakeOnCall(Handler[None, UUID]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> UUID:
        return (
            await self.ctx.document.command(ONCALL).create(OnCallCreate(on_call=True))
        ).id


# ....................... #
# Act handlers — each opens its own transaction at ``level`` so isolation can be dialed per run.


@attrs.define(slots=True, kw_only=True)
class _RMW(Handler[_CellArg, None]):
    """Read-modify-write one cell — the lost-update / read-modify-write anti-dependency source."""

    ctx: ExecutionContext
    level: IsolationLevel

    async def __call__(self, args: _CellArg) -> None:
        async with self.ctx.tx_ctx.scope(_ROUTE, isolation=self.level):
            row = await self.ctx.document.query(CELL).get(args.cell_id)
            await self.ctx.document.command(CELL).update(
                args.cell_id, row.rev, CellUpdate(value=row.value + 1)
            )


@attrs.define(slots=True, kw_only=True)
class _WriteSkew(Handler[_SkewArg, None]):
    """Read both on-call rows; if both are on call, drop *mine* — the write-skew shape (SI permits)."""

    ctx: ExecutionContext
    level: IsolationLevel

    async def __call__(self, args: _SkewArg) -> None:
        async with self.ctx.tx_ctx.scope(_ROUTE, isolation=self.level):
            query = self.ctx.document.query(ONCALL)
            mine = await query.get(args.mine)
            other = await query.get(args.other)

            if mine.on_call and other.on_call:
                await self.ctx.document.command(ONCALL).update(
                    args.mine, mine.rev, OnCallUpdate(on_call=False)
                )


@attrs.define(slots=True, kw_only=True)
class _ScanInsert(Handler[None, None]):
    """Scan for the marker; if none yet, insert one — predicate write skew / phantom (SI permits)."""

    ctx: ExecutionContext
    level: IsolationLevel

    async def __call__(self, _args: None) -> None:
        async with self.ctx.tx_ctx.scope(_ROUTE, isolation=self.level):
            if await self.ctx.document.query(CELL).count(_PREDICATE) == 0:
                await self.ctx.document.command(CELL).create(CellCreate(value=_MARKER))


@attrs.define(slots=True, kw_only=True)
class _FreshCell(Handler[None, None]):
    """Create a brand-new cell inside its own transaction — a committed transaction touching a UNIQUE
    key, so concurrent instances never conflict. A deliberately *vacuous* workload: concurrent
    committed transactions exist, but no serialization anomaly is possible (the non-vacuity foil)."""

    ctx: ExecutionContext
    level: IsolationLevel

    async def __call__(self, _args: None) -> None:
        async with self.ctx.tx_ctx.scope(_ROUTE, isolation=self.level):
            await self.ctx.document.command(CELL).create(CellCreate(value=0))


# ....................... #


def _skew_arg(state: ModelState, rng: random.Random) -> _SkewArg:
    """Both on-call ids, with a (seed-chosen) ``mine`` — so two racers tend to drop *different* rows
    (disjoint writes over a shared read set: the write skew SI permits but serializability forbids)."""

    ids = state.pool("oncall")
    mine = rng.choice(ids)
    other = next(handle for handle in ids if handle != mine)
    return _SkewArg(mine=mine, other=other)


SHAPES = ("rmw", "write_skew", "scan_insert")
"""The contention shapes the stress scenario can race: read-modify-write, write skew, scan-insert."""


def stress_scenario(
    *, cells: int = 2, oncall: int = 2, shapes: tuple[str, ...] = SHAPES
) -> Scenario:
    """The contended isolation-stress scenario: a bounded pool, raced read-modify-write / write-skew /
    scan-insert shapes. Level-independent — the isolation level lives in the registry's handlers.
    *shapes* selects which act rules to race (all by default), so a caller can isolate one anomaly."""

    act = {
        "rmw": Rule(
            op="rmw",
            requires=("cell",),
            arg=lambda state, rng: _CellArg(cell_id=state.pick("cell", rng)),
        ),
        "write_skew": Rule(
            op="write_skew",
            requires=("oncall",),
            enabled=lambda state: state.count("oncall") >= 2,
            arg=_skew_arg,
        ),
        "scan_insert": Rule(op="scan_insert"),
    }

    return Scenario(
        state=ModelState,
        arrange=(
            *(Rule(op="make_cell", produces="cell") for _ in range(cells)),
            *(Rule(op="make_oncall", produces="oncall") for _ in range(oncall)),
        ),
        act=tuple(act[shape] for shape in shapes),
    )


def disjoint_scenario() -> Scenario:
    """A non-contended foil: each act commits a transaction that creates a *fresh* cell (unique key),
    so concurrent committed transactions exist but never conflict. ``had_isolation_conflict`` must
    report this as vacuous — the discriminator that proves the non-vacuity signal isn't trivially true."""

    return Scenario(
        state=ModelState, arrange=(), act=(Rule(op="fresh_cell"),)
    )


def _descriptor(input_type: type | None) -> OperationDescriptor:
    return OperationDescriptor(
        input_type=input_type, output_type=None, description="isolation stress"
    )


def stress_registry(level: IsolationLevel) -> OperationRegistry:
    """The operation registry for :func:`stress_scenario`, with every act transaction opened at
    *level* — so one scenario stresses any isolation level."""

    return OperationRegistry(
        handlers={
            "make_cell": lambda ctx: _MakeCell(ctx=ctx),
            "make_oncall": lambda ctx: _MakeOnCall(ctx=ctx),
            "rmw": lambda ctx: _RMW(ctx=ctx, level=level),
            "write_skew": lambda ctx: _WriteSkew(ctx=ctx, level=level),
            "scan_insert": lambda ctx: _ScanInsert(ctx=ctx, level=level),
            "fresh_cell": lambda ctx: _FreshCell(ctx=ctx, level=level),
        },
        descriptors={
            "make_cell": _descriptor(None),
            "make_oncall": _descriptor(None),
            "rmw": _descriptor(_CellArg),
            "write_skew": _descriptor(_SkewArg),
            "scan_insert": _descriptor(None),
            "fresh_cell": _descriptor(None),
        },
    ).freeze()
