"""D family, clock seam — HLC misuse: the unmerged remote timestamp and the raw wall clock.

Two "nodes" share one virtual (or real) time source but stamp with different *disciplines*; the
fast node's clock runs a fixed skew ahead (clock skew is workload data here, not a time-source
hack, so the same provocation runs unchanged on a real backend). D4: a relay derives an event
from a received one but stamps it with its local reading only — never merging the remote
timestamp — so the causal successor sorts *below* its cause whenever the producer's clock is
ahead. D5: an appender stamps an ordering-critical stream with the raw wall reading instead of a
floor-respecting clock, so a fast-node append followed by a true-clock append runs the stream
backwards. The correct twins run the identical workloads with the HLC merge rule (max with the
observed timestamp, then advance) — the one-line discipline the mutants drop. Stamps persist as
packed integers in rows (port state), so both defects transfer as final-state reads.
"""

from __future__ import annotations

from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions.model import CoreException, ExceptionKind
from forze.base.primitives import HlcTimestamp, current_time_source
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_dst import ModelState, Rule, Scenario, Simulation
from forze_dst.invariants import expect
from forze_dst.markers import record_event
from forze_dst.misuse import MisuseCase
from forze_mock import MockDepsModule

# ----------------------- #


class EventRow(Document):
    stream: int
    seq: int
    stamp: int
    cause: int


class EventRowCreate(CreateDocumentCmd):
    stream: int
    seq: int
    stamp: int
    cause: int


class EventRowRead(ReadDocument):
    stream: int
    seq: int
    stamp: int
    cause: int


EVENT_SPEC = DocumentSpec(
    name="event_log",
    read=EventRowRead,
    write=DocumentWriteTypes(domain=EventRow, create_cmd=EventRowCreate),
)

STREAM = 1

FAST_SKEW_MS = 3_600_000
"""The fast node's clock skew (+1h) — far above any run's real or virtual elapsed time, so a
missing merge manifests deterministically on the mock and on a real backend alike."""


def wall_ms() -> int:
    return int(current_time_source().now().timestamp() * 1000)


def pack_physical(physical_ms: int) -> int:
    return HlcTimestamp(physical_ms=physical_ms, logical=0).pack()


class EmitCmd(BaseModel):
    stream: int


class RelayCmd(BaseModel):
    stream: int


class AppendCmd(BaseModel):
    stream: int
    fast: bool


# ....................... #
# D4 — ignore_remote_hlc: the relay stamps its derived event without merging the cause's
# timestamp.


@attrs.define(slots=True, kw_only=True)
class _Emit(Handler[EmitCmd, None]):
    """The fast producer: a root event stamped from its skewed local clock (cause = 0)."""

    ctx: ExecutionContext

    async def __call__(self, args: EmitCmd) -> None:
        async with self.ctx.tx_ctx.scope("mock"):
            seq = await self.ctx.document.query(EVENT_SPEC).count(
                {"$values": {"stream": args.stream}}
            )
            await self.ctx.document.command(EVENT_SPEC).create(
                EventRowCreate(
                    stream=args.stream,
                    seq=seq + 1,
                    stamp=pack_physical(wall_ms() + FAST_SKEW_MS),
                    cause=0,
                )
            )


@attrs.define(slots=True, kw_only=True)
class _Relay(Handler[RelayCmd, None]):
    """``merge=False`` is the MUTANT (D4 ignore_remote_hlc): the derived event is stamped from
    the local wall reading only, never lifted above the received timestamp."""

    ctx: ExecutionContext
    merge: bool

    async def __call__(self, args: RelayCmd) -> None:
        async with self.ctx.tx_ctx.scope("mock"):
            roots = await self.ctx.document.query(EVENT_SPEC).find_many(
                {"$values": {"stream": args.stream, "cause": {"$eq": 0}}}
            )
            if not roots.hits:
                return  # nothing to relay yet
            cause = max(root.stamp for root in roots.hits)

            local = pack_physical(wall_ms())
            if self.merge:
                # The HLC merge rule: the derived stamp must exceed everything observed.
                stamp = max(local, cause + 1)
            else:
                # MUTANT (D4 ignore_remote_hlc): local reading only — with the producer's
                # clock ahead, the causal successor sorts BELOW its cause.
                stamp = local

            seq = await self.ctx.document.query(EVENT_SPEC).count(
                {"$values": {"stream": args.stream}}
            )
            await self.ctx.document.command(EVENT_SPEC).create(
                EventRowCreate(stream=args.stream, seq=seq + 1, stamp=stamp, cause=cause)
            )


_RELAY_SCENARIO = Scenario(
    state=ModelState,
    act=(
        Rule(op="emit", arg=lambda _state, _rng: EmitCmd(stream=STREAM)),
        Rule(op="relay", arg=lambda _state, _rng: RelayCmd(stream=STREAM)),
    ),
)


def _relay_case(*, merge: bool) -> MisuseCase:
    registry = OperationRegistry(
        handlers={
            "emit": lambda ctx: _Emit(ctx=ctx),
            "relay": lambda ctx: _Relay(ctx=ctx, merge=merge),
        },
        descriptors={
            "emit": OperationDescriptor(
                input_type=EmitCmd, output_type=None, description="Emit a root event."
            ),
            "relay": OperationDescriptor(
                input_type=RelayCmd, output_type=None, description="Relay the latest root."
            ),
        },
    ).freeze()

    async def observe(ctx: ExecutionContext) -> None:
        async with ctx.tx_ctx.scope("mock"):
            rows = await ctx.document.query(EVENT_SPEC).find_many(
                {"$values": {"stream": STREAM, "cause": {"$gt": 0}}}
            )
        for row in rows.hits:
            record_event("causal_stamp", seq=row.seq, stamp=row.stamp, cause=row.cause)

    return MisuseCase(
        simulation=Simulation(
            operations=registry,
            deps=lambda: MockDepsModule(),
            observe=observe,
            invariants=[
                expect(
                    "causal_stamp",
                    lambda e: e.fields["stamp"] > e.fields["cause"],
                    message="a derived event was stamped at or below its cause "
                    "(remote HLC never merged)",
                )
            ],
        ),
        scenario=_RELAY_SCENARIO,
    )


# ....................... #
# D5 — nonmonotonic_clock: the append stamps an ordering-critical stream from the raw wall
# reading; a fast-node append followed by a true-clock append runs the stream backwards.


@attrs.define(slots=True, kw_only=True)
class _Append(Handler[AppendCmd, None]):
    """``floored=False`` is the MUTANT (D5 nonmonotonic_clock): the stamp is the node's raw
    wall reading; the correct twin lifts it above the stream's persisted high-water mark.

    Both twins guard the append itself with a seq-derived unique id (a concurrent duplicate
    conflicts and backs off), so the seeded difference is the stamp discipline alone — without
    the guard the *control's* read-max-write would itself race, which its clean band caught.
    """

    ctx: ExecutionContext
    floored: bool

    async def __call__(self, args: AppendCmd) -> None:
        try:
            async with self.ctx.tx_ctx.scope("mock"):
                rows = await self.ctx.document.query(EVENT_SPEC).find_many(
                    {"$values": {"stream": args.stream}}
                )
                seq = len(rows.hits) + 1
                wall = pack_physical(wall_ms() + (FAST_SKEW_MS if args.fast else 0))

                if self.floored:
                    # The floor-respecting discipline: never stamp at or below what the
                    # stream has already recorded.
                    high_water = max((row.stamp for row in rows.hits), default=0)
                    stamp = max(wall, high_water + 1)
                else:
                    # MUTANT (D5 nonmonotonic_clock): the raw wall reading where ordering
                    # matters — a slower node's append lands below the fast node's.
                    stamp = wall

                await self.ctx.document.command(EVENT_SPEC).create(
                    EventRowCreate(stream=args.stream, seq=seq, stamp=stamp, cause=0),
                    id=UUID(int=70000 + args.stream * 100 + seq),
                )
        except CoreException as error:
            if error.kind is not ExceptionKind.CONFLICT:
                raise  # a concurrent append won this seq — back off


_APPEND_SCENARIO = Scenario(
    state=ModelState,
    act=(
        Rule(
            op="append",
            arg=lambda _state, rng: AppendCmd(stream=STREAM, fast=rng.choice((True, False))),
        ),
    ),
)


def _append_case(*, floored: bool) -> MisuseCase:
    registry = OperationRegistry(
        handlers={"append": lambda ctx: _Append(ctx=ctx, floored=floored)},
        descriptors={
            "append": OperationDescriptor(
                input_type=AppendCmd, output_type=None, description="Append to the stream."
            )
        },
    ).freeze()

    async def observe(ctx: ExecutionContext) -> None:
        async with ctx.tx_ctx.scope("mock"):
            rows = await ctx.document.query(EVENT_SPEC).find_many(
                {"$values": {"stream": STREAM}}
            )
        ordered = sorted(rows.hits, key=lambda row: row.seq)
        for earlier, later in zip(ordered, ordered[1:], strict=False):
            record_event(
                "stream_order", earlier=earlier.stamp, later=later.stamp, seq=later.seq
            )

    return MisuseCase(
        simulation=Simulation(
            operations=registry,
            deps=lambda: MockDepsModule(),
            observe=observe,
            invariants=[
                expect(
                    "stream_order",
                    lambda e: e.fields["later"] > e.fields["earlier"],
                    message="the stream's stamps ran backwards (raw wall clock where "
                    "ordering matters)",
                )
            ],
        ),
        scenario=_APPEND_SCENARIO,
    )


# ....................... #


def d4_unmerged_remote_hlc() -> MisuseCase:
    return _relay_case(merge=False)


def ctrl_merged_relay() -> MisuseCase:
    return _relay_case(merge=True)


def d5_wall_clock_ordering() -> MisuseCase:
    return _append_case(floored=False)


def ctrl_floored_append() -> MisuseCase:
    return _append_case(floored=True)
