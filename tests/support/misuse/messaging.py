"""M family — messaging misuse mutants: a consumer without inbox deduplication.

The workload models broker redelivery by drawing message ids from a two-element pool. The
correct consumer runs the inbox pattern — insert the message id into the inbox table first (a
redelivery conflicts there) and apply the effect **in the same transaction**; the mutant applies
the effect unconditionally, so a redelivered message is processed twice. The oracle reads port
state (handled rows per message) — see the T-family module docstring for why markers are banned.
"""

from __future__ import annotations

from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.planning import OperationPlan
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions.model import CoreException, ExceptionKind
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_dst import ModelState, Rule, Scenario, Simulation
from forze_dst.faults import CrashPolicy
from forze_dst.invariants import expect
from forze_dst.markers import record_event
from forze_dst.misuse import MisuseCase
from forze_mock import MockDepsModule

# ----------------------- #

_POOL = (0, 1)
"""The message-id pool; any act_count >= 3 redelivers some message (at-least-once delivery)."""


class InboxRow(Document):
    message: int


class InboxRowCreate(CreateDocumentCmd):
    message: int


class InboxRowRead(ReadDocument):
    message: int


class HandledRow(Document):
    message: int


class HandledRowCreate(CreateDocumentCmd):
    message: int


class HandledRowRead(ReadDocument):
    message: int


INBOX_SPEC = DocumentSpec(
    name="consumer_inbox",
    read=InboxRowRead,
    write=DocumentWriteTypes(domain=InboxRow, create_cmd=InboxRowCreate),
)
HANDLED_SPEC = DocumentSpec(
    name="handled",
    read=HandledRowRead,
    write=DocumentWriteTypes(domain=HandledRow, create_cmd=HandledRowCreate),
)


class Delivery(BaseModel):
    message: int


# ....................... #


@attrs.define(slots=True, kw_only=True)
class _ConsumeWithoutInbox(Handler[Delivery, None]):
    ctx: ExecutionContext

    async def __call__(self, args: Delivery) -> None:
        # MUTANT (M2 drop_inbox_dedup): the effect is applied on every delivery — a redelivered
        # message is processed twice.
        await self.ctx.document.command(HANDLED_SPEC).create(HandledRowCreate(message=args.message))


@attrs.define(slots=True, kw_only=True)
class _ConsumeWithInbox(Handler[Delivery, None]):
    """CORRECT: inbox-first, effect in the same transaction — a redelivery conflicts and stops,
    and a concurrent duplicate loses the inbox row and its effect together at commit."""

    ctx: ExecutionContext

    async def __call__(self, args: Delivery) -> None:
        try:
            await self.ctx.document.command(INBOX_SPEC).create(
                InboxRowCreate(message=args.message), id=UUID(int=args.message + 1)
            )
        except CoreException as error:
            if error.kind is ExceptionKind.CONFLICT:
                return  # already processed — the redelivery is a no-op
            raise

        await self.ctx.document.command(HANDLED_SPEC).create(HandledRowCreate(message=args.message))


# ....................... #

_TX_PLAN = OperationPlan().bind_tx().set_route("mock").finish(deep=False)

_CAMPAIGN_POOL = tuple(range(100, 116))
"""The de-saturated campaign pool: two draws from 16 collide with probability ≈ 1/16."""

_REDELIVERY_SCENARIO = Scenario(
    state=ModelState,
    act=(Rule(op="consume", arg=lambda _state, rng: Delivery(message=rng.choice(_POOL))),),
)
_CAMPAIGN_SCENARIO = Scenario(
    state=ModelState,
    act=(Rule(op="consume", arg=lambda _state, rng: Delivery(message=rng.choice(_CAMPAIGN_POOL))),),
)


def _case(handler_factory, *, pooled: bool = False) -> MisuseCase:  # type: ignore[no-untyped-def]
    registry = OperationRegistry(
        handlers={"consume": handler_factory},
        plans={"consume": _TX_PLAN},
        descriptors={
            "consume": OperationDescriptor(
                input_type=Delivery, output_type=None, description="Consume a delivery."
            )
        },
    ).freeze()

    pool = _CAMPAIGN_POOL if pooled else _POOL

    async def observe(ctx: ExecutionContext) -> None:
        for message in pool:
            total = await ctx.document.query(HANDLED_SPEC).count({"$values": {"message": message}})
            record_event("handled_rows", message=message, total=total)

    return MisuseCase(
        simulation=Simulation(
            operations=registry,
            deps=lambda: MockDepsModule(),
            observe=observe,
            invariants=[
                expect(
                    "handled_rows",
                    lambda e: e.fields["total"] <= 1,
                    message="a message was processed more than once",
                )
            ],
        ),
        scenario=_CAMPAIGN_SCENARIO if pooled else _REDELIVERY_SCENARIO,
    )


# ....................... #
# M1 — the dual write: state and its outbox event committed in separate transactions. The
# crash → restart → recovery scenario kills the process between the two commits; recovery then
# finds state whose event never made it out (the canonical event-driven production bug).


class Shipment(Document):
    ref: int


class ShipmentCreate(CreateDocumentCmd):
    ref: int


class ShipmentRead(ReadDocument):
    ref: int


class OutboxEvent(Document):
    ref: int


class OutboxEventCreate(CreateDocumentCmd):
    ref: int


class OutboxEventRead(ReadDocument):
    ref: int


SHIPMENT_SPEC = DocumentSpec(
    name="shipments",
    read=ShipmentRead,
    write=DocumentWriteTypes(domain=Shipment, create_cmd=ShipmentCreate),
)
OUTBOX_EVENT_SPEC = DocumentSpec(
    name="outbox_events",
    read=OutboxEventRead,
    write=DocumentWriteTypes(domain=OutboxEvent, create_cmd=OutboxEventCreate),
)


class ShipCmd(BaseModel):
    ref: int


@attrs.define(slots=True, kw_only=True)
class _Ship(Handler[ShipCmd, None]):
    """``atomic=False`` is the MUTANT (M1 outbox_outside_tx): the outbox event is published in
    its own transaction, so a crash between the commits strands state without its event."""

    ctx: ExecutionContext
    atomic: bool

    async def __call__(self, args: ShipCmd) -> None:
        if self.atomic:
            async with self.ctx.tx_ctx.scope("mock"):
                await self.ctx.document.command(SHIPMENT_SPEC).create(ShipmentCreate(ref=args.ref))
                await self.ctx.document.command(OUTBOX_EVENT_SPEC).create(
                    OutboxEventCreate(ref=args.ref)
                )
            return

        # MUTANT (M1 outbox_outside_tx): the dual write — state commits alone.
        async with self.ctx.tx_ctx.scope("mock"):
            await self.ctx.document.command(SHIPMENT_SPEC).create(ShipmentCreate(ref=args.ref))
        async with self.ctx.tx_ctx.scope("mock"):
            await self.ctx.document.command(OUTBOX_EVENT_SPEC).create(
                OutboxEventCreate(ref=args.ref)
            )


_SHIP_SCENARIO = Scenario(
    state=ModelState,
    act=(Rule(op="ship", arg=lambda _state, rng: ShipCmd(ref=rng.randrange(10**9))),),
)

_CRASH = CrashPolicy(surface="document_command", probability=0.25)


def _ship_case(*, atomic: bool) -> MisuseCase:
    registry = OperationRegistry(
        handlers={"ship": lambda ctx: _Ship(ctx=ctx, atomic=atomic)},
        descriptors={
            "ship": OperationDescriptor(input_type=ShipCmd, output_type=None, description="Ship.")
        },
    ).freeze()

    async def observe(ctx: ExecutionContext) -> None:
        async with ctx.tx_ctx.scope("mock"):
            shipments = await ctx.document.query(SHIPMENT_SPEC).count()
            events = await ctx.document.query(OUTBOX_EVENT_SPEC).count()
        record_event("dual_write", shipments=shipments, events=events)

    return MisuseCase(
        simulation=Simulation(
            operations=registry,
            deps=lambda: MockDepsModule(),
            observe=observe,
            invariants=[
                expect(
                    "dual_write",
                    lambda e: e.fields["shipments"] == e.fields["events"],
                    message="state committed without its outbox event",
                )
            ],
        ),
        scenario=_SHIP_SCENARIO,
        crash=_CRASH,
    )


def m1_dual_write_shipment() -> MisuseCase:
    return _ship_case(atomic=False)


def ctrl_outbox_in_tx() -> MisuseCase:
    return _ship_case(atomic=True)


# ....................... #


def m2_consumer_without_inbox() -> MisuseCase:
    return _case(lambda ctx: _ConsumeWithoutInbox(ctx=ctx))


def m2_consumer_without_inbox_campaign() -> MisuseCase:
    return _case(lambda ctx: _ConsumeWithoutInbox(ctx=ctx), pooled=True)


def ctrl_inbox_consumer() -> MisuseCase:
    return _case(lambda ctx: _ConsumeWithInbox(ctx=ctx))
