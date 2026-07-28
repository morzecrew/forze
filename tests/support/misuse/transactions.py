"""T family — concurrency & transactions misuse mutants over a payment/reservation base.

Each factory returns a fresh :class:`~forze_dst.misuse.MisuseCase`; the mutant twins differ from
their controls by exactly one seeded misuse, marked ``MUTANT:`` at the line that carries it. The
base is the ``dst_payments`` shape: pay an order at most once, guarded by the order's ``rev``.

Authoring rule (learned the hard way): corpus oracles read **port state** (row counts via the
``observe`` hook), never trace markers — a marker recorded before a commit-time rev conflict is
stranded when the transaction rolls back, and would frame a correct control. The one exception is
T2, whose seeded misuse *is* a non-transactional external effect: there the stranded marker is
the bug being modeled, and no control uses markers at all.
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
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_dst import ModelState, Rule, Scenario, Simulation
from forze_dst.invariants import expect, no_duplicate_effect
from forze_dst.markers import record_event
from forze_dst.misuse import MisuseCase
from forze_mock import MockDepsModule

# ----------------------- #
# Domain — an order paid at most once; a payment row per charge; a per-user reservation.


class Order(Document):
    paid: bool = False


class OrderCreate(CreateDocumentCmd):
    paid: bool = False


class OrderUpdate(BaseDTO):
    paid: bool | None = None


class OrderRead(ReadDocument):
    paid: bool


class Payment(Document):
    order_id: UUID


class PaymentCreate(CreateDocumentCmd):
    order_id: UUID


class PaymentRead(ReadDocument):
    order_id: UUID


class Reservation(Document):
    guest: int


class ReservationCreate(CreateDocumentCmd):
    guest: int


class ReservationRead(ReadDocument):
    guest: int


ORDER_SPEC = DocumentSpec(
    name="orders",
    read=OrderRead,
    write=DocumentWriteTypes(domain=Order, create_cmd=OrderCreate, update_cmd=OrderUpdate),
)
PAYMENT_SPEC = DocumentSpec(
    name="payments",
    read=PaymentRead,
    write=DocumentWriteTypes(domain=Payment, create_cmd=PaymentCreate),
)
RESERVATION_SPEC = DocumentSpec(
    name="reservations",
    read=ReservationRead,
    write=DocumentWriteTypes(domain=Reservation, create_cmd=ReservationCreate),
)

ORDER_ID = UUID(int=1)
USER = 7


class PayCmd(BaseModel):
    order_id: UUID


class ReserveCmd(BaseModel):
    guest: int


# ....................... #
# Handlers — one correct shape per contrast, four seeded misuses.


@attrs.define(slots=True, kw_only=True)
class _PayRowAfterGuard(Handler[PayCmd, None]):
    """CORRECT: the charge row lands only after the rev-guarded transition — the loser's whole
    transaction (guard failure) never reaches the charge."""

    ctx: ExecutionContext

    async def __call__(self, args: PayCmd) -> None:
        order = await self.ctx.document.query(ORDER_SPEC).get(args.order_id)
        if order.paid:
            return

        await self.ctx.document.command(ORDER_SPEC).update(
            args.order_id, order.rev, OrderUpdate(paid=True)
        )
        await self.ctx.document.command(PAYMENT_SPEC).create(PaymentCreate(order_id=args.order_id))


@attrs.define(slots=True, kw_only=True)
class _PayBlind(Handler[PayCmd, None]):
    ctx: ExecutionContext

    async def __call__(self, args: PayCmd) -> None:
        order = await self.ctx.document.query(ORDER_SPEC).get(args.order_id)
        if order.paid:
            return

        # MUTANT (T1 drop_rev_guard): a blind bulk write instead of the rev-guarded update —
        # every concurrent payer "wins" the transition, so every one of them charges.
        await self.ctx.document.command(ORDER_SPEC).update_matching(
            {"$values": {"id": {"$eq": args.order_id}}},
            OrderUpdate(paid=True),
            return_new=False,
        )
        await self.ctx.document.command(PAYMENT_SPEC).create(PaymentCreate(order_id=args.order_id))


@attrs.define(slots=True, kw_only=True)
class _PayChargeBefore(Handler[PayCmd, None]):
    ctx: ExecutionContext

    async def __call__(self, args: PayCmd) -> None:
        order = await self.ctx.document.query(ORDER_SPEC).get(args.order_id)
        if order.paid:
            return

        # MUTANT (T2 effect_before_guard): the charge is an EXTERNAL side effect (a marker —
        # a call that leaves the process, not a port write) fired before the guarded
        # transition. The loser's transaction rolls its writes back; the charge already left.
        record_event("charged", order=str(args.order_id))
        await self.ctx.document.command(ORDER_SPEC).update(
            args.order_id, order.rev, OrderUpdate(paid=True)
        )


@attrs.define(slots=True, kw_only=True)
class _PayRowBeforeGuard(Handler[PayCmd, None]):
    """The charge row lands before the guarded transition — correct ONLY under a transaction."""

    ctx: ExecutionContext

    async def __call__(self, args: PayCmd) -> None:
        order = await self.ctx.document.query(ORDER_SPEC).get(args.order_id)
        if order.paid:
            return

        await self.ctx.document.command(PAYMENT_SPEC).create(PaymentCreate(order_id=args.order_id))
        await self.ctx.document.command(ORDER_SPEC).update(
            args.order_id, order.rev, OrderUpdate(paid=True)
        )


@attrs.define(slots=True, kw_only=True)
class _ReserveCheckThenAct(Handler[ReserveCmd, None]):
    ctx: ExecutionContext

    async def __call__(self, args: ReserveCmd) -> None:
        # MUTANT (T5 check_then_act): an unguarded read-check-write — two concurrent reservers
        # both see zero and both insert (TOCTOU over the aggregate).
        count = await self.ctx.document.query(RESERVATION_SPEC).count(
            {"$values": {"guest": args.guest}}
        )
        if count == 0:
            await self.ctx.document.command(RESERVATION_SPEC).create(
                ReservationCreate(guest=args.guest)
            )


@attrs.define(slots=True, kw_only=True)
class _ReserveUniqueKey(Handler[ReserveCmd, None]):
    """CORRECT: the reservation id derives from the user — the unique key closes the race."""

    ctx: ExecutionContext

    async def __call__(self, args: ReserveCmd) -> None:
        try:
            await self.ctx.document.command(RESERVATION_SPEC).create(
                ReservationCreate(guest=args.guest), id=UUID(int=args.guest)
            )
        except CoreException as error:
            if error.kind is not ExceptionKind.CONFLICT:
                raise  # a conflict means already reserved — rejected, not merged


# ....................... #
# Wiring — shared registry/scenario builders.

_TX_PLAN = OperationPlan().bind_tx().set_route("mock").finish(deep=False)

_PAY_SCENARIO = Scenario(
    state=ModelState,
    act=(Rule(op="pay", arg=lambda _state, _rng: PayCmd(order_id=ORDER_ID)),),
)
_RESERVE_SCENARIO = Scenario(
    state=ModelState,
    act=(Rule(op="reserve", arg=lambda _state, _rng: ReserveCmd(guest=USER)),),
)


def _pay_case(handler_factory, *, tx: bool, marker_oracle: bool = False) -> MisuseCase:  # type: ignore[no-untyped-def]
    registry = OperationRegistry(
        handlers={"pay": handler_factory},
        plans={"pay": _TX_PLAN} if tx else {},
        descriptors={
            "pay": OperationDescriptor(input_type=PayCmd, output_type=None, description="Pay.")
        },
    ).freeze()

    async def setup(ctx: ExecutionContext) -> None:
        await ctx.document.command(ORDER_SPEC).create(OrderCreate(), id=ORDER_ID)

    async def observe(ctx: ExecutionContext) -> None:
        total = await ctx.document.query(PAYMENT_SPEC).count({"$values": {"order_id": ORDER_ID}})
        record_event("payments", total=total)

    invariants = (
        [no_duplicate_effect("charged", by="order")]
        if marker_oracle
        else [expect("payments", lambda e: e.fields["total"] <= 1, message="charged more than once")]
    )

    return MisuseCase(
        simulation=Simulation(
            operations=registry,
            deps=lambda: MockDepsModule(),
            setup=setup,
            observe=observe,
            invariants=invariants,
        ),
        scenario=_PAY_SCENARIO,
    )


def _reserve_case(handler_factory) -> MisuseCase:  # type: ignore[no-untyped-def]
    registry = OperationRegistry(
        handlers={"reserve": handler_factory},
        plans={"reserve": _TX_PLAN},
        descriptors={
            "reserve": OperationDescriptor(
                input_type=ReserveCmd, output_type=None, description="Reserve."
            )
        },
    ).freeze()

    async def observe(ctx: ExecutionContext) -> None:
        total = await ctx.document.query(RESERVATION_SPEC).count({"$values": {"guest": USER}})
        record_event("reservations", total=total)

    return MisuseCase(
        simulation=Simulation(
            operations=registry,
            deps=lambda: MockDepsModule(),
            observe=observe,
            invariants=[
                expect("reservations", lambda e: e.fields["total"] <= 1, message="double booking")
            ],
        ),
        scenario=_RESERVE_SCENARIO,
    )


# ....................... #
# Factories — the corpus entries point here.


def t1_blind_write_payment() -> MisuseCase:
    return _pay_case(lambda ctx: _PayBlind(ctx=ctx), tx=True)


def t2_charge_before_guard() -> MisuseCase:
    return _pay_case(lambda ctx: _PayChargeBefore(ctx=ctx), tx=True, marker_oracle=True)


def t3_payment_outside_tx() -> MisuseCase:
    # MUTANT (T3 write_outside_tx): the SAME row-before-guard handler the adversarial control
    # runs safely under a transaction — with the transaction boundary removed. The loser's
    # charge row now survives its failed transition.
    return _pay_case(lambda ctx: _PayRowBeforeGuard(ctx=ctx), tx=False)


def t5_unchecked_reservation() -> MisuseCase:
    return _reserve_case(lambda ctx: _ReserveCheckThenAct(ctx=ctx))


def ctrl_row_after_guard() -> MisuseCase:
    return _pay_case(lambda ctx: _PayRowAfterGuard(ctx=ctx), tx=True)


def ctrl_row_before_guard_in_tx() -> MisuseCase:
    # Adversarial: LOOKS like effect-before-guard (the charge row lands first), but the
    # transaction covers it — the loser's row rolls back with its aborted transition.
    return _pay_case(lambda ctx: _PayRowBeforeGuard(ctx=ctx), tx=True)


def ctrl_unique_reservation() -> MisuseCase:
    return _reserve_case(lambda ctx: _ReserveUniqueKey(ctx=ctx))
