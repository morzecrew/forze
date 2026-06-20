"""Deterministic simulation of *time*: a reservation that expires mid-confirm.

DST runs on a **virtual clock that fast-forwards**, and simulated I/O has **latency** — a
real downstream takes wall-clock time, so the simulator advances the virtual clock for it
(configured test-side, applied at the port boundary). That lets DST exercise time-dependent
logic without waiting, without flakiness, and *without any artificial sleep in the handlers*.

The bug is purely about time, not concurrency: ``confirm`` checks the reservation is still
valid, *then* charges it through a slow payment downstream — but that call takes longer than
the reservation's TTL, so by the time the confirmation is written the hold has expired. A
classic check-then-act-across-time mistake (the fix is to re-validate against the expected
completion time, or hold a renewable lease). The handler just makes its normal port calls;
the *Simulation* says the payment downstream is slow, and DST fast-forwards it.

Try it (from the repo root)::

    forze dst run      examples.recipes.dst_reservation_ttl.app:simulation
    forze dst topology examples.recipes.dst_reservation_ttl.app:simulation
"""

from __future__ import annotations

from datetime import datetime, timedelta
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.primitives import utcnow
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument

from forze_dst import Simulation
from forze_dst.markers import record_event
from forze_dst.invariants import expect
from forze_mock import MockDepsModule

# ----------------------- #

TTL = timedelta(minutes=5)
"""How long a reservation is held before it expires."""

AUTH_LATENCY = timedelta(minutes=10)
"""Round-trip of the payment downstream — longer than the TTL (the heart of the bug)."""


class Reservation(Document):
    expires_at: datetime
    confirmed_at: datetime | None = None


class ReservationCreate(CreateDocumentCmd):
    expires_at: datetime


class ReservationUpdate(BaseDTO):
    confirmed_at: datetime | None = None


class ReservationRead(ReadDocument):
    expires_at: datetime
    confirmed_at: datetime | None = None


class Payment(Document):
    reservation_id: UUID


class PaymentCreate(CreateDocumentCmd):
    reservation_id: UUID


class PaymentRead(ReadDocument):
    reservation_id: UUID


RESERVATION_SPEC = DocumentSpec(
    name="reservations",
    read=ReservationRead,
    write=DocumentWriteTypes(
        domain=Reservation,
        create_cmd=ReservationCreate,
        update_cmd=ReservationUpdate,
    ),
)
PAYMENT_SPEC = DocumentSpec(
    name="payments",
    read=PaymentRead,
    write=DocumentWriteTypes(domain=Payment, create_cmd=PaymentCreate),
)


class ConfirmCmd(BaseModel):
    reservation_id: UUID


# ....................... #
# Operations — plain forze handlers over ports. No DST awareness, no artificial sleeps.


@attrs.define(slots=True, kw_only=True)
class _CreateReservation(Handler[None, UUID]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> UUID:
        reservation = await self.ctx.document.command(RESERVATION_SPEC).create(
            ReservationCreate(expires_at=utcnow() + TTL)
        )
        return reservation.id


@attrs.define(slots=True, kw_only=True)
class _Confirm(Handler[ConfirmCmd, None]):
    ctx: ExecutionContext

    async def __call__(self, args: ConfirmCmd) -> None:
        reservation = await self.ctx.document.query(RESERVATION_SPEC).get(
            args.reservation_id
        )
        if reservation.confirmed_at is not None:  # already confirmed
            return

        # Checked valid *now* — but the decision is made before the charge below.
        if utcnow() >= reservation.expires_at:
            return

        # Charge through the payment downstream — an ordinary port call. The simulator models
        # this call's latency (the payment processor's round-trip), so the virtual clock
        # advances here; in production it is wall-clock minutes. The bug: the hold can expire
        # during the charge, yet we still write the confirmation afterwards.
        await self.ctx.document.command(PAYMENT_SPEC).create(
            PaymentCreate(reservation_id=args.reservation_id)
        )
        await self.ctx.document.command(RESERVATION_SPEC).update(
            args.reservation_id, reservation.rev, ReservationUpdate(confirmed_at=utcnow())
        )


registry = OperationRegistry(
    handlers={
        "create_reservation": lambda ctx: _CreateReservation(ctx=ctx),
        "confirm": lambda ctx: _Confirm(ctx=ctx),
    },
    descriptors={
        "create_reservation": OperationDescriptor(
            input_type=None, output_type=None, description="Create a held reservation."
        ),
        "confirm": OperationDescriptor(
            input_type=ConfirmCmd, output_type=None, description="Confirm a reservation."
        ),
    },
).freeze()


# ....................... #
# Simulation — test-side: auto-mocked deps, the payment downstream's latency, an observe
# hook reading final state, and the time-dependent invariant.


def _latency(surface: str | None, route: str | None, op: str) -> float:
    # The payment downstream is slow; everything else is instant.
    del surface, op
    return AUTH_LATENCY.total_seconds() if route == "payments" else 0.0


async def _observe(ctx: ExecutionContext) -> None:
    page = await ctx.document.query(RESERVATION_SPEC).find_many()
    for reservation in page.hits:
        if reservation.confirmed_at is not None:
            record_event(
                "confirm",
                on_time=reservation.confirmed_at <= reservation.expires_at,
            )


simulation = Simulation(
    operations=registry,
    deps=lambda: MockDepsModule(),
    observe=_observe,
    latency=_latency,
    invariants=[
        expect(
            "confirm",
            lambda event: event.fields["on_time"],
            message="a reservation was confirmed after it expired",
        )
    ],
)
