"""WS1 checkpoint — a seeded fault interceptor over the port seam, on a *real* registry.

Proves the port-interception seam (WS1) carries a fault interceptor end-to-end against real
operations talking to real (mock-backed) ports — no bespoke per-port wrapper, no handler
instrumentation. ``PortFaultInterceptor`` raises a transient ``exc.infrastructure`` at the
matched port boundary (here the orders ``update``); ``pay`` charges (creates a payment row)
and *then* marks the order paid, so a fault on the update is a classic partial failure.

The payoff and the WS-state contract together: a **non-transactional** ``pay`` leaves an
orphan payment (DST finds the invariant violation); the **transaction-routed** ``pay``
survives the same injected fault because the faithful journal manager rolls the whole
operation back (no payment, no paid order). Faults compose with faithful transactions.
"""

from __future__ import annotations

import random
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.planning import OperationPlan
from forze.application.execution.operations.registry import OperationRegistry
from forze.domain.models import (
    BaseDTO,
    CreateDocumentCmd,
    Document,
    ReadDocument,
)
from forze_dst import ModelState, Rule, Scenario, Simulation, SimulationConfig, Strategy
from forze_dst.faults import PortFaultInterceptor
from forze_dst.invariants import expect
from forze_dst.markers import record_event
from forze_mock import MockDepsModule

# ----------------------- #
# Domain — orders + payments through the document port.


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


ORDER_SPEC = DocumentSpec(
    name="orders",
    read=OrderRead,
    write=DocumentWriteTypes(
        domain=Order, create_cmd=OrderCreate, update_cmd=OrderUpdate
    ),
)
PAYMENT_SPEC = DocumentSpec(
    name="payments",
    read=PaymentRead,
    write=DocumentWriteTypes(domain=Payment, create_cmd=PaymentCreate),
)


class PayCmd(BaseModel):
    order_id: UUID


# ....................... #
# Operations — plain forze handlers, no DST awareness.


@attrs.define(slots=True, kw_only=True)
class _CreateOrder(Handler[None, UUID]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> UUID:
        order = await self.ctx.document.command(ORDER_SPEC).create(OrderCreate())
        return order.id


@attrs.define(slots=True, kw_only=True)
class _Pay(Handler[PayCmd, None]):
    ctx: ExecutionContext

    async def __call__(self, args: PayCmd) -> None:
        order = await self.ctx.document.query(ORDER_SPEC).get(args.order_id)
        # Charge first, then mark paid — a fault injected on the orders ``update`` leaves the
        # payment behind. The op's transactionality decides whether that orphan persists.
        await self.ctx.document.command(PAYMENT_SPEC).create(
            PaymentCreate(order_id=args.order_id)
        )
        await self.ctx.document.command(ORDER_SPEC).update(
            args.order_id, order.rev, OrderUpdate(paid=True)
        )


def _registry(*, tx_routed: bool) -> OperationRegistry:
    handlers = {
        "create_order": lambda ctx: _CreateOrder(ctx=ctx),
        "pay": lambda ctx: _Pay(ctx=ctx),
    }
    descriptors = {
        "create_order": OperationDescriptor(
            input_type=None, output_type=None, description="Create an order."
        ),
        "pay": OperationDescriptor(
            input_type=PayCmd, output_type=None, description="Pay an order."
        ),
    }

    plans = {}
    if tx_routed:
        tx_plan = OperationPlan().bind_tx().set_route("mock").finish(deep=False)
        plans = dict.fromkeys(handlers, tx_plan)

    return OperationRegistry(
        handlers=handlers, plans=plans, descriptors=descriptors
    ).freeze()


# ....................... #
# Simulation — arrange an order, act a single pay; observe orphan payments.


_SCENARIO = Scenario(
    state=ModelState,
    arrange=(Rule(op="create_order", produces="order"),),
    act=(
        Rule(
            op="pay",
            requires=("order",),
            arg=lambda state, rng: PayCmd(order_id=state.pick("order", rng)),
        ),
    ),
)


async def _observe(ctx: ExecutionContext) -> None:
    # An orphan = a payment whose order was never marked paid (a partial failure that
    # the operation did not roll back).
    payments = await ctx.document.query(PAYMENT_SPEC).find_many()
    orphans = 0
    for payment in payments.hits:
        order = await ctx.document.query(ORDER_SPEC).get(payment.order_id)
        if not order.paid:
            orphans += 1
    record_event("orphans", count=orphans)


def _simulation(*, tx_routed: bool) -> Simulation:
    return Simulation(
        operations=_registry(tx_routed=tx_routed),
        deps=lambda: MockDepsModule(),
        observe=_observe,
        invariants=[
            expect(
                "orphans",
                lambda event: event.fields["count"] == 0,
                message="a payment was left without a paid order (partial failure)",
            )
        ],
        # A seeded fault on the orders ``update`` — the seam carries it to the real port.
        interceptors=lambda seed: (
            PortFaultInterceptor(
                rng=random.Random(seed),
                surface="document_command",
                route="orders",
                op="update",
                probability=1.0,
            ),
        ),
    )


# ....................... #


def test_fault_interceptor_finds_partial_failure_on_real_registry() -> None:
    # Non-transactional ``pay``: the injected fault on the update leaves the payment behind,
    # so DST finds the orphan — proving the seam carries a fault interceptor over real ports.
    report = _simulation(tx_routed=False).run(
        SimulationConfig(
            strategy=Strategy.SCENARIO, act_count=1, concurrency=1, seeds=range(3)
        ),
        scenario=_SCENARIO,
    )

    assert report is not None
    assert "without a paid order" in report.format()


def test_transaction_routed_pay_survives_the_same_injected_fault() -> None:
    # Same fault, but ``pay`` runs in a transaction: the faithful journal manager rolls the
    # whole operation back, so there is no orphan — faults compose with faithful transactions.
    report = _simulation(tx_routed=True).run(
        SimulationConfig(
            strategy=Strategy.SCENARIO, act_count=1, concurrency=1, seeds=range(3)
        ),
        scenario=_SCENARIO,
    )

    assert report is None
