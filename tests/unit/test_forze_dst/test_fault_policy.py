"""Declarative seeded faults & latency (S2) — FaultPolicy / LatencyProfile via the config.

Faults and latency are declared on ``SimulationConfig`` and compiled by the harness with
sub-seeds derived from the run's master seed — seeded BY CONSTRUCTION, no caller-supplied RNG
(closing the reproducibility footgun). A fault policy finds a partial-failure bug over a real
registry and reproduces from one seed; faults compose with faithful transactions; a latency
profile advances virtual time deterministically.
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
from forze.base.primitives import monotonic
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_dst import (
    Constant,
    FaultPolicy,
    FaultRule,
    LatencyProfile,
    LatencyRule,
    ModelState,
    Rule,
    Scenario,
    Simulation,
    SimulationConfig,
    Strategy,
    expect,
    record_event,
)
from forze_mock import MockDepsModule

# ----------------------- #
# Domain — orders + payments (faults) and a thing (latency).


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
    write=DocumentWriteTypes(domain=Order, create_cmd=OrderCreate, update_cmd=OrderUpdate),
)
PAYMENT_SPEC = DocumentSpec(
    name="payments",
    read=PaymentRead,
    write=DocumentWriteTypes(domain=Payment, create_cmd=PaymentCreate),
)


class PayCmd(BaseModel):
    order_id: UUID


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
        # Charge, then mark paid — a fault on the update leaves an orphan payment unless the
        # operation is transactional.
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
    plans = {}
    if tx_routed:
        plan = OperationPlan().bind_tx().set_route("mock").finish(deep=False)
        plans = {op: plan for op in handlers}
    return OperationRegistry(
        handlers=handlers,
        plans=plans,
        descriptors={
            "create_order": OperationDescriptor(
                input_type=None, output_type=None, description="x"
            ),
            "pay": OperationDescriptor(
                input_type=PayCmd, output_type=None, description="x"
            ),
        },
    ).freeze()


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


async def _observe_orphans(ctx: ExecutionContext) -> None:
    payments = await ctx.document.query(PAYMENT_SPEC).find_many()
    orphans = 0
    for payment in payments.hits:
        order = await ctx.document.query(ORDER_SPEC).get(payment.order_id)
        if not order.paid:
            orphans += 1
    record_event("orphans", count=orphans)


def _sim(*, tx_routed: bool) -> Simulation:
    return Simulation(
        operations=_registry(tx_routed=tx_routed),
        deps=lambda: MockDepsModule(),
        observe=_observe_orphans,
        invariants=[
            expect("orphans", lambda e: e.fields["count"] == 0, message="orphan payment")
        ],
    )


# A declarative fault: every orders ``update`` raises a transient error. No caller RNG.
_FAULTS = FaultPolicy(
    rules=(FaultRule(surface="document_command", route="orders", op="update", error=1.0),)
)


def _config(**kw: object) -> SimulationConfig:
    return SimulationConfig(
        strategy=Strategy.SCENARIO, act_count=1, concurrency=1, seeds=range(3), **kw
    )


# ....................... #


class TestFaultPolicy:
    def test_declarative_fault_finds_partial_failure(self) -> None:
        report = _sim(tx_routed=False).run(_config(faults=_FAULTS), scenario=_SCENARIO)
        assert report is not None
        assert "orphan payment" in report.format()

    def test_transaction_routed_survives_the_fault(self) -> None:
        # Faults compose with faithful transactions: the journal rolls the whole op back.
        report = _sim(tx_routed=True).run(_config(faults=_FAULTS), scenario=_SCENARIO)
        assert report is None

    def test_reproducible_from_one_seed(self) -> None:
        config = _config(faults=_FAULTS)
        a = _sim(tx_routed=False).run(config, scenario=_SCENARIO)
        b = _sim(tx_routed=False).run(config, scenario=_SCENARIO)
        assert a is not None and b is not None
        assert a.seed == b.seed


# ....................... #
# Latency — a constant per-route delay advances virtual time deterministically.


class Thing(Document):
    pass


class ThingCreate(CreateDocumentCmd):
    pass


class ThingRead(ReadDocument):
    pass


THING_SPEC = DocumentSpec(
    name="things",
    read=ThingRead,
    write=DocumentWriteTypes(domain=Thing, create_cmd=ThingCreate),
)


@attrs.define(slots=True, kw_only=True)
class _SlowCreate(Handler[None, None]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> None:
        await self.ctx.document.command(THING_SPEC).create(ThingCreate())


def _latency_sim() -> Simulation:
    registry = OperationRegistry(
        handlers={"slow": lambda ctx: _SlowCreate(ctx=ctx)},
        descriptors={
            "slow": OperationDescriptor(
                input_type=None, output_type=None, description="x"
            )
        },
    ).freeze()

    async def observe(_ctx: ExecutionContext) -> None:
        record_event("clock", elapsed=monotonic())  # virtual time after the workload

    return Simulation(
        operations=registry,
        deps=lambda: MockDepsModule(),
        observe=observe,
        invariants=[
            expect("clock", lambda e: e.fields["elapsed"] <= 0.5, message="slow")
        ],
    )


_LATENCY_SCENARIO = Scenario(state=ModelState, act=(Rule(op="slow"),))


class TestLatencyProfile:
    def test_constant_latency_advances_virtual_clock(self) -> None:
        # A 1s latency on the things create pushes virtual time past the 0.5s invariant.
        profile = LatencyProfile(
            rules=(LatencyRule(dist=Constant(1.0), route="things", op="create"),)
        )
        report = _latency_sim().run(
            _config(latency=profile), scenario=_LATENCY_SCENARIO
        )
        assert report is not None
        assert "slow" in report.format()

    def test_no_latency_leaves_the_clock_at_zero(self) -> None:
        report = _latency_sim().run(_config(), scenario=_LATENCY_SCENARIO)
        assert report is None  # the create is instant; virtual time stays at 0
