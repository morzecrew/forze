"""Crash → restart → recovery as a first-class harness scenario (S3) + the real-runtime path.

A seeded :class:`~forze_dst.CrashPolicy` kills the process at a matched port boundary: the
in-flight transaction rolls back, committed state persists, and the operation gets no chance
to compensate (a :class:`SimulatedCrash` is a ``BaseException``). The harness then restarts a
**fresh** ``ExecutionRuntime`` over the SAME persisted store — lifecycle startup runs, an
optional recovery pass redrives interrupted work — and checks the invariants against the
post-recovery world. Everything is seeded, so any recovery bug reproduces from one seed.

What the tests pin down:

* a crash between a committed write and a follow-up write leaves an orphan that **survives the
  restart** when the operation is not transactional — the recovery bug is found;
* the same operation **routed through a transaction** rolls the partial write back atomically —
  the crash leaves the store consistent;
* a :attr:`Simulation.recover` pass reconciles the orphan after restart — recovery holds;
* the run is reproducible from one seed;
* ``SimulationConfig.runtime`` drives the plain workload through the real
  ``ExecutionRuntime.scope()`` — lifecycle startup + graceful shutdown run under the simulation.
"""

from __future__ import annotations

from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler, LifecycleStep
from forze.application.execution import ExecutionContext
from forze.application.execution.lifecycle import FrozenLifecyclePlan, LifecyclePlan
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.planning import OperationPlan
from forze.application.execution.operations.registry import OperationRegistry
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_dst import ModelState, Rule, Scenario, Simulation, SimulationConfig, Strategy
from forze_dst.faults import CrashPolicy
from forze_dst.invariants import expect
from forze_dst.markers import record_event
from forze_mock import MockDepsModule

# ----------------------- #
# Domain — orders + payments: pay = create a payment, then mark the order paid.


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
        # Charge first (commits), then mark the order paid. A crash on the order update leaves
        # an orphan payment unless the whole operation is transactional.
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
        plans = dict.fromkeys(handlers, plan)
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


async def _reconcile(ctx: ExecutionContext) -> None:
    """Recovery pass: complete every charged-but-unpaid order over the persisted store."""

    payments = await ctx.document.query(PAYMENT_SPEC).find_many()
    for payment in payments.hits:
        order = await ctx.document.query(ORDER_SPEC).get(payment.order_id)
        if not order.paid:
            await ctx.document.command(ORDER_SPEC).update(
                payment.order_id, order.rev, OrderUpdate(paid=True)
            )


def _sim(*, tx_routed: bool, recover: object | None = None) -> Simulation:
    return Simulation(
        operations=_registry(tx_routed=tx_routed),
        deps=lambda: MockDepsModule(),
        observe=_observe_orphans,
        recover=recover,  # type: ignore[arg-type]
        invariants=[
            expect("orphans", lambda e: e.fields["count"] == 0, message="orphan payment")
        ],
    )


# A crash that fires on the order update — after the payment has committed.
_CRASH = CrashPolicy(surface="document_command", route="orders", op="update")


def _config(**kw: object) -> SimulationConfig:
    return SimulationConfig(
        strategy=Strategy.SCENARIO,
        act_count=1,
        concurrency=1,
        seeds=range(3),
        crash=_CRASH,
        **kw,
    )


# ....................... #


class TestCrashRestart:
    def test_orphan_survives_restart_without_transaction(self) -> None:
        # Payment committed, order update crashed → the orphan is still there after restart.
        report = _sim(tx_routed=False).run(_config(), scenario=_SCENARIO)
        assert report is not None
        assert "orphan payment" in report.format()

    def test_transaction_rolls_back_the_partial_write(self) -> None:
        # Routed through a tx, the crash rolls the whole op back — no orphan survives.
        report = _sim(tx_routed=True).run(_config(), scenario=_SCENARIO)
        assert report is None

    def test_recover_pass_reconciles_the_orphan(self) -> None:
        # The recovery pass completes the interrupted transfer over the persisted store.
        report = _sim(tx_routed=False, recover=_reconcile).run(
            _config(), scenario=_SCENARIO
        )
        assert report is None

    def test_reproducible_from_one_seed(self) -> None:
        config = _config()
        a = _sim(tx_routed=False).run(config, scenario=_SCENARIO)
        b = _sim(tx_routed=False).run(config, scenario=_SCENARIO)
        assert a is not None and b is not None
        assert a.seed == b.seed

    def test_crash_during_arrange_leaves_nothing(self) -> None:
        # The process dies while building state (serial arrange) → no order ever commits, so
        # the restart finds an empty, consistent store. Exercises the arrange-crash path.
        crash = CrashPolicy(surface="document_command", route="orders", op="create")
        report = _sim(tx_routed=False).run(
            SimulationConfig(
                strategy=Strategy.SCENARIO,
                act_count=1,
                concurrency=1,
                seeds=range(2),
                crash=crash,
            ),
            scenario=_SCENARIO,
        )
        assert report is None


# ....................... #
# The real-runtime path: drive the workload inside ExecutionRuntime.scope().


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
class _MakeThing(Handler[None, None]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> None:
        await self.ctx.document.command(THING_SPEC).create(ThingCreate())


def _thing_registry() -> OperationRegistry:
    return OperationRegistry(
        handlers={"make": lambda ctx: _MakeThing(ctx=ctx)},
        descriptors={
            "make": OperationDescriptor(
                input_type=None, output_type=None, description="x"
            )
        },
    ).freeze()


_THING_SCENARIO = Scenario(state=ModelState, act=(Rule(op="make"),))


class TestRuntimePath:
    def test_workload_runs_under_runtime_scope(self) -> None:
        # A lifecycle step records each startup/shutdown; with config.runtime the harness drives
        # the workload inside ExecutionRuntime.scope(), so both hooks run under the simulation.
        ran: list[str] = []

        async def _startup(_ctx: ExecutionContext) -> None:
            ran.append("startup")

        async def _shutdown(_ctx: ExecutionContext) -> None:
            ran.append("shutdown")

        plan: FrozenLifecyclePlan = LifecyclePlan.from_steps(
            LifecycleStep(id="probe", startup=_startup, shutdown=_shutdown)
        ).freeze()

        sim = Simulation(
            operations=_thing_registry(),
            deps=lambda: MockDepsModule(),
            lifecycle=plan,
        )
        report = sim.run(
            SimulationConfig(
                strategy=Strategy.SCENARIO,
                runtime=True,
                seeds=range(2),
                act_count=2,
                concurrency=2,
            ),
            scenario=_THING_SCENARIO,
        )

        assert report is None  # no invariants → nothing to violate
        assert "startup" in ran and "shutdown" in ran

    def test_bare_context_is_the_default(self) -> None:
        # Without the flag the lifecycle never runs (the bare-context default path).
        ran: list[str] = []

        async def _startup(_ctx: ExecutionContext) -> None:
            ran.append("startup")

        plan = LifecyclePlan.from_steps(
            LifecycleStep(id="probe", startup=_startup)
        ).freeze()

        sim = Simulation(
            operations=_thing_registry(),
            deps=lambda: MockDepsModule(),
            lifecycle=plan,
        )
        sim.run(
            SimulationConfig(
                strategy=Strategy.SCENARIO, seeds=range(1), act_count=1, concurrency=1
            ),
            scenario=_THING_SCENARIO,
        )

        assert ran == []
