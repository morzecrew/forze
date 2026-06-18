"""Unified config-driven exploration (S1) — ``Simulation.run(SimulationConfig)``.

One config-driven entrypoint replaces the strategy-specific explore methods; one master seed
drives every nondeterminism stream (schedule / faults / entropy / inputs) as independent
sub-seeds. A check-then-charge race (two concurrent pays each create a payment before the
optimistic-concurrency-guarded transition) is found and reproduced via each strategy.
"""

from __future__ import annotations

from uuid import UUID

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_dst import (
    ModelState,
    Rule,
    Scenario,
    SchedulerKind,
    Simulation,
    SimulationConfig,
    Strategy,
    expect,
    record_event,
)
from forze_mock import MockDepsModule

# ----------------------- #


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
        if order.paid:
            return
        # Non-transactional check-then-charge: the loser's rev-conflicting update raises, but
        # its payment row already persisted → a double charge.
        await self.ctx.document.command(PAYMENT_SPEC).create(
            PaymentCreate(order_id=args.order_id)
        )
        await self.ctx.document.command(ORDER_SPEC).update(
            args.order_id, order.rev, OrderUpdate(paid=True)
        )


_REGISTRY = OperationRegistry(
    handlers={
        "create_order": lambda ctx: _CreateOrder(ctx=ctx),
        "pay": lambda ctx: _Pay(ctx=ctx),
    },
    plans={},  # non-transactional, so the loser's payment is not rolled back
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


async def _observe(ctx: ExecutionContext) -> None:
    record_event("payments", total=await ctx.document.query(PAYMENT_SPEC).count())


def _sim() -> Simulation:
    return Simulation(
        operations=_REGISTRY,
        deps=lambda: MockDepsModule(),
        observe=_observe,
        invariants=[
            expect(
                "payments", lambda e: e.fields["total"] <= 1, message="double charge"
            )
        ],
    )


# ....................... #


def test_run_scenario_finds_and_reproduces() -> None:
    config = SimulationConfig(strategy=Strategy.SCENARIO, seeds=range(20), act_count=4)
    report = _sim().run(config, scenario=_SCENARIO)
    assert report is not None
    assert "double charge" in report.format()
    # Same config → same violating seed (one master seed reproduces the whole run).
    again = _sim().run(config, scenario=_SCENARIO)
    assert again is not None
    assert again.seed == report.seed


def test_run_dpor_finds() -> None:
    report = _sim().run(
        SimulationConfig(strategy=Strategy.DPOR, act_count=4, max_runs=200),
        scenario=_SCENARIO,
    )
    assert report is not None


def test_run_hypothesis_finds() -> None:
    report = _sim().run(
        SimulationConfig(strategy=Strategy.HYPOTHESIS, act_count=4, max_examples=100),
        scenario=_SCENARIO,
    )
    assert report is not None


def test_run_pct_scheduler_finds() -> None:
    report = _sim().run(
        SimulationConfig(
            strategy=Strategy.SCENARIO, scheduler=SchedulerKind.PCT, seeds=range(20)
        ),
        scenario=_SCENARIO,
    )
    assert report is not None


def test_run_fifo_is_deterministic_no_perturbation() -> None:
    config = SimulationConfig(
        strategy=Strategy.SCENARIO, scheduler=SchedulerKind.FIFO, seeds=range(5)
    )
    assert config.perturb is False
    # Whatever the FIFO outcome, it is identical across runs.
    a = _sim().run(config, scenario=_SCENARIO)
    b = _sim().run(config, scenario=_SCENARIO)
    assert (a is None) == (b is None)


def test_op_case_requires_cases() -> None:
    with pytest.raises(ValueError, match="OP_CASE"):
        _sim().run(SimulationConfig(strategy=Strategy.OP_CASE, seeds=range(1)))


def test_op_case_honors_pct_scheduler(monkeypatch: pytest.MonkeyPatch) -> None:
    # OP_CASE with scheduler=PCT must build a PCT scheduler, not silently fall back to the
    # random schedule-seed shuffle.
    from forze_dst import OperationCase, harness

    calls: list[object] = []
    real = harness.pct_scheduler_factory

    def spy(**kwargs: object) -> object:
        calls.append(kwargs)
        return real(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(harness, "pct_scheduler_factory", spy)

    _sim().run(
        SimulationConfig(
            strategy=Strategy.OP_CASE,
            scheduler=SchedulerKind.PCT,
            seeds=range(2),
            count=2,
            concurrency=2,
        ),
        cases=[OperationCase(op="create_order")],
    )

    assert calls, "OP_CASE with scheduler=PCT built no PCT scheduler"


def test_hypothesis_honors_pct_scheduler(monkeypatch: pytest.MonkeyPatch) -> None:
    # HYPOTHESIS with scheduler=PCT must build a PCT scheduler (only DPOR ignores the scheduler).
    from forze_dst import harness

    calls: list[object] = []
    real = harness.pct_scheduler_factory

    def spy(**kwargs: object) -> object:
        calls.append(kwargs)
        return real(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(harness, "pct_scheduler_factory", spy)

    _sim().run(
        SimulationConfig(
            strategy=Strategy.HYPOTHESIS,
            scheduler=SchedulerKind.PCT,
            act_count=4,
            max_examples=20,
        ),
        scenario=_SCENARIO,
    )

    assert calls, "HYPOTHESIS with scheduler=PCT built no PCT scheduler"
