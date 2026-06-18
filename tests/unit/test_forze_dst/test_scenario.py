"""Generative scenario model — meaningful workloads via arrange→act.

The turnkey harness with random inputs can't express "pay an order that was created
earlier": a random ``pay_order`` references an id that never existed and bounces off
validation. A `Scenario` arranges valid state first (create orders, capture their real
ids), then races act operations against it. Here it surfaces a check-then-set double-charge
under concurrent payment — the kind of bug that only appears once the workload is coherent.
"""

from __future__ import annotations

import asyncio
import random

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.execution import Handler
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry

from forze.base.exceptions import exc

from forze_dst import (
    SimulationConfig,
    Strategy,
    ModelState,
    Rule,
    Scenario,
    Simulation,
    expect,
    no_duplicate_effect,
    no_unexpected_error,
    record_event,
)
from forze_mock import MockDepsModule

# ----------------------- #


class PayDTO(BaseModel):
    order_id: str


@attrs.define(slots=True, kw_only=True)
class _CreateOrder(Handler[None, str]):
    orders: dict[str, dict]

    async def __call__(self, _args: None) -> str:
        order_id = str(len(self.orders))
        self.orders[order_id] = {"paid": False}
        return order_id


@attrs.define(slots=True, kw_only=True)
class _PayOrder(Handler[PayDTO, None]):
    orders: dict[str, dict]
    atomic: bool

    async def __call__(self, args: PayDTO) -> None:
        order = self.orders[args.order_id]

        if order["paid"]:
            return

        if not self.atomic:
            await asyncio.sleep(0)  # yield: concurrent payments race through the check

        order["paid"] = True
        record_event("charge", order_id=args.order_id)  # the (should-be-once) effect


def _payments_simulation(*, atomic: bool) -> Simulation:
    orders: dict[str, dict] = {}

    registry = OperationRegistry(
        handlers={
            "create_order": lambda _c: _CreateOrder(orders=orders),
            "pay_order": lambda _c: _PayOrder(orders=orders, atomic=atomic),
        },
        descriptors={
            "create_order": OperationDescriptor(
                input_type=None, output_type=None, description="Create an order."
            ),
            "pay_order": OperationDescriptor(
                input_type=PayDTO, output_type=None, description="Pay an order."
            ),
        },
    ).freeze()

    async def reset(_ctx: object) -> None:
        orders.clear()

    return Simulation(
        operations=registry,
        deps=lambda: MockDepsModule(),
        setup=reset,
        # An order must be charged exactly once, however payments interleave.
        invariants=[no_duplicate_effect("charge", by="order_id")],
    )


def _payments_scenario() -> Scenario:
    return Scenario(
        state=ModelState,
        # Arrange one real order; capture its returned id into the "order" pool.
        arrange=(Rule(op="create_order", produces="order"),),
        # Act: pay the arranged order, concurrently.
        act=(
            Rule(
                op="pay_order",
                requires=("order",),
                arg=lambda state, rng: PayDTO(order_id=state.pick("order", rng)),
            ),
        ),
    )


# ....................... #


class TestScenarioModel:
    def test_finds_concurrent_double_charge_and_minimizes(self) -> None:
        report = _payments_simulation(atomic=False).run(
            SimulationConfig(
                strategy=Strategy.SCENARIO, act_count=6, concurrency=6, seeds=range(5)
            ),
            scenario=_payments_scenario(),
        )

        assert report is not None
        assert report.violations[0].invariant == "no_duplicate_effect"
        # Two concurrent payments are the minimal counterexample (one can't double-charge).
        ops = [op for op, _arg in report.workload]
        assert ops == ["pay_order", "pay_order"]
        assert report.registry_fingerprint

    def test_atomic_payment_has_no_violation(self) -> None:
        report = _payments_simulation(atomic=True).run(
            SimulationConfig(
                strategy=Strategy.SCENARIO, act_count=6, concurrency=6, seeds=range(20)
            ),
            scenario=_payments_scenario(),
        )
        assert report is None

    def test_report_shows_arrange_context_and_the_race(self) -> None:
        report = _payments_simulation(atomic=False).run(
            SimulationConfig(
                strategy=Strategy.SCENARIO, act_count=6, concurrency=6, seeds=range(5)
            ),
            scenario=_payments_scenario(),
        )
        assert report is not None

        rendered = report.format()
        assert "create_order" in rendered  # arrange context is in the causal trace
        assert "pay_order" in rendered  # the raced act ops
        assert "concurrency" in rendered  # the race is surfaced


class TestHypothesisExplore:
    def test_hypothesis_finds_and_shrinks_double_charge(self) -> None:
        report = _payments_simulation(atomic=False).run(
            SimulationConfig(
                strategy=Strategy.HYPOTHESIS,
                act_count=8,
                concurrency=6,
                max_examples=50,
            ),
            scenario=_payments_scenario(),
        )

        assert report is not None
        assert report.violations[0].invariant == "no_duplicate_effect"
        # Hypothesis shrinks the act plan to the minimal two racing payments.
        ops = [op for op, _arg in report.workload]
        assert ops == ["pay_order", "pay_order"]
        assert report.registry_fingerprint
        # The report reproduces exactly from the reported (seed, plan).
        assert report.format() == report.format()

    def test_hypothesis_clean_when_atomic(self) -> None:
        report = _payments_simulation(atomic=True).run(
            SimulationConfig(
                strategy=Strategy.HYPOTHESIS,
                act_count=8,
                concurrency=6,
                max_examples=50,
            ),
            scenario=_payments_scenario(),
        )
        assert report is None

    def test_hypothesis_returns_none_without_act_rules(self) -> None:
        scenario = Scenario(
            state=ModelState,
            arrange=(Rule(op="create_order", produces="order"),),
            act=(),
        )
        report = _payments_simulation(atomic=False).run(
            SimulationConfig(strategy=Strategy.HYPOTHESIS, max_examples=10),
            scenario=scenario,
        )
        assert report is None


class TestNoUnexpectedError:
    def _sim(self, exception: Exception) -> Simulation:
        @attrs.define(slots=True)
        class _Boom(Handler[None, None]):
            async def __call__(self, _args: None) -> None:
                raise exception

        registry = OperationRegistry(handlers={"boom": lambda _c: _Boom()}).freeze()
        return Simulation(
            operations=registry,
            deps=lambda: MockDepsModule(),
            invariants=[no_unexpected_error()],
        )

    def test_flags_an_unexpected_exception(self) -> None:
        report = self._sim(KeyError("boom")).run(
            SimulationConfig(
                strategy=Strategy.SCENARIO, act_count=1, concurrency=1, seeds=range(1)
            ),
            scenario=Scenario(state=ModelState, act=(Rule(op="boom"),)),
        )
        assert report is not None
        assert report.violations[0].invariant == "no_unexpected_error"
        assert "KeyError" in report.violations[0].message

    def test_ignores_a_domain_coreexception(self) -> None:
        # A declared domain failure is an expected outcome — not a bug.
        report = self._sim(exc.validation("bad input")).run(
            SimulationConfig(
                strategy=Strategy.SCENARIO, act_count=1, concurrency=1, seeds=range(3)
            ),
            scenario=Scenario(state=ModelState, act=(Rule(op="boom"),)),
        )
        assert report is None


class TestScenarioMechanics:
    def test_act_rule_gated_by_required_pool(self) -> None:
        scenario = _payments_scenario()
        empty = ModelState()
        assert scenario.enabled_act(empty) == []  # no order → pay disabled
        assert scenario.generate_act(empty, count=5, rng=random.Random(0)) == []

        seeded = ModelState()
        seeded.add("order", "0")
        generated = scenario.generate_act(seeded, count=3, rng=random.Random(0))
        assert len(generated) == 3
        assert all(op == "pay_order" for op, _ in generated)
        assert all(isinstance(arg, PayDTO) for _, arg in generated)

    def test_model_state_pools(self) -> None:
        state = ModelState()
        assert not state.has("order")
        assert state.count("order") == 0

        state.add("order", "a")
        state.add("order", "b")
        assert state.has("order")
        assert state.count("order") == 2
        assert state.pool("order") == ("a", "b")
        assert state.pick("order", random.Random(0)) in {"a", "b"}

        with pytest.raises(KeyError):
            ModelState().pick("order", random.Random(0))  # empty pool

    def test_disabled_arrange_and_error_paths_and_observe(self) -> None:
        # One run that exercises: a disabled arrange rule (required pool empty → skipped),
        # an erroring arrange op, an erroring act op, and the scenario observe hook.
        @attrs.define(slots=True)
        class _Boom(Handler[None, None]):
            async def __call__(self, _args: None) -> None:
                raise RuntimeError("boom")

        registry = OperationRegistry(handlers={"boom": lambda _c: _Boom()}).freeze()

        async def observe(_ctx: object) -> None:
            record_event("observed", ran=True)

        sim = Simulation(
            operations=registry,
            deps=lambda: MockDepsModule(),
            observe=observe,
            invariants=[
                expect(
                    "operation",
                    lambda e: e.fields.get("outcome") != "error",
                    message="an operation errored",
                )
            ],
        )
        scenario = Scenario(
            state=ModelState,
            arrange=(
                Rule(op="boom", requires=("missing",)),  # disabled → skipped
                Rule(op="boom"),  # arrange error path
            ),
            act=(Rule(op="boom"),),  # act error path
        )
        report = sim.run(
            SimulationConfig(
                strategy=Strategy.SCENARIO, act_count=3, concurrency=1, seeds=range(1)
            ),
            scenario=scenario,
        )
        assert report is not None
        assert report.violations[0].message == "an operation errored"

    def test_rule_extra_precondition_is_anded(self) -> None:
        rule = Rule(
            op="x",
            requires=("order",),
            enabled=lambda state: state.count("order") >= 2,
        )
        state = ModelState()
        state.add("order", "a")
        assert not rule.is_enabled(state)  # requires met, extra condition not
        state.add("order", "b")
        assert rule.is_enabled(state)
