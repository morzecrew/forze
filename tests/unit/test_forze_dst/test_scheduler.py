"""PCT scheduler — priority-guided interleaving exploration with reproducibility.

The default random shuffle walks interleavings uniformly; PCT (Burckhardt et al. 2010)
biases toward the deep, specific orderings bugs need via task priorities + d-1 change
points, with a probabilistic depth-d guarantee. Both are seeded and reproducible. These
tests pin the determinism/reproducibility properties, that PCT explores more than one
interleaving, and that it drives the harness end-to-end.
"""

from __future__ import annotations

import asyncio
import random

import attrs
from pydantic import BaseModel

from forze.application.contracts.execution import Handler
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry

from forze_dst import ModelState, Pct, Rule, Scenario, Simulation, SimulationConfig, Strategy
from forze_dst.markers import record_event
from forze_dst.invariants import expect, no_duplicate_effect
from forze_dst.runtime import run_simulation
from forze_dst.scheduler import PCTScheduler, RandomScheduler, SystematicScheduler
from forze_mock import MockDepsModule

# ----------------------- #


async def _interleave_log(log: list[int]) -> None:
    # Three tasks each append their id twice across an await — the order is the interleaving.
    async def worker(worker_id: int) -> None:
        log.append(worker_id)
        await asyncio.sleep(0)
        log.append(worker_id)

    await asyncio.gather(*(worker(i) for i in range(3)))


def _run_with_pct(seed: int) -> list[int]:
    log: list[int] = []
    run_simulation(
        lambda: _interleave_log(log),
        seed=0,
        scheduler=PCTScheduler(random.Random(seed), depth=3, steps=8),
    )
    return log


# ....................... #


class TestRandomScheduler:
    def test_shuffles_deterministically(self) -> None:
        items = list(range(10))
        a = RandomScheduler(random.Random(1)).reorder(list(items), step=1)
        b = RandomScheduler(random.Random(1)).reorder(list(items), step=99)
        assert a == b  # step is ignored; same seed → same order
        assert sorted(a) == items  # a permutation, nothing lost


class TestPCTScheduler:
    def test_reproducible_for_a_fixed_seed(self) -> None:
        assert _run_with_pct(7) == _run_with_pct(
            7
        )  # same scheduler seed → same interleaving

    def test_explores_more_than_one_interleaving(self) -> None:
        orders = {tuple(_run_with_pct(seed)) for seed in range(30)}
        assert len(orders) > 1  # priority + change points reach distinct interleavings

    def test_reorders_ready_by_priority_with_change_point(self) -> None:
        # A hand-built scheduler with a change point at step 1 demotes the first-seen task.
        scheduler = PCTScheduler(random.Random(0), depth=2, steps=1)

        @attrs.define
        class _FakeHandle:
            task: object

            @property
            def _callback(self) -> object:
                return self  # __self__ resolves to a non-Task → treated as a plain callback

        # With no real tasks, ordering is stable (all rank -inf) — a smoke check the
        # reorder path runs and returns every handle.
        handles = [_FakeHandle(task=object()) for _ in range(3)]
        out = scheduler.reorder(list(handles), step=1)
        assert sorted(map(id, out)) == sorted(map(id, handles))


# ....................... #


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

    async def __call__(self, args: PayDTO) -> None:
        order = self.orders[args.order_id]
        if order["paid"]:
            return
        await asyncio.sleep(0)
        order["paid"] = True
        record_event("charge", order_id=args.order_id)


def _payments_simulation() -> Simulation:
    orders: dict[str, dict] = {}

    registry = OperationRegistry(
        handlers={
            "create_order": lambda _c: _CreateOrder(orders=orders),
            "pay_order": lambda _c: _PayOrder(orders=orders),
        },
        descriptors={
            "create_order": OperationDescriptor(
                input_type=None, output_type=None, description="Create."
            ),
            "pay_order": OperationDescriptor(
                input_type=PayDTO, output_type=None, description="Pay."
            ),
        },
    ).freeze()

    async def reset(_ctx: object) -> None:
        orders.clear()

    return Simulation(
        operations=registry,
        deps=lambda: MockDepsModule(),
        setup=reset,
        invariants=[no_duplicate_effect("charge", by="order_id")],
    )


def _payments_scenario() -> Scenario:
    return Scenario(
        state=ModelState,
        arrange=(Rule(op="create_order", produces="order"),),
        act=(
            Rule(
                op="pay_order",
                requires=("order",),
                arg=lambda state, rng: PayDTO(order_id=state.pick("order", rng)),
            ),
        ),
    )


class TestPCTDrivesHarness:
    def test_pct_scheduler_finds_double_charge(self) -> None:
        report = _payments_simulation().run(
            SimulationConfig(
                strategy=Strategy.SCENARIO,
                scheduler=Pct(depth=3, steps=12),
                act_count=6,
                concurrency=6,
                seeds=range(5),
            ),
            scenario=_payments_scenario(),
        )
        assert report is not None
        assert report.violations[0].invariant == "no_duplicate_effect"
        assert [op for op, _ in report.workload] == ["pay_order", "pay_order"]


# ....................... #


class TestSystematicSchedulerUnit:
    def test_choice_rotates_chosen_handle_to_front(self) -> None:
        scheduler = SystematicScheduler([2])
        out = scheduler.reorder(["a", "b", "c", "d"], step=1)
        assert out == ["c", "a", "b", "d"]  # index 2 to front, rest order kept
        assert scheduler.branching == [4]  # branching factor recorded

    def test_default_is_fifo_beyond_the_prefix(self) -> None:
        scheduler = SystematicScheduler([])  # empty prefix → choice 0 everywhere
        assert scheduler.reorder(["a", "b", "c"], step=1) == ["a", "b", "c"]

    def test_distinct_choices_yield_distinct_orders(self) -> None:
        def run(choices: list[int]) -> list[int]:
            log: list[int] = []

            async def scenario() -> None:
                async def worker(worker_id: int) -> None:
                    log.append(worker_id)

                await asyncio.gather(*(worker(i) for i in range(3)))

            run_simulation(scenario, seed=0, scheduler=SystematicScheduler(choices))
            return log

        assert run([0]) != run([2]) or run([1]) != run([0])  # control changes order


class TestDPOR:
    def test_finds_double_charge_systematically(self) -> None:
        report = _payments_simulation().run(
            SimulationConfig(
                strategy=Strategy.DPOR, act_count=3, concurrency=3, max_runs=200
            ),
            scenario=_payments_scenario(),
        )
        assert report is not None
        assert report.violations[0].invariant == "no_duplicate_effect"
        assert all(op == "pay_order" for op, _ in report.workload)

    def test_reproducible(self) -> None:
        a = _payments_simulation().run(
            SimulationConfig(
                strategy=Strategy.DPOR, act_count=3, concurrency=3, max_runs=200
            ),
            scenario=_payments_scenario(),
        )
        b = _payments_simulation().run(
            SimulationConfig(
                strategy=Strategy.DPOR, act_count=3, concurrency=3, max_runs=200
            ),
            scenario=_payments_scenario(),
        )
        assert a is not None and b is not None
        assert a.workload == b.workload
        assert a.violations[0].message == b.violations[0].message

    def test_safe_scenario_terminates_with_no_violation(self) -> None:
        # A single op with no shared state can't violate; the search must terminate (the
        # partial-order reduction + bound keep it finite) and return None.
        @attrs.define(slots=True)
        class _Noop(Handler[None, None]):
            async def __call__(self, _args: None) -> None:
                record_event("touched", ok=True)

        registry = OperationRegistry(handlers={"noop": lambda _c: _Noop()}).freeze()
        sim = Simulation(
            operations=registry,
            deps=lambda: MockDepsModule(),
            invariants=[expect("touched", lambda e: e.fields["ok"], message="never")],
        )
        scenario = Scenario(state=ModelState, act=(Rule(op="noop"),))

        report = sim.run(
            SimulationConfig(
                strategy=Strategy.DPOR, act_count=3, concurrency=3, max_runs=200
            ),
            scenario=scenario,
        )
        assert report is None
