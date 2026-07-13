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
from typing import Any

import attrs
from pydantic import BaseModel

from forze.application.contracts.execution import Handler
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry

from forze_dst import (
    ModelState,
    PCTScheduler,
    Rule,
    Scenario,
    Simulation,
    SimulationConfig,
    Strategy,
)
from forze_dst.markers import record_event
from forze_dst.invariants import Violation, expect, no_duplicate_effect
from forze_dst.engines import projection
from forze_dst.engines.scenario import _expand_frontier, explore_dpor, run_scenario
from forze_dst.oracle.invariants import check
from forze_dst.oracle.recorder import History
from forze_dst.runtime import run_simulation
from forze_dst.scheduler import PCTReorderer, RandomReorderer, SystematicReorderer
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
        scheduler=PCTReorderer(random.Random(seed), depth=3, steps=8),
    )
    return log


# ....................... #


class TestRandomReorderer:
    def test_shuffles_deterministically(self) -> None:
        items = list(range(10))
        a = RandomReorderer(random.Random(1)).reorder(list(items), step=1)
        b = RandomReorderer(random.Random(1)).reorder(list(items), step=99)
        assert a == b  # step is ignored; same seed → same order
        assert sorted(a) == items  # a permutation, nothing lost


class TestPCTReorderer:
    def test_reproducible_for_a_fixed_seed(self) -> None:
        assert _run_with_pct(7) == _run_with_pct(
            7
        )  # same scheduler seed → same interleaving

    def test_explores_more_than_one_interleaving(self) -> None:
        orders = {tuple(_run_with_pct(seed)) for seed in range(30)}
        assert len(orders) > 1  # priority + change points reach distinct interleavings

    def test_reorders_ready_by_priority_with_change_point(self) -> None:
        # A hand-built scheduler with a change point at step 1 demotes the first-seen task.
        scheduler = PCTReorderer(random.Random(0), depth=2, steps=1)

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
                scheduler=PCTScheduler(depth=3, steps=12),
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


class TestSystematicReordererUnit:
    def test_choice_rotates_chosen_handle_to_front(self) -> None:
        scheduler = SystematicReorderer([2])
        out = scheduler.reorder(["a", "b", "c", "d"], step=1)
        assert out == ["c", "a", "b", "d"]  # index 2 to front, rest order kept
        assert scheduler.branching == [4]  # branching factor recorded

    def test_default_is_fifo_beyond_the_prefix(self) -> None:
        scheduler = SystematicReorderer([])  # empty prefix → choice 0 everywhere
        assert scheduler.reorder(["a", "b", "c"], step=1) == ["a", "b", "c"]

    def test_distinct_choices_yield_distinct_orders(self) -> None:
        def run(choices: list[int]) -> list[int]:
            log: list[int] = []

            async def scenario() -> None:
                async def worker(worker_id: int) -> None:
                    log.append(worker_id)

                await asyncio.gather(*(worker(i) for i in range(3)))

            run_simulation(scenario, seed=0, scheduler=SystematicReorderer(choices))
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


# ....................... #

# A schedule-sensitive violation whose ONLY witness keeps the first branch point FIFO and deviates
# at a later one: two concurrent workers each mark two phases across a yield, and the violation is
# the interleaving where worker 1 wins phase 0 (a deviation) yet worker 0 wins phase 1 — reachable
# only by holding branch 0 FIFO and diverging afterwards.
_FIFO_THEN_DEVIATE_ORDER = ((1, 0), (0, 0), (0, 1), (1, 1))


def _two_phase_order(history: History) -> tuple[tuple[int, int], ...]:
    return tuple(
        (event.fields["worker"], event.fields["phase"])
        for event in sorted(history.events, key=lambda e: e.seq)
        if event.kind == "mark"
    )


def _order_invariant(target: tuple[tuple[int, int], ...]):
    def check_order(history: History) -> list[Violation]:
        if _two_phase_order(history) == target:
            return [Violation(invariant="fifo_then_deviate", message=f"order={target}")]
        return []

    return check_order


def _two_worker_two_phase_simulation(target: tuple[tuple[int, int], ...]) -> Simulation:
    def make(worker_id: int) -> Handler[None, None]:
        @attrs.define(slots=True)
        class _Worker(Handler[None, None]):
            async def __call__(self, _args: None) -> None:
                for phase in range(2):
                    await asyncio.sleep(
                        0
                    )  # a per-phase yield ⇒ two ordered branch points
                    record_event("mark", worker=worker_id, phase=phase)

        return _Worker()

    registry = OperationRegistry(
        handlers={f"w{i}": (lambda _c, i=i: make(i)) for i in range(2)},
        descriptors={
            f"w{i}": OperationDescriptor(
                input_type=None, output_type=None, description="w"
            )
            for i in range(2)
        },
    ).freeze()

    return Simulation(
        operations=registry,
        deps=lambda: MockDepsModule(),
        invariants=[_order_invariant(target)],
    )


def _two_worker_scenario() -> Scenario:
    return Scenario(state=ModelState, act=(Rule(op="w0"), Rule(op="w1")))


def _truncating_dpor_finds(
    sim: Simulation, scenario: Scenario, workload: tuple[str, ...], *, seed: int
) -> bool:
    """Re-drive the DPOR search with the OLD *truncating* frontier expansion (the bug).

    Identical to :func:`~forze_dst.engines.scenario.explore_dpor` except the expansion is the
    pre-fix ``choices[:tick]`` truncation, so it can never emit a FIFO-then-deviate vector. Used to
    prove the scenario below is genuinely unreachable by the buggy search.
    """

    epoch = SimulationConfig().epoch
    frontier: list[tuple[int, ...]] = [()]
    visited: set[tuple[int, ...]] = set()
    seen: set[object] = set()

    while frontier:
        choices = frontier.pop()

        if choices in visited:
            continue

        visited.add(choices)
        scheduler = SystematicReorderer(choices)
        history, _ = run_scenario(
            sim,
            scenario,
            act_workload=[(op, None) for op in workload],
            act_count=len(workload),
            concurrency=len(workload),
            seed=seed,
            schedule_seed=None,
            epoch=epoch,
            scheduler=scheduler,
        )

        if check(history, sim.invariants):
            return True

        signature = projection.outcome_signature(history)

        if signature in seen:
            continue

        seen.add(signature)

        for tick, size in enumerate(scheduler.branching):  # buggy truncation
            frontier.extend((*choices[:tick], alt) for alt in range(1, size))

    return False


class TestDPORCompleteness:
    def test_frontier_expansion_reaches_fifo_then_deviate_schedule(self) -> None:
        # Two binary branch points. To deviate *first* at branch 1 while branch 0 stays FIFO the
        # search must emit (0, 1). The zero-padded expansion does; the old truncation collapsed
        # every deep deviation onto branch 0, so it could only ever emit (1,).
        branching = [2, 2]

        expanded = _expand_frontier((), branching)
        assert (
            0,
            1,
        ) in expanded  # FIFO at branch 0, deviate at branch 1 — now reachable

        # The pre-fix truncating expansion could not reach it (its whole explored corner).
        old = [
            (*(())[:tick], alt)
            for tick, size in enumerate(branching)
            for alt in range(1, size)
        ]
        assert (0, 1) not in old
        assert set(old) == {(1,)}

    def test_dpor_finds_violation_only_reachable_by_fifo_then_deviate(self) -> None:
        # dpor_seed=1 fixes the generated act workload to (w0, w1); the only violating schedule
        # keeps branch 0 FIFO and deviates later. The zero-padded search finds it; the old
        # truncating search misses it entirely.
        sim = _two_worker_two_phase_simulation(_FIFO_THEN_DEVIATE_ORDER)
        scenario = _two_worker_scenario()

        report = sim.run(
            SimulationConfig(
                strategy=Strategy.DPOR,
                act_count=2,
                concurrency=2,
                max_runs=500,
                dpor_seed=1,
            ),
            scenario=scenario,
        )

        assert report is not None
        assert report.violations[0].invariant == "fifo_then_deviate"

        # The witness keeps an earlier branch point FIFO (a 0) before its first deviation — a
        # vector the old truncating expansion could never emit.
        assert report.choices is not None
        first_deviation = next(i for i, c in enumerate(report.choices) if c != 0)
        assert first_deviation > 0
        assert report.choices[0] == 0

        # The buggy truncating search finds nothing here — this is the completeness regression.
        assert not _truncating_dpor_finds(sim, scenario, ("w0", "w1"), seed=1)

        # And the counterexample reproduces exactly from its own captured choice vector.
        replayed, _ = run_scenario(
            sim,
            scenario,
            act_workload=[("w0", None), ("w1", None)],
            act_count=2,
            concurrency=2,
            seed=1,
            schedule_seed=None,
            epoch=SimulationConfig().epoch,
            scheduler=SystematicReorderer(report.choices),
        )
        assert check(replayed, sim.invariants)

        # The rendered repro carries the DPOR strategy and the exact interleaving.
        rendered = report.format()
        assert "strategy=Strategy.DPOR" in rendered
        assert "SystematicReorderer(choices=" in rendered


# ....................... #

# The signature-pruning boundary: a violation whose ONLY witness flips the worker order at a
# *silent* race (a shared list, nothing recorded) and flips it BACK at a recorded one — two
# deviations. Both workers run the SAME op (roles assigned by arrival, so an invoke-order flip is
# an isomorphic relabeling) and the recorded marks carry no worker identity, so every
# single-deviation schedule records exactly FIFO's events: the outcome signature cannot tell them
# apart, and the pruned search discards the flip-at-A-only intermediate whose subtree holds the
# witness. Exhaustive mode (``prune=False``) expands it and finds the violation.


def _hidden_race_simulation() -> tuple[Simulation, list[int]]:
    shared: dict[str, Any] = {}
    run_counter = [0]

    @attrs.define(slots=True)
    class _Worker(Handler[None, None]):
        async def __call__(self, _args: None) -> None:
            role = shared.setdefault("arrivals", 0)
            shared["arrivals"] = (
                role + 1
            )  # role by arrival ⇒ invoke flips are isomorphic

            await asyncio.sleep(
                0
            )  # race A: silent — only shared state, nothing recorded
            order_a: list[int] = shared.setdefault("a_order", [])
            order_a.append(role)
            ahead_at_a = role == 1 and order_a[0] == 1

            await asyncio.sleep(0)  # race B: recorded — but the mark names no worker
            behind_at_b = role == 1 and shared.get("b_done") is not None
            if role == 0:
                shared["b_done"] = True
            if ahead_at_a and behind_at_b:
                record_event("mark", raced=True)
            else:
                record_event("mark")

    registry = OperationRegistry(
        handlers={"w": lambda _c: _Worker()},
        descriptors={
            "w": OperationDescriptor(input_type=None, output_type=None, description="w")
        },
    ).freeze()

    def flip_then_restore(history: History) -> list[Violation]:
        if any(e.kind == "mark" and e.fields.get("raced") for e in history.events):
            return [
                Violation(
                    invariant="flip_then_restore", message="ahead at A, behind at B"
                )
            ]
        return []

    async def reset(_ctx: object) -> None:
        shared.clear()
        run_counter[0] += 1  # one setup per run — counts explored interleavings

    sim = Simulation(
        operations=registry,
        deps=lambda: MockDepsModule(),
        setup=reset,
        invariants=[flip_then_restore],
    )
    return sim, run_counter


def _hidden_race_scenario() -> Scenario:
    return Scenario(state=ModelState, act=(Rule(op="w"), Rule(op="w")))


class TestDPORPruningBoundary:
    def test_signature_pruning_misses_a_reachable_violation(self) -> None:
        # Pruning ON (the default): the flip-at-A-only intermediate records FIFO's exact events,
        # so it is pruned — and with it the only path to the double-deviation witness.
        sim, _ = _hidden_race_simulation()
        report = explore_dpor(
            sim,
            _hidden_race_scenario(),
            act_count=2,
            concurrency=2,
            seed=0,
            max_runs=2000,
        )
        assert report is None

    def test_exhaustive_walk_finds_it_and_explores_more(self) -> None:
        sim, pruned_runs = _hidden_race_simulation()
        missed = explore_dpor(
            sim,
            _hidden_race_scenario(),
            act_count=2,
            concurrency=2,
            seed=0,
            max_runs=2000,
        )
        assert missed is None

        sim, full_runs = _hidden_race_simulation()
        report = explore_dpor(
            sim,
            _hidden_race_scenario(),
            act_count=2,
            concurrency=2,
            seed=0,
            max_runs=2000,
            prune=False,
        )
        assert report is not None
        assert report.violations[0].invariant == "flip_then_restore"
        assert (
            full_runs[0] > pruned_runs[0]
        )  # exhaustive explores strictly more schedules

        # The witness needs two deviations — the chain the pruned search cut at its first link.
        assert report.choices is not None
        assert sum(1 for choice in report.choices if choice != 0) >= 2

        # And it reproduces exactly from its captured choice vector.
        sim, _ = _hidden_race_simulation()
        replayed, _workload = run_scenario(
            sim,
            _hidden_race_scenario(),
            act_workload=[("w", None), ("w", None)],
            act_count=2,
            concurrency=2,
            seed=0,
            schedule_seed=None,
            epoch=SimulationConfig().epoch,
            scheduler=SystematicReorderer(report.choices),
        )
        assert check(replayed, sim.invariants)

    def test_config_threads_the_switch_and_defaults_to_pruning(self) -> None:
        assert (
            SimulationConfig().dpor_prune is True
        )  # pruning stays the default (fast mode)

        sim, _ = _hidden_race_simulation()
        missed = sim.run(
            SimulationConfig(
                strategy=Strategy.DPOR, act_count=2, concurrency=2, max_runs=2000
            ),
            scenario=_hidden_race_scenario(),
        )
        assert missed is None

        sim, _ = _hidden_race_simulation()
        found = sim.run(
            SimulationConfig(
                strategy=Strategy.DPOR,
                act_count=2,
                concurrency=2,
                max_runs=2000,
                dpor_prune=False,
            ),
            scenario=_hidden_race_scenario(),
        )
        assert found is not None
        assert found.violations[0].invariant == "flip_then_restore"
