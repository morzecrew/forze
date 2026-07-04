"""SCENARIO engines — generative arrange→act exploration, plus its Hypothesis and DPOR variants.

The model-based strategy: arrange valid state serially (capturing real ids), then sample enabled
act calls and run them concurrently under perturbation. Three search frontends share one runner
(:func:`run_scenario`): :func:`explore` (per-seed sweep + greedy act minimization), :func:`explore_hypothesis`
(Hypothesis generates + shrinks the ``(seed, act-plan)`` space), and :func:`explore_dpor` (systematic
interleaving search over one fixed workload, pruned by observable-effect equivalence). Logic only —
the run substrate lives in :mod:`forze_dst.engines.context`.
"""

from __future__ import annotations

import asyncio
import random
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Sequence

from forze.application.execution import ExecutionContext
from forze.base.primitives import derive_seed
from forze_dst.engines import base, context, projection
from forze_dst.faults import SimulatedCrash
from forze_dst.oracle import ViolationReport
from forze_dst.oracle.invariants import check
from forze_dst.oracle.recorder import History, Recorder, record_event
from forze_dst.runtime import run_simulation
from forze_dst.scenario import Scenario
from forze_dst.scheduler import Reorderer, SystematicReorderer
from forze_dst.time_source import DEFAULT_EPOCH

if TYPE_CHECKING:
    from forze_dst.harness import Simulation

# ----------------------- #


def run_scenario(
    sim: "Simulation",
    scenario: Scenario,
    *,
    act_workload: Sequence[tuple[str, Any]] | None,
    act_count: int,
    concurrency: int,
    seed: int,
    schedule_seed: int | None,
    epoch: datetime,
    act_plan: Sequence[int] | None = None,
    scheduler: Reorderer | None = None,
) -> tuple[History, list[tuple[str, Any]]]:
    """Run a scenario: arrange serially, then act concurrently.

    The act workload comes from, in precedence: *act_workload* (concrete calls replayed, for
    minimization), *act_plan* (act-rule indices to fire, built post-arrange — the Hypothesis-driven
    path; disabled rules are skipped), else generated from the arranged state. Returns the recorded
    history and the act workload that ran.
    """

    recorder = Recorder(seed=seed)
    generated: list[tuple[str, Any]] = []

    async def driver() -> None:
        nonlocal generated

        async with context.execution_context(sim, derive_seed(seed, "fault")) as ctx:
            if sim.setup is not None:
                await sim.setup(ctx)

            rng = random.Random(derive_seed(seed, "input"))  # nosec B311
            state = scenario.state()

            # Arrange: serial, real ids captured into the model. Negative call ids keep arrange
            # spans distinct from (and never confused as concurrent with) act.
            for index, rule in enumerate(scenario.arrange):
                if not rule.is_enabled(state):
                    continue

                arg = rule.arg(state, rng)
                ok, result = await context.run_arrange_call(
                    sim, ctx, call_id=-(index + 1), op=rule.op, arg=arg
                )

                if ok and rule.produces is not None:
                    state.add(rule.produces, rule.capture(result))

            if act_workload is not None:
                generated = list(act_workload)
            elif act_plan is not None:
                generated = [
                    (rule.op, rule.arg(state, rng))
                    for index in act_plan
                    if (rule := scenario.act[index]).is_enabled(state)
                ]
            else:
                generated = scenario.generate_act(state, act_count, rng)

            semaphore = asyncio.Semaphore(concurrency)
            await asyncio.gather(
                *(
                    context.run_call(sim, ctx, semaphore, call_id=index, op=op, arg=arg)
                    for index, (op, arg) in enumerate(generated)
                )
            )

            if sim.observe is not None:
                await sim.observe(ctx)

            projection.fold_runtime_trace(ctx)

    context.run_recording(
        recorder,
        lambda: run_simulation(
            driver,
            seed=derive_seed(seed, "entropy"),
            schedule_seed=schedule_seed,
            epoch=epoch,
            scheduler=scheduler,
            latency=context.latency_for(sim, seed),
        ),
    )

    return recorder.history, generated


# ....................... #


async def drive_act(
    sim: "Simulation",
    ctx: ExecutionContext,
    generated: Sequence[tuple[str, Any]],
    *,
    concurrency: int,
) -> bool:
    """Run the act workload concurrently; return whether a :class:`SimulatedCrash` fired.

    On a crash the surviving in-flight tasks are cancelled — the process *dies*, so no sibling
    operation keeps running into the restart phase (they share the loop).
    """

    semaphore = asyncio.Semaphore(concurrency)
    tasks = [
        asyncio.ensure_future(
            context.run_call(sim, ctx, semaphore, call_id=index, op=op, arg=arg)
        )
        for index, (op, arg) in enumerate(generated)
    ]

    try:
        await asyncio.gather(*tasks)
        return False

    except SimulatedCrash:
        record_event("crash", phase="act")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        return True


# ....................... #


def attempt(
    sim: "Simulation",
    scenario: Scenario,
    *,
    act_count: int,
    concurrency: int,
    seed: int,
    perturb: bool,
    epoch: datetime,
    scheduler_factory: Callable[[int], Reorderer | None] | None = None,
) -> ViolationReport | None:
    """Run one seed's scenario; on a violation, minimize the act phase and report."""

    schedule_seed = derive_seed(seed, "schedule") if perturb else None

    def run(act: Sequence[tuple[str, Any]] | None) -> History:
        # Fresh scheduler per run (PCT is stateful): the initial run, every minimization
        # predicate, and the final replay must all explore the SAME schedule, or a counterexample
        # minimized against a mutated interleaving fails to reproduce from the reported seed.
        history, _ = run_scenario(
            sim,
            scenario,
            act_workload=act,
            act_count=act_count,
            concurrency=concurrency,
            seed=seed,
            schedule_seed=schedule_seed,
            epoch=epoch,
            scheduler=base.scheduler_for(seed, scheduler_factory),
        )
        return history

    def run_initial() -> tuple[History, Sequence[tuple[str, Any]]]:
        # The initial run samples + returns the act workload; minimization reduces the act phase
        # only (arrange is replayed identically, so captured act calls keep valid handles).
        history, act_workload = run_scenario(
            sim,
            scenario,
            act_workload=None,
            act_count=act_count,
            concurrency=concurrency,
            seed=seed,
            schedule_seed=schedule_seed,
            epoch=epoch,
            scheduler=base.scheduler_for(seed, scheduler_factory),
        )
        return history, act_workload

    return base.attempt_and_minimize(
        sim,
        seed=seed,
        schedule_seed=schedule_seed,
        run_initial=run_initial,
        run_subset=run,
        format_workload=tuple,
    )


# ....................... #


def explore(
    sim: "Simulation",
    scenario: Scenario,
    *,
    act_count: int = 20,
    concurrency: int = 4,
    seeds: Sequence[int],
    perturb: bool = True,
    epoch: datetime = DEFAULT_EPOCH,
    scheduler_factory: Callable[[int], Reorderer | None] | None = None,
) -> ViolationReport | None:
    """Drive a generative :class:`Scenario` per seed; on a violation, minimize + report.

    Each seed arranges valid state (serially, capturing real ids), then samples *act_count* enabled
    act calls and runs them concurrently under perturbation. The first violating seed's act phase is
    minimized to a 1-minimal set that still fails; arrange stays fixed. The report carries the seed,
    minimized act workload, full recorded history (arrange + act), and the registry fingerprint.

    *scheduler_factory* (e.g. :func:`forze_dst.scheduler.pct_reorderer_factory`) supplies a per-seed
    interleaving scheduler — PCT in place of the default uniform shuffle, to hunt deep interleavings
    with a better per-run probability.
    """

    return base.explore_seeds(
        seeds,
        lambda seed: attempt(
            sim,
            scenario,
            act_count=act_count,
            concurrency=concurrency,
            seed=seed,
            perturb=perturb,
            epoch=epoch,
            scheduler_factory=scheduler_factory,
        ),
    )


# ....................... #


def explore_hypothesis(
    sim: "Simulation",
    scenario: Scenario,
    *,
    max_act: int = 20,
    concurrency: int = 4,
    perturb: bool = True,
    epoch: datetime = DEFAULT_EPOCH,
    max_examples: int = 200,
    scheduler_factory: Callable[[int], Reorderer | None] | None = None,
) -> ViolationReport | None:
    """Drive a scenario with Hypothesis as the generate + shrink engine.

    Hypothesis searches the ``(seed, act-plan)`` space and, on a violation, shrinks to a minimal
    counterexample with its general-purpose shrinker — simplifying the seed and the act sequence far
    past the greedy drop of :func:`explore`. Each candidate still runs on the deterministic loop, so
    the returned report reproduces exactly.

    Returns the minimized :class:`ViolationReport`, or ``None`` if no violation is found within
    *max_examples*. Requires an act phase (no act rules → nothing to search).
    """

    try:
        from hypothesis import find, settings, strategies
        from hypothesis.errors import NoSuchExample

    except ImportError as error:  # pragma: no cover - optional extra
        raise RuntimeError(
            "explore_hypothesis needs hypothesis; install forze[dst]"
        ) from error

    if not scenario.act:
        return None

    def schedule_seed_of(seed: int) -> int | None:
        return derive_seed(seed, "schedule") if perturb else None

    # Fresh PCT scheduler per example (it is stateful), keyed by the example's seed so the found
    # counterexample reproduces under the same interleaving.
    def make_scheduler(seed: int) -> Reorderer | None:
        if scheduler_factory is None:
            return None

        return scheduler_factory(derive_seed(seed, "schedule"))

    plans = strategies.tuples(
        strategies.integers(min_value=0, max_value=2**31 - 1),
        strategies.lists(
            strategies.sampled_from(range(len(scenario.act))), max_size=max_act
        ),
    )

    def run(example: tuple[int, list[int]]) -> History:
        seed, plan = example
        history, _ = run_scenario(
            sim,
            scenario,
            act_workload=None,
            act_count=0,
            act_plan=plan,
            concurrency=concurrency,
            seed=seed,
            schedule_seed=schedule_seed_of(
                seed
            ),  # pyright: ignore[reportUnknownArgumentType]
            epoch=epoch,
            scheduler=make_scheduler(seed),
        )
        return history

    try:
        seed, plan = find(
            plans,
            lambda example: bool(check(run(example), sim.invariants)),
            settings=settings(max_examples=max_examples, deadline=None),
        )

    except NoSuchExample:
        return None

    history, generated = run_scenario(
        sim,
        scenario,
        act_workload=None,
        act_count=0,
        act_plan=plan,
        concurrency=concurrency,
        seed=seed,
        schedule_seed=schedule_seed_of(
            seed
        ),  # pyright: ignore[reportUnknownArgumentType]
        epoch=epoch,
        scheduler=make_scheduler(seed),
    )

    return ViolationReport(
        seed=seed,
        schedule_seed=schedule_seed_of(
            seed
        ),  # pyright: ignore[reportUnknownArgumentType]
        violations=tuple(check(history, sim.invariants)),
        workload=tuple(generated),
        history=history,
        registry_fingerprint=sim.fingerprint(),
        plan=tuple(plan),  # the act-plan that reproduces this counterexample
    )


# ....................... #


def _expand_frontier(
    choices: tuple[int, ...], branching: Sequence[int]
) -> list[tuple[int, ...]]:
    """Child choice vectors that deviate at each branch point of an explored interleaving.

    *branching* is the per-tick branching factor :class:`SystematicReorderer` recorded while
    running *choices*. For each tick whose factor exceeds one, emit a sibling that replays the
    parent's decisions for the ticks it covered, then takes a different first-choice at that tick.

    Beyond the parent's explicit prefix the replay is zero-padded (FIFO, choice ``0``): to deviate
    *first* at branch point ``tick`` past ``len(choices)``, the child must read as
    ``choices + (0,) * (tick - len(choices)) + (alternative,)`` so the reorderer keeps FIFO order
    at every intervening tick and only diverges at ``tick``. A bare ``choices[:tick]`` truncation
    would instead collapse every such deviation onto branch point ``len(choices)`` — so no explored
    vector could ever carry a ``0`` before a nonzero choice, and the search would cover only a
    vanishing corner of the interleaving tree rather than every reachable reordering.
    """

    frontier: list[tuple[int, ...]] = []

    for tick, size in enumerate(branching):
        prefix = (
            choices[:tick]
            if tick < len(choices)
            else (*choices, *(0,) * (tick - len(choices)))
        )
        frontier.extend((*prefix, alternative) for alternative in range(1, size))

    return frontier


# ....................... #


def explore_dpor(
    sim: "Simulation",
    scenario: Scenario,
    *,
    act_count: int = 6,
    concurrency: int = 4,
    seed: int = 0,
    max_runs: int = 500,
    epoch: datetime = DEFAULT_EPOCH,
) -> ViolationReport | None:
    """Systematically explore interleavings of a fixed workload (DPOR-family reduction).

    The complete, deterministic complement to :func:`explore_hypothesis` and PCT: it fixes one act
    workload (generated from *seed*), then walks the tree of per-tick scheduling choices depth-first
    via :class:`~forze_dst.scheduler.SystematicReorderer` — guaranteed to find a violation reachable
    by *reordering* that workload, within *max_runs*. A partial-order reduction prunes the search:
    an interleaving whose observable effect order matches one already seen is not expanded
    (equivalent continuations), so only orderings that change effects are explored.

    This operates at the loop's tick granularity (not per-memory-access), so the reduction is by
    observed effect-equivalence rather than a computed independence relation — sound (never expands
    a distinct outcome twice) and robust, though not the optimal per-access DPOR. Returns the first
    violating interleaving's report (the ``schedule`` reproduces it), or ``None`` if none within
    *max_runs*.
    """

    # Fix the workload once; vary only the interleaving across runs.
    _, workload = run_scenario(
        sim,
        scenario,
        act_workload=None,
        act_count=act_count,
        concurrency=concurrency,
        seed=seed,
        schedule_seed=None,
        epoch=epoch,
    )

    frontier: list[tuple[int, ...]] = [()]
    visited: set[tuple[int, ...]] = set()
    seen_signatures: set[tuple[Any, ...]] = set()
    runs = 0

    while frontier and runs < max_runs:
        choices = frontier.pop()

        if choices in visited:
            continue

        visited.add(choices)

        scheduler = SystematicReorderer(choices)
        history, _ = run_scenario(
            sim,
            scenario,
            act_workload=workload,
            act_count=act_count,
            concurrency=concurrency,
            seed=seed,
            schedule_seed=None,
            epoch=epoch,
            scheduler=scheduler,
        )
        runs += 1

        if check(history, sim.invariants):
            return ViolationReport(
                seed=seed,
                schedule_seed=None,
                violations=tuple(check(history, sim.invariants)),
                workload=tuple(workload),
                history=history,
                registry_fingerprint=sim.fingerprint(),
                choices=choices,  # the systematic interleaving that reproduces this run
            )

        signature = projection.outcome_signature(history)

        if signature in seen_signatures:
            continue  # observationally equivalent → its subtree is redundant

        seen_signatures.add(signature)

        # Expand: at each tick that branched, try every alternative first-choice, replaying the
        # parent's (zero-padded) prefix so a deviation can land at a *later* branch point.
        frontier.extend(_expand_frontier(choices, scheduler.branching))

    return None
