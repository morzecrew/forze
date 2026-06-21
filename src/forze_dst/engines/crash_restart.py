"""Crash → restart → recovery engine — kill the process mid-flight, restart over persisted state.

The recovery-bug strategy. Phase 1 drives arrange + act on a bare context behind a seeded
:class:`~forze_dst.faults.CrashPolicy`; when the crash fires the process *dies* (no graceful
shutdown — the in-flight tx rolls back, committed state persists). Phase 2 restarts a fresh runtime
over the SAME persisted store (lifecycle startup runs), drives the optional recover pass, then
observe — and the invariants check the post-recovery world. Catches lost after-commit work and
partial non-transactional writes. Shares the scenario runner's act-driving and the run substrate.
"""

from __future__ import annotations

import random
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Sequence

from forze.application.execution import ExecutionContext, ExecutionRuntime
from forze.base.primitives import derive_seed
from forze_dst.engines import base, context, projection
from forze_dst.engines import scenario as scenario_engine
from forze_dst.faults import SimulatedCrash, compile_crash
from forze_dst.oracle import ViolationReport
from forze_dst.oracle.recorder import History, Recorder, record_event
from forze_dst.runtime import run_simulation
from forze_dst.scenario import Scenario

if TYPE_CHECKING:
    from forze_dst.harness import Simulation

# ----------------------- #


def run_crash_restart(
    sim: "Simulation",
    scenario: Scenario,
    *,
    act_workload: Sequence[tuple[str, Any]] | None,
    act_count: int,
    concurrency: int,
    seed: int,
    schedule_seed: int | None,
    epoch: datetime,
    scheduler: object | None = None,
) -> tuple[History, list[tuple[str, Any]]]:
    """Run one crash → restart → recovery attempt over a single persisted store.

    The deps modules are built **once**, so the ``MockState`` they hold is the durable store that
    survives the crash. Phase 1 drives arrange + act on a bare context behind a seeded
    :class:`~forze_dst.faults.CrashPolicy`; when the crash fires the process dies (no graceful
    shutdown — the in-flight tx rolls back, committed state persists). Phase 2 restarts a **fresh**
    runtime over the SAME modules (lifecycle startup runs), drives the optional ``recover`` pass,
    then ``observe`` — all under the restart scope. Both phases' runtime traces are folded into one
    history, so the invariants see the whole arc.
    """

    config = sim.active_config
    assert config is not None and config.crash is not None  # nosec B101 - run() guard
    crash_policy = config.crash

    recorder = Recorder(seed=seed)
    generated: list[tuple[str, Any]] = []
    modules = context.build_modules(sim)
    fault_seed = derive_seed(seed, "fault")

    async def driver() -> None:
        nonlocal generated

        # --- Phase 1: workload under the seeded crash, on a bare (kill-able) context.
        crash = compile_crash(
            crash_policy,
            random.Random(derive_seed(seed, "crash")),  # nosec B311 - seeded sim crash
        )
        registry = context.registry_from_modules(
            sim, modules, fault_seed, extra=(crash,)
        )
        ctx = ExecutionContext(deps=registry.resolve())
        rng = random.Random(derive_seed(seed, "input"))  # nosec B311
        state = scenario.state()

        try:
            if sim.setup is not None:
                await sim.setup(ctx)

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

            else:
                generated = scenario.generate_act(state, act_count, rng)

            await scenario_engine.drive_act(
                sim, ctx, generated, concurrency=concurrency
            )

        except SimulatedCrash:
            # A crash during setup/arrange (serial, outside the act gather).
            record_event("crash", phase="arrange")

        finally:
            projection.fold_runtime_trace(ctx)  # the pre-crash trace

        # --- Phase 2: restart over the SAME persisted store, full runtime lifecycle.
        restart = context.registry_from_modules(sim, modules, fault_seed)
        runtime = ExecutionRuntime(deps=restart, lifecycle=sim.lifecycle)

        async with runtime.scope():
            rctx = runtime.get_context()

            if sim.recover is not None:
                await sim.recover(rctx)

            if sim.observe is not None:
                await sim.observe(rctx)

            projection.fold_runtime_trace(rctx)  # the post-restart trace

    context.run_recording(
        recorder,
        lambda: run_simulation(
            driver,
            seed=derive_seed(seed, "entropy"),
            schedule_seed=schedule_seed,
            scheduler=scheduler,
            epoch=epoch,
            latency=context.latency_for(sim, seed),
        ),
    )

    return recorder.history, generated


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
    scheduler_factory: Callable[[int], object] | None = None,
) -> ViolationReport | None:
    """Run one seed's crash/restart attempt; on a violation, minimize the act phase and report."""

    schedule_seed = derive_seed(seed, "schedule") if perturb else None

    def run(act: Sequence[tuple[str, Any]] | None) -> History:
        # Fresh scheduler per run (PCT is stateful) so the initial run, every minimization
        # predicate, and the final replay all explore the same interleaving.
        history, _ = run_crash_restart(
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
        # The initial run drives arrange + act and returns the act workload; minimization reduces
        # the act phase (the seeded crash re-fires on whatever matched call survives).
        history, act_workload = run_crash_restart(
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
    act_count: int,
    concurrency: int,
    seeds: Sequence[int],
    perturb: bool,
    epoch: datetime,
    scheduler_factory: Callable[[int], object] | None = None,
) -> ViolationReport | None:
    """Sweep seeds running the crash → restart → recovery scenario; report the first bug.

    Each seed drives arrange + act, dies at its own (seeded) crash point, restarts over the
    persisted store, recovers, and is checked against the invariants. The first seed whose
    post-recovery world violates an invariant (lost after-commit work, a partial non-transactional
    write) is minimized and reported, reproducible from that one seed.
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
