"""Run substrate — build a run's execution context, deps, and inputs (shared by every engine).

The plumbing each exploration engine stands on, lifted out of the harness so the engines read as
*search* logic over a small set of primitives: build the deps modules (fresh state per run), freeze
them into a runtime-traced registry with the run's seeded fault/interceptor seam, yield the
execution context (bare, or inside the real runtime when ``config.runtime`` is set), compile the
seeded latency model, auto-generate operation inputs, and run a single op (driven or arrange) while
anchoring its span. Every function takes the :class:`~forze_dst.harness.Simulation` as its context.
"""

from __future__ import annotations

import asyncio
import random
from contextlib import asynccontextmanager, suppress
from typing import TYPE_CHECKING, Any, AsyncGenerator, Callable, Sequence, cast

from forze.application.contracts.interception import PortInterceptor
from forze.application.execution import (
    DepsModule,
    DepsRegistry,
    ExecutionContext,
    ExecutionRuntime,
    FrozenDepsRegistry,
)
from forze.application.execution.interception import LatencyModel
from forze.application.execution.operations import run_operation
from forze.base.primitives import derive_seed
from forze_dst.engines.cases import Call, OperationCase
from forze_dst.faults import compile_fault_policy
from forze_dst.latency import compile_latency
from forze_dst.loop import SimulationDeadlock
from forze.application.execution.tracing import bind_tx_sequence
from forze_dst.oracle.recorder import Recorder, bind_recorder, record_event

if TYPE_CHECKING:
    from forze_dst.harness import Simulation

# ----------------------- #


def run_recording(recorder: Recorder, run: Callable[[], None]) -> None:
    """Run *run* under *recorder*, turning a deadlock into a recorded ``deadlock`` event.

    A :class:`~forze_dst.loop.SimulationDeadlock` — the loop gone quiescent with no ready work and
    no pending timer — is a genuine bug: a workload that cannot make progress. Recording it as an
    event (instead of letting it abort the sweep) lets the oracle report **and minimize** it like
    any other counterexample. ``check`` treats a ``deadlock`` event as a violation, so the whole
    find → shrink → report pipeline works with no further changes.
    """

    # Bind a fresh run-global transaction-id counter for the whole run (around the gather of all
    # tasks), so transactions get unique, replay-stable ids the isolation oracle groups by.
    with bind_recorder(recorder), bind_tx_sequence():
        try:
            run()
        except SimulationDeadlock as deadlock:
            events = recorder.history.events
            record_event(
                "deadlock",
                at=events[-1].at if events else 0.0,
                detail=str(deadlock),
            )


def build_modules(sim: "Simulation") -> tuple[DepsModule, ...]:
    """Build a fresh set of deps modules from the factory (fresh state per call)."""

    produced = sim.deps()
    return (
        tuple(produced)
        if isinstance(produced, (list, tuple))
        else (cast("DepsModule", produced),)
    )


# ....................... #


def registry_from_modules(
    sim: "Simulation",
    modules: tuple[DepsModule, ...],
    seed: int,
    *,
    extra: Sequence[PortInterceptor] = (),
) -> FrozenDepsRegistry:
    """Freeze *modules* into a runtime-traced registry with the run's seam interceptors.

    *seed* is the run's **fault** sub-seed (``derive_seed(master, "fault")``). The declarative
    ``config.faults`` is compiled here with ``random.Random(seed)`` — seeded by construction, no
    caller RNG — and registered deps-scoped alongside any *extra* interceptors (e.g. the crash
    interceptor) and the manual ``interceptors`` factory, so every resolved configurable port runs
    through them. Passing the same *modules* twice rebuilds the registry over the SAME state — the
    seam the crash/restart restart relies on.
    """

    capture_values = sim.active_config is not None and sim.active_config.capture_values
    registry = DepsRegistry.from_modules(*modules).with_tracing(
        runtime=True, capture_values=capture_values
    )

    interceptors: list[PortInterceptor] = list(extra)

    if sim.interceptors is not None:
        interceptors.extend(sim.interceptors(seed))

    if sim.active_config is not None and sim.active_config.faults is not None:
        interceptors.append(
            compile_fault_policy(
                sim.active_config.faults,
                random.Random(seed),  # nosec B311 - seeded sim faults, not crypto
            )
        )

    if interceptors:
        registry = registry.with_interceptors(*interceptors)

    return registry.freeze()


# ....................... #


def frozen_registry(sim: "Simulation", seed: int) -> FrozenDepsRegistry:
    """The run's frozen, runtime-traced deps registry over a fresh module set."""

    return registry_from_modules(sim, build_modules(sim), seed)


# ....................... #


@asynccontextmanager
async def execution_context(
    sim: "Simulation",
    fault_seed: int,
) -> AsyncGenerator[ExecutionContext]:
    """Yield the run's :class:`ExecutionContext` — bare by default, runtime-scoped on opt-in.

    With ``SimulationConfig.runtime`` set, the context is driven inside the real
    :meth:`ExecutionRuntime.scope` (lifecycle startup before the body, graceful drain + shutdown
    after) — the faithful path. Otherwise a bare context is built directly (the proven default: no
    background-task interference). *fault_seed* is the run's fault sub-seed, threaded into the
    registry's seeded interceptors.
    """

    registry = frozen_registry(sim, fault_seed)

    if sim.active_config is not None and sim.active_config.runtime:
        runtime = ExecutionRuntime(deps=registry, lifecycle=sim.lifecycle)

        async with runtime.scope():
            yield runtime.get_context()

    else:
        yield ExecutionContext(deps=registry.resolve())


# ....................... #


def latency_for(sim: "Simulation", seed: int) -> LatencyModel | None:
    """The run's latency model: the config profile (compiled from the latency sub-seed) if set,
    else the manual ``Simulation.latency`` callable escape hatch. *seed* is the master.
    """

    if sim.active_config is not None and sim.active_config.latency is not None:
        return compile_latency(
            sim.active_config.latency,
            random.Random(  # nosec B311 - seeded sim latency, not crypto
                derive_seed(seed, "latency")
            ),
        )

    return sim.latency


# ....................... #


def input_for(
    sim: "Simulation",
    op: str,
    rng: random.Random,
    case: OperationCase,
) -> Any:
    """Build an op's input: the case factory if given, else auto-generated from its input type."""

    if case.inputs is not None:
        return case.inputs(rng)

    descriptor = sim.operations.descriptors.get(op)
    input_type = descriptor.input_type if descriptor is not None else None

    if input_type is None:
        return None

    try:
        from polyfactory.factories.pydantic_factory import ModelFactory

    except ImportError as error:  # pragma: no cover - optional extra
        raise RuntimeError(
            f"auto-generating an input for {op!r} needs polyfactory; install "
            "forze[dst] or pass an explicit OperationCase.inputs factory"
        ) from error

    factory = ModelFactory.create_factory(input_type)
    factory.seed_random(rng.getrandbits(32))
    return factory.build()


# ....................... #


def generate(
    sim: "Simulation",
    cases: Sequence[OperationCase],
    count: int,
    seed: int,
) -> list[Call]:
    """Pick *count* weighted cases from *seed* and build each one's input — the op-case workload."""

    rng = random.Random(derive_seed(seed, "input"))  # nosec B311
    weights = [case.weight for case in cases]

    chosen = rng.choices(
        list(cases),
        weights=weights,
        k=count,
    )

    return [Call(op=case.op, arg=input_for(sim, case.op, rng, case)) for case in chosen]


# ....................... #


async def run_call(
    sim: "Simulation",
    ctx: ExecutionContext,
    semaphore: asyncio.Semaphore,
    *,
    call_id: int,
    op: str,
    arg: Any,
) -> None:
    """Run one operation concurrently, anchoring its span start.

    Only an ``op_start`` anchor (carrying the call id) is recorded here; the operation's
    **outcome** is the engine trace's to record (``run_operation`` emits an invoke→complete/error
    boundary, classified ``ok``/``failed``/``error``), and the harness projects per-op ``operation``
    events from that single source when folding the trace (see
    :func:`~forze_dst.engines.projection.fold_runtime_trace`). A domain failure or a bug is swallowed here
    so one call's failure never aborts the concurrent batch; a
    :class:`~forze_dst.faults.SimulatedCrash` (a ``BaseException``) is *not* caught — it propagates
    to model the process dying.
    """

    async with semaphore:
        record_event("op_start", call_id=call_id, op=op)

        with suppress(Exception):
            await run_operation(sim.operations, op, arg, ctx)


# ....................... #


async def run_arrange_call(
    sim: "Simulation",
    ctx: ExecutionContext,
    *,
    call_id: int,
    op: str,
    arg: Any,
) -> tuple[bool, Any]:
    """Run one arrange operation serially; anchor its span and return ``(ok, result)``.

    A failed arrange op produces nothing into the model (``ok`` is ``False``). Only the
    ``op_start`` anchor is recorded; the outcome lives in the engine trace (projected at fold
    time), so the broad ``except Exception`` here is deliberate and non-lossy — a domain failure
    *or* a latent bug is still surfaced to the oracle (``no_unexpected_error`` reads the ``error``
    outcome the engine trace classifies). A :class:`~forze_dst.faults.SimulatedCrash` is a
    ``BaseException`` and is *not* caught — it propagates to model the process dying mid-arrange.
    The result is returned directly so arrange can capture produced handles.
    """

    record_event("op_start", call_id=call_id, op=op)

    try:
        result = await run_operation(sim.operations, op, arg, ctx)
        return True, result

    except Exception:
        return False, None
