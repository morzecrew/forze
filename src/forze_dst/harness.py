"""Turnkey simulation harness — feed an app's operation registry + deps, observe bugs.

The integration layer over the DST engine (loop, entropy, recorder, oracle). You give it
your :class:`FrozenOperationRegistry`, a deps factory (typically ``lambda: MockDepsModule(...)``
— one module auto-mocks every port), and the invariants that must hold. It generates a
seeded workload of *real* operations (inputs auto-built from each operation's ``input_type``,
overridable), drives them through ``run_operation`` on the virtual-time loop under scheduler
perturbation, records every operation automatically, checks the invariants, and on a failure
searches/minimizes to a reproducible counterexample stamped with the registry's fingerprint.

``deps`` is a *factory* (called fresh per run) so each run starts from clean state, and so
this package stays free of any adapter dependency — the app supplies the mock module.
"""

from __future__ import annotations

import asyncio
import random
from datetime import datetime
from typing import Any, Awaitable, Callable, Sequence, final

import attrs

from forze.application.execution import DepsModule, DepsRegistry, ExecutionContext
from forze.application.execution.operations import run_operation
from forze.application.execution.operations.registry import FrozenOperationRegistry
from forze.base.primitives import monotonic
from forze_dst.invariants import Invariant, check
from forze_dst.oracle import ViolationReport, minimize
from forze_dst.recorder import History, Recorder, bind_recorder, record_event
from forze_dst.runtime import run_simulation
from forze_dst.time_source import DEFAULT_EPOCH

# ----------------------- #

DepsFactory = Callable[[], DepsModule]
Hook = Callable[[ExecutionContext], Awaitable[None]]

# ....................... #


@final
@attrs.define(frozen=True, kw_only=True)
class OperationCase:
    """One operation the workload may pick: its key, selection weight, and input source."""

    op: str
    """The operation to pick."""

    weight: float = 1.0
    """The weight of the operation."""

    inputs: Callable[[random.Random], Any] | None = None
    """Build an input for this op from a seeded RNG. ``None`` → auto-generate from the
    operation's declared ``input_type`` (``None`` input if it declares none)."""


# ....................... #


@final
@attrs.define(frozen=True, kw_only=True)
class _Call:
    op: str
    arg: Any


# ....................... #


@final
@attrs.define(kw_only=True)
class Simulation:
    """Drive an app's operations under deterministic simulation and check invariants."""

    operations: FrozenOperationRegistry
    """The operation registry to use for the simulation."""

    deps: DepsFactory
    """Builds the dependency wiring for a run — typically ``lambda: MockDepsModule(...)``.
    Called once per run so state is fresh."""

    invariants: Sequence[Invariant] = attrs.field(factory=tuple)
    """The invariants to check after the workload."""

    setup: Hook | None = None
    """Optional - seed initial state before the workload (e.g. create baseline rows)."""

    observe: Hook | None = None
    """Optional - record domain facts after the workload (e.g. final balances) via
    ``record_event`` so invariants can assert over them."""

    # ....................... #

    def fingerprint(self) -> str:
        """The operation catalog's structural fingerprint, from the core registry.

        Ties a counterexample to the code that produced it: if an operation's contract or
        declared plan facts change, the fingerprint changes and a stored seed can no
        longer be trusted to reproduce (see ``FrozenOperationRegistry.fingerprint``).
        """

        return self.operations.fingerprint()

    # ....................... #

    def _input_for(self, op: str, rng: random.Random, case: OperationCase) -> Any:
        if case.inputs is not None:
            return case.inputs(rng)

        descriptor = self.operations.descriptors.get(op)
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

    def _generate(
        self,
        cases: Sequence[OperationCase],
        count: int,
        seed: int,
    ) -> list[_Call]:
        rng = random.Random(seed)  # nosec B311
        weights = [case.weight for case in cases]

        chosen = rng.choices(
            list(cases),
            weights=weights,
            k=count,
        )

        return [
            _Call(op=case.op, arg=self._input_for(case.op, rng, case))
            for case in chosen
        ]

    # ....................... #

    def _run(
        self,
        workload: Sequence[_Call],
        *,
        concurrency: int,
        seed: int,
        schedule_seed: int | None,
        epoch: datetime,
    ) -> History:
        recorder = Recorder(seed=seed)

        async def scenario() -> None:
            frozen = (
                DepsRegistry.from_modules(self.deps())
                .with_tracing(runtime=True)
                .freeze()
                .resolve()
            )
            ctx = ExecutionContext(deps=frozen)

            if self.setup is not None:
                await self.setup(ctx)

            semaphore = asyncio.Semaphore(concurrency)

            async def run_one(call: _Call) -> None:
                async with semaphore:
                    invoked = monotonic()

                    try:
                        result = await run_operation(
                            self.operations, call.op, call.arg, ctx
                        )
                        record_event(
                            "operation",
                            op=call.op,
                            outcome="ok",
                            result=result,
                            invoked_at=invoked,
                            returned_at=monotonic(),
                        )

                    except Exception as error:
                        record_event(
                            "operation",
                            op=call.op,
                            outcome="error",
                            error=type(error).__name__,
                            invoked_at=invoked,
                            returned_at=monotonic(),
                        )

            await asyncio.gather(*(run_one(call) for call in workload))

            if self.observe is not None:
                await self.observe(ctx)

            # Fold the engine's runtime trace (port/tx/dispatch/op events) into the
            # history, preserving each event's own virtual-time stamp.
            trace = ctx.deps.runtime_trace()
            if trace is not None:
                for event in trace.events:
                    record_event(
                        "trace",
                        at=event.at,
                        trace_domain=event.domain,
                        op=event.op,
                        surface=event.surface,
                        route=event.route,
                        phase=event.phase,
                        tx_depth=event.tx_depth,
                    )

        with bind_recorder(recorder):
            run_simulation(
                scenario, seed=seed, schedule_seed=schedule_seed, epoch=epoch
            )

        return recorder.history

    # ....................... #

    def _attempt(
        self,
        *,
        cases: Sequence[OperationCase],
        count: int,
        concurrency: int,
        seed: int,
        perturb: bool,
        epoch: datetime,
    ) -> ViolationReport | None:
        schedule_seed = seed if perturb else None
        workload = self._generate(cases, count, seed)

        def run(items: Sequence[_Call]) -> History:
            return self._run(
                items,
                concurrency=concurrency,
                seed=seed,
                schedule_seed=schedule_seed,
                epoch=epoch,
            )

        if not check(run(workload), self.invariants):
            return None

        minimal = minimize(
            workload, lambda subset: bool(check(run(subset), self.invariants))
        )
        final_history = run(minimal)

        return ViolationReport(
            seed=seed,
            schedule_seed=schedule_seed,
            violations=tuple(check(final_history, self.invariants)),
            workload=tuple((call.op, call.arg) for call in minimal),
            history=final_history,
            registry_fingerprint=self.fingerprint(),
        )

    # ....................... #

    def explore(
        self,
        *,
        cases: Sequence[OperationCase],
        count: int = 50,
        concurrency: int = 4,
        seeds: Sequence[int],
        perturb: bool = True,
        epoch: datetime = DEFAULT_EPOCH,
    ) -> ViolationReport | None:
        """Generate + run a seeded workload per seed; on a violation, minimize and report.

        Each seed draws its own workload (operations + inputs) and, with *perturb*, its own
        interleaving. The first violating seed's workload is minimized to a 1-minimal set of
        operations that still fails; the report carries the seed, minimized workload,
        recorded history, and the registry fingerprint.
        """

        for seed in seeds:
            report = self._attempt(
                cases=cases,
                count=count,
                concurrency=concurrency,
                seed=seed,
                perturb=perturb,
                epoch=epoch,
            )
            if report is not None:
                return report

        return None
