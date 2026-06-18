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
from typing import Any, Awaitable, Callable, Sequence, cast, final

import attrs

from forze.application.execution import DepsModule, DepsRegistry, ExecutionContext
from forze.application.execution.operations import run_operation
from forze.application.execution.operations.registry import FrozenOperationRegistry
from forze.application.execution.tracing.cooperative import LatencyModel
from forze.base.exceptions import CoreException
from forze.base.primitives import monotonic
from forze_dst.derive import DEFAULT_CREATE_VERBS
from forze_dst.derive import derive_scenario as _derive_from_catalog
from forze_dst.invariants import Invariant, check
from forze_dst.oracle import ViolationReport, minimize
from forze_dst.reactive import ReactiveMap
from forze_dst.recorder import History, Recorder, bind_recorder, record_event
from forze_dst.runtime import run_simulation
from forze_dst.scenario import Scenario
from forze_dst.scheduler import SystematicScheduler
from forze_dst.time_source import DEFAULT_EPOCH

# ----------------------- #

DepsFactory = Callable[[], "DepsModule | Sequence[DepsModule]"]
Hook = Callable[[ExecutionContext], Awaitable[None]]


def _outcome_signature(history: History) -> tuple[Any, ...]:
    """The observable effect order of a run — operations + recorded facts, ignoring trace.

    Two interleavings with the same signature are observationally equivalent (same effects in
    the same order), so the explorer need not expand both — a partial-order reduction.
    """

    return tuple(
        (event.kind, event.fields.get("op"), event.fields.get("outcome"))
        if event.kind == "operation"
        else (event.kind, tuple(sorted(event.fields.items(), key=lambda kv: kv[0])))
        for event in history.events
        if event.kind not in ("trace", "op_start")
    )


# ....................... #


def _fold_runtime_trace(ctx: ExecutionContext) -> None:
    """Fold the engine's runtime trace into the recorded history, keeping each event's stamp."""

    trace = ctx.deps.runtime_trace()

    if trace is None:
        return

    for event in trace.events:
        record_event(
            "trace",
            at=event.at,
            trace_seq=event.seq,
            trace_domain=event.domain,
            op=event.op,
            surface=event.surface,
            route=event.route,
            phase=event.phase,
            tx_depth=event.tx_depth,
        )


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

    latency: LatencyModel | None = None
    """Optional - simulated I/O latency: ``(surface, route, op) -> seconds``, applied at each
    port boundary to advance the virtual clock (a real downstream takes time). Lets
    time-dependent bugs surface without artificial sleeps in handlers."""

    # ....................... #

    def fingerprint(self) -> str:
        """The operation catalog's structural fingerprint, from the core registry.

        Ties a counterexample to the code that produced it: if an operation's contract or
        declared plan facts change, the fingerprint changes and a stored seed can no
        longer be trusted to reproduce (see ``FrozenOperationRegistry.fingerprint``).
        """

        return self.operations.fingerprint()

    # ....................... #

    def _frozen_deps(self) -> Any:
        """Build the run's resolved, runtime-traced deps from the factory.

        The factory may return a single module or several (e.g. a mock module plus a
        ``DomainEventsDepsModule`` wiring saga / event-handler cascades).
        """

        produced = self.deps()
        modules: tuple[DepsModule, ...] = (
            tuple(produced)
            if isinstance(produced, (list, tuple))
            else (cast("DepsModule", produced),)
        )

        return (
            DepsRegistry.from_modules(*modules)
            .with_tracing(runtime=True)
            .freeze()
            .resolve()
        )

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
            ctx = ExecutionContext(deps=self._frozen_deps())

            if self.setup is not None:
                await self.setup(ctx)

            semaphore = asyncio.Semaphore(concurrency)

            async def run_one(call: _Call, index: int) -> None:
                async with semaphore:
                    # A start marker so the causal graph has a sequence-based interval
                    # per call — robust to concurrent ops sharing a virtual-time stamp
                    # (an ``await`` interleaves them without advancing the clock).
                    record_event("op_start", call_id=index, op=call.op)
                    invoked = monotonic()

                    try:
                        result = await run_operation(
                            self.operations, call.op, call.arg, ctx
                        )
                        record_event(
                            "operation",
                            call_id=index,
                            op=call.op,
                            outcome="ok",
                            result=result,
                            invoked_at=invoked,
                            returned_at=monotonic(),
                        )

                    except Exception as error:
                        record_event(
                            "operation",
                            call_id=index,
                            op=call.op,
                            outcome="error",
                            error=type(error).__name__,
                            unexpected=not isinstance(error, CoreException),
                            invoked_at=invoked,
                            returned_at=monotonic(),
                        )

            await asyncio.gather(
                *(run_one(call, index) for index, call in enumerate(workload))
            )

            if self.observe is not None:
                await self.observe(ctx)

            _fold_runtime_trace(ctx)

        with bind_recorder(recorder):
            run_simulation(
                scenario,
                seed=seed,
                schedule_seed=schedule_seed,
                epoch=epoch,
                latency=self.latency,
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

    # ....................... #

    async def _run_call(
        self,
        ctx: ExecutionContext,
        semaphore: asyncio.Semaphore,
        *,
        call_id: int,
        op: str,
        arg: Any,
    ) -> None:
        """Run one operation concurrently, recording its start marker and outcome."""

        async with semaphore:
            record_event("op_start", call_id=call_id, op=op)
            invoked = monotonic()

            try:
                result = await run_operation(self.operations, op, arg, ctx)
                record_event(
                    "operation",
                    call_id=call_id,
                    op=op,
                    outcome="ok",
                    result=result,
                    invoked_at=invoked,
                    returned_at=monotonic(),
                )

            except Exception as error:
                record_event(
                    "operation",
                    call_id=call_id,
                    op=op,
                    outcome="error",
                    error=type(error).__name__,
                    unexpected=not isinstance(error, CoreException),
                    invoked_at=invoked,
                    returned_at=monotonic(),
                )

    # ....................... #

    async def _run_arrange_call(
        self,
        ctx: ExecutionContext,
        *,
        call_id: int,
        op: str,
        arg: Any,
    ) -> tuple[bool, Any]:
        """Run one arrange operation serially; record it and return ``(ok, result)``.

        A failed arrange op produces nothing into the model (``ok`` is ``False``) but is
        still recorded, so the report shows where setup broke down.
        """

        record_event("op_start", call_id=call_id, op=op)
        invoked = monotonic()

        try:
            result = await run_operation(self.operations, op, arg, ctx)
            record_event(
                "operation",
                call_id=call_id,
                op=op,
                outcome="ok",
                result=result,
                invoked_at=invoked,
                returned_at=monotonic(),
            )
            return True, result

        except Exception as error:
            record_event(
                "operation",
                call_id=call_id,
                op=op,
                outcome="error",
                error=type(error).__name__,
                unexpected=not isinstance(error, CoreException),
                invoked_at=invoked,
                returned_at=monotonic(),
            )
            return False, None

    # ....................... #

    def _run_scenario(
        self,
        scenario: Scenario,
        *,
        act_workload: Sequence[tuple[str, Any]] | None,
        act_count: int,
        concurrency: int,
        seed: int,
        schedule_seed: int | None,
        epoch: datetime,
        act_plan: Sequence[int] | None = None,
        scheduler: object | None = None,
    ) -> tuple[History, list[tuple[str, Any]]]:
        """Run a scenario: arrange serially, then act concurrently.

        The act workload comes from, in precedence: *act_workload* (concrete calls replayed,
        for minimization), *act_plan* (act-rule indices to fire, built post-arrange — the
        Hypothesis-driven path; disabled rules are skipped), else generated from the arranged
        state. Returns the recorded history and the act workload that ran.
        """

        recorder = Recorder(seed=seed)
        generated: list[tuple[str, Any]] = []

        async def driver() -> None:
            nonlocal generated

            ctx = ExecutionContext(deps=self._frozen_deps())

            if self.setup is not None:
                await self.setup(ctx)

            rng = random.Random(seed)  # nosec B311
            state = scenario.state()

            # Arrange: serial, real ids captured into the model. Negative call ids keep
            # arrange spans distinct from (and never confused as concurrent with) act.
            for index, rule in enumerate(scenario.arrange):
                if not rule.is_enabled(state):
                    continue

                arg = rule.arg(state, rng)
                ok, result = await self._run_arrange_call(
                    ctx, call_id=-(index + 1), op=rule.op, arg=arg
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
                    self._run_call(ctx, semaphore, call_id=index, op=op, arg=arg)
                    for index, (op, arg) in enumerate(generated)
                )
            )

            if self.observe is not None:
                await self.observe(ctx)

            _fold_runtime_trace(ctx)

        with bind_recorder(recorder):
            run_simulation(
                driver,
                seed=seed,
                schedule_seed=schedule_seed,
                epoch=epoch,
                scheduler=scheduler,
                latency=self.latency,
            )

        return recorder.history, generated

    # ....................... #

    def _attempt_scenario(
        self,
        scenario: Scenario,
        *,
        act_count: int,
        concurrency: int,
        seed: int,
        perturb: bool,
        epoch: datetime,
        scheduler_factory: Callable[[int], object] | None = None,
    ) -> ViolationReport | None:
        schedule_seed = seed if perturb else None
        scheduler = None if scheduler_factory is None else scheduler_factory(seed)

        def run(act: Sequence[tuple[str, Any]] | None) -> History:
            history, _ = self._run_scenario(
                scenario,
                act_workload=act,
                act_count=act_count,
                concurrency=concurrency,
                seed=seed,
                schedule_seed=schedule_seed,
                epoch=epoch,
                scheduler=scheduler,
            )
            return history

        history, act_workload = self._run_scenario(
            scenario,
            act_workload=None,
            act_count=act_count,
            concurrency=concurrency,
            seed=seed,
            schedule_seed=schedule_seed,
            epoch=epoch,
            scheduler=scheduler,
        )

        if not check(history, self.invariants):
            return None

        # Minimize the act phase only; arrange is replayed identically (seeded), so the
        # captured act calls still reference valid arranged handles.
        minimal = minimize(
            act_workload, lambda subset: bool(check(run(subset), self.invariants))
        )
        final_history = run(minimal)

        return ViolationReport(
            seed=seed,
            schedule_seed=schedule_seed,
            violations=tuple(check(final_history, self.invariants)),
            workload=tuple(minimal),
            history=final_history,
            registry_fingerprint=self.fingerprint(),
        )

    # ....................... #

    def explore_scenario(
        self,
        scenario: Scenario,
        *,
        act_count: int = 20,
        concurrency: int = 4,
        seeds: Sequence[int],
        perturb: bool = True,
        epoch: datetime = DEFAULT_EPOCH,
        scheduler_factory: Callable[[int], object] | None = None,
    ) -> ViolationReport | None:
        """Drive a generative :class:`Scenario` per seed; on a violation, minimize + report.

        Each seed arranges valid state (serially, capturing real ids), then samples
        *act_count* enabled act calls and runs them concurrently under perturbation. The
        first violating seed's act phase is minimized to a 1-minimal set that still fails;
        arrange stays fixed. The report carries the seed, minimized act workload, full
        recorded history (arrange + act), and the registry fingerprint.

        *scheduler_factory* (e.g. :func:`forze_dst.scheduler.pct_scheduler_factory`) supplies
        a per-seed interleaving scheduler — PCT in place of the default uniform shuffle, to
        hunt deep interleavings with a better per-run probability.
        """

        for seed in seeds:
            report = self._attempt_scenario(
                scenario,
                act_count=act_count,
                concurrency=concurrency,
                seed=seed,
                perturb=perturb,
                epoch=epoch,
                scheduler_factory=scheduler_factory,
            )
            if report is not None:
                return report

        return None

    # ....................... #

    def explore_scenario_hypothesis(
        self,
        scenario: Scenario,
        *,
        max_act: int = 20,
        concurrency: int = 4,
        perturb: bool = True,
        epoch: datetime = DEFAULT_EPOCH,
        max_examples: int = 200,
    ) -> ViolationReport | None:
        """Drive a scenario with Hypothesis as the generate + shrink engine.

        Hypothesis searches the ``(seed, act-plan)`` space and, on a violation, shrinks to a
        minimal counterexample with its general-purpose shrinker — simplifying the seed and
        the act sequence far past the greedy drop of :meth:`explore_scenario`. Each candidate
        still runs on the deterministic loop, so the returned report reproduces exactly.

        Returns the minimized :class:`ViolationReport`, or ``None`` if no violation is found
        within *max_examples*. Requires an act phase (no act rules → nothing to search).
        """

        try:
            from hypothesis import find, settings, strategies
            from hypothesis.errors import NoSuchExample

        except ImportError as error:  # pragma: no cover - optional extra
            raise RuntimeError(
                "explore_scenario_hypothesis needs hypothesis; install forze[dst]"
            ) from error

        if not scenario.act:
            return None

        def schedule_seed_of(seed: int) -> int | None:
            return seed if perturb else None

        plans = strategies.tuples(
            strategies.integers(min_value=0, max_value=2**31 - 1),
            strategies.lists(
                strategies.sampled_from(range(len(scenario.act))), max_size=max_act
            ),
        )

        def run(example: tuple[int, list[int]]) -> History:
            seed, plan = example
            history, _ = self._run_scenario(
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
            )
            return history

        try:
            seed, plan = find(
                plans,
                lambda example: bool(check(run(example), self.invariants)),
                settings=settings(max_examples=max_examples, deadline=None),
            )

        except NoSuchExample:
            return None

        history, generated = self._run_scenario(
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
        )

        return ViolationReport(
            seed=seed,
            schedule_seed=schedule_seed_of(
                seed
            ),  # pyright: ignore[reportUnknownArgumentType]
            violations=tuple(check(history, self.invariants)),
            workload=tuple(generated),
            history=history,
            registry_fingerprint=self.fingerprint(),
        )

    # ....................... #

    def explore_scenario_dpor(
        self,
        scenario: Scenario,
        *,
        act_count: int = 6,
        concurrency: int = 4,
        seed: int = 0,
        max_runs: int = 500,
        epoch: datetime = DEFAULT_EPOCH,
    ) -> ViolationReport | None:
        """Systematically explore interleavings of a fixed workload (DPOR-family reduction).

        The complete, deterministic complement to :meth:`explore_scenario_hypothesis` and PCT:
        it fixes one act workload (generated from *seed*), then walks the tree of per-tick
        scheduling choices depth-first via :class:`~forze_dst.scheduler.SystematicScheduler` —
        guaranteed to find a violation reachable by *reordering* that workload, within
        *max_runs*. A partial-order reduction prunes the search: an interleaving whose
        observable effect order matches one already seen is not expanded (equivalent
        continuations), so only orderings that change effects are explored.

        This operates at the loop's tick granularity (not per-memory-access), so the
        reduction is by observed effect-equivalence rather than a computed independence
        relation — sound (never expands a distinct outcome twice) and robust, though not the
        optimal per-access DPOR. Returns the first violating interleaving's report (the
        ``schedule`` reproduces it), or ``None`` if none within *max_runs*.
        """

        # Fix the workload once; vary only the interleaving across runs.
        _, workload = self._run_scenario(
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

            scheduler = SystematicScheduler(choices)
            history, _ = self._run_scenario(
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

            if check(history, self.invariants):
                return ViolationReport(
                    seed=seed,
                    schedule_seed=None,
                    violations=tuple(check(history, self.invariants)),
                    workload=tuple(workload),
                    history=history,
                    registry_fingerprint=self.fingerprint(),
                )

            signature = _outcome_signature(history)
            if signature in seen_signatures:
                continue  # observationally equivalent → its subtree is redundant
            seen_signatures.add(signature)

            # Expand: at each tick that branched, try every alternative first-choice.
            for tick, size in enumerate(scheduler.branching):
                for alternative in range(1, size):
                    frontier.append((*choices[:tick], alternative))

        return None

    # ....................... #

    def reactive_map(
        self,
        *,
        create_verbs: frozenset[str] = DEFAULT_CREATE_VERBS,
        arrange_each: int = 1,
        seed: int = 0,
        epoch: datetime = DEFAULT_EPOCH,
    ) -> ReactiveMap:
        """Recover the reactive cascade topology by probing each candidate operation.

        For each operation the catalog derivation treats as an entry point, fire it once
        against the arranged state and read the engine trace: every operation invoked but not
        directly driven is a *cascade* (saga step / event handler), and every domain event
        dispatched along the way is recorded. The operation registries hold opaque callables,
        so this wiring is only knowable at runtime — this is how it is recovered.
        """

        base = _derive_from_catalog(
            self.operations, create_verbs=create_verbs, arrange_each=arrange_each
        )

        cascades: dict[str, frozenset[str]] = {}
        events: dict[str, frozenset[str]] = {}

        for rule in base.act:
            probe = Scenario(state=base.state, arrange=base.arrange, act=(rule,))
            history, _ = self._run_scenario(
                probe,
                act_workload=None,
                act_count=1,
                concurrency=1,
                seed=seed,
                schedule_seed=None,
                epoch=epoch,
            )

            direct = {
                event.fields.get("op")
                for event in history.events
                if event.kind == "operation"
            }
            invoked = {
                event.fields.get("op")
                for event in history.events
                if event.kind == "trace"
                and event.fields.get("trace_domain") == "operation"
                and event.fields.get("phase") == "invoke"
            }
            dispatched = {
                event.fields.get("surface")
                for event in history.events
                if event.kind == "trace"
                and event.fields.get("trace_domain") == "domain"
                and event.fields.get("op") == "dispatch"
            }

            cascades[rule.op] = frozenset(
                str(op) for op in (invoked - direct) if op is not None
            )
            events[rule.op] = frozenset(
                str(name) for name in dispatched if name is not None
            )

        return ReactiveMap(cascades=cascades, events=events)

    # ....................... #

    def derive_scenario(
        self,
        *,
        create_verbs: frozenset[str] = DEFAULT_CREATE_VERBS,
        arrange_each: int = 1,
        probe: bool = True,
        seed: int = 0,
        epoch: datetime = DEFAULT_EPOCH,
    ) -> Scenario:
        """Infer a draft :class:`Scenario` from the catalog, then refine it reactively.

        Starts from the static, name-driven catalog derivation (see
        :func:`forze_dst.derive.derive_scenario`); then, unless *probe* is disabled, recovers
        the reactive cascade topology (see :meth:`reactive_map`) and drops operations that are
        only ever triggered as cascades (saga steps, domain-event handlers) — they fire
        automatically when their trigger runs, so driving them directly would be unrealistic.
        """

        base = _derive_from_catalog(
            self.operations, create_verbs=create_verbs, arrange_each=arrange_each
        )

        if not probe:
            return base

        reactive = self.reactive_map(
            create_verbs=create_verbs,
            arrange_each=arrange_each,
            seed=seed,
            epoch=epoch,
        ).reactive_ops

        if not reactive:
            return base

        return Scenario(
            state=base.state,
            arrange=base.arrange,
            act=tuple(rule for rule in base.act if rule.op not in reactive),
        )
