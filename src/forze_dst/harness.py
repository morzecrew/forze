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
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, AsyncGenerator, Awaitable, Callable, Sequence, cast, final

import attrs

from forze.application.execution import (
    DepsModule,
    DepsRegistry,
    ExecutionContext,
    ExecutionRuntime,
    FrozenDepsRegistry,
)
from forze.application.execution.interception import LatencyModel, PortInterceptor
from forze.application.execution.lifecycle import FrozenLifecyclePlan
from forze.application.execution.operations import run_operation
from forze.application.execution.operations.registry import FrozenOperationRegistry
from forze.base.primitives import derive_seed
from forze_dst.derive import DEFAULT_CREATE_VERBS
from forze_dst.derive import derive_scenario as _derive_from_catalog
from forze_dst.invariants import Invariant, check
from forze_dst.oracle import ViolationReport, minimize
from forze_dst.reactive import ReactiveMap
from forze_dst.recorder import (
    History,
    Recorder,
    bind_recorder,
    current_recorder,
    record_event,
)
from forze_dst.runtime import run_simulation
from forze_dst.config import SchedulerKind, SimulationConfig, Strategy
from forze_dst.coverage import Behavior, CoverageStats, behavioral_coverage
from forze_dst.faults import SimulatedCrash, compile_crash, compile_fault_policy
from forze_dst.latency import compile_latency
from forze_dst.scenario import Scenario
from forze_dst.scheduler import SystematicScheduler, pct_scheduler_factory
from forze_dst.time_source import DEFAULT_EPOCH

# ----------------------- #

DepsFactory = Callable[[], "DepsModule | Sequence[DepsModule]"]
InterceptorFactory = Callable[[int], "Sequence[PortInterceptor]"]
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
    """Fold the engine's runtime trace into history, then project per-op ``operation`` events.

    The engine trace is the single source of truth for execution events — ports, transactions,
    domain dispatch, and the operation invoke→complete|error boundary (the engine classifies the
    terminal ``ok`` / ``failed`` / ``error``). Each event is folded as a ``trace`` event keeping
    its stamp. Operation outcomes are then **projected** into convenience ``operation`` events,
    one per boundary, sourced entirely from the trace and correlated to the harness's ``op_start``
    anchors for the call id (an ``op_start`` is immediately followed by its invoke with no
    intervening await, so the i-th anchor matches the i-th invoke). The harness records no
    operation outcome of its own — the trace is the single source (decision D6=c)."""

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
            key=event.key,
            outcome=event.outcome,
            error=event.error,
        )

    _project_operation_events(trace.events)


# ....................... #


def _project_operation_events(trace_events: Sequence[Any]) -> None:
    """Project ``operation`` events from the folded trace's operation boundaries.

    Each invoke is matched to its terminal (complete/error) per op in FIFO order, and to the
    recorder's ``op_start`` anchors by global ordinal (for the call id). The projected event
    carries the trace's own sequence numbers as the span interval (``start_seq``/``end_seq`` —
    true execution order, which never collides, unlike a shared virtual-time stamp). A boundary
    with no terminal (the process crashed mid-call) is projected ``incomplete``."""

    recorder = current_recorder()

    if recorder is None:
        return

    op_starts = [e for e in recorder.history.events if e.kind == "op_start"]
    invokes = [
        e for e in trace_events if e.domain == "operation" and e.phase == "invoke"
    ]

    terminals: dict[str, deque[Any]] = defaultdict(deque)
    for event in trace_events:
        if event.domain == "operation" and event.phase in ("complete", "error"):
            terminals[event.op].append(event)

    for index, invoke in enumerate(invokes):
        anchor = op_starts[index] if index < len(op_starts) else None
        call_id = anchor.fields.get("call_id") if anchor is not None else -1

        queue = terminals.get(invoke.op)
        terminal = queue.popleft() if queue else None

        if terminal is None:
            outcome, error = "incomplete", None
            returned_at, end_seq = invoke.at, invoke.seq
        else:
            outcome = terminal.outcome or "ok"
            error = terminal.error
            returned_at, end_seq = terminal.at, terminal.seq

        record_event(
            "operation",
            at=returned_at,
            call_id=call_id,
            op=invoke.op,
            outcome=outcome,
            error=error,
            invoked_at=invoke.at,
            returned_at=returned_at,
            start_seq=invoke.seq,
            end_seq=end_seq,
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
    ``record_event`` so invariants can assert over them. On a crash/restart run it runs after
    the restart (over the recovered state), so the invariants see the post-recovery world."""

    recover: Hook | None = None
    """Optional - a recovery pass run after a crash/restart restart, before :attr:`observe`,
    e.g. drive the outbox relay once to redeliver events the crash interrupted. Runs inside the
    restart runtime scope (lifecycle startup has completed), over the persisted store."""

    lifecycle: FrozenLifecyclePlan = attrs.field(factory=FrozenLifecyclePlan)
    """The lifecycle plan driven when ``SimulationConfig.runtime`` is set — and always on the
    crash/restart *restart* phase: startup runs before the workload / recovery, graceful drain +
    shutdown after. Empty by default (``scope()`` just builds the context). A startup step is the
    natural home for app-side recovery (relay drain, lease reclaim) that should run on every boot."""

    latency: LatencyModel | None = None
    """Optional - simulated I/O latency: ``(surface, route, op) -> seconds``, applied at each
    port boundary to advance the virtual clock (a real downstream takes time). Lets
    time-dependent bugs surface without artificial sleeps in handlers."""

    interceptors: InterceptorFactory | None = None
    """Optional - per-run port interceptors (e.g. seeded fault injection). A factory
    ``seed -> interceptors`` so each run gets a fresh, seed-derived chain; registered
    deps-scoped on every resolved configurable port, inside the runtime-tracing and resilience
    wraps. The cooperative/latency interceptor is added separately (run-scoped) by
    ``run_simulation``.

    REPRODUCIBILITY RULE: the factory MUST derive every interceptor's RNG from its ``seed``
    argument (``PortFaultInterceptor(rng=random.Random(seed), ...)``). Closing over a fixed
    seed decouples the fault stream from the run and breaks replay/minimization — the whole
    point of a single seed driving all nondeterminism. (The declarative ``SimulationConfig
    .faults`` derives its RNG itself, removing the footgun; prefer it.)"""

    _active_config: SimulationConfig | None = attrs.field(default=None, init=False)
    """The config of the in-progress :meth:`run`, so the per-run helpers can compile its
    seeded faults / latency from the derived sub-seeds. Run-scoped; ``None`` between runs."""

    # ....................... #

    def fingerprint(self) -> str:
        """The operation catalog's structural fingerprint, from the core registry.

        Ties a counterexample to the code that produced it: if an operation's contract or
        declared plan facts change, the fingerprint changes and a stored seed can no
        longer be trusted to reproduce (see ``FrozenOperationRegistry.fingerprint``).
        """

        return self.operations.fingerprint()

    # ....................... #

    def run(
        self,
        config: SimulationConfig,
        *,
        scenario: Scenario | None = None,
        cases: Sequence[OperationCase] | None = None,
    ) -> ViolationReport | None:
        """Explore under *config* — the single, config-driven entrypoint.

        One master seed per swept value drives every nondeterminism stream (schedule / faults
        / entropy / inputs), each an independent sub-seed; ``config.strategy`` selects how the
        workload is generated and explored. Provide *cases* for ``OP_CASE``; for the scenario
        strategies *scenario* is used, or auto-derived from the operation catalog if omitted.
        Returns the first violating seed's minimized, reproducible counterexample, or ``None``.
        """

        # Run-scoped: the per-run helpers compile this config's seeded faults/latency.
        self._active_config = config
        try:
            if config.crash is not None:
                # Crash → restart → recovery: scenario-shaped (arrange/act). Honors the chosen
                # interleaving scheduler (PCT when selected) just like the scenario strategy.
                sc = scenario if scenario is not None else self.derive_scenario()
                factory = (
                    pct_scheduler_factory(depth=config.pct_depth, steps=config.pct_steps)
                    if config.scheduler is SchedulerKind.PCT
                    else None
                )
                return self._explore_crash_restart(
                    sc,
                    act_count=config.act_count,
                    concurrency=config.concurrency,
                    seeds=config.seeds,
                    perturb=config.perturb,
                    epoch=config.epoch,
                    scheduler_factory=factory,
                )

            if config.strategy is Strategy.OP_CASE:
                if cases is None:
                    raise ValueError("OP_CASE strategy requires cases=")

                return self._explore(
                    cases=cases,
                    count=config.count,
                    concurrency=config.concurrency,
                    seeds=config.seeds,
                    perturb=config.perturb,
                    epoch=config.epoch,
                )

            sc = scenario if scenario is not None else self.derive_scenario()

            if config.strategy is Strategy.SCENARIO:
                factory = (
                    pct_scheduler_factory(depth=config.pct_depth, steps=config.pct_steps)
                    if config.scheduler is SchedulerKind.PCT
                    else None
                )
                return self._explore_scenario(
                    sc,
                    act_count=config.act_count,
                    concurrency=config.concurrency,
                    seeds=config.seeds,
                    perturb=config.perturb,
                    epoch=config.epoch,
                    scheduler_factory=factory,
                )

            if config.strategy is Strategy.HYPOTHESIS:
                return self._explore_scenario_hypothesis(
                    sc,
                    max_act=config.act_count,
                    concurrency=config.concurrency,
                    perturb=config.perturb,
                    epoch=config.epoch,
                    max_examples=config.max_examples,
                )

            # DPOR — drives its own systematic scheduler over one fixed workload.
            return self._explore_scenario_dpor(
                sc,
                act_count=config.act_count,
                concurrency=config.concurrency,
                seed=config.dpor_seed,
                max_runs=config.max_runs,
                epoch=config.epoch,
            )

        finally:
            self._active_config = None

    # ....................... #

    def coverage(
        self,
        config: SimulationConfig,
        *,
        scenario: Scenario | None = None,
    ) -> CoverageStats:
        """Coverage-guided sweep: explore seeds while behavior grows, stop once it saturates.

        Each seed runs the (auto-derived or given) scenario once; its behavioral coverage —
        operation outcomes, port edges, injected faults — accumulates. The sweep stops early
        after ``config.coverage_plateau`` consecutive seeds add nothing new (the exploration has
        saturated), so a ``seeds=range(500)`` pool right-sizes itself instead of running all 500.
        If a seed violates an invariant the sweep stops there and the minimized report rides along
        on :attr:`CoverageStats.violation`. Faults / latency apply exactly as in :meth:`run`.

        Returns a :class:`CoverageStats` — how much behavior was exercised, which seeds added it,
        and whether it saturated. The streams are still seeded, so the whole sweep reproduces.
        """

        self._active_config = config
        try:
            sc = scenario if scenario is not None else self.derive_scenario()

            behaviors: set[Behavior] = set()
            new_by_seed: list[tuple[int, int]] = []
            seeds_run = 0
            plateau = 0
            plateaued = False
            violation: ViolationReport | None = None

            for seed in config.seeds:
                schedule_seed = (
                    derive_seed(seed, "schedule") if config.perturb else None
                )
                history, _ = self._run_scenario(
                    sc,
                    act_workload=None,
                    act_count=config.act_count,
                    concurrency=config.concurrency,
                    seed=seed,
                    schedule_seed=schedule_seed,
                    epoch=config.epoch,
                )
                seeds_run += 1

                covered = behavioral_coverage(history)
                fresh = covered - behaviors
                new_by_seed.append((seed, len(fresh)))
                behaviors |= covered

                if check(history, self.invariants):
                    # A bug beats coverage: stop and hand back the minimized counterexample.
                    violation = self._attempt_scenario(
                        sc,
                        act_count=config.act_count,
                        concurrency=config.concurrency,
                        seed=seed,
                        perturb=config.perturb,
                        epoch=config.epoch,
                    )
                    if violation is not None:
                        break

                if fresh:
                    plateau = 0
                else:
                    plateau += 1
                    if config.coverage_plateau and plateau >= config.coverage_plateau:
                        plateaued = True
                        break

            return CoverageStats(
                behaviors=frozenset(behaviors),
                seeds_run=seeds_run,
                new_by_seed=tuple(new_by_seed),
                plateaued=plateaued,
                violation=violation,
            )

        finally:
            self._active_config = None

    # ....................... #

    def _modules(self) -> tuple[DepsModule, ...]:
        """Build a fresh set of deps modules from the factory (fresh state per call)."""

        produced = self.deps()
        return (
            tuple(produced)
            if isinstance(produced, (list, tuple))
            else (cast("DepsModule", produced),)
        )

    # ....................... #

    def _registry_from_modules(
        self,
        modules: tuple[DepsModule, ...],
        seed: int,
        *,
        extra: Sequence[PortInterceptor] = (),
    ) -> FrozenDepsRegistry:
        """Freeze *modules* into a runtime-traced registry with the run's seam interceptors.

        *seed* is the run's **fault** sub-seed (``derive_seed(master, "fault")``). The
        declarative ``config.faults`` is compiled here with ``random.Random(seed)`` — seeded by
        construction, no caller RNG — and registered deps-scoped alongside any *extra*
        interceptors (e.g. the crash interceptor) and the manual :attr:`interceptors` factory, so
        every resolved configurable port runs through them. Passing the same *modules* twice
        rebuilds the registry over the SAME state — the seam the crash/restart restart relies on.
        """

        registry = DepsRegistry.from_modules(*modules).with_tracing(runtime=True)

        interceptors: list[PortInterceptor] = list(extra)

        if self.interceptors is not None:
            interceptors.extend(self.interceptors(seed))

        if self._active_config is not None and self._active_config.faults is not None:
            interceptors.append(
                compile_fault_policy(
                    self._active_config.faults,
                    random.Random(seed),  # nosec B311 - seeded sim faults, not crypto
                )
            )

        if interceptors:
            registry = registry.with_interceptors(*interceptors)

        return registry.freeze()

    # ....................... #

    def _frozen_registry(self, seed: int) -> FrozenDepsRegistry:
        """The run's frozen, runtime-traced deps registry over a fresh module set."""

        return self._registry_from_modules(self._modules(), seed)

    # ....................... #

    @asynccontextmanager
    async def _context(self, fault_seed: int) -> AsyncGenerator[ExecutionContext]:
        """Yield the run's :class:`ExecutionContext` — bare by default, runtime-scoped on opt-in.

        With ``SimulationConfig.runtime`` set, the context is driven inside the real
        :meth:`ExecutionRuntime.scope` (lifecycle startup before the body, graceful drain +
        shutdown after) — the faithful path. Otherwise a bare context is built directly (the
        proven default: no background-task interference). *fault_seed* is the run's fault
        sub-seed, threaded into the registry's seeded interceptors.
        """

        registry = self._frozen_registry(fault_seed)

        if self._active_config is not None and self._active_config.runtime:
            runtime = ExecutionRuntime(deps=registry, lifecycle=self.lifecycle)
            async with runtime.scope():
                yield runtime.get_context()
        else:
            yield ExecutionContext(deps=registry.resolve())

    # ....................... #

    def _latency_for(self, seed: int) -> LatencyModel | None:
        """The run's latency model: the config profile (compiled from the latency sub-seed) if
        set, else the manual :attr:`latency` callable escape hatch. *seed* is the master."""

        if self._active_config is not None and self._active_config.latency is not None:
            return compile_latency(
                self._active_config.latency,
                random.Random(  # nosec B311 - seeded sim latency, not crypto
                    derive_seed(seed, "latency")
                ),
            )

        return self.latency

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
        rng = random.Random(derive_seed(seed, "input"))  # nosec B311
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
            async with self._context(derive_seed(seed, "fault")) as ctx:
                if self.setup is not None:
                    await self.setup(ctx)

                semaphore = asyncio.Semaphore(concurrency)
                await asyncio.gather(
                    *(
                        self._run_call(
                            ctx, semaphore, call_id=index, op=call.op, arg=call.arg
                        )
                        for index, call in enumerate(workload)
                    )
                )

                if self.observe is not None:
                    await self.observe(ctx)

                _fold_runtime_trace(ctx)

        with bind_recorder(recorder):
            run_simulation(
                scenario,
                seed=derive_seed(seed, "entropy"),
                schedule_seed=schedule_seed,
                epoch=epoch,
                latency=self._latency_for(seed),
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
        schedule_seed = derive_seed(seed, "schedule") if perturb else None
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

    def _explore(
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
        """Run one operation concurrently, anchoring its span start.

        Only an ``op_start`` anchor (carrying the call id) is recorded here; the operation's
        **outcome** is the engine trace's to record (``run_operation`` emits an invoke→complete/
        error boundary, classified ``ok``/``failed``/``error``), and the harness projects per-op
        ``operation`` events from that single source when folding the trace (see
        :func:`_fold_runtime_trace`). A domain failure or a bug is swallowed here so one call's
        failure never aborts the concurrent batch; a :class:`SimulatedCrash` (a ``BaseException``)
        is *not* caught — it propagates to model the process dying.
        """

        async with semaphore:
            record_event("op_start", call_id=call_id, op=op)

            try:
                await run_operation(self.operations, op, arg, ctx)
            except Exception:  # nosec B110 # noqa: BLE001 — outcome captured by the engine trace; one call's failure must never abort the batch
                pass

    # ....................... #

    async def _run_arrange_call(
        self,
        ctx: ExecutionContext,
        *,
        call_id: int,
        op: str,
        arg: Any,
    ) -> tuple[bool, Any]:
        """Run one arrange operation serially; anchor its span and return ``(ok, result)``.

        A failed arrange op produces nothing into the model (``ok`` is ``False``). Only the
        ``op_start`` anchor is recorded; the outcome lives in the engine trace (projected at
        fold time). The result is returned directly so arrange can capture produced handles.
        """

        record_event("op_start", call_id=call_id, op=op)

        try:
            result = await run_operation(self.operations, op, arg, ctx)
            return True, result

        except Exception:
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

            async with self._context(derive_seed(seed, "fault")) as ctx:
                if self.setup is not None:
                    await self.setup(ctx)

                rng = random.Random(derive_seed(seed, "input"))  # nosec B311
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
                seed=derive_seed(seed, "entropy"),
                schedule_seed=schedule_seed,
                epoch=epoch,
                scheduler=scheduler,
                latency=self._latency_for(seed),
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
        schedule_seed = derive_seed(seed, "schedule") if perturb else None

        # A PCT scheduler is stateful (it consumes priorities/change points as it runs), so a
        # fresh instance is built per run — the initial run, every minimization predicate, and
        # the final replay must all explore the SAME schedule, or a counterexample minimized
        # against a mutated interleaving fails to reproduce from the reported seed.
        def make_scheduler() -> object | None:
            if scheduler_factory is None:
                return None
            return scheduler_factory(derive_seed(seed, "schedule"))

        def run(act: Sequence[tuple[str, Any]] | None) -> History:
            history, _ = self._run_scenario(
                scenario,
                act_workload=act,
                act_count=act_count,
                concurrency=concurrency,
                seed=seed,
                schedule_seed=schedule_seed,
                epoch=epoch,
                scheduler=make_scheduler(),
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
            scheduler=make_scheduler(),
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

    async def _drive_act(
        self,
        ctx: ExecutionContext,
        generated: Sequence[tuple[str, Any]],
        *,
        concurrency: int,
    ) -> bool:
        """Run the act workload concurrently; return whether a :class:`SimulatedCrash` fired.

        On a crash the surviving in-flight tasks are cancelled — the process *dies*, so no
        sibling operation keeps running into the restart phase (they share the loop).
        """

        semaphore = asyncio.Semaphore(concurrency)
        tasks = [
            asyncio.ensure_future(
                self._run_call(ctx, semaphore, call_id=index, op=op, arg=arg)
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

    def _run_crash_restart(
        self,
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

        The deps modules are built **once**, so the ``MockState`` they hold is the durable store
        that survives the crash. Phase 1 drives arrange + act on a bare context behind a seeded
        :class:`~forze_dst.faults.CrashPolicy`; when the crash fires the process dies (no graceful
        shutdown — the in-flight tx rolls back, committed state persists). Phase 2 restarts a
        **fresh** runtime over the SAME modules (lifecycle startup runs), drives the optional
        :attr:`recover` pass, then :attr:`observe` — all under the restart scope. Both phases'
        runtime traces are folded into one history, so the invariants see the whole arc.
        """

        config = self._active_config
        assert config is not None and config.crash is not None  # nosec B101 - run() guard
        crash_policy = config.crash

        recorder = Recorder(seed=seed)
        generated: list[tuple[str, Any]] = []
        modules = self._modules()
        fault_seed = derive_seed(seed, "fault")

        async def driver() -> None:
            nonlocal generated

            # --- Phase 1: workload under the seeded crash, on a bare (kill-able) context.
            crash = compile_crash(
                crash_policy,
                random.Random(derive_seed(seed, "crash")),  # nosec B311 - seeded sim crash
            )
            registry = self._registry_from_modules(
                modules, fault_seed, extra=(crash,)
            )
            ctx = ExecutionContext(deps=registry.resolve())
            rng = random.Random(derive_seed(seed, "input"))  # nosec B311
            state = scenario.state()

            try:
                if self.setup is not None:
                    await self.setup(ctx)

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
                else:
                    generated = scenario.generate_act(state, act_count, rng)

                await self._drive_act(ctx, generated, concurrency=concurrency)

            except SimulatedCrash:
                # A crash during setup/arrange (serial, outside the act gather).
                record_event("crash", phase="arrange")

            finally:
                _fold_runtime_trace(ctx)  # the pre-crash trace

            # --- Phase 2: restart over the SAME persisted store, full runtime lifecycle.
            restart = self._registry_from_modules(modules, fault_seed)
            runtime = ExecutionRuntime(deps=restart, lifecycle=self.lifecycle)

            async with runtime.scope():
                rctx = runtime.get_context()

                if self.recover is not None:
                    await self.recover(rctx)

                if self.observe is not None:
                    await self.observe(rctx)

                _fold_runtime_trace(rctx)  # the post-restart trace

        with bind_recorder(recorder):
            run_simulation(
                driver,
                seed=derive_seed(seed, "entropy"),
                schedule_seed=schedule_seed,
                scheduler=scheduler,
                epoch=epoch,
                latency=self._latency_for(seed),
            )

        return recorder.history, generated

    # ....................... #

    def _attempt_crash_restart(
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
        schedule_seed = derive_seed(seed, "schedule") if perturb else None

        # Fresh per run (a PCT scheduler is stateful) so the initial run, every minimization
        # predicate, and the final replay all explore the same interleaving.
        def make_scheduler() -> object | None:
            if scheduler_factory is None:
                return None
            return scheduler_factory(derive_seed(seed, "schedule"))

        def run(act: Sequence[tuple[str, Any]] | None) -> History:
            history, _ = self._run_crash_restart(
                scenario,
                act_workload=act,
                act_count=act_count,
                concurrency=concurrency,
                seed=seed,
                schedule_seed=schedule_seed,
                epoch=epoch,
                scheduler=make_scheduler(),
            )
            return history

        history, act_workload = self._run_crash_restart(
            scenario,
            act_workload=None,
            act_count=act_count,
            concurrency=concurrency,
            seed=seed,
            schedule_seed=schedule_seed,
            epoch=epoch,
            scheduler=make_scheduler(),
        )

        if not check(history, self.invariants):
            return None

        # Minimize the act phase; the seeded crash re-fires on whatever matched call survives.
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

    def _explore_crash_restart(
        self,
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
        post-recovery world violates an invariant (lost after-commit work, a partial
        non-transactional write) is minimized and reported, reproducible from that one seed.
        """

        for seed in seeds:
            report = self._attempt_crash_restart(
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

    def _explore_scenario(
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

    def _explore_scenario_hypothesis(
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
            return derive_seed(seed, "schedule") if perturb else None

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
                schedule_seed=schedule_seed_of(seed),  # pyright: ignore[reportUnknownArgumentType]
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
            schedule_seed=schedule_seed_of(seed),  # pyright: ignore[reportUnknownArgumentType]
            epoch=epoch,
        )

        return ViolationReport(
            seed=seed,
            schedule_seed=schedule_seed_of(seed),  # pyright: ignore[reportUnknownArgumentType]
            violations=tuple(check(history, self.invariants)),
            workload=tuple(generated),
            history=history,
            registry_fingerprint=self.fingerprint(),
        )

    # ....................... #

    def _explore_scenario_dpor(
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

            # Ops the harness drove directly carry an ``op_start`` anchor; a cascade (saga step
            # / event handler) is invoked deep in a handler and has none — so it shows up in the
            # trace's invokes but not here, which is exactly the cascade set.
            direct = {
                event.fields.get("op")
                for event in history.events
                if event.kind == "op_start"
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
