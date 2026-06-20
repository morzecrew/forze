"""Turnkey simulation harness — the thin :class:`Simulation` facade over the DST engines.

You give it your :class:`FrozenOperationRegistry`, a deps factory (typically
``lambda: MockDepsModule(...)`` — one module auto-mocks every port), and the invariants that must
hold. ``run`` / ``coverage`` / ``coverage_guided`` are the entrypoints; each binds the config as the
run-scoped :attr:`active_config` (so the run substrate can compile its seeded faults/latency) and
delegates to an engine under :mod:`forze_dst.engines` — one module per strategy (op-case, scenario +
Hypothesis + DPOR, crash/restart, coverage-guided mutation). The substrate they share lives in
:mod:`forze_dst.context`; trace folding in :mod:`forze_dst.projection`.

``deps`` is a *factory* (called fresh per run) so each run starts from clean state, and so this
package stays free of any adapter dependency — the app supplies the mock module.
"""

from __future__ import annotations

from datetime import datetime
from typing import Awaitable, Callable, Sequence

import attrs

from forze.application.execution import DepsModule, ExecutionContext
from forze.application.execution.interception import LatencyModel, PortInterceptor
from forze.application.execution.lifecycle import FrozenLifecyclePlan
from forze.application.execution.operations.registry import FrozenOperationRegistry
from forze_dst import engines
from forze_dst.cases import OperationCase
from forze_dst.config import SimulationConfig
from forze_dst.coverage import CoverageStats
from forze_dst.derive import DEFAULT_CREATE_VERBS
from forze_dst.explore_guided import GuidedStats
from forze_dst.invariants import Invariant
from forze_dst.oracle import ViolationReport
from forze_dst.reactive import ReactiveMap
from forze_dst.scenario import Scenario
from forze_dst.time_source import DEFAULT_EPOCH

# ----------------------- #

DepsFactory = Callable[[], "DepsModule | Sequence[DepsModule]"]
InterceptorFactory = Callable[[int], "Sequence[PortInterceptor]"]
Hook = Callable[[ExecutionContext], Awaitable[None]]

# ....................... #


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

    active_config: SimulationConfig | None = attrs.field(default=None, init=False)
    """The config of the in-progress :meth:`run`, so the per-run substrate can compile its
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

        # Run-scoped: the per-run substrate compiles this config's seeded faults/latency.
        self.active_config = config
        try:
            return engines.dispatch(self, config, scenario=scenario, cases=cases)
        finally:
            self.active_config = None

    # ....................... #

    def coverage(
        self,
        config: SimulationConfig,
        *,
        scenario: Scenario | None = None,
    ) -> CoverageStats:
        """Coverage sweep: explore seeds while behavior grows, stop once it saturates.

        Each seed runs the (auto-derived or given) scenario once; its behavioral coverage —
        operation outcomes, port edges, injected faults — accumulates, with reachability folded in.
        The sweep stops early after ``config.coverage_plateau`` consecutive seeds add nothing new,
        or at the first violating seed (whose minimized report rides on
        :attr:`~forze_dst.coverage.CoverageStats.violation`). Faults / latency apply as in
        :meth:`run`; the streams are still seeded, so the whole sweep reproduces.
        """

        self.active_config = config
        try:
            return engines.run_coverage(self, config, scenario=scenario)
        finally:
            self.active_config = None

    # ....................... #

    def coverage_guided(
        self,
        config: SimulationConfig,
        *,
        cases: Sequence[OperationCase],
    ) -> GuidedStats:
        """Coverage-guided **mutation** sweep over *cases* — feedback-directed, not uniform.

        Keeps a corpus of inputs that each unlocked new behavior and mutates the productive ones
        (tweak an op, grow/shrink the workload, re-roll the schedule + faults) under an AFL-style
        power schedule, so behavior gated behind a rare op combination is reached far sooner than by
        independent seeds. One seed-derived lineage rooted at the first of ``config.seeds`` and
        bounded by ``config.guided_budget``; it stops at the first violation with a minimized report
        on :attr:`~forze_dst.explore_guided.GuidedStats.violation`. Reproduces from the master seed.
        """

        self.active_config = config
        try:
            return engines.run_guided(self, config, cases=cases)
        finally:
            self.active_config = None

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

        For each derived entry-point operation, fire it once against the arranged state and read the
        engine trace: every operation invoked but not directly driven is a *cascade* (saga step /
        event handler), and every domain event dispatched along the way is recorded. The registries
        hold opaque callables, so this wiring is only knowable at runtime — this is how it is found.
        """

        return engines.reactive_map(
            self,
            create_verbs=create_verbs,
            arrange_each=arrange_each,
            seed=seed,
            epoch=epoch,
        )

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

        Starts from the static, name-driven catalog derivation; then, unless *probe* is disabled,
        recovers the reactive topology (see :meth:`reactive_map`) and drops operations only ever
        triggered as cascades (saga steps, domain-event handlers) — they fire automatically when
        their trigger runs, so driving them directly would be unrealistic.
        """

        return engines.derive_scenario(
            self,
            create_verbs=create_verbs,
            arrange_each=arrange_each,
            probe=probe,
            seed=seed,
            epoch=epoch,
        )
