"""Scenario derivation + reactive probing — recover the cascade topology, infer a draft scenario.

The operation registries hold opaque callables, so the reactive wiring (which ops are saga steps /
event handlers, which domain events fire) is only knowable at runtime. :func:`reactive_map` recovers
it by firing each candidate op once and reading the engine trace; :func:`derive_scenario` starts from
the static catalog derivation and drops the cascade-only ops, so the auto-derived workload drives
only realistic entry points. Logic only — the caller manages the run-scoped ``active_config``.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from forze_dst.derive import DEFAULT_CREATE_VERBS
from forze_dst.derive import derive_scenario as _derive_from_catalog
from forze_dst.engines import scenario as scenario_engine
from forze_dst.reactive import ReactiveMap
from forze_dst.scenario import Scenario
from forze_dst.time_source import DEFAULT_EPOCH

if TYPE_CHECKING:
    from forze_dst.harness import Simulation

# ----------------------- #


def reactive_map(
    sim: Simulation,
    *,
    create_verbs: frozenset[str] = DEFAULT_CREATE_VERBS,
    arrange_each: int = 1,
    seed: int = 0,
    epoch: datetime = DEFAULT_EPOCH,
) -> ReactiveMap:
    """Recover the reactive cascade topology by probing each candidate operation.

    For each operation the catalog derivation treats as an entry point, fire it once against the
    arranged state and read the engine trace: every operation invoked but not directly driven is a
    *cascade* (saga step / event handler), and every domain event dispatched along the way is
    recorded. The operation registries hold opaque callables, so this wiring is only knowable at
    runtime — this is how it is recovered.
    """

    base = _derive_from_catalog(
        sim.operations, create_verbs=create_verbs, arrange_each=arrange_each
    )

    cascades: dict[str, frozenset[str]] = {}
    events: dict[str, frozenset[str]] = {}

    for rule in base.act:
        probe = Scenario(state=base.state, arrange=base.arrange, act=(rule,))
        history, _ = scenario_engine.run_scenario(
            sim,
            probe,
            act_workload=None,
            act_count=1,
            concurrency=1,
            seed=seed,
            schedule_seed=None,
            epoch=epoch,
        )

        # Ops the harness drove directly carry an ``op_start`` anchor; a cascade (saga step /
        # event handler) is invoked deep in a handler and has none — so it shows up in the trace's
        # invokes but not here, which is exactly the cascade set.
        direct = {event.fields.get("op") for event in history.events if event.kind == "op_start"}
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

        cascades[rule.op] = frozenset(str(op) for op in (invoked - direct) if op is not None)
        events[rule.op] = frozenset(str(name) for name in dispatched if name is not None)

    return ReactiveMap(cascades=cascades, events=events)


# ....................... #


def derive_scenario(
    sim: Simulation,
    *,
    create_verbs: frozenset[str] = DEFAULT_CREATE_VERBS,
    arrange_each: int = 1,
    probe: bool = True,
    seed: int = 0,
    epoch: datetime = DEFAULT_EPOCH,
) -> Scenario:
    """Infer a draft :class:`Scenario` from the catalog, then refine it reactively.

    Starts from the static, name-driven catalog derivation (see
    :func:`forze_dst.derive.derive_scenario`); then, unless *probe* is disabled, recovers the
    reactive cascade topology (see :func:`reactive_map`) and drops operations that are only ever
    triggered as cascades (saga steps, domain-event handlers) — they fire automatically when their
    trigger runs, so driving them directly would be unrealistic.
    """

    base = _derive_from_catalog(
        sim.operations, create_verbs=create_verbs, arrange_each=arrange_each
    )

    if not probe:
        return base

    reactive = reactive_map(
        sim,
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
