"""Static execution plan introspection (`explain`)."""

from typing import final

import attrs

from forze.application.execution.engine.stages import (
    STEP_EXPLAIN_TX_BUCKET,
    ScheduleMode,
    Stage,
    StepExplainKind,
)

from ..engine.model import OperationStages
from ..registry.graph import DispatchGraph
from .spec import MiddlewareSpec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class StepExplainRow:
    """One scheduled step in :class:`ExecutionPlanReport`."""

    stage: str
    label: str
    priority: int
    requires: frozenset[str]
    provides: frozenset[str]
    schedule_index: int
    kind: StepExplainKind
    schedule_mode: ScheduleMode
    dispatch_edge_count: int = 0


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ExecutionPlanReport:
    """Static introspection for :meth:`UsecaseRegistry.explain`."""

    op: str
    has_transaction: bool
    steps: tuple[StepExplainRow, ...]
    inter_op_outgoing_edges: frozenset[tuple[str, str]] = frozenset()


def factory_label(spec: MiddlewareSpec) -> str:
    fact = spec.factory
    qualname = getattr(fact, "__qualname__", None)

    if isinstance(qualname, str) and qualname:
        return qualname

    name = getattr(fact, "__name__", None)

    if isinstance(name, str) and name:
        return name

    return repr(fact)


def stage_schedule_mode(
    stage: Stage,
    specs: tuple[MiddlewareSpec, ...],
) -> ScheduleMode:
    if not stage.schedulable:
        return ScheduleMode.legacy_priority

    if any(spec.requires or spec.provides for spec in specs):
        return ScheduleMode.capability_topo

    return ScheduleMode.legacy_priority


def ordered_specs_for_explain(
    stage: Stage,
    specs: tuple[MiddlewareSpec, ...],
) -> tuple[MiddlewareSpec, ...]:
    if stage.schedulable:
        from ..engine.capabilities import execution_ordered_specs

        return execution_ordered_specs(specs, stage=stage.value)

    return specs


def append_explain_rows(
    rows: list[StepExplainRow],
    stage: Stage,
    specs: tuple[MiddlewareSpec, ...],
    *,
    kind: StepExplainKind,
    schedule_mode: ScheduleMode,
    start_idx: int,
) -> int:
    idx = start_idx

    for spec in specs:
        rows.append(
            StepExplainRow(
                stage=stage.value,
                label=spec.step_label or factory_label(spec),
                priority=spec.priority,
                requires=spec.requires,
                provides=spec.provides,
                schedule_index=idx,
                kind=kind,
                schedule_mode=schedule_mode,
                dispatch_edge_count=0,
            )
        )
        idx += 1

    return idx


def build_execution_plan_report(
    *,
    plan: OperationStages,
    op: str,
    registry_graph: DispatchGraph | None = None,
) -> ExecutionPlanReport:
    rows: list[StepExplainRow] = []
    idx = 0
    tx_emitted = False

    for stage in Stage.iter_chain_order():
        if plan.tx_route is not None and stage.requires_tx and not tx_emitted:
            rows.append(
                StepExplainRow(
                    stage=STEP_EXPLAIN_TX_BUCKET,
                    label=f"TxMiddleware(route={plan.tx_route})",
                    priority=0,
                    requires=frozenset(),
                    provides=frozenset(),
                    schedule_index=idx,
                    kind=StepExplainKind.tx,
                    schedule_mode=ScheduleMode.legacy_priority,
                    dispatch_edge_count=0,
                )
            )
            idx += 1
            tx_emitted = True

        specs = plan.specs_for_chain(stage)

        if stage is Stage.after_commit and plan.tx_route is None:
            continue

        if not specs:
            continue

        if stage is Stage.after_commit and plan.tx_route is None:
            continue

        mode = stage_schedule_mode(stage, specs)
        ordered = ordered_specs_for_explain(stage, specs)
        idx = append_explain_rows(
            rows,
            stage,
            ordered,
            kind=stage.explain_kind,
            schedule_mode=mode,
            start_idx=idx,
        )

    outgoing = (
        registry_graph.outgoing_edges_for(op)
        if registry_graph is not None
        else frozenset[tuple[str, str]]()
    )

    return ExecutionPlanReport(
        op=op,
        has_transaction=plan.has_transaction,
        steps=tuple(rows),
        inter_op_outgoing_edges=outgoing,
    )
