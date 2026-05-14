"""Static execution plan introspection (`explain`)."""

from typing import final

import attrs

from forze.application.execution.bucket import BucketKey, Phase
from forze.application.execution.plan_kinds import (
    STEP_EXPLAIN_TX_BUCKET,
    ScheduleMode,
    StepExplainKind,
)

from ..capabilities.scheduler import schedule_capability_specs
from .operation import OperationPlan
from .spec import MiddlewareSpec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class StepExplainRow:
    """One scheduled step in :class:`ExecutionPlanReport`."""

    bucket: str
    label: str
    priority: int
    requires: frozenset[str]
    provides: frozenset[str]
    schedule_index: int
    kind: StepExplainKind
    schedule_mode: ScheduleMode
    dispatch_edge_count: int = 0


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ExecutionPlanReport:
    """Static introspection for :meth:`UsecasePlan.explain`."""

    op: str
    use_capability_engine: bool
    has_transaction: bool
    steps: tuple[StepExplainRow, ...]


# ....................... #


def factory_label(spec: MiddlewareSpec) -> str:
    f = spec.factory
    qn = getattr(f, "__qualname__", None)

    if isinstance(qn, str) and qn:
        return qn

    name = getattr(f, "__name__", None)

    if isinstance(name, str) and name:
        return name

    return repr(f)


# ....................... #


def bucket_schedule_mode(
    key: BucketKey,
    specs: tuple[MiddlewareSpec, ...],
    *,
    use_capability_engine: bool,
) -> ScheduleMode:
    if not use_capability_engine:
        return ScheduleMode.legacy_priority

    if not key.capability_schedulable:
        return ScheduleMode.legacy_priority

    if any(s.requires or s.provides for s in specs):
        return ScheduleMode.capability_topo

    return ScheduleMode.legacy_priority


# ....................... #


def ordered_specs_for_explain(
    key: BucketKey,
    specs: tuple[MiddlewareSpec, ...],
    *,
    use_capability_engine: bool,
) -> tuple[MiddlewareSpec, ...]:
    if use_capability_engine and key.capability_schedulable:
        return schedule_capability_specs(specs, bucket=key.label)

    return specs


# ....................... #


def append_explain_rows(
    rows: list[StepExplainRow],
    bucket_label: str,
    specs: tuple[MiddlewareSpec, ...],
    *,
    kind: StepExplainKind,
    schedule_mode: ScheduleMode,
    start_idx: int,
) -> int:
    idx = start_idx

    for s in specs:
        rows.append(
            StepExplainRow(
                bucket=bucket_label,
                label=s.step_label or factory_label(s),
                priority=s.priority,
                requires=s.requires,
                provides=s.provides,
                schedule_index=idx,
                kind=kind,
                schedule_mode=schedule_mode,
                dispatch_edge_count=len(s.dispatch_edges),
            )
        )

        idx += 1

    return idx


# ....................... #


def build_execution_plan_report(
    *,
    plan: OperationPlan,
    op: str,
    use_capability_engine: bool,
) -> ExecutionPlanReport:
    rows: list[StepExplainRow] = []
    idx = 0
    tx_emitted = False

    for key in BucketKey.iter_chain_order():
        if plan.tx is not None and key.phase is Phase.in_tx and not tx_emitted:
            rows.append(
                StepExplainRow(
                    bucket=STEP_EXPLAIN_TX_BUCKET,
                    label=f"TxMiddleware(route={str(plan.tx.route)})",
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

        if plan.tx is None:
            if key.phase is Phase.in_tx or key is BucketKey.AFTER_COMMIT:
                continue

        specs = plan.specs_for_chain(key)
        mode = bucket_schedule_mode(
            key, specs, use_capability_engine=use_capability_engine
        )
        ordered = ordered_specs_for_explain(
            key, specs, use_capability_engine=use_capability_engine
        )
        idx = append_explain_rows(
            rows,
            key.label,
            ordered,
            kind=key.explain_kind,
            schedule_mode=mode,
            start_idx=idx,
        )

    return ExecutionPlanReport(
        op=op,
        use_capability_engine=use_capability_engine,
        has_transaction=plan.tx is not None,
        steps=tuple(rows),
    )
