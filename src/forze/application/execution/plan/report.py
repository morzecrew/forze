"""Static execution plan introspection (`explain`)."""

from typing import cast, final

import attrs

from forze.application.execution.bucket import (
    BUCKET_REGISTRY,
    CAPABILITY_SCHEDULABLE_BUCKETS,
    Bucket,
    coerce_bucket,
)
from forze.application.execution.plan_kinds import ScheduleMode, StepExplainKind

from ..capabilities.scheduler import schedule_capability_specs
from ..capabilities.trace import SchedulableCapabilitySpec
from .operation import OperationPlan
from .ordering import middleware_specs_for_usecase_tuple
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
    bucket: Bucket | str,
    specs: tuple[MiddlewareSpec, ...],
    *,
    use_capability_engine: bool,
) -> ScheduleMode:
    b = coerce_bucket(bucket)
    if not use_capability_engine:
        return "legacy_priority"

    if b not in CAPABILITY_SCHEDULABLE_BUCKETS:
        return "legacy_priority"

    if any(s.requires or s.provides for s in specs):
        return "capability_topo"

    return "legacy_priority"


# ....................... #


def ordered_specs_for_explain(
    bucket: Bucket | str,
    specs: tuple[MiddlewareSpec, ...],
    *,
    use_capability_engine: bool,
) -> tuple[MiddlewareSpec, ...]:
    b = coerce_bucket(bucket)
    if use_capability_engine and b in CAPABILITY_SCHEDULABLE_BUCKETS:
        return cast(
            tuple[MiddlewareSpec, ...],
            schedule_capability_specs(
                cast(tuple[SchedulableCapabilitySpec, ...], specs),
                bucket=b.value,
            ),
        )

    return specs


# ....................... #


def append_explain_rows(
    rows: list[StepExplainRow],
    bucket: Bucket | str,
    specs: tuple[MiddlewareSpec, ...],
    *,
    kind: StepExplainKind,
    schedule_mode: ScheduleMode,
    start_idx: int,
) -> int:
    idx = start_idx
    b = coerce_bucket(bucket).value

    for s in specs:
        rows.append(
            StepExplainRow(
                bucket=b,
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

    outer_segments: tuple[Bucket, ...] = (
        Bucket.outer_before,
        Bucket.outer_wrap,
        Bucket.outer_finally,
        Bucket.outer_on_failure,
    )

    for bucket in outer_segments:
        meta = BUCKET_REGISTRY[bucket]
        specs = middleware_specs_for_usecase_tuple(plan, bucket)
        mode = bucket_schedule_mode(
            bucket, specs, use_capability_engine=use_capability_engine
        )
        ordered = ordered_specs_for_explain(
            bucket, specs, use_capability_engine=use_capability_engine
        )
        idx = append_explain_rows(
            rows,
            bucket,
            ordered,
            kind=meta.explain_kind,
            schedule_mode=mode,
            start_idx=idx,
        )

    if plan.tx is not None:
        rows.append(
            StepExplainRow(
                bucket="tx",
                label=f"TxMiddleware(route={str(plan.tx.route)})",
                priority=0,
                requires=frozenset(),
                provides=frozenset(),
                schedule_index=idx,
                kind="tx",
                schedule_mode="legacy_priority",
                dispatch_edge_count=0,
            )
        )
        idx += 1

        in_tx_segments: tuple[Bucket, ...] = (
            Bucket.in_tx_before,
            Bucket.in_tx_finally,
            Bucket.in_tx_on_failure,
            Bucket.in_tx_wrap,
        )

        for bucket in in_tx_segments:
            meta = BUCKET_REGISTRY[bucket]
            specs = middleware_specs_for_usecase_tuple(plan, bucket)
            mode = bucket_schedule_mode(
                bucket, specs, use_capability_engine=use_capability_engine
            )
            ordered = ordered_specs_for_explain(
                bucket, specs, use_capability_engine=use_capability_engine
            )
            idx = append_explain_rows(
                rows,
                bucket,
                ordered,
                kind=meta.explain_kind,
                schedule_mode=mode,
                start_idx=idx,
            )

        for bucket in (Bucket.in_tx_after, Bucket.after_commit):
            meta = BUCKET_REGISTRY[bucket]
            specs = middleware_specs_for_usecase_tuple(plan, bucket)
            mode = bucket_schedule_mode(
                bucket, specs, use_capability_engine=use_capability_engine
            )
            ordered = ordered_specs_for_explain(
                bucket, specs, use_capability_engine=use_capability_engine
            )
            idx = append_explain_rows(
                rows,
                bucket,
                ordered,
                kind=meta.explain_kind,
                schedule_mode=mode,
                start_idx=idx,
            )

    oa_specs = middleware_specs_for_usecase_tuple(plan, Bucket.outer_after)
    oa_mode = bucket_schedule_mode(
        Bucket.outer_after, oa_specs, use_capability_engine=use_capability_engine
    )
    oa_ordered = ordered_specs_for_explain(
        Bucket.outer_after, oa_specs, use_capability_engine=use_capability_engine
    )
    append_explain_rows(
        rows,
        Bucket.outer_after,
        oa_ordered,
        kind=BUCKET_REGISTRY[Bucket.outer_after].explain_kind,
        schedule_mode=oa_mode,
        start_idx=idx,
    )

    return ExecutionPlanReport(
        op=op,
        use_capability_engine=use_capability_engine,
        has_transaction=plan.tx is not None,
        steps=tuple(rows),
    )
