"""Shared types for execution plan reports and scheduling."""

from enum import StrEnum

# ----------------------- #


class StepExplainKind(StrEnum):
    """Row kind in :class:`~forze.application.execution.plan.StepExplainRow`."""

    guard = "guard"
    effect = "effect"
    wrap = "wrap"
    finally_ = "finally"  # keyword-safe name; JSON/label value is ``finally``.
    on_failure = "on_failure"
    tx = "tx"


class ScheduleMode(StrEnum):
    """How specs in a bucket were ordered for :meth:`~forze.application.execution.plan.UsecasePlan.explain`."""

    legacy_priority = "legacy_priority"
    capability_topo = "capability_topo"


# Synthetic tx step (not a :class:`~forze.application.execution.bucket.BucketKey`).
STEP_EXPLAIN_TX_BUCKET = "tx"
