"""Shared literal types for execution plan reports and scheduling."""

from typing import Literal

# ----------------------- #

StepExplainKind = Literal["guard", "effect", "wrap", "finally", "on_failure", "tx"]
"""Row kind in :class:`~forze.application.execution.plan.StepExplainRow`."""

ScheduleMode = Literal["legacy_priority", "capability_topo"]
"""How specs in a bucket were ordered for :meth:`~forze.application.execution.plan.UsecasePlan.explain`."""
