from collections.abc import Sequence
from typing import Any

from forze.application.contracts.deps import DepKey
from forze.application.contracts.durable.workflow import (
    DurableWorkflowScheduleBootstrap,
)

from ...kernel.client import TemporalClientPort

# ----------------------- #

TemporalClientDepKey: DepKey[TemporalClientPort] = DepKey("temporal_client")
"""Key used to register a Temporal client (single cluster or routed) in the deps container."""

TemporalScheduleBootstrapDepKey: DepKey[Sequence[DurableWorkflowScheduleBootstrap[Any]]] = DepKey(
    "temporal_schedule_bootstrap",
)
"""Declarative workflow schedules upserted during Temporal lifecycle startup."""
