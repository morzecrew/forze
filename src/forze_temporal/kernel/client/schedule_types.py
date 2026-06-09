"""Shared types for Temporal schedule client operations."""

import attrs

from forze.application.contracts.durable.workflow import (
    DurableWorkflowScheduleDescription,
)

# ----------------------- #


@attrs.define(frozen=True, slots=True)
class TemporalScheduleListPage:
    """One page of schedule list results."""

    descriptions: tuple[DurableWorkflowScheduleDescription, ...]
    next_page_token: str | None
