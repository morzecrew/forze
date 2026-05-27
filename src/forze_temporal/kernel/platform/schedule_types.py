"""Shared types for Temporal schedule client operations."""

from dataclasses import dataclass

from forze.application.contracts.durable.workflow import (
    DurableWorkflowScheduleDescription,
)

# ----------------------- #


@dataclass(frozen=True, slots=True)
class TemporalScheduleListPage:
    """One page of schedule list results."""

    descriptions: tuple[DurableWorkflowScheduleDescription, ...]
    next_page_token: str | None
