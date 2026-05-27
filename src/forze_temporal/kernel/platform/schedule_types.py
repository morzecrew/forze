"""Shared types for Temporal schedule client operations."""

from dataclasses import dataclass

from forze.application.contracts.workflow import WorkflowScheduleDescription

# ----------------------- #


@dataclass(frozen=True, slots=True)
class TemporalScheduleListPage:
    """One page of schedule list results."""

    descriptions: tuple[WorkflowScheduleDescription, ...]
    next_page_token: str | None
