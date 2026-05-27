from typing import Any, Awaitable, Generic, Protocol, runtime_checkable

from .ports import BaseWorkflowPort
from .specs import (
    In,
    WorkflowScheduleDescription,
    WorkflowScheduleHandle,
    WorkflowScheduleTiming,
)

# ----------------------- #


@runtime_checkable
class WorkflowScheduleCommandPort(
    BaseWorkflowPort[In, Any],
    Generic[In],
    Protocol,
):
    """Port for managing workflow schedules (recurring or delayed starts)."""

    def create(
        self,
        schedule_id: str,
        args: In,
        timing: WorkflowScheduleTiming,
        *,
        workflow_id_template: str | None = None,
        trigger_immediately: bool = False,
        note: str | None = None,
    ) -> Awaitable[WorkflowScheduleHandle]:
        """Create a schedule; raise if it already exists."""
        ...  # pragma: no cover

    # ....................... #

    def upsert(
        self,
        schedule_id: str,
        args: In,
        timing: WorkflowScheduleTiming,
        *,
        workflow_id_template: str | None = None,
        trigger_immediately: bool = False,
        note: str | None = None,
    ) -> Awaitable[WorkflowScheduleHandle]:
        """Create a schedule or update it when it already exists."""
        ...  # pragma: no cover

    # ....................... #

    def update(
        self,
        handle: WorkflowScheduleHandle,
        *,
        timing: WorkflowScheduleTiming | None = None,
        args: In | None = None,
        workflow_id_template: str | None = None,
        note: str | None = None,
    ) -> Awaitable[None]:
        """Update an existing schedule."""
        ...  # pragma: no cover

    # ....................... #

    def delete(self, handle: WorkflowScheduleHandle) -> Awaitable[None]:
        """Delete a schedule."""
        ...  # pragma: no cover

    # ....................... #

    def pause(
        self,
        handle: WorkflowScheduleHandle,
        *,
        note: str | None = None,
    ) -> Awaitable[None]:
        """Pause a schedule."""
        ...  # pragma: no cover

    # ....................... #

    def unpause(
        self,
        handle: WorkflowScheduleHandle,
        *,
        note: str | None = None,
    ) -> Awaitable[None]:
        """Unpause a schedule."""
        ...  # pragma: no cover

    # ....................... #

    def trigger(self, handle: WorkflowScheduleHandle) -> Awaitable[None]:
        """Trigger a schedule to fire immediately."""
        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class WorkflowScheduleQueryPort(
    BaseWorkflowPort[In, Any],
    Generic[In],
    Protocol,
):
    """Port for reading workflow schedule state."""

    def describe(
        self,
        handle: WorkflowScheduleHandle,
    ) -> Awaitable[WorkflowScheduleDescription]:
        """Describe a schedule."""
        ...  # pragma: no cover

    # ....................... #

    def list(
        self,
        *,
        limit: int | None = None,
        next_page_token: str | None = None,
    ) -> Awaitable[tuple[tuple[WorkflowScheduleDescription, ...], str | None]]:
        """List schedules whose action targets this port's workflow spec."""
        ...  # pragma: no cover
