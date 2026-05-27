from typing import Any, Awaitable, Generic, Protocol, runtime_checkable

from .ports import BaseDurableWorkflowPort
from .specs import (
    DurableWorkflowScheduleDescription,
    DurableWorkflowScheduleHandle,
    DurableWorkflowScheduleTiming,
    In,
)

# ----------------------- #


@runtime_checkable
class DurableWorkflowScheduleCommandPort(
    BaseDurableWorkflowPort[In, Any],
    Generic[In],
    Protocol,
):
    """Port for managing workflow schedules (recurring or delayed starts)."""

    def create(
        self,
        schedule_id: str,
        args: In,
        timing: DurableWorkflowScheduleTiming,
        *,
        workflow_id_template: str | None = None,
        trigger_immediately: bool = False,
        note: str | None = None,
    ) -> Awaitable[DurableWorkflowScheduleHandle]:
        """Create a schedule; raise if it already exists."""
        ...  # pragma: no cover

    # ....................... #

    def upsert(
        self,
        schedule_id: str,
        args: In,
        timing: DurableWorkflowScheduleTiming,
        *,
        workflow_id_template: str | None = None,
        trigger_immediately: bool = False,
        note: str | None = None,
    ) -> Awaitable[DurableWorkflowScheduleHandle]:
        """Create a schedule or update it when it already exists."""
        ...  # pragma: no cover

    # ....................... #

    def update(
        self,
        handle: DurableWorkflowScheduleHandle,
        *,
        timing: DurableWorkflowScheduleTiming | None = None,
        args: In | None = None,
        workflow_id_template: str | None = None,
        note: str | None = None,
    ) -> Awaitable[None]:
        """Update an existing schedule."""
        ...  # pragma: no cover

    # ....................... #

    def delete(self, handle: DurableWorkflowScheduleHandle) -> Awaitable[None]:
        """Delete a schedule."""
        ...  # pragma: no cover

    # ....................... #

    def pause(
        self,
        handle: DurableWorkflowScheduleHandle,
        *,
        note: str | None = None,
    ) -> Awaitable[None]:
        """Pause a schedule."""
        ...  # pragma: no cover

    # ....................... #

    def unpause(
        self,
        handle: DurableWorkflowScheduleHandle,
        *,
        note: str | None = None,
    ) -> Awaitable[None]:
        """Unpause a schedule."""
        ...  # pragma: no cover

    # ....................... #

    def trigger(self, handle: DurableWorkflowScheduleHandle) -> Awaitable[None]:
        """Trigger a schedule to fire immediately."""
        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class DurableWorkflowScheduleQueryPort(
    BaseDurableWorkflowPort[In, Any],
    Generic[In],
    Protocol,
):
    """Port for reading workflow schedule state."""

    def describe(
        self,
        handle: DurableWorkflowScheduleHandle,
    ) -> Awaitable[DurableWorkflowScheduleDescription]:
        """Describe a schedule."""
        ...  # pragma: no cover

    # ....................... #

    def list(
        self,
        *,
        limit: int | None = None,
        next_page_token: str | None = None,
    ) -> Awaitable[tuple[tuple[DurableWorkflowScheduleDescription, ...], str | None]]:
        """List schedules whose action targets this port's workflow spec."""
        ...  # pragma: no cover
