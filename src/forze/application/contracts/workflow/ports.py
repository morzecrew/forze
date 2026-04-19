from typing import Awaitable, Generic, Protocol, runtime_checkable

from pydantic import BaseModel

from .specs import (
    In,
    Out,
    WorkflowHandle,
    WorkflowQuerySpec,
    WorkflowSignalSpec,
    WorkflowSpec,
    WorkflowUpdateSpec,
)

# ----------------------- #


class BaseWorkflowPort(Protocol, Generic[In, Out]):
    """Base port for long-running workflow orchestration engines."""

    spec: WorkflowSpec[In, Out]
    """The specification of the workflow."""


# ....................... #


@runtime_checkable
class WorkflowCommandPort(BaseWorkflowPort[In, Out], Generic[In, Out], Protocol):
    """Port for commands on long-running workflow orchestration engines."""

    def start(
        self,
        args: In,
        *,
        workflow_id: str | None = None,
        raise_on_already_started: bool = True,
    ) -> Awaitable[WorkflowHandle]:
        """Start a new workflow run."""
        ...  # pragma: no cover

    # ....................... #

    def signal[S: BaseModel](
        self,
        handle: WorkflowHandle,
        *,
        signal: WorkflowSignalSpec[S],
        args: S,
    ) -> Awaitable[None]:
        """Send a signal to an existing workflow instance."""
        ...  # pragma: no cover

    # ....................... #

    def update[U: BaseModel, Res: BaseModel](
        self,
        handle: WorkflowHandle,
        *,
        update: WorkflowUpdateSpec[U, Res],
        args: U,
    ) -> Awaitable[Res]:
        """Update an existing workflow instance."""
        ...  # pragma: no cover

    # ....................... #

    def cancel(self, handle: WorkflowHandle) -> Awaitable[None]:
        """Cancel a running workflow instance."""
        ...  # pragma: no cover

    # ....................... #

    def terminate(
        self,
        handle: WorkflowHandle,
        *,
        reason: str | None = None,
    ) -> Awaitable[None]:
        """Terminate a running workflow instance."""
        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class WorkflowQueryPort(BaseWorkflowPort[In, Out], Generic[In, Out], Protocol):
    """Port for queries on long-running workflow orchestration engines."""

    def query[Q: BaseModel, Res: BaseModel](
        self,
        handle: WorkflowHandle,
        *,
        query: WorkflowQuerySpec[Q, Res],
        args: Q,
    ) -> Awaitable[Res]:
        """Query an existing workflow instance."""
        ...  # pragma: no cover

    # ....................... #

    def result(self, handle: WorkflowHandle) -> Awaitable[Out]:
        """Get the result of a workflow run."""
        ...  # pragma: no cover
