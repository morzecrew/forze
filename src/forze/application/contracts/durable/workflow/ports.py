from typing import Awaitable, Generic, Protocol, runtime_checkable

from pydantic import BaseModel

from .specs import (
    DurableWorkflowHandle,
    DurableWorkflowQuerySpec,
    DurableWorkflowRunDescription,
    DurableWorkflowSignalSpec,
    DurableWorkflowSpec,
    DurableWorkflowUpdateSpec,
    In,
    Out,
)

# ----------------------- #


class BaseDurableWorkflowPort(Protocol, Generic[In, Out]):
    """Base port for long-running workflow orchestration engines."""

    spec: DurableWorkflowSpec[In, Out]
    """The specification of the workflow."""


# ....................... #


@runtime_checkable
class DurableWorkflowCommandPort(
    BaseDurableWorkflowPort[In, Out], Generic[In, Out], Protocol
):
    """Port for commands on long-running workflow orchestration engines.

    Child workflows and continue-as-new are deliberately **not** exposed on
    this contract: they only exist *inside* a workflow definition, which is
    already backend-native code — use the raw SDK there (e.g. Temporal's
    ``workflow.execute_child_workflow`` / ``workflow.continue_as_new``), per
    the escape-hatch policy. This port stays the app-side surface for
    starting and steering runs from outside the workflow.
    """

    def start(
        self,
        args: In,
        *,
        workflow_id: str | None = None,
        raise_on_already_started: bool = True,
    ) -> Awaitable[DurableWorkflowHandle]:
        """Start a new workflow run."""
        ...  # pragma: no cover

    # ....................... #

    def signal[S: BaseModel](
        self,
        handle: DurableWorkflowHandle,
        *,
        signal: DurableWorkflowSignalSpec[S],
        args: S,
    ) -> Awaitable[None]:
        """Send a signal to an existing workflow instance."""
        ...  # pragma: no cover

    # ....................... #

    def update[U: BaseModel, Res: BaseModel](
        self,
        handle: DurableWorkflowHandle,
        *,
        update: DurableWorkflowUpdateSpec[U, Res],
        args: U,
    ) -> Awaitable[Res]:
        """Update an existing workflow instance."""
        ...  # pragma: no cover

    # ....................... #

    def cancel(self, handle: DurableWorkflowHandle) -> Awaitable[None]:
        """Cancel a running workflow instance."""
        ...  # pragma: no cover

    # ....................... #

    def terminate(
        self,
        handle: DurableWorkflowHandle,
        *,
        reason: str | None = None,
    ) -> Awaitable[None]:
        """Terminate a running workflow instance."""
        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class DurableWorkflowQueryPort(
    BaseDurableWorkflowPort[In, Out], Generic[In, Out], Protocol
):
    """Port for queries on long-running workflow orchestration engines."""

    def query[Q: BaseModel, Res: BaseModel](
        self,
        handle: DurableWorkflowHandle,
        *,
        query: DurableWorkflowQuerySpec[Q, Res],
        args: Q,
    ) -> Awaitable[Res]:
        """Query an existing workflow instance."""
        ...  # pragma: no cover

    # ....................... #

    def result(self, handle: DurableWorkflowHandle) -> Awaitable[Out]:
        """Get the result of a workflow run."""
        ...  # pragma: no cover

    # ....................... #

    def describe(
        self,
        handle: DurableWorkflowHandle,
    ) -> Awaitable[DurableWorkflowRunDescription]:
        """Return coarse execution status for a workflow run."""
        ...  # pragma: no cover
