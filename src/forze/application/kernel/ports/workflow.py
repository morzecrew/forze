"""Port for long-running workflow orchestration engines."""

from typing import Any, Optional, Protocol, Sequence

from forze.base.primitives import JsonDict

# ----------------------- #
#! TODO: support status retrieval and success / failure handling or/and tracing


class WorkflowPort(Protocol):
    """Abstraction over a workflow engine such as Temporal or similar.

    The port is intentionally minimal and models only the operations the
    application kernel needs for starting and signalling workflows.
    """

    async def start(
        self,
        name: str,
        id: str,  # ? UUID?
        args: Sequence[Any],
        queue: Optional[str] = None,
    ) -> None:
        """Start a new workflow instance.

        :param name: Workflow type/name registered in the engine.
        :param id: External identifier for the workflow instance.
        :param args: Positional arguments forwarded to the workflow start call.
        :param queue: Optional task queue or routing key.
        """

    async def signal(
        self,
        id: str,  # ? UUID?
        signal: str,
        data: Sequence[JsonDict],  # ? support for pydantic models ?
    ) -> None:
        """Send a signal to an existing workflow instance.

        :param id: Workflow instance identifier.
        :param signal: Signal name to invoke.
        :param data: Payload items delivered with the signal.
        """
