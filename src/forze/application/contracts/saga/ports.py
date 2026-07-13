"""Port for executing a saga definition."""

from collections.abc import Awaitable
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

    from .value_objects import SagaDefinition

# ----------------------- #


@runtime_checkable
class SagaExecutorPort(Protocol):
    """Runs a saga definition, compensating completed steps in reverse on failure."""

    def run[Ctx](
        self,
        ctx: "ExecutionContext",
        definition: "SagaDefinition[Ctx]",
        initial: Ctx,
    ) -> Awaitable[Ctx]:
        """Run *definition* from *initial*; on a step failure, compensate the completed
        steps in reverse and raise. Returns the final saga context on success.
        """

        ...  # pragma: no cover
