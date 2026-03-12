from typing import TYPE_CHECKING, Protocol

from pydantic import BaseModel

from forze.base.primitives import JsonDict

if TYPE_CHECKING:
    from forze.application.execution import ExecutionContext

# ----------------------- #


class MappingStep[In: BaseModel](Protocol):
    """Protocol for a single step in a DTO mapping pipeline.

    Each step declares which output fields it produces via :meth:`produces` and
    contributes a patch dict when invoked. Steps run in sequence; the payload
    is passed through and merged with each step's patch. Implementers must not
    produce overlapping fields with other steps in the same mapper.
    """

    def produces(self) -> frozenset[str]:
        """Return the set of field names this step writes into the payload."""
        ...

    async def __call__(
        self,
        ctx: "ExecutionContext",
        source: In,
        payload: JsonDict,
    ) -> JsonDict:
        """Compute a patch dict to merge into the payload.

        :param ctx: Execution context for resolving ports (e.g. counters).
        :param source: Original Pydantic model being mapped.
        :param payload: Current payload dict (may be mutated conceptually via patch).
        :returns: Dict of field names to values; merged into payload.
        """
        ...
