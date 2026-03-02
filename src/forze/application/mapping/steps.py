"""Built-in mapping steps for common field injection.

Provides :class:`NumberIdStep` (counter-based ``number_id``) and
:class:`CreatorIdStep` (placeholder for actor-based ``creator_id``).
"""

from typing import TYPE_CHECKING

import attrs
from pydantic import BaseModel

from forze.base.primitives import JsonDict
from forze.domain.constants import CREATOR_ID_FIELD, NUMBER_ID_FIELD

from .mapper import MappingStep

if TYPE_CHECKING:
    from forze.application.execution import ExecutionContext

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class NumberIdStep(MappingStep):
    """Step that injects ``number_id`` from a counter in the execution context.

    Resolves :meth:`ExecutionContext.counter` for the given :attr:`namespace`,
    increments it, and returns a patch with the value. Requires a counter port
    to be registered for the namespace.
    """

    namespace: str
    """Counter namespace used to resolve the port via :meth:`ExecutionContext.counter`."""

    # ....................... #

    def produces(self) -> frozenset[str]:
        return frozenset({NUMBER_ID_FIELD})

    # ....................... #

    async def __call__(
        self,
        ctx: "ExecutionContext",
        source: BaseModel,
        payload: JsonDict,
    ) -> JsonDict:
        counter = ctx.counter(self.namespace)
        number_id = await counter.incr()

        return {NUMBER_ID_FIELD: number_id}


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class CreatorIdStep(MappingStep):
    """Placeholder step that would inject ``creator_id`` from the current actor.

    Not yet implemented; raises :exc:`NotImplementedError`. Intended for future
    integration with actor/tenant context to populate ``creator_id``.
    """

    def produces(self) -> frozenset[str]:
        return frozenset({CREATOR_ID_FIELD})

    # ....................... #

    async def __call__(
        self,
        ctx: "ExecutionContext",
        source: BaseModel,
        payload: JsonDict,
    ) -> JsonDict:
        raise NotImplementedError("CreatorIdStep is not implemented")
