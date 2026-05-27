import attrs
from pydantic import BaseModel

from forze.application.contracts.counter import CounterPort, CounterSpec
from forze.application.execution.context import ExecutionContext
from forze.application.mapping import (
    PydanticPipelineMapperStep,
    PydanticPipelineMapperStepFactory,
)
from forze.base.primitives import JsonDict

from .constants import NUMBER_ID_FIELD

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class NumberIdMappingStep(PydanticPipelineMapperStep[BaseModel]):
    """Mapping step that adds a number ID to the source model."""

    counter: CounterPort
    """Counter port."""

    # ....................... #

    async def __call__(self, source: tuple[BaseModel, JsonDict]) -> JsonDict:
        num = await self.counter.incr()

        return {NUMBER_ID_FIELD: num}


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class NumberIdMappingStepFactory(PydanticPipelineMapperStepFactory[BaseModel]):
    """Factory that builds a number ID mapping step."""

    spec: CounterSpec
    """Counter specification."""

    # ....................... #

    def __call__(self, ctx: "ExecutionContext") -> NumberIdMappingStep:
        return NumberIdMappingStep(counter=ctx.counter(self.spec))
