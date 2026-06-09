from typing import TYPE_CHECKING, Self, TypeVar, cast

import attrs
from pydantic import BaseModel

from forze.application.contracts.mapping import Mapper, MapperFactory
from forze.base.primitives import JsonDict
from forze.base.serialization import PydanticModelCodec, apply_dict_patch
from forze.domain.models import BaseDTO

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #

In = TypeVar("In", bound=BaseModel)

PydanticPipelineMapperStep = Mapper[tuple[In, JsonDict], JsonDict]
"""Mapping step that maps a Pydantic source model to a JSON dictionary."""

PydanticPipelineMapperStepFactory = MapperFactory[tuple[In, JsonDict], JsonDict]
"""Factory that builds a mapping step for a Pydantic source model to a JSON dictionary."""

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PydanticPipelineMapper[In: BaseModel, Out: BaseDTO](Mapper[In, Out]):
    """Pipeline mapper that maps a Pydantic source model to an output DTO."""

    in_: type[In]
    """Source model class for validation."""

    out: type[Out]
    """Target DTO model class for validation."""

    steps: tuple[PydanticPipelineMapperStep[In], ...] = attrs.field(factory=tuple)
    """Ordered sequence of mapping steps."""

    # ....................... #

    def with_steps(self, *steps: PydanticPipelineMapperStep[In]) -> Self:
        """Return a new mapper with additional steps appended."""

        return attrs.evolve(self, steps=(*self.steps, *steps))

    # ....................... #

    async def __call__(self, source: In) -> Out:
        """Map a Pydantic source model to an output DTO."""

        if self.in_ is self.out and not self.steps:
            return cast(Out, source)

        if not self.steps:
            return PydanticModelCodec(self.out).transform(source)

        payload = PydanticModelCodec(self.in_).encode_mapping(
            source,
            exclude={"unset": True},
        )

        for step in self.steps:
            patch = await step((source, payload))
            payload = apply_dict_patch(payload, patch)

        return PydanticModelCodec(self.out).decode_mapping(payload)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PydanticPipelineMapperFactory[In: BaseModel, Out: BaseDTO](
    MapperFactory[In, Out]
):
    """Factory that builds a pipeline mapper for a Pydantic source model to an output DTO."""

    in_: type[In]
    """Source model class for validation."""

    out: type[Out]
    """Target DTO model class for validation."""

    step_factories: tuple[PydanticPipelineMapperStepFactory[In], ...] = attrs.field(
        factory=tuple
    )
    """Ordered sequence of mapping step factories."""

    # ....................... #

    def __call__(self, ctx: "ExecutionContext") -> PydanticPipelineMapper[In, Out]:
        """Build a pipeline mapper for a Pydantic source model to an output DTO."""

        steps = [step_factory(ctx) for step_factory in self.step_factories]

        return PydanticPipelineMapper(
            in_=self.in_,
            out=self.out,
            steps=tuple(steps),
        )
