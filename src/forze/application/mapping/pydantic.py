from typing import Generic, Self, TypeVar, cast

import attrs
from pydantic import BaseModel

from forze.application.contracts.mapping import Mapper
from forze.base.primitives import JsonDict
from forze.base.serialization import apply_dict_patch, pydantic_dump, pydantic_validate
from forze.domain.models import BaseDTO

# ----------------------- #

In = TypeVar("In", bound=BaseModel)
Out = TypeVar("Out", bound=BaseDTO)

PydanticMapperStep = Mapper[tuple[In, JsonDict], JsonDict]

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PydanticMapper(Mapper[In, Out], Generic[In, Out]):
    """Pipeline that maps a Pydantic source model to an output DTO."""

    in_: type[In]
    """Source model class for validation."""

    out: type[Out]
    """Target DTO model class for validation."""

    steps: tuple[PydanticMapperStep[In], ...] = attrs.field(factory=tuple)
    """Ordered sequence of mapping steps."""

    # ....................... #

    def with_steps(self, *steps: PydanticMapperStep[In]) -> Self:
        """Return a new mapper with additional steps appended."""

        return attrs.evolve(self, steps=(*self.steps, *steps))

    # ....................... #

    async def __call__(self, source: In) -> Out:
        """Map a Pydantic source model to an output DTO."""

        if self.in_ is self.out and not self.steps:
            return cast(Out, source)

        payload = pydantic_dump(source, exclude={"unset": True})

        for step in self.steps:
            patch = await step((source, payload))
            payload = apply_dict_patch(payload, patch)

        result = pydantic_validate(self.out, payload)

        return result
