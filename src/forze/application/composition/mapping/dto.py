from typing import Generic, Self, TypeVar, cast, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.mapping import MapperPort
from forze.application.execution import ExecutionContext
from forze.base.errors import CoreError
from forze.base.primitives import JsonDict
from forze.base.serialization import apply_dict_patch, pydantic_dump, pydantic_validate
from forze.domain.models import BaseDTO

# ----------------------- #

In = TypeVar("In", bound=BaseModel)
Out = TypeVar("Out", bound=BaseDTO)

DTOMapperStep = MapperPort[tuple[In, JsonDict], JsonDict]

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DTOMapper(MapperPort[In, Out], Generic[In, Out]):
    """Pipeline that maps a Pydantic source model to an output DTO."""

    requires_ctx: bool = False
    """Whether the mapper requires a context."""

    in_: type[In]
    """Source model class for validation."""

    out: type[Out]
    """Target DTO model class for validation."""

    steps: tuple[DTOMapperStep[In], ...] = attrs.field(factory=tuple)
    """Ordered sequence of mapping steps."""

    # ....................... #

    def with_steps(self, *steps: DTOMapperStep[In]) -> Self:
        """Return a new mapper with additional steps appended."""

        return attrs.evolve(self, steps=(*self.steps, *steps))

    # ....................... #

    async def __call__(
        self,
        source: In,
        /,
        *,
        ctx: ExecutionContext | None = None,
    ) -> Out:
        if self.in_ is self.out and not self.steps:
            return cast(Out, source)

        if self.requires_ctx and ctx is None:
            raise CoreError("Execution context is required for this mapping")

        #! unset should be configurable ?
        payload = pydantic_dump(source, exclude={"unset": True})

        for _, step in enumerate(self.steps, start=1):
            patch = await step((source, payload), ctx=ctx)

            payload = apply_dict_patch(payload, patch)

        result = pydantic_validate(self.out, payload)

        return result
