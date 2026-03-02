from typing import TYPE_CHECKING, Self, final

import attrs
from pydantic import BaseModel

from forze.base.errors import CoreError
from forze.base.serialization import apply_dict_patch, pydantic_dump, pydantic_validate
from forze.domain.models import BaseDTO

from .policy import MappingPolicy
from .step import MappingStep

if TYPE_CHECKING:
    from forze.application.execution import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DTOMapper[Out: BaseDTO]:
    out: type[Out]
    steps: tuple[MappingStep, ...] = attrs.field(factory=tuple)
    policy: MappingPolicy = attrs.field(factory=MappingPolicy)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        used: set[str] = set()

        for step in self.steps:
            produces = step.produces()
            overlap = used.intersection(produces)

            if overlap:
                raise CoreError(
                    f"Mapping steps conflict: fields {sorted(overlap)} "
                    f"are produced by multiple steps"
                )

            used.update(produces)

    # ....................... #

    def with_steps(self, *steps: MappingStep) -> Self:
        return attrs.evolve(self, steps=(*self.steps, *steps))

    # ....................... #

    async def __call__(self, ctx: "ExecutionContext", source: BaseModel) -> Out:
        payload = pydantic_dump(source, exclude={"unset": True})

        for step in self.steps:
            patch = await step(ctx, source, payload)

            for k in step.produces():
                if k in payload and payload.get(k) != patch.get(k):
                    if not self.policy.can_overwrite(k):
                        raise CoreError(f"Field {k} is not allowed to be overwritten")

            payload = apply_dict_patch(payload, patch)

        return pydantic_validate(self.out, payload, forbid_extra=True)
