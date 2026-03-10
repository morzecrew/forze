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
    """Pipeline that maps a Pydantic source model to an output DTO.

    Dumps the source to a dict (excluding unset fields), runs each
    :class:`MappingStep` in order, merges patches, and validates the result
    into :attr:`out`. Steps must not produce overlapping fields; overwrites
    are governed by :attr:`policy`. Use :meth:`with_steps` to build mappers
    incrementally.
    """

    out: type[Out]
    """Target DTO model class for validation."""

    steps: tuple[MappingStep, ...] = attrs.field(factory=tuple)
    """Ordered sequence of mapping steps."""

    policy: MappingPolicy = attrs.field(factory=MappingPolicy)
    """Policy for allowing field overwrites."""

    _step_fields: tuple[frozenset[str], ...] = attrs.field(init=False, eq=False, repr=False)
    """Pre-computed produces() results per step."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        used: set[str] = set()
        step_fields: list[frozenset[str]] = []

        for step in self.steps:
            produces = step.produces()
            overlap = used.intersection(produces)

            if overlap:
                raise CoreError(
                    f"Mapping steps conflict: fields {sorted(overlap)} "
                    f"are produced by multiple steps"
                )

            used.update(produces)
            step_fields.append(frozenset(produces))

        object.__setattr__(self, "_step_fields", tuple(step_fields))

    # ....................... #

    def with_steps(self, *steps: MappingStep) -> Self:
        """Return a new mapper with additional steps appended.

        :param steps: Steps to append to the pipeline.
        :returns: A new :class:`DTOMapper` instance.
        :raises CoreError: If any step produces fields already produced by others.
        """
        return attrs.evolve(self, steps=(*self.steps, *steps))

    # ....................... #

    async def __call__(self, ctx: "ExecutionContext", source: BaseModel) -> Out:
        """Map the source model to the output DTO.

        Runs each step in sequence, merging patches into the payload. Raises
        :exc:`CoreError` if a step would overwrite a field not allowed by
        :attr:`policy`.

        :param ctx: Execution context for step resolution.
        :param source: Pydantic model to map.
        :returns: Validated instance of :attr:`out`.
        :raises CoreError: On step conflict or disallowed overwrite.
        """
        payload = pydantic_dump(source, exclude={"unset": True})

        for step, fields in zip(self.steps, self._step_fields, strict=True):
            patch = await step(ctx, source, payload)

            for k in fields:
                if k in payload and payload.get(k) != patch.get(k):
                    if not self.policy.can_overwrite(k):
                        raise CoreError(f"Field {k} is not allowed to be overwritten")

            payload = apply_dict_patch(payload, patch)

        return pydantic_validate(self.out, payload, forbid_extra=True)
