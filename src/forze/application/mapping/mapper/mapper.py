from typing import TYPE_CHECKING, Self, cast, final

import attrs
from pydantic import BaseModel

from forze.base.errors import CoreError
from forze.base.logging import getLogger
from forze.base.serialization import apply_dict_patch, pydantic_dump, pydantic_validate
from forze.domain.models import BaseDTO

from .policy import MappingPolicy
from .step import MappingStep

if TYPE_CHECKING:
    from forze.application.execution import ExecutionContext

# ----------------------- #

logger = getLogger(__name__).bind(scope="mapping")

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DTOMapper[In: BaseModel, Out: BaseDTO]:
    """Pipeline that maps a Pydantic source model to an output DTO.

    Dumps the source to a dict (excluding unset fields), runs each
    :class:`MappingStep` in order, merges patches, and validates the result
    into :attr:`out`. Steps must not produce overlapping fields; overwrites
    are governed by :attr:`policy`. Use :meth:`with_steps` to build mappers
    incrementally.
    """

    in_: type[In]
    """Source model class for validation."""

    out: type[Out]
    """Target DTO model class for validation."""

    steps: tuple[MappingStep[In], ...] = attrs.field(factory=tuple)
    """Ordered sequence of mapping steps."""

    policy: MappingPolicy = attrs.field(factory=MappingPolicy)
    """Policy for allowing field overwrites."""

    _step_fields: tuple[frozenset[str], ...] = attrs.field(
        init=False,
        eq=False,
        repr=False,
    )
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
        logger.trace(
            "DTOMapper initialized: {in_} -> {out} with {count} step(s), fields={fields}",
            sub={
                "in_": self.in_.__qualname__,
                "out": self.out.__qualname__,
                "count": len(self.steps),
                "fields": tuple(step_fields),
            },
        )

    # ....................... #

    def with_steps(self, *steps: MappingStep[In]) -> Self:
        """Return a new mapper with additional steps appended.

        :param steps: Steps to append to the pipeline.
        :returns: A new :class:`DTOMapper` instance.
        :raises CoreError: If any step produces fields already produced by others.
        """

        return attrs.evolve(self, steps=(*self.steps, *steps))

    # ....................... #

    async def __call__(
        self,
        ctx: "ExecutionContext",
        source: In,
    ) -> Out:
        """Map the source model to the output DTO.

        Runs each step in sequence, merging patches into the payload. Raises
        :exc:`CoreError` if a step would overwrite a field not allowed by
        :attr:`policy`.

        :param ctx: Execution context for step resolution.
        :param source: Source model to map.
        :returns: Validated instance of :attr:`out`.
        :raises CoreError: On step conflict or disallowed overwrite.
        """

        logger.debug(
            "Mapping {in_} -> {out}",
            sub={"in_": self.in_.__qualname__, "out": self.out.__qualname__},
        )

        with logger.section():
            if self.in_ is self.out and not self.steps:
                logger.trace(
                    "Source and target are the same class and no steps are defined, returning source directly"
                )
                return cast(Out, source)

            payload = pydantic_dump(source, exclude={"unset": True})
            logger.trace("Initial payload keys: {keys}", sub={"keys": tuple(payload.keys())})

            for i, (step, fields) in enumerate(
                zip(self.steps, self._step_fields, strict=True)
            ):
                logger.trace(
                    "Running step {index}/{total} ({qualname}), produces {fields}",
                    sub={
                        "index": i + 1,
                        "total": len(self.steps),
                        "qualname": type(step).__qualname__,
                        "fields": tuple(fields),
                    },
                )
                patch = await step(ctx, source, payload)

                for k in fields:
                    if k in payload and payload.get(k) != patch.get(k):
                        if not self.policy.can_overwrite(k):
                            raise CoreError(
                                f"Field {k} is not allowed to be overwritten"
                            )

                        logger.trace(
                            "Overwriting field {field} (allowed by policy)",
                            sub={"field": k},
                        )

                payload = apply_dict_patch(payload, patch)

            result = pydantic_validate(self.out, payload)

        return result
