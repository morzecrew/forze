from typing import Final

import attrs
from pydantic import BaseModel

from forze.application.contracts.counter import CounterPort, CounterSpec
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze_kits.mapping import (
    PydanticPipelineMapperStep,
    PydanticPipelineMapperStepFactory,
)

from .constants import NUMBER_ID_FIELD

# ----------------------- #

DEFAULT_NAME_FORMAT: Final[str] = "{name} #{number}"
"""How the number joins the named field by default: ``"Order"`` becomes ``"Order #12"``."""

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class NumberIdMappingStep(PydanticPipelineMapperStep[BaseModel]):
    """Mapping step that adds a number ID to the source model."""

    counter: CounterPort
    """Counter port."""

    name_field: str | None = None
    """A field to append the number to, or ``None`` to leave every field but the number alone."""

    name_format: str = DEFAULT_NAME_FORMAT
    """How the field's value and the number combine (``{name}`` and ``{number}``)."""

    # ....................... #

    async def __call__(self, source: tuple[BaseModel, JsonDict]) -> JsonDict:
        num = await self.counter.incr()
        patch: JsonDict = {NUMBER_ID_FIELD: num}

        if self.name_field is not None:
            # Read from the pipeline's payload, so an earlier step's value is the one named.
            name = source[1].get(self.name_field)

            # Nothing to append to: inventing a name is the caller's call, not the step's.
            if isinstance(name, str) and name:
                patch[self.name_field] = self.name_format.format(name=name, number=num)

        return patch


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class NumberIdMappingStepFactory(PydanticPipelineMapperStepFactory[BaseModel]):
    """Factory that builds a number ID mapping step."""

    spec: CounterSpec
    """Counter specification."""

    name_field: str | None = None
    """A field to append the number to (e.g. ``"name"``), or ``None`` to leave it alone. An
    empty or absent value is left as it is."""

    name_format: str = DEFAULT_NAME_FORMAT
    """How the field's value and the number combine, with ``{name}`` and ``{number}``."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.name_field is None:
            return

        try:
            self.name_format.format(name="", number=0)

        except (KeyError, IndexError, ValueError) as error:
            raise exc.configuration(
                f"name_format {self.name_format!r} must be a format string using only "
                "{name} and {number}.",
            ) from error

    # ....................... #

    def __call__(self, ctx: "ExecutionContext") -> NumberIdMappingStep:
        return NumberIdMappingStep(
            counter=ctx.counter(self.spec),
            name_field=self.name_field,
            name_format=self.name_format,
        )
