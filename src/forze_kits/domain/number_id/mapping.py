from string import Formatter
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
            # The payload first, so an earlier step's value is the one named; the source model
            # second, because the payload carries only the fields a caller set, not defaults.
            model, payload = source
            name = payload.get(self.name_field, getattr(model, self.name_field, None))

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
    empty or absent value is left as it is. The counter allocates on its own connection, so a
    create that fails after this step leaves a gap in the numbering, transaction or not."""

    name_format: str = DEFAULT_NAME_FORMAT
    """How the field's value and the number combine, with ``{name}`` and ``{number}``."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.name_field is None:
            return

        refusal = exc.configuration(
            f"name_format {self.name_format!r} must be a format string whose fields are exactly "
            "{name} and {number} — no attribute or index access, which would reach past the "
            "value the step fills.",
        )

        try:
            fields = {field for _, field, _, _ in Formatter().parse(self.name_format)}
            # A format spec the value cannot take ({name:d}) would fail on every create.
            self.name_format.format(name="Order", number=1)

        except (KeyError, IndexError, ValueError, TypeError, AttributeError) as error:
            raise refusal from error

        if not fields - {None} <= {"name", "number"}:
            raise refusal

    # ....................... #

    def __call__(self, ctx: "ExecutionContext") -> NumberIdMappingStep:
        return NumberIdMappingStep(
            counter=ctx.counter(self.spec),
            name_field=self.name_field,
            name_format=self.name_format,
        )
