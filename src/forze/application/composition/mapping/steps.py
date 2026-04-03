from typing import final

import attrs
from pydantic import BaseModel

from forze.application.execution import ExecutionContext
from forze.base.errors import CoreError
from forze.base.primitives import JsonDict
from forze.domain.constants import NUMBER_ID_FIELD
from forze.application.contracts.counter import CounterSpec

from .dto import DTOMapperStep

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class NumberIdStep(DTOMapperStep[BaseModel]):
    """Step that injects ``number_id`` using a counter port"""

    spec: CounterSpec
    """Counter spec used to resolve the port via :meth:`ExecutionContext.counter`."""

    # ....................... #

    async def __call__(
        self,
        source: tuple[BaseModel, JsonDict],
        /,
        *,
        ctx: ExecutionContext | None = None,
    ) -> JsonDict:
        if ctx is None:
            raise CoreError("Execution context is required for this step")

        cnt = ctx.counter(self.spec)
        number_id = await cnt.incr()

        return {NUMBER_ID_FIELD: number_id}


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class CreatorIdStep(DTOMapperStep[BaseModel]):
    """Step that injects ``creator_id`` using the current actor"""

    async def __call__(
        self,
        source: tuple[BaseModel, JsonDict],
        /,
        *,
        ctx: ExecutionContext | None = None,
    ) -> JsonDict:
        if ctx is None:
            raise CoreError("Execution context is required for this step")

        raise NotImplementedError("CreatorIdStep is not implemented")
