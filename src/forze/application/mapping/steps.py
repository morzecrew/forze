from typing import TYPE_CHECKING

import attrs
from pydantic import BaseModel

from forze.base.primitives import JsonDict
from forze.domain.constants import CREATOR_ID_FIELD, NUMBER_ID_FIELD

from .mapper import MappingStep

if TYPE_CHECKING:
    from forze.application.execution import ExecutionContext

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class NumberIdStep(MappingStep):
    namespace: str

    # ....................... #

    def produces(self) -> frozenset[str]:
        return frozenset({NUMBER_ID_FIELD})

    # ....................... #

    async def __call__(
        self,
        ctx: "ExecutionContext",
        source: BaseModel,
        payload: JsonDict,
    ) -> JsonDict:
        counter = ctx.counter(self.namespace)
        number_id = await counter.incr()

        return {NUMBER_ID_FIELD: number_id}


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class CreatorIdStep(MappingStep):
    def produces(self) -> frozenset[str]:
        return frozenset({CREATOR_ID_FIELD})

    # ....................... #

    async def __call__(
        self,
        ctx: "ExecutionContext",
        source: BaseModel,
        payload: JsonDict,
    ) -> JsonDict:
        raise NotImplementedError("CreatorIdStep is not implemented")
