from typing import TYPE_CHECKING, Protocol

from pydantic import BaseModel

from forze.base.primitives import JsonDict

if TYPE_CHECKING:
    from forze.application.execution import ExecutionContext

# ----------------------- #


class MappingStep(Protocol):
    def produces(self) -> frozenset[str]: ...

    async def __call__(
        self,
        ctx: "ExecutionContext",
        source: BaseModel,
        payload: JsonDict,
    ) -> JsonDict: ...
