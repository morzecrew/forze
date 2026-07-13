from collections.abc import Callable

import attrs
from pydantic import BaseModel

from forze.application.contracts.authn import AuthnIdentity
from forze.application.execution.context import ExecutionContext
from forze.base.primitives import JsonDict
from forze_kits.mapping import (
    PydanticPipelineMapperStep,
    PydanticPipelineMapperStepFactory,
)

from .constants import CREATOR_ID_FIELD

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class CreatorIdMappingStep(PydanticPipelineMapperStep[BaseModel]):
    """Mapping step that adds a creator ID to the source model."""

    resolver: Callable[[], AuthnIdentity | None]
    """Authn identity resolver."""

    # ....................... #

    async def __call__(self, source: tuple[BaseModel, JsonDict]) -> JsonDict:
        identity = self.resolver()

        if identity is None:
            return {CREATOR_ID_FIELD: None}

        return {CREATOR_ID_FIELD: identity.principal_id}


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class CreatorIdMappingStepFactory(PydanticPipelineMapperStepFactory[BaseModel]):
    """Factory that builds a creator ID mapping step."""

    def __call__(self, ctx: "ExecutionContext") -> CreatorIdMappingStep:
        return CreatorIdMappingStep(resolver=ctx.inv_ctx.get_authn)
