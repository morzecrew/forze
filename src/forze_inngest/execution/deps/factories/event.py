"""Inngest durable function event dep factory."""

from typing import Any, final

import attrs

from forze.application.contracts.durable.function import (
    DurableFunctionEventCommandDepPort,
    DurableFunctionEventSpec,
)
from forze.application.execution import ExecutionContext

from ....adapters import InngestEventCommandAdapter
from ..configs import InngestEventConfig
from ..keys import InngestClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableInngestEventCommand(DurableFunctionEventCommandDepPort):
    """Configurable Inngest event command adapter."""

    config: InngestEventConfig = attrs.field(
        validator=attrs.validators.instance_of(InngestEventConfig),
    )
    """Per-event route configuration."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: DurableFunctionEventSpec[Any],
    ) -> InngestEventCommandAdapter[Any]:
        client = ctx.deps.provide(InngestClientDepKey)

        include = self.config.include_execution_context

        return InngestEventCommandAdapter(
            client=client,
            spec=spec,
            execution_ctx=ctx,
            include_execution_context=include,
        )
