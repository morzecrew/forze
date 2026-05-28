"""Factory functions for Inngest durable function adapters."""

from typing import Any, final

import attrs

from forze.application.contracts.durable.function import (
    DurableFunctionEventCommandDepPort,
    DurableFunctionEventSpec,
)
from forze.application.execution import ExecutionContext

from ...adapters import InngestEventCommandAdapter
from .configs import InngestEventConfig
from .keys import InngestClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableInngestEventCommand(DurableFunctionEventCommandDepPort):
    """Configurable Inngest event command adapter."""

    config: InngestEventConfig
    """Per-event route configuration."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: DurableFunctionEventSpec[Any],
    ) -> InngestEventCommandAdapter[Any]:
        client = ctx.deps.provide(InngestClientDepKey)

        include = self.config.get("include_execution_context", True)

        return InngestEventCommandAdapter(
            client=client,
            spec=spec,
            execution_ctx=ctx,
            include_execution_context=include,
        )
