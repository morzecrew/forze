"""Temporal workflow schedule dep factories."""

from typing import Any, final

import attrs

from forze.application.contracts.durable.workflow import (
    DurableWorkflowScheduleCommandDepPort,
    DurableWorkflowScheduleQueryDepPort,
    DurableWorkflowSpec,
)
from forze.application.execution import ExecutionContext

from ....adapters import (
    TemporalWorkflowScheduleCommandAdapter,
    TemporalWorkflowScheduleQueryAdapter,
)
from ..configs import TemporalWorkflowConfig
from ..keys import TemporalClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableTemporalWorkflowScheduleQuery(DurableWorkflowScheduleQueryDepPort):
    """Configurable Temporal workflow schedule query adapter."""

    config: TemporalWorkflowConfig = attrs.field(
        validator=attrs.validators.instance_of(TemporalWorkflowConfig),
    )
    """Configuration for the workflow."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: DurableWorkflowSpec[Any, Any],
    ) -> TemporalWorkflowScheduleQueryAdapter[Any]:
        client = ctx.deps.provide(TemporalClientDepKey)

        return TemporalWorkflowScheduleQueryAdapter(
            client=client,
            queue=self.config.queue,
            spec=spec,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableTemporalWorkflowScheduleCommand(
    DurableWorkflowScheduleCommandDepPort
):
    """Configurable Temporal workflow schedule command adapter."""

    config: TemporalWorkflowConfig = attrs.field(
        validator=attrs.validators.instance_of(TemporalWorkflowConfig),
    )
    """Configuration for the workflow."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: DurableWorkflowSpec[Any, Any],
    ) -> TemporalWorkflowScheduleCommandAdapter[Any]:
        client = ctx.deps.provide(TemporalClientDepKey)

        return TemporalWorkflowScheduleCommandAdapter(
            client=client,
            queue=self.config.queue,
            spec=spec,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
