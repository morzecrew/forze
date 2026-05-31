"""Temporal workflow dep factories."""

from typing import Any, final

import attrs

from forze.application.contracts.durable.workflow import (
    DurableWorkflowCommandDepPort,
    DurableWorkflowQueryDepPort,
    DurableWorkflowSpec,
)
from forze.application.execution import ExecutionContext

from ....adapters import (
    TemporalWorkflowCommandAdapter,
    TemporalWorkflowQueryAdapter,
)
from ..configs import TemporalWorkflowConfig
from ..keys import TemporalClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableTemporalWorkflowQuery(DurableWorkflowQueryDepPort):
    """Configurable Temporal workflow query adapter."""

    config: TemporalWorkflowConfig = attrs.field(
        validator=attrs.validators.instance_of(TemporalWorkflowConfig),
    )
    """Configuration for the workflow."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: DurableWorkflowSpec[Any, Any],
    ) -> TemporalWorkflowQueryAdapter[Any, Any]:
        client = ctx.deps.provide(TemporalClientDepKey)

        return TemporalWorkflowQueryAdapter(
            client=client,
            queue=self.config.queue,
            spec=spec,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableTemporalWorkflowCommand(DurableWorkflowCommandDepPort):
    """Configurable Temporal workflow command adapter."""

    config: TemporalWorkflowConfig = attrs.field(
        validator=attrs.validators.instance_of(TemporalWorkflowConfig),
    )
    """Configuration for the workflow."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: DurableWorkflowSpec[Any, Any],
    ) -> TemporalWorkflowCommandAdapter[Any, Any]:
        client = ctx.deps.provide(TemporalClientDepKey)

        return TemporalWorkflowCommandAdapter(
            client=client,
            queue=self.config.queue,
            spec=spec,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
