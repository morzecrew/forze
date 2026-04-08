from typing import Any, final

import attrs

from forze.application.contracts.workflow import (
    WorkflowCommandDepPort,
    WorkflowQueryDepPort,
    WorkflowSpec,
)
from forze.application.execution import ExecutionContext

from ...adapters import TemporalWorkflowCommandAdapter, TemporalWorkflowQueryAdapter
from .configs import TemporalWorkflowConfig
from .keys import TemporalClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableTemporalWorkflowQuery(WorkflowQueryDepPort):
    """Configurable Temporal workflow query adapter."""

    config: TemporalWorkflowConfig
    """Configuration for the workflow."""

    # ....................... #

    def __call__(
        self, ctx: ExecutionContext, spec: WorkflowSpec[Any, Any]
    ) -> TemporalWorkflowQueryAdapter[Any, Any]:
        client = ctx.dep(TemporalClientDepKey)

        return TemporalWorkflowQueryAdapter(
            client=client,
            queue=self.config["queue"],
            spec=spec,
            tenant_aware=self.config.get("tenant_aware", False),
            tenant_provider=ctx.get_tenant_id,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableTemporalWorkflowCommand(WorkflowCommandDepPort):
    """Configurable Temporal workflow command adapter."""

    config: TemporalWorkflowConfig
    """Configuration for the workflow."""

    # ....................... #

    def __call__(
        self, ctx: ExecutionContext, spec: WorkflowSpec[Any, Any]
    ) -> TemporalWorkflowCommandAdapter[Any, Any]:
        client = ctx.dep(TemporalClientDepKey)

        return TemporalWorkflowCommandAdapter(
            client=client,
            queue=self.config["queue"],
            spec=spec,
            tenant_aware=self.config.get("tenant_aware", False),
            tenant_provider=ctx.get_tenant_id,
        )
