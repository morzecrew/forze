from typing import Any, final

import attrs

from forze.application.contracts.workflow import (
    WorkflowCommandDepPort,
    WorkflowQueryDepPort,
    WorkflowScheduleCommandDepPort,
    WorkflowScheduleQueryDepPort,
    WorkflowSpec,
)
from forze.application.execution import ExecutionContext

from ...adapters import (
    TemporalWorkflowCommandAdapter,
    TemporalWorkflowQueryAdapter,
    TemporalWorkflowScheduleCommandAdapter,
    TemporalWorkflowScheduleQueryAdapter,
)
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
        self,
        ctx: ExecutionContext,
        spec: WorkflowSpec[Any, Any],
    ) -> TemporalWorkflowQueryAdapter[Any, Any]:
        client = ctx.deps.provide(TemporalClientDepKey)

        return TemporalWorkflowQueryAdapter(
            client=client,
            queue=self.config["queue"],
            spec=spec,
            tenant_aware=self.config.get("tenant_aware", False),
            tenant_provider=ctx.inv.get_tenant,
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
        self,
        ctx: ExecutionContext,
        spec: WorkflowSpec[Any, Any],
    ) -> TemporalWorkflowCommandAdapter[Any, Any]:
        client = ctx.deps.provide(TemporalClientDepKey)

        return TemporalWorkflowCommandAdapter(
            client=client,
            queue=self.config["queue"],
            spec=spec,
            tenant_aware=self.config.get("tenant_aware", False),
            tenant_provider=ctx.inv.get_tenant,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableTemporalWorkflowScheduleQuery(WorkflowScheduleQueryDepPort):
    """Configurable Temporal workflow schedule query adapter."""

    config: TemporalWorkflowConfig
    """Configuration for the workflow."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: WorkflowSpec[Any, Any],
    ) -> TemporalWorkflowScheduleQueryAdapter[Any]:
        client = ctx.deps.provide(TemporalClientDepKey)

        return TemporalWorkflowScheduleQueryAdapter(
            client=client,
            queue=self.config["queue"],
            spec=spec,
            tenant_aware=self.config.get("tenant_aware", False),
            tenant_provider=ctx.inv.get_tenant,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableTemporalWorkflowScheduleCommand(WorkflowScheduleCommandDepPort):
    """Configurable Temporal workflow schedule command adapter."""

    config: TemporalWorkflowConfig
    """Configuration for the workflow."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: WorkflowSpec[Any, Any],
    ) -> TemporalWorkflowScheduleCommandAdapter[Any]:
        client = ctx.deps.provide(TemporalClientDepKey)

        return TemporalWorkflowScheduleCommandAdapter(
            client=client,
            queue=self.config["queue"],
            spec=spec,
            tenant_aware=self.config.get("tenant_aware", False),
            tenant_provider=ctx.inv.get_tenant,
        )
