from collections.abc import Sequence
from enum import StrEnum
from typing import Any, Mapping, final

import attrs

from forze.application.contracts.deps import DepKey
from forze.application.contracts.workflow import (
    WorkflowCommandDepKey,
    WorkflowQueryDepKey,
    WorkflowScheduleBootstrap,
    WorkflowScheduleCommandDepKey,
    WorkflowScheduleQueryDepKey,
)
from forze.application.execution import Deps, DepsModule

from ...kernel.platform import TemporalClientPort
from .configs import TemporalWorkflowConfig
from .deps import (
    ConfigurableTemporalWorkflowCommand,
    ConfigurableTemporalWorkflowQuery,
    ConfigurableTemporalWorkflowScheduleCommand,
    ConfigurableTemporalWorkflowScheduleQuery,
)
from .keys import TemporalClientDepKey, TemporalScheduleBootstrapDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class TemporalDepsModule[K: str | StrEnum](DepsModule[K]):
    """Dependency module that registers Temporal clients and adapters."""

    client: TemporalClientPort
    """Pre-constructed Temporal client (single cluster or routed, not connected until lifecycle)."""

    workflows: Mapping[K, TemporalWorkflowConfig] | None = attrs.field(default=None)
    """Mapping from workflow names to their Temporal-specific configurations."""

    schedule_bootstraps: Sequence[WorkflowScheduleBootstrap[Any]] | None = attrs.field(
        default=None,
    )
    """Declarative schedules upserted on Temporal lifecycle startup."""

    # ....................... #

    def __call__(self) -> Deps[K]:
        """Build a dependency container with Temporal-backed ports."""

        plain: dict[DepKey[Any], Any] = {TemporalClientDepKey: self.client}

        if self.schedule_bootstraps:
            plain[TemporalScheduleBootstrapDepKey] = self.schedule_bootstraps

        plain_deps = Deps[K].plain(plain)
        workflow_deps = Deps[K]()

        if self.workflows:
            workflow_deps = workflow_deps.merge(
                Deps[K].routed(
                    {
                        WorkflowQueryDepKey: {
                            name: ConfigurableTemporalWorkflowQuery(config=config)
                            for name, config in self.workflows.items()
                        },
                        WorkflowCommandDepKey: {
                            name: ConfigurableTemporalWorkflowCommand(config=config)
                            for name, config in self.workflows.items()
                        },
                        WorkflowScheduleQueryDepKey: {
                            name: ConfigurableTemporalWorkflowScheduleQuery(
                                config=config
                            )
                            for name, config in self.workflows.items()
                        },
                        WorkflowScheduleCommandDepKey: {
                            name: ConfigurableTemporalWorkflowScheduleCommand(
                                config=config,
                            )
                            for name, config in self.workflows.items()
                        },
                    }
                )
            )

        return plain_deps.merge(workflow_deps)
