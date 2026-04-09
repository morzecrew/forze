from enum import StrEnum
from typing import Mapping, final

import attrs

from forze.application.contracts.workflow import (
    WorkflowCommandDepKey,
    WorkflowQueryDepKey,
)
from forze.application.execution import Deps, DepsModule

from ...kernel.platform import TemporalClient
from .configs import TemporalWorkflowConfig
from .deps import ConfigurableTemporalWorkflowCommand, ConfigurableTemporalWorkflowQuery
from .keys import TemporalClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class TemporalDepsModule[K: str | StrEnum](DepsModule):
    """Dependency module that registers Temporal clients and adapters."""

    client: TemporalClient
    """Pre-constructed Temporal client (connection not yet initialized)."""

    workflows: Mapping[K, TemporalWorkflowConfig] | None = None
    """Mapping from workflow names to their Temporal-specific configurations."""

    # ....................... #

    def __call__(self) -> Deps[K]:
        """Build a dependency container with Temporal-backed ports."""

        plain_deps = Deps[K].plain({TemporalClientDepKey: self.client})
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
                    }
                )
            )

        return plain_deps.merge(workflow_deps)
