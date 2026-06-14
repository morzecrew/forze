from typing import Any, Sequence, final

import attrs

from forze.application.contracts.deps import DepKey
from forze.application.contracts.durable.workflow import (
    DurableWorkflowCommandDepKey,
    DurableWorkflowQueryDepKey,
    DurableWorkflowScheduleBootstrap,
    DurableWorkflowScheduleCommandDepKey,
    DurableWorkflowScheduleQueryDepKey,
)
from forze.application.contracts.tenancy import (
    TenancyRouteGroup,
    TenantIsolationMode,
    validate_module_tenancy,
    warn_dynamic_relation_with_tenant_aware,
)
from forze.application.execution import Deps, DepsModule
from forze.base.primitives import MappingConverter, StrKeyMapping

from ...kernel._logger import logger
from ...kernel.client import RoutedTemporalClient, TemporalClientPort
from .configs import TemporalWorkflowConfig
from .factories import (
    ConfigurableTemporalWorkflowCommand,
    ConfigurableTemporalWorkflowQuery,
    ConfigurableTemporalWorkflowScheduleCommand,
    ConfigurableTemporalWorkflowScheduleQuery,
)
from .keys import TemporalClientDepKey, TemporalScheduleBootstrapDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class TemporalDepsModule(DepsModule):
    """Dependency module that registers Temporal clients and adapters."""

    client: TemporalClientPort
    """Pre-constructed Temporal client (single cluster or routed, not connected until lifecycle)."""

    workflows: StrKeyMapping[TemporalWorkflowConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from workflow names to their Temporal-specific configurations."""

    schedule_bootstraps: Sequence[DurableWorkflowScheduleBootstrap[Any]] | None = (
        attrs.field(
            default=None,
        )
    )
    """Declarative schedules upserted on Temporal lifecycle startup."""

    required_tenant_isolation: TenantIsolationMode | None = attrs.field(default=None)
    """Declared minimum tenant isolation (``None`` = no floor).

    Workflows span: ``row`` (``tenant_aware``), ``schema`` (a per-tenant ``queue``
    resolver — a per-tenant task queue), ``database`` (a routed per-tenant client).
    """

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.workflows:
            for name, cfg in self.workflows.items():
                warn_dynamic_relation_with_tenant_aware(
                    integration="Temporal",
                    route_name=str(name),
                    kind="workflow",
                    tenant_aware=cfg.tenant_aware,
                    named_fields=[("queue", cfg.queue)],
                    log_warning=logger.warning,
                )

        validate_module_tenancy(
            integration="Temporal",
            client_is_routed=isinstance(self.client, RoutedTemporalClient),
            groups=[
                TenancyRouteGroup(
                    kind="workflow",
                    configs=self.workflows,
                    tenant_aware=lambda cfg: cfg.tenant_aware,
                    namespace_resolver=lambda cfg: cfg.queue,
                )
            ],
            required_isolation=self.required_tenant_isolation,
            validation_failed_code="temporal_tenancy_validation_failed",
            max_supported_isolation="database",
        )

    # ....................... #

    def __call__(self) -> Deps:
        """Build a dependency container with Temporal-backed ports."""

        plain: dict[DepKey[Any], Any] = {TemporalClientDepKey: self.client}

        if self.schedule_bootstraps:
            plain[TemporalScheduleBootstrapDepKey] = self.schedule_bootstraps

        plain_deps = Deps.plain(plain)
        workflow_deps = Deps()

        if self.workflows:
            workflow_deps = workflow_deps.merge(
                Deps.routed(
                    {
                        DurableWorkflowQueryDepKey: {
                            name: ConfigurableTemporalWorkflowQuery(config=config)
                            for name, config in self.workflows.items()
                        },
                        DurableWorkflowCommandDepKey: {
                            name: ConfigurableTemporalWorkflowCommand(config=config)
                            for name, config in self.workflows.items()
                        },
                        DurableWorkflowScheduleQueryDepKey: {
                            name: ConfigurableTemporalWorkflowScheduleQuery(
                                config=config
                            )
                            for name, config in self.workflows.items()
                        },
                        DurableWorkflowScheduleCommandDepKey: {
                            name: ConfigurableTemporalWorkflowScheduleCommand(
                                config=config,
                            )
                            for name, config in self.workflows.items()
                        },
                    }
                )
            )

        return plain_deps.merge(workflow_deps)
