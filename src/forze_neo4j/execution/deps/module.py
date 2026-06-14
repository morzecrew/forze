"""Neo4j dependency module for the application kernel."""

from typing import final

import attrs

from forze.application.contracts.graph import (
    GraphCommandDepKey,
    GraphQueryDepKey,
    GraphRawQueryDepKey,
)
from forze.application.contracts.tenancy import (
    TenancyRouteGroup,
    TenantIsolationMode,
    validate_module_tenancy,
)
from forze.application.execution import Deps, DepsModule
from forze.application.execution.deps.builders import (
    merge_deps,
    routed_shared_factories,
)
from forze.base.primitives import MappingConverter, StrKeyMapping

from ...kernel.client import Neo4jClientPort, RoutedNeo4jClient
from .configs import Neo4jGraphConfig
from .factories import ConfigurableNeo4jGraph
from .keys import Neo4jClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class Neo4jDepsModule(DepsModule):
    """Register the Neo4j client and graph-module ports.

    The client must be initialized separately (e.g. via :func:`neo4j_lifecycle_step`)
    before operations run.
    """

    client: Neo4jClientPort
    """Pre-constructed Neo4j client (driver not opened until lifecycle startup)."""

    graphs: StrKeyMapping[Neo4jGraphConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from graph module names to their Neo4j configuration."""

    required_tenant_isolation: TenantIsolationMode | None = attrs.field(default=None)
    """Declared minimum tenant isolation (``None`` = no floor).

    Neo4j spans ``tagged`` (property-partition via ``tenant_property``), ``namespace`` (a
    per-tenant ``database`` resolver — a per-tenant database on a shared cluster, Neo4j 4+
    multi-database), and ``dedicated`` (a :class:`RoutedNeo4jClient` — per-tenant
    instance / credentials).
    """

    # ....................... #

    def __attrs_post_init__(self) -> None:
        validate_module_tenancy(
            integration="Neo4j",
            client_is_routed=isinstance(self.client, RoutedNeo4jClient),
            groups=[
                TenancyRouteGroup(
                    kind="graph",
                    configs=self.graphs,
                    tenant_aware=lambda cfg: cfg.tenant_aware,
                    namespace_resolver=lambda cfg: cfg.database,
                )
            ],
            required_isolation=self.required_tenant_isolation,
            # Routed client → dedicated; a dynamic per-tenant ``database`` → namespace.
            max_supported_isolation="dedicated",
            validation_failed_code="neo4j_tenancy_validation_failed",
        )

    # ....................... #

    def __call__(self) -> Deps:
        """Build a dependency container with Neo4j-backed graph ports."""

        return merge_deps(
            routed_shared_factories(
                self.graphs,
                dep_keys=[
                    GraphQueryDepKey,
                    GraphCommandDepKey,
                    GraphRawQueryDepKey,
                ],
                factory=ConfigurableNeo4jGraph,
            ),
            plain={Neo4jClientDepKey: self.client},
        )
