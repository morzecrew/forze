"""Neo4j dependency module for the application kernel."""

from typing import Mapping, final

import attrs

from forze.application.contracts.graph import (
    GraphCommandDepKey,
    GraphQueryDepKey,
    GraphRawQueryDepKey,
)
from forze.application.execution import Deps, DepsModule
from forze.application.execution.deps.builders import (
    merge_deps,
    routed_shared_factories,
)
from forze.base.primitives import StrKey

from ...kernel.client import Neo4jClientPort
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

    graphs: Mapping[StrKey, Neo4jGraphConfig] | None = attrs.field(default=None)
    """Mapping from graph module names to their Neo4j configuration."""

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
