"""Configurable Neo4j graph adapter factory.

One factory builds the single :class:`Neo4jGraphAdapter`, which satisfies the graph
query, command, and raw-query ports; it is registered under all three dep keys.
"""

from typing import final

import attrs

from forze.application.contracts.graph import GraphModuleSpec
from forze.application.execution import ExecutionContext

from ....adapters import Neo4jGraphAdapter
from ..configs import Neo4jGraphConfig
from ..keys import Neo4jClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableNeo4jGraph:
    """Build a :class:`Neo4jGraphAdapter` for a graph module route."""

    config: Neo4jGraphConfig = attrs.field(
        validator=attrs.validators.instance_of(Neo4jGraphConfig),
    )

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: GraphModuleSpec,
    ) -> Neo4jGraphAdapter:
        client = ctx.deps.provide(Neo4jClientDepKey)

        return Neo4jGraphAdapter(
            spec=spec,
            client=client,
            tenant_aware=self.config.tenant_aware,
            tenant_provider=ctx.inv_ctx.get_tenant,
            tenant_property=self.config.tenant_property,
            database=self.config.database,
            traversal_isolation=self.config.traversal_isolation,
        )
