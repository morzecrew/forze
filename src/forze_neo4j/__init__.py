"""forze_neo4j — Neo4j / openCypher graph integration.

Implements the Forze graph contracts (``forze.application.contracts.graph``) on Neo4j
via the official async Bolt driver. The Cypher generation lives in
``forze_neo4j.kernel.cypher`` and is engine-driver-agnostic, so a future openCypher
sibling (Memgraph, Neptune, AGE) can reuse it.
"""

from ._compat import require_neo4j

require_neo4j()

# ....................... #

from .adapters import Neo4jGraphAdapter
from .execution import (
    ConfigurableNeo4jGraph,
    Neo4jClientDepKey,
    Neo4jDepsModule,
    Neo4jGraphConfig,
    Neo4jShutdownHook,
    Neo4jStartupHook,
    neo4j_lifecycle_step,
)
from .kernel.client import Neo4jClient, Neo4jClientPort, Neo4jConfig
from .kernel.relation import (
    NamedResourceSpec,
    coerce_named_resource_spec,
    is_static_named_resource,
    resolve_neo4j_database,
)

# ----------------------- #

__all__ = [
    "Neo4jClient",
    "Neo4jClientPort",
    "Neo4jConfig",
    "Neo4jGraphAdapter",
    "Neo4jDepsModule",
    "Neo4jClientDepKey",
    "Neo4jGraphConfig",
    "ConfigurableNeo4jGraph",
    "Neo4jStartupHook",
    "Neo4jShutdownHook",
    "neo4j_lifecycle_step",
    "NamedResourceSpec",
    "coerce_named_resource_spec",
    "is_static_named_resource",
    "resolve_neo4j_database",
]
