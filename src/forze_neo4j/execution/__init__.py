"""Neo4j execution wiring for the application kernel."""

from .deps import (
    ConfigurableNeo4jGraph,
    Neo4jClientDepKey,
    Neo4jDepsModule,
    Neo4jGraphConfig,
)
from .lifecycle import (
    Neo4jShutdownHook,
    Neo4jStartupHook,
    neo4j_lifecycle_step,
    routed_neo4j_lifecycle_step,
)

# ----------------------- #

__all__ = [
    "Neo4jDepsModule",
    "Neo4jClientDepKey",
    "Neo4jGraphConfig",
    "ConfigurableNeo4jGraph",
    "Neo4jStartupHook",
    "Neo4jShutdownHook",
    "neo4j_lifecycle_step",
    "routed_neo4j_lifecycle_step",
]
