"""Logger names for the forze_neo4j package."""

from enum import StrEnum
from typing import Final, final

# ----------------------- #


@final
class ForzeNeo4jLogger(StrEnum):
    """Forze Neo4j logger names."""

    ADAPTERS = "forze_neo4j.adapters"
    EXECUTION = "forze_neo4j.execution"
    KERNEL = "forze_neo4j.kernel"


# ....................... #

FORZE_NEO4J_LOGGER_NAMES: Final = list(map(str, ForzeNeo4jLogger))
