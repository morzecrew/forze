from forze.base.logging import Logger

from forze_neo4j._logging import ForzeNeo4jLogger

# ----------------------- #

logger = Logger(ForzeNeo4jLogger.KERNEL)
"""Neo4j kernel logger."""
