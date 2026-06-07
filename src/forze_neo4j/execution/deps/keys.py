"""Dependency keys for Neo4j services."""

from forze.application.contracts.deps import DepKey

from ...kernel.client import Neo4jClientPort

# ----------------------- #

Neo4jClientDepKey: DepKey[Neo4jClientPort] = DepKey("neo4j_client")
"""Key used to register a Neo4j client in the deps container."""
