"""Dependency keys for Postgres-related services."""

from forze.application.contracts.base import DepKey

from ...kernel.introspect import PostgresIntrospector
from ...kernel.platform import PostgresClientPort

# ----------------------- #

PostgresClientDepKey: DepKey[PostgresClientPort] = DepKey("postgres_client")
"""Key used to register a Postgres client (single-DSN or routed) in the deps container."""

PostgresIntrospectorDepKey: DepKey[PostgresIntrospector] = DepKey(
    "postgres_introspector"
)
"""Key used to register the :class:`PostgresIntrospector` in the deps container."""
