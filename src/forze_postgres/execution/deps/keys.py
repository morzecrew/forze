"""Dependency keys for Postgres-related services."""

from forze.application.contracts.deps import DepKey

from ...kernel.introspect import PostgresTypesProvider
from ...kernel.platform import PostgresClient

# ----------------------- #

PostgresClientDepKey: DepKey[PostgresClient] = DepKey("postgres_client")
"""Key used to register the :class:`PostgresClient` in the deps container."""

PostgresTypesProviderDepKey: DepKey[PostgresTypesProvider] = DepKey(
    "postgres_types_provider"
)
"""Key used to register the :class:`PostgresTypesProvider` in the deps container."""
