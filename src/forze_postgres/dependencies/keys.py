from forze.application.kernel.deps import DepKey

from ..kernel.introspect import PostgresTypesProvider
from ..kernel.platform import PostgresClient

# ----------------------- #

PostgresClientDepKey: DepKey[PostgresClient] = DepKey("postgres_client")
"""Key used to register the :class:`PostgresClient` implementation."""

PostgresTypesProviderDepKey: DepKey[PostgresTypesProvider] = DepKey(
    "postgres_types_provider"
)
"""Key used to register the :class:`PostgresTypesProvider` implementation."""
