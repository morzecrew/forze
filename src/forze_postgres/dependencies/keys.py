from forze.application.kernel.dependencies import DependencyKey

from ..kernel.platform import PostgresClient

# ----------------------- #

PostgresClientDependencyKey: DependencyKey[PostgresClient] = DependencyKey(
    "postgres_client"
)
"""Key used to register the :class:`PostgresClient` implementation."""
