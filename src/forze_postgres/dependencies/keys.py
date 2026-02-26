from forze.application.kernel.deps import DepKey

from ..kernel.platform import PostgresClient

# ----------------------- #

PostgresClientDepKey: DepKey[PostgresClient] = DepKey("postgres_client")
"""Key used to register the :class:`PostgresClient` implementation."""
