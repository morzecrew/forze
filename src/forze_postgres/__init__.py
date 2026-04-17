"""PostgreSQL integration for Forze."""

from ._compat import require_psycopg

require_psycopg()

# ....................... #

from .execution import (
    PostgresClientDepKey,
    PostgresDepsModule,
    PostgresDocumentConfig,
    PostgresHubSearchConfig,
    PostgresHubSearchMemberConfig,
    PostgresReadOnlyDocumentConfig,
    PostgresSearchConfig,
    postgres_lifecycle_step,
)
from .kernel.platform import PostgresClient, PostgresConfig

# ----------------------- #

__all__ = [
    "PostgresDepsModule",
    "PostgresClient",
    "PostgresConfig",
    "PostgresClientDepKey",
    "postgres_lifecycle_step",
    "PostgresDocumentConfig",
    "PostgresReadOnlyDocumentConfig",
    "PostgresSearchConfig",
    "PostgresHubSearchConfig",
    "PostgresHubSearchMemberConfig",
]
