"""PostgreSQL integration for Forze."""

from ._compat import require_psycopg

require_psycopg()

# ....................... #

from ._constants import FORZE_POSTGRES_LOGGER_NAMES
from .execution import (
    PostgresClientDepKey,
    PostgresDepsModule,
    PostgresDocumentConfig,
    PostgresFederatedSearchConfig,
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
    "PostgresFederatedSearchConfig",
    "PostgresHubSearchConfig",
    "PostgresHubSearchMemberConfig",
    "FORZE_POSTGRES_LOGGER_NAMES",
]
