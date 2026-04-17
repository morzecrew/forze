"""Postgres execution wiring for the application kernel."""

from .deps import (
    PostgresClientDepKey,
    PostgresDepsModule,
    PostgresDocumentConfig,
    PostgresHubSearchConfig,
    PostgresHubSearchMemberConfig,
    PostgresReadOnlyDocumentConfig,
    PostgresSearchConfig,
)
from .lifecycle import postgres_lifecycle_step

# ----------------------- #

__all__ = [
    "PostgresDepsModule",
    "PostgresClientDepKey",
    "postgres_lifecycle_step",
    "PostgresDocumentConfig",
    "PostgresReadOnlyDocumentConfig",
    "PostgresSearchConfig",
    "PostgresHubSearchConfig",
    "PostgresHubSearchMemberConfig",
]
