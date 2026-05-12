"""PostgreSQL integration for Forze."""

from ._compat import require_psycopg

require_psycopg()

# ....................... #

from .execution import (
    PgroongaScoreVersion,
    PostgresClientDepKey,
    PostgresDepsModule,
    PostgresDocumentConfig,
    PostgresDocumentSchemaSpec,
    PostgresFederatedSearchConfig,
    PostgresHubSearchConfig,
    PostgresHubSearchMemberConfig,
    PostgresReadOnlyDocumentConfig,
    PostgresSearchConfig,
    postgres_catalog_warmup_lifecycle_step,
    postgres_document_schema_spec_for_binding,
    postgres_document_schema_validation_lifecycle_step,
    postgres_lifecycle_step,
    routed_postgres_lifecycle_step,
    validate_postgres_document_schemas,
    warm_postgres_catalog,
)
from .kernel.platform import (
    PostgresClient,
    PostgresClientPort,
    PostgresConfig,
    RoutedPostgresClient,
)

# ----------------------- #

__all__ = [
    "PostgresDepsModule",
    "PostgresClient",
    "PostgresClientPort",
    "PostgresConfig",
    "RoutedPostgresClient",
    "PostgresClientDepKey",
    "postgres_lifecycle_step",
    "routed_postgres_lifecycle_step",
    "postgres_catalog_warmup_lifecycle_step",
    "warm_postgres_catalog",
    "postgres_document_schema_spec_for_binding",
    "postgres_document_schema_validation_lifecycle_step",
    "PostgresDocumentSchemaSpec",
    "validate_postgres_document_schemas",
    "PgroongaScoreVersion",
    "PostgresDocumentConfig",
    "PostgresReadOnlyDocumentConfig",
    "PostgresSearchConfig",
    "PostgresFederatedSearchConfig",
    "PostgresHubSearchConfig",
    "PostgresHubSearchMemberConfig",
]
