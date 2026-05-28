"""Postgres execution wiring for the application kernel."""

from ..kernel.validate_schema import (
    PostgresDocumentSchemaSpec,
    validate_postgres_document_schemas,
)
from .catalog_warmup import postgres_catalog_warmup_lifecycle_step
from .deps import (
    ConfigurablePostgresAnalytics,
    PgroongaScoreVersion,
    PostgresAnalyticsConfig,
    PostgresClientDepKey,
    PostgresDepsModule,
    PostgresDocumentConfig,
    PostgresQueryConfig,
    validate_postgres_analytics_config,
    PostgresFederatedSearchConfig,
    PostgresHubSearchConfig,
    PostgresHubSearchMemberConfig,
    PostgresReadOnlyDocumentConfig,
    PostgresSearchConfig,
)
from .document_schema import postgres_document_schema_validation_lifecycle_step
from .lifecycle import postgres_lifecycle_step, routed_postgres_lifecycle_step

# ----------------------- #

__all__ = [
    "PostgresDepsModule",
    "PostgresClientDepKey",
    "PostgresAnalyticsConfig",
    "PostgresQueryConfig",
    "ConfigurablePostgresAnalytics",
    "validate_postgres_analytics_config",
    "postgres_lifecycle_step",
    "routed_postgres_lifecycle_step",
    "postgres_catalog_warmup_lifecycle_step",
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
