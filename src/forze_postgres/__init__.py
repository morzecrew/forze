"""PostgreSQL integration for Forze."""

from ._compat import require_psycopg

require_psycopg()

# ....................... #

from .adapters import PostgresSchemaTenantProvisioner
from .execution import (
    POSTGRES_CLIENT_CAPABILITY,
    FtsEngine,
    PgroongaAuto,
    PgroongaEngine,
    PgroongaPlan,
    PgroongaScoreVersion,
    PostgresClientDepKey,
    PostgresDepsModule,
    PostgresDocumentConfig,
    PostgresDocumentSchemaSpec,
    PostgresFederatedSearchConfig,
    PostgresHubSearchConfig,
    PostgresHubSearchMemberConfig,
    PostgresLifecycleModule,
    PostgresReadOnlyDocumentConfig,
    PostgresSearchConfig,
    SearchEngineSpec,
    VectorEngine,
    postgres_catalog_warmup_lifecycle_step,
    postgres_document_schema_spec_for_binding,
    postgres_document_schema_validation_lifecycle_step,
    postgres_lifecycle_step,
    routed_postgres_lifecycle_step,
    validate_postgres_document_schemas,
    warm_postgres_catalog,
)
from .kernel.client import (
    PostgresClient,
    PostgresClientPort,
    PostgresConfig,
    RoutedPostgresClient,
)
from .kernel.relation import (
    RelationSpec,
    coerce_relation_spec,
    require_static_relation,
)

# ----------------------- #

__all__ = [
    "PostgresDepsModule",
    "PostgresSchemaTenantProvisioner",
    "PostgresClient",
    "PostgresClientPort",
    "PostgresConfig",
    "RoutedPostgresClient",
    "PostgresClientDepKey",
    "POSTGRES_CLIENT_CAPABILITY",
    "PostgresLifecycleModule",
    "postgres_lifecycle_step",
    "routed_postgres_lifecycle_step",
    "postgres_catalog_warmup_lifecycle_step",
    "postgres_document_schema_spec_for_binding",
    "postgres_document_schema_validation_lifecycle_step",
    "warm_postgres_catalog",
    "PostgresDocumentSchemaSpec",
    "validate_postgres_document_schemas",
    "PgroongaScoreVersion",
    "PostgresDocumentConfig",
    "PostgresReadOnlyDocumentConfig",
    "PostgresSearchConfig",
    "PostgresFederatedSearchConfig",
    "PostgresHubSearchConfig",
    "PostgresHubSearchMemberConfig",
    "FtsEngine",
    "PgroongaAuto",
    "PgroongaEngine",
    "PgroongaPlan",
    "SearchEngineSpec",
    "VectorEngine",
    "RelationSpec",
    "coerce_relation_spec",
    "require_static_relation",
]
