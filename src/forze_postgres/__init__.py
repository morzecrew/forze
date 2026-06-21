"""PostgreSQL integration for Forze."""

from ._compat import require_psycopg

require_psycopg()

# ....................... #

from .execution import (
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
    PostgresReadOnlyDocumentConfig,
    PostgresSearchConfig,
    SearchEngineSpec,
    VectorEngine,
    POSTGRES_CLIENT_CAPABILITY,
    PostgresLifecycleModule,
    postgres_catalog_warmup_lifecycle_step,
    postgres_document_schema_spec_for_binding,
    postgres_document_schema_validation_lifecycle_step,
    warm_postgres_catalog,
    postgres_lifecycle_step,
    routed_postgres_lifecycle_step,
    validate_postgres_document_schemas,
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
from .provisioning import PostgresSchemaTenantProvisioner

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
