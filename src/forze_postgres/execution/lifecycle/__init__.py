"""Postgres lifecycle steps (pool, catalog warmup, document schema validation)."""

from .catalog_warmup import (
    PostgresCatalogWarmupHook,
    postgres_catalog_warmup_lifecycle_step,
    warm_postgres_catalog,
)
from .document_schema import (
    PostgresDocumentSchemaValidationHook,
    postgres_document_schema_spec_for_binding,
    postgres_document_schema_validation_lifecycle_step,
)
from .capabilities import POSTGRES_CLIENT_CAPABILITY
from .module import PostgresLifecycleModule
from .pool import (
    PostgresShutdownHook,
    PostgresStartupHook,
    postgres_lifecycle_step,
    routed_postgres_lifecycle_step,
)

# ----------------------- #

__all__ = [
    "POSTGRES_CLIENT_CAPABILITY",
    "PostgresLifecycleModule",
    "PostgresCatalogWarmupHook",
    "PostgresDocumentSchemaValidationHook",
    "PostgresShutdownHook",
    "PostgresStartupHook",
    "postgres_catalog_warmup_lifecycle_step",
    "postgres_document_schema_spec_for_binding",
    "postgres_document_schema_validation_lifecycle_step",
    "postgres_lifecycle_step",
    "routed_postgres_lifecycle_step",
    "warm_postgres_catalog",
]
