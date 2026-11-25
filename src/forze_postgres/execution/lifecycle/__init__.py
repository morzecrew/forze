"""Postgres lifecycle steps (pool, catalog warmup, document and owned schema validation,
counter purge)."""

from ..deps.document_binding import postgres_document_schema_spec_for_binding
from .capabilities import POSTGRES_CLIENT_CAPABILITY
from .catalog_warmup import (
    PostgresCatalogWarmupHook,
    postgres_catalog_warmup_lifecycle_step,
    warm_postgres_catalog,
)
from .counter_purge import postgres_counter_purge_lifecycle_step
from .document_schema import (
    PostgresDocumentSchemaValidationHook,
    postgres_document_schema_specs,
    postgres_document_schema_validation_lifecycle_step,
)
from .module import PostgresLifecycleModule
from .owned_schema import (
    PostgresOwnedSchemaValidationHook,
    postgres_owned_schema_validation_lifecycle_step,
)
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
    "PostgresOwnedSchemaValidationHook",
    "PostgresShutdownHook",
    "PostgresStartupHook",
    "postgres_catalog_warmup_lifecycle_step",
    "postgres_counter_purge_lifecycle_step",
    "postgres_document_schema_spec_for_binding",
    "postgres_document_schema_specs",
    "postgres_document_schema_validation_lifecycle_step",
    "postgres_lifecycle_step",
    "postgres_owned_schema_validation_lifecycle_step",
    "routed_postgres_lifecycle_step",
    "warm_postgres_catalog",
]
