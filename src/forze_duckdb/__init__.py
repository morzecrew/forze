"""DuckDB integration for Forze analytics and dynamic-read contracts.

Query-only: an in-process DuckDB engine implementing :class:`AnalyticsQueryPort`
over object storage / local files (``read_parquet`` and friends). The lake source
binds in :class:`DuckDbAnalyticsConfig` / the lifecycle step, below the handler
boundary; handlers only ever name a ``query_key`` and an output type.

:class:`RoutedDuckDbClient` gives each tenant its own locked-down instance over its own
Iceberg warehouse or DuckLake, which is what lets ``DynamicReadPort`` run trusted,
runtime-compiled statements here (RFC 0078).
"""

from ._compat import require_duckdb

require_duckdb()

# ....................... #

from .execution import (
    DuckDbAnalyticsConfig,
    DuckDbClientDepKey,
    DuckDbDepsModule,
    DuckDbDynamicReadConfig,
    DuckDbQueryConfig,
    duckdb_lifecycle_step,
)
from .kernel.client import (
    DuckDbClient,
    DuckDbClientPort,
    DuckDbConfig,
    DuckDbDynamicReadClientPort,
    DuckDbQueryResult,
    DuckLakeCatalogCredentials,
    DuckLakeTenantCatalog,
    LakeCatalogCredentials,
    LakeTenantCatalog,
    RoutedDuckDbClient,
)
from .kernel.credentials import (
    GcsCredentials,
    GcsSecretPayload,
    ObjectStoreCredentials,
    S3Credentials,
    S3SecretPayload,
)
from .kernel.sources import (
    CsvSource,
    DeltaSource,
    DuckDbSource,
    IcebergSource,
    JsonSource,
    ParquetSource,
)
from .settings import DuckDbSettings

# ----------------------- #

__all__ = [
    "DuckDbDepsModule",
    "DuckDbClient",
    "DuckDbClientPort",
    "DuckDbDynamicReadClientPort",
    "DuckDbConfig",
    "DuckDbSettings",
    "DuckDbQueryResult",
    "RoutedDuckDbClient",
    "LakeTenantCatalog",
    "LakeCatalogCredentials",
    "DuckLakeTenantCatalog",
    "DuckLakeCatalogCredentials",
    "DuckDbClientDepKey",
    "duckdb_lifecycle_step",
    "DuckDbAnalyticsConfig",
    "DuckDbQueryConfig",
    "DuckDbDynamicReadConfig",
    "DuckDbSource",
    "ParquetSource",
    "CsvSource",
    "JsonSource",
    "IcebergSource",
    "DeltaSource",
    "ObjectStoreCredentials",
    "S3Credentials",
    "S3SecretPayload",
    "GcsCredentials",
    "GcsSecretPayload",
]
