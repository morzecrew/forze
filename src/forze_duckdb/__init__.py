"""DuckDB integration for Forze analytics contracts.

Query-only: an in-process DuckDB engine implementing :class:`AnalyticsQueryPort`
over object storage / local files (``read_parquet`` and friends). The lake source
binds in :class:`DuckDbAnalyticsConfig` / the lifecycle step, below the handler
boundary; handlers only ever name a ``query_key`` and an output type.
"""

from ._compat import require_duckdb

require_duckdb()

# ....................... #

from .execution import (
    DuckDbAnalyticsConfig,
    DuckDbClientDepKey,
    DuckDbDepsModule,
    DuckDbQueryConfig,
    duckdb_lifecycle_step,
)
from .kernel.client import (
    DuckDbClient,
    DuckDbClientPort,
    DuckDbConfig,
    DuckDbQueryResult,
)

# ----------------------- #

__all__ = [
    "DuckDbDepsModule",
    "DuckDbClient",
    "DuckDbClientPort",
    "DuckDbConfig",
    "DuckDbQueryResult",
    "DuckDbClientDepKey",
    "duckdb_lifecycle_step",
    "DuckDbAnalyticsConfig",
    "DuckDbQueryConfig",
]
