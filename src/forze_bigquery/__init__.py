"""Google BigQuery integration for Forze analytics contracts."""

from ._compat import require_bigquery

require_bigquery()

# ....................... #

from .execution import (
    BigQueryAnalyticsConfig,
    BigQueryClientDepKey,
    BigQueryDepsModule,
    BigQueryQueryConfig,
    bigquery_lifecycle_step,
)
from .kernel.platform import BigQueryClient, BigQueryClientPort, BigQueryConfig

# ----------------------- #

__all__ = [
    "BigQueryDepsModule",
    "BigQueryClient",
    "BigQueryClientPort",
    "BigQueryConfig",
    "BigQueryClientDepKey",
    "bigquery_lifecycle_step",
    "BigQueryAnalyticsConfig",
    "BigQueryQueryConfig",
]
