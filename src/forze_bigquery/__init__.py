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
    routed_bigquery_lifecycle_step,
)
from .kernel.client import (
    BigQueryClient,
    BigQueryClientPort,
    BigQueryConfig,
    BigQueryRoutingCredentials,
    RoutedBigQueryClient,
)
from .kernel.relation import (
    RelationSpec,
    coerce_relation_spec,
    resolve_bigquery_ingest_target,
)

# ----------------------- #

__all__ = [
    "BigQueryDepsModule",
    "BigQueryClient",
    "BigQueryClientPort",
    "RoutedBigQueryClient",
    "BigQueryRoutingCredentials",
    "BigQueryConfig",
    "BigQueryClientDepKey",
    "bigquery_lifecycle_step",
    "routed_bigquery_lifecycle_step",
    "BigQueryAnalyticsConfig",
    "BigQueryQueryConfig",
    "RelationSpec",
    "coerce_relation_spec",
    "resolve_bigquery_ingest_target",
]
