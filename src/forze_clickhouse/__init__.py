"""ClickHouse integration for Forze analytics contracts."""

from ._compat import require_clickhouse

require_clickhouse()

# ....................... #

from .execution import (
    ClickHouseAnalyticsConfig,
    ClickHouseClientDepKey,
    ClickHouseDepsModule,
    ClickHouseQueryConfig,
    clickhouse_lifecycle_step,
    routed_clickhouse_lifecycle_step,
)
from .kernel.client import (
    ClickHouseClient,
    ClickHouseClientPort,
    ClickHouseConfig,
    ClickHouseRoutingCredentials,
    RoutedClickHouseClient,
)
from .kernel.relation import (
    RelationSpec,
    coerce_relation_spec,
    resolve_clickhouse_ingest_target,
)

# ----------------------- #

__all__ = [
    "ClickHouseDepsModule",
    "ClickHouseClient",
    "ClickHouseClientPort",
    "RoutedClickHouseClient",
    "ClickHouseRoutingCredentials",
    "ClickHouseConfig",
    "ClickHouseClientDepKey",
    "clickhouse_lifecycle_step",
    "routed_clickhouse_lifecycle_step",
    "ClickHouseAnalyticsConfig",
    "ClickHouseQueryConfig",
    "RelationSpec",
    "coerce_relation_spec",
    "resolve_clickhouse_ingest_target",
]
