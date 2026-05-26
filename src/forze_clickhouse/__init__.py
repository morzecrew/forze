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
)
from .kernel.platform import ClickHouseClient, ClickHouseClientPort, ClickHouseConfig

# ----------------------- #

__all__ = [
    "ClickHouseDepsModule",
    "ClickHouseClient",
    "ClickHouseClientPort",
    "ClickHouseConfig",
    "ClickHouseClientDepKey",
    "clickhouse_lifecycle_step",
    "ClickHouseAnalyticsConfig",
    "ClickHouseQueryConfig",
]
