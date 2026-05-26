"""ClickHouse dependency module for the application kernel."""

from enum import StrEnum
from typing import Mapping, final

import attrs

from forze.application.contracts.analytics import (
    AnalyticsIngestDepKey,
    AnalyticsQueryDepKey,
)
from forze.application.execution import Deps, DepsModule

from ...kernel.platform import ClickHouseClientPort
from .configs import ClickHouseAnalyticsConfig
from .deps import ConfigurableClickHouseAnalytics
from .keys import ClickHouseClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ClickHouseDepsModule[K: str | StrEnum](DepsModule[K]):
    """Dependency module that registers ClickHouse client and analytics adapters."""

    client: ClickHouseClientPort
    """Pre-constructed ClickHouse client (initialized via :func:`clickhouse_lifecycle_step`)."""

    analytics: Mapping[K, ClickHouseAnalyticsConfig] | None = attrs.field(default=None)
    """Mapping from analytics route names to ClickHouse configuration."""

    # ....................... #

    def __call__(self) -> Deps[K]:
        plain_deps = Deps[K].plain({ClickHouseClientDepKey: self.client})
        analytics_deps = Deps[K]()

        if self.analytics:
            factory = ConfigurableClickHouseAnalytics
            analytics_deps = analytics_deps.merge(
                Deps[K].routed(
                    {
                        AnalyticsQueryDepKey: {
                            name: factory(config=config)
                            for name, config in self.analytics.items()
                        },
                        AnalyticsIngestDepKey: {
                            name: factory(config=config)
                            for name, config in self.analytics.items()
                        },
                    }
                )
            )

        return plain_deps.merge(analytics_deps)
