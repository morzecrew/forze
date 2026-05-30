"""ClickHouse dependency module for the application kernel."""

from typing import Mapping, final

import attrs

from forze.application.contracts.analytics import (
    AnalyticsIngestDepKey,
    AnalyticsQueryDepKey,
)
from forze.application.execution import Deps, DepsModule
from forze.base.primitives import StrKey

from ...kernel.platform import ClickHouseClientPort
from .configs import ClickHouseAnalyticsConfig
from .deps import ConfigurableClickHouseAnalytics
from .keys import ClickHouseClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ClickHouseDepsModule(DepsModule):
    """Dependency module that registers ClickHouse client and analytics adapters."""

    client: ClickHouseClientPort
    """Pre-constructed ClickHouse client (initialized via :func:`clickhouse_lifecycle_step`)."""

    analytics: Mapping[StrKey, ClickHouseAnalyticsConfig] | None = attrs.field(
        default=None
    )
    """Mapping from analytics route names to ClickHouse configuration."""

    # ....................... #

    def __call__(self) -> Deps:
        plain_deps = Deps.plain({ClickHouseClientDepKey: self.client})
        analytics_deps = Deps()

        if self.analytics:
            factory = ConfigurableClickHouseAnalytics
            analytics_deps = analytics_deps.merge(
                Deps.routed(
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
