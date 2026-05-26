"""BigQuery dependency module for the application kernel."""

from enum import StrEnum
from typing import Mapping, final

import attrs

from forze.application.contracts.analytics import (
    AnalyticsIngestDepKey,
    AnalyticsQueryDepKey,
)
from forze.application.execution import Deps, DepsModule

from ...kernel.platform import BigQueryClientPort
from .configs import BigQueryAnalyticsConfig
from .deps import ConfigurableBigQueryAnalytics
from .keys import BigQueryClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class BigQueryDepsModule[K: str | StrEnum](DepsModule[K]):
    """Dependency module that registers BigQuery client and analytics adapters."""

    client: BigQueryClientPort
    """Pre-constructed BigQuery client (initialized via :func:`bigquery_lifecycle_step`)."""

    analytics: Mapping[K, BigQueryAnalyticsConfig] | None = attrs.field(default=None)
    """Mapping from analytics route names to BigQuery configuration."""

    # ....................... #

    def __call__(self) -> Deps[K]:
        plain_deps = Deps[K].plain({BigQueryClientDepKey: self.client})
        analytics_deps = Deps[K]()

        if self.analytics:
            factory = ConfigurableBigQueryAnalytics
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
