"""BigQuery dependency module for the application kernel."""

from typing import Mapping, final

import attrs

from forze.application.contracts.analytics import (
    AnalyticsIngestDepKey,
    AnalyticsQueryDepKey,
)
from forze.application.execution import Deps, DepsModule
from forze.application.execution.deps.builders import merge_deps, routed_from_mapping
from forze.base.primitives import StrKey

from ...kernel.client import BigQueryClientPort
from .configs import BigQueryAnalyticsConfig
from .factories import ConfigurableBigQueryAnalytics
from .keys import BigQueryClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class BigQueryDepsModule(DepsModule):
    """Dependency module that registers BigQuery client and analytics adapters."""

    client: BigQueryClientPort
    """Pre-constructed BigQuery client (initialized via :func:`bigquery_lifecycle_step`)."""

    analytics: Mapping[StrKey, BigQueryAnalyticsConfig] | None = attrs.field(
        default=None
    )
    """Mapping from analytics route names to BigQuery configuration."""

    # ....................... #

    def __call__(self) -> Deps:
        return merge_deps(
            routed_from_mapping(
                self.analytics,
                bindings=[
                    (AnalyticsQueryDepKey, ConfigurableBigQueryAnalytics),
                    (AnalyticsIngestDepKey, ConfigurableBigQueryAnalytics),
                ],
            ),
            plain={BigQueryClientDepKey: self.client},
        )
