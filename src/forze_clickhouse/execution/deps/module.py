"""ClickHouse dependency module for the application kernel."""

from typing import Mapping, final

import attrs

from forze.application.contracts.analytics import (
    AnalyticsIngestDepKey,
    AnalyticsQueryDepKey,
)
from forze.application.execution import Deps, DepsModule
from forze.application.execution.deps.builders import merge_deps, routed_from_mapping
from forze.base.primitives import StrKey

from ...kernel.client import ClickHouseClientPort
from .configs import ClickHouseAnalyticsConfig
from .factories import ConfigurableClickHouseAnalytics
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
        return merge_deps(
            routed_from_mapping(
                self.analytics,
                bindings=[
                    (AnalyticsQueryDepKey, ConfigurableClickHouseAnalytics),
                    (AnalyticsIngestDepKey, ConfigurableClickHouseAnalytics),
                ],
            ),
            plain={ClickHouseClientDepKey: self.client},
        )
