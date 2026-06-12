"""DuckDB dependency module for the application kernel."""

from typing import final

import attrs

from forze.application.contracts.analytics import AnalyticsQueryDepKey
from forze.application.execution import Deps, DepsModule
from forze.application.execution.deps.builders import merge_deps, routed_from_mapping
from forze.base.primitives import MappingConverter, StrKeyMapping

from ...kernel.client import DuckDbClientPort
from .configs import DuckDbAnalyticsConfig
from .factories import ConfigurableDuckDbAnalytics
from .keys import DuckDbClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class DuckDbDepsModule(DepsModule):
    """Dependency module that registers the DuckDB client and analytics query adapters.

    DuckDB is query-only, so only :data:`AnalyticsQueryDepKey` is bound (no ingest).
    """

    client: DuckDbClientPort
    """Pre-constructed DuckDB client (initialized via :func:`duckdb_lifecycle_step`)."""

    analytics: StrKeyMapping[DuckDbAnalyticsConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from analytics route names to DuckDB configuration."""

    # ....................... #

    def __call__(self) -> Deps:
        return merge_deps(
            routed_from_mapping(
                self.analytics,
                bindings=[
                    (AnalyticsQueryDepKey, ConfigurableDuckDbAnalytics),
                ],
            ),
            plain={DuckDbClientDepKey: self.client},
        )
