"""Analytics contracts: named warehouse queries and optional append-only ingest."""

from .deps import (
    AnalyticsDeps,
    AnalyticsIngestDepKey,
    AnalyticsIngestDepPort,
    AnalyticsQueryDepKey,
    AnalyticsQueryDepPort,
)
from .ports import (
    AnalyticsIngestPort,
    AnalyticsQueryPort,
    BaseAnalyticsPort,
)
from .specs import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
    validate_analytics_spec,
)
from .types import AnalyticsRunOptions
from .value_objects import AnalyticsAppendResult, IngestSpec

# ----------------------- #

__all__ = [
    "AnalyticsAppendResult",
    "IngestSpec",
    "AnalyticsDeps",
    "AnalyticsIngestDepKey",
    "AnalyticsIngestDepPort",
    "AnalyticsIngestPort",
    "AnalyticsQueryDefinition",
    "AnalyticsQueryDepKey",
    "AnalyticsQueryDepPort",
    "AnalyticsQueryPort",
    "AnalyticsRunOptions",
    "AnalyticsSpec",
    "BaseAnalyticsPort",
    "validate_analytics_spec",
]
