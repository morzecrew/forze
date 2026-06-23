"""Postgres execution configs (frozen attrs)."""

from .analytics import PostgresAnalyticsConfig, PostgresQueryConfig
from .document import PostgresDocumentConfig, PostgresReadOnlyDocumentConfig
from .federated import (
    PostgresFederatedSearchConfig,
    PostgresFederatedSearchLeg,
    PostgresFederatedSearchLegHub,
    PostgresFederatedSearchLegSearch,
)
from .hub import PostgresHubSearchConfig, PostgresHubSearchMemberConfig
from .inbox import PostgresInboxConfig
from .outbox import PostgresOutboxConfig
from .procedures import PostgresProcedureConfig
from .search import (
    FtsEngine,
    PgroongaAuto,
    PgroongaEngine,
    PgroongaPlan,
    PgroongaScoreVersion,
    PostgresSearchConfig,
    SearchEngine,
    SearchEngineSpec,
    VectorEngine,
    VectorEngineDistance,
    validate_fts_groups_for_search_spec,
)

# ----------------------- #

__all__ = [
    "FtsEngine",
    "PgroongaAuto",
    "PgroongaEngine",
    "PgroongaPlan",
    "PgroongaScoreVersion",
    "PostgresAnalyticsConfig",
    "PostgresDocumentConfig",
    "PostgresFederatedSearchConfig",
    "PostgresFederatedSearchLeg",
    "PostgresFederatedSearchLegHub",
    "PostgresFederatedSearchLegSearch",
    "PostgresHubSearchConfig",
    "PostgresHubSearchMemberConfig",
    "PostgresInboxConfig",
    "PostgresOutboxConfig",
    "PostgresProcedureConfig",
    "PostgresQueryConfig",
    "PostgresReadOnlyDocumentConfig",
    "PostgresSearchConfig",
    "SearchEngine",
    "SearchEngineSpec",
    "VectorEngine",
    "VectorEngineDistance",
    "validate_fts_groups_for_search_spec",
]
