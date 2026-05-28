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
from .search import (
    PgroongaScoreVersion,
    PostgresSearchConfig,
    SearchEngine,
    VectorEngineDistance,
    validate_fts_groups_for_search_spec,
)

# ----------------------- #

__all__ = [
    "PgroongaScoreVersion",
    "PostgresAnalyticsConfig",
    "PostgresDocumentConfig",
    "PostgresFederatedSearchConfig",
    "PostgresFederatedSearchLeg",
    "PostgresFederatedSearchLegHub",
    "PostgresFederatedSearchLegSearch",
    "PostgresHubSearchConfig",
    "PostgresHubSearchMemberConfig",
    "PostgresQueryConfig",
    "PostgresReadOnlyDocumentConfig",
    "PostgresSearchConfig",
    "SearchEngine",
    "VectorEngineDistance",
    "validate_fts_groups_for_search_spec",
]
