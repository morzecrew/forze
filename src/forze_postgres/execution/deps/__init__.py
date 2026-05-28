from .configs import (
    PgroongaScoreVersion,
    PostgresAnalyticsConfig,
    PostgresDocumentConfig,
    PostgresFederatedSearchConfig,
    PostgresFederatedSearchLeg,
    PostgresFederatedSearchLegHub,
    PostgresFederatedSearchLegSearch,
    PostgresHubSearchConfig,
    PostgresHubSearchMemberConfig,
    PostgresQueryConfig,
    PostgresReadOnlyDocumentConfig,
    PostgresSearchConfig,
    SearchEngine,
    VectorEngineDistance,
    validate_fts_groups_for_search_spec,
)
from .factories import (
    ConfigurablePostgresAnalytics,
    ConfigurablePostgresDocument,
    ConfigurablePostgresFederatedSearch,
    ConfigurablePostgresHubSearch,
    ConfigurablePostgresReadOnlyDocument,
    ConfigurablePostgresSearch,
    postgres_txmanager,
)
from .keys import PostgresClientDepKey
from .module import PostgresDepsModule

# ----------------------- #

__all__ = [
    "PostgresDepsModule",
    "PostgresClientDepKey",
    "PgroongaScoreVersion",
    "PostgresAnalyticsConfig",
    "PostgresQueryConfig",
    "ConfigurablePostgresAnalytics",
    "ConfigurablePostgresDocument",
    "ConfigurablePostgresFederatedSearch",
    "ConfigurablePostgresHubSearch",
    "ConfigurablePostgresReadOnlyDocument",
    "ConfigurablePostgresSearch",
    "PostgresDocumentConfig",
    "PostgresSearchConfig",
    "PostgresFederatedSearchConfig",
    "PostgresFederatedSearchLeg",
    "PostgresFederatedSearchLegHub",
    "PostgresFederatedSearchLegSearch",
    "PostgresHubSearchConfig",
    "PostgresHubSearchMemberConfig",
    "PostgresReadOnlyDocumentConfig",
    "SearchEngine",
    "VectorEngineDistance",
    "validate_fts_groups_for_search_spec",
    "postgres_txmanager",
]
