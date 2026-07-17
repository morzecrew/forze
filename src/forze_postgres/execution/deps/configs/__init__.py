"""Postgres execution configs (frozen attrs)."""

from .analytics import PostgresAnalyticsConfig, PostgresQueryConfig
from .counter import PostgresCounterConfig
from .document import PostgresDocumentConfig, PostgresReadOnlyDocumentConfig
from .durable import (
    PostgresDurableRunConfig,
    PostgresDurableScheduleConfig,
    PostgresDurableStepConfig,
)
from .federated import (
    PostgresFederatedSearchConfig,
    PostgresFederatedSearchLeg,
    PostgresFederatedSearchLegHub,
    PostgresFederatedSearchLegSearch,
)
from .hlc_checkpoint import PostgresHlcCheckpointConfig
from .hub import PostgresHubSearchConfig, PostgresHubSearchMemberConfig
from .idempotency import PostgresIdempotencyConfig
from .inbox import PostgresInboxConfig
from .outbox import PostgresOutboxConfig
from .procedure import PostgresProcedureConfig
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
    "PostgresDurableRunConfig",
    "PostgresDurableScheduleConfig",
    "PostgresDurableStepConfig",
    "PostgresFederatedSearchConfig",
    "PostgresFederatedSearchLeg",
    "PostgresFederatedSearchLegHub",
    "PostgresFederatedSearchLegSearch",
    "PostgresHlcCheckpointConfig",
    "PostgresHubSearchConfig",
    "PostgresHubSearchMemberConfig",
    "PostgresCounterConfig",
    "PostgresIdempotencyConfig",
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
