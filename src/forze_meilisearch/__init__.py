"""Meilisearch integration for Forze search query and command ports."""

from forze_meilisearch._compat import require_meilisearch

require_meilisearch()

# ....................... #

from .execution import (
    MeilisearchClientDepKey,
    MeilisearchDepsModule,
    MeilisearchFederatedSearchConfig,
    MeilisearchSearchConfig,
    meilisearch_lifecycle_step,
    routed_meilisearch_lifecycle_step,
)
from .execution.deps import (
    ConfigurableMeilisearchFederatedSearch,
    ConfigurableMeilisearchSearch,
    ConfigurableMeilisearchSearchCommand,
)
from .kernel.client import (
    MeilisearchClient,
    MeilisearchClientPort,
    MeilisearchConfig,
    MeilisearchRoutingCredentials,
    RoutedMeilisearchClient,
)
from .kernel.relation import (
    NamedResourceSpec,
    coerce_named_resource_spec,
    is_static_named_resource,
    resolve_meilisearch_index_uid,
)

# ----------------------- #

__all__ = [
    "MeilisearchDepsModule",
    "MeilisearchClient",
    "MeilisearchClientPort",
    "RoutedMeilisearchClient",
    "MeilisearchRoutingCredentials",
    "MeilisearchConfig",
    "MeilisearchClientDepKey",
    "meilisearch_lifecycle_step",
    "routed_meilisearch_lifecycle_step",
    "MeilisearchSearchConfig",
    "MeilisearchFederatedSearchConfig",
    "ConfigurableMeilisearchSearch",
    "ConfigurableMeilisearchSearchCommand",
    "ConfigurableMeilisearchFederatedSearch",
    "NamedResourceSpec",
    "coerce_named_resource_spec",
    "is_static_named_resource",
    "resolve_meilisearch_index_uid",
]
