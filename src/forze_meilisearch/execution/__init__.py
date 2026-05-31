"""Meilisearch execution wiring (lifecycle, dependency module)."""

from .deps import (
    MeilisearchClientDepKey,
    MeilisearchDepsModule,
    MeilisearchFederatedSearchConfig,
    MeilisearchSearchConfig,
)
from .lifecycle import meilisearch_lifecycle_step, routed_meilisearch_lifecycle_step

# ----------------------- #

__all__ = [
    "MeilisearchDepsModule",
    "MeilisearchClientDepKey",
    "MeilisearchSearchConfig",
    "MeilisearchFederatedSearchConfig",
    "meilisearch_lifecycle_step",
    "routed_meilisearch_lifecycle_step",
]
