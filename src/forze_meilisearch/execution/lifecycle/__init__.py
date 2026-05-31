"""Meilisearch lifecycle steps (client pool startup and shutdown)."""

from .pool import (
    MeilisearchShutdownHook,
    MeilisearchStartupHook,
    meilisearch_lifecycle_step,
    routed_meilisearch_lifecycle_step,
)

# ----------------------- #

__all__ = [
    "MeilisearchShutdownHook",
    "MeilisearchStartupHook",
    "meilisearch_lifecycle_step",
    "routed_meilisearch_lifecycle_step",
]
