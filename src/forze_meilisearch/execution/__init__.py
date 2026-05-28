"""Meilisearch execution wiring (lifecycle, dependency module)."""

from .lifecycle import meilisearch_lifecycle_step, routed_meilisearch_lifecycle_step

__all__ = [
    "meilisearch_lifecycle_step",
    "routed_meilisearch_lifecycle_step",
]
