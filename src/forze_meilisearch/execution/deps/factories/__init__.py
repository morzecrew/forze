"""Meilisearch dependency factories."""

from .federated import ConfigurableMeilisearchFederatedSearch
from .search import (
    ConfigurableMeilisearchSearch,
    ConfigurableMeilisearchSearchCommand,
)

# ----------------------- #

__all__ = [
    "ConfigurableMeilisearchFederatedSearch",
    "ConfigurableMeilisearchSearch",
    "ConfigurableMeilisearchSearchCommand",
]
