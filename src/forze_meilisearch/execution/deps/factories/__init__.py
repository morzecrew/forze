"""Meilisearch dependency factories."""

from .federated import ConfigurableMeilisearchFederatedSearch
from .search import (
    ConfigurableMeilisearchSearch,
    ConfigurableMeilisearchSearchCommand,
    ConfigurableMeilisearchSearchManagement,
)

# ----------------------- #

__all__ = [
    "ConfigurableMeilisearchFederatedSearch",
    "ConfigurableMeilisearchSearch",
    "ConfigurableMeilisearchSearchCommand",
    "ConfigurableMeilisearchSearchManagement",
]
