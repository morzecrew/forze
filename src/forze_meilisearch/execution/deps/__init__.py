"""Meilisearch dependency keys, module, and factory functions."""

from .configs import (
    MeilisearchFederatedSearchConfig,
    MeilisearchFederation,
    MeilisearchSearchConfig,
)
from .factories import (
    ConfigurableMeilisearchFederatedSearch,
    ConfigurableMeilisearchSearch,
    ConfigurableMeilisearchSearchCommand,
    ConfigurableMeilisearchSearchManagement,
)
from .keys import MeilisearchClientDepKey
from .module import MeilisearchDepsModule

# ----------------------- #

__all__ = [
    "MeilisearchDepsModule",
    "MeilisearchClientDepKey",
    "MeilisearchSearchConfig",
    "MeilisearchFederatedSearchConfig",
    "MeilisearchFederation",
    "ConfigurableMeilisearchSearch",
    "ConfigurableMeilisearchSearchCommand",
    "ConfigurableMeilisearchSearchManagement",
    "ConfigurableMeilisearchFederatedSearch",
]
