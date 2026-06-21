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
    "ConfigurableMeilisearchFederatedSearch",
]
