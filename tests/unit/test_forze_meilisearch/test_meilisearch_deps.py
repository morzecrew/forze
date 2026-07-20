"""Unit tests for Meilisearch dependency factories."""

import pytest

from forze_meilisearch.execution.deps import (
    ConfigurableMeilisearchFederatedSearch,
    ConfigurableMeilisearchSearch,
    MeilisearchSearchConfig,
)


def test_rejects_mapping_config_search() -> None:
    with pytest.raises(TypeError, match="MeilisearchSearchConfig"):
        ConfigurableMeilisearchSearch(config={"index_uid": "articles"})


def test_rejects_mapping_config_federated() -> None:
    with pytest.raises(TypeError, match="MeilisearchFederatedSearchConfig"):
        ConfigurableMeilisearchFederatedSearch(
            config={
                "members": {
                    "a": MeilisearchSearchConfig(index_uid="a"),
                    "b": MeilisearchSearchConfig(index_uid="b"),
                },
            },
        )
