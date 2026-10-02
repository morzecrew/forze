"""Unit tests for Meilisearch dependency factories."""

from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import SearchSpec
from forze.application.execution import Deps
from forze.base.exceptions import CoreException, ExceptionKind
from forze_meilisearch.execution.deps import (
    ConfigurableMeilisearchFederatedSearch,
    ConfigurableMeilisearchSearch,
    MeilisearchSearchConfig,
)
from forze_meilisearch.execution.deps.keys import MeilisearchClientDepKey
from tests.support.execution_context import context_from_deps


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


# ....................... #


class TestAPinnedSortableListMustCoverTheDefaultSort:
    """An unsorted page sorts by ``default_sort``, which the engine refuses unless sortable."""

    class _Row(BaseModel):
        id: str
        title: str
        rank: int = 0

    _SPEC = SearchSpec(
        name="rows", model_type=_Row, fields=["title"], default_sort={"rank": "desc"}
    )

    def _port(self, sortable: list[str] | None) -> object:
        ctx = context_from_deps(Deps.plain({MeilisearchClientDepKey: MagicMock()}))
        config = MeilisearchSearchConfig(index_uid="rows", sortable_attributes=sortable)

        return ConfigurableMeilisearchSearch(config=config)(ctx, self._SPEC)

    def test_a_list_without_it_is_refused_when_the_port_is_built(self) -> None:
        with pytest.raises(CoreException, match="rank") as caught:
            self._port(["title"])

        assert caught.value.kind is ExceptionKind.CONFIGURATION

    @pytest.mark.parametrize("sortable", [None, ["title", "rank"]])
    def test_a_list_with_it_or_no_list_builds(self, sortable: list[str] | None) -> None:
        assert self._port(sortable) is not None
