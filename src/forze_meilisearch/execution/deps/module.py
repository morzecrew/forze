"""Meilisearch dependency module for the application kernel."""

from enum import StrEnum
from typing import Mapping, final

import attrs

from forze.application.contracts.search import (
    FederatedSearchQueryDepKey,
    SearchCommandDepKey,
    SearchQueryDepKey,
)
from forze.application.execution import Deps, DepsModule
from forze.base.exceptions import exc
from forze_meilisearch.execution.deps.configs import (
    MeilisearchFederatedSearchConfig,
    MeilisearchSearchConfig,
)
from forze_meilisearch.execution.deps.deps import (
    ConfigurableMeilisearchFederatedSearch,
    ConfigurableMeilisearchSearch,
    ConfigurableMeilisearchSearchCommand,
)
from forze_meilisearch.execution.deps.keys import MeilisearchClientDepKey
from forze_meilisearch.kernel.platform.port import MeilisearchClientPort

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class MeilisearchDepsModule[K: str | StrEnum](DepsModule[K]):
    """Registers Meilisearch client and search ports."""

    client: MeilisearchClientPort
    searches: Mapping[K, MeilisearchSearchConfig] | None = attrs.field(default=None)
    federated_searches: Mapping[K, MeilisearchFederatedSearchConfig] | None = attrs.field(
        default=None,
    )

    def __attrs_post_init__(self) -> None:
        if self.searches:
            for search_cfg in self.searches.values():
                if not search_cfg.get("index_uid"):
                    raise exc.configuration(
                        "Meilisearch search config requires index_uid.",
                    )

        if self.federated_searches:
            for fed_cfg in self.federated_searches.values():
                if len(fed_cfg["members"]) < 2:
                    raise exc.configuration(
                        "Federated Meilisearch search requires at least two members.",
                    )

    def __call__(self) -> Deps[K]:
        plain = Deps[K].plain({MeilisearchClientDepKey: self.client})
        search_deps = Deps[K]()
        fed_deps = Deps[K]()

        if self.searches:
            search_deps = search_deps.merge(
                Deps[K].routed(
                    {
                        SearchQueryDepKey: {
                            name: ConfigurableMeilisearchSearch(config=config)
                            for name, config in self.searches.items()
                        },
                        SearchCommandDepKey: {
                            name: ConfigurableMeilisearchSearchCommand(config=config)
                            for name, config in self.searches.items()
                        },
                    }
                )
            )

        if self.federated_searches:
            fed_deps = fed_deps.merge(
                Deps[K].routed(
                    {
                        FederatedSearchQueryDepKey: {
                            name: ConfigurableMeilisearchFederatedSearch(config=config)
                            for name, config in self.federated_searches.items()
                        }
                    }
                )
            )

        return plain.merge(search_deps, fed_deps)
