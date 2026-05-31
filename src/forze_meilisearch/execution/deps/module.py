"""Meilisearch dependency module for the application kernel."""

from typing import Mapping, final

import attrs

from forze.application.contracts.search import (
    FederatedSearchQueryDepKey,
    SearchCommandDepKey,
    SearchQueryDepKey,
)
from forze.application.contracts.tenancy import warn_dynamic_relation_with_tenant_aware
from forze.application.execution import Deps, DepsModule
from forze.base.primitives import StrKey
from forze_meilisearch.execution.deps.configs import (
    MeilisearchFederatedSearchConfig,
    MeilisearchSearchConfig,
)
from forze_meilisearch.execution.deps.factories import (
    ConfigurableMeilisearchFederatedSearch,
    ConfigurableMeilisearchSearch,
    ConfigurableMeilisearchSearchCommand,
)
from forze_meilisearch.execution.deps.keys import MeilisearchClientDepKey
from forze_meilisearch.kernel._logger import logger
from forze_meilisearch.kernel.client.port import MeilisearchClientPort

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class MeilisearchDepsModule(DepsModule):
    """Registers Meilisearch client and search ports."""

    client: MeilisearchClientPort
    searches: Mapping[StrKey, MeilisearchSearchConfig] | None = attrs.field(
        default=None
    )
    federated_searches: Mapping[StrKey, MeilisearchFederatedSearchConfig] | None = (
        attrs.field(
            default=None,
        )
    )

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.searches:
            for name, cfg in self.searches.items():
                warn_dynamic_relation_with_tenant_aware(
                    integration="Meilisearch",
                    route_name=str(name),
                    kind="search",
                    tenant_aware=cfg.tenant_aware,
                    named_fields=[("index_uid", cfg.index_uid)],
                    log_warning=logger.warning,
                )

        if self.federated_searches:
            for fed_name, fed_cfg in self.federated_searches.items():
                for member_name, member_cfg in fed_cfg.members.items():
                    warn_dynamic_relation_with_tenant_aware(
                        integration="Meilisearch",
                        route_name=f"{fed_name}.{member_name}",
                        kind="search",
                        tenant_aware=member_cfg.tenant_aware,
                        named_fields=[("index_uid", member_cfg.index_uid)],
                        log_warning=logger.warning,
                    )

    # ....................... #

    def __call__(self) -> Deps:
        plain = Deps.plain({MeilisearchClientDepKey: self.client})
        search_deps = Deps()
        fed_deps = Deps()

        if self.searches:
            search_deps = search_deps.merge(
                Deps.routed(
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
                Deps.routed(
                    {
                        FederatedSearchQueryDepKey: {
                            name: ConfigurableMeilisearchFederatedSearch(config=config)
                            for name, config in self.federated_searches.items()
                        }
                    }
                )
            )

        return plain.merge(search_deps, fed_deps)
