"""Meilisearch dependency module for the application kernel."""

from typing import final

import attrs

from forze.application.contracts.search import (
    FederatedSearchQueryDepKey,
    SearchCommandDepKey,
    SearchQueryDepKey,
)
from forze.application.contracts.tenancy import (
    warn_dynamic_relation_with_tenant_aware,
    warn_integration_routes,
)
from forze.application.execution import Deps, DepsModule
from forze.application.execution.deps.builders import merge_deps, routed_from_mapping
from forze.base.primitives import MappingConverter, StrKeyMapping
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

from ._warnings import MEILISEARCH_SEARCH_WARNING

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class MeilisearchDepsModule(DepsModule):
    """Registers Meilisearch client and search ports."""

    client: MeilisearchClientPort
    """Pre-constructed Meilisearch client."""

    searches: StrKeyMapping[MeilisearchSearchConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Mapping from search names to their Meilisearch-specific configurations."""

    federated_searches: StrKeyMapping[MeilisearchFederatedSearchConfig] | None = (
        attrs.field(
            default=None,
            converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
        )
    )
    """Mapping from federated search names to their Meilisearch-specific configurations."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        warn_integration_routes(
            integration="Meilisearch",
            routes=self.searches,
            warning=MEILISEARCH_SEARCH_WARNING,
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
        return merge_deps(
            routed_from_mapping(
                self.searches,
                bindings=[
                    (SearchQueryDepKey, ConfigurableMeilisearchSearch),
                    (SearchCommandDepKey, ConfigurableMeilisearchSearchCommand),
                ],
            ),
            routed_from_mapping(
                self.federated_searches,
                bindings=[
                    (FederatedSearchQueryDepKey, ConfigurableMeilisearchFederatedSearch)
                ],
            ),
            plain={MeilisearchClientDepKey: self.client},
        )
