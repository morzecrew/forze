"""Meilisearch federated search dep factory."""

from typing import Any, final

import attrs

from forze.application.contracts.search import (
    FederatedSearchQueryDepPort,
    FederatedSearchSpec,
    HubSearchSpec,
    SearchQueryPort,
)
from forze.application.execution import ExecutionContext
from forze.base.exceptions import exc
from forze_meilisearch.adapters.search._simple_base import (
    MeilisearchSimpleSearchAdapter,
)
from forze_meilisearch.adapters.search.federated import (
    MeilisearchFederatedSearchAdapter,
)
from forze_meilisearch.execution.deps.configs import MeilisearchFederatedSearchConfig
from forze_meilisearch.execution.deps.keys import MeilisearchClientDepKey

from .search import meilisearch_search_adapter, result_snapshot

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableMeilisearchFederatedSearch(FederatedSearchQueryDepPort):
    """Build :class:`MeilisearchFederatedSearchAdapter` from spec + config."""

    config: MeilisearchFederatedSearchConfig = attrs.field(
        validator=attrs.validators.instance_of(MeilisearchFederatedSearchConfig),
    )

    def __call__(
        self,
        context: ExecutionContext,
        spec: FederatedSearchSpec[Any],
    ) -> SearchQueryPort[Any]:
        self.config.validate_against_spec(spec)
        client = context.deps.provide(MeilisearchClientDepKey)

        legs: list[tuple[str, MeilisearchSimpleSearchAdapter[Any]]] = []

        for m in spec.members:
            if isinstance(m, HubSearchSpec):
                raise exc.internal(
                    "Hub members are not supported for Meilisearch federation."
                )

            c = self.config.members.get(m.name)

            if c is None:
                raise exc.internal(
                    f"Member {m.name!r} not found in MeilisearchFederatedSearchConfig.members.",
                )

            leg_cfg = c

            if not leg_cfg.tenant_aware and self.config.tenant_aware:
                leg_cfg = attrs.evolve(leg_cfg, tenant_aware=True)

            port = meilisearch_search_adapter(context, m, leg_cfg)
            legs.append((m.name, port))

        return MeilisearchFederatedSearchAdapter(
            federated_spec=spec,
            legs=tuple(legs),
            client=client,
            merge=self.config.merge,
            rrf_k=self.config.rrf_k,
            rrf_per_leg_limit=self.config.rrf_per_leg_limit,
            result_snapshot=result_snapshot(context, spec.snapshot),
        )
