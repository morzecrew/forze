"""Configurable Meilisearch search dependency builders."""

from __future__ import annotations

from typing import Any, final

import attrs
from pydantic import BaseModel

from forze.application.execution import ExecutionContext
from forze.application.contracts.search import (
    FederatedSearchQueryDepPort,
    FederatedSearchSpec,
    HubSearchSpec,
    SearchCommandDepPort,
    SearchCommandPort,
    SearchQueryDepPort,
    SearchQueryPort,
    SearchResultSnapshotDepKey,
    SearchResultSnapshotSpec,
    SearchSpec,
)
from forze.application.coordinators import SearchResultSnapshotCoordinator
from forze.base.exceptions import exc
from forze_meilisearch.adapters.search._command import MeilisearchSearchCommandAdapter
from forze_meilisearch.adapters.search._simple_base import MeilisearchSimpleSearchAdapter
from forze_meilisearch.adapters.search.federated import MeilisearchFederatedSearchAdapter
from forze_meilisearch.execution.deps.configs import (
    MeilisearchFederatedSearchConfig,
    MeilisearchSearchConfig,
)
from forze_meilisearch.execution.deps.keys import MeilisearchClientDepKey

# ----------------------- #


def _resolve_result_snapshot(
    context: ExecutionContext,
    spec: SearchResultSnapshotSpec | None,
) -> Any:
    if spec is None:
        return None

    if not (
        context.deps.exists(SearchResultSnapshotDepKey, route=spec.name)
        or context.deps.exists(SearchResultSnapshotDepKey)
    ):
        return None

    return context.deps.provide(SearchResultSnapshotDepKey, route=spec.name)(
        context,
        spec,
    )


def _snapshot_coord(
    context: ExecutionContext,
    spec: SearchResultSnapshotSpec | None,
) -> SearchResultSnapshotCoordinator | None:
    port = _resolve_result_snapshot(context, spec)

    if port is None:
        return None

    return SearchResultSnapshotCoordinator(store=port)


def _meilisearch_search_adapter[M: BaseModel](
    context: ExecutionContext,
    member_spec: SearchSpec[M],
    c: MeilisearchSearchConfig,
) -> MeilisearchSimpleSearchAdapter[M]:
    client = context.deps.provide(MeilisearchClientDepKey)
    tenant_aware = c.tenant_aware

    return MeilisearchSimpleSearchAdapter(
        spec=member_spec,
        config=c,
        client=client,
        tenant_provider=context.inv_ctx.get_tenant,
        tenant_aware=tenant_aware,
        snapshot_coord=_snapshot_coord(context, member_spec.snapshot),
    )


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableMeilisearchSearch(SearchQueryDepPort):
    """Build :class:`MeilisearchSimpleSearchAdapter` from spec + config."""

    config: MeilisearchSearchConfig

    def __call__(
        self,
        context: ExecutionContext,
        spec: SearchSpec[Any],
    ) -> SearchQueryPort[Any]:
        return _meilisearch_search_adapter(context, spec, self.config)


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableMeilisearchSearchCommand(SearchCommandDepPort):
    """Build :class:`MeilisearchSearchCommandAdapter` from spec + config."""

    config: MeilisearchSearchConfig

    def __call__(
        self,
        context: ExecutionContext,
        spec: SearchSpec[Any],
    ) -> SearchCommandPort[Any]:
        client = context.deps.provide(MeilisearchClientDepKey)
        tenant_aware = self.config.tenant_aware

        return MeilisearchSearchCommandAdapter(
            spec=spec,
            config=self.config,
            client=client,
            tenant_provider=context.inv_ctx.get_tenant,
            tenant_aware=tenant_aware,
        )


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableMeilisearchFederatedSearch(FederatedSearchQueryDepPort):
    """Build :class:`MeilisearchFederatedSearchAdapter` from spec + config."""

    config: MeilisearchFederatedSearchConfig

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
                raise exc.internal("Hub members are not supported for Meilisearch federation.")

            c = self.config.members.get(m.name)

            if c is None:
                raise exc.internal(
                    f"Member {m.name!r} not found in MeilisearchFederatedSearchConfig.members.",
                )

            leg_cfg = c

            if not leg_cfg.tenant_aware and self.config.tenant_aware:
                leg_cfg = attrs.evolve(leg_cfg, tenant_aware=True)

            port = _meilisearch_search_adapter(context, m, leg_cfg)
            legs.append((m.name, port))

        return MeilisearchFederatedSearchAdapter(
            federated_spec=spec,
            legs=tuple(legs),
            client=client,
            merge=self.config.merge,
            rrf_k=self.config.rrf_k,
            rrf_per_leg_limit=self.config.rrf_per_leg_limit,
            snapshot_coord=_snapshot_coord(context, spec.snapshot),
        )
