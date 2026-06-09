"""Meilisearch search dep factories."""

from typing import Any, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.search import (
    SearchCommandDepPort,
    SearchCommandPort,
    SearchQueryDepPort,
    SearchQueryPort,
    SearchResultSnapshotDepKey,
    SearchResultSnapshotSpec,
    SearchSpec,
)
from forze.application.integrations.search import SearchResultSnapshot
from forze.application.execution import ExecutionContext
from forze_meilisearch.adapters.search._command import MeilisearchSearchCommandAdapter
from forze_meilisearch.adapters.search._simple_base import (
    MeilisearchSimpleSearchAdapter,
)
from forze_meilisearch.execution.deps.configs import MeilisearchSearchConfig
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


# ....................... #


def result_snapshot(
    context: ExecutionContext,
    spec: SearchResultSnapshotSpec | None,
) -> SearchResultSnapshot | None:
    port = _resolve_result_snapshot(context, spec)

    if port is None:
        return None

    return SearchResultSnapshot(store=port)


# ....................... #


def meilisearch_search_adapter[M: BaseModel](
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
        result_snapshot=result_snapshot(context, member_spec.snapshot),
    )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableMeilisearchSearch(SearchQueryDepPort):
    """Build :class:`MeilisearchSimpleSearchAdapter` from spec + config."""

    config: MeilisearchSearchConfig = attrs.field(
        validator=attrs.validators.instance_of(MeilisearchSearchConfig),
    )

    def __call__(
        self,
        context: ExecutionContext,
        spec: SearchSpec[Any],
    ) -> SearchQueryPort[Any]:
        return meilisearch_search_adapter(context, spec, self.config)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableMeilisearchSearchCommand(SearchCommandDepPort):
    """Build :class:`MeilisearchSearchCommandAdapter` from spec + config."""

    config: MeilisearchSearchConfig = attrs.field(
        validator=attrs.validators.instance_of(MeilisearchSearchConfig),
    )

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
