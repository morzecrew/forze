"""Meilisearch search dep factories."""

from typing import Any, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.crypto import (
    DeterministicCipherDepKey,
    KeyringDepKey,
)
from forze.application.contracts.search import (
    SearchCommandDepPort,
    SearchCommandPort,
    SearchQueryDepPort,
    SearchQueryPort,
    SearchResultSnapshotDepKey,
    SearchResultSnapshotSpec,
    SearchSpec,
)
from forze.application.execution import ExecutionContext
from forze.application.integrations.search import (
    SearchResultSnapshot,
    resolve_search_read_codec_spec,
    search_spec_encrypts,
)
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
    *,
    encrypted: bool = False,
) -> SearchResultSnapshot | None:
    port = _resolve_result_snapshot(context, spec)

    if port is None:
        return None

    cipher = (
        context.deps.provide(KeyringDepKey)
        if encrypted and context.deps.exists(KeyringDepKey)
        else None
    )

    return SearchResultSnapshot(
        store=port, cipher=cipher, cipher_tenant=context.inv_ctx.get_tenant
    )


# ....................... #


def _encrypting_spec[M: BaseModel](
    context: ExecutionContext, spec: SearchSpec[M]
) -> SearchSpec[M]:
    """Wrap the read codec so encrypted/searchable fields are sealed in the index and
    decrypted on read (shared resolver — default AAD label, fail-closed)."""

    return resolve_search_read_codec_spec(
        spec,
        keyring=(
            context.deps.provide(KeyringDepKey)
            if context.deps.exists(KeyringDepKey)
            else None
        ),
        deterministic=(
            context.deps.provide(DeterministicCipherDepKey)
            if context.deps.exists(DeterministicCipherDepKey)
            else None
        ),
        tenant_provider=context.inv_ctx.get_tenant,
    )


def meilisearch_search_adapter[M: BaseModel](
    context: ExecutionContext,
    member_spec: SearchSpec[M],
    c: MeilisearchSearchConfig,
) -> MeilisearchSimpleSearchAdapter[M]:
    client = context.deps.provide(MeilisearchClientDepKey)
    tenant_aware = c.tenant_aware

    return MeilisearchSimpleSearchAdapter(
        spec=_encrypting_spec(context, member_spec),
        config=c,
        client=client,
        tenant_provider=context.inv_ctx.get_tenant,
        tenant_aware=tenant_aware,
        result_snapshot=result_snapshot(
            context, member_spec.snapshot, encrypted=search_spec_encrypts(member_spec)
        ),
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
            spec=_encrypting_spec(context, spec),
            config=self.config,
            client=client,
            tenant_provider=context.inv_ctx.get_tenant,
            tenant_aware=tenant_aware,
        )
