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
    SearchManagementDepPort,
    SearchManagementPort,
    SearchQueryDepPort,
    SearchQueryPort,
    SearchResultSnapshotSpec,
    SearchSpec,
    resolve_result_snapshot,
)
from forze.application.execution import ExecutionContext
from forze.application.integrations.search import (
    SearchResultSnapshot,
    resolve_search_read_codec_spec,
    resolve_snapshot_cipher,
    search_spec_encrypts,
)
from forze.base.exceptions import exc
from forze_meilisearch.adapters.search._command import (
    MeilisearchSearchCommandAdapter,
    MeilisearchSearchManagementAdapter,
)
from forze_meilisearch.adapters.search._search_params import (
    places_nulls,
    sort_attribute,
    sortable_attributes,
)
from forze_meilisearch.adapters.search._simple_base import (
    MeilisearchSimpleSearchAdapter,
)
from forze_meilisearch.execution.deps.configs import MeilisearchSearchConfig
from forze_meilisearch.execution.deps.keys import MeilisearchClientDepKey

# ....................... #


def result_snapshot(
    context: ExecutionContext,
    spec: SearchResultSnapshotSpec | None,
    *,
    encrypted: bool = False,
) -> SearchResultSnapshot | None:
    port = resolve_result_snapshot(context, spec)

    if port is None:
        return None

    cipher = resolve_snapshot_cipher(
        encrypted=encrypted,
        keyring=(
            context.deps.provide(KeyringDepKey) if context.deps.exists(KeyringDepKey) else None
        ),
    )

    return SearchResultSnapshot(store=port, cipher=cipher, cipher_tenant=context.inv_ctx.get_tenant)


# ....................... #


def _encrypting_spec[M: BaseModel](context: ExecutionContext, spec: SearchSpec[M]) -> SearchSpec[M]:
    """Wrap the read codec so encrypted/searchable fields are sealed in the index and
    decrypted on read (shared resolver — default AAD label, fail-closed)."""

    return resolve_search_read_codec_spec(
        spec,
        keyring=(
            context.deps.provide(KeyringDepKey) if context.deps.exists(KeyringDepKey) else None
        ),
        deterministic=(
            context.deps.provide(DeterministicCipherDepKey)
            if context.deps.exists(DeterministicCipherDepKey)
            else None
        ),
        tenant_provider=context.inv_ctx.get_tenant,
    )


def _refuse_an_unsortable_default_sort(spec: SearchSpec[Any], c: MeilisearchSearchConfig) -> None:
    """Refuse a ``default_sort`` the index cannot sort an unsorted page by.

    A pinned ``sortable_attributes`` that leaves out one of its fields, or a null placement in
    it, which Meilisearch cannot honour: either would fail every unsorted request, and as the
    caller's error. Refused when a query or a provisioning port is built, so ``ensure_index``
    never provisions an index its search port then refuses.
    """

    if not spec.default_sort:
        return

    if placing := [f for f, value in spec.default_sort.items() if places_nulls(value)]:
        raise exc.configuration(
            f"Meilisearch search {spec.name!r}: Meilisearch cannot place nulls, which the "
            f"default_sort asks for on {placing}; drop 'nulls' from them.",
        )

    sortable = sortable_attributes(spec, c)

    if missing := [f for f in spec.default_sort if sort_attribute(f, c) not in sortable]:
        raise exc.configuration(
            f"Meilisearch search {spec.name!r}: sortable_attributes leaves out the "
            f"default_sort field(s) {missing}; list them, or leave sortable_attributes unset.",
        )


# ....................... #


def meilisearch_search_adapter[M: BaseModel](
    context: ExecutionContext,
    member_spec: SearchSpec[M],
    c: MeilisearchSearchConfig,
) -> MeilisearchSimpleSearchAdapter[M]:
    _refuse_an_unsortable_default_sort(member_spec, c)
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


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableMeilisearchSearchManagement(SearchManagementDepPort):
    """Build the Meilisearch index-provisioning (``SearchManagementPort``) adapter."""

    config: MeilisearchSearchConfig = attrs.field(
        validator=attrs.validators.instance_of(MeilisearchSearchConfig),
    )

    def __call__(
        self,
        context: ExecutionContext,
        spec: SearchSpec[Any],
    ) -> SearchManagementPort:
        _refuse_an_unsortable_default_sort(spec, self.config)

        return MeilisearchSearchManagementAdapter(
            spec=_encrypting_spec(context, spec),
            config=self.config,
            client=context.deps.provide(MeilisearchClientDepKey),
            tenant_provider=context.inv_ctx.get_tenant,
            tenant_aware=self.config.tenant_aware,
        )
