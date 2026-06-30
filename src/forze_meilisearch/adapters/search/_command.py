"""Meilisearch :class:`~forze.application.contracts.search.SearchCommandPort` and
:class:`~forze.application.contracts.search.SearchManagementPort` adapters."""

from __future__ import annotations

from typing import Any, Sequence, cast, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.search import (
    SearchCommandPort,
    SearchManagementPort,
    SearchSpec,
)
from forze.application.contracts.tenancy import TENANT_ID_FIELD
from forze_meilisearch.adapters.search.base import MeilisearchSearchGateway
from forze_meilisearch.kernel.client.port import MeilisearchClientPort

# ----------------------- #

_BATCH_SIZE = 1000


@attrs.define(slots=True, kw_only=True, frozen=True)
class _MeilisearchSearchWriteBase[M: BaseModel](MeilisearchSearchGateway[M]):
    """Shared client + task-await plumbing for the write and management adapters."""

    client: MeilisearchClientPort
    spec: SearchSpec[M]

    # ....................... #

    @property
    def _wait_tasks(self) -> bool:
        return self.config.wait_for_tasks

    # ....................... #

    async def _await_task(self, task_info: Any) -> None:
        if not self._wait_tasks:
            return

        uid = int(getattr(task_info, "task_uid", getattr(task_info, "taskUid", 0)))
        await self.client.wait_for_task(uid)


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MeilisearchSearchCommandAdapter[M: BaseModel](
    _MeilisearchSearchWriteBase[M],
    SearchCommandPort[M],
):
    """Document writes (``SearchCommandPort``) for one Meilisearch search surface.

    Data-plane only — index provisioning lives on
    :class:`MeilisearchSearchManagementAdapter`.
    """

    async def upsert(self, documents: Sequence[M]) -> None:
        await self.upsert_many(documents)

    async def upsert_many(self, documents: Sequence[M]) -> None:
        if not documents:
            return

        index = self.client.index(await self._resolved_index_uid())
        # Warm the keyring once before the synchronous encrypting encode (no-op when the
        # route is not encrypted).
        await self.prepare_encrypt()
        payload = [self.to_index_document(d) for d in documents]

        for i in range(0, len(payload), _BATCH_SIZE):
            chunk = payload[i : i + _BATCH_SIZE]
            task = await index.add_documents(chunk, primary_key=self.primary_key)
            await self._await_task(task)

    async def delete(self, ids: Sequence[str]) -> None:
        if not ids:
            return

        index = self.client.index(await self._resolved_index_uid())
        task = await index.delete_documents(list(ids))
        await self._await_task(task)


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MeilisearchSearchManagementAdapter[M: BaseModel](
    _MeilisearchSearchWriteBase[M],
    SearchManagementPort,
):
    """Index provisioning (``SearchManagementPort``) for one Meilisearch search surface.

    Control-plane only — document writes live on
    :class:`MeilisearchSearchCommandAdapter`.
    """

    def _searchable_attributes(self) -> list[str]:
        configured = self.config.searchable_attributes

        if configured is not None:
            return [self.physical_path(f) for f in configured]

        return self.physical_paths(self.spec.fields)

    def _filterable_attributes(self) -> list[str]:
        configured = self.config.filterable_attributes

        attrs_list = (
            [self.physical_path(f) for f in configured]
            if configured is not None
            else list(
                dict.fromkeys(
                    [
                        self.primary_key,
                        *[self.physical_path(f) for f in self.spec.fields],
                    ]
                )
            )
        )

        if self.tenant_aware:
            tenant_attr = self.physical_path(TENANT_ID_FIELD)
            if tenant_attr not in attrs_list:
                attrs_list.append(tenant_attr)

        # Faceting requires the attribute to be filterable in Meilisearch, so a declared
        # facetable field must appear here even when the caller pinned filterable_attributes.
        for field in self.spec.facetable_fields:
            facet_attr = self.physical_path(field)
            if facet_attr not in attrs_list:
                attrs_list.append(facet_attr)

        return attrs_list

    def _sortable_attributes(self) -> list[str]:
        configured = self.config.sortable_attributes

        if configured is not None:
            return [self.physical_path(f) for f in configured]

        pk = self.primary_key
        fields = [self.physical_path(f) for f in self.spec.fields if f != pk]
        return list(dict.fromkeys([pk, *fields]))

    # ....................... #

    async def ensure_index(self) -> None:
        from meilisearch_python_sdk.models.settings import (
            FilterableAttributes,
            MeilisearchSettings,
        )

        index = await self.client.get_or_create_index(
            await self._resolved_index_uid(),
            primary_key=self.primary_key,
        )

        rules = self.config.ranking_rules

        settings = MeilisearchSettings(
            searchable_attributes=self._searchable_attributes(),
            filterable_attributes=cast(
                list[str | FilterableAttributes],
                self._filterable_attributes(),
            ),
            sortable_attributes=self._sortable_attributes(),
            ranking_rules=list(rules) if rules is not None else None,
        )

        task = await index.update_settings(settings)
        await self._await_task(task)

    async def delete_all(self) -> None:
        index = self.client.index(await self._resolved_index_uid())
        task = await index.delete_all_documents()
        await self._await_task(task)
