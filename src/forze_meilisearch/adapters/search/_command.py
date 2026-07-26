"""Meilisearch :class:`~forze.application.contracts.search.SearchCommandPort` and
:class:`~forze.application.contracts.search.SearchManagementPort` adapters."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, cast, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.search import (
    SearchCommandPort,
    SearchManagementPort,
    SearchSpec,
)
from forze.application.contracts.tenancy import TENANT_ID_FIELD
from forze.base.exceptions import exc
from forze_meilisearch.adapters.search._filter_render import (
    format_literal,
    safe_attribute,
)
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

    async def _await_task(
        self,
        task_info: Any,
        *,
        tolerate: frozenset[str] = frozenset(),
    ) -> None:
        """Await *task_info*, raising unless it succeeded or failed with a tolerated code.

        *tolerate* holds Meilisearch error codes whose failure already satisfies the
        caller's postcondition, so the task is not an error for that specific call.
        """

        if not self._wait_tasks:
            return

        uid = int(getattr(task_info, "task_uid", getattr(task_info, "taskUid", 0)))
        # Bound the wait so a stuck task raises (via the client's timeout mapping)
        # instead of hanging the caller forever.
        task = await self.client.wait_for_task(uid, timeout=self.config.task_wait_timeout)

        # A completed Meilisearch task can still have *failed* — treat any terminal
        # status other than ``succeeded`` as an error rather than silent success.
        status = str(getattr(task, "status", "") or "").lower()

        if not status or status == "succeeded":
            return

        # The SDK hands the task error back as a plain mapping (``code`` / ``type`` /
        # ``message``); anything else is stringified whole rather than dropped.
        raw_error = cast(Any, getattr(task, "error", None))
        fields: Mapping[str, Any] = (
            cast(Mapping[str, Any], raw_error) if isinstance(raw_error, Mapping) else {}
        )
        code = str(fields.get("code") or "")
        kind = str(fields.get("type") or "")
        message = str(fields.get("message") or "") or f"{raw_error}"

        if code in tolerate:
            return

        details = {"task_uid": uid, "status": status, "meili_code": code, "error": message}

        # Meilisearch labels a rejected request ``invalid_request``: the caller named a
        # missing index, sent a document the schema refuses, filtered on an attribute that
        # is not filterable. Classifying those as infrastructure implied a retryable server
        # fault for something that will fail identically forever, and the breaker counted
        # them against the engine's health. The engine's own message rides along, because
        # "task N did not succeed" alone gives a caller nothing to act on.
        if kind == "invalid_request":
            raise exc.precondition(
                f"Meilisearch rejected the request ({code or 'invalid_request'}): {message}",
                details=details,
            )

        raise exc.infrastructure(
            f"Meilisearch task {uid} did not succeed (status={status}): {message}",
            details=details,
        )


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
        tenant_filter = self._tenant_filter()

        if tenant_filter is not None:
            # Tagged tenancy: scope the delete so a foreign/guessed id in the shared
            # index can't remove another tenant's document.
            id_attr = safe_attribute(self.primary_key)
            id_list = ", ".join(format_literal(i) for i in ids)
            task = await index.delete_documents_by_filter(
                f"({id_attr} IN [{id_list}]) AND {tenant_filter}"
            )
        else:
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
            Pagination,
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
            # Provision the index's own cap to match the route's ``max_total_hits`` so the
            # fail-closed read guard and the engine agree on the ceiling.
            pagination=Pagination(max_total_hits=self.config.max_total_hits),
        )

        task = await index.update_settings(settings)
        await self._await_task(task)

    async def delete_all(self) -> None:
        index = self.client.index(await self._resolved_index_uid())
        tenant_filter = self._tenant_filter()

        if tenant_filter is not None:
            # Tagged tenancy: only this tenant's documents, never the whole shared
            # index — ``delete_all_documents`` would wipe every tenant.
            task = await index.delete_documents_by_filter(tenant_filter)
        else:
            task = await index.delete_all_documents()

        # An index that does not exist yet already satisfies "the index holds no
        # documents", so the wipe is a no-op rather than a failure. The documented
        # rebuild-from-scratch workflow is exactly delete_all-then-rebuild, which on a
        # fresh deployment runs before anything has provisioned the index — and it
        # succeeded against the in-memory adapter, so the failure only ever showed up
        # in a real one. Tolerated per call, not globally: the same code from an upsert
        # or a filtered delete still means the write went nowhere.
        await self._await_task(task, tolerate=frozenset({"index_not_found"}))
