"""In-memory search index command and management adapters (document store)."""

from __future__ import annotations

from typing import Sequence, final
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.search import (
    SearchCommandPort,
    SearchManagementPort,
    SearchSpec,
)
from forze.base.primitives import JsonDict
from forze_mock.state import MockState
from forze_mock.tenancy import MockTenancyMixin, partition_namespace

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class _MockSearchBase(MockTenancyMixin):
    """Shared in-memory bucket resolution for the search command/management adapters."""

    state: MockState
    spec: SearchSpec[BaseModel]
    namespace: str | None = None

    # ....................... #

    def _resolved_namespace(self) -> str:
        base = self.namespace if self.namespace is not None else str(self.spec.name)
        return partition_namespace(self.require_tenant_if_aware(), base)

    def _store(self) -> dict[UUID, JsonDict]:
        with self.state.lock:
            return self.state.documents.setdefault(self._resolved_namespace(), {})


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockSearchCommandAdapter(_MockSearchBase, SearchCommandPort[BaseModel]):
    """Mutate the same in-memory document bucket as :class:`MockSearchAdapter`.

    Data-plane only (``SearchCommandPort``); index provisioning lives on
    :class:`MockSearchManagementAdapter`."""

    async def upsert(self, documents: Sequence[BaseModel]) -> None:
        await self.upsert_many(documents)

    async def upsert_many(self, documents: Sequence[BaseModel]) -> None:
        with self.state.lock:
            store = self._store()
            for doc in documents:
                data = self.spec.resolved_read_codec.encode_persistence_mapping(doc)
                doc_id = data.get("id")
                if doc_id is None:
                    continue
                store[doc_id] = dict(data)

    async def delete(self, ids: Sequence[str]) -> None:
        with self.state.lock:
            store = self._store()
            for raw in ids:
                try:
                    uid = UUID(str(raw))
                except ValueError:
                    continue
                store.pop(uid, None)


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockSearchManagementAdapter(_MockSearchBase, SearchManagementPort):
    """Index provisioning (``SearchManagementPort``) over the in-memory bucket.

    Control-plane only (``ensure_index`` / ``delete_all``); document writes live on
    :class:`MockSearchCommandAdapter`."""

    async def ensure_index(self) -> None:
        with self.state.lock:
            self.state.documents.setdefault(self._resolved_namespace(), {})

    async def delete_all(self) -> None:
        with self.state.lock:
            self._store().clear()
