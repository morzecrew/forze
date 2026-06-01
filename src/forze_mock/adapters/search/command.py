"""In-memory search index command adapter (document store)."""

from __future__ import annotations

from typing import Sequence, final
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.search import SearchCommandPort, SearchSpec
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_persistence_dump
from forze_mock.state import MockState
from forze_mock.tenancy import MockTenancyMixin, partition_namespace

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockSearchCommandAdapter(MockTenancyMixin, SearchCommandPort[BaseModel]):
    """Mutate the same in-memory document bucket as :class:`MockSearchAdapter`."""

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

    # ....................... #

    async def ensure_index(self) -> None:
        with self.state.lock:
            self.state.documents.setdefault(self._resolved_namespace(), {})

    async def upsert(self, documents: Sequence[BaseModel]) -> None:
        await self.upsert_many(documents)

    async def upsert_many(self, documents: Sequence[BaseModel]) -> None:
        with self.state.lock:
            store = self._store()
            for doc in documents:
                data = pydantic_persistence_dump(doc)
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

    async def delete_all(self) -> None:
        with self.state.lock:
            self._store().clear()
