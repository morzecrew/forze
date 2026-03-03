"""In-memory stub for DocumentPort."""

from typing import Any, Optional, Sequence, Type, final
from uuid import UUID

from forze.application.contracts.document import (
    DocumentPort,
    DocumentSearchOptions,
)
from forze.application.contracts.query import (
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.base.primitives import JsonDict, utcnow, uuid7
from forze.domain.models import BaseDTO, CreateDocumentCmd, ReadDocument

# ----------------------- #


@final
class InMemoryDocumentPort(
    DocumentPort[ReadDocument, Any, CreateDocumentCmd, BaseDTO],
):
    """In-memory document store for unit tests. Stores by UUID."""

    def __init__(self, read_model: Type[ReadDocument] = ReadDocument) -> None:
        self._store: dict[UUID, JsonDict] = {}
        self._deleted: set[UUID] = set()
        self._read_model = read_model

    def _to_read(self, d: JsonDict) -> ReadDocument:
        return self._read_model.model_validate(d)

    def _to_dict(self, dto: CreateDocumentCmd | BaseDTO) -> JsonDict:
        data = dto.model_dump(mode="json")
        return {k: v for k, v in data.items() if v is not None}

    async def get(
        self,
        pk: UUID,
        *,
        for_update: bool = False,
        return_fields: Optional[Sequence[str]] = None,
    ) -> ReadDocument | JsonDict:
        if pk not in self._store or pk in self._deleted:
            raise KeyError(f"Document not found: {pk}")
        d = dict(self._store[pk])
        if return_fields:
            return {k: d[k] for k in return_fields if k in d}
        return self._to_read(d)

    async def get_many(
        self,
        pks: Sequence[UUID],
        *,
        return_fields: Optional[Sequence[str]] = None,
    ) -> Sequence[ReadDocument] | Sequence[JsonDict]:
        result: list[ReadDocument | JsonDict] = []
        for pk in pks:
            if pk in self._store and pk not in self._deleted:
                d = dict(self._store[pk])
                if return_fields:
                    result.append({k: d[k] for k in return_fields if k in d})
                else:
                    result.append(self._to_read(d))
        return result

    async def find(
        self,
        filters: QueryFilterExpression,
        *,
        for_update: bool = False,
        return_fields: Optional[Sequence[str]] = None,
    ) -> Optional[ReadDocument] | Optional[JsonDict]:
        items, _ = await self.find_many(
            filters=filters, limit=1, return_fields=return_fields
        )
        if not items:
            return None
        return items[0]

    async def find_many(
        self,
        filters: Optional[QueryFilterExpression] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sorts: Optional[QuerySortExpression] = None,
        *,
        return_fields: Optional[Sequence[str]] = None,
    ) -> tuple[list[ReadDocument] | list[JsonDict], int]:
        items = [self._store[pk] for pk in self._store if pk not in self._deleted]
        total = len(items)
        if offset:
            items = items[offset:]
        if limit:
            items = items[:limit]
        if return_fields:
            out = [{k: d[k] for k in return_fields if k in d} for d in items]
        else:
            out = [self._to_read(d) for d in items]
        return out, total

    async def count(self, filters: Optional[QueryFilterExpression] = None) -> int:
        return sum(1 for pk in self._store if pk not in self._deleted)

    async def search(
        self,
        query: str,
        filters: Optional[QueryFilterExpression] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sorts: Optional[QuerySortExpression] = None,
        options: Optional[DocumentSearchOptions] = None,
        *,
        return_fields: Optional[Sequence[str]] = None,
    ) -> tuple[list[ReadDocument] | list[JsonDict], int]:
        return await self.find_many(
            filters=filters,
            limit=limit,
            offset=offset,
            sorts=sorts,
            return_fields=return_fields,
        )

    async def create(self, dto: CreateDocumentCmd) -> ReadDocument:
        data = self._to_dict(dto)
        pk = data.get("id") or uuid7()
        now = utcnow()
        doc = {
            "id": pk,
            "rev": 1,
            "created_at": data.get("created_at") or now,
            "last_update_at": now,
            **{
                k: v
                for k, v in data.items()
                if k not in ("id", "rev", "created_at", "last_update_at")
            },
        }
        self._store[pk] = doc
        self._deleted.discard(pk)
        return self._to_read(doc)

    async def create_many(
        self, dtos: Sequence[CreateDocumentCmd]
    ) -> Sequence[ReadDocument]:
        return [await self.create(d) for d in dtos]

    async def update(
        self, pk: UUID, dto: BaseDTO, *, rev: Optional[int] = None
    ) -> ReadDocument:
        if pk not in self._store or pk in self._deleted:
            raise KeyError(f"Document not found: {pk}")
        doc = dict(self._store[pk])
        patch = self._to_dict(dto)
        doc.update(patch)
        doc["rev"] = doc.get("rev", 1) + 1
        doc["last_update_at"] = utcnow()
        self._store[pk] = doc
        return self._to_read(doc)

    async def update_many(
        self,
        pks: Sequence[UUID],
        dtos: Sequence[BaseDTO],
        *,
        revs: Optional[Sequence[int]] = None,
    ) -> Sequence[ReadDocument]:
        return [await self.update(pk, dto) for pk, dto in zip(pks, dtos)]

    async def touch(self, pk: UUID) -> ReadDocument:
        if pk not in self._store or pk in self._deleted:
            raise KeyError(f"Document not found: {pk}")
        doc = dict(self._store[pk])
        doc["last_update_at"] = utcnow()
        self._store[pk] = doc
        return self._to_read(doc)

    async def touch_many(self, pks: Sequence[UUID]) -> Sequence[ReadDocument]:
        return [await self.touch(pk) for pk in pks]

    async def kill(self, pk: UUID) -> None:
        self._store.pop(pk, None)
        self._deleted.discard(pk)

    async def kill_many(self, pks: Sequence[UUID]) -> None:
        for pk in pks:
            await self.kill(pk)

    async def delete(self, pk: UUID, *, rev: Optional[int] = None) -> ReadDocument:
        if pk not in self._store or pk in self._deleted:
            raise KeyError(f"Document not found: {pk}")
        self._deleted.add(pk)
        return self._to_read(self._store[pk])

    async def delete_many(
        self,
        pks: Sequence[UUID],
        *,
        revs: Optional[Sequence[int]] = None,
    ) -> Sequence[ReadDocument]:
        return [await self.delete(pk) for pk in pks]

    async def restore(self, pk: UUID, *, rev: Optional[int] = None) -> ReadDocument:
        if pk not in self._store:
            raise KeyError(f"Document not found: {pk}")
        self._deleted.discard(pk)
        return self._to_read(self._store[pk])

    async def restore_many(
        self,
        pks: Sequence[UUID],
        *,
        revs: Optional[Sequence[int]] = None,
    ) -> Sequence[ReadDocument]:
        return [await self.restore(pk) for pk in pks]
