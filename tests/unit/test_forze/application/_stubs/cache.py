"""In-memory stub for DocumentCachePort."""

from typing import Any, Optional, Sequence, final
from uuid import UUID

from forze.application.contracts.document import DocumentCachePort

# ----------------------- #


@final
class InMemoryDocumentCachePort(DocumentCachePort):
    """In-memory document cache for unit tests. Key: (pk, rev)."""

    def __init__(self) -> None:
        self._cache: dict[tuple[UUID, int], Any] = {}

    async def get(self, pk: UUID) -> Optional[Any]:
        matches = [(rev, v) for (k, rev), v in self._cache.items() if k == pk]
        if not matches:
            return None
        return max(matches, key=lambda x: x[0])[1]

    async def set(self, pk: UUID, rev: int, value: Any) -> None:
        self._cache[(pk, rev)] = value

    async def delete(self, pk: UUID, *, hard: bool) -> None:
        to_remove = [k for k in self._cache if k[0] == pk]
        for k in to_remove:
            del self._cache[k]

    async def get_many(
        self,
        pks: Sequence[UUID],
    ) -> tuple[dict[UUID, Any], list[UUID]]:
        found: dict[UUID, Any] = {}
        missing: list[UUID] = []
        for pk in pks:
            v = await self.get(pk)
            if v is not None:
                found[pk] = v
            else:
                missing.append(pk)
        return found, missing

    async def set_many(
        self,
        mapping: dict[tuple[UUID, int], Any],
    ) -> None:
        self._cache.update(mapping)

    async def delete_many(self, pks: Sequence[UUID], *, hard: bool) -> None:
        for pk in pks:
            await self.delete(pk, hard=hard)
