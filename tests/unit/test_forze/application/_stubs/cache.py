"""In-memory stub for CachePort."""

from typing import Any, Optional, Sequence, final


# ----------------------- #


@final
class InMemoryCachePort:
    """In-memory cache for unit tests. Implements :class:`CachePort`."""

    def __init__(self) -> None:
        self._cache: dict[str, Any] = {}
        self._versioned: dict[tuple[str, str], Any] = {}

    async def get(self, key: str) -> Optional[Any]:
        return self._cache.get(key)

    async def get_many(
        self,
        keys: Sequence[str],
    ) -> tuple[dict[str, Any], list[str]]:
        found: dict[str, Any] = {}
        missing: list[str] = []
        for k in keys:
            v = self._cache.get(k)
            if v is not None:
                found[k] = v
            else:
                missing.append(k)
        return found, missing

    async def set(self, key: str, value: Any) -> None:
        self._cache[key] = value

    async def set_versioned(self, key: str, version: str, value: Any) -> None:
        self._versioned[(key, version)] = value
        self._cache[key] = value

    async def set_many(self, key_mapping: dict[str, Any]) -> None:
        self._cache.update(key_mapping)

    async def set_many_versioned(
        self,
        key_version_mapping: dict[tuple[str, str], Any],
    ) -> None:
        for (k, v), val in key_version_mapping.items():
            self._versioned[(k, v)] = val
            self._cache[k] = val

    async def delete(self, key: str, *, hard: bool) -> None:
        self._cache.pop(key, None)
        to_remove = [kv for kv in self._versioned if kv[0] == key]
        for kv in to_remove:
            del self._versioned[kv]

    async def delete_many(self, keys: Sequence[str], *, hard: bool) -> None:
        for k in keys:
            await self.delete(k, hard=hard)
