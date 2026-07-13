"""In-memory cache adapter."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import (
    Any,
    final,
)

import attrs

from forze.application.contracts.cache import CachePort
from forze_mock.state import MockState
from forze_mock.tenancy import MockTenancyMixin

from ..query._types import _MISSING  # pyright: ignore[reportPrivateUsage]


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockCacheAdapter(MockTenancyMixin, CachePort):
    """In-memory cache adapter with plain and versioned entries."""

    state: MockState
    namespace: str

    # ....................... #

    def _ns(self) -> str:
        return self._partitioned_namespace(self.namespace)

    def _kv(self) -> dict[str, Any]:
        return self.state.cache_kv.setdefault(self._ns(), {})

    # ....................... #

    def _pointers(self) -> dict[str, str]:
        return self.state.cache_pointers.setdefault(self._ns(), {})

    # ....................... #

    def _bodies(self) -> dict[tuple[str, str], Any]:
        return self.state.cache_bodies.setdefault(self._ns(), {})

    # ....................... #

    async def get(self, key: str) -> Any | None:
        with self.state.lock:
            pointer = self._pointers().get(key)
            if pointer is not None:
                body = self._bodies().get((key, pointer), _MISSING)
                if body is not _MISSING:
                    return body
            return self._kv().get(key)

    # ....................... #

    async def exists(self, key: str) -> bool:
        with self.state.lock:
            pointer = self._pointers().get(key)
            if pointer is not None and (key, pointer) in self._bodies():
                return True
            return key in self._kv()

    # ....................... #

    async def get_many(self, keys: Sequence[str]) -> tuple[dict[str, Any], list[str]]:
        with self.state.lock:
            hits: dict[str, Any] = {}
            for key in keys:
                pointer = self._pointers().get(key)
                if pointer is not None:
                    body = self._bodies().get((key, pointer), _MISSING)
                    if body is not _MISSING:
                        hits[key] = body
                        continue
                if key in self._kv():
                    hits[key] = self._kv()[key]
            misses = [key for key in keys if key not in hits]
            return hits, misses

    # ....................... #

    async def set(
        self,
        key: str,
        value: Any,
        *,
        ttl: timedelta | None = None,
    ) -> None:
        # The mock has no clock-based expiry; per-entry ttl is accepted for
        # contract parity and ignored.
        with self.state.lock:
            self._kv()[key] = value

    # ....................... #

    async def set_versioned(
        self,
        key: str,
        version: str,
        value: Any,
        *,
        ttl: timedelta | None = None,
    ) -> None:
        with self.state.lock:
            self._pointers()[key] = version
            self._bodies()[(key, version)] = value

    # ....................... #

    async def set_many(
        self,
        key_mapping: Mapping[str, Any],
        *,
        ttl: timedelta | None = None,
    ) -> None:
        with self.state.lock:
            self._kv().update(key_mapping)

    # ....................... #

    async def set_many_versioned(
        self,
        key_version_mapping: Mapping[tuple[str, str], Any],
        *,
        ttl: timedelta | None = None,
    ) -> None:
        with self.state.lock:
            for (key, version), value in key_version_mapping.items():
                self._pointers()[key] = version
                self._bodies()[(key, version)] = value

    # ....................... #

    async def delete(self, key: str, *, hard: bool) -> None:
        with self.state.lock:
            self._kv().pop(key, None)
            if hard:
                stale = [k for k in self._bodies() if k[0] == key]
                for item in stale:
                    self._bodies().pop(item, None)
            self._pointers().pop(key, None)

    # ....................... #

    async def delete_many(self, keys: Sequence[str], *, hard: bool) -> None:
        for key in keys:
            await self.delete(key, hard=hard)
