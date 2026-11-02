"""In-memory cache adapter."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import (
    Any,
    final,
)

import attrs

from forze.application.contracts.cache import CachePort, InvalidationCallback, Unsubscribe
from forze_mock.state import MockState
from forze_mock.tenancy import MockTenancyMixin

from ..query._types import _MISSING  # pyright: ignore[reportPrivateUsage]


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockCacheAdapter(MockTenancyMixin, CachePort):
    """In-memory cache adapter with plain and versioned entries.

    Like Redis tracking pointers, a versioned write and the delete of a versioned entry are
    broadcast through :attr:`MockState.cache_push`; a plain write is not.
    """

    state: MockState
    namespace: str

    invalidation_push: bool = False
    """Whether :meth:`subscribe_invalidations` subscribes, as ``RedisCacheConfig`` has it."""

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

    def _channel(self) -> tuple[bool, str]:
        # A tenant-aware writer's keys carry the tenant, so only a tenant-aware subscriber
        # hears them, as with Redis key prefixes.
        return self.tenant_aware, self.namespace

    def _push(self, keys: Sequence[str]) -> None:
        tenant = self.require_tenant_if_aware()
        self.state.cache_push.publish(
            self._channel(),
            keys,
            tenant=str(tenant) if tenant is not None else None,
        )

    # ....................... #

    async def subscribe_invalidations(
        self,
        callback: InvalidationCallback,
        *,
        live_within: timedelta | None = None,
    ) -> Unsubscribe | None:
        """Hear every versioned write and delete in this namespace, from any port on this state.

        ``None`` unless :attr:`invalidation_push`. A tenant-aware port hears every tenant, the
        tenant on each event. The stream is live at once, so *live_within* changes nothing.
        """

        if not self.invalidation_push:
            return None

        return self.state.cache_push.subscribe(self._channel(), callback)

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

        self._push([key])

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

        self._push([key for key, _version in key_version_mapping])

    # ....................... #

    async def delete(self, key: str, *, hard: bool) -> None:
        with self.state.lock:
            self._kv().pop(key, None)
            if hard:
                stale = [k for k in self._bodies() if k[0] == key]
                for item in stale:
                    self._bodies().pop(item, None)
            unlinked = self._pointers().pop(key, None) is not None

        if unlinked:
            self._push([key])

    # ....................... #

    async def delete_many(self, keys: Sequence[str], *, hard: bool) -> None:
        for key in keys:
            await self.delete(key, hard=hard)
