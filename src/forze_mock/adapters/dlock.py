"""In-memory distributed lock adapter."""

from __future__ import annotations

import time
from datetime import timedelta
from typing import final

import attrs

from forze.application.contracts.dlock import (
    DistributedLockCommandPort,
    DistributedLockQueryPort,
    DistributedLockSpec,
)
from forze_mock.state import MockState
from forze_mock.tenancy import MockTenancyMixin

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockDistributedLockAdapter(
    MockTenancyMixin,
    DistributedLockQueryPort,
    DistributedLockCommandPort,
):
    """Process-local lock simulation with TTL semantics."""

    spec: DistributedLockSpec
    state: MockState
    namespace: str

    # ....................... #

    def _route(self) -> str:
        return self._partitioned_namespace(self.namespace)

    def _store(self) -> dict[str, tuple[str, float]]:
        with self.state.lock:
            return self.state.dlocks.setdefault(self._route(), {})

    # ....................... #

    def _now(self) -> float:
        return time.monotonic()

    def _is_expired(self, expires_at: float) -> bool:
        return self._now() >= expires_at

    # ....................... #

    async def is_locked(self, key: str) -> bool:
        with self.state.lock:
            entry = self._store().get(key)
            if entry is None:
                return False
            if self._is_expired(entry[1]):
                del self._store()[key]
                return False
            return True

    async def get_owner(self, key: str) -> str | None:
        with self.state.lock:
            entry = self._store().get(key)
            if entry is None or self._is_expired(entry[1]):
                if entry is not None:
                    del self._store()[key]
                return None
            return entry[0]

    async def get_ttl(self, key: str) -> timedelta | None:
        with self.state.lock:
            entry = self._store().get(key)
            if entry is None or self._is_expired(entry[1]):
                if entry is not None:
                    del self._store()[key]
                return None
            remaining = max(0.0, entry[1] - self._now())
            return timedelta(seconds=remaining)

    async def acquire(self, key: str, owner: str) -> bool:
        ttl = self.spec.ttl.total_seconds()
        expires = self._now() + ttl
        with self.state.lock:
            store = self._store()
            entry = store.get(key)
            if entry is not None and not self._is_expired(entry[1]):
                return False
            store[key] = (owner, expires)
            return True

    async def release(self, key: str, owner: str) -> bool:
        with self.state.lock:
            store = self._store()
            entry = store.get(key)
            if entry is None or self._is_expired(entry[1]):
                store.pop(key, None)
                return False
            if entry[0] != owner:
                return False
            del store[key]
            return True

    async def reset(self, key: str, owner: str) -> bool:
        ttl = self.spec.ttl.total_seconds()
        with self.state.lock:
            store = self._store()
            entry = store.get(key)
            if entry is None or self._is_expired(entry[1]):
                store.pop(key, None)
                return False
            if entry[0] != owner:
                return False
            store[key] = (owner, self._now() + ttl)
            return True
