"""In-memory counter adapter."""

from __future__ import annotations

from typing import (
    final,
)
import attrs
from forze.application.contracts.counter import CounterPort
from forze.base.exceptions import exc
from forze_mock.state import MockState
from forze_mock.tenancy import MockTenancyMixin, partition_namespace

@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockCounterAdapter(MockTenancyMixin, CounterPort):
    """In-memory counter adapter with namespace/suffix partitioning."""

    state: MockState
    namespace: str

    # ....................... #

    def _key(self, suffix: str | None) -> tuple[str, str | None]:
        ns = partition_namespace(self.require_tenant_if_aware(), self.namespace)
        return ns, suffix

    # ....................... #

    async def incr(self, by: int = 1, *, suffix: str | None = None) -> int:
        with self.state.lock:
            key = self._key(suffix)
            value = self.state.counters.get(key, 0) + by
            self.state.counters[key] = value
            return value

    # ....................... #

    async def incr_batch(
        self,
        size: int = 2,
        *,
        suffix: str | None = None,
    ) -> list[int]:
        if size <= 1:
            raise exc.internal("Size must be greater than 1")
        with self.state.lock:
            key = self._key(suffix)
            prev = self.state.counters.get(key, 0)
            curr = prev + size
            self.state.counters[key] = curr
            return list(range(prev + 1, curr + 1))

    # ....................... #

    async def decr(self, by: int = 1, *, suffix: str | None = None) -> int:
        with self.state.lock:
            key = self._key(suffix)
            value = self.state.counters.get(key, 0) - by
            self.state.counters[key] = value
            return value

    # ....................... #

    async def reset(self, value: int = 1, *, suffix: str | None = None) -> int:
        with self.state.lock:
            self.state.counters[self._key(suffix)] = value
            return value
