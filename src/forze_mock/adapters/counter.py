"""In-memory counter adapter."""

from __future__ import annotations

from collections.abc import Sequence
from typing import (
    final,
)

import attrs

from forze.application.contracts.counter import (
    CounterAdminPort,
    CounterEntry,
    CounterPort,
)
from forze.base.exceptions import exc
from forze_mock.state import MockState
from forze_mock.tenancy import MockTenancyMixin


@attrs.define(slots=True, kw_only=True, frozen=True)
class _MockCounterBase(MockTenancyMixin):
    """Shared namespace/suffix key resolution for the counter data and admin adapters."""

    state: MockState
    namespace: str

    # ....................... #

    def _key(self, suffix: str | None) -> tuple[str, str | None]:
        ns = self._partitioned_namespace(self.namespace)
        return ns, suffix


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockCounterAdapter(_MockCounterBase, CounterPort):
    """In-memory counter adapter with namespace/suffix partitioning."""

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
        if size < 1:
            raise exc.precondition("Batch size must be at least 1")
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


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockCounterAdminAdapter(_MockCounterBase, CounterAdminPort):
    """Enumerate the in-memory counters allocated under one namespace."""

    async def list_counters(self) -> Sequence[CounterEntry]:
        # ``state.counters`` is keyed ``(namespace, suffix)`` across *every* spec, so the
        # namespace has to be matched exactly — a prefix match would fold a spec named
        # ``orders`` together with one named ``orders_archive``. The tenant partition is
        # already baked into the resolved namespace, so this is scoped to the bound tenant
        # for free.
        namespace = self._partitioned_namespace(self.namespace)

        with self.state.lock:
            return [
                CounterEntry(suffix=suffix, value=value)
                for (ns, suffix), value in self.state.counters.items()
                if ns == namespace
            ]
