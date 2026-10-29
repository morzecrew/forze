"""In-memory counter adapter."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import (
    final,
)

import attrs

from forze.application.contracts.counter import (
    CounterAdminPort,
    CounterCapabilities,
    CounterEntry,
    CounterPort,
    validate_counter_ttl,
    validate_counter_value,
)
from forze.base.exceptions import exc
from forze.base.primitives import utcnow
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

    # ....................... #

    def _lapsed(self, key: tuple[str, str | None], now: datetime) -> bool:
        """Whether *key* carries a lifetime that has ended. Caller holds the state lock."""

        expires_at = self.state.counter_expiry.get(key)

        return expires_at is not None and expires_at <= now


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockCounterAdapter(_MockCounterBase, CounterPort):
    """In-memory counter adapter with namespace/suffix partitioning.

    Lifetimes read :func:`~forze.base.primitives.utcnow`, so a frozen or simulated clock
    drives expiry exactly as it drives every other timestamp the mock writes.
    """

    def capabilities(self) -> CounterCapabilities:
        """Report expiry support."""

        return CounterCapabilities(expiry=True)

    # ....................... #

    def _bump(self, by: int, suffix: str | None, ttl: timedelta | None, operation: str) -> int:
        validate_counter_ttl(ttl)

        with self.state.lock:
            key = self._key(suffix)
            now = utcnow()

            if self._lapsed(key, now):
                self.state.counters.pop(key, None)
                self.state.counter_expiry.pop(key, None)

            current = self.state.counters.get(key)

            if by == 0:  # a read: never creates the counter
                return current or 0

            value = (current or 0) + by
            # Python integers are unbounded and every real backend's are not, so the mock
            # has to refuse here or it would accept allocations that only fail in
            # production — the one divergence that makes a mock actively harmful.
            validate_counter_value(value, operation=operation)

            if current is None and ttl is not None:
                self.state.counter_expiry[key] = now + ttl

            self.state.counters[key] = value
            return value

    # ....................... #

    async def incr(
        self,
        by: int = 1,
        *,
        suffix: str | None = None,
        ttl: timedelta | None = None,
    ) -> int:
        return self._bump(by, suffix, ttl, "increment")

    # ....................... #

    async def incr_batch(
        self,
        size: int = 2,
        *,
        suffix: str | None = None,
        ttl: timedelta | None = None,
    ) -> list[int]:
        if size < 1:
            raise exc.precondition("Batch size must be at least 1")

        curr = self._bump(size, suffix, ttl, "batch allocation")
        return list(range(curr - size + 1, curr + 1))

    # ....................... #

    async def decr(
        self,
        by: int = 1,
        *,
        suffix: str | None = None,
        ttl: timedelta | None = None,
    ) -> int:
        return self._bump(-by, suffix, ttl, "decrement")

    # ....................... #

    async def reset(
        self,
        value: int = 1,
        *,
        suffix: str | None = None,
        ttl: timedelta | None = None,
    ) -> int:
        validate_counter_value(value, operation="reset")
        validate_counter_ttl(ttl)

        with self.state.lock:
            key = self._key(suffix)
            self.state.counters[key] = value

            # A reset replaces the lifetime too: no ttl means the counter no longer expires.
            if ttl is None:
                self.state.counter_expiry.pop(key, None)
            else:
                self.state.counter_expiry[key] = utcnow() + ttl

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
        now = utcnow()

        with self.state.lock:
            return [
                CounterEntry(suffix=key[1], value=value)
                for key, value in self.state.counters.items()
                if key[0] == namespace and not self._lapsed(key, now)
            ]
