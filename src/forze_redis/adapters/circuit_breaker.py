"""Redis-backed distributed circuit-breaker store."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast, final

import attrs

from forze.application.contracts.resilience import (
    BreakerKey,
    CircuitBreakerStore,
    CircuitBreakerStrategy,
    Transition,
)
from forze.application.execution.resilience import InMemoryCircuitBreakerStore
from forze.application.execution.tracing import record as trace_record
from forze.base.primitives import monotonic

from ..kernel.client import RedisClientPort
from ..kernel.scripts import CIRCUIT_BREAKER_ADMIT, CIRCUIT_BREAKER_RECORD

# ----------------------- #

_DEFAULT_NAMESPACE = "forze"
_MIN_KEY_TTL_MS = 60_000


def _transition(value: str) -> Transition:
    return None if value == "none" else cast(Transition, value)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class RedisCircuitBreakerStore(CircuitBreakerStore):
    """Distributed circuit-breaker state shared across replicas via Redis.

    Breaker counters/phase live in a Redis hash per ``(policy, route)``, mutated
    atomically by Lua using the **server clock** (no replica skew). A short-TTL local
    cache fast-paths the closed ``admit`` (saving a Redis read when healthy); every
    outcome is recorded to Redis so failure counts are shared and the fleet trips
    together. On **any** Redis error it falls open to a process-local store — the
    breaker must never become a per-call single point of failure.
    """

    client: RedisClientPort
    namespace: str = _DEFAULT_NAMESPACE
    local_cache_ttl: float = 0.25
    max_cache_entries: int = attrs.field(default=4096, validator=attrs.validators.ge(1))
    """Cap on the local fast-path cache. Bounds memory if ``route`` is derived from
    per-host / per-tenant / per-URL values (otherwise key cardinality is static).
    Evicting an entry only costs one extra Redis ``admit`` read, never correctness.
    Must be ``>= 1`` — a non-positive cap would break FIFO eviction in :meth:`_remember`."""
    fallback: CircuitBreakerStore = attrs.field(factory=InMemoryCircuitBreakerStore)
    clock: Callable[[], float] = monotonic

    _cache: dict[BreakerKey, tuple[str, float]] = attrs.field(factory=dict, init=False)

    # ....................... #

    def _remember(self, key: BreakerKey, phase: str) -> None:
        """Record the latest phase for *key*, bounding the fast-path cache size."""

        if key not in self._cache and len(self._cache) >= self.max_cache_entries:
            # FIFO-evict the oldest entry (dict preserves insertion order); the
            # fast-path is a 0.25s TTL optimization, so a miss just reads Redis.
            self._cache.pop(next(iter(self._cache)), None)

        self._cache[key] = (phase, self.clock() + self.local_cache_ttl)

    # ....................... #

    def _key(self, key: BreakerKey) -> str:
        policy, route = key
        suffix = route if route is not None else "-"
        return f"{self.namespace}:resilience:breaker:{policy}:{suffix}"

    # ....................... #

    def _strat_args(self, strat: CircuitBreakerStrategy) -> list[Any]:
        window_s = strat.sampling_window.total_seconds()
        break_s = strat.break_duration.total_seconds()
        ttl_ms = max(int(max(window_s, break_s) * 2 * 1000), _MIN_KEY_TTL_MS)

        return [
            strat.failure_ratio,
            window_s,
            strat.min_throughput,
            break_s,
            strat.half_open_max_calls,
            ttl_ms,
        ]

    # ....................... #

    async def admit(
        self,
        key: BreakerKey,
        strat: CircuitBreakerStrategy,
    ) -> tuple[bool, Transition]:
        cached = self._cache.get(key)

        if cached is not None and cached[0] == "closed" and cached[1] > self.clock():
            return True, None  # fast-path: closed + fresh, no Redis round-trip

        try:
            res = await self.client.run_script(
                CIRCUIT_BREAKER_ADMIT,
                [self._key(key)],
                self._strat_args(strat),
            )

        except Exception:  # noqa: BLE001 — fail-open: the store must never break calls
            self._degrade("admit", key)
            return await self.fallback.admit(key, strat)

        allowed, phase, transition = res.split(":")
        self._remember(key, phase)

        return allowed == "1", _transition(transition)

    # ....................... #

    async def record(
        self,
        key: BreakerKey,
        strat: CircuitBreakerStrategy,
        ok: bool,
    ) -> Transition:
        args: list[Any] = ["1" if ok else "0", *self._strat_args(strat)]

        try:
            res = await self.client.run_script(
                CIRCUIT_BREAKER_RECORD,
                [self._key(key)],
                args,
            )

        except Exception:  # noqa: BLE001 — fail-open
            self._degrade("record", key)
            return await self.fallback.record(key, strat, ok)

        phase, transition = res.split(":")
        self._remember(key, phase)

        return _transition(transition)

    # ....................... #

    def _degrade(self, op: str, key: BreakerKey) -> None:
        trace_record(
            domain="resilience",
            op="breaker_store_degraded",
            surface=f"redis_circuit_breaker.{op}",
            route=str(key[1]) if key[1] is not None else None,
            phase=str(key[0]),
        )


# ....................... #


def redis_circuit_breaker_store(
    client: RedisClientPort,
    *,
    namespace: str = _DEFAULT_NAMESPACE,
    local_cache_ttl: float = 0.25,
) -> RedisCircuitBreakerStore:
    """Build a distributed breaker store for ``ResilienceDepsModule(breaker_store=...)``.

    Pass the same ``RedisClientPort`` singleton used by :class:`RedisDepsModule`; the
    executor is a process-wide singleton, so wire this at composition time.
    """

    return RedisCircuitBreakerStore(
        client=client,
        namespace=namespace,
        local_cache_ttl=local_cache_ttl,
    )
