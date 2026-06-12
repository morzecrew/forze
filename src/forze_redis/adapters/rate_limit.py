"""Redis-backed distributed rate-limit store."""

from __future__ import annotations

from typing import Any, final

import attrs

from forze.application.contracts.resilience import RateLimitStrategy
from forze.application.execution.resilience import (
    InMemoryRateLimitStore,
    RateLimitKey,
    RateLimitStore,
)
from forze.application.execution.tracing import record as trace_record

from ..kernel.client import RedisClientPort
from ..kernel.scripts import RATE_LIMIT_ACQUIRE

# ----------------------- #

_DEFAULT_NAMESPACE = "forze"
_MIN_KEY_TTL_MS = 60_000


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class RedisRateLimitStore(RateLimitStore):
    """Distributed token buckets shared across replicas via Redis.

    With the process-local default each replica enforces ``permits/per``
    independently (fleet-effective rate = ``permits × replicas``); this store
    keeps the bucket in a Redis hash per ``(policy, route)``, mutated atomically
    by Lua using the **server clock** (no replica skew), so the declared rate is
    the *fleet's* rate. Every acquire is one Redis round-trip — a token is
    consumed per decision, so there is nothing cacheable (unlike the breaker's
    closed fast-path). On **any** Redis error it falls open to a process-local
    bucket — the limiter must never become a per-call single point of failure;
    degraded mode still rate-limits per replica.
    """

    client: RedisClientPort
    namespace: str = _DEFAULT_NAMESPACE
    fallback: RateLimitStore = attrs.field(factory=InMemoryRateLimitStore)

    # ....................... #

    def _key(self, key: RateLimitKey) -> str:
        policy, route = key
        suffix = route if route is not None else "-"
        return f"{self.namespace}:resilience:ratelimit:{policy}:{suffix}"

    # ....................... #

    def _strat_args(self, strat: RateLimitStrategy) -> list[Any]:
        rate = strat.permits / strat.per.total_seconds()
        # Long enough for an idle bucket to refill fully before the key expires
        # (expiry resets the bucket to full, which is the same state).
        ttl_ms = max(int(strat.capacity / rate * 2 * 1000), _MIN_KEY_TTL_MS)

        return [rate, strat.capacity, ttl_ms]

    # ....................... #

    async def try_acquire(
        self,
        key: RateLimitKey,
        strat: RateLimitStrategy,
    ) -> bool:
        try:
            res = await self.client.run_script(
                RATE_LIMIT_ACQUIRE,
                [self._key(key)],
                self._strat_args(strat),
            )

        except Exception:  # noqa: BLE001 — fail-open: the store must never break calls
            self._degrade(key)
            return await self.fallback.try_acquire(key, strat)

        return res == "1"

    # ....................... #

    def _degrade(self, key: RateLimitKey) -> None:
        trace_record(
            domain="resilience",
            op="rate_limit_store_degraded",
            surface="redis_rate_limit.try_acquire",
            route=str(key[1]) if key[1] is not None else None,
            phase=str(key[0]),
        )


# ....................... #


def redis_rate_limit_store(
    client: RedisClientPort,
    *,
    namespace: str = _DEFAULT_NAMESPACE,
) -> RedisRateLimitStore:
    """Build a distributed rate-limit store for ``ResilienceDepsModule(rate_limit_store=...)``.

    Pass the same ``RedisClientPort`` singleton used by :class:`RedisDepsModule`; the
    executor is a process-wide singleton, so wire this at composition time.
    """

    return RedisRateLimitStore(client=client, namespace=namespace)
