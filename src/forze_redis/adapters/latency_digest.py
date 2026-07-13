"""Redis-backed distributed latency digest (mergeable DDSketch).

The adaptive bulkhead's congestion signal is a windowed latency quantile. With
the in-process default each replica reacts to its own p95; this store keeps a
**mergeable DDSketch** in a Redis hash per ``(policy, route)`` — every replica's
``HINCRBY`` lands in the same bins, so the bins are the *fleet's* latency
distribution by construction and the AIMD limit reacts to fleet-wide pressure.

Bucketing is delegated to :class:`~forze.base.primitives.DDSketch` so the Redis
bins and any in-process sketch of the same ``relative_accuracy`` are identical.
Samples are recorded every call (a cheap ``HINCRBY``); the quantile read (which
scans the bins) is throttled by a short local cache, mirroring the breaker. On
**any** Redis error it falls open to a process-local digest — the signal must
never become a per-call single point of failure. A backoff drops the shared
hash (a fleet-wide fresh epoch), and an idle digest expires by TTL.
"""

from collections.abc import Callable
from typing import final

import attrs

from forze.application.contracts.resilience import (
    AdaptiveBulkheadStrategy,
    LatencyDigestKey,
    LatencyDigestStore,
)
from forze.application.execution.resilience import InMemoryLatencyDigestStore
from forze.application.execution.tracing import record as trace_record
from forze.base.primitives import DDSketch, monotonic

from ..kernel.client import RedisClientPort
from ..kernel.scripts import (
    LATENCY_DIGEST_QUANTILE,
    LATENCY_DIGEST_RECORD,
    LATENCY_DIGEST_RESET,
)

# ----------------------- #

_DEFAULT_NAMESPACE = "forze"
_DEFAULT_RELATIVE_ACCURACY = 0.01
_DEFAULT_WINDOW_TTL_MS = 60_000
_MIN_SAMPLES = 5
"""Warmup floor mirroring the in-process windowed-P² estimator: below this the
quantile read returns ``None`` and the AIMD holds the limit."""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class RedisLatencyDigestStore(LatencyDigestStore):
    """Distributed adaptive-bulkhead latency digest shared across replicas via Redis."""

    client: RedisClientPort
    namespace: str = _DEFAULT_NAMESPACE
    relative_accuracy: float = _DEFAULT_RELATIVE_ACCURACY
    window_ttl_ms: int = _DEFAULT_WINDOW_TTL_MS
    local_cache_ttl: float = 0.25
    fallback: LatencyDigestStore = attrs.Factory(InMemoryLatencyDigestStore)
    clock: Callable[[], float] = monotonic

    # ....................... #

    _bucketer: DDSketch = attrs.field(init=False)
    _cache: dict[LatencyDigestKey, tuple[float | None, float]] = attrs.field(
        factory=dict,
        init=False,
    )

    # ....................... #

    def __attrs_post_init__(self) -> None:
        # A throwaway sketch used purely for its bucketing math (index /
        # index_value) — it never stores counts; the bins live in Redis.
        self._bucketer = DDSketch(relative_accuracy=self.relative_accuracy)

    # ....................... #

    def _key(self, key: LatencyDigestKey) -> str:
        policy, route = key
        suffix = route if route is not None else "-"

        return f"{self.namespace}:resilience:latency:{policy}:{suffix}"

    # ....................... #

    async def observe(
        self,
        key: LatencyDigestKey,
        latency: float,
        strat: AdaptiveBulkheadStrategy,
    ) -> float | None:
        if latency <= 0.0:
            # A zero/negative-duration sample carries no latency signal — don't
            # record it (and don't mistake the bucketing's domain rejection for
            # a Redis failure). Serve the last known quantile, if still fresh.
            cached = self._cache.get(key)

            return cached[0] if cached is not None and cached[1] > self.clock() else None

        # Compute the bucket index outside the try so a domain error can never
        # masquerade as a Redis failure and silently route to the fallback.
        index = self._bucketer.index(latency)

        try:
            await self.client.run_script(
                LATENCY_DIGEST_RECORD,
                [self._key(key)],
                [str(index), self.window_ttl_ms],
            )

        except Exception:
            self._degrade("observe", key)
            return await self.fallback.observe(key, latency, strat)

        cached = self._cache.get(key)

        if cached is not None and cached[1] > self.clock():
            return cached[0]  # fast-path: reuse the recent quantile read

        try:
            res = await self.client.run_script(
                LATENCY_DIGEST_QUANTILE,
                [self._key(key)],
                [strat.latency_quantile, _MIN_SAMPLES],
            )
            # Decode inside the try: a malformed/out-of-range result must
            # degrade like a Redis error, never propagate and break the call
            # whose work already succeeded.
            value = None if res == "" else self._bucketer.index_value(int(res))

        except Exception:
            self._degrade("quantile", key)
            return await self.fallback.observe(key, latency, strat)

        self._cache[key] = (value, self.clock() + self.local_cache_ttl)

        return value

    # ....................... #

    async def reset(
        self,
        key: LatencyDigestKey,
        strat: AdaptiveBulkheadStrategy,
    ) -> None:
        self._cache.pop(key, None)

        try:
            await self.client.run_script(LATENCY_DIGEST_RESET, [self._key(key)], [])

        except Exception:
            self._degrade("reset", key)

        finally:
            # Reset the standby digest too (not just on Redis error), so a later
            # degradation doesn't serve a pre-backoff distribution.
            await self.fallback.reset(key, strat)

    # ....................... #

    @staticmethod
    def _degrade(op: str, key: LatencyDigestKey) -> None:
        trace_record(
            domain="resilience",
            op="latency_digest_degraded",
            surface=f"redis_latency_digest.{op}",
            route=str(key[1]) if key[1] is not None else None,
            phase=str(key[0]),
        )


# ....................... #


def redis_latency_digest_store(
    client: RedisClientPort,
    *,
    namespace: str = _DEFAULT_NAMESPACE,
    relative_accuracy: float = _DEFAULT_RELATIVE_ACCURACY,
    window_ttl_ms: int = _DEFAULT_WINDOW_TTL_MS,
    local_cache_ttl: float = 0.25,
) -> RedisLatencyDigestStore:
    """Build a distributed latency digest for ``ResilienceDepsModule(latency_digest_store=...)``.

    Pass the same ``RedisClientPort`` singleton used by :class:`RedisDepsModule`; the
    executor is a process-wide singleton, so wire this at composition time.
    """

    return RedisLatencyDigestStore(
        client=client,
        namespace=namespace,
        relative_accuracy=relative_accuracy,
        window_ttl_ms=window_ttl_ms,
        local_cache_ttl=local_cache_ttl,
    )
