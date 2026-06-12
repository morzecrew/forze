from datetime import timedelta
from typing import Any, Callable, final

import attrs

from forze.base.exceptions import exc

from ..base import BaseSpec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class L1Spec:
    """Opt-in in-process L1 ahead of the distributed document cache.

    Hot-document reads are served from process memory — decoded, no transport
    round-trip and no JSON decode — instead of hitting the cache backend.

    **This is a consistency contract change.** Writes invalidate the L1 only
    on the replica that performed them; other replicas serve their L1 entry
    until :attr:`ttl` expires. The TTL is therefore the **cross-replica
    staleness budget** — keep it small, and enable L1 only on read models
    that tolerate reads that stale. Same-replica read-your-writes is
    preserved (local writes refresh or invalidate the local L1).
    """

    ttl: timedelta
    """Maximum cross-replica staleness; entries expire after this. Must be
    strictly smaller than the owning :attr:`CacheSpec.ttl` so the backend
    cache still sees periodic reads (keeping early refresh functional)."""

    capacity: int = 1024
    """Maximum entries held in process memory (LRU-evicted beyond this)."""

    store_factory: "Callable[[L1Spec], Any] | None" = None
    """Eviction-policy seam: build a custom L1 store from this spec.

    The callable receives this spec and returns an object satisfying the
    integration-layer ``L1Store`` protocol (sync ``get``/``set``/
    ``invalidate``/``clear``). ``None`` (default) keeps the built-in LRU+TTL
    store. The in-box scan-resistant alternative is W-TinyLFU::

        from forze.application.integrations.document import tiny_lfu_l1_store

        L1Spec(ttl=..., store_factory=tiny_lfu_l1_store)
    """

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.ttl.total_seconds() <= 0:
            raise exc.configuration("L1 TTL must be positive")

        if self.capacity < 1:
            raise exc.configuration("L1 capacity must be >= 1")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class AgeBasedTtl:
    """Age-proportional entry lifetime (the HTTP heuristic-freshness rule).

    RFC 7234 §4.2.2, the rule every HTTP cache ships: an object's freshness
    lifetime is a fraction of its age since last modification — long-stable
    documents earn long cache lifetimes, recently-changed ones get short ones
    (write locality: what changed a minute ago will likely change again).
    At warm time the document coordinator computes
    ``ttl = clamp(alpha × (now − last_update_at), min_ttl, max_ttl)`` and
    writes the entry with that per-entry lifetime. Freshness for in-band
    writes is unaffected either way — write-invalidation handles it; this
    governs the out-of-band safety net and the revalidation cadence.
    """

    alpha: float = 0.1
    """Fraction of the document's age used as its lifetime (HTTP's 10%)."""

    min_ttl: timedelta = timedelta(seconds=30)
    """Floor — recently-changed documents revalidate at least this often."""

    max_ttl: timedelta = timedelta(hours=1)
    """Cap — even ancient documents revalidate within this bound."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.alpha <= 0:
            raise exc.configuration("Age-based TTL alpha must be positive")

        if self.min_ttl.total_seconds() <= 0:
            raise exc.configuration("Age-based TTL min_ttl must be positive")

        if self.min_ttl > self.max_ttl:
            raise exc.configuration("Age-based TTL min_ttl must be <= max_ttl")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class CacheSpec(BaseSpec):
    """Cache specification."""

    ttl: timedelta = timedelta(seconds=300)
    """Default TTL for cache entries."""

    ttl_pointer: timedelta = timedelta(seconds=60)
    """TTL for the cache pointers (when using versioned cache)."""

    early_refresh_beta: float | None = None
    """Opt-in probabilistic early refresh (XFetch) for document read-through.

    When set (typical ``1.0``), a cache hit may volunteer to recompute *before*
    expiry with probability rising as expiry nears, scaled by the entry's
    observed recompute cost — so refreshes desynchronize across replicas and a
    hot key never expires for everyone at once (Vattani et al., "Optimal
    Probabilistic Cache Stampede Prevention"). Entries gain a small metadata
    envelope; ``None`` (default) keeps the payload format byte-identical.
    Higher values refresh earlier/more often."""

    early_refresh_background: bool = False
    """Run elected early refreshes in the background instead of inline.

    By default the XFetch-elected reader pays the recompute latency. When
    enabled, the elected read serves the still-valid cached entry immediately
    and the refresh runs as a detached task — the reader never sees the
    refresh latency, and refresh *failures* become logged-only (the entry is
    still valid; a later election retries). Requires
    :attr:`early_refresh_beta`."""

    l1: L1Spec | None = None
    """Opt-in in-process L1 for document read-through (see :class:`L1Spec`).
    ``None`` (default) keeps every read on the backend cache."""

    sliding_ttl: timedelta | None = None
    """Opt-in sliding expiration (expire-after-access) for versioned entries.

    When set, a backend hit extends the entry's *pointer* lifetime to this
    idle window (extend-only, never shortened) — a hot entry stays cached for
    as long as it keeps being read instead of dying of TTL mid-heat, and a
    cold one expires within one quiet window of its last access. Seasonality
    and time-of-day patterns are handled by construction: nothing is
    predicted, the entry simply lives while in season. The entry's *body*
    TTL is never extended, so :attr:`ttl` (or the per-entry age-based
    lifetime) remains the **absolute revalidation cap** — a perpetually-hot
    entry still re-checks the source within that bound. Must be smaller than
    :attr:`ttl`."""

    age_ttl: AgeBasedTtl | None = None
    """Opt-in age-proportional per-entry lifetime (see :class:`AgeBasedTtl`).
    ``None`` (default) keeps the fixed :attr:`ttl`/:attr:`ttl_pointer`."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.ttl.total_seconds() <= 0:
            raise exc.configuration("TTL must be positive")

        if self.ttl_pointer.total_seconds() <= 0:
            raise exc.configuration("TTL pointer must be positive")

        if self.early_refresh_beta is not None and self.early_refresh_beta <= 0:
            raise exc.configuration("Early refresh beta must be positive")

        if self.early_refresh_background and self.early_refresh_beta is None:
            raise exc.configuration(
                "early_refresh_background requires early_refresh_beta — it "
                "only changes how an elected refresh runs",
            )

        if self.l1 is not None and self.l1.ttl >= self.ttl:
            raise exc.configuration(
                "L1 TTL must be strictly smaller than the cache TTL — the "
                "backend cache must keep seeing periodic reads",
            )

        if self.sliding_ttl is not None:
            if self.sliding_ttl.total_seconds() <= 0:
                raise exc.configuration("Sliding TTL must be positive")

            if self.sliding_ttl >= self.ttl:
                raise exc.configuration(
                    "Sliding TTL must be smaller than the cache TTL — the "
                    "body TTL is the absolute revalidation cap sliding "
                    "expiration is bounded by",
                )
