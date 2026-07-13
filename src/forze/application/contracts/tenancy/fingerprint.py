"""Per-tenant access-fingerprint refresh for routed client pools.

Compute and cache a tenant's credential fingerprint (the pool's slot-dedup key) on
tenant id, with optional TTL-driven re-resolution that evicts a stale pooled client
when the fingerprint changes. The DSN path resolves via a :class:`TenantSecretResolver`;
the structured path takes a caller-supplied fingerprint callable.
"""

from collections.abc import Awaitable, Callable, Sequence
from uuid import UUID

from forze.base.primitives.fingerprint import (
    connection_string_fingerprint,
    stable_fingerprint,
)

from ..secrets import TenantSecretResolver

# ----------------------- #


async def ensure_dsn_fingerprint(
    get_fingerprint: Callable[[UUID], str | None],
    set_fingerprint: Callable[[UUID, str], None],
    *,
    tenant_id: UUID,
    resolver: TenantSecretResolver,
    extra_parts: Sequence[str] = (),
    is_expired: Callable[[UUID], bool] | None = None,
    on_change: Callable[[UUID], Awaitable[None]] | None = None,
) -> str:
    """Resolve DSN once via *resolver*, compute slot fingerprint, cache on tenant id.

    When *is_expired* reports the cached fingerprint stale (optional TTL refresh), the
    DSN is re-resolved; if the fingerprint changed, *on_change* is awaited (to evict the
    now-stale pooled client) before the new fingerprint is cached.
    """

    cached = get_fingerprint(tenant_id)

    if cached is not None and (is_expired is None or not is_expired(tenant_id)):
        return cached

    dsn = await resolver.resolve_str(tenant_id)
    fp = stable_fingerprint(
        connection_string_fingerprint(dsn),
        *extra_parts,
    )

    if cached is not None and fp != cached and on_change is not None:
        await on_change(tenant_id)

    set_fingerprint(tenant_id, fp)

    return fp


# ....................... #


async def ensure_structured_fingerprint(
    get_fingerprint: Callable[[UUID], str | None],
    set_fingerprint: Callable[[UUID, str], None],
    *,
    tenant_id: UUID,
    fingerprint: Callable[[], Awaitable[str]],
    is_expired: Callable[[UUID], bool] | None = None,
    on_change: Callable[[UUID], Awaitable[None]] | None = None,
) -> str:
    """Compute and cache structured fingerprint for *tenant_id*.

    When *is_expired* reports the cached fingerprint stale (optional TTL refresh), it is
    recomputed; if it changed, *on_change* is awaited (to evict the now-stale pooled
    client) before the new fingerprint is cached.
    """

    cached = get_fingerprint(tenant_id)

    if cached is not None and (is_expired is None or not is_expired(tenant_id)):
        return cached

    fp = await fingerprint()

    if cached is not None and fp != cached and on_change is not None:
        await on_change(tenant_id)

    set_fingerprint(tenant_id, fp)

    return fp
