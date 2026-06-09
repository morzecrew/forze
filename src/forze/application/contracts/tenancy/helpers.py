"""Helpers for tenant identity in routed infrastructure clients."""

from typing import Awaitable, Callable, Mapping, Sequence
from uuid import UUID

from pydantic import BaseModel

from forze.base.exceptions import exc
from forze.base.primitives.fingerprint import (
    connection_string_fingerprint,
    stable_fingerprint,
)

from ..secrets import (
    SecretRef,
    SecretsPort,
    resolve_str_for_tenant,
    resolve_structured,
    secret_ref_for_tenant,
)
from .value_objects import TenantIdentity

# ----------------------- #

TENANT_ID_HEADER = "X-Tenant-Id"
"""HTTP header carrying an optional tenant id hint (UUID string)."""


def parse_tenant_hint(raw: str | None) -> UUID | None:
    """Parse a non-authoritative tenant hint string as a UUID, or return ``None``."""

    if raw is None:
        return None

    stripped = raw.strip()

    if not stripped:
        return None

    try:
        return UUID(stripped)

    except ValueError:
        return None


def coalesce_tenant_request_hints(
    *,
    issuer_hint: str | None = None,
    header_hint: str | None = None,
) -> UUID | None:
    """Coalesce issuer and header tenant hints into a single requested tenant id.

    Issuer hint takes precedence over the header. When both parse as UUIDs and
    differ, raises :class:`exc.authentication` with code ``tenant_conflict``.
    """

    from_issuer = parse_tenant_hint(issuer_hint)
    from_header = parse_tenant_hint(header_hint)

    if from_issuer is not None and from_header is not None and from_issuer != from_header:
        raise exc.authentication(
            "Conflicting tenant identities from credential and request hint",
            code="tenant_conflict",
        )

    return from_issuer or from_header


# ....................... #


def require_tenant_id(
    provider: Callable[[], UUID | TenantIdentity | None],
    *,
    message: str,
    code: str = "tenant_required",
) -> UUID:
    """Return the current tenant id from *provider* or raise :class:`exc.internal`."""

    value = provider()

    if value is None:
        raise exc.internal(message, code=code)

    if isinstance(value, TenantIdentity):
        return value.tenant_id

    return value


# ....................... #


def soft_tenant_id(
    provider: Callable[[], TenantIdentity | None] | None,
) -> UUID | None:
    """Return the current tenant id, or ``None`` (never raises).

    The soft counterpart to :func:`require_tenant_id`, for adapters that resolve
    tenant context opportunistically (e.g. analytics ingest-target resolution).
    """

    if provider is None:
        return None

    tenant = provider()

    return tenant.tenant_id if tenant is not None else None


# ....................... #


async def ensure_dsn_fingerprint(
    get_fingerprint: Callable[[UUID], str | None],
    set_fingerprint: Callable[[UUID, str], None],
    *,
    tenant_id: UUID,
    secrets: SecretsPort,
    ref_for_tenant: Callable[[UUID], SecretRef] | Mapping[UUID, SecretRef],
    backend: str,
    extra_parts: Sequence[str] = (),
    is_expired: Callable[[UUID], bool] | None = None,
    on_change: Callable[[UUID], Awaitable[None]] | None = None,
) -> str:
    """Resolve DSN once, compute slot fingerprint, cache on tenant id.

    When *is_expired* reports the cached fingerprint stale (optional TTL refresh), the
    DSN is re-resolved; if the fingerprint changed, *on_change* is awaited (to evict the
    now-stale pooled client) before the new fingerprint is cached.
    """

    cached = get_fingerprint(tenant_id)

    if cached is not None and (is_expired is None or not is_expired(tenant_id)):
        return cached

    ref = secret_ref_for_tenant(ref_for_tenant, tenant_id)

    dsn = await resolve_str_for_tenant(
        secrets,
        ref,
        tenant_id=tenant_id,
        backend=backend,
    )
    fp = stable_fingerprint(
        connection_string_fingerprint(dsn),
        *extra_parts,
    )

    if cached is not None and fp != cached and on_change is not None:
        await on_change(tenant_id)

    set_fingerprint(tenant_id, fp)

    return fp


# ....................... #


async def resolve_dsn_for_tenant(
    *,
    tenant_id: UUID,
    secrets: SecretsPort,
    ref_for_tenant: Callable[[UUID], SecretRef] | Mapping[UUID, SecretRef],
    backend: str,
) -> str:
    """Resolve DSN for *tenant_id*, wrapping unexpected errors."""

    ref = secret_ref_for_tenant(ref_for_tenant, tenant_id)

    return await resolve_str_for_tenant(
        secrets,
        ref,
        tenant_id=tenant_id,
        backend=backend,
    )


# ....................... #


async def resolve_structured_for_tenant[T: BaseModel](
    creds_type: type[T],
    *,
    tenant_id: UUID,
    secrets: SecretsPort,
    ref_for_tenant: Callable[[UUID], SecretRef] | Mapping[UUID, SecretRef],
    backend: str,
) -> T:
    """Resolve structured credentials for *tenant_id*, wrapping unexpected errors."""

    ref = secret_ref_for_tenant(ref_for_tenant, tenant_id)

    try:
        return await resolve_structured(
            secrets,
            ref,
            creds_type,
        )

    except exc:
        raise

    except Exception as e:
        raise exc.internal(
            f"Failed to resolve {backend} secret for tenant {tenant_id}: {e}",
        ) from e


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
