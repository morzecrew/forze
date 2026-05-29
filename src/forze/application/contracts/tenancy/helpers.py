"""Helpers for tenant identity in routed infrastructure clients."""

from typing import Awaitable, Callable, Sequence
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


async def ensure_dsn_fingerprint(
    get_fingerprint: Callable[[UUID], str | None],
    set_fingerprint: Callable[[UUID, str], None],
    *,
    tenant_id: UUID,
    secrets: SecretsPort,
    ref_for_tenant: Callable[[UUID], SecretRef],
    backend: str,
    extra_parts: Sequence[str] = (),
) -> str:
    """Resolve DSN once, compute slot fingerprint, cache on tenant id."""

    cached = get_fingerprint(tenant_id)

    if cached is not None:
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
    set_fingerprint(tenant_id, fp)

    return fp


# ....................... #


async def resolve_dsn_for_tenant(
    *,
    tenant_id: UUID,
    secrets: SecretsPort,
    ref_for_tenant: Callable[[UUID], SecretRef],
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
    ref_for_tenant: Callable[[UUID], SecretRef],
) -> T:
    """Resolve structured credentials for *tenant_id*, wrapping unexpected errors."""

    ref = secret_ref_for_tenant(ref_for_tenant, tenant_id)

    return await resolve_structured(
        secrets,
        ref,
        creds_type,
    )


# ....................... #


async def ensure_structured_fingerprint(
    get_fingerprint: Callable[[UUID], str | None],
    set_fingerprint: Callable[[UUID, str], None],
    *,
    tenant_id: UUID,
    fingerprint: Callable[[], Awaitable[str]],
) -> str:
    """Compute and cache structured fingerprint for *tenant_id*."""

    cached = get_fingerprint(tenant_id)

    if cached is not None:
        return cached

    fp = await fingerprint()
    set_fingerprint(tenant_id, fp)

    return fp
