"""Helpers for tenant identity in routed infrastructure clients."""

from collections.abc import Callable
from uuid import UUID

from forze.base.exceptions import exc

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
