"""Tenant-identity hints and current-tenant extraction.

Non-authoritative tenant hints (request header / token issuer) parse into a requested
tenant id, and the current authenticated tenant is read from a provider — softly
(:func:`soft_tenant_id`) or fail-closed (:func:`require_tenant_id`).
"""

from typing import Callable
from uuid import UUID

from forze.base.exceptions import exc

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
    """Return the current tenant id from *provider*, or raise :class:`exc.authentication`.

    A missing bound tenant is a **caller-caused** condition — the invocation context carries
    no tenant identity — not a server fault, so it egresses as an authentication failure
    (401-class), matching the ``TenantRequired`` before-hook and the adapter-level
    :meth:`~forze.application.contracts.tenancy.TenancyMixin.require_tenant_if_aware` guard
    (same ``tenant_required`` code). Raising ``internal`` here would surface a 500 for what
    is really an unauthenticated / tenant-less request.
    """

    value = provider()

    if value is None:
        raise exc.authentication(message, code=code)

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
