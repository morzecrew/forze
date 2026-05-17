"""Stable capability key strings for capability-driven usecase scheduling.

These keys label runtime facts produced or consumed by guards and effects within
a single plan bucket. They are intentionally plain strings so plans and tests can
compose them without importing heavy domain types.
"""

from typing import Final

# ----------------------- #

AUTHN_PRINCIPAL: Final[str] = "authn.principal"
"""Authenticated principal is available for downstream authorization."""

TENANCY_TENANT: Final[str] = "tenancy.tenant"
"""Active tenant context is available."""

AUTHZ_PERMITS_PREFIX: str = "authz.permits:"
"""Prefix for permission-scoped keys; use :func:`authz_permits_capability`."""


def authz_permits_capability(permission_key: str) -> str:
    """Return a capability key for a successful ``permits`` decision."""

    return f"{AUTHZ_PERMITS_PREFIX}{permission_key}"
