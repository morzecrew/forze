from uuid import UUID

from ..deps import ConvenientDeps, DepKey, SimpleDepPort
from .helpers import require_tenant_id
from .ports import TenantManagementPort, TenantResolverPort
from .value_objects import TenantIdentity

# ----------------------- #

TenantResolverDepPort = SimpleDepPort[TenantResolverPort]
"""Tenant resolver dependency port."""

TenantManagementDepPort = SimpleDepPort[TenantManagementPort]
"""Tenant management dependency port."""

# ....................... #

TenantResolverDepKey = DepKey[TenantResolverDepPort]("tenant_resolver")
"""Key used to register the :class:`TenantResolverPort` builder implementation."""

TenantManagementDepKey = DepKey[TenantManagementDepPort]("tenant_management")
"""Key used to register the :class:`TenantManagementPort` builder implementation."""

# ....................... #


class TenancyDeps(ConvenientDeps):
    """Convenience wrapper for tenacy dependencies."""

    def resolver(self) -> TenantResolverPort | None:
        """Resolve a tenant resolver port."""

        ctx = self._require_ctx()

        if not ctx.deps.exists(TenantResolverDepKey):
            return None

        return self._resolve_simple(TenantResolverDepKey)

    # ....................... #

    def manager(self) -> TenantManagementPort | None:
        """Resolve a tenant management port."""

        ctx = self._require_ctx()

        if not ctx.deps.exists(TenantManagementDepKey):
            return None

        return self._resolve_simple(TenantManagementDepKey)

    # ....................... #

    def current(self) -> TenantIdentity | None:
        """Return the current tenant identity, if any."""

        return self._require_ctx().inv_ctx.get_tenant()

    # ....................... #

    def require_current_id(self) -> UUID:
        """Return the current tenant id, raising if no tenant is bound.

        For manual scopers — a raw client-port caller (``PostgresClientPort`` etc.) that
        owns its own tenant filtering scopes its query with this instead of reaching into
        ``inv_ctx``.
        """

        return require_tenant_id(
            self._require_ctx().inv_ctx.get_tenant,
            message="Tenant ID is required",
        )
