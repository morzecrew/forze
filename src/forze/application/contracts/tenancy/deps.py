from ..base import ConvenientDeps, DepKey, SimpleDepPort
from .ports import TenantManagementPort, TenantResolverPort

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

        return ctx.deps.provide(TenantResolverDepKey)(ctx)

    # ....................... #

    def manager(self) -> TenantManagementPort | None:
        """Resolve a tenant management port."""

        ctx = self._require_ctx()

        if not ctx.deps.exists(TenantManagementDepKey):
            return None

        return ctx.deps.provide(TenantManagementDepKey)(ctx)
