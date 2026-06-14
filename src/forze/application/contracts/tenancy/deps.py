from uuid import UUID

from forze.base.exceptions import exc

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

    def require_resolver(self) -> TenantResolverPort:
        """Return the tenant resolver port, raising when none is registered.

        Raising variant of :meth:`resolver` (mirroring :meth:`require_current_id`)
        for callers that treat a missing resolver as a wiring error rather than
        a feature toggle.
        """

        resolver = self.resolver()

        if resolver is None:
            raise exc.configuration(
                "Tenant resolver is not registered "
                f"(no {TenantResolverDepKey.name!r} dependency)",
            )

        return resolver

    # ....................... #

    def manager(self) -> TenantManagementPort | None:
        """Resolve a tenant management port."""

        ctx = self._require_ctx()

        if not ctx.deps.exists(TenantManagementDepKey):
            return None

        return self._resolve_simple(TenantManagementDepKey)

    # ....................... #

    def require_manager(self) -> TenantManagementPort:
        """Return the tenant management port, raising when none is registered.

        Raising variant of :meth:`manager` for callers that treat a missing manager as a
        wiring error rather than a feature toggle.
        """

        manager = self.manager()

        if manager is None:
            raise exc.configuration(
                "Tenant management is not registered "
                f"(no {TenantManagementDepKey.name!r} dependency)",
            )

        return manager

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
