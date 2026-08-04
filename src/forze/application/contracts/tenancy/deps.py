from uuid import UUID

from forze.base.exceptions import exc
from forze.base.primitives import StrKey

from ..deps import ConvenientDeps, DepKey, SimpleDepPort
from .ports import TenantManagementPort, TenantResolverPort
from .tenant_hint import require_tenant_id
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

    def resolver(self, route: StrKey | None = None) -> TenantResolverPort | None:
        """Resolve a tenant resolver port for *route*, falling back to a plain registration.

        *route* is the tenancy route — conventionally the authn spec's name, since a
        credential and the tenancy it resolves through belong to the same profile. It is
        not optional in practice: :class:`~forze_identity.tenancy.TenancyDepsModule`
        registers this key **routed**, so a route-less lookup finds nothing and the caller
        silently behaves as if no tenancy plane were wired — no tenant bound, and every
        tenant-aware adapter then failing closed on a request that was correctly
        authenticated. Passing ``None`` keeps the plain-only lookup for wiring that
        registers it that way.
        """

        ctx = self._require_ctx()
        registered = ctx.deps.exists(TenantResolverDepKey, route=route) or (
            route is not None and ctx.deps.exists(TenantResolverDepKey)
        )

        if not registered:
            return None

        return self._resolve_simple(TenantResolverDepKey, route=route)

    # ....................... #

    def require_resolver(self, route: StrKey | None = None) -> TenantResolverPort:
        """Return the tenant resolver port, raising when none is registered.

        Raising variant of :meth:`resolver` (mirroring :meth:`require_current_id`)
        for callers that treat a missing resolver as a wiring error rather than
        a feature toggle.
        """

        resolver = self.resolver(route)

        if resolver is None:
            raise exc.configuration(
                f"Tenant resolver is not registered (no {TenantResolverDepKey.name!r} dependency)",
            )

        return resolver

    # ....................... #

    def manager(self, route: StrKey | None = None) -> TenantManagementPort | None:
        """Resolve a tenant management port for *route* — see :meth:`resolver`.

        Registered routed by ``TenancyDepsModule`` for the same reason, and route-less
        here for the same historical one.
        """

        ctx = self._require_ctx()
        registered = ctx.deps.exists(TenantManagementDepKey, route=route) or (
            route is not None and ctx.deps.exists(TenantManagementDepKey)
        )

        if not registered:
            return None

        return self._resolve_simple(TenantManagementDepKey, route=route)

    # ....................... #

    def require_manager(self, route: StrKey | None = None) -> TenantManagementPort:
        """Return the tenant management port, raising when none is registered.

        Raising variant of :meth:`manager` for callers that treat a missing manager as a
        wiring error rather than a feature toggle.
        """

        manager = self.manager(route)

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
