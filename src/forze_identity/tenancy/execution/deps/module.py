"""Tenancy dependency module for the application kernel."""

from collections.abc import Collection
from typing import final

import attrs

from forze.application.contracts.deps import Deps, DepsModule
from forze.application.contracts.tenancy import (
    TenantManagementDepKey,
    TenantProvisionerPort,
    TenantResolverDepKey,
    TenantResolverDepPort,
)
from forze.base.primitives import MappingConverter, StrKey, StrKeyMapping
from forze_identity._routes import normalize_route_set as _normalize_route_set

from .deps import (
    ConfigurableTenantManagement,
    ConfigurableTenantResolver,
)

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class TenancyDepsModule(DepsModule):
    """Registers tenant resolver and management factories (document-backed defaults)."""

    tenant_resolver: Collection[StrKey] | None = attrs.field(default=None)
    """Route names for :class:`~forze.application.contracts.tenancy.TenantResolverDepKey`."""

    tenant_management: Collection[StrKey] | None = attrs.field(default=None)
    """Route names for :class:`~forze.application.contracts.tenancy.TenantManagementDepKey`."""

    verify_tenant_active: bool = attrs.field(default=True)
    """Forwarded to :class:`~forze_tenancy.execution.deps.deps.ConfigurableTenantResolver`."""

    tenant_resolvers: StrKeyMapping[TenantResolverDepPort] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Optional per-route tenant resolver overrides (e.g. local file/env backend)."""

    tenant_provisioner: TenantProvisionerPort | None = attrs.field(default=None)
    """Optional infrastructure provisioner run on ``provision_tenant`` for management routes.

    Pass a :class:`~forze.application.contracts.tenancy.CompositeTenantProvisioner` of
    per-integration provisioners (e.g. an object-storage bucket provisioner) so onboarding a
    tenant also creates its per-tenant resources.
    """

    # ....................... #

    def __call__(self) -> Deps:
        tr = _normalize_route_set(self.tenant_resolver)
        tm = _normalize_route_set(self.tenant_management)

        if not tr and not tm:
            return Deps()

        merged: Deps = Deps()

        if tr:
            resolver_overrides = dict(self.tenant_resolvers or {})
            default_factory = ConfigurableTenantResolver(
                verify_tenant_active=self.verify_tenant_active,
            )

            merged = merged.merge(
                Deps.routed(
                    {
                        TenantResolverDepKey: {
                            name: resolver_overrides.get(name, default_factory) for name in tr
                        },
                    },
                ),
            )

        if tm:
            merged = merged.merge(
                Deps.routed(
                    {
                        TenantManagementDepKey: {
                            name: ConfigurableTenantManagement(
                                provisioner=self.tenant_provisioner,
                            )
                            for name in tm
                        },
                    },
                ),
            )

        return merged
