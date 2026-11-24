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
from forze.base.exceptions import exc
from forze.base.primitives import MappingConverter, StrKey, StrKeyMapping
from forze_identity._routes import normalize_route_set as _normalize_route_set
from forze_identity._routes import refuse_undeclared_overrides

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
    """Optional per-route tenant resolver overrides (e.g. local file/env backend).

    Each key must also be named in :attr:`tenant_resolver`; any other key is refused at
    construction rather than silently registering nothing.
    """

    tenant_provisioner: TenantProvisionerPort | None = attrs.field(default=None)
    """Optional infrastructure provisioner run on ``provision_tenant`` for management routes.

    Pass a :class:`~forze.application.contracts.tenancy.CompositeTenantProvisioner` of
    per-integration provisioners (e.g. an object-storage bucket provisioner) so onboarding a
    tenant also creates its per-tenant resources.
    """

    authz_route: StrKey | None = attrs.field(default=None)
    """Authz route whose role assignment ``detach_principal`` uses: leaving a tenant first
    revokes the roles the principal holds in it. Required with :attr:`tenant_management`
    unless :attr:`detach_keeps_roles` declares otherwise."""

    detach_keeps_roles: bool = attrs.field(default=False)
    """Declare that ``detach_principal`` leaves role bindings alone, for a deployment with
    no authz plane. A principal attached again then holds whatever it held in the tenant."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        refuse_undeclared_overrides(
            self.tenant_resolvers,
            self.tenant_resolver,
            field="tenant_resolvers",
            declared_field="tenant_resolver",
        )

        if self.authz_route is not None and self.detach_keeps_roles:
            raise exc.configuration(
                "TenancyDepsModule got both authz_route and detach_keeps_roles=True: name the "
                "authz route to revoke a leaving principal's roles, or keep them, not both.",
                code="tenancy_detach_roles_ambiguous",
            )

        # No module sees both planes here, so the choice is asked of every deployment with
        # tenant management; a default would silently keep a former member's roles.
        if (
            _normalize_route_set(self.tenant_management)
            and self.authz_route is None
            and not self.detach_keeps_roles
        ):
            raise exc.configuration(
                "TenancyDepsModule registers tenant management, so detach_principal must know "
                "what happens to the roles a principal holds in the tenant it leaves. Name the "
                "authz route to revoke them (authz_route=...), or pass detach_keeps_roles=True "
                "to keep them.",
                code="tenancy_detach_roles_unset",
            )

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
                                authz_route=self.authz_route,
                            )
                            for name in tm
                        },
                    },
                ),
            )

        return merged
