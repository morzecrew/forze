"""The boot check for permission providers: every key they declare exists in the catalog, and no
key the configuration owns is also granted through it."""

from collections.abc import Iterable
from contextlib import nullcontext
from typing import final

import attrs

from forze.application.contracts.authz import PermissionProvider
from forze.application.contracts.execution import LifecycleStep
from forze.application.contracts.tenancy import TenantIdentity, require_tenant_global
from forze.application.execution import ExecutionContext
from forze.application.integrations.authz import (
    ConfigGrantsProvider,
    check_permission_providers,
)

from ..application.specs import (
    group_permission_binding_spec,
    permission_definition_spec,
    principal_permission_binding_spec,
    role_permission_binding_spec,
)
from ..services.grants import check_provider_catalog

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class _CheckProviderKeys:
    providers: tuple[PermissionProvider, ...]
    tenant: TenantIdentity | None

    async def __call__(self, ctx: ExecutionContext) -> None:
        binding = (
            ctx.inv_ctx.bind_identity(tenant=self.tenant)
            if self.tenant is not None
            else nullcontext()
        )

        with binding:
            query = ctx.doc.query(permission_definition_spec)
            bindings = (
                ctx.doc.query(role_permission_binding_spec),
                ctx.doc.query(principal_permission_binding_spec),
                ctx.doc.query(group_permission_binding_spec),
            )

            if self.tenant is None:
                # Startup binds no tenant of its own, so each port the check reads must not
                # need one: the catalog when a provider declares keys, the bindings when a
                # config provider owns some.
                what = "The permission providers check"
                read: list[object] = [query] if any(p.keys for p in self.providers) else []

                if any(isinstance(p, ConfigGrantsProvider) and p.keys for p in self.providers):
                    read.extend(bindings)

                for port in read:
                    require_tenant_global(port, what=what)

            await check_provider_catalog(query, bindings, self.providers)


def permission_providers_lifecycle_step(
    providers: Iterable[PermissionProvider],
    *,
    tenant: TenantIdentity | None = None,
    name: str = "authz_permission_providers",
) -> LifecycleStep:
    """A startup step refusing to boot when a provider declares a key the catalog lacks, or when
    a key a :class:`~forze_identity.authz.ConfigGrantsProvider` declares is also granted through
    the catalog.

    Pass the providers given to ``AuthzKernelConfig(permission_providers=...)``. When the
    permission catalog is tenant-scoped, *tenant* is the tenant it is read under; without
    one, a tenant-scoped catalog fails startup with a ``configuration`` error
    (``tenant_global_required``).
    """

    declared = tuple(providers)
    check_permission_providers(declared)

    return LifecycleStep(
        id=name,
        startup=_CheckProviderKeys(providers=declared, tenant=tenant),
    )
