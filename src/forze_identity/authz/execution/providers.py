"""The boot check for permission providers: every key they declare exists in the catalog, and no
key the configuration owns is also granted through it."""

from collections.abc import Iterable
from contextlib import nullcontext
from typing import final

import attrs

from forze.application.contracts.authz import PermissionProvider
from forze.application.contracts.execution import LifecycleStep
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext
from forze.application.integrations.authz import check_permission_providers

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
            await check_provider_catalog(
                ctx.doc.query(permission_definition_spec),
                (
                    ctx.doc.query(role_permission_binding_spec),
                    ctx.doc.query(principal_permission_binding_spec),
                    ctx.doc.query(group_permission_binding_spec),
                ),
                self.providers,
            )


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
    permission catalog is tenant-scoped, *tenant* is the tenant it is read under.
    """

    declared = tuple(providers)
    check_permission_providers(declared)

    return LifecycleStep(
        id=name,
        startup=_CheckProviderKeys(providers=declared, tenant=tenant),
    )
