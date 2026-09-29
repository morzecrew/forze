"""The boot check for permission providers: every key they declare exists in the catalog."""

from collections.abc import Iterable
from contextlib import nullcontext
from typing import final

import attrs

from forze.application.contracts.authz import PermissionProvider
from forze.application.contracts.execution import LifecycleStep
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext

from ..application.specs import permission_definition_spec
from ..services.grants import check_declared_keys
from .deps.configs import check_permission_providers

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
            await check_declared_keys(ctx.doc.query(permission_definition_spec), self.providers)


def permission_providers_lifecycle_step(
    providers: Iterable[PermissionProvider],
    *,
    tenant: TenantIdentity | None = None,
    name: str = "authz_permission_providers",
) -> LifecycleStep:
    """A startup step refusing to boot when a provider declares a key the catalog lacks.

    Pass the providers given to ``AuthzKernelConfig(permission_providers=...)``. When the
    permission catalog is tenant-scoped, *tenant* is the tenant it is read under.
    """

    declared = tuple(providers)
    check_permission_providers(declared)

    return LifecycleStep(
        id=name,
        startup=_CheckProviderKeys(providers=declared, tenant=tenant),
    )
