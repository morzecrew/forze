"""The boot check for permission providers: every key they declare exists in the catalog."""

from collections.abc import Iterable
from contextlib import nullcontext
from typing import final

import attrs

from forze.application.contracts.authz import PermissionProvider
from forze.application.contracts.execution import LifecycleStep
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext
from forze.base.exceptions import exc

from ..application.specs import permission_definition_spec
from ..services.grants import fetch_all_document_hits

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class _CheckProviderKeys:
    providers: tuple[PermissionProvider, ...]
    tenant: TenantIdentity | None

    async def __call__(self, ctx: ExecutionContext) -> None:
        declared = sorted({key for provider in self.providers for key in provider.keys})

        if not declared:
            return

        binding = (
            ctx.inv_ctx.bind_identity(tenant=self.tenant)
            if self.tenant is not None
            else nullcontext()
        )

        with binding:
            rows = await fetch_all_document_hits(
                ctx.doc.query(permission_definition_spec),
                filters={"$values": {"permission_key": {"$in": declared}}},
            )

        known = {row.permission_key for row in rows}
        missing = {
            provider.name: sorted(provider.keys - known)
            for provider in self.providers
            if provider.keys - known
        }

        if missing:
            raise exc.configuration(
                f"Permission providers declare keys the permission catalog does not define: "
                f"{missing}. A typo here would deny forever; define the permissions or fix "
                "the keys.",
                code="authz_provider_unknown_keys",
            )


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

    return LifecycleStep(
        id=name,
        startup=_CheckProviderKeys(providers=tuple(providers), tenant=tenant),
    )
