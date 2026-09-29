"""The boot check for permission providers: every key they declare exists in the catalog."""

from collections.abc import Iterable
from contextlib import nullcontext
from typing import Final, final

import attrs

from forze.application.contracts.authz import PermissionProvider
from forze.application.contracts.execution import LifecycleStep
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext
from forze.base.exceptions import exc

from ..application.specs import permission_definition_spec
from ..services.grants import fetch_all_document_hits
from .deps.configs import check_permission_providers

# ----------------------- #


_IN_BATCH: Final = 1_000
"""The query parser's default ``$in`` limit."""


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

        known: set[str] = set()
        query = ctx.doc.query(permission_definition_spec)

        with binding:
            # In batches: a query may name at most _IN_BATCH values in one $in.
            for first in range(0, len(declared), _IN_BATCH):
                rows = await fetch_all_document_hits(
                    query,
                    filters={
                        "$values": {"permission_key": {"$in": declared[first : first + _IN_BATCH]}}
                    },
                )
                known |= {row.permission_key for row in rows}

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

    declared = tuple(providers)
    check_permission_providers(declared)

    return LifecycleStep(
        id=name,
        startup=_CheckProviderKeys(providers=declared, tenant=tenant),
    )
