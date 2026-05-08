from uuid import UUID

import attrs

from forze.base.errors import CoreError

from .ports import TenantProviderPort

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TenancyMixin:
    """Mixin to handle multi-tenancy."""

    tenant_aware: bool = False
    """Whether tenant ID is required for the class."""

    tenant_provider: TenantProviderPort | None = attrs.field(default=None)
    """Callable to provide the tenant ID."""

    # ....................... #

    def require_tenant_if_aware(self) -> UUID | None:
        if not self.tenant_aware:
            return None

        if self.tenant_provider is None:
            raise CoreError("Tenant provider is required")

        tenant = self.tenant_provider()

        if tenant is None:
            raise CoreError("Tenant ID is required")

        return tenant.tenant_id
