from typing import Callable
from uuid import UUID

import attrs
from pydantic import Field

from forze.base.errors import CoreError
from forze.domain.models import CoreModel

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class MultiTenancyMixin:
    """Mixin to handle multi-tenancy."""

    tenant_aware: bool = False
    """Whether tenant ID is required for the class."""

    tenant_provider: Callable[[], UUID | None] | None = attrs.field(default=None)
    """Callable to provide the tenant ID."""

    # ....................... #

    def require_tenant_if_aware(self) -> UUID | None:
        if not self.tenant_aware:
            return None

        if self.tenant_provider is None:
            raise CoreError("Tenant provider is required")

        tenant_id = self.tenant_provider()

        if tenant_id is None:
            raise CoreError("Tenant ID is required")

        return tenant_id


# ....................... #


class DomainTenantIdMixin(CoreModel):
    """Mixin adding an optional tenant ID field."""

    tenant_id: UUID | None = Field(default=None, frozen=True)
    """Tenant ID."""
