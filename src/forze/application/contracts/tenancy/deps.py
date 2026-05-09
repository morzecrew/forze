from typing import TYPE_CHECKING, Protocol

from ..base import DepKey
from .ports import TenantManagementPort, TenantResolverPort

if TYPE_CHECKING:
    from forze.application.execution import ExecutionContext

# ----------------------- #


class TenantResolverDepPort(Protocol):
    """Tenant resolver dependency port."""

    def __call__(self, ctx: "ExecutionContext") -> TenantResolverPort:
        """Build a tenant resolver port instance."""
        ...


# ....................... #


class TenantManagementDepPort(Protocol):
    """Tenant management dependency port."""

    def __call__(self, ctx: "ExecutionContext") -> TenantManagementPort:
        """Build a tenant management port instance."""
        ...


# ....................... #

TenantResolverDepKey = DepKey[TenantResolverDepPort]("tenant_resolver")
"""Key used to register the :class:`TenantResolverPort` builder implementation."""

TenantManagementDepKey = DepKey[TenantManagementDepPort]("tenant_management")
"""Key used to register the :class:`TenantManagementPort` builder implementation."""
