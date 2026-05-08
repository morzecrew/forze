from typing import TYPE_CHECKING, Protocol

from ..base import DepKey
from .ports import TenantResolverPort

if TYPE_CHECKING:
    from forze.application.execution import ExecutionContext

# ----------------------- #


class TenantResolverDepPort(Protocol):
    """Tenant resolver dependency port."""

    def __call__(self, ctx: "ExecutionContext") -> TenantResolverPort:
        """Build a tenant resolver port instance."""
        ...


# ....................... #

TenantResolverDepKey = DepKey[TenantResolverDepPort]("tenant_resolver")
"""Key used to register the :class:`TenantResolverPort` builder implementation."""
