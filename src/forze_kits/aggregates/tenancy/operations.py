"""Tenancy self-service operation kernel suffixes (the tenant selector)."""

from enum import StrEnum
from typing import final

# ----------------------- #


@final
class TenancyKernelOp(StrEnum):
    """Kernel segments (suffix only) for tenancy self-service usecase keys."""

    LIST_TENANTS = "list_tenants"
    """List the authenticated principal's active tenant memberships (the selector)."""

    SWITCH_TENANT = "switch_tenant"
    """Activate one of the principal's tenants — re-mint a token scoped to it."""
