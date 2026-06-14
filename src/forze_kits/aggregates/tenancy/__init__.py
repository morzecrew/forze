"""Tenant-selector self-service: list memberships and switch the active tenant."""

from .dto import TenantListDTO, TenantListItemDTO, TenantSwitchRequestDTO
from .factories import build_tenancy_registry
from .handlers import ListTenants, SwitchTenant
from .operations import TenancyKernelOp

# ----------------------- #

__all__ = [
    "TenancyKernelOp",
    "build_tenancy_registry",
    "ListTenants",
    "SwitchTenant",
    "TenantListDTO",
    "TenantListItemDTO",
    "TenantSwitchRequestDTO",
]
