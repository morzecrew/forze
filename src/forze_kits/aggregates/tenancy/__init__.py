"""Tenant-selector self-service: list memberships and switch the active tenant."""

from .dto import (
    TenantLeaveRequestDTO,
    TenantListDTO,
    TenantListItemDTO,
    TenantSwitchRequestDTO,
)
from .factories import build_tenancy_registry
from .handlers import LeaveTenant, ListTenants, SwitchTenant
from .operations import TenancyKernelOp

# ----------------------- #

__all__ = [
    "TenancyKernelOp",
    "build_tenancy_registry",
    "ListTenants",
    "SwitchTenant",
    "LeaveTenant",
    "TenantListDTO",
    "TenantListItemDTO",
    "TenantSwitchRequestDTO",
    "TenantLeaveRequestDTO",
]
