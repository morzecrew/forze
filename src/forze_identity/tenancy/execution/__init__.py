"""Execution wiring for tenancy (deps module and configurable factories)."""

from .deps import (
    ConfigurableTenantManagement,
    ConfigurableTenantResolver,
    TenancyDepsModule,
)

# ----------------------- #

__all__ = [
    "ConfigurableTenantManagement",
    "ConfigurableTenantResolver",
    "TenancyDepsModule",
]
