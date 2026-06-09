"""Reference tenancy aggregates and document-backed ports (:mod:`forze.application.contracts.tenancy`)."""

from .adapters import TenantManagementAdapter, TenantResolverAdapter
from .application.constants import TenancyResourceName
from .application.specs import principal_tenant_binding_spec, tenant_spec
from .execution.deps import (
    ConfigurableTenantManagement,
    ConfigurableTenantResolver,
    TenancyDepsModule,
)

# ----------------------- #

__all__ = [
    "ConfigurableTenantManagement",
    "ConfigurableTenantResolver",
    "TenancyDepsModule",
    "TenantManagementAdapter",
    "TenantResolverAdapter",
    "TenancyResourceName",
    "principal_tenant_binding_spec",
    "tenant_spec",
]
