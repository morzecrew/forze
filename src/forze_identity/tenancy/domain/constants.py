from enum import StrEnum

# ----------------------- #


class TenancyResourceName(StrEnum):
    """Document resource names for tenancy reference models."""

    TENANTS = "tenancy_tenants"
    PRINCIPAL_TENANT_BINDINGS = "tenancy_principal_tenant_bindings"
