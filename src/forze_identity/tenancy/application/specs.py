from forze.application.contracts.document import DocumentSpec

from ..domain.models.principal_tenant_binding import (
    CreatePrincipalTenantBindingCmd,
    PrincipalTenantBinding,
    ReadPrincipalTenantBinding,
)
from ..domain.models.tenant import (
    CreateTenantCmd,
    ReadTenant,
    Tenant,
    UpdateTenantCmd,
)
from .constants import TenancyResourceName

# ----------------------- #

tenant_spec = DocumentSpec(
    name=TenancyResourceName.TENANTS,
    read=ReadTenant,
    write={
        "domain": Tenant,
        "create_cmd": CreateTenantCmd,
        "update_cmd": UpdateTenantCmd,
    },
)

principal_tenant_binding_spec = DocumentSpec(
    name=TenancyResourceName.PRINCIPAL_TENANT_BINDINGS,
    read=ReadPrincipalTenantBinding,
    write={
        "domain": PrincipalTenantBinding,
        "create_cmd": CreatePrincipalTenantBindingCmd,
    },
)
