from .constants import TENANT_ID_FIELD
from .deps import (
    TenancyDeps,
    TenantManagementDepKey,
    TenantManagementDepPort,
    TenantResolverDepKey,
    TenantResolverDepPort,
)
from .fingerprint import ensure_dsn_fingerprint, ensure_structured_fingerprint
from .integration_config import TenantAwareIntegrationConfig
from .mixins import TenancyMixin
from .ports import TenantManagementPort, TenantProviderPort, TenantResolverPort
from .provisioning import (
    CompositeTenantProvisioner,
    FunctionTenantProvisioner,
    NoopTenantProvisioner,
    TenantProvisionerPort,
)
from .registry import TenantClientRegistry, TenantPoolStats
from .tenant_hint import (
    TENANT_ID_HEADER,
    coalesce_tenant_request_hints,
    parse_tenant_hint,
    require_tenant_id,
    soft_tenant_id,
)
from .value_objects import TenantIdentity
from .wiring import (
    IntegrationRouteWarning,
    TenancyRouteGroup,
    TenancyRouteSpec,
    TenantIsolationMode,
    derive_tenant_isolation_mode,
    isolation_satisfies,
    namespace_route_warning,
    validate_module_tenancy,
    validate_required_isolation,
    validate_routed_client_tenancy_wiring,
    warn_dynamic_relation_with_tenant_aware,
    warn_integration_routes,
)

# ----------------------- #

__all__ = [
    "TENANT_ID_FIELD",
    "TenantIdentity",
    "TenantManagementPort",
    "TenancyDeps",
    "TenantManagementDepKey",
    "TenantManagementDepPort",
    "TenantResolverPort",
    "TenantResolverDepKey",
    "TenantResolverDepPort",
    "TenantProviderPort",
    "TenantProvisionerPort",
    "NoopTenantProvisioner",
    "FunctionTenantProvisioner",
    "CompositeTenantProvisioner",
    "TenantAwareIntegrationConfig",
    "TenancyMixin",
    "TENANT_ID_HEADER",
    "coalesce_tenant_request_hints",
    "parse_tenant_hint",
    "require_tenant_id",
    "TenancyRouteSpec",
    "TenancyRouteGroup",
    "IntegrationRouteWarning",
    "TenantIsolationMode",
    "derive_tenant_isolation_mode",
    "isolation_satisfies",
    "validate_required_isolation",
    "validate_module_tenancy",
    "namespace_route_warning",
    "validate_routed_client_tenancy_wiring",
    "warn_dynamic_relation_with_tenant_aware",
    "warn_integration_routes",
    "TenantClientRegistry",
    "TenantPoolStats",
    "ensure_dsn_fingerprint",
    "ensure_structured_fingerprint",
    "soft_tenant_id",
]
