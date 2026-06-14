from .constants import TENANT_ID_FIELD
from .deps import (
    TenancyDeps,
    TenantManagementDepKey,
    TenantManagementDepPort,
    TenantResolverDepKey,
    TenantResolverDepPort,
)
from .helpers import (
    TENANT_ID_HEADER,
    coalesce_tenant_request_hints,
    ensure_dsn_fingerprint,
    ensure_structured_fingerprint,
    parse_tenant_hint,
    require_tenant_id,
    resolve_dsn_for_tenant,
    resolve_structured_for_tenant,
    soft_tenant_id,
)
from .integration_config import TenantAwareIntegrationConfig
from .mixins import TenancyMixin
from .ports import TenantManagementPort, TenantProviderPort, TenantResolverPort
from .registry import TenantClientRegistry, TenantPoolStats
from .value_objects import TenantIdentity
from .wiring import (
    IntegrationRouteWarning,
    TenancyRouteSpec,
    INTEGRATION_ISOLATION_CEILINGS,
    TenancyRouteGroup,
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
    "TenantAwareIntegrationConfig",
    "TenancyMixin",
    "TENANT_ID_HEADER",
    "coalesce_tenant_request_hints",
    "parse_tenant_hint",
    "require_tenant_id",
    "TenancyRouteSpec",
    "TenancyRouteGroup",
    "INTEGRATION_ISOLATION_CEILINGS",
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
    "resolve_dsn_for_tenant",
    "resolve_structured_for_tenant",
    "ensure_structured_fingerprint",
    "soft_tenant_id",
]
