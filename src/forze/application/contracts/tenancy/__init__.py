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
)
from .integration_config import TenantAwareIntegrationConfig
from .mixins import TenancyMixin
from .ports import TenantManagementPort, TenantProviderPort, TenantResolverPort
from .registry import TenantClientRegistry
from .value_objects import TenantIdentity
from .wiring import (
    TenancyRouteSpec,
    TenantIsolationMode,
    derive_tenant_isolation_mode,
    validate_routed_client_tenancy_wiring,
    warn_dynamic_relation_with_tenant_aware,
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
    "TenantIsolationMode",
    "derive_tenant_isolation_mode",
    "validate_routed_client_tenancy_wiring",
    "warn_dynamic_relation_with_tenant_aware",
    "TenantClientRegistry",
    "ensure_dsn_fingerprint",
    "resolve_dsn_for_tenant",
    "resolve_structured_for_tenant",
    "ensure_structured_fingerprint",
]
