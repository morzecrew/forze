"""Shared tenancy-tier validation for object-storage deps modules (S3 / GCS).

Object stores support the full isolation ladder: a per-tenant path prefix (``row``), a
per-tenant *bucket* resolver (``schema``), and a routed per-tenant client / credentials
(``database``). This derives the effective tier from the config an S3/GCS module already
carries and enforces a declared ``required_tenant_isolation`` floor.
"""

from typing import Any, Callable, Mapping, Protocol

from forze.application.contracts.resolution import is_static_named_resource
from forze.application.contracts.tenancy import (
    TenancyRouteSpec,
    TenantIsolationMode,
    validate_routed_client_tenancy_wiring,
)
from forze.base.primitives import StrKey, StrKeyMapping

# ----------------------- #


class _StorageRouteConfig(Protocol):
    """Structural storage config: a row-level tenant flag and a bucket resource."""

    @property
    def tenant_aware(self) -> bool: ...

    @property
    def bucket(self) -> Any:
        # ``Any`` (not ``NamedResourceSpec``) so attrs ``converter=`` fields, which type
        # checkers model as an opaque descriptor, still satisfy the protocol.
        ...


# ....................... #


def validate_storage_tenancy_wiring(
    *,
    integration: str,
    client_is_routed: bool,
    storages: StrKeyMapping[_StorageRouteConfig] | Mapping[StrKey, _StorageRouteConfig] | None,
    required_isolation: TenantIsolationMode | None,
    validation_failed_code: str,
    log_warning: Callable[..., None] | None = None,
) -> None:
    """Derive the storage isolation tier and enforce the declared floor (fail closed).

    Tier (strongest wins): routed client → ``database``; a per-tenant (dynamic) ``bucket``
    resolver → ``schema``; a ``tenant_aware`` route (path prefix) → ``row``; else ``none``.
    Object stores can reach every tier, so the capability ceiling is ``database``.
    """

    configs = list((storages or {}).values())
    routes = [
        TenancyRouteSpec(name=str(name), tenant_aware=cfg.tenant_aware, kind="storage")
        for name, cfg in (storages or {}).items()
    ]
    has_namespace_routing = any(
        not is_static_named_resource(cfg.bucket) for cfg in configs
    )

    validate_routed_client_tenancy_wiring(
        integration=integration,
        client_is_routed=client_is_routed,
        partition_key_set=True,
        routes=routes,
        partition_key_detail="",
        validation_failed_code=validation_failed_code,
        required_isolation=required_isolation,
        has_namespace_routing=has_namespace_routing,
        max_supported_isolation="database",
        log_warning=log_warning,
    )
