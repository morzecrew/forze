from typing import Any, Iterable
from uuid import UUID

from forze.application.contracts.authz.specs import AuthzSpec
from forze.application.contracts.document import (
    BaseDocumentPort,
    DocumentQueryPort,
    DocumentSpec,
)
from forze.base.exceptions import exc

from forze_identity._secure_spec import forbid_cache_and_history

from .._logger import logger
from ..domain.models.policy_principal import ReadPolicyPrincipal

# ----------------------- #


def validate_secure_authz_document_spec(spec: DocumentSpec[Any, Any, Any, Any]) -> None:
    """Reject cache/history on authz documents (same rationale as authn principal docs)."""

    forbid_cache_and_history(spec, label="Authz document")


validate_policy_principal_spec = validate_secure_authz_document_spec


# ....................... #


def validate_authz_query_ports(
    spec: AuthzSpec,
    ports: Iterable[BaseDocumentPort[Any, Any, Any, Any]],
) -> None:
    """Validate authz binding/catalog ports: secure-spec rules plus tenant isolation.

    Always forbids cache/history (see :func:`validate_secure_authz_document_spec`).
    Additionally, when the route is tenant-scoped
    (``spec.tenancy_mode == "require_invocation_tenant"``), **every** grant-resolution
    port must be tenant-aware. Grant resolution relies on the storage layer to
    partition bindings by tenant, so a non-tenant-aware binding/catalog port would let
    a principal's grants leak across tenants. Refusing to construct the adapter
    surfaces the misconfiguration at startup rather than failing open at request time.

    Conversely, when the route is ``global`` (the default) but the wired ports
    *are* tenant-aware — the detectable signal of a multi-tenant deployment —
    a warning is logged once per adapter: grant resolution ignores tenant
    context in global mode, so roles/permissions are shared across all tenants.
    That can be a deliberate choice (platform-wide roles over tenant-partitioned
    stores), hence a warning rather than an error.

    :raises CoreException: When a port enables cache/history, or when a tenant-scoped
        route is wired with a non-tenant-aware port.
    """

    require_tenant_aware = spec.tenancy_mode == "require_invocation_tenant"
    tenant_aware_ports: list[str] = []

    for port in ports:
        validate_secure_authz_document_spec(port.spec)

        if port.tenant_aware:
            tenant_aware_ports.append(port.spec.name)

        if require_tenant_aware and not port.tenant_aware:
            raise exc.configuration(
                "Authz grant-resolution port "
                f"{port.spec.name!r} must be tenant-aware (tenant_aware=True) on a "
                "tenant-scoped route (tenancy_mode='require_invocation_tenant'); "
                "otherwise effective grants are not partitioned by tenant.",
            )

    if spec.tenancy_mode == "global" and tenant_aware_ports:
        logger.warning(
            "Authz route uses tenancy_mode='global' over tenant-aware document "
            "ports: grant resolution ignores tenant context, so roles and "
            "permissions are shared across all tenants. Set "
            "tenancy_mode='require_invocation_tenant' on the AuthzSpec for "
            "per-tenant grant isolation; keep 'global' only if platform-wide "
            "roles over tenant-partitioned stores are intended.",
            route=spec.name,
            tenant_aware_ports=sorted(set(tenant_aware_ports)),
        )


# ....................... #


async def find_policy_principal_by_id(
    qry: DocumentQueryPort[ReadPolicyPrincipal],
    principal_id: UUID,
) -> ReadPolicyPrincipal | None:
    """Load policy principal by document id."""

    return await qry.find(
        filters={
            "$values": {
                "id": principal_id,
            },
        },
    )
