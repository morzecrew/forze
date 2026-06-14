"""Factory for the tenant-selector self-service registry."""

from forze.application.contracts.authn import AuthnSpec, TokenLifecycleDepKey
from forze.application.execution import ExecutionContext
from forze.application.execution.operations import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze.application.hooks.authn import AuthnRequired
from forze.base.primitives import StrKeyNamespace

from ..authn.dto import AuthnTokenResponseDTO
from .dto import TenantListDTO, TenantSwitchRequestDTO
from .handlers import ListTenants, SwitchTenant
from .operations import TenancyKernelOp

# ----------------------- #


def build_tenancy_registry(
    spec: AuthnSpec,
    *,
    ns: StrKeyNamespace | None = None,
) -> OperationRegistry:
    """Build the tenant-selector registry (``list_tenants`` / ``switch_tenant``).

    Re-uses *spec* (the authn route) so ``switch_tenant`` can re-mint a tenant-scoped token
    via that route's ``TokenLifecyclePort``. Both ops require a bound principal and are
    **tenant-unaware** (you are *selecting* the tenant, so none is bound yet). The principal's
    membership is the authority: ``switch_tenant`` validates the requested tenant via the
    ``TenantResolverPort`` before minting. Merge the result with the app's authn registry (or
    register it under the same namespace) and project it with
    :func:`~forze_fastapi.attach_tenancy_routes`.
    """

    ns = ns or spec.default_namespace

    def _list_tenants(ctx: ExecutionContext) -> ListTenants:
        return ListTenants(
            resolver=ctx.inv_ctx.get_authn,
            current_tenant=ctx.inv_ctx.get_tenant,
            tenant_management=ctx.tenancy.require_manager(),
        )

    def _switch_tenant(ctx: ExecutionContext) -> SwitchTenant:
        return SwitchTenant(
            resolver=ctx.inv_ctx.get_authn,
            tenant_resolver=ctx.tenancy.require_resolver(),
            token_lifecycle=ctx.deps.resolve_configurable(
                ctx,
                TokenLifecycleDepKey,
                spec,
                route=spec.name,
            ),
        )

    reg = OperationRegistry(
        handlers={
            ns.key(TenancyKernelOp.LIST_TENANTS): _list_tenants,
            ns.key(TenancyKernelOp.SWITCH_TENANT): _switch_tenant,
        },
    )

    reg = reg.set_descriptors(
        {
            TenancyKernelOp.LIST_TENANTS: OperationDescriptor(
                output_type=TenantListDTO,
                description=(
                    "List the authenticated principal's active tenant memberships "
                    "(the basis of a tenant / organization selector)."
                ),
            ),
            TenancyKernelOp.SWITCH_TENANT: OperationDescriptor(
                input_type=TenantSwitchRequestDTO,
                output_type=AuthnTokenResponseDTO,
                description=(
                    "Activate one of the principal's tenants and re-mint a token pair "
                    "scoped to it (validates membership first)."
                ),
            ),
        },
        namespace=ns,
    )

    # ``list_tenants`` is a read; ``switch_tenant`` mints tokens (a command). Both act on the
    # *current* principal's memberships, so they require a bound principal — declared as a hook
    # so the catalog flags ``requires_authn`` (projected into the FastAPI/MCP auth surfaces).
    reg = (
        reg.bind(ns.key(TenancyKernelOp.LIST_TENANTS))
        .as_query()
        .bind_outer()
        .before(AuthnRequired().to_step())
        .finish(deep=True)
    )

    return (
        reg.bind(ns.key(TenancyKernelOp.SWITCH_TENANT))
        .bind_outer()
        .before(AuthnRequired().to_step())
        .finish(deep=True)
    )
