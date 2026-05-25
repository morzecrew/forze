from uuid import UUID

from forze.base.errors import CoreError

from .value_objects import PrincipalRef

# ----------------------- #


def coalesce_authz_tenant_id(
    principal: PrincipalRef | UUID,
    *,
    tenant_id: UUID | None,
) -> UUID | None:
    """Return the effective tenant scope for an authz call.

    Explicit ``tenant_id`` wins over :attr:`PrincipalRef.tenant_id`. When both are
    set they must match or :class:`~forze.base.errors.CoreError` is raised.

    :param principal: Policy principal reference or bare principal id.
    :param tenant_id: Explicit tenant from the port call site.
    :returns: Resolved tenant id or ``None`` when the call is tenant-unscoped.
    """

    ref_tid = principal.tenant_id if isinstance(principal, PrincipalRef) else None

    if tenant_id is not None and ref_tid is not None and tenant_id != ref_tid:
        raise CoreError(
            "Conflicting tenant_id: PrincipalRef.tenant_id and explicit tenant_id disagree",
        )

    if tenant_id is not None:
        return tenant_id

    return ref_tid
