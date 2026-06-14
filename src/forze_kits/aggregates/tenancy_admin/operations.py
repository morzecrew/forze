"""Tenancy admin operation kernel suffixes (manage tenants and their members).

These are the privileged inverse of the self-service tenant selector
(:mod:`forze_kits.aggregates.tenancy`): they act on *arbitrary* tenants and principals, not the
caller's own membership, so they ship **unguarded** and the app must bind authn + authz.
"""

from enum import StrEnum
from typing import final

# ----------------------- #


@final
class TenancyAdminKernelOp(StrEnum):
    """Kernel segments (suffix only) for tenancy-admin usecase keys."""

    CREATE_TENANT = "create_tenant"
    """Provision a new tenant aggregate."""

    LIST_MEMBERS = "list_members"
    """List the principal ids that belong to a tenant."""

    INVITE_MEMBER = "invite_member"
    """Grant a principal membership in a tenant."""

    REMOVE_MEMBER = "remove_member"
    """Revoke a principal's membership in a tenant."""

    DEACTIVATE_TENANT = "deactivate_tenant"
    """Disable a tenant (the record lifecycle; infra teardown is ``deprovision_tenant``)."""
