from typing import Literal

import attrs

from ..base import BaseSpec

# ----------------------- #

AuthzTenancyMode = Literal["global", "require_invocation_tenant"]
"""How runtime authz uses tenant context from :class:`~forze.application.execution.context.invocation.InvocationContext`."""

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthzSpec(BaseSpec):
    """Specification for authorization behavior on a named route.

    :attr:`tenancy_mode`:
        ``global`` — policy evaluation does not require a bound tenant.
        ``require_invocation_tenant`` — runtime/scoping calls must carry the invocation
        tenant in :class:`~forze.application.contracts.authz.value_objects.decision.AuthzScope` when
        ``ctx.inv.get_tenant()`` is set.
    """

    tenancy_mode: AuthzTenancyMode = "global"
    """How strictly tenant context from the invocation must match policy scope.

    ``global`` (the default) resolves grants without tenant partitioning:
    roles, permissions, and bindings are shared across **all** tenants, even
    when the backing document ports are tenant-aware. That is a legitimate
    choice for deliberately platform-wide roles, but in a multi-tenant
    deployment it usually means the flag was forgotten — set
    ``require_invocation_tenant`` so grant resolution is partitioned by the
    invocation tenant (adapters then refuse non-tenant-aware ports at
    startup). Authz adapters log a warning when constructed in ``global``
    mode over tenant-aware ports.
    """

    enforce_principal_active: bool = True
    """When true, inactive policy principals are denied at runtime."""

    enforce_delegation_grant: bool = False
    """When true, a delegated call (a request whose subject carries an ``actor``) additionally
    requires an explicit ``may_act`` grant pairing the actor to the subject — checked via
    :class:`~forze.application.contracts.authz.ports.DelegationPort` *on top of* the
    least-privilege intersection. Off by default (intersection-only); enabling it without a
    wired delegation port is a configuration error (fails loud, never open)."""
