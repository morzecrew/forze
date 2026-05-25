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
    """How strictly tenant context from the invocation must match policy scope."""

    enforce_principal_active: bool = True
    """When true, inactive policy principals are denied at runtime."""
