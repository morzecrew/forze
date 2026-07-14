"""The identity plane's spec contributions.

An application that wires ``forze_identity`` inherits **nineteen document specs it never
writes** — sessions, password and API-key accounts, invites, resets, identity mappings, the
eleven authz tables, and the two tenancy ones. They are bound into its deps module by name,
they hold the most sensitive rows in the system, and until now nothing could enumerate them.

That is the single most consequential gap in an inventory built from author declarations
alone: an export that walked only what the author wrote would omit every credential, session
and grant in the application — and the artifact would look complete.
"""

from forze.application.contracts.inventory import SpecRegistry, SpecSource

from .authn.application.specs import (
    api_key_account_spec,
    identity_mapping_spec,
    password_account_spec,
    password_invite_spec,
    password_reset_spec,
    session_spec,
)
from .authz.application.specs import (
    delegation_grant_spec,
    group_permission_binding_spec,
    group_principal_binding_spec,
    group_role_binding_spec,
    group_spec,
    permission_definition_spec,
    policy_principal_spec,
    principal_permission_binding_spec,
    principal_role_binding_spec,
    role_definition_spec,
    role_permission_binding_spec,
)
from .tenancy.application.specs import principal_tenant_binding_spec, tenant_spec

# ----------------------- #

AUTHN_SPECS = (
    password_account_spec,
    api_key_account_spec,
    password_invite_spec,
    password_reset_spec,
    session_spec,
    identity_mapping_spec,
)
"""The six authn document specs. All but ``identity_mapping_spec`` are ``sensitive``."""

AUTHZ_SPECS = (
    policy_principal_spec,
    permission_definition_spec,
    role_definition_spec,
    group_spec,
    role_permission_binding_spec,
    principal_role_binding_spec,
    principal_permission_binding_spec,
    group_principal_binding_spec,
    group_role_binding_spec,
    group_permission_binding_spec,
    delegation_grant_spec,
)
"""The eleven authz document specs."""

TENANCY_SPECS = (tenant_spec, principal_tenant_binding_spec)
"""The two tenancy document specs."""


# ....................... #


def spec_contributions() -> SpecRegistry:
    """Every document spec the identity plane binds.

    Merge it into the application's inventory whenever any part of ``forze_identity`` is
    wired. All three planes come together deliberately: authn's dependencies already reach
    across into authz (its principal-eligibility check reads ``authz_policy_principals``), so
    a per-subpackage helper would leak that coupling onto the app.

    ``AuthnSpec`` / ``AuthzSpec`` are **not** here. They are policy — which credential
    families are enabled, how tenancy is enforced — carry no rows, and take a route name the
    *app* chooses. There is nothing to catalogue and nothing to export.
    """

    return SpecRegistry().register(
        *AUTHN_SPECS,
        *AUTHZ_SPECS,
        *TENANCY_SPECS,
        source=SpecSource.FRAMEWORK,
    )
