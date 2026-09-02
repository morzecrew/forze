"""The identity plane's spec contributions.

An application that wires ``forze_identity`` inherits **nineteen document specs it never
writes** — sessions, password and API-key accounts, invites, resets, identity mappings, the
eleven authz tables, and the two tenancy ones. They are bound into its deps module by name,
they hold the most sensitive rows in the system, and until now nothing could enumerate them.

That is the single most consequential gap in an inventory built from author declarations
alone: an export that walked only what the author wrote would omit every credential, session
and grant in the application — and the artifact would look complete.
"""

from collections.abc import Iterable
from typing import Any

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.inventory import SpecRegistry, SpecSource
from forze.base.exceptions import exc

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
# Feature-level groups — the spec sets the dependency modules actually consume, so an
# application binding a subset chooses a named group instead of rediscovering which
# tables belong together. Each group is pinned to its consumer by a test that records
# what the dependency factory resolves.

GRANT_RESOLUTION_SPECS = (
    permission_definition_spec,
    role_definition_spec,
    group_spec,
    role_permission_binding_spec,
    principal_role_binding_spec,
    principal_permission_binding_spec,
    group_principal_binding_spec,
    group_role_binding_spec,
    group_permission_binding_spec,
)
"""The nine specs the grant resolver reads. A principal can reach a role or permission
directly or through a group, so binding any subset of these resolves *fewer grants than
the database holds*, silently — bind all nine or none."""

AUTHZ_DECISION_SPECS = (policy_principal_spec, *GRANT_RESOLUTION_SPECS)
"""What an authorization decision (and policy scoping) reads: the policy principal plus
the whole grant-resolution set."""

DELEGATION_SPECS = (policy_principal_spec, delegation_grant_spec)
"""What the delegation (on-behalf-of) ports read and write."""

PASSWORD_LIFECYCLE_SPECS = (
    password_account_spec,
    password_reset_spec,
    password_invite_spec,
    session_spec,
)
"""What password lifecycle touches end to end: accounts, reset tokens, invites, and the
sessions revoked on password change or reset."""

TENANT_RESOLUTION_SPECS = (principal_tenant_binding_spec, tenant_spec)
"""What tenant resolution reads: the principal-tenant binding, plus the tenant row when
active-tenant verification is on."""


# ....................... #


def identity_document_names(
    specs: Iterable[DocumentSpec[Any, Any, Any, Any] | str] | None = None,
) -> tuple[str, ...]:
    """The validated spec names for a selection of identity documents.

    ``specs`` takes spec objects (the plane tuples or feature groups above) or bare
    names, defaulting to every identity spec; overlapping groups deduplicate, an empty
    selection is refused, and an unknown or foreign entry fails naming the spec — so a
    renamed or misspelled spec fails a test naming the spec rather than a deploy naming
    a missing binding. Where the rows live — schema, table or collection names, the
    backend config that binds them — is the application's, keyed by these names.
    """

    known = {str(spec.name): spec for spec in (*AUTHN_SPECS, *AUTHZ_SPECS, *TENANCY_SPECS)}

    names: dict[str, None] = {}

    for item in known.values() if specs is None else specs:
        name = item if isinstance(item, str) else str(item.name)

        if name not in known or (not isinstance(item, str) and item is not known[name]):
            raise exc.configuration(
                f"Unknown identity document spec {name!r}; known specs: {sorted(known)}",
            )

        names[name] = None

    if not names:
        # An empty selection binds nothing and would read as success; an emptied group
        # constant or a bad comprehension should fail here, at the seam.
        raise exc.configuration("Identity document selection is empty; pass specs or omit them")

    return tuple(names)


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
        identity=True,
    )
