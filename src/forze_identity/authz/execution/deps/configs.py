"""Kernel configuration and shared services for authz dependency wiring."""

from typing import final

import attrs

from ...services.policy import (
    DEFAULT_OWNER_OVERRIDE_PERMISSIONS,
    AuthzPolicyService,
    freeze_permission_keys,
)

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class AuthzKernelConfig:
    """Authz kernel options shared by every adapter built from one dependency graph."""

    owner_override_permissions: frozenset[str] = attrs.field(
        default=DEFAULT_OWNER_OVERRIDE_PERMISSIONS,
        converter=freeze_permission_keys,
    )
    """Permission keys that bypass the ``owner_id`` ABAC check in policy decisions.

    **Reserved by default:** ``admin`` and ``{resource_type}.admin`` (the
    placeholder is substituted with the resource type at evaluation time, e.g.
    ``invoice.admin``). A principal holding any of these keys overrides
    resource ownership, so do not reuse those names for unrelated app
    permissions. Pass an empty set to always enforce ownership, or different
    keys to rename the convention. See
    :class:`~forze_identity.authz.services.policy.AuthzPolicyService`.
    """


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class AuthzSharedServices:
    """Services constructed once per authz dependency graph."""

    policy: AuthzPolicyService


# ....................... #


def build_authz_shared_services(
    kernel: AuthzKernelConfig | None = None,
) -> AuthzSharedServices:
    """Build shared policy service."""

    kernel = kernel if kernel is not None else AuthzKernelConfig()

    return AuthzSharedServices(
        policy=AuthzPolicyService(
            owner_override_permissions=kernel.owner_override_permissions,
        ),
    )
