"""Kernel configuration and shared services for authz dependency wiring."""

from datetime import timedelta
from typing import final

import attrs

from forze.application.contracts.authz import PermissionProvider
from forze.application.integrations.authz import (
    DEFAULT_PROVIDER_TIMEOUT,
    check_permission_providers,
    check_provider_timeout,
)
from forze.base.exceptions import exc

from ...services.grants import ProviderKeyCheck
from ...services.grants_cache import GrantsCache
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

    permission_providers: tuple[PermissionProvider, ...] = attrs.field(
        factory=tuple, converter=tuple
    )
    """Providers deriving permissions per decision from documents or configuration, run in
    declaration order after the catalog grants. Their keys are checked against the catalog once
    per tenant on the first decision that runs them, and at boot when
    :func:`~forze_identity.authz.permission_providers_lifecycle_step` is registered."""

    permission_provider_timeout: timedelta | None = DEFAULT_PROVIDER_TIMEOUT
    """How long one provider's ``derive`` may take; one that misses it has failed, and denies
    the keys it declares. ``None`` removes the deadline."""

    grants_cache: GrantsCache | None = None
    """Remembers each principal's catalog grants for its TTL, which is then how long a removed
    binding keeps granting. ``None`` (the default) reads the bindings on every decision. See
    :class:`~forze_identity.authz.services.grants_cache.GrantsCache`."""

    def __attrs_post_init__(self) -> None:
        check_permission_providers(self.permission_providers)
        check_provider_timeout(self.permission_provider_timeout)

        if self.grants_cache is not None and not isinstance(self.grants_cache, GrantsCache):
            raise exc.configuration(
                f"AuthzKernelConfig.grants_cache must be a GrantsCache, not {self.grants_cache!r}"
            )


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class AuthzSharedServices:
    """Services constructed once per authz dependency graph."""

    policy: AuthzPolicyService

    permission_providers: tuple[PermissionProvider, ...] = ()

    permission_provider_timeout: timedelta | None = DEFAULT_PROVIDER_TIMEOUT

    provider_key_check: ProviderKeyCheck | None = None
    """Shared by every resolver built from this graph, so the check runs once per tenant."""

    grants_cache: GrantsCache | None = None
    """Shared by every resolver built from this graph; ``None`` caches nothing."""


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
        permission_providers=kernel.permission_providers,
        permission_provider_timeout=kernel.permission_provider_timeout,
        provider_key_check=ProviderKeyCheck(providers=kernel.permission_providers),
        grants_cache=kernel.grants_cache,
    )
