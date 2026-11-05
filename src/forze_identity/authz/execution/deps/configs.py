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
from forze_identity.authn.services import RevocationStates

from ...services.grants import ProviderKeyCheck
from ...services.grants_cache import GrantsCache, GrantsInvalidation
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
    binding that nothing broadcasts keeps granting. ``None`` (the default) reads the bindings on
    every decision. See
    :class:`~forze_identity.authz.services.grants_cache.GrantsCache`."""

    grants_invalidation: GrantsInvalidation | None = None
    """Broadcasts each committed invalidation of :attr:`grants_cache` to every process that
    registers :func:`~forze_identity.authz.grants_invalidation_lifecycle_step`, so a revoked
    grant stops working there within broadcast latency rather than the TTL. Needs
    :attr:`grants_cache`. See
    :class:`~forze_identity.authz.services.grants_cache.GrantsInvalidation`."""

    revocation_states: RevocationStates | None = None
    """When set, every write that may deactivate a policy principal (``is_active`` set to
    anything but ``True``, or the row removed) raises the principal's epoch in its transaction
    and writes it through after commit, through the document's command port, so a token issued
    before is refused. Authn skips its eligibility read only when this is the value its own
    kernel carries, and a decision about the principal that skip admitted, later in the same
    task and outside a transaction, skips its policy principal read too. Such a write then
    refuses to run outside a transaction. Requires
    :attr:`~forze_identity.authn.services.RevocationStates.key_mac`: the skip trusts a cached
    epoch, so the epoch must be signed."""

    def __attrs_post_init__(self) -> None:
        check_permission_providers(self.permission_providers)
        check_provider_timeout(self.permission_provider_timeout)

        if self.grants_cache is not None and not isinstance(self.grants_cache, GrantsCache):
            raise exc.configuration(
                f"AuthzKernelConfig.grants_cache must be a GrantsCache, not {self.grants_cache!r}"
            )

        if self.grants_invalidation is not None:
            if not isinstance(self.grants_invalidation, GrantsInvalidation):
                raise exc.configuration(
                    "AuthzKernelConfig.grants_invalidation must be a GrantsInvalidation, not "
                    f"{self.grants_invalidation!r}"
                )

            if self.grants_cache is None:
                raise exc.configuration(
                    "AuthzKernelConfig.grants_invalidation broadcasts invalidations of the grants "
                    "cache, and there is none: set grants_cache too.",
                    code="authz_grants_invalidation_without_cache",
                )

        if self.revocation_states is not None and self.revocation_states.key_mac is None:
            raise exc.configuration(
                "AuthzKernelConfig.revocation_states lets authn skip its eligibility read on a "
                "cached epoch, which whoever can write the states store could otherwise restore: "
                "set RevocationStates.key_mac so the epochs are signed.",
                code="authn_eligibility_skip_requires_key_mac",
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

    grants_invalidation: GrantsInvalidation | None = None
    """Shared by every resolver built from this graph; ``None`` broadcasts nothing."""

    revocation_states: RevocationStates | None = None
    """Lets a decision skip its policy principal read after a fast-path admission of the same
    principal, for its ``ttl``; ``None`` reads it on every decision."""


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
        grants_invalidation=kernel.grants_invalidation,
        revocation_states=kernel.revocation_states,
    )
