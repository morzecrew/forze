"""Kernel configuration and shared services for authz dependency wiring."""

from collections.abc import Iterable
from typing import final

import attrs

from forze.application.contracts.authz import PermissionProvider
from forze.base.exceptions import exc

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
    declaration order after the catalog grants. Their keys are checked against the catalog when
    the runtime starts — see :func:`~forze_identity.authz.permission_providers_lifecycle_step`."""

    def __attrs_post_init__(self) -> None:
        check_permission_providers(self.permission_providers)


def check_permission_providers(providers: Iterable[PermissionProvider]) -> None:
    """Refuse a declaration nobody meant: a blank or repeated name, or keys that are not a
    non-empty set of permission-key strings."""

    names: set[str] = set()

    for provider in providers:
        if not provider.name.strip() or provider.name in names:
            raise exc.configuration(
                f"Permission provider name {provider.name!r} is blank or used twice; a derived "
                "grant is attributed to its provider by name.",
                code="authz_provider_declaration",
            )

        if not provider.keys:
            raise exc.configuration(
                f"Permission provider {provider.name!r} declares no keys; a provider that may "
                "grant or deny nothing is a declaration nobody meant.",
                code="authz_provider_declaration",
            )

        # Every decision reads the declaration and takes set differences against it: a list or
        # a bare string would fail there rather than here, and a mutable set could be widened
        # after this check, past the catalog check at boot.
        if not isinstance(provider.keys, frozenset) or not all(
            isinstance(key, str) for key in provider.keys
        ):
            raise exc.configuration(
                f"Permission provider {provider.name!r} must declare its keys as a frozenset of "
                f"permission-key strings, not {type(provider.keys).__name__}.",
                code="authz_provider_declaration",
            )

        names.add(provider.name)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class AuthzSharedServices:
    """Services constructed once per authz dependency graph."""

    policy: AuthzPolicyService

    permission_providers: tuple[PermissionProvider, ...] = ()


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
    )
