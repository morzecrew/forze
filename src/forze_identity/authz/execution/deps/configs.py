"""Kernel configuration and shared services for authz dependency wiring."""

from typing import final

import attrs

from ...services.policy import AuthzPolicyService

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class AuthzKernelConfig:
    """Reserved for future authz kernel options (e.g. Casbin model path, feature flags)."""


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

    _ = kernel

    return AuthzSharedServices(policy=AuthzPolicyService())
