"""Authn dependency module for the application kernel."""

from collections.abc import Collection, Mapping
from enum import StrEnum
from typing import final

import attrs

from forze.application.contracts.authn import (
    ApiKeyLifecycleDepKey,
    AuthnDepKey,
    PasswordAccountProvisioningDepKey,
    PasswordLifecycleDepKey,
    TokenLifecycleDepKey,
)
from forze.application.execution import Deps, DepsModule
from forze.base.errors import CoreError

from .configs import (
    AuthnKernelConfig,
    AuthnRouteCaps,
    build_authn_shared_services,
    validate_route_caps,
    validate_shared_matches_route_sets,
)
from .deps import (
    ConfigurableApiKeyLifecycle,
    ConfigurableAuthn,
    ConfigurablePasswordAccountProvisioning,
    ConfigurablePasswordLifecycle,
    ConfigurableTokenLifecycle,
)

# ----------------------- #


def _normalize_route_set[K: str | StrEnum](routes: Collection[K] | None) -> frozenset[K]:
    return frozenset(routes) if routes else frozenset()


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class AuthnDepsModule[K: str | StrEnum](DepsModule[K]):
    """Registers authn dependency factories that resolve document ports via execution context."""

    kernel: AuthnKernelConfig | None = attrs.field(default=None)
    """Secrets and service configs; required when any route registration is non-empty."""

    authn: Mapping[K, AuthnRouteCaps] | None = attrs.field(default=None)
    token_lifecycle: Collection[K] | None = attrs.field(default=None)
    password_lifecycle: Collection[K] | None = attrs.field(default=None)
    api_key_lifecycle: Collection[K] | None = attrs.field(default=None)
    password_account_provisioning: Collection[K] | None = attrs.field(default=None)

    # ....................... #

    def __call__(self) -> Deps[K]:
        authn_map = self.authn or {}
        tl = _normalize_route_set(self.token_lifecycle)
        pl = _normalize_route_set(self.password_lifecycle)
        akl = _normalize_route_set(self.api_key_lifecycle)
        pap = _normalize_route_set(self.password_account_provisioning)

        has_registrations = bool(authn_map or tl or pl or akl or pap)

        if not has_registrations:
            return Deps[K]()

        if self.kernel is None:
            msg = "kernel is required when registering authn dependency routes"

            raise CoreError(msg)

        shared = build_authn_shared_services(self.kernel)

        validate_shared_matches_route_sets(
            shared=shared,
            token_lifecycle=tl,
            password_lifecycle=pl,
            api_key_lifecycle=akl,
            password_account_provisioning=pap,
        )

        merged: Deps[K] = Deps[K]()

        if authn_map:
            for caps in authn_map.values():
                validate_route_caps(shared, caps)

            merged = merged.merge(
                Deps[K].routed(
                    {
                        AuthnDepKey: {
                            name: ConfigurableAuthn(shared=shared, caps=caps)
                            for name, caps in authn_map.items()
                        },
                    },
                ),
            )

        if tl:
            merged = merged.merge(
                Deps[K].routed(
                    {
                        TokenLifecycleDepKey: {
                            name: ConfigurableTokenLifecycle(shared=shared) for name in tl
                        },
                    },
                ),
            )

        if pl:
            merged = merged.merge(
                Deps[K].routed(
                    {
                        PasswordLifecycleDepKey: {
                            name: ConfigurablePasswordLifecycle(shared=shared) for name in pl
                        },
                    },
                ),
            )

        if akl:
            merged = merged.merge(
                Deps[K].routed(
                    {
                        ApiKeyLifecycleDepKey: {
                            name: ConfigurableApiKeyLifecycle(shared=shared) for name in akl
                        },
                    },
                ),
            )

        if pap:
            merged = merged.merge(
                Deps[K].routed(
                    {
                        PasswordAccountProvisioningDepKey: {
                            name: ConfigurablePasswordAccountProvisioning(shared=shared)
                            for name in pap
                        },
                    },
                ),
            )

        return merged
