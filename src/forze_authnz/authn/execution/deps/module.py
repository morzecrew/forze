"""Authn dependency module for the application kernel."""

from collections.abc import Mapping
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

from .configs import (
    ApiKeyLifecycleRouteConfig,
    AuthnRouteConfig,
    PasswordLifecycleRouteConfig,
    PasswordProvisioningRouteConfig,
    TokenLifecycleRouteConfig,
)
from .deps import (
    ConfigurableApiKeyLifecycle,
    ConfigurableAuthn,
    ConfigurablePasswordAccountProvisioning,
    ConfigurablePasswordLifecycle,
    ConfigurableTokenLifecycle,
)

# ----------------------- #


def _validate_authn_route_config(cfg: AuthnRouteConfig) -> None:
    if cfg.password_svc is None and cfg.api_key_svc is None and cfg.access_svc is None:
        msg = "AuthnRouteConfig requires at least one of password_svc, api_key_svc, access_svc"

        raise ValueError(msg)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class AuthnDepsModule[K: str | StrEnum](DepsModule[K]):
    """Registers authn dependency factories that resolve document ports via execution context."""

    authn: Mapping[K, AuthnRouteConfig] | None = attrs.field(default=None)
    token_lifecycle: Mapping[K, TokenLifecycleRouteConfig] | None = attrs.field(default=None)
    password_lifecycle: Mapping[K, PasswordLifecycleRouteConfig] | None = attrs.field(
        default=None,
    )
    api_key_lifecycle: Mapping[K, ApiKeyLifecycleRouteConfig] | None = attrs.field(
        default=None,
    )
    password_account_provisioning: Mapping[K, PasswordProvisioningRouteConfig] | None = (
        attrs.field(default=None)
    )

    # ....................... #

    def __call__(self) -> Deps[K]:
        merged: Deps[K] = Deps[K]()

        if self.authn:
            for cfg in self.authn.values():
                _validate_authn_route_config(cfg)

            merged = merged.merge(
                Deps[K].routed(
                    {
                        AuthnDepKey: {
                            name: ConfigurableAuthn(config=cfg)
                            for name, cfg in self.authn.items()
                        },
                    },
                ),
            )

        if self.token_lifecycle:
            merged = merged.merge(
                Deps[K].routed(
                    {
                        TokenLifecycleDepKey: {
                            name: ConfigurableTokenLifecycle(config=cfg)
                            for name, cfg in self.token_lifecycle.items()
                        },
                    },
                ),
            )

        if self.password_lifecycle:
            merged = merged.merge(
                Deps[K].routed(
                    {
                        PasswordLifecycleDepKey: {
                            name: ConfigurablePasswordLifecycle(config=cfg)
                            for name, cfg in self.password_lifecycle.items()
                        },
                    },
                ),
            )

        if self.api_key_lifecycle:
            merged = merged.merge(
                Deps[K].routed(
                    {
                        ApiKeyLifecycleDepKey: {
                            name: ConfigurableApiKeyLifecycle(config=cfg)
                            for name, cfg in self.api_key_lifecycle.items()
                        },
                    },
                ),
            )

        if self.password_account_provisioning:
            merged = merged.merge(
                Deps[K].routed(
                    {
                        PasswordAccountProvisioningDepKey: {
                            name: ConfigurablePasswordAccountProvisioning(config=cfg)
                            for name, cfg in self.password_account_provisioning.items()
                        },
                    },
                ),
            )

        return merged
