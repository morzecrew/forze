"""Route configuration for authn dependency factories (services only, no persistence)."""

from typing import final

import attrs

from ...services import (
    AccessTokenService,
    ApiKeyService,
    PasswordService,
    RefreshTokenService,
)

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class AuthnRouteConfig:
    """Services backing :class:`~forze_authnz.authn.adapters.authentication.AuthnAdapter`."""

    password_svc: PasswordService | None = attrs.field(default=None)
    api_key_svc: ApiKeyService | None = attrs.field(default=None)
    access_svc: AccessTokenService | None = attrs.field(default=None)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class TokenLifecycleRouteConfig:
    """Services backing :class:`~forze_authnz.authn.adapters.token_lifecycle.TokenLifecycleAdapter`."""

    access_svc: AccessTokenService
    refresh_svc: RefreshTokenService


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class PasswordLifecycleRouteConfig:
    """Services backing :class:`~forze_authnz.authn.adapters.password_lifecycle.PasswordLifecycleAdapter`."""

    password_svc: PasswordService


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ApiKeyLifecycleRouteConfig:
    """Services backing :class:`~forze_authnz.authn.adapters.api_key_lifecycle.ApiKeyLifecycleAdapter`."""

    api_key_svc: ApiKeyService


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class PasswordProvisioningRouteConfig:
    """Services backing :class:`~forze_authnz.authn.adapters.password_provisioning.PasswordAccountProvisioningAdapter`."""

    password_svc: PasswordService
