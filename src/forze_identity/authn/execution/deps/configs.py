"""Kernel and shared-service configuration for authn dependency factories."""

from enum import StrEnum
from typing import Collection, Mapping, final

import attrs

from forze.application.contracts.authn import AuthnMethod
from forze.base.exceptions import exc

from ...services import (
    AccessTokenConfig,
    AccessTokenService,
    ApiKeyConfig,
    ApiKeyService,
    PasswordConfig,
    PasswordService,
    RefreshTokenConfig,
    RefreshTokenService,
)

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class AuthnKernelConfig:
    """Secrets and service tuning shared across all authn routes for one module.

    Optional sections omitting secrets disable building that service; route validation
    ensures the methods enabled in :class:`~forze.application.contracts.authn.AuthnSpec`
    only reference services that were built.
    """

    access_token_secret: bytes | None = attrs.field(
        default=None,
        validator=attrs.validators.optional(attrs.validators.min_len(32)),
    )
    """Minimum 32 bytes when set; required for token authentication and token lifecycle."""

    access_token: AccessTokenConfig = attrs.field(factory=AccessTokenConfig)
    """Access token service configuration."""

    refresh_token_pepper: bytes | None = attrs.field(
        default=None,
        validator=attrs.validators.optional(attrs.validators.min_len(32)),
    )
    """Minimum 32 bytes when set; required for token lifecycle."""

    refresh_token: RefreshTokenConfig = attrs.field(factory=RefreshTokenConfig)
    """Refresh token service configuration."""

    password: PasswordConfig | None = attrs.field(default=None)
    """When set, builds a single shared :class:`~forze_authn.services.password.PasswordService`."""

    api_key_pepper: bytes | None = attrs.field(
        default=None,
        validator=attrs.validators.optional(attrs.validators.min_len(32)),
    )
    """Minimum 32 bytes when set; required for API key authentication and API key lifecycle."""

    api_key: ApiKeyConfig = attrs.field(factory=ApiKeyConfig)
    """API key service configuration."""


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class AuthnSharedServices:
    """Services constructed once per :class:`AuthnKernelConfig`.

    Each field is optional; the corresponding kernel section gates its construction.
    Verifier dep factories pull only the services they need from this bundle.
    """

    access_svc: AccessTokenService | None
    refresh_svc: RefreshTokenService | None
    password_svc: PasswordService | None
    api_key_svc: ApiKeyService | None


# ....................... #


def build_authn_shared_services(kernel: AuthnKernelConfig) -> AuthnSharedServices:
    """Construct shared services from kernel configuration."""

    access_svc = (
        AccessTokenService(
            secret_key=kernel.access_token_secret,
            config=kernel.access_token,
        )
        if kernel.access_token_secret is not None
        else None
    )

    refresh_svc = (
        RefreshTokenService(
            pepper=kernel.refresh_token_pepper, config=kernel.refresh_token
        )
        if kernel.refresh_token_pepper is not None
        else None
    )

    password_svc = (
        PasswordService(config=kernel.password) if kernel.password is not None else None
    )

    api_key_svc = (
        ApiKeyService(pepper=kernel.api_key_pepper, config=kernel.api_key)
        if kernel.api_key_pepper is not None
        else None
    )

    return AuthnSharedServices(
        access_svc=access_svc,
        refresh_svc=refresh_svc,
        password_svc=password_svc,
        api_key_svc=api_key_svc,
    )


# ....................... #


def validate_route_methods(
    shared: AuthnSharedServices,
    methods: frozenset[AuthnMethod],
    *,
    skip_api_key_service: bool = False,
) -> None:
    """Ensure each enabled method has the matching service in ``shared``."""

    if not methods:
        msg = "AuthnSpec.enabled_methods must contain at least one credential family"
        raise exc.internal(msg)

    if "password" in methods and shared.password_svc is None:
        msg = "'password' method requires kernel.password"
        raise exc.internal(msg)

    if "api_key" in methods and shared.api_key_svc is None and not skip_api_key_service:
        msg = "'api_key' method requires kernel.api_key_pepper"
        raise exc.internal(msg)

    if "token" in methods and shared.access_svc is None:
        msg = "'token' method requires kernel.access_token_secret"
        raise exc.internal(msg)


# ....................... #


def validate_shared_matches_route_sets[K: str | StrEnum](
    *,
    shared: AuthnSharedServices,
    authn: Mapping[K, frozenset[AuthnMethod]],
    token_lifecycle: Collection[K],
    password_lifecycle: Collection[K],
    api_key_lifecycle: Collection[K],
    password_account_provisioning: Collection[K],
    api_key_verifier_overrides: Collection[K] | None = None,
) -> None:
    """Fail fast when routes require kernel sections that were not configured."""

    override_routes = frozenset(api_key_verifier_overrides or ())

    for route, methods in authn.items():
        validate_route_methods(
            shared,
            methods,
            skip_api_key_service=route in override_routes,
        )

    if token_lifecycle:
        if shared.access_svc is None:
            msg = "token_lifecycle routes require kernel.access_token_secret"

            raise exc.internal(msg)

        if shared.refresh_svc is None:
            msg = "token_lifecycle routes require kernel.refresh_token_pepper"

            raise exc.internal(msg)

    if password_lifecycle or password_account_provisioning:
        if shared.password_svc is None:
            msg = (
                "password_lifecycle / password_account_provisioning routes require "
                "kernel.password"
            )

            raise exc.internal(msg)

    if api_key_lifecycle:
        if shared.api_key_svc is None:
            msg = "api_key_lifecycle routes require kernel.api_key_pepper"

            raise exc.internal(msg)
