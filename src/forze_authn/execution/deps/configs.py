"""Kernel and shared-service configuration for authn dependency factories."""

from collections.abc import Collection, Mapping
from enum import StrEnum
from typing import final

import attrs

from forze.application.contracts.authn import AuthnMethod
from forze.base.errors import CoreError

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
) -> None:
    """Ensure each enabled method has the matching service in ``shared``."""

    if not methods:
        msg = "AuthnSpec.enabled_methods must contain at least one credential family"
        raise CoreError(msg)

    if "password" in methods and shared.password_svc is None:
        msg = "'password' method requires kernel.password"
        raise CoreError(msg)

    if "api_key" in methods and shared.api_key_svc is None:
        msg = "'api_key' method requires kernel.api_key_pepper"
        raise CoreError(msg)

    if "token" in methods and shared.access_svc is None:
        msg = "'token' method requires kernel.access_token_secret"
        raise CoreError(msg)


# ....................... #


def validate_shared_matches_route_sets[K: str | StrEnum](
    *,
    shared: AuthnSharedServices,
    authn: Mapping[K, frozenset[AuthnMethod]],
    token_lifecycle: Collection[K],
    password_lifecycle: Collection[K],
    api_key_lifecycle: Collection[K],
    password_account_provisioning: Collection[K],
) -> None:
    """Fail fast when routes require kernel sections that were not configured."""

    for methods in authn.values():
        validate_route_methods(shared, methods)

    if token_lifecycle:
        if shared.access_svc is None:
            msg = "token_lifecycle routes require kernel.access_token_secret"

            raise CoreError(msg)

        if shared.refresh_svc is None:
            msg = "token_lifecycle routes require kernel.refresh_token_pepper"

            raise CoreError(msg)

    if password_lifecycle or password_account_provisioning:
        if shared.password_svc is None:
            msg = (
                "password_lifecycle / password_account_provisioning routes require "
                "kernel.password"
            )

            raise CoreError(msg)

    if api_key_lifecycle:
        if shared.api_key_svc is None:
            msg = "api_key_lifecycle routes require kernel.api_key_pepper"

            raise CoreError(msg)
