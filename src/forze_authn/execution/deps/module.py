"""Authn dependency module for the application kernel."""

from collections.abc import Collection, Mapping
from enum import StrEnum
from typing import final

import attrs

from forze.application.contracts.authn import (
    ApiKeyLifecycleDepKey,
    ApiKeyVerifierDepKey,
    AuthnDepKey,
    AuthnDepPort,
    AuthnMethod,
    PasswordAccountProvisioningDepKey,
    PasswordLifecycleDepKey,
    PasswordVerifierDepKey,
    PrincipalResolverDepKey,
    PrincipalResolverDepPort,
    TokenLifecycleDepKey,
    TokenVerifierDepKey,
)
from forze.application.execution import Deps, DepsModule
from forze.base.errors import CoreError

from .configs import (
    AuthnKernelConfig,
    build_authn_shared_services,
    validate_shared_matches_route_sets,
)
from .deps import (
    ConfigurableApiKeyLifecycle,
    ConfigurableArgon2PasswordVerifier,
    ConfigurableAuthn,
    ConfigurableForzeJwtTokenVerifier,
    ConfigurableHmacApiKeyVerifier,
    ConfigurableJwtNativeUuidResolver,
    ConfigurablePasswordAccountProvisioning,
    ConfigurablePasswordLifecycle,
    ConfigurableTokenLifecycle,
)

# ----------------------- #


def _normalize_route_set[K: str | StrEnum](
    routes: Collection[K] | None,
) -> frozenset[K]:
    return frozenset(routes) if routes else frozenset()


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class AuthnDepsModule[K: str | StrEnum](DepsModule[K]):
    """Register authn dependency factories that resolve document ports via execution context.

    The ``authn`` mapping carries one entry per route, describing which credential families
    that route accepts. For each entry the module registers (i) the matching first-party
    verifier under :class:`PasswordVerifierDepKey` / :class:`TokenVerifierDepKey` /
    :class:`ApiKeyVerifierDepKey`, (ii) a default :class:`JwtNativeUuidResolver` under
    :class:`PrincipalResolverDepKey`, and (iii) the orchestrator under :class:`AuthnDepKey`.
    External integrations override individual verifiers/resolvers by merging additional
    :class:`Deps` after the module's output.
    """

    kernel: AuthnKernelConfig | None = attrs.field(default=None)
    """Secrets and service configs; required when any route registration is non-empty."""

    authn: Mapping[K, frozenset[AuthnMethod]] | None = attrs.field(default=None)
    """Per-route enabled credential families. Empty/omitted disables authentication wiring."""

    resolvers: Mapping[K, PrincipalResolverDepPort] | None = attrs.field(default=None)
    """Optional per-route principal resolver overrides; routes without an entry get :class:`JwtNativeUuidResolver`."""

    token_verifiers: Mapping[K, AuthnDepPort] | None = attrs.field(default=None)  # type: ignore[type-arg]
    """Optional per-route token verifier overrides; routes without an entry get :class:`ForzeJwtTokenVerifier`."""

    password_verifiers: Mapping[K, AuthnDepPort] | None = attrs.field(default=None)  # type: ignore[type-arg]
    """Optional per-route password verifier overrides; routes without an entry get :class:`Argon2PasswordVerifier`."""

    api_key_verifiers: Mapping[K, AuthnDepPort] | None = attrs.field(default=None)  # type: ignore[type-arg]
    """Optional per-route API key verifier overrides; routes without an entry get :class:`HmacApiKeyVerifier`."""

    token_lifecycle: Collection[K] | None = attrs.field(default=None)
    password_lifecycle: Collection[K] | None = attrs.field(default=None)
    api_key_lifecycle: Collection[K] | None = attrs.field(default=None)
    password_account_provisioning: Collection[K] | None = attrs.field(default=None)

    # ....................... #

    def __call__(self) -> Deps[K]:  # noqa: C901
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
            authn=authn_map,
            token_lifecycle=tl,
            password_lifecycle=pl,
            api_key_lifecycle=akl,
            password_account_provisioning=pap,
        )

        merged: Deps[K] = Deps[K]()

        if authn_map:
            password_verifier_routes: dict[K, object] = {}
            token_verifier_routes: dict[K, object] = {}
            api_key_verifier_routes: dict[K, object] = {}
            resolver_routes: dict[K, object] = {}
            authn_routes: dict[K, object] = {}

            password_overrides = dict(self.password_verifiers or {})
            token_overrides = dict(self.token_verifiers or {})
            api_key_overrides = dict(self.api_key_verifiers or {})
            resolver_overrides = dict(self.resolvers or {})

            for name, methods in authn_map.items():
                methods_fs = frozenset(methods)

                if "password" in methods_fs:
                    password_verifier_routes[name] = password_overrides.get(
                        name,
                        ConfigurableArgon2PasswordVerifier(shared=shared),
                    )

                if "token" in methods_fs:
                    token_verifier_routes[name] = token_overrides.get(
                        name,
                        ConfigurableForzeJwtTokenVerifier(shared=shared),
                    )

                if "api_key" in methods_fs:
                    api_key_verifier_routes[name] = api_key_overrides.get(
                        name,
                        ConfigurableHmacApiKeyVerifier(shared=shared),
                    )

                resolver_routes[name] = resolver_overrides.get(
                    name,
                    ConfigurableJwtNativeUuidResolver(),
                )

                authn_routes[name] = ConfigurableAuthn(enabled_methods=methods_fs)

            routed_map: dict[object, dict[K, object]] = {AuthnDepKey: authn_routes}

            if password_verifier_routes:
                routed_map[PasswordVerifierDepKey] = password_verifier_routes

            if token_verifier_routes:
                routed_map[TokenVerifierDepKey] = token_verifier_routes

            if api_key_verifier_routes:
                routed_map[ApiKeyVerifierDepKey] = api_key_verifier_routes

            if resolver_routes:
                routed_map[PrincipalResolverDepKey] = resolver_routes

            merged = merged.merge(Deps[K].routed(routed_map))  # type: ignore[arg-type]

        if tl:
            merged = merged.merge(
                Deps[K].routed(
                    {
                        TokenLifecycleDepKey: {
                            name: ConfigurableTokenLifecycle(shared=shared)
                            for name in tl
                        },
                    },
                ),
            )

        if pl:
            merged = merged.merge(
                Deps[K].routed(
                    {
                        PasswordLifecycleDepKey: {
                            name: ConfigurablePasswordLifecycle(shared=shared)
                            for name in pl
                        },
                    },
                ),
            )

        if akl:
            merged = merged.merge(
                Deps[K].routed(
                    {
                        ApiKeyLifecycleDepKey: {
                            name: ConfigurableApiKeyLifecycle(shared=shared)
                            for name in akl
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
