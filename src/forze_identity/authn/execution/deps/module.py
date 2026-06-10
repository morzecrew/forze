"""Authn dependency module for the application kernel."""

from typing import Collection, Mapping, final

import attrs

from forze.application.contracts.authn import (
    ApiKeyLifecycleDepKey,
    ApiKeyVerifierDepKey,
    ApiKeyVerifierDepPort,
    AuthnDepKey,
    AuthnMethod,
    PasswordAccountProvisioningDepKey,
    PasswordLifecycleDepKey,
    PasswordVerifierDepKey,
    PasswordVerifierDepPort,
    PrincipalDeactivationDepKey,
    PrincipalEligibilityDepKey,
    PrincipalResolverDepKey,
    PrincipalResolverDepPort,
    TokenLifecycleDepKey,
    TokenVerifierDepKey,
    TokenVerifierDepPort,
)
from forze.application.execution import Deps, DepsModule
from forze.base.exceptions import exc
from forze.base.primitives import StrKey
from forze_identity._routes import normalize_route_set as _normalize_route_set

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
    ConfigurablePolicyPrincipalEligibility,
    ConfigurablePrincipalDeactivation,
    ConfigurableTokenLifecycle,
)

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class AuthnDepsModule(DepsModule):
    """Register authn dependency factories that resolve document ports via execution context.

    The ``authn`` mapping carries one entry per route, describing which credential families
    that route accepts. For each entry the module registers:

    1. the matching first-party verifier under :class:`PasswordVerifierDepKey` / :class:`TokenVerifierDepKey` / :class:`ApiKeyVerifierDepKey`,
    2. a default :class:`JwtNativeUuidResolver` under :class:`PrincipalResolverDepKey`,
    3. the orchestrator under :class:`AuthnDepKey`.

    External integrations override individual verifiers/resolvers by merging additional
    :class:`Deps` after the module's output.
    """

    kernel: AuthnKernelConfig | None = attrs.field(default=None)
    """Secrets and service configs; required when any route registration is non-empty."""

    authn: Mapping[StrKey, frozenset[AuthnMethod]] | None = attrs.field(default=None)
    """Per-route enabled credential families. Empty/omitted disables authentication wiring."""

    resolvers: Mapping[StrKey, PrincipalResolverDepPort] | None = attrs.field(
        default=None
    )
    """Optional per-route principal resolver overrides; routes without an entry get :class:`JwtNativeUuidResolver`."""

    token_verifiers: Mapping[StrKey, TokenVerifierDepPort] | None = attrs.field(
        default=None
    )
    """Optional per-route token verifier overrides; routes without an entry get :class:`ForzeJwtTokenVerifier`."""

    password_verifiers: Mapping[StrKey, PasswordVerifierDepPort] | None = attrs.field(
        default=None
    )
    """Optional per-route password verifier overrides; routes without an entry get :class:`Argon2PasswordVerifier`."""

    api_key_verifiers: Mapping[StrKey, ApiKeyVerifierDepPort] | None = attrs.field(
        default=None
    )
    """Optional per-route API key verifier overrides; routes without an entry get :class:`HmacApiKeyVerifier`."""

    token_lifecycle: Collection[StrKey] | None = attrs.field(default=None)
    """Authn routes that should use first-party token lifecycle management."""

    password_lifecycle: Collection[StrKey] | None = attrs.field(default=None)
    """Authn routes that should use first-party password lifecycle management."""

    api_key_lifecycle: Collection[StrKey] | None = attrs.field(default=None)
    """Authn routes that should use first-party API key lifecycle management."""

    password_account_provisioning: Collection[StrKey] | None = attrs.field(default=None)
    """Authn routes that should use first-party password account provisioning."""

    principal_deactivation: Collection[StrKey] | None = attrs.field(default=None)
    """Authn routes that expose cascaded principal deactivation."""

    authz_route: StrKey | None = attrs.field(default=None)
    """Authz route name for :class:`PrincipalRegistryPort` when deactivation is registered."""

    actor_claim: str | None = attrs.field(default=None)
    """When set (e.g. ``"act"``), token routes read this claim as an RFC 8693 delegation
    actor and attach it as :attr:`AuthnIdentity.actor`; ``None`` ignores delegation claims."""

    revoke_sessions_on_password_change: bool = attrs.field(default=True)
    """Whether first-party password lifecycle routes revoke ALL of the principal's
    sessions after a successful password change ("log out everywhere"). The caller must
    re-authenticate with the new password; set ``False`` to keep existing sessions alive."""

    password_rehash_on_login: bool = attrs.field(default=False)
    """When ``True``, the default :class:`Argon2PasswordVerifier` upgrades stored hashes
    to the current Argon2 parameters after a successful login (wires the password
    account command port into the verifier). Best-effort; never fails the login."""

    # ....................... #

    def __call__(self) -> Deps:  # noqa: C901
        authn_map = self.authn or {}
        tl = _normalize_route_set(self.token_lifecycle)
        pl = _normalize_route_set(self.password_lifecycle)
        akl = _normalize_route_set(self.api_key_lifecycle)
        pap = _normalize_route_set(self.password_account_provisioning)
        pd = _normalize_route_set(self.principal_deactivation)

        has_registrations = bool(authn_map or tl or pl or akl or pap or pd)

        if not has_registrations:
            return Deps()

        if self.kernel is None:
            raise exc.internal(
                "kernel is required when registering authn dependency routes"
            )

        shared = build_authn_shared_services(self.kernel)

        api_key_overrides_keys = frozenset((self.api_key_verifiers or {}).keys())
        token_overrides_keys = frozenset((self.token_verifiers or {}).keys())
        resolver_overrides_keys = frozenset((self.resolvers or {}).keys())

        # An external token verifier paired with the default JwtNativeUuidResolver
        # would trust the external IdP's ``sub`` as an internal principal UUID —
        # a UUID-shaped external subject could collide with an internal principal.
        # Scoped to token_verifiers only: api_key/password overrides (e.g. the
        # builtin local wiring) legitimately emit internal principal UUIDs.
        for name in token_overrides_keys - resolver_overrides_keys:
            methods = authn_map.get(name)

            if methods is not None and "token" in methods:
                raise exc.configuration(
                    f"token_verifiers override for route {name!r} requires an "
                    "explicit resolvers override; the default JwtNativeUuidResolver "
                    "trusts the assertion subject as an internal principal UUID, "
                    "which is unsafe for external token verifiers",
                )

        validate_shared_matches_route_sets(
            shared=shared,
            authn=authn_map,
            token_lifecycle=tl,
            password_lifecycle=pl,
            api_key_lifecycle=akl,
            password_account_provisioning=pap,
            api_key_verifier_overrides=api_key_overrides_keys,
            token_verifier_overrides=token_overrides_keys,
        )

        merged: Deps = Deps()

        eligibility_routes = authn_map.keys() | tl | pl | akl | pap | pd
        if eligibility_routes:
            merged = merged.merge(
                Deps.routed(
                    {
                        PrincipalEligibilityDepKey: {
                            name: ConfigurablePolicyPrincipalEligibility()
                            for name in eligibility_routes
                        },
                    },
                ),
            )

        if pd:
            if self.authz_route is None:
                raise exc.internal(
                    "authz_route is required when principal_deactivation routes are registered",
                )
            merged = merged.merge(
                Deps.routed(
                    {
                        PrincipalDeactivationDepKey: {
                            name: ConfigurablePrincipalDeactivation(
                                authz_route=self.authz_route,
                            )
                            for name in pd
                        },
                    },
                ),
            )

        if authn_map:
            password_verifier_routes: dict[StrKey, object] = {}
            token_verifier_routes: dict[StrKey, object] = {}
            api_key_verifier_routes: dict[StrKey, object] = {}
            resolver_routes: dict[StrKey, object] = {}
            authn_routes: dict[StrKey, object] = {}

            password_overrides = dict(self.password_verifiers or {})
            token_overrides = dict(self.token_verifiers or {})
            api_key_overrides = dict(self.api_key_verifiers or {})
            resolver_overrides = dict(self.resolvers or {})

            for name, methods in authn_map.items():
                methods_fs = frozenset(methods)

                if "password" in methods_fs:
                    password_verifier_routes[name] = password_overrides.get(
                        name,
                        ConfigurableArgon2PasswordVerifier(
                            shared=shared,
                            rehash_on_login=self.password_rehash_on_login,
                        ),
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

                authn_routes[name] = ConfigurableAuthn(
                    enabled_methods=methods_fs,
                    actor_claim=self.actor_claim,
                )

            routed_map: dict[object, dict[StrKey, object]] = {AuthnDepKey: authn_routes}

            if password_verifier_routes:
                routed_map[PasswordVerifierDepKey] = password_verifier_routes

            if token_verifier_routes:
                routed_map[TokenVerifierDepKey] = token_verifier_routes

            if api_key_verifier_routes:
                routed_map[ApiKeyVerifierDepKey] = api_key_verifier_routes

            if resolver_routes:
                routed_map[PrincipalResolverDepKey] = resolver_routes

            merged = merged.merge(Deps.routed(routed_map))  # type: ignore[arg-type]

        if tl:
            merged = merged.merge(
                Deps.routed(
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
                Deps.routed(
                    {
                        PasswordLifecycleDepKey: {
                            name: ConfigurablePasswordLifecycle(
                                shared=shared,
                                revoke_sessions_on_password_change=(
                                    self.revoke_sessions_on_password_change
                                ),
                            )
                            for name in pl
                        },
                    },
                ),
            )

        if akl:
            merged = merged.merge(
                Deps.routed(
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
                Deps.routed(
                    {
                        PasswordAccountProvisioningDepKey: {
                            name: ConfigurablePasswordAccountProvisioning(shared=shared)
                            for name in pap
                        },
                    },
                ),
            )

        return merged
