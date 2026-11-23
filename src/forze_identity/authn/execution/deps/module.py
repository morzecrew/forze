"""Authn dependency module for the application kernel."""

from collections.abc import Collection
from typing import Literal, final

import attrs

from forze.application.contracts.authn import (
    ApiKeyLifecycleDepKey,
    ApiKeyVerifierDepKey,
    ApiKeyVerifierDepPort,
    AuthnDepKey,
    AuthnEventSinkDepKey,
    AuthnEventSinkDepPort,
    AuthnMethod,
    PasswordAccountProvisioningDepKey,
    PasswordLifecycleDepKey,
    PasswordResetDepKey,
    PasswordVerifierDepKey,
    PasswordVerifierDepPort,
    PrincipalDeactivationDepKey,
    PrincipalEligibilityDepKey,
    PrincipalEligibilityDepPort,
    PrincipalResolverDepKey,
    PrincipalResolverDepPort,
    TokenLifecycleDepKey,
    TokenVerifierDepKey,
    TokenVerifierDepPort,
)
from forze.application.contracts.deps import Deps, DepsModule
from forze.application.execution import WiringFinding
from forze.application.integrations.authn import LOCKOUT_COUNTER_ROUTE, LockoutConfig
from forze.base.exceptions import exc
from forze.base.primitives import MappingConverter, StrKey, StrKeyMapping
from forze_identity._routes import normalize_route_set as _normalize_route_set
from forze_identity._routes import refuse_undeclared_overrides
from forze_identity.authn._logger import logger

from .configs import (
    AuthnKernelConfig,
    build_authn_shared_services,
    validate_shared_matches_route_sets,
)
from .deps import (
    ConfigurableAllowAllEligibility,
    ConfigurableApiKeyLifecycle,
    ConfigurableArgon2PasswordVerifier,
    ConfigurableAuthn,
    ConfigurableForzeJwtTokenVerifier,
    ConfigurableHmacApiKeyVerifier,
    ConfigurableJwtNativeUuidResolver,
    ConfigurablePasswordAccountProvisioning,
    ConfigurablePasswordLifecycle,
    ConfigurablePasswordReset,
    ConfigurablePolicyPrincipalEligibility,
    ConfigurablePrincipalDeactivation,
    ConfigurableTokenLifecycle,
)

# ----------------------- #

type EligibilityChoice = Literal["policy_principal", "allow_all"] | PrincipalEligibilityDepPort
"""What :attr:`AuthnDepsModule.eligibility` accepts: one of the two built-in gates by name, or
a gate factory over the principal registry the deployment has."""

_ELIGIBILITY_NAMES = frozenset({"policy_principal", "allow_all"})

ALLOW_ALL_RULE = "admits every principal: deactivating one does not stop its authentication"
"""The rule an ``eligibility="allow_all"`` finding reports, for every route it covers."""

# ....................... #


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
    :class:`Deps` after the module's output, or through the per-route override mappings,
    whose keys must be routes in ``authn``: any other key is refused at construction.
    """

    kernel: AuthnKernelConfig | None = attrs.field(default=None)
    """Secrets and service configs; required when any route registration is non-empty."""

    authn: StrKeyMapping[frozenset[AuthnMethod]] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Per-route enabled credential families. Empty/omitted disables authentication wiring."""

    resolvers: StrKeyMapping[PrincipalResolverDepPort] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Optional per-route principal resolver overrides; routes without an entry get :class:`JwtNativeUuidResolver`."""

    token_verifiers: StrKeyMapping[TokenVerifierDepPort] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Optional per-route token verifier overrides; routes without an entry get :class:`ForzeJwtTokenVerifier`."""

    password_verifiers: StrKeyMapping[PasswordVerifierDepPort] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Optional per-route password verifier overrides; routes without an entry get :class:`Argon2PasswordVerifier`."""

    api_key_verifiers: StrKeyMapping[ApiKeyVerifierDepPort] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
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

    password_reset: Collection[StrKey] | None = attrs.field(default=None)
    """Authn routes that expose self-service password reset (requires
    ``kernel.password`` and ``kernel.reset_token_pepper``)."""

    principal_deactivation: Collection[StrKey] | None = attrs.field(default=None)
    """Authn routes that expose cascaded principal deactivation."""

    authz_route: StrKey | None = attrs.field(default=None)
    """Authz route name for :class:`PrincipalRegistryPort`: required when deactivation is
    registered, and used by API-key lifecycle routes to require that a delegation key's agent
    is a ``service`` principal (without it only the eligibility gate applies)."""

    actor_claim: str | None = attrs.field(default=None)
    """When set (e.g. ``"act"``), token routes read this claim as an RFC 8693 delegation
    actor and attach it as :attr:`AuthnIdentity.actor`; ``None`` ignores delegation claims."""

    revoke_sessions_on_password_change: bool = attrs.field(default=True)
    """Whether first-party password lifecycle routes revoke ALL of the principal's
    sessions after a successful password change ("log out everywhere"). The caller must
    re-authenticate with the new password; set ``False`` to keep existing sessions alive."""

    revoke_sessions_on_reset: bool = attrs.field(default=True)
    """Whether first-party password reset routes revoke ALL of the principal's
    sessions after a successful reset ("log out everywhere", matching the
    change-password posture). Set ``False`` only when sessions are managed by an
    external lifecycle."""

    password_rehash_on_login: bool = attrs.field(default=False)
    """When ``True``, the default :class:`Argon2PasswordVerifier` upgrades stored hashes
    to the current Argon2 parameters after a successful login (wires the password
    account command port into the verifier). Best-effort; never fails the login."""

    eligibility: EligibilityChoice = attrs.field(default="policy_principal")
    """Which principal-eligibility gate every wired route resolves.

    ``"policy_principal"`` (default): authentication and credential mutations require an
    ACTIVE policy principal document, the authz plane's principal registry. A gate factory
    (:data:`~forze.application.contracts.authn.PrincipalEligibilityDepPort`): the gate reads
    the registry the deployment names, such as the application's own user table, and must
    refuse a principal it does not know. ``"allow_all"``: nobody is checked, every principal
    is eligible; revocation then rests on credential lifecycle (session and API-key revoke)
    alone, and a deactivated principal no longer blocks token issuance. ``"allow_all"`` logs
    a warning when the module is constructed and is a production posture finding
    (:meth:`posture_findings`).

    Only the policy-principal gate skips its read after a fast-path admission; every other
    gate reads on every check. Any other string, or a value that is not callable, is refused
    at construction."""

    events: AuthnEventSinkDepPort | None = attrs.field(default=None)
    """Optional authn event sink factory (e.g.
    :class:`ConfigurableLoggingAuthnEventSink`). ``None`` (default) disables
    emission entirely.

    One **shared** sink registered for every route this module wires — the
    module's cross-cutting policy knobs (``actor_claim``,
    ``revoke_sessions_on_*``) are likewise module-wide, and observability is the
    same kind of concern. For a different sink per route, leave ``events`` unset
    and register :data:`AuthnEventSinkDepKey` for each route yourself. A second
    non-fallback registration for a route the module already wired is refused when
    the deps merge; a fallback one is shadowed by the module's sink."""

    lockout: LockoutConfig | None = attrs.field(default=None)
    """Optional fixed-window login lockout for password routes. ``None``
    (default) disables it. Requires a registered counter backend resolvable on
    :attr:`lockout_counter_route`."""

    lockout_counter_route: StrKey = attrs.field(default=LOCKOUT_COUNTER_ROUTE)
    """Counter route the lockout guard resolves its ``CounterPort`` from."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        # The annotation is not enforced at runtime: anything else must fail closed here,
        # never fall through to a permissive gate.
        valid = (
            self.eligibility in _ELIGIBILITY_NAMES
            if isinstance(self.eligibility, str)
            else callable(self.eligibility)
        )

        if not valid:
            raise exc.configuration(
                f"Unknown eligibility {self.eligibility!r}: expected 'policy_principal', "
                "'allow_all' or a gate factory called with (ctx, spec).",
                code="authn_eligibility_invalid",
            )

        # However the module is passed, it is constructed, so built ``Deps`` warn too.
        if self.eligibility == "allow_all":
            logger.warning(
                "authn.eligibility.allow_all",
                routes=sorted(str.__str__(route) for route in self._eligibility_routes()),
                detail=(
                    "eligibility='allow_all' checks nobody, so deactivating a principal does "
                    "not stop its authentication; a production posture refuses it unless an "
                    "Exempt names authn_eligibility:<route>"
                ),
            )

        # Every method resolves its principal through the route's one resolver, so a
        # resolver override needs only the route; a verifier override needs its method too.
        refuse_undeclared_overrides(
            self.resolvers, self.authn, field="resolvers", declared_field="authn"
        )
        verifiers: tuple[tuple[str, AuthnMethod, StrKeyMapping[object] | None], ...] = (
            ("token_verifiers", "token", self.token_verifiers),
            ("password_verifiers", "password", self.password_verifiers),
            ("api_key_verifiers", "api_key", self.api_key_verifiers),
        )

        for field, method, routes in verifiers:
            refuse_undeclared_overrides(routes, self.authn, field=field, declared_field="authn")
            unused = sorted(r for r in routes or () if method not in (self.authn or {})[r])

            if unused:
                raise exc.configuration(
                    f"{field} overrides routes {unused!r} whose authn methods do not include "
                    f"{method!r}; add {method!r} there or the override is never registered.",
                )

    # ....................... #

    def _eligibility_routes(self) -> frozenset[StrKey]:
        """Every route the module registers a principal-eligibility gate for."""

        return frozenset(
            (self.authn or {}).keys()
            | _normalize_route_set(self.token_lifecycle)
            | _normalize_route_set(self.password_lifecycle)
            | _normalize_route_set(self.api_key_lifecycle)
            | _normalize_route_set(self.password_account_provisioning)
            | _normalize_route_set(self.password_reset)
            | _normalize_route_set(self.principal_deactivation)
        )

    # ....................... #

    def posture_findings(self) -> tuple[WiringFinding, ...]:
        """One finding per route whose gate is ``eligibility="allow_all"``.

        The same condition the module warns about when constructed, reported so a production
        posture can refuse it. Exempt a route by its target, ``authn_eligibility:<route>``,
        with the reason the deployment checks nobody.
        """

        if self.eligibility != "allow_all":
            return ()

        return tuple(
            WiringFinding(
                source="forze_identity",
                target=f"authn_eligibility:{route}",
                rule=ALLOW_ALL_RULE,
            )
            # The value, not str(): a ``(str, Enum)`` route renders as ``Routes.MAIN``.
            for route in sorted(str.__str__(route) for route in self._eligibility_routes())
        )

    # ....................... #

    def __call__(self) -> Deps:
        authn_map = self.authn or {}
        tl = _normalize_route_set(self.token_lifecycle)
        pl = _normalize_route_set(self.password_lifecycle)
        akl = _normalize_route_set(self.api_key_lifecycle)
        pap = _normalize_route_set(self.password_account_provisioning)
        pr = _normalize_route_set(self.password_reset)
        pd = _normalize_route_set(self.principal_deactivation)

        has_registrations = bool(authn_map or tl or pl or akl or pap or pr or pd)

        if not has_registrations:
            return Deps()

        if self.kernel is None:
            raise exc.internal("kernel is required when registering authn dependency routes")

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
            password_reset=pr,
            api_key_verifier_overrides=api_key_overrides_keys,
            token_verifier_overrides=token_overrides_keys,
        )

        merged: Deps = Deps()

        eligibility_routes = self._eligibility_routes()
        if eligibility_routes:
            # The field is the only way to choose the gate: the routed registration here
            # conflicts with any later one for the same route, so the default is
            # non-overridable rather than merely default, and a gate other than the
            # policy-principal one is passed here or not at all.
            eligibility_factory: PrincipalEligibilityDepPort

            if self.eligibility == "policy_principal":
                eligibility_factory = ConfigurablePolicyPrincipalEligibility(shared=shared)

            elif self.eligibility == "allow_all":
                eligibility_factory = ConfigurableAllowAllEligibility()

            else:
                eligibility_factory = self.eligibility

            merged = merged.merge(
                Deps.routed(
                    {
                        PrincipalEligibilityDepKey: dict.fromkeys(
                            eligibility_routes, eligibility_factory
                        ),
                    },
                ),
            )

        # One shared sink for every wired route (observability is cross-cutting,
        # like actor_claim); override per route by merging an extra routed
        # AuthnEventSinkDepKey registration after this module's output.
        if self.events is not None and eligibility_routes:
            merged = merged.merge(
                Deps.routed(
                    {
                        AuthnEventSinkDepKey: dict.fromkeys(eligibility_routes, self.events),
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
                                shared=shared,
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
                    lockout=self.lockout if "password" in methods_fs else None,
                    lockout_counter_route=self.lockout_counter_route,
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
                            name: ConfigurableTokenLifecycle(shared=shared) for name in tl
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
                            name: ConfigurableApiKeyLifecycle(
                                shared=shared, authz_route=self.authz_route
                            )
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

        if pr:
            merged = merged.merge(
                Deps.routed(
                    {
                        PasswordResetDepKey: {
                            name: ConfigurablePasswordReset(
                                shared=shared,
                                revoke_sessions_on_reset=(self.revoke_sessions_on_reset),
                            )
                            for name in pr
                        },
                    },
                ),
            )

        return merged
