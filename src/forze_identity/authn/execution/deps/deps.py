"""Configurable authn dependency factories resolving document ports from execution context."""

from typing import final

import attrs

from forze.application.contracts.authn import (
    ApiKeyLifecyclePort,
    ApiKeyVerifierDepKey,
    ApiKeyVerifierPort,
    AuthnPort,
    AuthnSpec,
    PasswordAccountProvisioningPort,
    PasswordLifecyclePort,
    PasswordResetPort,
    PasswordVerifierDepKey,
    PasswordVerifierPort,
    PrincipalDeactivationPort,
    PrincipalEligibilityDepKey,
    PrincipalEligibilityPort,
    PrincipalResolverDepKey,
    PrincipalResolverPort,
    TokenLifecycleDepKey,
    TokenLifecyclePort,
    TokenVerifierDepKey,
    TokenVerifierPort,
)
from forze.application.contracts.authz import AuthzSpec
from forze.application.execution import ExecutionContext
from forze.base.exceptions import exc

from forze_identity.authz.application import policy_principal_spec

from ...adapters import (
    ApiKeyLifecycleAdapter,
    PasswordAccountProvisioningAdapter,
    PasswordLifecycleAdapter,
    PasswordResetAdapter,
    TokenLifecycleAdapter,
)
from ...adapters.credential_deactivation import AuthnCredentialDeactivationHelper
from ...adapters.principal_deactivation import PrincipalDeactivationAdapter
from ...adapters.principal_eligibility import PolicyPrincipalEligibilityAdapter
from ...application.specs import (
    api_key_account_spec,
    identity_mapping_spec,
    password_account_spec,
    password_invite_spec,
    password_reset_spec,
    session_spec,
)
from ...orchestrator import AuthnOrchestrator
from ...resolvers import (
    DeterministicUuidResolver,
    JwtNativeUuidResolver,
    MappingTableResolver,
)
from ...verifiers import (
    Argon2PasswordVerifier,
    ForzeJwtTokenVerifier,
    HmacApiKeyVerifier,
)
from forze.base.primitives import StrKey

from .configs import AuthnSharedServices

# ----------------------- #


def _resolve_eligibility(
    ctx: ExecutionContext,
    spec: AuthnSpec,
) -> PrincipalEligibilityPort:
    return ctx.deps.provide(PrincipalEligibilityDepKey, route=spec.name)(ctx, spec)


# ----------------------- #
# Verifier factories


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableArgon2PasswordVerifier:
    """Build :class:`Argon2PasswordVerifier` from the shared password service + ``ctx`` document port."""

    shared: AuthnSharedServices

    rehash_on_login: bool = attrs.field(default=False)
    """When ``True``, wire the password account command port so stored hashes are
    upgraded to current Argon2 parameters after a successful login."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> PasswordVerifierPort:
        _ = spec

        if self.shared.password_svc is None:
            raise exc.internal(
                "Password verifier requires kernel.password",
            )

        pa_cmd = (
            ctx.doc.command(password_account_spec) if self.rehash_on_login else None
        )

        return Argon2PasswordVerifier(
            password_svc=self.shared.password_svc,
            pa_qry=ctx.doc.query(password_account_spec),
            pa_cmd=pa_cmd,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableForzeJwtTokenVerifier:
    """Build :class:`ForzeJwtTokenVerifier` from the shared access-token service.

    Wires :attr:`ForzeJwtTokenVerifier.session_qry` so lifecycle-issued access JWTs
    (``sid`` claim) are invalidated on logout and refresh rotation.
    """

    shared: AuthnSharedServices

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> TokenVerifierPort:
        _ = spec

        if self.shared.access_svc is None:
            raise exc.internal(
                "Forze JWT token verifier requires kernel.access_token_secret",
            )

        return ForzeJwtTokenVerifier(
            access_svc=self.shared.access_svc,
            session_qry=ctx.doc.query(session_spec),
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableHmacApiKeyVerifier:
    """Build :class:`HmacApiKeyVerifier` from the shared API key service + ``ctx`` document port."""

    shared: AuthnSharedServices

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> ApiKeyVerifierPort:
        _ = spec

        if self.shared.api_key_svc is None:
            raise exc.internal(
                "API key verifier requires kernel.api_key_pepper",
            )

        return HmacApiKeyVerifier(
            api_key_svc=self.shared.api_key_svc,
            ak_qry=ctx.doc.query(api_key_account_spec),
        )


# ....................... #
# Resolver factories


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableJwtNativeUuidResolver:
    """Build :class:`JwtNativeUuidResolver` (no execution-context dependencies)."""

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> PrincipalResolverPort:
        _ = ctx, spec
        return JwtNativeUuidResolver()


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableDeterministicUuidResolver:
    """Build :class:`DeterministicUuidResolver` (no execution-context dependencies)."""

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> PrincipalResolverPort:
        _ = ctx, spec
        return DeterministicUuidResolver()


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableMappingTableResolver:
    """Build :class:`MappingTableResolver` from the identity-mapping document ports."""

    provision_on_first_sight: bool = False
    """Whether the resolver mints new principal ids on unknown subjects."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> PrincipalResolverPort:
        _ = spec

        cmd = (
            ctx.doc.command(identity_mapping_spec)
            if self.provision_on_first_sight
            else None
        )

        return MappingTableResolver(
            qry=ctx.doc.query(identity_mapping_spec),
            cmd=cmd,
            provision_on_first_sight=self.provision_on_first_sight,
        )


# ....................... #
# Orchestrator factory


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableAuthn:
    """Build :class:`AuthnOrchestrator` by composing per-method verifiers + resolver from deps."""

    enabled_methods: frozenset[str]
    """Snapshot of :attr:`AuthnSpec.enabled_methods`; pre-baked at registration time."""

    actor_claim: str | None = attrs.field(default=None)
    """Token claim carrying the RFC 8693 delegation actor; ``None`` ignores delegation."""

    # ....................... #

    def __call__(self, ctx: ExecutionContext, spec: AuthnSpec) -> AuthnPort:
        # Re-validate against the live spec to guard against route name reuse with
        # mismatched method sets (the registration snapshot wins).
        if frozenset(spec.enabled_methods) != self.enabled_methods:
            raise exc.internal(
                "AuthnSpec.enabled_methods does not match the enabled_methods registered "
                f"for route '{spec.name}'",
            )

        password_v: PasswordVerifierPort | None = None
        token_v: TokenVerifierPort | None = None
        api_key_v: ApiKeyVerifierPort | None = None

        if "password" in self.enabled_methods:
            password_v = ctx.deps.provide(PasswordVerifierDepKey, route=spec.name)(
                ctx, spec
            )

        if "token" in self.enabled_methods:
            token_v = ctx.deps.provide(TokenVerifierDepKey, route=spec.name)(ctx, spec)

        if "api_key" in self.enabled_methods:
            api_key_v = ctx.deps.provide(ApiKeyVerifierDepKey, route=spec.name)(
                ctx, spec
            )

        resolver = ctx.deps.provide(PrincipalResolverDepKey, route=spec.name)(ctx, spec)
        eligibility = _resolve_eligibility(ctx, spec)

        return AuthnOrchestrator(
            resolver=resolver,
            eligibility=eligibility,
            enabled_methods=self.enabled_methods,
            password_verifier=password_v,
            token_verifier=token_v,
            api_key_verifier=api_key_v,
            actor_claim=self.actor_claim,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePolicyPrincipalEligibility:
    """Build :class:`PolicyPrincipalEligibilityAdapter` from policy principal documents."""

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> PrincipalEligibilityPort:
        _ = spec
        return PolicyPrincipalEligibilityAdapter(
            principal_qry=ctx.doc.query(policy_principal_spec),
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePrincipalDeactivation:
    """Build :class:`PrincipalDeactivationAdapter` for cascaded offboarding."""

    authz_route: StrKey
    """Authz route name used to resolve :class:`PrincipalRegistryPort`."""

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> PrincipalDeactivationPort:
        registry = ctx.authz.principal_registry(AuthzSpec(name=self.authz_route))
        token_lifecycle = ctx.deps.provide(TokenLifecycleDepKey, route=spec.name)(
            ctx,
            spec,
        )
        credentials = AuthnCredentialDeactivationHelper(
            pa_qry=ctx.doc.query(password_account_spec),
            pa_cmd=ctx.doc.command(password_account_spec),
            ak_qry=ctx.doc.query(api_key_account_spec),
            ak_cmd=ctx.doc.command(api_key_account_spec),
        )
        return PrincipalDeactivationAdapter(
            principal_registry=registry,
            token_lifecycle=token_lifecycle,
            credentials=credentials,
        )


# ....................... #
# Lifecycle / provisioning factories (unchanged shapes)


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableTokenLifecycle:
    """Build :class:`TokenLifecycleAdapter`."""

    shared: AuthnSharedServices

    # ....................... #

    def __call__(self, ctx: ExecutionContext, spec: AuthnSpec) -> TokenLifecyclePort:
        _ = spec

        if self.shared.access_svc is None or self.shared.refresh_svc is None:
            raise exc.internal(
                "Token lifecycle requires kernel.access_token_secret and kernel.refresh_token_pepper",
            )

        return TokenLifecycleAdapter(
            access_svc=self.shared.access_svc,
            refresh_svc=self.shared.refresh_svc,
            session_qry=ctx.doc.query(session_spec),
            session_cmd=ctx.doc.command(session_spec),
            eligibility=_resolve_eligibility(ctx, spec),
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePasswordLifecycle:
    """Build :class:`PasswordLifecycleAdapter`."""

    shared: AuthnSharedServices

    revoke_sessions_on_password_change: bool = attrs.field(default=True)
    """Whether a successful password change revokes all of the principal's sessions
    ("log out everywhere"); wires the session document ports when enabled."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> PasswordLifecyclePort:
        _ = spec

        if self.shared.password_svc is None:
            raise exc.internal("Password lifecycle requires kernel.password")

        revoke = self.revoke_sessions_on_password_change

        return PasswordLifecycleAdapter(
            password_svc=self.shared.password_svc,
            pa_qry=ctx.doc.query(password_account_spec),
            pa_cmd=ctx.doc.command(password_account_spec),
            eligibility=_resolve_eligibility(ctx, spec),
            session_qry=ctx.doc.query(session_spec) if revoke else None,
            session_cmd=ctx.doc.command(session_spec) if revoke else None,
            revoke_sessions_on_password_change=revoke,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePasswordReset:
    """Build :class:`PasswordResetAdapter`."""

    shared: AuthnSharedServices

    revoke_sessions_on_reset: bool = attrs.field(default=True)
    """Whether a successful reset revokes all of the principal's sessions
    ("log out everywhere"); wires the session document ports when enabled."""

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> PasswordResetPort:
        _ = spec

        if self.shared.password_svc is None:
            raise exc.internal("Password reset requires kernel.password")

        if self.shared.reset_svc is None:
            raise exc.internal("Password reset requires kernel.reset_token_pepper")

        revoke = self.revoke_sessions_on_reset

        return PasswordResetAdapter(
            password_svc=self.shared.password_svc,
            reset_svc=self.shared.reset_svc,
            pa_qry=ctx.doc.query(password_account_spec),
            pa_cmd=ctx.doc.command(password_account_spec),
            reset_qry=ctx.doc.query(password_reset_spec),
            reset_cmd=ctx.doc.command(password_reset_spec),
            eligibility=_resolve_eligibility(ctx, spec),
            session_qry=ctx.doc.query(session_spec) if revoke else None,
            session_cmd=ctx.doc.command(session_spec) if revoke else None,
            revoke_sessions_on_reset=revoke,
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableApiKeyLifecycle:
    """Build :class:`ApiKeyLifecycleAdapter`."""

    shared: AuthnSharedServices

    # ....................... #

    def __call__(self, ctx: ExecutionContext, spec: AuthnSpec) -> ApiKeyLifecyclePort:
        _ = spec

        if self.shared.api_key_svc is None:
            raise exc.internal("API key lifecycle requires kernel.api_key_pepper")

        return ApiKeyLifecycleAdapter(
            api_key_svc=self.shared.api_key_svc,
            ak_qry=ctx.doc.query(api_key_account_spec),
            ak_cmd=ctx.doc.command(api_key_account_spec),
            eligibility=_resolve_eligibility(ctx, spec),
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePasswordAccountProvisioning:
    """Build :class:`PasswordAccountProvisioningAdapter`."""

    shared: AuthnSharedServices

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> PasswordAccountProvisioningPort:
        _ = spec

        if self.shared.password_svc is None:
            raise exc.internal("Password provisioning requires kernel.password")

        invite_svc = self.shared.invite_svc
        invite_qry = (
            ctx.doc.query(password_invite_spec) if invite_svc is not None else None
        )
        invite_cmd = (
            ctx.doc.command(password_invite_spec) if invite_svc is not None else None
        )

        return PasswordAccountProvisioningAdapter(
            password_svc=self.shared.password_svc,
            password_account_qry=ctx.doc.query(password_account_spec),
            password_account_cmd=ctx.doc.command(password_account_spec),
            eligibility=_resolve_eligibility(ctx, spec),
            invite_svc=invite_svc,
            invite_qry=invite_qry,
            invite_cmd=invite_cmd,
        )
