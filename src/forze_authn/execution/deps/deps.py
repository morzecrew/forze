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
    PasswordVerifierDepKey,
    PasswordVerifierPort,
    PrincipalResolverDepKey,
    PrincipalResolverPort,
    TokenLifecyclePort,
    TokenVerifierDepKey,
    TokenVerifierPort,
)
from forze.application.execution import ExecutionContext
from forze.base.errors import CoreError

from ...adapters import (
    ApiKeyLifecycleAdapter,
    PasswordAccountProvisioningAdapter,
    PasswordLifecycleAdapter,
    TokenLifecycleAdapter,
)
from ...application.specs import (
    api_key_account_spec,
    identity_mapping_spec,
    password_account_spec,
    principal_spec,
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
from .configs import AuthnSharedServices

# ----------------------- #
# Verifier factories


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableArgon2PasswordVerifier:
    """Build :class:`Argon2PasswordVerifier` from the shared password service + ``ctx`` document port."""

    shared: AuthnSharedServices

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> PasswordVerifierPort:
        _ = spec

        if self.shared.password_svc is None:
            raise CoreError(
                "Password verifier requires kernel.password",
            )

        return Argon2PasswordVerifier(
            password_svc=self.shared.password_svc,
            pa_qry=ctx.doc_query(password_account_spec),
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurableForzeJwtTokenVerifier:
    """Build :class:`ForzeJwtTokenVerifier` from the shared access-token service."""

    shared: AuthnSharedServices

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> TokenVerifierPort:
        _ = ctx, spec

        if self.shared.access_svc is None:
            raise CoreError(
                "Forze JWT token verifier requires kernel.access_token_secret",
            )

        return ForzeJwtTokenVerifier(access_svc=self.shared.access_svc)


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
            raise CoreError(
                "API key verifier requires kernel.api_key_pepper",
            )

        return HmacApiKeyVerifier(
            api_key_svc=self.shared.api_key_svc,
            ak_qry=ctx.doc_query(api_key_account_spec),
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
            ctx.doc_command(identity_mapping_spec)
            if self.provision_on_first_sight
            else None
        )

        return MappingTableResolver(
            qry=ctx.doc_query(identity_mapping_spec),
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

    # ....................... #

    def __call__(self, ctx: ExecutionContext, spec: AuthnSpec) -> AuthnPort:
        # Re-validate against the live spec to guard against route name reuse with
        # mismatched method sets (the registration snapshot wins).
        if frozenset(spec.enabled_methods) != self.enabled_methods:
            raise CoreError(
                "AuthnSpec.enabled_methods does not match the enabled_methods registered "
                f"for route '{spec.name}'",
            )

        password_v: PasswordVerifierPort | None = None
        token_v: TokenVerifierPort | None = None
        api_key_v: ApiKeyVerifierPort | None = None

        if "password" in self.enabled_methods:
            password_v = ctx.dep(PasswordVerifierDepKey, route=spec.name)(ctx, spec)

        if "token" in self.enabled_methods:
            token_v = ctx.dep(TokenVerifierDepKey, route=spec.name)(ctx, spec)

        if "api_key" in self.enabled_methods:
            api_key_v = ctx.dep(ApiKeyVerifierDepKey, route=spec.name)(ctx, spec)

        resolver = ctx.dep(PrincipalResolverDepKey, route=spec.name)(ctx, spec)

        return AuthnOrchestrator(
            resolver=resolver,
            enabled_methods=self.enabled_methods,
            password_verifier=password_v,
            token_verifier=token_v,
            api_key_verifier=api_key_v,
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
            raise CoreError(
                "Token lifecycle requires kernel.access_token_secret and kernel.refresh_token_pepper",
            )

        return TokenLifecycleAdapter(
            access_svc=self.shared.access_svc,
            refresh_svc=self.shared.refresh_svc,
            session_qry=ctx.doc_query(session_spec),
            session_cmd=ctx.doc_command(session_spec),
            principal_qry=ctx.doc_query(principal_spec),
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePasswordLifecycle:
    """Build :class:`PasswordLifecycleAdapter`."""

    shared: AuthnSharedServices

    # ....................... #

    def __call__(
        self,
        ctx: ExecutionContext,
        spec: AuthnSpec,
    ) -> PasswordLifecyclePort:
        _ = spec

        if self.shared.password_svc is None:
            raise CoreError("Password lifecycle requires kernel.password")

        return PasswordLifecycleAdapter(
            password_svc=self.shared.password_svc,
            pa_qry=ctx.doc_query(password_account_spec),
            pa_cmd=ctx.doc_command(password_account_spec),
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
            raise CoreError("API key lifecycle requires kernel.api_key_pepper")

        return ApiKeyLifecycleAdapter(
            api_key_svc=self.shared.api_key_svc,
            ak_qry=ctx.doc_query(api_key_account_spec),
            ak_cmd=ctx.doc_command(api_key_account_spec),
            principal_qry=ctx.doc_query(principal_spec),
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
            raise CoreError("Password provisioning requires kernel.password")

        return PasswordAccountProvisioningAdapter(
            password_svc=self.shared.password_svc,
            password_account_qry=ctx.doc_query(password_account_spec),
            password_account_cmd=ctx.doc_command(password_account_spec),
            principal_qry=ctx.doc_query(principal_spec),
        )
