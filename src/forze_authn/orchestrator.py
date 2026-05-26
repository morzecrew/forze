from typing import final

import attrs

from forze.application.contracts.authn import (
    AccessTokenCredentials,
    ApiKeyCredentials,
    ApiKeyVerifierPort,
    AuthnPort,
    AuthnResult,
    AuthnSpec,
    PasswordCredentials,
    PasswordVerifierPort,
    PrincipalResolverPort,
    TokenVerifierPort,
)
from forze.base.exceptions import exc

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnOrchestrator(AuthnPort):
    """Compose a verifier per credential family and a single principal resolver.

    Built once per authn route by :class:`~forze_authn.execution.deps.deps.ConfigurableAuthn`
    from the matching dep keys. ``enabled_methods`` mirrors :attr:`AuthnSpec.enabled_methods`
    and gates each ``authenticate_with_*`` method: invoking a disabled method raises
    :class:`AuthenticationError` instead of producing a half-defined identity.
    """

    resolver: PrincipalResolverPort
    """Principal resolver used by all enabled credential families on this route."""

    enabled_methods: frozenset[str]
    """Snapshot of :attr:`AuthnSpec.enabled_methods` for the route."""

    password_verifier: PasswordVerifierPort | None = attrs.field(default=None)
    """Optional password verifier; required when ``"password"`` is enabled."""

    token_verifier: TokenVerifierPort | None = attrs.field(default=None)
    """Optional token verifier; required when ``"token"`` is enabled."""

    api_key_verifier: ApiKeyVerifierPort | None = attrs.field(default=None)
    """Optional API key verifier; required when ``"api_key"`` is enabled."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if "password" in self.enabled_methods and self.password_verifier is None:
            raise exc.internal(
                "AuthnOrchestrator: 'password' is enabled but no PasswordVerifierPort was wired",
            )

        if "token" in self.enabled_methods and self.token_verifier is None:
            raise exc.internal(
                "AuthnOrchestrator: 'token' is enabled but no TokenVerifierPort was wired",
            )

        if "api_key" in self.enabled_methods and self.api_key_verifier is None:
            raise exc.internal(
                "AuthnOrchestrator: 'api_key' is enabled but no ApiKeyVerifierPort was wired",
            )

    # ....................... #

    @classmethod
    def from_spec(
        cls,
        spec: AuthnSpec,
        *,
        resolver: PrincipalResolverPort,
        password_verifier: PasswordVerifierPort | None = None,
        token_verifier: TokenVerifierPort | None = None,
        api_key_verifier: ApiKeyVerifierPort | None = None,
    ) -> "AuthnOrchestrator":
        """Convenience factory mirroring :class:`AuthnSpec` semantics."""

        return cls(
            resolver=resolver,
            enabled_methods=frozenset(spec.enabled_methods),
            password_verifier=password_verifier,
            token_verifier=token_verifier,
            api_key_verifier=api_key_verifier,
        )

    # ....................... #

    async def authenticate_with_password(
        self,
        credentials: PasswordCredentials,
    ) -> AuthnResult:
        if "password" not in self.enabled_methods or self.password_verifier is None:
            raise exc.authentication(
                "Password authentication is not enabled for this route",
                code="method_disabled",
            )

        assertion = await self.password_verifier.verify_password(credentials)
        identity = await self.resolver.resolve(assertion)

        return AuthnResult(
            identity=identity,
            issuer_tenant_hint=assertion.issuer_tenant_hint,
        )

    # ....................... #

    async def authenticate_with_token(
        self,
        credentials: AccessTokenCredentials,
    ) -> AuthnResult:
        if "token" not in self.enabled_methods or self.token_verifier is None:
            raise exc.authentication(
                "Token authentication is not enabled for this route",
                code="method_disabled",
            )

        assertion = await self.token_verifier.verify_token(credentials)
        identity = await self.resolver.resolve(assertion)

        return AuthnResult(
            identity=identity,
            issuer_tenant_hint=assertion.issuer_tenant_hint,
        )

    # ....................... #

    async def authenticate_with_api_key(
        self,
        credentials: ApiKeyCredentials,
    ) -> AuthnResult:
        if "api_key" not in self.enabled_methods or self.api_key_verifier is None:
            raise exc.authentication(
                "API key authentication is not enabled for this route",
                code="method_disabled",
            )

        assertion = await self.api_key_verifier.verify_api_key(credentials)
        identity = await self.resolver.resolve(assertion)

        return AuthnResult(
            identity=identity,
            issuer_tenant_hint=assertion.issuer_tenant_hint,
        )
