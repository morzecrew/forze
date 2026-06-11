"""Credential-family orchestration over the core authn contracts.

:class:`AuthnOrchestrator` is pure port composition — verifier-per-credential-family
plus a single principal resolver and an eligibility gate, with no crypto or backend
dependencies — so it lives in the integrations layer (the home of port-composition
adapters built only from ``forze.application.contracts``, like the staging outbox
command). Keeping it in core lets every adapter plane (``forze_identity`` and
``forze_mock`` alike) run real authn flows; ``forze_identity.authn`` remains its
user-facing facade and re-exports it permanently.
"""

from collections.abc import Mapping
from typing import Any, Final, cast, final

import attrs

from forze.application.contracts.authn import (
    AccessTokenCredentials,
    ApiKeyCredentials,
    ApiKeyVerifierPort,
    AuthnIdentity,
    AuthnPort,
    AuthnResult,
    AuthnSpec,
    PasswordCredentials,
    PasswordVerifierPort,
    PrincipalEligibilityPort,
    PrincipalResolverPort,
    TokenVerifierPort,
    VerifiedAssertion,
)
from forze.base.exceptions import exc

# ----------------------- #

_MAX_ACTOR_CHAIN_DEPTH: Final = 10
"""Bound on the RFC 8693 ``act`` delegation chain depth (defends against a deeply nested
``act`` claim driving unbounded recursion and resolver/eligibility query amplification)."""

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnOrchestrator(AuthnPort):
    """Compose a verifier per credential family and a single principal resolver.

    Built once per authn route by a configurable factory (e.g.
    :class:`~forze_authn.execution.deps.deps.ConfigurableAuthn` for the identity plane,
    or the mock module's authn factory) from the matching dep keys. ``enabled_methods``
    mirrors :attr:`AuthnSpec.enabled_methods` and gates each ``authenticate_with_*``
    method: invoking a disabled method raises :class:`AuthenticationError` instead of
    producing a half-defined identity.
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

    eligibility: PrincipalEligibilityPort
    """Principal eligibility gate applied after credential verification."""

    actor_claim: str | None = attrs.field(default=None)
    """When set (e.g. ``"act"``), the token's claim of this name is read as an RFC 8693
    delegation assertion: the on-behalf-of **actor** is resolved through the same principal
    resolver and attached as :attr:`AuthnIdentity.actor`. ``None`` (default) ignores any such
    claim. Only the token path honors it — password/API-key assertions carry no actor."""

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
        eligibility: PrincipalEligibilityPort,
        password_verifier: PasswordVerifierPort | None = None,
        token_verifier: TokenVerifierPort | None = None,
        api_key_verifier: ApiKeyVerifierPort | None = None,
        actor_claim: str | None = None,
    ) -> "AuthnOrchestrator":
        """Convenience factory mirroring :class:`AuthnSpec` semantics."""

        return cls(
            resolver=resolver,
            eligibility=eligibility,
            enabled_methods=frozenset(spec.enabled_methods),
            password_verifier=password_verifier,
            token_verifier=token_verifier,
            api_key_verifier=api_key_verifier,
            actor_claim=actor_claim,
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
        await self.eligibility.require_authentication_allowed(identity.principal_id)

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
        await self.eligibility.require_authentication_allowed(identity.principal_id)

        if self.actor_claim is not None:
            act = assertion.claims.get(self.actor_claim)

            if isinstance(act, Mapping):
                identity = attrs.evolve(
                    identity,
                    actor=await self._resolve_actor(
                        assertion, cast("Mapping[str, Any]", act)
                    ),
                )

        return AuthnResult(
            identity=identity,
            issuer_tenant_hint=assertion.issuer_tenant_hint,
        )

    # ....................... #

    async def _resolve_actor(
        self,
        parent: VerifiedAssertion,
        act: Mapping[str, Any],
        depth: int = 0,
    ) -> AuthnIdentity:
        """Resolve a delegation actor from an RFC 8693 ``act`` claim (chainable).

        The actor is asserted in the *same* issuer's namespace, so a derived assertion is
        resolved through the same :class:`PrincipalResolverPort`. A nested ``act`` (multi-hop
        delegation) recurses, building the actor chain on :attr:`AuthnIdentity.actor` — bounded
        by :data:`_MAX_ACTOR_CHAIN_DEPTH` to cap recursion and resolver/eligibility calls.
        """

        if depth >= _MAX_ACTOR_CHAIN_DEPTH:
            raise exc.authentication(
                "Delegation actor chain exceeds the maximum depth "
                f"({_MAX_ACTOR_CHAIN_DEPTH})",
                code="actor_chain_too_deep",
            )

        actor_subject = act.get("sub")

        if not isinstance(actor_subject, str):
            raise exc.authentication(
                "Delegation actor claim is missing a string 'sub'",
                code="invalid_actor_claim",
            )

        actor_assertion = attrs.evolve(
            parent,
            subject=actor_subject,
            claims=act,
        )
        actor = await self.resolver.resolve(actor_assertion)
        await self.eligibility.require_authentication_allowed(actor.principal_id)

        nested = act.get(self.actor_claim) if self.actor_claim is not None else None

        if isinstance(nested, Mapping):
            actor = attrs.evolve(
                actor,
                actor=await self._resolve_actor(
                    actor_assertion, cast("Mapping[str, Any]", nested), depth + 1
                ),
            )

        return actor

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
        await self.eligibility.require_authentication_allowed(identity.principal_id)

        return AuthnResult(
            identity=identity,
            issuer_tenant_hint=assertion.issuer_tenant_hint,
        )
