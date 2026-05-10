from typing import final
from uuid import UUID

import attrs

from forze.application.contracts.authn import (
    AuthnIdentity,
    PrincipalResolverPort,
    VerifiedAssertion,
)
from forze.base.errors import AuthenticationError

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class JwtNativeUuidResolver(PrincipalResolverPort):
    """Trust :attr:`VerifiedAssertion.subject` as an internal Forze :class:`UUID`.

    Fastest first-party path: when the verifier is :class:`ForzeJwtTokenVerifier`,
    :class:`Argon2PasswordVerifier`, or :class:`HmacApiKeyVerifier`, the subject is already
    an internal principal id rendered as a string. Optional ``tenant_hint`` is interpreted
    as a UUID when present.

    Use :class:`MappingTableResolver` or :class:`DeterministicUuidResolver` for assertions
    coming from external IdPs whose ``subject`` is not a Forze UUID.
    """

    # ....................... #

    async def resolve(self, assertion: VerifiedAssertion) -> AuthnIdentity:
        try:
            principal_id = UUID(assertion.subject)

        except ValueError as exc:
            raise AuthenticationError(
                "Subject is not a valid UUID; use a different resolver",
                code="invalid_principal_subject",
            ) from exc

        tenant_id: UUID | None = None

        if assertion.tenant_hint is not None:
            try:
                tenant_id = UUID(assertion.tenant_hint)

            except ValueError as exc:
                raise AuthenticationError(
                    "Tenant hint is not a valid UUID; use a different resolver",
                    code="invalid_tenant_hint",
                ) from exc

        return AuthnIdentity(principal_id=principal_id, tenant_id=tenant_id)
