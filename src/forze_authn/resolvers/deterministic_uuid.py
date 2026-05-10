from typing import final
from uuid import UUID

import attrs

from forze.application.contracts.authn import (
    AuthnIdentity,
    PrincipalResolverPort,
    VerifiedAssertion,
)
from forze.base.errors import AuthenticationError
from forze.base.primitives import uuid4

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DeterministicUuidResolver(PrincipalResolverPort):
    """Derive a stable :class:`AuthnIdentity` from ``(issuer, subject)`` without storage.

    Uses :func:`forze.base.primitives.uuid4` (deterministic from a SHA-256 hash of the
    serialized input). Always includes ``issuer`` in the input to avoid cross-IdP
    collisions when two providers reuse the same subject string. Stateless and idempotent;
    a good fit for prototyping or read-only scenarios. For deployments that need account
    merging or admin overrides, use :class:`MappingTableResolver` instead.
    """

    # ....................... #

    async def resolve(self, assertion: VerifiedAssertion) -> AuthnIdentity:
        principal_id = uuid4({"iss": assertion.issuer, "sub": assertion.subject})

        tenant_id: UUID | None = None

        if assertion.tenant_hint is not None:
            try:
                tenant_id = UUID(assertion.tenant_hint)

            except ValueError:
                tenant_id = uuid4(
                    {
                        "iss": assertion.issuer,
                        "tid": assertion.tenant_hint,
                    },
                )

        return AuthnIdentity(principal_id=principal_id, tenant_id=tenant_id)


# ....................... #


def derive_principal_id(issuer: str, subject: str) -> UUID:
    """Public helper for tests / migrations to compute the same UUID this resolver emits."""

    if not issuer or not subject:
        raise AuthenticationError("issuer and subject must be non-empty")

    return uuid4({"iss": issuer, "sub": subject})
