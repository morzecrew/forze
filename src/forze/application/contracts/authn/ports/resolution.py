from typing import Awaitable, Protocol

from ..value_objects import AuthnIdentity, VerifiedAssertion

# ----------------------- #


class PrincipalResolverPort(Protocol):
    """Map a :class:`VerifiedAssertion` to a canonical :class:`AuthnIdentity`.

    Resolvers encapsulate the deployment-specific policy: trust the external subject as a
    UUID, look it up in a registry table, derive a deterministic UUID via
    :func:`forze.base.primitives.uuid4` over ``(issuer, subject)``, or just-in-time
    provision a new principal. Multiple resolvers can co-exist behind the same orchestrator
    when more than one IdP is wired for a route.
    """

    def resolve(
        self,
        assertion: VerifiedAssertion,  # noqa: F841
    ) -> Awaitable[AuthnIdentity]:
        """Resolve a verified assertion to an internal :class:`AuthnIdentity`."""
        ...
