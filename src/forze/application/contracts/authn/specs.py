from typing import Literal

import attrs

from ..base import BaseSpec

# ----------------------- #

AuthnMethod = Literal["password", "token", "api_key"]
"""Supported credential families for an authn route."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnSpec(BaseSpec):
    """Specification for authentication behavior on a named route.

    The spec advertises which credential families a route accepts and which named
    verifier/resolver profiles to use; resolving the actual implementations is the dep
    layer's job. Profiles are how external IdPs (Firebase, Casdoor, generic OIDC) plug in
    without changing this contract: the deps module registers a ``TokenVerifierPort`` under
    a profile name and the spec just references that name.
    """

    enabled_methods: frozenset[AuthnMethod] = attrs.field(
        factory=lambda: frozenset({"token"}),
    )
    """Credential families enabled for this route. ``AuthnPort`` raises when invoked with a disabled method."""

    token_profile: str | None = attrs.field(default=None)
    """Optional token-verifier profile name; ``None`` means "use the route's default token verifier"."""

    password_profile: str | None = attrs.field(default=None)
    """Optional password-verifier profile name."""

    api_key_profile: str | None = attrs.field(default=None)
    """Optional API-key-verifier profile name."""

    resolver_profile: str | None = attrs.field(default=None)
    """Optional principal-resolver profile name; ``None`` means "use the route's default resolver"."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.enabled_methods:
            raise ValueError(
                "AuthnSpec.enabled_methods must contain at least one credential family",
            )
