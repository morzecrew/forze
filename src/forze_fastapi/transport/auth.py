"""Auth requirement value object for HTTP transport policies."""

from typing import Literal, final

import attrs

from forze.base.exceptions import exc
from forze.base.validators import NoneValidator

# ----------------------- #

AuthnRequirementSchemeName = Literal["bearer", "api_key", "cookie"]
"""OpenAPI scheme classification used by :class:`AuthnRequirement`."""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True, repr=False)
class AuthnRequirement:
    """Declare per-route authentication requirements.

    Exactly one of :attr:`token_header`, :attr:`token_cookie`,
    :attr:`api_key_header` must be set; the chosen field decides which OpenAPI
    security scheme the route exports and which transport the matching
    :class:`~forze_fastapi.middlewares.context.AuthnIdentityResolverPort` is
    expected to read on the incoming request.

    HTTP enforcement is applied by
    :class:`~forze_fastapi.transport.http.policies.RequirePrincipal`
    (principal must be bound on the execution context). Credential extraction
    remains in
    :class:`~forze_fastapi.middlewares.context.ContextBindingMiddleware`.
    """

    authn_route: str
    """:attr:`AuthnSpec.name` to dispatch through (used for OpenAPI scheme naming)."""

    token_header: str | None = attrs.field(default=None)
    """When set, the endpoint documents/expects an ``Authorization``-style header."""

    token_cookie: str | None = attrs.field(default=None)
    """When set, the endpoint documents/expects an access token in a named cookie."""

    api_key_header: str | None = attrs.field(default=None)
    """When set, the endpoint documents/expects an API key header."""

    bearer_format: str = attrs.field(default="JWT")
    """OpenAPI ``bearerFormat`` value (only relevant for token transports)."""

    description: str | None = attrs.field(default=None)
    """Human-readable description rendered into the OpenAPI security scheme."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.authn_route:
            raise exc.internal("AuthnRequirement.authn_route must be non-empty")

        if not NoneValidator.exactly_one(
            self.token_header,
            self.token_cookie,
            self.api_key_header,
        ):
            raise exc.internal(
                "AuthnRequirement requires exactly one of token_header, "
                "token_cookie, api_key_header",
            )

    # ....................... #

    @property
    def scheme_kind(self) -> AuthnRequirementSchemeName:
        """OpenAPI scheme kind derived from the active transport field."""

        if self.token_header is not None:
            return "bearer"

        if self.token_cookie is not None:
            return "cookie"

        return "api_key"

    # ....................... #

    @property
    def scheme_name(self) -> str:
        """OpenAPI ``securitySchemes`` entry name (deterministic, route-scoped)."""

        kind = self.scheme_kind

        return f"forze_authn__{self.authn_route}__{kind}"
