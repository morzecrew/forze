"""HTTP service execution configs."""

import base64
from collections.abc import Callable, Mapping
from datetime import timedelta
from typing import Literal, final
from uuid import UUID

import attrs
from pydantic import SecretStr

from forze.application.contracts.egress import (
    HTTP_EGRESS_UNACKNOWLEDGED,
    require_egress_acknowledged,
)
from forze.application.contracts.secrets import SecretRef
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig
from forze.base.exceptions import exc
from forze.base.serialization.pydantic import pydantic_secret_converter

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class HttpAuthConfig:
    """Static authentication applied to every request for a service route."""

    kind: Literal["bearer", "api_key", "header", "basic"] = "bearer"
    """Authentication style."""

    token: SecretStr | None = attrs.field(
        default=None,
        converter=attrs.converters.optional(pydantic_secret_converter),
        repr=False,
    )
    """Bearer token or API key value."""

    header_name: str = "Authorization"
    """Header name for ``api_key`` / ``header`` kinds."""

    prefix: str = "Bearer "
    """Value prefix for bearer tokens."""

    username: str | None = None
    """Client identifier for ``basic`` — the user half of HTTP Basic (RFC 7617).

    Cannot contain ``":"``: the decoded pair is split at the first colon, so a user-id
    carrying one moves the boundary and presents a different credential than the one
    written here."""

    password: SecretStr | None = attrs.field(
        default=None,
        converter=attrs.converters.optional(pydantic_secret_converter),
        repr=False,
    )
    """Client secret for ``basic``. Excluded from ``repr`` so a config dump cannot leak it."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        # Refused rather than silently unauthenticated: a half-declared Basic credential
        # would send no Authorization header at all, and the failure surfaces at the
        # counterparty as a rejected request rather than here as the wiring mistake it is.
        # The token-based kinds keep their existing behaviour — an absent token there has
        # always meant "no auth", and wiring depends on it.
        if self.kind != "basic":
            return

        if self.username is None or self.password is None:
            raise exc.configuration(
                "HttpAuthConfig: kind='basic' requires both username and password",
            )

        # RFC 7617 splits the decoded pair at the *first* colon, so a colon in the user-id
        # silently moves the boundary: "a:b" with password "c" decodes as user "a" with
        # password "b:c". The counterparty then rejects a credential that looks correct in
        # the config, which is the worst way for this to fail.
        if ":" in self.username:
            raise exc.configuration(
                "HttpAuthConfig: a basic user-id cannot contain ':' (RFC 7617)",
            )

    # ....................... #

    def auth_headers(self) -> dict[str, str]:
        """Headers to merge for this auth configuration."""

        if self.kind == "basic":
            # Both halves are present — the post-init refuses anything else.
            secret = self.password.get_secret_value() if self.password is not None else ""
            blob = base64.b64encode(f"{self.username}:{secret}".encode()).decode("ascii")

            return {self.header_name: f"Basic {blob}"}

        if self.token is None:
            return {}

        value = self.token.get_secret_value()

        match self.kind:
            case "bearer":
                return {self.header_name: f"{self.prefix}{value}"}

            case "api_key" | "header":
                return {self.header_name: value}

            case "basic":  # pragma: no cover - handled above, kept for exhaustiveness
                return {}


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class HttpServiceConfig(TenantAwareIntegrationConfig):
    """Infrastructure wiring for an :class:`~forze.application.contracts.http.HttpServiceSpec` route."""

    base_url: str | None = None
    """Static service base URL (non-tenant deployments)."""

    secret_ref_for_tenant: Callable[[UUID], SecretRef] | Mapping[UUID, SecretRef] | None = None
    """Per-tenant secret refs resolving :class:`~forze_http.kernel.client.HttpRoutingCredentials`."""

    timeout: timedelta = attrs.field(default=timedelta(seconds=30))
    """Per-request timeout override."""

    default_headers: dict[str, str] = attrs.field(factory=dict)
    """Headers merged into every request."""

    propagate_deadline: bool = attrs.field(default=True)
    """Attach the caller's remaining time budget as the
    ``X-Forze-Deadline-Budget`` header on every request when an invocation
    deadline is bound (no-op otherwise). Harmless to non-Forze receivers (an
    ignored header carrying only a duration); a Forze receiver honors it only
    behind its own opt-in (``bind_deadline_from_header``), and binding is
    tighten-only either way."""

    auth: HttpAuthConfig | None = None
    """Optional static authentication."""

    egress_sensitive: bool = False
    """This route sends business, personal or prompt data outside the trust boundary.

    A **data fact**, declared by the application: it describes what the route carries, not
    whether anyone approved it. Left ``False`` (the default) nothing changes — no gate, no
    span attribute, no behaviour difference for any existing wiring."""

    acknowledge_data_egress: bool = False
    """The operator accepts that the data named by :attr:`egress_sensitive` leaves.

    An **operator act**, separate from the declaration on purpose: a route that carries
    sensitive data but has not been acknowledged fails at wiring rather than shipping the
    data and noting it somewhere."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.timeout.total_seconds() <= 0:
            raise exc.configuration("Timeout must be positive")

        require_egress_acknowledged(
            subject="HttpServiceConfig",
            detail=(
                "this route is declared egress_sensitive=True, so it sends data outside "
                "the trust boundary and the operator must state that consciously."
            ),
            egress_sensitive=self.egress_sensitive,
            acknowledged=self.acknowledge_data_egress,
            code=HTTP_EGRESS_UNACKNOWLEDGED,
        )

        if self.tenant_aware:
            if self.base_url is not None:
                raise exc.configuration(
                    "HttpServiceConfig: set base_url on tenant secrets when "
                    "tenant_aware=True, not on the config",
                )

            return

        if self.base_url is None:
            raise exc.configuration(
                "HttpServiceConfig: base_url is required when tenant_aware=False",
            )

        if self.secret_ref_for_tenant is not None:
            raise exc.configuration(
                "HttpServiceConfig: secret_ref_for_tenant applies only when tenant_aware=True",
            )
