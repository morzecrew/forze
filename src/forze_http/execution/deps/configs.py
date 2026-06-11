"""HTTP service execution configs."""

from collections.abc import Callable, Mapping
from datetime import timedelta
from typing import Literal, final
from uuid import UUID

import attrs
from pydantic import SecretStr

from forze.application.contracts.secrets import SecretRef
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig
from forze.base.exceptions import exc
from forze.base.serialization.pydantic import pydantic_secret_converter

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class HttpAuthConfig:
    """Static authentication applied to every request for a service route."""

    kind: Literal["bearer", "api_key", "header"] = "bearer"
    """Authentication style."""

    token: SecretStr | None = attrs.field(
        default=None,
        converter=pydantic_secret_converter,
        repr=False,
    )
    """Bearer token or API key value."""

    header_name: str = "Authorization"
    """Header name for ``api_key`` / ``header`` kinds."""

    prefix: str = "Bearer "
    """Value prefix for bearer tokens."""

    # ....................... #

    def auth_headers(self) -> dict[str, str]:
        """Headers to merge for this auth configuration."""

        if self.token is None:
            return {}

        value = self.token.get_secret_value()

        match self.kind:
            case "bearer":
                return {self.header_name: f"{self.prefix}{value}"}

            case "api_key" | "header":
                return {self.header_name: value}


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class HttpServiceConfig(TenantAwareIntegrationConfig):
    """Infrastructure wiring for an :class:`~forze.application.contracts.http.HttpServiceSpec` route."""

    base_url: str | None = None
    """Static service base URL (non-tenant deployments)."""

    secret_ref_for_tenant: (
        Callable[[UUID], SecretRef] | Mapping[UUID, SecretRef] | None
    ) = None
    """Per-tenant secret refs resolving :class:`~forze_http.kernel.client.HttpRoutingCredentials`."""

    timeout: timedelta = attrs.field(default=timedelta(seconds=30))
    """Per-request timeout override."""

    default_headers: dict[str, str] = attrs.field(factory=dict)
    """Headers merged into every request."""

    auth: HttpAuthConfig | None = None
    """Optional static authentication."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.timeout.total_seconds() <= 0:
            raise exc.configuration("Timeout must be positive")

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
                "HttpServiceConfig: secret_ref_for_tenant applies only when "
                "tenant_aware=True",
            )
