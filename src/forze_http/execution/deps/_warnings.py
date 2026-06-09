"""Private tenancy warning descriptors for HTTP deps module."""

from forze.application.contracts.tenancy import IntegrationRouteWarning

from .configs import HttpxHttpServiceConfig

# ----------------------- #

HTTP_SERVICE_WARNING = IntegrationRouteWarning[HttpxHttpServiceConfig](
    kind="http_service",
    tenant_aware=lambda config: config.tenant_aware,
)
