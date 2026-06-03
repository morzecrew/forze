"""Private tenancy warning descriptors for Meilisearch deps module."""

from forze.application.contracts.tenancy import IntegrationRouteWarning

from .configs import MeilisearchSearchConfig

# ----------------------- #

MEILISEARCH_SEARCH_WARNING = IntegrationRouteWarning[MeilisearchSearchConfig](
    kind="search",
    tenant_aware=lambda config: config.tenant_aware,
    named_fields=lambda config: [("index_uid", config.index_uid)],
)
