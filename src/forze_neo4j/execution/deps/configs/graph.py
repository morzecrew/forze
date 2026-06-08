"""Configuration for a Neo4j-backed graph module route."""

from typing import final

import attrs

from forze.application.contracts.tenancy import TenantAwareIntegrationConfig

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class Neo4jGraphConfig(TenantAwareIntegrationConfig):
    """Wiring config for a graph module on Neo4j.

    Inherits ``tenant_aware`` from :class:`TenantAwareIntegrationConfig`. Tenant
    isolation (v1) is property partition: ``tenant_property`` is stamped on writes and
    matched on anchor nodes.
    """

    database: str | None = None
    """Default Neo4j database for this route (``None`` uses the client default)."""

    tenant_property: str = "tenant_id"
    """Vertex/edge property carrying the tenant id when ``tenant_aware``."""
