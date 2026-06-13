"""Configuration for a Neo4j-backed graph module route."""

from typing import Literal, final

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

    traversal_isolation: Literal["anchor", "full-path"] = "full-path"
    """How far tenant scoping reaches on ``neighbors``/``expand``/``shortest_path``.

    ``full-path`` (default) constrains every node on the traversal so a cross-tenant edge
    cannot surface a foreign node; ``anchor`` constrains only the start/endpoints (cheaper,
    safe only when no edge ever crosses a tenant boundary).
    """

    allow_raw_query: bool = True
    """Whether the whole-query raw hatch (``ctx.graph.raw``) is permitted.

    Raw is a trusted-caller escape (a caller-written query can read cross-tenant even with
    ``$tenant`` bound). Set ``False`` to fail closed in enforced-tenancy deployments and use
    the structured ports / ``scoped_walk`` instead.
    """
