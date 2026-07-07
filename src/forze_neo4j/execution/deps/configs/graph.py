"""Configuration for a Neo4j-backed graph module route."""

from typing import Literal, final

import attrs

from forze.application.contracts.resolution import (
    NamedResourceSpec,
    coerce_optional_named_resource_spec,
)
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

    database: NamedResourceSpec | None = attrs.field(
        default=None,
        converter=coerce_optional_named_resource_spec,
    )
    """Default Neo4j database for this route.

    A static name, a ``(tenant_id) -> str`` per-tenant resolver (the ``namespace`` tier — a
    per-tenant database on a shared cluster, Neo4j 4+ multi-database), or ``None`` (the
    client default)."""

    tenant_property: str = "tenant_id"
    """Vertex/edge property carrying the tenant id when ``tenant_aware``."""

    traversal_isolation: Literal["anchor", "full-path"] = attrs.field(
        default="full-path",
        validator=attrs.validators.in_(("anchor", "full-path")),
    )
    """How far tenant scoping reaches on ``neighbors``/``expand``/``shortest_path``.

    ``full-path`` (default) constrains every node on the traversal so a cross-tenant edge
    cannot surface a foreign node; ``anchor`` constrains only the start/endpoints (cheaper,
    safe only when no edge ever crosses a tenant boundary).
    """

    allow_raw_query: bool = False
    """Whether the whole-query raw hatch (``ctx.graph.raw``) is permitted.

    Raw is a trusted-caller escape (a caller-written query can read cross-tenant even with
    ``$tenant`` bound), so it is **disabled by default** (fail closed). Set ``True`` to opt in
    where trusted raw Cypher is genuinely needed; otherwise use the structured ports /
    ``scoped_walk``.
    """

    graph_algorithms: bool = False
    """Whether weighted-path queries (``ShortestPathParams.weight_property``) may use the
    Neo4j Graph Data Science (GDS) engine.

    **Off by default** — weighted paths project a tenant-filtered in-memory graph per call, so
    they are opt-in. When off, a weighted request fails closed (``graph_algorithm_unavailable``);
    when on, the query still fails closed with the same code if GDS is not actually installed.
    Unweighted paths never need this.
    """
