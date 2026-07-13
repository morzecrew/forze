---
name: forze-graph-contracts
description: >-
  Models Forze graph contracts with GraphModuleSpec, GraphNodeSpec,
  GraphEdgeSpec, graph refs, query/command ports, and dependency keys. Use when
  adding graph-shaped features, wiring the Neo4j integration (forze_neo4j), or
  building a custom graph adapter in your application.
---

# Forze graph contracts

Use when your application needs vertices, relationships, neighborhood expansion, or shortest-path style queries. Forze ships graph **contracts** in `forze.application.contracts.graph` and an official Neo4j integration (`forze_neo4j`, extra `neo4j`) whose `Neo4jDepsModule(client=..., graphs={module_name: Neo4jGraphConfig(...)})` registers `GraphQueryDepKey` / `GraphCommandDepKey` per graph module. For other engines (Arango-style), implement the ports yourself (see [`forze-custom-deps`](../forze-custom-deps/SKILL.md)).

## Module, node, and edge specs

Use a `GraphModuleSpec` for a bounded graph area. Node and edge kind names are logical names and should come from shared enums.

```python
from enum import StrEnum

from forze.application.contracts.graph import (
    GraphEdgeDirectionality,
    GraphEdgeEndpoint,
    GraphEdgeSpec,
    GraphModuleSpec,
    GraphNodeSpec,
    validate_graph_module_spec,
)


class GraphKind(StrEnum):
    PROJECT_GRAPH = "project-graph"
    PROJECT = "project"
    USER = "user"
    OWNS = "owns"


project_graph = GraphModuleSpec(
    name=GraphKind.PROJECT_GRAPH,
    nodes=(
        GraphNodeSpec(name=GraphKind.PROJECT, read=ProjectNode),
        GraphNodeSpec(name=GraphKind.USER, read=UserNode),
    ),
    edges=(
        GraphEdgeSpec(
            name=GraphKind.OWNS,
            read=OwnsEdge,
            identity="endpoints",  # at most one edge per (from, to) pair
            endpoints=(GraphEdgeEndpoint(from_kind="user", to_kind="project"),),
            directionality=GraphEdgeDirectionality.DIRECTED,
        ),
    ),
)
validate_graph_module_spec(project_graph)
```

`GraphEdgeEndpoint.from_kind` / `to_kind` are strings and must match node kind values in the same module.

Edge identity: the default `identity="key"` addresses each edge by a stable business key and **requires** `key_field` (a field of the edge read model); `ensure_edge` then upserts on that key so concurrent calls cannot create duplicates. `identity="endpoints"` means at most one edge of the kind per `(from, to)` pair, addressed by its endpoints.

## Resolving ports

Use the `ctx.graph` convenience helpers — `query` / `command` (plus `raw` and `management`) resolve routed ports keyed by `GraphModuleSpec.name`.

```python
from forze.application.contracts.graph import GraphDirection, VertexRef

query = ctx.graph.query(project_graph)
owner_rows = await query.neighbors(
    VertexRef(kind="user", key=user_id),
    direction=GraphDirection.OUT,
    edge_kinds=frozenset({"owns"}),
    limit=20,
)

command = ctx.graph.command(project_graph)
created = await command.create_vertex("project", CreateProjectNode(name="Demo"))
```

## Port semantics

`GraphQueryPort` covers `get_vertex`, `get_edge`, existence checks, counts, neighborhood queries, incident edges, expansion, `shortest_path` / `k_shortest_paths` (optionally weighted), scoped walks, and simple find operations.

`GraphCommandPort` covers create/update/delete for vertices and edges, batch creation, and ensure operations. Adapter implementations define stable key semantics through `VertexRef` and `EdgeRef`.

## Adapter guidance

Prefer `forze_neo4j` when Neo4j fits: `Neo4jDepsModule(client=..., graphs={...}, tx={...})` registers query/command (plus raw-query and management) ports per graph module, supports keyed-edge `ensure_edge` identity (`identity="key"` with `key_field`) and native/weighted `k_shortest_paths`, and offers tenant isolation tiers (tagged property, per-tenant database, routed client). For custom adapters, keep Cypher, AQL, and engine-specific query strings inside the adapter and register providers as routed deps under `GraphQueryDepKey` and `GraphCommandDepKey`, keyed by `GraphModuleSpec.name`.

## Anti-patterns

1. **Using graph contracts before validating the module spec** — duplicate or unknown kinds fail later.
2. **Putting engine labels/collection names in specs** — specs hold logical kinds; adapters map physical layout.
3. **Hand-rolling a Neo4j adapter** — `forze_neo4j` already ships one; write a custom `DepsModule` only for engines without an official integration.
4. **Mixing node kind names with module route names** — module name routes deps; node/edge names identify graph kinds.
5. **Using the document query DSL for graph traversals** — graph ports expose explicit traversal methods.

## Reference

> Docs are versioned. These links use `latest` (the newest release). If your app pins an older `forze` minor, replace `latest` in the URL with that version (e.g. `.../forze/0.3/...`) or use the version selector on the site.

- [Graph contracts](https://morzecrew.github.io/forze/latest/reference/contracts/graph/)
- [Neo4j integration](https://morzecrew.github.io/forze/latest/integrations/neo4j/)
- [Specs and wiring](https://morzecrew.github.io/forze/latest/writing-operation/wiring/)
- [`forze-custom-deps`](../forze-custom-deps/SKILL.md)
