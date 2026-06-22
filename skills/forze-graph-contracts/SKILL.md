---
name: forze-graph-contracts
description: >-
  Models Forze graph contracts with GraphModuleSpec, GraphNodeSpec,
  GraphEdgeSpec, graph refs, query/command ports, and dependency keys. Use when
  adding graph-shaped features and wiring a Neo4j or Arango-style adapter in
  your application.
---

# Forze graph contracts

Use when your application needs vertices, relationships, neighborhood expansion, or shortest-path style queries. Forze ships graph **contracts** in `forze.application.contracts.graph`; there is no official `forze_graph` integration package. Implement `GraphQueryDepKey` / `GraphCommandDepKey` in your app (see [`forze-custom-deps`](../forze-custom-deps/SKILL.md)) or use a vendor-specific adapter you maintain.

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
            endpoints=(GraphEdgeEndpoint(from_kind="user", to_kind="project"),),
            directionality=GraphEdgeDirectionality.DIRECTED,
        ),
    ),
)
validate_graph_module_spec(project_graph)
```

`GraphEdgeEndpoint.from_kind` / `to_kind` are strings and must match node kind values in the same module.

## Resolving ports

Graph ports do not have `ExecutionContext` convenience helpers. Resolve routed factories explicitly.

```python
from forze.application.contracts.graph import GraphCommandDepKey, GraphQueryDepKey, VertexRef

query = ctx.deps.resolve_configurable(
    ctx, GraphQueryDepKey, project_graph, route=project_graph.name
)
owner_rows = await query.neighbors(
    VertexRef(kind="user", key=user_id),
    direction=GraphDirection.OUT,
    edge_kinds=frozenset({"owns"}),
    limit=20,
)

command = ctx.deps.resolve_configurable(
    ctx, GraphCommandDepKey, project_graph, route=project_graph.name
)
created = await command.create_vertex("project", CreateProjectNode(name="Demo"))
```

## Port semantics

`GraphQueryPort` covers `get_vertex`, `get_edge`, existence checks, counts, neighborhood queries, incident edges, expansion, shortest path, and simple find operations.

`GraphCommandPort` covers create/update/delete for vertices and edges, batch creation, and ensure operations. Adapter implementations define stable key semantics through `VertexRef` and `EdgeRef`.

## Adapter guidance

Keep Cypher, AQL, and engine-specific query strings inside your adapter. Register graph providers as routed deps under `GraphQueryDepKey` and `GraphCommandDepKey`, keyed by `GraphModuleSpec.name`.

## Anti-patterns

1. **Using graph contracts before validating the module spec** — duplicate or unknown kinds fail later.
2. **Putting engine labels/collection names in specs** — specs hold logical kinds; adapters map physical layout.
3. **Assuming a built-in graph integration package exists** — add a custom `DepsModule` for your engine.
4. **Mixing node kind names with module route names** — module name routes deps; node/edge names identify graph kinds.
5. **Using the document query DSL for graph traversals** — graph ports expose explicit traversal methods.

## Reference

> Docs are versioned. These links use `latest` (the newest release). If your app pins an older `forze` minor, replace `latest` in the URL with that version (e.g. `.../forze/0.3/...`) or use the version selector on the site.

- [Graph contracts](https://morzecrew.github.io/forze/latest/reference/contracts/)
- [Specs and wiring](https://morzecrew.github.io/forze/latest/writing-operation/wiring/)
- [`forze-custom-deps`](../forze-custom-deps/SKILL.md)
