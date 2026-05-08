---
name: forze-graph-contracts
description: >-
  Models Forze graph contracts with GraphModuleSpec, GraphNodeSpec,
  GraphEdgeSpec, graph refs, query/command ports, and dependency keys. Use when
  adding graph-shaped features or preparing Neo4j/Arango-style adapters.
---

# Forze graph contracts

Use when an application feature needs vertices, relationships, neighborhood expansion, or shortest-path style queries. This skill covers **core contracts**; this repository does not currently ship a concrete graph adapter package or mock graph adapter.

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

query = ctx.dep(GraphQueryDepKey, route=project_graph.name)(ctx, project_graph)
owner_rows = await query.neighbors(
    VertexRef(kind="user", key=user_id),
    direction=GraphDirection.OUT,
    edge_kinds=frozenset({"owns"}),
    limit=20,
)

command = ctx.dep(GraphCommandDepKey, route=project_graph.name)(ctx, project_graph)
created = await command.create_vertex("project", CreateProjectNode(name="Demo"))
```

## Port semantics

`GraphQueryPort` covers `get_vertex`, `get_edge`, existence checks, counts, neighborhood queries, incident edges, expansion, shortest path, and simple find operations.

`GraphCommandPort` covers create/update/delete for vertices and edges, batch creation, and ensure operations. Adapter implementations define stable key semantics through `VertexRef` and `EdgeRef`.

## Adapter guidance

Keep Cypher, AQL, and engine-specific query strings inside adapters. Register graph providers as routed deps under `GraphQueryDepKey` and `GraphCommandDepKey`, keyed by `GraphModuleSpec.name`.

## Anti-patterns

1. **Using graph contracts before validating the module spec** — duplicate or unknown kinds fail later.
2. **Putting engine labels/collection names in specs** — specs hold logical kinds; adapters map physical layout.
3. **Assuming a built-in graph adapter exists** — add a custom deps module for Neo4j/Arango-style backends.
4. **Mixing node kind names with module route names** — module name routes deps; node/edge names identify graph kinds.
5. **Using the document query DSL for graph traversals** — graph ports expose explicit traversal methods.

## Reference

- [`src/forze/application/contracts/graph`](../../src/forze/application/contracts/graph)
- [`src/forze/application/contracts/base/specs.py`](../../src/forze/application/contracts/base/specs.py)
- [`src/forze/application/execution/deps.py`](../../src/forze/application/execution/deps.py)
