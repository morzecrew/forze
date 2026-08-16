# Graph

Node and edge specs, the traversal ports they resolve, and wiring Neo4j or a custom engine. Graph traversal is its own plane: the document query DSL does not reach it.

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
```

**The module validates itself at construction.** A duplicate kind name, an endpoint naming a node kind the module does not declare, or a `key_field` missing from its own read model fails at build — not at the first `get_edge`. You no longer call `validate_graph_module_spec` yourself.

`GraphEdgeEndpoint.from_kind` / `to_kind` are strings and must match node kind values in the same module.

Edge identity, and this is the decision to get right up front:

- `identity="key"` (the default) addresses each edge by a stable business key and **requires** `key_field` (a field of the edge read model); `ensure_edge` upserts on that key so concurrent calls cannot create duplicates. The shortest edge declaration you would write is a keyed edge with no key — that now fails at construction instead of at first use.
- `identity="endpoints"` means **at most one edge of the kind per `(from, to)` pair, and that uniqueness is enforced** — a second create conflicts. A kind that legitimately needs parallel edges between the same two vertices must declare a `key_field` and identify by it.

A node or edge `key_field` may not be sealed — an encrypted key cannot be addressed. It
must also be **string-typed** (`str`, or a `UUID`/str-enum, which reach the store as text):
`VertexRef.key` is a `str`, so a key field declared `int`/`float`/`bool`/`Decimal` is
refused at construction (`graph_non_string_key_field`). On a store that keeps the native
type, a keyed lookup would match nothing and every read would come back empty with no
error. Keep the number as an ordinary property.

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

For bounded-memory reads over a whole kind — an export, a reindex, a migration — the query port streams keyset pages: `find_vertices_stream` and `find_edges_stream`. Both are capability-gated; a backend without keyset support refuses rather than silently paging in memory. Deleting a vertex detaches its edges, an edge requires both endpoints to exist, a duplicate key conflicts, and an unknown kind raises — the mock enforces all four exactly as Neo4j does.

## Adapter guidance

Prefer `forze_neo4j` when Neo4j fits:

```python
from forze_neo4j import Neo4jClient, Neo4jDepsModule, Neo4jGraphConfig, neo4j_lifecycle_step

graph_module = Neo4jDepsModule(
    client=Neo4jClient(),
    graphs={
        GraphModule.SOCIAL: Neo4jGraphConfig(
            tenant_aware=True,
            # Every hop is checked, not just the anchor. `anchor` verifies the starting
            # vertex and lets a traversal leave the tenant through an edge; the default
            # refuses that, which is why it is the default.
            traversal_isolation="full-path",
            allow_raw_query=False,
        ),
    },
    tx={TxRoute.DEFAULT},
)
lifecycle = neo4j_lifecycle_step(uri="neo4j://localhost:7687", auth=("neo4j", secret))
```

`Neo4jDepsModule(client=..., graphs={...}, tx={...})` registers query/command (plus raw-query and management) ports per graph module, supports keyed-edge `ensure_edge` identity (`identity="key"` with `key_field`) and native/weighted `k_shortest_paths`, and offers tenant isolation tiers (tagged property, per-tenant database, routed client). For custom adapters, keep Cypher, AQL, and engine-specific query strings inside the adapter and register providers as routed deps under `GraphQueryDepKey` and `GraphCommandDepKey`, keyed by `GraphModuleSpec.name`.

## Anti-patterns

- **Declaring `identity="endpoints"` for a kind that needs parallel edges** — the pair is unique and a second create conflicts; give the kind a `key_field` instead.
- **Putting engine labels/collection names in specs** — specs hold logical kinds; adapters map physical layout.
- **Hand-rolling a Neo4j adapter** — `forze_neo4j` already ships one; write a custom `DepsModule` only for engines without an official integration.
- **Mixing node kind names with module route names** — module name routes deps; node/edge names identify graph kinds.
- **Using the document query DSL for graph traversals** — graph ports expose explicit traversal methods.

## Reference

- [Graph contracts](https://morzecrew.github.io/forze/latest/reference/contracts/graph/)
- [Neo4j integration](https://morzecrew.github.io/forze/latest/integrations/neo4j/)
