---
title: Graph
icon: lucide/share-2
summary: The graph contract — node and edge kinds, with query, command, and raw ports
---

The graph contract models a **bounded context** as typed node and edge *kinds* and gives
three ports over them: a **query** port (traverse), a **command** port (mutate), and a
**raw** port (an escape hatch for native queries). A `GraphModuleSpec` bundles the kinds;
refs (`VertexRef` / `EdgeRef`) address individual nodes and edges by kind + id.

```python
q = ctx.graph.query(spec)      # traverse
c = ctx.graph.command(spec)    # mutate
r = ctx.graph.raw(spec)        # native-query escape hatch
```

## Spec

`GraphModuleSpec` — a module-level bundle of vertex and edge kinds:

| Field | Type | Meaning |
|-------|------|---------|
| `name` | `str \| StrEnum` | the graph area / module name |
| `nodes` | `tuple[GraphNodeSpec, ...]` | the vertex kinds (each a named model) |
| `edges` | `tuple[GraphEdgeSpec, ...]` | the edge kinds (each a named model) |

Each `GraphNodeSpec` / `GraphEdgeSpec` carries a `name` (the *kind*) and a model. Field
[encryption](../../identity-tenancy-enc/encryption.md) covers nodes and key-addressed edges;
endpoint-identity edges reject `binds_record_id`.

## Query port  (`ctx.graph.query(spec)`)

| Method | Notes |
|--------|-------|
| `get_vertex(ref)` / `get_vertices(refs)` | fetch nodes by ref |
| `get_edge(ref)` / `get_edges(refs)` | fetch edges by ref |
| `neighbors(ref, …)` | adjacent nodes / edges, filtered by kind and direction |
| `shortest_path(from_ref, to_ref, …)` | a path between two nodes |

## Command port  (`ctx.graph.command(spec)`)

| Method | Notes |
|--------|-------|
| `create_vertex` / `create_vertices` | add nodes |
| `create_edge` / `create_edges` | add edges |
| `delete_vertex` / `delete_vertices` | remove nodes |
| `delete_edge` / `delete_edges` | remove edges |

## Raw port  (`ctx.graph.raw(spec)`)

`run(...)` executes a native query (Cypher) — the escape hatch for traversals the typed
ports don't express.

## Implemented by

| Backend | Integration |
|---------|-------------|
| Neo4j | [Neo4j](../../integrations/neo4j.md) |

A mock implements the surface for tests.
