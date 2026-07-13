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

Each `GraphNodeSpec` / `GraphEdgeSpec` carries a `name` (the *kind*), a `read` model, and
optional `create` / `update` models. An edge also declares its `identity` (`"key"` or
`"endpoints"`), its allowed `endpoints`, and its `directionality` (`directed` /
`symmetric`). Field [encryption](../../identity-tenancy-enc/encryption.md) covers nodes and
key-addressed edges; endpoint-identity edges reject `binds_record_id`.

## Query port  (`ctx.graph.query(spec)`)

| Method | Notes |
|--------|-------|
| `get_vertex(ref)` / `get_vertices(refs)` | fetch nodes by ref |
| `get_edge(ref)` / `get_edges(refs)` | fetch edges by ref |
| `vertex_exists(ref)` / `edge_exists(ref)` | presence checks |
| `find_vertices(node_kind, *, property_filter=None, limit=100, offset=0)` | list nodes of a kind, optionally filtered by properties |
| `find_edges(edge_kind, *, property_filter=None, limit=100, offset=0)` | list edges of a kind |
| `count_vertices(node_kind, *, property_filter=None)` / `count_edges(edge_kind, …)` | counts by kind |
| `neighbors(origin, direction, edge_kinds, *, limit, to_vertex_kinds=None)` | adjacent nodes with the connecting edge, filtered by kind and direction |
| `incident_edges(origin, direction, edge_kinds, *, limit)` | the edges touching a node |
| `vertex_degree(ref, …)` / `count_neighbors(ref, …)` | degree / distinct-neighbor counts by direction and edge kind |
| `expand(start, params)` | breadth-limited walk (`GraphWalkParams`: `max_depth`, `max_results`, direction, edge kinds) returning `GraphWalkStep`s |
| `scoped_walk(anchor, params)` | multi-step typed traversal (`ScopedWalkParams`: a tuple of `GraphPathStep`s with per-step hop bounds, a `target_kind`, a `limit`) |
| `shortest_path(from_ref, to_ref, params)` | one path (`ShortestPathParams`: `max_hops`, `edge_kinds`, optional `weight_property` for weighted/native paths — needs a GDS-capable backend, else `graph_algorithm_unavailable`) |
| `k_shortest_paths(from_ref, to_ref, params, *, k)` | the `k` best paths, same params |

## Command port  (`ctx.graph.command(spec)`)

Every create/ensure takes `return_new: bool = True`.

| Method | Notes |
|--------|-------|
| `create_vertex(node_kind, cmd)` / `create_vertices(items)` | add nodes |
| `create_edge(edge_kind, cmd)` / `create_edges(items)` | add edges |
| `ensure_vertex(node_kind, cmd)` / `ensure_edge(edge_kind, cmd)` | insert-when-missing (idempotent by identity) |
| `update_vertex(ref, cmd)` / `update_edge(ref, cmd)` | patch by ref |
| `delete_vertex(ref)` / `delete_vertices(refs)` | remove nodes |
| `delete_edge(ref)` / `delete_edges(refs)` | remove edges |

## Raw port  (`ctx.graph.raw(spec)`)

`run(query, params=None)` executes a native query (Cypher) — the escape hatch for
traversals the typed ports don't express.

## Management port  (`ctx.graph.management(spec)`)

Control-plane schema provisioning, separate from the data-plane command port:
`ensure_schema()` creates constraints/indexes for the declared kinds; `drop_schema()`
removes them.

## Implemented by

| Backend | Integration |
|---------|-------------|
| Neo4j | [Neo4j](../../integrations/neo4j.md) |

A mock implements the surface for tests.
