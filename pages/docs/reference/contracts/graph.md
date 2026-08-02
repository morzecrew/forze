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

### Choosing an edge `identity`

This is the one modelling decision the graph contract really asks of you, and it is **not** a
question about whether the edge happens to have an id field. It is: *what makes one of these
edges the same edge?*

| `identity` | Means | Then |
|---|---|---|
| `"endpoints"` | **At most one edge of this kind per `(from, to)` pair.** The pair *is* the identity. | `EdgeRef.by_endpoints(...)` addresses it. A second `create_edge` on the same pair raises `conflict` — use `ensure_edge` to leave the existing one alone, or `update_edge` to change it. |
| `"key"` *(default)* | Each edge has a business key of its own (`key_field`). | `EdgeRef.by_key(...)` addresses it. **Parallel edges between the same pair are allowed** — they are distinct entities with distinct keys. |

So: **if two edges of a kind can legitimately run between the same pair — two flights between
two cities, two roads between two towns — the kind is `"key"`, not `"endpoints"`.** That is not
a workaround for a limitation; an edge that is a distinct entity needs a key to *be* one.
Declaring such a kind `"endpoints"` leaves it with no identity at all: `get_edge` would return an
arbitrary one of the parallel edges and `update_edge` / `delete_edge` would hit every one.

`create_edge` enforces this: on an endpoint-identified kind, a second create on a pair that
already has an edge raises `conflict` rather than laying a parallel one. A malformed spec —
`identity="key"` with no `key_field`, an endpoint naming a node kind that does not exist, a key
field absent from its own read model — is refused when the `GraphModuleSpec` is *constructed*.

!!! tip "Finding pairs that already carry parallel edges"

    `create_edge` refuses to make them, but a graph written before it did not, and the raw query
    hatch (or any writer that is not forze) still can. The pair cursor surfaces them for free —
    it yields one batch per `(from, to)` pair, so a batch with more than one edge *is* a
    violating pair:

    ```python
    async for batch in ctx.graph.query(SOCIAL).find_edges_stream("FOLLOWS", chunk_size=1):
        if len(batch) > 1:
            ...  # this pair carries parallel edges — decide which one is real
    ```

    Worth running once after upgrading. The framework cannot pick for you, and an export will
    carry all of them (it carries the graph it *finds*, not the graph the spec hoped for).

## Query port  (`ctx.graph.query(spec)`)

| Method | Notes |
|--------|-------|
| `get_vertex(ref)` / `get_vertices(refs)` | fetch nodes by ref |
| `get_edge(ref)` / `get_edges(refs)` | fetch edges by ref |
| `vertex_exists(ref)` / `edge_exists(ref)` | presence checks |
| `find_vertices(node_kind, *, property_filter=None, limit=100, offset=0)` | list nodes of a kind, optionally filtered by properties |
| `find_edges(edge_kind, *, property_filter=None, limit=100, offset=0)` | list edges of a kind |
| `count_vertices(node_kind, *, property_filter=None)` / `count_edges(edge_kind, …)` | counts by kind |
| `neighbors(origin, direction, edge_kinds, *, limit, to_vertex_kinds=None)` | adjacent nodes with the connecting edge, filtered by kind and direction. `limit` truncates to an **arbitrary** subset — see below |
| `incident_edges(origin, direction, edge_kinds, *, limit)` | the edges touching a node |
| `vertex_degree(ref, …)` / `count_neighbors(ref, …)` | degree / distinct-neighbor counts by direction and edge kind |
| `expand(start, params)` | breadth-limited walk (`GraphWalkParams`: `max_depth`, `max_results`, direction, edge kinds) returning `GraphWalkStep`s |
| `scoped_walk(anchor, params)` | multi-step typed traversal (`ScopedWalkParams`: a tuple of `GraphPathStep`s with per-step hop bounds, a `target_kind`, a `limit`) |
| `shortest_path(from_ref, to_ref, params)` | one path (`ShortestPathParams`: `max_hops`, `edge_kinds`, optional `weight_property` for weighted/native paths — needs a GDS-capable backend, else `graph_algorithm_unavailable`) |
| `k_shortest_paths(from_ref, to_ref, params, *, k)` | the `k` best paths, same params |

### Streaming a whole kind

`find_vertices` and `find_edges` page by **offset**, which is fine for a screenful and wrong
for a sweep: `SKIP n` counts rows from the start of a result set that is being written
underneath it, so a node created before the cursor shifts every later row one place along and
the next page steps over one. For a migration or an export, a skipped page and an empty page
produce the same artifact.

The streaming reads seek by key instead — `key_field > last-seen` — so a bookmark cannot move:

| Method | Notes |
|--------|-------|
| `find_vertices_stream(node_kind, *, property_filter=None, chunk_size=500)` | async generator of keyset batches; walks the kind to exhaustion |
| `find_edges_stream(edge_kind, *, property_filter=None, chunk_size=500)` | same, for an edge kind of **either** identity |

```python
async for batch in ctx.graph.query(SOCIAL).find_vertices_stream("User", chunk_size=500):
    ...  # memory is bounded by chunk_size, whatever the graph's size
```

**What an edge bookmarks on.** A keyed edge (`identity="key"`) bookmarks on its own key. An
edge declared `identity="endpoints"` has no key of its own — that is what the declaration means
— so it bookmarks on the **`(tail, head)` node-key pair**, which *is* the identity the author
asserted. For such a kind, `chunk_size` bounds **pairs, not edges**, and every edge of a pair
is yielded together: nothing enforces the one-edge-per-pair identity (`create_edge` will add a
second parallel edge), so a page cut *within* a pair would leave edges behind a cursor that
seeks strictly past it.

They **fail closed** rather than serve a scan that looks complete and is not
(`graph_streaming_unsupported`):

- a backend that does not report `GraphReadCapabilities` supports neither stream;
- a multi-endpoint edge kind whose endpoint node kinds key on *different* properties — a
  `TAGGED` kind linking `Post → Tag` and `Note → Tag` where `Post` and `Note` key differently
  has no single ordering that covers both.

!!! warning "A key field may not be encrypted"

    Naming a kind's `key_field` in its own `encryption` policy is refused at spec construction
    (`graph_sealed_key_field`). **A sealed key is not a key.** A lookup by key compares the
    caller's *plaintext* against what the write *sealed*, so the two never meet: a vertex
    created under a sealed key could never be fetched, updated or deleted by that key again —
    a write-only black hole. It also has no order to page or seek on. Encrypting an ordinary
    property is entirely fine; this is about the key alone.

!!! warning "A key field must be a string"

    `VertexRef.key` is a `str`, so a `key_field` declared as `bool`, `int` or `float` is
    refused at spec construction (`graph_non_string_key_field`). A store that keeps the
    property in its native type never matches the string a keyed lookup binds — measured on
    Neo4j with an `int` key, the writes land and `find_vertices` returns the rows while
    `vertex_exists`, `get_vertex`, `vertex_degree` and `neighbors` all answer as though
    the vertex were not there. No error, just an empty graph, and the in-memory mock keys its
    store by `str(value)` so everything works there. Use a `str` key — or a `UUID` or
    str-valued enum, which reach the store as text — and keep the number as an ordinary
    property.

    A `Decimal` key **is** allowed: properties are written through `model_dump(mode="json")`,
    which renders it as a string, so Neo4j holds `STRING` and every keyed read resolves —
    exponent and high-precision forms included, byte-exact. Note that the key is that *text*:
    `Decimal("1.50")` and `Decimal("1.5")` are two different keys even though Python calls
    them equal.

!!! note "A bounded `neighbors` call returns an *arbitrary* subset"

    When `limit` is smaller than the neighbourhood, which neighbours come back is the engine's
    choice: Neo4j's adjacency `LIMIT` carries no `ORDER BY`, and the mock walks its edge list in
    insertion order. The differential compares how many arrive, not which — asking Neo4j for a
    deterministic page would mean sorting a whole neighbourhood before truncating it, and the
    cost lands hardest on the high-degree vertices a limit exists to protect.

    What you *can* rely on is that the page is **full**: a `to_vertex_kinds` filter is applied
    before the limit on every backend, so a short page means the neighbourhood really is
    exhausted. That distinction is load-bearing — the rows a filter drops are not a cursor you
    can page past, so a page cut short by filtering would read as "there are no more".

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
removes them. Both are idempotent, and safe to call in either order — a teardown that
runs before anything was provisioned succeeds.

On Neo4j this is **required for correctness, not just speed**: node-key uniqueness is the
constraint `ensure_schema()` creates, and without it Cypher `CREATE` will happily write a
second node under a key that is supposed to identify one. Run it at startup.

The mock implements the port so that startup step is exercisable in tests, but both
methods are no-ops there: the in-memory store keys vertices directly, so uniqueness is a
property of the data structure and `drop_schema()` cannot take it away. Observing what an
engine does *without* its constraints needs that engine.

## Implemented by

| Backend | Integration |
|---------|-------------|
| Neo4j | [Neo4j](../../integrations/neo4j.md) |

A mock implements the surface for tests.
