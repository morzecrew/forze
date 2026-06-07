# Graph contracts

Graph contracts model vertices, edges, neighborhood expansion, and path queries behind `GraphQueryPort` and `GraphCommandPort`. There is no official `forze_graph` integration package yet — wire a Neo4j, ArangoDB, or other engine adapter in your application with a custom `DepsModule` (see the **forze-graph-contracts** and **forze-custom-deps** agent skills).

## `GraphModuleSpec`

| Section | Details |
|---------|---------|
| Purpose | Names a bounded graph area and declares node/edge kinds for that module. |
| Import path | `from forze.application.contracts.graph import GraphModuleSpec, validate_graph_module_spec` |
| Required fields | `name`, `nodes`, `edges`. |
| Routing | `name` is the deps route (`route=spec.name`). |
| Minimal example | `project_graph = GraphModuleSpec(name="project-graph", nodes=(...), edges=(...))` |

Call `validate_graph_module_spec(project_graph)` after construction so duplicate or unknown kinds fail early.

## `GraphNodeSpec` / `GraphEdgeSpec`

Node and edge `name` values are **logical kinds** (use a shared `StrEnum`). Adapters map kinds to physical labels or collections. `GraphEdgeEndpoint.from_kind` / `to_kind` must match node kind strings in the same module.

## Dependency keys

| Key | Port |
|-----|------|
| `GraphQueryDepKey` | `GraphQueryPort` — reads, traversals, shortest path |
| `GraphCommandDepKey` | `GraphCommandPort` — create/update/delete vertices and edges |

Resolve explicitly (no `ExecutionContext` convenience helper):

```python
query = ctx.deps.resolve_configurable(
    ctx, GraphQueryDepKey, project_graph, route=project_graph.name
)
```

## Raw queries & tenancy

The structured ports inject tenant isolation automatically. The escape hatches do not — so
each has an explicit trust model:

- **`ctx.graph.raw(spec)` (`GraphRawQueryPort`)** — engine-specific Cypher. In a **tenant-aware**
  module it *fails closed* (raises if no tenant is bound, instead of running unscoped across all
  tenants) and binds the current tenant as `$tenant`. You must place the filter yourself:

  ```python
  await ctx.graph.raw(spec).run(
      "MATCH (n:User {tenant_id: $tenant}) RETURN n.id AS id", {}
  )
  ```

- **Kernel client ports** (`PostgresClientPort`, `Neo4jClientPort`, resolved via their DepKeys) —
  full bypass; you own all scoping. Get the tenant with `ctx.tenancy.require_current_id()`
  (raises if none bound) rather than reaching into `inv_ctx`. Routed Postgres is already isolated
  per-tenant pool. A query that must legitimately span tenants belongs in a **non**-tenant-aware
  module by construction.

## Related pages

- [Specs and wiring](../../concepts/specs-and-wiring.md)
- [Execution reference](../../reference/execution.md)
